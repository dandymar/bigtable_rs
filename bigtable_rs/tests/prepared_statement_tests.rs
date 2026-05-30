use bigtable_rs::bigtable::prepared_statement::PreparedStatement;
use bigtable_rs::bigtable::{BigTable, BigTableConnection};
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::PrepareQueryResponse;
use http_body_util::{Empty, Full};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use hyper_util::service::TowerToHyperService;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::time::Instant;
use tower::Service;

fn encode_grpc<T: prost::Message>(msg: T) -> Vec<u8> {
    let mut payload = Vec::new();
    msg.encode(&mut payload).unwrap();

    let mut body = Vec::new();
    body.push(0); // compression flag
    body.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    body.extend(payload);
    body
}

#[derive(Clone)]
struct CustomMockService {
    prepare_call_count: Arc<std::sync::atomic::AtomicUsize>,
    execute_call_count: Arc<std::sync::atomic::AtomicUsize>,
    next_prepare_token: Arc<std::sync::Mutex<String>>,
    next_prepare_ttl_secs: Arc<std::sync::atomic::AtomicU64>,
    should_expire_on_execute: Arc<std::sync::atomic::AtomicBool>,
}

impl Service<http::Request<hyper::body::Incoming>> for CustomMockService {
    type Response = http::Response<tonic::body::Body>;
    type Error = std::convert::Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: http::Request<hyper::body::Incoming>) -> Self::Future {
        let self_clone = self.clone();
        Box::pin(async move {
            let path = req.uri().path();

            if path == "/google.bigtable.v2.Bigtable/PrepareQuery" {
                self_clone
                    .prepare_call_count
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

                let token = self_clone.next_prepare_token.lock().unwrap().clone();
                let ttl_secs = self_clone
                    .next_prepare_ttl_secs
                    .load(std::sync::atomic::Ordering::SeqCst);

                let resp_msg = PrepareQueryResponse {
                    prepared_query: token.into_bytes(),
                    valid_until: Some(prost_types::Timestamp {
                        seconds: ttl_secs as i64,
                        nanos: 0,
                    }),
                    metadata: None,
                };

                let resp_bytes = encode_grpc(resp_msg);
                let response = http::Response::builder()
                    .status(200)
                    .header("content-type", "application/grpc")
                    .header("grpc-status", "0")
                    .body(tonic::body::Body::new(Full::new(bytes::Bytes::from(
                        resp_bytes,
                    ))))
                    .unwrap();
                return Ok(response);
            } else if path == "/google.bigtable.v2.Bigtable/ExecuteQuery" {
                self_clone
                    .execute_call_count
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

                if self_clone
                    .should_expire_on_execute
                    .load(std::sync::atomic::Ordering::SeqCst)
                {
                    self_clone
                        .should_expire_on_execute
                        .store(false, std::sync::atomic::Ordering::SeqCst);

                    let response = http::Response::builder()
                        .status(200)
                        .header("content-type", "application/grpc")
                        .header("grpc-status", "3") // InvalidArgument
                        .header(
                            "grpc-message",
                            "PREPARED_QUERY_EXPIRED: the prepared query has expired",
                        )
                        .body(tonic::body::Body::new(Empty::new()))
                        .unwrap();
                    return Ok(response);
                }

                let response = http::Response::builder()
                    .status(200)
                    .header("content-type", "application/grpc")
                    .header("grpc-status", "0")
                    .body(tonic::body::Body::new(Empty::new()))
                    .unwrap();
                return Ok(response);
            }

            let response = http::Response::builder()
                .status(404)
                .body(tonic::body::Body::new(Empty::new()))
                .unwrap();
            Ok(response)
        })
    }
}

async fn start_mock_server() -> (CustomMockService, BigTable) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    listener.set_nonblocking(true).unwrap();
    let listener = tokio::net::TcpListener::from_std(listener).unwrap();

    let service = CustomMockService {
        prepare_call_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        execute_call_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        next_prepare_token: Arc::new(std::sync::Mutex::new("initial_token".to_string())),
        next_prepare_ttl_secs: Arc::new(std::sync::atomic::AtomicU64::new(3600)),
        should_expire_on_execute: Arc::new(std::sync::atomic::AtomicBool::new(false)),
    };

    let service_clone = service.clone();
    tokio::spawn(async move {
        loop {
            if let Ok((stream, _)) = listener.accept().await {
                let io = TokioIo::new(stream);
                let service = TowerToHyperService::new(service_clone.clone());
                tokio::spawn(async move {
                    auto::Builder::new(TokioExecutor::new())
                        .serve_connection(io, service)
                        .await
                        .unwrap_or_default();
                });
            }
        }
    });

    let connection = BigTableConnection::new_with_emulator(
        &addr.to_string(),
        "mock-project",
        "mock-instance",
        false,
        1,
        None,
    )
    .unwrap();

    (service, connection.client())
}

#[tokio::test]
async fn test_prepared_statement_singleflight() {
    let (mock_service, client) = start_mock_server().await;
    let stmt = Arc::new(PreparedStatement::new(
        "SELECT * FROM table".to_string(),
        "default".to_string(),
    ));

    let mut handles = vec![];
    for _ in 0..50 {
        let stmt_clone = stmt.clone();
        let mut client_clone = client.clone();
        handles.push(tokio::spawn(async move {
            stmt_clone.get_or_prepare(&mut client_clone).await.unwrap()
        }));
    }

    for h in handles {
        let plan = h.await.unwrap();
        assert_eq!(plan.plan_token, b"initial_token");
    }

    assert_eq!(
        mock_service
            .prepare_call_count
            .load(std::sync::atomic::Ordering::SeqCst),
        1
    );
}

#[tokio::test]
async fn test_prepared_statement_reactive_retry() {
    let (mock_service, mut client) = start_mock_server().await;
    let stmt = PreparedStatement::new("SELECT * FROM table".to_string(), "default".to_string());

    let plan = stmt.get_or_prepare(&mut client).await.unwrap();
    assert_eq!(plan.plan_token, b"initial_token");

    *mock_service.next_prepare_token.lock().unwrap() = "new_token".to_string();
    mock_service
        .should_expire_on_execute
        .store(true, std::sync::atomic::Ordering::SeqCst);

    let _stream = stmt
        .execute_with_retry(&mut client, std::collections::HashMap::new())
        .await
        .unwrap();

    let active_plan = stmt.plan_state().load_full().unwrap();
    assert_eq!(active_plan.plan_token, b"new_token");

    assert_eq!(
        mock_service
            .prepare_call_count
            .load(std::sync::atomic::Ordering::SeqCst),
        2
    );
    assert_eq!(
        mock_service
            .execute_call_count
            .load(std::sync::atomic::Ordering::SeqCst),
        2
    );
}

#[tokio::test]
async fn test_proactive_background_refresh_rotation() {
    let _ = env_logger::try_init();
    tokio::time::pause();

    let (mock_service, mut client) = start_mock_server().await;
    let stmt = PreparedStatement::new("SELECT * FROM table".to_string(), "default".to_string());

    mock_service
        .next_prepare_ttl_secs
        .store(30, std::sync::atomic::Ordering::SeqCst);

    let start_instant = Instant::now();
    let _stream = stmt
        .execute_with_retry(&mut client, std::collections::HashMap::new())
        .await
        .unwrap();
    log::info!(
        "First execution finished. Elapsed: {}s",
        Instant::now().duration_since(start_instant).as_secs()
    );

    // Advance virtual time to T = 10s and execute again to mark the statement active
    tokio::time::advance(Duration::from_secs(10)).await;
    log::info!(
        "Time advanced by 10s. Elapsed: {}s",
        Instant::now().duration_since(start_instant).as_secs()
    );
    let _stream2 = stmt
        .execute_with_retry(&mut client, std::collections::HashMap::new())
        .await
        .unwrap();
    log::info!(
        "Second execution finished. Elapsed: {}s",
        Instant::now().duration_since(start_instant).as_secs()
    );

    let plan_v1 = stmt.plan_state().load_full().unwrap();
    assert_eq!(plan_v1.plan_token, b"initial_token");
    assert_eq!(
        mock_service
            .prepare_call_count
            .load(std::sync::atomic::Ordering::SeqCst),
        1
    );

    // Yield to let the spawned background task register its sleep timer
    tokio::task::yield_now().await;

    *mock_service.next_prepare_token.lock().unwrap() = "token_v2".to_string();

    // Proactive refresh offset for TTL <= 300 is TTL / 5 = 6s. So sleep_until(expires_at - 6s) = 24s relative to base.
    // We are currently at T = 10s. Let's advance time by another 14 seconds to reach T = 24s and trigger wakeup
    tokio::time::advance(Duration::from_secs(14)).await;
    log::info!(
        "Time advanced by another 15s. Elapsed: {}s",
        Instant::now().duration_since(start_instant).as_secs()
    );
    let mut success = false;
    for _i in 0..100 {
        tokio::task::yield_now().await;
        let active_plan = stmt.plan_state().load_full().unwrap();
        if active_plan.plan_token == b"token_v2" {
            success = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        success,
        "Failed to rotate plan token asynchronously within virtual timeout"
    );
    assert_eq!(
        mock_service
            .prepare_call_count
            .load(std::sync::atomic::Ordering::SeqCst),
        2
    );
}

#[tokio::test]
async fn test_prepared_statement_idle_pollution_guard() {
    tokio::time::pause();

    let (mock_service, mut client) = start_mock_server().await;
    let stmt = PreparedStatement::new("SELECT * FROM table".to_string(), "default".to_string());

    mock_service
        .next_prepare_ttl_secs
        .store(3600, std::sync::atomic::Ordering::SeqCst);

    let plan_v1 = stmt.get_or_prepare(&mut client).await.unwrap();
    assert_eq!(plan_v1.plan_token, b"initial_token");

    // Yield to let the spawned background task run and register its sleep timer
    tokio::task::yield_now().await;

    *mock_service.next_prepare_token.lock().unwrap() = "token_v2".to_string();

    tokio::time::advance(Duration::from_secs(3541)).await;
    tokio::time::sleep(Duration::from_secs(10)).await;

    let active_plan = stmt.plan_state().load_full().unwrap();
    assert_eq!(active_plan.plan_token, b"initial_token");
    assert_eq!(
        mock_service
            .prepare_call_count
            .load(std::sync::atomic::Ordering::SeqCst),
        1
    );
}
