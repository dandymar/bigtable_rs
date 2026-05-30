#![allow(deprecated)]

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
    pub executed_requests: Arc<
        std::sync::Mutex<
            Vec<googleapis_tonic_google_bigtable_v2::google::bigtable::v2::ExecuteQueryRequest>,
        >,
    >,
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

                use http_body_util::BodyExt;
                use prost::Message;
                let body = req.into_body();
                if let Ok(collected) = body.collect().await {
                    let bytes = collected.to_bytes();
                    if bytes.len() >= 5 {
                        let len =
                            u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]) as usize;
                        if bytes.len() >= 5 + len {
                            let payload = &bytes[5..5 + len];
                            if let Ok(decoded_req) = googleapis_tonic_google_bigtable_v2::google::bigtable::v2::ExecuteQueryRequest::decode(payload) {
                                self_clone.executed_requests.lock().unwrap().push(decoded_req);
                            }
                        }
                    }
                }

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
        executed_requests: Arc::new(std::sync::Mutex::new(Vec::new())),
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

#[tokio::test]
async fn test_client_transparent_raw_query_caching() {
    use googleapis_tonic_google_bigtable_v2::google;
    let (mock_service, mut client) = start_mock_server().await;

    let mut params = std::collections::HashMap::new();
    params.insert(
        "param1".to_string(),
        google::bigtable::v2::Value {
            kind: Some(google::bigtable::v2::value::Kind::StringValue(
                "val1".to_string(),
            )),
            ..Default::default()
        },
    );

    for _ in 0..5 {
        let request = google::bigtable::v2::ExecuteQueryRequest {
            instance_name: client.instance_name().to_string(),
            app_profile_id: "default".to_string(),
            query: "SELECT * FROM table WHERE col = @param1".to_string(),
            params: params.clone(),
            ..Default::default()
        };

        let _stream = client.execute_query(request).await.unwrap();
    }

    assert_eq!(
        mock_service
            .prepare_call_count
            .load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    assert_eq!(
        mock_service
            .execute_call_count
            .load(std::sync::atomic::Ordering::SeqCst),
        5
    );
}

#[tokio::test]
async fn test_client_prepare_bind_execute() {
    use googleapis_tonic_google_bigtable_v2::google;
    let (mock_service, mut client) = start_mock_server().await;

    let mut param_types = std::collections::HashMap::new();
    param_types.insert(
        "param1".to_string(),
        google::bigtable::v2::Type {
            kind: Some(google::bigtable::v2::r#type::Kind::StringType(
                Default::default(),
            )),
        },
    );

    let prepare_req = google::bigtable::v2::PrepareQueryRequest {
        instance_name: client.instance_name().to_string(),
        app_profile_id: "default".to_string(),
        query: "SELECT * FROM table WHERE col = @param1".to_string(),
        param_types,
        ..Default::default()
    };

    let prepare_resp = client.prepare_query(prepare_req).await.unwrap();
    assert_eq!(prepare_resp.prepared_query, b"initial_token");

    let mut params = std::collections::HashMap::new();
    params.insert(
        "param1".to_string(),
        google::bigtable::v2::Value {
            kind: Some(google::bigtable::v2::value::Kind::StringValue(
                "val1".to_string(),
            )),
            ..Default::default()
        },
    );

    let execute_req = google::bigtable::v2::ExecuteQueryRequest {
        instance_name: client.instance_name().to_string(),
        app_profile_id: "default".to_string(),
        prepared_query: prepare_resp.prepared_query.clone(),
        params,
        ..Default::default()
    };

    let _stream = client.execute_query(execute_req).await.unwrap();

    assert_eq!(
        mock_service
            .prepare_call_count
            .load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    assert_eq!(
        mock_service
            .execute_call_count
            .load(std::sync::atomic::Ordering::SeqCst),
        1
    );
}

#[tokio::test]
async fn test_client_proactive_refresh_under_paused_time() {
    let _ = env_logger::builder().is_test(true).try_init();
    use googleapis_tonic_google_bigtable_v2::google;
    tokio::time::pause();

    let (mock_service, mut client) = start_mock_server().await;

    mock_service
        .next_prepare_ttl_secs
        .store(30, std::sync::atomic::Ordering::SeqCst);

    let mut params = std::collections::HashMap::new();
    params.insert(
        "param1".to_string(),
        google::bigtable::v2::Value {
            kind: Some(google::bigtable::v2::value::Kind::StringValue(
                "val1".to_string(),
            )),
            ..Default::default()
        },
    );

    let request = google::bigtable::v2::ExecuteQueryRequest {
        instance_name: client.instance_name().to_string(),
        app_profile_id: "default".to_string(),
        query: "SELECT * FROM table WHERE col = @param1".to_string(),
        params: params.clone(),
        ..Default::default()
    };

    let _stream = client.execute_query(request.clone()).await.unwrap();

    assert_eq!(
        mock_service
            .prepare_call_count
            .load(std::sync::atomic::Ordering::SeqCst),
        1
    );

    // Advance virtual time to T = 10s and execute again to mark the statement active
    tokio::time::advance(Duration::from_secs(10)).await;
    let _stream2 = client.execute_query(request.clone()).await.unwrap();

    // Yield to allow the proactive background task to spawn and schedule sleep
    tokio::task::yield_now().await;

    *mock_service.next_prepare_token.lock().unwrap() = "token_v2".to_string();

    // Advance virtual time by 14 more seconds (total T = 24s) to trigger proactive refresh
    tokio::time::advance(Duration::from_secs(14)).await;

    let mut success = false;
    for _ in 0..100 {
        tokio::task::yield_now().await;
        let cache_match = client.statement_cache().lookup_by_token(b"token_v2");
        if cache_match.is_some() {
            success = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert!(
        success,
        "Proactive background task failed to refresh and register token_v2 in client cache"
    );

    assert_eq!(
        mock_service
            .prepare_call_count
            .load(std::sync::atomic::Ordering::SeqCst),
        2
    );
}

#[tokio::test]
async fn test_client_reactive_invalidation_and_retry() {
    use googleapis_tonic_google_bigtable_v2::google;
    let (mock_service, mut client) = start_mock_server().await;

    let mut params = std::collections::HashMap::new();
    params.insert(
        "param1".to_string(),
        google::bigtable::v2::Value {
            kind: Some(google::bigtable::v2::value::Kind::StringValue(
                "val1".to_string(),
            )),
            ..Default::default()
        },
    );

    let request = google::bigtable::v2::ExecuteQueryRequest {
        instance_name: client.instance_name().to_string(),
        app_profile_id: "default".to_string(),
        query: "SELECT * FROM table WHERE col = @param1".to_string(),
        params: params.clone(),
        ..Default::default()
    };

    let _stream = client.execute_query(request.clone()).await.unwrap();
    assert_eq!(
        mock_service
            .prepare_call_count
            .load(std::sync::atomic::Ordering::SeqCst),
        1
    );

    *mock_service.next_prepare_token.lock().unwrap() = "refreshed_token".to_string();
    mock_service
        .should_expire_on_execute
        .store(true, std::sync::atomic::Ordering::SeqCst);

    let _stream2 = client.execute_query(request).await.unwrap();

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
        3
    );

    let has_refreshed_token = client
        .statement_cache()
        .lookup_by_token(b"refreshed_token")
        .is_some();
    assert!(has_refreshed_token);
}

#[tokio::test]
async fn test_client_raw_query_transparent_caching() {
    use googleapis_tonic_google_bigtable_v2::google;
    let (mock_service, mut client) = start_mock_server().await;

    let mut params = std::collections::HashMap::new();
    params.insert(
        "param1".to_string(),
        google::bigtable::v2::Value {
            kind: Some(google::bigtable::v2::value::Kind::StringValue(
                "val1".to_string(),
            )),
            ..Default::default()
        },
    );

    for _ in 0..5 {
        let request = google::bigtable::v2::ExecuteQueryRequest {
            instance_name: client.instance_name().to_string(),
            app_profile_id: "default".to_string(),
            query: "SELECT * FROM table WHERE col = @param1".to_string(),
            params: params.clone(),
            ..Default::default()
        };

        let _stream = client.execute_query(request).await.unwrap();
    }

    assert_eq!(
        mock_service
            .prepare_call_count
            .load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    assert_eq!(
        mock_service
            .execute_call_count
            .load(std::sync::atomic::Ordering::SeqCst),
        5
    );
}

#[tokio::test]
async fn test_client_raw_query_proactive_refresh() {
    let _ = env_logger::builder().is_test(true).try_init();
    use googleapis_tonic_google_bigtable_v2::google;
    tokio::time::pause();

    let (mock_service, mut client) = start_mock_server().await;

    mock_service
        .next_prepare_ttl_secs
        .store(30, std::sync::atomic::Ordering::SeqCst);

    let mut params = std::collections::HashMap::new();
    params.insert(
        "param1".to_string(),
        google::bigtable::v2::Value {
            kind: Some(google::bigtable::v2::value::Kind::StringValue(
                "val1".to_string(),
            )),
            ..Default::default()
        },
    );

    let request = google::bigtable::v2::ExecuteQueryRequest {
        instance_name: client.instance_name().to_string(),
        app_profile_id: "default".to_string(),
        query: "SELECT * FROM table WHERE col = @param1".to_string(),
        params: params.clone(),
        ..Default::default()
    };

    let _stream = client.execute_query(request.clone()).await.unwrap();

    assert_eq!(
        mock_service
            .prepare_call_count
            .load(std::sync::atomic::Ordering::SeqCst),
        1
    );

    tokio::time::advance(Duration::from_secs(10)).await;
    let _stream2 = client.execute_query(request.clone()).await.unwrap();

    tokio::task::yield_now().await;

    *mock_service.next_prepare_token.lock().unwrap() = "token_v2".to_string();

    tokio::time::advance(Duration::from_secs(14)).await;

    let mut success = false;
    for _ in 0..100 {
        tokio::task::yield_now().await;
        let cache_match = client.statement_cache().lookup_by_token(b"token_v2");
        if cache_match.is_some() {
            success = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert!(
        success,
        "Proactive background task failed to refresh and register token_v2 in client cache"
    );

    let _stream3 = client.execute_query(request.clone()).await.unwrap();

    let reqs = mock_service.executed_requests.lock().unwrap();
    assert!(!reqs.is_empty());
    let last_req = reqs.last().unwrap();
    assert_eq!(last_req.prepared_query, b"token_v2");
}

#[tokio::test]
async fn test_client_raw_query_reactive_retry() {
    use googleapis_tonic_google_bigtable_v2::google;
    let (mock_service, mut client) = start_mock_server().await;

    let mut params = std::collections::HashMap::new();
    params.insert(
        "param1".to_string(),
        google::bigtable::v2::Value {
            kind: Some(google::bigtable::v2::value::Kind::StringValue(
                "val1".to_string(),
            )),
            ..Default::default()
        },
    );

    let request = google::bigtable::v2::ExecuteQueryRequest {
        instance_name: client.instance_name().to_string(),
        app_profile_id: "default".to_string(),
        query: "SELECT * FROM table WHERE col = @param1".to_string(),
        params: params.clone(),
        ..Default::default()
    };

    let _stream = client.execute_query(request.clone()).await.unwrap();
    assert_eq!(
        mock_service
            .prepare_call_count
            .load(std::sync::atomic::Ordering::SeqCst),
        1
    );

    *mock_service.next_prepare_token.lock().unwrap() = "refreshed_token".to_string();
    mock_service
        .should_expire_on_execute
        .store(true, std::sync::atomic::Ordering::SeqCst);

    let _stream2 = client.execute_query(request).await.unwrap();

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
        3
    );

    let has_refreshed_token = client
        .statement_cache()
        .lookup_by_token(b"refreshed_token")
        .is_some();
    assert!(has_refreshed_token);

    let has_old_token = client
        .statement_cache()
        .lookup_by_token(b"initial_token")
        .is_some();
    assert!(!has_old_token);
}

#[tokio::test]
async fn test_concurrency_singleflight_stampede() {
    use googleapis_tonic_google_bigtable_v2::google;
    let (mock_service, client) = start_mock_server().await;

    let mut params = std::collections::HashMap::new();
    params.insert(
        "param1".to_string(),
        google::bigtable::v2::Value {
            kind: Some(google::bigtable::v2::value::Kind::StringValue(
                "val1".to_string(),
            )),
            ..Default::default()
        },
    );

    let mut handles = vec![];
    for _ in 0..50 {
        let mut client_clone = client.clone();
        let params_clone = params.clone();
        handles.push(tokio::spawn(async move {
            let request = google::bigtable::v2::ExecuteQueryRequest {
                instance_name: client_clone.instance_name().to_string(),
                app_profile_id: "default".to_string(),
                query: "SELECT * FROM table WHERE col = @param1".to_string(),
                params: params_clone,
                ..Default::default()
            };
            client_clone.execute_query(request).await.unwrap()
        }));
    }

    for h in handles {
        let _stream = h.await.unwrap();
    }

    assert_eq!(
        mock_service
            .prepare_call_count
            .load(std::sync::atomic::Ordering::SeqCst),
        1
    );
}

#[tokio::test]
async fn test_ttl_safety_floor_override() {
    let (mock_service, mut client) = start_mock_server().await;

    mock_service
        .next_prepare_ttl_secs
        .store(2, std::sync::atomic::Ordering::SeqCst);

    let stmt = PreparedStatement::new("SELECT * FROM table".to_string(), "default".to_string());

    let start_time = Instant::now();

    let plan = stmt.get_or_prepare(&mut client).await.unwrap();

    let duration = plan.expires_at.duration_since(start_time);
    assert!(
        duration >= Duration::from_secs(10),
        "Expected expires_at to be at least 10s in the future, but got {:?}",
        duration
    );
}

#[tokio::test]
async fn test_token_pruning_leak_safety() {
    let _ = env_logger::builder().is_test(true).try_init();
    use googleapis_tonic_google_bigtable_v2::google;
    tokio::time::pause();

    let (mock_service, mut client) = start_mock_server().await;

    mock_service
        .next_prepare_ttl_secs
        .store(30, std::sync::atomic::Ordering::SeqCst);

    let mut params = std::collections::HashMap::new();
    params.insert(
        "param1".to_string(),
        google::bigtable::v2::Value {
            kind: Some(google::bigtable::v2::value::Kind::StringValue(
                "val1".to_string(),
            )),
            ..Default::default()
        },
    );

    let request = google::bigtable::v2::ExecuteQueryRequest {
        instance_name: client.instance_name().to_string(),
        app_profile_id: "default".to_string(),
        query: "SELECT * FROM table WHERE col = @param1".to_string(),
        params: params.clone(),
        ..Default::default()
    };

    let _stream = client.execute_query(request.clone()).await.unwrap();
    assert!(client
        .statement_cache()
        .lookup_by_token(b"initial_token")
        .is_some());

    tokio::time::advance(Duration::from_secs(10)).await;
    let _stream2 = client.execute_query(request.clone()).await.unwrap();

    tokio::task::yield_now().await;

    *mock_service.next_prepare_token.lock().unwrap() = "token_v2".to_string();

    tokio::time::advance(Duration::from_secs(14)).await;

    let mut success = false;
    for _ in 0..100 {
        tokio::task::yield_now().await;
        if client
            .statement_cache()
            .lookup_by_token(b"token_v2")
            .is_some()
        {
            success = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(success);

    // Advance virtual time by 11 seconds to exceed the 10s delayed grace period
    tokio::time::advance(Duration::from_secs(11)).await;
    tokio::task::yield_now().await;

    assert!(client
        .statement_cache()
        .lookup_by_token(b"initial_token")
        .is_none());
}

#[tokio::test]
async fn test_weak_reference_cleanup_and_lru() {
    let _ = env_logger::builder().is_test(true).try_init();
    use googleapis_tonic_google_bigtable_v2::google;
    tokio::time::pause();

    let (mock_service, mut client) = start_mock_server().await;

    mock_service
        .next_prepare_ttl_secs
        .store(30, std::sync::atomic::Ordering::SeqCst);

    let query = "SELECT * FROM table WHERE id = 1".to_string();
    let mut params = std::collections::HashMap::new();
    params.insert(
        "param1".to_string(),
        google::bigtable::v2::Value {
            kind: Some(google::bigtable::v2::value::Kind::StringValue(
                "val1".to_string(),
            )),
            ..Default::default()
        },
    );

    let request = google::bigtable::v2::ExecuteQueryRequest {
        instance_name: client.instance_name().to_string(),
        app_profile_id: "default".to_string(),
        query: query.clone(),
        params: params.clone(),
        ..Default::default()
    };

    let _stream = client.execute_query(request).await.unwrap();

    let weak_stmt = {
        let query_map = client.statement_cache().query_to_statement.read().unwrap();
        query_map.get(&query).cloned().unwrap()
    };

    assert!(weak_stmt.upgrade().is_some());

    {
        let mut lru = client.statement_cache().active_lru.lock().unwrap();
        lru.clear();
    }

    tokio::task::yield_now().await;

    tokio::time::advance(Duration::from_secs(24)).await;
    tokio::task::yield_now().await;

    assert!(
        weak_stmt.upgrade().is_none(),
        "Background task should have exited and dropped the statement"
    );

    for i in 0..1005 {
        let unique_query = format!("SELECT * FROM table WHERE id = {}", i);
        let stmt = Arc::new(bigtable_rs::bigtable::prepared_statement::PreparedStatementInner {
            query: unique_query.clone(),
            app_profile_id: "default".to_string(),
            param_types: std::collections::HashMap::new(),
            plan_state: arc_swap::ArcSwapOption::empty(),
            prepare_lock: tokio::sync::Mutex::new(()),
            base_instant: tokio::time::Instant::now(),
            last_executed_seconds: std::sync::atomic::AtomicU64::new(0),
            cache: std::sync::Arc::downgrade(&client.statement_cache_arc()),
        });
        client.statement_cache().get_or_insert_query(&unique_query, || stmt);
    }

    let lru_len = client.statement_cache().active_lru.lock().unwrap().len();
    assert_eq!(lru_len, 1000);
}

#[tokio::test]
async fn test_client_prepare_bind_execute_basic() {
    use googleapis_tonic_google_bigtable_v2::google;
    let (mock_service, mut client) = start_mock_server().await;

    let mut param_types = std::collections::HashMap::new();
    param_types.insert(
        "param1".to_string(),
        google::bigtable::v2::Type {
            kind: Some(google::bigtable::v2::r#type::Kind::StringType(
                Default::default(),
            )),
        },
    );

    let prepare_req = google::bigtable::v2::PrepareQueryRequest {
        instance_name: client.instance_name().to_string(),
        app_profile_id: "default".to_string(),
        query: "SELECT * FROM table WHERE col = @param1".to_string(),
        param_types,
        ..Default::default()
    };

    // 1. Call prepare_query and assert it returns b"initial_token"
    let prepare_resp = client.prepare_query(prepare_req).await.unwrap();
    assert_eq!(prepare_resp.prepared_query, b"initial_token");

    // 2. Verify lookup_by_query and lookup_by_token(b"initial_token") return Some(...)
    let has_query = client
        .statement_cache()
        .lookup_by_query("SELECT * FROM table WHERE col = @param1")
        .is_some();
    assert!(has_query);

    let has_token = client
        .statement_cache()
        .lookup_by_token(b"initial_token")
        .is_some();
    assert!(has_token);

    // 3. Construct ExecuteQueryRequest with prepared_query = b"initial_token" and params
    let mut params = std::collections::HashMap::new();
    params.insert(
        "param1".to_string(),
        google::bigtable::v2::Value {
            kind: Some(google::bigtable::v2::value::Kind::StringValue(
                "val1".to_string(),
            )),
            ..Default::default()
        },
    );

    let execute_req = google::bigtable::v2::ExecuteQueryRequest {
        instance_name: client.instance_name().to_string(),
        app_profile_id: "default".to_string(),
        prepared_query: prepare_resp.prepared_query.clone(),
        params,
        ..Default::default()
    };

    // 4. Call client.execute_query and assert success
    let _stream = client.execute_query(execute_req).await.unwrap();

    // 5. Assert mock server calls: prepare_count = 1, execute_count = 1
    assert_eq!(
        mock_service
            .prepare_call_count
            .load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    assert_eq!(
        mock_service
            .execute_call_count
            .load(std::sync::atomic::Ordering::SeqCst),
        1
    );
}

#[tokio::test]
async fn test_client_prepare_bind_execute_transparent_swap() {
    use googleapis_tonic_google_bigtable_v2::google;
    let _ = env_logger::builder().is_test(true).try_init();
    tokio::time::pause();

    let (mock_service, mut client) = start_mock_server().await;

    // 1. Pause virtual time. Call prepare_query to get b"initial_token" (TTL 30s)
    mock_service
        .next_prepare_ttl_secs
        .store(30, std::sync::atomic::Ordering::SeqCst);

    let mut param_types = std::collections::HashMap::new();
    param_types.insert(
        "param1".to_string(),
        google::bigtable::v2::Type {
            kind: Some(google::bigtable::v2::r#type::Kind::StringType(
                Default::default(),
            )),
        },
    );

    let prepare_req = google::bigtable::v2::PrepareQueryRequest {
        instance_name: client.instance_name().to_string(),
        app_profile_id: "default".to_string(),
        query: "SELECT * FROM table WHERE col = @param1".to_string(),
        param_types,
        ..Default::default()
    };

    let prepare_resp = client.prepare_query(prepare_req).await.unwrap();
    assert_eq!(prepare_resp.prepared_query, b"initial_token");

    // 2. Register b"token_v2" on the mock server
    *mock_service.next_prepare_token.lock().unwrap() = "token_v2".to_string();

    // Advance virtual time to T = 10s and execute again to mark the statement active
    tokio::time::advance(Duration::from_secs(10)).await;

    let mut params = std::collections::HashMap::new();
    params.insert(
        "param1".to_string(),
        google::bigtable::v2::Value {
            kind: Some(google::bigtable::v2::value::Kind::StringValue(
                "val1".to_string(),
            )),
            ..Default::default()
        },
    );

    let execute_req_init = google::bigtable::v2::ExecuteQueryRequest {
        instance_name: client.instance_name().to_string(),
        app_profile_id: "default".to_string(),
        prepared_query: b"initial_token".to_vec(),
        params: params.clone(),
        ..Default::default()
    };
    let _stream_init = client
        .execute_query(execute_req_init.clone())
        .await
        .unwrap();

    // Advance virtual time by another 10s to stay active, and execute again
    tokio::time::advance(Duration::from_secs(10)).await;
    let _stream_init2 = client.execute_query(execute_req_init).await.unwrap();

    // Yield to let background refresh task spawn
    tokio::task::yield_now().await;

    // 3. Advance virtual time by 4s (total 24s since prepare) to trigger proactive background rotation
    tokio::time::advance(Duration::from_secs(4)).await;

    // 4. Yield and assert cache mapping for b"token_v2" is registered successfully
    let mut success = false;
    for i in 0..100 {
        tokio::task::yield_now().await;
        let has_new_token = client
            .statement_cache()
            .lookup_by_token(b"token_v2")
            .is_some();
        log::info!(
            "Iteration {}: has_new_token={}",
            i,
            has_new_token
        );
        if has_new_token {
            success = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        success,
        "Failed to rotate token proactively"
    );

    // Clear logged requests before execution
    mock_service.executed_requests.lock().unwrap().clear();

    // 5. Construct ExecuteQueryRequest containing stale prepared_query = b"initial_token"
    let execute_req = google::bigtable::v2::ExecuteQueryRequest {
        instance_name: client.instance_name().to_string(),
        app_profile_id: "default".to_string(),
        prepared_query: b"initial_token".to_vec(),
        params,
        ..Default::default()
    };

    // 6. Call client.execute_query.
    // Since b"initial_token" lookup succeeds (grace period active), the client transparently swaps it with b"token_v2" inline!
    let _stream = client.execute_query(execute_req).await.unwrap();

    // 7. Assert that the request payload captured by the mock server actually contained b"token_v2" (Transparent Swap Verified!)
    let logged = mock_service.executed_requests.lock().unwrap();
    assert_eq!(logged.len(), 1);
    assert_eq!(logged[0].prepared_query, b"token_v2");
}

#[tokio::test]
async fn test_client_prepare_bind_execute_reactive_retry() {
    use googleapis_tonic_google_bigtable_v2::google;
    let (mock_service, mut client) = start_mock_server().await;

    let mut param_types = std::collections::HashMap::new();
    param_types.insert(
        "param1".to_string(),
        google::bigtable::v2::Type {
            kind: Some(google::bigtable::v2::r#type::Kind::StringType(
                Default::default(),
            )),
        },
    );

    // 1. Call prepare_query -> b"initial_token"
    let prepare_req = google::bigtable::v2::PrepareQueryRequest {
        instance_name: client.instance_name().to_string(),
        app_profile_id: "default".to_string(),
        query: "SELECT * FROM table WHERE col = @param1".to_string(),
        param_types,
        ..Default::default()
    };

    let prepare_resp = client.prepare_query(prepare_req).await.unwrap();
    assert_eq!(prepare_resp.prepared_query, b"initial_token");

    // 2. Configure mock server to return PREPARED_QUERY_EXPIRED on next execution, and b"refreshed_token" on next prepare.
    *mock_service.next_prepare_token.lock().unwrap() = "refreshed_token".to_string();
    mock_service
        .should_expire_on_execute
        .store(true, std::sync::atomic::Ordering::SeqCst);

    // 3. Execute query using b"initial_token"
    let mut params = std::collections::HashMap::new();
    params.insert(
        "param1".to_string(),
        google::bigtable::v2::Value {
            kind: Some(google::bigtable::v2::value::Kind::StringValue(
                "val1".to_string(),
            )),
            ..Default::default()
        },
    );

    let execute_req = google::bigtable::v2::ExecuteQueryRequest {
        instance_name: client.instance_name().to_string(),
        app_profile_id: "default".to_string(),
        prepared_query: b"initial_token".to_vec(),
        params,
        ..Default::default()
    };

    let _stream = client.execute_query(execute_req).await.unwrap();

    // 4. Assert execution succeeds, old token is evicted, and new token is cached
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

    let has_new_token = client
        .statement_cache()
        .lookup_by_token(b"refreshed_token")
        .is_some();
    let has_old_token = client
        .statement_cache()
        .lookup_by_token(b"initial_token")
        .is_some();

    assert!(has_new_token, "New refreshed_token must be cached");
    assert!(
        !has_old_token,
        "Old initial_token must be evicted/pruned from cache"
    );
}
