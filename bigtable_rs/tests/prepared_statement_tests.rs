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
    prepare_fail_count: Arc<std::sync::atomic::AtomicUsize>,
    prepare_error_status: Arc<std::sync::Mutex<Option<tonic::Status>>>,
    execute_always_expire: Arc<std::sync::atomic::AtomicBool>,
    prepare_delay: Arc<std::sync::Mutex<Option<Duration>>>,
    execute_delay: Arc<std::sync::Mutex<Option<Duration>>>,
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

                // Handle optional delay
                let delay = *self_clone.prepare_delay.lock().unwrap();
                if let Some(d) = delay {
                    tokio::time::sleep(d).await;
                }

                // Handle optional failure injection
                let fail_count = self_clone
                    .prepare_fail_count
                    .load(std::sync::atomic::Ordering::SeqCst);
                if fail_count > 0 {
                    self_clone
                        .prepare_fail_count
                        .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                    let status = self_clone
                        .prepare_error_status
                        .lock()
                        .unwrap()
                        .clone()
                        .unwrap_or_else(|| tonic::Status::unavailable("Service Unavailable"));
                    let response = http::Response::builder()
                        .status(200)
                        .header("content-type", "application/grpc")
                        .header("grpc-status", status.code().to_string())
                        .header("grpc-message", status.message().to_owned())
                        .body(tonic::body::Body::new(Empty::new()))
                        .unwrap();
                    return Ok(response);
                }

                let token = self_clone.next_prepare_token.lock().unwrap().clone();
                let ttl_secs = self_clone
                    .next_prepare_ttl_secs
                    .load(std::sync::atomic::Ordering::SeqCst);

                // valid_until must be an absolute Unix timestamp, not a relative duration.
                let expires =
                    std::time::SystemTime::now() + std::time::Duration::from_secs(ttl_secs);
                let since_epoch = expires
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default();

                let resp_msg = PrepareQueryResponse {
                    prepared_query: token.into_bytes(),
                    valid_until: Some(prost_types::Timestamp {
                        seconds: since_epoch.as_secs() as i64,
                        nanos: since_epoch.subsec_nanos() as i32,
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

                // Handle optional execute delay (simulates a slow network response)
                let delay = *self_clone.execute_delay.lock().unwrap();
                if let Some(d) = delay {
                    tokio::time::sleep(d).await;
                }

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

                let always_expire = self_clone
                    .execute_always_expire
                    .load(std::sync::atomic::Ordering::SeqCst);
                let should_expire = self_clone
                    .should_expire_on_execute
                    .load(std::sync::atomic::Ordering::SeqCst);
                if always_expire || should_expire {
                    if !always_expire {
                        self_clone
                            .should_expire_on_execute
                            .store(false, std::sync::atomic::Ordering::SeqCst);
                    }

                    let response = http::Response::builder()
                        .status(200)
                        .header("content-type", "application/grpc")
                        // Return FailedPrecondition (9) in tests to verify FailedPrecondition recovery fix!
                        .header("grpc-status", "9")
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
        prepare_fail_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        prepare_error_status: Arc::new(std::sync::Mutex::new(None)),
        execute_always_expire: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        prepare_delay: Arc::new(std::sync::Mutex::new(None)),
        execute_delay: Arc::new(std::sync::Mutex::new(None)),
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

    let (mock_service, mut client) = start_mock_server().await;
    tokio::time::pause();
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

    let plan_v1 = stmt.plan_state().load_full().unwrap();
    assert_eq!(plan_v1.plan_token, b"initial_token");
    assert_eq!(
        mock_service
            .prepare_call_count
            .load(std::sync::atomic::Ordering::SeqCst),
        1
    );

    // Compute the actual refresh threshold from the compiled plan's expires_at.
    // Virtual time auto-advances during gRPC I/O, so compile time may be much later
    // than the paused clock-zero. Advance to just past the actual refresh threshold.
    let ttl_secs = 30u64;
    let offset = Duration::from_secs(ttl_secs / 5);
    let expires_at = plan_v1.expires_at;
    let now = tokio::time::Instant::now();
    let time_to_refresh = (expires_at - offset)
        .checked_duration_since(now)
        .unwrap_or_default();

    // Set the next token before advancing so the background task always gets token_v2.
    *mock_service.next_prepare_token.lock().unwrap() = "token_v2".to_string();

    let pre_refresh_advance = time_to_refresh.saturating_sub(Duration::from_secs(2));
    tokio::time::advance(pre_refresh_advance).await;
    let _stream2 = stmt
        .execute_with_retry(&mut client, std::collections::HashMap::new())
        .await
        .unwrap();
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(3)).await;

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
}

#[tokio::test]
async fn test_prepared_statement_idle_pollution_guard() {
    let (mock_service, mut client) = start_mock_server().await;
    tokio::time::pause();
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

    let (mock_service, mut client) = start_mock_server().await;
    tokio::time::pause();

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

    // Compute the actual refresh threshold from the compiled plan's expires_at.
    // Virtual time auto-advances during gRPC I/O, so the actual compile time may be
    // much later than clock-zero. We must advance to just past the proactive refresh
    // threshold (expires_at - offset) rather than using a fixed advance.
    let inner = client
        .statement_cache()
        .lookup_by_query("SELECT * FROM table WHERE col = @param1", "default")
        .expect("statement should be cached");
    let expires_at = inner
        .plan_state
        .load_full()
        .expect("plan should be set")
        .expires_at;
    let ttl_secs = 30u64;
    let offset = Duration::from_secs(ttl_secs / 5); // = 6s
    let now = tokio::time::Instant::now();
    let time_to_refresh = (expires_at - offset)
        .checked_duration_since(now)
        .unwrap_or_default();

    // Set the next token before advancing so the background task always gets token_v2
    // regardless of when it fires (which is non-deterministic due to gRPC auto-advance).
    *mock_service.next_prepare_token.lock().unwrap() = "token_v2".to_string();

    // Execute a second time just before the refresh threshold to mark the statement active,
    // then advance past it to trigger the proactive background refresh.
    let pre_refresh_advance = time_to_refresh.saturating_sub(Duration::from_secs(2));
    tokio::time::advance(pre_refresh_advance).await;
    let _stream2 = client.execute_query(request.clone()).await.unwrap();
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(3)).await;

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

    let (mock_service, mut client) = start_mock_server().await;
    tokio::time::pause();

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

    // Compute the actual refresh threshold from the compiled plan's expires_at.
    let inner = client
        .statement_cache()
        .lookup_by_query("SELECT * FROM table WHERE col = @param1", "default")
        .expect("statement should be cached");
    let expires_at = inner
        .plan_state
        .load_full()
        .expect("plan should be set")
        .expires_at;
    let ttl_secs = 30u64;
    let offset = Duration::from_secs(ttl_secs / 5);
    let now = tokio::time::Instant::now();
    let time_to_refresh = (expires_at - offset)
        .checked_duration_since(now)
        .unwrap_or_default();

    // Set the next token before advancing so the background task always gets token_v2.
    *mock_service.next_prepare_token.lock().unwrap() = "token_v2".to_string();

    let pre_refresh_advance = time_to_refresh.saturating_sub(Duration::from_secs(2));
    tokio::time::advance(pre_refresh_advance).await;
    let _stream2 = client.execute_query(request.clone()).await.unwrap();
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(3)).await;

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

    let (mock_service, mut client) = start_mock_server().await;
    tokio::time::pause();

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

    // Compute the actual refresh threshold from the compiled plan's expires_at.
    let inner = client
        .statement_cache()
        .lookup_by_query("SELECT * FROM table WHERE col = @param1", "default")
        .expect("statement should be cached");
    let expires_at = inner
        .plan_state
        .load_full()
        .expect("plan should be set")
        .expires_at;
    let ttl_secs = 30u64;
    let offset = Duration::from_secs(ttl_secs / 5);
    let now = tokio::time::Instant::now();
    let time_to_refresh = (expires_at - offset)
        .checked_duration_since(now)
        .unwrap_or_default();

    // Set the next token before advancing so the background task always gets token_v2.
    *mock_service.next_prepare_token.lock().unwrap() = "token_v2".to_string();

    let pre_refresh_advance = time_to_refresh.saturating_sub(Duration::from_secs(2));
    tokio::time::advance(pre_refresh_advance).await;
    let _stream2 = client.execute_query(request.clone()).await.unwrap();
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(3)).await;

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

    // Advance past the 10s grace period for old token cleanup
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

    let (mock_service, mut client) = start_mock_server().await;
    tokio::time::pause();

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
        query_map
            .get(&(query.clone(), "default".to_string()))
            .cloned()
            .unwrap()
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
        let stmt = Arc::new(
            bigtable_rs::bigtable::prepared_statement::PreparedStatementInner {
                query: unique_query.clone(),
                app_profile_id: "default".to_string(),
                param_types: std::collections::HashMap::new(),
                plan_state: arc_swap::ArcSwapOption::empty(),
                prepare_lock: tokio::sync::Mutex::new(()),
                base_instant: tokio::time::Instant::now(),
                last_executed_seconds: std::sync::atomic::AtomicU64::new(0),
                cache: {
                    let c = std::sync::OnceLock::new();
                    let _ = c.set(std::sync::Arc::downgrade(&client.statement_cache_arc()));
                    c
                },
            },
        );
        client
            .statement_cache()
            .get_or_insert_query(&unique_query, "default", || stmt);
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
        .lookup_by_query("SELECT * FROM table WHERE col = @param1", "default")
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

    let (mock_service, mut client) = start_mock_server().await;
    tokio::time::pause();

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

    // Compute the actual refresh threshold from the compiled plan's expires_at.
    let inner = client
        .statement_cache()
        .lookup_by_query("SELECT * FROM table WHERE col = @param1", "default")
        .expect("statement should be cached");
    let expires_at = inner
        .plan_state
        .load_full()
        .expect("plan should be set")
        .expires_at;
    let ttl_secs = 30u64;
    let offset = Duration::from_secs(ttl_secs / 5);
    let now = tokio::time::Instant::now();
    let time_to_refresh = (expires_at - offset)
        .checked_duration_since(now)
        .unwrap_or_default();

    // 2. Register b"token_v2" before advancing so the background task always picks it up.
    *mock_service.next_prepare_token.lock().unwrap() = "token_v2".to_string();

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

    // Execute twice just before the refresh threshold to keep the statement active
    let pre_refresh_advance = time_to_refresh.saturating_sub(Duration::from_secs(2));
    tokio::time::advance(pre_refresh_advance).await;
    let _stream_init = client
        .execute_query(execute_req_init.clone())
        .await
        .unwrap();
    tokio::time::advance(Duration::from_secs(1)).await;
    let _stream_init2 = client.execute_query(execute_req_init).await.unwrap();
    tokio::task::yield_now().await;

    // 3. Advance past the refresh threshold to trigger proactive background rotation
    tokio::time::advance(Duration::from_secs(2)).await;

    // 4. Yield and assert cache mapping for b"token_v2" is registered successfully
    let mut success = false;
    for i in 0..100 {
        tokio::task::yield_now().await;
        let has_new_token = client
            .statement_cache()
            .lookup_by_token(b"token_v2")
            .is_some();
        log::info!("Iteration {}: has_new_token={}", i, has_new_token);
        if has_new_token {
            success = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(success, "Failed to rotate token proactively");

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

/// Regression test for the Use Case A / Use Case B cache bypass bug.
///
/// Before the fix, PreparedStatement::execute() called prepare_query_rpc() directly,
/// bypassing the query_to_statement index. A subsequent execute_query() call with the
/// same raw SQL would get a cache miss and issue a second PrepareQuery RPC.
///
/// After the fix, get_or_prepare() registers self.inner in query_to_statement before
/// calling prepare_query_rpc(), so both paths share the same compiled plan.
#[tokio::test]
async fn test_use_case_a_populates_cache_for_use_case_b() {
    use googleapis_tonic_google_bigtable_v2::google;
    let (mock_service, mut client) = start_mock_server().await;

    let sql = "SELECT * FROM table WHERE id = @id".to_string();

    // Use Case A: caller holds an explicit PreparedStatement (created outside the cache).
    let stmt = PreparedStatement::new(sql.clone(), "default".to_string());
    let plan = stmt.get_or_prepare(&mut client).await.unwrap();
    assert_eq!(plan.plan_token, b"initial_token");
    assert_eq!(
        mock_service
            .prepare_call_count
            .load(std::sync::atomic::Ordering::SeqCst),
        1,
        "first prepare via PreparedStatement::execute"
    );

    // The inner should now be visible in the query_to_statement index.
    assert!(
        client
            .statement_cache()
            .lookup_by_query(&sql, "default")
            .is_some(),
        "Use Case A inner must be registered in query_to_statement after get_or_prepare"
    );

    // Use Case B: execute_query with the same raw SQL — must reuse the cached plan.
    let mut params = std::collections::HashMap::new();
    params.insert(
        "id".to_string(),
        google::bigtable::v2::Value {
            kind: Some(google::bigtable::v2::value::Kind::IntValue(42)),
            ..Default::default()
        },
    );
    let request = google::bigtable::v2::ExecuteQueryRequest {
        instance_name: client.instance_name().to_string(),
        app_profile_id: "default".to_string(),
        query: sql.clone(),
        params,
        ..Default::default()
    };
    let _stream = client.execute_query(request).await.unwrap();

    // If the bug were present, prepare_count would be 2 here.
    assert_eq!(
        mock_service
            .prepare_call_count
            .load(std::sync::atomic::Ordering::SeqCst),
        1,
        "execute_query with same SQL must reuse the cached plan — no second PrepareQuery"
    );
}

/// Verifies that setting execute_query_with_prepare_disabled suppresses the automatic
/// PrepareQuery call even when raw SQL and params are provided. This flag lets callers
/// opt out of transparent preparation, for example when they know the server can handle
/// the raw SQL directly or when they want to manage preparation themselves.
#[tokio::test]
async fn test_execute_query_with_prepare_disabled_skips_prepare() {
    use googleapis_tonic_google_bigtable_v2::google;
    let (mock_service, mut client) = start_mock_server().await;

    client.set_execute_query_with_prepare_disabled(true);

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
        params,
        ..Default::default()
    };

    let _stream = client.execute_query(request).await.unwrap();

    assert_eq!(
        mock_service
            .prepare_call_count
            .load(std::sync::atomic::Ordering::SeqCst),
        0,
        "PrepareQuery must not be called when auto-prepare is disabled"
    );
    assert_eq!(
        mock_service
            .execute_call_count
            .load(std::sync::atomic::Ordering::SeqCst),
        1
    );
}

/// Verifies that PreparedStatement::execute() (the non-retry variant) compiles the plan
/// on the first call and reuses the cached plan on subsequent calls, and that the returned
/// stream is usable. This is distinct from execute_with_retry, which additionally handles
/// PREPARED_QUERY_EXPIRED by recompiling and retrying automatically.
#[tokio::test]
async fn test_prepared_statement_execute_non_retry() {
    let (mock_service, mut client) = start_mock_server().await;

    let stmt = PreparedStatement::new("SELECT * FROM table".to_string(), "default".to_string());

    // First execute: plan is not yet compiled, so PrepareQuery is called.
    let _stream = stmt
        .execute(&mut client, std::collections::HashMap::new())
        .await
        .unwrap();

    assert_eq!(
        mock_service
            .prepare_call_count
            .load(std::sync::atomic::Ordering::SeqCst),
        1,
        "first execute should compile the plan via PrepareQuery"
    );
    assert_eq!(
        mock_service
            .execute_call_count
            .load(std::sync::atomic::Ordering::SeqCst),
        1
    );

    // Second execute: the compiled plan is cached in plan_state, so PrepareQuery is
    // not called again.
    let _stream2 = stmt
        .execute(&mut client, std::collections::HashMap::new())
        .await
        .unwrap();

    assert_eq!(
        mock_service
            .prepare_call_count
            .load(std::sync::atomic::Ordering::SeqCst),
        1,
        "second execute must reuse the cached plan — PrepareQuery must not be called again"
    );
    assert_eq!(
        mock_service
            .execute_call_count
            .load(std::sync::atomic::Ordering::SeqCst),
        2
    );
}

/// Verifies the error path in execute_query when the server returns PREPARED_QUERY_EXPIRED
/// for a token that this client has no record of. This happens when a plan token was
/// produced by a different client instance and the cache on this client is empty.
/// The client cannot recompile from scratch because it has no SQL string or param types
/// associated with the token, so it must surface the error to the caller.
#[tokio::test]
async fn test_execute_query_expired_token_unknown_to_cache_returns_error() {
    let (mock_service, mut client) = start_mock_server().await;

    // Make the server return PREPARED_QUERY_EXPIRED on the first ExecuteQuery call.
    mock_service
        .should_expire_on_execute
        .store(true, std::sync::atomic::Ordering::SeqCst);

    // Submit a token that was never registered in this client's cache (it would come
    // from a different client instance in a real scenario).
    let request = googleapis_tonic_google_bigtable_v2::google::bigtable::v2::ExecuteQueryRequest {
        instance_name: client.instance_name().to_string(),
        app_profile_id: "default".to_string(),
        prepared_query: b"token-from-another-client".to_vec(),
        ..Default::default()
    };

    let result = client.execute_query(request).await;

    assert!(
        result.is_err(),
        "should return an error when the expired token is unknown to this client's cache"
    );
    // The client must not have attempted to recompile since it has no SQL to recompile from.
    assert_eq!(
        mock_service
            .prepare_call_count
            .load(std::sync::atomic::Ordering::SeqCst),
        0,
        "PrepareQuery must not be called — client has no SQL string to recompile from"
    );
    // Only one ExecuteQuery attempt: the client bails out immediately on expiry rather
    // than retrying, because there is nothing to retry with.
    assert_eq!(
        mock_service
            .execute_call_count
            .load(std::sync::atomic::Ordering::SeqCst),
        1
    );
}

/// Verifies that execute_query passes raw SQL through to the server unchanged when the
/// params map is empty. The auto-prepare coercion path requires at least one parameter
/// because it needs parameter values to infer the SQL type schema for PrepareQuery.
/// With no params there is nothing to infer, so the request goes straight to ExecuteQuery.
#[tokio::test]
async fn test_execute_query_raw_sql_without_params_skips_prepare() {
    use googleapis_tonic_google_bigtable_v2::google;
    let (mock_service, mut client) = start_mock_server().await;

    let request = google::bigtable::v2::ExecuteQueryRequest {
        instance_name: client.instance_name().to_string(),
        app_profile_id: "default".to_string(),
        query: "SELECT COUNT(*) FROM table".to_string(),
        params: std::collections::HashMap::new(), // no parameters
        ..Default::default()
    };

    let _stream = client.execute_query(request).await.unwrap();

    assert_eq!(
        mock_service
            .prepare_call_count
            .load(std::sync::atomic::Ordering::SeqCst),
        0,
        "PrepareQuery must not be called when the params map is empty"
    );
    assert_eq!(
        mock_service
            .execute_call_count
            .load(std::sync::atomic::Ordering::SeqCst),
        1
    );
}

/// Verifies that PreparedStatement::with_parameter_types() correctly stores the declared
/// SQL types and uses them when issuing the PrepareQuery RPC. This is the path callers
/// use when they want to declare types explicitly rather than relying on runtime inference,
/// for example to avoid the inference safeguard on floats, nulls, and complex types.
#[tokio::test]
async fn test_prepared_statement_with_parameter_types_compiles_and_executes() {
    use bigtable_rs::bigtable::sql::SqlType;
    let (mock_service, mut client) = start_mock_server().await;

    let mut param_types = std::collections::HashMap::new();
    param_types.insert("id".to_string(), SqlType::Int64);
    param_types.insert("label".to_string(), SqlType::String);
    param_types.insert("score".to_string(), SqlType::Float64);

    let stmt = PreparedStatement::with_parameter_types(
        "SELECT * FROM table WHERE id = @id".to_string(),
        "default".to_string(),
        param_types,
    );

    // Compilation should succeed and the declared param types should be included in
    // the PrepareQueryRequest sent to the server.
    let plan = stmt.get_or_prepare(&mut client).await.unwrap();
    assert_eq!(plan.plan_token, b"initial_token");
    assert_eq!(
        mock_service
            .prepare_call_count
            .load(std::sync::atomic::Ordering::SeqCst),
        1,
        "PrepareQuery should be called exactly once"
    );

    // Second call reuses the cached plan without another RPC.
    let plan2 = stmt.get_or_prepare(&mut client).await.unwrap();
    assert_eq!(plan2.plan_token, b"initial_token");
    assert_eq!(
        mock_service
            .prepare_call_count
            .load(std::sync::atomic::Ordering::SeqCst),
        1,
        "second get_or_prepare must use the cached plan"
    );
}

/// Verifies that get_or_prepare recompiles when the client-side expires_at has passed.
/// This is distinct from server-side PREPARED_QUERY_EXPIRED: here the client itself
/// determines the plan is stale (based on the TTL it received from the server) and
/// proactively recompiles before sending the next request.
#[tokio::test]
async fn test_get_or_prepare_recompiles_when_client_side_expires_at_passes() {
    let (mock_service, mut client) = start_mock_server().await;

    let stmt = PreparedStatement::new("SELECT * FROM table".to_string(), "default".to_string());

    // First compile — plan is fresh.
    let plan_v1 = stmt.get_or_prepare(&mut client).await.unwrap();
    assert_eq!(plan_v1.plan_token, b"initial_token");
    assert_eq!(
        mock_service
            .prepare_call_count
            .load(std::sync::atomic::Ordering::SeqCst),
        1
    );

    // Simulate client-side expiry by replacing the stored plan with one whose
    // expires_at is already in the past. This is exactly what happens in production
    // after the server-provided TTL elapses and the proactive background refresh
    // has not yet fired (e.g. the statement was idle).
    let expired_plan = Arc::new(
        bigtable_rs::bigtable::prepared_statement::CompiledPlanState {
            plan_token: b"initial_token".to_vec(),
            expires_at: tokio::time::Instant::now()
                .checked_sub(std::time::Duration::from_secs(1))
                .expect("instant subtraction"),
        },
    );
    stmt.plan_state().store(Some(expired_plan));

    // Point the mock at a new token so we can confirm a fresh compile occurred.
    *mock_service.next_prepare_token.lock().unwrap() = "recompiled_token".to_string();

    // get_or_prepare must detect the expired plan and issue a new PrepareQuery RPC.
    let plan_v2 = stmt.get_or_prepare(&mut client).await.unwrap();
    assert_eq!(plan_v2.plan_token, b"recompiled_token");
    assert_eq!(
        mock_service
            .prepare_call_count
            .load(std::sync::atomic::Ordering::SeqCst),
        2,
        "a new PrepareQuery RPC must be issued when the client-side plan has expired"
    );
}

/// Verifies that passing a FloatValue parameter without an explicit type annotation
/// causes execute_query to return a ParameterTypeInferenceFailed error. Float precision
/// (Float32 vs Float64) is ambiguous from the value alone, so the caller must annotate
/// with .with_type(SqlType::Float32) or .with_type(SqlType::Float64).
#[tokio::test]
async fn test_execute_query_float_param_without_explicit_type_returns_inference_error() {
    use bigtable_rs::bigtable::Error;
    let (_mock_service, mut client) = start_mock_server().await;

    let mut params = std::collections::HashMap::new();
    params.insert(
        "score".to_string(),
        googleapis_tonic_google_bigtable_v2::google::bigtable::v2::Value {
            kind: Some(
                googleapis_tonic_google_bigtable_v2::google::bigtable::v2::value::Kind::FloatValue(
                    1.5,
                ),
            ),
            r#type: None,
        },
    );

    let request = googleapis_tonic_google_bigtable_v2::google::bigtable::v2::ExecuteQueryRequest {
        instance_name: client.instance_name().to_string(),
        app_profile_id: "default".to_string(),
        query: "SELECT @score".to_string(),
        params,
        ..Default::default()
    };

    let result = client.execute_query(request).await;

    assert!(result.is_err(), "expected an error for untyped float param");
    assert!(
        matches!(
            result.unwrap_err(),
            Error::ParameterTypeInferenceFailed(ref field, ref msg)
                if field == "score" && msg.contains("float")
        ),
        "expected ParameterTypeInferenceFailed with param name 'score' and 'float' in the message"
    );
    assert_eq!(
        _mock_service
            .prepare_call_count
            .load(std::sync::atomic::Ordering::SeqCst),
        0,
        "no network call should be made when inference fails"
    );
}

/// Verifies that passing a null (kind: None) parameter without an explicit type annotation
/// causes execute_query to return a ParameterTypeInferenceFailed error. The SQL type of
/// null cannot be inferred, so the caller must annotate with .with_type().
#[tokio::test]
async fn test_execute_query_null_param_without_explicit_type_returns_inference_error() {
    use bigtable_rs::bigtable::Error;
    let (_mock_service, mut client) = start_mock_server().await;

    let mut params = std::collections::HashMap::new();
    params.insert(
        "optional_field".to_string(),
        googleapis_tonic_google_bigtable_v2::google::bigtable::v2::Value {
            kind: None, // null without an explicit type
            r#type: None,
        },
    );

    let request = googleapis_tonic_google_bigtable_v2::google::bigtable::v2::ExecuteQueryRequest {
        instance_name: client.instance_name().to_string(),
        app_profile_id: "default".to_string(),
        query: "SELECT @optional_field".to_string(),
        params,
        ..Default::default()
    };

    let result = client.execute_query(request).await;

    assert!(
        result.is_err(),
        "expected an error for null param without explicit type"
    );
    assert!(
        matches!(
            result.unwrap_err(),
            Error::ParameterTypeInferenceFailed(ref field, ref msg)
                if field == "optional_field" && msg.contains("Null/None")
        ),
        "expected ParameterTypeInferenceFailed with param name 'optional_field'"
    );
    assert_eq!(
        _mock_service
            .prepare_call_count
            .load(std::sync::atomic::Ordering::SeqCst),
        0,
        "no network call should be made when inference fails"
    );
}

/// Verifies that passing an ArrayValue parameter without an explicit type annotation without an explicit type annotation
/// causes execute_query to return a ParameterTypeInferenceFailed error before any
/// network call is made. Callers must use .with_type() to declare the element type
/// of arrays because the client cannot safely infer it from the value alone.
///
/// This is a non-integration-test port of test_safeguard_array_inference_rejection
/// from sql_parameters_tests.rs (which requires a real emulator).
#[tokio::test]
async fn test_execute_query_array_param_without_explicit_type_returns_inference_error() {
    use bigtable_rs::bigtable::Error;
    use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::{ArrayValue, Value};

    let (_mock_service, mut client) = start_mock_server().await;

    let mut params = std::collections::HashMap::new();
    params.insert(
        "arr".to_string(),
        Value {
            kind: Some(
                googleapis_tonic_google_bigtable_v2::google::bigtable::v2::value::Kind::ArrayValue(
                    ArrayValue::default(),
                ),
            ),
            r#type: None, // no explicit type — inference must fail
        },
    );

    let request = googleapis_tonic_google_bigtable_v2::google::bigtable::v2::ExecuteQueryRequest {
        instance_name: client.instance_name().to_string(),
        app_profile_id: "default".to_string(),
        query: "SELECT @arr".to_string(),
        params,
        ..Default::default()
    };

    let result = client.execute_query(request).await;

    assert!(result.is_err(), "expected an error for untyped array param");
    assert!(
        matches!(
            result.unwrap_err(),
            Error::ParameterTypeInferenceFailed(ref field, ref msg)
                if field == "arr" && msg.contains("ARRAY")
        ),
        "expected ParameterTypeInferenceFailed with param name 'arr' and ARRAY in the message"
    );
    // No network call should have been made — the error is caught before PrepareQuery.
    assert_eq!(
        _mock_service
            .prepare_call_count
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );
}

/// Verifies that attaching an explicit type via ValueExt::with_type() bypasses the
/// type inference guard entirely, allowing types that would otherwise be rejected
/// (floats, nulls, arrays) to pass through to PrepareQuery without error.
///
/// This is a non-integration-test port of test_explicit_type_bypass from
/// sql_parameters_tests.rs (which requires a real emulator).
#[tokio::test]
async fn test_execute_query_explicit_with_type_bypasses_inference_guard() {
    use bigtable_rs::bigtable::sql::{SqlType, ValueExt};
    use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::{ArrayValue, Value};

    let (mock_service, mut client) = start_mock_server().await;

    let mut params = std::collections::HashMap::new();

    // FloatValue without explicit type would be rejected by the inference guard.
    // With .with_type(Float64) the guard is skipped entirely.
    params.insert(
        "float_param".to_string(),
        Value {
            kind: Some(
                googleapis_tonic_google_bigtable_v2::google::bigtable::v2::value::Kind::FloatValue(
                    1.5,
                ),
            ),
            ..Default::default()
        }
        .with_type(SqlType::Float64),
    );

    // A null value (kind = None) without explicit type would be rejected.
    params.insert(
        "null_param".to_string(),
        Value {
            kind: None,
            ..Default::default()
        }
        .with_type(SqlType::String),
    );

    // An ArrayValue without explicit type would be rejected.
    params.insert(
        "array_param".to_string(),
        Value {
            kind: Some(
                googleapis_tonic_google_bigtable_v2::google::bigtable::v2::value::Kind::ArrayValue(
                    ArrayValue::default(),
                ),
            ),
            ..Default::default()
        }
        .with_type(SqlType::Array(Box::new(SqlType::Int64))),
    );

    let request = googleapis_tonic_google_bigtable_v2::google::bigtable::v2::ExecuteQueryRequest {
        instance_name: client.instance_name().to_string(),
        app_profile_id: "default".to_string(),
        query: "SELECT @float_param, @null_param, @array_param".to_string(),
        params,
        ..Default::default()
    };

    // The call must succeed (no inference error) and reach PrepareQuery + ExecuteQuery.
    let _stream = client.execute_query(request).await.unwrap();

    assert_eq!(
        mock_service
            .prepare_call_count
            .load(std::sync::atomic::Ordering::SeqCst),
        1,
        "PrepareQuery should be called — explicit types bypass the inference guard"
    );
    assert_eq!(
        mock_service
            .execute_call_count
            .load(std::sync::atomic::Ordering::SeqCst),
        1
    );
}

/// Regression test for the app_profile_id cache key bug.
///
/// Before the fix, query_to_statement was keyed only on the SQL string. Two callers using
/// the same SQL but different app_profile_id values would share the same cached plan, with
/// the second caller silently routing through the first caller's app profile. This is wrong
/// because app_profile_id controls how Bigtable routes the query to backend servers.
///
/// After the fix, the cache key is (SQL, app_profile_id), so each profile gets its own
/// independent compiled plan and its own PrepareQuery RPC.
#[tokio::test]
async fn test_prepare_query_different_app_profile_ids_get_independent_cache_entries() {
    use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::PrepareQueryRequest;

    let (mock_service, mut client) = start_mock_server().await;

    let sql = "SELECT * FROM table WHERE id = @id".to_string();

    // First prepare with profile_a — compiles and caches under (sql, "profile_a").
    let req_a = PrepareQueryRequest {
        instance_name: client.instance_name().to_string(),
        app_profile_id: "profile_a".to_string(),
        query: sql.clone(),
        ..Default::default()
    };
    *mock_service.next_prepare_token.lock().unwrap() = "token_a".to_string();
    let resp_a = client.prepare_query(req_a).await.unwrap();
    assert_eq!(resp_a.prepared_query, b"token_a");
    assert_eq!(
        mock_service
            .prepare_call_count
            .load(std::sync::atomic::Ordering::SeqCst),
        1
    );

    // Second prepare with profile_b and the same SQL — must issue a new PrepareQuery RPC,
    // not reuse the plan cached for profile_a.
    let req_b = PrepareQueryRequest {
        instance_name: client.instance_name().to_string(),
        app_profile_id: "profile_b".to_string(),
        query: sql.clone(),
        ..Default::default()
    };
    *mock_service.next_prepare_token.lock().unwrap() = "token_b".to_string();
    let resp_b = client.prepare_query(req_b).await.unwrap();
    assert_eq!(resp_b.prepared_query, b"token_b");
    assert_eq!(
        mock_service.prepare_call_count.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "different app_profile_id must produce an independent PrepareQuery RPC, not reuse the cached plan"
    );

    // Verify both entries exist independently in the cache.
    assert!(
        client
            .statement_cache()
            .lookup_by_query(&sql, "profile_a")
            .is_some(),
        "profile_a entry must remain in cache"
    );
    assert!(
        client
            .statement_cache()
            .lookup_by_query(&sql, "profile_b")
            .is_some(),
        "profile_b entry must be independently cached"
    );

    // A third call with profile_a must be a cache hit — no new RPC.
    let req_a2 = PrepareQueryRequest {
        instance_name: client.instance_name().to_string(),
        app_profile_id: "profile_a".to_string(),
        query: sql.clone(),
        ..Default::default()
    };
    let resp_a2 = client.prepare_query(req_a2).await.unwrap();
    assert_eq!(resp_a2.prepared_query, b"token_a");
    assert_eq!(
        mock_service
            .prepare_call_count
            .load(std::sync::atomic::Ordering::SeqCst),
        2,
        "third call with profile_a must be a cache hit"
    );
}

#[tokio::test]
async fn test_concurrent_stampede_different_app_profiles() {
    let (mock_service, client) = start_mock_server().await;
    let sql = "SELECT * FROM table WHERE id = @id".to_string();

    let mut params = std::collections::HashMap::new();
    params.insert(
        "id".to_string(),
        googleapis_tonic_google_bigtable_v2::google::bigtable::v2::Value {
            kind: Some(
                googleapis_tonic_google_bigtable_v2::google::bigtable::v2::value::Kind::StringValue(
                    "val".to_string(),
                ),
            ),
            ..Default::default()
        },
    );

    let mut handles = vec![];
    // Spawn 25 tasks for profile_a
    for _ in 0..25 {
        let mut client_clone = client.clone();
        let sql_clone = sql.clone();
        let params_clone = params.clone();
        handles.push(tokio::spawn(async move {
            let request =
                googleapis_tonic_google_bigtable_v2::google::bigtable::v2::ExecuteQueryRequest {
                    instance_name: client_clone.instance_name().to_string(),
                    app_profile_id: "profile_a".to_string(),
                    query: sql_clone,
                    params: params_clone,
                    ..Default::default()
                };
            client_clone.execute_query(request).await.unwrap()
        }));
    }
    // Spawn 25 tasks for profile_b
    for _ in 0..25 {
        let mut client_clone = client.clone();
        let sql_clone = sql.clone();
        let params_clone = params.clone();
        handles.push(tokio::spawn(async move {
            let request =
                googleapis_tonic_google_bigtable_v2::google::bigtable::v2::ExecuteQueryRequest {
                    instance_name: client_clone.instance_name().to_string(),
                    app_profile_id: "profile_b".to_string(),
                    query: sql_clone,
                    params: params_clone,
                    ..Default::default()
                };
            client_clone.execute_query(request).await.unwrap()
        }));
    }

    for h in handles {
        let _stream = h.await.unwrap();
    }

    // Assert exactly 2 PrepareQuery RPCs (one for each profile)
    assert_eq!(
        mock_service
            .prepare_call_count
            .load(std::sync::atomic::Ordering::SeqCst),
        2
    );
}

#[tokio::test]
async fn test_concurrent_stampede_on_expired_token_recovery() {
    let (mock_service, mut client) = start_mock_server().await;
    let stmt = Arc::new(PreparedStatement::new(
        "SELECT * FROM table".to_string(),
        "default".to_string(),
    ));

    // 1. First prepare to populate cache
    let plan = stmt.get_or_prepare(&mut client).await.unwrap();
    assert_eq!(plan.plan_token, b"initial_token");
    assert_eq!(
        mock_service
            .prepare_call_count
            .load(std::sync::atomic::Ordering::SeqCst),
        1
    );

    // 2. Force client-side plan to expire
    let expired_plan = Arc::new(
        bigtable_rs::bigtable::prepared_statement::CompiledPlanState {
            plan_token: b"initial_token".to_vec(),
            expires_at: tokio::time::Instant::now() - std::time::Duration::from_secs(1),
        },
    );
    stmt.plan_state().store(Some(expired_plan));

    // 3. Spawn 50 concurrent executions on the expired token
    *mock_service.next_prepare_token.lock().unwrap() = "recompiled_token".to_string();
    let mut handles = vec![];
    for _ in 0..50 {
        let stmt_clone = stmt.clone();
        let mut client_clone = client.clone();
        handles.push(tokio::spawn(async move {
            stmt_clone.get_or_prepare(&mut client_clone).await.unwrap()
        }));
    }

    for h in handles {
        let p = h.await.unwrap();
        assert_eq!(p.plan_token, b"recompiled_token");
    }

    // Exactly 1 initial + exactly 1 re-prepare = 2 prepares total!
    assert_eq!(
        mock_service
            .prepare_call_count
            .load(std::sync::atomic::Ordering::SeqCst),
        2
    );
}

#[tokio::test]
async fn test_persistent_expired_plan_bubbles_error() {
    let (mock_service, mut client) = start_mock_server().await;
    let stmt = PreparedStatement::new("SELECT * FROM table".to_string(), "default".to_string());

    // Configure mock server to persistently reject prepared queries
    mock_service
        .execute_always_expire
        .store(true, std::sync::atomic::Ordering::SeqCst);

    // Execute query — should retry up to limit and bubble error
    let res = stmt
        .execute_with_retry(&mut client, std::collections::HashMap::new())
        .await;
    assert!(res.is_err());

    // Initial prepare (1) + nested reactive retries = 9 prepares total!
    assert_eq!(
        mock_service
            .prepare_call_count
            .load(std::sync::atomic::Ordering::SeqCst),
        9
    );
    // 1 initial ExecuteQuery + retries = 9 total ExecuteQuery calls!
    assert_eq!(
        mock_service
            .execute_call_count
            .load(std::sync::atomic::Ordering::SeqCst),
        9
    );
}

#[tokio::test]
async fn test_transient_compilation_failure_recovery() {
    let (mock_service, mut client) = start_mock_server().await;
    let stmt = PreparedStatement::new("SELECT * FROM table".to_string(), "default".to_string());

    // Inject one-time PrepareQuery failure
    mock_service
        .prepare_fail_count
        .store(1, std::sync::atomic::Ordering::SeqCst);
    *mock_service.prepare_error_status.lock().unwrap() =
        Some(tonic::Status::unavailable("Transient failure"));

    // First call fails
    let res1 = stmt.get_or_prepare(&mut client).await;
    assert!(res1.is_err());

    // Next call succeeds as error is cleared
    let res2 = stmt.get_or_prepare(&mut client).await;
    assert!(res2.is_ok());
    assert_eq!(res2.unwrap().plan_token, b"initial_token");

    assert_eq!(
        mock_service
            .prepare_call_count
            .load(std::sync::atomic::Ordering::SeqCst),
        2
    );
}

#[tokio::test]
async fn test_proactive_refresh_slow_network_response() {
    let _ = env_logger::try_init();
    let (mock_service, mut client) = start_mock_server().await;
    tokio::time::pause();

    mock_service
        .next_prepare_ttl_secs
        .store(30, std::sync::atomic::Ordering::SeqCst);

    // First execution establishes the plan with no delay so the initial compile
    // does not cause unexpected virtual-clock auto-advance.
    let stmt = PreparedStatement::new("SELECT * FROM table".to_string(), "default".to_string());
    let _stream = stmt
        .execute_with_retry(&mut client, std::collections::HashMap::new())
        .await
        .unwrap();

    let plan_v1 = stmt.plan_state().load_full().unwrap();
    assert_eq!(plan_v1.plan_token, b"initial_token");
    assert_eq!(
        mock_service
            .prepare_call_count
            .load(std::sync::atomic::Ordering::SeqCst),
        1
    );

    // Inject the slow-network delay only now, so it applies to the background refresh
    // RPC but not the initial compile. Also arm token_v2 before advancing so the mock
    // returns it regardless of when its timer fires.
    *mock_service.prepare_delay.lock().unwrap() = Some(Duration::from_secs(10));
    *mock_service.next_prepare_token.lock().unwrap() = "token_v2".to_string();

    // Compute the actual refresh threshold from expires_at (robust to any
    // gRPC-induced virtual-clock auto-advance during the initial compile).
    let ttl_secs = 30u64;
    let offset = Duration::from_secs(ttl_secs / 5);
    let now = tokio::time::Instant::now();
    let time_to_refresh = (plan_v1.expires_at - offset)
        .checked_duration_since(now)
        .unwrap_or_default();

    // Advance to the refresh threshold so the background task wakes and starts its
    // (slow) PrepareQuery RPC. Execute once more to mark the statement active.
    let pre_refresh = time_to_refresh.saturating_sub(Duration::from_secs(2));
    tokio::time::advance(pre_refresh).await;

    let _stream2 = stmt
        .execute_with_retry(&mut client, std::collections::HashMap::new())
        .await
        .unwrap();

    tokio::time::advance(Duration::from_secs(3)).await; // cross the refresh threshold

    // Avoid Bug C (TCP Loopback Dispatch Race): deterministic polling loop for prepare_call_count to become 2
    let mut success = false;
    for _ in 0..100 {
        if mock_service
            .prepare_call_count
            .load(std::sync::atomic::Ordering::SeqCst)
            == 2
        {
            success = true;
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(
        success,
        "prepare_call_count did not reach 2 within the polling loop"
    );

    // Advance past the plan's client-side expiry so the concurrent get_or_prepare
    // takes the slow path and must wait on the prepare_lock held by the background task.
    tokio::time::advance(offset + Duration::from_secs(1)).await;

    // Spawn the concurrent query thread. It must block on the `prepare_lock` rather than making a second PrepareQuery RPC!
    let mut client_clone = client.clone();
    let stmt_clone = stmt.clone();
    let handle =
        tokio::spawn(async move { stmt_clone.get_or_prepare(&mut client_clone).await.unwrap() });

    // Avoid Bug D (Weak Concurrent Blocking Verification): yield 10 times to guarantee that the concurrent thread runs and blocks
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }

    // Before awaiting handle, explicitly advance virtual time by another 3 seconds (taking the clock to 45s)
    // to guarantee that the mock RPC's 10-second delay resolves, the proactive refresh releases the lock,
    // and the concurrent thread can acquire the lock and complete deterministically.
    tokio::time::advance(Duration::from_secs(3)).await;

    let plan = handle.await.unwrap();
    assert_eq!(plan.plan_token, b"token_v2");

    // Exactly 1 initial prepare + 1 proactive refresh = 2 total PrepareQuery RPCs.
    assert_eq!(
        mock_service
            .prepare_call_count
            .load(std::sync::atomic::Ordering::SeqCst),
        2
    );
}

#[tokio::test]
async fn test_proactive_refresh_pruning_after_reactive_retry() {
    let _ = env_logger::try_init();
    let (mock_service, mut client) = start_mock_server().await;
    tokio::time::pause();

    mock_service
        .next_prepare_ttl_secs
        .store(30, std::sync::atomic::Ordering::SeqCst);

    let stmt = PreparedStatement::new("SELECT * FROM table".to_string(), "default".to_string());
    let _stream = stmt
        .execute_with_retry(&mut client, std::collections::HashMap::new())
        .await
        .unwrap();

    // Token is initial_token. Trigger proactive task sleep.
    tokio::task::yield_now().await;

    // Compute actual refresh threshold from the compiled plan so the test is
    // robust to any H2-connection-setup auto-advance during the initial compile.
    let plan_v1 = stmt.plan_state().load_full().unwrap();
    let ttl_secs = 30u64;
    let offset = Duration::from_secs(ttl_secs / 5);
    let now = tokio::time::Instant::now();
    let time_to_refresh = (plan_v1.expires_at - offset)
        .checked_duration_since(now)
        .unwrap_or_default();

    // Trigger the reactive retry before the proactive refresh threshold.
    // Use a large TTL for the reactive token so it cannot expire due to further
    // auto-advance before the background task's double-check runs.
    let pre_refresh = time_to_refresh.saturating_sub(Duration::from_secs(5));
    tokio::time::advance(pre_refresh).await;
    *mock_service.next_prepare_token.lock().unwrap() = "reactive_token".to_string();
    mock_service
        .next_prepare_ttl_secs
        .store(3600, std::sync::atomic::Ordering::SeqCst);
    mock_service
        .should_expire_on_execute
        .store(true, std::sync::atomic::Ordering::SeqCst);

    // Execute query — reactive retry triggers and compiles reactive_token (TTL=3600s)
    let _stream2 = stmt
        .execute_with_retry(&mut client, std::collections::HashMap::new())
        .await
        .unwrap();
    let active_plan = stmt.plan_state().load_full().unwrap();
    assert_eq!(active_plan.plan_token, b"reactive_token");

    // Advance past the proactive refresh threshold so the original background task
    // wakes up. It must see reactive_token's far-future expires_at in the double-check
    // and exit cleanly without issuing any RPC.
    tokio::time::advance(Duration::from_secs(10)).await;

    // Yield multiple times to give the background task enough scheduling cycles to
    // run through its double-check path and return before we assert the count.
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }

    // Check prepare call count: 1 (initial) + 1 (reactive compile) = 2 total!
    // Proactive task did NOT dispatch an RPC.
    assert_eq!(
        mock_service
            .prepare_call_count
            .load(std::sync::atomic::Ordering::SeqCst),
        2
    );
}

/// Regression test for the post-RPC mark_used() fix in execute_query.
///
/// When an ExecuteQuery RPC is slow, the virtual clock can drift forward during the
/// await. If last_executed_seconds is only recorded before the RPC (pre-RPC mark), the
/// background refresh idle guard reads a timestamp that looks stale relative to the
/// drifted clock and incorrectly skips the refresh — even though the statement is
/// actively being used.
///
/// The fix records last_executed_seconds again in the Ok(resp) branch after the RPC
/// completes, correcting for any clock drift that accumulated during the await. This
/// test injects an execute_delay to simulate a slow network and verifies the background
/// refresh still fires correctly via execute_query (not PreparedStatement::execute).
#[tokio::test]
async fn test_execute_query_slow_response_does_not_cause_idle_guard_false_positive() {
    use googleapis_tonic_google_bigtable_v2::google;
    let _ = env_logger::builder().is_test(true).try_init();

    let (mock_service, mut client) = start_mock_server().await;
    tokio::time::pause();

    mock_service
        .next_prepare_ttl_secs
        .store(30, std::sync::atomic::Ordering::SeqCst);

    let mut params = std::collections::HashMap::new();
    params.insert(
        "id".to_string(),
        google::bigtable::v2::Value {
            kind: Some(google::bigtable::v2::value::Kind::IntValue(1)),
            ..Default::default()
        },
    );

    let request = google::bigtable::v2::ExecuteQueryRequest {
        instance_name: client.instance_name().to_string(),
        app_profile_id: "default".to_string(),
        query: "SELECT * FROM table WHERE id = @id".to_string(),
        params: params.clone(),
        ..Default::default()
    };

    // First execute: compiles and caches the plan. No delay yet.
    let _stream = client.execute_query(request.clone()).await.unwrap();
    assert_eq!(
        mock_service
            .prepare_call_count
            .load(std::sync::atomic::Ordering::SeqCst),
        1
    );

    // Compute the actual refresh threshold from expires_at.
    let inner = client
        .statement_cache()
        .lookup_by_query("SELECT * FROM table WHERE id = @id", "default")
        .expect("statement should be cached");
    let expires_at = inner.plan_state.load_full().unwrap().expires_at;
    let ttl_secs = 30u64;
    let offset = Duration::from_secs(ttl_secs / 5);
    let now = tokio::time::Instant::now();
    let time_to_refresh = (expires_at - offset)
        .checked_duration_since(now)
        .unwrap_or_default();

    // Set the next token and arm the execute delay BEFORE advancing time.
    // The delay simulates a slow network during the second execute, which causes the
    // virtual clock to drift forward during the await. Without the post-RPC mark_used()
    // fix, this drift would make the idle guard treat the statement as inactive.
    *mock_service.next_prepare_token.lock().unwrap() = "token_v2".to_string();
    *mock_service.execute_delay.lock().unwrap() = Some(Duration::from_secs(10));

    // Advance to near the refresh threshold, then execute with a slow response.
    // The execute_delay causes virtual-clock auto-advance during the RPC await.
    let pre_refresh = time_to_refresh.saturating_sub(Duration::from_secs(2));
    tokio::time::advance(pre_refresh).await;
    let _stream2 = client.execute_query(request.clone()).await.unwrap();
    tokio::task::yield_now().await;

    // Disable the execute delay and advance past the refresh threshold.
    *mock_service.execute_delay.lock().unwrap() = None;
    tokio::time::advance(Duration::from_secs(3)).await;

    // Poll for the proactive refresh to complete and register token_v2.
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

    assert!(
        success,
        "background refresh must fire after a slow execute — \
         last_executed_seconds must be updated post-RPC to correct for clock drift"
    );
    assert_eq!(
        mock_service
            .prepare_call_count
            .load(std::sync::atomic::Ordering::SeqCst),
        2,
        "exactly one proactive PrepareQuery RPC expected"
    );
}
