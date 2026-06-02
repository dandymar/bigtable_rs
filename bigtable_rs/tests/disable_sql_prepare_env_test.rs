#![allow(deprecated)]

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
async fn test_disable_sql_prepare_environment_variable() {
    let (mock_service, mut client) = start_mock_server().await;
    let query = "SELECT * FROM table WHERE val = @param".to_string();

    // Set the disable environment variable — isolated to this OS process!
    std::env::set_var("BIGTABLE_RUST_DISABLE_SQL_PREPARE_IN_EXECUTE", "true");

    // Create fresh client which loads this env var
    let connection = BigTableConnection::new_with_emulator(
        "127.0.0.1:50051", // dummy endpoint, won't connect
        "mock-project",
        "mock-instance",
        false,
        1,
        None,
    )
    .unwrap();
    let mut fresh_client = connection.client();
    // Override the inner client with the mock's client using public API configure_inner_client
    let inner_client = client.get_client().clone();
    fresh_client.configure_inner_client(move |_| inner_client.clone());

    let mut params = std::collections::HashMap::new();
    params.insert(
        "param".to_string(),
        googleapis_tonic_google_bigtable_v2::google::bigtable::v2::Value {
            kind: Some(
                googleapis_tonic_google_bigtable_v2::google::bigtable::v2::value::Kind::StringValue(
                    "val".to_string(),
                ),
            ),
            ..Default::default()
        },
    );

    let request = googleapis_tonic_google_bigtable_v2::google::bigtable::v2::ExecuteQueryRequest {
        instance_name: fresh_client.instance_name().to_string(),
        app_profile_id: "default".to_string(),
        query,
        params,
        ..Default::default()
    };

    let _stream = fresh_client.execute_query(request).await.unwrap();

    // Verify PrepareQuery was NOT called (prepare bypassed entirely!)
    assert_eq!(
        mock_service
            .prepare_call_count
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    assert_eq!(
        mock_service
            .execute_call_count
            .load(std::sync::atomic::Ordering::SeqCst),
        1
    );

    // Clean up env var
    std::env::remove_var("BIGTABLE_RUST_DISABLE_SQL_PREPARE_IN_EXECUTE");
}
