//! `bigtable` module provides a few convenient structs for calling Google Bigtable from Rust code.
//!
//!
//! Example usage:
//! ```rust,no_run
//! use bigtable_rs::bigtable;
//! use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::row_filter::{Chain, Filter};
//! use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::row_range::{EndKey, StartKey};
//! use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::{ReadRowsRequest, RowFilter, RowRange, RowSet};
//! use env_logger;
//! use std::error::Error;
//! use std::time::Duration;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn Error>> {
//!     env_logger::init();
//!
//!     let project_id = "project-1";
//!     let instance_name = "instance-1";
//!     let table_name = "table-1";
//!     let channel_size = 4;
//!     let timeout = Duration::from_secs(10);
//!
//!     let key_start: String = "key1".to_owned();
//!     let key_end: String = "key4".to_owned();
//!
//!     // make a bigtable client
//!     let connection = bigtable::BigTableConnection::new(
//!         project_id,
//!         instance_name,
//!         true,
//!         channel_size,
//!         Some(timeout),
//!     )
//!         .await?;
//!     let mut bigtable = connection.client();
//!
//!     // prepare a ReadRowsRequest
//!     let request = ReadRowsRequest {
//!         app_profile_id: "default".to_owned(),
//!         table_name: bigtable.get_full_table_name(table_name),
//!         rows_limit: 10,
//!         rows: Some(RowSet {
//!             row_keys: vec![], // use this field to put keys for reading specific rows
//!             row_ranges: vec![RowRange {
//!                 start_key: Some(StartKey::StartKeyClosed(key_start.into_bytes())),
//!                 end_key: Some(EndKey::EndKeyOpen(key_end.into_bytes())),
//!             }],
//!         }),
//!         filter: Some(RowFilter {
//!             filter: Some(Filter::Chain(Chain {
//!                 filters: vec![
//!                     RowFilter {
//!                         filter: Some(Filter::FamilyNameRegexFilter("cf1".to_owned())),
//!                     },
//!                     RowFilter {
//!                         filter: Some(Filter::ColumnQualifierRegexFilter("c1".as_bytes().to_vec())),
//!                     },
//!                     RowFilter {
//!                         filter: Some(Filter::CellsPerColumnLimitFilter(1)),
//!                     },
//!                 ],
//!             })),
//!         }),
//!         ..ReadRowsRequest::default()
//!     };
//!
//!     // calling bigtable API to get results
//!     let response = bigtable.read_rows(request).await?;
//!
//!     // simply print results for example usage
//!     response.into_iter().for_each(|(key, data)| {
//!         println!("------------\n{}", String::from_utf8(key.clone()).unwrap());
//!         data.into_iter().for_each(|row_cell| {
//!             println!(
//!                 "    [{}:{}] \"{}\" @ {}",
//!                 row_cell.family_name,
//!                 String::from_utf8(row_cell.qualifier).unwrap(),
//!                 String::from_utf8(row_cell.value).unwrap(),
//!                 row_cell.timestamp_micros
//!             )
//!         })
//!     });
//!
//!     Ok(())
//! }
//! ```

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use futures_util::Stream;
use gcp_auth::TokenProvider;
use log::info;
use thiserror::Error;
use tokio::net::UnixStream;
use tonic::metadata::MetadataValue;
use tonic::transport::Endpoint;
use tonic::IntoRequest;
use tonic::{
    codec::Streaming,
    transport::{channel::Change, Channel, ClientTlsConfig},
    Response,
};
use tower::ServiceBuilder;

use crate::auth_service::AuthSvc;
use crate::bigtable::read_rows::{decode_read_rows_response, decode_read_rows_response_stream};
use crate::{root_ca_certificate, util::get_row_range_from_prefix};
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::{
    bigtable_client::BigtableClient, MutateRowRequest, MutateRowResponse, MutateRowsRequest,
    MutateRowsResponse, ReadRowsRequest, RowSet, SampleRowKeysRequest, SampleRowKeysResponse,
};
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::{
    CheckAndMutateRowRequest, CheckAndMutateRowResponse, ExecuteQueryRequest, ExecuteQueryResponse,
    PrepareQueryRequest, PrepareQueryResponse,
};

pub mod prepared_statement;
pub mod read_rows;
pub mod sql;

/// An alias for Vec<u8> as row key
type RowKey = Vec<u8>;
/// A convenient Result type
type Result<T> = std::result::Result<T, Error>;

/// A data structure for returning the read content of a cell in a row.
#[derive(Debug)]
pub struct RowCell {
    pub family_name: String,
    pub qualifier: Vec<u8>,
    pub value: Vec<u8>,
    pub timestamp_micros: i64,
    pub labels: Vec<String>,
}

/// Error types the client may have
#[derive(Debug, Error)]
pub enum Error {
    #[error("AccessToken error: {0}")]
    AccessTokenError(String),

    #[error("Certificate error: {0}")]
    CertificateError(String),

    #[error("I/O Error: {0}")]
    IoError(std::io::Error),

    #[error("Transport error: {0}")]
    TransportError(tonic::transport::Error),

    #[error("Chunk error")]
    ChunkError(String),

    #[error("Row not found")]
    RowNotFound,

    #[error("Row write failed")]
    RowWriteFailed,

    #[error("Object not found: {0}")]
    ObjectNotFound(String),

    #[error("Object is corrupt: {0}")]
    ObjectCorrupt(String),

    #[error("RPC error: {0}")]
    RpcError(tonic::Status),

    #[error("Timeout error after {0} seconds")]
    TimeoutError(u64),

    #[error("GCPAuthError error: {0}")]
    GCPAuthError(#[from] gcp_auth::Error),

    #[error("Invalid metadata")]
    MetadataError(tonic::metadata::errors::InvalidMetadataValue),

    #[error("Parameter type inference failed for field '{0}': {1}")]
    ParameterTypeInferenceFailed(String, String),
}

impl std::convert::From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self::IoError(err)
    }
}

impl std::convert::From<tonic::transport::Error> for Error {
    fn from(err: tonic::transport::Error) -> Self {
        Self::TransportError(err)
    }
}

impl std::convert::From<tonic::Status> for Error {
    fn from(err: tonic::Status) -> Self {
        Self::RpcError(err)
    }
}

/// For initiate a Bigtable connection, then a `Bigtable` client can be made from it.
#[derive(Clone)]
pub struct BigTableConnection {
    client: BigtableClient<AuthSvc>,
    table_prefix: Arc<String>,
    instance_prefix: Arc<String>,
    timeout: Arc<Option<Duration>>,
    pub(crate) statement_cache: Arc<prepared_statement::ClientStatementCache>,
}

impl BigTableConnection {
    /// Establish a connection to the BigTable instance named `instance_name`.  If read-only access
    /// is required, the `read_only` flag should be used to reduce the requested OAuth2 scope.
    ///
    /// The GOOGLE_APPLICATION_CREDENTIALS environment variable will be used to determine the
    /// program name that contains the BigTable instance in addition to access credentials.
    ///
    /// The BIGTABLE_EMULATOR_HOST environment variable is also respected.
    ///
    /// `channel_size` defines the number of connections (or channels) established to Bigtable
    /// service, and the requests are load balanced onto all the channels.
    /// Consult the [Bigtable
    /// docs](https://docs.cloud.google.com/bigtable/docs/configure-connection-pools) for guidance
    /// on how to determine the optimal pool size for your application.
    /// As documented in [Cold starts and low
    /// QPS](https://docs.cloud.google.com/bigtable/docs/performance#cold-starts), you should
    /// configure the pool size in a way that ensures all channels receive a steady amount of load
    /// at all times. Failure to do so could result in latency spikes, as the server closes
    /// connections after a period of inactivity.
    /// Another approach to address this is to periodically send a low rate of artificial traffic
    /// to the table at all times, to ensure no connection becomes idle.
    /// If you are not sure what value to pick and your load is low, just start with 1.
    pub async fn new(
        project_id: &str,
        instance_name: &str,
        is_read_only: bool,
        channel_size: usize,
        timeout: Option<Duration>,
    ) -> Result<Self> {
        match std::env::var("BIGTABLE_EMULATOR_HOST") {
            Ok(endpoint) => Self::new_with_emulator(
                endpoint.as_str(),
                project_id,
                instance_name,
                is_read_only,
                channel_size,
                timeout,
            ),

            Err(_) => {
                let token_provider = gcp_auth::provider().await?;
                Self::new_with_token_provider(
                    project_id,
                    instance_name,
                    is_read_only,
                    channel_size,
                    timeout,
                    token_provider,
                )
            }
        }
    }
    /// Establish a connection to the BigTable instance named `instance_name`. If read-only access
    /// is required, the `read_only` flag should be used to reduce the requested OAuth2 scope.
    ///
    /// The `authentication_manager` variable will be used to determine the
    /// program name that contains the BigTable instance in addition to access credentials.
    ///
    /// `channel_size` defines the number of connections (or channels) established to Bigtable
    /// service, and the requests are load balanced onto all the channels.
    /// Consult the [Bigtable
    /// docs](https://docs.cloud.google.com/bigtable/docs/configure-connection-pools) for guidance
    /// on how to determine the optimal pool size for your application.
    /// As documented in [Cold starts and low
    /// QPS](https://docs.cloud.google.com/bigtable/docs/performance#cold-starts), you should
    /// configure the pool size in a way that ensures all channels receive a steady amount of load
    /// at all times. Failure to do so could result in latency spikes, as the server closes
    /// connections after a period of inactivity.
    /// Another approach to address this is to periodically send a low rate of artificial traffic
    /// to the table at all times, to ensure no connection becomes idle.
    /// If you are not sure what value to pick and your load is low, just start with 1.
    pub fn new_with_token_provider(
        project_id: &str,
        instance_name: &str,
        is_read_only: bool,
        channel_size: usize,
        timeout: Option<Duration>,
        token_provider: Arc<dyn TokenProvider>,
    ) -> Result<Self> {
        match std::env::var("BIGTABLE_EMULATOR_HOST") {
            Ok(endpoint) => Self::new_with_emulator(
                endpoint.as_str(),
                project_id,
                instance_name,
                is_read_only,
                channel_size,
                timeout,
            ),

            Err(_) => {
                let instance_prefix = format!("projects/{project_id}/instances/{instance_name}");
                let table_prefix = format!("{instance_prefix}/tables/");

                let channel_size = channel_size.max(1);
                let (channel, tx) = Channel::balance_channel(channel_size);
                for i in 0..channel_size {
                    let endpoint = Channel::from_static("https://bigtable.googleapis.com")
                        .tls_config(
                            ClientTlsConfig::new()
                                .ca_certificate(
                                    root_ca_certificate::load()
                                        .map_err(Error::CertificateError)
                                        .expect("root certificate error"),
                                )
                                .domain_name("bigtable.googleapis.com"),
                        )
                        .map_err(Error::TransportError)?
                        .http2_keep_alive_interval(Duration::from_secs(30))
                        .keep_alive_timeout(Duration::from_secs(10))
                        .keep_alive_while_idle(true);

                    let endpoint = if let Some(timeout) = timeout {
                        endpoint.timeout(timeout)
                    } else {
                        endpoint
                    };

                    // Use unique keys to ensure each channel has a dedicated HTTP connection
                    tx.try_send(Change::Insert(i, endpoint)).unwrap();
                }

                let token_provider = Some(token_provider);
                Ok(Self {
                    client: create_client(channel, token_provider, is_read_only),
                    table_prefix: Arc::new(table_prefix),
                    instance_prefix: Arc::new(instance_prefix),
                    timeout: Arc::new(timeout),
                    statement_cache: Arc::new(prepared_statement::ClientStatementCache::new()),
                })
            }
        }
    }

    /// Establish a connection to a BigTable emulator at [emulator_endpoint].
    /// This is usually covered by [Self::new] or [Self::new_with_auth_manager],
    /// which both support the `BIGTABLE_EMULATOR_HOST` env variable. However,
    /// this function can also be used directly, in case setting
    /// `BIGTABLE_EMULATOR_HOST` is inconvenient.
    pub fn new_with_emulator(
        emulator_endpoint: &str,
        project_id: &str,
        instance_name: &str,
        is_read_only: bool,
        channel_size: usize,
        timeout: Option<Duration>,
    ) -> Result<Self> {
        info!("Connecting to bigtable emulator at {}", emulator_endpoint);

        // configures the endpoint with the specified parameters
        fn configure_endpoint(endpoint: Endpoint, timeout: Option<Duration>) -> Endpoint {
            let endpoint = endpoint
                .http2_keep_alive_interval(Duration::from_secs(30))
                .keep_alive_timeout(Duration::from_secs(10))
                .keep_alive_while_idle(true);

            if let Some(timeout) = timeout {
                endpoint.timeout(timeout)
            } else {
                endpoint
            }
        }

        // Parse emulator_endpoint. Officially, it's only host:port,
        // but unix:///path/to/unix.sock also works in the Go SDK at least.
        // Having the emulator listen on unix domain sockets without ip2unix is
        // covered in https://github.com/googleapis/google-cloud-go/pull/9665.
        let channel = if let Some(path) = emulator_endpoint.strip_prefix("unix://") {
            // the URL doesn't matter, we use a custom connector.
            let endpoint = Endpoint::from_static("http://[::]:50051");
            let endpoint = configure_endpoint(endpoint, timeout);

            let path: String = path.to_string();
            let connector = tower::service_fn({
                move |_: tonic::transport::Uri| {
                    let path = path.clone();
                    async move {
                        let stream = UnixStream::connect(path).await?;
                        Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
                    }
                }
            });
            // TODO - somehow support channel_size for UDS here as well?
            endpoint.connect_with_connector_lazy(connector)
        } else {
            let channel_size = channel_size.max(1);
            let (channel, tx) = Channel::balance_channel(channel_size);
            for i in 0..channel_size {
                let endpoint = Channel::from_shared(format!("http://{}", emulator_endpoint))
                    .expect("invalid connection emulator uri");
                let endpoint = configure_endpoint(endpoint, timeout);

                // Use unique keys to ensure each channel has a dedicated HTTP connection
                tx.try_send(Change::Insert(i, endpoint)).unwrap();
            }
            channel
        };

        Ok(Self {
            client: create_client(channel, None, is_read_only),
            table_prefix: Arc::new(format!(
                "projects/{}/instances/{}/tables/",
                project_id, instance_name
            )),
            instance_prefix: Arc::new(format!(
                "projects/{}/instances/{}",
                project_id, instance_name
            )),
            timeout: Arc::new(timeout),
            statement_cache: Arc::new(prepared_statement::ClientStatementCache::new()),
        })
    }

    /// Create a new BigTable client by cloning needed properties.
    ///
    /// Clients require `&mut self`, due to `Tonic::transport::Channel` limitations, however
    /// the created new clients can be cheaply cloned and thus can be send to different threads
    pub fn client(&self) -> BigTable {
        let prepare_disabled = std::env::var("BIGTABLE_RUST_DISABLE_SQL_PREPARE_IN_EXECUTE")
            .unwrap_or_default()
            == "true";

        BigTable {
            client: self.client.clone(),
            instance_prefix: self.instance_prefix.clone(),
            table_prefix: self.table_prefix.clone(),
            timeout: self.timeout.clone(),
            execute_query_with_prepare_disabled: prepare_disabled,
            statement_cache: self.statement_cache.clone(),
        }
    }

    /// Provide a convenient method to update the inner `BigtableClient` so a newly configured client can be set
    pub fn configure_inner_client<F>(&mut self, config_fn: F)
    where
        F: FnOnce(BigtableClient<AuthSvc>) -> BigtableClient<AuthSvc>,
    {
        self.client = config_fn(self.client.clone());
    }
}

/// Helper function to create a BigtableClient<AuthSvc>
/// from a channel.
fn create_client(
    channel: Channel,
    token_provider: Option<Arc<dyn TokenProvider>>,
    read_only: bool,
) -> BigtableClient<AuthSvc> {
    let scopes = if read_only {
        "https://www.googleapis.com/auth/bigtable.data.readonly"
    } else {
        "https://www.googleapis.com/auth/bigtable.data"
    };

    let auth_svc = ServiceBuilder::new()
        .layer_fn(|c| AuthSvc::new(c, token_provider.clone(), scopes.to_string()))
        .service(channel);
    return BigtableClient::new(auth_svc);
}

/// The core struct for Bigtable client, which wraps a gPRC client defined by Bigtable proto.
/// In order to easily use this struct in multiple threads, we only store cloneable references here.
/// `BigtableClient<AuthSvc>` is a type alias of `BigtableClient` and it wraps a tonic Channel.
/// Cloning on `Bigtable` is cheap.
///
/// Bigtable can be created via `bigtable::BigTableConnection::new()` and cloned
/// ```rust,no_run
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///   use bigtable_rs::bigtable;
///   let connection = bigtable::BigTableConnection::new("p-id", "i-id", true, 1, None).await?;
///   let bt_client = connection.client();
///   // Cheap to clone clients and used in other places.
///   let bt_client2 = bt_client.clone();
///   Ok(())
/// }
/// ```
#[derive(Clone)]
pub struct BigTable {
    // clone is cheap with Channel, see https://docs.rs/tonic/latest/tonic/transport/struct.Channel.html
    pub(crate) client: BigtableClient<AuthSvc>,
    pub(crate) instance_prefix: Arc<String>,
    pub(crate) table_prefix: Arc<String>,
    pub(crate) timeout: Arc<Option<Duration>>,
    pub(crate) execute_query_with_prepare_disabled: bool,
    pub(crate) statement_cache: Arc<prepared_statement::ClientStatementCache>,
}

impl BigTable {
    /// Return the instance prefix name (projects/.../instances/...)
    pub fn instance_name(&self) -> &str {
        &self.instance_prefix
    }

    /// Returns a reference to the client-scoped statement cache registry
    pub fn statement_cache(&self) -> &prepared_statement::ClientStatementCache {
        &self.statement_cache
    }

    /// Returns a cloned Arc reference to the client-scoped statement cache registry (used inside tests)
    #[doc(hidden)]
    pub fn statement_cache_arc(&self) -> Arc<prepared_statement::ClientStatementCache> {
        self.statement_cache.clone()
    }

    /// Configure whether execute_query automatically prepares plans first.
    ///
    /// Defaults to false (automatic pre-compilation active by default).
    /// Pass true to disable and globally bypass preparation, forcing all direct SQL queries
    /// to execute legacy-style directly on the database gateway.
    pub fn set_execute_query_with_prepare_disabled(&mut self, disabled: bool) {
        self.execute_query_with_prepare_disabled = disabled;
    }

    /// Wrapped `check_and_mutate_row` method
    pub async fn check_and_mutate_row(
        &mut self,
        request: CheckAndMutateRowRequest,
    ) -> Result<CheckAndMutateRowResponse> {
        let response = self
            .client
            .check_and_mutate_row(request)
            .await?
            .into_inner();
        Ok(response)
    }

    /// Wrapped `read_rows` method
    pub async fn read_rows(
        &mut self,
        request: ReadRowsRequest,
    ) -> Result<Vec<(RowKey, Vec<RowCell>)>> {
        let response = self.client.read_rows(request).await?.into_inner();
        decode_read_rows_response(self.timeout.as_ref(), response).await
    }

    /// Provide `read_rows_with_prefix` method to allow using a prefix as key
    pub async fn read_rows_with_prefix(
        &mut self,
        mut request: ReadRowsRequest,
        prefix: Vec<u8>,
    ) -> Result<Vec<(RowKey, Vec<RowCell>)>> {
        let row_range = get_row_range_from_prefix(prefix);
        request.rows = Some(RowSet {
            row_keys: vec![], // use this field to put keys for reading specific rows
            row_ranges: vec![row_range],
        });
        let response = self.client.read_rows(request).await?.into_inner();
        decode_read_rows_response(self.timeout.as_ref(), response).await
    }

    /// Streaming support for `read_rows` method
    pub async fn stream_rows(
        &mut self,
        request: ReadRowsRequest,
    ) -> Result<impl Stream<Item = Result<(RowKey, Vec<RowCell>)>>> {
        let response = self.client.read_rows(request).await?.into_inner();
        let stream = decode_read_rows_response_stream(response).await;
        Ok(stream)
    }

    /// Streaming support for `read_rows_with_prefix` method
    pub async fn stream_rows_with_prefix(
        &mut self,
        mut request: ReadRowsRequest,
        prefix: Vec<u8>,
    ) -> Result<impl Stream<Item = Result<(RowKey, Vec<RowCell>)>>> {
        let row_range = get_row_range_from_prefix(prefix);
        request.rows = Some(RowSet {
            row_keys: vec![],
            row_ranges: vec![row_range],
        });
        let response = self.client.read_rows(request).await?.into_inner();
        let stream = decode_read_rows_response_stream(response).await;
        Ok(stream)
    }

    /// Wrapped `sample_row_keys` method
    pub async fn sample_row_keys(
        &mut self,
        request: SampleRowKeysRequest,
    ) -> Result<Streaming<SampleRowKeysResponse>> {
        let response = self.client.sample_row_keys(request).await?.into_inner();
        Ok(response)
    }

    /// Wrapped `mutate_row` method
    pub async fn mutate_row(
        &mut self,
        request: MutateRowRequest,
    ) -> Result<Response<MutateRowResponse>> {
        let response = self.client.mutate_row(request).await?;
        Ok(response)
    }

    /// Wrapped `mutate_rows` method
    pub async fn mutate_rows(
        &mut self,
        request: MutateRowsRequest,
    ) -> Result<Streaming<MutateRowsResponse>> {
        let response = self.client.mutate_rows(request).await?.into_inner();
        Ok(response)
    }

    /// Executes a SQL query against Bigtable, automatically handling query preparation
    /// and plan token caching on behalf of the caller.
    ///
    /// There are two ways to call this method:
    ///
    /// **Raw SQL** — set `request.query` to a SQL string and `request.params` to the
    /// parameter values. The library will call `prepare_query` internally on the first
    /// call for that SQL string and cache the resulting compiled plan token. Subsequent
    /// calls with the same SQL string skip the prepare step entirely and go straight to
    /// execution, saving a network round-trip.
    ///
    /// **Pre-compiled token** — set `request.prepared_query` to a token previously
    /// returned by `prepare_query`. The library will silently upgrade the token to a
    /// newer one if the background refresh task has already rotated it, so callers do
    /// not need to track token expiry themselves.
    #[allow(deprecated)]
    pub async fn execute_query(
        &mut self,
        mut request: ExecuteQueryRequest,
    ) -> Result<Streaming<ExecuteQueryResponse>> {
        let app_profile_id = request.app_profile_id.clone();

        // --- Raw SQL path ---
        // The caller provided a SQL string and parameter values rather than a
        // pre-compiled plan token. Before we can execute, Bigtable requires the query
        // to be compiled into an opaque token via the PrepareQuery RPC. We do that here
        // transparently so the caller never has to think about it.
        //
        // prepare_query checks the client-side cache first. If the same SQL string was
        // compiled in a previous call, the cached token is returned immediately with no
        // network round-trip. Only the very first call for a given SQL string hits the
        // wire. After prepare_query returns, we swap the compiled token into the request
        // and clear the raw SQL field so the rest of this method treats it identically
        // to a request that arrived with a token already set.
        if !self.execute_query_with_prepare_disabled
            && !request.query.is_empty()
            && !request.params.is_empty()
            && request.prepared_query.is_empty()
        {
            log::info!(
                "Coercing raw SQL query '{}' using PrepareQuery API with transparent caching",
                request.query
            );

            // Build the parameter type map that PrepareQuery needs. For each parameter
            // we first check whether the caller attached an explicit type annotation via
            // ValueExt::with_type(); if so, we use it directly. Otherwise we attempt to
            // infer the SQL type from the runtime value.
            //
            // Inference intentionally rejects ambiguous types (floats, nulls, arrays,
            // structs, maps) and returns an error asking the caller to annotate with
            // .with_type(). Sending the wrong type to the server would produce a
            // confusing server-side error, so we surface the problem here instead.
            let mut param_types = std::collections::HashMap::new();
            for (param_name, value) in &request.params {
                let pb_type = if let Some(val_type) = &value.r#type {
                    val_type.clone()
                } else {
                    infer_type_from_value(value).map_err(|e| {
                        // Attach the parameter name to the error so the caller knows
                        // exactly which parameter needs an explicit type annotation.
                        match e {
                            Error::ParameterTypeInferenceFailed(_, msg) => {
                                Error::ParameterTypeInferenceFailed(param_name.clone(), msg)
                            }
                            other => other,
                        }
                    })?
                };
                param_types.insert(param_name.clone(), pb_type);
            }

            let prepare_req = PrepareQueryRequest {
                instance_name: self.instance_prefix.to_string(),
                app_profile_id: app_profile_id.clone(),
                query: request.query.clone(),
                param_types,
                ..Default::default()
            };

            let prepare_resp = self.prepare_query(prepare_req).await?;

            // Replace the raw SQL with the compiled plan token. From this point on,
            // the request looks the same as one submitted with a token from the start.
            request.prepared_query = prepare_resp.prepared_query;
            request.query.clear();
            request.data_format = None;
        }

        // --- Token lookup and automatic upgrade ---
        // At this point the request always contains a compiled plan token (either
        // supplied by the caller or produced by the raw SQL path above). We look up
        // the token in the cache to do two things before sending the request to the server:
        //
        // 1. Record that this statement was used right now. The background refresh task
        //    uses this timestamp to decide whether a statement is still active. If a
        //    statement has not been executed for a long time, the background task will
        //    skip refreshing it to avoid wasting network calls on idle queries.
        //
        // 2. Check whether the background refresh task has already rotated to a newer
        //    token. If so, we silently upgrade the token in the request so the server
        //    receives the freshest available plan. The caller never needs to know that
        //    the token they passed is no longer the current one.
        let mut token_to_use = request.prepared_query.clone();

        let cached_stmt: Option<Arc<prepared_statement::PreparedStatementInner>> = if !token_to_use
            .is_empty()
            && request.query.is_empty()
        {
            if let Some(stmt) = self.statement_cache.lookup_by_token(&token_to_use) {
                // Record the current time as the last-used timestamp for this statement.
                if let Some(elapsed) =
                    tokio::time::Instant::now().checked_duration_since(stmt.base_instant)
                {
                    stmt.last_executed_seconds
                        .store(elapsed.as_secs(), std::sync::atomic::Ordering::Relaxed);
                }

                // If the background task has already refreshed the plan and stored a
                // newer token, upgrade transparently. The caller's token may be stale
                // but is still valid on the server for a short grace period, so this
                // upgrade is an optimization, not a requirement.
                if let Some(latest_plan) = stmt.plan_state.load_full() {
                    if latest_plan.plan_token != token_to_use {
                        log::info!("Transparently swapping stale prepared query token with proactively refreshed token");
                        token_to_use = latest_plan.plan_token.clone();
                    }
                }
                Some(stmt)
            } else {
                None
            }
        } else {
            None
        };

        // --- Execute with retry on server-side expiry ---
        // Send the ExecuteQuery RPC. On success, stream the results back to the caller.
        // The server may occasionally reject a plan token with PREPARED_QUERY_EXPIRED,
        // which means the server-side compiled plan has been invalidated (this is separate
        // from the client-side expiry tracked by expires_at — the server can expire a plan
        // early due to schema changes or rolling upgrades). When that happens we evict the
        // stale token, recompile, and retry automatically. We allow up to two retries
        // before giving up and returning the error to the caller.
        let mut retry_count = 0;
        loop {
            let mut tonic_req = request.clone().into_request();
            if !token_to_use.is_empty() {
                tonic_req.get_mut().prepared_query = token_to_use.clone();
            }

            tonic_req.metadata_mut().insert(
                "x-goog-request-params",
                MetadataValue::from_str(&format!(
                    "name={}&app_profile_id={}",
                    self.instance_prefix, app_profile_id
                ))
                .map_err(Error::MetadataError)?,
            );

            match self.client.execute_query(tonic_req).await {
                Ok(resp) => {
                    return Ok(resp.into_inner());
                }
                Err(status)
                    if (status.code() == tonic::Code::FailedPrecondition
                        || status.code() == tonic::Code::InvalidArgument)
                        && status.message().contains("PREPARED_QUERY_EXPIRED") =>
                {
                    if retry_count >= 2 {
                        return Err(Error::RpcError(status));
                    }
                    log::warn!("Server-side plan expired (PREPARED_QUERY_EXPIRED). Evicting plan and retrying compile...");

                    // Find the cached statement for this token. We already have it from
                    // the lookup above if the token was in the cache; otherwise search by
                    // the token bytes directly as a fallback.
                    let stmt = cached_stmt
                        .clone()
                        .or_else(|| self.statement_cache.lookup_by_token(&token_to_use));

                    // Remove the expired token from the cache so no other concurrent
                    // request picks it up and wastes a round-trip with the same bad token.
                    self.statement_cache.remove_token(&token_to_use);

                    if let Some(stmt) = stmt {
                        let current = stmt.plan_state.load();
                        if let Some(ref current_plan) = *current {
                            if current_plan.plan_token == token_to_use {
                                // The cached plan still points to the expired token. Clear it
                                // atomically so no other thread tries to use it, then recompile.
                                stmt.plan_state.compare_and_swap(&current, None);
                                let stmt_handle = prepared_statement::PreparedStatement {
                                    inner: stmt.clone(),
                                };
                                match stmt_handle.get_or_prepare(self).await {
                                    Ok(new_plan) => token_to_use = new_plan.plan_token.clone(),
                                    Err(e) => return Err(e),
                                }
                            } else {
                                // The background refresh task already compiled a fresh token
                                // while this request was in flight. Use that token for the retry
                                // instead of issuing another PrepareQuery RPC.
                                token_to_use = current_plan.plan_token.clone();
                            }
                        } else {
                            // The plan slot is empty, meaning another concurrent caller already
                            // evicted it. Call get_or_prepare which will recompile exactly once
                            // even if multiple threads reach this point simultaneously.
                            let stmt_handle = prepared_statement::PreparedStatement {
                                inner: stmt.clone(),
                            };
                            match stmt_handle.get_or_prepare(self).await {
                                Ok(new_plan) => token_to_use = new_plan.plan_token.clone(),
                                Err(e) => return Err(e),
                            }
                        }
                    } else {
                        // No cached statement found for this token. This can happen when
                        // a token was created outside this client instance and the cache
                        // has no record of it. Without a cached statement we cannot
                        // recompile, so surface the error to the caller.
                        return Err(Error::RpcError(status));
                    }

                    retry_count += 1;
                }
                Err(other) => return Err(Error::RpcError(other)),
            }
        }
    }

    /// Compiles a SQL query into a reusable plan token, caching the result so that
    /// repeated calls with the same SQL string do not re-compile.
    ///
    /// Bigtable's ExecuteQuery RPC requires a pre-compiled plan token rather than a
    /// raw SQL string. Compiling a query (PrepareQuery) is relatively expensive — it
    /// involves a network round-trip and server-side query planning. This method wraps
    /// that process with a client-side cache so the cost is paid only once per unique
    /// SQL string per client instance.
    ///
    /// The returned `PrepareQueryResponse` contains the compiled plan token and an
    /// expiry timestamp. Callers may pass the token directly to `execute_query`, or
    /// simply call `execute_query` with raw SQL and let it call this method automatically.
    ///
    /// # Cache key and param_types
    ///
    /// The cache is keyed on `(query, app_profile_id)`. The `param_types` field in the
    /// request is stored only on the **first** call for a given key — subsequent calls
    /// with the same SQL and app profile return the already-compiled plan regardless of
    /// what `param_types` they supply. If you need to compile the same SQL with
    /// different parameter types, use a distinct `app_profile_id` per variant, or call
    /// `execute_query` with raw SQL (which will infer types automatically on each call
    /// if the cache is cold, or reuse the cached plan if it is warm).
    pub async fn prepare_query(
        &mut self,
        request: PrepareQueryRequest,
    ) -> Result<PrepareQueryResponse> {
        let query = request.query.clone();
        let app_profile_id = request.app_profile_id.clone();

        // Look up the SQL string in the cache. If a compiled statement already exists
        // for this exact query, the cached entry is returned immediately. If not, a new
        // statement entry is created and inserted into the cache. The closure below is
        // the factory that builds the new entry — it only runs on a cache miss.
        //
        // Storing the entry now (before the network call) means that if two threads call
        // prepare_query for the same SQL at the same time, only one PrepareQuery RPC is
        // issued. The second thread finds the entry already in the cache and waits for
        // the first thread's compilation to finish via the per-statement prepare_lock
        // inside get_or_prepare.
        // Pre-validate and convert all parameter types from the request upfront.
        // Any unrecognized type or invalid nested schema triggers an immediate
        // ParameterTypeInferenceFailed error, rather than silently coercing to Bytes.
        let mut param_types = std::collections::HashMap::new();
        for (k, v) in &request.param_types {
            let sql_type = crate::bigtable::sql::SqlType::from_pb(v).ok_or_else(|| {
                Error::ParameterTypeInferenceFailed(
                    k.clone(),
                    "Failed to parse or validate parameter type schema".to_owned(),
                )
            })?;
            param_types.insert(k.clone(), sql_type);
        }

        let param_types_clone = param_types.clone();
        let statement = self
            .statement_cache
            .get_or_insert_query(&query, &app_profile_id, || {
                Arc::new(prepared_statement::PreparedStatementInner {
                    query: query.clone(),
                    app_profile_id: app_profile_id.clone(),
                    param_types: param_types_clone,
                    plan_state: arc_swap::ArcSwapOption::empty(), // no plan compiled yet
                    prepare_lock: tokio::sync::Mutex::new(()),    // serialises concurrent compiles
                    base_instant: tokio::time::Instant::now(), // reference point for idle tracking
                    last_executed_seconds: std::sync::atomic::AtomicU64::new(0),
                    cache: {
                        let c = std::sync::OnceLock::new();
                        let _ = c.set(Arc::downgrade(&self.statement_cache));
                        c
                    },
                })
            });

        // Compile the query if no valid plan is cached yet, or return the cached plan
        // if one exists and has not expired. This call is thread-safe: if multiple
        // callers reach here concurrently for the same statement, only one issues the
        // PrepareQuery RPC; the rest wait and then read the result from the shared
        // plan_state field.
        let stmt_handle = prepared_statement::PreparedStatement {
            inner: statement.clone(),
        };
        let plan = stmt_handle.get_or_prepare(self).await?;

        // Build the response. The plan token is the opaque bytes the server returned.
        // The valid_until timestamp tells callers when the token is expected to expire,
        // expressed as an absolute wall-clock time (Unix epoch seconds) so it can be
        // compared against the current time on any machine.
        let duration = plan
            .expires_at
            .checked_duration_since(tokio::time::Instant::now())
            .unwrap_or(std::time::Duration::from_secs(0));
        let expires_wall = std::time::SystemTime::now() + duration;
        let dur_since_epoch = expires_wall
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap_or(std::time::Duration::from_secs(0));

        Ok(PrepareQueryResponse {
            prepared_query: plan.plan_token.clone(),
            valid_until: Some(prost_types::Timestamp {
                seconds: dur_since_epoch.as_secs() as i64,
                nanos: dur_since_epoch.subsec_nanos() as i32,
            }),
            ..Default::default()
        })
    }

    /// Provide a convenient method to get the inner `BigtableClient` so user can use any methods
    /// defined from the Bigtable V2 gRPC API
    pub fn get_client(&mut self) -> &mut BigtableClient<AuthSvc> {
        &mut self.client
    }

    /// Provide a convenient method to update the inner `BigtableClient` config
    pub fn configure_inner_client<F>(&mut self, config_fn: F)
    where
        F: FnOnce(BigtableClient<AuthSvc>) -> BigtableClient<AuthSvc>,
    {
        self.client = config_fn(self.client.clone());
    }

    /// Provide a convenient method to get full table, which can be used for building requests
    pub fn get_full_table_name(&self, table_name: &str) -> String {
        [&self.table_prefix, table_name].concat()
    }
}

/// Internal helper to dynamically infer standard SQL Type schemas from protobuf Values.
/// This is used by the transparent SQL coercion pipeline.
pub(crate) fn infer_type_from_value(
    value: &googleapis_tonic_google_bigtable_v2::google::bigtable::v2::Value,
) -> std::result::Result<googleapis_tonic_google_bigtable_v2::google::bigtable::v2::Type, Error> {
    use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::r#type::Kind as TypeKind;
    use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::value::Kind;
    use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::Type;

    match &value.kind {
        // 1. Safeguard check: Block dynamic type mapping on Null/None value types:
        None => Err(Error::ParameterTypeInferenceFailed(
            "".to_owned(),
            "Cannot infer type of Null/None value. Please specify the explicit schema mapping manually using .with_type(...)".to_owned()
        )),

        // 2. Safeguard check: Block dynamic type mapping on floats (avoid precision ambiguity):
        Some(Kind::FloatValue(_)) => Err(Error::ParameterTypeInferenceFailed(
            "".to_owned(),
            "Cannot infer type of float. Please declare precision (either FLOAT32 or FLOAT64) manually using .with_type(...)".to_owned()
        )),

        // 3. Safeguard check: Block dynamic type mapping on Lists / Arrays / Structs:
        Some(Kind::ArrayValue(_)) => Err(Error::ParameterTypeInferenceFailed(
            "".to_owned(),
            "Cannot infer type of ARRAY/STRUCT/MAP parameters. Please declare schema manually using .with_type(...)".to_owned()
        )),

        // 4. Stable primitive scalar mapping pathways:
        Some(Kind::BytesValue(_)) | Some(Kind::RawValue(_)) => Ok(Type {
            kind: Some(TypeKind::BytesType(Default::default())),
        }),
        Some(Kind::StringValue(_)) => Ok(Type {
            kind: Some(TypeKind::StringType(Default::default())),
        }),
        Some(Kind::IntValue(_)) | Some(Kind::RawTimestampMicros(_)) => Ok(Type {
            kind: Some(TypeKind::Int64Type(Default::default())),
        }),
        Some(Kind::BoolValue(_)) => Ok(Type {
            kind: Some(TypeKind::BoolType(Default::default())),
        }),
        Some(Kind::TimestampValue(_)) => Ok(Type {
            kind: Some(TypeKind::TimestampType(Default::default())),
        }),
        Some(Kind::DateValue(_)) => Ok(Type {
            kind: Some(TypeKind::DateType(Default::default())),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::r#type::Kind as TypeKind;
    use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::value::Kind;
    use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::Value;

    #[test]
    fn test_infer_type_from_bytes() {
        let value = Value {
            kind: Some(Kind::BytesValue(vec![1, 2, 3])),
            ..Default::default()
        };
        let t = infer_type_from_value(&value).unwrap();
        assert!(matches!(t.kind, Some(TypeKind::BytesType(_))));
    }

    #[test]
    fn test_infer_type_from_raw_value() {
        let value = Value {
            kind: Some(Kind::RawValue(vec![1, 2, 3])),
            ..Default::default()
        };
        let t = infer_type_from_value(&value).unwrap();
        assert!(matches!(t.kind, Some(TypeKind::BytesType(_))));
    }

    #[test]
    fn test_infer_type_from_string() {
        let value = Value {
            kind: Some(Kind::StringValue("hello".to_string())),
            ..Default::default()
        };
        let t = infer_type_from_value(&value).unwrap();
        assert!(matches!(t.kind, Some(TypeKind::StringType(_))));
    }

    #[test]
    fn test_infer_type_from_int() {
        let value = Value {
            kind: Some(Kind::IntValue(42)),
            ..Default::default()
        };
        let t = infer_type_from_value(&value).unwrap();
        assert!(matches!(t.kind, Some(TypeKind::Int64Type(_))));
    }

    #[test]
    fn test_infer_type_from_bool() {
        let value = Value {
            kind: Some(Kind::BoolValue(true)),
            ..Default::default()
        };
        let t = infer_type_from_value(&value).unwrap();
        assert!(matches!(t.kind, Some(TypeKind::BoolType(_))));
    }

    #[test]
    fn test_infer_type_from_float_rejection() {
        let value = Value {
            kind: Some(Kind::FloatValue(123.45)),
            ..Default::default()
        };
        let t = infer_type_from_value(&value);
        assert!(t.is_err());
        assert!(matches!(
            t.unwrap_err(),
            Error::ParameterTypeInferenceFailed(_, detail) if detail.contains("float")
        ));
    }

    #[test]
    fn test_infer_type_unspecified_rejection() {
        let value = Value {
            kind: None,
            ..Default::default()
        };
        let t = infer_type_from_value(&value);
        assert!(t.is_err());
        assert!(matches!(
            t.unwrap_err(),
            Error::ParameterTypeInferenceFailed(_, detail) if detail.contains("Null/None")
        ));
    }

    #[test]
    fn test_infer_type_from_timestamp() {
        let value = Value {
            kind: Some(Kind::TimestampValue(prost_types::Timestamp {
                seconds: 1_700_000_000,
                nanos: 0,
            })),
            ..Default::default()
        };
        let t = infer_type_from_value(&value).unwrap();
        assert!(matches!(t.kind, Some(TypeKind::TimestampType(_))));
    }

    #[test]
    fn test_infer_type_from_date() {
        let value = Value {
            kind: Some(Kind::DateValue(
                googleapis_tonic_google_bigtable_v2::google::r#type::Date {
                    year: 2024,
                    month: 6,
                    day: 1,
                },
            )),
            ..Default::default()
        };
        let t = infer_type_from_value(&value).unwrap();
        assert!(matches!(t.kind, Some(TypeKind::DateType(_))));
    }

    #[test]
    fn test_infer_type_from_raw_timestamp_micros() {
        let value = Value {
            kind: Some(Kind::RawTimestampMicros(1_700_000_000_000_000)),
            ..Default::default()
        };
        let t = infer_type_from_value(&value).unwrap();
        assert!(matches!(t.kind, Some(TypeKind::Int64Type(_))));
    }

    #[test]
    fn test_infer_type_from_array_value_rejection() {
        use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::ArrayValue;
        let value = Value {
            kind: Some(Kind::ArrayValue(ArrayValue::default())),
            ..Default::default()
        };
        let t = infer_type_from_value(&value);
        assert!(t.is_err());
        assert!(matches!(
            t.unwrap_err(),
            Error::ParameterTypeInferenceFailed(_, detail) if detail.contains("ARRAY")
        ));
    }
}
