use crate::bigtable::sql::SqlType;
use crate::bigtable::{BigTable, Error, Result};
use arc_swap::ArcSwapOption;
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::{
    ExecuteQueryRequest, ExecuteQueryResponse, PrepareQueryRequest, Value,
};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::Instant;
use tonic::metadata::MetadataValue;
use tonic::IntoRequest;

/// Represents a compiled, cached query plan reference.
#[derive(Debug, Clone)]
pub struct CompiledPlanState {
    /// The opaque compiled prepared query plan token bytes returned by the server
    pub plan_token: Vec<u8>,
    /// Expiration deadline returned by the server (valid_until)
    pub expires_at: Instant,
}

/// Inner shared state of a prepared statement, wrapped in a single Arc.
struct PreparedStatementInner {
    /// The raw SQL query template (e.g., "SELECT * FROM users WHERE id = @id")
    query: String,
    /// The default application profile routing
    app_profile_id: String,
    /// Expected SQL type declarations of query parameters for server compilation
    param_types: std::collections::HashMap<String, SqlType>,
    /// Lock-free atomic storage for the compiled plan state reference
    plan_state: ArcSwapOption<CompiledPlanState>,
    /// Localized mutex to serialize PrepareQuery calls on cache misses/expirations
    prepare_lock: Mutex<()>,
    /// Fixed base instant to calculate relative elapsed time for lock-free atomics
    base_instant: Instant,
    /// Lock-free atomic elapsed seconds since base_instant when last executed
    last_executed_seconds: AtomicU64,
}

/// Exposes a prepared query statement handle. The cache is decentralized
/// and housed directly inside this instance, achieving lock-free, zero-contention reads!
#[derive(Clone)]
pub struct PreparedStatement {
    inner: Arc<PreparedStatementInner>,
}

impl PreparedStatement {
    /// Constructs a new PreparedStatement without parameterized types.
    pub fn new(query: String, app_profile_id: String) -> Self {
        Self::with_parameter_types(query, app_profile_id, std::collections::HashMap::new())
    }

    /// Constructs a new PreparedStatement with expected parameterized types.
    pub fn with_parameter_types(
        query: String,
        app_profile_id: String,
        param_types: std::collections::HashMap<String, SqlType>,
    ) -> Self {
        let base_instant = Instant::now();
        Self {
            inner: Arc::new(PreparedStatementInner {
                query,
                app_profile_id,
                param_types,
                plan_state: ArcSwapOption::empty(),
                prepare_lock: Mutex::new(()),
                base_instant,
                last_executed_seconds: AtomicU64::new(0),
            }),
        }
    }

    /// Access the lock-free plan state reference (used inside tests).
    #[doc(hidden)]
    pub fn plan_state(&self) -> &ArcSwapOption<CompiledPlanState> {
        &self.inner.plan_state
    }

    /// Updates the usage activity log timestamp to prevent cache pollution
    fn mark_used(&self) {
        if let Some(elapsed) = Instant::now().checked_duration_since(self.inner.base_instant) {
            self.inner
                .last_executed_seconds
                .store(elapsed.as_secs(), Ordering::Relaxed);
        }
    }

    /// Returns the active compiled plan token or executes the singleflight slow-path prepare.
    /// Ensures only one thread executes the gRPC PrepareQuery, protecting the server.
    pub async fn get_or_prepare(&self, client: &mut BigTable) -> Result<Arc<CompiledPlanState>> {
        // Mark used immediately upon entry to prepare logic to track active utilization
        self.mark_used();

        // 1. Fast Path: Lock-free atomic pointer load (Executes in single-digit nanoseconds)
        if let Some(plan) = self.inner.plan_state.load_full() {
            if Instant::now() < plan.expires_at {
                return Ok(plan);
            }
        }

        // 2. Slow Path: Acquire the local prepare lock to serialize compilation RPCs
        let _guard = self.inner.prepare_lock.lock().await;

        // Double-check cache state inside the lock
        if let Some(plan) = self.inner.plan_state.load_full() {
            if Instant::now() < plan.expires_at {
                return Ok(plan);
            }
        }

        // 3. Execute gRPC PrepareQuery
        let compiled_state = self.prepare_query_rpc(client).await?;

        // 4. Atomic Store: Instantly promote to active plan (Lock-free visible to all cores)
        self.inner.plan_state.store(Some(compiled_state.clone()));

        Ok(compiled_state)
    }

    /// Invokes the gRPC PrepareQuery RPC and schedules the proactive background refresh timer
    async fn prepare_query_rpc(&self, client: &mut BigTable) -> Result<Arc<CompiledPlanState>> {
        let instance_name = client.instance_prefix.to_string();

        let mut param_types = std::collections::HashMap::new();
        for (name, sql_type) in &self.inner.param_types {
            param_types.insert(name.clone(), sql_type.to_pb());
        }

        let prepare_request = PrepareQueryRequest {
            instance_name,
            app_profile_id: self.inner.app_profile_id.clone(),
            query: self.inner.query.clone(),
            param_types,
            data_format: None,
        };

        log::info!(
            "Compiling query plan via PrepareQuery: '{}'",
            self.inner.query
        );
        let mut tonic_req = prepare_request.into_request();
        tonic_req.metadata_mut().insert(
            "x-goog-request-params",
            MetadataValue::from_str(&format!(
                "name={}&app_profile_id={}",
                client.instance_prefix, self.inner.app_profile_id
            ))
            .map_err(Error::MetadataError)?,
        );

        let response = client
            .client
            .prepare_query(tonic_req)
            .await
            .map_err(Error::RpcError)?
            .into_inner();

        let ttl_duration = if let Some(valid_until) = response.valid_until {
            std::time::Duration::new(valid_until.seconds as u64, valid_until.nanos as u32)
        } else {
            std::time::Duration::from_secs(3600)
        };

        // Enforce a safety floor duration (minimum 10 seconds) to protect against rapid tight-loops
        let ttl_duration = std::cmp::max(ttl_duration, std::time::Duration::from_secs(10));

        let expires_at = Instant::now() + ttl_duration;
        let compiled_state = Arc::new(CompiledPlanState {
            plan_token: response.prepared_query,
            expires_at,
        });

        // Correct initialization: mark used immediately on successful compilation
        self.mark_used();

        // 5. Schedule the Proactive Background Refresh Task!
        log::info!("Scheduling proactive background refresh from prepare_query_rpc for '{}' (ttl={:?}, expires_at={:?})", self.inner.query, ttl_duration, expires_at);
        self.schedule_proactive_refresh(client.clone(), ttl_duration, expires_at);

        Ok(compiled_state)
    }

    /// Spawns a non-blocking background timer to refresh the plan token before it expires
    fn schedule_proactive_refresh(&self, mut client: BigTable, ttl: Duration, expires_at: Instant) {
        // Calculate the refresh offset (1 minute prior to expiration, scaled down for small TTLs)
        let offset = if ttl > Duration::from_secs(300) {
            Duration::from_secs(60)
        } else {
            ttl / 5 // Scale down to 20% of TTL for short-lived plans
        };

        let refresh_instant = expires_at - offset;

        // Downgrade the inner Arc to Weak to avoid capturing a strong cycle.
        // If the user drops all statements handles, the background task will exit instantly on next wakeup!
        let inner_weak = Arc::downgrade(&self.inner);

        tokio::spawn(async move {
            log::info!(
                "Spawned background task registered for '{}' (refresh_instant = {:?})",
                inner_weak
                    .upgrade()
                    .map(|i| i.query.clone())
                    .unwrap_or_default(),
                refresh_instant
            );
            // Sleep non-blockingly until the refresh threshold
            tokio::time::sleep_until(refresh_instant).await;
            log::info!(
                "Sleep finished! Background task waking up for '{}'",
                inner_weak
                    .upgrade()
                    .map(|i| i.query.clone())
                    .unwrap_or_default()
            );

            // Upgrade the Weak pointer to see if the handle is still alive
            let inner = match inner_weak.upgrade() {
                Some(inner) => inner,
                None => {
                    log::info!("PreparedStatement has been dropped. Stopping proactive background refresh.");
                    return;
                }
            };

            // Check usage activity to prevent cache pollution on idle statements.
            // Compare against saturating_sub(offset) to reflect wakeup occurring offset-seconds early.
            let now_seconds = Instant::now().duration_since(inner.base_instant).as_secs();
            let last_executed = inner.last_executed_seconds.load(Ordering::Relaxed);
            let idle_limit = ttl.saturating_sub(offset);

            log::info!(
                "Background task woke up for '{}'. now_seconds={}, last_executed={}, idle_limit={}",
                inner.query,
                now_seconds,
                last_executed,
                idle_limit.as_secs()
            );

            if now_seconds.saturating_sub(last_executed) > idle_limit.as_secs() {
                log::info!(
                    "PreparedStatement '{}' has been idle. Skipping proactive background refresh.",
                    inner.query
                );
                return;
            }

            // Acquire the localized lock to execute refresh
            let _guard = inner.prepare_lock.lock().await;

            // Double-check if another thread already ran an override/update
            if let Some(current_plan) = inner.plan_state.load_full() {
                if current_plan.expires_at > Instant::now() + offset {
                    return; // Already refreshed
                }
            }

            log::info!(
                "Proactive timer fired. Refreshing plan asynchronously for '{}'",
                inner.query
            );
            let instance_name = client.instance_prefix.to_string();

            let mut param_types = std::collections::HashMap::new();
            for (name, sql_type) in &inner.param_types {
                param_types.insert(name.clone(), sql_type.to_pb());
            }

            let prepare_request = PrepareQueryRequest {
                instance_name,
                app_profile_id: inner.app_profile_id.clone(),
                query: inner.query.clone(),
                param_types,
                data_format: None,
            };

            let mut tonic_req = prepare_request.into_request();
            if let Ok(metadata_val) = MetadataValue::from_str(&format!(
                "name={}&app_profile_id={}",
                client.instance_prefix, inner.app_profile_id
            )) {
                tonic_req
                    .metadata_mut()
                    .insert("x-goog-request-params", metadata_val);
            }

            match client.client.prepare_query(tonic_req).await {
                Ok(resp) => {
                    let resp = resp.into_inner();

                    // Asynchronous TTL Update: respect server TTL updates
                    let new_ttl = if let Some(valid_until) = resp.valid_until {
                        std::time::Duration::new(
                            valid_until.seconds as u64,
                            valid_until.nanos as u32,
                        )
                    } else {
                        ttl
                    };
                    let new_ttl = std::cmp::max(new_ttl, std::time::Duration::from_secs(10));

                    let new_expires_at = Instant::now() + new_ttl;
                    let new_plan = Arc::new(CompiledPlanState {
                        plan_token: resp.prepared_query,
                        expires_at: new_expires_at,
                    });

                    // Swap pointer atomically (instantly visible to serving threads)
                    inner.plan_state.store(Some(new_plan));
                    log::info!(
                        "Successfully rotated plan token asynchronously for '{}'",
                        inner.query
                    );

                    // RECURSIVE SCHEDULE: Continuous background loop execution
                    let stmt = PreparedStatement {
                        inner: inner.clone(),
                    };
                    stmt.schedule_proactive_refresh(client, new_ttl, new_expires_at);
                }
                Err(e) => {
                    log::error!(
                        "Failed to rotate plan token asynchronously for '{}': {:?}",
                        inner.query,
                        e
                    );
                }
            }
        });
    }

    /// Ergonomically binds dynamic parameter values and executes the prepared statement
    /// in a single, blazing-fast 1 RTT network call.
    pub async fn execute(
        &self,
        client: &mut BigTable,
        params: std::collections::HashMap<String, Value>,
    ) -> Result<tonic::codec::Streaming<ExecuteQueryResponse>> {
        // Update activity timestamp
        self.mark_used();

        let plan = self.get_or_prepare(client).await?;

        #[allow(deprecated)]
        let execute_request = ExecuteQueryRequest {
            instance_name: client.instance_prefix.to_string(),
            app_profile_id: self.inner.app_profile_id.clone(),
            prepared_query: plan.plan_token.clone(), // Swap query string with plan bytes
            query: "".to_string(),                   // MUST be empty for compiled plans
            params,
            data_format: None,
            ..Default::default()
        };

        client.execute_query(execute_request).await
    }

    /// Reactive retry: automatically traps PREPARED_QUERY_EXPIRED, evicts plan and retries
    pub async fn execute_with_retry(
        &self,
        client: &mut BigTable,
        params: std::collections::HashMap<String, Value>,
    ) -> Result<tonic::codec::Streaming<ExecuteQueryResponse>> {
        let mut retry_count = 0;
        loop {
            self.mark_used();
            let plan = self.get_or_prepare(client).await?;
            #[allow(deprecated)]
            let execute_request = ExecuteQueryRequest {
                instance_name: client.instance_prefix.to_string(),
                app_profile_id: self.inner.app_profile_id.clone(),
                prepared_query: plan.plan_token.clone(),
                query: "".to_string(),
                params: params.clone(),
                data_format: None,
                ..Default::default()
            };

            match client.execute_query(execute_request).await {
                Ok(stream) => return Ok(stream),
                Err(Error::RpcError(status))
                    if status.code() == tonic::Code::InvalidArgument
                        && status.message().contains("PREPARED_QUERY_EXPIRED") =>
                {
                    if retry_count >= 2 {
                        return Err(Error::RpcError(status));
                    }
                    log::warn!("Server-side plan expired (PREPARED_QUERY_EXPIRED). Evicting plan and retrying compile...");

                    // Lock-free Atomic CAS Eviction to guarantee we do not overwrite new valid concurrent plan swaps
                    let current = self.inner.plan_state.load();
                    if let Some(ref current_plan) = *current {
                        if current_plan.plan_token == plan.plan_token {
                            self.inner.plan_state.compare_and_swap(&current, None);
                        }
                    }

                    retry_count += 1;
                }
                Err(other) => return Err(other),
            }
        }
    }
}
