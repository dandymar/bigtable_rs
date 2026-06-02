use crate::bigtable::sql::SqlType;
use crate::bigtable::{BigTable, Error, Result};
use arc_swap::ArcSwapOption;
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::{
    ExecuteQueryRequest, ExecuteQueryResponse, PrepareQueryRequest, Value,
};
use std::collections::{HashMap, VecDeque};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, RwLock, Weak};
use std::time::Duration;
use tokio::sync::Mutex as AsyncMutex;
use tokio::time::Instant;
use tonic::metadata::MetadataValue;
use tonic::IntoRequest;

/// Converts a `google.protobuf.Timestamp` (absolute wall-clock expiry) to a TTL duration
/// relative to now. Falls back to `fallback` if the timestamp is absent or already in the past.
///
/// `valid_until.seconds` is a Unix epoch timestamp, NOT a duration. Treating it as a
/// `Duration::new(seconds, nanos)` would produce a multi-decade TTL on any real Bigtable server.
fn ttl_from_valid_until(
    valid_until: Option<prost_types::Timestamp>,
    fallback: Duration,
) -> Duration {
    if let Some(ts) = valid_until {
        let secs = ts.seconds.max(0) as u64;
        let nanos = ts.nanos.max(0) as u32;
        let expires_wall = std::time::UNIX_EPOCH + Duration::new(secs, nanos);
        let safety_ttl = std::time::Duration::from_secs(10);
        expires_wall
            .duration_since(std::time::SystemTime::now())
            .unwrap_or(safety_ttl)
    } else {
        fallback
    }
}

/// Represents a compiled, cached query plan reference.
#[derive(Debug, Clone)]
pub struct CompiledPlanState {
    /// The opaque compiled prepared query plan token bytes returned by the server
    pub plan_token: Vec<u8>,
    /// Expiration deadline returned by the server (valid_until)
    pub expires_at: Instant,
}

/// Thread-safe, client-scoped prepared statement cache.
pub struct ClientStatementCache {
    /// Maps (SQL query string, app_profile_id) pairs to their statement handles.
    /// The app_profile_id is included in the key because it affects query routing:
    /// the same SQL compiled for different app profiles produces independent plans.
    pub query_to_statement: RwLock<HashMap<(String, String), Weak<PreparedStatementInner>>>,
    /// Maps active plan token bytes to their statement handles
    pub token_to_statement: RwLock<HashMap<Vec<u8>, Weak<PreparedStatementInner>>>,
    /// Bounded LRU cache to hold strong references for raw SQL (Use Case B) statement handles
    pub active_lru: StdMutex<VecDeque<Arc<PreparedStatementInner>>>,
}

impl Default for ClientStatementCache {
    fn default() -> Self {
        Self::new()
    }
}

impl ClientStatementCache {
    /// Constructs a new ClientStatementCache.
    pub fn new() -> Self {
        Self {
            query_to_statement: RwLock::new(HashMap::new()),
            token_to_statement: RwLock::new(HashMap::new()),
            active_lru: StdMutex::new(VecDeque::new()),
        }
    }

    /// Inserts a query-to-statement mapping keyed on (query, app_profile_id).
    pub fn insert_query(&self, query: String, statement: Arc<PreparedStatementInner>) {
        let key = (query, statement.app_profile_id.clone());
        self.query_to_statement
            .write()
            .unwrap()
            .insert(key, Arc::downgrade(&statement));
    }

    /// Inserts a token-to-statement mapping.
    pub fn insert_token(&self, token: Vec<u8>, statement: Arc<PreparedStatementInner>) {
        self.token_to_statement
            .write()
            .unwrap()
            .insert(token, Arc::downgrade(&statement));
    }

    /// Removes a token-to-statement mapping.
    pub fn remove_token(&self, token: &[u8]) {
        self.token_to_statement.write().unwrap().remove(token);
    }

    /// Looks up a statement by its (query, app_profile_id) pair.
    pub fn lookup_by_query(
        &self,
        query: &str,
        app_profile_id: &str,
    ) -> Option<Arc<PreparedStatementInner>> {
        let mut _evicted_stmt: Option<Arc<PreparedStatementInner>> = None;
        let result = {
            let query_map = self.query_to_statement.read().unwrap();
            let key = (query.to_string(), app_profile_id.to_string());
            if let Some(weak_stmt) = query_map.get(&key) {
                if let Some(stmt) = weak_stmt.upgrade() {
                    // Low-contention LRU Promotion: Only promote if last executed was > 10s ago
                    let now = Instant::now().duration_since(stmt.base_instant).as_secs();
                    let last = stmt.last_executed_seconds.load(Ordering::Relaxed);
                    if now.saturating_sub(last) > 10 {
                        let mut lru = self.active_lru.lock().unwrap();
                        lru.retain(|x| !Arc::ptr_eq(x, &stmt));
                        lru.push_back(stmt.clone());
                        if lru.len() > 1000 {
                            _evicted_stmt = lru.pop_front();
                        }
                    }
                    Some(stmt)
                } else {
                    None
                }
            } else {
                None
            }
        }; // query_map read guard and active_lru lock guard go out of scope here

        // _evicted_stmt is dropped safely here, outside the RwLock/Mutex scope
        drop(_evicted_stmt);
        result
    }

    /// Looks up a statement by its compiled plan token bytes.
    pub fn lookup_by_token(&self, token: &[u8]) -> Option<Arc<PreparedStatementInner>> {
        let mut _evicted_stmt: Option<Arc<PreparedStatementInner>> = None;
        let result = {
            let token_map = self.token_to_statement.read().unwrap();
            if let Some(weak_stmt) = token_map.get(token) {
                if let Some(stmt) = weak_stmt.upgrade() {
                    // Low-contention LRU Promotion: Only promote if last executed was > 10s ago
                    let now = Instant::now().duration_since(stmt.base_instant).as_secs();
                    let last = stmt.last_executed_seconds.load(Ordering::Relaxed);
                    if now.saturating_sub(last) > 10 {
                        let mut lru = self.active_lru.lock().unwrap();
                        lru.retain(|x| !Arc::ptr_eq(x, &stmt));
                        lru.push_back(stmt.clone());
                        if lru.len() > 1000 {
                            _evicted_stmt = lru.pop_front();
                        }
                    }
                    Some(stmt)
                } else {
                    None
                }
            } else {
                None
            }
        }; // token_map read guard and active_lru lock guard go out of scope here

        // _evicted_stmt is dropped safely here, outside the RwLock/Mutex scope
        drop(_evicted_stmt);
        result
    }

    /// Looks up a statement by (query, app_profile_id), or inserts the one returned by `creator`
    /// if no entry exists. The app_profile_id is part of the key because the same SQL compiled
    /// for different app profiles produces independent plans that route to different servers.
    /// This prevents duplicate statements on concurrent cache misses for the same key.
    pub fn get_or_insert_query<F>(
        &self,
        query: &str,
        app_profile_id: &str,
        creator: F,
    ) -> Arc<PreparedStatementInner>
    where
        F: FnOnce() -> Arc<PreparedStatementInner>,
    {
        if let Some(stmt) = self.lookup_by_query(query, app_profile_id) {
            return stmt;
        }

        let key = (query.to_string(), app_profile_id.to_string());
        let mut _evicted_stmt: Option<Arc<PreparedStatementInner>> = None;
        let stmt = {
            let mut query_map = self.query_to_statement.write().unwrap();
            // Double check under write lock
            if let Some(weak_stmt) = query_map.get(&key) {
                if let Some(stmt) = weak_stmt.upgrade() {
                    let mut lru = self.active_lru.lock().unwrap();
                    lru.retain(|x| !Arc::ptr_eq(x, &stmt));
                    lru.push_back(stmt.clone());
                    if lru.len() > 1000 {
                        _evicted_stmt = lru.pop_front();
                    }
                    return stmt;
                }
            }
            let stmt = creator();
            query_map.insert(key, Arc::downgrade(&stmt));

            let mut lru = self.active_lru.lock().unwrap();
            lru.push_back(stmt.clone());
            if lru.len() > 1000 {
                _evicted_stmt = lru.pop_front();
            }
            stmt
        }; // query_map write guard and active_lru lock guard go out of scope here

        // _evicted_stmt is dropped safely here, outside the RwLock/Mutex scope
        drop(_evicted_stmt);
        stmt
    }
}

/// Inner shared state of a prepared statement, wrapped in a single Arc.
pub struct PreparedStatementInner {
    /// The raw SQL query template (e.g., "SELECT * FROM users WHERE id = @id")
    pub query: String,
    /// The default application profile routing
    pub app_profile_id: String,
    /// Expected SQL type declarations of query parameters for server compilation
    pub param_types: std::collections::HashMap<String, SqlType>,
    /// Lock-free atomic storage for the compiled plan state reference
    pub plan_state: ArcSwapOption<CompiledPlanState>,
    /// Localized mutex to serialize PrepareQuery calls on cache misses/expirations
    pub prepare_lock: AsyncMutex<()>,
    /// Fixed base instant to calculate relative elapsed time for lock-free atomics
    pub base_instant: Instant,
    /// Lock-free atomic elapsed seconds since base_instant when last executed
    pub last_executed_seconds: AtomicU64,
    /// Weak reference to the parent cache to enable clean deregistration on drop
    pub cache: std::sync::OnceLock<std::sync::Weak<ClientStatementCache>>,
}

/// Exposes a prepared query statement handle. The cache is decentralized
/// and housed directly inside this instance, achieving lock-free, zero-contention reads!
#[derive(Clone)]
pub struct PreparedStatement {
    pub(crate) inner: Arc<PreparedStatementInner>,
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
                prepare_lock: AsyncMutex::new(()),
                base_instant,
                last_executed_seconds: AtomicU64::new(0),
                cache: std::sync::OnceLock::new(),
            }),
        }
    }

    /// Constructs PreparedStatement from a shared inner state.
    pub fn from_inner(inner: Arc<PreparedStatementInner>) -> Self {
        Self { inner }
    }

    /// Returns the shared inner state of the prepared statement.
    pub fn into_inner(self) -> Arc<PreparedStatementInner> {
        self.inner
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

        // Ensure self.inner is registered in the client's query_to_statement index.
        // When PreparedStatement is created via PreparedStatement::new() (Use Case A),
        // it starts unlinked from any cache. Registering it here means a subsequent
        // execute_query call with the same raw SQL (Use Case B) will find and reuse
        // this compiled plan instead of issuing a redundant PrepareQuery RPC.
        //
        // get_or_insert_query is used (not insert_query) so that if another inner is
        // already registered for this query we don't clobber it — we just ensure ours
        // is present if the slot is empty.
        let _ = self
            .inner
            .cache
            .set(Arc::downgrade(&client.statement_cache));
        let inner_for_cache = Arc::clone(&self.inner);
        client.statement_cache.get_or_insert_query(
            &self.inner.query,
            &self.inner.app_profile_id,
            || inner_for_cache,
        );

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

        let ttl_duration =
            ttl_from_valid_until(response.valid_until, std::time::Duration::from_secs(3600));

        // Enforce a safety floor duration (minimum 10 seconds) to protect against rapid tight-loops
        let ttl_duration = std::cmp::max(ttl_duration, std::time::Duration::from_secs(10));

        let expires_at = Instant::now() + ttl_duration;
        let compiled_state = Arc::new(CompiledPlanState {
            plan_token: response.prepared_query,
            expires_at,
        });

        // Register initial token in client cache
        client
            .statement_cache
            .insert_token(compiled_state.plan_token.clone(), self.inner.clone());

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

            // Snapshot the current plan token before refreshing so we can clean it up
            // after the rotation. Done outside the RPC call to avoid holding a reference
            // across the await.
            let old_plan = inner.plan_state.load_full();

            // Delegate to prepare_query_rpc, which handles: building the request,
            // calling the gRPC, parsing the TTL correctly, registering the new token
            // in token_to_statement, marking the statement as used, and scheduling
            // the next proactive refresh cycle. This keeps the logic in one place
            // instead of duplicating it here.
            let stmt_handle = PreparedStatement {
                inner: inner.clone(),
            };
            match stmt_handle.prepare_query_rpc(&mut client).await {
                Ok(new_plan) => {
                    // Atomically promote the new plan so serving threads see it
                    // lock-free on their next fast-path check.
                    inner.plan_state.store(Some(new_plan.clone()));

                    // Remove the old token from the cache with a 10s grace period to
                    // allow any in-flight requests still carrying the stale token to finish.
                    if let Some(old) = old_plan {
                        if old.plan_token != new_plan.plan_token {
                            let client_clone = client.clone();
                            tokio::spawn(async move {
                                tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                                client_clone.statement_cache.remove_token(&old.plan_token);
                            });
                        }
                    }
                    log::info!(
                        "Successfully rotated plan token asynchronously for '{}'",
                        inner.query
                    );
                }
                Err(e) => {
                    log::error!(
                        "Failed to rotate plan token asynchronously for '{}': {:?}",
                        inner.query,
                        e
                    );
                    // Reschedule after a cooldown delay (10s, or 2s for short-lived plans)
                    let cooldown = if ttl > Duration::from_secs(30) {
                        Duration::from_secs(10)
                    } else {
                        Duration::from_secs(2)
                    };
                    let offset = if ttl > Duration::from_secs(300) {
                        Duration::from_secs(60)
                    } else {
                        ttl / 5
                    };
                    let next_expires_at = Instant::now() + cooldown + offset;
                    stmt_handle.schedule_proactive_refresh(
                        client.clone(),
                        ttl, // Pass the original TTL to keep the idle check large and correct
                        next_expires_at,
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
                    if (status.code() == tonic::Code::FailedPrecondition
                        || status.code() == tonic::Code::InvalidArgument)
                        && status.message().contains("PREPARED_QUERY_EXPIRED") =>
                {
                    if retry_count >= 2 {
                        return Err(Error::RpcError(status));
                    }
                    log::warn!("Server-side plan expired (PREPARED_QUERY_EXPIRED). Evicting plan and retrying compile...");

                    client.statement_cache.remove_token(&plan.plan_token);

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

impl Drop for PreparedStatementInner {
    fn drop(&mut self) {
        if let Some(weak_cache) = self.cache.get() {
            if let Some(cache) = weak_cache.upgrade() {
                // Remove from query map using the composite (query, app_profile_id) key
                {
                    let key = (self.query.clone(), self.app_profile_id.clone());
                    let mut query_map = cache.query_to_statement.write().unwrap();
                    if let Some(weak_ref) = query_map.get(&key) {
                        if weak_ref.strong_count() == 0 {
                            query_map.remove(&key);
                        }
                    }
                }
                // Remove from token map if plan token exists
                if let Some(plan) = self.plan_state.load_full() {
                    let mut token_map = cache.token_to_statement.write().unwrap();
                    if let Some(weak_ref) = token_map.get(&plan.plan_token) {
                        if weak_ref.strong_count() == 0 {
                            token_map.remove(&plan.plan_token);
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    // --- Unit tests for ttl_from_valid_until ---
    //
    // These tests are the primary regression guard for Bug 1: valid_until is an absolute
    // Unix timestamp, not a duration. The old code called Duration::new(seconds, nanos),
    // which for a typical server response of ~1,800,000,000 seconds would produce a
    // ~57-year TTL instead of the correct value.

    #[test]
    fn ttl_from_valid_until_converts_future_timestamp_to_remaining_duration() {
        // A timestamp 30 seconds in the future should produce a TTL of approximately 30s.
        let future = SystemTime::now() + Duration::from_secs(30);
        let since_epoch = future.duration_since(UNIX_EPOCH).unwrap();
        let ts = prost_types::Timestamp {
            seconds: since_epoch.as_secs() as i64,
            nanos: since_epoch.subsec_nanos() as i32,
        };

        let ttl = ttl_from_valid_until(Some(ts), Duration::from_secs(3600));

        // Allow a few seconds of slack for test execution time.
        assert!(
            ttl >= Duration::from_secs(25) && ttl <= Duration::from_secs(31),
            "expected ~30s TTL from a timestamp 30s in the future, got {:?}",
            ttl
        );
    }

    #[test]
    fn ttl_from_valid_until_past_timestamp_returns_fallback() {
        // A timestamp already in the past cannot produce a positive duration.
        // The function should return the fallback rather than panicking or returning zero.
        let past = SystemTime::now() - Duration::from_secs(60);
        let since_epoch = past.duration_since(UNIX_EPOCH).unwrap();
        let ts = prost_types::Timestamp {
            seconds: since_epoch.as_secs() as i64,
            nanos: since_epoch.subsec_nanos() as i32,
        };
        let fallback = Duration::from_secs(3600);

        let ttl = ttl_from_valid_until(Some(ts), fallback);

        assert_eq!(
            ttl,
            Duration::from_secs(10),
            "past timestamp should return the 10-second safety TTL floor"
        );
    }

    #[test]
    fn ttl_from_valid_until_none_returns_fallback() {
        let fallback = Duration::from_secs(3600);
        let ttl = ttl_from_valid_until(None, fallback);
        assert_eq!(ttl, fallback);
    }

    #[test]
    fn ttl_from_valid_until_rejects_small_integer_as_timestamp() {
        // Regression test: the old code did Duration::new(valid_until.seconds, nanos).
        // The mock server was written to match that bug, passing seconds=30 as a relative TTL.
        // A value of 30 as a Unix timestamp means Jan 1 1970 + 30s — firmly in the past.
        // Correct behaviour: return the fallback.
        // Buggy behaviour: return Duration::new(30, 0) = 30s.
        let ts = prost_types::Timestamp {
            seconds: 30,
            nanos: 0,
        };
        let fallback = Duration::from_secs(3600);

        let ttl = ttl_from_valid_until(Some(ts), fallback);

        assert_eq!(
            ttl, Duration::from_secs(10),
            "seconds=30 is a Unix timestamp from 1970, not a 30s duration — should use 10-second safety TTL floor"
        );
    }

    #[test]
    fn ttl_from_valid_until_large_epoch_seconds_would_have_been_wrong_under_old_bug() {
        // A typical server response uses ~1,800,000,000 Unix seconds (year ~2027).
        // Old bug: Duration::new(1_800_000_000, 0) ≈ 57 years.
        // Correct: that timestamp is ~1 year in the future from mid-2026, so TTL ≈ 1 year.
        // Either way the result must be far less than 57 years.
        let ts = prost_types::Timestamp {
            seconds: 1_800_000_000,
            nanos: 0,
        };
        let fallback = Duration::from_secs(3600);

        let ttl = ttl_from_valid_until(Some(ts), fallback);

        let fifty_seven_years = Duration::from_secs(57 * 365 * 24 * 3600);
        assert!(
            ttl < fifty_seven_years,
            "TTL must not be ~57 years — that would indicate the old Duration::new() bug. Got {:?}",
            ttl
        );
    }
}
