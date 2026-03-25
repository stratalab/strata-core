//! The Executor - single entry point to Strata's engine.
//!
//! The Executor is a stateless dispatcher that routes commands to the
//! appropriate primitive operations and converts results to outputs.

use std::sync::Arc;
use std::time::Instant;

use strata_engine::Database;
use strata_security::AccessMode;
use tracing::{debug, warn};

use crate::bridge::{to_core_branch_id, Primitives};
use crate::convert::convert_result;
use crate::types::BranchId;
use crate::{Command, Error, Output, Result};

/// The command executor - single entry point to Strata's engine.
///
/// The Executor is **stateless**: it holds references to the database substrate
/// but maintains no state of its own. All state lives in the engine.
///
/// # Thread Safety
///
/// Executor is `Send + Sync` and can be shared across threads.
///
/// # Example
///
/// ```text
/// use strata_executor::{Command, Executor, BranchId};
/// use strata_core::Value;
///
/// let executor = Executor::new(substrate);
///
/// // Branch is optional - omit it to use the default branch
/// let result = executor.execute(Command::KvPut {
///     branch: None,
///     key: "foo".into(),
///     value: Value::Int(42),
/// })?;
///
/// // Or provide an explicit branch
/// let result = executor.execute(Command::KvPut {
///     branch: Some(BranchId::from("my-branch")),
///     key: "foo".into(),
///     value: Value::Int(42),
/// })?;
/// ```
pub struct Executor {
    primitives: Arc<Primitives>,
    access_mode: AccessMode,
    /// Shared state for the embed refresh timer thread (condvar for instant shutdown).
    embed_refresh_state: Arc<EmbedRefreshState>,
    /// Handle for the embed refresh timer thread (joined on drop).
    embed_refresh_handle: Option<std::thread::JoinHandle<()>>,
}

/// Interval between automatic embed buffer flushes (Elasticsearch-style NRT refresh).
const EMBED_REFRESH_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

/// Shared state between the embed refresh timer thread and `Executor::drop`.
///
/// Uses a condvar so that shutdown signals wake the thread instantly
/// instead of waiting up to 1 second for `thread::sleep` to finish.
struct EmbedRefreshState {
    mu: std::sync::Mutex<bool>,
    cond: std::sync::Condvar,
}

impl Executor {
    /// Create a new executor from a database instance.
    pub fn new(db: Arc<Database>) -> Self {
        Self::new_with_mode(db, AccessMode::ReadWrite)
    }

    /// Create a new executor with an explicit access mode.
    pub fn new_with_mode(db: Arc<Database>, access_mode: AccessMode) -> Self {
        let primitives = Arc::new(Primitives::new(db));

        // Log a startup hint when auto_embed is on but the model isn't downloaded yet.
        #[cfg(feature = "embed")]
        if primitives.db.auto_embed_enabled() {
            let model_name = primitives.db.embed_model();
            let registry = strata_intelligence::ModelRegistry::new();
            // Only log if the model is known (in catalog) but not yet downloaded.
            // If the name is invalid, resolve() will surface a clear error on
            // first embed attempt — no need for a misleading startup hint.
            let is_known = registry
                .list_available()
                .iter()
                .any(|m| m.name.eq_ignore_ascii_case(&model_name));
            if is_known && registry.resolve(&model_name).is_err() {
                tracing::info!(
                    model = %model_name,
                    "auto_embed is enabled but model is not downloaded \u{2014} \
                     it will be fetched automatically on first write, or run: \
                     strata models pull {model_name}"
                );
            }
        }

        let state = Arc::new(EmbedRefreshState {
            mu: std::sync::Mutex::new(false),
            cond: std::sync::Condvar::new(),
        });
        let handle = Self::spawn_embed_refresh_thread(&primitives, &state);
        Self {
            primitives,
            access_mode,
            embed_refresh_state: state,
            embed_refresh_handle: Some(handle),
        }
    }

    /// Spawn a background thread that flushes the embed buffer every 1 second.
    ///
    /// This implements Elasticsearch-style near-real-time (NRT) refresh:
    /// embeddings become searchable within ~1 second of the write, even if
    /// the buffer hasn't reached `batch_size`.
    fn spawn_embed_refresh_thread(
        primitives: &Arc<Primitives>,
        state: &Arc<EmbedRefreshState>,
    ) -> std::thread::JoinHandle<()> {
        let p = Arc::clone(primitives);
        let st = Arc::clone(state);
        std::thread::Builder::new()
            .name("strata-embed-refresh".into())
            .spawn(move || {
                let mut guard = st.mu.lock().unwrap_or_else(|e| e.into_inner());
                while !*guard {
                    // Wait for shutdown signal or timeout (1s refresh interval).
                    // On shutdown, the condvar is signaled and we exit instantly.
                    let (g, _) = st
                        .cond
                        .wait_timeout(guard, EMBED_REFRESH_INTERVAL)
                        .unwrap_or_else(|e| e.into_inner());
                    guard = g;
                    if *guard {
                        break;
                    }
                    if !p.db.auto_embed_enabled() {
                        continue;
                    }
                    // Submit flush to the scheduler so it runs on a worker thread,
                    // keeping the timer thread lightweight.
                    let p_clone = Arc::clone(&p);
                    let _ =
                        p.db.scheduler()
                            .submit(strata_engine::TaskPriority::Normal, move || {
                                crate::handlers::embed_hook::flush_embed_buffer(&p_clone)
                            });
                }
            })
            .expect("failed to spawn embed refresh thread")
    }

    /// Returns the access mode of this executor.
    pub fn access_mode(&self) -> AccessMode {
        self.access_mode
    }

    /// Auto-register a space on first write to a non-default space.
    ///
    /// This is idempotent: calling it on an already-registered space just
    /// performs a single `txn.get()` check. The "default" space is skipped
    /// since it always exists implicitly.
    fn ensure_space_registered(&self, branch: &BranchId, space: &str) -> Result<()> {
        if space == "default" {
            return Ok(());
        }
        let core_branch = to_core_branch_id(branch)?;
        convert_result(self.primitives.space.register(core_branch, space))?;
        Ok(())
    }

    /// Execute a single command.
    ///
    /// Resolves any `None` branch fields to the default branch before dispatch.
    /// Returns the command result or an error.
    pub fn execute(&self, mut cmd: Command) -> Result<Output> {
        if self.access_mode == AccessMode::ReadOnly && cmd.is_write() {
            warn!(target: "strata::command", command = %cmd.name(), "Write rejected in read-only mode");
            let hint = if self.primitives.db.is_follower() {
                Some("This database is a read-only follower. Writes must go through the primary instance.".to_string())
            } else {
                Some("Database is in read-only mode.".to_string())
            };
            return Err(Error::AccessDenied {
                command: cmd.name().to_string(),
                hint,
            });
        }

        cmd.resolve_defaults();

        // Reject data commands targeting reserved _system branches
        if let Some(branch) = cmd.resolved_branch() {
            crate::handlers::reject_system_branch(branch)?;
        }

        self.dispatch(cmd)
    }

    /// Execute a command targeting the `_system_` branch.
    ///
    /// Skips the `reject_system_branch` guard, but still enforces access mode.
    /// Used by [`SystemBranch`](crate::api::SystemBranch) to provide internal
    /// access to the system branch.
    pub(crate) fn execute_internal(&self, mut cmd: Command) -> Result<Output> {
        if self.access_mode == AccessMode::ReadOnly && cmd.is_write() {
            warn!(target: "strata::command", command = %cmd.name(), "Write rejected in read-only mode");
            let hint = if self.primitives.db.is_follower() {
                Some("This database is a read-only follower. Writes must go through the primary instance.".to_string())
            } else {
                Some("Database is in read-only mode.".to_string())
            };
            return Err(Error::AccessDenied {
                command: cmd.name().to_string(),
                hint,
            });
        }

        cmd.resolve_defaults();
        self.dispatch(cmd)
    }

    /// Internal dispatch — shared by `execute` and `execute_internal`.
    fn dispatch(&self, cmd: Command) -> Result<Output> {
        let cmd_name = cmd.name();
        let start = Instant::now();

        let result = match cmd {
            // Database commands
            Command::Ping => Ok(Output::Pong {
                version: env!("CARGO_PKG_VERSION").to_string(),
            }),
            Command::Info => {
                let branch_count = self
                    .primitives
                    .branch
                    .list_branches()
                    .map(|ids| ids.len() as u64)
                    .unwrap_or(0);
                Ok(Output::DatabaseInfo(crate::types::DatabaseInfo {
                    version: env!("CARGO_PKG_VERSION").to_string(),
                    uptime_secs: self.primitives.db.uptime_secs(),
                    branch_count,
                    total_keys: self.primitives.db.approximate_total_keys(),
                }))
            }
            Command::Health => {
                let report = self.primitives.db.health();
                Ok(Output::Health(report))
            }
            Command::Metrics => {
                let metrics = self.primitives.db.metrics();
                Ok(Output::Metrics(metrics))
            }
            Command::Flush => {
                crate::handlers::embed_hook::flush_embed_buffer(&self.primitives);
                self.primitives.db.scheduler().drain();
                convert_result(self.primitives.db.flush())?;
                Ok(Output::Unit)
            }
            Command::Compact => {
                convert_result(self.primitives.db.compact())?;
                Ok(Output::Unit)
            }
            Command::Describe { branch } => {
                let branch = branch.expect("resolve_defaults fills this");
                crate::handlers::database::describe(&self.primitives, branch)
            }
            Command::EmbedStatus => {
                let info = crate::handlers::embed_hook::embed_status(&self.primitives);
                Ok(Output::EmbedStatus(info))
            }
            Command::ReindexEmbeddings { branch } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                let branch_id = crate::bridge::to_core_branch_id(&branch)?;
                crate::handlers::embed_hook::reindex_embeddings(&self.primitives, branch_id)
            }
            Command::ConfigGet => crate::handlers::config::config_get(&self.primitives),
            Command::ConfigureSet { key, value } => {
                crate::handlers::config::configure_set(&self.primitives, key, value)
            }
            Command::ConfigureGetKey { key } => {
                crate::handlers::config::configure_get_key(&self.primitives, key)
            }
            Command::ConfigSetAutoEmbed { enabled } => {
                crate::handlers::config::config_set_auto_embed(&self.primitives, enabled)
            }
            Command::AutoEmbedStatus => {
                crate::handlers::config::auto_embed_status(&self.primitives)
            }
            Command::DurabilityCounters => {
                crate::handlers::config::durability_counters(&self.primitives)
            }
            Command::TimeRange { branch } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                crate::handlers::vector::time_range(&self.primitives, branch)
            }

            // KV commands (MVP: 4 commands)
            Command::KvPut {
                branch,
                space,
                key,
                value,
            } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                let space = space.unwrap_or_else(|| "default".to_string());
                self.ensure_space_registered(&branch, &space)?;
                crate::handlers::kv::kv_put(&self.primitives, branch, space, key, value)
            }
            Command::KvBatchPut {
                branch,
                space,
                entries,
            } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                let space = space.unwrap_or_else(|| "default".to_string());
                self.ensure_space_registered(&branch, &space)?;
                crate::handlers::kv::kv_batch_put(&self.primitives, branch, space, entries)
            }
            Command::KvGet {
                branch,
                space,
                key,
                as_of,
            } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                let space = space.unwrap_or_else(|| "default".to_string());
                if let Some(ts) = as_of {
                    crate::handlers::kv::kv_get_at(&self.primitives, branch, space, key, ts)
                } else {
                    crate::handlers::kv::kv_get(&self.primitives, branch, space, key)
                }
            }
            Command::KvDelete { branch, space, key } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                let space = space.unwrap_or_else(|| "default".to_string());
                self.ensure_space_registered(&branch, &space)?;
                crate::handlers::kv::kv_delete(&self.primitives, branch, space, key)
            }
            Command::KvList {
                branch,
                space,
                prefix,
                cursor,
                limit,
                as_of,
            } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                let space = space.unwrap_or_else(|| "default".to_string());
                if let Some(ts) = as_of {
                    crate::handlers::kv::kv_list_at(&self.primitives, branch, space, prefix, ts)
                } else {
                    crate::handlers::kv::kv_list(
                        &self.primitives,
                        branch,
                        space,
                        prefix,
                        cursor,
                        limit,
                    )
                }
            }
            // Note: as_of is intentionally ignored for getv — version history
            // always returns all versions, not a point-in-time snapshot.
            Command::KvGetv {
                branch,
                space,
                key,
                as_of: _,
            } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                let space = space.unwrap_or_else(|| "default".to_string());
                crate::handlers::kv::kv_getv(&self.primitives, branch, space, key)
            }

            // JSON commands
            Command::JsonSet {
                branch,
                space,
                key,
                path,
                value,
            } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                let space = space.unwrap_or_else(|| "default".to_string());
                self.ensure_space_registered(&branch, &space)?;
                crate::handlers::json::json_set(&self.primitives, branch, space, key, path, value)
            }
            Command::JsonBatchSet {
                branch,
                space,
                entries,
            } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                let space = space.unwrap_or_else(|| "default".to_string());
                self.ensure_space_registered(&branch, &space)?;
                crate::handlers::json::json_batch_set(&self.primitives, branch, space, entries)
            }
            Command::JsonBatchGet {
                branch,
                space,
                entries,
            } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                let space = space.unwrap_or_else(|| "default".to_string());
                crate::handlers::json::json_batch_get(&self.primitives, branch, space, entries)
            }
            Command::JsonBatchDelete {
                branch,
                space,
                entries,
            } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                let space = space.unwrap_or_else(|| "default".to_string());
                self.ensure_space_registered(&branch, &space)?;
                crate::handlers::json::json_batch_delete(&self.primitives, branch, space, entries)
            }
            Command::JsonGet {
                branch,
                space,
                key,
                path,
                as_of,
            } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                let space = space.unwrap_or_else(|| "default".to_string());
                if let Some(ts) = as_of {
                    crate::handlers::json::json_get_at(
                        &self.primitives,
                        branch,
                        space,
                        key,
                        path,
                        ts,
                    )
                } else {
                    crate::handlers::json::json_get(&self.primitives, branch, space, key, path)
                }
            }
            // Note: as_of is intentionally ignored for getv — version history
            // always returns all versions, not a point-in-time snapshot.
            Command::JsonGetv {
                branch,
                space,
                key,
                as_of: _,
            } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                let space = space.unwrap_or_else(|| "default".to_string());
                crate::handlers::json::json_getv(&self.primitives, branch, space, key)
            }
            Command::JsonDelete {
                branch,
                space,
                key,
                path,
            } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                let space = space.unwrap_or_else(|| "default".to_string());
                self.ensure_space_registered(&branch, &space)?;
                crate::handlers::json::json_delete(&self.primitives, branch, space, key, path)
            }
            Command::JsonList {
                branch,
                space,
                prefix,
                cursor,
                limit,
                as_of,
            } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                let space = space.unwrap_or_else(|| "default".to_string());
                if let Some(ts) = as_of {
                    crate::handlers::json::json_list_at(&self.primitives, branch, space, prefix, ts)
                } else {
                    crate::handlers::json::json_list(
                        &self.primitives,
                        branch,
                        space,
                        prefix,
                        cursor,
                        limit,
                    )
                }
            }

            // Event commands (4 MVP)
            Command::EventBatchAppend {
                branch,
                space,
                entries,
            } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                let space = space.unwrap_or_else(|| "default".to_string());
                self.ensure_space_registered(&branch, &space)?;
                crate::handlers::event::event_batch_append(&self.primitives, branch, space, entries)
            }
            Command::EventAppend {
                branch,
                space,
                event_type,
                payload,
            } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                let space = space.unwrap_or_else(|| "default".to_string());
                self.ensure_space_registered(&branch, &space)?;
                crate::handlers::event::event_append(
                    &self.primitives,
                    branch,
                    space,
                    event_type,
                    payload,
                )
            }
            Command::EventGet {
                branch,
                space,
                sequence,
                as_of,
            } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                let space = space.unwrap_or_else(|| "default".to_string());
                if let Some(ts) = as_of {
                    crate::handlers::event::event_get_at(
                        &self.primitives,
                        branch,
                        space,
                        sequence,
                        ts,
                    )
                } else {
                    crate::handlers::event::event_get(&self.primitives, branch, space, sequence)
                }
            }
            Command::EventGetByType {
                branch,
                space,
                event_type,
                limit,
                after_sequence,
                as_of,
            } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                let space = space.unwrap_or_else(|| "default".to_string());
                if let Some(ts) = as_of {
                    crate::handlers::event::event_get_by_type_at(
                        &self.primitives,
                        branch,
                        space,
                        event_type,
                        ts,
                    )
                } else {
                    crate::handlers::event::event_get_by_type(
                        &self.primitives,
                        branch,
                        space,
                        event_type,
                        limit,
                        after_sequence,
                    )
                }
            }
            Command::EventLen { branch, space } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                let space = space.unwrap_or_else(|| "default".to_string());
                crate::handlers::event::event_len(&self.primitives, branch, space)
            }

            // Vector commands
            Command::VectorUpsert {
                branch,
                space,
                collection,
                key,
                vector,
                metadata,
            } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                let space = space.unwrap_or_else(|| "default".to_string());
                self.ensure_space_registered(&branch, &space)?;
                crate::handlers::vector::vector_upsert(
                    &self.primitives,
                    branch,
                    space,
                    collection,
                    key,
                    vector,
                    metadata,
                )
            }
            Command::VectorGet {
                branch,
                space,
                collection,
                key,
                as_of,
            } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                let space = space.unwrap_or_else(|| "default".to_string());
                if let Some(ts) = as_of {
                    crate::handlers::vector::vector_get_at(
                        &self.primitives,
                        branch,
                        space,
                        collection,
                        key,
                        ts,
                    )
                } else {
                    crate::handlers::vector::vector_get(
                        &self.primitives,
                        branch,
                        space,
                        collection,
                        key,
                    )
                }
            }
            Command::VectorDelete {
                branch,
                space,
                collection,
                key,
            } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                let space = space.unwrap_or_else(|| "default".to_string());
                self.ensure_space_registered(&branch, &space)?;
                crate::handlers::vector::vector_delete(
                    &self.primitives,
                    branch,
                    space,
                    collection,
                    key,
                )
            }
            Command::VectorSearch {
                branch,
                space,
                collection,
                query,
                k,
                filter,
                metric,
                as_of,
            } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                let space = space.unwrap_or_else(|| "default".to_string());
                if let Some(ts) = as_of {
                    crate::handlers::vector::vector_search_at(
                        &self.primitives,
                        branch,
                        space,
                        collection,
                        query,
                        k,
                        filter,
                        metric,
                        ts,
                    )
                } else {
                    crate::handlers::vector::vector_search(
                        &self.primitives,
                        branch,
                        space,
                        collection,
                        query,
                        k,
                        filter,
                        metric,
                    )
                }
            }
            Command::VectorCreateCollection {
                branch,
                space,
                collection,
                dimension,
                metric,
            } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                let space = space.unwrap_or_else(|| "default".to_string());
                self.ensure_space_registered(&branch, &space)?;
                crate::handlers::vector::vector_create_collection(
                    &self.primitives,
                    branch,
                    space,
                    collection,
                    dimension,
                    metric,
                )
            }
            Command::VectorDeleteCollection {
                branch,
                space,
                collection,
            } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                let space = space.unwrap_or_else(|| "default".to_string());
                self.ensure_space_registered(&branch, &space)?;
                crate::handlers::vector::vector_delete_collection(
                    &self.primitives,
                    branch,
                    space,
                    collection,
                )
            }
            Command::VectorListCollections { branch, space } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                let space = space.unwrap_or_else(|| "default".to_string());
                crate::handlers::vector::vector_list_collections(&self.primitives, branch, space)
            }
            Command::VectorCollectionStats {
                branch,
                space,
                collection,
            } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                let space = space.unwrap_or_else(|| "default".to_string());
                crate::handlers::vector::vector_collection_stats(
                    &self.primitives,
                    branch,
                    space,
                    collection,
                )
            }
            Command::VectorBatchUpsert {
                branch,
                space,
                collection,
                entries,
            } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                let space = space.unwrap_or_else(|| "default".to_string());
                self.ensure_space_registered(&branch, &space)?;
                crate::handlers::vector::vector_batch_upsert(
                    &self.primitives,
                    branch,
                    space,
                    collection,
                    entries,
                )
            }

            // Branch commands (5 MVP)
            Command::BranchCreate {
                branch_id,
                metadata,
            } => crate::handlers::branch::branch_create(&self.primitives, branch_id, metadata),
            Command::BranchGet { branch } => {
                crate::handlers::branch::branch_get(&self.primitives, branch)
            }
            Command::BranchList {
                state,
                limit,
                offset,
            } => crate::handlers::branch::branch_list(&self.primitives, state, limit, offset),
            Command::BranchExists { branch } => {
                crate::handlers::branch::branch_exists(&self.primitives, branch)
            }
            Command::BranchDelete { branch } => {
                crate::handlers::branch::branch_delete(&self.primitives, branch)
            }
            Command::BranchFork {
                source,
                destination,
                message,
                creator,
            } => crate::handlers::branch::branch_fork(
                &self.primitives,
                source,
                destination,
                message,
                creator,
            ),
            Command::BranchDiff {
                branch_a,
                branch_b,
                filter_primitives,
                filter_spaces,
                as_of,
            } => crate::handlers::branch::branch_diff(
                &self.primitives,
                branch_a,
                branch_b,
                filter_primitives,
                filter_spaces,
                as_of,
            ),
            Command::BranchDiffThreeWay {
                branch_a,
                branch_b,
            } => crate::handlers::branch::branch_diff_three_way(
                &self.primitives,
                branch_a,
                branch_b,
            ),
            Command::BranchMergeBase {
                branch_a,
                branch_b,
            } => crate::handlers::branch::branch_merge_base(
                &self.primitives,
                branch_a,
                branch_b,
            ),
            Command::BranchMerge {
                source,
                target,
                strategy,
                message,
                creator,
            } => crate::handlers::branch::branch_merge(
                &self.primitives,
                source,
                target,
                strategy,
                message,
                creator,
            ),

            // Tag commands
            Command::TagCreate {
                branch,
                name,
                version,
                message,
                creator,
            } => crate::handlers::branch::tag_create(
                &self.primitives,
                branch,
                name,
                version,
                message,
                creator,
            ),
            Command::TagDelete { branch, name } => {
                crate::handlers::branch::tag_delete(&self.primitives, branch, name)
            }
            Command::TagList { branch } => {
                crate::handlers::branch::tag_list(&self.primitives, branch)
            }
            Command::TagResolve { branch, name } => {
                crate::handlers::branch::tag_resolve(&self.primitives, branch, name)
            }

            // Note commands
            Command::NoteAdd {
                branch,
                version,
                message,
                author,
                metadata,
            } => crate::handlers::branch::note_add(
                &self.primitives,
                branch,
                version,
                message,
                author,
                metadata,
            ),
            Command::NoteGet { branch, version } => {
                crate::handlers::branch::note_get(&self.primitives, branch, version)
            }
            Command::NoteDelete { branch, version } => {
                crate::handlers::branch::note_delete(&self.primitives, branch, version)
            }

            Command::BranchRevert {
                branch,
                from_version,
                to_version,
            } => crate::handlers::branch::branch_revert(
                &self.primitives,
                branch,
                from_version,
                to_version,
            ),

            Command::BranchCherryPick {
                source,
                target,
                keys,
                filter_spaces,
                filter_keys,
                filter_primitives,
            } => crate::handlers::branch::branch_cherry_pick(
                &self.primitives,
                source,
                target,
                keys,
                filter_spaces,
                filter_keys,
                filter_primitives,
            ),

            // Transaction commands - handled by Session, not Executor
            Command::TxnBegin { .. }
            | Command::TxnCommit
            | Command::TxnRollback
            | Command::TxnInfo
            | Command::TxnIsActive => Err(Error::Internal {
                reason: "Transaction commands not yet implemented".to_string(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),

            // Retention commands
            Command::RetentionApply { branch } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                let branch_id = crate::bridge::to_core_branch_id(&branch)?;
                // Use the current version as the safe GC boundary:
                // all versions older than the current version are prunable
                // since they have been superseded by newer commits.
                let current = self.primitives.db.current_version();
                let _pruned = self.primitives.db.gc_versions_before(branch_id, current);
                Ok(Output::Unit)
            }
            Command::RetentionStats { .. } | Command::RetentionPreview { .. } => {
                Err(Error::Internal {
                    reason: "Retention commands not yet implemented".to_string(),
                    hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
                })
            }

            // Export command
            Command::DbExport {
                branch,
                space,
                primitive,
                format,
                prefix,
                limit,
                path,
            } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                let space = space.ok_or(Error::InvalidInput {
                    reason: "Space must be specified or resolved to default".into(),
                    hint: None,
                })?;
                crate::handlers::export::db_export(
                    &self.primitives,
                    branch,
                    space,
                    primitive,
                    format,
                    prefix,
                    limit,
                    path,
                )
            }

            // Bundle commands
            Command::BranchExport { branch_id, path } => {
                crate::handlers::branch::branch_export(&self.primitives, branch_id, path)
            }
            Command::BranchImport { path } => {
                crate::handlers::branch::branch_import(&self.primitives, path)
            }
            Command::BranchBundleValidate { path } => {
                crate::handlers::branch::branch_bundle_validate(path)
            }

            // Embedding commands
            Command::Embed { text } => crate::handlers::embed::embed(&self.primitives, text),
            Command::EmbedBatch { texts } => {
                crate::handlers::embed::embed_batch(&self.primitives, texts)
            }

            // Model management commands
            Command::ModelsList => crate::handlers::models::models_list(&self.primitives),
            Command::ModelsPull { name } => {
                crate::handlers::models::models_pull(&self.primitives, name)
            }
            Command::ModelsLocal => crate::handlers::models::models_local(&self.primitives),

            // Generation commands
            Command::Generate {
                model,
                prompt,
                max_tokens,
                temperature,
                top_k,
                top_p,
                seed,
                stop_tokens,
                stop_sequences,
            } => crate::handlers::generate::generate(
                &self.primitives,
                model,
                prompt,
                max_tokens,
                temperature,
                top_k,
                top_p,
                seed,
                stop_tokens,
                stop_sequences,
            ),
            Command::Tokenize {
                model,
                text,
                add_special_tokens,
            } => crate::handlers::generate::tokenize(
                &self.primitives,
                model,
                text,
                add_special_tokens,
            ),
            Command::Detokenize { model, ids } => {
                crate::handlers::generate::detokenize(&self.primitives, model, ids)
            }
            Command::GenerateUnload { model } => {
                crate::handlers::generate::generate_unload(&self.primitives, model)
            }

            // Intelligence commands
            Command::ConfigureModel {
                endpoint,
                model,
                api_key,
                timeout_ms,
            } => crate::handlers::configure_model::configure_model(
                &self.primitives,
                endpoint,
                model,
                api_key,
                timeout_ms,
            ),
            Command::Search {
                branch,
                space,
                search,
            } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                let space = space.unwrap_or_else(|| "default".to_string());
                crate::handlers::search::search(&self.primitives, branch, space, search)
            }

            // Data introspection commands
            Command::KvCount {
                branch,
                space,
                prefix,
            } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                let space = space.unwrap_or_else(|| "default".to_string());
                crate::handlers::kv::kv_count(&self.primitives, branch, space, prefix)
            }
            Command::JsonCount {
                branch,
                space,
                prefix,
            } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                let space = space.unwrap_or_else(|| "default".to_string());
                crate::handlers::json::json_count(&self.primitives, branch, space, prefix)
            }
            Command::KvSample {
                branch,
                space,
                prefix,
                count,
            } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                let space = space.unwrap_or_else(|| "default".to_string());
                crate::handlers::kv::kv_sample(
                    &self.primitives,
                    branch,
                    space,
                    prefix,
                    count.unwrap_or(5),
                )
            }
            Command::JsonSample {
                branch,
                space,
                prefix,
                count,
            } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                let space = space.unwrap_or_else(|| "default".to_string());
                crate::handlers::json::json_sample(
                    &self.primitives,
                    branch,
                    space,
                    prefix,
                    count.unwrap_or(5),
                )
            }
            Command::VectorSample {
                branch,
                space,
                collection,
                count,
            } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                let space = space.unwrap_or_else(|| "default".to_string());
                crate::handlers::vector::vector_sample(
                    &self.primitives,
                    branch,
                    space,
                    collection,
                    count.unwrap_or(5),
                )
            }

            // Space commands
            Command::SpaceList { branch } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                crate::handlers::space::space_list(&self.primitives, branch)
            }
            Command::SpaceCreate { branch, space } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                crate::handlers::space::space_create(&self.primitives, branch, space)
            }
            Command::SpaceDelete {
                branch,
                space,
                force,
            } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                crate::handlers::space::space_delete(&self.primitives, branch, space, force)
            }
            Command::SpaceExists { branch, space } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                crate::handlers::space::space_exists(&self.primitives, branch, space)
            }

            // Graph commands
            Command::GraphCreate {
                branch,
                graph,
                cascade_policy,
            } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                crate::handlers::graph::graph_create(
                    &self.primitives,
                    branch,
                    graph,
                    cascade_policy,
                )
            }
            Command::GraphDelete { branch, graph } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                crate::handlers::graph::graph_delete(&self.primitives, branch, graph)
            }
            Command::GraphList { branch } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                crate::handlers::graph::graph_list(&self.primitives, branch)
            }
            Command::GraphGetMeta { branch, graph } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                crate::handlers::graph::graph_get_meta(&self.primitives, branch, graph)
            }
            Command::GraphAddNode {
                branch,
                graph,
                node_id,
                entity_ref,
                properties,
                object_type,
            } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                crate::handlers::graph::graph_add_node(
                    &self.primitives,
                    branch,
                    graph,
                    node_id,
                    entity_ref,
                    properties,
                    object_type,
                )
            }
            Command::GraphGetNode {
                branch,
                graph,
                node_id,
            } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                crate::handlers::graph::graph_get_node(&self.primitives, branch, graph, node_id)
            }
            Command::GraphRemoveNode {
                branch,
                graph,
                node_id,
            } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                crate::handlers::graph::graph_remove_node(&self.primitives, branch, graph, node_id)
            }
            Command::GraphListNodes { branch, graph } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                crate::handlers::graph::graph_list_nodes(&self.primitives, branch, graph)
            }
            Command::GraphListNodesPaginated {
                branch,
                graph,
                limit,
                cursor,
            } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                crate::handlers::graph::graph_list_nodes_paginated(
                    &self.primitives,
                    branch,
                    graph,
                    limit,
                    cursor,
                )
            }
            Command::GraphAddEdge {
                branch,
                graph,
                src,
                dst,
                edge_type,
                weight,
                properties,
            } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                crate::handlers::graph::graph_add_edge(
                    &self.primitives,
                    branch,
                    graph,
                    src,
                    dst,
                    edge_type,
                    weight,
                    properties,
                )
            }
            Command::GraphRemoveEdge {
                branch,
                graph,
                src,
                dst,
                edge_type,
            } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                crate::handlers::graph::graph_remove_edge(
                    &self.primitives,
                    branch,
                    graph,
                    src,
                    dst,
                    edge_type,
                )
            }
            Command::GraphNeighbors {
                branch,
                graph,
                node_id,
                direction,
                edge_type,
            } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                crate::handlers::graph::graph_neighbors(
                    &self.primitives,
                    branch,
                    graph,
                    node_id,
                    direction,
                    edge_type,
                )
            }
            Command::GraphBulkInsert {
                branch,
                graph,
                nodes,
                edges,
                chunk_size,
            } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                crate::handlers::graph::graph_bulk_insert(
                    &self.primitives,
                    branch,
                    graph,
                    nodes,
                    edges,
                    chunk_size,
                )
            }
            Command::GraphBfs {
                branch,
                graph,
                start,
                max_depth,
                max_nodes,
                edge_types,
                direction,
            } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                crate::handlers::graph::graph_bfs(
                    &self.primitives,
                    branch,
                    graph,
                    start,
                    max_depth,
                    max_nodes,
                    edge_types,
                    direction,
                )
            }

            // Ontology commands
            Command::GraphDefineObjectType {
                branch,
                graph,
                definition,
            } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                crate::handlers::graph::graph_define_object_type(
                    &self.primitives,
                    branch,
                    graph,
                    definition,
                )
            }
            Command::GraphGetObjectType {
                branch,
                graph,
                name,
            } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                crate::handlers::graph::graph_get_object_type(&self.primitives, branch, graph, name)
            }
            Command::GraphListObjectTypes { branch, graph } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                crate::handlers::graph::graph_list_object_types(&self.primitives, branch, graph)
            }
            Command::GraphListOntologyTypes { branch, graph } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                crate::handlers::graph::graph_list_ontology_types(&self.primitives, branch, graph)
            }
            Command::GraphDeleteObjectType {
                branch,
                graph,
                name,
            } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                crate::handlers::graph::graph_delete_object_type(
                    &self.primitives,
                    branch,
                    graph,
                    name,
                )
            }
            Command::GraphDefineLinkType {
                branch,
                graph,
                definition,
            } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                crate::handlers::graph::graph_define_link_type(
                    &self.primitives,
                    branch,
                    graph,
                    definition,
                )
            }
            Command::GraphGetLinkType {
                branch,
                graph,
                name,
            } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                crate::handlers::graph::graph_get_link_type(&self.primitives, branch, graph, name)
            }
            Command::GraphListLinkTypes { branch, graph } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                crate::handlers::graph::graph_list_link_types(&self.primitives, branch, graph)
            }
            Command::GraphDeleteLinkType {
                branch,
                graph,
                name,
            } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                crate::handlers::graph::graph_delete_link_type(
                    &self.primitives,
                    branch,
                    graph,
                    name,
                )
            }
            Command::GraphFreezeOntology { branch, graph } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                crate::handlers::graph::graph_freeze_ontology(&self.primitives, branch, graph)
            }
            Command::GraphOntologyStatus { branch, graph } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                crate::handlers::graph::graph_ontology_status(&self.primitives, branch, graph)
            }
            Command::GraphOntologySummary { branch, graph } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                crate::handlers::graph::graph_ontology_summary(&self.primitives, branch, graph)
            }
            Command::GraphNodesByType {
                branch,
                graph,
                object_type,
            } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                crate::handlers::graph::graph_nodes_by_type(
                    &self.primitives,
                    branch,
                    graph,
                    object_type,
                )
            }

            // Graph Analytics
            Command::GraphWcc {
                branch,
                graph,
                top_n,
                include_all,
            } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                crate::handlers::graph::graph_wcc(
                    &self.primitives,
                    branch,
                    graph,
                    top_n,
                    include_all,
                )
            }
            Command::GraphCdlp {
                branch,
                graph,
                max_iterations,
                direction,
                top_n,
                include_all,
            } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                crate::handlers::graph::graph_cdlp(
                    &self.primitives,
                    branch,
                    graph,
                    max_iterations,
                    direction,
                    top_n,
                    include_all,
                )
            }
            Command::GraphPagerank {
                branch,
                graph,
                damping,
                max_iterations,
                tolerance,
                top_n,
                include_all,
            } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                crate::handlers::graph::graph_pagerank(
                    &self.primitives,
                    branch,
                    graph,
                    damping,
                    max_iterations,
                    tolerance,
                    top_n,
                    include_all,
                )
            }
            Command::GraphLcc {
                branch,
                graph,
                top_n,
                include_all,
            } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                crate::handlers::graph::graph_lcc(
                    &self.primitives,
                    branch,
                    graph,
                    top_n,
                    include_all,
                )
            }
            Command::GraphSssp {
                branch,
                graph,
                source,
                direction,
                top_n,
                include_all,
            } => {
                let branch = branch.ok_or(Error::InvalidInput {
                    reason: "Branch must be specified or resolved to default".into(),
                    hint: None,
                })?;
                crate::handlers::graph::graph_sssp(
                    &self.primitives,
                    branch,
                    graph,
                    source,
                    direction,
                    top_n,
                    include_all,
                )
            }
        };

        match &result {
            Ok(_) => {
                debug!(target: "strata::command", command = %cmd_name, duration_us = start.elapsed().as_micros() as u64, "Command executed");
            }
            Err(e) => {
                warn!(target: "strata::command", command = %cmd_name, duration_us = start.elapsed().as_micros() as u64, error = %e, "Command failed");
            }
        }

        result
    }

    /// Execute multiple commands sequentially.
    ///
    /// Returns all results in the same order as the input commands.
    /// Execution continues even if some commands fail.
    pub fn execute_many(&self, cmds: Vec<Command>) -> Vec<Result<Output>> {
        cmds.into_iter().map(|cmd| self.execute(cmd)).collect()
    }

    /// Get a reference to the underlying primitives.
    pub(crate) fn primitives(&self) -> &Arc<Primitives> {
        &self.primitives
    }
}

impl Drop for Executor {
    fn drop(&mut self) {
        // Signal the embed refresh timer thread to exit and wait for it.
        // The condvar wakes the thread instantly (no 1s sleep delay).
        {
            let mut guard = self
                .embed_refresh_state
                .mu
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            *guard = true;
            self.embed_refresh_state.cond.notify_one();
        }
        if let Some(handle) = self.embed_refresh_handle.take() {
            let _ = handle.join();
        }

        // Drain any pending embeddings so they aren't silently lost when the
        // executor is dropped without an explicit flush.
        crate::handlers::embed_hook::flush_embed_buffer(&self.primitives);
    }
}

// Static assertion: Executor must remain Send+Sync.
// If a future refactor adds a non-Send/Sync field, this will fail at compile time.
const _: () = {
    fn _assert_send<T: Send>() {}
    fn _assert_sync<T: Sync>() {}
    fn _check() {
        _assert_send::<Executor>();
        _assert_sync::<Executor>();
    }
};
