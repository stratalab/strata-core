//! Auto-embed hook called from write handlers.
//!
//! When auto-embedding is enabled and the `embed` feature is compiled in,
//! this module generates embeddings for text values and stores them in
//! shadow vector collections.
//!
//! Embeddings are buffered in an [`EmbedBuffer`] and flushed as a batch when
//! the buffer reaches `batch_size` items, or when [`flush_embed_buffer()`] is
//! called explicitly (e.g. on `db.flush()`).

use std::sync::Arc;

use crate::bridge::Primitives;
use crate::output::EmbedStatusInfo;

// Re-export shadow collection names from engine (single source of truth).
pub use strata_engine::database::{SHADOW_EVENT, SHADOW_JSON, SHADOW_KV, SHADOW_STATE};

/// Separator for composite shadow keys (ASCII Unit Separator).
/// Avoids ambiguity since "/" is allowed in both space and key names.
#[cfg(feature = "embed")]
const SHADOW_KEY_SEP: char = '\x1f';

/// In-memory state for auto-embedding shadow collection tracking.
///
/// Stored as a Database extension to share the created-collections cache
/// across all handles. Uses `Mutex<HashSet>` to avoid adding a `dashmap`
/// dependency to the executor crate.
#[cfg(feature = "embed")]
pub struct AutoEmbedState {
    /// Tracks which shadow collections have been created (keyed by "branch_id/collection_name").
    /// Prevents repeated `create_system_collection` calls on every write.
    shadow_collections_created: std::sync::Mutex<std::collections::HashSet<String>>,
}

#[cfg(feature = "embed")]
impl Default for AutoEmbedState {
    fn default() -> Self {
        Self {
            shadow_collections_created: std::sync::Mutex::new(std::collections::HashSet::new()),
        }
    }
}

#[cfg(feature = "embed")]
impl AutoEmbedState {
    fn contains(&self, key: &str) -> bool {
        self.shadow_collections_created
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains(key)
    }

    fn insert(&self, key: String) {
        self.shadow_collections_created
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(key);
    }
}

// ---------------------------------------------------------------------------
// Write-behind embed buffer
// ---------------------------------------------------------------------------

/// A pending embedding that has been buffered but not yet computed.
#[cfg(feature = "embed")]
struct PendingEmbed {
    branch_id: strata_core::types::BranchId,
    space: String,
    shadow_collection: &'static str,
    key: String,
    text: String,
    source_ref: strata_core::EntityRef,
}

/// Write-behind buffer for embedding requests.
///
/// Stored as a `Database` extension (`db.extension::<EmbedBuffer>()`).
/// Pending items accumulate until `batch_size` is reached (auto-flush) or
/// [`flush_embed_buffer()`] is called (manual flush on `db.flush()`).
#[cfg(feature = "embed")]
pub struct EmbedBuffer {
    pending: std::sync::Mutex<Vec<PendingEmbed>>,
    /// Serializes flush operations so only one `embed_batch` call runs at a
    /// time. The Metal GPU backend uses a shared deferred command buffer —
    /// concurrent `embed_batch` calls interleave operations and corrupt it.
    flush_lock: std::sync::Mutex<()>,
    /// Cumulative count of items pushed into the buffer.
    total_queued: std::sync::atomic::AtomicU64,
    /// Cumulative count of items successfully embedded.
    total_embedded: std::sync::atomic::AtomicU64,
    /// Cumulative count of items that failed embedding (model load / embed error).
    total_failed: std::sync::atomic::AtomicU64,
}

#[cfg(feature = "embed")]
impl Default for EmbedBuffer {
    fn default() -> Self {
        Self {
            pending: std::sync::Mutex::new(Vec::with_capacity(64)),
            flush_lock: std::sync::Mutex::new(()),
            total_queued: std::sync::atomic::AtomicU64::new(0),
            total_embedded: std::sync::atomic::AtomicU64::new(0),
            total_failed: std::sync::atomic::AtomicU64::new(0),
        }
    }
}

/// Buffer a text for embedding in a shadow vector collection.
///
/// Best-effort: failures are logged, never propagated to the caller.
/// When the buffer reaches `batch_size`, a flush is submitted to the
/// background scheduler (non-blocking). Falls back to synchronous flush
/// if the scheduler rejects (backpressure/shutdown).
///
/// # Consistency
///
/// Because auto-flush is asynchronous, a `delete` that races with an
/// in-flight background flush may see the embedding inserted *after* the
/// delete. This matches Elasticsearch's NRT model: embeddings are
/// eventually consistent with the source data.
#[cfg(feature = "embed")]
pub fn maybe_embed_text(
    p: &Arc<Primitives>,
    branch_id: strata_core::types::BranchId,
    space: &str,
    shadow_collection: &'static str,
    key: &str,
    text: &str,
    source_ref: strata_core::EntityRef,
) {
    if !p.db.auto_embed_enabled() {
        return;
    }

    let buf = match p.db.extension::<EmbedBuffer>() {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!(target: "strata::embed", error = %e, "Failed to get embed buffer");
            return;
        }
    };

    let should_flush = {
        let mut pending = buf.pending.lock().unwrap_or_else(|e| e.into_inner());
        pending.push(PendingEmbed {
            branch_id,
            space: space.to_owned(),
            shadow_collection,
            key: key.to_owned(),
            text: text.to_owned(),
            source_ref,
        });
        buf.total_queued
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        pending.len() >= p.db.embed_batch_size()
    };

    if should_flush {
        let p_clone = Arc::clone(p);
        if p.db
            .scheduler()
            .submit(strata_engine::TaskPriority::Normal, move || {
                flush_embed_buffer(&p_clone)
            })
            .is_err()
        {
            // Backpressure or shutdown — fall back to synchronous flush
            flush_embed_buffer(p);
        }
    }
}

/// No-op when the embed feature is not compiled in.
#[cfg(not(feature = "embed"))]
pub fn maybe_embed_text(
    _p: &Arc<Primitives>,
    _branch_id: strata_core::types::BranchId,
    _space: &str,
    _shadow_collection: &str,
    _key: &str,
    _text: &str,
    _source_ref: strata_core::EntityRef,
) {
}

/// Flush all pending embeddings: compute vectors in batch and insert.
///
/// Serialized by `flush_lock` — only one `embed_batch` call runs at a time.
/// The Metal GPU backend uses a shared deferred command buffer that is not
/// safe for concurrent `embed_batch` calls (interleaved command encoding).
/// A second concurrent caller blocks until the first completes, then drains
/// whatever has accumulated in the buffer since the first caller's `mem::take`.
#[cfg(feature = "embed")]
pub fn flush_embed_buffer(p: &Arc<Primitives>) {
    use strata_intelligence::embed::EmbedModelState;

    let buf = match p.db.extension::<EmbedBuffer>() {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!(target: "strata::embed", error = %e, "Failed to get embed buffer for flush");
            return;
        }
    };

    // Serialize flush operations — only one embed_batch at a time.
    let _flush_guard = buf.flush_lock.lock().unwrap_or_else(|e| e.into_inner());

    // Atomically drain the buffer.
    let batch = {
        let mut pending = buf.pending.lock().unwrap_or_else(|e| e.into_inner());
        std::mem::take(&mut *pending)
    };

    if batch.is_empty() {
        return;
    }

    let batch_len = batch.len() as u64;

    // Load engine once for the whole batch.
    let model_dir = p.db.model_dir();
    let embed_state = match p.db.extension::<EmbedModelState>() {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(target: "strata::embed", error = %e, "Failed to get embed model state");
            buf.total_failed
                .fetch_add(batch_len, std::sync::atomic::Ordering::Relaxed);
            return;
        }
    };

    let engine = match embed_state.get_or_load(&model_dir) {
        Ok(e) => e,
        Err(e) => {
            tracing::warn!(target: "strata::embed", error = %e, "Failed to load embedding model");
            buf.total_failed
                .fetch_add(batch_len, std::sync::atomic::Ordering::Relaxed);
            return;
        }
    };

    // Compute all embeddings in one Rust call (back-to-back forward passes).
    let texts: Vec<&str> = batch.iter().map(|pe| pe.text.as_str()).collect();
    let embeddings = match engine.embed_batch(&texts) {
        Ok(e) => e,
        Err(e) => {
            tracing::warn!(target: "strata::embed", error = %e, "Batch embedding failed");
            buf.total_failed
                .fetch_add(batch_len, std::sync::atomic::Ordering::Relaxed);
            return;
        }
    };
    let count = batch.len();

    // Insert each embedding into its shadow collection.
    for (pe, embedding) in batch.into_iter().zip(embeddings.iter()) {
        ensure_shadow_collection(p, pe.branch_id, pe.shadow_collection);

        let composite_key = format!("{}{}{}", pe.space, SHADOW_KEY_SEP, pe.key);
        let metadata = serde_json::json!({
            "source_space": pe.space,
            "source_key": pe.key,
        });

        if let Err(e) = p.vector.system_insert_with_source(
            pe.branch_id,
            pe.shadow_collection,
            &composite_key,
            embedding,
            Some(metadata),
            pe.source_ref,
        ) {
            tracing::warn!(
                target: "strata::embed",
                collection = pe.shadow_collection,
                key = composite_key,
                error = %e,
                "Failed to insert embedding"
            );
        }
    }

    buf.total_embedded
        .fetch_add(batch_len, std::sync::atomic::Ordering::Relaxed);

    tracing::debug!(
        target: "strata::embed",
        count,
        "Flushed embed buffer"
    );
}

/// No-op when the embed feature is not compiled in.
#[cfg(not(feature = "embed"))]
pub fn flush_embed_buffer(_p: &Arc<Primitives>) {}

/// Return a snapshot of the embedding pipeline status.
#[cfg(feature = "embed")]
pub fn embed_status(p: &Arc<Primitives>) -> EmbedStatusInfo {
    let (pending, total_queued, total_embedded, total_failed) =
        match p.db.extension::<EmbedBuffer>() {
            Ok(buf) => {
                let pending = buf.pending.lock().unwrap_or_else(|e| e.into_inner()).len();
                let queued = buf.total_queued.load(std::sync::atomic::Ordering::Relaxed);
                let embedded = buf
                    .total_embedded
                    .load(std::sync::atomic::Ordering::Relaxed);
                let failed = buf.total_failed.load(std::sync::atomic::Ordering::Relaxed);
                (pending, queued, embedded, failed)
            }
            Err(_) => (0, 0, 0, 0),
        };

    let stats = p.db.scheduler().stats();

    EmbedStatusInfo {
        auto_embed: p.db.auto_embed_enabled(),
        batch_size: p.db.embed_batch_size(),
        pending,
        total_queued,
        total_embedded,
        total_failed,
        scheduler_queue_depth: stats.queue_depth,
        scheduler_active_tasks: stats.active_tasks,
    }
}

/// Returns default status when the embed feature is not compiled in.
#[cfg(not(feature = "embed"))]
pub fn embed_status(p: &Arc<Primitives>) -> EmbedStatusInfo {
    let stats = p.db.scheduler().stats();
    EmbedStatusInfo {
        auto_embed: false,
        batch_size: p.db.embed_batch_size(),
        pending: 0,
        total_queued: 0,
        total_embedded: 0,
        total_failed: 0,
        scheduler_queue_depth: stats.queue_depth,
        scheduler_active_tasks: stats.active_tasks,
    }
}

/// Remove a shadow embedding entry on delete.
///
/// Also drains any matching pending embed from the buffer to prevent a
/// ghost embedding being inserted after the delete (write-behind race).
///
/// Best-effort: failures are logged, never propagated to the caller.
#[cfg(feature = "embed")]
pub fn maybe_remove_embedding(
    p: &Arc<Primitives>,
    branch_id: strata_core::types::BranchId,
    space: &str,
    shadow_collection: &str,
    key: &str,
) {
    if !p.db.auto_embed_enabled() {
        return;
    }

    // Drain any buffered-but-not-yet-flushed embed for this key to prevent a
    // ghost embedding from being inserted after the delete.
    if let Ok(buf) = p.db.extension::<EmbedBuffer>() {
        let mut pending = buf.pending.lock().unwrap_or_else(|e| e.into_inner());
        pending.retain(|pe| {
            !(pe.branch_id == branch_id
                && pe.shadow_collection == shadow_collection
                && pe.space == space
                && pe.key == key)
        });
    }

    let composite_key = format!("{}{}{}", space, SHADOW_KEY_SEP, key);

    if let Err(e) = p
        .vector
        .system_delete(branch_id, shadow_collection, &composite_key)
    {
        // Collection or vector may not exist yet (no embeds were ever created), that's fine.
        if !e.is_not_found() {
            tracing::warn!(
                target: "strata::embed",
                collection = shadow_collection,
                key = composite_key,
                error = %e,
                "Failed to remove shadow embedding"
            );
        }
    }
}

/// No-op when the embed feature is not compiled in.
#[cfg(not(feature = "embed"))]
pub fn maybe_remove_embedding(
    _p: &Arc<Primitives>,
    _branch_id: strata_core::types::BranchId,
    _space: &str,
    _shadow_collection: &str,
    _key: &str,
) {
}

/// Extract embeddable text from a Value.
#[cfg(feature = "embed")]
pub fn extract_text(value: &strata_core::Value) -> Option<String> {
    strata_intelligence::embed::extract::extract_text(value)
}

/// No-op when the embed feature is not compiled in.
#[cfg(not(feature = "embed"))]
pub fn extract_text(_value: &strata_core::Value) -> Option<String> {
    None
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(all(test, feature = "embed"))]
mod tests {
    use super::*;
    use strata_core::types::BranchId;

    /// Helper: create a Primitives with auto_embed enabled.
    fn setup() -> Arc<Primitives> {
        let db = strata_engine::Database::cache().expect("open cache db");
        db.set_auto_embed(true);
        let p = Arc::new(Primitives::new(db));
        // Ensure the default branch exists.
        let _ = p.branch.create_branch(&BranchId::default().to_string());
        p
    }

    /// Helper: read current buffer length.
    fn buffer_len(p: &Arc<Primitives>) -> usize {
        let buf = p.db.extension::<EmbedBuffer>().unwrap();
        let pending = buf.pending.lock().unwrap();
        pending.len()
    }

    /// Helper: push N items to the embed buffer directly.
    fn push_n(p: &Arc<Primitives>, n: usize) {
        let branch_id = BranchId::default();
        for i in 0..n {
            maybe_embed_text(
                p,
                branch_id,
                "default",
                SHADOW_KV,
                &format!("key-{}", i),
                &format!("text for key {}", i),
                strata_core::EntityRef::kv(branch_id, &format!("key-{}", i)),
            );
        }
    }

    #[test]
    fn test_buffer_accumulates_items() {
        let p = setup();
        assert_eq!(buffer_len(&p), 0);

        push_n(&p, 5);
        assert_eq!(buffer_len(&p), 5);
    }

    #[test]
    fn test_auto_flush_at_batch_size() {
        let p = setup();

        let batch_size = p.db.embed_batch_size(); // default: 256

        // Push (batch_size - 1) items — should NOT trigger auto-flush.
        push_n(&p, batch_size - 1);
        // Buffer should hold (batch_size - 1) items. No flush triggered
        // because threshold not reached.
        assert_eq!(buffer_len(&p), batch_size - 1);

        // Push one more item — reaches batch_size, triggers async auto-flush.
        push_n(&p, 1);
        // Wait for the background flush task to complete.
        p.db.scheduler().drain();
        // The flush drains the buffer (even though model load fails, the
        // drain via mem::take already happened).
        assert_eq!(buffer_len(&p), 0);
    }

    #[test]
    fn test_manual_flush_drains_buffer() {
        let p = setup();

        push_n(&p, 10);
        assert_eq!(buffer_len(&p), 10);

        // Manual flush drains the buffer (model load will fail in test, but
        // the drain is the first operation).
        flush_embed_buffer(&p);
        assert_eq!(buffer_len(&p), 0);
    }

    #[test]
    fn test_flush_empty_buffer_is_noop() {
        let p = setup();
        assert_eq!(buffer_len(&p), 0);

        // Should not panic or error.
        flush_embed_buffer(&p);
        assert_eq!(buffer_len(&p), 0);
    }

    #[test]
    fn test_delete_removes_pending_embed_from_buffer() {
        let p = setup();
        let branch_id = BranchId::default();

        // Buffer 3 items with different keys.
        maybe_embed_text(
            &p,
            branch_id,
            "default",
            SHADOW_KV,
            "keep-1",
            "text one",
            strata_core::EntityRef::kv(branch_id, "keep-1"),
        );
        maybe_embed_text(
            &p,
            branch_id,
            "default",
            SHADOW_KV,
            "to-delete",
            "text two",
            strata_core::EntityRef::kv(branch_id, "to-delete"),
        );
        maybe_embed_text(
            &p,
            branch_id,
            "default",
            SHADOW_KV,
            "keep-2",
            "text three",
            strata_core::EntityRef::kv(branch_id, "keep-2"),
        );
        assert_eq!(buffer_len(&p), 3);

        // Delete the middle key — should remove it from the buffer.
        maybe_remove_embedding(&p, branch_id, "default", SHADOW_KV, "to-delete");
        assert_eq!(buffer_len(&p), 2);

        // Verify the correct items remain.
        let buf = p.db.extension::<EmbedBuffer>().unwrap();
        let pending = buf.pending.lock().unwrap();
        assert_eq!(pending[0].key, "keep-1");
        assert_eq!(pending[1].key, "keep-2");
    }

    #[test]
    fn test_delete_nonexistent_key_leaves_buffer_intact() {
        let p = setup();
        let branch_id = BranchId::default();

        push_n(&p, 3);
        assert_eq!(buffer_len(&p), 3);

        // Delete a key that's not in the buffer — buffer unchanged.
        maybe_remove_embedding(&p, branch_id, "default", SHADOW_KV, "no-such-key");
        assert_eq!(buffer_len(&p), 3);
    }

    #[test]
    fn test_delete_only_removes_matching_collection() {
        let p = setup();
        let branch_id = BranchId::default();

        // Buffer an item in SHADOW_KV.
        maybe_embed_text(
            &p,
            branch_id,
            "default",
            SHADOW_KV,
            "shared-key",
            "kv text",
            strata_core::EntityRef::kv(branch_id, "shared-key"),
        );
        // Buffer an item in SHADOW_JSON with the same key name.
        maybe_embed_text(
            &p,
            branch_id,
            "default",
            SHADOW_JSON,
            "shared-key",
            "json text",
            strata_core::EntityRef::json(branch_id, "shared-key"),
        );
        assert_eq!(buffer_len(&p), 2);

        // Delete from SHADOW_KV only — SHADOW_JSON entry should remain.
        maybe_remove_embedding(&p, branch_id, "default", SHADOW_KV, "shared-key");
        assert_eq!(buffer_len(&p), 1);

        let buf = p.db.extension::<EmbedBuffer>().unwrap();
        let pending = buf.pending.lock().unwrap();
        assert_eq!(pending[0].shadow_collection, SHADOW_JSON);
    }

    #[test]
    fn test_disabled_auto_embed_skips_buffering() {
        let p = setup();
        p.db.set_auto_embed(false);

        push_n(&p, 10);
        // Nothing buffered because auto_embed is disabled.
        assert_eq!(buffer_len(&p), 0);
    }

    #[test]
    fn test_executor_drop_flushes_buffer() {
        use crate::Executor;

        let db = strata_engine::Database::cache().expect("open cache db");
        db.set_auto_embed(true);
        let executor = Executor::new(db);

        // Ensure the default branch exists.
        executor.execute(crate::Command::Ping).expect("ping works");

        let branch_id = BranchId::default();
        let p = executor.primitives().clone();

        // Buffer some items.
        for i in 0..5 {
            maybe_embed_text(
                &p,
                branch_id,
                "default",
                SHADOW_KV,
                &format!("drop-key-{}", i),
                &format!("text {}", i),
                strata_core::EntityRef::kv(branch_id, &format!("drop-key-{}", i)),
            );
        }
        assert_eq!(buffer_len(&p), 5);

        // Drop the executor — should flush the buffer.
        drop(executor);

        // Buffer should be empty after drop (items drained, even if model
        // load fails the drain still happens).
        assert_eq!(buffer_len(&p), 0);
    }

    /// The 1-second NRT refresh timer should flush a partially-filled buffer.
    ///
    /// Pushes items well below batch_size, waits for the timer to tick,
    /// and verifies the buffer is drained.
    #[test]
    fn test_nrt_timer_flushes_partial_buffer() {
        use crate::Executor;

        let db = strata_engine::Database::cache().expect("open cache db");
        db.set_auto_embed(true);
        let executor = Executor::new(db);

        executor.execute(crate::Command::Ping).expect("ping works");

        let branch_id = BranchId::default();
        let p = executor.primitives().clone();

        // Buffer a few items — well below batch_size, so auto-flush does NOT trigger.
        for i in 0..3 {
            maybe_embed_text(
                &p,
                branch_id,
                "default",
                SHADOW_KV,
                &format!("nrt-key-{}", i),
                &format!("nrt text {}", i),
                strata_core::EntityRef::kv(branch_id, &format!("nrt-key-{}", i)),
            );
        }
        assert_eq!(buffer_len(&p), 3);

        // Wait for the 1-second refresh timer to fire + scheduler to process.
        std::thread::sleep(std::time::Duration::from_millis(1500));
        p.db.scheduler().drain();

        // Timer should have flushed the buffer (drain via mem::take happens
        // even though model load fails in test).
        assert_eq!(buffer_len(&p), 0);
    }

    /// `Command::Flush` must drain in-flight background embed tasks before
    /// the WAL flush, ensuring all embeddings are persisted.
    #[test]
    fn test_flush_command_drains_async_embeds() {
        use crate::Executor;

        let db = strata_engine::Database::cache().expect("open cache db");
        db.set_auto_embed(true);
        let executor = Executor::new(db);

        executor.execute(crate::Command::Ping).expect("ping works");

        let branch_id = BranchId::default();
        let p = executor.primitives().clone();
        let batch_size = p.db.embed_batch_size();

        // Fill the buffer to trigger an async auto-flush.
        for i in 0..batch_size {
            maybe_embed_text(
                &p,
                branch_id,
                "default",
                SHADOW_KV,
                &format!("flush-cmd-key-{}", i),
                &format!("text {}", i),
                strata_core::EntityRef::kv(branch_id, &format!("flush-cmd-key-{}", i)),
            );
        }

        // Command::Flush should drain the scheduler (waiting for the async
        // embed task) and then flush the WAL.
        executor
            .execute(crate::Command::Flush)
            .expect("flush works");

        // Buffer must be empty after Command::Flush.
        assert_eq!(buffer_len(&p), 0);
    }

    /// Executor::drop completes instantly (no 1s sleep delay) because the
    /// timer thread uses a condvar that is signaled on drop.
    #[test]
    fn test_executor_drop_is_fast() {
        use crate::Executor;

        let db = strata_engine::Database::cache().expect("open cache db");
        let executor = Executor::new(db);

        let start = std::time::Instant::now();
        drop(executor);
        let elapsed = start.elapsed();

        // Drop should complete well under 1 second. If the condvar is broken
        // and we fall back to waiting for thread::sleep(1s), this fails.
        assert!(
            elapsed < std::time::Duration::from_millis(500),
            "Executor::drop took {:?}, expected <500ms",
            elapsed
        );
    }

    #[test]
    fn test_embed_status_counters() {
        let p = setup();

        // Initial state: all counters zero.
        let status = embed_status(&p);
        assert!(status.auto_embed);
        assert_eq!(status.pending, 0);
        assert_eq!(status.total_queued, 0);
        assert_eq!(status.total_embedded, 0);
        assert_eq!(status.total_failed, 0);

        // Push N items — total_queued should increment, pending should match.
        push_n(&p, 10);
        let status = embed_status(&p);
        assert_eq!(status.total_queued, 10);
        assert_eq!(status.pending, 10);

        // Flush drains the buffer. Depending on whether the model is
        // installed, items end up as embedded or failed.
        flush_embed_buffer(&p);
        let status = embed_status(&p);
        assert_eq!(status.pending, 0);
        assert_eq!(status.total_queued, 10);
        // All items should be accounted for: either embedded or failed.
        assert_eq!(status.total_embedded + status.total_failed, 10);
    }

    #[test]
    fn test_embed_status_via_executor_command() {
        use crate::Executor;

        let db = strata_engine::Database::cache().expect("open cache db");
        db.set_auto_embed(true);
        let executor = Executor::new(db);

        executor.execute(crate::Command::Ping).expect("ping works");

        // Check initial status via Command.
        match executor
            .execute(crate::Command::EmbedStatus)
            .expect("embed_status works")
        {
            crate::Output::EmbedStatus(info) => {
                assert!(info.auto_embed);
                assert_eq!(info.total_queued, 0);
                assert_eq!(info.pending, 0);
            }
            other => panic!("Expected EmbedStatus, got {:?}", other),
        }

        // Push some items and check again.
        let p = executor.primitives().clone();
        push_n(&p, 5);

        match executor
            .execute(crate::Command::EmbedStatus)
            .expect("embed_status works")
        {
            crate::Output::EmbedStatus(info) => {
                assert_eq!(info.total_queued, 5);
                assert_eq!(info.pending, 5);
            }
            other => panic!("Expected EmbedStatus, got {:?}", other),
        }
    }
}

/// Ensure a shadow collection exists, swallowing AlreadyExists errors.
///
/// Uses a per-Database cache to avoid repeated creation attempts on every write.
///
/// # Race Condition Safety
///
/// The check-then-act on `state.shadow_collections_created` is intentionally
/// non-atomic. If two threads race past the `contains_key` fast-path
/// simultaneously, both will call `create_system_collection`. The first call
/// succeeds; the second returns `CollectionAlreadyExists`, which is caught and
/// treated identically to success (the cache entry is inserted). This makes
/// the pattern safe despite the race: the worst case is a redundant creation
/// attempt that is harmlessly swallowed.
#[cfg(feature = "embed")]
fn ensure_shadow_collection(
    p: &Arc<Primitives>,
    branch_id: strata_core::types::BranchId,
    name: &str,
) {
    use strata_core::primitives::vector::VectorConfig;

    let cache_key = format!("{:?}{}{}", branch_id.as_bytes(), SHADOW_KEY_SEP, name);
    let state = match p.db.extension::<AutoEmbedState>() {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(target: "strata::embed", error = %e, "Failed to get auto-embed state");
            return;
        }
    };

    // Fast path: already created in this process lifetime
    if state.contains(&cache_key) {
        return;
    }

    let config = VectorConfig::for_minilm();

    match p.vector.create_system_collection(branch_id, name, config) {
        Ok(_) => {
            tracing::info!(target: "strata::embed", collection = name, "Created shadow embedding collection");
            state.insert(cache_key);
        }
        Err(strata_engine::vector::VectorError::CollectionAlreadyExists { .. }) => {
            // Already exists from a previous process run — mark as created
            state.insert(cache_key);
        }
        Err(e) => {
            tracing::warn!(
                target: "strata::embed",
                collection = name,
                error = %e,
                "Failed to create shadow collection"
            );
        }
    }
}
