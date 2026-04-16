//! GC, maintenance, follower refresh, and shutdown.

use std::sync::atomic::Ordering;
use strata_core::id::{CommitVersion, TxnId};
use strata_core::traits::Storage;
use strata_core::types::{BranchId, Key};
use strata_core::StrataResult;
use tracing::{info, warn};

use super::refresh::{
    clear_persisted_follower_state, persist_follower_state, BlockReason, BlockedTxn,
    BlockedTxnState, FollowerStatus, PersistedFollowerState, PreparedRefresh, RefreshGuard,
    RefreshOutcome, UnblockError,
};
use super::{Database, PersistenceMode};

impl Database {
    // Branch Lifecycle Cleanup
    // ========================================================================

    /// Garbage-collect old versions before the given version number.
    ///
    /// Removes old versions from version chains across all entries in the branch.
    /// Returns the number of pruned versions.
    pub fn gc_versions_before(&self, branch_id: BranchId, min_version: CommitVersion) -> usize {
        self.storage.gc_branch(&branch_id, min_version)
    }

    /// Get the current global version from the coordinator.
    ///
    /// This is the highest version allocated so far and serves as
    /// a safe GC boundary when no active snapshots need older versions.
    pub fn current_version(&self) -> CommitVersion {
        self.coordinator.current_version()
    }

    /// Compute the GC safe point — the oldest version that can be pruned.
    ///
    /// `safe_point = min(current_version - 1, min_active_version)`
    ///
    /// Versions strictly older than `safe_point` can be pruned from version
    /// chains. Returns 0 if no GC is safe (only one version or version 0).
    pub fn gc_safe_point(&self) -> u64 {
        let current = self.current_version().as_u64();
        if current <= 1 {
            return 0;
        }
        let version_bound = current - 1;
        match self.coordinator.min_active_version() {
            Some(min_active) => version_bound.min(min_active),
            None => {
                // No drain has occurred yet (gc_safe_version == 0 sentinel).
                // If there are active transactions, they may hold snapshots
                // at any version — block GC entirely until the first drain.
                if self.coordinator.active_count() > 0 {
                    return 0;
                }
                version_bound
            }
        }
    }

    /// Run garbage collection across all branches using the computed safe point.
    ///
    /// This is NOT called automatically — it must be invoked explicitly.
    /// A future PR will add a background maintenance thread.
    ///
    /// Returns `(safe_point, total_versions_pruned)`.
    pub fn run_gc(&self) -> (u64, usize) {
        let safe_point = self.gc_safe_point();
        if safe_point == 0 {
            return (0, 0);
        }

        let mut total_pruned = 0;
        for branch_id in self.storage.branch_ids() {
            total_pruned += self
                .storage
                .gc_branch(&branch_id, CommitVersion(safe_point));
        }

        if total_pruned > 0 {
            tracing::debug!(pruned = total_pruned, safe_point, "GC cycle complete");
        }

        (safe_point, total_pruned)
    }

    /// Run a full maintenance cycle: GC + TTL expiration.
    ///
    /// This is NOT called automatically. It must be invoked explicitly
    /// (e.g., from tests, CLI, or a future background thread).
    ///
    /// Returns `(safe_point, versions_pruned, ttl_entries_expired)`.
    pub fn run_maintenance(&self) -> (u64, usize, usize) {
        let (safe_point, pruned) = self.run_gc();
        let expired = self
            .storage
            .expire_ttl_keys(strata_core::Timestamp::now().as_micros());
        (safe_point, pruned, expired)
    }

    /// Remove the per-branch commit lock after a branch is deleted.
    ///
    /// This prevents unbounded growth of the commit_locks map in the
    /// TransactionManager when branches are repeatedly created and deleted.
    /// Returns `true` if removed (or didn't exist), `false` if skipped
    /// because a concurrent commit is in-flight.
    ///
    /// Should be called after `BranchIndex::delete_branch()` succeeds.
    pub fn remove_branch_lock(&self, branch_id: &BranchId) -> bool {
        self.coordinator.remove_branch_lock(branch_id)
    }

    // ========================================================================
    // Follower Refresh
    // ========================================================================

    /// Get the current status of this follower database.
    ///
    /// Returns information about watermarks, blocked state, and refresh progress.
    /// For non-follower databases, returns a status with zero watermarks.
    pub fn follower_status(&self) -> FollowerStatus {
        FollowerStatus {
            received_watermark: self.watermark.received(),
            applied_watermark: self.watermark.applied(),
            blocked_at: self.watermark.blocked(),
            refresh_in_progress: self.refresh_gate.is_in_progress(),
        }
    }

    fn persist_blocked_follower_state(&self, blocked: &BlockedTxnState) {
        let state = PersistedFollowerState {
            received_watermark: self.watermark.received(),
            applied_watermark: self.watermark.applied(),
            visible_version: CommitVersion(self.storage.version()),
            blocked: blocked.clone(),
        };
        if let Err(e) = persist_follower_state(self.data_dir(), &state) {
            warn!(
                target: "strata::follower::audit",
                path = ?self.data_dir().join(super::refresh::FOLLOWER_STATE_FILE),
                error = %e,
                "Failed to persist blocked follower state"
            );
        }
    }

    fn clear_blocked_follower_state_file(&self) {
        if let Err(e) = clear_persisted_follower_state(self.data_dir()) {
            warn!(
                target: "strata::follower::audit",
                path = ?self.data_dir().join(super::refresh::FOLLOWER_STATE_FILE),
                error = %e,
                "Failed to clear persisted follower state"
            );
        }
    }

    /// Skip a blocked WAL record after manual intervention.
    ///
    /// This is an administrative operation for when a follower is stuck on a
    /// problematic WAL record that cannot be processed. The operator must:
    /// 1. Diagnose the issue using `follower_status().blocked_at`
    /// 2. Manually remediate any downstream state if needed
    /// 3. Call this method with the exact blocked transaction ID
    ///
    /// # Audit Trail
    ///
    /// This operation is logged to both:
    /// - `tracing` target `strata::follower::audit`
    /// - `{data_dir}/follower_audit.log` (durable, append-only)
    ///
    /// # Errors
    ///
    /// Returns `UnblockError::Mismatch` if `txn_id` doesn't match the blocked
    /// transaction, `UnblockError::NotBlocked` if the follower isn't blocked,
    /// or `UnblockError::NotFollower` if this isn't a follower database.
    pub fn admin_skip_blocked_record(
        &self,
        txn_id: TxnId,
        reason: &str,
    ) -> Result<(), UnblockError> {
        if !self.follower {
            return Err(UnblockError::NotFollower);
        }

        let blocked = self.watermark.unblock_exact(txn_id)?;
        if let Some(version) = blocked.visibility_version {
            let _publish_guard = self.refresh_publish_guard();
            self.storage.advance_version(version);
            self.coordinator.catch_up_version(version.as_u64());
        }
        self.coordinator.catch_up_txn_id(txn_id.as_u64());
        self.clear_blocked_follower_state_file();

        // Log to tracing
        info!(
            target: "strata::follower::audit",
            txn_id = %txn_id,
            reason = %reason,
            "Admin skipped blocked WAL record"
        );

        // Write durable audit log
        let data_dir = self.data_dir();
        if !data_dir.as_os_str().is_empty() {
            let audit_path = data_dir.join("follower_audit.log");
            let timestamp = chrono::Utc::now().to_rfc3339();
            let entry = format!(
                "{}\tSKIP\ttxn_id={}\treason={}\n",
                timestamp,
                txn_id.as_u64(),
                reason
            );
            if let Err(e) = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&audit_path)
                .and_then(|mut f| std::io::Write::write_all(&mut f, entry.as_bytes()))
            {
                warn!(
                    target: "strata::follower::audit",
                    path = ?audit_path,
                    error = %e,
                    "Failed to write follower audit log"
                );
            }
        }

        Ok(())
    }

    /// Refresh local storage by tailing new WAL entries from the primary.
    ///
    /// In follower mode, the primary process may have appended new WAL records
    /// since this instance last read the WAL. `refresh()` reads any new records
    /// and applies them to local in-memory storage, bringing this instance
    /// up-to-date with all committed writes.
    ///
    /// Returns a `RefreshOutcome` describing what happened:
    /// - `CaughtUp`: All available records were applied
    /// - `Stuck`: Some records applied, then hit a blocking error
    /// - `NoProgress`: Already blocked, no records applied
    ///
    /// For non-follower databases, returns `CaughtUp { applied: 0, applied_through: TxnId::ZERO }`.
    ///
    /// # Single-flight semantics
    ///
    /// Only one refresh can be in progress at a time per database. Concurrent
    /// calls serialize via the internal `RefreshGate`.
    ///
    /// # Blocking behavior
    ///
    /// Unlike the legacy best-effort refresh, this implementation blocks on
    /// any failure and does NOT advance the contiguous watermark past the
    /// failed record. This ensures:
    /// - `applied_watermark` is always truthful
    /// - Secondary indexes never silently fall behind
    /// - Operators can diagnose and skip blocked records explicitly
    pub fn refresh(&self) -> RefreshOutcome {
        if !self.follower || self.persistence_mode == PersistenceMode::Ephemeral {
            return RefreshOutcome::CaughtUp {
                applied: 0,
                applied_through: TxnId::ZERO,
            };
        }

        // Acquire single-flight gate FIRST to prevent TOCTOU races on blocked state.
        let _guard = RefreshGuard::new(&self.refresh_gate);

        // Now check if blocked (safe under gate protection)
        if let Some(blocked) = self.watermark.blocked() {
            return RefreshOutcome::NoProgress {
                applied_through: self.watermark.applied(),
                blocked_at: blocked,
            };
        }

        let received_watermark = self.watermark.received().as_u64();
        let make_blocked_state = |txn_id: TxnId,
                                  reason: BlockReason,
                                  visibility_version: Option<CommitVersion>,
                                  skip_allowed: bool| {
            BlockedTxnState {
                blocked: BlockedTxn { txn_id, reason },
                visibility_version,
                skip_allowed,
            }
        };

        // Read the longest contiguous WAL prefix after the received watermark.
        let reader = strata_durability::wal::WalReader::new();
        let read_result =
            match reader.read_all_after_watermark_contiguous(&self.wal_dir, received_watermark) {
                Ok(r) => r,
                Err(e) => {
                    let blocked = make_blocked_state(
                        TxnId(received_watermark.saturating_add(1)),
                        BlockReason::Decode {
                            message: format!("WAL read failed: {}", e),
                        },
                        None,
                        false,
                    );
                    self.watermark.block_at(blocked.clone());
                    self.persist_blocked_follower_state(&blocked);
                    return RefreshOutcome::Stuck {
                        applied: 0,
                        applied_through: self.watermark.applied(),
                        blocked_at: blocked.blocked,
                    };
                }
            };

        if read_result.records.is_empty() && read_result.blocked.is_none() {
            return RefreshOutcome::CaughtUp {
                applied: 0,
                applied_through: self.watermark.applied(),
            };
        }

        let mut applied = 0usize;
        let mut pending_publications: Vec<Box<dyn PreparedRefresh>> = Vec::new();

        // Get refresh hooks
        let refresh_hooks = self
            .extension::<super::refresh::RefreshHooks>()
            .ok()
            .map(|h| h.hooks())
            .unwrap_or_default();

        for record in &read_result.records {
            let txn_id = record.txn_id;

            // Update received watermark (telemetry)
            self.watermark.set_received(txn_id);

            // Decode the payload
            let payload = match strata_concurrency::TransactionPayload::from_bytes(&record.writeset)
            {
                Ok(p) => p,
                Err(e) => {
                    let blocked = make_blocked_state(
                        txn_id,
                        BlockReason::Decode {
                            message: format!("corrupt payload: {}", e),
                        },
                        None,
                        true,
                    );
                    self.watermark.block_at(blocked.clone());
                    self.persist_blocked_follower_state(&blocked);
                    return RefreshOutcome::Stuck {
                        applied,
                        applied_through: self.watermark.applied(),
                        blocked_at: blocked.blocked,
                    };
                }
            };

            // Pre-read state for delete hooks (must happen before storage mutations)
            let hook_pre_reads: Vec<Vec<(Key, Vec<u8>)>> = refresh_hooks
                .iter()
                .map(|hook| hook.pre_delete_read(self, &payload.deletes))
                .collect();
            let deleted_values: Vec<(Key, strata_core::value::Value)> = payload
                .deletes
                .iter()
                .filter_map(
                    |key| match self.storage.get_versioned(key, CommitVersion::MAX) {
                        Ok(Some(vv)) => Some((key.clone(), vv.value)),
                        Ok(None) => None,
                        Err(_) => None,
                    },
                )
                .collect();

            // Apply puts and deletes atomically
            {
                let writes: Vec<_> = payload
                    .puts
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                let deletes: Vec<_> = payload.deletes.to_vec();
                if let Err(e) = self.storage.apply_recovery_atomic(
                    writes,
                    deletes,
                    CommitVersion(payload.version),
                    record.timestamp,
                    &payload.put_ttls,
                ) {
                    let blocked = make_blocked_state(
                        txn_id,
                        BlockReason::StorageApply {
                            message: format!("{}", e),
                        },
                        None,
                        true,
                    );
                    self.watermark.block_at(blocked.clone());
                    self.persist_blocked_follower_state(&blocked);
                    return RefreshOutcome::Stuck {
                        applied,
                        applied_through: self.watermark.applied(),
                        blocked_at: blocked.blocked,
                    };
                }
            }

            // --- Update refresh hooks (vector backends, search index, etc.) ---
            let puts_slice: Vec<(Key, strata_core::value::Value)> = payload
                .puts
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            for (hook, pre_reads) in refresh_hooks.iter().zip(hook_pre_reads.iter()) {
                match hook.apply_refresh(&puts_slice, pre_reads) {
                    Ok(pending) => pending_publications.push(pending),
                    Err(e) => {
                        let blocked = make_blocked_state(
                            txn_id,
                            BlockReason::SecondaryIndex {
                                hook_name: e.hook_name.clone(),
                                message: e.message.clone(),
                            },
                            Some(CommitVersion(payload.version)),
                            true,
                        );
                        self.watermark.block_at(blocked.clone());
                        self.persist_blocked_follower_state(&blocked);
                        return RefreshOutcome::Stuck {
                            applied,
                            applied_through: self.watermark.applied(),
                            blocked_at: blocked.blocked,
                        };
                    }
                }
            }

            if let Err(e) = self.watermark.try_advance(txn_id) {
                let blocked = make_blocked_state(
                    txn_id,
                    BlockReason::Decode {
                        message: format!("watermark advancement failed: {}", e),
                    },
                    Some(CommitVersion(payload.version)),
                    false,
                );
                self.watermark.block_at(blocked.clone());
                self.persist_blocked_follower_state(&blocked);
                return RefreshOutcome::Stuck {
                    applied,
                    applied_through: self.watermark.applied(),
                    blocked_at: blocked.blocked,
                };
            }

            // Publish staged derived-state changes and advance visibility as one
            // synchronized handoff to readers.
            {
                let _publish_guard = self.refresh_publish_guard();
                for pending in pending_publications.drain(..) {
                    pending.publish();
                }
                self.storage.advance_version(CommitVersion(payload.version));
                self.coordinator.catch_up_version(payload.version);
                self.coordinator.catch_up_txn_id(txn_id.as_u64());
            }

            // Notify replay observers (best-effort, errors logged not propagated).
            let branch_id = payload
                .puts
                .first()
                .map(|(k, _)| k.namespace.branch_id)
                .or_else(|| payload.deletes.first().map(|k| k.namespace.branch_id))
                .unwrap_or(BranchId::new());
            let entry_count = payload.puts.len() + payload.deletes.len();
            let info = super::observers::ReplayInfo {
                branch_id,
                commit_version: CommitVersion(payload.version),
                entry_count,
                puts: puts_slice,
                deleted_values,
            };
            self.replay_observers().notify(&info);

            applied += 1;
        }

        if let Some(blocked) = read_result.blocked {
            let txn_id = blocked.txn_id;
            let skip_allowed = blocked.skip_allowed;
            let detail = blocked.detail;
            let reason = match blocked.stop_reason {
                strata_durability::wal::ReadStopReason::Gap {
                    expected_txn_id,
                    observed_txn_id,
                } => BlockReason::Decode {
                    message: format!(
                        "missing WAL record at {}; next readable txn is {}",
                        expected_txn_id, observed_txn_id
                    ),
                },
                strata_durability::wal::ReadStopReason::ChecksumMismatch { .. }
                | strata_durability::wal::ReadStopReason::ParseError { .. } => {
                    BlockReason::Decode { message: detail }
                }
                strata_durability::wal::ReadStopReason::EndOfData
                | strata_durability::wal::ReadStopReason::PartialRecord => {
                    BlockReason::Decode { message: detail }
                }
            };
            let blocked = make_blocked_state(txn_id, reason, None, skip_allowed);
            self.watermark.block_at(blocked.clone());
            self.persist_blocked_follower_state(&blocked);
            return RefreshOutcome::Stuck {
                applied,
                applied_through: self.watermark.applied(),
                blocked_at: blocked.blocked,
            };
        }

        self.clear_blocked_follower_state_file();
        RefreshOutcome::CaughtUp {
            applied,
            applied_through: self.watermark.applied(),
        }
    }

    // Graceful Shutdown
    // ========================================================================

    /// Graceful shutdown - ensures all data is persisted
    ///
    /// Freeze all vector heaps to mmap files for crash-safe recovery.
    ///
    /// With lite KV records (embedding stripped), the mmap cache is required
    /// for the next recovery to reconstruct embeddings. This is called during
    /// shutdown and drop.
    /// Freeze all registered refresh hooks' state to disk.
    pub fn freeze_vector_heaps(&self) -> StrataResult<()> {
        if let Ok(hooks) = self.extension::<super::refresh::RefreshHooks>() {
            for hook in hooks.hooks() {
                hook.freeze_to_disk(self)?;
            }
        }
        Ok(())
    }

    /// Freeze the search index to disk for fast recovery on next open.
    pub(crate) fn freeze_search_index(&self) -> StrataResult<()> {
        let data_dir = self.data_dir();
        if data_dir.as_os_str().is_empty() {
            return Ok(()); // Ephemeral database — no persistence
        }

        if let Ok(index) = self.extension::<crate::search::InvertedIndex>() {
            index.freeze_to_disk()?;
        }
        Ok(())
    }

    /// This method:
    /// 1. Stops accepting new transactions
    /// 2. Waits for pending operations to complete
    /// 3. Flushes WAL based on durability mode
    ///
    /// # Example
    ///
    /// ```text
    /// db.shutdown()?;
    /// assert!(!db.is_open());
    /// ```
    pub fn shutdown(&self) -> StrataResult<()> {
        self.shutdown_started.store(true, Ordering::Release);

        // 1. Stop accepting new transactions
        self.accepting_transactions.store(false, Ordering::Release);

        // Followers have no background threads, WAL writer, or freeze targets.
        if self.follower {
            return Ok(());
        }

        // 2. Drain background tasks (embeddings etc.) before final WAL flush
        self.scheduler.drain();

        // 3. Wait for in-flight transactions to complete FIRST.
        //    Uses wait_for_idle() which polls with SeqCst ordering, ensuring
        //    visibility of active_count increments from record_start() on all
        //    architectures (fixes #1738 / 9.2.B).
        let timeout = std::time::Duration::from_secs(30);
        if !self.coordinator.wait_for_idle(timeout) {
            let remaining = self.coordinator.active_count();
            warn!(
                target: "strata::db",
                remaining,
                "Shutdown timed out waiting for {} active transaction(s) after {:?}",
                remaining,
                timeout,
            );
        }

        // 4. Signal the background flush thread to stop (after transactions are drained)
        // (E-5: the thread performs a final sync before exiting)
        self.stop_flush_thread();

        // 5. Final flush to ensure all data is persisted
        self.flush()?;

        // 6. Freeze all registered subsystems. Attempts all even if one fails.
        let freeze_result = self.run_freeze_hooks();
        self.shutdown_complete.store(true, Ordering::Release);
        freeze_result?;

        Ok(())
    }
}
