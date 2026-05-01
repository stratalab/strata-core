//! GC, maintenance, follower refresh, and shutdown.

use crate::{StrataError, StrataResult};
use std::sync::atomic::Ordering;
use std::time::Duration;
use strata_core::id::{CommitVersion, TxnId};
use strata_core::BranchId;
use strata_storage::durability::{ManifestError, ManifestManager};
use strata_storage::Key;
use tracing::{info, warn};

use super::refresh::{
    clear_persisted_follower_state, persist_follower_state, sync_path_parent, BlockReason,
    BlockedTxn, BlockedTxnState, FollowerStatus, PersistedFollowerState, PreparedRefresh,
    RefreshGuard, RefreshOutcome, UnblockError,
};
use super::{registry, Database, PersistenceMode};

/// Default deadline for [`Database::shutdown`] when no explicit deadline is given.
const DEFAULT_SHUTDOWN_DEADLINE: Duration = Duration::from_secs(30);

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
    ///
    /// Returns `(0, 0)` while shutdown is in progress or has completed (see
    /// [`Database::check_not_shutting_down`]).
    pub fn run_gc(&self) -> (u64, usize) {
        if self.check_not_shutting_down().is_err() {
            return (0, 0);
        }
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
    ///
    /// Returns `(0, 0, 0)` while shutdown is in progress or has completed
    /// (see [`Database::check_not_shutting_down`]).
    pub fn run_maintenance(&self) -> (u64, usize, usize) {
        if self.check_not_shutting_down().is_err() {
            return (0, 0, 0);
        }
        let (safe_point, pruned) = self.run_gc();
        let expired = self
            .storage
            .expire_ttl_keys(strata_core::Timestamp::now().as_micros());
        (safe_point, pruned, expired)
    }

    /// Prune old checkpoint snapshot files according to the configured
    /// `snapshot_retention.retain_count` (DG-015).
    ///
    /// Always retains:
    ///   - the `retain_count` newest snapshots, and
    ///   - the snapshot referenced by the live MANIFEST (recovery-critical).
    ///
    /// Best-effort: per-file delete failures are logged but do not abort the
    /// loop. The snapshots directory is fsynced once at the end if any file
    /// was actually removed. Returns the number of files deleted.
    ///
    /// Skipped (returns `Ok(0)`) for ephemeral mode, follower mode, and
    /// while shutdown is in progress.
    ///
    /// Called from `Database::checkpoint` after the new snapshot is durable.
    /// A future executor / CLI surface will expose this as an admin entry
    /// point — see `docs/design/durability/durability-storage-closure-epics.md`
    /// "What Comes After" section.
    pub(crate) fn prune_snapshots_once(&self) -> StrataResult<usize> {
        if self.check_not_shutting_down().is_err() {
            return Ok(0);
        }
        if self.persistence_mode == PersistenceMode::Ephemeral || self.follower {
            return Ok(0);
        }

        let snapshots_dir = self.data_dir.join("snapshots");
        if !snapshots_dir.exists() {
            return Ok(0);
        }

        let retain_count = self.config.read().snapshot_retention.retain_count.max(1);

        let manifest_path = self.data_dir.join("MANIFEST");
        let live_snapshot_id = if ManifestManager::exists(&manifest_path) {
            ManifestManager::load(manifest_path)
                .map_err(|e: ManifestError| {
                    StrataError::internal(format!(
                        "prune_snapshots: failed to load MANIFEST: {}",
                        e
                    ))
                })?
                .manifest()
                .snapshot_id
        } else {
            None
        };

        let snapshots =
            strata_storage::durability::list_snapshots(&snapshots_dir).map_err(|e| {
                StrataError::internal(format!(
                    "prune_snapshots: failed to list snapshots in {}: {}",
                    snapshots_dir.display(),
                    e
                ))
            })?;

        if snapshots.len() <= retain_count {
            return Ok(0);
        }

        let keep_from = snapshots.len().saturating_sub(retain_count);
        let mut deleted = 0usize;

        for (i, (id, path)) in snapshots.iter().enumerate() {
            if i >= keep_from {
                break;
            }
            if Some(*id) == live_snapshot_id {
                continue;
            }
            match std::fs::remove_file(path) {
                Ok(_) => deleted += 1,
                Err(e) => {
                    warn!(
                        target: "strata::durability",
                        snapshot_id = id,
                        path = ?path,
                        error = %e,
                        "Failed to delete pruned snapshot file"
                    );
                }
            }
        }

        if deleted > 0 {
            let dir_fd = std::fs::File::open(&snapshots_dir).map_err(|e| {
                StrataError::internal(format!(
                    "prune_snapshots: failed to open snapshots dir for fsync: {}",
                    e
                ))
            })?;
            dir_fd.sync_all().map_err(|e| {
                StrataError::internal(format!(
                    "prune_snapshots: failed to fsync snapshots dir: {}",
                    e
                ))
            })?;
            info!(
                target: "strata::durability",
                deleted,
                retained = retain_count,
                "Snapshot pruning complete"
            );
        }

        Ok(deleted)
    }

    /// Remove the per-branch commit lock after a branch is deleted.
    ///
    /// This prevents unbounded growth of the commit_locks map in the
    /// TransactionManager when branches are repeatedly created and deleted.
    /// Returns `true` if removed (or didn't exist), `false` if skipped
    /// because a concurrent commit is in-flight.
    ///
    /// Should be called from the post-commit cleanup phase after a logical
    /// branch delete has already committed.
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
    /// - `UnblockError::NotFollower` — this isn't a follower database.
    /// - `UnblockError::DatabaseClosed` — the database has been shut down;
    ///   admin skip is rejected to prevent a stale handle from mutating a
    ///   fresh instance's recovery artifacts after a reopen.
    /// - `UnblockError::Mismatch` — `txn_id` doesn't match the blocked
    ///   transaction's id.
    /// - `UnblockError::NotBlocked` — the follower isn't currently blocked.
    /// - `UnblockError::NotSkippable` — the blocked record's block reason
    ///   flags it as unsafe to skip.
    pub fn admin_skip_blocked_record(
        &self,
        txn_id: TxnId,
        reason: &str,
    ) -> Result<(), UnblockError> {
        if !self.follower {
            return Err(UnblockError::NotFollower);
        }
        // Admin skip writes the audit log, advances the watermark, and
        // bumps storage version — all mutating operations. Reject on a
        // closed handle so a stale `Arc<Database>` can't mutate a fresh
        // instance's recovery artifacts if one opens on the same path.
        if self.shutdown_complete.load(Ordering::Acquire) {
            return Err(UnblockError::DatabaseClosed);
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
                .and_then(|mut f| {
                    std::io::Write::write_all(&mut f, entry.as_bytes())?;
                    f.sync_all()?;
                    sync_path_parent(&audit_path)
                })
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
    ///
    /// ## B5.1 retention contract
    ///
    /// This is the staged-publication spine for branch-visible
    /// derived state per
    /// `docs/design/branching/branching-gc/branching-b5-convergence-and-observability.md`
    /// §"Convergence classes" / "Staged-publish". Search, vector,
    /// and graph-search refresh hooks classify as
    /// `ConvergenceClass::StagedPublish` (see
    /// `branch_retention` crate-internal vocabulary): they validate
    /// and stage updates, block publication on hook
    /// failure, and advance visibility together with the underlying
    /// branch-visible storage version. The "block on any failure"
    /// rule above is the contract guarantee that no follower-visible
    /// publication occurs before the underlying branch version is
    /// visible.
    pub fn refresh(&self) -> RefreshOutcome {
        if !self.follower || self.persistence_mode == PersistenceMode::Ephemeral {
            return RefreshOutcome::CaughtUp {
                applied: 0,
                applied_through: TxnId::ZERO,
            };
        }

        // Close-barrier guard. A follower returns `Ok(())` from
        // `shutdown_with_deadline` after quiescing, but nothing in the
        // mutating follower APIs checked `shutdown_complete` before this
        // change. Post-shutdown `refresh()` would still touch the WAL and
        // watermark. Treat a closed follower as a no-op that reports the
        // last applied state, matching the non-follower branch above.
        if self.shutdown_complete.load(Ordering::Acquire) {
            return RefreshOutcome::CaughtUp {
                applied: 0,
                applied_through: self.watermark.applied(),
            };
        }

        // Acquire single-flight gate FIRST to prevent TOCTOU races on blocked state.
        let _guard = RefreshGuard::new(&self.refresh_gate);

        // Re-check shutdown_complete under the gate. D6 runs freeze hooks on
        // follower shutdown under the same gate; if this refresh was waiting
        // at the gate while shutdown ran its freeze + set shutdown_complete,
        // we must NOT proceed to mutate search/vector state that freeze has
        // already serialized to disk.
        if self.shutdown_complete.load(Ordering::Acquire) {
            return RefreshOutcome::CaughtUp {
                applied: 0,
                applied_through: self.watermark.applied(),
            };
        }

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
        // T3-E12 §D3 Site 2: thread the cached `wal_codec` so encrypted
        // followers decode records on refresh. Without this wire,
        // encrypted followers recover at open and then get stuck on
        // the first refresh with a codec-decode error on the first
        // new record (pre-T3-E12 part 4 behavior).
        //
        // D2 / DG-002: codec-decode, gap, checksum, and parse failures all
        // come back via `Ok(WatermarkReadResult { blocked: Some(..) })` so
        // the prefix of already-decoded records is preserved and the blocked
        // txn id is the real failing record. The stop-reason match further
        // below dispatches each class uniformly through `BlockReason`.
        //
        // Residual `Err(_)` cases — directory-list I/O, segment-open I/O,
        // legacy-format header — mean the reader failed before it could
        // identify a concrete blocked record. Refresh must still degrade into
        // the documented blocked-state path rather than panic the process, so
        // pin the follower at the next expected txn with the typed error
        // detail for operator visibility.
        let reader = strata_storage::durability::wal::WalReader::new().with_codec(
            strata_storage::durability::codec::clone_codec(self.wal_codec.as_ref()),
        );
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
            let payload = match strata_storage::durability::TransactionPayload::from_bytes(
                &record.writeset,
            ) {
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
            let deleted_values: Vec<(Key, strata_core::Value)> = payload
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
            let puts_slice: Vec<(Key, strata_core::Value)> = payload
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
                    BlockReason::PostApplyInvariant {
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
                strata_storage::durability::wal::ReadStopReason::Gap {
                    expected_txn_id,
                    observed_txn_id,
                } => BlockReason::Decode {
                    message: format!(
                        "missing WAL record at {}; next readable txn is {}",
                        expected_txn_id, observed_txn_id
                    ),
                },
                strata_storage::durability::wal::ReadStopReason::ChecksumMismatch { .. }
                | strata_storage::durability::wal::ReadStopReason::ParseError { .. }
                | strata_storage::durability::wal::ReadStopReason::CodecDecode { .. } => {
                    BlockReason::Decode { message: detail }
                }
                strata_storage::durability::wal::ReadStopReason::EndOfData
                | strata_storage::durability::wal::ReadStopReason::PartialRecord => {
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
    ///
    /// Rejected on a closed database — the hook implementations write mmap
    /// files under `data_dir`, so letting a stale `Arc<Database>` rewrite
    /// them after a fresh `Database::open` on the same path would corrupt
    /// the new instance's recovery artifacts. Internal callers
    /// (`VectorSubsystem::freeze` invoked from `run_freeze_hooks`, and
    /// `Drop`'s fallback) only run while `shutdown_complete == false`, so
    /// the guard does not fire on the legitimate shutdown path.
    pub fn freeze_vector_heaps(&self) -> StrataResult<()> {
        self.check_not_closed()?;
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

    /// Shut down the database using the default 30s deadline.
    ///
    /// Equivalent to [`Database::shutdown_with_deadline`] with the default deadline.
    pub fn shutdown(&self) -> StrataResult<()> {
        self.shutdown_with_deadline(DEFAULT_SHUTDOWN_DEADLINE)
    }

    /// Authoritative ordered close barrier (CLAUDE.md Hard Rule 11 / D-DR-10).
    ///
    /// Ordering on a successful close:
    /// 1. Flip `accepting_transactions = false` (new `begin_transaction` rejected).
    /// 2. Drain background scheduler (embed hooks etc.) so no new transactions
    ///    are spawned after the idle wait begins.
    /// 3. Wait for the coordinator to report zero active transactions, bounded
    ///    by `deadline`. On timeout, restore `shutdown_started = false` and
    ///    `accepting_transactions = true` so the database remains usable,
    ///    then return [`StrataError::ShutdownTimeout`] **without** stopping
    ///    the flush thread, flushing the WAL, fsyncing the MANIFEST, or
    ///    running subsystem freeze hooks. Callers may let their in-flight
    ///    transactions commit, optionally do more work, and retry.
    /// 4. Stop the background flush thread (final sync inside the thread).
    /// 5. Final `flush()` of the WAL.
    /// 6. Fsync the MANIFEST atomically (tmp + rename + parent dir fsync).
    /// 7. Run subsystem freeze hooks in reverse-insertion order. Freeze failure
    ///    is **not** treated as completion: the registry slot stays claimed,
    ///    `shutdown_complete` stays `false`, and the `Drop` fallback still
    ///    gets a chance to run. A retry is safe because every step is
    ///    idempotent.
    /// 8. On success, release the `OPEN_DATABASES` registry slot **and**
    ///    drop the exclusive `.lock` file so a fresh `Database::open` on
    ///    the same path succeeds immediately, without waiting for `Drop`
    ///    on the old `Arc<Database>`. Only the primary singleton
    ///    registers itself; followers intentionally skip the registry
    ///    step so their shutdown does not evict a live primary sharing
    ///    the same path.
    ///
    /// Calling `shutdown*` twice is idempotent: the second call returns
    /// `Ok(())` once `shutdown_complete` is set.
    pub fn shutdown_with_deadline(&self, deadline: Duration) -> StrataResult<()> {
        if self.shutdown_complete.load(Ordering::Acquire) {
            return Ok(());
        }
        self.shutdown_started.store(true, Ordering::Release);
        self.accepting_transactions.store(false, Ordering::Release);

        // Drain background scheduler before the idle wait so its tasks don't
        // spawn new transactions after `accepting_transactions = false` (they
        // run via internal paths that bypass that gate).
        self.scheduler.drain();

        // Authoritative barrier. Polls with SeqCst ordering; see #1738.
        if !self.coordinator.wait_for_idle(deadline) {
            let active_txn_count = self.coordinator.active_count();
            // Restore the pre-shutdown flags so the database is usable again.
            // The caller can let in-flight work drain, optionally start new
            // work, and retry. Every step up to this point is idempotent, so
            // a subsequent `shutdown*` call replays safely.
            //
            // `accepting_transactions = false` is dual-purpose: it also
            // carries the WAL writer-halt signal that the flush thread
            // publishes at `open.rs:756` after a sync failure. The halt
            // publisher runs in two phases — set `WalWriterHealth::Halted`
            // under `wal_writer_health.lock()`, release the lock, then store
            // `accepting_transactions = false`. To avoid a TOCTOU race where
            // the publisher completes between our observation and our
            // write, we hold `wal_writer_health.lock()` across the
            // check-and-restore: the publisher's first phase cannot advance
            // while we hold that lock, and once we release, any subsequent
            // halt runs its full sequence — its final `store(false)` then
            // wins over our `store(true)` because it is ordered after our
            // release.
            self.shutdown_started.store(false, Ordering::Release);
            let health = self.wal_writer_health.lock();
            if matches!(*health, super::WalWriterHealth::Healthy) {
                self.accepting_transactions.store(true, Ordering::Release);
            }
            drop(health);
            return Err(StrataError::ShutdownTimeout { active_txn_count });
        }

        // Followers have no background flush thread, no WAL writer, don't
        // own the MANIFEST, hold no registry slot (they skip `OPEN_DATABASES`
        // — see `Database::open_runtime_follower`), and hold no exclusive
        // file lock. They DO install subsystems (search, vector, etc.), so
        // their freeze hooks must run on close — bypassing them was DG-010.
        //
        // Acquire `RefreshGuard` before running freeze hooks so an in-flight
        // `refresh()` finishes first and no new refresh begins until we
        // release. Refresh re-checks `shutdown_complete` under the gate, so
        // the moment we set that flag below any queued refresh sees the
        // closed state and short-circuits. Without this, freeze and refresh
        // would both mutate search/vector state concurrently.
        //
        // Freeze failure is not treated as successful shutdown: leave
        // `shutdown_complete` unset so Drop's fallback runs and a retry
        // re-enters the freeze step.
        if self.follower {
            let _refresh_guard = super::refresh::RefreshGuard::new(&self.refresh_gate);
            self.run_freeze_hooks()?;
            self.shutdown_complete.store(true, Ordering::Release);
            return Ok(());
        }

        // Flush thread performs a final sync on exit (E-5).
        self.stop_flush_thread();
        // `self.flush()` now rejects once `shutdown_started = true`; use the
        // unguarded internal body so shutdown can still drive its own final
        // flush.
        self.flush_internal()?;
        self.fsync_manifest()?;

        // Freeze attempts all subsystems even if one fails; returns first err.
        // A freeze error is NOT a successful shutdown: leave `shutdown_complete`
        // as-is (so an explicit retry can re-run, and Drop's fallback path is
        // not suppressed) and do not release the registry slot.
        self.run_freeze_hooks()?;

        self.shutdown_complete.store(true, Ordering::Release);
        self.release_registry_slot();
        self.release_file_lock();
        Ok(())
    }

    /// Release the exclusive `.lock` file so a concurrent `Database::open`
    /// on the same path can acquire it. Dropping the `File` closes the fd,
    /// which releases the OS-level `flock`. Ephemeral databases have no
    /// file lock, and on retry `take()` returns `None` (idempotent).
    fn release_file_lock(&self) {
        drop(self.lock_file.lock().take());
    }

    /// Atomically persist the MANIFEST so it is durable before freeze hooks run.
    ///
    /// `ManifestManager::persist()` performs tmp-write → `sync_all` → rename →
    /// parent-dir fsync. Skipped for ephemeral databases (no data_dir).
    fn fsync_manifest(&self) -> StrataResult<()> {
        if self.data_dir.as_os_str().is_empty() {
            return Ok(());
        }
        let mut mgr = self.load_or_create_manifest()?;
        mgr.persist()
            .map_err(|e| StrataError::internal(format!("manifest fsync on shutdown failed: {}", e)))
    }

    /// Release this database's entry from the global `OPEN_DATABASES` registry.
    ///
    /// Only disk-backed primaries register themselves (followers skip the
    /// registry entirely), so callers must ensure this is only invoked on a
    /// successful primary shutdown.
    fn release_registry_slot(&self) {
        debug_assert!(!self.follower, "followers must not touch OPEN_DATABASES");
        if self.persistence_mode == PersistenceMode::Disk {
            registry::unregister(&self.data_dir);
        }
    }
}
