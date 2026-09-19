//! Durable-local recovery orchestration.

use std::collections::BTreeSet;

use super::checkpoint::branch_durable_ranges_cover_interval;
#[cfg(debug_assertions)]
use super::checkpoint::branch_durable_rows_cover_interval;
use super::{
    preflight_table_manifest_with_checkpoint, require_generated_artifact_budget,
    LifecycleDurableLocalShell, LifecycleError, LifecycleLowerLayer, LifecycleResult,
    LifecycleTableManifestRecoveryOutcome, LifecycleTableManifestRecoveryStage,
    RecoveryDegradationClass, RecoveryFault, RecoveryFaultKind, RecoveryHealth, RecoveryStrictness,
    StorageBudgetLedger, StorageOpenPlan,
};
use crate::branch::state::snapshot::{
    install_snapshot_rows_into_branches, BranchSnapshotInstallOutcome, BranchSnapshotInstallRequest,
};
use crate::branch::state::BranchLocalState;
use crate::format::{
    decode_snapshot_row_payload, decode_snapshot_timeline_payload, encode_snapshot_row_section,
    FormatError, SnapshotContainer, SnapshotSection, SNAPSHOT_ROW_SECTION_KIND,
    SNAPSHOT_TIMELINE_SECTION_KIND, SNAPSHOT_TIMELINE_SECTION_KIND_LEGACY,
};
use crate::object::ObjectName;
use crate::row::StorageRow;
use crate::service::{
    QuarantineServiceError, SnapshotServiceError, WalRepair, WalServiceError, WalTruncation,
};
use crate::table::{ImmutableTableReader, TableCursor, TableIdentity, TableRow};
use strata_core::{CommitVersion, Timestamp};

#[derive(Debug)]
pub(crate) struct LifecycleRecoveryRuntime<'shell, 'backend, S> {
    shell: &'shell mut LifecycleDurableLocalShell<'backend, S>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LifecycleRecoveryRequest {
    strictness: RecoveryStrictness,
    max_faults: usize,
    max_snapshot_sections: usize,
    checkpoint_identity_seed: TableIdentity,
    table_object_references: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LifecycleRecoveryOutcome {
    health: RecoveryHealth,
    checkpoint: LifecycleRecoveredCheckpoint,
    wal: LifecycleRecoveredWal,
    quarantine: LifecycleRecoveredQuarantine,
    tables: LifecycleRecoveredTables,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LifecycleRecoveredCheckpoint {
    snapshot_id: Option<u64>,
    trusted_watermark: Option<CommitVersion>,
    section_count: usize,
    row_count: usize,
    install_outcome: Option<BranchSnapshotInstallOutcome>,
    // Checkpoint rows whose `branch_id` does not match the shell's seeded
    // branch. They are decoded here but installed post-catalog-build by
    // `bootstrap::install_non_seeded_checkpoint_rows`, which routes each
    // row to its catalog slot. Empty for single-branch checkpoints and
    // any catalog where the seeded branch is the only one with checkpoint
    // coverage.
    non_seeded_rows: Vec<StorageRow>,
    // W3.1b: every branch's retained-timeline group from the snapshot's
    // timeline section (validated ≤ watermark at decode). The seeded branch
    // is seeded during `recover_checkpoint`; the rest are seeded post-
    // catalog-build alongside the non-seeded row install.
    timeline_groups: Vec<crate::format::SnapshotTimelineBranchGroup>,
    // Identity seed for L0 table materialization during post-catalog
    // install. Carried from the recovery request so the seeded and non-
    // seeded installs share the same derivation base.
    install_identity_seed: Option<TableIdentity>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LifecycleRecoveredWal {
    replay_start: CommitVersion,
    /// #2567 S3a: the contiguity fence when recovering past a lost
    /// table-manifest base — bootstrap replays only records at or below it.
    /// `None` replays the whole tail. The records themselves are STREAMED,
    /// never carried here (the whole-tail `Vec` was #3319's transient).
    replay_ceiling: Option<CommitVersion>,
    record_count: usize,
    truncation: Option<WalTruncation>,
    repair: Option<WalRepair>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LifecycleRecoveredQuarantine {
    object: Option<ObjectName>,
    present: bool,
    byte_count: u64,
    entry_count: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LifecycleRecoveredTables {
    validated_count: usize,
    table_manifest: LifecycleTableManifestRecoveryOutcome,
}

impl<'shell, 'backend, S> LifecycleRecoveryRuntime<'shell, 'backend, S> {
    pub(crate) fn new(shell: &'shell mut LifecycleDurableLocalShell<'backend, S>) -> Self {
        Self { shell }
    }

    pub(crate) fn recover(
        &mut self,
        request: &LifecycleRecoveryRequest,
    ) -> LifecycleResult<LifecycleRecoveryOutcome> {
        self.shell.admit_recovery_step()?;
        request.validate_against_plan(self.shell.open_plan())?;

        let mut faults = Vec::new();
        let (checkpoint, recovered_branch) = self.recover_checkpoint(request, &mut faults)?;
        let quarantine = self.recover_quarantine(request, &mut faults)?;
        let (tables, table_manifest_stage) = self.recover_tables(request, &mut faults)?;
        let trusted_flush_watermark = validate_flush_watermark_is_recoverable(
            self.shell.assembly_facts().manifest_flush_watermark(),
            &checkpoint,
            &tables,
            table_manifest_stage
                .as_ref()
                .map(LifecycleTableManifestRecoveryStage::staged_branch),
            request.strictness(),
            &mut faults,
            request.max_faults(),
        )?;
        // A delta checkpoint records the durable table-manifest base floor it sits on
        // (`flushed_through_commit_id`). The snapshot is an orphaned delta — needing a base
        // that is now missing — when the recorded base floor sits strictly below the snapshot
        // watermark (rows above the floor are in the snapshot; the base below it is gone), or
        // when the snapshot carries no rows of its own (everything was flushed into the base).
        // A self-contained full snapshot (watermark == floor, with its own rows) needs no base
        // and is left alone. Trusting an orphaned delta's watermark would install a
        // non-contiguous gap, so recover only the WAL-contiguous prefix instead (empty when the
        // base is unrecoverable).
        //
        // MULTI-BRANCH GAP (guarded upstream): `table_manifest_stage` is the SEEDED branch's only
        // and `flushed_through` is global, so a non-seeded branch whose table manifest is lost is
        // not checked here. The checkpoint is prevented from recording such a snapshot: the
        // synchronous and background paths defer while any non-seeded branch holds a durable base
        // (`non_seeded_branch_has_durable_base`), and the close-drain path defers whenever any
        // non-seeded branch exists at all (its collector is seeded-only, so a published snapshot
        // would also drop non-seeded WAL rows outright — see #2624), so those rows stay in the
        // WAL and a full replay recovers them. The per-branch detection
        // that would let the checkpoint run (lifting the guard) is the deferred fix. Guard test:
        // `multi_branch_checkpoint_defers_so_lost_non_seeded_manifest_recovers_cleanly`; fix plan:
        // multi-branch-orphaned-delta-recovery-gap.md.
        let orphaned_delta = match (
            checkpoint.trusted_watermark(),
            self.shell.assembly_facts().manifest_flush_watermark(),
        ) {
            (Some(snapshot_watermark), Some(flush_watermark)) if table_manifest_stage.is_none() => {
                flush_watermark < snapshot_watermark
                    || recovered_branch
                        .as_ref()
                        .is_some_and(BranchLocalState::is_empty)
            }
            _ => false,
        };
        if orphaned_delta {
            push_fault(
                &mut faults,
                request.max_faults(),
                RecoveryFaultKind::MissingTableManifestBase,
                "delta checkpoint table-manifest base is missing",
            )?;
        }
        let replay_start = if orphaned_delta {
            CommitVersion::ZERO
        } else {
            trusted_replay_start(checkpoint.trusted_watermark(), trusted_flush_watermark)
        };
        if self.shell.assembly_facts().wal_chain_missing_at_open() {
            // #2777: lossy assembly proceeded past a checkpoint-attested store
            // whose WAL chain was gone (strict refuses at assemble). Whatever
            // the log held above the checkpoint is unrecoverable — the loss is
            // recorded loudly rather than presenting a degraded store as
            // healthy.
            push_fault(
                &mut faults,
                request.max_faults(),
                RecoveryFaultKind::WalCommittedSuffixMissing,
                "checkpoint-attested WAL chain missing at open; recovering from checkpoint alone",
            )?;
        }
        let wal = self.recover_wal(
            request,
            replay_start,
            orphaned_delta,
            &checkpoint,
            trusted_flush_watermark,
            &mut faults,
        )?;
        let health = recovery_health_from_faults(request, faults)?;
        match (recovered_branch, table_manifest_stage) {
            (Some(recovered_branch), Some(stage)) => {
                // Recovery Protocol rule 9: when both a checkpoint and a table
                // manifest are present, COMBINE them — reconstruct owned levels
                // from the manifest and layer the checkpoint's not-yet-durable
                // rows (those the manifest does not cover) on top — rather than
                // choosing one source as authoritative. Exact byte duplicates at
                // the same internal key are accepted; divergent bytes fail. The
                // manifest is the durable base (lower owned level + retained-history
                // timestamp coverage); the delta carries the active/frozen rows
                // above the flush watermark into the active memtable, matching a
                // synchronous baseline and keeping reads active-first newest-wins.
                // This is what lets a bounded delta checkpoint recover without
                // losing manifest-resident owned rows.
                // BS4.5b: close always writes a delta checkpoint, so this checkpoint+manifest COMBINE
                // path runs on every durable reopen. When the delta is empty (the common clean
                // flush+close case) both calls short-circuit in O(1) — O(metadata) open. Only a
                // non-empty delta (unflushed rows at close) still scans the full manifest to build its
                // key set (O(dataset)); making that functional dedup O(metadata) needs a facts-based
                // point-lookup and is a deferred follow-up. preflight stays a release guard (unlike the
                // demoted per-table scans) so a checkpoint/manifest byte conflict is still caught.
                let _overlap = preflight_table_manifest_with_checkpoint(
                    &recovered_branch,
                    stage.staged_branch(),
                )?;
                let delta = checkpoint_delta_rows(&recovered_branch, stage.staged_branch())?;
                self.shell.apply_table_manifest_recovery(stage);
                if !delta.is_empty() {
                    self.shell
                        .branch_state_mut()
                        .append_committed_rows_atomically(delta)
                        .map_err(branch_error)?;
                }
                // #2853: the COMBINE swapped in the staged manifest branch,
                // discarding the checkpoint-install instance whose retained
                // timeline carried the snapshot's group — the ONLY surviving
                // carrier of (version→timestamp) facts whose rows are retired.
                // Re-seed the surviving instance; without this the seeded
                // branch's index later force-completes EMPTY and provably
                // retained coverage is denied.
                seed_branch_timeline_from_groups(
                    self.shell.branch_state(),
                    checkpoint.timeline_groups(),
                );
            }
            (Some(_), None) if orphaned_delta => {
                // The snapshot is a delta whose durable table-manifest base was lost; discard
                // it. The WAL-contiguous prefix (empty when the base is unrecoverable) is
                // replayed onto the empty branch state by the caller.
            }
            (Some(recovered_branch), None) => {
                *self.shell.branch_state_mut() = recovered_branch;
            }
            (None, Some(stage)) => {
                self.shell.apply_table_manifest_recovery(stage);
            }
            (None, None) => {}
        }

        Ok(LifecycleRecoveryOutcome {
            health,
            checkpoint,
            wal,
            quarantine,
            tables,
        })
    }

    fn recover_checkpoint(
        &mut self,
        request: &LifecycleRecoveryRequest,
        faults: &mut Vec<RecoveryFault>,
    ) -> LifecycleResult<(LifecycleRecoveredCheckpoint, Option<BranchLocalState>)> {
        let snapshot_id = self.shell.assembly_facts().manifest_snapshot_id();
        let snapshot_watermark = manifest_snapshot_watermark(self.shell.assembly_facts())?;
        match (snapshot_id, snapshot_watermark) {
            (None, None) => Ok((LifecycleRecoveredCheckpoint::empty(), None)),
            (Some(id), Some(watermark)) => {
                self.load_and_install_checkpoint(request, faults, id, watermark)
            }
            (Some(_), None) | (None, Some(_)) => Err(LifecycleError::RecoveryFailed {
                reason: "manifest snapshot id and watermark must be present together",
            }),
        }
    }

    fn load_and_install_checkpoint(
        &mut self,
        request: &LifecycleRecoveryRequest,
        faults: &mut Vec<RecoveryFault>,
        snapshot_id: u64,
        watermark: CommitVersion,
    ) -> LifecycleResult<(LifecycleRecoveredCheckpoint, Option<BranchLocalState>)> {
        if watermark == CommitVersion::ZERO {
            return Err(LifecycleError::RecoveryFailed {
                reason: "manifest snapshot watermark must be nonzero",
            });
        }
        if snapshot_id == 0 {
            return Err(LifecycleError::RecoveryFailed {
                reason: "manifest snapshot id must be nonzero",
            });
        }

        let container = match self.shell.services().snapshot().load_required_for_codec(
            snapshot_id,
            *self.shell.assembly_facts().database_id(),
            self.shell.assembly_facts().codec_id(),
        ) {
            Ok(container) => container,
            Err(SnapshotServiceError::Missing { .. })
                if request.strictness == RecoveryStrictness::AllowExplicitLossyFallback =>
            {
                push_fault(
                    faults,
                    request.max_faults,
                    RecoveryFaultKind::MissingSnapshotObject,
                    "manifest-listed snapshot is missing",
                )?;
                return Ok((
                    LifecycleRecoveredCheckpoint::missing_lossy(snapshot_id),
                    None,
                ));
            }
            Err(SnapshotServiceError::Missing { .. }) => {
                // #2754: reached only when the manifest attests a snapshot (id +
                // watermark present) yet its objects are gone. Under strict
                // recovery on a quiescent single-writer directory that is
                // permanent loss, not a transient outage — refuse as
                // non-retryable recovery corruption instead of advising an
                // endless retry (mirrors the missing-manifest arm, #3015).
                return Err(LifecycleError::recovery_corruption(
                    "manifest attests a snapshot but its objects are missing",
                ));
            }
            Err(source) => {
                return Err(snapshot_error(source));
            }
        };

        validate_snapshot_watermark(&container, watermark)?;
        if container.sections().len() > request.max_snapshot_sections {
            return Err(LifecycleError::RecoveryFailed {
                reason: "snapshot section count exceeds lifecycle recovery limit",
            });
        }
        require_checkpoint_decode_budget(self.shell.budget(), container.sections())?;
        let rows = decode_checkpoint_rows(container.sections())?;
        validate_checkpoint_rows(watermark, &rows)?;
        let row_count = rows.len();
        let seeded_branch_id = self.shell.branch_state().branch_id();
        let (seeded_rows, non_seeded_rows): (Vec<_>, Vec<_>) = rows
            .into_iter()
            .partition(|row| row.physical_key().branch_id() == seeded_branch_id);
        let (recovered_branch, install_outcome) = install_checkpoint_rows(
            self.shell.branch_state().clone(),
            request.checkpoint_identity_seed(),
            seeded_rows,
        )?;
        // W3.1b: restore retained-timeline indexes from the snapshot's
        // timeline section (validated ≤ watermark). The seeded branch seeds
        // here; non-seeded branches seed post-catalog-build in bootstrap.
        // WAL-tail replay observations extend every seeded index, so reopen
        // never rescans the timeline space. An absent group leaves that
        // branch's index unseeded — the W3.1a scan fallback.
        let timeline_groups = decode_timeline_groups(container.sections(), watermark)?;
        if let Some(branch) = recovered_branch.as_ref() {
            seed_branch_timeline_from_groups(branch, &timeline_groups);
        }
        Ok((
            LifecycleRecoveredCheckpoint {
                snapshot_id: Some(snapshot_id),
                trusted_watermark: Some(watermark),
                section_count: container.sections().len(),
                row_count,
                install_outcome: Some(install_outcome),
                non_seeded_rows,
                timeline_groups,
                install_identity_seed: Some(request.checkpoint_identity_seed().clone()),
            },
            recovered_branch,
        ))
    }

    /// #2690: the durable commit watermark attests the highest commit version
    /// whose WAL record was sealed durable. Every attested commit must be
    /// reproducible from SOME recovery source — the checkpoint, the durable
    /// table manifest, or the surviving WAL records. If none reaches the
    /// watermark, WAL segments holding committed data were removed out of
    /// band; without this check recovery would silently reopen the store
    /// without them. The comparison is checkpoint-aware by construction (a
    /// dropped segment whose data the snapshot covers satisfies it), which is
    /// what the earlier segment-id marker could not express.
    fn verify_commit_watermark_recoverable(
        &mut self,
        request: &LifecycleRecoveryRequest,
        checkpoint: &LifecycleRecoveredCheckpoint,
        trusted_flush_watermark: Option<CommitVersion>,
        replay_start: CommitVersion,
        wal_max: u64,
        faults: &mut Vec<RecoveryFault>,
    ) -> LifecycleResult<()> {
        let Some(attested) = self.shell.services().wal().durable_commit_watermark() else {
            return Ok(());
        };
        let recoverable = [
            Some(replay_start.as_u64()),
            checkpoint.trusted_watermark().map(CommitVersion::as_u64),
            trusted_flush_watermark.map(CommitVersion::as_u64),
            Some(wal_max),
        ]
        .into_iter()
        .flatten()
        .max()
        .unwrap_or(0);
        if attested > recoverable {
            if request.strictness() == RecoveryStrictness::Strict {
                return Err(LifecycleError::recovery_corruption(
                    "durable WAL commit watermark attests commits above every recoverable source",
                ));
            }
            push_fault(
                faults,
                request.max_faults(),
                RecoveryFaultKind::WalCommittedSuffixMissing,
                "durable WAL commit watermark attests unrecoverable commits",
            )?;
        }
        Ok(())
    }

    fn recover_wal(
        &mut self,
        request: &LifecycleRecoveryRequest,
        replay_start: CommitVersion,
        require_contiguous: bool,
        checkpoint: &LifecycleRecoveredCheckpoint,
        trusted_flush_watermark: Option<CommitVersion>,
        faults: &mut Vec<RecoveryFault>,
    ) -> LifecycleResult<LifecycleRecoveredWal> {
        // #2567 S3a (#3319): a single STREAMING pass, retaining O(1) — the
        // whole-tail `Vec` (plus its contiguity `BTreeSet`) was the recovery
        // transient that tracked the unreclaimed WAL and OOM-killed the
        // 18 GiB field reopen. Replay itself streams again in bootstrap
        // (`replay_wal_into_catalog`), bounded by the ceiling computed here.
        let mut wal_max: u64 = 0;
        let mut contiguous_upper: u64 = replay_start.as_u64();
        let mut record_count: usize = 0;
        let truncation = self
            .shell
            .services()
            .wal()
            .visit_records_after(replay_start, &mut |record| {
                let version = record.commit_version().as_u64();
                wal_max = wal_max.max(version);
                // The replay stream is strictly ascending (bootstrap refuses
                // otherwise), so the contiguous run from `replay_start + 1`
                // is a single forward walk — no version set required. An
                // out-of-order tail computes a smaller fence and then fails
                // bootstrap's ordering check exactly as it always has.
                if contiguous_upper
                    .checked_add(1)
                    .is_some_and(|next| next == version)
                {
                    contiguous_upper = version;
                }
                record_count += 1;
                std::ops::ControlFlow::Continue(())
            })
            .map_err(wal_recovery_error)?;

        // Recovering past a lost table-manifest base: replay only the run of
        // commit versions contiguous from `replay_start + 1`, dropping any
        // orphaned tail above the first gap. Replaying a tail that sits above
        // the unrecoverable base would reintroduce the very gap recovery is
        // avoiding.
        let replay_ceiling = require_contiguous.then(|| CommitVersion::new(contiguous_upper));

        // The attestation backstop runs BEFORE any repair touches the tail: a
        // refusal must leave the torn bytes on disk for forensics, and the
        // surviving record set is identical before and after the repair (a
        // torn suffix never parses as a record). Under the contiguity fence
        // the attestation must see only the REPLAYED max — an orphaned tail
        // above the fence is dropped, so letting it satisfy the #2690
        // watermark would silently reopen without attested commits.
        let attestable_wal_max = match replay_ceiling {
            Some(ceiling) => wal_max.min(ceiling.as_u64()),
            None => wal_max,
        };
        self.verify_commit_watermark_recoverable(
            request,
            checkpoint,
            trusted_flush_watermark,
            replay_start,
            attestable_wal_max,
            faults,
        )?;

        let repair = match truncation.as_ref() {
            Some(truncation) => {
                // A torn FINAL record is the mid-append crash artifact: its
                // write never completed, so its covering sync never ran and no
                // ack was issued — the write-ordering contract keeps every
                // durable reference behind synced WAL bytes. Repairing it
                // loses nothing promised, so strict mode repairs it too and
                // records no fault; the commit-watermark verification that
                // follows recovery refuses (strict) if the repair dropped
                // anything attested. Lossy mode keeps its data-loss fault:
                // its damage simulations can tear acknowledged bytes.
                if request.strictness() != RecoveryStrictness::Strict {
                    push_fault(
                        faults,
                        request.max_faults(),
                        RecoveryFaultKind::WalTailRepairFailed,
                        "partial WAL tail repaired with data loss",
                    )?;
                }
                Some(
                    self.shell
                        .services_mut()
                        .wal_mut()
                        .repair_latest_tail(truncation)
                        .map_err(wal_repair_error)?,
                )
            }
            None => None,
        };

        Ok(LifecycleRecoveredWal {
            replay_start,
            replay_ceiling,
            record_count,
            truncation,
            repair,
        })
    }

    fn recover_tables(
        &mut self,
        request: &LifecycleRecoveryRequest,
        faults: &mut Vec<RecoveryFault>,
    ) -> LifecycleResult<(
        LifecycleRecoveredTables,
        Option<LifecycleTableManifestRecoveryStage>,
    )> {
        if request.table_object_references() != 0 {
            return Err(LifecycleError::RecoveryFailed {
                reason: "table object recovery references require a table manifest",
            });
        }
        let stage = match self.shell.stage_table_manifest_recovery() {
            Ok(stage) => stage,
            Err(error)
                if request.strictness() == RecoveryStrictness::AllowExplicitLossyFallback
                    && is_lossy_table_manifest_recovery_error(&error) =>
            {
                push_fault_for_branch(
                    faults,
                    request.max_faults(),
                    table_manifest_recovery_fault_kind(&error),
                    table_manifest_recovery_fault_reason(&error),
                    self.shell.branch_state().branch_id(),
                )?;
                return Ok((
                    LifecycleRecoveredTables {
                        validated_count: 0,
                        table_manifest: LifecycleTableManifestRecoveryOutcome::absent(),
                    },
                    None,
                ));
            }
            Err(error) => return Err(error),
        };
        let outcome = stage.outcome().clone();
        let table_manifest_stage = if outcome.manifest_sequence().is_some() {
            Some(stage)
        } else {
            None
        };
        Ok((
            LifecycleRecoveredTables {
                validated_count: 0,
                table_manifest: outcome,
            },
            table_manifest_stage,
        ))
    }

    fn recover_quarantine(
        &self,
        request: &LifecycleRecoveryRequest,
        faults: &mut Vec<RecoveryFault>,
    ) -> LifecycleResult<LifecycleRecoveredQuarantine> {
        let branch_id = self.shell.branch_state().branch_id();
        match self.shell.services().quarantine().load_inventory(
            branch_id,
            *self.shell.assembly_facts().database_id(),
            self.shell.assembly_facts().codec_id(),
        ) {
            Ok(load) => Ok(LifecycleRecoveredQuarantine {
                object: Some(load.object().clone()),
                present: load.is_present(),
                byte_count: load.byte_count(),
                entry_count: load.entry_count(),
            }),
            Err(source)
                if request.strictness == RecoveryStrictness::AllowExplicitLossyFallback
                    && is_quarantine_inventory_mismatch(&source) =>
            {
                let object = quarantine_error_object(&source);
                // The mismatch is scoped to this branch — attach the branch
                // id so downstream `safe_for_candidate` /
                // `fresh_for_candidate` admission checks can refuse
                // reclaim under Telemetry debt that names this branch.
                push_fault_for_branch(
                    faults,
                    request.max_faults,
                    RecoveryFaultKind::QuarantineInventoryMismatch,
                    "quarantine inventory mismatch",
                    branch_id,
                )?;
                Ok(LifecycleRecoveredQuarantine::unknown(object))
            }
            Err(source) => Err(quarantine_error(source)),
        }
    }
}

fn require_checkpoint_decode_budget(
    budget: &StorageBudgetLedger,
    sections: &[SnapshotSection],
) -> LifecycleResult<()> {
    let bytes = sections.iter().try_fold(0_u64, |total, section| {
        let payload_len = u64::try_from(section.payload().len()).map_err(|_| {
            LifecycleError::StorageBudgetExceeded {
                pool: super::StorageBudgetPool::GeneratedArtifact,
                requested_bytes: u64::MAX,
                used_bytes: total,
                limit_bytes: budget
                    .budget()
                    .pool_limit_bytes(super::StorageBudgetPool::GeneratedArtifact),
                requested_count: 0,
                used_count: 0,
                limit_count: None,
                reason: "checkpoint recovery decode byte count overflowed",
            }
        })?;
        total
            .checked_add(payload_len)
            .ok_or(LifecycleError::StorageBudgetExceeded {
                pool: super::StorageBudgetPool::GeneratedArtifact,
                requested_bytes: payload_len,
                used_bytes: total,
                limit_bytes: budget
                    .budget()
                    .pool_limit_bytes(super::StorageBudgetPool::GeneratedArtifact),
                requested_count: 0,
                used_count: 0,
                limit_count: None,
                reason: "checkpoint recovery decode byte count overflowed",
            })
    })?;
    require_generated_artifact_budget(
        budget,
        bytes,
        "checkpoint recovery decode exceeds storage budget",
    )
}

impl LifecycleRecoveryRequest {
    pub(crate) fn new(
        strictness: RecoveryStrictness,
        max_faults: usize,
        max_snapshot_sections: usize,
        checkpoint_identity_seed: impl Into<String>,
    ) -> LifecycleResult<Self> {
        let request = Self {
            strictness,
            max_faults,
            max_snapshot_sections,
            checkpoint_identity_seed: TableIdentity::new(checkpoint_identity_seed.into())
                .map_err(table_runtime_error)?,
            table_object_references: 0,
        };
        request.validate()?;
        Ok(request)
    }

    pub(crate) fn from_open_plan(plan: &StorageOpenPlan) -> LifecycleResult<Self> {
        Self::new(
            plan.recovery_policy(),
            plan.lifecycle_config().max_recovery_faults(),
            4096,
            "lifecycle-checkpoint",
        )
    }

    pub(crate) const fn strictness(&self) -> RecoveryStrictness {
        self.strictness
    }

    pub(crate) const fn max_faults(&self) -> usize {
        self.max_faults
    }

    pub(crate) const fn max_snapshot_sections(&self) -> usize {
        self.max_snapshot_sections
    }

    pub(crate) const fn checkpoint_identity_seed(&self) -> &TableIdentity {
        &self.checkpoint_identity_seed
    }

    #[allow(
        dead_code,
        reason = "table-object recovery references are introduced before table-backed checkpoint sections"
    )]
    pub(crate) const fn with_table_object_references(mut self, references: usize) -> Self {
        self.table_object_references = references;
        self
    }

    pub(crate) const fn table_object_references(&self) -> usize {
        self.table_object_references
    }

    fn validate(&self) -> LifecycleResult<()> {
        if self.max_faults == 0 {
            return Err(LifecycleError::InvalidConfig {
                field: "max_faults",
                reason: "must be nonzero",
            });
        }
        if self.max_snapshot_sections == 0 {
            return Err(LifecycleError::InvalidConfig {
                field: "max_snapshot_sections",
                reason: "must be nonzero",
            });
        }
        if self.table_object_references != 0 {
            return Err(LifecycleError::RecoveryFailed {
                reason: "table object recovery references require a table manifest",
            });
        }
        Ok(())
    }

    fn validate_against_plan(&self, plan: &StorageOpenPlan) -> LifecycleResult<()> {
        self.validate()?;
        if self.strictness == RecoveryStrictness::AllowExplicitLossyFallback
            && plan.recovery_policy() != RecoveryStrictness::AllowExplicitLossyFallback
        {
            return Err(LifecycleError::InvalidOpenPlan {
                reason: "lossy recovery request requires lossy open plan",
            });
        }
        Ok(())
    }
}

impl LifecycleRecoveryOutcome {
    pub(crate) const fn health(&self) -> &RecoveryHealth {
        &self.health
    }

    pub(crate) const fn checkpoint(&self) -> &LifecycleRecoveredCheckpoint {
        &self.checkpoint
    }

    /// See [`LifecycleRecoveredWal::set_replay_ceiling_for_test`].
    #[cfg(test)]
    pub(crate) fn wal_mut_for_test(&mut self) -> &mut LifecycleRecoveredWal {
        &mut self.wal
    }

    pub(crate) const fn wal(&self) -> &LifecycleRecoveredWal {
        &self.wal
    }

    pub(crate) const fn quarantine(&self) -> &LifecycleRecoveredQuarantine {
        &self.quarantine
    }

    pub(crate) const fn tables(&self) -> &LifecycleRecoveredTables {
        &self.tables
    }
}

impl LifecycleRecoveredCheckpoint {
    const fn empty() -> Self {
        Self {
            snapshot_id: None,
            trusted_watermark: None,
            section_count: 0,
            row_count: 0,
            install_outcome: None,
            non_seeded_rows: Vec::new(),
            timeline_groups: Vec::new(),
            install_identity_seed: None,
        }
    }

    const fn missing_lossy(snapshot_id: u64) -> Self {
        Self {
            snapshot_id: Some(snapshot_id),
            trusted_watermark: None,
            section_count: 0,
            row_count: 0,
            install_outcome: None,
            non_seeded_rows: Vec::new(),
            timeline_groups: Vec::new(),
            install_identity_seed: None,
        }
    }

    /// W3.1b: every branch's decoded timeline group (seeded branch already
    /// applied; non-seeded branches applied post-catalog-build).
    pub(crate) fn timeline_groups(&self) -> &[crate::format::SnapshotTimelineBranchGroup] {
        &self.timeline_groups
    }

    pub(crate) fn non_seeded_rows(&self) -> &[StorageRow] {
        &self.non_seeded_rows
    }

    pub(crate) fn install_identity_seed(&self) -> Option<&TableIdentity> {
        self.install_identity_seed.as_ref()
    }

    pub(crate) const fn snapshot_id(&self) -> Option<u64> {
        self.snapshot_id
    }

    pub(crate) const fn trusted_watermark(&self) -> Option<CommitVersion> {
        self.trusted_watermark
    }

    pub(crate) const fn section_count(&self) -> usize {
        self.section_count
    }

    pub(crate) const fn row_count(&self) -> usize {
        self.row_count
    }

    pub(crate) const fn install_outcome(&self) -> Option<&BranchSnapshotInstallOutcome> {
        self.install_outcome.as_ref()
    }

    pub(crate) fn timestamp_max(&self) -> Option<Timestamp> {
        self.install_outcome()
            .and_then(BranchSnapshotInstallOutcome::timestamp_max)
    }
}

impl LifecycleRecoveredWal {
    pub(crate) const fn replay_start(&self) -> CommitVersion {
        self.replay_start
    }

    pub(crate) const fn replay_ceiling(&self) -> Option<CommitVersion> {
        self.replay_ceiling
    }

    /// Lowers the fence so bootstrap's ceiling ENFORCEMENT is observable
    /// without fabricating a whole orphaned-delta store.
    #[cfg(test)]
    pub(crate) fn set_replay_ceiling_for_test(&mut self, ceiling: Option<CommitVersion>) {
        self.replay_ceiling = ceiling;
    }

    /// Collects exactly the records bootstrap will replay — verification
    /// only: production replay streams the tail (#3319) and must never
    /// materialize it.
    #[cfg(any(test, feature = "testkit"))]
    pub(crate) fn collect_replay_records_for_test(
        &self,
        wal: &crate::service::WalService<'_>,
    ) -> Result<Vec<crate::format::WalRecord>, crate::service::WalServiceError> {
        let mut records = Vec::new();
        wal.visit_records_after(self.replay_start, &mut |record| {
            if self
                .replay_ceiling
                .is_none_or(|ceiling| record.commit_version() <= ceiling)
            {
                records.push(record.clone());
            }
            std::ops::ControlFlow::Continue(())
        })?;
        Ok(records)
    }

    /// Records visited above `replay_start` — counted BEFORE the contiguity
    /// fence (an orphaned tail is visited, fenced, then dropped at replay).
    pub(crate) const fn record_count(&self) -> usize {
        self.record_count
    }

    pub(crate) const fn truncation(&self) -> Option<&WalTruncation> {
        self.truncation.as_ref()
    }

    pub(crate) const fn repair(&self) -> Option<&WalRepair> {
        self.repair.as_ref()
    }
}

impl LifecycleRecoveredQuarantine {
    const fn unknown(object: Option<ObjectName>) -> Self {
        Self {
            object,
            present: false,
            byte_count: 0,
            entry_count: 0,
        }
    }

    pub(crate) const fn object(&self) -> Option<&ObjectName> {
        self.object.as_ref()
    }

    pub(crate) const fn is_present(&self) -> bool {
        self.present
    }

    pub(crate) const fn byte_count(&self) -> u64 {
        self.byte_count
    }

    pub(crate) const fn entry_count(&self) -> usize {
        self.entry_count
    }
}

impl LifecycleRecoveredTables {
    pub(crate) const fn validated_count(&self) -> usize {
        self.validated_count
    }

    #[allow(
        dead_code,
        reason = "table-manifest recovery facts are consumed by lifecycle tests"
    )]
    pub(crate) const fn table_manifest(&self) -> &LifecycleTableManifestRecoveryOutcome {
        &self.table_manifest
    }
}

pub(crate) fn encode_checkpoint_row_section(
    rows: &[StorageRow],
) -> Result<SnapshotSection, FormatError> {
    encode_snapshot_row_section(rows)
}

/// W3.1b: decode every timeline group from the snapshot's timeline section,
/// failing closed when any entry exceeds the snapshot watermark (a corrupt or
/// mismatched section must not seed any index).
fn decode_timeline_groups(
    sections: &[SnapshotSection],
    watermark: CommitVersion,
) -> LifecycleResult<Vec<crate::format::SnapshotTimelineBranchGroup>> {
    let mut groups = Vec::new();
    for section in sections {
        // #3112 S2c: both the current section kind and the pre-`committed_at`
        // legacy kind are restorable; the decoder needs the kind because the
        // entry width differs.
        if section.section_kind() != SNAPSHOT_TIMELINE_SECTION_KIND
            && section.section_kind() != SNAPSHOT_TIMELINE_SECTION_KIND_LEGACY
        {
            continue;
        }
        let decoded = decode_snapshot_timeline_payload(section.payload(), section.section_kind())
            .map_err(format_error)?;
        for group in &decoded {
            if group
                .entries
                .last()
                .is_some_and(|entry| entry.commit_version.as_u64() > watermark.as_u64())
            {
                return Err(LifecycleError::RecoveryFailed {
                    reason: "timeline section entry exceeds snapshot watermark",
                });
            }
        }
        groups.extend(decoded);
    }
    Ok(groups)
}

/// W3.1c: the completeness INVARIANT — every branch leaves recovery with a
/// provably complete retained-timeline index. If the checkpoint section
/// already seeded it, this is a no-op. Otherwise seed from a one-time scan of
/// the branch's timeline-space rows: empty for a fresh branch (complete-empty
/// is exact), and the full pre-elision history for databases created before
/// W3.1c removed the rows (their rows still exist in tables). WAL-tail replay
/// observations extend the result. After this, the index is the timeline's
/// only read source — the scan never runs at read time again.
pub(crate) fn ensure_branch_timeline_complete(branch: &BranchLocalState) -> LifecycleResult<()> {
    if branch.retained_timeline().is_complete() {
        return Ok(());
    }
    let view = branch.capture_read_view().map_err(branch_error)?;
    let bounds = crate::branch::read::BranchScanBounds::unbounded(
        branch.branch_id(),
        crate::commit::COMMIT_TIMELINE_SPACE,
        crate::row::StorageSpaceId::COMMIT_TIMELINE,
    )
    .map_err(branch_error)?;
    let rows = view
        .scan_range_including_tombstones(&bounds, crate::branch::read::BranchReadBound::Latest)
        .map_err(branch_error)?;
    let timeline = crate::commit::CommitTimelineView::from_rows(
        branch.branch_id(),
        rows.iter().map(crate::branch::read::BranchHistoryRow::row),
    )
    .map_err(|source| {
        LifecycleError::lower_layer_with(
            crate::lifecycle::LifecycleLowerLayer::CommitRuntime,
            "commit runtime failed",
            source,
        )
    })?;
    let entries: Vec<crate::timeline_index::RetainedTimelineEntry> = timeline
        .entries_by_version()
        .iter()
        .map(|entry| {
            crate::timeline_index::RetainedTimelineEntry::new(
                entry.commit_version(),
                entry.commit_timestamp(),
            )
        })
        .collect::<Vec<_>>();
    branch.retained_timeline().seed_from_scan(&entries);
    Ok(())
}

/// Seed a branch's retained-timeline index from its decoded group, if present.
pub(crate) fn seed_branch_timeline_from_groups(
    branch: &BranchLocalState,
    groups: &[crate::format::SnapshotTimelineBranchGroup],
) {
    for group in groups {
        if group.branch_id != branch.branch_id() {
            continue;
        }
        let entries = group
            .entries
            .iter()
            .map(|entry| {
                // #3112 S2c: restore the persisted wall-clock instant with the
                // entry; a legacy kind-2 section yields None (unknown).
                crate::timeline_index::RetainedTimelineEntry::new(
                    entry.commit_version,
                    entry.commit_timestamp,
                )
                .with_committed_at(entry.committed_at)
            })
            .collect::<Vec<_>>();
        branch.retained_timeline().seed_from_scan(&entries);
    }
}

fn decode_checkpoint_rows(sections: &[SnapshotSection]) -> LifecycleResult<Vec<StorageRow>> {
    let mut rows = Vec::new();
    for section in sections {
        if section.section_kind() != SNAPSHOT_ROW_SECTION_KIND {
            continue;
        }
        rows.extend(decode_snapshot_row_payload(section.payload()).map_err(format_error)?);
    }
    Ok(rows)
}

fn install_checkpoint_rows(
    current_branch: BranchLocalState,
    identity_seed: &TableIdentity,
    rows: Vec<StorageRow>,
) -> LifecycleResult<(Option<BranchLocalState>, BranchSnapshotInstallOutcome)> {
    let branch_id = current_branch.branch_id();
    let mut branches = vec![current_branch];
    let request = BranchSnapshotInstallRequest::from_rows(identity_seed.as_str(), rows)
        .map_err(branch_error)?;
    let outcome =
        install_snapshot_rows_into_branches(&mut branches, &request).map_err(branch_error)?;
    let recovered_branch = branches
        .into_iter()
        .find(|branch| branch.branch_id() == branch_id);
    Ok((recovered_branch, outcome))
}

/// The checkpoint rows that the table manifest does not already cover, keyed by
/// internal key (physical key + commit version). For a full-superset checkpoint
/// this drops the owned-level rows the manifest also holds, leaving the
/// not-yet-durable active/frozen delta; for a bounded delta checkpoint it is
/// every row. Recovery installs this delta on top of the manifest-recovered
/// owned levels. Invariant (relied on for active-first newest-wins reads): every
/// delta row's commit version is above the manifest flush watermark, so a delta
/// row is strictly newer than any manifest-resident owned row at the same
/// physical key.
fn checkpoint_delta_rows(
    checkpoint_branch: &BranchLocalState,
    staged_manifest_branch: &BranchLocalState,
) -> LifecycleResult<Vec<StorageRow>> {
    // BS4.5b: a delta checkpoint holds only rows above the flush watermark; after a clean flush+close it
    // is empty. When it has no owned rows the delta is empty regardless of the manifest, so return early
    // and skip building the manifest key set — that scan is O(dataset) and, since close always writes a
    // (usually empty) delta checkpoint, would otherwise run on every reopen and defeat O(metadata) open.
    if checkpoint_branch
        .owned_levels()
        .iter()
        .flatten()
        .next()
        .is_none()
    {
        return Ok(Vec::new());
    }
    // BS4.4c: stream the owned tables through the cursor instead of materializing every row. The
    // manifest key set is now owned (cursor rows are transient), and the function becomes fallible.
    let mut manifest_keys = BTreeSet::<Vec<u8>>::new();
    for table in staged_manifest_branch.owned_levels().iter().flatten() {
        for_each_reader_row(table.reader(), |row| {
            manifest_keys.insert(row.key().as_slice().to_vec());
        })?;
    }
    let mut delta = Vec::new();
    for table in checkpoint_branch.owned_levels().iter().flatten() {
        for_each_reader_row(table.reader(), |row| {
            if !manifest_keys.contains(row.key().as_slice()) {
                delta.push(row.row().clone());
            }
        })?;
    }
    Ok(delta)
}

/// BS4.4c: walk a durable table's rows through its cursor (ascending internal-key order), applying `f`
/// per row without materializing the whole table. Cursor failures map to a recovery error.
/// C2: no-fill cursor (BS4.4g) — a one-shot recovery walk must not seed the
/// block cache in scan order; deliberate fill is the preheat's job.
fn for_each_reader_row(
    reader: &ImmutableTableReader<'_>,
    mut f: impl FnMut(&TableRow),
) -> LifecycleResult<()> {
    let mut cursor = reader.cursor_without_cache_fill();
    cursor
        .seek_to_first()
        .map_err(|_| checkpoint_delta_scan_failed())?;
    while let Some(row) = cursor.current() {
        f(row);
        cursor
            .advance()
            .map_err(|_| checkpoint_delta_scan_failed())?;
    }
    Ok(())
}

fn checkpoint_delta_scan_failed() -> LifecycleError {
    LifecycleError::RecoveryFailed {
        reason: "checkpoint delta cursor scan failed",
    }
}

fn trusted_replay_start(
    checkpoint_watermark: Option<CommitVersion>,
    flush_watermark: Option<CommitVersion>,
) -> CommitVersion {
    checkpoint_watermark
        .into_iter()
        .chain(flush_watermark)
        .max()
        .unwrap_or(CommitVersion::ZERO)
}

fn validate_flush_watermark_is_recoverable(
    flush_watermark: Option<CommitVersion>,
    checkpoint: &LifecycleRecoveredCheckpoint,
    tables: &LifecycleRecoveredTables,
    staged_table_manifest_branch: Option<&crate::branch::state::BranchLocalState>,
    strictness: RecoveryStrictness,
    faults: &mut Vec<RecoveryFault>,
    max_faults: usize,
) -> LifecycleResult<Option<CommitVersion>> {
    if let Some(flush_watermark) = flush_watermark {
        if checkpoint
            .trusted_watermark()
            .is_some_and(|watermark| flush_watermark <= watermark)
        {
            return Ok(Some(flush_watermark));
        }
        if table_manifest_covers_flush_watermark(
            flush_watermark,
            checkpoint.trusted_watermark(),
            tables,
            staged_table_manifest_branch,
        ) {
            return Ok(Some(flush_watermark));
        }
        if checkpoint.snapshot_id().is_some()
            && checkpoint.trusted_watermark().is_none()
            && strictness == RecoveryStrictness::AllowExplicitLossyFallback
        {
            push_fault(
                faults,
                max_faults,
                RecoveryFaultKind::MissingSnapshotObject,
                "manifest flush watermark lost with missing snapshot",
            )?;
            return Ok(None);
        }
        if checkpoint
            .trusted_watermark()
            .is_none_or(|watermark| flush_watermark > watermark)
        {
            return Err(LifecycleError::RecoveryFailed {
                reason: "manifest flush watermark requires recovered flushed table state",
            });
        }
    }
    Ok(None)
}

fn table_manifest_covers_flush_watermark(
    flush_watermark: CommitVersion,
    checkpoint_watermark: Option<CommitVersion>,
    tables: &LifecycleRecoveredTables,
    staged_branch: Option<&crate::branch::state::BranchLocalState>,
) -> bool {
    // O(1) facts check: the manifest's max durable commit version must reach the flush watermark.
    if tables
        .table_manifest()
        .install_outcome()
        .and_then(crate::branch::state::manifest_recovery::BranchTableManifestRecoveryOutcome::max_commit_version)
        .is_none_or(|max_commit_version| max_commit_version < flush_watermark)
    {
        return false;
    }
    let Some(branch) = staged_branch else {
        return false;
    };
    let checkpoint_watermark = checkpoint_watermark.unwrap_or(CommitVersion::ZERO);
    // BS4.5b: O(tables) release fail-safe — the durable tables' commit-range intervals must union-cover
    // (checkpoint_wm, flush_wm], catching an inter-table version gap (orphaned/partial durable state)
    // without a row scan. The exact per-version contiguity is additionally cross-checked under the debug
    // oracle, which also catches an intra-table gap the interval union cannot see. The old per-version
    // scan was O(dataset) at open when no checkpoint bounded the window.
    let ranges_cover = branch_durable_ranges_cover_interval(
        branch.owned_levels(),
        branch.inherited_layers(),
        checkpoint_watermark,
        flush_watermark,
    );
    #[cfg(debug_assertions)]
    if ranges_cover {
        debug_assert!(
            branch_durable_rows_cover_interval(
                branch.owned_levels(),
                branch.inherited_layers(),
                checkpoint_watermark,
                flush_watermark,
            ),
            "durable rows have an intra-table gap in the flush watermark interval despite range coverage",
        );
    }
    ranges_cover
}

fn manifest_snapshot_watermark(
    facts: &super::LifecycleDurableAssemblyFacts,
) -> LifecycleResult<Option<CommitVersion>> {
    facts
        .manifest_snapshot_watermark()
        .map(|watermark| {
            let version = CommitVersion::new(watermark);
            if version == CommitVersion::ZERO {
                return Err(LifecycleError::RecoveryFailed {
                    reason: "manifest snapshot watermark must be nonzero",
                });
            }
            Ok(version)
        })
        .transpose()
}

fn validate_snapshot_watermark(
    container: &SnapshotContainer,
    expected: CommitVersion,
) -> LifecycleResult<()> {
    if container.header().watermark_commit_version() != expected {
        return Err(LifecycleError::RecoveryFailed {
            reason: "snapshot watermark does not match database manifest",
        });
    }
    Ok(())
}

/// Validate checkpoint row watermarks before partitioning by branch.
/// `branch_id` membership validation now happens post-catalog-build in
/// `bootstrap::install_non_seeded_checkpoint_rows`, so unknown / Deleted
/// branches can be rejected against the rebuilt catalog rather than the
/// shell's seeded branch only.
fn validate_checkpoint_rows(watermark: CommitVersion, rows: &[StorageRow]) -> LifecycleResult<()> {
    for row in rows {
        if row.commit_version() > watermark {
            return Err(LifecycleError::RecoveryFailed {
                reason: "checkpoint row commit version exceeds snapshot watermark",
            });
        }
    }
    Ok(())
}

fn recovery_health_from_faults(
    request: &LifecycleRecoveryRequest,
    faults: Vec<RecoveryFault>,
) -> LifecycleResult<RecoveryHealth> {
    if faults.is_empty() {
        return Ok(RecoveryHealth::Healthy);
    }
    if request.strictness() == RecoveryStrictness::Strict {
        return Err(LifecycleError::RecoveryFailed {
            reason: "strict recovery cannot return degraded health",
        });
    }
    let class = degradation_class_for_faults(&faults);
    RecoveryHealth::degraded(class, faults)
}

fn degradation_class_for_faults(faults: &[RecoveryFault]) -> RecoveryDegradationClass {
    if faults.iter().any(|fault| {
        matches!(
            fault.kind(),
            RecoveryFaultKind::CorruptManifest
                | RecoveryFaultKind::MissingSnapshotObject
                | RecoveryFaultKind::MissingTableObject
                | RecoveryFaultKind::MissingTableManifestBase
                | RecoveryFaultKind::InheritedLayerLoss
                | RecoveryFaultKind::NoManifestFallback
                | RecoveryFaultKind::WalTailRepairFailed
                | RecoveryFaultKind::WalCommittedSuffixMissing
        )
    }) {
        RecoveryDegradationClass::DataLoss
    } else if faults
        .iter()
        .any(|fault| matches!(fault.kind(), RecoveryFaultKind::QuarantineInventoryMismatch))
    {
        RecoveryDegradationClass::Telemetry
    } else {
        RecoveryDegradationClass::PolicyDowngrade
    }
}

fn is_lossy_table_manifest_recovery_error(error: &LifecycleError) -> bool {
    matches!(
        error,
        LifecycleError::TableManifestRecoveryMismatch { .. }
            | LifecycleError::TableManifestBranchInstallFailed { .. }
    )
}

fn table_manifest_recovery_fault_kind(error: &LifecycleError) -> RecoveryFaultKind {
    match error {
        LifecycleError::TableManifestRecoveryMismatch { reason, .. }
            if *reason == "table manifest listed table object is missing"
                || *reason == "table manifest facts do not match table object"
                || *reason == "table manifest listed table object failed validation" =>
        {
            RecoveryFaultKind::MissingTableObject
        }
        LifecycleError::TableManifestBranchInstallFailed { .. } => {
            RecoveryFaultKind::InheritedLayerLoss
        }
        _ => RecoveryFaultKind::CorruptManifest,
    }
}

fn table_manifest_recovery_fault_reason(error: &LifecycleError) -> &'static str {
    match error {
        LifecycleError::TableManifestRecoveryMismatch { reason, .. }
        | LifecycleError::TableManifestBranchInstallFailed { reason, .. } => reason,
        _ => "table manifest recovery failed",
    }
}

fn push_fault(
    faults: &mut Vec<RecoveryFault>,
    max_faults: usize,
    kind: RecoveryFaultKind,
    reason: &'static str,
) -> LifecycleResult<()> {
    if faults.len() == max_faults {
        return Err(LifecycleError::RecoveryFailed {
            reason: "recovery fault limit exceeded",
        });
    }
    faults.push(RecoveryFault::new(kind, reason)?);
    Ok(())
}

fn push_fault_for_branch(
    faults: &mut Vec<RecoveryFault>,
    max_faults: usize,
    kind: RecoveryFaultKind,
    reason: &'static str,
    branch_id: strata_core::BranchId,
) -> LifecycleResult<()> {
    if faults.len() == max_faults {
        return Err(LifecycleError::RecoveryFailed {
            reason: "recovery fault limit exceeded",
        });
    }
    faults.push(RecoveryFault::new(kind, reason)?.with_affected_branch(branch_id));
    Ok(())
}

fn is_quarantine_inventory_mismatch(source: &QuarantineServiceError) -> bool {
    matches!(
        source,
        QuarantineServiceError::Decode { .. }
            | QuarantineServiceError::DatabaseMismatch { .. }
            | QuarantineServiceError::BranchMismatch { .. }
            | QuarantineServiceError::CodecMismatch { .. }
    )
}

fn quarantine_error_object(source: &QuarantineServiceError) -> Option<ObjectName> {
    match source {
        QuarantineServiceError::Decode { object, .. }
        | QuarantineServiceError::DatabaseMismatch { object, .. }
        | QuarantineServiceError::BranchMismatch { object, .. }
        | QuarantineServiceError::CodecMismatch { object, .. } => Some(object.clone()),
        _ => None,
    }
}

fn format_error(source: FormatError) -> LifecycleError {
    // A snapshot section that fails to decode is malformed durable state, not a
    // transient read failure: recovery cannot succeed on a retry.
    LifecycleError::recovery_corruption_with(
        "snapshot section failed to decode during recovery",
        source,
    )
}

fn snapshot_error(source: SnapshotServiceError) -> LifecycleError {
    let reason = match source {
        SnapshotServiceError::Missing { .. } => "manifest-listed snapshot is missing",
        SnapshotServiceError::Decode { .. } | SnapshotServiceError::Visit { .. } => {
            "snapshot decode failed"
        }
        SnapshotServiceError::CodecMismatch { .. } => "snapshot codec mismatch",
        SnapshotServiceError::DatabaseMismatch { .. } => "snapshot database mismatch",
        SnapshotServiceError::SnapshotIdMismatch { .. } => "snapshot id mismatch",
        _ => "snapshot service failed",
    };
    LifecycleError::lower_layer_with(LifecycleLowerLayer::Service, reason, source)
}

fn wal_error(source: WalServiceError) -> LifecycleError {
    LifecycleError::lower_layer_with(
        LifecycleLowerLayer::Service,
        "WAL recovery read failed",
        source,
    )
}

/// Classify a WAL read failure encountered during recovery. A decode failure
/// (`Format` — checksum/magic mismatch) means the durable log itself is
/// corrupt, so recovery cannot succeed on a retry: it is a permanent
/// [`LifecycleError::RecoveryCorruption`]. Every other WAL failure (backend IO,
/// listing, publish) may be transient and stays a lower-layer error.
fn wal_recovery_error(source: WalServiceError) -> LifecycleError {
    if source.is_durable_corruption() {
        return LifecycleError::recovery_corruption_with(
            "WAL segment failed to decode during recovery",
            source,
        );
    }
    wal_error(source)
}

fn wal_repair_error(source: WalServiceError) -> LifecycleError {
    LifecycleError::lower_layer_with(
        LifecycleLowerLayer::Service,
        "WAL latest-tail repair failed",
        source,
    )
}

fn quarantine_error(source: QuarantineServiceError) -> LifecycleError {
    LifecycleError::lower_layer_with(
        LifecycleLowerLayer::Service,
        "quarantine inventory recovery failed",
        source,
    )
}

fn branch_error(source: crate::branch::error::BranchRuntimeError) -> LifecycleError {
    LifecycleError::lower_layer_with(
        LifecycleLowerLayer::BranchRuntime,
        "checkpoint install failed",
        source,
    )
}

fn table_runtime_error(source: crate::table::TableRuntimeError) -> LifecycleError {
    LifecycleError::lower_layer_with(
        LifecycleLowerLayer::TableRuntime,
        "invalid recovery table identity",
        source,
    )
}
