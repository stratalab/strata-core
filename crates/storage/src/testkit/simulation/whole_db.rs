//! TCP4.11b — the whole-DB simulation: multi-branch workloads over
//! multi-epoch crash/recover cycles, on the unified deterministic substrate.
//!
//! One seed derives everything: the mutation workload, the action stream
//! (commits across a live branch set, forks — current and at-version —
//! delete/recreate cycles, maintenance cadence, clock advancement), each
//! epoch's ending (clean close or a seeded filesystem-model power loss),
//! and the next epoch continues on the surviving state — the `RocksDB`
//! continuous-crash shape with the STH-1 oracle carried across epochs.
//!
//! The single [`ExpectedState`] models every branch (its log is
//! branch-keyed). Forks seed the target branch by replaying the source's
//! surviving history compressed per original commit version, so the
//! version-strict prefix oracle holds on inherited rows. After a lossy
//! crash the model first drops every ack above the runtime's recovered
//! visible version (the clock legally re-issues those versions — a stale
//! model ack there would collide with the re-issue), then each branch
//! adopts its own surviving watermark (the model truncates above it); an
//! acknowledged branch deletion must stay dead under `ZeroLoss` and may
//! resurrect only under `OnDiskDamage`, where the resurrected content must
//! itself be a valid prefix.
//!
//! Continuous oracles: the per-branch prefix-of-history scan (every step,
//! on the touched branch), seeded **temporal probes** (`ReadBound::
//! AtVersion(w)` must equal the model's `live_state_at(w)`; probes below
//! the retained floor count as unavailable, never as silent passes), the
//! maintenance failure ring (must stay empty), the write-ordering
//! watchdog's confirmed report at每 epoch end, and branch-catalog
//! health-vs-truth after every reopen. Every run's facts replay bit-exact
//! from the seed.

use std::path::Path;
use std::time::Duration;

use strata_core::{BranchId, CommitVersion};

use crate::api::{
    BranchAction, BranchGeneration, BranchRequest, BranchStatus, CommitBatch, CommitOptions,
    DiagnosticsDetail, DiagnosticsFactState, DiagnosticsRequest, DiagnosticsScope,
    MaintenanceRequest, MaintenanceScope, MaintenanceTask, PrefixScanReadRequest, ReadBound,
    ReclaimBudget, RecoveryHealthSummary, StorageBackend, StorageCloseOptions,
    StorageDurabilityPolicy, StorageMaintenanceSchedulingPolicy, StorageOpenOptions,
    StorageOpenSummary, StorageRuntime,
};
use crate::testkit::recovery_oracle::model::{ExpectedState, OracleDurability, RecordedMutation};
use crate::testkit::recovery_oracle::verify::{
    classify_recovered, scan_recovered, CrashFamily, RecoveredState,
};
use crate::testkit::recovery_oracle::workload::{
    default_branch, generate_workload, oracle_prefix_key, oracle_space, to_commit_mutation,
    SCAN_LIMIT,
};
use crate::testkit::rng::SplitMix64;
use crate::testkit::{FsModel, TestkitError};

/// Decorrelate the whole-DB action stream from the mutation workload.
const WHOLE_DB_SALT: u64 = 0x5744_4253_696d_5f31;
/// Branch-id pool for forks/recreates (beyond the default branch).
const BRANCH_POOL: u8 = 4;
/// A temporal probe fires roughly every this many steps.
const TEMPORAL_PROBE_CADENCE: u64 = 8;
/// One idle tick of the reclaim cadence: the manual clock advances past the
/// quiescence debounce and the queue drains, so an armed idle wake fires and
/// an owed sweep is retried (space-reclamation contract §3.1, slice 13).
const RECLAIM_CADENCE_TICK_MS: u64 = 1_000;
/// The close-time reclaim budget under the sim: effectively unbounded, so the
/// close drive runs to its fixed point (debt gone or a sweep deferred) and never
/// depends on wall-clock time.
const SIM_CLOSE_RECLAIM_BUDGET: Duration = Duration::from_secs(3_600);
/// Every filesystem persistence model, for seeded epoch endings.
const FS_MODELS: [FsModel; 4] = [
    FsModel::OrderedAtomic,
    FsModel::ReorderedAppends,
    FsModel::GarbageUnsyncedTail,
    FsModel::SplitRename,
];

/// One seeded whole-DB action.
#[derive(Clone, Copy, Debug)]
enum DbAction {
    /// Commit the next workload batch to a seeded live branch.
    Commit,
    /// Fork a seeded live branch at its current state into a pool slot.
    ForkCurrent,
    /// Fork a seeded live branch at a seeded past watermark.
    ForkAtVersion,
    /// Delete a seeded live pool branch (the default branch is never deleted).
    DeleteBranch,
    /// Recreate a seeded dead pool branch id as a fresh empty branch.
    RecreateBranch,
    /// Drain all pending maintenance.
    DrainMaintenance,
    /// Request a flush on a seeded live branch.
    EnqueueFlush,
    /// Request a checkpoint on a seeded live branch.
    EnqueueCheckpoint,
    /// Advance the manual maintenance clock by a seeded jitter (ms).
    AdvanceClock(u64),
    /// One idle tick of the reclaim cadence: the clock advances past the
    /// quiescence debounce and the queue drains (slice 13).
    ReclaimCadence,
}

impl DbAction {
    const fn label(self) -> &'static str {
        match self {
            DbAction::Commit => "commit",
            DbAction::ForkCurrent => "fork_current",
            DbAction::ForkAtVersion => "fork_at_version",
            DbAction::DeleteBranch => "delete_branch",
            DbAction::RecreateBranch => "recreate_branch",
            DbAction::DrainMaintenance => "drain_maintenance",
            DbAction::EnqueueFlush => "enqueue_flush",
            DbAction::EnqueueCheckpoint => "enqueue_checkpoint",
            DbAction::AdvanceClock(_) => "advance_clock",
            DbAction::ReclaimCadence => "reclaim_cadence",
        }
    }
}

/// The version-domain bound the model adopts at a reopen. Under a lossy crash
/// family every ack above the runtime's recovered visible version was shed and
/// the recovered clock may legally re-issue those versions; the model must drop
/// its facts above the bound or the re-issue collides with them. Zero-loss
/// reopens keep the full acked history so real losses stay detectable.
pub(super) fn reopen_version_domain_bound(
    family: CrashFamily,
    recovered_visible: Option<CommitVersion>,
) -> Option<CommitVersion> {
    match family {
        CrashFamily::OnDiskDamage => recovered_visible,
        CrashFamily::ZeroLoss => None,
    }
}

fn draw_action(rng: &mut SplitMix64) -> DbAction {
    match rng.gen_u8_below(17) {
        // Commits dominate so every other action races real write load.
        0..=6 => DbAction::Commit,
        7 => DbAction::ForkCurrent,
        8 => DbAction::ForkAtVersion,
        9 => DbAction::DeleteBranch,
        10 => DbAction::RecreateBranch,
        11 => DbAction::DrainMaintenance,
        12 => DbAction::EnqueueFlush,
        13 => DbAction::EnqueueCheckpoint,
        14 | 15 => DbAction::AdvanceClock(u64::from(rng.gen_u8_below(50))),
        _ => DbAction::ReclaimCadence,
    }
}

/// How a seeded epoch ends.
#[derive(Clone, Copy, Debug, PartialEq)]
enum EpochEnding {
    /// The runtime is dropped without a close: nothing reclaims; the next
    /// open reconciles (space-reclamation contract §3.1).
    CleanDrop,
    /// A clean close: the close-time reclaim drive runs to its fixed point
    /// before the runtime goes (slice 13; contract §3.1 "Close").
    CleanClose,
    /// The #3612 shape: the runtime is dropped and a snapshot object appears
    /// at the next id with the manifest still attesting the previous one —
    /// what a crash between a checkpoint's snapshot publish and its manifest
    /// re-point leaves behind. The next open's reconcile prune owns it.
    OrphanedSnapshot,
    Crash(FsModel),
}

impl EpochEnding {
    fn draw(rng: &mut SplitMix64) -> Self {
        // Crashes dominate: the clean endings keep the zero-loss reopen
        // direction covered without dominating the sweep.
        match rng.gen_u8_below(8) {
            0 => EpochEnding::CleanDrop,
            1 => EpochEnding::CleanClose,
            2 => EpochEnding::OrphanedSnapshot,
            n => EpochEnding::Crash(FS_MODELS[usize::from((n - 3) % 4)]),
        }
    }

    const fn label(self) -> &'static str {
        match self {
            EpochEnding::CleanDrop => "clean_drop",
            EpochEnding::CleanClose => "clean_close",
            EpochEnding::OrphanedSnapshot => "crash_orphaned_snapshot",
            EpochEnding::Crash(FsModel::OrderedAtomic) => "crash_ordered_atomic",
            EpochEnding::Crash(FsModel::ReorderedAppends) => "crash_reordered_appends",
            EpochEnding::Crash(FsModel::GarbageUnsyncedTail) => "crash_garbage_tail",
            EpochEnding::Crash(FsModel::SplitRename) => "crash_split_rename",
        }
    }
}

/// Model-side branch bookkeeping.
#[derive(Clone, Debug, PartialEq)]
struct BranchBook {
    generation: u64,
    alive: bool,
    /// The deletion was acknowledged by the runtime (drives the
    /// stay-dead-under-`ZeroLoss` oracle after a crash).
    delete_acked: bool,
}

/// Deterministic facts of one whole-DB run — state and sequencing only, so a
/// seed replays them bit-exact.
#[derive(Clone, Debug, Default, PartialEq)]
pub(super) struct WholeDbFacts {
    action_trace: Vec<&'static str>,
    epoch_endings: Vec<&'static str>,
    ended_degraded: bool,
    acked_versions: Vec<u64>,
    forks: usize,
    deletes: usize,
    recreates: usize,
    temporal_probes_ok: usize,
    temporal_probes_unavailable: usize,
    forks_unavailable: usize,
    deletes_refused: usize,
    adopted_watermarks: Vec<(String, u64)>,
    resurrections: usize,
    fail_loud_epochs: usize,
    final_live_branches: Vec<String>,
    final_states: Vec<(String, RecoveredState)>,
    /// Every footprint audit the oracle took, in order (slice 13).
    footprint_audits: Vec<FootprintAudit>,
    /// Bytes every session's reclaim ledger reported released, summed.
    bytes_reclaimed: u64,
    /// Reclaim passes every session's ledger recorded, summed.
    reclaim_passes: u64,
    /// Orphaned snapshots the `OrphanedSnapshot` endings actually planted.
    orphaned_snapshots_planted: usize,
}

/// One Audit-tier footprint reading (space-reclamation contract §3.5), a
/// bit-exact function of the trajectory on the deterministic local fs.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) struct FootprintAudit {
    epoch: usize,
    phase: &'static str,
    unreferenced_objects: u64,
    unreferenced_bytes: u64,
    quarantined_objects: u64,
    snapshot_objects: u64,
    superseded_snapshots: u64,
    /// #3643: timeline segments the live snapshot no longer references.
    superseded_timeline_segments: u64,
    wal_reclaimable_bytes: u64,
    /// The ledger explained the remaining debt (a deferred mark or sweep).
    debt_deferred: bool,
}

/// What a footprint audit must find; each expectation names the contract
/// phase that guarantees it.
#[derive(Clone, Copy, Debug)]
struct FootprintExpectation {
    /// No unreferenced or quarantined table object — unless the ledger
    /// recorded why the sweep could not run (a documented deferral).
    no_object_debt: bool,
    /// No snapshot beside the attested one (the open reconcile pruned), and
    /// (#3643) no timeline segment the attested snapshot does not reference.
    no_superseded: bool,
}

impl FootprintExpectation {
    const NONE: Self = Self {
        no_object_debt: false,
        no_superseded: false,
    };
    const CLEAN: Self = Self {
        no_object_debt: true,
        no_superseded: true,
    };
}

impl WholeDbFacts {
    pub(super) fn bytes_reclaimed(&self) -> u64 {
        self.bytes_reclaimed
    }
    #[cfg(test)]
    pub(super) fn reclaim_passes(&self) -> u64 {
        self.reclaim_passes
    }
    #[cfg(test)]
    pub(super) fn footprint_audits(&self) -> &[FootprintAudit] {
        &self.footprint_audits
    }
    pub(super) fn orphaned_snapshots_planted(&self) -> usize {
        self.orphaned_snapshots_planted
    }
    pub(super) fn clean_closes(&self) -> usize {
        self.epoch_endings
            .iter()
            .filter(|label| **label == "clean_close")
            .count()
    }
    pub(super) fn forks(&self) -> usize {
        self.forks
    }
    pub(super) fn deletes(&self) -> usize {
        self.deletes
    }
    pub(super) fn temporal_probes_ok(&self) -> usize {
        self.temporal_probes_ok
    }
    pub(super) fn epochs(&self) -> usize {
        self.epoch_endings.len()
    }
    pub(super) fn crashed_epochs(&self) -> usize {
        self.epoch_endings
            .iter()
            .filter(|label| label.starts_with("crash"))
            .count()
    }
}

fn pool_branch(slot: u8) -> BranchId {
    BranchId::from_bytes([0xB0 + slot; BranchId::BYTE_LEN])
}

fn branch_label(branch: BranchId) -> String {
    if branch == default_branch() {
        "default".to_owned()
    } else {
        format!("pool-{:02x}", branch.as_bytes()[0])
    }
}

/// The whole-DB simulation state carried ACROSS epochs. Branch books live in
/// a small ordered `Vec` (`BranchId` has no `Ord`); the pool is at most five
/// branches, and insertion order is itself seed-deterministic.
struct WholeDbSim {
    seed: u64,
    durability: StorageDurabilityPolicy,
    model: ExpectedState,
    books: Vec<(BranchId, BranchBook)>,
    workload: Vec<Vec<RecordedMutation>>,
    commit_index: usize,
    facts: WholeDbFacts,
    /// The orphaned snapshot the last epoch planted, until a reopen proves
    /// the open reconcile pruned it.
    planted_orphan: Option<std::path::PathBuf>,
}

impl WholeDbSim {
    fn new(seed: u64, total_steps: usize) -> Self {
        let durability = if seed & 1 == 0 {
            StorageDurabilityPolicy::Always
        } else {
            StorageDurabilityPolicy::Standard
        };
        let oracle_durability = if matches!(durability, StorageDurabilityPolicy::Always) {
            OracleDurability::Always
        } else {
            OracleDurability::Standard
        };
        let books = vec![(
            default_branch(),
            BranchBook {
                generation: 1,
                alive: true,
                delete_acked: false,
            },
        )];
        Self {
            seed,
            durability,
            model: ExpectedState::new(oracle_durability),
            books,
            workload: generate_workload(seed, total_steps.max(1)),
            commit_index: 0,
            facts: WholeDbFacts::default(),
            planted_orphan: None,
        }
    }

    fn live_branches(&self) -> Vec<BranchId> {
        self.books
            .iter()
            .filter(|(_, book)| book.alive)
            .map(|(branch, _)| *branch)
            .collect()
    }

    fn book(&self, branch: BranchId) -> Option<&BranchBook> {
        self.books
            .iter()
            .find(|(id, _)| *id == branch)
            .map(|(_, book)| book)
    }

    fn book_mut(&mut self, branch: BranchId) -> Option<&mut BranchBook> {
        self.books
            .iter_mut()
            .find(|(id, _)| *id == branch)
            .map(|(_, book)| book)
    }

    fn upsert_book(&mut self, branch: BranchId, book: BranchBook) {
        if let Some(existing) = self.book_mut(branch) {
            *existing = book;
        } else {
            self.books.push((branch, book));
        }
    }

    fn pick_live(&self, rng: &mut SplitMix64) -> BranchId {
        let live = self.live_branches();
        live[usize::try_from(rng.next_u64() % live.len() as u64).expect("bounded")]
    }

    fn error(&self, step: usize, detail: impl Into<String>) -> TestkitError {
        TestkitError::new(format!(
            "[seed={} step={step}] {}",
            self.seed,
            detail.into()
        ))
    }

    /// Seed `target`'s model with `source`'s FULL acknowledged history up to
    /// `watermark`: a fork inherits MVCC history, not a flattened state — an
    /// at-version read below the fork point serves the source's original
    /// intermediate versions, so the model must mirror every inherited commit
    /// (found by the first temporal probe against a compressed seeding).
    fn seed_fork_model(&mut self, source: BranchId, target: BranchId, watermark: CommitVersion) {
        let mut versions = self.model.candidate_watermarks(source, watermark);
        versions.retain(|version| *version != CommitVersion::ZERO);
        versions.reverse(); // ascending
        for version in versions {
            if let Some(mutations) = self.model.mutations_at(source, version) {
                let mutations = mutations.to_vec();
                self.model.record_ack(target, version, mutations);
            }
        }
    }

    /// One seeded action against the open runtime.
    #[expect(
        clippy::too_many_lines,
        reason = "one arm per grammar action; each arm is a few lines"
    )]
    fn apply(
        &mut self,
        runtime: &mut StorageRuntime<'_>,
        rng: &mut SplitMix64,
        step: usize,
    ) -> Result<(), TestkitError> {
        let action = draw_action(rng);
        self.facts.action_trace.push(action.label());
        match action {
            DbAction::Commit => self.apply_commit(runtime, rng, step)?,
            DbAction::ForkCurrent | DbAction::ForkAtVersion => {
                let source = self.pick_live(rng);
                let slot = rng.gen_u8_below(BRANCH_POOL);
                let target = pool_branch(slot);
                if self.book(target).is_some_and(|book| book.alive) || target == source {
                    return Ok(()); // slot occupied this step; seeded no-op
                }
                let watermark = if matches!(action, DbAction::ForkCurrent) {
                    self.model.last_acked_version(source)
                } else {
                    let uppers = self
                        .model
                        .last_acked_version(source)
                        .map(|upper| self.model.candidate_watermarks(source, upper))
                        .unwrap_or_default();
                    // Skip the ZERO floor: fork-at-version needs history.
                    uppers
                        .into_iter()
                        .filter(|w| *w != CommitVersion::ZERO)
                        .nth(usize::from(rng.gen_u8_below(3)))
                };
                let Some(watermark) = watermark else {
                    return Ok(()); // source has no history yet; seeded no-op
                };
                let generation = self.book(target).map_or(1, |b| b.generation + 1);
                let fork_action = match action {
                    DbAction::ForkCurrent => BranchAction::ForkCurrent { source },
                    _ => BranchAction::ForkAtVersion {
                        source,
                        version: watermark,
                    },
                };
                match runtime.branch(&BranchRequest::new(
                    target,
                    fork_action,
                    Some(BranchGeneration::new(generation)),
                )) {
                    Ok(_) => {}
                    Err(crate::api::StorageApiError::RetainedHistoryUnavailable { .. }) => {
                        // The seeded watermark fell below the retained floor
                        // (pruning is part of the workload): a seeded no-op.
                        self.facts.forks_unavailable += 1;
                        return Ok(());
                    }
                    Err(err) => return Err(self.error(step, format!("fork: {err:?}"))),
                }
                self.model.forget_branch(target);
                self.seed_fork_model(source, target, watermark);
                self.upsert_book(
                    target,
                    BranchBook {
                        generation,
                        alive: true,
                        delete_acked: false,
                    },
                );
                self.facts.forks += 1;
            }
            DbAction::DeleteBranch => {
                let slot = rng.gen_u8_below(BRANCH_POOL);
                let target = pool_branch(slot);
                let Some(book) = self.book(target) else {
                    return Ok(());
                };
                if !book.alive {
                    return Ok(());
                }
                let generation = book.generation;
                match runtime.branch(&BranchRequest::new(
                    target,
                    BranchAction::Delete,
                    Some(BranchGeneration::new(generation)),
                )) {
                    Ok(_) => {
                        let book = self.book_mut(target).expect("book exists");
                        book.alive = false;
                        book.delete_acked = true;
                        self.facts.deletes += 1;
                    }
                    Err(crate::api::StorageApiError::BranchHasDependentChildren { .. }) => {
                        // DUR-008 (#2820): deleting a fork source that a
                        // layer-less child's recovery depends on is refused.
                        // A seeded no-op; the branch stays alive.
                        //
                        // Matched on the typed variant, not on a prefix of the
                        // reason string — the string was the only discriminant
                        // until #3196 gave the condition its own variant, and a
                        // reworded reason would have silently reclassified the
                        // refusal as a hard error here.
                        self.facts.deletes_refused += 1;
                    }
                    Err(err) => return Err(self.error(step, format!("delete: {err:?}"))),
                }
            }
            DbAction::RecreateBranch => {
                let slot = rng.gen_u8_below(BRANCH_POOL);
                let target = pool_branch(slot);
                let Some(book) = self.book(target) else {
                    return Ok(());
                };
                if book.alive {
                    return Ok(());
                }
                let generation = book.generation + 1;
                runtime
                    .branch(&BranchRequest::new(
                        target,
                        BranchAction::Create,
                        Some(BranchGeneration::new(generation)),
                    ))
                    .map_err(|err| self.error(step, format!("recreate: {err:?}")))?;
                // Fresh empty branch: the old log would poison the prefix
                // search, and the recreate supersedes the acked deletion.
                self.model.forget_branch(target);
                let book = self.book_mut(target).expect("book exists");
                book.generation = generation;
                book.alive = true;
                book.delete_acked = false;
                self.facts.recreates += 1;
            }
            DbAction::DrainMaintenance => {
                runtime
                    .drain_maintenance()
                    .map_err(|err| self.error(step, format!("drain: {err:?}")))?;
            }
            DbAction::EnqueueFlush => {
                let branch = self.pick_live(rng);
                let _ = runtime.enqueue_maintenance(&MaintenanceRequest::new(
                    MaintenanceTask::Flush,
                    MaintenanceScope::Branch(branch),
                ));
            }
            DbAction::EnqueueCheckpoint => {
                let branch = self.pick_live(rng);
                let _ = runtime.enqueue_maintenance(&MaintenanceRequest::new(
                    MaintenanceTask::Checkpoint,
                    MaintenanceScope::Branch(branch),
                ));
            }
            DbAction::AdvanceClock(ms) => {
                let _ = runtime.advance_maintenance_clock_for_test(Duration::from_millis(ms));
            }
            DbAction::ReclaimCadence => {
                let _ = runtime.advance_maintenance_clock_for_test(Duration::from_millis(
                    RECLAIM_CADENCE_TICK_MS,
                ));
                runtime
                    .drain_maintenance()
                    .map_err(|err| self.error(step, format!("reclaim cadence drain: {err:?}")))?;
            }
        }

        // Per-step safety on the touched surface: every live branch's visible
        // state must still be a zero-loss prefix of its acknowledged history.
        let branch = self.pick_live(rng);
        self.assert_branch_prefix(runtime, branch, CrashFamily::ZeroLoss, step, "step")?;

        // Seeded temporal probe.
        if rng.next_u64() % TEMPORAL_PROBE_CADENCE == 0 {
            self.temporal_probe(runtime, rng, step)?;
        }

        // The failure ring must stay silent (the #2763 surface).
        let status = runtime
            .maintenance_status()
            .map_err(|err| self.error(step, format!("status: {err:?}")))?;
        if !status.recent_failures().is_empty() {
            return Err(self.error(
                step,
                format!(
                    "maintenance failures recorded: {:?}",
                    status.recent_failures()
                ),
            ));
        }
        Ok(())
    }

    fn apply_commit(
        &mut self,
        runtime: &mut StorageRuntime<'_>,
        rng: &mut SplitMix64,
        step: usize,
    ) -> Result<(), TestkitError> {
        if self.commit_index >= self.workload.len() {
            return Ok(());
        }
        let branch = self.pick_live(rng);
        let mutations = self.workload[self.commit_index].clone();
        self.commit_index += 1;
        let batch = CommitBatch::new(
            branch,
            mutations.iter().map(to_commit_mutation).collect(),
            CommitOptions::default(),
        )
        .map_err(|err| self.error(step, format!("build batch: {err:?}")))?;
        let summary = runtime
            .commit(&batch)
            .map_err(|err| self.error(step, format!("commit: {err:?}")))?;
        self.facts
            .acked_versions
            .push(summary.commit_version().as_u64());
        self.model
            .record_ack(branch, summary.commit_version(), mutations);
        Ok(())
    }

    /// The recovered-prefix oracle on one branch's live scan.
    fn assert_branch_prefix(
        &self,
        runtime: &StorageRuntime<'_>,
        branch: BranchId,
        family: CrashFamily,
        step: usize,
        label: &str,
    ) -> Result<(), TestkitError> {
        let recovered = scan_recovered(
            runtime,
            branch,
            &oracle_space(),
            &oracle_prefix_key(),
            SCAN_LIMIT,
        )
        .map_err(|err| self.error(step, format!("{label} scan: {err}")))?;
        if let Err(violation) = classify_recovered(&self.model, branch, &recovered, family) {
            return Err(self.error(
                step,
                format!(
                    "{label} prefix violation on {}: {violation:?}",
                    branch_label(branch)
                ),
            ));
        }
        Ok(())
    }

    /// A seeded at-version read: the runtime's `AtVersion(w)` scan must equal
    /// the model's `live_state_at(w)` exactly. History pruned below the
    /// retained floor counts as unavailable — recorded, never a silent pass.
    fn temporal_probe(
        &mut self,
        runtime: &StorageRuntime<'_>,
        rng: &mut SplitMix64,
        step: usize,
    ) -> Result<(), TestkitError> {
        let branch = self.pick_live(rng);
        let Some(upper) = self.model.last_acked_version(branch) else {
            return Ok(());
        };
        let watermarks = self.model.candidate_watermarks(branch, upper);
        let candidates: Vec<CommitVersion> = watermarks
            .into_iter()
            .filter(|w| *w != CommitVersion::ZERO)
            .collect();
        if candidates.is_empty() {
            return Ok(());
        }
        let watermark =
            candidates[usize::try_from(rng.next_u64() % candidates.len() as u64).expect("bounded")];
        let outcome = runtime.scan_prefix(&PrefixScanReadRequest::new(
            branch,
            oracle_space(),
            oracle_prefix_key(),
            ReadBound::AtVersion(watermark),
            None,
        ));
        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(crate::api::StorageApiError::RetainedHistoryUnavailable { .. }) => {
                // The watermark fell below the retained-history bound: the
                // never-pruned timeline minimum after a lossy reopen, or the
                // MVCC retained-history floor once pruning is part of the
                // workload (#3502 Slice E). Both surface this exact variant —
                // any OTHER error is a real divergence, never a silent pass.
                self.facts.temporal_probes_unavailable += 1;
                return Ok(());
            }
            Err(other) => {
                return Err(self.error(
                    step,
                    format!(
                        "temporal probe on {} at v{} raised an unexpected error: {other:?}",
                        branch_label(branch),
                        watermark.as_u64(),
                    ),
                ));
            }
        };
        let mut observed = RecoveredState::new();
        for row in outcome.rows() {
            if row.is_tombstone() {
                continue;
            }
            if let Some(value) = row.value() {
                observed.insert(
                    (row.storage_space().clone(), row.key().clone()),
                    (value.clone(), row.commit_version()),
                );
            }
        }
        let expected = self.model.live_state_at(branch, watermark);
        if observed != expected {
            return Err(self.error(
                step,
                format!(
                    "temporal probe diverged on {} at v{}: observed {} rows, expected {}",
                    branch_label(branch),
                    watermark.as_u64(),
                    observed.len(),
                    expected.len()
                ),
            ));
        }
        self.facts.temporal_probes_ok += 1;
        Ok(())
    }

    /// Post-reopen reconciliation: classify every branch against the crash
    /// family, adopt surviving watermarks, enforce deletion semantics, and
    /// diff the branch catalog against the model (health-vs-truth).
    #[expect(
        clippy::too_many_lines,
        reason = "three reconciliation phases (live prefix+adoption, deletion semantics, catalog diff) share the branch loop"
    )]
    fn reconcile_after_reopen(
        &mut self,
        runtime: &mut StorageRuntime<'_>,
        family: CrashFamily,
        epoch: usize,
        recovered_visible: Option<CommitVersion>,
    ) -> Result<(), TestkitError> {
        // Version-domain adoption FIRST: a lossy crash sheds every ack above the
        // runtime's recovered visible version — by content AND by version domain.
        // The recovered clock legally RE-ISSUES those version numbers for new
        // commits, so a stale model ack above the bound collides with the
        // re-issue and poisons every later classify on the branch (a state-only
        // adoption can miss this: legally-shed commits that are live-state
        // no-ops make a higher cut state-identical to the true surviving one).
        if let Some(bound) = reopen_version_domain_bound(family, recovered_visible) {
            let ids: Vec<BranchId> = self.books.iter().map(|(branch, _)| *branch).collect();
            for branch in ids {
                self.model.truncate_branch_above(branch, bound);
            }
        }
        let books: Vec<(BranchId, BranchBook)> = self.books.clone();
        for (branch, book) in books {
            if book.alive {
                let recovered = scan_recovered(
                    runtime,
                    branch,
                    &oracle_space(),
                    &oracle_prefix_key(),
                    SCAN_LIMIT,
                )
                .map_err(|err| {
                    self.error(
                        epoch,
                        format!("reopen scan {}: {err}", branch_label(branch)),
                    )
                })?;
                if let Err(violation) = classify_recovered(&self.model, branch, &recovered, family)
                {
                    return Err(self.error(
                        epoch,
                        format!(
                            "reopen prefix violation on {}: {violation:?}",
                            branch_label(branch)
                        ),
                    ));
                }
                // A lossy crash may have shed an acked suffix WITHOUT that
                // being a violation — the classify above already accepted the
                // shorter prefix. The model must adopt the surviving
                // watermark unconditionally, or the next epoch's zero-loss
                // step checks would demand the shed rows forever.
                if matches!(family, CrashFamily::OnDiskDamage) {
                    let upper = self
                        .model
                        .max_version(branch)
                        .unwrap_or(CommitVersion::ZERO);
                    let survived = self
                        .model
                        .candidate_watermarks(branch, upper)
                        .into_iter()
                        .find(|w| self.model.live_state_at(branch, *w) == recovered);
                    if let Some(watermark) = survived {
                        if self.model.last_acked_version(branch) != Some(watermark) {
                            self.model.truncate_branch_above(branch, watermark);
                            self.facts
                                .adopted_watermarks
                                .push((branch_label(branch), watermark.as_u64()));
                        }
                    }
                }
            } else if book.delete_acked {
                // A dead branch: under ZeroLoss it must STAY dead; under a
                // lossy crash the deletion itself may be the lost suffix and
                // the branch resurrects with a valid pre-delete prefix.
                let scan = scan_recovered(
                    runtime,
                    branch,
                    &oracle_space(),
                    &oracle_prefix_key(),
                    SCAN_LIMIT,
                );
                match (family, scan) {
                    (CrashFamily::ZeroLoss, Ok(state)) if !state.is_empty() => {
                        return Err(self.error(
                            epoch,
                            format!(
                                "acked deletion of {} resurrected under zero-loss",
                                branch_label(branch)
                            ),
                        ));
                    }
                    (CrashFamily::OnDiskDamage, Ok(state)) if !state.is_empty() => {
                        if let Err(violation) = classify_recovered(
                            &self.model,
                            branch,
                            &state,
                            CrashFamily::OnDiskDamage,
                        ) {
                            return Err(self.error(
                                epoch,
                                format!(
                                    "resurrected {} is not a valid prefix: {violation:?}",
                                    branch_label(branch)
                                ),
                            ));
                        }
                        let book = self.book_mut(branch).expect("book exists");
                        book.alive = true;
                        book.delete_acked = false;
                        self.facts.resurrections += 1;
                    }
                    _ => {}
                }
            }
        }

        // Health-vs-truth: the branch catalog must agree with the model.
        let listed = runtime
            .branch(&BranchRequest::new(
                default_branch(),
                BranchAction::List,
                None,
            ))
            .map_err(|err| self.error(epoch, format!("branch list: {err:?}")))?;
        for summary in listed.branches() {
            let alive = summary.status() == BranchStatus::Active;
            let expected = self
                .book(summary.branch_id())
                .is_some_and(|book| book.alive);
            if alive != expected {
                return Err(self.error(
                    epoch,
                    format!(
                        "branch catalog disagrees with the model on {}: catalog alive={alive}, \
                         model alive={expected}",
                        branch_label(summary.branch_id())
                    ),
                ));
            }
        }

        // The footprint oracle (space-reclamation contract §3.1/§3.5, slice 13).
        // The inline scheduler runs the open's reclaim wake before the open
        // returns; the drain settles anything it chained. A zero-loss reopen
        // then holds no table-object debt and no snapshot beside the attested
        // one; a damaged reopen may legitimately defer, so it is only recorded.
        // (The close-time half of the contract is observed by the post-close
        // probe, before any open wake can run.)
        runtime
            .drain_maintenance()
            .map_err(|err| self.error(epoch, format!("open-wake drain: {err:?}")))?;
        // A snapshot the manifest never attested is pure garbage: a lossless
        // open's reconcile prune must have removed the one the last epoch
        // planted (contract §3.4, `ReconcileToAttested`).
        if let Some(planted) = self.planted_orphan.take() {
            if matches!(family, CrashFamily::ZeroLoss) && planted.exists() {
                return Err(self.error(
                    epoch,
                    format!(
                        "planted orphaned snapshot survived the open reconcile: {}",
                        planted.display()
                    ),
                ));
            }
        }
        let expectation = if matches!(family, CrashFamily::ZeroLoss) {
            FootprintExpectation::CLEAN
        } else {
            FootprintExpectation::NONE
        };
        self.audit_footprint(runtime, epoch, "reopen", expectation)
    }

    /// The close-time contract, observed on its own: after a clean close the
    /// store is reopened under the evaluate-and-enqueue policy, which queues
    /// the open's reclaim wake without running it, so the audit sees exactly
    /// what the close drive left behind. A healthy session whose ledger
    /// recorded no deferral must have left no table-object debt and no
    /// superseded snapshot.
    fn probe_post_close(
        &mut self,
        root: &Path,
        epoch: usize,
        zero_loss_session: bool,
        expect_no_debt: bool,
    ) -> Result<(), TestkitError> {
        let backend = StorageBackend::write_ordering_reordering_local_fs(root.to_path_buf());
        let runtime = StorageRuntime::open_with_backend(
            StorageOpenOptions::durable_local(self.durability)
                .with_maintenance_scheduling_policy(
                    StorageMaintenanceSchedulingPolicy::EvaluateAndEnqueue,
                )
                .with_strict_recovery(false),
            &backend,
        )
        .map_err(|err| self.error(epoch, format!("post-close probe open: {err:?}")))?
        .into_runtime();
        // #3665: a clean close of a zero-loss session loses nothing, so the
        // reopen that follows it recovers healthy. Judged on its own, before
        // the footprint (whose unknown counts a degraded reopen produces).
        if zero_loss_session {
            let health = runtime
                .diagnostics(DiagnosticsRequest::new(DiagnosticsScope::Global))
                .map_err(|err| self.error(epoch, format!("post-close health: {err:?}")))?
                .recovery()
                .health();
            if health != Some(RecoveryHealthSummary::Healthy) {
                return Err(self.error(
                    epoch,
                    format!("post-close reopen of a clean close is not healthy: {health:?}"),
                ));
            }
        }
        self.audit_footprint(
            &runtime,
            epoch,
            "post_close",
            FootprintExpectation {
                no_object_debt: expect_no_debt,
                // A completed checkpoint chains its `Superseded` prune, and a
                // clean close drains the prune before it stops; the open's
                // reconcile would hide a missed prune, so it is judged here.
                no_superseded: expect_no_debt,
            },
        )?;
        drop(runtime);
        Ok(())
    }

    /// One Audit-tier reading of the footprint, recorded into the facts and
    /// held against `expectation`.
    fn audit_footprint(
        &mut self,
        runtime: &StorageRuntime<'_>,
        epoch: usize,
        phase: &'static str,
        expectation: FootprintExpectation,
    ) -> Result<(), TestkitError> {
        let outcome = runtime
            .diagnostics(
                DiagnosticsRequest::new(DiagnosticsScope::Global)
                    .with_detail(DiagnosticsDetail::Audit),
            )
            .map_err(|err| self.error(epoch, format!("footprint audit ({phase}): {err:?}")))?;
        let footprint = outcome.footprint();
        let quarantine = outcome.quarantine();
        let reclaim = outcome.reclaim();
        if footprint.state() != DiagnosticsFactState::Known {
            return Err(self.error(
                epoch,
                format!("footprint audit ({phase}): state {:?}", footprint.state()),
            ));
        }
        let count = |value: Option<usize>| value.map_or(u64::MAX, |v| v as u64);
        let debt_deferred = reclaim
            .last_sweep()
            .is_some_and(|pass| pass.deferral().is_some())
            || reclaim
                .last_mark()
                .is_some_and(|pass| pass.deferral().is_some());
        let audit = FootprintAudit {
            epoch,
            phase,
            unreferenced_objects: count(footprint.unreferenced_objects()),
            unreferenced_bytes: footprint.unreferenced_bytes().unwrap_or(u64::MAX),
            quarantined_objects: count(quarantine.quarantined_objects()),
            snapshot_objects: count(footprint.snapshot_objects()),
            superseded_snapshots: count(footprint.superseded_snapshots()),
            superseded_timeline_segments: count(footprint.superseded_timeline_segments()),
            wal_reclaimable_bytes: footprint.wal_reclaimable_bytes().unwrap_or(u64::MAX),
            debt_deferred,
        };
        self.facts.footprint_audits.push(audit);
        let object_debt = audit.unreferenced_objects > 0 || audit.quarantined_objects > 0;
        if expectation.no_object_debt && object_debt && !audit.debt_deferred {
            return Err(self.error(
                epoch,
                format!(
                    "footprint audit ({phase}): {} unreferenced and {} quarantined table objects \
                     remain with no deferral recorded — reclaim did not run: {audit:?}",
                    audit.unreferenced_objects, audit.quarantined_objects
                ),
            ));
        }
        if expectation.no_superseded && audit.superseded_snapshots > 0 {
            return Err(self.error(
                epoch,
                format!(
                    "footprint audit ({phase}): {} superseded snapshots survive: {audit:?}",
                    audit.superseded_snapshots
                ),
            ));
        }
        if expectation.no_superseded && audit.superseded_timeline_segments > 0 {
            return Err(self.error(
                epoch,
                format!(
                    "footprint audit ({phase}): {} unreferenced timeline segments survive: \
                     {audit:?}",
                    audit.superseded_timeline_segments
                ),
            ));
        }
        Ok(())
    }

    /// Fold a session's reclaim ledger into the trajectory's totals, just
    /// before the session ends; reports whether the ledger recorded a deferral.
    fn fold_session_reclaim(
        &mut self,
        runtime: &StorageRuntime<'_>,
        epoch: usize,
    ) -> Result<bool, TestkitError> {
        let outcome = runtime
            .diagnostics(DiagnosticsRequest::new(DiagnosticsScope::Global))
            .map_err(|err| self.error(epoch, format!("session ledger: {err:?}")))?;
        let reclaim = outcome.reclaim();
        self.facts.bytes_reclaimed = self
            .facts
            .bytes_reclaimed
            .saturating_add(reclaim.total_bytes_reclaimed());
        self.facts.reclaim_passes = self
            .facts
            .reclaim_passes
            .saturating_add(reclaim.total_passes());
        // A session that recorded health debt is not the healthy session the
        // post-close audit judges (#3665): a branch whose table-manifest
        // publish failed (a `fork_at_version` child still holding its volatile
        // materialized table cannot build a manifest) keeps its flushed tables
        // owned in memory until the process ends, the close checkpoint carries
        // their rows, and the objects become garbage the next open reclaims.
        let healthy = outcome.recovery().health() == Some(RecoveryHealthSummary::Healthy);
        Ok(!healthy
            || reclaim
                .last_sweep()
                .is_some_and(|pass| pass.deferral().is_some())
            || reclaim
                .last_mark()
                .is_some_and(|pass| pass.deferral().is_some()))
    }
}

/// Plant the #3612 on-disk shape after a drop: a snapshot object at the next
/// id beside the attested one, exactly what a crash between the snapshot
/// publish and the manifest re-point leaves. Returns whether one was planted
/// (a store that never checkpointed has nothing to orphan).
fn plant_orphaned_snapshot(root: &Path) -> Result<Option<std::path::PathBuf>, TestkitError> {
    let prefix = crate::layout::ObjectLayout::snapshot_prefix()
        .map_err(|err| TestkitError::new(format!("snapshot layout: {err:?}")))?;
    let dir = root.join(prefix.as_str().trim_end_matches('/'));
    let Ok(entries) = std::fs::read_dir(&dir) else {
        return Ok(None);
    };
    let mut highest: Option<u64> = None;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(stem) = name.to_str().and_then(|n| n.strip_suffix(".object@")) else {
            continue;
        };
        if let Ok(id) = u64::from_str_radix(stem, 16) {
            highest = Some(highest.map_or(id, |h| h.max(id)));
        }
    }
    let Some(highest) = highest else {
        return Ok(None);
    };
    let source = crate::layout::ObjectLayout::snapshot(highest)
        .map_err(|err| TestkitError::new(format!("snapshot layout: {err:?}")))?;
    let orphan = crate::layout::ObjectLayout::snapshot(highest + 1)
        .map_err(|err| TestkitError::new(format!("snapshot layout: {err:?}")))?;
    let planted = root.join(format!("{}.object@", orphan.as_str()));
    std::fs::copy(root.join(format!("{}.object@", source.as_str())), &planted)
        .map_err(|err| TestkitError::new(format!("plant orphaned snapshot: {err}")))?;
    Ok(Some(planted))
}

/// The fail-closed contract after a LOSSY crash: recovery health goes
/// `Degraded {{ DataLoss }}` and mutating admission blocks non-retryably
/// (there is deliberately no acknowledge path on this surface — the engine
/// never opens lossy). The harness ends the trajectory there, recording it.
fn is_degraded_admission_block(error: &TestkitError) -> bool {
    let message = format!("{error:?}");
    message.contains("recovery health blocks mutating commit admission")
}

/// Run one whole-DB trajectory: `epochs` epochs of `steps_per_epoch` seeded
/// actions, each epoch ending in a seeded clean drop or filesystem-model
/// crash, with the model carried across reopens.
#[expect(
    clippy::too_many_lines,
    reason = "one linear trajectory driver: open, reconcile, steps, seeded ending per epoch"
)]
pub(super) fn run_whole_db_sim(
    root: &Path,
    seed: u64,
    epochs: usize,
    steps_per_epoch: usize,
) -> Result<WholeDbFacts, TestkitError> {
    let mut rng = SplitMix64::new(seed ^ WHOLE_DB_SALT);
    let mut sim = WholeDbSim::new(seed, epochs * steps_per_epoch);
    let mut family_next_open = CrashFamily::ZeroLoss;

    for epoch in 0..epochs {
        let backend = StorageBackend::write_ordering_reordering_local_fs(root.to_path_buf());
        let opened = StorageRuntime::open_with_backend(
            super::faults::deterministic_options(sim.durability).with_strict_recovery(false),
            &backend,
        );
        let (mut runtime, open_summary) = match opened {
            Ok(outcome) => {
                let (runtime, summary) = outcome.into_parts();
                (runtime, Some(summary))
            }
            Err(error) => {
                // Only a garbage-tail crash may refuse the reopen (fail-loud
                // CRC rejection); anything else is a real failure.
                if matches!(family_next_open, CrashFamily::OnDiskDamage) {
                    sim.facts.fail_loud_epochs += 1;
                    sim.facts.epoch_endings.push("fail_loud_open");
                    return Ok(sim.facts);
                }
                return Err(TestkitError::new(format!(
                    "[seed={seed} epoch={epoch}] reopen failed: {error:?}"
                )));
            }
        };
        super::faults::require_manual_clock(&runtime, seed)?;

        let session_family = family_next_open;
        if epoch > 0 {
            sim.reconcile_after_reopen(
                &mut runtime,
                family_next_open,
                epoch,
                open_summary.and_then(StorageOpenSummary::recovered_visible_version),
            )?;
        }

        let mut degraded = false;
        for step in 0..steps_per_epoch {
            match sim.apply(&mut runtime, &mut rng, epoch * steps_per_epoch + step) {
                Ok(()) => {}
                Err(error) if is_degraded_admission_block(&error) => {
                    sim.facts.ended_degraded = true;
                    sim.facts.epoch_endings.push("degraded_read_only");
                    degraded = true;
                    break;
                }
                Err(error) => return Err(error),
            }
        }
        if degraded {
            return Ok(sim.facts);
        }

        let ending = EpochEnding::draw(&mut rng);
        sim.facts.epoch_endings.push(ending.label());
        let deferred = sim.fold_session_reclaim(&runtime, epoch)?;
        match ending {
            EpochEnding::CleanDrop => {
                drop(runtime);
                family_next_open = CrashFamily::ZeroLoss;
            }
            EpochEnding::CleanClose => {
                runtime
                    .close_with_options(
                        StorageCloseOptions::graceful()
                            .with_reclaim_budget(ReclaimBudget::Bounded(SIM_CLOSE_RECLAIM_BUDGET)),
                    )
                    .map_err(|err| {
                        TestkitError::new(format!("[seed={seed} epoch={epoch}] close: {err:?}"))
                    })?;
                drop(runtime);
                let zero_loss = matches!(session_family, CrashFamily::ZeroLoss);
                sim.probe_post_close(root, epoch, zero_loss, zero_loss && !deferred)?;
                family_next_open = CrashFamily::ZeroLoss;
            }
            EpochEnding::OrphanedSnapshot => {
                drop(runtime);
                if let Some(planted) = plant_orphaned_snapshot(root)? {
                    sim.facts.orphaned_snapshots_planted += 1;
                    sim.planted_orphan = Some(planted);
                }
                family_next_open = CrashFamily::ZeroLoss;
            }
            EpochEnding::Crash(model) => {
                drop(runtime);
                // Ordering is judged before the power cut (the stream ends
                // there); CONFIRMED violations fail the run regardless of the
                // impending crash.
                super::faults::require_no_confirmed_ordering_violations(
                    &backend,
                    seed,
                    "whole-db epoch",
                )?;
                let _perturbed = backend.reordering_crash(model, seed ^ (epoch as u64))?;
                family_next_open = if matches!(sim.durability, StorageDurabilityPolicy::Always) {
                    CrashFamily::ZeroLoss
                } else {
                    CrashFamily::OnDiskDamage
                };
            }
        }
    }

    // Final quiesce epoch: clean reopen, reconcile, capture terminal state.
    let backend = StorageBackend::write_ordering_reordering_local_fs(root.to_path_buf());
    let opened = StorageRuntime::open_with_backend(
        super::faults::deterministic_options(StorageDurabilityPolicy::Standard)
            .with_strict_recovery(false),
        &backend,
    );
    let (mut runtime, open_summary) = match opened {
        Ok(outcome) => {
            let (runtime, summary) = outcome.into_parts();
            (runtime, Some(summary))
        }
        Err(_error) if matches!(family_next_open, CrashFamily::OnDiskDamage) => {
            sim.facts.fail_loud_epochs += 1;
            sim.facts.epoch_endings.push("fail_loud_open");
            return Ok(sim.facts);
        }
        Err(error) => {
            return Err(TestkitError::new(format!(
                "[seed={seed}] final reopen failed: {error:?}"
            )));
        }
    };
    sim.reconcile_after_reopen(
        &mut runtime,
        family_next_open,
        epochs,
        open_summary.and_then(StorageOpenSummary::recovered_visible_version),
    )?;
    sim.fold_session_reclaim(&runtime, epochs)?;
    for branch in sim.live_branches() {
        let state = scan_recovered(
            &runtime,
            branch,
            &oracle_space(),
            &oracle_prefix_key(),
            SCAN_LIMIT,
        )?;
        sim.facts.final_states.push((branch_label(branch), state));
        sim.facts.final_live_branches.push(branch_label(branch));
    }
    Ok(sim.facts)
}

#[cfg(test)]
mod tests {
    use super::run_whole_db_sim;

    /// The determinism guard at whole-DB scope: one seed, two directories,
    /// bit-identical multi-epoch trajectories.
    #[test]
    fn whole_db_sim_replays_bit_exact() {
        let dir_a = tempfile::tempdir().expect("tmp");
        let dir_b = tempfile::tempdir().expect("tmp");
        let first = run_whole_db_sim(dir_a.path(), 11, 3, 24).expect("first run");
        let second = run_whole_db_sim(dir_b.path(), 11, 3, 24).expect("second run");
        assert_eq!(
            first, second,
            "same seed produced divergent whole-DB trajectories"
        );
    }

    /// The sweep exercises the whole grammar (non-vacuity): forks happen,
    /// crashes happen, temporal probes succeed, and EVERY seed completes —
    /// both harness-era allowances (#2820, #2823) are gone.
    #[test]
    fn whole_db_sweep_is_non_vacuous() {
        let mut forks = 0;
        let mut crashes = 0;
        let mut probes = 0;
        let mut bytes_reclaimed = 0;
        let mut clean_closes = 0;
        let mut orphans_planted = 0;
        let mut audits = 0;
        for seed in 0..6u64 {
            let dir = tempfile::tempdir().expect("tmp");
            let facts = run_whole_db_sim(dir.path(), seed, 3, 24)
                .unwrap_or_else(|error| panic!("seed {seed}: {error:?}"));
            forks += facts.forks();
            crashes += facts.crashed_epochs();
            probes += facts.temporal_probes_ok();
            bytes_reclaimed += facts.bytes_reclaimed();
            clean_closes += facts.clean_closes();
            orphans_planted += facts.orphaned_snapshots_planted();
            audits += facts.footprint_audits().len();
        }
        assert!(forks > 0, "no fork ever happened across the sweep");
        assert!(crashes > 0, "no epoch ever crashed across the sweep");
        assert!(probes > 0, "no temporal probe ever succeeded");
        // Space-reclamation contract (slice 13): the oracle is not vacuous —
        // reclaim released bytes, clean closes happened, orphaned snapshots
        // were planted for the open reconcile, and audits were taken.
        assert!(bytes_reclaimed > 0, "no reclaim pass ever released bytes");
        assert!(clean_closes > 0, "no epoch ever closed cleanly");
        assert!(orphans_planted > 0, "no orphaned snapshot was ever planted");
        assert!(audits > 0, "no footprint audit was taken");
    }

    /// Promoted from the #2820 gate-7 pin (DUR-008): a trajectory where the
    /// durable delete REFUSED a recovery-dependent fork source completes
    /// cleanly, with legal deletes still working on the same run — the
    /// refusal fired (count pinned) and every reopen succeeded.
    #[test]
    fn fork_source_deletion_is_refused_and_recovery_survives() {
        let dir = tempfile::tempdir().expect("tmp");
        let facts = run_whole_db_sim(dir.path(), 6, 3, 24)
            .expect("a refusal-bearing trajectory completes cleanly");
        assert_eq!(
            facts.deletes_refused, 1,
            "the DUR-008 refusal never fired on the pinned trajectory: {facts:?}"
        );
        // Re-pinned for slice 13: the reclaim-cadence action and the
        // clean-close / orphaned-snapshot endings shift every rng draw.
        assert_eq!(facts.deletes, 1, "legal deletes must still work: {facts:?}");
    }

    /// Promoted from the #2823 gate-7 pin: the trajectory that once refused
    /// its reopen (a replay-redundant fork source whose fork snapshot hit
    /// byte-identical duplicate internal keys) now completes — identical
    /// redundancy collapses to one row (ACID-005), divergent duplicates
    /// still refuse.
    #[test]
    fn replay_redundant_fork_sources_recover_cleanly() {
        let dir = tempfile::tempdir().expect("tmp");
        run_whole_db_sim(dir.path(), 2, 3, 24).expect("the once-refusing seed completes cleanly");
    }

    /// Promoted from the #2831 gate-7 pin: the trajectory that once refused
    /// a live fork (and then the final recovery) on replay-redundant sources
    /// — byte-identical duplicate internal keys across sealed tables, the
    /// ACID-005 class at the inherited-layer, compaction-levels, and
    /// materialization validators — now completes end-to-end: identical
    /// redundancy collapses everywhere, divergent duplicates still refuse
    /// (the #2825 boundary, uniformly applied).
    #[test]
    fn replay_redundant_sources_fork_and_recover_cleanly() {
        let dir = tempfile::tempdir().expect("tmp");
        run_whole_db_sim(dir.path(), 28, 3, 24).expect("the once-refusing seed completes cleanly");
    }

    /// Promoted from the #2827 gate-7 pin: the trajectory that once bricked
    /// its epoch-2 reopen (a child manifest referencing a fork-materialized
    /// table object the `SplitRename` crash model had ILLEGALLY dropped — the
    /// production publish discipline dir-fsyncs every file birth, so a
    /// completed publish cannot vanish on power loss) now completes cleanly:
    /// the model correction removed the counterfactual damage.
    #[test]
    fn fork_object_publishes_survive_power_loss_models() {
        let dir = tempfile::tempdir().expect("tmp");
        run_whole_db_sim(dir.path(), 10, 3, 24).expect("the once-bricked seed completes cleanly");
    }

    /// Sabotage twin for the footprint oracle: table-object debt no reclaim
    /// pass touched (two flushed tables a compaction superseded, the chained
    /// mark never drained) must fail a clean-footprint audit — the ledger
    /// recorded no deferral, so the oracle must say reclaim did not run.
    #[test]
    fn sabotage_unreclaimed_debt_is_caught() {
        use crate::api::{
            CommitBatch, CommitOptions, MaintenanceRequest, MaintenanceScope, MaintenanceTask,
            StorageBackend, StorageDurabilityPolicy, StorageMaintenanceSchedulingPolicy,
            StorageOpenOptions, StorageRuntime,
        };
        use crate::testkit::recovery_oracle::workload::to_commit_mutation;

        // Evaluate-and-enqueue: the compaction's chained mark is queued and
        // never run (the inline scheduler would reclaim it at once — slice 8).
        let dir = tempfile::tempdir().expect("tmp");
        let backend = StorageBackend::local_fs(dir.path().to_path_buf());
        let mut runtime = StorageRuntime::open_with_backend(
            StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
                .with_maintenance_scheduling_policy(
                    StorageMaintenanceSchedulingPolicy::EvaluateAndEnqueue,
                ),
            &backend,
        )
        .expect("open")
        .into_runtime();
        let mut sim = super::WholeDbSim::new(0, 4);
        for index in 0..2 {
            let mutations = sim.workload[index].clone();
            let batch = CommitBatch::new(
                super::default_branch(),
                mutations.iter().map(to_commit_mutation).collect(),
                CommitOptions::default(),
            )
            .expect("batch");
            runtime.commit(&batch).expect("commit");
            runtime
                .flush_default_branch_for_test()
                .expect("flush one table");
        }
        runtime
            .maintenance(&MaintenanceRequest::new(
                MaintenanceTask::Compact,
                MaintenanceScope::Branch(super::default_branch()),
            ))
            .expect("compaction supersedes both tables");

        // Clean expectations against undrained debt: the audit must fail.
        let verdict = sim.audit_footprint(&runtime, 0, "twin", super::FootprintExpectation::CLEAN);
        assert!(
            verdict.is_err(),
            "unreclaimed table-object debt passed the footprint oracle — it is vacuous: {:?}",
            sim.facts.footprint_audits().last()
        );
        // The same reading under no expectation is recorded, not failed.
        sim.audit_footprint(&runtime, 0, "twin", super::FootprintExpectation::NONE)
            .expect("recording only");
        let last = sim.facts.footprint_audits().last().expect("recorded");
        assert!(last.unreferenced_objects >= 2, "{last:?}");
    }

    /// Sabotage twin for the superseded-snapshot half of the oracle: a
    /// snapshot below the manifest-live one that no prune removed must fail a
    /// clean audit. (The sweep's trajectories rarely end a checkpointed
    /// session cleanly, so this twin is what proves the check can fire.)
    #[test]
    fn sabotage_superseded_snapshot_is_caught() {
        use crate::api::{
            CommitBatch, CommitOptions, MaintenanceRequest, MaintenanceScope, MaintenanceTask,
            StorageBackend, StorageDurabilityPolicy, StorageRuntime,
        };
        use crate::layout::ObjectLayout;
        use crate::testkit::recovery_oracle::workload::to_commit_mutation;

        let dir = tempfile::tempdir().expect("tmp");
        let backend = StorageBackend::local_fs(dir.path().to_path_buf());
        let mut runtime = StorageRuntime::open_with_backend(
            crate::testkit::simulation::faults::deterministic_options(
                StorageDurabilityPolicy::Standard,
            ),
            &backend,
        )
        .expect("open")
        .into_runtime();
        let mut sim = super::WholeDbSim::new(0, 4);
        // Two checkpoints: the second supersedes the first, and its chained
        // `Superseded` prune removes it.
        for index in 0..2 {
            let batch = CommitBatch::new(
                super::default_branch(),
                sim.workload[index].iter().map(to_commit_mutation).collect(),
                CommitOptions::default(),
            )
            .expect("batch");
            runtime.commit(&batch).expect("commit");
            runtime
                .maintenance(&MaintenanceRequest::new(
                    MaintenanceTask::Checkpoint,
                    MaintenanceScope::Global,
                ))
                .expect("checkpoint");
            runtime
                .drain_maintenance()
                .expect("drain the chained prune");
        }
        sim.audit_footprint(&runtime, 0, "twin", super::FootprintExpectation::CLEAN)
            .expect("the chained prune left one snapshot");

        // Re-create the superseded predecessor of the live snapshot.
        let snapshots = dir.path().join(
            ObjectLayout::snapshot_prefix()
                .expect("prefix")
                .as_str()
                .trim_end_matches('/'),
        );
        let live = std::fs::read_dir(&snapshots)
            .expect("snapshot family")
            .flatten()
            .filter_map(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .and_then(|name| name.strip_suffix(".object@"))
                    .and_then(|stem| u64::from_str_radix(stem, 16).ok())
            })
            .max()
            .expect("a live snapshot");
        assert!(live >= 2, "the second checkpoint superseded the first");
        let object = |id: u64| {
            dir.path().join(format!(
                "{}.object@",
                ObjectLayout::snapshot(id).expect("layout").as_str()
            ))
        };
        std::fs::copy(object(live), object(live - 1)).expect("plant a superseded snapshot");

        let verdict = sim.audit_footprint(&runtime, 0, "twin", super::FootprintExpectation::CLEAN);
        assert!(
            verdict.is_err(),
            "a superseded snapshot passed the footprint oracle — it is vacuous"
        );
        let last = sim.facts.footprint_audits().last().expect("recorded");
        assert_eq!(last.superseded_snapshots, 1, "{last:?}");
    }

    /// #3643 non-vacuity twin: a timeline segment the live snapshot no longer
    /// references, sealed below it, is superseded debt the footprint oracle
    /// must see — the snapshot family counts its segments.
    #[test]
    fn sabotage_unreferenced_timeline_segment_is_caught() {
        use crate::api::{
            CommitBatch, CommitOptions, MaintenanceRequest, MaintenanceScope, MaintenanceTask,
            StorageBackend, StorageDurabilityPolicy, StorageRuntime,
        };
        use crate::layout::{ObjectLayout, TimelineSegmentId};
        use crate::testkit::recovery_oracle::workload::to_commit_mutation;

        let dir = tempfile::tempdir().expect("tmp");
        let backend = StorageBackend::local_fs(dir.path().to_path_buf());
        let mut runtime = StorageRuntime::open_with_backend(
            crate::testkit::simulation::faults::deterministic_options(
                StorageDurabilityPolicy::Standard,
            ),
            &backend,
        )
        .expect("open")
        .into_runtime();
        let mut sim = super::WholeDbSim::new(0, 4);
        for index in 0..2 {
            let batch = CommitBatch::new(
                super::default_branch(),
                sim.workload[index].iter().map(to_commit_mutation).collect(),
                CommitOptions::default(),
            )
            .expect("batch");
            runtime.commit(&batch).expect("commit");
            runtime
                .maintenance(&MaintenanceRequest::new(
                    MaintenanceTask::Checkpoint,
                    MaintenanceScope::Global,
                ))
                .expect("checkpoint");
            runtime
                .drain_maintenance()
                .expect("drain the chained prune");
        }
        sim.audit_footprint(&runtime, 0, "twin", super::FootprintExpectation::CLEAN)
            .expect("the chained prune left only live segments");

        // Plant a segment sealed by snapshot 1 that nothing references.
        let segment = |id: TimelineSegmentId| {
            dir.path().join(format!(
                "{}.object@",
                ObjectLayout::timeline_segment(id).expect("layout").as_str()
            ))
        };
        let live_tail = std::fs::read_dir(dir.path().join("timeline"))
            .expect("timeline family")
            .flatten()
            .flat_map(|sealing| {
                std::fs::read_dir(sealing.path())
                    .expect("sealing")
                    .flatten()
            })
            .map(|entry| entry.path())
            .next()
            .expect("a live segment");
        let planted = segment(TimelineSegmentId {
            sealing_snapshot_id: 1,
            ordinal: 7,
        });
        std::fs::create_dir_all(planted.parent().expect("dir")).expect("mkdir");
        std::fs::copy(live_tail, planted).expect("plant an unreferenced segment");

        let verdict = sim.audit_footprint(&runtime, 0, "twin", super::FootprintExpectation::CLEAN);
        assert!(
            verdict.is_err(),
            "an unreferenced timeline segment passed the footprint oracle — it is vacuous"
        );
    }

    /// Sabotage twin: a fork whose model seeding is SKIPPED must fire the
    /// per-branch prefix oracle — inherited rows with no model history are
    /// phantoms. Proves the fork-seeding half of the oracle is load-bearing.
    #[test]
    fn sabotage_unseeded_fork_is_caught() {
        use crate::api::{
            BranchAction, BranchGeneration, BranchRequest, CommitBatch, CommitOptions,
            StorageBackend, StorageDurabilityPolicy, StorageRuntime,
        };
        use crate::testkit::recovery_oracle::verify::CrashFamily;
        use crate::testkit::recovery_oracle::workload::to_commit_mutation;

        let dir = tempfile::tempdir().expect("tmp");
        let backend = StorageBackend::local_fs(dir.path().to_path_buf());
        let runtime = StorageRuntime::open_with_backend(
            crate::testkit::simulation::faults::deterministic_options(
                StorageDurabilityPolicy::Standard,
            ),
            &backend,
        )
        .expect("open")
        .into_runtime();

        let mut sim = super::WholeDbSim::new(0, 4);
        // One committed batch on the default branch, mirrored into the model.
        let mutations = sim.workload[0].clone();
        let batch = CommitBatch::new(
            super::default_branch(),
            mutations.iter().map(to_commit_mutation).collect(),
            CommitOptions::default(),
        )
        .expect("batch");
        let summary = runtime.commit(&batch).expect("commit");
        sim.model
            .record_ack(super::default_branch(), summary.commit_version(), mutations);

        // Fork b0 through the runtime but DELIBERATELY skip seed_fork_model.
        let target = super::pool_branch(0);
        runtime
            .branch(&BranchRequest::new(
                target,
                BranchAction::ForkCurrent {
                    source: super::default_branch(),
                },
                Some(BranchGeneration::new(1)),
            ))
            .expect("fork");

        let verdict = sim.assert_branch_prefix(&runtime, target, CrashFamily::ZeroLoss, 0, "twin");
        assert!(
            verdict.is_err(),
            "an unseeded fork passed the prefix oracle — the fork-seeding half is vacuous"
        );
    }

    /// Grammar labels are stable identifiers (they feed the bit-exact
    /// action trace, which cannot police its own labels — a mutated label
    /// mutates both replay runs identically).
    #[test]
    fn action_labels_are_stable() {
        use super::DbAction;
        let expected = [
            (DbAction::Commit, "commit"),
            (DbAction::ForkCurrent, "fork_current"),
            (DbAction::ForkAtVersion, "fork_at_version"),
            (DbAction::DeleteBranch, "delete_branch"),
            (DbAction::RecreateBranch, "recreate_branch"),
            (DbAction::DrainMaintenance, "drain_maintenance"),
            (DbAction::EnqueueFlush, "enqueue_flush"),
            (DbAction::EnqueueCheckpoint, "enqueue_checkpoint"),
            (DbAction::AdvanceClock(7), "advance_clock"),
            (DbAction::ReclaimCadence, "reclaim_cadence"),
        ];
        for (action, label) in expected {
            assert_eq!(action.label(), label);
        }
    }

    /// Seed 0's exact action mix, pinned: the bit-exact trace makes label
    /// counts constants of the seed, and asserting them kills grammar-arm
    /// and label mutants that both the counter smoke and the replay twins
    /// structurally miss (a mutated arm or label mutates both twin runs
    /// identically). Re-pin when the grammar deliberately changes.
    #[test]
    fn pinned_seed_action_mix_is_stable() {
        let dir = tempfile::tempdir().expect("tmp");
        let facts = run_whole_db_sim(dir.path(), 0, 3, 24).expect("run");
        let mut counts = std::collections::BTreeMap::new();
        for label in &facts.action_trace {
            *counts.entry(*label).or_insert(0usize) += 1;
        }
        // Re-pinned for #2853: previously-refused fork-at-version calls now
        // succeed inside trajectories, changing live-branch sets and the
        // conditional rng draws downstream (a deliberate semantic change).
        // Re-pinned for slice 13: `reclaim_cadence` joins the grammar and the
        // epoch endings draw from a wider range (a deliberate change).
        let expected: std::collections::BTreeMap<&str, usize> = [
            ("advance_clock", 6),
            ("commit", 32),
            ("delete_branch", 6),
            ("drain_maintenance", 7),
            ("enqueue_checkpoint", 5),
            ("fork_at_version", 3),
            ("fork_current", 2),
            ("reclaim_cadence", 3),
            ("recreate_branch", 8),
        ]
        .into_iter()
        .collect();
        assert_eq!(counts, expected, "seed 0's action mix drifted");
        // The per-run facts counters, exactly (the sweep sums can mask
        // per-run constant mutants by coincidence) — including the
        // no-op/unavailable counters nothing else observes.
        assert_eq!(
            (
                facts.deletes,
                facts.forks,
                facts.recreates,
                facts.deletes_refused,
                facts.forks_unavailable,
                facts.temporal_probes_unavailable,
            ),
            // Re-pinned for slice 13 (the grammar and endings changed).
            (2, 3, 2, 0, 0, 0),
            "per-run facts drifted: (deletes, forks, recreates, deletes_refused, forks_unavailable, probes_unavailable)",
        );
        // The session reclaim ledgers, folded across epochs: a constant of
        // the seed like every other counter.
        // Re-pinned for slice 12: checkpoints no longer defer on a flushed
        // non-seeded branch, so more checkpoints complete and more reclaim
        // passes follow them.
        assert_eq!(facts.reclaim_passes(), 18, "{facts:?}");
    }

    /// Pool ids and branch labels are stable identifiers.
    #[test]
    fn pool_ids_and_branch_labels_are_stable() {
        assert_eq!(super::pool_branch(0).as_bytes()[0], 0xB0);
        assert_eq!(super::pool_branch(3).as_bytes()[0], 0xB3);
        assert_eq!(
            super::branch_label(crate::testkit::recovery_oracle::workload::default_branch()),
            "default"
        );
        assert_eq!(super::branch_label(super::pool_branch(2)), "pool-b2");
    }

    /// The facts accessors at values a constant cannot fake: seed 4 counts
    /// three deletes and seed 5 one (re-pinned for slice 13; seed 0 counts
    /// two) — a `-> 1` accessor mutant survived
    /// three rounds because every other pinned config truly had one delete
    /// (and the sweep sum coincidentally matched three ones).
    #[test]
    fn facts_accessors_report_distinct_pinned_values() {
        let dir_a = tempfile::tempdir().expect("tmp");
        let four = run_whole_db_sim(dir_a.path(), 4, 3, 24).expect("seed 4");
        assert_eq!(four.deletes(), 3, "{four:?}");
        // Seed 4 ends two epochs by planting an orphaned snapshot.
        assert_eq!(four.orphaned_snapshots_planted, 2, "{four:?}");
        // Seed 4 recreates once — the counter's only >0 pin (seed 0 is 0).
        assert_eq!(four.recreates, 1, "{four:?}");
        let dir_b = tempfile::tempdir().expect("tmp");
        let five = run_whole_db_sim(dir_b.path(), 5, 3, 24).expect("seed 5");
        assert_eq!(five.deletes(), 1, "{five:?}");
    }

    /// Distinct seeds diverge (the explorer is not degenerate).
    #[test]
    fn whole_db_distinct_seeds_diverge() {
        let dir_a = tempfile::tempdir().expect("tmp");
        let dir_b = tempfile::tempdir().expect("tmp");
        let a = run_whole_db_sim(dir_a.path(), 1, 2, 24).expect("run a");
        let b = run_whole_db_sim(dir_b.path(), 3, 2, 24).expect("run b");
        assert_ne!(a, b, "distinct seeds produced identical trajectories");
    }

    /// #3502 Slice E: a deterministic pruning trajectory through the harness's
    /// own durable open path. A re-write-heavy history for one key is flushed
    /// into two L0 tables and then compacted under an opt-in retention window,
    /// proving the retained-history contract end to end: the floor advances
    /// (pruning FIRED — non-vacuity, via `retained_history_floor_for_test`), an
    /// at-version read below the floor RAISES `RetainedHistoryUnavailable`, the
    /// latest value still reads exactly, and a fork below the floor is refused
    /// (D2's guard, exercised through the simulation surface). This closes the
    /// gap the temporal/fork oracles left vacuous under the default `KeepAll`
    /// (the never-pruned sweep pins are untouched; exhaustive pruning fuzzing
    /// across the fault matrix is the E1b follow-up).
    #[test]
    fn pruning_enforces_the_retained_floor_on_reads_and_forks() {
        use crate::api::{
            BranchAction, BranchGeneration, BranchRequest, CommitBatch, CommitMutation,
            CommitOptions, MaintenanceRequest, MaintenanceScope, MaintenanceTask, PointReadRequest,
            ReadBound, StorageApiError, StorageBackend, StorageDurabilityPolicy, StorageKey,
            StorageRuntime, StorageSpaceId, StorageValue,
        };
        use strata_core::CommitVersion;

        let dir = tempfile::tempdir().expect("tmp");
        let backend = StorageBackend::local_fs(dir.path().to_path_buf());
        let mut runtime = StorageRuntime::open_with_backend(
            crate::testkit::simulation::faults::deterministic_options(
                StorageDurabilityPolicy::Standard,
            )
            .with_version_retention_window(Some(1)),
            &backend,
        )
        .expect("open")
        .into_runtime();

        let space = StorageSpaceId::new(vec![0x20]).expect("space");
        let key = StorageKey::new(b"k".to_vec()).expect("key");
        let put = |v: u8| {
            CommitBatch::new(
                super::default_branch(),
                vec![CommitMutation::Put {
                    storage_space: space.clone(),
                    key: key.clone(),
                    value: StorageValue::new(vec![b'v', v]),
                    ttl: None,
                }],
                CommitOptions::default(),
            )
            .expect("batch")
        };
        let flush = MaintenanceRequest::new(
            MaintenanceTask::Flush,
            MaintenanceScope::Branch(super::default_branch()),
        );

        // Two flushed L0 tables of three versions each (below the flush-followup
        // threshold, so nothing auto-races), then a forced pruning compaction.
        let mut versions = Vec::new();
        for v in 0..3u8 {
            versions.push(runtime.commit(&put(v)).expect("commit").commit_version());
        }
        runtime.maintenance(&flush).expect("flush first table");
        for v in 3..6u8 {
            versions.push(runtime.commit(&put(v)).expect("commit").commit_version());
        }
        runtime.maintenance(&flush).expect("flush second table");
        runtime
            .force_branch_compaction_for_test(super::default_branch())
            .expect("forced pruning compaction");

        // Non-vacuity: pruning actually published a floor above the origin.
        let floor = runtime
            .retained_history_floor_for_test(super::default_branch())
            .expect("floor query")
            .expect("a retained-history floor is published after pruning");
        assert!(
            floor > CommitVersion::ZERO,
            "floor did not advance: {floor:?}"
        );

        // Below the floor: the oldest version RAISES rather than serving a
        // too-new survivor.
        let err = runtime
            .read_point(&PointReadRequest::new(
                super::default_branch(),
                space.clone(),
                key.clone(),
                ReadBound::AtVersion(versions[0]),
            ))
            .expect_err("below-floor at-version read is unavailable");
        assert!(
            matches!(err, StorageApiError::RetainedHistoryUnavailable { .. }),
            "expected RetainedHistoryUnavailable, got {err:?}"
        );

        // The latest value still reads exactly.
        let latest = runtime
            .read_point(&PointReadRequest::new(
                super::default_branch(),
                space.clone(),
                key.clone(),
                ReadBound::Latest,
            ))
            .expect("latest read succeeds");
        assert_eq!(
            latest
                .row()
                .expect("row")
                .value()
                .expect("value")
                .as_bytes(),
            &[b'v', 5]
        );

        // A fork below the floor is refused (D2's fork guard, through the sim).
        let fork_err = runtime
            .branch(&BranchRequest::new(
                super::pool_branch(0),
                BranchAction::ForkAtVersion {
                    source: super::default_branch(),
                    version: versions[0],
                },
                Some(BranchGeneration::new(1)),
            ))
            .expect_err("below-floor fork is refused");
        assert!(
            matches!(fork_err, StorageApiError::RetainedHistoryUnavailable { .. }),
            "expected RetainedHistoryUnavailable, got {fork_err:?}"
        );
    }
}
