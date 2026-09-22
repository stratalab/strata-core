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
    MaintenanceRequest, MaintenanceScope, MaintenanceTask, PrefixScanReadRequest, ReadBound,
    StorageBackend, StorageDurabilityPolicy, StorageOpenSummary, StorageRuntime,
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
    match rng.gen_u8_below(16) {
        // Commits dominate so every other action races real write load.
        0..=6 => DbAction::Commit,
        7 => DbAction::ForkCurrent,
        8 => DbAction::ForkAtVersion,
        9 => DbAction::DeleteBranch,
        10 => DbAction::RecreateBranch,
        11 => DbAction::DrainMaintenance,
        12 => DbAction::EnqueueFlush,
        13 => DbAction::EnqueueCheckpoint,
        _ => DbAction::AdvanceClock(u64::from(rng.gen_u8_below(50))),
    }
}

/// How a seeded epoch ends.
#[derive(Clone, Copy, Debug)]
enum EpochEnding {
    CleanDrop,
    Crash(FsModel),
}

impl EpochEnding {
    fn draw(rng: &mut SplitMix64) -> Self {
        // Crashes dominate: the clean drop keeps the zero-loss reopen
        // direction covered without dominating the sweep.
        match rng.gen_u8_below(6) {
            0 => EpochEnding::CleanDrop,
            n => EpochEnding::Crash(FS_MODELS[usize::from((n - 1) % 4)]),
        }
    }

    const fn label(self) -> &'static str {
        match self {
            EpochEnding::CleanDrop => "clean_drop",
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
}

impl WholeDbFacts {
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
        runtime: &StorageRuntime<'_>,
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
        Ok(())
    }
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

        if epoch > 0 {
            sim.reconcile_after_reopen(
                &runtime,
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
        match ending {
            EpochEnding::CleanDrop => {
                drop(runtime);
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
    let (runtime, open_summary) = match opened {
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
        &runtime,
        family_next_open,
        epochs,
        open_summary.and_then(StorageOpenSummary::recovered_visible_version),
    )?;
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
        for seed in 0..6u64 {
            let dir = tempfile::tempdir().expect("tmp");
            let facts = run_whole_db_sim(dir.path(), seed, 3, 24)
                .unwrap_or_else(|error| panic!("seed {seed}: {error:?}"));
            forks += facts.forks();
            crashes += facts.crashed_epochs();
            probes += facts.temporal_probes_ok();
        }
        assert!(forks > 0, "no fork ever happened across the sweep");
        assert!(crashes > 0, "no epoch ever crashed across the sweep");
        assert!(probes > 0, "no temporal probe ever succeeded");
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
        assert_eq!(facts.deletes, 3, "legal deletes must still work: {facts:?}");
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
        let expected: std::collections::BTreeMap<&str, usize> = [
            ("advance_clock", 11),
            ("commit", 37),
            ("delete_branch", 5),
            ("drain_maintenance", 2),
            ("enqueue_checkpoint", 3),
            ("enqueue_flush", 5),
            ("fork_at_version", 4),
            ("fork_current", 2),
            ("recreate_branch", 3),
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
            // Re-pinned for #2853: two previously-refused fork-at-version
            // calls and two temporal probes now succeed on surviving coverage.
            (1, 5, 0, 0, 0, 3),
            "per-run facts drifted: (deletes, forks, recreates, deletes_refused, forks_unavailable, probes_unavailable)",
        );
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
    /// two deletes and seed 5 zero — a `-> 1` accessor mutant survived
    /// three rounds because every other pinned config truly had one delete
    /// (and the sweep sum coincidentally matched three ones).
    #[test]
    fn facts_accessors_report_distinct_pinned_values() {
        let dir_a = tempfile::tempdir().expect("tmp");
        let four = run_whole_db_sim(dir_a.path(), 4, 3, 24).expect("seed 4");
        assert_eq!(four.deletes(), 2, "{four:?}");
        // Seed 4 recreates once — the counter's only >0 pin (seed 0 is 0).
        assert_eq!(four.recreates, 1, "{four:?}");
        let dir_b = tempfile::tempdir().expect("tmp");
        let five = run_whole_db_sim(dir_b.path(), 5, 3, 24).expect("seed 5");
        assert_eq!(five.deletes(), 0, "{five:?}");
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
