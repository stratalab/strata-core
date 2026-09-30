//! Durable WAL-backed commit execution.

use super::cache::prepare_commit_rows;
use super::durable_gate::CommitUnresolvedDurableAdmission;
use super::{
    admit_mutating_commit, commit_conflict_validation_needs_source, require_wal_record_fits,
    validate_commit_conflicts, validate_commit_conflicts_without_source, CommitBatch,
    CommitBatchKind, CommitBranchGenerationGuard, CommitBranchGuardSet,
    CommitBranchReadViewConflictSource, CommitBranchRegistry, CommitDurabilityClass,
    CommitDurabilityMode, CommitFactAllocation, CommitFactAllocator, CommitLowerLayer,
    CommitOutcome, CommitRuntimeConfig, CommitRuntimeError, CommitRuntimeResult, CommitStamp,
    CommitTimestampSource, CommitUnresolvedDurable, CommitUnresolvedDurableGate,
    CommitVisibilityFacts, ValidatedCommitBatch, VisibleVersionPublish, VisibleVersionTracker,
};
use crate::branch::read::BranchReadView;
use crate::branch::state::BranchLocalState;
use crate::config::mode::DurabilityPolicy;
use crate::format::{WalCommitPayload, WalRecord};
use crate::observability::perf_trace;
use crate::row::StorageRow;
use crate::service::{WalAppend, WalService, WalServiceError};
use strata_core::{BranchId, CommitVersion, Timestamp};

#[derive(Debug)]
pub(crate) struct CommitDurableRuntime<'a, S, W, B = BranchLocalState, V = VisibleVersionTracker> {
    config: &'a CommitRuntimeConfig,
    registry: &'a CommitBranchRegistry,
    guard_set: &'a CommitBranchGuardSet,
    allocator: &'a mut CommitFactAllocator<S>,
    branch: &'a mut B,
    visible: &'a mut V,
    wal: &'a mut W,
    durable_gate: &'a CommitUnresolvedDurableGate,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CommitWalAppendFacts {
    segment_id: u64,
    start_offset: u64,
    bytes_written: u64,
    dirty_bytes: u64,
    forced_durable: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct CommitWalAppendError {
    durability_uncertain: bool,
    error: CommitRuntimeError,
}

pub(crate) trait CommitWalAppender {
    fn durability_policy(&self) -> DurabilityPolicy;

    /// The largest record frame this appender accepts. The durable runtime
    /// refuses a larger batch before allocating its commit version (#3698).
    /// Defaults to the production segment's limit for appenders with no
    /// segment of their own (test fakes).
    fn record_frame_limit(&self) -> u64 {
        crate::format::default_wal_record_frame_limit()
    }

    fn append_commit_record(
        &mut self,
        record: &WalRecord,
    ) -> Result<CommitWalAppendFacts, CommitWalAppendError>;

    /// Append without the `Always` policy's inline fsync (BS5.1 write groups). The group leader
    /// owns durability via one covering fsync (a group-sync ticket in BS5.2). Defaults to the
    /// plain append for appenders with no per-append fsync (test fakes; `Standard`-style
    /// appenders), where the two are equivalent.
    fn append_commit_record_deferring_durability(
        &mut self,
        record: &WalRecord,
    ) -> Result<CommitWalAppendFacts, CommitWalAppendError> {
        self.append_commit_record(record)
    }
}

/// Shared state for one write group (BS5.1): the leader constructs it at group start, each
/// member's execution threads through it, and the leader finalizes — one covering fsync for
/// `Always`, one visible publish to the group's max version. `frontier` carries
/// max(visible-at-group-start, versions applied by earlier members): with the publish deferred,
/// same-branch members validate conflicts and apply against it exactly as if every earlier member
/// had already published — serial-equivalent semantics.
#[derive(Debug)]
pub(crate) struct CommitGroupState {
    frontier: CommitVersion,
    first_version: Option<CommitVersion>,
    last_stamp: Option<CommitStamp>,
    last_durability: Option<CommitDurabilityClass>,
}

impl CommitGroupState {
    pub(crate) const fn new(visible_at_group_start: CommitVersion) -> Self {
        Self {
            frontier: visible_at_group_start,
            first_version: None,
            last_stamp: None,
            last_durability: None,
        }
    }

    pub(crate) const fn last_stamp(&self) -> Option<CommitStamp> {
        self.last_stamp
    }

    fn record_member(&mut self, stamp: CommitStamp, durability: CommitDurabilityClass) {
        self.frontier = stamp.commit_version();
        if self.first_version.is_none() {
            self.first_version = Some(stamp.commit_version());
        }
        self.last_stamp = Some(stamp);
        self.last_durability = Some(durability);
    }

    /// Widen `fact` to cover the whole group's version block (group-of-1 widening is the
    /// identity, preserving the single-commit fact byte-for-byte).
    pub(crate) fn widen_fact(
        &self,
        fact: CommitUnresolvedDurable,
    ) -> CommitRuntimeResult<CommitUnresolvedDurable> {
        match self.first_version {
            Some(first) => fact.covering_group_from(first),
            None => Ok(fact),
        }
    }
}

/// How an execution participates in the write-group protocol (BS5.1). `Solo` is the canonical
/// single-commit path — byte-identical to the pre-group protocol (its own gate span, per-append
/// policy fsync, immediate publish). `Member` runs under the leader's gate span, defers fsync and
/// publish to the leader's finalize, and records group-widened unresolved facts on failure.
pub(crate) enum CommitGroupRole<'g> {
    Solo,
    Member(&'g mut CommitGroupState),
}

pub(crate) trait CommitBranchApplyTarget {
    fn branch_id(&self) -> BranchId;
    fn max_commit_version(&self) -> Option<CommitVersion>;
    fn capture_read_view(&self) -> CommitRuntimeResult<BranchReadView>;
    fn validate_committed_rows_before_apply(
        &self,
        _rows: &[StorageRow],
    ) -> CommitRuntimeResult<()> {
        Ok(())
    }
    fn append_committed_rows_atomically(
        &mut self,
        rows: Vec<StorageRow>,
    ) -> CommitRuntimeResult<()>;
    /// #3112 S2b: attaches the commit's wall-clock instant to the
    /// retained-timeline entry the apply funnel just created. `committed_at` is
    /// commit-scoped and deliberately absent from rows (storage-format spec §10
    /// req 13), so the row-driven funnel cannot carry it; the caller invokes
    /// this only AFTER the apply succeeds, so a rolled-back batch leaves no
    /// instant behind just as it leaves no timeline trace.
    ///
    /// Defaulted to a no-op: a target with no timeline index (test doubles,
    /// perf probes) simply leaves the instant unknown, which is a legal state
    /// rather than a defect.
    fn observe_commit_instant(&mut self, _commit_version: CommitVersion, _instant: Timestamp) {}
}

pub(crate) trait CommitVisiblePublisher {
    fn visible_version(&self) -> CommitVersion;
    fn publish_from_facts(
        &mut self,
        facts: CommitVisibilityFacts,
    ) -> CommitRuntimeResult<VisibleVersionPublish>;
}

impl CommitBranchApplyTarget for BranchLocalState {
    fn branch_id(&self) -> BranchId {
        BranchLocalState::branch_id(self)
    }

    fn max_commit_version(&self) -> Option<CommitVersion> {
        BranchLocalState::max_commit_version(self)
    }

    fn capture_read_view(&self) -> CommitRuntimeResult<BranchReadView> {
        BranchLocalState::capture_read_view(self).map_err(|source| {
            CommitRuntimeError::lower_layer_with(
                CommitLowerLayer::BranchRuntime,
                "branch read view capture failed",
                source,
            )
        })
    }

    fn validate_committed_rows_before_apply(
        &self,
        _rows: &[StorageRow],
    ) -> CommitRuntimeResult<()> {
        Ok(())
    }

    fn append_committed_rows_atomically(
        &mut self,
        rows: Vec<StorageRow>,
    ) -> CommitRuntimeResult<()> {
        BranchLocalState::append_committed_rows_atomically(self, rows)
            .map(|_| ())
            .map_err(|source| {
                CommitRuntimeError::lower_layer_with(
                    CommitLowerLayer::BranchRuntime,
                    "branch state rejected commit rows",
                    source,
                )
            })
    }

    fn observe_commit_instant(&mut self, commit_version: CommitVersion, instant: Timestamp) {
        self.retained_timeline()
            .observe_committed_at(commit_version, instant);
    }
}

impl CommitVisiblePublisher for VisibleVersionTracker {
    fn visible_version(&self) -> CommitVersion {
        VisibleVersionTracker::visible_version(*self)
    }

    fn publish_from_facts(
        &mut self,
        facts: CommitVisibilityFacts,
    ) -> CommitRuntimeResult<VisibleVersionPublish> {
        VisibleVersionTracker::publish_from_facts(self, facts)
    }
}

impl<'a, S, W, B, V> CommitDurableRuntime<'a, S, W, B, V> {
    pub(crate) fn new(
        config: &'a CommitRuntimeConfig,
        registry: &'a CommitBranchRegistry,
        guard_set: &'a CommitBranchGuardSet,
        allocator: &'a mut CommitFactAllocator<S>,
        branch: &'a mut B,
        visible: &'a mut V,
        wal: &'a mut W,
        durable_gate: &'a CommitUnresolvedDurableGate,
    ) -> Self {
        Self {
            config,
            registry,
            guard_set,
            allocator,
            branch,
            visible,
            wal,
            durable_gate,
        }
    }
}

impl<S, W, B, V> CommitDurableRuntime<'_, S, W, B, V>
where
    S: CommitTimestampSource,
    W: CommitWalAppender,
    B: CommitBranchApplyTarget,
    V: CommitVisiblePublisher,
{
    /// Validate the batch against same-branch conflicts, capturing a read-view
    /// source only when the batch's mutations require one. Extracted from
    /// `execute` to keep that function within the per-function line budget.
    fn validate_commit_conflicts_for_batch(
        &mut self,
        batch: &ValidatedCommitBatch,
        current_visible_version: strata_core::CommitVersion,
    ) -> CommitRuntimeResult<()> {
        let conflict_timer = perf_trace::start_timer();
        if commit_conflict_validation_needs_source(batch) {
            let read_view = self.branch.capture_read_view()?;
            perf_trace::record_conflict_source_built();
            let conflict_source = CommitBranchReadViewConflictSource::new_at_version(
                &read_view,
                current_visible_version,
            );
            validate_commit_conflicts(batch, &conflict_source)?;
        } else {
            validate_commit_conflicts_without_source(batch)?;
        }
        perf_trace::record_commit_exec_conflict_elapsed(conflict_timer);
        Ok(())
    }

    pub(crate) fn execute(
        &mut self,
        batch: CommitBatch,
        generation_guard: CommitBranchGenerationGuard,
    ) -> CommitRuntimeResult<CommitOutcome> {
        self.execute_with_role(batch, generation_guard, CommitGroupRole::Solo, None, None)
    }

    /// Solo execution with a pipeline visibility floor (BS5.2): the branch-
    /// not-ahead check and conflict bounds validate against
    /// `max(visible, floor)` so a solo commit admitted while earlier groups
    /// are applied-but-unpublished (their covering fsync in flight) keeps
    /// exact serial semantics. With an empty pipeline `floor == visible` and
    /// this is byte-identical to [`execute`].
    pub(crate) fn execute_with_visibility_floor(
        &mut self,
        batch: CommitBatch,
        generation_guard: CommitBranchGenerationGuard,
        floor: CommitVersion,
    ) -> CommitRuntimeResult<CommitOutcome> {
        self.execute_with_role(
            batch,
            generation_guard,
            CommitGroupRole::Solo,
            Some(floor),
            None,
        )
    }

    /// Execute one member of a write group (BS5.1). The caller is the group
    /// leader: it holds the runtime lock and one durable-gate admission span
    /// for the whole group, executes members sequentially in version order,
    /// and finalizes with [`finalize_commit_group`]. The member defers its
    /// fsync and visible publish to that finalize; its outcome must not be
    /// released to the committing caller until finalize succeeds.
    pub(crate) fn execute_group_member(
        &mut self,
        batch: CommitBatch,
        generation_guard: CommitBranchGenerationGuard,
        group: &mut CommitGroupState,
    ) -> CommitRuntimeResult<CommitOutcome> {
        self.execute_with_role(
            batch,
            generation_guard,
            CommitGroupRole::Member(group),
            None,
            None,
        )
    }

    /// Member execution with the memtable apply DEFERRED (BS5.4 parallel
    /// cross-branch apply): everything through the WAL append runs exactly as
    /// [`execute_group_member`], but the committed rows are returned through
    /// `deferred_rows` instead of applied — the caller applies them against
    /// the branch's checked-out state before the group publishes. Only legal
    /// for a branch's FIRST member in the group (a later same-branch member's
    /// conflict source must see the earlier rows applied); the caller owns
    /// that rule.
    pub(crate) fn execute_group_member_deferring_apply(
        &mut self,
        batch: CommitBatch,
        generation_guard: CommitBranchGenerationGuard,
        group: &mut CommitGroupState,
        deferred_rows: &mut Option<Vec<StorageRow>>,
    ) -> CommitRuntimeResult<CommitOutcome> {
        self.execute_with_role(
            batch,
            generation_guard,
            CommitGroupRole::Member(group),
            None,
            Some(deferred_rows),
        )
    }

    fn execute_with_role(
        &mut self,
        batch: CommitBatch,
        generation_guard: CommitBranchGenerationGuard,
        role: CommitGroupRole<'_>,
        visibility_floor: Option<CommitVersion>,
        deferred_rows: Option<&mut Option<Vec<StorageRow>>>,
    ) -> CommitRuntimeResult<CommitOutcome> {
        let admission_timer = perf_trace::start_timer();
        let batch = batch.validate(self.config)?;
        let (required_policy, durability) = require_durable_mutating_batch(&batch)?;
        let branch_id = batch.batch().branch_id();
        if self.wal.durability_policy() != required_policy {
            return Err(CommitRuntimeError::DurabilityUnavailable {
                reason: "durable commit executor requires matching WAL durability policy",
            });
        }
        // Before the durable gate, the branch guard and above all the version
        // allocator: an oversized record is a permanent caller error, and
        // refusing it here burns no commit version (#3698). Validation already
        // checked the production segment; this is the configured one.
        require_wal_record_fits(batch.wal_record_frame_len(), self.wal.record_frame_limit())?;
        if branch_id != self.branch.branch_id() {
            return Err(CommitRuntimeError::BranchMismatch {
                expected: branch_id,
                actual: self.branch.branch_id(),
            });
        }
        let admission_pressure = record_admission_pressure_facts(&batch, self.config)?;
        // The unresolved durable gate is global for V1 visible-version safety,
        // so check it before target-branch admission, allocation, or WAL work.
        //
        // Commit-runtime lock order: this call records global admission and
        // releases the gate mutex before branch registry validation or branch
        // guard acquisition. The global admission token remains active through
        // WAL append, apply, and visible publication, but it does not carry a mutex guard.
        //
        // Group members run inside the leader's admission span instead of
        // opening their own (BS5.1): the leader admitted once for the whole
        // group, and member failures record facts widened to the group's
        // version range.
        let mut participation = match role {
            CommitGroupRole::Solo => {
                CommitGateParticipation::Solo(self.durable_gate.admit_mutating_commit()?)
            }
            CommitGroupRole::Member(group) => CommitGateParticipation::Member(group),
        };

        // Keep this guard alive through WAL append, apply, gate recording, and
        // visible publication. Same-branch commits cannot observe a stale
        // conflict window while this token is held. Same-branch contention is
        // intentionally reported as a typed, fail-fast retry condition.
        let _admission_guard =
            admit_mutating_commit(self.registry, self.guard_set, &batch, generation_guard)?;
        // Group members bound conflict validation and apply order at the group
        // frontier: earlier members are applied but unpublished, and the
        // frontier stands in for the visible version each would have published
        // solo — exact serial conflict semantics for same-branch members. A
        // solo commit under the pipeline (BS5.2) gets the same treatment via
        // its visibility floor.
        let current_visible_version = match &participation {
            CommitGateParticipation::Solo(_) => self
                .visible
                .visible_version()
                .max(visibility_floor.unwrap_or(CommitVersion::ZERO)),
            CommitGateParticipation::Member(group) => {
                self.visible.visible_version().max(group.frontier)
            }
        };
        require_branch_not_ahead_of_visible(
            self.branch.max_commit_version(),
            current_visible_version,
        )?;
        perf_trace::record_commit_exec_admission_elapsed(admission_timer);

        self.validate_commit_conflicts_for_batch(&batch, current_visible_version)?;

        perf_trace::record_commit_admission_accepted_under_pressure(
            admission_pressure.under_pressure(),
        );
        let stage_timer = perf_trace::start_timer();
        let allocation = self.allocator.allocate_for_batch(&batch)?;
        let stamp = require_mutating_allocation(allocation)?;
        require_allocated_after_visible(stamp, current_visible_version)?;
        let (combined_rows, mutation_counts) = prepare_commit_rows(batch, stamp, self.config)?;
        self.branch
            .validate_committed_rows_before_apply(&combined_rows)?;
        perf_trace::record_commit_exec_stage_elapsed(stage_timer);
        let wal_record_rows = combined_rows.len();
        let wal_record_timer = perf_trace::start_timer();
        let record = build_wal_record(stamp, combined_rows)?;
        perf_trace::record_commit_wal_record_built(wal_record_timer, wal_record_rows);
        self.append_record_for_participation(
            &participation,
            &record,
            required_policy,
            branch_id,
            stamp.commit_version(),
        )?;

        let combined_rows = record.into_commit_payload().into_rows();
        if let Some(deferred) = deferred_rows {
            // BS5.4: hand the committed rows to the caller for the group's
            // deferred (parallel, per-branch) apply. Post-WAL failure handling
            // moves with the apply: a deferred apply failure records the same
            // widened durable-not-applied fact at the group level.
            *deferred = Some(combined_rows);
        } else {
            let apply_timer = perf_trace::start_timer();
            let apply_result = self.branch.append_committed_rows_atomically(combined_rows);
            perf_trace::record_commit_exec_apply_elapsed(apply_timer);
            if let Err(source) = apply_result {
                let reason = "branch state rejected durable commit rows after WAL append";
                let fact = CommitUnresolvedDurable::durable_not_applied_with_facts(
                    stamp, durability, reason,
                )?;
                record_unresolved_for_participation(&mut participation, self.durable_gate, fact)?;
                return Err(CommitRuntimeError::durable_but_not_visible_with(
                    branch_id,
                    stamp.commit_version(),
                    reason,
                    source,
                ));
            }
            // #3112 S2b: the row-driven apply funnel cannot see the
            // commit-scoped `committed_at`, so upgrade the entry it just
            // created — only after a successful apply, matching the funnel's
            // rule that a rolled-back batch leaves no timeline trace.
            if let Some(instant) = stamp.committed_at() {
                self.branch
                    .observe_commit_instant(stamp.commit_version(), instant);
            }
        }

        let facts = visible_durable_facts(stamp)?;
        self.publish_or_defer_to_group(&mut participation, stamp, durability, facts)?;

        CommitOutcome::visible(branch_id, stamp, durability, mutation_counts, facts)
    }

    /// The role-conditioned WAL append: solo appends with the policy's inline
    /// fsync; a member defers durability to the group's covering sync (and
    /// satisfies `Always` through it, so the per-append policy check is
    /// solo-only).
    fn append_record_for_participation(
        &mut self,
        participation: &CommitGateParticipation<'_, '_>,
        record: &WalRecord,
        required_policy: DurabilityPolicy,
        branch_id: BranchId,
        commit_version: CommitVersion,
    ) -> CommitRuntimeResult<()> {
        let wal_append_timer = perf_trace::start_timer();
        let append_result = match participation {
            CommitGateParticipation::Solo(_) => self.wal.append_commit_record(record),
            CommitGateParticipation::Member(_) => {
                self.wal.append_commit_record_deferring_durability(record)
            }
        };
        let append = match append_result {
            Ok(append) => {
                perf_trace::record_commit_wal_append_elapsed(
                    wal_append_timer,
                    append.bytes_written,
                );
                append
            }
            Err(error) => {
                perf_trace::record_commit_wal_append_elapsed(wal_append_timer, 0);
                return Err(error.into_commit_error(branch_id, commit_version));
            }
        };
        if matches!(participation, CommitGateParticipation::Solo(_)) {
            require_append_satisfies_policy(required_policy, append, branch_id, commit_version)?;
        }
        Ok(())
    }

    /// Solo: publish visibility now (recording an `applied_not_visible` fact
    /// on failure). Member: publish is the leader's — one visible advance to
    /// the group max after the covering fsync — so only advance the group
    /// frontier past this stamp.
    fn publish_or_defer_to_group(
        &mut self,
        participation: &mut CommitGateParticipation<'_, '_>,
        stamp: CommitStamp,
        durability: CommitDurabilityClass,
        facts: CommitVisibilityFacts,
    ) -> CommitRuntimeResult<()> {
        match participation {
            CommitGateParticipation::Solo(admission) => {
                perf_trace::record_commit_visible_publish_attempt();
                let publish_timer = perf_trace::start_timer();
                let publish = self.visible.publish_from_facts(facts);
                perf_trace::record_commit_exec_publish_elapsed(publish_timer);
                if let Err(error) = publish {
                    perf_trace::record_commit_visible_publish_failure();
                    let reason = durable_visible_reason(&error);
                    admission.record_unresolved(CommitUnresolvedDurable::applied_not_visible(
                        stamp, durability, reason,
                    )?)?;
                    return Err(CommitRuntimeError::durable_but_not_visible_with(
                        stamp.branch_id(),
                        stamp.commit_version(),
                        reason,
                        error,
                    ));
                }
                perf_trace::record_commit_visible_publish_success();
            }
            CommitGateParticipation::Member(group) => {
                group.record_member(stamp, durability);
            }
        }
        Ok(())
    }
}

/// Internal per-execution view of gate participation: `Solo` holds its own
/// admission token; `Member` records through the leader's already-active gate
/// span, widening facts to the group's version range.
enum CommitGateParticipation<'a, 'g> {
    Solo(CommitUnresolvedDurableAdmission<'a>),
    Member(&'g mut CommitGroupState),
}

fn record_unresolved_for_participation(
    participation: &mut CommitGateParticipation<'_, '_>,
    gate: &CommitUnresolvedDurableGate,
    fact: CommitUnresolvedDurable,
) -> CommitRuntimeResult<()> {
    match participation {
        CommitGateParticipation::Solo(admission) => admission.record_unresolved(fact),
        CommitGateParticipation::Member(group) => gate.record_unresolved(group.widen_fact(fact)?),
    }
}

/// Group publish (BS5.2): one visible advance to the group's max version,
/// after the group's covering durability is settled (the pipelined leader's
/// off-lock fsync, or `Standard`'s by-class durability). Returns `Ok(None)`
/// for an empty group (no member reached the durable protocol). A publish
/// failure records a gate fact widened to the group's version range so
/// recovery reconciles the whole group; the caller treats it as group-fatal
/// and distributes the error to every member.
pub(crate) fn publish_commit_group<V>(
    group: &CommitGroupState,
    visible: &mut V,
    durable_gate: &CommitUnresolvedDurableGate,
) -> CommitRuntimeResult<Option<VisibleVersionPublish>>
where
    V: CommitVisiblePublisher,
{
    let (Some(stamp), Some(durability)) = (group.last_stamp, group.last_durability) else {
        return Ok(None);
    };
    // Pipelined groups settle out of order: a LATER group may already have
    // published above this one — its publish covers ours (version contiguity
    // plus its covering fsync imply everything below it is applied and
    // durable). A non-advancing publish is a no-op here, never a regression.
    if stamp.commit_version() <= visible.visible_version() {
        return Ok(Some(VisibleVersionPublish::Unchanged {
            current: visible.visible_version(),
        }));
    }
    let facts = visible_durable_facts(stamp)?;
    perf_trace::record_commit_visible_publish_attempt();
    let publish_timer = perf_trace::start_timer();
    let publish = visible.publish_from_facts(facts);
    perf_trace::record_commit_exec_publish_elapsed(publish_timer);
    match publish {
        Ok(publish) => {
            perf_trace::record_commit_visible_publish_success();
            Ok(Some(publish))
        }
        Err(error) => {
            perf_trace::record_commit_visible_publish_failure();
            let reason = durable_visible_reason(&error);
            durable_gate.record_unresolved(group.widen_fact(
                CommitUnresolvedDurable::applied_not_visible(stamp, durability, reason)?,
            )?)?;
            Err(CommitRuntimeError::durable_but_not_visible_with(
                stamp.branch_id(),
                stamp.commit_version(),
                reason,
                error,
            ))
        }
    }
}

fn record_admission_pressure_facts(
    batch: &ValidatedCommitBatch,
    config: &CommitRuntimeConfig,
) -> CommitRuntimeResult<super::CommitAdmissionPressureFacts> {
    let facts = batch.admission_pressure_facts(config)?;
    perf_trace::record_commit_admission_pressure_facts(
        facts.mutations(),
        facts.approximate_commit_bytes(),
        facts.under_pressure(),
        facts.would_require_maintenance(),
    );
    Ok(facts)
}

impl CommitWalAppendFacts {
    pub(crate) const fn new(
        segment_id: u64,
        start_offset: u64,
        bytes_written: u64,
        dirty_bytes: u64,
        forced_durable: bool,
    ) -> Self {
        Self {
            segment_id,
            start_offset,
            bytes_written,
            dirty_bytes,
            forced_durable,
        }
    }

    pub(crate) fn from_wal_append(append: &WalAppend) -> Self {
        Self::new(
            append.segment_id(),
            append.start_offset(),
            append.bytes_written(),
            append.dirty_bytes(),
            append.forced_durable(),
        )
    }

    pub(crate) const fn forced_durable(self) -> bool {
        self.forced_durable
    }
}

impl CommitWalAppendError {
    pub(crate) const fn clean(error: CommitRuntimeError) -> Self {
        Self {
            durability_uncertain: false,
            error,
        }
    }

    pub(crate) const fn uncertain(error: CommitRuntimeError) -> Self {
        Self {
            durability_uncertain: true,
            error,
        }
    }

    fn into_commit_error(
        self,
        branch_id: strata_core::BranchId,
        version: strata_core::CommitVersion,
    ) -> CommitRuntimeError {
        if self.durability_uncertain {
            CommitRuntimeError::durability_uncertain_with(
                branch_id,
                version,
                "WAL append durability is uncertain",
                self.error,
            )
        } else {
            self.error
        }
    }
}

impl CommitWalAppender for WalService<'_> {
    fn durability_policy(&self) -> DurabilityPolicy {
        self.durability_policy()
    }

    fn record_frame_limit(&self) -> u64 {
        WalService::record_frame_limit(self)
    }

    fn append_commit_record(
        &mut self,
        record: &WalRecord,
    ) -> Result<CommitWalAppendFacts, CommitWalAppendError> {
        self.append(record)
            .map(|append| CommitWalAppendFacts::from_wal_append(&append))
            .map_err(map_wal_service_append_error)
    }

    fn append_commit_record_deferring_durability(
        &mut self,
        record: &WalRecord,
    ) -> Result<CommitWalAppendFacts, CommitWalAppendError> {
        self.append_deferring_durability(record)
            .map(|append| CommitWalAppendFacts::from_wal_append(&append))
            .map_err(map_wal_service_append_error)
    }
}

fn require_durable_mutating_batch(
    batch: &ValidatedCommitBatch,
) -> CommitRuntimeResult<(DurabilityPolicy, CommitDurabilityClass)> {
    if batch.batch().kind() != CommitBatchKind::Mutating {
        return Err(CommitRuntimeError::InvalidBatch {
            reason: "durable commit executor requires mutating batch",
        });
    }
    match batch.batch().options().durability() {
        CommitDurabilityMode::Standard => {
            Ok((DurabilityPolicy::Standard, CommitDurabilityClass::Standard))
        }
        CommitDurabilityMode::Always => {
            Ok((DurabilityPolicy::Always, CommitDurabilityClass::Always))
        }
        CommitDurabilityMode::Cache => Err(CommitRuntimeError::DurabilityUnavailable {
            reason: "durable commit executor requires durable mode",
        }),
    }
}

fn require_mutating_allocation(
    allocation: CommitFactAllocation,
) -> CommitRuntimeResult<CommitStamp> {
    match allocation {
        CommitFactAllocation::Mutating { stamp, .. } => Ok(stamp),
        CommitFactAllocation::ReadOnly { .. } => Err(CommitRuntimeError::InvalidCommitState {
            reason: "durable commit executor expected mutating allocation",
        }),
    }
}

fn require_allocated_after_visible(
    stamp: CommitStamp,
    current_visible_version: strata_core::CommitVersion,
) -> CommitRuntimeResult<()> {
    if stamp.commit_version() <= current_visible_version {
        return Err(CommitRuntimeError::InvalidCommitState {
            reason: "allocated commit version must be greater than current visible version",
        });
    }
    Ok(())
}

fn require_branch_not_ahead_of_visible(
    branch_max_commit_version: Option<strata_core::CommitVersion>,
    current_visible_version: strata_core::CommitVersion,
) -> CommitRuntimeResult<()> {
    // The V1 visible-version tracker is global, but this executor owns only the
    // target branch state. Cross-branch applied-not-visible durable rows are
    // excluded by the global unresolved durable gate and recovery ownership;
    // this local check fails closed for the branch this commit can expose.
    if branch_max_commit_version.is_some_and(|version| version > current_visible_version) {
        return Err(CommitRuntimeError::InvalidCommitState {
            reason: "branch has applied rows above current visible version",
        });
    }
    Ok(())
}

fn build_wal_record(
    stamp: CommitStamp,
    rows: Vec<crate::row::StorageRow>,
) -> CommitRuntimeResult<WalRecord> {
    let payload = WalCommitPayload::new(rows).map_err(|source| {
        CommitRuntimeError::lower_layer_with(
            CommitLowerLayer::WalFormat,
            "WAL commit payload rejected durable rows",
            source,
        )
    })?;
    WalRecord::new(
        stamp.commit_version(),
        stamp.branch_id(),
        stamp.commit_timestamp(),
        payload,
    )
    // The commit's wall-clock instant rides the record so a reopen recovers it
    // instead of reporting the commit as undated (#3112 S2).
    .map(|record| record.with_committed_at(stamp.committed_at()))
    .map_err(|source| {
        CommitRuntimeError::lower_layer_with(
            CommitLowerLayer::WalFormat,
            "WAL record rejected durable row facts",
            source,
        )
    })
}

fn require_append_satisfies_policy(
    policy: DurabilityPolicy,
    append: CommitWalAppendFacts,
    branch_id: strata_core::BranchId,
    version: strata_core::CommitVersion,
) -> CommitRuntimeResult<()> {
    if policy == DurabilityPolicy::Always && !append.forced_durable() {
        return Err(CommitRuntimeError::DurabilityUncertain {
            branch_id,
            commit_version: version,
            reason: "always durability requires a forced WAL append",
            source: None,
        });
    }
    Ok(())
}

fn visible_durable_facts(stamp: CommitStamp) -> CommitRuntimeResult<CommitVisibilityFacts> {
    CommitVisibilityFacts::new(
        Some(stamp.commit_version()),
        Some(stamp.commit_version()),
        Some(stamp.commit_version()),
        Some(stamp.commit_version()),
        Some(stamp.commit_version()),
    )
}

fn durable_visible_reason(error: &CommitRuntimeError) -> &'static str {
    match error {
        CommitRuntimeError::InvalidVisibilityFacts { reason }
        | CommitRuntimeError::InvalidCommitState { reason } => reason,
        _ => "visible publication failed after durable branch apply",
    }
}

fn map_wal_service_append_error(error: WalServiceError) -> CommitWalAppendError {
    if error.is_writer_halted_append_failure() {
        CommitWalAppendError::clean(CommitRuntimeError::DurabilityUnavailable {
            reason: "WAL writer is halted; reopen before appending",
        })
    } else if is_uncertain_wal_append_error(&error) {
        CommitWalAppendError::uncertain(CommitRuntimeError::lower_layer_with(
            CommitLowerLayer::WalService,
            "WAL append durability is uncertain",
            error,
        ))
    } else {
        CommitWalAppendError::clean(CommitRuntimeError::lower_layer_with(
            CommitLowerLayer::WalService,
            "WAL append failed before commit visibility",
            error,
        ))
    }
}

fn is_uncertain_wal_append_error(error: &WalServiceError) -> bool {
    error.is_durability_uncertain_append_failure()
}
