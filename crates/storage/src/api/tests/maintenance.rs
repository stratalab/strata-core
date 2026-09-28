use super::*;
// The ledger's typed facts are asserted only by the durable (localfs) tests;
// the cache-runtime test needs no reclaim type.
#[cfg(feature = "localfs")]
use crate::lifecycle::{MaintenanceDeferralReason, ReclaimFamily, ReclaimOutcome};

fn open_runtime() -> StorageRuntime<'static> {
    StorageRuntime::open_ephemeral()
        .expect("open ephemeral runtime")
        .into_runtime()
}

fn open_manual_runtime() -> StorageRuntime<'static> {
    StorageRuntime::open(
        StorageOpenOptions::cache().with_maintenance_scheduling_policy(
            StorageMaintenanceSchedulingPolicy::EvaluateAndEnqueue,
        ),
    )
    .expect("open manual runtime")
    .into_runtime()
}

#[cfg(feature = "localfs")]
fn open_durable_runtime_with_options(
    name: &str,
    options: StorageOpenOptions,
) -> StorageRuntime<'static> {
    let backend = StorageBackend::local_fs(temp_dir_for_api_test(name));
    StorageRuntime::open_with_backend(options, crate::testkit::leak_static(backend))
        .expect("open durable runtime")
        .into_runtime()
}

#[cfg(feature = "localfs")]
fn open_durable_runtime_with_backend(
    name: &str,
    options: StorageOpenOptions,
) -> (&'static StorageBackend, StorageRuntime<'static>) {
    let backend =
        crate::testkit::leak_static(StorageBackend::local_fs(temp_dir_for_api_test(name)));
    let runtime = StorageRuntime::open_with_backend(options, backend)
        .expect("open durable runtime")
        .into_runtime();
    (backend, runtime)
}

fn branch() -> BranchId {
    StorageRuntime::default_branch_id_for_test()
}

fn branch_with(byte: u8) -> BranchId {
    branch_id(byte)
}

fn engine_space() -> StorageSpaceId {
    StorageSpaceId::new(vec![0x20]).expect("engine storage space")
}

fn api_key(bytes: &[u8]) -> StorageKey {
    StorageKey::new(bytes.to_vec()).expect("valid key")
}

fn put_batch(key: &[u8], value: &[u8]) -> CommitBatch {
    put_batch_for(branch(), key, value)
}

fn put_batch_for(branch_id: BranchId, key: &[u8], value: &[u8]) -> CommitBatch {
    CommitBatch::new(
        branch_id,
        vec![CommitMutation::Put {
            storage_space: engine_space(),
            key: api_key(key),
            value: StorageValue::new(value.to_vec()),
            ttl: None,
        }],
        CommitOptions::default(),
    )
    .expect("valid put batch")
}

fn terminal_owned_level() -> u8 {
    let max_level_count = crate::branch::config::BranchRuntimeConfig::default().max_level_count();
    u8::try_from(max_level_count.saturating_sub(1)).expect("configured level fits in u8")
}

fn owned_table_count_at(layout: &crate::branch::facts::BranchSourceLayout, level: u8) -> usize {
    if level == 0 {
        return layout.owned_l0_tables();
    }
    layout
        .owned_nonzero_level_table_counts()
        .iter()
        .find(|count| count.level().raw() == level)
        .map_or(0, |count| count.table_count())
}

fn fork_branch(runtime: &mut StorageRuntime<'_>, child: BranchId) {
    runtime
        .branch(&BranchRequest::new(
            child,
            BranchAction::ForkCurrent { source: branch() },
            Some(BranchGeneration::new(1)),
        ))
        .expect("fork branch");
}

#[cfg(feature = "localfs")]
pub(super) fn stage_quarantine_object(
    backend: &StorageBackend,
    branch_id: BranchId,
    table_id: &str,
    bytes: &[u8],
) {
    let source = crate::layout::ObjectLayout::table_object(&branch_id.to_string(), 0, table_id)
        .expect("source table object");
    backend
        .as_backend()
        .write_object(&source, bytes)
        .expect("write source table");
    let request = crate::lifecycle::LifecycleQuarantineRequest::from_source_object(
        branch_id,
        [0x53; 16],
        crate::lifecycle::LifecycleCodecId::identity(),
        source,
        Timestamp::from_micros(42),
        crate::lifecycle::LifecycleQuarantineProof::safe(crate::lifecycle::RecoveryHealth::Healthy),
    )
    .expect("quarantine request");
    let outcome = crate::lifecycle::quarantine_object(
        &crate::service::QuarantineService::new(backend.as_backend()),
        &request,
    );
    assert_eq!(
        outcome.status(),
        crate::lifecycle::LifecycleQuarantineStatus::QuarantinedSourceDeleted
    );
    assert!(outcome.inventory_object().is_some());
    assert!(outcome.quarantine_object().is_some());
}

#[test]
fn maintenance_request_snapshot_pruning_is_constructible() {
    let request =
        MaintenanceRequest::new(MaintenanceTask::SnapshotPruning, MaintenanceScope::Global);

    assert_eq!(request.task(), MaintenanceTask::SnapshotPruning);
    assert_eq!(request.scope(), MaintenanceScope::Global);
}

#[test]
fn api_maintenance_status_reports_empty_queue() {
    let runtime = open_runtime();

    let status = runtime.maintenance_status().expect("maintenance status");

    assert_eq!(status.pending_tasks(), 0);
    assert_eq!(status.active_task(), None);
    assert_eq!(status.enqueued(), 0);
}

#[test]
fn api_checkpoint_cache_mode_returns_deferred() {
    let mut runtime = open_runtime();
    let request = MaintenanceRequest::new(MaintenanceTask::Checkpoint, MaintenanceScope::Global);

    let outcome = runtime.maintenance(&request).expect("checkpoint outcome");

    assert_eq!(outcome.task(), MaintenanceTask::Checkpoint);
    assert_eq!(outcome.status(), MaintenanceSummaryStatus::Deferred);
    assert_eq!(
        outcome.reason_class(),
        Some(MaintenanceReasonClass::Deferred)
    );
    assert_eq!(outcome.affected_objects(), 0);
}

#[test]
#[cfg(feature = "localfs")]
fn api_checkpoint_returns_watermark_facts() {
    let mut runtime = open_durable_runtime_with_options(
        "maintenance-checkpoint-facts",
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
    );
    let commit = runtime
        .commit(&put_batch(b"checkpoint", b"value"))
        .expect("commit before checkpoint");
    let request = MaintenanceRequest::new(MaintenanceTask::Checkpoint, MaintenanceScope::Global);

    let outcome = runtime.maintenance(&request).expect("checkpoint outcome");

    assert_eq!(outcome.task(), MaintenanceTask::Checkpoint);
    assert_eq!(outcome.status(), MaintenanceSummaryStatus::Completed);
    assert_eq!(
        outcome.checkpoint_watermark(),
        Some(commit.commit_version())
    );
    assert!(outcome.snapshot_id().is_some());
    assert!(outcome.rows_processed() > 0);
    assert!(!outcome.wal_truncated());
    assert_eq!(outcome.source_error_code(), None);
}

#[test]
#[cfg(feature = "localfs")]
fn checkpoint_defers_while_a_fork_holds_unmaterialized_inherited_layers() {
    let mut runtime = open_durable_runtime_with_options(
        "checkpoint-fork-inherited-defers",
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
    );
    runtime
        .commit(&put_batch(b"base-row", b"value"))
        .expect("commit base");
    // Seal an owned table on the seeded branch so the fork is COW-structural:
    // the child references that table through an inherited layer and owns no
    // tables of its own.
    let flush = runtime
        .maintenance(&MaintenanceRequest::new(
            MaintenanceTask::Flush,
            MaintenanceScope::Branch(branch()),
        ))
        .expect("flush outcome");
    assert_eq!(flush.status(), MaintenanceSummaryStatus::Completed);
    fork_branch(&mut runtime, branch_with(0xC5));

    // The fork's inherited layers are not materialized yet, so a checkpoint
    // cannot include it. That is a structural deferral — the same posture as
    // the durable-base guard one state later in the fork's lifecycle — never
    // a hard task failure (#2798 recorded failed_precondition.lifecycle.
    // branch_runtime here and tripped the stress lane's failure gate).
    let outcome = runtime
        .maintenance(&MaintenanceRequest::new(
            MaintenanceTask::Checkpoint,
            MaintenanceScope::Global,
        ))
        .expect("checkpoint outcome");
    assert_eq!(
        outcome.status(),
        MaintenanceSummaryStatus::Deferred,
        "{outcome:?}"
    );
    assert_eq!(
        outcome.reason_class(),
        Some(MaintenanceReasonClass::Deferred),
        "{outcome:?}"
    );
}

#[test]
fn api_checkpoint_after_close_rejects() {
    let mut runtime = open_runtime();
    runtime.close().expect("close runtime");
    let request = MaintenanceRequest::new(MaintenanceTask::Checkpoint, MaintenanceScope::Global);

    let error = runtime
        .maintenance(&request)
        .expect_err("closed runtime rejects checkpoint");

    assert_eq!(error.class(), StorageApiErrorClass::FailedPrecondition);
    assert_eq!(error.code(), "failed_precondition.storage_api.state");
}

#[test]
fn api_flush_returns_publication_facts() {
    let mut runtime = open_runtime();
    runtime
        .commit(&put_batch(b"key", b"value"))
        .expect("commit");
    let request =
        MaintenanceRequest::new(MaintenanceTask::Flush, MaintenanceScope::Branch(branch()));

    let outcome = runtime.maintenance(&request).expect("flush outcome");

    assert_eq!(outcome.task(), MaintenanceTask::Flush);
    assert_eq!(outcome.status(), MaintenanceSummaryStatus::Completed);
    assert!(outcome.rows_processed() > 0);
    assert!(!outcome.wal_truncated());
}

#[test]
fn api_flush_does_not_claim_wal_truncation_without_proof() {
    let mut runtime = open_runtime();
    runtime
        .commit(&put_batch(b"no-proof", b"value"))
        .expect("commit");
    let request =
        MaintenanceRequest::new(MaintenanceTask::Flush, MaintenanceScope::Branch(branch()));

    let outcome = runtime.maintenance(&request).expect("flush outcome");

    assert_eq!(outcome.status(), MaintenanceSummaryStatus::Completed);
    assert!(!outcome.wal_truncated());
    assert!(outcome.checkpoint_watermark().is_none());
}

#[test]
fn api_flush_global_scope_rejects() {
    let mut runtime = open_runtime();
    let request = MaintenanceRequest::new(MaintenanceTask::Flush, MaintenanceScope::Global);

    let error = runtime
        .maintenance(&request)
        .expect_err("global flush scope rejects");

    assert_eq!(error.class(), StorageApiErrorClass::InvalidArgument);
    assert_eq!(error.code(), "invalid_argument.storage_api.argument");
}

#[test]
fn api_flush_failure_preserves_orphan_facts() {
    let object = "tables/00000000-0000-0000-0000-000000000001/l0/orphan".to_string();
    let lower = crate::lifecycle::MaintenanceOutcome::new(
        crate::lifecycle::MaintenanceTaskKind::Flush,
        crate::lifecycle::MaintenanceOutcomeStatus::Failed,
    )
    .with_affected_object_names(vec![object.clone()])
    .with_effects(1, 0, true)
    .with_source_error(
        crate::lifecycle::LifecycleError::flush_publication_orphaned_with(
            Some(object.clone()),
            "flush published an object that was not installed",
            SourceError,
        ),
    );
    let request =
        MaintenanceRequest::new(MaintenanceTask::Flush, MaintenanceScope::Branch(branch()));

    let outcome = map_maintenance_outcome_for_test(request, &lower);

    assert_eq!(outcome.status(), MaintenanceSummaryStatus::Failed);
    assert_eq!(outcome.affected_objects(), 1);
    assert_eq!(outcome.affected_object_names(), &[object]);
    assert_eq!(
        outcome.source_error_code(),
        Some("ambiguous_commit.storage_api.durable_uncertain")
    );
    assert_eq!(
        outcome.source_error_class(),
        Some(StorageApiErrorClass::AmbiguousCommit)
    );
    assert!(outcome.retryable());
}

#[test]
fn api_compaction_returns_rewrite_facts() {
    let mut runtime = open_runtime();
    let request =
        MaintenanceRequest::new(MaintenanceTask::Compact, MaintenanceScope::Branch(branch()));

    let outcome = runtime.maintenance(&request).expect("compaction outcome");

    assert_eq!(outcome.task(), MaintenanceTask::Compact);
    assert_eq!(outcome.scope(), MaintenanceScope::Branch(branch()));
    assert_eq!(outcome.status(), MaintenanceSummaryStatus::Deferred);
    assert_eq!(
        outcome.reason_class(),
        Some(MaintenanceReasonClass::Deferred)
    );
}

#[test]
fn api_compaction_reports_checkpoint_debt() {
    let lower = crate::lifecycle::MaintenanceOutcome::new(
        crate::lifecycle::MaintenanceTaskKind::Compaction,
        crate::lifecycle::MaintenanceOutcomeStatus::Completed,
    )
    .with_checkpoint_required(true)
    .with_affected_object_names(vec!["tables/default/l1/rewrite".to_string()])
    .with_effects(1, 4096, false);
    let request =
        MaintenanceRequest::new(MaintenanceTask::Compact, MaintenanceScope::Branch(branch()));

    let outcome = map_maintenance_outcome_for_test(request, &lower);

    assert_eq!(outcome.status(), MaintenanceSummaryStatus::Completed);
    assert!(outcome.checkpoint_required());
    assert_eq!(outcome.affected_objects(), 1);
    assert_eq!(outcome.bytes_reclaimed(), 4096);
}

#[test]
fn api_materialization_returns_rewrite_facts() {
    let lower = crate::lifecycle::MaintenanceOutcome::new(
        crate::lifecycle::MaintenanceTaskKind::Materialization,
        crate::lifecycle::MaintenanceOutcomeStatus::Completed,
    )
    .with_affected_object_names(vec!["tables/default/l0/materialized".to_string()])
    .with_effects(1, 2048, false)
    .with_state_changes(1)
    .with_checkpoint_required(true);
    let request = MaintenanceRequest::new(
        MaintenanceTask::Materialize,
        MaintenanceScope::Branch(branch()),
    );

    let outcome = map_maintenance_outcome_for_test(request, &lower);

    assert_eq!(outcome.task(), MaintenanceTask::Materialize);
    assert_eq!(outcome.scope(), MaintenanceScope::Branch(branch()));
    assert_eq!(outcome.status(), MaintenanceSummaryStatus::Completed);
    assert_eq!(outcome.affected_objects(), 1);
    assert_eq!(outcome.bytes_reclaimed(), 2048);
    assert!(outcome.state_changes() > 0);
    assert!(outcome.checkpoint_required());
}

#[test]
fn api_materialization_uses_stable_handle() {
    let mut runtime = open_runtime();
    runtime
        .commit(&put_batch(b"stable", b"value"))
        .expect("commit parent");
    let child = branch_with(0x73);
    fork_branch(&mut runtime, child);
    let request = MaintenanceRequest::new(
        MaintenanceTask::Materialize,
        MaintenanceScope::Branch(child),
    );

    runtime
        .enqueue_maintenance(&request)
        .expect("enqueue materialization");
    let drain = runtime.drain_maintenance().expect("drain materialization");

    assert_eq!(drain.drained_tasks(), 1);
    assert_eq!(drain.outcomes()[0].task(), MaintenanceTask::Materialize);
    assert_eq!(drain.outcomes()[0].scope(), MaintenanceScope::Branch(child));
    assert_ne!(
        drain.outcomes()[0].status(),
        MaintenanceSummaryStatus::Failed
    );
}

#[test]
fn api_materialization_direct_call_runs_requested_task_not_prior_queue_entry() {
    let mut runtime = open_manual_runtime();
    runtime
        .commit(&put_batch(b"direct-stable", b"value"))
        .expect("commit parent");
    let first_child = branch_with(0x75);
    let second_child = branch_with(0x76);
    fork_branch(&mut runtime, first_child);
    fork_branch(&mut runtime, second_child);
    let queued = MaintenanceRequest::new(
        MaintenanceTask::Materialize,
        MaintenanceScope::Branch(first_child),
    );
    let direct = MaintenanceRequest::new(
        MaintenanceTask::Materialize,
        MaintenanceScope::Branch(second_child),
    );

    runtime
        .enqueue_maintenance(&queued)
        .expect("enqueue first materialization");
    let direct_outcome = runtime
        .maintenance(&direct)
        .expect("run direct materialization");
    let drain = runtime.drain_maintenance().expect("drain remaining task");

    assert_eq!(
        direct_outcome.scope(),
        MaintenanceScope::Branch(second_child)
    );
    assert_eq!(drain.drained_tasks(), 1);
    assert_eq!(
        drain.outcomes()[0].scope(),
        MaintenanceScope::Branch(first_child)
    );
}

#[test]
fn api_rewrite_cache_mode_does_not_call_durable_services() {
    let mut runtime = open_runtime();
    let request =
        MaintenanceRequest::new(MaintenanceTask::Compact, MaintenanceScope::Branch(branch()));

    let outcome = runtime.maintenance(&request).expect("cache compaction");

    // Cache mode has no durable service handles; Deferred is the public
    // boundary signal that the compact request did not enter a durable path.
    assert_eq!(outcome.status(), MaintenanceSummaryStatus::Deferred);
    assert_eq!(runtime.state(), StorageRuntimeState::Open);
}

/// #3502 Slice D: with an opt-in keep-newer-than retention window, a durable
/// compaction builds the pruning proof and drops versions older than
/// `visible - window` — observable through Slice A: an `as_of` read of the
/// oldest (now pruned) version RAISES, while the latest value still reads.
#[cfg(feature = "localfs")]
#[test]
fn api_opt_in_version_retention_prunes_old_versions() {
    let options = StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
        .with_version_retention_window(Some(1));
    let mut runtime = open_durable_runtime_with_options("prune-optin", options);

    // Two batches of three versions, each flushed into its own L0 table — two
    // tables, below the flush-followup pressure threshold, so no compaction
    // races. Then FORCE one compaction (deterministically, at visible =
    // versions[5]) which, under the opt-in keep-newer-than-1 window, prunes
    // versions below `visible - 1`, keeping one below-floor survivor. The
    // oldest version is dropped.
    let flush = MaintenanceRequest::new(MaintenanceTask::Flush, MaintenanceScope::Branch(branch()));
    let mut versions = Vec::new();
    for index in 0..3u8 {
        let summary = runtime
            .commit(&put_batch(b"k", &[b'v', index]))
            .expect("commit version");
        versions.push(summary.commit_version());
    }
    runtime.maintenance(&flush).expect("flush first table");
    for index in 3..6u8 {
        let summary = runtime
            .commit(&put_batch(b"k", &[b'v', index]))
            .expect("commit version");
        versions.push(summary.commit_version());
    }
    runtime.maintenance(&flush).expect("flush second table");
    runtime
        .force_branch_compaction_for_test(branch())
        .expect("force pruning compaction");

    let error = runtime
        .read_point(&PointReadRequest::new(
            branch(),
            engine_space(),
            api_key(b"k"),
            ReadBound::AtVersion(versions[0]),
        ))
        .expect_err("oldest version was pruned");
    assert_eq!(error.class(), StorageApiErrorClass::HistoryUnavailable);

    let latest = runtime
        .read_point(&PointReadRequest::new(
            branch(),
            engine_space(),
            api_key(b"k"),
            ReadBound::Latest,
        ))
        .expect("latest read");
    assert_eq!(
        latest
            .row()
            .expect("row")
            .value()
            .expect("value")
            .as_bytes(),
        &[b'v', 5]
    );
}

/// #3502 Slice D control: the default `KeepAll` policy never prunes — the
/// oldest version still reads exactly after a compaction.
#[cfg(feature = "localfs")]
#[test]
fn api_default_keep_all_retention_keeps_old_versions() {
    let options = StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard);
    let mut runtime = open_durable_runtime_with_options("prune-keepall", options);

    let mut versions = Vec::new();
    for index in 0..6u8 {
        let summary = runtime
            .commit(&put_batch(b"k", &[b'v', index]))
            .expect("commit version");
        versions.push(summary.commit_version());
        runtime
            .maintenance(&MaintenanceRequest::new(
                MaintenanceTask::Flush,
                MaintenanceScope::Branch(branch()),
            ))
            .expect("flush");
    }
    runtime
        .maintenance(&MaintenanceRequest::new(
            MaintenanceTask::Compact,
            MaintenanceScope::Branch(branch()),
        ))
        .expect("compact keeps all");

    let outcome = runtime
        .read_point(&PointReadRequest::new(
            branch(),
            engine_space(),
            api_key(b"k"),
            ReadBound::AtVersion(versions[0]),
        ))
        .expect("oldest version retained under KeepAll");
    assert_eq!(
        outcome
            .row()
            .expect("row")
            .value()
            .expect("value")
            .as_bytes(),
        &[b'v', 0]
    );
}

/// #3524 (was #3520): opting into `KeepRecentVersions` on a REOPENED `KeepAll`
/// database must PRUNE. A reopened branch used to recover `Unknown` timestamp
/// coverage (only a born-in-process branch got `mark_complete_from_birth`; the
/// manifest persists the version floor but not coverage), so the pruning proof
/// could never be attested and retention silently no-op'd for the whole session
/// (#3520 turned the hard failure into a graceful skip; this makes it actually
/// prune). Recovery now re-establishes coverage from the durable state — a
/// never-pruned branch recovers `Complete` — so a post-reopen compaction prunes
/// below the floor and an `as_of` read of the oldest version RAISES.
#[cfg(feature = "localfs")]
#[test]
fn test_retention_optin_on_reopened_db_prunes_after_coverage_reestablished() {
    let flush = MaintenanceRequest::new(MaintenanceTask::Flush, MaintenanceScope::Branch(branch()));
    // A KeepAll durable database with some flushed history, cleanly closed.
    let (backend, mut runtime) = open_durable_runtime_with_backend(
        "retention-reopen-prunes",
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
    );
    let mut versions = Vec::new();
    for index in 0..3u8 {
        versions.push(
            runtime
                .commit(&put_batch(b"k", &[b'v', index]))
                .expect("commit")
                .commit_version(),
        );
    }
    runtime.maintenance(&flush).expect("flush before close");
    runtime.close().expect("clean close");
    drop(runtime);

    // Reopen the same store WITH an opt-in retention window, then write, flush
    // and force a compaction. Recovery re-established `Complete` coverage (the
    // branch was never pruned), so the compaction prunes below the floor.
    let mut runtime = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
            .with_version_retention_window(Some(1)),
        backend,
    )
    .expect("reopen with retention window")
    .into_runtime();
    // Confirm the oldest version is RETAINED right after reopen (nothing pruned
    // yet). This also seeds the retained-timeline index from data rows (#3519),
    // so the post-compaction read below raises only if pruning actually dropped
    // the version — not because the timeline index could not resolve it.
    let before = runtime
        .read_point(&PointReadRequest::new(
            branch(),
            engine_space(),
            api_key(b"k"),
            ReadBound::AtVersion(versions[0]),
        ))
        .expect("oldest version retained right after reopen");
    assert_eq!(
        before
            .row()
            .expect("row")
            .value()
            .expect("value")
            .as_bytes(),
        &[b'v', 0]
    );
    for index in 3..6u8 {
        versions.push(
            runtime
                .commit(&put_batch(b"k", &[b'v', index]))
                .expect("commit after reopen")
                .commit_version(),
        );
    }
    runtime.maintenance(&flush).expect("flush after reopen");
    runtime
        .force_branch_compaction_for_test(branch())
        .expect("compaction");
    // Coverage was re-established on reopen, so the compaction pruned and
    // published a retained-history floor (the canonical "pruning fired" signal).
    assert!(
        runtime
            .retained_history_floor_for_test(branch())
            .expect("floor query")
            .is_some(),
        "reopened retention must re-establish coverage and publish a pruning floor",
    );

    // The oldest version was pruned below the published floor — an `as_of` read
    // of it RAISES. (Before the coverage fix, pruning was skipped and it read.)
    let error = runtime
        .read_point(&PointReadRequest::new(
            branch(),
            engine_space(),
            api_key(b"k"),
            ReadBound::AtVersion(versions[0]),
        ))
        .expect_err("oldest version was pruned after reopen");
    assert_eq!(error.class(), StorageApiErrorClass::HistoryUnavailable);

    // The latest value still reads.
    let latest = runtime
        .read_point(&PointReadRequest::new(
            branch(),
            engine_space(),
            api_key(b"k"),
            ReadBound::Latest,
        ))
        .expect("latest read");
    assert_eq!(
        latest
            .row()
            .expect("row")
            .value()
            .expect("value")
            .as_bytes(),
        &[b'v', 5]
    );
}

/// #3502 Slice D2: the shared-table safety gate. A COW fork re-references one
/// of the branch's L0 tables, lifting its reference count to 2 while a later
/// table stays branch-private (count 1) — a MIXED snapshot. The whole-branch
/// gate requires EVERY referenced table to be unshared, so it refuses pruning
/// wholesale, and the oldest version survives the compaction: a fork's
/// inherited reads are never rewritten out from under it. The mixed snapshot
/// (not two shared tables) is deliberate — it distinguishes the `all`-tables
/// gate from an `any`-table one, which would wrongly prune on the private
/// table alone. This is the multi-branch case the single-branch count proxy
/// rejected wholesale; the derived check refuses only the genuinely shared
/// branch.
#[cfg(feature = "localfs")]
#[test]
fn api_shared_tables_block_version_pruning() {
    let options = StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
        .with_version_retention_window(Some(1));
    let mut runtime = open_durable_runtime_with_options("prune-shared", options);

    let flush = MaintenanceRequest::new(MaintenanceTask::Flush, MaintenanceScope::Branch(branch()));
    let mut versions = Vec::new();
    for index in 0..3u8 {
        versions.push(
            runtime
                .commit(&put_batch(b"k", &[b'v', index]))
                .expect("commit version")
                .commit_version(),
        );
    }
    runtime.maintenance(&flush).expect("flush first table");

    // Fork BEFORE the second flush: the child inherits only the first table, so
    // that table's reference count is 2 (shared) while the second stays 1
    // (branch-private) — a mixed snapshot.
    fork_branch(&mut runtime, branch_with(0xC5));

    for index in 3..6u8 {
        versions.push(
            runtime
                .commit(&put_batch(b"k", &[b'v', index]))
                .expect("commit version")
                .commit_version(),
        );
    }
    runtime.maintenance(&flush).expect("flush second table");

    runtime
        .force_branch_compaction_for_test(branch())
        .expect("force compaction (pruning refused while shared)");

    // The oldest version is retained: had the gate mis-judged the shared
    // tables as prunable, this read would raise `HistoryUnavailable`.
    let outcome = runtime
        .read_point(&PointReadRequest::new(
            branch(),
            engine_space(),
            api_key(b"k"),
            ReadBound::AtVersion(versions[0]),
        ))
        .expect("oldest version retained while tables are shared");
    assert_eq!(
        outcome
            .row()
            .expect("row")
            .value()
            .expect("value")
            .as_bytes(),
        &[b'v', 0]
    );
}

/// #3502 / #3509: forking below a published retained-history floor RAISES,
/// mirroring the read-path contract — a fork at an unavailable version must not
/// silently inherit pruned tables and read absent history for keys whose
/// sub-floor versions were dropped. A fork exactly AT the floor, and above it,
/// still succeeds (the floor version is retained, CMP-002). The fork path
/// otherwise validates only the never-pruned timeline bounds, so the timeline
/// check passes for a below-floor version and this data-floor gate is what
/// refuses it.
#[test]
fn api_fork_below_retained_history_floor_is_rejected() {
    let mut runtime = open_runtime();
    let mut versions = Vec::new();
    for index in 0..3u8 {
        versions.push(
            runtime
                .commit(&put_batch(b"k", &[b'v', index]))
                .expect("commit version")
                .commit_version(),
        );
    }
    // Publish a retained-history floor at the middle version (what pruning does).
    runtime
        .set_retained_history_floor_for_test(branch(), versions[1])
        .expect("set retained history floor");

    // Below the floor: the fork is refused (its child would read absent history).
    let error = runtime
        .branch(&BranchRequest::new(
            branch_with(0xC1),
            BranchAction::ForkAtVersion {
                source: branch(),
                version: versions[0],
            },
            Some(BranchGeneration::new(1)),
        ))
        .expect_err("fork below the retained floor is rejected");
    assert_eq!(error.class(), StorageApiErrorClass::HistoryUnavailable);

    // Exactly AT the floor still forks — the floor version is retained.
    runtime
        .branch(&BranchRequest::new(
            branch_with(0xC2),
            BranchAction::ForkAtVersion {
                source: branch(),
                version: versions[1],
            },
            Some(BranchGeneration::new(1)),
        ))
        .expect("fork at the floor succeeds");

    // Above the floor still forks.
    runtime
        .branch(&BranchRequest::new(
            branch_with(0xC3),
            BranchAction::ForkAtVersion {
                source: branch(),
                version: versions[2],
            },
            Some(BranchGeneration::new(1)),
        ))
        .expect("fork above the floor succeeds");
}

/// #3515: a fork child INHERITS the source's retained-history floor. Forking at
/// or above the floor is legal (D2), but the resulting child must still refuse
/// an at-version read below the inherited floor — otherwise a legal fork
/// launders history the source legally pruned. The child at/above the floor
/// still reads, and a grandchild fork below the inherited floor is refused.
#[cfg(feature = "localfs")]
#[test]
fn test_fork_child_inherits_retained_history_floor() {
    let mut runtime = open_durable_runtime_with_options(
        "fork-inherits-floor",
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
    );
    let mut versions = Vec::new();
    for index in 0..4u8 {
        versions.push(
            runtime
                .commit(&put_batch(b"k", &[b'v', index]))
                .expect("commit version")
                .commit_version(),
        );
    }
    // Seal the versions into owned tables so the COW historical fork can
    // reference them, then publish a floor at versions[2] (what pruning does).
    runtime
        .maintenance(&MaintenanceRequest::new(
            MaintenanceTask::Flush,
            MaintenanceScope::Branch(branch()),
        ))
        .expect("flush");
    runtime
        .set_retained_history_floor_for_test(branch(), versions[2])
        .expect("set retained history floor");

    // A LEGAL fork at the latest version (>= floor) succeeds.
    let child = branch_with(0xD1);
    runtime
        .branch(&BranchRequest::new(
            child,
            BranchAction::ForkAtVersion {
                source: branch(),
                version: versions[3],
            },
            Some(BranchGeneration::new(1)),
        ))
        .expect("legal fork at/above the floor");

    // The child inherited the floor: an at-version read below it RAISES rather
    // than serving history the source legally pruned.
    let error = runtime
        .read_point(&PointReadRequest::new(
            child,
            engine_space(),
            api_key(b"k"),
            ReadBound::AtVersion(versions[0]),
        ))
        .expect_err("child must refuse below-inherited-floor history");
    assert_eq!(error.class(), StorageApiErrorClass::HistoryUnavailable);

    // Direction control: at/above the inherited floor the child still reads.
    let ok = runtime
        .read_point(&PointReadRequest::new(
            child,
            engine_space(),
            api_key(b"k"),
            ReadBound::AtVersion(versions[3]),
        ))
        .expect("child reads at/above the inherited floor");
    assert_eq!(
        ok.row().expect("row").value().expect("value").as_bytes(),
        &[b'v', 3]
    );

    // And a grandchild fork below the inherited floor is refused (the child
    // carries a real floor now, so D2's guard fires for its descendants too).
    let grandchild_error = runtime
        .branch(&BranchRequest::new(
            branch_with(0xD2),
            BranchAction::ForkAtVersion {
                source: child,
                version: versions[0],
            },
            Some(BranchGeneration::new(1)),
        ))
        .expect_err("grandchild fork below the inherited floor is refused");
    assert_eq!(
        grandchild_error.class(),
        StorageApiErrorClass::HistoryUnavailable
    );
}

/// #3515: fork-CURRENT (not just fork-at-version) inherits the floor too. The
/// fix lives in both COW constructors; this covers `fork_into_empty_child`.
#[cfg(feature = "localfs")]
#[test]
fn test_fork_current_child_inherits_retained_history_floor() {
    let mut runtime = open_durable_runtime_with_options(
        "fork-current-inherits-floor",
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
    );
    let mut versions = Vec::new();
    for index in 0..4u8 {
        versions.push(
            runtime
                .commit(&put_batch(b"k", &[b'v', index]))
                .expect("commit version")
                .commit_version(),
        );
    }
    runtime
        .maintenance(&MaintenanceRequest::new(
            MaintenanceTask::Flush,
            MaintenanceScope::Branch(branch()),
        ))
        .expect("flush");
    runtime
        .set_retained_history_floor_for_test(branch(), versions[2])
        .expect("set retained history floor");

    // Fork the source at its current head (fork_into_empty_child).
    fork_branch(&mut runtime, branch_with(0xD5));

    let error = runtime
        .read_point(&PointReadRequest::new(
            branch_with(0xD5),
            engine_space(),
            api_key(b"k"),
            ReadBound::AtVersion(versions[0]),
        ))
        .expect_err("fork-current child must refuse below-inherited-floor history");
    assert_eq!(error.class(), StorageApiErrorClass::HistoryUnavailable);
}

/// #3515: the inherited floor PERSISTS across reopen — the child's floor is
/// written into its manifest at fork (slice C's `manifest_retained_version_floor`
/// derives from the branch's floor) and recovered on restart, so a below-floor
/// read on the child still raises after a reopen.
#[cfg(feature = "localfs")]
#[test]
fn test_fork_child_inherited_floor_persists_across_reopen() {
    let (backend, mut runtime) = open_durable_runtime_with_backend(
        "fork-floor-persist",
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
    );
    let mut versions = Vec::new();
    for index in 0..4u8 {
        versions.push(
            runtime
                .commit(&put_batch(b"k", &[b'v', index]))
                .expect("commit version")
                .commit_version(),
        );
    }
    runtime
        .maintenance(&MaintenanceRequest::new(
            MaintenanceTask::Flush,
            MaintenanceScope::Branch(branch()),
        ))
        .expect("flush");
    runtime
        .set_retained_history_floor_for_test(branch(), versions[2])
        .expect("set retained history floor");
    let child = branch_with(0xD3);
    runtime
        .branch(&BranchRequest::new(
            child,
            BranchAction::ForkAtVersion {
                source: branch(),
                version: versions[3],
            },
            Some(BranchGeneration::new(1)),
        ))
        .expect("legal fork at/above the floor");
    // Close and reopen against the same backend.
    drop(runtime);
    let runtime = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
        backend,
    )
    .expect("reopen")
    .into_runtime();

    // The child's inherited floor survived recovery.
    let error = runtime
        .read_point(&PointReadRequest::new(
            child,
            engine_space(),
            api_key(b"k"),
            ReadBound::AtVersion(versions[0]),
        ))
        .expect_err("child refuses below-inherited-floor history after reopen");
    assert_eq!(error.class(), StorageApiErrorClass::HistoryUnavailable);
}

/// #3516: explicit compaction (`maintenance(Compact)` → the fixed-point drain)
/// must apply the database's configured table codec, not silently rewrite
/// uncompressed. Highly compressible data compacted under the default Zstd
/// config produces far smaller table bytes than the same data compacted under
/// an Uncompressed config — proving the drain honors the codec (regression for
/// #3499/#3492, which shipped compression on flush + the flush-followup and
/// background compaction paths but not the fixed-point drain).
#[cfg(feature = "localfs")]
#[test]
fn test_explicit_compaction_applies_configured_codec() {
    fn table_bytes(root: &std::path::Path) -> u64 {
        let mut total = 0u64;
        let mut stack = vec![root.join("tables")];
        while let Some(dir) = stack.pop() {
            let Ok(entries) = std::fs::read_dir(&dir) else {
                continue;
            };
            for entry in entries.flatten() {
                match entry.file_type() {
                    Ok(file_type) if file_type.is_dir() => stack.push(entry.path()),
                    Ok(_) => {
                        if let Ok(meta) = entry.metadata() {
                            total = total.saturating_add(meta.len());
                        }
                    }
                    Err(_) => {}
                }
            }
        }
        total
    }
    fn compacted_table_bytes(name: &str, compression: crate::format::TableCompression) -> u64 {
        let root = temp_dir_for_api_test(name);
        let backend = crate::testkit::leak_static(StorageBackend::local_fs(root.clone()));
        let mut runtime = StorageRuntime::open_with_backend(
            StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
                .with_table_compression_for_test(compression),
            backend,
        )
        .expect("open")
        .into_runtime();
        let flush =
            MaintenanceRequest::new(MaintenanceTask::Flush, MaintenanceScope::Branch(branch()));
        // Highly compressible payloads across two flushed L0 tables, so the
        // fixed-point drain has something to merge and rewrite.
        let value = vec![b'z'; 2048];
        for pass in 0..2u8 {
            for index in 0..64u32 {
                runtime
                    .commit(&put_batch(format!("k{pass}-{index:04}").as_bytes(), &value))
                    .expect("commit");
            }
            runtime.maintenance(&flush).expect("flush");
        }
        runtime
            .maintenance(&MaintenanceRequest::new(
                MaintenanceTask::Compact,
                MaintenanceScope::Branch(branch()),
            ))
            .expect("compact");
        table_bytes(&root)
    }

    let zstd = compacted_table_bytes(
        "explicit-compact-zstd",
        crate::format::TableCompression::Zstd,
    );
    let uncompressed = compacted_table_bytes(
        "explicit-compact-uncompressed",
        crate::format::TableCompression::Uncompressed,
    );
    assert!(
        zstd.saturating_mul(2) < uncompressed,
        "explicit compaction did not apply the configured Zstd codec: \
         zstd={zstd} uncompressed={uncompressed}"
    );
}

/// #3519: under the default `KeepAll` policy nothing is pruned, so every
/// committed version stays readable across a clean reopen. W3.1c elided the
/// `COMMIT_TIMELINE` rows, leaving a commit's version→timestamp fact durable
/// only on its data rows and in a checkpoint's timeline group; a flush without
/// a subsequent checkpoint (close defers it while the non-seeded `_system_`
/// branch exists) lost the group, and the reopen's timeline-space scan then
/// fabricated an empty index that raised `HistoryUnavailable` on a retained
/// `as_of` read. The read-time fallback now rebuilds the timeline from the
/// branch's own data rows, so the flushed history reads back — by version AND
/// by timestamp.
#[cfg(feature = "localfs")]
#[test]
fn test_keepall_reopen_retains_historical_reads() {
    let (backend, mut runtime) = open_durable_runtime_with_backend(
        "keepall-reopen-repro",
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
    );
    let mut versions = Vec::new();
    for index in 0..4u8 {
        versions.push(
            runtime
                .commit(&put_batch(b"k", &[b'v', index]))
                .expect("commit")
                .commit_version(),
        );
    }
    runtime
        .maintenance(&MaintenanceRequest::new(
            MaintenanceTask::Flush,
            MaintenanceScope::Branch(branch()),
        ))
        .expect("flush");
    // Before reopen: the oldest version reads exactly. Capture its commit
    // timestamp so the reopened store can also be probed by wall-clock as_of.
    let before = runtime
        .read_point(&PointReadRequest::new(
            branch(),
            engine_space(),
            api_key(b"k"),
            ReadBound::AtVersion(versions[0]),
        ))
        .expect("v0 reads before reopen");
    let oldest_timestamp = before.row().expect("row").commit_timestamp();
    assert_eq!(
        before
            .row()
            .expect("row")
            .value()
            .expect("value")
            .as_bytes(),
        &[b'v', 0]
    );

    runtime.close().expect("clean close (checkpoint)");
    drop(runtime);
    let runtime = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
        backend,
    )
    .expect("reopen")
    .into_runtime();

    // After a CLEAN reopen the same retained version must still read.
    let after = runtime
        .read_point(&PointReadRequest::new(
            branch(),
            engine_space(),
            api_key(b"k"),
            ReadBound::AtVersion(versions[0]),
        ))
        .expect("v0 must still read after a clean reopen");
    assert_eq!(
        after.row().expect("row").value().expect("value").as_bytes(),
        &[b'v', 0]
    );

    // The timestamp path shares the same reconstruction fallback: an as_of read
    // at the oldest commit's timestamp must resolve rather than raise.
    let by_timestamp = runtime
        .read_point(&PointReadRequest::new(
            branch(),
            engine_space(),
            api_key(b"k"),
            ReadBound::AtTimestamp(oldest_timestamp),
        ))
        .expect("as_of-by-timestamp must resolve retained history after reopen");
    assert!(
        by_timestamp.row().is_some(),
        "timestamp as_of at the oldest retained commit returned no row"
    );
}

/// #3519 direction control: rebuilding the timeline from data rows must NOT
/// resurrect genuinely pruned history. Under an opt-in retention window the
/// oldest version is dropped below the published floor; after a clean reopen a
/// sub-floor `as_of` read must still RAISE `HistoryUnavailable`, because the
/// floor gate runs before the (now data-derived) timeline lookup.
#[cfg(feature = "localfs")]
#[test]
fn test_pruned_history_stays_unavailable_after_reopen() {
    let options = StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
        .with_version_retention_window(Some(1));
    let (backend, mut runtime) = open_durable_runtime_with_backend("prune-reopen-control", options);

    let flush = MaintenanceRequest::new(MaintenanceTask::Flush, MaintenanceScope::Branch(branch()));
    let mut versions = Vec::new();
    for index in 0..3u8 {
        versions.push(
            runtime
                .commit(&put_batch(b"k", &[b'v', index]))
                .expect("commit version")
                .commit_version(),
        );
    }
    runtime.maintenance(&flush).expect("flush first table");
    for index in 3..6u8 {
        versions.push(
            runtime
                .commit(&put_batch(b"k", &[b'v', index]))
                .expect("commit version")
                .commit_version(),
        );
    }
    runtime.maintenance(&flush).expect("flush second table");
    runtime
        .force_branch_compaction_for_test(branch())
        .expect("force pruning compaction");

    runtime.close().expect("clean close");
    drop(runtime);
    let runtime = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
            .with_version_retention_window(Some(1)),
        backend,
    )
    .expect("reopen")
    .into_runtime();

    let error = runtime
        .read_point(&PointReadRequest::new(
            branch(),
            engine_space(),
            api_key(b"k"),
            ReadBound::AtVersion(versions[0]),
        ))
        .expect_err("oldest version was pruned and must stay unavailable after reopen");
    assert_eq!(error.class(), StorageApiErrorClass::HistoryUnavailable);

    // The latest value still reads — the reconstruction did not lose live data.
    let latest = runtime
        .read_point(&PointReadRequest::new(
            branch(),
            engine_space(),
            api_key(b"k"),
            ReadBound::Latest,
        ))
        .expect("latest read after reopen");
    assert_eq!(
        latest
            .row()
            .expect("row")
            .value()
            .expect("value")
            .as_bytes(),
        &[b'v', 5]
    );
}

#[test]
fn api_explicit_compact_after_flush_drains_branch_sources() {
    let mut runtime = open_runtime();
    runtime
        .commit(&put_batch(b"drain-key", b"drain-value"))
        .expect("commit");
    let flush = MaintenanceRequest::new(MaintenanceTask::Flush, MaintenanceScope::Branch(branch()));
    let compact =
        MaintenanceRequest::new(MaintenanceTask::Compact, MaintenanceScope::Branch(branch()));

    let flush_outcome = runtime.maintenance(&flush).expect("flush outcome");
    let flushed = runtime
        .branch_source_layout_for_test(branch())
        .expect("flushed layout");
    let compact_outcome = runtime.maintenance(&compact).expect("compact outcome");
    let compacted = runtime
        .branch_source_layout_for_test(branch())
        .expect("compacted layout");
    let repeated = runtime.maintenance(&compact).expect("repeat compact");
    let repeated_layout = runtime
        .branch_source_layout_for_test(branch())
        .expect("repeat layout");
    let read = runtime
        .read_point(&PointReadRequest::new(
            branch(),
            engine_space(),
            api_key(b"drain-key"),
            ReadBound::Latest,
        ))
        .expect("read after compact");

    assert_eq!(flush_outcome.status(), MaintenanceSummaryStatus::Completed);
    assert_eq!(flushed.owned_l0_tables(), 1);
    assert_eq!(
        compact_outcome.status(),
        MaintenanceSummaryStatus::Completed
    );
    assert!(compact_outcome.state_changes() > 1);
    assert_eq!(compacted.owned_l0_tables(), 0);
    assert_eq!(owned_table_count_at(&compacted, terminal_owned_level()), 1);
    assert_eq!(compacted.owned_total_tables(), 1);
    assert_eq!(repeated.status(), MaintenanceSummaryStatus::Deferred);
    assert_eq!(
        repeated.reason_class(),
        Some(MaintenanceReasonClass::Deferred)
    );
    assert_eq!(repeated.state_changes(), 0);
    assert_eq!(repeated_layout, compacted);
    assert_eq!(
        runtime
            .maintenance_status()
            .expect("maintenance status")
            .pending_tasks(),
        0
    );
    assert_eq!(
        read.row().expect("row").value().expect("value").as_bytes(),
        b"drain-value"
    );
}

#[test]
fn api_queued_compact_keeps_table_level_semantics() {
    let mut runtime = open_runtime();
    runtime
        .commit(&put_batch(b"queued-key", b"queued-value"))
        .expect("commit");
    let flush = MaintenanceRequest::new(MaintenanceTask::Flush, MaintenanceScope::Branch(branch()));
    let compact =
        MaintenanceRequest::new(MaintenanceTask::Compact, MaintenanceScope::Branch(branch()));

    runtime.maintenance(&flush).expect("flush outcome");
    runtime
        .enqueue_maintenance(&compact)
        .expect("enqueue compact");
    let drain = runtime.drain_maintenance().expect("drain compact");
    let layout = runtime
        .branch_source_layout_for_test(branch())
        .expect("layout");

    assert_eq!(drain.drained_tasks(), 1);
    assert_eq!(drain.outcomes()[0].task(), MaintenanceTask::Compact);
    assert_eq!(
        drain.outcomes()[0].status(),
        MaintenanceSummaryStatus::Completed
    );
    assert_eq!(layout.owned_l0_tables(), 0);
    assert_eq!(owned_table_count_at(&layout, 1), 1);
    assert_eq!(owned_table_count_at(&layout, terminal_owned_level()), 0);
}

#[test]
#[cfg(feature = "localfs")]
fn api_explicit_compact_reports_equivalent_cache_and_durable_layouts() {
    let mut cache = open_runtime();
    let mut durable = open_durable_runtime_with_options(
        "maintenance-compact-layout-equivalence",
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
    );
    let flush = MaintenanceRequest::new(MaintenanceTask::Flush, MaintenanceScope::Branch(branch()));
    let compact =
        MaintenanceRequest::new(MaintenanceTask::Compact, MaintenanceScope::Branch(branch()));

    for runtime in [&mut cache, &mut durable] {
        runtime
            .commit(&put_batch(b"equivalent-key", b"equivalent-value"))
            .expect("commit");
        runtime.maintenance(&flush).expect("flush");
        let outcome = runtime.maintenance(&compact).expect("compact");
        assert_eq!(outcome.status(), MaintenanceSummaryStatus::Completed);
    }

    let cache_layout = cache
        .branch_source_layout_for_test(branch())
        .expect("cache layout");
    let durable_layout = durable
        .branch_source_layout_for_test(branch())
        .expect("durable layout");

    assert_eq!(cache_layout, durable_layout);
    assert_eq!(cache_layout.owned_l0_tables(), 0);
    assert_eq!(
        owned_table_count_at(&cache_layout, terminal_owned_level()),
        1
    );
}

#[test]
fn api_wal_growth_policy_status_reports_no_durable_action_for_cache() {
    let mut runtime = open_runtime();
    let request = MaintenanceRequest::new(MaintenanceTask::WalGrowth, MaintenanceScope::Global);

    let outcome = runtime.maintenance(&request).expect("wal growth outcome");
    let growth = outcome.wal_growth().expect("wal growth facts");

    assert_eq!(outcome.status(), MaintenanceSummaryStatus::Completed);
    assert_eq!(growth.status(), MaintenanceWalGrowthStatus::NoDurableAction);
    assert!(!growth.checkpoint_enqueued());
}

#[test]
fn api_wal_growth_policy_status_reports_not_needed() {
    #[cfg(feature = "localfs")]
    let mut runtime = open_durable_runtime_with_options(
        "maintenance-wal-growth-below-threshold",
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
            .with_wal_growth_policy(StorageWalGrowthPolicy::thresholds(
                u64::MAX,
                usize::MAX,
                u64::MAX,
            )),
    );
    #[cfg(not(feature = "localfs"))]
    let mut runtime = open_runtime();
    let request = MaintenanceRequest::new(MaintenanceTask::WalGrowth, MaintenanceScope::Global);

    let outcome = runtime.maintenance(&request).expect("WAL growth outcome");
    let growth = outcome.wal_growth().expect("WAL growth summary");

    #[cfg(feature = "localfs")]
    assert_eq!(growth.status(), MaintenanceWalGrowthStatus::BelowThreshold);
    #[cfg(not(feature = "localfs"))]
    assert_eq!(growth.status(), MaintenanceWalGrowthStatus::NoDurableAction);
    assert!(!growth.checkpoint_enqueued());
}

#[test]
#[cfg(feature = "localfs")]
fn api_wal_growth_policy_status_reports_disabled() {
    let mut runtime = open_durable_runtime_with_options(
        "maintenance-wal-growth-disabled",
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
            .with_wal_growth_policy(StorageWalGrowthPolicy::Disabled),
    );
    let request = MaintenanceRequest::new(MaintenanceTask::WalGrowth, MaintenanceScope::Global);

    let outcome = runtime.maintenance(&request).expect("WAL growth outcome");
    let growth = outcome.wal_growth().expect("WAL growth summary");

    assert_eq!(growth.status(), MaintenanceWalGrowthStatus::Disabled);
    assert!(!growth.checkpoint_enqueued());
}

#[test]
#[cfg(feature = "localfs")]
fn api_wal_growth_policy_status_reports_checkpoint_due() {
    let options = StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
        .with_maintenance_scheduling_policy(StorageMaintenanceSchedulingPolicy::EvaluateAndEnqueue)
        .with_wal_growth_policy(StorageWalGrowthPolicy::thresholds(u64::MAX, usize::MAX, 1));
    let mut runtime = open_durable_runtime_with_options("maintenance-wal-growth-due", options);
    runtime
        .commit(&put_batch(b"growth-a", b"value"))
        .expect("first commit");
    runtime
        .commit(&put_batch(b"growth-b", b"value"))
        .expect("second commit");
    let request = MaintenanceRequest::new(MaintenanceTask::WalGrowth, MaintenanceScope::Global);

    let outcome = runtime.maintenance(&request).expect("WAL growth outcome");
    let growth = outcome.wal_growth().expect("WAL growth summary");

    assert_eq!(outcome.task(), MaintenanceTask::WalGrowth);
    assert!(matches!(
        growth.status(),
        MaintenanceWalGrowthStatus::MaintenanceEnqueued
            | MaintenanceWalGrowthStatus::MaintenanceCoalesced
    ));
    assert_eq!(
        growth.trigger(),
        Some(MaintenanceWalGrowthTrigger::CommitsSinceCheckpoint)
    );
    assert!(growth.checkpoint_enqueued());
    assert_eq!(growth.commits_since_checkpoint(), 2);
    let status = runtime.maintenance_status().expect("status");
    assert!(status.pending_tasks() >= 4);
    let pending_kinds = runtime.pending_lifecycle_maintenance_kinds_for_test();
    for expected in [
        crate::lifecycle::MaintenanceTaskKind::Flush,
        crate::lifecycle::MaintenanceTaskKind::Checkpoint,
        crate::lifecycle::MaintenanceTaskKind::FlushWatermark,
        crate::lifecycle::MaintenanceTaskKind::WalTruncation,
    ] {
        assert!(
            pending_kinds.contains(&expected),
            "WAL growth should enqueue {expected:?}; pending={pending_kinds:?}"
        );
    }
}

#[test]
fn api_wal_growth_trigger_runs_supported_path() {
    let mut runtime = open_runtime();
    let request = MaintenanceRequest::new(MaintenanceTask::WalGrowth, MaintenanceScope::Global);

    let outcome = runtime.maintenance(&request).expect("WAL growth outcome");

    assert_eq!(outcome.task(), MaintenanceTask::WalGrowth);
    assert!(outcome.wal_growth().is_some());
}

#[test]
fn api_wal_growth_enqueue_rejects_direct_queueing() {
    let runtime = open_runtime();
    let request = MaintenanceRequest::new(MaintenanceTask::WalGrowth, MaintenanceScope::Global);

    let error = runtime
        .enqueue_maintenance(&request)
        .expect_err("WAL growth is policy-evaluated directly");

    assert_eq!(error.class(), StorageApiErrorClass::InvalidArgument);
}

#[test]
fn api_maintenance_enqueue_and_drain_are_deterministic() {
    let mut runtime = open_runtime();
    runtime
        .commit(&put_batch(b"queue", b"value"))
        .expect("commit");
    let request =
        MaintenanceRequest::new(MaintenanceTask::Flush, MaintenanceScope::Branch(branch()));

    let queued = runtime
        .enqueue_maintenance(&request)
        .expect("enqueue flush");
    assert_eq!(queued.pending_tasks(), 1);
    assert_eq!(queued.enqueued(), 1);

    let drain = runtime.drain_maintenance().expect("drain maintenance");

    assert_eq!(drain.drained_tasks(), 1);
    assert_eq!(drain.outcomes().len(), 1);
    assert_eq!(drain.outcomes()[0].task(), MaintenanceTask::Flush);
    assert_eq!(drain.queue().pending_tasks(), 0);
}

#[test]
fn api_maintenance_queue_status_reports_pending_only() {
    let runtime = open_manual_runtime();
    runtime
        .commit(&put_batch(b"status", b"value"))
        .expect("commit");
    let request =
        MaintenanceRequest::new(MaintenanceTask::Flush, MaintenanceScope::Branch(branch()));

    let queued = runtime
        .enqueue_maintenance(&request)
        .expect("enqueue flush");
    let status = runtime.maintenance_status().expect("status");

    assert_eq!(queued.pending_tasks(), 1);
    assert_eq!(status.pending_tasks(), 1);
    // Active task ids are transient inside run_task and are not observable from
    // this synchronous API without a dedicated hook.
    assert_eq!(status.active_task(), None);
}

#[test]
fn api_cache_durable_only_enqueue_rejects_without_stranding_tasks() {
    let runtime = open_runtime();
    let cases = [
        MaintenanceRequest::new(MaintenanceTask::Checkpoint, MaintenanceScope::Global),
        MaintenanceRequest::new(MaintenanceTask::Retain, MaintenanceScope::Global),
        MaintenanceRequest::new(MaintenanceTask::SnapshotPruning, MaintenanceScope::Global),
        MaintenanceRequest::new(MaintenanceTask::Reclaim, MaintenanceScope::Branch(branch())),
        MaintenanceRequest::new(MaintenanceTask::Quarantine, MaintenanceScope::Global),
        MaintenanceRequest::new(MaintenanceTask::Purge, MaintenanceScope::Branch(branch())),
        MaintenanceRequest::new(MaintenanceTask::Repair, MaintenanceScope::Global),
    ];

    for request in cases {
        let error = runtime
            .enqueue_maintenance(&request)
            .expect_err("cache durable-only enqueue rejects");

        assert_eq!(error.class(), StorageApiErrorClass::FailedPrecondition);
        assert_eq!(
            runtime
                .maintenance_status()
                .expect("maintenance status")
                .pending_tasks(),
            0
        );
    }
}

#[test]
fn api_maintenance_drain_is_deterministic() {
    let mut runtime = open_manual_runtime();
    runtime
        .commit(&put_batch(b"first", b"value"))
        .expect("first commit");
    let flush = MaintenanceRequest::new(MaintenanceTask::Flush, MaintenanceScope::Branch(branch()));
    let compact =
        MaintenanceRequest::new(MaintenanceTask::Compact, MaintenanceScope::Branch(branch()));

    runtime
        .enqueue_maintenance(&compact)
        .expect("enqueue compact");
    runtime.enqueue_maintenance(&flush).expect("enqueue flush");
    let drain = runtime.drain_maintenance().expect("drain");

    assert_eq!(drain.outcomes()[0].task(), MaintenanceTask::Flush);
    assert_eq!(drain.queue().pending_tasks(), 0);
}

#[test]
fn api_maintenance_drain_preserves_branch_scope() {
    let mut runtime = open_runtime();
    runtime
        .commit(&put_batch(b"parent-scope", b"value"))
        .expect("parent commit");
    let child = branch_with(0x74);
    fork_branch(&mut runtime, child);
    runtime
        .commit(&put_batch_for(child, b"child-scope", b"value"))
        .expect("child commit");
    let request = MaintenanceRequest::new(MaintenanceTask::Flush, MaintenanceScope::Branch(child));

    runtime
        .enqueue_maintenance(&request)
        .expect("enqueue child flush");
    let drain = runtime.drain_maintenance().expect("drain child flush");

    assert_eq!(drain.drained_tasks(), 1);
    assert_eq!(drain.outcomes()[0].scope(), MaintenanceScope::Branch(child));
    assert_ne!(
        drain.outcomes()[0].status(),
        MaintenanceSummaryStatus::Failed
    );
}

#[test]
#[cfg(feature = "localfs")]
fn api_repair_branch_scope_round_trips_through_drain() {
    let (backend, mut runtime) = open_durable_runtime_with_backend(
        "maintenance-repair-branch-scope",
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
            .with_maintenance_scheduling_policy(
                StorageMaintenanceSchedulingPolicy::EvaluateAndEnqueue,
            ),
    );
    let child = branch_with(0x77);
    runtime
        .commit(&put_batch(b"repair-parent", b"value"))
        .expect("seed parent history");
    fork_branch(&mut runtime, child);
    let orphan =
        crate::layout::ObjectLayout::quarantine_object(&child.to_string(), "api-branch-orphan")
            .expect("orphan quarantine object");
    backend
        .as_backend()
        .write_object(&orphan, b"orphan")
        .expect("write orphan quarantine object");
    let request = MaintenanceRequest::new(MaintenanceTask::Repair, MaintenanceScope::Branch(child));

    runtime
        .enqueue_maintenance(&request)
        .expect("enqueue branch repair");
    let drain = runtime.drain_maintenance().expect("drain branch repair");

    assert_eq!(drain.drained_tasks(), 1);
    assert_eq!(drain.outcomes()[0].task(), MaintenanceTask::Repair);
    assert_eq!(drain.outcomes()[0].scope(), MaintenanceScope::Branch(child));
    assert!(drain.outcomes()[0].recovery_health().is_some());
}

#[test]
fn api_maintenance_after_close_rejects() {
    let mut runtime = open_runtime();
    runtime.close().expect("close");
    let request =
        MaintenanceRequest::new(MaintenanceTask::Flush, MaintenanceScope::Branch(branch()));

    let error = runtime
        .maintenance(&request)
        .expect_err("closed runtime rejects maintenance");

    assert_eq!(error.class(), StorageApiErrorClass::FailedPrecondition);
    assert_eq!(error.code(), "failed_precondition.storage_api.state");
}

#[test]
fn api_compact_after_close_rejects() {
    let mut runtime = open_runtime();
    runtime.close().expect("close");
    let request =
        MaintenanceRequest::new(MaintenanceTask::Compact, MaintenanceScope::Branch(branch()));

    let error = runtime
        .maintenance(&request)
        .expect_err("closed runtime rejects compact");

    assert_eq!(error.class(), StorageApiErrorClass::FailedPrecondition);
    assert_eq!(error.code(), "failed_precondition.storage_api.state");
}

#[test]
fn api_retention_without_current_proof_rejects() {
    let proof = crate::lifecycle::LifecycleRetentionProof::new(
        crate::lifecycle::LifecycleRetentionProofStatus::Incomplete,
        crate::lifecycle::RecoveryHealth::Healthy,
        None,
        None,
        None,
        Some("missing current proof"),
    );
    let lower = crate::lifecycle::LifecycleRetentionOutcome::from_decisions(proof, Vec::new(), 0)
        .expect("incomplete proof outcome")
        .maintenance_outcome();
    let request = MaintenanceRequest::new(MaintenanceTask::Retain, MaintenanceScope::Global);

    let outcome = map_maintenance_outcome_for_test(request, &lower);

    assert_eq!(outcome.task(), MaintenanceTask::Retain);
    assert_eq!(outcome.status(), MaintenanceSummaryStatus::Deferred);
    assert_eq!(outcome.reason(), Some("retention proof is incomplete"));
    assert_eq!(
        outcome.recovery_health(),
        Some(RecoveryHealthSummary::Degraded)
    );
}

#[test]
fn api_retention_table_objects_deferred_when_unsupported() {
    let mut runtime = open_runtime();
    let request =
        MaintenanceRequest::new(MaintenanceTask::Reclaim, MaintenanceScope::Branch(branch()));

    let outcome = runtime.maintenance(&request).expect("reclaim outcome");

    assert_eq!(outcome.task(), MaintenanceTask::Reclaim);
    assert_eq!(outcome.status(), MaintenanceSummaryStatus::Deferred);
}

#[test]
fn api_retention_noop_does_not_report_successful_table_deletion() {
    let lower = crate::lifecycle::MaintenanceOutcome::new(
        crate::lifecycle::MaintenanceTaskKind::Retention,
        crate::lifecycle::MaintenanceOutcomeStatus::Deferred,
    )
    .with_reason("retention scope not supported by generic path");
    let request =
        MaintenanceRequest::new(MaintenanceTask::Reclaim, MaintenanceScope::Branch(branch()));

    let outcome = map_maintenance_outcome_for_test(request, &lower);

    assert_eq!(outcome.status(), MaintenanceSummaryStatus::Deferred);
    assert_eq!(outcome.affected_objects(), 0);
    assert_eq!(outcome.bytes_reclaimed(), 0);
    assert_eq!(outcome.state_changes(), 0);
}

#[test]
fn api_checkpoint_truncation_debt_does_not_fail_checkpoint_summary() {
    let lower = crate::lifecycle::MaintenanceOutcome::new(
        crate::lifecycle::MaintenanceTaskKind::Checkpoint,
        crate::lifecycle::MaintenanceOutcomeStatus::Completed,
    )
    .with_reason("WAL truncation follow-up has health debt")
    .with_source_error(
        crate::lifecycle::LifecycleError::CheckpointSnapshotOrphaned {
            object: Some("snapshots/0000000000000001".to_string()),
            reason: "snapshot published before manifest update",
        },
    );
    let request = MaintenanceRequest::new(MaintenanceTask::Checkpoint, MaintenanceScope::Global);

    let outcome = map_maintenance_outcome_for_test(request, &lower);

    assert_eq!(outcome.task(), MaintenanceTask::Checkpoint);
    assert_eq!(outcome.status(), MaintenanceSummaryStatus::Completed);
    assert_eq!(outcome.reason_class(), None);
    assert_eq!(
        outcome.source_error_code(),
        Some("failed_precondition.storage_api.maintenance")
    );
}

#[test]
fn api_snapshot_pruning_preserves_required_snapshot() {
    #[cfg(feature = "localfs")]
    let mut runtime = open_durable_runtime_with_options(
        "maintenance-snapshot-pruning-preserves-required",
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
    );
    #[cfg(not(feature = "localfs"))]
    let mut runtime = open_runtime();
    #[cfg(feature = "localfs")]
    {
        runtime
            .commit(&put_batch(b"snapshot-prune", b"value"))
            .expect("commit before checkpoint");
        runtime
            .maintenance(&MaintenanceRequest::new(
                MaintenanceTask::Checkpoint,
                MaintenanceScope::Global,
            ))
            .expect("checkpoint before pruning");
    }
    let request =
        MaintenanceRequest::new(MaintenanceTask::SnapshotPruning, MaintenanceScope::Global);

    let outcome = runtime
        .maintenance(&request)
        .expect("snapshot pruning outcome");

    assert_eq!(outcome.task(), MaintenanceTask::SnapshotPruning);
    #[cfg(feature = "localfs")]
    {
        assert_eq!(outcome.status(), MaintenanceSummaryStatus::Completed);
        assert_eq!(outcome.affected_objects(), 1);
        assert_eq!(outcome.bytes_reclaimed(), 0);
        assert_eq!(outcome.state_changes(), 0);
        // A prune that protected the only snapshot reclaimed nothing, and the
        // ledger says exactly that for the snapshot family.
        let prune = runtime
            .reclaim_ledger_for_test()
            .expect("durable runtime has a reclaim ledger")
            .last(ReclaimFamily::SnapshotPrune)
            .expect("snapshot prune recorded");
        assert_eq!(prune.outcome(), ReclaimOutcome::Nothing, "{prune:?}");
        assert_eq!(prune.objects_affected(), 1);
        assert_eq!(prune.deferral(), None);
    }
    #[cfg(not(feature = "localfs"))]
    {
        assert_eq!(outcome.status(), MaintenanceSummaryStatus::Deferred);
        assert_eq!(
            outcome.reason(),
            Some("cache runtime does not support durable snapshot pruning maintenance")
        );
        assert_eq!(runtime.reclaim_ledger_for_test(), None);
    }
}

#[cfg(feature = "localfs")]
fn snapshot_object_ids(backend: &StorageBackend) -> Vec<u64> {
    crate::service::SnapshotService::new(backend.as_backend())
        .list_snapshots()
        .expect("list snapshots")
        .iter()
        .map(crate::service::SnapshotObject::snapshot_id)
        .collect()
}

#[cfg(feature = "localfs")]
fn attested_snapshot_id(backend: &StorageBackend) -> Option<u64> {
    crate::service::DatabaseManifestService::new(backend.as_backend())
        .load_required()
        .expect("database manifest")
        .snapshot_id()
}

#[cfg(feature = "localfs")]
fn checkpoint_completed(runtime: &mut StorageRuntime<'static>) {
    let outcome = runtime
        .maintenance(&MaintenanceRequest::new(
            MaintenanceTask::Checkpoint,
            MaintenanceScope::Global,
        ))
        .expect("checkpoint");
    assert_eq!(outcome.status(), MaintenanceSummaryStatus::Completed);
}

/// Space-reclamation contract §3.4 (slice 5, #3592): every completed
/// checkpoint chains a superseded-snapshot prune, so a session that
/// checkpoints N times owns one snapshot object once the queue drains.
#[cfg(feature = "localfs")]
#[test]
fn api_completed_checkpoints_chain_a_prune_that_leaves_one_snapshot() {
    let (backend, mut runtime) = open_durable_runtime_with_backend(
        "snapshot-prune-chain",
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
            .with_maintenance_scheduling_policy(
                StorageMaintenanceSchedulingPolicy::EvaluateAndEnqueue,
            ),
    );
    for key in [b"chain-a" as &[u8], b"chain-b", b"chain-c"] {
        runtime.commit(&put_batch(key, b"value")).expect("commit");
        checkpoint_completed(&mut runtime);
    }
    let attested = attested_snapshot_id(backend).expect("attested snapshot");
    assert_eq!(
        snapshot_object_ids(backend).len(),
        3,
        "enqueue-only scheduling leaves the chained prunes queued"
    );

    let drain = runtime.drain_maintenance().expect("drain");

    assert!(drain.outcomes().iter().any(|outcome| {
        outcome.task() == MaintenanceTask::SnapshotPruning
            && outcome.status() == MaintenanceSummaryStatus::Completed
    }));
    assert_eq!(snapshot_object_ids(backend), vec![attested]);
    runtime.close().expect("close");
}

/// Space-reclamation contract §3.4 (slice 5, #3592): the open reclaim window
/// reconciles the snapshot family to the manifest-attested id — superseded
/// objects a prior session never pruned AND a crash orphan above it.
#[cfg(feature = "localfs")]
#[test]
fn api_reopen_reconciles_snapshots_to_the_attested_id() {
    let root = temp_dir_for_api_test("snapshot-prune-reopen");
    let backend: &'static StorageBackend =
        crate::testkit::leak_static(StorageBackend::local_fs(root));
    let options = || {
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
            .with_maintenance_scheduling_policy(
                StorageMaintenanceSchedulingPolicy::EvaluateAndEnqueue,
            )
    };
    {
        let mut runtime = StorageRuntime::open_with_backend(options(), backend)
            .expect("open")
            .into_runtime();
        for key in [b"reopen-a" as &[u8], b"reopen-b"] {
            runtime.commit(&put_batch(key, b"value")).expect("commit");
            checkpoint_completed(&mut runtime);
        }
        runtime.close().expect("close");
    }
    let attested = attested_snapshot_id(backend).expect("attested snapshot");
    // A crash orphan: a snapshot object the manifest never attested.
    let orphan_id = attested + 7;
    let live_object = crate::layout::ObjectLayout::snapshot(attested).expect("live object");
    let orphan_object = crate::layout::ObjectLayout::snapshot(orphan_id).expect("orphan object");
    let live_bytes = backend
        .as_backend()
        .read_object(&live_object)
        .expect("live snapshot bytes");
    backend
        .as_backend()
        .write_object(&orphan_object, &live_bytes)
        .expect("plant orphan");
    let before_open = snapshot_object_ids(backend);
    assert!(before_open.contains(&orphan_id));
    assert!(before_open.len() >= 2);
    // #3622: the bytes the reconcile will free — every snapshot but the
    // attested one.
    let doomed_bytes: u64 = before_open
        .iter()
        .filter(|id| **id != attested)
        .map(|id| {
            let object = crate::layout::ObjectLayout::snapshot(*id).expect("object");
            backend
                .as_backend()
                .read_object(&object)
                .expect("snapshot bytes")
                .len() as u64
        })
        .sum();
    assert!(doomed_bytes > 0);

    let mut runtime = StorageRuntime::open_with_backend(options(), backend)
        .expect("reopen")
        .into_runtime();

    assert_eq!(
        snapshot_object_ids(backend),
        before_open,
        "open reclaims nothing inline"
    );
    let drain = runtime.drain_maintenance().expect("drain");
    let prune = drain
        .outcomes()
        .iter()
        .find(|outcome| {
            outcome.task() == MaintenanceTask::SnapshotPruning
                && outcome.status() == MaintenanceSummaryStatus::Completed
        })
        .expect("the reconcile prune completed");
    // #3622: the prune reports the bytes it freed, and so does the ledger.
    assert_eq!(prune.bytes_reclaimed(), doomed_bytes);
    let ledger = runtime.reclaim_ledger_for_test().expect("ledger");
    assert_eq!(
        ledger
            .last(ReclaimFamily::SnapshotPrune)
            .map(crate::lifecycle::ReclaimPass::bytes_reclaimed),
        Some(doomed_bytes),
        "{ledger:?}"
    );
    assert_eq!(snapshot_object_ids(backend), vec![attested]);
    assert_eq!(attested_snapshot_id(backend), Some(attested));
    let value = runtime
        .read_point(&PointReadRequest::new(
            branch(),
            engine_space(),
            api_key(b"reopen-b"),
            ReadBound::Latest,
        ))
        .expect("read")
        .row()
        .map(|row| row.value().expect("put row").as_bytes().to_vec());
    assert_eq!(value, Some(b"value".to_vec()));
    runtime.close().expect("close");
}

#[test]
fn api_cache_runtime_has_no_reclaim_ledger() {
    // A cache runtime owns no durable objects to reclaim, so it exposes no
    // ledger (space-reclamation contract §3.5). Unlike the assertion inside
    // `api_snapshot_pruning_preserves_required_snapshot`, this holds regardless
    // of the `localfs` feature, so it runs in the primary workspace CI lane.
    let runtime = open_manual_runtime();
    assert_eq!(runtime.reclaim_ledger_for_test(), None);
}

#[test]
fn api_quarantine_via_queue_defers_without_explicit_request() {
    let mut runtime = open_runtime();
    let request = MaintenanceRequest::new(MaintenanceTask::Quarantine, MaintenanceScope::Global);

    let outcome = runtime.maintenance(&request).expect("quarantine outcome");

    assert_eq!(outcome.task(), MaintenanceTask::Quarantine);
    assert_eq!(outcome.status(), MaintenanceSummaryStatus::Deferred);
    assert_eq!(
        outcome.reason(),
        Some("cache runtime does not support durable quarantine maintenance")
    );
    assert_eq!(
        outcome.reason_class(),
        Some(MaintenanceReasonClass::Deferred)
    );
    assert_eq!(outcome.affected_objects(), 0);
}

#[test]
fn api_purge_requires_fresh_proof() {
    #[cfg(feature = "localfs")]
    {
        let (backend, mut runtime) = open_durable_runtime_with_backend(
            "maintenance-purge-fresh-proof",
            StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
        );
        stage_quarantine_object(backend, branch(), "api-purge-source", b"purge-me");
        let request =
            MaintenanceRequest::new(MaintenanceTask::Purge, MaintenanceScope::Branch(branch()));

        let outcome = runtime.maintenance(&request).expect("purge outcome");

        assert_eq!(outcome.task(), MaintenanceTask::Purge);
        assert_eq!(outcome.status(), MaintenanceSummaryStatus::Completed);
        assert!(outcome.bytes_reclaimed() >= b"purge-me".len() as u64);
        assert!(outcome.affected_objects() >= 2);
    }

    #[cfg(not(feature = "localfs"))]
    {
        let mut runtime = open_runtime();
        let request =
            MaintenanceRequest::new(MaintenanceTask::Purge, MaintenanceScope::Branch(branch()));

        let outcome = runtime.maintenance(&request).expect("purge outcome");

        assert_eq!(outcome.status(), MaintenanceSummaryStatus::Deferred);
        assert_eq!(
            outcome.reason_class(),
            Some(MaintenanceReasonClass::Deferred)
        );
    }
}

#[test]
fn api_repair_reports_reconciliation_facts() {
    #[cfg(feature = "localfs")]
    let (backend, mut runtime) = open_durable_runtime_with_backend(
        "maintenance-repair-reconciliation-facts",
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
    );
    #[cfg(feature = "localfs")]
    {
        let orphan = crate::layout::ObjectLayout::quarantine_object(
            &branch().to_string(),
            "api-repair-orphan",
        )
        .expect("orphan quarantine object");
        backend
            .as_backend()
            .write_object(&orphan, b"orphan")
            .expect("write orphan quarantine object");
    }
    #[cfg(not(feature = "localfs"))]
    let mut runtime = open_runtime();
    let request = MaintenanceRequest::new(MaintenanceTask::Repair, MaintenanceScope::Global);

    let outcome = runtime.maintenance(&request).expect("repair outcome");

    assert_eq!(outcome.task(), MaintenanceTask::Repair);
    #[cfg(feature = "localfs")]
    {
        assert_eq!(outcome.status(), MaintenanceSummaryStatus::Completed);
        assert!(outcome.recovery_health().is_some());
        assert!(outcome.affected_objects() > 0);
    }
    #[cfg(not(feature = "localfs"))]
    {
        assert_eq!(outcome.status(), MaintenanceSummaryStatus::Deferred);
        assert_eq!(
            outcome.reason(),
            Some("cache runtime does not support quarantine repair maintenance")
        );
    }
}

#[test]
fn api_reclaim_degraded_health_blocks_when_required() {
    let health = crate::lifecycle::RecoveryHealth::degraded(
        crate::lifecycle::RecoveryDegradationClass::PolicyDowngrade,
        vec![crate::lifecycle::RecoveryFault::new(
            crate::lifecycle::RecoveryFaultKind::QuarantineInventoryMismatch,
            "current recovery health blocks reclaim",
        )
        .expect("fault")],
    )
    .expect("degraded health");
    let proof = crate::lifecycle::LifecycleRetentionProof::new(
        crate::lifecycle::LifecycleRetentionProofStatus::BlockedByRecoveryHealth,
        health,
        None,
        Some(CommitVersion::new(1)),
        Some(CommitVersion::new(1)),
        None,
    );
    let lower = crate::lifecycle::LifecycleRetentionOutcome::from_decisions(proof, Vec::new(), 0)
        .expect("blocked proof outcome")
        .maintenance_outcome();
    let request =
        MaintenanceRequest::new(MaintenanceTask::Reclaim, MaintenanceScope::Branch(branch()));

    let outcome = map_maintenance_outcome_for_test(request, &lower);

    assert_eq!(outcome.task(), MaintenanceTask::Reclaim);
    assert_eq!(outcome.status(), MaintenanceSummaryStatus::Deferred);
    assert_eq!(outcome.reason(), Some("recovery health blocks retention"));
    assert_eq!(
        outcome.recovery_health(),
        Some(RecoveryHealthSummary::Degraded)
    );
}

#[test]
fn api_rewrite_unknown_branch_rejects() {
    let mut runtime = open_runtime();
    let unknown = BranchId::from_bytes([0x55; BranchId::BYTE_LEN]);
    let request =
        MaintenanceRequest::new(MaintenanceTask::Compact, MaintenanceScope::Branch(unknown));

    let error = runtime
        .maintenance(&request)
        .expect_err("unknown branch rejects rewrite maintenance");

    assert_eq!(error.class(), StorageApiErrorClass::NotFound);
}

// ---- Table-object GC (mark → sweep → purge) ----

/// Table DATA object files on disk under `tables/<branch>/l*/` — the physical footprint the GC
/// reclaims. Manifests (`tables/<branch>/manifest.object@`) are excluded: they are not data
/// objects and are never quarantine candidates.
#[cfg(feature = "localfs")]
fn table_data_object_files(root: &std::path::Path) -> std::collections::BTreeSet<String> {
    let mut files = std::collections::BTreeSet::new();
    let tables = root.join("tables");
    let Ok(branches) = std::fs::read_dir(&tables) else {
        return files;
    };
    for branch_dir in branches.flatten() {
        let Ok(levels) = std::fs::read_dir(branch_dir.path()) else {
            continue;
        };
        for level_dir in levels.flatten() {
            if !level_dir.path().is_dir() {
                continue;
            }
            let Ok(objects) = std::fs::read_dir(level_dir.path()) else {
                continue;
            };
            for object in objects.flatten() {
                if object.path().is_file() {
                    files.insert(object.path().display().to_string());
                }
            }
        }
    }
    files
}

/// Drain the maintenance queue to a fixed point: each drain runs the queued tasks (including the
/// GC chain's self-enqueued follow-ups); repeat until a drain finds nothing.
#[cfg(feature = "localfs")]
pub(super) fn drain_maintenance_to_idle(runtime: &mut StorageRuntime<'static>) {
    for _ in 0..8 {
        // Slice 8: a foreground verb wakes the worker for the follow-ups it
        // queued, so the worker may hold part of the chain; settle it before
        // and after each foreground drain so "idle" means both are empty.
        runtime.wait_background_idle_for_test();
        let drain = runtime.drain_maintenance().expect("drain maintenance");
        runtime.wait_background_idle_for_test();
        if drain.drained_tasks() == 0
            && runtime
                .maintenance_status()
                .expect("status")
                .pending_tasks()
                == 0
        {
            return;
        }
    }
    panic!("maintenance queue did not reach idle within the drain budget");
}

/// End-to-end table-object GC: a compaction supersedes L0 objects; the auto-enqueued
/// retention (mark) → quarantine (sweep) → purge chain physically deletes them; reads stay
/// correct. This is the regression test for the unbounded space amplification (4,819 objects /
/// 92 GB vs ~577 live at 10M) where superseded objects were never reclaimed.
#[cfg(feature = "localfs")]
#[test]
fn api_compaction_gc_reclaims_superseded_table_objects() {
    let root = temp_dir_for_api_test("maintenance-gc-end-to-end");
    let backend = crate::testkit::leak_static(StorageBackend::local_fs(root.clone()));
    let mut runtime = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
        backend,
    )
    .expect("open durable runtime")
    .into_runtime();

    runtime.commit(&put_batch(b"gc-a", b"one")).expect("commit");
    runtime
        .flush_default_branch_for_test()
        .expect("flush first L0 table");
    runtime.commit(&put_batch(b"gc-a", b"two")).expect("commit");
    runtime.commit(&put_batch(b"gc-b", b"x")).expect("commit");
    runtime
        .flush_default_branch_for_test()
        .expect("flush second L0 table");
    let before = table_data_object_files(&root);
    assert!(
        before.len() >= 2,
        "two flushes must produce at least two data objects, got {}",
        before.len()
    );

    let compact =
        MaintenanceRequest::new(MaintenanceTask::Compact, MaintenanceScope::Branch(branch()));
    runtime.maintenance(&compact).expect("compact");
    drain_maintenance_to_idle(&mut runtime);

    let after = table_data_object_files(&root);
    for superseded in &before {
        assert!(
            !after.contains(superseded),
            "superseded object {superseded} must be reclaimed",
        );
    }
    assert!(
        !after.is_empty(),
        "the compaction output object must survive the sweep",
    );
    let value = runtime
        .read_point(&PointReadRequest::new(
            branch(),
            engine_space(),
            api_key(b"gc-a"),
            ReadBound::Latest,
        ))
        .expect("read after gc")
        .row()
        .expect("row present")
        .value()
        .expect("put row")
        .as_bytes()
        .to_vec();
    assert_eq!(value, b"two");

    // The reclaim ledger recorded every family the chain ran, from the one
    // executor completion hook: the mark found candidates, the sweep staged
    // bytes, the purge deleted objects.
    let ledger = runtime
        .reclaim_ledger_for_test()
        .expect("durable runtime has a reclaim ledger");
    let mark = ledger
        .last(ReclaimFamily::TableObjectMark)
        .expect("mark recorded");
    assert!(
        mark.objects_affected() >= 1,
        "mark saw candidates: {mark:?}"
    );
    let sweep = ledger
        .last(ReclaimFamily::TableObjectSweep)
        .expect("sweep recorded");
    assert_eq!(sweep.outcome(), ReclaimOutcome::Reclaimed, "{sweep:?}");
    assert!(sweep.bytes_reclaimed() > 0, "{sweep:?}");
    assert!(sweep.objects_affected() >= before.len(), "{sweep:?}");
    let purge = ledger
        .last(ReclaimFamily::QuarantinePurge)
        .expect("purge recorded");
    assert_eq!(purge.outcome(), ReclaimOutcome::Reclaimed, "{purge:?}");
    assert!(purge.state_changes() >= before.len(), "{purge:?}");
    assert!(ledger.totals().bytes_reclaimed() >= sweep.bytes_reclaimed());
    assert!(ledger.totals().reclaimed_passes() >= 2);

    // The ledger is per runtime: a second database in the same process has
    // run nothing and reports the empty ledger.
    let other_root = temp_dir_for_api_test("maintenance-gc-end-to-end-other");
    let other_backend = crate::testkit::leak_static(StorageBackend::local_fs(other_root));
    let other = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
        other_backend,
    )
    .expect("open second durable runtime")
    .into_runtime();
    assert_eq!(
        other.reclaim_ledger_for_test(),
        Some(crate::lifecycle::ReclaimLedger::default())
    );
}

/// COW invariant: a fork child's inherited references keep shared parent objects alive through
/// the parent's compaction AND a full GC cycle — an object is deletable only when unreachable
/// from EVERY branch. The child's fork-time manifest (slice 1) is what makes its references
/// durably visible to the mark.
#[cfg(feature = "localfs")]
#[test]
fn api_cow_fork_child_pins_shared_objects_against_gc() {
    let root = temp_dir_for_api_test("maintenance-gc-cow-pin");
    let backend = crate::testkit::leak_static(StorageBackend::local_fs(root.clone()));
    let mut runtime = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
        backend,
    )
    .expect("open durable runtime")
    .into_runtime();

    // Parent data in two flushed tables, then a COW fork referencing them.
    runtime
        .commit(&put_batch(b"cow-pin-a", b"pre-fork"))
        .expect("commit");
    runtime
        .flush_default_branch_for_test()
        .expect("flush first parent table");
    runtime
        .commit(&put_batch(b"cow-pin-b", b"pre-fork"))
        .expect("commit");
    runtime
        .flush_default_branch_for_test()
        .expect("flush second parent table");
    let fork_time_objects = table_data_object_files(&root);
    let child = branch_with(0x77);
    fork_branch(&mut runtime, child);

    // Post-fork parent churn + compaction: the fork-time tables leave the PARENT's manifest,
    // but the child's manifest still references them.
    runtime
        .commit(&put_batch(b"cow-pin-a", b"post-fork"))
        .expect("commit");
    runtime
        .flush_default_branch_for_test()
        .expect("flush post-fork parent table");
    let compact =
        MaintenanceRequest::new(MaintenanceTask::Compact, MaintenanceScope::Branch(branch()));
    runtime.maintenance(&compact).expect("compact parent");
    drain_maintenance_to_idle(&mut runtime);

    let after = table_data_object_files(&root);
    for shared in &fork_time_objects {
        assert!(
            after.contains(shared),
            "object {shared} is reachable from the fork child and must survive the parent's \
             compaction + GC",
        );
    }
    // The child reads pre-fork values through the retained shared objects.
    let child_value = runtime
        .read_point(&PointReadRequest::new(
            child,
            engine_space(),
            api_key(b"cow-pin-a"),
            ReadBound::Latest,
        ))
        .expect("child read after gc")
        .row()
        .expect("child row present")
        .value()
        .expect("put row")
        .as_bytes()
        .to_vec();
    assert_eq!(child_value, b"pre-fork");
}

/// The same mark → sweep → purge chain driven by the BACKGROUND drain rounds
/// (BS5.5): the sweep's per-object staging and the purge's deletes run OFF the
/// runtime lock through `SweepStage` / `PurgeStage` steps on worker threads.
/// The inline `drain_maintenance` variant above cannot reach that path.
#[cfg(feature = "localfs")]
#[test]
fn api_background_gc_reclaims_superseded_table_objects_off_lock() {
    let root = temp_dir_for_api_test("maintenance-gc-background-off-lock");
    let backend = crate::testkit::leak_static(StorageBackend::local_fs(root.clone()));
    let mut runtime = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
        backend,
    )
    .expect("open durable runtime")
    .into_runtime();

    runtime.commit(&put_batch(b"gc-a", b"one")).expect("commit");
    runtime
        .flush_default_branch_for_test()
        .expect("flush first L0 table");
    runtime.commit(&put_batch(b"gc-a", b"two")).expect("commit");
    runtime.commit(&put_batch(b"gc-b", b"x")).expect("commit");
    runtime
        .flush_default_branch_for_test()
        .expect("flush second L0 table");
    let before = table_data_object_files(&root);
    assert!(
        before.len() >= 2,
        "two flushes must produce at least two data objects, got {}",
        before.len()
    );

    let compact =
        MaintenanceRequest::new(MaintenanceTask::Compact, MaintenanceScope::Branch(branch()));
    runtime.maintenance(&compact).expect("compact");
    // Let the BACKGROUND workers run the chain: compaction publish enqueues the
    // mark, the mark chains the sweep (off-lock staging), the sweep chains the
    // purge (off-lock deletes). Each link needs a wake, and a quiescent runtime
    // only wakes on commits — so nudge with unrelated commits, exactly the way
    // a live workload drives the chain.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    let mut nudge = 0u64;
    loop {
        runtime
            .commit(&put_batch(format!("gc-nudge-{nudge}").as_bytes(), b"n"))
            .expect("nudge commit");
        nudge += 1;
        runtime.wait_background_idle_for_test();
        let after = table_data_object_files(&root);
        if before.iter().all(|superseded| !after.contains(superseded)) {
            assert!(
                !after.is_empty(),
                "the compaction output object must survive the sweep",
            );
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "background GC did not reclaim superseded objects: before={before:?} after={after:?}",
        );
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    let value = runtime
        .read_point(&PointReadRequest::new(
            branch(),
            engine_space(),
            api_key(b"gc-a"),
            ReadBound::Latest,
        ))
        .expect("read after background gc")
        .row()
        .and_then(|row| row.value().map(|value| value.as_bytes().to_vec()));
    assert_eq!(
        value,
        Some(b"two".to_vec()),
        "reads must stay correct after off-lock reclaim",
    );
    // The off-lock sweep (`finish_quarantine_sweep`) reports the bytes it
    // staged, like the inline runner: the ledger's byte total exceeds what the
    // purge alone reported, so the sweep contributed bytes even if a later
    // empty sweep pass overwrote its slot.
    let ledger = runtime
        .reclaim_ledger_for_test()
        .expect("durable runtime has a reclaim ledger");
    assert!(
        ledger.last(ReclaimFamily::TableObjectSweep).is_some(),
        "{ledger:?}"
    );
    let purge = ledger
        .last(ReclaimFamily::QuarantinePurge)
        .expect("purge recorded");
    assert_eq!(purge.outcome(), ReclaimOutcome::Reclaimed, "{purge:?}");
    let sweep = ledger
        .last(ReclaimFamily::TableObjectSweep)
        .expect("sweep recorded");
    assert!(
        sweep.bytes_reclaimed() > 0,
        "the off-lock sweep must report staged bytes: {ledger:?}"
    );
    // #3619: the staged bytes are released once, by the purge — the total
    // counts them once.
    assert_eq!(
        ledger.totals().bytes_reclaimed(),
        purge.bytes_reclaimed(),
        "{ledger:?}"
    );
    // #3619: the off-lock sweep's staged objects left the durable table
    // catalogue, so the Live tier counts exactly the table objects on disk.
    let live = runtime
        .diagnostics(DiagnosticsRequest::new(DiagnosticsScope::Global))
        .expect("live diagnostics")
        .footprint()
        .live_table_objects();
    assert_eq!(live, Some(table_data_object_files(&root).len()));
    runtime.close().expect("close durable runtime");
}

/// #3047 diagnostic: when a branch scan's lazy block read fails (its durable table object is
/// gone underneath the still-referenced reader), the error MUST preserve the underlying
/// table-runtime cause (`failed_precondition.branch.table_runtime`, source carried) rather than
/// discarding it into the opaque `failed_precondition.branch.state`. Regression for the erased
/// cause that made the #3046 fault-injection flake undiagnosable.
#[cfg(feature = "localfs")]
#[test]
fn scan_read_failure_preserves_table_runtime_cause() {
    let root = temp_dir_for_api_test("scan-read-failure-cause");
    let backend = crate::testkit::leak_static(StorageBackend::local_fs(root.clone()));
    let mut runtime = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
        backend,
    )
    .expect("open durable runtime")
    .into_runtime();

    runtime
        .commit(&put_batch(b"cause-a", b"one"))
        .expect("commit");
    runtime
        .flush_default_branch_for_test()
        .expect("flush the row into an L0 data object");
    let objects = table_data_object_files(&root);
    assert!(!objects.is_empty(), "the flush produced a data object");
    // The durable L0 reader is lazy (BS4.4j): it block-reads its object on demand. Remove the
    // object file under the still-installed reader and drop the block cache so the next scan is
    // forced to read the now-missing object from the backend — the exact effect of the
    // reader-vs-sweep race in #3047, reproduced deterministically.
    for object in &objects {
        std::fs::remove_file(object).expect("delete the data object file");
    }
    runtime.clear_block_cache_for_test();

    let error = runtime
        .scan_prefix(&PrefixScanReadRequest::new(
            branch(),
            engine_space(),
            api_key(b"cause-"),
            ReadBound::Latest,
            None,
        ))
        .expect_err("the scan must fail once its object is gone");
    // The API surface is stable: both the old and fixed paths classify a failed table read as an
    // internal storage error (a vanished object is never a caller precondition).
    assert_eq!(error.class(), StorageApiErrorClass::Internal);
    assert_eq!(error.code(), "internal.storage_api.branch");
    // The fix: the branch scan error PRESERVES its underlying table-read cause instead of erasing
    // it into the opaque `branch scan cursor seek failed`. Without the fix the branch-layer cause
    // terminates the source chain; with it, the chain continues down to the missing-object backend
    // failure — the difference that turns an undiagnosable flake into a self-explaining error.
    let branch_cause =
        std::error::Error::source(&error).expect("the storage error wraps a branch-layer cause");
    assert!(
        std::error::Error::source(branch_cause).is_some(),
        "the branch scan read failure must preserve its underlying table-runtime cause, not \
         dead-end at an opaque branch-state error",
    );
    runtime.close().expect("close durable runtime");
}

/// Reader interlock: the sweep defers while a retired read view is still held (durable readers
/// are name-addressed with no held fd — deleting a superseded source object would break their
/// block fetches), and proceeds once the reader drops it.
#[cfg(feature = "localfs")]
#[test]
fn api_gc_sweep_defers_while_retired_read_view_is_held() {
    let root = temp_dir_for_api_test("maintenance-gc-reader-interlock");
    let backend = crate::testkit::leak_static(StorageBackend::local_fs(root.clone()));
    let mut runtime = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
        backend,
    )
    .expect("open durable runtime")
    .into_runtime();

    runtime
        .commit(&put_batch(b"pin-read-a", b"one"))
        .expect("commit");
    runtime
        .flush_default_branch_for_test()
        .expect("flush first L0 table");
    runtime
        .commit(&put_batch(b"pin-read-a", b"two"))
        .expect("commit");
    runtime
        .flush_default_branch_for_test()
        .expect("flush second L0 table");
    let before = table_data_object_files(&root);

    // An off-lock reader holds the pre-compaction view across the whole GC cycle.
    let held_view = runtime
        .load_snapshot_for_test(branch())
        .expect("published snapshot");
    let compact =
        MaintenanceRequest::new(MaintenanceTask::Compact, MaintenanceScope::Branch(branch()));
    runtime.maintenance(&compact).expect("compact");
    drain_maintenance_to_idle(&mut runtime);
    let with_reader = table_data_object_files(&root);
    for superseded in &before {
        assert!(
            with_reader.contains(superseded),
            "object {superseded} may still be read through the retired view and must survive \
             the sweep",
        );
    }
    // The ledger records the deferral with its typed reason, never prose.
    let deferred = runtime
        .reclaim_ledger_for_test()
        .expect("durable runtime has a reclaim ledger")
        .last(ReclaimFamily::TableObjectSweep)
        .expect("deferred sweep recorded");
    assert_eq!(deferred.outcome(), ReclaimOutcome::Deferred, "{deferred:?}");
    assert_eq!(
        deferred.deferral(),
        Some(MaintenanceDeferralReason::ReaderPinned),
        "{deferred:?}"
    );

    // Reader done: the next cycle reclaims.
    drop(held_view);
    let reclaim =
        MaintenanceRequest::new(MaintenanceTask::Reclaim, MaintenanceScope::Branch(branch()));
    runtime.maintenance(&reclaim).expect("reclaim");
    drain_maintenance_to_idle(&mut runtime);
    let after = table_data_object_files(&root);
    for superseded in &before {
        assert!(
            !after.contains(superseded),
            "superseded object {superseded} must be reclaimed once the retired view is dropped",
        );
    }
    let ledger = runtime
        .reclaim_ledger_for_test()
        .expect("durable runtime has a reclaim ledger");
    let sweep = ledger
        .last(ReclaimFamily::TableObjectSweep)
        .expect("sweep recorded");
    assert_eq!(sweep.outcome(), ReclaimOutcome::Reclaimed, "{sweep:?}");
    assert_eq!(sweep.deferral(), None);
    assert_eq!(ledger.totals().deferred_passes(), 1, "{ledger:?}");
}

/// Reopen reconcile: stale objects left by a prior session (here: a planted orphan simulating a
/// crash between object write and manifest publish, or a pre-GC session's leak) are reclaimed by
/// the post-recovery mark without any explicit API call.
#[cfg(feature = "localfs")]
#[test]
fn api_reopen_reconciles_stale_table_objects() {
    let root = temp_dir_for_api_test("maintenance-gc-reopen-reconcile");
    let orphan_path;
    {
        let backend = crate::testkit::leak_static(StorageBackend::local_fs(root.clone()));
        let mut runtime = StorageRuntime::open_with_backend(
            StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
            backend,
        )
        .expect("open durable runtime")
        .into_runtime();
        runtime
            .commit(&put_batch(b"reconcile-a", b"live"))
            .expect("commit");
        runtime
            .flush_default_branch_for_test()
            .expect("flush live table");
        runtime.close().expect("close");

        // Plant an orphan data object: a byte-copy of the live table under a fresh identity —
        // inventory-listed, valid shape, referenced by no manifest.
        let live = table_data_object_files(&root);
        let source = live.iter().next().expect("one live object").clone();
        let source_path = std::path::PathBuf::from(&source);
        orphan_path = source_path
            .with_file_name("00000000000000000000000000009999.object@")
            .display()
            .to_string();
        std::fs::copy(&source_path, &orphan_path).expect("plant orphan object");
    }

    let backend = crate::testkit::leak_static(StorageBackend::local_fs(root.clone()));
    let runtime = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
        backend,
    )
    .expect("reopen durable runtime")
    .into_runtime();
    // Space-reclamation contract §3.1 (slice 4): the open-time wake runs the
    // post-recovery mark → sweep → purge on the worker, so the reconcile needs
    // no call at all — a foreground drain here would race the worker for the
    // same chain. Wait for the worker instead.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        runtime.wait_background_idle_for_test();
        if !table_data_object_files(&root).contains(&orphan_path) {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the planted orphan must be reclaimed by the post-recovery reconcile",
        );
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    let value = runtime
        .read_point(&PointReadRequest::new(
            branch(),
            engine_space(),
            api_key(b"reconcile-a"),
            ReadBound::Latest,
        ))
        .expect("read after reconcile")
        .row()
        .expect("live row present")
        .value()
        .expect("put row")
        .as_bytes()
        .to_vec();
    assert_eq!(value, b"live");
}

/// B2: a durable runtime opened with a custom data-block byte target builds
/// flush tables at that granularity — many small blocks instead of one big
/// one — and the rows read back identically.
#[test]
#[cfg(feature = "localfs")]
fn api_flush_honors_configured_data_block_bytes() {
    let mut runtime = open_durable_runtime_with_options(
        "maintenance-data-block-bytes",
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
            .with_data_block_bytes(4 * 1024),
    );
    // ~64KB of rows: one block at the 64KB default, ~16 blocks at 4KB.
    let payload = vec![0x42u8; 2 * 1024];
    for index in 0..32u8 {
        let key = format!("block-bytes-{index:02}");
        runtime
            .commit(&put_batch(key.as_bytes(), &payload))
            .expect("commit row");
    }
    let request =
        MaintenanceRequest::new(MaintenanceTask::Flush, MaintenanceScope::Branch(branch()));
    let outcome = runtime.maintenance(&request).expect("flush outcome");
    assert_eq!(outcome.status(), MaintenanceSummaryStatus::Completed);

    for index in 0..32u8 {
        let key = format!("block-bytes-{index:02}");
        let outcome = runtime
            .read_point(&PointReadRequest::new(
                branch(),
                engine_space(),
                api_key(key.as_bytes()),
                ReadBound::Latest,
            ))
            .expect("read flushed row");
        let row = outcome.row().expect("row present");
        assert_eq!(row.value().expect("value").as_bytes(), payload.as_slice());
    }
}

#[test]
#[cfg(feature = "localfs")]
fn wal_growth_backpressure_paces_by_driving_further_policy_evaluations() {
    use std::time::Duration;

    let mut runtime = open_durable_runtime_with_options(
        "wal-growth-backpressure-paces",
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
            .with_wal_growth_policy(StorageWalGrowthPolicy::thresholds(1, usize::MAX, u64::MAX)),
    );
    assert!(runtime.set_background_block_wait_for_test(
        Duration::from_millis(5),
        Duration::from_millis(200),
        1,
    ));

    // One commit crosses both the 1-byte trigger and the 16x backpressure
    // bound, so its own evaluation enqueues the four-task growth bundle and
    // the post-commit pacing loop must run at least one iteration — each
    // iteration re-evaluates the policy, which enqueues or coalesces another
    // bundle. The counters are cumulative, so the assertion is race-free
    // against background workers draining the queue.
    runtime
        .commit(&put_batch(b"growth-pacing", &[0x42; 256]))
        .expect("commit over backpressure");
    let status = runtime.maintenance_status().expect("maintenance status");
    assert!(
        status.enqueued() + status.coalesced() >= 8,
        "the pacing loop never re-evaluated the growth policy: {status:?}"
    );
}

#[test]
fn wal_growth_pacing_waits_only_on_evaluations_that_enqueued_maintenance() {
    use crate::api::runtime::wal_growth_pacing_applies;
    use crate::lifecycle::LifecycleWalGrowthStatus;

    assert!(wal_growth_pacing_applies(
        LifecycleWalGrowthStatus::MaintenanceEnqueued
    ));
    assert!(wal_growth_pacing_applies(
        LifecycleWalGrowthStatus::MaintenanceCoalesced
    ));
    // A deferred evaluation enqueued nothing; pacing the writer would wait on
    // relief the deferred task class cannot deliver.
    assert!(!wal_growth_pacing_applies(
        LifecycleWalGrowthStatus::Deferred
    ));
    assert!(!wal_growth_pacing_applies(
        LifecycleWalGrowthStatus::Disabled
    ));
    assert!(!wal_growth_pacing_applies(
        LifecycleWalGrowthStatus::BelowThreshold
    ));
    assert!(!wal_growth_pacing_applies(
        LifecycleWalGrowthStatus::NoDurableAction
    ));
}

/// Sum of the on-disk table DATA object bytes (`tables/<branch>/l*/`) — the
/// compressed block footprint, manifests excluded.
#[cfg(feature = "localfs")]
fn table_data_object_bytes(root: &std::path::Path) -> u64 {
    table_data_object_files(root)
        .iter()
        .filter_map(|path| std::fs::metadata(path).ok())
        .map(|meta| meta.len())
        .sum()
}

/// #3499 end-to-end compression observation: a Zstd-configured durable open
/// writes materially smaller table blocks than an Uncompressed one for a highly
/// compressible payload. Reads succeed under either codec (the block frame is
/// self-describing), so only an on-disk size assertion proves the codec
/// actually reached the table builder — this is the test that fails if any link
/// in options → lifecycle config → builder → flush stops threading it. It runs
/// at default features so the per-diff mutation gate (lane A) exercises it.
#[cfg(feature = "localfs")]
#[test]
fn zstd_flush_shrinks_on_disk_tables_versus_uncompressed() {
    let payload = vec![b'a'; 4096];

    let flush_and_measure = |name: &str, compression: crate::format::TableCompression| -> u64 {
        let root = temp_dir_for_api_test(name);
        let backend = crate::testkit::leak_static(StorageBackend::local_fs(root.clone()));
        let mut runtime = StorageRuntime::open_with_backend(
            StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
                .with_table_compression_for_test(compression),
            backend,
        )
        .expect("open durable runtime")
        .into_runtime();
        for index in 0..256u32 {
            let key = format!("z-{index:08}");
            runtime
                .commit(&put_batch(key.as_bytes(), &payload))
                .expect("commit compressible row");
        }
        runtime
            .flush_default_branch_for_test()
            .expect("flush L0 table");
        table_data_object_bytes(&root)
    };

    let zstd_bytes = flush_and_measure("compress-zstd", crate::format::TableCompression::Zstd);
    let plain_bytes = flush_and_measure(
        "compress-plain",
        crate::format::TableCompression::Uncompressed,
    );

    assert!(
        plain_bytes > 0 && zstd_bytes > 0,
        "both flushes must produce L0 tables (zstd={zstd_bytes} B, plain={plain_bytes} B)"
    );
    // ~1 MiB of a single repeated byte compresses to a tiny fraction. Half the
    // uncompressed footprint is a deliberately loose ceiling — the real ratio is
    // far better — so allocator/framing overhead can never flake it.
    assert!(
        zstd_bytes * 2 < plain_bytes,
        "Zstd table footprint ({zstd_bytes} B) must be under half the uncompressed \
         footprint ({plain_bytes} B) for a highly compressible payload — the \
         compression codec is not reaching the table builder"
    );
}

/// The BACKGROUND checkpoint path (off-lock build, under-lock publish) takes
/// the same delta-cap deferral: the publish step chains a flush and a retried
/// checkpoint, and the worker completes the retry with a bounded delta — the
/// snapshot the runtime could not publish in one go still lands.
#[cfg(feature = "localfs")]
#[test]
fn api_background_checkpoint_over_cap_flushes_first_then_completes() {
    let root = temp_dir_for_api_test("maintenance-checkpoint-delta-cap-background");
    let backend = crate::testkit::leak_static(StorageBackend::local_fs(root));
    let runtime = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
        backend,
    )
    .expect("open durable runtime")
    .into_runtime();
    runtime.set_checkpoint_delta_cap_for_test(2048);
    for index in 0u8..3 {
        runtime
            .commit(&background_put_batch(
                format!("delta-cap-{index}").as_bytes(),
                vec![index; 1024],
            ))
            .expect("commit");
    }
    let before = runtime
        .diagnostics(DiagnosticsRequest::new(DiagnosticsScope::Global))
        .expect("diagnostics before");
    assert_eq!(before.checkpoint().snapshot_id(), None);

    runtime
        .enqueue_maintenance(&MaintenanceRequest::new(
            MaintenanceTask::Checkpoint,
            MaintenanceScope::Global,
        ))
        .expect("enqueue background checkpoint");
    runtime.wait_background_idle_for_test();

    let after = runtime
        .diagnostics(DiagnosticsRequest::new(DiagnosticsScope::Global))
        .expect("diagnostics after");
    assert!(
        after.checkpoint().snapshot_id().is_some(),
        "the chained flush + retried checkpoint must publish: {:?}",
        after.checkpoint()
    );
    // The publish alone does not prove the delta-cap path ran: a 3 KiB delta
    // fits the real 64 MiB ceiling, so an unwired cap would publish it in one
    // pass. Two facts only the deferral produces: the checkpoint deferred at
    // least once, and the chained flush drained the memtable into a durable
    // owned table (a plain checkpoint snapshots the delta without flushing).
    let status = runtime.maintenance_status().expect("maintenance status");
    assert!(
        status.deferred() >= 1,
        "the over-cap checkpoint must defer before the retry: {status:?}"
    );
    let layout = runtime
        .branch_source_layout_for_test(branch())
        .expect("source layout after");
    assert!(
        layout.owned_total_tables() >= 1,
        "the chained flush must have created a durable table: {layout:?}"
    );
}

/// Space-reclamation contract §3.2 (slice 11): the background off-lock
/// checkpoint build records the durable-base branch set too — the flushed
/// default branch is the one member of the published snapshot's section.
#[cfg(feature = "localfs")]
#[test]
fn api_background_checkpoint_records_durable_base_branches() {
    let root = temp_dir_for_api_test("maintenance-checkpoint-durable-base-set");
    let backend = crate::testkit::leak_static(StorageBackend::local_fs(root.clone()));
    let mut runtime = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
        backend,
    )
    .expect("open durable runtime")
    .into_runtime();
    runtime
        .commit(&put_batch(b"durable-base", b"value"))
        .expect("commit");
    runtime
        .flush_default_branch_for_test()
        .expect("flush gives the default branch a durable base");
    runtime
        .commit(&put_batch(b"delta-tail", b"value"))
        .expect("commit tail");
    let checkpoint = MaintenanceRequest::new(
        MaintenanceTask::Checkpoint,
        MaintenanceScope::Branch(branch()),
    );
    runtime.maintenance(&checkpoint).expect("checkpoint");
    drain_maintenance_to_idle(&mut runtime);
    runtime.close().expect("close");

    let snapshots = root.join("snapshots");
    let mut names: Vec<String> = std::fs::read_dir(&snapshots)
        .expect("snapshots dir")
        .map(|entry| {
            entry
                .expect("entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    names.sort();
    let newest = names.last().expect("a published snapshot");
    let bytes = std::fs::read(snapshots.join(newest)).expect("snapshot bytes");
    let container = crate::format::decode_snapshot_container(&bytes).expect("snapshot container");
    let recorded: Vec<Vec<strata_core::BranchId>> = container
        .sections()
        .iter()
        .filter(|section| {
            section.section_kind() == crate::format::SNAPSHOT_FLUSHED_BRANCHES_SECTION_KIND
        })
        .map(|section| {
            crate::format::decode_snapshot_flushed_branches_payload(section.payload())
                .expect("durable-base set")
        })
        .collect();
    assert_eq!(recorded, vec![vec![branch()]]);
}

/// Space-reclamation contract §3.1 (slice 4): leave a prior session's reclaim
/// debt on disk. Two flushed L0 objects are superseded by a compaction whose
/// mark → sweep → purge chain never runs (the close's own reclaim drain is
/// disabled, so the queued mark is cancelled), and one unflushed tail row is committed so
/// a later flush would be observable as a new table object. Returns the root,
/// the backend, the superseded objects, and the full on-disk set at close.
#[cfg(feature = "localfs")]
fn plant_reclaim_debt_and_close(
    name: &str,
) -> (
    std::path::PathBuf,
    &'static StorageBackend,
    std::collections::BTreeSet<String>,
    std::collections::BTreeSet<String>,
) {
    let root = temp_dir_for_api_test(name);
    let backend = crate::testkit::leak_static(StorageBackend::local_fs(root.clone()));
    // No worker: every maintenance step below is driven by hand, so the
    // chain stops exactly where this helper stops it.
    let options = StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
        .with_maintenance_scheduling_policy(StorageMaintenanceSchedulingPolicy::EvaluateAndEnqueue);
    let mut runtime = StorageRuntime::open_with_backend(options, backend)
        .expect("open durable runtime")
        .into_runtime();
    runtime.commit(&put_batch(b"gc-a", b"one")).expect("commit");
    runtime
        .flush_default_branch_for_test()
        .expect("flush first L0 table");
    runtime.commit(&put_batch(b"gc-a", b"two")).expect("commit");
    runtime.commit(&put_batch(b"gc-b", b"x")).expect("commit");
    runtime
        .flush_default_branch_for_test()
        .expect("flush second L0 table");
    let superseded = table_data_object_files(&root);
    assert!(superseded.len() >= 2, "two flushes: {superseded:?}");

    let compact =
        MaintenanceRequest::new(MaintenanceTask::Compact, MaintenanceScope::Branch(branch()));
    // The compaction request runs the compaction itself; its publish queues the
    // reclaim mark, which nothing below runs (the close drain is disabled, so
    // the close cancels it and the debt is left for the reopen).
    let compacted = runtime.maintenance(&compact).expect("compact");
    assert_eq!(compacted.task(), MaintenanceTask::Compact);
    let at_close = table_data_object_files(&root);
    assert!(
        superseded.iter().all(|object| at_close.contains(object)),
        "the superseded objects are still on disk: {at_close:?}"
    );
    assert!(
        at_close.len() > superseded.len(),
        "the compaction output object exists"
    );
    runtime
        .commit(&put_batch(b"tail", b"unflushed"))
        .expect("commit the unflushed tail row");
    runtime
        .close_with_options(
            StorageCloseOptions::graceful().with_reclaim_budget(ReclaimBudget::Disabled),
        )
        .expect("close");
    assert_eq!(
        table_data_object_files(&root),
        at_close,
        "with the close drain disabled the debt survives for the reopen"
    );
    (root, backend, superseded, at_close)
}

#[cfg(feature = "localfs")]
fn read_value(runtime: &StorageRuntime<'static>, key: &[u8]) -> Option<Vec<u8>> {
    runtime
        .read_point(&PointReadRequest::new(
            branch(),
            engine_space(),
            api_key(key),
            ReadBound::Latest,
        ))
        .expect("read")
        .row()
        .and_then(|row| row.value().map(|value| value.as_bytes().to_vec()))
}

/// The open-time wake alone — no commit, no explicit maintenance — drives the
/// reopened session's mark → sweep → purge on the real worker, and nothing
/// else runs: the on-disk set shrinks by exactly the superseded objects.
#[cfg(feature = "localfs")]
#[test]
fn api_open_existing_wakes_the_worker_and_reclaims_prior_debt() {
    let (root, backend, superseded, at_close) =
        plant_reclaim_debt_and_close("maintenance-open-wake-reclaims");
    let expected: std::collections::BTreeSet<String> =
        at_close.difference(&superseded).cloned().collect();
    let mut runtime = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
        backend,
    )
    .expect("reopen durable runtime")
    .into_runtime();

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        runtime.wait_background_idle_for_test();
        let now = table_data_object_files(&root);
        if superseded.iter().all(|object| !now.contains(object)) {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the open-time wake did not reclaim the prior session's debt: {now:?}"
        );
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    assert_eq!(
        table_data_object_files(&root),
        expected,
        "only the superseded objects went; a read-only session flushes nothing"
    );
    assert_eq!(
        runtime.reclaim_only_scope_for_test(),
        Some(crate::lifecycle::ReclaimOnlyScope::Active),
        "reclaim does not end the reclaim-only scope"
    );
    assert_eq!(read_value(&runtime, b"gc-a"), Some(b"two".to_vec()));
    assert_eq!(read_value(&runtime, b"tail"), Some(b"unflushed".to_vec()));
    // Slice 8: the open wake is a ledger fact.
    let ledger = runtime
        .reclaim_ledger_for_test()
        .expect("durable runtime has a reclaim ledger");
    assert!(ledger.last_open_wake().is_some(), "{ledger:?}");
    assert_eq!(ledger.open_wakes(), 1, "{ledger:?}");
    runtime.close().expect("close");
}

/// The same reclaim under the deterministic inline executor: a read-only
/// session reclaims on its first progress wait, with no commit ever issued.
#[cfg(feature = "localfs")]
#[test]
fn api_read_only_reopen_reclaims_deterministically() {
    let (root, backend, superseded, at_close) =
        plant_reclaim_debt_and_close("maintenance-open-wake-inline");
    let expected: std::collections::BTreeSet<String> =
        at_close.difference(&superseded).cloned().collect();
    let options = StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
        .with_maintenance_scheduling_policy(
            StorageMaintenanceSchedulingPolicy::DeterministicInline,
        );
    let mut runtime = StorageRuntime::open_with_backend(options, backend)
        .expect("reopen durable runtime")
        .into_runtime();
    assert_eq!(read_value(&runtime, b"gc-a"), Some(b"two".to_vec()));
    runtime.wait_background_idle_for_test();
    assert_eq!(table_data_object_files(&root), expected);
    assert_eq!(
        runtime.reclaim_only_scope_for_test(),
        Some(crate::lifecycle::ReclaimOnlyScope::Active)
    );
    runtime.close().expect("close");
}

/// #3626: quarantine a prior session left behind — swept, never purged (a
/// crash after the sweep, a close whose reclaim budget ran out between sweep
/// and purge, or a rejected purge enqueue) — is purged by the reopened
/// session with no writes. The mark
/// finds nothing unreferenced (the objects are already quarantined) and the
/// sweep quarantines nothing, so the purge must be chained on the quarantine
/// inventory, not on this pass's count.
#[cfg(feature = "localfs")]
#[test]
fn api_reopen_purges_quarantine_a_prior_session_left_unpurged() {
    let root = temp_dir_for_api_test("maintenance-stranded-quarantine");
    let backend = crate::testkit::leak_static(StorageBackend::local_fs(root.clone()));
    let manual = StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
        .with_maintenance_scheduling_policy(StorageMaintenanceSchedulingPolicy::EvaluateAndEnqueue);
    let mut runtime = StorageRuntime::open_with_backend(manual, backend)
        .expect("open durable runtime")
        .into_runtime();
    runtime.commit(&put_batch(b"gc-a", b"one")).expect("commit");
    runtime
        .flush_default_branch_for_test()
        .expect("flush first L0 table");
    runtime.commit(&put_batch(b"gc-a", b"two")).expect("commit");
    runtime
        .flush_default_branch_for_test()
        .expect("flush second L0 table");
    let superseded = table_data_object_files(&root);
    assert!(superseded.len() >= 2, "two flushes: {superseded:?}");
    runtime
        .maintenance(&MaintenanceRequest::new(
            MaintenanceTask::Compact,
            MaintenanceScope::Branch(branch()),
        ))
        .expect("compact");
    // Mark, then sweep: the superseded inputs move to quarantine and the sweep
    // queues its purge, which nothing runs.
    runtime
        .maintenance(&MaintenanceRequest::new(
            MaintenanceTask::Reclaim,
            MaintenanceScope::Branch(branch()),
        ))
        .expect("mark");
    runtime
        .maintenance(&MaintenanceRequest::new(
            MaintenanceTask::Quarantine,
            MaintenanceScope::Global,
        ))
        .expect("sweep");
    let quarantined = |runtime: &StorageRuntime<'_>| {
        runtime
            .diagnostics(
                DiagnosticsRequest::new(DiagnosticsScope::Global)
                    .with_detail(DiagnosticsDetail::Audit),
            )
            .expect("audit")
            .quarantine()
            .quarantined_objects()
    };
    assert!(
        quarantined(&runtime).is_some_and(|count| count >= 2),
        "the sweep quarantined the superseded inputs: {:?}",
        quarantined(&runtime)
    );
    // The session ends before the queued purge runs (a crash here; a clean
    // close whose reclaim budget expires between sweep and purge leaves the
    // same state). The quarantine and its record are durable.
    drop(runtime);

    let inline = StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
        .with_maintenance_scheduling_policy(
            StorageMaintenanceSchedulingPolicy::DeterministicInline,
        );
    let mut runtime = StorageRuntime::open_with_backend(inline, backend)
        .expect("reopen durable runtime")
        .into_runtime();
    runtime.wait_background_idle_for_test();
    assert_eq!(
        quarantined(&runtime),
        Some(0),
        "the reopened session purges the stranded quarantine"
    );
    assert_eq!(
        runtime
            .maintenance_status()
            .expect("status")
            .pending_tasks(),
        0
    );
    let ledger = runtime.reclaim_ledger_for_test().expect("ledger");
    let purge = ledger
        .last(ReclaimFamily::QuarantinePurge)
        .expect("the reopen ran a purge");
    assert_eq!(purge.outcome(), ReclaimOutcome::Reclaimed, "{ledger:?}");
    assert!(
        superseded
            .iter()
            .all(|object| !table_data_object_files(&root).contains(object)),
        "the superseded inputs are gone"
    );
    runtime.close().expect("close");
}

/// Direction control for #3626: a reopen with an empty quarantine queues no
/// purge — the ledger records none.
#[cfg(feature = "localfs")]
#[test]
fn api_reopen_with_an_empty_quarantine_queues_no_purge() {
    let root = temp_dir_for_api_test("maintenance-empty-quarantine-reopen");
    let backend = crate::testkit::leak_static(StorageBackend::local_fs(root));
    let inline = StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
        .with_maintenance_scheduling_policy(
            StorageMaintenanceSchedulingPolicy::DeterministicInline,
        );
    let mut runtime = StorageRuntime::open_with_backend(inline, backend)
        .expect("open durable runtime")
        .into_runtime();
    runtime.commit(&put_batch(b"k", b"v")).expect("commit");
    runtime.close().expect("close");
    let mut runtime = StorageRuntime::open_with_backend(inline, backend)
        .expect("reopen durable runtime")
        .into_runtime();
    runtime.wait_background_idle_for_test();
    let ledger = runtime.reclaim_ledger_for_test().expect("ledger");
    assert!(ledger.last_open_wake().is_some(), "{ledger:?}");
    assert_eq!(
        ledger.last(ReclaimFamily::QuarantinePurge),
        None,
        "no purge for an empty quarantine: {ledger:?}"
    );
    runtime.close().expect("close");
}

/// Direction control for #3626 on the background worker, whose sweep takes
/// the off-lock path (`finish_quarantine_sweep`): a reopen whose sweep
/// quarantines nothing queues no purge.
#[cfg(feature = "localfs")]
#[test]
fn api_background_sweep_that_quarantines_nothing_queues_no_purge() {
    let root = temp_dir_for_api_test("maintenance-empty-quarantine-background");
    let backend = crate::testkit::leak_static(StorageBackend::local_fs(root));
    let background = StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
        .with_maintenance_scheduling_policy(StorageMaintenanceSchedulingPolicy::Background);
    let mut runtime = StorageRuntime::open_with_backend(background, backend)
        .expect("open durable runtime")
        .into_runtime();
    runtime.commit(&put_batch(b"k", b"v")).expect("commit");
    runtime.close().expect("close");
    let mut runtime = StorageRuntime::open_with_backend(background, backend)
        .expect("reopen durable runtime")
        .into_runtime();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    let ledger = loop {
        runtime.wait_background_idle_for_test();
        let ledger = runtime.reclaim_ledger_for_test().expect("ledger");
        if ledger.last(ReclaimFamily::TableObjectSweep).is_some()
            || std::time::Instant::now() >= deadline
        {
            break ledger;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    };
    assert!(
        ledger.last(ReclaimFamily::TableObjectSweep).is_some(),
        "the reopen's sweep ran on the worker: {ledger:?}"
    );
    assert_eq!(
        ledger.last(ReclaimFamily::QuarantinePurge),
        None,
        "no purge for a sweep that quarantined nothing: {ledger:?}"
    );
    runtime.close().expect("close");
}

/// #3619 on the foreground sweep verb (the inline sweep runner): the objects it
/// moves to quarantine leave the durable table catalogue, so the Live tier
/// counts exactly the table objects still on disk.
#[cfg(feature = "localfs")]
#[test]
fn api_foreground_sweep_drops_staged_objects_from_the_live_tier() {
    let root = temp_dir_for_api_test("maintenance-foreground-sweep-live-tier");
    let backend = crate::testkit::leak_static(StorageBackend::local_fs(root.clone()));
    let manual = StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
        .with_maintenance_scheduling_policy(StorageMaintenanceSchedulingPolicy::EvaluateAndEnqueue);
    let mut runtime = StorageRuntime::open_with_backend(manual, backend)
        .expect("open durable runtime")
        .into_runtime();
    runtime.commit(&put_batch(b"gc-a", b"one")).expect("commit");
    runtime
        .flush_default_branch_for_test()
        .expect("flush first L0 table");
    runtime.commit(&put_batch(b"gc-a", b"two")).expect("commit");
    runtime
        .flush_default_branch_for_test()
        .expect("flush second L0 table");
    runtime
        .maintenance(&MaintenanceRequest::new(
            MaintenanceTask::Compact,
            MaintenanceScope::Branch(branch()),
        ))
        .expect("compact");
    runtime
        .maintenance(&MaintenanceRequest::new(
            MaintenanceTask::Reclaim,
            MaintenanceScope::Branch(branch()),
        ))
        .expect("mark");
    let live = |runtime: &StorageRuntime<'_>| {
        runtime
            .diagnostics(DiagnosticsRequest::new(DiagnosticsScope::Global))
            .expect("live diagnostics")
            .footprint()
            .live_table_objects()
    };
    assert_eq!(
        live(&runtime),
        Some(3),
        "both inputs and the output before the sweep"
    );
    runtime
        .maintenance(&MaintenanceRequest::new(
            MaintenanceTask::Quarantine,
            MaintenanceScope::Global,
        ))
        .expect("sweep");
    assert_eq!(
        table_data_object_files(&root).len(),
        1,
        "the sweep moved both inputs"
    );
    assert_eq!(
        live(&runtime),
        Some(1),
        "the Live tier counts only what is on disk"
    );
    runtime.close().expect("close");
}

/// DUR-018: `open` itself reclaims nothing. Without a worker the wake is a
/// no-op, so the debt is intact right after open — and still reclaimable by an
/// explicit foreground drain, proving bootstrap queued the real mark.
#[cfg(feature = "localfs")]
#[test]
fn api_open_reclaims_nothing_inline_without_a_worker() {
    let (root, backend, superseded, at_close) =
        plant_reclaim_debt_and_close("maintenance-open-wake-no-worker");
    let expected: std::collections::BTreeSet<String> =
        at_close.difference(&superseded).cloned().collect();
    let options = StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
        .with_maintenance_scheduling_policy(StorageMaintenanceSchedulingPolicy::EvaluateAndEnqueue);
    let mut runtime = StorageRuntime::open_with_backend(options, backend)
        .expect("reopen durable runtime")
        .into_runtime();
    assert_eq!(
        table_data_object_files(&root),
        at_close,
        "open must not reclaim on the caller's thread"
    );
    assert_eq!(
        runtime.reclaim_only_scope_for_test(),
        Some(crate::lifecycle::ReclaimOnlyScope::Active)
    );
    drain_maintenance_to_idle(&mut runtime);
    assert_eq!(table_data_object_files(&root), expected);
    runtime.close().expect("close");
}

/// A created database has no backlog: it starts outside the reclaim-only
/// scope and its wake-less open leaves the queue empty. A reopened one stays
/// in the scope through reclaim and leaves it only on its first commit.
#[cfg(feature = "localfs")]
#[test]
fn api_created_open_starts_inactive_and_a_reopen_stays_active_until_the_first_commit() {
    let root = temp_dir_for_api_test("maintenance-open-wake-scope");
    let backend = crate::testkit::leak_static(StorageBackend::local_fs(root));
    let options = StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
        .with_maintenance_scheduling_policy(
            StorageMaintenanceSchedulingPolicy::DeterministicInline,
        );
    let mut runtime = StorageRuntime::open_with_backend(options, backend)
        .expect("create durable runtime")
        .into_runtime();
    assert_eq!(
        runtime.reclaim_only_scope_for_test(),
        Some(crate::lifecycle::ReclaimOnlyScope::Inactive)
    );
    runtime.wait_background_idle_for_test();
    assert_eq!(
        runtime
            .maintenance_status()
            .expect("status")
            .pending_tasks(),
        0,
        "a created open queues nothing"
    );
    runtime.commit(&put_batch(b"seed", b"v")).expect("commit");
    runtime.close().expect("close");

    let mut runtime = StorageRuntime::open_with_backend(options, backend)
        .expect("reopen durable runtime")
        .into_runtime();
    assert_eq!(
        runtime.reclaim_only_scope_for_test(),
        Some(crate::lifecycle::ReclaimOnlyScope::Active)
    );
    runtime.wait_background_idle_for_test();
    assert_eq!(
        runtime.reclaim_only_scope_for_test(),
        Some(crate::lifecycle::ReclaimOnlyScope::Active),
        "reclaim maintenance does not end the scope"
    );
    runtime
        .commit(&put_batch(b"first-write", b"v"))
        .expect("commit");
    assert_eq!(
        runtime.reclaim_only_scope_for_test(),
        Some(crate::lifecycle::ReclaimOnlyScope::Inactive),
        "the first applied commit ends the scope"
    );
    runtime.close().expect("close");
}

/// While the scope is active a background drain admits the reclaim tier and
/// refuses the upper tier: a queued flush stays queued (no new table object)
/// until the first commit, after which it runs.
#[cfg(feature = "localfs")]
#[test]
fn api_upper_tier_waits_for_the_first_commit_in_a_reclaim_only_session() {
    let (root, backend, superseded, at_close) =
        plant_reclaim_debt_and_close("maintenance-open-wake-upper-tier");
    let expected: std::collections::BTreeSet<String> =
        at_close.difference(&superseded).cloned().collect();
    let options = StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
        .with_maintenance_scheduling_policy(
            StorageMaintenanceSchedulingPolicy::DeterministicInline,
        );
    let mut runtime = StorageRuntime::open_with_backend(options, backend)
        .expect("reopen durable runtime")
        .into_runtime();
    // The replayed tail row sits in the active memtable; rotating it freezes a
    // table for a flush to write, so a flush becomes observable on disk.
    runtime
        .rotate_default_branch_for_test()
        .expect("rotate the replayed tail");
    // A BACKGROUND flush task (the public `maintenance` verb runs its request
    // in the foreground — explicit caller intent, which the scope never
    // gates), then a low-tier wake like the one `open` arms.
    runtime
        .enqueue_lifecycle_maintenance_for_test(crate::lifecycle::MaintenanceTaskRequest::flush(
            branch(),
        ))
        .expect("enqueue background flush");
    runtime.submit_stale_background_wake_for_test();
    runtime.wait_background_idle_for_test();
    assert_eq!(
        table_data_object_files(&root),
        expected,
        "reclaim ran, the flush did not"
    );
    assert!(
        runtime
            .maintenance_status()
            .expect("status")
            .pending_tasks()
            >= 1,
        "the refused flush stays queued"
    );

    runtime
        .commit(&put_batch(b"first-write", b"v"))
        .expect("commit");
    runtime.wait_background_idle_for_test();
    assert_eq!(
        runtime.reclaim_only_scope_for_test(),
        Some(crate::lifecycle::ReclaimOnlyScope::Inactive)
    );
    let after_commit = table_data_object_files(&root);
    assert!(
        after_commit.len() > expected.len(),
        "the flush ran once the session wrote: {after_commit:?}"
    );
    assert_eq!(read_value(&runtime, b"tail"), Some(b"unflushed".to_vec()));
    runtime.close().expect("close");
}

/// The scope ends at the first write's ADMISSION, not its apply: a reopened
/// branch under blocking L0 pressure has its first commit wait for compaction
/// progress, and a scope that only ended at apply would withhold that very
/// progress from the worker (the deadlock `recovery_budget` caught). Until a
/// write is attempted, the same session leaves the backlog alone.
#[cfg(feature = "localfs")]
#[test]
fn api_first_write_under_blocking_pressure_ends_the_reclaim_only_scope_at_admission() {
    let root = temp_dir_for_api_test("maintenance-open-wake-blocked-first-write");
    let backend = crate::testkit::leak_static(StorageBackend::local_fs(root.clone()));
    {
        // No worker while seeding, so every flush leaves its own L0 table.
        let options = StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
            .with_maintenance_scheduling_policy(
                StorageMaintenanceSchedulingPolicy::EvaluateAndEnqueue,
            );
        let mut runtime = StorageRuntime::open_with_backend(options, backend)
            .expect("open durable runtime")
            .into_runtime();
        for index in 0..crate::lifecycle::LEVEL_ZERO_BLOCKING_COMPACTION_THRESHOLD {
            runtime
                .commit(&put_batch(format!("l0-{index}").as_bytes(), b"v"))
                .expect("commit");
            runtime
                .flush_default_branch_for_test()
                .expect("flush leaves one more L0 table");
        }
        runtime.close().expect("close");
    }
    let at_close = table_data_object_files(&root);
    assert!(
        at_close.len() >= crate::lifecycle::LEVEL_ZERO_BLOCKING_COMPACTION_THRESHOLD,
        "the backlog must be at the blocking threshold: {}",
        at_close.len()
    );

    let mut runtime = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
        backend,
    )
    .expect("reopen durable runtime")
    .into_runtime();
    // Read-only so far: the open-time wake reclaims, it does not compact.
    runtime.wait_background_idle_for_test();
    assert_eq!(
        table_data_object_files(&root),
        at_close,
        "a session that has not written leaves the L0 backlog to a writer"
    );
    assert_eq!(
        runtime.reclaim_only_scope_for_test(),
        Some(crate::lifecycle::ReclaimOnlyScope::Active)
    );

    // The first write: its admission ends the scope, the worker compacts, the
    // admission wait sees progress, the commit lands.
    runtime
        .commit(&put_batch(b"first-write", b"v"))
        .expect("the first write must be admitted once maintenance can progress");
    assert_eq!(
        runtime.reclaim_only_scope_for_test(),
        Some(crate::lifecycle::ReclaimOnlyScope::Inactive)
    );
    runtime.wait_background_idle_for_test();
    assert!(
        table_data_object_files(&root).len() < at_close.len(),
        "compaction ran once the session wrote"
    );
    runtime.close().expect("close");
}

/// Space-reclamation contract §3.1 (slice 3): a session with reclaim debt.
/// Two flushed L0 objects are superseded by a compaction whose reclaim mark is
/// queued but has not run, and one unflushed tail row keeps a WAL tail. No
/// worker, so nothing runs until the test drives it. Returns the open runtime
/// with the superseded objects and the full on-disk set before close.
#[cfg(feature = "localfs")]
fn plant_reclaim_debt(
    backend: &'static StorageBackend,
    root: &std::path::Path,
) -> (
    StorageRuntime<'static>,
    std::collections::BTreeSet<String>,
    std::collections::BTreeSet<String>,
) {
    let options = StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
        .with_maintenance_scheduling_policy(StorageMaintenanceSchedulingPolicy::EvaluateAndEnqueue);
    let mut runtime = StorageRuntime::open_with_backend(options, backend)
        .expect("open durable runtime")
        .into_runtime();
    runtime.commit(&put_batch(b"gc-a", b"one")).expect("commit");
    runtime
        .flush_default_branch_for_test()
        .expect("flush first L0 table");
    runtime.commit(&put_batch(b"gc-a", b"two")).expect("commit");
    runtime.commit(&put_batch(b"gc-b", b"x")).expect("commit");
    runtime
        .flush_default_branch_for_test()
        .expect("flush second L0 table");
    let superseded = table_data_object_files(root);
    assert!(superseded.len() >= 2, "two flushes: {superseded:?}");
    let compact =
        MaintenanceRequest::new(MaintenanceTask::Compact, MaintenanceScope::Branch(branch()));
    let compacted = runtime.maintenance(&compact).expect("compact");
    assert_eq!(compacted.task(), MaintenanceTask::Compact);
    runtime
        .commit(&put_batch(b"tail", b"unflushed"))
        .expect("commit the unflushed tail row");
    let before_close = table_data_object_files(root);
    assert!(
        superseded
            .iter()
            .all(|object| before_close.contains(object)),
        "the superseded objects are still on disk: {before_close:?}"
    );
    assert!(
        before_close.len() > superseded.len(),
        "the compaction output exists"
    );
    (runtime, superseded, before_close)
}

#[cfg(feature = "localfs")]
fn wal_segment_files(root: &std::path::Path) -> std::collections::BTreeSet<String> {
    std::fs::read_dir(root.join("wal"))
        .map(|entries| {
            entries
                .flatten()
                .filter(|entry| entry.path().is_file())
                .map(|entry| entry.path().display().to_string())
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(feature = "localfs")]
fn quarantine_object_files(root: &std::path::Path) -> std::collections::BTreeSet<String> {
    let mut files = std::collections::BTreeSet::new();
    let Ok(branches) = std::fs::read_dir(root.join("quarantine")) else {
        return files;
    };
    for branch_dir in branches.flatten() {
        let Ok(objects) = std::fs::read_dir(branch_dir.path()) else {
            continue;
        };
        for object in objects.flatten() {
            // The inventory manifest is bookkeeping, not a quarantined object.
            let name = object.file_name().to_string_lossy().into_owned();
            if object.path().is_file() && !name.starts_with("manifest") {
                files.insert(object.path().display().to_string());
            }
        }
    }
    files
}

#[cfg(feature = "localfs")]
fn reopen_and_drain(backend: &'static StorageBackend) -> StorageRuntime<'static> {
    let options = StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
        .with_maintenance_scheduling_policy(StorageMaintenanceSchedulingPolicy::EvaluateAndEnqueue);
    let mut runtime = StorageRuntime::open_with_backend(options, backend)
        .expect("reopen durable runtime")
        .into_runtime();
    drain_maintenance_to_idle(&mut runtime);
    runtime
}

/// A clean close drains the session's reclaim debt itself: the on-disk set
/// shrinks by exactly the superseded objects, and the reopened database reads
/// as before.
#[cfg(feature = "localfs")]
#[test]
fn api_clean_close_drains_the_sessions_reclaim_debt() {
    let root = temp_dir_for_api_test("maintenance-close-drain-reclaims");
    let backend = crate::testkit::leak_static(StorageBackend::local_fs(root.clone()));
    let (mut runtime, superseded, before_close) = plant_reclaim_debt(backend, &root);
    let expected: std::collections::BTreeSet<String> =
        before_close.difference(&superseded).cloned().collect();

    // A generous budget with a tight elapsed bound: the drive must stop the
    // moment a sweep finds nothing left, never spin the budget down.
    let started = std::time::Instant::now();
    let close = runtime
        .close_with_options(
            StorageCloseOptions::graceful()
                .with_reclaim_budget(ReclaimBudget::Bounded(std::time::Duration::from_secs(2))),
        )
        .expect("close");
    assert!(
        started.elapsed() < std::time::Duration::from_secs(1),
        "the drive stopped once the debt was gone: {:?}",
        started.elapsed()
    );
    assert!(close.maintenance_drained(), "{close:?}");
    assert_eq!(table_data_object_files(&root), expected);
    assert!(
        quarantine_object_files(&root).is_empty(),
        "the purge emptied the quarantine: {:?}",
        quarantine_object_files(&root)
    );

    let runtime = reopen_and_drain(backend);
    assert_eq!(read_value(&runtime, b"gc-a"), Some(b"two".to_vec()));
    assert_eq!(read_value(&runtime, b"tail"), Some(b"unflushed".to_vec()));
    assert_eq!(table_data_object_files(&root), expected);
}

/// The drive is bounded by its budget: a zero budget runs no round, the debt
/// survives the close intact, and the next open reclaims it.
#[cfg(feature = "localfs")]
#[test]
fn api_close_reclaim_is_bounded_by_the_budget() {
    let root = temp_dir_for_api_test("maintenance-close-drain-budget");
    let backend = crate::testkit::leak_static(StorageBackend::local_fs(root.clone()));
    let (mut runtime, superseded, before_close) = plant_reclaim_debt(backend, &root);
    let expected: std::collections::BTreeSet<String> =
        before_close.difference(&superseded).cloned().collect();

    runtime
        .close_with_options(
            StorageCloseOptions::graceful()
                .with_reclaim_budget(ReclaimBudget::Bounded(std::time::Duration::ZERO)),
        )
        .expect("close");
    assert_eq!(
        table_data_object_files(&root),
        before_close,
        "a zero budget reclaims nothing at close"
    );

    let _runtime = reopen_and_drain(backend);
    assert_eq!(
        table_data_object_files(&root),
        expected,
        "the next open reclaims what the close left"
    );
}

/// `ReclaimBudget::Disabled` skips the drive entirely.
#[cfg(feature = "localfs")]
#[test]
fn api_disabled_reclaim_budget_skips_the_close_drain() {
    let root = temp_dir_for_api_test("maintenance-close-drain-disabled");
    let backend = crate::testkit::leak_static(StorageBackend::local_fs(root.clone()));
    let (mut runtime, _superseded, before_close) = plant_reclaim_debt(backend, &root);

    runtime
        .close_with_options(
            StorageCloseOptions::graceful().with_reclaim_budget(ReclaimBudget::Disabled),
        )
        .expect("close");
    assert_eq!(table_data_object_files(&root), before_close);
}

/// A retired read view still held across the close pins its objects: the
/// close-time sweep defers, the debt survives, and the next open reclaims
/// once the view is gone.
#[cfg(feature = "localfs")]
#[test]
fn api_close_reclaim_defers_behind_a_held_read_view() {
    let root = temp_dir_for_api_test("maintenance-close-drain-reader-pin");
    let backend = crate::testkit::leak_static(StorageBackend::local_fs(root.clone()));
    let options = StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
        .with_maintenance_scheduling_policy(StorageMaintenanceSchedulingPolicy::EvaluateAndEnqueue);
    let mut runtime = StorageRuntime::open_with_backend(options, backend)
        .expect("open durable runtime")
        .into_runtime();
    runtime
        .commit(&put_batch(b"pin-a", b"one"))
        .expect("commit");
    runtime
        .flush_default_branch_for_test()
        .expect("flush first L0 table");
    runtime
        .commit(&put_batch(b"pin-a", b"two"))
        .expect("commit");
    runtime
        .flush_default_branch_for_test()
        .expect("flush second L0 table");
    let superseded = table_data_object_files(&root);
    // The view is taken BEFORE the compaction retires the objects it reads.
    let held_view = runtime
        .load_snapshot_for_test(branch())
        .expect("published snapshot");
    let compact =
        MaintenanceRequest::new(MaintenanceTask::Compact, MaintenanceScope::Branch(branch()));
    runtime.maintenance(&compact).expect("compact");
    let before_close = table_data_object_files(&root);
    let expected: std::collections::BTreeSet<String> =
        before_close.difference(&superseded).cloned().collect();

    runtime.close().expect("close");
    assert_eq!(
        table_data_object_files(&root),
        before_close,
        "a held retired view defers the close-time sweep"
    );
    drop(held_view);

    let _runtime = reopen_and_drain(backend);
    assert_eq!(table_data_object_files(&root), expected);
}

/// Space-reclamation contract §3.1 (slice 7): after the object drive the
/// close checkpoints and truncates the WAL behind the snapshot — every
/// pre-close segment goes, at most the fresh active segment remains, and the
/// unflushed tail row reads back from the snapshot on the next open.
#[cfg(feature = "localfs")]
#[test]
fn api_close_reclaim_truncates_the_wal_behind_the_close_checkpoint() {
    let root = temp_dir_for_api_test("maintenance-close-drain-wal-tail");
    let backend = crate::testkit::leak_static(StorageBackend::local_fs(root.clone()));
    let (mut runtime, superseded, before_close) = plant_reclaim_debt(backend, &root);
    let expected: std::collections::BTreeSet<String> =
        before_close.difference(&superseded).cloned().collect();
    let wal_before = wal_segment_files(&root);
    assert!(
        !wal_before.is_empty(),
        "the unflushed tail row lives in the WAL"
    );

    runtime.close().expect("close");
    assert_eq!(table_data_object_files(&root), expected);
    let wal_after = wal_segment_files(&root);
    assert!(
        wal_after.is_disjoint(&wal_before),
        "the close checkpoint covers every pre-close segment and truncates it: \
         before={wal_before:?} after={wal_after:?}"
    );
    assert!(
        wal_after.len() <= 1,
        "at most the fresh active segment remains: {wal_after:?}"
    );

    let reopened = reopen_and_drain(backend);
    assert_eq!(
        read_value(&reopened, b"tail").as_deref(),
        Some(b"unflushed".as_slice()),
        "the tail row reads back from the close checkpoint's snapshot"
    );
}

/// #3625: the close-time flush — commit `rows` rows of `value_len` bytes of
/// ordinary text on the default branch, 32 rows per commit.
#[cfg(feature = "localfs")]
fn commit_text_rows(runtime: &mut StorageRuntime<'_>, rows: usize, value_len: usize) {
    let words = [
        "branch", "commit", "table", "reclaim", "snapshot", "session", "agent", "tool",
    ];
    for chunk in (0..rows).collect::<Vec<_>>().chunks(32) {
        let mutations = chunk
            .iter()
            .map(|i| {
                let mut text = String::new();
                let mut w = *i;
                while text.len() < value_len {
                    text.push_str(words[w % words.len()]);
                    text.push(' ');
                    w = w.wrapping_mul(7).wrapping_add(3);
                }
                text.truncate(value_len);
                CommitMutation::Put {
                    storage_space: engine_space(),
                    key: api_key(format!("row-{i:06}").as_bytes()),
                    value: StorageValue::new(text.into_bytes()),
                    ttl: None,
                }
            })
            .collect();
        runtime
            .commit(
                &CommitBatch::new(branch(), mutations, CommitOptions::default()).expect("batch"),
            )
            .expect("commit");
    }
}

#[cfg(feature = "localfs")]
fn snapshot_bytes_on_disk(root: &std::path::Path) -> u64 {
    std::fs::read_dir(root.join("snapshots"))
        .map(|entries| {
            entries
                .flatten()
                .filter_map(|entry| entry.metadata().ok())
                .map(|metadata| metadata.len())
                .sum()
        })
        .unwrap_or(0)
}

/// #3625: a clean close flushes a large unflushed delta into (compressed)
/// tables before its checkpoint, so the snapshot carries only a small delta
/// instead of every row uncompressed; the rows read back after a reopen.
#[cfg(feature = "localfs")]
#[test]
fn api_close_flushes_a_large_delta_into_tables_before_its_checkpoint() {
    let root = temp_dir_for_api_test("maintenance-close-flush-large-delta");
    let backend = crate::testkit::leak_static(StorageBackend::local_fs(root.clone()));
    let inline = StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
        .with_maintenance_scheduling_policy(
            StorageMaintenanceSchedulingPolicy::DeterministicInline,
        );
    let mut runtime = StorageRuntime::open_with_backend(inline, backend)
        .expect("open durable runtime")
        .into_runtime();
    commit_text_rows(&mut runtime, 256, 1024);
    assert!(
        table_data_object_files(&root).is_empty(),
        "nothing flushed during the session"
    );
    runtime.close().expect("close");
    assert!(
        !table_data_object_files(&root).is_empty(),
        "the close flushed the large delta into a table"
    );
    let snapshot = snapshot_bytes_on_disk(&root);
    assert!(
        snapshot < 64 * 1024,
        "the snapshot no longer carries the 256 KiB delta: {snapshot} bytes"
    );
    let reopened = reopen_and_drain(backend);
    assert_eq!(
        read_value(&reopened, b"row-000255").map(|value| value.len()),
        Some(1024),
        "the flushed rows read back"
    );
}

/// #3625 direction control: a small delta stays in the close snapshot — no
/// table (and no level-0 churn) for a short session.
#[cfg(feature = "localfs")]
#[test]
fn api_close_keeps_a_small_delta_in_the_snapshot() {
    let root = temp_dir_for_api_test("maintenance-close-flush-small-delta");
    let backend = crate::testkit::leak_static(StorageBackend::local_fs(root.clone()));
    let inline = StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
        .with_maintenance_scheduling_policy(
            StorageMaintenanceSchedulingPolicy::DeterministicInline,
        );
    let mut runtime = StorageRuntime::open_with_backend(inline, backend)
        .expect("open durable runtime")
        .into_runtime();
    commit_text_rows(&mut runtime, 8, 256);
    runtime.close().expect("close");
    assert!(
        table_data_object_files(&root).is_empty(),
        "a small delta is not worth a table"
    );
    assert!(snapshot_bytes_on_disk(&root) > 0, "the snapshot holds it");
    let reopened = reopen_and_drain(backend);
    assert_eq!(
        read_value(&reopened, b"row-000007").map(|value| value.len()),
        Some(256)
    );
}

/// A backend that refuses deletes during the close-time sweep must not turn
/// the clean close into a failure: the close completes and the debt stays on
/// disk (what reclaims it afterwards is #3608).
#[cfg(feature = "localfs")]
#[test]
fn api_close_reclaim_fault_leaves_the_debt_for_the_next_open() {
    use crate::testkit::{BackendOperation, FaultKind, FaultMode, FaultRule, FaultScript};
    let root = temp_dir_for_api_test("maintenance-close-drain-fault");
    let faulting = crate::testkit::leak_static(StorageBackend::faulting_local_fs(
        root.clone(),
        FaultScript::new([FaultRule::with_mode(
            BackendOperation::DeleteObject,
            std::num::NonZeroU64::new(1).expect("nonzero"),
            FaultKind::Unavailable,
            FaultMode::Continuously,
        )]),
    ));
    let (mut runtime, superseded, before_close) = plant_reclaim_debt(faulting, &root);
    let expected: std::collections::BTreeSet<String> =
        before_close.difference(&superseded).cloned().collect();

    runtime
        .close()
        .expect("a failing close-time reclaim never fails the close");
    assert!(
        superseded
            .iter()
            .all(|object| table_data_object_files(&root).contains(object)),
        "refused deletes leave the debt on disk"
    );

    // #3608 (slice 8): the refused deletes left this branch's quarantine
    // entries with their sources still on disk. The next open's mark names
    // them retry candidates, the sweep retries the delete through the existing
    // entries, and the chained purge removes the copies.
    let healthy = crate::testkit::leak_static(StorageBackend::local_fs(root.clone()));
    let mut reopened = reopen_and_drain(healthy);
    assert_eq!(
        table_data_object_files(&root),
        expected,
        "the reopen retries the refused source deletes"
    );
    assert!(
        quarantine_object_files(&root).is_empty(),
        "the purge removed the quarantine copies"
    );
    let ledger = reopened
        .reclaim_ledger_for_test()
        .expect("durable runtime has a reclaim ledger");
    let sweep = ledger
        .last(ReclaimFamily::TableObjectSweep)
        .expect("the reopen's sweep is recorded");
    assert_eq!(sweep.outcome(), ReclaimOutcome::Reclaimed, "{sweep:?}");
    assert_eq!(
        sweep.objects_affected(),
        superseded.len(),
        "every refused source was retried: {sweep:?}"
    );
    assert_eq!(read_value(&reopened, b"gc-a"), Some(b"two".to_vec()));
    reopened.close().expect("close");
}

/// More superseded objects than one sweep stages: the drive runs the extra
/// rounds inside the budget until nothing is left, and purges after each.
#[cfg(feature = "localfs")]
#[test]
fn api_close_reclaim_runs_multiple_rounds_within_the_budget() {
    let root = temp_dir_for_api_test("maintenance-close-drain-multi-round");
    let backend = crate::testkit::leak_static(StorageBackend::local_fs(root.clone()));
    let options = StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
        .with_maintenance_scheduling_policy(StorageMaintenanceSchedulingPolicy::EvaluateAndEnqueue);
    let mut runtime = StorageRuntime::open_with_backend(options, backend)
        .expect("open durable runtime")
        .into_runtime();
    // One more L0 object than a single sweep stages.
    for index in 0..=crate::lifecycle::TABLE_OBJECT_SWEEP_MAX_OBJECTS {
        runtime
            .commit(&put_batch(format!("multi-{index}").as_bytes(), b"v"))
            .expect("commit");
        runtime
            .flush_default_branch_for_test()
            .expect("flush one more L0 table");
    }
    let superseded = table_data_object_files(&root);
    assert!(superseded.len() > crate::lifecycle::TABLE_OBJECT_SWEEP_MAX_OBJECTS);
    let compact =
        MaintenanceRequest::new(MaintenanceTask::Compact, MaintenanceScope::Branch(branch()));
    runtime.maintenance(&compact).expect("compact");
    let before_close = table_data_object_files(&root);
    let expected: std::collections::BTreeSet<String> =
        before_close.difference(&superseded).cloned().collect();
    assert!(!expected.is_empty(), "the compaction output exists");

    // Staging dozens of objects (each a durable publish) can outrun the
    // default budget on a slow disk; the rounds, not the clock, are under test.
    runtime
        .close_with_options(
            StorageCloseOptions::graceful()
                .with_reclaim_budget(ReclaimBudget::Bounded(std::time::Duration::from_secs(30))),
        )
        .expect("close");
    assert_eq!(table_data_object_files(&root), expected);
    assert!(quarantine_object_files(&root).is_empty());
}

// ---------------------------------------------------------------------------
// Space-reclamation contract §3.1 (slice 8): the idle (quiescence) wake, the
// one-wake-per-quiet-period rule, and debt-aware low-tier fairness.
// ---------------------------------------------------------------------------

/// The idle-wake debounce the slice-8 scenarios open with (manual clock).
#[cfg(feature = "localfs")]
const IDLE_WAKE_DEBOUNCE_MILLIS: u64 = 100;

#[cfg(feature = "localfs")]
fn open_inline_durable_runtime(
    name: &str,
    options: StorageOpenOptions,
) -> (StorageRuntime<'static>, std::path::PathBuf) {
    let root = temp_dir_for_api_test(name);
    let backend = crate::testkit::leak_static(StorageBackend::local_fs(root.clone()));
    let runtime = StorageRuntime::open_with_backend(
        options
            .with_maintenance_scheduling_policy(
                StorageMaintenanceSchedulingPolicy::DeterministicInline,
            )
            .with_quiescence_debounce_millis_for_test(IDLE_WAKE_DEBOUNCE_MILLIS),
        backend,
    )
    .expect("open durable runtime")
    .into_runtime();
    (runtime, root)
}

/// Two flushed L0 tables superseded by a compaction while an off-lock reader
/// holds the pre-compaction view: the chain runs on the inline executor and
/// the sweep defers `ReaderPinned`. Returns the superseded object files.
#[cfg(feature = "localfs")]
fn plant_reader_deferred_debt(
    runtime: &mut StorageRuntime<'static>,
    root: &std::path::Path,
) -> (
    std::collections::BTreeSet<String>,
    std::sync::Arc<crate::branch::read::BranchReadView>,
) {
    runtime
        .commit(&put_batch(b"idle-a", b"one"))
        .expect("commit");
    runtime
        .flush_default_branch_for_test()
        .expect("flush first L0 table");
    runtime
        .commit(&put_batch(b"idle-a", b"two"))
        .expect("commit");
    runtime
        .flush_default_branch_for_test()
        .expect("flush second L0 table");
    let superseded = table_data_object_files(root);
    let held_view = runtime
        .load_snapshot_for_test(branch())
        .expect("published snapshot");
    let compact =
        MaintenanceRequest::new(MaintenanceTask::Compact, MaintenanceScope::Branch(branch()));
    runtime.maintenance(&compact).expect("compact");
    runtime.wait_background_idle_for_test();
    let deferred = runtime
        .reclaim_ledger_for_test()
        .expect("ledger")
        .last(ReclaimFamily::TableObjectSweep)
        .expect("the sweep ran");
    assert_eq!(deferred.outcome(), ReclaimOutcome::Deferred, "{deferred:?}");
    assert_eq!(
        deferred.deferral(),
        Some(MaintenanceDeferralReason::ReaderPinned)
    );
    assert!(
        superseded
            .iter()
            .all(|object| table_data_object_files(root).contains(object)),
        "the deferred sweep left every superseded object"
    );
    (superseded, held_view)
}

/// The reader drops during a quiet period: no commit, no publish, no explicit
/// verb — the idle wake alone retries the sweep after the debounce, and the
/// superseded objects go.
#[cfg(feature = "localfs")]
#[test]
fn api_idle_wake_retries_a_reader_deferred_sweep_without_any_commit() {
    let (mut runtime, root) = open_inline_durable_runtime(
        "maintenance-idle-wake-retries",
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
    );
    let (superseded, held_view) = plant_reader_deferred_debt(&mut runtime, &root);
    let visible_before = runtime.visible_version_for_test();
    assert!(
        runtime.reclaim_owed_for_test(),
        "the deferred sweep left debt owed"
    );

    drop(held_view);
    // Short of the debounce: nothing fires.
    assert!(
        runtime.advance_maintenance_clock_for_test(std::time::Duration::from_millis(
            IDLE_WAKE_DEBOUNCE_MILLIS - 1
        ))
    );
    runtime.wait_background_idle_for_test();
    assert_eq!(
        runtime
            .reclaim_ledger_for_test()
            .expect("ledger")
            .idle_wakes(),
        0
    );
    assert!(superseded
        .iter()
        .all(|object| table_data_object_files(&root).contains(object)));

    // The debounce elapses: the one idle wake runs the sweep and its purge.
    assert!(runtime.advance_maintenance_clock_for_test(std::time::Duration::from_millis(1)));
    runtime.wait_background_idle_for_test();

    let ledger = runtime.reclaim_ledger_for_test().expect("ledger");
    assert_eq!(ledger.idle_wakes(), 1, "{ledger:?}");
    assert_eq!(ledger.last_idle_wake(), Some(visible_before), "{ledger:?}");
    let sweep = ledger
        .last(ReclaimFamily::TableObjectSweep)
        .expect("sweep recorded");
    assert_eq!(sweep.outcome(), ReclaimOutcome::Reclaimed, "{sweep:?}");
    assert!(
        superseded
            .iter()
            .all(|object| !table_data_object_files(&root).contains(object)),
        "the idle wake's sweep reclaimed the superseded objects"
    );
    assert_eq!(
        runtime.visible_version_for_test(),
        visible_before,
        "no commit was needed"
    );
    // Only a sweep that finds nothing proves the family clean: the debt stays
    // owed past the purge, and the next quiet period's wake settles it.
    assert!(runtime.reclaim_owed_for_test(), "{ledger:?}");
    runtime
        .commit(&put_batch(b"idle-b", b"three"))
        .expect("commit");
    runtime.wait_background_idle_for_test();
    assert!(
        runtime.advance_maintenance_clock_for_test(std::time::Duration::from_millis(
            IDLE_WAKE_DEBOUNCE_MILLIS
        ))
    );
    runtime.wait_background_idle_for_test();
    let ledger = runtime.reclaim_ledger_for_test().expect("ledger");
    assert_eq!(ledger.idle_wakes(), 2, "{ledger:?}");
    assert_eq!(
        ledger
            .last(ReclaimFamily::TableObjectSweep)
            .map(crate::lifecycle::ReclaimPass::outcome),
        Some(ReclaimOutcome::Nothing)
    );
    assert!(
        !runtime.reclaim_owed_for_test(),
        "the clean sweep settled the debt"
    );
    assert_eq!(runtime.suspected_debt_objects_for_test(), 0);
}

/// A runtime with nothing owed arms no idle wake: time passing runs nothing.
#[cfg(feature = "localfs")]
#[test]
fn api_clean_idle_runtime_arms_no_idle_wake() {
    let (runtime, _root) = open_inline_durable_runtime(
        "maintenance-idle-wake-clean",
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
    );
    runtime
        .commit(&put_batch(b"clean-a", b"one"))
        .expect("commit");
    runtime.wait_background_idle_for_test();
    assert!(!runtime.reclaim_owed_for_test());
    let started_before = runtime.maintenance_status().expect("status").started();

    assert!(
        runtime.advance_maintenance_clock_for_test(std::time::Duration::from_millis(
            IDLE_WAKE_DEBOUNCE_MILLIS * 10
        ))
    );
    runtime.wait_background_idle_for_test();

    let ledger = runtime.reclaim_ledger_for_test().expect("ledger");
    assert_eq!(ledger.idle_wakes(), 0, "{ledger:?}");
    assert_eq!(
        runtime.maintenance_status().expect("status").started(),
        started_before,
        "no task started on the passage of time"
    );
}

/// One idle wake per quiet period: a sweep still deferred at the wake waits
/// for the next activity edge (a commit's drain), which arms a fresh wake.
/// Never a periodic poll against a long-held reader.
#[cfg(feature = "localfs")]
#[test]
fn api_idle_wake_fires_once_per_quiet_period_and_rearms_on_activity() {
    let (mut runtime, root) = open_inline_durable_runtime(
        "maintenance-idle-wake-once",
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
    );
    let (superseded, held_view) = plant_reader_deferred_debt(&mut runtime, &root);
    let debounce = std::time::Duration::from_millis(IDLE_WAKE_DEBOUNCE_MILLIS);

    // Wake 1: the reader is still held, the sweep defers again.
    assert!(runtime.advance_maintenance_clock_for_test(debounce));
    runtime.wait_background_idle_for_test();
    assert_eq!(
        runtime
            .reclaim_ledger_for_test()
            .expect("ledger")
            .idle_wakes(),
        1
    );
    assert!(runtime.reclaim_owed_for_test());
    // Time alone never wakes again in the same quiet period.
    for _ in 0..3 {
        assert!(runtime.advance_maintenance_clock_for_test(debounce));
        runtime.wait_background_idle_for_test();
    }
    assert_eq!(
        runtime
            .reclaim_ledger_for_test()
            .expect("ledger")
            .idle_wakes(),
        1,
        "the quiet period's one idle wake is spent"
    );
    assert!(superseded
        .iter()
        .all(|object| table_data_object_files(&root).contains(object)));

    // Activity (a commit's ordinary drain) starts a new quiet period.
    runtime
        .commit(&put_batch(b"idle-b", b"three"))
        .expect("commit");
    runtime.wait_background_idle_for_test();
    assert!(runtime.advance_maintenance_clock_for_test(debounce));
    runtime.wait_background_idle_for_test();
    assert_eq!(
        runtime
            .reclaim_ledger_for_test()
            .expect("ledger")
            .idle_wakes(),
        2
    );

    // The reader goes; the next period's wake reclaims.
    drop(held_view);
    runtime
        .commit(&put_batch(b"idle-c", b"four"))
        .expect("commit");
    runtime.wait_background_idle_for_test();
    assert!(runtime.advance_maintenance_clock_for_test(debounce));
    runtime.wait_background_idle_for_test();
    let ledger = runtime.reclaim_ledger_for_test().expect("ledger");
    assert_eq!(ledger.idle_wakes(), 3, "{ledger:?}");
    assert!(
        superseded
            .iter()
            .all(|object| !table_data_object_files(&root).contains(object)),
        "the third wake's sweep reclaimed the superseded objects"
    );
    // The debt stays owed until a sweep proves the family clean (the next
    // quiet period's wake); see the retry scenario for that settle.
    assert!(runtime.reclaim_owed_for_test(), "{ledger:?}");
}

/// Debt settled between arming and firing: the idle wake finds nothing owed
/// and queues no sweep — the ledger records the wake and no new pass.
#[cfg(feature = "localfs")]
#[test]
fn api_idle_wake_after_the_debt_settled_queues_no_sweep() {
    let (mut runtime, root) = open_inline_durable_runtime(
        "maintenance-idle-wake-settled",
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
    );
    let (superseded, held_view) = plant_reader_deferred_debt(&mut runtime, &root);
    drop(held_view);
    // Two explicit reclaims: the first stages and purges, the second's sweep
    // finds nothing and settles the debt — all before the debounce elapses.
    let reclaim =
        MaintenanceRequest::new(MaintenanceTask::Reclaim, MaintenanceScope::Branch(branch()));
    for _ in 0..2 {
        runtime.maintenance(&reclaim).expect("reclaim");
        runtime.wait_background_idle_for_test();
    }
    assert!(superseded
        .iter()
        .all(|object| !table_data_object_files(&root).contains(object)));
    assert!(
        !runtime.reclaim_owed_for_test(),
        "the second sweep settled the debt"
    );
    let passes_before = runtime
        .reclaim_ledger_for_test()
        .expect("ledger")
        .totals()
        .passes();

    assert!(
        runtime.advance_maintenance_clock_for_test(std::time::Duration::from_millis(
            IDLE_WAKE_DEBOUNCE_MILLIS
        ))
    );
    runtime.wait_background_idle_for_test();

    let ledger = runtime.reclaim_ledger_for_test().expect("ledger");
    assert_eq!(
        ledger.idle_wakes(),
        1,
        "the armed wake still fires: {ledger:?}"
    );
    assert_eq!(
        ledger.totals().passes(),
        passes_before,
        "a wake with nothing owed runs no pass: {ledger:?}"
    );
}

/// The foreground verb wakes the worker only for the follow-ups it queued. A
/// verb that leaves nothing pending is not an activity edge: it runs no empty
/// round and does not re-open the quiet period whose one idle wake is spent —
/// time alone still wakes nothing, exactly as before the verb.
#[cfg(feature = "localfs")]
#[test]
fn api_foreground_verb_with_nothing_queued_is_not_an_activity_edge() {
    let (mut runtime, root) = open_inline_durable_runtime(
        "maintenance-verb-no-wake",
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
    );
    let (_superseded, _held_view) = plant_reader_deferred_debt(&mut runtime, &root);
    let debounce = std::time::Duration::from_millis(IDLE_WAKE_DEBOUNCE_MILLIS);
    // Spend the quiet period's one idle wake; the reader still pins the sweep.
    assert!(runtime.advance_maintenance_clock_for_test(debounce));
    runtime.wait_background_idle_for_test();
    assert_eq!(
        runtime
            .reclaim_ledger_for_test()
            .expect("ledger")
            .idle_wakes(),
        1
    );
    assert!(runtime.reclaim_owed_for_test());
    assert_eq!(
        runtime
            .maintenance_status()
            .expect("status")
            .pending_tasks(),
        0
    );

    // A flush with nothing to flush queues no follow-up.
    let flush = MaintenanceRequest::new(MaintenanceTask::Flush, MaintenanceScope::Branch(branch()));
    runtime.maintenance(&flush).expect("flush verb");
    runtime.wait_background_idle_for_test();
    assert_eq!(
        runtime
            .maintenance_status()
            .expect("status")
            .pending_tasks(),
        0,
        "the verb left nothing queued"
    );
    let started_after_verb = runtime.maintenance_status().expect("status").started();

    for _ in 0..3 {
        assert!(runtime.advance_maintenance_clock_for_test(debounce));
        runtime.wait_background_idle_for_test();
    }
    let ledger = runtime.reclaim_ledger_for_test().expect("ledger");
    assert_eq!(
        ledger.idle_wakes(),
        1,
        "a verb with nothing queued must not wake the worker and re-arm the spent period: {ledger:?}"
    );
    assert_eq!(
        runtime.maintenance_status().expect("status").started(),
        started_after_verb,
        "no task started on the passage of time"
    );
}

/// A round's pending count is the queue's size plus the armed preheat flag:
/// a round that made progress with work still queued re-arms the next round
/// by itself, so the follow-ups one task chains drain without any further
/// external wake. One task per wake makes every chained follow-up its own
/// round; a single enqueue (one wake) is the only external signal.
#[cfg(feature = "localfs")]
#[test]
fn api_round_with_work_still_queued_rearms_without_an_external_wake() {
    let (runtime, _root) = open_inline_durable_runtime(
        "maintenance-round-rearm",
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
            .with_background_max_tasks_per_wake(1),
    );
    for index in 0..3u8 {
        runtime
            .commit(&put_batch(format!("rearm-{index}").as_bytes(), b"value"))
            .expect("commit");
    }
    runtime.wait_background_idle_for_test();
    assert_eq!(
        runtime
            .maintenance_status()
            .expect("status")
            .pending_tasks(),
        0
    );

    // One wake, several tasks: the completed checkpoint chains its snapshot
    // prune and WAL-truncation follow-ups, each drained by its own round.
    runtime
        .enqueue_maintenance(&MaintenanceRequest::new(
            MaintenanceTask::Checkpoint,
            MaintenanceScope::Global,
        ))
        .expect("enqueue checkpoint");
    runtime.wait_background_idle_for_test();

    let status = runtime.maintenance_status().expect("status");
    assert_eq!(
        status.pending_tasks(),
        0,
        "every chained follow-up drained on self-armed rounds: {status:?}"
    );
    assert!(
        status.completed() >= 2,
        "the checkpoint and at least one follow-up ran: {status:?}"
    );
    let after = runtime
        .diagnostics(DiagnosticsRequest::new(DiagnosticsScope::Global))
        .expect("diagnostics");
    assert!(after.checkpoint().snapshot_id().is_some());
}

/// A fixed-point compaction that removes no input table owes nothing: it
/// notes no debt and queues no mark, so compacting a branch with nothing to
/// compact leaves the reclaim ledger and the queue untouched.
#[cfg(feature = "localfs")]
#[test]
fn api_compaction_that_removes_no_input_queues_no_mark() {
    let (mut runtime, _root) = open_inline_durable_runtime(
        "maintenance-compact-no-input",
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
    );
    runtime.commit(&put_batch(b"solo", b"one")).expect("commit");
    runtime.wait_background_idle_for_test();
    assert!(!runtime.reclaim_owed_for_test());
    let started_before = runtime.maintenance_status().expect("status").started();

    // No durable table on the branch: the compaction drains to its fixed
    // point at once and removes nothing.
    let compact =
        MaintenanceRequest::new(MaintenanceTask::Compact, MaintenanceScope::Branch(branch()));
    runtime.maintenance(&compact).expect("compact");
    assert_eq!(
        runtime
            .maintenance_status()
            .expect("status")
            .pending_tasks(),
        0,
        "nothing removed, nothing to mark"
    );
    assert!(
        !runtime.reclaim_owed_for_test(),
        "a compaction that removed nothing notes no debt"
    );
    runtime.wait_background_idle_for_test();
    let ledger = runtime.reclaim_ledger_for_test().expect("ledger");
    assert_eq!(
        ledger.last(ReclaimFamily::TableObjectMark),
        None,
        "{ledger:?}"
    );
    assert_eq!(
        runtime.maintenance_status().expect("status").started(),
        started_before,
        "no task started for a no-op compaction"
    );
}

/// The per-task runners share the rule: only a `Completed` rewrite dropped
/// input refs, so a compaction or materialization that finds nothing to do
/// notes no debt and queues no mark (direction control for the debt note at
/// each runner's completion site).
#[cfg(feature = "localfs")]
#[test]
fn api_noop_compaction_and_materialization_runners_owe_nothing() {
    let (mut runtime, _root) = open_inline_durable_runtime(
        "maintenance-noop-runners",
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
    );
    runtime.commit(&put_batch(b"solo", b"one")).expect("commit");
    runtime.wait_background_idle_for_test();
    assert!(!runtime.reclaim_owed_for_test());

    // No durable table: the compaction runner itself finds nothing to rewrite.
    runtime
        .force_branch_compaction_for_test(branch())
        .expect("forced compaction");
    assert_eq!(
        runtime
            .maintenance_status()
            .expect("status")
            .pending_tasks(),
        0,
        "a no-op compaction queues no mark"
    );
    assert!(
        !runtime.reclaim_owed_for_test(),
        "a no-op compaction notes no debt"
    );

    // A root branch holds no inherited layers: nothing to materialize.
    let materialize = MaintenanceRequest::new(
        MaintenanceTask::Materialize,
        MaintenanceScope::Branch(branch()),
    );
    let summary = runtime.maintenance(&materialize).expect("materialize");
    assert_eq!(summary.status(), MaintenanceSummaryStatus::Deferred);
    assert_eq!(
        runtime
            .maintenance_status()
            .expect("status")
            .pending_tasks(),
        0,
        "a no-op materialization queues no mark"
    );
    assert!(
        !runtime.reclaim_owed_for_test(),
        "a no-op materialization notes no debt"
    );
    // The inline scheduler would run a wrongly queued mark at once, leaving
    // nothing pending; the ledger is where it would show.
    runtime.wait_background_idle_for_test();
    let ledger = runtime.reclaim_ledger_for_test().expect("ledger");
    assert_eq!(
        ledger.last(ReclaimFamily::TableObjectMark),
        None,
        "neither no-op runner queued a mark: {ledger:?}"
    );
}

/// Close with an idle wake armed: the close completes (its own drive tries the
/// sweep once more and yields to the held reader) and the armed wake is
/// cancelled with the workers — it never fires into a closed runtime.
#[cfg(feature = "localfs")]
#[test]
fn api_close_cancels_an_armed_idle_wake() {
    let (mut runtime, root) = open_inline_durable_runtime(
        "maintenance-idle-wake-close",
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
    );
    let (superseded, held_view) = plant_reader_deferred_debt(&mut runtime, &root);
    assert!(runtime.reclaim_owed_for_test());

    let close = runtime.close().expect("close");
    assert!(close.maintenance_drained());
    // A closed runtime has no clock to advance and no worker to wake.
    assert!(!runtime.advance_maintenance_clock_for_test(std::time::Duration::from_secs(1)));
    assert!(superseded
        .iter()
        .all(|object| table_data_object_files(&root).contains(object)));
    drop(held_view);
}

/// Debt-aware fairness: with suspected debt at the threshold a round services
/// the low tier ahead of an upper-tier task even before the fairness floor;
/// below the threshold the ladder runs the upper tier first.
#[cfg(feature = "localfs")]
fn fairness_scenario(name: &str, threshold_objects: u64) -> (bool, bool) {
    let (mut runtime, _root) = open_inline_durable_runtime(
        name,
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
            .with_background_max_tasks_per_wake(1)
            .with_low_tier_debt_threshold_objects_for_test(threshold_objects),
    );
    runtime
        .commit(&put_batch(b"fair-a", b"one"))
        .expect("commit");
    runtime
        .flush_default_branch_for_test()
        .expect("flush first L0 table");
    runtime
        .commit(&put_batch(b"fair-a", b"two"))
        .expect("commit");
    runtime
        .flush_default_branch_for_test()
        .expect("flush second L0 table");
    // The compaction drops two input refs: debt 2, the mark queued and run by
    // the notified single-task round; the sweep it chains stays queued.
    let compact =
        MaintenanceRequest::new(MaintenanceTask::Compact, MaintenanceScope::Branch(branch()));
    runtime.maintenance(&compact).expect("compact");
    assert!(runtime.reclaim_owed_for_test());
    assert!(runtime.suspected_debt_objects_for_test() >= 2);
    let sweep_ran_before = runtime
        .reclaim_ledger_for_test()
        .expect("ledger")
        .last(ReclaimFamily::TableObjectSweep)
        .is_some();
    // An upper-tier task (a checkpoint of the two commits) joins the queue; the
    // enqueue's wake runs exactly one round of one task.
    runtime
        .commit(&put_batch(b"fair-b", b"three"))
        .expect("commit");
    runtime
        .enqueue_lifecycle_maintenance_for_test(
            crate::lifecycle::MaintenanceTaskRequest::checkpoint(),
        )
        .expect("enqueue checkpoint");
    // The enqueue's wake coalesces onto the round the mark's chain queued;
    // run exactly that one round of one task.
    assert!(runtime.run_one_background_round_for_test());
    let sweep_ran_after = runtime
        .reclaim_ledger_for_test()
        .expect("ledger")
        .last(ReclaimFamily::TableObjectSweep)
        .is_some();
    runtime.wait_background_idle_for_test();
    (sweep_ran_before, sweep_ran_after)
}

#[cfg(feature = "localfs")]
#[test]
fn api_debt_at_the_threshold_services_reclaim_before_the_fairness_floor() {
    let (before, after) = fairness_scenario("maintenance-fairness-debt", 2);
    assert!(!before, "the sweep was still queued behind the mark");
    assert!(
        after,
        "with debt at the threshold the round ran the sweep, not the checkpoint"
    );
}

#[cfg(feature = "localfs")]
#[test]
fn api_debt_below_the_threshold_keeps_the_upper_tier_first() {
    let (before, after) = fairness_scenario("maintenance-fairness-floor", 1_000);
    assert!(!before, "the sweep was still queued behind the mark");
    assert!(
        !after,
        "below the threshold the ladder ran the checkpoint first; the sweep waits for the floor"
    );
}

/// #3646 (from the external #3596 review): before the sweep moves a
/// superseded input, the catalogue still records it and the audit counts it
/// as unreferenced; the Audit tier's table and unreferenced facts must still
/// add up to the physical table bytes, each file counted once.
#[cfg(feature = "localfs")]
#[test]
fn api_audit_footprint_families_are_disjoint_before_the_sweep() {
    let root = temp_dir_for_api_test("review-3596-footprint-overlap");
    let backend = crate::testkit::leak_static(StorageBackend::local_fs(root.clone()));
    let (runtime, _, _) = plant_reclaim_debt(backend, &root);
    let footprint = runtime
        .diagnostics(
            DiagnosticsRequest::new(DiagnosticsScope::Global).with_detail(DiagnosticsDetail::Audit),
        )
        .expect("audit")
        .footprint();
    let actual = table_data_object_bytes(&root);
    assert!(
        footprint.unreferenced_bytes().unwrap() > 0,
        "fixture has debt"
    );
    assert_eq!(
        footprint.live_table_bytes().unwrap() + footprint.unreferenced_bytes().unwrap(),
        actual,
        "canonical table families must not count a file twice: {footprint:?}"
    );
}

/// #3646, the other side of the overlap: after a reopen the table catalog holds
/// only the tables the manifests reference, so the prior session's orphans are
/// unreferenced but NOT catalogued — the live figure must not subtract them.
#[cfg(feature = "localfs")]
#[test]
fn api_audit_footprint_does_not_subtract_uncatalogued_orphans() {
    let (root, backend, superseded, _) =
        plant_reclaim_debt_and_close("maintenance-footprint-uncatalogued-orphans");
    let options = StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
        .with_maintenance_scheduling_policy(StorageMaintenanceSchedulingPolicy::EvaluateAndEnqueue);
    let runtime = StorageRuntime::open_with_backend(options, backend)
        .expect("reopen durable runtime")
        .into_runtime();
    let footprint = runtime
        .diagnostics(
            DiagnosticsRequest::new(DiagnosticsScope::Global).with_detail(DiagnosticsDetail::Audit),
        )
        .expect("audit")
        .footprint();
    assert_eq!(
        footprint.unreferenced_objects(),
        Some(superseded.len()),
        "{footprint:?}"
    );
    assert_eq!(
        footprint.live_table_bytes().unwrap() + footprint.unreferenced_bytes().unwrap(),
        table_data_object_bytes(&root),
        "each file counted once, orphans uncatalogued: {footprint:?}"
    );
}
