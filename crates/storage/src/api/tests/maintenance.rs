use super::*;

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
fn stage_quarantine_object(
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
    }
    #[cfg(not(feature = "localfs"))]
    {
        assert_eq!(outcome.status(), MaintenanceSummaryStatus::Deferred);
        assert_eq!(
            outcome.reason(),
            Some("cache runtime does not support durable snapshot pruning maintenance")
        );
    }
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
fn drain_maintenance_to_idle(runtime: &mut StorageRuntime<'static>) {
    for _ in 0..8 {
        let drain = runtime.drain_maintenance().expect("drain maintenance");
        if drain.drained_tasks() == 0 {
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
    let mut runtime = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
        backend,
    )
    .expect("reopen durable runtime")
    .into_runtime();
    drain_maintenance_to_idle(&mut runtime);

    let after = table_data_object_files(&root);
    assert!(
        !after.contains(&orphan_path),
        "the planted orphan must be reclaimed by the post-recovery reconcile",
    );
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
