use super::*;

fn open_runtime() -> StorageRuntime<'static> {
    StorageRuntime::open_ephemeral()
        .expect("open ephemeral runtime")
        .into_runtime()
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

fn put_at(
    runtime: &mut StorageRuntime<'_>,
    branch_id: BranchId,
    key: &[u8],
    value: &[u8],
    timestamp: u64,
) -> CommitSummary {
    runtime
        .commit_for_test(
            &put_batch_for(branch_id, key, value),
            Timestamp::from_micros(timestamp),
        )
        .expect("commit")
}

fn branch_request(branch_id: BranchId, action: BranchAction) -> BranchRequest {
    BranchRequest::new(branch_id, action, Some(BranchGeneration::new(1)))
}

fn create_request(branch_id: BranchId) -> BranchRequest {
    branch_request(branch_id, BranchAction::Create)
}

fn describe_request(branch_id: BranchId) -> BranchRequest {
    BranchRequest::new(branch_id, BranchAction::Describe, None)
}

fn list_request() -> BranchRequest {
    BranchRequest::new(branch(), BranchAction::List, None)
}

fn read_value(runtime: &StorageRuntime<'_>, branch_id: BranchId, key: &[u8]) -> Option<Vec<u8>> {
    runtime
        .read_point(&PointReadRequest::new(
            branch_id,
            engine_space(),
            api_key(key),
            ReadBound::Latest,
        ))
        .expect("read")
        .row()
        .and_then(|row| row.value().map(|value| value.as_bytes().to_vec()))
}

#[test]
fn branch_create_returns_generation() {
    let runtime = open_runtime();
    let new_branch = branch_with(0x20);

    let outcome = runtime
        .branch(&create_request(new_branch))
        .expect("create branch");
    let summary = outcome.branch().expect("created branch");

    assert_eq!(outcome.operation(), BranchOperation::Created);
    assert_eq!(summary.branch_id(), new_branch);
    assert_eq!(summary.status(), BranchStatus::Active);
    assert_eq!(summary.generation(), BranchGeneration::new(1));
    assert_eq!(outcome.generation_after(), Some(BranchGeneration::new(1)));
}

#[test]
fn branch_create_duplicate_rejects() {
    let runtime = open_runtime();

    let error = runtime
        .branch(&create_request(branch()))
        .expect_err("duplicate branch rejected");

    assert_eq!(error.class(), StorageApiErrorClass::AlreadyExists);
}

#[test]
fn branch_create_invalid_identifier_rejects() {
    let runtime = open_runtime();
    let zero = BranchId::from_bytes([0; BranchId::BYTE_LEN]);

    let error = runtime
        .branch(&create_request(zero))
        .expect_err("zero branch rejected");

    assert_eq!(error.class(), StorageApiErrorClass::InvalidArgument);
    assert_eq!(error.code(), "invalid_argument.storage_api.argument");
}

#[test]
fn branch_list_is_deterministic() {
    let runtime = open_runtime();
    runtime
        .branch(&create_request(branch_with(0x33)))
        .expect("create third");
    runtime
        .branch(&create_request(branch_with(0x22)))
        .expect("create second");

    let outcome = runtime.branch(&list_request()).expect("list branches");
    let ids = outcome
        .branches()
        .iter()
        .map(|branch| branch.branch_id())
        .collect::<Vec<_>>();

    assert_eq!(outcome.operation(), BranchOperation::Listed);
    assert_eq!(ids, vec![branch(), branch_with(0x22), branch_with(0x33)]);
}

#[test]
fn branch_describe_reports_generation() {
    let runtime = open_runtime();

    let outcome = runtime
        .branch(&describe_request(branch()))
        .expect("describe branch");
    let summary = outcome.branch().expect("branch");

    assert_eq!(outcome.operation(), BranchOperation::Described);
    assert_eq!(summary.branch_id(), branch());
    assert_eq!(summary.generation(), BranchGeneration::new(1));
    assert_eq!(summary.status(), BranchStatus::Active);
}

#[test]
fn branch_describe_unknown_rejects() {
    let runtime = open_runtime();

    let error = runtime
        .branch(&describe_request(branch_with(0x41)))
        .expect_err("unknown branch rejected");

    assert_eq!(error.class(), StorageApiErrorClass::NotFound);
}

#[test]
fn branch_fork_current_copies_visible_frontier() {
    let mut runtime = open_runtime();
    put_at(&mut runtime, branch(), b"key", b"old", 10);
    put_at(&mut runtime, branch(), b"key", b"new", 20);
    let child = branch_with(0x42);

    let outcome = runtime
        .branch(&branch_request(
            child,
            BranchAction::ForkCurrent { source: branch() },
        ))
        .expect("fork current");

    assert_eq!(outcome.operation(), BranchOperation::Forked);
    assert_eq!(outcome.source_branch_id(), Some(branch()));
    assert_eq!(outcome.fork_version(), Some(CommitVersion::new(2)));
    assert_eq!(read_value(&runtime, child, b"key"), Some(b"new".to_vec()));
}

#[test]
fn branch_fork_current_preserves_inherited_visibility() {
    let mut runtime = open_runtime();
    put_at(&mut runtime, branch(), b"shared", b"parent", 10);
    let child = branch_with(0x43);

    runtime
        .branch(&branch_request(
            child,
            BranchAction::ForkCurrent { source: branch() },
        ))
        .expect("fork");
    put_at(&mut runtime, child, b"child-only", b"child", 20);

    assert_eq!(
        read_value(&runtime, child, b"shared"),
        Some(b"parent".to_vec())
    );
    assert_eq!(
        read_value(&runtime, child, b"child-only"),
        Some(b"child".to_vec())
    );
    assert_eq!(read_value(&runtime, branch(), b"child-only"), None);
}

#[test]
fn branch_fork_at_retained_version_succeeds() {
    let mut runtime = open_runtime();
    let first = put_at(&mut runtime, branch(), b"history", b"one", 10);
    put_at(&mut runtime, branch(), b"history", b"two", 20);
    let child = branch_with(0x44);

    let outcome = runtime
        .branch(&branch_request(
            child,
            BranchAction::ForkAtVersion {
                source: branch(),
                version: first.commit_version(),
            },
        ))
        .expect("fork at retained version");

    assert_eq!(outcome.fork_version(), Some(first.commit_version()));
    assert_eq!(
        read_value(&runtime, child, b"history"),
        Some(b"one".to_vec())
    );
}

#[test]
fn branch_fork_at_retained_watermark_between_commits_succeeds() {
    let mut runtime = open_runtime();
    put_at(&mut runtime, branch(), b"history-gap", b"one", 10);
    let other = branch_with(0x55);
    runtime
        .branch(&create_request(other))
        .expect("create other");
    put_at(&mut runtime, other, b"other", b"two", 20);
    put_at(&mut runtime, branch(), b"history-gap", b"three", 30);
    let child = branch_with(0x56);

    let outcome = runtime
        .branch(&branch_request(
            child,
            BranchAction::ForkAtVersion {
                source: branch(),
                version: CommitVersion::new(2),
            },
        ))
        .expect("fork at retained watermark");

    assert_eq!(outcome.fork_version(), Some(CommitVersion::new(2)));
    assert_eq!(
        read_value(&runtime, child, b"history-gap"),
        Some(b"one".to_vec())
    );
}

#[test]
fn branch_fork_at_unretained_version_rejects() {
    let mut runtime = open_runtime();
    put_at(&mut runtime, branch(), b"history", b"one", 10);

    let error = runtime
        .branch(&branch_request(
            branch_with(0x45),
            BranchAction::ForkAtVersion {
                source: branch(),
                version: CommitVersion::new(99),
            },
        ))
        .expect_err("unretained version rejected");

    assert_eq!(error.class(), StorageApiErrorClass::HistoryUnavailable);
}

#[test]
fn branch_fork_from_empty_source_creates_empty_parented_child() {
    // #2521: forking a history-less source is the legitimate empty-fork case
    // — an empty child at version zero with parent linkage intact. The old
    // rejection forced the engine into a silent `create_branch` fallback
    // that produced an UNPARENTED child (the silent-data-loss half of the
    // fork-of-a-fork regression).
    let runtime = open_runtime();

    let outcome = runtime
        .branch(&branch_request(
            branch_with(0x5a),
            BranchAction::ForkCurrent { source: branch() },
        ))
        .expect("empty source forks at version zero");
    let child = outcome.branches().first().expect("forked child");
    let parent = child.parent().expect("parent linkage survives");
    assert_eq!(parent.source_branch_id(), branch());
    assert_eq!(parent.fork_version(), CommitVersion::ZERO);
}

#[test]
fn branch_fork_invalid_source_identifier_rejects() {
    let runtime = open_runtime();
    let zero = BranchId::from_bytes([0; BranchId::BYTE_LEN]);

    let error = runtime
        .branch(&branch_request(
            branch_with(0x57),
            BranchAction::ForkCurrent { source: zero },
        ))
        .expect_err("zero source branch rejected");

    assert_eq!(error.class(), StorageApiErrorClass::InvalidArgument);
    assert_eq!(error.code(), "invalid_argument.storage_api.argument");
}

#[test]
fn branch_fork_at_timestamp_resolves_timeline() {
    let mut runtime = open_runtime();
    let first = put_at(&mut runtime, branch(), b"timed", b"one", 10);
    put_at(&mut runtime, branch(), b"timed", b"two", 30);
    let child = branch_with(0x46);

    let outcome = runtime
        .branch(&branch_request(
            child,
            BranchAction::ForkAtTimestamp {
                source: branch(),
                timestamp: Timestamp::from_micros(20),
            },
        ))
        .expect("fork at timestamp");

    assert_eq!(outcome.fork_version(), Some(first.commit_version()));
    assert_eq!(outcome.fork_timestamp(), Some(Timestamp::from_micros(20)));
    assert_eq!(read_value(&runtime, child, b"timed"), Some(b"one".to_vec()));
}

#[test]
fn branch_fork_at_unretained_timestamp_rejects() {
    let mut runtime = open_runtime();
    put_at(&mut runtime, branch(), b"timed", b"one", 10);

    let error = runtime
        .branch(&branch_request(
            branch_with(0x47),
            BranchAction::ForkAtTimestamp {
                source: branch(),
                timestamp: Timestamp::from_micros(5),
            },
        ))
        .expect_err("unretained timestamp rejected");

    assert_eq!(error.class(), StorageApiErrorClass::HistoryUnavailable);
}

#[test]
fn branch_fork_generation_mismatch_rejects() {
    let mut runtime = open_runtime();
    put_at(&mut runtime, branch(), b"history", b"one", 10);
    let child = branch_with(0x48);
    runtime
        .branch(&branch_request(
            child,
            BranchAction::ForkCurrent { source: branch() },
        ))
        .expect("initial fork");
    runtime
        .branch(&branch_request(child, BranchAction::Delete))
        .expect("delete destination");

    let error = runtime
        .branch(&BranchRequest::new(
            child,
            BranchAction::ForkCurrent { source: branch() },
            Some(BranchGeneration::new(1)),
        ))
        .expect_err("destination generation mismatch rejected");

    assert_eq!(
        error.code(),
        "failed_precondition.storage_api.branch_generation"
    );
}

#[test]
fn branch_fork_after_close_rejects() {
    let mut runtime = open_runtime();
    runtime.close().expect("close");

    let error = runtime
        .branch(&branch_request(
            branch_with(0x49),
            BranchAction::ForkCurrent { source: branch() },
        ))
        .expect_err("closed runtime rejected");

    assert_eq!(error.class(), StorageApiErrorClass::FailedPrecondition);
}

#[test]
fn branch_clear_removes_visible_rows() {
    let mut runtime = open_runtime();
    put_at(&mut runtime, branch(), b"clear", b"value", 10);

    let outcome = runtime
        .branch(&branch_request(branch(), BranchAction::Clear))
        .expect("clear");

    assert_eq!(outcome.operation(), BranchOperation::Cleared);
    assert_eq!(read_value(&runtime, branch(), b"clear"), None);
}

#[test]
fn branch_clear_preserves_branch_identity() {
    let mut runtime = open_runtime();
    put_at(&mut runtime, branch(), b"clear", b"value", 10);

    let outcome = runtime
        .branch(&branch_request(branch(), BranchAction::Clear))
        .expect("clear");
    let summary = outcome.branch().expect("cleared branch");

    assert_eq!(summary.branch_id(), branch());
    assert_eq!(summary.status(), BranchStatus::Active);
    assert_eq!(summary.generation(), BranchGeneration::new(1));
}

#[test]
fn branch_clear_generation_mismatch_rejects() {
    let runtime = open_runtime();

    let error = runtime
        .branch(&BranchRequest::new(
            branch(),
            BranchAction::Clear,
            Some(BranchGeneration::new(2)),
        ))
        .expect_err("generation mismatch rejected");

    assert_eq!(
        error.code(),
        "failed_precondition.storage_api.branch_generation"
    );
}

#[test]
fn branch_clear_with_pinned_view_reports_protected_release() {
    let mut runtime = open_runtime();
    put_at(&mut runtime, branch(), b"pinned", b"value", 10);
    runtime
        .flush_default_branch_for_test()
        .expect("flush table");
    runtime
        .pin_branch_reachability_for_test(branch())
        .expect("pin reachability");

    let outcome = runtime
        .branch(&branch_request(branch(), BranchAction::Clear))
        .expect("clear with pinned reachability");
    let cleanup = outcome.cleanup().expect("cleanup");

    assert_eq!(outcome.operation(), BranchOperation::Cleared);
    assert_eq!(read_value(&runtime, branch(), b"pinned"), None);
    assert_eq!(cleanup.releasable_tables(), 0);
    assert!(
        cleanup.protected_tables() > 0,
        "pinned reachability must block table release"
    );
}

#[test]
fn branch_delete_removes_from_list() {
    let runtime = open_runtime();
    let child = branch_with(0x4a);
    runtime.branch(&create_request(child)).expect("create");

    runtime
        .branch(&branch_request(child, BranchAction::Delete))
        .expect("delete");
    let branches = runtime.branch(&list_request()).expect("list");

    assert!(!branches
        .branches()
        .iter()
        .any(|summary| summary.branch_id() == child));
}

#[test]
fn branch_delete_generation_mismatch_rejects() {
    let runtime = open_runtime();
    let child = branch_with(0x4b);
    runtime.branch(&create_request(child)).expect("create");

    let error = runtime
        .branch(&BranchRequest::new(
            child,
            BranchAction::Delete,
            Some(BranchGeneration::new(2)),
        ))
        .expect_err("generation mismatch rejected");

    assert_eq!(
        error.code(),
        "failed_precondition.storage_api.branch_generation"
    );
}

#[test]
fn branch_delete_with_pinned_view_reports_protected_release() {
    let mut runtime = open_runtime();
    put_at(&mut runtime, branch(), b"pinned-delete", b"value", 10);
    runtime
        .flush_default_branch_for_test()
        .expect("flush table");
    runtime
        .pin_branch_reachability_for_test(branch())
        .expect("pin reachability");
    runtime
        .branch(&create_request(branch_with(0x4c)))
        .expect("create remaining active branch");

    let outcome = runtime
        .branch(&branch_request(branch(), BranchAction::Delete))
        .expect("delete with pinned reachability");
    let cleanup = outcome.cleanup().expect("cleanup");

    assert_eq!(outcome.operation(), BranchOperation::Deleted);
    assert_eq!(cleanup.releasable_tables(), 0);
    assert!(
        cleanup.protected_tables() > 0,
        "pinned reachability must block table release"
    );
}

#[test]
fn branch_recreate_deleted_reports_generation_transition() {
    let runtime = open_runtime();
    let child = branch_with(0x58);
    runtime.branch(&create_request(child)).expect("create");
    runtime
        .branch(&branch_request(child, BranchAction::Delete))
        .expect("delete");

    let outcome = runtime
        .branch(&BranchRequest::new(
            child,
            BranchAction::Create,
            Some(BranchGeneration::new(2)),
        ))
        .expect("recreate deleted branch");
    let summary = outcome.branch().expect("branch summary");

    assert_eq!(outcome.operation(), BranchOperation::Created);
    assert_eq!(outcome.generation_before(), Some(BranchGeneration::new(1)));
    assert_eq!(outcome.generation_after(), Some(BranchGeneration::new(2)));
    assert_eq!(summary.status(), BranchStatus::Active);
    assert_eq!(summary.generation(), BranchGeneration::new(2));
}

#[cfg(feature = "localfs")]
#[test]
fn durable_branch_catalog_round_trips_after_reopen() {
    let root = temp_dir_for_api_test("branch-durable-roundtrip");
    let backend = StorageBackend::local_fs(root.clone());
    let mut runtime = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
        &backend,
    )
    .expect("durable open")
    .into_runtime();
    put_at(&mut runtime, branch(), b"durable-branch", b"parent", 10);
    let child = branch_with(0x59);
    runtime
        .branch(&branch_request(
            child,
            BranchAction::ForkCurrent { source: branch() },
        ))
        .expect("fork durable branch");
    runtime.close().expect("close durable runtime");
    drop(runtime);

    let backend = StorageBackend::local_fs(root);
    let runtime = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
        &backend,
    )
    .expect("durable reopen")
    .into_runtime();
    let described = runtime
        .branch(&describe_request(child))
        .expect("describe recovered branch")
        .branch()
        .expect("branch summary");

    assert_eq!(described.status(), BranchStatus::Active);
    assert_eq!(
        described
            .parent()
            .map(BranchParentSummary::source_branch_id),
        Some(branch())
    );
    assert_eq!(
        read_value(&runtime, child, b"durable-branch"),
        Some(b"parent".to_vec())
    );
}

/// Fork-manifest fix: a `ForkCurrent` child of a FLUSHED parent is a COW fork (inherited layers,
/// no row copies), and the fork now publishes the child's table manifest at fork time — so after
/// reopen the child reads the parent's rows through manifest-recovered layers, not through the
/// O(parent dataset) `rebuild_fork_snapshot_rows` fallback. Complements the unflushed variant
/// above (whose eager child still recovers through the gated fallback).
#[cfg(feature = "localfs")]
#[test]
fn durable_flushed_parent_cow_fork_round_trips_after_reopen() {
    let root = temp_dir_for_api_test("branch-durable-cow-fork-roundtrip");
    let child = branch_with(0x5a);
    {
        let backend = StorageBackend::local_fs(root.clone());
        let mut runtime = StorageRuntime::open_with_backend(
            StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
            &backend,
        )
        .expect("durable open")
        .into_runtime();
        put_at(&mut runtime, branch(), b"cow-fork", b"parent", 10);
        runtime
            .flush_default_branch_for_test()
            .expect("flush the parent so the fork is COW");
        runtime
            .branch(&branch_request(
                child,
                BranchAction::ForkCurrent { source: branch() },
            ))
            .expect("fork the flushed parent");
        runtime.close().expect("close durable runtime");
    }

    let backend = StorageBackend::local_fs(root);
    let runtime = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
        &backend,
    )
    .expect("durable reopen")
    .into_runtime();
    let described = runtime
        .branch(&describe_request(child))
        .expect("describe recovered branch")
        .branch()
        .expect("branch summary");
    assert_eq!(described.status(), BranchStatus::Active);
    assert_eq!(
        described
            .parent()
            .map(BranchParentSummary::source_branch_id),
        Some(branch())
    );
    assert_eq!(
        read_value(&runtime, child, b"cow-fork"),
        Some(b"parent".to_vec()),
        "the child must read the parent's row through its manifest-recovered inherited layer",
    );
}

/// Fork-manifest fix enabler: fork-time child manifests interleave manifest sequences across
/// branches (parent seq → child seq → parent seq), while recovery applies manifests in branch-id
/// order — `record_recovered_manifest` must tolerate the reordering (the strict runtime
/// regression check would fail recovery here).
#[cfg(feature = "localfs")]
#[test]
fn durable_interleaved_branch_manifest_sequences_recover() {
    let root = temp_dir_for_api_test("branch-durable-interleaved-manifests");
    let child = branch_with(0x5c);
    {
        let backend = StorageBackend::local_fs(root.clone());
        let mut runtime = StorageRuntime::open_with_backend(
            StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
            &backend,
        )
        .expect("durable open")
        .into_runtime();
        // Parent manifest (seq A) → child manifest at fork (seq B > A) → parent manifest again
        // (seq C > B). Recovery loads them in branch-id order, so a strict sequence check would
        // see C then B and refuse.
        put_at(&mut runtime, branch(), b"interleaved-a", b"first", 10);
        runtime
            .flush_default_branch_for_test()
            .expect("first parent flush publishes the parent manifest");
        runtime
            .branch(&branch_request(
                child,
                BranchAction::ForkCurrent { source: branch() },
            ))
            .expect("fork publishes the child manifest");
        put_at(&mut runtime, branch(), b"interleaved-b", b"second", 20);
        runtime
            .flush_default_branch_for_test()
            .expect("second parent flush republishes the parent manifest");
        runtime.close().expect("close durable runtime");
    }

    let backend = StorageBackend::local_fs(root);
    let runtime = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
        &backend,
    )
    .expect("durable reopen with interleaved manifest sequences")
    .into_runtime();
    assert_eq!(
        read_value(&runtime, branch(), b"interleaved-b"),
        Some(b"second".to_vec())
    );
    assert_eq!(
        read_value(&runtime, child, b"interleaved-a"),
        Some(b"first".to_vec()),
        "the child sees pre-fork parent rows, not post-fork ones",
    );
    assert_eq!(
        read_value(&runtime, child, b"interleaved-b"),
        None,
        "post-fork parent writes must not leak into the child",
    );
}

/// Fork-manifest fix crash window: the fork's catalog publish landed but its child-manifest
/// publish did not (simulated by deleting the child's manifest object). Reopen must still succeed
/// and the child must read the parent's rows — via the narrowly-kept `rebuild_fork_snapshot_rows`
/// fallback for layer-less children.
#[cfg(feature = "localfs")]
#[test]
fn durable_fork_child_manifest_crash_window_recovers_via_rebuild() {
    let root = temp_dir_for_api_test("branch-durable-fork-crash-window");
    let child = branch_with(0x5d);
    {
        let backend = StorageBackend::local_fs(root.clone());
        let mut runtime = StorageRuntime::open_with_backend(
            StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
            &backend,
        )
        .expect("durable open")
        .into_runtime();
        put_at(&mut runtime, branch(), b"crash-window", b"parent", 10);
        runtime
            .flush_default_branch_for_test()
            .expect("flush the parent so the fork is COW");
        runtime
            .branch(&branch_request(
                child,
                BranchAction::ForkCurrent { source: branch() },
            ))
            .expect("fork the flushed parent");
        runtime.close().expect("close durable runtime");
    }

    // Simulate the crash window: the child's fork-time table manifest never became durable.
    // (`.object@` is the localfs backend's on-disk object-file suffix.)
    let child_manifest = root
        .join("tables")
        .join(child.to_string())
        .join("manifest.object@");
    assert!(
        child_manifest.is_file(),
        "the fork must have published the child's table manifest at {}",
        child_manifest.display()
    );
    std::fs::remove_file(&child_manifest).expect("delete the child's table manifest");

    let backend = StorageBackend::local_fs(root);
    let runtime = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
        &backend,
    )
    .expect("durable reopen without the child manifest")
    .into_runtime();
    assert_eq!(
        read_value(&runtime, child, b"crash-window"),
        Some(b"parent".to_vec()),
        "a layer-less child must recover its fork view through the rebuild fallback",
    );
}

/// A layer-less fork child's reopen rebuild runs on EVERY reopen, but its inherited rows can
/// already be durable in the child's OWNED tables — a previous reopen's rebuild followed by a
/// flush seals them. Re-materializing those rows again puts a second copy into the memtable; the
/// next flush seals a second durable copy of the same internal key, and the next compaction of
/// the child's tables fails loudly with `DuplicateInternalKey`. The rebuild must elide rows that
/// are already present anywhere in the child's recovered state, not just in its active memtable.
#[cfg(feature = "localfs")]
#[test]
fn durable_layerless_fork_rebuild_elides_rows_already_flushed_durable() {
    use crate::api::{
        MaintenanceRequest, MaintenanceScope, MaintenanceSummaryStatus, MaintenanceTask,
    };

    let root = temp_dir_for_api_test("branch-durable-rebuild-elision");
    let child = branch_with(0x5e);
    {
        let backend = StorageBackend::local_fs(root.clone());
        let mut runtime = StorageRuntime::open_with_backend(
            StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
            &backend,
        )
        .expect("durable open")
        .into_runtime();
        put_at(&mut runtime, branch(), b"rebuild-a", b"parent-a", 10);
        put_at(&mut runtime, branch(), b"rebuild-b", b"parent-b", 20);
        // No parent flush: the volatile source makes the fork eager, so the child stays
        // layer-less and recovers through the rebuild fallback on every reopen.
        runtime
            .branch(&branch_request(
                child,
                BranchAction::ForkCurrent { source: branch() },
            ))
            .expect("fork the unflushed parent");
        // The child's own commit makes the first flush's content differ from the second
        // reopen's re-materialized slice — without it, the redundant re-flush would collapse
        // into the first table's content-derived identity and hide the duplicate.
        put_at(&mut runtime, child, b"rebuild-own", b"child", 30);
        runtime.close().expect("close durable runtime");
    }
    {
        // Reopen #1: the rebuild re-materializes the parent's rows into the child's memtable.
        // Flushing the child seals that inherited content into a durable owned table.
        let backend = StorageBackend::local_fs(root.clone());
        let mut runtime = StorageRuntime::open_with_backend(
            StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
            &backend,
        )
        .expect("first durable reopen")
        .into_runtime();
        runtime
            .enqueue_maintenance(&MaintenanceRequest::new(
                MaintenanceTask::Flush,
                MaintenanceScope::Branch(child),
            ))
            .expect("enqueue child flush");
        runtime
            .drain_maintenance()
            .expect("flush the child's rebuilt rows into a durable owned table");
        runtime.close().expect("close after child flush");
    }

    // Reopen #2: the child is still layer-less (owned tables, no inherited layers), so the
    // rebuild runs again — it must elide the rows the first flush already made durable.
    let backend = StorageBackend::local_fs(root);
    let mut runtime = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
        &backend,
    )
    .expect("second durable reopen")
    .into_runtime();
    runtime
        .enqueue_maintenance(&MaintenanceRequest::new(
            MaintenanceTask::Flush,
            MaintenanceScope::Branch(child),
        ))
        .expect("enqueue second child flush");
    runtime
        .enqueue_maintenance(&MaintenanceRequest::new(
            MaintenanceTask::Compact,
            MaintenanceScope::Branch(child),
        ))
        .expect("enqueue child compaction");
    let drain = runtime
        .drain_maintenance()
        .expect("maintenance drain must run to completion");
    for outcome in drain.outcomes() {
        assert_ne!(
            outcome.status(),
            MaintenanceSummaryStatus::Failed,
            "maintenance must not fail after fork-rebuild reopen cycles: task {:?} reported {:?}",
            outcome.task(),
            outcome.source_error_code(),
        );
    }
    // The best-effort chained compaction reports through the failure ring, not the drain
    // outcomes — the ring must stay silent (the whole-DB sim's maintenance oracle).
    let status = runtime.maintenance_status().expect("maintenance status");
    assert!(
        status.recent_failures().is_empty(),
        "the maintenance failure ring must stay silent after fork-rebuild reopen cycles: {:?}",
        status.recent_failures(),
    );
    assert_eq!(
        read_value(&runtime, child, b"rebuild-a"),
        Some(b"parent-a".to_vec()),
        "the child keeps its inherited rows across reopen/flush/compact cycles",
    );
    assert_eq!(
        read_value(&runtime, child, b"rebuild-b"),
        Some(b"parent-b".to_vec()),
        "the child keeps its inherited rows across reopen/flush/compact cycles",
    );
    assert_eq!(
        read_value(&runtime, child, b"rebuild-own"),
        Some(b"child".to_vec()),
        "the child keeps its own post-fork row across reopen/flush/compact cycles",
    );
}

/// A checkpoint over a snapshot-recovered base must stay self-contained. Reopening from a
/// checkpoint (with no table manifest) installs the snapshot's rows as a VOLATILE owned
/// table — no durable catalog entry backs it. A later checkpoint must capture those rows
/// (they have no durable home outside the superseded snapshot) and must not record the
/// volatile table as a flushed base; deltaing over it and truncating the WAL durably
/// loses the rows, and the next recovery either gaps or (correctly) refuses the orphaned
/// delta and recovers an empty prefix.
#[cfg(feature = "localfs")]
#[test]
fn checkpoint_over_a_snapshot_recovered_base_stays_self_contained() {
    use crate::api::{MaintenanceRequest, MaintenanceScope, MaintenanceTask};

    let root = temp_dir_for_api_test("branch-durable-checkpoint-volatile-base");
    {
        let backend = StorageBackend::local_fs(root.clone());
        let mut runtime = StorageRuntime::open_with_backend(
            StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
            &backend,
        )
        .expect("durable open")
        .into_runtime();
        put_at(&mut runtime, branch(), b"ckpt-a", b"one", 10);
        put_at(&mut runtime, branch(), b"ckpt-b", b"two", 20);
        runtime
            .enqueue_maintenance(&MaintenanceRequest::new(
                MaintenanceTask::Checkpoint,
                MaintenanceScope::Global,
            ))
            .expect("enqueue first checkpoint");
        runtime.drain_maintenance().expect("first checkpoint");
        runtime.close().expect("first close");
    }
    {
        // Reopen #1: no table manifest exists, so the snapshot's rows install as a
        // volatile owned table. Commit one more row and checkpoint again — the new
        // snapshot must carry ALL three rows' content.
        let backend = StorageBackend::local_fs(root.clone());
        let mut runtime = StorageRuntime::open_with_backend(
            StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
            &backend,
        )
        .expect("first durable reopen")
        .into_runtime();
        put_at(&mut runtime, branch(), b"ckpt-c", b"three", 30);
        runtime
            .enqueue_maintenance(&MaintenanceRequest::new(
                MaintenanceTask::Checkpoint,
                MaintenanceScope::Global,
            ))
            .expect("enqueue second checkpoint");
        runtime
            .drain_maintenance()
            .expect("second checkpoint + reclaim");
        runtime.close().expect("second close");
    }

    let backend = StorageBackend::local_fs(root);
    let runtime = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
        &backend,
    )
    .expect("second durable reopen")
    .into_runtime();
    assert_eq!(
        read_value(&runtime, branch(), b"ckpt-a"),
        Some(b"one".to_vec()),
        "snapshot-recovered rows must survive the next checkpoint cycle",
    );
    assert_eq!(
        read_value(&runtime, branch(), b"ckpt-b"),
        Some(b"two".to_vec()),
        "snapshot-recovered rows must survive the next checkpoint cycle",
    );
    assert_eq!(
        read_value(&runtime, branch(), b"ckpt-c"),
        Some(b"three".to_vec()),
        "the delta row must survive alongside the recovered base",
    );
}

/// Did a drain round make progress, or is the queue genuinely churning?
///
/// Two things count as progress, and the second is the one #2953 was missing:
///
/// - the queue shrank, or
/// - a task is in flight. An active task cannot shrink the queue until it
///   finishes, and while it holds its lane `next_startable_task_index` filters
///   out every queued task sharing that lane — so `drain_maintenance` returns
///   immediately having started nothing. That is waiting, not churn.
///
/// Pure so it can be truth-tabled directly; the drain loop reads its verdict.
const fn drain_round_made_progress(
    pending: usize,
    previous: usize,
    active_task: Option<u64>,
) -> bool {
    pending < previous || active_task.is_some()
}

#[test]
fn a_drain_round_counts_an_in_flight_task_as_progress() {
    // Shrinking is progress whether or not anything is active.
    assert!(drain_round_made_progress(1, 2, None));
    assert!(drain_round_made_progress(1, 2, Some(7)));

    // #2953: not shrinking is NOT churn while a task is in flight — it holds
    // its lane, so every queued task sharing that lane is unstartable.
    assert!(drain_round_made_progress(1, 1, Some(7)));
    assert!(drain_round_made_progress(2, 1, Some(7)));

    // Neither shrinking nor waiting on anything: this is the churn the
    // assertion exists to catch, and it must still be caught.
    assert!(!drain_round_made_progress(1, 1, None));
    assert!(!drain_round_made_progress(2, 1, None));
}

/// A branch-scoped compaction task legally races a branch delete: enqueued while the
/// branch was live, drained after it was deleted. The stale task's target is gone —
/// the drain must consume it as Canceled, not fail the drain and not record a
/// maintenance failure: the branch's tables are reclaimed by delete cleanup, so the
/// task's work is vacuously complete. (A Deferred task would be wrong too: a re-created
/// name shares the branch id under a new generation, and the stale task must never run
/// against it.)
#[cfg(feature = "localfs")]
#[test]
fn drain_cancels_branch_scoped_compaction_enqueued_before_the_branch_was_deleted() {
    use crate::api::{
        MaintenanceRequest, MaintenanceScope, MaintenanceSummaryStatus, MaintenanceTask,
    };

    let root = temp_dir_for_api_test("branch-drain-cancel-deleted-scope");
    let backend = StorageBackend::local_fs(root);
    let mut runtime = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
        &backend,
    )
    .expect("durable open")
    .into_runtime();
    let victim = branch_with(0x5f);
    runtime.branch(&create_request(victim)).expect("create");
    // Two flushed tables make the branch a genuine compaction candidate before the race.
    put_at(&mut runtime, victim, b"cancel-a", b"row", 10);
    runtime
        .enqueue_maintenance(&MaintenanceRequest::new(
            MaintenanceTask::Flush,
            MaintenanceScope::Branch(victim),
        ))
        .expect("enqueue first flush");
    runtime.drain_maintenance().expect("first flush");
    put_at(&mut runtime, victim, b"cancel-b", b"row", 20);
    runtime
        .enqueue_maintenance(&MaintenanceRequest::new(
            MaintenanceTask::Flush,
            MaintenanceScope::Branch(victim),
        ))
        .expect("enqueue second flush");
    runtime.drain_maintenance().expect("second flush");

    // Put an unflushed row in the memtable, then enqueue BOTH branch-scoped task kinds
    // the race can strand: a flush and a compaction. Both must cancel after the delete.
    put_at(&mut runtime, victim, b"cancel-c", b"row", 30);
    runtime
        .enqueue_maintenance(&MaintenanceRequest::new(
            MaintenanceTask::Flush,
            MaintenanceScope::Branch(victim),
        ))
        .expect("enqueue flush while the branch is live");
    runtime
        .enqueue_maintenance(&MaintenanceRequest::new(
            MaintenanceTask::Compact,
            MaintenanceScope::Branch(victim),
        ))
        .expect("enqueue compaction while the branch is live");
    runtime
        .branch(&branch_request(victim, BranchAction::Delete))
        .expect("delete the branch after the tasks were enqueued");

    // #2867: a drain's background rounds may chain follow-up enqueues (e.g. the
    // table-object retention that follows a completed task) that land after the
    // drain's queue snapshot — drain to a fixed point before judging the queue.
    // Every round still requires no Failed outcome and a silent failure ring.
    // #3182: the round count used to be a flat 5, which conflated "still
    // draining" with "churning" — the two states the assertion below claims to
    // tell apart. A drain that was legitimately still making progress on its
    // fifth round failed the test. Drain to an actual fixed point instead:
    // keep going while the queue is shrinking, and stop early only when it
    // stops shrinking, which is the churn this is meant to catch. The outer
    // bound is a safety net against an infinite loop, not the real condition.
    //
    // #2953: shrinking is not the only form of progress. A queued task whose
    // lane is already occupied is pending-but-UNSTARTABLE:
    // `next_startable_task_index` filters it out, `run_next_matching` returns
    // `None`, and `drain_maintenance` therefore returns having done nothing
    // while the queue is still non-empty. The count does not fall, and three
    // such rounds used to be declared churn — which is why raising the round
    // count twice (#2868, #3209) never fixed this: the blocker is a held lane,
    // not elapsed time. An in-flight task is progress we cannot see in the
    // queue length, so it is not churn.
    let safety_rounds = 64;
    let stalled_rounds_before_giving_up = 3;
    let mut pending = usize::MAX;
    let mut previous = usize::MAX;
    let mut stalled = 0;
    for _ in 0..safety_rounds {
        let drain = runtime
            .drain_maintenance()
            .expect("draining a stale branch-scoped task must not fail the drain");
        for outcome in drain.outcomes() {
            assert_ne!(
                outcome.status(),
                MaintenanceSummaryStatus::Failed,
                "a deleted task target is a legal race, not a maintenance failure: task {:?} reported {:?}",
                outcome.task(),
                outcome.source_error_code(),
            );
        }
        let status = runtime.maintenance_status().expect("maintenance status");
        assert!(
            status.recent_failures().is_empty(),
            "the failure ring must stay silent for the enqueue/delete race: {:?}",
            status.recent_failures(),
        );
        pending = status.pending_tasks();
        if pending == 0 {
            break;
        }
        // Shrinking is progress; so is a task in flight, which cannot shrink
        // the queue until it finishes. Only a queue that is neither shrinking
        // nor waiting on anything is churning.
        if drain_round_made_progress(pending, previous, status.active_task()) {
            stalled = 0;
        } else {
            stalled += 1;
            if stalled >= stalled_rounds_before_giving_up {
                break;
            }
        }
        previous = pending;
    }
    assert_eq!(
        pending, 0,
        "the queue must drain to a fixed point: the stale tasks are consumed, \
         not churning. {pending} task(s) still queued after the count stopped \
         falling, so this is churn rather than slow progress",
    );
}

#[cfg(feature = "localfs")]
#[test]
fn durable_branch_delete_allows_reopen_after_process_drop() {
    let root = temp_dir_for_api_test("branch-durable-delete-reopen");
    let backend = StorageBackend::local_fs(root.clone());
    let child = branch_with(0x5a);
    let mut runtime = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
        &backend,
    )
    .expect("durable open")
    .into_runtime();
    runtime
        .branch(&create_request(child))
        .expect("create branch");
    put_at(&mut runtime, child, b"deleted-branch-row", b"value", 10);
    runtime
        .branch(&branch_request(child, BranchAction::Delete))
        .expect("delete branch");
    drop(runtime);

    let backend = StorageBackend::local_fs(root);
    let runtime = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
        &backend,
    )
    .expect("durable reopen after branch delete")
    .into_runtime();
    let described = runtime
        .branch(&describe_request(child))
        .expect("describe deleted branch")
        .branch()
        .expect("branch summary");

    assert_eq!(described.status(), BranchStatus::Deleted);
}

#[test]
fn branch_delete_refused_while_layerless_fork_children_live() {
    // Durable: the refusal protects RECOVERY, so cache mode (no recovery)
    // deliberately keeps unrestricted deletes — the branch-DAG model pins
    // that. Replay is likewise exempt (a WAL'd delete already happened).
    let root = temp_dir_for_api_test("branch-layerless-parent-delete-refusal");
    let backend = StorageBackend::local_fs(root);
    let parent = branch_with(0x60);
    let child = branch_with(0x61);
    let mut runtime = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
        &backend,
    )
    .expect("durable open")
    .into_runtime();
    runtime
        .branch(&create_request(parent))
        .expect("create parent");
    let first = put_at(&mut runtime, parent, b"parent-row", b"one", 10);
    put_at(&mut runtime, parent, b"parent-row", b"two", 20);

    // A historical (eager) fork is layer-less: its materialized rows are not
    // WAL'd, so recovery re-materializes them from the parent's state.
    runtime
        .branch(&branch_request(
            child,
            BranchAction::ForkAtVersion {
                source: parent,
                version: first.commit_version(),
            },
        ))
        .expect("historical fork");

    // Deleting the source while such a child lives would arm a permanent
    // recovery failure — it must refuse.
    let error = runtime
        .branch(&branch_request(parent, BranchAction::Delete))
        .expect_err("deleting the layer-less fork's source must refuse");
    assert_eq!(
        error.code(),
        "failed_precondition.storage_api.branch_dependent_children",
        "the refusal must name the dependency, not the generic state class (#3196)"
    );

    // Direction control: once the dependent child is gone, the parent
    // delete proceeds.
    runtime
        .branch(&branch_request(child, BranchAction::Delete))
        .expect("delete child");
    runtime
        .branch(&branch_request(parent, BranchAction::Delete))
        .expect("delete parent after its children are gone");
}

#[test]
fn branch_delete_of_empty_fork_source_stays_allowed() {
    // A fork of a rowless parent re-materializes nothing at recovery: the
    // dependency check must consult the actual fork-visible rows (an
    // always-dependent fold would refuse here — the mutation the engine's
    // cache-mode DAG model cannot see).
    let root = temp_dir_for_api_test("branch-empty-fork-parent-delete");
    let backend = StorageBackend::local_fs(root);
    let parent = branch_with(0x66);
    let child = branch_with(0x67);
    let runtime = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
        &backend,
    )
    .expect("durable open")
    .into_runtime();
    runtime
        .branch(&create_request(parent))
        .expect("create parent");
    runtime
        .branch(&branch_request(
            child,
            BranchAction::ForkCurrent { source: parent },
        ))
        .expect("fork empty parent");
    runtime
        .branch(&branch_request(parent, BranchAction::Delete))
        .expect("an empty fork keeps its parent deletable");
}

#[test]
fn branch_delete_of_layered_fork_source_stays_allowed() {
    use crate::api::{MaintenanceRequest, MaintenanceScope, MaintenanceTask};

    // Durable, with the parent's rows flushed to owned tables BEFORE the
    // fork: the hybrid fork child then carries durably published inherited
    // layers, its recovery never dereferences the parent, and the parent
    // stays deletable — the exact boundary of the #2820 refusal. (A fork
    // from an UNFLUSHED parent copies unsealed rows that are not WAL'd:
    // that child is layer-less and correctly blocks the delete.)
    let root = temp_dir_for_api_test("branch-layered-parent-delete");
    let backend = StorageBackend::local_fs(root);
    let parent = branch_with(0x64);
    let child = branch_with(0x65);
    let mut runtime = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
            .with_maintenance_scheduling_policy(
                crate::api::StorageMaintenanceSchedulingPolicy::EvaluateAndEnqueue,
            ),
        &backend,
    )
    .expect("durable open")
    .into_runtime();
    runtime
        .branch(&create_request(parent))
        .expect("create parent");
    put_at(&mut runtime, parent, b"layered-row", b"one", 10);
    runtime
        .enqueue_maintenance(&MaintenanceRequest::new(
            MaintenanceTask::Flush,
            MaintenanceScope::Branch(parent),
        ))
        .expect("enqueue flush");
    runtime.drain_maintenance().expect("drain flush");

    runtime
        .branch(&branch_request(
            child,
            BranchAction::ForkCurrent { source: parent },
        ))
        .expect("hybrid fork of a flushed parent");
    runtime
        .branch(&branch_request(parent, BranchAction::Delete))
        .expect("layered child keeps its parent deletable");
    assert_eq!(
        read_value(&runtime, child, b"layered-row"),
        Some(b"one".to_vec()),
        "the child keeps serving inherited rows after the parent delete"
    );
}

#[test]
fn fork_parent_deletion_cannot_brick_recovery() {
    let root = temp_dir_for_api_test("branch-fork-parent-delete-recovery");
    let backend = StorageBackend::local_fs(root.clone());
    let parent = branch_with(0x62);
    let child = branch_with(0x63);
    {
        let mut runtime = StorageRuntime::open_with_backend(
            StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
            &backend,
        )
        .expect("durable open")
        .into_runtime();
        runtime
            .branch(&create_request(parent))
            .expect("create parent");
        let first = put_at(&mut runtime, parent, b"lineage", b"one", 10);
        put_at(&mut runtime, parent, b"lineage", b"two", 20);
        runtime
            .branch(&branch_request(
                child,
                BranchAction::ForkAtVersion {
                    source: parent,
                    version: first.commit_version(),
                },
            ))
            .expect("historical fork");

        // The refusal is what makes the store recoverable: an accepted
        // delete here left the child's recovery re-materialization with no
        // source and permanently bricked the reopen.
        let error = runtime
            .branch(&branch_request(parent, BranchAction::Delete))
            .expect_err("deleting the historical fork's source must refuse");
        assert_eq!(
            error.code(),
            "failed_precondition.storage_api.branch_dependent_children",
            "the refusal must name the dependency, not the generic state class (#3196)"
        );
    }

    let backend = StorageBackend::local_fs(root);
    let runtime = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
        &backend,
    )
    .expect("reopen after refused parent delete")
    .into_runtime();
    assert_eq!(
        read_value(&runtime, child, b"lineage"),
        Some(b"one".to_vec()),
        "the historical fork serves its fork-version state after reopen"
    );
}

#[test]
fn branch_delete_unknown_rejects() {
    let runtime = open_runtime();

    let error = runtime
        .branch(&branch_request(branch_with(0x4d), BranchAction::Delete))
        .expect_err("unknown branch rejected");

    assert_eq!(error.class(), StorageApiErrorClass::NotFound);
}

#[test]
fn branch_delete_already_deleted_rejects() {
    let runtime = open_runtime();
    let child = branch_with(0x5b);
    runtime.branch(&create_request(child)).expect("create");
    runtime
        .branch(&branch_request(child, BranchAction::Delete))
        .expect("delete");

    let error = runtime
        .branch(&branch_request(child, BranchAction::Delete))
        .expect_err("deleted branch rejected");

    assert_eq!(error.class(), StorageApiErrorClass::FailedPrecondition);
    assert_eq!(error.code(), "failed_precondition.storage_api.state");
}

#[test]
fn branch_delete_reports_cleanup_facts() {
    let runtime = open_runtime();
    let child = branch_with(0x4e);
    runtime.branch(&create_request(child)).expect("create");

    let outcome = runtime
        .branch(&branch_request(child, BranchAction::Delete))
        .expect("delete");
    let cleanup = outcome.cleanup().expect("cleanup facts");

    assert_eq!(outcome.operation(), BranchOperation::Deleted);
    assert_eq!(cleanup.removed_refs(), 0);
    assert_eq!(cleanup.releasable_tables(), 0);
    assert_eq!(cleanup.protected_tables(), 0);
}

#[test]
fn branch_delete_last_required_branch_rejects() {
    let runtime = open_runtime();

    let error = runtime
        .branch(&branch_request(branch(), BranchAction::Delete))
        .expect_err("last branch delete rejected");

    assert_eq!(error.class(), StorageApiErrorClass::FailedPrecondition);
}

fn branch_api_source() -> String {
    [include_str!("../branch.rs"), super::RUNTIME_SOURCE]
        .join("\n")
        .to_ascii_lowercase()
}

#[test]
fn branch_api_has_no_merge_method() {
    let source = branch_api_source();

    assert!(!source.contains("merge"));
}

#[test]
fn branch_api_has_no_cherry_pick_method() {
    let source = branch_api_source();

    assert!(!source.contains("cherry"));
}

#[test]
fn branch_api_has_no_revert_method() {
    let source = branch_api_source();

    assert!(!source.contains("revert"));
}

#[test]
fn branch_api_has_no_restore_method() {
    let source = branch_api_source();

    assert!(!source.contains("restore"));
}

#[test]
fn branch_api_has_no_publish_review_method() {
    let source = branch_api_source();

    assert!(!source.contains("pub fn publish"));
    assert!(!source.contains("pub fn review"));
}

/// #2826: after delete + re-fork of the same branch id, recovery must not
/// resurrect the dead predecessor generation's rows into the new fork. The
/// ghost commit is deliberately the LAST global commit before the fork, so
/// the fence's `<=` boundary (record version == visible-at-fork) is exactly
/// what this test exercises; the fresh post-fork commit and the inherited
/// source row pin both legal directions.
#[test]
fn fork_onto_deleted_name_does_not_resurrect_predecessor_rows_after_reopen() {
    let root = temp_dir_for_api_test("branch-fork-generation-fence");
    let backend = StorageBackend::local_fs(root.clone());
    let source = branch_with(0x71);
    let victim = branch_with(0x70);
    {
        let mut runtime = StorageRuntime::open_with_backend(
            StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
            &backend,
        )
        .expect("durable open")
        .into_runtime();
        runtime
            .branch(&create_request(source))
            .expect("create source");
        put_at(&mut runtime, source, b"base", b"inherited", 10);
        runtime
            .branch(&create_request(victim))
            .expect("create victim generation 1");
        put_at(&mut runtime, victim, b"ghost", b"dead", 20);
        runtime
            .branch(&branch_request(victim, BranchAction::Delete))
            .expect("delete victim generation 1");
        runtime
            .branch(&BranchRequest::new(
                victim,
                BranchAction::ForkCurrent { source },
                Some(BranchGeneration::new(2)),
            ))
            .expect("re-fork the deleted name as generation 2");
        assert_eq!(
            read_value(&runtime, victim, b"ghost"),
            None,
            "live runtime already fences the dead generation"
        );
        put_at(&mut runtime, victim, b"fresh", b"alive", 30);
    }

    let runtime = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
        &backend,
    )
    .expect("reopen")
    .into_runtime();
    assert_eq!(
        read_value(&runtime, victim, b"ghost"),
        None,
        "recovery must not resurrect the deleted generation's row"
    );
    assert_eq!(
        read_value(&runtime, victim, b"base"),
        Some(b"inherited".to_vec()),
        "the fork keeps serving its inherited source row after reopen"
    );
    assert_eq!(
        read_value(&runtime, victim, b"fresh"),
        Some(b"alive".to_vec()),
        "the new generation's own commit survives reopen"
    );
}

/// #2826 (recreate direction): a fresh empty re-creation of a deleted name
/// must stay empty across reopen — the predecessor's WAL records belong to
/// a dead generation.
#[test]
fn recreate_after_delete_does_not_resurrect_predecessor_rows_after_reopen() {
    let root = temp_dir_for_api_test("branch-recreate-generation-fence");
    let backend = StorageBackend::local_fs(root.clone());
    let victim = branch_with(0x72);
    {
        let mut runtime = StorageRuntime::open_with_backend(
            StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
            &backend,
        )
        .expect("durable open")
        .into_runtime();
        runtime
            .branch(&create_request(victim))
            .expect("create victim generation 1");
        // Two ghost commits: the earlier one sits STRICTLY below the
        // recreate point and the later one exactly AT it, so the fence's
        // `<` and `==` arms are both load-bearing here.
        put_at(&mut runtime, victim, b"ghost", b"dead", 10);
        put_at(&mut runtime, victim, b"ghost-two", b"dead-too", 15);
        runtime
            .branch(&branch_request(victim, BranchAction::Delete))
            .expect("delete victim generation 1");
        runtime
            .branch(&BranchRequest::new(
                victim,
                BranchAction::Create,
                Some(BranchGeneration::new(2)),
            ))
            .expect("recreate the deleted name as generation 2");
        assert_eq!(
            read_value(&runtime, victim, b"ghost"),
            None,
            "live runtime already fences the dead generation"
        );
        put_at(&mut runtime, victim, b"fresh", b"alive", 20);
    }

    let runtime = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
        &backend,
    )
    .expect("reopen")
    .into_runtime();
    assert_eq!(
        read_value(&runtime, victim, b"ghost"),
        None,
        "recovery must not resurrect the deleted generation's earlier row"
    );
    assert_eq!(
        read_value(&runtime, victim, b"ghost-two"),
        None,
        "recovery must not resurrect the deleted generation's boundary row"
    );
    assert_eq!(
        read_value(&runtime, victim, b"fresh"),
        Some(b"alive".to_vec()),
        "the new generation's own commit survives reopen"
    );
}

/// #2830 (checkpoint door): a checkpoint captures generation 1's rows; the
/// name is deleted and re-created; recovery must not install the dead
/// generation's checkpoint rows into the fresh branch. Two ghost commits
/// put the fence's strictly-below and at-boundary arms on the line.
#[test]
fn recreate_after_checkpoint_does_not_resurrect_predecessor_rows() {
    use crate::api::{MaintenanceRequest, MaintenanceScope, MaintenanceTask};

    let root = temp_dir_for_api_test("branch-recreate-checkpoint-fence");
    let backend = StorageBackend::local_fs(root.clone());
    let victim = branch_with(0x73);
    {
        let mut runtime = StorageRuntime::open_with_backend(
            StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
                .with_maintenance_scheduling_policy(
                    crate::api::StorageMaintenanceSchedulingPolicy::EvaluateAndEnqueue,
                ),
            &backend,
        )
        .expect("durable open")
        .into_runtime();
        runtime
            .branch(&create_request(victim))
            .expect("create victim generation 1");
        put_at(&mut runtime, victim, b"ghost", b"dead", 10);
        put_at(&mut runtime, victim, b"ghost-two", b"dead-too", 15);
        runtime
            .enqueue_maintenance(&MaintenanceRequest::new(
                MaintenanceTask::Checkpoint,
                MaintenanceScope::Global,
            ))
            .expect("enqueue checkpoint");
        runtime.drain_maintenance().expect("drain checkpoint");
        runtime
            .branch(&branch_request(victim, BranchAction::Delete))
            .expect("delete victim generation 1");
        runtime
            .branch(&BranchRequest::new(
                victim,
                BranchAction::Create,
                Some(BranchGeneration::new(2)),
            ))
            .expect("recreate the deleted name as generation 2");
        put_at(&mut runtime, victim, b"fresh", b"alive", 20);
    }

    let runtime = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
        &backend,
    )
    .expect("reopen")
    .into_runtime();
    assert_eq!(
        read_value(&runtime, victim, b"ghost"),
        None,
        "checkpointed dead-generation row must not resurrect"
    );
    assert_eq!(
        read_value(&runtime, victim, b"ghost-two"),
        None,
        "checkpointed dead-generation boundary row must not resurrect"
    );
    assert_eq!(
        read_value(&runtime, victim, b"fresh"),
        Some(b"alive".to_vec()),
        "the new generation's own commit survives reopen"
    );
}

/// #2830 (table-manifest door): generation 1's rows are FLUSHED to owned
/// tables (durable table manifest) before the delete; the recreate leaves
/// the stale manifest on disk; recovery must not attach the dead
/// generation's tables to the fresh branch.
#[test]
fn recreate_after_flush_does_not_resurrect_predecessor_tables() {
    use crate::api::{MaintenanceRequest, MaintenanceScope, MaintenanceTask};

    let root = temp_dir_for_api_test("branch-recreate-manifest-fence");
    let backend = StorageBackend::local_fs(root.clone());
    let victim = branch_with(0x74);
    {
        let mut runtime = StorageRuntime::open_with_backend(
            StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
                .with_maintenance_scheduling_policy(
                    crate::api::StorageMaintenanceSchedulingPolicy::EvaluateAndEnqueue,
                ),
            &backend,
        )
        .expect("durable open")
        .into_runtime();
        runtime
            .branch(&create_request(victim))
            .expect("create victim generation 1");
        put_at(&mut runtime, victim, b"ghost", b"dead", 10);
        runtime
            .enqueue_maintenance(&MaintenanceRequest::new(
                MaintenanceTask::Flush,
                MaintenanceScope::Branch(victim),
            ))
            .expect("enqueue flush");
        runtime.drain_maintenance().expect("drain flush");
        runtime
            .branch(&branch_request(victim, BranchAction::Delete))
            .expect("delete victim generation 1");
        runtime
            .branch(&BranchRequest::new(
                victim,
                BranchAction::Create,
                Some(BranchGeneration::new(2)),
            ))
            .expect("recreate the deleted name as generation 2");
        put_at(&mut runtime, victim, b"fresh", b"alive", 20);
    }

    let runtime = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
        &backend,
    )
    .expect("reopen")
    .into_runtime();
    assert_eq!(
        read_value(&runtime, victim, b"ghost"),
        None,
        "flushed dead-generation table row must not resurrect"
    );
    assert_eq!(
        read_value(&runtime, victim, b"fresh"),
        Some(b"alive".to_vec()),
        "the new generation's own commit survives reopen"
    );
}

/// #2830 direction control: a LEGITIMATE parentless branch (`created_at`
/// stamped, no delete/recreate anywhere) keeps its flushed tables across
/// reopen — the stale-manifest skip must not fire on a live generation.
/// Kills the `manifest_max_commit_version -> None / Some(default)` stub
/// mutants: either stub misclassifies this manifest as stale and the row
/// vanishes. The initial commit on another branch makes the victim's
/// `created_at` Some (visible > 0), which is what routes recovery through
/// the fence at all.
#[test]
fn legitimate_flushed_branch_survives_reopen_with_created_at_stamped() {
    use crate::api::{MaintenanceRequest, MaintenanceScope, MaintenanceTask};

    let root = temp_dir_for_api_test("branch-legit-manifest-not-fenced");
    let backend = StorageBackend::local_fs(root.clone());
    let keeper = branch_with(0x75);
    {
        let mut runtime = StorageRuntime::open_with_backend(
            StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
                .with_maintenance_scheduling_policy(
                    crate::api::StorageMaintenanceSchedulingPolicy::EvaluateAndEnqueue,
                ),
            &backend,
        )
        .expect("durable open")
        .into_runtime();
        put_at(&mut runtime, branch(), b"warm-up", b"one", 5);
        runtime
            .branch(&create_request(keeper))
            .expect("create keeper with created_at stamped");
        put_at(&mut runtime, keeper, b"kept", b"safe", 10);
        runtime
            .enqueue_maintenance(&MaintenanceRequest::new(
                MaintenanceTask::Flush,
                MaintenanceScope::Branch(keeper),
            ))
            .expect("enqueue flush");
        runtime.drain_maintenance().expect("drain flush");
        // Checkpoint AFTER the flush: replay starts above the row, so the
        // flushed table (via the manifest) is the row's sole recovery
        // source — a wrongly skipped manifest is a lost row, not a
        // WAL-replay-healed one.
        runtime
            .enqueue_maintenance(&MaintenanceRequest::new(
                MaintenanceTask::Checkpoint,
                MaintenanceScope::Global,
            ))
            .expect("enqueue checkpoint");
        runtime.drain_maintenance().expect("drain checkpoint");
    }

    let runtime = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
        &backend,
    )
    .expect("reopen")
    .into_runtime();
    assert_eq!(
        read_value(&runtime, keeper, b"kept"),
        Some(b"safe".to_vec()),
        "a live generation's flushed table must survive reopen"
    );
}

/// #2830: after delete + recreate + checkpoint + reopen, a timestamp from
/// the DEAD generation's era must stay unresolvable — stale timeline
/// entries seeded into the fresh branch would let fork-at-timestamp
/// resolve a version the generation never had. The fresh-era fork is the
/// in-test direction control. Kills the fence-inversion (`!` deletion)
/// mutant in the timeline seeding.
#[test]
fn recreated_branch_refuses_dead_generation_timestamps_after_reopen() {
    use crate::api::{MaintenanceRequest, MaintenanceScope, MaintenanceTask};

    let root = temp_dir_for_api_test("branch-recreate-timeline-fence");
    let backend = StorageBackend::local_fs(root.clone());
    let victim = branch_with(0x76);
    {
        let mut runtime = StorageRuntime::open_with_backend(
            StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
                .with_maintenance_scheduling_policy(
                    crate::api::StorageMaintenanceSchedulingPolicy::EvaluateAndEnqueue,
                ),
            &backend,
        )
        .expect("durable open")
        .into_runtime();
        runtime
            .branch(&create_request(victim))
            .expect("create victim generation 1");
        put_at(&mut runtime, victim, b"ghost", b"dead", 10);
        runtime
            .branch(&branch_request(victim, BranchAction::Delete))
            .expect("delete victim generation 1");
        runtime
            .branch(&BranchRequest::new(
                victim,
                BranchAction::Create,
                Some(BranchGeneration::new(2)),
            ))
            .expect("recreate as generation 2");
        put_at(&mut runtime, victim, b"fresh", b"alive", 30);
        runtime
            .enqueue_maintenance(&MaintenanceRequest::new(
                MaintenanceTask::Checkpoint,
                MaintenanceScope::Global,
            ))
            .expect("enqueue checkpoint");
        runtime.drain_maintenance().expect("drain checkpoint");
    }

    let runtime = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
        &backend,
    )
    .expect("reopen")
    .into_runtime();
    let error = runtime
        .branch(&branch_request(
            branch_with(0x77),
            BranchAction::ForkAtTimestamp {
                source: victim,
                timestamp: Timestamp::from_micros(10),
            },
        ))
        .expect_err("dead-generation-era timestamp must stay unresolvable");
    assert_eq!(error.class(), StorageApiErrorClass::HistoryUnavailable);
    let outcome = runtime
        .branch(&branch_request(
            branch_with(0x78),
            BranchAction::ForkAtTimestamp {
                source: victim,
                timestamp: Timestamp::from_micros(30),
            },
        ))
        .expect("fresh-era timestamp resolves");
    assert_eq!(
        read_value(&runtime, branch_with(0x78), b"fresh"),
        Some(b"alive".to_vec()),
        "the fresh-era fork serves the new generation's row"
    );
    let _ = outcome;
}

/// #2830 direction control: a FORK child's checkpoint-covered timeline
/// keeps its inherited-era entries across reopen — the fence must never
/// filter fork children (their inherited history sits at or below
/// `created_at` by design). Kills the `&&`->`||` mutant in the timeline
/// seeding's parentless qualification.
#[test]
fn fork_child_keeps_inherited_era_timestamps_after_reopen() {
    use crate::api::{MaintenanceRequest, MaintenanceScope, MaintenanceTask};

    let root = temp_dir_for_api_test("branch-fork-timeline-not-fenced");
    let backend = StorageBackend::local_fs(root.clone());
    let source = branch_with(0x79);
    let child = branch_with(0x7a);
    {
        let mut runtime = StorageRuntime::open_with_backend(
            StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
                .with_maintenance_scheduling_policy(
                    crate::api::StorageMaintenanceSchedulingPolicy::EvaluateAndEnqueue,
                ),
            &backend,
        )
        .expect("durable open")
        .into_runtime();
        runtime
            .branch(&create_request(source))
            .expect("create source");
        put_at(&mut runtime, source, b"lineage", b"one", 10);
        put_at(&mut runtime, source, b"lineage", b"two", 30);
        runtime
            .branch(&branch_request(child, BranchAction::ForkCurrent { source }))
            .expect("fork child");
        runtime
            .enqueue_maintenance(&MaintenanceRequest::new(
                MaintenanceTask::Checkpoint,
                MaintenanceScope::Global,
            ))
            .expect("enqueue checkpoint");
        runtime.drain_maintenance().expect("drain checkpoint");
    }

    let runtime = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
        &backend,
    )
    .expect("reopen")
    .into_runtime();
    let outcome = runtime
        .branch(&branch_request(
            branch_with(0x7b),
            BranchAction::ForkAtTimestamp {
                source: child,
                timestamp: Timestamp::from_micros(20),
            },
        ))
        .expect("inherited-era timestamp resolves on the fork child after reopen");
    assert_eq!(
        read_value(&runtime, branch_with(0x7b), b"lineage"),
        Some(b"one".to_vec()),
        "the inherited-era fork serves the pre-fork row"
    );
    let _ = outcome;
}

/// #2847: a checkpoint records a non-seeded branch's memtable rows while the
/// branch holds no durable base (the structural guard correctly passes);
/// a LATER flush publishes that branch's table manifest, durably violating
/// the "snapshot never coexists with a non-seeded durable base" invariant
/// from the flush side, where no guard exists. Reopen must COMBINE the
/// manifest base with the checkpoint's (byte-identical) rows — pre-fix it
/// refused with `InvalidSnapshotInstall` and the store was permanently
/// unopenable.
#[test]
fn checkpoint_then_flush_of_non_seeded_branch_survives_reopen() {
    use crate::api::{MaintenanceRequest, MaintenanceScope, MaintenanceTask};

    let root = temp_dir_for_api_test("branch-ckpt-then-flush-combine");
    let backend = StorageBackend::local_fs(root.clone());
    let victim = branch_with(0x7c);
    {
        let mut runtime = StorageRuntime::open_with_backend(
            StorageOpenOptions::durable_local(StorageDurabilityPolicy::Always)
                .with_maintenance_scheduling_policy(
                    crate::api::StorageMaintenanceSchedulingPolicy::EvaluateAndEnqueue,
                ),
            &backend,
        )
        .expect("durable open")
        .into_runtime();
        put_at(&mut runtime, branch(), b"seeded-row", b"base", 5);
        runtime
            .branch(&create_request(victim))
            .expect("create victim");
        put_at(&mut runtime, victim, b"covered", b"both", 10);
        put_at(&mut runtime, victim, b"covered-two", b"both-too", 15);
        // Checkpoint FIRST: victim has no owned tables, so the structural
        // guard passes and the snapshot records victim's memtable rows.
        runtime
            .enqueue_maintenance(&MaintenanceRequest::new(
                MaintenanceTask::Checkpoint,
                MaintenanceScope::Global,
            ))
            .expect("enqueue checkpoint");
        runtime.drain_maintenance().expect("drain checkpoint");
        // Flush AFTER: victim gains a durable table-manifest base covering
        // the same rows the live snapshot already carries.
        runtime
            .enqueue_maintenance(&MaintenanceRequest::new(
                MaintenanceTask::Flush,
                MaintenanceScope::Branch(victim),
            ))
            .expect("enqueue flush");
        runtime.drain_maintenance().expect("drain flush");
        // A WAL-tail row above the flush: replay must still land it.
        put_at(&mut runtime, victim, b"tail", b"walled", 20);
    }

    let runtime = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Always),
        &backend,
    )
    .expect("reopen must combine the checkpoint with the flushed base, not refuse")
    .into_runtime();
    assert_eq!(
        read_value(&runtime, victim, b"covered"),
        Some(b"both".to_vec()),
        "manifest-and-checkpoint row survives the combine"
    );
    assert_eq!(
        read_value(&runtime, victim, b"covered-two"),
        Some(b"both-too".to_vec()),
        "second overlapping row survives the combine"
    );
    assert_eq!(
        read_value(&runtime, victim, b"tail"),
        Some(b"walled".to_vec()),
        "the WAL-tail row above the flush replays"
    );
    assert_eq!(
        read_value(&runtime, branch(), b"seeded-row"),
        Some(b"base".to_vec()),
        "the seeded branch is untouched"
    );
    // The combined state must be fork-visible: a fresh fork of the victim
    // sees every row the combine reassembled.
    let child = branch_with(0x7e);
    runtime
        .branch(&branch_request(
            child,
            BranchAction::ForkCurrent { source: victim },
        ))
        .expect("fork the combined branch");
    assert_eq!(
        read_value(&runtime, child, b"covered"),
        Some(b"both".to_vec()),
        "fork of the combined branch inherits the manifest-covered row"
    );
    assert_eq!(
        read_value(&runtime, child, b"tail"),
        Some(b"walled".to_vec()),
        "fork of the combined branch inherits the WAL-tail row"
    );
}

/// #2847, the DST seed-52 shape: the non-seeded branch is an eager
/// (fork-at-version) child whose materialized rows live in its memtable.
/// Checkpoint captures them, a later flush makes them durable, and reopen
/// must combine rather than refuse.
#[test]
fn eager_fork_checkpoint_then_flush_survives_reopen() {
    use crate::api::{MaintenanceRequest, MaintenanceScope, MaintenanceTask};

    let root = temp_dir_for_api_test("branch-fork-ckpt-then-flush-combine");
    let backend = StorageBackend::local_fs(root.clone());
    let child = branch_with(0x7d);
    {
        let mut runtime = StorageRuntime::open_with_backend(
            StorageOpenOptions::durable_local(StorageDurabilityPolicy::Always)
                .with_maintenance_scheduling_policy(
                    crate::api::StorageMaintenanceSchedulingPolicy::EvaluateAndEnqueue,
                ),
            &backend,
        )
        .expect("durable open")
        .into_runtime();
        let first = put_at(&mut runtime, branch(), b"lineage", b"one", 10);
        put_at(&mut runtime, branch(), b"lineage", b"two", 20);
        runtime
            .branch(&branch_request(
                child,
                BranchAction::ForkAtVersion {
                    source: branch(),
                    version: first.commit_version(),
                },
            ))
            .expect("eager fork at version");
        put_at(&mut runtime, child, b"own", b"mine", 30);
        runtime
            .enqueue_maintenance(&MaintenanceRequest::new(
                MaintenanceTask::Checkpoint,
                MaintenanceScope::Branch(child),
            ))
            .expect("enqueue checkpoint");
        runtime.drain_maintenance().expect("drain checkpoint");
        runtime
            .enqueue_maintenance(&MaintenanceRequest::new(
                MaintenanceTask::Flush,
                MaintenanceScope::Branch(child),
            ))
            .expect("enqueue flush");
        runtime.drain_maintenance().expect("drain flush");
    }

    let runtime = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Always),
        &backend,
    )
    .expect("reopen must combine the checkpoint with the flushed fork base")
    .into_runtime();
    assert_eq!(
        read_value(&runtime, child, b"lineage"),
        Some(b"one".to_vec()),
        "the fork-materialized pre-fork row survives"
    );
    assert_eq!(
        read_value(&runtime, child, b"own"),
        Some(b"mine".to_vec()),
        "the child's own post-fork row survives"
    );
    assert_eq!(
        read_value(&runtime, branch(), b"lineage"),
        Some(b"two".to_vec()),
        "the source branch stays at its own head"
    );
}

/// #2833: re-fork a deleted name from a source that ITSELF has inherited
/// layers (the EAGER fork path). The dead generation's flushed manifest must
/// not survive as the new generation's recovery provenance — pre-fix, the
/// eager path published no child manifest, the stale gen-1 artifact remained
/// on disk, and the recovered branch silently served gen 1's lineage
/// (dropping `root-new`, inherited via the REAL parent). Both flushes are
/// load-bearing: the source flush gives gen 1's layer real tables (else the
/// recovery rebuild heals the staleness), the child flush durably publishes
/// gen 1's manifest.
#[test]
fn eager_refork_over_deleted_name_recovers_current_lineage() {
    let root = temp_dir_for_api_test("triage-eager-refork");
    let backend = StorageBackend::local_fs(root.clone());
    let mid = branch_with(0x87);
    let leaf = branch_with(0x88);
    {
        let mut runtime = StorageRuntime::open_with_backend(
            StorageOpenOptions::durable_local(StorageDurabilityPolicy::Always)
                .with_maintenance_scheduling_policy(
                    crate::api::StorageMaintenanceSchedulingPolicy::EvaluateAndEnqueue,
                ),
            &backend,
        )
        .expect("open 1")
        .into_runtime();
        put_at(&mut runtime, branch(), b"root-old", b"old-v", 10);
        // Flush DEFAULT first: gen 1's inherited layer must reference real
        // flushed source tables (a table-less layer counts as layer-less and
        // the #2820 recovery rebuild would heal the staleness).
        runtime
            .enqueue_maintenance(&crate::api::MaintenanceRequest::new(
                crate::api::MaintenanceTask::Flush,
                crate::api::MaintenanceScope::Branch(branch()),
            ))
            .expect("enqueue default flush");
        runtime.drain_maintenance().expect("drain default flush");
        // leaf gen 1: COW fork of default (publishes a child manifest with
        // gen 1's inherited provenance).
        runtime
            .branch(&branch_request(
                leaf,
                BranchAction::ForkCurrent { source: branch() },
            ))
            .expect("fork leaf gen 1");
        put_at(&mut runtime, branch(), b"root-new", b"new-v", 20);
        // mid: fork of default AFTER root-new (mid has an inherited layer).
        runtime
            .branch(&branch_request(
                mid,
                BranchAction::ForkCurrent { source: branch() },
            ))
            .expect("fork mid");
        // Flush the gen-1 leaf: the flush publishes its table manifest WITH
        // the gen-1 inherited-layer record — the artifact that must not
        // outlive the generation.
        put_at(&mut runtime, leaf, b"leaf-ghost", b"dead", 25);
        runtime
            .enqueue_maintenance(&crate::api::MaintenanceRequest::new(
                crate::api::MaintenanceTask::Flush,
                crate::api::MaintenanceScope::Branch(leaf),
            ))
            .expect("enqueue flush");
        runtime.drain_maintenance().expect("drain flush");
        // delete leaf gen 1, re-fork the name from MID (source has inherited
        // layers -> EAGER path -> no manifest publish).
        runtime
            .branch(&branch_request(leaf, BranchAction::Delete))
            .expect("delete leaf gen 1");
        runtime
            .branch(&BranchRequest::new(
                leaf,
                BranchAction::ForkCurrent { source: mid },
                Some(BranchGeneration::new(2)),
            ))
            .expect("re-fork leaf gen 2 from mid");
        // The layer-less re-fork must have REMOVED gen 1's stale manifest
        // (and the layered mid fork must have PUBLISHED one) — both sides of
        // the boundary observed on disk.
        assert!(
            !table_manifest_path(&root, leaf).exists(),
            "the eager re-fork removes the dead generation's manifest"
        );
        assert!(
            table_manifest_path(&root, mid).exists(),
            "the layered fork keeps publishing its manifest"
        );
        put_at(&mut runtime, leaf, b"leaf-own", b"own-v", 30);
        assert_eq!(
            read_value(&runtime, leaf, b"root-new"),
            Some(b"new-v".to_vec()),
            "live: gen 2 inherits root-new via mid"
        );
    }
    let runtime = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Always),
        &backend,
    )
    .expect("reopen")
    .into_runtime();
    assert_eq!(
        read_value(&runtime, leaf, b"root-new"),
        Some(b"new-v".to_vec()),
        "recovered gen 2 must inherit root-new via mid, not gen 1's stale lineage"
    );
    assert_eq!(
        read_value(&runtime, leaf, b"leaf-own"),
        Some(b"own-v".to_vec()),
        "recovered gen 2 keeps its own row"
    );
}

/// On-disk path of a branch's table-manifest object (`tables/<branch>/manifest`
/// + the `.object@` suffix).
fn table_manifest_path(root: &std::path::Path, branch_id: BranchId) -> std::path::PathBuf {
    root.join(format!("tables/{branch_id}/manifest.object@"))
}

/// #2833 direction control: an eager fork onto a FRESH name (no predecessor,
/// nothing to remove) still recovers through the fork rebuild — the removal
/// arm is a `NotFound` no-op there.
#[test]
fn eager_fork_on_fresh_name_recovers_current_lineage() {
    use crate::api::{MaintenanceRequest, MaintenanceScope, MaintenanceTask};

    let root = temp_dir_for_api_test("eager-fork-fresh-name");
    let backend = StorageBackend::local_fs(root.clone());
    let mid = branch_with(0x89);
    let leaf = branch_with(0x8a);
    {
        let mut runtime = StorageRuntime::open_with_backend(
            StorageOpenOptions::durable_local(StorageDurabilityPolicy::Always)
                .with_maintenance_scheduling_policy(
                    crate::api::StorageMaintenanceSchedulingPolicy::EvaluateAndEnqueue,
                ),
            &backend,
        )
        .expect("open")
        .into_runtime();
        put_at(&mut runtime, branch(), b"root-row", b"root-v", 10);
        runtime
            .enqueue_maintenance(&MaintenanceRequest::new(
                MaintenanceTask::Flush,
                MaintenanceScope::Branch(branch()),
            ))
            .expect("enqueue default flush");
        runtime.drain_maintenance().expect("drain default flush");
        runtime
            .branch(&branch_request(
                mid,
                BranchAction::ForkCurrent { source: branch() },
            ))
            .expect("fork mid (layered)");
        runtime
            .branch(&branch_request(
                leaf,
                BranchAction::ForkCurrent { source: mid },
            ))
            .expect("eager fork leaf from mid on a fresh name");
        put_at(&mut runtime, leaf, b"leaf-own", b"own-v", 20);
    }
    let runtime = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Always),
        &backend,
    )
    .expect("reopen")
    .into_runtime();
    assert_eq!(
        read_value(&runtime, leaf, b"root-row"),
        Some(b"root-v".to_vec()),
        "fresh-name eager fork inherits through the chain after reopen"
    );
    assert_eq!(
        read_value(&runtime, leaf, b"leaf-own"),
        Some(b"own-v".to_vec()),
        "fresh-name eager fork keeps its own row"
    );
}

/// #2850: the recovered commit clock must never regress below the surviving
/// branch catalog's version anchors. Branch lifecycle ops are durably fenced,
/// so the catalog (with `created_at` / fork anchors) survives a crash that
/// sheds the unsynced WAL under Standard durability — the allocator must
/// resume ABOVE those anchors, or every version-anchored catalog fact
/// (generation fences, fork re-materialization) silently refers to different
/// content on the restarted clock.
#[test]
fn recovered_commit_clock_stays_above_surviving_catalog_anchors() {
    use crate::testkit::FsModel;

    let root = temp_dir_for_api_test("clock-above-catalog-anchors");
    let backend = StorageBackend::reordering_local_fs(root.clone());
    let anchor = branch_with(0x7f);
    {
        let mut runtime = StorageRuntime::open_with_backend(
            StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
            &backend,
        )
        .expect("durable open")
        .into_runtime();
        // Three unsynced commits under Standard's coalescing window.
        put_at(&mut runtime, branch(), b"shed-a", b"1", 10);
        put_at(&mut runtime, branch(), b"shed-b", b"2", 20);
        let last = put_at(&mut runtime, branch(), b"shed-c", b"3", 30);
        assert_eq!(last.commit_version(), CommitVersion::new(3));
        // The durably-fenced catalog publish stamps created_at = 3.
        runtime
            .branch(&create_request(anchor))
            .expect("create anchor branch");
        drop(runtime);
        // Power loss: unsynced WAL bytes shed; the catalog object survives.
        backend
            .reordering_crash(FsModel::OrderedAtomic, 7)
            .expect("materialize crash");
    }

    let mut runtime = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
        &backend,
    )
    .expect("reopen after crash")
    .into_runtime();
    // Precondition, not the oracle: the crash must actually shed the
    // unsynced commits, or this test silently goes vacuous.
    assert_eq!(
        read_value(&runtime, branch(), b"shed-a"),
        None,
        "precondition: the ordered-atomic crash must shed the unsynced WAL"
    );
    let summary = put_at(&mut runtime, branch(), b"fresh", b"f", 40);
    assert!(
        summary.commit_version() > CommitVersion::new(3),
        "recovered clock must resume above the surviving catalog anchor \
         (created_at=3), got {:?}",
        summary.commit_version()
    );
}

/// #2850, the behavioral half: a commit accepted on the restarted clock at a
/// version at/below a surviving `created_at` anchor is silently eaten by the
/// #2830/#2826 generation fences at the NEXT recovery — an acked row
/// vanishes (the DST's Gap shape). With the clock fixed the fences stay
/// sound by construction.
#[test]
fn post_crash_commits_survive_the_next_reopen_despite_catalog_anchors() {
    use crate::testkit::FsModel;

    let root = temp_dir_for_api_test("post-crash-commit-survives-fences");
    let backend = StorageBackend::reordering_local_fs(root.clone());
    let anchor = branch_with(0x80);
    {
        let mut runtime = StorageRuntime::open_with_backend(
            StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
            &backend,
        )
        .expect("durable open")
        .into_runtime();
        put_at(&mut runtime, branch(), b"shed-a", b"1", 10);
        put_at(&mut runtime, branch(), b"shed-b", b"2", 20);
        put_at(&mut runtime, branch(), b"shed-c", b"3", 30);
        runtime
            .branch(&create_request(anchor))
            .expect("create anchor branch");
        drop(runtime);
        backend
            .reordering_crash(FsModel::OrderedAtomic, 7)
            .expect("materialize crash");
    }
    {
        let mut runtime = StorageRuntime::open_with_backend(
            StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
            &backend,
        )
        .expect("reopen after crash")
        .into_runtime();
        // An acked commit on the anchor branch after the lossy reopen.
        put_at(&mut runtime, anchor, b"kept", b"alive", 40);
        // Clean close: this row is durable.
    }

    let runtime = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
        &backend,
    )
    .expect("clean reopen")
    .into_runtime();
    assert_eq!(
        read_value(&runtime, anchor, b"kept"),
        Some(b"alive".to_vec()),
        "an acked post-crash commit must survive the next reopen"
    );
}

/// #2850, the deleted-descriptor arm: a DEAD branch's anchors (deletion
/// watermark, creation stamp) are still allocated versions — the recovered
/// clock must resume above them too, or the acked-deletion fences judge the
/// restarted clock's commits against a dead generation's era.
#[test]
fn recovered_commit_clock_stays_above_deleted_branch_anchors() {
    use crate::testkit::FsModel;

    let root = temp_dir_for_api_test("clock-above-deleted-anchors");
    let backend = StorageBackend::reordering_local_fs(root.clone());
    let victim = branch_with(0x81);
    {
        let mut runtime = StorageRuntime::open_with_backend(
            StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
            &backend,
        )
        .expect("durable open")
        .into_runtime();
        put_at(&mut runtime, branch(), b"shed-a", b"1", 10);
        put_at(&mut runtime, branch(), b"shed-b", b"2", 20);
        put_at(&mut runtime, branch(), b"shed-c", b"3", 30);
        // Durably-fenced create THEN delete: the catalog's only surviving
        // anchor for this name is a Deleted descriptor (created_at=3,
        // deleted_at=3).
        runtime
            .branch(&create_request(victim))
            .expect("create victim");
        runtime
            .branch(&branch_request(victim, BranchAction::Delete))
            .expect("delete victim");
        drop(runtime);
        backend
            .reordering_crash(FsModel::OrderedAtomic, 7)
            .expect("materialize crash");
    }

    let mut runtime = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
        &backend,
    )
    .expect("reopen after crash")
    .into_runtime();
    assert_eq!(
        read_value(&runtime, branch(), b"shed-a"),
        None,
        "precondition: the ordered-atomic crash must shed the unsynced WAL"
    );
    let summary = put_at(&mut runtime, branch(), b"fresh", b"f", 40);
    assert!(
        summary.commit_version() > CommitVersion::new(3),
        "recovered clock must resume above the DEAD descriptor's anchors, got {:?}",
        summary.commit_version()
    );
}

/// #2852: `fork_current` must anchor on the source's current CONTENT
/// watermark, not on retained-timeline coverage. After a lossy crash,
/// flush-published tables recover content whose (version→timestamp)
/// timeline facts shed with the WAL — the checkpoint's timeline groups
/// cover only their own watermark. The timeline-based resolution degraded
/// to "no retained history" and silently forked an EMPTY child over a
/// fully-populated source (the DST's live-step `LostAck` shape: seeds
/// 83/154/164/178 on tracker #2828).
#[test]
fn fork_current_captures_content_that_outlives_timeline_coverage() {
    use crate::api::{MaintenanceRequest, MaintenanceScope, MaintenanceTask};
    use crate::testkit::FsModel;

    let root = temp_dir_for_api_test("fork-current-content-beyond-timeline");
    let backend = StorageBackend::reordering_local_fs(root.clone());
    let expected_watermark;
    {
        let mut runtime = StorageRuntime::open_with_backend(
            StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
                .with_maintenance_scheduling_policy(
                    crate::api::StorageMaintenanceSchedulingPolicy::EvaluateAndEnqueue,
                ),
            &backend,
        )
        .expect("durable open")
        .into_runtime();
        put_at(&mut runtime, branch(), b"early", b"e", 10);
        // Checkpoint: persisted timeline coverage ends HERE.
        runtime
            .enqueue_maintenance(&MaintenanceRequest::new(
                MaintenanceTask::Checkpoint,
                MaintenanceScope::Global,
            ))
            .expect("enqueue checkpoint");
        runtime.drain_maintenance().expect("drain checkpoint");
        // Post-checkpoint commits, then a flush: the rows become durable
        // through the table manifest while their timeline facts live only
        // in the (unsynced) WAL.
        put_at(&mut runtime, branch(), b"late-a", b"1", 20);
        let last = put_at(&mut runtime, branch(), b"late-b", b"2", 30);
        expected_watermark = last.commit_version();
        runtime
            .enqueue_maintenance(&MaintenanceRequest::new(
                MaintenanceTask::Flush,
                MaintenanceScope::Branch(branch()),
            ))
            .expect("enqueue flush");
        runtime.drain_maintenance().expect("drain flush");
        drop(runtime);
        backend
            .reordering_crash(FsModel::OrderedAtomic, 7)
            .expect("materialize crash");
    }

    let runtime = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
        &backend,
    )
    .expect("reopen after crash")
    .into_runtime();
    // Precondition, not the oracle: the flushed post-checkpoint content must
    // recover, or the choreography failed to produce content-beyond-timeline.
    assert_eq!(
        read_value(&runtime, branch(), b"late-b"),
        Some(b"2".to_vec()),
        "precondition: flush-published post-checkpoint content must recover"
    );
    let child = branch_with(0x82);
    let outcome = runtime
        .branch(&branch_request(
            child,
            BranchAction::ForkCurrent { source: branch() },
        ))
        .expect("fork_current of a populated source");
    assert_eq!(
        outcome.fork_version(),
        Some(expected_watermark),
        "fork_current must anchor on the source's content watermark"
    );
    assert_eq!(
        read_value(&runtime, child, b"late-b"),
        Some(b"2".to_vec()),
        "the child must serve content that outlived timeline coverage"
    );
    assert_eq!(
        read_value(&runtime, child, b"early"),
        Some(b"e".to_vec()),
        "the child must serve the checkpoint-covered content too"
    );
}

/// #2852's availability leg (#2853): after a lossy crash, flush-published
/// content legally outlives timeline coverage (index tip < content watermark).
/// The retained timeline must then SHRINK to the index's provable prefix — not
/// vanish into the empty post-elision scan. A fork at a version the surviving
/// timeline provably covers succeeds; a version past the tip (content present,
/// mapping shed) keeps refusing.
#[test]
fn fork_at_version_inside_surviving_timeline_coverage_succeeds_after_lossy_crash() {
    use crate::api::{MaintenanceRequest, MaintenanceScope, MaintenanceTask};
    use crate::testkit::FsModel;

    let root = temp_dir_for_api_test("fork-at-version-surviving-timeline");
    let backend = StorageBackend::reordering_local_fs(root.clone());
    let covered_version;
    let shed_version;
    {
        let mut runtime = StorageRuntime::open_with_backend(
            StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
                .with_maintenance_scheduling_policy(
                    crate::api::StorageMaintenanceSchedulingPolicy::EvaluateAndEnqueue,
                ),
            &backend,
        )
        .expect("durable open")
        .into_runtime();
        covered_version = put_at(&mut runtime, branch(), b"early", b"e", 10).commit_version();
        // Complete the live retained-timeline index (a timestamp read scans
        // and seeds it) so the checkpoint captures its timeline group — the
        // recovered index then provably covers the pre-checkpoint prefix.
        let warmed = runtime
            .read_point(&PointReadRequest::new(
                branch(),
                engine_space(),
                api_key(b"early"),
                ReadBound::AtTimestamp(strata_core::Timestamp::from_micros(10)),
            ))
            .expect("timestamp read completes the timeline index");
        assert!(warmed.row().is_some(), "warm read must see the early row");
        runtime
            .enqueue_maintenance(&MaintenanceRequest::new(
                MaintenanceTask::Checkpoint,
                MaintenanceScope::Global,
            ))
            .expect("enqueue checkpoint");
        runtime.drain_maintenance().expect("drain checkpoint");
        put_at(&mut runtime, branch(), b"late-a", b"1", 20);
        shed_version = put_at(&mut runtime, branch(), b"late-b", b"2", 30).commit_version();
        runtime
            .enqueue_maintenance(&MaintenanceRequest::new(
                MaintenanceTask::Flush,
                MaintenanceScope::Branch(branch()),
            ))
            .expect("enqueue flush");
        runtime.drain_maintenance().expect("drain flush");
        drop(runtime);
        backend
            .reordering_crash(FsModel::OrderedAtomic, 7)
            .expect("materialize crash");
    }

    let runtime = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
        &backend,
    )
    .expect("reopen after crash")
    .into_runtime();
    assert_eq!(
        read_value(&runtime, branch(), b"late-b"),
        Some(b"2".to_vec()),
        "precondition: flush-published post-checkpoint content must recover"
    );

    // Inside the surviving coverage: the checkpoint-covered version forks.
    let child = branch_with(0x84);
    let outcome = runtime
        .branch(&branch_request(
            child,
            BranchAction::ForkAtVersion {
                source: branch(),
                version: covered_version,
            },
        ))
        .expect("fork at a version the surviving timeline provably covers");
    assert_eq!(outcome.fork_version(), Some(covered_version));
    assert_eq!(
        read_value(&runtime, child, b"early"),
        Some(b"e".to_vec()),
        "the fork serves the covered prefix",
    );
    assert_eq!(
        read_value(&runtime, child, b"late-a"),
        None,
        "post-fork-version content must not leak into the child",
    );

    // Past the tip: the content exists but its timeline mapping was shed —
    // the refusal stays.
    let error = runtime
        .branch(&branch_request(
            branch_with(0x85),
            BranchAction::ForkAtVersion {
                source: branch(),
                version: shed_version,
            },
        ))
        .expect_err("a shed-coverage version keeps refusing");
    assert_eq!(error.class(), StorageApiErrorClass::HistoryUnavailable);
}

/// #2852 direction control: the #2521 legitimate-empty-fork case — a source
/// with NO rows at all — must keep forking an empty child (parent linkage,
/// fork version zero) rather than start refusing.
#[test]
fn fork_current_of_a_rowless_source_stays_a_legitimate_empty_fork() {
    let root = temp_dir_for_api_test("fork-current-rowless-source");
    let backend = StorageBackend::local_fs(root);
    let runtime = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
        &backend,
    )
    .expect("durable open")
    .into_runtime();
    let empty_source = branch_with(0x83);
    runtime
        .branch(&create_request(empty_source))
        .expect("create empty source");
    let child = branch_with(0x84);
    let outcome = runtime
        .branch(&branch_request(
            child,
            BranchAction::ForkCurrent {
                source: empty_source,
            },
        ))
        .expect("fork_current of a rowless source is the legitimate empty fork");
    assert_eq!(
        outcome.fork_version(),
        Some(CommitVersion::ZERO),
        "a rowless source forks at version zero"
    );
    assert_eq!(
        outcome.source_branch_id(),
        Some(empty_source),
        "the empty fork keeps its parent linkage"
    );
    assert_eq!(
        read_value(&runtime, child, b"anything"),
        None,
        "the empty fork serves no rows"
    );
}

/// #2833: a checkpoint captured while gen-1 of a branch name lived survives
/// that name's delete + re-creation BY FORK. The dead generation's rows all
/// sit at `version <= created_at` of the new generation (#2826/#2850), the
/// same band the WAL fence already drops unconditionally — but the
/// checkpoint-row fence exempted fork children, so the dead rows installed
/// into the re-created name at the next reopen (the DST's reopen-Phantom
/// shape: seeds 94/134/142/180 on tracker #2828). Inherited content is NOT
/// the checkpoint's job: the fork rebuild re-materializes it from the live
/// parent.
#[test]
fn refork_of_a_deleted_name_does_not_resurrect_dead_generation_checkpoint_rows() {
    use crate::api::{MaintenanceRequest, MaintenanceScope, MaintenanceTask};

    let root = temp_dir_for_api_test("refork-no-dead-checkpoint-rows");
    let backend = StorageBackend::local_fs(root);
    let victim = branch_with(0x85);
    {
        let mut runtime = StorageRuntime::open_with_backend(
            StorageOpenOptions::durable_local(StorageDurabilityPolicy::Always),
            &backend,
        )
        .expect("durable open")
        .into_runtime();
        put_at(&mut runtime, branch(), b"base", b"b", 5);
        put_at(&mut runtime, branch(), b"base-two", b"b2", 10);
        // Gen-1 life: an eager fork child with a distinctive own row.
        runtime
            .branch(&branch_request(
                victim,
                BranchAction::ForkCurrent { source: branch() },
            ))
            .expect("fork gen-1 victim");
        put_at(&mut runtime, victim, b"dead-own", b"gone", 15);
        // Clean close: the reopen below rebuilds the victim's rows as plain
        // recovered rows (a live eager fork holds volatile tables, which
        // defer checkpoints as manifest-publish debt).
    }
    {
        let mut runtime = StorageRuntime::open_with_backend(
            StorageOpenOptions::durable_local(StorageDurabilityPolicy::Always)
                .with_maintenance_scheduling_policy(
                    crate::api::StorageMaintenanceSchedulingPolicy::EvaluateAndEnqueue,
                ),
            &backend,
        )
        .expect("first reopen")
        .into_runtime();
        // The checkpoint records the victim's gen-1 rows (inherited + own).
        runtime
            .enqueue_maintenance(&MaintenanceRequest::new(
                MaintenanceTask::Checkpoint,
                MaintenanceScope::Global,
            ))
            .expect("enqueue checkpoint");
        runtime.drain_maintenance().expect("drain checkpoint");
        // Kill gen-1, re-create the SAME name by fork (gen 2). Its
        // `created_at` upper-bounds every gen-1 version.
        runtime
            .branch(&branch_request(victim, BranchAction::Delete))
            .expect("delete gen-1 victim");
        runtime
            .branch(&BranchRequest::new(
                victim,
                BranchAction::ForkCurrent { source: branch() },
                Some(BranchGeneration::new(2)),
            ))
            .expect("re-fork victim as gen 2");
        put_at(&mut runtime, victim, b"new-own", b"alive", 20);
        // Clean close: the stale checkpoint is the latest one on disk.
    }

    let runtime = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Always),
        &backend,
    )
    .expect("reopen")
    .into_runtime();
    assert_eq!(
        read_value(&runtime, victim, b"dead-own"),
        None,
        "a dead generation's checkpoint row must not resurrect into the re-forked name"
    );
    assert_eq!(
        read_value(&runtime, victim, b"base"),
        Some(b"b".to_vec()),
        "gen-2's inherited content re-materializes from the live parent"
    );
    assert_eq!(
        read_value(&runtime, victim, b"base-two"),
        Some(b"b2".to_vec()),
        "the full inherited slice survives the fence"
    );
    assert_eq!(
        read_value(&runtime, victim, b"new-own"),
        Some(b"alive".to_vec()),
        "gen-2's own post-fork row survives"
    );
    assert_eq!(
        read_value(&runtime, branch(), b"base"),
        Some(b"b".to_vec()),
        "the parent is untouched"
    );
}

/// #2833 direction control: the fence keeps a fork child's OWN rows
/// (`version > created_at`). Recovery does not replay the WAL below the
/// snapshot watermark for non-seeded branches, so the checkpoint is the
/// child's own row's sole source here — a fence that over-drops loses it.
#[test]
fn fork_child_own_rows_survive_reopen_through_the_checkpoint() {
    use crate::api::{MaintenanceRequest, MaintenanceScope, MaintenanceTask};

    let root = temp_dir_for_api_test("fork-child-own-rows-via-checkpoint");
    let backend = StorageBackend::local_fs(root);
    let child = branch_with(0x86);
    {
        let mut runtime = StorageRuntime::open_with_backend(
            StorageOpenOptions::durable_local(StorageDurabilityPolicy::Always),
            &backend,
        )
        .expect("durable open")
        .into_runtime();
        put_at(&mut runtime, branch(), b"base", b"b", 5);
        runtime
            .branch(&branch_request(
                child,
                BranchAction::ForkCurrent { source: branch() },
            ))
            .expect("fork child");
        // Close and reopen so the child's rebuilt rows carry no
        // manifest-publish debt (a live eager fork defers checkpoints).
    }
    {
        let mut runtime = StorageRuntime::open_with_backend(
            StorageOpenOptions::durable_local(StorageDurabilityPolicy::Always)
                .with_maintenance_scheduling_policy(
                    crate::api::StorageMaintenanceSchedulingPolicy::EvaluateAndEnqueue,
                ),
            &backend,
        )
        .expect("first reopen")
        .into_runtime();
        // An own row ABOVE created_at, then a checkpoint covering it.
        put_at(&mut runtime, child, b"own", b"kept", 10);
        runtime
            .enqueue_maintenance(&MaintenanceRequest::new(
                MaintenanceTask::Checkpoint,
                MaintenanceScope::Global,
            ))
            .expect("enqueue checkpoint");
        runtime.drain_maintenance().expect("drain checkpoint");
    }

    let runtime = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Always),
        &backend,
    )
    .expect("reopen")
    .into_runtime();
    assert_eq!(
        read_value(&runtime, child, b"own"),
        Some(b"kept".to_vec()),
        "the child's own above-created_at row must install from the checkpoint"
    );
    assert_eq!(
        read_value(&runtime, child, b"base"),
        Some(b"b".to_vec()),
        "the child's inherited content survives"
    );
}

/// #2855, the seed-183 Gap shape: a COW fork built over a source whose
/// in-fork sealed tables are VOLATILE (an eager fork child's materialized
/// table has no durable catalog entry) yields a child whose fork-time
/// manifest publish fails best-effort. The child then LOOKS layered, the
/// #2820 delete guard exempts it, the parent delete is allowed — and at the
/// next reopen the child recovers layer-less, the fork rebuild skips its
/// Deleted parent, and the entire inherited slice silently vanishes.
#[test]
fn cow_fork_over_a_volatile_source_keeps_inheritance_across_reopen() {
    let root = temp_dir_for_api_test("cow-over-volatile-keeps-inheritance");
    let backend = StorageBackend::local_fs(root);
    let mid = branch_with(0x87);
    let leaf = branch_with(0x88);
    {
        let mut runtime = StorageRuntime::open_with_backend(
            StorageOpenOptions::durable_local(StorageDurabilityPolicy::Always),
            &backend,
        )
        .expect("durable open")
        .into_runtime();
        put_at(&mut runtime, branch(), b"base", b"b", 5);
        // Eager fork child: `mid` holds a volatile materialized table.
        runtime
            .branch(&branch_request(
                mid,
                BranchAction::ForkCurrent { source: branch() },
            ))
            .expect("eager fork of an unflushed source");
        put_at(&mut runtime, mid, b"mid-own", b"m", 10);
        // The trigger: a fork whose COW eligibility sees mid's volatile table.
        runtime
            .branch(&branch_request(
                leaf,
                BranchAction::ForkCurrent { source: mid },
            ))
            .expect("fork of the eager child");
        // Whether this delete succeeds is exactly what the fix changes
        // (pre-fix: allowed, the leaf looks layered; post-fix: refused, the
        // leaf is layer-less and recovery-depends on mid). The oracle below
        // is the leaf's CONTENT after reopen, so the outcome is recorded
        // and deliberately not asserted here.
        let _version_dependent = runtime.branch(&branch_request(mid, BranchAction::Delete));
    }

    let runtime = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Always),
        &backend,
    )
    .expect("reopen")
    .into_runtime();
    assert_eq!(
        read_value(&runtime, leaf, b"base"),
        Some(b"b".to_vec()),
        "the leaf must keep its transitively inherited row across reopen"
    );
    assert_eq!(
        read_value(&runtime, leaf, b"mid-own"),
        Some(b"m".to_vec()),
        "the leaf must keep the mid-generation row across reopen"
    );
}

/// #2855, the seed-134 Phantom shape: a branch name whose gen-1 was a COW
/// child WITH a published manifest is deleted, then re-created by forking a
/// VOLATILE source. Pre-fix the gen-2 publish fails (volatile layer target),
/// the `layers == 0` stale-manifest removal never runs, and the next reopen
/// adopts the DEAD gen-1's manifest as the re-created name's provenance —
/// serving content the gen-2 lineage never had.
#[test]
fn refork_over_a_deleted_name_does_not_adopt_the_dead_generations_manifest() {
    use crate::api::{MaintenanceRequest, MaintenanceScope, MaintenanceTask};

    let root = temp_dir_for_api_test("refork-no-dead-manifest-adoption");
    let backend = StorageBackend::local_fs(root);
    let victim = branch_with(0x89);
    let aux = branch_with(0x8a);
    let helper = branch_with(0x8b);
    {
        let mut runtime = StorageRuntime::open_with_backend(
            StorageOpenOptions::durable_local(StorageDurabilityPolicy::Always)
                .with_maintenance_scheduling_policy(
                    crate::api::StorageMaintenanceSchedulingPolicy::EvaluateAndEnqueue,
                ),
            &backend,
        )
        .expect("durable open")
        .into_runtime();
        // Gen-1 life: COW child over the seeded branch's FLUSHED (durable)
        // tables — its manifest publish succeeds and lands on disk.
        put_at(&mut runtime, branch(), b"dead-marker", b"gone", 5);
        runtime
            .enqueue_maintenance(&MaintenanceRequest::new(
                MaintenanceTask::Flush,
                MaintenanceScope::Branch(branch()),
            ))
            .expect("enqueue flush");
        runtime.drain_maintenance().expect("drain flush");
        runtime
            .branch(&branch_request(
                victim,
                BranchAction::ForkCurrent { source: branch() },
            ))
            .expect("gen-1 COW fork over durable tables");
        runtime
            .branch(&branch_request(victim, BranchAction::Delete))
            .expect("delete gen-1 victim");
        // A volatile source for gen-2: an eager child of an unflushed branch.
        runtime
            .branch(&create_request(aux))
            .expect("create aux source");
        put_at(&mut runtime, aux, b"aux-row", b"a", 10);
        runtime
            .branch(&branch_request(
                helper,
                BranchAction::ForkCurrent { source: aux },
            ))
            .expect("eager fork of the unflushed aux");
        put_at(&mut runtime, helper, b"helper-own", b"h", 15);
        // Re-create the victim NAME by forking the volatile-backed helper.
        runtime
            .branch(&BranchRequest::new(
                victim,
                BranchAction::ForkCurrent { source: helper },
                Some(BranchGeneration::new(2)),
            ))
            .expect("re-fork victim as gen 2 over a volatile source");
    }

    let runtime = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Always),
        &backend,
    )
    .expect("reopen")
    .into_runtime();
    assert_eq!(
        read_value(&runtime, victim, b"dead-marker"),
        None,
        "the re-forked name must not serve the dead generation's manifest lineage"
    );
    assert_eq!(
        read_value(&runtime, victim, b"aux-row"),
        Some(b"a".to_vec()),
        "gen-2's real transitive inheritance survives reopen"
    );
    assert_eq!(
        read_value(&runtime, victim, b"helper-own"),
        Some(b"h".to_vec()),
        "gen-2's direct inheritance survives reopen"
    );
}
