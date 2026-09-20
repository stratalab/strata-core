//! #2521/#2522 regression suite: fork state and timeline coverage across
//! RESTARTS. Every prior fork test was in-process; both defects only
//! manifested through recovery (catalog restore rebuilt a forked branch's
//! retained-timeline index as complete-but-EMPTY, erasing inherited pre-fork
//! coverage; the engine then silently forked an "empty" source into an
//! unparented empty child). These tests drive fork → own write → close →
//! reopen — the CLI's one-process-per-command shape — and pin:
//! fork-of-a-fork inheritance (#2521), pre-fork as-of resolution on forks
//! and grandforks (#2522), and empty-source fork-at-zero semantics with
//! parent linkage intact. Gated on `localfs` (durable reopen is the point).

use super::*;

fn open_durable_runtime(root: std::path::PathBuf) -> StorageRuntime<'static> {
    StorageRuntime::open_local(root)
        .expect("open durable runtime")
        .into_runtime()
}

fn default_branch() -> BranchId {
    StorageRuntime::default_branch_id_for_test()
}

fn fork_branch_id(byte: u8) -> BranchId {
    BranchId::from_bytes([byte; BranchId::BYTE_LEN])
}

fn engine_space() -> StorageSpaceId {
    StorageSpaceId::new(vec![0x20]).expect("engine storage space")
}

fn api_key(bytes: &[u8]) -> StorageKey {
    StorageKey::new(bytes.to_vec()).expect("valid API key")
}

fn put(runtime: &mut StorageRuntime<'static>, branch_id: BranchId, value: &[u8], ts: u64) {
    let batch = CommitBatch::new(
        branch_id,
        vec![CommitMutation::Put {
            storage_space: engine_space(),
            key: api_key(b"city"),
            value: StorageValue::new(value.to_vec()),
            ttl: None,
        }],
        CommitOptions::default().require_conflict_check(false),
    )
    .expect("valid put batch");
    runtime
        .commit_for_test(&batch, Timestamp::from_micros(ts))
        .expect("commit put");
}

fn fork_current(runtime: &mut StorageRuntime<'static>, child: BranchId, source: BranchId) {
    runtime
        .branch(&BranchRequest::new(
            child,
            BranchAction::ForkCurrent { source },
            Some(BranchGeneration::new(1)),
        ))
        .expect("fork current");
}

fn read_at(
    runtime: &StorageRuntime<'static>,
    branch_id: BranchId,
    bound: ReadBound,
) -> Option<Vec<u8>> {
    runtime
        .read_point(&PointReadRequest::new(
            branch_id,
            engine_space(),
            api_key(b"city"),
            bound,
        ))
        .expect("point read")
        .row()
        .map(|row| row.value().expect("put row").as_bytes().to_vec())
}

/// #2521: the CLI repro shape, one reopen per step. The grandchild must read
/// the middle branch's post-fork write; the middle branch stays intact.
#[test]
fn fork_of_a_fork_inherits_the_middle_branch_across_reopens() {
    let root = temp_dir_for_api_test("fork-of-fork-reopen");
    let feature = fork_branch_id(0xA1);
    let grandchild = fork_branch_id(0xA2);

    {
        let mut runtime = open_durable_runtime(root.clone());
        put(&mut runtime, default_branch(), b"paris", 10);
        runtime.close().expect("close");
    }
    {
        let mut runtime = open_durable_runtime(root.clone());
        fork_current(&mut runtime, feature, default_branch());
        runtime.close().expect("close");
    }
    {
        let mut runtime = open_durable_runtime(root.clone());
        put(&mut runtime, feature, b"tokyo", 20);
        runtime.close().expect("close");
    }
    {
        let mut runtime = open_durable_runtime(root.clone());
        fork_current(&mut runtime, grandchild, feature);
        runtime.close().expect("close");
    }

    let runtime = open_durable_runtime(root);
    assert_eq!(
        read_at(&runtime, grandchild, ReadBound::Latest),
        Some(b"tokyo".to_vec()),
        "the grandchild must inherit the middle branch's post-fork write"
    );
    assert_eq!(
        read_at(&runtime, feature, ReadBound::Latest),
        Some(b"tokyo".to_vec()),
        "the middle branch stays intact"
    );
    assert_eq!(
        read_at(&runtime, default_branch(), ReadBound::Latest),
        Some(b"paris".to_vec()),
        "the root branch stays intact"
    );
}

/// #2522: a fork (and a grandfork) reopened from disk must resolve an as-of
/// read at a PRE-fork timestamp through the inherited timeline coverage.
#[test]
fn fork_resolves_pre_fork_as_of_across_reopens() {
    let root = temp_dir_for_api_test("fork-as-of-reopen");
    let fork = fork_branch_id(0xB1);
    let grand = fork_branch_id(0xB2);

    {
        let mut runtime = open_durable_runtime(root.clone());
        put(&mut runtime, default_branch(), b"paris", 10);
        put(&mut runtime, default_branch(), b"london", 20);
        runtime.close().expect("close");
    }
    {
        let mut runtime = open_durable_runtime(root.clone());
        fork_current(&mut runtime, fork, default_branch());
        runtime.close().expect("close");
    }
    {
        let mut runtime = open_durable_runtime(root.clone());
        put(&mut runtime, fork, b"tokyo", 30);
        runtime.close().expect("close");
    }
    {
        let mut runtime = open_durable_runtime(root.clone());
        fork_current(&mut runtime, grand, fork);
        runtime.close().expect("close");
    }

    let runtime = open_durable_runtime(root);
    let t1 = ReadBound::AtTimestamp(Timestamp::from_micros(10));
    assert_eq!(
        read_at(&runtime, default_branch(), t1),
        Some(b"paris".to_vec()),
        "parent as-of t1"
    );
    assert_eq!(
        read_at(&runtime, fork, t1),
        Some(b"paris".to_vec()),
        "fork as-of a pre-fork timestamp resolves through inherited coverage"
    );
    assert_eq!(
        read_at(&runtime, grand, t1),
        Some(b"paris".to_vec()),
        "grandfork as-of a pre-fork timestamp resolves through two hops"
    );
    assert_eq!(
        read_at(&runtime, grand, ReadBound::Latest),
        Some(b"tokyo".to_vec()),
        "grandfork head reads the middle branch's write"
    );
    // fork == as-of-V equivalence: the fork's own post-fork write is visible
    // at its own timestamp and invisible before it.
    assert_eq!(
        read_at(
            &runtime,
            fork,
            ReadBound::AtTimestamp(Timestamp::from_micros(30))
        ),
        Some(b"tokyo".to_vec()),
    );
    assert_eq!(
        read_at(
            &runtime,
            fork,
            ReadBound::AtTimestamp(Timestamp::from_micros(20))
        ),
        Some(b"london".to_vec()),
    );
}

/// #2521 (engine fallback removal): forking a branch with NO commit history
/// is the legitimate empty-fork case — storage forks at version zero and the
/// child keeps its parent linkage instead of degrading to an unparented
/// create.
#[test]
fn fork_of_an_empty_branch_keeps_parent_linkage() {
    let root = temp_dir_for_api_test("fork-empty-source");
    let empty_parent = fork_branch_id(0xC1);
    let child = fork_branch_id(0xC2);

    let mut runtime = open_durable_runtime(root);
    runtime
        .branch(&BranchRequest::new(
            empty_parent,
            BranchAction::Create,
            Some(BranchGeneration::new(1)),
        ))
        .expect("create empty parent");
    fork_current(&mut runtime, child, empty_parent);
    let described = runtime
        .branch(&BranchRequest::new(child, BranchAction::Describe, None))
        .expect("describe child");
    let summary = described.branches().first().expect("child summary");
    let parent = summary.parent().expect("child must keep parent linkage");
    assert_eq!(parent.source_branch_id(), empty_parent);
    assert_eq!(parent.fork_version(), CommitVersion::ZERO);
    assert_eq!(read_at(&runtime, child, ReadBound::Latest), None);
}

fn put_key(
    runtime: &mut StorageRuntime<'static>,
    branch_id: BranchId,
    key: &[u8],
    value: &[u8],
    ts: u64,
) {
    let batch = CommitBatch::new(
        branch_id,
        vec![CommitMutation::Put {
            storage_space: engine_space(),
            key: api_key(key),
            value: StorageValue::new(value.to_vec()),
            ttl: None,
        }],
        CommitOptions::default().require_conflict_check(false),
    )
    .expect("valid put batch");
    runtime
        .commit_for_test(&batch, Timestamp::from_micros(ts))
        .expect("commit put");
}

fn read_key_at(
    runtime: &StorageRuntime<'static>,
    branch_id: BranchId,
    key: &[u8],
    bound: ReadBound,
) -> Option<Vec<u8>> {
    runtime
        .read_point(&PointReadRequest::new(
            branch_id,
            engine_space(),
            api_key(key),
            bound,
        ))
        .expect("point read")
        .row()
        .map(|row| row.value().expect("put row").as_bytes().to_vec())
}

/// #2527: `fork_current` with unflushed rows takes the HYBRID COW path —
/// sealed rows ride the inherited layer, the unsealed slice becomes one
/// durably-published child L0 table — and the child reads BOTH across a
/// reopen (the slice is covered by the fork-time child manifest; recovery's
/// rebuild correctly skips layered children).
#[test]
fn fork_with_unflushed_rows_is_cow_and_survives_reopen() {
    let root = temp_dir_for_api_test("hybrid-fork-reopen");
    let child = fork_branch_id(0x31);
    {
        let mut runtime = open_durable_runtime(root.clone());
        put_key(
            &mut runtime,
            default_branch(),
            b"sealed",
            b"sealed-val",
            1_000,
        );
        runtime
            .flush_default_branch_for_test()
            .expect("flush sealed rows");
        put_key(
            &mut runtime,
            default_branch(),
            b"unsealed",
            b"unsealed-val",
            2_000,
        );
        fork_current(&mut runtime, child, default_branch());

        // The child sees the sealed row (via the COW layer) AND the unsealed
        // row (via the published slice) at fork time.
        assert_eq!(
            read_key_at(&runtime, child, b"sealed", ReadBound::Latest),
            Some(b"sealed-val".to_vec()),
        );
        assert_eq!(
            read_key_at(&runtime, child, b"unsealed", ReadBound::Latest),
            Some(b"unsealed-val".to_vec()),
        );

        // Post-fork divergence stays isolated in both directions.
        put_key(
            &mut runtime,
            default_branch(),
            b"unsealed",
            b"parent-after",
            3_000,
        );
        put_key(&mut runtime, child, b"sealed", b"child-after", 3_500);
        assert_eq!(
            read_key_at(&runtime, child, b"unsealed", ReadBound::Latest),
            Some(b"unsealed-val".to_vec()),
            "parent writes after the fork must not leak into the child",
        );
        assert_eq!(
            read_key_at(&runtime, default_branch(), b"sealed", ReadBound::Latest),
            Some(b"sealed-val".to_vec()),
            "child writes must not leak into the parent",
        );
        runtime.close().expect("close before reopen");
    }

    // The CLI shape: a fresh process. The child's unsealed slice must be
    // durable (tiny sessions never checkpoint; the parent's WAL replays into
    // the PARENT, so only the fork-time manifest covers the child).
    let runtime = open_durable_runtime(root);
    assert_eq!(
        read_key_at(&runtime, child, b"sealed", ReadBound::Latest),
        Some(b"child-after".to_vec()),
        "the child's own post-fork write survives the reopen",
    );
    assert_eq!(
        read_key_at(&runtime, child, b"unsealed", ReadBound::Latest),
        Some(b"unsealed-val".to_vec()),
        "the fork-time unsealed slice survives the reopen",
    );
}

/// #3494 safety guard: WAL reclaim must NOT drop time-travel history. Opens
/// with an aggressive reclaim policy (checkpoint + truncation on every commit)
/// so the reclaim rotation fires constantly, then asserts a PRE-fork as-of read
/// still resolves across a reopen. An earlier boot-time reclaim variant failed
/// exactly this (pruned pre-fork timeline history); operation-time reclaim must
/// preserve it. Deterministic-inline scheduling makes the reclaim run in-thread.
#[test]
fn aggressive_reclaim_preserves_pre_fork_as_of_across_reopens() {
    fn open_reclaiming(root: std::path::PathBuf) -> StorageRuntime<'static> {
        let backend: &'static StorageBackend = Box::leak(Box::new(StorageBackend::local_fs(root)));
        StorageRuntime::open_with_backend(
            StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
                .with_maintenance_scheduling_policy(
                    StorageMaintenanceSchedulingPolicy::DeterministicInline,
                )
                .with_wal_growth_policy(StorageWalGrowthPolicy::Thresholds {
                    max_retained_wal_bytes: 1,
                    max_retained_wal_segments: 1,
                    max_commits_since_checkpoint: 1,
                }),
            backend,
        )
        .expect("open reclaiming runtime")
        .into_runtime()
    }

    let root = temp_dir_for_api_test("reclaim-as-of-reopen");
    let fork = fork_branch_id(0xC1);

    {
        let mut runtime = open_reclaiming(root.clone());
        put(&mut runtime, default_branch(), b"paris", 10);
        put(&mut runtime, default_branch(), b"london", 20);
        runtime.drain_maintenance().expect("drain reclaim");
        runtime.close().expect("close");
    }
    {
        let mut runtime = open_reclaiming(root.clone());
        fork_current(&mut runtime, fork, default_branch());
        put(&mut runtime, fork, b"tokyo", 30);
        runtime.drain_maintenance().expect("drain reclaim");
        runtime.close().expect("close");
    }

    let runtime = open_reclaiming(root);
    let pre_fork = ReadBound::AtTimestamp(Timestamp::from_micros(10));
    assert_eq!(
        read_at(&runtime, default_branch(), pre_fork),
        Some(b"paris".to_vec()),
        "parent pre-fork as-of survives aggressive reclaim",
    );
    assert_eq!(
        read_at(&runtime, fork, pre_fork),
        Some(b"paris".to_vec()),
        "fork pre-fork as-of survives aggressive reclaim (the #3494 regression)",
    );
    assert_eq!(
        read_at(
            &runtime,
            fork,
            ReadBound::AtTimestamp(Timestamp::from_micros(20))
        ),
        Some(b"london".to_vec()),
        "mid-history as-of survives aggressive reclaim",
    );
    assert_eq!(
        read_at(&runtime, fork, ReadBound::Latest),
        Some(b"tokyo".to_vec()),
        "the fork head still reads its own write",
    );
}
