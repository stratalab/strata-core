//! #3560: a historical (`as_of`) prefix/range scan honours its limit inside the storage merge.
//!
//! Before the fix the `Latest` arm threaded `limit` into the merge while the resolved-bound arm
//! (`AtVersion` / `AtTimestamp` / `after_version`) materialized the whole range and truncated in
//! `map_scan_rows`. These tests pin two things:
//!
//! * **Results are unchanged.** A limited `as_of` page is exactly the prefix of the unbounded
//!   `as_of` scan (the oracle), and cursor continuation concatenates to the oracle, across
//!   tombstones, keys whose newest version is above `as_of`, keys created after `as_of`, TTL rows
//!   expired at the selected frontier, and sources spread over the memtable, L0 and a deeper
//!   level. The oracle itself is cross-checked against point reads at the same bound.
//! * **Work is page-sized** (`perf-trace`): the branch merge emits only the rows up to the
//!   `limit`-th counted row instead of the whole prefix.

use super::*;

use std::time::Duration;

#[cfg(feature = "perf-trace")]
use crate::observability::perf_trace;

const KEYS: usize = 300;

fn branch() -> BranchId {
    StorageRuntime::default_branch_id_for_test()
}

fn space() -> StorageSpaceId {
    StorageSpaceId::new(vec![0x20]).expect("engine storage space")
}

fn key(bytes: &[u8]) -> StorageKey {
    StorageKey::new(bytes.to_vec()).expect("valid API key")
}

fn row_key(index: usize) -> Vec<u8> {
    format!("row-{index:03}").into_bytes()
}

/// Keys that sort between `row-150` and `row-151` and carry a short TTL.
fn ttl_key(index: usize) -> Vec<u8> {
    format!("row-150-t{index:02}").into_bytes()
}

/// Keys first written by the last commit — absent from every earlier `as_of`.
fn late_key(index: usize) -> Vec<u8> {
    format!("row-{index:03}n").into_bytes()
}

fn put(key_bytes: Vec<u8>, value: &[u8], ttl: Option<Duration>) -> CommitMutation {
    CommitMutation::Put {
        storage_space: space(),
        key: StorageKey::new(key_bytes).expect("valid API key"),
        value: StorageValue::new(value.to_vec()),
        ttl,
    }
}

fn delete(key_bytes: Vec<u8>) -> CommitMutation {
    CommitMutation::Delete {
        storage_space: space(),
        key: StorageKey::new(key_bytes).expect("valid API key"),
    }
}

fn commit(
    runtime: &mut StorageRuntime<'static>,
    mutations: Vec<CommitMutation>,
    ts: u64,
) -> CommitSummary {
    let batch = CommitBatch::new(
        branch(),
        mutations,
        CommitOptions::default().require_conflict_check(false),
    )
    .expect("valid batch");
    runtime
        .commit_for_test(&batch, Timestamp::from_micros(ts))
        .expect("commit")
}

fn maintain(runtime: &mut StorageRuntime<'static>, task: MaintenanceTask) {
    runtime
        .maintenance(&MaintenanceRequest::new(
            task,
            MaintenanceScope::Branch(branch()),
        ))
        .expect("maintenance");
}

/// The committed versions of the layered fixture.
struct Fixture {
    runtime: StorageRuntime<'static>,
    /// Every key the fixture ever wrote, sorted.
    universe: Vec<Vec<u8>>,
    v1: CommitVersion,
    v2: CommitVersion,
    v3: CommitVersion,
    v4: CommitVersion,
}

/// Four commits over one prefix, spread across source layers:
///
/// * v1 (ts 10) — put `a` on every `row-NNN`; flushed and compacted to a deeper level.
/// * v2 (ts 20) — put `b` on even keys, delete every `i % 5 == 0` key, put 20 TTL keys that
///   expire at ts 21; flushed to L0.
/// * v3 (ts 30) — put `c` on `i % 3 == 0` (resurrecting some deletes), delete `i % 7 == 0`;
///   stays in the memtable.
/// * v4 (ts 40) — create `row-NNNn` for `i % 4 == 0`; memtable.
fn layered_fixture() -> Fixture {
    let mut runtime = StorageRuntime::open_ephemeral()
        .expect("open ephemeral runtime")
        .into_runtime();
    let mut universe = Vec::new();

    let v1 = commit(
        &mut runtime,
        (0..KEYS).map(|i| put(row_key(i), b"a", None)).collect(),
        10,
    )
    .commit_version();
    universe.extend((0..KEYS).map(row_key));
    maintain(&mut runtime, MaintenanceTask::Flush);
    maintain(&mut runtime, MaintenanceTask::Compact);

    let mut v2_mutations = Vec::new();
    for i in 0..KEYS {
        if i % 5 == 0 {
            v2_mutations.push(delete(row_key(i)));
        } else if i % 2 == 0 {
            v2_mutations.push(put(row_key(i), b"b", None));
        }
    }
    for j in 0..20 {
        v2_mutations.push(put(ttl_key(j), b"ttl", Some(Duration::from_micros(1))));
        universe.push(ttl_key(j));
    }
    let v2 = commit(&mut runtime, v2_mutations, 20).commit_version();
    maintain(&mut runtime, MaintenanceTask::Flush);

    let mut v3_mutations = Vec::new();
    for i in 0..KEYS {
        if i % 3 == 0 {
            v3_mutations.push(put(row_key(i), b"c", None));
        } else if i % 7 == 0 {
            v3_mutations.push(delete(row_key(i)));
        }
    }
    let v3 = commit(&mut runtime, v3_mutations, 30).commit_version();

    let v4 = commit(
        &mut runtime,
        (0..KEYS)
            .filter(|i| i % 4 == 0)
            .map(|i| put(late_key(i), b"d", None))
            .collect(),
        40,
    )
    .commit_version();
    universe.extend((0..KEYS).filter(|i| i % 4 == 0).map(late_key));
    universe.sort();

    let layout = runtime
        .branch_source_layout_for_test(branch())
        .expect("source layout");
    assert_eq!(layout.owned_l0_tables(), 1, "v2 lives in L0");
    assert!(
        layout.owned_total_tables() >= 2,
        "v1 lives below L0: {} total tables",
        layout.owned_total_tables()
    );

    Fixture {
        runtime,
        universe,
        v1,
        v2,
        v3,
        v4,
    }
}

fn prefix_scan(
    runtime: &StorageRuntime<'static>,
    bound: ReadBound,
    limit: Option<usize>,
) -> Vec<StorageReadRow> {
    runtime
        .scan_prefix(&PrefixScanReadRequest::new(
            branch(),
            space(),
            key(b"row-"),
            bound,
            limit.map(|limit| ReadLimit::new(limit).expect("non-zero limit")),
        ))
        .expect("prefix scan")
        .rows()
        .to_vec()
}

fn range_page(
    runtime: &StorageRuntime<'static>,
    start: &[u8],
    bound: ReadBound,
    limit: usize,
) -> Vec<StorageReadRow> {
    runtime
        .scan_range(&ScanReadRequest::new(
            branch(),
            space(),
            ScanRange::new(Some(key(start)), Some(key(b"row-~"))).expect("valid range"),
            bound,
            Some(ReadLimit::new(limit).expect("non-zero limit")),
        ))
        .expect("range page")
        .rows()
        .to_vec()
}

/// Pages the range with cursor continuation until an empty page.
fn paged_range(
    runtime: &StorageRuntime<'static>,
    bound: ReadBound,
    limit: usize,
) -> Vec<StorageReadRow> {
    let mut rows = Vec::new();
    let mut start = b"row-".to_vec();
    loop {
        let page = range_page(runtime, &start, bound, limit);
        assert!(page.len() <= limit, "a page never exceeds its limit");
        let Some(last) = page.last() else {
            break;
        };
        start = last.key().as_bytes().to_vec();
        start.push(0);
        rows.extend(page);
    }
    rows
}

fn bounds_under_test(fixture: &Fixture) -> Vec<ReadBound> {
    vec![
        ReadBound::AtVersion(fixture.v1),
        ReadBound::AtVersion(fixture.v2),
        ReadBound::AtVersion(fixture.v3),
        ReadBound::AtVersion(fixture.v4),
        ReadBound::AtTimestamp(Timestamp::from_micros(25)),
        ReadBound::AtTimestamp(Timestamp::from_micros(35)),
    ]
}

/// The unbounded `as_of` scan agrees key-for-key with point reads at the same bound, so it is a
/// trustworthy oracle for the limited pages.
#[test]
fn unbounded_as_of_scan_matches_point_reads_at_every_bound() {
    let fixture = layered_fixture();
    for bound in bounds_under_test(&fixture) {
        let oracle = prefix_scan(&fixture.runtime, bound, None);
        let mut expected = Vec::new();
        for key_bytes in &fixture.universe {
            let point = fixture
                .runtime
                .read_point(&PointReadRequest::new(
                    branch(),
                    space(),
                    key(key_bytes),
                    bound,
                ))
                .expect("point read");
            if let Some(row) = point.row() {
                expected.push(row.clone());
            }
        }
        assert_eq!(
            oracle, expected,
            "oracle diverges from point reads at {bound:?}"
        );
    }
}

/// Direction controls on the oracle's content: an older version is served when the newest is
/// above `as_of`, deletes surface as tombstones, keys created later are absent, and TTL rows
/// expired at the selected frontier are dropped.
#[test]
fn as_of_scan_serves_the_version_at_the_bound() {
    let fixture = layered_fixture();
    let at_v2 = prefix_scan(&fixture.runtime, ReadBound::AtVersion(fixture.v2), None);
    let find = |rows: &[StorageReadRow], k: &[u8]| {
        rows.iter().find(|row| row.key().as_bytes() == k).cloned()
    };
    let value = |row: StorageReadRow| row.value().expect("put row").as_bytes().to_vec();

    // row-003: `a` at v1, `c` at v3 — v2 must serve the older `a`.
    assert_eq!(value(find(&at_v2, b"row-003").expect("row-003")), b"a");
    // row-004: `b` at v2.
    assert_eq!(value(find(&at_v2, b"row-004").expect("row-004")), b"b");
    // row-005: deleted at v2.
    assert!(find(&at_v2, b"row-005").expect("row-005").is_tombstone());
    // row-000n: created at v4.
    assert!(find(&at_v2, b"row-000n").is_none());
    // TTL rows are live at v2 (ts 20 < expiry 21) …
    assert!(find(&at_v2, &ttl_key(0)).is_some());
    // … and expired at v3 (ts 30).
    let at_v3 = prefix_scan(&fixture.runtime, ReadBound::AtVersion(fixture.v3), None);
    assert!(find(&at_v3, &ttl_key(0)).is_none());
    // row-015: deleted at v2, resurrected with `c` at v3.
    assert_eq!(value(find(&at_v3, b"row-015").expect("row-015")), b"c");
    // row-007: deleted at v3.
    assert!(find(&at_v3, b"row-007").expect("row-007").is_tombstone());
}

/// A limited `as_of` prefix page is exactly the oracle's prefix, for every bound and limit.
#[test]
fn limited_as_of_prefix_scan_is_the_oracle_prefix() {
    let fixture = layered_fixture();
    for bound in bounds_under_test(&fixture) {
        let oracle = prefix_scan(&fixture.runtime, bound, None);
        for limit in [1, 2, 5, 17, 64, 150, 1_000] {
            let page = prefix_scan(&fixture.runtime, bound, Some(limit));
            let expected = &oracle[..limit.min(oracle.len())];
            assert_eq!(page, expected, "prefix page at {bound:?} limit {limit}");
        }
    }
}

/// Cursor continuation over limited `as_of` range pages concatenates to the oracle — no key
/// skipped, none repeated — including across the TTL block and layer boundaries.
#[test]
fn as_of_range_continuation_concatenates_to_the_oracle() {
    let fixture = layered_fixture();
    for bound in bounds_under_test(&fixture) {
        let oracle = prefix_scan(&fixture.runtime, bound, None);
        for limit in [1, 3, 7, 64] {
            let paged = paged_range(&fixture.runtime, bound, limit);
            assert_eq!(paged, oracle, "paged range at {bound:?} limit {limit}");
        }
    }
}

/// The merge's limit count uses the same TTL frontier the mapping filters with: a run of rows
/// expired at the selected frontier ahead of the live rows (and no tombstones to pad the page)
/// must not fill the page. Counting expired rows would stop the merge on them and return an
/// empty or short page.
#[test]
fn limited_as_of_scan_skips_rows_expired_at_the_selected_frontier() {
    let mut runtime = StorageRuntime::open_ephemeral()
        .expect("open ephemeral runtime")
        .into_runtime();
    let expiring = (0..10)
        .map(|i| {
            put(
                format!("exp-{i:02}").into_bytes(),
                b"old",
                Some(Duration::from_micros(1)),
            )
        })
        .collect();
    commit(&mut runtime, expiring, 10);
    let live = (10..20)
        .map(|i| put(format!("exp-{i:02}").into_bytes(), b"new", None))
        .collect();
    let at = commit(&mut runtime, live, 20).commit_version();

    let bound = ReadBound::AtVersion(at);
    let request = |limit: Option<usize>| {
        PrefixScanReadRequest::new(
            branch(),
            space(),
            key(b"exp-"),
            bound,
            limit.map(|limit| ReadLimit::new(limit).expect("non-zero limit")),
        )
    };
    let oracle = runtime
        .scan_prefix(&request(None))
        .expect("oracle scan")
        .rows()
        .to_vec();
    assert_eq!(
        oracle.len(),
        10,
        "only the ten live rows survive the frontier"
    );
    for limit in [1, 5, 10] {
        let page = runtime
            .scan_prefix(&request(Some(limit)))
            .expect("limited scan")
            .rows()
            .to_vec();
        assert_eq!(page, &oracle[..limit], "limit {limit}");
    }
    let mut start = b"exp-".to_vec();
    let mut paged = Vec::new();
    loop {
        let page = runtime
            .scan_range(&ScanReadRequest::new(
                branch(),
                space(),
                ScanRange::new(Some(key(&start)), Some(key(b"exp-~"))).expect("valid range"),
                bound,
                Some(ReadLimit::new(3).expect("non-zero limit")),
            ))
            .expect("range page")
            .rows()
            .to_vec();
        let Some(last) = page.last() else {
            break;
        };
        start = last.key().as_bytes().to_vec();
        start.push(0);
        paged.extend(page);
    }
    assert_eq!(paged, oracle);
}

/// `after_version` prefix reads take the resolved-bound arm too: the limited read is the prefix
/// of the unbounded one.
#[test]
fn limited_after_version_prefix_scan_is_the_oracle_prefix() {
    let fixture = layered_fixture();
    let scan = |limit: Option<usize>| {
        fixture
            .runtime
            .scan_prefix(
                &PrefixScanReadRequest::new(
                    branch(),
                    space(),
                    key(b"row-"),
                    ReadBound::AtVersion(fixture.v4),
                    limit.map(|limit| ReadLimit::new(limit).expect("non-zero limit")),
                )
                .with_after_version(fixture.v1),
            )
            .expect("after_version scan")
            .rows()
            .to_vec()
    };
    let oracle = scan(None);
    assert!(
        oracle.iter().all(|row| row.commit_version() > fixture.v1),
        "after_version excludes v1 rows"
    );
    for limit in [1, 4, 33] {
        assert_eq!(scan(Some(limit)), &oracle[..limit.min(oracle.len())]);
    }
}

/// MVCC-009: the limited `as_of` scan still RAISES below the retained-history floor — the limit
/// never turns an unavailable version into a clamped page.
#[test]
fn limited_as_of_scan_below_the_retained_floor_still_raises() {
    let mut fixture = layered_fixture();
    fixture
        .runtime
        .set_retained_history_floor_for_test(branch(), fixture.v2)
        .expect("set retained history floor");

    let below = fixture
        .runtime
        .scan_range(&ScanReadRequest::new(
            branch(),
            space(),
            ScanRange::new(Some(key(b"row-")), Some(key(b"row-~"))).expect("valid range"),
            ReadBound::AtVersion(fixture.v1),
            Some(ReadLimit::new(1).expect("non-zero limit")),
        ))
        .expect_err("below-floor as_of scan is unavailable");
    assert_eq!(below.class(), StorageApiErrorClass::HistoryUnavailable);

    let at_floor = range_page(
        &fixture.runtime,
        b"row-",
        ReadBound::AtVersion(fixture.v2),
        1,
    );
    assert_eq!(at_floor.len(), 1, "at-floor page still resolves");
}

/// How many merge-emitted rows a page with `limit` needs: every oracle entry up to and
/// including the `limit`-th live one (tombstones are emitted but never fill a slot).
#[cfg(feature = "perf-trace")]
fn rows_needed_for_page(oracle: &[StorageReadRow], limit: usize) -> usize {
    let mut live = 0;
    for (index, row) in oracle.iter().enumerate() {
        if !row.is_tombstone() {
            live += 1;
            if live == limit {
                return index + 1;
            }
        }
    }
    oracle.len()
}

/// The work bound itself: a limited `as_of` page makes the storage merge emit only the rows the
/// page needs, not the whole prefix. Before #3560 every row below was the full prefix.
#[cfg(feature = "perf-trace")]
#[test]
fn limited_as_of_scan_does_page_sized_merge_work() {
    let fixture = layered_fixture();
    for bound in bounds_under_test(&fixture) {
        let oracle = prefix_scan(&fixture.runtime, bound, None);
        assert!(oracle.len() > 250, "the prefix is large at {bound:?}");
        for limit in [1, 5, 20] {
            let needed = rows_needed_for_page(&oracle, limit);

            let capture = perf_trace::begin_test_capture();
            let page = prefix_scan(&fixture.runtime, bound, Some(limit));
            let prefix_perf = perf_trace::snapshot();
            drop(capture);
            assert_eq!(page.len(), limit);
            assert_eq!(
                prefix_perf.scan_rows_returned(),
                u64::try_from(needed).expect("fits"),
                "prefix merge emitted more than the page needs at {bound:?} limit {limit}"
            );

            let capture = perf_trace::begin_test_capture();
            let page = range_page(&fixture.runtime, b"row-", bound, limit);
            let range_perf = perf_trace::snapshot();
            drop(capture);
            assert_eq!(page.len(), limit);
            assert_eq!(
                range_perf.scan_rows_returned(),
                u64::try_from(needed).expect("fits"),
                "range merge emitted more than the page needs at {bound:?} limit {limit}"
            );
            // Each emitted key has at most four versions in the fixture; the merge may look at
            // one key past the page, so the candidates visited stay page-sized too.
            let visited_ceiling = u64::try_from((needed + 1) * 4).expect("fits");
            assert!(
                range_perf.scan_rows_visited() <= visited_ceiling,
                "range merge visited {} rows for a {limit}-row page at {bound:?}",
                range_perf.scan_rows_visited()
            );
        }
    }
}
