use super::*;

use std::time::Duration;

use crate::branch::read::BranchTimestampCoverage;
#[cfg(feature = "perf-trace")]
use crate::observability::perf_trace;

fn open_runtime() -> StorageRuntime<'static> {
    StorageRuntime::open_ephemeral()
        .expect("open ephemeral runtime")
        .into_runtime()
}

#[cfg(feature = "perf-trace")]
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
fn open_durable_runtime(root: std::path::PathBuf) -> StorageRuntime<'static> {
    StorageRuntime::open_local(root)
        .expect("open durable runtime")
        .into_runtime()
}

fn branch() -> BranchId {
    StorageRuntime::default_branch_id_for_test()
}

fn other_branch() -> BranchId {
    branch_id(0x44)
}

fn engine_space() -> StorageSpaceId {
    StorageSpaceId::new(vec![0x20]).expect("engine storage space")
}

fn api_key(bytes: &[u8]) -> StorageKey {
    StorageKey::new(bytes.to_vec()).expect("valid API key")
}

fn put_batch(key: &[u8], value: &[u8]) -> CommitBatch {
    put_batch_with_ttl(key, value, None)
}

fn put_batch_with_ttl(key: &[u8], value: &[u8], ttl: Option<Duration>) -> CommitBatch {
    CommitBatch::new(
        branch(),
        vec![CommitMutation::Put {
            storage_space: engine_space(),
            key: api_key(key),
            value: StorageValue::new(value.to_vec()),
            ttl,
        }],
        CommitOptions::default().require_conflict_check(false),
    )
    .expect("valid put batch")
}

fn delete_batch(key: &[u8]) -> CommitBatch {
    CommitBatch::new(
        branch(),
        vec![CommitMutation::Delete {
            storage_space: engine_space(),
            key: api_key(key),
        }],
        CommitOptions::default().require_conflict_check(false),
    )
    .expect("valid delete batch")
}

fn commit_put(
    runtime: &mut StorageRuntime<'static>,
    key: &[u8],
    value: &[u8],
    ts: u64,
) -> CommitSummary {
    runtime
        .commit_for_test(&put_batch(key, value), Timestamp::from_micros(ts))
        .expect("commit put")
}

fn commit_put_with_ttl(
    runtime: &mut StorageRuntime<'static>,
    key: &[u8],
    value: &[u8],
    ts: u64,
    ttl: Duration,
) -> CommitSummary {
    runtime
        .commit_for_test(
            &put_batch_with_ttl(key, value, Some(ttl)),
            Timestamp::from_micros(ts),
        )
        .expect("commit put with ttl")
}

fn commit_delete(runtime: &mut StorageRuntime<'static>, key: &[u8], ts: u64) -> CommitSummary {
    runtime
        .commit_for_test(&delete_batch(key), Timestamp::from_micros(ts))
        .expect("commit delete")
}

fn point_request(key: &[u8], bound: ReadBound) -> PointReadRequest {
    PointReadRequest::new(branch(), engine_space(), api_key(key), bound)
}

fn point_request_for(branch_id: BranchId, key: &[u8], bound: ReadBound) -> PointReadRequest {
    PointReadRequest::new(branch_id, engine_space(), api_key(key), bound)
}

fn read_value(row: &StorageReadRow) -> &[u8] {
    row.value().expect("put row").as_bytes()
}

#[test]
fn commit_summary_carries_the_committed_at_from_the_batch_options() {
    // #3112: a wall-clock `committed_at` supplied on the batch options must ride
    // the whole commit path — options -> stamp -> outcome -> summary — out to the
    // caller unchanged, and stay separate from the logical commit timestamp.
    let mut runtime = open_runtime();
    let committed_at = Timestamp::from_micros(1_788_000_000_000_000);
    let batch = CommitBatch::new(
        branch(),
        vec![CommitMutation::Put {
            storage_space: engine_space(),
            key: api_key(b"k"),
            value: StorageValue::new(b"v".to_vec()),
            ttl: None,
        }],
        CommitOptions::default()
            .require_conflict_check(false)
            .with_committed_at(committed_at),
    )
    .expect("valid put batch");
    let summary = runtime
        .commit_for_test(&batch, Timestamp::from_micros(10))
        .expect("commit succeeds");
    assert_eq!(summary.committed_at(), Some(committed_at));
    assert_eq!(summary.commit_timestamp(), Timestamp::from_micros(10));

    // Direction control: a batch that supplies no committed_at leaves it unset.
    let bare_summary = commit_put(&mut runtime, b"k2", b"v2", 20);
    assert_eq!(bare_summary.committed_at(), None);
}

#[test]
fn read_latest_returns_newest_visible_value() {
    let mut runtime = open_runtime();
    commit_put(&mut runtime, b"alpha", b"old", 10);
    let latest = commit_put(&mut runtime, b"alpha", b"new", 20);

    let outcome = runtime
        .read_point(&point_request(b"alpha", ReadBound::Latest))
        .expect("read latest");
    let row = outcome.row().expect("row present");
    assert_eq!(read_value(row), b"new");
    assert_eq!(row.commit_version(), latest.commit_version());
    assert_eq!(row.commit_timestamp(), latest.commit_timestamp());
    assert!(!row.is_tombstone());
    // B4: the moved exit is identical to the borrowed peek.
    let peeked = outcome.row().cloned();
    assert_eq!(outcome.into_row(), peeked);
}

#[test]
fn read_latest_returns_none_for_absent_key() {
    let mut runtime = open_runtime();
    commit_put(&mut runtime, b"alpha", b"value", 10);

    let outcome = runtime
        .read_point(&point_request(b"missing", ReadBound::Latest))
        .expect("read absent");
    assert!(outcome.row().is_none());
}

#[test]
fn read_latest_returns_tombstone_fact_for_visible_delete() {
    let mut runtime = open_runtime();
    commit_put(&mut runtime, b"alpha", b"value", 10);
    let deleted = commit_delete(&mut runtime, b"alpha", 20);

    let outcome = runtime
        .read_point(&point_request(b"alpha", ReadBound::Latest))
        .expect("read tombstone");
    let row = outcome.row().expect("tombstone fact");
    assert!(row.is_tombstone());
    assert!(row.value().is_none());
    assert_eq!(row.commit_version(), deleted.commit_version());
    // B4: tombstone facts also survive the moved exit unchanged.
    let peeked = outcome.row().cloned();
    assert_eq!(outcome.into_row(), peeked);
}

#[cfg(feature = "perf-trace")]
#[test]
fn read_latest_uses_borrowed_bounded_point_path() {
    let mut runtime = open_manual_runtime();
    for index in 0..64u8 {
        let key = vec![b'k', index];
        let value = vec![b'v', index];
        commit_put(&mut runtime, &key, &value, u64::from(index) + 1);
    }

    let _perf_trace = perf_trace::begin_test_capture();
    for index in 0..64u8 {
        let key = vec![b'k', index];
        let outcome = runtime
            .read_point(&point_request(&key, ReadBound::Latest))
            .expect("latest read");
        assert_eq!(
            read_value(outcome.row().expect("row present")),
            &[b'v', index]
        );
    }

    let snapshot = perf_trace::snapshot();
    assert_eq!(snapshot.read_view_captures(), 0);
    assert_eq!(snapshot.read_view_rows_cloned(), 0);
    assert_eq!(snapshot.point_rows_visited(), 64);
    assert_eq!(snapshot.point_candidates_materialized(), 64);
    assert_eq!(snapshot.table_seeks(), 64);
}

#[test]
fn read_at_version_returns_exact_retained_value() {
    let mut runtime = open_runtime();
    let first = commit_put(&mut runtime, b"alpha", b"old", 10);
    commit_put(&mut runtime, b"alpha", b"new", 20);

    let outcome = runtime
        .read_point(&point_request(
            b"alpha",
            ReadBound::AtVersion(first.commit_version()),
        ))
        .expect("read version");
    assert_eq!(read_value(outcome.row().expect("row present")), b"old");
}

#[test]
fn read_at_version_uses_latest_at_or_before_version() {
    let mut runtime = open_runtime();
    let first = commit_put(&mut runtime, b"alpha", b"old", 10);
    let second = commit_put(&mut runtime, b"alpha", b"new", 20);
    commit_put(&mut runtime, b"beta", b"separate", 30);

    let outcome = runtime
        .read_point(&point_request(
            b"alpha",
            ReadBound::AtVersion(second.commit_version()),
        ))
        .expect("read version");
    let row = outcome.row().expect("row present");
    assert_eq!(read_value(row), b"new");
    assert!(row.commit_version() > first.commit_version());
}

#[test]
fn read_at_version_rejects_unretained_history() {
    let mut runtime = open_runtime();
    commit_put(&mut runtime, b"alpha", b"value", 10);

    let error = runtime
        .read_point(&point_request(
            b"alpha",
            ReadBound::AtVersion(CommitVersion::ZERO),
        ))
        .expect_err("zero version is before retained timeline");
    assert_eq!(error.class(), StorageApiErrorClass::HistoryUnavailable);
}

#[test]
fn read_at_version_rejects_unrecorded_future_version() {
    let mut runtime = open_runtime();
    commit_put(&mut runtime, b"alpha", b"value", 10);

    let error = runtime
        .read_point(&point_request(
            b"alpha",
            ReadBound::AtVersion(CommitVersion::new(99)),
        ))
        .expect_err("unrecorded version is not a retained frontier");
    assert_eq!(error.class(), StorageApiErrorClass::HistoryUnavailable);
}

#[test]
fn read_at_timestamp_resolves_to_commit_version() {
    let mut runtime = open_runtime();
    let first = commit_put(&mut runtime, b"alpha", b"old", 10);
    commit_put(&mut runtime, b"alpha", b"new", 30);

    let outcome = runtime
        .read_point(&point_request(
            b"alpha",
            ReadBound::AtTimestamp(Timestamp::from_micros(20)),
        ))
        .expect("read timestamp");
    let row = outcome.row().expect("row present");
    assert_eq!(read_value(row), b"old");
    assert_eq!(row.commit_version(), first.commit_version());
}

/// W3.1a oracle: timestamp reads answer identically before and after the
/// retained-timeline index warms. The first probe pass runs on a cold index
/// (scan fallback, which seeds it); the second pass takes the index fast
/// path; the third pass runs after further commits extend the seeded index
/// through the apply-funnel observations. Every answer is checked against
/// the known committed history.
#[test]
fn retained_timeline_index_matches_history_before_and_after_warming() {
    let mut runtime = open_runtime();
    let stamps = [10u64, 11, 15, 40, 90, 200];
    for (index, ts) in stamps.iter().enumerate() {
        commit_put(&mut runtime, b"alpha", format!("v{index}").as_bytes(), *ts);
    }

    let expect_at = |runtime: &StorageRuntime<'static>, probe: u64, history: &[u64]| {
        let outcome = runtime
            .read_point(&point_request(
                b"alpha",
                ReadBound::AtTimestamp(Timestamp::from_micros(probe)),
            ))
            .expect("timestamp read");
        let row = outcome.row().expect("row present");
        let expected_index = history.iter().filter(|ts| **ts <= probe).count() - 1;
        assert_eq!(
            read_value(row),
            format!("v{expected_index}").as_bytes(),
            "probe {probe} must resolve to the last commit at or before it"
        );
    };
    let expect_unavailable = |runtime: &StorageRuntime<'static>, probe: u64| {
        let error = runtime
            .read_point(&point_request(
                b"alpha",
                ReadBound::AtTimestamp(Timestamp::from_micros(probe)),
            ))
            .expect_err("out-of-history probe must reject");
        assert_eq!(error.class(), StorageApiErrorClass::HistoryUnavailable);
    };

    // Pass 1 (cold index: scan fallback seeds it) and pass 2 (index fast
    // path) must agree probe-for-probe.
    for _pass in 0..2 {
        expect_unavailable(&runtime, 9);
        for probe in [10u64, 11, 12, 15, 39, 40, 89, 90, 199, 200] {
            expect_at(&runtime, probe, &stamps);
        }
        expect_unavailable(&runtime, 201);
    }

    // Extend the history: the seeded index grows via apply-funnel
    // observations and keeps answering exactly.
    commit_put(&mut runtime, b"alpha", b"v6", 500);
    let extended = [10u64, 11, 15, 40, 90, 200, 500];
    for probe in [200u64, 250, 500] {
        expect_at(&runtime, probe, &extended);
    }
    expect_unavailable(&runtime, 501);
}

#[test]
fn read_at_timestamp_after_latest_rejects() {
    let mut runtime = open_runtime();
    commit_put(&mut runtime, b"alpha", b"value", 10);

    let error = runtime
        .read_point(&point_request(
            b"alpha",
            ReadBound::AtTimestamp(Timestamp::from_micros(20)),
        ))
        .expect_err("after-latest timestamp read must not clamp to current");
    assert_eq!(error.class(), StorageApiErrorClass::HistoryUnavailable);
}

#[test]
fn read_at_timestamp_rejects_insufficient_history() {
    let mut runtime = open_runtime();
    commit_put(&mut runtime, b"alpha", b"value", 50);
    runtime
        .set_timestamp_coverage_for_test(
            branch(),
            BranchTimestampCoverage::complete_since(Timestamp::from_micros(40)),
        )
        .expect("set coverage");

    let error = runtime
        .read_point(&point_request(
            b"alpha",
            ReadBound::AtTimestamp(Timestamp::from_micros(10)),
        ))
        .expect_err("timestamp before retained history");
    assert_eq!(error.class(), StorageApiErrorClass::HistoryUnavailable);
}

#[test]
fn read_after_close_rejects_closed_runtime() {
    let mut runtime = open_runtime();
    commit_put(&mut runtime, b"alpha", b"value", 10);
    runtime.close().expect("close runtime");

    let error = runtime
        .read_point(&point_request(b"alpha", ReadBound::Latest))
        .expect_err("closed runtime rejected");
    assert_eq!(error.class(), StorageApiErrorClass::FailedPrecondition);

    let error = runtime
        .scan_prefix(&PrefixScanReadRequest::new(
            branch(),
            engine_space(),
            api_key(b"a"),
            ReadBound::Latest,
            None,
        ))
        .expect_err("closed runtime rejects latest prefix scan");
    assert_eq!(error.class(), StorageApiErrorClass::FailedPrecondition);

    let error = runtime
        .scan_range(&ScanReadRequest::new(
            branch(),
            engine_space(),
            ScanRange::new(Some(api_key(b"a")), Some(api_key(b"z"))).expect("valid range"),
            ReadBound::Latest,
            None,
        ))
        .expect_err("closed runtime rejects latest range scan");
    assert_eq!(error.class(), StorageApiErrorClass::FailedPrecondition);
}

#[test]
fn read_unknown_branch_rejects() {
    let mut runtime = open_runtime();
    commit_put(&mut runtime, b"alpha", b"value", 10);

    let error = runtime
        .read_point(&point_request_for(
            other_branch(),
            b"alpha",
            ReadBound::Latest,
        ))
        .expect_err("unknown branch rejected");
    assert_eq!(error.class(), StorageApiErrorClass::NotFound);
}

#[test]
fn read_at_version_applies_ttl_at_selected_frontier() {
    let mut runtime = open_runtime();
    let first = commit_put_with_ttl(
        &mut runtime,
        b"alpha",
        b"value",
        10,
        Duration::from_micros(5),
    );
    let second = commit_put(&mut runtime, b"beta", b"other", 20);

    let before_expiry = runtime
        .read_point(&point_request(
            b"alpha",
            ReadBound::AtVersion(first.commit_version()),
        ))
        .expect("read before expiry");
    assert_eq!(
        read_value(before_expiry.row().expect("row before expiry")),
        b"value"
    );

    let after_expiry = runtime
        .read_point(&point_request(
            b"alpha",
            ReadBound::AtVersion(second.commit_version()),
        ))
        .expect("read after expiry");
    assert!(after_expiry.row().is_none());
}

#[test]
fn read_at_timestamp_applies_ttl_at_matched_commit_frontier() {
    let mut runtime = open_runtime();
    commit_put_with_ttl(
        &mut runtime,
        b"alpha",
        b"value",
        10,
        Duration::from_micros(12),
    );
    commit_put(&mut runtime, b"beta", b"beta", 20);
    commit_put(&mut runtime, b"gamma", b"gamma", 30);

    let outcome = runtime
        .read_point(&point_request(
            b"alpha",
            ReadBound::AtTimestamp(Timestamp::from_micros(25)),
        ))
        .expect("timestamp read between commits");
    assert_eq!(
        read_value(outcome.row().expect("ttl is evaluated at matched commit")),
        b"value"
    );
}

#[test]
fn scan_at_version_applies_ttl_at_selected_frontier() {
    let mut runtime = open_runtime();
    commit_put_with_ttl(&mut runtime, b"item-a", b"a", 10, Duration::from_micros(5));
    let second = commit_put(&mut runtime, b"item-b", b"b", 20);

    let scan = runtime
        .scan_prefix(&PrefixScanReadRequest::new(
            branch(),
            engine_space(),
            api_key(b"item-"),
            ReadBound::AtVersion(second.commit_version()),
            None,
        ))
        .expect("scan after ttl expiry");
    assert_eq!(scan.rows().len(), 1);
    assert_eq!(scan.rows()[0].key().as_bytes(), b"item-b");
}

#[test]
fn history_returns_newest_first() {
    let mut runtime = open_runtime();
    commit_put(&mut runtime, b"alpha", b"one", 10);
    commit_put(&mut runtime, b"alpha", b"two", 20);
    commit_put(&mut runtime, b"alpha", b"three", 30);

    let history = runtime
        .read_history(&HistoryReadRequest::new(
            branch(),
            engine_space(),
            api_key(b"alpha"),
        ))
        .expect("history");
    let values: Vec<&[u8]> = history.rows().iter().map(read_value).collect();
    assert_eq!(
        values,
        vec![b"three".as_slice(), b"two".as_slice(), b"one".as_slice()]
    );
}

#[test]
fn history_limit_is_enforced() {
    let mut runtime = open_runtime();
    commit_put(&mut runtime, b"alpha", b"one", 10);
    commit_put(&mut runtime, b"alpha", b"two", 20);

    let history = runtime
        .read_history(
            &HistoryReadRequest::new(branch(), engine_space(), api_key(b"alpha"))
                .limit(ReadLimit::new(1).expect("valid limit")),
        )
        .expect("history");
    assert_eq!(history.rows().len(), 1);
    assert_eq!(read_value(&history.rows()[0]), b"two");
}

#[test]
fn history_before_version_excludes_newer_versions() {
    let mut runtime = open_runtime();
    let first = commit_put(&mut runtime, b"alpha", b"one", 10);
    let second = commit_put(&mut runtime, b"alpha", b"two", 20);
    commit_put(&mut runtime, b"alpha", b"three", 30);

    let history = runtime
        .read_history(
            &HistoryReadRequest::new(branch(), engine_space(), api_key(b"alpha"))
                .before_version(second.commit_version()),
        )
        .expect("history");
    assert_eq!(history.rows().len(), 1);
    assert_eq!(history.rows()[0].commit_version(), first.commit_version());
}

#[test]
fn history_preserves_tombstone_entries() {
    let mut runtime = open_runtime();
    commit_put(&mut runtime, b"alpha", b"one", 10);
    let deleted = commit_delete(&mut runtime, b"alpha", 20);

    let history = runtime
        .read_history(&HistoryReadRequest::new(
            branch(),
            engine_space(),
            api_key(b"alpha"),
        ))
        .expect("history");
    assert!(history.rows()[0].is_tombstone());
    assert_eq!(history.rows()[0].commit_version(), deleted.commit_version());
}

#[test]
fn history_pruned_versions_return_retention_error() {
    let mut runtime = open_runtime();
    commit_put(&mut runtime, b"alpha", b"one", 10);

    let error = runtime
        .read_history(
            &HistoryReadRequest::new(branch(), engine_space(), api_key(b"alpha"))
                .before_version(CommitVersion::ZERO),
        )
        .expect_err("unretained history rejected");
    assert_eq!(error.class(), StorageApiErrorClass::HistoryUnavailable);
}

#[test]
fn history_empty_key_returns_empty_history() {
    let mut runtime = open_runtime();
    commit_put(&mut runtime, b"alpha", b"one", 10);

    let history = runtime
        .read_history(&HistoryReadRequest::new(
            branch(),
            engine_space(),
            api_key(b"missing"),
        ))
        .expect("history");
    assert!(history.rows().is_empty());
}

#[test]
fn prefix_scan_returns_sorted_keys() {
    let mut runtime = open_runtime();
    commit_put(&mut runtime, b"item-c", b"c", 10);
    commit_put(&mut runtime, b"item-a", b"a", 20);
    commit_put(&mut runtime, b"other", b"x", 30);
    commit_put(&mut runtime, b"item-b", b"b", 40);

    let scan = runtime
        .scan_prefix(&PrefixScanReadRequest::new(
            branch(),
            engine_space(),
            api_key(b"item-"),
            ReadBound::Latest,
            None,
        ))
        .expect("prefix scan");
    let keys: Vec<&[u8]> = scan.rows().iter().map(|row| row.key().as_bytes()).collect();
    assert_eq!(
        keys,
        vec![
            b"item-a".as_slice(),
            b"item-b".as_slice(),
            b"item-c".as_slice()
        ]
    );
}

#[test]
fn prefix_scan_applies_version_bound() {
    let mut runtime = open_runtime();
    let first = commit_put(&mut runtime, b"item-a", b"old", 10);
    commit_put(&mut runtime, b"item-a", b"new", 20);

    let scan = runtime
        .scan_prefix(&PrefixScanReadRequest::new(
            branch(),
            engine_space(),
            api_key(b"item-"),
            ReadBound::AtVersion(first.commit_version()),
            None,
        ))
        .expect("prefix scan");
    assert_eq!(scan.rows().len(), 1);
    assert_eq!(read_value(&scan.rows()[0]), b"old");
}

#[test]
fn prefix_scan_applies_timestamp_bound() {
    let mut runtime = open_runtime();
    commit_put(&mut runtime, b"item-a", b"old", 10);
    commit_put(&mut runtime, b"item-a", b"new", 30);

    let scan = runtime
        .scan_prefix(&PrefixScanReadRequest::new(
            branch(),
            engine_space(),
            api_key(b"item-"),
            ReadBound::AtTimestamp(Timestamp::from_micros(20)),
            None,
        ))
        .expect("prefix scan");
    assert_eq!(read_value(&scan.rows()[0]), b"old");
}

#[test]
fn prefix_scan_limit_is_stable() {
    let mut runtime = open_runtime();
    commit_put(&mut runtime, b"item-b", b"b", 10);
    commit_put(&mut runtime, b"item-a", b"a", 20);
    commit_put(&mut runtime, b"item-c", b"c", 30);

    let scan = runtime
        .scan_prefix(&PrefixScanReadRequest::new(
            branch(),
            engine_space(),
            api_key(b"item-"),
            ReadBound::Latest,
            Some(ReadLimit::new(2).expect("valid limit")),
        ))
        .expect("prefix scan");
    let keys: Vec<&[u8]> = scan.rows().iter().map(|row| row.key().as_bytes()).collect();
    assert_eq!(keys, vec![b"item-a".as_slice(), b"item-b".as_slice()]);
}

#[test]
fn range_scan_respects_start_and_end() {
    let mut runtime = open_runtime();
    for name in [
        b"a".as_slice(),
        b"b".as_slice(),
        b"c".as_slice(),
        b"d".as_slice(),
    ] {
        commit_put(&mut runtime, name, name, 10 + u64::from(name[0]));
    }

    let scan = runtime
        .scan_range(&ScanReadRequest::new(
            branch(),
            engine_space(),
            ScanRange::new(Some(api_key(b"b")), Some(api_key(b"d"))).expect("valid range"),
            ReadBound::Latest,
            None,
        ))
        .expect("range scan");
    let keys: Vec<&[u8]> = scan.rows().iter().map(|row| row.key().as_bytes()).collect();
    assert_eq!(keys, vec![b"b".as_slice(), b"c".as_slice()]);
}

#[test]
fn range_scan_empty_range_returns_empty() {
    let mut runtime = open_runtime();
    commit_put(&mut runtime, b"a", b"a", 10);

    let scan = runtime
        .scan_range(&ScanReadRequest::new(
            branch(),
            engine_space(),
            ScanRange::new(Some(api_key(b"x")), Some(api_key(b"z"))).expect("valid range"),
            ReadBound::Latest,
            None,
        ))
        .expect("range scan");
    assert!(scan.rows().is_empty());
}

#[test]
fn range_scan_tombstone_visibility_matches_point_read() {
    let mut runtime = open_runtime();
    commit_put(&mut runtime, b"b", b"value", 10);
    commit_delete(&mut runtime, b"b", 20);

    let point = runtime
        .read_point(&point_request(b"b", ReadBound::Latest))
        .expect("point read");
    let scan = runtime
        .scan_range(&ScanReadRequest::new(
            branch(),
            engine_space(),
            ScanRange::new(Some(api_key(b"a")), Some(api_key(b"c"))).expect("valid range"),
            ReadBound::Latest,
            None,
        ))
        .expect("range scan");
    assert!(point.row().expect("point tombstone").is_tombstone());
    assert!(scan.rows()[0].is_tombstone());
}

#[test]
fn scan_inherited_rows_match_point_reads() {
    let mut runtime = open_runtime();
    commit_put(&mut runtime, b"item-a", b"a", 10);
    commit_put(&mut runtime, b"item-b", b"b", 20);
    runtime
        .flush_default_branch_for_test()
        .expect("flush parent branch");
    let child = other_branch();
    runtime
        .fork_default_branch_for_test(child)
        .expect("fork branch");

    let point = runtime
        .read_point(&point_request_for(child, b"item-a", ReadBound::Latest))
        .expect("point read");
    let scan = runtime
        .scan_prefix(&PrefixScanReadRequest::new(
            child,
            engine_space(),
            api_key(b"item-"),
            ReadBound::Latest,
            None,
        ))
        .expect("prefix scan");
    assert_eq!(read_value(point.row().expect("inherited point")), b"a");
    assert_eq!(scan.rows().len(), 2);
    assert_eq!(read_value(&scan.rows()[0]), b"a");
}

#[test]
fn timestamp_lookup_returns_newest_commit_at_or_before_timestamp() {
    let mut runtime = open_runtime();
    let first = commit_put(&mut runtime, b"a", b"a", 10);
    commit_put(&mut runtime, b"b", b"b", 30);

    let lookup = runtime
        .lookup_version_at_or_before_timestamp(TimestampLookupRequest::new(
            branch(),
            Timestamp::from_micros(20),
        ))
        .expect("timeline lookup");
    assert_eq!(lookup.matched_version(), first.commit_version());
    assert_eq!(lookup.matched_timestamp(), first.commit_timestamp());
}

#[test]
fn timestamp_lookup_equal_timestamps_uses_greatest_version() {
    let mut runtime = open_runtime();
    commit_put(&mut runtime, b"a", b"a", 10);
    let second = commit_put(&mut runtime, b"b", b"b", 10);

    let lookup = runtime
        .lookup_version_at_or_before_timestamp(TimestampLookupRequest::new(
            branch(),
            Timestamp::from_micros(10),
        ))
        .expect("timeline lookup");
    assert_eq!(lookup.matched_version(), second.commit_version());
}

#[test]
fn timestamp_lookup_before_retained_range_rejects() {
    let mut runtime = open_runtime();
    commit_put(&mut runtime, b"a", b"a", 50);

    let error = runtime
        .lookup_version_at_or_before_timestamp(TimestampLookupRequest::new(
            branch(),
            Timestamp::from_micros(10),
        ))
        .expect_err("timestamp before retained timeline");
    assert_eq!(error.class(), StorageApiErrorClass::HistoryUnavailable);
}

#[test]
fn timestamp_lookup_after_latest_returns_matched_with_miss_flag() {
    let mut runtime = open_runtime();
    let latest = commit_put(&mut runtime, b"a", b"a", 50);

    let lookup = runtime
        .lookup_version_at_or_before_timestamp(TimestampLookupRequest::new(
            branch(),
            Timestamp::from_micros(60),
        ))
        .expect("after-latest timeline lookup");
    assert_eq!(lookup.matched_version(), latest.commit_version());
    assert_eq!(lookup.matched_timestamp(), latest.commit_timestamp());
    assert_eq!(
        lookup.miss(),
        Some(TimestampLookupMiss::AfterLatestRetained)
    );
}

#[test]
fn timeline_lookups_track_commits_without_timeline_rows() {
    let mut runtime = open_runtime();
    let commit = commit_put(&mut runtime, b"atomic-timeline", b"value", 70);

    let point = runtime
        .read_point(&point_request(b"atomic-timeline", ReadBound::Latest))
        .expect("point read after commit");
    let lookup = runtime
        .lookup_version_at_or_before_timestamp(TimestampLookupRequest::new(
            branch(),
            commit.commit_timestamp(),
        ))
        .expect("timeline lookup after commit");
    let reverse = runtime
        .lookup_timestamp_for_version(VersionLookupRequest::new(branch(), commit.commit_version()))
        .expect("version lookup after commit");

    assert_eq!(commit.put_count(), 1);
    assert_eq!(commit.delete_count(), 0);
    assert_eq!(commit.timeline_row_count(), 0);
    assert_eq!(
        point.row().expect("user row").commit_version(),
        commit.commit_version()
    );
    assert_eq!(lookup.matched_version(), commit.commit_version());
    assert_eq!(lookup.matched_timestamp(), commit.commit_timestamp());
    assert_eq!(reverse.timestamp(), commit.commit_timestamp());
}

#[test]
fn version_lookup_returns_commit_timestamp() {
    let mut runtime = open_runtime();
    let commit = commit_put(&mut runtime, b"a", b"a", 50);

    let lookup = runtime
        .lookup_timestamp_for_version(VersionLookupRequest::new(branch(), commit.commit_version()))
        .expect("version lookup");
    assert_eq!(lookup.timestamp(), commit.commit_timestamp());
}

#[test]
fn version_lookup_unretained_version_rejects() {
    let mut runtime = open_runtime();
    commit_put(&mut runtime, b"a", b"a", 50);

    let error = runtime
        .lookup_timestamp_for_version(VersionLookupRequest::new(branch(), CommitVersion::ZERO))
        .expect_err("version outside retained timeline");
    assert_eq!(error.class(), StorageApiErrorClass::HistoryUnavailable);
}

#[test]
fn timeline_bounds_report_retained_range() {
    let mut runtime = open_runtime();
    let first = commit_put(&mut runtime, b"a", b"a", 10);
    let second = commit_put(&mut runtime, b"b", b"b", 30);

    let bounds = runtime
        .timeline_bounds(TimelineBoundsRequest::new(branch()))
        .expect("timeline bounds");
    assert_eq!(bounds.min_timestamp(), Some(first.commit_timestamp()));
    assert_eq!(bounds.max_timestamp(), Some(second.commit_timestamp()));
    assert_eq!(bounds.min_version(), Some(first.commit_version()));
    assert_eq!(bounds.max_version(), Some(second.commit_version()));
}

#[test]
fn timeline_lookup_survives_flush_and_compaction() {
    let mut runtime = open_runtime();
    let first = commit_put(&mut runtime, b"compact-a", b"a", 10);
    let second = commit_put(&mut runtime, b"compact-b", b"b", 30);
    let before_lookup = runtime
        .lookup_version_at_or_before_timestamp(TimestampLookupRequest::new(
            branch(),
            Timestamp::from_micros(20),
        ))
        .expect("timeline lookup before compaction");
    runtime
        .maintenance(&MaintenanceRequest::new(
            MaintenanceTask::Flush,
            MaintenanceScope::Branch(branch()),
        ))
        .expect("flush timeline rows");
    let flushed = runtime
        .branch_source_layout_for_test(branch())
        .expect("flushed source layout");
    let compacted_outcome = runtime
        .maintenance(&MaintenanceRequest::new(
            MaintenanceTask::Compact,
            MaintenanceScope::Branch(branch()),
        ))
        .expect("compact timeline rows");
    let compacted = runtime
        .branch_source_layout_for_test(branch())
        .expect("compacted source layout");

    let after_lookup = runtime
        .lookup_version_at_or_before_timestamp(TimestampLookupRequest::new(
            branch(),
            Timestamp::from_micros(20),
        ))
        .expect("timeline lookup after compaction");
    let after_reverse = runtime
        .lookup_timestamp_for_version(VersionLookupRequest::new(branch(), second.commit_version()))
        .expect("version lookup after compaction");

    assert_eq!(before_lookup.matched_version(), first.commit_version());
    assert_eq!(flushed.owned_l0_tables(), 1);
    assert_eq!(
        compacted_outcome.status(),
        MaintenanceSummaryStatus::Completed
    );
    assert_eq!(compacted.owned_l0_tables(), 0);
    assert_eq!(compacted.owned_total_tables(), 1);
    assert_eq!(after_lookup, before_lookup);
    assert_eq!(after_reverse.timestamp(), second.commit_timestamp());
}

/// Commits `key`/`value` at logical timestamp `ts` with an explicit wall-clock
/// instant attached (#3112 S2b).
fn commit_put_with_committed_at(
    runtime: &mut StorageRuntime<'static>,
    key: &[u8],
    value: &[u8],
    ts: u64,
    committed_at: Timestamp,
) -> CommitSummary {
    let batch = CommitBatch::new(
        branch(),
        vec![CommitMutation::Put {
            storage_space: engine_space(),
            key: api_key(key),
            value: StorageValue::new(value.to_vec()),
            ttl: None,
        }],
        CommitOptions::default()
            .require_conflict_check(false)
            .with_committed_at(committed_at),
    )
    .expect("valid put batch");
    runtime
        .commit_for_test(&batch, Timestamp::from_micros(ts))
        .expect("commit put with committed_at")
}

/// #3112 S2b: the wall-clock instant reaches the retained-timeline index on a
/// LIVE commit. The apply funnel is row-driven and rows never carry the
/// instant, so this proves the commit runtime's post-apply upgrade actually
/// runs — without it the entry would exist with the instant silently unknown.
#[test]
fn committed_at_reaches_the_timeline_index_on_a_live_commit() {
    let mut runtime = open_runtime();
    let committed_at = Timestamp::from_micros(1_788_000_000_000_000);
    let summary = commit_put_with_committed_at(&mut runtime, b"k", b"v", 10, committed_at);

    assert_eq!(summary.committed_at(), Some(committed_at));
    assert_eq!(
        runtime
            .retained_committed_at_for_test(branch(), summary.commit_version())
            .expect("inspect index"),
        Some(committed_at),
    );

    // Direction control: a commit that supplies no instant leaves the entry
    // unknown rather than inventing one.
    let bare = commit_put(&mut runtime, b"k2", b"v2", 20);
    assert_eq!(
        runtime
            .retained_committed_at_for_test(branch(), bare.commit_version())
            .expect("inspect index"),
        None,
    );
}

/// #3112 S2b: the instant survives a durable reopen that replays the WAL tail.
/// S2a made it durable in the record; replay must restore it onto the rebuilt
/// timeline entry instead of silently downgrading every recovered commit to
/// unknown. No checkpoint here on purpose — this is the replay path.
#[cfg(feature = "localfs")]
#[test]
fn committed_at_survives_a_durable_reopen_through_wal_replay() {
    let root = temp_dir_for_api_test("read-committed-at-wal-replay");
    let committed_at = Timestamp::from_micros(1_788_000_000_000_000);
    let version = {
        let mut runtime = open_durable_runtime(root.clone());
        let summary = commit_put_with_committed_at(&mut runtime, b"k", b"v", 10, committed_at);
        assert_eq!(summary.committed_at(), Some(committed_at));
        runtime.close().expect("close durable runtime");
        summary.commit_version()
    };

    let runtime = open_durable_runtime(root);
    assert_eq!(
        runtime
            .retained_committed_at_for_test(branch(), version)
            .expect("inspect post-reopen"),
        Some(committed_at),
        "WAL replay must restore the recorded wall-clock instant"
    );
}

/// #3112 S2c: the instant survives a reopen whose index is restored from the
/// CHECKPOINT section. S2b covered the WAL-replay path; this pins the other
/// restore path — the checkpoint's timeline section now persists `committed_at`
/// (section kind 3), so a commit already folded into a checkpoint keeps its
/// real date instead of coming back undated.
#[cfg(feature = "localfs")]
#[test]
fn committed_at_survives_a_durable_reopen_through_the_checkpoint_section() {
    let root = temp_dir_for_api_test("read-committed-at-checkpoint");
    let committed_at = Timestamp::from_micros(1_788_000_000_000_000);
    let version = {
        let mut runtime = open_durable_runtime(root.clone());
        let summary = commit_put_with_committed_at(&mut runtime, b"k", b"v", 10, committed_at);
        // Seed by scan first: a fresh durable branch starts unproven, and only a
        // COMPLETE index is persisted into the checkpoint section.
        runtime
            .read_point(&point_request(
                b"k",
                ReadBound::AtTimestamp(Timestamp::from_micros(10)),
            ))
            .expect("pre-close timestamp read");
        assert!(runtime
            .retained_timeline_complete_for_test(branch())
            .expect("complete pre-close"));
        let checkpoint = MaintenanceRequest::new(
            MaintenanceTask::Checkpoint,
            MaintenanceScope::Branch(branch()),
        );
        runtime.maintenance(&checkpoint).expect("checkpoint");
        runtime.close().expect("close durable runtime");
        summary.commit_version()
    };

    let runtime = open_durable_runtime(root);
    // Complete before any read means the index came from the checkpoint
    // section, not from a seeding scan.
    assert!(runtime
        .retained_timeline_complete_for_test(branch())
        .expect("complete post-reopen"));
    assert_eq!(
        runtime
            .retained_committed_at_for_test(branch(), version)
            .expect("inspect post-reopen"),
        Some(committed_at),
        "the checkpoint section must restore the wall-clock instant"
    );
}

/// #3112 S4: the batch instant lookup through the real API.
///
/// History joins these onto rows by commit version, so order fidelity is the
/// contract — and an unknown instant must come back as `None` rather than as
/// an error. That is the deliberate contrast with a wall-clock `as_of`, which
/// refuses when it cannot vouch for dates: "when did this happen" answering
/// "unknown" is useful, but "what did it look like then" answering with the
/// wrong commit is not.
#[cfg(feature = "localfs")]
#[test]
fn commit_instants_answers_in_order_and_reports_unknown_without_failing() {
    let root = temp_dir_for_api_test("read-commit-instants");
    let mut runtime = open_durable_runtime(root);
    let first_instant = Timestamp::from_micros(1_788_000_000_000_000);
    let second_instant = Timestamp::from_micros(1_788_000_000_000_500);

    // An undated commit first (the pre-epic shape), then two dated ones.
    let undated = commit_put(&mut runtime, b"k", b"v0", 10);
    let first = commit_put_with_committed_at(&mut runtime, b"k", b"v1", 20, first_instant);
    let second = commit_put_with_committed_at(&mut runtime, b"k", b"v2", 30, second_instant);

    // Prove coverage, so `None` below means "undated", not "unproven".
    runtime
        .read_point(&point_request(
            b"k",
            ReadBound::AtTimestamp(Timestamp::from_micros(20)),
        ))
        .expect("seeding read");
    assert!(runtime
        .retained_timeline_complete_for_test(branch())
        .expect("index is complete"));

    // Newest-first, the order a history view asks in.
    let asked = vec![
        second.commit_version(),
        first.commit_version(),
        undated.commit_version(),
    ];
    assert_eq!(
        runtime
            .commit_instants(&CommitInstantsRequest::new(branch(), asked))
            .expect("instants resolve"),
        vec![Some(second_instant), Some(first_instant), None],
        "answers follow the order asked, and the undated commit reports unknown"
    );

    // An empty request is an empty answer, not a round trip.
    assert!(runtime
        .commit_instants(&CommitInstantsRequest::new(branch(), Vec::new()))
        .expect("empty request succeeds")
        .is_empty());

    // An unproven index yields all-unknown rather than an error: history is
    // still exact, only its dates are unavailable.
    runtime
        .mark_retained_timeline_incomplete_for_test(branch())
        .expect("mark unproven");
    assert_eq!(
        runtime
            .commit_instants(&CommitInstantsRequest::new(
                branch(),
                vec![first.commit_version(), second.commit_version()]
            ))
            .expect("an unproven index still answers"),
        vec![None, None],
        "unknown dates are a usable answer; a refusal here would be wrong"
    );
}

/// #3112 S3a: the shape every database created before this epic has — an
/// undated prefix followed by dated commits. Resolution must work over the
/// dated part while refusing, DISTINGUISHABLY, for a target that falls before
/// it. Reporting "before retained history" there would be a lie: that history
/// is retained and readable, only its wall-clock position is unknown.
#[cfg(feature = "localfs")]
#[test]
fn wall_clock_resolution_rides_over_an_undated_prefix() {
    let root = temp_dir_for_api_test("read-wall-clock-undated-prefix");
    let mut runtime = open_durable_runtime(root);
    let dated_instant = Timestamp::from_micros(1_788_000_000_000_000);

    // Two commits with no instant (the pre-epic shape), then a dated one.
    commit_put(&mut runtime, b"k", b"v1", 10);
    commit_put(&mut runtime, b"k", b"v2", 20);
    let dated = commit_put_with_committed_at(&mut runtime, b"k", b"v3", 30, dated_instant);

    // Prove the index is complete, so a refusal below is about DATING rather
    // than about unproven coverage.
    runtime
        .read_point(&point_request(
            b"k",
            ReadBound::AtTimestamp(Timestamp::from_micros(10)),
        ))
        .expect("seeding read");
    assert!(runtime
        .retained_timeline_complete_for_test(branch())
        .expect("index is complete"));

    let resolved = runtime
        .resolve_wall_clock(WallClockLookupRequest::new(branch(), dated_instant))
        .expect("the dated commit resolves");
    assert_eq!(resolved.version(), dated.commit_version());
    assert_eq!(
        resolved.timestamp(),
        Timestamp::from_micros(30),
        "resolution yields the LOGICAL timestamp the read then runs at"
    );
    assert_eq!(resolved.committed_at(), dated_instant);

    let error = runtime
        .resolve_wall_clock(WallClockLookupRequest::new(
            branch(),
            Timestamp::from_micros(dated_instant.as_micros() - 1),
        ))
        .expect_err("a target before the dated range must refuse");
    let StorageApiError::TimestampHistoryUnavailable { reason, .. } = error else {
        panic!("unexpected error: {error:?}");
    };
    assert!(
        reason.contains("undated"),
        "the refusal must name the undated prefix, not claim history is missing: {reason}"
    );

    // The undated commits are still perfectly readable by logical as_of — the
    // point of the distinct reason is that only their DATE is unknown.
    assert!(runtime
        .read_point(&point_request(
            b"k",
            ReadBound::AtTimestamp(Timestamp::from_micros(10)),
        ))
        .expect("undated history is still readable")
        .row()
        .is_some());
}

/// #3112 S3a / F1: wall-clock history has NO scan fallback, because
/// `committed_at` is commit-scoped and never written to timeline rows
/// (storage-format spec §10 req 13). A scan cannot supply it even where the
/// scan itself succeeds — on legacy pre-elision rows or a testkit view — so an
/// index that cannot prove coverage makes the question unanswerable rather than
/// merely slow.
///
/// The failure mode pinned here is the dangerous one: silently answering with
/// logical semantics, or with a boundary the index cannot actually vouch for.
#[cfg(feature = "localfs")]
#[test]
fn wall_clock_resolution_refuses_while_the_index_is_unproven() {
    let root = temp_dir_for_api_test("read-wall-clock-unproven");
    let mut runtime = open_durable_runtime(root);
    let instant = Timestamp::from_micros(1_788_000_000_000_000);
    let committed = commit_put_with_committed_at(&mut runtime, b"k", b"v", 10, instant);
    let logical = committed.commit_timestamp();

    // Drop to unproven coverage: the state a fork restored from the catalog
    // manifest and a corruption-guard poison both leave behind.
    runtime
        .mark_retained_timeline_incomplete_for_test(branch())
        .expect("mark unproven");
    assert!(!runtime
        .retained_timeline_complete_for_test(branch())
        .expect("index is unproven"));

    let error = runtime
        .resolve_wall_clock(WallClockLookupRequest::new(branch(), instant))
        .expect_err("an unproven index cannot answer a wall-clock question");
    let StorageApiError::TimestampHistoryUnavailable { reason, .. } = error else {
        panic!("unexpected error: {error:?}");
    };
    assert!(
        reason.contains("unavailable"),
        "unproven coverage must report unavailability: {reason}"
    );

    // A timeline lookup re-seeds the index from a scan on its way through, so
    // coverage becomes provable again and the same question now answers. The
    // refusal above is a statement about coverage, not a permanent verdict.
    //
    // The read's own outcome is deliberately ignored: what it returns depends
    // on whether this database still has timeline rows to scan (W3.1c retired
    // them), while the re-seed it performs on the way happens either way — and
    // the re-seed is the whole point here.
    let _ = runtime.read_point(&point_request(b"k", ReadBound::AtTimestamp(logical)));
    assert!(runtime
        .retained_timeline_complete_for_test(branch())
        .expect("the lookup re-seeded coverage"));
    assert_eq!(
        runtime
            .resolve_wall_clock(WallClockLookupRequest::new(branch(), instant))
            .expect("resolves once coverage is proven")
            .committed_at(),
        instant,
        "and the instant survived the re-seed rather than degrading to unknown"
    );
}

/// W3.1b oracle: a durable reopen restores the retained-timeline index
/// COMPLETE from the checkpoint section, before any read runs a seeding
/// scan — and the restored index answers exactly. The pre-close `as_of`
/// seeds the fresh database's index (the durable-open initial branch is
/// deliberately not complete-from-birth), so the close-time checkpoint
/// persists it.
#[cfg(feature = "localfs")]
#[test]
fn retained_timeline_restores_complete_across_durable_reopen() {
    let root = temp_dir_for_api_test("read-timeline-restore-complete");
    {
        let mut runtime = open_durable_runtime(root.clone());
        commit_put(&mut runtime, b"warm-a", b"old", 10);
        commit_put(&mut runtime, b"warm-a", b"new", 30);
        // Seed by scan (fresh durable DBs start unproven), so the close-time
        // checkpoint persists the index.
        let outcome = runtime
            .read_point(&point_request(
                b"warm-a",
                ReadBound::AtTimestamp(Timestamp::from_micros(20)),
            ))
            .expect("pre-close timestamp read");
        assert_eq!(read_value(outcome.row().expect("row")), b"old");
        assert!(runtime
            .retained_timeline_complete_for_test(branch())
            .expect("inspect pre-close"));
        // Explicit checkpoint: the close ladder's own checkpoint is
        // policy-gated for tiny WAL tails, and the persistence path under
        // test is checkpoint-time section writing.
        let checkpoint = MaintenanceRequest::new(
            MaintenanceTask::Checkpoint,
            MaintenanceScope::Branch(branch()),
        );
        runtime.maintenance(&checkpoint).expect("checkpoint");
        runtime.close().expect("close durable runtime");
    }

    let runtime = open_durable_runtime(root);
    // Complete BEFORE any read: restored from the checkpoint section, not
    // seeded by a scan.
    assert!(runtime
        .retained_timeline_complete_for_test(branch())
        .expect("inspect post-reopen"));
    let outcome = runtime
        .read_point(&point_request(
            b"warm-a",
            ReadBound::AtTimestamp(Timestamp::from_micros(20)),
        ))
        .expect("post-reopen timestamp read");
    assert_eq!(read_value(outcome.row().expect("row")), b"old");
    let outcome = runtime
        .read_point(&point_request(
            b"warm-a",
            ReadBound::AtTimestamp(Timestamp::from_micros(30)),
        ))
        .expect("post-reopen latest read");
    assert_eq!(read_value(outcome.row().expect("row")), b"new");
}

#[cfg(feature = "localfs")]
#[test]
fn timeline_lookup_survives_durable_recovery() {
    let root = temp_dir_for_api_test("read-timeline-durable-recovery");
    let first;
    let second;
    {
        let mut runtime = open_durable_runtime(root.clone());
        first = commit_put(&mut runtime, b"recover-a", b"a", 10);
        second = commit_put(&mut runtime, b"recover-b", b"b", 30);
        runtime.close().expect("close durable runtime");
    }

    let runtime = open_durable_runtime(root);
    let timestamp_lookup = runtime
        .lookup_version_at_or_before_timestamp(TimestampLookupRequest::new(
            branch(),
            Timestamp::from_micros(20),
        ))
        .expect("timeline lookup after durable recovery");
    let version_lookup = runtime
        .lookup_timestamp_for_version(VersionLookupRequest::new(branch(), second.commit_version()))
        .expect("version lookup after durable recovery");

    assert_eq!(timestamp_lookup.matched_version(), first.commit_version());
    assert_eq!(
        timestamp_lookup.matched_timestamp(),
        first.commit_timestamp()
    );
    assert_eq!(version_lookup.timestamp(), second.commit_timestamp());
}

#[cfg(feature = "perf-trace")]
#[test]
fn timeline_lookup_over_many_user_rows_scans_no_user_rows() {
    let _capture = perf_trace::begin_test_capture();
    let mut runtime = open_manual_runtime();
    let retained_commits = 32usize;
    for index in 0..retained_commits {
        let key = format!("user-row-{index:03}");
        commit_put(
            &mut runtime,
            key.as_bytes(),
            b"value",
            u64::try_from((index + 1) * 10).expect("timestamp fits u64"),
        );
    }

    let lookup = runtime
        .lookup_version_at_or_before_timestamp(TimestampLookupRequest::new(
            branch(),
            Timestamp::from_micros(175),
        ))
        .expect("timeline lookup");

    let perf = perf_trace::snapshot();
    assert_eq!(lookup.matched_version(), CommitVersion::new(17));
    // W3.1a-c: the lookup is served entirely by the retained in-memory index
    // — no stored rows are scanned, no view is materialized, no reconcile
    // runs. These counters pinned the pre-index scan-and-reconcile path; the
    // test's thesis ("scans no user rows") is now strictly stronger: it
    // scans no stored rows at all.
    assert_eq!(perf.commit_timeline_view_rows_scanned(), 0);
    assert_eq!(perf.commit_timeline_timestamp_facts(), 0);
    assert_eq!(perf.commit_timeline_version_facts(), 0);
    assert_eq!(perf.commit_timeline_reconcile_entry_checks(), 0);
    // The index's own bisect still probes entries: ~log2(32)+1.
    assert_eq!(perf.commit_timeline_lookup_entries_scanned(), 6);
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn generated_read_contract_matches_model_for_mutations_and_reads() {
    use std::collections::BTreeMap;

    use proptest::collection::vec;
    use proptest::prelude::any;
    use proptest::test_runner::{Config, TestCaseError, TestRunner};

    let mut runner = TestRunner::new(Config {
        cases: 48,
        ..Config::default()
    });
    runner
        .run(&vec(any::<u8>(), 1..=96), |script| {
            let runtime = open_runtime();
            let mut model = BTreeMap::<Vec<u8>, Vec<ModelRow>>::new();

            for (index, chunk) in script.chunks(4).take(24).enumerate() {
                let key = vec![b'k', b'0' + chunk.get(1).copied().unwrap_or(0) % 4];
                let timestamp = Timestamp::from_micros(10 + u64::try_from(index).unwrap() * 10);
                let value =
                    (chunk[0] % 4 != 0).then(|| vec![b'v', chunk.get(2).copied().unwrap_or(0)]);
                let summary = if let Some(value) = &value {
                    runtime
                        .commit_for_test(&put_batch(&key, value), timestamp)
                        .map_err(|error| TestCaseError::fail(error.to_string()))?
                } else {
                    runtime
                        .commit_for_test(&delete_batch(&key), timestamp)
                        .map_err(|error| TestCaseError::fail(error.to_string()))?
                };
                model.entry(key.clone()).or_default().push(ModelRow {
                    key: key.clone(),
                    value,
                    commit_version: summary.commit_version(),
                    commit_timestamp: summary.commit_timestamp(),
                });

                assert_point_matches_model(&runtime, &model, &key, ReadBound::Latest)?;
                assert_point_matches_model(
                    &runtime,
                    &model,
                    &key,
                    ReadBound::AtVersion(summary.commit_version()),
                )?;
                assert_point_matches_model(
                    &runtime,
                    &model,
                    &key,
                    ReadBound::AtTimestamp(summary.commit_timestamp()),
                )?;
                assert_history_matches_model(&runtime, &model, &key)?;
                assert_prefix_scan_matches_model(&runtime, &model, ReadBound::Latest)?;
                assert_prefix_scan_matches_model(
                    &runtime,
                    &model,
                    ReadBound::AtVersion(summary.commit_version()),
                )?;
            }
            Ok(())
        })
        .expect("generated API read model");
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone, Debug, Eq, PartialEq)]
struct ModelRow {
    key: Vec<u8>,
    value: Option<Vec<u8>>,
    commit_version: CommitVersion,
    commit_timestamp: Timestamp,
}

#[cfg(not(target_arch = "wasm32"))]
fn assert_point_matches_model(
    runtime: &StorageRuntime<'static>,
    model: &std::collections::BTreeMap<Vec<u8>, Vec<ModelRow>>,
    key: &[u8],
    bound: ReadBound,
) -> Result<(), proptest::test_runner::TestCaseError> {
    let outcome = runtime
        .read_point(&point_request(key, bound))
        .map_err(|error| proptest::test_runner::TestCaseError::fail(error.to_string()))?;
    let expected = model_visible_row(model.get(key), bound);
    assert_api_row_matches_model(outcome.row(), expected)
}

#[cfg(not(target_arch = "wasm32"))]
fn assert_history_matches_model(
    runtime: &StorageRuntime<'static>,
    model: &std::collections::BTreeMap<Vec<u8>, Vec<ModelRow>>,
    key: &[u8],
) -> Result<(), proptest::test_runner::TestCaseError> {
    let limit = ReadLimit::new(3)
        .map_err(|error| proptest::test_runner::TestCaseError::fail(error.to_string()))?;
    let outcome = runtime
        .read_history(&HistoryReadRequest::new(branch(), engine_space(), api_key(key)).limit(limit))
        .map_err(|error| proptest::test_runner::TestCaseError::fail(error.to_string()))?;
    let expected = model
        .get(key)
        .into_iter()
        .flat_map(|rows| rows.iter().rev().take(limit.get()));
    for (actual, expected) in outcome.rows().iter().zip(expected.clone()) {
        assert_storage_row_matches_model(actual, expected)?;
    }
    if outcome.rows().len() != expected.count() {
        return Err(proptest::test_runner::TestCaseError::fail(
            "history row count disagrees with model",
        ));
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
fn assert_prefix_scan_matches_model(
    runtime: &StorageRuntime<'static>,
    model: &std::collections::BTreeMap<Vec<u8>, Vec<ModelRow>>,
    bound: ReadBound,
) -> Result<(), proptest::test_runner::TestCaseError> {
    let outcome = runtime
        .scan_prefix(&PrefixScanReadRequest::new(
            branch(),
            engine_space(),
            api_key(b"k"),
            bound,
            None,
        ))
        .map_err(|error| proptest::test_runner::TestCaseError::fail(error.to_string()))?;
    let expected = model
        .values()
        .filter_map(|rows| model_visible_row(Some(rows), bound))
        .collect::<Vec<_>>();
    for (actual, expected) in outcome.rows().iter().zip(&expected) {
        assert_storage_row_matches_model(actual, expected)?;
    }
    if outcome.rows().len() != expected.len() {
        return Err(proptest::test_runner::TestCaseError::fail(
            "scan row count disagrees with model",
        ));
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
fn model_visible_row(rows: Option<&Vec<ModelRow>>, bound: ReadBound) -> Option<&ModelRow> {
    rows?.iter().rev().find(|row| match bound {
        ReadBound::Latest => true,
        ReadBound::AtVersion(version) => row.commit_version <= version,
        ReadBound::AtTimestamp(timestamp) => row.commit_timestamp <= timestamp,
    })
}

#[cfg(not(target_arch = "wasm32"))]
fn assert_api_row_matches_model(
    actual: Option<&StorageReadRow>,
    expected: Option<&ModelRow>,
) -> Result<(), proptest::test_runner::TestCaseError> {
    match (actual, expected) {
        (None, None) => Ok(()),
        (Some(actual), Some(expected)) => assert_storage_row_matches_model(actual, expected),
        _ => Err(proptest::test_runner::TestCaseError::fail(
            "point row presence disagrees with model",
        )),
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn assert_storage_row_matches_model(
    actual: &StorageReadRow,
    expected: &ModelRow,
) -> Result<(), proptest::test_runner::TestCaseError> {
    if actual.key().as_bytes() != expected.key
        || actual.commit_version() != expected.commit_version
        || actual.commit_timestamp() != expected.commit_timestamp
        || actual.value().map(StorageValue::as_bytes) != expected.value.as_deref()
        || actual.is_tombstone() != expected.value.is_none()
    {
        return Err(proptest::test_runner::TestCaseError::fail(
            "row facts disagree with model",
        ));
    }
    Ok(())
}

// --- TCP3.3c: L9 negative paths for timeline / immutable-source / maintenance
// --- methods the deep-dive found lacked negative coverage. Each asserts the
// --- stable code (not just class) so the boundary contract is pinned.

#[test]
fn scan_immutable_sources_on_a_missing_branch_rejects() {
    let mut runtime = open_runtime();
    commit_put(&mut runtime, b"a", b"a", 10);

    let error = runtime
        .scan_immutable_sources(&ImmutableSourceScanReadRequest::new(
            other_branch(),
            engine_space(),
            ScanRange::new(Some(api_key(b"a")), Some(api_key(b"z"))).expect("valid range"),
            ReadBound::Latest,
        ))
        .expect_err("scan on a branch that does not exist must reject");
    assert_eq!(error.code(), "not_found.storage_api.branch");
}

#[test]
fn scan_immutable_sources_on_a_closed_runtime_rejects() {
    let mut runtime = open_runtime();
    commit_put(&mut runtime, b"a", b"a", 10);
    runtime.close().expect("close");

    let error = runtime
        .scan_immutable_sources(&ImmutableSourceScanReadRequest::new(
            branch(),
            engine_space(),
            ScanRange::new(Some(api_key(b"a")), Some(api_key(b"z"))).expect("valid range"),
            ReadBound::Latest,
        ))
        .expect_err("closed runtime rejects immutable-source scan");
    assert_eq!(error.code(), "failed_precondition.storage_api.state");
}

#[test]
fn timeline_lookups_on_a_missing_branch_reject() {
    let mut runtime = open_runtime();
    commit_put(&mut runtime, b"a", b"a", 10);

    let bounds = runtime
        .timeline_bounds(TimelineBoundsRequest::new(other_branch()))
        .expect_err("timeline bounds on a missing branch must reject");
    assert_eq!(bounds.code(), "not_found.storage_api.branch");

    let at = runtime
        .lookup_version_at_or_before_timestamp(TimestampLookupRequest::new(
            other_branch(),
            Timestamp::from_micros(5),
        ))
        .expect_err("version-at-timestamp on a missing branch must reject");
    assert_eq!(at.code(), "not_found.storage_api.branch");

    let for_version = runtime
        .lookup_timestamp_for_version(VersionLookupRequest::new(
            other_branch(),
            CommitVersion::new(1),
        ))
        .expect_err("timestamp-for-version on a missing branch must reject");
    assert_eq!(for_version.code(), "not_found.storage_api.branch");
}

#[test]
fn timestamp_lookup_before_retained_history_reports_history_unavailable() {
    let mut runtime = open_runtime();
    commit_put(&mut runtime, b"a", b"a", 100);

    // A timestamp strictly before the earliest retained commit.
    let error = runtime
        .lookup_version_at_or_before_timestamp(TimestampLookupRequest::new(
            branch(),
            Timestamp::from_micros(1),
        ))
        .expect_err("timestamp before retained history must reject");
    assert_eq!(error.code(), "history_unavailable.storage_api.timestamp");
}

#[test]
fn maintenance_drain_on_a_closed_runtime_rejects() {
    let mut runtime = open_runtime();
    commit_put(&mut runtime, b"a", b"a", 10);
    runtime.close().expect("close");

    let drain = runtime
        .drain_maintenance()
        .expect_err("closed runtime rejects maintenance drain");
    assert_eq!(drain.code(), "failed_precondition.storage_api.state");

    let run = runtime
        .run_next_maintenance()
        .expect_err("closed runtime rejects run-next maintenance");
    assert_eq!(run.code(), "failed_precondition.storage_api.state");
}
