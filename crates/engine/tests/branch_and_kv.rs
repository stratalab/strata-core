//! Branch and KV conformance tests.

mod common;

use strata_core::Timestamp;
use strata_engine::api::CommitOutcome;
use strata_engine::{
    BranchName, Database, EngineErrorClass, KvKey, KvScanRow, KvService, ProductSpace,
};

#[cfg(feature = "localfs")]
use common::open_durable_database;
use common::{
    assert_branch_value, assert_default_branch_exists, assert_no_storage_type_in_engine_error,
    branch, key, open_cache_database, space, value,
};

/// #3112 S4: history rows carry their own commit's wall-clock instant.
///
/// Lives in the ENGINE crate on purpose. The join that attaches instants to
/// rows is engine code, and the mutation gate runs each mutated crate's own
/// tests — an end-to-end test over in the executor crate proves the behavior
/// but kills none of these mutants.
#[test]
fn kv_history_rows_carry_their_commits_wall_clock_instant() {
    const YEAR_2020_MICROS: u64 = 1_577_836_800_000_000;
    let database = open_cache_database().expect("cache open succeeds");
    let mut kv = database
        .kv(branch("default"), space("default"))
        .expect("KV service opens");

    // Two commits, separated in wall-clock time so each row's instant is
    // individually identifiable — a join that shifted rows would be visible.
    let first = kv
        .put(key(b"k"), value(b"one"))
        .expect("first put succeeds")
        .commit()
        .committed_at()
        .expect("write ack carries an instant");
    std::thread::sleep(std::time::Duration::from_millis(2));
    let second = kv
        .put(key(b"k"), value(b"two"))
        .expect("second put succeeds")
        .commit()
        .committed_at()
        .expect("write ack carries an instant");
    assert!(second > first, "commits must land at distinct instants");

    let history = kv
        .get_versions(&key(b"k"))
        .expect("history read succeeds")
        .expect("the key has history");
    let rows = history.rows();
    assert_eq!(rows.len(), 2);

    // Newest-first, each row carrying the instant its own write ack reported.
    assert_eq!(
        rows[0].committed_at(),
        Some(second),
        "the newest row must carry the second commit's instant"
    );
    assert_eq!(
        rows[1].committed_at(),
        Some(first),
        "the oldest row must carry the first commit's instant"
    );

    // And these are real dates, not the logical counter — the distinction the
    // whole epic exists to make.
    assert!(
        rows[0]
            .committed_at()
            .expect("newest row is dated")
            .as_micros()
            > YEAR_2020_MICROS
    );
    assert!(rows[0].timestamp().as_micros() < YEAR_2020_MICROS);
}

#[test]
fn several_capability_services_can_be_held_at_once() {
    let database = open_cache_database().expect("cache open succeeds");
    // Establish the branch/space through an exclusive handle first.
    database
        .kv(branch("default"), space("default"))
        .expect("kv opens")
        .put(key(b"seed"), value(b"v"))
        .expect("seed write");

    // The point of the change: three services, alive simultaneously, from a
    // SHARED borrow. Before this, the second call did not compile — `kv`
    // already borrowed the database mutably.
    let db = &database;
    let mut kv = db
        .kv(branch("default"), space("default"))
        .expect("kv opens");
    let mut json = db
        .json(branch("default"), space("default"))
        .expect("json opens");
    let mut events = db
        .event(branch("default"), space("default"))
        .expect("event opens");

    // All three are usable while the others are held.
    assert!(kv.get(&key(b"seed")).expect("kv read").is_some());
    assert_eq!(json.count(None).expect("json count"), 0);
    assert_eq!(events.len().expect("event len").count(), 0);
}

#[test]
fn cache_write_ack_carries_a_wall_clock_committed_at() {
    // #3112: the engine stamps a real wall-clock `committed_at` (UTC epoch
    // micros) onto every generated commit, distinct from the logical commit
    // clock. 2020-01-01 in epoch micros: any real clock is far above it, and the
    // logical per-commit counter (seeded near 1) far below.
    const YEAR_2020_MICROS: u64 = 1_577_836_800_000_000;
    let database = open_cache_database().expect("cache open succeeds");
    let mut kv = database
        .kv(branch("default"), space("default"))
        .expect("KV service opens");
    let outcome = kv.put(key(b"k"), value(b"v")).expect("put succeeds");
    let commit = outcome.commit();
    let committed_at = commit
        .committed_at()
        .expect("write ack carries a committed_at")
        .as_micros();
    assert!(
        committed_at > YEAR_2020_MICROS,
        "committed_at {committed_at} is not a wall-clock instant"
    );
    assert!(
        commit.timestamp().as_micros() < YEAR_2020_MICROS,
        "logical timestamp {} should be a small counter, not wall-clock",
        commit.timestamp().as_micros()
    );
}

#[test]
fn cache_database_supports_branch_and_kv_workflow() {
    let mut database = open_cache_database().expect("cache open succeeds");

    let branches = database
        .branches()
        .expect("branch service opens")
        .list()
        .expect("branch list succeeds");
    assert!(branches
        .iter()
        .any(|summary| summary.name().as_str() == "default"));

    {
        let mut kv = database
            .kv(branch("default"), space("default"))
            .expect("KV service opens");
        kv.put(key(b"key-a"), value(b"value-a"))
            .expect("put succeeds");
        let found = kv
            .get(&key(b"key-a"))
            .expect("get succeeds")
            .expect("value");
        assert_eq!(found.as_bytes(), b"value-a");
        assert!(kv.delete(key(b"key-a")).expect("delete succeeds").deleted());
        assert!(kv.get(&key(b"key-a")).expect("get succeeds").is_none());
        kv.put(key(b"shared"), value(b"base"))
            .expect("base put succeeds");
    }

    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("branch create succeeds");

    assert_branch_value(&mut database, "feature", "default", b"shared", b"base");

    {
        let mut feature = database
            .kv(branch("feature"), space("default"))
            .expect("feature KV opens");
        feature
            .put(key(b"shared"), value(b"feature"))
            .expect("feature put succeeds");
    }

    assert_branch_value(&mut database, "default", "default", b"shared", b"base");
    assert_branch_value(&mut database, "feature", "default", b"shared", b"feature");

    let mut default_kv = database
        .kv(branch("default"), space("default"))
        .expect("default KV opens");
    let default_history = default_kv
        .get_versions(&key(b"shared"))
        .expect("default history succeeds")
        .expect("default history exists");
    assert_eq!(
        default_history.rows()[0]
            .value()
            .expect("default history value")
            .as_bytes(),
        b"base"
    );
    drop(default_kv);

    let mut feature_kv = database
        .kv(branch("feature"), space("default"))
        .expect("feature KV opens");
    let feature_history = feature_kv
        .get_versions(&key(b"shared"))
        .expect("feature history succeeds")
        .expect("feature history exists");
    assert_eq!(
        feature_history.rows()[0]
            .value()
            .expect("feature history value")
            .as_bytes(),
        b"feature"
    );
}

#[test]
fn kv_contract_runs_in_cache_and_durable_modes() {
    run_database_modes(exercise_kv_contract);
}

#[test]
fn invalid_inputs_return_engine_errors() {
    let error = BranchName::new("_system_").expect_err("reserved branch rejected");
    assert_eq!(error.class(), EngineErrorClass::InvalidInput);
    assert_eq!(error.code(), "invalid_argument.engine.branch_name_reserved");
    assert!(!error.retryable());
    assert_no_storage_type_in_engine_error(&error);

    let error = BranchName::new("").expect_err("empty branch rejected");
    assert_eq!(error.class(), EngineErrorClass::InvalidInput);
    assert_eq!(error.code(), "invalid_argument.engine.branch_name");

    let error = BranchName::new("_internal").expect_err("internal branch rejected");
    assert_eq!(error.class(), EngineErrorClass::InvalidInput);
    assert_eq!(error.code(), "invalid_argument.engine.branch_name_reserved");

    let error = ProductSpace::new("_system_").expect_err("reserved space rejected");
    assert_eq!(error.class(), EngineErrorClass::InvalidInput);
    assert_eq!(
        error.code(),
        "invalid_argument.engine.product_space_reserved"
    );
    assert_no_storage_type_in_engine_error(&error);

    let error = ProductSpace::new("").expect_err("empty space rejected");
    assert_eq!(error.class(), EngineErrorClass::InvalidInput);
    assert_eq!(error.code(), "invalid_argument.engine.product_space");

    let error = KvKey::new(Vec::new()).expect_err("empty key rejected");
    assert_eq!(error.class(), EngineErrorClass::InvalidInput);
    assert_eq!(error.code(), "invalid_argument.engine.kv_key");

    let empty = strata_engine::KvValue::new(Vec::new());
    assert!(empty.as_bytes().is_empty());
}

#[test]
fn duplicate_and_missing_branches_are_stable_errors() {
    let mut database = open_cache_database().expect("cache open succeeds");
    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("first create succeeds");

    let error = database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect_err("duplicate branch rejected");
    assert_eq!(error.class(), EngineErrorClass::Conflict);
    assert_eq!(error.code(), "already_exists.engine.branch");
    assert!(!error.retryable());
    assert_no_storage_type_in_engine_error(&error);

    let error = database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("missing"), branch("from-missing"))
        .expect_err("missing source rejected");
    assert_eq!(error.class(), EngineErrorClass::NotFound);
    assert_eq!(error.code(), "not_found.engine.branch");

    let error = database
        .kv(branch("missing"), space("default"))
        .map(|_| ())
        .expect_err("missing branch rejected at service construction");
    assert_eq!(error.class(), EngineErrorClass::NotFound);
    assert_eq!(error.code(), "not_found.engine.branch");
    assert!(!error.retryable());
    assert_no_storage_type_in_engine_error(&error);
}

#[test]
fn branch_lookup_returns_stable_summary() {
    let mut database = open_cache_database().expect("cache open succeeds");
    assert_default_branch_exists(&mut database);

    let created = database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("branch create succeeds");
    assert_eq!(created.branch().name().as_str(), "feature");
    assert_eq!(created.branch().generation(), 1);

    let summary = database
        .branches()
        .expect("branch service opens")
        .get(&branch("feature"))
        .expect("feature branch exists");
    assert_eq!(summary.name().as_str(), "feature");
    assert_eq!(summary.generation(), created.branch().generation());
}

#[test]
fn kv_edge_cases_are_stable() {
    let database = open_cache_database().expect("cache open succeeds");

    {
        let mut kv = database
            .kv(branch("default"), space("default"))
            .expect("KV service opens");
        assert!(kv
            .get(&key(b"missing"))
            .expect("missing key read succeeds")
            .is_none());
        assert!(!kv
            .delete(key(b"missing"))
            .expect("delete missing key succeeds")
            .deleted());
        assert!(kv
            .delete(key(b"still-missing"))
            .expect("delete missing key succeeds")
            .commit()
            .is_none());

        kv.put(key(b"rewrite"), value(b"first"))
            .expect("first put succeeds");
        kv.put(key(b"rewrite"), value(b"second"))
            .expect("overwrite succeeds");
        let found = kv
            .get(&key(b"rewrite"))
            .expect("read succeeds")
            .expect("value exists");
        assert_eq!(found.as_bytes(), b"second");

        kv.put(key(&[0, 1, 255]), value(&[255, 0, 1]))
            .expect("binary put succeeds");
        let found = kv
            .get(&key(&[0, 1, 255]))
            .expect("binary read succeeds")
            .expect("binary value exists");
        assert_eq!(found.as_bytes(), &[255, 0, 1]);

        let large = vec![7; 64 * 1024];
        kv.put(key(b"large"), value(&large))
            .expect("large put succeeds");
        let found = kv
            .get(&key(b"large"))
            .expect("large read succeeds")
            .expect("large value exists");
        assert_eq!(found.as_bytes(), large.as_slice());

        kv.put(key(b"empty"), value(b""))
            .expect("empty value put succeeds");
        let found = kv
            .get(&key(b"empty"))
            .expect("empty value read succeeds")
            .expect("empty value exists");
        assert!(found.as_bytes().is_empty());
    }

    let error = database
        .kv(branch("missing"), space("default"))
        .map(|_| ())
        .expect_err("missing branch rejected at service construction");
    assert_eq!(error.class(), EngineErrorClass::NotFound);
    assert_eq!(error.code(), "not_found.engine.branch");

    let error = database
        .kv(branch("missing"), space("default"))
        .map(|_| ())
        .expect_err("missing branch delete rejected at service construction");
    assert_eq!(error.class(), EngineErrorClass::NotFound);
    assert_eq!(error.code(), "not_found.engine.branch");
}

#[test]
fn kv_batch_put_and_delete_are_single_commit_operations() {
    let database = open_cache_database().expect("cache open succeeds");
    let mut kv = database
        .kv(branch("default"), space("default"))
        .expect("KV service opens");

    let outcome = kv
        .put_batch([
            (key(b"batch-a"), value(b"value-a")),
            (key(b"batch-b"), value(b"value-b")),
            (key(b"batch-c"), value(b"value-c")),
        ])
        .expect("batch put succeeds")
        .commit();
    assert_eq!(outcome.put_count(), 3);
    assert_eq!(outcome.delete_count(), 0);

    for (key_bytes, expected) in [
        (b"batch-a".as_slice(), b"value-a".as_slice()),
        (b"batch-b".as_slice(), b"value-b".as_slice()),
        (b"batch-c".as_slice(), b"value-c".as_slice()),
    ] {
        let found = kv
            .get(&key(key_bytes))
            .expect("batch value read succeeds")
            .expect("batch value exists");
        assert_eq!(found.as_bytes(), expected);
    }

    let outcome = kv
        .delete_batch([key(b"batch-a"), key(b"batch-c")])
        .expect("batch delete succeeds");
    assert_eq!(outcome.deleted(), &[true, true]);
    let commit = outcome.commit().expect("delete batch committed");
    assert_eq!(commit.put_count(), 0);
    assert_eq!(commit.delete_count(), 2);
    let missing_batch = kv
        .delete_batch([key(b"batch-missing")])
        .expect("missing batch delete succeeds");
    assert_eq!(missing_batch.deleted(), &[false]);
    assert!(missing_batch.commit().is_none());
    assert!(kv
        .get(&key(b"batch-a"))
        .expect("deleted value read succeeds")
        .is_none());
    assert!(kv
        .get(&key(b"batch-c"))
        .expect("deleted value read succeeds")
        .is_none());
    let found = kv
        .get(&key(b"batch-b"))
        .expect("remaining value read succeeds")
        .expect("remaining value exists");
    assert_eq!(found.as_bytes(), b"value-b");
}

#[test]
fn kv_batch_validation_is_stable() {
    let database = open_cache_database().expect("cache open succeeds");
    let mut kv = database
        .kv(branch("default"), space("default"))
        .expect("KV service opens");

    let error = kv
        .put_batch(std::iter::empty::<(
            strata_engine::KvKey,
            strata_engine::KvValue,
        )>())
        .expect_err("empty put batch rejected");
    assert_eq!(error.class(), EngineErrorClass::InvalidInput);
    assert_eq!(error.code(), "invalid_argument.engine.kv_batch");
    assert_no_storage_type_in_engine_error(&error);

    let error = kv
        .put_batch([
            (key(b"duplicate"), value(b"first")),
            (key(b"duplicate"), value(b"second")),
        ])
        .expect_err("duplicate put batch rejected");
    assert_eq!(error.class(), EngineErrorClass::InvalidInput);
    assert_eq!(
        error.code(),
        "invalid_argument.engine.kv_batch_duplicate_key"
    );
    assert_no_storage_type_in_engine_error(&error);

    let error = kv
        .delete_batch(std::iter::empty::<strata_engine::KvKey>())
        .expect_err("empty delete batch rejected");
    assert_eq!(error.class(), EngineErrorClass::InvalidInput);
    assert_eq!(error.code(), "invalid_argument.engine.kv_batch");

    let error = kv
        .delete_batch([key(b"duplicate"), key(b"duplicate")])
        .expect_err("duplicate delete batch rejected");
    assert_eq!(error.class(), EngineErrorClass::InvalidInput);
    assert_eq!(
        error.code(),
        "invalid_argument.engine.kv_batch_duplicate_key"
    );
}

#[test]
fn kv_versioned_and_point_in_time_reads_preserve_commit_metadata() {
    let database = open_cache_database().expect("cache open succeeds");
    let mut kv = database
        .kv(branch("default"), space("default"))
        .expect("KV service opens");
    let sequence = write_alpha_history(&mut kv);

    let latest = kv
        .get_versioned(&sequence.alpha)
        .expect("versioned read succeeds")
        .expect("latest value exists");
    assert_eq!(latest.value().as_bytes(), b"third");
    assert_eq!(latest.version(), sequence.third.version());
    assert_eq!(latest.timestamp(), sequence.third.timestamp());

    assert_eq!(
        kv.get_at_version(&sequence.alpha, sequence.first.version())
            .expect("version read succeeds")
            .expect("first value exists")
            .as_bytes(),
        b"first"
    );
    assert_eq!(
        kv.get_at_version(&sequence.alpha, sequence.second.version())
            .expect("version read succeeds")
            .expect("second value exists")
            .as_bytes(),
        b"second"
    );
    assert!(kv
        .get_at_version(&sequence.alpha, sequence.removed.version())
        .expect("delete version read succeeds")
        .is_none());
    // A timestamp before the retained history is an out-of-range diagnostic,
    // not ordinary absence (F8): the caller can distinguish trimmed/before-history
    // from "the key never existed at that time".
    assert_eq!(
        kv.get_at(&sequence.alpha, Timestamp::EPOCH)
            .expect_err("before-history read is a diagnostic")
            .code(),
        "history_unavailable.engine.persistence_history"
    );
    assert_eq!(
        kv.get_at(&sequence.alpha, sequence.third.timestamp())
            .expect("timestamp read succeeds")
            .expect("timestamp value exists")
            .as_bytes(),
        b"third"
    );
    let historical = kv
        .get_versioned_at(&sequence.alpha, sequence.third.timestamp())
        .expect("versioned timestamp read succeeds")
        .expect("timestamp value exists");
    assert_eq!(historical.value().as_bytes(), b"third");
    assert_eq!(historical.version(), sequence.third.version());
    assert_eq!(historical.timestamp(), sequence.third.timestamp());
    // A timestamp after the latest retained commit does not silently clamp to
    // current state (F7); it surfaces an after-latest diagnostic so the caller
    // chooses current intentionally rather than reading it through a future
    // timestamp.
    assert_eq!(
        kv.get_at(&sequence.alpha, Timestamp::MAX)
            .expect_err("after-latest read is a diagnostic")
            .code(),
        "history_unavailable.engine.persistence_history"
    );
}

#[test]
fn kv_history_returns_newest_first_rows_with_tombstones() {
    let database = open_cache_database().expect("cache open succeeds");
    let mut kv = database
        .kv(branch("default"), space("default"))
        .expect("KV service opens");
    let sequence = write_alpha_history(&mut kv);

    let history = kv
        .get_versions(&sequence.alpha)
        .expect("history read succeeds")
        .expect("history exists");
    let rows = history.rows();
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0].version(), sequence.third.version());
    assert_eq!(
        rows[0].value().expect("third history value").as_bytes(),
        b"third"
    );
    assert_eq!(rows[1].version(), sequence.removed.version());
    assert!(rows[1].is_tombstone());
    assert!(rows[1].value().is_none());
    assert_eq!(rows[2].version(), sequence.second.version());
    assert_eq!(
        rows[2].value().expect("second history value").as_bytes(),
        b"second"
    );
    assert_eq!(rows[3].version(), sequence.first.version());
    assert_eq!(
        rows[3].value().expect("first history value").as_bytes(),
        b"first"
    );
    assert!(kv
        .get_versions(&sequence.missing)
        .expect("missing history read succeeds")
        .is_none());
}

#[test]
fn kv_first_write_in_new_space_hides_control_rows_from_commit_counts() {
    let database = open_cache_database().expect("cache open succeeds");
    let mut kv = database
        .kv(branch("default"), space("tenant"))
        .expect("tenant KV service opens");

    let outcome = kv
        .put(key(b"first"), value(b"value"))
        .expect("first tenant put succeeds")
        .commit();
    assert_eq!(outcome.put_count(), 1);
    assert_eq!(outcome.delete_count(), 0);

    let batch = kv
        .put_batch([
            (key(b"second"), value(b"value-2")),
            (key(b"third"), value(b"value-3")),
        ])
        .expect("tenant batch put succeeds")
        .commit();
    assert_eq!(batch.put_count(), 2);
    assert_eq!(batch.delete_count(), 0);
}

#[test]
fn kv_batch_reads_and_exists_are_positional() {
    let database = open_cache_database().expect("cache open succeeds");
    let mut kv = database
        .kv(branch("default"), space("default"))
        .expect("KV service opens");
    let sequence = write_alpha_history(&mut kv);

    let positional = kv
        .batch_get(&[
            sequence.alpha.clone(),
            sequence.missing.clone(),
            sequence.alpha.clone(),
        ])
        .expect("batch read succeeds");
    assert_eq!(positional.len(), 3);
    assert_eq!(
        positional[0]
            .as_ref()
            .expect("first positional value")
            .value()
            .as_bytes(),
        b"third"
    );
    assert!(positional[1].is_none());
    assert_eq!(
        positional[2]
            .as_ref()
            .expect("duplicate positional value")
            .version(),
        sequence.third.version()
    );
    assert!(kv.exists(&sequence.alpha).expect("exists succeeds"));
    assert!(!kv
        .exists(&sequence.missing)
        .expect("missing exists succeeds"));
    assert_eq!(
        kv.batch_exists(&[sequence.alpha, sequence.missing.clone(), sequence.missing,])
            .expect("batch exists succeeds"),
        vec![true, false, false]
    );
}

#[test]
fn kv_list_page_and_timestamp_list_suppress_tombstones() {
    let (database, visible_timestamp) = open_database_with_kv_shape();

    let mut kv = database
        .kv(branch("default"), space("default"))
        .expect("KV service opens");
    let app_prefix = key(b"app:");
    let listed = kv.list(Some(&app_prefix)).expect("prefix list succeeds");
    assert_key_bytes(&listed, &[b"app:001", b"app:003", b"app:004", b"app:005"]);
    assert_eq!(
        kv.count(Some(&app_prefix)).expect("prefix count succeeds"),
        4
    );
    // A before-history timestamp scan is an out-of-range diagnostic, not an
    // empty result (F8).
    assert_eq!(
        kv.list_at(Some(&app_prefix), Timestamp::EPOCH)
            .expect_err("before-history list is a diagnostic")
            .code(),
        "history_unavailable.engine.persistence_history"
    );
    assert_key_bytes(
        &kv.list_at(Some(&app_prefix), visible_timestamp)
            .expect("timestamp list succeeds"),
        &[b"app:001", b"app:003", b"app:004", b"app:005"],
    );
    // An after-latest timestamp scan surfaces the diagnostic rather than
    // clamping to the latest listing (F7).
    assert_eq!(
        kv.list_at(Some(&app_prefix), Timestamp::MAX)
            .expect_err("after-latest list is a diagnostic")
            .code(),
        "history_unavailable.engine.persistence_history"
    );

    let first_page = kv
        .list_page(Some(&app_prefix), None, 2)
        .expect("first page succeeds");
    assert_key_bytes(first_page.keys(), &[b"app:001", b"app:003"]);
    assert!(first_page.has_more());
    assert_eq!(
        first_page.cursor().expect("first page cursor").as_bytes(),
        b"app:003"
    );
    let second_page = kv
        .list_page(Some(&app_prefix), first_page.cursor(), 2)
        .expect("second page succeeds");
    assert_key_bytes(second_page.keys(), &[b"app:004", b"app:005"]);
    assert!(!second_page.has_more());
    assert!(second_page.cursor().is_none());
}

#[test]
fn kv_scan_and_sample_suppress_tombstones() {
    let (database, _) = open_database_with_kv_shape();

    let mut kv = database
        .kv(branch("default"), space("default"))
        .expect("KV service opens");
    let app_prefix = key(b"app:");

    let range = kv
        .scan_range(Some(&key(b"app:001")), Some(&key(b"app:005")), None)
        .expect("range scan succeeds");
    assert_row_key_bytes(&range, &[b"app:001", b"app:003", b"app:004"]);
    assert!(kv
        .scan_range(Some(&key(b"app:003")), Some(&key(b"app:003")), None)
        .expect("equal range succeeds")
        .is_empty());
    assert!(kv
        .scan_range(Some(&key(b"app:005")), Some(&key(b"app:001")), None)
        .expect("inverted range succeeds")
        .is_empty());
    let limited = kv
        .scan(Some(&key(b"app:001")), Some(2))
        .expect("limited scan succeeds");
    assert_row_key_bytes(&limited, &[b"app:001", b"app:003"]);
    let limited_after_tombstones = kv
        .scan(Some(&key(b"app:000")), Some(3))
        .expect("limited tombstone scan succeeds");
    assert_row_key_bytes(
        &limited_after_tombstones,
        &[b"app:001", b"app:003", b"app:004"],
    );
    assert!(kv
        .scan(None, Some(0))
        .expect("zero scan succeeds")
        .is_empty());

    let zero_sample = kv
        .sample(Some(&app_prefix), 0)
        .expect("zero sample succeeds");
    assert_eq!(zero_sample.total_count(), 4);
    assert!(zero_sample.rows().is_empty());
    let full_sample = kv
        .sample(Some(&app_prefix), 10)
        .expect("full sample succeeds");
    assert_eq!(full_sample.total_count(), 4);
    assert_row_key_bytes(
        full_sample.rows(),
        &[b"app:001", b"app:003", b"app:004", b"app:005"],
    );
    let partial_sample = kv
        .sample(Some(&app_prefix), 2)
        .expect("partial sample succeeds");
    assert_eq!(partial_sample.total_count(), 4);
    assert_eq!(partial_sample.rows().len(), 2);
    assert!(partial_sample
        .rows()
        .iter()
        .all(|row| row.key().as_bytes().starts_with(app_prefix.as_bytes())));
}

#[test]
fn kv_branch_and_space_scans_stay_isolated() {
    let (database, _) = open_database_with_kv_shape();
    let app_prefix = key(b"app:");

    let mut kv = database
        .kv(branch("default"), space("default"))
        .expect("KV service opens");
    let all_default_keys = kv.list(None).expect("full list succeeds");
    assert!(!all_default_keys
        .iter()
        .any(|listed| listed.as_bytes() == b"feature-only"));
    drop(kv);

    let mut other = database
        .kv(branch("default"), space("other"))
        .expect("other space opens");
    assert_eq!(
        other
            .get(&key(b"app:001"))
            .expect("other space read succeeds")
            .expect("other space value exists")
            .as_bytes(),
        b"other-space"
    );
    assert_eq!(
        other
            .count(Some(&app_prefix))
            .expect("other space count succeeds"),
        1
    );
    drop(other);

    let mut feature = database
        .kv(branch("feature"), space("default"))
        .expect("feature KV opens");
    assert!(feature
        .list(None)
        .expect("feature list succeeds")
        .iter()
        .any(|listed| listed.as_bytes() == b"feature-only"));
}

#[cfg(feature = "localfs")]
#[test]
fn durable_reopen_preserves_kv_read_surface() {
    let temp = tempfile::tempdir().expect("temp dir");
    let path = temp.path().join("db");
    let first;
    let second;

    {
        let mut database = open_durable_database(&path).expect("durable open succeeds");
        let mut kv = database
            .kv(branch("default"), space("default"))
            .expect("KV service opens");
        first = kv
            .put(key(b"alpha"), value(b"first"))
            .expect("first put succeeds")
            .commit();
        second = kv
            .put(key(b"alpha"), value(b"second"))
            .expect("second put succeeds")
            .commit();
        kv.put_batch([
            (key(b"prefix-a"), value(b"value-a")),
            (key(b"prefix-b"), value(b"value-b")),
        ])
        .expect("batch put succeeds");
        kv.delete(key(b"prefix-a")).expect("prefix delete succeeds");
        drop(kv);
        let close = database.close().expect("durable close succeeds");
        assert!(close.durable());
    }

    {
        let database = open_durable_database(&path).expect("durable reopen succeeds");
        let mut kv = database
            .kv(branch("default"), space("default"))
            .expect("KV service opens");
        assert_eq!(
            kv.get(&key(b"alpha"))
                .expect("latest read succeeds")
                .expect("latest value exists")
                .as_bytes(),
            b"second"
        );
        assert_eq!(
            kv.get_at_version(&key(b"alpha"), first.version())
                .expect("version read succeeds")
                .expect("first value exists")
                .as_bytes(),
            b"first"
        );
        let history = kv
            .get_versions(&key(b"alpha"))
            .expect("history read succeeds")
            .expect("history exists");
        assert_eq!(history.rows()[0].version(), second.version());
        assert_eq!(
            history.rows()[0]
                .value()
                .expect("latest history value")
                .as_bytes(),
            b"second"
        );
        assert_eq!(history.rows()[1].version(), first.version());
        assert_key_bytes(
            &kv.list(Some(&key(b"prefix")))
                .expect("prefix list succeeds"),
            &[b"prefix-b"],
        );
        assert_eq!(
            kv.count(Some(&key(b"prefix")))
                .expect("prefix count succeeds"),
            1
        );
        assert_row_key_bytes(
            &kv.scan_range(Some(&key(b"prefix-a")), Some(&key(b"prefix-c")), None)
                .expect("range scan succeeds"),
            &[b"prefix-b"],
        );
        let sample = kv
            .sample(Some(&key(b"prefix")), 3)
            .expect("sample succeeds");
        assert_eq!(sample.total_count(), 1);
        assert_row_key_bytes(sample.rows(), &[b"prefix-b"]);
    }
}

#[test]
fn branch_create_from_empty_source_stays_isolated() {
    let mut database = open_cache_database().expect("cache open succeeds");
    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("branch create succeeds");

    database
        .kv(branch("default"), space("default"))
        .expect("default KV opens")
        .put(key(b"late"), value(b"default"))
        .expect("default put succeeds");

    let mut feature = database
        .kv(branch("feature"), space("default"))
        .expect("feature KV opens");
    assert!(feature
        .get(&key(b"late"))
        .expect("feature read succeeds")
        .is_none());
}

struct AlphaHistory {
    alpha: KvKey,
    missing: KvKey,
    first: CommitOutcome,
    second: CommitOutcome,
    removed: CommitOutcome,
    third: CommitOutcome,
}

fn write_alpha_history(kv: &mut KvService<'_>) -> AlphaHistory {
    let alpha = key(b"alpha");
    let missing = key(b"missing");
    let first = kv
        .put(alpha.clone(), value(b"first"))
        .expect("first put succeeds")
        .commit();
    let second = kv
        .put(alpha.clone(), value(b"second"))
        .expect("second put succeeds")
        .commit();
    let removed = kv
        .delete(alpha.clone())
        .expect("delete commit succeeds")
        .commit()
        .expect("delete committed");
    let third = kv
        .put(alpha.clone(), value(b"third"))
        .expect("third put succeeds")
        .commit();
    AlphaHistory {
        alpha,
        missing,
        first,
        second,
        removed,
        third,
    }
}

fn open_database_with_kv_shape() -> (Database, Timestamp) {
    let mut database = open_cache_database().expect("cache open succeeds");
    let visible_timestamp = {
        let mut kv = database
            .kv(branch("default"), space("default"))
            .expect("KV service opens");
        kv.put_batch([
            (key(b"app:000"), value(b"deleted-0")),
            (key(b"app:000b"), value(b"deleted-0b")),
            (key(b"app:001"), value(b"value-1")),
            (key(b"app:002"), value(b"value-2")),
            (key(b"app:003"), value(b"value-3")),
            (key(b"app:004"), value(b"value-4")),
            (key(b"bee:001"), value(b"value-b")),
        ])
        .expect("batch put succeeds");
        assert_eq!(
            kv.delete_batch([key(b"app:000"), key(b"app:000b"), key(b"app:002")])
                .expect("prefix tombstones succeed")
                .deleted(),
            &[true, true, true]
        );
        kv.put(key(b"app:005"), value(b"value-5"))
            .expect("tail put succeeds")
            .commit()
            .timestamp()
    };
    database
        .kv(branch("default"), space("other"))
        .expect("other space opens")
        .put(key(b"app:001"), value(b"other-space"))
        .expect("other space put succeeds");
    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("branch create succeeds");
    database
        .kv(branch("feature"), space("default"))
        .expect("feature KV opens")
        .put(key(b"feature-only"), value(b"feature"))
        .expect("feature put succeeds");
    (database, visible_timestamp)
}

fn assert_key_bytes(keys: &[KvKey], expected: &[&[u8]]) {
    let actual: Vec<_> = keys.iter().map(KvKey::as_bytes).collect();
    assert_eq!(actual, expected);
}

fn assert_row_key_bytes(rows: &[KvScanRow], expected: &[&[u8]]) {
    let actual: Vec<_> = rows.iter().map(|row| row.key().as_bytes()).collect();
    assert_eq!(actual, expected);
}

fn run_database_modes(mut exercise: impl FnMut(&mut Database)) {
    let mut cache = open_cache_database().expect("cache open succeeds");
    exercise(&mut cache);

    #[cfg(feature = "localfs")]
    {
        let temp = tempfile::tempdir().expect("temp dir");
        let path = temp.path().join("db");
        let mut durable = open_durable_database(&path).expect("durable open succeeds");
        exercise(&mut durable);
        let close = durable.close().expect("durable close succeeds");
        assert!(close.durable());
    }
}

fn exercise_kv_contract(database: &mut Database) {
    let keys = KvContractKeys::new();
    let facts = assert_default_kv_contract(database, &keys);
    assert_empty_space_contract(database);
    assert_space_isolation_contract(database, &keys);
    assert_branch_isolation_contract(database, &keys, &facts);
}

struct KvContractKeys {
    app_prefix: KvKey,
    alpha: KvKey,
    beta: KvKey,
    missing: KvKey,
}

impl KvContractKeys {
    fn new() -> Self {
        Self {
            app_prefix: key(b"app:"),
            alpha: key(b"alpha"),
            beta: key(b"beta"),
            missing: key(b"missing"),
        }
    }
}

struct KvContractFacts {
    alpha_second: CommitOutcome,
    beta_create: CommitOutcome,
    beta_delete: CommitOutcome,
    app_batch: CommitOutcome,
}

fn assert_default_kv_contract(database: &mut Database, keys: &KvContractKeys) -> KvContractFacts {
    let mut kv = database
        .kv(branch("default"), space("default"))
        .expect("KV service opens");
    let (alpha_second, beta_create, app_batch) = seed_default_kv(&mut kv, keys);
    assert_batch_read_contract(&mut kv, keys, &alpha_second);
    let beta_delete = assert_delete_history_and_temporal_reads(&mut kv, keys, &beta_create);
    assert_list_page_scan_count_and_sample(&mut kv, keys, &beta_create, &app_batch);
    KvContractFacts {
        alpha_second,
        beta_create,
        beta_delete,
        app_batch,
    }
}

fn seed_default_kv(
    kv: &mut KvService<'_>,
    keys: &KvContractKeys,
) -> (CommitOutcome, CommitOutcome, CommitOutcome) {
    let alpha_first = kv
        .put(keys.alpha.clone(), value(b"one"))
        .expect("first put succeeds")
        .commit();
    assert_eq!(alpha_first.put_count(), 1);
    assert_eq!(alpha_first.delete_count(), 0);
    assert_eq!(
        kv.get(&keys.alpha)
            .expect("latest read succeeds")
            .expect("alpha exists")
            .as_bytes(),
        b"one"
    );
    let alpha_versioned = kv
        .get_versioned(&keys.alpha)
        .expect("versioned read succeeds")
        .expect("alpha exists");
    assert_eq!(alpha_versioned.version(), alpha_first.version());
    assert_eq!(alpha_versioned.timestamp(), alpha_first.timestamp());

    let beta_create = kv
        .put(keys.beta.clone(), value(b"beta"))
        .expect("beta put succeeds")
        .commit();
    let alpha_second = kv
        .put(keys.alpha.clone(), value(b"two"))
        .expect("second put succeeds")
        .commit();
    let app_batch = kv
        .put_batch([
            (key(b"app:001"), value(b"value-1")),
            (key(b"app:003"), value(b"value-3")),
            (key(&[0, 1]), value(b"binary")),
        ])
        .expect("batch put succeeds")
        .commit();
    assert_eq!(app_batch.put_count(), 3);
    assert_eq!(app_batch.delete_count(), 0);
    (alpha_second, beta_create, app_batch)
}

fn assert_batch_read_contract(
    kv: &mut KvService<'_>,
    keys: &KvContractKeys,
    alpha_second: &CommitOutcome,
) {
    let positional = kv
        .batch_get(&[keys.alpha.clone(), keys.missing.clone(), keys.alpha.clone()])
        .expect("batch read succeeds");
    assert_eq!(positional.len(), 3);
    assert_eq!(
        positional[0]
            .as_ref()
            .expect("first alpha")
            .value()
            .as_bytes(),
        b"two"
    );
    assert!(positional[1].is_none());
    assert_eq!(
        positional[2].as_ref().expect("second alpha").version(),
        alpha_second.version()
    );
    assert_eq!(
        positional[2].as_ref().expect("second alpha").timestamp(),
        alpha_second.timestamp()
    );
    assert_eq!(
        kv.batch_exists(&[keys.alpha.clone(), keys.missing.clone(), keys.alpha.clone(),])
            .expect("batch exists succeeds"),
        vec![true, false, true]
    );
}

fn assert_delete_history_and_temporal_reads(
    kv: &mut KvService<'_>,
    keys: &KvContractKeys,
    beta_create: &CommitOutcome,
) -> CommitOutcome {
    assert!(kv.exists(&keys.beta).expect("beta exists succeeds"));
    let beta_delete = kv.delete(keys.beta.clone()).expect("beta delete succeeds");
    assert!(beta_delete.deleted());
    let beta_delete = beta_delete.commit().expect("beta delete committed");
    assert_eq!(beta_delete.delete_count(), 1);
    assert!(!kv.exists(&keys.beta).expect("beta missing succeeds"));
    assert!(kv.get(&keys.beta).expect("beta read succeeds").is_none());
    assert_eq!(
        kv.get_at_version(&keys.beta, beta_create.version())
            .expect("version read succeeds")
            .expect("beta existed")
            .as_bytes(),
        b"beta"
    );
    assert!(kv
        .get_at_version(&keys.beta, beta_delete.version())
        .expect("delete version read succeeds")
        .is_none());
    assert_eq!(
        kv.get_at(&keys.beta, beta_create.timestamp())
            .expect("timestamp read succeeds")
            .expect("beta existed")
            .as_bytes(),
        b"beta"
    );
    assert!(kv
        .get_at(&keys.beta, beta_delete.timestamp())
        .expect("delete timestamp read succeeds")
        .is_none());
    assert_beta_history(kv, keys, &beta_delete);
    beta_delete
}

fn assert_beta_history(kv: &mut KvService<'_>, keys: &KvContractKeys, beta_delete: &CommitOutcome) {
    let beta_history = kv
        .get_versions(&keys.beta)
        .expect("history read succeeds")
        .expect("beta history exists");
    assert_eq!(beta_history.rows().len(), 2);
    assert!(beta_history.rows()[0].is_tombstone());
    assert_eq!(beta_history.rows()[0].version(), beta_delete.version());
    assert_eq!(
        beta_history.rows()[1]
            .value()
            .expect("beta history value")
            .as_bytes(),
        b"beta"
    );
    assert!(kv
        .get_versions(&keys.missing)
        .expect("missing history read succeeds")
        .is_none());
}

fn assert_list_page_scan_count_and_sample(
    kv: &mut KvService<'_>,
    keys: &KvContractKeys,
    beta_create: &CommitOutcome,
    app_batch: &CommitOutcome,
) {
    assert_list_and_page_contract(kv, keys, beta_create, app_batch);
    assert_scan_contract(kv, app_batch);
    assert_count_and_sample_contract(kv, keys);
}

fn assert_list_and_page_contract(
    kv: &mut KvService<'_>,
    keys: &KvContractKeys,
    beta_create: &CommitOutcome,
    app_batch: &CommitOutcome,
) {
    assert_key_bytes(
        &kv.list(Some(&keys.app_prefix))
            .expect("prefix list succeeds"),
        &[b"app:001", b"app:003"],
    );
    assert!(kv
        .list(Some(&key(b"absent:")))
        .expect("missing prefix list succeeds")
        .is_empty());
    assert_key_bytes(
        &kv.list(Some(&key(&[0]))).expect("binary prefix succeeds"),
        &[&[0, 1]],
    );
    assert!(kv
        .list_at(Some(&keys.app_prefix), beta_create.timestamp())
        .expect("early timestamp list succeeds")
        .is_empty());
    assert_key_bytes(
        &kv.list_at(Some(&keys.app_prefix), app_batch.timestamp())
            .expect("timestamp list succeeds"),
        &[b"app:001", b"app:003"],
    );

    let empty_page = kv
        .list_page(Some(&keys.app_prefix), None, 0)
        .expect("zero page succeeds");
    assert!(empty_page.keys().is_empty());
    assert!(!empty_page.has_more());
    assert!(empty_page.cursor().is_none());

    let first_page = kv
        .list_page(Some(&keys.app_prefix), None, 1)
        .expect("first page succeeds");
    assert_key_bytes(first_page.keys(), &[b"app:001"]);
    assert!(first_page.has_more());
    let second_page = kv
        .list_page(Some(&keys.app_prefix), first_page.cursor(), 1)
        .expect("second page succeeds");
    assert_key_bytes(second_page.keys(), &[b"app:003"]);
    assert!(!second_page.has_more());
    assert!(second_page.cursor().is_none());
}

fn assert_scan_contract(kv: &mut KvService<'_>, app_batch: &CommitOutcome) {
    let first_row = kv.scan(None, Some(1)).expect("limited scan succeeds");
    assert_eq!(first_row.len(), 1);
    assert_eq!(first_row[0].version(), app_batch.version());
    assert_eq!(first_row[0].timestamp(), app_batch.timestamp());
    assert_row_key_bytes(
        &kv.scan_range(Some(&key(b"app:002")), Some(&key(b"app:004")), None)
            .expect("bounded scan succeeds"),
        &[b"app:003"],
    );
    assert_row_key_bytes(
        &kv.scan(Some(&key(b"app:002")), Some(1))
            .expect("between-key scan succeeds"),
        &[b"app:003"],
    );
}

fn assert_count_and_sample_contract(kv: &mut KvService<'_>, keys: &KvContractKeys) {
    assert_eq!(kv.count(Some(&keys.app_prefix)).expect("count succeeds"), 2);
    let zero_sample = kv
        .sample(Some(&keys.app_prefix), 0)
        .expect("zero sample succeeds");
    assert_eq!(zero_sample.total_count(), 2);
    assert!(zero_sample.rows().is_empty());
    let first_sample = kv
        .sample(Some(&keys.app_prefix), 1)
        .expect("sample succeeds");
    let second_sample = kv
        .sample(Some(&keys.app_prefix), 1)
        .expect("repeat sample succeeds");
    assert_eq!(first_sample, second_sample);
    assert_eq!(first_sample.rows().len(), 1);
    assert!(first_sample.rows()[0]
        .key()
        .as_bytes()
        .starts_with(keys.app_prefix.as_bytes()));
}

fn assert_empty_space_contract(database: &mut Database) {
    let mut empty = database
        .kv(branch("default"), space("empty"))
        .expect("empty space opens");
    assert_eq!(empty.count(None).expect("empty count succeeds"), 0);
    let sample = empty.sample(None, 2).expect("empty sample succeeds");
    assert_eq!(sample.total_count(), 0);
    assert!(sample.rows().is_empty());
}

fn assert_space_isolation_contract(database: &mut Database, keys: &KvContractKeys) {
    let mut other = database
        .kv(branch("default"), space("other"))
        .expect("other space opens");
    other
        .put(keys.alpha.clone(), value(b"other"))
        .expect("other space put succeeds");
    assert_eq!(
        other
            .get(&keys.alpha)
            .expect("other read succeeds")
            .expect("other value exists")
            .as_bytes(),
        b"other"
    );
    assert!(other
        .list(Some(&keys.app_prefix))
        .expect("other prefix list succeeds")
        .is_empty());
}

fn assert_branch_isolation_contract(
    database: &mut Database,
    keys: &KvContractKeys,
    facts: &KvContractFacts,
) {
    assert_branch_value(database, "default", "default", b"alpha", b"two");
    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("feature branch succeeds");

    let mut feature = database
        .kv(branch("feature"), space("default"))
        .expect("feature KV opens");
    feature
        .put(keys.alpha.clone(), value(b"feature"))
        .expect("feature put succeeds");
    assert_eq!(
        feature
            .get(&keys.alpha)
            .expect("feature read succeeds")
            .expect("feature value exists")
            .as_bytes(),
        b"feature"
    );
    assert_eq!(
        feature
            .count(Some(&keys.app_prefix))
            .expect("feature count succeeds"),
        2
    );
    assert_row_key_bytes(
        feature
            .sample(Some(&keys.app_prefix), 2)
            .expect("feature sample succeeds")
            .rows(),
        &[b"app:001", b"app:003"],
    );
    assert_eq!(
        feature
            .get_versions(&keys.alpha)
            .expect("feature history succeeds")
            .expect("feature history exists")
            .rows()[0]
            .value()
            .expect("feature history value")
            .as_bytes(),
        b"feature"
    );
    assert_eq!(facts.alpha_second.put_count(), 1);
    assert_eq!(facts.beta_create.put_count(), 1);
    assert_eq!(facts.beta_delete.delete_count(), 1);
    assert_eq!(facts.app_batch.put_count(), 3);
    drop(feature);

    assert_branch_value(database, "default", "default", b"alpha", b"two");
}
