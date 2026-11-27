//! Branch compare conformance tests (M12B1).

mod common;

use serde_json::json;
use strata_engine::{
    BranchComparison, BranchStateSelector, ComparedCapability, ComparedEntity, EngineErrorClass,
    JsonDocumentId, JsonPath, JsonValue, SpaceComparison,
};

use common::{
    branch, key, open_cache_database, open_durable_database, reopen_durable_database_after_drop,
    space, value,
};

fn doc_id(value: &str) -> JsonDocumentId {
    JsonDocumentId::new(value).expect("valid document id")
}

fn root() -> JsonPath {
    JsonPath::root()
}

fn json_value(value: serde_json::Value) -> JsonValue {
    JsonValue::new(value).expect("valid JSON value")
}

fn find<'a>(
    comparison: &'a BranchComparison,
    capability: ComparedCapability,
    space_name: &str,
) -> Option<&'a SpaceComparison> {
    comparison
        .comparisons()
        .iter()
        .find(|entry| entry.capability() == capability && entry.space().as_str() == space_name)
}

fn identities(entries: &[ComparedEntity]) -> Vec<Vec<u8>> {
    entries
        .iter()
        .map(|entry| entry.identity().to_vec())
        .collect()
}

#[test]
fn compare_reports_kv_and_json_added_removed_modified() {
    let mut database = open_cache_database().expect("cache open succeeds");

    {
        let mut kv = database
            .kv(branch("default"), space("default"))
            .expect("KV opens");
        kv.put(key(b"a"), value(b"1")).expect("put a");
        kv.put(key(b"b"), value(b"2")).expect("put b");
        kv.put(key(b"c"), value(b"3")).expect("put c");
    }
    {
        let mut json = database
            .json(branch("default"), space("default"))
            .expect("JSON opens");
        json.set_or_create(doc_id("profile"), &root(), json_value(json!({"v": 1})))
            .expect("set profile");
        json.set_or_create(doc_id("config"), &root(), json_value(json!({"k": true})))
            .expect("set config");
    }

    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("fork succeeds");

    let added_version = {
        let mut kv = database
            .kv(branch("feature"), space("default"))
            .expect("feature KV opens");
        kv.put(key(b"a"), value(b"1x")).expect("modify a");
        kv.delete(key(b"c")).expect("delete c");
        kv.put(key(b"d"), value(b"4"))
            .expect("add d")
            .commit()
            .version()
    };
    {
        let mut json = database
            .json(branch("feature"), space("default"))
            .expect("feature JSON opens");
        json.set_or_create(doc_id("profile"), &root(), json_value(json!({"v": 2})))
            .expect("modify profile");
    }

    let comparison = database
        .branches()
        .expect("branch service opens")
        .compare(
            &branch("default"),
            &branch("feature"),
            BranchStateSelector::Current,
        )
        .expect("compare succeeds");

    assert_eq!(comparison.branch_a().as_str(), "default");
    assert_eq!(comparison.branch_b().as_str(), "feature");
    assert!(!comparison.is_empty());

    let kv_diff = find(&comparison, ComparedCapability::Kv, "default").expect("KV diff is present");
    assert_eq!(identities(kv_diff.added()), vec![b"d".to_vec()]);
    assert_eq!(identities(kv_diff.removed()), vec![b"c".to_vec()]);
    assert_eq!(identities(kv_diff.modified()), vec![b"a".to_vec()]);
    assert_eq!(kv_diff.added()[0].version(), added_version);

    let json_diff =
        find(&comparison, ComparedCapability::Json, "default").expect("JSON diff is present");
    assert!(json_diff.added().is_empty());
    assert!(json_diff.removed().is_empty());
    assert_eq!(identities(json_diff.modified()), vec![b"profile".to_vec()]);
}

#[test]
fn compare_of_an_unchanged_fork_is_empty() {
    let mut database = open_cache_database().expect("cache open succeeds");
    {
        let mut kv = database
            .kv(branch("default"), space("default"))
            .expect("KV opens");
        kv.put(key(b"a"), value(b"1")).expect("put a");
    }
    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("fork succeeds");

    let comparison = database
        .branches()
        .expect("branch service opens")
        .compare(
            &branch("default"),
            &branch("feature"),
            BranchStateSelector::Current,
        )
        .expect("compare succeeds");
    assert!(comparison.is_empty());
    assert!(comparison.comparisons().is_empty());
}

#[test]
fn compare_reports_a_space_present_on_only_one_branch() {
    let mut database = open_cache_database().expect("cache open succeeds");
    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("fork succeeds");
    database
        .spaces(branch("feature"))
        .expect("space service opens")
        .create(space("extra"))
        .expect("space create succeeds");
    {
        let mut kv = database
            .kv(branch("feature"), space("extra"))
            .expect("feature KV opens");
        kv.put(key(b"only"), value(b"here")).expect("put only");
    }

    let comparison = database
        .branches()
        .expect("branch service opens")
        .compare(
            &branch("default"),
            &branch("feature"),
            BranchStateSelector::Current,
        )
        .expect("compare succeeds");

    let extra_diff =
        find(&comparison, ComparedCapability::Kv, "extra").expect("extra-space diff present");
    assert_eq!(identities(extra_diff.added()), vec![b"only".to_vec()]);
    assert!(extra_diff.removed().is_empty());
    assert!(extra_diff.modified().is_empty());
}

#[test]
fn compare_with_a_missing_branch_is_not_found() {
    let mut database = open_cache_database().expect("cache open succeeds");
    let error = database
        .branches()
        .expect("branch service opens")
        .compare(
            &branch("default"),
            &branch("ghost"),
            BranchStateSelector::Current,
        )
        .expect_err("a missing branch is rejected");
    assert_eq!(error.class(), EngineErrorClass::NotFound);
    assert_eq!(error.code(), "not_found.engine.branch");
}

#[test]
fn compare_at_a_timestamp_reads_each_branch_frontier() {
    let mut database = open_cache_database().expect("cache open succeeds");
    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("fork succeeds");

    // Feature writes first; default writes only afterward. So at the feature
    // write's timestamp, default has not written `k` yet.
    let feature_timestamp = {
        let mut kv = database
            .kv(branch("feature"), space("default"))
            .expect("feature KV opens");
        kv.put(key(b"k"), value(b"same"))
            .expect("feature put")
            .commit()
            .timestamp()
    };
    {
        let mut kv = database
            .kv(branch("default"), space("default"))
            .expect("default KV opens");
        kv.put(key(b"k"), value(b"same")).expect("default k first");
        kv.put(key(b"k"), value(b"changed"))
            .expect("default k changed");
    }

    // Current: default k=changed, feature k=same -> modified.
    let current = database
        .branches()
        .expect("branch service opens")
        .compare(
            &branch("default"),
            &branch("feature"),
            BranchStateSelector::Current,
        )
        .expect("current compare succeeds");
    let kv_now =
        find(&current, ComparedCapability::Kv, "default").expect("current KV diff present");
    assert_eq!(identities(kv_now.modified()), vec![b"k".to_vec()]);

    // As of the feature write's timestamp, default has no `k` yet, so `k` reads
    // as added on feature rather than modified.
    let past = database
        .branches()
        .expect("branch service opens")
        .compare(
            &branch("default"),
            &branch("feature"),
            BranchStateSelector::AtTimestamp(feature_timestamp),
        )
        .expect("at-timestamp compare succeeds");
    let kv_past =
        find(&past, ComparedCapability::Kv, "default").expect("at-timestamp KV diff present");
    assert_eq!(identities(kv_past.added()), vec![b"k".to_vec()]);
    assert!(kv_past.modified().is_empty());
}

#[test]
fn compare_at_a_timestamp_after_a_branch_latest_is_history_unavailable() {
    let mut database = open_cache_database().expect("cache open succeeds");
    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("fork succeeds");
    {
        let mut kv = database
            .kv(branch("default"), space("default"))
            .expect("default KV opens");
        kv.put(key(b"k"), value(b"v1")).expect("default put");
    }
    // Feature commits last, so its timestamp is after default's latest.
    let feature_timestamp = {
        let mut kv = database
            .kv(branch("feature"), space("default"))
            .expect("feature KV opens");
        kv.put(key(b"k"), value(b"v2"))
            .expect("feature put")
            .commit()
            .timestamp()
    };

    // Reading default as of a timestamp after its latest commit does not clamp;
    // it raises, and the workflow surfaces the diagnostic.
    let error = database
        .branches()
        .expect("branch service opens")
        .compare(
            &branch("default"),
            &branch("feature"),
            BranchStateSelector::AtTimestamp(feature_timestamp),
        )
        .expect_err("a timestamp after a branch's latest is rejected");
    assert_eq!(
        error.code(),
        "history_unavailable.engine.persistence_history"
    );
}

/// Creates `tenant` on default with `k = v1`, then forks `feature` from it.
fn seed_tenant_space_and_fork(database: &mut strata_engine::Database) {
    database
        .spaces(branch("default"))
        .expect("space service opens")
        .create(space("tenant"))
        .expect("space create succeeds");
    database
        .kv(branch("default"), space("tenant"))
        .expect("tenant KV opens")
        .put(key(b"k"), value(b"v1"))
        .expect("put k");
    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("fork succeeds");
}

fn delete_tenant(database: &mut strata_engine::Database, on: &str) {
    database
        .spaces(branch(on))
        .expect("space service opens")
        .delete(&space("tenant"), true)
        .expect("space delete succeeds");
}

fn compare_default_and_feature(
    database: &mut strata_engine::Database,
    selector: BranchStateSelector,
) -> BranchComparison {
    database
        .branches()
        .expect("branch service opens")
        .compare(&branch("default"), &branch("feature"), selector)
        .expect("compare succeeds")
}

/// A space that every compared branch has unregistered since the instant
/// still compares at that instant: its spaces come from each branch's space
/// index as it was then, not from today's. `unregister` either deletes the
/// space outright or leaves it mid-sweep (marked, rows still stored).
fn space_unregistered_since_on_every_branch_compares_then(
    database: &mut strata_engine::Database,
    unregister: fn(&mut strata_engine::Database, &str),
) {
    seed_tenant_space_and_fork(database);
    let then = database
        .kv(branch("feature"), space("tenant"))
        .expect("feature tenant KV opens")
        .put(key(b"k"), value(b"v2"))
        .expect("feature put")
        .commit()
        .timestamp();
    // Both branches unregister the space after `then`, so neither lists it
    // now, and both branches' latest commits come after `then`.
    unregister(database, "default");
    unregister(database, "feature");

    let past = compare_default_and_feature(database, BranchStateSelector::AtTimestamp(then));
    let tenant = find(&past, ComparedCapability::Kv, "tenant")
        .expect("the unregistered space compares at the instant it existed");
    assert_eq!(identities(tenant.modified()), vec![b"k".to_vec()]);
    assert!(tenant.added().is_empty());
    assert!(tenant.removed().is_empty());

    // Direction control: today the space is gone on both branches.
    let current = compare_default_and_feature(database, BranchStateSelector::Current);
    assert!(find(&current, ComparedCapability::Kv, "tenant").is_none());
}

#[test]
fn test_compare_at_a_timestamp_reads_a_space_deleted_since_on_every_branch() {
    let mut database = open_cache_database().expect("cache open succeeds");
    space_unregistered_since_on_every_branch_compares_then(&mut database, delete_tenant);
}

/// Durable, and compared after a reopen: recovery rebuilds the timeline the
/// past instant resolves through, and the historical index must survive it.
#[test]
fn test_compare_at_a_timestamp_reads_a_space_deleted_since_on_every_branch_after_a_reopen() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let mut database = open_durable_database(tempdir.path()).expect("durable open succeeds");
    seed_tenant_space_and_fork(&mut database);
    let then = database
        .kv(branch("feature"), space("tenant"))
        .expect("feature tenant KV opens")
        .put(key(b"k"), value(b"v2"))
        .expect("feature put")
        .commit()
        .timestamp();
    delete_tenant(&mut database, "default");
    delete_tenant(&mut database, "feature");
    database.close().expect("close succeeds");
    drop(database);
    let mut database =
        reopen_durable_database_after_drop(tempdir.path()).expect("durable reopen succeeds");

    let past = compare_default_and_feature(&mut database, BranchStateSelector::AtTimestamp(then));
    let tenant = find(&past, ComparedCapability::Kv, "tenant")
        .expect("the deleted space compares at the instant it existed");
    assert_eq!(identities(tenant.modified()), vec![b"k".to_vec()]);
    let current = compare_default_and_feature(&mut database, BranchStateSelector::Current);
    assert!(find(&current, ComparedCapability::Kv, "tenant").is_none());
}

/// The same with the space left mid-sweep on both branches: marked and
/// unregistered, its rows still stored (#3574).
#[cfg(feature = "testkit")]
#[test]
fn test_compare_at_a_timestamp_reads_a_space_mid_sweep_on_every_branch() {
    fn mark_tenant(database: &mut strata_engine::Database, on: &str) {
        database
            .spaces(branch(on))
            .expect("space service opens")
            .begin_space_delete_for_test(&space("tenant"))
            .expect("the deletion begins");
    }
    let mut database = open_cache_database().expect("cache open succeeds");
    space_unregistered_since_on_every_branch_compares_then(&mut database, mark_tenant);
}

/// Control: a space deleted since the instant on only one branch compares at
/// that instant as it was on both (equal here), and today as absent on the
/// deleted side. This held before #3589 too, because the other branch still
/// lists the space today; it guards that the per-instant enumeration keeps it.
#[test]
fn test_compare_at_a_timestamp_reads_a_space_deleted_since_on_one_branch() {
    let mut database = open_cache_database().expect("cache open succeeds");
    seed_tenant_space_and_fork(&mut database);
    let then = database
        .kv(branch("feature"), space("default"))
        .expect("feature KV opens")
        .put(key(b"elsewhere"), value(b"x"))
        .expect("feature put")
        .commit()
        .timestamp();
    delete_tenant(&mut database, "default");

    let past = compare_default_and_feature(&mut database, BranchStateSelector::AtTimestamp(then));
    assert!(
        find(&past, ComparedCapability::Kv, "tenant").is_none(),
        "at the instant both branches hold k = v1 in tenant"
    );

    let current = compare_default_and_feature(&mut database, BranchStateSelector::Current);
    let tenant = find(&current, ComparedCapability::Kv, "tenant").expect("the space differs today");
    assert_eq!(identities(tenant.added()), vec![b"k".to_vec()]);
    assert!(tenant.removed().is_empty());
    assert!(tenant.modified().is_empty());
}
