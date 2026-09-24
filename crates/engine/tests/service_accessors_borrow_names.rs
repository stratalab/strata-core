//! The service accessors accept a borrowed branch name and product space.
//!
//! A caller holding a `BranchName` and `ProductSpace` for a whole session —
//! a save that writes JSON, a graph, and an event; a periodic tick that
//! persists — should not clone them at every call. The accessors take
//! `impl Into<_>`, so an owned value is moved at no cost and a reference is
//! cloned exactly once, inside the constructor that must own it.

mod common;

use strata_engine::{BranchName, KvKey, KvValue, ProductSpace};

use common::open_cache_database;

/// One branch name and one space, held for a session, serve every accessor
/// by reference — nothing is moved, nothing is cloned at the call site.
#[test]
fn service_accessors_take_branch_and_space_by_reference() {
    let mut database = open_cache_database().expect("cache open");
    let branch = BranchName::new("default").expect("branch name");
    let space = ProductSpace::new("default").expect("product space");

    // A service built from borrowed names is fully functional, not merely
    // constructible: a write through one borrowed handle is read back through
    // a second, independently borrowed one.
    let key = KvKey::new("k").expect("key");
    database
        .kv(&branch, &space)
        .expect("kv service")
        .put(key.clone(), KvValue::new("v"))
        .expect("put through a borrowed-name service");
    let value = database
        .kv(&branch, &space)
        .expect("kv service again")
        .get(&key)
        .expect("get through a borrowed-name service")
        .expect("the write is visible");
    assert_eq!(value.as_bytes(), b"v");

    database.json(&branch, &space).expect("json service");
    database.event(&branch, &space).expect("event service");
    database.graph(&branch, &space).expect("graph service");
    database.vector(&branch, &space).expect("vector service");
    database.spaces(&branch).expect("space service");

    // The originals were borrowed, not moved: still here, still the same.
    assert_eq!(branch.as_str(), "default");
    assert_eq!(space.as_str(), "default");
}

/// A borrowed name converts to an owned equal — the one clone a by-reference
/// call pays, and it pays it inside the constructor rather than at every
/// call site.
#[test]
fn borrowed_branch_name_converts_to_an_equal_owned_name() {
    let branch = BranchName::new("feature-x").expect("branch name");
    let owned = BranchName::from(&branch);
    assert_eq!(owned, branch);
    assert_eq!(owned.as_str(), "feature-x");
}

/// Same for the product space.
#[test]
fn borrowed_product_space_converts_to_an_equal_owned_space() {
    let space = ProductSpace::new("orders").expect("product space");
    let owned = ProductSpace::from(&space);
    assert_eq!(owned, space);
    assert_eq!(owned.as_str(), "orders");
}
