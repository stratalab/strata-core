//! Common test utilities for executor tests

use std::sync::Arc;
use strata_engine::Database;
use strata_executor::{Executor, Output, Session, Strata};

/// Create an executor with an in-memory database
pub fn create_executor() -> Executor {
    let db = Database::cache().unwrap();
    strata_graph::branch_dag::init_system_branch(&db);
    // Register the branch DAG hook so that fork / merge / revert /
    // cherry-pick / create / delete operations through this executor
    // record events in the `_branch_dag` graph. Idempotent via OnceCell.
    strata_graph::register_branch_dag_hook_implementation();
    Executor::new(db)
}

/// Create a Strata API wrapper with an in-memory database
pub fn create_strata() -> Strata {
    Strata::cache().unwrap()
}

/// Create a Session with an in-memory database
pub fn create_session() -> Session {
    let db = Database::cache().unwrap();
    strata_graph::branch_dag::init_system_branch(&db);
    strata_graph::register_branch_dag_hook_implementation();
    Session::new(db)
}

/// Create a database for shared use
pub fn create_db() -> Arc<Database> {
    let db = Database::cache().unwrap();
    strata_graph::branch_dag::init_system_branch(&db);
    strata_graph::register_branch_dag_hook_implementation();
    db
}

/// Helper to create an event payload (must be an Object)
pub fn event_payload(key: &str, value: strata_core::Value) -> strata_core::Value {
    strata_core::Value::object([(key.to_string(), value)].into_iter().collect())
}

/// Extract version from Output::Version
#[allow(dead_code)]
pub fn extract_version(output: &Output) -> u64 {
    match output {
        Output::Version(v) => *v,
        _ => panic!("Expected Output::Version, got {:?}", output),
    }
}

/// Extract bool from Output::Bool
#[allow(dead_code)]
pub fn extract_bool(output: &Output) -> bool {
    match output {
        Output::Bool(b) => *b,
        _ => panic!("Expected Output::Bool, got {:?}", output),
    }
}

/// Extract value from Output::Maybe or Output::MaybeVersioned.
/// Returns Option<Value> regardless of which variant is used.
#[allow(dead_code)]
pub fn extract_maybe_value(output: Output) -> Option<strata_core::Value> {
    match output {
        Output::MaybeVersioned(v) => v.map(|vv| vv.value),
        Output::Maybe(v) => v,
        _ => panic!(
            "Expected Output::Maybe or Output::MaybeVersioned, got {:?}",
            output
        ),
    }
}

/// Returns true if the output is a Some value (either Maybe or MaybeVersioned).
#[allow(dead_code)]
pub fn is_some_value(output: &Output) -> bool {
    matches!(
        output,
        Output::Maybe(Some(_)) | Output::MaybeVersioned(Some(_))
    )
}

/// Returns true if the output is None (either Maybe or MaybeVersioned).
#[allow(dead_code)]
pub fn is_none_value(output: &Output) -> bool {
    matches!(output, Output::Maybe(None) | Output::MaybeVersioned(None))
}
