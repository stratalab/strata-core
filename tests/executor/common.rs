//! Common test utilities for executor tests

use std::sync::Arc;
use strata_engine::database::OpenSpec;
use strata_engine::{Database, SearchSubsystem};
use strata_executor::{Executor, Output, Session};
use strata_vector::VectorSubsystem;

fn test_cache_db() -> Arc<Database> {
    Database::open_runtime(
        OpenSpec::cache()
            .with_subsystem(strata_graph::GraphSubsystem)
            .with_subsystem(VectorSubsystem)
            .with_subsystem(SearchSubsystem),
    )
    .unwrap()
}

/// Create an executor with an in-memory database
pub(crate) fn create_executor() -> Executor {
    Executor::new(test_cache_db())
}

/// Create a Session with an in-memory database
pub(crate) fn create_session() -> Session {
    Session::new(test_cache_db())
}

/// Create a database for shared use
pub(crate) fn create_db() -> Arc<Database> {
    test_cache_db()
}

/// Helper to create an event payload (must be an Object)
pub(crate) fn event_payload(key: &str, value: strata_core::Value) -> strata_core::Value {
    strata_core::Value::object([(key.to_string(), value)].into_iter().collect())
}

/// Extract version from Output::Version
#[allow(dead_code)]
pub(crate) fn extract_version(output: &Output) -> u64 {
    match output {
        Output::Version(v) => *v,
        _ => panic!("Expected Output::Version, got {:?}", output),
    }
}

/// Extract bool from Output::Bool
#[allow(dead_code)]
pub(crate) fn extract_bool(output: &Output) -> bool {
    match output {
        Output::Bool(b) => *b,
        _ => panic!("Expected Output::Bool, got {:?}", output),
    }
}

/// Extract value from Output::Maybe or Output::MaybeVersioned.
/// Returns Option<Value> regardless of which variant is used.
#[allow(dead_code)]
pub(crate) fn extract_maybe_value(output: Output) -> Option<strata_core::Value> {
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
pub(crate) fn is_some_value(output: &Output) -> bool {
    matches!(
        output,
        Output::Maybe(Some(_)) | Output::MaybeVersioned(Some(_))
    )
}

/// Returns true if the output is None (either Maybe or MaybeVersioned).
#[allow(dead_code)]
pub(crate) fn is_none_value(output: &Output) -> bool {
    matches!(output, Output::Maybe(None) | Output::MaybeVersioned(None))
}
