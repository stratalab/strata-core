//! Downstream surface checks for the engine-owned parent error boundary.

use std::sync::Arc;

use strata_core::{contract::PrimitiveType, BranchId};
use strata_engine::{
    database::OpenSpec, BranchRef, Database, PrimitiveDegradedReason, StrataError, StrataResult,
};
use strata_storage::StorageError;

#[test]
fn invalid_branch_name_surfaces_engine_owned_error_from_real_api() {
    fn create_invalid_branch(db: &Arc<Database>) -> StrataResult<()> {
        db.branches().create("").map(|_| ())?;
        Ok(())
    }

    let db = Database::open_runtime(OpenSpec::cache()).unwrap();
    let err = create_invalid_branch(&db).unwrap_err();

    assert!(matches!(
        err,
        StrataError::InvalidInput { ref message }
        if message.contains("branch name cannot be empty")
    ));
}

#[test]
fn engine_database_methods_return_engine_owned_result_surface() {
    fn read_time_range(db: &Database, branch_id: BranchId) -> StrataResult<Option<(u64, u64)>> {
        db.time_range(branch_id)
    }

    let db = Database::open_runtime(OpenSpec::cache()).unwrap();
    let branch_id = BranchId::from_user_name("main");

    assert_eq!(read_time_range(&db, branch_id).unwrap(), None);
}

#[test]
fn storage_corruption_lifts_cleanly_into_engine_parent_error() {
    fn lift_storage_error() -> StrataResult<()> {
        Err(StorageError::corruption("segment block truncated").into())
    }

    let err = lift_storage_error().unwrap_err();
    assert!(matches!(
        err,
        StrataError::Corruption { ref message }
        if message == "segment block truncated"
    ));
}

#[test]
fn primitive_degraded_branch_field_round_trips_through_engine_branchref_surface() {
    let branch = BranchRef::new(BranchId::from_user_name("main"), 3);
    let err = StrataError::primitive_degraded(
        branch,
        PrimitiveType::Vector,
        "docs",
        PrimitiveDegradedReason::ConfigMismatch,
    );

    match err {
        StrataError::PrimitiveDegraded {
            branch: err_branch,
            primitive,
            name,
            reason,
        } => {
            assert_eq!(err_branch, branch);
            assert_eq!(primitive, PrimitiveType::Vector);
            assert_eq!(name, "docs");
            assert_eq!(reason, PrimitiveDegradedReason::ConfigMismatch);
        }
        other => panic!("expected PrimitiveDegraded, got {other:?}"),
    }
}
