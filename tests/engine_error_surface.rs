//! Downstream surface checks for the engine-owned parent error boundary.

use strata_core::{BranchId, StrataError as LegacyStrataError};
use strata_engine::{database::OpenSpec, Database, StrataError, StrataResult};
use strata_storage::StorageError;

#[test]
fn engine_reexports_parent_error_for_downstream_consumers() {
    fn fail() -> StrataResult<()> {
        Err(StrataError::invalid_input("surface check"))
    }

    fn propagate() -> StrataResult<()> {
        fail()?;
        Ok(())
    }

    let err = propagate().unwrap_err();
    assert!(matches!(err, StrataError::InvalidInput { .. }));
    assert!(matches!(err, LegacyStrataError::InvalidInput { .. }));
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
