//! Recovery Participant Registry
//!
//! Defines the interface for primitives that need to participate in
//! Database recovery. This is for primitives with runtime state that
//! lives outside SegmentedStore (e.g., VectorStore's in-memory backends).
//!
//! ## Design Philosophy
//!
//! Recovery is a cold-path, correctness-critical system. The design is:
//! - Explicit, boring, and obvious
//! - Minimal trait surface
//! - No over-generalization
//!
//! ## How It Works
//!
//! 1. Database opens and performs KV recovery via RecoveryCoordinator
//! 2. Database calls `recover_all_participants()` which invokes each registered participant
//! 3. Each participant replays its WAL entries into its runtime state
//! 4. Database is ready for use
//!
//! ## Registration
//!
//! Primitives register their recovery functions at startup:
//!
//! ```text
//! use strata_engine::{register_recovery_participant, RecoveryParticipant};
//!
//! // Called once at initialization
//! register_recovery_participant(RecoveryParticipant::new("vector", recover_vector_state));
//! ```

use parking_lot::RwLock;
use strata_core::StrataResult;
use tracing::info;

/// Function signature for primitive recovery
///
/// Takes a reference to the Database and performs recovery for a specific
/// primitive's runtime state. The function should:
/// 1. Scan the KV store for relevant entries (already recovered from WAL)
/// 2. Rebuild runtime state into the primitive's extension state
/// 3. Return Ok(()) on success or an error on failure
///
/// Recovery functions are stateless - they use the Database's extension
/// mechanism to access shared state.
pub type RecoveryFn = fn(&crate::database::Database) -> StrataResult<()>;

/// Registry entry for a recovery participant
#[derive(Clone)]
pub struct RecoveryParticipant {
    /// Human-readable name for logging
    pub name: &'static str,
    /// Recovery function to call
    pub recover: RecoveryFn,
}

impl RecoveryParticipant {
    /// Create a new recovery participant
    pub const fn new(name: &'static str, recover: RecoveryFn) -> Self {
        Self { name, recover }
    }
}

impl std::fmt::Debug for RecoveryParticipant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecoveryParticipant")
            .field("name", &self.name)
            .finish()
    }
}

/// Global registry of recovery participants
///
/// Uses lazy initialization with a RwLock for thread-safe access.
static RECOVERY_REGISTRY: once_cell::sync::Lazy<RwLock<Vec<RecoveryParticipant>>> =
    once_cell::sync::Lazy::new(|| RwLock::new(Vec::new()));

/// Register a recovery participant
///
/// This should be called once during application initialization,
/// before any Database is opened.
///
/// # Thread Safety
///
/// This function is thread-safe and can be called from multiple threads,
/// though typically it's called once during startup.
pub fn register_recovery_participant(participant: RecoveryParticipant) {
    let mut registry = RECOVERY_REGISTRY.write();
    // Avoid duplicate registration
    if !registry.iter().any(|p| p.name == participant.name) {
        info!(target: "strata::recovery", name = participant.name, "Registered recovery participant");
        registry.push(participant);
    }
}

/// Run recovery for all registered participants
///
/// Called by Database::open_with_mode after KV recovery completes.
/// Each participant's recovery function is called in registration order.
///
/// # Errors
///
/// Returns the first error encountered. If a participant fails,
/// subsequent participants are not called.
pub fn recover_all_participants(db: &crate::database::Database) -> StrataResult<()> {
    let registry = RECOVERY_REGISTRY.read();

    for participant in registry.iter() {
        info!(target: "strata::recovery", name = participant.name, "Running primitive recovery");
        (participant.recover)(db)?;
        info!(target: "strata::recovery", name = participant.name, "Primitive recovery complete");
    }

    Ok(())
}

/// Clear the recovery registry (for testing only)
#[cfg(test)]
pub fn clear_recovery_registry() {
    let mut registry = RECOVERY_REGISTRY.write();
    registry.clear();
}

/// Get the number of registered participants (for testing only)
#[cfg(test)]
pub fn recovery_registry_count() -> usize {
    let registry = RECOVERY_REGISTRY.read();
    registry.len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    // Use a mutex to serialize tests that modify the global registry
    static TEST_LOCK: once_cell::sync::Lazy<Mutex<()>> =
        once_cell::sync::Lazy::new(|| Mutex::new(()));

    // Mock recovery function that succeeds
    fn mock_recovery_success(_db: &crate::database::Database) -> StrataResult<()> {
        Ok(())
    }

    #[test]
    fn test_recovery_participant_new() {
        let participant = RecoveryParticipant::new("test", mock_recovery_success);
        assert_eq!(participant.name, "test");
    }

    #[test]
    fn test_recovery_participant_debug() {
        let participant = RecoveryParticipant::new("vector", mock_recovery_success);
        let debug_str = format!("{:?}", participant);
        assert!(debug_str.contains("RecoveryParticipant"));
        assert!(debug_str.contains("vector"));
    }

    #[test]
    fn test_recovery_participant_clone() {
        let participant = RecoveryParticipant::new("test", mock_recovery_success);
        let cloned = participant.clone();
        assert_eq!(participant.name, cloned.name);
    }

    #[test]
    fn test_register_and_count() {
        let _lock = TEST_LOCK.lock();
        clear_recovery_registry();

        assert_eq!(recovery_registry_count(), 0);

        register_recovery_participant(RecoveryParticipant::new("p1", mock_recovery_success));
        assert_eq!(recovery_registry_count(), 1);

        register_recovery_participant(RecoveryParticipant::new("p2", mock_recovery_success));
        assert_eq!(recovery_registry_count(), 2);

        register_recovery_participant(RecoveryParticipant::new("p3", mock_recovery_success));
        assert_eq!(recovery_registry_count(), 3);
    }

    #[test]
    fn test_duplicate_registration_prevented() {
        let _lock = TEST_LOCK.lock();
        clear_recovery_registry();

        register_recovery_participant(RecoveryParticipant::new("dup_name", mock_recovery_success));
        assert_eq!(recovery_registry_count(), 1);

        // Same name should be ignored
        register_recovery_participant(RecoveryParticipant::new("dup_name", mock_recovery_success));
        assert_eq!(recovery_registry_count(), 1);

        // Different name should work
        register_recovery_participant(RecoveryParticipant::new(
            "other_name",
            mock_recovery_success,
        ));
        assert_eq!(recovery_registry_count(), 2);
    }

    #[test]
    fn test_clear_recovery_registry() {
        let _lock = TEST_LOCK.lock();

        register_recovery_participant(RecoveryParticipant::new(
            "clear_test",
            mock_recovery_success,
        ));
        assert!(recovery_registry_count() > 0);

        clear_recovery_registry();
        assert_eq!(recovery_registry_count(), 0);
    }

    #[test]
    fn test_recover_empty_registry() {
        use tempfile::tempdir;

        let _lock = TEST_LOCK.lock();
        clear_recovery_registry();

        let dir = tempdir().unwrap();
        let db = crate::database::Database::open(dir.path()).unwrap();

        let result = recover_all_participants(&db);
        assert!(result.is_ok(), "Empty registry should succeed");

        db.shutdown().unwrap();
    }

    #[test]
    fn test_recover_calls_participant() {
        use tempfile::tempdir;

        let _lock = TEST_LOCK.lock();
        clear_recovery_registry();

        static CALL_COUNT: AtomicUsize = AtomicUsize::new(0);

        fn counting_recovery(_db: &crate::database::Database) -> StrataResult<()> {
            CALL_COUNT.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        CALL_COUNT.store(0, Ordering::SeqCst);

        let dir = tempdir().unwrap();
        let db = crate::database::Database::open(dir.path()).unwrap();

        register_recovery_participant(RecoveryParticipant::new("counter", counting_recovery));

        let result = recover_all_participants(&db);
        assert!(result.is_ok());
        assert_eq!(
            CALL_COUNT.load(Ordering::SeqCst),
            1,
            "Recovery should be called exactly once"
        );

        db.shutdown().unwrap();
    }

    #[test]
    fn test_recover_calls_in_order() {
        use tempfile::tempdir;

        let _lock = TEST_LOCK.lock();
        clear_recovery_registry();

        static ORDER: once_cell::sync::Lazy<Mutex<Vec<&'static str>>> =
            once_cell::sync::Lazy::new(|| Mutex::new(Vec::new()));

        fn first_recovery(_db: &crate::database::Database) -> StrataResult<()> {
            ORDER.lock().push("first");
            Ok(())
        }

        fn second_recovery(_db: &crate::database::Database) -> StrataResult<()> {
            ORDER.lock().push("second");
            Ok(())
        }

        fn third_recovery(_db: &crate::database::Database) -> StrataResult<()> {
            ORDER.lock().push("third");
            Ok(())
        }

        ORDER.lock().clear();

        let dir = tempdir().unwrap();
        let db = crate::database::Database::open(dir.path()).unwrap();

        register_recovery_participant(RecoveryParticipant::new("first", first_recovery));
        register_recovery_participant(RecoveryParticipant::new("second", second_recovery));
        register_recovery_participant(RecoveryParticipant::new("third", third_recovery));

        let result = recover_all_participants(&db);
        assert!(result.is_ok());

        let order = ORDER.lock();
        assert_eq!(
            order.as_slice(),
            &["first", "second", "third"],
            "Should call in registration order"
        );

        db.shutdown().unwrap();
    }

    #[test]
    fn test_recover_error_stops_execution() {
        use tempfile::tempdir;

        let _lock = TEST_LOCK.lock();
        clear_recovery_registry();

        static CALLED: once_cell::sync::Lazy<Mutex<Vec<&'static str>>> =
            once_cell::sync::Lazy::new(|| Mutex::new(Vec::new()));

        fn success_recovery(_db: &crate::database::Database) -> StrataResult<()> {
            CALLED.lock().push("success");
            Ok(())
        }

        fn failing_recovery(_db: &crate::database::Database) -> StrataResult<()> {
            CALLED.lock().push("failing");
            Err(strata_core::StrataError::corruption("test failure"))
        }

        fn should_not_run(_db: &crate::database::Database) -> StrataResult<()> {
            CALLED.lock().push("should_not_run");
            Ok(())
        }

        CALLED.lock().clear();

        let dir = tempdir().unwrap();
        let db = crate::database::Database::open(dir.path()).unwrap();

        register_recovery_participant(RecoveryParticipant::new("success", success_recovery));
        register_recovery_participant(RecoveryParticipant::new("failing", failing_recovery));
        register_recovery_participant(RecoveryParticipant::new("never", should_not_run));

        let result = recover_all_participants(&db);
        assert!(result.is_err(), "Should return error");

        let called = CALLED.lock();
        assert_eq!(called.len(), 2, "Should stop after failure");
        assert!(called.contains(&"success"));
        assert!(called.contains(&"failing"));
        assert!(
            !called.contains(&"should_not_run"),
            "Should not call participant after failure"
        );

        db.shutdown().unwrap();
    }

    #[test]
    fn test_concurrent_registration_no_data_race() {
        use std::thread;

        let _lock = TEST_LOCK.lock();
        clear_recovery_registry();

        let barrier = Arc::new(std::sync::Barrier::new(10));
        let handles: Vec<_> = (0..10)
            .map(|i| {
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    // Each thread registers with a unique static name
                    let name: &'static str = match i {
                        0 => "concurrent_0",
                        1 => "concurrent_1",
                        2 => "concurrent_2",
                        3 => "concurrent_3",
                        4 => "concurrent_4",
                        5 => "concurrent_5",
                        6 => "concurrent_6",
                        7 => "concurrent_7",
                        8 => "concurrent_8",
                        _ => "concurrent_9",
                    };
                    register_recovery_participant(RecoveryParticipant::new(
                        name,
                        mock_recovery_success,
                    ));
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(
            recovery_registry_count(),
            10,
            "All 10 unique participants should be registered"
        );
    }

    #[test]
    fn test_concurrent_duplicate_registration_safe() {
        use std::thread;

        let _lock = TEST_LOCK.lock();
        clear_recovery_registry();

        let barrier = Arc::new(std::sync::Barrier::new(10));
        let handles: Vec<_> = (0..10)
            .map(|_| {
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    // All threads try same name
                    register_recovery_participant(RecoveryParticipant::new(
                        "same_name",
                        mock_recovery_success,
                    ));
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(
            recovery_registry_count(),
            1,
            "Only one should be registered despite concurrent attempts"
        );
    }
}
