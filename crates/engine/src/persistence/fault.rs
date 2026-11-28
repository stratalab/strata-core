//! Deterministic storage-fault injection for engine boundary tests.
//!
//! [`FaultOp`] names the persistence operations a test can target. The schedule
//! and the public [`StorageFaultKind`] selector are test-only: in production
//! builds `guard_fault` compiles to a no-op, so the seam adds no behavior to a
//! shipped engine. Faults are fabricated as storage errors and flow through the
//! real `map_storage_error`, so tests exercise the genuine error-translation
//! path rather than a stand-in.

/// Persistence operation a fault can target.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FaultOp {
    Commit,
    Read,
    Scan,
    Branch,
}

impl FaultOp {
    /// A dense index for per-kind tallies.
    #[cfg(any(test, feature = "testkit"))]
    const fn index(self) -> usize {
        match self {
            Self::Commit => 0,
            Self::Read => 1,
            Self::Scan => 2,
            Self::Branch => 3,
        }
    }
}

#[cfg(any(test, feature = "testkit"))]
pub(crate) use injection::{CorruptionSchedule, FaultSchedule};
#[cfg(any(test, feature = "testkit"))]
pub use injection::{RowCorruption, StorageFaultKind};

#[cfg(any(test, feature = "testkit"))]
mod injection {
    use std::sync::{Mutex, MutexGuard};

    use strata_core::BranchId;
    use strata_storage::api::{StorageApiError, StorageApiLowerLayer};

    /// Borrows a schedule's entries, recovering from poisoning.
    ///
    /// A test that panics mid-operation must not make the schedule
    /// permanently unusable for the rest of the run — the schedule holds no
    /// invariant a panic could break, only a pending-work list.
    fn entries_array(lock: &Mutex<[u64; 4]>) -> MutexGuard<'_, [u64; 4]> {
        lock.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn entries<T>(lock: &Mutex<Vec<T>>) -> MutexGuard<'_, Vec<T>> {
        lock.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    use super::FaultOp;

    /// The storage failure a test wants the next matching operation to surface.
    ///
    /// Each variant maps to a concrete `StorageApiError`, so the engine's real
    /// `map_storage_error` decides the resulting engine status.
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub enum StorageFaultKind {
        /// Retryable resource-budget exhaustion.
        ResourceExhausted,
        /// Commit whose durability could not be proven.
        AmbiguousCommit,
        /// Degraded recovery (corruption-class) signal.
        RecoveryDegraded,
        /// Retryable lower-layer unavailability.
        Unavailable,
        /// Target was not found.
        NotFound,
        /// Request conflicted with existing state.
        Conflict,
    }

    impl StorageFaultKind {
        fn into_storage_error(self) -> StorageApiError {
            match self {
                Self::ResourceExhausted => StorageApiError::ResourceExhausted {
                    resource: "memory",
                    requested_bytes: 0,
                    used_bytes: 0,
                    limit_bytes: 0,
                    reason: "injected storage fault",
                },
                Self::AmbiguousCommit => {
                    StorageApiError::durable_uncertain("injected storage fault")
                }
                Self::RecoveryDegraded => StorageApiError::RecoveryDegraded {
                    reason: "injected storage fault",
                },
                Self::Unavailable => StorageApiError::lower_layer_with(
                    StorageApiLowerLayer::Service,
                    "injected storage fault",
                    std::io::Error::other("injected storage fault"),
                ),
                Self::NotFound => StorageApiError::BranchNotFound {
                    branch_id: BranchId::from_bytes([0; BranchId::BYTE_LEN]),
                },
                Self::Conflict => StorageApiError::Conflict {
                    branch_id: BranchId::from_bytes([0; BranchId::BYTE_LEN]),
                    storage_space: None,
                    key_fingerprint: None,
                    user_key_len: None,
                    reason: "injected storage fault",
                },
            }
        }
    }

    struct ScheduledFault {
        op: FaultOp,
        error: StorageApiError,
        /// Number of matching operations that pass before this fault fires.
        skip: usize,
    }

    /// A test closure to run on the calling thread when a matching operation
    /// reaches the fault seam: before a lone read or commit reaches storage,
    /// or just after a batch read's answer is read (#3582).
    struct ScheduledHook {
        op: FaultOp,
        hook: Box<dyn FnOnce() + Send>,
        /// Number of matching operations that pass before this hook runs.
        skip: usize,
    }

    /// FIFO schedule of pending faults, matched and consumed per operation.
    ///
    /// Interior mutability so arming and consuming take `&self` (#3156). The
    /// engine's read path is `&self`, and this test-only hook is the only
    /// reason it ever needed `&mut` — the production build's `guard_fault` has
    /// always been `&self` and a no-op. A `Mutex` rather than a `RefCell`
    /// because the same hook must survive the multi-threaded reader tests that
    /// a shared read path exists to enable; the lock cost is irrelevant in a
    /// schedule that only exists under `cfg(test)`.
    #[derive(Default)]
    pub(crate) struct FaultSchedule {
        entries: Mutex<Vec<ScheduledFault>>,
        hooks: Mutex<Vec<ScheduledHook>>,
        /// How many operations of each kind reached the seam, so a test can
        /// count an operation's persistence calls and pause at each one in
        /// turn (the concurrency conformance harness).
        calls: Mutex<[u64; 4]>,
    }

    impl FaultSchedule {
        /// Arms a fault that fires after `skip` matching operations pass.
        pub(crate) fn arm(&self, op: FaultOp, kind: StorageFaultKind, skip: usize) {
            entries(&self.entries).push(ScheduledFault {
                op,
                error: kind.into_storage_error(),
                skip,
            });
        }

        /// Returns the fault for `op` if one is due, letting `skip` occurrences
        /// pass through to real storage first.
        pub(crate) fn take(&self, op: FaultOp) -> Option<StorageApiError> {
            let mut entries = entries(&self.entries);
            let position = entries.iter().position(|entry| entry.op == op)?;
            if entries[position].skip > 0 {
                entries[position].skip -= 1;
                return None;
            }
            Some(entries.remove(position).error)
        }

        /// How many `op` operations have reached the seam so far.
        #[cfg(test)]
        pub(crate) fn calls(&self, op: FaultOp) -> u64 {
            entries_array(&self.calls)[op.index()]
        }

        /// Arms a hook that runs when the matching operation that follows
        /// `skip` passing ones reaches the fault seam, so a test can hold a
        /// multi-entry read between two of its entries while another thread
        /// commits (#3582).
        pub(crate) fn arm_hook(&self, op: FaultOp, skip: usize, hook: Box<dyn FnOnce() + Send>) {
            entries(&self.hooks).push(ScheduledHook { op, hook, skip });
        }

        /// Runs the hook for `op` if one is due. The schedule's lock is
        /// released before the hook runs, so a hook may block while other
        /// threads' operations (the commit it waits for among them) pass
        /// through the schedule.
        pub(crate) fn run_hook(&self, op: FaultOp) {
            entries_array(&self.calls)[op.index()] += 1;
            let hook = {
                let mut hooks = entries(&self.hooks);
                let Some(position) = hooks.iter().position(|entry| entry.op == op) else {
                    return;
                };
                if hooks[position].skip > 0 {
                    hooks[position].skip -= 1;
                    return;
                }
                hooks.remove(position).hook
            };
            hook();
        }
    }

    /// A content corruption to apply to the rows a successful read returns —
    /// modelling on-disk bit-rot that the storage layer hands back intact but
    /// whose *content* the engine's decoders must reject. Distinct from
    /// [`StorageFaultKind`], which fails the operation outright; this lets the
    /// read succeed and corrupts what it yields, so the engine's `data_loss.*`
    /// detectors run on the genuine decode path.
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub enum RowCorruption {
        /// Strip the value cell from the row (`Some(bytes)` -> `None`), as if the
        /// stored row lost its value.
        DropValue,
        /// Replace the value bytes with malformed content: present, but not
        /// decodable as the record the caller expects.
        SetValue(Vec<u8>),
        /// Replace the key bytes with malformed content — for rows whose *key*
        /// is itself a decoded record (index keys), so the key decoder rejects
        /// them before the value is ever read.
        SetKey(Vec<u8>),
    }

    impl RowCorruption {
        /// Applies this corruption to one row's key and value cells.
        pub(crate) fn apply(&self, key: &mut Vec<u8>, value: &mut Option<Vec<u8>>) {
            match self {
                Self::DropValue => *value = None,
                Self::SetValue(bytes) => *value = Some(bytes.clone()),
                Self::SetKey(bytes) => key.clone_from(bytes),
            }
        }
    }

    struct ScheduledCorruption {
        op: FaultOp,
        corruption: RowCorruption,
        /// Number of matching operations that pass before this corruption fires.
        skip: usize,
    }

    /// FIFO schedule of pending row corruptions, matched and consumed per
    /// operation exactly like [`FaultSchedule`].
    #[derive(Default)]
    pub(crate) struct CorruptionSchedule {
        entries: Mutex<Vec<ScheduledCorruption>>,
    }

    impl CorruptionSchedule {
        /// Arms a corruption that fires after `skip` matching operations pass.
        pub(crate) fn arm(&self, op: FaultOp, corruption: RowCorruption, skip: usize) {
            entries(&self.entries).push(ScheduledCorruption {
                op,
                corruption,
                skip,
            });
        }

        /// Returns the corruption for `op` if one is due, letting `skip`
        /// occurrences pass through uncorrupted first.
        pub(crate) fn take(&self, op: FaultOp) -> Option<RowCorruption> {
            let mut entries = entries(&self.entries);
            let position = entries.iter().position(|entry| entry.op == op)?;
            if entries[position].skip > 0 {
                entries[position].skip -= 1;
                return None;
            }
            Some(entries.remove(position).corruption)
        }
    }
}
