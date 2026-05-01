//! # Lock Ordering (Deadlock Prevention)
//!
//! All lock acquisitions in Strata MUST follow this order. Acquiring a
//! lower-numbered lock while holding a higher-numbered lock is forbidden.
//!
//! | Level | Lock                    | Location                              | Held By                                |
//! |-------|-------------------------|---------------------------------------|----------------------------------------|
//! | 1     | Quiesce RwLock          | `TransactionManager.commit_quiesce`   | Commits (shared), Checkpoint (excl)    |
//! | 2     | Per-branch commit Mutex | `TransactionManager.commit_locks`     | Commit protocol                        |
//! | 3     | Deletion barrier RwLock | `SegmentRefRegistry.deletion_barrier` | Fork (shared), Compaction (excl)       |
//! | 4     | WAL Mutex               | `WalWriter` via `Arc<Mutex>`          | WAL append                             |
//! | 5     | DashMap shard guards    | `SegmentedStore.branches`             | Storage reads/writes                   |
//!
//! ## Rules
//!
//! - **Never acquire Level N while holding Level N+k.**
//!   For example, never acquire the quiesce lock (1) while holding a branch
//!   lock (2), and never acquire a branch lock (2) while holding the WAL lock (4).
//!
//! - **DashMap guards (Level 5) must be dropped before any I/O.**
//!   Clone the `Arc` from the entry, then drop the shard guard before locking
//!   the inner `Mutex` or performing disk writes.
//!
//! - **WAL lock (Level 4) is inside the branch lock (Level 2).**
//!   The commit path acquires: quiesce read (1) → branch mutex (2) → WAL mutex (4).
//!   The WAL lock is released before `apply_writes` to avoid holding it during
//!   storage application.
//!
//! - **Deletion barrier (Level 3) is independent of the commit path.**
//!   Fork holds a read guard (Level 3); compaction holds a write guard (Level 3).
//!   Neither path holds commit locks (Level 2) at the same time.
//!
//! ## Commit path lock sequence
//!
//! ```text
//! commit_inner():
//!   1. commit_quiesce.read()          — Level 1 (shared)
//!   2. commit_locks.entry() → clone   — Level 5 (shard guard, dropped immediately)
//!   3. commit_mutex.lock()            — Level 2
//!   4. wal.lock() → append → drop     — Level 4 (brief, inside Level 2)
//!   5. apply_writes()                 — Level 5 (shard guards, brief)
//!   6. drop commit_mutex              — releases Level 2
//!   7. drop quiesce guard             — releases Level 1
//! ```
//!
//! ## Checkpoint path
//!
//! ```text
//! quiesced_version():
//!   1. commit_quiesce.write()         — Level 1 (exclusive, drains all commits)
//!   2. read version counter
//!   3. drop quiesce guard             — releases Level 1
//! ```
//!
//! ## Fork path
//!
//! ```text
//! fork_branch():
//!   1. deletion_barrier.read()        — Level 3 (shared)
//!   2. source branch DashMap guard    — Level 5 (brief, for segment snapshot)
//!   3. increment refcounts
//!   4. drop deletion barrier          — releases Level 3
//! ```
//!
//! ## Compaction delete path
//!
//! ```text
//! delete_segment_if_unreferenced():
//!   1. deletion_barrier.write()       — Level 3 (exclusive)
//!   2. check refcount, delete file
//!   3. drop deletion barrier          — releases Level 3
//! ```
//!
//! ## Adding new locks
//!
//! When introducing a new lock:
//! 1. Assign it a level in the hierarchy above.
//! 2. Annotate every acquisition site with `// Lock Level N: <name>`.
//! 3. Verify no caller holds a lower-numbered lock when acquiring it.
//! 4. Update this module's documentation.
