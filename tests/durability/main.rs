//! Integration tests for the durability layer.
//!
//! These tests exercise WAL, snapshot, and recovery behaviors at the
//! Database level — things that cannot be tested in unit tests because
//! they require a real Database lifecycle (open → write → close → reopen).
//!
//! Unit tests in `crates/storage/src/durability/` cover encoding, decoding,
//! corruption detection, and replay logic in isolation. These integration
//! tests cover the end-to-end guarantees of the storage-owned durability
//! runtime.

#[path = "../common/mod.rs"]
mod common;

mod crash_recovery;
mod cross_primitive_recovery;
mod mode_equivalence;
mod recovery_invariants;
mod snapshot_lifecycle;
mod stress;
mod wal_lifecycle;
