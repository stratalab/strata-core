//! Transaction Runtime Integration Tests
//!
//! Tests for the storage-owned OCC transaction runtime: snapshot isolation,
//! CAS, conflict detection, lifecycle, and the durability commit adapter.

#[path = "../common/mod.rs"]
mod common;

mod cas_operations;
mod concurrent_transactions;
mod conflict_detection;
mod manager_commit;
mod occ_invariants;
mod snapshot_isolation;
mod stress;
mod transaction_lifecycle;
mod transaction_states;
mod version_counter;
