//! Integration Tests
//!
//! Comprehensive cross-layer tests organized by test dimensions:
//! - Storage mode: persistent vs ephemeral
//! - Durability: none, batched, strict
//! - Primitives: single vs cross-primitive
//! - Scale: 1k, 10k, 100k records
//! - Branching: branch isolation and forking

#[path = "../common/mod.rs"]
mod common;

mod branching;
mod branching_control_store_recovery;
mod branching_convergence_differential;
mod branching_gc_quarantine_recovery;
mod branching_generation_migration;
mod branching_guardrails;
mod branching_lifecycle_gate;
mod branching_lifecycle_restart;
mod branching_merge_lineage_edges;
mod branching_recreate_state_machine;
mod branching_retention_matrix;
mod branching_same_name_race;
mod merge_base_characterization;
mod modes;
mod primitives;
mod recovery_cross_crate;
mod scale;
