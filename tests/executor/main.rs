//! Executor Layer Tests
//!
//! Tests for the strata-executor crate which provides:
//! - Command enum (114 variants) - the instruction set
//! - Output enum - typed results
//! - Executor - stateless command dispatch
//! - Session - stateful transaction support
//! - Strata - high-level typed wrapper API

mod common;

mod adversarial;
mod branch_invariants;
mod command_dispatch;
mod cross_primitive_time_travel;
mod error_handling;
mod serialization;
mod session_transactions;
mod strata_api;
