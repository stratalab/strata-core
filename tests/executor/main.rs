//! Executor Layer Tests
//!
//! Tests for the strata-executor crate which provides:
//! - Command enum (114 variants) - the instruction set
//! - Output enum - typed results
//! - Executor - stateless command dispatch
//! - Session - stateful transaction support
//! - Strata - high-level typed facade behavior

mod common;

mod adversarial;
mod branch_invariants;
mod command_dispatch;
mod error_handling;
mod ex10_commands;
mod ex11_commands;
mod ex5_commands;
mod serialization;
mod session_transactions;
