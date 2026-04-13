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
mod modes;
mod primitives;
mod recovery_cross_crate;
mod scale;
