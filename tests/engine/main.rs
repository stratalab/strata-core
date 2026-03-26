//! Engine Crate Integration Tests
//!
//! Tests for Database, 6 Primitives, and cross-cutting concerns.

#[path = "../common/mod.rs"]
mod common;

mod database;
mod primitives;

mod acid_concurrent;
mod acid_properties;
mod adversarial;
mod adversarial_deep;
mod branch_isolation;
mod cross_primitive;
mod p0_concurrency;
mod stress;
