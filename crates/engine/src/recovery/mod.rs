//! Recovery module for primitive recovery
//!
//! Contains the `Subsystem` trait for pluggable recovery and shutdown hooks.

mod subsystem;

pub use subsystem::Subsystem;
