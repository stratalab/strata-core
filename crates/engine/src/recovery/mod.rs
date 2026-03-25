//! Recovery module for primitive recovery
//!
//! This module contains:
//! - `subsystem`: Subsystem trait for pluggable recovery and shutdown hooks
//! - `participant`: Legacy recovery participant registry (deprecated, use Subsystem trait)

mod participant;
mod subsystem;

pub use participant::{
    recover_all_participants, register_recovery_participant, RecoveryFn, RecoveryParticipant,
};
pub use subsystem::Subsystem;

#[cfg(test)]
pub use participant::{clear_recovery_registry, recovery_registry_count};
