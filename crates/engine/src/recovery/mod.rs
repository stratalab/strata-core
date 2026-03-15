//! Recovery module for primitive recovery
//!
//! This module contains:
//! - `participant`: Recovery participant registry for primitives with runtime state

mod participant;

pub use participant::{
    recover_all_participants, register_recovery_participant, RecoveryFn, RecoveryParticipant,
};

#[cfg(test)]
pub use participant::{clear_recovery_registry, recovery_registry_count};
