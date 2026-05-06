//! Public command boundary for Strata.
//!
//! This crate defines the protocol and execution surface shared by Strata's
//! public frontends. It exposes:
//!
//! - command and output types
//! - executor-facing errors
//! - session-scoped execution handles
//! - a transport-neutral executor surface

#[cfg(feature = "arrow")]
mod arrow;
mod bridge;
mod command;
mod compat;
mod convert;
mod error;
mod executor;
mod handlers;
mod ipc;
mod output;
mod session;
mod suggest;
mod types;

pub use command::Command;
pub use compat::Strata;
pub use error::{Error, ErrorSeverity};
pub use executor::Executor;
pub use ipc::IpcServer;
pub use output::{EmbedStatusInfo, Output};
pub use session::Session;
pub use types::*;

pub use strata_core::Value;
pub use strata_engine::{
    apply_hardware_profile_if_defaults, apply_profile_if_defaults, detect_hardware, AccessMode,
    BranchDiffEntry, BranchDiffResult, CherryPickFilter, CherryPickInfo, ConflictEntry, DiffFilter,
    DiffOptions, DiffSummary, DurabilityMode, ForkInfo, HardwareInfo, MergeInfo, MergeStrategy,
    ModelConfig, OpenOptions, Profile, RevertInfo, SpaceDiff, StorageConfig, StorageIterator,
    StrataConfig, SystemMetrics, WalCounters,
};
pub use strata_storage::{Key, Namespace};

/// Result type for executor operations.
pub type Result<T> = std::result::Result<T, Error>;
