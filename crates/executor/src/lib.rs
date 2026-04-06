//! # Strata Executor
//!
//! The public API for StrataDB - an embedded database for AI applications.
//!
//! This is the only crate users need to import. It provides:
//! - [`Strata`] - The main database interface with data primitives
//! - [`Value`] - The universal value type for all data
//! - [`Command`]/[`Output`] - Low-level command interface (for SDKs)
//!
//! ## Quick Start
//!
//! ```text
//! use strata_executor::{Strata, Value};
//!
//! // Open a database
//! let mut db = Strata::open("/path/to/data")?;
//!
//! // Store data
//! db.kv_put("user:123", Value::String("Alice".into()))?;
//!
//! // Retrieve data
//! let value = db.kv_get("user:123")?;
//! ```
//!
//! ## Data Primitives
//!
//! | Primitive | Use Case |
//! |-----------|----------|
//! | **KV** | General key-value storage |
//! | **EventLog** | Immutable append-only streams |
//! | **JSONStore** | Structured documents with paths |
//! | **VectorStore** | Embeddings & similarity search |
//! | **Run** | Data isolation (like git branches) |
//!
//! ## Branch Context
//!
//! Data is isolated by "branches" (like git branches). Use `create_branch()` and `set_branch()`:
//!
//! ```text
//! db.create_branch("experiment-1")?;    // Create a new blank branch
//! db.set_branch("experiment-1")?;       // Switch to it
//! db.kv_put("key", Value::Int(42))?; // Data goes to experiment-1
//!
//! db.set_branch("default")?;            // Switch back
//! // Data from experiment-1 is not visible here
//! ```

#![warn(missing_docs)]

mod api;
pub(crate) mod bridge;
mod command;
mod convert;
mod error;
mod executor;
pub(crate) mod ipc;
pub(crate) mod json;
mod output;
mod session;
pub(crate) mod suggest;
mod types;

// Handler modules
mod handlers;

// Arrow interoperability layer (optional)
#[cfg(feature = "arrow")]
pub mod arrow;

// Test modules
#[cfg(test)]
mod tests;

// =============================================================================
// Public API - Everything users need is re-exported here
// =============================================================================

// Core types
pub use api::{
    BranchDiffEntry, BranchDiffResult, Branches, CherryPickFilter, CherryPickInfo, ConflictEntry,
    DiffFilter, DiffOptions, DiffSummary, ForkInfo, MergeInfo, MergeStrategy, RevertInfo,
    SpaceDiff, Strata, SystemBranch,
};
pub use command::Command;
pub use error::{Error, ErrorSeverity};
pub use executor::Executor;
pub use ipc::IpcServer;
pub use output::{EmbedStatusInfo, Output};
pub use session::Session;
pub use types::*;

// Re-export core types so users don't need strata-core directly
pub use strata_core::{Key, Namespace, Value};

// Re-export StorageIterator (return type of Strata::kv_iterator)
pub use strata_engine::StorageIterator;

// Re-export security types so users don't need strata-security directly
pub use strata_security::{AccessMode, OpenOptions};

// Re-export WAL counters (return type of Strata::durability_counters)
pub use strata_engine::WalCounters;

// Re-export configuration types so users don't need strata-engine directly
pub use strata_engine::{ModelConfig, StorageConfig, StrataConfig, SystemMetrics};

// Re-export hardware profile types so the CLI (and users) can detect + tune
// without depending on strata-engine directly.
pub use strata_engine::{
    apply_hardware_profile_if_defaults, apply_profile_if_defaults, detect_hardware, HardwareInfo,
    Profile,
};

// Re-export Database and DurabilityMode so users can open/create databases
// and create sessions without depending on strata-engine directly
pub use strata_engine::{Database, DurabilityMode};

/// Result type for executor operations
pub type Result<T> = std::result::Result<T, Error>;
