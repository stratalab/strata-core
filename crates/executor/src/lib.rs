//! # Strata Executor
//!
//! The public API for StrataDB - an embedded database for AI applications.
//!
//! This is the only crate users need to import. It provides:
//! - [`Strata`] - The main database interface with 6 data primitives
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
//! | **StateCell** | Mutable state with compare-and-swap |
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
pub(crate) mod suggest;
pub(crate) mod json;
mod output;
mod session;
mod types;

// Handler modules
mod handlers;

// Test modules
#[cfg(test)]
mod tests;

// =============================================================================
// Public API - Everything users need is re-exported here
// =============================================================================

// Core types
pub use api::{
    BranchDiffEntry, BranchDiffResult, Branches, ConflictEntry, DiffSummary, ForkInfo, MergeInfo,
    MergeStrategy, SpaceDiff, Strata,
};
pub use command::Command;
pub use error::Error;
pub use executor::Executor;
pub use output::{EmbedStatusInfo, Output};
pub use session::Session;
pub use types::*;

// Re-export Value from strata_core so users don't need to import it
pub use strata_core::Value;

// Re-export security types so users don't need strata-security directly
pub use strata_security::{AccessMode, OpenOptions};

// Re-export WAL counters (return type of Strata::durability_counters)
pub use strata_engine::WalCounters;

// Re-export configuration types so users don't need strata-engine directly
pub use strata_engine::{ModelConfig, StrataConfig};

// Re-export Database and DurabilityMode so users can open/create databases
// and create sessions without depending on strata-engine directly
pub use strata_engine::{Database, DurabilityMode};

/// Result type for executor operations
pub type Result<T> = std::result::Result<T, Error>;
