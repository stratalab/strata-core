//! Command handlers organized by primitive category.
//!
//! Each submodule handles commands for a specific primitive:
//!
//! | Module | Commands | Primitive |
//! |--------|----------|-----------|
//! | `kv` | 15 | KVStore, KVStoreBatch |
//! | `json` | 17 | JsonStore |
//! | `event` | 11 | EventLog |
//! | `state` | 8 | StateCell |
//! | `vector` | 19 | VectorStore |
//! | `branch` | 24 | BranchIndex |
//! | `transaction` | 5 | TransactionControl |
//! | `retention` | 3 | RetentionSubstrate |
//! | `database` | 4 | Database-level |

pub mod branch;
pub mod config;
pub mod database;
pub mod configure_model;
pub mod embed;
pub mod embed_hook;
pub mod event;
pub mod generate;
pub mod graph;
pub mod json;
pub mod kv;
pub mod models;
pub mod search;
pub mod space;
pub mod state;
pub mod vector;

use std::sync::Arc;

use crate::bridge::Primitives;
use crate::convert::convert_result;
use crate::types::BranchId;
use crate::{Error, Result};

/// Validate that a branch exists before performing a write operation (#951).
///
/// The default branch is always allowed (it is implicit and not stored in BranchIndex).
/// For all other branches, checks `BranchIndex::exists()` and returns
/// `Error::BranchNotFound` with a "did you mean?" suggestion if the branch does not exist.
pub(crate) fn require_branch_exists(p: &Arc<Primitives>, branch: &BranchId) -> Result<()> {
    if branch.is_default() {
        return Ok(());
    }
    let exists = convert_result(p.branch.exists(branch.as_str()))?;
    if !exists {
        let name = branch.as_str().to_string();
        let branches = p.branch.list_branches().unwrap_or_default();
        let hint = crate::suggest::format_hint("branches", &branches, &name, 2);
        return Err(Error::BranchNotFound { branch: name, hint });
    }
    Ok(())
}

// Transaction commands are deferred because the Executor is stateless by design.
// Transactions require session state management which would need additional design work.
//
// Retention commands (RetentionApply, RetentionStats, RetentionPreview) are deferred
// as they require additional infrastructure for garbage collection statistics.
//
// Database commands (Ping, Info, Flush, Compact) are implemented directly in executor.rs.
