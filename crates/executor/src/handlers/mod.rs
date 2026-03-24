//! Command handlers organized by primitive category.
//!
//! Each submodule handles commands for a specific primitive:
//!
//! | Module | Commands | Primitive |
//! |--------|----------|-----------|
//! | `kv` | 15 | KVStore, KVStoreBatch |
//! | `json` | 17 | JsonStore |
//! | `event` | 11 | EventLog |
//! | `vector` | 19 | VectorStore |
//! | `branch` | 24 | BranchIndex |
//! | `transaction` | 5 | TransactionControl |
//! | `retention` | 3 | RetentionSubstrate |
//! | `database` | 4 | Database-level |

pub mod branch;
pub mod config;
pub mod configure_model;
pub mod database;
pub mod embed;
pub mod embed_hook;
pub mod event;
pub mod export;
pub mod generate;
pub mod graph;
pub mod json;
pub mod kv;
pub mod models;
pub mod search;
pub mod space;
pub mod vector;

use std::sync::Arc;

use crate::bridge::Primitives;
use crate::convert::convert_result;
use crate::types::BranchId;
use crate::{Error, Result};

/// Reject operations targeting reserved `_system` branches.
///
/// Any branch whose name starts with `_system` is reserved for internal use
/// and must not be directly accessed by user commands.
pub(crate) fn reject_system_branch(branch: &BranchId) -> Result<()> {
    if branch.as_str().starts_with("_system") {
        return Err(Error::InvalidInput {
            reason: format!("Branch '{}' is reserved for system use", branch.as_str()),
            hint: Some(
                "Branches starting with '_system' are internal and cannot be accessed directly."
                    .to_string(),
            ),
        });
    }
    Ok(())
}

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

/// Deterministic evenly-spaced sampling: pick `count` indices from `0..total`.
///
/// Returns indices spread evenly across the range for representative coverage.
pub(crate) fn sample_indices(total: usize, count: usize) -> Vec<usize> {
    if count >= total {
        return (0..total).collect();
    }
    if count == 0 {
        return vec![];
    }
    let step = total as f64 / count as f64;
    (0..count).map(|i| (i as f64 * step) as usize).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sample_indices_count_zero() {
        assert_eq!(sample_indices(10, 0), Vec::<usize>::new());
    }

    #[test]
    fn sample_indices_total_zero() {
        assert_eq!(sample_indices(0, 5), Vec::<usize>::new());
    }

    #[test]
    fn sample_indices_both_zero() {
        assert_eq!(sample_indices(0, 0), Vec::<usize>::new());
    }

    #[test]
    fn sample_indices_count_one() {
        assert_eq!(sample_indices(10, 1), vec![0]);
    }

    #[test]
    fn sample_indices_total_one_count_one() {
        assert_eq!(sample_indices(1, 1), vec![0]);
    }

    #[test]
    fn sample_indices_count_equals_total() {
        assert_eq!(sample_indices(5, 5), vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn sample_indices_count_exceeds_total() {
        // Should return all indices when count > total
        assert_eq!(sample_indices(3, 10), vec![0, 1, 2]);
    }

    #[test]
    fn sample_indices_even_spread() {
        // 10 items, pick 5 → should pick evenly spaced indices
        let indices = sample_indices(10, 5);
        assert_eq!(indices.len(), 5);
        // Each index should be in bounds
        for &idx in &indices {
            assert!(idx < 10);
        }
        // Indices should be monotonically increasing (sorted)
        for pair in indices.windows(2) {
            assert!(pair[0] < pair[1], "indices must be strictly increasing");
        }
    }

    #[test]
    fn sample_indices_two_from_large() {
        let indices = sample_indices(1000, 2);
        assert_eq!(indices.len(), 2);
        assert_eq!(indices[0], 0);
        assert_eq!(indices[1], 500);
    }

    #[test]
    fn sample_indices_no_duplicates() {
        let indices = sample_indices(100, 50);
        assert_eq!(indices.len(), 50);
        let unique: std::collections::HashSet<usize> = indices.iter().copied().collect();
        assert_eq!(unique.len(), 50, "all indices must be unique");
    }
}

// Transaction commands are deferred because the Executor is stateless by design.
// Transactions require session state management which would need additional design work.
//
// Retention commands (RetentionApply, RetentionStats, RetentionPreview) are deferred
// as they require additional infrastructure for garbage collection statistics.
//
// Database commands (Ping, Info, Flush, Compact) are implemented directly in executor.rs.
