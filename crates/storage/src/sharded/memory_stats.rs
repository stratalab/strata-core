//! Memory usage statistics for the storage layer.

use strata_core::types::BranchId;

/// Memory usage statistics for the entire storage layer.
#[derive(Debug, Clone)]
pub struct StorageMemoryStats {
    /// Total number of branches
    pub total_branches: usize,
    /// Total number of entries across all branches
    pub total_entries: usize,
    /// Estimated total memory usage in bytes
    pub estimated_bytes: usize,
    /// Per-branch breakdown
    pub per_branch: Vec<BranchMemoryStats>,
}

/// Memory usage statistics for a single branch.
#[derive(Debug, Clone)]
pub struct BranchMemoryStats {
    /// Branch identifier
    pub branch_id: BranchId,
    /// Number of entries in this branch
    pub entry_count: usize,
    /// Whether the BTreeSet ordered-key index has been built
    pub has_btree_index: bool,
    /// Estimated memory usage in bytes
    pub estimated_bytes: usize,
}
