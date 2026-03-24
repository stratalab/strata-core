//! Branch module for branch lifecycle management and handles
//!
//! This module contains:
//! - `index`: BranchIndex for creating, deleting, and managing branches
//! - `handle`: BranchHandle facade for branch-scoped operations

mod handle;
mod index;

pub use handle::{BranchHandle, EventHandle, JsonHandle, KvHandle};
pub use index::{resolve_branch_name, BranchIndex, BranchMetadata, BranchStatus};
