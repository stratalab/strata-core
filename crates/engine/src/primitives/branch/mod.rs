//! Branch module for branch lifecycle management and handles
//!
//! This module contains:
//! - `index`: internal branch metadata/index implementation
//! - `handle`: BranchHandle facade for branch-scoped operations

mod handle;
mod index;

pub use handle::{BranchHandle, BranchTransaction, EventHandle, JsonHandle, KvHandle};
pub(crate) use index::{
    aliases_default_branch_sentinel, default_branch_marker_key, read_default_branch_marker,
    validate_reserved_branch_aliases, write_default_branch_marker, BranchIndex,
    DeleteBranchCommitted, DeleteBranchCompletion, DeletePostCommitOptions,
};
pub use index::{resolve_branch_name, BranchMetadata, BranchStatus};
