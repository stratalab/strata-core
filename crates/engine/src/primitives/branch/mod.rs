//! Branch module for branch lifecycle management and handles
//!
//! This module contains:
//! - `index`: internal branch metadata/index implementation
//! - `handle`: BranchHandle facade for branch-scoped operations

mod handle;
mod index;

pub use handle::{BranchHandle, EventHandle, JsonHandle, KvHandle};
pub use index::{BranchMetadata, BranchStatus, resolve_branch_name};
pub(crate) use index::{BranchIndex, read_default_branch_marker, write_default_branch_marker};
