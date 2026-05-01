//! Branch-bundle compatibility crate.
//!
//! The canonical lower durability runtime now lives in
//! `strata_storage::durability`.
//!
//! This crate remains only until `ST5`, when the branch-oriented
//! `branch_bundle` workflow is lifted into `strata-engine`.

pub mod branch_bundle;
