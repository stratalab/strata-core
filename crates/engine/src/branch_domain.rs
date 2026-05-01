//! Branch lifecycle, DAG, and replay metadata used by the engine.

mod branch;
mod branch_dag;
mod branch_types;

pub use branch::{
    aliases_default_branch_sentinel, BranchControlRecord, BranchGeneration, BranchLifecycleStatus,
    BranchRef, ForkAnchor,
};
pub use branch_dag::{
    is_system_branch, CherryPickRecord, DagBranchInfo, DagBranchStatus, DagEventId, ForkRecord,
    MergeRecord, RevertRecord, BRANCH_DAG_GRAPH, SYSTEM_BRANCH,
};
pub use branch_types::{BranchEventOffsets, BranchMetadata, BranchStatus};
