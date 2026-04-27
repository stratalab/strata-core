//! Branch lifecycle, DAG, and replay metadata used by the engine.

pub use strata_core::branch::{
    aliases_default_branch_sentinel, BranchControlRecord, BranchGeneration, BranchLifecycleStatus,
    BranchRef, ForkAnchor,
};
pub use strata_core::branch_dag::{
    is_system_branch, CherryPickRecord, DagBranchInfo, DagBranchStatus, DagEventId, ForkRecord,
    MergeRecord, RevertRecord, BRANCH_DAG_GRAPH, SYSTEM_BRANCH,
};
pub use strata_core::branch_types::{BranchEventOffsets, BranchMetadata, BranchStatus};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn branch_domain_constants_and_helpers_match_legacy_core() {
        assert_eq!(SYSTEM_BRANCH, strata_core::branch_dag::SYSTEM_BRANCH);
        assert_eq!(BRANCH_DAG_GRAPH, strata_core::branch_dag::BRANCH_DAG_GRAPH);
        assert!(is_system_branch("_system_meta"));
        assert!(!is_system_branch("default"));
        assert!(aliases_default_branch_sentinel(
            "00000000-0000-0000-0000-000000000000"
        ));
    }

    #[test]
    fn branch_domain_types_serialize_through_engine_surface() {
        let metadata = BranchMetadata::new(
            strata_core::BranchId::from_user_name("main"),
            strata_core::Timestamp::from(11),
            7,
        );
        let roundtrip: BranchMetadata =
            serde_json::from_str(&serde_json::to_string(&metadata).unwrap()).unwrap();
        assert_eq!(roundtrip.branch_id, metadata.branch_id);
        assert_eq!(roundtrip.begin_wal_offset, 7);

        let info = DagBranchInfo {
            name: "feature".into(),
            status: DagBranchStatus::Active,
            created_at: Some(10),
            updated_at: Some(12),
            message: Some("branch".into()),
            creator: Some("tester".into()),
            forked_from: None,
            merges: Vec::new(),
            children: vec!["feature/child".into()],
        };
        let info_roundtrip: DagBranchInfo =
            serde_json::from_str(&serde_json::to_string(&info).unwrap()).unwrap();
        assert_eq!(info_roundtrip.name, "feature");
    }
}
