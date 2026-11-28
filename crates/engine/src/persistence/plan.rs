//! Engine-owned persistence commit plans.

use strata_core::BranchId;

use super::RowMutation;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CommitPlan {
    branch_id: BranchId,
    mutations: Vec<RowMutation>,
    expected_generation: Option<u64>,
}

impl CommitPlan {
    pub(crate) const fn new(
        branch_id: BranchId,
        mutations: Vec<RowMutation>,
        expected_generation: Option<u64>,
    ) -> Self {
        Self {
            branch_id,
            mutations,
            expected_generation,
        }
    }

    pub(crate) const fn branch_id(&self) -> BranchId {
        self.branch_id
    }

    pub(crate) fn mutations(&self) -> &[RowMutation] {
        &self.mutations
    }

    pub(crate) const fn expected_generation(&self) -> Option<u64> {
        self.expected_generation
    }
}
