//! Table-manifest recovery into branch-local state.

use super::{compaction, BranchLocalState};
use crate::branch::config::BranchRuntimeConfig;
use crate::branch::error::{BranchRuntimeError, BranchRuntimeResult};
use crate::branch::facts::InheritedLayerStatus;
use crate::branch::read::{
    BranchInheritedLayer, BranchLayout, BranchOwnedTable, BranchTimestampCoverage,
};
use std::collections::BTreeSet;
use std::sync::Arc;
use strata_core::{BranchId, CommitVersion, Timestamp};
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BranchTableManifestRecoveryRequest {
    branch_id: BranchId,
    owned_levels: Vec<Vec<BranchOwnedTable>>,
    inherited_layers: Vec<BranchInheritedLayer>,
    timestamp_coverage: BranchTimestampCoverage,
    /// #3502 Slice C: the retained-history version floor recovered from the
    /// manifest, re-applied onto branch state so post-restart `as_of` reads
    /// below a pruned floor keep raising (`None` = unbounded).
    retained_history_floor: Option<CommitVersion>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BranchTableManifestRecoveryOutcome {
    branch_id: BranchId,
    owned_table_count: usize,
    inherited_layer_count: usize,
    inherited_table_count: usize,
    max_commit_version: Option<CommitVersion>,
    timestamp_max: Option<Timestamp>,
}

impl BranchTableManifestRecoveryRequest {
    pub(crate) fn new(
        branch_id: BranchId,
        owned_levels: Vec<Vec<BranchOwnedTable>>,
        inherited_layers: Vec<BranchInheritedLayer>,
    ) -> BranchRuntimeResult<Self> {
        let request = Self {
            branch_id,
            owned_levels,
            inherited_layers,
            timestamp_coverage: BranchTimestampCoverage::unknown(),
            retained_history_floor: None,
        };
        request.validate()?;
        Ok(request)
    }

    pub(crate) fn with_timestamp_coverage(
        mut self,
        timestamp_coverage: BranchTimestampCoverage,
    ) -> Self {
        self.timestamp_coverage = timestamp_coverage;
        self
    }

    /// #3502 Slice C: carry the recovered retained-history version floor.
    pub(crate) fn with_retained_history_floor(mut self, floor: Option<CommitVersion>) -> Self {
        self.retained_history_floor = floor;
        self
    }

    pub(crate) const fn branch_id(&self) -> BranchId {
        self.branch_id
    }

    fn validate(&self) -> BranchRuntimeResult<()> {
        validate_manifest_recovery_inherited_layers(self.branch_id, &self.inherited_layers)?;
        validate_manifest_recovery_table_identities(&self.owned_levels, &self.inherited_layers)
    }

    fn validate_against_config(&self, config: BranchRuntimeConfig) -> BranchRuntimeResult<()> {
        if self.owned_levels.len() > config.max_level_count() {
            return Err(BranchRuntimeError::InvalidBranchState {
                reason: "table manifest recovery level count exceeds branch runtime configuration",
            });
        }
        if self.inherited_layers.len() > config.max_inherited_layers() {
            return Err(BranchRuntimeError::InvalidInheritedLayer {
                reason:
                    "table manifest recovery inherited layer count exceeds branch runtime configuration",
            });
        }
        Ok(())
    }
}

impl BranchTableManifestRecoveryOutcome {
    #[allow(
        dead_code,
        reason = "table-manifest recovery diagnostics are consumed by lifecycle tests"
    )]
    pub(crate) const fn branch_id(&self) -> BranchId {
        self.branch_id
    }

    #[allow(
        dead_code,
        reason = "table-manifest recovery diagnostics are consumed by lifecycle tests"
    )]
    pub(crate) const fn owned_table_count(&self) -> usize {
        self.owned_table_count
    }

    #[allow(
        dead_code,
        reason = "table-manifest recovery diagnostics are consumed by lifecycle tests"
    )]
    pub(crate) const fn inherited_layer_count(&self) -> usize {
        self.inherited_layer_count
    }

    #[allow(
        dead_code,
        reason = "table-manifest recovery diagnostics are consumed by lifecycle tests"
    )]
    pub(crate) const fn inherited_table_count(&self) -> usize {
        self.inherited_table_count
    }

    pub(crate) const fn total_table_count(&self) -> usize {
        self.owned_table_count + self.inherited_table_count
    }

    #[allow(
        dead_code,
        reason = "table-manifest recovery diagnostics are consumed by lifecycle tests"
    )]
    pub(crate) const fn max_commit_version(&self) -> Option<CommitVersion> {
        self.max_commit_version
    }

    #[allow(
        dead_code,
        reason = "table-manifest recovery diagnostics are consumed by lifecycle tests"
    )]
    pub(crate) const fn timestamp_max(&self) -> Option<Timestamp> {
        self.timestamp_max
    }
}

impl BranchLocalState {
    pub(crate) fn install_table_manifest_recovery(
        &mut self,
        request: BranchTableManifestRecoveryRequest,
    ) -> BranchRuntimeResult<BranchTableManifestRecoveryOutcome> {
        if request.branch_id() != self.branch_id {
            return Err(BranchRuntimeError::InvalidBranchState {
                reason: "table manifest recovery branch id must match branch state",
            });
        }
        if !self.is_empty() {
            return Err(BranchRuntimeError::InvalidBranchState {
                reason: "table manifest recovery requires an empty branch table state",
            });
        }
        request.validate_against_config(self.config)?;

        let mut staged = Self::new(self.branch_id, self.config)?;
        let mut layout = BranchLayout::from_levels(request.owned_levels);
        layout
            .levels_mut()
            .resize_with(self.config.max_level_count(), Vec::new);
        staged.layout = Arc::new(layout);
        staged
            .compact_pointers
            .resize_with(self.config.max_level_count(), || None);
        staged.inherited_layers = request.inherited_layers;
        staged.timestamp_coverage = request.timestamp_coverage;
        staged.retained_history_floor = request.retained_history_floor;
        compaction::validate_compaction_levels(staged.owned_levels())?;
        validate_manifest_recovery_inherited_layers(staged.branch_id, &staged.inherited_layers)?;
        validate_manifest_recovery_table_identities(
            staged.owned_levels(),
            &staged.inherited_layers,
        )?;
        staged.refresh_observed_row_facts();
        staged.reachability_snapshot()?;
        staged.validate_read_view_sources()?;

        let outcome = BranchTableManifestRecoveryOutcome {
            branch_id: staged.branch_id,
            owned_table_count: staged.owned_table_count(),
            inherited_layer_count: staged.inherited_layer_count(),
            inherited_table_count: staged.inherited_table_count(),
            max_commit_version: staged.max_commit_version,
            timestamp_max: staged.timestamp_max,
        };
        *self = staged;
        Ok(outcome)
    }
}

fn validate_manifest_recovery_table_identities(
    owned_levels: &[Vec<BranchOwnedTable>],
    inherited_layers: &[BranchInheritedLayer],
) -> BranchRuntimeResult<()> {
    let mut seen = BTreeSet::<&str>::new();
    for table in owned_levels.iter().flatten().chain(
        inherited_layers
            .iter()
            .flat_map(BranchInheritedLayer::owned_levels)
            .flatten(),
    ) {
        if !seen.insert(table.descriptor().identity().as_str()) {
            return Err(BranchRuntimeError::InvalidBranchState {
                reason: "table manifest recovery must not contain duplicate table identities",
            });
        }
    }
    Ok(())
}

fn validate_manifest_recovery_inherited_layers(
    branch_id: BranchId,
    layers: &[BranchInheritedLayer],
) -> BranchRuntimeResult<()> {
    let mut previous_fork_version = None::<CommitVersion>;
    let mut source_branches = BTreeSet::<[u8; BranchId::BYTE_LEN]>::new();
    for layer in layers {
        if layer.source_branch_id() == branch_id {
            return Err(BranchRuntimeError::InvalidInheritedLayer {
                reason: "table manifest inherited layer source branch must differ from branch",
            });
        }
        if !source_branches.insert(*layer.source_branch_id().as_bytes()) {
            return Err(BranchRuntimeError::InvalidInheritedLayer {
                reason: "table manifest inherited source branches must be unique",
            });
        }
        if previous_fork_version
            .is_some_and(|previous| layer.fork_version().as_u64() > previous.as_u64())
        {
            return Err(BranchRuntimeError::InvalidInheritedLayer {
                reason: "table manifest inherited layers must be nearest-first by fork version",
            });
        }
        previous_fork_version = Some(layer.fork_version());
        if layer.status() == InheritedLayerStatus::Unavailable {
            return Err(BranchRuntimeError::InvalidInheritedLayer {
                reason: "table manifest recovery cannot install unavailable inherited layers",
            });
        }
    }
    Ok(())
}
