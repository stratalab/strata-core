//! Fork and inherited-layer attachment helpers for branch-local state.

use super::BranchLocalState;
use crate::branch::error::{BranchRuntimeError, BranchRuntimeResult};
use crate::branch::facts::{InheritedLayerDescriptor, InheritedLayerStatus};
use crate::branch::read::{inherited_table_count, BranchInheritedLayer, BranchOwnedTable};
use std::collections::BTreeSet;
use strata_core::{BranchId, CommitVersion};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BranchForkOutcome {
    source_branch_id: BranchId,
    destination_branch_id: BranchId,
    fork_version: CommitVersion,
    inherited_layer_count: usize,
    inherited_table_count: usize,
}

impl BranchForkOutcome {
    pub(crate) const fn source_branch_id(self) -> BranchId {
        self.source_branch_id
    }

    pub(crate) const fn destination_branch_id(self) -> BranchId {
        self.destination_branch_id
    }

    pub(crate) const fn fork_version(self) -> CommitVersion {
        self.fork_version
    }

    pub(crate) const fn inherited_layer_count(self) -> usize {
        self.inherited_layer_count
    }

    pub(crate) const fn inherited_table_count(self) -> usize {
        self.inherited_table_count
    }
}

impl BranchLocalState {
    /// #3515: the retained-history floor a fork child inherits from its source.
    /// A child references (COW) or copies (materialized) the source's `<= V`
    /// tables, which the source may have pruned below its floor — so the child
    /// inherits that floor verbatim (pre-floor history is unavailable to it
    /// too). The one place this policy lives; a future rule (e.g. bounding by
    /// the fork version) changes here.
    pub(crate) const fn fork_child_retained_floor(
        source_floor: Option<CommitVersion>,
    ) -> Option<CommitVersion> {
        source_floor
    }

    pub(crate) fn attach_inherited_layers(
        &mut self,
        layers: Vec<BranchInheritedLayer>,
    ) -> BranchRuntimeResult<BranchForkOutcome> {
        self.validate_inherited_attach(&layers)?;
        let inherited_layer_count = layers.len();
        let inherited_table_count = inherited_table_count(&layers);
        self.inherited_layers = layers;
        self.refresh_observed_row_facts();
        Ok(BranchForkOutcome {
            source_branch_id: self
                .inherited_layers
                .first()
                .map_or(self.branch_id, BranchInheritedLayer::source_branch_id),
            destination_branch_id: self.branch_id,
            fork_version: self
                .inherited_layers
                .first()
                .map_or(CommitVersion::ZERO, BranchInheritedLayer::fork_version),
            inherited_layer_count,
            inherited_table_count,
        })
    }

    pub(crate) fn fork_into_empty_child(
        &self,
        destination_branch_id: BranchId,
    ) -> BranchRuntimeResult<(Self, BranchForkOutcome)> {
        if destination_branch_id == self.branch_id {
            return Err(BranchRuntimeError::InvalidInheritedLayer {
                reason: "fork source and destination branches must differ",
            });
        }
        if !self.active.is_empty() || !self.frozen.is_empty() {
            return Err(BranchRuntimeError::InvalidInheritedLayer {
                reason: "fork source must flush active and frozen rows before inheritance capture",
            });
        }
        let observed_rows = self.observe_rows();
        if observed_rows.max_commit_version.is_none() {
            return Err(BranchRuntimeError::InvalidInheritedLayer {
                reason: "fork source must contain at least one retained row",
            });
        }

        let fork_version = observed_rows
            .max_commit_version
            .expect("fork source retained-row precondition checked");
        let mut layers = Vec::with_capacity(self.inherited_layers.len() + 1);
        layers.push(BranchInheritedLayer::new(
            InheritedLayerDescriptor::new(
                self.branch_id,
                fork_version,
                InheritedLayerStatus::Active,
                self.owned_table_count(),
            ),
            self.owned_levels().to_vec(),
        )?);
        for layer in &self.inherited_layers {
            if let Some(layer) = layer.clone_active_for_fork()? {
                layers.push(layer);
            }
        }

        let mut child = Self::new(destination_branch_id, self.config)?;
        // #3515: the child inherits the source's tables, which the source may
        // have pruned below its retained-history floor — so the child inherits
        // that floor too. Without this a legal fork (at/above the floor) yields
        // a child that serves discarded pre-floor history and can fork
        // grandchildren below it (MVCC-009).
        child.set_retained_history_floor(Self::fork_child_retained_floor(
            self.retained_history_floor(),
        ));
        let attach_outcome = child.attach_inherited_layers(layers)?;
        let outcome = BranchForkOutcome {
            source_branch_id: self.branch_id,
            destination_branch_id,
            fork_version,
            inherited_layer_count: attach_outcome.inherited_layer_count(),
            inherited_table_count: attach_outcome.inherited_table_count(),
        };
        Ok((child, outcome))
    }

    /// Copy-on-write historical fork: build a child that references the source's owned tables at
    /// `fork_version = V` via a single straddle inherited layer, instead of materializing the source's
    /// `<= V` rows. The caller (`fork_at_retained_version`) gates this on the source having no `<= V`
    /// rows in active/frozen and no inherited layers of its own, so every `<= V` row is durable in an
    /// owned table and the layer's version-capped reads reproduce `source as_of V`. Unlike
    /// `fork_into_empty_child`, the source may hold `> V` active/frozen rows — they are outside the fork.
    pub(crate) fn fork_into_empty_child_at_version(
        &self,
        destination_branch_id: BranchId,
        fork_version: CommitVersion,
    ) -> BranchRuntimeResult<(Self, BranchForkOutcome)> {
        if destination_branch_id == self.branch_id {
            return Err(BranchRuntimeError::InvalidInheritedLayer {
                reason: "fork source and destination branches must differ",
            });
        }
        if !self.inherited_layers.is_empty() {
            return Err(BranchRuntimeError::InvalidInheritedLayer {
                reason: "copy-on-write historical fork requires a source with no inherited layers",
            });
        }
        // Reference only the owned tables that hold at least one `<= V` row. A table entirely above V
        // (`min > V`) has no in-fork rows — the child does not need it, and Slice 1's constructor would
        // reject it. Boundary (straddle) tables are admitted and version-capped at read time.
        let owned_levels: Vec<Vec<BranchOwnedTable>> = self
            .owned_levels()
            .iter()
            .map(|level| {
                level
                    .iter()
                    .filter(|table| {
                        table.facts().commit_range().min().as_u64() <= fork_version.as_u64()
                    })
                    .cloned()
                    .collect()
            })
            .collect();
        let owned_table_count: usize = owned_levels.iter().map(Vec::len).sum();
        if owned_table_count == 0 {
            return Err(BranchRuntimeError::InvalidInheritedLayer {
                reason: "fork source must contain at least one retained row at or before the fork version",
            });
        }
        let layer = BranchInheritedLayer::new(
            InheritedLayerDescriptor::new(
                self.branch_id,
                fork_version,
                InheritedLayerStatus::Active,
                owned_table_count,
            ),
            owned_levels,
        )?;
        let mut child = Self::new(destination_branch_id, self.config)?;
        // #3515: inherit the source's retained-history floor (see the twin in
        // `fork_into_empty_child`) — the COW layer references the source's
        // pruned tables, so pre-floor history is unavailable to the child too.
        child.set_retained_history_floor(Self::fork_child_retained_floor(
            self.retained_history_floor(),
        ));
        let attach_outcome = child.attach_inherited_layers(vec![layer])?;
        let outcome = BranchForkOutcome {
            source_branch_id: self.branch_id,
            destination_branch_id,
            fork_version,
            inherited_layer_count: attach_outcome.inherited_layer_count(),
            inherited_table_count: attach_outcome.inherited_table_count(),
        };
        Ok((child, outcome))
    }

    fn validate_inherited_attach(
        &self,
        layers: &[BranchInheritedLayer],
    ) -> BranchRuntimeResult<()> {
        if !self.inherited_layers.is_empty() {
            return Err(BranchRuntimeError::InvalidBranchState {
                reason: "branch already has inherited layers",
            });
        }
        if !self.active.is_empty() || !self.frozen.is_empty() || self.owned_table_count() != 0 {
            return Err(BranchRuntimeError::InvalidBranchState {
                reason: "inherited layers can only attach to an empty own branch state",
            });
        }
        if layers.is_empty() {
            return Err(BranchRuntimeError::InvalidInheritedLayer {
                reason: "inherited layer attach must include at least one layer",
            });
        }
        if layers.len() > self.config.max_inherited_layers() {
            return Err(BranchRuntimeError::InvalidInheritedLayer {
                reason: "inherited layer count exceeds branch runtime configuration",
            });
        }
        let mut previous_fork_version = None::<CommitVersion>;
        let mut source_branches = BTreeSet::<[u8; BranchId::BYTE_LEN]>::new();
        for layer in layers {
            if layer.source_branch_id() == self.branch_id {
                return Err(BranchRuntimeError::InvalidInheritedLayer {
                    reason: "inherited layer source branch must differ from child branch",
                });
            }
            if !source_branches.insert(*layer.source_branch_id().as_bytes()) {
                return Err(BranchRuntimeError::InvalidInheritedLayer {
                    reason: "inherited layer source branches must be unique",
                });
            }
            if previous_fork_version
                .is_some_and(|previous| layer.fork_version().as_u64() > previous.as_u64())
            {
                return Err(BranchRuntimeError::InvalidInheritedLayer {
                    reason: "inherited layers must be ordered nearest-first by fork version",
                });
            }
            previous_fork_version = Some(layer.fork_version());
            if layer.status() == InheritedLayerStatus::Unavailable {
                return Err(BranchRuntimeError::InvalidInheritedLayer {
                    reason: "unavailable inherited layers cannot attach",
                });
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::BranchLocalState;
    use strata_core::CommitVersion;

    /// #3515: a fork child inherits the source's retained-history floor
    /// verbatim (`None` stays unbounded; a floor carries through unchanged).
    #[test]
    fn fork_child_inherits_the_source_floor_verbatim() {
        assert_eq!(BranchLocalState::fork_child_retained_floor(None), None);
        assert_eq!(
            BranchLocalState::fork_child_retained_floor(Some(CommitVersion::new(7))),
            Some(CommitVersion::new(7))
        );
        assert_eq!(
            BranchLocalState::fork_child_retained_floor(Some(CommitVersion::ZERO)),
            Some(CommitVersion::ZERO)
        );
    }
}
