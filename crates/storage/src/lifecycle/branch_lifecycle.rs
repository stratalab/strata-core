//! Storage-internal branch lifecycle catalog.

use super::{LifecycleError, LifecycleLowerLayer, LifecycleResult};
use crate::branch::config::BranchRuntimeConfig;
use crate::branch::facts::{
    BranchReachabilityAggregate, BranchReachabilitySnapshot, BranchReleasePlan, BranchTableRef,
    BranchTableReferenceKind,
};
use crate::branch::read::{BranchReadView, BranchTimestampCoverage};

/// #2527: builds the hybrid COW fork's unsealed-rows table — one child L0
/// table over the source's unsealed `<= V` rows (already rewritten to the
/// child branch). `Ok(None)` declines the hybrid and falls back to the
/// eager materialization.
pub(crate) type ForkUnsealedTableBuilder<'a> =
    &'a mut dyn FnMut(
        strata_core::BranchId,
        strata_core::CommitVersion,
        Vec<crate::row::StorageRow>,
    ) -> LifecycleResult<Option<BranchOwnedTable>>;

/// #2855: whether a sealed table is durably cataloged — the COW fork's layer
/// premise. The durable runtime passes its table-catalog lookup; `None`
/// (cache mode, no recovery) trusts every table. A layer over a VOLATILE
/// table cannot be manifest-covered, which silently broke every downstream
/// durability contract (delete-guard exemption, stale-manifest removal,
/// recovery rebuild skip).
pub(crate) type ForkSealedTableDurability<'a> = &'a dyn Fn(&crate::table::TableIdentity) -> bool;
use crate::branch::read::BranchOwnedTable;
use crate::branch::state::snapshot::{
    install_snapshot_rows_into_branches, BranchSnapshotInstallRequest,
};
use crate::branch::state::BranchLocalState;
use crate::commit::{
    CommitBranchDescriptor, CommitBranchGeneration, CommitBranchGenerationGuard,
    CommitBranchRegistry, CommitBranchState, CommitRuntimeError,
};
use std::collections::{BTreeMap, BTreeSet};
use strata_core::{BranchId, CommitVersion, Timestamp};

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LifecycleBranchStatus {
    Active,
    Deleted,
}

/// Compile-time witness that a recovery-only catalog mutation is being
/// performed from inside the bootstrap path. Required by
/// [`LifecycleBranchCatalog::set_parent_for_recovery`] and any future
/// helper that mutates descriptor metadata outside the normal
/// generation-guarded surface. The constructor is private to the
/// `lifecycle::durable::bootstrap` module; non-recovery callers cannot
/// mint a token, so any cross-module misuse fails at compile time.
#[derive(Clone, Copy, Debug)]
pub(crate) struct RecoveryExclusivityToken {
    _private: (),
}

impl RecoveryExclusivityToken {
    /// Build a token. This constructor is `pub(super)` so only modules
    /// inside `crate::lifecycle` can call it; the source-guard test
    /// `recovery_exclusivity_token_is_minted_only_in_bootstrap` (see
    /// `tests/lifecycle_source_guard.rs`) tightens the policy further
    /// by rejecting any caller outside `lifecycle/durable/bootstrap.rs`.
    pub(super) const fn new() -> Self {
        Self { _private: () }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct LifecycleBranchParent {
    source_branch_id: BranchId,
    fork_version: CommitVersion,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct LifecycleBranchDescriptor {
    branch_id: BranchId,
    generation: CommitBranchGeneration,
    status: LifecycleBranchStatus,
    parent: Option<LifecycleBranchParent>,
    created_at: Option<CommitVersion>,
    deleted_at: Option<CommitVersion>,
    state_revision: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LifecycleBranchCreateOutcome {
    descriptor: LifecycleBranchDescriptor,
    branch_count: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LifecycleBranchForkOutcome {
    descriptor: LifecycleBranchDescriptor,
    source_branch_id: BranchId,
    fork_version: CommitVersion,
    inherited_layer_count: usize,
    inherited_table_count: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LifecycleBranchClearOutcome {
    descriptor: LifecycleBranchDescriptor,
    release_plan: BranchReleasePlan,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LifecycleBranchDeleteOutcome {
    descriptor: LifecycleBranchDescriptor,
    release_plan: BranchReleasePlan,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LifecyclePinnedBranchReachability {
    pin_id: u64,
    descriptor: LifecycleBranchDescriptor,
    snapshot: BranchReachabilitySnapshot,
}

#[derive(Clone, Debug)]
struct LifecycleBranchEntry {
    descriptor: LifecycleBranchDescriptor,
    state: Option<BranchLocalState>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct LifecyclePinnedBranchReachabilityRecord {
    pin_id: u64,
    descriptor: LifecycleBranchDescriptor,
    snapshot: BranchReachabilitySnapshot,
}

#[derive(Clone, Debug)]
pub(crate) struct LifecycleBranchCatalog {
    branch_config: BranchRuntimeConfig,
    entries: Vec<LifecycleBranchEntry>,
    registry: CommitBranchRegistry,
    pinned_snapshots: Vec<LifecyclePinnedBranchReachabilityRecord>,
    next_pin_id: u64,
    /// Branches whose state is temporarily checked OUT by ownership transfer
    /// (BS5.4 parallel cross-branch apply). The group leader holds the runtime
    /// mutex for the whole checkout window, so nothing else can observe the
    /// absence; this list turns any accidental same-thread access into a
    /// fail-closed "checked out" rejection instead of a misleading "deleted".
    checked_out: Vec<BranchId>,
}

impl LifecycleBranchParent {
    pub(crate) const fn new(source_branch_id: BranchId, fork_version: CommitVersion) -> Self {
        Self {
            source_branch_id,
            fork_version,
        }
    }

    pub(crate) const fn source_branch_id(self) -> BranchId {
        self.source_branch_id
    }

    pub(crate) const fn fork_version(self) -> CommitVersion {
        self.fork_version
    }
}

impl LifecycleBranchDescriptor {
    pub(crate) fn active(
        branch_id: BranchId,
        generation: CommitBranchGeneration,
        created_at: Option<CommitVersion>,
    ) -> Self {
        Self {
            branch_id,
            generation,
            status: LifecycleBranchStatus::Active,
            parent: None,
            created_at,
            deleted_at: None,
            state_revision: 0,
        }
    }

    fn with_status(mut self, status: LifecycleBranchStatus) -> Self {
        self.status = status;
        self
    }

    pub(crate) fn with_parent(mut self, parent: LifecycleBranchParent) -> Self {
        self.parent = Some(parent);
        self
    }

    pub(crate) fn with_deleted_at(mut self, deleted_at: Option<CommitVersion>) -> Self {
        self.deleted_at = deleted_at;
        self
    }

    fn with_next_revision(mut self) -> Self {
        self.state_revision = self.state_revision.saturating_add(1);
        self
    }

    pub(crate) const fn branch_id(self) -> BranchId {
        self.branch_id
    }

    pub(crate) const fn generation(self) -> CommitBranchGeneration {
        self.generation
    }

    pub(crate) const fn status(self) -> LifecycleBranchStatus {
        self.status
    }

    pub(crate) const fn parent(self) -> Option<LifecycleBranchParent> {
        self.parent
    }

    pub(crate) const fn created_at(self) -> Option<CommitVersion> {
        self.created_at
    }

    pub(crate) const fn with_created_at(mut self, created_at: Option<CommitVersion>) -> Self {
        self.created_at = created_at;
        self
    }

    pub(crate) const fn deleted_at(self) -> Option<CommitVersion> {
        self.deleted_at
    }

    pub(crate) const fn state_revision(self) -> u64 {
        self.state_revision
    }
}

impl LifecycleBranchCreateOutcome {
    pub(crate) const fn descriptor(&self) -> LifecycleBranchDescriptor {
        self.descriptor
    }

    pub(crate) const fn branch_count(&self) -> usize {
        self.branch_count
    }
}

impl LifecycleBranchForkOutcome {
    pub(crate) const fn descriptor(&self) -> LifecycleBranchDescriptor {
        self.descriptor
    }

    pub(crate) const fn source_branch_id(&self) -> BranchId {
        self.source_branch_id
    }

    pub(crate) const fn fork_version(&self) -> CommitVersion {
        self.fork_version
    }

    pub(crate) const fn inherited_layer_count(&self) -> usize {
        self.inherited_layer_count
    }

    pub(crate) const fn inherited_table_count(&self) -> usize {
        self.inherited_table_count
    }
}

impl LifecycleBranchClearOutcome {
    pub(crate) const fn descriptor(&self) -> LifecycleBranchDescriptor {
        self.descriptor
    }

    pub(crate) const fn release_plan(&self) -> &BranchReleasePlan {
        &self.release_plan
    }
}

impl LifecycleBranchDeleteOutcome {
    pub(crate) const fn descriptor(&self) -> LifecycleBranchDescriptor {
        self.descriptor
    }

    pub(crate) const fn release_plan(&self) -> &BranchReleasePlan {
        &self.release_plan
    }
}

impl LifecyclePinnedBranchReachability {
    pub(crate) const fn descriptor(&self) -> LifecycleBranchDescriptor {
        self.descriptor
    }

    pub(crate) fn table_refs(&self) -> &[BranchTableRef] {
        self.snapshot.table_refs()
    }
}

impl LifecycleBranchCatalog {
    pub(crate) fn new(branch_config: BranchRuntimeConfig) -> LifecycleResult<Self> {
        branch_config.validate().map_err(branch_error)?;
        Ok(Self {
            branch_config,
            entries: Vec::new(),
            registry: CommitBranchRegistry::new(),
            pinned_snapshots: Vec::new(),
            next_pin_id: 1,
            checked_out: Vec::new(),
        })
    }

    pub(crate) fn with_initial_branch(
        branch_config: BranchRuntimeConfig,
        branch_id: BranchId,
        generation: CommitBranchGeneration,
        created_at: Option<CommitVersion>,
    ) -> LifecycleResult<Self> {
        let mut catalog = Self::new(branch_config)?;
        catalog.create_branch(branch_id, generation, created_at)?;
        Ok(catalog)
    }

    pub(crate) fn with_existing_branch(
        state: &BranchLocalState,
        generation: CommitBranchGeneration,
        created_at: Option<CommitVersion>,
    ) -> LifecycleResult<Self> {
        let branch_config = state.config();
        let branch_id = state.branch_id();
        let mut catalog = Self::new(branch_config)?;
        catalog.create_branch(branch_id, generation, created_at)?;
        catalog.seed_active_branch_state(state)?;
        Ok(catalog)
    }

    pub(crate) fn create_branch(
        &mut self,
        branch_id: BranchId,
        generation: CommitBranchGeneration,
        created_at: Option<CommitVersion>,
    ) -> LifecycleResult<LifecycleBranchCreateOutcome> {
        match self.find_entry_index(branch_id) {
            Some(index)
                if self.entries[index].descriptor.status() == LifecycleBranchStatus::Deleted =>
            {
                self.recreate_deleted_branch(branch_id, generation, created_at)
            }
            Some(_) => Err(LifecycleError::BranchAlreadyExists { branch_id }),
            None => {
                let mut state =
                    BranchLocalState::new(branch_id, self.branch_config).map_err(branch_error)?;
                // W3.1b / #3502 D0: a branch born in-process has provably
                // complete (empty) timeline AND timestamp coverage —
                // checkpoints persist its retained index without a seeding
                // scan, and version pruning may attest its complete history.
                state.mark_complete_from_birth();
                let descriptor =
                    LifecycleBranchDescriptor::active(branch_id, generation, created_at);
                self.registry
                    .register_active(branch_id, generation)
                    .map_err(commit_error)?;
                self.entries.push(LifecycleBranchEntry {
                    descriptor,
                    state: Some(state),
                });
                self.sort_entries();
                Ok(LifecycleBranchCreateOutcome {
                    descriptor,
                    branch_count: self.entries.len(),
                })
            }
        }
    }

    /// Set the parent descriptor for a branch during recovery. The
    /// `replay_branch_catalog_manifest` path uses `create_branch` to
    /// install non-seeded entries, which initializes them without a
    /// parent; this helper attaches the parent metadata recovered from
    /// the `BranchCatalogManifest`. Production fork paths set parent on
    /// initial install via `install_new_branch_state`, so this method is
    /// recovery-only — enforced at compile time by the
    /// [`RecoveryExclusivityToken`] parameter (constructable only inside
    /// `lifecycle::durable::bootstrap`).
    pub(crate) fn set_parent_for_recovery(
        &mut self,
        branch_id: BranchId,
        parent: LifecycleBranchParent,
        _token: RecoveryExclusivityToken,
    ) -> LifecycleResult<()> {
        let index = self.entry_index(branch_id)?;
        self.entries[index].descriptor = self.entries[index].descriptor.with_parent(parent);
        Ok(())
    }

    /// #2826: adopt the durable manifest's `created_at` for the SEEDED
    /// branch during recovery. The shell registers the seeded branch before
    /// the manifest is read, so its creation point is unknown at that
    /// moment — and the WAL-replay generation fence depends on it (a
    /// deleted-and-recreated seeded branch must fence its predecessor's
    /// records like any other). Recovery-only, like `set_parent_for_recovery`.
    pub(crate) fn set_created_at_for_recovery(
        &mut self,
        branch_id: BranchId,
        created_at: Option<CommitVersion>,
        _token: RecoveryExclusivityToken,
    ) -> LifecycleResult<()> {
        let index = self.entry_index(branch_id)?;
        self.entries[index].descriptor = self.entries[index].descriptor.with_created_at(created_at);
        Ok(())
    }

    pub(crate) fn recreate_deleted_branch(
        &mut self,
        branch_id: BranchId,
        generation: CommitBranchGeneration,
        created_at: Option<CommitVersion>,
    ) -> LifecycleResult<LifecycleBranchCreateOutcome> {
        let index = self.entry_index(branch_id)?;
        let current = self.entries[index].descriptor;
        if current.status() != LifecycleBranchStatus::Deleted {
            return Err(LifecycleError::BranchAlreadyExists { branch_id });
        }
        if current.generation().get() == u64::MAX {
            return Err(LifecycleError::BranchGenerationExhausted {
                branch_id,
                generation: current.generation().get(),
            });
        }
        if generation <= current.generation() {
            return Err(LifecycleError::BranchGenerationMismatch {
                branch_id,
                expected: current.generation().get().saturating_add(1),
                actual: generation.get(),
            });
        }

        let mut state =
            BranchLocalState::new(branch_id, self.branch_config).map_err(branch_error)?;
        // W3.1b / #3502 D0: rebirth is an empty in-process state — complete
        // timeline AND timestamp coverage from birth.
        state.mark_complete_from_birth();
        let descriptor = LifecycleBranchDescriptor::active(branch_id, generation, created_at)
            .with_next_revision();
        self.registry
            .recreate_active(branch_id, generation)
            .map_err(commit_error)?;
        self.entries[index] = LifecycleBranchEntry {
            descriptor,
            state: Some(state),
        };
        self.sort_entries();
        Ok(LifecycleBranchCreateOutcome {
            descriptor,
            branch_count: self.entries.len(),
        })
    }

    /// Look up the live descriptor for a branch, including deleted entries.
    ///
    /// Callers use this to inspect non-mutating descriptor facts
    /// (`generation`, `status`, `created_at`, `deleted_at`, `parent`) without
    /// acquiring a branch-state borrow. Returns `BranchNotFound` if the
    /// `branch_id` has never been registered.
    #[allow(
        dead_code,
        reason = "general-purpose descriptor accessor; consumed once the deferred replay-safety slice ships"
    )]
    pub(crate) fn lookup_descriptor(
        &self,
        branch_id: BranchId,
    ) -> LifecycleResult<LifecycleBranchDescriptor> {
        let index = self.entry_index(branch_id)?;
        Ok(self.entries[index].descriptor)
    }

    pub(crate) fn list_branches(&self, include_deleted: bool) -> Vec<LifecycleBranchDescriptor> {
        let mut descriptors = self
            .entries
            .iter()
            .filter(|entry| {
                include_deleted || entry.descriptor.status() != LifecycleBranchStatus::Deleted
            })
            .map(|entry| entry.descriptor)
            .collect::<Vec<_>>();
        descriptors.sort_by_key(|descriptor| *descriptor.branch_id().as_bytes());
        descriptors
    }

    /// Build a deterministic snapshot of catalog descriptors for durable
    /// publication. Entries are sorted by `branch_id` byte order (mirroring
    /// the catalog's internal ordering). Storage-internal clear / delete
    /// are atomic synchronous transitions — only `Active` and `Deleted`
    /// are externally observable.
    pub(crate) fn durable_entries(
        &self,
    ) -> Result<Vec<crate::format::BranchCatalogEntry>, crate::format::FormatError> {
        use crate::format::{BranchCatalogEntry, BranchCatalogParent, BranchCatalogStatus};
        let mut entries = Vec::with_capacity(self.entries.len());
        for entry in &self.entries {
            let descriptor = entry.descriptor;
            let status = match descriptor.status() {
                LifecycleBranchStatus::Active => BranchCatalogStatus::Active,
                LifecycleBranchStatus::Deleted => BranchCatalogStatus::Deleted,
            };
            let mut durable = BranchCatalogEntry::new(
                descriptor.branch_id(),
                descriptor.generation().get(),
                status,
            )?
            .with_state_revision(descriptor.state_revision());
            if let Some(parent) = descriptor.parent() {
                durable = durable.with_parent(BranchCatalogParent::new(
                    parent.source_branch_id(),
                    parent.fork_version().as_u64(),
                ));
            }
            if let Some(created) = descriptor.created_at() {
                durable = durable.with_created_at(created.as_u64())?;
            }
            if let Some(deleted) = descriptor.deleted_at() {
                durable = durable.with_deleted_at(deleted.as_u64())?;
            }
            entries.push(durable);
        }
        Ok(entries)
    }

    pub(crate) fn lookup(&self, branch_id: BranchId) -> LifecycleResult<LifecycleBranchDescriptor> {
        Ok(self.entry(branch_id)?.descriptor)
    }

    /// Whether the branch's descriptor survives with status Deleted — the state a
    /// branch-scoped maintenance task's target is left in when a delete races the
    /// queue between enqueue and run. Absent or live entries are NOT "deleted":
    /// their errors keep their own semantics at the accessor that hits them.
    pub(crate) fn branch_is_deleted(&self, branch_id: BranchId) -> bool {
        self.lookup(branch_id)
            .is_ok_and(|descriptor| descriptor.status() == LifecycleBranchStatus::Deleted)
    }

    pub(crate) fn branch_state(&self, branch_id: BranchId) -> LifecycleResult<&BranchLocalState> {
        self.active_entry(branch_id)?
            .state
            .as_ref()
            .ok_or(LifecycleError::BranchNotWritable {
                branch_id,
                state: self.absent_state_reason(branch_id),
            })
    }

    /// Why a live entry's state slot is empty: checked out for the write
    /// group's parallel apply (BS5.4), or deleted.
    fn absent_state_reason(&self, branch_id: BranchId) -> &'static str {
        if self.checked_out.contains(&branch_id) {
            "checked out"
        } else {
            "deleted"
        }
    }

    /// BS5.4: check a branch's state OUT of the catalog by ownership transfer
    /// for the write group's parallel cross-branch apply. The caller (the
    /// group leader) holds the runtime mutex for the entire checkout window
    /// and MUST hand it back with [`check_in_branch_state`](Self::check_in_branch_state)
    /// before releasing it; while checked out, every accessor fails closed.
    pub(crate) fn take_branch_state(
        &mut self,
        branch_id: BranchId,
        generation_guard: CommitBranchGenerationGuard,
    ) -> LifecycleResult<BranchLocalState> {
        let index = self.active_entry_index(branch_id)?;
        let descriptor = self.entries[index].descriptor;
        require_generation(descriptor, generation_guard)?;
        self.advance_state_revision(index);
        let state =
            self.entries[index]
                .state
                .take()
                .ok_or_else(|| LifecycleError::BranchNotWritable {
                    branch_id,
                    state: self.absent_state_reason(branch_id),
                })?;
        self.checked_out.push(branch_id);
        Ok(state)
    }

    /// BS5.4: return a checked-out branch state (see
    /// [`take_branch_state`](Self::take_branch_state)).
    pub(crate) fn check_in_branch_state(&mut self, state: BranchLocalState) -> LifecycleResult<()> {
        let branch_id = state.branch_id();
        let Some(position) = self.checked_out.iter().position(|id| *id == branch_id) else {
            return Err(LifecycleError::InvalidLifecycleState {
                reason: "checked-in branch state was not checked out",
            });
        };
        let index = self.active_entry_index(branch_id)?;
        if self.entries[index].state.is_some() {
            return Err(LifecycleError::InvalidLifecycleState {
                reason: "checked-out branch state slot is unexpectedly occupied",
            });
        }
        self.entries[index].state = Some(state);
        self.checked_out.swap_remove(position);
        Ok(())
    }

    pub(crate) fn branch_state_mut(
        &mut self,
        branch_id: BranchId,
        generation_guard: CommitBranchGenerationGuard,
    ) -> LifecycleResult<&mut BranchLocalState> {
        let index = self.active_entry_index(branch_id)?;
        let descriptor = self.entries[index].descriptor;
        require_generation(descriptor, generation_guard)?;
        self.advance_state_revision(index);
        let absent = self.absent_state_reason(branch_id);
        self.entries[index]
            .state
            .as_mut()
            .ok_or(LifecycleError::BranchNotWritable {
                branch_id,
                state: absent,
            })
    }

    /// Split-borrow variant: returns `&mut BranchLocalState` and a
    /// concurrent immutable `&CommitBranchRegistry` reference, both
    /// borrowed from disjoint fields of the catalog. Used by the runtime
    /// commit path so the commit runtime can validate generation guards
    /// against the registry while holding a mutable branch state.
    pub(crate) fn branch_state_mut_with_registry(
        &mut self,
        branch_id: BranchId,
        generation_guard: CommitBranchGenerationGuard,
    ) -> LifecycleResult<(&mut BranchLocalState, &CommitBranchRegistry)> {
        let index = self.active_entry_index(branch_id)?;
        let descriptor = self.entries[index].descriptor;
        require_generation(descriptor, generation_guard)?;
        // Direct field access splits the borrows: `entries` is mutably
        // borrowed, `registry` is immutably borrowed, but the compiler
        // tracks them disjointly.
        let entries = &mut self.entries;
        let registry = &self.registry;
        let entry = &mut entries[index];
        entry.descriptor = entry.descriptor.with_next_revision();
        let branch = entry
            .state
            .as_mut()
            .ok_or(LifecycleError::BranchNotWritable {
                branch_id,
                state: "checked out or deleted",
            })?;
        Ok((branch, registry))
    }

    pub(crate) fn replace_active_branch_state(
        &mut self,
        branch_id: BranchId,
        generation_guard: CommitBranchGenerationGuard,
        state: BranchLocalState,
    ) -> LifecycleResult<BranchReachabilitySnapshot> {
        if state.branch_id() != branch_id {
            return Err(LifecycleError::BranchStateMismatch {
                expected: branch_id,
                actual: state.branch_id(),
            });
        }
        let index = self.active_entry_index(branch_id)?;
        require_generation(self.entries[index].descriptor, generation_guard)?;
        let snapshot = state.reachability_snapshot().map_err(branch_error)?;
        self.advance_state_revision(index);
        self.entries[index].state = Some(state);
        Ok(snapshot)
    }

    fn seed_active_branch_state(
        &mut self,
        state: &BranchLocalState,
    ) -> LifecycleResult<BranchReachabilitySnapshot> {
        let index = self.active_entry_index(state.branch_id())?;
        let snapshot = state.reachability_snapshot().map_err(branch_error)?;
        self.entries[index].state = Some(state.clone());
        Ok(snapshot)
    }

    pub(crate) fn replace_active_branch_state_with_descriptor(
        &mut self,
        expected_descriptor: LifecycleBranchDescriptor,
        state: BranchLocalState,
    ) -> LifecycleResult<BranchReachabilitySnapshot> {
        if state.branch_id() != expected_descriptor.branch_id() {
            return Err(LifecycleError::BranchStateMismatch {
                expected: expected_descriptor.branch_id(),
                actual: state.branch_id(),
            });
        }
        let index = self.active_entry_index(expected_descriptor.branch_id())?;
        if self.entries[index].descriptor != expected_descriptor {
            return Err(LifecycleError::BranchNotWritable {
                branch_id: expected_descriptor.branch_id(),
                state: "stale branch descriptor",
            });
        }
        let snapshot = state.reachability_snapshot().map_err(branch_error)?;
        self.advance_state_revision(index);
        self.entries[index].state = Some(state);
        Ok(snapshot)
    }

    pub(crate) fn capture_read_view(
        &mut self,
        branch_id: BranchId,
    ) -> LifecycleResult<BranchReadView> {
        let descriptor = self.active_entry(branch_id)?.descriptor;
        let snapshot = self
            .branch_state(branch_id)?
            .reachability_snapshot()
            .map_err(branch_error)?;
        let view = self
            .branch_state(branch_id)?
            .capture_read_view()
            .map_err(branch_error)?;
        self.pin_snapshot(descriptor, snapshot)?;
        Ok(view)
    }

    pub(crate) fn pin_reachability(
        &mut self,
        branch_id: BranchId,
    ) -> LifecycleResult<LifecyclePinnedBranchReachability> {
        let descriptor = self.active_entry(branch_id)?.descriptor;
        let snapshot = self
            .branch_state(branch_id)?
            .reachability_snapshot()
            .map_err(branch_error)?;
        self.pin_snapshot(descriptor, snapshot)
    }

    pub(crate) fn release_pinned_reachability(
        &mut self,
        pin: &LifecyclePinnedBranchReachability,
    ) -> bool {
        let before = self.pinned_snapshots.len();
        self.pinned_snapshots
            .retain(|record| record.pin_id != pin.pin_id);
        before != self.pinned_snapshots.len()
    }

    pub(crate) fn clear_branch(
        &mut self,
        branch_id: BranchId,
        generation_guard: CommitBranchGenerationGuard,
    ) -> LifecycleResult<LifecycleBranchClearOutcome> {
        let index = self.active_entry_index(branch_id)?;
        let descriptor = self.entries[index].descriptor;
        require_generation(descriptor, generation_guard)?;
        let old_snapshot = self.entries[index]
            .state
            .as_ref()
            .expect("active entry has state")
            .reachability_snapshot()
            .map_err(branch_error)?;
        let mut empty_state =
            BranchLocalState::new(branch_id, self.branch_config).map_err(branch_error)?;
        // W3.1b / #3502 D0: a cleared branch restarts with empty, complete
        // timeline AND timestamp coverage.
        empty_state.mark_complete_from_birth();
        let release_plan = self.release_plan_after_removing(branch_id, &old_snapshot)?;

        let active = descriptor.with_next_revision();
        self.entries[index].state = Some(empty_state);
        self.entries[index].descriptor = active;
        Ok(LifecycleBranchClearOutcome {
            descriptor: active,
            release_plan,
        })
    }

    /// #2820: whether any active fork child's RECOVERY depends on `branch_id`
    /// — layer-less (its inherited rows were eagerly copied, not layered) AND
    /// with non-empty fork-visible rows in this source. Deleting the source
    /// while such a child lives arms a permanent recovery failure
    /// (`rebuild_fork_snapshot_rows` re-materializes from the source), so the
    /// DURABLE runtime's live delete refuses on it — the branch-entry analog
    /// of the COW-001 segment refcount, scoped exactly to the rebuild's own
    /// conditions. Layered children (hybrid `fork_current`, #2527) ride
    /// durably published manifests and segment reachability, and empty forks
    /// re-materialize nothing: both keep their parent deletable. Cache mode
    /// has no recovery and replay re-applies history, so neither consults
    /// this. A checked-out state on either side is treated as dependent
    /// (fail closed).
    pub(crate) fn require_no_recovery_dependent_children(
        &self,
        branch_id: BranchId,
    ) -> LifecycleResult<()> {
        let source_state = self
            .find_entry_index(branch_id)
            .and_then(|index| self.entries[index].state.as_ref());
        for child in self.entries.iter().filter(|entry| {
            entry.descriptor.status() == LifecycleBranchStatus::Active
                && entry
                    .descriptor
                    .parent()
                    .is_some_and(|parent| parent.source_branch_id() == branch_id)
                && entry
                    .state
                    .as_ref()
                    .is_none_or(|state| state.inherited_layers().is_empty())
        }) {
            let depends = match (child.descriptor.parent(), source_state) {
                (Some(parent), Some(source_state)) => !source_state
                    .fork_snapshot_rows(parent.fork_version(), child.descriptor.branch_id())
                    .map_err(branch_error)?
                    .is_empty(),
                // Checked-out source state (or a parentless match, which the
                // filter forbids): fail closed.
                _ => true,
            };
            if depends {
                return Err(LifecycleError::BranchHasRecoveryDependentChildren { branch_id });
            }
        }
        Ok(())
    }

    pub(crate) fn delete_branch(
        &mut self,
        branch_id: BranchId,
        generation_guard: CommitBranchGenerationGuard,
        deleted_at: Option<CommitVersion>,
    ) -> LifecycleResult<LifecycleBranchDeleteOutcome> {
        let index = self.active_entry_index(branch_id)?;
        let descriptor = self.entries[index].descriptor;
        require_generation(descriptor, generation_guard)?;
        let old_snapshot = self.entries[index]
            .state
            .as_ref()
            .expect("active entry has state")
            .reachability_snapshot()
            .map_err(branch_error)?;
        let release_plan = self.release_plan_after_removing(branch_id, &old_snapshot)?;

        self.registry
            .mark_deleting(branch_id)
            .map_err(commit_error)?;
        let deleted = descriptor
            .with_status(LifecycleBranchStatus::Deleted)
            .with_deleted_at(deleted_at)
            .with_next_revision();
        self.registry
            .mark_deleted(branch_id)
            .map_err(commit_error)?;
        self.entries[index].descriptor = deleted;
        self.entries[index].state = None;
        Ok(LifecycleBranchDeleteOutcome {
            descriptor: deleted,
            release_plan,
        })
    }

    pub(crate) fn fork_current(
        &mut self,
        source_branch_id: BranchId,
        destination_branch_id: BranchId,
        destination_generation: CommitBranchGeneration,
        created_at: Option<CommitVersion>,
    ) -> LifecycleResult<LifecycleBranchForkOutcome> {
        self.require_destination_available(destination_branch_id, destination_generation)?;
        let source = self.branch_state(source_branch_id)?.clone();
        if source.active_row_count() > 0 || source.frozen_table_count() > 0 {
            return Err(LifecycleError::SourceHasUnflushedRows {
                branch_id: source_branch_id,
            });
        }
        let (child, fork_outcome) = source
            .fork_into_empty_child(destination_branch_id)
            .map_err(branch_error)?;
        Self::seed_child_timeline_from_parent(&source, &child, fork_outcome.fork_version());
        let parent = LifecycleBranchParent::new(source_branch_id, fork_outcome.fork_version());
        // #2826: `created_at` is the globally visible version when the fork
        // happened (the fork POINT lives in `parent.fork_version`). Recovery
        // fences dead-generation WAL records on it, so it must upper-bound
        // every version a deleted predecessor of this branch id could have
        // committed.
        let descriptor = LifecycleBranchDescriptor::active(
            destination_branch_id,
            destination_generation,
            created_at,
        )
        .with_parent(parent);
        self.install_new_branch_state(descriptor, child)?;
        Ok(LifecycleBranchForkOutcome {
            descriptor,
            source_branch_id,
            fork_version: fork_outcome.fork_version(),
            inherited_layer_count: fork_outcome.inherited_layer_count(),
            inherited_table_count: fork_outcome.inherited_table_count(),
        })
    }

    /// W3.1c: the child's retained timeline = the parent's history at the
    /// fork point (its own commits observe from here on). The parent's index
    /// is the era-independent source — post-elision there are no timeline
    /// rows to copy. EVERY fork path must seed: `fork_current` skipped this
    /// and a fork-at-head child failed closed on any pre-fork as-of read
    /// (#2522).
    ///
    /// An incomplete parent (mid-WAL-replay forks — replay re-executes the
    /// fork before the parent's index is re-completed) leaves the child
    /// INCOMPLETE. Completing here from the child's own scan would mark an
    /// empty index "complete from birth", permanently erasing the inherited
    /// pre-fork coverage; recovery's fork-derivation pass re-seeds
    /// incomplete children from the parent chain once the parents complete,
    /// and an incomplete child still resolves legacy pre-elision rows
    /// through the scan fallback.
    fn seed_child_timeline_from_parent(
        source: &BranchLocalState,
        child: &BranchLocalState,
        fork_version: CommitVersion,
    ) {
        if let Some(entries) = source.retained_timeline().snapshot_entries(fork_version) {
            child.retained_timeline().seed_from_scan(&entries);
        }
    }

    /// The eager fork fallback: materialize the source's whole `<= V` state
    /// into fresh child tables. O(dataset) — kept for fork-of-fork sources
    /// and sources with no sealed `<= V` table to reference (#2527 moved
    /// every other shape to the hybrid COW path).
    fn materialized_fork_child(
        source: &BranchLocalState,
        branch_config: BranchRuntimeConfig,
        destination_branch_id: BranchId,
        destination_generation: CommitBranchGeneration,
        fork_version: CommitVersion,
    ) -> LifecycleResult<BranchLocalState> {
        let rows = source
            .fork_snapshot_rows(fork_version, destination_branch_id)
            .map_err(branch_error)?;
        let mut states =
            vec![BranchLocalState::new(destination_branch_id, branch_config)
                .map_err(branch_error)?];
        if !rows.is_empty() {
            let request = BranchSnapshotInstallRequest::from_rows(
                format!(
                    "branch-lifecycle-fork-at-{}-{}-{}",
                    destination_branch_id,
                    destination_generation.get(),
                    fork_version.as_u64()
                ),
                rows,
            )
            .map_err(branch_error)?;
            install_snapshot_rows_into_branches(&mut states, &request).map_err(branch_error)?;
        }
        let mut child = states
            .into_iter()
            .next()
            .expect("destination state is always present");
        // #3515: a materialized fork copies only the source's `<= V` rows, which
        // the source may have pruned below its retained-history floor — so the
        // child inherits that floor (the COW twins do the same in `fork.rs`).
        child.set_retained_history_floor(BranchLocalState::fork_child_retained_floor(
            source.retained_history_floor(),
        ));
        Ok(child)
    }

    pub(crate) fn fork_at_retained_version(
        &mut self,
        source_branch_id: BranchId,
        destination_branch_id: BranchId,
        destination_generation: CommitBranchGeneration,
        fork_version: CommitVersion,
        retained_floor: CommitVersion,
        created_at: Option<CommitVersion>,
    ) -> LifecycleResult<LifecycleBranchForkOutcome> {
        self.fork_at_retained_version_with_unsealed_builder(
            source_branch_id,
            destination_branch_id,
            destination_generation,
            fork_version,
            retained_floor,
            created_at,
            None,
            None,
        )
    }

    /// #2527: the hybrid COW fork entry. `unsealed_table_builder` turns the
    /// source's unsealed `<= V` rows into ONE child L0 table (the durable
    /// runtime publishes a real table object so the fork-time child manifest
    /// covers it across restarts); returning `None` — or passing no builder —
    /// falls back to the eager whole-state materialization.
    /// #2855: `sealed_table_is_durable` gates COW eligibility — every in-fork
    /// sealed table must be durably cataloged, or the fork falls back to the
    /// eager path (whose layer-less child keeps the delete guard and the
    /// recovery rebuild sound).
    #[expect(
        clippy::too_many_arguments,
        reason = "the fork parameter set is fixed by the lifecycle contract; both trailing hooks are optional capabilities"
    )]
    pub(crate) fn fork_at_retained_version_with_unsealed_builder(
        &mut self,
        source_branch_id: BranchId,
        destination_branch_id: BranchId,
        destination_generation: CommitBranchGeneration,
        fork_version: CommitVersion,
        retained_floor: CommitVersion,
        created_at: Option<CommitVersion>,
        unsealed_table_builder: Option<ForkUnsealedTableBuilder<'_>>,
        sealed_table_is_durable: Option<ForkSealedTableDurability<'_>>,
    ) -> LifecycleResult<LifecycleBranchForkOutcome> {
        self.require_destination_available(destination_branch_id, destination_generation)?;
        if fork_version < retained_floor {
            return Err(LifecycleError::BranchHistoryUnavailable {
                branch_id: source_branch_id,
                reason: "requested fork version is below retained history",
            });
        }
        let source = self.branch_state(source_branch_id)?;
        let source_facts = source.facts().map_err(branch_error)?;
        // #2521: a rowless source forks only at version zero — the legitimate
        // empty-fork case. There is nothing to inherit, so the child is a
        // plain empty branch that keeps its parent linkage (the COW child
        // builders rightly refuse an inherited layer over zero rows); its
        // complete-from-birth timeline is exact because the parent has no
        // history either. Any other version has no rows to cover it.
        let visible = match source_facts.max_commit_version() {
            Some(visible) => visible,
            None if fork_version == CommitVersion::ZERO => {
                let child = BranchLocalState::new(destination_branch_id, self.branch_config)
                    .map_err(branch_error)?;
                // #3502 D0: an empty fork's timestamp coverage stays the fresh
                // default (Unknown, conservative) — an empty branch has no
                // versions to prune, so pruning-completeness is moot here.
                child.retained_timeline().mark_complete_from_birth();
                let parent = LifecycleBranchParent::new(source_branch_id, CommitVersion::ZERO);
                // #2826: even an empty fork stamps visible-at-fork — a
                // deleted predecessor of this branch id may have committed
                // rows the recovery fence must exclude. `None` only when the
                // store has no commits at all (the codec reserves zero), in
                // which case no predecessor row can exist either.
                let descriptor = LifecycleBranchDescriptor::active(
                    destination_branch_id,
                    destination_generation,
                    created_at,
                )
                .with_parent(parent);
                self.install_new_branch_state(descriptor, child)?;
                return Ok(LifecycleBranchForkOutcome {
                    descriptor,
                    source_branch_id,
                    fork_version: CommitVersion::ZERO,
                    inherited_layer_count: 0,
                    inherited_table_count: 0,
                });
            }
            None => {
                return Err(LifecycleError::BranchHistoryUnavailable {
                    branch_id: source_branch_id,
                    reason: "source branch has no retained rows",
                });
            }
        };
        if fork_version > visible {
            return Err(LifecycleError::BranchHistoryUnavailable {
                branch_id: source_branch_id,
                reason: "requested fork version is newer than visible branch version",
            });
        }

        // Historical-fork COW (Option A + #2527 hybrid): when the source owns no inherited layers,
        // build a copy-on-write child that references the source's owned tables at
        // `fork_version = V` instead of materializing the whole `<= V` state (O(dataset) reads and
        // a full duplicate — the #2527 seconds-scale `fork_current`). Unsealed `<= V` rows (active/
        // frozen — bounded by the rotation threshold, so `fork_current` almost always has some) ride
        // a single small L0 table the caller builds via `unsealed_table_builder`; it shadows the
        // layer, and its union with the layer is exactly the `<= V` state. The eager snapshot
        // install remains for: sources that are themselves forks, sources whose `<= V` rows are
        // entirely unsealed (no table to reference), and callers without a builder.
        let has_unsealed = source.has_in_fork_unsealed_rows(fork_version);
        let cow_structural = cow_fork_structural(source, fork_version, sealed_table_is_durable);
        let unsealed_table = match (cow_structural && has_unsealed, unsealed_table_builder) {
            (true, Some(builder)) => {
                let rows = source
                    .fork_unsealed_snapshot_rows(fork_version, destination_branch_id)
                    .map_err(branch_error)?;
                builder(destination_branch_id, fork_version, rows)?
            }
            _ => None,
        };
        let cow_eligible = cow_structural && (!has_unsealed || unsealed_table.is_some());
        let (child, inherited_layer_count, inherited_table_count) = if cow_eligible {
            let (mut child, outcome) = source
                .fork_into_empty_child_at_version(destination_branch_id, fork_version)
                .map_err(branch_error)?;
            if let Some(table) = unsealed_table {
                // Installed AFTER the layer attach (layers attach only to an
                // empty child); key-capped `<= V` rows shadow the layer.
                child.install_l0_table(table).map_err(branch_error)?;
            }
            (
                child,
                outcome.inherited_layer_count(),
                outcome.inherited_table_count(),
            )
        } else {
            let child = Self::materialized_fork_child(
                source,
                self.branch_config,
                destination_branch_id,
                destination_generation,
                fork_version,
            )?;
            (child, 0, 0)
        };
        Self::seed_child_timeline_from_parent(source, &child, fork_version);
        let parent = LifecycleBranchParent::new(source_branch_id, fork_version);
        // #2826: visible-at-fork, NOT the fork point (`parent.fork_version`
        // carries that) — the recovery generation fence depends on it.
        let descriptor = LifecycleBranchDescriptor::active(
            destination_branch_id,
            destination_generation,
            created_at,
        )
        .with_parent(parent);
        self.install_new_branch_state(descriptor, child)?;
        Ok(LifecycleBranchForkOutcome {
            descriptor,
            source_branch_id,
            fork_version,
            inherited_layer_count,
            inherited_table_count,
        })
    }

    pub(crate) fn fork_at_retained_timestamp(
        &mut self,
        source_branch_id: BranchId,
        destination_branch_id: BranchId,
        destination_generation: CommitBranchGeneration,
        timestamp: Timestamp,
        retained_floor: CommitVersion,
        created_at: Option<CommitVersion>,
    ) -> LifecycleResult<LifecycleBranchForkOutcome> {
        self.fork_at_retained_timestamp_with_unsealed_builder(
            source_branch_id,
            destination_branch_id,
            destination_generation,
            timestamp,
            retained_floor,
            created_at,
            None,
            None,
        )
    }

    /// #2527: see [`Self::fork_at_retained_version_with_unsealed_builder`].
    #[expect(
        clippy::too_many_arguments,
        reason = "the fork parameter set is fixed by the lifecycle contract; both trailing hooks are optional capabilities"
    )]
    pub(crate) fn fork_at_retained_timestamp_with_unsealed_builder(
        &mut self,
        source_branch_id: BranchId,
        destination_branch_id: BranchId,
        destination_generation: CommitBranchGeneration,
        timestamp: Timestamp,
        retained_floor: CommitVersion,
        created_at: Option<CommitVersion>,
        unsealed_table_builder: Option<ForkUnsealedTableBuilder<'_>>,
        sealed_table_is_durable: Option<ForkSealedTableDurability<'_>>,
    ) -> LifecycleResult<LifecycleBranchForkOutcome> {
        self.require_destination_available(destination_branch_id, destination_generation)?;
        let source = self.branch_state(source_branch_id)?;
        match source.timestamp_coverage() {
            BranchTimestampCoverage::Complete => {}
            BranchTimestampCoverage::CompleteSince { earliest_timestamp } => {
                if timestamp < earliest_timestamp {
                    return Err(LifecycleError::InsufficientTimestampHistory {
                        branch_id: source_branch_id,
                        reason: "requested timestamp is below retained timestamp coverage",
                    });
                }
            }
            BranchTimestampCoverage::Unknown => {
                return Err(LifecycleError::InsufficientTimestampHistory {
                    branch_id: source_branch_id,
                    reason: "branch has no retained timestamp coverage",
                });
            }
        }
        let resolved = source
            .resolve_timestamp_to_commit_version(timestamp)
            .map_err(branch_error)?
            .ok_or(LifecycleError::InsufficientTimestampHistory {
                branch_id: source_branch_id,
                reason: "branch has no rows at or before requested timestamp",
            })?;
        self.fork_at_retained_version_with_unsealed_builder(
            source_branch_id,
            destination_branch_id,
            destination_generation,
            resolved,
            retained_floor,
            created_at,
            unsealed_table_builder,
            sealed_table_is_durable,
        )
    }

    pub(crate) fn registry(&self) -> &CommitBranchRegistry {
        &self.registry
    }

    fn install_new_branch_state(
        &mut self,
        descriptor: LifecycleBranchDescriptor,
        state: BranchLocalState,
    ) -> LifecycleResult<()> {
        if state.branch_id() != descriptor.branch_id() {
            return Err(LifecycleError::BranchStateMismatch {
                expected: descriptor.branch_id(),
                actual: state.branch_id(),
            });
        }
        match self.find_entry_index(descriptor.branch_id()) {
            Some(index)
                if self.entries[index].descriptor.status() == LifecycleBranchStatus::Deleted =>
            {
                self.registry
                    .recreate_active(descriptor.branch_id(), descriptor.generation())
                    .map_err(commit_error)?;
                self.entries[index] = LifecycleBranchEntry {
                    descriptor,
                    state: Some(state),
                };
            }
            Some(_) => {
                return Err(LifecycleError::BranchAlreadyExists {
                    branch_id: descriptor.branch_id(),
                });
            }
            None => {
                self.registry
                    .register_active(descriptor.branch_id(), descriptor.generation())
                    .map_err(commit_error)?;
                self.entries.push(LifecycleBranchEntry {
                    descriptor,
                    state: Some(state),
                });
            }
        }
        self.sort_entries();
        Ok(())
    }

    fn require_destination_available(
        &self,
        destination_branch_id: BranchId,
        destination_generation: CommitBranchGeneration,
    ) -> LifecycleResult<()> {
        match self.entry(destination_branch_id) {
            Ok(entry) if entry.descriptor.status() == LifecycleBranchStatus::Deleted => {
                if entry.descriptor.generation().get() == u64::MAX {
                    return Err(LifecycleError::BranchGenerationExhausted {
                        branch_id: destination_branch_id,
                        generation: entry.descriptor.generation().get(),
                    });
                }
                if destination_generation <= entry.descriptor.generation() {
                    return Err(LifecycleError::BranchGenerationMismatch {
                        branch_id: destination_branch_id,
                        expected: entry.descriptor.generation().get().saturating_add(1),
                        actual: destination_generation.get(),
                    });
                }
                Ok(())
            }
            Ok(_) => Err(LifecycleError::BranchAlreadyExists {
                branch_id: destination_branch_id,
            }),
            Err(LifecycleError::BranchNotFound { .. }) => Ok(()),
            Err(error) => Err(error),
        }
    }

    fn release_plan_after_removing(
        &self,
        branch_id: BranchId,
        removed: &BranchReachabilitySnapshot,
    ) -> LifecycleResult<BranchReleasePlan> {
        let aggregate = self.aggregate_reachability_after_current_state(branch_id)?;
        BranchReleasePlan::from_removed_refs(
            branch_id,
            removed.table_refs().to_vec(),
            &aggregate,
            None,
        )
        .map_err(branch_error)
    }

    fn aggregate_reachability_after_current_state(
        &self,
        removed_branch_id: BranchId,
    ) -> LifecycleResult<BranchReachabilityAggregate> {
        let mut refs_by_branch =
            BTreeMap::<[u8; BranchId::BYTE_LEN], (BranchId, Vec<BranchTableRef>)>::new();
        for snapshot in &self.pinned_snapshots {
            append_pinned_snapshot_refs(&mut refs_by_branch, snapshot);
        }
        for entry in &self.entries {
            if entry.descriptor.status() == LifecycleBranchStatus::Active
                && entry.descriptor.branch_id() != removed_branch_id
            {
                if let Some(state) = &entry.state {
                    let snapshot = state.reachability_snapshot().map_err(branch_error)?;
                    append_snapshot_refs(&mut refs_by_branch, &snapshot);
                }
            }
        }
        let mut snapshots = Vec::with_capacity(refs_by_branch.len());
        for (branch_id, refs) in refs_by_branch.into_values() {
            snapshots.push(dedup_reachability_snapshot(branch_id, refs)?);
        }
        BranchReachabilityAggregate::from_snapshots(&snapshots).map_err(branch_error)
    }

    fn active_entry(&self, branch_id: BranchId) -> LifecycleResult<&LifecycleBranchEntry> {
        let entry = self.entry(branch_id)?;
        require_active_descriptor(entry.descriptor)?;
        Ok(entry)
    }

    fn active_entry_index(&self, branch_id: BranchId) -> LifecycleResult<usize> {
        let index = self.entry_index(branch_id)?;
        require_active_descriptor(self.entries[index].descriptor)?;
        Ok(index)
    }

    fn entry(&self, branch_id: BranchId) -> LifecycleResult<&LifecycleBranchEntry> {
        let index = self.entry_index(branch_id)?;
        Ok(&self.entries[index])
    }

    fn entry_index(&self, branch_id: BranchId) -> LifecycleResult<usize> {
        self.find_entry_index(branch_id)
            .ok_or(LifecycleError::BranchNotFound { branch_id })
    }

    fn find_entry_index(&self, branch_id: BranchId) -> Option<usize> {
        self.entries
            .iter()
            .position(|entry| entry.descriptor.branch_id() == branch_id)
    }

    fn sort_entries(&mut self) {
        self.entries
            .sort_by_key(|entry| *entry.descriptor.branch_id().as_bytes());
    }

    fn advance_state_revision(&mut self, index: usize) {
        self.entries[index].descriptor = self.entries[index].descriptor.with_next_revision();
    }

    fn pin_snapshot(
        &mut self,
        descriptor: LifecycleBranchDescriptor,
        snapshot: BranchReachabilitySnapshot,
    ) -> LifecycleResult<LifecyclePinnedBranchReachability> {
        let pin_id = self.next_pin_id;
        self.next_pin_id =
            self.next_pin_id
                .checked_add(1)
                .ok_or(LifecycleError::InvalidConfig {
                    field: "branch_reachability_pin",
                    reason: "pin id exhausted",
                })?;
        let record = LifecyclePinnedBranchReachabilityRecord {
            pin_id,
            descriptor,
            snapshot,
        };
        self.pinned_snapshots.push(record.clone());
        Ok(LifecyclePinnedBranchReachability {
            pin_id: record.pin_id,
            descriptor: record.descriptor,
            snapshot: record.snapshot,
        })
    }
}

fn append_snapshot_refs(
    refs_by_branch: &mut BTreeMap<[u8; BranchId::BYTE_LEN], (BranchId, Vec<BranchTableRef>)>,
    snapshot: &BranchReachabilitySnapshot,
) {
    refs_by_branch
        .entry(*snapshot.branch_id().as_bytes())
        .or_insert_with(|| (snapshot.branch_id(), Vec::new()))
        .1
        .extend(snapshot.table_refs().iter().cloned());
}

fn append_pinned_snapshot_refs(
    refs_by_branch: &mut BTreeMap<[u8; BranchId::BYTE_LEN], (BranchId, Vec<BranchTableRef>)>,
    record: &LifecyclePinnedBranchReachabilityRecord,
) {
    refs_by_branch
        .entry(*record.snapshot.branch_id().as_bytes())
        .or_insert_with(|| (record.snapshot.branch_id(), Vec::new()))
        .1
        .extend(record.snapshot.table_refs().iter().cloned());
}

fn dedup_reachability_snapshot(
    branch_id: BranchId,
    refs: Vec<BranchTableRef>,
) -> LifecycleResult<BranchReachabilitySnapshot> {
    let mut seen = BTreeSet::new();
    let mut unique_refs = Vec::with_capacity(refs.len());
    for table_ref in refs {
        if seen.insert(table_ref_dedup_key(&table_ref)) {
            unique_refs.push(table_ref);
        }
    }
    BranchReachabilitySnapshot::new(branch_id, unique_refs).map_err(branch_error)
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct LifecycleTableRefDedupKey {
    table_identity: String,
    reference_kind_rank: u8,
    owner_branch: [u8; BranchId::BYTE_LEN],
    table_branch: [u8; BranchId::BYTE_LEN],
    source_branch: [u8; BranchId::BYTE_LEN],
    fork_version: u64,
    layer_index: usize,
    level: u8,
    table_index: usize,
}

/// #2527/#2855: whether the source qualifies for the hybrid COW fork at
/// `fork_version` — no inherited layers, at least one in-fork sealed table
/// (any table with a `<= V` row participates), and every in-fork sealed
/// table durably cataloged. The durability premise is #2855's: a COW layer
/// over a VOLATILE table (an eager fork child's materialized table, a
/// checkpoint-recovered base) guarantees the child's fork-time manifest
/// publish fails ("volatile rewrite output without durable catalog entry"),
/// leaving a child that LOOKS layered without durable coverage — the eager
/// path handles those sources instead. `None` (cache mode, no recovery)
/// trusts every table.
fn cow_fork_structural(
    source: &BranchLocalState,
    fork_version: CommitVersion,
    sealed_table_is_durable: Option<ForkSealedTableDurability<'_>>,
) -> bool {
    if !source.inherited_layers().is_empty() {
        return false;
    }
    let in_fork_sealed: Vec<&BranchOwnedTable> = source
        .owned_levels()
        .iter()
        .flatten()
        .filter(|table| table.facts().commit_range().min().as_u64() <= fork_version.as_u64())
        .collect();
    if in_fork_sealed.is_empty() {
        return false;
    }
    sealed_table_is_durable.is_none_or(|is_durable| {
        in_fork_sealed
            .iter()
            .all(|table| is_durable(table.descriptor().identity()))
    })
}

fn table_ref_dedup_key(table_ref: &BranchTableRef) -> LifecycleTableRefDedupKey {
    let mut source_branch = [0; BranchId::BYTE_LEN];
    let mut fork_version = 0;
    let mut layer_index = 0;
    let reference_kind_rank = match table_ref.reference_kind() {
        BranchTableReferenceKind::Owned => 0,
        BranchTableReferenceKind::Replacement {
            source_branch_id,
            fork_version: version,
        } => {
            source_branch = *source_branch_id.as_bytes();
            fork_version = version.as_u64();
            1
        }
        BranchTableReferenceKind::Inherited {
            source_branch_id,
            fork_version: version,
            layer_index: index,
        } => {
            source_branch = *source_branch_id.as_bytes();
            fork_version = version.as_u64();
            layer_index = index;
            2
        }
        BranchTableReferenceKind::MaterializingSource {
            source_branch_id,
            fork_version: version,
            layer_index: index,
        } => {
            source_branch = *source_branch_id.as_bytes();
            fork_version = version.as_u64();
            layer_index = index;
            3
        }
    };
    LifecycleTableRefDedupKey {
        table_identity: table_ref.table_identity().as_str().to_owned(),
        reference_kind_rank,
        owner_branch: *table_ref.owner_branch_id().as_bytes(),
        table_branch: *table_ref.table_branch_id().as_bytes(),
        source_branch,
        fork_version,
        layer_index,
        level: table_ref.level().raw(),
        table_index: table_ref.table_index(),
    }
}

fn require_active_descriptor(descriptor: LifecycleBranchDescriptor) -> LifecycleResult<()> {
    if descriptor.status() == LifecycleBranchStatus::Active {
        return Ok(());
    }
    Err(LifecycleError::BranchNotWritable {
        branch_id: descriptor.branch_id(),
        state: descriptor.status().name(),
    })
}

fn require_generation(
    descriptor: LifecycleBranchDescriptor,
    guard: CommitBranchGenerationGuard,
) -> LifecycleResult<()> {
    match guard {
        // sync_active_branch_state is the only legitimate sync-without-guard path;
        // every other catalog mutation must thread a typed generation guard so
        // stale queued work cannot race with create/clear/delete/fork.
        CommitBranchGenerationGuard::NotSupplied => Err(LifecycleError::BranchGenerationMismatch {
            branch_id: descriptor.branch_id(),
            expected: descriptor.generation().get(),
            actual: 0,
        }),
        CommitBranchGenerationGuard::Exact(actual) if actual == descriptor.generation() => Ok(()),
        CommitBranchGenerationGuard::Exact(actual) => {
            Err(LifecycleError::BranchGenerationMismatch {
                branch_id: descriptor.branch_id(),
                expected: descriptor.generation().get(),
                actual: actual.get(),
            })
        }
    }
}

impl LifecycleBranchStatus {
    const fn name(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Deleted => "deleted",
        }
    }
}

fn branch_error(error: impl std::error::Error + Send + Sync + 'static) -> LifecycleError {
    LifecycleError::lower_layer_with(
        LifecycleLowerLayer::BranchRuntime,
        "branch lifecycle branch runtime failed",
        error,
    )
}

fn commit_error(error: CommitRuntimeError) -> LifecycleError {
    match error {
        CommitRuntimeError::BranchAlreadyExists { branch_id } => {
            LifecycleError::BranchAlreadyExists { branch_id }
        }
        CommitRuntimeError::BranchNotFound { branch_id } => {
            LifecycleError::BranchNotFound { branch_id }
        }
        CommitRuntimeError::BranchGenerationMismatch {
            branch_id,
            expected,
            actual,
        } => LifecycleError::BranchGenerationMismatch {
            branch_id,
            expected,
            actual,
        },
        CommitRuntimeError::BranchGenerationExhausted {
            branch_id,
            generation,
        } => LifecycleError::BranchGenerationExhausted {
            branch_id,
            generation,
        },
        CommitRuntimeError::BranchNotWritable { branch_id, reason } => {
            LifecycleError::BranchNotWritable {
                branch_id,
                state: reason,
            }
        }
        other => LifecycleError::lower_layer_with(
            LifecycleLowerLayer::CommitRuntime,
            "branch lifecycle commit registry failed",
            other,
        ),
    }
}

impl From<LifecycleBranchDescriptor> for CommitBranchDescriptor {
    fn from(descriptor: LifecycleBranchDescriptor) -> Self {
        let state = match descriptor.status() {
            LifecycleBranchStatus::Active => CommitBranchState::Active,
            LifecycleBranchStatus::Deleted => CommitBranchState::Deleted,
        };
        Self::new(descriptor.branch_id(), descriptor.generation(), state)
    }
}
