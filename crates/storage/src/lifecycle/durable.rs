//! Durable-local lifecycle service assembly.

use super::{
    branch_config_with_storage_budget, stage_table_manifest_for_branch,
    table_block_cache_from_storage_budget, validate_backend_capabilities_for_open,
    LifecycleCapabilityOutcome, LifecycleDurableTableCatalog, LifecycleError, LifecycleLowerLayer,
    LifecycleOperationKind, LifecycleResult, LifecycleState, LifecycleStateMachine,
    LifecycleTableManifestRecoveryStage, LifecycleTransitionTrigger, RecoveryStrictness,
    StorageBudgetLedger, StorageMode, StorageOpenDisposition, StorageOpenPlan,
};
use crate::backend::{
    Backend, BackendError, BackendErrorKind, BackendHandle, BackendWriterGuard, PublishFailureKind,
};
use crate::branch::config::BranchRuntimeConfig;
use crate::branch::state::BranchLocalState;
use crate::commit::{
    CommitBranchGeneration, CommitBranchGuardSet, CommitBranchRegistry, CommitFactAllocator,
    CommitManualTimestampSource, CommitRuntimeConfig, CommitRuntimeError, CommitTimestampGuard,
    CommitUnresolvedDurable, CommitUnresolvedDurableGate, CommitVersionAllocator,
    VisibleVersionTracker,
};
use crate::config::mode::DurabilityPolicy;
use crate::format::DatabaseManifest;
use crate::layout::{LayoutError, ObjectFamily, ObjectLayout};
use crate::object::ObjectName;
use crate::service::{
    verify_wal_segment_inventory, wal_segments_present, BranchCatalogManifestService,
    CheckpointService, DatabaseManifestService, ManifestRole, ManifestServiceError,
    PendingReleasesManifestService, QuarantineService, SnapshotService, TableManifestService,
    TableObjectReaderService, TableObjectService, WalSegmentMetadataSidecarService, WalService,
    WalServiceConfig, WalServiceError,
};
use std::fmt;
use strata_core::{BranchId, CommitVersion};

mod bootstrap;
mod close;
mod inflight;
mod maintenance;

pub(crate) use inflight::{InFlightOutputsGuard, InFlightTableOutputs};

pub(crate) use bootstrap::{
    combine_non_seeded_checkpoint_rows, descriptor_version_anchor,
    parentless_content_predates_generation, record_predates_current_generation,
    DurableGroupApplyDone, DurableGroupApplyWork, DurableGroupInFlight, DurableGroupMemberResult,
    LifecycleDurableLocalRuntime, LifecycleRecoveryBootstrapReport,
};
pub(crate) use maintenance::{
    DurableBackgroundMaintenanceBuild, DurableBackgroundMaintenanceBuilt,
    DurableBackgroundMaintenanceStep, PreparedPublishStep,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LifecycleDurableLocalOpenRequest {
    plan: StorageOpenPlan,
    database_id: [u8; 16],
    initial_branch_id: BranchId,
    branch_generation: CommitBranchGeneration,
    branch_config: BranchRuntimeConfig,
    commit_config: CommitRuntimeConfig,
    wal_config: WalServiceConfig,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LifecycleDurableAssemblyFacts {
    mode: StorageMode,
    disposition: StorageOpenDisposition,
    database_id: [u8; 16],
    codec_id: String,
    durability_policy: DurabilityPolicy,
    active_wal_segment: u64,
    writer_lock_object: ObjectName,
    manifest_snapshot_watermark: Option<u64>,
    manifest_snapshot_id: Option<u64>,
    manifest_flush_watermark: Option<CommitVersion>,
    /// #2777: the checkpoint-attested WAL chain was absent at open and lossy
    /// assembly recreated a fresh log — recovery must record the loss.
    wal_chain_missing_at_open: bool,
}

pub(crate) struct LifecycleDurableLocalServices<'a> {
    capability_outcome: LifecycleCapabilityOutcome,
    manifest: DatabaseManifestService<'a>,
    table_manifest: TableManifestService<'a>,
    branch_catalog_manifest: BranchCatalogManifestService<'a>,
    pending_releases_manifest: PendingReleasesManifestService<'a>,
    wal: WalService<'a>,
    wal_sidecar: WalSegmentMetadataSidecarService<'a>,
    snapshot: SnapshotService<'a>,
    table_object: TableObjectService<'static>,
    checkpoint: CheckpointService<'a>,
    // 'static like `table_object`: sweep/purge staging clones this service
    // into off-lock steps (BS5.5) — the backend handle is owned.
    quarantine: QuarantineService<'static>,
    assembly_facts: LifecycleDurableAssemblyFacts,
    writer_guard: Option<BackendWriterGuard>,
}

pub(crate) struct LifecycleDurableLocalShell<'a, S = CommitManualTimestampSource> {
    state: LifecycleStateMachine,
    open_plan: StorageOpenPlan,
    services: LifecycleDurableLocalServices<'a>,
    branch: BranchLocalState,
    registry: CommitBranchRegistry,
    guard_set: CommitBranchGuardSet,
    allocator: CommitFactAllocator<S>,
    visible: VisibleVersionTracker,
    durable_gate: CommitUnresolvedDurableGate,
    commit_config: CommitRuntimeConfig,
    table_catalog: LifecycleDurableTableCatalog,
    budget: StorageBudgetLedger,
}

impl LifecycleDurableLocalOpenRequest {
    pub(crate) fn new(
        plan: StorageOpenPlan,
        database_id: [u8; 16],
        initial_branch_id: BranchId,
        branch_generation: CommitBranchGeneration,
        branch_config: BranchRuntimeConfig,
        commit_config: CommitRuntimeConfig,
        wal_config: WalServiceConfig,
    ) -> LifecycleResult<Self> {
        let request = Self {
            plan,
            database_id,
            initial_branch_id,
            branch_generation,
            branch_config,
            commit_config,
            wal_config,
        };
        request.validate()?;
        Ok(request)
    }

    pub(crate) fn plan(&self) -> &StorageOpenPlan {
        &self.plan
    }

    pub(crate) const fn database_id(&self) -> [u8; 16] {
        self.database_id
    }

    pub(crate) const fn initial_branch_id(&self) -> BranchId {
        self.initial_branch_id
    }

    pub(crate) const fn branch_generation(&self) -> CommitBranchGeneration {
        self.branch_generation
    }

    pub(crate) const fn branch_config(&self) -> BranchRuntimeConfig {
        self.branch_config
    }

    pub(crate) const fn commit_config(&self) -> CommitRuntimeConfig {
        self.commit_config
    }

    pub(crate) const fn wal_config(&self) -> WalServiceConfig {
        self.wal_config
    }

    fn validate(&self) -> LifecycleResult<()> {
        self.plan.validate()?;
        durable_policy_for_mode(self.plan.storage_mode())?;
        if self.plan.codec_id().as_str() != self.wal_config.codec_id() {
            return Err(LifecycleError::InvalidOpenPlan {
                reason: "durable open plan codec must match WAL codec",
            });
        }
        self.branch_config.validate().map_err(branch_error)?;
        self.commit_config.validate().map_err(commit_error)?;
        self.wal_config.validate().map_err(wal_error)?;
        Ok(())
    }
}

impl LifecycleDurableAssemblyFacts {
    fn new(
        mode: StorageMode,
        disposition: StorageOpenDisposition,
        manifest: &DatabaseManifest,
        durability_policy: DurabilityPolicy,
        writer_lock_object: ObjectName,
        wal_chain_missing_at_open: bool,
    ) -> Self {
        Self {
            mode,
            disposition,
            database_id: *manifest.database_id(),
            codec_id: manifest.codec_id().to_owned(),
            durability_policy,
            // The MANIFEST pointer, persisted at the last published checkpoint.
            // It may lag the writer's resumed segment: `WalService::open`
            // reconciles against the on-disk tail (#2555), and the live value
            // is `services.wal().active_segment_id()`.
            active_wal_segment: manifest.active_wal_segment(),
            writer_lock_object,
            manifest_snapshot_watermark: manifest.snapshot_watermark(),
            manifest_snapshot_id: manifest.snapshot_id(),
            manifest_flush_watermark: manifest.flushed_through_commit_id(),
            wal_chain_missing_at_open,
        }
    }

    pub(crate) const fn wal_chain_missing_at_open(&self) -> bool {
        self.wal_chain_missing_at_open
    }

    pub(crate) const fn mode(&self) -> StorageMode {
        self.mode
    }

    pub(crate) const fn disposition(&self) -> StorageOpenDisposition {
        self.disposition
    }

    pub(crate) const fn database_id(&self) -> &[u8; 16] {
        &self.database_id
    }

    pub(crate) fn codec_id(&self) -> &str {
        &self.codec_id
    }

    pub(crate) const fn durability_policy(&self) -> DurabilityPolicy {
        self.durability_policy
    }

    pub(crate) const fn active_wal_segment(&self) -> u64 {
        self.active_wal_segment
    }

    pub(crate) const fn writer_lock_object(&self) -> &ObjectName {
        &self.writer_lock_object
    }

    pub(crate) const fn manifest_snapshot_watermark(&self) -> Option<u64> {
        self.manifest_snapshot_watermark
    }

    pub(crate) const fn manifest_snapshot_id(&self) -> Option<u64> {
        self.manifest_snapshot_id
    }

    pub(crate) const fn manifest_flush_watermark(&self) -> Option<CommitVersion> {
        self.manifest_flush_watermark
    }
}

impl<'a> LifecycleDurableLocalServices<'a> {
    pub(crate) const fn capability_outcome(&self) -> &LifecycleCapabilityOutcome {
        &self.capability_outcome
    }

    pub(crate) const fn assembly_facts(&self) -> &LifecycleDurableAssemblyFacts {
        &self.assembly_facts
    }

    pub(crate) const fn writer_guard(&self) -> Option<&BackendWriterGuard> {
        self.writer_guard.as_ref()
    }

    pub(crate) fn release_writer_guard(&mut self) -> bool {
        self.writer_guard.take().is_some()
    }

    pub(crate) const fn wal(&self) -> &WalService<'a> {
        &self.wal
    }

    pub(crate) fn wal_mut(&mut self) -> &mut WalService<'a> {
        &mut self.wal
    }

    /// Split borrow for the WAL-truncation path (#3494): the runner reads the
    /// retention proof from the manifest while mutating the WAL (reclaim
    /// rotation), which a pair of accessor calls cannot express.
    pub(crate) fn manifest_and_wal_mut(
        &mut self,
    ) -> (&DatabaseManifestService<'a>, &mut WalService<'a>) {
        (&self.manifest, &mut self.wal)
    }

    pub(crate) const fn manifest(&self) -> &DatabaseManifestService<'a> {
        &self.manifest
    }

    pub(crate) const fn table_manifest(&self) -> &TableManifestService<'a> {
        &self.table_manifest
    }

    pub(crate) const fn branch_catalog_manifest(&self) -> &BranchCatalogManifestService<'a> {
        &self.branch_catalog_manifest
    }

    pub(crate) const fn pending_releases_manifest(&self) -> &PendingReleasesManifestService<'a> {
        &self.pending_releases_manifest
    }

    pub(crate) const fn wal_sidecar(&self) -> &WalSegmentMetadataSidecarService<'a> {
        &self.wal_sidecar
    }

    pub(crate) const fn snapshot(&self) -> &SnapshotService<'a> {
        &self.snapshot
    }

    pub(crate) const fn table_object(&self) -> &TableObjectService<'static> {
        &self.table_object
    }

    pub(crate) const fn table_reader(&self) -> &TableObjectReaderService<'static> {
        &self.table_object
    }

    pub(crate) const fn checkpoint(&self) -> &CheckpointService<'a> {
        &self.checkpoint
    }

    pub(crate) const fn quarantine(&self) -> &QuarantineService<'static> {
        &self.quarantine
    }
}

impl<'a, S> LifecycleDurableLocalShell<'a, S> {
    pub(crate) fn assemble(
        request: LifecycleDurableLocalOpenRequest,
        backend: impl Into<BackendHandle<'static>>,
        timestamp_source: S,
    ) -> LifecycleResult<Self> {
        request.validate()?;
        let backend = backend.into();
        let mut state = LifecycleStateMachine::new();
        require_admitted(state, LifecycleOperationKind::Open)?;
        state.transition(LifecycleTransitionTrigger::OpenRequested)?;

        let capability_outcome = validate_backend_capabilities_for_open(request.plan(), &backend)?;
        let durability_policy = required_durability_policy(&capability_outcome)?;

        let writer_lock_object = ObjectLayout::writer_lock().map_err(layout_error)?;
        let writer_guard = backend
            .acquire_writer_lock(&writer_lock_object)
            .map_err(writer_lock_open_error)?;

        let manifest_service = DatabaseManifestService::new(backend.clone());
        let (manifest, disposition) =
            load_or_create_manifest(&manifest_service, &request, &backend)?;
        validate_manifest_identity(&manifest, &request)?;

        // #2765: a manifest that attests a published checkpoint proves durable
        // history existed, so the WAL chain through that watermark must be on
        // disk. With zero segment objects, `WalService::open` would recreate a
        // fresh empty log and recovery would present a gutted store as a
        // healthy empty database — strict mode refuses. Explicit lossy
        // recovery is the operator's informed reopen path for exactly this
        // damage (a torn rename can drop the segment legally, #2777): it
        // proceeds, recovers what the checkpoint holds, and records the
        // missing chain as a data-loss recovery fault. A manifest with no
        // checkpoint attestation (a first creation torn before its log or
        // creation checkpoint landed) is still allowed to recreate: nothing
        // acknowledged can exist yet.
        let wal_chain_missing_at_open =
            checkpoint_attested_wal_chain_missing(disposition, &manifest, &backend)?;
        if wal_chain_missing_at_open
            && request.plan().recovery_policy() == RecoveryStrictness::Strict
        {
            return Err(LifecycleError::recovery_corruption(
                "database manifest attests a checkpoint but no WAL segment objects exist",
            ));
        }

        // #2690: fail closed before recovery can silently resume onto a fresh
        // empty log when a WAL segment was removed out of band. The durable
        // watermark is authoritative and self-gating — absent for a fresh or
        // crash-during-creation database, present only once acknowledged data
        // existed.
        //
        // KNOWN GAP (surfaced by the fault-simulation sweep, seed 3): a
        // checkpointed database keeps its committed data in the snapshot and so
        // legitimately tolerates an absent WAL segment, but this unconditional
        // check refuses on the absence alone. Gating on the manifest's
        // checkpoint facts does not fix it, because a crash can drop the very
        // manifest update that recorded the checkpoint (SplitRename), leaving the
        // reopened manifest claiming no checkpoint. Resolving this needs a
        // checkpoint-comparable marker (e.g. a durable highest-committed-version
        // watermark, design §7 Q7) rather than a bare segment id — parked for
        // deliberation.
        verify_wal_segment_inventory(&backend).map_err(wal_open_error)?;

        let wal = WalService::open(
            backend.clone(),
            request.database_id(),
            manifest.active_wal_segment(),
            durability_policy,
            request.wal_config(),
        )
        .map_err(wal_open_error)?;
        if wal.active_segment_id() > manifest.active_wal_segment() {
            // Expected after post-checkpoint segment rolls: the manifest pointer
            // advances only when a checkpoint publishes, so the writer resumed
            // at the on-disk tail instead (#2555). The breadcrumb explains why
            // the live writer disagrees with the manifest until the next
            // published checkpoint.
            tracing::warn!(
                manifest_segment = manifest.active_wal_segment(),
                resumed_segment = wal.active_segment_id(),
                "WAL writer resumed past the manifest's stale segment pointer",
            );
        }

        let budget = StorageBudgetLedger::new(request.plan().lifecycle_config().storage_budget())?;
        let block_cache = table_block_cache_from_storage_budget(budget.budget())?;
        let branch_config =
            branch_config_with_storage_budget(request.branch_config(), budget.budget())?;
        let branch = BranchLocalState::new(request.initial_branch_id(), branch_config)
            .map_err(branch_error)?;
        let mut registry = CommitBranchRegistry::new();
        registry
            .register_active(request.initial_branch_id(), request.branch_generation())
            .map_err(commit_error)?;

        let assembly_facts = LifecycleDurableAssemblyFacts::new(
            request.plan().storage_mode(),
            disposition,
            &manifest,
            durability_policy,
            writer_lock_object,
            wal_chain_missing_at_open,
        );

        let services = LifecycleDurableLocalServices {
            capability_outcome,
            manifest: manifest_service,
            table_manifest: TableManifestService::new(backend.clone()),
            branch_catalog_manifest: BranchCatalogManifestService::new(backend.clone()),
            pending_releases_manifest: PendingReleasesManifestService::new(backend.clone()),
            wal,
            wal_sidecar: WalSegmentMetadataSidecarService::new(backend.clone()),
            snapshot: SnapshotService::new(backend.clone()),
            table_object: match block_cache {
                Some(cache) => TableObjectService::new(backend.clone()).with_block_cache(cache),
                None => TableObjectService::new(backend.clone()),
            },
            checkpoint: CheckpointService::new(backend.clone()),
            quarantine: QuarantineService::new(backend),
            assembly_facts,
            writer_guard: Some(writer_guard),
        };

        state.transition(LifecycleTransitionTrigger::DurableRecoveryRequired)?;
        let commit_config = request.commit_config();

        Ok(Self {
            state,
            open_plan: request.plan,
            services,
            branch,
            registry,
            guard_set: CommitBranchGuardSet::new(),
            allocator: CommitFactAllocator::new(
                CommitVersionAllocator::default(),
                CommitTimestampGuard::default(),
                timestamp_source,
            ),
            visible: VisibleVersionTracker::default(),
            durable_gate: CommitUnresolvedDurableGate::new(),
            commit_config,
            table_catalog: LifecycleDurableTableCatalog::new(),
            budget,
        })
    }

    pub(crate) const fn state(&self) -> LifecycleState {
        self.state.state()
    }

    pub(crate) const fn open_plan(&self) -> &StorageOpenPlan {
        &self.open_plan
    }

    pub(crate) const fn services(&self) -> &LifecycleDurableLocalServices<'a> {
        &self.services
    }

    pub(crate) fn services_mut(&mut self) -> &mut LifecycleDurableLocalServices<'a> {
        &mut self.services
    }

    pub(crate) const fn assembly_facts(&self) -> &LifecycleDurableAssemblyFacts {
        self.services.assembly_facts()
    }

    pub(crate) const fn branch_state(&self) -> &BranchLocalState {
        &self.branch
    }

    pub(crate) fn branch_state_mut(&mut self) -> &mut BranchLocalState {
        &mut self.branch
    }

    pub(crate) const fn budget(&self) -> &StorageBudgetLedger {
        &self.budget
    }

    pub(crate) fn stage_table_manifest_recovery(
        &self,
    ) -> LifecycleResult<LifecycleTableManifestRecoveryStage> {
        stage_table_manifest_for_branch(
            &self.branch,
            self.services.table_manifest(),
            self.services.table_reader(),
            &self.table_catalog,
            Some(&self.budget),
        )
    }

    pub(crate) fn apply_table_manifest_recovery(
        &mut self,
        stage: LifecycleTableManifestRecoveryStage,
    ) {
        let (branch, catalog, _outcome) = stage.into_parts();
        self.branch = branch;
        self.table_catalog = catalog;
    }

    pub(crate) const fn registry(&self) -> &CommitBranchRegistry {
        &self.registry
    }

    pub(crate) const fn guard_set(&self) -> &CommitBranchGuardSet {
        &self.guard_set
    }

    pub(crate) const fn allocator(&self) -> &CommitFactAllocator<S> {
        &self.allocator
    }

    pub(crate) const fn durable_gate(&self) -> &CommitUnresolvedDurableGate {
        &self.durable_gate
    }

    pub(crate) const fn commit_config(&self) -> CommitRuntimeConfig {
        self.commit_config
    }

    pub(crate) const fn visible_version(&self) -> CommitVersion {
        self.visible.visible_version()
    }

    pub(crate) fn unresolved_durable(&self) -> LifecycleResult<Option<CommitUnresolvedDurable>> {
        self.durable_gate.unresolved().map_err(commit_error)
    }

    pub(crate) fn admit_recovery_step(&self) -> LifecycleResult<()> {
        require_admitted(self.state, LifecycleOperationKind::RecoveryStep)
    }

    pub(crate) fn admit_ordinary_read(&self) -> LifecycleResult<()> {
        require_admitted(self.state, LifecycleOperationKind::OrdinaryRead)
    }

    pub(crate) fn admit_commit(&self) -> LifecycleResult<()> {
        require_admitted(self.state, LifecycleOperationKind::Commit)
    }

    pub(crate) fn admit_ordinary_maintenance(&self) -> LifecycleResult<()> {
        require_admitted(self.state, LifecycleOperationKind::OrdinaryMaintenance)
    }

    pub(crate) fn admit_health_query(&self) -> LifecycleResult<()> {
        require_admitted(self.state, LifecycleOperationKind::HealthQuery)
    }
}

impl fmt::Debug for LifecycleDurableLocalServices<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LifecycleDurableLocalServices")
            .field("capability_outcome", &self.capability_outcome)
            .field("assembly_facts", &self.assembly_facts)
            .field("writer_guard", &self.writer_guard)
            .field("active_wal_segment", &self.wal.active_segment_id())
            .finish_non_exhaustive()
    }
}

impl<S> fmt::Debug for LifecycleDurableLocalShell<'_, S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LifecycleDurableLocalShell")
            .field("state", &self.state)
            .field("open_plan", &self.open_plan)
            .field("services", &self.services)
            .field("branch_id", &self.branch.branch_id())
            .field("visible_version", &self.visible.visible_version())
            .finish_non_exhaustive()
    }
}

/// The durable policy the capability validation resolved — its absence on a
/// durable-local open plan is a caller error.
fn required_durability_policy(
    outcome: &LifecycleCapabilityOutcome,
) -> LifecycleResult<DurabilityPolicy> {
    outcome
        .durability_policy()
        .ok_or(LifecycleError::InvalidOpenPlan {
            reason: "durable local assembly requires durable policy",
        })
}

/// The object families that cannot precede the database manifest: the
/// manifest is the FIRST durable publish of a new store and its replace is
/// atomic, so any object in these families without a manifest means the
/// manifest was LOST, not that the store is new. `Locks` (acquired before
/// the manifest) and `Temporary` (publish scratch a first-create crash can
/// leave behind) are legitimately pre-manifest and excluded.
const POST_MANIFEST_FAMILIES: [ObjectFamily; 5] = [
    ObjectFamily::Wal,
    ObjectFamily::Meta,
    ObjectFamily::Tables,
    ObjectFamily::Snapshots,
    ObjectFamily::Quarantine,
];

/// True when any post-manifest durable object exists under `backend` — the
/// missing-manifest disambiguator between "new store" and "damaged store".
fn durable_residue_exists(backend: &BackendHandle<'static>) -> LifecycleResult<bool> {
    for family in POST_MANIFEST_FAMILIES {
        let prefix = ObjectLayout::family_prefix(family).map_err(layout_error)?;
        if !backend
            .list_prefix(&prefix)
            .map_err(backend_error)?
            .is_empty()
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn load_or_create_manifest(
    service: &DatabaseManifestService<'_>,
    request: &LifecycleDurableLocalOpenRequest,
    backend: &BackendHandle<'static>,
) -> LifecycleResult<(DatabaseManifest, StorageOpenDisposition)> {
    if let Some(manifest) = service.load_current().map_err(manifest_error)? {
        return Ok((manifest, StorageOpenDisposition::OpenedExisting));
    }

    // #3015: a store with durable objects but no manifest has LOST it —
    // creating a fresh manifest here silently erased the snapshot-id floor,
    // flush watermark, and checkpoint lineage, then reported Healthy while
    // checkpoints collided with orphaned snapshot objects forever. Refuse
    // as recovery corruption; only a genuinely new store may create. One
    // reload first absorbs a concurrent creator that completed between the
    // load above and the residue listing (a completed creation publishes
    // the manifest before any other durable object, so residue plus a
    // still-absent manifest can only mean loss).
    if durable_residue_exists(backend)? {
        if let Some(manifest) = service.load_current().map_err(manifest_error)? {
            return Ok((manifest, StorageOpenDisposition::OpenedExisting));
        }
        return Err(LifecycleError::recovery_corruption_with(
            "database manifest is missing while durable objects exist",
            ManifestServiceError::Missing {
                role: ManifestRole::Database,
                object: ObjectLayout::database_manifest().map_err(layout_error)?,
            },
        ));
    }

    match service.create_initial(request.database_id(), request.plan().codec_id().as_str()) {
        Ok(write) => Ok((write.manifest().clone(), StorageOpenDisposition::Created)),
        Err(ManifestServiceError::Publish { source, .. })
            if source.kind() == PublishFailureKind::PreconditionFailed =>
        {
            let manifest = service.load_required().map_err(manifest_error)?;
            Ok((manifest, StorageOpenDisposition::OpenedExisting))
        }
        Err(error) => Err(manifest_error(error)),
    }
}

fn validate_manifest_identity(
    manifest: &DatabaseManifest,
    request: &LifecycleDurableLocalOpenRequest,
) -> LifecycleResult<()> {
    if manifest.database_id() != &request.database_id() {
        return Err(LifecycleError::InvalidOpenPlan {
            reason: "database manifest id does not match durable open request",
        });
    }
    if manifest.codec_id() != request.plan().codec_id().as_str() {
        return Err(LifecycleError::InvalidOpenPlan {
            reason: "database manifest codec does not match durable open request",
        });
    }
    Ok(())
}

fn durable_policy_for_mode(mode: StorageMode) -> LifecycleResult<DurabilityPolicy> {
    match mode {
        StorageMode::DurableLocalStandard => Ok(DurabilityPolicy::Standard),
        StorageMode::DurableLocalAlways => Ok(DurabilityPolicy::Always),
        StorageMode::Cache => Err(LifecycleError::InvalidOpenPlan {
            reason: "durable local assembly requires durable local storage mode",
        }),
        StorageMode::ObjectDurableCandidate => Err(LifecycleError::InvalidOpenPlan {
            reason:
                "object-durable mode requires fenced object publication before runtime assembly",
        }),
    }
}

fn require_admitted(
    state: LifecycleStateMachine,
    operation: LifecycleOperationKind,
) -> LifecycleResult<()> {
    let admission = state.admit(operation);
    if admission.is_allowed() {
        Ok(())
    } else {
        Err(LifecycleError::InvalidLifecycleState {
            reason: admission
                .rejection_reason()
                .unwrap_or("operation is not admitted in current lifecycle state"),
        })
    }
}

fn backend_error(error: BackendError) -> LifecycleError {
    LifecycleError::lower_layer_with(LifecycleLowerLayer::Backend, "backend failed", error)
}

/// Classify the first backend touch of an open — acquiring the writer lock,
/// which creates the database directory. A path-shape failure here (the parent
/// directory is missing, or the path is a file rather than a directory) is a
/// caller-supplied invalid argument, not a transient backend outage a caller
/// should retry. Every other failure stays a lower-layer backend error.
fn writer_lock_open_error(error: BackendError) -> LifecycleError {
    match error.kind() {
        BackendErrorKind::NotFound => LifecycleError::InvalidConfig {
            field: "path",
            reason: "database path or a parent directory does not exist",
        },
        BackendErrorKind::Corruption => LifecycleError::InvalidConfig {
            field: "path",
            reason: "database path is not a directory",
        },
        // Another opener holds the writer lock. This is a precondition the
        // caller can satisfy — by closing the other handle or attaching to it —
        // but never by retrying the same open, so it must not arrive as a
        // transient backend outage (#3005, #3167).
        BackendErrorKind::AlreadyExists => LifecycleError::WriterLockHeld,
        _ => backend_error(error),
    }
}

fn checkpoint_attested_wal_chain_missing(
    disposition: StorageOpenDisposition,
    manifest: &DatabaseManifest,
    backend: &BackendHandle<'static>,
) -> LifecycleResult<bool> {
    Ok(disposition == StorageOpenDisposition::OpenedExisting
        && manifest.snapshot_watermark().is_some()
        && !wal_segments_present(backend.as_backend()).map_err(wal_open_error)?)
}

fn layout_error(error: LayoutError) -> LifecycleError {
    LifecycleError::lower_layer_with(LifecycleLowerLayer::Layout, "layout failed", error)
}

fn manifest_error(error: ManifestServiceError) -> LifecycleError {
    let reason = match &error {
        ManifestServiceError::Publish { source, .. } => match source.kind() {
            PublishFailureKind::VisibilityUnknown => "database manifest publish visibility unknown",
            PublishFailureKind::VisibleDurabilityUnconfirmed => {
                "database manifest publish durability unconfirmed"
            }
            PublishFailureKind::FailedBeforeVisibility => {
                "database manifest publish failed before visibility"
            }
            PublishFailureKind::PreconditionFailed => {
                "database manifest create precondition failed"
            }
            PublishFailureKind::Unsupported => "database manifest publish unsupported",
        },
        ManifestServiceError::Delete { .. } => "database manifest delete failed",
        ManifestServiceError::Missing { .. } => "database manifest missing",
        ManifestServiceError::CodecMismatch { .. } => "database manifest codec mismatch",
        ManifestServiceError::Decode { .. } => "database manifest decode failed",
        ManifestServiceError::Read { .. } => "database manifest read failed",
        ManifestServiceError::List { .. } => "database manifest list failed",
        ManifestServiceError::Layout { .. } => "database manifest layout failed",
        ManifestServiceError::Encode { .. } => "database manifest encode failed",
        ManifestServiceError::InvalidPublishMetadata { .. } => {
            "database manifest publish metadata invalid"
        }
        ManifestServiceError::InvalidRecoveryFact { .. } => {
            "database manifest recovery fact invalid"
        }
    };
    LifecycleError::lower_layer_with(LifecycleLowerLayer::Service, reason, error)
}

fn wal_error(error: WalServiceError) -> LifecycleError {
    LifecycleError::lower_layer_with(LifecycleLowerLayer::Service, "WAL service failed", error)
}

/// Classify a WAL failure while opening the service during assembly. A segment
/// that fails to decode (checksum/magic mismatch) is permanent corruption,
/// refused with a non-retryable recovery error rather than a transient
/// lower-layer outage.
fn wal_open_error(error: WalServiceError) -> LifecycleError {
    if error.is_durable_corruption() {
        return LifecycleError::recovery_corruption_with(
            "WAL segment failed to decode while opening the database",
            error,
        );
    }
    wal_error(error)
}

fn branch_error(error: impl std::error::Error + Send + Sync + 'static) -> LifecycleError {
    LifecycleError::lower_layer_with(
        LifecycleLowerLayer::BranchRuntime,
        "branch runtime failed",
        error,
    )
}

pub(super) fn commit_error(error: CommitRuntimeError) -> LifecycleError {
    match error {
        CommitRuntimeError::InvalidTimelineFact { reason }
        | CommitRuntimeError::TimelineConflict { reason } => {
            LifecycleError::TimelineRecoveryMismatch { reason }
        }
        other => LifecycleError::lower_layer_with(
            LifecycleLowerLayer::CommitRuntime,
            "commit runtime failed",
            other,
        ),
    }
}
