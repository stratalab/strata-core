//! Branch table-manifest publication and recovery helpers.

use super::{
    require_manifest_catalog_budget, require_table_reader_budget, telemetry_health_debt,
    LifecycleError, LifecycleLowerLayer, LifecycleResult, MaintenanceOutcome,
    MaintenanceOutcomeStatus, StorageBudgetLedger,
};
use crate::backend::{BackendErrorKind, PublishError, PublishFailureKind};
use crate::branch::facts::{
    BranchLevel, BranchTableDescriptor, InheritedLayerDescriptor, InheritedLayerStatus,
};
use crate::branch::read::{BranchInheritedLayer, BranchMaterializationSource, BranchOwnedTable};
use crate::branch::state::manifest_recovery::{
    BranchTableManifestRecoveryOutcome, BranchTableManifestRecoveryRequest,
};
use crate::branch::state::BranchLocalState;
use crate::format::{
    decode_table_row_split_extension_section, encode_table_manifest,
    table_row_split_extension_section, FormatError, TableManifest, TableManifestInheritedLayer,
    TableManifestInheritedLayerStatus, TableManifestLevel, TableManifestTableBounds,
    TableManifestTableFacts, TableManifestTableProvenance, TableManifestTableRef, TableRowSplit,
};
use crate::object::ObjectName;
use crate::row::StorageRow;
use crate::service::{
    ManifestRole, ManifestServiceError, TableManifestService, TableManifestWrite, TableObjectFacts,
    TableObjectReadError, TableObjectReaderService,
};
use crate::table::{
    ImmutableTableReader, TableCursor, TableIdentity, TableReaderConfig, TableRow,
    TableRuntimeFacts, TableSummaryExtras,
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use strata_core::BranchId;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LifecycleDurableTableCatalog {
    entries: BTreeMap<String, LifecycleDurableTableCatalogEntry>,
    objects: BTreeMap<String, String>,
    next_manifest_sequence: u64,
    // Set when a table is installed in-memory (`record_table`) and cleared when a manifest is
    // durably recorded (`record_manifest` / `record_reserved_manifest`). A manifest publish
    // that fails leaves this set: the in-memory branch state then owns L0 rows the durable
    // manifest does not cover, so a checkpoint must defer rather than advance the WAL-replay
    // floor past them. In-memory bookkeeping only — recovery rebuilds the catalog via
    // `record_manifest`, which clears it, so a reopen always starts settled (`false`).
    //
    // Catalog-global, not per-branch — correct for a single flushing branch (the regime the
    // fault soak exercises). Multiple branches can flush (the maintenance coverage scan in
    // `durable/maintenance.rs` enqueues flush per-branch under pressure), so branch B's
    // `record_manifest` could clear the flag branch A's install set, masking branch A's debt from
    // the checkpoint defer. The multi-branch checkpoint guard covers this in practice — a checkpoint
    // also defers while any non-seeded branch holds a durable base, and a branch carrying publish
    // debt owns exactly such a base, so the masked-debt snapshot is not recorded. A per-branch debt
    // set is the deferred precise fix; see multi-branch-orphaned-delta-recovery-gap.md.
    manifest_publish_pending: bool,
    // #2553: the reclaim mark must never sweep an object that a recovery-relevant manifest
    // still references. Two per-branch object-name frontiers make that predicate local:
    //
    // - `confirmed_frontiers`: the object names listed by the branch's last durably-CONFIRMED
    //   manifest — the exact set recovery loads if the process dies now (the crash-revert
    //   target). A visible-but-unconfirmed replace does NOT advance this.
    // - `pending_publications`: the names listed by a built manifest whose off-lock persist is
    //   in flight or ended publication-uncertain. Inserted when the publish acquires the
    //   per-branch slot (deferred attempts never persist and add nothing), replaced by the
    //   confirmed frontier on a confirmed publish, and KEPT on an uncertain persist — the bytes
    //   may be durable even while reads still serve the prior manifest.
    //
    // The sweep unions both into its pinned set, closing the window where a rewrite consumed
    // inputs out of branch state while the manifest that still lists them had not yet landed
    // (or could revert on crash). Bounded: at most one set of names per branch per map.
    confirmed_frontiers: BTreeMap<[u8; 16], Arc<BTreeSet<ObjectName>>>,
    pending_publications: BTreeMap<[u8; 16], Arc<BTreeSet<ObjectName>>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LifecycleDurableTableCatalogEntry {
    identity: TableIdentity,
    object_facts: TableObjectFacts,
    provenance: TableManifestTableProvenance,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LifecycleTableManifestRecoveryOutcome {
    manifest_object: Option<ObjectName>,
    manifest_sequence: Option<u64>,
    table_count: usize,
    install_outcome: Option<BranchTableManifestRecoveryOutcome>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LifecycleTableManifestRecoveryStage {
    branch: BranchLocalState,
    catalog: LifecycleDurableTableCatalog,
    outcome: LifecycleTableManifestRecoveryOutcome,
}

impl LifecycleDurableTableCatalog {
    pub(crate) fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
            objects: BTreeMap::new(),
            next_manifest_sequence: 1,
            manifest_publish_pending: false,
            confirmed_frontiers: BTreeMap::new(),
            pending_publications: BTreeMap::new(),
        }
    }

    pub(crate) fn record_table(
        &mut self,
        identity: TableIdentity,
        object_facts: TableObjectFacts,
    ) -> LifecycleResult<()> {
        self.record_table_with_provenance(
            identity,
            object_facts,
            TableManifestTableProvenance::Flush,
        )
    }

    pub(crate) fn record_table_with_provenance(
        &mut self,
        identity: TableIdentity,
        object_facts: TableObjectFacts,
        provenance: TableManifestTableProvenance,
    ) -> LifecycleResult<()> {
        let key = identity.as_str().to_owned();
        let object_key = object_facts.object().as_str().to_owned();
        let entry = LifecycleDurableTableCatalogEntry {
            identity,
            object_facts,
            provenance,
        };
        if let Some(existing) = self.entries.get(&key) {
            if existing == &entry {
                return Ok(());
            }
            return Err(LifecycleError::TableManifestPublicationFailed {
                reason: "table identity has conflicting durable object facts",
                source: None,
            });
        }
        if self
            .objects
            .get(&object_key)
            .is_some_and(|existing_identity| existing_identity != &key)
        {
            return Err(LifecycleError::TableManifestPublicationFailed {
                reason: "table object has conflicting durable table identity",
                source: None,
            });
        }
        self.objects.insert(object_key, key.clone());
        self.entries.insert(key, entry);
        // A new table is installed in-memory; the durable manifest no longer covers every
        // owned L0 row until the next successful manifest publish records it.
        self.manifest_publish_pending = true;
        Ok(())
    }

    pub(crate) fn record_manifest(&mut self, manifest: &TableManifest) -> LifecycleResult<()> {
        if manifest.manifest_sequence() < self.next_manifest_sequence {
            return Err(LifecycleError::TableManifestPublicationFailed {
                reason: "table manifest sequence regresses below catalog state",
                source: None,
            });
        }
        self.record_manifest_tables(manifest)?;
        self.next_manifest_sequence = manifest.manifest_sequence().checked_add(1).ok_or(
            LifecycleError::TableManifestPublicationFailed {
                reason: "table manifest sequence overflow",
                source: None,
            },
        )?;
        // The manifest is durably recorded: the catalog and the durable manifest agree again.
        self.manifest_publish_pending = false;
        self.advance_confirmed_frontier(manifest.branch_id(), manifest_object_names(manifest));
        Ok(())
    }

    /// Advances the branch's confirmed frontier to a durably-recorded manifest's object names
    /// and drops any pending publication it supersedes (#2553).
    fn advance_confirmed_frontier(
        &mut self,
        branch_id: BranchId,
        names: Arc<BTreeSet<ObjectName>>,
    ) {
        let key = *branch_id.as_bytes();
        self.confirmed_frontiers.insert(key, names);
        self.pending_publications.remove(&key);
    }

    /// Registers the object names of a built manifest whose off-lock persist is about to run
    /// (#2553). Called only after the per-branch publish slot is acquired — deferred attempts
    /// never persist and must not accumulate protection. The entry is superseded by the next
    /// confirmed publish for the branch; an uncertain persist keeps it (the bytes may be
    /// durable even while reads serve the prior manifest).
    pub(crate) fn record_pending_publication(
        &mut self,
        branch_id: BranchId,
        names: Arc<BTreeSet<ObjectName>>,
    ) {
        self.pending_publications
            .insert(*branch_id.as_bytes(), names);
    }

    /// Every object name protected because a recovery-relevant manifest may still list it:
    /// the last durably-confirmed manifest per branch (the crash-revert target) plus any
    /// built-but-unconfirmed publication (#2553). The reclaim mark unions these into its
    /// pinned set.
    pub(crate) fn manifest_frontier_pinned_objects(&self) -> Vec<ObjectName> {
        self.confirmed_frontiers
            .values()
            .chain(self.pending_publications.values())
            .flat_map(|names| names.iter().cloned())
            .collect()
    }

    /// Drops both frontier sets for a deleted branch. Safe only once the branch tombstone is
    /// durable: recovery then ignores the branch's table manifest, so nothing recovery-relevant
    /// references the objects and normal reachability governs their reclaim.
    pub(crate) fn clear_branch_frontier(&mut self, branch_id: BranchId) {
        let key = *branch_id.as_bytes();
        self.confirmed_frontiers.remove(&key);
        self.pending_publications.remove(&key);
    }

    /// Confirms a reserved-sequence manifest's off-lock publish as durable and clears the
    /// pending-manifest debt (the sequence marker was advanced at reservation time). O(1) by
    /// design (BS5.3): the reserved manifest was serialized FROM this catalog under the lock at
    /// reserve time, and the publish's phase one recorded every newly installed table before the
    /// manifest was built — re-recording all entries here was O(catalog) of clones and compares
    /// per publish under the runtime lock (measured: 944 ms of a 3 s sustained-write window,
    /// ~17 ms per flush at steady state, starving writers), and could silently resurrect entries
    /// removed between the publish phases. Recovery-side manifests still validate entry-by-entry
    /// via [`record_recovered_manifest`](Self::record_recovered_manifest).
    ///
    /// The frontier advance stays O(1): `names` was collected during the manifest build walk
    /// (already O(tables) under the lock) and moves in as an `Arc` swap (#2553).
    pub(crate) fn confirm_reserved_manifest_published(
        &mut self,
        branch_id: BranchId,
        names: Arc<BTreeSet<ObjectName>>,
    ) {
        self.manifest_publish_pending = false;
        self.advance_confirmed_frontier(branch_id, names);
    }

    /// Records a manifest loaded during recovery. Recovery applies per-branch manifests in
    /// branch-id order, not sequence order, so interleaved cross-branch sequences (parent seq 1 →
    /// fork child seq 2 → parent seq 3, with the parent's manifest applied first) are the normal
    /// shape once forks publish child manifests — a lower sequence here is reordering, not
    /// regression. The sequence marker advances to `max(next, seq + 1)` so post-recovery runtime
    /// publishes stay strictly monotonic; [`record_manifest`](Self::record_manifest) keeps the
    /// strict regression check for those runtime publishes.
    pub(crate) fn record_recovered_manifest(
        &mut self,
        manifest: &TableManifest,
    ) -> LifecycleResult<()> {
        self.record_manifest_tables(manifest)?;
        let advanced = manifest.manifest_sequence().checked_add(1).ok_or(
            LifecycleError::TableManifestPublicationFailed {
                reason: "table manifest sequence overflow",
                source: None,
            },
        )?;
        self.next_manifest_sequence = self.next_manifest_sequence.max(advanced);
        // The loaded manifest is durable by definition: catalog and durable manifest agree.
        self.manifest_publish_pending = false;
        self.advance_confirmed_frontier(manifest.branch_id(), manifest_object_names(manifest));
        Ok(())
    }

    fn record_manifest_tables(&mut self, manifest: &TableManifest) -> LifecycleResult<()> {
        for table in manifest_table_refs(manifest) {
            self.record_table_with_provenance(
                table.table_identity().clone(),
                TableObjectFacts::from_table_manifest_ref(table),
                table.provenance().clone(),
            )?;
        }
        Ok(())
    }

    /// Reserves the next manifest sequence under the runtime lock. The caller stamps the
    /// reserved value into the manifest it persists off-lock; advancing the counter here (rather
    /// than at record time) keeps cross-branch sequences unique and monotonic when the durable
    /// write runs off-lock under a per-branch publish lock.
    pub(crate) fn reserve_manifest_sequence(&mut self) -> LifecycleResult<u64> {
        let reserved = self.next_manifest_sequence;
        self.next_manifest_sequence =
            reserved
                .checked_add(1)
                .ok_or(LifecycleError::TableManifestPublicationFailed {
                    reason: "table manifest sequence overflow",
                    source: None,
                })?;
        Ok(reserved)
    }

    pub(crate) fn build_manifest(
        &self,
        branch: &BranchLocalState,
    ) -> LifecycleResult<TableManifest> {
        self.build_manifest_with_sequence(branch, self.next_manifest_sequence)
    }

    pub(crate) fn build_manifest_with_sequence(
        &self,
        branch: &BranchLocalState,
        manifest_sequence: u64,
    ) -> LifecycleResult<TableManifest> {
        // BS4.4g: collect the per-table put/tombstone split in the same walk that builds the
        // manifest's table records, so `row_splits` is positionally aligned with the tables
        // recovery decodes (top-level levels, then each inherited layer's levels).
        let mut row_splits = Vec::<TableRowSplit>::new();
        let levels = manifest_levels_from_owned(branch.owned_levels(), self, &mut row_splits)?;
        let inherited_layers =
            manifest_inherited_layers(branch.inherited_layers(), self, &mut row_splits)?;
        let mut extensions = Vec::new();
        if let Some(facts) =
            super::retained_history_extension::RetainedHistoryFacts::from_timestamp_coverage(
                branch.timestamp_coverage(),
                manifest_retained_version_floor(branch),
            )
        {
            extensions.push(facts.to_extension_section().map_err(format_error)?);
        }
        // Sorted-by-kind order: "storage.retained_history" precedes "storage.table_row_split".
        if !row_splits.is_empty() {
            extensions.push(table_row_split_extension_section(&row_splits).map_err(format_error)?);
        }
        TableManifest::new(
            branch.branch_id(),
            None,
            manifest_sequence,
            levels,
            inherited_layers,
            extensions,
        )
        .map_err(format_error)
    }

    #[allow(
        dead_code,
        reason = "manifest sequence is asserted by table-manifest tests"
    )]
    pub(crate) const fn next_manifest_sequence(&self) -> u64 {
        self.next_manifest_sequence
    }

    pub(crate) fn entry_count(&self) -> usize {
        self.entries.len()
    }

    /// Whether a table has been installed in-memory without a subsequent successful manifest
    /// publish — i.e. the durable manifest does not yet cover every owned L0 row. A checkpoint
    /// must defer while this holds so it cannot advance the WAL-replay floor past rows a clean
    /// reopen could not recover.
    pub(crate) const fn has_outstanding_manifest_debt(&self) -> bool {
        self.manifest_publish_pending
    }

    pub(crate) fn object_count(&self) -> usize {
        self.objects.len()
    }

    /// The durable object backing `identity`, if the catalog knows it. Used by the table-object
    /// sweep to map in-memory branch state (owned + inherited tables) onto inventory object names
    /// so in-memory-reachable objects are pinned against reclaim.
    pub(crate) fn object_for_identity(&self, identity: &TableIdentity) -> Option<&ObjectName> {
        self.entries
            .get(identity.as_str())
            .map(|entry| entry.object_facts.object())
    }

    fn entry_for(
        &self,
        identity: &TableIdentity,
    ) -> LifecycleResult<&LifecycleDurableTableCatalogEntry> {
        self.entries
            .get(identity.as_str())
            .ok_or(LifecycleError::TableManifestPublicationFailed {
            reason:
                "branch table state contains volatile rewrite output without durable catalog entry",
            source: None,
        })
    }
}

impl Default for LifecycleDurableTableCatalog {
    fn default() -> Self {
        Self::new()
    }
}

impl LifecycleTableManifestRecoveryOutcome {
    pub(crate) const fn absent() -> Self {
        Self {
            manifest_object: None,
            manifest_sequence: None,
            table_count: 0,
            install_outcome: None,
        }
    }

    fn installed(
        manifest_object: ObjectName,
        manifest: &TableManifest,
        install_outcome: BranchTableManifestRecoveryOutcome,
    ) -> Self {
        Self {
            manifest_object: Some(manifest_object),
            manifest_sequence: Some(manifest.manifest_sequence()),
            table_count: install_outcome.total_table_count(),
            install_outcome: Some(install_outcome),
        }
    }

    #[allow(
        dead_code,
        reason = "table-manifest recovery facts are consumed by lifecycle tests"
    )]
    pub(crate) const fn manifest_object(&self) -> Option<&ObjectName> {
        self.manifest_object.as_ref()
    }

    #[allow(
        dead_code,
        reason = "table-manifest recovery facts are consumed by lifecycle tests"
    )]
    pub(crate) const fn manifest_sequence(&self) -> Option<u64> {
        self.manifest_sequence
    }

    #[allow(
        dead_code,
        reason = "table-manifest recovery facts are consumed by lifecycle tests"
    )]
    pub(crate) const fn table_count(&self) -> usize {
        self.table_count
    }

    #[allow(
        dead_code,
        reason = "table-manifest recovery facts are consumed by lifecycle tests"
    )]
    pub(crate) const fn install_outcome(&self) -> Option<&BranchTableManifestRecoveryOutcome> {
        self.install_outcome.as_ref()
    }
}

impl LifecycleTableManifestRecoveryStage {
    fn new(
        branch: BranchLocalState,
        catalog: LifecycleDurableTableCatalog,
        outcome: LifecycleTableManifestRecoveryOutcome,
    ) -> Self {
        Self {
            branch,
            catalog,
            outcome,
        }
    }

    pub(crate) const fn outcome(&self) -> &LifecycleTableManifestRecoveryOutcome {
        &self.outcome
    }

    pub(crate) const fn staged_branch(&self) -> &BranchLocalState {
        &self.branch
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        BranchLocalState,
        LifecycleDurableTableCatalog,
        LifecycleTableManifestRecoveryOutcome,
    ) {
        (self.branch, self.catalog, self.outcome)
    }
}

pub(crate) fn preflight_table_manifest_with_checkpoint(
    checkpoint_branch: &BranchLocalState,
    staged_branch: &BranchLocalState,
) -> LifecycleResult<bool> {
    // BS4.5b: with no checkpoint delta rows there is no overlap to check, so skip the O(dataset) manifest
    // scan — close writes a (usually empty) delta checkpoint on every reopen, so this guard is what keeps
    // the common combine path O(metadata). A non-empty delta still runs the full byte-divergence check.
    if checkpoint_branch
        .owned_levels()
        .iter()
        .flatten()
        .next()
        .is_none()
    {
        return Ok(false);
    }
    // BS4.4c: stream owned tables through the cursor; the checkpoint map is now keyed + valued by owned
    // bytes (cursor rows are transient) rather than borrowing into a materialized row slice.
    let mut checkpoint_rows = BTreeMap::<Vec<u8>, StorageRow>::new();
    for table in checkpoint_branch.owned_levels().iter().flatten() {
        try_for_each_reader_row(table.reader(), |row| {
            checkpoint_rows.insert(row.key().as_slice().to_vec(), row.row().clone());
            Ok(())
        })?;
    }
    let mut any_overlap = false;
    for table in staged_branch.owned_levels().iter().flatten() {
        try_for_each_reader_row(table.reader(), |row| {
            if let Some(checkpoint_row) = checkpoint_rows.get(row.key().as_slice()) {
                if checkpoint_row != row.row() {
                    return Err(LifecycleError::table_manifest_checkpoint_conflict(
                        "manifest row bytes diverge from checkpoint at internal key",
                    ));
                }
                any_overlap = true;
            }
            Ok(())
        })?;
    }
    Ok(any_overlap)
}

/// BS4.4c: walk a durable table's rows through its cursor (ascending internal-key order), applying the
/// fallible `f` per row without materializing the whole table. Cursor failures map to a manifest error.
/// C2: no-fill cursor (BS4.4g) — a one-shot recovery-time validation walk
/// must not seed the block cache in scan order; deliberate fill is the
/// preheat's job.
pub(crate) fn try_for_each_reader_row(
    reader: &ImmutableTableReader<'_>,
    mut f: impl FnMut(&TableRow) -> LifecycleResult<()>,
) -> LifecycleResult<()> {
    let mut cursor = reader.cursor_without_cache_fill();
    cursor
        .seek_to_first()
        .map_err(|_| manifest_reader_scan_failed())?;
    while let Some(row) = cursor.current() {
        f(row)?;
        cursor
            .advance()
            .map_err(|_| manifest_reader_scan_failed())?;
    }
    Ok(())
}

#[allow(
    dead_code,
    reason = "budget-free wrapper preserves the table-manifest helper surface for direct tests"
)]
pub(crate) fn publish_table_manifest_for_branch(
    branch: &BranchLocalState,
    service: &TableManifestService<'_>,
    catalog: &mut LifecycleDurableTableCatalog,
) -> LifecycleResult<TableManifestWrite> {
    publish_table_manifest_for_branch_with_budget(branch, service, catalog, None)
}

pub(crate) fn publish_table_manifest_for_branch_with_budget(
    branch: &BranchLocalState,
    service: &TableManifestService<'_>,
    catalog: &mut LifecycleDurableTableCatalog,
    budget: Option<&StorageBudgetLedger>,
) -> LifecycleResult<TableManifestWrite> {
    let manifest = catalog.build_manifest(branch)?;
    let write = persist_reserved_manifest(service, branch.branch_id(), &manifest, budget)?;
    catalog.record_manifest(write.manifest())?;
    Ok(write)
}

/// Persists an already-built (and, on the off-lock path, sequence-reserved) table manifest:
/// the budget check + encode + durable `publish_replace_manifest` (the fsync). This is the only
/// step the off-lock publish phase runs while the global runtime lock is released; it touches no
/// catalog state, so it is safe to call between the under-lock reserve and record phases.
pub(crate) fn persist_reserved_manifest(
    service: &TableManifestService<'_>,
    branch_id: BranchId,
    manifest: &TableManifest,
    budget: Option<&StorageBudgetLedger>,
) -> LifecycleResult<TableManifestWrite> {
    if let Some(budget) = budget {
        let bytes = encode_table_manifest(manifest).map_err(format_error)?;
        require_manifest_catalog_budget(
            budget,
            bytes.len() as u64,
            1,
            "table manifest exceeds manifest catalog budget",
        )?;
    }
    let persist_start = crate::observability::perf_trace::start_timer();
    let write_result = service.publish_replace_manifest(branch_id, manifest);
    crate::observability::perf_trace::record_lifecycle_background_publish_manifest_persist(
        crate::observability::perf_trace::timer_elapsed(persist_start),
    );
    write_result.map_err(table_manifest_service_error)
}

pub(crate) fn stage_table_manifest_for_branch(
    branch: &BranchLocalState,
    service: &TableManifestService<'_>,
    reader_service: &TableObjectReaderService<'static>,
    catalog: &LifecycleDurableTableCatalog,
    budget: Option<&StorageBudgetLedger>,
) -> LifecycleResult<LifecycleTableManifestRecoveryStage> {
    let mut staged_branch = branch.clone();
    let mut staged_catalog = catalog.clone();
    let outcome = recover_table_manifest_for_branch(
        &mut staged_branch,
        service,
        reader_service,
        &mut staged_catalog,
        budget,
    )?;
    Ok(LifecycleTableManifestRecoveryStage::new(
        staged_branch,
        staged_catalog,
        outcome,
    ))
}

pub(crate) fn table_manifest_debt_outcome(
    mut outcome: MaintenanceOutcome,
    error: LifecycleError,
) -> MaintenanceOutcome {
    outcome = outcome
        .with_status(MaintenanceOutcomeStatus::Completed)
        .with_reason("table manifest publication needs recovery")
        .with_source_error(error);
    if let Ok(health) = telemetry_health_debt("table manifest publication needs recovery") {
        outcome = outcome.with_recovery_health(health);
    }
    outcome
}

pub(crate) fn recover_table_manifest_for_branch(
    branch: &mut BranchLocalState,
    service: &TableManifestService<'_>,
    reader_service: &TableObjectReaderService<'static>,
    catalog: &mut LifecycleDurableTableCatalog,
    budget: Option<&StorageBudgetLedger>,
) -> LifecycleResult<LifecycleTableManifestRecoveryOutcome> {
    let Some(manifest) = service
        .load_current(branch.branch_id())
        .map_err(table_manifest_service_error)?
    else {
        return Ok(LifecycleTableManifestRecoveryOutcome::absent());
    };
    apply_loaded_table_manifest_to_branch(branch, &manifest, reader_service, catalog, budget)
}

pub(crate) fn apply_loaded_table_manifest_to_branch(
    branch: &mut BranchLocalState,
    manifest: &TableManifest,
    reader_service: &TableObjectReaderService<'static>,
    catalog: &mut LifecycleDurableTableCatalog,
    budget: Option<&StorageBudgetLedger>,
) -> LifecycleResult<LifecycleTableManifestRecoveryOutcome> {
    if manifest.branch_id() != branch.branch_id() {
        return Err(LifecycleError::RecoveryFailed {
            reason: "table manifest branch id does not match branch state",
        });
    }
    let manifest_object =
        crate::layout::ObjectLayout::branch_table_manifest(&branch.branch_id().to_string())
            .map_err(|source| {
                LifecycleError::lower_layer_with(
                    LifecycleLowerLayer::Layout,
                    "table manifest object layout failed",
                    source,
                )
            })?;
    let request = recovery_request_from_manifest(manifest, reader_service, catalog, budget)?;
    let install_outcome = branch
        .install_table_manifest_recovery(request)
        .map_err(branch_error)?;
    catalog.record_recovered_manifest(manifest)?;
    Ok(LifecycleTableManifestRecoveryOutcome::installed(
        manifest_object,
        manifest,
        install_outcome,
    ))
}

fn recovery_request_from_manifest(
    manifest: &TableManifest,
    reader_service: &TableObjectReaderService<'static>,
    catalog: &mut LifecycleDurableTableCatalog,
    budget: Option<&StorageBudgetLedger>,
) -> LifecycleResult<BranchTableManifestRecoveryRequest> {
    // BS4.4j: rebuild each table's summary from the manifest record + the BS4.4g row-split section
    // rather than scanning its (now lazy) rows. The split Vec is flat/global in write order — top-level
    // levels, then each inherited layer, level-order/table-order — so one running cursor threads the
    // recovery walk in lockstep with the writer's push order.
    let splits = decode_table_row_split_extension_section(manifest.extension_sections())
        .map_err(format_error)?
        .unwrap_or_default();
    let mut split_cursor = 0usize;
    let owned_levels = recover_manifest_levels(
        manifest.branch_id(),
        manifest.levels(),
        reader_service,
        catalog,
        budget,
        &splits,
        &mut split_cursor,
    )?;
    let mut inherited_layers = Vec::with_capacity(manifest.inherited_layers().len());
    for layer in manifest.inherited_layers() {
        inherited_layers.push(recover_manifest_inherited_layer(
            layer,
            reader_service,
            catalog,
            budget,
            &splits,
            &mut split_cursor,
        )?);
    }
    if split_cursor != splits.len() {
        return Err(LifecycleError::TableManifestRecoveryMismatch {
            reason: "row-split extension count does not match recovered table count",
            source: None,
        });
    }
    let request = BranchTableManifestRecoveryRequest::new(
        manifest.branch_id(),
        owned_levels,
        inherited_layers,
    )
    .map_err(branch_error)?;
    let retained =
        super::retained_history_extension::RetainedHistoryFacts::from_extension_sections(
            manifest.extension_sections(),
        )
        .map_err(format_error)?;
    Ok(match retained {
        // #3502 Slice C: restore the version floor alongside the timestamp
        // coverage so post-restart `as_of` reads below a pruned floor raise.
        Some(facts) => request
            .with_timestamp_coverage(facts.to_timestamp_coverage())
            .with_retained_history_floor(Some(facts.retained_version_floor)),
        None => request,
    })
}

fn recover_manifest_inherited_layer(
    layer: &TableManifestInheritedLayer,
    reader_service: &TableObjectReaderService<'static>,
    catalog: &mut LifecycleDurableTableCatalog,
    budget: Option<&StorageBudgetLedger>,
    splits: &[TableRowSplit],
    split_cursor: &mut usize,
) -> LifecycleResult<BranchInheritedLayer> {
    let owned_levels = recover_manifest_levels(
        layer.source_branch_id(),
        layer.levels(),
        reader_service,
        catalog,
        budget,
        splits,
        split_cursor,
    )?;
    let table_count = owned_levels.iter().map(Vec::len).sum();
    BranchInheritedLayer::new(
        InheritedLayerDescriptor::new(
            layer.source_branch_id(),
            layer.fork_version(),
            inherited_status_from_manifest(layer.status()),
            table_count,
        ),
        owned_levels,
    )
    .map_err(branch_error)
}

fn recover_manifest_levels(
    branch_id: BranchId,
    levels: &[TableManifestLevel],
    reader_service: &TableObjectReaderService<'static>,
    catalog: &mut LifecycleDurableTableCatalog,
    budget: Option<&StorageBudgetLedger>,
    splits: &[TableRowSplit],
    split_cursor: &mut usize,
) -> LifecycleResult<Vec<Vec<BranchOwnedTable>>> {
    let mut owned_levels = Vec::<Vec<BranchOwnedTable>>::new();
    for level in levels {
        let level_index = usize::from(level.level().raw());
        if owned_levels.len() <= level_index {
            owned_levels.resize_with(level_index + 1, Vec::new);
        }
        // BS4.4j: consume one row-split per table in `level.tables()` order — the exact order the writer
        // pushed them (`manifest_levels_from_owned`) — via the running cursor.
        let mut tables = Vec::with_capacity(level.tables().len());
        for table in level.tables() {
            tables.push(recover_manifest_table(
                branch_id,
                level.level(),
                table,
                reader_service,
                catalog,
                budget,
                splits,
                split_cursor,
            )?);
        }
        owned_levels[level_index] = tables;
    }
    Ok(owned_levels)
}

fn recover_manifest_table(
    branch_id: BranchId,
    level: BranchLevel,
    table: &TableManifestTableRef,
    reader_service: &TableObjectReaderService<'static>,
    catalog: &mut LifecycleDurableTableCatalog,
    budget: Option<&StorageBudgetLedger>,
    splits: &[TableRowSplit],
    split_cursor: &mut usize,
) -> LifecycleResult<BranchOwnedTable> {
    let object_facts = TableObjectFacts::from_table_manifest_ref(table);
    let reader = reader_service
        .open_reader(
            table.table_identity().clone(),
            &object_facts,
            TableReaderConfig::default().deny_runtime_materialization(),
        )
        .map_err(table_read_error)?;
    // BS4.5a: charge the lazy reader's *metadata-resident* footprint (index + properties + filter
    // frame), not the full encoded object. A durable dataset is no longer bounded by RAM — only
    // reader metadata + caches + memtables are. Charged after the O(metadata) open so admission
    // uses the same `resident_size_bytes()` seam as the DB-wide runtime total.
    if let Some(budget) = budget {
        require_table_reader_budget(
            budget,
            reader.resident_size_bytes(),
            "table manifest recovery reader exceeds storage budget",
        )?;
    }
    // BS4.4j: consume this table's row-split (in walk order — the writer's push order) BEFORE
    // validation, so the single verification scan can release-check the put/tombstone counts alongside
    // the timestamp/physical bounds. Without this the counts would be trusted from the persisted split
    // on release builds (only the install-time debug oracle cross-checks them).
    let split = splits
        .get(*split_cursor)
        .ok_or(LifecycleError::TableManifestRecoveryMismatch {
            reason: "row-split extension is shorter than the recovered table count",
            source: None,
        })?;
    *split_cursor += 1;
    validate_manifest_reader_facts(table, split, &reader)?;
    catalog.record_table_with_provenance(
        table.table_identity().clone(),
        object_facts,
        table.provenance().clone(),
    )?;
    // Build the summary from the manifest record + the (now count-validated) positional row-split.
    let extras = TableSummaryExtras::from_parts(
        table.facts().timestamp_min(),
        table.facts().timestamp_max(),
        table.bounds().physical_first().to_vec(),
        table.bounds().physical_last().to_vec(),
        split.put_rows(),
        split.tombstone_rows(),
    );
    branch_table_from_reader(branch_id, level, table.provenance(), reader, extras)
}

fn branch_table_from_reader(
    branch_id: BranchId,
    level: BranchLevel,
    provenance: &TableManifestTableProvenance,
    reader: ImmutableTableReader<'static>,
    extras: TableSummaryExtras,
) -> LifecycleResult<BranchOwnedTable> {
    let identity = reader.facts().identity().clone();
    let descriptor = BranchTableDescriptor::new(identity, reader.facts().clone(), level)
        .map_err(branch_error)?;
    match provenance {
        TableManifestTableProvenance::MaterializationReplacement {
            source_branch_id,
            fork_version,
        } => BranchOwnedTable::new_materialization_replacement(
            branch_id,
            descriptor,
            reader,
            extras,
            BranchMaterializationSource::new(*source_branch_id, *fork_version),
        )
        .map_err(branch_error),
        TableManifestTableProvenance::Flush
        | TableManifestTableProvenance::SnapshotInstall
        | TableManifestTableProvenance::Compaction
        | TableManifestTableProvenance::Recovered => {
            BranchOwnedTable::new(branch_id, descriptor, reader, extras).map_err(branch_error)
        }
    }
}

/// #3502 Slice C: the retained-version floor to persist in the manifest — the
/// pruning floor Slice B published (the lowest retained version), else the
/// branch's max commit version as a defensive fallback for any narrowing not
/// driven by a version prune, else zero. Persisting `max_commit_version` when a
/// real floor exists would over-reject on restore (it would raise `as_of` reads
/// of still-retained versions). Returned as a bare `CommitVersion` — not the
/// `LifecycleResult` alias — so the mutation gate can judge the choice.
fn manifest_retained_version_floor(branch: &BranchLocalState) -> strata_core::CommitVersion {
    branch
        .retained_history_floor()
        .or_else(|| branch.max_commit_version())
        .unwrap_or(strata_core::CommitVersion::ZERO)
}

fn manifest_levels_from_owned(
    owned_levels: &[Vec<BranchOwnedTable>],
    catalog: &LifecycleDurableTableCatalog,
    row_splits: &mut Vec<TableRowSplit>,
) -> LifecycleResult<Vec<TableManifestLevel>> {
    let mut levels = Vec::new();
    for (level_index, tables) in owned_levels.iter().enumerate() {
        if tables.is_empty() {
            continue;
        }
        let level = BranchLevel::new(u8::try_from(level_index).map_err(|_| {
            LifecycleError::TableManifestPublicationFailed {
                reason: "branch level index does not fit table manifest level",
                source: None,
            }
        })?);
        // Push each table's split and its manifest record in lockstep, so `row_splits` stays
        // positionally aligned with the manifest tables (the BS4.4g extension is keyed by order).
        // Coupling: these `row_splits` are collected in this pre-canonicalization loop order, while
        // `TableManifestLevel::new` re-sorts `manifest_tables` (`canonicalize_level_tables`). Alignment
        // therefore relies on that sort being a no-op — which is enforced: `validate_level_tables`
        // requires `table.order() == index` afterward, and `order` is this loop index, so any reorder
        // fails the build rather than silently desyncing the split array from the table array.
        let mut manifest_tables = Vec::with_capacity(tables.len());
        for (order, table) in tables.iter().enumerate() {
            let extras = table.extras();
            row_splits.push(TableRowSplit::new(
                extras.put_rows(),
                extras.tombstone_rows(),
            ));
            manifest_tables.push(table_ref_from_branch_table(table, order, catalog)?);
        }
        levels.push(TableManifestLevel::new(level, manifest_tables).map_err(format_error)?);
    }
    Ok(levels)
}

fn manifest_inherited_layers(
    inherited_layers: &[BranchInheritedLayer],
    catalog: &LifecycleDurableTableCatalog,
    row_splits: &mut Vec<TableRowSplit>,
) -> LifecycleResult<Vec<TableManifestInheritedLayer>> {
    let mut result = Vec::with_capacity(inherited_layers.len());
    for (order, layer) in inherited_layers.iter().enumerate() {
        let levels = manifest_levels_from_owned(layer.owned_levels(), catalog, row_splits)?;
        result.push(
            TableManifestInheritedLayer::new(
                u32::try_from(order).map_err(|_| {
                    LifecycleError::TableManifestPublicationFailed {
                        reason: "inherited layer order does not fit table manifest",
                        source: None,
                    }
                })?,
                layer.source_branch_id(),
                None,
                layer.fork_version(),
                manifest_status_from_inherited(layer.status()),
                levels,
            )
            .map_err(format_error)?,
        );
    }
    Ok(result)
}

fn table_ref_from_branch_table(
    table: &BranchOwnedTable,
    order: usize,
    catalog: &LifecycleDurableTableCatalog,
) -> LifecycleResult<TableManifestTableRef> {
    let entry = catalog.entry_for(table.descriptor().identity())?;
    validate_catalog_entry_matches_table(entry, table.facts())?;
    let facts = table.facts();
    let extras = table.extras();
    // Read the per-table summary cached at seal time instead of rescanning the
    // table's rows. Internal-key bounds come from the table facts' key range;
    // timestamp and physical-key bounds come from the cached summary. The values
    // are identical to a full row scan (recovery validation cross-checks them),
    // so the manifest bytes are unchanged.
    let manifest_facts = TableManifestTableFacts::new(
        facts.byte_count(),
        facts.row_count(),
        facts.data_block_count(),
        facts.commit_range().min(),
        facts.commit_range().max(),
        extras.timestamp_min(),
        extras.timestamp_max(),
    )
    .map_err(format_error)?;
    let manifest_bounds = TableManifestTableBounds::new(
        extras.physical_key_min().to_vec(),
        extras.physical_key_max().to_vec(),
        facts.key_range().first_key().to_vec(),
        facts.key_range().last_key().to_vec(),
    )
    .map_err(format_error)?;
    TableManifestTableRef::new(
        table.descriptor().identity().clone(),
        entry.object_facts.object().clone(),
        u32::try_from(order).map_err(|_| LifecycleError::TableManifestPublicationFailed {
            reason: "table order does not fit table manifest",
            source: None,
        })?,
        manifest_facts,
        manifest_bounds,
        manifest_table_provenance(table, entry)?,
    )
    .map_err(format_error)
}

fn validate_catalog_entry_matches_table(
    entry: &LifecycleDurableTableCatalogEntry,
    facts: &TableRuntimeFacts,
) -> LifecycleResult<()> {
    if entry.identity != *facts.identity()
        || entry.object_facts.byte_count() != facts.byte_count()
        || entry.object_facts.row_count() != facts.row_count()
        || entry.object_facts.data_block_count() != facts.data_block_count()
        || entry.object_facts.commit_min() != facts.commit_range().min()
        || entry.object_facts.commit_max() != facts.commit_range().max()
    {
        return Err(LifecycleError::TableManifestPublicationFailed {
            reason: "catalog table object facts do not match reachable table",
            source: None,
        });
    }
    Ok(())
}

fn manifest_table_provenance(
    table: &BranchOwnedTable,
    entry: &LifecycleDurableTableCatalogEntry,
) -> LifecycleResult<TableManifestTableProvenance> {
    if let Some(source) = table.materialization_source() {
        TableManifestTableProvenance::materialization_replacement(
            source.source_branch_id(),
            source.fork_version(),
        )
        .map_err(format_error)
    } else {
        Ok(entry.provenance.clone())
    }
}

fn validate_manifest_reader_facts(
    table: &TableManifestTableRef,
    split: &TableRowSplit,
    reader: &ImmutableTableReader,
) -> LifecycleResult<()> {
    // BS4.5b: fast open is O(tables), not O(rows). Verify the manifest record against the table
    // *footer* — byte/row/block counts, commit range, and internal-key bounds are all metadata, so this
    // reads no data block. The row-derived fields the footer lacks (timestamp bounds, physical-key
    // bounds, and the exact put/tombstone breakdown) are trusted from the CRC-protected manifest record
    // here — they feed `TableSummaryExtras::from_parts` at recovery — and cross-checked against the
    // actual rows only under the debug oracle below. The one release guard the footer still gives the
    // split is that its counts sum to the object's row count. Before BS4.5b this did a full O(rows)
    // cursor pass per table, making open O(dataset).

    // Footer counts + commit range vs the manifest record. The timestamp bounds are taken from the
    // manifest itself (trusted; verified against rows only in debug), so the comparison exercises only
    // the footer-backed fields.
    let facts = TableManifestTableFacts::new(
        reader.facts().byte_count(),
        reader.facts().row_count(),
        reader.facts().data_block_count(),
        reader.facts().commit_range().min(),
        reader.facts().commit_range().max(),
        table.facts().timestamp_min(),
        table.facts().timestamp_max(),
    )
    .map_err(format_error)?;
    if &facts != table.facts() {
        return Err(LifecycleError::TableManifestRecoveryMismatch {
            reason: "table manifest facts do not match table object",
            source: None,
        });
    }

    // The row-split counts must sum to the object's row count (footer). The exact put/tombstone
    // breakdown is verified against rows only in debug.
    if split.put_rows().saturating_add(split.tombstone_rows()) != reader.facts().row_count() {
        return Err(LifecycleError::TableManifestRecoveryMismatch {
            reason: "table row-split counts do not match table object",
            source: None,
        });
    }

    // Footer internal-key bounds vs the manifest record. The physical-key bounds are taken from the
    // manifest itself (trusted; verified against rows only in debug).
    let bounds = TableManifestTableBounds::new(
        table.bounds().physical_first().to_vec(),
        table.bounds().physical_last().to_vec(),
        reader.facts().key_range().first_key().to_vec(),
        reader.facts().key_range().last_key().to_vec(),
    )
    .map_err(format_error)?;
    if &bounds != table.bounds() {
        return Err(LifecycleError::TableManifestRecoveryMismatch {
            reason: "table manifest bounds do not match table object",
            source: None,
        });
    }
    #[cfg(debug_assertions)]
    {
        // BS4.5b oracle: verify the trusted manifest fields (timestamp bounds, physical-key bounds, and
        // the exact put/tombstone split) against a full scan. The manifest-derived summary is exactly
        // what recovery installs via `from_parts`; a `from_rows` scan of the same sealed table must
        // yield a byte-identical summary. Reads via the oracle hatch (bypasses the BS4.4d guard);
        // release open never does this.
        let scanned_rows = reader
            .materialize_rows_for_oracle()
            .map_err(|_| manifest_reader_scan_failed())?;
        let manifest_extras = TableSummaryExtras::from_parts(
            table.facts().timestamp_min(),
            table.facts().timestamp_max(),
            table.bounds().physical_first().to_vec(),
            table.bounds().physical_last().to_vec(),
            split.put_rows(),
            split.tombstone_rows(),
        );
        let scanned_extras = TableSummaryExtras::from_rows(&scanned_rows)
            .map_err(|_| manifest_reader_scan_failed())?;
        debug_assert_eq!(
            manifest_extras, scanned_extras,
            "manifest-trusted summary (timestamp/physical bounds, put/tombstone split) diverged from a full scan",
        );
    }
    Ok(())
}

fn manifest_reader_scan_failed() -> LifecycleError {
    LifecycleError::TableManifestRecoveryMismatch {
        reason: "table manifest verify cursor scan failed",
        source: None,
    }
}

fn manifest_table_refs(manifest: &TableManifest) -> impl Iterator<Item = &TableManifestTableRef> {
    manifest
        .levels()
        .iter()
        .flat_map(TableManifestLevel::tables)
        .chain(
            manifest
                .inherited_layers()
                .iter()
                .flat_map(TableManifestInheritedLayer::levels)
                .flat_map(TableManifestLevel::tables),
        )
}

/// Every table object name a manifest references (owned levels + inherited layers) — the
/// unit of the #2553 frontier sets.
pub(crate) fn manifest_object_names(manifest: &TableManifest) -> Arc<BTreeSet<ObjectName>> {
    Arc::new(
        manifest_table_refs(manifest)
            .map(|table| table.object().clone())
            .collect(),
    )
}

const fn manifest_status_from_inherited(
    status: InheritedLayerStatus,
) -> TableManifestInheritedLayerStatus {
    match status {
        InheritedLayerStatus::Active => TableManifestInheritedLayerStatus::Active,
        InheritedLayerStatus::Materializing => TableManifestInheritedLayerStatus::Materializing,
        InheritedLayerStatus::Materialized => TableManifestInheritedLayerStatus::Materialized,
        InheritedLayerStatus::Unavailable => TableManifestInheritedLayerStatus::Unavailable,
    }
}

const fn inherited_status_from_manifest(
    status: TableManifestInheritedLayerStatus,
) -> InheritedLayerStatus {
    match status {
        TableManifestInheritedLayerStatus::Active => InheritedLayerStatus::Active,
        TableManifestInheritedLayerStatus::Materializing => InheritedLayerStatus::Materializing,
        TableManifestInheritedLayerStatus::Materialized => InheritedLayerStatus::Materialized,
        TableManifestInheritedLayerStatus::Unavailable => InheritedLayerStatus::Unavailable,
    }
}

fn format_error(error: FormatError) -> LifecycleError {
    LifecycleError::lower_layer_with(
        LifecycleLowerLayer::Format,
        "table manifest format failed",
        error,
    )
}

fn table_manifest_service_error(error: ManifestServiceError) -> LifecycleError {
    let reason = match &error {
        ManifestServiceError::Publish {
            role: ManifestRole::Table,
            source,
        } if matches!(
            source.kind(),
            PublishFailureKind::VisibilityUnknown
                | PublishFailureKind::VisibleDurabilityUnconfirmed
        ) =>
        {
            return LifecycleError::table_manifest_publication_uncertain_with(
                table_manifest_publish_reason(source),
                error,
            );
        }
        ManifestServiceError::Publish {
            role: ManifestRole::Table,
            source,
        } => table_manifest_publish_reason(source),
        ManifestServiceError::Missing {
            role: ManifestRole::Table,
            ..
        } => "table manifest missing",
        ManifestServiceError::Decode {
            role: ManifestRole::Table,
            ..
        } => "table manifest decode failed",
        ManifestServiceError::Encode {
            role: ManifestRole::Table,
            ..
        } => "table manifest encode failed",
        ManifestServiceError::Read {
            role: ManifestRole::Table,
            ..
        } => "table manifest read failed",
        ManifestServiceError::Layout {
            role: ManifestRole::Table,
            ..
        } => "table manifest layout failed",
        ManifestServiceError::InvalidPublishMetadata {
            role: ManifestRole::Table,
            ..
        } => "table manifest publish metadata invalid",
        ManifestServiceError::InvalidRecoveryFact {
            role: ManifestRole::Table,
            ..
        } => "table manifest recovery fact invalid",
        _ => "manifest service role mismatch",
    };
    match error {
        ManifestServiceError::Decode { .. } | ManifestServiceError::InvalidRecoveryFact { .. } => {
            LifecycleError::table_manifest_recovery_mismatch_with(reason, error)
        }
        ManifestServiceError::Publish { .. } => {
            LifecycleError::table_manifest_publication_failed_with(reason, error)
        }
        other => LifecycleError::lower_layer_with(LifecycleLowerLayer::Service, reason, other),
    }
}

fn table_manifest_publish_reason(error: &PublishError) -> &'static str {
    match error.kind() {
        PublishFailureKind::Unsupported => "table manifest publish unsupported",
        PublishFailureKind::PreconditionFailed => "table manifest publish precondition failed",
        PublishFailureKind::FailedBeforeVisibility => {
            "table manifest publish failed before visibility"
        }
        PublishFailureKind::VisibilityUnknown => "table manifest publish visibility unknown",
        PublishFailureKind::VisibleDurabilityUnconfirmed => {
            "table manifest publish durability unconfirmed"
        }
    }
}

fn table_read_error(error: TableObjectReadError) -> LifecycleError {
    match error {
        TableObjectReadError::Backend { object, source }
            if source.kind() == BackendErrorKind::NotFound =>
        {
            LifecycleError::table_manifest_recovery_mismatch_with(
                "table manifest listed table object is missing",
                TableObjectReadError::Backend { object, source },
            )
        }
        TableObjectReadError::FactMismatch { .. } => {
            LifecycleError::table_manifest_recovery_mismatch_with(
                "table manifest facts do not match table object",
                error,
            )
        }
        TableObjectReadError::Source { .. } | TableObjectReadError::Table { .. } => {
            LifecycleError::table_manifest_recovery_mismatch_with(
                "table manifest listed table object failed validation",
                error,
            )
        }
        other => LifecycleError::lower_layer_with(
            LifecycleLowerLayer::Service,
            "table manifest listed table object read failed",
            other,
        ),
    }
}

fn branch_error(error: impl std::error::Error + Send + Sync + 'static) -> LifecycleError {
    LifecycleError::table_manifest_branch_install_failed_with(
        "table manifest branch recovery failed",
        error,
    )
}
