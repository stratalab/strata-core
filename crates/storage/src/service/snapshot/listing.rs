use super::{
    require_capability, validate_snapshot_id, SnapshotService, SnapshotServiceError,
    SnapshotServiceResult,
};
use crate::backend::{
    Backend, BackendCapability, BackendError, BackendErrorKind, DeleteDurability, DeleteError,
    DeleteOutcome,
};
use crate::layout::{ObjectLayout, SnapshotObjectClassification};
use crate::object::ObjectName;
use crate::service::{durable_cleanup_failure, durable_cleanup_succeeded};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SnapshotObject {
    snapshot_id: u64,
    object: ObjectName,
}

impl SnapshotObject {
    const fn new(snapshot_id: u64, object: ObjectName) -> Self {
        Self {
            snapshot_id,
            object,
        }
    }

    pub(crate) const fn snapshot_id(&self) -> u64 {
        self.snapshot_id
    }

    pub(crate) const fn object(&self) -> &ObjectName {
        &self.object
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SnapshotDeleteFailure {
    snapshot: SnapshotObject,
    source: DeleteError,
}

impl SnapshotDeleteFailure {
    const fn new(snapshot: SnapshotObject, source: DeleteError) -> Self {
        Self { snapshot, source }
    }

    pub(crate) const fn snapshot(&self) -> &SnapshotObject {
        &self.snapshot
    }

    pub(crate) const fn source(&self) -> &BackendError {
        self.source.source_error()
    }

    pub(crate) const fn delete_error(&self) -> &DeleteError {
        &self.source
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SnapshotDeleteOutcome {
    snapshot: SnapshotObject,
    outcome: DeleteOutcome,
}

impl SnapshotDeleteOutcome {
    const fn new(snapshot: SnapshotObject, outcome: DeleteOutcome) -> Self {
        Self { snapshot, outcome }
    }

    pub(crate) const fn snapshot(&self) -> &SnapshotObject {
        &self.snapshot
    }

    pub(crate) const fn outcome(&self) -> &DeleteOutcome {
        &self.outcome
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct SnapshotDeleteReport {
    deleted: Vec<SnapshotObject>,
    delete_outcomes: Vec<SnapshotDeleteOutcome>,
    protected: Vec<SnapshotObject>,
    failed: Vec<SnapshotDeleteFailure>,
}

impl SnapshotDeleteReport {
    fn record_deleted(&mut self, snapshot: SnapshotObject, outcome: DeleteOutcome) {
        self.delete_outcomes
            .push(SnapshotDeleteOutcome::new(snapshot.clone(), outcome));
        self.deleted.push(snapshot);
    }

    fn record_protected(&mut self, snapshot: SnapshotObject) {
        self.protected.push(snapshot);
    }

    fn record_failed(&mut self, snapshot: SnapshotObject, source: DeleteError) {
        self.failed
            .push(SnapshotDeleteFailure::new(snapshot, source));
    }

    pub(crate) fn deleted(&self) -> &[SnapshotObject] {
        &self.deleted
    }

    pub(crate) fn delete_outcomes(&self) -> &[SnapshotDeleteOutcome] {
        &self.delete_outcomes
    }

    pub(crate) fn protected(&self) -> &[SnapshotObject] {
        &self.protected
    }

    pub(crate) fn failed(&self) -> &[SnapshotDeleteFailure] {
        &self.failed
    }
}

/// Space-reclamation contract §3.4 (slice 5, #3592): which snapshot objects a
/// prune deletes. Every mode protects the manifest-attested (live) snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SnapshotPruneMode {
    /// The explicit caller verb: keep the newest N objects (and the live one).
    RetainNewest,
    /// After a completed checkpoint: delete every object below the live id.
    /// An id at or above the live id is never touched — it may be a publish in
    /// flight whose manifest re-point has not landed yet.
    Superseded,
    /// Queued at open: delete every object other than the attested one,
    /// reclaiming a crash orphan (a newer id the manifest never attested) as
    /// well as the superseded ones. Safe against a checkpoint in the same
    /// session because a checkpoint writes its snapshot and re-points the
    /// manifest under the runtime lock the prune also runs under — an
    /// unattested object is never a publish in flight, only a crash's leftover.
    ReconcileToAttested,
}

/// `Superseded`: an object strictly below the live id is dead — the live
/// snapshot supersedes it and recovery loads only the attested id.
pub(crate) const fn superseded_snapshot(snapshot_id: u64, live_snapshot_id: u64) -> bool {
    snapshot_id < live_snapshot_id
}

/// `ReconcileToAttested`: any object other than the attested one is dead once
/// no publish can be in flight (the open reclaim window).
pub(crate) const fn reconcilable_orphan(snapshot_id: u64, attested_snapshot_id: u64) -> bool {
    snapshot_id != attested_snapshot_id
}

impl SnapshotService<'_> {
    pub(crate) fn list_snapshots(&self) -> SnapshotServiceResult<Vec<SnapshotObject>> {
        list_snapshot_objects(&self.backend)
    }

    pub(crate) fn latest_snapshot(&self) -> SnapshotServiceResult<Option<SnapshotObject>> {
        Ok(self.list_snapshots()?.into_iter().next_back())
    }

    /// Space-reclamation contract §3.5 (slice 2, audit tier): every listed
    /// snapshot object with its on-disk size. An object that vanishes between
    /// the listing and its stat (a concurrent prune) is dropped.
    pub(crate) fn list_snapshot_sizes(&self) -> SnapshotServiceResult<Vec<(SnapshotObject, u64)>> {
        require_capability(&self.backend, BackendCapability::ObjectMetadata)?;
        let mut sizes = Vec::new();
        for snapshot in self.list_snapshots()? {
            match self.backend.object_metadata(snapshot.object()) {
                Ok(metadata) => sizes.push((snapshot, metadata.size_bytes())),
                Err(source) if source.kind() == BackendErrorKind::NotFound => {}
                Err(source) => return Err(SnapshotServiceError::List { source }),
            }
        }
        Ok(sizes)
    }

    /// The explicit caller verb: newest-N with live protection. The mode-aware
    /// entry `prune_snapshots_with_mode` is the single implementation.
    pub(crate) fn prune_snapshots(
        &self,
        live_snapshot_id: Option<u64>,
        retain_newest: usize,
    ) -> SnapshotServiceResult<SnapshotDeleteReport> {
        self.prune_snapshots_with_mode(
            live_snapshot_id,
            SnapshotPruneMode::RetainNewest,
            retain_newest,
        )
    }

    pub(crate) fn prune_snapshots_with_mode(
        &self,
        live_snapshot_id: Option<u64>,
        mode: SnapshotPruneMode,
        retain_newest: usize,
    ) -> SnapshotServiceResult<SnapshotDeleteReport> {
        if let Some(snapshot_id) = live_snapshot_id {
            validate_snapshot_id(snapshot_id)?;
        }
        require_capability(&self.backend, BackendCapability::DeleteObject)?;

        let snapshots = self.list_snapshots()?;
        let retain_newest = retain_newest.max(1);
        let retain_start = snapshots.len().saturating_sub(retain_newest);
        let mut report = SnapshotDeleteReport::default();

        for (index, snapshot) in snapshots.into_iter().enumerate() {
            let live = live_snapshot_id == Some(snapshot.snapshot_id());
            // The newest-window verb keeps its shape with or without a live id.
            // The proof-driven modes can prove nothing dead without one, so they
            // protect everything then (the lifecycle proof never admits that
            // shape, but the service must not depend on it).
            let protected = match mode {
                SnapshotPruneMode::RetainNewest => live || index >= retain_start,
                SnapshotPruneMode::Superseded => live_snapshot_id
                    .is_none_or(|live_id| !superseded_snapshot(snapshot.snapshot_id(), live_id)),
                SnapshotPruneMode::ReconcileToAttested => live_snapshot_id
                    .is_none_or(|attested| !reconcilable_orphan(snapshot.snapshot_id(), attested)),
            };
            if protected {
                report.record_protected(snapshot);
                continue;
            }

            // Pruning executes caller-supplied retention intent. A per-object
            // delete failure must not hide deletes that already succeeded or
            // snapshots that were explicitly protected.
            match self.backend.delete_object(snapshot.object()) {
                Ok(outcome) if durable_cleanup_succeeded(&outcome) => {
                    report.record_deleted(snapshot, outcome);
                }
                Ok(outcome) => report.record_failed(snapshot, durable_cleanup_failure(&outcome)),
                Err(source) if source.source_error().kind() == BackendErrorKind::NotFound => {
                    let outcome = DeleteOutcome::already_missing(
                        snapshot.object().clone(),
                        DeleteDurability::NonDurable,
                    );
                    report.record_deleted(snapshot, outcome);
                }
                Err(source) => report.record_failed(snapshot, source),
            }
        }

        Ok(report)
    }
}

fn snapshot_prefix() -> SnapshotServiceResult<crate::object::ObjectPrefix> {
    ObjectLayout::snapshot_prefix().map_err(|source| SnapshotServiceError::Layout { source })
}

fn list_snapshot_objects(backend: &dyn Backend) -> SnapshotServiceResult<Vec<SnapshotObject>> {
    require_capability(backend, BackendCapability::ListPrefix)?;
    let prefix = snapshot_prefix()?;
    let mut snapshots = Vec::new();
    for object in backend
        .list_prefix(&prefix)
        .map_err(|source| SnapshotServiceError::List { source })?
    {
        if let Some(snapshot) = parse_snapshot_object(object)? {
            snapshots.push(snapshot);
        }
    }
    snapshots.sort_by_key(SnapshotObject::snapshot_id);
    Ok(snapshots)
}

fn parse_snapshot_object(object: ObjectName) -> SnapshotServiceResult<Option<SnapshotObject>> {
    let snapshot_id = match ObjectLayout::classify_snapshot_object(&object) {
        Ok(Some(SnapshotObjectClassification::Snapshot { snapshot_id })) => snapshot_id,
        Ok(None) => {
            // Some object stores expose weak prefix behavior. Objects outside
            // the snapshot family are ignored, but malformed names inside the
            // family fail closed because they can represent ambiguous recovery
            // state.
            return Ok(None);
        }
        Err(_) => return Err(invalid_listed_snapshot_object(object)),
    };
    if snapshot_id == 0 {
        return Err(invalid_listed_snapshot_object(object));
    }
    Ok(Some(SnapshotObject::new(snapshot_id, object)))
}

fn invalid_listed_snapshot_object(object: ObjectName) -> SnapshotServiceError {
    SnapshotServiceError::InvalidListedObject {
        object,
        source: BackendError::new(
            BackendErrorKind::InvalidObjectName,
            "invalid snapshot object name in listing",
        ),
    }
}
