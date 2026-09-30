//! STH-3b write-ordering watchdog.
//!
//! The `SQLite` bug class this guards against: a database write that reaches
//! disk before the journal sync it depends on. Strata's equivalent invariant:
//! no manifest, snapshot, or table object may be *published* (made durably
//! visible) while a WAL segment still holds appended bytes that were never
//! synced — a crash in that window recovers dependent state whose WAL basis
//! is gone. The engine enforces sync-before-publish; this decorator is the
//! regression tripwire that proves it on the real operation stream.
//!
//! The watchdog is a pure observer: it records a logical order for every
//! append/sync/publish/delete, tracks each WAL segment's unsynced boundary,
//! and files a typed [`WriteOrderingViolation`] at every checked publish that
//! happens over unsynced WAL bytes. It never alters an operation's outcome.
//!
//! What a byte-level observer can and cannot decide (#2785): at publish time,
//! "publish depends on unsynced bytes" and "publish concurrent with an
//! unrelated in-flight append tail" present the *same* event shape — a sync
//! clears a segment's whole boundary, so dependency is invisible in the byte
//! stream. The two shapes differ only in the future: an in-flight tail gets a
//! covering durability event moments later; a dependency-violating tail never
//! does (a crash in the window loses it). So a flagged publish is held
//! *pending* and exonerated when a later sync or authorized whole-segment
//! publish covers the bytes it raced; discarding those bytes first (delete,
//! replace, shorter rewrite) — or the stream ending without covering them —
//! confirms the violation. The checked property is exactly the crash-harmful
//! shape: no dependent publish survives over WAL bytes that never became
//! durable. Steady-state dependency ordering (sync strictly *before* the
//! dependent publish begins) is the engine's own gate (BS5.2 flush covers
//! only rows at or below the visible watermark, which advances only after its
//! covering group sync) and is exercised by the deterministic fault lane in
//! `crates/storage/tests/write_ordering.rs`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use crate::backend::{
    Backend, BackendAppend, BackendCapabilities, BackendFence, BackendMetadata, BackendRange,
    BackendResult, BackendWriterGuard, DeleteResult, PublishMode, PublishOutcome, PublishResult,
};
use crate::layout::{ObjectFamily, ObjectLayout};
use crate::object::{ObjectName, ObjectPrefix};

/// A dependent publish observed while a WAL segment held unsynced bytes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteOrderingViolation {
    published_object: String,
    published_family: CheckedPublishFamily,
    publish_order: u64,
    wal_object: String,
    unsynced_bytes: u64,
    last_append_order: u64,
    last_sync_order: Option<u64>,
}

impl WriteOrderingViolation {
    #[must_use]
    pub fn published_object(&self) -> &str {
        &self.published_object
    }
    #[must_use]
    pub const fn published_family(&self) -> CheckedPublishFamily {
        self.published_family
    }
    #[must_use]
    pub const fn publish_order(&self) -> u64 {
        self.publish_order
    }
    #[must_use]
    pub fn wal_object(&self) -> &str {
        &self.wal_object
    }
    #[must_use]
    pub const fn unsynced_bytes(&self) -> u64 {
        self.unsynced_bytes
    }
    #[must_use]
    pub const fn last_append_order(&self) -> u64 {
        self.last_append_order
    }
    #[must_use]
    pub const fn last_sync_order(&self) -> Option<u64> {
        self.last_sync_order
    }
}

/// The object families whose publishes depend on WAL durability.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CheckedPublishFamily {
    Manifest,
    Snapshot,
    Table,
}

impl CheckedPublishFamily {
    fn classify(name: &ObjectName) -> Option<Self> {
        match ObjectFamily::from_object_name(name)? {
            ObjectFamily::Manifest => Some(Self::Manifest),
            ObjectFamily::Snapshots => Some(Self::Snapshot),
            ObjectFamily::Tables => Some(Self::Table),
            _ => None,
        }
    }
}

/// Aggregate facts from a watchdog run, for non-vacuity assertions: a green
/// run that checked no publishes or saw no WAL appends proves nothing.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct WriteOrderingReport {
    wal_segment_appends: usize,
    wal_segment_syncs: usize,
    wal_segment_publishes: usize,
    publishes_checked: usize,
    exonerated_publishes: usize,
    violations: Vec<WriteOrderingViolation>,
}

impl WriteOrderingReport {
    #[must_use]
    pub const fn wal_segment_appends(&self) -> usize {
        self.wal_segment_appends
    }
    #[must_use]
    pub const fn wal_segment_syncs(&self) -> usize {
        self.wal_segment_syncs
    }
    #[must_use]
    pub const fn wal_segment_publishes(&self) -> usize {
        self.wal_segment_publishes
    }
    /// Events that made WAL bytes durable: explicit syncs plus authorized
    /// whole-segment publishes (the buffered WAL's rewrite path).
    #[must_use]
    pub const fn wal_durability_events(&self) -> usize {
        self.wal_segment_syncs + self.wal_segment_publishes
    }
    #[must_use]
    pub const fn publishes_checked(&self) -> usize {
        self.publishes_checked
    }
    /// Pending publish-over-unsynced-bytes findings (one per raced segment)
    /// that a later durability event covered — observed concurrency, not a
    /// violation.
    #[must_use]
    pub const fn exonerated_publishes(&self) -> usize {
        self.exonerated_publishes
    }
    #[must_use]
    pub fn violations(&self) -> &[WriteOrderingViolation] {
        &self.violations
    }
}

/// Per-WAL-segment durability boundary in logical order.
#[derive(Clone, Copy, Debug, Default)]
struct WalBoundary {
    appended_bytes: u64,
    synced_bytes: u64,
    last_append_order: u64,
    last_sync_order: Option<u64>,
}

/// A checked publish observed over unsynced WAL bytes, awaiting its verdict:
/// a later durability event reaching `durable_mark` proves the bytes were an
/// in-flight tail (exonerated); the bytes being discarded first — or never
/// becoming durable — confirms the violation.
#[derive(Debug)]
struct PendingViolation {
    violation: WriteOrderingViolation,
    durable_mark: u64,
}

#[derive(Debug, Default)]
struct WatchdogState {
    next_order: u64,
    wal_segments: HashMap<String, WalBoundary>,
    pending: Vec<PendingViolation>,
    report: WriteOrderingReport,
}

impl WatchdogState {
    fn order(&mut self) -> u64 {
        self.next_order += 1;
        self.next_order
    }

    /// A durability event made `name`'s content durable through
    /// `durable_bytes`: exonerate every pending publish whose raced bytes it
    /// covers.
    fn settle_pending(&mut self, name: &str, durable_bytes: u64) {
        let before = self.pending.len();
        self.pending.retain(|pending| {
            pending.violation.wal_object != name || pending.durable_mark > durable_bytes
        });
        self.report.exonerated_publishes += before - self.pending.len();
    }

    /// `name`'s unsynced content was discarded (deleted or replaced) before a
    /// durability event covered it: every pending publish over it depended on
    /// bytes that provably never became durable.
    fn confirm_pending(&mut self, name: &str) {
        let (confirmed, kept): (Vec<_>, Vec<_>) = std::mem::take(&mut self.pending)
            .into_iter()
            .partition(|pending| pending.violation.wal_object == name);
        self.pending = kept;
        self.report
            .violations
            .extend(confirmed.into_iter().map(|pending| pending.violation));
    }
}

/// A `Backend` decorator that asserts the write-ordering invariant over the
/// real operation stream. Test / `fault-injection`-only.
#[derive(Clone, Debug)]
pub struct WriteOrderingWatchdog<B> {
    inner: B,
    state: Arc<Mutex<WatchdogState>>,
}

impl<B> WriteOrderingWatchdog<B> {
    pub fn new(inner: B) -> Self {
        Self {
            inner,
            state: Arc::new(Mutex::new(WatchdogState::default())),
        }
    }

    #[must_use]
    pub fn inner(&self) -> &B {
        &self.inner
    }

    /// The run's aggregate facts and every violation standing at this moment:
    /// confirmed ones plus pending ones whose raced bytes have not become
    /// durable yet. Take the report only after the stream quiesces — a clean
    /// close syncs every tail, so anything still pending never became durable.
    #[must_use]
    pub fn report(&self) -> WriteOrderingReport {
        let state = self.lock();
        let mut report = state.report.clone();
        report.violations.extend(
            state
                .pending
                .iter()
                .map(|pending| pending.violation.clone()),
        );
        report
    }

    /// CONFIRMED violations only — publishes whose raced bytes were later
    /// discarded (the direction with no innocent explanation). Safe to take
    /// mid-stream or on a faulted/crashed run that never quiesces cleanly:
    /// pending entries are excluded because an in-flight append tail is
    /// indistinguishable from a dependency violation until the future settles
    /// it (#2785).
    #[must_use]
    pub fn confirmed_report(&self) -> WriteOrderingReport {
        self.lock().report.clone()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, WatchdogState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn is_wal_segment(name: &ObjectName) -> bool {
        // Sidecar metadata also lives under wal/ but is an optional fallback
        // artifact, not the record of truth — only segments carry the
        // ordering dependency.
        ObjectLayout::has_wal_segment_prefix(name)
            && !ObjectLayout::has_wal_segment_metadata_prefix(name)
    }

    fn record_wal_append(&self, name: &ObjectName, bytes: u64) {
        if !Self::is_wal_segment(name) {
            return;
        }
        let mut state = self.lock();
        let order = state.order();
        let boundary = state
            .wal_segments
            .entry(name.as_str().to_owned())
            .or_default();
        boundary.appended_bytes += bytes;
        boundary.last_append_order = order;
        state.report.wal_segment_appends += 1;
    }

    fn record_wal_sync(&self, name: &ObjectName) {
        if !Self::is_wal_segment(name) {
            return;
        }
        let mut state = self.lock();
        let order = state.order();
        let boundary = state
            .wal_segments
            .entry(name.as_str().to_owned())
            .or_default();
        boundary.synced_bytes = boundary.appended_bytes;
        boundary.last_sync_order = Some(order);
        let durable_bytes = boundary.synced_bytes;
        state.report.wal_segment_syncs += 1;
        state.settle_pending(name.as_str(), durable_bytes);
    }

    /// An authorized whole-segment publish (the buffered WAL's rewrite path)
    /// atomically replaces the object's durable content: the boundary resets
    /// to the published length, fully durable.
    fn record_wal_publish(&self, name: &ObjectName, published_bytes: u64) {
        if !Self::is_wal_segment(name) {
            return;
        }
        let mut state = self.lock();
        let order = state.order();
        let boundary = state
            .wal_segments
            .entry(name.as_str().to_owned())
            .or_default();
        boundary.appended_bytes = published_bytes;
        boundary.synced_bytes = published_bytes;
        boundary.last_sync_order = Some(order);
        state.report.wal_segment_publishes += 1;
        state.settle_pending(name.as_str(), published_bytes);
        // A shorter rewrite discarded the bytes above the published length;
        // confirm the survivors now — leaving them pending would let later
        // appends reuse the same byte positions and falsely exonerate them.
        state.confirm_pending(name.as_str());
    }

    fn record_wal_delete(&self, name: &ObjectName) {
        if !Self::is_wal_segment(name) {
            return;
        }
        let mut state = self.lock();
        state.wal_segments.remove(name.as_str());
        state.confirm_pending(name.as_str());
    }

    fn check_publish(&self, name: &ObjectName) {
        let Some(family) = CheckedPublishFamily::classify(name) else {
            return;
        };
        let mut state = self.lock();
        let publish_order = state.order();
        state.report.publishes_checked += 1;
        let mut flagged = Vec::new();
        for (wal_object, boundary) in &state.wal_segments {
            if boundary.appended_bytes > boundary.synced_bytes {
                flagged.push(PendingViolation {
                    violation: WriteOrderingViolation {
                        published_object: name.as_str().to_owned(),
                        published_family: family,
                        publish_order,
                        wal_object: wal_object.clone(),
                        unsynced_bytes: boundary.appended_bytes - boundary.synced_bytes,
                        last_append_order: boundary.last_append_order,
                        last_sync_order: boundary.last_sync_order,
                    },
                    durable_mark: boundary.appended_bytes,
                });
            }
        }
        state.pending.extend(flagged);
    }
}

impl<B: Backend> Backend for WriteOrderingWatchdog<B> {
    fn capabilities(&self) -> BackendCapabilities {
        self.inner.capabilities()
    }

    fn read_object(&self, name: &ObjectName) -> BackendResult<Vec<u8>> {
        self.inner.read_object(name)
    }

    fn read_range(&self, name: &ObjectName, range: BackendRange) -> BackendResult<Vec<u8>> {
        self.inner.read_range(name, range)
    }

    fn write_object(&self, name: &ObjectName, bytes: &[u8]) -> BackendResult<BackendMetadata> {
        let metadata = self.inner.write_object(name, bytes)?;
        // A whole-object write replaces content; for a WAL segment (not used
        // by the V1 durable path, but modeled for completeness) it resets the
        // unsynced boundary to the full new length.
        if Self::is_wal_segment(name) {
            let mut state = self.lock();
            let order = state.order();
            let boundary = state
                .wal_segments
                .entry(name.as_str().to_owned())
                .or_default();
            boundary.appended_bytes = bytes.len() as u64;
            boundary.synced_bytes = 0;
            boundary.last_append_order = order;
            // The replaced content discarded any raced unsynced tail.
            state.confirm_pending(name.as_str());
        }
        Ok(metadata)
    }

    fn delete_object(&self, name: &ObjectName) -> DeleteResult {
        let outcome = self.inner.delete_object(name)?;
        self.record_wal_delete(name);
        Ok(outcome)
    }

    fn list_prefix(&self, prefix: &ObjectPrefix) -> BackendResult<Vec<ObjectName>> {
        self.inner.list_prefix(prefix)
    }

    fn object_metadata(&self, name: &ObjectName) -> BackendResult<BackendMetadata> {
        self.inner.object_metadata(name)
    }

    fn remove_empty_dirs_under(
        &self,
        prefix: &ObjectPrefix,
    ) -> Result<u64, crate::backend::BackendError> {
        self.inner.remove_empty_dirs_under(prefix)
    }

    fn acquire_writer_lock(&self, name: &ObjectName) -> BackendResult<BackendWriterGuard> {
        self.inner.acquire_writer_lock(name)
    }

    fn append_object(&self, name: &ObjectName, bytes: &[u8]) -> BackendResult<BackendAppend> {
        let append = self.inner.append_object(name, bytes)?;
        self.record_wal_append(name, bytes.len() as u64);
        Ok(append)
    }

    fn sync_object(&self, name: &ObjectName) -> BackendResult<()> {
        self.inner.sync_object(name)?;
        self.record_wal_sync(name);
        Ok(())
    }

    fn conditional_create(
        &self,
        name: &ObjectName,
        bytes: &[u8],
    ) -> BackendResult<BackendMetadata> {
        self.inner.conditional_create(name, bytes)
    }

    fn conditional_update(
        &self,
        name: &ObjectName,
        expected: &BackendFence,
        bytes: &[u8],
    ) -> BackendResult<BackendMetadata> {
        self.inner.conditional_update(name, expected, bytes)
    }

    fn publish_object(
        &self,
        name: &ObjectName,
        bytes: &[u8],
        mode: PublishMode,
    ) -> PublishResult<PublishOutcome> {
        let outcome = self.inner.publish_object(name, bytes, mode)?;
        // Check dependents first, then account the published object itself:
        // an atomically published WAL segment is durable by construction.
        self.check_publish(name);
        self.record_wal_publish(name, bytes.len() as u64);
        Ok(outcome)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::{
        CommitBatch, CommitMutation, CommitOptions, MaintenanceRequest, MaintenanceScope,
        MaintenanceTask, StorageBackend, StorageDurabilityPolicy, StorageKey,
        StorageMaintenanceSchedulingPolicy, StorageOpenOptions, StorageRuntime, StorageSpaceId,
        StorageValue,
    };
    use crate::backend::local_fs::LocalFsBackend;
    use strata_core::BranchId;

    fn watchdog(root: &std::path::Path) -> WriteOrderingWatchdog<LocalFsBackend> {
        WriteOrderingWatchdog::new(LocalFsBackend::new(root))
    }

    fn wal_segment(id: u64) -> ObjectName {
        ObjectLayout::wal_segment(id).expect("wal segment name")
    }

    fn manifest() -> ObjectName {
        ObjectLayout::database_manifest().expect("manifest name")
    }

    // --- detector non-vacuity: recorder-driven sequences ---
    // These drive the watchdog's recorders directly (no filesystem): the
    // LocalFs backend guards WAL-object mutations behind its authorized
    // retention path, and the thin trait delegation is proven by the live
    // runtime tests below.

    #[test]
    fn publish_over_unsynced_wal_is_a_typed_violation() {
        let dir = tempfile::tempdir().expect("tmp");
        let backend = watchdog(dir.path());
        backend.record_wal_append(&wal_segment(1), 12);
        backend.check_publish(&manifest());
        let report = backend.report();
        assert_eq!(report.publishes_checked(), 1);
        let [violation] = report.violations() else {
            panic!("expected exactly one violation: {report:?}");
        };
        assert_eq!(violation.published_family(), CheckedPublishFamily::Manifest);
        assert_eq!(violation.unsynced_bytes(), 12);
        assert_eq!(violation.wal_object(), wal_segment(1).as_str());
        assert!(violation.last_sync_order().is_none());
        assert!(violation.publish_order() > violation.last_append_order());
    }

    #[test]
    fn partially_synced_segment_reports_only_the_unsynced_tail() {
        let dir = tempfile::tempdir().expect("tmp");
        let backend = watchdog(dir.path());
        backend.record_wal_append(&wal_segment(1), 100);
        backend.record_wal_sync(&wal_segment(1));
        backend.record_wal_append(&wal_segment(1), 50);
        backend.check_publish(&manifest());
        let report = backend.report();
        let [violation] = report.violations() else {
            panic!("expected exactly one violation: {report:?}");
        };
        // The synced 100 bytes are not part of the raced window.
        assert_eq!(violation.unsynced_bytes(), 50);
        assert!(violation.last_sync_order().is_some());
    }

    #[test]
    fn publish_over_in_flight_appends_is_exonerated_by_the_covering_sync() {
        let dir = tempfile::tempdir().expect("tmp");
        let backend = watchdog(dir.path());
        backend.record_wal_append(&wal_segment(1), 100);
        backend.record_wal_sync(&wal_segment(1));
        // In-flight tail: appended after the last sync, durable moments later.
        backend.record_wal_append(&wal_segment(1), 50);
        backend.check_publish(&manifest());
        backend.record_wal_sync(&wal_segment(1));
        let report = backend.report();
        assert_eq!(report.publishes_checked(), 1);
        assert_eq!(report.exonerated_publishes(), 1);
        assert!(report.violations().is_empty(), "{report:?}");
    }

    #[test]
    fn covering_whole_segment_publish_exonerates_a_pending_violation() {
        let dir = tempfile::tempdir().expect("tmp");
        let backend = watchdog(dir.path());
        backend.record_wal_append(&wal_segment(1), 12);
        backend.check_publish(&manifest());
        // The buffered WAL's rewrite covers the tail: 40 >= the 12-byte mark.
        backend.record_wal_publish(&wal_segment(1), 40);
        let report = backend.report();
        assert_eq!(report.exonerated_publishes(), 1);
        assert!(report.violations().is_empty(), "{report:?}");
    }

    #[test]
    fn sync_of_a_different_segment_does_not_exonerate() {
        let dir = tempfile::tempdir().expect("tmp");
        let backend = watchdog(dir.path());
        backend.record_wal_append(&wal_segment(1), 12);
        backend.record_wal_append(&wal_segment(2), 8);
        backend.record_wal_sync(&wal_segment(2));
        backend.check_publish(&manifest());
        backend.record_wal_sync(&wal_segment(2));
        let report = backend.report();
        assert_eq!(report.exonerated_publishes(), 0);
        let [violation] = report.violations() else {
            panic!("expected exactly one violation: {report:?}");
        };
        assert_eq!(violation.wal_object(), wal_segment(1).as_str());
    }

    #[test]
    fn wal_bytes_discarded_while_a_publish_is_pending_stay_a_violation() {
        let dir = tempfile::tempdir().expect("tmp");
        let backend = watchdog(dir.path());
        backend.record_wal_append(&wal_segment(1), 12);
        backend.check_publish(&manifest());
        // The segment is deleted before its tail ever became durable: the
        // publish depended on bytes that are now provably gone.
        backend.record_wal_delete(&wal_segment(1));
        let report = backend.report();
        assert_eq!(report.exonerated_publishes(), 0);
        let [violation] = report.violations() else {
            panic!("expected exactly one violation: {report:?}");
        };
        assert_eq!(violation.unsynced_bytes(), 12);
    }

    #[test]
    fn shorter_segment_rewrite_does_not_exonerate_the_undurable_tail() {
        let dir = tempfile::tempdir().expect("tmp");
        let backend = watchdog(dir.path());
        backend.record_wal_append(&wal_segment(1), 12);
        backend.check_publish(&manifest());
        // A repair-style rewrite truncates below the 12-byte mark: the bytes
        // the publish raced never became durable.
        backend.record_wal_publish(&wal_segment(1), 4);
        // Regrowing past the old mark with fresh bytes must not exonerate the
        // confirmed violation — same offsets, different bytes.
        backend.record_wal_append(&wal_segment(1), 20);
        backend.record_wal_sync(&wal_segment(1));
        let report = backend.report();
        assert_eq!(report.exonerated_publishes(), 0);
        assert_eq!(report.violations().len(), 1, "{report:?}");
    }

    #[test]
    fn publish_after_wal_sync_is_clean() {
        let dir = tempfile::tempdir().expect("tmp");
        let backend = watchdog(dir.path());
        backend.record_wal_append(&wal_segment(1), 12);
        backend.record_wal_sync(&wal_segment(1));
        backend.check_publish(&manifest());
        let report = backend.report();
        assert_eq!(report.wal_segment_appends(), 1);
        assert_eq!(report.wal_segment_syncs(), 1);
        assert_eq!(report.publishes_checked(), 1);
        assert!(report.violations().is_empty(), "{report:?}");
    }

    #[test]
    fn snapshot_and_table_publishes_are_also_checked() {
        let dir = tempfile::tempdir().expect("tmp");
        let backend = watchdog(dir.path());
        backend.record_wal_append(&wal_segment(1), 1);
        backend.check_publish(&ObjectLayout::snapshot(1).expect("snapshot name"));
        backend.check_publish(
            &ObjectLayout::branch_table_manifest("branch").expect("table manifest name"),
        );
        let report = backend.report();
        assert_eq!(report.publishes_checked(), 2);
        assert_eq!(report.violations().len(), 2);
        assert_eq!(
            report.violations()[0].published_family(),
            CheckedPublishFamily::Snapshot
        );
        assert_eq!(
            report.violations()[1].published_family(),
            CheckedPublishFamily::Table
        );
    }

    #[test]
    fn wal_sidecar_metadata_is_not_a_publish_dependency() {
        let dir = tempfile::tempdir().expect("tmp");
        let backend = watchdog(dir.path());
        // An unsynced optional sidecar must not flag the publish: segments are
        // the record of truth, sidecars have a documented fallback.
        backend.record_wal_append(
            &ObjectLayout::wal_segment_metadata(1).expect("sidecar name"),
            7,
        );
        backend.check_publish(&manifest());
        let report = backend.report();
        assert_eq!(report.wal_segment_appends(), 0);
        assert!(report.violations().is_empty(), "{report:?}");
    }

    #[test]
    fn deleted_wal_segment_is_no_longer_a_dependency() {
        let dir = tempfile::tempdir().expect("tmp");
        let backend = watchdog(dir.path());
        backend.record_wal_append(&wal_segment(1), 12);
        backend.record_wal_delete(&wal_segment(1));
        backend.check_publish(&manifest());
        assert!(backend.report().violations().is_empty());
    }

    #[test]
    fn atomically_published_wal_segment_counts_as_durable() {
        let dir = tempfile::tempdir().expect("tmp");
        let backend = watchdog(dir.path());
        backend.record_wal_append(&wal_segment(1), 12);
        backend.record_wal_publish(&wal_segment(1), 40);
        backend.check_publish(&manifest());
        let report = backend.report();
        assert_eq!(report.wal_segment_publishes(), 1);
        assert_eq!(report.wal_durability_events(), 1);
        assert!(report.violations().is_empty(), "{report:?}");
    }

    // --- real operation streams: the invariant over the live engine ---

    fn default_branch() -> BranchId {
        BranchId::from_bytes([0x01; BranchId::BYTE_LEN])
    }

    fn commit_value_rows(
        runtime: &StorageRuntime<'_>,
        branch: BranchId,
        count: usize,
        value_len: usize,
    ) {
        let space = StorageSpaceId::new(vec![0x20]).expect("space");
        for index in 0..count {
            let key = StorageKey::new(vec![0x77, u8::try_from(index % 251).expect("key byte")])
                .expect("key");
            let batch = CommitBatch::new(
                branch,
                vec![CommitMutation::Put {
                    storage_space: space.clone(),
                    key,
                    value: StorageValue::new(vec![0x5A; value_len]),
                    ttl: None,
                }],
                CommitOptions::default(),
            )
            .expect("batch");
            runtime.commit(&batch).expect("commit");
        }
    }

    fn drain_all_maintenance(runtime: &mut StorageRuntime<'_>, branch: BranchId) {
        for task in [
            MaintenanceTask::Flush,
            MaintenanceTask::Checkpoint,
            MaintenanceTask::Compact,
            MaintenanceTask::SnapshotPruning,
        ] {
            // Enqueue results are discarded: unsupported scopes are no-ops and
            // the drain runs whatever was accepted.
            let _ = runtime.enqueue_maintenance(&MaintenanceRequest::new(
                task,
                MaintenanceScope::Branch(branch),
            ));
            let _ = runtime
                .enqueue_maintenance(&MaintenanceRequest::new(task, MaintenanceScope::Global));
            runtime.drain_maintenance().expect("drain");
        }
    }

    fn assert_clean_run(
        durability: StorageDurabilityPolicy,
        wal_segment_size: Option<u64>,
        expect_explicit_syncs: bool,
    ) {
        let dir = tempfile::tempdir().expect("tmp");
        let backend = StorageBackend::write_ordering_local_fs(dir.path());
        let branch = default_branch();
        {
            let mut options = StorageOpenOptions::durable_local(durability)
                .with_maintenance_scheduling_policy(
                    StorageMaintenanceSchedulingPolicy::EvaluateAndEnqueue,
                );
            if let Some(size) = wal_segment_size {
                options = options.with_wal_segment_size_for_test(size);
            }
            let mut runtime = StorageRuntime::open_with_backend(options, &backend)
                .expect("open")
                .into_runtime();
            commit_value_rows(&runtime, branch, 24, 256);
            drain_all_maintenance(&mut runtime, branch);
            commit_value_rows(&runtime, branch, 8, 256);
            drain_all_maintenance(&mut runtime, branch);
            // Quiesce through the real durability barrier: a bare drop is the
            // documented lossy-abandon path (WalService::drop flushes without
            // fsync), so only close() settles the in-flight tail that pending
            // exoneration is waiting on.
            runtime.close().expect("close");
        }
        let report = backend
            .write_ordering_report()
            .expect("watchdog backend reports");
        assert!(
            report.violations().is_empty(),
            "write-ordering violations on the live {durability:?} path: {:?}",
            report.violations()
        );
        // Non-vacuity: the run must actually have exercised the invariant.
        assert!(report.wal_segment_appends() > 0, "no WAL appends observed");
        assert!(report.publishes_checked() > 0, "no publishes checked");
        assert!(
            report.wal_durability_events() > 0,
            "no WAL durability events (sync or segment publish) observed"
        );
        if expect_explicit_syncs {
            assert!(report.wal_segment_syncs() > 0, "no WAL syncs observed");
        }
    }

    #[test]
    fn always_durability_stream_has_no_ordering_violations() {
        assert_clean_run(StorageDurabilityPolicy::Always, None, true);
    }

    #[test]
    fn standard_durability_stream_has_no_ordering_violations() {
        assert_clean_run(StorageDurabilityPolicy::Standard, None, false);
    }

    #[test]
    fn wal_rotation_under_small_segments_has_no_ordering_violations() {
        assert_clean_run(StorageDurabilityPolicy::Always, Some(1024), true);
        assert_clean_run(StorageDurabilityPolicy::Standard, Some(1024), false);
    }

    #[test]
    fn recovery_reopen_has_no_ordering_violations() {
        let dir = tempfile::tempdir().expect("tmp");
        let branch = default_branch();
        {
            // Stage with a plain backend and crash (drop without close) so the
            // reopen below has a WAL tail to replay.
            let staging = StorageBackend::local_fs(dir.path());
            let runtime = StorageRuntime::open_with_backend(
                StorageOpenOptions::durable_local(StorageDurabilityPolicy::Always)
                    .with_maintenance_scheduling_policy(
                        StorageMaintenanceSchedulingPolicy::EvaluateAndEnqueue,
                    ),
                &staging,
            )
            .expect("staging open")
            .into_runtime();
            commit_value_rows(&runtime, branch, 12, 256);
        }
        let backend = StorageBackend::write_ordering_local_fs(dir.path());
        {
            let mut runtime = StorageRuntime::open_with_backend(
                StorageOpenOptions::durable_local(StorageDurabilityPolicy::Always)
                    .with_maintenance_scheduling_policy(
                        StorageMaintenanceSchedulingPolicy::EvaluateAndEnqueue,
                    ),
                &backend,
            )
            .expect("recovery reopen")
            .into_runtime();
            commit_value_rows(&runtime, branch, 4, 256);
            drain_all_maintenance(&mut runtime, branch);
            runtime.close().expect("close");
        }
        let report = backend
            .write_ordering_report()
            .expect("watchdog backend reports");
        assert!(
            report.violations().is_empty(),
            "write-ordering violations on the recovery path: {:?}",
            report.violations()
        );
        assert!(report.publishes_checked() > 0, "no publishes checked");
    }
}
