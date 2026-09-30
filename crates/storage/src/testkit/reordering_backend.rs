//! Durability-realism backend: a `Backend` decorator that records each object's
//! unsynced boundary and, on a simulated crash, materializes a
//! filesystem-persistence-model crash state on the real `LocalFsBackend` files.
//!
//! POSIX does not promise ordered writes, atomic rename, or torn-free fsync. This
//! models the disk that lies: only fsync'd bytes are guaranteed; the un-fsync'd
//! tail may be lost, partially persisted, or filled with garbage, and a rename may
//! be observed undone. Crash states are materialized via the `crash_sim`
//! primitives and handed back to a plain reopen for the recovery oracle.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::backend::local_fs::LocalFsBackend;
use crate::backend::{
    Backend, BackendAppend, BackendCapabilities, BackendFence, BackendMetadata, BackendRange,
    BackendResult, BackendWriterGuard, DeleteResult, PublishMode, PublishOutcome, PublishResult,
};
use crate::object::{ObjectName, ObjectPrefix};
use crate::testkit::integration_harness::crash_sim;
use crate::testkit::rng::SplitMix64;
use crate::testkit::TestkitError;

/// A filesystem persistence model — how a power-loss crash state relates to the
/// writes that were issued. Each models a guarantee POSIX does *not* make.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FsModel {
    /// Ordered + atomic: only fsync'd bytes survive; the un-fsync'd tail is lost.
    OrderedAtomic,
    /// Un-fsync'd appends persist only up to a partial, earlier offset.
    ReorderedAppends,
    /// The un-fsync'd tail is present but filled with garbage (a torn write).
    GarbageUnsyncedTail,
    /// Historical "rename observed undone" model — now damage-free (#2827):
    /// every file birth in this backend is a completed durable publish (temp
    /// fsync → rename → parent-directory fsync), so power loss cannot undo a
    /// directory entry. Whole-file damage is the artifact-fault lane's job
    /// (TCP4.9a); the LEGAL power-loss analog — an un-fsync'd DELETE undone,
    /// resurrecting the file — is tracked as its successor semantics.
    SplitRename,
}

#[derive(Clone, Copy, Debug, Default)]
struct ObjectDurability {
    /// Bytes written so far (in the page cache; survives `drop`, not power loss).
    current_len: u64,
    /// Bytes confirmed durable by the last `sync_object` (or an atomic publish).
    durable_len: u64,
}

#[derive(Debug, Default)]
struct ReorderState {
    objects: HashMap<String, (ObjectName, ObjectDurability)>,
}

fn as_u64(len: usize) -> u64 {
    u64::try_from(len).unwrap_or(u64::MAX)
}

/// A `Backend` decorator over `LocalFsBackend` that records each object's unsynced
/// boundary so a simulated crash can materialize a persistence-model crash state.
/// Test / `fault-injection`-only.
// BS4.4h: `state` is `Arc`-shared so the backend is `Clone` and can produce an owned handle;
// a clone shares the same reorder state (runtime's owned clone + the test's retained handle).
#[derive(Clone, Debug)]
pub struct ReorderingBackend {
    inner: LocalFsBackend,
    state: Arc<Mutex<ReorderState>>,
}

impl ReorderingBackend {
    /// Wrap a fresh local-filesystem backend rooted at `root`.
    pub fn local_fs(root: impl Into<std::path::PathBuf>) -> Self {
        Self {
            inner: LocalFsBackend::new(root),
            state: Arc::new(Mutex::new(ReorderState::default())),
        }
    }

    pub(crate) fn inner(&self) -> &LocalFsBackend {
        &self.inner
    }

    fn record_set_len(&self, name: &ObjectName, current_len: u64) {
        if let Ok(mut state) = self.state.lock() {
            let entry = state
                .objects
                .entry(name.as_str().to_owned())
                .or_insert_with(|| (name.clone(), ObjectDurability::default()));
            entry.1.current_len = current_len;
        }
    }

    fn record_append(&self, name: &ObjectName, added: u64) {
        if let Ok(mut state) = self.state.lock() {
            let entry = state
                .objects
                .entry(name.as_str().to_owned())
                .or_insert_with(|| (name.clone(), ObjectDurability::default()));
            entry.1.current_len = entry.1.current_len.saturating_add(added);
        }
    }

    fn record_sync(&self, name: &ObjectName) {
        if let Ok(mut state) = self.state.lock() {
            if let Some(entry) = state.objects.get_mut(name.as_str()) {
                entry.1.durable_len = entry.1.current_len;
            }
        }
    }

    fn record_publish(&self, name: &ObjectName, len: u64) {
        if let Ok(mut state) = self.state.lock() {
            state.objects.insert(
                name.as_str().to_owned(),
                (
                    name.clone(),
                    ObjectDurability {
                        current_len: len,
                        durable_len: len,
                    },
                ),
            );
        }
    }

    fn record_delete(&self, name: &ObjectName) {
        if let Ok(mut state) = self.state.lock() {
            state.objects.remove(name.as_str());
        }
    }

    /// Objects with an un-fsync'd tail, in deterministic (name-sorted) order so a
    /// seeded crash replays identically.
    fn unsynced(state: &ReorderState) -> Vec<(ObjectName, ObjectDurability)> {
        let mut objects: Vec<(ObjectName, ObjectDurability)> = state
            .objects
            .values()
            .filter(|(_, durability)| durability.current_len > durability.durable_len)
            .map(|(name, durability)| (name.clone(), *durability))
            .collect();
        objects.sort_by(|left, right| left.0.as_str().cmp(right.0.as_str()));
        objects
    }

    /// Materialize a crash state for `model` (seeded), perturbing the real files.
    /// Returns whether anything was actually perturbed (for non-vacuousness).
    pub fn crash(&self, model: FsModel, seed: u64) -> Result<bool, TestkitError> {
        let mut rng = SplitMix64::new(seed ^ 0x5253_544d_5f33_64a5);
        let state = self
            .state
            .lock()
            .map_err(|_| TestkitError::new("reorder state mutex poisoned"))?;
        let root = self.inner.root();
        let mut perturbed = false;
        match model {
            FsModel::OrderedAtomic => {
                for (name, durability) in Self::unsynced(&state) {
                    crash_sim::truncate_object(root, &name, durability.durable_len)?;
                    perturbed = true;
                }
            }
            FsModel::ReorderedAppends => {
                for (name, durability) in Self::unsynced(&state) {
                    let span = durability.current_len - durability.durable_len;
                    let keep = durability.durable_len + (rng.next_u64() % (span + 1));
                    crash_sim::truncate_object(root, &name, keep)?;
                    perturbed = true;
                }
            }
            FsModel::GarbageUnsyncedTail => {
                for (name, durability) in Self::unsynced(&state) {
                    let span = durability.current_len - durability.durable_len;
                    let offset = durability.durable_len + (rng.next_u64() % span);
                    crash_sim::corrupt_object_byte(root, &name, offset, 0xff)?;
                    perturbed = true;
                }
            }
            FsModel::SplitRename => {
                // #2827: no perturbation — see the variant's doc. The drop this
                // arm used to perform materialized a state the production
                // publish discipline makes unreachable, and the whole-DB lane
                // rightly convicted those states as bricks.
            }
        }
        Ok(perturbed)
    }
}

impl Backend for ReorderingBackend {
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
        self.record_set_len(name, as_u64(bytes.len()));
        Ok(metadata)
    }

    fn delete_object(&self, name: &ObjectName) -> DeleteResult {
        let outcome = self.inner.delete_object(name)?;
        self.record_delete(name);
        Ok(outcome)
    }

    fn list_prefix(&self, prefix: &ObjectPrefix) -> BackendResult<Vec<ObjectName>> {
        self.inner.list_prefix(prefix)
    }

    fn object_metadata(&self, name: &ObjectName) -> BackendResult<BackendMetadata> {
        self.inner.object_metadata(name)
    }

    fn acquire_writer_lock(&self, name: &ObjectName) -> BackendResult<BackendWriterGuard> {
        self.inner.acquire_writer_lock(name)
    }

    fn append_object(&self, name: &ObjectName, bytes: &[u8]) -> BackendResult<BackendAppend> {
        let append = self.inner.append_object(name, bytes)?;
        self.record_append(name, as_u64(bytes.len()));
        Ok(append)
    }

    fn sync_object(&self, name: &ObjectName) -> BackendResult<()> {
        self.inner.sync_object(name)?;
        self.record_sync(name);
        Ok(())
    }

    fn conditional_create(
        &self,
        name: &ObjectName,
        bytes: &[u8],
    ) -> BackendResult<BackendMetadata> {
        let metadata = self.inner.conditional_create(name, bytes)?;
        self.record_set_len(name, as_u64(bytes.len()));
        Ok(metadata)
    }

    fn conditional_update(
        &self,
        name: &ObjectName,
        expected: &BackendFence,
        bytes: &[u8],
    ) -> BackendResult<BackendMetadata> {
        let metadata = self.inner.conditional_update(name, expected, bytes)?;
        self.record_set_len(name, as_u64(bytes.len()));
        Ok(metadata)
    }

    fn publish_object(
        &self,
        name: &ObjectName,
        bytes: &[u8],
        mode: PublishMode,
    ) -> PublishResult<PublishOutcome> {
        let outcome = self.inner.publish_object(name, bytes, mode)?;
        self.record_publish(name, as_u64(bytes.len()));
        Ok(outcome)
    }

    // #3721: a completed link is a completed create-mode publish of `to` (its
    // parent directory is fsynced before success), over bytes the source's
    // own publish already made durable — so `to` is tracked as fully durable,
    // exactly like a publish of the same length.
    fn link_object(
        &self,
        from: &ObjectName,
        to: &ObjectName,
    ) -> Result<PublishOutcome, crate::backend::PublishError> {
        let outcome = self.inner.link_object(from, to)?;
        self.record_publish(to, outcome.metadata().size_bytes());
        Ok(outcome)
    }
}

#[cfg(test)]
mod model_legality_tests {
    //! #2827: `SplitRename` may only drop files whose DIRECTORY ENTRY is not
    //! crash-durable. The production publish path fsyncs the temp file, the
    //! rename, AND the parent directory — a completed publish cannot legally
    //! vanish whole. The append path (`sync_object` = `sync_all` only) never
    //! syncs the directory, so an append-created file with no subsequent
    //! same-directory publish CAN legally vanish — and a later publish into
    //! the same directory durably commits every earlier entry too.
    use super::ReorderingBackend;
    use crate::backend::{Backend, PublishMode};
    use crate::object::ObjectName;
    use crate::testkit::FsModel;

    fn object(name: &str) -> ObjectName {
        ObjectName::new(name).expect("valid object name")
    }

    /// On-disk path for an object: `LocalFs` stores `<name>.object@` (the
    /// suffix INCLUDES the `@` — the #2804 triage gotcha).
    fn object_path(root: &std::path::Path, name: &ObjectName) -> std::path::PathBuf {
        root.join(format!("{}.object@", name.as_str()))
    }

    #[test]
    fn split_rename_never_drops_a_completed_publish() {
        for seed in 0..16u64 {
            let dir = tempfile::tempdir().expect("tmp");
            let backend = ReorderingBackend::local_fs(dir.path());
            let published = object("tables/branch-a/l0000/table-one");
            backend
                .publish_object(&published, b"published-bytes", PublishMode::Create)
                .expect("publish");
            let _ = backend
                .crash(FsModel::SplitRename, seed)
                .expect("crash materialization");
            assert!(
                object_path(dir.path(), &published).exists(),
                "seed {seed}: a completed publish (temp fsync + rename + parent-dir \
                 fsync) must survive every crash materialization"
            );
        }
    }

    /// A deleted object leaves crash tracking with it: an unsynced tail
    /// recorded before the delete must not drive a materialization against
    /// the now-missing file (kills a `record_delete -> ()` stub, which
    /// leaves the tombstoned entry in the durability map).
    #[test]
    fn deleted_objects_leave_crash_tracking() {
        let dir = tempfile::tempdir().expect("tmp");
        let backend = ReorderingBackend::local_fs(dir.path());
        let doomed = object("tables/branch-a/l0000/doomed");
        backend
            .publish_object(&doomed, b"published", PublishMode::Create)
            .expect("publish");
        backend
            .append_object(&doomed, b"unsynced-tail")
            .expect("append");
        backend.delete_object(&doomed).expect("delete");
        let perturbed = backend
            .crash(FsModel::OrderedAtomic, 7)
            .expect("crash materialization must not touch the deleted file");
        assert!(
            !perturbed,
            "nothing unsynced remains after the delete: {perturbed:?}"
        );
        assert!(
            !object_path(dir.path(), &doomed).exists(),
            "the deleted object must stay deleted"
        );
    }

    #[test]
    fn split_rename_leaves_segment_shaped_objects_intact() {
        // WAL segments are BORN via a durable publish (header) then extended
        // by appends — their directory entry is crash-durable from birth.
        for seed in 0..16u64 {
            let dir = tempfile::tempdir().expect("tmp");
            let backend = ReorderingBackend::local_fs(dir.path());
            let segment = object("wal/segment-one");
            backend
                .publish_object(&segment, b"segment-header", PublishMode::Create)
                .expect("publish header");
            backend
                .append_object(&segment, b"appended-tail")
                .expect("append");
            backend.sync_object(&segment).expect("sync contents");
            let _ = backend
                .crash(FsModel::SplitRename, seed)
                .expect("crash materialization");
            assert!(
                object_path(dir.path(), &segment).exists(),
                "seed {seed}: a publish-created, append-extended file's \
                 directory entry is durable — it must survive"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::FsModel;
    use crate::api::{
        CommitBatch, CommitMutation, CommitOptions, StorageBackend, StorageDurabilityPolicy,
        StorageMaintenanceSchedulingPolicy, StorageOpenOptions, StorageRuntime, StorageValue,
    };
    use crate::testkit::recovery_oracle::verify::scan_recovered;
    use crate::testkit::recovery_oracle::workload::{
        default_branch, oracle_key, oracle_prefix_key, oracle_space, SCAN_LIMIT,
    };

    fn open(backend: &StorageBackend, durability: StorageDurabilityPolicy) -> StorageRuntime<'_> {
        StorageRuntime::open_with_backend(
            StorageOpenOptions::durable_local(durability).with_maintenance_scheduling_policy(
                StorageMaintenanceSchedulingPolicy::EvaluateAndEnqueue,
            ),
            backend,
        )
        .expect("open reordering backend")
        .into_runtime()
    }

    fn commit_put(runtime: &mut StorageRuntime<'_>, index: u8) {
        let batch = CommitBatch::new(
            default_branch(),
            vec![CommitMutation::Put {
                storage_space: oracle_space(),
                key: oracle_key(index),
                value: StorageValue::new(vec![index]),
                ttl: None,
            }],
            CommitOptions::default(),
        )
        .expect("commit batch");
        runtime.commit(&batch).expect("commit");
    }

    /// Reopen on a plain backend (lossy) and count the recovered live keys.
    fn recovered_count(root: &std::path::Path) -> usize {
        let backend = StorageBackend::local_fs(root.to_path_buf());
        // Retry-on-Unavailable absorbs the prior runtime's detached-worker
        // writer-lock window (#2727); any other failure stays loud.
        let runtime = crate::testkit::reopen_retry::open_with_retry_on_unavailable(|| {
            StorageRuntime::open_with_backend(
                StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
                    .with_strict_recovery(false)
                    .with_maintenance_scheduling_policy(
                        StorageMaintenanceSchedulingPolicy::EvaluateAndEnqueue,
                    ),
                &backend,
            )
        })
        .expect("reopen")
        .into_runtime();
        scan_recovered(
            &runtime,
            default_branch(),
            &oracle_space(),
            &oracle_prefix_key(),
            SCAN_LIMIT,
        )
        .expect("scan")
        .len()
    }

    #[test]
    fn always_ordered_atomic_loses_nothing() {
        let dir = tempfile::tempdir().expect("tmp");
        let backend = StorageBackend::reordering_local_fs(dir.path().to_path_buf());
        {
            let mut runtime = open(&backend, StorageDurabilityPolicy::Always);
            for index in 0u8..4 {
                commit_put(&mut runtime, index);
            }
        }
        // Every commit was fsync'd, so an ordered-atomic power loss has no unsynced
        // tail to drop — nothing is perturbed and nothing is lost.
        let perturbed = backend
            .reordering_crash(FsModel::OrderedAtomic, 1)
            .expect("crash");
        assert!(
            !perturbed,
            "Always durability left an unsynced tail to lose"
        );
        assert_eq!(
            recovered_count(dir.path()),
            4,
            "Always lost an acknowledged commit under power loss"
        );
    }

    #[test]
    fn standard_ordered_atomic_truncates_unsynced_tail() {
        let dir = tempfile::tempdir().expect("tmp");
        let backend = StorageBackend::reordering_local_fs(dir.path().to_path_buf());
        {
            let mut runtime = open(&backend, StorageDurabilityPolicy::Standard);
            for index in 0u8..4 {
                commit_put(&mut runtime, index);
            }
        }
        // Standard does not fsync per commit, so the unsynced WAL tail is dropped;
        // recovery yields a (possibly empty) prefix of what was committed.
        let perturbed = backend
            .reordering_crash(FsModel::OrderedAtomic, 1)
            .expect("crash");
        assert!(perturbed, "Standard left no unsynced tail to truncate");
        assert!(
            recovered_count(dir.path()) <= 4,
            "recovered more than was committed"
        );
    }
}
