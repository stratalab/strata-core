//! B5.2 — Safe reclaim protocol (quarantine-based) for `SegmentedStore`.
//!
//! The [`crate::segmented::SegmentedStore`] reclaim path routes through this
//! module instead of immediately unlinking segment files. The protocol
//! implements Stages 2–5 of
//! `docs/design/branching/branching-gc/branching-retention-contract.md`
//! §"Reclaim protocol":
//!
//! 1. **Stage 2 — manifest proof.** The caller's runtime accelerator check
//!    (`SegmentRefRegistry::is_referenced` plus the live-set scan in
//!    `gc_orphan_segments`) serves as the manifest proof, under the
//!    degraded-recovery refusal gate (`BarrierKind::RecoveryHealthGate`).
//!    When recovery health is `DataLoss`, `PolicyDowngrade`, or any
//!    non-`Telemetry` degradation, reclaim refuses **before** any
//!    filesystem mutation — addressing the B5.2 review Finding 1.
//! 2. **Stage 3 — quarantine rename.** After the proof succeeds, the
//!    segment file is renamed into `<branch_dir>/__quarantine__/` on the
//!    same filesystem namespace. Before Stage 4 publishes the inventory,
//!    the durable `quarantine.manifest` entry is written *first*
//!    (publish-before-rename) so a crash between Stage 4 publish and
//!    Stage 3 rename does not leave an on-disk orphan that reopen cannot
//!    correlate with the inventory.
//! 3. **Stage 4 — durable publish.** The per-branch `quarantine.manifest`
//!    (see [`crate::quarantine`]) is rewritten atomically with the new
//!    entry. Its temp-file + fsync + rename + fsync(dir) idiom matches
//!    `segments.manifest`.
//! 4. **Stage 5 — final purge.** A later [`SegmentedStore::purge_all_quarantines`]
//!    call drains each branch's quarantine manifest, unlinking the
//!    listed files and atomically rewriting the manifest with any
//!    entries whose unlink failed. Degraded recovery blocks purge per
//!    the same gate.
//!
//! **Reopen reconciliation** (see
//! [`SegmentedStore::reconcile_quarantine_on_recovery`]): disagreement
//! between the inventory and the on-disk quarantine directory degrades
//! recovery health to `PolicyDowngrade` with
//! [`crate::segmented::RecoveryFault::QuarantineInventoryMismatch`]. Per
//! §"Quarantine reconciliation", the implementation prefers retention
//! and blocks reclaim until a full rebuild-equivalent reconciliation
//! completes (KD10).
//!
//! Engine-facing attribution (`retention_report()` at the engine layer)
//! consumes the storage-layer snapshot produced by
//! [`SegmentedStore::retention_snapshot`] below, joins it with
//! `BranchControlStore` for generation-aware `BranchRef` attribution,
//! and returns `Err(StrataError::RetentionReportUnavailable)` when
//! recovery health cannot sustain trustworthy attribution (contract
//! §"Canonical blocker attribution", convergence doc §"retention
//! report contract" hard-fail rule).

use std::collections::{HashMap, HashSet};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use strata_core::id::CommitVersion;
use strata_core::types::BranchId;

use crate::quarantine::{
    read_quarantine_manifest, write_quarantine_manifest, QuarantineEntry, QUARANTINE_DIR,
    QUARANTINE_FILENAME,
};
use crate::segmented::{DegradationClass, RecoveryFault, RecoveryHealth, SegmentedStore};
use crate::{StorageError, StorageResult};

/// Summary of a [`SegmentedStore::purge_all_quarantines`] call.
#[derive(Debug, Clone, Copy, Default)]
#[non_exhaustive]
pub struct PurgeReport {
    /// Number of files unlinked during this call.
    pub files_purged: usize,
    /// Number of branch quarantines that were fully drained (inventory now empty).
    pub branches_drained: usize,
}

/// Storage-layer retention facts for one branch directory.
///
/// Produced by [`SegmentedStore::retention_snapshot`] and consumed by the
/// engine-layer `retention_report()` to attribute bytes to
/// `BranchRef` lifecycle instances. The snapshot is manifest-derived;
/// runtime accelerators (the segment ref registry) are used only to
/// classify own-segment bytes as exclusive vs shared.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct StorageBranchRetention {
    /// Directory identity — matches the on-disk `<segments_dir>/<hex>` name.
    pub branch_id: BranchId,
    /// Own-segment file bytes that no descendant's inherited-layer
    /// manifest currently references (ref registry zero).
    pub exclusive_bytes: u64,
    /// Own-segment file bytes that at least one descendant's
    /// inherited-layer manifest references. Retained until the
    /// descendant releases (release-at-clear-or-materialize).
    pub shared_bytes: u64,
    /// Bytes visible to this branch through inherited-layer manifests;
    /// the bytes physically live under the source branch's directory.
    pub inherited_layer_bytes: u64,
    /// Per-layer detail for `inherited_layer_bytes`.
    pub inherited_layers: Vec<StorageInheritedLayerInfo>,
    /// Bytes currently in this branch's `__quarantine__/` awaiting Stage-5 purge.
    pub quarantined_bytes: u64,
    /// Total number of segments in this branch's quarantine inventory.
    pub quarantined_segment_count: usize,
}

impl StorageBranchRetention {
    /// Empty retention facts for `branch_id` (zero bytes everywhere).
    /// Used by engine-layer attribution when a control record has no
    /// matching storage directory yet (e.g. a freshly-created branch
    /// before any flush).
    pub fn empty(branch_id: BranchId) -> Self {
        Self {
            branch_id,
            exclusive_bytes: 0,
            shared_bytes: 0,
            inherited_layer_bytes: 0,
            inherited_layers: Vec::new(),
            quarantined_bytes: 0,
            quarantined_segment_count: 0,
        }
    }
}

/// One entry in a [`StorageBranchRetention::inherited_layers`] list.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct StorageInheritedLayerInfo {
    /// The storage branch whose directory physically holds these bytes.
    pub source_branch_id: BranchId,
    /// Fork point that bounds descendant visibility into the source snapshot.
    pub fork_version: CommitVersion,
    /// Total bytes across every segment in this layer.
    pub bytes: u64,
    /// `true` once the layer has been fully materialized into the child's
    /// own segments (status == `Materialized` in the manifest).
    pub is_materialized: bool,
}

impl SegmentedStore {
    /// Check the `BarrierKind::RecoveryHealthGate` before any reclaim step.
    ///
    /// Shared helper used by both the quarantine-rename step and the
    /// final-purge step. Returns [`StorageError::GcRefusedDegradedRecovery`]
    /// when the most recent recovery produced a non-`Telemetry` degradation.
    pub(crate) fn check_reclaim_allowed(&self) -> StorageResult<()> {
        match &**self.last_recovery_health.load() {
            RecoveryHealth::Healthy
            | RecoveryHealth::Degraded {
                class: DegradationClass::Telemetry,
                ..
            } => Ok(()),
            RecoveryHealth::Degraded { class, .. } => {
                Err(StorageError::GcRefusedDegradedRecovery { class: *class })
            }
        }
    }

    /// Stages 2–4 of the reclaim protocol: prove manifest unreachability,
    /// publish the quarantine inventory entry, and rename the segment
    /// file into `<branch_dir>/__quarantine__/`.
    ///
    /// Returns `Ok(true)` if the segment was quarantined, `Ok(false)` if
    /// the runtime accelerator refused (still referenced — the caller's
    /// Stage 2 check). Returns `Err(GcRefusedDegradedRecovery)` when the
    /// recovery-health gate blocks reclaim; the file is left untouched
    /// at its original location.
    ///
    /// `seg_path` is the current on-disk location of the segment file
    /// and `file_id` is its stable [`KVSegment::file_id`] value. The
    /// call is a no-op (returning `Ok(false)`) if the file does not
    /// exist at `seg_path` anymore (e.g. a prior reclaim already moved
    /// it).
    ///
    /// [`KVSegment::file_id`]: crate::segment::KVSegment::file_id
    pub(crate) fn quarantine_segment_if_unreferenced(
        &self,
        seg_path: &Path,
        file_id: u64,
    ) -> StorageResult<bool> {
        // Gate — must refuse before any filesystem mutation.
        self.check_reclaim_allowed()?;

        let branch_dir = match seg_path.parent() {
            Some(p) => p.to_path_buf(),
            None => return Ok(false),
        };
        let filename = match seg_path.file_name().and_then(|n| n.to_str()) {
            Some(s) => s.to_string(),
            None => return Ok(false),
        };

        // Idempotency: if the file is already in quarantine (rename from a
        // prior run completed but inventory publish did not), return false
        // and let reconciliation handle it.
        if !seg_path.exists() {
            return Ok(false);
        }

        // Stage 2 manifest proof (runtime accelerator, degraded-gate above
        // ensures the accelerator is trustworthy) + Stages 3/4. The
        // deletion barrier prevents a concurrent fork from incrementing
        // the refcount between the check and the rename.
        let _guard = self.ref_registry.deletion_write_guard();
        if self.ref_registry.is_referenced(file_id) {
            return Ok(false);
        }
        crate::block_cache::global_cache().invalidate_file(file_id);

        // Stage 4 publish-before-rename — ensures a crash between publish
        // and rename leaves the file at its original path with the
        // inventory listing it, which reopen reconciliation handles by
        // dropping the stale entry (no health degrade).
        let existing = read_quarantine_manifest(&branch_dir)
            .map_err(|inner| StorageError::QuarantinePublishFailed {
                dir: branch_dir.clone(),
                inner,
            })?
            .unwrap_or_default();
        let mut entries = existing.entries;
        if !entries.iter().any(|e| e.segment_id == file_id) {
            entries.push(QuarantineEntry {
                segment_id: file_id,
                filename: filename.clone(),
            });
        }
        write_quarantine_manifest(&branch_dir, &entries)?;

        // Stage 3 rename — create __quarantine__/ if absent.
        let quarantine_dir = branch_dir.join(QUARANTINE_DIR);
        if !quarantine_dir.exists() {
            std::fs::create_dir_all(&quarantine_dir).map_err(|inner| {
                StorageError::QuarantinePublishFailed {
                    dir: branch_dir.clone(),
                    inner,
                }
            })?;
        }
        let quarantine_path = quarantine_dir.join(&filename);
        if let Err(e) = std::fs::rename(seg_path, &quarantine_path) {
            // Rename failed: inventory still lists the entry but file is at
            // original path. Reopen reconciliation drops the stale entry
            // without degrading; subsequent GC will re-nominate.
            return Err(StorageError::QuarantinePublishFailed {
                dir: branch_dir,
                inner: e,
            });
        }

        // Fsync branch_dir so the rename is durable before any purge step.
        if let Ok(dir_fd) = std::fs::File::open(&branch_dir) {
            let _ = dir_fd.sync_all();
        }

        Ok(true)
    }

    /// Stage 5 — final purge of every branch's `quarantine.manifest`.
    ///
    /// Drains each branch's inventory, unlinking the listed files from
    /// `<branch_dir>/__quarantine__/` and rewriting the manifest with
    /// any entries whose unlink failed. Blocks under degraded recovery
    /// per the contract's §"Stage 5. Final purge" guard.
    ///
    /// Idempotent on repeated calls: empty inventories are no-ops.
    pub fn purge_all_quarantines(&self) -> StorageResult<PurgeReport> {
        self.check_reclaim_allowed()?;
        let Some(segments_dir) = self.segments_dir.as_ref() else {
            return Ok(PurgeReport::default());
        };

        let mut report = PurgeReport::default();
        let Ok(entries) = std::fs::read_dir(segments_dir) else {
            return Ok(report);
        };

        for entry in entries.flatten() {
            let branch_path = entry.path();
            if !branch_path.is_dir() {
                continue;
            }
            if let Some(count) = self.purge_quarantine_for_branch(&branch_path)? {
                report.files_purged += count;
                report.branches_drained += 1;
            }
        }

        Ok(report)
    }

    /// Stage 5 for a single branch directory. Returns `Ok(None)` when no
    /// inventory exists; `Ok(Some(count))` with the number of files
    /// actually unlinked.
    ///
    /// Retained entries (files whose unlink failed) are rewritten back
    /// into the manifest so subsequent passes retry safely.
    ///
    /// Holds the global `deletion_write_guard` across the read,
    /// unlink, and manifest rewrite. That serialization matters:
    /// without it, a concurrent
    /// [`SegmentedStore::quarantine_segment_if_unreferenced`] could
    /// publish a new inventory entry between this function's read and
    /// rewrite, and the rewrite would silently overwrite (or unlink)
    /// the freshly-quarantined file. The same guard is what quarantine
    /// uses, so they mutually exclude on the inventory ledger without
    /// needing a second lock.
    pub(crate) fn purge_quarantine_for_branch(
        &self,
        branch_dir: &Path,
    ) -> StorageResult<Option<usize>> {
        self.check_reclaim_allowed()?;

        let _guard = self.ref_registry.deletion_write_guard();

        let mut manifest = match read_quarantine_manifest(branch_dir) {
            Ok(Some(m)) => m,
            Ok(None) => return Ok(None),
            Err(inner) => {
                return Err(StorageError::QuarantinePublishFailed {
                    dir: branch_dir.to_path_buf(),
                    inner,
                })
            }
        };

        if manifest.entries.is_empty() {
            // Inventory is empty; best-effort remove the __quarantine__/ dir.
            let qdir = branch_dir.join(QUARANTINE_DIR);
            let _ = std::fs::remove_dir(&qdir);
            // And remove the empty quarantine.manifest itself.
            let _ = std::fs::remove_file(branch_dir.join(QUARANTINE_FILENAME));
            return Ok(Some(0));
        }

        let quarantine_dir = branch_dir.join(QUARANTINE_DIR);
        let mut purged = 0usize;
        let mut remaining: Vec<QuarantineEntry> = Vec::new();
        for entry in manifest.entries.drain(..) {
            let path = quarantine_dir.join(&entry.filename);
            match std::fs::remove_file(&path) {
                Ok(()) => purged += 1,
                Err(e) if e.kind() == io::ErrorKind::NotFound => purged += 1,
                Err(e) => {
                    tracing::warn!(
                        target: "strata::storage::gc",
                        branch_dir = %branch_dir.display(),
                        segment_id = entry.segment_id,
                        file = %entry.filename,
                        error = %e,
                        "failed to purge quarantined segment; retrying on next GC"
                    );
                    remaining.push(entry);
                }
            }
        }

        if remaining.is_empty() {
            // Rewrite an empty manifest (atomic) then best-effort remove it
            // plus the quarantine dir.
            let _ = std::fs::remove_file(branch_dir.join(QUARANTINE_FILENAME));
            let _ = std::fs::remove_dir(&quarantine_dir);
        } else {
            write_quarantine_manifest(branch_dir, &remaining)?;
        }

        Ok(Some(purged))
    }

    /// Storage-layer snapshot of retention facts per branch directory.
    ///
    /// Manifest-derived own-segment + inherited-layer byte totals plus
    /// `__quarantine__/` inventory, classified into exclusive vs shared
    /// using the runtime ref registry. Used by the engine-layer
    /// `retention_report()` as the storage half of the attribution
    /// join. Safe to call under degraded recovery (it reports what is
    /// retained — it does not attempt reclaim).
    pub fn retention_snapshot(&self) -> StorageResult<Vec<StorageBranchRetention>> {
        let mut out: Vec<StorageBranchRetention> = Vec::with_capacity(self.branches.len());

        for branch in &self.branches {
            let branch_id = *branch.key();
            let state = branch.value();
            let ver = state.version.load();

            let mut exclusive_bytes = 0u64;
            let mut shared_bytes = 0u64;
            for level in &ver.levels {
                for seg in level {
                    let size = seg.file_size();
                    if self.ref_registry.is_referenced(seg.file_id()) {
                        shared_bytes += size;
                    } else {
                        exclusive_bytes += size;
                    }
                }
            }

            let mut inherited_layers: Vec<StorageInheritedLayerInfo> =
                Vec::with_capacity(state.inherited_layers.len());
            let mut inherited_layer_bytes = 0u64;
            for layer in &state.inherited_layers {
                let mut bytes = 0u64;
                for level in &layer.segments.levels {
                    for seg in level {
                        bytes += seg.file_size();
                    }
                }
                inherited_layer_bytes += bytes;
                inherited_layers.push(StorageInheritedLayerInfo {
                    source_branch_id: layer.source_branch_id,
                    fork_version: layer.fork_version,
                    bytes,
                    is_materialized: matches!(
                        layer.status,
                        crate::segmented::LayerStatus::Materialized
                    ),
                });
            }

            let (quarantined_bytes, quarantined_segment_count) =
                self.branch_quarantine_bytes(branch_id).unwrap_or((0, 0));

            out.push(StorageBranchRetention {
                branch_id,
                exclusive_bytes,
                shared_bytes,
                inherited_layer_bytes,
                inherited_layers,
                quarantined_bytes,
                quarantined_segment_count,
            });
        }

        Ok(out)
    }

    /// Read quarantine inventory for a single branch and compute the total
    /// bytes + segment count. Returns `None` if there is no `segments_dir`
    /// or no inventory file. Errors during I/O are swallowed — retention
    /// reporting is best-effort.
    fn branch_quarantine_bytes(&self, branch_id: BranchId) -> Option<(u64, usize)> {
        let segments_dir = self.segments_dir.as_ref()?;
        let branch_dir = segments_dir.join(crate::segmented::hex_encode_branch(&branch_id));
        let manifest = read_quarantine_manifest(&branch_dir).ok().flatten()?;
        let quarantine_dir = branch_dir.join(QUARANTINE_DIR);
        let mut total = 0u64;
        for entry in &manifest.entries {
            let path = quarantine_dir.join(&entry.filename);
            if let Ok(meta) = std::fs::metadata(&path) {
                total += meta.len();
            }
        }
        Some((total, manifest.entries.len()))
    }

    /// Reopen-time reconciliation of per-branch quarantine state.
    ///
    /// Called from [`SegmentedStore::recover_segments`] after the main
    /// walk. Appends faults to `faults` when disagreement is observed
    /// (classified as `PolicyDowngrade` per the contract's
    /// §"Quarantine reconciliation" rule — prefer retention, block
    /// reclaim). Stale inventory entries for files already purged or
    /// never renamed are dropped in place without degrading.
    ///
    /// Kept as a method on `SegmentedStore` for call-site ergonomics
    /// (the reopen walk already holds a `&self` reference); the
    /// implementation does not touch `self` because reconciliation is
    /// purely filesystem-driven.
    #[allow(clippy::unused_self)]
    pub(crate) fn reconcile_quarantine_on_recovery(
        &self,
        segments_dir: &Path,
        manifest_live_filenames: &HashMap<BranchId, HashSet<String>>,
        faults: &mut Vec<RecoveryFault>,
    ) {
        let Ok(entries) = std::fs::read_dir(segments_dir) else {
            return;
        };

        for dir_entry in entries.flatten() {
            let branch_path = dir_entry.path();
            if !branch_path.is_dir() {
                continue;
            }
            let Some(dir_name) = dir_entry.file_name().to_str().map(String::from) else {
                continue;
            };
            let Some(branch_id) = crate::segmented::hex_decode_branch(&dir_name) else {
                continue;
            };

            let live = manifest_live_filenames
                .get(&branch_id)
                .cloned()
                .unwrap_or_default();
            reconcile_single_branch(&branch_path, branch_id, &live, faults);
        }
    }
}

fn reconcile_single_branch(
    branch_path: &Path,
    branch_id: BranchId,
    manifest_live_filenames: &HashSet<String>,
    faults: &mut Vec<RecoveryFault>,
) {
    let quarantine_dir = branch_path.join(QUARANTINE_DIR);
    let quarantine_dir_exists = quarantine_dir.is_dir();

    let manifest = match read_quarantine_manifest(branch_path) {
        Ok(m) => m,
        Err(e) => {
            faults.push(RecoveryFault::QuarantineInventoryMismatch {
                branch_id,
                reason: format!("failed to read quarantine.manifest: {e}"),
            });
            return;
        }
    };

    // Case: no inventory but __quarantine__/ holds files — pure mismatch,
    // prefer retention and degrade trust.
    if manifest.is_none() {
        if quarantine_dir_exists {
            if let Ok(mut qentries) = std::fs::read_dir(&quarantine_dir) {
                if qentries.any(|e| e.as_ref().is_ok_and(|e| e.path().is_file())) {
                    faults.push(RecoveryFault::QuarantineInventoryMismatch {
                        branch_id,
                        reason: "__quarantine__/ contains files but no quarantine.manifest"
                            .to_string(),
                    });
                }
            }
        }
        return;
    }
    let manifest = manifest.unwrap();

    // Collect on-disk quarantine filenames.
    let mut on_disk: HashSet<String> = HashSet::new();
    if let Ok(q_entries) = std::fs::read_dir(&quarantine_dir) {
        for q in q_entries.flatten() {
            if q.path().is_file() {
                if let Some(name) = q.file_name().to_str() {
                    on_disk.insert(name.to_string());
                }
            }
        }
    }

    let inventory_filenames: HashSet<String> = manifest
        .entries
        .iter()
        .map(|e| e.filename.clone())
        .collect();

    // Mismatch A: file in __quarantine__/ but not in inventory → degrade.
    for name in &on_disk {
        if !inventory_filenames.contains(name) {
            faults.push(RecoveryFault::QuarantineInventoryMismatch {
                branch_id,
                reason: format!("quarantined file {name} not listed in inventory"),
            });
            return;
        }
    }

    // Mismatch B: inventory claims file X but a live manifest still
    // references X — reclaim never should have been allowed. Prefer
    // retention and degrade.
    for entry in &manifest.entries {
        if manifest_live_filenames.contains(&entry.filename) {
            faults.push(RecoveryFault::QuarantineInventoryMismatch {
                branch_id,
                reason: format!(
                    "inventory lists {} but a live manifest still references it",
                    entry.filename
                ),
            });
            return;
        }
    }

    // Reconcile benign cases: inventory entries whose files are neither
    // in quarantine nor manifest-referenced (e.g. rename never completed
    // or purge already removed the file) are dropped from the inventory.
    let mut reconciled: Vec<QuarantineEntry> = Vec::new();
    let mut dropped = 0usize;
    for entry in manifest.entries {
        if on_disk.contains(&entry.filename) {
            reconciled.push(entry);
        } else {
            dropped += 1;
        }
    }

    if dropped > 0 {
        if let Err(e) = write_quarantine_manifest(branch_path, &reconciled) {
            faults.push(RecoveryFault::QuarantineInventoryMismatch {
                branch_id,
                reason: format!("failed to rewrite reconciled quarantine.manifest: {e}"),
            });
        }
    }

    if reconciled.is_empty() && !on_disk.is_empty() {
        // Defensive: should be unreachable given mismatch A above.
        faults.push(RecoveryFault::QuarantineInventoryMismatch {
            branch_id,
            reason: "reconciliation left a non-empty quarantine dir with an empty inventory"
                .to_string(),
        });
    }

    if reconciled.is_empty() && on_disk.is_empty() {
        // Fully drained — best-effort cleanup.
        let _ = std::fs::remove_file(branch_path.join(QUARANTINE_FILENAME));
        let _ = std::fs::remove_dir(&quarantine_dir);
    }

    // Silence unused warning in release builds with tracing disabled.
    let _ = Arc::<()>::default();
    let _: &PathBuf = &quarantine_dir;
}
