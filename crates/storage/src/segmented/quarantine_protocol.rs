//! B5.2 — Safe reclaim protocol (quarantine-based) for `SegmentedStore`.
//!
//! The [`crate::segmented::SegmentedStore`] reclaim path routes through this
//! module instead of immediately unlinking segment files. The protocol
//! implements Stages 2–5 of
//! `docs/design/branching/branching-gc/branching-retention-contract.md`
//! §"Reclaim protocol":
//!
//! 1. **Stage 2 — manifest proof.** Candidate selection still starts from
//!    the runtime accelerator (`SegmentRefRegistry::is_referenced` plus
//!    the live-set scan in `gc_orphan_segments`), but the authoritative
//!    reclaim proof is a walk of recovery-trusted `segments.manifest`
//!    own entries and inherited-layer entries across the on-disk branch
//!    directories. When recovery health is `DataLoss`,
//!    `PolicyDowngrade`, or any non-`Telemetry` degradation, reclaim
//!    refuses **before** any filesystem mutation.
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
use strata_core::BranchId;

use crate::quarantine::{
    read_quarantine_manifest, write_quarantine_manifest, QuarantineEntry, QUARANTINE_DIR,
    QUARANTINE_FILENAME,
};
use crate::segmented::{DegradationClass, RecoveryFault, RecoveryHealth, SegmentedStore};
use crate::{StorageError, StorageResult};

fn manifest_entry_matches_segment(
    entry: &crate::manifest::ManifestEntry,
    file_id: u64,
    filename: &str,
) -> bool {
    entry.filename == filename
        || entry.filename == format!("{file_id}.sst")
        || entry
            .filename
            .strip_suffix(".sst")
            .and_then(|stem| stem.parse::<u64>().ok())
            == Some(file_id)
}

#[derive(Debug, Clone)]
struct RetentionTopLevelSegment {
    filename: String,
    size: u64,
}

#[derive(Debug, Clone)]
struct BranchQuarantineState {
    manifest: Option<crate::quarantine::QuarantineManifest>,
    files_on_disk: HashMap<String, u64>,
}

#[derive(Debug, Clone)]
struct RetentionBranchDirView {
    branch_id: BranchId,
    branch_dir: PathBuf,
    manifest: Option<crate::manifest::SegmentManifest>,
    top_level_segments: HashMap<String, RetentionTopLevelSegment>,
    quarantine: Option<BranchQuarantineState>,
}

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
/// `BranchRef` lifecycle instances. The snapshot is manifest-derived
/// except for the explicit B5.2 legacy `NoManifestFallbackUsed`
/// recovery path, where the last recovery health already recorded that
/// top-level segments were promoted as the visible branch state.
/// Own-segment shared/exclusive classification comes from
/// inherited-layer reachability rather than runtime refcounts.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct StorageBranchRetention {
    /// Directory identity — matches the on-disk `<segments_dir>/<hex>` name.
    pub branch_id: BranchId,
    /// Own-segment file bytes that no descendant's inherited-layer
    /// manifest currently references.
    pub exclusive_bytes: u64,
    /// Own-segment file bytes that at least one descendant's
    /// inherited-layer manifest references. Retained until the
    /// descendant releases (release-at-clear-or-materialize).
    pub shared_bytes: u64,
    /// Descendant-held bytes that are no longer owned by this branch's
    /// current manifest.
    ///
    /// This is the same-name recreate / deleted-parent-with-live-child
    /// shape: the bytes still live under this branch directory and are
    /// still manifest-reachable through a descendant inherited layer,
    /// but they belong to an older lifecycle instance rather than the
    /// current live manifest. Engine-layer attribution must surface
    /// these as orphan storage, not as `shared_bytes` on the current
    /// live branch entry.
    pub detached_shared_bytes: u64,
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
            detached_shared_bytes: 0,
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

    fn manifest_proof_allows_reclaim(
        &self,
        file_id: u64,
        filename: &str,
        exclude_branch_dir: Option<&Path>,
    ) -> StorageResult<bool> {
        for view in self.retention_branch_dir_views(false)? {
            if exclude_branch_dir.is_some_and(|excluded| excluded == view.branch_dir.as_path()) {
                continue;
            }
            let Some(manifest) = view.manifest.as_ref() else {
                continue;
            };

            if manifest
                .entries
                .iter()
                .any(|entry| manifest_entry_matches_segment(entry, file_id, filename))
            {
                return Ok(false);
            }

            if manifest.inherited_layers.iter().any(|layer| {
                layer.status != 2
                    && layer
                        .entries
                        .iter()
                        .any(|entry| manifest_entry_matches_segment(entry, file_id, filename))
            }) {
                return Ok(false);
            }
        }

        Ok(true)
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
        self.quarantine_segment_if_unreferenced_inner(seg_path, file_id, None)
    }

    pub(crate) fn quarantine_segment_if_unreferenced_for_cleared_branch(
        &self,
        seg_path: &Path,
        file_id: u64,
    ) -> StorageResult<bool> {
        self.quarantine_segment_if_unreferenced_inner(seg_path, file_id, seg_path.parent())
    }

    fn quarantine_segment_if_unreferenced_inner(
        &self,
        seg_path: &Path,
        file_id: u64,
        exclude_branch_dir: Option<&Path>,
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

        // Stage 2 candidate-selection + manifest proof. The deletion barrier
        // prevents a concurrent fork from incrementing the runtime refcount
        // between the fast-path candidate screen and the authoritative
        // manifest walk.
        let _guard = self.ref_registry.deletion_write_guard();
        if self.ref_registry.is_referenced(file_id) {
            return Ok(false);
        }
        if !self.manifest_proof_allows_reclaim(file_id, &filename, exclude_branch_dir)? {
            return Err(StorageError::ReclaimRefusedManifestProof {
                segment_id: file_id,
            });
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

        // Cross-directory rename durability: the file moved from
        // `branch_dir` into `branch_dir/__quarantine__/`. Both namespace
        // parents must be durably published before we report success.
        sync_dir_for_quarantine(&quarantine_dir)?;
        sync_dir_for_quarantine(&branch_dir)?;

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
    /// from inherited-layer reachability. The same branch-directory ledger
    /// also feeds Stage-2 manifest proof so reclaim and reporting do not
    /// drift. Used by the engine-layer
    /// `retention_report()` as the storage half of the attribution
    /// join. Safe to call under degraded recovery (it reports what is
    /// retained — it does not attempt reclaim).
    pub fn retention_snapshot(&self) -> StorageResult<Vec<StorageBranchRetention>> {
        let mut out: Vec<StorageBranchRetention> = Vec::new();

        let views = self.retention_branch_dir_views(true)?;
        let views_by_id: HashMap<BranchId, &RetentionBranchDirView> =
            views.iter().map(|view| (view.branch_id, view)).collect();
        let descendant_held = descendant_held_filenames_by_source(&views);
        let manifest_live_filenames = manifest_live_filenames_from_views(&views);
        let recovery_health = self.last_recovery_health.load();
        let legacy_fallback_branches = branches_with_legacy_no_manifest_fallback(&recovery_health);

        for view in &views {
            let mut accounted_top_level: HashSet<String> = HashSet::new();
            let mut exclusive_bytes = 0u64;
            let mut shared_bytes = 0u64;
            let mut detached_shared_bytes = 0u64;
            let shared_filenames = descendant_held.get(&view.branch_id);

            if let Some(manifest) = view.manifest.as_ref() {
                for entry in &manifest.entries {
                    accounted_top_level.insert(entry.filename.clone());
                    if let Some(file) = view.top_level_segments.get(&entry.filename) {
                        if shared_filenames
                            .is_some_and(|filenames| filenames.contains(&entry.filename))
                        {
                            shared_bytes += file.size;
                        } else {
                            exclusive_bytes += file.size;
                        }
                    }
                }
            } else if legacy_fallback_branches.contains(&view.branch_id) {
                // Explicit legacy fallback: the last recovery admitted this
                // branch by promoting every discovered top-level `.sst` into
                // visible own state under `PolicyDowngrade`.
                for file in view.top_level_segments.values() {
                    if shared_filenames.is_some_and(|filenames| filenames.contains(&file.filename))
                    {
                        shared_bytes += file.size;
                    } else {
                        exclusive_bytes += file.size;
                    }
                }
            } else if !view.top_level_segments.is_empty() {
                // Missing manifest truth is only attribution-safe when the
                // last recovery explicitly admitted the legacy fallback for
                // this branch. Otherwise a live branch directory with own
                // segment files but no manifest is untrusted and the engine
                // must refuse `retention_report()`.
                return Err(StorageError::RecoveryFault(
                    RecoveryFault::CorruptManifest {
                        branch_id: view.branch_id,
                        inner: io::Error::new(
                            io::ErrorKind::NotFound,
                            format!(
                                "segments.manifest missing for branch {} during retention snapshot",
                                view.branch_dir.display()
                            ),
                        ),
                    },
                ));
            }

            for file in view.top_level_segments.values() {
                if accounted_top_level.contains(&file.filename) {
                    continue;
                }

                // Report only manifest-reachable truth.
                //
                // Top-level `.sst` files that are not listed in the branch's
                // own manifest and are not held by any descendant inherited
                // layer are post-publish reclaim debt, not live retention.
                // Compaction intentionally publishes the new manifest before
                // it routes old files through reclaim, so raw disk presence
                // alone must not inflate `exclusive_bytes` / `shared_bytes`.
                if shared_filenames.is_some_and(|filenames| filenames.contains(&file.filename)) {
                    detached_shared_bytes += file.size;
                }
            }

            let mut inherited_layers: Vec<StorageInheritedLayerInfo> = Vec::new();
            let mut inherited_layer_bytes = 0u64;
            if let Some(manifest) = view.manifest.as_ref() {
                for layer in &manifest.inherited_layers {
                    if layer.status == 2 {
                        continue;
                    }
                    let bytes = views_by_id
                        .get(&layer.source_branch_id)
                        .map(|source_view| {
                            layer
                                .entries
                                .iter()
                                .filter_map(|entry| {
                                    source_view
                                        .top_level_segments
                                        .get(&entry.filename)
                                        .map(|file| file.size)
                                })
                                .sum()
                        })
                        .unwrap_or(0);
                    inherited_layer_bytes += bytes;
                    inherited_layers.push(StorageInheritedLayerInfo {
                        source_branch_id: layer.source_branch_id,
                        fork_version: layer.fork_version,
                        bytes,
                        is_materialized: layer.status == 2,
                    });
                }
            }

            let empty_live_filenames = HashSet::new();
            let (quarantined_bytes, quarantined_segment_count) = quarantine_stats_from_state(
                view.branch_id,
                view.quarantine.as_ref().expect("quarantine loaded"),
                manifest_live_filenames
                    .get(&view.branch_id)
                    .unwrap_or(&empty_live_filenames),
            )?;

            if exclusive_bytes == 0
                && shared_bytes == 0
                && detached_shared_bytes == 0
                && inherited_layer_bytes == 0
                && quarantined_bytes == 0
            {
                continue;
            }

            out.push(StorageBranchRetention {
                branch_id: view.branch_id,
                exclusive_bytes,
                shared_bytes,
                detached_shared_bytes,
                inherited_layer_bytes,
                inherited_layers,
                quarantined_bytes,
                quarantined_segment_count,
            });
        }

        Ok(out)
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
    pub(crate) fn reconcile_quarantine_on_recovery(&self, faults: &mut Vec<RecoveryFault>) {
        let views = self.retention_branch_dir_views_for_recovery(faults);
        let manifest_live_filenames = manifest_live_filenames_from_views(&views);
        for view in &views {
            let live = manifest_live_filenames
                .get(&view.branch_id)
                .cloned()
                .unwrap_or_default();
            reconcile_single_branch(&view.branch_dir, view.branch_id, &live, faults);
        }
    }
}

fn sync_dir_for_quarantine(dir: &Path) -> StorageResult<()> {
    let dir_fd =
        std::fs::File::open(dir).map_err(|inner| StorageError::QuarantinePublishFailed {
            dir: dir.to_path_buf(),
            inner,
        })?;
    dir_fd
        .sync_all()
        .map_err(|inner| StorageError::QuarantinePublishFailed {
            dir: dir.to_path_buf(),
            inner,
        })
}

fn load_branch_quarantine_state(
    branch_dir: &Path,
    branch_id: BranchId,
) -> StorageResult<BranchQuarantineState> {
    let quarantine_dir = branch_dir.join(QUARANTINE_DIR);
    let files_on_disk: HashMap<String, u64> = if quarantine_dir.is_dir() {
        let mut files = HashMap::new();
        for entry in std::fs::read_dir(&quarantine_dir)? {
            let entry = entry?;
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            let Some(filename) = entry.file_name().to_str().map(str::to_owned) else {
                return Err(StorageError::QuarantineReconciliationFailed {
                    branch_id,
                    reason: "non-utf8 filename in __quarantine__".to_string(),
                });
            };
            let size = entry.metadata()?.len();
            files.insert(filename, size);
        }
        files
    } else {
        HashMap::new()
    };

    let manifest = read_quarantine_manifest(branch_dir).map_err(|inner| {
        StorageError::QuarantineReconciliationFailed {
            branch_id,
            reason: format!("failed to read quarantine inventory: {inner}"),
        }
    })?;

    Ok(BranchQuarantineState {
        manifest,
        files_on_disk,
    })
}

fn reconcile_single_branch(
    branch_path: &Path,
    branch_id: BranchId,
    manifest_live_filenames: &HashSet<String>,
    faults: &mut Vec<RecoveryFault>,
) {
    let quarantine_dir = branch_path.join(QUARANTINE_DIR);
    let state = match load_branch_quarantine_state(branch_path, branch_id) {
        Ok(state) => state,
        Err(StorageError::QuarantineReconciliationFailed { reason, .. }) => {
            faults.push(RecoveryFault::QuarantineInventoryMismatch { branch_id, reason });
            return;
        }
        Err(e) => {
            faults.push(RecoveryFault::QuarantineInventoryMismatch {
                branch_id,
                reason: e.to_string(),
            });
            return;
        }
    };

    // Case: no inventory but __quarantine__/ holds files — pure mismatch,
    // prefer retention and degrade trust.
    if state.manifest.is_none() {
        if !state.files_on_disk.is_empty() {
            faults.push(RecoveryFault::QuarantineInventoryMismatch {
                branch_id,
                reason: "__quarantine__/ contains files but no quarantine.manifest".to_string(),
            });
        }
        return;
    }
    let manifest = state.manifest.unwrap();

    // Collect on-disk quarantine filenames.
    let on_disk: HashSet<String> = state.files_on_disk.keys().cloned().collect();

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

impl SegmentedStore {
    fn retention_branch_dir_views(
        &self,
        load_quarantine: bool,
    ) -> StorageResult<Vec<RetentionBranchDirView>> {
        let Some(segments_dir) = self.segments_dir.as_ref() else {
            return Ok(Vec::new());
        };
        let entries = match std::fs::read_dir(segments_dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(StorageError::Io(e)),
        };

        let mut out = Vec::new();
        for entry in entries {
            let entry = entry.map_err(StorageError::Io)?;
            let branch_dir = entry.path();
            if !branch_dir.is_dir() {
                continue;
            }
            let Some(branch_id) = entry
                .file_name()
                .to_str()
                .and_then(crate::segmented::hex_decode_branch)
            else {
                continue;
            };
            out.push(scan_branch_dir_view(
                &branch_dir,
                branch_id,
                load_quarantine,
            )?);
        }

        Ok(out)
    }

    fn retention_branch_dir_views_for_recovery(
        &self,
        faults: &mut Vec<RecoveryFault>,
    ) -> Vec<RetentionBranchDirView> {
        let Some(segments_dir) = self.segments_dir.as_ref() else {
            return Vec::new();
        };
        let entries = match std::fs::read_dir(segments_dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Vec::new(),
            Err(e) => {
                faults.push(RecoveryFault::Io(e));
                return Vec::new();
            }
        };

        let mut out = Vec::new();
        for entry in entries {
            let entry = match entry {
                Ok(entry) => entry,
                Err(e) => {
                    faults.push(RecoveryFault::Io(e));
                    continue;
                }
            };
            let branch_dir = entry.path();
            if !branch_dir.is_dir() {
                continue;
            }
            let Some(branch_id) = entry
                .file_name()
                .to_str()
                .and_then(crate::segmented::hex_decode_branch)
            else {
                continue;
            };
            match scan_branch_dir_view(&branch_dir, branch_id, false) {
                Ok(view) => out.push(view),
                Err(StorageError::RecoveryFault(fault)) => faults.push(fault),
                Err(StorageError::Io(e)) => faults.push(RecoveryFault::Io(e)),
                Err(StorageError::QuarantineReconciliationFailed { branch_id, reason }) => {
                    faults.push(RecoveryFault::QuarantineInventoryMismatch { branch_id, reason })
                }
                Err(other) => faults.push(RecoveryFault::Io(io::Error::other(other.to_string()))),
            }
        }

        out
    }
}

fn scan_branch_dir_view(
    branch_dir: &Path,
    branch_id: BranchId,
    load_quarantine: bool,
) -> StorageResult<RetentionBranchDirView> {
    let manifest = match crate::manifest::read_manifest(branch_dir) {
        Ok(manifest) => manifest,
        Err(inner) => {
            return Err(StorageError::RecoveryFault(
                RecoveryFault::CorruptManifest { branch_id, inner },
            ));
        }
    };

    let mut top_level_segments = HashMap::new();
    for entry in std::fs::read_dir(branch_dir).map_err(StorageError::Io)? {
        let entry = entry.map_err(StorageError::Io)?;
        let path = entry.path();
        if !path.is_file() || path.extension().and_then(|x| x.to_str()) != Some("sst") {
            continue;
        }
        let Some(filename) = entry.file_name().to_str().map(str::to_owned) else {
            return Err(StorageError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("non-utf8 segment filename in {}", branch_dir.display()),
            )));
        };
        let size = entry.metadata().map_err(StorageError::Io)?.len();
        top_level_segments.insert(
            filename.clone(),
            RetentionTopLevelSegment { filename, size },
        );
    }

    let quarantine = if load_quarantine {
        Some(load_branch_quarantine_state(branch_dir, branch_id)?)
    } else {
        None
    };

    Ok(RetentionBranchDirView {
        branch_id,
        branch_dir: branch_dir.to_path_buf(),
        manifest,
        top_level_segments,
        quarantine,
    })
}

fn descendant_held_filenames_by_source(
    views: &[RetentionBranchDirView],
) -> HashMap<BranchId, HashSet<String>> {
    let mut out: HashMap<BranchId, HashSet<String>> = HashMap::new();

    for view in views {
        let Some(manifest) = view.manifest.as_ref() else {
            continue;
        };
        for layer in &manifest.inherited_layers {
            if layer.status == 2 {
                continue;
            }
            let entry = out.entry(layer.source_branch_id).or_default();
            for manifest_entry in &layer.entries {
                entry.insert(manifest_entry.filename.clone());
            }
        }
    }

    out
}

fn manifest_live_filenames_from_views(
    views: &[RetentionBranchDirView],
) -> HashMap<BranchId, HashSet<String>> {
    let mut out: HashMap<BranchId, HashSet<String>> = HashMap::new();

    for view in views {
        let Some(manifest) = view.manifest.as_ref() else {
            continue;
        };

        let own = out.entry(view.branch_id).or_default();
        for entry in &manifest.entries {
            own.insert(entry.filename.clone());
        }

        for layer in &manifest.inherited_layers {
            if layer.status == 2 {
                continue;
            }
            let source = out.entry(layer.source_branch_id).or_default();
            for entry in &layer.entries {
                source.insert(entry.filename.clone());
            }
        }
    }

    out
}

fn branches_with_legacy_no_manifest_fallback(health: &RecoveryHealth) -> HashSet<BranchId> {
    let mut out = HashSet::new();
    if let RecoveryHealth::Degraded { faults, .. } = health {
        for fault in faults.iter() {
            if let RecoveryFault::NoManifestFallbackUsed { branch_id, .. } = fault {
                out.insert(*branch_id);
            }
        }
    }
    out
}

fn quarantine_stats_from_state(
    branch_id: BranchId,
    state: &BranchQuarantineState,
    manifest_live_filenames: &HashSet<String>,
) -> StorageResult<(u64, usize)> {
    match &state.manifest {
        None => {
            if state.files_on_disk.is_empty() {
                Ok((0, 0))
            } else {
                Err(StorageError::QuarantineReconciliationFailed {
                    branch_id,
                    reason: "files present in __quarantine__ with no inventory".to_string(),
                })
            }
        }
        Some(manifest) => {
            let inventory_filenames: HashSet<String> = manifest
                .entries
                .iter()
                .map(|entry| entry.filename.clone())
                .collect();
            let files_present: HashSet<String> = state.files_on_disk.keys().cloned().collect();

            for filename in &files_present {
                if !inventory_filenames.contains(filename) {
                    return Err(StorageError::QuarantineReconciliationFailed {
                        branch_id,
                        reason: "inventory does not match on-disk quarantine contents".to_string(),
                    });
                }
            }

            let mut retained_entry_count = 0usize;
            let total = manifest
                .entries
                .iter()
                .filter(|entry| {
                    if files_present.contains(&entry.filename) {
                        retained_entry_count += 1;
                        return true;
                    }

                    if manifest_live_filenames.contains(&entry.filename) {
                        // This is the same mismatch reopen reconciliation
                        // refuses: inventory claims quarantine membership for a
                        // file still reachable from a live manifest.
                        return true;
                    }

                    // Benign stale entry: publish happened, but rename never
                    // completed (or purge already removed the file). Reopen
                    // reconciliation drops these entries in place; the
                    // in-session report path should not fail closed on the same
                    // state.
                    false
                })
                .map(|entry| {
                    if !files_present.contains(&entry.filename)
                        && manifest_live_filenames.contains(&entry.filename)
                    {
                        return Err(StorageError::QuarantineReconciliationFailed {
                            branch_id,
                            reason: "inventory does not match on-disk quarantine contents"
                                .to_string(),
                        });
                    }
                    state
                        .files_on_disk
                        .get(&entry.filename)
                        .copied()
                        .ok_or_else(|| StorageError::QuarantineReconciliationFailed {
                            branch_id,
                            reason: "inventory does not match on-disk quarantine contents"
                                .to_string(),
                        })
                })
                .collect::<StorageResult<Vec<_>>>()?
                .into_iter()
                .sum();
            Ok((total, retained_entry_count))
        }
    }
}
