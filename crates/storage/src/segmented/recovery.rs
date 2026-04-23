//! Typed outcome for [`SegmentedStore::recover_segments`].
//!
//! Before SE2 this API returned `RecoverSegmentsInfo` — a stats struct whose
//! fields the engine had to interpret (`max_commit_id` fed directly into a
//! version-floor bump; `layers_dropped` and `corrupt_manifest_branches` carried
//! meaning only for specific branch paths). That shape left three problems:
//!
//! 1. Callers reimplemented classification (was this "lossy but safe" or
//!    "authoritative data gone"?).
//! 2. Callers managed version-floor semantics on storage's behalf.
//! 3. Silent-skip arms inside the walk never surfaced anywhere: corrupt `.sst`
//!    and missing manifest-listed segments just moved a counter.
//!
//! The types here collapse that into one self-contained restart primitive:
//!
//! - [`RecoveredState`] — every piece of state the caller needs (version floor,
//!   per-branch version map, count summary, classified health).
//! - [`RecoveryHealth`] — `Healthy` or `Degraded { faults, class }`.
//! - [`DegradationClass`] — lets callers (D4) branch on severity without
//!   iterating raw faults: `DataLoss` is authoritative; `PolicyDowngrade` is
//!   operator-consentable; `Telemetry` is rebuildable.
//! - [`RecoveryFault`] — the specific defects the walk observed, one per site.

use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use strata_core::id::CommitVersion;
use strata_core::types::BranchId;
use thiserror::Error;

/// Self-contained outcome of [`SegmentedStore::recover_segments`].
///
/// The engine consumes this directly via
/// `TransactionCoordinator::apply_storage_recovery`; it never needs to reach
/// into storage internals to rebuild version state or interpret a raw fault
/// list.
#[derive(Debug)]
#[non_exhaustive]
pub struct RecoveredState {
    /// Version floor the coordinator should adopt — the max `CommitVersion`
    /// observed across every loaded segment (own and inherited). Zero when
    /// recovery found no segments.
    pub version_floor: CommitVersion,
    /// Per-branch max `CommitVersion` seen in loaded segments. Mirrors the
    /// value now installed on `BranchState::max_version` for each branch
    /// touched during recovery. Authoritative for post-restart `fork_branch`
    /// and snapshot-driven version arithmetic. Keyed by `BranchId` (which
    /// does not implement `Ord`, so `HashMap` rather than `BTreeMap`).
    pub branch_versions: HashMap<BranchId, CommitVersion>,
    /// Total number of `.sst` segments loaded across all branches.
    pub segments_loaded: usize,
    /// Number of branch subdirectories that contributed at least one loaded
    /// segment or inherited layer.
    pub branches_recovered: usize,
    /// Classified recovery health. See [`RecoveryHealth`] and
    /// [`DegradationClass`].
    pub health: RecoveryHealth,
}

impl RecoveredState {
    /// A clean-recovery outcome with nothing loaded (e.g. ephemeral store or
    /// an empty segments directory).
    pub fn empty() -> Self {
        Self {
            version_floor: CommitVersion::ZERO,
            branch_versions: HashMap::new(),
            segments_loaded: 0,
            branches_recovered: 0,
            health: RecoveryHealth::Healthy,
        }
    }
}

/// Classified health of a storage recovery.
///
/// Callers match on the variant (and, for `Degraded`, on
/// [`DegradationClass`]) to decide strict-vs-lossy policy. They do not
/// iterate `faults` to reconstruct what storage already knows.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum RecoveryHealth {
    /// Recovery completed with no observed faults.
    Healthy,
    /// Recovery completed but observed one or more faults.
    Degraded {
        /// Ordered list of faults observed during the walk.
        faults: Arc<[RecoveryFault]>,
        /// Severity-dominant classification across `faults`.
        class: DegradationClass,
    },
}

/// Severity class for a [`RecoveryHealth::Degraded`] outcome.
///
/// Strict callers refuse `DataLoss`. `PolicyDowngrade` is operator-consentable
/// (explicit `--allow-missing-manifest` or equivalent). `Telemetry` is
/// always acceptable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum DegradationClass {
    /// Authoritative state was lost: corrupt segment, corrupt manifest,
    /// manifest-listed-but-missing segment, inherited layer whose source
    /// branch is gone, or a per-entry I/O failure.
    DataLoss,
    /// A legacy-compatible fallback was engaged (e.g. no-manifest → L0
    /// promotion). Safe to continue only with explicit operator consent.
    PolicyDowngrade,
    /// A rebuildable-cache failure was skipped. Not currently emitted; the
    /// variant exists so future faults that fit this category can be
    /// classified without widening the enum.
    Telemetry,
}

impl DegradationClass {
    /// Pick the worst class across a list of faults. Returns `Telemetry` only
    /// when every fault classifies there (or the list is empty, which
    /// callers should never observe — they classify `Healthy` first).
    pub fn worst(faults: &[RecoveryFault]) -> Self {
        let mut worst = DegradationClass::Telemetry;
        for fault in faults {
            let class = fault.class();
            if class.severity() > worst.severity() {
                worst = class;
            }
        }
        worst
    }

    fn severity(self) -> u8 {
        match self {
            DegradationClass::Telemetry => 0,
            DegradationClass::PolicyDowngrade => 1,
            DegradationClass::DataLoss => 2,
        }
    }
}

/// A single defect observed during storage recovery.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum RecoveryFault {
    /// A `.sst` file failed to open as a segment. The file on disk is
    /// retained; subsequent orphan GC may reclaim it once a known-good
    /// manifest is present.
    #[error("corrupt segment file {file:?}: {inner}")]
    CorruptSegment {
        /// Absolute path to the segment file that failed to open.
        file: PathBuf,
        /// Underlying I/O or decode error from `KVSegment::open`.
        #[source]
        inner: io::Error,
    },
    /// A branch's `segments.manifest` failed to parse. The branch's own
    /// segments are NOT loaded (would reintroduce orphans — #1680); children
    /// inheriting from this branch may still pick up segments individually.
    #[error("corrupt manifest for branch {branch_id}: {inner}")]
    CorruptManifest {
        /// Branch whose manifest could not be parsed.
        branch_id: BranchId,
        /// Underlying read/parse error.
        #[source]
        inner: io::Error,
    },
    /// The manifest listed a segment filename that is not on disk. The branch
    /// continues loading the segments that are present; the missing one
    /// represents authoritative loss.
    #[error("manifest for branch {branch_id} lists missing file {file}")]
    MissingManifestListed {
        /// Branch whose manifest references the missing file.
        branch_id: BranchId,
        /// Filename recorded in the manifest but not found on disk.
        file: String,
    },
    /// A child branch's inherited layer lost one or more required segment
    /// entries during recovery. The child loses visibility into data that was
    /// visible before the crash, even if some entries from the same layer
    /// were still loadable.
    #[error(
        "inherited layer lost for branch {child} (source {source_branch}, fork_version {fork_version})"
    )]
    InheritedLayerLost {
        /// Child branch whose layer was dropped.
        child: BranchId,
        /// Source branch from which the layer derived. Named `source_branch`
        /// rather than `source` to avoid collision with thiserror's magic
        /// `#[source]` field detection.
        source_branch: BranchId,
        /// Fork version that, together with `source_branch`, identifies the
        /// inherited layer whose visibility was degraded.
        fork_version: CommitVersion,
    },
    /// A branch had no manifest on disk; recovery promoted every discovered
    /// `.sst` to L0 for backward compatibility. Strict callers refuse; lossy
    /// callers with explicit operator opt-in accept.
    #[error("no manifest for branch {branch_id}; {segments_promoted} segments promoted to L0 via legacy fallback")]
    NoManifestFallbackUsed {
        /// Branch whose segments were promoted.
        branch_id: BranchId,
        /// Number of segments promoted to L0.
        segments_promoted: usize,
    },
    /// A per-entry I/O failure that prevented a single branch or segment
    /// from loading but did not block the rest of the walk. Pre-walk I/O
    /// failures are returned as `Err(StorageError)` instead.
    #[error("recovery I/O error: {0}")]
    Io(#[source] io::Error),
    /// Reopen quarantine reconciliation observed disagreement between
    /// the branch's `quarantine.manifest` inventory and the on-disk
    /// `__quarantine__/` contents. Per the B5 retention contract
    /// §"Quarantine reconciliation", the implementation prefers
    /// retention and degrades reclaim trust until a full
    /// rebuild-equivalent reconciliation completes.
    #[error("quarantine inventory mismatch for branch {branch_id}: {reason}")]
    QuarantineInventoryMismatch {
        /// Branch whose quarantine state could not be reconciled.
        branch_id: BranchId,
        /// Short description of the disagreement observed.
        reason: String,
    },
}

impl RecoveryFault {
    /// Classify this fault into a [`DegradationClass`].
    pub fn class(&self) -> DegradationClass {
        match self {
            RecoveryFault::CorruptSegment { .. }
            | RecoveryFault::CorruptManifest { .. }
            | RecoveryFault::MissingManifestListed { .. }
            | RecoveryFault::InheritedLayerLost { .. }
            | RecoveryFault::Io(_) => DegradationClass::DataLoss,
            RecoveryFault::NoManifestFallbackUsed { .. }
            | RecoveryFault::QuarantineInventoryMismatch { .. } => {
                DegradationClass::PolicyDowngrade
            }
        }
    }
}

impl RecoveryHealth {
    /// Construct a [`RecoveryHealth`] from a fault vec: `Healthy` when empty,
    /// `Degraded` otherwise with the severity-dominant class precomputed.
    pub fn from_faults(faults: Vec<RecoveryFault>) -> Self {
        if faults.is_empty() {
            RecoveryHealth::Healthy
        } else {
            let class = DegradationClass::worst(&faults);
            RecoveryHealth::Degraded {
                faults: faults.into(),
                class,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worst_picks_data_loss_over_policy_downgrade() {
        let faults = vec![
            RecoveryFault::NoManifestFallbackUsed {
                branch_id: BranchId::from_bytes([1; 16]),
                segments_promoted: 2,
            },
            RecoveryFault::CorruptSegment {
                file: PathBuf::from("/tmp/x.sst"),
                inner: io::Error::new(io::ErrorKind::InvalidData, "bad"),
            },
        ];
        assert_eq!(DegradationClass::worst(&faults), DegradationClass::DataLoss);
    }

    #[test]
    fn worst_picks_policy_downgrade_when_no_data_loss() {
        let faults = vec![RecoveryFault::NoManifestFallbackUsed {
            branch_id: BranchId::from_bytes([2; 16]),
            segments_promoted: 1,
        }];
        assert_eq!(
            DegradationClass::worst(&faults),
            DegradationClass::PolicyDowngrade
        );
    }
}
