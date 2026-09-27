//! Lifecycle health facts.

use super::{LifecycleError, LifecycleResult};
use strata_core::BranchId;

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub(crate) enum RecoveryHealth {
    Healthy,
    Degraded {
        class: RecoveryDegradationClass,
        faults: Vec<RecoveryFault>,
    },
    Failed {
        fault: RecoveryFault,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub(crate) enum RecoveryDegradationClass {
    DataLoss,
    PolicyDowngrade,
    Telemetry,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RecoveryFault {
    kind: RecoveryFaultKind,
    reason: &'static str,
    // Optional branch the fault was raised against. `None` means the
    // fault is unscoped (global/service-level) — callers asserting
    // unrelatedness to a candidate must treat unscoped faults
    // conservatively. Threaded from quarantine inventory mismatches,
    // branch-scoped manifest issues, and other branch-specific faults
    // when the caller knows the affected branch.
    affected_branch: Option<BranchId>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub(crate) enum RecoveryFaultKind {
    CorruptManifest,
    CorruptSnapshot,
    CorruptWal,
    MissingManifestObject,
    MissingSnapshotObject,
    MissingTableObject,
    MissingTableManifestBase,
    InheritedLayerLoss,
    NoManifestFallback,
    IoFailure,
    QuarantineInventoryMismatch,
    TimelineMismatch,
    WalTailRepairFailed,
    /// #2690: the durable commit watermark attests sealed-durable commits that
    /// no recovery source (checkpoint, table manifest, surviving WAL records)
    /// can reproduce — WAL segments holding committed data were removed.
    WalCommittedSuffixMissing,
}

impl RecoveryHealth {
    pub(crate) fn degraded(
        class: RecoveryDegradationClass,
        faults: Vec<RecoveryFault>,
    ) -> LifecycleResult<Self> {
        if faults.is_empty() {
            return Err(LifecycleError::RecoveryFailed {
                reason: "degraded recovery health requires at least one fault",
            });
        }
        Ok(Self::Degraded { class, faults })
    }

    pub(crate) const fn failed(fault: RecoveryFault) -> Self {
        Self::Failed { fault }
    }

    pub(crate) const fn is_healthy(&self) -> bool {
        matches!(self, Self::Healthy)
    }

    pub(crate) const fn fault_count(&self) -> usize {
        match self {
            Self::Healthy => 0,
            Self::Degraded { faults, .. } => faults.len(),
            Self::Failed { .. } => 1,
        }
    }

    /// Every fault this health carries (none when healthy).
    pub(crate) fn faults(&self) -> &[RecoveryFault] {
        match self {
            Self::Healthy => &[],
            Self::Degraded { faults, .. } => faults,
            Self::Failed { fault } => std::slice::from_ref(fault),
        }
    }
}

impl RecoveryFault {
    pub(crate) fn new(kind: RecoveryFaultKind, reason: &'static str) -> LifecycleResult<Self> {
        if reason.is_empty() {
            return Err(LifecycleError::RecoveryFailed {
                reason: "recovery fault reason must not be empty",
            });
        }
        Ok(Self {
            kind,
            reason,
            affected_branch: None,
        })
    }

    /// Attach the branch this fault was raised against. Used by callers
    /// (e.g. quarantine inventory mismatches) that know the affected
    /// branch so downstream admission checks can prove the fault is
    /// unrelated to a candidate before allowing reclaim under Telemetry
    /// debt.
    pub(crate) fn with_affected_branch(mut self, branch: BranchId) -> Self {
        self.affected_branch = Some(branch);
        self
    }

    pub(crate) const fn kind(&self) -> RecoveryFaultKind {
        self.kind
    }

    pub(crate) const fn reason(&self) -> &'static str {
        self.reason
    }

    pub(crate) const fn affected_branch(&self) -> Option<BranchId> {
        self.affected_branch
    }
}

impl RecoveryHealth {
    /// Returns `true` when the health has at least one fault whose
    /// affected branch matches `branch`. Used by quarantine/purge
    /// admission to refuse reclaim under Telemetry debt that names the
    /// candidate's branch — an "unrelatedness" check defined in plan
    /// rule 2 of Quarantine Admission. Unscoped faults
    /// (`affected_branch == None`) do not contribute to the match here;
    /// callers that want a conservative reject for unscoped faults
    /// inspect `recovery_health_blocks_reclaim` (defined in
    /// `crate::lifecycle::quarantine`) which treats any non-Telemetry
    /// `Degraded` and any `Failed` fault as blocking regardless of
    /// scope.
    pub(crate) fn has_fault_targeting_branch(&self, branch: BranchId) -> bool {
        match self {
            Self::Healthy => false,
            Self::Degraded { faults, .. } => faults
                .iter()
                .any(|fault| fault.affected_branch() == Some(branch)),
            Self::Failed { fault } => fault.affected_branch() == Some(branch),
        }
    }
}
