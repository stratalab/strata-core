//! Proof-bound row pruning for branch compaction.

use super::error::{BranchCompactionInvalidity, BranchRuntimeError, BranchRuntimeResult};
use super::facts::SharedTableRegistry;
use super::read::{BranchInheritedLayer, BranchOwnedTable, BranchTimestampCoverage};
use super::state::compaction::{BranchCompactionCandidate, BranchCompactionRetentionPolicy};
use super::state::BranchLocalState;
use crate::row::PhysicalKey;
use crate::table::{
    MergeTableCursor, TableCompactionDecision, TableCompactionDropReason, TableCompactionPolicy,
    TableCompactionRowContext, TableCursor, TableIdentity, TableRow, TableRuntimeResult,
};
use strata_core::{BranchId, CommitVersion, Timestamp};

const FINGERPRINT_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FINGERPRINT_PRIME: u64 = 0x0000_0100_0000_01b3;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BranchCompactionPruningProof {
    branch_id: BranchId,
    proof_epoch: u64,
    recovery_health_epoch: u64,
    branch_state_fingerprint: u64,
    retained_version_floor: CommitVersion,
    retained_timestamp_floor: Option<Timestamp>,
    visible_version: CommitVersion,
    pinned_view_floor: Option<CommitVersion>,
    table_manifest_coverage_floor: CommitVersion,
    // Row-elision gates are merged onto the parent proof so tombstone and
    // TTL deletion cannot be detached from branch freshness, recovery health,
    // inherited-layer safety, and shared-table safety. Pinned by
    // branch::tests::row_pruning::tombstone_ttl.
    tombstone_elision_safe: bool,
    ttl_expired_at_or_before: Option<Timestamp>,
    max_versions_per_key: Option<usize>,
    // These gates are intentionally parent-proof fields: row pruning can only
    // delete below-floor rows after the same fresh proof has established both
    // inherited-layer absence and candidate-table non-sharing.
    no_readable_inherited_layers: bool,
    candidate_tables_not_shared: bool,
    recovery_health: BranchRecoveryHealthAttestation,
}

/// Attestation that the caller has consulted live recovery health and
/// found it suitable for pruning.
///
/// The lifecycle layer owns the source of recovery health
/// (`RecoveryHealth`). Branch-layer pruning cannot reach upward to the
/// lifecycle crate, so the lifecycle code must construct this
/// attestation by inspecting its own `RecoveryHealth` value and only
/// setting `Healthy` when `RecoveryHealth::Healthy` is observed.
///
/// Unit tests construct `Healthy` directly because they bypass the
/// lifecycle layer; lifecycle integration tests/production code must
/// use the documented helper.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BranchRecoveryHealthAttestation {
    /// Caller has not attested recovery is healthy. Pruning rejects.
    Unknown,
    /// Caller has attested live `RecoveryHealth::Healthy`. Pruning may
    /// proceed (subject to the other proof gates).
    Healthy,
}

#[derive(Clone, Debug)]
pub(crate) struct BranchCompactionPruningPolicy {
    retention_policy: BranchCompactionRetentionPolicy,
    proof: BranchCompactionPruningProof,
    current_key: Option<PhysicalKey>,
    kept_value_versions_for_key: usize,
    kept_below_floor_survivor_for_key: bool,
}

#[allow(
    dead_code,
    reason = "proof accessors are consumed across direct, lifecycle, and generated pruning coverage"
)]
impl BranchCompactionPruningProof {
    pub(crate) fn new(
        branch_id: BranchId,
        proof_epoch: u64,
        recovery_health_epoch: u64,
        branch_state_fingerprint: u64,
        retained_version_floor: CommitVersion,
        visible_version: CommitVersion,
    ) -> BranchRuntimeResult<Self> {
        let proof = Self {
            branch_id,
            proof_epoch,
            recovery_health_epoch,
            branch_state_fingerprint,
            retained_version_floor,
            retained_timestamp_floor: None,
            visible_version,
            pinned_view_floor: None,
            table_manifest_coverage_floor: retained_version_floor,
            tombstone_elision_safe: false,
            ttl_expired_at_or_before: None,
            max_versions_per_key: None,
            no_readable_inherited_layers: false,
            candidate_tables_not_shared: false,
            recovery_health: BranchRecoveryHealthAttestation::Unknown,
        };
        proof.validate_static()?;
        Ok(proof)
    }

    pub(crate) fn from_branch_state(
        branch: &BranchLocalState,
        retained_version_floor: CommitVersion,
    ) -> BranchRuntimeResult<Self> {
        let visible_version = branch
            .max_commit_version()
            .unwrap_or(CommitVersion::ZERO)
            .max(retained_version_floor);
        Self::new(
            branch.branch_id(),
            1,
            1,
            branch_pruning_fingerprint(branch),
            retained_version_floor,
            visible_version,
        )
    }

    pub(crate) const fn branch_id(self) -> BranchId {
        self.branch_id
    }

    pub(crate) const fn retained_version_floor(self) -> CommitVersion {
        self.retained_version_floor
    }

    pub(crate) const fn retained_timestamp_floor(self) -> Option<Timestamp> {
        self.retained_timestamp_floor
    }

    pub(crate) const fn visible_version(self) -> CommitVersion {
        self.visible_version
    }

    pub(crate) const fn branch_state_fingerprint(self) -> u64 {
        self.branch_state_fingerprint
    }

    pub(crate) const fn max_versions_per_key(self) -> Option<usize> {
        self.max_versions_per_key
    }

    pub(crate) const fn tombstone_elision_safe(self) -> bool {
        self.tombstone_elision_safe
    }

    pub(crate) const fn ttl_expired_at_or_before(self) -> Option<Timestamp> {
        self.ttl_expired_at_or_before
    }

    pub(crate) const fn no_readable_inherited_layers(self) -> bool {
        self.no_readable_inherited_layers
    }

    pub(crate) const fn candidate_tables_not_shared(self) -> bool {
        self.candidate_tables_not_shared
    }

    pub(crate) const fn recovery_health(self) -> BranchRecoveryHealthAttestation {
        self.recovery_health
    }

    pub(crate) fn with_retained_timestamp_floor(
        mut self,
        timestamp: Timestamp,
    ) -> BranchRuntimeResult<Self> {
        self.retained_timestamp_floor = Some(timestamp);
        self.validate_static()?;
        Ok(self)
    }

    pub(crate) fn with_pinned_view_floor(
        mut self,
        floor: CommitVersion,
    ) -> BranchRuntimeResult<Self> {
        self.pinned_view_floor = Some(floor);
        self.validate_static()?;
        Ok(self)
    }

    pub(crate) fn with_table_manifest_coverage_floor(
        mut self,
        floor: CommitVersion,
    ) -> BranchRuntimeResult<Self> {
        self.table_manifest_coverage_floor = floor;
        self.validate_static()?;
        Ok(self)
    }

    pub(crate) fn with_tombstone_elision(mut self) -> BranchRuntimeResult<Self> {
        self.tombstone_elision_safe = true;
        self.validate_static()?;
        Ok(self)
    }

    pub(crate) fn with_ttl_elision(
        mut self,
        expired_at_or_before: Timestamp,
    ) -> BranchRuntimeResult<Self> {
        self.ttl_expired_at_or_before = Some(expired_at_or_before);
        self.validate_static()?;
        Ok(self)
    }

    pub(crate) fn with_max_versions_per_key(
        mut self,
        max_versions_per_key: usize,
    ) -> BranchRuntimeResult<Self> {
        self.max_versions_per_key = Some(max_versions_per_key);
        self.validate_static()?;
        Ok(self)
    }

    pub(crate) fn with_no_readable_inherited_layers(mut self) -> BranchRuntimeResult<Self> {
        self.no_readable_inherited_layers = true;
        self.validate_static()?;
        Ok(self)
    }

    pub(crate) fn with_candidate_tables_not_shared(mut self) -> BranchRuntimeResult<Self> {
        self.candidate_tables_not_shared = true;
        self.validate_static()?;
        Ok(self)
    }

    pub(crate) fn with_recovery_health(
        mut self,
        attestation: BranchRecoveryHealthAttestation,
    ) -> BranchRuntimeResult<Self> {
        self.recovery_health = attestation;
        self.validate_static()?;
        Ok(self)
    }

    /// Return true only when a fresh `SharedTableRegistry` shows that no
    /// candidate input/overlap table identity is referenced by more than one
    /// branch snapshot.
    pub(crate) fn derive_candidate_tables_not_shared(
        candidate: &BranchCompactionCandidate,
        registry: &SharedTableRegistry,
    ) -> bool {
        for table_ref in candidate
            .input_refs()
            .iter()
            .chain(candidate.overlap_refs().iter())
        {
            if registry.reference_count(table_ref.table_identity()) > 1 {
                // The candidate table is referenced from more than just
                // this branch's own snapshot — pruning may affect another
                // branch's reads.
                return false;
            }
        }
        true
    }

    fn validate_static(self) -> BranchRuntimeResult<()> {
        if self.proof_epoch == 0 {
            return Err(BranchRuntimeError::InvalidCompaction {
                reason: BranchCompactionInvalidity::ProofEpochInvalid,
            });
        }
        if self.recovery_health_epoch == 0 {
            return Err(BranchRuntimeError::InvalidCompaction {
                reason: BranchCompactionInvalidity::ProofUnsafeRecoveryHealth,
            });
        }
        if self.branch_state_fingerprint == 0 {
            return Err(BranchRuntimeError::InvalidCompaction {
                reason: BranchCompactionInvalidity::ProofFingerprintInvalid,
            });
        }
        if self.retained_version_floor > self.visible_version {
            return Err(BranchRuntimeError::InvalidCompaction {
                reason: BranchCompactionInvalidity::RetainedFloorAboveVisible,
            });
        }
        if self
            .pinned_view_floor
            .is_some_and(|floor| floor < self.retained_version_floor)
        {
            return Err(BranchRuntimeError::InvalidCompaction {
                reason: BranchCompactionInvalidity::PinnedViewBelowFloor,
            });
        }
        if self.table_manifest_coverage_floor > self.retained_version_floor {
            return Err(BranchRuntimeError::InvalidCompaction {
                reason: BranchCompactionInvalidity::TableManifestCoverageBeyondFloor,
            });
        }
        if let (Some(retained_timestamp_floor), Some(timestamp)) =
            (self.retained_timestamp_floor, self.ttl_expired_at_or_before)
        {
            if timestamp > retained_timestamp_floor {
                return Err(BranchRuntimeError::InvalidCompaction {
                    reason: BranchCompactionInvalidity::TtlCutoffExceedsTimestampFloor,
                });
            }
        }
        Ok(())
    }

    pub(crate) fn validate_for_branch(
        self,
        branch: &BranchLocalState,
        candidate: &BranchCompactionCandidate,
        retention_policy: BranchCompactionRetentionPolicy,
    ) -> BranchRuntimeResult<()> {
        self.validate_static()?;
        if retention_policy == BranchCompactionRetentionPolicy::KeepAll {
            return Ok(());
        }
        if self.branch_id != branch.branch_id() {
            return Err(BranchRuntimeError::InvalidCompaction {
                reason: BranchCompactionInvalidity::ProofBranchMismatch,
            });
        }
        if self.branch_state_fingerprint != branch_pruning_fingerprint(branch) {
            return Err(BranchRuntimeError::InvalidCompaction {
                reason: BranchCompactionInvalidity::ProofStale,
            });
        }
        // The proof's visible_version is a self-declared witness of the
        // caller's observation of the branch. The fingerprint above
        // binds to actual row contents, so a caller who claims to have
        // seen FEWER versions than actually exist is inconsistent with
        // the state they fingerprinted — reject. Claiming to have seen
        // more is allowed (e.g., when the caller wants `floor > max_commit`
        // to drop all history below floor).
        let actual_visible = branch.max_commit_version().unwrap_or(CommitVersion::ZERO);
        if self.visible_version < actual_visible {
            return Err(BranchRuntimeError::InvalidCompaction {
                reason: BranchCompactionInvalidity::ProofVisibleVersionBelowState,
            });
        }
        if self.retained_timestamp_floor.is_none() {
            return Err(BranchRuntimeError::InvalidCompaction {
                reason: BranchCompactionInvalidity::RetainedTimestampFloorMissing,
            });
        }
        validate_timestamp_floor(branch.timestamp_coverage(), self.retained_timestamp_floor)?;
        validate_inheritance_safety(branch.inherited_layers(), self.no_readable_inherited_layers)?;
        validate_shared_table_safety(self.candidate_tables_not_shared)?;
        validate_recovery_health(self.recovery_health)?;
        validate_policy_specific_safety(branch, candidate, retention_policy, self)?;
        Ok(())
    }
}

impl BranchCompactionPruningPolicy {
    pub(crate) fn new(
        retention_policy: BranchCompactionRetentionPolicy,
        proof: BranchCompactionPruningProof,
    ) -> Self {
        Self {
            retention_policy,
            proof,
            current_key: None,
            kept_value_versions_for_key: 0,
            kept_below_floor_survivor_for_key: false,
        }
    }

    fn reset_key_if_needed(&mut self, row: &TableRow) {
        if self.current_key.as_ref() != Some(row.physical_key()) {
            self.current_key = Some(row.physical_key().clone());
            self.kept_value_versions_for_key = 0;
            self.kept_below_floor_survivor_for_key = false;
        }
    }

    fn record_keep(&mut self, row: &TableRow) {
        if !row.is_tombstone() {
            self.kept_value_versions_for_key = self.kept_value_versions_for_key.saturating_add(1);
            if self.row_is_below_floors(row) {
                self.kept_below_floor_survivor_for_key = true;
            }
        }
    }

    fn row_is_below_floors(&self, row: &TableRow) -> bool {
        if row.commit_version() >= self.proof.retained_version_floor {
            return false;
        }
        if self
            .proof
            .retained_timestamp_floor
            .is_some_and(|floor| row.commit_timestamp() >= floor)
        {
            return false;
        }
        true
    }

    fn decide_below_floor_value(&mut self) -> TableCompactionDecision {
        let max_versions = self.proof.max_versions_per_key.unwrap_or(0);
        if !self.kept_below_floor_survivor_for_key {
            return TableCompactionDecision::Keep;
        }
        if max_versions != 0 && self.kept_value_versions_for_key < max_versions {
            return TableCompactionDecision::Keep;
        }
        TableCompactionDecision::drop(TableCompactionDropReason::OlderVersion)
    }

    fn decide_expired_value(&mut self, row: &TableRow) -> TableCompactionDecision {
        if !row_is_expired_by_proof(row, self.proof.ttl_expired_at_or_before) {
            return TableCompactionDecision::Keep;
        }
        if self.kept_value_versions_for_key == 0 && !self.kept_below_floor_survivor_for_key {
            return TableCompactionDecision::Keep;
        }
        TableCompactionDecision::drop(TableCompactionDropReason::Expired)
    }

    fn decide_row(&mut self, row: &TableRow) -> TableCompactionDecision {
        self.reset_key_if_needed(row);
        if row.physical_key().branch_id() != self.proof.branch_id {
            return TableCompactionDecision::Keep;
        }
        if self.proof.retained_version_floor == CommitVersion::ZERO {
            return TableCompactionDecision::Keep;
        }
        if !self.row_is_below_floors(row) {
            return TableCompactionDecision::Keep;
        }
        match self.retention_policy {
            BranchCompactionRetentionPolicy::KeepAll => TableCompactionDecision::Keep,
            BranchCompactionRetentionPolicy::DropOlderVersions => {
                if row.is_tombstone() {
                    TableCompactionDecision::Keep
                } else {
                    self.decide_below_floor_value()
                }
            }
            BranchCompactionRetentionPolicy::DropTombstones => {
                if row.is_tombstone() {
                    TableCompactionDecision::drop(TableCompactionDropReason::TombstoneElided)
                } else {
                    TableCompactionDecision::Keep
                }
            }
            BranchCompactionRetentionPolicy::DropExpired => self.decide_expired_value(row),
        }
    }
}

impl TableCompactionPolicy for BranchCompactionPruningPolicy {
    fn decide(
        &mut self,
        _context: &TableCompactionRowContext<'_>,
        row: &TableRow,
    ) -> TableRuntimeResult<TableCompactionDecision> {
        let decision = self.decide_row(row);
        if decision == TableCompactionDecision::Keep {
            self.record_keep(row);
        }
        Ok(decision)
    }
}

pub(crate) fn branch_pruning_fingerprint(branch: &BranchLocalState) -> u64 {
    let mut hash = FINGERPRINT_OFFSET;
    hash_bytes(&mut hash, branch.branch_id().as_bytes());
    hash_u64(&mut hash, branch.active_row_count() as u64);
    for row in branch.active().iter() {
        hash_row(&mut hash, row.as_ref());
    }
    hash_u64(&mut hash, branch.frozen_table_count() as u64);
    for table in branch.frozen() {
        hash_u64(&mut hash, table.len() as u64);
        for row in table.iter() {
            hash_row(&mut hash, row.as_ref());
        }
    }
    hash_owned_levels(&mut hash, branch.owned_levels());
    hash_u64(&mut hash, branch.inherited_layer_count() as u64);
    for layer in branch.inherited_layers() {
        hash_bytes(&mut hash, layer.source_branch_id().as_bytes());
        hash_u64(&mut hash, layer.fork_version().as_u64());
        hash_u64(&mut hash, layer.status() as u64);
        hash_owned_levels(&mut hash, layer.owned_levels());
    }
    hash.max(1)
}

fn validate_timestamp_floor(
    coverage: BranchTimestampCoverage,
    floor: Option<Timestamp>,
) -> BranchRuntimeResult<()> {
    let Some(floor) = floor else {
        return Ok(());
    };
    if coverage.covers_timestamp_floor(floor) {
        Ok(())
    } else {
        Err(BranchRuntimeError::InvalidCompaction {
            reason: BranchCompactionInvalidity::TimestampFloorWithoutCoverage,
        })
    }
}

fn validate_inheritance_safety(
    layers: &[BranchInheritedLayer],
    no_readable_inherited_layers: bool,
) -> BranchRuntimeResult<()> {
    if !no_readable_inherited_layers {
        return Err(BranchRuntimeError::InvalidCompaction {
            reason: BranchCompactionInvalidity::InheritedLayerUnknown,
        });
    }
    if !layers.is_empty() {
        return Err(BranchRuntimeError::InvalidCompaction {
            reason: BranchCompactionInvalidity::InheritedLayerUnsafe,
        });
    }
    Ok(())
}

fn validate_shared_table_safety(candidate_tables_not_shared: bool) -> BranchRuntimeResult<()> {
    if !candidate_tables_not_shared {
        return Err(BranchRuntimeError::InvalidCompaction {
            reason: BranchCompactionInvalidity::SharedTableSafetyUnknown,
        });
    }
    Ok(())
}

fn validate_recovery_health(
    attestation: BranchRecoveryHealthAttestation,
) -> BranchRuntimeResult<()> {
    match attestation {
        BranchRecoveryHealthAttestation::Healthy => Ok(()),
        BranchRecoveryHealthAttestation::Unknown => Err(BranchRuntimeError::InvalidCompaction {
            reason: BranchCompactionInvalidity::ProofUnsafeRecoveryHealth,
        }),
    }
}

fn validate_policy_specific_safety(
    branch: &BranchLocalState,
    candidate: &BranchCompactionCandidate,
    retention_policy: BranchCompactionRetentionPolicy,
    proof: BranchCompactionPruningProof,
) -> BranchRuntimeResult<()> {
    match retention_policy {
        BranchCompactionRetentionPolicy::KeepAll
        | BranchCompactionRetentionPolicy::DropOlderVersions => Ok(()),
        BranchCompactionRetentionPolicy::DropTombstones => {
            if !proof.tombstone_elision_safe {
                return Err(BranchRuntimeError::InvalidCompaction {
                    reason: BranchCompactionInvalidity::TombstoneElisionMissing,
                });
            }
            if !candidate.bottommost_for_branch()
                || !candidate_is_last_configured_level(branch, candidate)
            {
                return Err(BranchRuntimeError::InvalidCompaction {
                    reason: BranchCompactionInvalidity::TombstoneElisionNotBottommost,
                });
            }
            if candidate_has_tombstone_resurrection_risk(branch, candidate, proof)? {
                return Err(BranchRuntimeError::InvalidCompaction {
                    reason: BranchCompactionInvalidity::TombstoneResurrectionRisk,
                });
            }
            Ok(())
        }
        BranchCompactionRetentionPolicy::DropExpired => {
            if proof.ttl_expired_at_or_before.is_none() {
                return Err(BranchRuntimeError::InvalidCompaction {
                    reason: BranchCompactionInvalidity::TtlElisionMissing,
                });
            }
            if !candidate.bottommost_for_branch()
                || !candidate_is_last_configured_level(branch, candidate)
            {
                return Err(BranchRuntimeError::InvalidCompaction {
                    reason: BranchCompactionInvalidity::TtlElisionNotBottommost,
                });
            }
            Ok(())
        }
    }
}

fn candidate_is_last_configured_level(
    branch: &BranchLocalState,
    candidate: &BranchCompactionCandidate,
) -> bool {
    usize::from(candidate.output_level().raw()).saturating_add(1)
        == branch.config().max_level_count()
}

fn candidate_has_tombstone_resurrection_risk(
    branch: &BranchLocalState,
    candidate: &BranchCompactionCandidate,
    proof: BranchCompactionPruningProof,
) -> BranchRuntimeResult<bool> {
    // BS4.4e: stream the candidate + overlap tables through a merged cursor instead of flattening every
    // row into a `Vec`. The merge yields ascending internal key, so rows of one physical key are
    // contiguous and — via the inverted commit suffix — in descending commit order. Within a physical-key
    // group a below-floor tombstone seen at a higher commit would resurrect any later (lower-commit)
    // surviving live row; O(1) state per group replaces the old O(n^2) self-join.
    let mut cursor = candidate_merge_cursor(branch, candidate)?;
    cursor
        .seek_to_first()
        .map_err(|source| BranchRuntimeError::TableRuntime { source })?;
    let mut group_key: Option<PhysicalKey> = None;
    let mut max_below_floor_tombstone_commit: Option<CommitVersion> = None;
    while let Some(row) = cursor.current() {
        // Copy out what we need so the row borrow ends before `advance` (same idiom as
        // `try_for_each_reader_row`).
        let physical_key = row.physical_key().clone();
        let commit = row.commit_version();
        let is_tombstone = row.is_tombstone();
        let below_floors = row_below_floors(row, proof);
        let would_drop = row_would_drop_as_expired(row, proof);
        if group_key.as_ref() != Some(&physical_key) {
            group_key = Some(physical_key);
            max_below_floor_tombstone_commit = None;
        }
        // Resurrection risk ⟺ a surviving live row exists STRICTLY below a below-floor tombstone of the
        // same key (matches the old self-join's `other.commit < tombstone.commit`). The strict compare
        // avoids treating a same-commit duplicate internal key — an invariant violation — as risk.
        if !is_tombstone
            && !would_drop
            && max_below_floor_tombstone_commit
                .is_some_and(|tombstone| tombstone.as_u64() > commit.as_u64())
        {
            return Ok(true);
        }
        if is_tombstone && below_floors {
            max_below_floor_tombstone_commit = Some(match max_below_floor_tombstone_commit {
                Some(existing) if existing.as_u64() >= commit.as_u64() => existing,
                _ => commit,
            });
        }
        cursor
            .advance()
            .map_err(|source| BranchRuntimeError::TableRuntime { source })?;
    }
    Ok(false)
}

fn candidate_merge_cursor<'a>(
    branch: &'a BranchLocalState,
    candidate: &BranchCompactionCandidate,
) -> BranchRuntimeResult<MergeTableCursor<'a>> {
    let mut children: Vec<Box<dyn TableCursor + 'a>> = Vec::new();
    for table_ref in candidate
        .input_refs()
        .iter()
        .chain(candidate.overlap_refs())
    {
        let level_index = usize::from(table_ref.level().raw());
        let Some(level) = branch.owned_levels().get(level_index) else {
            return Err(BranchRuntimeError::InvalidCompaction {
                reason: BranchCompactionInvalidity::CandidateMissingLevel,
            });
        };
        let Some(table) = level.get(table_ref.table_index()) else {
            return Err(BranchRuntimeError::InvalidCompaction {
                reason: BranchCompactionInvalidity::CandidateMissingTable,
            });
        };
        children.push(Box::new(table.reader().cursor()));
    }
    Ok(MergeTableCursor::new(children))
}

fn row_below_floors(row: &TableRow, proof: BranchCompactionPruningProof) -> bool {
    if row.commit_version() >= proof.retained_version_floor {
        return false;
    }
    if proof
        .retained_timestamp_floor
        .is_some_and(|floor| row.commit_timestamp() >= floor)
    {
        return false;
    }
    true
}

fn row_would_drop_as_expired(row: &TableRow, proof: BranchCompactionPruningProof) -> bool {
    row_below_floors(row, proof) && row_is_expired_by_proof(row, proof.ttl_expired_at_or_before)
}

fn row_is_expired_by_proof(row: &TableRow, expired_at_or_before: Option<Timestamp>) -> bool {
    expired_at_or_before.is_some_and(|timestamp| {
        !row.is_tombstone() && row.expires_at() != Timestamp::EPOCH && row.expires_at() <= timestamp
    })
}

fn hash_owned_levels(hash: &mut u64, levels: &[Vec<BranchOwnedTable>]) {
    hash_u64(hash, levels.len() as u64);
    for (level_index, level) in levels.iter().enumerate() {
        hash_u64(hash, level_index as u64);
        hash_u64(hash, level.len() as u64);
        for table in level {
            hash_table_identity(hash, table.descriptor().identity());
            // Bind to the materialization source identity rather than the
            // (now-stale) layer index of any inherited layer this table
            // replaced. Tables created via materialization carry the source
            // branch and fork version; pre-materialization proofs hash a
            // `None` marker and post-materialization proofs hash the source
            // identity, so a proof built before materialization will not
            // validate against the post-materialization branch state.
            match table.materialization_source() {
                Some(source) => {
                    hash_u64(hash, 1);
                    hash_bytes(hash, source.source_branch_id().as_bytes());
                    hash_u64(hash, source.fork_version().as_u64());
                }
                None => hash_u64(hash, 0),
            }
            hash_table_facts(hash, table);
        }
    }
}

/// BS4.4e: hash a sealed owned/inherited table by its facts+extras instead of streaming every row.
/// Sealed tables are content-immutable and install rejects identity collisions, so within a process
/// identity ⟹ content; the identity is already folded in by the caller and the facts/extras below are
/// defense-in-depth. This keeps the pruning-proof fingerprint O(1) per table once tables are disk-backed
/// (BS4.4f) — a `table.rows()` scan would otherwise trip the materialization guard.
fn hash_table_facts(hash: &mut u64, table: &BranchOwnedTable) {
    let facts = table.facts();
    hash_u64(hash, facts.row_count());
    hash_u64(hash, facts.byte_count());
    hash_u64(hash, facts.commit_range().min().as_u64());
    hash_u64(hash, facts.commit_range().max().as_u64());
    hash_bytes(hash, facts.key_range().first_key());
    hash_bytes(hash, facts.key_range().last_key());
    let extras = table.extras();
    hash_optional_timestamp(hash, extras.timestamp_min());
    hash_optional_timestamp(hash, extras.timestamp_max());
    hash_u64(hash, extras.put_rows());
    hash_u64(hash, extras.tombstone_rows());
}

fn hash_optional_timestamp(hash: &mut u64, timestamp: Option<Timestamp>) {
    match timestamp {
        Some(timestamp) => {
            hash_u64(hash, 1);
            hash_u64(hash, timestamp.as_micros());
        }
        None => hash_u64(hash, 0),
    }
}

fn hash_table_identity(hash: &mut u64, identity: &TableIdentity) {
    hash_bytes(hash, identity.as_str().as_bytes());
}

fn hash_row(hash: &mut u64, row: &TableRow) {
    hash_bytes(hash, row.physical_key().branch_id().as_bytes());
    hash_bytes(hash, row.physical_key().space().as_bytes());
    hash_bytes(hash, &[row.physical_key().storage_space_id().raw()]);
    hash_bytes(hash, row.physical_key().user_key());
    hash_u64(hash, row.commit_version().as_u64());
    hash_u64(hash, row.commit_timestamp().as_micros());
    hash_u64(hash, row.expires_at().as_micros());
    hash_bytes(hash, &[u8::from(row.is_tombstone())]);
    hash_bytes(hash, row.value());
}

fn hash_u64(hash: &mut u64, value: u64) {
    hash_bytes(hash, &value.to_le_bytes());
}

fn hash_bytes(hash: &mut u64, bytes: &[u8]) {
    hash_u64_raw(hash, bytes.len() as u64);
    for byte in bytes {
        *hash ^= u64::from(*byte);
        *hash = hash.wrapping_mul(FINGERPRINT_PRIME);
    }
}

fn hash_u64_raw(hash: &mut u64, value: u64) {
    for byte in value.to_le_bytes() {
        *hash ^= u64::from(byte);
        *hash = hash.wrapping_mul(FINGERPRINT_PRIME);
    }
}

#[cfg(test)]
mod tests {
    use super::{validate_timestamp_floor, BranchTimestampCoverage};
    use crate::branch::error::BranchCompactionInvalidity;
    use crate::branch::error::BranchRuntimeError;
    use strata_core::Timestamp;

    // #3520: coverage-vs-floor truth table (the single rule the pruning builder
    // skips on and the apply-time validation rejects on).
    #[test]
    fn covers_timestamp_floor_truth_table() {
        let floor = Timestamp::from_micros(10);
        // Complete attests any floor.
        assert!(BranchTimestampCoverage::complete().covers_timestamp_floor(floor));
        // CompleteSince attests a floor at or after its earliest proven point.
        assert!(
            BranchTimestampCoverage::complete_since(Timestamp::from_micros(10))
                .covers_timestamp_floor(floor)
        );
        assert!(
            BranchTimestampCoverage::complete_since(Timestamp::from_micros(5))
                .covers_timestamp_floor(floor)
        );
        // CompleteSince does NOT attest a floor before its earliest point.
        assert!(
            !BranchTimestampCoverage::complete_since(Timestamp::from_micros(11))
                .covers_timestamp_floor(floor)
        );
        // Unknown attests nothing.
        assert!(!BranchTimestampCoverage::unknown().covers_timestamp_floor(floor));
    }

    #[test]
    fn validate_timestamp_floor_matches_coverage() {
        let floor = Timestamp::from_micros(10);
        // No floor: always OK, regardless of coverage.
        assert!(validate_timestamp_floor(BranchTimestampCoverage::unknown(), None).is_ok());
        // Attested floors pass.
        assert!(validate_timestamp_floor(BranchTimestampCoverage::complete(), Some(floor)).is_ok());
        assert!(validate_timestamp_floor(
            BranchTimestampCoverage::complete_since(Timestamp::from_micros(5)),
            Some(floor)
        )
        .is_ok());
        // Unattested floors raise TimestampFloorWithoutCoverage.
        for coverage in [
            BranchTimestampCoverage::unknown(),
            BranchTimestampCoverage::complete_since(Timestamp::from_micros(11)),
        ] {
            let error = validate_timestamp_floor(coverage, Some(floor))
                .expect_err("unattested floor must be rejected");
            assert!(matches!(
                error,
                BranchRuntimeError::InvalidCompaction {
                    reason: BranchCompactionInvalidity::TimestampFloorWithoutCoverage
                }
            ));
        }
    }
}
