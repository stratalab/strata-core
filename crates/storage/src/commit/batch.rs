//! Commit batch, mutation, validation fact, and stamping model.

use super::{
    CommitAdmissionPressureFacts, CommitRuntimeConfig, CommitRuntimeError, CommitRuntimeResult,
};
use crate::format::{storage_row_encoded_len, MAX_WAL_COMMIT_PAYLOAD_ROW_BYTES};
use crate::observability::perf_trace;
use crate::row::{PhysicalKey, StorageRow};
use std::collections::HashSet;
use std::fmt;
use std::num::NonZeroUsize;
use strata_core::{BranchId, CommitVersion, Timestamp};

const APPROX_MUTATION_FIXED_BYTES: usize = BranchId::BYTE_LEN + 32;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CommitBatch {
    branch_id: BranchId,
    kind: CommitBatchKind,
    mutations: Vec<CommitMutation>,
    validation: CommitValidationFacts,
    options: CommitBatchOptions,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CommitBatchKind {
    Mutating,
    ReadOnlyDiagnostic,
}

#[derive(Clone, Eq, PartialEq)]
pub(crate) enum CommitMutation {
    Put {
        key: PhysicalKey,
        value: Vec<u8>,
        expires_at: CommitExpiry,
        retention: CommitRetentionHint,
    },
    Delete {
        key: PhysicalKey,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CommitExpiry {
    None,
    At(Timestamp),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CommitRetentionHint {
    Append,
    KeepLastNonZero(NonZeroUsize),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CommitBatchOptions {
    durability: CommitDurabilityMode,
    conflict_validation: CommitConflictValidationMode,
    duplicate_policy: CommitDuplicateKeyPolicy,
    timestamp_policy: CommitTimestampPolicy,
    origin: CommitOrigin,
    committed_at: Option<Timestamp>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CommitDurabilityMode {
    Cache,
    Standard,
    Always,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CommitConflictValidationMode {
    Validate,
    Skip,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CommitDuplicateKeyPolicy {
    Reject,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CommitTimestampPolicy {
    RuntimeGenerated,
    /// A runtime-generated timestamp candidate read BEFORE the runtime lock (the API reads it
    /// early so TTL/expiry facts and the commit stamp derive from one frontier). Clamps up to
    /// the monotonic floor like `RuntimeGenerated` — under concurrent writers another commit
    /// may allocate a later floor between the read and the lock, which is ordinary interleaving,
    /// not a caller error. Only truly caller-supplied stamps use the strict `Explicit` path.
    RuntimeGeneratedBase(Timestamp),
    Explicit(Timestamp),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CommitOrigin {
    StorageRuntime,
    Diagnostic,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct CommitValidationFacts {
    read_set: Vec<CommitReadFact>,
    cas_set: Vec<CommitCasFact>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CommitReadFact {
    key: PhysicalKey,
    observed: CommitObservedVersion,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CommitCasFact {
    key: PhysicalKey,
    expected: CommitObservedVersion,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CommitObservedVersion {
    Missing,
    Present(CommitVersion),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CommitStamp {
    branch_id: BranchId,
    commit_version: CommitVersion,
    commit_timestamp: Timestamp,
    /// Wall-clock instant the commit was applied (UTC epoch micros), supplied
    /// from above (the engine owns the time source). `None` when unset — a
    /// reconstructed stamp or a caller that did not provide one. Distinct from
    /// `commit_timestamp`, which is the logical commit-timeline clock (#3112).
    committed_at: Option<Timestamp>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ValidatedCommitBatch {
    batch: CommitBatch,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct PhysicalKeyIdentity<'a> {
    branch_id: BranchId,
    space: &'a str,
    storage_space_id: crate::row::StorageSpaceId,
    user_key: &'a [u8],
}

impl<'a> From<&'a PhysicalKey> for PhysicalKeyIdentity<'a> {
    fn from(key: &'a PhysicalKey) -> Self {
        Self {
            branch_id: key.branch_id(),
            space: key.space(),
            storage_space_id: key.storage_space_id(),
            user_key: key.user_key(),
        }
    }
}

impl CommitBatch {
    pub(crate) fn mutating(
        branch_id: BranchId,
        mutations: Vec<CommitMutation>,
        validation: CommitValidationFacts,
        options: CommitBatchOptions,
    ) -> Self {
        Self {
            branch_id,
            kind: CommitBatchKind::Mutating,
            mutations,
            validation,
            options,
        }
    }

    pub(crate) fn read_only_diagnostic(
        branch_id: BranchId,
        validation: CommitValidationFacts,
        options: CommitBatchOptions,
    ) -> Self {
        Self {
            branch_id,
            kind: CommitBatchKind::ReadOnlyDiagnostic,
            mutations: Vec::new(),
            validation,
            options,
        }
    }

    pub(crate) const fn branch_id(&self) -> BranchId {
        self.branch_id
    }

    pub(crate) const fn kind(&self) -> CommitBatchKind {
        self.kind
    }

    pub(crate) fn mutations(&self) -> &[CommitMutation] {
        &self.mutations
    }

    pub(crate) const fn validation(&self) -> &CommitValidationFacts {
        &self.validation
    }

    pub(crate) const fn options(&self) -> CommitBatchOptions {
        self.options
    }

    pub(crate) fn validate(
        self,
        config: &CommitRuntimeConfig,
    ) -> CommitRuntimeResult<ValidatedCommitBatch> {
        let timer = perf_trace::start_timer();
        let result = (|| {
            config.validate()?;
            validate_batch_shape(&self, config)?;
            Ok(ValidatedCommitBatch { batch: self })
        })();
        perf_trace::record_runtime_batch_validate_elapsed(timer);
        result
    }
}

impl CommitMutation {
    pub(crate) fn put(
        key: PhysicalKey,
        value: impl Into<Vec<u8>>,
        expires_at: CommitExpiry,
        retention: CommitRetentionHint,
    ) -> Self {
        Self::Put {
            key,
            value: value.into(),
            expires_at,
            retention,
        }
    }

    pub(crate) const fn delete(key: PhysicalKey) -> Self {
        Self::Delete { key }
    }

    pub(crate) const fn physical_key(&self) -> &PhysicalKey {
        match self {
            Self::Put { key, .. } | Self::Delete { key } => key,
        }
    }

    pub(crate) fn value(&self) -> Option<&[u8]> {
        match self {
            Self::Put { value, .. } => Some(value),
            Self::Delete { .. } => None,
        }
    }

    pub(crate) const fn expires_at(&self) -> Option<CommitExpiry> {
        match self {
            Self::Put { expires_at, .. } => Some(*expires_at),
            Self::Delete { .. } => None,
        }
    }

    pub(crate) const fn retention(&self) -> Option<CommitRetentionHint> {
        match self {
            Self::Put { retention, .. } => Some(*retention),
            Self::Delete { .. } => None,
        }
    }
}

impl fmt::Debug for CommitMutation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Put {
                key,
                value,
                expires_at,
                retention,
            } => formatter
                .debug_struct("Put")
                .field("key", key)
                .field("value_len", &value.len())
                .field("expires_at", expires_at)
                .field("retention", retention)
                .finish(),
            Self::Delete { key } => formatter.debug_struct("Delete").field("key", key).finish(),
        }
    }
}

impl CommitExpiry {
    fn to_storage_timestamp(self) -> CommitRuntimeResult<Timestamp> {
        match self {
            Self::None => Ok(Timestamp::EPOCH),
            Self::At(timestamp) if timestamp == Timestamp::EPOCH => {
                Err(CommitRuntimeError::InvalidMutation {
                    reason: "expiry at epoch is reserved as no expiry",
                })
            }
            Self::At(timestamp) => Ok(timestamp),
        }
    }
}

impl CommitBatchOptions {
    pub(crate) const fn new(
        durability: CommitDurabilityMode,
        conflict_validation: CommitConflictValidationMode,
        duplicate_policy: CommitDuplicateKeyPolicy,
        timestamp_policy: CommitTimestampPolicy,
        origin: CommitOrigin,
    ) -> Self {
        Self {
            durability,
            conflict_validation,
            duplicate_policy,
            timestamp_policy,
            origin,
            committed_at: None,
        }
    }

    /// Attaches the wall-clock instant the engine sampled for this commit. Kept
    /// as a builder so the `new` signature (and its call sites) stay put (#3112).
    pub(crate) const fn with_committed_at(mut self, committed_at: Option<Timestamp>) -> Self {
        self.committed_at = committed_at;
        self
    }

    pub(crate) const fn committed_at(self) -> Option<Timestamp> {
        self.committed_at
    }

    pub(crate) const fn durability(self) -> CommitDurabilityMode {
        self.durability
    }

    pub(crate) const fn conflict_validation(self) -> CommitConflictValidationMode {
        self.conflict_validation
    }

    pub(crate) const fn duplicate_policy(self) -> CommitDuplicateKeyPolicy {
        self.duplicate_policy
    }

    pub(crate) const fn timestamp_policy(self) -> CommitTimestampPolicy {
        self.timestamp_policy
    }

    pub(crate) const fn origin(self) -> CommitOrigin {
        self.origin
    }
}

impl Default for CommitBatchOptions {
    fn default() -> Self {
        Self {
            durability: CommitDurabilityMode::Cache,
            conflict_validation: CommitConflictValidationMode::Validate,
            duplicate_policy: CommitDuplicateKeyPolicy::Reject,
            timestamp_policy: CommitTimestampPolicy::RuntimeGenerated,
            origin: CommitOrigin::StorageRuntime,
            committed_at: None,
        }
    }
}

impl CommitValidationFacts {
    pub(crate) fn new(read_set: Vec<CommitReadFact>, cas_set: Vec<CommitCasFact>) -> Self {
        Self { read_set, cas_set }
    }

    pub(crate) const fn empty() -> Self {
        Self {
            read_set: Vec::new(),
            cas_set: Vec::new(),
        }
    }

    pub(crate) fn read_set(&self) -> &[CommitReadFact] {
        &self.read_set
    }

    pub(crate) fn cas_set(&self) -> &[CommitCasFact] {
        &self.cas_set
    }

    fn total_facts(&self) -> CommitRuntimeResult<usize> {
        self.read_set.len().checked_add(self.cas_set.len()).ok_or(
            CommitRuntimeError::InvalidValidationFacts {
                reason: "validation fact count overflow",
            },
        )
    }
}

impl CommitReadFact {
    pub(crate) const fn new(key: PhysicalKey, observed: CommitObservedVersion) -> Self {
        Self { key, observed }
    }

    pub(crate) const fn physical_key(&self) -> &PhysicalKey {
        &self.key
    }

    pub(crate) const fn observed(&self) -> CommitObservedVersion {
        self.observed
    }
}

impl CommitCasFact {
    pub(crate) const fn new(key: PhysicalKey, expected: CommitObservedVersion) -> Self {
        Self { key, expected }
    }

    pub(crate) const fn physical_key(&self) -> &PhysicalKey {
        &self.key
    }

    pub(crate) const fn expected(&self) -> CommitObservedVersion {
        self.expected
    }
}

impl CommitObservedVersion {
    fn validate(self) -> CommitRuntimeResult<()> {
        match self {
            Self::Present(version) if version == CommitVersion::ZERO => {
                Err(CommitRuntimeError::InvalidValidationFacts {
                    reason: "missing observed version must use Missing",
                })
            }
            Self::Missing | Self::Present(_) => Ok(()),
        }
    }
}

impl CommitStamp {
    pub(crate) fn new(
        branch_id: BranchId,
        commit_version: CommitVersion,
        commit_timestamp: Timestamp,
    ) -> CommitRuntimeResult<Self> {
        if commit_version == CommitVersion::ZERO {
            return Err(CommitRuntimeError::InvalidCommitState {
                reason: "commit version must be nonzero",
            });
        }
        Ok(Self {
            branch_id,
            commit_version,
            commit_timestamp,
            committed_at: None,
        })
    }

    /// Attaches the wall-clock instant the commit was applied. The engine
    /// supplies it (it owns the wasm-safe time source); storage only carries it.
    pub(crate) const fn with_committed_at(mut self, committed_at: Option<Timestamp>) -> Self {
        self.committed_at = committed_at;
        self
    }

    pub(crate) const fn branch_id(self) -> BranchId {
        self.branch_id
    }

    pub(crate) const fn commit_version(self) -> CommitVersion {
        self.commit_version
    }

    pub(crate) const fn commit_timestamp(self) -> Timestamp {
        self.commit_timestamp
    }

    pub(crate) const fn committed_at(self) -> Option<Timestamp> {
        self.committed_at
    }
}

impl ValidatedCommitBatch {
    pub(crate) const fn batch(&self) -> &CommitBatch {
        &self.batch
    }

    pub(crate) fn admission_pressure_facts(
        &self,
        config: &CommitRuntimeConfig,
    ) -> CommitRuntimeResult<CommitAdmissionPressureFacts> {
        let mut puts = 0usize;
        let mut deletes = 0usize;
        let mut approximate_commit_bytes = 0usize;
        for mutation in &self.batch.mutations {
            match mutation {
                CommitMutation::Put { .. } => puts = puts.saturating_add(1),
                CommitMutation::Delete { .. } => deletes = deletes.saturating_add(1),
            }
            approximate_commit_bytes =
                approximate_commit_bytes.saturating_add(approximate_mutation_bytes(mutation));
        }
        let mutations =
            puts.checked_add(deletes)
                .ok_or(CommitRuntimeError::InvalidCommitState {
                    reason: "commit admission mutation count overflow",
                })?;
        CommitAdmissionPressureFacts::new(
            mutations,
            puts,
            deletes,
            approximate_commit_bytes,
            config.admission_pressure_thresholds(),
        )
    }

    pub(crate) fn stamp_user_rows(
        &self,
        stamp: CommitStamp,
    ) -> CommitRuntimeResult<Vec<StorageRow>> {
        if self.batch.kind != CommitBatchKind::Mutating {
            return Err(CommitRuntimeError::InvalidBatch {
                reason: "read-only diagnostic batch cannot stamp rows",
            });
        }
        if stamp.branch_id() != self.batch.branch_id {
            return Err(CommitRuntimeError::BranchMismatch {
                expected: self.batch.branch_id,
                actual: stamp.branch_id(),
            });
        }

        let mut rows = Vec::with_capacity(self.batch.mutations.len());
        for mutation in &self.batch.mutations {
            rows.push(stamp_mutation(mutation, stamp)?);
        }

        Ok(rows)
    }

    pub(crate) fn into_stamped_user_rows(
        self,
        stamp: CommitStamp,
    ) -> CommitRuntimeResult<Vec<StorageRow>> {
        let batch = self.batch;
        if batch.kind != CommitBatchKind::Mutating {
            return Err(CommitRuntimeError::InvalidBatch {
                reason: "read-only diagnostic batch cannot stamp rows",
            });
        }
        if stamp.branch_id() != batch.branch_id {
            return Err(CommitRuntimeError::BranchMismatch {
                expected: batch.branch_id,
                actual: stamp.branch_id(),
            });
        }

        let mut rows = Vec::with_capacity(batch.mutations.len());
        for mutation in batch.mutations {
            rows.push(stamp_mutation_owned(mutation, stamp)?);
        }

        Ok(rows)
    }
}

fn approximate_mutation_bytes(mutation: &CommitMutation) -> usize {
    let key = mutation.physical_key();
    let key_bytes = key
        .space()
        .len()
        .saturating_add(1)
        .saturating_add(key.user_key().len());
    let value_bytes = mutation.value().map_or(0, <[u8]>::len);
    APPROX_MUTATION_FIXED_BYTES
        .saturating_add(key_bytes)
        .saturating_add(value_bytes)
}

fn validate_batch_shape(
    batch: &CommitBatch,
    config: &CommitRuntimeConfig,
) -> CommitRuntimeResult<()> {
    match batch.kind {
        CommitBatchKind::Mutating if batch.mutations.is_empty() => {
            return Err(CommitRuntimeError::InvalidBatch {
                reason: "mutating batch must contain at least one mutation",
            });
        }
        CommitBatchKind::ReadOnlyDiagnostic if !batch.mutations.is_empty() => {
            return Err(CommitRuntimeError::InvalidBatch {
                reason: "read-only diagnostic batch must not contain mutations",
            });
        }
        CommitBatchKind::Mutating | CommitBatchKind::ReadOnlyDiagnostic => {}
    }

    if batch.mutations.len() > config.max_mutations_per_batch() {
        return Err(CommitRuntimeError::InvalidBatch {
            reason: "mutation count exceeds configured limit",
        });
    }
    if batch.validation.total_facts()? > config.max_validation_facts_per_batch() {
        return Err(CommitRuntimeError::InvalidValidationFacts {
            reason: "validation fact count exceeds configured limit",
        });
    }
    if batch.mutations.len() > config.max_commit_rows_per_batch() {
        return Err(CommitRuntimeError::InvalidBatch {
            reason: "stamped row count exceeds configured limit",
        });
    }

    validate_mutation_branches(batch.branch_id, &batch.mutations)?;
    validate_fact_branches(batch.branch_id, &batch.validation)?;
    validate_mutation_spaces(&batch.mutations)?;
    validate_fact_spaces(&batch.validation)?;
    validate_duplicate_mutations(&batch.mutations)?;
    validate_duplicate_read_facts(batch.validation.read_set())?;
    validate_duplicate_cas_facts(batch.validation.cas_set())?;
    validate_observed_versions(&batch.validation)?;
    validate_mutation_expiry(&batch.mutations)?;
    validate_mutation_row_size(&batch.mutations)?;
    Ok(())
}

fn validate_mutation_branches(
    branch_id: BranchId,
    mutations: &[CommitMutation],
) -> CommitRuntimeResult<()> {
    for mutation in mutations {
        require_branch(branch_id, mutation.physical_key())?;
    }
    Ok(())
}

fn validate_fact_branches(
    branch_id: BranchId,
    validation: &CommitValidationFacts,
) -> CommitRuntimeResult<()> {
    for fact in validation.read_set() {
        require_branch(branch_id, fact.physical_key())?;
    }
    for fact in validation.cas_set() {
        require_branch(branch_id, fact.physical_key())?;
    }
    Ok(())
}

fn validate_mutation_spaces(mutations: &[CommitMutation]) -> CommitRuntimeResult<()> {
    for mutation in mutations {
        require_engine_space(mutation.physical_key())?;
    }
    Ok(())
}

fn validate_fact_spaces(validation: &CommitValidationFacts) -> CommitRuntimeResult<()> {
    for fact in validation.read_set() {
        require_engine_space(fact.physical_key())?;
    }
    for fact in validation.cas_set() {
        require_engine_space(fact.physical_key())?;
    }
    Ok(())
}

fn validate_duplicate_mutations(mutations: &[CommitMutation]) -> CommitRuntimeResult<()> {
    if mutations.len() < 2 {
        #[cfg(feature = "perf-trace")]
        perf_trace::record_runtime_duplicate_mutation_key_checks(mutations.len());
        return Ok(());
    }

    #[cfg(feature = "perf-trace")]
    let mut checks = 0usize;
    let mut seen = HashSet::with_capacity(mutations.len());
    for mutation in mutations {
        #[cfg(feature = "perf-trace")]
        {
            checks = checks.saturating_add(1);
        }
        let key = mutation.physical_key();
        if !seen.insert(PhysicalKeyIdentity::from(key)) {
            #[cfg(feature = "perf-trace")]
            perf_trace::record_runtime_duplicate_mutation_key_checks(checks);
            return Err(CommitRuntimeError::DuplicateMutationKey {
                space_id: key.storage_space_id(),
            });
        }
    }
    #[cfg(feature = "perf-trace")]
    perf_trace::record_runtime_duplicate_mutation_key_checks(checks);
    Ok(())
}

fn validate_duplicate_read_facts(facts: &[CommitReadFact]) -> CommitRuntimeResult<()> {
    for (index, fact) in facts.iter().enumerate() {
        if facts[..index]
            .iter()
            .any(|seen| seen.physical_key() == fact.physical_key())
        {
            return Err(CommitRuntimeError::InvalidValidationFacts {
                reason: "duplicate read fact",
            });
        }
    }
    Ok(())
}

fn validate_duplicate_cas_facts(facts: &[CommitCasFact]) -> CommitRuntimeResult<()> {
    for (index, fact) in facts.iter().enumerate() {
        if facts[..index]
            .iter()
            .any(|seen| seen.physical_key() == fact.physical_key())
        {
            return Err(CommitRuntimeError::InvalidValidationFacts {
                reason: "duplicate cas fact",
            });
        }
    }
    Ok(())
}

fn validate_observed_versions(validation: &CommitValidationFacts) -> CommitRuntimeResult<()> {
    for fact in validation.read_set() {
        fact.observed().validate()?;
    }
    for fact in validation.cas_set() {
        fact.expected().validate()?;
    }
    Ok(())
}

fn validate_mutation_expiry(mutations: &[CommitMutation]) -> CommitRuntimeResult<()> {
    for mutation in mutations {
        if let Some(expires_at) = mutation.expires_at() {
            expires_at.to_storage_timestamp()?;
        }
    }
    Ok(())
}

/// Refuse a mutation whose encoded storage row is larger than one WAL commit
/// payload row can hold.
///
/// The cap belongs to the durable format, but the refusal must not: cache mode
/// never reaches the WAL encoder (hard rule 14), so without this check cache
/// accepts rows durable mode cannot encode. Cache is the mode the quickstart,
/// the compiled rustdoc examples and the browser playground all run, so the
/// divergence surfaced at the moment of going to production — the worst
/// possible time for it (#3391).
///
/// Both durability modes reach this through `CommitBatch::validate`, and the
/// row length is exact rather than an upper bound, so this refuses no write
/// that durable mode would have accepted.
fn validate_mutation_row_size(mutations: &[CommitMutation]) -> CommitRuntimeResult<()> {
    for mutation in mutations {
        let row_len = storage_row_encoded_len(
            mutation.physical_key(),
            mutation.value().map_or(0, <[u8]>::len),
        );
        if row_len > MAX_WAL_COMMIT_PAYLOAD_ROW_BYTES {
            return Err(CommitRuntimeError::MutationTooLarge {
                row_len,
                max_row_len: MAX_WAL_COMMIT_PAYLOAD_ROW_BYTES,
            });
        }
    }
    Ok(())
}

fn require_branch(branch_id: BranchId, key: &PhysicalKey) -> CommitRuntimeResult<()> {
    if key.branch_id() == branch_id {
        Ok(())
    } else {
        Err(CommitRuntimeError::BranchMismatch {
            expected: branch_id,
            actual: key.branch_id(),
        })
    }
}

fn require_engine_space(key: &PhysicalKey) -> CommitRuntimeResult<()> {
    if key.storage_space_id().is_engine_owned() {
        Ok(())
    } else {
        Err(CommitRuntimeError::StorageOwnedMutationSpace {
            space_id: key.storage_space_id(),
        })
    }
}

fn stamp_mutation(
    mutation: &CommitMutation,
    stamp: CommitStamp,
) -> CommitRuntimeResult<StorageRow> {
    match mutation {
        CommitMutation::Put {
            key,
            value,
            expires_at,
            ..
        } => Ok(StorageRow::put(
            key.clone(),
            stamp.commit_version(),
            stamp.commit_timestamp(),
            expires_at.to_storage_timestamp()?,
            value.clone(),
        )),
        CommitMutation::Delete { key } => Ok(StorageRow::tombstone(
            key.clone(),
            stamp.commit_version(),
            stamp.commit_timestamp(),
        )),
    }
}

fn stamp_mutation_owned(
    mutation: CommitMutation,
    stamp: CommitStamp,
) -> CommitRuntimeResult<StorageRow> {
    match mutation {
        CommitMutation::Put {
            key,
            value,
            expires_at,
            ..
        } => Ok(StorageRow::put(
            key,
            stamp.commit_version(),
            stamp.commit_timestamp(),
            expires_at.to_storage_timestamp()?,
            value,
        )),
        CommitMutation::Delete { key } => Ok(StorageRow::tombstone(
            key,
            stamp.commit_version(),
            stamp.commit_timestamp(),
        )),
    }
}
