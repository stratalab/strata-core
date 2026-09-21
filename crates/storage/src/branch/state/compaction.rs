//! Branch-owned table compaction planning and installation.

use std::sync::Arc;

use super::{branch_reachable_table_identity_exists, insert_sorted_by_range, BranchLocalState};
use crate::branch::error::{BranchCompactionInvalidity, BranchRuntimeError, BranchRuntimeResult};
use crate::branch::facts::BranchTableReferenceKind;
use crate::branch::facts::{BranchLevel, BranchTableDescriptor, BranchTableRef};
use crate::branch::pruning::{BranchCompactionPruningPolicy, BranchCompactionPruningProof};
use crate::branch::read::{
    table_physical_first_key, table_physical_ranges_overlap, BranchInheritedLayer, BranchLayout,
    BranchMaterializationSource, BranchOwnedTable, BranchTimestampCoverage,
};
use crate::observability::perf_trace;
#[cfg(any(test, debug_assertions))]
use crate::table::TableInternalKeyBytes;
use crate::table::{
    BuiltTableArtifact, CompactionCutBoundary, CompactionOutputCutHints, ImmutableTableReader,
    TableBuilderConfig, TableCompactionConfig, TableCompactionDecision, TableCompactionInput,
    TableCompactionPolicy, TableCompactionReport, TableCompactionRowContext,
    TableCompactionSourceId, TableCompactor, TableCursor, TableIdentity, TableKeyBounds,
    TablePhysicalKeyBound, TablePhysicalKeyBytes, TableReaderConfig, TableRow, TableRuntimeResult,
};
use std::collections::BTreeSet;
use strata_core::BranchId;

const BRANCH_COMPACTION_SOURCE_METADATA_HASH_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const BRANCH_COMPACTION_SOURCE_METADATA_HASH_PRIME: u64 = 0x0000_0100_0000_01b3;

fn keep_all_policy() -> impl TableCompactionPolicy {
    |_: &TableCompactionRowContext<'_>, _: &TableRow| Ok(TableCompactionDecision::Keep)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BranchCompactionKind {
    CompactL0,
    CompactL0ToLevelOne,
    CompactLevel {
        level: BranchLevel,
        table_index: usize,
    },
    CompactBottommostLevel {
        level: BranchLevel,
        start_table_index: usize,
        table_count: usize,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BranchCompactionRetentionPolicy {
    KeepAll,
    DropOlderVersions,
    DropTombstones,
    DropExpired,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BranchCompactionRequest {
    branch_id: BranchId,
    kind: BranchCompactionKind,
    retention_policy: BranchCompactionRetentionPolicy,
    pruning_proof: Option<BranchCompactionPruningProof>,
    output_identity_seed: TableIdentity,
    table_compaction_config: TableCompactionConfig,
    table_builder_config: TableBuilderConfig,
    /// W1.1: byte bound for one L0→L1 pass (input tables + L1 overlap).
    /// `None` = unbounded (the pre-W1.1 behavior). The planner trims the L0
    /// input to the largest OLDEST-FIRST SUFFIX fitting the bound (always at
    /// least one table, so progress is guaranteed); non-L0 kinds ignore it.
    max_pass_input_bytes: Option<u64>,
    /// W1.3a: cut output tables so each overlaps at most this many bytes of
    /// the level BELOW the output level ("grandparent" tables). `None` = no
    /// cutting (the pre-W1.3a behavior). Bounds the input size of every
    /// FUTURE pass that compacts an output table down a level.
    output_grandparent_overlap_max_bytes: Option<u64>,
}

impl BranchCompactionRequest {
    pub(crate) fn new(
        branch_id: BranchId,
        kind: BranchCompactionKind,
        output_identity_seed: impl Into<String>,
    ) -> BranchRuntimeResult<Self> {
        let output_identity_seed = TableIdentity::new(output_identity_seed.into())
            .map_err(|source| BranchRuntimeError::TableRuntime { source })?;
        Ok(Self {
            branch_id,
            kind,
            retention_policy: BranchCompactionRetentionPolicy::KeepAll,
            pruning_proof: None,
            output_identity_seed,
            table_compaction_config: TableCompactionConfig::default(),
            table_builder_config: TableBuilderConfig::default(),
            max_pass_input_bytes: None,
            output_grandparent_overlap_max_bytes: None,
        })
    }

    /// W1.1: bound one L0→L1 pass's input bytes (see the field doc).
    pub(crate) const fn with_max_pass_input_bytes(mut self, bound: u64) -> Self {
        self.max_pass_input_bytes = Some(bound);
        self
    }

    pub(crate) const fn max_pass_input_bytes(&self) -> Option<u64> {
        self.max_pass_input_bytes
    }

    /// W1.3a: bound each output table's grandparent overlap (see the field doc).
    pub(crate) const fn with_output_grandparent_overlap_max_bytes(mut self, bound: u64) -> Self {
        self.output_grandparent_overlap_max_bytes = Some(bound);
        self
    }

    pub(crate) const fn output_grandparent_overlap_max_bytes(&self) -> Option<u64> {
        self.output_grandparent_overlap_max_bytes
    }

    pub(crate) const fn branch_id(&self) -> BranchId {
        self.branch_id
    }

    pub(crate) const fn kind(&self) -> BranchCompactionKind {
        self.kind
    }

    pub(crate) const fn retention_policy(&self) -> BranchCompactionRetentionPolicy {
        self.retention_policy
    }

    pub(crate) const fn output_identity_seed(&self) -> &TableIdentity {
        &self.output_identity_seed
    }

    pub(crate) const fn pruning_proof(&self) -> Option<BranchCompactionPruningProof> {
        self.pruning_proof
    }

    pub(crate) const fn table_compaction_config(&self) -> TableCompactionConfig {
        self.table_compaction_config
    }

    pub(crate) const fn table_builder_config(&self) -> TableBuilderConfig {
        self.table_builder_config
    }

    pub(crate) fn with_retention_policy(
        mut self,
        retention_policy: BranchCompactionRetentionPolicy,
    ) -> Self {
        self.retention_policy = retention_policy;
        self
    }

    pub(crate) fn with_pruning_proof(mut self, proof: BranchCompactionPruningProof) -> Self {
        self.pruning_proof = Some(proof);
        self
    }

    pub(crate) fn with_table_compaction_config(
        mut self,
        table_compaction_config: TableCompactionConfig,
    ) -> Self {
        self.table_compaction_config = table_compaction_config;
        self
    }

    pub(crate) fn with_table_builder_config(
        mut self,
        table_builder_config: TableBuilderConfig,
    ) -> Self {
        self.table_builder_config = table_builder_config;
        self
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BranchCompactionNoopReason {
    EmptyInputLevel,
    NotEnoughInputTables,
    LastLevel,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BranchCompactionOperation {
    TableRewrite,
    MetadataPromotion,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BranchCompactionNoPromotionReason {
    MultipleInputTables,
    TargetLevelOverlap,
    DeeperLevelOverlapBudgetExceeded,
    RowPruningRequested,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BranchCompactionCandidate {
    branch_id: BranchId,
    operation: BranchCompactionOperation,
    no_promotion_reason: Option<BranchCompactionNoPromotionReason>,
    input_refs: Vec<BranchTableRef>,
    overlap_refs: Vec<BranchTableRef>,
    output_level: BranchLevel,
    bottommost_for_branch: bool,
    source_count: usize,
    input_row_count: u64,
}

impl BranchCompactionCandidate {
    fn table_rewrite(
        branch_id: BranchId,
        input_refs: Vec<BranchTableRef>,
        overlap_refs: Vec<BranchTableRef>,
        output_level: BranchLevel,
        bottommost_for_branch: bool,
        input_row_count: u64,
        no_promotion_reason: Option<BranchCompactionNoPromotionReason>,
    ) -> Self {
        let source_count = input_refs.len().saturating_add(overlap_refs.len());
        Self {
            branch_id,
            operation: BranchCompactionOperation::TableRewrite,
            no_promotion_reason,
            input_refs,
            overlap_refs,
            output_level,
            bottommost_for_branch,
            source_count,
            input_row_count,
        }
    }

    fn metadata_promotion(
        branch_id: BranchId,
        input_refs: Vec<BranchTableRef>,
        output_level: BranchLevel,
        bottommost_for_branch: bool,
        input_row_count: u64,
    ) -> Self {
        Self {
            branch_id,
            operation: BranchCompactionOperation::MetadataPromotion,
            no_promotion_reason: None,
            source_count: input_refs.len(),
            input_refs,
            overlap_refs: Vec::new(),
            output_level,
            bottommost_for_branch,
            input_row_count,
        }
    }

    pub(crate) const fn branch_id(&self) -> BranchId {
        self.branch_id
    }

    pub(crate) const fn operation(&self) -> BranchCompactionOperation {
        self.operation
    }

    pub(crate) const fn no_promotion_reason(&self) -> Option<BranchCompactionNoPromotionReason> {
        self.no_promotion_reason
    }

    pub(crate) const fn requires_table_rewrite(&self) -> bool {
        matches!(self.operation, BranchCompactionOperation::TableRewrite)
    }

    pub(crate) const fn is_metadata_promotion(&self) -> bool {
        matches!(self.operation, BranchCompactionOperation::MetadataPromotion)
    }

    pub(crate) fn input_refs(&self) -> &[BranchTableRef] {
        &self.input_refs
    }

    pub(crate) fn overlap_refs(&self) -> &[BranchTableRef] {
        &self.overlap_refs
    }

    pub(crate) const fn output_level(&self) -> BranchLevel {
        self.output_level
    }

    pub(crate) const fn bottommost_for_branch(&self) -> bool {
        self.bottommost_for_branch
    }

    pub(crate) const fn source_count(&self) -> usize {
        self.source_count
    }

    pub(crate) const fn input_row_count(&self) -> u64 {
        self.input_row_count
    }

    fn removed_refs(&self) -> Vec<BranchTableRef> {
        let mut refs = self.input_refs.clone();
        refs.extend(self.overlap_refs.clone());
        refs
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BranchCompactionPlan {
    branch_id: BranchId,
    kind: BranchCompactionKind,
    candidate: Option<BranchCompactionCandidate>,
    noop_reason: Option<BranchCompactionNoopReason>,
}

impl BranchCompactionPlan {
    fn with_candidate(
        branch_id: BranchId,
        kind: BranchCompactionKind,
        candidate: BranchCompactionCandidate,
    ) -> Self {
        Self {
            branch_id,
            kind,
            candidate: Some(candidate),
            noop_reason: None,
        }
    }

    fn no_candidate(
        branch_id: BranchId,
        kind: BranchCompactionKind,
        noop_reason: BranchCompactionNoopReason,
    ) -> Self {
        Self {
            branch_id,
            kind,
            candidate: None,
            noop_reason: Some(noop_reason),
        }
    }

    pub(crate) const fn branch_id(&self) -> BranchId {
        self.branch_id
    }

    pub(crate) const fn kind(&self) -> BranchCompactionKind {
        self.kind
    }

    pub(crate) const fn candidate(&self) -> Option<&BranchCompactionCandidate> {
        self.candidate.as_ref()
    }

    pub(crate) const fn noop_reason(&self) -> Option<BranchCompactionNoopReason> {
        self.noop_reason
    }

    pub(crate) fn output_level(&self) -> Option<BranchLevel> {
        self.candidate
            .as_ref()
            .map(BranchCompactionCandidate::output_level)
    }

    pub(crate) fn materialization_source(&self) -> Option<BranchMaterializationSource> {
        self.candidate
            .as_ref()
            .and_then(BranchLocalState::compaction_output_materialization_source)
    }

    pub(crate) fn is_metadata_promotion(&self) -> bool {
        self.candidate
            .as_ref()
            .is_some_and(BranchCompactionCandidate::is_metadata_promotion)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BranchCompactionOutcome {
    branch_id: BranchId,
    noop_reason: Option<BranchCompactionNoopReason>,
    candidate: Option<BranchCompactionCandidate>,
    output_refs: Vec<BranchTableRef>,
    removed_refs: Vec<BranchTableRef>,
    table_report: Option<TableCompactionReport>,
    owned_table_count: usize,
}

impl BranchCompactionOutcome {
    fn no_candidate(
        branch_id: BranchId,
        reason: BranchCompactionNoopReason,
        owned_table_count: usize,
    ) -> Self {
        Self {
            branch_id,
            noop_reason: Some(reason),
            candidate: None,
            output_refs: Vec::new(),
            removed_refs: Vec::new(),
            table_report: None,
            owned_table_count,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn installed(
        branch_id: BranchId,
        candidate: BranchCompactionCandidate,
        output_refs: Vec<BranchTableRef>,
        removed_refs: Vec<BranchTableRef>,
        table_report: Option<TableCompactionReport>,
        owned_table_count: usize,
    ) -> Self {
        Self {
            branch_id,
            noop_reason: None,
            candidate: Some(candidate),
            output_refs,
            removed_refs,
            table_report,
            owned_table_count,
        }
    }

    pub(crate) const fn branch_id(&self) -> BranchId {
        self.branch_id
    }

    pub(crate) const fn noop_reason(&self) -> Option<BranchCompactionNoopReason> {
        self.noop_reason
    }

    pub(crate) const fn installed_replacement_tables(&self) -> bool {
        self.noop_reason.is_none()
    }

    pub(crate) const fn candidate(&self) -> Option<&BranchCompactionCandidate> {
        self.candidate.as_ref()
    }

    pub(crate) fn output_refs(&self) -> &[BranchTableRef] {
        &self.output_refs
    }

    pub(crate) fn removed_refs(&self) -> &[BranchTableRef] {
        &self.removed_refs
    }

    pub(crate) const fn table_report(&self) -> Option<&TableCompactionReport> {
        self.table_report.as_ref()
    }

    pub(crate) const fn owned_table_count(&self) -> usize {
        self.owned_table_count
    }
}

struct BranchTableCompactionSource<'a> {
    id: TableCompactionSourceId,
    table: &'a BranchOwnedTable,
    /// Optional physical-key range restricting the cursor to one subcompaction's slice; `None`
    /// scans the whole table (serial compaction).
    bounds: Option<TableKeyBounds>,
}

impl<'a> BranchTableCompactionSource<'a> {
    fn new(
        id: TableCompactionSourceId,
        table: &'a BranchOwnedTable,
        bounds: Option<TableKeyBounds>,
    ) -> Self {
        Self { id, table, bounds }
    }
}

impl TableCompactionInput for BranchTableCompactionSource<'_> {
    fn id(&self) -> &TableCompactionSourceId {
        &self.id
    }

    fn open_cursor(&self) -> TableRuntimeResult<Box<dyn TableCursor + '_>> {
        perf_trace::record_branch_compaction_source_opens(1);
        // BS4.4g: compaction reads each source table once, so its cursor must not fill the block cache.
        match &self.bounds {
            Some(bounds) => Ok(Box::new(
                self.table
                    .reader()
                    .bounded_cursor_without_cache_fill(bounds.clone()),
            )),
            None => Ok(Box::new(self.table.reader().cursor_without_cache_fill())),
        }
    }

    fn requires_source_order_validation(&self) -> bool {
        false
    }
}

impl BranchLocalState {
    pub(crate) fn plan_branch_compaction(
        &self,
        request: &BranchCompactionRequest,
    ) -> BranchRuntimeResult<BranchCompactionPlan> {
        self.validate_compaction_request(request)?;
        match request.kind() {
            BranchCompactionKind::CompactL0 => self.plan_l0_compaction(request.kind()),
            BranchCompactionKind::CompactL0ToLevelOne => self.plan_l0_to_l1_compaction(request),
            BranchCompactionKind::CompactLevel { level, table_index } => {
                self.plan_nonzero_level_compaction(request, level, table_index)
            }
            BranchCompactionKind::CompactBottommostLevel {
                level,
                start_table_index,
                table_count,
            } => self.plan_bottommost_level_compaction(
                request,
                level,
                start_table_index,
                table_count,
            ),
        }
    }

    pub(crate) fn compact_branch_owned_tables(
        &mut self,
        request: &BranchCompactionRequest,
    ) -> BranchRuntimeResult<BranchCompactionOutcome> {
        let plan = self.plan_branch_compaction(request)?;
        self.install_branch_compaction_plan(request, &plan)
    }

    pub(crate) fn prepare_branch_compaction_plan(
        &self,
        request: &BranchCompactionRequest,
        plan: &BranchCompactionPlan,
    ) -> BranchRuntimeResult<Option<(Vec<BuiltTableArtifact>, TableCompactionReport)>> {
        self.prepare_branch_compaction_plan_bounded(request, plan, None, 0)
    }

    /// Build one subcompaction's slice of a compaction: identical to
    /// [`prepare_branch_compaction_plan`] but restricting every input cursor to `bounds` (a
    /// half-open physical-key range) and salting the output-table identities with
    /// `subcompaction_index`, so N disjoint ranges can be built in parallel without colliding on
    /// output identities (each range restarts its output index at 0). `None` bounds + index 0 is
    /// the whole compaction (serial, unchanged).
    pub(crate) fn prepare_branch_compaction_plan_bounded(
        &self,
        request: &BranchCompactionRequest,
        plan: &BranchCompactionPlan,
        bounds: Option<&TableKeyBounds>,
        subcompaction_index: usize,
    ) -> BranchRuntimeResult<Option<(Vec<BuiltTableArtifact>, TableCompactionReport)>> {
        let mut artifacts = Vec::new();
        let report = self.prepare_branch_compaction_plan_bounded_into(
            request,
            plan,
            bounds,
            subcompaction_index,
            &mut |artifact| {
                artifacts.push(artifact);
                Ok(())
            },
        )?;
        Ok(report.map(|report| (artifacts, report)))
    }

    /// [`prepare_branch_compaction_plan_bounded`] with each completed output
    /// table handed to `sink` as it finishes (W1.2c): a publishing sink frees
    /// each output's encoded bytes immediately instead of accumulating the
    /// whole pass's outputs in heap (measured 20.6GB live at peak — ledger
    /// § T4). The sink's error surfaces as the compaction's error; callers
    /// that publish keep their partial-publish cleanup semantics.
    pub(crate) fn prepare_branch_compaction_plan_bounded_into(
        &self,
        request: &BranchCompactionRequest,
        plan: &BranchCompactionPlan,
        bounds: Option<&TableKeyBounds>,
        subcompaction_index: usize,
        sink: &mut dyn FnMut(BuiltTableArtifact) -> crate::table::TableRuntimeResult<()>,
    ) -> BranchRuntimeResult<Option<TableCompactionReport>> {
        self.validate_compaction_request(request)?;
        if plan.branch_id() != self.branch_id || plan.kind() != request.kind() {
            return Err(BranchRuntimeError::InvalidCompaction {
                reason: BranchCompactionInvalidity::Generic(
                    "compaction plan must match request branch and kind",
                ),
            });
        }
        let Some(candidate) = plan.candidate() else {
            return Ok(None);
        };
        if candidate.is_metadata_promotion() {
            validate_metadata_promotion_request(request)?;
            return Ok(None);
        }
        self.require_candidate_current(candidate)?;
        let sources = self.compaction_sources(candidate, bounds)?;
        let source_refs = sources
            .iter()
            .map(|source| source as &dyn TableCompactionInput)
            .collect::<Vec<_>>();
        let compactor = TableCompactor::new(
            request.table_compaction_config(),
            request.table_builder_config(),
        )
        .map_err(|source| BranchRuntimeError::TableRuntime { source })?;
        let compactor = match self.grandparent_cut_hints(candidate, request)? {
            Some(hints) => compactor.with_output_cut_hints(hints),
            None => compactor,
        };
        // Salt the output-table identity seed per subcompaction so parallel ranges (each of which
        // restarts its output index at 0) never produce colliding output-table identities.
        let output_identity_seed = if subcompaction_index == 0 {
            request.output_identity_seed().clone()
        } else {
            TableIdentity::new(format!(
                "{}-sc{subcompaction_index}",
                request.output_identity_seed().as_str(),
            ))
            .map_err(|source| BranchRuntimeError::TableRuntime { source })?
        };
        let report = match request.retention_policy() {
            BranchCompactionRetentionPolicy::KeepAll => {
                let mut policy = keep_all_policy();
                compactor
                    .compact_inputs_into(&output_identity_seed, &source_refs, &mut policy, sink)
                    .map_err(|source| BranchRuntimeError::TableRuntime { source })?
            }
            BranchCompactionRetentionPolicy::DropOlderVersions
            | BranchCompactionRetentionPolicy::DropTombstones
            | BranchCompactionRetentionPolicy::DropExpired => {
                let proof =
                    request
                        .pruning_proof()
                        .ok_or(BranchRuntimeError::InvalidCompaction {
                            reason: BranchCompactionInvalidity::ProofMissing,
                        })?;
                proof.validate_for_branch(self, candidate, request.retention_policy())?;
                let mut policy =
                    BranchCompactionPruningPolicy::new(request.retention_policy(), proof);
                compactor
                    .compact_inputs_into(&output_identity_seed, &source_refs, &mut policy, sink)
                    .map_err(|source| BranchRuntimeError::TableRuntime { source })?
            }
        };
        perf_trace::record_branch_compaction_peak_buffered_rows(report.peak_buffered_rows());
        Ok(Some(report))
    }

    pub(crate) fn install_branch_compaction_plan(
        &mut self,
        request: &BranchCompactionRequest,
        plan: &BranchCompactionPlan,
    ) -> BranchRuntimeResult<BranchCompactionOutcome> {
        self.validate_compaction_request(request)?;
        if plan.branch_id() != self.branch_id || plan.kind() != request.kind() {
            return Err(BranchRuntimeError::InvalidCompaction {
                reason: BranchCompactionInvalidity::Generic(
                    "compaction plan must match request branch and kind",
                ),
            });
        }
        let Some(candidate) = plan.candidate() else {
            return Ok(BranchCompactionOutcome::no_candidate(
                self.branch_id,
                plan.noop_reason()
                    .expect("no-candidate compaction plan records reason"),
                self.owned_table_count(),
            ));
        };
        if candidate.is_metadata_promotion() {
            return self.install_branch_compaction_metadata_promotion(request, plan, candidate);
        }
        let (artifacts, report) = self.prepare_branch_compaction_plan(request, plan)?.ok_or(
            BranchRuntimeError::InvalidCompaction {
                reason: BranchCompactionInvalidity::Generic(
                    "compaction candidate must produce prepared output",
                ),
            },
        )?;
        let output_tables = self.compaction_output_tables(
            candidate.output_level(),
            artifacts,
            Self::compaction_output_materialization_source(candidate),
        )?;
        self.install_branch_compaction_prepared_plan(request, plan, output_tables, report)
    }

    pub(crate) fn install_branch_compaction_prepared_plan(
        &mut self,
        request: &BranchCompactionRequest,
        plan: &BranchCompactionPlan,
        output_tables: Vec<BranchOwnedTable>,
        report: TableCompactionReport,
    ) -> BranchRuntimeResult<BranchCompactionOutcome> {
        self.validate_compaction_request(request)?;
        if plan.branch_id() != self.branch_id || plan.kind() != request.kind() {
            return Err(BranchRuntimeError::InvalidCompaction {
                reason: BranchCompactionInvalidity::Generic(
                    "compaction plan must match request branch and kind",
                ),
            });
        }
        let Some(candidate) = plan.candidate() else {
            return Ok(BranchCompactionOutcome::no_candidate(
                self.branch_id,
                plan.noop_reason()
                    .expect("no-candidate compaction plan records reason"),
                self.owned_table_count(),
            ));
        };
        self.require_candidate_current(candidate)?;
        if output_tables
            .iter()
            .any(|table| table.descriptor().level() != candidate.output_level())
        {
            return Err(BranchRuntimeError::InvalidCompaction {
                reason: BranchCompactionInvalidity::Generic(
                    "prepared compaction output level must match candidate",
                ),
            });
        }
        let output_identities = output_tables
            .iter()
            .map(|table| table.descriptor().identity().clone())
            .collect::<Vec<_>>();
        validate_compaction_output_identities(
            &output_identities,
            self.owned_levels(),
            &self.inherited_layers,
        )?;
        self.require_candidate_current(candidate)?;
        let compact_pointer = self.next_compact_pointer_after_success(request.kind(), candidate);

        let mut replacement_levels = self.owned_levels().to_vec();
        remove_compacted_tables(&mut replacement_levels, candidate)?;
        insert_compaction_outputs(&mut replacement_levels, candidate, output_tables)?;
        validate_compaction_levels(&replacement_levels)?;

        self.layout = Arc::new(BranchLayout::from_levels(replacement_levels));
        self.advance_compact_pointer(compact_pointer);
        self.refresh_observed_row_facts();
        if report.dropped_rows() != 0 {
            if let Some(proof) = request.pruning_proof() {
                // #3502 Slice B: the version pruning floor rises in lockstep
                // with the actual deletion, so a later api-layer `as_of` below
                // it raises (Slice A) rather than serving the single below-floor
                // survivor CMP-002 keeps.
                self.set_retained_history_floor(Some(proof.retained_version_floor()));
                if let Some(floor) = proof.retained_timestamp_floor() {
                    self.timestamp_coverage = BranchTimestampCoverage::complete_since(floor);
                }
            }
        }

        let output_refs =
            self.compaction_output_refs(candidate.output_level(), &output_identities)?;
        Ok(BranchCompactionOutcome::installed(
            self.branch_id,
            candidate.clone(),
            output_refs,
            candidate.removed_refs(),
            Some(report),
            self.owned_table_count(),
        ))
    }

    fn install_branch_compaction_metadata_promotion(
        &mut self,
        request: &BranchCompactionRequest,
        plan: &BranchCompactionPlan,
        candidate: &BranchCompactionCandidate,
    ) -> BranchRuntimeResult<BranchCompactionOutcome> {
        self.validate_compaction_request(request)?;
        validate_metadata_promotion_request(request)?;
        if plan.branch_id() != self.branch_id || plan.kind() != request.kind() {
            return Err(BranchRuntimeError::InvalidCompaction {
                reason: BranchCompactionInvalidity::Generic(
                    "compaction plan must match request branch and kind",
                ),
            });
        }
        validate_metadata_promotion_candidate(candidate)?;
        self.require_candidate_current(candidate)?;

        let promoted_table = self.promoted_compaction_table(candidate)?;
        let output_identity = promoted_table.descriptor().identity().clone();
        let compact_pointer = self.next_compact_pointer_after_success(request.kind(), candidate);
        let mut replacement_levels = self.owned_levels().to_vec();
        remove_compacted_tables(&mut replacement_levels, candidate)?;
        validate_promoted_table_identity(
            &output_identity,
            &replacement_levels,
            &self.inherited_layers,
        )?;
        insert_compaction_outputs(&mut replacement_levels, candidate, vec![promoted_table])?;
        validate_compaction_levels(&replacement_levels)?;

        self.layout = Arc::new(BranchLayout::from_levels(replacement_levels));
        self.advance_compact_pointer(compact_pointer);
        self.refresh_observed_row_facts();

        let output_refs =
            self.compaction_output_refs(candidate.output_level(), &[output_identity])?;
        Ok(BranchCompactionOutcome::installed(
            self.branch_id,
            candidate.clone(),
            output_refs,
            candidate.removed_refs(),
            None,
            self.owned_table_count(),
        ))
    }

    fn validate_compaction_request(
        &self,
        request: &BranchCompactionRequest,
    ) -> BranchRuntimeResult<()> {
        if request.branch_id() != self.branch_id {
            return Err(BranchRuntimeError::InvalidCompaction {
                reason: BranchCompactionInvalidity::Generic(
                    "compaction request branch id must match branch state",
                ),
            });
        }
        if request.retention_policy() == BranchCompactionRetentionPolicy::KeepAll
            && request.pruning_proof().is_some()
        {
            return Err(BranchRuntimeError::InvalidCompaction {
                reason: BranchCompactionInvalidity::Generic(
                    "branch compaction keep-all request must not carry a pruning proof",
                ),
            });
        }
        if request.retention_policy() != BranchCompactionRetentionPolicy::KeepAll
            && request.pruning_proof().is_none()
        {
            return Err(BranchRuntimeError::InvalidCompaction {
                reason: BranchCompactionInvalidity::ProofMissing,
            });
        }
        request
            .table_compaction_config()
            .validate()
            .map_err(|source| BranchRuntimeError::TableRuntime { source })?;
        request
            .table_builder_config()
            .validate()
            .map_err(|source| BranchRuntimeError::TableRuntime { source })?;
        Ok(())
    }

    fn plan_l0_compaction(
        &self,
        kind: BranchCompactionKind,
    ) -> BranchRuntimeResult<BranchCompactionPlan> {
        let level_index = 0;
        let input_count = self.owned_levels()[level_index].len();
        if input_count == 0 {
            return Ok(BranchCompactionPlan::no_candidate(
                self.branch_id,
                kind,
                BranchCompactionNoopReason::EmptyInputLevel,
            ));
        }
        if input_count < 2 {
            return Ok(BranchCompactionPlan::no_candidate(
                self.branch_id,
                kind,
                BranchCompactionNoopReason::NotEnoughInputTables,
            ));
        }
        let input_refs = self.table_refs_at_level(level_index, 0..input_count)?;
        let input_row_count = self.table_ref_row_count(&input_refs)?;
        let candidate = BranchCompactionCandidate::table_rewrite(
            self.branch_id,
            input_refs,
            Vec::new(),
            BranchLevel::ZERO,
            self.is_bottommost_output_level(0),
            input_row_count,
            None,
        );
        Ok(BranchCompactionPlan::with_candidate(
            self.branch_id,
            kind,
            candidate,
        ))
    }

    fn plan_l0_to_l1_compaction(
        &self,
        request: &BranchCompactionRequest,
    ) -> BranchRuntimeResult<BranchCompactionPlan> {
        let kind = request.kind();
        if self.owned_levels().len() < 2 {
            return Ok(BranchCompactionPlan::no_candidate(
                self.branch_id,
                kind,
                BranchCompactionNoopReason::LastLevel,
            ));
        }
        let input_count = self.owned_levels()[0].len();
        if input_count == 0 {
            return Ok(BranchCompactionPlan::no_candidate(
                self.branch_id,
                kind,
                BranchCompactionNoopReason::EmptyInputLevel,
            ));
        }
        // W1.1: bound the pass. L0 is NEWEST-FIRST (installs at index 0), so
        // the oldest-first consumption unit is the index SUFFIX — grow it
        // oldest-to-newer until input + L1-overlap bytes would exceed the
        // bound. SUFFIX-ONLY is load-bearing for recency ordering: any
        // non-suffix subset could move a newer row to L1 while an older row
        // for the same key stays in L0, inverting shadowing in later merges.
        // At least one table is always taken so passes make progress; the io
        // policy's defer path remains the backstop for single-oversized
        // inputs.
        let selected_count = match request.max_pass_input_bytes() {
            None => input_count,
            Some(bound) => self.bounded_l0_suffix_len(input_count, bound)?,
        };
        let input_refs =
            self.table_refs_at_level(0, (input_count - selected_count)..input_count)?;
        debug_assert!(
            input_refs
                .last()
                .is_none_or(|last| last.table_index() == input_count - 1),
            "bounded L0 selection must end at the oldest table (suffix-only invariant)"
        );
        let overlap_refs = self.overlapping_refs_for_output_range(&input_refs, 1)?;
        let input_row_count = self
            .table_ref_row_count(&input_refs)?
            .saturating_add(self.table_ref_row_count(&overlap_refs)?);
        let no_promotion_reason =
            metadata_promotion_blocker(request, input_refs.len(), overlap_refs.len(), None, None);
        if no_promotion_reason.is_none() {
            let candidate = BranchCompactionCandidate::metadata_promotion(
                self.branch_id,
                input_refs,
                BranchLevel::new(1),
                self.is_bottommost_output_level(1),
                input_row_count,
            );
            return Ok(BranchCompactionPlan::with_candidate(
                self.branch_id,
                kind,
                candidate,
            ));
        }
        let candidate = BranchCompactionCandidate::table_rewrite(
            self.branch_id,
            input_refs,
            overlap_refs,
            BranchLevel::new(1),
            self.is_bottommost_output_level(1),
            input_row_count,
            no_promotion_reason,
        );
        Ok(BranchCompactionPlan::with_candidate(
            self.branch_id,
            kind,
            candidate,
        ))
    }

    fn plan_nonzero_level_compaction(
        &self,
        request: &BranchCompactionRequest,
        level: BranchLevel,
        table_index: usize,
    ) -> BranchRuntimeResult<BranchCompactionPlan> {
        let kind = request.kind();
        let level_index = usize::from(level.raw());
        if level_index == 0 {
            return Err(BranchRuntimeError::InvalidCompaction {
                reason: BranchCompactionInvalidity::Generic(
                    "level-zero compaction requests must use CompactL0",
                ),
            });
        }
        if level_index >= self.owned_levels().len() {
            return Err(BranchRuntimeError::InvalidCompaction {
                reason: BranchCompactionInvalidity::Generic(
                    "compaction level is outside configured level count",
                ),
            });
        }
        let output_level_index =
            level_index
                .checked_add(1)
                .ok_or(BranchRuntimeError::InvalidCompaction {
                    reason: BranchCompactionInvalidity::Generic(
                        "compaction output level index overflowed",
                    ),
                })?;
        if output_level_index >= self.owned_levels().len()
            || u8::try_from(output_level_index).is_err()
        {
            return Ok(BranchCompactionPlan::no_candidate(
                self.branch_id,
                kind,
                BranchCompactionNoopReason::LastLevel,
            ));
        }
        if self.owned_levels()[level_index].is_empty() {
            return Ok(BranchCompactionPlan::no_candidate(
                self.branch_id,
                kind,
                BranchCompactionNoopReason::EmptyInputLevel,
            ));
        }
        if table_index >= self.owned_levels()[level_index].len() {
            return Err(BranchRuntimeError::InvalidCompaction {
                reason: BranchCompactionInvalidity::Generic(
                    "compaction table index is outside requested level",
                ),
            });
        }
        let input_refs =
            self.nonzero_input_refs_for_compaction(level_index, table_index, output_level_index)?;
        let overlap_refs =
            self.overlapping_refs_for_input_range(&input_refs, output_level_index)?;
        let input_row_count = self
            .table_ref_row_count(&input_refs)?
            .saturating_add(self.table_ref_row_count(&overlap_refs)?);
        let output_level = BranchLevel::new(u8::try_from(output_level_index).map_err(|_| {
            BranchRuntimeError::InvalidCompaction {
                reason: BranchCompactionInvalidity::Generic(
                    "compaction output level must fit in BranchLevel",
                ),
            }
        })?);
        let deeper_overlap_bytes =
            self.overlapping_table_bytes_for_input_range(&input_refs, level_index + 2);
        let input_byte_count = self.table_ref_byte_count(&input_refs)?;
        let no_promotion_reason = metadata_promotion_blocker(
            request,
            input_refs.len(),
            overlap_refs.len(),
            Some(deeper_overlap_bytes),
            Some(input_byte_count),
        );
        if no_promotion_reason.is_none() {
            let candidate = BranchCompactionCandidate::metadata_promotion(
                self.branch_id,
                input_refs,
                output_level,
                self.is_bottommost_output_level(output_level_index),
                input_row_count,
            );
            return Ok(BranchCompactionPlan::with_candidate(
                self.branch_id,
                kind,
                candidate,
            ));
        }
        let candidate = BranchCompactionCandidate::table_rewrite(
            self.branch_id,
            input_refs,
            overlap_refs,
            output_level,
            self.is_bottommost_output_level(output_level_index),
            input_row_count,
            no_promotion_reason,
        );
        Ok(BranchCompactionPlan::with_candidate(
            self.branch_id,
            kind,
            candidate,
        ))
    }

    fn nonzero_input_refs_for_compaction(
        &self,
        level_index: usize,
        table_index: usize,
        output_level_index: usize,
    ) -> BranchRuntimeResult<Vec<BranchTableRef>> {
        let mut start = table_index;
        let mut end = table_index
            .checked_add(1)
            .ok_or(BranchRuntimeError::InvalidCompaction {
                reason: BranchCompactionInvalidity::Generic("compaction input range overflowed"),
            })?;
        loop {
            let input_refs = self.table_refs_at_level(level_index, start..end)?;
            let overlap_refs =
                self.overlapping_refs_for_input_range(&input_refs, output_level_index)?;
            if overlap_refs.is_empty() {
                return Ok(input_refs);
            }
            let (overlap_first, overlap_last) = self.table_refs_physical_key_span(&overlap_refs)?;
            let Some((expanded_start, expanded_end)) = self.table_run_overlapping_physical_span(
                level_index,
                &overlap_first,
                &overlap_last,
            )?
            else {
                return Ok(input_refs);
            };
            let next_start = start.min(expanded_start);
            let next_end = end.max(expanded_end);
            if next_start == start && next_end == end {
                return Ok(input_refs);
            }
            start = next_start;
            end = next_end;
        }
    }

    fn plan_bottommost_level_compaction(
        &self,
        request: &BranchCompactionRequest,
        level: BranchLevel,
        start_table_index: usize,
        table_count: usize,
    ) -> BranchRuntimeResult<BranchCompactionPlan> {
        let kind = request.kind();
        let level_index = usize::from(level.raw());
        if level_index == 0 {
            return Err(BranchRuntimeError::InvalidCompaction {
                reason: BranchCompactionInvalidity::Generic(
                    "bottommost compaction requests must target a nonzero level",
                ),
            });
        }
        if level_index >= self.owned_levels().len() {
            return Err(BranchRuntimeError::InvalidCompaction {
                reason: BranchCompactionInvalidity::Generic(
                    "bottommost compaction level is outside configured level count",
                ),
            });
        }
        if level_index + 1 != self.owned_levels().len() {
            return Err(BranchRuntimeError::InvalidCompaction {
                reason: BranchCompactionInvalidity::Generic(
                    "bottommost compaction must target the final configured level",
                ),
            });
        }
        if self.owned_levels()[level_index].is_empty() {
            return Ok(BranchCompactionPlan::no_candidate(
                self.branch_id,
                kind,
                BranchCompactionNoopReason::EmptyInputLevel,
            ));
        }
        if table_count < 2 {
            return Ok(BranchCompactionPlan::no_candidate(
                self.branch_id,
                kind,
                BranchCompactionNoopReason::NotEnoughInputTables,
            ));
        }
        let end_table_index = start_table_index.checked_add(table_count).ok_or(
            BranchRuntimeError::InvalidCompaction {
                reason: BranchCompactionInvalidity::Generic(
                    "bottommost compaction input range overflowed",
                ),
            },
        )?;
        if end_table_index > self.owned_levels()[level_index].len() {
            return Err(BranchRuntimeError::InvalidCompaction {
                reason: BranchCompactionInvalidity::Generic(
                    "bottommost compaction input range is outside requested level",
                ),
            });
        }
        let input_refs =
            self.table_refs_at_level(level_index, start_table_index..end_table_index)?;
        let input_row_count = self.table_ref_row_count(&input_refs)?;
        let candidate = BranchCompactionCandidate::table_rewrite(
            self.branch_id,
            input_refs,
            Vec::new(),
            level,
            true,
            input_row_count,
            Some(BranchCompactionNoPromotionReason::MultipleInputTables),
        );
        Ok(BranchCompactionPlan::with_candidate(
            self.branch_id,
            kind,
            candidate,
        ))
    }

    /// W1.1: the largest oldest-first L0 SUFFIX whose input bytes plus L1
    /// overlap bytes fit `bound` (minimum one table). Grows the suffix one
    /// table at a time from the oldest (highest index) toward newer entries,
    /// recomputing the overlap for the widened key range each step — L0 is
    /// small by construction (the blocking threshold caps it at ~36), so the
    /// quadratic-in-L0 overlap recomputation is bounded and cheap next to the
    /// merge it sizes.
    fn bounded_l0_suffix_len(&self, input_count: usize, bound: u64) -> BranchRuntimeResult<usize> {
        let mut selected = 1usize;
        while selected < input_count {
            let next = selected + 1;
            if self.l0_suffix_pass_bytes(input_count, next)? > bound {
                break;
            }
            selected = next;
        }
        Ok(selected)
    }

    /// Input + L1-overlap byte total for the oldest-first L0 suffix of
    /// `selected` tables.
    fn l0_suffix_pass_bytes(
        &self,
        input_count: usize,
        selected: usize,
    ) -> BranchRuntimeResult<u64> {
        let refs = self.table_refs_at_level(0, (input_count - selected)..input_count)?;
        let overlap = self.overlapping_refs_for_output_range(&refs, 1)?;
        let mut bytes = 0u64;
        for table_ref in refs.iter().chain(overlap.iter()) {
            let table =
                self.table_for_ref(table_ref)
                    .ok_or(BranchRuntimeError::InvalidCompaction {
                        reason: BranchCompactionInvalidity::Generic(
                            "compaction table ref must exist",
                        ),
                    })?;
            bytes = bytes.saturating_add(table.facts().byte_count());
        }
        Ok(bytes)
    }

    /// W1.3a: output-cut hints for a table-rewrite pass — the start boundaries
    /// and byte weights of every table at the level BELOW the candidate's
    /// output level ("grandparents"). `None` when the request carries no
    /// overlap bound, the output level is bottommost, or the grandparent
    /// level is empty (nothing to bound against — behavior is then identical
    /// to pre-W1.3a).
    fn grandparent_cut_hints(
        &self,
        candidate: &BranchCompactionCandidate,
        request: &BranchCompactionRequest,
    ) -> BranchRuntimeResult<Option<CompactionOutputCutHints>> {
        let Some(max_overlap_bytes) = request.output_grandparent_overlap_max_bytes() else {
            return Ok(None);
        };
        let grandparent_level_index = usize::from(candidate.output_level().raw()).saturating_add(1);
        let Some(grandparent_tables) = self.owned_levels().get(grandparent_level_index) else {
            return Ok(None);
        };
        if grandparent_tables.is_empty() {
            return Ok(None);
        }
        let mut boundaries = Vec::with_capacity(grandparent_tables.len());
        for table in grandparent_tables {
            let Some(first_key) = table_physical_first_key(table) else {
                continue;
            };
            boundaries.push(CompactionCutBoundary::new(
                first_key.as_slice().to_vec(),
                table.facts().byte_count(),
            ));
        }
        if boundaries.is_empty() {
            return Ok(None);
        }
        CompactionOutputCutHints::new(boundaries, max_overlap_bytes)
            .map(Some)
            .map_err(|source| BranchRuntimeError::TableRuntime { source })
    }

    fn table_refs_at_level(
        &self,
        level_index: usize,
        range: std::ops::Range<usize>,
    ) -> BranchRuntimeResult<Vec<BranchTableRef>> {
        let level = BranchLevel::new(u8::try_from(level_index).map_err(|_| {
            BranchRuntimeError::InvalidCompaction {
                reason: BranchCompactionInvalidity::Generic(
                    "compaction table level must fit in BranchLevel",
                ),
            }
        })?);
        range
            .map(|table_index| {
                let table = self.owned_levels()[level_index].get(table_index).ok_or(
                    BranchRuntimeError::InvalidCompaction {
                        reason: BranchCompactionInvalidity::Generic(
                            "compaction table index must exist",
                        ),
                    },
                )?;
                branch_table_ref_for_owned(self.branch_id, level, table_index, table)
            })
            .collect()
    }

    fn overlapping_refs_for_input_range(
        &self,
        input_refs: &[BranchTableRef],
        target_level_index: usize,
    ) -> BranchRuntimeResult<Vec<BranchTableRef>> {
        let target_level = BranchLevel::new(u8::try_from(target_level_index).map_err(|_| {
            BranchRuntimeError::InvalidCompaction {
                reason: BranchCompactionInvalidity::Generic(
                    "compaction target level must fit in BranchLevel",
                ),
            }
        })?);
        let mut refs = Vec::new();
        for (table_index, table) in self.owned_levels()[target_level_index].iter().enumerate() {
            if input_refs.iter().any(|input_ref| {
                self.table_for_ref(input_ref)
                    .is_some_and(|input_table| table_physical_ranges_overlap(input_table, table))
            }) {
                refs.push(branch_table_ref_for_owned(
                    self.branch_id,
                    target_level,
                    table_index,
                    table,
                )?);
            }
        }
        Ok(refs)
    }

    fn overlapping_refs_for_output_range(
        &self,
        input_refs: &[BranchTableRef],
        target_level_index: usize,
    ) -> BranchRuntimeResult<Vec<BranchTableRef>> {
        let target_level = BranchLevel::new(u8::try_from(target_level_index).map_err(|_| {
            BranchRuntimeError::InvalidCompaction {
                reason: BranchCompactionInvalidity::Generic(
                    "compaction target level must fit in BranchLevel",
                ),
            }
        })?);
        let Some(target_tables) = self.owned_levels().get(target_level_index) else {
            return Err(BranchRuntimeError::InvalidCompaction {
                reason: BranchCompactionInvalidity::Generic("compaction target level must exist"),
            });
        };
        let (input_first, input_last) = self.table_refs_physical_key_span(input_refs)?;
        let mut refs = Vec::new();
        for (table_index, table) in target_tables.iter().enumerate() {
            let (table_first, table_last) = compaction_table_physical_key_bounds(table)?;
            if input_first <= table_last && table_first <= input_last {
                refs.push(branch_table_ref_for_owned(
                    self.branch_id,
                    target_level,
                    table_index,
                    table,
                )?);
            }
        }
        Ok(refs)
    }

    fn table_refs_physical_key_span(
        &self,
        refs: &[BranchTableRef],
    ) -> BranchRuntimeResult<(TablePhysicalKeyBytes, TablePhysicalKeyBytes)> {
        let mut span = None::<(TablePhysicalKeyBytes, TablePhysicalKeyBytes)>;
        for table_ref in refs {
            let table =
                self.table_for_ref(table_ref)
                    .ok_or(BranchRuntimeError::InvalidCompaction {
                        reason: BranchCompactionInvalidity::Generic(
                            "compaction table ref must exist",
                        ),
                    })?;
            let (first, last) = compaction_table_physical_key_bounds(table)?;
            span = Some(match span {
                None => (first, last),
                Some((current_first, current_last)) => {
                    (current_first.min(first), current_last.max(last))
                }
            });
        }
        span.ok_or(BranchRuntimeError::InvalidCompaction {
            reason: BranchCompactionInvalidity::Generic(
                "compaction input range must contain at least one table",
            ),
        })
    }

    fn table_run_overlapping_physical_span(
        &self,
        level_index: usize,
        span_first: &TablePhysicalKeyBytes,
        span_last: &TablePhysicalKeyBytes,
    ) -> BranchRuntimeResult<Option<(usize, usize)>> {
        let Some(level) = self.owned_levels().get(level_index) else {
            return Err(BranchRuntimeError::InvalidCompaction {
                reason: BranchCompactionInvalidity::Generic("compaction source level must exist"),
            });
        };
        let mut run = None::<(usize, usize)>;
        for (table_index, table) in level.iter().enumerate() {
            let (table_first, table_last) = compaction_table_physical_key_bounds(table)?;
            if table_first.as_slice() <= span_last.as_slice()
                && span_first.as_slice() <= table_last.as_slice()
            {
                run = Some(match run {
                    None => (table_index, table_index + 1),
                    Some((start, _)) => (start, table_index + 1),
                });
            }
        }
        Ok(run)
    }

    fn overlapping_table_bytes_for_input_range(
        &self,
        input_refs: &[BranchTableRef],
        target_level_index: usize,
    ) -> u64 {
        let Some(target_level) = self.owned_levels().get(target_level_index) else {
            return 0;
        };
        let mut byte_count = 0u64;
        for table in target_level {
            if input_refs.iter().any(|input_ref| {
                self.table_for_ref(input_ref)
                    .is_some_and(|input_table| table_physical_ranges_overlap(input_table, table))
            }) {
                byte_count = byte_count.saturating_add(table.facts().byte_count());
            }
        }
        byte_count
    }

    fn table_ref_row_count(&self, refs: &[BranchTableRef]) -> BranchRuntimeResult<u64> {
        let mut count = 0u64;
        for table_ref in refs {
            let table =
                self.table_for_ref(table_ref)
                    .ok_or(BranchRuntimeError::InvalidCompaction {
                        reason: BranchCompactionInvalidity::Generic(
                            "compaction table ref must exist",
                        ),
                    })?;
            // BS4.4a-i: the row count is on facts (u64) — no materialization needed.
            count = count.saturating_add(table.facts().row_count());
        }
        Ok(count)
    }

    fn table_ref_byte_count(&self, refs: &[BranchTableRef]) -> BranchRuntimeResult<u64> {
        let mut count = 0u64;
        for table_ref in refs {
            let table =
                self.table_for_ref(table_ref)
                    .ok_or(BranchRuntimeError::InvalidCompaction {
                        reason: BranchCompactionInvalidity::Generic(
                            "compaction table ref must exist",
                        ),
                    })?;
            count = count.saturating_add(table.facts().byte_count());
        }
        Ok(count)
    }

    fn table_for_ref(&self, table_ref: &BranchTableRef) -> Option<&BranchOwnedTable> {
        let level_index = usize::from(table_ref.level().raw());
        self.owned_levels()
            .get(level_index)?
            .get(table_ref.table_index())
            .filter(|table| {
                table.descriptor().identity() == table_ref.table_identity()
                    && table.branch_id() == self.branch_id
            })
    }

    fn table_for_candidate_ref(
        &self,
        candidate: &BranchCompactionCandidate,
        table_ref: &BranchTableRef,
    ) -> Option<&BranchOwnedTable> {
        if let Some(table) = self
            .table_for_ref(table_ref)
            .filter(|table| table_matches_ref_kind(table, table_ref))
        {
            return Some(table);
        }
        if !candidate_ref_allows_l0_index_rebase(candidate, table_ref) {
            return None;
        }
        self.owned_levels()
            .first()?
            .iter()
            .find(|table| table_matches_ref(table, candidate.branch_id(), table_ref))
    }

    fn is_bottommost_output_level(&self, output_level_index: usize) -> bool {
        self.owned_levels()
            .iter()
            .enumerate()
            .skip(output_level_index + 1)
            .all(|(_, tables)| tables.is_empty())
    }

    fn require_candidate_current(
        &self,
        candidate: &BranchCompactionCandidate,
    ) -> BranchRuntimeResult<()> {
        if candidate.branch_id() != self.branch_id {
            return Err(BranchRuntimeError::InvalidCompaction {
                reason: BranchCompactionInvalidity::Generic(
                    "compaction candidate branch must match branch state",
                ),
            });
        }
        for table_ref in candidate
            .input_refs()
            .iter()
            .chain(candidate.overlap_refs().iter())
        {
            let Some(table) = self.table_for_candidate_ref(candidate, table_ref) else {
                return Err(BranchRuntimeError::InvalidCompaction {
                    reason: BranchCompactionInvalidity::StaleCandidate,
                });
            };
            match table_ref.reference_kind() {
                BranchTableReferenceKind::Owned | BranchTableReferenceKind::Replacement { .. } => {}
                BranchTableReferenceKind::Inherited { .. }
                | BranchTableReferenceKind::MaterializingSource { .. } => {
                    return Err(BranchRuntimeError::InvalidCompaction {
                        reason: BranchCompactionInvalidity::Generic(
                            "compaction candidate must reference branch-owned tables",
                        ),
                    });
                }
            }
            if table.level() != table_ref.level() {
                return Err(BranchRuntimeError::InvalidCompaction {
                    reason: BranchCompactionInvalidity::Generic(
                        "compaction candidate table level is stale",
                    ),
                });
            }
        }
        Ok(())
    }

    fn compaction_sources(
        &self,
        candidate: &BranchCompactionCandidate,
        bounds: Option<&TableKeyBounds>,
    ) -> BranchRuntimeResult<Vec<BranchTableCompactionSource<'_>>> {
        candidate
            .input_refs()
            .iter()
            .chain(candidate.overlap_refs().iter())
            .enumerate()
            .map(|(source_index, table_ref)| {
                let table = self.table_for_candidate_ref(candidate, table_ref).ok_or(
                    BranchRuntimeError::InvalidCompaction {
                        reason: BranchCompactionInvalidity::Generic(
                            "compaction candidate source table must exist",
                        ),
                    },
                )?;
                let source_hash = table_source_metadata_hash(table_ref, table.facts());
                let source_id =
                    TableCompactionSourceId::new(format!("s{source_index}-h{source_hash:016x}",))
                        .map_err(|source| BranchRuntimeError::TableRuntime { source })?;
                Ok(BranchTableCompactionSource::new(
                    source_id,
                    table,
                    bounds.cloned(),
                ))
            })
            .collect()
    }

    /// Compute up to `n - 1` half-open physical-key boundaries that split the compaction's input
    /// into ~equal-byte ranges for parallel subcompaction builds (a size-weighted
    /// one-anchor-per-table sweep). Returns fewer boundaries — or none, meaning "run serially" —
    /// when the input is small (`target_range_size >= total`) or the keys are insufficiently
    /// distinct. Boundaries fall on table `last_key`s (physical keys), so every version of a key
    /// stays on one side.
    pub(crate) fn compaction_subcompaction_boundaries(
        &self,
        candidate: &BranchCompactionCandidate,
        n: usize,
        target_output_bytes: u64,
    ) -> BranchRuntimeResult<Vec<TablePhysicalKeyBytes>> {
        if n <= 1 {
            return Ok(Vec::new());
        }
        let mut anchors: Vec<(TablePhysicalKeyBytes, u64)> = Vec::new();
        let mut total_bytes: u64 = 0;
        for table_ref in candidate
            .input_refs()
            .iter()
            .chain(candidate.overlap_refs().iter())
        {
            let table = self.table_for_candidate_ref(candidate, table_ref).ok_or(
                BranchRuntimeError::InvalidCompaction {
                    reason: BranchCompactionInvalidity::Generic(
                        "compaction candidate source table must exist",
                    ),
                },
            )?;
            let (_first, last) = compaction_table_physical_key_bounds(table)?;
            let bytes = table.facts().byte_count();
            total_bytes = total_bytes.saturating_add(bytes);
            anchors.push((last, bytes));
        }
        if total_bytes == 0 {
            return Ok(Vec::new());
        }
        anchors.sort_by(|a, b| a.0.cmp(&b.0));
        let n_u64 = u64::try_from(n).unwrap_or(u64::MAX).max(1);
        let target_range_size = (total_bytes / n_u64).max(target_output_bytes);
        if target_range_size == 0 || target_range_size >= total_bytes {
            return Ok(Vec::new());
        }
        let mut boundaries: Vec<TablePhysicalKeyBytes> = Vec::new();
        let mut cumulative: u64 = 0;
        let mut next_threshold = target_range_size;
        for (last_key, bytes) in anchors {
            cumulative = cumulative.saturating_add(bytes);
            if cumulative > next_threshold && boundaries.last() != Some(&last_key) {
                boundaries.push(last_key);
                next_threshold = next_threshold.saturating_add(target_range_size);
                if boundaries.len() >= n - 1 {
                    break;
                }
            }
        }
        Ok(boundaries)
    }

    /// The `n` half-open physical-key ranges to build in parallel for this candidate (or a single
    /// unbounded range when the input is too small or the keys too few to split).
    pub(crate) fn subcompaction_ranges_for_candidate(
        &self,
        candidate: &BranchCompactionCandidate,
        n: usize,
        target_output_bytes: u64,
    ) -> BranchRuntimeResult<Vec<Option<TableKeyBounds>>> {
        let boundaries =
            self.compaction_subcompaction_boundaries(candidate, n, target_output_bytes)?;
        subcompaction_ranges(&boundaries)
    }

    pub(crate) fn compaction_output_tables(
        &self,
        output_level: BranchLevel,
        artifacts: Vec<BuiltTableArtifact>,
        materialization_source: Option<BranchMaterializationSource>,
    ) -> BranchRuntimeResult<Vec<BranchOwnedTable>> {
        let mut tables = Vec::with_capacity(artifacts.len());
        for artifact in artifacts {
            let extras = artifact.extras().clone();
            let (bytes, facts, rows) = artifact.into_parts_with_rows();
            let identity = facts.identity().clone();
            let reader = ImmutableTableReader::from_validated_rows(
                facts,
                &bytes,
                rows,
                TableReaderConfig::default().with_eager_filter_unavailable(),
            )
            .map_err(|source| BranchRuntimeError::TableRuntime { source })?;
            let descriptor =
                BranchTableDescriptor::new(identity.clone(), reader.facts().clone(), output_level)?;
            let table = if let Some(source) = materialization_source {
                BranchOwnedTable::new_materialization_replacement(
                    self.branch_id,
                    descriptor,
                    reader,
                    extras,
                    source,
                )?
            } else {
                BranchOwnedTable::new(self.branch_id, descriptor, reader, extras)?
            };
            tables.push(table);
        }
        Ok(tables)
    }

    fn promoted_compaction_table(
        &self,
        candidate: &BranchCompactionCandidate,
    ) -> BranchRuntimeResult<BranchOwnedTable> {
        validate_metadata_promotion_candidate(candidate)?;
        let input_ref =
            candidate
                .input_refs()
                .first()
                .ok_or(BranchRuntimeError::InvalidCompaction {
                    reason: BranchCompactionInvalidity::Generic(
                        "metadata promotion requires one input table",
                    ),
                })?;
        let table = self.table_for_candidate_ref(candidate, input_ref).ok_or(
            BranchRuntimeError::InvalidCompaction {
                reason: BranchCompactionInvalidity::Generic(
                    "metadata promotion input table must exist",
                ),
            },
        )?;
        let descriptor = BranchTableDescriptor::new(
            table.descriptor().identity().clone(),
            table.facts().clone(),
            candidate.output_level(),
        )?;
        if let Some(source) = table.materialization_source() {
            BranchOwnedTable::new_materialization_replacement(
                self.branch_id,
                descriptor,
                table.reader().clone(),
                table.extras().clone(),
                source,
            )
        } else {
            BranchOwnedTable::new(
                self.branch_id,
                descriptor,
                table.reader().clone(),
                table.extras().clone(),
            )
        }
    }

    fn compaction_output_materialization_source(
        candidate: &BranchCompactionCandidate,
    ) -> Option<BranchMaterializationSource> {
        let mut source = None::<BranchMaterializationSource>;
        for table_ref in candidate
            .input_refs()
            .iter()
            .chain(candidate.overlap_refs().iter())
        {
            let BranchTableReferenceKind::Replacement {
                source_branch_id,
                fork_version,
            } = table_ref.reference_kind()
            else {
                return None;
            };
            let table_source = BranchMaterializationSource::new(source_branch_id, fork_version);
            if source.is_some_and(|existing| existing != table_source) {
                return None;
            }
            source = Some(table_source);
        }
        source
    }

    fn compaction_output_refs(
        &self,
        output_level: BranchLevel,
        output_identities: &[TableIdentity],
    ) -> BranchRuntimeResult<Vec<BranchTableRef>> {
        let level_index = usize::from(output_level.raw());
        let level =
            self.owned_levels()
                .get(level_index)
                .ok_or(BranchRuntimeError::InvalidCompaction {
                    reason: BranchCompactionInvalidity::Generic(
                        "compaction output level must exist",
                    ),
                })?;
        output_identities
            .iter()
            .map(|identity| {
                let (table_index, table) = level
                    .iter()
                    .enumerate()
                    .find(|(_, table)| table.descriptor().identity() == identity)
                    .ok_or(BranchRuntimeError::InvalidCompaction {
                        reason: BranchCompactionInvalidity::Generic(
                            "compaction output table must be installed",
                        ),
                    })?;
                branch_table_ref_for_owned(self.branch_id, output_level, table_index, table)
            })
            .collect()
    }

    fn next_compact_pointer_after_success(
        &self,
        kind: BranchCompactionKind,
        candidate: &BranchCompactionCandidate,
    ) -> Option<(BranchLevel, TablePhysicalKeyBytes)> {
        let BranchCompactionKind::CompactLevel { level, .. } = kind else {
            return None;
        };
        let input_ref = candidate
            .input_refs()
            .iter()
            .rev()
            .find(|input_ref| input_ref.level() == level)?;
        if input_ref.level() != level {
            return None;
        }
        self.table_for_candidate_ref(candidate, input_ref)
            .and_then(table_physical_last_key)
            .map(|pointer| (level, pointer))
    }

    fn advance_compact_pointer(&mut self, pointer: Option<(BranchLevel, TablePhysicalKeyBytes)>) {
        let Some((level, pointer)) = pointer else {
            return;
        };
        if let Some(slot) = self.compact_pointers.get_mut(usize::from(level.raw())) {
            *slot = Some(pointer);
        }
    }
}

fn table_physical_last_key(table: &BranchOwnedTable) -> Option<TablePhysicalKeyBytes> {
    let last_key = table.facts().key_range().last_key();
    (!last_key.is_empty()).then(|| TablePhysicalKeyBytes::from_encoded_internal_key(last_key))
}

fn table_source_metadata_hash(
    table_ref: &BranchTableRef,
    facts: &crate::table::TableRuntimeFacts,
) -> u64 {
    let mut hash = BRANCH_COMPACTION_SOURCE_METADATA_HASH_OFFSET;
    hash_source_metadata_u64(&mut hash, u64::from(table_ref.level().raw()));
    hash_source_metadata_u64(&mut hash, table_ref.table_index() as u64);
    hash_source_metadata_bytes(&mut hash, table_ref.table_identity().as_str().as_bytes());
    hash_source_metadata_u64(&mut hash, facts.row_count());
    hash_source_metadata_u64(&mut hash, facts.commit_range().min().as_u64());
    hash_source_metadata_u64(&mut hash, facts.commit_range().max().as_u64());
    hash
}

fn hash_source_metadata_u64(hash: &mut u64, value: u64) {
    hash_source_metadata_bytes(hash, &value.to_le_bytes());
}

fn hash_source_metadata_bytes(hash: &mut u64, bytes: &[u8]) {
    hash_source_metadata_u64_raw(hash, bytes.len() as u64);
    for byte in bytes {
        *hash ^= u64::from(*byte);
        *hash = hash.wrapping_mul(BRANCH_COMPACTION_SOURCE_METADATA_HASH_PRIME);
    }
}

fn hash_source_metadata_u64_raw(hash: &mut u64, value: u64) {
    for byte in value.to_le_bytes() {
        *hash ^= u64::from(byte);
        *hash = hash.wrapping_mul(BRANCH_COMPACTION_SOURCE_METADATA_HASH_PRIME);
    }
}

fn branch_table_ref_for_owned(
    branch_id: BranchId,
    level: BranchLevel,
    table_index: usize,
    table: &BranchOwnedTable,
) -> BranchRuntimeResult<BranchTableRef> {
    if let Some(materialization_source) = table.materialization_source() {
        BranchTableRef::replacement(
            branch_id,
            materialization_source.source_branch_id(),
            materialization_source.fork_version(),
            level,
            table_index,
            table.descriptor().identity().clone(),
        )
    } else {
        BranchTableRef::owned(
            branch_id,
            level,
            table_index,
            table.descriptor().identity().clone(),
        )
    }
}

fn candidate_ref_allows_l0_index_rebase(
    candidate: &BranchCompactionCandidate,
    table_ref: &BranchTableRef,
) -> bool {
    table_ref.level() == BranchLevel::ZERO
        && candidate.output_level() == BranchLevel::new(1)
        && candidate
            .input_refs()
            .iter()
            .any(|input_ref| input_ref == table_ref)
}

fn table_matches_ref_kind(table: &BranchOwnedTable, table_ref: &BranchTableRef) -> bool {
    match table_ref.reference_kind() {
        BranchTableReferenceKind::Owned => table.materialization_source().is_none(),
        BranchTableReferenceKind::Replacement {
            source_branch_id,
            fork_version,
        } => {
            table.materialization_source()
                == Some(BranchMaterializationSource::new(
                    source_branch_id,
                    fork_version,
                ))
        }
        BranchTableReferenceKind::Inherited { .. }
        | BranchTableReferenceKind::MaterializingSource { .. } => false,
    }
}

fn table_matches_ref(
    table: &BranchOwnedTable,
    branch_id: BranchId,
    table_ref: &BranchTableRef,
) -> bool {
    table.descriptor().identity() == table_ref.table_identity()
        && table.branch_id() == branch_id
        && table.level() == table_ref.level()
        && table_matches_ref_kind(table, table_ref)
}

fn compacted_table_removal_index(
    owned_levels: &[Vec<BranchOwnedTable>],
    candidate: &BranchCompactionCandidate,
    table_ref: &BranchTableRef,
) -> BranchRuntimeResult<(usize, usize)> {
    let level_index = usize::from(table_ref.level().raw());
    let level = owned_levels
        .get(level_index)
        .ok_or(BranchRuntimeError::InvalidCompaction {
            reason: BranchCompactionInvalidity::Generic("compaction removal level must exist"),
        })?;
    if let Some(table) = level.get(table_ref.table_index()) {
        if table_matches_ref(table, candidate.branch_id(), table_ref) {
            return Ok((level_index, table_ref.table_index()));
        }
    }
    if candidate_ref_allows_l0_index_rebase(candidate, table_ref) {
        let table_index = level
            .iter()
            .position(|table| table_matches_ref(table, candidate.branch_id(), table_ref))
            .ok_or(BranchRuntimeError::InvalidCompaction {
                reason: BranchCompactionInvalidity::Generic(
                    "compaction removal table identity is stale",
                ),
            })?;
        return Ok((level_index, table_index));
    }
    if level.get(table_ref.table_index()).is_some() {
        Err(BranchRuntimeError::InvalidCompaction {
            reason: BranchCompactionInvalidity::Generic(
                "compaction removal table identity is stale",
            ),
        })
    } else {
        Err(BranchRuntimeError::InvalidCompaction {
            reason: BranchCompactionInvalidity::Generic(
                "compaction removal table index must exist",
            ),
        })
    }
}

fn remove_compacted_tables(
    owned_levels: &mut [Vec<BranchOwnedTable>],
    candidate: &BranchCompactionCandidate,
) -> BranchRuntimeResult<()> {
    let mut removals = Vec::new();
    for table_ref in candidate.removed_refs() {
        let (level_index, table_index) =
            compacted_table_removal_index(owned_levels, candidate, &table_ref)?;
        removals.push((level_index, table_index, table_ref));
    }
    removals.sort_by(|left, right| right.0.cmp(&left.0).then_with(|| right.1.cmp(&left.1)));

    for (level_index, table_index, table_ref) in removals {
        let level =
            owned_levels
                .get_mut(level_index)
                .ok_or(BranchRuntimeError::InvalidCompaction {
                    reason: BranchCompactionInvalidity::Generic(
                        "compaction removal level must exist",
                    ),
                })?;
        let table = level
            .get(table_index)
            .ok_or(BranchRuntimeError::InvalidCompaction {
                reason: BranchCompactionInvalidity::Generic(
                    "compaction removal table index must exist",
                ),
            })?;
        if !table_matches_ref(table, candidate.branch_id(), &table_ref) {
            return Err(BranchRuntimeError::InvalidCompaction {
                reason: BranchCompactionInvalidity::Generic(
                    "compaction removal table identity is stale",
                ),
            });
        }
        level.remove(table_index);
    }
    Ok(())
}

fn insert_compaction_outputs(
    owned_levels: &mut [Vec<BranchOwnedTable>],
    candidate: &BranchCompactionCandidate,
    mut output_tables: Vec<BranchOwnedTable>,
) -> BranchRuntimeResult<()> {
    let output_level_index = usize::from(candidate.output_level().raw());
    let output_level =
        owned_levels
            .get_mut(output_level_index)
            .ok_or(BranchRuntimeError::InvalidCompaction {
                reason: BranchCompactionInvalidity::Generic("compaction output level must exist"),
            })?;

    for table in &output_tables {
        if table.level() != candidate.output_level() {
            return Err(BranchRuntimeError::InvalidCompaction {
                reason: BranchCompactionInvalidity::Generic(
                    "compaction output table level must match candidate",
                ),
            });
        }
    }

    if candidate.output_level() == BranchLevel::ZERO {
        let insert_index = candidate
            .removed_refs()
            .iter()
            .filter(|table_ref| table_ref.level() == BranchLevel::ZERO)
            .map(BranchTableRef::table_index)
            .min()
            .unwrap_or(output_level.len())
            .min(output_level.len());
        for table in output_tables.drain(..).rev() {
            output_level.insert(insert_index, table);
        }
    } else {
        for table in output_tables {
            insert_sorted_by_range(output_level, table)?;
        }
    }
    Ok(())
}

fn validate_compaction_output_identities(
    output_identities: &[TableIdentity],
    owned_levels: &[Vec<BranchOwnedTable>],
    inherited_layers: &[BranchInheritedLayer],
) -> BranchRuntimeResult<()> {
    let mut output_seen = BTreeSet::<&str>::new();
    for identity in output_identities {
        if !output_seen.insert(identity.as_str()) {
            return Err(BranchRuntimeError::InvalidCompaction {
                reason: BranchCompactionInvalidity::Generic(
                    "compaction output identities must be unique",
                ),
            });
        }
        if branch_reachable_table_identity_exists(identity, owned_levels, inherited_layers) {
            return Err(BranchRuntimeError::InvalidCompaction {
                reason: BranchCompactionInvalidity::Generic(
                    "compaction output identity must not collide with existing reachable table",
                ),
            });
        }
    }
    Ok(())
}

fn validate_metadata_promotion_request(
    request: &BranchCompactionRequest,
) -> BranchRuntimeResult<()> {
    if request.retention_policy() != BranchCompactionRetentionPolicy::KeepAll {
        return Err(BranchRuntimeError::InvalidCompaction {
            reason: BranchCompactionInvalidity::Generic(
                "metadata promotion cannot apply row-retention pruning",
            ),
        });
    }
    if request.pruning_proof().is_some() {
        return Err(BranchRuntimeError::InvalidCompaction {
            reason: BranchCompactionInvalidity::Generic(
                "metadata promotion cannot carry a pruning proof",
            ),
        });
    }
    Ok(())
}

fn metadata_promotion_blocker(
    request: &BranchCompactionRequest,
    input_table_count: usize,
    overlap_table_count: usize,
    deeper_level_overlap_bytes: Option<u64>,
    input_byte_count: Option<u64>,
) -> Option<BranchCompactionNoPromotionReason> {
    if request.retention_policy() != BranchCompactionRetentionPolicy::KeepAll
        || request.pruning_proof().is_some()
    {
        return Some(BranchCompactionNoPromotionReason::RowPruningRequested);
    }
    if overlap_table_count != 0 {
        return Some(BranchCompactionNoPromotionReason::TargetLevelOverlap);
    }
    if input_table_count != 1 {
        return Some(BranchCompactionNoPromotionReason::MultipleInputTables);
    }
    let target_output_bytes = request.table_compaction_config().target_output_bytes();
    if input_byte_count.is_some_and(|bytes| bytes > target_output_bytes)
        && deeper_level_overlap_bytes
            .is_some_and(|bytes| bytes > target_output_bytes.saturating_mul(10))
    {
        return Some(BranchCompactionNoPromotionReason::DeeperLevelOverlapBudgetExceeded);
    }
    None
}

fn validate_metadata_promotion_candidate(
    candidate: &BranchCompactionCandidate,
) -> BranchRuntimeResult<()> {
    if !candidate.is_metadata_promotion() {
        return Err(BranchRuntimeError::InvalidCompaction {
            reason: BranchCompactionInvalidity::Generic(
                "metadata promotion candidate must use metadata promotion operation",
            ),
        });
    }
    if candidate.input_refs().len() != 1 || !candidate.overlap_refs().is_empty() {
        return Err(BranchRuntimeError::InvalidCompaction {
            reason: BranchCompactionInvalidity::Generic(
                "metadata promotion requires one input table and no overlap tables",
            ),
        });
    }
    let input_level = usize::from(candidate.input_refs()[0].level().raw());
    if candidate
        .input_refs()
        .iter()
        .any(|input_ref| usize::from(input_ref.level().raw()) != input_level)
    {
        return Err(BranchRuntimeError::InvalidCompaction {
            reason: BranchCompactionInvalidity::Generic(
                "metadata promotion input tables must come from one level",
            ),
        });
    }
    let output_level = usize::from(candidate.output_level().raw());
    if output_level != input_level + 1 {
        return Err(BranchRuntimeError::InvalidCompaction {
            reason: BranchCompactionInvalidity::Generic(
                "metadata promotion must move one level forward",
            ),
        });
    }
    Ok(())
}

fn validate_promoted_table_identity(
    output_identity: &TableIdentity,
    owned_levels_after_removal: &[Vec<BranchOwnedTable>],
    inherited_layers: &[BranchInheritedLayer],
) -> BranchRuntimeResult<()> {
    if branch_reachable_table_identity_exists(
        output_identity,
        owned_levels_after_removal,
        inherited_layers,
    ) {
        return Err(BranchRuntimeError::InvalidCompaction {
            reason: BranchCompactionInvalidity::Generic(
                "metadata promotion identity must not remain reachable after input removal",
            ),
        });
    }
    Ok(())
}

pub(super) fn validate_compaction_levels(
    owned_levels: &[Vec<BranchOwnedTable>],
) -> BranchRuntimeResult<()> {
    #[cfg(any(test, debug_assertions))]
    let mut seen_keys =
        std::collections::BTreeMap::<TableInternalKeyBytes, crate::row::StorageRow>::new();
    for (level_index, level) in owned_levels.iter().enumerate() {
        let branch_level = BranchLevel::new(u8::try_from(level_index).map_err(|_| {
            BranchRuntimeError::InvalidCompaction {
                reason: BranchCompactionInvalidity::Generic(
                    "compaction level index must fit in BranchLevel",
                ),
            }
        })?);
        let mut previous_bounds = None;
        for table in level {
            if table.level() != branch_level {
                return Err(BranchRuntimeError::InvalidCompaction {
                    reason: BranchCompactionInvalidity::Generic(
                        "compaction table level must match installed level",
                    ),
                });
            }
            if branch_level != BranchLevel::ZERO {
                let (first_key, last_key) = compaction_table_physical_key_bounds(table)?;
                if previous_bounds
                    .as_ref()
                    .is_some_and(|(previous_first, _)| previous_first > &first_key)
                {
                    return Err(BranchRuntimeError::InvalidCompaction { reason: BranchCompactionInvalidity::Generic("compaction output leaves nonzero-level tables out of physical-key order") });
                }
                if previous_bounds
                    .as_ref()
                    .is_some_and(|(_, previous_last)| previous_last >= &first_key)
                {
                    return Err(BranchRuntimeError::InvalidCompaction {
                        reason: BranchCompactionInvalidity::Generic(
                            "compaction output leaves overlapping nonzero-level physical key ranges",
                        ),
                    });
                }
                previous_bounds = Some((first_key, last_key));
            }
            #[cfg(any(test, debug_assertions))]
            {
                for row in &table.materialize_rows_for_oracle() {
                    match seen_keys.get(row.key()) {
                        // #2831 / ACID-005: idempotent recovery replay
                        // legitimately leaves the SAME row byte-identical in
                        // more than one sealed table; the read path shadows
                        // it. Only a DIVERGENT duplicate is corruption
                        // (the #2825 boundary).
                        Some(existing) if existing == row.row() => {}
                        Some(_) => {
                            return Err(BranchRuntimeError::InvalidCompaction {
                                reason: BranchCompactionInvalidity::Generic(
                                    "compaction levels must not contain divergent duplicate \
                                     internal keys",
                                ),
                            });
                        }
                        None => {
                            seen_keys.insert(row.key().clone(), row.row().clone());
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

fn compaction_table_physical_key_bounds(
    table: &BranchOwnedTable,
) -> BranchRuntimeResult<(TablePhysicalKeyBytes, TablePhysicalKeyBytes)> {
    let first_key = table.facts().key_range().first_key();
    let last_key = table.facts().key_range().last_key();
    if first_key.is_empty() || last_key.is_empty() {
        return Err(BranchRuntimeError::InvalidCompaction {
            reason: BranchCompactionInvalidity::Generic(
                "compaction output table must contain non-empty physical key bounds",
            ),
        });
    }
    Ok((
        TablePhysicalKeyBytes::from_encoded_internal_key(first_key),
        TablePhysicalKeyBytes::from_encoded_internal_key(last_key),
    ))
}

/// Convert `n-1` subcompaction boundaries into `n` half-open physical-key ranges (unbounded on
/// the open ends). An empty boundary list yields a single unbounded range (`vec![None]`), i.e. a
/// serial build.
pub(crate) fn subcompaction_ranges(
    boundaries: &[TablePhysicalKeyBytes],
) -> BranchRuntimeResult<Vec<Option<TableKeyBounds>>> {
    if boundaries.is_empty() {
        return Ok(vec![None]);
    }
    let empty_prefix = TablePhysicalKeyBytes::empty();
    let mut ranges = Vec::with_capacity(boundaries.len() + 1);
    for index in 0..=boundaries.len() {
        let lower = if index == 0 {
            TablePhysicalKeyBound::Unbounded
        } else {
            TablePhysicalKeyBound::Included(boundaries[index - 1].clone())
        };
        let upper = if index == boundaries.len() {
            TablePhysicalKeyBound::Unbounded
        } else {
            TablePhysicalKeyBound::Excluded(boundaries[index].clone())
        };
        let bounds = TableKeyBounds::physical_range(&empty_prefix, lower, upper)
            .map_err(|source| BranchRuntimeError::TableRuntime { source })?;
        ranges.push(Some(bounds));
    }
    Ok(ranges)
}
