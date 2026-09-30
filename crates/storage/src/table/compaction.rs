//! Generic table compaction.

use super::{
    validate_strictly_sorted_unique_rows, BuiltTableArtifact, ImmutableTableBuilder,
    ImmutableTableStreamingBuilder, TableBuilderConfig, TableCompactionConfig, TableCursor,
    TableIdentity, TableInternalKeyBytes, TableKeyBound, TableKeyBounds, TablePhysicalKeyBound,
    TableRow, TableRuntimeError, TableRuntimeResult, MERGE_HEAP_THRESHOLD,
};
use crate::observability::perf_trace;
use std::cmp::Ordering;

const MAX_SOURCE_ID_BYTES: usize = 128;
const OUTPUT_IDENTITY_METADATA_HASH_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const OUTPUT_IDENTITY_METADATA_HASH_PRIME: u64 = 0x0000_0100_0000_01b3;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TableCompactor {
    config: TableCompactionConfig,
    builder_config: TableBuilderConfig,
    output_cut_hints: Option<CompactionOutputCutHints>,
    /// #3469: fingerprint of the caller-owned inputs that decide output bytes
    /// but that this layer cannot see (the row policy's parameters, the
    /// subcompaction key bounds). Folded into every output identity.
    output_identity_salt: u64,
}

impl TableCompactor {
    pub(crate) fn new(
        config: TableCompactionConfig,
        builder_config: TableBuilderConfig,
    ) -> TableRuntimeResult<Self> {
        config.validate()?;
        builder_config.validate()?;
        Ok(Self {
            config,
            builder_config,
            output_cut_hints: None,
            output_identity_salt: 0,
        })
    }

    /// #3469: fold the caller's byte-affecting inputs (see
    /// [`OutputContentHasher`]) into every output identity.
    pub(crate) const fn with_output_identity_salt(mut self, salt: u64) -> Self {
        self.output_identity_salt = salt;
        self
    }

    /// W1.3a: cut output tables so each spans a bounded number of bytes of the
    /// level below the output level (see [`CompactionOutputCutHints`]).
    pub(crate) fn with_output_cut_hints(mut self, hints: CompactionOutputCutHints) -> Self {
        self.output_cut_hints = Some(hints);
        self
    }

    pub(crate) fn compact<P: TableCompactionPolicy + ?Sized>(
        &self,
        output_identity_seed: &TableIdentity,
        sources: &[TableCompactionSource],
        policy: &mut P,
    ) -> TableRuntimeResult<TableCompactionOutput> {
        let sources = sources
            .iter()
            .map(|source| source as &dyn TableCompactionInput)
            .collect::<Vec<_>>();
        self.compact_inputs(output_identity_seed, &sources, policy)
    }

    /// Run compaction with an explicit cross-source duplicate-key validation pass.
    ///
    /// Default compaction treats exact internal-key duplication across sources as
    /// corrupt input rather than a release-mode row-resolution feature.
    pub(crate) fn compact_validating_global_duplicates<P: TableCompactionPolicy + ?Sized>(
        &self,
        output_identity_seed: &TableIdentity,
        sources: &[TableCompactionSource],
        policy: &mut P,
    ) -> TableRuntimeResult<TableCompactionOutput> {
        let sources = sources
            .iter()
            .map(|source| source as &dyn TableCompactionInput)
            .collect::<Vec<_>>();
        self.compact_inputs_validating_global_duplicates(output_identity_seed, &sources, policy)
    }

    pub(crate) fn compact_inputs<P: TableCompactionPolicy + ?Sized>(
        &self,
        output_identity_seed: &TableIdentity,
        sources: &[&dyn TableCompactionInput],
        policy: &mut P,
    ) -> TableRuntimeResult<TableCompactionOutput> {
        compact_table_inputs(
            self,
            output_identity_seed,
            sources,
            policy,
            GlobalDuplicateValidation::Skip,
        )
    }

    /// [`compact_inputs`](Self::compact_inputs) with each completed output
    /// table handed to `sink` as it finishes instead of accumulated
    /// (W1.2c). Accumulation held every output's complete encoded bytes and
    /// rows in heap until the whole pass published — measured as 20.6GB live
    /// at peak under a 10M compaction-churn workload (billion-scale-ledger.md
    /// § T4). A sink that publishes-and-drops bounds the build's heap to the
    /// one in-progress table regardless of pass size.
    pub(crate) fn compact_inputs_into<P: TableCompactionPolicy + ?Sized>(
        &self,
        output_identity_seed: &TableIdentity,
        sources: &[&dyn TableCompactionInput],
        policy: &mut P,
        sink: &mut dyn FnMut(BuiltTableArtifact) -> TableRuntimeResult<()>,
    ) -> TableRuntimeResult<TableCompactionReport> {
        compact_table_inputs_into(
            self,
            output_identity_seed,
            sources,
            policy,
            GlobalDuplicateValidation::Skip,
            sink,
        )
    }

    /// Run cursor-input compaction with an explicit cross-source duplicate-key validation pass.
    pub(crate) fn compact_inputs_validating_global_duplicates<P: TableCompactionPolicy + ?Sized>(
        &self,
        output_identity_seed: &TableIdentity,
        sources: &[&dyn TableCompactionInput],
        policy: &mut P,
    ) -> TableRuntimeResult<TableCompactionOutput> {
        compact_table_inputs(
            self,
            output_identity_seed,
            sources,
            policy,
            GlobalDuplicateValidation::Run,
        )
    }

    pub(crate) const fn config(&self) -> TableCompactionConfig {
        self.config
    }

    pub(crate) const fn builder_config(&self) -> TableBuilderConfig {
        self.builder_config
    }
}

impl Default for TableCompactor {
    fn default() -> Self {
        Self::new(
            TableCompactionConfig::default(),
            TableBuilderConfig::default(),
        )
        .expect("default table compactor configuration is valid")
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GlobalDuplicateValidation {
    Skip,
    Run,
}

/// W1.3a: one "grandparent" boundary — the start of a table at the level BELOW
/// the compaction's output level, weighted by that table's byte count.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CompactionCutBoundary {
    start_physical_key: Vec<u8>,
    byte_count: u64,
}

impl CompactionCutBoundary {
    pub(crate) const fn new(start_physical_key: Vec<u8>, byte_count: u64) -> Self {
        Self {
            start_physical_key,
            byte_count,
        }
    }
}

/// W1.3a: hints for cutting compaction output tables by their overlap with the
/// level below the output level ("grandparent" tables in `RocksDB` terminology —
/// `max_compaction_bytes` / `CompactionOutputs::ShouldStopBefore` in
/// `db/compaction/compaction_outputs.cc`). Bounding an output table's grandparent
/// overlap bounds the input size of every FUTURE pass that compacts that output
/// down a level: without it, a full-keyspan L(n) table forces its L(n)→L(n+1)
/// pass to absorb the entire L(n+1) overlap in one unbounded pass (the 40-70s
/// stall monsters — ledger § stall root cause).
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CompactionOutputCutHints {
    /// Grandparent-table start boundaries, sorted ascending by physical key.
    boundaries: Vec<CompactionCutBoundary>,
    /// Cut the current output before a row that would push the output's
    /// spanned grandparent bytes past this bound.
    max_overlap_bytes: u64,
}

impl CompactionOutputCutHints {
    pub(crate) fn new(
        mut boundaries: Vec<CompactionCutBoundary>,
        max_overlap_bytes: u64,
    ) -> TableRuntimeResult<Self> {
        if max_overlap_bytes == 0 {
            return Err(TableRuntimeError::InvalidConfig {
                field: "max_overlap_bytes",
                reason: "must be nonzero",
            });
        }
        boundaries.sort_by(|a, b| a.start_physical_key.cmp(&b.start_physical_key));
        Ok(Self {
            boundaries,
            max_overlap_bytes,
        })
    }

    fn is_empty(&self) -> bool {
        self.boundaries.is_empty()
    }
}

/// Walks the sorted grandparent boundaries alongside the merged-row cursor and
/// decides where to cut the current output. Accounting is interval-based: the
/// grandparent starting at boundary `i` is treated as covering
/// `[start_i, start_{i+1})`, and an output's overlap is the byte sum of every
/// interval its key span touches. An output must span at least one CROSSED
/// boundary before it can be cut (`output_crossings >= 1`), so a single
/// grandparent larger than the bound cannot force degenerate near-empty
/// outputs — worst-case overlap is `max_overlap_bytes` plus one grandparent.
struct GrandparentCutTracker<'a> {
    hints: &'a CompactionOutputCutHints,
    /// First boundary index the merged-row cursor has not yet reached.
    next_boundary: usize,
    /// Grandparent bytes spanned by the current pending output (including the
    /// interval containing its first row).
    output_overlap_bytes: u64,
    /// Boundaries crossed while the current output had rows.
    output_crossings: u64,
}

impl<'a> GrandparentCutTracker<'a> {
    fn new(hints: &'a CompactionOutputCutHints) -> Option<Self> {
        if hints.is_empty() {
            return None;
        }
        Some(Self {
            hints,
            next_boundary: 0,
            output_overlap_bytes: 0,
            output_crossings: 0,
        })
    }

    /// Advance to `row_physical_key` and report whether the current output
    /// should be cut before appending this row. Never cuts inside one physical
    /// key (versions of a key stay in one table, matching
    /// [`should_split_before`]) and never cuts an empty output.
    fn should_cut_before(
        &mut self,
        row_physical_key: &[u8],
        pending_has_rows: bool,
        pending_last_physical_key: Option<&[u8]>,
    ) -> bool {
        while self.next_boundary < self.hints.boundaries.len()
            && row_physical_key
                >= self.hints.boundaries[self.next_boundary]
                    .start_physical_key
                    .as_slice()
        {
            if pending_has_rows {
                self.output_overlap_bytes = self
                    .output_overlap_bytes
                    .saturating_add(self.hints.boundaries[self.next_boundary].byte_count);
                self.output_crossings = self.output_crossings.saturating_add(1);
            }
            self.next_boundary = self.next_boundary.saturating_add(1);
        }
        if !pending_has_rows {
            // The output will start at this row: its overlap begins at the
            // interval containing the row.
            self.rebase_to_containing_interval();
            return false;
        }
        if pending_last_physical_key == Some(row_physical_key) {
            return false;
        }
        self.output_crossings >= 1 && self.output_overlap_bytes > self.hints.max_overlap_bytes
    }

    /// The current output was finished; the NEXT pushed row starts a new
    /// output whose overlap begins at the interval containing that row (the
    /// boundary cursor has already advanced to it).
    fn rebase_to_containing_interval(&mut self) {
        self.output_crossings = 0;
        self.output_overlap_bytes = match self.next_boundary.checked_sub(1) {
            Some(containing) => self.hints.boundaries[containing].byte_count,
            None => 0,
        };
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TableCompactionSourceId {
    text: String,
}

impl TableCompactionSourceId {
    pub(crate) fn new(text: impl Into<String>) -> TableRuntimeResult<Self> {
        let text = text.into();
        if text.is_empty() {
            return Err(TableRuntimeError::InvalidConfig {
                field: "compaction_source_id",
                reason: "must not be empty",
            });
        }
        if text.len() > MAX_SOURCE_ID_BYTES {
            return Err(TableRuntimeError::InvalidConfig {
                field: "compaction_source_id",
                reason: "is too large",
            });
        }
        if text.bytes().any(|byte| byte == 0) {
            return Err(TableRuntimeError::InvalidConfig {
                field: "compaction_source_id",
                reason: "must not contain nul bytes",
            });
        }
        Ok(Self { text })
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.text
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TableCompactionSource {
    id: TableCompactionSourceId,
    rows: Vec<TableRow>,
}

impl TableCompactionSource {
    pub(crate) fn from_rows(
        id: TableCompactionSourceId,
        rows: Vec<TableRow>,
    ) -> TableRuntimeResult<Self> {
        validate_strictly_sorted_unique_rows(&rows)?;
        Ok(Self { id, rows })
    }

    pub(crate) fn from_cursor(
        id: TableCompactionSourceId,
        cursor: &mut impl TableCursor,
    ) -> TableRuntimeResult<Self> {
        let mut rows = Vec::new();
        cursor.seek_to_first()?;
        while let Some(row) = cursor.current() {
            rows.push(row.clone());
            cursor.advance()?;
        }
        Self::from_rows(id, rows)
    }

    pub(crate) const fn id(&self) -> &TableCompactionSourceId {
        &self.id
    }

    pub(crate) fn len(&self) -> usize {
        self.rows.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    fn cursor(&self) -> Box<dyn TableCursor + '_> {
        Box::new(TableCompactionRowsCursor::new(&self.rows))
    }
}

pub(crate) trait TableCompactionInput {
    fn id(&self) -> &TableCompactionSourceId;
    fn open_cursor(&self) -> TableRuntimeResult<Box<dyn TableCursor + '_>>;

    fn requires_source_order_validation(&self) -> bool {
        true
    }
}

impl TableCompactionInput for TableCompactionSource {
    fn id(&self) -> &TableCompactionSourceId {
        self.id()
    }

    fn open_cursor(&self) -> TableRuntimeResult<Box<dyn TableCursor + '_>> {
        Ok(self.cursor())
    }

    fn requires_source_order_validation(&self) -> bool {
        false
    }
}

struct TableCompactionRowsCursor<'a> {
    rows: &'a [TableRow],
    position: Option<usize>,
}

impl<'a> TableCompactionRowsCursor<'a> {
    const fn new(rows: &'a [TableRow]) -> Self {
        Self {
            rows,
            position: None,
        }
    }

    fn seek_position(&self, target: &TableInternalKeyBytes) -> Option<usize> {
        let index = match self.rows.binary_search_by(|row| row.key().cmp(target)) {
            Ok(index) | Err(index) => index,
        };
        (index < self.rows.len()).then_some(index)
    }
}

impl TableCursor for TableCompactionRowsCursor<'_> {
    fn seek_to_first(&mut self) -> TableRuntimeResult<()> {
        self.position = (!self.rows.is_empty()).then_some(0);
        Ok(())
    }

    fn seek(&mut self, target: &TableInternalKeyBytes) -> TableRuntimeResult<()> {
        self.position = self.seek_position(target);
        Ok(())
    }

    fn advance(&mut self) -> TableRuntimeResult<()> {
        self.position = self.position.and_then(|position| {
            let next = position.saturating_add(1);
            (next < self.rows.len()).then_some(next)
        });
        Ok(())
    }

    fn current(&self) -> Option<&TableRow> {
        self.position.and_then(|position| self.rows.get(position))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TableCompactionDecision {
    Keep,
    Drop { reason: TableCompactionDropReason },
}

impl TableCompactionDecision {
    pub(crate) const fn drop(reason: TableCompactionDropReason) -> Self {
        Self::Drop { reason }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TableCompactionDropReason {
    CallerSelected,
    OlderVersion,
    TombstoneElided,
    Expired,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TableCompactionDropSummary {
    reason: TableCompactionDropReason,
    rows: u64,
}

impl TableCompactionDropSummary {
    const fn new(reason: TableCompactionDropReason) -> Self {
        Self { reason, rows: 0 }
    }

    pub(crate) const fn reason(self) -> TableCompactionDropReason {
        self.reason
    }

    pub(crate) const fn rows(self) -> u64 {
        self.rows
    }

    fn increment(&mut self) {
        self.rows = self.rows.saturating_add(1);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TableCompactionRowContext<'a> {
    source_id: &'a TableCompactionSourceId,
    source_index: usize,
    source_row_index: usize,
    merged_row_index: u64,
    previous_kept_key: Option<&'a TableInternalKeyBytes>,
}

impl<'a> TableCompactionRowContext<'a> {
    pub(crate) const fn source_id(self) -> &'a TableCompactionSourceId {
        self.source_id
    }

    pub(crate) const fn source_index(self) -> usize {
        self.source_index
    }

    pub(crate) const fn source_row_index(self) -> usize {
        self.source_row_index
    }

    pub(crate) const fn merged_row_index(self) -> u64 {
        self.merged_row_index
    }

    pub(crate) const fn previous_kept_key(self) -> Option<&'a TableInternalKeyBytes> {
        self.previous_kept_key
    }
}

pub(crate) trait TableCompactionPolicy {
    fn decide(
        &mut self,
        context: &TableCompactionRowContext<'_>,
        row: &TableRow,
    ) -> TableRuntimeResult<TableCompactionDecision>;
}

impl<F> TableCompactionPolicy for F
where
    F: for<'a> FnMut(
        &TableCompactionRowContext<'a>,
        &TableRow,
    ) -> TableRuntimeResult<TableCompactionDecision>,
{
    fn decide(
        &mut self,
        context: &TableCompactionRowContext<'_>,
        row: &TableRow,
    ) -> TableRuntimeResult<TableCompactionDecision> {
        self(context, row)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TableCompactionReport {
    input_sources: usize,
    input_rows: u64,
    kept_rows: u64,
    dropped_rows: u64,
    output_tables: usize,
    output_bytes: u64,
    split_count: u64,
    /// W1.3a: output cuts forced by the grandparent-overlap bound (a subset of
    /// `split_count`'s output boundaries).
    grandparent_cut_count: u64,
    peak_buffered_rows: usize,
    drop_summaries: Vec<TableCompactionDropSummary>,
}

impl TableCompactionReport {
    fn new(input_sources: usize) -> Self {
        Self {
            input_sources,
            input_rows: 0,
            kept_rows: 0,
            dropped_rows: 0,
            output_tables: 0,
            output_bytes: 0,
            split_count: 0,
            grandparent_cut_count: 0,
            peak_buffered_rows: 0,
            drop_summaries: Vec::new(),
        }
    }

    pub(crate) const fn input_sources(&self) -> usize {
        self.input_sources
    }

    pub(crate) const fn input_rows(&self) -> u64 {
        self.input_rows
    }

    pub(crate) const fn kept_rows(&self) -> u64 {
        self.kept_rows
    }

    pub(crate) const fn dropped_rows(&self) -> u64 {
        self.dropped_rows
    }

    pub(crate) const fn output_tables(&self) -> usize {
        self.output_tables
    }

    pub(crate) const fn output_bytes(&self) -> u64 {
        self.output_bytes
    }

    pub(crate) const fn split_count(&self) -> u64 {
        self.split_count
    }

    pub(crate) const fn grandparent_cut_count(&self) -> u64 {
        self.grandparent_cut_count
    }

    pub(crate) const fn peak_buffered_rows(&self) -> usize {
        self.peak_buffered_rows
    }

    pub(crate) fn drop_summaries(&self) -> &[TableCompactionDropSummary] {
        &self.drop_summaries
    }

    fn record_peak_buffered_rows(&mut self, rows: usize) {
        self.peak_buffered_rows = self.peak_buffered_rows.max(rows);
    }

    fn record_keep(&mut self) {
        self.kept_rows = self.kept_rows.saturating_add(1);
    }

    fn record_drop(&mut self, reason: TableCompactionDropReason) {
        self.dropped_rows = self.dropped_rows.saturating_add(1);
        if let Some(summary) = self
            .drop_summaries
            .iter_mut()
            .find(|summary| summary.reason == reason)
        {
            summary.increment();
            return;
        }
        let mut summary = TableCompactionDropSummary::new(reason);
        summary.increment();
        self.drop_summaries.push(summary);
    }

    /// Fold another range-build's report into this one, so N subcompaction reports aggregate
    /// into the single report of the whole compaction. `input_sources` is retained (identical
    /// across ranges — it is the candidate's source count, not per-range); `split_count` is
    /// recomputed as `output_tables - 1` to match the single-merge semantics.
    pub(crate) fn accumulate(&mut self, other: &Self) {
        self.input_rows = self.input_rows.saturating_add(other.input_rows);
        self.kept_rows = self.kept_rows.saturating_add(other.kept_rows);
        self.dropped_rows = self.dropped_rows.saturating_add(other.dropped_rows);
        self.output_tables = self.output_tables.saturating_add(other.output_tables);
        self.output_bytes = self.output_bytes.saturating_add(other.output_bytes);
        self.grandparent_cut_count = self
            .grandparent_cut_count
            .saturating_add(other.grandparent_cut_count);
        self.peak_buffered_rows = self.peak_buffered_rows.max(other.peak_buffered_rows);
        for summary in &other.drop_summaries {
            if let Some(existing) = self
                .drop_summaries
                .iter_mut()
                .find(|existing| existing.reason == summary.reason)
            {
                existing.rows = existing.rows.saturating_add(summary.rows);
            } else {
                self.drop_summaries.push(*summary);
            }
        }
        self.split_count = u64::try_from(self.output_tables)
            .unwrap_or(u64::MAX)
            .saturating_sub(1);
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TableCompactionOutput {
    artifacts: Vec<BuiltTableArtifact>,
    report: TableCompactionReport,
}

impl TableCompactionOutput {
    fn new(artifacts: Vec<BuiltTableArtifact>, report: TableCompactionReport) -> Self {
        Self { artifacts, report }
    }

    pub(crate) fn artifacts(&self) -> &[BuiltTableArtifact] {
        &self.artifacts
    }

    pub(crate) const fn report(&self) -> &TableCompactionReport {
        &self.report
    }

    pub(crate) fn into_parts(self) -> (Vec<BuiltTableArtifact>, TableCompactionReport) {
        (self.artifacts, self.report)
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct TableStreamingArtifactReport {
    output_tables: usize,
    output_bytes: u64,
    peak_buffered_rows: usize,
}

impl TableStreamingArtifactReport {
    pub(crate) const fn output_tables(&self) -> usize {
        self.output_tables
    }

    pub(crate) const fn peak_buffered_rows(&self) -> usize {
        self.peak_buffered_rows
    }

    fn record_peak_buffered_rows(&mut self, rows: usize) {
        self.peak_buffered_rows = self.peak_buffered_rows.max(rows);
    }
}

pub(crate) struct TableStreamingArtifactBuilder<F> {
    builder: ImmutableTableBuilder,
    max_rows_per_output: usize,
    pending_rows: Vec<TableRow>,
    artifacts: Vec<BuiltTableArtifact>,
    report: TableStreamingArtifactReport,
    identity_for_output: F,
}

impl<F> TableStreamingArtifactBuilder<F>
where
    F: FnMut(usize, &[TableRow]) -> TableRuntimeResult<TableIdentity>,
{
    pub(crate) fn new(
        builder_config: TableBuilderConfig,
        max_rows_per_output: usize,
        identity_for_output: F,
    ) -> TableRuntimeResult<Self> {
        if max_rows_per_output == 0 {
            return Err(TableRuntimeError::InvalidConfig {
                field: "max_rows_per_output",
                reason: "must be nonzero",
            });
        }
        Ok(Self {
            builder: ImmutableTableBuilder::new(builder_config)?,
            max_rows_per_output,
            pending_rows: Vec::new(),
            artifacts: Vec::new(),
            report: TableStreamingArtifactReport::default(),
            identity_for_output,
        })
    }

    pub(crate) fn push(&mut self, row: TableRow) -> TableRuntimeResult<()> {
        if self.pending_rows.len() >= self.max_rows_per_output {
            self.flush()?;
        }
        self.pending_rows.push(row);
        self.report
            .record_peak_buffered_rows(self.pending_rows.len());
        Ok(())
    }

    pub(crate) fn finish(
        mut self,
    ) -> TableRuntimeResult<(Vec<BuiltTableArtifact>, TableStreamingArtifactReport)> {
        self.flush()?;
        Ok((self.artifacts, self.report))
    }

    fn flush(&mut self) -> TableRuntimeResult<()> {
        if self.pending_rows.is_empty() {
            return Ok(());
        }
        let output_index = self.artifacts.len();
        let rows = std::mem::take(&mut self.pending_rows);
        let identity = (self.identity_for_output)(output_index, &rows)?;
        let artifact = build_table_artifact_from_rows(&self.builder, identity, &rows)?;
        self.report.output_bytes = self
            .report
            .output_bytes
            .saturating_add(artifact.byte_count());
        self.artifacts.push(artifact);
        self.report.output_tables = self.artifacts.len();
        Ok(())
    }
}

fn compact_table_inputs(
    compactor: &TableCompactor,
    output_identity_seed: &TableIdentity,
    sources: &[&dyn TableCompactionInput],
    policy: &mut (impl TableCompactionPolicy + ?Sized),
    global_duplicate_validation: GlobalDuplicateValidation,
) -> TableRuntimeResult<TableCompactionOutput> {
    let mut artifacts = Vec::new();
    let report = compact_table_inputs_into(
        compactor,
        output_identity_seed,
        sources,
        policy,
        global_duplicate_validation,
        &mut |artifact| {
            artifacts.push(artifact);
            Ok(())
        },
    )?;
    Ok(TableCompactionOutput::new(artifacts, report))
}

fn compact_table_inputs_into(
    compactor: &TableCompactor,
    output_identity_seed: &TableIdentity,
    sources: &[&dyn TableCompactionInput],
    policy: &mut (impl TableCompactionPolicy + ?Sized),
    global_duplicate_validation: GlobalDuplicateValidation,
    sink: &mut dyn FnMut(BuiltTableArtifact) -> TableRuntimeResult<()>,
) -> TableRuntimeResult<TableCompactionReport> {
    let builder = ImmutableTableBuilder::new(compactor.builder_config)?;
    let mut report = TableCompactionReport::new(sources.len());
    let source_identity = output_content_identity(compactor, sources);
    let mut merged = TableCompactionMergeCursor::new(sources)?;
    if global_duplicate_validation == GlobalDuplicateValidation::Run {
        validate_no_global_duplicate_internal_keys(&mut merged)?;
        merged.seek_to_first()?;
    }

    let target_output_bytes = compactor.config.target_output_bytes();
    require_nonzero_target_output_bytes(target_output_bytes)?;
    let mut pending_output = PendingCompactionOutput::new(
        builder,
        output_identity_seed,
        source_identity,
        compactor.config.max_output_tables(),
    );
    let mut previous_kept_key: Option<TableInternalKeyBytes> = None;
    let mut cut_tracker = compactor
        .output_cut_hints
        .as_ref()
        .and_then(GrandparentCutTracker::new);
    let merge_timer = perf_trace::start_timer();

    while let Some(current) = merged.current() {
        report.input_rows = report.input_rows.saturating_add(1);

        let context = TableCompactionRowContext {
            source_id: current.source_id,
            source_index: current.source_index,
            source_row_index: current.source_row_index,
            merged_row_index: report.input_rows.saturating_sub(1),
            previous_kept_key: previous_kept_key.as_ref(),
        };
        match policy.decide(&context, current.row)? {
            TableCompactionDecision::Keep => {
                let row_approximate_bytes = row_approximate_size_bytes(current.row)?;
                let current_physical_key = current.row.key().physical_key_bytes();
                let size_split = should_split_before(
                    pending_output.has_rows(),
                    pending_output.approximate_bytes(),
                    pending_output.last_physical_key(),
                    target_output_bytes,
                    row_approximate_bytes,
                    current_physical_key,
                );
                let grandparent_cut = cut_tracker.as_mut().is_some_and(|tracker| {
                    tracker.should_cut_before(
                        current_physical_key,
                        pending_output.has_rows(),
                        pending_output.last_physical_key(),
                    )
                });
                if size_split || grandparent_cut {
                    pending_output.finish_current(sink, &mut report)?;
                    if grandparent_cut {
                        report.grandparent_cut_count =
                            report.grandparent_cut_count.saturating_add(1);
                    }
                    if let Some(tracker) = cut_tracker.as_mut() {
                        tracker.rebase_to_containing_interval();
                    }
                }
                pending_output.push_row(
                    &mut report,
                    current.row,
                    row_approximate_bytes,
                    current_physical_key,
                )?;
                update_previous_kept_key(&mut previous_kept_key, current.row.key());
                report.record_keep();
                perf_trace::record_table_compaction_keep();
            }
            TableCompactionDecision::Drop { reason } => {
                report.record_drop(reason);
                perf_trace::record_table_compaction_drop();
            }
        }
        merged.advance()?;
    }
    perf_trace::record_table_compaction_merge_elapsed(
        perf_trace::timer_elapsed(merge_timer),
        report.input_rows,
    );

    pending_output.finish_current(sink, &mut report)?;

    perf_trace::record_table_compaction_peak_buffered_rows(report.peak_buffered_rows());

    Ok(report)
}

fn validate_no_global_duplicate_internal_keys(
    merged: &mut TableCompactionMergeCursor<'_>,
) -> TableRuntimeResult<()> {
    let mut previous_key: Option<TableInternalKeyBytes> = None;
    while let Some(current) = merged.current() {
        perf_trace::record_table_compaction_pre_validation_row();
        if let Some(previous) = &previous_key {
            if previous == current.row().key() {
                return Err(TableRuntimeError::DuplicateInternalKey {
                    key: current.row().encoded_key().to_vec(),
                });
            }
        }
        previous_key = Some(current.row().key().clone());
        merged.advance()?;
    }
    Ok(())
}

struct PendingCompactionOutput<'a> {
    builder: ImmutableTableBuilder,
    output_identity_seed: &'a TableIdentity,
    source_identity: u64,
    max_output_tables: usize,
    current: Option<ImmutableTableStreamingBuilder>,
    current_rows: usize,
    current_approximate_bytes: u64,
    current_last_physical_key: Option<Vec<u8>>,
    /// Outputs already handed to the sink — the output-index seed and the
    /// `max_output_tables` bound (artifacts are no longer accumulated here).
    finished_outputs: usize,
}

impl<'a> PendingCompactionOutput<'a> {
    fn new(
        builder: ImmutableTableBuilder,
        output_identity_seed: &'a TableIdentity,
        source_identity: u64,
        max_output_tables: usize,
    ) -> Self {
        Self {
            builder,
            output_identity_seed,
            source_identity,
            max_output_tables,
            finished_outputs: 0,
            current: None,
            current_rows: 0,
            current_approximate_bytes: 0,
            current_last_physical_key: None,
        }
    }

    const fn has_rows(&self) -> bool {
        self.current_rows > 0
    }

    const fn approximate_bytes(&self) -> u64 {
        self.current_approximate_bytes
    }

    fn last_physical_key(&self) -> Option<&[u8]> {
        self.current_last_physical_key.as_deref()
    }

    fn push_row(
        &mut self,
        report: &mut TableCompactionReport,
        row: &TableRow,
        row_approximate_bytes: u64,
        row_physical_key: &[u8],
    ) -> TableRuntimeResult<()> {
        if self.current.is_none() {
            if self.finished_outputs >= self.max_output_tables {
                return Err(TableRuntimeError::InvalidRange {
                    field: "max_output_tables",
                });
            }
            let output_index = self.finished_outputs;
            let identity = output_identity(
                self.output_identity_seed,
                self.source_identity,
                output_index,
            )?;
            self.current = Some(self.builder.begin_streaming(identity)?);
        }

        let current = self
            .current
            .as_mut()
            .expect("current output is initialized");
        current.append(row)?;
        self.current_rows = self.current_rows.saturating_add(1);
        self.current_approximate_bytes = self
            .current_approximate_bytes
            .saturating_add(row_approximate_bytes);
        update_pending_last_physical_key(&mut self.current_last_physical_key, row_physical_key);
        report.record_peak_buffered_rows(current.peak_buffered_rows());
        Ok(())
    }

    fn finish_current(
        &mut self,
        sink: &mut dyn FnMut(BuiltTableArtifact) -> TableRuntimeResult<()>,
        report: &mut TableCompactionReport,
    ) -> TableRuntimeResult<()> {
        if self.current_rows == 0 {
            return Ok(());
        }
        let current = self.current.take().ok_or(TableRuntimeError::InvalidRange {
            field: "pending_compaction_output",
        })?;
        self.current_rows = 0;
        self.current_approximate_bytes = 0;
        self.current_last_physical_key = None;

        let artifact = current.finish()?;
        perf_trace::record_table_compaction_output_table_built();
        report.output_bytes = report.output_bytes.saturating_add(artifact.byte_count());
        // Hand the completed output to the sink BEFORE building the next one:
        // a publishing sink frees the artifact's bytes immediately (W1.2c).
        sink(artifact)?;
        self.finished_outputs = self.finished_outputs.saturating_add(1);
        report.output_tables = self.finished_outputs;
        report.split_count = report.output_tables.saturating_sub(1) as u64;
        Ok(())
    }
}

pub(crate) struct TableCompactionMergeCursor<'a> {
    sources: Vec<TableCompactionSourceCursor<'a>>,
    selection: TableCompactionMergeSelection,
}

struct TableCompactionSourceCursor<'a> {
    source_id: &'a TableCompactionSourceId,
    source_index: usize,
    source_row_index: usize,
    validate_source_order: bool,
    last_key: Option<TableInternalKeyBytes>,
    cursor: Box<dyn TableCursor + 'a>,
}

enum TableCompactionMergeSelection {
    Linear { current_source: Option<usize> },
    Heap(TableCompactionIndexHeap),
}

pub(crate) struct TableCompactionMergedRow<'a> {
    source_id: &'a TableCompactionSourceId,
    source_index: usize,
    source_row_index: usize,
    row: &'a TableRow,
}

impl<'a> TableCompactionMergedRow<'a> {
    pub(crate) const fn row(&self) -> &'a TableRow {
        self.row
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct TableCompactionIndexHeap {
    source_indices: Vec<usize>,
}

impl<'a> TableCompactionMergeCursor<'a> {
    pub(crate) fn new(sources: &'a [&'a dyn TableCompactionInput]) -> TableRuntimeResult<Self> {
        let mut cursors = Vec::with_capacity(sources.len());
        let mut selection = TableCompactionMergeSelection::for_source_count(sources.len());
        for (source_index, source) in sources.iter().enumerate() {
            let mut cursor = source.open_cursor()?;
            perf_trace::record_table_compaction_merge_cursor_opens(1);
            cursor.seek_to_first()?;
            let validate_source_order = source.requires_source_order_validation();
            let last_key = if validate_source_order {
                cursor.current_key().map(clone_source_order_key)
            } else {
                None
            };
            cursors.push(TableCompactionSourceCursor {
                source_id: source.id(),
                source_index,
                source_row_index: 0,
                validate_source_order,
                last_key,
                cursor,
            });
        }
        selection.rebuild_current_sources(&cursors);
        Ok(Self {
            sources: cursors,
            selection,
        })
    }

    pub(crate) fn seek_to_first(&mut self) -> TableRuntimeResult<()> {
        self.selection.clear();
        for source in &mut self.sources {
            source.cursor.seek_to_first()?;
            source.source_row_index = 0;
            source.last_key = if source.validate_source_order {
                source.cursor.current_key().map(clone_source_order_key)
            } else {
                None
            };
        }
        self.selection.rebuild_current_sources(&self.sources);
        Ok(())
    }

    pub(crate) fn current(&self) -> Option<TableCompactionMergedRow<'_>> {
        let source_index = self.selection.current_source_index(&self.sources)?;
        let source = self.sources.get(source_index)?;
        let row = source.cursor.current()?;
        Some(TableCompactionMergedRow {
            source_id: source.source_id,
            source_index: source.source_index,
            source_row_index: source.source_row_index,
            row,
        })
    }

    pub(crate) fn advance(&mut self) -> TableRuntimeResult<()> {
        let Some(source_index) = self.selection.take_current_source_index(&self.sources) else {
            return Ok(());
        };
        perf_trace::record_table_compaction_merge_advance();
        {
            let source =
                self.sources
                    .get_mut(source_index)
                    .ok_or(TableRuntimeError::InvalidRange {
                        field: "compaction_source_index",
                    })?;
            source.cursor.advance()?;
            source.source_row_index = source.source_row_index.saturating_add(1);
            if let Some(key) = source.cursor.current_key() {
                if source.validate_source_order {
                    validate_compaction_source_key_order(source.last_key.as_ref(), key)?;
                    source.last_key = Some(clone_source_order_key(key));
                }
            }
        }
        self.selection
            .push_current_source(source_index, &self.sources);
        self.selection.refresh_current_source(&self.sources);
        Ok(())
    }
}

impl TableCompactionMergeSelection {
    fn for_source_count(source_count: usize) -> Self {
        if source_count <= MERGE_HEAP_THRESHOLD {
            Self::Linear {
                current_source: None,
            }
        } else {
            Self::Heap(TableCompactionIndexHeap::default())
        }
    }

    fn clear(&mut self) {
        match self {
            Self::Linear { current_source } => *current_source = None,
            Self::Heap(heap) => heap.clear(),
        }
    }

    fn rebuild_current_sources(&mut self, sources: &[TableCompactionSourceCursor<'_>]) {
        match self {
            Self::Linear { current_source } => {
                *current_source = select_linear_compaction_source(sources);
            }
            Self::Heap(heap) => heap.rebuild(sources),
        }
    }

    fn push_current_source(
        &mut self,
        source_index: usize,
        sources: &[TableCompactionSourceCursor<'_>],
    ) {
        if let Self::Heap(heap) = self {
            heap.push(source_index, sources);
        }
    }

    fn current_source_index(&self, sources: &[TableCompactionSourceCursor<'_>]) -> Option<usize> {
        match self {
            Self::Linear { current_source } => *current_source,
            Self::Heap(heap) => heap.peek(sources),
        }
    }

    fn take_current_source_index(
        &mut self,
        sources: &[TableCompactionSourceCursor<'_>],
    ) -> Option<usize> {
        match self {
            Self::Linear { current_source } => *current_source,
            Self::Heap(heap) => heap.pop(sources),
        }
    }

    fn refresh_current_source(&mut self, sources: &[TableCompactionSourceCursor<'_>]) {
        if let Self::Linear { current_source } = self {
            *current_source = select_linear_compaction_source(sources);
        }
    }
}

impl TableCompactionIndexHeap {
    fn clear(&mut self) {
        self.source_indices.clear();
    }

    fn rebuild(&mut self, sources: &[TableCompactionSourceCursor<'_>]) {
        self.clear();
        for source_index in 0..sources.len() {
            self.push(source_index, sources);
        }
    }

    fn push(&mut self, source_index: usize, sources: &[TableCompactionSourceCursor<'_>]) {
        if source_current_key(sources, source_index).is_none() {
            return;
        }
        self.source_indices.push(source_index);
        self.sift_up(self.source_indices.len().saturating_sub(1), sources);
    }

    fn peek(&self, sources: &[TableCompactionSourceCursor<'_>]) -> Option<usize> {
        self.source_indices
            .first()
            .copied()
            .filter(|source_index| source_current_key(sources, *source_index).is_some())
    }

    fn pop(&mut self, sources: &[TableCompactionSourceCursor<'_>]) -> Option<usize> {
        let selected = *self.source_indices.first()?;
        let last = self.source_indices.pop().expect("heap is nonempty");
        if !self.source_indices.is_empty() {
            self.source_indices[0] = last;
            self.sift_down(0, sources);
        }
        Some(selected)
    }

    fn sift_up(&mut self, mut index: usize, sources: &[TableCompactionSourceCursor<'_>]) {
        while index > 0 {
            let parent = (index - 1) / 2;
            if !source_precedes(
                sources,
                self.source_indices[index],
                self.source_indices[parent],
            ) {
                break;
            }
            self.source_indices.swap(index, parent);
            index = parent;
        }
    }

    fn sift_down(&mut self, mut index: usize, sources: &[TableCompactionSourceCursor<'_>]) {
        loop {
            let left = index.saturating_mul(2).saturating_add(1);
            let right = left.saturating_add(1);
            let mut smallest = index;
            if left < self.source_indices.len()
                && source_precedes(
                    sources,
                    self.source_indices[left],
                    self.source_indices[smallest],
                )
            {
                smallest = left;
            }
            if right < self.source_indices.len()
                && source_precedes(
                    sources,
                    self.source_indices[right],
                    self.source_indices[smallest],
                )
            {
                smallest = right;
            }
            if smallest == index {
                break;
            }
            self.source_indices.swap(index, smallest);
            index = smallest;
        }
    }
}

fn select_linear_compaction_source(sources: &[TableCompactionSourceCursor<'_>]) -> Option<usize> {
    let mut selected: Option<(usize, &TableInternalKeyBytes)> = None;
    for (source_index, source) in sources.iter().enumerate() {
        let Some(key) = source.cursor.current_key() else {
            continue;
        };
        let replace = match selected {
            None => true,
            Some((selected_source, selected_key)) => {
                key < selected_key || (key == selected_key && source_index < selected_source)
            }
        };
        if replace {
            selected = Some((source_index, key));
        }
    }
    selected.map(|(source_index, _)| source_index)
}

fn source_current_key<'sources>(
    sources: &'sources [TableCompactionSourceCursor<'_>],
    source_index: usize,
) -> Option<&'sources TableInternalKeyBytes> {
    sources.get(source_index)?.cursor.current_key()
}

fn source_precedes(
    sources: &[TableCompactionSourceCursor<'_>],
    left_source_index: usize,
    right_source_index: usize,
) -> bool {
    let Some(left_key) = source_current_key(sources, left_source_index) else {
        return false;
    };
    let Some(right_key) = source_current_key(sources, right_source_index) else {
        return true;
    };
    left_key < right_key || (left_key == right_key && left_source_index < right_source_index)
}

fn clone_source_order_key(key: &TableInternalKeyBytes) -> TableInternalKeyBytes {
    perf_trace::record_table_compaction_source_order_key_clone();
    key.clone()
}

fn validate_compaction_source_key_order(
    previous: Option<&TableInternalKeyBytes>,
    current: &TableInternalKeyBytes,
) -> TableRuntimeResult<()> {
    let Some(previous) = previous else {
        return Ok(());
    };
    match previous.cmp(current) {
        Ordering::Less => Ok(()),
        Ordering::Equal => Err(TableRuntimeError::DuplicateInternalKey {
            key: current.as_slice().to_vec(),
        }),
        Ordering::Greater => Err(TableRuntimeError::InvalidRowOrder {
            previous: previous.as_slice().to_vec(),
            current: current.as_slice().to_vec(),
        }),
    }
}

fn require_nonzero_target_output_bytes(target_output_bytes: u64) -> TableRuntimeResult<()> {
    if target_output_bytes == 0 {
        return Err(TableRuntimeError::InvalidConfig {
            field: "target_output_bytes",
            reason: "must be nonzero",
        });
    }
    Ok(())
}

fn should_split_before(
    pending_has_rows: bool,
    pending_approximate_bytes: u64,
    pending_last_physical_key: Option<&[u8]>,
    target_output_bytes: u64,
    row_approximate_bytes: u64,
    row_physical_key: &[u8],
) -> bool {
    if !pending_has_rows {
        return false;
    }
    let would_cross_target =
        pending_approximate_bytes.saturating_add(row_approximate_bytes) > target_output_bytes;
    if !would_cross_target {
        return false;
    }
    pending_last_physical_key != Some(row_physical_key)
}

fn row_approximate_size_bytes(row: &TableRow) -> TableRuntimeResult<u64> {
    u64::try_from(row.approximate_size_bytes()).map_err(|_| TableRuntimeError::InvalidRange {
        field: "row_approximate_size",
    })
}

fn update_pending_last_physical_key(
    pending_last_physical_key: &mut Option<Vec<u8>>,
    row_physical_key: &[u8],
) {
    if pending_last_physical_key.as_deref() == Some(row_physical_key) {
        return;
    }
    perf_trace::record_table_compaction_boundary_key_allocation();
    if let Some(buffer) = pending_last_physical_key.as_mut() {
        if buffer.capacity() < row_physical_key.len() {
            perf_trace::record_table_compaction_boundary_key_buffer_allocation();
        } else {
            perf_trace::record_table_compaction_boundary_key_buffer_reuse();
        }
        buffer.clear();
        buffer.extend_from_slice(row_physical_key);
    } else {
        perf_trace::record_table_compaction_boundary_key_buffer_allocation();
        *pending_last_physical_key = Some(row_physical_key.to_vec());
    }
}

fn update_previous_kept_key(
    previous_kept_key: &mut Option<TableInternalKeyBytes>,
    current_key: &TableInternalKeyBytes,
) {
    if let Some(previous) = previous_kept_key.as_mut() {
        perf_trace::record_table_compaction_previous_key_buffer_reuse();
        previous.copy_from(current_key);
    } else {
        perf_trace::record_table_compaction_previous_key_buffer_allocation();
        *previous_kept_key = Some(current_key.clone());
    }
}

fn build_table_artifact_from_rows(
    builder: &ImmutableTableBuilder,
    identity: TableIdentity,
    rows: &[TableRow],
) -> TableRuntimeResult<BuiltTableArtifact> {
    builder.build_from_rows(identity, rows)
}

fn output_identity(
    seed: &TableIdentity,
    source_identity: u64,
    output_index: usize,
) -> TableRuntimeResult<TableIdentity> {
    TableIdentity::new(format!(
        "{}-{source_identity:016x}-{output_index:08x}",
        seed.as_str()
    ))
}

/// #3469: the content hash in a compaction output's identity. Output ids are
/// create-if-absent and a retry ADOPTS an existing id after a byte-exact check
/// (DUR-016), so the id must be content-complete: every input that decides
/// the output's bytes is folded in, making id-equal imply byte-equal. That
/// keeps a byte mismatch on adoption meaning corruption (fail-closed is
/// right), and gives a retry whose byte-affecting inputs drifted (codec,
/// block size, split target, grandparent cuts, pruning floors, subcompaction
/// bounds) a DISTINCT name instead of a hard failure on the stale orphan.
///
/// Inputs: the source tables (immutable, named by content-derived source
/// ids), the compaction and builder configs, the grandparent cut hints, and
/// the caller's salt for what this layer cannot see. The output index is
/// appended separately by [`output_identity`].
fn output_content_identity(
    compactor: &TableCompactor,
    sources: &[&dyn TableCompactionInput],
) -> u64 {
    let TableCompactor {
        config,
        builder_config,
        output_cut_hints,
        output_identity_salt,
    } = compactor;
    let mut hasher = OutputContentHasher::new();
    hasher.word(sources.len() as u64);
    for source in sources {
        hasher.bytes(source.id().as_str().as_bytes());
    }
    for word in config.output_content_words() {
        hasher.word(word);
    }
    for word in builder_config.output_content_words() {
        hasher.word(word);
    }
    match output_cut_hints {
        None => hasher.word(0),
        Some(CompactionOutputCutHints {
            boundaries,
            max_overlap_bytes,
        }) => {
            hasher.word(1);
            hasher.word(*max_overlap_bytes);
            hasher.word(boundaries.len() as u64);
            for CompactionCutBoundary {
                start_physical_key,
                byte_count,
            } in boundaries
            {
                hasher.bytes(start_physical_key);
                hasher.word(*byte_count);
            }
        }
    }
    hasher.word(*output_identity_salt);
    hasher.finish()
}

/// #3469: a length-prefixed FNV-1a accumulator for output-identity content
/// fingerprints (words and byte strings are length-framed, so distinct input
/// sequences cannot concatenate to the same stream).
#[derive(Clone, Copy, Debug)]
pub(crate) struct OutputContentHasher {
    hash: u64,
}

impl OutputContentHasher {
    pub(crate) const fn new() -> Self {
        Self {
            hash: OUTPUT_IDENTITY_METADATA_HASH_OFFSET,
        }
    }

    pub(crate) fn word(&mut self, value: u64) {
        hash_metadata_u64(&mut self.hash, value);
    }

    pub(crate) fn bytes(&mut self, bytes: &[u8]) {
        hash_metadata_bytes(&mut self.hash, bytes);
    }

    /// The key range a (sub)compaction reads decides which rows its outputs
    /// hold, so it is part of the output content.
    pub(crate) fn key_bounds(&mut self, bounds: Option<&TableKeyBounds>) {
        match bounds {
            None => self.word(0),
            Some(TableKeyBounds::Range { lower, upper }) => {
                self.word(1);
                self.internal_bound(lower);
                self.internal_bound(upper);
            }
            Some(TableKeyBounds::Prefix(prefix)) => {
                self.word(2);
                self.bytes(prefix);
            }
            Some(TableKeyBounds::PhysicalRange {
                physical_prefix,
                lower,
                upper,
            }) => {
                self.word(3);
                self.bytes(physical_prefix);
                self.physical_bound(lower);
                self.physical_bound(upper);
            }
        }
    }

    fn internal_bound(&mut self, bound: &TableKeyBound) {
        match bound {
            TableKeyBound::Unbounded => self.word(0),
            TableKeyBound::Included(key) => {
                self.word(1);
                self.bytes(key.as_slice());
            }
            TableKeyBound::Excluded(key) => {
                self.word(2);
                self.bytes(key.as_slice());
            }
        }
    }

    fn physical_bound(&mut self, bound: &TablePhysicalKeyBound) {
        match bound {
            TablePhysicalKeyBound::Unbounded => self.word(0),
            TablePhysicalKeyBound::Included(key) => {
                self.word(1);
                self.bytes(key.as_slice());
            }
            TablePhysicalKeyBound::Excluded(key) => {
                self.word(2);
                self.bytes(key.as_slice());
            }
        }
    }

    pub(crate) const fn finish(self) -> u64 {
        self.hash
    }
}

fn hash_metadata_u64(hash: &mut u64, value: u64) {
    hash_metadata_bytes(hash, &value.to_le_bytes());
}

fn hash_metadata_bytes(hash: &mut u64, bytes: &[u8]) {
    hash_metadata_u64_raw(hash, bytes.len() as u64);
    for byte in bytes {
        *hash ^= u64::from(*byte);
        *hash = hash.wrapping_mul(OUTPUT_IDENTITY_METADATA_HASH_PRIME);
    }
}

fn hash_metadata_u64_raw(hash: &mut u64, value: u64) {
    for byte in value.to_le_bytes() {
        *hash ^= u64::from(byte);
        *hash = hash.wrapping_mul(OUTPUT_IDENTITY_METADATA_HASH_PRIME);
    }
}
