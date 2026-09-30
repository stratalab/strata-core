use super::config::BranchRuntimeConfig;
use super::error::{
    BranchCompactionInvalidity, BranchRuntimeError, BranchRuntimeResult,
    BranchTimestampHistorySource,
};
use super::facts::{
    BranchLevel, BranchLevelTableCount, BranchProtectedTable, BranchProtectionReason,
    BranchReachabilityAggregate, BranchReachabilityFacts, BranchReachabilitySnapshot,
    BranchReleasePlan, BranchStateFacts, BranchTableDescriptor, BranchTableRef,
    BranchTableReferenceKind, InheritedLayerDescriptor, InheritedLayerStatus, SharedTableRegistry,
};
use super::identity::{
    require_physical_key_branch, require_row_branch, rewrite_physical_key_branch,
    rewrite_row_branch, row_matches_branch, BranchRowIdentity,
};
use super::pruning::{
    branch_pruning_fingerprint, BranchCompactionPruningProof, BranchRecoveryHealthAttestation,
};
use super::read::{
    BranchEffectiveReadBound, BranchHistoryOptions, BranchHistoryRow, BranchInheritedLayer,
    BranchMaterializationSource, BranchOwnedTable, BranchReadBound, BranchReadView,
    BranchRowSource, BranchScanBounds, BranchTimestampCoverage, BranchUserKeyBound,
    BranchVisibleRow,
};
use super::state::compaction::{
    BranchCompactionCandidate, BranchCompactionKind, BranchCompactionNoPromotionReason,
    BranchCompactionNoopReason, BranchCompactionOperation, BranchCompactionOutcome,
    BranchCompactionPlan, BranchCompactionRequest, BranchCompactionRetentionPolicy,
};
use super::state::fork::BranchForkOutcome;
use super::state::manifest_recovery::BranchTableManifestRecoveryRequest;
use super::state::materialization::{
    BranchMaterializationOutcome, BranchMaterializationRecovery, BranchMaterializationRequest,
};
use super::state::read_hooks::{BranchStateDescriptor, BranchViewDescriptor};
use super::state::rotation::BranchRotationSkipReason;
use super::state::snapshot::{
    install_snapshot_rows_into_branches, BranchSnapshotInstallGroup, BranchSnapshotInstallOutcome,
    BranchSnapshotInstallRequest, BranchSnapshotMissingBranchPolicy,
};
use super::state::{BranchImmutableInstallOutcome, BranchLocalState, BranchRotationOutcome};
use crate::row::{PhysicalKey, StorageRow, StorageSpaceId};
use crate::table::{
    sort_table_rows_by_key, ImmutableTableBuilder, ImmutableTableReader, MutableTable,
    TableBuilderConfig, TableCommitRange, TableCompactionConfig, TableCompactionDecision,
    TableCompactionPolicy, TableCompactionRowContext, TableCompactionSource,
    TableCompactionSourceId, TableCompactor, TableIdentity, TableInternalKeyBytes, TableKeyRange,
    TablePhysicalKeyBytes, TableReaderConfig, TableRow, TableRuntimeFacts,
};
use std::error::Error;
use std::fmt;
use strata_core::{BranchId, CommitVersion, Timestamp};

fn keep_all_policy() -> impl TableCompactionPolicy {
    |_: &TableCompactionRowContext<'_>, _: &TableRow| Ok(TableCompactionDecision::Keep)
}

mod facts_reachability;
mod flush_install;
#[cfg(feature = "perf-trace")]
mod history_pruning;
mod identity_state;
mod immutable_reads;
mod inheritance_materialization;
mod owned_compaction;
mod point_pruning;
mod read_view;
mod row_pruning;
#[cfg(feature = "perf-trace")]
mod scan_pruning;
mod scan_runtime;
mod shape_aggregates;
mod snapshot_install;
mod source_layout;

fn assert_invalid_config_field(
    result: BranchRuntimeResult<BranchRuntimeConfig>,
    expected_field: &'static str,
) {
    match result {
        Err(BranchRuntimeError::InvalidConfig { field, .. }) => {
            assert_eq!(field, expected_field);
        }
        other => panic!("expected invalid config field {expected_field}, got {other:?}"),
    }
}

fn assert_duplicate_internal_key(error: &BranchRuntimeError) {
    assert!(matches!(
        error,
        BranchRuntimeError::TableRuntime {
            source: crate::table::TableRuntimeError::DuplicateInternalKey { .. },
        }
    ));
    assert!(error.source().is_some());
    assert!(!error.to_string().contains("secret-payload"));
}

fn branch_id(byte: u8) -> BranchId {
    BranchId::from_bytes([byte; BranchId::BYTE_LEN])
}

fn table_facts(identity: &str) -> TableRuntimeFacts {
    TableRuntimeFacts::new(
        TableIdentity::new(identity).expect("identity"),
        2,
        1,
        TableKeyRange::new(vec![0x01], vec![0x02]).expect("key range"),
        TableCommitRange::new(CommitVersion::new(1), CommitVersion::new(2)).expect("commit range"),
        128,
    )
    .expect("table facts")
}

// BS4.4f: the constructor takes extras as an argument; tests derive it from the (eager) reader's rows.
fn table_summary_extras(reader: &ImmutableTableReader<'_>) -> crate::table::TableSummaryExtras {
    crate::table::TableSummaryExtras::from_rows(reader.rows()).expect("table summary extras")
}

fn branch_owned_table(
    branch: BranchId,
    level: BranchLevel,
    identity: &str,
    rows: Vec<StorageRow>,
) -> BranchOwnedTable {
    let reader = immutable_reader(identity, rows);
    let descriptor = branch_table_descriptor(level, &reader);
    let extras = table_summary_extras(&reader);
    BranchOwnedTable::new(branch, descriptor, reader, extras).expect("branch-owned table")
}

fn branch_inherited_layer(
    source: BranchId,
    fork_version: CommitVersion,
    status: InheritedLayerStatus,
    owned_levels: Vec<Vec<BranchOwnedTable>>,
) -> BranchInheritedLayer {
    let table_count = owned_levels.iter().map(Vec::len).sum();
    BranchInheritedLayer::new(
        InheritedLayerDescriptor::new(source, fork_version, status, table_count),
        owned_levels,
    )
    .expect("branch inherited layer")
}

fn branch_inherited_layer_unchecked_for_fork_gate_tests(
    source: BranchId,
    fork_version: CommitVersion,
    status: InheritedLayerStatus,
    owned_levels: Vec<Vec<BranchOwnedTable>>,
) -> BranchInheritedLayer {
    let table_count = owned_levels.iter().map(Vec::len).sum();
    BranchInheritedLayer::new_unchecked_for_test(
        InheritedLayerDescriptor::new(source, fork_version, status, table_count),
        owned_levels,
    )
}

fn branch_table_descriptor(
    level: BranchLevel,
    reader: &ImmutableTableReader,
) -> BranchTableDescriptor {
    BranchTableDescriptor::new(
        reader.facts().identity().clone(),
        reader.facts().clone(),
        level,
    )
    .expect("branch table descriptor")
}

fn immutable_reader(identity: &str, rows: Vec<StorageRow>) -> ImmutableTableReader<'static> {
    let mut rows = rows.into_iter().map(TableRow::new).collect::<Vec<_>>();
    sort_table_rows_by_key(&mut rows);
    let identity = TableIdentity::new(identity).expect("identity");
    let builder = ImmutableTableBuilder::new(TableBuilderConfig::default()).expect("builder");
    let artifact = builder
        .build_from_rows(identity.clone(), &rows)
        .expect("built table");
    ImmutableTableReader::open_bytes(
        identity,
        artifact.into_bytes(),
        TableReaderConfig::default(),
    )
    .expect("immutable table reader")
}

fn expected_keep_all_compaction_output_identity(
    output_seed: &TableIdentity,
    sources: Vec<(TableCompactionSourceId, Vec<StorageRow>)>,
) -> TableIdentity {
    let sources = sources
        .into_iter()
        .map(|(source_id, rows)| {
            let mut table_rows = rows.into_iter().map(TableRow::new).collect::<Vec<_>>();
            sort_table_rows_by_key(&mut table_rows);
            TableCompactionSource::from_rows(source_id, table_rows).expect("source rows")
        })
        .collect::<Vec<_>>();
    // #3469: mirror the branch layer's content-complete identity — a default
    // `KeepAll`, unbounded request salts with its policy and bounds.
    let compactor = TableCompactor::default().with_output_identity_salt(
        super::state::compaction::rewrite_output_identity_salt(
            BranchCompactionRetentionPolicy::KeepAll,
            None,
            None,
        ),
    );
    let mut policy = keep_all_policy();
    let output = compactor
        .compact(output_seed, &sources, &mut policy)
        .expect("expected compaction");
    output.artifacts()[0].facts().identity().clone()
}

fn branch_compaction_source_id(
    source_index: usize,
    level: BranchLevel,
    table_index: usize,
    table_identity: &str,
    rows: &[StorageRow],
) -> TableCompactionSourceId {
    let row_count = rows.len();
    let min_commit = rows
        .iter()
        .map(StorageRow::commit_version)
        .min()
        .expect("source rows are not empty");
    let max_commit = rows
        .iter()
        .map(StorageRow::commit_version)
        .max()
        .expect("source rows are not empty");
    let source_hash = test_branch_source_metadata_hash(
        level,
        table_index,
        table_identity,
        row_count as u64,
        min_commit.as_u64(),
        max_commit.as_u64(),
    );
    TableCompactionSourceId::new(format!("s{source_index}-h{source_hash:016x}",))
        .expect("source id")
}

fn test_branch_source_metadata_hash(
    level: BranchLevel,
    table_index: usize,
    table_identity: &str,
    row_count: u64,
    commit_min: u64,
    commit_max: u64,
) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = OFFSET;
    test_hash_source_metadata_u64(&mut hash, u64::from(level.raw()), PRIME);
    test_hash_source_metadata_u64(&mut hash, table_index as u64, PRIME);
    test_hash_source_metadata_bytes(&mut hash, table_identity.as_bytes(), PRIME);
    test_hash_source_metadata_u64(&mut hash, row_count, PRIME);
    test_hash_source_metadata_u64(&mut hash, commit_min, PRIME);
    test_hash_source_metadata_u64(&mut hash, commit_max, PRIME);
    hash
}

fn test_hash_source_metadata_u64(hash: &mut u64, value: u64, prime: u64) {
    test_hash_source_metadata_bytes(hash, &value.to_le_bytes(), prime);
}

fn test_hash_source_metadata_bytes(hash: &mut u64, bytes: &[u8], prime: u64) {
    test_hash_source_metadata_u64_raw(hash, bytes.len() as u64, prime);
    for byte in bytes {
        *hash ^= u64::from(*byte);
        *hash = hash.wrapping_mul(prime);
    }
}

fn test_hash_source_metadata_u64_raw(hash: &mut u64, value: u64, prime: u64) {
    for byte in value.to_le_bytes() {
        *hash ^= u64::from(byte);
        *hash = hash.wrapping_mul(prime);
    }
}

fn sorted_storage_rows(mut rows: Vec<StorageRow>) -> Vec<StorageRow> {
    sort_storage_rows_by_internal_key(&mut rows);
    rows
}

fn sort_storage_rows_by_internal_key(rows: &mut [StorageRow]) {
    rows.sort_by_key(TableInternalKeyBytes::from_row);
}

fn row_versions(rows: &[TableRow]) -> Vec<u64> {
    rows.iter()
        .map(|row| row.commit_version().as_u64())
        .collect()
}

fn matching_versions(rows: &[TableRow], bound: BranchEffectiveReadBound) -> Vec<u64> {
    rows.iter()
        .filter(|row| bound.matches_row(row.row()))
        .map(|row| row.commit_version().as_u64())
        .collect()
}

fn history_versions(rows: &[BranchHistoryRow]) -> Vec<u64> {
    rows.iter()
        .map(|row| row.row().commit_version().as_u64())
        .collect()
}

fn history_user_keys(rows: &[BranchHistoryRow]) -> Vec<Vec<u8>> {
    rows.iter()
        .map(|row| row.row().physical_key().user_key().to_vec())
        .collect()
}

fn scan_user_keys(rows: &[BranchVisibleRow]) -> Vec<Vec<u8>> {
    rows.iter()
        .map(|row| row.row().physical_key().user_key().to_vec())
        .collect()
}

fn assert_visible_row(
    actual: Option<&BranchVisibleRow>,
    expected_row: &StorageRow,
    expected_source: BranchRowSource,
) {
    let actual = actual.expect("visible row");
    assert_eq!(actual.row(), expected_row);
    assert_eq!(actual.source(), expected_source);
}

fn visible_storage_row(actual: Option<BranchVisibleRow>) -> Option<StorageRow> {
    actual.map(|row| row.row().clone())
}

fn storage_row(branch_id: BranchId, version: u64) -> StorageRow {
    storage_row_with(
        branch_id,
        b"key".to_vec(),
        version,
        version,
        Timestamp::EPOCH,
        b"row-bytes".to_vec(),
    )
}

fn storage_row_with(
    branch_id: BranchId,
    user_key: Vec<u8>,
    version: u64,
    timestamp: u64,
    expires_at: Timestamp,
    value: Vec<u8>,
) -> StorageRow {
    StorageRow::put(
        physical_key(branch_id, user_key),
        CommitVersion::new(version),
        Timestamp::from_micros(timestamp),
        expires_at,
        value,
    )
}

fn storage_row_with_named_space(
    branch_id: BranchId,
    space_name: &str,
    storage_space_id: StorageSpaceId,
    user_key: Vec<u8>,
    version: u64,
    timestamp: u64,
    value: Vec<u8>,
) -> StorageRow {
    StorageRow::put(
        physical_key_with(branch_id, space_name, storage_space_id, user_key),
        CommitVersion::new(version),
        Timestamp::from_micros(timestamp),
        Timestamp::EPOCH,
        value,
    )
}

fn tombstone_row(
    branch_id: BranchId,
    user_key: Vec<u8>,
    version: u64,
    timestamp: u64,
) -> StorageRow {
    StorageRow::tombstone(
        physical_key(branch_id, user_key),
        CommitVersion::new(version),
        Timestamp::from_micros(timestamp),
    )
}

fn physical_key(branch_id: BranchId, user_key: Vec<u8>) -> PhysicalKey {
    let space = StorageSpaceId::engine(0x20).expect("engine storage space");
    physical_key_with(branch_id, "default", space, user_key)
}

fn physical_key_with(
    branch_id: BranchId,
    space_name: &str,
    storage_space_id: StorageSpaceId,
    user_key: Vec<u8>,
) -> PhysicalKey {
    PhysicalKey::new(branch_id, space_name.to_owned(), storage_space_id, user_key)
        .expect("physical key")
}

#[derive(Debug)]
struct LeafError;

impl fmt::Display for LeafError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("leaf source")
    }
}

impl Error for LeafError {}
