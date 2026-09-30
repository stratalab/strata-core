//! Generated lifecycle table rewrite contract helpers.

use super::{ensure, script_byte};
use crate::backend::{
    Backend, BackendAppend, BackendCapabilities, BackendError, BackendErrorKind, BackendMetadata,
    BackendRange, BackendResult, BackendWriterGuard, PublishDurability, PublishError,
    PublishFailureKind, PublishMode, PublishOutcome, PublishResult,
    DURABLE_LOCAL_MODE_REQUIREMENTS,
};
use crate::branch::config::BranchRuntimeConfig;
use crate::branch::facts::{BranchLevel, BranchTableDescriptor};
use crate::branch::read::BranchOwnedTable;
use crate::branch::state::compaction::BranchCompactionKind;
use crate::branch::state::BranchLocalState;
use crate::commit::{CommitBranchGeneration, CommitManualTimestampSource, CommitRuntimeConfig};
use crate::lifecycle::{
    collect_storage_pressure, compact_cache_branch, compact_durable_branch,
    materialize_cache_branch, LifecycleCodecId, LifecycleCompactionRequest,
    LifecycleCompactionStatus, LifecycleConfig, LifecycleDurableLocalOpenRequest,
    LifecycleDurableLocalRuntime, LifecycleDurableLocalShell, LifecycleMaintenanceExecutor,
    LifecycleMaterializationRequest, LifecycleMaterializationStatus, LifecycleRecoveryRequest,
    LifecycleRecoveryRuntime, LifecycleStoragePressureReason, LifecycleStoragePressureSeverity,
    MaintenanceOutcomeStatus, RecoveryStrictness, StorageMode, StorageOpenPlan,
};
use crate::object::{ObjectName, ObjectPrefix};
use crate::row::{PhysicalKey, StorageRow, StorageSpaceId};
use crate::service::WalServiceConfig;
use crate::table::{
    sort_table_rows_by_key, ImmutableTableBuilder, ImmutableTableReader, TableBuilderConfig,
    TableIdentity, TableReaderConfig, TableRow,
};
use crate::testkit::TestkitError;
use std::collections::BTreeMap;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use strata_core::{BranchId, CommitVersion, Timestamp};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct LifecycleTableRewriteContractOutcome {
    cache_compactions: usize,
    durable_compactions: usize,
    materializations: usize,
    pressure_cases: usize,
    compaction_output_published: usize,
    materialization_output_published: usize,
    output_reopened: usize,
    install_after_publish: usize,
    manifest_after_install: usize,
    publish_failed_before_install: usize,
    install_failed_after_publish: usize,
    manifest_failed_after_install: usize,
    orphan_output_recorded: usize,
    no_pruning_observed: usize,
}

pub fn check_lifecycle_table_rewrite_contract(
    script: &[u8],
) -> Result<LifecycleTableRewriteContractOutcome, TestkitError> {
    let mut outcome = LifecycleTableRewriteContractOutcome::default();
    check_cache_compaction(script, &mut outcome)?;
    check_durable_compaction(script, &mut outcome)?;
    check_manifest_backed_durable_compaction(script, &mut outcome)?;
    check_manifest_backed_durable_materialization(script, &mut outcome)?;
    check_publish_failure_before_install(script, &mut outcome)?;
    check_install_failure_after_publish(script, &mut outcome)?;
    check_manifest_failure_after_install(script, &mut outcome)?;
    check_cache_materialization(script, &mut outcome)?;
    check_storage_pressure(script, &mut outcome)?;
    Ok(outcome)
}

impl LifecycleTableRewriteContractOutcome {
    pub const fn cache_compaction_cases(&self) -> usize {
        self.cache_compactions
    }

    pub const fn durable_compaction_cases(&self) -> usize {
        self.durable_compactions
    }

    pub const fn materialization_cases(&self) -> usize {
        self.materializations
    }

    pub const fn pressure_cases(&self) -> usize {
        self.pressure_cases
    }

    pub const fn compaction_output_published_cases(&self) -> usize {
        self.compaction_output_published
    }

    pub const fn materialization_output_published_cases(&self) -> usize {
        self.materialization_output_published
    }

    pub const fn output_reopened_cases(&self) -> usize {
        self.output_reopened
    }

    pub const fn install_after_publish_cases(&self) -> usize {
        self.install_after_publish
    }

    pub const fn manifest_after_install_cases(&self) -> usize {
        self.manifest_after_install
    }

    pub const fn publish_failed_before_install_cases(&self) -> usize {
        self.publish_failed_before_install
    }

    pub const fn install_failed_after_publish_cases(&self) -> usize {
        self.install_failed_after_publish
    }

    pub const fn manifest_failed_after_install_cases(&self) -> usize {
        self.manifest_failed_after_install
    }

    pub const fn orphan_output_recorded_cases(&self) -> usize {
        self.orphan_output_recorded
    }

    pub const fn no_pruning_observed_cases(&self) -> usize {
        self.no_pruning_observed
    }
}

fn check_cache_compaction(
    script: &[u8],
    outcome: &mut LifecycleTableRewriteContractOutcome,
) -> Result<(), TestkitError> {
    let branch = branch_id(script_byte(script, 0).max(1));
    let mut state = compactable_state(branch, "rewrite-cache-left", "rewrite-cache-right")?;
    let key = physical_key(branch, b"shared")?;
    let request = LifecycleCompactionRequest::new(
        branch,
        BranchCompactionKind::CompactL0,
        format!("rewrite-cache-{}", script_byte(script, 1)),
    )
    .map_err(rewrite_error)?;

    let rewrite = compact_cache_branch(&mut state, &request).map_err(rewrite_error)?;

    ensure(
        rewrite.status() == LifecycleCompactionStatus::Completed,
        "cache compaction did not complete",
    )?;
    ensure(
        rewrite.maintenance_outcome().status() == MaintenanceOutcomeStatus::Completed,
        "cache compaction did not produce completed maintenance outcome",
    )?;
    ensure(
        latest_value(&state, &key)?.as_deref() == Some(b"newer".as_slice()),
        "cache compaction changed visible value",
    )?;
    outcome.cache_compactions += 1;
    Ok(())
}

fn check_durable_compaction(
    script: &[u8],
    outcome: &mut LifecycleTableRewriteContractOutcome,
) -> Result<(), TestkitError> {
    let branch = branch_id(script_byte(script, 2).max(2));
    let mut state = compactable_state(branch, "rewrite-durable-left", "rewrite-durable-right")?;
    let request = LifecycleCompactionRequest::new(
        branch,
        BranchCompactionKind::CompactL0,
        format!("rewrite-durable-{}", script_byte(script, 3)),
    )
    .map_err(rewrite_error)?;

    let rewrite = compact_durable_branch(&mut state, &request).map_err(rewrite_error)?;

    ensure(
        rewrite.status() == LifecycleCompactionStatus::CompletedCheckpointRequired,
        "durable compaction did not report checkpoint debt",
    )?;
    ensure(
        rewrite.checkpoint_required(),
        "durable compaction did not set checkpoint-required flag",
    )?;
    ensure(
        rewrite.maintenance_outcome().checkpoint_required(),
        "durable compaction did not preserve checkpoint debt in maintenance outcome",
    )?;
    outcome.durable_compactions += 1;
    Ok(())
}

fn check_cache_materialization(
    script: &[u8],
    outcome: &mut LifecycleTableRewriteContractOutcome,
) -> Result<(), TestkitError> {
    let parent_seed = script_byte(script, 4).max(3);
    let mut child_seed = script_byte(script, 5).max(4);
    if child_seed == parent_seed {
        child_seed ^= 0x80;
    }
    let parent = branch_id(parent_seed);
    let child = branch_id(child_seed);
    let mut parent_state = BranchLocalState::empty(parent);
    install_l0_table(
        &mut parent_state,
        parent,
        "rewrite-material-parent",
        vec![put_row(parent, b"inherited", 1, 1_000, b"parent")?],
    )?;
    let (mut child_state, _) = parent_state
        .fork_into_empty_child(child)
        .map_err(rewrite_error)?;
    let key = physical_key(child, b"inherited")?;
    let request = LifecycleMaterializationRequest::new(
        child,
        0,
        format!("rewrite-material-{}", script_byte(script, 6)),
    )
    .map_err(rewrite_error)?;

    let materialized =
        materialize_cache_branch(&mut child_state, &request).map_err(rewrite_error)?;

    ensure(
        materialized.status() == LifecycleMaterializationStatus::Completed,
        "cache materialization did not complete",
    )?;
    ensure(
        child_state.inherited_layer_count() == 0,
        "cache materialization left inherited layer visible",
    )?;
    ensure(
        latest_value(&child_state, &key)?.as_deref() == Some(b"parent".as_slice()),
        "cache materialization changed visible value",
    )?;
    outcome.materializations += 1;
    Ok(())
}

fn check_storage_pressure(
    script: &[u8],
    outcome: &mut LifecycleTableRewriteContractOutcome,
) -> Result<(), TestkitError> {
    let branch = branch_id(script_byte(script, 7).max(5));
    let mut state = BranchLocalState::empty(branch);
    for index in 0..4 {
        state
            .append_committed_row(put_row(
                branch,
                &[b'f', index],
                u64::from(index) + 1,
                (u64::from(index) + 1) * 100,
                b"frozen",
            )?)
            .map_err(rewrite_error)?;
        state.rotate_active();
    }
    let maintenance = LifecycleMaintenanceExecutor::new(8)
        .map_err(rewrite_error)?
        .status();

    let pressure = collect_storage_pressure(&state, maintenance);

    ensure(
        pressure.severity() == LifecycleStoragePressureSeverity::BlockMutatingAdmission,
        "storage pressure did not block mutating admission for frozen backlog",
    )?;
    ensure(
        pressure.reason() == LifecycleStoragePressureReason::FrozenBacklog,
        "storage pressure did not report frozen backlog",
    )?;
    ensure(
        pressure.suggested_task().is_some(),
        "storage pressure did not suggest maintenance",
    )?;
    outcome.pressure_cases += 1;
    Ok(())
}

fn check_manifest_backed_durable_compaction(
    script: &[u8],
    outcome: &mut LifecycleTableRewriteContractOutcome,
) -> Result<(), TestkitError> {
    let branch = branch_id(script_byte(script, 8).max(6));
    let backend: &'static RewriteBackend = crate::testkit::leak_static(RewriteBackend::new());
    let mut runtime = open_runtime(branch, backend)?;
    install_l0_table(
        runtime.branch_state_mut(),
        branch,
        "rewrite-manifest-left",
        vec![put_row(branch, b"shared", 1, 1_000, b"older")?],
    )?;
    install_l0_table(
        runtime.branch_state_mut(),
        branch,
        "rewrite-manifest-right",
        vec![put_row(branch, b"shared", 2, 2_000, b"newer")?],
    )?;
    let request = LifecycleCompactionRequest::new(
        branch,
        BranchCompactionKind::CompactL0,
        format!("rewrite-manifest-{}", script_byte(script, 9)),
    )
    .map_err(rewrite_error)?;

    let rewrite = runtime
        .compact_branch_tables(&request)
        .map_err(rewrite_error)?;
    let manifest = runtime
        .services()
        .table_manifest()
        .load_current(branch)
        .map_err(rewrite_error)?
        .ok_or_else(|| TestkitError::new("durable compaction did not publish table manifest"))?;

    ensure(
        rewrite.status() == LifecycleCompactionStatus::CompletedDurable,
        "manifest-backed durable compaction did not complete durably",
    )?;
    ensure(
        manifest
            .levels()
            .iter()
            .map(|level| level.tables().len())
            .sum::<usize>()
            == 1,
        "manifest-backed compaction did not publish exactly one output table",
    )?;
    ensure(
        latest_value(runtime.branch_state(), &physical_key(branch, b"shared")?)?.as_deref()
            == Some(b"newer".as_slice()),
        "manifest-backed compaction changed visible value",
    )?;
    ensure(
        runtime
            .branch_state()
            .capture_read_view()
            .map_err(rewrite_error)?
            .history(
                &physical_key(branch, b"shared")?,
                crate::branch::read::BranchHistoryOptions::all(),
            )
            .map_err(rewrite_error)?
            .len()
            == 2,
        "manifest-backed compaction pruned old versions",
    )?;
    outcome.durable_compactions += 1;
    outcome.compaction_output_published += 1;
    outcome.output_reopened += 1;
    outcome.install_after_publish += 1;
    outcome.manifest_after_install += 1;
    outcome.no_pruning_observed += 1;
    Ok(())
}

fn check_manifest_backed_durable_materialization(
    script: &[u8],
    outcome: &mut LifecycleTableRewriteContractOutcome,
) -> Result<(), TestkitError> {
    let parent_seed = script_byte(script, 10).max(7);
    let mut child_seed = script_byte(script, 11).max(8);
    if child_seed == parent_seed {
        child_seed ^= 0x80;
    }
    let parent = branch_id(parent_seed);
    let child = branch_id(child_seed);
    let backend: &'static RewriteBackend = crate::testkit::leak_static(RewriteBackend::new());
    let mut runtime = open_runtime(child, backend)?;
    let mut parent_state = BranchLocalState::empty(parent);
    install_l0_table(
        &mut parent_state,
        parent,
        "rewrite-manifest-parent",
        vec![put_row(parent, b"inherited", 3, 3_000, b"parent")?],
    )?;
    let (child_state, _) = parent_state
        .fork_into_empty_child(child)
        .map_err(rewrite_error)?;
    *runtime.branch_state_mut() = child_state;
    let request = LifecycleMaterializationRequest::new(
        child,
        0,
        format!("rewrite-manifest-material-{}", script_byte(script, 12)),
    )
    .map_err(rewrite_error)?;

    let rewrite = runtime
        .materialize_inherited_layer(&request)
        .map_err(rewrite_error)?;
    let manifest = runtime
        .services()
        .table_manifest()
        .load_current(child)
        .map_err(rewrite_error)?
        .ok_or_else(|| TestkitError::new("durable materialization did not publish manifest"))?;

    ensure(
        rewrite.status() == LifecycleMaterializationStatus::CompletedDurable,
        "manifest-backed materialization did not complete durably",
    )?;
    ensure(
        manifest.inherited_layers().is_empty(),
        "manifest-backed materialization left inherited layers in manifest",
    )?;
    ensure(
        latest_value(runtime.branch_state(), &physical_key(child, b"inherited")?)?.as_deref()
            == Some(b"parent".as_slice()),
        "manifest-backed materialization changed visible value",
    )?;
    outcome.materialization_output_published += 1;
    outcome.output_reopened += 1;
    outcome.install_after_publish += 1;
    outcome.manifest_after_install += 1;
    Ok(())
}

fn check_publish_failure_before_install(
    script: &[u8],
    outcome: &mut LifecycleTableRewriteContractOutcome,
) -> Result<(), TestkitError> {
    let branch = branch_id(script_byte(script, 13).max(9));
    let backend: &'static RewriteBackend =
        crate::testkit::leak_static(RewriteBackend::fail_table_object_create());
    let mut runtime = open_runtime(branch, backend)?;
    install_l0_table(
        runtime.branch_state_mut(),
        branch,
        "rewrite-fail-left",
        vec![put_row(branch, b"shared", 1, 1_000, b"older")?],
    )?;
    install_l0_table(
        runtime.branch_state_mut(),
        branch,
        "rewrite-fail-right",
        vec![put_row(branch, b"shared", 2, 2_000, b"newer")?],
    )?;
    let key = physical_key(branch, b"shared")?;
    let before = latest_value(runtime.branch_state(), &key)?;
    let request = LifecycleCompactionRequest::new(
        branch,
        BranchCompactionKind::CompactL0,
        format!("rewrite-fail-{}", script_byte(script, 14)),
    )
    .map_err(rewrite_error)?;

    let error = runtime
        .compact_branch_tables(&request)
        .expect_err("publish failure should reject");

    ensure(
        error.code() == "failed_precondition.lifecycle.rewrite_publication",
        "publish failure had wrong error code",
    )?;
    ensure(
        latest_value(runtime.branch_state(), &key)? == before,
        "publish failure changed reads before install",
    )?;
    outcome.publish_failed_before_install += 1;
    Ok(())
}

fn check_install_failure_after_publish(
    script: &[u8],
    outcome: &mut LifecycleTableRewriteContractOutcome,
) -> Result<(), TestkitError> {
    let branch = branch_id(script_byte(script, 17).max(11));
    let backend: &'static RewriteBackend = crate::testkit::leak_static(RewriteBackend::new());
    let mut runtime = open_runtime(branch, backend)?;
    install_l0_table(
        runtime.branch_state_mut(),
        branch,
        "rewrite-install-fail-left",
        vec![put_row(branch, b"shared", 1, 1_000, b"older")?],
    )?;
    install_l0_table(
        runtime.branch_state_mut(),
        branch,
        "rewrite-install-fail-right",
        vec![put_row(branch, b"shared", 2, 2_000, b"newer")?],
    )?;
    let seed = format!("rewrite-install-fail-{}", script_byte(script, 18));
    let request =
        LifecycleCompactionRequest::new(branch, BranchCompactionKind::CompactL0, seed.as_str())
            .map_err(rewrite_error)?;
    // #3469: the output identity is content-complete (builder/compaction
    // config, budget-derived split target, cut hints, policy), so learn the
    // durable path's exact output name from a scout run over identical state
    // on a separate backend rather than re-deriving it from a bare request.
    let scout_backend: &'static RewriteBackend = crate::testkit::leak_static(RewriteBackend::new());
    let mut scout = open_runtime(branch, scout_backend)?;
    *scout.branch_state_mut() = runtime.branch_state().clone();
    let scout_outcome = scout
        .compact_branch_tables(&request)
        .map_err(rewrite_error)?;
    let scout_object = scout_outcome
        .durable_output_objects()
        .first()
        .ok_or_else(|| TestkitError::new("expected scout output object"))?;
    let predicted_identity = scout_object
        .as_str()
        .rsplit('/')
        .next()
        .ok_or_else(|| TestkitError::new("expected scout output object leaf"))?
        .to_owned();
    // Plant a colliding table with the predicted output identity so the
    // lifecycle install fails AFTER output publication, producing a
    // RewritePublicationOrphaned that names the orphan object. L2, not L1: L1
    // is the L0 rewrite's grandparent level, whose cut hints are part of the
    // output identity, so planting there would change the identity itself.
    install_owned_table_at_level(
        runtime.branch_state_mut(),
        branch,
        BranchLevel::new(2),
        predicted_identity.as_str(),
        vec![put_row(branch, b"collision", 99, 99_000, b"collision")?],
    )?;

    let error = runtime
        .compact_branch_tables(&request)
        .expect_err("install collision should reject");

    ensure(
        error.code() == "ambiguous_commit.lifecycle.rewrite_publication_orphan",
        "install failure after publish did not produce an orphan-named lifecycle error",
    )?;
    outcome.install_failed_after_publish += 1;
    outcome.orphan_output_recorded += 1;
    Ok(())
}

fn check_manifest_failure_after_install(
    script: &[u8],
    outcome: &mut LifecycleTableRewriteContractOutcome,
) -> Result<(), TestkitError> {
    let branch = branch_id(script_byte(script, 15).max(10));
    let backend: &'static RewriteBackend =
        crate::testkit::leak_static(RewriteBackend::fail_table_manifest_replace());
    let mut runtime = open_runtime(branch, backend)?;
    install_l0_table(
        runtime.branch_state_mut(),
        branch,
        "rewrite-manifest-fail-left",
        vec![put_row(branch, b"shared", 1, 1_000, b"older")?],
    )?;
    install_l0_table(
        runtime.branch_state_mut(),
        branch,
        "rewrite-manifest-fail-right",
        vec![put_row(branch, b"shared", 2, 2_000, b"newer")?],
    )?;
    let request = LifecycleCompactionRequest::new(
        branch,
        BranchCompactionKind::CompactL0,
        format!("rewrite-manifest-fail-{}", script_byte(script, 16)),
    )
    .map_err(rewrite_error)?;

    let rewrite = runtime
        .compact_branch_tables(&request)
        .map_err(rewrite_error)?;

    ensure(
        rewrite.status() == LifecycleCompactionStatus::CompletedManifestDebt,
        "manifest failure after install did not report manifest debt",
    )?;
    ensure(
        latest_value(runtime.branch_state(), &physical_key(branch, b"shared")?)?.as_deref()
            == Some(b"newer".as_slice()),
        "manifest failure after install did not keep new reads visible",
    )?;
    // Manifest publish failure after install leaves the output INSTALLED in the
    // branch state and catalog; it is forward-progress debt, not an orphan.
    // Orphan counting is owned by check_install_failure_after_publish.
    outcome.manifest_failed_after_install += 1;
    Ok(())
}

fn compactable_state(
    branch: BranchId,
    left_identity: &str,
    right_identity: &str,
) -> Result<BranchLocalState, TestkitError> {
    let mut state = BranchLocalState::empty(branch);
    install_l0_table(
        &mut state,
        branch,
        left_identity,
        vec![put_row(branch, b"shared", 1, 1_000, b"older")?],
    )?;
    install_l0_table(
        &mut state,
        branch,
        right_identity,
        vec![put_row(branch, b"shared", 2, 2_000, b"newer")?],
    )?;
    Ok(state)
}

fn install_l0_table(
    state: &mut BranchLocalState,
    branch: BranchId,
    identity: &str,
    rows: Vec<StorageRow>,
) -> Result<(), TestkitError> {
    install_owned_table_at_level(state, branch, BranchLevel::ZERO, identity, rows)
}

fn install_owned_table_at_level(
    state: &mut BranchLocalState,
    branch: BranchId,
    level: BranchLevel,
    identity: &str,
    rows: Vec<StorageRow>,
) -> Result<(), TestkitError> {
    state
        .install_owned_table_at_level(level, owned_table(branch, level, identity, rows)?)
        .map_err(rewrite_error)?;
    Ok(())
}

fn owned_table(
    branch: BranchId,
    level: BranchLevel,
    identity: &str,
    rows: Vec<StorageRow>,
) -> Result<BranchOwnedTable, TestkitError> {
    let identity = TableIdentity::new(identity).map_err(rewrite_error)?;
    let mut table_rows = rows.into_iter().map(TableRow::new).collect::<Vec<_>>();
    sort_table_rows_by_key(&mut table_rows);
    let artifact = ImmutableTableBuilder::new(TableBuilderConfig::default())
        .map_err(rewrite_error)?
        .build_from_rows(identity.clone(), &table_rows)
        .map_err(rewrite_error)?;
    let reader = ImmutableTableReader::open_bytes(
        identity.clone(),
        artifact.into_bytes(),
        TableReaderConfig::default(),
    )
    .map_err(rewrite_error)?;
    let descriptor = BranchTableDescriptor::new(identity, reader.facts().clone(), level)
        .map_err(rewrite_error)?;
    let extras =
        crate::table::TableSummaryExtras::from_rows(reader.rows()).map_err(rewrite_error)?;
    BranchOwnedTable::new(branch, descriptor, reader, extras).map_err(rewrite_error)
}

fn latest_value(
    state: &BranchLocalState,
    key: &PhysicalKey,
) -> Result<Option<Vec<u8>>, TestkitError> {
    Ok(state
        .capture_read_view()
        .map_err(rewrite_error)?
        .latest(key)
        .map_err(rewrite_error)?
        .map(|row| row.row().value().to_vec()))
}

fn put_row(
    branch: BranchId,
    user_key: &[u8],
    version: u64,
    timestamp: u64,
    value: &[u8],
) -> Result<StorageRow, TestkitError> {
    Ok(StorageRow::put(
        physical_key(branch, user_key)?,
        CommitVersion::new(version),
        Timestamp::from_micros(timestamp),
        Timestamp::EPOCH,
        value.to_vec(),
    ))
}

fn physical_key(branch: BranchId, user_key: &[u8]) -> Result<PhysicalKey, TestkitError> {
    PhysicalKey::new(
        branch,
        "rewrite",
        StorageSpaceId::engine(0x56).map_err(rewrite_error)?,
        user_key.to_vec(),
    )
    .map_err(rewrite_error)
}

fn branch_id(seed: u8) -> BranchId {
    BranchId::from_bytes([seed; BranchId::BYTE_LEN])
}

fn rewrite_error(error: impl std::error::Error) -> TestkitError {
    TestkitError::new(error.to_string())
}

fn open_runtime(
    branch: BranchId,
    backend: &'static RewriteBackend,
) -> Result<LifecycleDurableLocalRuntime<'static, CommitManualTimestampSource>, TestkitError> {
    let request = LifecycleDurableLocalOpenRequest::new(
        StorageOpenPlan::new(
            StorageMode::DurableLocalStandard,
            LifecycleCodecId::identity(),
            RecoveryStrictness::Strict,
            LifecycleConfig::default(),
        )
        .map_err(rewrite_error)?,
        [0x56; 16],
        branch,
        CommitBranchGeneration::new(1).map_err(rewrite_error)?,
        BranchRuntimeConfig::default(),
        CommitRuntimeConfig::default(),
        WalServiceConfig::default()
            .with_append_buffer_bytes(crate::service::DEFAULT_WAL_APPEND_BUFFER_BYTES),
    )
    .map_err(rewrite_error)?;
    let mut shell = LifecycleDurableLocalShell::assemble(
        request,
        backend,
        CommitManualTimestampSource::new(Timestamp::from_micros(10_000)),
    )
    .map_err(rewrite_error)?;
    let recovery_request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).map_err(rewrite_error)?;
    let recovery = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&recovery_request)
        .map_err(rewrite_error)?;
    shell.complete_recovery(&recovery).map_err(rewrite_error)
}

#[derive(Debug)]
struct RewriteBackend {
    objects: Mutex<BTreeMap<ObjectName, Vec<u8>>>,
    fail_table_object_create: bool,
    fail_table_manifest_replace: bool,
    lock_held: Arc<AtomicBool>,
}

impl RewriteBackend {
    fn new() -> Self {
        Self {
            objects: Mutex::new(BTreeMap::new()),
            fail_table_object_create: false,
            fail_table_manifest_replace: false,
            lock_held: Arc::new(AtomicBool::new(false)),
        }
    }

    fn fail_table_object_create() -> Self {
        Self {
            fail_table_object_create: true,
            ..Self::new()
        }
    }

    fn fail_table_manifest_replace() -> Self {
        Self {
            fail_table_manifest_replace: true,
            ..Self::new()
        }
    }
}

impl Backend for RewriteBackend {
    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities::from_slice(DURABLE_LOCAL_MODE_REQUIREMENTS)
    }

    fn read_object(&self, name: &ObjectName) -> BackendResult<Vec<u8>> {
        self.objects
            .lock()
            .expect("objects")
            .get(name)
            .cloned()
            .ok_or_else(|| BackendError::new(BackendErrorKind::NotFound, "object not found"))
    }

    fn read_range(&self, name: &ObjectName, range: BackendRange) -> BackendResult<Vec<u8>> {
        let bytes = self.read_object(name)?;
        let start = usize::try_from(range.offset()).unwrap_or(usize::MAX);
        let end = usize::try_from(range.end_offset().unwrap_or(u64::MAX)).unwrap_or(usize::MAX);
        Ok(bytes[start.min(bytes.len())..end.min(bytes.len())].to_vec())
    }

    fn write_object(&self, name: &ObjectName, bytes: &[u8]) -> BackendResult<BackendMetadata> {
        self.objects
            .lock()
            .expect("objects")
            .insert(name.clone(), bytes.to_vec());
        Ok(BackendMetadata::new(bytes.len() as u64, None))
    }

    fn delete_object(&self, name: &ObjectName) -> crate::backend::DeleteResult {
        let removed = self.objects.lock().expect("objects").remove(name).is_some();
        crate::backend::durable_delete_result(name, removed)
    }

    fn list_prefix(&self, prefix: &ObjectPrefix) -> BackendResult<Vec<ObjectName>> {
        let mut names = self
            .objects
            .lock()
            .expect("objects")
            .keys()
            .filter(|name| name.as_str().starts_with(prefix.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        names.sort();
        Ok(names)
    }

    fn object_metadata(&self, name: &ObjectName) -> BackendResult<BackendMetadata> {
        self.objects
            .lock()
            .expect("objects")
            .get(name)
            .map(|bytes| BackendMetadata::new(bytes.len() as u64, None))
            .ok_or_else(|| BackendError::new(BackendErrorKind::NotFound, "object not found"))
    }

    fn acquire_writer_lock(&self, name: &ObjectName) -> BackendResult<BackendWriterGuard> {
        if self.lock_held.swap(true, Ordering::SeqCst) {
            return Err(BackendError::new(
                BackendErrorKind::Unavailable,
                "writer lock already held",
            ));
        }
        Ok(BackendWriterGuard::new(
            name.clone(),
            RewriteWriterGuard {
                locked: Arc::clone(&self.lock_held),
            },
        ))
    }

    fn append_object(&self, name: &ObjectName, bytes: &[u8]) -> BackendResult<BackendAppend> {
        let mut objects = self.objects.lock().expect("objects");
        let object = objects.entry(name.clone()).or_default();
        let start_offset = object.len() as u64;
        object.extend_from_slice(bytes);
        Ok(BackendAppend::new(
            start_offset,
            bytes.len() as u64,
            BackendMetadata::new(object.len() as u64, None),
        ))
    }

    fn sync_object(&self, _name: &ObjectName) -> crate::backend::BackendResult<()> {
        Ok(())
    }

    fn publish_object(
        &self,
        name: &ObjectName,
        bytes: &[u8],
        mode: PublishMode,
    ) -> PublishResult<PublishOutcome> {
        let is_table_object = mode == PublishMode::Create
            && name.as_str().starts_with("tables/")
            && !name.as_str().ends_with("/manifest");
        let is_table_manifest = mode == PublishMode::Replace
            && name.as_str().starts_with("tables/")
            && name.as_str().ends_with("/manifest");
        if self.fail_table_object_create && is_table_object {
            return Err(PublishError::new(
                name.clone(),
                PublishFailureKind::FailedBeforeVisibility,
                BackendError::new(BackendErrorKind::Unavailable, "table object create failed"),
            ));
        }
        if self.fail_table_manifest_replace && is_table_manifest {
            return Err(PublishError::precondition_failed(
                name,
                "table manifest replace failed",
            ));
        }
        if mode == PublishMode::Create && self.objects.lock().expect("objects").contains_key(name) {
            return Err(PublishError::precondition_failed(name, "object exists"));
        }
        self.write_object(name, bytes).map_err(|error| {
            PublishError::new(
                name.clone(),
                PublishFailureKind::FailedBeforeVisibility,
                error,
            )
        })?;
        Ok(PublishOutcome::new(
            name.clone(),
            BackendMetadata::new(bytes.len() as u64, None),
            PublishDurability::Durable,
        ))
    }
}

struct RewriteWriterGuard {
    locked: Arc<AtomicBool>,
}

impl Drop for RewriteWriterGuard {
    fn drop(&mut self) {
        self.locked.store(false, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Default-lane pin (#3469): the install-after-publish orphan scenario
    /// (scout-predicted output identity, collision planted outside the
    /// grandparent level) must hold and count outside the feature-gated
    /// `lifecycle_properties` target.
    #[test]
    fn install_failure_after_publish_scenario_holds_and_counts() {
        let mut outcome = LifecycleTableRewriteContractOutcome::default();
        check_install_failure_after_publish(b"rewrite-default-lane-pin", &mut outcome)
            .expect("install failure after publish scenario holds");
        assert_eq!(outcome.install_failed_after_publish_cases(), 1);
        assert_eq!(outcome.orphan_output_recorded_cases(), 1);
    }
}
