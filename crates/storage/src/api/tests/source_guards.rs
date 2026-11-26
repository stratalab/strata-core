#[test]
fn source_guard_public_open_options_do_not_default_to_deterministic_inline() {
    let options_source = include_str!("../options.rs");

    assert!(
        !options_source
            .contains("maintenance_scheduling_policy: StorageMaintenanceSchedulingPolicy::DeterministicInline"),
        "public open option constructors must not default to deterministic inline maintenance"
    );
}

#[test]
fn source_guard_api_does_not_export_background_scheduler_internals() {
    let api_mod_source = include_str!("../mod.rs");

    for private_symbol in [
        "BackgroundScheduler",
        "BackgroundSchedulerStats",
        "BackgroundTaskPriority",
        "BackgroundBackpressureError",
    ] {
        assert!(
            !api_mod_source.contains(private_symbol),
            "public API module must not expose old-engine/background scheduler internals: {private_symbol}"
        );
    }
}

#[test]
fn source_guard_background_scheduler_is_local_storage_next_port() {
    let background_source = include_str!("../../lifecycle/background.rs");

    assert!(
        background_source.contains("storage port of `crates/engine/src/background.rs`"),
        "background scheduler must document the old-engine source it ports"
    );
    assert!(
        !background_source.contains("strata_engine")
            && !background_source.contains("strata-engine"),
        "storage background scheduler must be a local port, not a strata-engine dependency"
    );
}

#[test]
fn source_guard_background_priority_maps_lifecycle_work_by_pressure_cost() {
    let runtime_source = super::RUNTIME_SOURCE;
    let priority_mapping = runtime_source
        .split("const fn background_priority_for_task_request")
        .nth(1)
        .expect("background priority mapping function is present")
        .split("fn drain_cache_background_round")
        .next()
        .expect("priority mapping precedes background drain");
    let compact_source = priority_mapping.split_whitespace().collect::<String>();

    assert!(
        compact_source.contains(
            "LifecycleMaintenanceTaskKind::Flush|LifecycleMaintenanceTaskKind::Checkpoint|LifecycleMaintenanceTaskKind::FlushWatermark|LifecycleMaintenanceTaskKind::WalTruncation=>BackgroundTaskPriority::High"
        ),
        "flush/checkpoint/WAL-retention work must wake the background worker at high priority"
    );
    assert!(
        compact_source.contains(
            "LifecycleMaintenanceTaskKind::Compaction|LifecycleMaintenanceTaskKind::Materialization=>BackgroundTaskPriority::Normal"
        ),
        "compaction and materialization must use normal background priority"
    );
    assert!(
        compact_source.contains(
            "LifecycleMaintenanceTaskKind::HealthCollection|LifecycleMaintenanceTaskKind::Retention|LifecycleMaintenanceTaskKind::SnapshotPruning|LifecycleMaintenanceTaskKind::Quarantine|LifecycleMaintenanceTaskKind::Purge|LifecycleMaintenanceTaskKind::Repair|LifecycleMaintenanceTaskKind::CachePreheat=>BackgroundTaskPriority::Low"
        ),
        "health, retention, pruning, quarantine, purge, repair, and preheat work must stay low priority"
    );
}

#[test]
fn source_guard_background_build_runs_before_publish_lock() {
    let runtime_source = super::RUNTIME_SOURCE;
    // The cache drain stays two-phase (volatile mode has no manifest fsync): the unlocked build
    // precedes the single publish lock.
    assert_background_drain_build_before_publish_lock(
        runtime_source
            .split("fn drain_cache_background_round")
            .nth(1)
            .expect("cache background drain function is present")
            .split("fn run_next_durable_maintenance")
            .next()
            .expect("cache background drain precedes durable helpers"),
        "cache",
    );
    // The durable drain is three-phase: a first lock block installs and reserves
    // (`begin_publish_phase`), the manifest fsync runs off-lock (`persist_off_lock`), and a second
    // lock block records and finishes (`finish_publish_phase`).
    assert_durable_background_drain_three_phase_publish(
        runtime_source
            .split("fn drain_durable_background_round")
            .nth(1)
            .expect("durable background drain function is present")
            .split("fn map_generation_guard")
            .next()
            .expect("durable background drain precedes map_generation_guard"),
    );
}

/// The durable background drain must build unlocked, then run two locked phases around an off-lock
/// manifest fsync. Guards that the fsync (`persist_reserved_manifest` / `publish_replace_manifest`)
/// is NOT performed inside either locked phase, and that phase one reserves under the lock and
/// claims the per-branch publish slot with a try-lock.
fn assert_durable_background_drain_three_phase_publish(drain_source: &str) {
    let build_index = drain_source
        .find("let build_result =")
        .expect("durable background drain must build the task");
    let begin_index = drain_source
        .find("let begin = {")
        .expect("durable background drain must enter phase one under the lock");
    let first_lock_index = drain_source[begin_index..]
        .find("let mut runtime = runtime.lock();")
        .map(|index| begin_index + index)
        .expect("durable background drain phase one must lock");
    let persist_index = drain_source
        .find("persist_off_lock()")
        .expect("durable background drain must persist the manifest off-lock");
    let finish_index = drain_source
        .find("let finish = {")
        .expect("durable background drain must enter phase three under the lock");
    let second_lock_index = drain_source[finish_index..]
        .find("let mut runtime = runtime.lock();")
        .map(|index| finish_index + index)
        .expect("durable background drain phase three must lock");

    assert!(
        build_index < begin_index && begin_index < persist_index && persist_index < finish_index,
        "durable background drain must build, then phase one, then off-lock persist, then phase three"
    );
    assert!(
        first_lock_index < persist_index,
        "durable background drain phase one must hold the lock before the off-lock persist"
    );
    assert!(
        persist_index < second_lock_index,
        "durable background drain off-lock persist must run before re-acquiring the lock"
    );
    assert!(
        drain_source[begin_index..persist_index].contains("begin_publish_phase"),
        "durable background drain phase one must call begin_publish_phase"
    );
    assert!(
        drain_source[finish_index..].contains("finish_publish_phase"),
        "durable background drain phase three must call finish_publish_phase"
    );
    // The manifest fsync must not run inside either locked phase: phase one ends at the off-lock
    // persist, phase three runs after it. Neither locked region may call the durable persist.
    for forbidden in ["persist_reserved_manifest", "publish_replace_manifest"] {
        assert!(
            !drain_source[begin_index..persist_index].contains(forbidden),
            "durable background drain phase one must not call {forbidden} (the fsync is off-lock)"
        );
    }

    // Phase one (in the lifecycle layer) reserves the manifest sequence under the lock and claims
    // the per-branch publish slot with a try-lock so the global lock is never blocked on the slot.
    let maintenance_source = include_str!("../../lifecycle/durable/maintenance.rs");
    let begin_phase = maintenance_source
        .split("fn begin_off_lock_publish")
        .nth(1)
        .expect("begin_off_lock_publish is present")
        .split("fn finish_locked_publish")
        .next()
        .expect("begin_off_lock_publish precedes finish_locked_publish");
    assert!(
        begin_phase.contains("reserve_manifest_sequence"),
        "off-lock publish phase one must reserve the manifest sequence under the lock"
    );
    assert!(
        begin_phase.contains("try_acquire_branch_publish_guard"),
        "off-lock publish phase one must claim the per-branch publish slot before releasing the lock"
    );
    assert!(
        !begin_phase.contains("persist_reserved_manifest"),
        "off-lock publish phase one must not fsync the manifest under the lock"
    );
    // The per-branch publish slot must be a try-claim, never a blocking acquire, so the global
    // lock is never blocked on it (the global -> per-branch order cannot deadlock). Only the
    // close waits for a slot, and it does so without the global lock (#3747).
    let bootstrap_source = include_str!("../../lifecycle/durable/bootstrap.rs");
    let try_acquire = bootstrap_source
        .split("fn try_acquire_branch_publish_guard")
        .nth(1)
        .expect("try_acquire_branch_publish_guard is present")
        .split("fn ")
        .next()
        .expect("try_acquire_branch_publish_guard has a body");
    assert!(
        try_acquire.contains("BranchPublishSlots::try_acquire"),
        "the runtime must claim its per-branch publish slot through BranchPublishSlots::try_acquire"
    );
    let slots_source = include_str!("../../lifecycle/durable/publish_slots.rs");
    let slot_claim = slots_source
        .split("fn try_acquire(")
        .nth(1)
        .expect("BranchPublishSlots::try_acquire is present")
        .split("fn ")
        .next()
        .expect("BranchPublishSlots::try_acquire has a body");
    assert!(
        slot_claim.contains(".insert(") && !slot_claim.contains(".wait"),
        "per-branch publish slot must be claimed with a try-claim (set insert), never a wait"
    );
}

#[test]
fn source_guard_background_drain_records_start_and_publish_failures() {
    let runtime_source = super::RUNTIME_SOURCE;
    for (label, source) in [
        (
            "cache",
            runtime_source
                .split("fn drain_cache_background_round")
                .nth(1)
                .expect("cache background drain function is present")
                .split("fn run_next_durable_maintenance")
                .next()
                .expect("cache background drain precedes durable helpers"),
        ),
        (
            "durable",
            runtime_source
                .split("fn drain_durable_background_round")
                .nth(1)
                .expect("durable background drain function is present")
                .split("fn map_generation_guard")
                .next()
                .expect("durable background drain precedes map_generation_guard"),
        ),
    ] {
        assert!(
            source.contains("record_lifecycle_background_task_start_failure"),
            "{label} background drain must record start failures before stopping"
        );
        assert!(
            source.contains("record_lifecycle_background_task_publish_failure"),
            "{label} background drain must record publish failures before stopping"
        );
    }
}

fn assert_background_drain_build_before_publish_lock(drain_source: &str, label: &str) {
    let build_index = drain_source
        .find("let build_result =")
        .unwrap_or_else(|| panic!("{label} background drain must build the task"));
    let publish_index = drain_source
        .find("let publish = {")
        .unwrap_or_else(|| panic!("{label} background drain must enter a publish section"));
    let publish_lock_index = drain_source[publish_index..]
        .find("let mut runtime = runtime.lock();")
        .map_or_else(
            || panic!("{label} background drain must lock only for publish"),
            |index| publish_index + index,
        );

    assert!(
        build_index < publish_index && build_index < publish_lock_index,
        "{label} background drain must finish unlocked build work before acquiring the publish lock"
    );
}

#[test]
fn source_guard_publish_reads_cached_table_summary_not_row_scan() {
    // C1+C2 made the durable publish critical section build the table manifest from
    // per-table cached summaries (TableSummaryExtras) instead of rescanning every
    // row of every table. Guard the two hot-path functions so a regression cannot
    // silently reintroduce the O(resident-rows) scans that made publish O(N^2).
    let manifest_source = include_str!("../../lifecycle/table_manifest.rs");
    let table_ref = manifest_source
        .split("fn table_ref_from_branch_table")
        .nth(1)
        .expect("table_ref_from_branch_table is present")
        .split("fn validate_catalog_entry_matches_table")
        .next()
        .expect("table_ref_from_branch_table precedes validate_catalog_entry_matches_table");
    assert!(
        table_ref.contains(".extras()"),
        "manifest table-ref must read the cached per-table summary"
    );
    for forbidden in ["manifest_table_bounds(", "timestamp_bounds(", ".rows()"] {
        assert!(
            !table_ref.contains(forbidden),
            "manifest table-ref must not rescan rows via {forbidden} (publish stays O(tables))"
        );
    }

    // Flush install moves frozen rows into an L0 table without changing the row
    // set, so it must not trigger a full observed-row-facts rescan (the dominant
    // pre-C1+C2 publish-lock cost).
    let state_source = include_str!("../../branch/state.rs");
    let flush_install = state_source
        .split("fn replace_frozen_with_level_zero_table")
        .nth(1)
        .expect("replace_frozen_with_level_zero_table is present")
        .split("fn matching_frozen_replacement_index")
        .next()
        .expect("replace_frozen_with_level_zero_table precedes matching_frozen_replacement_index");
    assert!(
        !flush_install.contains("refresh_observed_row_facts"),
        "flush install must not rescan observed-row facts (rows are unchanged frozen->L0)"
    );

    // The branch fact refresh must fold cached per-table summaries, not rescan rows
    // on the hot path; observe_rows stays only as the debug oracle / recovery path.
    let refresh = state_source
        .split("fn refresh_observed_row_facts")
        .nth(1)
        .expect("refresh_observed_row_facts is present")
        .split("fn observe_rows_from_summaries")
        .next()
        .expect("refresh_observed_row_facts precedes observe_rows_from_summaries");
    assert!(
        refresh.contains("observe_rows_from_summaries"),
        "refresh_observed_row_facts must fold cached summaries, not rescan rows"
    );
}

#[test]
fn source_guard_lazy_reachable_scans_stream_via_cursor() {
    // BS4.4g: these prod folds can observe a lazy/disk-resident durable table (via fork or the
    // checkpoint boundary), so each must stream rows through a cursor (`for_each_reader_row`) — never
    // a full materialization via `rows()` or the debug-only `materialize_rows_for_oracle` hatch. A
    // plain `rows()` would trip the BS4.4d guard; the hatch would silently materialize every row off a
    // hot path. This locks the BS4.4g cursor conversion against regression.
    fn assert_streams_rows(source: &str, start: &str, end: &str, label: &str) {
        let body = source
            .split(start)
            .nth(1)
            .unwrap_or_else(|| panic!("{label}: {start} is present"))
            .split(end)
            .next()
            .unwrap_or_else(|| panic!("{label}: {start} precedes {end}"));
        for forbidden in [".rows()", "materialize_rows_for_oracle"] {
            assert!(
                !body.contains(forbidden),
                "{label} must stream rows via a cursor, not a full materialization ({forbidden})"
            );
        }
        assert!(
            body.contains("for_each_reader_row"),
            "{label} must fold rows through the `for_each_reader_row` cursor helper"
        );
    }

    // Historical-fork COW: `record_inherited_layer_summary` (and the read-view fold) no longer scan a
    // straddle table on every recompute — they fold a cached `<= fork_version` summary computed once by
    // `compute_straddle_read_view_summary`, which is where the cursor scan now lives. So the summary
    // fold need only avoid full materialization; the streaming guarantee moves to the helper below.
    fn assert_no_full_materialization(source: &str, start: &str, end: &str, label: &str) {
        let body = source
            .split(start)
            .nth(1)
            .unwrap_or_else(|| panic!("{label}: {start} is present"))
            .split(end)
            .next()
            .unwrap_or_else(|| panic!("{label}: {start} precedes {end}"));
        for forbidden in [".rows()", "materialize_rows_for_oracle"] {
            assert!(
                !body.contains(forbidden),
                "{label} must not fully materialize a lazy table ({forbidden})"
            );
        }
    }

    let state = include_str!("../../branch/state.rs");
    assert_streams_rows(
        state,
        "fn observe_own_rows",
        "fn observe_rows",
        "observe_own_rows",
    );
    assert_streams_rows(
        state,
        "fn record_inherited_layer(",
        "fn record_commit_version",
        "record_inherited_layer",
    );
    assert_no_full_materialization(
        state,
        "fn record_inherited_layer_summary",
        "fn branch_reachable_table_identity_exists",
        "record_inherited_layer_summary",
    );

    let read = include_str!("../../branch/read.rs");
    assert_streams_rows(
        read,
        "fn compute_straddle_read_view_summary",
        "struct BranchInheritedLayer",
        "compute_straddle_read_view_summary",
    );

    let checkpoint = include_str!("../../lifecycle/checkpoint.rs");
    assert_streams_rows(
        checkpoint,
        "fn branch_durable_commit_versions_in_interval",
        "fn branch_checkpoint_flush_boundary",
        "branch_durable_commit_versions_in_interval",
    );
    assert_streams_rows(
        checkpoint,
        "fn branch_checkpoint_flush_boundary",
        "fn branch_durable_rows_cover_interval",
        "branch_checkpoint_flush_boundary",
    );
}

#[test]
fn source_guard_background_controller_uses_executor_trait_and_clock() {
    let runtime_source = super::RUNTIME_SOURCE;
    let controller_block = runtime_source
        .split("struct BackgroundRuntimeController")
        .nth(1)
        .expect("background runtime controller is present")
        .split("impl fmt::Debug for BackgroundRuntimeController")
        .next()
        .expect("controller field block precedes debug impl");

    assert!(
        controller_block.contains("Arc<dyn MaintenanceExecutor>"),
        "controller must hold the maintenance executor trait"
    );
    assert!(
        controller_block.contains("Arc<dyn MaintenanceClock>"),
        "controller must hold the maintenance clock trait"
    );
    assert!(
        !controller_block.contains("BackgroundScheduler"),
        "controller must not hold the concrete threaded scheduler"
    );
}

#[test]
fn source_guard_background_drive_logic_uses_maintenance_clock() {
    let runtime_source = super::RUNTIME_SOURCE;
    for (label, source) in [
        (
            "cache",
            runtime_source
                .split("fn drain_cache_background_round")
                .nth(1)
                .expect("cache drain function is present")
                .split("fn run_next_durable_maintenance")
                .next()
                .expect("cache drain precedes durable helper"),
        ),
        (
            "durable",
            runtime_source
                .split("fn drain_durable_background_round")
                .nth(1)
                .expect("durable drain function is present")
                .split("fn map_generation_guard")
                .next()
                .expect("durable drain precedes map_generation_guard"),
        ),
        (
            "pressure-wait",
            runtime_source
                .split("fn background_wait_after_pressure_rejection")
                .nth(1)
                .expect("pressure wait function is present")
                .split("fn enqueue_pressure_maintenance_for_background_wait")
                .next()
                .expect("pressure wait precedes enqueue helper"),
        ),
    ] {
        assert!(
            source.contains("MaintenanceClock") || source.contains("MaintenanceInstant"),
            "{label} drive logic must use the maintenance clock boundary"
        );
        assert!(
            !source.contains("Instant::now"),
            "{label} drive logic must not read wall-clock time directly"
        );
    }
}

#[test]
fn source_guard_pressure_wait_gates_watchdog_reset_on_real_maintenance_progress() {
    let runtime_source = super::RUNTIME_SOURCE;
    let pressure_wait = runtime_source
        .split("fn background_wait_after_pressure_rejection")
        .nth(1)
        .expect("pressure wait function is present")
        .split("fn enqueue_pressure_maintenance_for_background_wait")
        .next()
        .expect("pressure wait precedes enqueue helper");

    // The stall watchdog may reset only on real maintenance progress: the
    // lifecycle maintenance completion count advancing, or the backlog shrinking.
    // It must not reset on the executor-level "progressed" flag, which reports
    // true when the executor is merely idle and would let a dead/stuck executor
    // reset the watchdog forever (an unbounded hang).
    assert!(
        pressure_wait.contains("background_lifecycle_completed_for_current_runtime"),
        "pressure wait must gate progress on the lifecycle maintenance completion count"
    );
    assert!(
        pressure_wait.contains("record_lifecycle_write_admission_wait_progress_reset"),
        "pressure wait must record when the stall watchdog is reset on progress"
    );
}

#[test]
fn source_guard_close_with_options_documents_background_panic_retry_contract() {
    let runtime_source = super::RUNTIME_SOURCE;
    let close_doc = runtime_source
        .split("pub fn close_with_options")
        .next()
        .expect("runtime source includes close_with_options");

    assert!(
        close_doc.contains("background worker panic")
            && close_doc.contains("leaves the runtime open")
            && close_doc.contains("retry close"),
        "close_with_options docs must describe retry after shutdown-time background panic"
    );
}

#[test]
fn source_guard_maintenance_executor_trait_hides_threading_types() {
    let background_source = include_str!("../../lifecycle/background.rs");
    let trait_source = background_source
        .split("pub(crate) trait MaintenanceExecutor")
        .nth(1)
        .expect("maintenance executor trait is present")
        .split("struct TaskEnvelope")
        .next()
        .expect("trait precedes task envelope");

    for forbidden in [
        "std::thread",
        "JoinHandle",
        "parking_lot",
        "Condvar",
        "Instant",
    ] {
        assert!(
            !trait_source.contains(forbidden),
            "MaintenanceExecutor trait signature must not expose {forbidden}"
        );
    }
}

#[test]
fn source_guard_deterministic_inline_maps_to_background_drive_path() {
    let runtime_source = super::RUNTIME_SOURCE;
    let mapping = runtime_source
        .split("const fn map_maintenance_scheduling_policy")
        .nth(1)
        .expect("maintenance policy mapping is present")
        .split("const fn background_executor_mode")
        .next()
        .expect("mapping precedes executor mode helper");
    let compact_mapping = mapping.split_whitespace().collect::<String>();

    assert!(
        compact_mapping.contains(
            "StorageMaintenanceSchedulingPolicy::DeterministicInline=>{LifecycleMaintenanceSchedulingPolicy::Background}"
        ),
        "API deterministic-inline policy must run the Background lifecycle drive path"
    );
}

#[test]
fn source_guard_lifecycle_inline_paths_are_marked_transitional() {
    for (label, source) in [
        ("cache", include_str!("../../lifecycle/cache.rs")),
        (
            "durable",
            include_str!("../../lifecycle/durable/maintenance.rs"),
        ),
    ] {
        for function_name in [
            "fn run_inline_post_commit_maintenance",
            "fn run_inline_admission_maintenance",
        ] {
            let function_block = source
                .split(function_name)
                .nth(1)
                .unwrap_or_else(|| panic!("{label} {function_name} is present"))
                .split("fn ")
                .next()
                .expect("function block precedes next function");
            assert!(
                function_block.contains("Simulation-boundary deletion condition"),
                "{label} {function_name} must be marked as transitional while it exists"
            );
        }
    }
}

#[test]
fn lifecycle_simulation_boundary_source_guards_are_registered() {
    let api_tests_source = include_str!("source_guards.rs");
    for guard_name in [
        "source_guard_background_controller_uses_executor_trait_and_clock",
        "source_guard_background_drive_logic_uses_maintenance_clock",
        "source_guard_maintenance_executor_trait_hides_threading_types",
        "source_guard_deterministic_inline_maps_to_background_drive_path",
        "source_guard_lifecycle_inline_paths_are_marked_transitional",
    ] {
        assert!(
            api_tests_source.contains(guard_name),
            "lifecycle simulation boundary guard {guard_name} must stay registered"
        );
    }
}

#[test]
fn source_guard_rewrite_publication_installs_lazy_disk_resident_output() {
    let rewrite_source = include_str!("../../lifecycle/rewrite_publication.rs");
    let publish_artifact_source = rewrite_source
        .split("fn publish_rewrite_artifact")
        .nth(1)
        .expect("rewrite artifact publication function is present")
        .split("fn require_optional_rewrite_generated_budget")
        .next()
        .expect("publish artifact precedes budget helpers");
    let publish_or_load_source = rewrite_source
        .split("fn publish_or_load_rewrite_output")
        .nth(1)
        .expect("rewrite publish-or-load helper is present")
        .split("fn published_object_names")
        .next()
        .expect("publish-or-load helper precedes object-name helper");

    assert!(
        publish_artifact_source.contains("into_parts()")
            && !publish_artifact_source.contains("into_parts_with_rows"),
        "BS4.4l: durable rewrite publication must drop build-time rows, not carry them forward"
    );
    assert!(
        publish_artifact_source.contains(".open_reader(")
            && !publish_artifact_source.contains("open_reader_from_validated_rows"),
        "BS4.4l: durable rewrite publication must install a lazy, disk-resident reader over the published object"
    );
    assert!(
        publish_artifact_source.contains("deny_runtime_materialization"),
        "BS4.4l: durable rewrite output reader must guard against accidental full materialization"
    );
    assert!(
        publish_or_load_source.contains("publish_create_prevalidated"),
        "durable rewrite publication must publish with build-time table facts"
    );
    assert!(
        !publish_or_load_source.contains("publish_create(")
            && !publish_or_load_source.contains("decode_immutable_table"),
        "durable rewrite publication must not re-decode table facts"
    );
}

#[test]
fn source_guard_merge_cost_cache_compaction_installs_validated_row_readers() {
    let compaction_source = include_str!("../../branch/state/compaction.rs");
    let output_tables_source = compaction_source
        .split("fn compaction_output_tables")
        .nth(1)
        .expect("cache compaction output installation function is present")
        .split("fn record_lifecycle_compaction_outcome")
        .next()
        .expect("output installation precedes lifecycle outcome helper");

    assert!(
        output_tables_source.contains("into_parts_with_rows"),
        "cache compaction output installation must carry build-time rows forward"
    );
    assert!(
        output_tables_source.contains("ImmutableTableReader::from_validated_rows"),
        "cache compaction output installation must use validated-row reader handoff"
    );
    assert!(
        !output_tables_source.contains("ImmutableTableReader::open_bytes"),
        "cache compaction output installation must not reopen/reparse output bytes"
    );
}

#[test]
fn source_guard_merge_cost_table_build_facts_come_from_streaming_metadata() {
    let builder_source = include_str!("../../table/builder.rs");
    let artifact_source = builder_source
        .split("fn build_table_artifact_from_streaming_output")
        .nth(1)
        .expect("streaming artifact build helper is present")
        .split("fn validate_builder_row_shape")
        .next()
        .expect("streaming artifact helper precedes validation helpers");

    assert!(
        artifact_source.contains("TableRuntimeFacts::new"),
        "table artifact facts must be constructed from streaming metadata"
    );
    assert!(
        artifact_source.contains("record_table_build_facts_from_streaming_metadata"),
        "table artifact build must record the streaming-metadata facts path"
    );
    assert!(
        !builder_source.contains("decode_immutable_table")
            && !builder_source.contains("table_facts_from_decoded"),
        "table builder must not decode freshly-built table bytes to recover facts"
    );
}

#[test]
fn merge_cost_source_guards_are_registered() {
    let api_tests_source = include_str!("source_guards.rs");
    for guard_name in [
        "source_guard_merge_cost_rewrite_publication_uses_prevalidated_row_handoff",
        "source_guard_merge_cost_cache_compaction_installs_validated_row_readers",
        "source_guard_merge_cost_table_build_facts_come_from_streaming_metadata",
    ] {
        assert!(
            api_tests_source.contains(guard_name),
            "merge-cost source guard {guard_name} must stay registered"
        );
    }
}
