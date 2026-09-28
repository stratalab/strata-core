//! Source guards for the lifecycle boundary.

#![deny(unsafe_code)]

mod common;

use std::fs;
use std::path::{Path, PathBuf};

#[test]
fn lifecycle_source_does_not_import_engine_product_or_raw_io() {
    let root = common::crate_root();
    let background_scheduler = root.join("src/lifecycle/background.rs");

    for file in lifecycle_source_files(&root) {
        let text = fs::read_to_string(&file).expect("read lifecycle source");
        let guarded_text = if file == background_scheduler {
            text.replace("crates/engine/src/background.rs", "")
        } else {
            text.clone()
        };
        assert!(
            !contains_forbidden_import_or_io_text(&guarded_text),
            "{} uses forbidden lifecycle dependency or grouped IO surface",
            file.strip_prefix(&root).unwrap_or(&file).display()
        );
        for (line_number, line) in text.lines().enumerate() {
            if file == background_scheduler && line.contains("crates/engine/src/background.rs") {
                continue;
            }
            assert!(
                !contains_forbidden_import_or_io(&strip_allowed_dark_launch_env_reads(line)),
                "{}:{} uses forbidden lifecycle dependency or IO surface: {line}",
                file.strip_prefix(&root).unwrap_or(&file).display(),
                line_number + 1
            );
            assert!(
                !contains_forbidden_product_vocabulary(line),
                "{}:{} uses forbidden product lifecycle vocabulary: {line}",
                file.strip_prefix(&root).unwrap_or(&file).display(),
                line_number + 1
            );
        }
    }
}

#[test]
fn recovery_exclusivity_token_is_minted_only_in_bootstrap() {
    let root = common::crate_root();
    let allowed_minting_file = root.join("src/lifecycle/durable/bootstrap.rs");
    let definition_file = root.join("src/lifecycle/branch_lifecycle.rs");

    for file in lifecycle_source_files(&root)
        .into_iter()
        .chain(lifecycle_testkit_files(&root).into_iter())
    {
        if file == allowed_minting_file || file == definition_file {
            continue;
        }
        let text = fs::read_to_string(&file).expect("read lifecycle source");
        for (line_number, line) in text.lines().enumerate() {
            assert!(
                !line.contains("RecoveryExclusivityToken::new("),
                "{}:{} mints RecoveryExclusivityToken outside the bootstrap module; \
                 only `lifecycle/durable/bootstrap.rs` may construct this token: {line}",
                file.strip_prefix(&root).unwrap_or(&file).display(),
                line_number + 1,
            );
        }
    }
}

#[test]
fn lifecycle_implementation_avoids_architecture_labels() {
    let root = common::crate_root();
    for file in lifecycle_source_files(&root)
        .into_iter()
        .chain(lifecycle_testkit_files(&root).into_iter())
        .chain(lifecycle_unit_test_files(&root).into_iter())
        .chain(lifecycle_integration_test_files(&root).into_iter())
    {
        let text = fs::read_to_string(&file).expect("read lifecycle implementation source");
        for (line_number, line) in text.lines().enumerate() {
            assert!(
                !common::source_guard_helpers::contains_milestone_label(line),
                "{}:{} contains architecture label: {line}",
                file.strip_prefix(&root).unwrap_or(&file).display(),
                line_number + 1
            );
        }
    }
}

#[test]
fn lifecycle_maintenance_tests_avoid_sleeps_and_thread_spawns() {
    let root = common::crate_root();
    for file in [
        root.join("src/lifecycle/tests/maintenance.rs"),
        root.join("src/testkit/lifecycle/maintenance.rs"),
        root.join("tests/lifecycle_maintenance.rs"),
    ] {
        let text = fs::read_to_string(&file).expect("read lifecycle maintenance test source");
        for (line_number, line) in text.lines().enumerate() {
            assert!(
                !contains_sleep_or_thread_spawn(line),
                "{}:{} uses nondeterministic maintenance test primitive: {line}",
                file.strip_prefix(&root).unwrap_or(&file).display(),
                line_number + 1
            );
        }
    }
}

#[test]
fn lifecycle_background_scheduler_records_engine_port_safety_hooks() {
    let root = common::crate_root();
    let path = root.join("src/lifecycle/background.rs");
    let text = fs::read_to_string(&path).expect("read lifecycle background scheduler source");

    for required in [
        "crates/engine/src/background.rs",
        "Authoritative shutdown check under lock",
        "catch_unwind",
        "ActiveTaskGuard",
        "fetch_sub(1, AtomicOrdering::AcqRel)",
        "work_ready.notify_all()",
        "drain_cond.notify_all()",
    ] {
        assert!(
            text.contains(required),
            "background scheduler port is missing required safety hook: {required}"
        );
    }
}

#[test]
fn lifecycle_compaction_io_budget_tests_explicitly_configure_byte_budget() {
    let root = common::crate_root();
    let checks = [
        (
            root.join("src/lifecycle/tests/cache.rs"),
            "cache_explicit_compaction_drain_obeys_io_budget_policy",
        ),
        (
            root.join("src/lifecycle/tests/compaction/publication_plan.rs"),
            "durable_explicit_compaction_drain_obeys_io_budget_policy_before_publish",
        ),
        (
            root.join("src/lifecycle/tests/compaction/mod.rs"),
            "metadata_promotion_compaction_records_avoided_rewrite_bytes",
        ),
        (
            root.join("src/lifecycle/tests/compaction/mod.rs"),
            "constrained_compaction_io_budget_defers_without_mutating_sources",
        ),
        (
            root.join("src/lifecycle/tests/compaction/mod.rs"),
            "compaction_resource_policy_is_deterministic_for_repeated_budget_checks",
        ),
        (
            root.join("src/lifecycle/tests/compaction/mod.rs"),
            "generated_compaction_io_budget_sweep_defers_rewrites_by_estimated_size",
        ),
        (
            root.join("src/lifecycle/tests/compaction/mod.rs"),
            "generated_metadata_promotion_and_rewrite_candidates_follow_io_budget_policy",
        ),
    ];

    for (file, function_name) in checks {
        let text = fs::read_to_string(&file).expect("read lifecycle compaction test source");
        let function_source = rust_function_source(&text, function_name)
            .unwrap_or_else(|| panic!("missing test function {function_name}"));
        assert!(
            function_source.contains("LifecycleCompactionIoPolicy::per_task_byte_budget("),
            "{}::{function_name} must explicitly opt into a byte budget; \
             LifecycleCompactionIoPolicy defaults to Unlimited",
            file.strip_prefix(&root).unwrap_or(&file).display()
        );
    }
}

#[test]
fn lifecycle_stays_crate_private() {
    let root = common::crate_root();
    let lib = fs::read_to_string(root.join("src/lib.rs")).expect("read lib.rs");
    assert!(lib.contains("mod lifecycle;"));
    assert!(!lib.contains("pub mod lifecycle;"));

    for file in lifecycle_source_files(&root) {
        let text = fs::read_to_string(&file).expect("read lifecycle source");
        for (line_number, line) in text.lines().enumerate() {
            assert!(
                !is_public_surface_leak(line),
                "{}:{} exposes lifecycle API publicly: {line}",
                file.strip_prefix(&root).unwrap_or(&file).display(),
                line_number + 1
            );
        }
    }
}

#[test]
fn lower_layers_do_not_import_lifecycle_upward() {
    let root = common::crate_root();
    for source_dir in [
        "src/backend",
        "src/layout",
        "src/object",
        "src/format",
        "src/service",
        "src/table",
        "src/branch",
        "src/commit",
        "src/row",
    ] {
        let mut files = Vec::new();
        collect_rs_files(&root.join(source_dir), &mut files);
        for file in files {
            let text = fs::read_to_string(&file).expect("read lower layer source");
            assert!(
                !imports_lifecycle_text(&text),
                "{} imports upward into lifecycle",
                file.strip_prefix(&root).unwrap_or(&file).display()
            );
            for (line_number, line) in text.lines().enumerate() {
                assert!(
                    !imports_lifecycle(line),
                    "{}:{} imports upward into lifecycle: {line}",
                    file.strip_prefix(&root).unwrap_or(&file).display(),
                    line_number + 1
                );
            }
        }
    }
}

#[test]
fn lifecycle_capability_validator_stays_preflight_only() {
    let root = common::crate_root();
    let path = root.join("src/lifecycle/capability.rs");
    let text = fs::read_to_string(&path).expect("read lifecycle capability source");
    assert!(
        !contains_forbidden_capability_preflight_dependency_text(&text),
        "lifecycle capability validator imports service/runtime assembly dependencies"
    );

    for (line_number, line) in text.lines().enumerate() {
        assert!(
            !contains_forbidden_capability_preflight_dependency(line),
            "src/lifecycle/capability.rs:{} imports or calls forbidden preflight dependency: {line}",
            line_number + 1
        );
    }
}

#[test]
fn lifecycle_cache_runtime_stays_cache_only() {
    let root = common::crate_root();
    let path = root.join("src/lifecycle/cache.rs");
    let text = fs::read_to_string(&path).expect("read lifecycle cache source");
    assert!(
        !contains_forbidden_cache_runtime_dependency_text(&text),
        "lifecycle cache runtime imports durable service or object-layout dependencies"
    );

    for (line_number, line) in text.lines().enumerate() {
        assert!(
            !contains_forbidden_cache_runtime_dependency(line),
            "src/lifecycle/cache.rs:{} imports or calls forbidden cache runtime dependency: {line}",
            line_number + 1
        );
    }
}

#[test]
fn lifecycle_branch_lifecycle_source_stays_storage_internal() {
    let root = common::crate_root();
    for relative in [
        "src/lifecycle/branch_lifecycle.rs",
        "src/lifecycle/tests/branch_lifecycle/catalog.rs",
        "src/lifecycle/tests/branch_lifecycle/clear_delete.rs",
        "src/lifecycle/tests/branch_lifecycle/fork.rs",
        "src/lifecycle/tests/branch_lifecycle/isolation.rs",
        "src/lifecycle/tests/branch_lifecycle/mod.rs",
        "src/testkit/lifecycle/branch_lifecycle.rs",
        "tests/lifecycle_branch_lifecycle.rs",
    ] {
        let path = root.join(relative);
        let text = fs::read_to_string(&path).expect("read branch lifecycle source");
        for (line_number, line) in text.lines().enumerate() {
            assert!(
                !contains_forbidden_branch_lifecycle_dependency(line),
                "{}:{} violates branch lifecycle source guard: {line}",
                relative,
                line_number + 1
            );
        }
    }
}

#[test]
fn cache_compaction_does_not_call_table_object_service() {
    let root = common::crate_root();
    for relative in ["src/lifecycle/cache.rs", "src/lifecycle/compaction.rs"] {
        let path = root.join(relative);
        let text = fs::read_to_string(&path).expect("read cache table rewrite source");
        for (line_number, line) in text.lines().enumerate() {
            assert!(
                !contains_cache_table_object_service_dependency(line),
                "{}:{} calls table object service from cache table rewrite path: {line}",
                relative,
                line_number + 1
            );
        }
    }
}

#[test]
fn cache_rewrite_path_does_not_import_table_object_publication() {
    let root = common::crate_root();
    for relative in ["src/lifecycle/cache.rs", "src/lifecycle/compaction.rs"] {
        let path = root.join(relative);
        let text = fs::read_to_string(&path).expect("read cache rewrite source");
        for (line_number, line) in text.lines().enumerate() {
            assert!(
                !contains_cache_table_object_service_dependency(line),
                "{}:{} imports table object publication from cache rewrite path: {line}",
                relative,
                line_number + 1
            );
        }
    }
}

#[test]
fn stateless_compaction_task_conversion_does_not_select_nonzero_table_zero() {
    let root = common::crate_root();
    let path = root.join("src/lifecycle/compaction.rs");
    let text = fs::read_to_string(&path).expect("read lifecycle compaction source");
    let body = text
        .split("pub(crate) fn compaction_request_from_maintenance_task")
        .nth(1)
        .expect("find stateless compaction task converter")
        .split("pub(crate) fn current_compaction_request_from_maintenance_task")
        .next()
        .expect("find current compaction task converter");

    assert!(
        !body.contains("BranchCompactionKind::CompactLevel"),
        "stateless compaction task conversion must not build nonzero table requests"
    );
    assert!(
        !body.contains("table_index: 0"),
        "stateless compaction task conversion must not choose a nonzero input table"
    );
}

#[test]
fn compaction_shape_semantic_decisions_are_recorded() {
    let root = common::crate_root();
    let repo = root
        .parent()
        .and_then(Path::parent)
        .expect("crate has repository root");
    let path = repo.join("docs/architecture/storage/l8-lifecycle-recovery-maintenance.md");
    let text = fs::read_to_string(&path).expect("read lifecycle architecture doc");
    let normalized_text = text.split_whitespace().collect::<Vec<_>>().join(" ");

    for required in [
        "segmented level-target calculation",
        "Empty layouts start L1 at 1 MiB",
        "multiplies the previous target by 10",
        "clamp the base between 1 MiB and 256 MiB",
        "Table compaction output target bytes are separate from level pressure target bytes",
        "compact pointer for each nonzero level",
        "first table whose max physical key is greater than the pointer",
        "wraps to the first table",
        "Ordinary write-pressure scoring skips the final configured level",
        "bottommost consolidation is only selected by an explicit level-scoped maintenance task",
        "lower branch and table compaction layers",
        "deferred split-budget fact",
    ] {
        assert!(
            normalized_text.contains(required),
            "missing compaction shape semantic decision text: {required}"
        );
    }
}

#[test]
fn compaction_resource_semantic_decisions_and_benchmark_fields_are_recorded() {
    let root = common::crate_root();
    let repo = root
        .parent()
        .and_then(Path::parent)
        .expect("crate has repository root");
    let doc_path = repo.join("docs/architecture/storage/l8-lifecycle-recovery-maintenance.md");
    let doc = fs::read_to_string(&doc_path).expect("read lifecycle architecture doc");
    let normalized_doc = doc.split_whitespace().collect::<Vec<_>>().join(" ");
    let runner_path = repo.join("benchmarks/src/bin/storage_next_l9_scale.rs");
    let runner = fs::read_to_string(&runner_path).expect("read storage scale runner");

    for required in [
        "Lifecycle compaction records table bytes read for rewrite operations",
        "Metadata-only promotion reports the source bytes it avoided rewriting",
        "Queued and explicit fixed-point compaction drains share this policy",
        "Flush pressure preempts queued compaction for the same branch",
        "Memory release remains measure-first",
    ] {
        assert!(
            normalized_doc.contains(required),
            "missing compaction resource semantic decision text: {required}"
        );
    }

    for required in [
        "lifecycle_compaction_input_bytes",
        "lifecycle_compaction_output_bytes",
        "lifecycle_compaction_metadata_bytes_avoided",
        "lifecycle_compaction_rewrite_bytes_per_row",
        "lifecycle_compaction_io_budget_consumed_bytes",
        "lifecycle_compaction_io_budget_deferrals",
        "lifecycle_compaction_flush_preemptions",
        "\"lifecycle_compaction\"",
        "lifecycle_snapshot_floor_advancements",
        "lifecycle_snapshot_floor_implicit_rejections",
        "lifecycle_snapshot_pruning_with_proof",
        "\"lifecycle_snapshot_pruning\"",
    ] {
        assert!(
            runner.contains(required),
            "storage scale runner does not emit compaction resource metric: {required}"
        );
    }
}

#[test]
fn background_closed_loop_ci_guard_is_named_and_gated() {
    let root = common::crate_root();
    let source = ["src/api/tests/mod.rs", "src/api/tests/background_scale.rs"]
        .iter()
        .map(|relative| fs::read_to_string(root.join(relative)).expect("read API test module"))
        .collect::<String>();

    for required in [
        "fn lifecycle_background_closed_loop_scaled_cache_converges_without_public_drain",
        "fn lifecycle_background_closed_loop_scaled_durable_bounds_wal_without_public_drain",
        "scaled_closed_loop_test_profile()",
        "with_storage_budget_for_test",
        "assert_scaled_compaction_amplification_below_gate",
        "lifecycle_write_admission_wait_timeouts()",
        "max_retained_segments <= 16",
        "max_retained_bytes <= 128 * 1024",
        "report.source_layout().owned_l0_tables() <= 3",
        "max_clearable_nonzero_fanout <= 3",
        "assert_background_closed_loop_reads",
        ".read_point(&PointReadRequest::new(",
        ".scan_prefix(&PrefixScanReadRequest::new(",
    ] {
        assert!(
            source.contains(required),
            "background closed-loop CI guard is missing required invariant: {required}"
        );
    }
}

#[test]
fn snapshot_pruning_ownership_semantic_decisions_are_recorded() {
    let root = common::crate_root();
    let repo = root
        .parent()
        .and_then(Path::parent)
        .expect("crate has repository root");
    let path = repo.join("docs/architecture/storage/l8-lifecycle-recovery-maintenance.md");
    let text = fs::read_to_string(&path).expect("read lifecycle architecture doc");
    let normalized_text = text.split_whitespace().collect::<Vec<_>>().join(" ");

    for required in [
        "Snapshot object pruning is separate from source-shape maintenance",
        "does not implement an implicit `set_snapshot_floor` or `gc_safe_point`",
        "Snapshot-floor advancement is owned by the caller-supplied retention proof",
        // Space-reclamation contract (DUR-019, 1.2.6): the explicit verbs
        // plus exactly two proof-driven modes, each with its safety rule.
        "Pruning callers are explicit retention and snapshot-pruning maintenance requests, plus the two proof-driven modes",
        "current manifest snapshot id plus snapshot watermark",
        "`SnapshotPruneMode::Superseded` is chained after every completed checkpoint",
        "it deletes only ids below the live one, never one at or above it",
        "`SnapshotPruneMode::ReconcileToAttested` is queued at reopen",
        "Flush drains, compaction chains, materialization, and benchmark source-shape drains still never advance the floor or prune snapshots",
        "Benchmarks must report source-shape maintenance separately from pruning",
    ] {
        assert!(
            normalized_text.contains(required),
            "missing snapshot pruning ownership decision text: {required}"
        );
    }
}

#[test]
fn automatic_storage_pressure_does_not_suggest_pruning_tasks() {
    let root = common::crate_root();
    let path = root.join("src/lifecycle/compaction.rs");
    let text = fs::read_to_string(&path).expect("read lifecycle compaction source");
    let pressure_body = text
        .split("pub(crate) fn collect_storage_pressure")
        .nth(1)
        .expect("find storage pressure collector")
        .split("pub(crate) fn table_rewrite_task_request_for_branch")
        .next()
        .expect("find storage pressure region");

    for forbidden in [
        "MaintenanceTaskRequest::snapshot_pruning",
        "MaintenanceTaskRequest::retention",
    ] {
        assert!(
            !pressure_body.contains(forbidden),
            "automatic storage pressure must not suggest pruning task: {forbidden}"
        );
    }
}

#[test]
fn recovery_preserves_snapshot_floor_ownership_boundary() {
    let root = common::crate_root();
    for relative in [
        "src/lifecycle/recovery.rs",
        "src/lifecycle/durable/bootstrap.rs",
    ] {
        let path = root.join(relative);
        let text = fs::read_to_string(&path).expect("read lifecycle recovery source");

        for forbidden in [
            "prune_snapshots_with_proof",
            "reject_implicit_snapshot_floor_advancement",
            "record_lifecycle_snapshot_floor_implicit_rejection",
            "record_lifecycle_snapshot_pruning_with_proof",
            "LIFECYCLE_SNAPSHOT_FLOOR_ADVANCEMENTS",
        ] {
            assert!(
                !text.contains(forbidden),
                "{relative} must not advance snapshot floors or prune snapshots during recovery: {forbidden}"
            );
        }
    }
}

#[test]
fn maintenance_coverage_semantic_decisions_are_recorded() {
    let root = common::crate_root();
    let repo = root
        .parent()
        .and_then(Path::parent)
        .expect("crate has repository root");
    let path = repo.join("docs/architecture/storage/l8-lifecycle-recovery-maintenance.md");
    let text = fs::read_to_string(&path).expect("read lifecycle architecture doc");
    let normalized_text = text.split_whitespace().collect::<Vec<_>>().join(" ");

    for required in [
        "Maintenance Coverage Trigger Model",
        "after a successful mutating commit",
        "deterministic live branch list",
        "quiet branches with frozen-table, L0, nonzero-level, or inherited-layer backlog",
        "committing branch is inspected for scan accounting but is not enqueued",
        "flush-before-compaction ordering",
        "Cross-branch coverage work is queued deterministically and is not driven inline",
        "Coverage idle rounds are consecutive coverage passes",
        "at most five idle rounds",
        "close-required drain or closing state owns the lifecycle",
    ] {
        assert!(
            normalized_text.contains(required),
            "missing maintenance coverage semantic decision text: {required}"
        );
    }
}

#[test]
fn lifecycle_flush_source_does_not_manage_watermarks_or_log_retention() {
    let root = common::crate_root();
    let path = root.join("src/lifecycle/flush.rs");
    let text = fs::read_to_string(&path).expect("read lifecycle flush source");

    for (line_number, line) in text.lines().enumerate() {
        assert!(
            !contains_forbidden_flush_dependency(line),
            "src/lifecycle/flush.rs:{} calls forbidden flush dependency: {line}",
            line_number + 1
        );
    }
}

#[test]
fn lifecycle_durable_runtime_stays_bootstrap_only() {
    let root = common::crate_root();
    let path = root.join("src/lifecycle/durable.rs");
    let text = fs::read_to_string(&path).expect("read lifecycle durable source");

    for (line_number, line) in text.lines().enumerate() {
        assert!(
            !contains_forbidden_durable_runtime_dependency(line),
            "src/lifecycle/durable.rs:{} calls forbidden durable bootstrap dependency: {line}",
            line_number + 1
        );
    }
}

#[test]
fn lifecycle_bootstrap_runtime_does_not_perform_durable_assembly() {
    let root = common::crate_root();
    let path = root.join("src/lifecycle/durable/bootstrap.rs");
    let text = fs::read_to_string(&path).expect("read lifecycle durable bootstrap source");

    for (line_number, line) in text.lines().enumerate() {
        assert!(
            !contains_forbidden_bootstrap_assembly_dependency(line),
            "src/lifecycle/durable/bootstrap.rs:{} calls forbidden durable assembly dependency: {line}",
            line_number + 1
        );
    }
}

#[test]
fn lifecycle_recovery_runtime_does_not_call_commit_replay_or_product_hooks() {
    let root = common::crate_root();
    let path = root.join("src/lifecycle/recovery.rs");
    let text = fs::read_to_string(&path).expect("read lifecycle recovery source");

    for (line_number, line) in text.lines().enumerate() {
        assert!(
            !contains_forbidden_recovery_runtime_dependency(line),
            "src/lifecycle/recovery.rs:{} calls forbidden recovery dependency: {line}",
            line_number + 1
        );
    }
}

#[test]
fn table_manifest_recovery_does_not_list_table_prefix_for_reachability() {
    let root = common::crate_root();
    let path = root.join("src/lifecycle/table_manifest.rs");
    let text = fs::read_to_string(&path).expect("read lifecycle table manifest source");

    for (line_number, line) in text.lines().enumerate() {
        assert!(
            !contains_forbidden_table_manifest_recovery_dependency(line),
            "src/lifecycle/table_manifest.rs:{} calls forbidden table-manifest recovery dependency: {line}",
            line_number + 1
        );
    }
}

#[test]
fn table_manifest_publication_does_not_touch_wal_truncation() {
    let root = common::crate_root();
    for relative in [
        "src/lifecycle/table_manifest.rs",
        "src/lifecycle/durable/maintenance.rs",
    ] {
        let path = root.join(relative);
        let text = fs::read_to_string(&path).expect("read table manifest publication source");
        for (line_number, line) in text.lines().enumerate() {
            if !line.to_ascii_lowercase().contains("table_manifest") {
                continue;
            }
            assert!(
                !contains_forbidden_table_manifest_publication_dependency(line),
                "{}:{} calls forbidden table-manifest publication dependency: {line}",
                relative,
                line_number + 1
            );
        }
    }
}

#[test]
fn cache_mode_does_not_import_table_manifest_service() {
    let root = common::crate_root();
    let path = root.join("src/lifecycle/cache.rs");
    let text = fs::read_to_string(&path).expect("read lifecycle cache source");

    for (line_number, line) in text.lines().enumerate() {
        assert!(
            !line.to_ascii_lowercase().contains("tablemanifestservice"),
            "src/lifecycle/cache.rs:{} imports durable table-manifest service: {line}",
            line_number + 1
        );
    }
}

#[test]
fn table_manifest_watermark_does_not_import_raw_io() {
    assert_table_manifest_watermark_source_clean();
}

#[test]
fn table_manifest_watermark_does_not_scan_wal_segments() {
    assert_table_manifest_watermark_source_clean();
}

#[test]
fn wal_truncation_does_not_parse_wal_objects_in_lifecycle() {
    assert_table_manifest_watermark_source_clean();
}

#[test]
fn table_manifest_watermark_does_not_decode_table_bytes_directly() {
    assert_table_manifest_watermark_source_clean();
}

#[test]
fn table_manifest_watermark_does_not_import_backend_delete() {
    assert_table_manifest_watermark_source_clean();
}

#[test]
fn table_manifest_watermark_does_not_import_engine_or_product_crates() {
    assert_table_manifest_watermark_source_clean();
}

#[test]
fn table_manifest_watermark_does_not_import_stratahub() {
    assert_table_manifest_watermark_source_clean();
}

#[test]
fn table_manifest_watermark_does_not_import_primitive_modules() {
    assert_table_manifest_watermark_source_clean();
}

#[test]
fn cache_mode_does_not_import_table_manifest_watermark_runner() {
    let root = common::crate_root();
    let path = root.join("src/lifecycle/cache.rs");
    let text = fs::read_to_string(&path).expect("read lifecycle cache source");

    for (line_number, line) in text.lines().enumerate() {
        assert!(
            !contains_table_manifest_watermark_runner_dependency(line),
            "src/lifecycle/cache.rs:{} imports table-manifest watermark runner: {line}",
            line_number + 1
        );
    }
}

#[test]
fn manifest_service_does_not_import_lifecycle() {
    let root = common::crate_root();
    let path = root.join("src/service/manifest.rs");
    let text = fs::read_to_string(&path).expect("read manifest service source");
    assert!(
        !imports_lifecycle_text(&text),
        "service manifest layer imports lifecycle"
    );
}

#[test]
fn lifecycle_checkpoint_runtime_avoids_segment_parsing_and_direct_delete() {
    let root = common::crate_root();
    let path = root.join("src/lifecycle/checkpoint.rs");
    let text = fs::read_to_string(&path).expect("read lifecycle checkpoint source");

    for (line_number, line) in text.lines().enumerate() {
        assert!(
            !contains_forbidden_checkpoint_dependency(line),
            "src/lifecycle/checkpoint.rs:{} calls forbidden checkpoint dependency: {line}",
            line_number + 1
        );
    }
}

#[test]
fn lifecycle_maintenance_executor_stays_scheduler_only() {
    let root = common::crate_root();
    let path = root.join("src/lifecycle/maintenance.rs");
    let text = fs::read_to_string(&path).expect("read lifecycle maintenance source");

    for (line_number, line) in text.lines().enumerate() {
        assert!(
            !contains_forbidden_maintenance_executor_dependency(line),
            "src/lifecycle/maintenance.rs:{} calls forbidden scheduler dependency: {line}",
            line_number + 1
        );
    }
}

#[test]
fn lifecycle_durable_maintenance_stays_out_of_assembly_and_bootstrap() {
    let root = common::crate_root();
    let maintenance_path = root.join("src/lifecycle/durable/maintenance.rs");
    let maintenance =
        fs::read_to_string(&maintenance_path).expect("read durable maintenance source");
    for (line_number, line) in maintenance.lines().enumerate() {
        assert!(
            !contains_forbidden_durable_maintenance_dependency(line),
            "src/lifecycle/durable/maintenance.rs:{} calls forbidden durable maintenance dependency: {line}",
            line_number + 1
        );
    }

    let bootstrap_path = root.join("src/lifecycle/durable/bootstrap.rs");
    let bootstrap = fs::read_to_string(&bootstrap_path).expect("read durable bootstrap source");
    for (line_number, line) in bootstrap.lines().enumerate() {
        assert!(
            !contains_forbidden_bootstrap_maintenance_dependency(line),
            "src/lifecycle/durable/bootstrap.rs:{} calls forbidden maintenance dependency: {line}",
            line_number + 1
        );
    }
}

#[test]
fn lifecycle_durable_close_stays_out_of_assembly_bootstrap_and_cache() {
    let root = common::crate_root();
    let close_path = root.join("src/lifecycle/durable/close.rs");
    let close = fs::read_to_string(&close_path).expect("read durable close source");
    for (line_number, line) in close.lines().enumerate() {
        assert!(
            !contains_forbidden_durable_close_dependency(line),
            "src/lifecycle/durable/close.rs:{} calls forbidden durable close dependency: {line}",
            line_number + 1
        );
    }

    let bootstrap_path = root.join("src/lifecycle/durable/bootstrap.rs");
    let bootstrap = fs::read_to_string(&bootstrap_path).expect("read durable bootstrap source");
    for (line_number, line) in bootstrap.lines().enumerate() {
        assert!(
            !contains_forbidden_bootstrap_close_dependency(line),
            "src/lifecycle/durable/bootstrap.rs:{} calls forbidden close dependency: {line}",
            line_number + 1
        );
    }

    let cache_path = root.join("src/lifecycle/cache.rs");
    let cache = fs::read_to_string(&cache_path).expect("read cache source");
    for (line_number, line) in cache.lines().enumerate() {
        assert!(
            !contains_forbidden_cache_close_dependency(line),
            "src/lifecycle/cache.rs:{} calls forbidden cache close dependency: {line}",
            line_number + 1
        );
    }
}

#[test]
fn lifecycle_generated_assurance_stays_in_testkit_tests_or_fuzz() {
    let root = common::crate_root();
    let allowed = [
        root.join("src/testkit/lifecycle/script.rs"),
        root.join("src/testkit/lifecycle/fault.rs"),
        root.join("src/testkit/lifecycle/crash.rs"),
        root.join("tests/lifecycle_properties.rs"),
        root.join("tests/lifecycle_maintenance.rs"),
        root.join("tests/lifecycle_faults.rs"),
        root.join("tests/lifecycle_fuzz_inventory.rs"),
        root.join("tests/lifecycle_closeout.rs"),
        root.join("tests/crash_recovery.rs"),
    ];

    for path in allowed {
        assert!(path.exists(), "{} missing", path.display());
    }
}

#[test]
fn lifecycle_production_does_not_import_testkit_or_fuzz() {
    let root = common::crate_root();
    for file in lifecycle_source_files(&root) {
        let text = fs::read_to_string(&file).expect("read lifecycle source");
        let compact = compact_uncommented_lowercase(&text);
        assert!(
            !compact.contains("crate::testkit")
                && !compact.contains("testkit::")
                && !compact.contains("fuzz"),
            "{} imports generated-assurance helpers",
            file.strip_prefix(&root).unwrap_or(&file).display()
        );
    }
}

#[test]
fn lifecycle_fuzz_targets_use_distinct_contracts() {
    let root = common::crate_root();
    let targets = [
        (
            "lifecycle_recovery",
            "check_lifecycle_recovery_fuzz_contract",
        ),
        (
            "lifecycle_maintenance",
            "check_lifecycle_maintenance_fuzz_contract",
        ),
        (
            "lifecycle_retention",
            "check_lifecycle_retention_fuzz_contract",
        ),
    ];

    let manifest = fs::read_to_string(root.join("fuzz/Cargo.toml")).expect("read fuzz manifest");
    let all_contracts = [
        "check_lifecycle_recovery_fuzz_contract",
        "check_lifecycle_maintenance_fuzz_contract",
        "check_lifecycle_retention_fuzz_contract",
    ];
    for (target, contract) in targets {
        assert!(manifest.contains(&format!("name = \"{target}\"")));
        let path = root.join(format!("fuzz/fuzz_targets/{target}.rs"));
        let text = fs::read_to_string(&path).expect("read lifecycle fuzz target");
        assert!(text.contains(contract), "{target} does not call {contract}");
        for other in all_contracts {
            if other != contract {
                assert!(
                    !text.contains(other),
                    "{target} should not call unrelated lifecycle fuzz contract {other}"
                );
            }
        }
        assert!(
            !text.contains("check_lifecycle_scaffold_contract"),
            "{target} calls scaffold-only contract"
        );
    }
    let script = fs::read_to_string(root.join("src/testkit/lifecycle/script.rs"))
        .expect("read lifecycle generated script contract");
    assert!(
        !script.contains("check_lifecycle_generated_script_contract(data)"),
        "fuzz contracts route through the aggregate generated script"
    );
    assert!(script.contains("check_lifecycle_recovery_contract(data)"));
    assert!(script.contains("check_lifecycle_maintenance_contract(bounded_slice(data"));
    assert!(script.contains("check_lifecycle_retention_contract(data)"));
}

#[test]
fn lifecycle_fuzz_corpora_are_seeded() {
    let root = common::crate_root();
    for target in [
        "lifecycle_recovery",
        "lifecycle_maintenance",
        "lifecycle_retention",
    ] {
        let dir = root.join(format!("fuzz/corpus/{target}"));
        let files = corpus_files(&dir);
        assert!(files.len() >= 3, "{target} corpus has too few seeds");
        for file in files {
            let metadata = fs::metadata(&file).expect("read corpus seed metadata");
            assert!(metadata.len() > 0, "{} is empty", file.display());
        }
    }
}

#[test]
fn lifecycle_crash_tests_are_feature_gated() {
    let root = common::crate_root();
    let text =
        fs::read_to_string(root.join("tests/crash_recovery.rs")).expect("read crash recovery test");

    assert!(text.contains("feature = \"localfs\""));
    assert!(text.contains("feature = \"testkit\""));
    assert!(text.contains("not(target_arch = \"wasm32\")"));
    assert!(text.contains("run_localfs_crash_recovery_harness"));
}

#[test]
fn ignored_crash_tests_have_nonignored_phase_equivalents() {
    const MARKER_PREFIX: &str = "// crash-harness phase-equivalent: ";
    let root = common::crate_root();
    let path = root.join("tests/crash_recovery.rs");
    let text = fs::read_to_string(&path).expect("read crash recovery test");
    let lines: Vec<&str> = text.lines().collect();

    let mut violations: Vec<String> = Vec::new();
    for (index, line) in lines.iter().enumerate() {
        if !line.trim_start().starts_with("#[ignore]") {
            continue;
        }
        let lower = index.saturating_sub(5);
        let upper = (index + 6).min(lines.len());
        let Some(pair_name) = lines[lower..upper]
            .iter()
            .find_map(|nearby| nearby.trim_start().strip_prefix(MARKER_PREFIX))
            .map(|tail| tail.trim().to_owned())
        else {
            violations.push(format!(
                "{}:{} #[ignore] missing '{}<fn>' marker within 5 lines",
                path.display(),
                index + 1,
                MARKER_PREFIX,
            ));
            continue;
        };
        let needle = format!("fn {pair_name}(");
        if !text.contains(&needle) {
            violations.push(format!(
                "{}:{} declares phase-equivalent `{pair_name}` but `{needle}` is not defined in the file",
                path.display(),
                index + 1,
            ));
        }
    }
    assert!(
        violations.is_empty(),
        "ignored-crash-test pairing violations:\n  {}",
        violations.join("\n  "),
    );
}

#[test]
fn lifecycle_generated_properties_assert_input_derived_counters() {
    let root = common::crate_root();
    let text = fs::read_to_string(root.join("tests/lifecycle_properties.rs"))
        .expect("read lifecycle properties");

    for counter in [
        "input_open_recovery_close_route_cases",
        "input_maintenance_route_cases",
        "input_reclaim_route_cases",
    ] {
        assert!(
            text.contains(counter),
            "missing generated counter {counter}"
        );
    }
}

#[test]
fn lifecycle_assurance_tests_avoid_sleeps_and_thread_spawns() {
    let root = common::crate_root();
    for relative in [
        "src/testkit/lifecycle/script.rs",
        "src/testkit/lifecycle/fault.rs",
        "src/testkit/lifecycle/crash.rs",
        "tests/lifecycle_properties.rs",
        "tests/lifecycle_maintenance.rs",
        "tests/lifecycle_faults.rs",
        "tests/lifecycle_fuzz_inventory.rs",
        "tests/lifecycle_closeout.rs",
        "tests/crash_recovery.rs",
    ] {
        let path = root.join(relative);
        let text = fs::read_to_string(&path).expect("read assurance source");
        for (line_number, line) in text.lines().enumerate() {
            assert!(
                !contains_sleep_or_thread_spawn(line),
                "{}:{} uses nondeterministic assurance primitive: {line}",
                relative,
                line_number + 1
            );
        }
    }
}

#[test]
fn lifecycle_table_rewrite_source_uses_branch_runtime_boundaries() {
    let root = common::crate_root();
    let path = root.join("src/lifecycle/compaction.rs");
    let text = fs::read_to_string(&path).expect("read lifecycle table rewrite source");

    for (line_number, line) in text.lines().enumerate() {
        assert!(
            !contains_forbidden_table_rewrite_dependency(line),
            "src/lifecycle/compaction.rs:{} calls forbidden table rewrite dependency: {line}",
            line_number + 1
        );
    }
}

#[test]
fn lifecycle_rewrite_publication_avoids_cleanup_pruning_and_product_dependencies() {
    assert_rewrite_publication_source_excludes(contains_forbidden_rewrite_publication_dependency);
}

#[test]
fn durable_rewrite_publication_does_not_import_raw_io() {
    assert_rewrite_publication_source_excludes(|line| {
        let lower = line.to_ascii_lowercase();
        ["std::fs", "std::path", "std::env", "openoptions", "mmap"]
            .iter()
            .any(|needle| lower.contains(needle))
    });
}

#[test]
fn durable_rewrite_publication_does_not_import_backend_delete() {
    assert_rewrite_publication_source_excludes(|line| {
        let lower = line.to_ascii_lowercase();
        [
            "delete_object(",
            "delete_covered_segments(",
            "truncate_wal(",
        ]
        .iter()
        .any(|needle| lower.contains(needle))
    });
}

#[test]
fn durable_rewrite_publication_does_not_import_quarantine_mutation() {
    assert_rewrite_publication_source_excludes(|line| {
        line.to_ascii_lowercase().contains("quarantine_object(")
    });
}

#[test]
fn durable_rewrite_publication_does_not_import_purge() {
    assert_rewrite_publication_source_excludes(|line| {
        let lower = line.to_ascii_lowercase();
        lower.contains("purge_quarantine") || lower.contains("purge_object(")
    });
}

#[test]
fn durable_rewrite_publication_does_not_import_row_pruning_policy() {
    assert_rewrite_publication_source_excludes(|line| {
        let lower = line.to_ascii_lowercase();
        [
            "dropolderversions",
            "droptombstones",
            "dropexpired",
            "retention_policy",
            "prune",
        ]
        .iter()
        .any(|needle| lower.contains(needle))
    });
}

#[test]
fn durable_rewrite_publication_does_not_import_engine_or_product_crates() {
    assert_rewrite_publication_source_excludes(|line| {
        let lower = line.to_ascii_lowercase();
        lower.contains("strata_engine")
            || lower.contains("strata_intelligence")
            || lower.contains("retention_report")
    });
}

#[test]
fn durable_rewrite_publication_does_not_import_stratahub() {
    assert_rewrite_publication_source_excludes(|line| {
        line.to_ascii_lowercase().contains("stratahub")
    });
}

#[test]
fn durable_rewrite_publication_does_not_import_primitive_modules() {
    assert_rewrite_publication_source_excludes(|line| {
        let lower = line.to_ascii_lowercase();
        ["primitive", "graph", "vector", "json"]
            .iter()
            .any(|needle| lower.contains(needle))
    });
}

#[test]
fn row_pruning_does_not_import_raw_io_or_object_cleanup() {
    assert_row_pruning_source_excludes(contains_forbidden_row_pruning_dependency);
}

#[test]
fn row_pruning_does_not_import_raw_io() {
    assert_row_pruning_source_excludes(|line| {
        let lower = line.to_ascii_lowercase();
        ["std::fs", "std::path", "std::env", "openoptions", "mmap"]
            .iter()
            .any(|needle| lower.contains(needle))
    });
}

#[test]
fn row_pruning_does_not_import_backend_delete() {
    assert_row_pruning_source_excludes(|line| {
        let lower = line.to_ascii_lowercase();
        [
            "backend.delete_object(",
            ".delete_object(",
            "delete_covered_segments(",
        ]
        .iter()
        .any(|needle| lower.contains(needle))
    });
}

#[test]
fn row_pruning_does_not_delete_table_objects() {
    row_pruning_does_not_import_backend_delete();
}

#[test]
fn row_pruning_does_not_import_quarantine_or_purge() {
    assert_row_pruning_source_excludes(|line| {
        let lower = line.to_ascii_lowercase();
        ["quarantine_object(", "purge_quarantine"]
            .iter()
            .any(|needle| lower.contains(needle))
    });
}

#[test]
fn row_pruning_does_not_quarantine_table_objects() {
    assert_row_pruning_source_excludes(|line| {
        line.to_ascii_lowercase().contains("quarantine_object(")
    });
}

#[test]
fn row_pruning_does_not_purge_objects() {
    assert_row_pruning_source_excludes(|line| {
        line.to_ascii_lowercase().contains("purge_quarantine")
    });
}

#[test]
fn row_pruning_does_not_import_snapshot_pruning() {
    assert_row_pruning_source_excludes(|line| {
        line.to_ascii_lowercase().contains("prune_snapshots(")
    });
}

#[test]
fn row_pruning_does_not_prune_snapshots() {
    row_pruning_does_not_import_snapshot_pruning();
}

#[test]
fn row_pruning_does_not_import_wal_truncation() {
    assert_row_pruning_source_excludes(|line| line.to_ascii_lowercase().contains("truncate_wal("));
}

#[test]
fn row_pruning_does_not_truncate_wal() {
    row_pruning_does_not_import_wal_truncation();
}

#[test]
fn row_pruning_does_not_persist_flush_watermark() {
    assert_row_pruning_source_excludes(|line| {
        line.to_ascii_lowercase()
            .contains("persist_flush_watermark(")
    });
}

#[test]
fn row_pruning_does_not_publish_database_manifest_directly() {
    assert_row_pruning_source_excludes(|line| {
        let lower = line.to_ascii_lowercase();
        lower.contains("databasemanifestservice") || lower.contains("publish_replace_manifest(")
    });
}

#[test]
fn row_pruning_does_not_import_product_policy() {
    assert_row_pruning_source_excludes(|line| {
        let lower = line.to_ascii_lowercase();
        ["strata_engine", "strata_intelligence", "retention_report"]
            .iter()
            .any(|needle| lower.contains(needle))
    });
}

#[test]
fn row_pruning_does_not_import_stratahub() {
    assert_row_pruning_source_excludes(|line| line.to_ascii_lowercase().contains("stratahub"));
}

#[test]
fn row_pruning_does_not_import_primitive_modules() {
    assert_row_pruning_source_excludes(|line| {
        let lower = line.to_ascii_lowercase();
        ["primitive", "graph", "vector", "json"]
            .iter()
            .any(|needle| lower.contains(needle))
    });
}

#[test]
fn row_pruning_does_not_use_wall_clock() {
    assert_row_pruning_source_excludes(|line| {
        let lower = line.to_ascii_lowercase();
        ["timestamp::now", "systemtime"]
            .iter()
            .any(|needle| lower.contains(needle))
    });
}

#[test]
fn row_pruning_code_and_fixture_names_do_not_use_milestone_labels() {
    let root = common::crate_root();
    let paths = [
        root.join("src/branch/pruning.rs"),
        root.join("src/branch/tests/row_pruning.rs"),
        root.join("src/branch/tests/row_pruning/required_plan.rs"),
        root.join("src/branch/tests/row_pruning/tombstone_ttl.rs"),
        root.join("src/lifecycle/compaction.rs"),
    ];
    let release_token = ['l', '8'].iter().collect::<String>();
    let milestone_token = ['m', '4'].iter().collect::<String>();
    for path in paths {
        let text = fs::read_to_string(&path).expect("read row pruning source");
        for (line_number, line) in text.lines().enumerate() {
            let lower = line.to_ascii_lowercase();
            assert!(
                !lower.contains(&release_token) && !lower.contains(&milestone_token),
                "{}:{} uses milestone vocabulary in row-pruning code: {line}",
                path.strip_prefix(&root).unwrap_or(&path).display(),
                line_number + 1
            );
        }
    }
}

#[test]
fn row_pruning_does_not_use_wall_clock_or_product_policy() {
    assert_row_pruning_source_excludes(|line| {
        let lower = line.to_ascii_lowercase();
        [
            "timestamp::now",
            "systemtime",
            "strata_engine",
            "strata_intelligence",
            "stratahub",
            "primitive",
            "graph",
            "vector",
            "json",
            "retention_report",
        ]
        .iter()
        .any(|needle| lower.contains(needle))
    });
}

#[test]
fn lifecycle_retention_source_delegates_durable_mutation() {
    let root = common::crate_root();
    let path = root.join("src/lifecycle/retention.rs");
    let text = fs::read_to_string(&path).expect("read lifecycle retention source");

    for (line_number, line) in text.lines().enumerate() {
        assert!(
            !contains_forbidden_retention_dependency(line),
            "src/lifecycle/retention.rs:{} calls forbidden retention dependency: {line}",
            line_number + 1
        );
    }
}

#[test]
fn memory_budget_does_not_probe_host_memory_or_use_global_cache() {
    assert_budget_source_excludes(contains_forbidden_budget_dependency);
}

#[test]
fn memory_budget_does_not_probe_host_memory() {
    assert_budget_source_excludes(|line| {
        let lower = line.to_ascii_lowercase();
        lower.contains("/proc/meminfo")
            || lower.contains("memavailable")
            || lower.contains("sysinfo")
            || lower.contains("available_memory")
            || lower.contains("host_memory")
    });
}

#[test]
fn memory_budget_does_not_use_process_global_cache() {
    assert_budget_source_excludes(|line| {
        let lower = line.to_ascii_lowercase();
        lower.contains("global_cache")
            || lower.contains("static global")
            || lower.contains("static mut")
            || lower.contains("oncelock<")
            || lower.contains("lazy_static")
            || lower.contains("once_cell")
    });
}

#[test]
fn memory_budget_does_not_import_product_resource_policy() {
    assert_budget_source_excludes(|line| {
        let compact = line.to_ascii_lowercase();
        compact.contains("strata_engine")
            || compact.contains("strata_intelligence")
            || compact.contains("resource_profile")
            || compact.contains("resource_policy")
            || compact.contains("primitive")
            || compact.contains("stratahub")
    });
}

#[test]
fn memory_budget_does_not_import_raw_io() {
    assert_budget_source_excludes(|line| {
        let compact = line.to_ascii_lowercase();
        compact.contains("std::fs")
            || compact.contains("std::path")
            || compact.contains("openoptions")
            || compact.contains("mmap")
            || compact.contains("std::env")
    });
}

#[test]
fn memory_budget_does_not_import_object_cleanup_boundaries() {
    assert_budget_source_excludes(|line| {
        let compact = line.to_ascii_lowercase();
        compact.contains("delete_object")
            || compact.contains("quarantineservice")
            || compact.contains("purge_quarantine")
    });
}

#[test]
fn memory_budget_does_not_import_backend_delete_or_quarantine() {
    assert_budget_source_excludes(|line| {
        let compact = line.to_ascii_lowercase();
        compact.contains("delete_object")
            || compact.contains("quarantine_object")
            || compact.contains("quarantineservice")
            || compact.contains("purge_quarantine")
    });
}

#[test]
fn memory_budget_does_not_import_stratahub() {
    assert_budget_source_excludes(|line| line.to_ascii_lowercase().contains("stratahub"));
}

#[test]
fn memory_budget_does_not_import_primitive_modules() {
    assert_budget_source_excludes(|line| line.to_ascii_lowercase().contains("primitive"));
}

#[test]
fn memory_budget_code_and_fixture_names_do_not_use_milestone_labels() {
    let root = common::crate_root();
    let forbidden = [format!("l{}", 8), format!("l{}", 7), format!("m{}", 4)];
    for relative in [
        "src/lifecycle/budget.rs",
        "src/lifecycle/tests/budget.rs",
        "src/lifecycle/tests/budget_runtime.rs",
    ] {
        let path = root.join(relative);
        let text = fs::read_to_string(&path).expect("read storage budget source");
        for (line_number, line) in text.lines().enumerate() {
            let compact = uncommented_text(line).to_ascii_lowercase();
            assert!(
                forbidden.iter().all(|label| !compact.contains(label)),
                "{relative}:{} contains architecture label in storage budget code: {line}",
                line_number + 1
            );
        }
    }
}

#[test]
fn lazy_reader_does_not_full_read_durable_object_on_open() {
    assert_lazy_reader_source_excludes(|line| {
        let compact = uncommented_text(line).to_ascii_lowercase();
        compact.contains("read_full")
            || compact.contains("read_full_source")
            || compact.contains("read_object(")
    });
}

#[test]
fn lazy_open_path_does_not_perform_full_object_reads() {
    // `service/table.rs` is intentionally exempt because it owns the
    // publish-dedup `require_exact_bytes` helper that reads the entire
    // object once via `read_all_for_exact_match`. That helper is a
    // post-publish equality check, not an open path. The two files below
    // are the lazy-open paths and must never perform a full-object read.
    assert_lazy_open_path_source_excludes(|line| {
        let compact = uncommented_text(line).to_ascii_lowercase();
        compact.contains("read_all_for_exact_match")
            || compact.contains("read_all(")
            || compact.contains("read_full")
            || compact.contains("read_full_source")
            || compact.contains("read_object(")
    });
}

#[test]
fn lazy_reader_does_not_import_raw_io() {
    assert_lazy_reader_source_excludes(|line| {
        let compact = uncommented_text(line).to_ascii_lowercase();
        compact.contains("std::fs")
            || compact.contains("std::path::path")
            || compact.contains("file::")
            || compact.contains("openoptions")
            || compact.contains("mmap")
    });
}

#[test]
fn lazy_reader_does_not_use_path_cache_identity_or_global_cache() {
    assert_lazy_reader_source_excludes(|line| {
        let compact = uncommented_text(line).to_ascii_lowercase();
        compact.contains("file_path_hash")
            || compact.contains("path_hash")
            || compact.contains("static global")
            || (compact.contains("static") && compact.contains("oncelock<"))
            || compact.contains("lazy_static")
    });
}

#[test]
fn lazy_reader_does_not_import_product_or_cleanup_policy() {
    assert_lazy_reader_source_excludes(|line| {
        let compact = uncommented_text(line).to_ascii_lowercase();
        compact.contains("strata_engine")
            || compact.contains("stratahub")
            || compact.contains("primitive")
            || compact.contains("vector")
            || compact.contains("graph")
            || compact.contains("delete_object")
            || compact.contains("quarantine_object")
    });
}

#[test]
fn lazy_reader_code_and_fixture_names_do_not_use_milestone_labels() {
    let forbidden = [format!("l{}", 8), format!("l{}", 7), format!("m{}", 4)];
    let root = common::crate_root();
    for relative in [
        "src/table/reader.rs",
        "src/service/table.rs",
        "src/lifecycle/table_manifest.rs",
    ] {
        let path = root.join(relative);
        let text = production_text(&path);
        for (line_number, line) in text.lines().enumerate() {
            let compact = uncommented_text(line).to_ascii_lowercase();
            assert!(
                forbidden.iter().all(|label| !compact.contains(label)),
                "{relative}:{} contains architecture label in lazy reader path: {line}",
                line_number + 1
            );
        }
    }
}

#[test]
fn lifecycle_table_reachability_source_is_classification_only() {
    assert_table_reachability_source_clean();
}

#[test]
fn table_reachability_does_not_import_raw_io() {
    assert_table_reachability_source_clean();
}

#[test]
fn table_reachability_does_not_import_backend_delete() {
    assert_table_reachability_source_clean();
}

#[test]
fn table_reachability_does_not_import_quarantine_mutation() {
    assert_table_reachability_source_clean();
}

#[test]
fn table_reachability_does_not_import_purge() {
    assert_table_reachability_source_clean();
}

#[test]
fn table_reachability_does_not_import_engine_or_product_crates() {
    assert_table_reachability_source_clean();
}

#[test]
fn table_reachability_does_not_import_stratahub() {
    assert_table_reachability_source_clean();
}

#[test]
fn table_reachability_does_not_import_primitive_modules() {
    assert_table_reachability_source_clean();
}

#[test]
fn table_reachability_does_not_use_product_retention_report() {
    assert_table_reachability_source_clean();
}

fn assert_table_reachability_source_clean() {
    let root = common::crate_root();
    let path = root.join("src/lifecycle/table_reachability.rs");
    let text = fs::read_to_string(&path).expect("read lifecycle table reachability source");

    for (line_number, line) in text.lines().enumerate() {
        assert!(
            !contains_forbidden_table_reachability_dependency(line),
            "src/lifecycle/table_reachability.rs:{} calls forbidden table reachability dependency: {line}",
            line_number + 1
        );
    }
}

fn assert_budget_fixtures() {
    assert!(contains_forbidden_budget_dependency("use std::fs;"));
    assert!(contains_forbidden_budget_dependency("let _: PathBuf;"));
    assert!(contains_forbidden_budget_dependency(
        "let _ = std::env::var(\"MEMORY\");"
    ));
    assert!(contains_forbidden_budget_dependency(
        "let _ = sysinfo::System::new_all();"
    ));
    assert!(contains_forbidden_budget_dependency(
        "let _ = proc_meminfo_available_bytes();"
    ));
    assert!(contains_forbidden_budget_dependency(
        "static GLOBAL_CACHE: TableBlockCache = TableBlockCache::disabled();"
    ));
    assert!(!contains_forbidden_budget_dependency(
        "StorageRuntimeBudget StorageBudgetLedger StorageBudgetPool"
    ));
}

fn assert_budget_source_excludes(predicate: impl Fn(&str) -> bool) {
    let root = common::crate_root();
    for relative in ["src/lifecycle/budget.rs"] {
        let path = root.join(relative);
        let text = fs::read_to_string(&path).expect("read budget source");
        for (line_number, line) in text.lines().enumerate() {
            assert!(
                !predicate(line),
                "{}:{} violates storage budget source guard: {line}",
                relative,
                line_number + 1
            );
        }
    }
}

fn assert_lazy_reader_source_excludes(predicate: impl Fn(&str) -> bool) {
    let root = common::crate_root();
    for relative in [
        "src/table/reader.rs",
        "src/service/table.rs",
        "src/lifecycle/table_manifest.rs",
    ] {
        let path = root.join(relative);
        let text = production_text(&path);
        for (line_number, line) in text.lines().enumerate() {
            assert!(
                !predicate(line),
                "{relative}:{} violates lazy reader source guard: {line}",
                line_number + 1
            );
        }
    }
}

fn assert_lazy_open_path_source_excludes(predicate: impl Fn(&str) -> bool) {
    let root = common::crate_root();
    for relative in ["src/table/reader.rs", "src/lifecycle/table_manifest.rs"] {
        let path = root.join(relative);
        let text = production_text(&path);
        for (line_number, line) in text.lines().enumerate() {
            assert!(
                !predicate(line),
                "{relative}:{} violates lazy open path source guard: {line}",
                line_number + 1
            );
        }
    }
}

fn production_text(path: &Path) -> String {
    let text = fs::read_to_string(path).expect("read production source");
    text.split("#[cfg(test)]")
        .next()
        .unwrap_or(text.as_str())
        .to_string()
}

fn assert_table_manifest_watermark_source_clean() {
    let root = common::crate_root();
    for relative in [
        "src/lifecycle/checkpoint.rs",
        "src/lifecycle/durable/maintenance.rs",
    ] {
        let path = root.join(relative);
        let text = fs::read_to_string(&path).expect("read table-manifest watermark source");
        for (line_number, line) in text.lines().enumerate() {
            assert!(
                !contains_forbidden_table_manifest_watermark_dependency(line),
                "{}:{} calls forbidden table-manifest watermark dependency: {line}",
                relative,
                line_number + 1
            );
        }
    }
}

#[test]
fn lifecycle_quarantine_source_uses_quarantine_service_boundary() {
    let root = common::crate_root();
    let path = root.join("src/lifecycle/quarantine.rs");
    let text = fs::read_to_string(&path).expect("read lifecycle quarantine source");

    for (line_number, line) in text.lines().enumerate() {
        assert!(
            !contains_forbidden_quarantine_dependency(line),
            "src/lifecycle/quarantine.rs:{} calls forbidden quarantine dependency: {line}",
            line_number + 1
        );
    }
}

#[test]
fn lifecycle_source_guard_catches_fixture_violations() {
    assert_general_source_guard_fixtures();
    assert_capability_preflight_fixtures();
    assert_cache_runtime_fixtures();
    assert_durable_runtime_fixtures();
    assert_flush_runtime_fixtures();
    assert_checkpoint_runtime_fixtures();
    assert_recovery_runtime_fixtures();
    assert_maintenance_executor_fixtures();
    assert_durable_maintenance_fixtures();
    assert_durable_close_fixtures();
    assert_table_rewrite_fixtures();
    assert_retention_fixtures();
    assert_table_reachability_fixtures();
    assert_budget_fixtures();
    assert_quarantine_fixtures();
    assert_public_surface_fixtures();
}

fn assert_general_source_guard_fixtures() {
    assert!(contains_forbidden_import_or_io(
        "use strata_engine::StorageRuntime;"
    ));
    assert!(contains_forbidden_import_or_io("let _: OpenOptions;"));
    assert!(contains_forbidden_import_or_io(
        "use crate::testkit::LifecycleHarness;"
    ));
    assert!(contains_forbidden_import_or_io_text(
        "use crate::{\n    testkit::LifecycleHarness,\n};"
    ));
    assert!(contains_forbidden_import_or_io_text(
        "use crate::{\n    api::StorageApi,\n};"
    ));
    assert!(contains_forbidden_import_or_io(
        "let _ = std::fs::read(\"manifest\");"
    ));
    assert!(contains_forbidden_import_or_io_text(
        "use std::{\n    fs,\n    path,\n};"
    ));
    assert!(contains_forbidden_import_or_io("let _: PathBuf;"));
    assert!(contains_forbidden_import_or_io(
        "let _ = std::env::var(\"X\");"
    ));
    assert!(contains_forbidden_product_vocabulary(
        "Database::open with OpenOptions"
    ));
    assert!(contains_forbidden_product_vocabulary(
        "database::open with openoptions"
    ));
    assert!(contains_forbidden_product_vocabulary(
        "manual maintenance command"
    ));
    assert!(contains_forbidden_product_vocabulary(
        "let _: VersionedValue;"
    ));
    assert!(contains_forbidden_product_vocabulary("let _: EntityRef;"));
    assert!(contains_forbidden_product_vocabulary("let _: JsonValue;"));
    assert!(contains_forbidden_product_vocabulary("let _: Graph;"));
    assert!(contains_forbidden_product_vocabulary("let _: Vector;"));
    assert!(contains_forbidden_product_vocabulary("let _: Search;"));
    assert!(contains_forbidden_product_vocabulary(
        "let _: event module;"
    ));
    assert!(contains_forbidden_product_vocabulary("let _: Embedding;"));
    assert!(contains_forbidden_product_vocabulary("let _: Inference;"));
    assert!(contains_forbidden_product_vocabulary("use StrataHub;"));
    assert!(contains_forbidden_product_vocabulary("use stratahub;"));
    assert!(contains_forbidden_product_vocabulary("refresh follower"));
    assert!(contains_forbidden_product_vocabulary("follower mode"));
    assert!(contains_forbidden_product_vocabulary(
        "let _: TransactionContext;"
    ));
    assert!(contains_forbidden_product_vocabulary(
        "begin_transaction();"
    ));
    assert!(imports_lifecycle("use crate::lifecycle::LifecycleState;"));
    assert!(imports_lifecycle_text(
        "use crate::{\n    lifecycle::LifecycleState,\n};"
    ));
    assert!(!contains_forbidden_product_vocabulary(
        "BranchId CommitVersion StorageRow WalService RecoveryHealth MaintenanceTask"
    ));
    assert!(!contains_forbidden_import_or_io(
        "RecoveryHealth MaintenanceTask CloseOutcome"
    ));
}

fn assert_capability_preflight_fixtures() {
    assert!(contains_forbidden_capability_preflight_dependency(
        "use crate::service::WalService;"
    ));
    assert!(contains_forbidden_capability_preflight_dependency(
        "use crate::layout::ObjectLayout;"
    ));
    assert!(contains_forbidden_capability_preflight_dependency_text(
        "use crate::{\n    commit::CommitRuntime,\n};"
    ));
    assert!(contains_forbidden_capability_preflight_dependency(
        "backend.read_object(name)?;"
    ));
    assert!(contains_forbidden_capability_preflight_dependency(
        "backend.acquire_writer_lock(name)?;"
    ));
}

fn assert_cache_runtime_fixtures() {
    assert!(contains_forbidden_cache_runtime_dependency(
        "use crate::service::WalService;"
    ));
    assert!(contains_forbidden_cache_runtime_dependency_text(
        "use crate::{\n    layout::ObjectLayout,\n};"
    ));
    assert!(contains_forbidden_cache_runtime_dependency(
        "backend.read_object(name)?;"
    ));
    assert!(contains_forbidden_cache_runtime_dependency(
        "backend.read_range(name, range)?;"
    ));
    assert!(contains_forbidden_cache_runtime_dependency(
        "backend.write_object(name, bytes)?;"
    ));
    assert!(contains_forbidden_cache_runtime_dependency(
        "backend.delete_object(name)?;"
    ));
    assert!(contains_forbidden_cache_runtime_dependency(
        "backend.list_prefix(prefix)?;"
    ));
    assert!(contains_forbidden_cache_runtime_dependency(
        "backend.object_metadata(name)?;"
    ));
    assert!(contains_forbidden_cache_runtime_dependency(
        "backend.publish_object(name, bytes, mode)?;"
    ));
    assert!(contains_cache_table_object_service_dependency(
        "TableObjectService::new(backend)"
    ));
    assert!(contains_cache_table_object_service_dependency(
        "table_service.publish_create(object, bytes)?"
    ));
}

fn assert_durable_runtime_fixtures() {
    assert!(contains_forbidden_durable_runtime_dependency(
        "let name = \"locks/writer\";"
    ));
    assert!(contains_forbidden_durable_runtime_dependency(
        "CommitReplayRuntime::new();"
    ));
    assert!(contains_forbidden_durable_runtime_dependency(
        "let _: CommitReplayRequest;"
    ));
    assert!(contains_forbidden_durable_runtime_dependency(
        "allocator.catch_up_to_recovered_version(version);"
    ));
    assert!(contains_forbidden_durable_runtime_dependency(
        "visible.catch_up_visible_after_replay(version);"
    ));
    assert!(contains_forbidden_durable_runtime_dependency(
        "WalRecord::new(version, branch, timestamp, payload);"
    ));
    assert!(contains_forbidden_durable_runtime_dependency(
        "checkpoint_service.checkpoint(request)?;"
    ));
    assert!(contains_forbidden_durable_runtime_dependency(
        "quarantine.load_inventory(branch, db, codec)?;"
    ));
    assert!(contains_forbidden_bootstrap_assembly_dependency(
        "backend.acquire_writer_lock(&lock)?;"
    ));
    assert!(contains_forbidden_bootstrap_assembly_dependency(
        "WalService::open(backend, segment, policy, config)?;"
    ));
    assert!(contains_forbidden_bootstrap_assembly_dependency(
        "DatabaseManifestService::new(backend);"
    ));
}

fn assert_flush_runtime_fixtures() {
    assert!(contains_forbidden_flush_dependency(
        "service.persist_flush_watermark(version)?;"
    ));
    assert!(contains_forbidden_flush_dependency(
        "wal.delete_covered_segments(proof)?;"
    ));
    assert!(contains_forbidden_flush_dependency(
        "checkpoint_service.checkpoint(request)?;"
    ));
    assert!(contains_forbidden_flush_dependency(
        "quarantine.load_inventory(branch, db, codec)?;"
    ));
}

fn assert_checkpoint_runtime_fixtures() {
    assert!(contains_forbidden_checkpoint_dependency(
        "decode_wal_record(bytes)?;"
    ));
    assert!(contains_forbidden_checkpoint_dependency(
        "ObjectLayout::wal_segment(1)?;"
    ));
    assert!(contains_forbidden_checkpoint_dependency(
        "name.as_str().split('/')"
    ));
    assert!(contains_forbidden_checkpoint_dependency(
        "backend.delete_object(name)?;"
    ));
    assert!(!contains_forbidden_checkpoint_dependency(
        "WalRetentionProof::snapshot_watermark(version)"
    ));
    assert!(!contains_forbidden_checkpoint_dependency(
        "wal.delete_covered_segments(proof)"
    ));
}

fn assert_recovery_runtime_fixtures() {
    assert!(contains_forbidden_recovery_runtime_dependency(
        "CommitReplayRuntime::new();"
    ));
    assert!(contains_forbidden_recovery_runtime_dependency(
        "execute_durable_commit(request)?;"
    ));
    assert!(contains_forbidden_recovery_runtime_dependency(
        "visible.publish_from_facts(facts)?;"
    ));
    assert!(contains_forbidden_recovery_runtime_dependency(
        "primitive_registry.reconstruct(row)?;"
    ));
}

fn assert_table_rewrite_fixtures() {
    assert!(contains_forbidden_table_rewrite_dependency(
        "truncate_wal(service, request)?;"
    ));
    assert!(contains_forbidden_table_rewrite_dependency(
        "persist_flush_watermark(version)?;"
    ));
    assert!(contains_forbidden_table_rewrite_dependency(
        "delete_covered_segments(proof)?;"
    ));
    assert!(contains_forbidden_table_rewrite_dependency(
        "TableObjectService::new(backend);"
    ));
    assert!(contains_forbidden_table_rewrite_dependency(
        "TableCompactor::new(config, builder)?;"
    ));
    assert!(contains_forbidden_table_rewrite_dependency(
        "quarantine.load_inventory(branch, db, codec)?;"
    ));
    assert!(contains_forbidden_rewrite_publication_dependency(
        "delete_covered_segments(proof)?;"
    ));
    assert!(contains_forbidden_rewrite_publication_dependency(
        "quarantine_object(object)?;"
    ));
    assert!(contains_forbidden_rewrite_publication_dependency(
        "BranchCompactionRetentionPolicy::DropExpired"
    ));
    assert!(contains_forbidden_rewrite_publication_dependency(
        "use strata_engine::Runtime;"
    ));
    assert!(!contains_forbidden_rewrite_publication_dependency(
        "TableObjectService::new(backend);"
    ));
}

fn assert_retention_fixtures() {
    assert!(contains_forbidden_retention_dependency(
        "backend.delete_object(name)?;"
    ));
    assert!(contains_forbidden_retention_dependency(
        "wal.delete_covered_segments(proof)?;"
    ));
    assert!(contains_forbidden_retention_dependency(
        "quarantine.quarantine_object(object)?;"
    ));
    assert!(contains_forbidden_retention_dependency(
        "quarantine.purge_object(object)?;"
    ));
    assert!(contains_forbidden_retention_dependency(
        "ObjectLayout::wal_segment(1)?;"
    ));
    assert!(contains_forbidden_retention_dependency(
        "let bytes = std::fs::read(path)?;"
    ));
    assert!(contains_forbidden_retention_dependency(
        "let path = std::path::Path::new(\"db\");"
    ));
    assert!(contains_forbidden_retention_dependency(
        "let value = std::env::var(\"HOME\")?;"
    ));
    assert!(contains_forbidden_retention_dependency(
        "let file = std::fs::File::open(path)?;"
    ));
    assert!(contains_forbidden_retention_dependency(
        "let mmap = memmap2::Mmap::map(&file)?;"
    ));
    assert!(contains_forbidden_retention_dependency(
        "strata_engine::retention::scan();"
    ));
    assert!(contains_forbidden_retention_dependency(
        "strata_intelligence::retention::scan();"
    ));
    assert!(contains_forbidden_retention_dependency(
        "primitive_registry.retention_report();"
    ));
    assert!(contains_forbidden_retention_dependency(
        "name.as_str().split('/')"
    ));
    assert!(!contains_forbidden_retention_dependency(
        "snapshots.prune_snapshots(live, retain)?;"
    ));
}

fn assert_table_reachability_fixtures() {
    assert!(contains_forbidden_table_reachability_dependency(
        "let bytes = std::fs::read(path)?;"
    ));
    assert!(contains_forbidden_table_reachability_dependency(
        "let path = std::path::Path::new(\"db\");"
    ));
    assert!(contains_forbidden_table_reachability_dependency(
        "let value = std::env::var(\"HOME\")?;"
    ));
    assert!(contains_forbidden_table_reachability_dependency(
        "backend.delete_object(name)?;"
    ));
    assert!(contains_forbidden_table_reachability_dependency(
        "purge_quarantine(branch)?;"
    ));
    assert!(contains_forbidden_table_reachability_dependency(
        "quarantine_object(request)?;"
    ));
    assert!(contains_forbidden_table_reachability_dependency(
        "retention_report::from_storage(decisions);"
    ));
    assert!(contains_forbidden_table_reachability_dependency(
        "strata_engine::storage::retention();"
    ));
    assert!(contains_forbidden_table_reachability_dependency(
        "stratahub::storage::retention();"
    ));
    assert!(contains_forbidden_table_reachability_dependency(
        "primitive_registry.retention_policy();"
    ));
    assert!(contains_forbidden_table_reachability_dependency(
        "let graph = ReachabilityGraph::new();"
    ));
    assert!(contains_forbidden_table_reachability_dependency(
        "let vector = vec![object];"
    ));
    assert!(contains_forbidden_table_reachability_dependency(
        "serde_json::to_vec(&decision)?;"
    ));
    assert!(!contains_forbidden_table_reachability_dependency(
        "LifecycleRetentionDecisionRecord::table(object, decision, reason);"
    ));
}

fn assert_quarantine_fixtures() {
    assert!(contains_forbidden_quarantine_dependency(
        "backend.delete_object(name)?;"
    ));
    assert!(contains_forbidden_quarantine_dependency(
        "publish_object(name, bytes, mode)?;"
    ));
    assert!(contains_forbidden_quarantine_dependency(
        "encode_quarantine_inventory(bytes)?;"
    ));
    assert!(contains_forbidden_quarantine_dependency(
        "delete_covered_segments(proof)?;"
    ));
    assert!(contains_forbidden_quarantine_dependency(
        "snapshots.prune_snapshots(live, retain)?;"
    ));
    assert!(!contains_forbidden_quarantine_dependency(
        "service.quarantine_object(&request)?;"
    ));
    assert!(!contains_forbidden_quarantine_dependency(
        "service.purge_quarantine(request)?;"
    ));
}

fn assert_maintenance_executor_fixtures() {
    assert!(contains_forbidden_maintenance_executor_dependency(
        "TableObjectService::new(backend);"
    ));
    assert!(contains_forbidden_maintenance_executor_dependency(
        "checkpoint_durable_branch(branch, services, guards, visible, request)?;"
    ));
    assert!(contains_forbidden_maintenance_executor_dependency(
        "LifecycleDurableLocalRuntime::open(request)?;"
    ));
}

fn assert_durable_maintenance_fixtures() {
    assert!(contains_forbidden_durable_maintenance_dependency(
        "DatabaseManifestService::new(backend);"
    ));
    assert!(contains_forbidden_durable_maintenance_dependency(
        "CommitReplayRuntime::new(config);"
    ));
    assert!(contains_forbidden_bootstrap_maintenance_dependency(
        "self.run_next_flush_maintenance()?;"
    ));
    assert!(contains_forbidden_bootstrap_maintenance_dependency(
        "self.compact_branch_tables(request)?;"
    ));
}

fn assert_durable_close_fixtures() {
    assert!(contains_forbidden_durable_close_dependency(
        "WalService::open(backend, segment, policy, config)?;"
    ));
    assert!(contains_forbidden_durable_close_dependency(
        "CommitReplayRuntime::new(config);"
    ));
    assert!(contains_forbidden_durable_close_dependency(
        "complete_recovery(recovery)?;"
    ));
    assert!(contains_forbidden_bootstrap_close_dependency(
        "self.services.wal_mut().close()?;"
    ));
    assert!(contains_forbidden_bootstrap_close_dependency(
        "self.services.release_writer_guard();"
    ));
    assert!(contains_forbidden_cache_close_dependency(
        "self.services.wal_mut().close()?;"
    ));
    assert!(contains_forbidden_cache_close_dependency(
        "self.services.release_writer_guard();"
    ));
}

fn assert_public_surface_fixtures() {
    assert!(is_public_surface_leak("pub struct LifecycleRuntime;"));

    assert!(!is_public_surface_leak(
        "pub(crate) struct LifecycleRuntime;"
    ));
}

fn lifecycle_source_files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    collect_rs_files(&root.join("src/lifecycle"), &mut files);
    files.sort();
    files
}

fn lifecycle_testkit_files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    collect_rs_files(&root.join("src/testkit/lifecycle"), &mut files);
    files.sort();
    files
}

fn lifecycle_unit_test_files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    collect_rs_files(&root.join("src/lifecycle/tests"), &mut files);
    files.sort();
    files
}

fn lifecycle_integration_test_files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let tests_dir = root.join("tests");
    for entry in fs::read_dir(tests_dir).expect("read integration tests dir") {
        let path = entry.expect("read integration test entry").path();
        let stem = path.file_stem().and_then(|name| name.to_str());
        // `lifecycle_hardening_closeout.rs` is intentionally excluded: it
        // legitimately references milestone-named paths in the hardening
        // docs and porting log for inventory verification. Excluding it
        // from the milestone-label scan preserves the scan's intent (no
        // labels in implementation code) without blocking the closeout
        // test from asserting on real artifact names.
        if stem.is_some_and(|name| {
            name.starts_with("lifecycle_") && name != "lifecycle_hardening_closeout"
        }) && path
            .extension()
            .and_then(|extension| extension.to_str())
            .is_some_and(|extension| extension.eq_ignore_ascii_case("rs"))
        {
            files.push(path);
        }
    }
    files.sort();
    files
}

fn collect_rs_files(dir: &Path, files: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).expect("read source dir") {
        let entry = entry.expect("read source entry");
        let path = entry.path();
        if path.is_dir() {
            if path.file_name().is_none_or(|name| name != "tests") {
                collect_rs_files(&path, files);
            }
        } else if path.extension().is_some_and(|extension| extension == "rs")
            && path.file_name().is_some_and(|name| name != "tests.rs")
        {
            files.push(path);
        }
    }
}

fn corpus_files(dir: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    for entry in fs::read_dir(dir).expect("read corpus dir") {
        let path = entry.expect("read corpus entry").path();
        if path.is_file() {
            files.push(path);
        }
    }
    files.sort();
    files
}

fn compact_uncommented_lowercase(text: &str) -> String {
    uncommented_text(text)
        .chars()
        .filter(|character| !character.is_whitespace())
        .collect::<String>()
        .to_ascii_lowercase()
}

fn contains_forbidden_import_or_io(line: &str) -> bool {
    let lower = line.to_ascii_lowercase();
    [
        "strata_engine",
        "strata-engine",
        "crates/engine",
        "crate::api",
        "crate::testkit",
        "std::fs",
        "std::path",
        "std::env",
        "env::var",
        "mmap",
        "memmap",
        "openoptions",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
        || contains_ascii_word(line, "Path")
        || contains_ascii_word(line, "PathBuf")
        || contains_ascii_word(line, "File")
}

fn contains_sleep_or_thread_spawn(line: &str) -> bool {
    let lower = line.to_ascii_lowercase();
    [
        "std::thread",
        "thread::spawn",
        "thread::sleep",
        "tokio::time::sleep",
        "std::time::duration",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

/// Dark-launch perf A/B toggles read once from the environment. These are the
/// ONLY sanctioned lifecycle env reads; each dies with its calibration:
/// `STRATA_ADMISSION` when the legacy admission path is retired at readiness
/// hardening, the lane/subcompaction knobs when the W1 compaction calibration
/// fixes their defaults. Any other env read still fails the guard.
const ALLOWED_DARK_LAUNCH_ENV_READS: [&str; 3] = [
    "std::env::var(\"STRATA_ADMISSION\")",
    "std::env::var(\"STRATA_COMPACTION_LANES\")",
    "std::env::var(\"STRATA_SUBCOMPACTIONS\")",
];

fn strip_allowed_dark_launch_env_reads(text: &str) -> String {
    let mut text = text.to_owned();
    for allowed in ALLOWED_DARK_LAUNCH_ENV_READS {
        text = text.replace(allowed, "");
    }
    text
}

fn contains_forbidden_import_or_io_text(text: &str) -> bool {
    let text = strip_allowed_dark_launch_env_reads(text);
    let text = text.as_str();
    let compact: String = uncommented_text(text)
        .chars()
        .filter(|character| !character.is_whitespace())
        .collect::<String>()
        .to_ascii_lowercase();
    contains_forbidden_import_or_io(text)
        || [
            "usestd::{fs",
            "usestd::{path",
            "usestd::{env",
            "crate::{api",
            "crate::{testkit",
        ]
        .iter()
        .any(|needle| compact.contains(needle))
}

fn contains_forbidden_product_vocabulary(line: &str) -> bool {
    let lower = line.to_ascii_lowercase();
    [
        "database::open",
        "openoptions",
        "productopen",
        "productrecovery",
        "versionedvalue",
        "entityref",
        "jsonvalue",
        "graph",
        "vector",
        "search",
        "event module",
        "embedding",
        "inference",
        "stratahub",
        "follower",
        "transactioncontext",
        "begin_transaction",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
        || [
            "public maintenance",
            "manual maintenance command",
            "refresh follower",
            "follower mode",
        ]
        .iter()
        .any(|needle| lower.contains(needle))
}

fn imports_lifecycle(line: &str) -> bool {
    imports_lifecycle_text(line)
}

fn imports_lifecycle_text(text: &str) -> bool {
    let compact: String = uncommented_text(text)
        .chars()
        .filter(|character| !character.is_whitespace())
        .collect();
    compact.contains("crate::lifecycle")
        || (compact.contains("crate::{") && compact.contains("lifecycle::"))
        || compact.contains("super::lifecycle")
        || (compact.contains("super::{") && compact.contains("lifecycle::"))
}

fn contains_forbidden_capability_preflight_dependency(line: &str) -> bool {
    let lower = line.to_ascii_lowercase();
    [
        "crate::service",
        "crate::layout",
        "crate::format",
        "crate::table",
        "crate::branch",
        "crate::commit",
        "objectlayout",
        "walservice",
        "databasemanifest",
        "snapshotservice",
        "tableobjectservice",
        "read_object(",
        "read_range(",
        "write_object(",
        "delete_object(",
        "list_prefix(",
        "object_metadata(",
        "append_object(",
        "sync_object(",
        "publish_object(",
        "conditional_create(",
        "conditional_update(",
        "acquire_writer_lock(",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

fn contains_forbidden_capability_preflight_dependency_text(text: &str) -> bool {
    let compact: String = uncommented_text(text)
        .chars()
        .filter(|character| !character.is_whitespace())
        .collect::<String>()
        .to_ascii_lowercase();
    contains_forbidden_capability_preflight_dependency(text)
        || [
            "crate::{service",
            "crate::{layout",
            "crate::{format",
            "crate::{table",
            "crate::{branch",
            "crate::{commit",
        ]
        .iter()
        .any(|needle| compact.contains(needle))
}

fn contains_forbidden_cache_runtime_dependency(line: &str) -> bool {
    let lower = line.to_ascii_lowercase();
    [
        "crate::service",
        "crate::layout",
        "crate::format",
        "objectlayout",
        "walservice",
        "snapshotservice",
        "databasemanifestservice",
        "tablemanifestservice",
        "quarantineservice",
        "tableobjectservice",
        "read_object(",
        "read_range(",
        "write_object(",
        "delete_object(",
        "list_prefix(",
        "object_metadata(",
        "acquire_writer_lock(",
        "append_object(",
        "sync_object(",
        "publish_object(",
        "conditional_create(",
        "conditional_update(",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

fn contains_forbidden_cache_runtime_dependency_text(text: &str) -> bool {
    let compact: String = uncommented_text(text)
        .chars()
        .filter(|character| !character.is_whitespace())
        .collect::<String>()
        .to_ascii_lowercase();
    contains_forbidden_cache_runtime_dependency(text)
        || [
            "crate::{service",
            "crate::{layout",
            "crate::{format",
            "usecrate::service",
            "usecrate::layout",
            "usecrate::format",
        ]
        .iter()
        .any(|needle| compact.contains(needle))
        || contains_forbidden_cache_runtime_dependency(&compact)
}

fn contains_cache_table_object_service_dependency(line: &str) -> bool {
    let lower = line.to_ascii_lowercase();
    [
        "tableobjectservice",
        "tableobjectreaderservice",
        "publish_create(",
        "open_reader(",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

fn contains_forbidden_durable_runtime_dependency(line: &str) -> bool {
    let lower = line.to_ascii_lowercase();
    [
        "commitreplayruntime",
        "commitreplayrequest",
        "catch_up_to_recovered_version(",
        "catch_up_visible_after_replay(",
        "\"locks/writer\"",
        "walrecord::new",
        "checkpoint_service.checkpoint(",
        "load_inventory(",
        "read_after_commit_version(",
        "repair_latest_tail(",
        "delete_covered_segments(",
        "list_snapshots(",
        "open_reader(",
        "execute_cache_commit(",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

fn contains_forbidden_bootstrap_assembly_dependency(line: &str) -> bool {
    let lower = line.to_ascii_lowercase();
    [
        "acquire_writer_lock(",
        "objectlayout::writer_lock",
        "databasemanifestservice::new",
        "load_or_create_manifest(",
        "walservice::open",
        "tablemanifestservice::new",
        "snapshotservice::new",
        "tableobjectservice::new",
        "checkpointservice::new",
        "quarantineservice::new",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

fn contains_forbidden_flush_dependency(line: &str) -> bool {
    let lower = line.to_ascii_lowercase();
    [
        "persist_flush_watermark(",
        "delete_covered_segments(",
        "repair_latest_tail(",
        "checkpointservice",
        "checkpoint_service",
        "databasemanifestservice",
        "walservice",
        "quarantineservice",
        "quarantine.",
        "snapshotservice",
        "tablemanifestservice",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

fn contains_forbidden_checkpoint_dependency(line: &str) -> bool {
    let lower = line.to_ascii_lowercase();
    [
        "decode_wal_record",
        "decode_wal_record_envelope",
        "decode_wal_segment",
        "encode_wal_record",
        "objectlayout::wal_segment",
        "objectlayout::wal_metadata",
        "backend.delete_object(",
        "read_range(",
        ".as_str().split",
        "strip_prefix(",
        "parse::<u64>",
        "std::fs",
        "std::path",
        "std::env",
        "std::fs::file",
        "openoptions",
        "mmap",
        "strata_engine",
        "strata_intelligence",
        "primitive",
        "retention_report",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

fn contains_forbidden_recovery_runtime_dependency(line: &str) -> bool {
    let lower = line.to_ascii_lowercase();
    [
        "commitreplayruntime",
        "execute_durable_commit",
        "execute_cache_commit",
        "publish_from_facts(",
        "catch_up_to_recovered_version(",
        "catch_up_to_recovered_timestamp(",
        "primitive_registry",
        "reconstruct(",
        "stratahub",
        "follower",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

fn contains_forbidden_table_manifest_recovery_dependency(line: &str) -> bool {
    let lower = line.to_ascii_lowercase();
    [
        "list_prefix(",
        "read_dir",
        "std::fs",
        "std::path",
        "std::env",
        "openoptions",
        "mmap",
        "strata_engine",
        "strata_intelligence",
        "stratahub",
        "primitive",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

fn contains_forbidden_table_manifest_publication_dependency(line: &str) -> bool {
    let lower = line.to_ascii_lowercase();
    [
        "truncate_wal(",
        "delete_covered_segments(",
        "persist_flush_watermark(",
        "repair_latest_tail(",
        "wal_truncation",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

fn contains_forbidden_table_manifest_watermark_dependency(line: &str) -> bool {
    let lower = line.to_ascii_lowercase();
    [
        "std::fs",
        "std::path",
        "read_dir",
        "list_prefix(",
        "decode_wal",
        "walrecord",
        "decode_immutable_table",
        "backend.delete_object(",
        ".delete_object(",
        "strata_engine",
        "strata_intelligence",
        "stratahub",
        "primitive",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

fn contains_table_manifest_watermark_runner_dependency(line: &str) -> bool {
    let lower = line.to_ascii_lowercase();
    [
        "persist_table_manifest_flush_watermark",
        "lifecycletablemanifestflushcoverageproof",
        "tablemanifestcovered",
        "tablemanifestservice",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

fn contains_forbidden_table_rewrite_dependency(line: &str) -> bool {
    let lower = line.to_ascii_lowercase();
    [
        "truncate_wal(",
        "persist_flush_watermark(",
        "delete_covered_segments(",
        "checkpoint_durable_branch(",
        "checkpoint_request",
        "wal_truncation",
        "quarantine",
        "purge",
        "repair_latest_tail",
        "tableobjectservice",
        "publish_create(",
        "open_reader(",
        "tablecompactor",
        "keepalltablecompactionpolicy",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

fn assert_rewrite_publication_source_excludes(predicate: impl Fn(&str) -> bool) {
    let root = common::crate_root();
    let path = root.join("src/lifecycle/rewrite_publication.rs");
    let text = fs::read_to_string(&path).expect("read lifecycle rewrite publication source");

    for (line_number, line) in text.lines().enumerate() {
        assert!(
            !predicate(line),
            "src/lifecycle/rewrite_publication.rs:{} calls forbidden rewrite publication dependency: {line}",
            line_number + 1
        );
    }
}

fn assert_row_pruning_source_excludes(predicate: impl Fn(&str) -> bool) {
    let root = common::crate_root();
    let paths = [
        root.join("src/branch/pruning.rs"),
        root.join("src/lifecycle/compaction.rs"),
    ];
    for path in paths {
        let text = fs::read_to_string(&path).expect("read row pruning source");
        for (line_number, line) in text.lines().enumerate() {
            assert!(
                !predicate(line),
                "{}:{} calls forbidden row pruning dependency: {line}",
                path.strip_prefix(&root).unwrap_or(&path).display(),
                line_number + 1
            );
        }
    }
}

fn contains_forbidden_row_pruning_dependency(line: &str) -> bool {
    let lower = line.to_ascii_lowercase();
    [
        "std::fs",
        "std::path",
        "std::env",
        "openoptions",
        "mmap",
        "backend.delete_object(",
        ".delete_object(",
        "delete_covered_segments(",
        "quarantine_object(",
        "purge_quarantine",
        "prune_snapshots(",
        "truncate_wal(",
        "persist_flush_watermark(",
        "tableobjectservice",
        "walservice",
        "snapshotservice",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

fn contains_forbidden_rewrite_publication_dependency(line: &str) -> bool {
    let lower = line.to_ascii_lowercase();
    [
        "std::fs",
        "std::path",
        "std::env",
        "openoptions",
        "mmap",
        "truncate_wal(",
        "persist_flush_watermark(",
        "delete_covered_segments(",
        "backend.delete_object(",
        ".delete_object(",
        "quarantine_object(",
        "purge_quarantine",
        "purge_object(",
        "repair_latest_tail(",
        "dropolderversions",
        "droptombstones",
        "dropexpired",
        "retention_policy",
        "strata_engine",
        "strata_intelligence",
        "stratahub",
        "primitive",
        "graph",
        "vector",
        "json",
        "retention_report",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

fn contains_forbidden_retention_dependency(line: &str) -> bool {
    let lower = line.to_ascii_lowercase();
    [
        "backend.delete_object(",
        "delete_covered_segments(",
        "walservice",
        "objectlayout::wal_segment",
        "objectlayout::wal_metadata",
        "quarantineservice",
        "quarantine_object(",
        "purge_object(",
        ".as_str().split",
        "strip_prefix(",
        "parse::<u64>",
        "std::fs",
        "std::path",
        "std::env",
        "std::fs::file",
        "openoptions",
        "mmap",
        "strata_engine",
        "strata_intelligence",
        "primitive",
        "retention_report",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

fn contains_forbidden_budget_dependency(line: &str) -> bool {
    let lower = line.to_ascii_lowercase();
    [
        "std::fs",
        "std::path",
        "std::env",
        "env::var",
        "openoptions",
        "mmap",
        "memmap",
        "sysinfo",
        "proc_meminfo",
        "available_memory",
        "host_memory",
        "num_cpus",
        "global_cache",
        "static mut",
        "lazy_static",
        "once_cell",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
        || contains_ascii_word(line, "Path")
        || contains_ascii_word(line, "PathBuf")
        || contains_ascii_word(line, "File")
}

fn contains_forbidden_table_reachability_dependency(line: &str) -> bool {
    let lower = line.to_ascii_lowercase();
    [
        "std::fs",
        "std::path::path",
        "std::env",
        "delete_object",
        "purge_quarantine",
        "quarantine_object",
        "retention_report",
        "strata_engine",
        "stratahub",
        "primitive",
        "graph",
        "vector",
        "json",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

fn contains_forbidden_branch_lifecycle_dependency(line: &str) -> bool {
    let lower = line.to_ascii_lowercase();
    [
        "std::fs",
        "std::path",
        "openoptions",
        "mmap",
        "std::env",
        "delete_object",
        "tableobjectservice",
        "strata_engine",
        "strata_intelligence",
        "primitive",
        "query",
        "remote",
        "stratahub",
        "workspace policy",
        "permission",
        "cherry-pick",
        "merge",
        "revert",
        "restore",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
        || contains_ascii_word(line, "Path")
        || contains_ascii_word(line, "PathBuf")
        || contains_ascii_word(line, "File")
}

fn contains_forbidden_quarantine_dependency(line: &str) -> bool {
    let lower = line.to_ascii_lowercase();
    [
        "backend.",
        "delete_object(",
        "publish_object(",
        "conditional_create(",
        "conditional_update(",
        "encode_quarantine_inventory",
        "decode_quarantine_inventory",
        "walservice",
        "snapshotservice",
        "tableobjectservice",
        "checkpointservice",
        "databasemanifestservice",
        "tablemanifestservice",
        "delete_covered_segments(",
        "persist_flush_watermark(",
        "prune_snapshots(",
        "compact_branch_tables(",
        "materialize_inherited_layer(",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

fn contains_forbidden_maintenance_executor_dependency(line: &str) -> bool {
    let lower = line.to_ascii_lowercase();
    [
        "tableobjectservice",
        "tableobjectreaderservice",
        "databasemanifestservice",
        "checkpointservice",
        "walservice",
        "quarantineservice",
        "lifecycledurablelocalruntime",
        "flush_durable_branch(",
        "checkpoint_durable_branch(",
        "compact_durable_branch(",
        "materialize_durable_branch(",
        "publish_create(",
        "delete_covered_segments(",
        "persist_flush_watermark(",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

fn contains_forbidden_durable_maintenance_dependency(line: &str) -> bool {
    let lower = line.to_ascii_lowercase();
    [
        "databasemanifestservice::new",
        "walservice::open",
        "tablemanifestservice::new",
        "snapshotservice::new",
        "tableobjectservice::new",
        "checkpointservice::new",
        "quarantineservice::new",
        "commitreplayruntime",
        "complete_recovery(",
        "recover(",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

fn contains_forbidden_durable_close_dependency(line: &str) -> bool {
    let lower = line.to_ascii_lowercase();
    [
        "databasemanifestservice::new",
        "walservice::open",
        "tablemanifestservice::new",
        "snapshotservice::new",
        "tableobjectservice::new",
        "checkpointservice::new",
        "quarantineservice::new",
        "commitreplayruntime",
        "complete_recovery(",
        "recover(",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

fn contains_forbidden_bootstrap_maintenance_dependency(line: &str) -> bool {
    let lower = line.to_ascii_lowercase();
    [
        "flush_frozen(",
        "run_next_flush_maintenance(",
        "run_next_checkpoint_maintenance(",
        "run_next_wal_truncation_maintenance(",
        "run_next_compaction_maintenance(",
        "run_next_materialization_maintenance(",
        "compact_branch_tables(",
        "materialize_inherited_layer(",
        "persist_flush_watermark(",
        "truncate_wal(",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

fn contains_forbidden_bootstrap_close_dependency(line: &str) -> bool {
    // `try_begin_quiesce(` is intentionally NOT in this list. Quiesce is
    // a shared primitive used by checkpoint, durable close, and the five
    // branch-lifecycle wrappers (clear, delete, three fork variants).
    // Bootstrap is allowed to invoke it directly.
    let lower = line.to_ascii_lowercase();
    [
        "closeoutcome",
        "closeoutcomeeffects",
        "closeoutcomestatus",
        "closephase",
        "drain_for_close(",
        "cancel_pending_for_close(",
        "wal_mut().close(",
        "release_writer_guard(",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

fn contains_forbidden_cache_close_dependency(line: &str) -> bool {
    let lower = line.to_ascii_lowercase();
    [
        "wal_mut().close(",
        "release_writer_guard(",
        "databasemanifestservice",
        "walservice",
        "snapshotservice",
        "tablemanifestservice",
        "quarantineservice",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

fn uncommented_text(text: &str) -> String {
    let mut uncommented = String::new();
    for line in text.lines() {
        uncommented.push_str(line.split("//").next().unwrap_or_default());
        uncommented.push('\n');
    }
    uncommented
}

fn rust_function_source<'a>(text: &'a str, function_name: &str) -> Option<&'a str> {
    let marker = format!("fn {function_name}");
    let start = text.find(&marker)?;
    let rest = &text[start..];
    let end = rest.find("\n#[cfg").or_else(|| rest.find("\n#[test]"));
    Some(match end {
        Some(end) => &rest[..end],
        None => rest,
    })
}

fn is_public_surface_leak(line: &str) -> bool {
    let trimmed = line.trim_start();
    if trimmed.starts_with("pub(crate)") {
        return false;
    }
    [
        "pub struct ",
        "pub enum ",
        "pub trait ",
        "pub type ",
        "pub fn ",
        "pub const ",
        "pub static ",
        "pub mod ",
        "pub use ",
    ]
    .iter()
    .any(|needle| trimmed.starts_with(needle))
}

fn contains_ascii_word(line: &str, word: &str) -> bool {
    line.split(|character: char| !character.is_ascii_alphanumeric() && character != '_')
        .any(|token| token == word)
}
