//! #2690 — WAL segment-loss detection at full-runtime scale.
//!
//! These cases need sealed-but-uncheckpointed segments, which requires the
//! test-only tiny segment size (`with_wal_segment_size_for_test`) to force
//! real rotations at CI scale — hence they live in-crate rather than in the
//! public-API integration suite (whose sole-deletion cases need no knob).
//! The stages close with close-time reclaim DISABLED: a default clean close
//! checkpoints and truncates the WAL (space-reclamation contract §3.1,
//! slice 7), which would leave nothing uncheckpointed to lose.

#[cfg(test)]
#[cfg(all(feature = "localfs", not(target_arch = "wasm32")))]
mod tests {
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    use strata_core::BranchId;

    use crate::api::{
        CommitBatch, CommitMutation, CommitOptions, MaintenanceRequest, MaintenanceScope,
        MaintenanceSummaryStatus, MaintenanceTask, PointReadRequest, ReadBound, ReclaimBudget,
        StorageCloseOptions, StorageDurabilityPolicy, StorageKey, StorageOpenOptions,
        StorageRuntime, StorageSpaceId, StorageValue,
    };

    static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(0);

    fn temp_root(name: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "strata-wal-loss-{name}-{}-{}",
            std::process::id(),
            NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed)
        ));
        if path.exists() {
            std::fs::remove_dir_all(&path).expect("clear old temp dir");
        }
        path
    }

    fn options() -> StorageOpenOptions {
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
            .with_wal_segment_size_for_test(1024)
    }

    fn branch() -> BranchId {
        BranchId::from_bytes([0x01; BranchId::BYTE_LEN])
    }

    fn space() -> StorageSpaceId {
        StorageSpaceId::new(vec![0x20]).expect("engine space")
    }

    fn put(runtime: &mut StorageRuntime<'_>, index: u32) {
        let key = StorageKey::new(format!("acked-{index:04}").into_bytes()).expect("key");
        let batch = CommitBatch::new(
            branch(),
            vec![CommitMutation::Put {
                storage_space: space(),
                key,
                value: StorageValue::new(vec![b'v'; 96]),
                ttl: None,
            }],
            CommitOptions::default(),
        )
        .expect("commit batch");
        runtime.commit(&batch).expect("durable commit");
    }

    fn checkpoint(runtime: &mut StorageRuntime<'_>) {
        let summary = runtime
            .maintenance(&MaintenanceRequest::new(
                MaintenanceTask::Checkpoint,
                MaintenanceScope::Global,
            ))
            .expect("checkpoint request");
        assert_eq!(summary.status(), MaintenanceSummaryStatus::Completed);
    }

    fn wal_segment_paths(root: &Path) -> Vec<PathBuf> {
        let mut segments: Vec<PathBuf> = std::fs::read_dir(root.join("wal"))
            .expect("wal dir")
            .flatten()
            .map(|entry| entry.path())
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| {
                        name.strip_suffix(".object@").is_some_and(|id| {
                            id.len() == 16 && id.bytes().all(|byte| byte.is_ascii_hexdigit())
                        })
                    })
            })
            .collect();
        segments.sort();
        segments
    }

    /// Deleting the newest WAL segment — whose commits sit ABOVE the
    /// checkpoint — refuses: the close-time watermark attests versions
    /// neither the snapshot nor the surviving log can reproduce.
    #[test]
    fn deleting_uncheckpointed_tail_segment_refuses_open() {
        let root = temp_root("tail");
        {
            let mut runtime = StorageRuntime::open_durable_local_with_options(&root, options())
                .expect("durable open")
                .into_runtime();
            for index in 0u32..4 {
                put(&mut runtime, index);
            }
            checkpoint(&mut runtime);
            for index in 4u32..8 {
                put(&mut runtime, index);
            }
            runtime
                .close_with_options(
                    StorageCloseOptions::graceful().with_reclaim_budget(ReclaimBudget::Disabled),
                )
                .expect("clean close publishes the commit watermark");
        }

        let segments = wal_segment_paths(&root);
        assert!(
            segments.len() >= 2,
            "the tiny segment size must have rolled the log; found {}",
            segments.len()
        );
        std::fs::remove_file(segments.last().expect("tail segment")).expect("delete tail segment");

        let error = StorageRuntime::open_durable_local_with_options(&root, options())
            .expect_err("the tail held commits above the checkpoint watermark");
        assert_eq!(
            error.code(),
            "failed_precondition.storage_api.recovery_degraded",
            "uncheckpointed tail loss surfaces the recovery-degraded code"
        );
    }

    /// Lossy recovery (`with_strict_recovery(false)`) records the loss as a
    /// degradation instead of refusing: the operator explicitly chose
    /// salvage-what-remains, and the health surface must say what was lost.
    #[test]
    fn lossy_recovery_degrades_instead_of_refusing_on_watermark_loss() {
        let root = temp_root("lossy");
        {
            let mut runtime = StorageRuntime::open_durable_local_with_options(&root, options())
                .expect("durable open")
                .into_runtime();
            for index in 0u32..4 {
                put(&mut runtime, index);
            }
            checkpoint(&mut runtime);
            for index in 4u32..8 {
                put(&mut runtime, index);
            }
            runtime
                .close_with_options(
                    StorageCloseOptions::graceful().with_reclaim_budget(ReclaimBudget::Disabled),
                )
                .expect("clean close publishes the commit watermark");
        }

        let segments = wal_segment_paths(&root);
        assert!(segments.len() >= 2, "the stage must have rolled the log");
        std::fs::remove_file(segments.last().expect("tail segment")).expect("delete tail segment");

        let outcome = StorageRuntime::open_durable_local_with_options(
            &root,
            options().with_strict_recovery(false),
        )
        .expect("lossy recovery salvages the surviving prefix");
        assert_eq!(
            outcome.summary().recovery_health(),
            crate::api::RecoveryHealthSummary::Degraded,
            "the salvage must be reported as degraded, never healthy"
        );
    }

    /// The falsifier inverse: a segment whose data the checkpoint fully
    /// covers may legitimately vanish (retention would have trimmed it) —
    /// reopen succeeds with every row intact. This exact state
    /// false-positived the abandoned segment-id watermark design.
    #[test]
    fn deleting_checkpoint_covered_segment_still_opens_with_all_data() {
        let root = temp_root("covered");
        {
            let mut runtime = StorageRuntime::open_durable_local_with_options(&root, options())
                .expect("durable open")
                .into_runtime();
            for index in 0u32..8 {
                put(&mut runtime, index);
            }
            checkpoint(&mut runtime);
            runtime.close().expect("clean close");
        }

        // A decoy that only a broken name filter would classify as a segment:
        // 16 characters but not hex (kills the &&/|| filter mutation). It is
        // removed again before reopen — the production listing is stricter
        // still and refuses unparseable names under wal/ outright.
        let decoy = root.join("wal").join("zzzzzzzzzzzzzzzz.object@");
        std::fs::write(&decoy, b"junk").expect("write decoy");
        let filtered = wal_segment_paths(&root);
        assert!(
            filtered.iter().all(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| !name.starts_with('z'))
            }),
            "the decoy must never be classified as a WAL segment"
        );
        std::fs::remove_file(&decoy).expect("remove decoy");
        let segments = wal_segment_paths(&root);
        assert!(
            segments.len() >= 2,
            "the tiny segment size must have rolled the log; found {}",
            segments.len()
        );
        std::fs::remove_file(segments.first().expect("oldest segment"))
            .expect("delete old segment");

        let runtime = StorageRuntime::open_durable_local_with_options(&root, options())
            .expect("checkpoint-covered segment absence is recoverable")
            .into_runtime();
        for index in 0u32..8 {
            let key = StorageKey::new(format!("acked-{index:04}").into_bytes()).expect("key");
            let read = runtime
                .read_point(&PointReadRequest::new(
                    branch(),
                    space(),
                    key,
                    ReadBound::Latest,
                ))
                .expect("read succeeds");
            assert!(
                read.row().is_some(),
                "checkpoint-covered key {index} must survive"
            );
        }
    }

    // ---- #2768 (partial): the attested watermark record must survive ----

    fn put_on(
        runtime: &StorageRuntime<'_>,
        branch_id: BranchId,
        index: u32,
        value_len: usize,
    ) -> crate::api::StorageApiResult<crate::api::CommitSummary> {
        let key = StorageKey::new(format!("acked-{index:04}").into_bytes()).expect("key");
        let batch = CommitBatch::new(
            branch_id,
            vec![CommitMutation::Put {
                storage_space: space(),
                key,
                value: StorageValue::new(vec![b'v'; value_len]),
                ttl: None,
            }],
            CommitOptions::default(),
        )
        .expect("commit batch");
        runtime.commit(&batch)
    }

    fn assert_all_present(
        runtime: &StorageRuntime<'_>,
        branch_id: BranchId,
        indices: std::ops::Range<u32>,
    ) {
        for index in indices {
            let key = StorageKey::new(format!("acked-{index:04}").into_bytes()).expect("key");
            let read = runtime
                .read_point(&PointReadRequest::new(
                    branch_id,
                    space(),
                    key,
                    ReadBound::Latest,
                ))
                .expect("read succeeds");
            assert!(read.row().is_some(), "acked key {index} must survive");
        }
    }

    /// Stage: 4 puts, a checkpoint, then enough puts to seal several
    /// segments, and a CRASH (drop, no close): the durable watermark is the
    /// max commit of the newest SEALED segment, and the active segment above
    /// it holds later commits.
    fn stage_sealed_watermark_crash(root: &Path) -> usize {
        let mut runtime = StorageRuntime::open_durable_local_with_options(root, options())
            .expect("durable open")
            .into_runtime();
        for index in 0u32..4 {
            put(&mut runtime, index);
        }
        checkpoint(&mut runtime);
        for index in 4u32..24 {
            put(&mut runtime, index);
        }
        drop(runtime);
        let segments = wal_segment_paths(root);
        assert!(
            segments.len() >= 3,
            "the stage must seal several segments; found {}",
            segments.len()
        );
        segments.len()
    }

    /// #2768: deleting every segment below the active one — the newest
    /// sealed segment among them holds the record the durable watermark
    /// attests — leaves a contiguous inventory (only the active segment) whose
    /// surviving records sit ABOVE the watermark, so the #2690 max comparison
    /// passes. Recovery must still refuse: the attested record itself is
    /// unrecoverable. Before the fix this opened Healthy with acked keys gone.
    #[test]
    fn deleting_the_sealed_segment_holding_the_watermark_record_refuses_open() {
        let root = temp_root("watermark-record");
        stage_sealed_watermark_crash(&root);
        let segments = wal_segment_paths(&root);
        for segment in &segments[..segments.len() - 1] {
            std::fs::remove_file(segment).expect("delete sealed segment");
        }

        let error = StorageRuntime::open_durable_local_with_options(&root, options())
            .expect_err("the attested watermark record was deleted");
        assert_eq!(
            error.code(),
            "failed_precondition.storage_api.recovery_degraded",
            "a missing attested record surfaces the recovery-degraded code"
        );
    }

    /// Lossy recovery degrades (data loss recorded) instead of refusing.
    #[test]
    fn lossy_recovery_degrades_when_the_watermark_record_is_missing() {
        let root = temp_root("watermark-record-lossy");
        stage_sealed_watermark_crash(&root);
        let segments = wal_segment_paths(&root);
        for segment in &segments[..segments.len() - 1] {
            std::fs::remove_file(segment).expect("delete sealed segment");
        }

        let outcome = StorageRuntime::open_durable_local_with_options(
            &root,
            options().with_strict_recovery(false),
        )
        .expect("lossy recovery salvages");
        assert_eq!(
            outcome.summary().recovery_health(),
            crate::api::RecoveryHealthSummary::Degraded,
            "the salvage must be reported as degraded, never healthy"
        );
    }

    /// Direction control: the same crashed stage with nothing deleted opens
    /// Healthy with every acked key (the watermark record is present).
    #[test]
    fn crashed_stage_with_the_watermark_record_present_opens_healthy() {
        let root = temp_root("watermark-record-present");
        stage_sealed_watermark_crash(&root);
        let outcome = StorageRuntime::open_durable_local_with_options(&root, options())
            .expect("nothing was lost");
        assert_eq!(
            outcome.summary().recovery_health(),
            crate::api::RecoveryHealthSummary::Healthy
        );
        assert_all_present(&outcome.into_runtime(), branch(), 0..24);
    }

    /// Direction control: the clean close's seal + checkpoint + truncate
    /// (DUR-019) removes the segment holding the watermark record — the
    /// checkpoint covers it, so reopen is Healthy. Reopened twice so the
    /// close-left empty active segment is exercised as the whole log.
    #[test]
    fn close_checkpoint_seal_and_truncate_reopens_healthy() {
        let root = temp_root("watermark-close");
        {
            let mut runtime = StorageRuntime::open_durable_local_with_options(&root, options())
                .expect("durable open")
                .into_runtime();
            for index in 0u32..24 {
                put(&mut runtime, index);
            }
            runtime
                .close()
                .expect("clean close checkpoints and truncates");
        }
        for _ in 0..2 {
            let outcome = StorageRuntime::open_durable_local_with_options(&root, options())
                .expect("covered watermark record");
            assert_eq!(
                outcome.summary().recovery_health(),
                crate::api::RecoveryHealthSummary::Healthy
            );
            let mut runtime = outcome.into_runtime();
            assert_all_present(&runtime, branch(), 0..24);
            runtime
                .close_with_options(
                    StorageCloseOptions::graceful().with_reclaim_budget(ReclaimBudget::Disabled),
                )
                .expect("close without new writes");
        }
    }

    /// Direction control: two branches interleaving commits across sealed
    /// segments, a checkpoint in the middle, then a crash.
    #[test]
    fn multi_branch_crash_reopens_healthy() {
        let root = temp_root("watermark-multi-branch");
        let other = BranchId::from_bytes([0x02; BranchId::BYTE_LEN]);
        {
            let mut runtime = StorageRuntime::open_durable_local_with_options(&root, options())
                .expect("durable open")
                .into_runtime();
            runtime
                .branch(&crate::api::BranchRequest::new(
                    other,
                    crate::api::BranchAction::Create,
                    Some(crate::api::BranchGeneration::new(1)),
                ))
                .expect("create second branch");
            for index in 0u32..8 {
                put_on(&runtime, branch(), index, 96).expect("put main");
                put_on(&runtime, other, index, 96).expect("put other");
            }
            runtime
                .maintenance(&MaintenanceRequest::new(
                    MaintenanceTask::Checkpoint,
                    MaintenanceScope::Global,
                ))
                .expect("checkpoint");
            for index in 8u32..20 {
                put_on(&runtime, branch(), index, 96).expect("put main");
                put_on(&runtime, other, index, 96).expect("put other");
            }
            drop(runtime);
        }
        assert!(
            wal_segment_paths(&root).len() >= 3,
            "the stage must seal segments"
        );
        let outcome = StorageRuntime::open_durable_local_with_options(&root, options())
            .expect("nothing was lost");
        assert_eq!(
            outcome.summary().recovery_health(),
            crate::api::RecoveryHealthSummary::Healthy
        );
        let runtime = outcome.into_runtime();
        assert_all_present(&runtime, branch(), 0..20);
        assert_all_present(&runtime, other, 0..20);
    }

    /// Direction control: cache mode has no WAL and no watermark; it opens
    /// and serves writes unaffected.
    #[test]
    fn cache_mode_is_unaffected() {
        let runtime = StorageRuntime::open(StorageOpenOptions::cache())
            .expect("cache open")
            .into_runtime();
        let cache_branch = StorageRuntime::default_branch_id_for_test();
        for index in 0u32..8 {
            put_on(&runtime, cache_branch, index, 96).expect("cache put");
        }
        assert_all_present(&runtime, cache_branch, 0..8);
    }
}
