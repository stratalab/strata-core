use std::error::Error;
use std::fmt;
#[cfg(feature = "localfs")]
use std::path::Path;
#[cfg(feature = "localfs")]
use std::path::PathBuf;

use super::*;

#[cfg(feature = "perf-trace")]
mod admission;
mod background;
#[cfg(any(feature = "localfs", feature = "perf-trace"))]
mod background_scale;
mod branch;
mod cache;
#[cfg(all(unix, feature = "localfs", feature = "perf-trace"))]
mod checkpoint;
mod commit;
#[cfg(feature = "localfs")]
mod concurrent_history;
mod diagnostics;
#[cfg(all(feature = "localfs", feature = "perf-trace"))]
mod disk_resident_reads;
#[cfg(feature = "localfs")]
mod fork_reopen;
mod liveness_matrix;
mod maintenance;
mod off_lock_concurrency;
mod off_lock_interleaving;
mod off_lock_perf;
mod open_close;
mod open_options;
#[cfg(all(feature = "localfs", feature = "perf-trace"))]
mod preheat;
mod read;
mod source_guards;
#[cfg(feature = "localfs")]
mod stress_random;
mod surface;

/// Concatenated source of the split runtime module, for source-introspection guards
/// that assert on the runtime implementation as a whole.
const RUNTIME_SOURCE: &str = concat!(
    include_str!("../runtime/mod.rs"),
    include_str!("../runtime/background.rs"),
    include_str!("../runtime/data.rs"),
    include_str!("../runtime/diagnostics.rs"),
    include_str!("../runtime/error.rs"),
    include_str!("../runtime/maintenance.rs"),
    include_str!("../runtime/open_close.rs"),
);

fn assert_result_type<T>(result: StorageApiResult<T>) -> StorageApiResult<T> {
    result
}

fn branch_id(byte: u8) -> BranchId {
    BranchId::from_bytes([byte; BranchId::BYTE_LEN])
}

fn space() -> StorageSpaceId {
    StorageSpaceId::new(b"space".to_vec()).expect("valid storage space")
}

fn key(name: &[u8]) -> StorageKey {
    StorageKey::new(name.to_vec()).expect("valid key")
}

fn background_space() -> StorageSpaceId {
    StorageSpaceId::new(vec![0x20]).expect("engine storage space")
}

fn background_put_batch(name: &[u8], value: Vec<u8>) -> CommitBatch {
    CommitBatch::new(
        StorageRuntime::default_branch_id_for_test(),
        vec![CommitMutation::Put {
            storage_space: background_space(),
            key: key(name),
            value: StorageValue::new(value),
            ttl: None,
        }],
        CommitOptions::default(),
    )
    .expect("valid background put batch")
}

#[cfg(feature = "perf-trace")]
fn background_put_batch_range(
    key_prefix: &str,
    start: usize,
    end: usize,
    value: &[u8],
) -> CommitBatch {
    let mut mutations = Vec::with_capacity(end.saturating_sub(start));
    for index in start..end {
        let key_string = format!("{key_prefix}{index:08}");
        mutations.push(CommitMutation::Put {
            storage_space: background_space(),
            key: key(key_string.as_bytes()),
            value: StorageValue::new(value.to_vec()),
            ttl: None,
        });
    }
    CommitBatch::new(
        StorageRuntime::default_branch_id_for_test(),
        mutations,
        CommitOptions::default().require_conflict_check(false),
    )
    .expect("valid background put batch range")
}

fn default_background_worker_count() -> usize {
    StorageBackgroundMaintenanceOptions::product_default().worker_count()
}

#[cfg_attr(not(feature = "perf-trace"), allow(dead_code))]
fn default_terminal_nonzero_level() -> u8 {
    let max_level_count = crate::branch::config::BranchRuntimeConfig::default().max_level_count();
    u8::try_from(max_level_count.saturating_sub(1)).expect("configured level fits in u8")
}

#[cfg(feature = "perf-trace")]
const SCALED_COMPACTION_AMPLIFICATION_GATE: u128 = 4;

#[cfg(feature = "perf-trace")]
const SCALED_CLOSED_LOOP_CACHE_ROWS: usize = 50_000;
#[cfg(feature = "perf-trace")]
const SCALED_CLOSED_LOOP_CACHE_BATCH_SIZE: usize = 1_000;
#[cfg(feature = "perf-trace")]
const SCALED_CLOSED_LOOP_CACHE_VALUE_BYTES: usize = 150;
#[cfg(all(feature = "localfs", feature = "perf-trace"))]
const SCALED_CLOSED_LOOP_DURABLE_ROWS: usize = 160;
#[cfg(all(feature = "localfs", feature = "perf-trace"))]
const SCALED_CLOSED_LOOP_DURABLE_VALUE_BYTES: usize = 256;

#[cfg(feature = "perf-trace")]
fn assert_scaled_compaction_amplification_below_gate(
    perf: &crate::observability::perf_trace::StoragePerfSnapshot,
    logical_rows: u64,
    logical_bytes: u64,
    context: &str,
) {
    let input_rows = u128::from(perf.lifecycle_compaction_input_rows());
    let input_bytes = u128::from(perf.lifecycle_compaction_input_bytes());
    let logical_rows = u128::from(logical_rows);
    let logical_bytes = u128::from(logical_bytes);
    let row_limit = logical_rows.saturating_mul(SCALED_COMPACTION_AMPLIFICATION_GATE);
    let byte_limit = logical_bytes.saturating_mul(SCALED_COMPACTION_AMPLIFICATION_GATE);
    let row_amp_millix = if logical_rows == 0 {
        0
    } else {
        input_rows.saturating_mul(1_000) / logical_rows
    };
    let byte_amp_millix = if logical_bytes == 0 {
        0
    } else {
        input_bytes.saturating_mul(1_000) / logical_bytes
    };

    assert!(
        input_rows <= row_limit,
        "{context} exceeded scaled row rewrite amplification gate: input_rows={input_rows}, logical_rows={logical_rows}, amp_millix={row_amp_millix}, gate={}x, operations={}, l0_ops={}, l0_to_l1_ops={}, nonzero_ops={}, bottommost_ops={}, input_tables={}, overlap_tables={}, output_tables={}, nonzero_input_rows={}, nonzero_input_bytes={}, metadata_bytes_avoided={}, selected={}, resubmits={}",
        SCALED_COMPACTION_AMPLIFICATION_GATE,
        perf.lifecycle_compaction_operations_completed(),
        perf.lifecycle_compaction_l0_operations(),
        perf.lifecycle_compaction_l0_to_level_one_operations(),
        perf.lifecycle_compaction_nonzero_operations(),
        perf.lifecycle_compaction_bottommost_operations(),
        perf.lifecycle_compaction_input_tables(),
        perf.lifecycle_compaction_overlap_tables(),
        perf.lifecycle_compaction_output_tables(),
        perf.lifecycle_compaction_nonzero_input_rows(),
        perf.lifecycle_compaction_nonzero_input_bytes(),
        perf.lifecycle_compaction_metadata_bytes_avoided(),
        perf.lifecycle_compaction_selected(),
        perf.lifecycle_compaction_resubmits()
    );
    assert!(
        input_bytes <= byte_limit,
        "{context} exceeded scaled byte rewrite amplification gate: input_bytes={input_bytes}, logical_bytes={logical_bytes}, amp_millix={byte_amp_millix}, gate={}x, operations={}, l0_ops={}, l0_to_l1_ops={}, nonzero_ops={}, bottommost_ops={}, input_tables={}, overlap_tables={}, output_tables={}, nonzero_input_rows={}, nonzero_input_bytes={}, metadata_bytes_avoided={}, selected={}, resubmits={}",
        SCALED_COMPACTION_AMPLIFICATION_GATE,
        perf.lifecycle_compaction_operations_completed(),
        perf.lifecycle_compaction_l0_operations(),
        perf.lifecycle_compaction_l0_to_level_one_operations(),
        perf.lifecycle_compaction_nonzero_operations(),
        perf.lifecycle_compaction_bottommost_operations(),
        perf.lifecycle_compaction_input_tables(),
        perf.lifecycle_compaction_overlap_tables(),
        perf.lifecycle_compaction_output_tables(),
        perf.lifecycle_compaction_nonzero_input_rows(),
        perf.lifecycle_compaction_nonzero_input_bytes(),
        perf.lifecycle_compaction_metadata_bytes_avoided(),
        perf.lifecycle_compaction_selected(),
        perf.lifecycle_compaction_resubmits()
    );
}

#[cfg(feature = "perf-trace")]
fn assert_background_closed_loop_reads(
    runtime: &StorageRuntime<'_>,
    key_prefix: &str,
    expected_rows: usize,
    expected_value: &[u8],
) {
    for index in [
        0,
        expected_rows / 3,
        (expected_rows * 2) / 3,
        expected_rows - 1,
    ] {
        let key_string = format!("{key_prefix}{index:08}");
        let point = runtime
            .read_point(&PointReadRequest::new(
                StorageRuntime::default_branch_id_for_test(),
                background_space(),
                key(key_string.as_bytes()),
                ReadBound::Latest,
            ))
            .unwrap_or_else(|error| {
                panic!("background closed-loop point read {key_string} failed: {error}")
            });
        let row = point
            .row()
            .unwrap_or_else(|| panic!("background closed-loop point read {key_string} missed"));
        assert_eq!(
            row.value().map(StorageValue::as_bytes),
            Some(expected_value),
            "background closed-loop point read {key_string} returned the wrong value"
        );
    }

    let scan = runtime
        .scan_prefix(&PrefixScanReadRequest::new(
            StorageRuntime::default_branch_id_for_test(),
            background_space(),
            key(key_prefix.as_bytes()),
            ReadBound::Latest,
            None,
        ))
        .expect("background closed-loop prefix scan");
    assert_eq!(
        scan.rows().len(),
        expected_rows,
        "background closed-loop prefix scan returned the wrong row count"
    );
    let mut previous_key = None;
    for row in scan.rows() {
        assert_eq!(
            row.value().map(StorageValue::as_bytes),
            Some(expected_value),
            "background closed-loop prefix scan returned the wrong value for {:?}",
            row.key()
        );
        if let Some(previous) = previous_key {
            assert!(
                previous < row.key().as_bytes(),
                "background closed-loop prefix scan returned unsorted rows"
            );
        }
        previous_key = Some(row.key().as_bytes());
    }
}

fn background_raw_row(name: &[u8], version: u64) -> crate::row::StorageRow {
    background_raw_row_with_value(name, version, b"value".to_vec())
}

fn background_raw_row_with_value(
    name: &[u8],
    version: u64,
    value: Vec<u8>,
) -> crate::row::StorageRow {
    let physical_key = crate::row::PhysicalKey::new(
        StorageRuntime::default_branch_id_for_test(),
        "api",
        crate::row::StorageSpaceId::engine(0x20).expect("engine-owned row storage space"),
        name,
    )
    .expect("valid raw row key");
    crate::row::StorageRow::put(
        physical_key,
        CommitVersion::new(version),
        Timestamp::from_micros(version),
        Timestamp::EPOCH,
        value,
    )
}

fn background_owned_table_count_at(
    layout: &crate::branch::facts::BranchSourceLayout,
    level: u8,
) -> usize {
    if level == 0 {
        return layout.owned_l0_tables();
    }
    layout
        .owned_nonzero_level_table_counts()
        .iter()
        .find(|count| count.level().raw() == level)
        .map_or(0, |count| count.table_count())
}

#[cfg(feature = "localfs")]
fn temp_dir_for_api_test(name: &str) -> PathBuf {
    static NEXT_TEMP_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    let mut path = std::env::temp_dir();
    path.push(format!(
        "strata-storage-api-{name}-{}-{}",
        std::process::id(),
        NEXT_TEMP_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    if path.exists() {
        std::fs::remove_dir_all(&path).expect("clear old temp dir");
    }
    path
}

/// Reopens a path whose previous runtime was DROPPED rather than closed.
///
/// A drop bounds background shutdown to a 250 ms quiesce window and detaches
/// a worker that misses it; the detached worker keeps the writer lock until
/// its task finishes, which on a loaded runner can be seconds, so an
/// immediate same-process reopen is refused with `WriterLockHeld` (#2837,
/// #3546; the product-contract question is #3694). Many tests drop on
/// purpose to simulate a crash, so the fix belongs here, not in a `close()`:
/// the reopen retries ONLY the writer-lock refusal, within the testkit's
/// wall-clock deadline, and returns every other error unchanged at once.
///
/// A reopen that follows an explicit `close()` never needs this — close
/// releases the writer lock before it returns.
#[cfg(feature = "localfs")]
fn reopen_after_drop<T>(open: impl FnMut() -> StorageApiResult<T>) -> StorageApiResult<T> {
    crate::testkit::reopen_retry::open_with_retry_on_unavailable(open)
}

#[cfg(feature = "localfs")]
fn wal_segment_file_count(root: &Path) -> usize {
    std::fs::read_dir(root.join("wal")).map_or(0, |entries| {
        entries
            .filter_map(Result::ok)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .is_some_and(|name| name.ends_with(".object@"))
            })
            .count()
    })
}

#[derive(Debug)]
struct SourceError;

impl fmt::Display for SourceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("source failure")
    }
}

impl Error for SourceError {}

#[derive(Debug)]
struct PayloadSourceError;

impl fmt::Display for PayloadSourceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("source leaked secret-payload [1, 2, 3]")
    }
}

impl Error for PayloadSourceError {}
