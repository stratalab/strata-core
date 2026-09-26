//! TCP4.6d — corpus harvest: real-engine artifacts as committed fuzz seeds.
//!
//! The corpus-recombination insight (`ClickHouse`'s AST fuzzer, `dbsqlfuzz`'s
//! seed pools): a mutator splicing fragments of REAL inputs out-yields one
//! mutating from scratch. libFuzzer already does the splicing (crossover
//! within each target's corpus); what it needs is real material. This module
//! supplies it: it drives an actual durable store through a scripted
//! lifecycle, then PROBES every on-disk object against every format decoder
//! and emits a seed for each accepting pair — real manifests, WAL segments,
//! snapshots, and table objects, exactly as the engine wrote them. Formats
//! with no standalone on-disk object (keys, rows, WAL records, headers) get
//! canonical constructor-built seeds instead.
//!
//! Probing routes through `decode_format_bytes`, so every emitted seed also
//! passes the 4.6b round-trip oracle at harvest time, and the committed grid
//! re-verifies per-PR: a committed seed that stops decoding is a violation
//! of the M3 format freeze, surfaced loudly. Nightly runs restore these
//! seeds alongside the compounding corpus cache (`fuzz.yml`), so discovered
//! inputs and committed real-artifact seeds recombine across nights.

use std::path::Path;

use strata_core::{BranchId, CommitVersion, Timestamp};

use super::format_fuzz::{decode_format_bytes, FormatDecodeOutcome, FormatDecoder};
use super::recovery_oracle::workload::{default_branch, oracle_key, oracle_space};
use crate::api::{
    CommitBatch, CommitMutation, CommitOptions, MaintenanceRequest, MaintenanceScope,
    MaintenanceTask, StorageDurabilityPolicy, StorageOpenOptions, StorageRuntime, StorageValue,
};
use crate::format::{
    encode_internal_key, encode_physical_key, encode_segment_metadata, encode_snapshot_watermark,
    encode_storage_row, encode_wal_commit_payload, encode_wal_record_into_reusing,
    encode_wal_segment_header, SegmentMetadata, SnapshotWatermark, WalCommitPayload, WalRecord,
    WalSegmentHeader,
};
use crate::row::{InternalKey, PhysicalKey, StorageRow, StorageSpaceId};
use crate::testkit::TestkitError;

/// One corpus seed: the fuzz target directory it belongs to, a stable-ish
/// name, and the bytes.
#[derive(Clone, Debug)]
pub struct HarvestSeed {
    pub target: &'static str,
    pub name: String,
    pub bytes: Vec<u8>,
}

/// The format fuzz targets and their decoder arms — the probe matrix.
pub(crate) const FORMAT_TARGETS: &[(&str, FormatDecoder)] = &[
    (
        "format_branch_catalog_manifest",
        FormatDecoder::BranchCatalogManifest,
    ),
    ("format_key", FormatDecoder::Key),
    ("format_manifest", FormatDecoder::Manifest),
    (
        "format_pending_releases_manifest",
        FormatDecoder::PendingReleasesManifest,
    ),
    ("format_quarantine", FormatDecoder::QuarantineInventory),
    (
        "format_retained_history_extension",
        FormatDecoder::RetainedHistoryExtensionPayload,
    ),
    ("format_segment_metadata", FormatDecoder::SegmentMetadata),
    ("format_snapshot_envelope", FormatDecoder::SnapshotEnvelope),
    (
        "format_snapshot_flushed_branches_payload",
        FormatDecoder::SnapshotFlushedBranchesPayload,
    ),
    (
        "format_snapshot_row_payload",
        FormatDecoder::SnapshotRowPayload,
    ),
    (
        "format_snapshot_timeline_payload",
        FormatDecoder::SnapshotTimelinePayload,
    ),
    ("format_storage_row", FormatDecoder::StorageRow),
    ("format_table_artifact", FormatDecoder::TableArtifact),
    ("format_table_block", FormatDecoder::TableBlock),
    (
        "format_table_block_trusted",
        FormatDecoder::TableBlockTrusted,
    ),
    (
        "format_table_block_indexed_seek",
        FormatDecoder::TableBlockIndexedSeek,
    ),
    ("format_table_manifest", FormatDecoder::TableManifest),
    ("format_wal_commit_payload", FormatDecoder::WalCommitPayload),
    ("format_wal_record", FormatDecoder::WalRecord),
    ("format_wal_segment_header", FormatDecoder::WalSegmentHeader),
    ("format_watermark", FormatDecoder::Watermark),
];

fn commit_rows(
    runtime: &StorageRuntime<'static>,
    branch: BranchId,
    mutations: Vec<CommitMutation>,
) -> Result<(), TestkitError> {
    let batch = CommitBatch::new(branch, mutations, CommitOptions::default())
        .map_err(|err| TestkitError::new(format!("harvest batch: {err:?}")))?;
    runtime
        .commit(&batch)
        .map(|_| ())
        .map_err(|err| TestkitError::new(format!("harvest commit: {err:?}")))
}

// Literal (key, fill, size) tables — value sizes span one byte to a few
// KiB so table blocks carry real variety for the block-family targets.
// Literals rather than arithmetic: the shape is arbitrary entropy, and
// computed variants would be unkillable mutation-gate surface.
const FIRST_ROUND_PUTS: &[(u8, u8, usize)] = &[
    (0, 0xC0, 1),
    (1, 0xC1, 801),
    (2, 0xC2, 1601),
    (3, 0xC3, 2401),
    (4, 0xC4, 3201),
    (5, 0xC5, 4001),
];

// Two flush rounds produce two real table objects, so per-family accept
// counters genuinely count (and the artifact corpus carries variety).
const FLUSH_ROUND_PUTS: &[&[(u8, u8, usize)]] = &[
    &[(8, 0xE8, 384), (9, 0xE9, 424)],
    &[(10, 0xEA, 464), (11, 0xEB, 504)],
];

/// Drives the real store whose artifacts become seeds: varied-size puts, a
/// delete, flush, and checkpoint, closed cleanly so every object is durable
/// and quiescent.
fn build_harvest_store(root: &Path) -> Result<(), TestkitError> {
    let options = StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard);
    let outcome = StorageRuntime::open_durable_local_with_options(root.to_path_buf(), options)
        .map_err(|err| TestkitError::new(format!("harvest open: {err:?}")))?;
    let mut runtime = outcome.into_runtime();
    let branch = default_branch();

    for &(key, fill, size) in FIRST_ROUND_PUTS {
        commit_rows(
            &runtime,
            branch,
            vec![CommitMutation::Put {
                storage_space: oracle_space(),
                key: oracle_key(key),
                value: StorageValue::new(vec![fill; size]),
                ttl: None,
            }],
        )?;
    }
    commit_rows(
        &runtime,
        branch,
        vec![CommitMutation::Delete {
            storage_space: oracle_space(),
            key: oracle_key(2),
        }],
    )?;

    // Two flush rounds produce two real table objects, so per-family accept
    // counters genuinely count.
    for round_puts in FLUSH_ROUND_PUTS {
        for &(key, fill, size) in *round_puts {
            commit_rows(
                &runtime,
                branch,
                vec![CommitMutation::Put {
                    storage_space: oracle_space(),
                    key: oracle_key(key),
                    value: StorageValue::new(vec![fill; size]),
                    ttl: None,
                }],
            )?;
        }
        for task in [MaintenanceTask::Flush, MaintenanceTask::Checkpoint] {
            let request = MaintenanceRequest::new(task, MaintenanceScope::Branch(branch));
            // The manual drain can lose the enqueued task to the runtime's
            // background lane (`MaintenanceRejected` "no longer startable" —
            // the dual-mutation harness's scheduling race). Retry, then
            // tolerate: the graceful close below joins the background lane,
            // so the work lands before harvesting either way.
            for _ in 0..4u8 {
                match runtime
                    .enqueue_maintenance(&request)
                    .and_then(|_| runtime.drain_maintenance())
                {
                    Ok(_) => break,
                    // Retry; a rejection on the final try falls out of the
                    // loop tolerated (the close joins the background lane).
                    Err(crate::api::StorageApiError::MaintenanceRejected { .. }) => {}
                    Err(err) => {
                        return Err(TestkitError::new(format!("harvest maintenance: {err:?}")));
                    }
                }
            }
        }
    }
    runtime
        .close()
        .map_err(|err| TestkitError::new(format!("harvest close: {err:?}")))?;
    Ok(())
}

/// Sorted recursive listing of `(relative path, bytes)` for every regular
/// file under `root`.
fn list_store_files(root: &Path) -> Result<Vec<(String, Vec<u8>)>, TestkitError> {
    let mut files = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let entries = std::fs::read_dir(&dir)
            .map_err(|err| TestkitError::new(format!("harvest list {}: {err}", dir.display())))?;
        for entry in entries {
            let entry = entry.map_err(|err| TestkitError::new(format!("harvest entry: {err}")))?;
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.is_file() {
                let relative = path
                    .strip_prefix(root)
                    .map_err(|err| TestkitError::new(format!("harvest prefix: {err}")))?
                    .to_string_lossy()
                    .into_owned();
                let bytes = std::fs::read(&path)
                    .map_err(|err| TestkitError::new(format!("harvest read: {err}")))?;
                files.push((relative, bytes));
            }
        }
    }
    files.sort();
    Ok(files)
}

/// The canonical constructor-built seeds for formats without a standalone
/// on-disk object. Every one MUST decode through its arm — a refusal here is
/// a harvest bug, surfaced loudly.
fn built_seeds() -> Result<Vec<HarvestSeed>, TestkitError> {
    let branch = BranchId::from_bytes([0x44; BranchId::BYTE_LEN]);
    let space =
        StorageSpaceId::engine(0x20).map_err(|err| TestkitError::new(format!("space: {err:?}")))?;
    let physical = PhysicalKey::new(branch, "default", space, b"harvest-key".to_vec())
        .map_err(|err| TestkitError::new(format!("physical key: {err:?}")))?;
    let row = StorageRow::put(
        physical.clone(),
        CommitVersion::new(7),
        Timestamp::from_micros(1_700_000_000_000_000),
        Timestamp::EPOCH,
        b"harvest-value".to_vec(),
    );
    let tombstone = StorageRow::tombstone(
        physical.clone(),
        CommitVersion::new(8),
        Timestamp::from_micros(1_700_000_000_000_001),
    );
    let record = WalRecord::new(
        CommitVersion::new(7),
        branch,
        Timestamp::from_micros(1_700_000_000_000_000),
        WalCommitPayload::new(vec![row.clone()])
            .map_err(|err| TestkitError::new(format!("payload: {err:?}")))?,
    )
    .map_err(|err| TestkitError::new(format!("record: {err:?}")))?;

    let seeds = vec![
        HarvestSeed {
            target: "format_key",
            name: "built-physical-key".to_owned(),
            bytes: encode_physical_key(&physical),
        },
        HarvestSeed {
            target: "format_key",
            name: "built-internal-key".to_owned(),
            bytes: encode_internal_key(&InternalKey::new(physical, CommitVersion::new(7))),
        },
        HarvestSeed {
            target: "format_storage_row",
            name: "built-put-row".to_owned(),
            bytes: encode_storage_row(&row)
                .map_err(|err| TestkitError::new(format!("encode row: {err:?}")))?,
        },
        HarvestSeed {
            target: "format_storage_row",
            name: "built-tombstone-row".to_owned(),
            bytes: encode_storage_row(&tombstone)
                .map_err(|err| TestkitError::new(format!("encode tombstone: {err:?}")))?,
        },
        HarvestSeed {
            target: "format_wal_record",
            name: "built-one-put-record".to_owned(),
            bytes: {
                let (mut bytes, mut payload, mut rows) = (Vec::new(), Vec::new(), Vec::new());
                encode_wal_record_into_reusing(&record, &mut bytes, &mut payload, &mut rows)
                    .map_err(|err| TestkitError::new(format!("encode record: {err:?}")))?;
                bytes
            },
        },
        HarvestSeed {
            target: "format_wal_commit_payload",
            name: "built-put-and-tombstone".to_owned(),
            bytes: encode_wal_commit_payload(
                &WalCommitPayload::new(vec![row, tombstone])
                    .map_err(|err| TestkitError::new(format!("payload: {err:?}")))?,
            )
            .map_err(|err| TestkitError::new(format!("encode payload: {err:?}")))?,
        },
        HarvestSeed {
            target: "format_wal_segment_header",
            name: "built-header".to_owned(),
            bytes: encode_wal_segment_header(&WalSegmentHeader::new(5, [0x22; 16])),
        },
        HarvestSeed {
            target: "format_segment_metadata",
            name: "built-empty-segment".to_owned(),
            bytes: encode_segment_metadata(&SegmentMetadata::empty(9)),
        },
        HarvestSeed {
            target: "format_watermark",
            name: "built-present-watermark".to_owned(),
            // The snapshot watermark is a manifest-internal payload — no
            // standalone object exists to harvest.
            bytes: encode_snapshot_watermark(SnapshotWatermark::Present {
                snapshot_id: 7,
                watermark_commit_version: CommitVersion::new(9),
                updated_at: Timestamp::from_micros(1_700_000_000_000_000),
            })
            .map_err(|err| TestkitError::new(format!("encode watermark: {err:?}")))?,
        },
    ];
    verify_built_seeds(&seeds)?;
    Ok(seeds)
}

/// Every constructor-built seed MUST decode through its arm — a refusal is a
/// harvest bug, surfaced loudly.
fn verify_built_seeds(seeds: &[HarvestSeed]) -> Result<(), TestkitError> {
    for seed in seeds {
        let decoder = FORMAT_TARGETS
            .iter()
            .find(|(target, _)| *target == seed.target)
            .map(|(_, decoder)| *decoder)
            .ok_or_else(|| TestkitError::new(format!("unmapped target {}", seed.target)))?;
        if decode_format_bytes(decoder, &seed.bytes) != FormatDecodeOutcome::Accepted {
            return Err(TestkitError::new(format!(
                "built seed {}/{} does not decode",
                seed.target, seed.name
            )));
        }
    }
    Ok(())
}

/// Harvests the full seed set: build a real store under `root`, probe every
/// on-disk object against every format decoder (emitting a seed per
/// accepting pair — the round-trip oracle runs inside the probe), then
/// append the constructor-built seeds and the layout/service extras.
pub fn harvest_format_corpus(root: &Path) -> Result<Vec<HarvestSeed>, TestkitError> {
    build_harvest_store(root)?;
    let files = list_store_files(root)?;

    let mut seeds = Vec::new();
    for (target, decoder) in FORMAT_TARGETS {
        let mut accepted = 0usize;
        for (relative, bytes) in &files {
            if decode_format_bytes(*decoder, bytes) == FormatDecodeOutcome::Accepted {
                let family = relative.split('/').next().unwrap_or("root").to_owned();
                seeds.push(HarvestSeed {
                    target,
                    name: format!("harvest-{family}-{accepted}"),
                    bytes: bytes.clone(),
                });
                accepted += 1;
            }
        }
    }
    seeds.extend(built_seeds()?);

    // Layout targets: the harvested store's own relative paths are real
    // object names — exactly what the classifier sees during recovery.
    for (index, (relative, _)) in files
        .iter()
        .filter(|(relative, _)| relative.ends_with(".object@"))
        .enumerate()
        .take(6)
    {
        seeds.push(HarvestSeed {
            target: "layout_object_name",
            name: format!("harvest-name-{index}"),
            bytes: relative.clone().into_bytes(),
        });
    }
    let mut id_seed = 1u64.to_le_bytes().to_vec();
    id_seed.extend_from_slice(&u64::MAX.to_le_bytes());
    seeds.push(HarvestSeed {
        target: "layout_id_roundtrip",
        name: "built-two-ids".to_owned(),
        bytes: id_seed,
    });
    seeds.push(HarvestSeed {
        target: "service_snapshot",
        name: "built-basic".to_owned(),
        bytes: (0u8..16).collect(),
    });
    Ok(seeds)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    #[test]
    fn a_non_decoding_built_seed_is_refused() {
        let bogus = HarvestSeed {
            target: "format_manifest",
            name: "built-bogus".to_owned(),
            bytes: vec![0xFF; 8],
        };
        assert!(
            verify_built_seeds(std::slice::from_ref(&bogus)).is_err(),
            "a built seed that does not decode must red the harvester"
        );
    }

    /// The committed-seed drift gate: every format seed the fuzz crate's
    /// gitignore allowlists must decode `Accepted` through its target's arm
    /// (which also runs the 4.6b round-trip oracle). A red here means a
    /// committed seed stopped decoding — an M3 format-freeze violation.
    #[test]
    fn committed_format_corpus_seeds_decode_and_roundtrip() {
        let fuzz_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fuzz");
        let allowlist = std::fs::read_to_string(fuzz_root.join(".gitignore")).expect("gitignore");
        let mut verified = 0usize;
        for line in allowlist.lines() {
            let Some(entry) = line.strip_prefix("!/corpus/") else {
                continue;
            };
            let Some((dir, file)) = entry.split_once('/') else {
                continue; // a directory allowlist line, not a seed
            };
            if file.is_empty() {
                continue;
            }
            let path = fuzz_root.join("corpus").join(dir).join(file);
            let bytes = std::fs::read(&path).unwrap_or_else(|err| {
                panic!("allowlisted seed missing: {}: {err}", path.display())
            });
            assert!(
                !bytes.is_empty(),
                "empty committed seed: {}",
                path.display()
            );
            let valid_intent = ["valid-", "harvest-", "built-"]
                .iter()
                .any(|prefix| file.starts_with(prefix));
            if let Some((_, decoder)) = FORMAT_TARGETS.iter().find(|(target, _)| *target == dir) {
                if valid_intent {
                    assert_eq!(
                        decode_format_bytes(*decoder, &bytes),
                        FormatDecodeOutcome::Accepted,
                        "committed valid-intent seed no longer decodes: {}",
                        path.display()
                    );
                    verified += 1;
                }
            }
        }
        assert!(
            verified >= 32,
            "the format seed grid shrank to {verified} — committed seeds went missing"
        );
    }

    /// The harvester itself works end to end: a fresh store yields real
    /// seeds for the load-bearing families, and every built seed decodes.
    /// With `STRATA_CORPUS_HARVEST=1` the seeds are also written into the
    /// fuzz corpus tree for review and commit (the regeneration path).
    #[test]
    fn harvest_yields_real_seeds_for_the_load_bearing_families() {
        let dir = tempfile::tempdir().expect("tmp");
        let seeds = harvest_format_corpus(dir.path()).expect("harvest");
        for required in [
            "format_manifest",
            "format_watermark",
            "format_snapshot_envelope",
            "format_table_manifest",
            "format_key",
            "format_storage_row",
            "format_wal_record",
            "format_wal_segment_header",
            "format_segment_metadata",
            "layout_object_name",
        ] {
            assert!(
                seeds.iter().any(|seed| seed.target == required),
                "no seed harvested for {required}: {:?}",
                seeds
                    .iter()
                    .map(|seed| (seed.target, seed.name.clone()))
                    .collect::<Vec<_>>()
            );
        }

        // Every harvested format seed must itself decode Accepted (an
        // inverted probe emits rejecting bytes), seed names must be unique
        // per target (a dead accept counter collides them), and the second
        // flush round must yield a second real table artifact.
        let mut names = std::collections::BTreeSet::new();
        for seed in &seeds {
            assert!(
                names.insert((seed.target, seed.name.clone())),
                "duplicate seed name: {}/{}",
                seed.target,
                seed.name
            );
            if let Some((_, decoder)) = FORMAT_TARGETS
                .iter()
                .find(|(target, _)| *target == seed.target)
            {
                assert_eq!(
                    decode_format_bytes(*decoder, &seed.bytes),
                    FormatDecodeOutcome::Accepted,
                    "harvested seed does not decode: {}/{}",
                    seed.target,
                    seed.name
                );
            }
        }
        assert!(
            seeds
                .iter()
                .filter(|seed| seed.target == "format_table_artifact")
                .count()
                >= 2,
            "the two flush rounds must yield two table artifacts"
        );

        if std::env::var("STRATA_CORPUS_HARVEST").is_ok() {
            let corpus = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fuzz/corpus");
            for seed in &seeds {
                let dir = corpus.join(seed.target);
                std::fs::create_dir_all(&dir).expect("corpus dir");
                std::fs::write(dir.join(&seed.name), &seed.bytes).expect("write seed");
            }
            eprintln!("wrote {} seeds into {}", seeds.len(), corpus.display());
        }
    }
}
