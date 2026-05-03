//! Decode `LoadedSnapshot` sections and install them into `SegmentedStore`.
//!
//! This is the engine-side inverse of `compaction::collect_checkpoint_data`.
//! It walks the per-primitive sections of a `LoadedSnapshot`, deserializes
//! each section via [`SnapshotSerializer`], stages generic decoded row groups,
//! and hands those groups to storage's decoded-row install helper.
//!
//! Recovery calls this from the `on_snapshot` callback passed to
//! `RecoveryCoordinator::recover` so checkpoint-only restart (no WAL covering
//! some pre-snapshot range) produces the same observable state as the
//! original commits. The T3-E5 follow-up made snapshot install
//! retention-complete: tombstones are installed as `DecodedSnapshotValue::
//! Tombstone` so deletes survive checkpoint+compact+reopen; KV TTL carries
//! through; and the Branch section is dispatched via the new `branch_id`
//! field on `BranchSnapshotEntry`.
//!
//! Graph-primitive standalone sections are still skipped — today
//! `collect_checkpoint_data` emits Graph entries inside the KV section with
//! `type_tag = TypeTag::Graph`, and the KV decoder below routes them to the
//! correct storage type. A future standalone Graph section would need its
//! own decoder.

use std::collections::HashMap;

use crate::{StrataError, StrataResult};
use strata_core::id::CommitVersion;
use strata_core::BranchId;
use strata_core::Value;
use strata_storage::durability::codec::StorageCodec;
use strata_storage::durability::format::primitive_tags;
use strata_storage::durability::{
    install_decoded_snapshot_rows, LoadedSnapshot, SnapshotSerializer,
    StorageDecodedSnapshotInstallError, StorageDecodedSnapshotInstallGroup,
    StorageDecodedSnapshotInstallInput, StorageDecodedSnapshotInstallPlan,
};
use strata_storage::{
    DecodedSnapshotEntry, DecodedSnapshotValue, SegmentedStore, StorageError, TypeTag,
};
use tracing::warn;

/// Counts of entries installed per primitive during a snapshot install.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct InstallStats {
    /// KV entries installed (TypeTag::KV). Includes tombstones.
    pub kv: usize,
    /// Graph entries installed (TypeTag::Graph) — routed through the KV section.
    pub graph: usize,
    /// Event entries installed.
    pub events: usize,
    /// JSON entries installed. Includes tombstones.
    pub json: usize,
    /// Vector collection configs installed (the `__config__/{name}` rows).
    pub vector_configs: usize,
    /// Individual vector records installed. Includes tombstones.
    pub vectors: usize,
    /// Branch metadata entries installed.
    pub branches: usize,
    /// Sections that were present in the snapshot but intentionally skipped
    /// (standalone Graph section when non-empty — today KV-section routing
    /// handles all graph entries).
    pub sections_skipped: usize,
}

impl InstallStats {
    /// Total number of logical entries written into storage.
    pub(crate) fn total_installed(&self) -> usize {
        self.kv
            + self.graph
            + self.events
            + self.json
            + self.vector_configs
            + self.vectors
            + self.branches
    }

    fn add_installed(&mut self, other: &InstallStats) {
        self.kv += other.kv;
        self.graph += other.graph;
        self.events += other.events;
        self.json += other.json;
        self.vector_configs += other.vector_configs;
        self.vectors += other.vectors;
        self.branches += other.branches;
        self.sections_skipped += other.sections_skipped;
    }
}

#[derive(Debug, Default)]
struct SnapshotInstallPlan {
    sections_skipped: usize,
    groups: Vec<PendingInstallGroup>,
}

impl SnapshotInstallPlan {
    fn push_staged_group(
        &mut self,
        branch_bytes: [u8; 16],
        type_tag: TypeTag,
        staged: StagedGroup,
    ) {
        let branch_id = BranchId::from_bytes(branch_bytes);
        self.groups.push(PendingInstallGroup {
            group: StorageDecodedSnapshotInstallGroup::new(branch_id, type_tag, staged.entries),
            stats: staged.stats,
        });
    }

    fn push_branch_groups(&mut self, type_tag: TypeTag, groups: HashMap<[u8; 16], StagedGroup>) {
        for (branch_bytes, staged) in groups {
            self.push_staged_group(branch_bytes, type_tag, staged);
        }
    }

    fn push_typed_groups(&mut self, groups: HashMap<([u8; 16], TypeTag), StagedGroup>) {
        for ((branch_bytes, type_tag), staged) in groups {
            self.push_staged_group(branch_bytes, type_tag, staged);
        }
    }

    fn into_storage_plan(self) -> (StorageDecodedSnapshotInstallPlan, InstallStats) {
        let mut stats = InstallStats {
            sections_skipped: self.sections_skipped,
            ..InstallStats::default()
        };
        let mut storage_groups = Vec::with_capacity(self.groups.len());
        for group in self.groups {
            stats.add_installed(&group.stats);
            storage_groups.push(group.group);
        }
        (
            StorageDecodedSnapshotInstallPlan::new(storage_groups),
            stats,
        )
    }
}

#[derive(Debug)]
struct PendingInstallGroup {
    group: StorageDecodedSnapshotInstallGroup,
    stats: InstallStats,
}

#[derive(Debug, Default)]
struct StagedGroup {
    entries: Vec<DecodedSnapshotEntry>,
    stats: InstallStats,
}

/// Install every section of `snapshot` into `storage`, preserving the original
/// commit version, timestamp, and tombstone state recorded in the checkpoint.
///
/// Returns `InstallStats` describing what was installed. Sections with
/// unknown primitive tags are treated as corruption and returned as error —
/// an unrecognized tag means we are reading a newer snapshot format than
/// this binary supports, which is unsafe to ignore.
pub(crate) fn install_snapshot(
    snapshot: &LoadedSnapshot,
    codec: &dyn StorageCodec,
    storage: &SegmentedStore,
) -> StrataResult<InstallStats> {
    if snapshot.codec_id != codec.codec_id() {
        return Err(StrataError::corruption(format!(
            "Snapshot codec mismatch during install: snapshot header {}, install codec {}",
            snapshot.codec_id,
            codec.codec_id()
        )));
    }

    // SnapshotReader already validated the snapshot file's codec id. The
    // primitive section payloads are checkpoint-format bytes and must be
    // decoded with the same canonical codec used by CheckpointCoordinator.
    let serializer = SnapshotSerializer::canonical_primitive_section();
    let (storage_plan, stats) =
        decode_snapshot_install_plan(snapshot, &serializer)?.into_storage_plan();
    let expected_groups = storage_plan.groups.len();
    let expected_rows = storage_plan
        .groups
        .iter()
        .map(|group| group.entries.len())
        .sum::<usize>();
    let storage_stats = install_decoded_snapshot_rows(StorageDecodedSnapshotInstallInput {
        storage,
        plan: &storage_plan,
    })
    .map_err(|err| map_decoded_snapshot_install_error(snapshot.snapshot_id(), err))?;

    if storage_stats.groups_installed != expected_groups
        || storage_stats.rows_installed != expected_rows
    {
        return Err(StrataError::internal(format!(
            "snapshot {} decoded-row install reported {} groups/{} rows, expected {} groups/{} rows",
            snapshot.snapshot_id(),
            storage_stats.groups_installed,
            storage_stats.rows_installed,
            expected_groups,
            expected_rows
        )));
    }
    if stats.total_installed() != expected_rows {
        return Err(StrataError::internal(format!(
            "snapshot {} primitive install stats counted {} rows, decoded plan contained {} rows",
            snapshot.snapshot_id(),
            stats.total_installed(),
            expected_rows
        )));
    }
    Ok(stats)
}

fn map_decoded_snapshot_install_error(
    snapshot_id: u64,
    err: StorageDecodedSnapshotInstallError,
) -> StrataError {
    match err {
        StorageDecodedSnapshotInstallError::Install {
            group_index,
            branch_id,
            storage_family,
            source,
        } => {
            let message = format!(
                "Snapshot {snapshot_id} decoded-row install group {group_index} for branch \
                 {branch_id} storage family 0x{storage_family:02x} failed: {source}"
            );
            match source {
                StorageError::Corruption { .. } => StrataError::corruption(message),
                StorageError::Io(inner) => StrataError::storage_with_source(message, inner),
                _ => StrataError::storage(message),
            }
        }
        other => StrataError::corruption(format!(
            "Snapshot {} decoded-row install rejected generic storage rows: {}",
            snapshot_id, other
        )),
    }
}

fn decode_snapshot_install_plan(
    snapshot: &LoadedSnapshot,
    serializer: &SnapshotSerializer,
) -> StrataResult<SnapshotInstallPlan> {
    let mut plan = SnapshotInstallPlan::default();
    for section in &snapshot.sections {
        match section.primitive_type {
            primitive_tags::KV => decode_kv_section(serializer, &section.data, &mut plan)?,
            primitive_tags::EVENT => decode_event_section(serializer, &section.data, &mut plan)?,
            primitive_tags::JSON => decode_json_section(serializer, &section.data, &mut plan)?,
            primitive_tags::VECTOR => decode_vector_section(serializer, &section.data, &mut plan)?,
            primitive_tags::BRANCH => decode_branch_section(serializer, &section.data, &mut plan)?,
            primitive_tags::GRAPH => {
                // Graph entries are written inside the KV section today; a
                // standalone Graph section is not emitted by
                // collect_checkpoint_data. Tolerate an empty section for
                // forward-compat but warn if one ever shows up with data.
                if !section.data.is_empty() {
                    warn!(
                        target: "strata::recovery",
                        section = "Graph",
                        bytes = section.data.len(),
                        "Skipping standalone Graph snapshot section: no decoder wired; \
                         Graph entries are expected in the KV section"
                    );
                    plan.sections_skipped += 1;
                }
            }
            other => {
                return Err(StrataError::corruption(format!(
                    "Unknown snapshot primitive tag 0x{:02x} at section boundary",
                    other
                )));
            }
        }
    }
    Ok(plan)
}

/// Decode one KV section and stage entries grouped by `(branch_id, type_tag)`.
///
/// The KV section carries both KV and Graph entries (`type_tag` discriminates);
/// install routes them to the appropriate storage type. Tombstones are
/// installed as `DecodedSnapshotValue::Tombstone`; TTL is propagated.
fn decode_kv_section(
    serializer: &SnapshotSerializer,
    data: &[u8],
    plan: &mut SnapshotInstallPlan,
) -> StrataResult<()> {
    let entries = serializer
        .deserialize_kv(data)
        .map_err(|e| StrataError::corruption(format!("KV section decode failed: {}", e)))?;

    let mut groups: HashMap<([u8; 16], TypeTag), StagedGroup> = HashMap::new();
    for entry in entries {
        let type_tag = TypeTag::from_byte(entry.type_tag).ok_or_else(|| {
            StrataError::corruption(format!(
                "Invalid TypeTag 0x{:02x} in KV snapshot entry",
                entry.type_tag
            ))
        })?;
        match type_tag {
            TypeTag::KV | TypeTag::Graph => {}
            other => {
                return Err(StrataError::corruption(format!(
                    "Invalid TypeTag 0x{:02x} ({:?}) in KV snapshot entry: KV section only supports KV and Graph",
                    entry.type_tag, other
                )));
            }
        }
        let payload = if entry.is_tombstone {
            DecodedSnapshotValue::Tombstone
        } else {
            DecodedSnapshotValue::Value(decode_value_json(&entry.value, "KV")?)
        };
        let decoded = DecodedSnapshotEntry {
            space: entry.space,
            user_key: entry.user_key,
            payload,
            version: CommitVersion(entry.version),
            timestamp_micros: entry.timestamp,
            ttl_ms: entry.ttl_ms,
        };
        let group = groups.entry((entry.branch_id, type_tag)).or_default();
        group.entries.push(decoded);
        match type_tag {
            TypeTag::Graph => group.stats.graph += 1,
            TypeTag::KV => group.stats.kv += 1,
            _ => unreachable!("KV section type tag was validated above"),
        }
    }
    plan.push_typed_groups(groups);
    Ok(())
}

/// Decode an Event section and install entries per-branch.
///
/// Event keys are reconstructed from the 8-byte big-endian sequence number,
/// matching the invariant in `Key::new_event`.
fn decode_event_section(
    serializer: &SnapshotSerializer,
    data: &[u8],
    plan: &mut SnapshotInstallPlan,
) -> StrataResult<()> {
    let entries = serializer
        .deserialize_events(data)
        .map_err(|e| StrataError::corruption(format!("Event section decode failed: {}", e)))?;

    let mut groups: HashMap<[u8; 16], StagedGroup> = HashMap::new();
    for entry in entries {
        let value = decode_value_json(&entry.payload, "Event")?;
        let decoded = DecodedSnapshotEntry {
            space: entry.space,
            user_key: entry.sequence.to_be_bytes().to_vec(),
            payload: DecodedSnapshotValue::Value(value),
            version: CommitVersion(entry.version),
            timestamp_micros: entry.timestamp,
            ttl_ms: 0,
        };
        let group = groups.entry(entry.branch_id).or_default();
        group.entries.push(decoded);
        group.stats.events += 1;
    }
    plan.push_branch_groups(TypeTag::Event, groups);
    Ok(())
}

/// Decode a JSON section and install entries per-branch with doc-id as the
/// user key (matching the Json primitive's key encoding). Tombstones on
/// deleted documents are preserved as `DecodedSnapshotValue::Tombstone`.
fn decode_json_section(
    serializer: &SnapshotSerializer,
    data: &[u8],
    plan: &mut SnapshotInstallPlan,
) -> StrataResult<()> {
    let entries = serializer
        .deserialize_json(data)
        .map_err(|e| StrataError::corruption(format!("JSON section decode failed: {}", e)))?;

    let mut groups: HashMap<[u8; 16], StagedGroup> = HashMap::new();
    for entry in entries {
        let payload = if entry.is_tombstone {
            DecodedSnapshotValue::Tombstone
        } else {
            DecodedSnapshotValue::Value(decode_value_json(&entry.content, "Json")?)
        };
        let decoded = DecodedSnapshotEntry {
            space: entry.space,
            user_key: entry.doc_id.into_bytes(),
            payload,
            version: CommitVersion(entry.version),
            timestamp_micros: entry.timestamp,
            ttl_ms: 0,
        };
        let group = groups.entry(entry.branch_id).or_default();
        group.entries.push(decoded);
        group.stats.json += 1;
    }
    plan.push_branch_groups(TypeTag::Json, groups);
    Ok(())
}

/// Decode a Branch section and install entries grouped by `branch_id`.
///
/// Branch metadata in today's engine lives under the global nil-UUID sentinel
/// (see `strata_engine::primitives::branch::index::global_branch_id`), but
/// the DTO carries the id explicitly so install stays robust against future
/// per-branch scoping without another format break.
///
/// `user_key` is the UTF-8 bytes of the original key string. Values are
/// stored as JSON-encoded bytes (matching the checkpoint collector) unless
/// the entry is a tombstone, in which case the value is empty.
fn decode_branch_section(
    serializer: &SnapshotSerializer,
    data: &[u8],
    plan: &mut SnapshotInstallPlan,
) -> StrataResult<()> {
    let entries = serializer
        .deserialize_branches(data)
        .map_err(|e| StrataError::corruption(format!("Branch section decode failed: {}", e)))?;

    let mut groups: HashMap<[u8; 16], StagedGroup> = HashMap::new();
    for entry in entries {
        // Explicit tombstone marker (matches KV/JSON/Vector). Using the flag
        // instead of inferring from empty-value bytes prevents a silent
        // misclassification if a live entry ever serializes to zero bytes.
        let payload = if entry.is_tombstone {
            DecodedSnapshotValue::Tombstone
        } else {
            DecodedSnapshotValue::Value(decode_value_json(&entry.value, "Branch")?)
        };
        let decoded = DecodedSnapshotEntry {
            // Branch metadata lives in the conventional "default" space per
            // `Namespace::for_branch`; storage reconstructs the full
            // `Namespace` from branch_id + space during decoded-row install.
            space: "default".to_string(),
            user_key: entry.key.into_bytes(),
            payload,
            version: CommitVersion(entry.version),
            timestamp_micros: entry.timestamp,
            ttl_ms: 0,
        };
        let group = groups.entry(entry.branch_id).or_default();
        group.entries.push(decoded);
        group.stats.branches += 1;
    }
    plan.push_branch_groups(TypeTag::Branch, groups);
    Ok(())
}

/// Decode a Vector section and install two classes of entries per collection:
///
/// 1. The collection-config row keyed `__config__/{name}`.
/// 2. One row per vector keyed `{name}/{vector_key}`, carrying the raw
///    `VectorRecord` bytes so subsystem recovery can reconstruct the index.
fn decode_vector_section(
    serializer: &SnapshotSerializer,
    data: &[u8],
    plan: &mut SnapshotInstallPlan,
) -> StrataResult<()> {
    let collections = serializer
        .deserialize_vectors(data)
        .map_err(|e| StrataError::corruption(format!("Vector section decode failed: {}", e)))?;

    let mut groups: HashMap<[u8; 16], StagedGroup> = HashMap::new();
    for collection in collections {
        let branch_bytes = collection.branch_id;
        let space = collection.space.clone();
        let name = collection.name.clone();

        // Collection config: `__config__/{name}` key carrying the raw
        // serialized config bytes as Value::Bytes (matches the compaction
        // collect path which also treats config as Value::Bytes).
        let config_key = format!("__config__/{}", name).into_bytes();
        let group = groups.entry(branch_bytes).or_default();
        group.entries.push(DecodedSnapshotEntry {
            space: space.clone(),
            user_key: config_key,
            payload: DecodedSnapshotValue::Value(Value::Bytes(collection.config.clone())),
            version: CommitVersion(collection.config_version),
            timestamp_micros: collection.config_timestamp,
            ttl_ms: 0,
        });
        group.stats.vector_configs += 1;

        for vector in collection.vectors {
            let vec_key = format!("{}/{}", name, vector.key).into_bytes();
            let payload = if vector.is_tombstone {
                DecodedSnapshotValue::Tombstone
            } else {
                // `raw_value` is the original serialized `VectorRecord`
                // bytes; reinstalling as `Value::Bytes` preserves the exact
                // payload that subsystem recovery expects to decode.
                DecodedSnapshotValue::Value(Value::Bytes(vector.raw_value))
            };
            let group = groups.entry(branch_bytes).or_default();
            group.entries.push(DecodedSnapshotEntry {
                space: space.clone(),
                user_key: vec_key,
                payload,
                version: CommitVersion(vector.version),
                timestamp_micros: vector.timestamp,
                ttl_ms: 0,
            });
            group.stats.vectors += 1;
        }
    }

    plan.push_branch_groups(TypeTag::Vector, groups);
    Ok(())
}

/// Decode a JSON-encoded `Value` blob with a section-scoped error message.
fn decode_value_json(bytes: &[u8], section: &str) -> StrataResult<Value> {
    serde_json::from_slice(bytes).map_err(|e| {
        StrataError::corruption(format!(
            "{} snapshot Value JSON decode failed: {}",
            section, e
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use strata_core::contract::Version;
    use strata_core::id::CommitVersion;
    use strata_core::BranchId;
    use strata_core::Value;
    use strata_storage::durability::codec::{CodecError, IdentityCodec, StorageCodec};
    use strata_storage::durability::format::{
        BranchSnapshotEntry, EventSnapshotEntry, JsonSnapshotEntry, KvSnapshotEntry,
        VectorCollectionSnapshotEntry, VectorSnapshotEntry,
    };
    use strata_storage::durability::{
        disk_snapshot::{SnapshotSection, SnapshotWriter},
        SnapshotReader,
    };
    use strata_storage::{Key, Namespace};

    fn writer_for(dir: &std::path::Path) -> SnapshotWriter {
        SnapshotWriter::new(dir.to_path_buf(), Box::new(IdentityCodec), [9u8; 16]).unwrap()
    }

    fn writer_for_codec(dir: &std::path::Path, codec: Box<dyn StorageCodec>) -> SnapshotWriter {
        SnapshotWriter::new(dir.to_path_buf(), codec, [9u8; 16]).unwrap()
    }

    fn load_snapshot(path: &std::path::Path) -> LoadedSnapshot {
        SnapshotReader::new(Box::new(IdentityCodec))
            .load(path)
            .unwrap()
    }

    fn load_snapshot_with_codec(
        path: &std::path::Path,
        codec: Box<dyn StorageCodec>,
    ) -> LoadedSnapshot {
        SnapshotReader::new(codec).load(path).unwrap()
    }

    fn serializer() -> SnapshotSerializer {
        SnapshotSerializer::canonical_primitive_section()
    }

    fn ns(branch_id: BranchId, space: &str) -> Arc<Namespace> {
        Arc::new(Namespace::for_branch_space(branch_id, space))
    }

    fn assert_error_contains(err: impl std::fmt::Display, expected: &str) {
        let msg = err.to_string();
        assert!(
            msg.contains(expected),
            "error should contain `{}`, got: {}",
            expected,
            msg
        );
    }

    #[test]
    fn decoded_storage_install_error_mapping_preserves_snapshot_group_context() {
        let branch_id = BranchId::from_bytes([0x42; 16]);
        let err = StorageDecodedSnapshotInstallError::Install {
            group_index: 7,
            branch_id,
            storage_family: TypeTag::KV.as_byte(),
            source: StorageError::corruption("synthetic lower install failure"),
        };

        let mapped = map_decoded_snapshot_install_error(99, err);

        assert!(
            matches!(mapped, StrataError::Corruption { .. }),
            "lower storage corruption should remain a corruption error"
        );
        assert_error_contains(&mapped, "Snapshot 99 decoded-row install group 7");
        assert_error_contains(&mapped, &branch_id.to_string());
        assert_error_contains(&mapped, "storage family 0x01");
        assert_error_contains(&mapped, "synthetic lower install failure");
    }

    #[derive(Debug, Clone, Copy)]
    struct RejectingDecodeCodec;

    impl StorageCodec for RejectingDecodeCodec {
        fn encode(&self, data: &[u8]) -> Vec<u8> {
            data.to_vec()
        }

        fn decode(&self, data: &[u8]) -> Result<Vec<u8>, CodecError> {
            Err(CodecError::decode(
                "test codec must not decode primitive snapshot sections",
                self.codec_id(),
                data.len(),
            ))
        }

        fn codec_id(&self) -> &str {
            "rejecting-decode-test"
        }

        fn clone_box(&self) -> Box<dyn StorageCodec> {
            Box::new(*self)
        }
    }

    #[test]
    fn install_round_trips_kv_entries() {
        let dir = tempfile::tempdir().unwrap();
        let branch_id = BranchId::new();

        let kv_bytes = serializer().serialize_kv(&[
            KvSnapshotEntry {
                branch_id: *branch_id.as_bytes(),
                space: "default".to_string(),
                type_tag: TypeTag::KV.as_byte(),
                user_key: b"alpha".to_vec(),
                value: serde_json::to_vec(&Value::Int(1)).unwrap(),
                version: 5,
                timestamp: 1_000,
                ttl_ms: 0,
                is_tombstone: false,
            },
            KvSnapshotEntry {
                branch_id: *branch_id.as_bytes(),
                space: "default".to_string(),
                type_tag: TypeTag::KV.as_byte(),
                user_key: b"beta".to_vec(),
                value: serde_json::to_vec(&Value::String("two".into())).unwrap(),
                version: 6,
                timestamp: 2_000,
                ttl_ms: 0,
                is_tombstone: false,
            },
        ]);

        let info = writer_for(dir.path())
            .create_snapshot(
                1,
                6,
                vec![SnapshotSection::new(primitive_tags::KV, kv_bytes)],
            )
            .unwrap();
        let snapshot = load_snapshot(&info.path);

        let storage = SegmentedStore::new();
        let stats = install_snapshot(&snapshot, &IdentityCodec, &storage).unwrap();
        assert_eq!(stats.kv, 2);
        assert_eq!(stats.total_installed(), 2);

        let alpha = storage
            .get_versioned(
                &Key::new_kv(ns(branch_id, "default"), "alpha"),
                CommitVersion::MAX,
            )
            .unwrap()
            .expect("alpha must be installed");
        assert_eq!(alpha.value, Value::Int(1));
        assert_eq!(alpha.version, Version::Txn(5));

        let beta = storage
            .get_versioned(
                &Key::new_kv(ns(branch_id, "default"), "beta"),
                CommitVersion::MAX,
            )
            .unwrap()
            .expect("beta must be installed");
        assert_eq!(beta.value, Value::String("two".into()));
        assert_eq!(beta.version, Version::Txn(6));
    }

    #[test]
    fn install_routes_graph_tag_into_graph_storage() {
        let dir = tempfile::tempdir().unwrap();
        let branch_id = BranchId::new();

        let kv_bytes = serializer().serialize_kv(&[
            KvSnapshotEntry {
                branch_id: *branch_id.as_bytes(),
                space: "_graph_".to_string(),
                type_tag: TypeTag::Graph.as_byte(),
                user_key: b"node:1".to_vec(),
                value: serde_json::to_vec(&Value::String("node-one".into())).unwrap(),
                version: 10,
                timestamp: 100,
                ttl_ms: 0,
                is_tombstone: false,
            },
            KvSnapshotEntry {
                branch_id: *branch_id.as_bytes(),
                space: "default".to_string(),
                type_tag: TypeTag::KV.as_byte(),
                user_key: b"k".to_vec(),
                value: serde_json::to_vec(&Value::Int(42)).unwrap(),
                version: 11,
                timestamp: 200,
                ttl_ms: 0,
                is_tombstone: false,
            },
        ]);

        let info = writer_for(dir.path())
            .create_snapshot(
                2,
                11,
                vec![SnapshotSection::new(primitive_tags::KV, kv_bytes)],
            )
            .unwrap();
        let snapshot = load_snapshot(&info.path);

        let storage = SegmentedStore::new();
        let stats = install_snapshot(&snapshot, &IdentityCodec, &storage).unwrap();
        assert_eq!(stats.graph, 1);
        assert_eq!(stats.kv, 1);

        let node_key = Key::new(ns(branch_id, "_graph_"), TypeTag::Graph, b"node:1".to_vec());
        let node = storage
            .get_versioned(&node_key, CommitVersion::MAX)
            .unwrap()
            .expect("graph node must be installed via Graph tag routing");
        assert_eq!(node.value, Value::String("node-one".into()));
    }

    #[test]
    fn install_preserves_commit_version_and_timestamp() {
        let dir = tempfile::tempdir().unwrap();
        let branch_id = BranchId::new();

        let kv_bytes = serializer().serialize_kv(&[KvSnapshotEntry {
            branch_id: *branch_id.as_bytes(),
            space: "default".to_string(),
            type_tag: TypeTag::KV.as_byte(),
            user_key: b"k".to_vec(),
            value: serde_json::to_vec(&Value::Int(7)).unwrap(),
            version: 999,
            timestamp: 1_234_567,
            ttl_ms: 0,
            is_tombstone: false,
        }]);

        let info = writer_for(dir.path())
            .create_snapshot(
                3,
                999,
                vec![SnapshotSection::new(primitive_tags::KV, kv_bytes)],
            )
            .unwrap();
        let snapshot = load_snapshot(&info.path);

        let storage = SegmentedStore::new();
        install_snapshot(&snapshot, &IdentityCodec, &storage).unwrap();

        let entry = storage
            .get_versioned(
                &Key::new_kv(ns(branch_id, "default"), "k"),
                CommitVersion::MAX,
            )
            .unwrap()
            .unwrap();
        assert_eq!(entry.version, Version::Txn(999));
        assert_eq!(entry.timestamp.as_micros(), 1_234_567);
    }

    #[test]
    fn install_events_uses_big_endian_sequence_key() {
        let dir = tempfile::tempdir().unwrap();
        let branch_id = BranchId::new();

        let event_bytes = serializer().serialize_events(&[EventSnapshotEntry {
            branch_id: *branch_id.as_bytes(),
            space: "stream".to_string(),
            sequence: 0x01020304,
            payload: serde_json::to_vec(&Value::String("payload".into())).unwrap(),
            version: 50,
            timestamp: 9_000,
        }]);

        let info = writer_for(dir.path())
            .create_snapshot(
                4,
                50,
                vec![SnapshotSection::new(primitive_tags::EVENT, event_bytes)],
            )
            .unwrap();
        let snapshot = load_snapshot(&info.path);

        let storage = SegmentedStore::new();
        let stats = install_snapshot(&snapshot, &IdentityCodec, &storage).unwrap();
        assert_eq!(stats.events, 1);

        let key = Key::new_event(ns(branch_id, "stream"), 0x01020304);
        let entry = storage
            .get_versioned(&key, CommitVersion::MAX)
            .unwrap()
            .expect("event must be installed under its big-endian sequence key");
        assert_eq!(entry.value, Value::String("payload".into()));
    }

    #[test]
    fn install_json_uses_doc_id_as_user_key() {
        let dir = tempfile::tempdir().unwrap();
        let branch_id = BranchId::new();

        let json_bytes = serializer().serialize_json(&[JsonSnapshotEntry {
            branch_id: *branch_id.as_bytes(),
            space: "docs".to_string(),
            doc_id: "doc-42".to_string(),
            content: serde_json::to_vec(&Value::String("content".into())).unwrap(),
            version: 100,
            timestamp: 77,
            is_tombstone: false,
        }]);

        let info = writer_for(dir.path())
            .create_snapshot(
                5,
                100,
                vec![SnapshotSection::new(primitive_tags::JSON, json_bytes)],
            )
            .unwrap();
        let snapshot = load_snapshot(&info.path);

        let storage = SegmentedStore::new();
        let stats = install_snapshot(&snapshot, &IdentityCodec, &storage).unwrap();
        assert_eq!(stats.json, 1);

        let key = Key::new(ns(branch_id, "docs"), TypeTag::Json, b"doc-42".to_vec());
        let entry = storage
            .get_versioned(&key, CommitVersion::MAX)
            .unwrap()
            .expect("json doc must be installed under its doc-id user_key");
        assert_eq!(entry.value, Value::String("content".into()));
    }

    #[test]
    fn install_vectors_installs_config_and_per_vector_rows() {
        let dir = tempfile::tempdir().unwrap();
        let branch_id = BranchId::new();

        let vec_bytes = serializer().serialize_vectors(&[VectorCollectionSnapshotEntry {
            branch_id: *branch_id.as_bytes(),
            space: "default".to_string(),
            name: "col".to_string(),
            config: b"collection-config".to_vec(),
            config_version: 12,
            config_timestamp: 1_500,
            vectors: vec![VectorSnapshotEntry {
                key: "vec1".to_string(),
                vector_id: 1,
                embedding: vec![0.1, 0.2],
                metadata: vec![],
                raw_value: b"raw-record-bytes".to_vec(),
                version: 13,
                timestamp: 1_600,
                is_tombstone: false,
            }],
        }]);

        let info = writer_for(dir.path())
            .create_snapshot(
                6,
                13,
                vec![SnapshotSection::new(primitive_tags::VECTOR, vec_bytes)],
            )
            .unwrap();
        let snapshot = load_snapshot(&info.path);

        let storage = SegmentedStore::new();
        let stats = install_snapshot(&snapshot, &IdentityCodec, &storage).unwrap();
        assert_eq!(stats.vector_configs, 1);
        assert_eq!(stats.vectors, 1);

        let config_key = Key::new(
            ns(branch_id, "default"),
            TypeTag::Vector,
            b"__config__/col".to_vec(),
        );
        let config = storage
            .get_versioned(&config_key, CommitVersion::MAX)
            .unwrap()
            .expect("collection config row must be installed");
        match config.value {
            Value::Bytes(b) => assert_eq!(b, b"collection-config".to_vec()),
            other => panic!("expected Value::Bytes, got {:?}", other),
        }

        let vec_key = Key::new(
            ns(branch_id, "default"),
            TypeTag::Vector,
            b"col/vec1".to_vec(),
        );
        let vec_entry = storage
            .get_versioned(&vec_key, CommitVersion::MAX)
            .unwrap()
            .expect("vector row must be installed");
        match vec_entry.value {
            Value::Bytes(b) => assert_eq!(b, b"raw-record-bytes".to_vec()),
            other => panic!("expected Value::Bytes, got {:?}", other),
        }
    }

    #[test]
    fn install_round_trips_branch_entries() {
        let dir = tempfile::tempdir().unwrap();
        // Branch metadata lives under the nil-UUID global branch today;
        // the DTO carries it explicitly so install dispatches correctly.
        let global_branch = BranchId::from_bytes([0; 16]);

        let branch_bytes = serializer().serialize_branches(&[
            strata_storage::durability::format::BranchSnapshotEntry {
                branch_id: *global_branch.as_bytes(),
                key: "my-branch".to_string(),
                value: serde_json::to_vec(&Value::String("branch-metadata".into())).unwrap(),
                version: 42,
                timestamp: 1_234,
                is_tombstone: false,
            },
        ]);

        let info = writer_for(dir.path())
            .create_snapshot(
                7,
                42,
                vec![SnapshotSection::new(primitive_tags::BRANCH, branch_bytes)],
            )
            .unwrap();
        let snapshot = load_snapshot(&info.path);

        let storage = SegmentedStore::new();
        let stats = install_snapshot(&snapshot, &IdentityCodec, &storage).unwrap();
        assert_eq!(stats.branches, 1);
        assert_eq!(stats.sections_skipped, 0);

        // The entry must land under the (global_branch, "default",
        // TypeTag::Branch) cell with the original key as user_key bytes.
        let ns = Arc::new(Namespace::for_branch(global_branch));
        let key = Key::new(ns, TypeTag::Branch, b"my-branch".to_vec());
        let entry = storage
            .get_versioned(&key, CommitVersion::MAX)
            .unwrap()
            .expect("branch entry must be installed");
        assert_eq!(entry.value, Value::String("branch-metadata".into()));
        assert_eq!(entry.version, Version::Txn(42));
    }

    #[test]
    fn install_propagates_branch_tombstone_as_tombstone() {
        // An explicit tombstone in `BranchSnapshotEntry` installs as a
        // storage-level tombstone so deleted branch metadata round-trips
        // through checkpoint + compact + reopen as a deletion, not as a
        // resurrected live record.
        let dir = tempfile::tempdir().unwrap();
        let global_branch = BranchId::from_bytes([0; 16]);

        let branch_bytes = serializer().serialize_branches(&[
            strata_storage::durability::format::BranchSnapshotEntry {
                branch_id: *global_branch.as_bytes(),
                key: "dropped".to_string(),
                value: serde_json::to_vec(&Value::String("before-delete".into())).unwrap(),
                version: 98,
                timestamp: 1_000,
                is_tombstone: false,
            },
            strata_storage::durability::format::BranchSnapshotEntry {
                branch_id: *global_branch.as_bytes(),
                key: "dropped".to_string(),
                value: Vec::new(),
                version: 99,
                timestamp: 2_000,
                is_tombstone: true,
            },
        ]);

        let info = writer_for(dir.path())
            .create_snapshot(
                12,
                99,
                vec![SnapshotSection::new(primitive_tags::BRANCH, branch_bytes)],
            )
            .unwrap();
        let snapshot = load_snapshot(&info.path);

        let storage = SegmentedStore::new();
        install_snapshot(&snapshot, &IdentityCodec, &storage).unwrap();

        let ns = Arc::new(Namespace::for_branch(global_branch));
        let key = Key::new(ns, TypeTag::Branch, b"dropped".to_vec());
        let before_delete = storage
            .get_versioned(&key, CommitVersion(98))
            .unwrap()
            .expect("older branch metadata should still be present below the tombstone");
        assert_eq!(before_delete.value, Value::String("before-delete".into()));

        let observed = storage.get_versioned(&key, CommitVersion::MAX).unwrap();
        assert!(
            observed.is_none(),
            "branch tombstone must hide the entry on reads"
        );
    }

    #[test]
    fn install_propagates_kv_tombstone_as_tombstone() {
        // A tombstoned KV entry must be installed as a storage-level
        // tombstone so reads return `None` after snapshot-only restart.
        let dir = tempfile::tempdir().unwrap();
        let branch_id = BranchId::new();

        let kv_bytes = serializer().serialize_kv(&[
            KvSnapshotEntry {
                branch_id: *branch_id.as_bytes(),
                space: "default".to_string(),
                type_tag: TypeTag::KV.as_byte(),
                user_key: b"deleted".to_vec(),
                value: serde_json::to_vec(&Value::String("before-delete".into())).unwrap(),
                version: 9,
                timestamp: 400,
                ttl_ms: 0,
                is_tombstone: false,
            },
            KvSnapshotEntry {
                branch_id: *branch_id.as_bytes(),
                space: "default".to_string(),
                type_tag: TypeTag::KV.as_byte(),
                user_key: b"deleted".to_vec(),
                value: Vec::new(),
                version: 10,
                timestamp: 500,
                ttl_ms: 0,
                is_tombstone: true,
            },
        ]);

        let info = writer_for(dir.path())
            .create_snapshot(
                10,
                10,
                vec![SnapshotSection::new(primitive_tags::KV, kv_bytes)],
            )
            .unwrap();
        let snapshot = load_snapshot(&info.path);

        let storage = SegmentedStore::new();
        install_snapshot(&snapshot, &IdentityCodec, &storage).unwrap();

        // A tombstoned snapshot entry surfaces as `None` on read.
        let key = Key::new_kv(ns(branch_id, "default"), "deleted");
        let before_delete = storage
            .get_versioned(&key, CommitVersion(9))
            .unwrap()
            .expect("older KV row should still be present below the tombstone");
        assert_eq!(before_delete.value, Value::String("before-delete".into()));

        let observed = storage.get_versioned(&key, CommitVersion::MAX).unwrap();
        assert!(observed.is_none(), "tombstone must hide the key from reads");
    }

    #[test]
    fn install_propagates_kv_ttl_into_storage() {
        // A KV entry with a non-zero ttl_ms must expire at the correct
        // timestamp after snapshot install — the TTL barrier survives
        // checkpoint + compact + reopen.
        let dir = tempfile::tempdir().unwrap();
        let branch_id = BranchId::new();
        let ttl_ms: u64 = 30_000;
        let commit_ts_micros: u64 = 1_000_000;

        let kv_bytes = serializer().serialize_kv(&[KvSnapshotEntry {
            branch_id: *branch_id.as_bytes(),
            space: "default".to_string(),
            type_tag: TypeTag::KV.as_byte(),
            user_key: b"expires".to_vec(),
            value: serde_json::to_vec(&Value::String("temporary".into())).unwrap(),
            version: 1,
            timestamp: commit_ts_micros,
            ttl_ms,
            is_tombstone: false,
        }]);

        let info = writer_for(dir.path())
            .create_snapshot(
                11,
                1,
                vec![SnapshotSection::new(primitive_tags::KV, kv_bytes)],
            )
            .unwrap();
        let snapshot = load_snapshot(&info.path);

        let storage = SegmentedStore::new();
        install_snapshot(&snapshot, &IdentityCodec, &storage).unwrap();

        let key = Key::new_kv(ns(branch_id, "default"), "expires");

        // Before the TTL elapses, the entry is visible.
        let pre_expiry = storage
            .get_at_timestamp(&key, commit_ts_micros + ttl_ms * 1_000 - 1)
            .unwrap();
        assert!(
            pre_expiry.is_some(),
            "entry must be visible before TTL elapses"
        );

        // Past the TTL window, the entry expires.
        let post_expiry = storage
            .get_at_timestamp(&key, commit_ts_micros + ttl_ms * 1_000 + 1)
            .unwrap();
        assert!(
            post_expiry.is_none(),
            "TTL barrier from the snapshot must expire the entry after the configured window"
        );
    }

    #[test]
    fn install_graph_standalone_nonempty_section_is_skipped() {
        // Graph standalone sections are not emitted by today's
        // collect_checkpoint_data; if a future writer (or a corrupt file)
        // produces one with payload bytes, install must skip it with a
        // warning rather than silently drop entries or error out.
        let dir = tempfile::tempdir().unwrap();
        let info = writer_for(dir.path())
            .create_snapshot(
                9,
                0,
                vec![SnapshotSection::new(
                    primitive_tags::GRAPH,
                    b"opaque-graph-payload".to_vec(),
                )],
            )
            .unwrap();
        let snapshot = load_snapshot(&info.path);

        let storage = SegmentedStore::new();
        let stats = install_snapshot(&snapshot, &IdentityCodec, &storage).unwrap();
        assert_eq!(stats.sections_skipped, 1);
        assert_eq!(stats.total_installed(), 0);
    }

    #[test]
    fn install_rejects_unknown_primitive_tag() {
        let dir = tempfile::tempdir().unwrap();
        // Writer validates tag at create_snapshot, so we must build a
        // snapshot with a known tag first and test the decoder by injecting
        // a synthetic LoadedSnapshot with an unknown tag.
        let info = writer_for(dir.path())
            .create_snapshot(
                8,
                0,
                vec![SnapshotSection::new(primitive_tags::KV, vec![0, 0, 0, 0])],
            )
            .unwrap();
        let mut snapshot = load_snapshot(&info.path);
        // Replace the real section with a bogus primitive type.
        snapshot.sections[0].primitive_type = 0xFE;

        let storage = SegmentedStore::new();
        let err = install_snapshot(&snapshot, &IdentityCodec, &storage)
            .expect_err("unknown primitive tag must be rejected");
        assert!(
            err.to_string().contains("0xfe"),
            "error should mention the unknown tag, got: {}",
            err
        );
    }

    #[test]
    fn install_rejects_invalid_kv_type_tag() {
        let dir = tempfile::tempdir().unwrap();
        let branch_id = BranchId::new();

        let kv_bytes = serializer().serialize_kv(&[KvSnapshotEntry {
            branch_id: *branch_id.as_bytes(),
            space: "default".to_string(),
            type_tag: 0xfd,
            user_key: b"bad-tag".to_vec(),
            value: serde_json::to_vec(&Value::Int(1)).unwrap(),
            version: 1,
            timestamp: 10,
            ttl_ms: 0,
            is_tombstone: false,
        }]);

        let info = writer_for(dir.path())
            .create_snapshot(
                13,
                1,
                vec![SnapshotSection::new(primitive_tags::KV, kv_bytes)],
            )
            .unwrap();
        let snapshot = load_snapshot(&info.path);

        let storage = SegmentedStore::new();
        let err = install_snapshot(&snapshot, &IdentityCodec, &storage)
            .expect_err("invalid KV type tag must be rejected");
        assert_error_contains(err, "Invalid TypeTag 0xfd in KV snapshot entry");
    }

    #[test]
    fn install_rejects_known_non_kv_type_tag_in_kv_section() {
        for disallowed in [
            TypeTag::Event,
            TypeTag::Branch,
            TypeTag::Space,
            TypeTag::Vector,
            TypeTag::Json,
        ] {
            let dir = tempfile::tempdir().unwrap();
            let branch_id = BranchId::new();

            let kv_bytes = serializer().serialize_kv(&[KvSnapshotEntry {
                branch_id: *branch_id.as_bytes(),
                space: "default".to_string(),
                type_tag: disallowed.as_byte(),
                user_key: b"wrong-family".to_vec(),
                value: serde_json::to_vec(&Value::Int(1)).unwrap(),
                version: 1,
                timestamp: 10,
                ttl_ms: 0,
                is_tombstone: false,
            }]);

            let info = writer_for(dir.path())
                .create_snapshot(
                    30 + disallowed.as_byte() as u64,
                    1,
                    vec![SnapshotSection::new(primitive_tags::KV, kv_bytes)],
                )
                .unwrap();
            let snapshot = load_snapshot(&info.path);

            let storage = SegmentedStore::new();
            let err = install_snapshot(&snapshot, &IdentityCodec, &storage)
                .expect_err("known non-KV type tag must be rejected inside KV section");
            assert_error_contains(err, "KV section only supports KV and Graph");
        }
    }

    #[test]
    fn install_uses_canonical_section_codec_even_when_snapshot_file_codec_differs() {
        let dir = tempfile::tempdir().unwrap();
        let branch_id = BranchId::new();
        let kv_bytes = serializer().serialize_kv(&[KvSnapshotEntry {
            branch_id: *branch_id.as_bytes(),
            space: "default".to_string(),
            type_tag: TypeTag::KV.as_byte(),
            user_key: b"codec-key".to_vec(),
            value: serde_json::to_vec(&Value::String("codec-value".into())).unwrap(),
            version: 1,
            timestamp: 10,
            ttl_ms: 0,
            is_tombstone: false,
        }]);

        let info = writer_for_codec(dir.path(), Box::new(RejectingDecodeCodec))
            .create_snapshot(
                14,
                1,
                vec![SnapshotSection::new(primitive_tags::KV, kv_bytes)],
            )
            .unwrap();
        let snapshot = load_snapshot_with_codec(&info.path, Box::new(RejectingDecodeCodec));
        assert_eq!(snapshot.codec_id, "rejecting-decode-test");

        let storage = SegmentedStore::new();
        let codec = RejectingDecodeCodec;
        let stats = install_snapshot(&snapshot, &codec, &storage)
            .expect("primitive section decode must not use the snapshot header codec");
        assert_eq!(stats.kv, 1);

        let observed = storage
            .get_versioned(
                &Key::new_kv(ns(branch_id, "default"), "codec-key"),
                CommitVersion::MAX,
            )
            .unwrap()
            .expect("KV row must install through canonical section decode");
        assert_eq!(observed.value, Value::String("codec-value".into()));
    }

    #[test]
    fn install_rejects_snapshot_codec_mismatch_before_mutation() {
        let dir = tempfile::tempdir().unwrap();
        let branch_id = BranchId::new();
        let kv_bytes = serializer().serialize_kv(&[KvSnapshotEntry {
            branch_id: *branch_id.as_bytes(),
            space: "default".to_string(),
            type_tag: TypeTag::KV.as_byte(),
            user_key: b"codec-mismatch".to_vec(),
            value: serde_json::to_vec(&Value::Int(1)).unwrap(),
            version: 1,
            timestamp: 10,
            ttl_ms: 0,
            is_tombstone: false,
        }]);

        let info = writer_for_codec(dir.path(), Box::new(RejectingDecodeCodec))
            .create_snapshot(
                40,
                1,
                vec![SnapshotSection::new(primitive_tags::KV, kv_bytes)],
            )
            .unwrap();
        let snapshot = load_snapshot_with_codec(&info.path, Box::new(RejectingDecodeCodec));

        let storage = SegmentedStore::new();
        let err = install_snapshot(&snapshot, &IdentityCodec, &storage)
            .expect_err("install must reject a caller-supplied codec mismatch");
        assert_error_contains(err, "Snapshot codec mismatch during install");

        let observed = storage
            .get_versioned(
                &Key::new_kv(ns(branch_id, "default"), "codec-mismatch"),
                CommitVersion::MAX,
            )
            .unwrap();
        assert!(
            observed.is_none(),
            "codec mismatch must fail before mutating storage"
        );
    }

    #[test]
    fn install_rejects_invalid_kv_type_tag_without_partial_mutation() {
        let dir = tempfile::tempdir().unwrap();
        let branch_id = BranchId::new();

        let kv_bytes = serializer().serialize_kv(&[
            KvSnapshotEntry {
                branch_id: *branch_id.as_bytes(),
                space: "default".to_string(),
                type_tag: TypeTag::KV.as_byte(),
                user_key: b"valid-before-invalid".to_vec(),
                value: serde_json::to_vec(&Value::Int(1)).unwrap(),
                version: 1,
                timestamp: 10,
                ttl_ms: 0,
                is_tombstone: false,
            },
            KvSnapshotEntry {
                branch_id: *branch_id.as_bytes(),
                space: "default".to_string(),
                type_tag: 0xfd,
                user_key: b"invalid".to_vec(),
                value: serde_json::to_vec(&Value::Int(2)).unwrap(),
                version: 2,
                timestamp: 20,
                ttl_ms: 0,
                is_tombstone: false,
            },
        ]);

        let info = writer_for(dir.path())
            .create_snapshot(
                15,
                2,
                vec![SnapshotSection::new(primitive_tags::KV, kv_bytes)],
            )
            .unwrap();
        let snapshot = load_snapshot(&info.path);

        let storage = SegmentedStore::new();
        let err = install_snapshot(&snapshot, &IdentityCodec, &storage)
            .expect_err("invalid tag must reject the whole snapshot before mutation");
        assert_error_contains(err, "Invalid TypeTag 0xfd in KV snapshot entry");

        let key = Key::new_kv(ns(branch_id, "default"), "valid-before-invalid");
        let observed = storage.get_versioned(&key, CommitVersion::MAX).unwrap();
        assert!(
            observed.is_none(),
            "valid earlier KV entry must not be installed when the same snapshot has an invalid tag"
        );
    }

    #[test]
    fn install_rejects_known_non_kv_type_tag_without_partial_mutation() {
        let dir = tempfile::tempdir().unwrap();
        let branch_id = BranchId::new();

        let kv_bytes = serializer().serialize_kv(&[
            KvSnapshotEntry {
                branch_id: *branch_id.as_bytes(),
                space: "default".to_string(),
                type_tag: TypeTag::KV.as_byte(),
                user_key: b"valid-before-wrong-family".to_vec(),
                value: serde_json::to_vec(&Value::Int(1)).unwrap(),
                version: 1,
                timestamp: 10,
                ttl_ms: 0,
                is_tombstone: false,
            },
            KvSnapshotEntry {
                branch_id: *branch_id.as_bytes(),
                space: "default".to_string(),
                type_tag: TypeTag::Event.as_byte(),
                user_key: b"wrong-family".to_vec(),
                value: serde_json::to_vec(&Value::Int(2)).unwrap(),
                version: 2,
                timestamp: 20,
                ttl_ms: 0,
                is_tombstone: false,
            },
        ]);

        let info = writer_for(dir.path())
            .create_snapshot(
                41,
                2,
                vec![SnapshotSection::new(primitive_tags::KV, kv_bytes)],
            )
            .unwrap();
        let snapshot = load_snapshot(&info.path);

        let storage = SegmentedStore::new();
        let err = install_snapshot(&snapshot, &IdentityCodec, &storage)
            .expect_err("known non-KV tag must reject the whole snapshot before mutation");
        assert_error_contains(err, "KV section only supports KV and Graph");

        let key = Key::new_kv(ns(branch_id, "default"), "valid-before-wrong-family");
        let observed = storage.get_versioned(&key, CommitVersion::MAX).unwrap();
        assert!(
            observed.is_none(),
            "valid earlier KV entry must not be installed when the same snapshot has a wrong-family tag"
        );
    }

    #[test]
    fn install_rejects_zero_version_from_storage_helper_without_partial_mutation() {
        let dir = tempfile::tempdir().unwrap();
        let branch_id = BranchId::new();

        let kv_bytes = serializer().serialize_kv(&[
            KvSnapshotEntry {
                branch_id: *branch_id.as_bytes(),
                space: "default".to_string(),
                type_tag: TypeTag::KV.as_byte(),
                user_key: b"valid-before-zero-version".to_vec(),
                value: serde_json::to_vec(&Value::Int(1)).unwrap(),
                version: 1,
                timestamp: 10,
                ttl_ms: 0,
                is_tombstone: false,
            },
            KvSnapshotEntry {
                branch_id: *branch_id.as_bytes(),
                space: "default".to_string(),
                type_tag: TypeTag::KV.as_byte(),
                user_key: b"zero-version".to_vec(),
                value: serde_json::to_vec(&Value::Int(2)).unwrap(),
                version: 0,
                timestamp: 20,
                ttl_ms: 0,
                is_tombstone: false,
            },
        ]);

        let info = writer_for(dir.path())
            .create_snapshot(
                42,
                1,
                vec![SnapshotSection::new(primitive_tags::KV, kv_bytes)],
            )
            .unwrap();
        let snapshot = load_snapshot(&info.path);

        let storage = SegmentedStore::new();
        let err = install_snapshot(&snapshot, &IdentityCodec, &storage)
            .expect_err("zero-version storage rows must reject the whole snapshot before mutation");
        assert_error_contains(err, "zero commit version");

        let key = Key::new_kv(ns(branch_id, "default"), "valid-before-zero-version");
        let observed = storage.get_versioned(&key, CommitVersion::MAX).unwrap();
        assert!(
            observed.is_none(),
            "valid earlier KV entry must not be installed when storage generic validation rejects a later row"
        );
    }

    #[test]
    fn install_rejects_duplicate_rows_from_storage_helper_without_partial_mutation() {
        let dir = tempfile::tempdir().unwrap();
        let branch_id = BranchId::new();

        let kv_bytes = serializer().serialize_kv(&[
            KvSnapshotEntry {
                branch_id: *branch_id.as_bytes(),
                space: "default".to_string(),
                type_tag: TypeTag::KV.as_byte(),
                user_key: b"duplicate-row".to_vec(),
                value: serde_json::to_vec(&Value::Int(1)).unwrap(),
                version: 3,
                timestamp: 10,
                ttl_ms: 0,
                is_tombstone: false,
            },
            KvSnapshotEntry {
                branch_id: *branch_id.as_bytes(),
                space: "default".to_string(),
                type_tag: TypeTag::KV.as_byte(),
                user_key: b"duplicate-row".to_vec(),
                value: serde_json::to_vec(&Value::Int(2)).unwrap(),
                version: 3,
                timestamp: 20,
                ttl_ms: 0,
                is_tombstone: false,
            },
        ]);

        let info = writer_for(dir.path())
            .create_snapshot(
                43,
                3,
                vec![SnapshotSection::new(primitive_tags::KV, kv_bytes)],
            )
            .unwrap();
        let snapshot = load_snapshot(&info.path);

        let storage = SegmentedStore::new();
        let err = install_snapshot(&snapshot, &IdentityCodec, &storage)
            .expect_err("duplicate storage rows must reject the whole snapshot before mutation");
        assert_error_contains(err, "duplicate row");

        let key = Key::new_kv(ns(branch_id, "default"), "duplicate-row");
        let observed = storage.get_versioned(&key, CommitVersion::MAX).unwrap();
        assert!(
            observed.is_none(),
            "duplicate row rejection must happen before installing the first duplicate"
        );
    }

    #[test]
    fn install_rejects_duplicate_rows_across_sections_without_partial_mutation() {
        let dir = tempfile::tempdir().unwrap();
        let branch_id = BranchId::new();

        let first_kv_bytes = serializer().serialize_kv(&[KvSnapshotEntry {
            branch_id: *branch_id.as_bytes(),
            space: "default".to_string(),
            type_tag: TypeTag::KV.as_byte(),
            user_key: b"duplicate-across-sections".to_vec(),
            value: serde_json::to_vec(&Value::Int(1)).unwrap(),
            version: 4,
            timestamp: 10,
            ttl_ms: 0,
            is_tombstone: false,
        }]);
        let second_kv_bytes = serializer().serialize_kv(&[KvSnapshotEntry {
            branch_id: *branch_id.as_bytes(),
            space: "default".to_string(),
            type_tag: TypeTag::KV.as_byte(),
            user_key: b"duplicate-across-sections".to_vec(),
            value: serde_json::to_vec(&Value::Int(2)).unwrap(),
            version: 4,
            timestamp: 20,
            ttl_ms: 0,
            is_tombstone: false,
        }]);

        let info = writer_for(dir.path())
            .create_snapshot(
                44,
                4,
                vec![
                    SnapshotSection::new(primitive_tags::KV, first_kv_bytes),
                    SnapshotSection::new(primitive_tags::KV, second_kv_bytes),
                ],
            )
            .unwrap();
        let snapshot = load_snapshot(&info.path);

        let storage = SegmentedStore::new();
        let err = install_snapshot(&snapshot, &IdentityCodec, &storage)
            .expect_err("duplicate storage rows across sections must reject before mutation");
        assert_error_contains(&err, "Snapshot 44 decoded-row install rejected");
        assert_error_contains(&err, "duplicate row");
        let key_hex = b"duplicate-across-sections"
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        assert_error_contains(&err, &key_hex);

        let key = Key::new_kv(ns(branch_id, "default"), "duplicate-across-sections");
        let observed = storage.get_versioned(&key, CommitVersion::MAX).unwrap();
        assert!(
            observed.is_none(),
            "duplicate row rejection across sections must happen before installing the first row"
        );
    }

    #[test]
    fn install_rejects_unknown_later_section_without_partial_mutation() {
        let dir = tempfile::tempdir().unwrap();
        let branch_id = BranchId::new();

        let kv_bytes = serializer().serialize_kv(&[KvSnapshotEntry {
            branch_id: *branch_id.as_bytes(),
            space: "default".to_string(),
            type_tag: TypeTag::KV.as_byte(),
            user_key: b"valid-before-unknown-section".to_vec(),
            value: serde_json::to_vec(&Value::Int(1)).unwrap(),
            version: 1,
            timestamp: 10,
            ttl_ms: 0,
            is_tombstone: false,
        }]);

        let info = writer_for(dir.path())
            .create_snapshot(
                16,
                1,
                vec![
                    SnapshotSection::new(primitive_tags::KV, kv_bytes),
                    SnapshotSection::new(primitive_tags::GRAPH, Vec::new()),
                ],
            )
            .unwrap();
        let mut snapshot = load_snapshot(&info.path);
        snapshot.sections[1].primitive_type = 0xfe;

        let storage = SegmentedStore::new();
        let err = install_snapshot(&snapshot, &IdentityCodec, &storage)
            .expect_err("unknown later section must reject the whole snapshot before mutation");
        assert_error_contains(err, "Unknown snapshot primitive tag 0xfe");

        let key = Key::new_kv(ns(branch_id, "default"), "valid-before-unknown-section");
        let observed = storage.get_versioned(&key, CommitVersion::MAX).unwrap();
        assert!(
            observed.is_none(),
            "earlier valid section must not be installed when a later section is invalid"
        );
    }

    #[test]
    fn install_propagates_json_tombstone_as_tombstone() {
        let dir = tempfile::tempdir().unwrap();
        let branch_id = BranchId::new();

        let json_bytes = serializer().serialize_json(&[
            JsonSnapshotEntry {
                branch_id: *branch_id.as_bytes(),
                space: "docs".to_string(),
                doc_id: "deleted-doc".to_string(),
                content: serde_json::to_vec(&Value::String("before-delete".into())).unwrap(),
                version: 1,
                timestamp: 10,
                is_tombstone: false,
            },
            JsonSnapshotEntry {
                branch_id: *branch_id.as_bytes(),
                space: "docs".to_string(),
                doc_id: "deleted-doc".to_string(),
                content: Vec::new(),
                version: 2,
                timestamp: 20,
                is_tombstone: true,
            },
        ]);

        let info = writer_for(dir.path())
            .create_snapshot(
                17,
                2,
                vec![SnapshotSection::new(primitive_tags::JSON, json_bytes)],
            )
            .unwrap();
        let snapshot = load_snapshot(&info.path);

        let storage = SegmentedStore::new();
        let stats = install_snapshot(&snapshot, &IdentityCodec, &storage).unwrap();
        assert_eq!(stats.json, 2);

        let key = Key::new(
            ns(branch_id, "docs"),
            TypeTag::Json,
            b"deleted-doc".to_vec(),
        );
        let before_delete = storage
            .get_versioned(&key, CommitVersion(1))
            .unwrap()
            .expect("older JSON version should still be present below the tombstone");
        assert_eq!(before_delete.value, Value::String("before-delete".into()));

        let observed = storage.get_versioned(&key, CommitVersion::MAX).unwrap();
        assert!(
            observed.is_none(),
            "JSON tombstone must hide the older live row"
        );
    }

    #[test]
    fn install_propagates_vector_tombstone_as_tombstone() {
        let dir = tempfile::tempdir().unwrap();
        let branch_id = BranchId::new();

        let vector_bytes = serializer().serialize_vectors(&[VectorCollectionSnapshotEntry {
            branch_id: *branch_id.as_bytes(),
            space: "default".to_string(),
            name: "col".to_string(),
            config: b"collection-config".to_vec(),
            config_version: 1,
            config_timestamp: 10,
            vectors: vec![
                VectorSnapshotEntry {
                    key: "vec1".to_string(),
                    vector_id: 1,
                    embedding: vec![0.1, 0.2],
                    metadata: Vec::new(),
                    raw_value: b"raw-live".to_vec(),
                    version: 2,
                    timestamp: 20,
                    is_tombstone: false,
                },
                VectorSnapshotEntry {
                    key: "vec1".to_string(),
                    vector_id: 1,
                    embedding: Vec::new(),
                    metadata: Vec::new(),
                    raw_value: Vec::new(),
                    version: 3,
                    timestamp: 30,
                    is_tombstone: true,
                },
            ],
        }]);

        let info = writer_for(dir.path())
            .create_snapshot(
                18,
                3,
                vec![SnapshotSection::new(primitive_tags::VECTOR, vector_bytes)],
            )
            .unwrap();
        let snapshot = load_snapshot(&info.path);

        let storage = SegmentedStore::new();
        let stats = install_snapshot(&snapshot, &IdentityCodec, &storage).unwrap();
        assert_eq!(stats.vector_configs, 1);
        assert_eq!(stats.vectors, 2);

        let key = Key::new(
            ns(branch_id, "default"),
            TypeTag::Vector,
            b"col/vec1".to_vec(),
        );
        let before_delete = storage
            .get_versioned(&key, CommitVersion(2))
            .unwrap()
            .expect("older vector row should still be present below the tombstone");
        assert_eq!(before_delete.value, Value::Bytes(b"raw-live".to_vec()));

        let observed = storage.get_versioned(&key, CommitVersion::MAX).unwrap();
        assert!(
            observed.is_none(),
            "Vector tombstone must hide the older live row"
        );
    }

    #[test]
    fn install_rejects_corrupt_kv_section_with_section_context() {
        let dir = tempfile::tempdir().unwrap();
        let info = writer_for(dir.path())
            .create_snapshot(
                19,
                1,
                vec![SnapshotSection::new(primitive_tags::KV, vec![1, 0, 0, 0])],
            )
            .unwrap();
        let snapshot = load_snapshot(&info.path);

        let storage = SegmentedStore::new();
        let err = install_snapshot(&snapshot, &IdentityCodec, &storage)
            .expect_err("corrupt KV section must be rejected");
        assert_error_contains(err, "KV section decode failed");
    }

    #[test]
    fn install_rejects_corrupt_non_kv_sections_with_section_context() {
        for (snapshot_id, tag, expected) in [
            (20, primitive_tags::EVENT, "Event section decode failed"),
            (21, primitive_tags::JSON, "JSON section decode failed"),
            (22, primitive_tags::BRANCH, "Branch section decode failed"),
            (23, primitive_tags::VECTOR, "Vector section decode failed"),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let info = writer_for(dir.path())
                .create_snapshot(
                    snapshot_id,
                    1,
                    vec![SnapshotSection::new(tag, vec![1, 0, 0, 0])],
                )
                .unwrap();
            let snapshot = load_snapshot(&info.path);

            let storage = SegmentedStore::new();
            let err = install_snapshot(&snapshot, &IdentityCodec, &storage)
                .expect_err("corrupt primitive section must be rejected");
            assert_error_contains(err, expected);
        }
    }

    #[test]
    fn install_rejects_trailing_section_data_with_section_context() {
        for (snapshot_id, tag, expected) in [
            (50, primitive_tags::KV, "KV section decode failed"),
            (51, primitive_tags::EVENT, "Event section decode failed"),
            (52, primitive_tags::JSON, "JSON section decode failed"),
            (53, primitive_tags::BRANCH, "Branch section decode failed"),
            (54, primitive_tags::VECTOR, "Vector section decode failed"),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let mut data = 0u32.to_le_bytes().to_vec();
            data.push(0xff);
            let info = writer_for(dir.path())
                .create_snapshot(snapshot_id, 1, vec![SnapshotSection::new(tag, data)])
                .unwrap();
            let snapshot = load_snapshot(&info.path);

            let storage = SegmentedStore::new();
            let err = install_snapshot(&snapshot, &IdentityCodec, &storage)
                .expect_err("trailing primitive section data must be rejected");
            assert_error_contains(&err, expected);
            assert_error_contains(&err, "Trailing data after snapshot section");
        }
    }

    #[test]
    fn install_rejects_huge_entry_counts_with_section_context() {
        for (snapshot_id, tag, expected) in [
            (60, primitive_tags::KV, "KV section decode failed"),
            (61, primitive_tags::EVENT, "Event section decode failed"),
            (62, primitive_tags::JSON, "JSON section decode failed"),
            (63, primitive_tags::BRANCH, "Branch section decode failed"),
            (64, primitive_tags::VECTOR, "Vector section decode failed"),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let data = u32::MAX.to_le_bytes().to_vec();
            let info = writer_for(dir.path())
                .create_snapshot(snapshot_id, 1, vec![SnapshotSection::new(tag, data)])
                .unwrap();
            let snapshot = load_snapshot(&info.path);

            let storage = SegmentedStore::new();
            let err = install_snapshot(&snapshot, &IdentityCodec, &storage)
                .expect_err("huge primitive section counts must be rejected without preallocation");
            assert_error_contains(&err, expected);
            assert_error_contains(&err, "Unexpected end of data");
        }
    }

    #[test]
    fn install_rejects_corrupt_value_json_with_section_context() {
        let branch_id = BranchId::new();
        let global_branch = BranchId::from_bytes([0; 16]);
        let cases = vec![
            (
                primitive_tags::KV,
                serializer().serialize_kv(&[KvSnapshotEntry {
                    branch_id: *branch_id.as_bytes(),
                    space: "default".to_string(),
                    type_tag: TypeTag::KV.as_byte(),
                    user_key: b"bad-json".to_vec(),
                    value: b"not-json".to_vec(),
                    version: 1,
                    timestamp: 10,
                    ttl_ms: 0,
                    is_tombstone: false,
                }]),
                "KV snapshot Value JSON decode failed",
            ),
            (
                primitive_tags::EVENT,
                serializer().serialize_events(&[EventSnapshotEntry {
                    branch_id: *branch_id.as_bytes(),
                    space: "stream".to_string(),
                    sequence: 1,
                    payload: b"not-json".to_vec(),
                    version: 1,
                    timestamp: 10,
                }]),
                "Event snapshot Value JSON decode failed",
            ),
            (
                primitive_tags::JSON,
                serializer().serialize_json(&[JsonSnapshotEntry {
                    branch_id: *branch_id.as_bytes(),
                    space: "docs".to_string(),
                    doc_id: "bad-json".to_string(),
                    content: b"not-json".to_vec(),
                    version: 1,
                    timestamp: 10,
                    is_tombstone: false,
                }]),
                "Json snapshot Value JSON decode failed",
            ),
            (
                primitive_tags::BRANCH,
                serializer().serialize_branches(&[BranchSnapshotEntry {
                    branch_id: *global_branch.as_bytes(),
                    key: "bad-json".to_string(),
                    value: b"not-json".to_vec(),
                    version: 1,
                    timestamp: 10,
                    is_tombstone: false,
                }]),
                "Branch snapshot Value JSON decode failed",
            ),
        ];

        for (index, (tag, bytes, expected)) in cases.into_iter().enumerate() {
            let dir = tempfile::tempdir().unwrap();
            let info = writer_for(dir.path())
                .create_snapshot(24 + index as u64, 1, vec![SnapshotSection::new(tag, bytes)])
                .unwrap();
            let snapshot = load_snapshot(&info.path);

            let storage = SegmentedStore::new();
            let err = install_snapshot(&snapshot, &IdentityCodec, &storage)
                .expect_err("corrupt JSON-encoded Value must be rejected");
            assert_error_contains(err, expected);
        }
    }
}
