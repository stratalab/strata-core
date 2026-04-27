//! Decode `LoadedSnapshot` sections and install them into `SegmentedStore`.
//!
//! This is the engine-side inverse of `compaction::collect_checkpoint_data`.
//! It walks the per-primitive sections of a `LoadedSnapshot`, deserializes
//! each section via [`SnapshotSerializer`], and routes decoded entries into
//! [`SegmentedStore::install_snapshot_entries`] grouped by
//! `(branch_id, type_tag)`.
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
use strata_core::value::Value;
use strata_core::{BranchId, TypeTag};
use strata_durability::codec::{clone_codec, StorageCodec};
use strata_durability::format::primitive_tags;
use strata_durability::{LoadedSnapshot, SnapshotSerializer};
use strata_storage::{DecodedSnapshotEntry, DecodedSnapshotValue, SegmentedStore};
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
    let serializer = SnapshotSerializer::new(clone_codec(codec));
    let mut stats = InstallStats::default();
    for section in &snapshot.sections {
        match section.primitive_type {
            primitive_tags::KV => {
                install_kv_section(&serializer, &section.data, storage, &mut stats)?
            }
            primitive_tags::EVENT => {
                install_event_section(&serializer, &section.data, storage, &mut stats)?
            }
            primitive_tags::JSON => {
                install_json_section(&serializer, &section.data, storage, &mut stats)?
            }
            primitive_tags::VECTOR => {
                install_vector_section(&serializer, &section.data, storage, &mut stats)?
            }
            primitive_tags::BRANCH => {
                install_branch_section(&serializer, &section.data, storage, &mut stats)?
            }
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
                    stats.sections_skipped += 1;
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
    Ok(stats)
}

/// Decode one KV section and install entries grouped by `(branch_id, type_tag)`.
///
/// The KV section carries both KV and Graph entries (`type_tag` discriminates);
/// install routes them to the appropriate storage type. Tombstones are
/// installed as `DecodedSnapshotValue::Tombstone`; TTL is propagated.
fn install_kv_section(
    serializer: &SnapshotSerializer,
    data: &[u8],
    storage: &SegmentedStore,
    stats: &mut InstallStats,
) -> StrataResult<()> {
    let entries = serializer
        .deserialize_kv(data)
        .map_err(|e| StrataError::corruption(format!("KV section decode failed: {}", e)))?;

    let mut groups: HashMap<([u8; 16], u8), Vec<DecodedSnapshotEntry>> = HashMap::new();
    for entry in entries {
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
        groups
            .entry((entry.branch_id, entry.type_tag))
            .or_default()
            .push(decoded);
    }
    for ((branch_bytes, tag_byte), group_entries) in groups {
        let branch_id = BranchId::from_bytes(branch_bytes);
        let type_tag = TypeTag::from_byte(tag_byte).ok_or_else(|| {
            StrataError::corruption(format!(
                "Invalid TypeTag 0x{:02x} in KV snapshot entry",
                tag_byte
            ))
        })?;
        let count = storage.install_snapshot_entries(branch_id, type_tag, &group_entries)?;
        match type_tag {
            TypeTag::Graph => stats.graph += count,
            _ => stats.kv += count,
        }
    }
    Ok(())
}

/// Decode an Event section and install entries per-branch.
///
/// Event keys are reconstructed from the 8-byte big-endian sequence number,
/// matching the invariant in `Key::new_event`.
fn install_event_section(
    serializer: &SnapshotSerializer,
    data: &[u8],
    storage: &SegmentedStore,
    stats: &mut InstallStats,
) -> StrataResult<()> {
    let entries = serializer
        .deserialize_events(data)
        .map_err(|e| StrataError::corruption(format!("Event section decode failed: {}", e)))?;

    let mut groups: HashMap<[u8; 16], Vec<DecodedSnapshotEntry>> = HashMap::new();
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
        groups.entry(entry.branch_id).or_default().push(decoded);
    }
    for (branch_bytes, group_entries) in groups {
        let branch_id = BranchId::from_bytes(branch_bytes);
        let count = storage.install_snapshot_entries(branch_id, TypeTag::Event, &group_entries)?;
        stats.events += count;
    }
    Ok(())
}

/// Decode a JSON section and install entries per-branch with doc-id as the
/// user key (matching the Json primitive's key encoding). Tombstones on
/// deleted documents are preserved as `DecodedSnapshotValue::Tombstone`.
fn install_json_section(
    serializer: &SnapshotSerializer,
    data: &[u8],
    storage: &SegmentedStore,
    stats: &mut InstallStats,
) -> StrataResult<()> {
    let entries = serializer
        .deserialize_json(data)
        .map_err(|e| StrataError::corruption(format!("JSON section decode failed: {}", e)))?;

    let mut groups: HashMap<[u8; 16], Vec<DecodedSnapshotEntry>> = HashMap::new();
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
        groups.entry(entry.branch_id).or_default().push(decoded);
    }
    for (branch_bytes, group_entries) in groups {
        let branch_id = BranchId::from_bytes(branch_bytes);
        let count = storage.install_snapshot_entries(branch_id, TypeTag::Json, &group_entries)?;
        stats.json += count;
    }
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
fn install_branch_section(
    serializer: &SnapshotSerializer,
    data: &[u8],
    storage: &SegmentedStore,
    stats: &mut InstallStats,
) -> StrataResult<()> {
    let entries = serializer
        .deserialize_branches(data)
        .map_err(|e| StrataError::corruption(format!("Branch section decode failed: {}", e)))?;

    let mut groups: HashMap<[u8; 16], Vec<DecodedSnapshotEntry>> = HashMap::new();
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
            // `Namespace::for_branch`; install reconstructs the full
            // `Namespace` from branch_id + space inside
            // `SegmentedStore::install_snapshot_entries`.
            space: "default".to_string(),
            user_key: entry.key.into_bytes(),
            payload,
            version: CommitVersion(entry.version),
            timestamp_micros: entry.timestamp,
            ttl_ms: 0,
        };
        groups.entry(entry.branch_id).or_default().push(decoded);
    }
    for (branch_bytes, group_entries) in groups {
        let branch_id = BranchId::from_bytes(branch_bytes);
        let count = storage.install_snapshot_entries(branch_id, TypeTag::Branch, &group_entries)?;
        stats.branches += count;
    }
    Ok(())
}

/// Decode a Vector section and install two classes of entries per collection:
///
/// 1. The collection-config row keyed `__config__/{name}`.
/// 2. One row per vector keyed `{name}/{vector_key}`, carrying the raw
///    `VectorRecord` bytes so subsystem recovery can reconstruct the index.
fn install_vector_section(
    serializer: &SnapshotSerializer,
    data: &[u8],
    storage: &SegmentedStore,
    stats: &mut InstallStats,
) -> StrataResult<()> {
    let collections = serializer
        .deserialize_vectors(data)
        .map_err(|e| StrataError::corruption(format!("Vector section decode failed: {}", e)))?;

    let mut groups: HashMap<[u8; 16], Vec<DecodedSnapshotEntry>> = HashMap::new();
    for collection in collections {
        let branch_bytes = collection.branch_id;
        let space = collection.space.clone();
        let name = collection.name.clone();

        // Collection config: `__config__/{name}` key carrying the raw
        // serialized config bytes as Value::Bytes (matches the compaction
        // collect path which also treats config as Value::Bytes).
        let config_key = format!("__config__/{}", name).into_bytes();
        groups
            .entry(branch_bytes)
            .or_default()
            .push(DecodedSnapshotEntry {
                space: space.clone(),
                user_key: config_key,
                payload: DecodedSnapshotValue::Value(Value::Bytes(collection.config.clone())),
                version: CommitVersion(collection.config_version),
                timestamp_micros: collection.config_timestamp,
                ttl_ms: 0,
            });
        stats.vector_configs += 1;

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
            groups
                .entry(branch_bytes)
                .or_default()
                .push(DecodedSnapshotEntry {
                    space: space.clone(),
                    user_key: vec_key,
                    payload,
                    version: CommitVersion(vector.version),
                    timestamp_micros: vector.timestamp,
                    ttl_ms: 0,
                });
            stats.vectors += 1;
        }
    }

    for (branch_bytes, group_entries) in groups {
        let branch_id = BranchId::from_bytes(branch_bytes);
        storage.install_snapshot_entries(branch_id, TypeTag::Vector, &group_entries)?;
    }
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
    use strata_core::traits::Storage;
    use strata_core::value::Value;
    use strata_core::{BranchId, Key, Namespace};
    use strata_durability::codec::IdentityCodec;
    use strata_durability::format::{
        EventSnapshotEntry, JsonSnapshotEntry, KvSnapshotEntry, VectorCollectionSnapshotEntry,
        VectorSnapshotEntry,
    };
    use strata_durability::{
        disk_snapshot::{SnapshotSection, SnapshotWriter},
        SnapshotReader,
    };

    fn writer_for(dir: &std::path::Path) -> SnapshotWriter {
        SnapshotWriter::new(dir.to_path_buf(), Box::new(IdentityCodec), [9u8; 16]).unwrap()
    }

    fn load_snapshot(path: &std::path::Path) -> LoadedSnapshot {
        SnapshotReader::new(Box::new(IdentityCodec))
            .load(path)
            .unwrap()
    }

    fn serializer() -> SnapshotSerializer {
        SnapshotSerializer::new(Box::new(IdentityCodec))
    }

    fn ns(branch_id: BranchId, space: &str) -> Arc<Namespace> {
        Arc::new(Namespace::for_branch_space(branch_id, space))
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

        let branch_bytes = strata_durability::SnapshotSerializer::new(Box::new(IdentityCodec))
            .serialize_branches(&[strata_durability::format::BranchSnapshotEntry {
                branch_id: *global_branch.as_bytes(),
                key: "my-branch".to_string(),
                value: serde_json::to_vec(&Value::String("branch-metadata".into())).unwrap(),
                version: 42,
                timestamp: 1_234,
                is_tombstone: false,
            }]);

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

        let branch_bytes = strata_durability::SnapshotSerializer::new(Box::new(IdentityCodec))
            .serialize_branches(&[strata_durability::format::BranchSnapshotEntry {
                branch_id: *global_branch.as_bytes(),
                key: "dropped".to_string(),
                value: Vec::new(),
                version: 99,
                timestamp: 2_000,
                is_tombstone: true,
            }]);

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

        let kv_bytes = serializer().serialize_kv(&[KvSnapshotEntry {
            branch_id: *branch_id.as_bytes(),
            space: "default".to_string(),
            type_tag: TypeTag::KV.as_byte(),
            user_key: b"deleted".to_vec(),
            value: Vec::new(),
            version: 10,
            timestamp: 500,
            ttl_ms: 0,
            is_tombstone: true,
        }]);

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
}
