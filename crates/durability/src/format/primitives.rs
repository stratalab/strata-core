//! Primitive serialization for snapshots
//!
//! Each primitive has a defined binary format for snapshot storage.
//! All values pass through the codec for encoding/decoding.
//!
//! # Binary Format
//!
//! All sections start with a 4-byte count of entries, followed by the entries.
//! Strings are length-prefixed (4-byte length + bytes).
//! All integers are little-endian.

use crate::codec::StorageCodec;

/// Cursor-based reader for length-prefixed binary snapshot data.
///
/// Centralizes bounds checking so deserialize methods focus on field order.
struct CursorReader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> CursorReader<'a> {
    fn new(data: &'a [u8]) -> Self {
        CursorReader { data, pos: 0 }
    }

    fn ensure(&self, n: usize) -> Result<(), PrimitiveSerializeError> {
        if self.pos + n > self.data.len() {
            Err(PrimitiveSerializeError::UnexpectedEof)
        } else {
            Ok(())
        }
    }

    fn read_u32(&mut self) -> Result<u32, PrimitiveSerializeError> {
        self.ensure(4)?;
        let val = u32::from_le_bytes(self.data[self.pos..self.pos + 4].try_into().unwrap());
        self.pos += 4;
        Ok(val)
    }

    fn read_u64(&mut self) -> Result<u64, PrimitiveSerializeError> {
        self.ensure(8)?;
        let val = u64::from_le_bytes(self.data[self.pos..self.pos + 8].try_into().unwrap());
        self.pos += 8;
        Ok(val)
    }

    fn read_u8(&mut self) -> Result<u8, PrimitiveSerializeError> {
        self.ensure(1)?;
        let val = self.data[self.pos];
        self.pos += 1;
        Ok(val)
    }

    fn read_f32(&mut self) -> Result<f32, PrimitiveSerializeError> {
        self.ensure(4)?;
        let val = f32::from_le_bytes(self.data[self.pos..self.pos + 4].try_into().unwrap());
        self.pos += 4;
        Ok(val)
    }

    fn read_exact(&mut self, n: usize) -> Result<&'a [u8], PrimitiveSerializeError> {
        self.ensure(n)?;
        let slice = &self.data[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice)
    }

    /// Read a length-prefixed UTF-8 string (4-byte len + bytes).
    fn read_string(&mut self) -> Result<String, PrimitiveSerializeError> {
        let len = self.read_u32()? as usize;
        let bytes = self.read_exact(len)?;
        String::from_utf8(bytes.to_vec()).map_err(|_| PrimitiveSerializeError::InvalidUtf8)
    }

    /// Read a length-prefixed byte blob (4-byte len + bytes).
    fn read_blob(&mut self) -> Result<&'a [u8], PrimitiveSerializeError> {
        let len = self.read_u32()? as usize;
        self.read_exact(len)
    }
}

/// Snapshot entry for KV primitive (v2 format)
///
/// Format:
/// branch_id(16) + space_len(4) + space + type_tag(1) + user_key_len(4) + user_key
/// + value_len(4) + value + version(8) + timestamp(8) + ttl_ms(8) + is_tombstone(1)
///
/// Retention metadata (`ttl_ms`, `is_tombstone`) was added in the T3-E5
/// follow-up so snapshots are retention-complete — compact() can delete WAL
/// segments covered by a snapshot without losing TTL or tombstone state.
/// When `is_tombstone` is true, `value` is conventionally empty but
/// serialized identically so the format layout stays fixed.
#[derive(Debug, Clone, PartialEq)]
pub struct KvSnapshotEntry {
    /// Branch identifier (UUID bytes)
    pub branch_id: [u8; 16],
    /// Namespace space name
    pub space: String,
    /// Storage type tag (`TypeTag` byte) for KV/Graph disambiguation
    pub type_tag: u8,
    /// Raw user-key bytes
    pub user_key: Vec<u8>,
    /// Value bytes (pre-codec). Empty when `is_tombstone` is true.
    pub value: Vec<u8>,
    /// Version counter
    pub version: u64,
    /// Timestamp (microseconds since epoch)
    pub timestamp: u64,
    /// Per-key TTL in milliseconds (0 = no expiry).
    pub ttl_ms: u64,
    /// Whether this entry is a deletion tombstone.
    pub is_tombstone: bool,
}

/// Snapshot entry for Event primitive
///
/// Format:
/// branch_id(16) + space_len(4) + space + sequence(8)
/// + payload_len(4) + payload + version(8) + timestamp(8)
#[derive(Debug, Clone, PartialEq)]
pub struct EventSnapshotEntry {
    /// Branch identifier (UUID bytes)
    pub branch_id: [u8; 16],
    /// Namespace space name
    pub space: String,
    /// Event sequence number
    pub sequence: u64,
    /// Event payload bytes (pre-codec)
    pub payload: Vec<u8>,
    /// MVCC commit version
    pub version: u64,
    /// Timestamp (microseconds since epoch)
    pub timestamp: u64,
}

/// Snapshot entry for Branch primitive (v2 format)
///
/// Format:
/// branch_id(16) + key_len(4) + key + value_len(4) + value + version(8)
/// + timestamp(8) + is_tombstone(1)
///
/// The explicit `branch_id` field was added in the T3-E5 follow-up so
/// install can dispatch entries to the correct branch on reopen. In today's
/// engine all branch metadata lives under the global nil-UUID sentinel
/// (see `strata_engine::primitives::branch::index::global_branch_id`),
/// but the DTO carries the id explicitly so future per-branch metadata
/// scoping works without another format break.
///
/// `is_tombstone` is explicit (matching the KV/JSON/Vector pattern) so
/// `branches.delete(name)` round-trips losslessly through checkpoint +
/// compact + reopen without install having to infer tombstone status from
/// a zero-length value payload.
#[derive(Debug, Clone, PartialEq)]
pub struct BranchSnapshotEntry {
    /// Branch identifier this entry belongs to (UUID bytes).
    pub branch_id: [u8; 16],
    /// Branch primitive storage key in the global namespace.
    pub key: String,
    /// Serialized `Value` bytes for the branch entry. Empty when
    /// `is_tombstone` is true.
    pub value: Vec<u8>,
    /// MVCC commit version
    pub version: u64,
    /// Timestamp (microseconds)
    pub timestamp: u64,
    /// Whether this entry is a branch-metadata deletion tombstone.
    pub is_tombstone: bool,
}

/// Snapshot entry for Json primitive (v2 format)
///
/// Format:
/// branch_id(16) + space_len(4) + space + doc_id_len(4) + doc_id
/// + content_len(4) + content + version(8) + timestamp(8) + is_tombstone(1)
///
/// JSON documents can be deleted via the `destroy` operation; the tombstone
/// marker preserves that state through checkpoint + compact + reopen.
/// When `is_tombstone` is true, `content` is conventionally empty but
/// serialized identically so the layout stays fixed.
#[derive(Debug, Clone, PartialEq)]
pub struct JsonSnapshotEntry {
    /// Branch identifier (UUID bytes)
    pub branch_id: [u8; 16],
    /// Namespace space name
    pub space: String,
    /// Document identifier
    pub doc_id: String,
    /// JSON content bytes (pre-codec). Empty when `is_tombstone` is true.
    pub content: Vec<u8>,
    /// Version counter
    pub version: u64,
    /// Timestamp (microseconds)
    pub timestamp: u64,
    /// Whether this document entry is a deletion tombstone.
    pub is_tombstone: bool,
}

/// Snapshot entry for Vector primitive (collection level)
///
/// Format:
/// branch_id(16) + space_len(4) + space + collection_name_len(4) + name
/// + config_len(4) + config + config_version(8) + config_timestamp(8)
/// + vectors_count(4) + [vectors...]
#[derive(Debug, Clone, PartialEq)]
pub struct VectorCollectionSnapshotEntry {
    /// Branch identifier (UUID bytes)
    pub branch_id: [u8; 16],
    /// Namespace space name
    pub space: String,
    /// Collection name
    pub name: String,
    /// Collection configuration as serialized bytes
    pub config: Vec<u8>,
    /// MVCC commit version for the collection config entry
    pub config_version: u64,
    /// Timestamp for the collection config entry
    pub config_timestamp: u64,
    /// Vectors in this collection
    pub vectors: Vec<VectorSnapshotEntry>,
}

/// Individual vector within a collection (v2 format)
///
/// Format:
/// key_len(4) + key + vector_id(8) + dimensions(4) + [f32...]
/// + metadata_len(4) + metadata + raw_value_len(4) + raw_value
/// + version(8) + timestamp(8) + is_tombstone(1)
///
/// Individual vector rows can be deleted (remove-by-vector-id), so the
/// tombstone marker preserves that state through checkpoint + compact +
/// reopen. Collection-level config does not carry a tombstone — collection
/// lifecycle is branch-scoped, not per-row.
#[derive(Debug, Clone, PartialEq)]
pub struct VectorSnapshotEntry {
    /// Vector key
    pub key: String,
    /// Internal vector ID
    pub vector_id: u64,
    /// Embedding values
    pub embedding: Vec<f32>,
    /// Optional metadata as serialized bytes
    pub metadata: Vec<u8>,
    /// Raw serialized `VectorRecord` bytes for lossless reinstall.
    pub raw_value: Vec<u8>,
    /// MVCC commit version for this vector entry
    pub version: u64,
    /// Timestamp for this vector entry
    pub timestamp: u64,
    /// Whether this vector row is a deletion tombstone.
    pub is_tombstone: bool,
}

/// Serializer for snapshot primitive data
pub struct SnapshotSerializer {
    codec: Box<dyn StorageCodec>,
}

impl SnapshotSerializer {
    /// Create a new serializer with the given codec
    pub fn new(codec: Box<dyn StorageCodec>) -> Self {
        SnapshotSerializer { codec }
    }

    /// Serialize KV entries to bytes (v2 format).
    pub fn serialize_kv(&self, entries: &[KvSnapshotEntry]) -> Vec<u8> {
        let mut data = Vec::new();

        // Entry count
        data.extend_from_slice(&(entries.len() as u32).to_le_bytes());

        for entry in entries {
            data.extend_from_slice(&entry.branch_id);

            let space_bytes = entry.space.as_bytes();
            data.extend_from_slice(&(space_bytes.len() as u32).to_le_bytes());
            data.extend_from_slice(space_bytes);

            data.push(entry.type_tag);

            data.extend_from_slice(&(entry.user_key.len() as u32).to_le_bytes());
            data.extend_from_slice(&entry.user_key);

            // Value (through codec). Empty for tombstones.
            let value_bytes = self.codec.encode(&entry.value);
            data.extend_from_slice(&(value_bytes.len() as u32).to_le_bytes());
            data.extend_from_slice(&value_bytes);

            // Version and timestamp
            data.extend_from_slice(&entry.version.to_le_bytes());
            data.extend_from_slice(&entry.timestamp.to_le_bytes());

            // Retention metadata (v2)
            data.extend_from_slice(&entry.ttl_ms.to_le_bytes());
            data.push(if entry.is_tombstone { 1 } else { 0 });
        }

        data
    }

    /// Deserialize KV entries from bytes (v2 format).
    pub fn deserialize_kv(
        &self,
        data: &[u8],
    ) -> Result<Vec<KvSnapshotEntry>, PrimitiveSerializeError> {
        let mut r = CursorReader::new(data);
        let count = r.read_u32()? as usize;
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            let branch_id: [u8; 16] = r.read_exact(16)?.try_into().unwrap();
            let space = r.read_string()?;
            let type_tag = r.read_u8()?;
            let user_key = r.read_blob()?.to_vec();
            let value = self.codec.decode(r.read_blob()?)?;
            let version = r.read_u64()?;
            let timestamp = r.read_u64()?;
            let ttl_ms = r.read_u64()?;
            let is_tombstone = r.read_u8()? != 0;
            entries.push(KvSnapshotEntry {
                branch_id,
                space,
                type_tag,
                user_key,
                value,
                version,
                timestamp,
                ttl_ms,
                is_tombstone,
            });
        }
        Ok(entries)
    }

    /// Serialize Event entries to bytes
    pub fn serialize_events(&self, entries: &[EventSnapshotEntry]) -> Vec<u8> {
        let mut data = Vec::new();

        data.extend_from_slice(&(entries.len() as u32).to_le_bytes());

        for entry in entries {
            data.extend_from_slice(&entry.branch_id);

            let space_bytes = entry.space.as_bytes();
            data.extend_from_slice(&(space_bytes.len() as u32).to_le_bytes());
            data.extend_from_slice(space_bytes);

            data.extend_from_slice(&entry.sequence.to_le_bytes());

            let payload_bytes = self.codec.encode(&entry.payload);
            data.extend_from_slice(&(payload_bytes.len() as u32).to_le_bytes());
            data.extend_from_slice(&payload_bytes);

            data.extend_from_slice(&entry.version.to_le_bytes());
            data.extend_from_slice(&entry.timestamp.to_le_bytes());
        }

        data
    }

    /// Deserialize Event entries from bytes
    pub fn deserialize_events(
        &self,
        data: &[u8],
    ) -> Result<Vec<EventSnapshotEntry>, PrimitiveSerializeError> {
        let mut r = CursorReader::new(data);
        let count = r.read_u32()? as usize;
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            let branch_id: [u8; 16] = r.read_exact(16)?.try_into().unwrap();
            let space = r.read_string()?;
            let sequence = r.read_u64()?;
            let payload = self.codec.decode(r.read_blob()?)?;
            let version = r.read_u64()?;
            let timestamp = r.read_u64()?;
            entries.push(EventSnapshotEntry {
                branch_id,
                space,
                sequence,
                payload,
                version,
                timestamp,
            });
        }
        Ok(entries)
    }

    /// Serialize Branch entries to bytes (v2 format).
    pub fn serialize_branches(&self, entries: &[BranchSnapshotEntry]) -> Vec<u8> {
        let mut data = Vec::new();

        data.extend_from_slice(&(entries.len() as u32).to_le_bytes());

        for entry in entries {
            data.extend_from_slice(&entry.branch_id);

            let key_bytes = entry.key.as_bytes();
            data.extend_from_slice(&(key_bytes.len() as u32).to_le_bytes());
            data.extend_from_slice(key_bytes);

            let value_bytes = self.codec.encode(&entry.value);
            data.extend_from_slice(&(value_bytes.len() as u32).to_le_bytes());
            data.extend_from_slice(&value_bytes);

            data.extend_from_slice(&entry.version.to_le_bytes());
            data.extend_from_slice(&entry.timestamp.to_le_bytes());

            // Retention metadata (v2): explicit tombstone marker, matching
            // the KV/JSON/Vector pattern instead of inferring from empty bytes.
            data.push(if entry.is_tombstone { 1 } else { 0 });
        }

        data
    }

    /// Deserialize Branch entries from bytes (v2 format).
    pub fn deserialize_branches(
        &self,
        data: &[u8],
    ) -> Result<Vec<BranchSnapshotEntry>, PrimitiveSerializeError> {
        let mut r = CursorReader::new(data);
        let count = r.read_u32()? as usize;
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            let branch_id: [u8; 16] = r.read_exact(16)?.try_into().unwrap();
            let key = r.read_string()?;
            let value = self.codec.decode(r.read_blob()?)?;
            let version = r.read_u64()?;
            let timestamp = r.read_u64()?;
            let is_tombstone = r.read_u8()? != 0;
            entries.push(BranchSnapshotEntry {
                branch_id,
                key,
                value,
                version,
                timestamp,
                is_tombstone,
            });
        }
        Ok(entries)
    }

    /// Serialize Json entries to bytes
    pub fn serialize_json(&self, entries: &[JsonSnapshotEntry]) -> Vec<u8> {
        let mut data = Vec::new();

        data.extend_from_slice(&(entries.len() as u32).to_le_bytes());

        for entry in entries {
            data.extend_from_slice(&entry.branch_id);

            let space_bytes = entry.space.as_bytes();
            data.extend_from_slice(&(space_bytes.len() as u32).to_le_bytes());
            data.extend_from_slice(space_bytes);

            let doc_id_bytes = entry.doc_id.as_bytes();
            data.extend_from_slice(&(doc_id_bytes.len() as u32).to_le_bytes());
            data.extend_from_slice(doc_id_bytes);

            let content_bytes = self.codec.encode(&entry.content);
            data.extend_from_slice(&(content_bytes.len() as u32).to_le_bytes());
            data.extend_from_slice(&content_bytes);

            data.extend_from_slice(&entry.version.to_le_bytes());
            data.extend_from_slice(&entry.timestamp.to_le_bytes());

            // Retention metadata (v2): tombstone marker.
            data.push(if entry.is_tombstone { 1 } else { 0 });
        }

        data
    }

    /// Deserialize Json entries from bytes (v2 format).
    pub fn deserialize_json(
        &self,
        data: &[u8],
    ) -> Result<Vec<JsonSnapshotEntry>, PrimitiveSerializeError> {
        let mut r = CursorReader::new(data);
        let count = r.read_u32()? as usize;
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            let branch_id: [u8; 16] = r.read_exact(16)?.try_into().unwrap();
            let space = r.read_string()?;
            let doc_id = r.read_string()?;
            let content = self.codec.decode(r.read_blob()?)?;
            let version = r.read_u64()?;
            let timestamp = r.read_u64()?;
            let is_tombstone = r.read_u8()? != 0;
            entries.push(JsonSnapshotEntry {
                branch_id,
                space,
                doc_id,
                content,
                version,
                timestamp,
                is_tombstone,
            });
        }
        Ok(entries)
    }

    /// Serialize Vector collections to bytes
    pub fn serialize_vectors(&self, collections: &[VectorCollectionSnapshotEntry]) -> Vec<u8> {
        let mut data = Vec::new();

        data.extend_from_slice(&(collections.len() as u32).to_le_bytes());

        for collection in collections {
            data.extend_from_slice(&collection.branch_id);

            let space_bytes = collection.space.as_bytes();
            data.extend_from_slice(&(space_bytes.len() as u32).to_le_bytes());
            data.extend_from_slice(space_bytes);

            // Collection name
            let name_bytes = collection.name.as_bytes();
            data.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
            data.extend_from_slice(name_bytes);

            // Config (through codec)
            let config_bytes = self.codec.encode(&collection.config);
            data.extend_from_slice(&(config_bytes.len() as u32).to_le_bytes());
            data.extend_from_slice(&config_bytes);
            data.extend_from_slice(&collection.config_version.to_le_bytes());
            data.extend_from_slice(&collection.config_timestamp.to_le_bytes());

            // Vectors
            data.extend_from_slice(&(collection.vectors.len() as u32).to_le_bytes());
            for vector in &collection.vectors {
                // Key
                let key_bytes = vector.key.as_bytes();
                data.extend_from_slice(&(key_bytes.len() as u32).to_le_bytes());
                data.extend_from_slice(key_bytes);

                // Vector ID
                data.extend_from_slice(&vector.vector_id.to_le_bytes());

                // Embedding dimensions and values
                data.extend_from_slice(&(vector.embedding.len() as u32).to_le_bytes());
                for &value in &vector.embedding {
                    data.extend_from_slice(&value.to_le_bytes());
                }

                // Metadata (through codec)
                let metadata_bytes = self.codec.encode(&vector.metadata);
                data.extend_from_slice(&(metadata_bytes.len() as u32).to_le_bytes());
                data.extend_from_slice(&metadata_bytes);
                data.extend_from_slice(&(vector.raw_value.len() as u32).to_le_bytes());
                data.extend_from_slice(&vector.raw_value);
                data.extend_from_slice(&vector.version.to_le_bytes());
                data.extend_from_slice(&vector.timestamp.to_le_bytes());

                // Retention metadata (v2): per-vector tombstone marker.
                data.push(if vector.is_tombstone { 1 } else { 0 });
            }
        }

        data
    }

    /// Deserialize Vector collections from bytes (v2 format).
    pub fn deserialize_vectors(
        &self,
        data: &[u8],
    ) -> Result<Vec<VectorCollectionSnapshotEntry>, PrimitiveSerializeError> {
        let mut r = CursorReader::new(data);
        let collections_count = r.read_u32()? as usize;
        let mut collections = Vec::with_capacity(collections_count);

        for _ in 0..collections_count {
            let branch_id: [u8; 16] = r.read_exact(16)?.try_into().unwrap();
            let space = r.read_string()?;
            let name = r.read_string()?;
            let config = self.codec.decode(r.read_blob()?)?;
            let config_version = r.read_u64()?;
            let config_timestamp = r.read_u64()?;
            let vectors_count = r.read_u32()? as usize;

            let mut vectors = Vec::with_capacity(vectors_count);
            for _ in 0..vectors_count {
                let key = r.read_string()?;
                let vector_id = r.read_u64()?;
                let dims = r.read_u32()? as usize;
                let mut embedding = Vec::with_capacity(dims);
                for _ in 0..dims {
                    embedding.push(r.read_f32()?);
                }
                let metadata = self.codec.decode(r.read_blob()?)?;
                let raw_value = r.read_blob()?.to_vec();
                let version = r.read_u64()?;
                let timestamp = r.read_u64()?;
                let is_tombstone = r.read_u8()? != 0;
                vectors.push(VectorSnapshotEntry {
                    key,
                    vector_id,
                    embedding,
                    metadata,
                    raw_value,
                    version,
                    timestamp,
                    is_tombstone,
                });
            }

            collections.push(VectorCollectionSnapshotEntry {
                branch_id,
                space,
                name,
                config,
                config_version,
                config_timestamp,
                vectors,
            });
        }

        Ok(collections)
    }
}

/// Errors that can occur during primitive serialization
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum PrimitiveSerializeError {
    /// Unexpected end of data
    #[error("Unexpected end of data")]
    UnexpectedEof,
    /// Invalid UTF-8 string
    #[error("Invalid UTF-8 string")]
    InvalidUtf8,
    /// Codec error
    #[error("Codec error: {0}")]
    Codec(#[from] crate::codec::CodecError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::IdentityCodec;

    fn test_serializer() -> SnapshotSerializer {
        SnapshotSerializer::new(Box::new(IdentityCodec))
    }

    fn test_branch(bytes: u8) -> [u8; 16] {
        [bytes; 16]
    }

    #[test]
    fn test_kv_roundtrip() {
        let serializer = test_serializer();

        let entries = vec![
            KvSnapshotEntry {
                branch_id: test_branch(1),
                space: "default".to_string(),
                type_tag: 0x01,
                user_key: b"key1".to_vec(),
                value: b"value1".to_vec(),
                version: 1,
                timestamp: 1000,
                ttl_ms: 0,
                is_tombstone: false,
            },
            KvSnapshotEntry {
                branch_id: test_branch(2),
                space: "tenant_a".to_string(),
                type_tag: 0x07,
                user_key: b"key2".to_vec(),
                value: b"value2".to_vec(),
                version: 2,
                timestamp: 2000,
                ttl_ms: 60_000,
                is_tombstone: false,
            },
        ];

        let data = serializer.serialize_kv(&entries);
        let parsed = serializer.deserialize_kv(&data).unwrap();

        assert_eq!(entries, parsed);
    }

    #[test]
    fn test_kv_empty() {
        let serializer = test_serializer();

        let entries: Vec<KvSnapshotEntry> = vec![];
        let data = serializer.serialize_kv(&entries);
        let parsed = serializer.deserialize_kv(&data).unwrap();

        assert!(parsed.is_empty());
    }

    #[test]
    fn test_kv_unicode() {
        let serializer = test_serializer();

        let entries = vec![KvSnapshotEntry {
            branch_id: test_branch(9),
            space: "tenant_emoji".to_string(),
            type_tag: 0x01,
            user_key: "key_\u{1F600}_emoji".as_bytes().to_vec(),
            value: "value_\u{4E2D}\u{6587}_chinese".as_bytes().to_vec(),
            version: 42,
            timestamp: 9999,
            ttl_ms: 0,
            is_tombstone: false,
        }];

        let data = serializer.serialize_kv(&entries);
        let parsed = serializer.deserialize_kv(&data).unwrap();

        assert_eq!(entries, parsed);
    }

    #[test]
    fn test_kv_binary_keys_roundtrip() {
        let serializer = test_serializer();

        let entries = vec![KvSnapshotEntry {
            branch_id: test_branch(10),
            space: "binary".to_string(),
            type_tag: 0x01,
            user_key: vec![0x00, 0xFF, 0x80, 0x01],
            value: b"value".to_vec(),
            version: 7,
            timestamp: 12_345,
            ttl_ms: 0,
            is_tombstone: false,
        }];

        let data = serializer.serialize_kv(&entries);
        let parsed = serializer.deserialize_kv(&data).unwrap();

        assert_eq!(entries, parsed);
    }

    #[test]
    fn test_events_roundtrip() {
        let serializer = test_serializer();

        let entries = vec![
            EventSnapshotEntry {
                branch_id: test_branch(3),
                space: "default".to_string(),
                sequence: 1,
                payload: b"event1".to_vec(),
                version: 11,
                timestamp: 1000,
            },
            EventSnapshotEntry {
                branch_id: test_branch(4),
                space: "tenant_events".to_string(),
                sequence: 2,
                payload: b"event2".to_vec(),
                version: 12,
                timestamp: 2000,
            },
        ];

        let data = serializer.serialize_events(&entries);
        let parsed = serializer.deserialize_events(&data).unwrap();

        assert_eq!(entries, parsed);
    }

    #[test]
    fn test_branches_roundtrip() {
        let serializer = test_serializer();

        let entries = vec![
            BranchSnapshotEntry {
                branch_id: test_branch(0),
                key: "my-branch".to_string(),
                value: b"{}".to_vec(),
                version: 17,
                timestamp: 1000,
                is_tombstone: false,
            },
            BranchSnapshotEntry {
                branch_id: test_branch(0),
                key: "deleted-branch".to_string(),
                value: Vec::new(),
                version: 18,
                timestamp: 2000,
                is_tombstone: true,
            },
        ];

        let data = serializer.serialize_branches(&entries);
        let parsed = serializer.deserialize_branches(&data).unwrap();

        assert_eq!(entries, parsed);
    }

    #[test]
    fn test_json_roundtrip() {
        let serializer = test_serializer();

        let entries = vec![
            JsonSnapshotEntry {
                branch_id: test_branch(5),
                space: "default".to_string(),
                doc_id: "doc1".to_string(),
                content: b"{\"name\":\"test\"}".to_vec(),
                version: 1,
                timestamp: 1000,
                is_tombstone: false,
            },
            JsonSnapshotEntry {
                branch_id: test_branch(6),
                space: "products".to_string(),
                doc_id: "doc2".to_string(),
                content: b"{\"value\":42}".to_vec(),
                version: 2,
                timestamp: 2000,
                is_tombstone: false,
            },
        ];

        let data = serializer.serialize_json(&entries);
        let parsed = serializer.deserialize_json(&data).unwrap();

        assert_eq!(entries, parsed);
    }

    #[test]
    fn test_vectors_roundtrip() {
        let serializer = test_serializer();

        let collections = vec![VectorCollectionSnapshotEntry {
            branch_id: test_branch(7),
            space: "embeddings".to_string(),
            name: "embeddings".to_string(),
            config: b"{\"dimensions\":384}".to_vec(),
            config_version: 21,
            config_timestamp: 1111,
            vectors: vec![
                VectorSnapshotEntry {
                    key: "vec1".to_string(),
                    vector_id: 1,
                    embedding: vec![0.1, 0.2, 0.3],
                    metadata: b"{}".to_vec(),
                    raw_value: b"raw-vec1".to_vec(),
                    version: 31,
                    timestamp: 2222,
                    is_tombstone: false,
                },
                VectorSnapshotEntry {
                    key: "vec2".to_string(),
                    vector_id: 2,
                    embedding: vec![0.4, 0.5, 0.6],
                    metadata: b"{\"label\":\"test\"}".to_vec(),
                    raw_value: b"raw-vec2".to_vec(),
                    version: 32,
                    timestamp: 3333,
                    is_tombstone: false,
                },
            ],
        }];

        let data = serializer.serialize_vectors(&collections);
        let parsed = serializer.deserialize_vectors(&data).unwrap();

        assert_eq!(collections, parsed);
    }

    #[test]
    fn test_vectors_high_dimension() {
        let serializer = test_serializer();

        // Test with 384 dimensions (MiniLM)
        let embedding: Vec<f32> = (0..384).map(|i| i as f32 * 0.001).collect();

        let collections = vec![VectorCollectionSnapshotEntry {
            branch_id: test_branch(8),
            space: "semantic".to_string(),
            name: "minilm".to_string(),
            config: b"{\"dimensions\":384}".to_vec(),
            config_version: 41,
            config_timestamp: 4444,
            vectors: vec![VectorSnapshotEntry {
                key: "high-dim".to_string(),
                vector_id: 1,
                embedding: embedding.clone(),
                metadata: vec![],
                raw_value: vec![1, 2, 3, 4],
                version: 51,
                timestamp: 5555,
                is_tombstone: false,
            }],
        }];

        let data = serializer.serialize_vectors(&collections);
        let parsed = serializer.deserialize_vectors(&data).unwrap();

        assert_eq!(parsed[0].vectors[0].embedding.len(), 384);
        assert_eq!(parsed[0].vectors[0].embedding, embedding);
    }

    #[test]
    fn test_deserialize_truncated_data() {
        let serializer = test_serializer();

        // Truncated KV data
        let result = serializer.deserialize_kv(&[0, 0, 0, 1]); // Says 1 entry but no data
        assert!(matches!(
            result,
            Err(PrimitiveSerializeError::UnexpectedEof)
        ));

        // Too short
        let result = serializer.deserialize_kv(&[0, 0]);
        assert!(matches!(
            result,
            Err(PrimitiveSerializeError::UnexpectedEof)
        ));
    }
}
