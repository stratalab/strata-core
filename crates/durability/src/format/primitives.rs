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

/// Snapshot entry for KV primitive
///
/// Format: key_len(4) + key + value_len(4) + value + version(8) + timestamp(8)
#[derive(Debug, Clone, PartialEq)]
pub struct KvSnapshotEntry {
    /// Key string
    pub key: String,
    /// Value bytes (pre-codec)
    pub value: Vec<u8>,
    /// Version counter
    pub version: u64,
    /// Timestamp (microseconds since epoch)
    pub timestamp: u64,
}

/// Snapshot entry for Event primitive
///
/// Format: sequence(8) + payload_len(4) + payload + timestamp(8)
#[derive(Debug, Clone, PartialEq)]
pub struct EventSnapshotEntry {
    /// Event sequence number
    pub sequence: u64,
    /// Event payload bytes (pre-codec)
    pub payload: Vec<u8>,
    /// Timestamp (microseconds since epoch)
    pub timestamp: u64,
}

/// Snapshot entry for Run primitive
///
/// Format: branch_id(16 bytes UUID) + name_len(4) + name + created_at(8) + metadata_len(4) + metadata
#[derive(Debug, Clone, PartialEq)]
pub struct BranchSnapshotEntry {
    /// Run identifier (UUID bytes)
    pub branch_id: [u8; 16],
    /// Run name
    pub name: String,
    /// Creation timestamp (microseconds)
    pub created_at: u64,
    /// Metadata as serialized bytes
    pub metadata: Vec<u8>,
}

/// Snapshot entry for Json primitive
///
/// Format: doc_id_len(4) + doc_id + content_len(4) + content + version(8) + timestamp(8)
#[derive(Debug, Clone, PartialEq)]
pub struct JsonSnapshotEntry {
    /// Document identifier
    pub doc_id: String,
    /// JSON content bytes (pre-codec)
    pub content: Vec<u8>,
    /// Version counter
    pub version: u64,
    /// Timestamp (microseconds)
    pub timestamp: u64,
}

/// Snapshot entry for Vector primitive (collection level)
///
/// Format: collection_name_len(4) + name + config_len(4) + config + vectors_count(4) + [vectors...]
#[derive(Debug, Clone, PartialEq)]
pub struct VectorCollectionSnapshotEntry {
    /// Collection name
    pub name: String,
    /// Collection configuration as serialized bytes
    pub config: Vec<u8>,
    /// Vectors in this collection
    pub vectors: Vec<VectorSnapshotEntry>,
}

/// Individual vector within a collection
///
/// Format: key_len(4) + key + vector_id(8) + dimensions(4) + [f32...] + metadata_len(4) + metadata
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

    /// Serialize KV entries to bytes
    pub fn serialize_kv(&self, entries: &[KvSnapshotEntry]) -> Vec<u8> {
        let mut data = Vec::new();

        // Entry count
        data.extend_from_slice(&(entries.len() as u32).to_le_bytes());

        for entry in entries {
            // Key
            let key_bytes = entry.key.as_bytes();
            data.extend_from_slice(&(key_bytes.len() as u32).to_le_bytes());
            data.extend_from_slice(key_bytes);

            // Value (through codec)
            let value_bytes = self.codec.encode(&entry.value);
            data.extend_from_slice(&(value_bytes.len() as u32).to_le_bytes());
            data.extend_from_slice(&value_bytes);

            // Version and timestamp
            data.extend_from_slice(&entry.version.to_le_bytes());
            data.extend_from_slice(&entry.timestamp.to_le_bytes());
        }

        data
    }

    /// Deserialize KV entries from bytes
    pub fn deserialize_kv(
        &self,
        data: &[u8],
    ) -> Result<Vec<KvSnapshotEntry>, PrimitiveSerializeError> {
        let mut r = CursorReader::new(data);
        let count = r.read_u32()? as usize;
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            let key = r.read_string()?;
            let value = self.codec.decode(r.read_blob()?)?;
            let version = r.read_u64()?;
            let timestamp = r.read_u64()?;
            entries.push(KvSnapshotEntry {
                key,
                value,
                version,
                timestamp,
            });
        }
        Ok(entries)
    }

    /// Serialize Event entries to bytes
    pub fn serialize_events(&self, entries: &[EventSnapshotEntry]) -> Vec<u8> {
        let mut data = Vec::new();

        data.extend_from_slice(&(entries.len() as u32).to_le_bytes());

        for entry in entries {
            data.extend_from_slice(&entry.sequence.to_le_bytes());

            let payload_bytes = self.codec.encode(&entry.payload);
            data.extend_from_slice(&(payload_bytes.len() as u32).to_le_bytes());
            data.extend_from_slice(&payload_bytes);

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
            let sequence = r.read_u64()?;
            let payload = self.codec.decode(r.read_blob()?)?;
            let timestamp = r.read_u64()?;
            entries.push(EventSnapshotEntry {
                sequence,
                payload,
                timestamp,
            });
        }
        Ok(entries)
    }

    /// Serialize Run entries to bytes
    pub fn serialize_branches(&self, entries: &[BranchSnapshotEntry]) -> Vec<u8> {
        let mut data = Vec::new();

        data.extend_from_slice(&(entries.len() as u32).to_le_bytes());

        for entry in entries {
            data.extend_from_slice(&entry.branch_id);

            let name_bytes = entry.name.as_bytes();
            data.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
            data.extend_from_slice(name_bytes);

            data.extend_from_slice(&entry.created_at.to_le_bytes());

            let metadata_bytes = self.codec.encode(&entry.metadata);
            data.extend_from_slice(&(metadata_bytes.len() as u32).to_le_bytes());
            data.extend_from_slice(&metadata_bytes);
        }

        data
    }

    /// Deserialize Run entries from bytes
    pub fn deserialize_branches(
        &self,
        data: &[u8],
    ) -> Result<Vec<BranchSnapshotEntry>, PrimitiveSerializeError> {
        let mut r = CursorReader::new(data);
        let count = r.read_u32()? as usize;
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            let branch_id: [u8; 16] = r.read_exact(16)?.try_into().unwrap();
            let name = r.read_string()?;
            let created_at = r.read_u64()?;
            let metadata = self.codec.decode(r.read_blob()?)?;
            entries.push(BranchSnapshotEntry {
                branch_id,
                name,
                created_at,
                metadata,
            });
        }
        Ok(entries)
    }

    /// Serialize Json entries to bytes
    pub fn serialize_json(&self, entries: &[JsonSnapshotEntry]) -> Vec<u8> {
        let mut data = Vec::new();

        data.extend_from_slice(&(entries.len() as u32).to_le_bytes());

        for entry in entries {
            let doc_id_bytes = entry.doc_id.as_bytes();
            data.extend_from_slice(&(doc_id_bytes.len() as u32).to_le_bytes());
            data.extend_from_slice(doc_id_bytes);

            let content_bytes = self.codec.encode(&entry.content);
            data.extend_from_slice(&(content_bytes.len() as u32).to_le_bytes());
            data.extend_from_slice(&content_bytes);

            data.extend_from_slice(&entry.version.to_le_bytes());
            data.extend_from_slice(&entry.timestamp.to_le_bytes());
        }

        data
    }

    /// Deserialize Json entries from bytes
    pub fn deserialize_json(
        &self,
        data: &[u8],
    ) -> Result<Vec<JsonSnapshotEntry>, PrimitiveSerializeError> {
        let mut r = CursorReader::new(data);
        let count = r.read_u32()? as usize;
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            let doc_id = r.read_string()?;
            let content = self.codec.decode(r.read_blob()?)?;
            let version = r.read_u64()?;
            let timestamp = r.read_u64()?;
            entries.push(JsonSnapshotEntry {
                doc_id,
                content,
                version,
                timestamp,
            });
        }
        Ok(entries)
    }

    /// Serialize Vector collections to bytes
    pub fn serialize_vectors(&self, collections: &[VectorCollectionSnapshotEntry]) -> Vec<u8> {
        let mut data = Vec::new();

        data.extend_from_slice(&(collections.len() as u32).to_le_bytes());

        for collection in collections {
            // Collection name
            let name_bytes = collection.name.as_bytes();
            data.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
            data.extend_from_slice(name_bytes);

            // Config (through codec)
            let config_bytes = self.codec.encode(&collection.config);
            data.extend_from_slice(&(config_bytes.len() as u32).to_le_bytes());
            data.extend_from_slice(&config_bytes);

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
            }
        }

        data
    }

    /// Deserialize Vector collections from bytes
    pub fn deserialize_vectors(
        &self,
        data: &[u8],
    ) -> Result<Vec<VectorCollectionSnapshotEntry>, PrimitiveSerializeError> {
        let mut r = CursorReader::new(data);
        let collections_count = r.read_u32()? as usize;
        let mut collections = Vec::with_capacity(collections_count);

        for _ in 0..collections_count {
            let name = r.read_string()?;
            let config = self.codec.decode(r.read_blob()?)?;
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
                vectors.push(VectorSnapshotEntry {
                    key,
                    vector_id,
                    embedding,
                    metadata,
                });
            }

            collections.push(VectorCollectionSnapshotEntry {
                name,
                config,
                vectors,
            });
        }

        Ok(collections)
    }
}

/// Errors that can occur during primitive serialization
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

    #[test]
    fn test_kv_roundtrip() {
        let serializer = test_serializer();

        let entries = vec![
            KvSnapshotEntry {
                key: "key1".to_string(),
                value: b"value1".to_vec(),
                version: 1,
                timestamp: 1000,
            },
            KvSnapshotEntry {
                key: "key2".to_string(),
                value: b"value2".to_vec(),
                version: 2,
                timestamp: 2000,
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
            key: "key_\u{1F600}_emoji".to_string(),
            value: "value_\u{4E2D}\u{6587}_chinese".as_bytes().to_vec(),
            version: 42,
            timestamp: 9999,
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
                sequence: 1,
                payload: b"event1".to_vec(),
                timestamp: 1000,
            },
            EventSnapshotEntry {
                sequence: 2,
                payload: b"event2".to_vec(),
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

        let entries = vec![BranchSnapshotEntry {
            branch_id: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            name: "my-branch".to_string(),
            created_at: 1000,
            metadata: b"{}".to_vec(),
        }];

        let data = serializer.serialize_branches(&entries);
        let parsed = serializer.deserialize_branches(&data).unwrap();

        assert_eq!(entries, parsed);
    }

    #[test]
    fn test_json_roundtrip() {
        let serializer = test_serializer();

        let entries = vec![
            JsonSnapshotEntry {
                doc_id: "doc1".to_string(),
                content: b"{\"name\":\"test\"}".to_vec(),
                version: 1,
                timestamp: 1000,
            },
            JsonSnapshotEntry {
                doc_id: "doc2".to_string(),
                content: b"{\"value\":42}".to_vec(),
                version: 2,
                timestamp: 2000,
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
            name: "embeddings".to_string(),
            config: b"{\"dimensions\":384}".to_vec(),
            vectors: vec![
                VectorSnapshotEntry {
                    key: "vec1".to_string(),
                    vector_id: 1,
                    embedding: vec![0.1, 0.2, 0.3],
                    metadata: b"{}".to_vec(),
                },
                VectorSnapshotEntry {
                    key: "vec2".to_string(),
                    vector_id: 2,
                    embedding: vec![0.4, 0.5, 0.6],
                    metadata: b"{\"label\":\"test\"}".to_vec(),
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
            name: "minilm".to_string(),
            config: b"{\"dimensions\":384}".to_vec(),
            vectors: vec![VectorSnapshotEntry {
                key: "high-dim".to_string(),
                vector_id: 1,
                embedding: embedding.clone(),
                metadata: vec![],
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
