//! Transaction writeset serialization format.
//!
//! A writeset contains all mutations from a committed transaction.
//! This module provides compact binary serialization for WAL persistence.
//!
//! # Format
//!
//! ```text
//! Writeset Layout:
//! ┌──────────────────┬──────────────────────────────────────────────┐
//! │ Count (4 bytes)  │ Mutations (variable)                         │
//! └──────────────────┴──────────────────────────────────────────────┘
//!
//! Mutation Layout:
//! ┌──────────────────┬──────────────────┬────────────────────────────┐
//! │ Tag (1 byte)     │ EntityRef        │ Value/Version (variant)    │
//! └──────────────────┴──────────────────┴────────────────────────────┘
//!
//! EntityRef Layout:
//! ┌──────────────────┬──────────────────┬────────────────────────────┐
//! │ Tag (1 byte)     │ BranchId (16 bytes) │ Variant fields             │
//! └──────────────────┴──────────────────┴────────────────────────────┘
//! ```

use strata_core::{BranchId, EntityRef};

use super::primitive_tags;

/// Mutation tag bytes
const MUTATION_PUT: u8 = 0x01;
const MUTATION_DELETE: u8 = 0x02;
const MUTATION_APPEND: u8 = 0x03;

/// A mutation within a transaction writeset.
///
/// Each mutation represents a single operation that modifies the database state.
/// The version is assigned by the engine before persistence, NOT by storage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Mutation {
    /// Put (create or update) a value.
    ///
    /// Used for KV, State, Json, Vector, Run primitives.
    Put {
        /// Entity being modified
        entity_ref: EntityRef,
        /// Serialized value (codec-encoded)
        value: Vec<u8>,
        /// Version assigned by engine
        version: u64,
    },

    /// Delete an entity.
    Delete {
        /// Entity being deleted
        entity_ref: EntityRef,
    },

    /// Append to an append-only entity.
    ///
    /// Used for Event and Trace primitives.
    Append {
        /// Entity being appended to
        entity_ref: EntityRef,
        /// Serialized value (codec-encoded)
        value: Vec<u8>,
        /// Version assigned by engine
        version: u64,
    },
}

impl Mutation {
    /// Get the entity reference for this mutation.
    pub fn entity_ref(&self) -> &EntityRef {
        match self {
            Mutation::Put { entity_ref, .. } => entity_ref,
            Mutation::Delete { entity_ref } => entity_ref,
            Mutation::Append { entity_ref, .. } => entity_ref,
        }
    }
}

/// Transaction writeset containing all mutations.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Writeset {
    /// List of mutations in commit order
    pub mutations: Vec<Mutation>,
}

impl Writeset {
    /// Create an empty writeset.
    pub fn new() -> Self {
        Writeset {
            mutations: Vec::new(),
        }
    }

    /// Create a writeset with the given capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Writeset {
            mutations: Vec::with_capacity(capacity),
        }
    }

    /// Add a Put mutation.
    pub fn put(&mut self, entity_ref: EntityRef, value: Vec<u8>, version: u64) {
        self.mutations.push(Mutation::Put {
            entity_ref,
            value,
            version,
        });
    }

    /// Add a Delete mutation.
    pub fn delete(&mut self, entity_ref: EntityRef) {
        self.mutations.push(Mutation::Delete { entity_ref });
    }

    /// Add an Append mutation.
    pub fn append(&mut self, entity_ref: EntityRef, value: Vec<u8>, version: u64) {
        self.mutations.push(Mutation::Append {
            entity_ref,
            value,
            version,
        });
    }

    /// Check if writeset is empty.
    pub fn is_empty(&self) -> bool {
        self.mutations.is_empty()
    }

    /// Get number of mutations.
    pub fn len(&self) -> usize {
        self.mutations.len()
    }

    /// Serialize writeset to bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        // Write mutation count
        bytes.extend_from_slice(&(self.mutations.len() as u32).to_le_bytes());

        // Write each mutation
        for mutation in &self.mutations {
            Self::write_mutation(&mut bytes, mutation);
        }

        bytes
    }

    /// Deserialize writeset from bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, WritesetError> {
        if bytes.len() < 4 {
            return Err(WritesetError::InsufficientData);
        }

        let count = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
        let mut cursor = 4;
        let mut mutations = Vec::with_capacity(count);

        for _ in 0..count {
            let (mutation, consumed) = Self::read_mutation(&bytes[cursor..])?;
            mutations.push(mutation);
            cursor += consumed;
        }

        Ok(Writeset { mutations })
    }

    /// Write a single mutation to the byte buffer.
    fn write_mutation(bytes: &mut Vec<u8>, mutation: &Mutation) {
        match mutation {
            Mutation::Put {
                entity_ref,
                value,
                version,
            } => {
                bytes.push(MUTATION_PUT);
                Self::write_entity_ref(bytes, entity_ref);
                bytes.extend_from_slice(&version.to_le_bytes());
                bytes.extend_from_slice(&(value.len() as u32).to_le_bytes());
                bytes.extend_from_slice(value);
            }
            Mutation::Delete { entity_ref } => {
                bytes.push(MUTATION_DELETE);
                Self::write_entity_ref(bytes, entity_ref);
            }
            Mutation::Append {
                entity_ref,
                value,
                version,
            } => {
                bytes.push(MUTATION_APPEND);
                Self::write_entity_ref(bytes, entity_ref);
                bytes.extend_from_slice(&version.to_le_bytes());
                bytes.extend_from_slice(&(value.len() as u32).to_le_bytes());
                bytes.extend_from_slice(value);
            }
        }
    }

    /// Read a single mutation from bytes.
    fn read_mutation(bytes: &[u8]) -> Result<(Mutation, usize), WritesetError> {
        if bytes.is_empty() {
            return Err(WritesetError::InsufficientData);
        }

        let tag = bytes[0];
        let mut cursor = 1;

        match tag {
            MUTATION_PUT => {
                let (entity_ref, consumed) = Self::read_entity_ref(&bytes[cursor..])?;
                cursor += consumed;

                if bytes.len() < cursor + 12 {
                    return Err(WritesetError::InsufficientData);
                }

                let version = u64::from_le_bytes(bytes[cursor..cursor + 8].try_into().unwrap());
                cursor += 8;

                let value_len =
                    u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().unwrap()) as usize;
                cursor += 4;

                if bytes.len() < cursor + value_len {
                    return Err(WritesetError::InsufficientData);
                }

                let value = bytes[cursor..cursor + value_len].to_vec();
                cursor += value_len;

                Ok((
                    Mutation::Put {
                        entity_ref,
                        value,
                        version,
                    },
                    cursor,
                ))
            }
            MUTATION_DELETE => {
                let (entity_ref, consumed) = Self::read_entity_ref(&bytes[cursor..])?;
                cursor += consumed;
                Ok((Mutation::Delete { entity_ref }, cursor))
            }
            MUTATION_APPEND => {
                let (entity_ref, consumed) = Self::read_entity_ref(&bytes[cursor..])?;
                cursor += consumed;

                if bytes.len() < cursor + 12 {
                    return Err(WritesetError::InsufficientData);
                }

                let version = u64::from_le_bytes(bytes[cursor..cursor + 8].try_into().unwrap());
                cursor += 8;

                let value_len =
                    u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().unwrap()) as usize;
                cursor += 4;

                if bytes.len() < cursor + value_len {
                    return Err(WritesetError::InsufficientData);
                }

                let value = bytes[cursor..cursor + value_len].to_vec();
                cursor += value_len;

                Ok((
                    Mutation::Append {
                        entity_ref,
                        value,
                        version,
                    },
                    cursor,
                ))
            }
            _ => Err(WritesetError::InvalidMutationTag(tag)),
        }
    }

    /// Write an EntityRef to the byte buffer.
    fn write_entity_ref(bytes: &mut Vec<u8>, entity_ref: &EntityRef) {
        match entity_ref {
            EntityRef::Kv { branch_id, key } => {
                bytes.push(primitive_tags::KV);
                bytes.extend_from_slice(branch_id.as_bytes());
                Self::write_string(bytes, key);
            }
            EntityRef::Event {
                branch_id,
                sequence,
            } => {
                bytes.push(primitive_tags::EVENT);
                bytes.extend_from_slice(branch_id.as_bytes());
                bytes.extend_from_slice(&sequence.to_le_bytes());
            }
            EntityRef::Branch { branch_id } => {
                bytes.push(primitive_tags::BRANCH);
                bytes.extend_from_slice(branch_id.as_bytes());
            }
            EntityRef::Json { branch_id, doc_id } => {
                bytes.push(primitive_tags::JSON);
                bytes.extend_from_slice(branch_id.as_bytes());
                Self::write_string(bytes, doc_id);
            }
            EntityRef::Vector {
                branch_id,
                collection,
                key,
            } => {
                bytes.push(primitive_tags::VECTOR);
                bytes.extend_from_slice(branch_id.as_bytes());
                Self::write_string(bytes, collection);
                Self::write_string(bytes, key);
            }
        }
    }

    /// Read an EntityRef from bytes.
    fn read_entity_ref(bytes: &[u8]) -> Result<(EntityRef, usize), WritesetError> {
        if bytes.is_empty() {
            return Err(WritesetError::InsufficientData);
        }

        let tag = bytes[0];
        let mut cursor = 1;

        // All variants have branch_id first (16 bytes)
        if bytes.len() < cursor + 16 {
            return Err(WritesetError::InsufficientData);
        }

        let branch_id_bytes: [u8; 16] = bytes[cursor..cursor + 16].try_into().unwrap();
        let branch_id = BranchId::from_bytes(branch_id_bytes);
        cursor += 16;

        match tag {
            primitive_tags::KV => {
                let (key, consumed) = Self::read_string(&bytes[cursor..])?;
                cursor += consumed;
                Ok((EntityRef::Kv { branch_id, key }, cursor))
            }
            primitive_tags::EVENT => {
                if bytes.len() < cursor + 8 {
                    return Err(WritesetError::InsufficientData);
                }
                let sequence = u64::from_le_bytes(bytes[cursor..cursor + 8].try_into().unwrap());
                cursor += 8;
                Ok((
                    EntityRef::Event {
                        branch_id,
                        sequence,
                    },
                    cursor,
                ))
            }
            primitive_tags::BRANCH => Ok((EntityRef::Branch { branch_id }, cursor)),
            primitive_tags::JSON => {
                let (doc_id, consumed) = Self::read_string(&bytes[cursor..])?;
                cursor += consumed;
                Ok((EntityRef::Json { branch_id, doc_id }, cursor))
            }
            primitive_tags::VECTOR => {
                let (collection, consumed) = Self::read_string(&bytes[cursor..])?;
                cursor += consumed;
                let (key, consumed) = Self::read_string(&bytes[cursor..])?;
                cursor += consumed;
                Ok((
                    EntityRef::Vector {
                        branch_id,
                        collection,
                        key,
                    },
                    cursor,
                ))
            }
            _ => Err(WritesetError::InvalidEntityRefTag(tag)),
        }
    }

    /// Write a length-prefixed string.
    fn write_string(bytes: &mut Vec<u8>, s: &str) {
        let str_bytes = s.as_bytes();
        bytes.extend_from_slice(&(str_bytes.len() as u32).to_le_bytes());
        bytes.extend_from_slice(str_bytes);
    }

    /// Read a length-prefixed string.
    fn read_string(bytes: &[u8]) -> Result<(String, usize), WritesetError> {
        if bytes.len() < 4 {
            return Err(WritesetError::InsufficientData);
        }

        let len = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;

        if bytes.len() < 4 + len {
            return Err(WritesetError::InsufficientData);
        }

        let s = String::from_utf8(bytes[4..4 + len].to_vec())
            .map_err(|_| WritesetError::InvalidString)?;

        Ok((s, 4 + len))
    }
}

/// Writeset parsing errors.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum WritesetError {
    /// Not enough data to parse
    #[error("Insufficient data")]
    InsufficientData,

    /// Invalid mutation tag byte
    #[error("Invalid mutation tag: {0:#04x}")]
    InvalidMutationTag(u8),

    /// Invalid entity ref tag byte
    #[error("Invalid entity ref tag: {0:#04x}")]
    InvalidEntityRefTag(u8),

    /// Invalid entity ref structure
    #[error("Invalid entity ref")]
    InvalidEntityRef,

    /// Invalid string (not UTF-8)
    #[error("Invalid string encoding")]
    InvalidString,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_branch_id() -> BranchId {
        BranchId::from_bytes([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16])
    }

    #[test]
    fn test_writeset_empty() {
        let ws = Writeset::new();
        assert!(ws.is_empty());
        assert_eq!(ws.len(), 0);

        let bytes = ws.to_bytes();
        assert_eq!(bytes, vec![0, 0, 0, 0]); // count = 0

        let restored = Writeset::from_bytes(&bytes).unwrap();
        assert!(restored.is_empty());
    }

    #[test]
    fn test_writeset_put_kv() {
        let branch_id = test_branch_id();
        let entity_ref = EntityRef::kv(branch_id, "my-key");

        let mut ws = Writeset::new();
        ws.put(entity_ref.clone(), vec![1, 2, 3], 42);

        let bytes = ws.to_bytes();
        let restored = Writeset::from_bytes(&bytes).unwrap();

        assert_eq!(restored.len(), 1);
        match &restored.mutations[0] {
            Mutation::Put {
                entity_ref: ref_,
                value,
                version,
            } => {
                assert_eq!(ref_, &entity_ref);
                assert_eq!(value, &vec![1, 2, 3]);
                assert_eq!(*version, 42);
            }
            _ => panic!("Expected Put mutation"),
        }
    }

    #[test]
    fn test_writeset_delete() {
        let branch_id = test_branch_id();
        let entity_ref = EntityRef::kv(branch_id, "my-key");

        let mut ws = Writeset::new();
        ws.delete(entity_ref.clone());

        let bytes = ws.to_bytes();
        let restored = Writeset::from_bytes(&bytes).unwrap();

        assert_eq!(restored.len(), 1);
        match &restored.mutations[0] {
            Mutation::Delete { entity_ref: ref_ } => {
                assert_eq!(ref_, &entity_ref);
            }
            _ => panic!("Expected Delete mutation"),
        }
    }

    #[test]
    fn test_writeset_append() {
        let branch_id = test_branch_id();
        let entity_ref = EntityRef::event(branch_id, 100);

        let mut ws = Writeset::new();
        ws.append(entity_ref.clone(), vec![4, 5, 6, 7], 123);

        let bytes = ws.to_bytes();
        let restored = Writeset::from_bytes(&bytes).unwrap();

        assert_eq!(restored.len(), 1);
        match &restored.mutations[0] {
            Mutation::Append {
                entity_ref: ref_,
                value,
                version,
            } => {
                assert_eq!(ref_, &entity_ref);
                assert_eq!(value, &vec![4, 5, 6, 7]);
                assert_eq!(*version, 123);
            }
            _ => panic!("Expected Append mutation"),
        }
    }

    #[test]
    fn test_writeset_multiple_mutations() {
        let branch_id = test_branch_id();

        let mut ws = Writeset::new();
        ws.put(EntityRef::kv(branch_id, "key1"), vec![1], 1);
        ws.put(EntityRef::kv(branch_id, "key2"), vec![2], 2);
        ws.delete(EntityRef::kv(branch_id, "key3"));
        ws.append(EntityRef::event(branch_id, 1), vec![3], 3);

        assert_eq!(ws.len(), 4);

        let bytes = ws.to_bytes();
        let restored = Writeset::from_bytes(&bytes).unwrap();

        assert_eq!(restored.len(), 4);
        assert_eq!(ws, restored);
    }

    #[test]
    fn test_entity_ref_all_variants() {
        let branch_id = test_branch_id();

        let refs = vec![
            EntityRef::kv(branch_id, "key"),
            EntityRef::event(branch_id, 42),
            EntityRef::branch(branch_id),
            EntityRef::json(branch_id, "test-doc"),
            EntityRef::vector(branch_id, "collection", "vec-key"),
        ];

        for entity_ref in refs {
            let mut ws = Writeset::new();
            ws.put(entity_ref.clone(), vec![1, 2, 3], 1);

            let bytes = ws.to_bytes();
            let restored = Writeset::from_bytes(&bytes).unwrap();

            match &restored.mutations[0] {
                Mutation::Put {
                    entity_ref: ref_, ..
                } => {
                    assert_eq!(ref_, &entity_ref, "EntityRef variant should roundtrip");
                }
                _ => panic!("Expected Put mutation"),
            }
        }
    }

    #[test]
    fn test_writeset_large_value() {
        let branch_id = test_branch_id();
        let large_value = vec![0xAB; 10000];

        let mut ws = Writeset::new();
        ws.put(EntityRef::kv(branch_id, "large"), large_value.clone(), 1);

        let bytes = ws.to_bytes();
        let restored = Writeset::from_bytes(&bytes).unwrap();

        match &restored.mutations[0] {
            Mutation::Put { value, .. } => {
                assert_eq!(value, &large_value);
            }
            _ => panic!("Expected Put mutation"),
        }
    }

    #[test]
    fn test_writeset_unicode_keys() {
        let branch_id = test_branch_id();

        let mut ws = Writeset::new();
        ws.put(EntityRef::kv(branch_id, "键值对"), vec![1], 1);
        ws.put(
            EntityRef::vector(branch_id, "коллекция", "κλειδί"),
            vec![3],
            3,
        );

        let bytes = ws.to_bytes();
        let restored = Writeset::from_bytes(&bytes).unwrap();

        assert_eq!(ws, restored);
    }

    #[test]
    #[cfg(debug_assertions)]
    fn test_writeset_empty_strings() {
        let branch_id = test_branch_id();
        // In debug mode, empty keys trigger debug_assert panic in EntityRef constructors
        let result = std::panic::catch_unwind(move || EntityRef::kv(branch_id, ""));
        assert!(result.is_err(), "Empty key should panic in debug mode");
    }

    #[test]
    #[cfg(not(debug_assertions))]
    fn test_writeset_empty_strings() {
        let branch_id = test_branch_id();

        let mut ws = Writeset::new();
        ws.put(EntityRef::kv(branch_id, ""), vec![1], 1);
        ws.put(EntityRef::vector(branch_id, "", ""), vec![2], 2);

        let bytes = ws.to_bytes();
        let restored = Writeset::from_bytes(&bytes).unwrap();

        assert_eq!(ws, restored);
    }

    #[test]
    fn test_writeset_insufficient_data() {
        // Too short for count
        assert!(Writeset::from_bytes(&[1, 2]).is_err());

        // Count says 1 mutation but no data
        assert!(Writeset::from_bytes(&[1, 0, 0, 0]).is_err());
    }

    #[test]
    fn test_writeset_invalid_mutation_tag() {
        // Invalid mutation tag (0xFF)
        let bytes = vec![1, 0, 0, 0, 0xFF];
        let result = Writeset::from_bytes(&bytes);
        assert!(matches!(
            result,
            Err(WritesetError::InvalidMutationTag(0xFF))
        ));
    }

    #[test]
    fn test_mutation_entity_ref() {
        let branch_id = test_branch_id();
        let entity_ref = EntityRef::kv(branch_id, "key");

        let put = Mutation::Put {
            entity_ref: entity_ref.clone(),
            value: vec![1],
            version: 1,
        };
        assert_eq!(put.entity_ref(), &entity_ref);

        let delete = Mutation::Delete {
            entity_ref: entity_ref.clone(),
        };
        assert_eq!(delete.entity_ref(), &entity_ref);

        let append = Mutation::Append {
            entity_ref: entity_ref.clone(),
            value: vec![2],
            version: 2,
        };
        assert_eq!(append.entity_ref(), &entity_ref);
    }

    #[test]
    fn test_writeset_with_capacity() {
        let ws = Writeset::with_capacity(100);
        assert!(ws.is_empty());
        assert!(ws.mutations.capacity() >= 100);
    }
}
