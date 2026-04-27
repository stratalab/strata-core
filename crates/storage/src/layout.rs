//! Unified storage layout surface.
//!
//! This module owns the namespace and key language used by the persistence
//! substrate.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;

pub use strata_core::BranchId;

/// Namespace: branch + space isolation.
///
/// Namespaces provide branch-level and space-level isolation of data.
/// Branch isolation is the primary mechanism; spaces provide organizational
/// grouping within a branch.
///
/// Format: `branch_id/space`
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Namespace {
    /// Branch identifier.
    pub branch_id: BranchId,
    /// Space identifier (organizational namespace within a branch).
    #[serde(default = "default_space_name")]
    pub space: String,
}

fn default_space_name() -> String {
    "default".to_string()
}

impl Namespace {
    /// Create a new namespace with the given branch and space.
    pub fn new(branch_id: BranchId, space: String) -> Self {
        debug_assert!(!space.is_empty(), "Namespace space must not be empty");
        Self { branch_id, space }
    }

    /// Create a namespace for a branch with default space.
    pub fn for_branch(branch_id: BranchId) -> Self {
        Self {
            branch_id,
            space: "default".to_string(),
        }
    }

    /// Create a namespace for a branch and space.
    pub fn for_branch_space(branch_id: BranchId, space: &str) -> Self {
        Self {
            branch_id,
            space: space.to_string(),
        }
    }
}

impl fmt::Display for Namespace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.branch_id, self.space)
    }
}

impl Ord for Namespace {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.branch_id
            .as_bytes()
            .cmp(other.branch_id.as_bytes())
            .then(self.space.cmp(&other.space))
    }
}

impl PartialOrd for Namespace {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Type tag for discriminating primitive types in unified storage.
///
/// These values are part of the on-disk format and must not change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
#[repr(u8)]
pub enum TypeTag {
    /// Key-value primitive data.
    KV = 0x01,
    /// Event log entries.
    Event = 0x02,
    /// Branch index entries.
    Branch = 0x03,
    /// Space metadata entries.
    Space = 0x04,
    /// Vector store entries.
    Vector = 0x05,
    /// JSON document store entries.
    Json = 0x06,
    /// Graph store entries.
    Graph = 0x07,
}

impl TypeTag {
    /// Convert to byte representation.
    pub fn as_byte(&self) -> u8 {
        *self as u8
    }

    /// Try to create from byte.
    pub fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            0x01 => Some(TypeTag::KV),
            0x02 => Some(TypeTag::Event),
            0x03 => Some(TypeTag::Branch),
            0x04 => Some(TypeTag::Space),
            0x05 => Some(TypeTag::Vector),
            0x06 => Some(TypeTag::Json),
            0x07 => Some(TypeTag::Graph),
            _ => None,
        }
    }
}

/// Unified key for all storage types.
///
/// Keys are ordered by: namespace -> type tag -> user key.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Key {
    /// Namespace (branch/space hierarchy).
    pub namespace: Arc<Namespace>,
    /// Type discriminator.
    pub type_tag: TypeTag,
    /// User-defined key bytes.
    pub user_key: Box<[u8]>,
}

impl Key {
    /// Create a new key with the given namespace, type tag, and user key.
    pub fn new(namespace: Arc<Namespace>, type_tag: TypeTag, user_key: Vec<u8>) -> Self {
        Self {
            namespace,
            type_tag,
            user_key: user_key.into_boxed_slice(),
        }
    }

    /// Create a KV key.
    pub fn new_kv(namespace: Arc<Namespace>, key: impl AsRef<[u8]>) -> Self {
        Self::new(namespace, TypeTag::KV, key.as_ref().to_vec())
    }

    /// Create a graph key.
    pub fn new_graph(namespace: Arc<Namespace>, key: impl AsRef<[u8]>) -> Self {
        Self::new(namespace, TypeTag::Graph, key.as_ref().to_vec())
    }

    /// Create an event key with sequence number.
    pub fn new_event(namespace: Arc<Namespace>, seq: u64) -> Self {
        Self::new(namespace, TypeTag::Event, seq.to_be_bytes().to_vec())
    }

    /// Create an event log metadata key.
    pub fn new_event_meta(namespace: Arc<Namespace>) -> Self {
        Self::new(namespace, TypeTag::Event, b"__meta__".to_vec())
    }

    /// Create an event type index key.
    pub fn new_event_type_idx(namespace: Arc<Namespace>, event_type: &str, sequence: u64) -> Self {
        let mut user_key = Vec::with_capacity(8 + event_type.len() + 1 + 8);
        user_key.extend_from_slice(b"__tidx__");
        user_key.extend_from_slice(event_type.as_bytes());
        user_key.push(0);
        user_key.extend_from_slice(&sequence.to_be_bytes());
        Self::new(namespace, TypeTag::Event, user_key)
    }

    /// Create a prefix key for scanning all type index entries of a given event type.
    pub fn new_event_type_idx_prefix(namespace: Arc<Namespace>, event_type: &str) -> Self {
        let mut user_key = Vec::with_capacity(8 + event_type.len() + 1);
        user_key.extend_from_slice(b"__tidx__");
        user_key.extend_from_slice(event_type.as_bytes());
        user_key.push(0);
        Self::new(namespace, TypeTag::Event, user_key)
    }

    /// Create a branch index key.
    pub fn new_branch(namespace: Arc<Namespace>, branch_id: BranchId) -> Self {
        Self::new(namespace, TypeTag::Branch, branch_id.as_bytes().to_vec())
    }

    /// Create a branch index key from string branch id.
    pub fn new_branch_with_id(namespace: Arc<Namespace>, branch_id: &str) -> Self {
        Self::new(namespace, TypeTag::Branch, branch_id.as_bytes().to_vec())
    }

    /// Create a branch index secondary index key.
    pub fn new_branch_index(
        namespace: Arc<Namespace>,
        index_type: &str,
        index_value: &str,
        branch_id: &str,
    ) -> Self {
        let key_data = format!("__idx_{}__{}__{}", index_type, index_value, branch_id);
        Self::new(namespace, TypeTag::Branch, key_data.into_bytes())
    }

    /// Create key for JSON document storage.
    pub fn new_json(namespace: Arc<Namespace>, doc_id: &str) -> Self {
        Self::new(namespace, TypeTag::Json, doc_id.as_bytes().to_vec())
    }

    /// Create prefix for scanning all JSON docs in a namespace.
    pub fn new_json_prefix(namespace: Arc<Namespace>) -> Self {
        Self::new(namespace, TypeTag::Json, vec![])
    }

    /// Create key for vector metadata.
    pub fn new_vector(namespace: Arc<Namespace>, collection: &str, key: &str) -> Self {
        let user_key = format!("{}/{}", collection, key);
        Self::new(namespace, TypeTag::Vector, user_key.into_bytes())
    }

    /// Create key for collection configuration.
    pub fn new_vector_config(namespace: Arc<Namespace>, collection: &str) -> Self {
        let user_key = format!("__config__/{}", collection);
        Self::new(namespace, TypeTag::Vector, user_key.into_bytes())
    }

    /// Create prefix for scanning all vectors in a collection.
    pub fn vector_collection_prefix(namespace: Arc<Namespace>, collection: &str) -> Self {
        let user_key = format!("{}/", collection);
        Self::new(namespace, TypeTag::Vector, user_key.into_bytes())
    }

    /// Create prefix for scanning all vector collections.
    pub fn new_vector_config_prefix(namespace: Arc<Namespace>) -> Self {
        Self::new(namespace, TypeTag::Vector, b"__config__/".to_vec())
    }

    /// Create a space metadata key.
    pub fn new_space(branch_id: BranchId, space_name: &str) -> Self {
        let namespace = Arc::new(Namespace::for_branch(branch_id));
        Self::new(namespace, TypeTag::Space, space_name.as_bytes().to_vec())
    }

    /// Prefix for scanning all space metadata in a branch.
    pub fn new_space_prefix(branch_id: BranchId) -> Self {
        let namespace = Arc::new(Namespace::for_branch(branch_id));
        Self::new(namespace, TypeTag::Space, vec![])
    }

    /// Extract user key as string when it is valid UTF-8.
    pub fn user_key_string(&self) -> Option<String> {
        std::str::from_utf8(&self.user_key)
            .ok()
            .map(|s| s.to_string())
    }

    /// Check whether this key starts with the given prefix.
    pub fn starts_with(&self, prefix: &Key) -> bool {
        self.namespace == prefix.namespace
            && self.type_tag == prefix.type_tag
            && self.user_key.starts_with(&prefix.user_key)
    }

    /// Create a copy of this key with a different branch id.
    pub fn with_branch_id(&self, branch_id: BranchId) -> Self {
        Key::new(
            Arc::new(Namespace::new(branch_id, self.namespace.space.clone())),
            self.type_tag,
            self.user_key.to_vec(),
        )
    }
}

impl Ord for Key {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.namespace
            .cmp(&other.namespace)
            .then(self.type_tag.cmp(&other.type_tag))
            .then(self.user_key.cmp(&other.user_key))
    }
}

impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Validate a space name according to naming rules.
pub fn validate_space_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("Space name cannot be empty".into());
    }
    if name.len() > 64 {
        return Err("Space name cannot exceed 64 characters".into());
    }
    if !name.as_bytes()[0].is_ascii_lowercase() {
        return Err("Space name must start with a lowercase letter".into());
    }
    if !name
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'_')
    {
        return Err(
            "Space name can only contain lowercase letters, digits, hyphens, and underscores"
                .into(),
        );
    }
    if name.starts_with("_system_") {
        return Err("Space names starting with '_system_' are reserved".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn type_tag_roundtrips_through_byte_encoding() {
        for tag in [
            TypeTag::KV,
            TypeTag::Event,
            TypeTag::Branch,
            TypeTag::Space,
            TypeTag::Vector,
            TypeTag::Json,
            TypeTag::Graph,
        ] {
            assert_eq!(TypeTag::from_byte(tag.as_byte()), Some(tag));
        }
        assert_eq!(TypeTag::from_byte(0xFF), None);
    }

    #[test]
    fn key_helpers_and_prefix_matching_work_through_storage_surface() {
        let ns = Arc::new(Namespace::for_branch(BranchId::from_user_name("layout")));
        let prefix = Key::new_kv(ns.clone(), "user:");
        let full = Key::new_kv(ns.clone(), "user:alice");
        let event_prefix = Key::new_event_type_idx_prefix(ns, "user_created");

        assert!(full.starts_with(&prefix));
        assert!(std::str::from_utf8(&event_prefix.user_key)
            .unwrap()
            .starts_with("__tidx__user_created"));
    }

    #[test]
    fn space_name_validation_is_available_through_storage_surface() {
        assert!(validate_space_name("default").is_ok());
        assert!(validate_space_name("tenant_a-1").is_ok());
        assert!(validate_space_name("_system_index").is_err());
    }
}
