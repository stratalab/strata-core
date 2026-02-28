//! Core types for Strata database
//!
//! This module defines the foundational types:
//! - BranchId: Unique identifier for agent branches
//! - Namespace: Hierarchical namespace (tenant/app/agent/branch/space)
//! - TypeTag: Type discriminator for unified storage
//! - Key: Composite key (namespace + type_tag + user_key)

use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;
use uuid::Uuid;

/// Unique identifier for an agent branch
///
/// A BranchId is a wrapper around a UUID v4, providing unique identification
/// for each agent execution branch. BranchIds are used throughout the system
/// to scope data and enable branch-specific queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BranchId(Uuid);

impl BranchId {
    /// Create a new random BranchId using UUID v4
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Create a BranchId from raw bytes
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(Uuid::from_bytes(bytes))
    }

    /// Parse a BranchId from a string representation
    ///
    /// Accepts standard UUID format (with or without hyphens).
    ///
    /// # Errors
    /// Returns None if the string is not a valid UUID.
    pub fn from_string(s: &str) -> Option<Self> {
        Uuid::parse_str(s).ok().map(Self)
    }

    /// Get the raw bytes of this BranchId
    pub fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }
}

impl Default for BranchId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for BranchId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Hierarchical namespace: tenant → app → agent → branch → space
///
/// Namespaces provide multi-tenant isolation and hierarchical organization
/// of data. The hierarchy enables efficient querying and access control.
///
/// Format: "tenant/app/agent/branch_id/space"
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Namespace {
    /// Tenant identifier (top-level isolation)
    pub tenant: String,
    /// Application identifier
    pub app: String,
    /// Agent identifier
    pub agent: String,
    /// Branch identifier
    pub branch_id: BranchId,
    /// Space identifier (organizational namespace within a branch)
    #[serde(default = "default_space_name")]
    pub space: String,
}

fn default_space_name() -> String {
    "default".to_string()
}

impl Namespace {
    /// Create a new namespace
    pub fn new(
        tenant: String,
        app: String,
        agent: String,
        branch_id: BranchId,
        space: String,
    ) -> Self {
        Self {
            tenant,
            app,
            agent,
            branch_id,
            space,
        }
    }

    /// Create a namespace for a branch with default tenant/app/agent/space
    ///
    /// This is a convenience method for primitives that only need
    /// branch-level isolation. Uses "default" for tenant, app, agent, and space.
    pub fn for_branch(branch_id: BranchId) -> Self {
        Self {
            tenant: "default".to_string(),
            app: "default".to_string(),
            agent: "default".to_string(),
            branch_id,
            space: "default".to_string(),
        }
    }

    /// Create a namespace for a branch and space with default tenant/app/agent
    pub fn for_branch_space(branch_id: BranchId, space: &str) -> Self {
        Self {
            tenant: "default".to_string(),
            app: "default".to_string(),
            agent: "default".to_string(),
            branch_id,
            space: space.to_string(),
        }
    }
}

impl fmt::Display for Namespace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}/{}/{}/{}/{}",
            self.tenant, self.app, self.agent, self.branch_id, self.space
        )
    }
}

// Ord implementation for BTreeMap key ordering
// Orders by: tenant → app → agent → branch_id → space
impl Ord for Namespace {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.tenant
            .cmp(&other.tenant)
            .then(self.app.cmp(&other.app))
            .then(self.agent.cmp(&other.agent))
            .then(self.branch_id.0.cmp(&other.branch_id.0))
            .then(self.space.cmp(&other.space))
    }
}

impl PartialOrd for Namespace {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Type tag for discriminating primitive types in unified storage
///
/// The unified storage design uses a single BTreeMap with type-tagged keys
/// instead of separate stores per primitive. This TypeTag enum enables
/// type discrimination and defines the sort order in BTreeMap.
///
/// ## TypeTag Values
///
/// These values are part of the on-disk format and MUST NOT change:
/// - KV = 0x01
/// - Event = 0x02
/// - State = 0x03
/// - Branch = 0x05
/// - Vector = 0x10 (vector metadata)
/// - Json = 0x11 (JSON primitive)
/// - VectorConfig = 0x12 (vector collection config)
///
/// Note: 0x04 was formerly Trace (TraceStore was removed in 0.12.0)
///
/// Ordering: KV < Event < State < Branch < Vector < Json < VectorConfig
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
#[repr(u8)]
pub enum TypeTag {
    /// Key-Value primitive data
    KV = 0x01,
    /// Event log entries
    Event = 0x02,
    /// State cell records (renamed from StateMachine )
    State = 0x03,
    /// Reserved for backwards compatibility (TraceStore was removed)
    #[deprecated(since = "0.12.0", note = "TraceStore primitive was removed")]
    Trace = 0x04,
    /// Branch index entries
    Branch = 0x05,
    /// Space metadata entries
    Space = 0x06,
    /// Vector store entries
    Vector = 0x10,
    /// JSON document store entries
    Json = 0x11,
    /// Vector collection configuration
    VectorConfig = 0x12,
}

impl TypeTag {
    /// Convert to byte representation
    pub fn as_byte(&self) -> u8 {
        *self as u8
    }

    /// Try to create from byte
    #[allow(deprecated)]
    pub fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            0x01 => Some(TypeTag::KV),
            0x02 => Some(TypeTag::Event),
            0x03 => Some(TypeTag::State),
            0x04 => Some(TypeTag::Trace), // Deprecated but needed for backwards compatibility
            0x05 => Some(TypeTag::Branch),
            0x06 => Some(TypeTag::Space),
            0x10 => Some(TypeTag::Vector),
            0x11 => Some(TypeTag::Json),
            0x12 => Some(TypeTag::VectorConfig),
            _ => None,
        }
    }
}

/// Unified key for all storage types
///
/// A Key combines namespace, type tag, and user-defined key bytes to create
/// a composite key that enables efficient prefix scans and type discrimination
/// in the unified BTreeMap storage.
///
/// # Ordering
///
/// Keys are ordered by: namespace → type_tag → user_key
///
/// This ordering is critical for BTreeMap efficiency:
/// - All keys for a namespace are grouped together
/// - Within a namespace, keys are grouped by type
/// - Within a type, keys are ordered by user_key (enabling prefix scans)
///
/// # Examples
///
/// ```
/// use strata_core::{Key, Namespace, TypeTag, BranchId};
/// use std::sync::Arc;
///
/// let branch_id = BranchId::new();
/// let ns = Arc::new(Namespace::new("tenant".to_string(), "app".to_string(),
///                         "agent".to_string(), branch_id, "default".to_string()));
///
/// // Create a KV key
/// let key = Key::new_kv(ns.clone(), "session_state");
///
/// // Create an event key with sequence number
/// let event_key = Key::new_event(ns.clone(), 42);
///
/// // Create a prefix for scanning
/// let prefix = Key::new_kv(ns.clone(), "user:");
/// let user_key = Key::new_kv(ns.clone(), "user:alice");
/// assert!(user_key.starts_with(&prefix));
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Key {
    /// Namespace (tenant/app/agent/branch hierarchy)
    /// Wrapped in Arc for memory-efficient sharing — all keys in the same
    /// namespace (e.g., all graph edges on a branch) share a single allocation.
    pub namespace: Arc<Namespace>,
    /// Type discriminator (KV, Event, State, Trace, Branch, etc.)
    pub type_tag: TypeTag,
    /// User-defined key bytes (supports arbitrary binary keys)
    pub user_key: Vec<u8>,
}

impl Key {
    /// Create a new key with the given namespace, type tag, and user key
    pub fn new(namespace: Arc<Namespace>, type_tag: TypeTag, user_key: Vec<u8>) -> Self {
        Self {
            namespace,
            type_tag,
            user_key,
        }
    }

    /// Create a KV key
    ///
    /// Helper that automatically sets type_tag to TypeTag::KV
    pub fn new_kv(namespace: Arc<Namespace>, key: impl AsRef<[u8]>) -> Self {
        Self::new(namespace, TypeTag::KV, key.as_ref().to_vec())
    }

    /// Create an event key with sequence number
    ///
    /// Helper that automatically sets type_tag to TypeTag::Event and
    /// encodes the sequence number as big-endian bytes
    pub fn new_event(namespace: Arc<Namespace>, seq: u64) -> Self {
        Self::new(namespace, TypeTag::Event, seq.to_be_bytes().to_vec())
    }

    /// Create an event log metadata key
    ///
    /// The metadata key stores: { next_sequence: u64, head_hash: [u8; 32] }
    pub fn new_event_meta(namespace: Arc<Namespace>) -> Self {
        Self::new(namespace, TypeTag::Event, b"__meta__".to_vec())
    }

    /// Create an event type index key
    ///
    /// Stores a per-type sequence index entry for efficient `get_by_type` lookups.
    /// Key format: `__tidx__{event_type}\0{sequence_be_bytes}`
    ///
    /// The null byte separator ensures correct prefix scanning: scanning
    /// `__tidx__{event_type}\0` matches only that exact type. Big-endian
    /// sequence bytes ensure results are returned in sequence order.
    pub fn new_event_type_idx(namespace: Arc<Namespace>, event_type: &str, sequence: u64) -> Self {
        let mut user_key = Vec::with_capacity(8 + event_type.len() + 1 + 8);
        user_key.extend_from_slice(b"__tidx__");
        user_key.extend_from_slice(event_type.as_bytes());
        user_key.push(0); // null separator
        user_key.extend_from_slice(&sequence.to_be_bytes());
        Self::new(namespace, TypeTag::Event, user_key)
    }

    /// Create a prefix key for scanning all type index entries of a given event type
    ///
    /// Used by `get_by_type` to find all sequence numbers for a specific event type.
    pub fn new_event_type_idx_prefix(namespace: Arc<Namespace>, event_type: &str) -> Self {
        let mut user_key = Vec::with_capacity(8 + event_type.len() + 1);
        user_key.extend_from_slice(b"__tidx__");
        user_key.extend_from_slice(event_type.as_bytes());
        user_key.push(0); // null separator
        Self::new(namespace, TypeTag::Event, user_key)
    }

    /// Create a state cell key
    ///
    /// Helper that automatically sets type_tag to TypeTag::State
    pub fn new_state(namespace: Arc<Namespace>, key: impl AsRef<[u8]>) -> Self {
        Self::new(namespace, TypeTag::State, key.as_ref().to_vec())
    }

    /// Create a branch index key
    ///
    /// Helper that automatically sets type_tag to TypeTag::Branch and
    /// uses the branch_id as the key
    pub fn new_branch(namespace: Arc<Namespace>, branch_id: BranchId) -> Self {
        Self::new(namespace, TypeTag::Branch, branch_id.as_bytes().to_vec())
    }

    /// Create a branch index key from string branch_id
    ///
    /// Alternative helper that accepts string branch_id for index keys
    pub fn new_branch_with_id(namespace: Arc<Namespace>, branch_id: &str) -> Self {
        Self::new(namespace, TypeTag::Branch, branch_id.as_bytes().to_vec())
    }

    /// Create a branch index secondary index key
    ///
    /// Index keys enable efficient queries by status, tag, or parent.
    /// Format: `__idx_{index_type}__{index_value}__{branch_id}`
    ///
    /// Example index types:
    /// - by-status: `__idx_status__Active__branch123`
    /// - by-tag: `__idx_tag__experiment__branch123`
    /// - by-parent: `__idx_parent__parent123__branch123`
    pub fn new_branch_index(
        namespace: Arc<Namespace>,
        index_type: &str,
        index_value: &str,
        branch_id: &str,
    ) -> Self {
        let key_data = format!("__idx_{}__{}__{}", index_type, index_value, branch_id);
        Self::new(namespace, TypeTag::Branch, key_data.into_bytes())
    }

    /// Create key for JSON document storage
    ///
    /// Helper that automatically sets type_tag to TypeTag::Json and
    /// uses the document ID string as the key (consistent with KV, State).
    ///
    /// # Example
    ///
    /// ```
    /// use strata_core::{Key, Namespace, TypeTag, BranchId};
    /// use std::sync::Arc;
    ///
    /// let branch_id = BranchId::new();
    /// let namespace = Arc::new(Namespace::for_branch(branch_id));
    /// let key = Key::new_json(namespace, "my-document");
    /// assert_eq!(key.type_tag, TypeTag::Json);
    /// ```
    pub fn new_json(namespace: Arc<Namespace>, doc_id: &str) -> Self {
        Self::new(namespace, TypeTag::Json, doc_id.as_bytes().to_vec())
    }

    /// Create prefix for scanning all JSON docs in namespace
    ///
    /// This key can be used with starts_with() to match all JSON
    /// documents in a namespace.
    pub fn new_json_prefix(namespace: Arc<Namespace>) -> Self {
        Self::new(namespace, TypeTag::Json, vec![])
    }

    /// Create key for vector metadata
    ///
    /// Format: namespace + TypeTag::Vector + collection_name + "/" + vector_key
    pub fn new_vector(namespace: Arc<Namespace>, collection: &str, key: &str) -> Self {
        let user_key = format!("{}/{}", collection, key);
        Self::new(namespace, TypeTag::Vector, user_key.into_bytes())
    }

    /// Create key for collection configuration
    ///
    /// Format: namespace + TypeTag::VectorConfig + collection_name
    pub fn new_vector_config(namespace: Arc<Namespace>, collection: &str) -> Self {
        Self::new(
            namespace,
            TypeTag::VectorConfig,
            collection.as_bytes().to_vec(),
        )
    }

    /// Create prefix for scanning all vectors in a collection
    pub fn vector_collection_prefix(namespace: Arc<Namespace>, collection: &str) -> Self {
        let user_key = format!("{}/", collection);
        Self::new(namespace, TypeTag::Vector, user_key.into_bytes())
    }

    /// Create prefix for scanning all vector collections
    pub fn new_vector_config_prefix(namespace: Arc<Namespace>) -> Self {
        Self::new(namespace, TypeTag::VectorConfig, vec![])
    }

    /// Create a space metadata key.
    ///
    /// Uses the branch-level namespace (space is "default") to store
    /// space metadata. This avoids circular dependency where space
    /// metadata would be stored in the space itself.
    pub fn new_space(branch_id: BranchId, space_name: &str) -> Self {
        let namespace = Arc::new(Namespace::for_branch(branch_id));
        Self::new(namespace, TypeTag::Space, space_name.as_bytes().to_vec())
    }

    /// Prefix for scanning all space metadata in a branch.
    pub fn new_space_prefix(branch_id: BranchId) -> Self {
        let namespace = Arc::new(Namespace::for_branch(branch_id));
        Self::new(namespace, TypeTag::Space, vec![])
    }

    /// Extract user key as string (if valid UTF-8)
    ///
    /// Returns None if the user_key is not valid UTF-8
    pub fn user_key_string(&self) -> Option<String> {
        String::from_utf8(self.user_key.clone()).ok()
    }

    /// Check if this key starts with the given prefix
    ///
    /// For a key to match a prefix:
    /// - namespace must be equal
    /// - type_tag must be equal
    /// - user_key must start with prefix.user_key
    ///
    /// This enables efficient prefix scans in BTreeMap:
    /// ```
    /// # use strata_core::{Key, Namespace, BranchId};
    /// # use std::sync::Arc;
    /// # let branch_id = BranchId::new();
    /// # let ns = Arc::new(Namespace::new("t".to_string(), "a".to_string(), "ag".to_string(), branch_id, "default".to_string()));
    /// let prefix = Key::new_kv(ns.clone(), "user:");
    /// let key = Key::new_kv(ns.clone(), "user:alice");
    /// assert!(key.starts_with(&prefix));
    /// ```
    pub fn starts_with(&self, prefix: &Key) -> bool {
        self.namespace == prefix.namespace
            && self.type_tag == prefix.type_tag
            && self.user_key.starts_with(&prefix.user_key)
    }
}

/// Ordering implementation for BTreeMap
///
/// Keys are ordered by: namespace → type_tag → user_key
/// This ordering is critical for efficient prefix scans
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
///
/// Rules:
/// - Must not be empty
/// - Max length: 64 characters
/// - Must start with a lowercase letter
/// - Only lowercase letters, digits, hyphens, and underscores allowed
/// - Names starting with `_system_` are reserved
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
    use super::*;

    // ========================================
    // BranchId Tests
    // ========================================

    #[test]
    fn test_branch_id_creation_uniqueness() {
        let id1 = BranchId::new();
        let id2 = BranchId::new();
        assert_ne!(id1, id2, "BranchIds should be unique");
    }

    #[test]
    fn test_branch_id_serialization_roundtrip() {
        let id = BranchId::new();
        let bytes = id.as_bytes();
        let restored = BranchId::from_bytes(*bytes);
        assert_eq!(id, restored, "BranchId should roundtrip through bytes");
    }

    #[test]
    fn test_branch_id_display() {
        let id = BranchId::new();
        let s = format!("{}", id);
        assert!(!s.is_empty(), "Display should produce non-empty string");
        assert_eq!(
            s.len(),
            36,
            "UUID v4 should format as 36 characters with hyphens"
        );
    }

    #[test]
    fn test_branch_id_hash_consistency() {
        use std::collections::HashSet;

        let id1 = BranchId::new();
        let id2 = id1; // Copy

        let mut set = HashSet::new();
        set.insert(id1);

        assert!(
            set.contains(&id2),
            "Hash should be consistent for copied BranchId"
        );

        let id3 = BranchId::new();
        set.insert(id3);

        assert_eq!(
            set.len(),
            2,
            "Different BranchIds should have different hashes"
        );
    }

    #[test]
    fn test_branch_id_default() {
        let id1 = BranchId::default();
        let id2 = BranchId::default();
        assert_ne!(id1, id2, "Default BranchIds should be unique");
    }

    #[test]
    fn test_branch_id_nil_uuid() {
        let nil = BranchId::from_bytes([0u8; 16]);
        let display = format!("{}", nil);
        assert_eq!(display, "00000000-0000-0000-0000-000000000000");
        // Nil UUID should still be usable as a key
        let mut set = std::collections::HashSet::new();
        set.insert(nil);
        assert!(set.contains(&nil));
    }

    #[test]
    fn test_branch_id_from_string_nil() {
        let nil = BranchId::from_string("00000000-0000-0000-0000-000000000000");
        assert!(nil.is_some());
        assert_eq!(*nil.unwrap().as_bytes(), [0u8; 16]);
    }

    #[test]
    fn test_branch_id_bytes_roundtrip_preserves_all_bits() {
        // Ensure no bits are lost in from_bytes/as_bytes
        let bytes: [u8; 16] = [
            0xFF, 0x00, 0xAA, 0x55, 0x01, 0x02, 0x03, 0x04, 0x80, 0x7F, 0xFE, 0xFD, 0x10, 0x20,
            0x30, 0x40,
        ];
        let id = BranchId::from_bytes(bytes);
        assert_eq!(*id.as_bytes(), bytes);
    }

    // ========================================
    // BranchId::from_string Tests
    // ========================================

    #[test]
    fn test_branch_id_from_string_valid_with_hyphens() {
        let uuid_str = "550e8400-e29b-41d4-a716-446655440000";
        let result = BranchId::from_string(uuid_str);
        assert!(result.is_some(), "Should parse valid UUID with hyphens");

        let id = result.unwrap();
        let display = format!("{}", id);
        assert_eq!(display, uuid_str, "Display should match input");
    }

    #[test]
    fn test_branch_id_from_string_valid_without_hyphens() {
        let uuid_str = "550e8400e29b41d4a716446655440000";
        let result = BranchId::from_string(uuid_str);
        assert!(result.is_some(), "Should parse valid UUID without hyphens");

        // Verify it parsed correctly by checking display includes hyphens
        let id = result.unwrap();
        let display = format!("{}", id);
        assert_eq!(display.len(), 36, "Display should have hyphens added");
        assert!(display.contains('-'), "Display should have hyphens");
    }

    #[test]
    fn test_branch_id_from_string_valid_uppercase() {
        let uuid_str = "550E8400-E29B-41D4-A716-446655440000";
        let result = BranchId::from_string(uuid_str);
        assert!(result.is_some(), "Should parse uppercase UUID");
    }

    #[test]
    fn test_branch_id_from_string_invalid_too_short() {
        let result = BranchId::from_string("550e8400-e29b-41d4");
        assert!(result.is_none(), "Should reject UUID that's too short");
    }

    #[test]
    fn test_branch_id_from_string_invalid_too_long() {
        let result = BranchId::from_string("550e8400-e29b-41d4-a716-446655440000-extra");
        assert!(result.is_none(), "Should reject UUID that's too long");
    }

    #[test]
    fn test_branch_id_from_string_invalid_characters() {
        let result = BranchId::from_string("550e8400-e29b-41d4-a716-44665544ZZZZ");
        assert!(result.is_none(), "Should reject non-hex characters");
    }

    #[test]
    fn test_branch_id_from_string_invalid_format() {
        // Wrong hyphen positions
        let result = BranchId::from_string("550e-8400e29b-41d4a716-446655440000");
        assert!(result.is_none(), "Should reject malformed hyphen positions");
    }

    #[test]
    fn test_branch_id_from_string_empty() {
        let result = BranchId::from_string("");
        assert!(result.is_none(), "Should reject empty string");
    }

    #[test]
    fn test_branch_id_from_string_whitespace() {
        let result = BranchId::from_string("  550e8400-e29b-41d4-a716-446655440000  ");
        assert!(result.is_none(), "Should reject string with whitespace");
    }

    #[test]
    fn test_branch_id_from_string_roundtrip() {
        // Create a BranchId, get its string, parse it back
        let original = BranchId::new();
        let as_string = format!("{}", original);
        let parsed = BranchId::from_string(&as_string);

        assert!(parsed.is_some(), "Should parse back its own Display output");
        assert_eq!(parsed.unwrap(), original, "Roundtrip should preserve value");
    }

    // ========================================
    // Namespace Tests
    // ========================================

    #[test]
    fn test_namespace_construction() {
        let branch_id = BranchId::new();
        let ns = Namespace::new(
            "acme".to_string(),
            "chatbot".to_string(),
            "agent-42".to_string(),
            branch_id,
            "default".to_string(),
        );

        assert_eq!(ns.tenant, "acme");
        assert_eq!(ns.app, "chatbot");
        assert_eq!(ns.agent, "agent-42");
        assert_eq!(ns.branch_id, branch_id);
    }

    #[test]
    fn test_namespace_display_format() {
        let branch_id = BranchId::new();
        let ns = Namespace::new(
            "acme".to_string(),
            "chatbot".to_string(),
            "agent-42".to_string(),
            branch_id,
            "default".to_string(),
        );

        let display_str = format!("{}", ns);
        let expected = format!("acme/chatbot/agent-42/{}/default", branch_id);
        assert_eq!(
            display_str, expected,
            "Namespace should format as tenant/app/agent/branch_id/space"
        );
    }

    #[test]
    fn test_namespace_equality() {
        let branch_id1 = BranchId::new();
        let branch_id2 = BranchId::new();

        let ns1 = Namespace::new(
            "acme".to_string(),
            "chatbot".to_string(),
            "agent-42".to_string(),
            branch_id1,
            "default".to_string(),
        );

        let ns2 = Namespace::new(
            "acme".to_string(),
            "chatbot".to_string(),
            "agent-42".to_string(),
            branch_id1,
            "default".to_string(),
        );

        let ns3 = Namespace::new(
            "acme".to_string(),
            "chatbot".to_string(),
            "agent-42".to_string(),
            branch_id2,
            "default".to_string(),
        );

        assert_eq!(ns1, ns2, "Namespaces with same values should be equal");
        assert_ne!(
            ns1, ns3,
            "Namespaces with different branch_ids should not be equal"
        );
    }

    #[test]
    fn test_namespace_for_branch() {
        let branch_id = BranchId::new();
        let ns = Namespace::for_branch(branch_id);
        assert_eq!(ns.tenant, "default");
        assert_eq!(ns.app, "default");
        assert_eq!(ns.agent, "default");
        assert_eq!(ns.branch_id, branch_id);
    }

    #[test]
    fn test_namespace_for_branch_different_branches_differ() {
        let ns1 = Namespace::for_branch(BranchId::new());
        let ns2 = Namespace::for_branch(BranchId::new());
        assert_ne!(
            ns1, ns2,
            "Different branch_ids should produce different namespaces"
        );
    }

    #[test]
    fn test_namespace_display_empty_components() {
        let branch_id = BranchId::new();
        let ns = Namespace::new(
            "".to_string(),
            "".to_string(),
            "".to_string(),
            branch_id,
            "default".to_string(),
        );
        let display = format!("{}", ns);
        // Should produce "///branch_id/default" with empty components
        assert!(
            display.starts_with("///"),
            "Empty components should produce leading slashes"
        );
        assert!(display.ends_with("/default"));
    }

    #[test]
    fn test_namespace_ordering_is_total() {
        // Verify Ord is consistent with PartialOrd
        let branch_id = BranchId::new();
        let ns1 = Namespace::for_branch(branch_id);
        let ns2 = Namespace::for_branch(branch_id);
        assert_eq!(ns1.partial_cmp(&ns2), Some(std::cmp::Ordering::Equal));
        assert_eq!(ns1.cmp(&ns2), std::cmp::Ordering::Equal);
    }

    #[test]
    fn test_namespace_with_special_characters() {
        let branch_id = BranchId::new();
        let ns = Namespace::new(
            "tenant-1".to_string(),
            "my_app".to_string(),
            "agent.42".to_string(),
            branch_id,
            "default".to_string(),
        );

        let display = format!("{}", ns);
        assert!(display.contains("tenant-1"));
        assert!(display.contains("my_app"));
        assert!(display.contains("agent.42"));
    }

    #[test]
    fn test_namespace_with_empty_strings() {
        let branch_id = BranchId::new();
        let ns = Namespace::new(
            "".to_string(),
            "".to_string(),
            "".to_string(),
            branch_id,
            "default".to_string(),
        );

        // Should still construct, even if semantically invalid
        assert_eq!(ns.tenant, "");
        assert_eq!(ns.app, "");
        assert_eq!(ns.agent, "");
    }

    #[test]
    fn test_namespace_ordering() {
        let branch1 = BranchId::new();
        let branch2 = BranchId::new();

        let ns1 = Namespace::new(
            "tenant1".to_string(),
            "app1".to_string(),
            "agent1".to_string(),
            branch1,
            "default".to_string(),
        );
        let ns2 = Namespace::new(
            "tenant1".to_string(),
            "app1".to_string(),
            "agent1".to_string(),
            branch2,
            "default".to_string(),
        );
        let ns3 = Namespace::new(
            "tenant2".to_string(),
            "app1".to_string(),
            "agent1".to_string(),
            branch1,
            "default".to_string(),
        );
        let ns4 = Namespace::new(
            "tenant1".to_string(),
            "app2".to_string(),
            "agent1".to_string(),
            branch1,
            "default".to_string(),
        );
        let ns5 = Namespace::new(
            "tenant1".to_string(),
            "app1".to_string(),
            "agent2".to_string(),
            branch1,
            "default".to_string(),
        );

        // Same tenant/app/agent, different branch_id - order depends on UUID
        assert_ne!(ns1, ns2);

        // Different tenant should sort differently
        assert!(ns1 < ns3, "tenant1 should be less than tenant2");

        // Different app within same tenant
        assert!(ns1 < ns4, "app1 should be less than app2");

        // Different agent within same tenant/app
        assert!(ns5 > ns1, "agent2 should be greater than agent1");
    }

    #[test]
    fn test_namespace_serialization() {
        let branch_id = BranchId::new();
        let ns = Namespace::new(
            "acme".to_string(),
            "myapp".to_string(),
            "agent-42".to_string(),
            branch_id,
            "default".to_string(),
        );

        let json = serde_json::to_string(&ns).unwrap();
        let ns2: Namespace = serde_json::from_str(&json).unwrap();

        assert_eq!(ns, ns2, "Namespace should roundtrip through JSON");
    }

    #[test]
    fn test_namespace_btreemap_ordering() {
        use std::collections::BTreeMap;

        let branch1 = BranchId::new();
        let branch2 = BranchId::new();

        let ns1 = Namespace::new(
            "acme".to_string(),
            "app1".to_string(),
            "agent1".to_string(),
            branch1,
            "default".to_string(),
        );
        let ns2 = Namespace::new(
            "acme".to_string(),
            "app1".to_string(),
            "agent2".to_string(),
            branch2,
            "default".to_string(),
        );
        let ns3 = Namespace::new(
            "acme".to_string(),
            "app2".to_string(),
            "agent1".to_string(),
            branch1,
            "default".to_string(),
        );

        let mut map = BTreeMap::new();
        map.insert(ns3.clone(), "value3");
        map.insert(ns1.clone(), "value1");
        map.insert(ns2.clone(), "value2");

        // Collect keys in order
        let keys: Vec<_> = map.keys().cloned().collect();

        // Should be ordered: ns1 (app1/agent1) < ns2 (app1/agent2) < ns3 (app2/agent1)
        assert_eq!(keys[0], ns1);
        assert_eq!(keys[1], ns2);
        assert_eq!(keys[2], ns3);
    }

    // ========================================
    // TypeTag Tests
    // ========================================

    #[test]
    #[allow(deprecated)]
    fn test_typetag_ordering() {
        // TypeTag ordering must be stable for BTreeMap
        assert!(TypeTag::KV < TypeTag::Event);
        assert!(TypeTag::Event < TypeTag::State);
        assert!(TypeTag::State < TypeTag::Branch);
        assert!(TypeTag::Branch < TypeTag::Space);
        assert!(TypeTag::Space < TypeTag::Vector);
        assert!(TypeTag::Vector < TypeTag::Json);

        // Verify numeric values match spec
        assert_eq!(TypeTag::KV as u8, 0x01);
        assert_eq!(TypeTag::Event as u8, 0x02);
        assert_eq!(TypeTag::State as u8, 0x03);
        // TypeTag::Trace (0x04) is deprecated but still exists for backwards compatibility
        assert_eq!(TypeTag::Trace as u8, 0x04);
        assert_eq!(TypeTag::Branch as u8, 0x05);
        assert_eq!(TypeTag::Space as u8, 0x06);
        assert_eq!(TypeTag::Vector as u8, 0x10);
        assert_eq!(TypeTag::Json as u8, 0x11);
    }

    #[test]
    #[allow(deprecated)]
    fn test_typetag_as_byte() {
        assert_eq!(TypeTag::KV.as_byte(), 0x01);
        assert_eq!(TypeTag::Event.as_byte(), 0x02);
        assert_eq!(TypeTag::State.as_byte(), 0x03);
        // TypeTag::Trace (0x04) is deprecated but still exists for backwards compatibility
        assert_eq!(TypeTag::Trace.as_byte(), 0x04);
        assert_eq!(TypeTag::Branch.as_byte(), 0x05);
        assert_eq!(TypeTag::Space.as_byte(), 0x06);
        assert_eq!(TypeTag::Vector.as_byte(), 0x10);
        assert_eq!(TypeTag::Json.as_byte(), 0x11);
    }

    #[test]
    #[allow(deprecated)]
    fn test_typetag_from_byte() {
        assert_eq!(TypeTag::from_byte(0x01), Some(TypeTag::KV));
        assert_eq!(TypeTag::from_byte(0x02), Some(TypeTag::Event));
        assert_eq!(TypeTag::from_byte(0x03), Some(TypeTag::State));
        // 0x04 still parses to Trace for backwards compatibility
        assert_eq!(TypeTag::from_byte(0x04), Some(TypeTag::Trace));
        assert_eq!(TypeTag::from_byte(0x05), Some(TypeTag::Branch));
        assert_eq!(TypeTag::from_byte(0x06), Some(TypeTag::Space));
        assert_eq!(TypeTag::from_byte(0x10), Some(TypeTag::Vector));
        assert_eq!(TypeTag::from_byte(0x11), Some(TypeTag::Json));
        assert_eq!(TypeTag::from_byte(0x00), None);
        assert_eq!(TypeTag::from_byte(0xFF), None);
    }

    #[test]
    fn test_typetag_no_collisions() {
        // Ensure all TypeTag values are unique
        let tags = [
            TypeTag::KV,
            TypeTag::Event,
            TypeTag::State,
            TypeTag::Branch,
            TypeTag::Space,
            TypeTag::Vector,
            TypeTag::Json,
        ];
        let bytes: Vec<u8> = tags.iter().map(|t| t.as_byte()).collect();
        let unique: std::collections::HashSet<u8> = bytes.iter().cloned().collect();
        assert_eq!(bytes.len(), unique.len(), "TypeTag values must be unique");
    }

    #[test]
    fn test_typetag_serialization() {
        // Test JSON serialization roundtrip for all variants
        let tags = vec![
            TypeTag::KV,
            TypeTag::Event,
            TypeTag::State,
            TypeTag::Branch,
            TypeTag::Space,
            TypeTag::Vector,
            TypeTag::Json,
        ];

        for tag in tags {
            let json = serde_json::to_string(&tag).unwrap();
            let restored: TypeTag = serde_json::from_str(&json).unwrap();
            assert_eq!(
                tag, restored,
                "TypeTag {:?} should roundtrip through JSON",
                tag
            );
        }
    }

    #[test]
    fn test_typetag_from_byte_gap_values_return_none() {
        // Bytes between defined variants must return None (on-disk format safety)
        for byte in [
            0x00, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x13, 0x14, 0x20, 0x80,
            0xFE, 0xFF,
        ] {
            assert_eq!(
                TypeTag::from_byte(byte),
                None,
                "Byte 0x{:02X} should not map to any TypeTag",
                byte
            );
        }
    }

    #[test]
    fn test_typetag_vectorconfig_byte_roundtrip() {
        // VectorConfig (0x12) was added later - verify it's properly wired
        assert_eq!(TypeTag::VectorConfig.as_byte(), 0x12);
        assert_eq!(TypeTag::from_byte(0x12), Some(TypeTag::VectorConfig));
    }

    #[test]
    fn test_typetag_as_byte_from_byte_roundtrip_exhaustive() {
        // Every valid TypeTag must roundtrip through as_byte/from_byte
        #[allow(deprecated)]
        let all_tags = [
            TypeTag::KV,
            TypeTag::Event,
            TypeTag::State,
            TypeTag::Trace,
            TypeTag::Branch,
            TypeTag::Space,
            TypeTag::Vector,
            TypeTag::Json,
            TypeTag::VectorConfig,
        ];
        for tag in all_tags {
            let byte = tag.as_byte();
            let restored = TypeTag::from_byte(byte);
            assert_eq!(
                restored,
                Some(tag),
                "TypeTag {:?} (0x{:02X}) failed roundtrip",
                tag,
                byte
            );
        }
    }

    #[test]
    fn test_typetag_ordering_matches_byte_values() {
        // BTreeMap ordering must match the numeric byte values
        #[allow(deprecated)]
        let tags_in_order = [
            TypeTag::KV,
            TypeTag::Event,
            TypeTag::State,
            TypeTag::Trace,
            TypeTag::Branch,
            TypeTag::Space,
            TypeTag::Vector,
            TypeTag::Json,
            TypeTag::VectorConfig,
        ];
        for window in tags_in_order.windows(2) {
            assert!(
                window[0] < window[1],
                "{:?} (0x{:02X}) should sort before {:?} (0x{:02X})",
                window[0],
                window[0].as_byte(),
                window[1],
                window[1].as_byte()
            );
        }
    }

    #[test]
    fn test_typetag_hash() {
        use std::collections::HashSet;

        let mut set = HashSet::new();
        set.insert(TypeTag::KV);
        set.insert(TypeTag::Event);
        set.insert(TypeTag::KV); // Duplicate

        assert_eq!(set.len(), 2, "Set should contain 2 unique TypeTags");
        assert!(set.contains(&TypeTag::KV));
        assert!(set.contains(&TypeTag::Event));
    }

    // ========================================
    // Key Tests
    // ========================================

    #[test]
    fn test_key_construction() {
        let branch_id = BranchId::new();
        let ns = Arc::new(Namespace::new(
            "tenant".to_string(),
            "app".to_string(),
            "agent".to_string(),
            branch_id,
            "default".to_string(),
        ));

        // Test generic constructor
        let key = Key::new(ns.clone(), TypeTag::KV, b"mykey".to_vec());
        assert_eq!(key.namespace, ns);
        assert_eq!(key.type_tag, TypeTag::KV);
        assert_eq!(key.user_key, b"mykey");
    }

    #[test]
    fn test_key_helpers() {
        let branch_id = BranchId::new();
        let ns = Arc::new(Namespace::new(
            "tenant".to_string(),
            "app".to_string(),
            "agent".to_string(),
            branch_id,
            "default".to_string(),
        ));

        // Test KV helper
        let kv_key = Key::new_kv(ns.clone(), "mykey");
        assert_eq!(kv_key.type_tag, TypeTag::KV);
        assert_eq!(kv_key.user_key, b"mykey");

        // Test event helper
        let event_key = Key::new_event(ns.clone(), 42);
        assert_eq!(event_key.type_tag, TypeTag::Event);
        assert_eq!(
            u64::from_be_bytes(event_key.user_key.as_slice().try_into().unwrap()),
            42
        );

        // Test state cell helper
        let state_key = Key::new_state(ns.clone(), "state1");
        assert_eq!(state_key.type_tag, TypeTag::State);
        assert_eq!(state_key.user_key, b"state1");

        // Test branch index helper
        let branch_key = Key::new_branch(ns.clone(), branch_id);
        assert_eq!(branch_key.type_tag, TypeTag::Branch);
        assert_eq!(branch_key.user_key, branch_id.as_bytes().to_vec());
    }

    #[test]
    fn test_new_event_meta() {
        let branch_id = BranchId::new();
        let ns = Arc::new(Namespace::new(
            "tenant".to_string(),
            "app".to_string(),
            "agent".to_string(),
            branch_id,
            "default".to_string(),
        ));

        let key = Key::new_event_meta(ns);
        assert_eq!(key.type_tag, TypeTag::Event);
        assert_eq!(key.user_key, b"__meta__");
    }

    #[test]
    fn test_new_branch_index() {
        let branch_id = BranchId::new();
        let ns = Arc::new(Namespace::new(
            "tenant".to_string(),
            "app".to_string(),
            "agent".to_string(),
            branch_id,
            "default".to_string(),
        ));

        // Test by-status index
        let key = Key::new_branch_index(ns.clone(), "status", "Active", "branch-123");
        assert_eq!(key.type_tag, TypeTag::Branch);
        assert!(key
            .user_key_string()
            .unwrap()
            .contains("__idx_status__Active__branch-123"));

        // Test by-tag index
        let tag_key = Key::new_branch_index(ns.clone(), "tag", "experiment", "branch-456");
        assert!(tag_key
            .user_key_string()
            .unwrap()
            .contains("__idx_tag__experiment__branch-456"));
    }

    #[test]
    fn test_user_key_string() {
        let branch_id = BranchId::new();
        let ns = Arc::new(Namespace::new(
            "tenant".to_string(),
            "app".to_string(),
            "agent".to_string(),
            branch_id,
            "default".to_string(),
        ));

        // Valid UTF-8
        let key = Key::new_kv(ns.clone(), "hello-world");
        assert_eq!(key.user_key_string(), Some("hello-world".to_string()));

        // Invalid UTF-8 (binary data)
        let binary_key = Key::new(ns.clone(), TypeTag::KV, vec![0xFF, 0xFE, 0x00, 0x01]);
        assert_eq!(binary_key.user_key_string(), None);
    }

    #[test]
    fn test_event_keys_sort_by_sequence() {
        let branch_id = BranchId::new();
        let ns = Arc::new(Namespace::new(
            "tenant".to_string(),
            "app".to_string(),
            "agent".to_string(),
            branch_id,
            "default".to_string(),
        ));

        let key1 = Key::new_event(ns.clone(), 1);
        let key2 = Key::new_event(ns.clone(), 10);
        let key3 = Key::new_event(ns.clone(), 100);

        // Big-endian encoding ensures lexicographic sort = numeric sort
        assert!(key1 < key2);
        assert!(key2 < key3);
    }

    #[test]
    fn test_keys_with_same_inputs_are_equal() {
        let branch_id = BranchId::new();
        let ns1 = Arc::new(Namespace::new(
            "tenant".to_string(),
            "app".to_string(),
            "agent".to_string(),
            branch_id,
            "default".to_string(),
        ));
        let ns2 = Arc::new(Namespace::new(
            "tenant".to_string(),
            "app".to_string(),
            "agent".to_string(),
            branch_id,
            "default".to_string(),
        ));

        let key1 = Key::new_kv(ns1, "same-key");
        let key2 = Key::new_kv(ns2, "same-key");
        assert_eq!(key1, key2);
    }

    #[test]
    fn test_key_btree_ordering() {
        use std::collections::BTreeMap;

        let branch1 = BranchId::new();

        let ns1 = Arc::new(Namespace::new(
            "tenant1".to_string(),
            "app1".to_string(),
            "agent1".to_string(),
            branch1,
            "default".to_string(),
        ));
        let ns2 = Arc::new(Namespace::new(
            "tenant2".to_string(),
            "app1".to_string(),
            "agent1".to_string(),
            branch1,
            "default".to_string(),
        ));

        // Test ordering: namespace → type_tag → user_key
        let key1 = Key::new_kv(ns1.clone(), b"aaa");
        let key2 = Key::new_kv(ns1.clone(), b"zzz");
        let key3 = Key::new_event(ns1.clone(), 1);
        let key4 = Key::new_kv(ns2.clone(), b"aaa");

        // Same namespace, same type, different user_key
        assert!(key1 < key2, "user_key 'aaa' should be < 'zzz'");

        // Same namespace, different type (KV < Event)
        assert!(key1 < key3, "TypeTag::KV should be < TypeTag::Event");

        // Different namespace (tenant1 < tenant2)
        assert!(key1 < key4, "ns1 should be < ns2");

        // Test BTreeMap ordering
        let mut map = BTreeMap::new();
        map.insert(key4.clone(), "value4");
        map.insert(key2.clone(), "value2");
        map.insert(key1.clone(), "value1");
        map.insert(key3.clone(), "value3");

        let keys: Vec<_> = map.keys().cloned().collect();

        // Expected order: key1 (ns1/KV/aaa) < key2 (ns1/KV/zzz) < key3 (ns1/Event/1) < key4 (ns2/KV/aaa)
        assert_eq!(keys[0], key1);
        assert_eq!(keys[1], key2);
        assert_eq!(keys[2], key3);
        assert_eq!(keys[3], key4);
    }

    #[test]
    fn test_key_ordering_components() {
        let branch_id = BranchId::new();
        let ns1 = Arc::new(Namespace::new(
            "a".to_string(),
            "app".to_string(),
            "agent".to_string(),
            branch_id,
            "default".to_string(),
        ));
        let ns2 = Arc::new(Namespace::new(
            "b".to_string(),
            "app".to_string(),
            "agent".to_string(),
            branch_id,
            "default".to_string(),
        ));

        let key1 = Key::new(ns1.clone(), TypeTag::KV, b"key1".to_vec());
        let key2 = Key::new(ns1.clone(), TypeTag::Event, b"key1".to_vec());
        let key3 = Key::new(ns1.clone(), TypeTag::KV, b"key2".to_vec());
        let key4 = Key::new(ns2.clone(), TypeTag::KV, b"key1".to_vec());

        // Test namespace ordering (first component)
        assert!(
            key1 < key4,
            "Different namespace: ordering by namespace first"
        );

        // Test type_tag ordering (second component, same namespace)
        assert!(
            key1 < key2,
            "Same namespace, different type: ordering by type_tag"
        );

        // Test user_key ordering (third component, same namespace and type)
        assert!(key1 < key3, "Same namespace and type: ordering by user_key");
    }

    #[test]
    fn test_key_prefix_matching() {
        let branch_id = BranchId::new();
        let ns = Arc::new(Namespace::new(
            "tenant".to_string(),
            "app".to_string(),
            "agent".to_string(),
            branch_id,
            "default".to_string(),
        ));

        let prefix = Key::new_kv(ns.clone(), b"user:");
        let key1 = Key::new_kv(ns.clone(), b"user:alice");
        let key2 = Key::new_kv(ns.clone(), b"user:bob");
        let key3 = Key::new_kv(ns.clone(), b"config:foo");
        let key4 = Key::new_event(ns.clone(), 1);

        // Should match keys with same namespace, type, and user_key prefix
        assert!(
            key1.starts_with(&prefix),
            "user:alice should match prefix user:"
        );
        assert!(
            key2.starts_with(&prefix),
            "user:bob should match prefix user:"
        );

        // Should not match different user_key prefix
        assert!(
            !key3.starts_with(&prefix),
            "config:foo should not match prefix user:"
        );

        // Should not match different type_tag
        assert!(
            !key4.starts_with(&prefix),
            "Event type should not match KV prefix"
        );
    }

    #[test]
    fn test_key_prefix_matching_empty() {
        let branch_id = BranchId::new();
        let ns = Arc::new(Namespace::new(
            "tenant".to_string(),
            "app".to_string(),
            "agent".to_string(),
            branch_id,
            "default".to_string(),
        ));

        // Empty prefix should match all keys of same namespace and type
        let prefix = Key::new_kv(ns.clone(), b"");
        let key1 = Key::new_kv(ns.clone(), b"anything");
        let key2 = Key::new_kv(ns.clone(), b"");

        assert!(
            key1.starts_with(&prefix),
            "Any key should match empty prefix"
        );
        assert!(
            key2.starts_with(&prefix),
            "Empty key should match empty prefix"
        );
    }

    #[test]
    fn test_key_serialization() {
        let branch_id = BranchId::new();
        let ns = Arc::new(Namespace::new(
            "tenant".to_string(),
            "app".to_string(),
            "agent".to_string(),
            branch_id,
            "default".to_string(),
        ));
        let key = Key::new_kv(ns, "testkey");

        // Test JSON roundtrip
        let json = serde_json::to_string(&key).unwrap();
        let key2: Key = serde_json::from_str(&json).unwrap();
        assert_eq!(key, key2, "Key should roundtrip through JSON");
    }

    #[test]
    fn test_key_equality() {
        let branch_id = BranchId::new();
        let ns = Arc::new(Namespace::new(
            "tenant".to_string(),
            "app".to_string(),
            "agent".to_string(),
            branch_id,
            "default".to_string(),
        ));

        let key1 = Key::new_kv(ns.clone(), "mykey");
        let key2 = Key::new_kv(ns.clone(), "mykey");
        let key3 = Key::new_kv(ns.clone(), "other");

        assert_eq!(key1, key2, "Identical keys should be equal");
        assert_ne!(key1, key3, "Different user_keys should not be equal");
    }

    #[test]
    fn test_key_user_key_string_empty() {
        let ns = Arc::new(Namespace::for_branch(BranchId::new()));
        let key = Key::new_kv(ns, b"");
        assert_eq!(key.user_key_string(), Some(String::new()));
    }

    #[test]
    fn test_key_new_vector() {
        let ns = Arc::new(Namespace::for_branch(BranchId::new()));
        let key = Key::new_vector(ns.clone(), "my_collection", "vec_001");
        assert_eq!(key.type_tag, TypeTag::Vector);
        assert_eq!(key.user_key_string().unwrap(), "my_collection/vec_001");
    }

    #[test]
    fn test_key_new_vector_config() {
        let ns = Arc::new(Namespace::for_branch(BranchId::new()));
        let key = Key::new_vector_config(ns.clone(), "my_collection");
        assert_eq!(key.type_tag, TypeTag::VectorConfig);
        assert_eq!(key.user_key_string().unwrap(), "my_collection");
    }

    #[test]
    fn test_key_vector_collection_prefix_matches_vectors() {
        let ns = Arc::new(Namespace::for_branch(BranchId::new()));
        let prefix = Key::vector_collection_prefix(ns.clone(), "coll");
        let vec_key = Key::new_vector(ns.clone(), "coll", "vec_1");
        let other_coll = Key::new_vector(ns.clone(), "other", "vec_1");

        assert!(
            vec_key.starts_with(&prefix),
            "Vector in same collection should match prefix"
        );
        assert!(
            !other_coll.starts_with(&prefix),
            "Vector in different collection should not match"
        );
    }

    #[test]
    fn test_key_vector_config_prefix_matches_configs() {
        let ns = Arc::new(Namespace::for_branch(BranchId::new()));
        let prefix = Key::new_vector_config_prefix(ns.clone());
        let config_key = Key::new_vector_config(ns.clone(), "any_collection");
        let vector_key = Key::new_vector(ns.clone(), "any_collection", "v1");

        assert!(
            config_key.starts_with(&prefix),
            "Config should match config prefix"
        );
        assert!(
            !vector_key.starts_with(&prefix),
            "Vector key should not match config prefix (different TypeTag)"
        );
    }

    #[test]
    fn test_key_starts_with_different_namespace_never_matches() {
        let ns1 = Arc::new(Namespace::for_branch(BranchId::new()));
        let ns2 = Arc::new(Namespace::for_branch(BranchId::new()));
        let prefix = Key::new_kv(ns1, b"");
        let key = Key::new_kv(ns2, b"anything");
        assert!(
            !key.starts_with(&prefix),
            "Different namespace should never match"
        );
    }

    #[test]
    fn test_key_event_sequence_zero_and_max() {
        let ns = Arc::new(Namespace::for_branch(BranchId::new()));
        let key_zero = Key::new_event(ns.clone(), 0);
        let key_max = Key::new_event(ns.clone(), u64::MAX);

        // Zero should sort before max
        assert!(key_zero < key_max);

        // Verify roundtrip of sequence numbers
        let seq_zero = u64::from_be_bytes(key_zero.user_key.as_slice().try_into().unwrap());
        let seq_max = u64::from_be_bytes(key_max.user_key.as_slice().try_into().unwrap());
        assert_eq!(seq_zero, 0);
        assert_eq!(seq_max, u64::MAX);
    }

    #[test]
    fn test_key_new_branch_with_id_string_vs_branch_id() {
        let ns = Arc::new(Namespace::for_branch(BranchId::new()));
        let branch_id = BranchId::new();
        let branch_str = format!("{}", branch_id);

        let key_from_id = Key::new_branch(ns.clone(), branch_id);
        let key_from_str = Key::new_branch_with_id(ns.clone(), &branch_str);

        // These use different byte representations (UUID bytes vs UTF-8 string bytes)
        assert_ne!(
            key_from_id.user_key, key_from_str.user_key,
            "new_branch uses 16 UUID bytes, new_branch_with_id uses 36 UTF-8 bytes"
        );
    }

    #[test]
    fn test_key_hash() {
        use std::collections::HashSet;

        let branch_id = BranchId::new();
        let ns = Arc::new(Namespace::new(
            "tenant".to_string(),
            "app".to_string(),
            "agent".to_string(),
            branch_id,
            "default".to_string(),
        ));

        let key1 = Key::new_kv(ns.clone(), "key1");
        let key2 = Key::new_kv(ns.clone(), "key2");
        let key3 = Key::new_kv(ns.clone(), "key1"); // Duplicate

        let mut set = HashSet::new();
        set.insert(key1);
        set.insert(key2);
        set.insert(key3);

        assert_eq!(set.len(), 2, "Set should contain 2 unique keys");
    }

    #[test]
    fn test_key_binary_user_key() {
        let branch_id = BranchId::new();
        let ns = Arc::new(Namespace::new(
            "tenant".to_string(),
            "app".to_string(),
            "agent".to_string(),
            branch_id,
            "default".to_string(),
        ));

        // Test with binary data (not UTF-8)
        let binary_data = vec![0u8, 1, 2, 255, 254, 253];
        let key = Key::new(ns.clone(), TypeTag::KV, binary_data.clone());

        assert_eq!(
            key.user_key, binary_data,
            "Binary user_key should be preserved"
        );
    }

    // ========================================
    // Key::new_json Tests
    // ========================================

    #[test]
    fn test_key_new_json() {
        let branch_id = BranchId::new();
        let doc_id = "test-doc";
        let namespace = Arc::new(Namespace::for_branch(branch_id));
        let key = Key::new_json(namespace.clone(), doc_id);

        assert_eq!(key.type_tag, TypeTag::Json);
        assert_eq!(key.namespace, namespace);
        assert_eq!(key.user_key, doc_id.as_bytes().to_vec());
    }

    #[test]
    fn test_key_new_json_prefix() {
        let branch_id = BranchId::new();
        let namespace = Arc::new(Namespace::for_branch(branch_id));
        let prefix = Key::new_json_prefix(namespace.clone());

        assert_eq!(prefix.type_tag, TypeTag::Json);
        assert_eq!(prefix.namespace, namespace);
        assert!(prefix.user_key.is_empty());

        // Test prefix matching
        let doc_id = "test-doc";
        let key = Key::new_json(namespace.clone(), doc_id);
        assert!(
            key.starts_with(&prefix),
            "JSON key should match JSON prefix"
        );
    }

    #[test]
    fn test_key_json_different_docs_different_keys() {
        let branch_id = BranchId::new();
        let namespace = Arc::new(Namespace::for_branch(branch_id));
        let doc_id1 = "doc-1";
        let doc_id2 = "doc-2";

        let key1 = Key::new_json(namespace.clone(), doc_id1);
        let key2 = Key::new_json(namespace.clone(), doc_id2);

        assert_ne!(key1, key2, "Different docs should have different keys");
    }

    #[test]
    fn test_key_json_same_doc_same_key() {
        let branch_id = BranchId::new();
        let namespace = Arc::new(Namespace::for_branch(branch_id));
        let doc_id = "test-doc";

        let key1 = Key::new_json(namespace.clone(), doc_id);
        let key2 = Key::new_json(namespace.clone(), doc_id);

        assert_eq!(key1, key2, "Same doc should have same key");
    }

    #[test]
    fn test_key_json_ordering_with_other_types() {
        let branch_id = BranchId::new();
        let namespace = Arc::new(Namespace::for_branch(branch_id));
        let doc_id = "test-doc";

        let kv_key = Key::new_kv(namespace.clone(), "test");
        let event_key = Key::new_event(namespace.clone(), 1);
        let json_key = Key::new_json(namespace.clone(), doc_id);

        // JSON keys should sort after all other types (0x11 > 0x10 > 0x05 > ...)
        assert!(kv_key < json_key, "KV should be < JSON");
        assert!(event_key < json_key, "Event should be < JSON");
    }

    #[test]
    fn test_key_json_does_not_match_other_type_prefix() {
        let branch_id = BranchId::new();
        let namespace = Arc::new(Namespace::for_branch(branch_id));
        let doc_id = "test-doc";

        let json_key = Key::new_json(namespace.clone(), doc_id);
        let kv_prefix = Key::new_kv(namespace.clone(), "");

        assert!(
            !json_key.starts_with(&kv_prefix),
            "JSON key should not match KV prefix"
        );
    }
}
