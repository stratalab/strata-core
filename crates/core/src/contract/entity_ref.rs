//! Universal entity reference type
//!
//! This type expresses Invariant 1: Everything is Addressable.
//! Every entity in the database can be referenced by an EntityRef.
//!
//! ## The Problem EntityRef Solves
//!
//! Different primitives have different key structures:
//! - KV: namespace + user_key
//! - EventLog: namespace + sequence
//! - etc.
//!
//! EntityRef provides a **uniform way to reference any entity**.
//!
//! ## Structure
//!
//! Every EntityRef has:
//! - `branch_id`: The branch this entity belongs to (Invariant 5: Branch-scoped)
//! - Primitive-specific fields
//!
//! ## Usage
//!
//! ```
//! use strata_core::{EntityRef, BranchId, PrimitiveType};
//!
//! let branch_id = BranchId::new();
//!
//! // Reference a KV entry
//! let kv_ref = EntityRef::kv(branch_id, "my-key");
//!
//! // Reference an event
//! let event_ref = EntityRef::event(branch_id, 42);
//!
//! // Get the primitive type
//! assert_eq!(kv_ref.primitive_type(), PrimitiveType::Kv);
//! ```

use super::PrimitiveType;
use crate::types::BranchId;
use serde::{Deserialize, Serialize};

/// Universal reference to any entity in the database
///
/// EntityRef is the canonical way to identify any piece of data.
/// It combines branch_id (scope) with primitive-specific addressing.
///
/// ## Invariants
///
/// - Every EntityRef has exactly one variant (primitive type)
/// - Every EntityRef has a branch_id
/// - EntityRef variants match PrimitiveType variants 1:1
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EntityRef {
    /// Reference to a KV entry
    Kv {
        /// Branch scope
        branch_id: BranchId,
        /// Key (user-defined)
        key: String,
    },

    /// Reference to an event in the log
    Event {
        /// Branch scope
        branch_id: BranchId,
        /// Sequence number in the event log
        sequence: u64,
    },

    /// Reference to a branch's metadata
    Branch {
        /// The branch being referenced (also the scope)
        branch_id: BranchId,
    },

    /// Reference to a JSON document
    Json {
        /// Branch scope
        branch_id: BranchId,
        /// Document ID (user-provided string key)
        doc_id: String,
    },

    /// Reference to a vector entry
    Vector {
        /// Branch scope
        branch_id: BranchId,
        /// Collection name
        collection: String,
        /// Key within collection
        key: String,
    },

    /// Reference to a graph entry (node, edge, or metadata)
    Graph {
        /// Branch scope
        branch_id: BranchId,
        /// Space the graph lives in (e.g. `"default"`, `"tenant_a"`, or
        /// `"_graph_"` for the system DAG). Two graphs with the same
        /// `(branch_id, key)` in different spaces are distinct entities;
        /// dropping the `space` field would let them collide in the
        /// search index, leaking data across tenants.
        space: String,
        /// Graph name and key (e.g., "mygraph/n/node1")
        key: String,
    },
}

impl EntityRef {
    // =========================================================================
    // Constructors
    // =========================================================================

    /// Create a KV entity reference
    pub fn kv(branch_id: BranchId, key: impl Into<String>) -> Self {
        let key = key.into();
        debug_assert!(!key.is_empty(), "EntityRef::kv key must not be empty");
        EntityRef::Kv { branch_id, key }
    }

    /// Create an event entity reference
    pub fn event(branch_id: BranchId, sequence: u64) -> Self {
        EntityRef::Event {
            branch_id,
            sequence,
        }
    }

    /// Create a branch entity reference
    pub fn branch(branch_id: BranchId) -> Self {
        EntityRef::Branch { branch_id }
    }

    /// Create a JSON document entity reference
    pub fn json(branch_id: BranchId, doc_id: impl Into<String>) -> Self {
        let doc_id = doc_id.into();
        debug_assert!(
            !doc_id.is_empty(),
            "EntityRef::json doc_id must not be empty"
        );
        EntityRef::Json { branch_id, doc_id }
    }

    /// Create a graph entity reference. `space` identifies which
    /// per-branch namespace the graph lives in (e.g. `"default"`,
    /// `"tenant_a"`, or `"_graph_"` for the system DAG).
    pub fn graph(branch_id: BranchId, space: impl Into<String>, key: impl Into<String>) -> Self {
        EntityRef::Graph {
            branch_id,
            space: space.into(),
            key: key.into(),
        }
    }

    /// Create a vector entity reference
    pub fn vector(
        branch_id: BranchId,
        collection: impl Into<String>,
        key: impl Into<String>,
    ) -> Self {
        let collection = collection.into();
        let key = key.into();
        debug_assert!(
            !collection.is_empty(),
            "EntityRef::vector collection must not be empty"
        );
        // Note: key may be empty for collection-level references (e.g., error reporting)
        EntityRef::Vector {
            branch_id,
            collection,
            key,
        }
    }

    // =========================================================================
    // Accessors
    // =========================================================================

    /// Get the branch_id this entity belongs to
    ///
    /// All entities are branch-scoped (Invariant 5).
    pub fn branch_id(&self) -> BranchId {
        match self {
            EntityRef::Kv { branch_id, .. } => *branch_id,
            EntityRef::Event { branch_id, .. } => *branch_id,
            EntityRef::Branch { branch_id } => *branch_id,
            EntityRef::Json { branch_id, .. } => *branch_id,
            EntityRef::Vector { branch_id, .. } => *branch_id,
            EntityRef::Graph { branch_id, .. } => *branch_id,
        }
    }

    /// Get the primitive type of this entity
    pub fn primitive_type(&self) -> PrimitiveType {
        match self {
            EntityRef::Kv { .. } => PrimitiveType::Kv,
            EntityRef::Event { .. } => PrimitiveType::Event,
            EntityRef::Branch { .. } => PrimitiveType::Branch,
            EntityRef::Json { .. } => PrimitiveType::Json,
            EntityRef::Vector { .. } => PrimitiveType::Vector,
            EntityRef::Graph { .. } => PrimitiveType::Graph,
        }
    }

    // =========================================================================
    // Type Checks
    // =========================================================================

    /// Check if this is a KV reference
    pub fn is_kv(&self) -> bool {
        matches!(self, EntityRef::Kv { .. })
    }

    /// Check if this is an event reference
    pub fn is_event(&self) -> bool {
        matches!(self, EntityRef::Event { .. })
    }

    /// Check if this is a branch reference
    pub fn is_branch(&self) -> bool {
        matches!(self, EntityRef::Branch { .. })
    }

    /// Check if this is a JSON reference
    pub fn is_json(&self) -> bool {
        matches!(self, EntityRef::Json { .. })
    }

    /// Check if this is a vector reference
    pub fn is_vector(&self) -> bool {
        matches!(self, EntityRef::Vector { .. })
    }

    /// Check if this is a graph reference
    pub fn is_graph(&self) -> bool {
        matches!(self, EntityRef::Graph { .. })
    }

    // =========================================================================
    // Extraction
    // =========================================================================

    /// Get the KV key if this is a KV reference
    pub fn kv_key(&self) -> Option<&str> {
        match self {
            EntityRef::Kv { key, .. } => Some(key),
            _ => None,
        }
    }

    /// Get the event sequence if this is an event reference
    pub fn event_sequence(&self) -> Option<u64> {
        match self {
            EntityRef::Event { sequence, .. } => Some(*sequence),
            _ => None,
        }
    }

    /// Get the JSON doc ID if this is a JSON reference
    pub fn json_doc_id(&self) -> Option<&str> {
        match self {
            EntityRef::Json { doc_id, .. } => Some(doc_id),
            _ => None,
        }
    }

    /// Get the vector collection and key if this is a vector reference
    pub fn vector_location(&self) -> Option<(&str, &str)> {
        match self {
            EntityRef::Vector {
                collection, key, ..
            } => Some((collection, key)),
            _ => None,
        }
    }

    /// Get the graph key if this is a graph reference
    pub fn graph_key(&self) -> Option<&str> {
        match self {
            EntityRef::Graph { key, .. } => Some(key),
            _ => None,
        }
    }
}

impl std::fmt::Display for EntityRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EntityRef::Kv { branch_id, key } => {
                write!(f, "kv://{}/{}", branch_id, key)
            }
            EntityRef::Event {
                branch_id,
                sequence,
            } => {
                write!(f, "event://{}/{}", branch_id, sequence)
            }
            EntityRef::Branch { branch_id } => {
                write!(f, "branch://{}", branch_id)
            }
            EntityRef::Json { branch_id, doc_id } => {
                write!(f, "json://{}/{}", branch_id, doc_id)
            }
            EntityRef::Vector {
                branch_id,
                collection,
                key,
            } => {
                write!(f, "vector://{}/{}/{}", branch_id, collection, key)
            }
            EntityRef::Graph {
                branch_id,
                space,
                key,
            } => {
                write!(f, "graph://{}/{}/{}", branch_id, space, key)
            }
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_entity_ref_kv() {
        let branch_id = BranchId::new();
        let ref_ = EntityRef::kv(branch_id, "my-key");

        assert!(ref_.is_kv());
        assert!(!ref_.is_event());
        assert_eq!(ref_.branch_id(), branch_id);
        assert_eq!(ref_.primitive_type(), PrimitiveType::Kv);
        assert_eq!(ref_.kv_key(), Some("my-key"));
    }

    #[test]
    fn test_entity_ref_event() {
        let branch_id = BranchId::new();
        let ref_ = EntityRef::event(branch_id, 42);

        assert!(ref_.is_event());
        assert_eq!(ref_.branch_id(), branch_id);
        assert_eq!(ref_.primitive_type(), PrimitiveType::Event);
        assert_eq!(ref_.event_sequence(), Some(42));
    }

    #[test]
    fn test_entity_ref_branch() {
        let branch_id = BranchId::new();
        let ref_ = EntityRef::branch(branch_id);

        assert!(ref_.is_branch());
        assert_eq!(ref_.branch_id(), branch_id);
        assert_eq!(ref_.primitive_type(), PrimitiveType::Branch);
    }

    #[test]
    fn test_entity_ref_json() {
        let branch_id = BranchId::new();
        let doc_id = "test-doc";
        let ref_ = EntityRef::json(branch_id, doc_id);

        assert!(ref_.is_json());
        assert_eq!(ref_.branch_id(), branch_id);
        assert_eq!(ref_.primitive_type(), PrimitiveType::Json);
        assert_eq!(ref_.json_doc_id(), Some(doc_id));
    }

    #[test]
    fn test_entity_ref_vector() {
        let branch_id = BranchId::new();
        let ref_ = EntityRef::vector(branch_id, "embeddings", "doc-1");

        assert!(ref_.is_vector());
        assert_eq!(ref_.branch_id(), branch_id);
        assert_eq!(ref_.primitive_type(), PrimitiveType::Vector);
        assert_eq!(ref_.vector_location(), Some(("embeddings", "doc-1")));
    }

    #[test]
    fn test_entity_ref_graph() {
        let branch_id = BranchId::new();
        let ref_ = EntityRef::graph(branch_id, "default", "mygraph/n/node1");

        assert!(ref_.is_graph());
        assert!(!ref_.is_kv());
        assert_eq!(ref_.branch_id(), branch_id);
        assert_eq!(ref_.primitive_type(), PrimitiveType::Graph);
        assert_eq!(ref_.graph_key(), Some("mygraph/n/node1"));
    }

    #[test]
    fn test_entity_ref_display() {
        let branch_id = BranchId::new();

        let kv = EntityRef::kv(branch_id, "key");
        assert!(format!("{}", kv).starts_with("kv://"));

        let event = EntityRef::event(branch_id, 42);
        assert!(format!("{}", event).starts_with("event://"));

        let branch_ref = EntityRef::branch(branch_id);
        assert!(format!("{}", branch_ref).starts_with("branch://"));

        let json = EntityRef::json(branch_id, "test-doc");
        assert!(format!("{}", json).starts_with("json://"));

        let vector = EntityRef::vector(branch_id, "col", "key");
        assert!(format!("{}", vector).starts_with("vector://"));

        let graph = EntityRef::graph(branch_id, "default", "g/n/1");
        assert!(format!("{}", graph).starts_with("graph://"));
    }

    #[test]
    fn test_entity_ref_equality() {
        let branch_id = BranchId::new();

        let ref1 = EntityRef::kv(branch_id, "key");
        let ref2 = EntityRef::kv(branch_id, "key");
        let ref3 = EntityRef::kv(branch_id, "other");

        assert_eq!(ref1, ref2);
        assert_ne!(ref1, ref3);
    }

    #[test]
    fn test_entity_ref_hash() {
        use std::collections::HashSet;

        let branch_id = BranchId::new();

        let mut set = HashSet::new();
        set.insert(EntityRef::kv(branch_id, "key1"));
        set.insert(EntityRef::kv(branch_id, "key2"));
        set.insert(EntityRef::kv(branch_id, "key1")); // Duplicate

        assert_eq!(set.len(), 2);
    }

    #[test]
    fn test_entity_ref_serialization() {
        let branch_id = BranchId::new();
        let refs = vec![
            EntityRef::kv(branch_id, "key"),
            EntityRef::event(branch_id, 42),
            EntityRef::branch(branch_id),
            EntityRef::json(branch_id, "test-doc"),
            EntityRef::vector(branch_id, "col", "key"),
            EntityRef::graph(branch_id, "default", "g/n/1"),
        ];

        for ref_ in refs {
            let json = serde_json::to_string(&ref_).unwrap();
            let restored: EntityRef = serde_json::from_str(&json).unwrap();
            assert_eq!(ref_, restored);
        }
    }

    #[test]
    fn test_wrong_extraction_returns_none() {
        let branch_id = BranchId::new();
        let kv_ref = EntityRef::kv(branch_id, "key");

        // Wrong extractors should return None
        assert!(kv_ref.event_sequence().is_none());
        assert!(kv_ref.json_doc_id().is_none());
        assert!(kv_ref.vector_location().is_none());
        assert!(kv_ref.graph_key().is_none());
    }

    #[test]
    fn test_all_primitive_types_covered() {
        let branch_id = BranchId::new();

        // Create one of each type
        let refs = [
            EntityRef::kv(branch_id, "k"),
            EntityRef::event(branch_id, 0),
            EntityRef::branch(branch_id),
            EntityRef::json(branch_id, "j"),
            EntityRef::vector(branch_id, "c", "k"),
            EntityRef::graph(branch_id, "default", "g/n/1"),
        ];

        // Verify they map to all 6 primitive types
        let types: std::collections::HashSet<_> = refs.iter().map(|r| r.primitive_type()).collect();
        assert_eq!(types.len(), 6);
    }

    #[test]
    fn test_entity_ref_json_with_string() {
        let branch_id = BranchId::new();
        let ref_ = EntityRef::json(branch_id, String::from("owned-doc-id"));
        assert!(ref_.is_json());
        assert_eq!(ref_.json_doc_id(), Some("owned-doc-id"));
    }

    #[test]
    fn test_entity_ref_type_checks_are_exclusive() {
        let branch_id = BranchId::new();
        let refs = vec![
            EntityRef::kv(branch_id, "k"),
            EntityRef::event(branch_id, 0),
            EntityRef::branch(branch_id),
            EntityRef::json(branch_id, "j"),
            EntityRef::vector(branch_id, "c", "k"),
            EntityRef::graph(branch_id, "default", "g/n/1"),
        ];

        for r in &refs {
            let checks = [
                r.is_kv(),
                r.is_event(),
                r.is_branch(),
                r.is_json(),
                r.is_vector(),
                r.is_graph(),
            ];
            assert_eq!(
                checks.iter().filter(|&&b| b).count(),
                1,
                "Exactly one type check should be true for {:?}",
                r
            );
        }
    }

    #[test]
    fn test_entity_ref_different_branches_differ() {
        let r1 = BranchId::new();
        let r2 = BranchId::new();
        let ref1 = EntityRef::kv(r1, "key");
        let ref2 = EntityRef::kv(r2, "key");
        assert_ne!(ref1, ref2);
    }

    #[cfg(not(debug_assertions))]
    #[test]
    fn test_entity_ref_empty_string_keys_release() {
        // In release builds, empty keys are allowed (no debug_assert)
        let branch_id = BranchId::new();
        let kv = EntityRef::kv(branch_id, "");
        assert_eq!(kv.kv_key(), Some(""));

        let json = EntityRef::json(branch_id, "");
        assert_eq!(json.json_doc_id(), Some(""));
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "must not be empty")]
    fn test_entity_ref_empty_kv_key_debug_panics() {
        let branch_id = BranchId::new();
        let _kv = EntityRef::kv(branch_id, "");
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "EntityRef::json doc_id must not be empty")]
    fn test_entity_ref_empty_json_docid_debug_panics() {
        let branch_id = BranchId::new();
        let _json = EntityRef::json(branch_id, "");
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "EntityRef::vector collection must not be empty")]
    fn test_entity_ref_empty_vector_collection_debug_panics() {
        let branch_id = BranchId::new();
        let _vector = EntityRef::vector(branch_id, "", "key");
    }

    #[test]
    fn test_entity_ref_display_contains_branch_id() {
        let branch_id = BranchId::new();
        let branch_str = format!("{}", branch_id);

        let kv = EntityRef::kv(branch_id, "mykey");
        assert!(
            format!("{}", kv).contains(&branch_str),
            "Display should contain branch_id"
        );

        let event = EntityRef::event(branch_id, 42);
        let display = format!("{}", event);
        assert!(display.contains(&branch_str));
        assert!(display.contains("42"));
    }

    #[test]
    fn test_entity_ref_cross_type_never_equal() {
        let branch_id = BranchId::new();
        // Even with same branch_id and key-like values, different types are never equal
        let kv = EntityRef::kv(branch_id, "name");
        let json = EntityRef::json(branch_id, "name");
        assert_ne!(kv, json);
    }

    #[test]
    fn test_entity_ref_vector_location_with_special_chars() {
        let branch_id = BranchId::new();
        let v = EntityRef::vector(branch_id, "col/with/slash", "key with spaces");
        assert_eq!(
            v.vector_location(),
            Some(("col/with/slash", "key with spaces"))
        );
    }

    #[test]
    fn test_entity_ref_event_sequence_zero() {
        let branch_id = BranchId::new();
        let e = EntityRef::event(branch_id, 0);
        assert_eq!(e.event_sequence(), Some(0));
    }
}
