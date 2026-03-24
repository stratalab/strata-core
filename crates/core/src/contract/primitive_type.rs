//! Primitive type enumeration
//!
//! This type supports Invariant 6: Everything is Introspectable.
//! Every entity can report what kind of primitive it is.
//!
//! ## The Five Primitives
//!
//! The database has exactly five primitives:
//!
//! | Primitive | Purpose | Versioning |
//! |-----------|---------|------------|
//! | Kv | Key-value store | TxnId |
//! | Event | Append-only event log | Sequence |
//! | Branch | Branch lifecycle management | TxnId |
//! | Json | JSON document store | TxnId |
//! | Vector | Vector similarity search | TxnId |

use serde::{Deserialize, Serialize};

/// The five primitive types in the database
///
/// This enum identifies which primitive a value or operation belongs to.
/// Used for type discrimination, routing, and introspection.
///
/// ## Invariant
///
/// This enum MUST have exactly 5 variants - one for each primitive.
/// Adding a new primitive requires adding a variant here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PrimitiveType {
    /// Key-Value store
    ///
    /// Simple key-value storage with CRUD operations.
    /// Versioning: TxnId
    Kv,

    /// Event log
    ///
    /// Append-only event stream with sequence numbers.
    /// Versioning: Sequence
    Event,

    /// Branch index
    ///
    /// Branch lifecycle management (create, status, metadata).
    /// Versioning: TxnId
    Branch,

    /// JSON document store
    ///
    /// JSON documents with path-based operations.
    /// Versioning: TxnId
    Json,

    /// Vector store
    ///
    /// Vector similarity search with HNSW index.
    /// Versioning: TxnId
    Vector,
}

impl PrimitiveType {
    /// All primitive types (for iteration)
    pub const ALL: [PrimitiveType; 5] = [
        PrimitiveType::Kv,
        PrimitiveType::Event,
        PrimitiveType::Branch,
        PrimitiveType::Json,
        PrimitiveType::Vector,
    ];

    /// Get all primitive types as a slice
    pub fn all() -> &'static [PrimitiveType] {
        &Self::ALL
    }

    /// Human-readable display name
    pub const fn name(&self) -> &'static str {
        match self {
            PrimitiveType::Kv => "KVStore",
            PrimitiveType::Event => "EventLog",
            PrimitiveType::Branch => "BranchIndex",
            PrimitiveType::Json => "JsonStore",
            PrimitiveType::Vector => "VectorStore",
        }
    }

    /// Short identifier (for serialization, URIs, etc.)
    pub const fn id(&self) -> &'static str {
        match self {
            PrimitiveType::Kv => "kv",
            PrimitiveType::Event => "event",
            PrimitiveType::Branch => "branch",
            PrimitiveType::Json => "json",
            PrimitiveType::Vector => "vector",
        }
    }

    /// Parse from short identifier
    pub fn from_id(id: &str) -> Option<Self> {
        match id {
            "kv" => Some(PrimitiveType::Kv),
            "event" => Some(PrimitiveType::Event),
            "branch" => Some(PrimitiveType::Branch),
            "json" => Some(PrimitiveType::Json),
            "vector" => Some(PrimitiveType::Vector),
            _ => None,
        }
    }

    /// Check if this primitive supports CRUD lifecycle
    ///
    /// Kv, Branch, Json, Vector support full CRUD.
    /// Event is append-only (CR only).
    pub const fn supports_crud(&self) -> bool {
        match self {
            PrimitiveType::Kv => true,
            PrimitiveType::Event => false, // Append-only
            PrimitiveType::Branch => true,
            PrimitiveType::Json => true,
            PrimitiveType::Vector => true,
        }
    }

    /// Check if this primitive is append-only
    pub const fn is_append_only(&self) -> bool {
        !self.supports_crud()
    }

    /// Get the WAL entry type range for this primitive
    ///
    /// Each primitive has a reserved byte range in the WAL format:
    /// - KV: 0x10-0x1F
    /// - JSON: 0x20-0x2F
    /// - Event: 0x30-0x3F
    /// - Branch: 0x40-0x4F
    /// - Vector: 0x50-0x5F
    pub const fn entry_type_range(&self) -> (u8, u8) {
        match self {
            PrimitiveType::Kv => (0x10, 0x1F),
            PrimitiveType::Json => (0x20, 0x2F),
            PrimitiveType::Event => (0x30, 0x3F),
            PrimitiveType::Branch => (0x40, 0x4F),
            PrimitiveType::Vector => (0x50, 0x5F),
        }
    }

    /// Get the primitive ID for snapshot sections
    ///
    /// These IDs are used in snapshot file format to identify sections.
    pub const fn primitive_id(&self) -> u8 {
        match self {
            PrimitiveType::Kv => 1,
            PrimitiveType::Json => 2,
            PrimitiveType::Event => 3,
            PrimitiveType::Branch => 4,
            PrimitiveType::Vector => 5,
        }
    }
}

impl std::fmt::Display for PrimitiveType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_primitive_type_all() {
        let all = PrimitiveType::all();
        assert_eq!(all.len(), 5);

        // Verify all variants are present
        assert!(all.contains(&PrimitiveType::Kv));
        assert!(all.contains(&PrimitiveType::Event));
        assert!(all.contains(&PrimitiveType::Branch));
        assert!(all.contains(&PrimitiveType::Json));
        assert!(all.contains(&PrimitiveType::Vector));
    }

    #[test]
    fn test_primitive_type_const_all() {
        assert_eq!(PrimitiveType::ALL.len(), 5);
    }

    #[test]
    fn test_primitive_type_names() {
        assert_eq!(PrimitiveType::Kv.name(), "KVStore");
        assert_eq!(PrimitiveType::Event.name(), "EventLog");
        assert_eq!(PrimitiveType::Branch.name(), "BranchIndex");
        assert_eq!(PrimitiveType::Json.name(), "JsonStore");
        assert_eq!(PrimitiveType::Vector.name(), "VectorStore");
    }

    #[test]
    fn test_primitive_type_ids() {
        assert_eq!(PrimitiveType::Kv.id(), "kv");
        assert_eq!(PrimitiveType::Event.id(), "event");
        assert_eq!(PrimitiveType::Branch.id(), "branch");
        assert_eq!(PrimitiveType::Json.id(), "json");
        assert_eq!(PrimitiveType::Vector.id(), "vector");
    }

    #[test]
    fn test_primitive_type_from_id() {
        assert_eq!(PrimitiveType::from_id("kv"), Some(PrimitiveType::Kv));
        assert_eq!(PrimitiveType::from_id("event"), Some(PrimitiveType::Event));
        assert_eq!(
            PrimitiveType::from_id("branch"),
            Some(PrimitiveType::Branch)
        );
        assert_eq!(PrimitiveType::from_id("json"), Some(PrimitiveType::Json));
        assert_eq!(
            PrimitiveType::from_id("vector"),
            Some(PrimitiveType::Vector)
        );
        assert_eq!(PrimitiveType::from_id("state"), None);
        assert_eq!(PrimitiveType::from_id("invalid"), None);
    }

    #[test]
    fn test_primitive_type_roundtrip() {
        for pt in PrimitiveType::all() {
            let id = pt.id();
            let restored = PrimitiveType::from_id(id).unwrap();
            assert_eq!(*pt, restored);
        }
    }

    #[test]
    fn test_primitive_type_display() {
        assert_eq!(format!("{}", PrimitiveType::Kv), "KVStore");
        assert_eq!(format!("{}", PrimitiveType::Json), "JsonStore");
        assert_eq!(format!("{}", PrimitiveType::Vector), "VectorStore");
    }

    #[test]
    fn test_primitive_type_supports_crud() {
        // Full CRUD
        assert!(PrimitiveType::Kv.supports_crud());
        assert!(PrimitiveType::Branch.supports_crud());
        assert!(PrimitiveType::Json.supports_crud());
        assert!(PrimitiveType::Vector.supports_crud());

        // Append-only (no delete/update)
        assert!(!PrimitiveType::Event.supports_crud());
    }

    #[test]
    fn test_primitive_type_is_append_only() {
        assert!(PrimitiveType::Event.is_append_only());

        assert!(!PrimitiveType::Kv.is_append_only());
        assert!(!PrimitiveType::Branch.is_append_only());
        assert!(!PrimitiveType::Json.is_append_only());
        assert!(!PrimitiveType::Vector.is_append_only());
    }

    #[test]
    fn test_primitive_type_copy() {
        let pt = PrimitiveType::Kv;
        let pt2 = pt; // Copy
        assert_eq!(pt, pt2);
    }

    #[test]
    fn test_primitive_type_hash() {
        use std::collections::HashSet;

        let mut set = HashSet::new();
        for pt in PrimitiveType::all() {
            set.insert(*pt);
        }
        assert_eq!(set.len(), 5, "All PrimitiveTypes should be unique");
    }

    #[test]
    fn test_primitive_type_serialization() {
        for pt in PrimitiveType::all() {
            let json = serde_json::to_string(pt).unwrap();
            let restored: PrimitiveType = serde_json::from_str(&json).unwrap();
            assert_eq!(*pt, restored);
        }
    }

    #[test]
    fn test_primitive_type_equality() {
        assert_eq!(PrimitiveType::Kv, PrimitiveType::Kv);
        assert_ne!(PrimitiveType::Kv, PrimitiveType::Event);
        assert_ne!(PrimitiveType::Event, PrimitiveType::Branch);
    }

    #[test]
    fn test_entry_type_range() {
        assert_eq!(PrimitiveType::Kv.entry_type_range(), (0x10, 0x1F));
        assert_eq!(PrimitiveType::Json.entry_type_range(), (0x20, 0x2F));
        assert_eq!(PrimitiveType::Event.entry_type_range(), (0x30, 0x3F));
        assert_eq!(PrimitiveType::Branch.entry_type_range(), (0x40, 0x4F));
        assert_eq!(PrimitiveType::Vector.entry_type_range(), (0x50, 0x5F));
    }

    #[test]
    fn test_primitive_id() {
        assert_eq!(PrimitiveType::Kv.primitive_id(), 1);
        assert_eq!(PrimitiveType::Json.primitive_id(), 2);
        assert_eq!(PrimitiveType::Event.primitive_id(), 3);
        assert_eq!(PrimitiveType::Branch.primitive_id(), 4);
        assert_eq!(PrimitiveType::Vector.primitive_id(), 5);
    }

    #[test]
    fn test_entry_type_ranges_do_not_overlap() {
        let ranges: Vec<(u8, u8)> = PrimitiveType::ALL
            .iter()
            .map(|pt| pt.entry_type_range())
            .collect();
        for i in 0..ranges.len() {
            for j in (i + 1)..ranges.len() {
                let (a_lo, a_hi) = ranges[i];
                let (b_lo, b_hi) = ranges[j];
                assert!(
                    a_hi < b_lo || b_hi < a_lo,
                    "Entry type ranges overlap: {:?} ({:02X}-{:02X}) and {:?} ({:02X}-{:02X})",
                    PrimitiveType::ALL[i],
                    a_lo,
                    a_hi,
                    PrimitiveType::ALL[j],
                    b_lo,
                    b_hi
                );
            }
        }
    }

    #[test]
    fn test_primitive_id_uniqueness() {
        let ids: std::collections::HashSet<u8> = PrimitiveType::ALL
            .iter()
            .map(|pt| pt.primitive_id())
            .collect();
        assert_eq!(ids.len(), 5, "All primitive IDs must be unique");
    }

    #[test]
    fn test_from_id_case_sensitive() {
        assert_eq!(PrimitiveType::from_id("KV"), None);
        assert_eq!(PrimitiveType::from_id("Kv"), None);
        assert_eq!(PrimitiveType::from_id("EVENT"), None);
        assert_eq!(PrimitiveType::from_id("Json"), None);
    }

    #[test]
    fn test_from_id_empty_string() {
        assert_eq!(PrimitiveType::from_id(""), None);
    }

    #[test]
    fn test_id_roundtrip_exhaustive() {
        for pt in PrimitiveType::ALL {
            let id = pt.id();
            let parsed = PrimitiveType::from_id(id);
            assert_eq!(parsed, Some(pt), "{:?}.id()={} should round-trip", pt, id);
        }
    }

    #[test]
    fn test_append_only_is_inverse_of_supports_crud() {
        for pt in PrimitiveType::ALL {
            assert_eq!(
                pt.is_append_only(),
                !pt.supports_crud(),
                "{:?}: is_append_only and supports_crud must be inverses",
                pt
            );
        }
    }
}
