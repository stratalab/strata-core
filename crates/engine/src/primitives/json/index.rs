//! Secondary index support for JsonStore
//!
//! Indexes are stored as regular KV entries in the storage layer using reserved
//! space names (`_idx_{space}_{index_name}`). This gives us branch-awareness,
//! MVCC, compaction, and durability for free.
//!
//! Index metadata is stored in `_idx_meta_{space}` as JSON documents.

use serde::{Deserialize, Serialize};
use std::sync::Arc;
use strata_core::primitives::json::{JsonPath, JsonValue};
use strata_core::types::{BranchId, Key, Namespace, TypeTag};
use strata_core::{StrataError, StrataResult};

// =============================================================================
// Index Types
// =============================================================================

/// Type of secondary index
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexType {
    /// Index on numeric (f64) fields. Supports range queries.
    Numeric,
    /// Index on categorical/enum string fields. Supports exact match.
    Tag,
    /// Index on text string fields. Supports exact match and prefix queries.
    Text,
}

impl std::fmt::Display for IndexType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IndexType::Numeric => write!(f, "numeric"),
            IndexType::Tag => write!(f, "tag"),
            IndexType::Text => write!(f, "text"),
        }
    }
}

/// Definition of a secondary index on a JSON collection
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IndexDef {
    /// Index name (unique within a space)
    pub name: String,
    /// The space this index belongs to
    pub collection_space: String,
    /// JSON path to the indexed field (e.g., "$.price", "$.status")
    pub field_path: String,
    /// Type of index
    pub index_type: IndexType,
    /// Creation timestamp (microseconds since epoch)
    pub created_at: u64,
}

impl IndexDef {
    /// Create a new index definition with current timestamp
    pub fn new(
        name: impl Into<String>,
        collection_space: impl Into<String>,
        field_path: impl Into<String>,
        index_type: IndexType,
    ) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;
        Self {
            name: name.into(),
            collection_space: collection_space.into(),
            field_path: field_path.into(),
            index_type,
            created_at: now,
        }
    }

    /// Parse the field_path into a JsonPath for value extraction
    pub fn json_path(&self) -> StrataResult<JsonPath> {
        // Strip leading "$." if present (we store paths like "$.price")
        let path_str = self
            .field_path
            .strip_prefix("$.")
            .unwrap_or(&self.field_path);
        if path_str.is_empty() {
            return Ok(JsonPath::root());
        }
        path_str
            .parse()
            .map_err(|e| StrataError::invalid_input(format!("Invalid field path: {}", e)))
    }
}

// =============================================================================
// Value Encoding — order-preserving byte representations
// =============================================================================

/// Separator byte between encoded value and doc_id in index keys.
/// Using 0xFF which is greater than any byte-stuffed content (where 0x00
/// is escaped as 0x00 0x01), ensuring encoded_value sorts before separator.
const INDEX_KEY_SEPARATOR: u8 = 0xFF;

/// Encode an f64 into 8 bytes with order-preserving properties.
///
/// Uses the standard IEEE 754 sign-bit flip technique:
/// - Positive floats: flip the sign bit (0x80..) so they sort after negatives
/// - Negative floats: flip ALL bits so more-negative sorts before less-negative
///
/// This maps the f64 total order to unsigned byte order.
pub fn encode_numeric(val: f64) -> [u8; 8] {
    let bits = val.to_bits();
    let encoded = if bits & (1u64 << 63) != 0 {
        // Negative: flip all bits
        !bits
    } else {
        // Positive (or zero): flip sign bit
        bits ^ (1u64 << 63)
    };
    encoded.to_be_bytes()
}

/// Decode 8 bytes back to f64, inverse of encode_numeric.
pub fn decode_numeric(bytes: [u8; 8]) -> f64 {
    let encoded = u64::from_be_bytes(bytes);
    let bits = if encoded & (1u64 << 63) != 0 {
        // Was positive: flip sign bit back
        encoded ^ (1u64 << 63)
    } else {
        // Was negative: flip all bits back
        !encoded
    };
    f64::from_bits(bits)
}

/// Encode a tag value: lowercase UTF-8 bytes.
pub fn encode_tag(val: &str) -> Vec<u8> {
    val.to_lowercase().into_bytes()
}

/// Encode a text value: lowercase UTF-8, truncated to 128 bytes at a char boundary.
pub fn encode_text(val: &str) -> Vec<u8> {
    let lower = val.to_lowercase();
    if lower.len() <= 128 {
        return lower.into_bytes();
    }
    // Truncate at a character boundary to avoid splitting multi-byte UTF-8.
    // Walk backward from byte 128 to find a char boundary.
    let mut end = 128;
    while end > 0 && !lower.is_char_boundary(end) {
        end -= 1;
    }
    lower.as_bytes()[..end].to_vec()
}

/// Encode a JSON value according to the index type.
///
/// Returns None if the value type doesn't match the index type
/// (e.g., a string in a numeric index).
pub fn encode_value(value: &JsonValue, index_type: IndexType) -> Option<Vec<u8>> {
    match index_type {
        IndexType::Numeric => {
            let n = value.as_f64()?;
            Some(encode_numeric(n).to_vec())
        }
        IndexType::Tag => {
            let s = value.as_str()?;
            Some(encode_tag(s))
        }
        IndexType::Text => {
            let s = value.as_str()?;
            Some(encode_text(s))
        }
    }
}

// =============================================================================
// Index Key Construction
// =============================================================================

/// Build the space name used for index entries.
/// Format: `_idx/{collection_space}/{index_name}`
///
/// Uses `/` as delimiter to avoid ambiguity when space or index names
/// contain underscores (e.g., `_idx_a_b_c` is ambiguous, `_idx/a_b/c` is not).
pub fn index_space_name(collection_space: &str, index_name: &str) -> String {
    format!("_idx/{}/{}", collection_space, index_name)
}

/// Build the space name used for index metadata.
/// Format: `_idx_meta/{collection_space}`
pub fn index_meta_space_name(collection_space: &str) -> String {
    format!("_idx_meta/{}", collection_space)
}

/// Build an index entry key.
///
/// The user_key portion is: `encoded_value ++ 0xFF ++ doc_id`
/// This ensures entries sort by value, then by doc_id within the same value.
pub fn index_entry_key(
    branch_id: &BranchId,
    collection_space: &str,
    index_name: &str,
    encoded_value: &[u8],
    doc_id: &str,
) -> Key {
    let space = index_space_name(collection_space, index_name);
    let ns = Arc::new(Namespace::for_branch_space(*branch_id, &space));

    // Build composite user key: encoded_value ++ 0xFF ++ doc_id
    let mut user_key = Vec::with_capacity(encoded_value.len() + 1 + doc_id.len());
    user_key.extend_from_slice(encoded_value);
    user_key.push(INDEX_KEY_SEPARATOR);
    user_key.extend_from_slice(doc_id.as_bytes());

    Key::new(ns, TypeTag::Json, user_key)
}

/// Build a prefix key for scanning all entries of an index.
pub fn index_scan_prefix(branch_id: &BranchId, collection_space: &str, index_name: &str) -> Key {
    let space = index_space_name(collection_space, index_name);
    let ns = Arc::new(Namespace::for_branch_space(*branch_id, &space));
    Key::new(ns, TypeTag::Json, vec![])
}

/// Build a prefix key for scanning index entries starting at a given value.
pub fn index_value_prefix(
    branch_id: &BranchId,
    collection_space: &str,
    index_name: &str,
    encoded_value: &[u8],
) -> Key {
    let space = index_space_name(collection_space, index_name);
    let ns = Arc::new(Namespace::for_branch_space(*branch_id, &space));
    Key::new(ns, TypeTag::Json, encoded_value.to_vec())
}

/// Extract the doc_id from an index entry's user_key bytes.
///
/// The user key is: `encoded_value ++ 0xFF ++ doc_id`
/// We find the last 0xFF separator and take everything after it.
pub fn extract_doc_id_from_index_key(user_key: &[u8]) -> Option<String> {
    // Find the last 0xFF separator
    let sep_pos = user_key.iter().rposition(|&b| b == INDEX_KEY_SEPARATOR)?;
    let doc_id_bytes = &user_key[sep_pos + 1..];
    std::str::from_utf8(doc_id_bytes)
        .ok()
        .map(|s| s.to_string())
}

/// Extract the field value from a JSON document at the given path.
pub fn extract_field_value(doc_value: &JsonValue, field_path: &str) -> Option<JsonValue> {
    let path_str = field_path.strip_prefix("$.").unwrap_or(field_path);
    if path_str.is_empty() {
        return Some(doc_value.clone());
    }
    let path: JsonPath = path_str.parse().ok()?;
    strata_core::primitives::json::get_at_path(doc_value, &path).cloned()
}

// =============================================================================
// Index Lookup (for query execution)
// =============================================================================

use std::collections::HashSet;
use strata_concurrency::TransactionContext;

/// Look up all doc_ids matching an exact value in an index.
pub fn lookup_eq(
    txn: &mut TransactionContext,
    branch_id: &BranchId,
    space: &str,
    index_name: &str,
    encoded_value: &[u8],
) -> StrataResult<Vec<String>> {
    // Scan with encoded_value ++ separator as prefix, so "active" doesn't
    // false-match "actively" (both start with "active" but only "active\xFF"
    // matches "active\xFF<doc_id>").
    let mut exact_prefix = Vec::with_capacity(encoded_value.len() + 1);
    exact_prefix.extend_from_slice(encoded_value);
    exact_prefix.push(INDEX_KEY_SEPARATOR);
    let prefix = index_value_prefix(branch_id, space, index_name, &exact_prefix);
    let entries = txn.scan_prefix(&prefix)?;
    Ok(entries
        .iter()
        .filter_map(|(k, _)| extract_doc_id_from_index_key(&k.user_key))
        .collect())
}

/// Look up all doc_ids in a value range [lower, upper].
///
/// Scans the full index and filters by encoded value bounds.
/// Both bounds are optional (None = unbounded on that side).
#[allow(clippy::too_many_arguments)]
pub fn lookup_range(
    txn: &mut TransactionContext,
    branch_id: &BranchId,
    space: &str,
    index_name: &str,
    lower: Option<&[u8]>,
    upper: Option<&[u8]>,
    lower_inclusive: bool,
    upper_inclusive: bool,
) -> StrataResult<Vec<String>> {
    let prefix = index_scan_prefix(branch_id, space, index_name);
    let entries = txn.scan_prefix(&prefix)?;

    let mut doc_ids = Vec::new();
    for (key, _) in &entries {
        let user_key = &key.user_key;
        // Find the separator to split value from doc_id
        let sep_pos = match user_key.iter().rposition(|&b| b == INDEX_KEY_SEPARATOR) {
            Some(pos) => pos,
            None => continue,
        };
        let value_bytes = &user_key[..sep_pos];

        // Check lower bound
        if let Some(lo) = lower {
            if lower_inclusive {
                if value_bytes < lo {
                    continue;
                }
            } else if value_bytes <= lo {
                continue;
            }
        }

        // Check upper bound
        if let Some(hi) = upper {
            if upper_inclusive {
                if value_bytes > hi {
                    continue;
                }
            } else if value_bytes >= hi {
                continue;
            }
        }

        if let Some(doc_id) = extract_doc_id_from_index_key(user_key) {
            doc_ids.push(doc_id);
        }
    }
    Ok(doc_ids)
}

/// Look up all doc_ids matching a text prefix in an index.
pub fn lookup_prefix(
    txn: &mut TransactionContext,
    branch_id: &BranchId,
    space: &str,
    index_name: &str,
    prefix_bytes: &[u8],
) -> StrataResult<Vec<String>> {
    let prefix = index_value_prefix(branch_id, space, index_name, prefix_bytes);
    let entries = txn.scan_prefix(&prefix)?;
    Ok(entries
        .iter()
        .filter_map(|(k, _)| extract_doc_id_from_index_key(&k.user_key))
        .collect())
}

/// Scan all entries of an index in order, returning doc_ids in index value order.
///
/// Used for sort-by-indexed-field: the KV layer stores index entries in
/// order-preserving byte order, so a prefix scan returns them sorted.
pub fn scan_index_ordered(
    txn: &mut TransactionContext,
    branch_id: &BranchId,
    space: &str,
    index_name: &str,
) -> StrataResult<Vec<String>> {
    let prefix = index_scan_prefix(branch_id, space, index_name);
    let entries = txn.scan_prefix(&prefix)?;
    Ok(entries
        .iter()
        .filter_map(|(k, _)| extract_doc_id_from_index_key(&k.user_key))
        .collect())
}

/// Resolve a FieldFilter against available indexes, returning matching doc_ids.
///
/// Returns an error if a predicate references a field with no corresponding index.
pub fn resolve_filter(
    txn: &mut TransactionContext,
    branch_id: &BranchId,
    space: &str,
    filter: &crate::search::FieldFilter,
    indexes: &[IndexDef],
) -> StrataResult<HashSet<String>> {
    use crate::search::FieldFilter;

    match filter {
        FieldFilter::Predicate(pred) => resolve_predicate(txn, branch_id, space, pred, indexes),
        FieldFilter::And(filters) => {
            let mut result: Option<HashSet<String>> = None;
            for f in filters {
                let set = resolve_filter(txn, branch_id, space, f, indexes)?;
                result = Some(match result {
                    Some(acc) => acc.intersection(&set).cloned().collect(),
                    None => set,
                });
            }
            Ok(result.unwrap_or_default())
        }
        FieldFilter::Or(filters) => {
            let mut result = HashSet::new();
            for f in filters {
                let set = resolve_filter(txn, branch_id, space, f, indexes)?;
                result.extend(set);
            }
            Ok(result)
        }
    }
}

/// Resolve a single predicate against indexes.
fn resolve_predicate(
    txn: &mut TransactionContext,
    branch_id: &BranchId,
    space: &str,
    pred: &crate::search::FieldPredicate,
    indexes: &[IndexDef],
) -> StrataResult<HashSet<String>> {
    use crate::search::FieldPredicate;

    match pred {
        FieldPredicate::Eq { field, value } => {
            let idx = find_index_for_field(field, indexes)?;
            let encoded = encode_value(value, idx.index_type).ok_or_else(|| {
                StrataError::invalid_input(format!(
                    "Value type mismatch for index '{}' (expected {})",
                    idx.name, idx.index_type
                ))
            })?;
            let doc_ids = lookup_eq(txn, branch_id, space, &idx.name, &encoded)?;
            Ok(doc_ids.into_iter().collect())
        }
        FieldPredicate::Range {
            field,
            lower,
            upper,
            lower_inclusive,
            upper_inclusive,
        } => {
            let idx = find_index_for_field(field, indexes)?;
            let lo = match lower {
                Some(v) => Some(encode_value(v, idx.index_type).ok_or_else(|| {
                    StrataError::invalid_input(format!(
                        "Lower bound type mismatch for index '{}'",
                        idx.name
                    ))
                })?),
                None => None,
            };
            let hi = match upper {
                Some(v) => Some(encode_value(v, idx.index_type).ok_or_else(|| {
                    StrataError::invalid_input(format!(
                        "Upper bound type mismatch for index '{}'",
                        idx.name
                    ))
                })?),
                None => None,
            };
            let doc_ids = lookup_range(
                txn,
                branch_id,
                space,
                &idx.name,
                lo.as_deref(),
                hi.as_deref(),
                *lower_inclusive,
                *upper_inclusive,
            )?;
            Ok(doc_ids.into_iter().collect())
        }
        FieldPredicate::Prefix { field, prefix } => {
            let idx = find_index_for_field(field, indexes)?;
            let prefix_bytes = encode_text(prefix);
            let doc_ids = lookup_prefix(txn, branch_id, space, &idx.name, &prefix_bytes)?;
            Ok(doc_ids.into_iter().collect())
        }
    }
}

/// Find the index definition that covers a given field path.
pub fn find_index_for_field<'a>(
    field: &str,
    indexes: &'a [IndexDef],
) -> StrataResult<&'a IndexDef> {
    indexes
        .iter()
        .find(|idx| idx.field_path == field)
        .ok_or_else(|| {
            StrataError::invalid_input(format!(
                "No index found for field '{}'. Create one with create_index()",
                field
            ))
        })
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // --- Numeric encoding ---

    #[test]
    fn numeric_roundtrip() {
        let values = vec![
            0.0,
            1.0,
            -1.0,
            42.5,
            -42.5,
            f64::MIN,
            f64::MAX,
            f64::EPSILON,
            f64::MIN_POSITIVE,
            1e-300,
            1e300,
        ];
        for v in values {
            let encoded = encode_numeric(v);
            let decoded = decode_numeric(encoded);
            assert_eq!(v, decoded, "roundtrip failed for {}", v);
        }
    }

    #[test]
    fn numeric_preserves_order() {
        let ordered = vec![
            f64::NEG_INFINITY,
            -1e300,
            -42.5,
            -1.0,
            -f64::MIN_POSITIVE,
            -0.0,
            0.0,
            f64::MIN_POSITIVE,
            1.0,
            42.5,
            1e300,
            f64::INFINITY,
        ];
        for pair in ordered.windows(2) {
            let a = encode_numeric(pair[0]);
            let b = encode_numeric(pair[1]);
            assert!(
                a <= b,
                "order violated: encode({}) = {:?} > encode({}) = {:?}",
                pair[0],
                a,
                pair[1],
                b
            );
        }
    }

    #[test]
    fn numeric_nan_roundtrip() {
        let encoded = encode_numeric(f64::NAN);
        let decoded = decode_numeric(encoded);
        assert!(decoded.is_nan());
    }

    #[test]
    fn numeric_neg_zero_roundtrip() {
        let encoded = encode_numeric(-0.0);
        let decoded = decode_numeric(encoded);
        assert_eq!(decoded.to_bits(), (-0.0f64).to_bits());
    }

    // --- Tag encoding ---

    #[test]
    fn tag_lowercases() {
        assert_eq!(encode_tag("Active"), b"active".to_vec());
        assert_eq!(encode_tag("PENDING"), b"pending".to_vec());
        assert_eq!(encode_tag("active"), b"active".to_vec());
    }

    // --- Text encoding ---

    #[test]
    fn text_truncates_at_128_bytes() {
        let long_str = "a".repeat(200);
        let encoded = encode_text(&long_str);
        assert_eq!(encoded.len(), 128);
    }

    #[test]
    fn text_lowercases() {
        assert_eq!(encode_text("Hello World"), b"hello world".to_vec());
    }

    #[test]
    fn text_truncates_at_char_boundary() {
        // 'é' is 2 bytes in UTF-8. Place it so it spans the 128-byte boundary.
        let s = format!("{}{}", "a".repeat(127), "é"); // 127 + 2 = 129 bytes
        let encoded = encode_text(&s);
        // Should truncate to 127 bytes (before the 'é'), not split the character
        assert_eq!(encoded.len(), 127);
        assert!(std::str::from_utf8(&encoded).is_ok());
    }

    // --- encode_value ---

    #[test]
    fn encode_value_numeric_from_json() {
        let val: JsonValue = serde_json::json!(42.5).into();
        let encoded = encode_value(&val, IndexType::Numeric);
        assert!(encoded.is_some());
        assert_eq!(encoded.unwrap(), encode_numeric(42.5).to_vec());
    }

    #[test]
    fn encode_value_type_mismatch_returns_none() {
        let string_val: JsonValue = serde_json::json!("hello").into();
        assert!(encode_value(&string_val, IndexType::Numeric).is_none());

        let num_val: JsonValue = serde_json::json!(42).into();
        assert!(encode_value(&num_val, IndexType::Tag).is_none());
    }

    // --- Index key construction ---

    #[test]
    fn index_space_name_format() {
        assert_eq!(
            index_space_name("default", "price_idx"),
            "_idx/default/price_idx"
        );
        assert_eq!(
            index_space_name("products", "status"),
            "_idx/products/status"
        );
    }

    #[test]
    fn index_space_name_no_ambiguity() {
        // These should produce different space names despite shared substrings
        assert_ne!(index_space_name("a_b", "c"), index_space_name("a", "b_c"));
    }

    #[test]
    fn index_meta_space_name_format() {
        assert_eq!(index_meta_space_name("default"), "_idx_meta/default");
    }

    #[test]
    fn extract_doc_id_from_key() {
        // Simulate: encoded_value(8 bytes) ++ 0xFF ++ "doc-1"
        let mut user_key = vec![0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]; // encoded 0.0
        user_key.push(INDEX_KEY_SEPARATOR);
        user_key.extend_from_slice(b"doc-1");

        let doc_id = extract_doc_id_from_index_key(&user_key);
        assert_eq!(doc_id, Some("doc-1".to_string()));
    }

    #[test]
    fn extract_doc_id_no_separator() {
        let user_key = b"noseparator";
        assert_eq!(extract_doc_id_from_index_key(user_key), None);
    }

    // --- IndexDef ---

    #[test]
    fn index_def_json_roundtrip() {
        let def = IndexDef::new("price_idx", "default", "$.price", IndexType::Numeric);
        let json = serde_json::to_string(&def).unwrap();
        let parsed: IndexDef = serde_json::from_str(&json).unwrap();
        assert_eq!(def.name, parsed.name);
        assert_eq!(def.field_path, parsed.field_path);
        assert_eq!(def.index_type, parsed.index_type);
    }

    #[test]
    fn index_def_json_path_parsing() {
        let def = IndexDef::new("price_idx", "default", "$.price", IndexType::Numeric);
        let path = def.json_path().unwrap();
        assert_eq!(path.segments().len(), 1);

        let def2 = IndexDef::new("name_idx", "default", "$.user.name", IndexType::Text);
        let path2 = def2.json_path().unwrap();
        assert_eq!(path2.segments().len(), 2);
    }

    // --- extract_field_value ---

    #[test]
    fn extract_field_value_simple() {
        let doc: JsonValue = serde_json::json!({"price": 29.99, "name": "widget"}).into();
        let val = extract_field_value(&doc, "$.price");
        assert_eq!(val, Some(serde_json::json!(29.99).into()));
    }

    #[test]
    fn extract_field_value_nested() {
        let doc: JsonValue = serde_json::json!({"user": {"name": "Alice"}}).into();
        let val = extract_field_value(&doc, "$.user.name");
        assert_eq!(val, Some(serde_json::json!("Alice").into()));
    }

    #[test]
    fn extract_field_value_missing() {
        let doc: JsonValue = serde_json::json!({"price": 29.99}).into();
        let val = extract_field_value(&doc, "$.nonexistent");
        assert!(val.is_none());
    }
}
