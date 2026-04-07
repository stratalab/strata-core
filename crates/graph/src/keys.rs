//! Key construction and parsing for graph storage.
//!
//! All graph data is stored under the `_graph_` space using Graph-type keys.
//! Key format uses `/` as a separator between path segments.

use std::sync::Arc;

use dashmap::DashMap;
use once_cell::sync::Lazy;
#[cfg(test)]
use strata_core::types::TypeTag;
use strata_core::types::{BranchId, Key, Namespace};
use strata_core::{StrataError, StrataResult};

/// Global cache of `Arc<Namespace>` per `(branch, space)` pair. One heap
/// allocation per pair, ever — all subsequent calls return `Arc::clone()`
/// (atomic refcount bump, zero heap allocation). Fixes graph OOM (#1297).
///
/// Keyed by `(BranchId, String)` so different spaces on the same branch
/// get distinct namespaces. The `String` allocation per lookup is
/// regrettable but necessary for hash key equality; profile if it shows
/// up as a bottleneck.
static NS_CACHE: Lazy<DashMap<(BranchId, String), Arc<Namespace>>> = Lazy::new(DashMap::new);

/// Separator used between path segments in graph keys.
const SEP: char = '/';

/// Maximum length (in bytes) for graph names, node IDs, and edge types.
const MAX_IDENTIFIER_BYTES: usize = 255;

// =============================================================================
// Validation
// =============================================================================

/// Validate a graph name.
pub fn validate_graph_name(name: &str) -> StrataResult<()> {
    if name.is_empty() {
        return Err(StrataError::invalid_input("Graph name must not be empty"));
    }
    if name.contains(SEP) {
        return Err(StrataError::invalid_input(
            "Graph name must not contain '/'",
        ));
    }
    if name.starts_with("__") {
        return Err(StrataError::invalid_input(
            "Graph name must not start with '__' (reserved)",
        ));
    }
    if name.len() > MAX_IDENTIFIER_BYTES {
        return Err(StrataError::invalid_input(format!(
            "Graph name exceeds maximum length of {} bytes",
            MAX_IDENTIFIER_BYTES
        )));
    }
    Ok(())
}

/// Validate a node ID.
pub fn validate_node_id(id: &str) -> StrataResult<()> {
    if id.is_empty() {
        return Err(StrataError::invalid_input("Node ID must not be empty"));
    }
    if id.contains(SEP) {
        return Err(StrataError::invalid_input("Node ID must not contain '/'"));
    }
    if id.len() > MAX_IDENTIFIER_BYTES {
        return Err(StrataError::invalid_input(format!(
            "Node ID exceeds maximum length of {} bytes",
            MAX_IDENTIFIER_BYTES
        )));
    }
    Ok(())
}

/// Validate an edge type.
pub fn validate_edge_type(t: &str) -> StrataResult<()> {
    if t.is_empty() {
        return Err(StrataError::invalid_input("Edge type must not be empty"));
    }
    if t.contains(SEP) {
        return Err(StrataError::invalid_input("Edge type must not contain '/'"));
    }
    if t.len() > MAX_IDENTIFIER_BYTES {
        return Err(StrataError::invalid_input(format!(
            "Edge type exceeds maximum length of {} bytes",
            MAX_IDENTIFIER_BYTES
        )));
    }
    Ok(())
}

// =============================================================================
// Key Construction
// =============================================================================

/// The reserved space name where the system branch DAG graph lives.
///
/// Used **only** by `branch_dag.rs` to keep the system DAG isolated from
/// user spaces. It is NOT a default for user graph CRUD — user graph
/// CRUD goes through `Strata::current_space` (default `"default"`),
/// exactly like KV / JSON / Vector / Event.
///
/// `_graph_` is also unreachable through the user API because space-name
/// validation rejects names starting with `_`. Direct GraphStore
/// callers (test fixtures, branch_dag) can still pass it explicitly.
pub const GRAPH_SPACE: &str = "_graph_";

/// Build a namespace for graph operations on `(branch_id, space)`.
///
/// Returns a cached `Arc<Namespace>` — one heap allocation per
/// `(branch, space)` pair, ever. Subsequent calls return `Arc::clone()`
/// (atomic refcount bump only).
pub fn graph_namespace(branch_id: BranchId, space: &str) -> Arc<Namespace> {
    NS_CACHE
        .entry((branch_id, space.to_string()))
        .or_insert_with(|| Arc::new(Namespace::for_branch_space(branch_id, space)))
        .clone()
}

/// Remove a cached namespace entry for `(branch, space)`.
///
/// Removes a single `(branch, space)` entry. Callers that want to
/// invalidate every space for a branch must iterate the cache themselves.
pub fn invalidate_namespace_cache(branch_id: &BranchId, space: &str) {
    NS_CACHE.remove(&(*branch_id, space.to_string()));
}

/// Build a full storage Key from a `user_key` string in the graph namespace
/// for `(branch_id, space)`.
pub fn graph_key(branch_id: BranchId, space: &str, user_key: &str) -> Key {
    Key::new_graph(graph_namespace(branch_id, space), user_key)
}

// --- Packed adjacency list keys ---

/// Key for a forward (outgoing) adjacency list: `{graph}/fwd/{node_id}`
pub fn forward_adj_key(graph: &str, node_id: &str) -> String {
    format!("{}{SEP}fwd{SEP}{}", graph, node_id)
}

/// Key for a reverse (incoming) adjacency list: `{graph}/rev/{node_id}`
pub fn reverse_adj_key(graph: &str, node_id: &str) -> String {
    format!("{}{SEP}rev{SEP}{}", graph, node_id)
}

/// Prefix for all forward adjacency lists in a graph: `{graph}/fwd/`
pub fn all_forward_adj_prefix(graph: &str) -> String {
    format!("{}{SEP}fwd{SEP}", graph)
}

/// Prefix for all reverse adjacency lists in a graph: `{graph}/rev/`
pub fn all_reverse_adj_prefix(graph: &str) -> String {
    format!("{}{SEP}rev{SEP}", graph)
}

/// Parse a forward adjacency key back into node_id.
pub fn parse_forward_adj_key(graph: &str, user_key: &str) -> Option<String> {
    let prefix = format!("{}{SEP}fwd{SEP}", graph);
    user_key.strip_prefix(&prefix).map(|s| s.to_string())
}

/// Parse a reverse adjacency key back into node_id.
///
/// Mirror of `parse_forward_adj_key`. Used by the semantic graph merge,
/// which classifies storage entries by key shape and needs to identify
/// reverse adjacency lists distinctly from forward adjacency lists.
pub fn parse_reverse_adj_key(graph: &str, user_key: &str) -> Option<String> {
    let prefix = format!("{}{SEP}rev{SEP}", graph);
    user_key.strip_prefix(&prefix).map(|s| s.to_string())
}

// --- Node keys ---

/// Key for a node: `{graph}/n/{node_id}`
pub fn node_key(graph: &str, node_id: &str) -> String {
    format!("{}{SEP}n{SEP}{}", graph, node_id)
}

/// Prefix for all nodes in a graph: `{graph}/n/`
pub fn all_nodes_prefix(graph: &str) -> String {
    format!("{}{SEP}n{SEP}", graph)
}

/// Parse a node key back into node_id.
pub fn parse_node_key(graph: &str, user_key: &str) -> Option<String> {
    let prefix = format!("{}{SEP}n{SEP}", graph);
    user_key.strip_prefix(&prefix).map(|s| s.to_string())
}

// --- Meta key ---

/// Key for graph metadata: `{graph}/__meta__`
pub fn meta_key(graph: &str) -> String {
    format!("{}{SEP}__meta__", graph)
}

// --- Ref index keys ---

/// Encode a URI for use in key paths.
///
/// Escapes: `%` → `%25`, `/` → `%2F`, null byte → `%00`,
/// control characters (U+0001–U+001F) → `%XX`.
fn encode_uri(uri: &str) -> String {
    let mut out = String::with_capacity(uri.len());
    for b in uri.bytes() {
        match b {
            b'%' => out.push_str("%25"),
            b'/' => out.push_str("%2F"),
            0x00..=0x1F => {
                out.push('%');
                out.push(char::from(b"0123456789ABCDEF"[(b >> 4) as usize]));
                out.push(char::from(b"0123456789ABCDEF"[(b & 0x0F) as usize]));
            }
            _ => out.push(b as char),
        }
    }
    out
}

/// Decode a URI from a key path (reverses `encode_uri`).
fn decode_uri(encoded: &str) -> String {
    let mut out = String::with_capacity(encoded.len());
    let bytes = encoded.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(hi), Some(lo)) = (hex_val(bytes[i + 1]), hex_val(bytes[i + 2])) {
                out.push((hi << 4 | lo) as char);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

/// Parse a single hex digit (case-insensitive).
fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'A'..=b'F' => Some(b - b'A' + 10),
        b'a'..=b'f' => Some(b - b'a' + 10),
        _ => None,
    }
}

/// Key for the reverse entity-ref index: `__ref__/{encoded_uri}/{graph}/{node_id}`
pub fn ref_index_key(entity_ref_uri: &str, graph: &str, node_id: &str) -> String {
    format!(
        "__ref__{SEP}{}{SEP}{}{SEP}{}",
        encode_uri(entity_ref_uri),
        graph,
        node_id
    )
}

/// Prefix for all ref index entries for a given entity URI: `__ref__/{encoded_uri}/`
pub fn ref_index_prefix(entity_ref_uri: &str) -> String {
    format!("__ref__{SEP}{}{SEP}", encode_uri(entity_ref_uri))
}

/// Parse a ref index key back into (entity_ref_uri, graph, node_id).
pub fn parse_ref_index_key(user_key: &str) -> Option<(String, String, String)> {
    let prefix = "__ref__/";
    let rest = user_key.strip_prefix(prefix)?;
    let parts: Vec<&str> = rest.splitn(3, SEP).collect();
    if parts.len() == 3 {
        Some((
            decode_uri(parts[0]),
            parts[1].to_string(),
            parts[2].to_string(),
        ))
    } else {
        None
    }
}

// --- Ontology type keys ---

/// Key for an object type definition: `{graph}/__types__/object/{name}`
pub fn object_type_key(graph: &str, name: &str) -> String {
    format!("{}{SEP}__types__{SEP}object{SEP}{}", graph, name)
}

/// Key for a link type definition: `{graph}/__types__/link/{name}`
pub fn link_type_key(graph: &str, name: &str) -> String {
    format!("{}{SEP}__types__{SEP}link{SEP}{}", graph, name)
}

/// Prefix for all object type definitions: `{graph}/__types__/object/`
pub fn all_object_types_prefix(graph: &str) -> String {
    format!("{}{SEP}__types__{SEP}object{SEP}", graph)
}

/// Prefix for all link type definitions: `{graph}/__types__/link/`
pub fn all_link_types_prefix(graph: &str) -> String {
    format!("{}{SEP}__types__{SEP}link{SEP}", graph)
}

/// Parse an object type key back into the type name.
pub fn parse_object_type_key(graph: &str, user_key: &str) -> Option<String> {
    let prefix = format!("{}{SEP}__types__{SEP}object{SEP}", graph);
    user_key.strip_prefix(&prefix).map(|s| s.to_string())
}

/// Parse a link type key back into the type name.
pub fn parse_link_type_key(graph: &str, user_key: &str) -> Option<String> {
    let prefix = format!("{}{SEP}__types__{SEP}link{SEP}", graph);
    user_key.strip_prefix(&prefix).map(|s| s.to_string())
}

// --- Type index keys ---

/// Key for the type index: `{graph}/__by_type__/{object_type}/{node_id}`
pub fn type_index_key(graph: &str, object_type: &str, node_id: &str) -> String {
    format!(
        "{}{SEP}__by_type__{SEP}{}{SEP}{}",
        graph, object_type, node_id
    )
}

/// Prefix for all type index entries of a given type: `{graph}/__by_type__/{object_type}/`
pub fn type_index_prefix(graph: &str, object_type: &str) -> String {
    format!("{}{SEP}__by_type__{SEP}{}{SEP}", graph, object_type)
}

/// Parse a type index key back into (object_type, node_id).
pub fn parse_type_index_key(graph: &str, user_key: &str) -> Option<(String, String)> {
    let prefix = format!("{}{SEP}__by_type__{SEP}", graph);
    let rest = user_key.strip_prefix(&prefix)?;
    let parts: Vec<&str> = rest.splitn(2, SEP).collect();
    if parts.len() == 2 {
        Some((parts[0].to_string(), parts[1].to_string()))
    } else {
        None
    }
}

/// Validate a type name (same rules as edge types: non-empty, no `/`, no `__` prefix).
pub fn validate_type_name(name: &str) -> StrataResult<()> {
    if name.is_empty() {
        return Err(StrataError::invalid_input("Type name must not be empty"));
    }
    if name.contains(SEP) {
        return Err(StrataError::invalid_input("Type name must not contain '/'"));
    }
    if name.starts_with("__") {
        return Err(StrataError::invalid_input(
            "Type name must not start with '__' (reserved)",
        ));
    }
    Ok(())
}

// --- Graph catalog key ---

/// Key for the graph catalog listing all graph names: `__catalog__`
pub fn graph_catalog_key() -> &'static str {
    "__catalog__"
}

// --- Edge type count keys ---

/// Key for per-type edge count: `{graph}/__edge_count__/{edge_type}`
pub fn edge_type_count_key(graph: &str, edge_type: &str) -> String {
    format!("{}{SEP}__edge_count__{SEP}{}", graph, edge_type)
}

// --- Broad prefixes ---

/// Prefix for all keys in a graph: `{graph}/`
pub fn graph_prefix(graph: &str) -> String {
    format!("{}{SEP}", graph)
}

// =============================================================================
// Helpers
// =============================================================================

/// Build a storage Key for a graph `user_key` on `(branch_id, space)`.
pub fn storage_key(branch_id: BranchId, space: &str, user_key: &str) -> Key {
    graph_key(branch_id, space, user_key)
}

/// Check that a storage key has the expected TypeTag::Graph.
#[cfg(test)]
fn assert_graph_tag(key: &Key) {
    assert_eq!(key.type_tag, TypeTag::Graph);
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Round-trip tests ---

    #[test]
    fn forward_adj_key_roundtrip() {
        let key = forward_adj_key("g", "A");
        assert_eq!(key, "g/fwd/A");
        let node_id = parse_forward_adj_key("g", &key).unwrap();
        assert_eq!(node_id, "A");
    }

    #[test]
    fn reverse_adj_key_format() {
        let key = reverse_adj_key("g", "B");
        assert_eq!(key, "g/rev/B");
    }

    #[test]
    fn reverse_adj_key_roundtrip() {
        let key = reverse_adj_key("g", "B");
        let node_id = parse_reverse_adj_key("g", &key).unwrap();
        assert_eq!(node_id, "B");
    }

    #[test]
    fn parse_reverse_adj_key_wrong_graph_returns_none() {
        let key = reverse_adj_key("g", "A");
        assert!(parse_reverse_adj_key("other", &key).is_none());
    }

    #[test]
    fn parse_reverse_adj_key_does_not_match_forward() {
        // Reverse parser must NOT match forward keys (the only difference is fwd vs rev).
        let fwd = forward_adj_key("g", "A");
        assert!(parse_reverse_adj_key("g", &fwd).is_none());
    }

    #[test]
    fn node_key_roundtrip() {
        let key = node_key("g", "patient-4821");
        let id = parse_node_key("g", &key).unwrap();
        assert_eq!(id, "patient-4821");
    }

    #[test]
    fn ref_index_key_roundtrip() {
        let key = ref_index_key("kv://main/patient-4821", "g", "patient-4821");
        let (uri, graph, node_id) = parse_ref_index_key(&key).unwrap();
        assert_eq!(uri, "kv://main/patient-4821");
        assert_eq!(graph, "g");
        assert_eq!(node_id, "patient-4821");
    }

    #[test]
    fn meta_key_format() {
        assert_eq!(meta_key("myg"), "myg/__meta__");
    }

    // --- Prefix correctness ---

    #[test]
    fn forward_adj_prefix_is_prefix() {
        let key = forward_adj_key("g", "A");
        assert!(key.starts_with(&all_forward_adj_prefix("g")));
    }

    #[test]
    fn reverse_adj_prefix_is_prefix() {
        let key = reverse_adj_key("g", "B");
        assert!(key.starts_with(&all_reverse_adj_prefix("g")));
    }

    #[test]
    fn all_nodes_prefix_matches_node_keys() {
        let key = node_key("g", "X");
        assert!(key.starts_with(&all_nodes_prefix("g")));
    }

    #[test]
    fn graph_prefix_matches_all_keys() {
        let nk = node_key("g", "X");
        let fk = forward_adj_key("g", "A");
        let rk = reverse_adj_key("g", "B");
        let mk = meta_key("g");
        let pfx = graph_prefix("g");
        assert!(nk.starts_with(&pfx));
        assert!(fk.starts_with(&pfx));
        assert!(rk.starts_with(&pfx));
        assert!(mk.starts_with(&pfx));
    }

    // --- Validation ---

    #[test]
    fn validate_graph_name_empty() {
        assert!(validate_graph_name("").is_err());
    }

    #[test]
    fn validate_graph_name_slash() {
        assert!(validate_graph_name("has/slash").is_err());
    }

    #[test]
    fn validate_graph_name_reserved() {
        assert!(validate_graph_name("__reserved").is_err());
    }

    #[test]
    fn validate_graph_name_valid() {
        assert!(validate_graph_name("valid-name").is_ok());
    }

    #[test]
    fn validate_node_id_empty() {
        assert!(validate_node_id("").is_err());
    }

    #[test]
    fn validate_node_id_slash() {
        assert!(validate_node_id("has/slash").is_err());
    }

    #[test]
    fn validate_edge_type_empty() {
        assert!(validate_edge_type("").is_err());
    }

    #[test]
    fn validate_edge_type_slash() {
        assert!(validate_edge_type("has/slash").is_err());
    }

    #[test]
    fn validate_edge_type_valid() {
        assert!(validate_edge_type("VALID_TYPE").is_ok());
    }

    // --- Edge cases ---

    #[test]
    fn single_character_names_work() {
        assert!(validate_graph_name("g").is_ok());
        assert!(validate_node_id("n").is_ok());
        assert!(validate_edge_type("E").is_ok());
    }

    #[test]
    fn unicode_node_ids_work() {
        assert!(validate_node_id("患者").is_ok());
        let key = node_key("g", "患者");
        let id = parse_node_key("g", &key).unwrap();
        assert_eq!(id, "患者");
    }

    #[test]
    fn storage_key_has_graph_type_tag() {
        let branch = BranchId::from_bytes([0u8; 16]);
        let key = storage_key(branch, GRAPH_SPACE, "test/key");
        assert_graph_tag(&key);
    }

    // --- URI encoding edge cases ---

    #[test]
    fn uri_encode_decode_with_percent_2f() {
        // URI containing literal "%2F" should round-trip correctly
        let uri = "kv://main/key%2Fwith%2Fencoded";
        let encoded = encode_uri(uri);
        let decoded = decode_uri(&encoded);
        assert_eq!(decoded, uri);
    }

    #[test]
    fn uri_encode_decode_with_percent_25() {
        // URI containing literal "%25" should round-trip correctly
        let uri = "kv://main/100%25done";
        let encoded = encode_uri(uri);
        let decoded = decode_uri(&encoded);
        assert_eq!(decoded, uri);
    }

    #[test]
    fn uri_encode_decode_with_slashes_and_percents() {
        // URI with both slashes and percent signs
        let uri = "kv://main/path/to/100%25/file";
        let encoded = encode_uri(uri);
        let decoded = decode_uri(&encoded);
        assert_eq!(decoded, uri);
    }

    #[test]
    fn uri_encode_decode_percent_2f_and_real_slash() {
        // URI containing both a literal "%2F" and real slashes
        let uri = "json://main/doc%2Fwith%2Fslashes/and/real/slashes";
        let encoded = encode_uri(uri);
        let decoded = decode_uri(&encoded);
        assert_eq!(decoded, uri);
    }

    #[test]
    fn ref_index_key_roundtrip_with_complex_uri() {
        // Full ref index round-trip with a URI containing slashes
        let uri = "kv://main/path/to/thing";
        let key = ref_index_key(uri, "mygraph", "node1");
        let (decoded_uri, graph, node_id) = parse_ref_index_key(&key).unwrap();
        assert_eq!(decoded_uri, uri);
        assert_eq!(graph, "mygraph");
        assert_eq!(node_id, "node1");
    }

    // --- Negative parsing tests ---

    #[test]
    fn parse_forward_adj_key_wrong_graph_returns_none() {
        let key = forward_adj_key("g", "A");
        assert!(parse_forward_adj_key("other", &key).is_none());
    }

    #[test]
    fn parse_forward_adj_key_malformed_returns_none() {
        assert!(parse_forward_adj_key("g", "g/fwd").is_none());
        assert!(parse_forward_adj_key("g", "garbage").is_none());
    }

    #[test]
    fn parse_node_key_wrong_graph_returns_none() {
        let key = node_key("g", "n1");
        assert!(parse_node_key("other", &key).is_none());
    }

    #[test]
    fn parse_ref_index_key_malformed_returns_none() {
        assert!(parse_ref_index_key("not_a_ref_key").is_none());
        assert!(parse_ref_index_key("__ref__/only_one_part").is_none());
    }

    // --- Cross-type parsing tests ---

    #[test]
    fn forward_adj_key_not_parseable_as_node() {
        let key = forward_adj_key("g", "A");
        // fwd key should not parse as a node key (prefix is fwd, not n)
        assert!(parse_node_key("g", &key).is_none());
    }

    #[test]
    fn node_key_not_parseable_as_forward_adj() {
        let key = node_key("g", "mynode");
        assert!(parse_forward_adj_key("g", &key).is_none());
    }

    // --- Ontology key tests ---

    #[test]
    fn object_type_key_roundtrip() {
        let key = object_type_key("g", "Patient");
        let name = parse_object_type_key("g", &key).unwrap();
        assert_eq!(name, "Patient");
    }

    #[test]
    fn link_type_key_roundtrip() {
        let key = link_type_key("g", "HAS_RESULT");
        let name = parse_link_type_key("g", &key).unwrap();
        assert_eq!(name, "HAS_RESULT");
    }

    #[test]
    fn object_type_key_prefix_correctness() {
        let key = object_type_key("g", "Patient");
        assert!(key.starts_with(&all_object_types_prefix("g")));
    }

    #[test]
    fn link_type_key_prefix_correctness() {
        let key = link_type_key("g", "HAS_RESULT");
        assert!(key.starts_with(&all_link_types_prefix("g")));
    }

    #[test]
    fn type_index_key_roundtrip() {
        let key = type_index_key("g", "Patient", "p1");
        let (obj_type, node_id) = parse_type_index_key("g", &key).unwrap();
        assert_eq!(obj_type, "Patient");
        assert_eq!(node_id, "p1");
    }

    #[test]
    fn type_index_key_prefix_correctness() {
        let key = type_index_key("g", "Patient", "p1");
        assert!(key.starts_with(&type_index_prefix("g", "Patient")));
    }

    #[test]
    fn type_index_key_different_types_different_prefixes() {
        let key_patient = type_index_key("g", "Patient", "p1");
        let key_lab = type_index_key("g", "LabResult", "l1");
        assert!(key_patient.starts_with(&type_index_prefix("g", "Patient")));
        assert!(!key_patient.starts_with(&type_index_prefix("g", "LabResult")));
        assert!(key_lab.starts_with(&type_index_prefix("g", "LabResult")));
    }

    #[test]
    fn ontology_keys_under_graph_prefix() {
        let otk = object_type_key("g", "Patient");
        let ltk = link_type_key("g", "HAS_RESULT");
        let tik = type_index_key("g", "Patient", "p1");
        let pfx = graph_prefix("g");
        assert!(otk.starts_with(&pfx));
        assert!(ltk.starts_with(&pfx));
        assert!(tik.starts_with(&pfx));
    }

    #[test]
    fn validate_type_name_empty() {
        assert!(validate_type_name("").is_err());
    }

    #[test]
    fn validate_type_name_slash() {
        assert!(validate_type_name("has/slash").is_err());
    }

    #[test]
    fn validate_type_name_reserved_prefix() {
        assert!(validate_type_name("__reserved").is_err());
    }

    #[test]
    fn validate_type_name_valid() {
        assert!(validate_type_name("Patient").is_ok());
        assert!(validate_type_name("HAS_RESULT").is_ok());
        assert!(validate_type_name("my-type").is_ok());
    }

    #[test]
    fn parse_object_type_key_wrong_graph_returns_none() {
        let key = object_type_key("g", "Patient");
        assert!(parse_object_type_key("other", &key).is_none());
    }

    #[test]
    fn parse_link_type_key_wrong_graph_returns_none() {
        let key = link_type_key("g", "KNOWS");
        assert!(parse_link_type_key("other", &key).is_none());
    }

    #[test]
    fn parse_type_index_key_wrong_graph_returns_none() {
        let key = type_index_key("g", "Patient", "p1");
        assert!(parse_type_index_key("other", &key).is_none());
    }

    // --- Namespace cache tests (#1297) ---

    #[test]
    fn namespace_cache_returns_same_arc_pointer() {
        // Use a unique branch ID to avoid interference from other tests
        let branch =
            BranchId::from_bytes([0xCA, 0xCE, 0x01, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        let ns1 = graph_namespace(branch, GRAPH_SPACE);
        let ns2 = graph_namespace(branch, GRAPH_SPACE);
        // Must be the *same* heap allocation (Arc pointer equality), not just
        // value-equal — this is the whole point of the cache.
        assert!(Arc::ptr_eq(&ns1, &ns2));
    }

    #[test]
    fn namespace_cache_different_branches_return_different_namespaces() {
        let branch_a =
            BranchId::from_bytes([0xCA, 0xCE, 0x02, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        let branch_b =
            BranchId::from_bytes([0xCA, 0xCE, 0x03, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        let ns_a = graph_namespace(branch_a, GRAPH_SPACE);
        let ns_b = graph_namespace(branch_b, GRAPH_SPACE);
        // Different branches → different namespace content
        assert_ne!(*ns_a, *ns_b);
        assert_eq!(ns_a.branch_id, branch_a);
        assert_eq!(ns_b.branch_id, branch_b);
        // Both must use the graph space
        assert_eq!(ns_a.space, GRAPH_SPACE);
        assert_eq!(ns_b.space, GRAPH_SPACE);
    }

    #[test]
    fn namespace_cache_different_spaces_return_different_namespaces() {
        // Same branch, two different spaces → distinct cache entries and
        // distinct Namespace contents.
        let branch =
            BranchId::from_bytes([0xCA, 0xCE, 0x07, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        let ns_default = graph_namespace(branch, "default");
        let ns_tenant = graph_namespace(branch, "tenant_a");
        // Same branch, different spaces → different namespace contents.
        assert_ne!(*ns_default, *ns_tenant);
        assert_eq!(ns_default.space, "default");
        assert_eq!(ns_tenant.space, "tenant_a");
        // And both must be the same instance on subsequent lookups.
        let ns_default_again = graph_namespace(branch, "default");
        let ns_tenant_again = graph_namespace(branch, "tenant_a");
        assert!(Arc::ptr_eq(&ns_default, &ns_default_again));
        assert!(Arc::ptr_eq(&ns_tenant, &ns_tenant_again));
    }

    #[test]
    fn invalidate_namespace_cache_forces_new_allocation() {
        let branch =
            BranchId::from_bytes([0xCA, 0xCE, 0x04, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        let ns_before = graph_namespace(branch, GRAPH_SPACE);

        invalidate_namespace_cache(&branch, GRAPH_SPACE);

        let ns_after = graph_namespace(branch, GRAPH_SPACE);
        // Value must be identical (same branch, same space)
        assert_eq!(*ns_before, *ns_after);
        // But must be a *new* heap allocation (different Arc pointer)
        assert!(!Arc::ptr_eq(&ns_before, &ns_after));
    }

    #[test]
    fn invalidate_namespace_cache_only_evicts_target_space() {
        // Invalidating one space must not evict another space's namespace
        // entry on the same branch.
        let branch =
            BranchId::from_bytes([0xCA, 0xCE, 0x08, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        let ns_default = graph_namespace(branch, "default");
        let ns_tenant = graph_namespace(branch, "tenant_a");

        invalidate_namespace_cache(&branch, "default");

        // tenant_a's entry must still be the same Arc — no eviction.
        let ns_tenant_after = graph_namespace(branch, "tenant_a");
        assert!(Arc::ptr_eq(&ns_tenant, &ns_tenant_after));
        // default's entry must be a new allocation now.
        let ns_default_after = graph_namespace(branch, "default");
        assert!(!Arc::ptr_eq(&ns_default, &ns_default_after));
    }

    #[test]
    fn uri_encode_decode_null_byte() {
        let uri = "kv://main/has\0null";
        let encoded = encode_uri(uri);
        assert!(encoded.contains("%00"));
        assert!(!encoded.contains('\0'));
        let decoded = decode_uri(&encoded);
        assert_eq!(decoded, uri);
    }

    #[test]
    fn uri_encode_decode_control_chars() {
        let uri = "kv://main/tab\there\x01\x1F";
        let encoded = encode_uri(uri);
        assert!(encoded.contains("%09")); // tab
        assert!(encoded.contains("%01"));
        assert!(encoded.contains("%1F"));
        let decoded = decode_uri(&encoded);
        assert_eq!(decoded, uri);
    }

    #[test]
    fn uri_encode_decode_mixed_special_chars() {
        // Mix of slashes, percent, null, and control chars
        let uri = "kv://main/path\0/100%/\x01end";
        let encoded = encode_uri(uri);
        let decoded = decode_uri(&encoded);
        assert_eq!(decoded, uri);
    }

    #[test]
    fn invalidate_nonexistent_branch_is_noop() {
        let branch =
            BranchId::from_bytes([0xCA, 0xCE, 0x05, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        // Should not panic or error
        invalidate_namespace_cache(&branch, GRAPH_SPACE);
    }

    #[test]
    fn hoisted_namespace_key_equals_storage_key() {
        // Verify that the bulk_insert optimization path
        // (Key::new_graph(ns.clone(), ...)) produces keys identical to
        // the standard path (storage_key(branch_id, ...)).
        let branch =
            BranchId::from_bytes([0xCA, 0xCE, 0x06, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        let ns = graph_namespace(branch, GRAPH_SPACE);

        // Test with various user_key patterns used in graph operations
        let user_keys = [
            node_key("mygraph", "patient-1"),
            forward_adj_key("mygraph", "A"),
            reverse_adj_key("mygraph", "B"),
            ref_index_key("kv://main/key1", "mygraph", "n1"),
            type_index_key("mygraph", "Patient", "p1"),
            meta_key("mygraph"),
        ];

        for user_key in &user_keys {
            let via_storage = storage_key(branch, GRAPH_SPACE, user_key);
            let via_hoisted = Key::new_graph(ns.clone(), user_key);
            assert_eq!(
                via_storage, via_hoisted,
                "Key mismatch for user_key={user_key:?}"
            );
            // Also verify ordering is consistent (used in BTreeMap scans)
            assert_eq!(
                via_storage.cmp(&via_hoisted),
                std::cmp::Ordering::Equal,
                "Ord mismatch for user_key={user_key:?}"
            );
        }
    }

    // --- Identifier length limit tests (G-6) ---

    #[test]
    fn test_graph_name_length_limit() {
        // Exactly at limit — should pass
        let at_limit = "a".repeat(MAX_IDENTIFIER_BYTES);
        assert!(validate_graph_name(&at_limit).is_ok());

        // One byte over — should fail with descriptive message
        let over_limit = "a".repeat(MAX_IDENTIFIER_BYTES + 1);
        let err = validate_graph_name(&over_limit).unwrap_err().to_string();
        assert!(
            err.contains("exceeds maximum length"),
            "Error should mention max length: {}",
            err
        );
    }

    #[test]
    fn test_node_id_length_limit() {
        let at_limit = "n".repeat(MAX_IDENTIFIER_BYTES);
        assert!(validate_node_id(&at_limit).is_ok());

        let over_limit = "n".repeat(MAX_IDENTIFIER_BYTES + 1);
        let err = validate_node_id(&over_limit).unwrap_err().to_string();
        assert!(
            err.contains("exceeds maximum length"),
            "Error should mention max length: {}",
            err
        );
    }

    #[test]
    fn test_edge_type_length_limit() {
        let at_limit = "E".repeat(MAX_IDENTIFIER_BYTES);
        assert!(validate_edge_type(&at_limit).is_ok());

        let over_limit = "E".repeat(MAX_IDENTIFIER_BYTES + 1);
        let err = validate_edge_type(&over_limit).unwrap_err().to_string();
        assert!(
            err.contains("exceeds maximum length"),
            "Error should mention max length: {}",
            err
        );
    }
}
