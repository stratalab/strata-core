//! Key construction and parsing for graph storage.
//!
//! All graph data is stored under the `_graph_` space using KV-type keys.
//! Key format uses `/` as a separator between path segments.

#[cfg(test)]
use strata_core::types::TypeTag;
use strata_core::types::{BranchId, Key, Namespace};
use strata_core::{StrataError, StrataResult};

/// Separator used between path segments in graph keys.
const SEP: char = '/';

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
    Ok(())
}

// =============================================================================
// Key Construction
// =============================================================================

/// The reserved space name for graph data.
pub const GRAPH_SPACE: &str = "_graph_";

/// Build a namespace for graph operations on a given branch.
pub fn graph_namespace(branch_id: BranchId) -> Namespace {
    Namespace::for_branch_space(branch_id, GRAPH_SPACE)
}

/// Build a full storage Key from a user_key string in the graph namespace.
pub fn graph_key(branch_id: BranchId, user_key: &str) -> Key {
    Key::new_kv(graph_namespace(branch_id), user_key)
}

// --- Forward edge keys ---

/// Key for a forward edge: `{graph}/e/{src}/{edge_type}/{dst}`
pub fn forward_edge_key(graph: &str, src: &str, edge_type: &str, dst: &str) -> String {
    format!("{}{SEP}e{SEP}{}{SEP}{}{SEP}{}", graph, src, edge_type, dst)
}

/// Prefix for all forward edges from a given source: `{graph}/e/{src}/`
pub fn forward_edges_prefix(graph: &str, src: &str) -> String {
    format!("{}{SEP}e{SEP}{}{SEP}", graph, src)
}

/// Prefix for forward edges of a specific type: `{graph}/e/{src}/{edge_type}/`
pub fn forward_edges_typed_prefix(graph: &str, src: &str, edge_type: &str) -> String {
    format!("{}{SEP}e{SEP}{}{SEP}{}{SEP}", graph, src, edge_type)
}

/// Parse a forward edge key back into (src, edge_type, dst).
pub fn parse_forward_edge_key(graph: &str, user_key: &str) -> Option<(String, String, String)> {
    let prefix = format!("{}{SEP}e{SEP}", graph);
    let rest = user_key.strip_prefix(&prefix)?;
    let parts: Vec<&str> = rest.splitn(3, SEP).collect();
    if parts.len() == 3 {
        Some((
            parts[0].to_string(),
            parts[1].to_string(),
            parts[2].to_string(),
        ))
    } else {
        None
    }
}

// --- Reverse edge keys ---

/// Key for a reverse edge: `{graph}/r/{dst}/{edge_type}/{src}`
pub fn reverse_edge_key(graph: &str, dst: &str, edge_type: &str, src: &str) -> String {
    format!("{}{SEP}r{SEP}{}{SEP}{}{SEP}{}", graph, dst, edge_type, src)
}

/// Prefix for all reverse edges to a given destination: `{graph}/r/{dst}/`
pub fn reverse_edges_prefix(graph: &str, dst: &str) -> String {
    format!("{}{SEP}r{SEP}{}{SEP}", graph, dst)
}

/// Parse a reverse edge key back into (dst, edge_type, src).
pub fn parse_reverse_edge_key(graph: &str, user_key: &str) -> Option<(String, String, String)> {
    let prefix = format!("{}{SEP}r{SEP}", graph);
    let rest = user_key.strip_prefix(&prefix)?;
    let parts: Vec<&str> = rest.splitn(3, SEP).collect();
    if parts.len() == 3 {
        Some((
            parts[0].to_string(),
            parts[1].to_string(),
            parts[2].to_string(),
        ))
    } else {
        None
    }
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

/// Encode a URI for use in key paths (escape `/` → `%2F`, `%` → `%25`).
fn encode_uri(uri: &str) -> String {
    uri.replace('%', "%25").replace('/', "%2F")
}

/// Decode a URI from a key path.
fn decode_uri(encoded: &str) -> String {
    encoded.replace("%2F", "/").replace("%25", "%")
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

// --- Broad prefixes ---

/// Prefix for all forward edge keys in a graph: `{graph}/e/`
pub fn all_edges_prefix(graph: &str) -> String {
    format!("{}{SEP}e{SEP}", graph)
}

/// Prefix for all reverse edge keys in a graph: `{graph}/r/`
pub fn all_reverse_edges_prefix(graph: &str) -> String {
    format!("{}{SEP}r{SEP}", graph)
}

/// Prefix for all keys in a graph: `{graph}/`
pub fn graph_prefix(graph: &str) -> String {
    format!("{}{SEP}", graph)
}

// =============================================================================
// Helpers
// =============================================================================

/// Build a storage Key for a graph user_key on a specific branch.
pub fn storage_key(branch_id: BranchId, user_key: &str) -> Key {
    graph_key(branch_id, user_key)
}

/// Check that a storage key has the expected TypeTag::KV.
#[cfg(test)]
fn assert_kv_tag(key: &Key) {
    assert_eq!(key.type_tag, TypeTag::KV);
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Round-trip tests ---

    #[test]
    fn forward_edge_key_roundtrip() {
        let key = forward_edge_key("g", "A", "KNOWS", "B");
        let (src, edge_type, dst) = parse_forward_edge_key("g", &key).unwrap();
        assert_eq!(src, "A");
        assert_eq!(edge_type, "KNOWS");
        assert_eq!(dst, "B");
    }

    #[test]
    fn reverse_edge_key_roundtrip() {
        let key = reverse_edge_key("g", "B", "KNOWS", "A");
        let (dst, edge_type, src) = parse_reverse_edge_key("g", &key).unwrap();
        assert_eq!(dst, "B");
        assert_eq!(edge_type, "KNOWS");
        assert_eq!(src, "A");
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
    fn forward_edges_prefix_is_prefix() {
        let key = forward_edge_key("g", "A", "T", "B");
        assert!(key.starts_with(&forward_edges_prefix("g", "A")));
    }

    #[test]
    fn forward_edges_typed_prefix_matches_type() {
        let key = forward_edge_key("g", "A", "T", "B");
        assert!(key.starts_with(&forward_edges_typed_prefix("g", "A", "T")));

        let other_key = forward_edge_key("g", "A", "OTHER", "B");
        assert!(!other_key.starts_with(&forward_edges_typed_prefix("g", "A", "T")));
    }

    #[test]
    fn reverse_edges_prefix_is_prefix() {
        let key = reverse_edge_key("g", "B", "T", "A");
        assert!(key.starts_with(&reverse_edges_prefix("g", "B")));
    }

    #[test]
    fn all_edges_prefix_matches_forward_keys() {
        let key = forward_edge_key("g", "A", "T", "B");
        assert!(key.starts_with(&all_edges_prefix("g")));
    }

    #[test]
    fn all_nodes_prefix_matches_node_keys() {
        let key = node_key("g", "X");
        assert!(key.starts_with(&all_nodes_prefix("g")));
    }

    #[test]
    fn graph_prefix_matches_all_keys() {
        let nk = node_key("g", "X");
        let fk = forward_edge_key("g", "A", "T", "B");
        let mk = meta_key("g");
        let pfx = graph_prefix("g");
        assert!(nk.starts_with(&pfx));
        assert!(fk.starts_with(&pfx));
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
    fn storage_key_has_kv_type_tag() {
        let branch = BranchId::from_bytes([0u8; 16]);
        let key = storage_key(branch, "test/key");
        assert_kv_tag(&key);
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
    fn parse_forward_edge_key_wrong_graph_returns_none() {
        let key = forward_edge_key("g", "A", "T", "B");
        assert!(parse_forward_edge_key("other", &key).is_none());
    }

    #[test]
    fn parse_forward_edge_key_malformed_returns_none() {
        assert!(parse_forward_edge_key("g", "g/e/A").is_none());
        assert!(parse_forward_edge_key("g", "garbage").is_none());
    }

    #[test]
    fn parse_reverse_edge_key_wrong_graph_returns_none() {
        let key = reverse_edge_key("g", "B", "T", "A");
        assert!(parse_reverse_edge_key("other", &key).is_none());
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
    fn forward_edge_key_not_parseable_as_node() {
        let key = forward_edge_key("g", "A", "T", "B");
        assert!(
            parse_node_key("g", &key).is_none() || {
                // It may parse as a node key but the result should not be "A"
                // since the format is different
                let parsed = parse_node_key("g", &key).unwrap();
                parsed != "A"
            }
        );
    }

    #[test]
    fn node_key_not_parseable_as_forward_edge() {
        let key = node_key("g", "mynode");
        assert!(parse_forward_edge_key("g", &key).is_none());
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
}
