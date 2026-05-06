//! Packed binary adjacency list encoding for graph edges.
//!
//! Stores all edges from/to a single node in a single KV entry as `Value::Bytes`,
//! reducing entry count by ~27x compared to one-entry-per-edge.
//!
//! ## Wire format
//!
//! ```text
//! Header:
//!   [u32: edge_count]                          (little-endian)
//!
//! Per edge (repeated edge_count times):
//!   [u16: edge_type_len] [u8 × len: edge_type] (UTF-8)
//!   [u16: target_id_len] [u8 × len: target_id] (UTF-8)
//!   [f64: weight]                               (little-endian)
//!   [u32: properties_len]                       (0 = no properties)
//!   [u8 × len: properties_json]                 (only if len > 0)
//! ```

use crate::{StrataError, StrataResult};

use super::types::EdgeData;

/// Maximum adjacency list size in bytes (16 MB).
const MAX_ADJ_LIST_BYTES: usize = 16 * 1024 * 1024;

/// Encode a list of edges into a packed binary adjacency list.
///
/// Each entry is `(target_id, edge_type, EdgeData)`.
pub fn encode(edges: &[(&str, &str, &EdgeData)]) -> Vec<u8> {
    let mut buf = Vec::new();
    // Header: edge count
    buf.extend_from_slice(&(edges.len() as u32).to_le_bytes());
    for &(target_id, edge_type, data) in edges {
        write_edge(&mut buf, target_id, edge_type, data);
    }
    buf
}

/// Decode a packed binary adjacency list into `(target_id, edge_type, EdgeData)` triples.
pub fn decode(bytes: &[u8]) -> StrataResult<Vec<(String, String, EdgeData)>> {
    if bytes.len() < 4 {
        return Err(StrataError::serialization(
            "Packed adjacency list too short for header".to_string(),
        ));
    }
    let count = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
    let mut result = Vec::with_capacity(count);
    let mut pos = 4;
    for _ in 0..count {
        let (target_id, edge_type, data, new_pos) = read_edge(bytes, pos)?;
        result.push((target_id, edge_type, data));
        pos = new_pos;
    }
    Ok(result)
}

/// Append an edge to an existing packed buffer (read-modify-write).
///
/// Updates the header count and appends the new entry bytes.
/// Returns an error if the result would exceed `MAX_ADJ_LIST_BYTES`.
pub fn append_edge(
    buf: &mut Vec<u8>,
    target_id: &str,
    edge_type: &str,
    data: &EdgeData,
) -> StrataResult<()> {
    if buf.len() < 4 {
        // Initialize empty list
        buf.clear();
        buf.extend_from_slice(&0u32.to_le_bytes());
    }
    // Estimate new size before mutating: 2 + edge_type + 2 + target_id + 8 (f64) + 4 (props_len) + props
    let props_len = match &data.properties {
        Some(props) => serde_json::to_string(props).unwrap_or_default().len(),
        None => 0,
    };
    let entry_size = 2 + edge_type.len() + 2 + target_id.len() + 8 + 4 + props_len;
    if buf.len() + entry_size > MAX_ADJ_LIST_BYTES {
        return Err(StrataError::invalid_input(format!(
            "Adjacency list would exceed maximum size of {} bytes (~500K+ edges per node)",
            MAX_ADJ_LIST_BYTES
        )));
    }
    // Increment count
    let old_count = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
    let new_count = old_count + 1;
    buf[0..4].copy_from_slice(&new_count.to_le_bytes());
    // Append entry
    write_edge(buf, target_id, edge_type, data);
    Ok(())
}

/// Remove the first edge matching `(target_id, edge_type)` from a packed list.
///
/// Returns `Some(new_bytes)` if found and removed, `None` if not found.
/// If removing the last edge, returns `Some` with an empty list (count=0).
pub fn remove_edge(bytes: &[u8], target_id: &str, edge_type: &str) -> Option<Vec<u8>> {
    if bytes.len() < 4 {
        return None;
    }
    let count = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
    let mut pos = 4;
    let mut found = false;
    let mut skip_start = 0;
    let mut skip_end = 0;

    for _ in 0..count {
        let entry_start = pos;
        let (tid, et, _data, new_pos) = read_edge(bytes, pos).ok()?;
        if !found && tid == target_id && et == edge_type {
            found = true;
            skip_start = entry_start;
            skip_end = new_pos;
        }
        pos = new_pos;
    }

    if !found {
        return None;
    }

    let new_count = (count - 1) as u32;
    let mut result = Vec::with_capacity(bytes.len() - (skip_end - skip_start));
    result.extend_from_slice(&new_count.to_le_bytes());
    result.extend_from_slice(&bytes[4..skip_start]);
    result.extend_from_slice(&bytes[skip_end..pos]);
    Some(result)
}

/// Find a specific edge in a packed list by `(target_id, edge_type)`.
pub fn find_edge(bytes: &[u8], target_id: &str, edge_type: &str) -> Option<EdgeData> {
    if bytes.len() < 4 {
        return None;
    }
    let count = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
    let mut pos = 4;
    for _ in 0..count {
        let (tid, et, data, new_pos) = read_edge(bytes, pos).ok()?;
        if tid == target_id && et == edge_type {
            return Some(data);
        }
        pos = new_pos;
    }
    None
}

/// Read the edge count from a packed header (no full decode).
pub fn edge_count(bytes: &[u8]) -> u32 {
    if bytes.len() < 4 {
        return 0;
    }
    u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
}

/// Merge two packed adjacency lists by appending `new` edges onto `existing`.
///
/// Updates the header count and concatenates the edge bytes. No decoding needed.
pub fn merge(mut existing: Vec<u8>, new: &[u8]) -> Vec<u8> {
    if new.len() <= 4 {
        return existing;
    }
    if existing.len() < 4 {
        return new.to_vec();
    }
    let existing_count = u32::from_le_bytes([existing[0], existing[1], existing[2], existing[3]]);
    let new_count = u32::from_le_bytes([new[0], new[1], new[2], new[3]]);
    let total = existing_count + new_count;
    existing[0..4].copy_from_slice(&total.to_le_bytes());
    existing.extend_from_slice(&new[4..]);
    existing
}

/// Encode an empty adjacency list (count = 0).
pub fn empty() -> Vec<u8> {
    0u32.to_le_bytes().to_vec()
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

fn write_edge(buf: &mut Vec<u8>, target_id: &str, edge_type: &str, data: &EdgeData) {
    // edge_type
    buf.extend_from_slice(&(edge_type.len() as u16).to_le_bytes());
    buf.extend_from_slice(edge_type.as_bytes());
    // target_id
    buf.extend_from_slice(&(target_id.len() as u16).to_le_bytes());
    buf.extend_from_slice(target_id.as_bytes());
    // weight
    buf.extend_from_slice(&data.weight.to_le_bytes());
    // properties
    match &data.properties {
        Some(props) => {
            let json = serde_json::to_string(props).unwrap_or_default();
            buf.extend_from_slice(&(json.len() as u32).to_le_bytes());
            buf.extend_from_slice(json.as_bytes());
        }
        None => {
            buf.extend_from_slice(&0u32.to_le_bytes());
        }
    }
}

fn read_edge(bytes: &[u8], mut pos: usize) -> StrataResult<(String, String, EdgeData, usize)> {
    let err =
        || StrataError::serialization("Packed adjacency list truncated or corrupt".to_string());

    // edge_type
    if pos + 2 > bytes.len() {
        return Err(err());
    }
    let et_len = u16::from_le_bytes([bytes[pos], bytes[pos + 1]]) as usize;
    pos += 2;
    if pos + et_len > bytes.len() {
        return Err(err());
    }
    let edge_type = std::str::from_utf8(&bytes[pos..pos + et_len])
        .map_err(|_| err())?
        .to_string();
    pos += et_len;

    // target_id
    if pos + 2 > bytes.len() {
        return Err(err());
    }
    let tid_len = u16::from_le_bytes([bytes[pos], bytes[pos + 1]]) as usize;
    pos += 2;
    if pos + tid_len > bytes.len() {
        return Err(err());
    }
    let target_id = std::str::from_utf8(&bytes[pos..pos + tid_len])
        .map_err(|_| err())?
        .to_string();
    pos += tid_len;

    // weight
    if pos + 8 > bytes.len() {
        return Err(err());
    }
    let weight = f64::from_le_bytes([
        bytes[pos],
        bytes[pos + 1],
        bytes[pos + 2],
        bytes[pos + 3],
        bytes[pos + 4],
        bytes[pos + 5],
        bytes[pos + 6],
        bytes[pos + 7],
    ]);
    pos += 8;

    // properties
    if pos + 4 > bytes.len() {
        return Err(err());
    }
    let props_len =
        u32::from_le_bytes([bytes[pos], bytes[pos + 1], bytes[pos + 2], bytes[pos + 3]]) as usize;
    pos += 4;
    let properties = if props_len > 0 {
        if pos + props_len > bytes.len() {
            return Err(err());
        }
        let json_str = std::str::from_utf8(&bytes[pos..pos + props_len]).map_err(|_| err())?;
        let val: serde_json::Value = serde_json::from_str(json_str)
            .map_err(|e| StrataError::serialization(e.to_string()))?;
        pos += props_len;
        Some(val)
    } else {
        None
    };

    Ok((target_id, edge_type, EdgeData { weight, properties }, pos))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_data() -> EdgeData {
        EdgeData::default()
    }

    fn weighted_data(w: f64) -> EdgeData {
        EdgeData {
            weight: w,
            properties: None,
        }
    }

    fn data_with_props() -> EdgeData {
        EdgeData {
            weight: 0.75,
            properties: Some(serde_json::json!({"confidence": "high", "source": "manual"})),
        }
    }

    #[test]
    fn empty_list() {
        let buf = encode(&[]);
        assert_eq!(edge_count(&buf), 0);
        let decoded = decode(&buf).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn single_edge_roundtrip() {
        let data = default_data();
        let buf = encode(&[("B", "KNOWS", &data)]);
        assert_eq!(edge_count(&buf), 1);
        let decoded = decode(&buf).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].0, "B");
        assert_eq!(decoded[0].1, "KNOWS");
        assert_eq!(decoded[0].2.weight, 1.0);
        assert!(decoded[0].2.properties.is_none());
    }

    #[test]
    fn many_edges_roundtrip() {
        let d1 = default_data();
        let d2 = weighted_data(0.5);
        let d3 = data_with_props();
        let edges: Vec<(&str, &str, &EdgeData)> = vec![
            ("B", "KNOWS", &d1),
            ("C", "LIKES", &d2),
            ("D", "WORKS_WITH", &d3),
        ];
        let buf = encode(&edges);
        assert_eq!(edge_count(&buf), 3);
        let decoded = decode(&buf).unwrap();
        assert_eq!(decoded.len(), 3);
        assert_eq!(decoded[0], ("B".to_string(), "KNOWS".to_string(), d1));
        assert_eq!(decoded[1], ("C".to_string(), "LIKES".to_string(), d2));
        assert_eq!(decoded[2].0, "D");
        assert_eq!(decoded[2].1, "WORKS_WITH");
        assert!((decoded[2].2.weight - 0.75).abs() < f64::EPSILON);
        assert!(decoded[2].2.properties.is_some());
    }

    #[test]
    fn append_to_empty() {
        let mut buf = empty();
        append_edge(&mut buf, "X", "E", &default_data()).unwrap();
        assert_eq!(edge_count(&buf), 1);
        let decoded = decode(&buf).unwrap();
        assert_eq!(decoded[0].0, "X");
    }

    #[test]
    fn append_multiple() {
        let mut buf = empty();
        append_edge(&mut buf, "A", "E1", &default_data()).unwrap();
        append_edge(&mut buf, "B", "E2", &weighted_data(2.0)).unwrap();
        append_edge(&mut buf, "C", "E3", &data_with_props()).unwrap();
        assert_eq!(edge_count(&buf), 3);
        let decoded = decode(&buf).unwrap();
        assert_eq!(decoded.len(), 3);
        assert_eq!(decoded[0].0, "A");
        assert_eq!(decoded[1].0, "B");
        assert_eq!(decoded[2].0, "C");
    }

    #[test]
    fn find_edge_present() {
        let d = weighted_data(0.42);
        let buf = encode(&[("B", "KNOWS", &default_data()), ("C", "LIKES", &d)]);
        let found = find_edge(&buf, "C", "LIKES").unwrap();
        assert!((found.weight - 0.42).abs() < f64::EPSILON);
    }

    #[test]
    fn find_edge_missing() {
        let buf = encode(&[("B", "KNOWS", &default_data())]);
        assert!(find_edge(&buf, "C", "KNOWS").is_none());
        assert!(find_edge(&buf, "B", "LIKES").is_none());
    }

    #[test]
    fn find_edge_empty() {
        let buf = empty();
        assert!(find_edge(&buf, "A", "E").is_none());
    }

    #[test]
    fn remove_edge_present() {
        let d1 = default_data();
        let d2 = weighted_data(0.5);
        let buf = encode(&[("B", "KNOWS", &d1), ("C", "LIKES", &d2)]);
        let new_buf = remove_edge(&buf, "B", "KNOWS").unwrap();
        assert_eq!(edge_count(&new_buf), 1);
        let decoded = decode(&new_buf).unwrap();
        assert_eq!(decoded[0].0, "C");
        assert_eq!(decoded[0].1, "LIKES");
    }

    #[test]
    fn remove_edge_missing() {
        let buf = encode(&[("B", "KNOWS", &default_data())]);
        assert!(remove_edge(&buf, "X", "KNOWS").is_none());
    }

    #[test]
    fn remove_last_edge() {
        let buf = encode(&[("B", "KNOWS", &default_data())]);
        let new_buf = remove_edge(&buf, "B", "KNOWS").unwrap();
        assert_eq!(edge_count(&new_buf), 0);
        let decoded = decode(&new_buf).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn remove_middle_edge() {
        let d = default_data();
        let buf = encode(&[("A", "E1", &d), ("B", "E2", &d), ("C", "E3", &d)]);
        let new_buf = remove_edge(&buf, "B", "E2").unwrap();
        assert_eq!(edge_count(&new_buf), 2);
        let decoded = decode(&new_buf).unwrap();
        assert_eq!(decoded[0].0, "A");
        assert_eq!(decoded[1].0, "C");
    }

    #[test]
    fn properties_roundtrip() {
        let d = data_with_props();
        let buf = encode(&[("B", "E", &d)]);
        let decoded = decode(&buf).unwrap();
        assert_eq!(decoded[0].2.properties, d.properties);
    }

    #[test]
    fn truncated_buffer_errors() {
        assert!(decode(&[]).is_err());
        assert!(decode(&[1, 0, 0]).is_err());
        // Valid header saying 1 edge but no data
        assert!(decode(&[1, 0, 0, 0]).is_err());
    }

    #[test]
    fn edge_count_on_short_buffer() {
        assert_eq!(edge_count(&[]), 0);
        assert_eq!(edge_count(&[1, 2]), 0);
    }

    #[test]
    fn unicode_edge_type_and_target_roundtrip() {
        let d = default_data();
        let buf = encode(&[("nœud_ünîcödé", "关系类型", &d)]);
        let decoded = decode(&buf).unwrap();
        assert_eq!(decoded[0].0, "nœud_ünîcödé");
        assert_eq!(decoded[0].1, "关系类型");
    }

    #[test]
    fn duplicate_edges_preserved() {
        // Same (target, edge_type) can appear multiple times — packed format allows it
        let d1 = weighted_data(1.0);
        let d2 = weighted_data(2.0);
        let buf = encode(&[("B", "E", &d1), ("B", "E", &d2)]);
        assert_eq!(edge_count(&buf), 2);
        let decoded = decode(&buf).unwrap();
        assert_eq!(decoded.len(), 2);
        assert!((decoded[0].2.weight - 1.0).abs() < f64::EPSILON);
        assert!((decoded[1].2.weight - 2.0).abs() < f64::EPSILON);
    }

    #[test]
    fn find_edge_returns_correct_properties() {
        let d1 = EdgeData {
            weight: 1.0,
            properties: Some(serde_json::json!({"label": "first"})),
        };
        let d2 = EdgeData {
            weight: 2.0,
            properties: Some(serde_json::json!({"label": "second"})),
        };
        let buf = encode(&[("B", "E1", &d1), ("C", "E2", &d2)]);
        let found = find_edge(&buf, "C", "E2").unwrap();
        assert!((found.weight - 2.0).abs() < f64::EPSILON);
        assert_eq!(
            found.properties.unwrap()["label"],
            serde_json::json!("second")
        );
    }

    #[test]
    fn remove_edge_preserves_remaining_data() {
        let d1 = weighted_data(1.0);
        let d2 = data_with_props();
        let d3 = weighted_data(3.0);
        let buf = encode(&[("A", "E1", &d1), ("B", "E2", &d2), ("C", "E3", &d3)]);
        let new_buf = remove_edge(&buf, "B", "E2").unwrap();
        let decoded = decode(&new_buf).unwrap();
        assert_eq!(decoded.len(), 2);
        // First edge preserved
        assert_eq!(decoded[0].0, "A");
        assert!((decoded[0].2.weight - 1.0).abs() < f64::EPSILON);
        assert!(decoded[0].2.properties.is_none());
        // Third edge preserved
        assert_eq!(decoded[1].0, "C");
        assert!((decoded[1].2.weight - 3.0).abs() < f64::EPSILON);
        assert!(decoded[1].2.properties.is_none());
    }

    #[test]
    fn remove_edge_with_duplicates_removes_only_first() {
        let d1 = weighted_data(1.0);
        let d2 = weighted_data(2.0);
        let buf = encode(&[("B", "E", &d1), ("B", "E", &d2)]);
        let new_buf = remove_edge(&buf, "B", "E").unwrap();
        assert_eq!(edge_count(&new_buf), 1);
        let decoded = decode(&new_buf).unwrap();
        // Second occurrence (weight 2.0) should remain
        assert!((decoded[0].2.weight - 2.0).abs() < f64::EPSILON);
    }

    #[test]
    fn append_edge_overflow_does_not_corrupt_buffer() {
        // Build a buffer just under the limit, then try to add a huge edge
        let mut buf = empty();
        append_edge(&mut buf, "A", "E", &default_data()).unwrap();
        let count_before = edge_count(&buf);
        let len_before = buf.len();

        // Create an edge with a massive properties blob that would exceed 16 MB
        let big_props = serde_json::json!({"data": "x".repeat(MAX_ADJ_LIST_BYTES + 1)});
        let big_data = EdgeData {
            weight: 1.0,
            properties: Some(big_props),
        };
        let result = append_edge(&mut buf, "B", "E2", &big_data);
        assert!(result.is_err());
        // Buffer should be unchanged
        assert_eq!(edge_count(&buf), count_before);
        assert_eq!(buf.len(), len_before);
    }

    #[test]
    fn empty_strings_roundtrip() {
        // Edge type and target_id can technically be empty strings
        let d = default_data();
        let buf = encode(&[("", "", &d)]);
        let decoded = decode(&buf).unwrap();
        assert_eq!(decoded[0].0, "");
        assert_eq!(decoded[0].1, "");
    }

    #[test]
    fn find_edge_with_duplicates_returns_first() {
        let d1 = weighted_data(1.0);
        let d2 = weighted_data(2.0);
        let buf = encode(&[("B", "E", &d1), ("B", "E", &d2)]);
        let found = find_edge(&buf, "B", "E").unwrap();
        assert!((found.weight - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn append_then_find() {
        let mut buf = empty();
        let d1 = weighted_data(1.0);
        let d2 = weighted_data(2.0);
        append_edge(&mut buf, "A", "E1", &d1).unwrap();
        append_edge(&mut buf, "B", "E2", &d2).unwrap();
        assert!(find_edge(&buf, "A", "E1").is_some());
        assert!(find_edge(&buf, "B", "E2").is_some());
        assert!(find_edge(&buf, "A", "E2").is_none());
    }

    #[test]
    fn append_then_remove_then_decode() {
        let mut buf = empty();
        append_edge(&mut buf, "A", "E1", &default_data()).unwrap();
        append_edge(&mut buf, "B", "E2", &weighted_data(0.5)).unwrap();
        append_edge(&mut buf, "C", "E3", &data_with_props()).unwrap();
        let buf = remove_edge(&buf, "B", "E2").unwrap();
        let decoded = decode(&buf).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].0, "A");
        assert_eq!(decoded[1].0, "C");
        assert!(decoded[1].2.properties.is_some());
    }
}
