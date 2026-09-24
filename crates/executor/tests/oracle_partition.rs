//! TCP4.10 partitioning + cross-algorithm oracles (the TLP analog).
//!
//! Partition a query by a predicate and require the parts to reassemble into
//! the whole — the verdict is an algebraic identity, not an expected value.
//! Plus the cross-algorithm differential the class shares: two algorithms
//! that must agree where their domains coincide (#2692's fixed seed, pinned
//! as a permanent contract).

use serde_json::json;

#[path = "parity/support.rs"]
mod support;

fn item_keys(page: &serde_json::Value) -> Vec<String> {
    page["data"]["items"]
        .as_array()
        .unwrap_or_else(|| panic!("page carries items: {page}"))
        .iter()
        .map(|item| {
            item.as_str()
                .unwrap_or_else(|| panic!("list items are keys: {item}"))
                .to_owned()
        })
        .collect()
}

/// TLP over the keyspace: partitioning a two-letter key universe by first
/// byte must reassemble exactly into the full listing — disjoint, complete.
#[test]
fn kv_prefix_partition_reassembles_into_the_full_listing() {
    let mut executor = support::executor();
    let mut entries = Vec::new();
    for index in 0..10_u8 {
        entries.push(json!({"key": support_base64(&[b'a', index]), "value": "b25l"}));
        entries.push(json!({"key": support_base64(&[b'b', index]), "value": "b25l"}));
    }
    support::run(
        &mut executor,
        &json!({"type": "kv_batch_put", "entries": entries}),
    );

    let total = support::run(&mut executor, &json!({"type": "kv_count"}))["data"]
        .as_u64()
        .expect("kv_count returns a count");
    let part_a = item_keys(&support::run(
        &mut executor,
        &json!({"type": "kv_list", "prefix": support_base64(b"a"), "limit": 1000}),
    ));
    let part_b = item_keys(&support::run(
        &mut executor,
        &json!({"type": "kv_list", "prefix": support_base64(b"b"), "limit": 1000}),
    ));

    assert_eq!(
        part_a.len() + part_b.len(),
        usize::try_from(total).expect("count fits usize"),
        "partition is complete against the total count"
    );
    let mut reassembled = [part_a, part_b].concat();
    let before = reassembled.len();
    reassembled.sort();
    reassembled.dedup();
    assert_eq!(reassembled.len(), before, "partitions are disjoint");
}

/// TLP over the event log: the per-type listings enumerated by
/// `event_list_types` must reassemble exactly into the full log.
#[test]
fn event_type_partition_reassembles_into_the_full_log() {
    let mut executor = support::executor();
    for index in 0..15_u64 {
        support::run(
            &mut executor,
            &json!({"type": "event_append", "event_type": format!("t{}", index % 4),
                    "payload": {"i": index}}),
        );
    }
    let full = support::run(&mut executor, &json!({"type": "event_list", "limit": 1000}));
    let total = full["data"]["items"]
        .as_array()
        .unwrap_or_else(|| panic!("event_list carries items: {full}"))
        .len();

    let types = support::run(&mut executor, &json!({"type": "event_list_types"}));
    let mut partitioned = 0;
    for event_type in types["data"]["items"]
        .as_array()
        .unwrap_or_else(|| panic!("event_list_types carries items: {types}"))
    {
        let event_type = event_type.as_str().expect("type names are strings");
        let part = support::run(
            &mut executor,
            &json!({"type": "event_list", "event_type": event_type, "limit": 1000}),
        );
        partitioned += part["data"]["items"]
            .as_array()
            .unwrap_or_else(|| panic!("typed event_list carries items: {part}"))
            .len();
    }
    assert_eq!(
        partitioned, total,
        "per-type partitions reassemble into the full log"
    );
}

/// Cross-algorithm contract (#2692's fixed seed, permanent): on a unit-weight
/// graph, SSSP distances equal BFS depths for every reachable node.
#[test]
fn contract_2692_unit_weight_sssp_equals_bfs_depths() {
    let mut executor = support::executor();
    support::run(
        &mut executor,
        &json!({"type": "graph_create", "graph": "uw"}),
    );
    let edges = [
        ("a", "b"),
        ("b", "c"),
        ("c", "d"),
        ("a", "e"),
        ("e", "d"),
        ("b", "e"),
    ];
    let mut operations: Vec<_> = ["a", "b", "c", "d", "e"]
        .iter()
        .map(|node| json!({"type": "upsert_node", "node_id": node, "data": {}}))
        .collect();
    operations.extend(edges.iter().map(|(src, dst)| {
        json!({"type": "upsert_edge", "src": src, "dst": dst, "edge_type": "link",
               "data": {"weight": 1.0}})
    }));
    // A `detour` shortcut a -> d: unfiltered, both algorithms take it (d at
    // 1); restricted to `link`, both must ignore it (d at 2 via e).
    operations.push(
        json!({"type": "upsert_edge", "src": "a", "dst": "d", "edge_type": "detour",
                           "data": {"weight": 1.0}}),
    );
    support::run(
        &mut executor,
        &json!({"type": "graph_batch_write", "graph": "uw", "operations": operations}),
    );

    let mut assert_sssp_equals_bfs = |edge_types: Option<Vec<&str>>, expected_d: u64| {
        let mut bfs_request =
            json!({"type": "graph_bfs", "graph": "uw", "start": "a", "max_depth": 10});
        let mut sssp_request = json!({"type": "graph_sssp", "graph": "uw", "source": "a"});
        if let Some(types) = &edge_types {
            bfs_request["edge_types"] = json!(types);
            sssp_request["edge_types"] = json!(types);
        }
        let bfs = support::run(&mut executor, &bfs_request);
        let depths = bfs["data"]["depths"]
            .as_object()
            .unwrap_or_else(|| panic!("bfs carries depths: {bfs}"));
        let sssp = support::run(&mut executor, &sssp_request);
        let distances = sssp["data"]["distances"]
            .as_object()
            .unwrap_or_else(|| panic!("sssp carries distances: {sssp}"));

        assert_eq!(
            depths.len(),
            distances.len(),
            "both algorithms reach the same node set under {edge_types:?}"
        );
        assert_eq!(
            depths["d"].as_u64(),
            Some(expected_d),
            "the detour is taken or ignored as the filter says"
        );
        for (node, depth) in depths {
            let depth = depth.as_u64().expect("depth is integral");
            let distance = distances[node].as_f64().expect("distance is numeric");
            // Depths here are tiny (<16); the u64->f64 cast is exact.
            #[allow(clippy::cast_precision_loss)]
            let depth_f = depth as f64;
            assert!(
                (distance - depth_f).abs() < f64::EPSILON,
                "node `{node}`: sssp distance {distance} != bfs depth {depth} on unit weights under {edge_types:?}"
            );
        }
    };
    assert_sssp_equals_bfs(None, 1);
    assert_sssp_equals_bfs(Some(vec!["link"]), 2);
}

/// Minimal base64 (shared shape with the pivot oracle's helper).
fn support_base64(bytes: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        out.push(TABLE[(n >> 18) as usize & 0x3f] as char);
        out.push(TABLE[(n >> 12) as usize & 0x3f] as char);
        out.push(if chunk.len() > 1 {
            TABLE[(n >> 6) as usize & 0x3f] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            TABLE[n as usize & 0x3f] as char
        } else {
            '='
        });
    }
    out
}
