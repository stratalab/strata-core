# Graph Store Guide

Model relationships between entities using a property graph with typed nodes and edges.

## API Overview

### Lifecycle

| Method | Signature | Returns |
|--------|-----------|---------|
| `graph_create` | `(graph: &str) -> Result<()>` | |
| `graph_create_with_policy` | `(graph: &str, cascade_policy: Option<&str>) -> Result<()>` | |
| `graph_delete` | `(graph: &str) -> Result<()>` | |
| `graph_list` | `() -> Result<Vec<String>>` | Graph names |
| `graph_get_meta` | `(graph: &str) -> Result<Option<Value>>` | Graph metadata or None |

### Node CRUD

| Method | Signature | Returns |
|--------|-----------|---------|
| `graph_add_node` | `(graph, node_id, entity_ref?, properties?) -> Result<()>` | |
| `graph_add_node_typed` | `(graph, node_id, entity_ref?, properties?, object_type?) -> Result<()>` | |
| `graph_get_node` | `(graph, node_id) -> Result<Option<Value>>` | Node data or None |
| `graph_remove_node` | `(graph, node_id) -> Result<()>` | |
| `graph_list_nodes` | `(graph) -> Result<Vec<String>>` | Node IDs |

### Edge CRUD

| Method | Signature | Returns |
|--------|-----------|---------|
| `graph_add_edge` | `(graph, src, dst, edge_type, weight?, properties?) -> Result<()>` | |
| `graph_remove_edge` | `(graph, src, dst, edge_type) -> Result<()>` | |

### Bulk

| Method | Signature | Returns |
|--------|-----------|---------|
| `graph_bulk_insert` | `(graph, nodes, edges) -> Result<(u64, u64)>` | (nodes_inserted, edges_inserted) |

### Traversal

| Method | Signature | Returns |
|--------|-----------|---------|
| `graph_neighbors` | `(graph, node_id, direction, edge_type?) -> Result<Vec<GraphNeighborHit>>` | Neighbor list |
| `graph_bfs` | `(graph, start, max_depth, max_nodes?, edge_types?, direction?) -> Result<GraphBfsResult>` | BFS result |

## Creating a Graph

Use `graph_create` to create a new empty graph on the current branch:

```rust
let db = Strata::cache()?;

db.graph_create("social")?;

// Verify it exists
let graphs = db.graph_list()?;
assert!(graphs.contains(&"social".to_string()));
```

### Cascade Policy

`graph_create_with_policy` lets you control what happens to edges when a node is removed:

```rust
// "cascade" — removing a node also removes all its edges (default-like behavior)
db.graph_create_with_policy("social", Some("cascade"))?;

// "detach" — edges are silently detached when a node is removed
db.graph_create_with_policy("links", Some("detach"))?;

// "ignore" — no special edge handling
db.graph_create_with_policy("tags", Some("ignore"))?;
```

## Adding Nodes

Add nodes with `graph_add_node`. Each node has an ID and optional properties:

```rust
use serde_json::json;

let db = Strata::cache()?;
db.graph_create("social")?;

// Simple node with no properties
db.graph_add_node("social", "alice", None, None)?;

// Node with properties
db.graph_add_node("social", "bob", None, Some(json!({
    "name": "Bob",
    "age": 30
})))?;

db.graph_add_node("social", "carol", None, Some(json!({
    "name": "Carol",
    "age": 25
})))?;
```

### Entity References

Bind a node to an existing KV or JSON document using `entity_ref`:

```rust
db.kv_put("user:alice", json!({"email": "alice@example.com"}))?;

// Bind the graph node to the KV entry
db.graph_add_node("social", "alice", Some("kv://default/user:alice"), Some(json!({
    "name": "Alice"
})))?;
```

### Typed Nodes

Use `graph_add_node_typed` to associate nodes with an ontology object type (see the [Ontology Guide](ontology.md)):

```rust
db.graph_add_node_typed(
    "social", "alice", None,
    Some(json!({"name": "Alice"})),
    Some("Person"),
)?;
```

## Adding Edges

Edges are directed and typed. Each edge connects a source node to a destination node:

```rust
let db = Strata::cache()?;
db.graph_create("social")?;

db.graph_add_node("social", "alice", None, None)?;
db.graph_add_node("social", "bob", None, None)?;
db.graph_add_node("social", "carol", None, None)?;

// alice follows bob
db.graph_add_edge("social", "alice", "bob", "FOLLOWS", None, None)?;

// bob follows carol
db.graph_add_edge("social", "bob", "carol", "FOLLOWS", None, None)?;

// alice follows carol
db.graph_add_edge("social", "alice", "carol", "FOLLOWS", None, None)?;

// alice likes bob's post — with a weight and properties
db.graph_add_edge("social", "alice", "bob", "LIKES", Some(0.9), Some(json!({
    "timestamp": "2025-01-15T10:30:00Z"
})))?;
```

## Querying

### Get a Node

```rust
let node = db.graph_get_node("social", "bob")?;
// Returns the node's properties, entity_ref, etc. as a Value
```

### List All Nodes

```rust
let nodes = db.graph_list_nodes("social")?;
// ["alice", "bob", "carol"]
```

### Neighbors

`graph_neighbors` returns the immediate neighbors of a node. The `direction` parameter controls which edges to follow:

- `"outgoing"` — nodes this node points to
- `"incoming"` — nodes that point to this node
- `"both"` — all connected nodes

```rust
// Who does alice follow?
let following = db.graph_neighbors("social", "alice", "outgoing", Some("FOLLOWS"))?;
// [GraphNeighborHit { node_id: "bob", edge_type: "FOLLOWS", weight: 1.0 },
//  GraphNeighborHit { node_id: "carol", edge_type: "FOLLOWS", weight: 1.0 }]

// Who follows carol?
let followers = db.graph_neighbors("social", "carol", "incoming", Some("FOLLOWS"))?;
// [GraphNeighborHit { node_id: "alice", ... }, GraphNeighborHit { node_id: "bob", ... }]

// All connections for bob (any edge type)
let all = db.graph_neighbors("social", "bob", "both", None)?;
```

### BFS Traversal

`graph_bfs` performs a breadth-first search from a start node, returning all reachable nodes up to a given depth:

```rust
let result = db.graph_bfs("social", "alice", 3, None, None, None)?;

// result.visited — node IDs in BFS order: ["alice", "bob", "carol"]
// result.depths  — depth map: {"alice": 0, "bob": 1, "carol": 1}
// result.edges   — edges traversed: [("alice","bob","FOLLOWS"), ("alice","carol","FOLLOWS")]
```

You can filter by edge type, limit the number of nodes, or restrict direction:

```rust
// Only follow FOLLOWS edges, max 10 nodes, outgoing only
let result = db.graph_bfs(
    "social",
    "alice",
    5,                                        // max_depth
    Some(10),                                 // max_nodes
    Some(vec!["FOLLOWS".to_string()]),        // edge_types
    Some("outgoing"),                         // direction
)?;
```

## Bulk Loading

For loading large datasets, `graph_bulk_insert` is significantly faster than individual `graph_add_node`/`graph_add_edge` calls. It uses chunked transactions internally to handle large payloads efficiently.

```rust
let db = Strata::cache()?;
db.graph_create("social")?;

let nodes = vec![
    ("alice", None, Some(json!({"name": "Alice"}))),
    ("bob",   None, Some(json!({"name": "Bob"}))),
    ("carol", None, Some(json!({"name": "Carol"}))),
];

let edges = vec![
    ("alice", "bob",   "FOLLOWS", None, None),
    ("bob",   "carol", "FOLLOWS", None, None),
    ("alice", "carol", "FOLLOWS", None, None),
];

let (nodes_inserted, edges_inserted) = db.graph_bulk_insert("social", &nodes, &edges)?;
assert_eq!(nodes_inserted, 3);
assert_eq!(edges_inserted, 3);
```

## Removing Data

### Remove a Node

Removing a node also removes all its incident edges (regardless of direction):

```rust
db.graph_remove_node("social", "bob")?;

// bob is gone, and so are all edges to/from bob
let nodes = db.graph_list_nodes("social")?;
assert!(!nodes.contains(&"bob".to_string()));
```

### Remove an Edge

Remove a specific edge by source, destination, and type:

```rust
db.graph_remove_edge("social", "alice", "carol", "FOLLOWS")?;
```

### Delete a Graph

Delete the entire graph and all its data:

```rust
db.graph_delete("social")?;
```

## Branch Isolation (Not Space-Scoped)

Graph data is **branch-scoped only** — unlike KV, JSON, and other primitives, graphs are not isolated by space. All graphs on a branch are visible regardless of which space is active. This is intentional: graphs model cross-cutting relationships across your data, and entity refs can point into any space (e.g. `kv://default/key`, `json://other-space/doc`). Graph names serve as the namespace boundary within a branch.

Each branch has its own independent copy of graph state, and graphs created on one branch are not visible on others:

```rust
let db = Strata::cache()?;

db.graph_create("social")?;
db.graph_add_node("social", "alice", None, None)?;

// Fork to a new branch
db.fork_branch("experiment")?;
db.set_branch("experiment")?;

// The forked branch inherits all graph data
let nodes = db.graph_list_nodes("social")?;
assert!(nodes.contains(&"alice".to_string()));

// Changes on the fork don't affect the original branch
db.graph_add_node("social", "dave", None, None)?;
db.set_branch("default")?;
let nodes = db.graph_list_nodes("social")?;
assert!(!nodes.contains(&"dave".to_string()));
```

## Next

- [Ontology Guide](ontology.md) — define typed schemas with validation
- [Knowledge Graph Cookbook](../cookbook/knowledge-graph.md) — end-to-end recipe combining graph + ontology
- [API Quick Reference](../reference/api-quick-reference.md) — all method signatures
