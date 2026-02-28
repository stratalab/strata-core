# Graph Analytics Cookbook

Analyze a social network using Strata's built-in graph algorithms. This recipe builds a small graph, then runs all six analytics algorithms to extract insights.

## Setup: Build a Social Network

```rust
let db = Strata::cache()?;
db.graph_create("social")?;

// Add people
for name in &["alice", "bob", "carol", "dave", "eve", "frank"] {
    db.graph_add_node("social", name, None, None)?;
}

// Add relationships (directed: "follows")
db.graph_add_edge("social", "alice", "bob",   "FOLLOWS", None, None)?;
db.graph_add_edge("social", "bob",   "alice", "FOLLOWS", None, None)?;
db.graph_add_edge("social", "alice", "carol", "FOLLOWS", None, None)?;
db.graph_add_edge("social", "carol", "alice", "FOLLOWS", None, None)?;
db.graph_add_edge("social", "bob",   "carol", "FOLLOWS", None, None)?;
db.graph_add_edge("social", "carol", "bob",   "FOLLOWS", None, None)?;
db.graph_add_edge("social", "carol", "dave",  "FOLLOWS", None, None)?;
db.graph_add_edge("social", "dave",  "eve",   "FOLLOWS", None, None)?;
db.graph_add_edge("social", "eve",   "dave",  "FOLLOWS", None, None)?;
db.graph_add_edge("social", "dave",  "frank", "FOLLOWS", None, None)?;
db.graph_add_edge("social", "frank", "dave",  "FOLLOWS", None, None)?;
db.graph_add_edge("social", "eve",   "frank", "FOLLOWS", None, None)?;
db.graph_add_edge("social", "frank", "eve",   "FOLLOWS", None, None)?;
```

This creates two clusters connected by a bridge:

```
  alice --- bob          dave --- eve
     \   /        bridge    \   /
      carol  ------------->  frank
```

The equivalent CLI setup:

```
graph create social
graph add-node social alice
graph add-node social bob
graph add-node social carol
graph add-node social dave
graph add-node social eve
graph add-node social frank
graph add-edge social alice bob   FOLLOWS
graph add-edge social bob   alice FOLLOWS
graph add-edge social alice carol FOLLOWS
graph add-edge social carol alice FOLLOWS
graph add-edge social bob   carol FOLLOWS
graph add-edge social carol bob   FOLLOWS
graph add-edge social carol dave  FOLLOWS
graph add-edge social dave  eve   FOLLOWS
graph add-edge social eve   dave  FOLLOWS
graph add-edge social dave  frank FOLLOWS
graph add-edge social frank dave  FOLLOWS
graph add-edge social eve   frank FOLLOWS
graph add-edge social frank eve   FOLLOWS
```

## 1. BFS — Explore the Graph

Starting from alice, explore the network up to depth 3:

```rust
let result = db.graph_bfs("social", "alice", 3, None, None, None)?;

// Visited in order: ["alice", "bob", "carol", "dave", "eve", "frank"]
// Depths: alice=0, bob=1, carol=1, dave=2, eve=3, frank=3
```

```
graph bfs social alice 3
```

**Use case:** Friend-of-friend discovery, reachability queries, shortest hop count.

## 2. WCC — Find Connected Components

Identify disconnected groups:

```rust
let components = db.graph_wcc("social")?;

// All six nodes are in the same component (connected via carol→dave bridge)
// {"alice": 0, "bob": 0, "carol": 0, "dave": 0, "eve": 0, "frank": 0}
```

```
graph wcc social
```

Now remove the bridge and check again:

```rust
db.graph_remove_edge("social", "carol", "dave", "FOLLOWS")?;
let components = db.graph_wcc("social")?;

// Two components:
// {"alice": 0, "bob": 0, "carol": 0, "dave": 1, "eve": 1, "frank": 1}
```

**Use case:** Finding isolated subgraphs, detecting network partitions, data quality checks.

## 3. CDLP — Detect Communities

Discover natural groupings through label propagation:

```rust
let communities = db.graph_cdlp("social", 10, None)?;

// Nodes in the same tight cluster converge to the same label:
// alice, bob, carol → community A
// dave, eve, frank  → community B
```

```
graph cdlp social 10
```

Try directed community detection:

```rust
// Only consider outgoing edges
let communities = db.graph_cdlp("social", 10, Some("outgoing"))?;
```

```
graph cdlp social 10 --direction outgoing
```

**Use case:** Interest-based user grouping, topic clustering, organizational structure detection.

## 4. PageRank — Rank by Importance

Find the most influential nodes:

```rust
let ranks = db.graph_pagerank("social", None, None, None)?;

// carol likely ranks highest — she bridges two clusters and receives
// links from both sides. The sum of all ranks equals 1.0.
```

```
graph pagerank social
```

Tune the algorithm:

```rust
// Higher damping (0.95) emphasizes link structure more
let ranks = db.graph_pagerank("social", Some(0.95), Some(100), Some(1e-8))?;
```

```
graph pagerank social --damping 0.95 --max-iterations 100 --tolerance 0.00000001
```

**Use case:** Identifying key influencers, ranking documents, finding critical infrastructure nodes.

## 5. LCC — Measure Local Clustering

How tightly connected are each node's neighbors?

```rust
let coefficients = db.graph_lcc("social")?;

// alice: 1.0 — both of her neighbors (bob, carol) are connected to each other
// carol: lower — she has neighbors in both clusters, and cross-cluster
//   neighbors don't know each other
// dave: high — his neighbors (eve, frank) are connected
```

```
graph lcc social
```

**Use case:** Identifying cliques, measuring social cohesion, spam detection (spammers tend to have low LCC).

## 6. SSSP — Shortest Paths

Find the shortest path from a source to all other nodes:

```rust
let distances = db.graph_sssp("social", "alice", None)?;

// {"alice": 0.0, "bob": 1.0, "carol": 1.0, "dave": 2.0, "eve": 3.0, "frank": 3.0}
```

```
graph sssp social alice
```

With weighted edges:

```rust
// Add weighted edges (e.g., latency in milliseconds)
db.graph_add_edge("net", "server_a", "server_b", "LINK", Some(5.0), None)?;
db.graph_add_edge("net", "server_b", "server_c", "LINK", Some(10.0), None)?;
db.graph_add_edge("net", "server_a", "server_c", "LINK", Some(20.0), None)?;

let distances = db.graph_sssp("net", "server_a", None)?;
// {"server_a": 0.0, "server_b": 5.0, "server_c": 15.0}
// server_c via server_b (5+10=15) is shorter than the direct link (20)
```

Traverse in reverse (who can reach me?):

```rust
let distances = db.graph_sssp("social", "dave", Some("incoming"))?;
// Finds shortest paths from all nodes TO dave (by following edges in reverse)
```

```
graph sssp social dave --direction incoming
```

**Use case:** Network routing, supply chain optimization, social distance measurement.

## Putting It All Together

A typical analytics workflow combines multiple algorithms:

```rust
let db = Strata::cache()?;

// 1. Check connectivity
let components = db.graph_wcc("social")?;
let num_components: std::collections::HashSet<_> = components.values().collect();
println!("{} connected component(s)", num_components.len());

// 2. Detect communities within the largest component
let communities = db.graph_cdlp("social", 20, None)?;

// 3. Rank nodes by importance
let ranks = db.graph_pagerank("social", None, None, None)?;

// 4. Find the top influencer
let top_node = ranks.iter()
    .max_by(|a, b| a.1.partial_cmp(b.1).unwrap())
    .map(|(node, rank)| (node.clone(), *rank));
println!("Top influencer: {:?}", top_node);

// 5. Measure how tightly-knit each community is
let lcc = db.graph_lcc("social")?;
let avg_clustering: f64 = lcc.values().sum::<f64>() / lcc.len() as f64;
println!("Average clustering coefficient: {:.3}", avg_clustering);

// 6. Shortest paths from the top influencer
if let Some((top, _)) = &top_node {
    let distances = db.graph_sssp("social", top, None)?;
    let max_dist = distances.values().cloned().fold(0.0f64, f64::max);
    println!("Max distance from {}: {}", top, max_dist);
}
```

## Algorithm Reference

| Algorithm | What it computes | Output type | Key parameters |
|-----------|-----------------|-------------|----------------|
| **BFS** | Level-by-level traversal | Visited nodes, depths, edges | `max_depth`, `direction` |
| **WCC** | Connected components | Node → component ID (u64) | None |
| **CDLP** | Community labels | Node → label (u64) | `max_iterations`, `direction` |
| **PageRank** | Node importance | Node → rank (f64, sum = 1.0) | `damping`, `max_iterations`, `tolerance` |
| **LCC** | Neighbor connectivity | Node → coefficient (f64, 0.0–1.0) | None |
| **SSSP** | Shortest weighted paths | Node → distance (f64) | `source`, `direction` |

## Next

- [Graph Guide](../guides/graph.md) — full graph API reference
- [Knowledge Graph Cookbook](knowledge-graph.md) — typed graphs with ontology
