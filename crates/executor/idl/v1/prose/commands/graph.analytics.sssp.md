---
summary: Compute shortest-path distances from a source.
mcp_description: Use this when the user wants weighted shortest-path distances from one node to every reachable node (single-source shortest path / Dijkstra-style queries).
---

Computes weighted shortest-path distances from a source node over a consistent snapshot, and for every reachable node the predecessor its cheapest walk arrived from — follow `predecessors` back to the source to unpack a path. Edge weights (default 1.0) accumulate along paths; unreachable nodes are omitted from the result. Direction defaults to `outgoing`. `edge_types` restricts the walk to the listed types (a type the graph does not contain restricts to nothing, as for `bfs`); a negative-weight edge among the selected types refuses with `failed_precondition.engine.graph_negative_weight`. The source node must exist (`not_found.engine.graph_node`). Accepts an optional snapshot budget and `as_of` for time travel.
