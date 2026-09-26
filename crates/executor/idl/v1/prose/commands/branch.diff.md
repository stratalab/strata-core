---
summary: Compare two branches and report the entities that differ across every primitive.
mcp_description: Use this when the user wants to see what changed between two branches — which keys, documents, vectors, events, or graph nodes/edges/ontology were added, removed, or modified.
---

Compares two branches and reports the authored entities that differ, grouped by
capability and space: entries `added` on `branch_b`, `removed` relative to
`branch_a`, and `modified` on both sides. The comparison is directional from
`branch_a` to `branch_b`.

Every data primitive is compared — key-value, JSON documents, vectors, event
streams, and graphs. Graph changes are reported per row class: nodes, edges, and
ontology appear as separate capabilities in the result. Derived rows (search and
vector indexes, graph reverse maps) are omitted. A graph or space whose deletion is
still being swept on a branch compares as absent on that branch, exactly as a deleted
one does. A missing branch is rejected with `not_found.engine.branch`.
