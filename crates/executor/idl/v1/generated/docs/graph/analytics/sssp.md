---
title: "Compute graph shortest paths"
description: "Compute shortest-path distances from a source."
source: strata-core@1.2.1
section: graph
---

Computes weighted shortest-path distances from a source node over a consistent snapshot. Edge weights (default 1.0) accumulate along paths; unreachable nodes are omitted from the result. Direction defaults to `outgoing`. The source node must exist (`not_found.engine.graph_node`). Accepts an optional snapshot budget and `as_of` for time travel.

Analytics commands compute over a consistent snapshot of the visible graph and return a complete result payload in one response. They accept optional snapshot budgets and an `as_of` timestamp for time travel; results are deterministic for a fixed graph state.

## Examples

Single-source shortest paths from a source node.

### CLI

```console
$ strata graph create g
created graph g
$ strata graph add-node g a
created node a in g
$ strata graph add-node g b
created node b in g
$ strata graph add-node g c
created node c in g
$ strata graph add-edge g a knows b
created edge a -[knows]-> b in g
$ strata graph add-edge g b knows c
created edge b -[knows]-> c in g
$ strata graph sssp g a --direction outgoing
NODE  DISTANCE
a          0.0
b          1.0
c          2.0
```

### Wire

```json
{"graph":"g","type":"graph_create"}
{"graph":"g","node_id":"a","type":"graph_add_node"}
{"graph":"g","node_id":"b","type":"graph_add_node"}
{"graph":"g","node_id":"c","type":"graph_add_node"}
{"dst":"b","edge_type":"knows","graph":"g","src":"a","type":"graph_add_edge"}
{"dst":"c","edge_type":"knows","graph":"g","src":"b","type":"graph_add_edge"}
{"direction":"outgoing","graph":"g","source":"a","type":"graph_sssp"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `as_of` | `integer` | no | Read as of a position on the logical commit timeline — the `timestamp` from `history` output, not the `version`, and never a calendar date. Reads the graph state visible at that timeline position. To read as of a real time, use `as_of_time` instead. |
| `as_of_time` | `integer` | no | Read as of a real time: a wall-clock instant in microseconds since the Unix epoch (UTC), as reported by `committed_at` on a write ack or on any `history` row. Resolves to the commit at or before that instant, and fails rather than guessing if the instant falls outside the branch's recorded history. Mutually exclusive with `as_of`. |
| `budget` | `GraphAnalyticsBudget` | no | Optional snapshot size bounds. Defaults to the engine limits. |
| `direction` | `GraphDirection` | no | Optional traversal direction. Defaults to outgoing. |
| `graph` | `string` | yes | Graph name. |
| `source` | `string` | yes | Source node id. |

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`AnalyticsResult<GraphSsspData>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.product_space`](https://stratadb.org/e/invalid_argument.engine.product_space)
- [`invalid_argument.engine.graph_name`](https://stratadb.org/e/invalid_argument.engine.graph_name)
- [`not_found.engine.graph`](https://stratadb.org/e/not_found.engine.graph)
- [`invalid_argument.engine.graph_node_id`](https://stratadb.org/e/invalid_argument.engine.graph_node_id)
- [`not_found.engine.graph_node`](https://stratadb.org/e/not_found.engine.graph_node)
- [`resource_exhausted.engine.graph_analytics_budget`](https://stratadb.org/e/resource_exhausted.engine.graph_analytics_budget)
- [`invalid_argument.executor.graph_analytics_budget`](https://stratadb.org/e/invalid_argument.executor.graph_analytics_budget)

## Invocation

- CLI: `strata graph sssp`
- Wire type: `graph_sssp`
