---
title: "Traverse graph breadth-first"
description: "Run a bounded breadth-first traversal."
source: strata-core@1.2.0
section: graph
---

Runs a breadth-first traversal from a start node over a consistent snapshot, bounded by `max_depth` (default 100) and `max_nodes` (default 10000). Returns visited node ids in traversal order, a depth per node, and the tree edges in discovery order. Direction defaults to `outgoing`; an optional edge-type list restricts every hop. The start node must exist (`not_found.engine.graph_node`).

Analytics commands compute over a consistent snapshot of the visible graph and return a complete result payload in one response. They accept optional snapshot budgets and an `as_of` timestamp for time travel; results are deterministic for a fixed graph state.

## Examples

Breadth-first traversal from a start node.

### CLI

```console
$ strata graph create g
$ strata graph add-node g a
$ strata graph add-node g b
$ strata graph add-node g c
$ strata graph add-edge g a knows b
$ strata graph add-edge g b knows c
$ strata graph bfs g a
```

### Wire

```json
{"graph":"g","type":"graph_create"}
{"graph":"g","node_id":"a","type":"graph_add_node"}
{"graph":"g","node_id":"b","type":"graph_add_node"}
{"graph":"g","node_id":"c","type":"graph_add_node"}
{"dst":"b","edge_type":"knows","graph":"g","src":"a","type":"graph_add_edge"}
{"dst":"c","edge_type":"knows","graph":"g","src":"b","type":"graph_add_edge"}
{"graph":"g","start":"a","type":"graph_bfs"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `as_of` | `integer` | no | Read as of a position on the logical commit timeline — the `timestamp` from `history` output, not the `version`, and never a calendar date. Reads the graph state visible at that timeline position. To read as of a real time, use `as_of_time` instead. |
| `as_of_time` | `integer` | no | Read as of a real time: a wall-clock instant in microseconds since the Unix epoch (UTC), as reported by `committed_at` on a write ack or on any `history` row. Resolves to the commit at or before that instant, and fails rather than guessing if the instant falls outside the branch's recorded history. Mutually exclusive with `as_of`. |
| `budget` | `GraphAnalyticsBudget` | no | Optional snapshot size bounds. Defaults to the engine limits. |
| `direction` | `GraphDirection` | no | Optional traversal direction. Defaults to outgoing. |
| `edge_types` | `string[]` | no | Optional edge-type restriction applied at every hop. |
| `graph` | `string` | yes | Graph name. |
| `max_depth` | `integer` | no | Optional depth bound. Defaults to 100. |
| `max_nodes` | `integer` | no | Optional visited-node bound. Defaults to 10000. |
| `start` | `string` | yes | Start node id. |

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`AnalyticsResult<GraphBfsData>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.product_space`](https://stratadb.org/e/invalid_argument.engine.product_space)
- [`invalid_argument.engine.graph_name`](https://stratadb.org/e/invalid_argument.engine.graph_name)
- [`not_found.engine.graph`](https://stratadb.org/e/not_found.engine.graph)
- [`invalid_argument.engine.graph_node_id`](https://stratadb.org/e/invalid_argument.engine.graph_node_id)
- [`invalid_argument.engine.graph_edge_type`](https://stratadb.org/e/invalid_argument.engine.graph_edge_type)
- [`not_found.engine.graph_node`](https://stratadb.org/e/not_found.engine.graph_node)
- [`resource_exhausted.engine.graph_analytics_budget`](https://stratadb.org/e/resource_exhausted.engine.graph_analytics_budget)
- [`invalid_argument.executor.graph_analytics_budget`](https://stratadb.org/e/invalid_argument.executor.graph_analytics_budget)

## Invocation

- CLI: `strata graph bfs`
- Wire type: `graph_bfs`
