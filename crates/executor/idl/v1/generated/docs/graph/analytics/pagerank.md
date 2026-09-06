---
title: "Compute graph pagerank"
description: "Compute PageRank importance scores."
source: strata-core@1.2.0
section: graph
---

Computes PageRank over a consistent snapshot. Tunable damping (default 0.85), iteration bound (default 20), and convergence tolerance (default 1e-6); the response reports how many iterations actually ran. Optional personalization seeds steer both teleport and dangling mass toward weighted nodes, and the response flags `personalized: true`. Results are deterministic for a fixed graph state. Accepts an optional snapshot budget and `as_of` for time travel.

Analytics commands compute over a consistent snapshot of the visible graph and return a complete result payload in one response. They accept optional snapshot budgets and an `as_of` timestamp for time travel; results are deterministic for a fixed graph state.

## Examples

Compute PageRank importance scores.

### CLI

```console
$ strata graph create g
$ strata graph add-node g a
$ strata graph add-node g b
$ strata graph add-node g c
$ strata graph add-edge g a knows b
$ strata graph add-edge g b knows c
$ strata graph pagerank g
```

### Wire

```json
{"graph":"g","type":"graph_create"}
{"graph":"g","node_id":"a","type":"graph_add_node"}
{"graph":"g","node_id":"b","type":"graph_add_node"}
{"graph":"g","node_id":"c","type":"graph_add_node"}
{"dst":"b","edge_type":"knows","graph":"g","src":"a","type":"graph_add_edge"}
{"dst":"c","edge_type":"knows","graph":"g","src":"b","type":"graph_add_edge"}
{"graph":"g","type":"graph_pagerank"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `as_of` | `integer` | no | Read as of a position on the logical commit timeline — the `timestamp` from `history` output, not the `version`, and never a calendar date. Reads the graph state visible at that timeline position. To read as of a real time, use `as_of_time` instead. |
| `as_of_time` | `integer` | no | Read as of a real time: a wall-clock instant in microseconds since the Unix epoch (UTC), as reported by `committed_at` on a write ack or on any `history` row. Resolves to the commit at or before that instant, and fails rather than guessing if the instant falls outside the branch's recorded history. Mutually exclusive with `as_of`. |
| `budget` | `GraphAnalyticsBudget` | no | Optional snapshot size bounds. Defaults to the engine limits. |
| `damping` | `number` | no | Optional damping factor. Defaults to 0.85. |
| `graph` | `string` | yes | Graph name. |
| `max_iterations` | `integer` | no | Optional iteration bound. Defaults to 20. |
| `personalization` | `object` | no | Optional seed weights (node id to weight). When present, both teleport and dangling mass follow the seeds. |
| `tolerance` | `number` | no | Optional convergence tolerance. Defaults to 1e-6. |

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`AnalyticsResult<GraphPagerankData>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.product_space`](https://stratadb.org/e/invalid_argument.engine.product_space)
- [`invalid_argument.engine.graph_name`](https://stratadb.org/e/invalid_argument.engine.graph_name)
- [`not_found.engine.graph`](https://stratadb.org/e/not_found.engine.graph)
- [`invalid_argument.engine.graph_pagerank_options`](https://stratadb.org/e/invalid_argument.engine.graph_pagerank_options)
- [`invalid_argument.engine.graph_personalization`](https://stratadb.org/e/invalid_argument.engine.graph_personalization)
- [`resource_exhausted.engine.graph_analytics_budget`](https://stratadb.org/e/resource_exhausted.engine.graph_analytics_budget)
- [`invalid_argument.executor.graph_analytics_budget`](https://stratadb.org/e/invalid_argument.executor.graph_analytics_budget)

## Invocation

- CLI: `strata graph pagerank`
- Wire type: `graph_pagerank`
