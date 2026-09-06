---
title: "List graph neighbors"
description: "List a node's neighbors."
source: strata-core@1.2.0
section: graph
---

Walks a node's edges and returns one hit per traversed edge. Direction is `outgoing`, `incoming`, or `both`; an optional edge-type filter restricts the walk. Each hit embeds both the traversed edge and the neighbor node in full, so a follow-up read is rarely needed. A missing node yields an empty page.

Paginated responses use opaque cursors. Clients should pass the returned cursor back to the same command shape and must not parse cursor contents.

## Examples

Find a node's neighbors along outgoing edges.

### CLI

```console
$ strata graph create social
$ strata graph add-node social alice
$ strata graph add-node social bob
$ strata graph add-edge social alice knows bob
$ strata graph neighbors social alice --direction outgoing
```

### Wire

```json
{"graph":"social","type":"graph_create"}
{"graph":"social","node_id":"alice","type":"graph_add_node"}
{"graph":"social","node_id":"bob","type":"graph_add_node"}
{"dst":"bob","edge_type":"knows","graph":"social","src":"alice","type":"graph_add_edge"}
{"direction":"outgoing","graph":"social","node_id":"alice","type":"graph_neighbors"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `as_of` | `integer` | no | Read as of a position on the logical commit timeline — the `timestamp` from `history` output, not the `version`, and never a calendar date. Reads the graph state visible at that timeline position. To read as of a real time, use `as_of_time` instead. |
| `as_of_time` | `integer` | no | Read as of a real time: a wall-clock instant in microseconds since the Unix epoch (UTC), as reported by `committed_at` on a write ack or on any `history` row. Resolves to the commit at or before that instant, and fails rather than guessing if the instant falls outside the branch's recorded history. Mutually exclusive with `as_of`. |
| `cursor` | `string` | no | Optional exclusive cursor. |
| `direction` | `GraphDirection` | yes | Traversal direction. |
| `edge_type` | `string` | no | Optional edge type filter. |
| `graph` | `string` | yes | Graph name. |
| `limit` | `integer` | no | Optional item limit. Defaults to 100. |
| `node_id` | `string` | yes | Node id. |

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`Page<GraphNeighborHit, String>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.product_space`](https://stratadb.org/e/invalid_argument.engine.product_space)
- [`invalid_argument.engine.graph_name`](https://stratadb.org/e/invalid_argument.engine.graph_name)
- [`not_found.engine.graph`](https://stratadb.org/e/not_found.engine.graph)
- [`invalid_argument.engine.graph_node_id`](https://stratadb.org/e/invalid_argument.engine.graph_node_id)
- [`invalid_argument.engine.graph_edge_type`](https://stratadb.org/e/invalid_argument.engine.graph_edge_type)

## Invocation

- CLI: `strata graph neighbors`
- Wire type: `graph_neighbors`
