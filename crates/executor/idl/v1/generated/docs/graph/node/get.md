---
title: "Get graph node"
description: "Read one graph node."
source: strata-core@1.2.0
section: graph
---

Reads one node by id, returning its properties, declared object type, entity binding, and last-write commit coordinates. A removed or never-written node reads back as no data. Accepts `as_of` for time travel.

Optional reads distinguish present data from missing data. When version or timestamp facts exist on the executor output, SDK mappings should preserve them.

## Examples

Read a node's properties, or nothing if absent.

### CLI

```console
$ strata graph create social
$ strata graph add-node social alice --type person --properties {"age":30}
$ strata graph get-node social alice
$ strata graph get-node social absent
```

### Wire

```json
{"graph":"social","type":"graph_create"}
{"graph":"social","node_id":"alice","object_type":"person","properties":{"age":30},"type":"graph_add_node"}
{"graph":"social","node_id":"alice","type":"graph_get_node"}
{"graph":"social","node_id":"absent","type":"graph_get_node"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `as_of` | `integer` | no | Read as of a position on the logical commit timeline — the `timestamp` from `history` output, not the `version`, and never a calendar date. Reads the graph state visible at that timeline position. To read as of a real time, use `as_of_time` instead. |
| `as_of_time` | `integer` | no | Read as of a real time: a wall-clock instant in microseconds since the Unix epoch (UTC), as reported by `committed_at` on a write ack or on any `history` row. Resolves to the commit at or before that instant, and fails rather than guessing if the instant falls outside the branch's recorded history. Mutually exclusive with `as_of`. |
| `graph` | `string` | yes | Graph name. |
| `node_id` | `string` | yes | Node id. |

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`Maybe<GraphNodeDataOutput>` — a miss returns nothing rather than raising.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.product_space`](https://stratadb.org/e/invalid_argument.engine.product_space)
- [`invalid_argument.engine.graph_name`](https://stratadb.org/e/invalid_argument.engine.graph_name)
- [`not_found.engine.graph`](https://stratadb.org/e/not_found.engine.graph)
- [`invalid_argument.engine.graph_node_id`](https://stratadb.org/e/invalid_argument.engine.graph_node_id)

## Invocation

- CLI: `strata graph get-node`
- Wire type: `graph_get_node`
