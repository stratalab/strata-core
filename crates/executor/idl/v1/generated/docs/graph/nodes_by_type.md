---
title: "List graph nodes by type"
description: "List nodes declaring an object type."
source: strata-core@1.2.0
section: graph
---

Lists the nodes that declare a given object type, in node-id order. The type index is maintained from each node's declared `object_type`, so this works whether the ontology is draft or frozen. Accepts a limit, an exclusive cursor, and `as_of` for time travel.

Paginated responses use opaque cursors. Clients should pass the returned cursor back to the same command shape and must not parse cursor contents.

## Examples

List nodes of a given object type.

### CLI

```console
$ strata graph create g
$ strata graph add-node g a --type person
$ strata graph add-node g b --type person
$ strata graph nodes-by-type g person
```

### Wire

```json
{"graph":"g","type":"graph_create"}
{"graph":"g","node_id":"a","object_type":"person","type":"graph_add_node"}
{"graph":"g","node_id":"b","object_type":"person","type":"graph_add_node"}
{"graph":"g","object_type":"person","type":"graph_nodes_by_type"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `as_of` | `integer` | no | Read as of a position on the logical commit timeline — the `timestamp` from `history` output, not the `version`, and never a calendar date. Reads the graph state visible at that timeline position. To read as of a real time, use `as_of_time` instead. |
| `as_of_time` | `integer` | no | Read as of a real time: a wall-clock instant in microseconds since the Unix epoch (UTC), as reported by `committed_at` on a write ack or on any `history` row. Resolves to the commit at or before that instant, and fails rather than guessing if the instant falls outside the branch's recorded history. Mutually exclusive with `as_of`. |
| `cursor` | `string` | no | Optional exclusive node id cursor. |
| `graph` | `string` | yes | Graph name. |
| `limit` | `integer` | no | Optional item limit. Defaults to 100. |
| `object_type` | `string` | yes | Object type name. |

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`Page<GraphNodeDataOutput, String>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.product_space`](https://stratadb.org/e/invalid_argument.engine.product_space)
- [`invalid_argument.engine.graph_name`](https://stratadb.org/e/invalid_argument.engine.graph_name)
- [`not_found.engine.graph`](https://stratadb.org/e/not_found.engine.graph)
- [`invalid_argument.engine.graph_type_name`](https://stratadb.org/e/invalid_argument.engine.graph_type_name)
- [`invalid_argument.engine.graph_node_id`](https://stratadb.org/e/invalid_argument.engine.graph_node_id)

## Invocation

- CLI: `strata graph nodes-by-type`
- Wire type: `graph_nodes_by_type`
