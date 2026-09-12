---
title: "Sample graph nodes"
description: "Sample graph nodes."
source: strata-core@1.2.1
section: graph
---

Returns a deterministic representative sample of nodes from a graph: the total live node count and up to `count` nodes (default 10), evenly strided over the ordered node ids. Latest state only.

Paginated responses use opaque cursors. Clients should pass the returned cursor back to the same command shape and must not parse cursor contents.

## Examples

A representative sample of nodes plus the total count.

### CLI

```console
$ strata graph create g
created graph g
$ strata graph add-node g a
created node a in g
$ strata graph add-node g b
created node b in g
$ strata graph sample g
NODE_ID  OBJECT_TYPE  VERSION
a        -                  …
b        -                  …
```

`…` stands for a value that varies by run or by machine — an instant, a version, an id, a path.

### Wire

```json
{"graph":"g","type":"graph_create"}
{"graph":"g","node_id":"a","type":"graph_add_node"}
{"graph":"g","node_id":"b","type":"graph_add_node"}
{"graph":"g","type":"graph_sample"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `count` | `integer` | no | Optional sample count. Defaults to 10. |
| `graph` | `string` | yes | Graph name. |

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`SamplePage<GraphNodeDataOutput>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.product_space`](https://stratadb.org/e/invalid_argument.engine.product_space)
- [`invalid_argument.engine.graph_name`](https://stratadb.org/e/invalid_argument.engine.graph_name)
- [`not_found.engine.graph`](https://stratadb.org/e/not_found.engine.graph)

## Invocation

- CLI: `strata graph sample`
- Wire type: `graph_sample`
