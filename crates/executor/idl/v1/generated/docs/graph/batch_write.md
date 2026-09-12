---
title: "Batch write graph"
description: "Apply graph mutations atomically."
source: strata-core@1.2.1
section: graph
---

Applies a list of graph operations - `upsert_node`, `delete_node`, `upsert_edge`, `delete_edge` - in one engine commit. Validation failures (bad ids, missing edge endpoints, frozen-ontology violations) reject the whole batch; nothing is partially applied. The response reports one positional item result per operation, all sharing the same commit receipt.

This whole-batch atomicity is deliberate, and differs from the itemwise `kv`, `json`, and `event` batch writes (where the valid items of a mixed batch still commit and the invalid one carries a typed per-item error). A graph batch mixes node and edge upserts, and an edge references its endpoint nodes, so applying only the valid items could leave an edge pointing at a node that never landed. Rejecting the whole batch keeps graph references consistent - referential integrity is the reason the graph channel is atomic where its siblings are itemwise.

Atomic batches validate every operation up front and apply all of them in one engine commit, or none at all. The response still reports one positional item result per operation; all item results share the same commit receipt.

## Examples

Apply several node and edge mutations in one atomic commit.

### CLI

```console
$ strata graph create g
created graph g
$ strata command run --command-json '{"graph":"g","operations":[{"data":{"object_type":"person"},"node_id":"a","type":"upsert_node"},{"data":{"object_type":"person"},"node_id":"b","type":"upsert_node"},{"data":{},"dst":"b","edge_type":"knows","src":"a","type":"upsert_edge"}],"type":"graph_batch_write"}'  # All operations land in one engine commit, or none do.
#  STATUS  EFFECT   OPERATION    CREATED
0  ok      created  upsert_node  true
1  ok      created  upsert_node  true
2  ok      created  upsert_edge  true
$ strata graph meta g
graph            g
node_count       2
edge_count       1
created_version  3
updated_version  4
```

### Wire

```json
{"graph":"g","type":"graph_create"}
{"graph":"g","operations":[{"data":{"object_type":"person"},"node_id":"a","type":"upsert_node"},{"data":{"object_type":"person"},"node_id":"b","type":"upsert_node"},{"data":{},"dst":"b","edge_type":"knows","src":"a","type":"upsert_edge"}],"type":"graph_batch_write"}
{"graph":"g","type":"graph_get_meta"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `graph` | `string` | yes | Graph name. |
| `operations` | `GraphBatchOperation[]` | yes | Batch operations. |

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`BatchResult<GraphBatchItemResult>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.product_space`](https://stratadb.org/e/invalid_argument.engine.product_space)
- [`invalid_argument.engine.graph_name`](https://stratadb.org/e/invalid_argument.engine.graph_name)
- [`not_found.engine.graph`](https://stratadb.org/e/not_found.engine.graph)
- [`invalid_argument.engine.graph_batch`](https://stratadb.org/e/invalid_argument.engine.graph_batch)
- [`invalid_argument.engine.graph_node_id`](https://stratadb.org/e/invalid_argument.engine.graph_node_id)
- [`invalid_argument.engine.graph_edge_type`](https://stratadb.org/e/invalid_argument.engine.graph_edge_type)
- [`invalid_argument.engine.graph_edge_endpoint`](https://stratadb.org/e/invalid_argument.engine.graph_edge_endpoint)
- [`failed_precondition.engine.graph_ontology_node_type`](https://stratadb.org/e/failed_precondition.engine.graph_ontology_node_type)
- [`failed_precondition.engine.graph_ontology_edge_type`](https://stratadb.org/e/failed_precondition.engine.graph_ontology_edge_type)

## Invocation

- CLI: via `strata command run` (no dedicated verb)
- Wire type: `graph_batch_write`
