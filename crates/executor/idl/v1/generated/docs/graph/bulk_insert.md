---
title: "Bulk insert graph data"
description: "Bulk-load nodes and edges in chunks."
source: strata-core@1.2.1
section: graph
---

Ingests a payload of nodes and edges in chunked commits: nodes first, then edges, so edges may reference nodes from the same payload. Node objects use the key `node_id`; edges use `src`, `edge_type`, `dst`, and optional `weight` (default 1.0) and `properties`. `chunk_size` bounds items per commit (default 512, clamped at 800). The acknowledgement reports inserted counts, the number of chunk commits, and the final chunk's commit receipt.

Successful mutations return an acknowledgement of the outcome: for a state-changing write, the affected target with the mutation effect and commit facts; for mutations that produce a domain result (such as a branch or a promotion outcome), that result object.

## Examples

Insert many nodes and edges in one commit.

### CLI

```console
$ strata graph create g
created graph g
$ strata command run --command-json '{"edges":[{"dst":"b","edge_type":"knows","src":"a"}],"graph":"g","nodes":[{"node_id":"a","object_type":"person"},{"node_id":"b","object_type":"person"}],"type":"graph_bulk_insert"}'
inserted 2 nodes, 1 edge into g
$ strata graph meta g
graph            g
node_count       2
edge_count       1
created_version  3
updated_version  5
```

### Wire

```json
{"graph":"g","type":"graph_create"}
{"edges":[{"dst":"b","edge_type":"knows","src":"a"}],"graph":"g","nodes":[{"node_id":"a","object_type":"person"},{"node_id":"b","object_type":"person"}],"type":"graph_bulk_insert"}
{"graph":"g","type":"graph_get_meta"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `chunk_size` | `integer` | no | Optional items-per-commit chunk size. Defaults to 512; values above 800 clamp so one chunk fits one storage commit. |
| `edges` | `GraphBulkEdge[]` | no | Edges to upsert; endpoints must exist or arrive in `nodes`. |
| `graph` | `string` | yes | Graph name. |
| `nodes` | `GraphBulkNode[]` | no | Nodes to upsert (committed before edges). |

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`MutationAck` — an acknowledgement with no payload.

**Transitional wire:** the response currently carries a bare record rather than a mutation acknowledgement; the declaration is the target shape and the wire is scheduled to be normalised.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.product_space`](https://stratadb.org/e/invalid_argument.engine.product_space)
- [`invalid_argument.engine.graph_name`](https://stratadb.org/e/invalid_argument.engine.graph_name)
- [`not_found.engine.graph`](https://stratadb.org/e/not_found.engine.graph)
- [`invalid_argument.engine.graph_node_id`](https://stratadb.org/e/invalid_argument.engine.graph_node_id)
- [`invalid_argument.engine.graph_edge_type`](https://stratadb.org/e/invalid_argument.engine.graph_edge_type)
- [`invalid_argument.engine.graph_edge_weight`](https://stratadb.org/e/invalid_argument.engine.graph_edge_weight)
- [`invalid_argument.engine.graph_edge_endpoint`](https://stratadb.org/e/invalid_argument.engine.graph_edge_endpoint)
- [`invalid_argument.engine.graph_properties`](https://stratadb.org/e/invalid_argument.engine.graph_properties)
- [`invalid_argument.engine.graph_properties_too_large`](https://stratadb.org/e/invalid_argument.engine.graph_properties_too_large)
- [`failed_precondition.engine.graph_negative_weight`](https://stratadb.org/e/failed_precondition.engine.graph_negative_weight)
- [`failed_precondition.engine.graph_ontology_node_type`](https://stratadb.org/e/failed_precondition.engine.graph_ontology_node_type)
- [`failed_precondition.engine.graph_ontology_edge_type`](https://stratadb.org/e/failed_precondition.engine.graph_ontology_edge_type)

## Invocation

- CLI: `strata graph bulk-insert`
- Wire type: `graph_bulk_insert`
