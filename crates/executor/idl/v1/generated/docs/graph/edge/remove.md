---
title: "Remove graph edge"
description: "Remove a graph edge."
source: strata-core@1.2.2
section: graph
---

Removes one directed edge by its `(src, edge_type, dst)` triple. The endpoints are untouched. Removing an edge that does not exist is not an error: the acknowledgement reports `deleted: false`.

Successful mutations return an acknowledgement of the outcome: for a state-changing write, the affected target with the mutation effect and commit facts; for mutations that produce a domain result (such as a branch or a promotion outcome), that result object.

## Examples

Remove an edge.

### CLI

```console
$ strata graph create social
created graph social
$ strata graph add-node social alice
created node alice in social
$ strata graph add-node social bob
created node bob in social
$ strata graph add-edge social alice knows bob
created edge alice -[knows]-> bob in social
$ strata graph remove-edge social alice knows bob
deleted edge alice -[knows]-> bob in social
$ strata graph get-edge social alice knows bob
(nil)
```

### Wire

```json
{"graph":"social","type":"graph_create"}
{"graph":"social","node_id":"alice","type":"graph_add_node"}
{"graph":"social","node_id":"bob","type":"graph_add_node"}
{"dst":"bob","edge_type":"knows","graph":"social","src":"alice","type":"graph_add_edge"}
{"dst":"bob","edge_type":"knows","graph":"social","src":"alice","type":"graph_remove_edge"}
{"dst":"bob","edge_type":"knows","graph":"social","src":"alice","type":"graph_get_edge"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `dst` | `string` | yes | Destination node id. |
| `edge_type` | `string` | yes | Edge type. |
| `graph` | `string` | yes | Graph name. |
| `src` | `string` | yes | Source node id. |

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`MutationAck` — an acknowledgement with no payload.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.product_space`](https://stratadb.org/e/invalid_argument.engine.product_space)
- [`invalid_argument.engine.graph_name`](https://stratadb.org/e/invalid_argument.engine.graph_name)
- [`not_found.engine.graph`](https://stratadb.org/e/not_found.engine.graph)
- [`invalid_argument.engine.graph_node_id`](https://stratadb.org/e/invalid_argument.engine.graph_node_id)
- [`invalid_argument.engine.graph_edge_type`](https://stratadb.org/e/invalid_argument.engine.graph_edge_type)

## Invocation

- CLI: `strata graph remove-edge`
- Wire type: `graph_remove_edge`
