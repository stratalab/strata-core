---
title: "Remove graph node"
description: "Remove a graph node and its edges."
source: strata-core@1.2.1
section: graph
---

Removes a node and every edge incident to it in one commit. Removing a node that does not exist is not an error: the acknowledgement reports `deleted: false`.

Successful mutations return an acknowledgement of the outcome: for a state-changing write, the affected target with the mutation effect and commit facts; for mutations that produce a domain result (such as a branch or a promotion outcome), that result object.

## Examples

Remove a node.

### CLI

```console
$ strata graph create social
$ strata graph add-node social alice
$ strata graph remove-node social alice
$ strata graph get-node social alice
```

### Wire

```json
{"graph":"social","type":"graph_create"}
{"graph":"social","node_id":"alice","type":"graph_add_node"}
{"graph":"social","node_id":"alice","type":"graph_remove_node"}
{"graph":"social","node_id":"alice","type":"graph_get_node"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `graph` | `string` | yes | Graph name. |
| `node_id` | `string` | yes | Node id. |

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

## Invocation

- CLI: `strata graph remove-node`
- Wire type: `graph_remove_node`
