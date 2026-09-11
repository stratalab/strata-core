---
title: "Apply graph delete policy"
description: "Apply a delete policy to bound graph facts."
source: strata-core@1.2.1
section: graph
---

Applies an explicit policy to every graph node bound to the given entity target: `cascade` deletes the bound nodes and their incident edges, `detach` keeps the nodes but removes their bindings, and `keep_dangling` preserves the bindings so traversal can report the target's status. The acknowledgement reports how many bound nodes the policy covered.

Successful mutations return an acknowledgement of the outcome: for a state-changing write, the affected target with the mutation effect and commit facts; for mutations that produce a domain result (such as a branch or a promotion outcome), that result object.

## Examples

Cascade-delete graph facts bound to an entity.

### CLI

```console
$ strata graph create kb
$ strata command run --command-json '{"binding":{"target":{"key":"user:1","primitive":"kv","space":"default"}},"graph":"kb","node_id":"ada","type":"graph_add_node"}'
$ strata command run --command-json '{"policy":"cascade","target":{"key":"user:1","primitive":"kv","space":"default"},"type":"graph_apply_delete_policy"}'  # cascade removes the bound node and its incident edges.
$ strata graph get-node kb ada
```

### Wire

```json
{"graph":"kb","type":"graph_create"}
{"binding":{"target":{"key":"user:1","primitive":"kv","space":"default"}},"graph":"kb","node_id":"ada","type":"graph_add_node"}
{"policy":"cascade","target":{"key":"user:1","primitive":"kv","space":"default"},"type":"graph_apply_delete_policy"}
{"graph":"kb","node_id":"ada","type":"graph_get_node"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `policy` | `GraphDeletePolicy` | yes | Policy to apply: `cascade`, `detach`, or `keep_dangling`. |
| `target` | `GraphBindingTarget` | yes | The bound entity target. |

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`MutationAck` — an acknowledgement with no payload.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.product_space`](https://stratadb.org/e/invalid_argument.engine.product_space)
- [`invalid_argument.engine.graph_binding`](https://stratadb.org/e/invalid_argument.engine.graph_binding)

## Invocation

- CLI: via `strata command run` (no dedicated verb)
- Wire type: `graph_apply_delete_policy`
