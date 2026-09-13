---
title: "Delete graph"
description: "Delete a graph and its visible data."
source: strata-core@1.2.2
section: graph
---

Deletes a named graph and every visible node, edge, binding, and ontology row it owns. Deleting a graph that does not exist is not an error: the acknowledgement reports `deleted: false` with a `not_found` effect. Earlier states remain readable through time travel on other commands.

Successful mutations return an acknowledgement of the outcome: for a state-changing write, the affected target with the mutation effect and commit facts; for mutations that produce a domain result (such as a branch or a promotion outcome), that result object.

## Examples

Delete a graph.

### CLI

```console
$ strata graph create temp
created graph temp
$ strata graph delete temp
deleted graph temp
$ strata graph list
(empty)
```

### Wire

```json
{"graph":"temp","type":"graph_create"}
{"graph":"temp","type":"graph_delete"}
{"type":"graph_list"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `graph` | `string` | yes | Graph name. |

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`MutationAck` — an acknowledgement with no payload.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.product_space`](https://stratadb.org/e/invalid_argument.engine.product_space)
- [`invalid_argument.engine.graph_name`](https://stratadb.org/e/invalid_argument.engine.graph_name)
- [`not_found.engine.graph`](https://stratadb.org/e/not_found.engine.graph)

## Invocation

- CLI: `strata graph delete`
- Wire type: `graph_delete`
