---
title: "Create graph"
description: "Create a named graph."
source: strata-core@1.2.1
section: graph
---

Creates an empty named graph in the selected space and returns its metadata, including node and edge counts (zero at creation) and the create commit coordinates. A database can hold many graphs; graph names are unique per branch and space. Creating a name that already exists fails with `already_exists.engine.graph`.

Successful mutations return an acknowledgement of the outcome: for a state-changing write, the affected target with the mutation effect and commit facts; for mutations that produce a domain result (such as a branch or a promotion outcome), that result object.

## Examples

Create a named graph.

### CLI

```console
$ strata graph create social
created graph social
$ strata graph list
social
```

### Wire

```json
{"graph":"social","type":"graph_create"}
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
- [`already_exists.engine.graph`](https://stratadb.org/e/already_exists.engine.graph)
- [`invalid_argument.engine.graph_name_reserved`](https://stratadb.org/e/invalid_argument.engine.graph_name_reserved)

## Invocation

- CLI: `strata graph create`
- Wire type: `graph_create`
