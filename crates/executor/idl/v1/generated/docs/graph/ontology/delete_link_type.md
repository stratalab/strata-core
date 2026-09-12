---
title: "Delete graph link type"
description: "Delete a draft link type."
source: strata-core@1.2.1
section: graph
---

Removes a link type from the graph's draft ontology. Deleting a type that was never declared is not an error: the acknowledgement reports `deleted: false`. Once the ontology is frozen this command fails with `failed_precondition.engine.graph_ontology_frozen`.

Successful mutations return an acknowledgement of the outcome: for a state-changing write, the affected target with the mutation effect and commit facts; for mutations that produce a domain result (such as a branch or a promotion outcome), that result object.

## Examples

Remove a link type from the ontology.

### CLI

```console
$ strata graph create g
created graph g
$ strata graph ontology define-object-type g person
created object type person in g
$ strata graph ontology define-link-type g knows person person
created link type knows in g
$ strata graph ontology delete-link-type g knows
deleted link type knows in g
```

### Wire

```json
{"graph":"g","type":"graph_create"}
{"graph":"g","name":"person","type":"graph_define_object_type"}
{"graph":"g","name":"knows","source":"person","target":"person","type":"graph_define_link_type"}
{"graph":"g","name":"knows","type":"graph_delete_link_type"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `graph` | `string` | yes | Graph name. |
| `name` | `string` | yes | Link type name. |

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`MutationAck` — an acknowledgement with no payload.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.product_space`](https://stratadb.org/e/invalid_argument.engine.product_space)
- [`invalid_argument.engine.graph_name`](https://stratadb.org/e/invalid_argument.engine.graph_name)
- [`not_found.engine.graph`](https://stratadb.org/e/not_found.engine.graph)
- [`invalid_argument.engine.graph_type_name`](https://stratadb.org/e/invalid_argument.engine.graph_type_name)
- [`failed_precondition.engine.graph_ontology_frozen`](https://stratadb.org/e/failed_precondition.engine.graph_ontology_frozen)

## Invocation

- CLI: `strata graph ontology delete-link-type`
- Wire type: `graph_delete_link_type`
