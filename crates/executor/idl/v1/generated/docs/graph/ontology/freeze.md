---
title: "Freeze graph ontology"
description: "Freeze the graph ontology."
source: strata-core@1.2.2
section: graph
---

Validates the draft ontology and freezes it. Validation requires at least one declared type and rejects link types whose source or target reference undeclared object types (`failed_precondition.engine.graph_ontology_freeze`). After freezing, writes enforce declared node object types, required properties, and link-type endpoint rules; the ontology itself can no longer change (`failed_precondition.engine.graph_ontology_frozen`).

Successful mutations return an acknowledgement of the outcome: for a state-changing write, the affected target with the mutation effect and commit facts; for mutations that produce a domain result (such as a branch or a promotion outcome), that result object.

## Examples

Freeze the ontology so its types can no longer change.

### CLI

```console
$ strata graph create g
created graph g
$ strata graph ontology define-object-type g person
created object type person in g
$ strata graph ontology freeze g
froze ontology of g (1 object type, 0 link types)
$ strata graph ontology get g
graph         g
status        frozen
version       …
object_types
  NAME    PROPERTIES
  person  -
link_types    -
```

`…` stands for a value that varies by run or by machine — an instant, a version, an id, a path.

### Wire

```json
{"graph":"g","type":"graph_create"}
{"graph":"g","name":"person","type":"graph_define_object_type"}
{"graph":"g","type":"graph_freeze_ontology"}
{"graph":"g","type":"graph_get_ontology"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `graph` | `string` | yes | Graph name. |

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
- [`failed_precondition.engine.graph_ontology_freeze`](https://stratadb.org/e/failed_precondition.engine.graph_ontology_freeze)
- [`failed_precondition.engine.graph_ontology_frozen`](https://stratadb.org/e/failed_precondition.engine.graph_ontology_frozen)

## Invocation

- CLI: `strata graph ontology freeze`
- Wire type: `graph_freeze_ontology`
