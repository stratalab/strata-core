---
title: "Define graph object type"
description: "Define a graph object type."
source: strata-core@1.2.2
section: graph
---

Declares an object type in the graph's ontology: a name plus property definitions (`value_type`, `required`). While the ontology is a draft, redefining a type replaces it freely. After `graph.ontology.freeze`, the ontology is immutable and this command fails with `failed_precondition.engine.graph_ontology_frozen`.

Successful mutations return an acknowledgement of the outcome: for a state-changing write, the affected target with the mutation effect and commit facts; for mutations that produce a domain result (such as a branch or a promotion outcome), that result object.

## Examples

Define an object (node) type.

### CLI

```console
$ strata graph create g
created graph g
$ strata graph ontology define-object-type g person
created object type person in g
$ strata graph ontology summary g
graph         g
status        draft
version       …
object_types
  NAME    NODE_COUNT  PROPERTIES
  person           0  -
link_types    -
```

`…` stands for a value that varies by run or by machine — an instant, a version, an id, a path.

### Wire

```json
{"graph":"g","type":"graph_create"}
{"graph":"g","name":"person","type":"graph_define_object_type"}
{"graph":"g","type":"graph_ontology_summary"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `graph` | `string` | yes | Graph name. |
| `name` | `string` | yes | Object type name. |
| `properties` | `object` | no | Declared properties by name. |

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
- [`invalid_argument.engine.graph_type_name_reserved`](https://stratadb.org/e/invalid_argument.engine.graph_type_name_reserved)
- [`invalid_argument.engine.graph_property_name`](https://stratadb.org/e/invalid_argument.engine.graph_property_name)
- [`failed_precondition.engine.graph_ontology_frozen`](https://stratadb.org/e/failed_precondition.engine.graph_ontology_frozen)

## Invocation

- CLI: `strata graph ontology define-object-type`
- Wire type: `graph_define_object_type`
