---
title: "Define graph link type"
description: "Define a graph link type."
source: strata-core@1.2.1
section: graph
---

Declares a link type in the graph's ontology: a name, its source and target object types, an optional cardinality hint (for example `many-to-one`), and property definitions. Source and target must name declared object types by the time the ontology is frozen. After freezing, this command fails with `failed_precondition.engine.graph_ontology_frozen`.

Successful mutations return an acknowledgement of the outcome: for a state-changing write, the affected target with the mutation effect and commit facts; for mutations that produce a domain result (such as a branch or a promotion outcome), that result object.

## Examples

Define a link (edge) type between two object types.

### CLI

```console
$ strata graph create g
created graph g
$ strata graph ontology define-object-type g person
created object type person in g
$ strata graph ontology define-link-type g knows person person
created link type knows in g
$ strata graph ontology get g
graph         g
status        draft
version       …
object_types
  NAME    PROPERTIES
  person  -
link_types
  CARDINALITY  NAME   PROPERTIES  SOURCE  TARGET
  -            knows  -           person  person
```

`…` stands for a value that varies by run or by machine — an instant, a version, an id, a path.

### Wire

```json
{"graph":"g","type":"graph_create"}
{"graph":"g","name":"person","type":"graph_define_object_type"}
{"graph":"g","name":"knows","source":"person","target":"person","type":"graph_define_link_type"}
{"graph":"g","type":"graph_get_ontology"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `cardinality` | `string` | no | Optional cardinality: one of `one-to-one`, `one-to-many`, `many-to-one`, or `many-to-many`. Unknown values are rejected. |
| `graph` | `string` | yes | Graph name. |
| `name` | `string` | yes | Link type name. |
| `properties` | `object` | no | Declared properties by name. |
| `source` | `string` | yes | Declared source object type. |
| `target` | `string` | yes | Declared target object type. |

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

- CLI: `strata graph ontology define-link-type`
- Wire type: `graph_define_link_type`
