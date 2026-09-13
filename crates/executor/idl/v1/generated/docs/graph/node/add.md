---
title: "Add graph node"
description: "Add or replace a graph node."
source: strata-core@1.2.2
section: graph
---

Adds a node to a graph or replaces it if the node id already exists. A node carries optional JSON properties, an optional declared object type (validated once the graph's ontology is frozen), and an optional entity binding that links the node to a row in another primitive. Cross-branch bindings are rejected.

Successful mutations return an acknowledgement of the outcome: for a state-changing write, the affected target with the mutation effect and commit facts; for mutations that produce a domain result (such as a branch or a promotion outcome), that result object.

## Examples

Add a node with an object type and properties.

### CLI

```console
$ strata graph create social
created graph social
$ strata graph add-node social alice --type person --properties '{"age":30}'
created node alice in social
$ strata graph get-node social alice
node_id      alice
graph        social
object_type  person
properties   {"age":30}
version      …
```

`…` stands for a value that varies by run or by machine — an instant, a version, an id, a path.

### Wire

```json
{"graph":"social","type":"graph_create"}
{"graph":"social","node_id":"alice","object_type":"person","properties":{"age":30},"type":"graph_add_node"}
{"graph":"social","node_id":"alice","type":"graph_get_node"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `binding` | `GraphEntityBinding` | no | Optional entity binding. |
| `graph` | `string` | yes | Graph name. |
| `node_id` | `string` | yes | Node id. |
| `object_type` | `string` | no | Optional declared object type (validated once the ontology is frozen). |
| `properties` | `any` | no | Optional node properties. |

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
- [`invalid_argument.engine.graph_properties`](https://stratadb.org/e/invalid_argument.engine.graph_properties)
- [`invalid_argument.engine.graph_properties_too_large`](https://stratadb.org/e/invalid_argument.engine.graph_properties_too_large)
- [`invalid_argument.engine.graph_type_hint`](https://stratadb.org/e/invalid_argument.engine.graph_type_hint)
- [`invalid_argument.engine.graph_binding`](https://stratadb.org/e/invalid_argument.engine.graph_binding)
- [`unsupported.engine.graph_binding_cross_branch`](https://stratadb.org/e/unsupported.engine.graph_binding_cross_branch)
- [`failed_precondition.engine.graph_ontology_node_type`](https://stratadb.org/e/failed_precondition.engine.graph_ontology_node_type)
- [`failed_precondition.engine.graph_ontology_required_property`](https://stratadb.org/e/failed_precondition.engine.graph_ontology_required_property)

## Invocation

- CLI: `strata graph add-node`
- Wire type: `graph_add_node`
