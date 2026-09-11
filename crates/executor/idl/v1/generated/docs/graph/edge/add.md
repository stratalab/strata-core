---
title: "Add graph edge"
description: "Add or replace a graph edge."
source: strata-core@1.2.1
section: graph
---

Adds a directed edge `src -[edge_type]-> dst` or replaces it if the same triple already exists. Both endpoints must already exist; writing an edge to a missing node fails with `invalid_argument.engine.graph_edge_endpoint`. Weight defaults to 1.0 and must not be negative. Once the graph's ontology is frozen, the edge type and its endpoint object types are validated against the declared link types.

Successful mutations return an acknowledgement of the outcome: for a state-changing write, the affected target with the mutation effect and commit facts; for mutations that produce a domain result (such as a branch or a promotion outcome), that result object.

## Examples

Add a typed edge between two nodes.

### CLI

```console
$ strata graph create social
$ strata graph add-node social alice
$ strata graph add-node social bob
$ strata graph add-edge social alice knows bob
$ strata graph get-edge social alice knows bob
```

### Wire

```json
{"graph":"social","type":"graph_create"}
{"graph":"social","node_id":"alice","type":"graph_add_node"}
{"graph":"social","node_id":"bob","type":"graph_add_node"}
{"dst":"bob","edge_type":"knows","graph":"social","src":"alice","type":"graph_add_edge"}
{"dst":"bob","edge_type":"knows","graph":"social","src":"alice","type":"graph_get_edge"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `dst` | `string` | yes | Destination node id. |
| `edge_type` | `string` | yes | Edge type. |
| `graph` | `string` | yes | Graph name. |
| `properties` | `any` | no | Optional edge properties. |
| `src` | `string` | yes | Source node id. |
| `weight` | `number` | no | Optional edge weight. Defaults to 1.0. |

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
- [`invalid_argument.engine.graph_edge_type_reserved`](https://stratadb.org/e/invalid_argument.engine.graph_edge_type_reserved)
- [`invalid_argument.engine.graph_edge_weight`](https://stratadb.org/e/invalid_argument.engine.graph_edge_weight)
- [`invalid_argument.engine.graph_edge_endpoint`](https://stratadb.org/e/invalid_argument.engine.graph_edge_endpoint)
- [`invalid_argument.engine.graph_properties`](https://stratadb.org/e/invalid_argument.engine.graph_properties)
- [`invalid_argument.engine.graph_properties_too_large`](https://stratadb.org/e/invalid_argument.engine.graph_properties_too_large)
- [`failed_precondition.engine.graph_negative_weight`](https://stratadb.org/e/failed_precondition.engine.graph_negative_weight)
- [`failed_precondition.engine.graph_ontology_edge_type`](https://stratadb.org/e/failed_precondition.engine.graph_ontology_edge_type)
- [`failed_precondition.engine.graph_ontology_endpoint_type`](https://stratadb.org/e/failed_precondition.engine.graph_ontology_endpoint_type)

## Invocation

- CLI: `strata graph add-edge`
- Wire type: `graph_add_edge`
