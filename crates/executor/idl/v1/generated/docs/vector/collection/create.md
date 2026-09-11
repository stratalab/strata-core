---
title: "Create vector collection"
description: "Create a vector collection with a dimension, metric, and optionally the model that produces its vectors."
source: strata-core@1.2.1
section: vector
---

Creates a collection for dense vectors. The dimension and metric become part of the collection contract for future upserts and queries. Passing `embedding_model` records which model produces the collection's vectors: it lets `text` be embedded for upserts and queries, and lets a query embedded with a different model be refused rather than silently compared. A collection created without one can declare it later with `vector.collection.set_embedding_model`.

Successful mutations return an acknowledgement of the outcome: for a state-changing write, the affected target with the mutation effect and commit facts; for mutations that produce a domain result (such as a branch or a promotion outcome), that result object.

## Examples

Create a vector collection, then confirm its dimension.

### CLI

```console
$ strata vector collection create docs 3 --metric cosine
$ strata vector collection stats docs
```

### Wire

```json
{"collection":"docs","dimension":3,"metric":"cosine","type":"vector_create_collection"}
{"collection":"docs","type":"vector_collection_stats"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `collection` | `string` | yes | Collection name. |
| `dimension` | `integer` | yes | Embedding dimension. |
| `embedding_model` | `string` | no | The model that produces this collection's vectors (D9). What the record governs: `text` on `vector upsert` and `vector query` is embedded with this model and no other, so text writes and searches cannot mix models — the failure dimension cannot catch, since two models at the same width return neighbours that are ranked and meaningless. What it cannot govern: a `vector` supplied directly carries no model, so Strata cannot check one against the record; supplying a vector is the caller's statement that this model produced it. |
| `metric` | `VectorDistanceMetric` | yes | Distance metric. |

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`MutationAck<VectorCollectionCreate>`.

**Transitional wire:** the response currently carries a page rather than a mutation acknowledgement; the declaration is the target shape and the wire is scheduled to be normalised.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.product_space`](https://stratadb.org/e/invalid_argument.engine.product_space)
- [`invalid_argument.engine.vector_collection`](https://stratadb.org/e/invalid_argument.engine.vector_collection)
- [`invalid_argument.engine.vector_key`](https://stratadb.org/e/invalid_argument.engine.vector_key)
- [`not_found.engine.vector_collection`](https://stratadb.org/e/not_found.engine.vector_collection)
- [`invalid_argument.engine.vector_dimension`](https://stratadb.org/e/invalid_argument.engine.vector_dimension)
- [`invalid_argument.engine.embedding_model`](https://stratadb.org/e/invalid_argument.engine.embedding_model)
- [`invalid_argument.executor.vector_dimension`](https://stratadb.org/e/invalid_argument.executor.vector_dimension)

## Invocation

- CLI: `strata vector collection create`
- Wire type: `vector_create_collection`
