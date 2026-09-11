---
title: "Read vector collection stats"
description: "Read facts for one vector collection."
source: strata-core@1.2.1
section: vector
---

Reads collection-level facts for one vector collection. The current wire response uses the collection-list output with one item.

Status commands return a scalar or compact status payload and do not mutate database state.

## Examples

Read a collection's dimension, metric, and size.

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

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`StatusResponse<VectorCollectionInfo>`.

**Transitional wire:** the response currently carries a page rather than a bare record; the declaration is the target shape and the wire is scheduled to be normalised.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.product_space`](https://stratadb.org/e/invalid_argument.engine.product_space)
- [`invalid_argument.engine.vector_collection`](https://stratadb.org/e/invalid_argument.engine.vector_collection)
- [`invalid_argument.engine.vector_key`](https://stratadb.org/e/invalid_argument.engine.vector_key)
- [`not_found.engine.vector_collection`](https://stratadb.org/e/not_found.engine.vector_collection)

## Invocation

- CLI: `strata vector collection stats`
- Wire type: `vector_collection_stats`
