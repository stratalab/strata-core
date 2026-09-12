---
title: "Batch upsert vectors"
description: "Upsert multiple vectors in one itemwise batch."
source: strata-core@1.2.1
section: vector
---

Writes multiple vector entries and returns positional mutation results. Valid items share commit facts where the engine applies them together.

Itemwise batches return one positional item result per input item. The outer batch status summarizes whether all, some, or none of the items succeeded.

## Examples

Upsert many vectors in one commit.

### CLI

```console
$ strata vector collection create docs 3 --metric cosine
created collection docs (3 dimensions, cosine)
$ strata command run --command-json '{"collection":"docs","entries":[{"key":"a","vector":[1.0,0.0,0.0]},{"key":"b","vector":[0.0,1.0,0.0]}],"type":"vector_batch_upsert"}'
#  STATUS  EFFECT   VECTOR_REVISION
0  ok      created                1
1  ok      created                1
$ strata vector count docs
2
```

### Wire

```json
{"collection":"docs","dimension":3,"metric":"cosine","type":"vector_create_collection"}
{"collection":"docs","entries":[{"key":"a","vector":[1.0,0.0,0.0]},{"key":"b","vector":[0.0,1.0,0.0]}],"type":"vector_batch_upsert"}
{"collection":"docs","type":"vector_count"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `collection` | `string` | yes | Collection name. |
| `entries` | `BatchVectorEntry[]` | yes | Entries to write. |

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`BatchResult<VectorBatchItemResult>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.product_space`](https://stratadb.org/e/invalid_argument.engine.product_space)
- [`invalid_argument.engine.vector_collection`](https://stratadb.org/e/invalid_argument.engine.vector_collection)
- [`invalid_argument.engine.vector_key`](https://stratadb.org/e/invalid_argument.engine.vector_key)
- [`not_found.engine.vector_collection`](https://stratadb.org/e/not_found.engine.vector_collection)
- [`invalid_argument.engine.vector_batch`](https://stratadb.org/e/invalid_argument.engine.vector_batch)
- [`invalid_argument.engine.vector_dimension`](https://stratadb.org/e/invalid_argument.engine.vector_dimension)
- [`invalid_argument.engine.vector_embedding`](https://stratadb.org/e/invalid_argument.engine.vector_embedding)
- [`invalid_argument.executor.vector_dimension`](https://stratadb.org/e/invalid_argument.executor.vector_dimension)
- [`invalid_argument.executor.vector_batch_duplicate_key`](https://stratadb.org/e/invalid_argument.executor.vector_batch_duplicate_key)

## Invocation

- CLI: via `strata command run` (no dedicated verb)
- Wire type: `vector_batch_upsert`
