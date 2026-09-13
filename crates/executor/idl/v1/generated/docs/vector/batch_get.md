---
title: "Batch get vectors"
description: "Read multiple vectors by key."
source: strata-core@1.2.2
section: vector
---

Reads several vector keys and returns positional item results. Each item records found or missing state.

Itemwise batches return one positional item result per input item. The outer batch status summarizes whether all, some, or none of the items succeeded.

## Examples

Read many vectors at once.

### CLI

```console
$ strata vector collection create docs 3 --metric cosine
created collection docs (3 dimensions, cosine)
$ strata command run --command-json '{"collection":"docs","entries":[{"key":"a","vector":[1.0,0.0,0.0]},{"key":"b","vector":[0.0,1.0,0.0]}],"type":"vector_batch_upsert"}'
#  STATUS  EFFECT   VECTOR_REVISION
0  ok      created                1
1  ok      created                1
$ strata command run --command-json '{"collection":"docs","keys":["a","b"],"type":"vector_batch_get"}'
#  STATUS  KEY  VERSION  VECTOR_REVISION  EMBEDDING      METADATA
0  ok      a          …                1  [1.0,0.0,0.0]  -
1  ok      b          …                1  [0.0,1.0,0.0]  -
```

`…` stands for a value that varies by run or by machine — an instant, a version, an id, a path.

### Wire

```json
{"collection":"docs","dimension":3,"metric":"cosine","type":"vector_create_collection"}
{"collection":"docs","entries":[{"key":"a","vector":[1.0,0.0,0.0]},{"key":"b","vector":[0.0,1.0,0.0]}],"type":"vector_batch_upsert"}
{"collection":"docs","keys":["a","b"],"type":"vector_batch_get"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `collection` | `string` | yes | Collection name. |
| `keys` | `string[]` | yes | Keys to read. |

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`BatchResult<VectorBatchGetItemResult>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.product_space`](https://stratadb.org/e/invalid_argument.engine.product_space)
- [`invalid_argument.engine.vector_collection`](https://stratadb.org/e/invalid_argument.engine.vector_collection)
- [`invalid_argument.engine.vector_key`](https://stratadb.org/e/invalid_argument.engine.vector_key)
- [`not_found.engine.vector_collection`](https://stratadb.org/e/not_found.engine.vector_collection)
- [`invalid_argument.engine.vector_batch`](https://stratadb.org/e/invalid_argument.engine.vector_batch)

## Invocation

- CLI: via `strata command run` (no dedicated verb)
- Wire type: `vector_batch_get`
