---
title: "Batch check vector existence"
description: "Check existence for multiple vector keys."
source: strata-core@1.2.1
section: vector
---

Checks several vector keys in one collection and returns positional boolean status values. The response preserves the input order.

Itemwise batches return one positional item result per input item. The outer batch status summarizes whether all, some, or none of the items succeeded.

## Examples

Check existence for many keys at once.

### CLI

```console
$ strata vector collection create docs 3 --metric cosine
created collection docs (3 dimensions, cosine)
$ strata command run --command-json '{"collection":"docs","entries":[{"key":"a","vector":[1.0,0.0,0.0]}],"type":"vector_batch_upsert"}'
#  STATUS  EFFECT   VECTOR_REVISION
0  ok      created                1
$ strata command run --command-json '{"collection":"docs","keys":["a","missing"],"type":"vector_batch_exists"}'
#  STATUS  EXISTS
0  ok      true
1  ok      false
```

### Wire

```json
{"collection":"docs","dimension":3,"metric":"cosine","type":"vector_create_collection"}
{"collection":"docs","entries":[{"key":"a","vector":[1.0,0.0,0.0]}],"type":"vector_batch_upsert"}
{"collection":"docs","keys":["a","missing"],"type":"vector_batch_exists"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `collection` | `string` | yes | Collection name. |
| `keys` | `string[]` | yes | Vector keys to check. |

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`BatchResult<BatchExistsPresence>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.product_space`](https://stratadb.org/e/invalid_argument.engine.product_space)
- [`invalid_argument.engine.vector_collection`](https://stratadb.org/e/invalid_argument.engine.vector_collection)
- [`invalid_argument.engine.vector_key`](https://stratadb.org/e/invalid_argument.engine.vector_key)
- [`not_found.engine.vector_collection`](https://stratadb.org/e/not_found.engine.vector_collection)

## Invocation

- CLI: via `strata command run` (no dedicated verb)
- Wire type: `vector_batch_exists`
