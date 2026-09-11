---
title: "Check vector existence"
description: "Check whether one vector key exists."
source: strata-core@1.2.1
section: vector
---

Returns a boolean status for one vector key without loading the embedding.

Status commands return a scalar or compact status payload and do not mutate database state.

## Examples

Check whether a key exists in a collection.

### CLI

```console
$ strata vector collection create docs 3 --metric cosine
$ strata vector upsert docs a [1.0,0.0,0.0]
$ strata vector exists docs a
$ strata vector exists docs absent
```

### Wire

```json
{"collection":"docs","dimension":3,"metric":"cosine","type":"vector_create_collection"}
{"collection":"docs","key":"a","type":"vector_upsert","vector":[1.0,0.0,0.0]}
{"collection":"docs","key":"a","type":"vector_exists"}
{"collection":"docs","key":"absent","type":"vector_exists"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `collection` | `string` | yes | Collection name. |
| `key` | `string` | yes | Vector key. |

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`StatusValue<boolean>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.product_space`](https://stratadb.org/e/invalid_argument.engine.product_space)
- [`invalid_argument.engine.vector_collection`](https://stratadb.org/e/invalid_argument.engine.vector_collection)
- [`invalid_argument.engine.vector_key`](https://stratadb.org/e/invalid_argument.engine.vector_key)
- [`not_found.engine.vector_collection`](https://stratadb.org/e/not_found.engine.vector_collection)

## Invocation

- CLI: `strata vector exists`
- Wire type: `vector_exists`
