---
title: "Sample vectors"
description: "Sample vectors with values and version facts."
source: strata-core@1.2.1
section: vector
---

Returns a deterministic representative sample of vectors from a collection: the total live count and up to `count` entries (default 10), evenly strided over the ordered keys. Latest state only.

Paginated responses use opaque cursors. Clients should pass the returned cursor back to the same command shape and must not parse cursor contents.

## Examples

A representative sample of stored vectors plus the total count.

### CLI

```console
$ strata vector collection create docs 3 --metric cosine
created collection docs (3 dimensions, cosine)
$ strata vector upsert docs a '[1.0,0.0,0.0]'
created a in docs
$ strata vector upsert docs b '[0.0,1.0,0.0]'
created b in docs
$ strata vector sample docs
KEY  VERSION  VECTOR_REVISION  EMBEDDING      METADATA
a          …                1  [1.0,0.0,0.0]  -
b          …                1  [0.0,1.0,0.0]  -
```

`…` stands for a value that varies by run or by machine — an instant, a version, an id, a path.

### Wire

```json
{"collection":"docs","dimension":3,"metric":"cosine","type":"vector_create_collection"}
{"collection":"docs","key":"a","type":"vector_upsert","vector":[1.0,0.0,0.0]}
{"collection":"docs","key":"b","type":"vector_upsert","vector":[0.0,1.0,0.0]}
{"collection":"docs","type":"vector_sample"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `collection` | `string` | yes | Collection name. |
| `count` | `integer` | no | Optional sample count. Defaults to 10. |

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`SamplePage<VectorVersionedData>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.product_space`](https://stratadb.org/e/invalid_argument.engine.product_space)
- [`invalid_argument.engine.vector_collection`](https://stratadb.org/e/invalid_argument.engine.vector_collection)
- [`invalid_argument.engine.vector_key`](https://stratadb.org/e/invalid_argument.engine.vector_key)
- [`not_found.engine.vector_collection`](https://stratadb.org/e/not_found.engine.vector_collection)

## Invocation

- CLI: `strata vector sample`
- Wire type: `vector_sample`
