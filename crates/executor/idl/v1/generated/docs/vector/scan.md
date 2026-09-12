---
title: "Scan vectors"
description: "Scan vectors with values and version facts."
source: strata-core@1.2.1
section: vector
---

Scans visible vectors in a collection starting at an optional key. Each item includes the key, embedding, metadata, and commit metadata exposed by the executor output.

Paginated responses use opaque cursors. Clients should pass the returned cursor back to the same command shape and must not parse cursor contents.

## Examples

Scan stored vectors from the start, in key order.

### CLI

```console
$ strata vector collection create docs 3 --metric cosine
created collection docs (3 dimensions, cosine)
$ strata vector upsert docs a [1.0,0.0,0.0]
created a in docs
$ strata vector upsert docs b [0.0,1.0,0.0]
created b in docs
$ strata vector scan docs
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
{"collection":"docs","type":"vector_scan"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `collection` | `string` | yes | Collection name. |
| `limit` | `integer` | no | Optional row limit. |
| `start` | `string` | no | Optional inclusive start key. |

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`Page<VectorVersionedData>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.product_space`](https://stratadb.org/e/invalid_argument.engine.product_space)
- [`invalid_argument.engine.vector_collection`](https://stratadb.org/e/invalid_argument.engine.vector_collection)
- [`invalid_argument.engine.vector_key`](https://stratadb.org/e/invalid_argument.engine.vector_key)
- [`not_found.engine.vector_collection`](https://stratadb.org/e/not_found.engine.vector_collection)

## Invocation

- CLI: `strata vector scan`
- Wire type: `vector_scan`
