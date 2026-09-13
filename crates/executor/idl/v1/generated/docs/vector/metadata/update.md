---
title: "Update vector metadata"
description: "Patch metadata for one vector."
source: strata-core@1.2.2
section: vector
---

Applies a top-level metadata patch to one visible vector. Missing vectors return a no-op mutation acknowledgement.

Successful mutations return an acknowledgement of the outcome: for a state-changing write, the affected target with the mutation effect and commit facts; for mutations that produce a domain result (such as a branch or a promotion outcome), that result object.

## Examples

Patch the metadata of an existing vector.

### CLI

```console
$ strata vector collection create docs 3 --metric cosine
created collection docs (3 dimensions, cosine)
$ strata vector upsert docs a '[1.0,0.0,0.0]' --metadata '{"tag":"x"}'
created a in docs
$ strata vector update-metadata docs a '{"tag":"z"}'
updated a in docs
$ strata vector get docs a
key              a
vector_revision  2
version          …
embedding        [1.0,0.0,0.0]
metadata         {"tag":"z"}
```

`…` stands for a value that varies by run or by machine — an instant, a version, an id, a path.

### Wire

```json
{"collection":"docs","dimension":3,"metric":"cosine","type":"vector_create_collection"}
{"collection":"docs","key":"a","metadata":{"tag":"x"},"type":"vector_upsert","vector":[1.0,0.0,0.0]}
{"collection":"docs","key":"a","patch":{"tag":"z"},"type":"vector_update_metadata"}
{"collection":"docs","key":"a","type":"vector_get"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `collection` | `string` | yes | Collection name. |
| `key` | `string` | yes | Vector key. |
| `patch` | `any` | yes | Top-level metadata patch. |

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`MutationAck` — an acknowledgement with no payload.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.product_space`](https://stratadb.org/e/invalid_argument.engine.product_space)
- [`invalid_argument.engine.vector_collection`](https://stratadb.org/e/invalid_argument.engine.vector_collection)
- [`invalid_argument.engine.vector_key`](https://stratadb.org/e/invalid_argument.engine.vector_key)
- [`not_found.engine.vector_collection`](https://stratadb.org/e/not_found.engine.vector_collection)
- [`invalid_argument.engine.vector_metadata_patch`](https://stratadb.org/e/invalid_argument.engine.vector_metadata_patch)

## Invocation

- CLI: `strata vector update-metadata`
- Wire type: `vector_update_metadata`
