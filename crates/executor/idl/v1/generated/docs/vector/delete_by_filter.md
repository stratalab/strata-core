---
title: "Delete vectors by filter"
description: "Delete vectors matching a metadata filter."
source: strata-core@1.2.1
section: vector
---

Scans the collection for visible vectors matching the metadata filter and deletes the matching rows as a bulk mutation.

Successful mutations return an acknowledgement of the outcome: for a state-changing write, the affected target with the mutation effect and commit facts; for mutations that produce a domain result (such as a branch or a promotion outcome), that result object.

## Examples

Delete every vector whose metadata matches a filter.

### CLI

```console
$ strata vector collection create docs 3 --metric cosine
created collection docs (3 dimensions, cosine)
$ strata vector upsert docs a '[1.0,0.0,0.0]' --metadata '{"tag":"keep"}'
created a in docs
$ strata vector upsert docs b '[0.0,1.0,0.0]' --metadata '{"tag":"drop"}'
created b in docs
$ strata vector delete-by-filter docs --filter '{"conditions":[{"field":"tag","op":"eq","value":{"type":"string","value":"drop"}}]}'
deleted 1 vector from docs
$ strata vector count docs
1
```

### Wire

```json
{"collection":"docs","dimension":3,"metric":"cosine","type":"vector_create_collection"}
{"collection":"docs","key":"a","metadata":{"tag":"keep"},"type":"vector_upsert","vector":[1.0,0.0,0.0]}
{"collection":"docs","key":"b","metadata":{"tag":"drop"},"type":"vector_upsert","vector":[0.0,1.0,0.0]}
{"collection":"docs","filter":{"conditions":[{"field":"tag","op":"eq","value":{"type":"string","value":"drop"}}]},"type":"vector_delete_by_filter"}
{"collection":"docs","type":"vector_count"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `collection` | `string` | yes | Collection name. |
| `filter` | `VectorMetadataFilter` | yes | Metadata filter. |

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
- [`invalid_argument.engine.vector_filter`](https://stratadb.org/e/invalid_argument.engine.vector_filter)

## Invocation

- CLI: `strata vector delete-by-filter`
- Wire type: `vector_delete_by_filter`
