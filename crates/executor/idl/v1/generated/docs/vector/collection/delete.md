---
title: "Delete vector collection"
description: "Delete a vector collection."
source: strata-core@1.2.1
section: vector
---

Deletes the selected vector collection from the current branch and space. The current wire response is a transitional boolean status.

Successful mutations return an acknowledgement of the outcome: for a state-changing write, the affected target with the mutation effect and commit facts; for mutations that produce a domain result (such as a branch or a promotion outcome), that result object.

## Examples

Delete a collection.

### CLI

```console
$ strata vector collection create temp 3 --metric cosine
$ strata vector collection delete temp
$ strata vector collection list
```

### Wire

```json
{"collection":"temp","dimension":3,"metric":"cosine","type":"vector_create_collection"}
{"collection":"temp","type":"vector_delete_collection"}
{"type":"vector_list_collections"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `collection` | `string` | yes | Collection name. |

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`MutationAck<VectorCollectionDelete>`.

**Transitional wire:** the response currently carries a bare `boolean` rather than a mutation acknowledgement; the declaration is the target shape and the wire is scheduled to be normalised.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.product_space`](https://stratadb.org/e/invalid_argument.engine.product_space)
- [`invalid_argument.engine.vector_collection`](https://stratadb.org/e/invalid_argument.engine.vector_collection)
- [`invalid_argument.engine.vector_key`](https://stratadb.org/e/invalid_argument.engine.vector_key)
- [`not_found.engine.vector_collection`](https://stratadb.org/e/not_found.engine.vector_collection)

## Invocation

- CLI: `strata vector collection delete`
- Wire type: `vector_delete_collection`
