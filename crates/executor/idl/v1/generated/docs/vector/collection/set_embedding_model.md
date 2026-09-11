---
title: "Declare vector collection embedding model"
description: "Declare the embedding model a vector collection's vectors come from."
source: strata-core@1.2.1
section: vector
---

Records, once, which embedding model produces this collection's vectors. A declaration, not a verification: a stored vector carries no model, so this takes the caller's word for the vectors present; from then on `text` on upsert and query is embedded with this model and no other, and a collection with no recorded model cannot embed `text` at all. A vector supplied directly is not checked against the record — it cannot be — so supplying one remains the caller's statement that the recorded model produced it. Declaring the model a collection already records is a no-op and commits nothing. Declaring a different one is refused with `failed_precondition.engine.embedding_model_mismatch`: the stored vectors came from the recorded model, so create a separate collection for the other one. The current wire response uses the collection-list output with one item.

Successful mutations return an acknowledgement of the outcome: for a state-changing write, the affected target with the mutation effect and commit facts; for mutations that produce a domain result (such as a branch or a promotion outcome), that result object.

## Examples

Declare a collection's embedding model so text can be stored and searched.

### CLI

```console
$ strata vector collection create docs 3 --metric cosine
$ strata vector collection set-embedding-model docs openai:text-embedding-3-small  # Declared once; repeating it with the same model is a no-op.
```

### Wire

```json
{"collection":"docs","dimension":3,"metric":"cosine","type":"vector_create_collection"}
{"collection":"docs","model":"openai:text-embedding-3-small","type":"vector_set_embedding_model"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `collection` | `string` | yes | Collection name. |
| `model` | `string` | yes | The model that produced, and will produce, this collection's vectors. |

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`MutationAck` — an acknowledgement with no payload.

**Transitional wire:** the response currently carries a page rather than a mutation acknowledgement; the declaration is the target shape and the wire is scheduled to be normalised.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.product_space`](https://stratadb.org/e/invalid_argument.engine.product_space)
- [`invalid_argument.engine.vector_collection`](https://stratadb.org/e/invalid_argument.engine.vector_collection)
- [`invalid_argument.engine.vector_key`](https://stratadb.org/e/invalid_argument.engine.vector_key)
- [`not_found.engine.vector_collection`](https://stratadb.org/e/not_found.engine.vector_collection)
- [`invalid_argument.engine.embedding_model`](https://stratadb.org/e/invalid_argument.engine.embedding_model)
- [`failed_precondition.engine.embedding_model_mismatch`](https://stratadb.org/e/failed_precondition.engine.embedding_model_mismatch)

## Invocation

- CLI: `strata vector collection set-embedding-model`
- Wire type: `vector_set_embedding_model`
