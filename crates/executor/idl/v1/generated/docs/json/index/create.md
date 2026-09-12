---
title: "Create JSON index"
description: "Create a JSON secondary index on a field path."
source: strata-core@1.2.1
section: json
---

Creates a secondary index over one JSON field path with a numeric, tag, or text kind. Existing documents are indexed at creation and future writes maintain the index automatically. The current wire response is a transitional bare index definition.

Successful mutations return an acknowledgement of the outcome: for a state-changing write, the affected target with the mutation effect and commit facts; for mutations that produce a domain result (such as a branch or a promotion outcome), that result object.

## Examples

Create a secondary index on a JSON field.

### CLI

```console
$ strata json index create by_name $.name --index-type tag
created index by_name on name (tag)
$ strata json index list
NAME     FIELD_PATH  INDEX_TYPE  SPACE    CREATED_VERSION
by_name  name        tag         default                3
```

### Wire

```json
{"field_path":"$.name","index_type":"tag","name":"by_name","type":"json_create_index"}
{"type":"json_list_indexes"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `field_path` | `string` | yes | Indexed field path. |
| `index_type` | `JsonIndexType` | yes | Index kind. |
| `name` | `string` | yes | Index name. |

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`MutationAck` — an acknowledgement with no payload.

**Transitional wire:** the response currently carries a bare record rather than a mutation acknowledgement; the declaration is the target shape and the wire is scheduled to be normalised.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.product_space`](https://stratadb.org/e/invalid_argument.engine.product_space)
- [`invalid_argument.engine.json_index_name`](https://stratadb.org/e/invalid_argument.engine.json_index_name)
- [`invalid_argument.engine.json_index_name_reserved`](https://stratadb.org/e/invalid_argument.engine.json_index_name_reserved)
- [`invalid_argument.engine.json_path`](https://stratadb.org/e/invalid_argument.engine.json_path)
- [`invalid_argument.engine.json_path_too_long`](https://stratadb.org/e/invalid_argument.engine.json_path_too_long)
- [`already_exists.engine.json_index`](https://stratadb.org/e/already_exists.engine.json_index)

## Invocation

- CLI: `strata json index create`
- Wire type: `json_create_index`
