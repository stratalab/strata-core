---
title: "Set JSON value"
description: "Set a JSON value at a document path, creating the document when missing."
source: strata-core@1.2.1
section: json
---

Writes a JSON value at a path inside a document, creating the document and any missing intermediate objects when needed. Setting the root path `$` replaces the whole document; setting a nested path like `$.profile.name` updates one field and records a new document version.

Successful mutations return an acknowledgement of the outcome: for a state-changing write, the affected target with the mutation effect and commit facts; for mutations that produce a domain result (such as a branch or a promotion outcome), that result object.

## Examples

Store a JSON document, then read it back.

### CLI

```console
$ strata json set user $ {"age":30,"name":"alice"}
$ strata json get user $
```

### Wire

```json
{"key":"user","path":"$","type":"json_set","value":{"age":30,"name":"alice"}}
{"key":"user","path":"$","type":"json_get"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `key` | `string` | yes | Document key. |
| `path` | `string` | yes | JSON path. |
| `value` | `any` | yes | JSON value. |

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`MutationAck` — an acknowledgement with no payload.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.product_space`](https://stratadb.org/e/invalid_argument.engine.product_space)
- [`invalid_argument.engine.json_document_id`](https://stratadb.org/e/invalid_argument.engine.json_document_id)
- [`invalid_argument.engine.json_path`](https://stratadb.org/e/invalid_argument.engine.json_path)
- [`invalid_argument.engine.json_path_too_long`](https://stratadb.org/e/invalid_argument.engine.json_path_too_long)
- [`invalid_argument.engine.json_path_not_found`](https://stratadb.org/e/invalid_argument.engine.json_path_not_found)
- [`invalid_argument.engine.json_path_type`](https://stratadb.org/e/invalid_argument.engine.json_path_type)
- [`invalid_argument.engine.json_value`](https://stratadb.org/e/invalid_argument.engine.json_value)
- [`invalid_argument.engine.json_document_too_large`](https://stratadb.org/e/invalid_argument.engine.json_document_too_large)
- [`invalid_argument.engine.json_document_too_deep`](https://stratadb.org/e/invalid_argument.engine.json_document_too_deep)
- [`invalid_argument.engine.json_array_too_large`](https://stratadb.org/e/invalid_argument.engine.json_array_too_large)

## Invocation

- CLI: `strata json set`
- Wire type: `json_set`
