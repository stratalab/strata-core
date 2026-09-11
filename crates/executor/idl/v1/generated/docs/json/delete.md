---
title: "Delete JSON value"
description: "Delete a whole JSON document or one path inside it."
source: strata-core@1.2.1
section: json
---

Deletes the root path `$` to remove the whole document, or a nested path to remove one field or array element. Missing documents and paths produce a no-op delete acknowledgement rather than an error.

Successful mutations return an acknowledgement of the outcome: for a state-changing write, the affected target with the mutation effect and commit facts; for mutations that produce a domain result (such as a branch or a promotion outcome), that result object.

## Examples

Delete a document.

### CLI

```console
$ strata json set temp $ {"x":1}
$ strata json delete temp $
$ strata json exists temp
```

### Wire

```json
{"key":"temp","path":"$","type":"json_set","value":{"x":1}}
{"key":"temp","path":"$","type":"json_delete"}
{"key":"temp","type":"json_exists"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `key` | `string` | yes | Document key. |
| `path` | `string` | yes | JSON path. |

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
- [`invalid_argument.engine.json_path_type`](https://stratadb.org/e/invalid_argument.engine.json_path_type)

## Invocation

- CLI: `strata json delete`
- Wire type: `json_delete`
