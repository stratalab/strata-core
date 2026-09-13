---
title: "Check JSON document existence"
description: "Check whether one JSON document exists."
source: strata-core@1.2.2
section: json
---

Returns a boolean status for one JSON document key without loading the stored document.

Status commands return a scalar or compact status payload and do not mutate database state.

## Examples

Check whether a document exists.

### CLI

```console
$ strata json set user '$' '{"name":"alice"}'
created user
$ strata json exists user
true
$ strata json exists absent
false
```

### Wire

```json
{"key":"user","path":"$","type":"json_set","value":{"name":"alice"}}
{"key":"user","type":"json_exists"}
{"key":"absent","type":"json_exists"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `key` | `string` | yes | Document key. |

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`StatusValue<boolean>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.product_space`](https://stratadb.org/e/invalid_argument.engine.product_space)
- [`invalid_argument.engine.json_document_id`](https://stratadb.org/e/invalid_argument.engine.json_document_id)

## Invocation

- CLI: `strata json exists`
- Wire type: `json_exists`
