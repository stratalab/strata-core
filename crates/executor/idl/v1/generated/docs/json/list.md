---
title: "List JSON document keys"
description: "List JSON document keys with optional prefix filtering."
source: strata-core@1.2.0
section: json
---

Lists visible JSON document keys in byte order. Prefix, cursor, limit, and timestamp parameters constrain the page returned by the executor.

Paginated responses use opaque cursors. Clients should pass the returned cursor back to the same command shape and must not parse cursor contents.

## Examples

List document keys under a prefix, in key order.

### CLI

```console
$ strata json set user:1 $ {"v":1}
$ strata json set user:2 $ {"v":2}
$ strata json set other $ {"v":3}
$ strata json list --prefix user:
```

### Wire

```json
{"key":"user:1","path":"$","type":"json_set","value":{"v":1}}
{"key":"user:2","path":"$","type":"json_set","value":{"v":2}}
{"key":"other","path":"$","type":"json_set","value":{"v":3}}
{"prefix":"user:","type":"json_list"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `as_of` | `integer` | no | Read as of a position on the logical commit timeline — the `timestamp` from `history` output, not the `version`, and never a calendar date. To read as of a real time, use `as_of_time` instead. |
| `as_of_time` | `integer` | no | Read as of a real time: a wall-clock instant in microseconds since the Unix epoch (UTC), as reported by `committed_at` on a write ack or on any `history` row. Resolves to the commit at or before that instant, and fails rather than guessing if the instant falls outside the branch's recorded history. Mutually exclusive with `as_of`. |
| `cursor` | `string` | no | Optional document key cursor. |
| `limit` | `integer` | no | Optional item limit. |
| `prefix` | `string` | no | Optional document key prefix. |

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`Page<String, String>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.product_space`](https://stratadb.org/e/invalid_argument.engine.product_space)
- [`invalid_argument.engine.json_document_id`](https://stratadb.org/e/invalid_argument.engine.json_document_id)

## Invocation

- CLI: `strata json list`
- Wire type: `json_list`
