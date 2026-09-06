---
title: "List KV keys"
description: "List KV keys with optional prefix filtering."
source: strata-core@1.2.0
section: kv
---

Lists visible KV keys in byte order. Prefix, cursor, limit, and timestamp parameters constrain the page returned by the executor.

Paginated responses use opaque cursors. Clients should pass the returned cursor back to the same command shape and must not parse cursor contents.

## Examples

List keys under a prefix, in key order.

### CLI

```console
$ strata kv put user:1 a
$ strata kv put user:2 b
$ strata kv put other c
$ strata kv list --prefix user:
```

### Wire

```json
{"key":"dXNlcjox","type":"kv_put","value":"YQ=="}
{"key":"dXNlcjoy","type":"kv_put","value":"Yg=="}
{"key":"b3RoZXI=","type":"kv_put","value":"Yw=="}
{"prefix":"dXNlcjo=","type":"kv_list"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `as_of` | `integer` | no | Read as of a position on the logical commit timeline — the `timestamp` from `history` output, not the `version`, and never a calendar date. To read as of a real time, use `as_of_time` instead. |
| `as_of_time` | `integer` | no | Read as of a real time: a wall-clock instant in microseconds since the Unix epoch (UTC), as reported by `committed_at` on a write ack or on any `history` row. Resolves to the commit at or before that instant, and fails rather than guessing if the instant falls outside the branch's recorded history. Mutually exclusive with `as_of`. |
| `cursor` | `Bytes` | no | Optional key cursor. |
| `limit` | `integer` | no | Optional item limit. Defaults to 100. |
| `prefix` | `Bytes` | no | Optional key prefix. |

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`Page<Bytes, Bytes>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.product_space`](https://stratadb.org/e/invalid_argument.engine.product_space)
- [`invalid_argument.engine.kv_key`](https://stratadb.org/e/invalid_argument.engine.kv_key)

## Invocation

- CLI: `strata kv list`
- Wire type: `kv_list`
