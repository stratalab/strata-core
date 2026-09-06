---
title: "Count JSON documents"
description: "Count visible JSON documents."
source: strata-core@1.2.0
section: json
---

Counts visible JSON documents in the selected branch and space, optionally constrained by a document key prefix.

Status commands return a scalar or compact status payload and do not mutate database state.

## Examples

Count stored documents.

### CLI

```console
$ strata json set a $ {"v":1}
$ strata json set b $ {"v":2}
$ strata json count
```

### Wire

```json
{"key":"a","path":"$","type":"json_set","value":{"v":1}}
{"key":"b","path":"$","type":"json_set","value":{"v":2}}
{"type":"json_count"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `as_of` | `integer` | no | Read as of a position on the logical commit timeline — the `timestamp` from `history` output, not the `version`, and never a calendar date. To read as of a real time, use `as_of_time` instead. |
| `as_of_time` | `integer` | no | Read as of a real time: a wall-clock instant in microseconds since the Unix epoch (UTC), as reported by `committed_at` on a write ack or on any `history` row. Resolves to the commit at or before that instant, and fails rather than guessing if the instant falls outside the branch's recorded history. Mutually exclusive with `as_of`. |
| `prefix` | `string` | no | Optional document key prefix. |

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`StatusValue<u64>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.product_space`](https://stratadb.org/e/invalid_argument.engine.product_space)
- [`invalid_argument.engine.json_document_id`](https://stratadb.org/e/invalid_argument.engine.json_document_id)

## Invocation

- CLI: `strata json count`
- Wire type: `json_count`
