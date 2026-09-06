---
title: "List events"
description: "List events with optional type filter and cursor."
source: strata-core@1.2.0
section: event
---

Lists events from the selected branch and space in sequence order. An optional event type narrows the results, the sequence cursor continues a previous page, and the optional timestamp reads the log visible at that commit time.

Paginated responses use opaque cursors. Clients should pass the returned cursor back to the same command shape and must not parse cursor contents.

## Examples

List events in sequence order.

### CLI

```console
$ strata event append user.created {"id":1}
$ strata event append user.updated {"id":2}
$ strata event list
```

### Wire

```json
{"event_type":"user.created","payload":{"id":1},"type":"event_append"}
{"event_type":"user.updated","payload":{"id":2},"type":"event_append"}
{"type":"event_list"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `after_sequence` | `integer` | no | Optional exclusive sequence cursor. |
| `as_of` | `integer` | no | Read as of a position on the logical commit timeline — the `timestamp` from `history` output, not the `version`, and never a calendar date. To read as of a real time, use `as_of_time` instead. |
| `as_of_time` | `integer` | no | Read as of a real time: a wall-clock instant in microseconds since the Unix epoch (UTC), as reported by `committed_at` on a write ack or on any `history` row. Resolves to the commit at or before that instant, and fails rather than guessing if the instant falls outside the branch's recorded history. Mutually exclusive with `as_of`. |
| `event_type` | `string` | no | Optional event type filter. |
| `limit` | `integer` | no | Optional item limit. |

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`Page<EventVersionedData, u64>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.product_space`](https://stratadb.org/e/invalid_argument.engine.product_space)
- [`invalid_argument.engine.event_type`](https://stratadb.org/e/invalid_argument.engine.event_type)
- [`invalid_argument.executor.limit`](https://stratadb.org/e/invalid_argument.executor.limit)

## Invocation

- CLI: `strata event list`
- Wire type: `event_list`
