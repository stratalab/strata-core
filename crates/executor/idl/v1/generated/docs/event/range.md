---
title: "Read event sequence range"
description: "Read a range of events by sequence number."
source: strata-core@1.2.1
section: event
---

Reads events from the selected branch and space by sequence range. The start sequence is inclusive and the optional end sequence is exclusive; reverse direction returns the same `[start_seq, end_seq)` window in descending order (newest first). An optional event type narrows the results.

Paginated responses use opaque cursors. Clients should pass the returned cursor back to the same command shape and must not parse cursor contents.

## Examples

Read a range of events by sequence.

### CLI

```console
$ strata event append user.created {"id":1}
$ strata event append user.updated {"id":2}
$ strata event range 0 --direction forward
```

### Wire

```json
{"event_type":"user.created","payload":{"id":1},"type":"event_append"}
{"event_type":"user.updated","payload":{"id":2},"type":"event_append"}
{"direction":"forward","start_seq":0,"type":"event_range"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `direction` | `EventRangeDirection` | yes | Result ordering. |
| `end_seq` | `integer` | no | Optional exclusive upper bound of the sequence window (same in both directions). |
| `event_type` | `string` | no | Optional event type filter. |
| `limit` | `integer` | no | Optional item limit. |
| `start_seq` | `integer` | yes | Inclusive lower bound of the sequence window (same in both directions). |

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`Page<EventVersionedData>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.product_space`](https://stratadb.org/e/invalid_argument.engine.product_space)
- [`invalid_argument.engine.event_type`](https://stratadb.org/e/invalid_argument.engine.event_type)
- [`invalid_argument.executor.limit`](https://stratadb.org/e/invalid_argument.executor.limit)

## Invocation

- CLI: `strata event range`
- Wire type: `event_range`
