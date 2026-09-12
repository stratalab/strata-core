---
title: "Read event time range"
description: "Read a range of events by occurrence time."
source: strata-core@1.2.1
section: event
---

Reads events from the selected branch and space whose append timestamps fall inside a half-open `[start_ts, end_ts)` microsecond window — the start is inclusive and the end is exclusive, matching the sequence-addressed range. This queries when events occurred; historical log states are the timestamped read commands' job. An optional event type narrows the results.

Paginated responses use opaque cursors. Clients should pass the returned cursor back to the same command shape and must not parse cursor contents.

## Examples

Read a range of events by timestamp.

### CLI

```console
$ strata event append user.created {"id":1}
appended user.created #0
$ strata event append user.updated {"id":2}
appended user.updated #1
$ strata event range-time 0 --direction forward
SEQUENCE  EVENT_TYPE    TIMESTAMP                       PAYLOAD
       0  user.created  …          …               UTC  {"id":1}
       1  user.updated  …          …               UTC  {"id":2}
```

`…` stands for a value that varies by run or by machine — an instant, a version, an id, a path.

### Wire

```json
{"event_type":"user.created","payload":{"id":1},"type":"event_append"}
{"event_type":"user.updated","payload":{"id":2},"type":"event_append"}
{"direction":"forward","start_ts":0,"type":"event_range_by_time"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `direction` | `EventRangeDirection` | yes | Result ordering. |
| `end_ts` | `integer` | no | Optional exclusive end timestamp in microseconds (half-open window, matching the sequence-addressed range's exclusive end). |
| `event_type` | `string` | no | Optional event type filter. |
| `limit` | `integer` | no | Optional item limit. |
| `start_ts` | `integer` | yes | Inclusive start timestamp in microseconds. |

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

- CLI: `strata event range-time`
- Wire type: `event_range_by_time`
