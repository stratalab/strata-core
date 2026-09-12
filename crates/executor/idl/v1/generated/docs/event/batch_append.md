---
title: "Batch append events"
description: "Append multiple events in one commit."
source: strata-core@1.2.1
section: event
---

Appends multiple events to the selected branch and space in one engine commit. Sequences are assigned in entry order. Entries that fail validation report a positional item error while valid entries still append.

Itemwise batches return one positional item result per input item. The outer batch status summarizes whether all, some, or none of the items succeeded.

## Examples

Append many events in one commit.

### CLI

```console
$ strata command run --command-json '{"entries":[{"event_type":"user.created","payload":{"id":1}},{"event_type":"user.updated","payload":{"id":2}}],"type":"event_batch_append"}'
#  STATUS  EFFECT   EVENT_TYPE    SEQUENCE
0  ok      created  user.created         0
1  ok      created  user.updated         1
$ strata event count
2
```

### Wire

```json
{"entries":[{"event_type":"user.created","payload":{"id":1}},{"event_type":"user.updated","payload":{"id":2}}],"type":"event_batch_append"}
{"type":"event_count"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `entries` | `BatchEventEntry[]` | yes | Events to append. |

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`BatchResult<EventBatchAppendItemResult>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.product_space`](https://stratadb.org/e/invalid_argument.engine.product_space)
- [`invalid_argument.engine.event_batch`](https://stratadb.org/e/invalid_argument.engine.event_batch)
- [`invalid_argument.engine.event_type`](https://stratadb.org/e/invalid_argument.engine.event_type)
- [`invalid_argument.engine.event_payload`](https://stratadb.org/e/invalid_argument.engine.event_payload)
- [`invalid_argument.engine.event_payload_too_large`](https://stratadb.org/e/invalid_argument.engine.event_payload_too_large)

## Invocation

- CLI: via `strata command run` (no dedicated verb)
- Wire type: `event_batch_append`
