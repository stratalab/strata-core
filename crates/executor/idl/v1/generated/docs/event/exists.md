---
title: "Check event existence"
description: "Check whether an event sequence exists."
source: strata-core@1.2.2
section: event
---

Checks whether one event sequence exists in the selected branch and space without returning the record.

Status commands return a scalar or compact status payload and do not mutate database state.

## Examples

Check whether an event sequence exists.

### CLI

```console
$ strata event append user.created '{"id":1}'
appended user.created #0
$ strata event exists 0
true
$ strata event exists 999
false
```

### Wire

```json
{"event_type":"user.created","payload":{"id":1},"type":"event_append"}
{"sequence":0,"type":"event_exists"}
{"sequence":999,"type":"event_exists"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `sequence` | `integer` | yes | Event sequence. |

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`StatusValue<boolean>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.product_space`](https://stratadb.org/e/invalid_argument.engine.product_space)

## Invocation

- CLI: `strata event exists`
- Wire type: `event_exists`
