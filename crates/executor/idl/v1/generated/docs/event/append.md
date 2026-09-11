---
title: "Append event"
description: "Append one event to the branch event log."
source: strata-core@1.2.1
section: event
---

Appends one event to the selected branch and space. Strata assigns the next sequence number, stamps the event with its append timestamp, and links it into the tamper-evident hash chain. Events are immutable once appended.

Successful mutations return an acknowledgement of the outcome: for a state-changing write, the affected target with the mutation effect and commit facts; for mutations that produce a domain result (such as a branch or a promotion outcome), that result object.

## Examples

Append an event to the log.

### CLI

```console
$ strata event append user.created {"id":1}
$ strata event count
```

### Wire

```json
{"event_type":"user.created","payload":{"id":1},"type":"event_append"}
{"type":"event_count"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `event_type` | `string` | yes | Event type. |
| `payload` | `any` | yes | Event payload. |

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`MutationAck` — an acknowledgement with no payload.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.product_space`](https://stratadb.org/e/invalid_argument.engine.product_space)
- [`invalid_argument.engine.event_type`](https://stratadb.org/e/invalid_argument.engine.event_type)
- [`invalid_argument.engine.event_payload`](https://stratadb.org/e/invalid_argument.engine.event_payload)
- [`invalid_argument.engine.event_payload_too_large`](https://stratadb.org/e/invalid_argument.engine.event_payload_too_large)

## Invocation

- CLI: `strata event append`
- Wire type: `event_append`
