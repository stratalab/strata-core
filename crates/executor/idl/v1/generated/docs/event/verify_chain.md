---
title: "Verify event chain"
description: "Verify event log density and hash linkage."
source: strata-core@1.2.1
section: event
---

Verifies that the visible event log in the selected branch and space is dense and hash-linked: sequences are contiguous from zero, the genesis record links to the all-zeros hash, and every record's hash matches its content and predecessor.

Status commands return a scalar or compact status payload and do not mutate database state.

## Examples

Verify the integrity of the event hash chain.

### CLI

```console
$ strata event append user.created {"id":1}
appended user.created #0
$ strata event verify-chain
valid          true
length         1
first_invalid  -
error          -
```

### Wire

```json
{"event_type":"user.created","payload":{"id":1},"type":"event_append"}
{"type":"event_verify_chain"}
```

## Parameters

_No parameters._

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`StatusResponse<EventChainVerification>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.product_space`](https://stratadb.org/e/invalid_argument.engine.product_space)

## Invocation

- CLI: `strata event verify-chain`
- Wire type: `event_verify_chain`
