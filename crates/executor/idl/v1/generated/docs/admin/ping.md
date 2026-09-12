---
title: "Ping database"
description: "Check that the database handle is live."
source: strata-core@1.2.1
section: admin
---

Lightweight liveness check. Returns the engine package version without touching branches, spaces, or primitive data. Use it to confirm the handle is open and responsive before issuing heavier commands.

Status commands return a scalar or compact status payload and do not mutate database state.

## Examples

Check the database handle is live.

### CLI

```console
$ strata ping
pong …
```

`…` stands for a value that varies by run or by machine — an instant, a version, an id, a path.

### Wire

```json
{"type":"ping"}
```

## Parameters

_No parameters._

## Returns

`StatusResponse<AdminPing>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)

## Invocation

- CLI: `strata ping`
- Wire type: `ping`
