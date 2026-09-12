---
title: "Read IPC status"
description: "Report this process's multi-process IPC state."
source: strata-core@1.2.1
section: admin
---

Reports the same-machine multi-process state for this handle: whether this process owns the store (holds the writer lock) or is a client of another owner, whether it is hosting a broker socket, the socket path and owning process id when one exists, and the number of clients currently connected to the host. A single-process open (cache, or a durable open with IPC off) reports `is_owner: true`, `hosting: false`, and no socket.

Status commands return a scalar or compact status payload and do not mutate database state.

## Examples

Check whether this process hosts the multi-process broker socket.

### CLI

```console
$ strata ipc status
hosting       false
is_owner      true
client_count  0
```

### Wire

```json
{"type":"ipc_status"}
```

## Parameters

_No parameters._

## Returns

`StatusResponse<AdminIpcStatus>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)

## Invocation

- CLI: `strata ipc status`
- Wire type: `ipc_status`
