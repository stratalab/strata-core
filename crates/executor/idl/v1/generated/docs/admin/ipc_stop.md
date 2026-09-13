---
title: "Stop IPC hosting"
description: "Stop hosting the multi-process broker socket."
source: strata-core@1.2.2
section: admin
---

Stops hosting the same-machine broker socket for this store: the listener stops accepting connections and the store is no longer reachable by new clients. Run from a client, it forwards to the owner, which stops hosting (ending that client's own connection). The store stays open in this process; the socket files are unlinked when the owner closes. Idempotent — a process that was not hosting reports `stopped: false`.

Status commands return a scalar or compact status payload and do not mutate database state.

## Examples

Stop hosting the multi-process broker socket.

### CLI

```console
$ strata ipc stop
stopped  false
```

### Wire

```json
{"type":"ipc_stop"}
```

## Parameters

_No parameters._

## Returns

`StatusResponse<AdminIpcStop>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)

## Invocation

- CLI: `strata ipc stop`
- Wire type: `ipc_stop`
