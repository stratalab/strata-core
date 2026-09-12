---
title: "Read sanitized config"
description: "Read sanitized configuration facts."
source: strata-core@1.2.1
section: admin
---

Returns sanitized configuration facts: the open target, whether this open created the database, durability, and the default branch. Only allowlisted, non-sensitive facts are exposed; no filesystem paths, credentials, or provider keys are ever returned.

Status commands return a scalar or compact status payload and do not mutate database state.

## Examples

Read sanitized configuration facts.

### CLI

```console
$ strata config get
target          cache
durable         false
default_branch  default
created         true
```

### Wire

```json
{"type":"config_get"}
```

## Parameters

_No parameters._

## Returns

`StatusResponse<AdminConfig>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)

## Invocation

- CLI: `strata config get`
- Wire type: `config_get`
