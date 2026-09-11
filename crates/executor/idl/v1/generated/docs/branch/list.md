---
title: "List branches"
description: "List active branches with their lineage facts."
source: strata-core@1.2.1
section: branch
---

Lists every active branch as a terminal page. Each item carries the branch name, deterministic branch id, generation, status, parent lineage (fork version and timestamp when forked), and logical creation version.

Paginated responses use opaque cursors. Clients should pass the returned cursor back to the same command shape and must not parse cursor contents.

## Examples

List branches.

### CLI

```console
$ strata branch create feature
$ strata branch list
```

### Wire

```json
{"branch":"feature","type":"branch_create"}
{"type":"branch_list"}
```

## Parameters

_No parameters._

## Returns

`Page<BranchItem>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)

## Invocation

- CLI: `strata branch list`
- Wire type: `branch_list`
