---
title: "List product spaces"
description: "List product spaces on a branch."
source: strata-core@1.2.1
section: space
---

Lists the product space names cataloged on the target branch as a terminal page. Every branch has a `default` space; additional spaces isolate data namespaces within the same branch.

Paginated responses use opaque cursors. Clients should pass the returned cursor back to the same command shape and must not parse cursor contents.

## Examples

List product spaces.

### CLI

```console
$ strata space create app
$ strata space list
```

### Wire

```json
{"space":"app","type":"space_create"}
{"type":"space_list"}
```

## Parameters

_No parameters._

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`Page<string>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)

## Invocation

- CLI: `strata space list`
- Wire type: `space_list`
