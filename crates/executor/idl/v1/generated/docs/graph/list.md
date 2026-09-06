---
title: "List graphs"
description: "List graph names."
source: strata-core@1.2.0
section: graph
---

Lists graph names in lexicographic order. Accepts an optional item limit (default 100), an exclusive name cursor for continuation, and an `as_of` timestamp to list the graphs visible at an earlier instant.

Paginated responses use opaque cursors. Clients should pass the returned cursor back to the same command shape and must not parse cursor contents.

## Examples

List graphs.

### CLI

```console
$ strata graph create social
$ strata graph list
```

### Wire

```json
{"graph":"social","type":"graph_create"}
{"type":"graph_list"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `as_of` | `integer` | no | Read as of a position on the logical commit timeline — the `timestamp` from `history` output, not the `version`, and never a calendar date. Reads the graph state visible at that timeline position. To read as of a real time, use `as_of_time` instead. |
| `as_of_time` | `integer` | no | Read as of a real time: a wall-clock instant in microseconds since the Unix epoch (UTC), as reported by `committed_at` on a write ack or on any `history` row. Resolves to the commit at or before that instant, and fails rather than guessing if the instant falls outside the branch's recorded history. Mutually exclusive with `as_of`. |
| `cursor` | `string` | no | Optional exclusive graph cursor. |
| `limit` | `integer` | no | Optional item limit. Defaults to 100. |

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`Page<String, String>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.product_space`](https://stratadb.org/e/invalid_argument.engine.product_space)
- [`invalid_argument.engine.graph_name`](https://stratadb.org/e/invalid_argument.engine.graph_name)

## Invocation

- CLI: `strata graph list`
- Wire type: `graph_list`
