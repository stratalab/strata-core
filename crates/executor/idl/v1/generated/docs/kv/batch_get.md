---
title: "Batch get KV values"
description: "Read multiple KV values by key."
source: strata-core@1.2.7
section: kv
---

Reads several KV keys and returns positional item results. Each item records whether the corresponding key was found and includes value metadata when present.

Every key is read from one snapshot, so a commit that lands while the batch runs is visible to every key or to none. With `as_of` (a position on the logical commit timeline) or `as_of_time` (a wall-clock instant), every key is read as of that one position, so a batch can hydrate the entries a version-pinned read named — a graph query at a version, say — without mixing in later state. Keys keep their order, a repeated key is answered again, and a key absent at that position is a miss. An instant outside the branch's recorded history fails the whole batch rather than answering from the latest state: `history_unavailable.engine.before_first_commit` or `history_unavailable.engine.after_latest_commit` names the end it missed, `history_unavailable.engine.undated_history` refuses an `as_of_time` over commits that record no wall-clock time, and `history_unavailable.engine.persistence_history` refuses history a retention policy pruned; setting both clocks at once is refused.

Itemwise batches return one positional item result per input item. The outer batch status summarizes whether all, some, or none of the items succeeded.

## Examples

Read many keys at once; a missing key comes back as null.

### CLI

```console
$ strata command run --command-json '{"entries":[{"key":"YQ==","value":"MQ=="},{"key":"Yg==","value":"Mg=="}],"type":"kv_batch_put"}'
#  STATUS  EFFECT   KEY
0  ok      created  a
1  ok      created  b
$ strata command run --command-json '{"keys":["YQ==","Yg==","bWlzc2luZw=="],"type":"kv_batch_get"}'
#  STATUS  KEY      VERSION  VALUE
0  ok      a              …  1
1  ok      b              …  2
2  miss    missing        -  -
-- itemwise: 2 ok, 1 miss
```

`…` stands for a value that varies by run or by machine — an instant, a version, an id, a path.

### Wire

```json
{"entries":[{"key":"YQ==","value":"MQ=="},{"key":"Yg==","value":"Mg=="}],"type":"kv_batch_put"}
{"keys":["YQ==","Yg==","bWlzc2luZw=="],"type":"kv_batch_get"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `as_of` | `integer` | no | Read every key as of a position on the logical commit timeline — the `timestamp` from `history` output, not the `version`, and never a calendar date. To read as of a real time, use `as_of_time` instead. |
| `as_of_time` | `integer` | no | Read every key as of a real time: a wall-clock instant in microseconds since the Unix epoch (UTC), as reported by `committed_at` on a write ack or on any `history` row. Resolves to the commit at or before that instant, and fails rather than guessing if the instant falls outside the branch's recorded history. Mutually exclusive with `as_of`. |
| `keys` | `Bytes[]` | yes | Keys to read. |

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`BatchResult<BatchGetItemResult>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.product_space`](https://stratadb.org/e/invalid_argument.engine.product_space)
- [`invalid_argument.engine.kv_key`](https://stratadb.org/e/invalid_argument.engine.kv_key)
- [`invalid_argument.engine.kv_batch`](https://stratadb.org/e/invalid_argument.engine.kv_batch)
- [`history_unavailable.engine.persistence_history`](https://stratadb.org/e/history_unavailable.engine.persistence_history)
- [`history_unavailable.engine.before_first_commit`](https://stratadb.org/e/history_unavailable.engine.before_first_commit)
- [`history_unavailable.engine.after_latest_commit`](https://stratadb.org/e/history_unavailable.engine.after_latest_commit)
- [`history_unavailable.engine.undated_history`](https://stratadb.org/e/history_unavailable.engine.undated_history)

## Invocation

- CLI: via `strata command run` (no dedicated verb)
- Wire type: `kv_batch_get`
