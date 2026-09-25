---
title: "Batch get JSON values"
description: "Read multiple JSON values by document and path."
source: strata-core@1.2.5
section: json
---

Reads several document/path entries and returns positional item results. Each item records whether the value was found and includes version metadata when present.

With `as_of` (a position on the logical commit timeline) or `as_of_time` (a wall-clock instant), every entry is read as of that one position, so a batch can hydrate the documents a version-pinned read named — a graph query at a version, say — without mixing in later state. Entries keep their order, a repeated entry is answered again, and a document absent at that position is a miss. An instant outside the branch's recorded history fails the whole batch with `history_unavailable.engine.persistence_history` rather than answering from the latest state; setting both clocks at once is refused.

Itemwise batches return one positional item result per input item. The outer batch status summarizes whether all, some, or none of the items succeeded.

## Examples

Read many documents at once.

### CLI

```console
$ strata command run --command-json '{"entries":[{"key":"a","path":"$","value":{"v":1}},{"key":"b","path":"$","value":{"v":2}}],"type":"json_batch_set"}'
#  STATUS  EFFECT   DOCUMENT_VERSION
0  ok      created                 …
1  ok      created                 …
$ strata command run --command-json '{"entries":[{"key":"a","path":"$"},{"key":"b","path":"$"}],"type":"json_batch_get"}'
#  STATUS  DOCUMENT_VERSION  VERSION  VALUE
0  ok                     …        …  {"v":1}
1  ok                     …        …  {"v":2}
```

`…` stands for a value that varies by run or by machine — an instant, a version, an id, a path.

### Wire

```json
{"entries":[{"key":"a","path":"$","value":{"v":1}},{"key":"b","path":"$","value":{"v":2}}],"type":"json_batch_set"}
{"entries":[{"key":"a","path":"$"},{"key":"b","path":"$"}],"type":"json_batch_get"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `as_of` | `integer` | no | Read every entry as of a position on the logical commit timeline — the `timestamp` from `history` output, not the `version`, and never a calendar date. To read as of a real time, use `as_of_time` instead. |
| `as_of_time` | `integer` | no | Read every entry as of a real time: a wall-clock instant in microseconds since the Unix epoch (UTC), as reported by `committed_at` on a write ack or on any `history` row. Resolves to the commit at or before that instant, and fails rather than guessing if the instant falls outside the branch's recorded history. Mutually exclusive with `as_of`. |
| `entries` | `BatchJsonGetEntry[]` | yes | Entries to read. |

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`BatchResult<JsonBatchGetItemResult>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.product_space`](https://stratadb.org/e/invalid_argument.engine.product_space)
- [`invalid_argument.engine.json_document_id`](https://stratadb.org/e/invalid_argument.engine.json_document_id)
- [`invalid_argument.engine.json_path`](https://stratadb.org/e/invalid_argument.engine.json_path)
- [`invalid_argument.engine.json_path_too_long`](https://stratadb.org/e/invalid_argument.engine.json_path_too_long)

## Invocation

- CLI: via `strata command run` (no dedicated verb)
- Wire type: `json_batch_get`
