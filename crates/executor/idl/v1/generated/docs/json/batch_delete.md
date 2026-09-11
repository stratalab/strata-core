---
title: "Batch delete JSON values"
description: "Delete multiple JSON documents or paths in one itemwise batch."
source: strata-core@1.2.1
section: json
---

Deletes multiple document/path entries and returns one positional mutation result per entry. Missing documents and paths are represented as no-op item results; applied items share one engine commit.

Itemwise batches return one positional item result per input item. The outer batch status summarizes whether all, some, or none of the items succeeded.

## Examples

Delete many documents in one commit.

### CLI

```console
$ strata command run --command-json '{"entries":[{"key":"a","path":"$","value":{"v":1}}],"type":"json_batch_set"}'
$ strata command run --command-json '{"entries":[{"key":"a","path":"$"}],"type":"json_batch_delete"}'
$ strata json exists a
```

### Wire

```json
{"entries":[{"key":"a","path":"$","value":{"v":1}}],"type":"json_batch_set"}
{"entries":[{"key":"a","path":"$"}],"type":"json_batch_delete"}
{"key":"a","type":"json_exists"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `entries` | `BatchJsonDeleteEntry[]` | yes | Entries to delete. |

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`BatchResult<JsonBatchItemResult>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.product_space`](https://stratadb.org/e/invalid_argument.engine.product_space)
- [`invalid_argument.engine.json_document_id`](https://stratadb.org/e/invalid_argument.engine.json_document_id)
- [`invalid_argument.engine.json_path`](https://stratadb.org/e/invalid_argument.engine.json_path)
- [`invalid_argument.engine.json_path_too_long`](https://stratadb.org/e/invalid_argument.engine.json_path_too_long)
- [`invalid_argument.engine.json_path_type`](https://stratadb.org/e/invalid_argument.engine.json_path_type)
- [`invalid_argument.executor.json_batch_duplicate_key`](https://stratadb.org/e/invalid_argument.executor.json_batch_duplicate_key)

## Invocation

- CLI: via `strata command run` (no dedicated verb)
- Wire type: `json_batch_delete`
