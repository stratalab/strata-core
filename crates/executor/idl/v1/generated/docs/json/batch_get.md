---
title: "Batch get JSON values"
description: "Read multiple JSON values by document and path."
source: strata-core@1.2.1
section: json
---

Reads several document/path entries and returns positional item results. Each item records whether the value was found and includes version metadata when present.

Itemwise batches return one positional item result per input item. The outer batch status summarizes whether all, some, or none of the items succeeded.

## Examples

Read many documents at once.

### CLI

```console
$ strata command run --command-json '{"entries":[{"key":"a","path":"$","value":{"v":1}},{"key":"b","path":"$","value":{"v":2}}],"type":"json_batch_set"}'
$ strata command run --command-json '{"entries":[{"key":"a","path":"$"},{"key":"b","path":"$"}],"type":"json_batch_get"}'
```

### Wire

```json
{"entries":[{"key":"a","path":"$","value":{"v":1}},{"key":"b","path":"$","value":{"v":2}}],"type":"json_batch_set"}
{"entries":[{"key":"a","path":"$"},{"key":"b","path":"$"}],"type":"json_batch_get"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
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
