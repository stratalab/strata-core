---
title: "Batch check JSON document existence"
description: "Check existence for multiple JSON documents."
source: strata-core@1.2.1
section: json
---

Checks several JSON documents and returns positional boolean status values. The response preserves the input order.

Itemwise batches return one positional item result per input item. The outer batch status summarizes whether all, some, or none of the items succeeded.

## Examples

Check existence for many document keys at once.

### CLI

```console
$ strata command run --command-json '{"entries":[{"key":"a","path":"$","value":{"v":1}}],"type":"json_batch_set"}'
#  STATUS  EFFECT   DOCUMENT_VERSION
0  ok      created                 …
$ strata command run --command-json '{"keys":["a","missing"],"type":"json_batch_exists"}'
#  STATUS  EXISTS
0  ok      true
1  ok      false
```

`…` stands for a value that varies by run or by machine — an instant, a version, an id, a path.

### Wire

```json
{"entries":[{"key":"a","path":"$","value":{"v":1}}],"type":"json_batch_set"}
{"keys":["a","missing"],"type":"json_batch_exists"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `keys` | `string[]` | yes | Document keys to check. |

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`BatchResult<BatchExistsPresence>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.product_space`](https://stratadb.org/e/invalid_argument.engine.product_space)
- [`invalid_argument.engine.json_document_id`](https://stratadb.org/e/invalid_argument.engine.json_document_id)

## Invocation

- CLI: via `strata command run` (no dedicated verb)
- Wire type: `json_batch_exists`
