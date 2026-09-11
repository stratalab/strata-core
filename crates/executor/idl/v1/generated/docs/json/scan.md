---
title: "Scan JSON documents"
description: "Scan JSON documents with values and version facts."
source: strata-core@1.2.1
section: json
---

Scans visible JSON documents starting at an optional document key. Each item includes the key, full document value, and commit metadata exposed by the executor output.

Paginated responses use opaque cursors. Clients should pass the returned cursor back to the same command shape and must not parse cursor contents.

## Examples

Scan documents from the start, in key order.

### CLI

```console
$ strata json set a $ {"v":1}
$ strata json set b $ {"v":2}
$ strata json scan
```

### Wire

```json
{"key":"a","path":"$","type":"json_set","value":{"v":1}}
{"key":"b","path":"$","type":"json_set","value":{"v":2}}
{"type":"json_scan"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `limit` | `integer` | no | Optional row limit. |
| `start` | `string` | no | Optional inclusive start document key. |

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`Page<JsonSampleItem>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.product_space`](https://stratadb.org/e/invalid_argument.engine.product_space)
- [`invalid_argument.engine.json_document_id`](https://stratadb.org/e/invalid_argument.engine.json_document_id)

## Invocation

- CLI: `strata json scan`
- Wire type: `json_scan`
