---
title: "Read JSON document history"
description: "Read retained version history for one JSON document."
source: strata-core@1.2.2
section: json
---

Returns retained full-document history rows for a JSON document, newest first, including delete tombstones. A document with no retained history maps to an optional-read result.

Optional reads distinguish present data from missing data. When version or timestamp facts exist on the executor output, SDK mappings should preserve them.

## Examples

A document that never existed has no history.

### CLI

```console
$ strata json history absent
(nil)
```

### Wire

```json
{"key":"absent","type":"json_history"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `key` | `string` | yes | Document key. |

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`Maybe<Vec<JsonHistoryItem>>` — a miss returns nothing rather than raising.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.product_space`](https://stratadb.org/e/invalid_argument.engine.product_space)
- [`invalid_argument.engine.json_document_id`](https://stratadb.org/e/invalid_argument.engine.json_document_id)

## Invocation

- CLI: `strata json history`
- Wire type: `json_history`
