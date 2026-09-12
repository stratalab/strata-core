---
title: "Export Arrow file"
description: "Export a product primitive to an Arrow-compatible file."
source: strata-core@1.2.1
section: arrow
---

Exports a product primitive from the selected branch and space to an Arrow-compatible file (Parquet, CSV, or JSONL). Graph exports treat the path as a stem and write separate node and edge files. Returns a summary of the exported primitive, the concrete output paths, the row count, and the total output size.

Status commands return a scalar or compact status payload and do not mutate database state.

## Examples

Export a primitive to a Parquet file, then import it back.

### CLI

```console
$ strata kv put greeting hello
created greeting
$ strata command run --command-json '{"format":"parquet","path":"/tmp/exports/kv.parquet","primitive":"kv","type":"arrow_export"}'  # One file per primitive; Parquet by default.
exported 1 row of kv to … (1.9 kB)
$ strata kv delete greeting
deleted greeting
$ strata arrow import /tmp/exports/kv.parquet --target kv
imported 1 row into kv from … (0 skipped)
$ strata kv get greeting
hello
```

`…` stands for a value that varies by run or by machine — an instant, a version, an id, a path.

### Wire

```json
{"key":"Z3JlZXRpbmc=","type":"kv_put","value":"aGVsbG8="}
{"format":"parquet","path":"/tmp/exports/kv.parquet","primitive":"kv","type":"arrow_export"}
{"key":"Z3JlZXRpbmc=","type":"kv_delete"}
{"file_path":"/tmp/exports/kv.parquet","target":"kv","type":"arrow_import"}
{"key":"Z3JlZXRpbmc=","type":"kv_get"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `collection` | `string` | no | Target vector collection for vector exports. |
| `event_type` | `string` | no | Optional event type filter for event exports. |
| `format` | `ArrowFileFormat` | yes | Output file format. |
| `graph` | `string` | no | Target graph for graph exports. |
| `limit` | `integer` | no | Optional row limit. |
| `path` | `string` | yes | Output file path. Graph exports treat this as a stem and return concrete node and edge paths. |
| `prefix` | `string` | no | Optional key, document, vector-key, or node-id prefix. |
| `primitive` | `ArrowExportPrimitive` | yes | Product primitive to export. |

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`StatusResponse<ArrowExportResult>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.product_space`](https://stratadb.org/e/invalid_argument.engine.product_space)
- [`unsupported.executor.arrow_feature_disabled`](https://stratadb.org/e/unsupported.executor.arrow_feature_disabled)
- [`invalid_argument.executor.arrow_format`](https://stratadb.org/e/invalid_argument.executor.arrow_format)
- [`invalid_argument.executor.arrow_empty_export`](https://stratadb.org/e/invalid_argument.executor.arrow_empty_export)
- [`invalid_argument.executor.arrow_vector_dimension`](https://stratadb.org/e/invalid_argument.executor.arrow_vector_dimension)
- [`invalid_argument.executor.arrow_graph`](https://stratadb.org/e/invalid_argument.executor.arrow_graph)
- [`invalid_argument.executor.arrow_collection`](https://stratadb.org/e/invalid_argument.executor.arrow_collection)
- [`unavailable.executor.arrow_io`](https://stratadb.org/e/unavailable.executor.arrow_io)
- [`internal.executor.arrow`](https://stratadb.org/e/internal.executor.arrow)

## Invocation

- CLI: `strata arrow export`
- Wire type: `arrow_export`
