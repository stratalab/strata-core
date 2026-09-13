---
title: "Import Arrow file"
description: "Import an Arrow-compatible file into a product primitive."
source: strata-core@1.2.2
section: arrow
---

Imports an Arrow-compatible file (Parquet, CSV, or JSONL) into a product primitive on the selected branch and space. Rows are written through the standard batch commands, so the import commits like any other write. Returns a summary of the target primitive, the input file, and the imported, skipped, and batch counts.

Status commands return a scalar or compact status payload and do not mutate database state.

## Examples

Import a primitive's rows from a Parquet file written by export.

### CLI

```console
$ strata kv put greeting hello
created greeting
$ strata command run --command-json '{"format":"parquet","path":"/tmp/exports/kv.parquet","primitive":"kv","type":"arrow_export"}'
exported 1 row of kv to … (1.9 kB)
$ strata kv delete greeting
deleted greeting
$ strata arrow import /tmp/exports/kv.parquet --target kv  # Rows are keyed by their source column; kv restores greeting=hello.
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
| `collection` | `string` | no | Target vector collection for vector imports. |
| `file_path` | `string` | yes | Input file path. |
| `format` | `ArrowFileFormat` | no | Input file format. Defaults to extension detection. |
| `graph` | `string` | no | Target graph for graph imports. |
| `key_column` | `string` | no | Optional key column override. |
| `target` | `ArrowImportTarget` | yes | Product primitive to import into. |
| `value_column` | `string` | no | Optional value, document, or embedding column override. |

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`StatusResponse<ArrowImportResult>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.product_space`](https://stratadb.org/e/invalid_argument.engine.product_space)
- [`unsupported.executor.arrow_feature_disabled`](https://stratadb.org/e/unsupported.executor.arrow_feature_disabled)
- [`invalid_argument.executor.arrow_format`](https://stratadb.org/e/invalid_argument.executor.arrow_format)
- [`invalid_argument.executor.arrow_input_missing`](https://stratadb.org/e/invalid_argument.executor.arrow_input_missing)
- [`invalid_argument.executor.arrow_key_column`](https://stratadb.org/e/invalid_argument.executor.arrow_key_column)
- [`invalid_argument.executor.arrow_value_column`](https://stratadb.org/e/invalid_argument.executor.arrow_value_column)
- [`invalid_argument.executor.arrow_collection`](https://stratadb.org/e/invalid_argument.executor.arrow_collection)
- [`invalid_argument.executor.arrow_embedding_type`](https://stratadb.org/e/invalid_argument.executor.arrow_embedding_type)
- [`invalid_argument.executor.arrow_encoding`](https://stratadb.org/e/invalid_argument.executor.arrow_encoding)
- [`invalid_argument.executor.arrow_json_key`](https://stratadb.org/e/invalid_argument.executor.arrow_json_key)
- [`invalid_argument.executor.arrow_non_finite_float`](https://stratadb.org/e/invalid_argument.executor.arrow_non_finite_float)
- [`invalid_argument.executor.arrow_base64`](https://stratadb.org/e/invalid_argument.executor.arrow_base64)
- [`invalid_argument.executor.arrow_vector_key`](https://stratadb.org/e/invalid_argument.executor.arrow_vector_key)
- [`invalid_argument.executor.arrow_event`](https://stratadb.org/e/invalid_argument.executor.arrow_event)
- [`invalid_argument.executor.arrow_graph`](https://stratadb.org/e/invalid_argument.executor.arrow_graph)
- [`not_found.engine.vector_collection`](https://stratadb.org/e/not_found.engine.vector_collection)
- [`unavailable.executor.arrow_io`](https://stratadb.org/e/unavailable.executor.arrow_io)
- [`internal.executor.arrow`](https://stratadb.org/e/internal.executor.arrow)

## Invocation

- CLI: `strata arrow import`
- Wire type: `arrow_import`
