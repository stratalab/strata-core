---
title: "Read one config value"
description: "Read one sanitized configuration value by key."
source: strata-core@1.2.1
section: admin
---

Returns one sanitized configuration value from the allowlist by key, or a null value when the key is not recognized. Only allowlisted, non-sensitive keys are served; an empty key is rejected with `invalid_argument.engine.config_key`.

Optional reads distinguish present data from missing data. When version or timestamp facts exist on the executor output, SDK mappings should preserve them.

## Examples

Read one configuration value by key, or nothing if unknown.

### CLI

```console
$ strata config get-key missing
(nil)
```

### Wire

```json
{"key":"missing","type":"configure_get_key"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `key` | `string` | yes | Config key. |

## Returns

`Maybe<string>` — a miss returns nothing rather than raising.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`invalid_argument.engine.config_key`](https://stratadb.org/e/invalid_argument.engine.config_key)

## Invocation

- CLI: `strata config get-key`
- Wire type: `configure_get_key`
