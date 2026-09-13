---
title: "Report inference readiness"
description: "Report what this binary can do before anything is attempted."
source: strata-core@1.2.2
section: inference
---

Reports the inference facts that are knowable up front: whether this build can execute local models and download them, which providers are compiled in, which of those have an API key and where it was found, and how many catalogued models are already on disk.

Released binaries ship the cloud providers and leave local model execution out, so `local_execution` is false in them and every catalogued local model is unavailable until `strata inference install-local` adds it; `local_remedy` says so. Knowing that from `status` is the point: previously the only way to find out was to run an operation and read the failure.

`key_source` names where a key was read from: the environment variable, or the config file's path when the key was set with `strata config set <provider>.api_key`. The runtime asks the environment first and the config file second, and reports the one that answered, so `key_source` is exactly what a `generate` or `embed` call would use. The key itself is never returned.

`base_url` is the endpoint a call to the provider would reach, and `base_url_source` names what redirected it: the provider's own environment variable (`base_url_env_var` — `OPENAI_BASE_URL`, `ANTHROPIC_BASE_URL`, `GOOGLE_GEMINI_BASE_URL`, with the same meaning as in each provider's SDK), or the config file's path when it was set with `strata config set <provider>.base_url`. The environment wins over the file, as for a key. When neither is set, `base_url` is the provider's public endpoint and `base_url_source` is absent. A provider that is not reached over HTTP reports none of the three.

The model directory is shared by every database on the machine, so a model downloaded once is available to all of them.

## Examples

Check what this build can do before running an inference command.

### CLI

```console
$ strata inference status  # reports build, providers, keys, and on-disk models without attempting a call
build	cloud providers only
	local models: this build runs cloud models only, and a bare model name means a local model. Either name a cloud model instead — `openai:<model>`, `google:<model>` or `anthropic:<model>` — or run `strata inference install-local` to add local execution.

providers
  openai	no key -- `strata config set openai.api_key <key>`, or export OPENAI_API_KEY
  anthropic	no key -- `strata config set anthropic.api_key <key>`, or export ANTHROPIC_API_KEY
  google	no key -- `strata config set google.api_key <key>`, or export GOOGLE_API_KEY
  local	not in this build

models	…
```

`…` stands for a value that varies by run or by machine — an instant, a version, an id, a path.

### Wire

```json
{"type":"inference_status"}
```

## Parameters

_No parameters._

## Returns

`InferenceStatus`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)

## Invocation

- CLI: `strata inference status`
- Wire type: `inference_status`
