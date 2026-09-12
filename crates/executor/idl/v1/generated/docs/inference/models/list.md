---
title: "List catalog models"
description: "List catalog inference models."
source: strata-core@1.2.1
section: inference
---

Lists every model in Strata's built-in catalog as a terminal page. Each entry reports the model's task (embed, generate, or rank), architecture, default quantization, embedding dimension, HuggingFace repository, approximate artifact size, and whether the model artifact is already present in the local model directory. Use `inference models local` to see only the downloaded models, or `inference models pull` to fetch one.

## Examples

List the built-in model catalog (spans embedding, generation, ranking).

### CLI

```console
$ strata inference models list
miniLM	embed	bert	f16	unavailable	45 MB
nomic-embed	embed	nomic-bert	q8_0	unavailable	260 MB
bge-m3	embed	xlm-roberta	q8_0	unavailable	1.2 GB
gemma-embed	embed	gemma3	q8_0	unavailable	320 MB
gpt2	generate	gpt2	q8_0	unavailable	178 MB
tinyllama	generate	llama	q4_k_m	unavailable	670 MB
qwen3:1.7b	generate	qwen3	q8_0	unavailable	2 GB
gemma3:1b	generate	gemma3	q4_k_m	unavailable	780 MB
phi3.5	generate	phi3	q4_k_m	unavailable	2.4 GB
llama3.1:8b	generate	llama	q4_k_m	unavailable	4.6 GB
qwen3:8b	generate	qwen3	q4_k_m	unavailable	4.7 GB

11 model(s) unavailable: this build cannot run local models -- a bare name like these means a local model. Add local execution with `strata inference install-local`, or name a cloud model instead (`openai:<model>`, `google:<model>`, `anthropic:<model>`).
```

### Wire

```json
{"type":"inference_models_list"}
```

## Parameters

_No parameters._

## Returns

`Page<ModelInfo>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)

## Invocation

- CLI: `strata inference models list`
- Wire type: `inference_models_list`
