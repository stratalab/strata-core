# Inference Architecture

Status: current — describes shipped 1.2.x behaviour (#3134)

## Purpose

This document defines the target architecture for `inference`, the layer
that executes model calls for Strata.

Inference is intentionally much smaller than storage or engine.
The current `strata-inference` crate is already close to the right boundary:
it owns local model execution, cloud provider adapters, embeddings, reranking,
model registry behavior, and model download support. It does not depend on
engine or storage, and it does not own Strata product semantics.

The V1 job is therefore not an invasive rewrite. The job is to make the
boundary explicit, harden the unsafe/native and network surfaces, and give
intelligence a stable lower layer for model execution.

## Related Documents

Architecture anchors:

1. [strata-v1-architecture.md](./strata-v1-architecture.md)
2. [engine-architecture.md](./engine-architecture.md)
3. [runtime-resource-profile-architecture.md](./runtime-resource-profile-architecture.md)
4. [v1-error-and-diagnostics-contract.md](./v1-error-and-diagnostics-contract.md)
5. [v1-testing-and-conformance-plan.md](./v1-testing-and-conformance-plan.md)
6. [v1-engineering-standards.md](./v1-engineering-standards.md)

Engine and product contracts that consume inference:

1. [engine/retrieval-and-derived-state-contract.md](./engine/retrieval-and-derived-state-contract.md)
2. [engine/product-pathway-conformance-plan.md](./engine/product-pathway-conformance-plan.md)
3. [docs/product/pathways/retrieval-and-intelligence.md](../product/pathways/retrieval-and-intelligence.md)

[intelligence-architecture.md](./intelligence-architecture.md) sits
between engine and inference. Intelligence owns model-dependent
Strata behavior. Inference owns provider execution.

## Requirement Language

1. Must means V1 inference architecture is incomplete without it.
2. Should means expected for V1 unless a later architecture decision records a
   clear deferral.
3. May means allowed but not required for V1.

## Product Role

Inference exists so Strata can support:

1. Local model-backed generation.
2. Cloud model-backed generation.
3. Embedding generation.
4. Cross-encoder reranking.
5. Retrieval augmentation through intelligence.
6. Search recipe stages that need expansion, reranking, or answer generation.
7. Future Autosearch workflows that run many bounded model-assisted trials.

Inference must not become a database layer. It should know models,
providers, requests, responses, and execution diagnostics. It should not know
branches, storage spaces, recipes, search indexes, IPC sessions, StrataHub
identity, or database commits.

## Current Codebase Evidence

The current crate is `crates/inference`.

Verified high-level shape:

1. `strata-inference` has no normal dependency on `strata-engine`,
   `strata-storage`, `strata-core`, or `strata-intelligence`.
2. The normal dependency graph is small: `thiserror` and `tracing`, plus
   feature-gated provider, download, and native-build dependencies.
3. `crates/intelligence` is the current intentional consumer. Executor and CLI
   consume inference-facing types through intelligence.
4. The crate already has feature gates for `local`, `download`, `anthropic`,
   `openai`, and `google`.
5. `cargo check -p strata-inference --no-default-features` succeeds.

Current files and responsibilities:

1. `src/lib.rs`
   - `GenerateRequest`
   - `GenerateResponse`
   - `StopReason`
   - `ProviderKind`
   - `InferenceEngine`
   - `parse_model_spec`

   The free loaders `load`, `load_embedder` and `load_ranker` were removed
   with the resolver: every model is reached through `InferenceRuntime`,
   which resolves the spec once and hands the loaders a resolved model.

2. `src/resolve.rs`
   - `InferenceRuntime::resolve` — the one answer to "can this model be used
     here, and if not, why not?" (`ResolvedModel`, `ModelSource`,
     `Availability`, `require_ready`)
   - identity-first check order: malformed → unknown → wrong task → not
     built → network → key → not downloaded
   - design: `docs/design/inference-model-resolution.md`

3. `src/generate.rs`
   - `GenerationEngine`
   - local generation dispatch
   - cloud generation dispatch
   - provider selection

4. `src/embed.rs`
   - `EmbeddingEngine`
   - local llama.cpp embedding
   - batch embedding
   - embedding dimension and health checks

5. `src/cloud_embed.rs`
   - cloud embedding for OpenAI and Google

6. `src/rank.rs`
   - local cross-encoder ranking

7. `src/provider/`
   - local provider wrapper
   - Anthropic adapter
   - OpenAI adapter
   - Google adapter
   - provider request/response JSON mapping

8. `src/llama/`
   - llama.cpp FFI definitions
   - context lifecycle
   - tokenization
   - local generation, embedding, and ranking helpers

9. `src/registry/`
   - model catalog
   - aliases and quantization variants
   - model directory resolution through `STRATA_MODELS_DIR` or default user
     model directory
   - optional model download support

10. `build.rs`
   - feature-gated local llama.cpp native build
   - vendor build configuration
   - skip hook for check-only builds through
     `STRATA_INFERENCE_SKIP_LLAMA_CPP_BUILD_FOR_CHECK`

## Binding V1 Decisions

1. **Inference has no dependency on engine, storage, or intelligence.**
   Dependency direction is one-way: intelligence may call inference.
   Inference must not call back into Strata data layers.

2. **Inference owns provider execution, not Strata semantics.**
   It can generate text, embed text, rank passages, load models, and report
   provider/model diagnostics. It must not decide search recipes, branch
   behavior, autoembedding policy, RAG prompts, IPC command semantics, or
   persistence layout.

3. **Intelligence is the Strata-aware model layer.**
   Intelligence decides when to ask for embeddings, expansion, reranking,
   or generation. Inference only performs the requested model operation.

4. **The V1 inference API remains synchronous.**
   Strata's public product APIs may choose their own threading model. The
   inference crate should not force async into the lower model execution
   boundary for V1. Cloud provider adapters use blocking HTTP at this layer; if
   a future async product API exists, the bridge belongs above inference.

5. **No hidden database-open network calls.**
   Cloud provider calls happen only when an inference request is executed.
   Model downloads happen only behind the download feature and through an
   explicit or clearly diagnosable model acquisition path. Opening a Strata
   database must not implicitly call model providers.

6. **Local llama.cpp is the only V1 native unsafe boundary.**
   All unsafe FFI calls, raw pointer lifetimes, native context ownership, and
   `Send` or `Sync` claims must stay isolated under the local runtime module.
   This is a target invariant: current pre-cutover helpers that contain unsafe
   outside `local/` must be folded into that module during implementation.

7. **Cloud providers are optional feature-gated adapters.**
   A minimal no-default build must compile without cloud provider code and
   without the local native build. Product builds may choose larger feature
   sets.

8. **Credentials are caller-owned secrets.**
   Inference may accept API keys and read environment variables where the
   loader contract says so. It must not persist secrets, put secrets in Debug
   output, or write secrets into model registry metadata.

9. **Provider-neutral request/response types are the stable boundary.**
   Provider-specific mapping belongs inside provider adapters. Unsupported
   request knobs must be surfaced as capability facts or diagnostics before V1
   freeze; silently ignoring important requested behavior is not acceptable for
   stable product semantics.

10. **Model registry is a local artifact resolver, not a StrataHub client.**
    Registry behavior can map names, aliases, tasks, quantization variants, and
    local files. StrataHub dataset clone, fleet identity, and cloud sync are
    outside inference.

11. **Inference errors must become structured enough for upper layers.**
    The current string-wrapping `InferenceError` shape is acceptable evidence,
    but V1 needs stable classes for invalid request, unsupported provider,
    unsupported operation, unsupported request parameter, missing API key,
    provider authentication failure, provider rate limit, provider timeout,
    provider unavailable, provider malformed response, local native runtime
    failure, local model load failure, missing model, registry corruption,
    download disabled, download failed, download verification failed, and IO
    failure.

12. **Runtime resource profiles may provide hints, not policy.**
    Engine or intelligence may pass resolved budgets and preferences down.
    Inference may consume hints such as context size, batch size, thread
    count, memory budget, GPU preference, timeout, and provider selection.
    Inference does not classify the host or own product-wide budgets.

13. **The inference crate default is minimal.**
    V1 inference should compile without selecting a provider runtime by
    default. Product crates may opt into `local` or cloud providers explicitly,
    but the lower crate should not surprise lightweight consumers, wasm builds,
    sandboxed agents, or CI with a native llama.cpp build.

14. **V1 model specs use deterministic first-colon provider parsing.**
    The stable V1 grammar is `provider:model`, split at the first colon.
    The provider segment is one of the closed set of provider names
    (`local`, `openai`, `anthropic`, `google`); the parser
    (`parse_model_spec`) is lenient about how it is spelled — it trims the
    spec and the segment and matches the name case-insensitively, so
    `OpenAI:gpt-4o` and ` openai : gpt-4o ` name the same model. A spec
    whose first segment is not a provider name is a local model name in
    full — the catalog's own names are colon-shaped (`qwen3:1.7b`,
    `tinyllama:q8_0`), so an unrecognised prefix is not an error (#3222).
    Everything after the provider's colon is the model name: provider-opaque
    text that may contain `/`, `-`, `.`, further `:`, or provider-defined
    characters, passed through as written (an empty name, or an empty spec,
    is `inference.invalid_request`). Task selection is not encoded in the
    string; it comes from the operation path: generate, embed, or rank.
    The post-V1 OpenAI-compatible endpoint grammar is reserved as
    `openai-compatible:<endpoint-id>:<model>`.

    This rule states the grammar and nothing about how a parsed spec
    resolves. What each spec form yields — which availability, which error
    code, in which build and environment — is decided by the resolver and
    proven cell by cell by `crates/inference/tests/resolution_matrix.rs`,
    the authoritative statement of resolution behaviour; this rule does not
    restate its cells (#3257). The user-facing list of spec forms is the one
    in `crates/executor/idl/v1/prose/commands/inference.capability.md`, from
    which the generated reference derives.

15. **Embedding and ranking get explicit operation DTOs.**
    V1 should add `EmbedRequest`, `EmbedResponse`, `RankRequest`, and
    `RankResponse`. The current trait methods may remain convenience methods,
    but intelligence needs operation metadata, provider/model identity,
    dimensions, item counts, warnings, degraded settings, and per-item
    diagnostics. V1 embed and rank responses are allowed to contain item-level
    success or item-level failure outcomes. Provider-level failures, missing
    models, authentication failures, and malformed provider responses fail the
    whole operation.

16. **Material user intent must not be silently ignored.**
    If a caller explicitly sets a request knob and the provider cannot honor
    it, the adapter must reject the request or return a structured hard
    diagnostic. Default or irrelevant knobs may be omitted quietly. This is
    especially important for seed, grammar or constrained output, stop tokens,
    timeouts, and context limits.

17. **User-actionable inference failures belong in the global V1 registry.**
    Stable product-facing failures should use an `inference.*` error family.
    Raw provider bodies, raw llama.cpp messages, and low-level native details
    remain inference-local context.

18. **Model artifacts are machine-local by default.**
    Model binaries should not live inside Strata databases by default. The
    registry uses `STRATA_MODELS_DIR` when set and otherwise a user-level
    Strata model directory such as `~/.strata/models`. Databases store model
    specs, recipe references, and provenance, not copied model binaries.

19. **V1 supports timeouts, not guaranteed cancellation.**
    Cloud providers should have explicit request timeouts. Local cancellation
    is not a V1 product promise unless the local runtime supports it cleanly.
    `StopReason::Cancelled` may remain reserved for implementations that can
    prove cancellation semantics. If a cached local engine times out or becomes
    unhealthy, the cache entry must be evicted before the next request for the
    same model spec.

20. **Real model smoke tests are opt-in.**
    Ordinary CI should run compile, parser, registry, provider-mapping, and
    fake-provider tests. Tests requiring real GGUF model artifacts or live
    provider calls are manual, nightly, or explicitly environment-gated.

21. **Cloud ranking is deferred.**
    Ranking remains in the provider contract, but V1 cloud providers may return
    stable `unsupported_operation` diagnostics. Local cross-encoder ranking is
    sufficient for the V1 minimum.

22. **llama.cpp requires a focused unsafe audit before V1 freeze.**
    The audit must cover FFI signatures and layouts, null checks,
    model/context ownership, drop order, buffer lifetimes, `Send`/`Sync`
    claims, mutex boundaries, panic behavior, and lifecycle stress tests for
    load, generate, embed, and rank. The audit report should live at
    `docs/audits/llama-ffi-unsafe-audit.md`, require a second reviewer, and
    reconcile the crate-level unsafe policy: inference denies unsafe code
    outside the `local` feature/module boundary and allows audited unsafe only
    there.

23. **External on-prem model runtimes are post-V1.**
    vLLM, NVIDIA NIM, Ollama, LM Studio, llama.cpp server, and other
    OpenAI-compatible local or private endpoints are strategically important,
    but they are not part of the V1 minimum. V1 should preserve a clean provider
    adapter shape so an `openai-compatible` endpoint provider can be added
    later without changing intelligence or engine architecture.

24. **Inference hides provider execution from intelligence.**
    Inference is responsible for turning provider-specific execution into
    Strata-neutral outputs: generated text, token counts, embeddings, ranking
    scores, capability facts, and diagnostics. Intelligence must not need
    to understand whether those outputs came from embedded llama.cpp, a cloud
    provider, or a future OpenAI-compatible local/private endpoint.

25. **Network policy is enforced before provider execution.**
    Engine runtime profile or request context decides whether network use is
    allowed. Intelligence passes the resolved policy with the operation.
    Inference enforces it before issuing cloud HTTP or model downloads and
    reports `failed_precondition.network_disabled` through the global error
    contract when the request would leave the machine.

## Responsibilities

Inference owns:

1. Provider-neutral model operation DTOs.
2. Provider identity and model spec parsing.
3. Capability reporting for generation, embedding, and ranking.
4. Local llama.cpp model/context lifecycle.
5. Cloud provider request construction and response parsing.
6. Model registry lookup and local model path resolution.
7. Optional model download mechanics.
8. Embedding vector production.
9. Cross-encoder reranking scores.
10. Generation responses and stop reasons.
11. Provider-local diagnostics and errors.
12. Secret redaction in all Debug and Display paths that can contain
    credentials.
13. Feature-gated compilation envelopes.
14. Test utilities for fake providers and deterministic model responses, where
    needed by intelligence tests.

Inference does not own:

1. Strata database open behavior.
2. Storage, WAL, manifest, checkpoint, or snapshot behavior.
3. Branches, versions, time travel, diffs, merges, or commits.
4. EntityRef, storage-space IDs, or row encoding.
5. Search recipes, recipe provenance, search index storage, or retrieval
   planning.
6. Autoembedding policy or shadow-vector writeback.
7. RAG prompt policy or context selection.
8. Autosearch strategy.
9. IPC transport or command authorization.
10. CLI UX.
11. StrataHub clone, publish, fleet, or sync behavior.
12. Persistent secret storage.

## Target Crate Shape

The exact file names can change during implementation, but the target structure
should be domain-shaped and small:

```text
crates/inference/
  Cargo.toml
  build.rs                    # local native build only
  src/
    lib.rs                    # public re-exports only
    api/                      # request, response, provider kind, capabilities
    error/                    # typed errors and diagnostics
    model/                    # model specs, tasks, resolved model identity
    registry/                 # catalog, local model store, optional download
    provider/                 # provider trait plus cloud adapters
      anthropic.rs
      google.rs
      openai.rs
    local/                    # llama.cpp runtime and FFI boundary
      ffi.rs
      context.rs
      generation.rs
      embedding.rs
      ranking.rs
    testkit/                  # feature-gated fake providers and fixtures
```

The current crate does not need to be reorganized immediately into this exact
tree. The important target is conceptual:

1. Public API types are separate from provider implementations.
2. Error and diagnostics are a module, not a bag of strings.
3. Local native code is isolated.
4. Cloud provider adapters are isolated.
5. Registry and download mechanics are isolated.
6. Test fixtures do not leak into the production API.

The target shape follows the V1 engineering standards. Roadmap labels and
cleanup-era labels must not become inference module names, feature flags, test
names, errors, telemetry fields, or public APIs. Temporary `inference`
package naming is build-branch scaffolding only; code inside the crate should
use permanent model/provider vocabulary.

## Public API Boundary

The V1 public inference boundary should contain:

1. `ProviderKind`
2. `ModelTask`
3. `ModelInfo`
4. `ModelSpec`
5. `ResolvedModel`
6. `parse_model_spec`
7. `GenerateRequest`
8. `GenerateResponse`
9. `EmbedRequest`
10. `EmbedResponse`
11. `EmbedItemOutcome`
12. `RankRequest`
13. `RankResponse`
14. `RankItemOutcome`
15. `InferenceCapability`
16. `Generator`
17. `Embedder`
18. `Reranker`
19. `InferenceEngine`
20. `InferenceError`

The current crate has `GenerateRequest`, `GenerateResponse`, `ProviderKind`,
`ModelInfo`, `ModelTask`, `InferenceEngine`, and operation-specific engines.
It does not yet have explicit request/response DTOs for embed and rank. V1
should add them if intelligence needs stable structured diagnostics,
operation metadata, or per-item failures.

The primary intelligence-facing traits are task-specific:

1. `Generator` accepts `GenerateRequest` and returns `GenerateResponse`.
2. `Embedder` accepts `EmbedRequest` and returns `EmbedResponse`.
3. `Reranker` accepts `RankRequest` and returns `RankResponse`.

`InferenceEngine` may remain as an advanced aggregate trait or compatibility
surface, but intelligence should load the narrow task trait it needs. An
autoembedding worker should load an `Embedder`, not a full engine that might
fail later because embedding is unsupported.

`InferenceCapability` is a provider/model/task capability declaration. Its V1
shape should include supported operations, supported request knobs, token or
context limits where known, batch limits where known, embedding dimension where
known, whether network is required, whether authentication is required,
download requirements, and timeout behavior. Consumers use it to reject
unsupported user intent before execution and to populate diagnostics; it is not
a second model registry.

`EmbedResponse` and `RankResponse` use item outcomes so intelligence can
retry, drop, or quarantine only failed items when the provider supports partial
results. Whole-operation failures remain errors.

## Provider Contract

A provider adapter must declare:

1. Provider kind.
2. Supported operations:
   - generate
   - embed
   - rank
3. Supported request knobs:
   - max tokens
   - temperature
   - top-k
   - top-p
   - seed
   - stop sequences
   - stop tokens
   - grammar or constrained output
4. Token and context limits when known.
5. Timeout behavior.
6. Retry classification for provider failures.
7. Authentication requirement.
8. Whether requests leave the local machine.

Provider adapters should not silently discard material user intent. If a field
cannot be honored, the adapter should either reject the request or return a
diagnostic that intelligence can expose in search or generation stats.

Provider adapters may translate Strata-neutral request DTOs into provider JSON,
but provider JSON should remain private to the adapter.

## Local Runtime

The local runtime owns llama.cpp integration.

V1 requirements:

1. Unsafe FFI remains isolated under the local runtime module.
2. Public safe wrappers document ownership, lifetime, and thread-safety
   assumptions.
3. Any `Send` or `Sync` implementation over native pointers is justified by a
   local invariant and tested where possible.
4. Generation, embedding, and ranking contexts may use different llama.cpp
   execution paths if the underlying model type requires it.
5. Local engines should expose health checks for poisoned or inconsistent
   native state.
6. Build configuration is feature-gated.
7. Check-only builds can bypass native compilation where the workspace needs
   that behavior.

The current code already uses mutex-protected local embedding state and a
separate llama.cpp context layer. Before V1, the local runtime needs a focused
unsafe audit.

## Cloud Runtime

Cloud providers are optional.

V1 requirements:

1. Cloud provider code compiles only when the corresponding feature is enabled.
2. API keys are accepted as caller inputs or read from documented environment
   variables.
3. API keys never appear in Debug or Display output.
4. HTTP request construction is deterministic and unit-testable without live
   network calls.
5. Provider errors are mapped into stable inference error classes.
6. Timeouts are explicit.
7. Rate limits, authentication failures, missing credentials, unavailable
   providers, malformed responses, and unsupported operations are distinct
   enough for upper layers.
8. Cloud calls are never made from database open, storage recovery, or engine
   maintenance paths.

Provider-specific model names can remain provider-specific strings. V1 does
not need a universal model taxonomy.

## Model Registry And Downloads

The model registry maps user-facing model names to local artifacts.

V1 registry responsibilities:

1. Resolve model aliases.
2. Resolve model tasks:
   - generation
   - embedding
   - ranking
3. Resolve quantization variants where relevant.
4. Resolve local model paths.
5. Report model metadata.
6. Detect missing local artifacts.
7. Validate downloaded artifacts when size or checksum metadata exists.
8. Keep failed or partial downloads from being mistaken for valid models.

Download behavior is optional and feature-gated.

Download requirements:

1. Downloads must be user-visible or caller-explicit.
2. Partial downloads must write through a temporary path and publish only after
   validation.
3. Failed downloads should leave clear diagnostics and avoid corrupting an
   existing valid model file.
4. Download errors must distinguish unavailable network, HTTP failure, checksum
   mismatch, permission failure, and disk-space or IO failure where possible.

The registry is not a package manager and not a StrataHub dataset client. It
only resolves inference model artifacts.

Clone/import behavior should be predictable when model artifacts are absent on
the receiving machine. Source rows remain readable. Shadow-vector rows and
derived manifests may name the model that produced them, but inference
does not fetch that model implicitly. A retrieval or rebuild path that needs the
missing model reports `inference.missing_model`; engine/intelligence map that
to embedding-unavailable or embedding-model-mismatch behavior according to the
recipe and derived-state manifest.

## Runtime Resource Integration

Inference may consume resource hints from upper layers.

Useful hints include:

1. Maximum model memory.
2. Maximum context size.
3. Maximum batch size.
4. Thread count.
5. GPU preference.
6. Provider preference.
7. Per-request timeout.
8. Whether network use is allowed.
9. Whether model download is allowed.

Inference should not probe the full host and choose the product profile.
That belongs to runtime resource profile architecture above this layer. The
inference layer should only apply concrete resolved hints.

## Errors And Diagnostics

V1 inference errors should align with the V1 error and diagnostics contract.

Required error classes:

1. Invalid request.
2. Unsupported provider.
3. Unsupported operation.
4. Unsupported request parameter.
5. Missing API key.
6. Provider authentication failure.
7. Provider rate limit.
8. Provider quota exhausted (billing, not throttling).
9. Provider model not found (the provider does not serve that name).
10. Provider timeout.
11. Provider unavailable.
12. Provider malformed response.
13. Local native runtime failure.
14. Local model load failure.
15. Missing model.
16. Registry corruption.
17. Download disabled.
18. Download failed.
19. Download verification failed.
20. IO failure.

Required global starter codes:

1. `inference.invalid_request`
2. `inference.missing_model`
3. `inference.model_load_failed`
4. `inference.unsupported_provider`
5. `inference.unsupported_operation`
6. `inference.unsupported_parameter`
7. `inference.missing_api_key`
8. `inference.provider_auth_failed`
9. `inference.provider_rate_limited`
10. `inference.provider_quota_exhausted`
11. `inference.provider_model_not_found`
12. `inference.provider_timeout`
13. `inference.provider_unavailable`
14. `inference.provider_malformed_response`
15. `inference.download_disabled`
16. `inference.download_failed`
17. `inference.download_verification_failed`
18. `inference.local_runtime_failed`
19. `inference.registry_corrupt`
20. `inference.io_failure`

The authoritative class and retry-policy mapping lives in
`v1-error-and-diagnostics-contract.md`. Inference must not introduce a
new product-facing `inference.*` code without adding that global mapping.

Diagnostics should include:

1. Provider kind.
2. Model name or model alias.
3. Resolved local path when applicable.
4. Requested operation.
5. Feature gate required when a feature is disabled.
6. Whether the operation would use network.
7. Retry classification where known.

Diagnostics must not include:

1. API keys.
2. Raw authorization headers.
3. Full prompt text by default.
4. User document contents by default.

## Feature Flags

The target feature model should preserve a minimal build:

1. `default`
   Minimal provider-free build. Product crates opt into provider runtimes
   explicitly.

2. `local`
   Enables local llama.cpp generation, embedding, and ranking.

3. `download`
   Enables model artifact download.

4. `anthropic`
   Enables Anthropic generation.

5. `openai`
   Enables OpenAI generation and embeddings where implemented.

6. `google`
   Enables Google generation and embeddings where implemented.

7. `testkit`
   Enables fake providers and deterministic fixtures for upper-layer tests.
   This feature must be explicitly test-only and must not become a second
   production API.

8. `openai-compatible`
   Post-V1 provider adapter for user-configured OpenAI-compatible endpoints
   such as vLLM, NIM, Ollama, LM Studio, or llama.cpp server. V1 should reserve
   the extension point but does not need to ship this adapter or expose a
   working feature flag before implementation.

No-default builds are important for:

1. compile-time boundary checks,
2. consumers that only need model metadata,
3. builds that cannot compile native llama.cpp,
4. future browser or sandboxed targets where inference execution is delegated
   elsewhere.

## Testing And Conformance

Inference needs a smaller but sharper test plan than storage or engine.

Required tests:

1. No-default compile test.
2. Feature matrix compile tests:
   - `local`
   - `download`
   - `anthropic`
   - `openai`
   - `google`
   - `openai` + `google`
   - `anthropic` + `openai` + `google`
   - `local` + `download`
   - `local` + `openai`
   - maximum product build: `local`, `download`, `anthropic`, `openai`,
     `google`, and `testkit`
3. Dependency guard proving inference does not import engine, storage, or
   intelligence.
4. Provider request JSON golden tests.
5. Provider response parsing tests.
6. Provider error mapping tests.
7. API key redaction tests.
8. Model spec parser tests.
9. Model registry alias and quantization tests.
10. Missing model and corrupted model metadata tests.
11. Download partial-file and verification-failure tests when `download` is
    enabled.
12. Embed/rank item-outcome tests for partial item failures and whole-operation
    failures.
13. Local runtime constructor error tests that do not require real models.
14. Local smoke tests gated behind explicit model fixtures.
15. Unsafe FFI layout and lifecycle tests where practical.
16. Fake provider tests for intelligence search, RAG, and Autosearch
    conformance.

Live provider calls should not run in ordinary CI. They may exist as
opt-in tests that require explicit environment variables and are skipped by
default.

## V1 Minimum

The V1 inference minimum is:

1. No engine or storage dependency.
2. No-default build compiles.
3. Local feature builds where the llama.cpp vendor setup is present.
4. Provider-neutral generation request/response.
5. Provider-neutral embedding and ranking DTOs.
6. Local generation, embedding, and ranking behind the local feature.
7. Optional cloud generation for Anthropic, OpenAI, and Google.
8. Optional cloud embedding for providers that support it.
9. Model registry with local resolution.
10. Optional model download with safe publish semantics.
11. Secret-redacted provider adapters.
12. Structured enough errors for intelligence to map failures cleanly.
13. Fake provider/testkit support for deterministic upper-layer tests.

Post-V1 inference targets include:

1. OpenAI-compatible local/private endpoint adapter.
2. Endpoint capability declarations or discovery.
3. Named endpoint configuration.
4. Streaming support where upper layers need it.
5. Provider-specific compatibility diagnostics for OpenAI-compatible endpoints.

## Closed Design Questions

The first-pass draft closes these choices:

1. Provider runtimes are explicit features; the lower inference crate default
   is minimal.
2. V1 model specs use first-colon `provider:model` parsing with a
   case-insensitive, whitespace-tolerant provider segment; a spec without a
   provider prefix is a local model name in full (rule 14). Resolution
   outcomes are the resolution matrix's to state, not this document's.
3. Embedding and ranking get explicit request/response DTOs with item-level
   outcomes.
4. Explicit unsupported request knobs are rejected or surfaced as hard
   diagnostics.
5. User-actionable inference failures use the global `inference.*` error
   family.
6. Model artifacts live in the machine-level model directory, not inside
   databases by default.
7. V1 promises timeouts, not guaranteed cancellation.
8. Real model and live provider tests are opt-in.
9. Cloud ranking is deferred with stable unsupported-operation diagnostics.
10. llama.cpp FFI needs a focused unsafe audit before V1 freeze.
11. External OpenAI-compatible on-prem runtimes are post-V1, with only the
    provider extension point preserved in V1.
12. Intelligence-facing execution uses task-specific `Generator`, `Embedder`,
    and `Reranker` traits.
13. Streaming generation is post-V1 unless product requirements explicitly
    pull it forward before implementation.

The remaining inference design work is implementation detail: exact DTO field
names and exact fake-provider testkit shape.

## Implementation Stance

Inference should not repeat the engine/storage cleanup pattern. The
current boundary is already mostly healthy. The right implementation path is:

1. Keep the crate small.
2. Add the missing structured contracts.
3. Harden unsafe and network edges.
4. Add deterministic provider/testkit support for intelligence.
5. Preserve the no-engine/no-storage dependency boundary.

If the implementation starts creating Strata-specific concepts inside
inference, the design is drifting upward into intelligence.
