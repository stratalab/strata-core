# Epics & User Stories: strata-inference

Reference: [PLAN-strata-inference.md](./PLAN-strata-inference.md)

**Approach:** TDD — write tests first, then implement until they pass. After each epic, full review of implementation + test quality before moving to the next.

---

## Epic 1: Crate Scaffold & Core Types

**Goal:** A new `crates/inference/` workspace crate that compiles with `cargo build`. Public types defined, error enum in place, no functionality yet — just the skeleton.

### Story 1.1: Create crate and wire into workspace
- Create `crates/inference/Cargo.toml` with workspace metadata and feature flags
- Add `"crates/inference"` to root `Cargo.toml` workspace members
- Create `src/lib.rs` with module declarations
- Create `src/error.rs` with `InferenceError` enum
- **Tests:**
  - `cargo build` succeeds with no features
  - `cargo build --features local` succeeds
  - `cargo build --features anthropic,openai,google` succeeds
  - `cargo test` (workspace-wide) still passes — no regressions
- **Acceptance:** `strata-inference` appears as a workspace member, compiles on all feature combinations

### Story 1.2: Define public request/response types
- Create `GenerateRequest` with `Default` impl
- Create `GenerateResponse`
- Create `StopReason` enum
- Create `ProviderKind` enum
- **Tests (TDD — write these first):**
  - `GenerateRequest::default()` has expected values (max_tokens=256, temperature=0.0, top_k=0, top_p=1.0, seed=None, empty stop vecs)
  - `StopReason` has Display impl, all variants round-trip through to_string/from_string
  - `ProviderKind` equality, Debug, Clone, Copy all work
  - `GenerateResponse` can be constructed and fields accessed
- **Acceptance:** All types are public, documented, and tested

### Story 1.3: Define InferenceError with conversions
- `InferenceError` variants: `LlamaCpp`, `Provider`, `Registry`, `Io`, `NotSupported`
- Implement `From<std::io::Error>` for `InferenceError`
- Implement `Display` via thiserror
- Implement `Clone` (needed for caching errors in GenerateModelState)
- **Tests (TDD):**
  - Each variant formats correctly via Display
  - `io::Error` converts to `InferenceError::Io`
  - Clone produces equal error
  - Error messages contain the original context string
- **Acceptance:** Error type is complete, cloneable, displayable

### Epic 1 Review Gate
- [ ] `cargo build` — all feature combos compile
- [ ] `cargo test -p strata-inference` — all type tests pass
- [ ] `cargo test` (full workspace) — no regressions
- [ ] Review: Are types complete enough for downstream epics? Any missing fields?
- [ ] Review: Are Default impls sensible?

---

## Epic 2: Cross-Platform DynLib + llama.h FFI

**Goal:** Load `libllama.{so,dylib,dll}` at runtime on any platform, resolve all ~35 function pointers, validate struct layouts with compile-time assertions.

### Story 2.1: Cross-platform DynLib loader
- Create `src/llama/dl.rs`
- `#[cfg(unix)]` path: `dlopen` / `dlsym` / `dlclose` via `libc`
- `#[cfg(windows)]` path: `LoadLibraryA` / `GetProcAddress` / `FreeLibrary`
- Platform-aware library search: env var → exe-relative → user-local → system
- Platform-aware file extension (`.so` / `.dylib` / `.dll`)
- `Drop` impl calls dlclose/FreeLibrary
- **Tests (TDD):**
  - `DynLib::open("nonexistent_library")` returns descriptive error (not panic)
  - `DynLib::sym()` with invalid symbol returns error
  - Library search order is correct: env override takes priority
  - Platform extension logic: `.so` on linux, `.dylib` on macos, `.dll` on windows (compile-time test via cfg)
  - Error messages include the library name and platform-specific detail
- **Acceptance:** DynLib works on the current platform (Linux), Windows path compiles but can't be integration-tested without Windows

### Story 2.2: #[repr(C)] struct definitions
- Create `src/llama/ffi.rs`
- Define: `LlamaModelParams` (72 bytes), `LlamaContextParams` (136 bytes), `LlamaSamplerChainParams` (1 byte), `LlamaBatch` (56 bytes)
- Type aliases: `LlamaModel`, `LlamaContext`, `LlamaSampler`, `LlamaVocab`, `LlamaToken`
- Pooling type constant: `LLAMA_POOLING_TYPE_MEAN`
- **Tests (TDD):**
  - Compile-time size assertions: `assert!(size_of::<LlamaModelParams>() == 72)`
  - Compile-time size assertions: `assert!(size_of::<LlamaContextParams>() == 136)`
  - Compile-time size assertions: `assert!(size_of::<LlamaSamplerChainParams>() == 1)`
  - Compile-time size assertions: `assert!(size_of::<LlamaBatch>() == 56)`
  - Compile-time alignment checks for each struct
  - Type aliases are correct sizes (all pointers = 8 bytes on 64-bit)
- **Acceptance:** Struct layouts match llama.h exactly, verified by compile-time assertions

### Story 2.3: Function pointer loading + LlamaCppApi
- `load_sym!` macro for symbol resolution
- `LlamaCppApi` struct holding all ~35 function pointers + DynLib
- `LlamaCppApi::load()` — finds and loads libllama, resolves all symbols
- Runtime probe: after `model_default_params()`, verify `use_mmap` defaults to `true`
- `Drop` calls `llama_backend_free()`
- Safe wrapper methods for every function pointer
- **Tests (TDD):**
  - `LlamaCppApi::load()` with `LLAMA_LIB_PATH` pointing to real libllama succeeds
  - `LlamaCppApi::load()` with `LLAMA_LIB_PATH=""` and no system lib returns clear error
  - Runtime probe catches version mismatch (mock test: corrupt a default param)
  - All safe wrapper methods exist and are callable (type-checked at compile time)
  - `Drop` doesn't panic on double-free or after error
- **Edge case tests:**
  - Load with path containing spaces
  - Load with path containing unicode
  - Concurrent loads from multiple threads (should each get independent handle)

### Epic 2 Review Gate
- [ ] `cargo test -p strata-inference --features local` — all FFI tests pass
- [ ] `cargo test` (full workspace) — no regressions
- [ ] Review: All ~35 function pointers have safe wrappers?
- [ ] Review: Error messages are actionable (include path tried, dlopen error)?
- [ ] Review: No unsafe code leaks outside `dl.rs` and `ffi.rs`?
- [ ] Review: Windows `#[cfg]` path compiles (cross-check with `cargo check --target x86_64-pc-windows-msvc` if toolchain available)?

---

## Epic 3: Local Embedding Engine

**Goal:** `EmbeddingEngine` that loads a GGUF model via llama.cpp and produces L2-normalized embedding vectors. Thread-safe.

### Story 3.1: LlamaCppContext — model/context lifecycle
- Create `src/llama/context.rs`
- `LlamaCppContext` struct: api, model, ctx, vocab, n_embd, n_ctx, vocab_size, bos_id, eos_id, has_encoder
- `load_for_embedding(path)` — embeddings=true, pooling=MEAN
- `load_for_generation(path, ctx_override)` — embeddings=false, GPU layers=999
- `tokenize(text, add_special) -> Vec<LlamaToken>`
- `detokenize(tokens) -> String`
- `clear_memory()`
- `Drop` — free context then model (order matters)
- `unsafe impl Send` (only accessed through Mutex)
- **Tests (TDD — require libllama + model files):**
  - Load MiniLM for embedding — succeeds, n_embd=384
  - Load GPT-2 for generation — succeeds, vocab_size=50257
  - Load nonexistent file — returns InferenceError, not panic
  - Load with path to non-GGUF file — returns clear error
  - Tokenize "Hello world" — returns non-empty token vec
  - Tokenize empty string — returns empty vec
  - Tokenize with add_special=true vs false — different lengths (BOS/EOS added)
  - Detokenize round-trip: tokenize("hello") → detokenize → contains "hello"
  - Detokenize empty slice — returns empty string
  - clear_memory — doesn't panic, can be called multiple times
  - Drop — context freed before model (verify no segfault via running test)
- **Edge case tests:**
  - Tokenize very long text (>10K chars) — truncation works, no OOM
  - Tokenize text with unicode, emoji, null bytes
  - Multiple load-then-drop cycles — no memory leak (run under valgrind in CI)

### Story 3.2: EmbeddingEngine with Mutex
- Create `src/embed.rs`
- `EmbeddingEngine` wraps `Mutex<LlamaCppContext>`
- `from_gguf(path)` — loads for embedding
- `from_registry(name)` — resolves name via registry, then loads (blocked until Epic 5, stub for now)
- `embed(text) -> Result<Vec<f32>>` — tokenize, batch, encode/decode, get_embeddings, L2 normalize
- `embed_batch(texts)` — sequential embed per text, collect results
- `embedding_dim() -> usize`
- Thread-safe: verify Send + Sync at compile time
- **Tests (TDD — require libllama + MiniLM model):**
  - Embed "Hello world" — returns Vec<f32> of length 384
  - Embedding is L2-normalized: dot product with self ≈ 1.0
  - Embed empty string — returns zero vector (or error, define behavior)
  - Embed two similar texts — cosine similarity > 0.8
  - Embed two unrelated texts — cosine similarity < 0.5
  - embed_batch with 3 texts — returns 3 vectors, each length 384
  - embed_batch with empty slice — returns empty vec
  - embedding_dim() — returns 384 for MiniLM
  - Concurrent embed from 4 threads — all succeed (Mutex serializes correctly)
  - Embed after previous embed failed (e.g., bad input) — engine still usable
- **Edge case tests:**
  - Embed text longer than model context (>512 tokens for MiniLM) — truncation works, returns valid embedding
  - Embed single character — works
  - Embed only whitespace — defined behavior (zero vec or valid embedding)
  - Embed with unicode/emoji — works
  - Rapid sequential embeds (100x) — no memory growth, no crash

### Epic 3 Review Gate
- [ ] `cargo test -p strata-inference --features local` — all embedding tests pass
- [ ] Review: Cosine similarity between strata-inference and old external engine > 0.999?
- [ ] Review: Are error paths tested (not just happy path)?
- [ ] Review: Thread safety actually tested with concurrent threads, not just compile-time assert?
- [ ] Review: No memory leaks on repeated embed calls?
- [ ] Review: Embedding of unusual inputs (empty, long, unicode) all have defined behavior?

---

## Epic 4: Local Generation Engine

**Goal:** `GenerationEngine` with local llama.cpp backend. Greedy and stochastic sampling. Streaming support.

### Story 4.1: Local generation provider
- Create `src/provider/mod.rs` with `ProviderKind` enum and provider dispatch types
- Create `src/provider/local.rs` — wraps `LlamaCppContext` for generation
- Build sampler chain: temp=0 → greedy, temp>0 → top_k + top_p + temp + dist(seed)
- Decode loop: prefill → sample → stop checks (EOG, stop_tokens, max_tokens, context length)
- **Tests (TDD — require libllama + GPT-2 model):**
  - Greedy generation (temp=0): "Hello, my name is" → deterministic output, same every time
  - Greedy generation matches llama.cpp reference output exactly
  - Stochastic generation (temp=0.8): same prompt produces different output with different seeds
  - Same seed → same output (reproducibility)
  - max_tokens=1 — generates exactly 1 token
  - max_tokens=0 — returns empty response (or error, define behavior)
  - Stop tokens: provide EOS token ID → stops at EOS
  - Stop tokens: provide a custom token ID → stops when that token generated
  - Context length exceeded — returns StopReason::ContextLength
  - Empty prompt — returns InferenceError
  - Prompt longer than context — returns InferenceError with clear message
- **Edge case tests:**
  - Generate with top_k=1 — equivalent to greedy
  - Generate with top_p=0.0 — equivalent to greedy (or defined behavior)
  - Generate with temperature=100.0 — doesn't crash (extreme but valid)
  - Generate with negative temperature — clamped to 0 or returns error
  - Repeated generation calls on same engine — KV cache cleared properly each time
  - Prompt with only special tokens — defined behavior

### Story 4.2: GenerationEngine with provider dispatch
- Create `src/generate.rs`
- `GenerationEngine` enum dispatch: `Local(LocalProvider)` (cloud variants added in Epic 7)
- `from_gguf(path)` — creates local engine
- `from_registry(name)` — resolves via registry (stubbed until Epic 5)
- `generate(request) -> GenerateResponse` — delegates to provider
- `encode(text) -> Vec<u32>` — local tokenizer
- `decode(ids) -> String` — local detokenizer
- `is_local()` → true, `provider()` → `ProviderKind::Local`
- **Tests (TDD):**
  - `from_gguf` + `generate` — end-to-end works
  - `is_local()` returns true
  - `provider()` returns `ProviderKind::Local`
  - `encode("Hello")` returns non-empty vec of u32
  - `decode(encode("Hello"))` round-trips to original text (approximately — tokenizer may normalize)
  - `encode` then `decode` preserves meaning for ASCII text
  - `GenerateResponse` fields populated correctly: text non-empty, stop_reason set, token counts > 0
  - `generate` with default `GenerateRequest` — works with sensible defaults
- **Edge case tests:**
  - `decode` with invalid token IDs — doesn't panic, returns lossy string or error
  - `encode` empty string — returns empty or error
  - Multiple sequential generates — each produces independent output (KV cache reset)

### Epic 4 Review Gate
- [ ] `cargo test -p strata-inference --features local` — all generation tests pass
- [ ] Review: Greedy output matches llama.cpp CLI exactly?
- [ ] Review: Sampling reproducibility with seeds actually verified?
- [ ] Review: All StopReason variants exercised in tests?
- [ ] Review: Error cases (empty prompt, oversized prompt) tested, not just happy path?
- [ ] Review: KV cache properly cleared between calls (no state leakage)?
- [ ] Review: Sampler chain freed after each generate call (no resource leak)?

---

## Epic 5: Model Registry

**Goal:** Catalog of known models, name resolution to local paths, download from HuggingFace.

### Story 5.1: Static catalog + name resolution
- Create `src/registry/catalog.rs` — `CATALOG: &[CatalogEntry]` with embedding and generation models
- Create `src/registry/mod.rs` — `ModelRegistry` struct, `ModelInfo` struct
- `ModelRegistry::new()` — default models dir (`~/.strata/models/` or `STRATA_MODELS_DIR`)
- `ModelRegistry::with_dir(dir)` — custom dir
- `resolve(name)` — parse "name", "name:variant", find in catalog, check if file exists locally
- `list_available()` — all catalog entries with is_local flag
- `list_local()` — only locally present models
- **Tests (TDD):**
  - `ModelRegistry::new()` — models_dir is `~/.strata/models/` (or env override)
  - `ModelRegistry::with_dir(tmp)` — uses provided dir
  - Resolve "miniLM" with model file present → returns path
  - Resolve "miniLM" with model file absent → returns error with download instructions
  - Resolve "nonexistent_model" → returns error listing available models
  - Resolve "qwen3:8b" → correct catalog entry
  - Resolve "qwen3:8b:q8_0" → correct variant
  - Resolve alias "qwen3-8b" → same as "qwen3:8b"
  - `list_available()` — returns all catalog entries, count > 0
  - `list_available()` — each entry has non-empty name, architecture, default_quant
  - `list_local()` with empty dir — returns empty vec
  - `list_local()` with one model present — returns exactly that model with is_local=true
  - `ModelInfo` fields populated correctly: task, embedding_dim (>0 for embed models, 0 for gen models)
- **Edge case tests:**
  - Resolve with empty string — error
  - Resolve with ":" only — error
  - Resolve with "model:variant:extra:parts" — error or ignores extra
  - Models dir doesn't exist — creates it or returns clear error
  - Models dir is read-only — resolve still works for listing, pull fails gracefully
  - Concurrent resolve from multiple threads — no race conditions
  - `STRATA_MODELS_DIR` env var overrides default

### Story 5.2: Model download from HuggingFace
- Create `src/registry/download.rs` (feature-gated behind `download`)
- `pull(name)` — download GGUF from HuggingFace to models_dir
- `pull_with_progress(name, callback)` — progress reporting
- URL pattern: `https://huggingface.co/{repo}/resolve/main/{file}`
- Temp file → rename on completion (atomic, no partial files)
- Lock file to prevent concurrent downloads of same model
- **Tests (TDD):**
  - `pull("miniLM")` with model already present — returns existing path, no HTTP call
  - `pull` error when network unavailable — clear error message (mock or offline test)
  - Pull creates file in correct location with correct name
  - Partial download (simulated interrupt) — temp file cleaned up, no corrupt model left
  - Lock file prevents duplicate concurrent downloads
  - Lock file cleaned up after successful download
  - Stale lock file (>30 min) is ignored and overwritten
  - `pull_with_progress` — callback receives increasing byte counts
  - `pull_with_progress` — total_bytes > 0 for known models
  - Pull unknown model name — error before any HTTP call
- **Edge case tests:**
  - Download to dir with spaces in path
  - Download when disk is full — error, no partial file
  - Lock file exists but process is dead — handles stale lock

### Story 5.3: Wire from_registry into EmbeddingEngine and GenerationEngine
- `EmbeddingEngine::from_registry(name)` — resolve + load
- `GenerationEngine::from_registry(name)` — resolve + load
- **Tests (TDD):**
  - `EmbeddingEngine::from_registry("miniLM")` — works end-to-end (model must be downloaded)
  - `GenerationEngine::from_registry("gpt2")` — works end-to-end
  - `from_registry("nonexistent")` — returns InferenceError::Registry with helpful message

### Epic 5 Review Gate
- [ ] `cargo test -p strata-inference --features local,download` — all registry tests pass
- [ ] Review: Catalog has all models from external strata-inference?
- [ ] Review: Name parsing handles all formats correctly (name, name:size, name:size:quant)?
- [ ] Review: Download is atomic (no partial files)?
- [ ] Review: Concurrent download protection actually works (test with threads)?
- [ ] Review: Error messages are user-friendly (include model names, available options)?
- [ ] Review: `from_registry` works end-to-end for both embedding and generation?

---

## Epic 6: Integration — Wire into strata-intelligence + strata-executor

**Goal:** Replace the external strata-inference dependency. All existing embedding and generation tests in strata-intelligence and strata-executor pass against the new crate.

### Story 6.1: Update strata-intelligence dependency
- Change `Cargo.toml`: `strata-inference = { path = "../inference", ... }`
- Update `lib.rs` re-exports: `GenerateRequest`, `GenerateResponse`, `ProviderKind`
- Update `embed/mod.rs` — `EmbedModelState` uses new `EmbeddingEngine`
- Update `generate.rs` — `GenerateModelState` uses new `GenerationEngine` + `GenerateRequest`
- Update `embed/download.rs` — uses new `ModelRegistry`
- **Tests:**
  - `cargo build -p strata-intelligence --features embed` — compiles
  - All existing `strata-intelligence` unit tests pass
  - `EmbedModelState::get_or_load` works with new engine
  - `GenerateModelState::get_or_load` works with new engine
  - `with_engine` helper works with new `GenerationEngine`

### Story 6.2: Update strata-executor handlers
- Update `handlers/generate.rs` — build `GenerateRequest` instead of `GenerationConfig`
- Update `handlers/models.rs` — uses new `ModelRegistry` types
- Verify `handlers/embed.rs` — no changes needed (same API)
- Verify `handlers/embed_hook.rs` — no changes needed
- **Tests:**
  - `cargo build --features embed` — full workspace compiles
  - `cargo test --features embed` — all executor tests pass
  - `cargo test` (no features) — all tests pass, no regressions
  - Generate command produces `GenerationResult` with correct fields
  - Tokenize/Detokenize commands still work

### Story 6.3: Remove external strata-inference dependency
- Remove path reference to `../../../strata-inference` from anywhere in workspace
- Ensure no import path references the old external crate
- **Tests:**
  - `cargo build --features embed` — succeeds without external repo
  - Move/rename external strata-inference dir → workspace still builds
  - Full `cargo test --features embed` passes

### Epic 6 Review Gate
- [ ] `cargo build --features embed` — workspace compiles
- [ ] `cargo test --features embed` — all tests pass (intelligence + executor + inference)
- [ ] `cargo test` (no features) — no regressions in non-embed code
- [ ] Review: No references to external strata-inference remain in any Cargo.toml?
- [ ] Review: All re-exports in strata-intelligence match the new types?
- [ ] Review: Generate handler correctly maps all fields to GenerateRequest?
- [ ] Review: Feature cascade correct (root → executor → intelligence → inference)?

---

## Epic 7: Cloud Generation Providers

**Goal:** Generate text via Claude, GPT, and Gemini APIs. Same `GenerationEngine.generate()` interface as local.

### Story 7.1: Provider trait + cloud dispatch in GenerationEngine
- Define internal provider dispatch (enum-based, not trait-object)
- `GenerationEngine::cloud(provider, api_key, model)` — constructor
- `is_local()` returns false for cloud engines
- `provider()` returns correct `ProviderKind`
- `encode()`/`decode()` return `Err(NotSupported)` for cloud
- **Tests (TDD):**
  - `GenerationEngine::cloud(Anthropic, key, model)` — constructs without error
  - `cloud_engine.is_local()` returns false
  - `cloud_engine.provider()` returns `ProviderKind::Anthropic`
  - `cloud_engine.encode("text")` returns `Err(NotSupported)`
  - `cloud_engine.decode(&[1,2,3])` returns `Err(NotSupported)`
  - Construction with empty API key — returns error
  - Construction with empty model name — returns error

### Story 7.2: Anthropic (Claude) provider
- Create `src/provider/anthropic.rs`
- Build JSON request for `POST /v1/messages`
- Parse response: extract text, stop_reason, token counts
- Map errors: HTTP 401 → "invalid API key", 429 → "rate limited", 500 → "server error"
- **Tests (TDD):**
  - Request JSON structure is correct (validate against API spec)
  - Response parsing: normal completion → text + StopReason::StopToken
  - Response parsing: max_tokens hit → StopReason::MaxTokens
  - Response parsing: empty content array → error
  - Error mapping: 401 → descriptive "invalid API key" error
  - Error mapping: 429 → descriptive "rate limited" error
  - Error mapping: 500 → includes error body from API
  - Error mapping: network timeout → descriptive error
  - Temperature=0 passed correctly to API
  - top_p=1.0 (disabled) — omitted from request or sent as 1.0
  - top_k and seed silently ignored (not supported by Anthropic)
  - Integration test (with real key, gated behind env var): generate a short response

### Story 7.3: OpenAI (GPT) provider
- Create `src/provider/openai.rs`
- Build JSON request for `POST /v1/chat/completions`
- Parse response: extract text from choices[0], finish_reason, usage
- Map errors: same pattern as Anthropic
- **Tests (TDD):**
  - Request JSON structure matches OpenAI spec
  - Response parsing: `finish_reason: "stop"` → StopReason::StopToken
  - Response parsing: `finish_reason: "length"` → StopReason::MaxTokens
  - Response parsing: empty choices array → error
  - Error mapping: 401, 429, 500 all produce descriptive errors
  - Seed is included in request when provided
  - top_k silently ignored (not supported by OpenAI chat completions)
  - Integration test (gated behind env var)

### Story 7.4: Google (Gemini) provider
- Create `src/provider/google.rs`
- Build JSON request for `POST /v1beta/models/{model}:generateContent`
- API key in query parameter (not header)
- Parse response: extract text from candidates[0], finishReason, usageMetadata
- **Tests (TDD):**
  - Request JSON structure matches Gemini spec
  - API key is in URL query param, not header
  - Response parsing: `finishReason: "STOP"` → StopReason::StopToken
  - Response parsing: `finishReason: "MAX_TOKENS"` → StopReason::MaxTokens
  - Response parsing: empty candidates → error
  - Response parsing: candidate with no content → error
  - Error mapping: 400 (bad request), 403 (forbidden), 429, 500
  - top_k IS supported by Gemini — included in generationConfig
  - Seed silently ignored (not supported by Gemini)
  - Integration test (gated behind env var)

### Epic 7 Review Gate
- [ ] `cargo test -p strata-inference --features anthropic,openai,google` — all provider tests pass
- [ ] Review: Request JSON matches each API's actual spec (verify against docs)?
- [ ] Review: All HTTP error codes mapped to descriptive errors?
- [ ] Review: Network timeout handled (not hanging forever)?
- [ ] Review: API keys not logged or included in error messages?
- [ ] Review: Unsupported parameters (top_k for OpenAI, seed for Gemini) handled gracefully?
- [ ] Review: Integration tests pass with real API keys (manual run)?

---

## Epic 8: Database Configuration for API Keys + Provider Selection

**Goal:** Users configure providers and API keys via `CONFIGURE SET`. strata-intelligence reads config from database and passes to strata-inference.

### Story 8.1: Configuration keys in strata-engine
- Add configuration keys to strata-engine's Database config system: `provider`, `default_model`, `anthropic_api_key`, `openai_api_key`, `google_api_key`
- Config values persist across restarts (stored in database)
- Config values are per-database instance
- **Tests (TDD):**
  - Set and get `provider` → returns same value
  - Set `provider` to invalid value → accepted (validation happens at use time) or rejected
  - Default `provider` when not set → `"local"`
  - Set and get API keys → returns same value
  - Config persists after database close and reopen
  - Different databases have independent configs

### Story 8.2: CONFIGURE SET/GET commands in executor
- Add `Command::ConfigureSet { key, value }` and `Command::ConfigureGet { key }`
- Handler reads/writes database config
- **Tests (TDD):**
  - `CONFIGURE SET provider "anthropic"` → OK
  - `CONFIGURE GET provider` → returns "anthropic"
  - `CONFIGURE SET anthropic_api_key "sk-..."` → OK
  - `CONFIGURE GET anthropic_api_key` → returns the key
  - Set unknown key → OK (generic config) or error (strict key set)
  - Get unset key → returns default or null

### Story 8.3: GenerateModelState reads config for cloud dispatch
- Update `strata-intelligence/generate.rs` — `get_or_load` reads provider/key from db config
- If provider=local → `GenerationEngine::from_registry(model)`
- If provider=anthropic/openai/google → `GenerationEngine::cloud(provider, key, model)`
- Key missing → clear error: "Set your API key with: CONFIGURE SET anthropic_api_key ..."
- **Tests (TDD):**
  - Config provider=local → local engine created
  - Config provider=anthropic + key set → cloud engine created
  - Config provider=anthropic + key missing → error with helpful message
  - Config provider=anthropic + empty key → error
  - Changing provider config and calling generate again → new engine type created
  - Model override in Generate command → overrides default_model config

### Epic 8 Review Gate
- [ ] `cargo test --features embed` — config tests pass
- [ ] Review: API keys stored securely in database (not in plain text logs)?
- [ ] Review: Error messages guide user to fix the issue (include CONFIGURE SET examples)?
- [ ] Review: Config changes take effect on next generate call (cached engine invalidated)?
- [ ] Review: Per-database isolation tested (two databases, different configs)?

---

## Epic 9: CI/CD — Build libllama for All Platforms

**Goal:** Automated CI builds libllama for all 6 platform targets. Release archives include the library.

### Story 9.1: LLAMA_CPP_VERSION pinning
- Create `LLAMA_CPP_VERSION` file in repo root with pinned commit/tag
- Document the pinning process and how to bump

### Story 9.2: CI workflow for libllama builds
- GitHub Actions workflow: `.github/workflows/build-libllama.yml`
- Matrix: 6 targets (linux x86/arm, macos intel/arm, windows x86/arm)
- Build steps: clone pinned llama.cpp → cmake → build → upload artifact
- Triggered on: push to main (if LLAMA_CPP_VERSION changed), manual dispatch, release tags

### Story 9.3: Release packaging
- Update release workflow to:
  - Build strata binary for each target
  - Download corresponding libllama artifact
  - Package together: `strata-v{version}-{platform}.{tar.gz,zip}`
- Archive layout matches the installation layout from the plan

### Story 9.4: Platform smoke tests
- CI job that runs on each platform after build:
  - Load libllama → verify symbol resolution
  - Load MiniLM → embed "test" → verify 384-dim output
  - Load GPT-2 → generate 5 tokens greedy → verify output matches expected
- **Tests:**
  - Linux x86_64: CUDA build loads and runs
  - Linux aarch64: NEON build loads and runs
  - macOS arm64: Metal build loads and runs
  - macOS x86_64: AVX2 build loads and runs
  - Windows x86_64: CUDA build loads and runs
  - Windows aarch64: CPU build loads and runs

### Epic 9 Review Gate
- [ ] All 6 platform builds succeed in CI
- [ ] Release archives have correct layout
- [ ] Smoke tests pass on all platforms
- [ ] Library discovery works in all installation scenarios (exe-relative, user-local, env override)
- [ ] Archive sizes are reasonable (~3-5 MB for libllama)

---

## Summary: Epic Dependencies

```
Epic 1 (Scaffold)
  ↓
Epic 2 (FFI)
  ↓
Epic 3 (Embedding) ──┐
  ↓                   │
Epic 4 (Generation) ──┤
  ↓                   │
Epic 5 (Registry) ────┘
  ↓
Epic 6 (Integration)
  ↓
Epic 7 (Cloud) ←── can start after Epic 6, independent of Epic 9
  ↓
Epic 8 (Config) ←── depends on Epic 7
  ↓
Epic 9 (CI/CD) ←── can start after Epic 2, independent of Epics 7-8
```

Epics 7+8 (cloud) and Epic 9 (CI/CD) can be worked in parallel after Epic 6.

---

## TDD Discipline

For every story:
1. **Write tests first** — define expected behavior before writing implementation
2. **Run tests — they should fail** (red)
3. **Implement the minimum code** to make tests pass (green)
4. **Refactor** — clean up while keeping tests green
5. **Add edge case tests** — unusual inputs, error paths, concurrency
6. **Review test quality** — tests should be:
   - **Not shallow**: test behavior, not just "function exists"
   - **Deterministic**: no flaky tests, seeds for randomness
   - **Isolated**: each test independent, uses temp dirs for file operations
   - **Fast**: unit tests < 1s each, integration tests < 10s each
   - **Named descriptively**: test name explains what it verifies

### Test Categories

| Category | Location | Requires | Run With |
|----------|----------|----------|----------|
| Unit (types, errors, catalog) | `src/*.rs` `#[cfg(test)]` | Nothing | `cargo test -p strata-inference` |
| FFI struct layout | `src/llama/ffi.rs` `#[cfg(test)]` | Nothing | `cargo test -p strata-inference --features local` |
| Integration (embedding) | `tests/embed.rs` | libllama + MiniLM model | `cargo test -p strata-inference --features local` |
| Integration (generation) | `tests/generate.rs` | libllama + GPT-2 model | `cargo test -p strata-inference --features local` |
| Integration (registry) | `tests/registry.rs` | Network or pre-downloaded models | `cargo test -p strata-inference --features local,download` |
| Cloud (mocked) | `src/provider/*.rs` `#[cfg(test)]` | Nothing | `cargo test -p strata-inference --features anthropic,openai,google` |
| Cloud (live) | `tests/cloud_live.rs` | API keys in env | `ANTHROPIC_API_KEY=... cargo test -p strata-inference --features anthropic -- cloud_live` |
| Workspace integration | `cargo test --features embed` | libllama + models | Full workspace |
