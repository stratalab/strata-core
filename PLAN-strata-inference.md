# Plan: strata-inference Crate in strata-core

## Goal

Create a new `strata-inference` workspace crate inside strata-core that:
1. Wraps llama.h via dynamic FFI for **local** embedding and generation
2. Adds **cloud provider** support (Claude, GPT, Gemini) for generation
3. Owns **model lifecycle** (registry, download, load/unload/cache)
4. Presents a **unified API** so strata-intelligence doesn't know or care whether inference is local or cloud
5. **Bundles pre-built libllama** for every platform — zero user friction, just works
6. **Runs everywhere** — from Raspberry Pi to Xeon servers, Linux/macOS/Windows, ARM and x86

The external strata-inference repo becomes archived. strata-core no longer depends on it.

---

## Architecture

```
strata-core workspace
│
├── strata-inference/          # NEW — inference provider abstraction
│   ├── llama/                 # llama.h FFI (local embedding + generation)
│   ├── provider/              # Cloud generation providers
│   ├── registry/              # Model catalog, download, resolution
│   ├── embed.rs               # EmbeddingEngine (always local)
│   ├── generate.rs            # GenerationEngine (local or cloud, unified)
│   └── error.rs               # InferenceError
│
├── strata-intelligence/       # Unchanged role — AI features for the database
│   └── (depends on strata-inference, manages DB-level state)
│
└── strata-executor/           # Unchanged — wires commands to handlers
    └── (depends on strata-intelligence)
```

**Dependency chain:** `strata-executor → strata-intelligence → strata-inference`

---

## Bundled libllama — Every Platform, Zero Friction

### Distribution Strategy

Pre-built `libllama` binaries ship alongside the strata binary in every release. Users never interact with llama.cpp directly. No cmake, no compiler, no setup — `cargo install stratadb` or download a release archive and it just works.

### Platform Matrix

| Platform | Arch | Library | GPU Acceleration | Target Use |
|----------|------|---------|-----------------|------------|
| Linux | x86_64 | `libllama.so` | CUDA + CPU | Servers, workstations, cloud VMs |
| Linux | aarch64 | `libllama.so` | CPU (NEON SIMD) | Raspberry Pi 4/5, ARM servers (Graviton, Ampere) |
| macOS | arm64 | `libllama.dylib` | Metal + CPU | Apple Silicon Macs (M1-M4) |
| macOS | x86_64 | `libllama.dylib` | CPU (AVX2) | Older Intel Macs |
| Windows | x86_64 | `llama.dll` | CUDA + CPU | Windows workstations, servers |
| Windows | aarch64 | `llama.dll` | CPU | Windows ARM devices (Surface Pro, Snapdragon) |

**Size:** ~3-5 MB per platform (single shared library).

llama.cpp compiles with optimal SIMD for each target:
- **x86_64**: AVX2/AVX-512 (auto-detected at runtime by ggml)
- **aarch64**: NEON (standard on all ARM64, including RPi 4/5)
- This means a Raspberry Pi 4 (4GB) can run MiniLM embeddings and small generation models (TinyLlama, Phi-3-mini) at usable speeds

### DynLib Loader — Cross-Platform

The `dl.rs` module handles library loading on all platforms:

```rust
// Unix (Linux + macOS): dlopen / dlsym / dlclose
#[cfg(unix)]
// Uses libc::dlopen, libc::dlsym, libc::dlclose

// Windows: LoadLibraryA / GetProcAddress / FreeLibrary
#[cfg(windows)]
// Uses winapi::um::libloaderapi::{LoadLibraryA, GetProcAddress, FreeLibrary}
```

Library file extension is platform-aware:
- Linux: `.so`
- macOS: `.dylib`
- Windows: `.dll`

### Installation Layout

**Unix (Linux + macOS):**
```
~/.strata/
├── bin/
│   └── strata              # CLI binary
├── lib/
│   └── libllama.{so,dylib} # Bundled llama.cpp library
└── models/
    └── *.gguf              # Downloaded models
```

System-wide alternative:
```
/usr/local/bin/strata
/usr/local/lib/strata/libllama.{so,dylib}
```

**Windows:**
```
%LOCALAPPDATA%\strata\
├── bin\
│   └── strata.exe
├── lib\
│   └── llama.dll
└── models\
    └── *.gguf
```

Or alongside the exe (simplest for Windows):
```
C:\Program Files\Strata\
├── strata.exe
├── llama.dll              # Same directory = Windows finds it automatically
└── models\
```

### Library Discovery Order

strata-inference finds libllama at runtime:

1. **`LLAMA_LIB_PATH` env var** — exact path override (developers/testing)
2. **Same directory as executable** — Windows convention, works everywhere
3. **`../lib/` relative to executable** — Unix convention (`bin/` + `lib/` layout)
4. **`~/.strata/lib/`** (Unix) or `%LOCALAPPDATA%\strata\lib\` (Windows) — user-local
5. **System search** — `dlopen("libllama.so")` / `LoadLibraryA("llama.dll")` default paths

This means:
- Standard installs just work on every platform
- `cargo install` works if libllama is placed in `~/.strata/lib/`
- Windows users who unzip a release archive get `strata.exe` + `llama.dll` side by side — done
- Developers can override with `LLAMA_LIB_PATH`
- If missing, clear error: *"libllama not found. Reinstall strata or set LLAMA_LIB_PATH."*

### CI/CD Build Matrix

```yaml
strategy:
  matrix:
    include:
      - os: ubuntu-latest
        target: x86_64-unknown-linux-gnu
        llama_flags: "-DGGML_CUDA=ON"
        lib_name: libllama.so

      - os: ubuntu-latest  # cross-compile
        target: aarch64-unknown-linux-gnu
        llama_flags: ""    # CPU only, NEON auto-enabled
        lib_name: libllama.so

      - os: macos-14       # M1 runner
        target: aarch64-apple-darwin
        llama_flags: "-DGGML_METAL=ON"
        lib_name: libllama.dylib

      - os: macos-13       # Intel runner
        target: x86_64-apple-darwin
        llama_flags: ""
        lib_name: libllama.dylib

      - os: windows-latest
        target: x86_64-pc-windows-msvc
        llama_flags: "-DGGML_CUDA=ON"
        lib_name: llama.dll

      - os: windows-latest  # cross-compile or native ARM runner
        target: aarch64-pc-windows-msvc
        llama_flags: ""
        lib_name: llama.dll
```

Build steps per platform:
```bash
# Clone llama.cpp at pinned commit
git clone --depth 1 --branch <pinned-tag> https://github.com/ggml-org/llama.cpp

# Build
cmake -B build $LLAMA_FLAGS -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=ON
cmake --build build --config Release --target llama

# Package: strata binary + libllama in release archive
# Linux/macOS: .tar.gz    Windows: .zip
```

The llama.cpp commit is pinned in a `LLAMA_CPP_VERSION` file in the repo root. Bumping this triggers a rebuild of all 6 platform binaries.

### Release Archive Contents

```
strata-v0.7.0-linux-x86_64.tar.gz
├── bin/strata
├── lib/libllama.so
└── README.md

strata-v0.7.0-windows-x86_64.zip
├── strata.exe
├── llama.dll
└── README.md

strata-v0.7.0-macos-arm64.tar.gz
├── bin/strata
├── lib/libllama.dylib
└── README.md
```

---

## API Keys: Database Configuration

### Design

API keys are stored in the **database configuration** — not env vars, not config files. Since Strata is an embedded database, the database itself is the natural place for configuration.

strata-intelligence reads keys from the database config and passes them to strata-inference at engine creation time. strata-inference receives keys as parameters — it never reads env vars or config files for keys.

### User Experience

```
> CONFIGURE SET provider "anthropic"
OK

> CONFIGURE SET anthropic_api_key "sk-ant-api03-..."
OK

> CONFIGURE SET default_model "claude-sonnet-4-20250514"
OK

> GENERATE "Explain quantum computing in one sentence"
Quantum computing uses quantum mechanical phenomena like superposition...
```

Or for local:
```
> CONFIGURE SET provider "local"
OK

> CONFIGURE SET default_model "qwen3:8b"
OK

> GENERATE "Explain quantum computing in one sentence"
```

### Configuration Keys

| Key | Values | Default | Description |
|-----|--------|---------|-------------|
| `provider` | `local`, `anthropic`, `openai`, `google` | `local` | Default generation provider |
| `default_model` | model name/identifier | none | Default model for generation |
| `anthropic_api_key` | string | none | Claude API key |
| `openai_api_key` | string | none | GPT API key |
| `google_api_key` | string | none | Gemini API key |

### Flow

```
User: GENERATE "prompt"
  ↓
Executor: reads provider + model + api_key from db config
  ↓
Intelligence: GenerateModelState::get_or_load(provider, model, api_key)
  ↓
Inference:
  if provider == "local":
    GenerationEngine::from_registry(model)      # loads GGUF via llama.cpp
  else:
    GenerationEngine::cloud(provider, api_key, model)  # stores HTTP config
  ↓
engine.generate(request) → response
```

### Where Config Lives

strata-engine's `Database` already has a configuration system. The config keys above are stored there. strata-intelligence reads them via `db.config()` or equivalent. This means:
- Keys persist across restarts (stored in the database)
- Keys are per-database (different databases can use different providers)
- No separate config file to manage
- Keys can be set programmatically via any SDK (Rust, Python, Node)

---

## Module Layout

```
crates/inference/
├── Cargo.toml
└── src/
    ├── lib.rs                 # Public exports
    ├── error.rs               # InferenceError enum
    │
    ├── llama/                 # llama.h FFI layer
    │   ├── mod.rs             # Re-exports
    │   ├── dl.rs              # DynLib — cross-platform dlopen/LoadLibrary (~100 LOC)
    │   ├── ffi.rs             # #[repr(C)] structs, function pointers (~500 LOC)
    │   └── context.rs         # Model/context lifecycle, tokenization (~250 LOC)
    │
    ├── provider/              # Generation providers
    │   ├── mod.rs             # GenerationProvider trait + ProviderKind enum
    │   ├── local.rs           # llama.cpp local generation
    │   ├── anthropic.rs       # Claude API (messages endpoint)
    │   ├── openai.rs          # GPT API (chat completions)
    │   └── google.rs          # Gemini API (generateContent)
    │
    ├── registry/              # Model catalog + download
    │   ├── mod.rs             # ModelRegistry, ModelInfo
    │   ├── catalog.rs         # Static catalog entries
    │   └── download.rs        # HuggingFace download with progress
    │
    ├── embed.rs               # EmbeddingEngine (local-only, wraps llama FFI)
    └── generate.rs            # GenerationEngine (dispatches local vs cloud)
```

---

## Public API Surface

### EmbeddingEngine (always local)

```rust
pub struct EmbeddingEngine { /* wraps llama context */ }

impl EmbeddingEngine {
    pub fn from_registry(name: &str) -> Result<Self, InferenceError>;
    pub fn from_gguf(path: impl AsRef<Path>) -> Result<Self, InferenceError>;
    pub fn embed(&self, text: &str) -> Result<Vec<f32>, InferenceError>;
    pub fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, InferenceError>;
    pub fn embedding_dim(&self) -> usize;
}
```

Internally uses the llama.h FFI with `embeddings: true`, `pooling_type: MEAN`.
Thread-safe via internal Mutex (same pattern as existing LlamaCppEmbeddingEngine).

### GenerationEngine (local or cloud)

```rust
pub struct GenerationEngine { /* provider dispatch */ }

impl GenerationEngine {
    // Local (llama.cpp)
    pub fn from_registry(name: &str) -> Result<Self, InferenceError>;
    pub fn from_gguf(path: impl AsRef<Path>) -> Result<Self, InferenceError>;

    // Cloud
    pub fn cloud(
        provider: ProviderKind,
        api_key: String,
        model: String,
    ) -> Result<Self, InferenceError>;

    // Unified API — works for both local and cloud
    pub fn generate(
        &mut self,
        request: &GenerateRequest,
    ) -> Result<GenerateResponse, InferenceError>;

    // Tokenize/detokenize (local only, returns error for cloud)
    pub fn encode(&self, text: &str) -> Result<Vec<u32>, InferenceError>;
    pub fn decode(&self, ids: &[u32]) -> Result<String, InferenceError>;

    // Info
    pub fn is_local(&self) -> bool;
    pub fn provider(&self) -> ProviderKind;
}
```

### Request/Response Types (provider-agnostic)

```rust
#[derive(Debug, Clone)]
pub struct GenerateRequest {
    pub prompt: String,
    pub max_tokens: usize,            // default: 256
    pub temperature: f32,             // default: 0.0 (greedy)
    pub top_k: usize,                 // default: 0 (disabled), local only
    pub top_p: f32,                   // default: 1.0 (disabled)
    pub seed: Option<u64>,            // local only
    pub stop_sequences: Vec<String>,  // text-level stop sequences
    pub stop_tokens: Vec<u32>,        // token-level stops, local only
}

#[derive(Debug, Clone)]
pub struct GenerateResponse {
    pub text: String,
    pub stop_reason: StopReason,
    pub prompt_tokens: usize,
    pub completion_tokens: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopReason {
    StopToken,
    MaxTokens,
    ContextLength,
    Cancelled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderKind {
    Local,       // llama.cpp
    Anthropic,   // Claude
    OpenAI,      // GPT
    Google,      // Gemini
}
```

### ModelRegistry (moved from external strata-inference)

```rust
pub struct ModelRegistry { /* models_dir */ }

impl ModelRegistry {
    pub fn new() -> Self;                                          // ~/.strata/models/
    pub fn with_dir(dir: PathBuf) -> Self;
    pub fn models_dir(&self) -> &Path;
    pub fn resolve(&self, name: &str) -> Result<PathBuf, InferenceError>;
    pub fn pull(&self, name: &str) -> Result<PathBuf, InferenceError>;           // feature = "download"
    pub fn pull_with_progress(&self, name: &str, cb: impl Fn(u64, u64)) -> Result<PathBuf, InferenceError>;
    pub fn list_available(&self) -> Vec<ModelInfo>;
    pub fn list_local(&self) -> Vec<ModelInfo>;
}
```

### InferenceError

```rust
#[derive(Debug, thiserror::Error)]
pub enum InferenceError {
    #[error("llama.cpp: {0}")]
    LlamaCpp(String),

    #[error("provider error: {0}")]
    Provider(String),

    #[error("registry error: {0}")]
    Registry(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("not supported: {0}")]
    NotSupported(String),
}
```

---

## Provider Implementation Details

### Local Provider (llama.cpp)

Reuses the llama.h FFI layer we already built in the external strata-inference repo.
Files to port: `ffi.rs`, `context.rs`, `dl.rs` (adapted — self-contained, cross-platform).

**generate()** flow:
1. Convert `GenerateRequest.prompt` → token IDs via llama tokenizer
2. Build sampler chain from temperature/top_k/top_p/seed
3. Prefill prompt tokens
4. Autoregressive decode loop with stop conditions
5. Return `GenerateResponse`

**embed()** flow:
1. Tokenize → batch → encode/decode → get_embeddings → L2 normalize

### Cloud Providers

Each cloud provider implements a simple function that:
1. Builds an HTTP request body (JSON) with the prompt and parameters
2. Sends via `ureq` (already in workspace, synchronous)
3. Parses the JSON response
4. Maps to `GenerateResponse`

#### Anthropic (Claude)

```
POST https://api.anthropic.com/v1/messages
Headers: x-api-key, anthropic-version: 2023-06-01, content-type: application/json
Body: { model, max_tokens, messages: [{ role: "user", content: prompt }], temperature, top_p }
Response: { content: [{ text }], stop_reason, usage: { input_tokens, output_tokens } }
```

Mapping: `stop_reason: "end_turn" → StopToken, "max_tokens" → MaxTokens`

#### OpenAI (GPT)

```
POST https://api.openai.com/v1/chat/completions
Headers: Authorization: Bearer {key}, content-type: application/json
Body: { model, max_tokens, messages: [{ role: "user", content: prompt }], temperature, top_p, seed }
Response: { choices: [{ message: { content }, finish_reason }], usage: { prompt_tokens, completion_tokens } }
```

Mapping: `finish_reason: "stop" → StopToken, "length" → MaxTokens`

#### Google (Gemini)

```
POST https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={key}
Headers: content-type: application/json
Body: { contents: [{ parts: [{ text: prompt }] }], generationConfig: { maxOutputTokens, temperature, topP, topK } }
Response: { candidates: [{ content: { parts: [{ text }] }, finishReason }], usageMetadata: { promptTokenCount, candidatesTokenCount } }
```

Mapping: `finishReason: "STOP" → StopToken, "MAX_TOKENS" → MaxTokens`

---

## Feature Flags

```toml
[features]
default = ["local"]

# Local inference via llama.cpp (loads bundled libllama at runtime)
local = []

# Model download from HuggingFace
download = ["dep:ureq"]

# Cloud providers (each adds ureq + serde_json)
anthropic = ["dep:ureq", "dep:serde", "dep:serde_json"]
openai = ["dep:ureq", "dep:serde", "dep:serde_json"]
google = ["dep:ureq", "dep:serde", "dep:serde_json"]
```

### Feature Cascade (Root → Executor → Intelligence → Inference)

```
Root Cargo.toml:
  embed           → strata-executor/embed
  embed-cloud     → strata-executor/embed-cloud    (adds cloud providers)

strata-executor:
  embed           → strata-intelligence/embed
  embed-cloud     → strata-intelligence/embed-cloud

strata-intelligence:
  embed           → strata-inference/local + strata-inference/download
  embed-cloud     → strata-inference/anthropic + strata-inference/openai + strata-inference/google
```

---

## Changes to strata-intelligence

strata-intelligence remains a **thin adapter** — model lifecycle state for the database.
Changes are minimal:

1. **Cargo.toml**: Change dependency from external `strata-inference` to workspace `strata-inference`
   ```toml
   # Before:
   strata-inference = { path = "../../../strata-inference", optional = true, features = ["registry"] }
   # After:
   strata-inference = { path = "../inference", optional = true, features = ["local", "download"] }
   ```

2. **lib.rs re-exports**: Update import paths (types may have moved)
   - `GenerationConfig` → `GenerateRequest`
   - `GenerationOutput` → `GenerateResponse`
   - Add re-export of `ProviderKind`

3. **embed/mod.rs**: No changes needed — same `EmbeddingEngine` API

4. **generate.rs**: Updates for new types + cloud support
   - `GenerateModelState` now stores either local or cloud engines
   - `get_or_load()` accepts provider config (provider kind, API key, model name)
   - Reads provider/key from database config: `db.config("provider")`, `db.config("anthropic_api_key")`, etc.
   - `with_engine()` stays the same pattern

5. **embed/download.rs**: No changes — same `ModelRegistry` API

---

## Changes to strata-executor

Minimal changes:

1. **handlers/generate.rs**: Build `GenerateRequest` instead of `GenerationConfig`
   - Map the same fields (max_tokens, temperature, top_k, top_p, seed, stop_tokens)
   - Read provider/api_key/model from database config when not specified in command

2. **command.rs**: Extend `Generate` command variant
   ```rust
   Generate {
       model: Option<String>,    // CHANGED: optional, falls back to db config default_model
       prompt: String,
       // ... existing fields ...
       provider: Option<String>,   // NEW: override db config provider
   }
   ```
   Note: No `api_key` in the command — keys come from database config only.

3. **handlers/embed.rs**: No changes needed
4. **handlers/embed_hook.rs**: No changes needed

---

## Cargo.toml for New Crate

```toml
[package]
name = "strata-inference"
version.workspace = true
edition.workspace = true
rust-version.workspace = true
authors.workspace = true
license.workspace = true
repository.workspace = true
publish = false
description = "Inference engine for Strata — local (llama.cpp) and cloud (Claude, GPT, Gemini)"

[features]
default = ["local"]
local = []
download = ["dep:ureq"]
anthropic = ["dep:ureq", "dep:serde", "dep:serde_json"]
openai = ["dep:ureq", "dep:serde", "dep:serde_json"]
google = ["dep:ureq", "dep:serde", "dep:serde_json"]

[dependencies]
thiserror = { workspace = true }
tracing = { workspace = true }

# Optional: HTTP client (for download + cloud providers)
ureq = { workspace = true, optional = true }

# Optional: JSON serialization (for cloud provider request/response)
serde = { workspace = true, optional = true }
serde_json = { workspace = true, optional = true }
```

---

## Implementation Order

### Phase 1: Core crate + local inference (port what we built)
```
1. Create crates/inference/ directory structure
2. Port dl.rs — cross-platform (Unix dlopen + Windows LoadLibraryA)
3. Port ffi.rs (adapt imports from dl.rs)
4. Port context.rs (adapt imports)
5. Port embed.rs → src/embed.rs (EmbeddingEngine wrapping llama context)
6. Port local generation → src/provider/local.rs
7. Create src/generate.rs (GenerationEngine with local-only dispatch)
8. Create src/error.rs
9. Create src/lib.rs with public exports
10. Port registry/ (catalog.rs, download.rs, mod.rs)
11. Wire into workspace Cargo.toml
```

### Phase 2: Update strata-intelligence + strata-executor
```
12. Update strata-intelligence/Cargo.toml → point to workspace crate
13. Update strata-intelligence imports (type renames)
14. Update strata-executor generate handler (GenerateRequest)
15. Verify: cargo build --features embed
16. Verify: cargo test --features embed (with bundled libllama)
```

### Phase 3: Cloud providers
```
17. Create provider trait + dispatch in generate.rs
18. Implement anthropic.rs
19. Implement openai.rs
20. Implement google.rs
21. Add CONFIGURE SET commands for provider/api_key/model in executor
22. Update generate handler to read db config for cloud dispatch
23. Tests: unit tests with mock HTTP (or integration with real keys)
```

### Phase 4: Bundled libllama + Release Packaging
```
24. Add CI workflow to build libllama for all 6 platform targets
25. Add LLAMA_CPP_VERSION pinning file
26. Update release packaging to produce platform archives with libllama included
27. Update installer scripts (if any) to place libllama in correct location
28. Test on all platforms: Linux x86/ARM, macOS Intel/ARM, Windows x86/ARM
```

### Phase 5: Cleanup
```
29. Remove external strata-inference dependency from workspace
30. Update root Cargo.toml features (embed-cloud, etc.)
31. Archive external strata-inference repo
```

---

## What Moves, What Stays

| Component | From | To |
|-----------|------|----|
| llama.h FFI (ffi.rs, context.rs) | external strata-inference | `crates/inference/src/llama/` |
| DynLib (dl.rs) | external strata-inference | `crates/inference/src/llama/dl.rs` (+ Windows support) |
| EmbeddingEngine | external strata-inference | `crates/inference/src/embed.rs` |
| GenerationEngine | external strata-inference | `crates/inference/src/generate.rs` |
| ModelRegistry + catalog | external strata-inference | `crates/inference/src/registry/` |
| SamplingConfig | external strata-inference | merged into `GenerateRequest` |
| Cloud providers | new | `crates/inference/src/provider/` |
| EmbedModelState | strata-intelligence | stays in strata-intelligence |
| GenerateModelState | strata-intelligence | stays in strata-intelligence |
| Auto-embed hook | strata-executor | stays in strata-executor |
| API key storage | new | database config (strata-engine) |
| libllama binary | user's responsibility | bundled in release archive (all 6 platforms) |

---

## What Gets Dropped

The following from the external strata-inference repo are **not ported** (llama.cpp handles all of this):

- `src/gguf/` — GGUF parser (llama.cpp parses GGUF internally)
- `src/tokenizer/` — BPE/WordPiece tokenizers (llama.cpp tokenizes internally)
- `src/tensor/` — Tensor types (llama.cpp manages tensors)
- `src/backend/cpu.rs` — CPU compute backend
- `src/backend/metal/` — Metal GPU backend
- `src/backend/cuda/` — CUDA GPU backend
- `src/model/` — Transformer layer implementation
- `src/engine/sampler.rs` — Sampling logic (llama.cpp samples internally)
- `src/bin/` — CLI binaries (strata-cli is the CLI)

**Total code dropped: ~15,000 LOC** of native inference code, replaced by ~800 LOC of FFI bindings + ~400 LOC of cloud providers.

---

## Key Design Decisions

1. **Prompt-based request (not messages)**: `GenerateRequest` uses a `prompt: String` field. strata-intelligence or strata-executor is responsible for formatting messages into a prompt string before calling generate(). This keeps strata-inference simple — it doesn't need to know about chat templates. Cloud providers wrap the prompt as a single user message internally.

2. **ureq not reqwest**: The workspace already uses `ureq 3` (synchronous HTTP). Cloud API calls are synchronous and blocking. This is fine because generation is already a blocking operation (the generate handler holds a per-model mutex). No need to add tokio/async complexity.

3. **Embedding is always local**: No cloud embedding endpoint. Local llama.cpp embedding with MiniLM is fast (~30ms), free, and privacy-preserving. Cloud embedding APIs exist but add latency and cost for marginal quality improvement on an RPi you get embeddings for free with no internet required.

4. **Provider selection at engine creation time**: You create a `GenerationEngine` for either local or a specific cloud provider. You don't switch providers per-request. This matches the existing `GenerateModelState` pattern where each model name maps to one cached engine.

5. **Token-level operations are local-only**: `encode()` and `decode()` return `Err(NotSupported)` for cloud providers. This is inherent — cloud APIs don't expose tokenizer internals.

6. **API keys in database config**: Keys are stored in the database via `CONFIGURE SET`. strata-intelligence reads them from the database at engine creation time and passes them to strata-inference. strata-inference never reads env vars or config files — it receives keys as constructor parameters. This means different databases can use different providers/keys.

7. **Bundled libllama for every platform**: Pre-built libllama ships with every strata release for all 6 targets (Linux x86/ARM, macOS Intel/ARM, Windows x86/ARM). No cmake, no compiler, no user action. CI builds from a pinned llama.cpp commit. strata-inference finds the library relative to the executable at runtime.

8. **Pinned llama.cpp version**: A `LLAMA_CPP_VERSION` file in the repo root specifies the exact llama.cpp commit/tag. The `#[repr(C)]` struct layouts in `ffi.rs` must match this version. Bumping the version triggers rebuilds of all 6 platform binaries and a review of struct layouts.

9. **Runs everywhere**: From a Raspberry Pi 4 running MiniLM embeddings over NEON SIMD, to a Xeon server with CUDA running Qwen3-8B, to a MacBook with Metal, to a Windows laptop. Same binary, same API, same user experience. This is the differentiator — Redis can't do inference, and no other embedded DB ships with bundled AI that works offline on every platform.
