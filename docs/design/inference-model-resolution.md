# Inference: one resolver for model specs

**Status:** proposed 2026-09-09. Follows the #3124 design
(`inference-developer-experience.md`); nothing here is built. Tracking issue
**#3261** (slices S0–S4 and T). Every file:line below was read on `main` at
`98f8f324`.

The #3222 fix (PR #3259, "a non-provider prefix is a local model name") was a
25-line parser change. Reviewing it produced four new issues (#3255, #3256,
#3257, #3258), and inventorying the path for this document produced a fifth
(#3260). None of the five is a regression from #3259; every one was already
there. That ratio — one fix, five findings — is the signal that the inference
model-resolution path is not a set of bugs but one design gap with many
symptoms, and that fixing it one `/audit-fix` at a time will not converge.

This document does what #3216 asked for on one bounded surface: name the
mechanism that produces the defects, replace it, and leave behind an
*executable* contract that catches the next one.

---

## 1. The principle

> **One question, one answerer.** "Can this model run here — and if not, why?"
> is answered once, as data, and every surface renders that data. No surface
> re-derives the answer from a message string, a command field, or its own
> reading of the model directory.

Three corollaries, each of which today's code violates:

- **Codes come from the answer, not from prose.** An error code is chosen at
  the site that knows why the operation cannot proceed. It is never recovered
  afterwards by matching words in a message (#3216 pattern 2).
- **Surfaces render; they do not resolve.** The CLI's download offer, the
  `capability` payload, `status`, the human error line and the `--json`
  envelope all show the same `ResolvedModel`. None of them owns a second copy
  of the logic.
- **The contract is a table that runs.** The grammar and resolution rules live
  in a matrix test that enumerates spec forms × registry state × key state ×
  build × verb. Prose (architecture rule 14, IDL prose, README) points at the
  matrix; it does not restate cells.

---

## 2. The path today

A model spec reaches an inference call along three layers, and each layer
re-derives part of the answer on its own.

| Layer | What it decides | Where | Notes |
|---|---|---|---|
| spec → provider | `parse_model_spec` | `crates/inference/src/lib.rs:369` | first-colon split; non-provider prefix = local name (#3222) |
| model → availability | runtime + registry | `crates/inference/src/runtime.rs`, `registry/mod.rs` | downloaded? key? feature? — decided per entry point |
| collection → model | engine | `recorded_embedding_model[_at]`, called from `crates/executor/src/executor/vector.rs:223-224` | the `--text` paths never see a spec in the command |

Inside the middle layer, the runtime's public entry points do not agree with
each other about the first two questions:

| Entry point (`runtime.rs`) | parses spec? | directory it reads | existence check | key check |
|---|---|---|---|---|
| `capability` :415 | yes | `self.registry()` → honours `config.models_dir` | `registry.info()` (None on miss) | provider readiness |
| `status` :370 | per-provider | `self.registry()` | list_local | `provider_is_ready` :1293 |
| `list_models` / `list_local_models` :318/:323 | no | `self.registry()` | — | — |
| `pull_model` :328 | **no** (#3255) | `self.registry()` | `resolve_or_pull(raw string)` | — |
| `generate` / `chat` :466/:523 | yes | **`ModelRegistry::new()`** in `generate.rs:164` — ignores `config.models_dir` (#3260) | `registry.resolve` | env only, `lib.rs:511` |
| `embeddings` / `embed[_batch]` :588/:655/:704 | yes | **`ModelRegistry::new()`** in `embed.rs:82` (#3260) | `registry.resolve` | env only, `lib.rs:589` |
| `rank` :763 | yes | **`ModelRegistry::new()`** in `rank.rs:75` (#3260) | `registry.resolve` | env only |
| `tokenize` / `detokenize` :618/:639 | yes | as generate | as generate | — |
| `unload` :791 | yes | — | — | — |

So `capability` and `status` can report a model as present and runnable from
one directory while `generate` loads from another; `pull` looks up
`local:qwen3:1.7b` verbatim and fails on a string every other verb accepts.

The registry itself has one "downloaded" predicate (`model_file_is_downloaded`,
`registry/mod.rs:39`: regular file, non-empty) and one catalog
(`registry/catalog.rs:6`, 16 entries, case-insensitive `find_entry` :288). Those
are sound. What is missing is a single caller that composes them.

### 2.1 How a refusal gets its code

`InferenceError` (`crates/inference/src/error.rs:9`) has eight variants. Six are
`String`s; two are typed (`RegistryFailed { kind }`, `ProviderFailed { kind }`,
added by #3217). Construction sites on `main`:

| Variant | shape | sites |
|---|---|---|
| `Provider(String)` | string | 99 |
| `NotSupported(String)` | string | 63 |
| `LlamaCpp(String)` | string | 39 |
| `Registry(String)` | string | 34 |
| `InvalidSpec(String)` | string | 8 |
| `ProviderFailed { kind, .. }` | typed | 6 |
| `RegistryFailed { kind, .. }` | typed | 1 |
| `Io(String)` | string | 0 (#3252) |

For the string variants, `code()` (`error.rs:335`) recovers the code by
substring-matching the message: `registry_code` :435 maps "unknown model" and
"not found locally" to `inference.missing_model`, "download" to
`download_failed`, anything else to `registry_corrupt`; `not_supported_code`
:452 maps the word "provider" to `unsupported_provider`. The consequence is
visible in product code — a comment in `runtime.rs:1350-1356` explains that the
refusal text for a lean build must avoid the *word* "provider" so that the
classifier does not silently change its code. Message wording is load-bearing;
`provider_classification.rs` pins twenty-eight such checks.

This is exactly why #3256 exists: a catalog miss (`Registry("Unknown model
…")`, `registry/mod.rs:396`) and a catalogued-but-not-downloaded model
(`RegistryFailed { MissingModel }`, :245) are different facts that arrive at
the same code, because one of them was classified from prose.

### 2.2 What reaches the wire

`From<InferenceError> for ExecutorError` (`crates/executor/src/error.rs:521-543`)
takes `value.code()`, looks up the registry row (single authority since #3243)
and passes **`Vec::new()` for `details`**. Every inference error on the wire
carries zero structured details. The schema name
`strata.error.details.inference.v1` is declared on every row
(`error_registry.rs:16`, rows :450-626) and defined nowhere — a contract with no
implementation (#3216 pattern 1).

The CLI's download offer (`crates/cli/src/lib.rs:957-996`) therefore has to
reconstruct "why unavailable" itself: it compares the code to the literal
`b"inference.missing_model"` (:1019-1021) and takes the model name from the
command's own `model` field for five `Inference*` variants (:1024-1035). It
cannot serve `vector --text` (the model is in the collection record, #3226); it
offers names that are not in the catalog and could never be pulled (#3256);
and it re-issues the spec verbatim to `pull`, which does not parse it (#3255).

### 2.3 What the test lanes reach

`crates/executor/idl/v1/unreplayed-error-codes.yaml:53-72` lists **every**
`inference.*` code as unreplayed, with the reason "a live inference provider".
That is true for the nine `provider_*` codes. It is not true for
`missing_model`, `missing_api_key`, `invalid_request`, `unsupported_provider`,
`unsupported_operation` and `download_disabled`: those are resolution-time
refusals that need no network, no model file and no key — they need a resolver
that can be pointed at an empty temp directory. They are unreplayed because the
replay lane's `FakeInferenceService` (`idl_tooling/verify.rs:37-42`) fakes
*resolution* along with execution: it keys off fixed names (`fake-embed`,
`fake-generate`, `fake-rank`, `testkit.rs:374`) and never touches the parser or
the registry. The IDL's drift guards stop exactly where resolution begins.

---

## 3. The defects are symptoms

Grouped by the mechanism that produces them. Fixing a row without its root
leaves the root to produce the next row.

### Root A — no single resolver

| Issue | Symptom | Where the re-derivation lives |
|---|---|---|
| #3222 (fixed, #3259) | `qwen3:1.7b` rejected as unknown provider | parser guessed at catalog membership |
| #3255 | `pull local:qwen3:1.7b` fails `missing_model` | `pull_model` skips `parse_model_spec` (`runtime.rs:328`) |
| #3260 | `capability` says present, `generate` says missing | three loaders build their own `ModelRegistry::new()` |
| #3226 | no download offer for `vector --text` | CLI reads the model off the command, not the answer |
| #3221 (fixed, S3a) | library callers get `missing_api_key` for a configured key | key lookup was env-only inside inference; the config bridge was a CLI-side `set_var` (`cli/src/lib.rs:1054-1066`) |

### Root B — the code carries less than the resolver knew

| Issue | Symptom | Cause |
|---|---|---|
| #3256 | D8 offers to download names that are not in the catalog; refusal names `strata models list` (no such verb) | catalog-miss and not-downloaded share `missing_model`; hint is class-generic; verb spelled in prose |
| #3252 | `inference.io_failure` declared, documented, never produced | code exists in the registry, no site can raise it |
| #3216 p.2 | wording changes change codes | string variants + substring classifiers |
| (this doc) | zero `details` on every inference error | `From<InferenceError>` passes `Vec::new()`; schema name has no definition |

### Root C — facts restated by hand

| Issue | Restated fact | Copies |
|---|---|---|
| #3257 | spec grammar | rule 14 (`inference-architecture.md:215-230`) and closing item 2 (:769) say **case-sensitive** and **whitespace invalid**; `parse_model_spec` trims (`lib.rs:370,380`) and `ProviderKind::from_str` lowercases (`lib.rs:220`); `api_contract.rs:256` pins the lenient behaviour |
| #3235 | model size | CLI divides by `1_048_576` and prints "MB" (`render.rs:413,446`, test :1681); inference `format_size` is decimal (`registry/mod.rs:445`); same file prints 638.9 vs 670 |
| #3250 | the 16-code embed error set | hand-copied into `inference.embed`, `vector.upsert`, `vector.query`; aligned only by an after-the-fact test |
| #3233 (fixed, S3a) | bare `strata inference …` | README :137-157 showed four bare commands; none ran (`invalid_argument.cli.no_database`); `provider-api-keys.md` quietly used `--cache` |
| #3045 | catalog `hf_repo` | two reranker repos do not exist; nothing checks |
| #3224 | Homebrew formula name | `strata-local` named in a refusal, absent from the tap |
| — | `api_key_env_var(Local) => "STRATA_LOCAL_API_KEY"` (`lib.rs:408`, "unused, but complete") | a variable nothing reads, presented as a fact |
| — | `docs/product/strata-v1-cli-sdk-experience.md:373,392,483` | `strata models pull` — verb without its `inference` segment |

### Root D — the gates that should have caught A–C are blind here

- Mutation gate: #3225 (exit-3 precedence), #3227 (`Result` alias unviable),
  #3254 (`local`-gated code in mixed files), #3258 (non-`Default` enum arms),
  #3220. A diff to `parse_model_spec` under the default lane sees the `local`
  arms as unreachable.
- Replay lane: §2.3 — no inference code is replayed.
- Doc gates: `check-docs` proves generated docs match the IDL; nothing checks
  that a command named in README, a design doc, or a registry hint parses.

---

## 4. Decisions this proposes

| # | Decision | Closes / enables |
|---|---|---|
| R1 | One `resolve(spec, use) -> ResolvedModel` in `strata-inference`; `Availability` is a typed enum; `require_ready()` is the only place an availability becomes an error | A: #3255, #3260; the substrate for everything below |
| R2 | Split `inference.missing_model` (catalogued, not downloaded) from a new `inference.unknown_model` (not in catalog / path absent) | #3256 |
| R3 | Inference errors carry `details` under `strata.error.details.inference.v1`, defined by one Rust type; `capability` returns the same data; D8 reads details, not codes and command fields | #3226, #3256, #3216 p.1, #3244 (data not sentinel) |
| R4 | Provider keys come from injected `ProviderSettings`; executor installs env-then-config; the CLI `set_var` bridge is deleted — **done** (S3a) | #3221, rule 9 |
| R5 | One `ModelRegistry`, built once in `InferenceRuntime::new`; the loaders take the resolved path and look nothing up | #3260 |
| R6 | `pull_model` goes through the resolver; a cloud spec is `unsupported_operation`; network-disabled is the typed `DownloadDisabled` | #3255 |
| R7 | One size formatter (decimal), exported from inference, used by the CLI | #3235 |
| R8 | Prose derives or points: rule 14 becomes a two-sentence grammar plus a pointer to the matrix; commands named in docs and hints must parse; catalog repos are checked nightly | #3257, #3233, #3045, `strata models pull` |
| R9 | Bare one-shot `strata inference <verb>` (all but `install-local`) opens an ephemeral cache connection when no database target is given — **done** (S3a) | #3233 |

### R1 — the resolver

As built in S1 (`crates/inference/src/resolve.rs`); the shape below is the
code, not the proposal it replaced. Where S1 departed from the first draft the
reason is given inline.

```rust
// crates/inference/src/resolve.rs

/// What a caller is about to do with a model. `None` at a call site means
/// the caller only needs to *locate* it (`pull`, `capability`).
pub enum ModelUse {
    Run(ModelTask),
    /// Encode or decode with the vocabulary: needs local execution and the
    /// file, but any catalogued task's model will do. A `ModelTask` could
    /// not say this, which is why `use` replaced `task`.
    Tokenize,
}

pub struct ResolvedModel {
    /// The spec as given, outer whitespace removed.
    pub spec: String,
    pub provider: ProviderKind,
    /// Provider-side model name, catalog name, or path — after provider parsing.
    pub name: String,
    pub source: ModelSource,
    pub availability: Availability,
}

pub enum ModelSource {
    /// `path` is `models_dir/<hf_file>`, present or not: the loaders take a
    /// path and nothing else, so the resolver is the one place that joins it.
    Catalog { entry: &'static CatalogEntry, variant: &'static QuantVariant, path: PathBuf },
    GgufPath(PathBuf),
    /// A local name the catalog does not know; `entry` is the model matched
    /// when only the quant suffix was unknown, so the refusal can list the
    /// quants that exist (the `tinyllama:q99` row, #3264).
    Uncatalogued { entry: Option<&'static CatalogEntry> },
    Cloud,
}

/// At most one reason, decided in the order the variants are listed (Q9).
pub enum Availability {
    Ready,
    NotInCatalog,
    PathMissing,
    TaskNotSupported { requested: ModelUse },
    LocalExecutionNotBuilt,
    ProviderNotBuilt,
    NetworkDisabled,
    /// `config_key` is built from the provider (`"<provider>.api_key"`), so
    /// it is a `String`, not a static.
    KeyMissing { env_var: &'static str, config_key: String },
    NotDownloaded { pull_spec: String, size_bytes: u64 },
}

impl InferenceRuntime {
    /// Errs only for a malformed spec (`inference.invalid_request`). Every
    /// other outcome is data.
    pub fn resolve(&self, spec: &str, use_: Option<ModelUse>) -> Result<ResolvedModel, InferenceError>;
}

impl ResolvedModel {
    /// Total match over `Availability` → typed `InferenceError`.
    pub fn require_ready(&self) -> Result<(), InferenceError>;
    /// The file a local model loads from, present or not; `None` for cloud
    /// models and names the catalog does not know.
    pub fn local_path(&self) -> Option<&Path>;
}
```

`resolve` composes what already exists: `parse_model_spec`,
`ModelRegistry::lookup` (one call that answers found / unknown model / unknown
quant, with the file's path and presence), `looks_like_path`, the `cfg!`
feature checks, and the key predicate. Every runtime entry point in §2 calls
`resolve` first; `capability` returns it; the loaders receive the resolved
path and never look anything up again.

`resolve` is a pure function of (spec, use, catalog, directory listing, key
presence, build): the free `pub(crate) fn resolve(registry, network_enabled,
key_present: &dyn Fn(ProviderKind) -> bool, spec, use_)` takes each as an
argument, and `InferenceRuntime::resolve` supplies the runtime's registry, its
network setting and an environment-variable predicate. R4 replaces that
predicate with the `ProviderSettings`; nothing else moves. That is what makes
the matrix in §5 possible.

**Locate versus run.** `use_ == None` checks identity and presence only:
malformed → not in catalog → (local) not downloaded / path missing. A cloud
model asked only to be located is `Ready` whatever the build, the network or
the key say — every remaining check is about reaching the provider, which
only a run does — so `pull` refuses a cloud spec on its *source*, not on its
availability, and `capability` never refuses at all. A local model asked only
to be located still reports `NotDownloaded`, because that is a fact about the
file, but not `LocalExecutionNotBuilt` or `TaskNotSupported`, which are facts
about running it.

**Not built: loaders that resolve.** The first draft had the loaders take the
registry. S1 deleted the free `load` / `load_embedder` / `load_ranker`
functions and the `from_registry*` constructors instead: a loader takes a
path (`from_gguf`), and the only thing that turns a name into a path is
`resolve`. Two answerers for the same question is what R1 is against.

### R2 — two codes for two facts

| Availability | Code | Class / retry | Hint (registry, class-generic) |
|---|---|---|---|
| `NotDownloaded` | `inference.missing_model` (kept; meaning narrowed) | FailedPrecondition / AfterStateChange | "Pull the model before retrying." |
| `NotInCatalog`, `PathMissing` | **`inference.unknown_model`** (new) | NotFound / Never | "Check the model name against the catalog." |
| `LocalExecutionNotBuilt` | `inference.unsupported_operation` (as today) | Unsupported / Never | unchanged |
| `ProviderNotBuilt` | `inference.unsupported_provider` (as today) | | unchanged |
| `TaskNotSupported` | `inference.unsupported_operation` (as today) | | unchanged |
| `NetworkDisabled` | `inference.unsupported_operation` (as today) | | unchanged |
| `KeyMissing` | `inference.missing_api_key` (as today) | | unchanged |

`require_ready` checks the variants in the order §5.4 fixes (Q9): identity
first, then task, then build, then network, then key, then download state.
The order is part of the contract because the matrix pins one answer per
cell, and the answer for a cell that fails two checks is the earlier one.

S1 shipped the order with today's codes: `NotInCatalog` and `PathMissing`
render as `inference.missing_model` until S2 adds `unknown_model`, and the
matrix carries exactly those cells as `KNOWN_RED` under #3256 (210 in every
lane). The typed variant behind `unsupported_operation` / `unsupported_provider`
is `InferenceError::Unsupported { kind: UnsupportedKind, message }` — the code
comes from the kind, so a reworded message cannot move it (#3216).

`unknown_model` is a wire change: `errors.yaml`, a registry row, the IDL error
sets (via the named set #3250 proposes, so it is added once), a CHANGELOG line,
and a `/e/unknown_model` page at the next release. Whether a missing GGUF path
is `unknown_model` or `model_load_failed` is open (§10); the recommendation is
`unknown_model`, because nothing was loaded.

The typed errors are constructed at the raise site with their kind. The
substring classifiers in `error.rs` keep working for the string variants the
resolver does not own (llama.cpp and provider HTTP paths); the resolver never
produces a string variant, and `provider_classification.rs` gains no new
cases. Retiring the classifiers for the remaining ~230 sites is #3216's
second step, not this document's.

### R3 — the answer travels as data

```rust
// crates/inference/src/resolve.rs
/// The wire shape of `Availability`. This type *is* the definition of
/// `strata.error.details.inference.v1`.
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct AvailabilityDetails {
    pub model: String,
    pub provider: ProviderKind,
    pub availability: AvailabilityKind,          // snake_case string on the wire
    #[serde(skip_serializing_if = "Option::is_none")] pub pull_spec: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")] pub size_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")] pub key_env_var: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")] pub config_key: Option<&'static str>,
    /// Set by the executor when the spec came from a collection record.
    #[serde(skip_serializing_if = "Option::is_none")] pub collection: Option<String>,
}
```

- `From<InferenceError> for ExecutorError` converts `AvailabilityDetails` into
  `Vec<ErrorDetail>` (`engine/src/diagnostics/error.rs:100`). Keys are the
  struct's field names; a test serializes every `Availability` variant and
  asserts the key set, so the schema finally has a definition and a guard.
- `InferenceCapability` (`runtime.rs:150`) gains `availability` and the
  same optional fields. `capability` and a refusal show the same answer; the
  `can_*` booleans are derived from it.
- D8 (`cli/src/lib.rs:957-996`) deserializes the details through the
  executor's re-export of `AvailabilityDetails`, offers only when
  `availability == NotDownloaded`, and pulls `pull_spec`. No literal code
  compare (`is_missing_model` :1019 goes), no `missing_model_spec` (:1024
  goes), and `vector upsert/query --text` are covered for free because the
  model arrived in the answer, not the command (#3226). Names not in the
  catalog are never offered (#3256).
- The vector `--text` path (`executor/src/executor/vector.rs:207-228`) adds
  `collection` to the details so the refusal says which record the spec came
  from.

The registry hints stay class-generic (one string per code, no interpolation).
The one surface that types a concrete verb is the CLI, which owns its verb
spelling; §R8 makes that spelling parse-checked.

### R4 — keys are looked up, not copied into the environment

Shipped in S3a as `ProviderSettings` (`crates/inference/src/settings.rs`):

```rust
// crates/inference/src/settings.rs
pub trait ProviderSettings: Send + Sync {
    fn key(&self, provider: ProviderKind) -> Option<ProviderKey>;   // value + KeySource
    fn api_base(&self, provider: ProviderKind) -> Option<String> { None } // consumed in S3b (#3270)
}
pub struct EnvProviderSettings;                // OPENAI_API_KEY etc.; what `new` installs
impl InferenceRuntime {
    pub fn new(config: InferenceRuntimeConfig) -> Self;            // environment only
    pub fn with_settings(config: InferenceRuntimeConfig, settings: Arc<dyn ProviderSettings>) -> Self;
}
```

`InferenceRuntimeConfig` stays pure data; the settings are injected at
construction. `ProviderKey { value, source }` is Debug-redacted, has no
`PartialEq`, and exposes the secret through one named accessor. `KeySource`
is `Environment(variable)`, `ConfigFile(path)` or `Application`, and
`status.providers[].key_source` is its label — reported by the runtime
because the settings told it.

- Inference imports nothing from the workspace (rule 3), so it cannot read
  `~/.config/strata/config.toml`. Executor imports `strata-hub` behind its
  `hub` feature (`executor/Cargo.toml:22,61`), so executor composes
  `EnvThenConfig { env: EnvProviderSettings, config_path }` over
  `strata_hub::read_provider_key(path, section)` in
  `crates/executor/src/inference_settings.rs`; without `hub` it installs
  `EnvProviderSettings` alone. Env wins (D5). The file is read per lookup, so
  a `config set` is seen by the next call; an unreadable or malformed file
  logs a warning naming the path and counts as no key (follow-up: `doctor`
  should surface that file state).
- One `default_inference_runtime()` is the runtime every database opens with
  (`Executor::from_database`) and the one `strata doctor` inspects, so what
  doctor reports is what a command finds.
- `load_provider_keys_into_env`, its `std::env::set_var`, `config_backed_keys`
  and `name_config_key_sources` are deleted from the CLI. `set_var` was
  process-global state (rule 9) and becomes `unsafe` in edition 2024; this
  removed the last one on the inference path.
- Every executor caller — CLI, IPC, MCP, wasm-less SDK paths — gets
  config-file keys (#3221). `crates/stratadb` (the embedded facade) has no
  inference surface at all (`stratadb/Cargo.toml:15` depends on engine only),
  so #3221 is closed for every path that exists; the facade is stated plainly
  as out of scope rather than half-served.
- Tests inject `testkit::{NoKeys, FakeKeys}` instead of mutating process
  environment. The inference matrix's `keys`/`no-keys` child modes remain: they
  pin the environment source `new` installs, which is a real product path.

### R5, R6, R7 — mechanical

- R5 (built in S1): `InferenceRuntime::new` builds `registry: ModelRegistry`
  once from `config.models_dir` (else `STRATA_MODELS_DIR`, else
  `~/.strata/models`). The loaders' own `ModelRegistry::new()` calls went with
  the loaders' registry-taking constructors (R1, "not built"): the runtime
  resolves, then hands `from_gguf` the resolved path (#3260).
  `ModelRegistry::resolve` — the registry's own name-to-path renderer — is
  deleted; `require_ready` is the only place a lookup becomes a message.
- R6 (built in S1): `pull_model(spec)` → `resolve(spec, None)`; `Cloud` →
  `unsupported_operation`; `NotInCatalog` / `PathMissing` → `missing_model`
  (S2: `unknown_model`); `Ready` → return the path, in every build, network on
  or off; `NotDownloaded` → download when the network is on and the build
  carries `download`, else `RegistryFailed { DownloadDisabled }` — the typed
  kind, not `NotSupported("… network access")` classified by the word
  "network" (#3255). The IDL declares `invalid_request` and
  `unsupported_operation` on `inference.models.pull`, and `invalid_request`
  on `tokenize` / `detokenize` / `rank`: the resolver runs before the
  local-execution check, so a malformed spec reaches those verbs as what it
  is (#3262's precedence, delivered here; its `unknown_model` half is S2's).
- R7: `format_size` (`registry/mod.rs:445`, decimal MB/GB) becomes `pub`,
  re-exported by executor as `format_model_size`; `render.rs:413-446` uses it
  and the `"1.0 MB"` assertion at :1681 moves to the one formatter (#3235).

### R8 — prose derives or points

- Rule 14 and closing item 2 in `inference-architecture.md` shrink to: the
  grammar (`provider:model`, first colon, non-provider prefix = local name in
  full, task from the operation), the leniency decision (§10 Q2), and *"the
  authoritative behaviour is `crates/inference/tests/resolution_matrix.rs`; this
  rule does not restate cells"* (#3257).
- `prose/commands/inference.capability.md:6` remains the one hand-written list
  of spec forms; each form it names is a matrix row, and `check-docs` carries it
  to the generated docs.
- A `cli` test extracts every `strata …` invocation from `README.md`,
  `docs/inference/*.md`, `docs/design/inference*.md`, the registry hints and
  the D8 prompt, and runs it through the real clap parser
  (`Cli::try_parse_from`). It catches `strata models pull`, and it makes the
  #3124 rule *"every command named on this surface must exist"* a guard
  instead of a sentence.
- Catalog: a net-gated nightly test issues a `HEAD` for every `hf_repo` /
  `hf_file` in `CATALOG` (#3045). The two dead reranker entries are a product
  decision (§10 Q3) — remove now, or publish the repos.

### R9 — inference verbs do not need a database

The inference verbs are machine-global (shared model directory, no database
state). Shipped in S3a: `OpenIntent::InferenceOneShot` (`cli/src/open.rs`),
chosen by `one_shot_intent` from the parsed command — a one-shot inference
verb with no `--db`/positional/`STRATA_DB`/`--cache` opens the ephemeral
cache connection, the same object `--cache` opens; an explicit target is
honored exactly as for any one-shot, and the current directory is never
opened (`implicit_interactive_target` stays REPL-only). Every other one-shot
keeps the refusal (`invalid_argument.cli.no_database`); the REPL, pipe and
MCP paths are untouched. `install-local` keeps its pre-open interception.
README :137-157 is true as written; `docs/inference/provider-api-keys.md`
dropped `--cache` from its examples.

---

## 5. The contract matrix

The matrix is the acceptance instrument for every slice and the thing that
outlives them. It replaces the prose contract as the authority.

### 5.1 Dimensions

| Dimension | Values |
|---|---|
| spec form | `""`, `"   "`, `"openai:"`, `"local:"`, `"miniLM"`, `"MINILM"`, `"  miniLM  "`, `"local:miniLM"`, `"qwen3:1.7b"`, `"qwen3:1.7b:q8_0"`, `"tinyllama:q8_0"`, `"tinyllama:q99"`, `"nope"`, `"nope:thing"`, `"local:nope"`, `"a:b:c:d"`, `"<tmp>/present.gguf"`, `"<tmp>/absent.gguf"`, `"openai:gpt-4o-mini"`, `"OpenAI:gpt-4o-mini"`, `"anthropic:claude-x"`, `"google:x"`, `"openai-compatible:ep:m"` |
| registry state | empty dir; dir holding a non-empty `miniLM` file; dir holding a zero-length file |
| key state | none; env (inference matrix: the source `new` installs); env-then-config (executor `inference_settings` truth tables + CLI `config_behavior` end to end, since S3a) |
| build | `cfg!(feature = "local")`, per-provider `cfg!` — the expectation is computed, and the matrix runs under both mutation-lane feature sets |
| task / verb | generate, embed, rank, tokenize, pull, capability |

A cell's expectation is `(provider, name, source kind, availability)` or a
malformed-spec error; at executor level it is `(code, class, details keys)`.

### 5.2 Where it lives

- `crates/inference/tests/resolution_matrix.rs`: the cells are the product of
  the §5.1 dimensions, the expectation is *computed* by one function
  (`expected`) from the identity of the spec, the directory, the network
  flag, the build and the verb — there is no hand-written table to drift. The
  harness builds `InferenceRuntime::new(config)` over a `tempdir` and runs the
  cells in a child process with a scrubbed environment (`env_clear`, `HOME`
  and `STRATA_MODELS_DIR` under the tempdir, `STRATA_HF_ENDPOINT` pointed at
  a closed loopback port so an attempted download is refused at once), once
  with no key and once with a fake key exported into every
  `CLOUD_PROVIDER_KEYS` variable; the second run must differ from the first
  only on key cells. Nothing in the process environment is mutated in place.
  Here "key" means "env var" — the `EnvProviderSettings` source `new`
  installs — and `status.key_source` is asserted to name that variable. The
  env-then-config composition is the executor's (R4) and is pinned there.
- `crates/executor/tests/inference_resolution_wire.rs` (feature `inference`):
  the executor-level cells, live rather than replayed. Specs are derived from
  `CATALOG` and `CLOUD_PROVIDER_KEYS` plus the malformed and path forms
  (2,222 cells across directory × network × verb). It asserts three
  relations per cell, none of which is "the code is X": (1) the executor's
  code equals the code the `InferenceService` trait returned for the same
  call — the executor adds nothing and loses nothing; (2) the envelope's class
  and retry policy equal the registry row for that code, and the row exists;
  (3) the code is declared in the command's IDL error set
  (`command-index.json`). Row fidelity turned out to be enforced three times
  over on the way out (`From<InferenceError>`, `render_status`,
  `normalize_explicit_status` each re-read the registry); only a plant in the
  innermost layer reddens the cells, which is worth knowing before anyone
  tries to simplify that path.
- Executor level, S2b (shipped): the resolution-time cells are **replay
  fixtures** (`fixtures.error_cases`) for `inference.generate`,
  `inference.embed`, `inference.rank`, `inference.tokenize`,
  `inference.detokenize`, `inference.models.pull`, `inference.capability`,
  `vector.upsert` and `vector.query`. The testkit fake fakes *execution
  only*: `FakeInferenceService` composes the real `resolve` over the real
  catalog (a `ModelRegistry` on a directory that does not exist), and fakes
  what happens after `require_ready`. Its world is fixed in its own favour
  for what the environment decides — every catalogued model present, every
  provider built and keyed, the network on — and the override sits *after*
  the resolver rather than in a `cfg!`, so a fixture replays the same under
  every feature set (in a build without `local` the resolver answers
  `LocalExecutionNotBuilt` before it looks for the file; with it,
  `NotDownloaded`). So `unknown_model`, `invalid_request` and
  `unsupported_operation` left `unreplayed-error-codes.yaml` (budget 115 →
  112; `missing_model`, `missing_api_key`, `download_disabled` and
  `unsupported_provider` are environment facts the fake world fixes, covered
  by the matrix and the real-runtime executor tests), and the IDL's existing
  guards reach resolution for the first time. The replay lane injects the
  fake for every command, not only the inference family, which is what lets
  a `vector --text` case meet the resolver. One knob shapes the world:
  `with_undownloaded(pull_spec)` holds a catalogued variant off disk until it
  is pulled, so a consumer's refuse → pull → retry loop (D8) runs end to end
  in-process. No parallel harness.
- Cloud `Ready` cells assert `capability` only; nothing in the matrix sends a
  request. The 21 cells that would (a key present, the network on) are the
  matrix's only never-run cells; they need a runtime-level provider base URL
  the way downloads have `STRATA_HF_ENDPOINT` — **#3270**, proposed for S3
  beside R4.

### 5.3 Known red, and falsification

- The matrix lands in S0 against today's code. Cells whose result differs from
  the expectation go in a `KNOWN_RED` list keyed by issue number. A test
  asserts every `KNOWN_RED` cell **still fails**, so a fix must delete its
  entry — the same shrink-only discipline as `unreplayed-error-codes.yaml`.
  The red cells *are* the bug inventory; any red cell without an issue gets
  one when S0 opens.
- Before S0 merges, #3222 is re-planted locally (revert the `parse_model_spec`
  arm) and the matrix must go red on exactly the `qwen3:1.7b` /
  `tinyllama:q8_0` / `nope:thing` rows. #3255 and #3260 are live and serve as
  proof that the pull and directory dimensions detect what the audit found.
  A guard that has not been shown to fail is not evidence (#3216).

**What S0 found (2026-09-09).** The matrix ran green in three build shapes:
the default lane (cloud providers, no `local`, no `download`) executes 828
cells of which 323 are known red; the `download` lane (mutation lane C's
shape) executes 811 and carries 321; the `local,download` lane executes 811,
skips 17 that would download or send, and carries 397. No CI lane builds
`local`, so the local-lane numbers come from a developer run and the
`KNOWN_RED` entries scoped to that lane are checked only there.

- The #3222 re-plant went red on 75 cells, all on the predicted rows:
  `qwen3:1.7b`, `qwen3:1.7b:q8_0`, `tinyllama:q8_0` (18 each), `nope:thing`
  and `a:b:c:d` (6 each), `openai-compatible:ep:m` (3), and `tinyllama:q99`
  moved from one wrong answer to another (6).
- Three executor plants proved the three wire relations live: rewriting
  every `generate` error to `invalid_request` in dispatch (380 red, relation
  1); changing the class and retry policy in `normalize_explicit_status`
  (1,842 red, relation 2); dropping `inference.missing_api_key` from
  `inference_generate` in `command-index.json` (12 red, relation 3). Plants
  in `From<InferenceError>` and `render_status` alone were invisible for the
  reason §5.2 gives.
- Red cells without an issue got one: **#3262** (a non-`local` build answers
  "local execution not built" before establishing what the spec is, so
  malformed specs and unknown names alike become `unsupported_operation`),
  **#3263** (a model asked to do a task it lacks is never `TaskNotSupported`;
  the answer is `unsupported_provider` or `missing_model` depending on which
  check runs first), **#3264** (an unknown quant of a known model —
  `tinyllama:q99` — is `registry_corrupt`, the `registry_code` fallthrough).
  #3255 gained two findings: a present, catalogued model is refused by
  `pull` when the network is off or the build lacks `download`, and `pull`
  in a download build hands the raw spec to the registry, bypassing the
  parser's trim and `local:` handling.
- Two facts worth keeping: the load verbs never download (only `pull_model`
  does, so a load cell can never touch the network and needs no skip), and
  `capability` answers for every spec form in every build without a single
  red cell — it is the one verb that already behaves like a resolver, which
  is why the matrix asserts only `provider` identity on it today and why R3
  makes it the carrier of the answer.

**What S1 changed (2026-09-09).** With every verb going through `resolve`,
`KNOWN_RED` shrank to the two #3256 entries (unknown names and absent paths
answer `missing_model` until S2): 210 known-red cells in each of the default,
`download`, and `local,download` lanes, down from 323 / 321 / 397. The
#3255, #3260, #3262-precedence, #3263 and #3264 entries left because their
cells now match the contract, not because the contract moved — with one
exception, decided rather than discovered: a cloud spec asked only to be
located (`pull`, `capability`) is `Ready`, so `pull` refuses it on its source
(R1, "locate versus run"). Two tests outside the matrix had been passing on
the developer's real `~/.strata/models` (a present `miniLM` made a
network-off `pull` succeed, which is now the contract); both were made
hermetic with a temp models directory. The executor wire matrix gained the
`detokenize` verb, which is what found that its `invalid_request` declaration
was missing. The S1 mutation report then moved the last download cells from
"never run" to observed: `pull` of a catalogued model that is not on disk,
with the network on in a `download` build, is expected to *attempt* the
download and answer `download_failed` against the unreachable hub (a `pull`
that skipped the attempt would answer `Ok` or `download_disabled`), which is
what caught the surviving `pull_variant` mutant.

### 5.4 Cells where contract and code disagree today

These are decided in S0, not discovered later:

| Cell | Rule 14 says | Code does | Recommendation |
|---|---|---|---|
| `"OpenAI:gpt-4o-mini"` | case-sensitive → not a provider → local name | `from_str` lowercases → OpenAI (`lib.rs:220`) | keep lenient; fix the rule (#3257) |
| `"  miniLM  "` | whitespace invalid | trimmed → `miniLM` (`lib.rs:370`) | keep lenient; fix the rule |
| `"openai-compatible:ep:m"` | reserved grammar | local name → today `missing_model`, after R2 `unknown_model` | pin `unknown_model`; reserving means no promise, and the future grammar change becomes a visible contract change |
| `"a:b:c:d"` | opaque after first colon | local name; `find_entry_by_parts` returns None for ≥4 parts (`catalog.rs:301`) | `unknown_model` |
| embed model asked to `generate` | task from the operation path | not pinned anywhere | `TaskNotSupported` → `unsupported_operation` (Q7) |
| zero-length model file | — | `model_file_is_downloaded` false → `missing_model`; `check_and_clean_corrupt` on load | `NotDownloaded` (the file is not a model); pull overwrites |

S0 pinned these in `expected()`; the matrix is now the authority for them.
Four more rows were not in the document and had to be decided when the
cells were written — they are the precedence questions, Q9–Q12 in §10:

| Cell | Code does | S0 pins | Why |
|---|---|---|---|
| malformed or unknown spec, build without `local` | `unsupported_operation` — `tokenize`/`rank` refuse before parsing; `generate`/`embed` refuse on `provider == Local` before the catalog (#3262) | `invalid_request` / `unknown_model` | identity before capability (Q9): nobody is told to install local execution for a model that does not exist |
| wrong-task model, any build | `unsupported_provider` (cloud: the word "provider" in the message) or `missing_model` (local: the loader asks the registry for a model of the wrong task) (#3263) | `unsupported_operation` | Q7, and task before build (Q9): the answer must not depend on which check ran first |
| `tinyllama:q99` | `registry_corrupt` (#3264) | `unknown_model` | an unknown quant is the same fact as an unknown name |
| cloud spec, network off | `unsupported_operation` from the gate, before any key is read | `unsupported_operation`, before `missing_api_key` | Q11; the design gains `NetworkDisabled` for it |
| `pull` of a present catalogued model, network off or no `download` | `download_disabled` (#3255) | `Ok` — the file is there | pull resolves first (Q10); download is only for `NotDownloaded` |
| `pull` of `"  miniLM  "` / `local:miniLM`, present, download build | `missing_model` — the raw string goes to the registry (#3255) | `Ok` | pull parses like every other verb (Q10) |
| `pull` of a GGUF path | (download build) `missing_model` | present → `Ok`; absent → `unknown_model` | a path is an identity, not a catalog name (Q10) |

The zero-length row above is pinned for `pull` as well (Q12): a zero-length
file is `NotDownloaded`, so `pull` re-downloads it rather than returning the
path. Code already agrees; the cell is green.

---

## 6. Slices

Each slice is one PR, ≤1,500 LOC, with the matrix as its acceptance: the
slice's cells leave `KNOWN_RED`, no other cell changes. Issues in the "held"
column are **not** to be `/audit-fix`ed individually while this plan runs.

| Slice | Content | Wire change | Held issues it closes |
|---|---|---|---|
| **S0** | Matrix (inference level, all cells) + `KNOWN_RED`; executor-level wire cells (pass-through, registry row, IDL declaration); falsification by re-planting #3222 — **done** (PR #3265), §5.3 | none | — (filed #3262, #3263, #3264) |
| **S1** | R1 `resolve` / `ResolvedModel` / `Availability` / `require_ready`; R5 one registry; R6 pull through the resolver; the free loaders and `from_registry*` deleted — **done** (PR #3269), §5.3 | declarations only: `pull` gains `unsupported_operation` (cloud spec) and `invalid_request`; `tokenize` / `detokenize` / `rank` gain `invalid_request` | #3255, #3260, #3263 |
| **S2** | R2 `unknown_model`; R3 `AvailabilityDetails` on the wire and in `capability`; D8 reads details; testkit fake composes the real resolver; replay fixtures for resolution-time codes; #3252 decided (Q4, wired) — **done** in two PRs: S2a (PR #3289: `unknown_model`, details on the wire) and S2b (`capability` fields, `io_failure` produced, fake composes the resolver, replay fixtures, D8 on details), §5.2 | **yes** — new code, new details, `capability` fields; release notes in both PR bodies | #3256, #3262, #3264, #3286 (S2a); #3226, #3252 (S2b) |
| **S3** | Two PRs. **S3a — done**: R4 `ProviderSettings` (constructor-injected; executor composes env-then-config; CLI bridge deleted; `doctor` reads the executor's default runtime); R9 `OpenIntent::InferenceOneShot`; key dimension at executor level; docs drop `--cache`, §R4/§R9. **S3b**: `api_base` on `ProviderSettings` (config + env), `with_api_base` un-gated, the 21 `Expect::NeverRun` matrix cells go live | S3a: `status` key-source field semantics (same values, produced by the runtime); one-shot inference verbs run without a target. S3b: base-URL settings | S3a: #3221, #3233. S3b: #3270 |
| **S4** | R7 one size formatter; R8 rule 14 rewrite, clap-parse guard over docs and hints, nightly catalog check, dead-entry decision; `STRATA_LOCAL_API_KEY` removed; `strata models pull` fixed | none | #3235, #3257, #3045 |
| **T** | Tooling lane: mutation-gate self-check (a diff that touches product code and yields zero viable mutants fails the gate); #3225 exit-3 precedence; #3227 `Result` alias; #3254 `local`-gated code in mixed files; #3258 non-`Default` enum arms; #3220 | none | #3225, #3227, #3254, #3258, #3220 |

Sequencing: **T runs in parallel from the start** — every slice S1–S4 will
touch `local`-gated code in mixed files, and the gate must be able to see it.
S0 → S1 → S2 → S3 → S4 are ordered by dependency (S2 needs the resolver; S3's
key dimension needs R4; S4's rule-14 rewrite needs the matrix to point at).

Two pieces of error-registry mechanism work are prerequisites for S2 and stay
their own PRs, ahead of it: **#3250** (named error sets, so `unknown_model` is
added to one set, not three lists) and **#3244** (registry row unconditional,
hint override explicit). They are not symptoms of the resolver; they are the
tooling S2 lands on.

Not in this plan: #3224 (Homebrew formula — "we'll deal with homebrew
separately"), #3234, #3039, the intelligence layer, and #3216's second step
(carrying codes at all ~288 raise sites). The resolver's own errors are typed
from day one, which is that step applied to the surface this document owns.

---

## 7. Who is reading this

The caller is usually a coding agent running `--json`. What it needs from a
refusal is not a better sentence but the facts: which model, which provider,
why it is unavailable, and the exact argument to pass to the command that
fixes it. R3 puts those in `details`; `availability` is a closed string enum
documented in the IDL; `pull_spec` is the string to type. The human renderer
(`render.rs:962-994`) shows the same fields. Every command any surface names —
registry hints, the D8 prompt, README, design docs — parses under the real
CLI, by test (R8).

---

## 8. Where we stand

Verified against `main` at `98f8f324`; rows updated for S1 and S2 (S2a, S2b).

| Area | State | Gap |
|---|---|---|
| Parser | `parse_model_spec` correct after #3259; lenient on case and whitespace | rule 14 contradicts it (#3257) |
| Catalog / registry | sound predicates; case-insensitive lookup; one directory resolver; **S1**: one registry per runtime, `lookup` the only catalog question, loaders take a path | — |
| Availability | **S1**: `resolve` → `ResolvedModel` / `Availability` for every verb, `require_ready` the only renderer; **S2**: `Availability` reaches the wire as `details` and `capability` as `availability` / `pull_spec` / `size_bytes` / `key_env_var` / `config_key` — one `AvailabilityDetails` type, both directions | — |
| Error codes | typed kinds exist (#3217) but 6 of 8 variants are strings, ~240 of ~250 construction sites; substring classifiers load-bearing; **S2**: `unknown_model` split from `missing_model` (#3256); `io_failure` produced for a filesystem that cannot say whether a file is there (#3252, Q4) | substring classifiers (#3216 step 2, out of scope) |
| Wire details | **S2**: `strata.error.details.inference.v1` defined by `AvailabilityDetails`, produced by every resolution refusal, flattened by the executor and read back by `ExecutorError::inference_availability`; `size_bytes` readable from its decimal string | — |
| D8 offer | **S2b**: keyed on `details.availability == not_downloaded` and `details.pull_spec`, not on a code or a command field; covers `vector --text` (the model from the collection's record); never offers a name the catalog does not know (#3226); a refused pull is not re-offered | — |
| Keys | `strata config set <provider>.api_key` stored 0600; env wins; **S3a**: the runtime learns keys from injected `ProviderSettings` — executor composes env-then-config, `status.key_source` reports the source that answered, `doctor` reads the same runtime | an unreadable or malformed config file logs a warning and counts as no key; `doctor` does not yet inspect the file (follow-up) |
| No-database use | `install-local` intercepted pre-open; **S3a**: every one-shot inference verb with no target runs in an ephemeral cache session (`OpenIntent::InferenceOneShot`); README and `provider-api-keys.md` agree | stratadb.org still shows `strata --cache inference …` (site handoff after S3a) |
| Sizes | one decimal formatter in inference | CLI has a second, mislabelled one (#3235) |
| Test reach | parser pinned (`api_contract.rs`); capability honesty pinned; wire==registry pinned; **S0 matrix** (`resolution_matrix.rs`, `inference_resolution_wire.rs`) with `KNOWN_RED` as the bug inventory; **S2b**: resolution-time codes replayed from IDL error cases against the fake-composes-real-resolver world, D8's loop driven end to end against `with_undownloaded` | no CI lane builds `local`, so the local-lane cells run only on a developer machine; mutation gate blind to `local` arms (#3254/#3258) and to guards that are equivalent programs in every CI lane (#3267); cloud dispatch after `require_ready` observable only with a live key (#3270) |

---

## 9. Out of scope

- Retiring the substring classifiers for llama.cpp and provider-HTTP errors
  (#3216 step 2).
- `crates/stratadb` inference surface — the facade has none; adding one is a
  product decision, not a resolver fix.
- The intelligence layer (deferred, #3171).
- Homebrew (`strata-local` formula, #3224).
- Streaming, OpenAI-compatible endpoints, multi-model routing.

---

## 10. Decisions to make

| # | Question | Recommendation |
|---|---|---|
| Q1 | Name and class of the new code | `inference.unknown_model`, class NotFound, retry Never — as #3256 proposes |
| Q2 | Case / whitespace leniency | keep the code lenient; rewrite rule 14 and closing item 2; the matrix pins it |
| Q3 | Dead reranker catalog entries (#3045) | remove them in S4 unless the repos are published first; a catalogued model that cannot be pulled is a false fact |
| Q4 | `inference.io_failure` (#3252) | wire it: the resolver's single filesystem touchpoint maps non-`NotFound` `io::Error`s to `Io`; the `every_constructible_inference_error` floor becomes an equality |
| Q5 | Missing GGUF path | `unknown_model` with `details.model` = the path; `model_load_failed` stays for files that exist and fail to load |
| Q6 | `openai-compatible:` reserved prefix | parses as a local name today; pin `unknown_model`; the future grammar lands as a visible contract change |
| Q7 | Wrong-task model (embed model asked to generate) | `Availability::TaskNotSupported` → `unsupported_operation` |
| Q8 | T before S0, or parallel | parallel; S0 does not touch product code, so the gate's blind spots do not affect it |
| Q9 | Check order when a cell fails more than one check | identity before capability: malformed → unknown → wrong task → execution / provider not built → network disabled → key missing → not downloaded. Pinned by S0's `expected()`; S1's `require_ready` implements it in that order |
| Q10 | What `pull` is | task-neutral, needs no execution build, resolves first: present catalogued file → `Ok` without network; path present → `Ok`; path absent → `unknown_model`; cloud → `unsupported_operation` (R6); downloads only for `NotDownloaded` |
| Q11 | Cloud verb, network off, no key | `unsupported_operation` from a new `NetworkDisabled` availability, before the key is read — the refusal that does not change when the key is added |
| Q12 | Zero-length file under `pull` | `NotDownloaded`; pull overwrites (same answer as the load verbs, §5.4) |

Q9–Q12 were made by S0 because the matrix cannot hold a cell without an
answer; they are recorded here so that S1 implements them rather than
re-deciding them. Reversing one means changing `expected()` and the cells
that follow from it — a visible diff, which is the point.

---

## Appendix — restatements register

What is derived (generated or guarded from a single source) and what is
written by hand on this path. The plan's job is to move rows from the second
table to the first or delete them.

**Derived today**

| Fact | Source | Carried to |
|---|---|---|
| per-command error sets | `commands/*.yaml` | generated docs, `command-index.json`, SDKs, `strata agents errors` |
| code → class / message / hint / retry | `error_registry.rs` rows | wire envelope, `/e/` pages, `docs/errors/registry.md`; pinned wire==registry (`inference_behavior.rs:301`) |
| command docs | IDL + `prose/commands/*.md` | `generated/docs/`, enforced by `check-docs` |
| examples | `examples/*.yaml` | replayed by `verify-examples` |
| parser behaviour | `api_contract.rs` | — (pins only; no prose derives from it) |

**Hand-written today (targets)**

| Fact | Copies | Plan |
|---|---|---|
| spec grammar | rule 14, closing item 2, `inference.capability.md:6`, README examples | matrix is the authority; rule 14 points; IDL prose is the one list |
| "why unavailable" | `error.code()` substring tables; D8 `is_missing_model` + `missing_model_spec`; `render.rs` status prose | `Availability` computed once, rendered everywhere |
| 16-code embed set | `inference.embed`, `vector.upsert`, `vector.query` | named set (#3250) |
| model size | `render.rs` MiB math + inference decimal | one formatter (R7) |
| model directory | `ModelRegistry::new()` ×4 | one registry (R5) |
| provider key | env read ×3 + CLI `set_var` bridge + CLI status relabel | one `ProviderSettings` (R4, **done** in S3a) |
| commands named in prose | README, `provider-api-keys.md`, `strata-v1-cli-sdk-experience.md`, registry hints, D8 prompt | clap-parse guard (R8) |
| catalog repos | `catalog.rs` `hf_repo` strings | nightly HEAD check (R8) |
| `STRATA_LOCAL_API_KEY` | `lib.rs:408` | delete |
| `strata.error.details.inference.v1` | name on 20 rows, no definition | `AvailabilityDetails` is the definition (R3) |
