# Inference: the developer experience we want

**Status:** decisions D1–D11 accepted 2026-09-07. Nothing here is built yet
except where the gap table says so; the sequencing in §5 is the plan of record.

Strata's inference stack has two halves that behave nothing alike, and a user
meets both without being told they are different:

- **Cloud** — an HTTP client and an API key. Ships in every binary. Costs money
  per call, needs the network, and the failure modes are somebody else's
  service.
- **Embedded** — a vendored llama.cpp, models on disk, execution in-process.
  Ships in *no* released binary. Costs nothing per call, needs no network, and
  the failure modes are ours.

Shipping only cloud in the release is right: llama.cpp and cmake would turn a
12 MB download into a multi-hundred-megabyte one, for a capability most users
never ask for. The mistake is not the build — it is that **nothing tells a user
which binary they have until an operation fails**, and there is no way to get
the other one short of installing a Rust toolchain.

And inference in Strata is not one feature. Generation is a stateless call that
returns text. **Embedding writes into the database**, and every vector already
stored carries an implicit dependency on the model that produced it. Those two
need different journeys, and today they have the same one.

---

## 1. The principle

> **A user should never learn what their binary can do by watching it fail.**

Everything below follows from that. Capability is knowable before the call —
which build, which providers, which keys, which models — so the surface owes
the user that answer up front, in one place, in terms they can act on.

Second principle, for the embedding half:

> **A vector is only meaningful next to vectors from the same model.** The
> database should know which model wrote each collection and refuse the
> mismatch, rather than silently returning nonsense neighbours.

---

## 2. The four journeys

### 2.1 "I want to generate text"

```
strata inference generate <model> "..."
│
├─ model spec names a cloud provider (openai:, anthropic:, google:)
│   ├─ provider compiled in?  ──no──▶ REFUSE: this build has no <provider>.
│   │                                  (cannot happen in a released binary —
│   │                                   cloud is always in)
│   ├─ key available?         ──no──▶ REFUSE: no key for <provider>.
│   │                                  → strata config set <provider>.api_key
│   │                                  → or export <PROVIDER>_API_KEY
│   ├─ provider rejects key?  ──yes─▶ REFUSE: <provider> rejected the key.
│   │                                  → check it at <provider console URL>
│   │                                  → strata config set <provider>.api_key
│   ├─ network unreachable?   ──yes─▶ REFUSE (retryable): cannot reach
│   │                                  <provider>. → --offline uses local models
│   └─ ▶ generate
│
└─ model spec names a catalogued local model
    ├─ local execution in this build? ──no──▶ REFUSE: this build cannot run
    │                                          local models.
    │                                          → strata inference install-local
    │                                          → or use a cloud model
    ├─ model downloaded?              ──no──▶ interactive: offer the download
    │                                          (size, source), then pull
    │                                          --json / non-tty: REFUSE
    │                                          → strata inference models pull <m>
    └─ ▶ load and generate
```

The three refusals a released binary can produce today all collapse into one
message that names **the two ways forward**: change the build, or change the
model.

### 2.2 "I want to embed"

Embedding has everything above, plus the part that makes it different: the
output belongs to a collection, and a collection can only hold one model's
vectors.

```
strata <db> vector upsert <collection> <key> --text "..."
│
├─ collection exists?              ──no──▶ REFUSE, name the create command
├─ collection has an embedding model recorded?
│   ├─ no  (created by hand, raw vectors)  ──▶ REFUSE (embedding_model_missing):
│   │                                           pass --vector, or declare once:
│   │                                           vector collection
│   │                                             set-embedding-model <c> <model>
│   └─ yes ▶ use THAT model, not a default
├─ that model runnable here?       ──no──▶ REFUSE: this collection was built
│                                           with <model>, which this build
│                                           cannot run.
│                                           → strata inference install-local
├─ downloaded?                     ──no──▶ pull (as above)
└─ ▶ embed, then store (one command; see D10 on why not one commit)
```

And the query side must agree:

```
strata <db> vector query <collection> --text "..."
└─ embeds with the collection's recorded model, always.
   A caller who passes a raw --vector of the wrong dimension is refused;
   a caller who passes --text never has to know which model to name.
```

The failure this prevents is the quiet one: embedding a query with `miniLM`
against a collection built with `nomic-embed` returns results, ranked, with no
error and no meaning.

### 2.3 "I want local models and I have the lean binary"

```
strata inference install-local
│
├─ already runs local models?  ──yes──▶ no-op, changed: false
├─ Homebrew-managed binary?    ──yes──▶ REFUSE, name the formula:
│                                        brew install stratalab/tap/strata-local
├─ curl-installed?
│   ├─ resolve the -local asset for this target triple, at THIS version
│   ├─ download + verify SHA-256 BEFORE touching anything
│   └─ atomically replace the running binary
└─ ▶ strata inference status now reports local_execution: true
```

This is not new machinery. `strata update` already resolves a release, fetches
the target-triple tarball and `checksums-sha256.txt`, verifies in-process, and
swaps the binary atomically. `install-local` is that flow pointed at a
different asset.

**As built (#3217), two things differ from the first sketch of this flow.** It
does not show the asset's size and ask: the command is itself the request, the
way `strata update` is, and the reader in §3b cannot answer a prompt — a
confirmation would block the agent the command exists for, and it would be a
second consent for a fetch the user just typed. And a Homebrew-managed binary
is refused with the formula's name rather than swapped; the `strata-local`
formula it names does not exist yet (#3224), so until it does that message
points at nothing. Two properties the sketch left implicit are contracts: it
installs the version it is running, not the latest (adding a capability must
not also move you across releases), and it is idempotent, so an agent that
cannot tell whether it already ran it can run it again.

**What it costs us:** a second build per target in the release matrix, with
cmake and the vendored llama.cpp. CI time and release size, not user cost. CUDA
stays a source build for now; it is a third axis (driver and toolkit
versions) and does not belong in a first cut.

**The alternative we are rejecting:** telling users to install Rust and cmake
and build from source. That is a fine answer for a contributor and a wall for
everybody else.

### 2.4 "What can I actually do right now?"

One command, answering every question above before anything fails:

<!-- illustrative -->
```
$ strata inference status

build          lean (cloud providers only)
               local models: not available    → strata inference install-local

providers
  openai       ready          key from OPENAI_API_KEY
  anthropic    ready          key from ~/.config/strata/config.toml
  google       no key         → strata config set google.api_key
  local        not in build   → strata inference install-local

models         ~/.strata/models  (shared by every database)
  downloaded   miniLM 42.9 MB · nomic-embed 247.9 MB
  catalogued   9 more          → strata inference models list
```

This is the surface #3124 asks for, generalised: it is the answer to "will this
work", available before the attempt, in one place.

---

## 3. Decisions this proposes

| # | Decision | Rationale |
|---|---|---|
| D1 | Ship a **second release asset** per target: `strata-<version>-<triple>-local.tar.gz` | Local execution stops requiring a Rust toolchain |
| D2 | `strata inference install-local` swaps the binary, reusing `update`'s verified-download path | The machinery exists; the risk is understood |
| D3 | `can_*` on `capability` means **"this binary, right now"** | A flag a caller must AND with another flag is a bug factory — the docs author got it wrong, so everyone will |
| D4 | `ModelInfo.runnable` distinguishes *file on disk* from *can execute* | `is_local` answers the wrong question and reads like the right one |
| D5 | **Persistent API keys** in `~/.config/strata/config.toml` (0600); env vars still win | Exporting a variable in every shell is not a configuration story |
| D6 | Distinct, actionable failures for **no key / rejected key / unreachable** | Three different user actions, one error today |
| D7 | Models stay in `~/.strata/models`, **shared across databases**, and we say so | Already true; nowhere documented, so nobody relies on it |
| D8 | Interactive first use **offers the download**; `--json` and non-tty refuse with the pull command | An agent must never block on a hidden 600 MB fetch |
| D9 | Collections **record their embedding model**; mismatched embed or query refuses with `failed_precondition.embedding_model_mismatch` | CLAUDE.md rule 24 already promises this. It does not exist |
| D10 | `vector upsert --text` / `vector query --text` embed and store in **one command**, orchestrated in executor | Otherwise every user hand-rolls embed → capture JSON → upsert. **Engine cannot import inference** (hard rules 2–3, and `strata-engine` has no inference dependency), so the embed call lives in executor and the *store* is the single commit — the embedding precedes it and can fail without writing anything. This is real logic in the layer CLAUDE.md calls thin; it is a recorded exception, taken because the intelligence layer that would own it is deferred with no target release (#3171) |
| D11 | `strata inference status` is the single truth surface | Replaces "find out by failing" |

---

## 3a. What was decided

Accepted 2026-09-07:

- **D1/D2 — ship a second `-local` asset per target**, and `strata inference
  install-local` swaps the binary. CUDA stays a source build for now; it
  multiplies the matrix by driver and toolkit versions, and a GPU user is the
  likeliest to tolerate a build. Homebrew and Windows are open questions in §6.
- **D3 — the `can_*` change is a 1.2.x bug fix.** The field contradicts
  `provider_feature_enabled` in the same object; a flag that reads `true` when
  the answer is `false` is a defect, not a contract. Wire-visible on
  `inference capability`, so it is called out in the release notes.
- **D5 — keys in the global config at mode 0600**, environment variables still
  winning. **Correction: this was already built** and the gap table below was
  wrong about it. `strata config set openai.api_key <key>` writes it,
  `read_global_provider_key` read it, and `load_provider_keys_into_env` filled
  env gaps before any inference command ran. The original claim ("environment
  variables only") came from grepping `crates/inference` alone and missing that
  the CLI resolved config keys a layer above. What remained of D5 was surfacing
  it: the no-key hint now names `strata config set`, which is persistent and
  executable by an agent, rather than only `export`. (The CLI-side bridge was
  itself the gap #3221 named — only the CLI got config keys — and was replaced
  in S3a of `inference-model-resolution.md` by `ProviderSettings` injected
  into the runtime, so every executor caller resolves keys the same way.)
- **D9 — a collection with no recorded model refuses `--text`** and names a
  command to declare one. Raw `--vector` keeps working. Inference-by-dimension
  was rejected: 384 and 768 are shared by several models, so it would guess,
  and guessing wrong silently is the exact failure this removes.

  As built (#3218), the command is `vector collection set-embedding-model
  <collection> <model>`. It is a declaration, not a verification: the engine
  cannot tell which model produced a vector — stored or supplied later, since
  a vector carries no model — so it takes the caller's word for both. What the
  record holds to the model is `--text`: text is embedded with the recorded
  model and no other, on upsert and on query. A raw `--vector` is the caller's
  statement that the recorded model produced it; Strata records the model, it
  cannot verify a vector against it (explicit vector provenance or a stricter
  collection mode is a possible follow-up, #3231). It is
  one-time — redeclaring the same model commits nothing, and a different one
  is refused, because changing the model under existing vectors is the mixing
  rule 24 forbids. The missing-model refusal has its own code,
  `failed_precondition.engine.embedding_model_missing`, so a caller can tell
  "nothing recorded" from "two models disagree". Rule 24's spelling of the
  mismatch code is two segments; rule 27 requires three, so the registered
  code is `failed_precondition.engine.embedding_model_mismatch`.
- **D10 — executor orchestrates**, as a recorded exception to the thin-executor
  rule (see the table above). Built in #3218 as `vector upsert --text` and
  `vector query --text`; writes embed as documents and searches as queries,
  since instruction-tuned embedders produce different vectors for the two.

## 3b. Who is reading this

The caller of these surfaces is usually a **coding agent**, not a person at a
terminal. That is not a detail of tone; it decides what a remediation may say:

- An agent reads `--json`, not the table. Remediations therefore travel **in the
  payload** (`local_remedy`, `key_env_var`, `model_prefix`), not only in prose
  the renderer composes.
- An agent has **no Rust toolchain** and cannot answer a build prompt.
  `cargo install --features inference-local` is not a remediation for it; it is
  a dead end that happens to look like one.
- An agent will execute whatever command we print. So every command named on
  this surface **must exist and must work** — which is why the no-key hint names
  `strata config set` (built) and the local hint names
  `strata inference install-local` (D2, landing before this ships).
- `model_prefix` exists for exactly this reader: having found a ready provider,
  an agent needs the string to type, not a sentence describing it.

The same rule applies to interactive prompts (D8): an agent cannot answer one,
so `--json` and non-tty must refuse with the command to run instead.

## 4. Where we stand

Verified against `main` at 1.2.1.

| Capability | Today | Gap |
|---|---|---|
| Cloud generation | ✅ works, three providers in the released binary | — |
| Local execution in releases | ❌ compiled out | **D1, D2** — `-local` asset in the #3124 branch |
| Way to add local later | ❌ source build only | **D1, D2** — `install-local` in the #3124 branch |
| `capability` truthfulness | ❌ `can_embed: true` beside `provider_feature_enabled: false` | **D3** — fixed in the #3124 branch |
| Model list truthfulness | ❌ lists 11 models the binary cannot load; `is_local` is about the file | **D4** — fixed in the #3124 branch |
| Refusal messages | ❌ seven phrasings, none actionable | partly fixed in the #3124 branch |
| API key configuration | ✅ `strata config set <provider>.api_key`, 0600, env wins | — (was wrongly listed as missing) |
| Key failure diagnosis | ❌ "not set" is distinguished; rejected vs unreachable are not | **D6** |
| Model storage shared across DBs | ✅ `~/.strata/models`, global | **D7** (document it) |
| Model download | ❌ rides with `local`, so a released binary cannot pull at all | **D1** |
| First-use download prompt | ❌ no prompt, no auto-pull | **D8** — fixed in the #3124 branch; `vector --text` not yet covered (#3226) |
| Removing a model | ❌ no `models rm` | minor |
| Embedding model provenance | ❌ **nothing records it**; rule 24's error code does not exist in the codebase | **D9** — fixed in #3218 |
| Embed → store in one step | ❌ `inference embed` returns vectors to the caller; `vector upsert` takes them | **D10** — fixed in #3218 |
| Query-time embedding | ❌ caller must embed separately with the right model, unaided | **D9, D10** — fixed in #3218 |
| Single status surface | ❌ none; `capability` answers per model | **D11** — `strata inference status` in the #3124 branch |
| `strata doctor` covers inference | ❌ checks binary, home, database only | **D11** — fixed in the #3124 branch |

### The two that matter most

**D9/D10 (embedding provenance and one-step embed) is the product gap.** Without
it Strata's embedding is an OpenAI proxy that happens to live in the same
binary: the user does the model bookkeeping, and the database — which is the
thing that knows what is stored — helps not at all. With it, a collection is
self-describing, a query cannot be silently wrong, and `vector upsert --text` is
the shortest path from a document to a searchable database of any system in
this class. This is also the piece the deferred intelligence layer (M8, #3171)
was going to own, and it does not need that layer: the provenance belongs in
engine next to the collection config.

**D1/D2 (install-local) is the distribution gap.** It converts "local inference
requires a Rust toolchain" into one command, and it is mostly assembly of parts
we already have.

---

## 5. Suggested sequencing

1. **Honesty first** (#3124, branch open) — `capability`, `runnable`, one
   refusal phrasing, README. Small, and it stops the surface lying today.
2. **`strata inference status`** (D11) — the single truth surface. Cheap, and
   it is what the docs site wants to link.
3. **Keys** (D5, D6) — persistent config plus three distinguishable failures.
   Self-contained, no distribution changes.
4. **Embedding provenance** (D9) — engine records the model on the collection
   and refuses mismatches. Largest engine change; unblocks D10.
5. **One-step embed** (D10) — `vector upsert --text`, `vector query --text`.
6. **install-local** (D1, D2) — release matrix, second asset, the swap command.
   Last because it is the only one that changes what we ship.

Steps 1–3 make the current binary honest and configurable. Steps 4–5 make
embedding a database feature rather than a proxied API call. Step 6 removes the
toolchain wall.

---

## 6. Open questions

- **Homebrew:** a separate `strata-local` formula, or a formula option? A tap
  can carry both, but a user who `brew install`s one and then runs
  `install-local` needs a coherent answer. `install-local` already refuses a
  Homebrew binary by naming `stratalab/tap/strata-local`; publishing that
  formula is #3224.
- **Windows:** `install-local` inherits `update`'s platform support. Local
  execution on Windows is untested.
- **CUDA:** a third asset per target, or permanently a source build? A GPU user
  is likelier to tolerate a build, but they are also the user who most wants
  local inference.
- ~~**Existing collections:** if D9 lands after users have vector data, what is
  the story for a collection with no recorded model?~~ Answered in #3218: all
  three. `--text` refuses with `embedding_model_missing`, raw vectors keep
  working, and `vector collection set-embedding-model` declares once (D9).
- **Key precedence:** env over config is proposed. Does a per-database override
  belong here too? (Related: the deferred per-device inference config work.)
