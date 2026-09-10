<div align="center">

# Strata

**Branch, time-travel, and search your data like code.**

The embedded database for the agent era — five data models, git-like branching,
and built-in time travel. One binary, one directory, zero infrastructure.

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.91%2B-orange.svg)](https://www.rust-lang.org)
[![Version](https://img.shields.io/badge/version-1.2.1-brightgreen.svg)](https://stratadb.org/changelog)

[Website](https://stratadb.org) · [Documentation](https://stratadb.org/docs) · [Playground](https://stratadb.org/playground) · [Agent skills](https://github.com/stratalab/strata-agent-skills)

</div>

---

Strata is what a database looks like when versioning isn't an afterthought. It runs inside your process like SQLite — no server, no containers, no ops — but every write is versioned, any state can be forked instantly, and any moment in history can be read back, across key-value pairs, JSON documents, event logs, vectors, and graphs.

```bash
strata ./mydb kv put user:ada '{"role":"engineer"}'

strata ./mydb branch fork default experiment              # instant copy-on-write fork
strata ./mydb --branch experiment kv put user:ada '{"role":"cto"}'

strata ./mydb --branch experiment kv get user:ada         # {"role":"cto"}
strata ./mydb kv get user:ada                             # {"role":"engineer"} — untouched
```

Run an experiment on a fork. Let an agent loose on a branch. Read yesterday's state without having built a snapshot system. Delete the branch, and it never happened.

## Highlights

- 🌿 **Branch anything, instantly.** Forks are copy-on-write and constant-time regardless of database size, and isolate *all* data models at once. Compare two branches, then promote one into another with a single atomic merge.
- ⏳ **Time travel is built in.** Every write gets a version and timestamp. Read any key, document, event range, vector search, or graph *as of* any past moment with `--as-of`.
- 🧩 **Five data models, one engine.** KV, JSON documents, append-only events, vector search, and property graphs share one storage substrate, one branch model, one history.
- 📦 **Embedded, like SQLite.** A single binary and a single data directory. Use it as a Rust library, a CLI, or an MCP server. It also runs in the browser via WebAssembly — in volatile cache mode, the same as `--cache`: nothing is persisted (#3151).
- 🤖 **Agent-native.** `strata mcp serve` exposes the database to Claude, Cursor, or any MCP client. Every command emits clean JSON with `--json`. Events are hash-chained for tamper-evident audit trails.
- 🛡️ **Durable by default.** Write-ahead log, crash recovery, and explicit durability modes — or run pure in-memory with `--cache` when persistence is noise.

## The five data models

Point any command at a database path (or `--cache` for ephemeral in-memory). Every command accepts `--branch` and `--space`, and reads accept `--as-of`.

**Key-value** — working memory with full version history:

```bash
strata ./mydb kv put user:ada '{"name":"Ada","role":"engineer"}'
strata ./mydb kv history user:ada
strata ./mydb kv list --prefix user:
```

**JSON documents** — path-level reads and writes, secondary indexes:

```bash
strata ./mydb json set config '$.model' '"claude"'
strata ./mydb json get config '$.model'          # "claude"
```

**Events** — append-only, hash-chained, verifiable:

```bash
strata ./mydb event append tool_call '{"tool":"search","query":"docs"}'
strata ./mydb event list --event-type tool_call
strata ./mydb event verify-chain                 # sequence density + hash linkage
```

**Vectors** — similarity search with metadata filters:

```bash
strata ./mydb vector collection create embeddings 384
strata ./mydb vector upsert embeddings doc1 @embedding.json --metadata '{"title":"intro"}'
strata ./mydb vector query embeddings @query.json -k 5
```

Record the embedding model on the collection and Strata embeds for you — text
writes and searches then use that model and no other:

```bash
strata ./mydb vector collection create notes 1536 --embedding-model openai:text-embedding-3-small
strata ./mydb vector upsert notes n1 --text "hello"
strata ./mydb vector query notes --text "hi" -k 5
```

**Graph** — property graphs with real algorithms, not just traversal:

```bash
strata ./mydb graph create social
strata ./mydb graph add-edge social ada knows lin
strata ./mydb graph pagerank social              # also: wcc, sssp, cdlp, lcc, neighbors
strata ./mydb graph bulk-insert social --file graph.json
```

## Branching and time travel

Branches are the core abstraction, not a bolt-on. A fork captures every data model at a point in time; branches then evolve independently — and you can compare them and promote work back with a merge.

```bash
# Agent A explores on its own branch; production is untouchable from there
strata ./mydb branch fork default agent-a
strata ./mydb --branch agent-a kv put plan '{"step":1}'

# Compare every data model between two branches, then promote the work back
# in one atomic commit (KV, JSON, and vectors merge; events and graphs compare)
strata ./mydb branch diff default agent-a
strata ./mydb branch merge agent-a default        # strict refuses conflicts; source-wins overwrites

# Time travel: read state as of any past timestamp — on any branch
strata ./mydb kv get user:ada --as-of 1783660565504764
strata ./mydb vector query embeddings @query.json --as-of 1783660565504764

# Keep the branch, or make it never have happened
strata ./mydb branch delete agent-a
```

This is what makes Strata fit agents: exploration is cheap, mistakes are disposable, and every state an agent ever produced remains inspectable after the fact.

## Built for agents

```bash
strata --db ./agent-memory mcp serve       # MCP server over stdio — plug into Claude, Cursor, ...
strata ./mydb agents guide                 # self-describing surface, written for LLMs
strata ./mydb kv get user:ada --json       # every command speaks compact JSON
```

Skills that teach agents this whole surface — usage, branching, time travel — live in
[strata-agent-skills](https://github.com/stratalab/strata-agent-skills):
`npx skills add stratalab/strata-agent-skills`, or `/plugin marketplace add stratalab/strata-agent-skills`
in Claude Code. The same repo carries the one-command workspace setup (CLI + MCP + skills).

Model execution is in the box too — embeddings and generation, from the same binary.
Inference commands work on models, not on a database's data, so they need no database:

```bash
export OPENAI_API_KEY=...                                  # or ANTHROPIC_API_KEY, GOOGLE_API_KEY
strata inference embed openai:text-embedding-3-small "how do branches work?"
strata inference generate openai:gpt-4o-mini "summarize this changelog"
```

Keys persist, so you set them once rather than exporting in every shell:

```bash
strata config set openai.api_key sk-...   # stored at 0600; env still wins
strata config set openai.base_url http://localhost:8000/v1   # or OPENAI_BASE_URL: a proxy or compatible server
strata inference status                   # what this build can do, before you try
```

**The default binary runs cloud models, not local ones.** Local GGUF execution
means a vendored llama.cpp and cmake, which would turn a 12 MB download into a
far larger one for a capability most users never ask for — so it is opt-in.
`strata inference status` says so up front, and `models list` marks every local
entry `unavailable`. Add local execution without a Rust toolchain:

```bash
strata inference install-local           # swaps in the local-capable build
strata inference models pull miniLM      # models live in ~/.strata/models,
strata inference embed miniLM "..."      # shared by every database
```

## Use it as a library

The same engine, embedded in your Rust process. The crate is not on crates.io
yet (see [Status](#status)) — depend on it by path:

```rust
use stratadb::prelude::*;

let mut db = Database::open_cache(CacheOpenOptions::new())?.into_database();
let mut kv = db.kv(
    BranchName::new("default")?,
    ProductSpace::new("default")?,
)?;

kv.put(KvKey::new("greeting")?, KvValue::new(b"hello".to_vec()))?;
assert!(kv.get(&KvKey::new("greeting")?)?.is_some());
```

Durable databases open the same way with `Database::open_local(path, DurableLocalOpenOptions::new())`. The browser build (`crates/wasm`) exposes the **cache-mode** engine to JavaScript via WebAssembly. Cache mode is volatile by design — no WAL, no manifest, no checkpoints — so a browser database lives only as long as the page.

The prelude carries what a first program needs. Beyond it, the crate root holds
`Database`, the open options, and the error types, and every other type is
namespaced by the capability it belongs to — `stratadb::json`, `stratadb::event`,
`stratadb::vector`, `stratadb::graph`, `stratadb::branch`, `stratadb::artifact`.

## Install

**Install script** (macOS, Linux — checksum-verified):

```bash
curl -fsSL https://stratadb.org/install.sh | sh
strata --cache ping                 # pong 1.2.1
```

**Homebrew:** `brew install stratalab/tap/strata`

**Python:** `pip install stratadb`

**From source** (Rust 1.91+):

```bash
git clone https://github.com/stratalab/strata-core
cd strata-core
cargo install --path crates/cli     # installs the `strata` binary
strata init
```

**MSRV 1.91, develop on 1.94.** 1.91 is the compatibility promise — CI verifies
every PR builds the library and binary targets on it. Contributors build with
the 1.94.1 pin in `rust-toolchain.toml` so lints and formatting are reproducible;
running the test suite may need it. Both numbers are intended (#3150).

## How it works

Under every data model sits a single branch-aware MVCC storage engine: one write-ahead log, one commit clock, one copy-on-write branch tree. KV, JSON, events, vectors, and graph are capabilities layered over that substrate — which is why a fork captures all of them atomically and why time travel works uniformly everywhere. Durable databases recover from crashes via WAL replay; cache mode skips persistence entirely and lives in memory.

A database is a directory. Copy it, back it up, `rsync` it — it behaves the way embedded databases should.

Deeper internals live in [`docs/architecture/`](docs/architecture/).

## Status

Strata 1.2.1 is released. The on-disk format, error codes, and CLI surface documented here are stable contracts. The four distributions are not on one release train:

| Surface | How you get it | Where it is |
|---|---|---|
| CLI (`strata`) | `curl … \| sh`, `brew install stratalab/tap/strata` | 1.2.1, released |
| Python (`stratadb`) | `pip install stratadb` | on PyPI, its own train |
| Rust (`stratadb`) | path dependency on this repo | **not on crates.io** — every crate is `publish = false` |
| Node / TypeScript | — | not shipped |

`cargo add stratadb` does not work yet; the crates.io name is reserved for the
release train that flips `publish`.

## License

[Apache 2.0](LICENSE)
