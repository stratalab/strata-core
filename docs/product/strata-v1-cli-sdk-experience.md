# Strata V1 CLI and SDK Experience

Status: V1 product direction

## Purpose

This document defines the first-time and steady-state user experience for
Strata across the CLI, Python SDK, Node SDK, Rust SDK, and agent-driven usage.

The core decision is that Strata is embedded first. The CLI is a convenient
shell and scripting surface over the same database APIs that SDKs use. It is
not a required daemon, installer step, or control plane.

## Product Position

Strata should feel like SQLite or DuckDB at the usage boundary:

1. A user can install a package and immediately open a database from code.
2. A user can install a CLI and immediately open a database from a terminal.
3. A durable database is rooted at an explicit path.
4. Cache mode is explicit, ephemeral, and process-local.
5. Local AI assets are shared machine assets under `~/.strata`.
6. Database configuration is database-local and created when the database is
   created.

Strata should not require a global server, daemon, login, or first-run setup
before a normal embedded database can be created.

## Install Surfaces

Users may discover Strata through several entry points:

1. A standalone installer such as `curl ... | sh`.
2. Homebrew or another system package manager.
3. `pip install strata`.
4. `npm install` or `npx strata`.
5. A Rust crate dependency.
6. An AI agent or MCP server that invokes the CLI or SDK.

All entry points must preserve the same product model:

1. The CLI can guide humans through setup.
2. SDKs can create and open databases without requiring CLI setup.
3. Local AI setup is optional and explicit.
4. Cloud provider use is explicit and can be driven by environment variables
   or SDK configuration.

## Database Lifecycle

### Durable Database

A durable database is created at a path and owns its database-local metadata,
configuration, storage files, indexes, branches, spaces, and recovery state.

Creation should use a dedicated command:

```sh
strata new ./my-db
```

Opening should use the path directly:

```sh
strata ./my-db
```

Opening from inside a database root should start the REPL:

```sh
cd ./my-db
strata
```

One-shot scripting should use an explicit database selector:

```sh
strata --db ./my-db kv put user Claude
```

### Cache Database

Cache mode is not an initialized database directory. It is an explicit
non-durable runtime mode.

```sh
strata --cache
strata :memory:
```

Cache mode must have these properties:

1. No database directory.
2. No WAL.
3. No recovery.
4. No database-local config file.
5. No persistence after process exit.
6. Same executor command surface as durable mode, except commands that require
   durable persistence must return clear capability errors.

The REPL banner must make the lifecycle obvious:

```text
Strata cache database
Mode: cache
Persistence: none, discarded on exit
```

Ambiguous combinations should be rejected:

```sh
strata --cache ./my-db
```

The error should explain that a path means durable-local mode and cache mode is
pathless.

## First-Time CLI Experience

The CLI should have a guided first-time setup command:

```sh
strata init
```

`strata init` is machine setup and onboarding. It is not required before SDK
usage and should not be the steady-state command for creating every database.

`strata init` should:

1. Create `~/.strata` if it does not exist.
2. Detect hardware and show the detected runtime envelope.
3. Explain the default database profile that will be used for newly created
   durable databases.
4. Ask whether the user wants local AI support.
5. If local AI is enabled, let the user choose recommended models and download
   them into `~/.strata/models`.
6. Record machine-level local AI preferences where appropriate.
7. Teach the next commands.

The end of a successful `strata init` should be concrete:

```text
Setup complete.

Create your first database:
  strata new ./my-db

Open an existing database:
  strata ./my-db

Start a temporary cache database:
  strata --cache
```

For automation, `strata init` should support non-interactive flags:

```sh
strata init --yes --no-local-ai
strata init --yes --local-ai --model qwen3-embedding
```

Non-interactive init must not download models unless the user explicitly asks
for local AI model installation.

## Database Creation and Profile Detection

Profile detection must live in the shared database creation path, not only in
the CLI.

When any surface creates a durable database with the default auto profile, the
engine should:

1. Detect hardware.
2. Choose a safe profile such as embedded, desktop, or server.
3. Resolve concrete storage and runtime settings.
4. Write user intent and explicit overrides into database-local configuration.
5. Expose resolved storage and runtime settings through diagnostics.
6. Open the database with the resolved settings.

Existing databases should read stored database-local user intent and explicit
overrides. Auto-derived runtime values may change when the same database is
opened on a different machine; explicit profile or memory-budget pins must not
be silently changed.

This is required so these flows are equivalent:

```sh
strata new ./my-db
```

```python
db = strata.open("./my-db", create=True)
```

```ts
const db = await strata.open("./my-db", { create: true });
```

## CLI Path Resolution

The CLI should resolve database targets in this order:

1. `--db <path>` opens the specified durable database for one-shot commands.
2. A positional path opens that durable database in the REPL.
3. `--cache` or `:memory:` opens an ephemeral cache database.
4. With no path, if the current directory is a database root, open it.
5. With no path, if a parent directory is a database root, open that root.
6. Otherwise, show a clear hint.

Outside a database, this should not silently create anything:

```text
No Strata database found.

Create one with:
  strata new ./my-db

Start a temporary cache database:
  strata --cache
```

If the user runs an interactive command against a missing path:

```sh
strata ./my-db
```

the CLI may offer:

```text
No Strata database found at ./my-db.

Create it now? [y/N]
Equivalent command: strata new ./my-db
```

In non-interactive mode, the same situation must fail with a hint rather than
prompting.

## REPL Experience

Running `strata <path>` or `strata` inside a database should open a REPL.

The prompt should show the database, branch, and space:

```text
strata:my-db main/default>
```

The REPL should be a thin interactive shell over the same command contract used
by one-shot CLI commands, SDK command execution, MCP, and future language
bindings. It should not grow a separate database semantics model.

Expected REPL basics:

1. `help` shows common commands and points to `help kv`, `help json`, etc.
2. `mode` or `info` shows durable/cache mode and local AI availability.
3. Branch and space context can be inspected and changed intentionally.
4. `exit` and `quit` leave the REPL.
5. Errors should include the command that would fix the issue where possible.

## One-Shot CLI Experience

One-shot commands are for scripts, agents, CI, and shell workflows.

Examples:

```sh
strata --db ./my-db kv put user Claude
strata --db ./my-db kv get user
strata --db ./my-db json set users/u1 '{"name":"Claude"}'
strata --db ./my-db branch create experiment-a
strata --cache kv put temp value
```

One-shot commands should support machine-readable output:

```sh
strata --db ./my-db --json kv get user
```

Rules:

1. One-shot commands must not prompt unless explicitly asked.
2. Missing databases must fail with `strata new <path>` guidance.
3. Cache mode must be explicit.
4. Network calls must be explicit.
5. Local model downloads must be explicit.

## SDK First-Time Experience

SDK users should not have to run `strata init`.

Python:

```python
import strata

db = strata.open("./my-db", create=True)
db.kv.put("user", "Claude")
```

Node:

```ts
import { open } from "strata";

const db = await open("./my-db", { create: true });
await db.kv.put("user", "Claude");
```

Rust:

```rust
let db = strata::open("./my-db").create(true)?;
db.kv().put("user", "Claude")?;
```

When `create=True` is used, the shared database creation path must perform
profile detection and write database-local configuration exactly as the CLI
would.

If a database does not exist and create is not enabled, SDKs should return an
actionable error:

```text
No Strata database found at ./my-db. Create it with create=true or run:
  strata new ./my-db
```

## SDK Ongoing Experience

After creation, SDK users should open databases directly:

```python
db = strata.open("./my-db")
```

```ts
const db = await open("./my-db");
```

SDKs should expose cache mode explicitly:

```python
db = strata.open_cache()
```

```ts
const db = await openCache();
```

SDKs should not depend on CLI-only config, shell state, or `strata init`.
SDK behavior should be stable in containers, notebooks, servers, tests, and
agent sandboxes.

## Local AI Experience

Local AI is the one intentionally machine-level Strata feature. Models can be
large and should be shared across databases.

Machine-level assets belong under:

```text
~/.strata/models
```

Local AI setup can happen through the CLI:

```sh
strata init --local-ai
strata inference models list
strata inference models pull qwen3-embedding
```

or through SDK helper APIs:

```python
strata.ai.setup_local(model="qwen3-embedding")
```

```ts
await strata.ai.setupLocal({ model: "qwen3-embedding" });
```

Using local AI without the required model should fail with a direct fix:

```text
Local model qwen3-embedding is not installed.

Install it with:
  strata inference models pull qwen3-embedding
```

Local AI rules:

1. Model downloads must be explicit.
2. Model assets are shared across databases.
3. Database opens must not download models.
4. Cache databases may use local AI if the model exists.
5. Durable databases may reference local AI configuration, but the model bytes
   remain machine-level assets.

## Cloud AI Experience

Cloud providers should not require `strata init`.

Users should be able to provide credentials through environment variables,
SDK configuration, or explicit CLI flags, depending on the surface.

Examples:

```sh
OPENAI_API_KEY=... strata --db ./my-db ai generate ...
```

```python
db = strata.open("./my-db")
answer = db.ai.generate(provider="openai", model="gpt-4.1-mini", prompt="...")
```

Rules:

1. Database open must not call cloud providers.
2. Commands that call cloud providers must be explicit.
3. Missing credentials must produce actionable errors.
4. Credentials should not be written into database-local config by default.
5. The CLI may help users configure credentials, but the core SDK path must not
   depend on CLI setup.

## Agent Experience

Agents should be able to operate Strata without prompts.

Recommended agent flow:

```sh
strata new ./workspace-db --profile auto --yes
strata --db ./workspace-db --json kv put task/status started
strata --db ./workspace-db --json branch create experiment-a
```

Agent rules:

1. Prompts are disabled when stdin is not a TTY.
2. Commands have JSON output.
3. Errors include stable codes and remediation.
4. Cache mode is available for scratch state.
5. Durable mode is explicit through a path.
6. Network and model-download behavior is never implicit.

## Command Summary

Human CLI:

```sh
strata init
strata new ./my-db
strata ./my-db
cd ./my-db && strata
strata --cache
```

Script CLI:

```sh
strata --db ./my-db kv put user Claude
strata --db ./my-db --json kv get user
strata --cache kv put temp value
```

SDK:

```python
db = strata.open("./my-db", create=True)
db = strata.open("./my-db")
db = strata.open_cache()
```

Local AI:

```sh
strata inference models pull qwen3-embedding
```

## V1 Pathway Coverage

The CLI and SDK experience must cover every V1 pathway from
`docs/product/strata-v1-user-pathways.md`. Coverage does not mean every pathway
gets a unique top-level command. It means every pathway has a clear durable,
cache, CLI, SDK, REPL, scripting, or capability-error story.

| Pathway | Experience coverage |
| --- | --- |
| 1. Create or open a local embedded database | `strata new <path>`, `strata <path>`, `strata --db <path> ...`, and SDK `open(path, create=true)` all use the same shared durable database creation/open path. |
| 2. Open an ephemeral cache database | `strata --cache`, `strata :memory:`, and SDK `open_cache()` create explicit process-local databases with no persistence, WAL, recovery, or database-local config. |
| 3. Open a database read-only | CLI and SDK open options must include read-only mode. Mutating executor commands must return capability errors when the database is read-only. |
| 4. Share a local database through IPC | The CLI should expose an explicit serve/share mode over an already-open database. SDK and agent clients should connect through the same executor command contract rather than opening unsafe second writers. |
| 5. Clone a portable dataset | `strata clone <source> <destination>` should create a normal durable local database that can then be opened with `strata <destination>` or SDK `open(destination)`. |
| 6. Use a cloned dataset offline | Once cloned, no special UX exists. The cloned database behaves like any durable database, including branch, write, search, export, and local-only operation. |
| 7. Write and read key-value data | CLI one-shot, REPL, SDK, and agent commands expose KV operations through the same executor command names and result shapes. |
| 8. Write and read JSON documents | CLI one-shot, REPL, SDK, and agent commands expose JSON operations through the same executor command names and result shapes. |
| 9. Append and query events | CLI one-shot, REPL, SDK, and agent commands expose event append/read/range operations through the same executor command names and result shapes. |
| 10. Create and manage graphs | CLI one-shot, REPL, SDK, and agent commands expose graph create/delete/list/inspect operations through the same executor command names and result shapes. |
| 11. Model graph entities and relationships | Graph node/edge commands must be available through CLI, REPL, SDK, and JSON-output agent usage with explicit branch and space context. |
| 12. Define graph ontology | Ontology commands should appear under graph-facing command groups and SDK namespaces. If deferred from an early release, the command group must return clear unavailable capability errors. |
| 13. Traverse and query graph neighborhoods | Traversal commands should support CLI flags, REPL syntax, SDK options, JSON output, and branch/space/time context. |
| 14. Run graph analytics | Analytics commands should be explicit, potentially long-running operations. Cache and unsupported storage modes may return capability errors when analytics indexes or resources are unavailable. |
| 15. Store and query vectors | Vector upsert/fetch/query/list/update/delete commands must be available through CLI, REPL, SDK, and JSON-output agent usage. |
| 16. Run keyword search | Search commands should be exposed through CLI, REPL, SDK, and recipes. If an index is missing, errors should explain how indexing is enabled or repaired. |
| 17. Run semantic or hybrid search | Semantic and hybrid search should be explicit search or recipe commands. Missing embeddings, models, credentials, or indexes should produce actionable errors. |
| 18. Run graph-aware retrieval | Graph-aware retrieval should be a recipe or retrieval command that clearly states graph dependency requirements and fails cleanly when graph data or indexes are unavailable. |
| 19. Use search recipes | Recipes should be named, inspectable, and executable through CLI, REPL, SDK, and JSON output. Recipe configuration belongs to safe config surfaces, not ad hoc hidden files. |
| 20. Use query expansion and reranking | Expansion and reranking must be explicit inference-backed recipe stages. They may use local or cloud AI, but database open must not trigger network calls or model downloads. |
| 21. Ask retrieval-backed questions | Question-answering commands should be explicit AI/retrieval commands with clear provider/model selection, grounding diagnostics, and no hidden provider calls. |
| 22. Configure auto-embedding and indexing | Users should enable, inspect, repair, and reindex derived state through safe command groups. The UX should not expose low-level flush or compaction as normal maintenance. |
| 23. Manage models and inference configuration | `strata inference models ...`, `strata init --local-ai`, SDK local-AI helpers, and provider configuration cover model listing, pulling, local availability, and cloud-provider setup. |
| 24. Generate, tokenize, and detokenize text | Inference utility commands should be explicit CLI/SDK calls and must work independently of database creation when no database context is required. |
| 25. Create and manage branch workspaces | Branch create/list/delete/select/inspect commands should be available through CLI, REPL context, SDK namespaces, and agent JSON output. |
| 26. Inspect record history | History commands should work across supported primitives and expose versions, timestamps, values, and deletion markers through CLI, SDK, and JSON output. |
| 27. Read data as of a point in time | CLI flags, REPL context, and SDK options should allow timestamp/version reads without changing the current branch head. |
| 28. Scrub and explain a branch timeline | Timeline commands should expose available time ranges, resolved timestamps/versions, and change summaries through CLI, SDK, and machine-readable output. |
| 29. Create a branch from historical state | Branch creation must accept current, timestamp, or retained-version sources through CLI flags and SDK options, with clear errors for expired history. |
| 30. Compare, promote, copy, and restore branch changes | Branch workflow commands should be explicit and previewable. Destructive or compensating writes should require confirmation interactively and `--yes` in scripts. |
| 31. Organize data with spaces | Space selection must be part of REPL context and command options. SDKs should expose spaces without forcing users into raw key-prefix management. |
| 32. Import and export primitive data | Import/export commands, including Arrow where supported, should be explicit CLI/SDK operations with clear primitive, branch, space, and format selection. |
| 33. Inspect database state | `info`, `describe`, `health`, `metrics`, and durability counters should be available through REPL, one-shot CLI, SDK, and JSON output. |
| 34. Recover from ordinary failures | Open/reopen errors must distinguish crash recovery, lock conflict, unsupported backend, corrupt metadata, and config errors, with safe remediation guidance. |
| 35. Configure Strata safely | Safe configuration is split between database-local resolved config, `~/.strata` local-AI assets/preferences, environment credentials, and explicit recipe/provider commands. Secrets must not be written into database config by default. |
| 36. Run Strata from the CLI | The CLI supports guided setup, durable database creation, REPL, one-shot commands, JSON output, cache mode, local AI setup, and script-safe noninteractive behavior. |
| 37. Use Strata from application code | SDKs can create/open durable databases, open cache databases, use all primitive command groups, configure local/cloud AI explicitly, and never depend on CLI setup. |
| 38. Use Strata in agent or sandbox workflows | Agents use noninteractive commands, JSON output, explicit paths, explicit cache mode, explicit network/model actions, stable errors, and no hidden prompts. |
| 39. Choose a storage backend intentionally | Open/create options should name the storage mode or backend. Unsupported combinations must return capability errors rather than silently falling back to a different durability model. |

## Post-V1 and Non-Pathway Alignment

The experience contract should leave room for the post-V1 directional pathways
without implementing them prematurely:

1. Dataset discovery, publishing, and public lineage should extend `clone`,
   import/export, and branch provenance rather than changing local open/create
   semantics.
2. Fleet visibility should be a separate product surface. It must not turn the
   embedded database into a daemon-first product.
3. Backup, sync, and movement should be explicit commands with visible source
   and destination choices, not hidden background behavior.

The explicit V1 non-pathways constrain the CLI and SDK experience:

1. No follower mode as a normal user-facing multi-process story. IPC/share mode
   is the supported path for another local process to use an already-open
   database.
2. No public begin/commit/rollback workflow. User-facing commands should remain
   command-oriented, even when the engine uses transactions internally.
3. No legacy branch bundle workflow. Portable datasets should use clone/import
   style flows.
4. No disk-backed cache mode. Cache is pathless and non-durable; durable-local
   is path-backed and recoverable.
5. No hidden network behavior. Clone, cloud AI, model download, and future sync
   must be explicit.
6. No public tags and notes for V1 unless later product work reintroduces them
   for dataset release or provenance.
7. No manual maintenance as normal UX. Flush, compact, checkpoint, and retention
   controls should stay internal or diagnostic unless a later product decision
   introduces a safe administrative surface.

## Binding Decisions

1. `strata init` is first-time machine setup, not required SDK setup.
2. `strata new <path>` is the primary durable database creation command.
3. `strata <path>` opens a durable database REPL.
4. `strata --db <path> ...` runs one-shot commands against a durable database.
5. `strata --cache` opens or uses an ephemeral cache database.
6. SDK database creation performs profile detection through the shared open path.
7. Existing durable databases use stored database-local configuration.
8. Local AI model assets live under `~/.strata`.
9. Database opens never download models or call cloud providers implicitly.
10. The REPL and CLI commands must delegate through the same command contract as
    SDK, MCP, and agent surfaces.

## Deferred Questions

1. Whether `strata init <path>` should remain as a compatibility alias for
   `strata new <path>`.
2. Whether the CLI should support named cache sessions beyond process-local
   cache mode.
3. Whether cloud credential helpers should write user-level preferences under
   `~/.strata` or remain environment/configuration driven.
4. The exact database-local config filename and schema for the resolved profile.
5. The final recommended local AI model list for first-run setup.
