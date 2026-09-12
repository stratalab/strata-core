# Strata Deployment, Install, and First-Run Experience

**Status:** Design proposal (workstream #4 of the V1 hardening track)
**Date:** 2026-07-06
**Spans:** `strata-core` (binary, CLI, executor metadata), `stratadb.org` (installer, docs, llms.txt),
`strata-python`, `strata-nodesdk`, `strata-mcp`, `strata-foundry` (console, on ice), `strata-eval` (measurement)

---

## 1. Thesis

Strata's primary installer is not a human. It is Claude Code, Codex, Cursor, or another coding agent
that was told "use Strata for this" — or that chose Strata on its own. That agent must be able to go
from *nothing installed* to *verified first write and read* **without a single web search**, in a
handful of tool calls, on any platform.

This inverts the usual DX playbook. Humans forgive ambiguity and browse docs; agents burn tool calls,
hallucinate flags, and fall back to web search the moment local information runs out. A world-class
first-run experience for Strata therefore means:

1. **One-line install in every ecosystem** — and every install's last output teaches the next command.
2. **Self-describing artifacts** — the binary and every SDK carry their own complete, current,
   machine-readable documentation. The web is a mirror, not the source.
3. **Deterministic first contact** — no surprising side effects, no hidden state, errors that teach.
4. **One name, one version, one release train** — identical verbs, identical error codes, identical
   version number on every channel.
5. **Measured, not assumed** — onboarding is a benchmarked surface with regression gates, exactly like
   read latency.

The rest of this document specifies each of these, grounded in what exists today.

---

## 2. Personas and budgets

| Persona | Entry point | Success criterion | Budget |
|---|---|---|---|
| **Coding agent** (Claude Code, Codex, Cursor, Devin) | told "use Strata" or reads it in a repo's AGENTS.md | install → verified `put`/`get` → productive API use | **≤ 3 tool calls to first verified write; 0 web searches** |
| **Agent runtime** (Claude Desktop, MCP client) | MCP registry / config snippet | `strata` tools appear and work on first message | 1 config line, no manual build |
| **Human developer** | web search, HN, a comparison page | REPL open with data inside | **≤ 60 seconds** from copy-paste to first read |
| **Human evaluator** | wants to poke without installing | browser playground / console | 0 installs (post-V1: WASM playground, Foundry console) |
| **CI / production** | Dockerfile, GitHub Action, lockfile | pinned version, checksum-verified, non-interactive | reproducible, silent |

The agent budget — **≤ 3 tool calls, 0 web searches** — is the design constraint that everything
below serves. A typical agent transcript should be:

```
call 1:  curl -fsSL https://stratadb.org/install.sh | sh -s -- --quiet
call 2:  strata agents guide          # complete offline usage guide, from the binary itself
call 3:  strata --db ./app-data kv put greeting '"hello"' && strata --db ./app-data kv get greeting
```

---

## 3. What exists today (verified inventory)

The channel skeleton already exists — more than the adoption-strategy doc assumed. What is missing is
coherence, the agent-facing layer, and release automation.

| Channel | Artifact | State |
|---|---|---|
| **cURL** | `stratadb.org/public/install.sh` — platform detect, GitHub Releases download, PATH setup for bash/zsh/fish, teach-next-step epilogue | **Live.** Human UX is good. No checksum verification, no version pinning, no non-TTY/agent mode; suggested first commands don't match binary behavior (§6.1) |
| **CLI binary** | `strata` from `crates/cli` — REPL + one-shot, global `--json`/`--format`, `run --command-json` (structured command execution), `strata init` (returns JSON with `next_steps`) | Built, `publish = false`. No agent guide surface yet |
| **Executor metadata** | `executor/src/cli_metadata.rs`, `idl_tooling.rs`, `error_registry.rs` | **The key asset.** A machine-readable catalog of every command and error already exists in code; nothing surfaces it to users or agents yet |
| **pip / uv** | `stratadb` on PyPI (strata-python repo, PyO3/maturin), v0.14.5 | Live, binds the **old** architecture; V1 cutover pending (M9) |
| **npm** | `@stratadb/core` (strata-nodesdk repo, napi-rs prebuilds), v0.15.0 | Live, old architecture; V1 cutover pending |
| **MCP** | `strata-mcp` repo — Rust server, 61 tools | Works, but install is `cargo install`/from-source only — a non-starter for MCP's config-file ecosystem. No `npx` path, no registry listings |
| **Homebrew** | Formula drafted (worktree), placeholder URLs/sha | Not shipped; no tap |
| **Website / docs** | `stratadb.org` (Astro): getting-started, concepts, reference, cookbook, guides; `llms.txt` + `llms-full.txt` generated pages | Live. llms.txt not yet install-aware or version-stamped |
| **UI console** | `strata-foundry` (SwiftUI macOS app) | On ice during V1 per project policy; reserve the download slot, do not couple |
| **Docker / GH Action** | — | Do not exist |
| **Cargo (as library)** | workspace crates | `-next` names, unpublished; shed suffix at M9B |

### 3.1 Coherence findings (must fix regardless of design choices)

These were found by cross-reading the artifacts; each one individually breaks the agent budget
because an agent following one artifact's pointers lands on a different artifact's reality.

| # | Finding | Evidence |
|---|---|---|
| F1 | **Four different GitHub org names in shipped/drafted artifacts.** An agent resolving source or releases gets four different answers | `strata-ai-labs/strata-core` (install.sh, brew draft), `strata-systems/strata-*` (strata-mcp README), `stratalab/strata-node` (nodesdk package.json), `anibjoshi/strata` (workspace Cargo.toml `repository`) |
| F2 | **No version train.** core 0.6.1, cli 1.0.0, python 0.14.5, node 0.15.0. "What version of Strata do you have?" has no answer | workspace + SDK manifests |
| F3 | **install.sh teaches commands the binary doesn't do.** Epilogue suggests bare `strata kv put greeting "hello world"`, but a bare one-shot command opens **the current working directory** as a durable database (`cli/src/open.rs:30`) — turning `$HOME` or a repo root into a Strata database as a side effect of a hello-world | install.sh `print_success` vs `open.rs` |
| F4 | **No checksum verification or version pinning in install.sh.** `curl \| sh` from GitHub `releases/latest` with no sha256 check and no `STRATA_VERSION` override | install.sh `get_latest_version`/`download_and_install` |
| F5 | **install.sh advertises `strata ai`** — a surface that doesn't exist in cli | install.sh epilogue |
| F6 | **MCP server requires a Rust toolchain to install.** The MCP ecosystem's lingua franca is an `npx`/`uvx` one-liner in a JSON config | strata-mcp README |

---

## 4. Design principles

**P1 — The binary is the documentation.** Every artifact an agent can reach locally (binary, wheel,
npm package) embeds its complete usage guide, command catalog, and error registry, generated from the
executor's metadata (the IDL, workstream #5). The web mirrors the artifact, never the reverse.

**P2 — Every install ends by teaching the next step**, and the taught step must be *tested against
the shipped binary* (F3 is what happens otherwise).

**P3 — No surprising side effects.** First contact never creates files the user didn't name.
Deterministic beats magical, especially for agents that run commands in whatever cwd they happen
to be in.

**P4 — Errors teach.** Every error carries its stable code (`<class>.<area>.<detail>` — already the
contract), a one-line hint, and a stable short URL. A failed call should be self-correcting on the
next attempt without a web search.

**P5 — One name, one version, one release train.** A single canonical org, fixed package names, one
version number across binary/wheel/npm/MCP, released together from one tag.

**P6 — Same verbs everywhere.** CLI, MCP tools, Python, and Node expose the executor's command
surface under the same names with the same JSON shapes and the same error codes. Learning one channel
is learning all of them. (This is the executor charter; the first-run experience is where it pays.)

**P7 — Onboarding is a benchmarked surface.** Agent-driven install evals with tool-call and
web-search budgets, run per release, with regression gates.

---

## 5. Golden paths (target transcripts per channel)

These are the *specified* experiences — each one becomes a CI-verified transcript (§10). Human and
agent variants shown where they differ.

### 5.1 cURL (human)

```
$ curl -fsSL https://stratadb.org/install.sh | sh
  ✓ Detected linux x86_64
  ✓ Downloaded strata v1.0.0 (sha256 verified)
  ✓ Installed to ~/.strata/bin/strata
  ✓ Added to PATH in ~/.bashrc

  Strata is ready.  Restart your shell or run:  source ~/.bashrc

  Try:
    strata                      Interactive REPL (in-memory, nothing written to disk)
    strata ./mydb               Open or create a database at ./mydb
    strata agents guide         Full usage guide (for you or your AI agent)

  Docs: https://stratadb.org/docs
```

### 5.2 cURL (agent / non-TTY)

Non-TTY output is already de-colored; add `--quiet` (or auto-detect `! -t 1`) for a
line-oriented, parseable epilogue, plus `--json` for structured:

```
$ curl -fsSL https://stratadb.org/install.sh | sh -s -- --quiet
strata 1.0.0 installed: /home/user/.strata/bin/strata (sha256 verified)
path: updated ~/.bashrc; for this session: export PATH="$HOME/.strata/bin:$PATH"
next: strata agents guide
```

Pinning for reproducibility: `STRATA_VERSION=1.0.0 curl ... | sh` and `sh -s -- --version 1.0.0`.

### 5.3 Homebrew

```
$ brew install stratadb/tap/strata
...
==> strata was installed
    Run `strata agents guide` for the full usage guide.
```

Formula lives in a `homebrew-tap` repo under the canonical org; bumped automatically by the release
pipeline (§9). The drafted formula is correct in shape — it needs real URLs, shas, and a caveats
block that teaches the same three commands as install.sh.

### 5.4 cargo

```
$ cargo install strata-cli          # the binary
$ cargo add stratadb                # embedded library use in a Rust project
```

Both published from the M9B rename (crates shed `-next`). `strata-cli`'s crates.io README is the
same generated quickstart as everywhere else.

### 5.5 pip / uv (Python)

```
$ uv add stratadb          # or: pip install stratadb
$ python -c "
import stratadb
db = stratadb.Strata('./app-data')
db.kv.put('greeting', 'hello')
print(db.kv.get('greeting'))
"
hello
```

Requirements on the wheel (strata-python repo):
- Prebuilt wheels: manylinux + musllinux (x86_64, aarch64), macOS (arm64, x86_64), Windows x86_64.
  abi3 so one wheel per platform. **No Rust toolchain ever required to `pip install`.**
- `py.typed` + complete stubs — agents read type stubs before docs.
- `stratadb.agents_guide()` returns the same generated guide as `strata agents guide` (P1); the
  README embedded as PyPI long-description contains the full quickstart inline, because agents
  read `site-packages` and lockfile metadata before the web.

### 5.6 npm / npx (Node)

```
$ npm install @stratadb/core
$ node -e "
const { Strata } = require('@stratadb/core');
const db = new Strata('./app-data');
db.kv.put('greeting', 'hello');
console.log(db.kv.get('greeting'));
"
hello
```

Same requirements as Python: napi prebuilds for the same platform matrix (exists), complete `.d.ts`
(exists), embedded README quickstart, `agentsGuide()` export.

### 5.7 MCP (the agent-runtime channel)

One line for every major client:

```
$ claude mcp add strata -- npx -y @stratadb/mcp --db ~/strata/agent-memory
```

or the config snippet (Claude Desktop, Cursor, Windsurf, VS Code — same JSON dialect):

```json
{ "mcpServers": { "strata": { "command": "npx", "args": ["-y", "@stratadb/mcp", "--db", "~/strata/agent-memory"] } } }
```

`@stratadb/mcp` is a thin npm shim that resolves/downloads the platform binary (the same GitHub
release asset install.sh uses, sha-verified, cached under `~/.strata/bin`) and executes it. This is
the standard MCP distribution pattern and eliminates F6. Design details in §8.

### 5.8 Docker

```
$ docker run -it -v "$PWD/data:/data" stratadb/strata /data
strata:data>
```

A `FROM scratch`/distroless image containing just the static musl binary; entrypoint is the CLI.
Primary use: instant REPL try-out and CI. Not a server — the image docs must repeat the embedded
framing (Strata is SQLite-shaped, not a service).

### 5.9 GitHub Action

```yaml
- uses: stratadb/setup-strata@v1
  with: { version: "1.0.0" }
```

Thin wrapper over install.sh with pinning + caching. Exists for CI pipelines and for *agents that
write CI pipelines*.

### 5.10 UI console (post-V1 slot)

`strata-foundry` is on ice during V1 (project policy). The design reserves: a `/download` page slot
on stratadb.org, and the rule that the console bundles its own engine but reports the same unified
version. Nothing in V1 couples to it.

---

## 6. First contact with the binary

### 6.1 Fix the bare-command footgun (F3)

Current behavior: `strata kv put greeting hello` with no path opens **cwd** as a durable database —
creating WAL/manifest/lock structures wherever the user (or agent!) happens to be standing. Agents
run one-shot commands from arbitrary directories; this litters repos with accidental databases and
makes hello-world non-idempotent.

**Decided (2026-07-06) — resolution order for one-shot commands:**

1. Positional path or `--db <path>` — explicit, wins.
2. `STRATA_DB` environment variable — lets an agent set the target once per session.
3. Otherwise: **refuse with a teaching error**, never open cwd implicitly:

```
error [invalid_argument.cli.no_database]: no database specified
  hint: pass a path (strata ./mydb kv put …), set STRATA_DB, or use --cache for ephemeral
```

`strata` with no arguments (interactive) opens the REPL in **cache mode** — full functionality,
zero files created, banner states exactly that:

```
strata 1.0.0 — in-memory session (nothing persisted; open a path to keep data)
type `help` for commands, `open ./mydb` to persist  |  agents: run `strata agents guide`
strata:cache>
```

### 6.2 `strata init` (exists — align it)

`strata init` already returns structured JSON with `next_steps`. Keep it minimal (create
`~/.strata`, report), and make its `next_steps` the same canonical three commands taught by
install.sh and brew caveats. One source generates all of these strings (§7).

### 6.3 `strata doctor`

One command an agent or human runs when anything is off:

<!-- illustrative -->
```
$ strata doctor --json
{ "binary": "1.0.0", "platform": "linux-x86_64", "home": "~/.strata", "path_ok": true,
  "databases_seen": [...], "issues": [] }
```

Checks: version, platform, PATH visibility, `~/.strata` permissions, and (given a path) database
health summary. Every failure includes an error code + hint (P4). This is also the install
verification step: `install.sh` ends by running `strata doctor --quiet` instead of just `--version`.

---

## 7. The self-describing surface (`strata agents`)

**This is the centerpiece of the agent story, and it is nearly free**: `executor` already
maintains `cli_metadata.rs`, `idl_tooling.rs`, and `error_registry.rs` — a complete machine-readable
catalog of the command surface and error codes. Nothing surfaces it. Expose it as a first-class
subcommand family:

| Command | Output | Consumer |
|---|---|---|
| `strata agents guide` | Complete markdown usage guide: mental model (6 primitives, branches, time travel), install matrix, the 20 most-used commands with examples, error-code cheat sheet, links as *stable slugs* not prose | Any agent, offline; equivalent of `llms-full.txt` but version-locked to the installed binary |
| `strata agents commands [--json]` | Full command catalog: name, args, types, output shape, examples — generated from executor metadata | Agents needing exact signatures; SDK/docs generators |
| `strata agents errors [--json]` | The error registry: every code, class, meaning, hint | Agents recovering from failures |
| `strata agents init` | Writes `.strata/AGENTS.md` into the current repo and (with confirmation) appends a pointer block to the repo's `AGENTS.md`/`CLAUDE.md`; offers an `.mcp.json` entry | Repo-level onboarding: after this, every future agent session in that repo knows Strata without any discovery |

Generation rules:

- All of it is **generated from executor metadata at build time** — the guide can never drift from
  the binary (drift is a CI failure, §10). This is the first consumer of the IDL workstream (#5):
  IDL → CLI help, `agents guide`, MCP tool schemas, SDK docstrings, website reference, llms.txt.
- The same generated guide ships as: crates.io README (`strata-cli`), PyPI long-description +
  `stratadb.agents_guide()`, npm README + `agentsGuide()`, `strata-mcp` orientation tool (§8), and
  `stratadb.org/llms-full.txt`. **One generator, seven mirrors.**
- `--help` everywhere mentions it. The REPL banner mentions it. install.sh's `next:` line is it.
  An agent that lands anywhere on the surface is one hop from the whole map.

### 7.1 The repo-level pointer block

What `strata agents init` appends to `AGENTS.md`/`CLAUDE.md` (kept under ~10 lines on purpose —
it's a pointer, not a manual):

```markdown
## Strata
This repo uses Strata (embedded database — SQLite-shaped, zero-config).
- Full usage guide: run `strata agents guide` (offline, version-matched)
- Command catalog JSON: `strata agents commands --json`; errors: `strata agents errors --json`
- Database path: ./app-data (set STRATA_DB or pass --db; never rely on cwd)
- Structured output: add --json to any command; execute JSON commands via `strata run --command-json`
```

### 7.2 Errors that teach (P4)

The error contract (`<class>.<area>.<detail>`, registry in executor) gains two fields surfaced on
every channel: `hint` (one actionable line) and `ref` (stable short slug, e.g.
`stratadb.org/e/failed_precondition.embedding_model_mismatch`). CLI human output renders the hint;
`--json` output and SDK exceptions carry code + hint + ref. Tests continue to assert codes only
(hard rule 29). Result: an agent that gets an error can self-correct on the next tool call — the
error *is* the documentation for the failure mode.

---

## 8. MCP experience

MCP is the channel where "user" and "agent" collapse, so its first-run bar is highest: a person
pastes one config line, and the *model* has to succeed from there.

1. **Distribution: `npx -y @stratadb/mcp`** (F6 fix). Thin npm shim → platform binary from the same
   release assets, sha-verified, cached. `uvx stratadb-mcp` mirror for the Python-native crowd is a
   cheap follow-on (same shim pattern on PyPI).
2. **V1 architecture: thin transport over executor**, exactly like cli — same verbs, same
   JSON shapes, same error codes (P6). **Decided (2026-07-06): the server folds into the main
   binary as `strata mcp serve`** (the shim then just execs the installed binary); the `strata-mcp`
   repo becomes the packaging/registry home. One artifact, version-locked with the engine, and
   every CLI install is automatically an MCP install.
3. **Tool surface: curated, not exhaustive.** 61 tools blow out client tool budgets and drown the
   model in choices. Ship ~15–20 curated tools covering the 6 primitives' core verbs + search +
   branch, and two meta-tools:
   - `strata_guide` — returns the agents guide (orientation; described so models call it first
     when unsure);
   - `strata_command` — escape hatch that executes any cataloged command by name + JSON args
     (the `run --command-json` machinery), so the long tail stays reachable without 40 extra
     tool schemas.
4. **Tool descriptions are teaching text** — each one carries a one-line example and names its
   error codes. They are generated from the same metadata as everything else.
5. **Registry presence:** official MCP registry, Smithery, mcp.so, Claude Desktop extension bundle.
   Registry listings are how agent *runtimes* discover Strata — this is the agent-world equivalent
   of SEO, and it's a few hours of work per registry once the npx path exists.

---

## 9. One name, one version, one release train (P5)

### 9.1 Naming decisions (blocking — F1/F2)

| Decision | Resolution | Notes |
|---|---|---|
| GitHub org | **`stratalab`** (decided 2026-07-06) | replaces `strata-ai-labs` (install.sh, brew draft), `strata-systems` (strata-mcp README), `anibjoshi` (workspace Cargo.toml); nodesdk already points here. Sweep all artifacts |
| Binary | `strata` | settled |
| PyPI | `stratadb` | live — keep |
| npm | `@stratadb/core`, `@stratadb/mcp` | scope live — keep |
| crates.io | `stratadb` (lib), `strata-cli` (bin) | at M9B rename |
| Docker Hub / GHCR | `stratadb/strata` | |
| Version | **one number everywhere**, starting `1.0.0` at V1 promotion | SDK versions jump to match; F2 dies |

### 9.2 The release train

One tag on `strata-core` (`vX.Y.Z`) fans out, fully automated:

```
tag v1.0.0
 ├─ build matrix: {linux-gnu, linux-musl, macos, windows} × {x86_64, aarch64}
 │    → GitHub Release assets + SHA256SUMS (+ sigstore signing, post-V1 ok)
 ├─ publish crates (stratadb, strata-cli)
 ├─ dispatch → strata-python:   build wheels at same version → PyPI
 ├─ dispatch → strata-nodesdk:  napi prebuilds at same version → npm
 ├─ dispatch → strata-mcp:      shim republish pinning same version → npm
 ├─ bump homebrew-tap formula (urls + shas)
 ├─ push Docker image
 └─ stratadb.org: regenerate install manifest, llms.txt version stamp, docs reference
```

install.sh stops calling `releases/latest` at runtime and instead reads a small
`stratadb.org/release.json` manifest (version, per-platform URLs + shas) — which also gives agents
and the GitHub Action a stable, rate-limit-free version endpoint.

### 9.3 llms.txt spec (stratadb.org)

The generated `llms.txt`/`llms-full.txt` already exist; upgrade them to be install-aware:

- **Header block:** what Strata is (embedded, SQLite-shaped, 6 primitives), current version,
  release date.
- **Install matrix:** the exact one-liners from §5, one per channel, marked by ecosystem.
- **The pointer that ends web search:** "after install, run `strata agents guide` — the binary
  carries complete offline documentation."
- `llms-full.txt` = the same generated guide the binary embeds. Version-stamped so an agent can
  detect skew between site and installed binary.

---

## 10. Measurement: onboarding as a benchmarked surface (P7)

### 10.1 Agent onboarding evals

In `strata-eval` (framework exists): clean container, an agent (Claude, Codex) with shell access,
tasks like —

- "Install Strata and store/retrieve a key."
- "Add Strata to this Python project and index 50 documents for semantic search."
- "Wire Strata as MCP memory for this Claude Desktop config."

**Metrics:** tool calls to first verified write, web searches (target: 0), hallucinated
flags/commands (each one is a docs bug — file it), wall clock, success rate.
**Cadence:** every release candidate, plus when any channel artifact changes. Regressions gate the
release exactly like a perf regression. Hallucination reports are a goldmine: they tell you what
agents *expect* the surface to be — sometimes the right fix is an alias, not a doc.

### 10.2 CI transcript guards (per artifact)

- install.sh matrix: {sh, bash, zsh, fish} × {ubuntu, macos} × {x86_64, aarch64}, TTY and non-TTY,
  fresh and re-run (idempotency), pinned and latest; asserts the epilogue's suggested commands
  actually work against the installed binary (F3 can never come back).
- `strata agents guide/commands/errors`: golden-tested against executor metadata — drift fails CI.
- Each golden path in §5 is an executable transcript test in its owning repo.
- Wheel/napi smoke: `pip install stratadb && python -c "...put/get..."` on every platform in the
  matrix, same for npm — *post-publish* against the real registries, not just pre-publish.

---

## 11. Delivery map

| # | Deliverable | Repo | Depends on | Size |
|---|---|---|---|---|
| D1 | Repo-URL sweep to `stratalab` across all artifacts (org decided) — **strata-core swept 2026-07-06, other repos pending** | all | — | hours |
| D2 | Bare-command footgun fix + teaching error + `STRATA_DB` (§6.1) — **landed 2026-07-06** | strata-core (cli) | — | S |
| D3 | `strata agents` family generated from executor metadata (§7) — **landed 2026-07-06** (catalog coverage grows with IDL #5) | strata-core | cli_metadata/idl_tooling (exist); full IDL (#5) refines later | M |
| D4 | Error `hint` + `ref` surfaced on all channels (§7.2) — **landed 2026-07-06** (refs are `stratadb.org/e/<code>`) | strata-core | error_registry (exists) | S–M |
| D5 | `strata doctor` (§6.3) — **landed 2026-07-06** | strata-core | — | S |
| D6 | install.sh: sha256 verify, `STRATA_VERSION`/`--version`, `--quiet`/`--json`, release.json manifest, doctor-verify, aligned epilogue (§5.1–5.2, F4) | stratadb.org | D2, release assets | S–M |
| D7 | Release train automation (§9.2) | strata-core + all | D1 | M |
| D8 | `@stratadb/mcp` npx shim + `strata mcp serve` V1 transport + curated tools + registry listings (§8) — **`strata mcp serve` + 20 curated tools landed 2026-07-06; shim + registries pending (strata-mcp)** | strata-core + strata-mcp | D3, D7; M9 SDK cutover | M–L |
| D9 | SDK cutover carries agent surfaces: guide fn, stubs/types, embedded quickstart, unified version (§5.5–5.6) | strata-python, strata-nodesdk | M9, D3, D7 | M (each) |
| D10 | Homebrew tap, Docker image, GitHub Action (§5.3, 5.8, 5.9) | new small repos | D7 | S (each) |
| D11 | llms.txt upgrade + docs install matrix (§9.3) | stratadb.org | D3, D7 | S |
| D12 | Onboarding evals + CI transcript guards (§10) | strata-eval + each repo | D6–D9 | M |

Sequencing: **D1 first** (it's a decision, and every artifact embeds the answer). D2–D5 are
strata-core work that can land now on the V1 line and belong to M9's CLI/executor scope. D6 is
independent website work. D7 unlocks D8–D11. D12 wraps the train.

The V1 roadmap hook is **M9** ("Executor, CLI, SDK, tests, benches, docs cutover"): D2–D5 and D8's
transport are M9 implementation slices; D9 is the SDK-cutover milestone itself; D12 belongs beside
M9F/M10's bench re-baselining.

---

## 12. Decisions and open questions

### Decided (2026-07-06)

1. **Canonical GitHub org: `stratalab`** — sweep all artifacts (D1).
2. **MCP folds into the main binary** as `strata mcp serve`; `strata-mcp` repo becomes the
   npx-shim packaging/registry home (§8).
3. **Bare one-shot commands refuse with a teaching error** — explicit path → `STRATA_DB` →
   refuse; never open cwd implicitly (§6.1).
4. **Error refs are stable short slugs**: `https://stratadb.org/e/<code>` — implemented in D4
   across the render config, IDL, and generated indexes (decided 2026-07-06).

### Still open (owner decisions)

4. **Telemetry**: recommendation is **none, ever** in the embedded artifact (privacy is a selling
   point for an embedded DB; also removes a class of first-run friction). install.sh likewise. OK?
5. **Windows**: wheels/npm prebuilds are table stakes; is native `strata.exe` + winget/scoop in V1
   scope or fast-follow?
6. **`strata ai`** (advertised by install.sh today, F5): is the strata-ai agent part of the V1
   binary story, or does that line go away until it ships?
