# User Flows

How a user encounters Strata, from install to daily use.

## The architectural premise

Strata is a **service-on-disk hybrid**, shaped like Cargo + rustup, *not*
like SQLite and *not* like Ollama.

- **Strata-the-product** gets installed once per machine and has user-level
  state: hardware-tuned config, downloaded model weights, recent-DB list.
- **Strata-the-database** is a self-contained directory that you can create
  anywhere, copy between machines, and back up by copying.
- **Shared access** is a property of the database, not a runtime mode.
  At creation, a database is either **single** (one process at a time, the
  default) or **shared** (multiple processes via a per-database server).
  See [single-vs-shared.md](single-vs-shared.md) for the full model. There
  is no global Strata daemon — a "server", when one runs, is scoped to
  exactly one shared database and bound to `<db>/strata.sock`.

This shape exists because two facts pull in opposite directions:
- AI model weights are GBs. They must be downloaded once and shared across
  every database — so we can't be pure SQLite.
- A database is a unit of work that often belongs with a project. It must
  be portable and not "registered" with a service — so we can't be pure
  Ollama.

Cargo is the cleanest precedent: rustup installs the toolchain and a shared
crate cache; each project is just a directory you can move. Strata's first
run is "rustup-init"; creating a database is `cargo new`; opening one is
`cd project && cargo run`.

## The shape of Strata's state

Where things live on disk. This is the contract — every flow below
manipulates one of these locations.

### User-level (one per machine)

| Path | Contents | Created by |
|---|---|---|
| `~/.config/strata/config.toml` | Preferences, recent DBs, last opened, hardware profile | `strata setup` |
| `~/.cache/strata/models/` | Downloaded model weights (embeddings, generation) | `strata setup`, `strata models pull` |
| `~/.local/share/strata/log/` | Diagnostic logs (rotated) | Any `strata` invocation |

On macOS, the Foundry app uses the same `~/.config/strata/` and
`~/.cache/strata/`. It does *not* use a separate `~/Library/Application
Support/Strata/` — one source of truth, shared between CLI and GUI.

### Per-database (portable, copyable)

| Path | Contents |
|---|---|
| `<db>/manifest.toml` | Database identity, format version, creation time |
| `<db>/wal/` | Write-ahead log |
| `<db>/sst/` | Sorted string tables (LSM levels) |
| `<db>/branches/` | Branch metadata |
| `<db>/recipes/` | Per-DB recipe configs |
| `<db>/strata.sock` | Shared-access socket (only while `strata up` is running on this DB) |
| `<db>/strata.pid` | PID of the shared-access server (only while running) |

A database directory is the unit of portability. Copying `<db>/` to another
machine works (assuming compatible Strata versions and the target machine
has the right models in its `~/.cache/strata/models/`).

## Flow 1 — First-time setup

The user installed `strata` (via Homebrew, cargo install, or the macOS
installer). They've never run it on this machine. The next thing they do
is run something — `strata`, `strata new`, or open Foundry.

### What happens

If `~/.config/strata/config.toml` does not exist, **any** entry point
(`strata`, `strata new`, Foundry first launch) triggers setup. Setup is
also explicitly invokable as `strata setup`.

```
$ strata
First time on this machine. Setting up Strata.

Detecting hardware…
  CPU: Apple M3 Pro (12 cores, AVX/NEON: NEON, AMX)
  RAM: 32 GB
  Disk: 1.2 TB free on /
  GPU: Apple Silicon (Metal)

Tuning defaults…
  block_cache_size: 4 GB (RAM × 0.125)
  compaction_threads: 6 (cores × 0.5)
  embed_runtime: metal

Downloading default models…
  bge-small-en-v1.5      (133 MB)  ✓
  qwen2.5-0.5b-instruct  (380 MB)  ✓

Wrote ~/.config/strata/config.toml
Wrote 2 models to ~/.cache/strata/models/

Setup complete in 47 s.
```

### On-disk effects

1. Probe hardware (CPU features via `cpuid`/`sysctl`, RAM via `sysctl`,
   GPU via Metal/CUDA discovery, disk free via `statfs`).
2. Write `~/.config/strata/config.toml` with derived defaults plus a
   `[hardware]` section recording the probe (so re-tuning on hardware
   change is detectable).
3. Create `~/.cache/strata/models/`.
4. Download the default model bundle (embedding + small generation
   model). Configurable via `--minimal` to skip generation, or `--no-models`
   to skip both.

### Idempotency

Re-running `strata setup` reprobes hardware and refreshes config but does
**not** re-download models. `strata setup --refresh-models` does. Setup is
safe to re-run; it never destroys user data.

### Error cases

| Case | Behavior |
|---|---|
| No network during model download | Setup writes config, marks models as deferred, prints `strata models pull` instruction. CLI is usable for non-AI work immediately. |
| Insufficient disk for models (<2 GB free) | Setup refuses, prints exact free-space delta needed. No partial state. |
| Hardware probe fails (rare — non-x86, non-ARM) | Setup writes a minimal conservative config and warns. User can override later. |
| Permission denied on `~/.config/strata/` | Setup refuses with the exact path and the `chown` command to fix it. |

## Flow 2 — Create a new database

The user has Strata set up. They want a new database.

### Default path (single-mode)

```
$ strata new
Creating database at ~/Documents/Strata… ✓
Mode: single (one process at a time)
Set as current.
```

`strata new` defaults to **single-mode** — the dominant case for personal
agents, project-rooted scratch DBs, and notebooks. To create a shared
database from day one, pass `--shared`:

```
$ strata new ~/team/kb --shared
Creating database at ~/team/kb… ✓
Mode: shared (multiple processes via a per-DB server)
Set as current.
```

A single-mode database can be promoted later with `strata share start`
without recreation. See [single-vs-shared.md](single-vs-shared.md).

If `~/Documents/Strata` already exists and is a Strata database, refuse
with the path and a hint to use `strata` (without `new`) to open it.
If the path exists but is *not* a Strata database, refuse — never
overwrite user files.

### Custom path

```
$ strata new ~/Documents/GitHub/myapp/data
Creating database at ~/Documents/GitHub/myapp/data… ✓
Set as current.
```

Path is created if its parent exists. The parent is **not** auto-created —
typo protection.

### What "Set as current" means

After `strata new`, the new path is appended to `recent_dbs` in
`~/.config/strata/config.toml` and set as `last_opened`. Subsequent
invocations of `strata` (with no args) open this DB by default.
"Current" is *not* a hard mode — it's a default that's overridable
by `--db`, env var, or directory walk-up. (See Flow 3.)

### On-disk effects

1. `mkdir -p <path>` (if parent exists) and create the standard
   sub-layout (`wal/`, `sst/`, `branches/`, `recipes/`).
2. Write `<path>/manifest.toml` with a fresh database UUID, the
   current Strata format version, and creation timestamp.
3. Seed default recipes (the `seed` operation that's currently a
   separate command).
4. Update `~/.config/strata/config.toml`:
   - Append to `recent_dbs` (max 10 entries, oldest evicted)
   - Set `last_opened = "<path>"`

### Error cases

| Case | Behavior |
|---|---|
| Path exists, is a Strata DB | Refuse with hint: "Already a Strata database. Use `strata` to open it." |
| Path exists, is not a Strata DB (has files) | Refuse with the path and contents. Never overwrite. |
| Parent directory missing | Refuse with the missing parent path. Don't auto-create — typo protection. |
| Permission denied | Refuse with the exact path. |
| Disk full | Refuse cleanly; don't leave a half-created directory. |

## Flow 3 — Access an existing database

The user wants to open a database that already exists.

### Resolution order (the rule)

When `strata` is invoked with no `--db` flag, it resolves the database
path in this order. First match wins:

1. `--db <path>` flag (explicit, beats everything)
2. `STRATA_DB` environment variable
3. Walk up from CWD looking for a directory containing `.strata/`
   (git-style — the database is *adjacent*, not inside)
4. `last_opened` from `~/.config/strata/config.toml`
5. `~/Documents/Strata` if it exists and is a valid DB
6. Otherwise: prompt to run `strata new`

The `.strata/` discovery is what makes per-project workflows feel right.
If the user has `~/Documents/GitHub/myapp/data/` as their DB, they create
a `~/Documents/GitHub/myapp/.strata` symlink (or marker file) pointing at
it, and then `strata` from anywhere inside `myapp/` Just Works.

### One-shot vs. persistent

```
$ strata --db /path/to/db query "users where age > 21"
```
One-shot: `--db` is a per-invocation flag. Doesn't change `last_opened`.

```
$ strata cd /path/to/db
Now using /path/to/db
```
Persistent: updates `last_opened` and adds to `recent_dbs`. Subsequent
bare `strata` invocations open this DB.

The `cd` verb is deliberate — it borrows the mental model of changing
directories, with no daemon and no global lock. Two terminals can `strata
cd` to different DBs and they don't fight.

### From inside a database directory

```
$ cd ~/Documents/GitHub/myapp/data
$ strata
Opened ~/Documents/GitHub/myapp/data
> _
```

If CWD *is* a Strata database directory (has `manifest.toml`), open it
directly. This is the inverse of rule 3 — same intent, no marker needed.

### Error cases

| Case | Behavior |
|---|---|
| `--db` path doesn't exist | Refuse with the path. Suggest `strata new <path>` if the parent exists. |
| Path exists but isn't a Strata DB | Refuse with the path and what's missing (`manifest.toml`). |
| Format version mismatch (DB newer than CLI) | Refuse, name both versions, link to upgrade docs. |
| Format version mismatch (DB older) | Open read-only, warn, offer `strata migrate`. |
| Single-mode DB, second process tries to open | Refuse with the holder's PID and the message *"This database is single-user. Run `strata share start <path>` to make it shared."* |
| Shared-mode DB, server already running | Connect via the socket transparently. |
| Shared-mode DB, server not running | Auto-start the server, then connect. (No `share start` needed.) |
| `<db>/strata.sock` is stale (server died) | Detect, clean up, auto-start a fresh server. |

## Flow 4 — Menu bar launcher (macOS)

A small helper application that lives in the menu bar and provides quick
access to recent databases and the daemon. Ships with Foundry; not a
separate install.

### What it is

- A SwiftUI `MenuBarExtra` app, built as an `LSUIElement` (no dock icon).
- Bundled inside Foundry's `.app` and registered to launch at login (with
  the user's permission, asked at Foundry first run).
- Reads the *same* `~/.config/strata/config.toml` the CLI uses. No
  separate state.

### What it does

```
Strata
├─ Open Database…             ⌘O
├─ New Database…              ⌘N
├─ ──────────
├─ Recent
│   ├─ ~/Documents/Strata
│   ├─ ~/Documents/GitHub/myapp/data
│   └─ Clear Recent
├─ ──────────
├─ Shared access
│   ├─ ~/team/kb              (shared — running, 2 clients)
│   └─ ~/Documents/Strata     (single — convert to shared)
├─ Open Foundry
├─ Preferences…
└─ Quit Strata
```

Click a recent → launches Foundry with `--db <path>` (or activates the
existing window if it's already on that DB). "New Database…" → launches
Foundry's new-DB sheet. "Open Foundry" → activates Foundry without
opening a DB.

### What it does NOT do — the rule

**The menu bar process must not hold any database state, run any queries,
or perform any background work.** It is a launcher. If it ever needs to,
it is wrong.

This rule is what keeps Strata from drifting into Ollama-shape. The line
is bright:

| Allowed | Not allowed |
|---|---|
| Read `config.toml` for recents | Open a DB handle |
| Spawn Foundry / `strata` CLI | Run a query, even a "small" one |
| Read each recent's `<db>/strata.pid` to show server status | Manage server lifecycle beyond start/stop buttons |
| Display recent DBs as menu items | Cache "the last query result" for quick display |

If a future feature wants to live in the menu bar and do work, that
feature wants a daemon, not a menu bar item. They are different things.

### Idle footprint

Target: ≤8 MB RSS, no background CPU when idle. The menu bar app reads
`config.toml` on click, not continuously. The PID file is checked when
the user opens the menu, not polled.

## Foundry handoff

Foundry is the desktop GUI. It uses the same on-disk format and the same
user-level state.

- **First Foundry launch on a fresh machine** → triggers `strata setup`
  (Flow 1) via a guided welcome screen. The same probe, the same model
  download. After setup, Foundry shows the "no database yet" state with
  the same affordances as Flow 2.
- **Foundry's "Open Database"** → uses the same resolution as Flow 3,
  surfacing recents from `config.toml`.
- **Foundry's "New Database"** → executes Flow 2.
- **Foundry can drive the daemon** → start/stop `strata up` from the app
  preferences, useful for users who want shared access between Foundry
  and CLI scripts.

The CLI and Foundry are *peers*, not master/slave. Either one can do any
of the flows, and they see each other's state immediately because they
share `~/.config/strata/`.

## Decision log (what we deliberately chose)

These are the calls that make the rest of the system fall into place.
Worth re-litigating if assumptions change.

| Decision | Why | Tradeoff |
|---|---|---|
| Service-on-disk hybrid, not pure SQLite | GB-scale model weights need a shared cache | First-run ceremony is unavoidable |
| Database is a directory, not a service-managed entity | Portability, project-rooted workflows, easy backup | No central registry — users can lose track of DBs |
| Shared-access is a manifest mode (single or shared), set at creation | Intent-as-data; lets single-mode DBs be optimized aggressively; per-DB scope keeps the model honest | Users have to think about it once at creation (defaulting to single makes this rare) |
| `~/.config/strata/` shared between CLI and Foundry | One source of truth | Foundry must respect CLI's recent-DB list and vice versa |
| Menu bar app is a launcher, never a service | Discoverability without architectural drift | Tempting to add "small" features to it; resist |
| Git-style `.strata/` marker for project discovery | Per-project DB workflows feel native | One more thing to explain in onboarding |
| Default DB at `~/Documents/Strata` | Findable in Finder, obvious to non-technical users | Some users will want it elsewhere — `strata new <path>` covers this |

## Open questions (need decisions before implementation)

1. **Format-version migrations** — Flow 3 mentions `strata migrate` as the
   path for older DBs. We don't have one yet. Need a versioning scheme
   and a migration command before we ship 1.0.
2. **Marker file vs. symlink for project discovery** — `.strata/` as a
   directory containing the DB? A marker file pointing at the DB? A
   symlink? Each has UX and OS-portability implications. Suggest:
   marker file (`.strata` containing the path, single line). Decide before
   shipping discovery.
3. **Model bundle scope** — Flow 1 downloads "the default model bundle".
   Need to specify which models, how big total, and how to keep this
   sub-1 GB for the first-run experience.
4. **Shared-access ergonomics** — resolved. See
   [single-vs-shared.md](single-vs-shared.md). `up`/`down` retire in favor
   of `share start` / `share stop` / `share status`, and the mode lives in
   the manifest.
5. **Foundry concurrent access** — handled by the manifest mode. A
   single-mode DB in Foundry holds the only handle; a shared-mode DB
   connects via the per-DB server like any other client. The "Convert to
   shared" affordance lives in Foundry's per-DB settings panel.
6. **Telemetry on first run** — opt in, opt out, or no telemetry at all?
   Strata leans heavily on hardware tuning, so anonymous hardware-class
   stats would meaningfully improve defaults. Probably worth asking once
   at setup, with default = no.
5. **Telemetry on first run** — opt in, opt out, or no telemetry at all?
   Strata leans heavily on hardware tuning, so anonymous hardware-class
   stats would meaningfully improve defaults. Probably worth asking once
   at setup, with default = no.
