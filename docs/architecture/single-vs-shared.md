# Single vs. Shared Databases

A Strata database is either **single-mode** (one process at a time, ever)
or **shared-mode** (multiple processes through a per-database server). The
mode is recorded in the database's manifest and is a property of the
database, not a runtime flag.

This document is the reference for what each mode guarantees, how a user
moves between them, and the lifecycle of the per-database server.

## Why two modes

The realistic workload split:

- **Single user, single process** (an agent's memory, a developer's local
  scratch DB, a project-rooted notebook) — the dominant case. Wants zero
  ceremony, zero IPC overhead, aggressive optimization for one writer.
- **Multiple processes accessing one database** (two agents collaborating,
  Foundry + a CLI script, a CI runner reading while a developer writes)
  — the team / production case. Wants concurrent access without the user
  micro-managing it.

Trying to serve both with one model leaves something on the floor. A
mode at the manifest level lets each kind of database be honest about
what it is.

## Modes

### Single

```toml
# <db>/manifest.toml
mode = "single"
```

**Guarantees:**

- Exactly one process can hold the database open at a time.
- Second-process attempts get a clear error: *"This database is single-user.
  Run `strata share start` to make it shared."*
- The storage layer can use process-local state freely (caches, lock-free
  read paths, looser fsync defaults).

**Server machinery:** none. No socket, no PID file, no daemon code path
runs.

**Default for `strata new`.** Most databases start here and stay here.

### Shared

```toml
# <db>/manifest.toml
mode = "shared"
```

**Guarantees:**

- Multiple processes can access the database concurrently, mediated by a
  per-database server bound to `<db>/strata.sock`.
- The server is auto-started on first open and auto-stopped on idle.
- Storage settings are tuned for cross-process visibility (tighter fsync,
  bounded memtables, no process-local invariants).

**Server machinery:** active when in use, idle otherwise. See
[Server lifecycle](#server-lifecycle) below.

## The verb structure

| Command | Effect |
|---|---|
| `strata new <path>` | Create a single-mode database (default). |
| `strata new <path> --shared` | Create a shared-mode database. |
| `strata share start <path>` | If single → promote to shared and start the server. If shared → start the server (no-op if already running). |
| `strata share stop <path>` | Stop the server. Database stays shared. |
| `strata share status <path>` | Show server state, connected clients, uptime. |
| `strata convert <path> --single` | Demote shared → single. Refuses if anyone is connected. |

`up` and `down` retire. The `share` namespace makes the per-database scope
explicit at every call site.

## Behavior of `share start` by case

`share start` is the single entry point users learn for "I want this
database to be shareable now." It works in three cases.

### 1. Single-mode database, no active client

1. Verify the database has no active connection (refuse if held).
2. Quiesce: flush WAL, sync to disk.
3. Update `manifest.toml`: `mode = "shared"`.
4. Apply shared-mode defaults to settings the user hasn't explicitly
   overridden (durability cadence, memtable bounds, cross-process flags).
5. Daemonize the server, bind `<db>/strata.sock`, write `<db>/strata.pid`.
6. Exit successfully — the next `strata --db <path> ...` connects via the
   socket.

### 2. Shared-mode database, server not running

1. Read `<db>/strata.pid`. If alive, exit with the running PID and a
   note that the server is already up.
2. Daemonize the server, bind socket, write PID file.
3. Exit.

### 3. Shared-mode database, server already running

No-op. Exit successfully with status output.

### Failure modes

| Case | Behavior |
|---|---|
| Single-mode DB has an active connection | Refuse. Print the holder's PID. User must close the holder before `share start`. |
| `<db>/strata.sock` exists but server is dead (stale) | Detect (`/proc` check or socket connect attempt with timeout), clean up, proceed. |
| Permission denied on `manifest.toml` | Refuse with the path. |
| Disk full during quiesce | Refuse cleanly. Manifest is not updated. Database stays single. |

## Server lifecycle

The per-database server runs only while a shared database is in use.

### Auto-start (no `share start` needed)

When a process opens a shared database and finds no server running:

1. Acquire a brief promotion lock on the manifest.
2. Daemonize a server child, which binds the socket.
3. Connect to the socket as a client.
4. Exit the parent ceremony.

The user sees no ceremony. The first `strata --db <shared-db>` against a
quiet database transparently spawns the server.

### Auto-stop on idle

The server tracks connected clients. When client count drops to zero, a
configurable idle timer starts (default: 5 minutes). When the timer
expires:

1. Server closes the socket (rejecting new connections cleanly with a
   "server stopping" notice).
2. Flushes the WAL, syncs the manifest.
3. Removes `<db>/strata.sock` and `<db>/strata.pid`.
4. Exits.

The next opener triggers auto-start again.

### Always-on (CI / production)

For users who want the server to run regardless of client count:

```
strata share start <path> --keepalive
```

This sets `auto_stop = false` for the running server. The server stays up
until explicitly stopped via `strata share stop <path>` or the host
shuts down. The flag does not change the manifest — it's a runtime
property of *this* server invocation.

### Crash recovery

If the server process dies uncleanly (kill -9, OOM, panic):

- `<db>/strata.sock` and `<db>/strata.pid` are stale.
- Next opener detects this (PID lookup returns "no such process"), cleans
  up, and proceeds with auto-start.
- WAL recovery runs as it would for any unclean shutdown.

## Conversion semantics

### Single → Shared (`share start`)

Bundled with serving, as described above. The conversion itself is small:
update one field in `manifest.toml`. The risk is in **applying shared-mode
settings**: durability defaults are tighter (more frequent fsync), the
memtable bound is lower (so other processes see writes promptly), and any
single-mode optimizations that relied on process-local state are turned
off.

If the user has explicitly overridden those settings in their config,
their values are preserved — `share start` only changes settings that
are at their single-mode default.

### Shared → Single (`convert --single`)

This is the rare and dangerous direction. Spec to be filled in (see
[Open questions](#open-questions)) but the rough shape:

1. Verify no active client (refuse otherwise).
2. Stop the server cleanly.
3. Verify no in-flight transactions (refuse if any).
4. Update `manifest.toml`: `mode = "single"`.
5. Optionally relax shared-mode settings to single-mode defaults
   (durability, fsync, memtable) — only those still at their shared
   defaults.
6. Done.

The reason this needs more spec: shared-mode databases may have
state — branches created concurrently, ontology updates from multiple
clients — that single-mode invariants assume can't exist. Demotion may
need to verify those invariants hold, or refuse if they don't.

## Foundry interaction

- **Single-mode database in Foundry:** Foundry holds the only handle.
  Closing the window or quitting the app releases it. Other CLI processes
  can then open it.
- **Shared-mode database in Foundry:** Foundry connects via the socket
  like any other client. The server runs while Foundry is open and any
  CLI scripts are connected; idle timer kicks in when everyone disconnects.
- **Per-database settings panel** in Foundry has a "Mode: Single-user"
  row with a "Convert to shared" button that runs `share start` on the
  database. The reverse direction surfaces a confirmation dialog with
  the in-flight-transaction check.

## Default = single

We default to single-mode for two reasons.

### The dominant case is single

Personal agents, project-rooted scratch DBs, developer notebooks, the
"I'm trying out Strata" first-run database — all single-process, all the
time. Defaulting to shared would impose IPC overhead and a server process
on the 80% of databases that don't need it.

### Friction teaches the model

A user who hits *"This database is single-user — run `strata share start`
to make it shared"* learns the distinction once. A user who gets seamless
auto-promotion never has to think about it, then is surprised later when
single-mode optimizations aren't available, or when their shared database
behaves differently from another single user's "small" database.

The error message is the documentation. It points at the verb that solves
the problem.

## What this replaces

This RFC replaces `strata up` / `strata down` and resolves Open Question
#4 in [user-flows.md](user-flows.md). The retired behavior:

| Old | New |
|---|---|
| `strata up` (default `.strata`, ambient lifecycle) | `strata share start <path>` (explicit, per-DB) |
| `strata down` | `strata share stop <path>` |
| Implicit "is the daemon running?" status | `strata share status <path>` |
| No notion of mode in the database | `mode` in `manifest.toml` |
| Lock-held error: *"couldn't acquire lock"* | Clear error: *"single-user database; run `share start`"* |

Migration for existing databases: any database created with the old
schema has no `mode` field. The migration tool stamps `mode = "single"`
on first open after the upgrade. Users who were running `strata up` can
re-promote with `strata share start`.

## Open questions

1. **`convert --single` spec.** The shared → single direction needs a
   real specification, including: how branches created in shared mode
   are verified safe to demote, whether ontology state needs reconciling,
   what happens to in-flight transactions. Defer to a follow-up RFC; ship
   `share start` first since it covers the common direction.

2. **Idle timeout default.** 5 minutes is a guess. Real workloads
   (Foundry session lengths, CI runner patterns) should drive the default.
   Configurable in `~/.config/strata/config.toml` from day one.

3. **Multi-host shared mode.** This RFC covers single-host shared access
   via Unix domain sockets. A future "shared across hosts" mode (TCP, TLS,
   auth) is a different RFC and not in scope here.

4. **Settings divergence between modes.** When a user `share start`s a
   single-mode DB, which settings get tightened, and how do we record
   "the user explicitly set this so don't touch it" vs. "this was a
   default we're free to update"? Probably needs a `[settings.overrides]`
   section in the manifest with explicit user-set keys.

5. **Foundry idle behavior.** When a user closes Foundry on a shared
   database but the server has other connected clients, Foundry should
   detach cleanly. When Foundry is the *only* client and the user closes
   the window, the server's idle timer starts immediately. The detach
   protocol needs to be specified at the IPC level.
