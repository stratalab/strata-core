# Transparent IPC: Concurrent Process Access via Unix Domain Socket

## Problem

Strata is an embedded database — only one process can hold the write lock at a time.
In practice, multiple processes need concurrent write access to the same database:

```
strata ai (Node SDK)  ──┐
Python script (PyO3)  ──┤── all need write access to the same .strata/
CLI (strata kv put)   ──┤
Another Node app      ──┘
```

Today, whichever process opens the database first holds the exclusive `.lock`,
and every other process gets "database is already in use by another process."

This is the correct behavior for an embedded database, but it creates a poor
user experience. A user running `strata ai` in one terminal can't run
`strata kv get` in another. A Python analytics script can't read while the
Node-based AI agent is writing.

## Design: Implicit Host-Client Model

When the first process opens a database in write mode, it becomes the **host**:
- Opens the database normally (acquires `.lock`)
- Spawns a background thread listening on `.strata/strata.sock`
- Handles incoming command requests from client processes

When a subsequent process tries to open the same database:
- Detects `.lock` is held (fs2 `try_lock_exclusive` fails)
- Checks if `.strata/strata.sock` exists and is connectable
- If yes: enters **client mode** — sends commands over the socket
- If no: reports the existing "database in use" error

The user never starts a server. The user never configures ports. Any process
that opens the database — CLI, Node SDK, Python SDK — just figures it out.

```
# Terminal 1 — strata ai (Node SDK) becomes the host
$ strata ai
> Building knowledge graph...

# Terminal 2 — CLI becomes a client transparently
$ strata kv get greeting
"hello world"

# Terminal 3 — Python script, also a transparent client
$ python3 -c "
from strata import Strata
db = Strata('.strata')  # detects lock, connects via socket
print(db.kv.get('greeting'))
"
hello world
```

## Multi-SDK Requirements

The IPC layer must live in the **engine/executor crate**, not the CLI.
This is because all SDKs embed the database via FFI:

- **Node SDK** (`strata-node`): napi-rs bindings → Rust executor
- **Python SDK** (`strata-python`): PyO3 bindings → Rust executor
- **CLI** (`strata-cli`): direct Rust

All three call `Strata::open()` which calls `Database::open()` in the engine.
The socket server and client must be wired into this path so every SDK gets
IPC for free:

```
Strata::open(path)
  └── Database::open(path)
        ├── try_lock_exclusive() → success → spawn socket server → return DB
        └── try_lock_exclusive() → LOCKED
              ├── connect(strata.sock) → success → return IPC-backed handle
              └── connect failed → error "database in use"
```

### SDK-Transparent API

The `Strata` / `Session` API must work identically regardless of whether the
database is local or IPC-backed:

```rust
// Direct (host process)
let db = Strata::open(".strata")?;
let session = db.session();
session.execute(Command::KvPut { ... })?;

// IPC (client process) — same API, different backing
let db = Strata::open(".strata")?;  // transparently connects via socket
let session = db.session();
session.execute(Command::KvPut { ... })?;  // serialized over socket
```

Node and Python SDKs call into the same Rust `Strata::open()`, so they
inherit this behavior without any SDK-side changes.

## Wire Protocol

All types are already serializable:
- `Command` (121 variants) — `#[derive(Serialize, Deserialize)]`
- `Output` (40+ variants) — `#[derive(Serialize, Deserialize)]`
- `Error` (35+ variants) — `#[derive(Serialize, Deserialize)]`

**Format**: Length-prefixed MessagePack over Unix domain socket.

```
[4 bytes: payload length (big-endian u32)] [payload bytes]
```

**Request**:
```rust
#[derive(Serialize, Deserialize)]
struct Request {
    id: u64,
    command: Command,
}
```

**Response**:
```rust
#[derive(Serialize, Deserialize)]
struct Response {
    id: u64,
    result: Result<Output, Error>,
}
```

MessagePack is chosen over JSON for performance (binary, no parsing overhead)
and over bincode for forward compatibility (tagged fields survive version skew).

## Session-per-Connection

Each socket connection gets its own `Session`. This is critical because
transactions are stateful — `TxnBegin` → `KvPut` → `TxnCommit` must all
execute on the same session. One session per connection preserves this:

```
Connection A:  TxnBegin → KvPut "x" → KvPut "y" → TxnCommit
Connection B:  KvGet "z"  (independent, own session, no transaction)
```

The host process manages a pool of sessions, one per active connection.
When a connection closes, its session is dropped (rolling back any
uncommitted transaction, matching the existing behavior).

## Host Lifecycle

### Startup (in Database::open)

1. Acquire `.lock` (existing behavior)
2. Create Unix socket at `<data_dir>/strata.sock`
3. Spawn listener thread: `accept()` loop
4. For each connection: spawn handler thread with a new `Session`

### Shutdown

1. Signal listener thread to stop (via `AtomicBool` or channel)
2. Close all active connections (drop sessions, auto-rollback txns)
3. Remove `strata.sock`
4. Release `.lock` (existing behavior via `Drop`)

### Stale Socket Recovery

If a process crashes without cleanup, `strata.sock` may remain on disk.
Detection: attempt `connect()` — if it fails with `ConnectionRefused`,
the socket is stale. Delete it and proceed with normal open.

## Implementation Layers

### Layer 1: Wire Protocol (`crates/executor/src/ipc/`)

- `wire.rs` — Length-prefixed MessagePack read/write
- `protocol.rs` — `Request`, `Response` types

### Layer 2: Socket Server (`crates/executor/src/ipc/`)

- `server.rs` — Listener thread, connection handler threads
- Spawned by `Strata::open()` when write lock is acquired
- Each connection gets a `Session` and a read loop

### Layer 3: Socket Client (`crates/executor/src/ipc/`)

- `client.rs` — `IpcSession` implementing the same execute interface
- Returns a `Strata` handle backed by socket I/O instead of local DB
- SDKs (Node, Python) get this for free via their existing FFI bindings

### Layer 4: Transparent Fallback (`crates/executor/src/api/`)

- `Strata::open()` attempts direct open, falls back to IPC on lock conflict
- No changes needed in CLI, Node SDK, or Python SDK

## What This Does NOT Change

- Database is still embedded — no separate server binary
- Single-writer guarantee is preserved — host serializes all writes
- `--follower` mode still works independently (read-only, no socket)
- No network exposure — Unix socket with filesystem permissions only
- No configuration — fully automatic
- SDK APIs unchanged — `Strata::open()` works the same way

## Limitations

- **Unix-only** — Unix domain sockets don't exist on Windows. Windows
  could use named pipes (`\\.\pipe\strata-<hash>`) as a future extension.
- **Local only** — No remote access. This is intentional. Remote access
  would require auth, TLS, and protocol versioning — that's a server, not
  an embedded database.
- **Single host** — Only one host process per database. Multiple clients
  can connect, but writes are serialized through the host's single writer.
  This matches the embedded model.
- **Host process exit** — When the host exits, all client connections break.
  Clients get an I/O error and must reconnect (which means reopening the
  database — one of them becomes the new host). This is acceptable for
  the expected use case.

## Performance Considerations

- **IPC overhead**: ~50-100us per roundtrip for Unix domain sockets
  (negligible compared to disk I/O for writes)
- **Serialization**: MessagePack is ~10x faster than JSON for serde
- **Connection cost**: One thread per connection on the host. For the
  expected use case (2-3 concurrent SDK sessions), this is fine.
  If needed, can migrate to async (tokio) later.

## Alternatives Considered

**WAL-level concurrent writes (SQLite model)**: Would require shared-memory
coordination, page-level locking, and fundamental changes to the storage
engine. Far more complex for the same user-facing result.

**Advisory locking with retry**: Simple but terrible UX — commands would
randomly fail or hang. Doesn't solve the problem, just hides it.

**Named pipes**: Simpler protocol but unidirectional. Would need two pipes
per connection (request + response), making lifecycle management harder.

**gRPC/HTTP**: Overkill. Adds dependencies (tonic, hyper), requires port
allocation, and signals "this is a server" — the opposite of the intent.
