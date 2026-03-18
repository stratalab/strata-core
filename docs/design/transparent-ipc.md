# Transparent IPC: Concurrent Process Access via Unix Domain Socket

## Problem

Strata is an embedded database — only one process can hold the write lock at a time.
When `strata ai` is running in one terminal, `strata kv get` in another terminal fails
with "database is already in use by another process."

This is the correct behavior for an embedded database, but it creates a poor user
experience. The user shouldn't have to think about which terminal holds the lock.

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

The user never starts a server. The user never configures ports. The CLI
just figures it out.

```
# Terminal 1 — becomes the host automatically
$ strata ai
> Building knowledge graph...

# Terminal 2 — becomes a client transparently
$ strata kv get greeting
"hello world"

# Terminal 3 — another client, writes forwarded to host
$ strata kv put status "running"
OK
```

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

### Startup (in Database::open or CLI open path)

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

## Client Mode in CLI

In `open_database()` (cli/src/main.rs):

```
fn open_database(matches, path) -> Result<Strata, String> {
    // Try normal open first
    match Strata::open_with(path, opts) {
        Ok(db) => Ok(db),
        Err(e) if e.is_locked() => {
            // Database is held by another process — try IPC
            let sock = Path::new(path).join("strata.sock");
            if sock.exists() {
                // Enter client mode
                Ok(Strata::connect(sock)?)
            } else {
                Err(e)  // No socket — real lock conflict
            }
        }
        Err(e) => Err(e),
    }
}
```

The `Strata::connect()` constructor returns a `Strata` handle backed by
a socket connection instead of a local database. The `Session::execute()`
method serializes the command, sends it over the socket, and deserializes
the response. From the CLI's perspective, it's the same `Strata` API.

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
- `Strata::connect(sock_path)` constructor

### Layer 4: CLI Integration (`crates/cli/src/main.rs`)

- Transparent fallback in `open_database()`
- No changes to command dispatch, REPL, or output formatting

## What This Does NOT Change

- Database is still embedded — no separate server binary
- Single-writer guarantee is preserved — host serializes all writes
- `--follower` mode still works independently (read-only, no socket)
- No network exposure — Unix socket with filesystem permissions only
- No configuration — fully automatic

## Limitations

- **Unix-only** — Unix domain sockets don't exist on Windows. Windows
  could use named pipes (`\\.\pipe\strata-<hash>`) as a future extension.
- **Local only** — No remote access. This is intentional. Remote access
  would require auth, TLS, and protocol versioning — that's a server, not
  an embedded database.
- **Single host** — Only one host process per database. Multiple clients
  can connect, but writes are serialized through the host's single writer.
  This matches the embedded model.

## Performance Considerations

- **IPC overhead**: ~50-100us per roundtrip for Unix domain sockets
  (negligible compared to disk I/O for writes)
- **Serialization**: MessagePack is ~10x faster than JSON for serde
- **Connection cost**: One thread per connection on the host. For the
  expected use case (2-3 concurrent CLI sessions), this is fine.
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
