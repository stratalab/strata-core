# Multi-process access: one mode, and nobody sees a lock

**Status:** proposed 2026-09-15. Tracking issue **#3430** (slices M1-M6). Decisions 1–4 in §2 are made; the four items
in §6 are open and must be answered before slicing. Every observation below
was taken on `main` at `4de4ae1c` by running the binary, not by reading the
design. Follows the shape of `cli-output-contract.md`: name the mechanism,
replace it, leave an executable contract behind.

---

## 1. The principle

> **Multi-process access is the engine's business, not the caller's.** A
> database is a directory; opening it works whether or not another process has
> it open, from any surface, with no mode to choose and no lock to reason
> about.

Today the caller chooses a mode (`--ipc host|client|off`), discovers
contention as an error, cannot find out who holds the database, and in the
common case cannot release it without killing a process. Every one of those is
a consequence of one decision, named in §4.

---

## 2. What is decided

1. **One mode.** `IpcMode::{Host, Client, Off}` collapses. A durable database
   always participates; there is no opt-out and no flag. (Cache and wasm are
   exempt by construction — see §6.1.)
2. **The writer lock is released at the end of a transaction**, not held for
   the life of the process, and a contended write **waits** for a bounded time
   rather than failing immediately.
3. **The lock file carries the owner's identity**, so any process can answer
   "who has this open?" by reading a file.
4. **`ipc status` reports what it knows to a human and an agent**, not only
   under `--json`.

## 3. What is broken today, measured

Run against `main` at `4de4ae1c`:

**A bare REPL holds the lock and does not host**, though `--ipc <MODE>`
documents `host` as the default:

```console
$ strata <db>                 # REPL, default mode
  hosting false, is_owner true, and no socket in the directory
$ strata --ipc host <db>      # explicit
  hosting true, socket created, a second process writes through it
```

**Against that non-hosting holder, nothing works from outside:**

```console
$ strata <db> ipc status   -> failed_precondition.engine.writer_lock
$ strata <db> stop         -> failed_precondition.engine.writer_lock
```

You cannot ask who owns it. You cannot release it. Terminating the holder is
the only remedy. The same is true of a library embedder, which has neither a
host nor a client (#3128).

**The lock file is empty.** `locks/writer.object@` is zero bytes; the held
file descriptor carries the advisory exclusion and the file carries nothing,
so there is no way to learn the owner without connecting to it — which is
precisely what fails.

**`ipc status` knows the answer and does not print it.** The payload carries
`owner_pid`, `socket_path` and a per-client list (pid, name, version,
protocol, access). All three are in `DELIBERATELY_UNSHOWN`, reasoned as
"machine specifics of the current host, not facts about the database". That
rule is right for a database record and wrong for the one command whose
subject *is* the host.

**What does work, and is better than the peers:** a hosting owner brokers full
**read/write** for other processes. A client is not a reader. DuckDB gives one
writer or many readers; RocksDB gives one process. Strata's capability is
ahead of both — its operability is behind.

## 4. The root cause: hosting and locking are fused

A process hosts **because** it holds the lock, and it holds the lock
**because** it opened first. One fact serves two unrelated purposes, and every
symptom in §3 follows:

- ownership cannot be *queried*, because asking requires connecting to the
  thing that holds it;
- ownership cannot be *released*, because releasing it means ending the
  process that holds it;
- ownership cannot be *transferred*, because there is no moment at which no
  one holds it.

**The design move is to separate them.**

| | scope | contended | concern |
|---|---|---|---|
| **Lock** | one transaction | yes, with a bounded wait | storage |
| **Hosting** | a process's session | no — one host at a time, by election | transport |

Once separated, the lock is a short-lived storage detail nobody outside the
engine ever names, and hosting is a role that can start, stop and **migrate**
while no lock is held. "Transfer ownership" becomes expressible for the first
time: hand off the socket at a moment when no transaction is open.

## 5. How the peers do it, and what that argues

| | coordination | concurrency | who owns it? | released |
|---|---|---|---|---|
| SQLite (WAL) | advisory locks on the db + `-shm` | many readers **+** one writer | no query; `SQLITE_BUSY` | per transaction |
| SQLite (rollback) | same | many readers **xor** one writer | no | per transaction |
| DuckDB | file lock | one read-write **or** many read-only | error text only | process exit |
| LMDB | shared-memory lock file | MVCC readers + one writer | **yes** — a reader table of pids, reaped by `mdb_reader_check` | per transaction |
| RocksDB | `LOCK` file | one process (a secondary tails the WAL) | no | process exit |
| **Strata today** | advisory lock **+ a Unix-socket broker** | one owner + brokered read/write clients | only if hosting | process exit |

**No peer has a broker.** Each makes the file format and locking protocol the
coordination point: processes do not talk to each other, they agree on bytes
on disk. Strata has a broker because its engine holds per-process state —
memtables, block cache, branch catalog, space index — that a second process
cannot safely write around. SQLite is multi-process because its format was
designed for it.

So the broker is not gratuitous, and this epic does not delete it. Two
borrowings are worth taking outright:

- **LMDB's reader table** is the precedent for decision 3. Identity in a file
  that anyone can read, with dead entries reaped, needs no connection and no
  protocol.
- **SQLite's `busy_timeout`** is decision 2's second half. Waiting and
  retrying converts the ordinary case — someone else is mid-write — from an
  error into a pause.

## 6. Open, and blocking

**6.1 What "every database" means.** Cache mode is non-durable and
single-process by construction (`Connection::cache`, "hosts nothing"), and
wasm has no sockets at all. The rule has to be *every durable local database*,
and the exemption must be stated rather than discovered. Does a cache database
answer `ipc status` with a defined "not applicable", or is the command absent
there?

**6.2 Windows.** The transport is `UnixStream` with no `cfg` gating. Making
IPC mandatory makes Windows support mandatory with it (#2871 G10). AF_UNIX
exists on Windows 10+, which is probably the answer, but it is now on the
critical path rather than beside it.

**6.3 The library surface forces #3128.** If every durable database
participates, `stratadb` — today a 271-line pure re-export of engine — cannot
stay one. Either it gains an executor dependency, or the transport moves to a
layer both can reach. This epic cannot dodge the question the way #3128 was
allowed to.

**6.4 Owner crash and election.** With clients attached and the host gone,
someone must become the host. Transaction-scoped locks make the boundary
clean — a crash leaves a recoverable WAL and no held lock — but the election
itself (who wins, how the losers learn, what a client does mid-request) is
undesigned. This is the part of the epic with genuine distributed-systems
content and it should not be estimated as plumbing.

**6.5 Cost of hosting on the common path.** If opening read-write always
binds a socket, every one-shot `strata kv get k` pays bind+listen. Lazy
hosting avoids it but needs a way for a second process to ask a non-hosting
owner to start — which is the chicken-and-egg that decision 3's lock file may
solve. Measure before choosing; `perf_floors.py` governs the write path.

## 7. Shape of the work

Sequenced so each slice is useful alone and none is a prerequisite for
deciding the next:

- **M1 — identity in the lock file.** Write `{pid, started_at, socket_path?}`
  into `locks/writer`; `ipc status` reads it without connecting; stale entries
  reaped on open. Fixes "who owns it" for the non-hosting holder outright, and
  needs no protocol change. *Independent of every other slice.*
- **M2 — `ipc status` for a reader.** Show what the payload already carries.
  Removes three `DELIBERATELY_UNSHOWN` entries, and states in the contract why
  this command is the exception to the machine-specifics rule.
- **M3 — busy wait.** A bounded wait with backoff on a contended open/write,
  configurable, defaulting to something small and non-zero. Turns `writer_lock`
  from the common answer into the rare one.
- **M4 — transaction-scoped locking.** The storage project: release between
  commits, which means unflushed state must be recoverable by another process
  rather than owned by this one. The largest slice and the one that makes M5
  possible.
- **M5 — hosting as a migratable role.** Election, handoff, client reconnect.
  Depends on M4 for a well-defined no-lock-held moment.
- **M6 — collapse the mode.** Delete `IpcMode::Off` and the flag; every
  durable open participates. Last, because it is only safe once contention
  never surfaces as an error.

M1–M3 retire the operability complaints and are days, not weeks. M4–M6 are the
architecture, and M4 is where the real cost is.

## 8. What this deletes

- `--ipc <MODE>` and `IpcMode`, three variants and the branching on them.
- The `host (default)` claim that a REPL does not honour.
- `failed_precondition.engine.writer_lock` as a thing a user ever sees —
  it becomes an internal, retried condition.
- The `strata start` / `strata stop` pair as the only way to make a database
  reachable, and the `DELIBERATELY_UNSHOWN` entries hiding the host's identity.
