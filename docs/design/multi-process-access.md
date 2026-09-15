# Multi-process access: broker, or coherence?

**Status:** open question, parked 2026-09-15 pending a dedicated design
session. This document exists to make the question precise and to record what
was measured, so the session starts from evidence rather than from first
principles. **It does not decide.** Tracking issue **#3430**, parked with it.

Every observation was taken on `main` at `4de4ae1c` by running the binary and
reading the source, not by reading the existing design.

---

## 1. The question

Strata is the only embedded database of its peers with a **broker**: one
process owns the engine and every other process sends it requests over a Unix
socket. SQLite, DuckDB, LMDB and RocksDB have none — each makes the file
format and locking protocol the coordination point, so processes never talk to
each other.

The question this document exists to frame:

> **Does Strata keep the broker and make it invisible, or make the storage
> layer multi-process-safe and delete it?**

An earlier draft of this document assumed the first and laid out slices. That
assumption is withdrawn. The second option turns out to be closer than it
looked, and the first has costs that were not written down.

---

## 2. Why the broker exists — precisely

Not because of locking. Because of **one in-memory structure**.

`crates/storage/src/branch/state/read_hooks.rs:294`:

> the published snapshot sees commits appended to the **shared memtable**

A durable commit is written to the WAL, so it is *durable*. But it becomes
*visible* through a memtable in the committing process's own memory. A second
process holding the same directory open would not see it without replaying the
WAL, which is recovery, not a read path.

That is the entire gap, stated as sharply as it can be:

> **Strata has a process-private memtable where SQLite has a shared WAL index.**

SQLite's WAL mode works across processes because its equivalent structure —
the `-shm` index that says what is in the WAL and where — is a shared-memory
file every process maps. LMDB goes further: every process mmaps the same
pages, so there is only ever one copy of anything. Neither needs a broker,
because neither has private state to reconcile.

### 2.1 The coherence problem, decomposed

If the broker were deleted, three kinds of per-process state would need to
become coherent. They are not equally hard:

| state | difficulty | why |
|---|---|---|
| **Memtable** — commits not yet flushed | **hard** | Private by construction. Either it becomes shared memory (SQLite's answer) or readers learn to read the WAL directly. This is the real work. |
| **Block cache** over table objects | easy | LSM table objects are immutable once written. A cache over immutable objects is already coherent; it only needs to observe that the manifest moved. |
| **Manifest, branch catalog, space index** | moderate | Versioned metadata. A generation counter in a small shared file makes "has this changed?" cheap, which is the same trick SQLite's `-shm` uses for the WAL. |

So the honest estimate is not "rewrite storage". It is **one hard problem and
two tractable ones** — and the hard one has a known shape, because two peers
have solved exactly it.

---

## 3. What the broker costs

Measured or read, not assumed.

**Clients are not embedded.** The pitch for an embedded database is that there
is no IPC. A broker makes that true for one process and false for the rest. A
client's read is serialize → socket → deserialize → execute → serialize →
socket → deserialize, where SQLite's second process reads its own mapping. A
socket round trip is tens of microseconds against a read that should be low
single digits.

**One funnel.** `crates/executor/src/ipc/server.rs:4`:

> Every connection's requests serialize through a shared `Mutex<Executor>`

Two clients reading unrelated keys queue behind each other. #2879 (G8) lifts
the mutex; it does not remove the funnel.

**The asymmetry is invisible and non-deterministic.** The same code runs at
two very different speeds depending on which process opened first. Benchmarks
measure whichever role they happened to land in; a test that passes as owner
can fail on timing as a client.

**Lifecycle coupling.** Clients depend on a process they do not control — a
REPL someone walked away from, an editor extension that gets killed. With file
locking, no process's fate is tied to another's.

**A socket inside the data directory.** `strata.sock` sits beside the WAL, so
`cp -r`, `rsync`, `tar` and container copies encounter a socket. "A database
is a directory" acquires a caveat.

**A second permissions mechanism.** File modes govern the data; socket modes
govern who may issue commands, writes included. The two can disagree.

**A protocol is a compatibility surface** that a broker-free design does not
have at all — already at rev 2, with version skew between client and owner to
support.

**Everything must be correct twice.** Every command has an in-process path and
a brokered path; that doubled matrix is what the G1–G7 hardening wave was.

### 3.1 What the broker buys

Stated fairly, because a differential runs both ways. A Strata client gets
full **read/write** concurrently with the owner. DuckDB gives one writer *or*
many readers; RocksDB gives one process. Strata's multi-process *capability*
is ahead of both peers that chose not to solve coherence — it is the
operability that is behind.

---

## 4. What is broken today, regardless of which way this goes

These are defects under either architecture.

**A bare REPL holds the lock and does not host**, though `--ipc <MODE>`
documents `host` as its default:

```console
$ strata <db>                 # hosting false, is_owner true, no socket
$ strata --ipc host <db>      # hosting true, socket created, second process writes through it
```

**Against that non-hosting holder, from outside:**

```console
$ strata <db> ipc status   -> failed_precondition.engine.writer_lock
$ strata <db> stop         -> failed_precondition.engine.writer_lock
```

Ownership cannot be queried, cannot be released, and termination is the only
remedy. The same holds for a library embedder (#3128).

**The lock file is empty.** `locks/writer.object@` is zero bytes: the held
file descriptor carries the advisory exclusion and the file carries nothing,
so the owner cannot be identified without connecting to it — which is exactly
what fails.

**`ipc status` knows and does not say.** The payload carries `owner_pid`,
`socket_path` and a per-client list; all three are in `DELIBERATELY_UNSHOWN`
as "machine specifics of the current host, not facts about the database". That
rule is right for a database record and wrong for the one command whose
subject is the host.

---

## 5. Decisions that hold either way

Four decisions were taken before the question reopened. Three of them are
independent of the answer, and can proceed while it is parked:

1. **The writer lock is released at the end of a transaction**, with a bounded
   wait on contention rather than an immediate failure. Independent — and a
   **prerequisite** for the broker-free design, since coherence is only
   tractable when nobody holds a lock for a process lifetime.
2. **The lock file carries the owner's identity.** Independent, and under a
   broker-free design it becomes the *only* ownership mechanism. This is
   LMDB's reader table, minus the shared memory.
3. **One mode, no opt-out.** Independent in intent: under a broker it means
   every durable database participates; without one it means there is no mode
   because there is nothing to choose.
4. **`ipc status` answers a reader, not only `--json`.** Partly dependent — if
   the broker goes, the command is no longer about IPC at all. It becomes
   "who has this database open", answered from the lock file.

"Every database" cannot be literal either way: cache mode is non-durable and
single-process by construction (`Connection::cache` hosts nothing) and wasm has
no sockets. The rule is *every durable local database*, and the exemption has
to be stated rather than discovered.

---

## 6. What the design session has to settle

**6.1 The memtable.** Shared memory, as SQLite chose, or readers that read the
WAL directly? This is the decision everything else follows from. Worth knowing
before the session: how large a memtable gets in practice, and whether its
structure survives being placed in a mapping.

**6.2 Whether the broker survives as an optimization.** These are not
exclusive. A coherent storage layer with an *optional* broker for
high-fan-out cases is a third answer, and it is worth asking whether that is
the best of both or the worst.

**6.3 What it costs to keep MVCC snapshots across processes.** A reader needs
a consistent view for the length of its read. In-process that is a snapshot
object; across processes it is a registration another process must not
collect under — LMDB's reader table exists for exactly this, and the COW GC
rule (deletion requires reachability across all branches) is the Strata-side
constraint it would have to respect.

**6.4 Windows.** The transport is `UnixStream` with no `cfg` gating (#2871
G10). Mandatory IPC puts it on the critical path; deleting the broker removes
the question entirely. The two answers differ sharply here.

**6.5 The library surface.** Either answer settles #3128, differently: with a
broker, `stratadb` must gain a transport it does not have; without one, it
needs nothing, because opening the directory is the whole mechanism.

**6.6 What can be measured now.** The client-versus-owner latency gap is
measurable today and nobody has measured it. A number would make §3's first
cost concrete instead of argued, and it is the cheapest input the session
could have.

---

## 7. Parked

No slices are proposed. The earlier draft's M1–M6 assumed the broker survives,
and three of those slices (identity in the lock file, `ipc status` for a
reader, the busy wait) are the §5 decisions that hold regardless — they can be
picked up independently and are not blocked by this question.

What is parked is the architecture: the memtable, the broker's fate, and
everything that depends on which way those go.
