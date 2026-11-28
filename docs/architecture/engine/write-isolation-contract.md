# Write isolation for concurrent `&self` writers — design

Status: **Accepted, 2026-11-27** (Phase B of the #3782 program; decisions in §6). Revision 1 proposed engine optimistic transactions and was withdrawn as out of line with Strata's settled single-writer model (§3).

Evidence:
- [`docs/audit/concurrency-inventory.md`](../../audit/concurrency-inventory.md): 26
  sites, 5 patterns.
- The conformance harness (`crates/engine/src/concurrency_conformance/`): 934 ordered
  pairs, **250 not serializable**.

## 1. The contract we owe

`Database` is `Send + Sync`, and `kv` / `json` / `event` / `graph` borrow `&self`
(#3126). The `stratadb` facade invites sharing one handle across threads. So the engine
owes this:

> **Any two engine operations that run at once on one `Database` leave the state, and
> return the answers, of one of their two serial orders.** A read operation answers
> from one committed state.

That is per-operation serializability. It is exactly what the harness checks, and a
design is done when the harness ledger (`ledger.rs`) is empty. It is not cross-operation
or cross-capability transactions. Those stay out of scope (#3127): there is no public
transaction API (CLAUDE.md, Out of V1 scope).

## 2. Why it fails today

An engine operation is several persistence calls: point reads, prefix scans, then one
or more commits. Each read loads its own snapshot. Each commit carries only an expected
branch generation, never what was read. The five patterns (inventory, Summary):

| | Pattern | Fixed by |
|---|---|---|
| P1 | Read-modify-write of a shared aggregate row (event log metadata, graph metadata, ontology row, space index, JSON document) | the commit is **conditioned** on the row as read |
| P2 | A precondition check, then a commit (create gates, edge endpoints, the force-delete emptiness check, `created` flags) | the condition must also cover the **rows the check read** |
| P3 | Derived rows maintained from a stale old value (indexes, type-index and binding rows, incident edges) | the same condition: the old value is a read |
| P4 | Multi-commit operations enumerated on an old snapshot (chunked delete, `bulk_insert`, the pending-deletion sweep) | the operation is **exclusive on its target for its duration** |
| P5 | Multi-read readers with no single snapshot | **one snapshot per read operation** |

Storage already has what P1–P3 need: per-key commit conditions (CAS facts, validated
under the branch commit guard, ACID-003) and point read facts. It has **no range
validation**, so a scan cannot be conditioned directly. P4 and every scan need another
mechanism.

## 3. What is already decided, and what it implies

The design is not open. Strata has already settled its concurrency model, in four
places, and every one says the same thing: **reads are concurrent, writes are
exclusive.**

| Source | What it settled |
|---|---|
| Multi-process RFC + IPC epic (#2840–#2846) | "One engine process per store, ever; multi-user is purely a transport problem at the executor." Every IPC, CLI, MCP and SDK request runs one at a time through `Mutex<Executor>`. |
| IPC evolution, G8 (#2879, `ipc-evolution-design.md` §7.1) | The planned relaxation of that mutex is "reads execute concurrently, writes exclusively", relying on "MVCC already provides snapshot semantics". |
| Positioning (embedded, never client-server) | The SQLite model: one writer at a time, readers that do not block it. |
| Handle model (#3126, `database-handle-model.md`) | `&self` services were for "ergonomics first, throughput second": several services held at once, "readers not queueing behind writers", "twenty **branches** writable from twenty threads". It never set a goal of two writers on one branch. |

The code states the same discipline (`GraphService::metadata_mutation`): its safety "is
the branch's single-writer discipline (one `&mut` service per handle, one writer per
branch across processes)".

**So the bug is narrow.** #3126 removed the `&mut` exclusivity that enforced one writer
per branch, and put nothing in its place inside the engine. Every writer gap in the
ledger (patterns P1–P4) is two writers on one branch, and every reader gap (P5) is a
reader with no single snapshot. The fix restores the decided model inside the engine. It
does not replace that model with per-row concurrency control.

## 4. Design: one writer per branch, snapshot readers

1. **Writes are exclusive per branch.**
   - Every engine write operation holds its branch's **writer guard** from its first read
     to its last commit. That includes the multi-commit operations: chunked delete,
     `bulk_insert`, the pending-deletion sweep.
   - Writers on different branches run in parallel: branch-per-agent, and BS5.4's
     parallel per-branch group apply.
   - The guard is per-database state (a branch-keyed lock in `StoragePersistence`). It
     is in-process only, which is enough because the writer lock guarantees one engine
     process per store.
2. **Reads are concurrent and answer from one snapshot.**
   - A read operation takes no guard and runs against one loaded view: `#3758`'s scan
     snapshot, generalized with point reads.
   - A reader never waits for a writer, and never sees half of one.
3. **The type system enforces it.**
   - `StoragePersistence::commit` requires a `&BranchWriter`, obtainable only from
     `persistence.begin_write(branch)`.
   - A service cannot commit without holding its branch, so the next primitive cannot
     reintroduce the class.
   - Multi-read readers go through `persistence.read_view(branch)`.

That closes every pattern:

| | Pattern | Why it closes |
|---|---|---|
| P1–P3 | read-modify-write, check-then-commit, derived rows | nothing else commits to the branch between the write's reads and its commit |
| P4 | multi-commit operations | the guard spans all their commits |
| P5 | torn readers | one snapshot per read operation |

The aggregate roots, commit conditions, retry loops and per-row turns of the first draft
are all unnecessary. With one writer per branch there is no race left for them to settle.
#3780's per-log turn is the same idea at a finer grain, and the branch guard subsumes it.

### What it costs, measured

Same-branch writers, 8 threads against one thread doing the same work (release build,
distinct KV keys):

| Mode | Today, 8 threads vs 1 | Under the branch guard |
|---|---|---|
| cache | 0.73–0.80× (contention makes it slower) | ~1.0× |
| durable, `Standard` | 0.97–1.04× | ~1.0× |
| durable, `Always` (NVMe) | 2.0–3.3× (writers share the covering fsync) | 1.0× |

- The one real cost is same-branch writers under `Always` durability. They no longer
  share an fsync. That is exactly the workload the decided model does not target: many
  concurrent writers on one branch.
- Cross-branch writers keep BS5's group commit and BS5.4's parallel apply.
- IPC clients are unaffected. They never had write concurrency (`Mutex<Executor>`), and
  G8 keeps writes exclusive.

### Alternatives considered

- **Engine optimistic transactions over aggregate roots** (the first draft):
  - commits conditioned on every row read;
  - per-aggregate turns;
  - a scan named against the root row that covers it, which every write to the
    aggregate must rewrite.

  It is complete, and it keeps same-branch `Always` group commit. But it builds
  multi-writer concurrency control for a database whose model is single-writer. It means
  more mechanism, more invariants (the root rewrite), and retry semantics to document.
  **Rejected:** it solves a problem Strata decided not to have.
- **One writer per database (SQLite exactly):** simpler, but it serializes branch-per-agent
  writers, and loses BS5.4 and the handle model's "twenty branches" goal.
  **Rejected.**
- **Write-set-only snapshot isolation:** misses check-then-commit races (write skew).
  **Rejected.**
- **Revert to `&mut` services:** readers would queue behind writers again, undoing the
  handle model. **Rejected.**

## 5. Phase C plan

| Slice | Content | Ledger effect |
|---|---|---|
| C1 | The branch writer guard + `begin_write` / `BranchWriter`. `commit` takes `&BranchWriter`, and every write method acquires its branch once at its public boundary and passes the guard down (no re-entry). The compiler finds every site. Events fold #3780 in: the guard replaces its per-log turn, and the condition and retry are dropped. The harness learns to detect a writer queued on the guard. ACID-008 is rewritten as "a write holds its branch's writer from its first read to its last commit", since the mechanism it described is gone. | **−238** (every writer gap) |
| C2 | Read views (`read_view(branch)`: one snapshot, point reads and scans), and multi-read readers moved onto them: kv / json `batch_exists`, paged scans, event `verify_chain` / `list_types`, graph readers | **−12** |
| C3 | Extend ACID-008 with the read rule: "a read answers from one snapshot". Harness in durable mode. Randomized linearizability checker (A3) as the nightly net. Close #3776–#3783 and #3774. | ledger **empty** |

C1 is one mechanical, compiler-driven change: `commit` gains a parameter, and every
write site acquires the guard. It replaces the first draft's eight family-by-family PRs.
If its diff exceeds the 1,500-line guideline, split it by capability behind a transitional
`commit_unguarded`, to be deleted at the end of C1.

## 6. Decisions (2026-11-27)

1. **One writer per branch**, not per database. Branch-per-agent writers stay parallel.
2. **#3780 merged as the interim fix** (`2fce620b`). C1 replaces its per-log turn with the
   branch guard and drops its condition and retry.
3. **The executor keeps no lock of its own.** It is a thin layer over the engine. When G8
   (#2879) lifts `Mutex<Executor>`, reads run concurrently on the engine's snapshot views
   and writes rely on the engine's branch writer guard. No executor-wide write lock.
4. **C1–C3 ride 1.2.8.**
