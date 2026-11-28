# Concurrency inventory — read-modify-write and torn reads

Status: **Phase A1 working document** (the #3776 follow-up program). This records, per
engine surface, every write that reads storage before it commits and every read built
from more than one storage read, as traced against `main` at `fd4768c5`. It feeds the
Phase A2 commit-seam harness (which proves each entry red or green) and the Phase B
design.

## Summary

**26 distinct sites** across kv, json, event, vector (latent) and graph, plus the shared
space-registration path. **No surface is free of the class.** The races reduce to five
patterns, and the design has to answer each one:

| # | Pattern | Example sites | Worst outcome | Live? |
|---|---|---|---|---|
| P1 | **Read-modify-write of a shared aggregate row**: the event log metadata, a graph's metadata counts, the ontology row, a branch's space index, a whole JSON document | 1, 3, 10, 18, 21 | **acknowledged write silently lost** (events #3776, JSON fields #3777, a space dropped from the index #3778); a freeze silently undone | yes |
| P2 | **Precondition check, then commit** (TOCTOU): create-if-absent gates, the edge-endpoint check, the force-delete emptiness check, the `created` / `deleted` flags | 2, 6, 7, 14, 19, 20, 22 | duplicate create succeeds; a **dangling edge**, then a **false `data_loss.engine.graph_index`** in every analytic; a non-force delete destroys data | yes |
| P3 | **Derived rows maintained from a stale old value**: JSON index diffs, graph type-index and binding cleanup, the incident-edge sweep | 4, 5, 19, 20 | stale or dangling index rows; a **false `data_loss`** on a later read | yes |
| P4 | **Multi-commit operations whose enumeration predates later commits**: chunked graph delete, `bulk_insert`, the pending-deletion sweep, the delete mark | 11, 16, 22 | another writer's acknowledged rows tombstoned; **an index listing a space with no catalog row, so reopen fails**; orphans inherited by a re-created graph | yes |
| P5 | **Multi-read readers without one snapshot**: index-then-hydrate, existence-then-read, paged scans, per-key batch reads, metadata-then-scan | 8, 9, 12, 13, 15, 23, 24, 25, 26 | **false `data_loss` on a healthy database** (`neighbors`, `nodes_by_type`, `verify_chain`); torn pages; `None` instead of `not_found` | yes |

**What this says about the design.**
- P1–P3 are what a per-operation **read set validated at commit** fixes: every row and
  range the operation read must be unchanged when it commits.
- P5 is what **one snapshot per operation** fixes.
- P4 can't be fixed by a single commit's validation. An operation spanning several
  commits has to be exclusive on its target for its duration, or fence other writers
  with a marker they condition on.
- **Liveness:** #3780's measurement showed that validation without serialization starves
  a writer under contention.

Only one entry point makes any of this live: a direct Rust caller sharing `&Database`
across threads, which the `stratadb` facade advertises.

## Why these races are reachable

`Database::{kv, json, event, vector, graph}` borrow `&self` and `Database` is
`Send + Sync` (#3126), so threads sharing one handle write the same branch at once. An
engine method often makes several storage calls (point reads, prefix scans, then a
commit). Until #3780, `StoragePersistence::commit` sent `CommitOptions::default()` with
only an expected branch generation: no read set, no condition. `Database::{spaces,
branches, admin}` take `&mut self`, and the executor and IPC serialize every request, so
the races below reach only embedded callers sharing a `&Database`.

Notation: `L` = `ReadSelector::Latest`. Each storage call loads its own snapshot unless
marked **[1 snap]** (`read_rows` / `read_points` / `scan_snapshot`).

## KV + JSON (52 public methods traced)

### Read-modify-write sites

1. **`space::registration_mutations`** (`control/space.rs:51-79`), reached by every kv,
   json, event, vector and graph commit through `commit_batch` (kv `:706`, json `:1242`).
   The index row is read at `:56` and rewritten whole at `:70-73`. Concurrent first
   writes to two new spaces drop one of them from the index while its catalog row
   exists. **Confirmed: #3778** (100 new spaces, 25 listed).
   - Sub-site, traced only: `finish_pending_deletion` (`:64` → `:370-380`). Two first
     writers into a space with a pending deletion each see the mark. B's tombstone scan
     can tombstone A's acknowledged row and A's new catalog row. Until B commits, the
     index lists the space with no catalog row, so other writers get
     `data_loss.engine.space_catalog` (`:571`).
2. **KV existence gate** (`kv/service.rs:76-80`, `:503-510`). The `created` / `deleted`
   flags come out wrong. `delete_batch` can also **lose a delete**: a key read as absent
   gets no tombstone, so a concurrent `put` that commits before the batch survives it.
3. **JSON whole-document RMW.** `apply_set` (`json/service.rs:864-891`, used by `set`,
   `set_or_create` and `batch_set_or_create`), path `delete` (`:385-413`) and
   `batch_delete_entries` (`:563-599`) each rewrite the whole document from their read.
   Results:
   - lost sibling-path updates, **confirmed: #3777** (100 sets kept about 25 fields);
   - **resurrection** of a concurrently deleted document;
   - colliding `document_version`.
4. **JSON index diff computed from a stale old value.** `index_mutations_for_change`
   (`:1110-1141`) is fed by the document reads at `:873`, `:385`, `:515` and `:563`, so
   `create`, every `set*` and every `delete*` / `batch_delete*` can leave stale or
   missing entries. Impact is latent today: the only index readers are
   `json_index_entry_count_for_test` and space usage/delete. But bad entries persist,
   and a re-created index with the same name inherits them.
5. **Index definitions read on their own snapshot.** `load_indexes` (`:1075-1108`) runs
   against `create_index` / `drop_index`, so a write can miss a just-created index or
   write into a just-dropped one.
6. **`create_index` / `drop_index`** (`:738-824`).
   - The existence gate on the meta row allows duplicate creates, even with different
     field paths.
   - The backfill scan's snapshot is separate from the gate and the commit, so it can
     record f=1 → X where the document holds f=2, and miss f=2 → X.
   - A drop can leave orphan entries, which a re-created index inherits.
7. **JSON `create` existence gate** (`:106-115`). A duplicate create succeeds and leaves
   stale index entries.

### Torn readers

8. **`batch_exists`**: KV `:260-272`, JSON `:360-372`. It reads each key with its own
   `read_row`, where `batch_get` uses `read_rows`. One commit can produce `[false, true]`.
   Tracked as part of #3757.
9. **Paged or limited scans under `Latest`.** KV `scan_keys_after_cursor` (`:575-631`)
   and `scan_range_limited` (`:633-668`); JSON `scan_ids_after_cursor` (`:913-972`) and
   `scan_range_limited` (`:974-1009`). A page can show y without b. Tracked as #3759 for
   `json.scan`. `list_at` / `list_at_page` are safe at T ≤ head; T ahead of head is
   unverified.

### Safe

Single point reads, `read_rows`-backed `batch_get*`, single unbounded scans, `count*`,
`sample`, and `get_versions` (history, then `committed_at` keyed by immutable versions).

## Event (live: `Database::event` is `&self`)

10. **Event log metadata RMW**: `batch_append` (`event/service.rs:106` read, `:140-143`
    metadata put, `:144` commit). Two appends land at one sequence and one acknowledged
    event is overwritten. The chain then breaks permanently (`verify_chain` is invalid
    forever), `len()` disagrees with `list()`, and a phantom type-index row is left.
    **Confirmed: #3776**, fixed by #3780 (commit condition plus per-log turn).
    `replay_append` (`:173`) has the same shape and is exclusive today (artifact import
    holds `&mut`); #3780 conditions it too.
11. **Pending-deletion sweep inside registration**: `control/space.rs:64` → pending read
    `:375`/`:420` → sweep scans `:253-271` → sweep commits `:344-364`, each on its own
    snapshot and committed before the caller's commit. T1 appends to space s, finishes
    the sweep, registers s and commits. T2 (a KV put to s, say) read "pending" before
    T1's sweep committed, but runs its sweep scan after T1's commit. **T2's sweep
    tombstones T1's acknowledged rows.** It needs an interrupted space delete to set up.
    Live for every `&self` service.

Torn readers:
12. **`verify_chain`** (`:448` metadata, then `:450` scan). An append committing between
    the two reads makes it report "event log length does not match metadata" on a healthy
    log: **false corruption**.
13. **`list_types`** runs `OrderedTextScan::distinct_values` (`persistence/scan_ordered.rs:239`),
    one `scan_range` per probe. It can return {m, z} when no snapshot ever had z without a.

Safe: single reads, `range` (rows below `next_sequence` commit with the metadata and
nothing `&self` rewrites them), `range_by_time`, `list` / `list_page`, `len*`,
`get_by_type*`.

## Vector (latent: `Database::vector` is `&mut self`, `api/database.rs:377-393`)

Every race here goes live the day `vector()` becomes `&self`; the method's doc already
names that as follow-up work. `VectorArtifactStore` is plain maps reached only through
the exclusive borrow, and each cache hit is checked against the manifest ref read by the
same query.

14. Config and row read-modify-writes:
    - `create_collection` existence gate (`:152`): two creates with different dimensions
      both succeed.
    - `delete_collection` (`:185` config, `:192` row scan): orphans a concurrent upsert,
      and gets past `force=false`.
    - `declare_embedding_model` (`:336`): two models both recorded as Ok, which breaks
      rule 24.
    - `upsert` / `batch_upsert` revisions (`:3007-3046`): both writers acknowledge the
      same revision.
    - `update_metadata` (`:724`) / `update_embedding` (`:787`): a lost update, and
      resurrection after a delete.
    - `delete_matching` (`:1783`): deletes a row a concurrent update moved out of the
      filter.
    - `batch_delete` (`:956`): double `deleted` flags.
15. Torn readers:
    - `batch_get` (`:927`) and `batch_exists` (`:604`), one read per key; #3757 tracks
      them.
    - paged scans (`:2919`, `:2967`);
    - config-then-rows pairs (`:2788`, `:421-422`, `:484-486`);
    - `query` under `Latest` (`:1834`, `:2086`, `:2850`, `:2864`), which scores rows from
      another collection incarnation against a stale config.

## Graph (live: `Database::graph` is `&self`)

Every graph writer is a read-modify-write. Line numbers are `data/graph/service.rs`
unless noted.

18. **Metadata-row RMW** (`metadata_mutation_marking` 2912-2952; `with_state` hard-sets
    `deleting:false`, `record.rs:62-75`). Callers are 576, 766, 848, 1003, 1205, 1249,
    1397 and 2064. Effects:
    - lost count updates: **#3779**, red tests on local branch
      `wip/issue-3779-graph-race-tests`;
    - a double decrement, followed later by a **false `data_loss.engine.graph_metadata`**
      underflow (`record.rs:154-171`);
    - a deleted graph **resurrected** by a writer that read before the delete;
    - a chunked-delete mark **cleared mid-sweep**.
19. **`created` and old-record detection from unfenced point reads** (`node_record`
    2968-2975, `edge_record` 3005-3016, the batch overlay 3148-3197). Effects:
    - double counts;
    - leftover type-index and binding rows, so `nodes_by_type(A)` returns a type-B node
      and `bindings_for_entity` returns a stale node;
    - `upsert_node` racing `delete_node` leaves a dangling type-index row, so
      `nodes_by_type` raises a **false `data_loss.engine.graph_index`**.
20. **The endpoint check races the incident-edge sweep.** Endpoint checks are in
    `upsert_edge` 983-988, `validate_bulk_input` 1300-1318 (once for a whole multi-chunk
    import) and `overlay_endpoints` 1982-1983. Sweeps are `stored_incident_edges`
    3082-3115, from 842, 559 and 1966. Result: **dangling edges**, so `neighbors`,
    `adjacency_index` and every analytic raise **false `data_loss.engine.graph_index`**
    (`adjacency.rs:283-290`, 3207-3211). `apply_binding_delete_policy` cascade has the
    same shape, and its detach path silently loses a concurrent node update.
21. **Ontology-row RMW** (`mutable_ontology` 2592-2608 + `write_ontology` 2610-2624, from
    2128, 2149, 2171, 2187 and 2206). Effects:
    - a lost definition;
    - **freeze silently undone**: `freeze` racing `define_object_type` leaves Draft, and
      `delete_object_type` racing `freeze` un-freezes and leaves a link to an undeclared
      type;
    - an orphan ontology row, so a re-created graph is born frozen.

    A mid-import freeze also lets later `bulk_insert` chunks commit unvalidated rows.
22. **`delete_graph` row enumeration** (`graph_row_tombstones` 191-240: 6–7 independent
    scans, taken before the mark at 153 vs 167; the force check 138-151 is a separate
    read). Effects:
    - a **non-force delete destroys data** (the #3122 guard is bypassed);
    - orphan rows, which a re-created graph inherits with counts 0;
    - `create_graph` reports success for a graph a concurrent delete then removes.

    `create_graph` (76) existence gate: two creates both succeed, and a racing upsert's
    count is reset to 0, setting up the underflow in item 18.
23. **Existence check outside the read (#3774).** `require_graph_with_selector` 2756-2776
    runs before every reader's read (813, 863, 941, 1376, 1479, 1736, 2103, 2284, 2368,
    2495). Effects: `Ok(None)` or empty results instead of `not_found`, and a half-swept
    view of a chunked delete.
24. **Index-then-hydrate with corruption on a miss.** `neighbors` 1652 → 3199-3213 and
    `nodes_by_type` 2319-2326 raise a **false `data_loss.engine.graph_index`** under
    `Latest` when a delete commits between the index read and the hydrate.
    `OrderedTextScan` issues one `scan_range` per bucket probe or refill
    (`scan_ordered.rs:153, 239-247, 313`), tearing pages in `list_nodes`,
    `nodes_by_type` and `neighbors`.
25. **Multi-scan aggregates (#3774):** `scan_graph_state` 2846-2880 (legacy `graph_info`
    and its backfill) and `ontology_summary` 2369-2407.
26. **`bindings_for_entity`** 1811-1842: index-only, with a separate per-graph meta read,
    so it can return a binding from a deleted and re-created graph.

Correction to the raw audit: "a graph commit racing `space delete`" is unreachable
(`spaces()` is `&mut`).

Safe: `list_graphs*` (one scan), `resolve_binding_target*`, `marked_graphs`, the
analytics over a built index, and `adjacency_index`'s node/edge consistency (fixed in
#3758; only its existence check is open).

## Control plane, branches, spaces, admin

### Concurrency entry points

- **The only in-process entry point:** a direct Rust caller sharing `&Database` across
  threads. `Database` is Send + Sync (asserted in `engine/tests/concurrent_writers.rs:26-30`,
  which already exercises different branches at `:39` and the same branch at `:105`).
  `kv` / `json` / `event` / `graph` take `&self`, and the `stratadb` facade advertises
  this (`stratadb/src/lib.rs:83-96`).
- **Not entry points:**
  - the executor, which owns `Database` with `execute(&mut self)`;
  - IPC, MCP and `Connection`, which all go through `Mutex<Executor>`;
  - gpu-cache (`Arc<Mutex<Database>>`), wasm (a single Executor) and hub (sequential);
  - a second process: the writer lock refuses a second opener;
  - engine background threads: there are none.

  Storage background threads (maintenance, subcompaction, the group-commit leader) do
  physical work only and never run an engine read-then-commit.
- **`&mut` and therefore safe today:** spaces, branches (create, fork, delete, promote,
  compare), admin, vector, import / export, close and `set_remote_origin`. `ControlPlane`
  has no interior mutability, so health state and branch lookups in `&self` services
  cannot race. #3761 was closed as latent on this ground.

### Read-modify-write sites

16. **R2: the pending-deletion sweep** (`control/space.rs:370-380` with `:344-365`, reached
    from site 1 at `:64`). The tombstone list comes from a `Latest` scan, then is
    committed later as blind deletes over several commits, ending with a blind delete of
    the catalog row.
    - **Race A (an acknowledged write is lost):** T2 reads the pending mark and scans
      [a]. T1 then does the whole path: sweep, register, `put a=new`, acknowledged. T2's
      sweep commits `del a`.
    - **Race B (lasting catalog corruption):** as race A, but T2's last commit fails
      after its sweep. Causes include a batch over 4096 mutations, an fsync halt, or a
      crash. The index then lists S with no catalog row, so every later write to S fails
      `data_loss.engine.space_catalog` (`:571`) and **reopen fails** (`bootstrap.rs:419`).
    - **Race C:** T1 read the index before its sweep, so the index-drop window of site 1
      widens from microseconds to the whole sweep.
    - The same shape exists in graph scope: `graph/service.rs:153-175` and `:244-280`,
      the marked graph metadata row.
17. **R3, latent (correct only because their callers are exclusive):**
    - the pending index written as a one-name list (`bootstrap.rs:170-174`);
    - the branch index rebuilt from the in-memory map (`:215-231`);
    - `seed_required_space_rows` (`space.rs:468-499`);
    - promote's target-index reconciliation (`space.rs:88-137`, `promote.rs:236`);
    - the ambient replay timestamps (`adapter.rs:871-895`).

    Relaxing any of these entry points to `&self` turns it into a site-1 style lost
    update.

### Consequences of site 1 (the space dropped from the index)

`spaces list` / `exists` miss the space. `space delete` returns `deleted=false` and
leaves the data. `export_branch` omits it, and compare, preview and promote skip it.
Open-time validation walks index entries only, so nothing detects it. It heals on the
next write to that space.

## Phase A2 results: the conformance harness

`crates/engine/src/concurrency_conformance/` runs every ordered pair of each catalog's
operations. The first operation is held before each of its persistence calls in turn
(every point read, scan and commit) while the second runs. The answers and the final
state must equal one of the two serial orders. Readers are catalogued too, so they are
held between their reads while a writer commits. The run is deterministic: the same red
set every time, in about 17 s, cache mode.

| Family | Operations | Ordered pairs | Not serializable | Issue |
|---|---|---|---|---|
| kv (writers + `batch_exists`) | 8 | 64 | 23 writers, 1 reader | #3781, #3757 |
| json (writers + `batch_exists`) | 12 | 144 | 62 | #3777 |
| event (writers + `verify_chain`, `types`) | 5 | 25 | 3 readers (writers green with #3780) | #3783 |
| graph (writers + 6 readers, two graphs) | 26 | 676 | 132 writers, 8 readers (+1 by contract) | #3779, #3774 |
| space registration (cross-capability) | 5 | 25 | 21 | #3778 |
| **total** | | **934** | **250 gaps + 1 by contract** | umbrella **#3782** |

Outcomes the harness reproduces, each an acknowledged-success history that no serial
order explains:

- **An acknowledged write destroyed**:
  - JSON sibling-path sets lost;
  - a space dropped from the index;
  - a non-force `graph.delete(e)` removing a node a concurrent `node.add` reported as
    created (#3122's guard bypassed);
  - events, before #3780: the harness turns all 9 event pairs red if #3780's condition
    and turn are planted out.
- **False corruption on a healthy database**:
  - a dangling edge, then `adjacency_index` → `data_loss.engine.graph_index`;
  - `verify_chain` reporting a healthy log invalid;
  - graph readers (`neighbors`, `nodes_by_type`) raising `data_loss` across a delete.
- **Derived state wrong**:
  - stale and orphan JSON index entries (`delete` vs `set`, `index.drop` vs `set`);
  - graph counts drifting (`node.add` twice gives `created=true` twice and counts +2).
- **Wrong answers**: KV `created` / `deleted` flags, double creates of JSON documents and
  indexes, and a `freeze` reported while a definition lands after it.

### Not yet exercised (harness coverage gaps)

- **Vector** is excluded: `Database::vector` is `&mut`. Its inventory races are latent.
- **Multi-page readers** (paged scans that refill, `list_types` with adversarial type
  order) need catalogue entries shaped to force a refill.
- **Durable mode:** the harness runs cache mode only. Durable parity is a follow-up.
- **The pending-deletion sweep races** (item 16) need an interrupted space delete as the
  seed. Setting that up needs `&mut` space delete, so the seed would arm a commit fault
  mid-delete.
