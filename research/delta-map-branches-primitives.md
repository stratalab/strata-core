# Delta Map: Branches And Secondary Primitives

This is the second filled delta map against the target architecture.

Scope:

- branch identity and ancestry
- branch merge semantics
- branch DAG and status overlays
- primitive lifecycle ownership
- derived-state policy
- vector/search/graph cleanup

This map assumes the runtime/durability delta in:

- [delta-map-runtime-durability.md](/home/anibjoshi/Documents/GitHub/strata-core/research/delta-map-runtime-durability.md)

and the target architecture in:

- [target-architecture-program.md](/home/anibjoshi/Documents/GitHub/strata-core/research/target-architecture-program.md)

## Target Architecture Reference

The relevant target assumptions are:

- keep the storage-layer branching model
- one branch identity model
- one ancestry model owned by the engine
- DAG/status are projections, not source of truth
- primary primitive correctness comes from storage + engine transaction semantics
- derived subsystems are explicitly classified and lifecycle-owned
- no primitive owns multiple persistence stories

## Gap Table

| Domain | Current authoritative path | Shadow / competing path | Target truth | Gap type | Severity | Disposition | Acceptance test |
|---|---|---|---|---|---|---|---|
| Branch identity | storage and commit locking use deterministic branch IDs from names | branch metadata stores random UUID strings; DAG keys by branch name | one canonical engine-owned branch identity, with projections referencing it rather than redefining it | semantic_inconsistency | critical | rewrite_boundary | branch metadata, DAG projections, and runtime APIs all refer to the same canonical branch identifier and name reuse creates a new generation cleanly |
| Branch ancestry / merge base | storage-level fork metadata plus optional executor-supplied DAG override | repeated merges and materialized branches depend on best-effort DAG history | one engine-owned ancestry store and merge-base algorithm | authority_split | critical | rewrite_boundary | merge-base correctness does not depend on executor override or optional graph hook registration |
| Branch DAG authority | engine branch ops succeed without DAG; DAG writes are best-effort hooks | executor relies on DAG history selectively for merge-base improvement | DAG is either a non-authoritative projection with no semantic dependency, or engine-owned metadata with required consistency | derived_state_confusion | high | consolidate | disabling DAG projection does not change branch correctness, only observability; if enabled, projection is rebuildable from authoritative engine metadata |
| Branch status model | engine only really enforces active/deleting behavior | `core`, `graph`, and `engine` expose incompatible status vocabularies; cache is dormant | one branch lifecycle model, with projection-only statuses clearly separated from enforced statuses | semantic_inconsistency | high | consolidate | branch list/filter/write behavior uses one status contract and tests cover archived/merged/deleted semantics if they are supported |
| Branch delete/recreate semantics | delete removes metadata and storage state | DAG name-keyed lineage and stale delete tracking can contaminate recreated names | branch deletion and recreation are generation-safe | lifecycle_leak | critical | rewrite_boundary | delete + recreate same name behaves as a new branch generation with no inherited stale status or ancestry |
| Branch mutation consistency | fork, merge, cherry-pick, revert, diff use different snapshot and validation discipline | some mutation paths are target-protected while others plan against live state or use weaker blind-write behavior | all branch mutations use one explicit snapshot and target-validation contract | semantic_inconsistency | high | consolidate | merge, cherry-pick, revert, and diff all operate on captured snapshots and document the same OCC guarantees |
| LastWriterWins semantics | current merge policy is effectively source-wins on conflict | name suggests timestamp-based newest-write resolution | merge policy names match actual semantics | control_plane_drift | medium | document_or_rename | tests and API/docs use truthful merge-policy names and no longer imply timestamp-based resolution if that is not what the engine does |
| Three-way diff quality | public `diff_three_way()` is weaker than merge planner snapshot discipline | merge path is stricter than inspection path | inspection APIs either match merge snapshot semantics or are explicitly documented as weaker live views | semantic_inconsistency | medium | consolidate | diff inspection results are stable under concurrent branch activity or are clearly labeled as live/non-transactional |
| Primitive contract | JSON/Event are engine-native; Graph/Search/Vector use overlays, hooks, caches, and globals | there is no single primitive lifecycle contract above storage | each primitive declares source of truth, derived state, recover/refresh/freeze/merge ownership | authority_split | high | rewrite_boundary | every primitive is classified in one ownership table and no lifecycle behavior exists outside its declared contract |
| Derived-state classification | search indexes, branch DAG, status cache, vector mmap state are partially treated as important runtime state | some overlays look authoritative because they persist or influence behavior | derived state is either rebuildable projection or explicitly promoted with durability contract | derived_state_confusion | high | consolidate | each persisted overlay has an explicit class: primary truth, persisted derived, or ephemeral cache |
| Search architecture | search is a shared derived subsystem with subsystem-driven recovery and engine-hardcoded refresh | cache open bypasses subsystem path; refresh logic is not expressed through the same contract as startup/freeze | search is explicit shared derived infrastructure with one lifecycle path | authority_split | medium | consolidate | normal open, follower refresh, cache mode, and freeze all go through one declared search lifecycle contract |
| Vector durable truth | vector truth is KV-backed and rebuildable from storage | dormant vector WAL and vector snapshot stacks still exist as alternate persistence stories | one vector durability story: KV truth plus optional caches | zombie_stack | high | delete | vector WAL/snapshot stacks are removed or fully demoted from the product surface |
| Vector lifecycle ownership | vector uses subsystem recovery, refresh hooks, extension state, merge callbacks, and reload/freeze side channels | no single vector runtime contract exists | vector runtime behavior is declared once: recover, refresh, merge hooks if any, freeze, cache rebuild | authority_split | high | rewrite_boundary | vector behavior no longer depends on mixed hook types and process-global registration to exist correctly on a live database |
| Vector merge semantics | engine branch merge delegates precheck/post-commit behavior to vector-side callbacks | semantic behavior is partly engine-owned and partly global registration | if vector semantic merge exists, it is installed through the instance-local primitive contract, not process-global singleton registration | authority_split | high | consolidate | vector merge behavior is deterministic per database instance and testable without process-global setup order |
| Vector cache role | `.vec` and `.hgr` files are documented as caches but participate heavily in runtime performance and restart path | frozen heaps are persisted through refresh-hook freeze path rather than explicit vector cache contract | vector caches are explicitly rebuildable accelerators with their own lifecycle, not hidden primary state | derived_state_confusion | medium | consolidate | deleting cache files never changes correctness, only warmup/perf; tests verify full rebuild from KV truth |
| Graph data vs graph overlay | ordinary graph rows are storage-native | branch DAG/status and graph semantic merge live as optional graph-side overlays | separate graph data primitive from branch-control-plane projections | derived_state_confusion | high | rewrite_boundary | graph storage APIs and branch-DAG/status projections have separate documented contracts and code ownership |
| Graph merge ownership | graph semantic merge depends on graph-side registration | fallback engine behavior is tactical refusal when callbacks are absent | graph merge semantics are either engine-owned or explicitly optional and non-authoritative | authority_split | medium | consolidate | graph merge behavior is consistent across correctly opened instances without requiring process-global registration order |
| Event branch semantics | event stream is engine-native and append-only | branch merge only performs divergence refusal; no semantic rebase/append merge exists | explicit product decision: strict refusal as a hard invariant, or first-class event rebase/merge workflow | semantic_inconsistency | medium | document_or_promote | branch merge behavior for events is explicit in API/docs/tests and no broader "Git-like merge" claim contradicts it |
| JSON public semantics | internal JSON merge is path-aware and relatively rich | public transactional JSON behavior is still mostly document-level read/modify/write | public JSON contract is aligned with internal merge/concurrency truth | semantic_inconsistency | low | document_or_incremental_improve | API/docs/tests reflect the real granularity of JSON conflict detection and merge behavior |
| Primitive-specific global state | vector merge handlers, graph merge planners, and DAG hooks are process-global | primitive behavior can change based on registration order and first use | primitive semantics are instance-local and declared at open/runtime composition time | authority_split | critical | rewrite_boundary | running two databases in one process with different primitive composition no longer shares semantic global state |

## Priority Clusters

## Cluster A: Branch identity and ancestry

This cluster fixes:

- branch identity fragmentation
- merge-base externalization
- DAG authority confusion
- delete/recreate contamination
- inconsistent status models

This is the most important branch cluster.

### Concrete target

Build one engine-owned branch metadata model containing:

- canonical branch identifier
- branch name
- parent / fork ancestry references
- generation-safe lineage identity
- enforced lifecycle state

Then treat DAG/status as projections over that source of truth.

## Cluster B: Branch mutation contract

This cluster fixes:

- merge/diff/revert/cherry-pick inconsistency
- misleading merge-policy naming
- inspection-vs-apply snapshot mismatch

### Concrete target

All branch mutation and branch-inspection paths should declare:

- source snapshot policy
- target validation policy
- conflict semantics

The engine should own this consistently.

## Cluster C: Primitive lifecycle normalization

This cluster fixes:

- lack of one primitive contract
- search lifecycle split
- vector lifecycle fragmentation
- graph primitive vs overlay confusion
- derived-state classification drift

### Concrete target

For each primitive or subsystem, define:

- primary truth
- persisted derived state
- ephemeral caches
- open/recover owner
- follower refresh owner
- merge owner
- freeze owner

No behavior should live outside that declared contract.

## Cluster D: Vector simplification

This is a special cluster because vector is the most composition-sensitive primitive.

It fixes:

- vector zombie durability stacks
- mixed extension/hook/callback ownership
- hidden cache semantics
- process-global merge behavior

### Concrete target

Vector should become:

- KV truth
- one vector subsystem/runtime contract
- optional rebuildable caches
- instance-local merge/refresh/freeze behavior

No second WAL.
No second snapshot format.
No process-global semantic registration.

## Recommended Execution Order

1. Cluster A: branch identity and ancestry
2. Cluster B: branch mutation contract
3. Cluster C: primitive lifecycle normalization
4. Cluster D: vector simplification

This order matters because vector, graph, and branch DAG all currently rely on branch-runtime ambiguity.
Branch truth should be cleaned up before the overlay primitives are rewritten around it.

## Recommended Dispositions

### Promote or internalize

- storage-level branch ancestry truths into engine-owned metadata
- the useful parts of DAG as projection/rebuild logic, not branch authority
- search as an explicitly declared shared derived subsystem

### Delete

- vector WAL
- vector snapshot stack
- any primitive-global registration that survives once instance-local composition exists

### Rewrite boundary

- branch identity model
- branch lifecycle/status model
- vector lifecycle contract
- graph data vs branch overlay separation

## Acceptance-Test Program

These are the tests that should exist before this workstream is considered complete.

### Branch identity tests

- delete + recreate branch name produces a new generation with clean ancestry
- DAG/projection rebuild does not alter branch correctness
- merge-base remains correct after repeated merges and materialization without executor override

### Branch mutation tests

- merge, revert, cherry-pick, and diff all operate on declared snapshot semantics
- conflicting concurrent target edits are handled consistently across all branch mutation APIs
- merge-policy names and behaviors match exactly

### Primitive lifecycle tests

- each primitive has one declared recover/refresh/freeze owner
- cache database, primary database, and follower database expose the same primitive contract for installed primitives
- deleting any declared derived-state cache triggers rebuild, not correctness loss

### Vector tests

- vector collections rebuild fully from KV truth after deleting cache files
- follower refresh updates vector state through the declared contract only
- vector merge behavior is deterministic without process-global registration ordering

### Graph/search tests

- graph data remains correct without DAG/status projection
- branch DAG/status can be rebuilt from authoritative branch metadata if retained as projection
- search recovery, refresh, and freeze all run through one declared lifecycle path

## Bottom Line

The branch and primitive delta is not mainly about low-level data correctness.
The storage layer is stronger than the runtime layer here.

The real problems are:

1. branch truth is split across incompatible identity and ancestry models
2. overlay state is allowed to influence behavior without being authoritative
3. primitives do not share one lifecycle contract
4. vector is carrying the most runtime-attachment debt in the system

That means this workstream should focus on semantic authority and lifecycle ownership, not on rewriting the underlying storage mechanics.
