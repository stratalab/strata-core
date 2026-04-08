# Secondary Primitives Audit

Date: 2026-04-08

Scope: `graph`, `vector`, `search`, `event`, `JSON`

Method: code inspection only. This pass traces each primitive through the same lifecycle lens:

- durable source of truth
- open/recovery path
- follower refresh path
- branch merge behavior
- freeze/shutdown behavior
- extra runtime state outside storage

## Cross-Primitive Verdict

These five primitives do not sit at the same architectural level.

- `JSON` and `Event` are mostly engine-native primitives. Their durable truth lives in storage, and their runtime surface is comparatively small.
- `Graph` data is storage-native, but graph merge semantics and branch DAG/status behavior are optional overlays that must be registered from the graph crate.
- `Search` is not primary truth. It is a shared derived subsystem backed by its own in-memory and on-disk cache format.
- `Vector` is KV-backed for truth, but operationally it is the most fragmented primitive: recovery, refresh, merge, and fast-path state are spread across extensions, hooks, mmap caches, and global registration.

The main architectural pattern is simple:

- the closer a primitive stays to storage plus normal engine transactions, the cleaner it is
- the more in-memory runtime state it grows, the more it escapes the engine's canonical lifecycle

## Matrix

| Primitive | Durable truth | Open / recovery | Refresh | Merge | Freeze / shutdown | Main seam |
|---|---|---|---|---|---|---|
| JSON | `TypeTag::Json` KV entries plus `_idx*` index subspaces | No dedicated subsystem required | None beyond normal storage visibility | Engine-owned `JsonMergeHandler` with path-level three-way merge | No primitive-local freeze path | Public API is more document-level than internal merge semantics |
| Event | `TypeTag::Event` KV entries plus metadata and per-type index keys | No dedicated subsystem required | None beyond normal storage visibility | Engine-owned divergence precheck only; no semantic merge | No primitive-local freeze path | Divergent concurrent appends are refused, not merged |
| Graph | `TypeTag::Graph` KV entries in `_graph_` space | No graph-data recovery path; `GraphSubsystem` bootstraps DAG/status overlay | No graph-specific hook; search refresh sees graph docs through shared index | Semantic merge depends on graph-side registration; fallback refuses divergent merges | No graph-data freeze path | Ordinary graph data and branch-DAG/status overlay are mixed together conceptually |
| Search | Shared `InvertedIndex` extension plus frozen manifest / `.sidx` cache files | `SearchSubsystem` rebuilds from KV/Event/Graph/JSON or loads disk cache | Engine hardcodes search refresh inside `Database::refresh()` | Not a branch-merge primitive; rebuilt / updated as derived state | `freeze_search_index()` | Shared derived state with its own lifecycle outside storage |
| Vector | KV-backed collection config and vector records; mmap files are caches | `VectorSubsystem` rebuilds backends from KV, optionally loads mmap caches, then installs refresh hook | `RefreshHook` extension updates vector state after follower replay | Engine precheck / post-commit depend on vector-side callbacks and reload | `freeze_vector_heaps()` is routed through refresh hooks | Largest split between storage truth and runtime-attached state |

## JSON

`JsonStore` is the cleanest model of a higher-level primitive in the engine.

- It is a stateless facade over `Arc<Database>`.
- All durable truth is persisted in normal storage under `TypeTag::Json`.
- Secondary indexes are also persisted in storage under `_idx/...` and `_idx_meta/...`.
- Branch merge semantics are owned inside the engine through `JsonMergeHandler`.

What makes JSON relatively strong:

- it does not need a recovery subsystem to become usable after open
- it does not rely on database extensions for core correctness
- its secondary indexes are still storage-backed, not separate runtime-only state
- its merge path is engine-owned rather than callback-owned

Main seam:

- The public JSON API is still more document-level than the internal merge/conflict model. Internally, branch merge can do path-aware three-way combination of one document. Day-to-day transactional JSON usage still goes through document read/modify/write patterns, so the externally visible concurrency contract is weaker than the merge machinery suggests.

Verdict:

- architecturally healthy
- closest thing to a "reference primitive" for the rest of the system

## Event

`EventLog` is also mostly engine-native, but with intentionally narrow semantics.

- Durable truth is the append-only event stream in storage under `TypeTag::Event`.
- The stream metadata key tracks `next_sequence`, `head_hash`, hash version, and per-stream metadata.
- Appends are ordinary engine transactions that write the event, update metadata, and add per-type index keys.
- Search indexing is a best-effort side effect into the shared `InvertedIndex`.

What makes Event relatively clean:

- no dedicated subsystem is required for recovery
- no primitive-local freeze or startup rebuild is required for correctness
- core invariants are explicit: single ordered append stream, causal hash chain, append-only storage

Main seam:

- Branch merge semantics are intentionally incomplete. `EventMergeHandler` only runs divergence detection. If both sides advanced the event stream past the ancestor, the merge is refused. There is no first-class event rebase or semantic append merge.

Verdict:

- architecturally coherent for single-branch append workloads
- weak once branch workflows expect semantic reconciliation

## Graph

Graph needs to be split mentally into two different things:

- ordinary graph data
- branch DAG and branch status overlay

Ordinary graph data is relatively straightforward:

- `GraphStore` is a stateless facade over `Arc<Database>`
- nodes and edges live in storage under `TypeTag::Graph` in `_graph_`
- transactional semantics come from the engine and storage layers
- graph documents are searchable only because they participate in the shared search index

The architectural seams show up above that layer:

- semantic graph merge is not intrinsic to the engine; it depends on `register_graph_semantic_merge()`
- if that callback is not registered, the engine falls back to tactical refusal of divergent graph merges
- `GraphSubsystem` is not about graph-data recovery; it bootstraps `_system_`, `_branch_dag`, and status-cache overlay behavior
- branch DAG writes are documented as best-effort and non-authoritative
- `BranchStatusCache` is an in-memory extension keyed by branch name, not by resolved branch identity

This means graph is only half a primitive:

- graph data itself behaves like an engine-native stored primitive
- graph branch semantics and lifecycle overlays behave like optional runtime attachments

Verdict:

- graph data path is credible
- graph lifecycle semantics are not first-class and still depend on registration and overlay state

## Search

Search is a shared derived subsystem, not a source-of-truth primitive.

Its real architecture is:

- in-memory `InvertedIndex` as a database extension
- optional on-disk frozen cache under `{data_dir}/search/`
- `SearchSubsystem` to rebuild from storage or load the frozen cache
- engine-owned refresh logic inside `Database::refresh()`

Important properties:

- search recovery scans KV, Event, Graph, and JSON entries
- JSON internal index spaces are explicitly skipped so BM25 does not ingest index storage
- the on-disk search format is treated as a cache; failures fall back to rebuild
- cache databases bypass subsystem recovery and directly enable the in-memory index

Search is therefore operationally central but architecturally subordinate:

- it cuts across multiple primitives
- it has its own freeze/load lifecycle
- it is not needed for correctness of the underlying data

Main seam:

- search lifecycle is partly subsystem-driven and partly engine-hardcoded. Startup recovery comes from `SearchSubsystem`, but follower refresh directly mutates the inverted index in `Database::refresh()` rather than going through the same generic extension contract used by vector.

Verdict:

- acceptable as a derived subsystem
- should be treated explicitly as shared derived infrastructure, not as a peer durable primitive

## Vector

Vector is the most complicated secondary primitive in the codebase.

Its durable truth is still storage-backed:

- collection config and vector records live in KV
- recovery can always rebuild from storage
- mmap heap files and graph files are explicitly documented as caches

But the runtime shape is much more fragmented than any other primitive:

- `VectorBackendState` stores live backends as a database extension
- `VectorSubsystem` performs startup recovery
- startup recovery also registers a `RefreshHook`
- follower refresh depends on that hook being present
- merge precheck / post-merge behavior depends on vector-side callback registration
- shutdown freeze is implemented through the generic refresh-hook container
- there is also dormant vector-specific WAL and snapshot code elsewhere in the crate

That produces several architectural consequences:

- vector correctness still depends on storage truth, but usable runtime state depends on extra attachment work during open
- vector freeze is conceptually mis-layered because it is routed through refresh hooks, not a vector-specific lifecycle contract
- vector merge behavior is not entirely engine-owned; the engine has callback slots and the vector crate fills them
- follower behavior is more composition-sensitive for vector than for the other primitives

Vector is not broken in the same way as a shadow stack, because the active path is real and KV-backed. But it is the clearest example of a primitive whose runtime behavior has spread across too many attachment mechanisms.

Verdict:

- KV durability story is defensible
- runtime composition story is the weakest of the five primitives

## Comparative Findings

### 1. There is no single primitive contract above storage

The engine exposes one storage and transaction substrate, but the primitives layer diverges quickly:

- JSON and Event mainly rely on storage + transaction semantics
- Search adds subsystem recovery and its own persisted cache
- Graph adds registration-driven merge and branch overlays
- Vector adds extensions, hooks, callbacks, and caches

This makes primitive complexity non-linear. Once a primitive acquires runtime-local state, it tends to escape the engine's base contract.

### 2. Search is the shared overlay that crosses primitive boundaries

Search touches KV, Event, Graph, and JSON. It is not merely "the search primitive"; it is a cross-cutting derived subsystem layered on top of multiple primary data models. That means composition bugs in search affect multiple primitives at once.

### 3. Vector is the most composition-sensitive primitive

Vector has the highest number of moving parts:

- storage truth
- startup rebuild
- mmap fast path
- live backend extension state
- follower refresh hook
- merge callback registration
- merge-time reload
- freeze-to-disk cache

No other primitive spreads itself across that many runtime contracts.

### 4. Graph is bifurcated

The graph crate contains both:

- ordinary graph storage behavior
- branch-DAG and status machinery that acts like a control-plane overlay

Those are different responsibilities. Treating them as one "graph primitive" obscures where the actual seams are.

### 5. JSON and Event show what the cleaner architecture looks like

They are not perfect, but they mostly follow a better rule:

- durable truth is in storage
- runtime behavior is driven by engine transactions
- there is little or no extra recovery attachment required

That is the healthier model.

## Fix Direction

If Strata wants a cleaner primitive architecture, the target should be:

- primary primitive correctness comes only from storage plus engine transaction semantics
- derived subsystems are explicitly declared as overlays
- recovery, refresh, merge, and freeze are all owned by one explicit primitive contract, not a mixture of callbacks, hooks, and globals

Concrete direction by primitive:

- JSON: keep as the reference model; tighten the public concurrency story to better match internal merge semantics
- Event: add an explicit branch-level event reconciliation model, or keep strict refusal but document it as a hard product limit
- Graph: separate ordinary graph storage from branch-DAG/status overlay in the architecture and internalize graph merge registration
- Search: formalize it as shared derived infrastructure with one consistent lifecycle path
- Vector: collapse lifecycle ownership so recovery, refresh, merge reload, and freeze are driven by one explicit primitive runtime contract

## Overall Verdict

The secondary primitives are not uniformly broken. They fall on a spectrum:

- healthiest: `JSON`
- healthy but semantically narrow: `Event`
- healthy data path, weak overlay integration: `Graph`
- acceptable derived subsystem, composition-sensitive: `Search`
- most internally fragmented: `Vector`

The biggest architectural risk in this area is not data corruption. It is lifecycle inconsistency:

- some primitives are available directly from storage
- some need subsystem recovery
- some need refresh hooks
- some need global callback registration
- some need local frozen caches to be performant

That inconsistency is what makes the secondary-primitive layer hard to reason about.
