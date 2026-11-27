# Engine Temporal Context And Timeline Resolver Contract

Status: current — describes shipped 1.2.x behaviour (#3134)

## Purpose

This document defines the engine contract for temporal reads, retained
history, timeline resolution, and time-travel product behavior.

Strata's storage layer owns generic branch timelines: commit versions,
commit timestamps, retention bounds, and the mapping from timestamp to retained
commit frontier. Engine owns the product meaning of those facts:

1. What `current`, `version`, `as_of`, and history mean to users.
2. How those selectors apply to KV, JSON, event, vector, graph, relationships,
   search, branching, restore, and retrieval.
3. How retained-history misses, tombstones, TTL, unsupported derived state, and
   malformed historical values surface.
4. How normal commands avoid ad hoc timestamp scans and instead use one shared
   temporal model.

The target flow is:

```text
product request
  -> parse temporal selector
  -> build branch-local temporal context
  -> resolve context through the timeline resolver
  -> read rows through the persistence adapter at the resolved frontier
  -> decode and apply capability semantics
  -> return values plus selected/observed temporal metadata
```

Storage remains primitive-agnostic. It does not know whether a retained row is
KV, JSON, event, vector, graph, search, relationship, recipe, or control-plane
data. Engine must not reintroduce storage-specific timeline machinery in
each capability.

## Related Documents

Read this with:

1. `docs/product/strata-v1-product-requirements.md`
2. `docs/product/strata-v1-feature-inventory.md`
3. `docs/product/strata-v1-user-pathways.md`
4. `docs/product/pathways/branching-versioning-time-travel.md`
5. `docs/product/strata-v1-versioning-time-travel.md`
6. `docs/product/strata-v1-branching-direction.md`
7. `docs/architecture/strata-v1-architecture.md`
8. `docs/architecture/engine-architecture.md`
9. `docs/architecture/storage/commit-timeline-substrate.md`
10. `docs/architecture/storage/l9-storage-api-boundary.md`
11. `docs/architecture/engine/primitive-implementation-contract.md`
12. `docs/architecture/engine/entity-ref-and-relationship-layer-contract.md`
13. `docs/architecture/engine/persistence-adapter-contract.md`
14. `docs/architecture/engine/branch-operation-and-capability-adapter-contract.md`

Follow-up contracts that depend on this one:

1. Control-plane layout contract.
2. Retrieval and derived-state contract.
3. IPC and serializable command-boundary contract.
4. Public API and CLI cleanup contract.

## Requirement Language

1. Must means the temporal contract is incomplete without it.
2. Should means expected unless a later architecture decision records a clear
   deferral.
3. May means allowed but not required for V1.

## Current Code Evidence

The current codebase already contains many temporal pieces, but they are not
owned by one engine concept.

1. `crates/core/src/contract/timestamp.rs` defines `Timestamp` as microseconds
   since the Unix epoch.
2. `crates/core/src/contract/version.rs` defines a broad `Version` enum with
   transaction, sequence, and counter variants. That is current compatibility
   vocabulary, not a sufficient storage commit timeline model by itself.
3. `crates/core/src/contract/versioned.rs` combines value, version, and
   timestamp for some outputs.
4. The executor exposes command-specific `as_of` timestamp fields for KV, JSON,
   event, vector, search, and branch diff paths.
5. `KvGet` and `JsonGet` currently lose version metadata on historical
   timestamp reads because `as_of` returns `Maybe` while current reads return
   `MaybeVersioned`.
6. `KvGetv`, `JsonGetv`, and `VectorGetv` expose history-like paths, but there
   is no single history output contract across capabilities.
7. Event commands use event timestamps for filtering paths. Current event code
   creates that timestamp at append time and often treats it as commit-time-like
   visibility data. V1 still needs to distinguish event-domain time from the
   storage commit timeline so future event ingestion does not collapse the two
   concepts accidentally.
8. Vector search has temporal support, but current code contains correctness
   caveats around inline metadata and source creation time versus visible
   commit time.
9. Search handlers combine `as_of`, diff, time range, and history enrichment,
   but they do not share a first-class temporal context with KV/JSON/vector.
10. Storage currently supports timestamp-bounded reads and scans, but the
    storage design adds an explicit per-branch commit timeline so whole
    branch operations can resolve timestamps to versions without scanning rows.

The target is not to rename these pieces. The target is to make the shared
temporal contract explicit so every capability consumes it the same way.

## Definitions

### Commit Version

A commit version is the authoritative storage timeline position for a branch.

Engine must treat commit version as the resolved frontier for visibility.
Capability-local versions such as event sequence numbers or vector counters may
remain product metadata, but they do not replace the branch commit version for
time-travel correctness.

### Commit Timestamp

A commit timestamp is the timestamp attached to a commit in the branch timeline.

Storage is responsible for making commit timestamps monotonic enough for
timestamp resolution within a branch. If the physical clock repeats or moves
backward, storage must still expose deterministic ordering through the
commit version tie-breaker.

### Temporal Selector

A temporal selector is the user's requested point:

```text
current
version:<commit-version>
time:<timestamp>
range:<start-selector>..<end-selector>
```

V1 must support `current`, retained commit `version`, retained commit `time`,
and ranges whose endpoints are temporal selectors. Per-record history is an
operation over a retained row chain, not a point selector. History requests may
carry temporal bounds, but they do not resolve to one frontier the way `current`,
`version`, and `time` do.

Ranges are required where existing product pathways need branch comparison,
restore, or timeline inspection, but they may be exposed through specific
commands rather than a universal public range API.

### Temporal Context

A temporal context is the engine-owned, branch-local interpretation of a
selector:

```text
TemporalContext {
    branch: BranchRef,
    selector: TemporalSelector,
    capability: CapabilityId,
    space: Option<SpaceRef>,
    strictness: TemporalStrictness,
}
```

This is conceptual shape, not a required Rust type name.

Temporal context is never ambient. Commands, branch workflows, relationship
resolution, and retrieval must pass the relevant branch and selector explicitly.

### Resolved Frontier

A resolved frontier is the storage commit position selected for a branch:

```text
ResolvedFrontier {
    branch_id: BranchId,
    selected: TemporalSelector,
    commit_version: CommitVersion,
    commit_timestamp: Timestamp,
    bounds: RetainedTimelineBounds,
    resolution: ResolutionKind,
}
```

`ResolutionKind` records whether the selector was current, exact version, exact
timestamp, or timestamp rounded down to the latest commit at or before the
requested time.

After-latest timestamp requests do not produce a normal resolved frontier for V1
product operations. They produce a typed out-of-range diagnostic so users choose
`current` intentionally instead of accidentally reading current state through a
future timestamp.

### Temporal Read View

A temporal read view is the retention-safe read scope used after frontier
resolution:

```text
TemporalReadView {
    frontier: ResolvedFrontier,
    retention_pin: RetentionPin,
}
```

This is conceptual shape, not a required Rust type name.

Any logical operation that reads more than one row must keep a storage read view,
retention pin, snapshot guard, or equivalent mechanism alive until the operation
finishes. Resolving a frontier is not enough if compaction or retention can
reclaim required rows before the reads complete.

### Observed Version

An observed version is the actual version of a record returned by a read.

For a point read at a resolved frontier, the observed record may be older than
the frontier if the record was last changed earlier. Product outputs should
distinguish:

1. The selected branch frontier.
2. The observed record version and timestamp.
3. Any capability-local version, such as event sequence or vector revision.

### Retained Timeline Bounds

Retained timeline bounds describe what branch commit timeline history is still
available:

```text
RetainedTimelineBounds {
    oldest_version: Option<CommitVersion>,
    oldest_timestamp: Option<Timestamp>,
    latest_version: Option<CommitVersion>,
    latest_timestamp: Option<Timestamp>,
    retention_reason: RetentionReason,
}
```

The exact Rust shape may differ, but the information must exist at the engine
boundary when a command reports an unavailable branch timestamp or version.

### Retained Record History Bounds

Retained record history bounds describe what history is still available for one
logical record, relationship, or capability-owned entity:

```text
RetainedRecordHistoryBounds {
    key_or_entity: EntityRef,
    oldest_observed_version: Option<CommitVersion>,
    oldest_observed_timestamp: Option<Timestamp>,
    latest_observed_version: Option<CommitVersion>,
    latest_observed_timestamp: Option<Timestamp>,
    completeness: HistoryCompleteness,
}
```

Branch timeline bounds and record history bounds are related but not identical.
A branch may retain timeline entries for a period where a specific record has no
history, and a record may be absent, deleted, or pruned independently of the
branch's overall timeline.

## Binding Decisions

### 1. `as_of` Means Timestamp

`as_of` is a timestamp selector. It must not mean "version" in one capability
and "timestamp" in another.

The user-facing version selector is a retained commit version. Existing `getv`
vocabulary maps to version/history behavior, not timestamp behavior.

### 2. Commit Version Is The Authoritative Frontier

Timestamp selectors must resolve to a commit version before engine performs
multi-row, cross-capability, branch, restore, or retrieval operations.

Single-row reads may use storage timestamp helpers internally only if the
persistence adapter can prove they produce the same retained-frontier semantics
and the same diagnostics as timeline resolution. The normal target path is still
timeline resolution first.

### 3. Current Is A Temporal Selector

`current` is not an implicit escape hatch. For any operation that reads more
than one row, more than one capability, or more than one branch, engine must
resolve current state once per branch at operation start and use that fixed
frontier for the operation.

This applies to:

1. Prefix scans and list commands.
2. Branch compare.
3. Branch restore.
4. Relationship traversal that fetches target entities.
5. Search and retrieval pipelines.
6. Export and clone reads.

### 4. Resolved Frontiers Must Be Retention-Safe

Resolving a temporal selector must also establish a read view, retention pin,
snapshot guard, or equivalent storage guarantee for operations that read more
than one row.

The guarantee is:

```text
rows required to answer the resolved frontier cannot be reclaimed while the
logical operation is still reading them
```

Without this rule, a long scan, retrieval pipeline, or branch comparison could
resolve a valid retained frontier and then lose required rows to retention or
compaction before the operation finishes.

### 5. Timestamp Resolution Is Branch-Local

A timestamp resolves against one branch timeline. Comparing two branches at the
same timestamp produces two resolved frontiers, one per branch.

The same wall-clock timestamp may resolve to different commit versions on
different branches.

### 6. Timestamp Resolution Is At-Or-Before

For `time:<timestamp>` within retained branch bounds, the resolver selects the
latest retained commit on the branch whose commit timestamp is less than or equal
to the requested timestamp.

If multiple commits have the same timestamp, the greatest commit version wins.

If the timestamp is before the oldest retained commit, the resolver returns a
retained-history diagnostic rather than reporting ordinary not-found.

If the timestamp is after the latest retained commit, the resolver returns an
after-latest diagnostic rather than silently clamping to current state. Product
surfaces that want current state should use `current`.

An internal maintenance or diagnostic API may explicitly ask storage for the
latest retained commit while reporting an after-latest condition, but that is not
the V1 product meaning of `time:<timestamp>`.

### 7. Version Resolution Is Exact

For `version:<commit-version>`, the resolver must validate that the requested
commit version is a retained branch timeline point and select that exact version
as the branch frontier, or fail with a typed diagnostic.

Row reads performed at that frontier still use normal MVCC visibility: a record
last changed at an older commit may be the value observed at the selected
frontier. The resolver must not round the branch frontier itself to a nearby
commit version.

### 8. Missing History Is Not Ordinary Absence

If a value cannot be returned because the requested point is outside retained
history, the result must not be indistinguishable from "the key never existed".

The engine must distinguish at least:

1. Key absent at the selected frontier.
2. Key deleted at or before the selected frontier.
3. Key may have existed but required history was trimmed.
4. Requested branch timeline is missing or corrupt.
5. Capability does not support the requested temporal mode.

### 9. Tombstone And TTL Semantics Apply At The Selected Frontier

Tombstones and TTL must be evaluated against the selected temporal frontier, not
against wall-clock time at query execution.

For version selectors, engine must resolve the selected commit version to
its commit timestamp before evaluating TTL or returning temporal metadata.

If a record existed at the selected frontier but has since expired or been
compacted away, the output must report retained-history or TTL retention facts
instead of silently using current state.

### 10. Derived State Must Declare Temporal Compatibility

Derived state is not automatically temporal just because source rows are
temporal.

Every derived engine surface must declare one of these compatibility classes:

1. Exact: the derived index is versioned and can answer at the resolved
   frontier.
2. Source-filtered: the derived index may produce candidates, but every returned
   result is verified against source rows at the resolved frontier.
3. Rebuild-required: the derived state can answer only after rebuilding against
   the resolved frontier.
4. Unsupported: the capability refuses the temporal request with a typed
   diagnostic.

Current derived state must never be used for historical correctness unless it is
classified as exact or source-filtered for that request.

### 11. Event-Domain Time Is Distinct From Commit Time

Event records may carry an event timestamp. In the current implementation, that
timestamp is normally append time. In future ingestion paths, it may be an
occurrence timestamp supplied by the user or application. Either way, that
timestamp is event-domain data.

Temporal context uses commit timeline time for visibility. Event-domain time
filters are event capability semantics layered on top of the resolved commit
frontier.

For example:

```text
event.list(as_of = 2026-05-10T12:00:00Z, event_timestamp_before = 2026-05-09)
```

means:

1. Resolve the branch commit frontier at or before May 10.
2. Read retained events visible at that frontier.
3. Apply the event-domain time filter inside the event capability.

The two timestamps must not be conflated.

### 12. Relationship Resolution Uses The Same Temporal Context

Graph traversal and relationship reads that return target entities must resolve
those target entities at the same temporal frontier unless the caller explicitly
requests pinned historical references.

If the relationship exists at the selected frontier but the target entity does
not, the result must report dangling, deleted, or history-trimmed target status.

### 13. Branch Workflows Reuse The Resolver

Branch-from-time, branch-from-version, compare-at-time, restore-at-time, and
selected-copy-from-history must all use the same timeline resolver.

Branch workflows must not implement separate timestamp lookup logic.

### 14. Raw Timestamp Syntax Is Not The Contract

The current CLI often exposes raw microsecond timestamps. V1 should accept
machine-stable forms, but user-facing surfaces should prefer ISO-8601 and
structured temporal selectors.

The temporal context contract owns the normalized selector. CLI/API syntax
belongs to the public API and CLI cleanup contract.

## Engine Responsibilities

Engine owns:

1. Parsing normalized product selectors into temporal contexts.
2. Resolving contexts through the persistence adapter.
3. Enforcing one frontier per branch per logical operation.
4. Mapping storage timeline misses into product diagnostics.
5. Applying capability semantics at the resolved frontier.
6. Returning selected and observed temporal metadata.
7. Ensuring derived state is exact, source-filtered, rebuilt, or refused.
8. Coordinating temporal branch workflows.
9. Testing product-level time travel behavior across capabilities.

Engine must not:

1. Store a second authoritative commit timeline outside storage.
2. Let each capability define incompatible `as_of` or history semantics.
3. Use current derived state to answer historical queries without verification.
4. Convert retained-history misses into ordinary not-found.
5. Ask storage to understand KV/JSON/event/vector/graph semantics.

## Storage Responsibilities Consumed By Engine

Storage must expose, through L9 and the persistence adapter:

1. Current branch frontier.
2. Exact retained version resolution.
3. Timestamp-to-version resolution.
4. Version-to-timestamp resolution.
5. Retained timeline bounds.
6. Version-bounded point reads.
7. Version-bounded prefix/range scans.
8. Retained history reads.
9. Retention-safe read views, pins, snapshots, or equivalent guards.
10. Tombstone and TTL visibility facts.
11. Timeline corruption and recovery diagnostics.
12. Cache-mode durability facts.

Engine consumes those facts. It does not infer them by reading storage
internals.

## Temporal Selector Semantics

### Current Reads

Current reads select the branch's current visible frontier.

For point reads, the selected frontier should be included when the output format
supports metadata. For list, scan, search, branch compare, and retrieval, the
frontier must be fixed and retention-pinned before the operation reads rows.

### Version Reads

Version reads select a retained commit version.

V1 must support:

1. Point read at version.
2. Prefix/range scan at version where the capability supports scans.
3. Branch compare at version.
4. Branch creation from version.
5. Restore/copy from version.

Version reads fail if the version is not retained, belongs to a different
branch lineage where that matters, or is otherwise not reachable for the
requested branch.

### Timestamp Reads

Timestamp reads select the retained commit frontier at or before a timestamp
when the requested timestamp is inside retained branch timeline bounds.

V1 must support:

1. Point read as of timestamp.
2. Prefix/range scan as of timestamp.
3. Branch compare as of timestamp.
4. Branch creation from timestamp.
5. Restore/copy from timestamp.
6. Search/retrieval as of timestamp where derived state can answer correctly.

The resolver reports whether resolution was exact, rounded down, or rejected as
before-history, after-latest, or inside a pruned gap.

### History Reads

History reads return retained changes for a record or relationship.

History output must include:

1. Commit version.
2. Commit timestamp.
3. Value or deletion marker.
4. Capability-local version when relevant.
5. Record history bounds when the history may be incomplete.
6. Branch timeline bounds when the requested history bound cannot resolve.

History order should be newest-first by default for interactive use, with an
explicit option for oldest-first if needed by export or replay tooling.

### Range Reads

Range selectors are used by compare, restore preview, timeline inspection, and
some search/retrieval diagnostics.

A range endpoint may be current, version, or timestamp. The resolver converts
each endpoint to a retained commit frontier before the capability compares row
state.

## Capability Requirements

### KV

KV must support current, version, timestamp, list/scan at temporal frontier, and
history with deletion markers.

Existing `get`, `as_of`, and `getv` surfaces should be normalized so historical
reads can return metadata instead of dropping to value-only output.

### JSON

JSON must support the same temporal selectors as KV for documents.

Patch-level history may remain a higher-level product decision. The temporal
contract only requires retained document versions, timestamps, deletion markers,
and retained-history diagnostics.

### Event

Events must distinguish commit time from event-domain time. In simple append
paths these may be created from the same clock reading, but they remain separate
semantic fields.

Sequence-based event reads remain useful, but event sequence is not the branch
commit frontier. Event temporal reads must first select visible commits, then
apply event-domain filters such as type, sequence, and event timestamp.

### Vector

Vector records and vector search must support temporal point reads and history
where retained source rows exist.

Vector similarity search as of a timestamp is V1-supported through
source-filtering by default. It may be performance-unoptimized. A request is
allowed only when one of these is true:

1. The vector index can answer exactly at the resolved frontier.
2. The vector index can produce candidates and the engine verifies every
   returned candidate against source rows at the resolved frontier.
3. The command clearly reports that temporal vector search is unsupported for
   the selected backend/index.

Using current vector metadata for historical correctness is forbidden.
If the selected backend or index can miss valid historical candidates when used
as a candidate generator, the engine must refuse the historical vector search
rather than return an incomplete result as exact.

### Graph And Relationships

Graph nodes, edges, relationship bindings, reverse maps, and traversal results
must be evaluated at the selected frontier.

Traversal may return:

1. Graph node identity.
2. Bound entity ref.
3. Target entity status at the selected frontier.
4. Selected and observed temporal metadata where the output supports it.

Ontology and graph analytics may refuse historical operation until they have an
exact or source-filtered temporal implementation.

### Search And Retrieval

Search and retrieval combine source rows, derived indexes, graph relationships,
and ranking. They must treat temporal compatibility as a first-class result
fact.

Search as of a timestamp must not return documents that are invisible at the
resolved frontier. Query expansion, reranking, and generation context may be
derived from current models, but selected source rows must be visible at the
requested frontier and provenance must record the temporal point used.

## Diagnostics

The resolver and capability layer should map failures into the V1 error and
diagnostics contract. The exact error code names belong to the error registry,
but V1 must distinguish these cases:

1. Invalid temporal selector syntax.
2. Unsupported selector for capability.
3. Missing branch.
4. Missing timeline.
5. Corrupt timeline.
6. Requested version not retained.
7. Requested timestamp before retained history.
8. Requested timestamp after latest retained commit.
9. Requested timestamp in a pruned gap.
10. Key absent at selected frontier.
11. Key deleted at selected frontier.
12. Value malformed at selected frontier.
13. TTL expired at selected frontier.
14. Derived state stale for selected frontier.
15. Derived state unsupported for selected frontier.
16. Cache mode cannot make durable timeline guarantees after reopen.
17. Backend does not support the requested history or retention behavior.

The diagnostic payload should include the branch, selector, timeline bounds,
record history bounds, and resolved frontier when available.

## Output Metadata

Every structured temporal output must be able to expose this shape. Human compact
renderings may choose a smaller display:

```text
TemporalMetadata {
    selected_branch: BranchRef,
    selected_selector: Option<TemporalSelector>,
    selected_frontier: Option<CommitVersion>,
    selected_timestamp: Option<Timestamp>,
    observed_version: Option<CommitVersion>,
    observed_timestamp: Option<Timestamp>,
    capability_version: Option<CapabilityVersion>,
    timeline_bounds: Option<RetainedTimelineBounds>,
    record_history_bounds: Option<RetainedRecordHistoryBounds>,
    resolution: Option<ResolutionKind>,
}
```

Human compact output modes may omit fields for readability, but structured SDK,
IPC, JSON, and machine-readable CLI output must be able to carry selected
frontier, selected timestamp, observed version/timestamp, resolution facts,
timeline bounds, and record-history bounds for temporal reads. This is required
for Strata AI, debugging, history explanations, and reproducible retrieval.
`selected_selector` is optional only for outputs such as unbounded history that
are not point-in-time reads.

## Cache Mode

Cache mode may maintain an in-memory timeline during the lifetime of the
database handle.

Cache mode must not claim durable retained-history behavior across process
restart. If a cache database exposes temporal reads, diagnostics must make the
non-durable timeline explicit when relevant.

Cache mode still must obey the same in-process temporal semantics:

1. `as_of` means timestamp.
2. `getv` means version/history.
3. Multi-row operations use one frontier.
4. Derived state cannot silently mix current and historical data.

## Clone, Import, Export, And StrataHub Substrate

Temporal metadata is part of the portability substrate.

Clone/import/export workflows must either:

1. Preserve commit versions, commit timestamps, timeline bounds, record-history
   bounds, and branch timeline identity where the format supports it.
2. Rebase the timeline deliberately and report that the imported database has a
   new timeline.

They must not silently produce a database where historical commands appear to
work but refer to different temporal points than the source.

StrataHub and internal fleet-management implementations may build richer
timeline provenance above this contract, but they should not require storage or
engine to change the V1 temporal model.

## Conformance Tests

The engine test suite must include:

1. `as_of` is timestamp-only across KV, JSON, event, vector, graph, search, and
   branch diff surfaces that support it. Event's `range` (by sequence) and
   `range_by_time` (by event-domain time) are latest-only and take no `as_of`.
   Event's commit-timeline reads are `as_of` on `get`, `len` and `list`; `list`
   pages forward with a sequence cursor, and there is no newest-first read as
   of a commit. A sequence number is not a stable index into history: a forced
   space delete restarts the counter, so a latest `range` bounded by an as-of
   `len` can read the new log under the old numbers (#3145).
2. `getv` and history surfaces return retained versions, timestamps, deletion
   markers, timeline bounds, and record-history bounds.
3. Current multi-row reads resolve one frontier and do not observe a concurrent
   later commit mid-operation.
4. Timestamp resolution selects the latest retained commit at or before the
   requested timestamp when the timestamp is inside retained branch bounds.
5. Duplicate commit timestamps resolve by greatest commit version.
6. Clock rollback still produces deterministic branch timeline ordering.
7. Before-history, after-latest, and pruned-gap cases surface distinct
   diagnostics; after-latest does not silently clamp to current state.
8. Absent, deleted, malformed, TTL-expired, and history-trimmed values are
   distinguishable.
9. Multi-row temporal operations keep a retention-safe read view alive until the
   operation finishes.
10. Branch-from-time and compare-at-time use the same resolver as reads.
11. Relationship traversal resolves targets at the same frontier as graph
    edges.
12. Event-domain time filters do not replace commit-time visibility.
13. Temporal vector/search paths either prove exact/source-filtered behavior or
    refuse with a typed diagnostic.
14. Cache mode preserves in-process timeline behavior and reports non-durable
    history after reopen.
15. Storage timeline corruption maps to product diagnostics without falling back
    to ad hoc row scans.
16. IPC and local handles return equivalent temporal results and diagnostics.

## Deferred Or Open Questions

These questions do not block the contract, but they must be resolved before V1
API freeze:

1. Exact Rust type names for `TemporalContext`, `TemporalSelector`,
   `ResolvedFrontier`, and `TemporalMetadata`.
2. Public CLI syntax for version selectors, timestamp selectors, and ranges.
3. Default retained-history policy and output wording.
4. Whether historical derived search may ever return explicitly approximate
   results, or whether V1 always requires exact/source-filtered/refused.
5. How much event-domain time filtering belongs in the shared temporal API
   versus the event capability API.
6. Whether pinned historical relationship references are a V1 feature or a
   post-V1 extension.
7. Whether cache-mode timeline bounds are exposed through the same public
   metadata as durable modes or marked as ephemeral-only.

## V1 Minimum

For V1, the minimum acceptable implementation is:

1. A shared engine temporal context used by reads, branch workflows,
   relationships, search, and retrieval.
2. `as_of` consistently means timestamp.
3. Version selectors consistently name exact retained branch-frontier commit
   versions.
4. Current state resolves once per branch for multi-row and cross-capability
   operations.
5. Timestamp selectors resolve through the branch commit timeline to retained
   commit versions.
6. Point reads, scans, history, branch compare, branch-from-history, and restore
   use resolved frontiers.
7. Structured outputs can carry selected frontier, observed version/timestamp,
   deletion markers, timeline bounds, and record-history bounds.
8. Missing history, absent values, deleted values, TTL expiry, and unsupported
   temporal derived state are distinct diagnostics.
9. Event-domain time is separate from commit timeline time.
10. Derived state must be exact, source-filtered, rebuild-required, or refused
    for temporal requests.
11. Multi-row temporal operations use retention-safe read views or equivalent
    storage guards.
12. Cache mode reports non-durable timeline behavior.
13. Conformance tests cover the temporal semantics above across local and IPC
    paths.

## Next Step

The next contract should be the control-plane layout contract.

That contract should define the `_system_` branch, branch-local system spaces,
capability registry, storage-space registry persistence, temporal/derived-state
metadata, provenance, and control-plane write authority.
