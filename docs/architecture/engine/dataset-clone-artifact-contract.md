# Engine Dataset Clone Artifact Contract

Status: current — describes shipped 1.2.x behaviour (#3134)

## Purpose

This document defines the engine contract for portable Strata dataset
artifacts.

The V1 product path is:

```text
strata clone <source> <destination>
Strata.open("<destination>")
```

After clone, the destination is a normal Strata database. The user can work
offline, branch, search, relate, mutate, export, and inspect data without
contacting the source.

The `.strata` artifact is the portable clone, publish, backup, and dataset
distribution package. It is not the live database format.

## Related Documents

Read this with:

1. `docs/product/strata-v1-product-requirements.md`
2. `docs/product/stratahub-product-direction.md`
3. `docs/product/pathways/runtime-and-portability.md`
4. `docs/architecture/strata-v1-architecture.md`
5. `docs/architecture/stratahub-substrate-architecture.md`
6. `docs/architecture/storage-architecture.md`
7. `docs/architecture/storage/l9-storage-api-boundary.md`
8. `docs/architecture/v1-error-and-diagnostics-contract.md`
9. `docs/architecture/engine/control-plane-layout-contract.md`
10. `docs/architecture/engine/retrieval-and-derived-state-contract.md`
11. `docs/architecture/engine/persistence-adapter-contract.md`
12. `docs/architecture/engine/ipc-and-command-boundary-contract.md`

The storage format specification owns durable storage bytes. This document owns
the product-level artifact contract above those bytes.

## Requirement Language

The words "must", "should", and "may" are used intentionally:

1. **Must** means V1 architecture depends on the rule.
2. **Should** means the rule is expected for V1 unless implementation discovers
   a concrete reason to amend this contract.
3. **May** means allowed but not required.

## Current Code Evidence

The current engine has branch bundle machinery in
`crates/engine/src/bundle/`. It writes `.branchbundle.tar.zst` archives with
manifest, branch metadata, and replay payloads.

That machinery is useful evidence for validation, checksums, atomic artifact
write, snapshot-isolated export, and replay import. It is not the V1 product
artifact:

1. It is branch-oriented, not dataset/database-oriented.
2. It uses branch-bundle vocabulary and extension.
3. It replays imported payloads as transactions.
4. It does not define StrataHub-compatible dataset identity, instance identity,
   provenance, derived-state rebuild markers, or clone semantics.

V1 should treat branch bundles as legacy data-movement machinery to learn from,
not as the artifact shape to preserve.

## Core Distinction

### Live Database

A live Strata database is directory-shaped. The directory contains storage
objects such as manifests, WAL, tables, snapshots, lock files, temporary
publish state, quarantine state, and engine-owned control rows.

Applications open the live database directory:

```text
Strata.open("~/Documents/Strata/titanic")
```

V1 should not chase a single-file live database. Strata's runtime needs room
for branch-aware storage, WAL, snapshots, derived-state rebuilds, quarantine,
temporary publish, local IPC files, and backend-specific object mapping.

### Portable Artifact

A `.strata` artifact is a sealed package used to move Strata data between
places:

```text
strata clone https://hub.example/titanic.strata ~/Documents/Strata/titanic
```

The artifact may be a single file. That is a distribution convenience, not a
runtime constraint. The clone operation validates the artifact and materializes
a normal live database at the destination.

### Future Direct Inspection

V1 does not require `Strata.open("dataset.strata")`.

A later release may support read-only inspection of `.strata` artifacts without
materializing a database, but that would be an additional artifact-reader path,
not a replacement for the live database directory.

## Binding Decisions

1. **`.strata` is a portable artifact, not a live database.**
   It is used for clone, publish, backup, release, validation, and transfer.
   Normal reads and writes target a materialized database.

2. **Clone creates local ownership.**
   Clone mints or assigns local database and instance identity while preserving
   provenance back to the source artifact, dataset, branch, and version.

3. **Clone is self-contained after completion.**
   A cloned database must open and operate without contacting the artifact
   source, StrataHub, a private hub, or any remote provider.

4. **The artifact is provider-neutral.**
   StrataHub is one possible host. Private hubs, local files, HTTP servers, and
   future providers may host the same artifact family.

5. **Storage is hub-agnostic.**
   Storage must not know accounts, organizations, hub URLs, remote refs,
   auth tokens, sync policies, or dataset discovery.

6. **Engine owns artifact product semantics.**
   Engine owns clone/import/export meaning, dataset metadata, provenance,
   branch selection, derived-state policy, error mapping, and compatibility
   checks.

7. **Storage owns generic install mechanics.**
   Storage may provide row-native export/install/checksum primitives, but
   it does not decide what a dataset means or where it came from.

8. **Source rows are authoritative.**
   Derived search/vector/graph state is rebuildable. Artifacts may include
   derived state only when it is explicitly validated against source coverage
   and recipe/configuration metadata.

9. **Secrets are excluded.**
   Hub credentials, refresh tokens, signing keys, machine credentials, local
   IPC files, and hidden fleet registration state must not be included.

10. **Artifacts are untrusted input.**
    Clone must validate the artifact before the destination becomes usable.
    Validation must reject unsupported format, unsupported features,
    invalid checksums, inconsistent branch metadata, path traversal, and size
    policy violations.

11. **Import is staged.**
    A failed clone must not leave a valid-looking database at the destination.
    Partial files may remain only in clearly marked temporary or quarantine
    locations.

12. **Sync is not implied.**
    A `.strata` artifact is not a sync protocol, remote mount, replication
    stream, distributed lock, or hosted database service.

13. **Branch and version scope is explicit.**
    An artifact must declare whether it contains a single branch point, a branch
    with retained history, multiple branches, or a full database export.

14. **Pre-V1 legacy migration is not a constraint.**
    Strata is pre-launch. V1 does not need to preserve old branch-bundle or
    pre-V1 database compatibility unless a later decision explicitly adds it.

## Artifact Kinds

V1 should keep artifact kinds small.

### Dataset Clone Artifact

The primary `.strata` artifact. It contains enough committed source data,
engine metadata, branch metadata, and provenance to create a normal local
database.

This is the required V1 path.

**Point-in-time, not history (#3198).** A dataset clone captures the *current*
committed value of each document, KV pair, vector entry, event, and graph
element on the exported branch — not its version history. Reopening a clone
shows one version per document even where the source retained several. This is
deliberate: the clone is an MVCC-consistent snapshot, so `time_travel` is **not**
a dataset-clone bundle capability and export never declares it in
`required_capabilities`. A history-carrying export is a possible future
capability (compare the Database Backup Artifact's "may preserve more history"
below); were it to land, it would declare `time_travel` and revise this
statement. Pinned by `json_export_is_point_in_time_not_history` in
`crates/engine/tests/artifact_export.rs`, and the capability absence by
`export_satisfies_the_output_invariants` in `crates/hub/tests/export_bundle.rs`.

### Database Backup Artifact

A backup-oriented artifact may use the same outer contract but different
policy:

1. It may include more local database metadata.
2. It may preserve more history.
3. It may be private by default.
4. It may be optimized for restore instead of public distribution.

The backup artifact is allowed, but V1 should not let backup needs complicate
the dataset clone path.

### Branch Artifact

A branch-scoped artifact may exist later for advanced collaboration, but it is
not the central V1 data movement story. Legacy branch bundles should not define
the V1 artifact vocabulary.

## Artifact Contents

The exact byte container is intentionally not frozen here. The artifact should
conceptually contain an outer manifest plus payload sections.

### Manifest

The manifest must describe:

1. Artifact format version.
2. Producer Strata version.
3. Producer engine data-capability registry version.
4. Required storage format version.
5. Artifact identity.
6. Optional dataset identity.
7. Optional source bundle or release identity.
8. Source database identity as provenance, not as the destination identity.
9. Source instance identity as provenance when appropriate.
10. Creation timestamp.
11. Artifact kind.
12. Scope: single branch point, branch history, multi-branch snapshot, or full
    database export.
13. Included branches and user-visible branch names.
14. Included commit-version and timestamp bounds.
15. Included storage-space IDs and their engine registry names.
16. Included data capabilities.
17. Counts and byte sizes.
18. Payload checksums.
19. Whole-artifact checksum or digest.
20. Required backend capabilities for import.
21. Derived-state disposition.
22. Provenance metadata.
23. License or trust metadata when supplied.
24. Declared redaction policy for optional metadata.
25. Format extensions required to read the artifact.

The manifest must not contain credentials or executable hooks.

### Source Data Payload

The source data payload contains committed user-authored data:

1. KV records.
2. JSON document records.
3. Event records.
4. Vector records supplied by the user or produced as source data.
5. Graph relationship records when relationships are authored state.
6. Tombstones and retained-history records needed to preserve declared branch
   and temporal semantics.
7. TTL metadata when retained rows require it.

Storage-space IDs identify row families. Engine owns the meaning of those
row families.

### Control Payload

The control payload contains engine-owned records needed to interpret the
source data:

1. Branch catalog records for included branches.
2. Commit timeline bounds and, when exported, timeline records.
3. Storage-space registry records.
4. Data-capability registry records.
5. Dataset metadata.
6. Provenance records.
7. Remote refs when explicitly included.
8. Recipe records.
9. Projection or derived-state manifests that are needed to decide whether
   derived state is valid or must be rebuilt.

Control records are data, not storage mechanics. They should be represented
through the same engine-controlled storage-space discipline as other
engine-owned rows.

### Derived-State Payload

Derived state is optional.

Examples:

1. BM25/text indexes.
2. Shadow vector embeddings.
3. Vector search indexes.
4. Graph traversal indexes and reverse maps.
5. Search recipe output caches.
6. Query expansion or reranking caches.

If included, derived state must carry enough metadata to validate:

1. Source coverage.
2. Source version or timestamp bounds.
3. Recipe/configuration identity.
4. Model identity where embeddings or generated text are involved.
5. Watermarks.
6. Checksums.
7. Whether the destination can use the derived state.

If validation fails, import must either reject the artifact or omit derived
state and mark it for rebuild. It must not silently install stale derived rows
as usable state.

Default V1 posture: omit derived state unless there is a clear reason to carry
it. Source data plus rebuild markers is the safer baseline.

### Excluded Runtime State

Clone artifacts must not include live runtime mechanics:

1. Active WAL segments.
2. Live MANIFEST files as the authority for the destination.
3. Lock files.
4. IPC sockets.
5. PID files.
6. Temporary publish files.
7. Quarantine files.
8. Process-local cache contents.
9. Local metrics snapshots unless explicitly exported as metadata.
10. Background job leases.
11. Provider credentials.
12. Hub credentials.
13. Machine identity secrets.
14. Local-only configuration that would surprise the destination user.

The clone process materializes a fresh database layout. It does not copy a live
database directory byte-for-byte.

## Scope Semantics

Every artifact must make its scope explicit.

### Single Branch At Version

Contains one branch resolved at one commit version.

This is the simplest dataset release shape. It should be enough for many
StrataHub Library datasets.

### Single Branch With History

Contains one branch plus retained history between declared version or timestamp
bounds.

This supports time travel and branch-from-history after clone, within the
declared retention window.

### Multi-Branch Snapshot

Contains multiple branches resolved at declared versions.

This supports datasets that intentionally publish experiment branches, curated
variants, or review branches.

### Full Database Export

Contains all selected branches, retained history, control metadata, and
provenance needed to reconstruct a database-level backup or migration.

This is more complex than a dataset clone artifact and should not be the only
way to publish simple datasets.

## Branch And Timeline Rules

1. `as_of` selectors must be resolved to commit versions before export.
2. Commit versions remain local ordering tokens, not global dataset version
   IDs.
3. If an artifact includes history, it must declare the first and last included
   commit versions and timestamps per branch.
4. If history has been truncated by retention, the artifact must say so.
5. Branch lineage should include only the branches and history points that are
   actually included.
6. Branch-from-time after clone is supported only within the artifact's
   included timeline bounds.
7. Branch names in the artifact are user-visible metadata. Internal destination
   branch identities may be remapped during clone.
8. Destination branch identities must not collide with existing destination
   state unless the user requested an explicit overwrite or merge mode.

## Clone Flow

The normal clone flow is:

1. Resolve the source.
   The source may be a local file, StrataHub URL, private hub URL, HTTP URL,
   object URL, or another explicitly supported provider.

2. Fetch bytes to a staging location.
   Network and provider fetch logic belongs above storage. Storage should
   only see local or provider-neutral object operations selected by the caller.

3. Read the outer manifest.
   The reader validates magic, artifact version, manifest checksum, declared
   sizes, and required features before reading large payloads.

4. Validate compatibility.
   The engine checks storage format version, engine capability registry,
   storage-space IDs, branch metadata, declared data capabilities, backend
   requirements, and policy.

5. Validate integrity.
   The clone path checks payload checksums, whole-artifact digest when
   available, entry counts, section lengths, and container paths.

6. Plan the destination.
   The destination must be empty, absent, or explicitly marked for overwrite.
   Clone must not merge into an existing database as an accidental side effect.

7. Create a staging database.
   The destination is built under a temporary directory or backend-native
   staging namespace. If the backend cannot atomically publish a whole
   directory or namespace, the staged database must carry an incomplete marker
   that normal open rejects.

8. Mint local identity.
   The destination receives new local database and instance identity while
   preserving artifact, dataset, source branch, and source version provenance.

9. Install committed source rows.
   Storage installs row-native state through storage-owned mechanics. Engine
   controls row-family meaning and any product-level translation.

10. Install control metadata.
    Engine installs branch catalog, registry, recipe, provenance, and derived
    state markers needed for normal open.

11. Handle derived state.
    Valid derived state may be installed. Missing or invalid derived state must
    be marked for rebuild, not treated as corruption.

12. Finalize atomically.
    The staging database is promoted to the destination using backend-supported
    publish mechanics. If the backend only supports weaker publication, clone
    must remove the incomplete marker last and report the weaker durability or
    atomicity semantics in diagnostics.

13. Verify open.
    The clone path should verify enough metadata to know the database can be
    opened normally.

14. Clean up staging.
    Temporary state is removed or quarantined with clear diagnostics.

After step 12, the destination must be a normal database. The artifact source is
not a runtime dependency.

## Partial Failure Rules

Clone failure must leave one of these states:

1. No destination exists.
2. A clearly named staging directory or namespace remains.
3. A quarantined failed import remains with diagnostics.

Clone failure must not leave:

1. A normal-looking destination database that opens successfully with partial
   data.
2. A destination missing provenance, branch catalog, or registry records needed
   for correct interpretation.
3. A destination whose derived state appears usable when source coverage was
   not validated.
4. A destination whose local identity was reused from the artifact source.

Normal open must reject any database carrying an incomplete clone/import marker.

## Export Flow

The normal export flow is:

1. Resolve scope.
   Engine resolves branch names, versions, timestamps, and history windows to
   concrete branch/version bounds.

2. Freeze a read view.
   Export must use a consistent view. Concurrent writes must not produce mixed
   branch or version state inside one artifact.

3. Collect source rows.
   Engine asks storage for row-native data through the persistence adapter
   contract. Storage returns rows and checksums, not product meaning.

4. Collect control rows.
   Engine includes only the control-plane records needed for the declared
   artifact kind and scope.

5. Decide derived-state policy.
   Engine either omits derived state, includes validated derived state, or
   records rebuild requirements.

6. Write to a temporary artifact.
   The writer streams sections, computes checksums, and avoids exposing a
   partial artifact as complete.

7. Seal the manifest.
   The final manifest records counts, sizes, checksums, compatibility, and
   provenance.

8. Publish the artifact.
   The artifact is atomically moved or published to the requested location where
   the backend supports it. Otherwise the result must report weaker publish
   semantics explicitly.

## Provenance

The artifact should preserve enough provenance to answer:

1. What dataset or source did this come from?
2. Which source branch and version were exported?
3. When was it exported?
4. Which Strata version produced it?
5. Which data capabilities and storage-space registry entries were used?
6. Which retained timeline window is included?
7. Which derived-state recipes or model identities were relevant?
8. Which license, trust, or usage metadata applies?
9. Was this produced from a public hub, private hub, local file, or local
   database?

Provenance is not an authority for local correctness. The materialized database
is the authority after clone.

## StrataHub And Private Hub Compatibility

The same artifact contract must support:

1. Public StrataHub Library datasets.
2. Private organization hubs.
3. Local file exchange.
4. HTTP or object-store artifact hosting.
5. Future backup and restore products.

The artifact must not hard-code `stratahub.com`.

Hub-specific metadata may wrap or index an artifact outside the artifact. The
artifact itself should contain enough manifest, provenance, compatibility, and
integrity information for clone to work from a file without hub API calls.

## Security And Trust

Clone artifacts are untrusted input.

Validation must defend against:

1. Unsupported artifact versions.
2. Unsupported required features.
3. Oversized declared sections.
4. Section length mismatches.
5. Checksum mismatch.
6. Whole-artifact digest mismatch.
7. Path traversal in container entries.
8. Duplicate or conflicting manifest entries.
9. Storage-space ID collisions.
10. Unknown required storage-space IDs.
11. Invalid branch metadata.
12. Invalid commit timeline bounds.
13. Invalid row encoding.
14. Derived-state/source mismatch.
15. Compression bombs.
16. Unexpected executable payloads.
17. Secret-like metadata where policy forbids it.

The clone path should fail closed. When it can safely continue by omitting
optional derived state, it must report that decision in diagnostics.

## Error And Diagnostic Shape

Clone/import/export errors should map into the V1 diagnostics contract.

Required error classes include:

1. Invalid artifact.
2. Unsupported artifact version.
3. Unsupported required feature.
4. Checksum mismatch.
5. Artifact truncated.
6. Artifact too large for policy.
7. Destination exists.
8. Destination publish failed.
9. Partial import cleaned up.
10. Partial import quarantined.
11. Backend capability missing.
12. Source unavailable.
13. Network disabled.
14. Provider authentication failed.
15. Provider authorization failed.
16. Derived state rejected.
17. Provenance policy rejected.

Diagnostics should include:

1. Artifact path or redacted source URL.
2. Destination path.
3. Artifact identity when readable.
4. Dataset identity when readable.
5. Failed section name.
6. Expected and actual checksum when safe to show.
7. Required feature or capability.
8. Cleanup outcome.
9. Whether retry can use the same request.

Errors must not leak credentials or full sensitive URLs.

## IPC And Local Ownership

Clone creates a database. Opening and sharing that database follows the normal
local ownership rules.

If another local process owns the destination after clone, clients should use
the local IPC command boundary. `.strata` artifacts do not introduce a server
mode and do not permit multiple writers to bypass the normal local owner.

## Conformance Requirements

The conformance suite should eventually include:

1. Clone a local `.strata` artifact into an empty local directory.
2. Reject clone into an existing destination without explicit overwrite.
3. Reject invalid manifest before installing rows.
4. Reject checksum mismatch before destination promotion.
5. Reject path traversal inside the artifact container.
6. Reject unsupported required feature.
7. Reject storage-space ID collision.
8. Verify clone mints destination identity.
9. Verify provenance points back to the source artifact.
10. Verify cloned database opens offline.
11. Verify source rows round trip for KV, JSON, event, vector, and graph
    relationship data.
12. Verify branch/version scope is preserved.
13. Verify branch-from-time works within included timeline bounds.
14. Verify branch-from-time fails clearly outside included timeline bounds.
15. Verify omitted derived state is marked rebuild-required.
16. Verify invalid derived state is not installed silently.
17. Verify clone failure cleans or quarantines staging state.
18. Verify artifact export uses a consistent read view under concurrent writes.
19. Verify no credentials or IPC files are included.
20. Verify private-hub and public-hub URLs are treated as provider-neutral
    sources.

## Non-Goals

This contract does not define:

1. The exact `.strata` byte container.
2. The final manifest serialization format.
3. Compression or signing algorithms.
4. StrataHub hosted APIs.
5. A sync protocol.
6. A remote query protocol.
7. A single-file live database.
8. Direct read/write open of `.strata` artifacts.
9. Legacy branch-bundle compatibility.
10. Public transaction export/import commands.

## Open Questions

These questions must be answered before artifact implementation is frozen:

1. What exact outer container should `.strata` use?
2. Is the manifest encoded as JSON, MessagePack, a storage-native binary
   format, or another stable format?
3. Does V1 require artifact signing, or are checksums enough for V1?
4. What default compression should artifact payloads use?
5. Is encryption part of V1 artifacts or deferred?
6. Which artifact scopes are required for V1 beyond single-branch dataset
   clone?
7. Should derived state be omitted by default for all public artifacts?
8. Which provider fetch schemes are in V1 CLI scope?
9. Does engine own URL fetch directly, or does CLI/SDK fetch to a local
   artifact first?
10. What metadata schema should represent license, trust, and PII warnings?
11. Should a later read-only artifact reader share code with clone validation?
12. Should backup artifacts use the same extension or a different subtype?

## V1 Minimum

The V1 minimum is:

1. `.strata` means portable artifact.
2. Clone materializes a normal database directory.
3. Clone validates before destination promotion.
4. Clone is offline after completion.
5. Clone mints local identity and preserves provenance.
6. Source rows are authoritative.
7. Derived state is omitted or validated and rebuild-marked.
8. Secrets and local runtime files are excluded.
9. Branch/version scope is explicit.
10. Legacy branch bundles do not define the V1 product artifact.
