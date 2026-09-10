# StrataDB Engine Invariant Catalog

> **Purpose**: Durable specification of correctness properties for the StrataDB engine.
> Survives code refactors — anchored to architectural properties, not file paths or line numbers.
> Used as a standing audit checklist: after any significant change, verify the relevant invariants.
>
> **How to use**: Each invariant has an **Audit** instruction. Execute it against the current codebase.
> The instruction tells you *what to find*, not *where to find it*. If the code moved, the invariant
> still applies — just search for it.
>
> **Maintenance**: Update when the *architecture* changes, not when code is refactored.
> If a new compaction strategy is added, add invariants for it. If a function is renamed, do nothing.
>
> **Categories**: LSM (8), CMP (8), COW (9), MVCC (8), ACID (7), ARCH (10, one retired), SCALE (11), DUR (15) = 76 entries, 75 active
>
> **2026-08-19 V1 refresh**: a four-way audit of every entry against the post-promotion codebase
> re-anchored the pre-V1 families (LSM/CMP/COW/MVCC/ACID/ARCH/SCALE) to V1 mechanisms, retired
> ARCH-006 (its subject was removed), rewrote ACID-004/ACID-007 whose premises no longer exist,
> and added the post-promotion law (generation fencing, derived timeline, pruning proofs,
> deferred durability, the unresolved-durable gate). File anchors below are hints as of that
> refresh — the property, not the path, is the invariant.

---

## LSM — Storage Engine Invariants

### LSM-001: InternalKey sort order

Internal-key byte ordering MUST produce ascending order by
`(branch_id, space, storage_space_id, user_key)` and descending order by `commit_version`
within the same logical key. This is achieved by bitwise-NOT encoding of the version suffix
(`!version` big-endian). If this invariant is broken, every MVCC read, every merge
iteration, and every compaction produces wrong results.

**Audit**: Find the physical-key encoding (`format/key.rs`). Verify the version is inverted
before big-endian encoding and the prefix layout matches. Test:
`internal_key_sorts_newest_version_first_for_same_key`.

### LSM-002: Byte-stuffing roundtrip

The user_key encoding uses byte-stuffing (`0x00` → `0x00 0x01`, terminated by `0x00 0x00`) to preserve
lexicographic ordering for arbitrary binary keys. Encoding then decoding MUST produce the original
bytes for all inputs, including: empty input, input containing `0x00`, input containing `0x00 0x01`,
input ending with `0x00`.

**Audit**: Find `encode_escaped` and `decode_escaped`. Verify roundtrip correctness. Check that no
valid encoded sequence is a prefix of another (unambiguous parsing). Verify the terminator `0x00 0x00`
cannot appear within the encoded data.

### LSM-003: Read path level ordering

The point-read path MUST consider sources in this order: active memtable → frozen memtables
(newest first) → owned L0 tables (all of them) → L1 → ... → L{max} → inherited COW layers
(nearest ancestor first). Selection is version-max under the read bound, not first-match:
every candidate under the bound competes and the highest commit version wins; early exit is
legal only when no remaining source can hold a version above the already-selected one.
Within L0, ALL tables are consulted (their key ranges overlap); L0 file order is not
load-bearing for correctness.

**Audit**: Find the ordered point-candidate selection (`select_ordered_visible_point_candidate`,
`branch/read.rs`). Trace the source ordering and the early-exit predicate
(`remaining_max_commit <= selected_commit`). Verify no early return can skip a source whose
max commit version exceeds the current candidate's.

### LSM-004: Memtable rotation atomicity

When the active memtable exceeds its rotation threshold, the freeze-and-swap MUST be atomic
with respect to readers and writers. A reader must never see a state where an entry is in
neither the frozen nor the new active table. Writes into a frozen table MUST be impossible.
V1 enforces this structurally: `freeze(self)` consumes the mutable table by value and the
resulting `FrozenTable` has no insert surface (type-level prevention, stronger than a flag);
rotation runs under `&mut self` behind the runtime mutex, so no writer can race the swap,
and readers observe pre- or post-rotation snapshots only (DUR-002 covers the republish
ordering).

**Audit**: Find `rotate_active` (`branch/state/rotation.rs`) and `MutableTable::freeze`
(`table/mutable.rs`). Verify freeze consumes by value and `FrozenTable` exposes no mutation.
Verify rotation is reachable only under the runtime's exclusive access.

### LSM-005: Bloom filter correctness with rewritten keys

Bloom filters are built from `typed_key_prefix` bytes that include the branch_id. When a COW child
queries an inherited segment, it rewrites the key to the source's branch_id before probing. The
rewritten key MUST produce the exact same bytes that were used when building the bloom filter.
Any divergence causes false negatives — data silently invisible.

**Audit**: The bloom is built over encoded physical-key bytes (`physical_key_bytes()`,
branch_id included, commit suffix stripped — `TableBloomFilter::build`, `table/builder.rs`).
Verify the inherited-layer path rewrites the key (`rewrite_physical_key_branch`,
`branch/identity.rs`) BEFORE building the lookup/probe (`branch/read.rs`), producing bytes
identical to the source builder's. Note the pruning pin test is `perf-trace`-gated
(`branch_point_read_prunes_inherited_nonzero_levels_after_key_rewrite`).

### LSM-006: Block cache keying independence from branches

The block cache is keyed by `(table identity, block address)` where the table identity is the
immutable object-identity string — never a path, never the caller. Shared tables (accessed
from multiple branches via COW inheritance) MUST use the same cache entries regardless of
which branch is reading. Cache keys MUST NOT include branch_id.

**Audit**: Find `TableBlockCacheKey` (`table/cache.rs`) and its construction from
`TableCacheTableId::new(identity.as_str())` (`table/reader.rs`). Verify no branch reference
exists in the cache module. `table_cache_keys_use_table_identity_not_path` pins the
identity-not-path direction; `table_object_reader_service_shared_cache_hits_across_readers`
pins cross-reader sharing.

### LSM-007: Table object immutability

Once a table object (`tables/<branch>/<level>/<table_id>`) is published, it MUST NOT be
modified. Publication is create-only: the object publisher refuses an existing object,
installs via temp-write + fsync + no-clobber hard link, and all reads are positional against
the immutable object. Flush and compaction create NEW table objects; nothing rewrites an
existing one. Concurrent readers holding reader handles to old tables therefore always see
consistent data.

**Audit**: Verify `TableObjectService::publish_create` goes through
`ObjectPublisher::publish_durable_create` with `PublishMode::Create` (refusal on existing —
`backend/local_fs.rs`), and the install path uses no-clobber linking. Grep for any production
`set_len`/`truncate`/re-`write` on table objects outside test fault-injectors.

### LSM-008: Unique internal-key law

No two rows may share an identical encoded internal key `(physical key, commit version)`
within a memtable or across the sources of a compaction. Violations are hard
`DuplicateInternalKey` errors — never silently resolved — because MVCC candidate selection
depends on the pair's uniqueness. Byte-identical replay redundancy is the one legal
duplicate shape and is classified explicitly at the replay boundary (ACID-005), never
inside table machinery.

**Audit**: Verify the memtable insert rejects duplicate internal keys (`table/mutable.rs`),
sorted-unique validation runs at table build (`table/key.rs`), and compaction's global
duplicate pass fails closed (`table/compaction.rs`).

---

## LSM — Compaction Invariants

### CMP-001: Tombstone preservation in non-bottommost compaction

A tombstone entry MUST NOT be dropped during compaction unless the compaction is bottommost
(no lower levels contain data for this key range). Dropping a non-bottommost tombstone resurrects
the entry it shadows in a lower level.

**Audit**: Find all code paths that skip or drop tombstone entries during compaction output.
For each, verify it is gated on a bottommost check. Cross-reference with RocksDB's
`BottommostLevelCompaction` logic. Note: the `drop_expired` flag controls TTL cleanup (bottommost only),
but tombstone cleanup for dead keys may be a separate code path — verify both.

### CMP-002: Version pruning respects the retained-version floor

`BranchCompactionPruningPolicy` MUST:
- Emit all rows with `commit_version ≥ retained_version_floor` (a version exactly at the floor is kept)
- Keep exactly ONE below-floor survivor per logical key (the newest below-floor value), dropping
  the rest as `OlderVersion`
- Keep below-floor tombstones under `DropOlderVersions` (tombstone elision is CMP-001/CMP-007's
  separate, bottommost-gated policy)
- Respect the `max_versions` cap
- Combine the version floor with the timestamp floor (`row_is_below_floors`)

When the floor is 0, all versions pass through unchanged. Every pruning run is additionally
gated by the full safety proof (ARCH-005).

**Audit**: Find `BranchCompactionPruningPolicy` and `decide_below_floor_value`
(`branch/pruning.rs`). Verify the rules above, including floor==0 pass-through, exactly-at-floor
kept, only-tombstone-below-floor kept, and the `row_is_below_floors` version+timestamp combine.

### CMP-003: Grandparent overlap control

When compacting L_n → L_{n+1}, output table splitting MUST respect the per-request
`max_overlap_bytes` threshold to prevent pathological write amplification during the *next*
compaction (L_{n+1} → L_{n+2}). The cut predicate tracks cumulative overlap with
grandparent-level (output_level+1) boundaries and forces a cut when exceeded — but never
mid-key (CMP-008) and never on an empty output.

**Audit**: Find `GrandparentCutTracker::should_cut_before` (`table/compaction.rs`) and its
wiring via `grandparent_cut_hints` at output_level+1 (`branch/state/compaction.rs`). Verify
the accumulation loop handles the output key jumping past multiple grandparent boundaries
at once.

### CMP-004: Compaction publish-before-reclaim ordering

The per-branch table manifest MUST be durably published BEFORE any superseded table object
becomes reclaimable. V1's shape: install the new layout and record objects → publish the
table manifest (crash-safe temp + fsync + rename) → only then enqueue the retention mark;
physical deletion happens in the deferred Quarantine→Purge sweep, never inline with the
install. The sweep cannot delete objects referenced by the last durably-confirmed manifest
or by any in-flight publication (ARCH-009's frontier pin). A crash at any point therefore
leaves recovery a manifest whose every listed object exists.

**Audit**: Trace `install_prepared_durable_compaction` →
`publish_compaction_outcome_manifest` (`lifecycle/rewrite_publication.rs`) → the GC mark
enqueued after publish (`lifecycle/durable/maintenance.rs`). Verify no deletion is reachable
before the manifest publish. The recovery direction is pinned by
`strict_recovery_rejects_missing_manifest_listed_table_object`.

### CMP-005: Dynamic level sizing correctness

The dynamic level target computation (based on RocksDB's `CalculateBaseBytes`) MUST:
1. Find the largest non-empty non-L0 level
2. Compute base = bottom_bytes / multiplier^(bottom_level - 1)
3. Clamp base between MIN_BASE_BYTES and MAX_BASE_BYTES
4. If unclamped base < MIN_BASE_BYTES, raise base_level until base ≥ MIN_BASE_BYTES;
   levels below base_level get MAX_BASE_BYTES as a passive lower-bound clamp
5. Forward-compute targets from base_level with saturation

Targets MUST be refreshed after every compaction and flush.

**Audit**: Find the level target computation function. Step through the algorithm with concrete numbers.
Verify it handles: empty database (all levels empty), single-level spike (L3 has 10GB, others empty),
very small databases (< MIN_BASE_BYTES total), data concentrated in deep levels (tiny unclamped base).
Verify refresh is called after every segment version swap.

### CMP-006: Concurrent compaction and flush safety

A flush may install a new L0 table while a background compaction over older L0 tables is in
flight. The compaction install snapshots the current levels at install time and removes ONLY
its candidate input tables, matched by CONTENT IDENTITY (`table_matches_ref`: identity +
branch + level + kind), never by pointer equality — so a concurrently-flushed table is
preserved, L0 index positions are re-based by identity, and a stale candidate set is refused
("identity is stale"). Tables MUST NOT be lost or duplicated across the swap. Installs are
serialized under the runtime mutex.

**Audit**: Find `remove_compacted_tables` and `require_candidate_current`
(`branch/state/compaction.rs`). Verify identity-based filtering and the stale-candidate
refusal. `branch_compaction_l0_to_l1_prepared_plan_publishes_around_concurrent_flush` pins
the concurrent-flush survival.

### CMP-007: Tombstone elision refuses resurrection risk

Even at bottommost, tombstone elision MUST be refused when dropping a below-floor tombstone
would resurrect a strictly-lower-commit surviving live row of the same key. The bottommost
gate (CMP-001) alone does not capture this: a below-floor live survivor kept by CMP-002's
one-survivor rule can sit UNDER the tombstone being elided in the same output.

**Audit**: Find `candidate_has_tombstone_resurrection_risk` inside
`validate_policy_specific_safety` (`branch/pruning.rs`); verify `DropTombstones` fails closed
with `TombstoneResurrectionRisk` when the risk holds, and that the check runs against the
actual candidate row set, not level shape alone.

### CMP-008: All versions of a physical key land in one output table

Neither the size-split predicate nor the grandparent-cut predicate may cut a compaction
output mid-key: every version of a physical key in the input set lands in the same output
table. A mid-key cut would let table-level bounds separate versions that MVCC candidate
selection assumes co-located.

**Audit**: Verify both `should_split_before` and `GrandparentCutTracker::should_cut_before`
(`table/compaction.rs`) refuse to cut while the next row shares the pending output's last
physical key.

---

## COW — Copy-on-Write Branching Invariants

### COW-001: Shared table deletion requires proven unreachability

A table object referenced by ANY branch — through an owned level, an inherited layer, or an
in-memory pin — MUST NOT be deleted. V1 decides deletability by a stateless reachability
proof, not runtime refcounts: each retention pass unions every branch's durable manifest
references (`live_table_objects`) with every branch's in-memory pins
(`in_memory_pinned_table_objects` iterates ALL branches), and only objects in neither set
become quarantine candidates. There is no "untracked = free to delete" class — an object is
either proven unreachable or it is retained. (The pre-V1 `SegmentRefRegistry`/refcount model
is gone; its names are a forbidden-string guard in the retention tests.)

**Audit**: Find `LifecycleTableObjectRetentionOutcome::new`
(`lifecycle/table_reachability.rs`) and the all-branch pin collection
(`lifecycle/durable/maintenance.rs`). Verify the union covers owned + inherited + pinned
across every branch including deleted-but-surviving descriptors, and that the only physical
delete path is the quarantine sweep over proven-unreachable objects.

### COW-002: Fork capture is atomic against source maintenance

A fork MUST capture the source branch's table set with no window in which concurrent
maintenance on the source could reclaim a table between capture and the child's reference
becoming visible. V1 enforces this by serialization, not refcount ordering: the whole fork
(capture + child attach) runs under the runtime slot lock, and the next retention pass
already sees the child's references (COW-001's all-branch union), so no gap exists in which
the child's inherited tables are unreachable. (The pre-V1 DashMap-guard/refcount race this
entry originally described cannot occur — there are no per-branch guards and no refcounts.)

**Audit**: Find `fork_branch_at_version` (`api/runtime/mod.rs`) and verify the slot lock is
held across capture and attach. Verify the retention pass's reachability union (COW-001)
includes the new child's inherited layers on the first pass after the fork.

### COW-003: Inherited layer version gate

When reading through an inherited layer, the effective version ceiling is `min(max_version, fork_version)`.
Entries with `commit_id > fork_version` MUST be invisible to the child branch. This ensures branch
isolation — parent writes after the fork are invisible to the child.

**Audit**: Find `BranchEffectiveReadBound::for_inherited_layer` and `row_version_in_bound`
(`branch/read.rs`). Verify every inherited-layer access site (point, history, summary, all
scan variants) applies the bound. Tests:
`forked_branch_isolated_from_parent_post_fork_commits`,
`forked_branch_at_timestamp_before_fork_returns_parent_row`.

### COW-004: Materialization preserves commit_ids

When inherited entries are materialized into the child's own segments, the original `commit_id`
MUST be preserved, not reassigned. Reassigning would break: (a) MVCC reads at the fork_version
(the entries would be invisible), (b) merge base computation (ancestor state would be lost).

**Audit**: Find `rewrite_row_branch` (`branch/identity.rs`, called from materialization).
Verify the rewrite swaps only the structured key's branch identity and preserves commit
version and commit timestamp on both the put and tombstone arms.

### COW-005: Recovery loads all branch state before fork children re-materialize

All branch descriptors and all per-branch table manifests MUST be recovered before any
layer-less fork child re-materializes from its source (`rebuild_fork_snapshot_rows`); a
multi-pass loop handles fork chains. Layered children recover their inherited layers from
their OWN manifest, never from a directory scan of the source. A Deleted source is skipped
by the rebuild — sound ONLY because DUR-008 refuses deleting a source while a layer-less
child depends on it (this is a hard invariant pair, not a warning-with-data-loss).

**Audit**: Find `recover_per_branch_table_manifests` and the rebuild loop
(`lifecycle/durable/bootstrap.rs`). Verify manifest recovery completes before any rebuild,
the loop converges for fork chains, and the Deleted-source skip is paired with DUR-008's
delete refusal.

### COW-006: Post-recovery reachability is recomputed, never trusted

Table reachability after a crash MUST be recomputed statelessly from durable state — every
branch's recovered manifest plus live pins — never restored from a persisted counter. There
is no durable refcount to rebuild or trust: the first retention pass after recovery derives
the full reachable set from scratch (COW-001's union), so a crash cannot leave a stale count
that either leaks objects forever or frees a reachable one.

**Audit**: Verify no persisted reachability/refcount state exists (the retention tests'
forbidden-string guard covers the old names); verify the retention pass derives its set
purely from recovered manifests + in-memory pins each time it runs
(`lifecycle/table_reachability.rs`).

### COW-007: WAL replay is generation-fenced by `created_at`

WAL records carry no branch generation, so a record for a branch id whose name was deleted
and re-created is indistinguishable from the current generation's by id alone. Replay MUST
skip records at `commit_version <= descriptor.created_at` (the dead predecessor generation's
band) while still counting the skipped record's version into the recovered clock
(`replayed_max`) — the version was really allocated. Branch creation stamps `created_at`
with the globally visible version at creation, making the fence exact: every predecessor
record is `<= created_at`, every own record is `> created_at`.

**Audit**: Find `record_predates_current_generation` and its replay call site
(`lifecycle/durable/bootstrap.rs`); verify fenced records still fold into `replayed_max`.
Truth table: `generation_fence_truth_table` (`lifecycle/tests/recovery.rs`). Origin:
#2826/#2832. The checkpoint-row twin of this fence lives in DUR-010.

### COW-008: Durable base restore is generation-fenced

A per-branch table manifest surviving from a DEAD generation of a re-created parentless
branch (its max commit version `<= created_at`) MUST be skipped at recovery — restoring it
would resurrect the deleted generation's base state under the new name.

**Audit**: Find the fence inside `recover_per_branch_table_manifests`
(`lifecycle/durable/bootstrap.rs`). Truth table: `base_restore_generation_fence_truth_table`
(`lifecycle/tests/recovery.rs`). Origin: #2830/#2834.

### COW-009: Fork layer structure and flattening precedence

Inherited-layer attach MUST validate structure: layers nearest-first (fork_version
descending), unique source ids, none self-referential, none Unavailable
(`validate_inherited_attach`). Fork snapshot flattening MUST give own rows precedence over
inherited rows on key collision, and cap each inherited layer's contribution at
`min(watermark, layer.fork_version)` (`fork_snapshot_rows`).

**Audit**: Verify both functions (`branch/state/fork.rs`, `branch/state/snapshot.rs`)
enforce the stated rules; the validation tests live in
`branch/tests/inheritance_materialization/`.

---

## MVCC — Multi-Version Concurrency Control Invariants

### MVCC-001: Version visibility boundary

A reader at snapshot version V MUST NEVER see an entry with `commit_id > V`. This holds across
active memtable, frozen memtables, all segment levels, and inherited COW layers. This is the
foundational guarantee for snapshot isolation.

**Audit**: Find every point-lookup and scan code path. Verify each filters by `max_version` / `commit_id`.
Check that no fallthrough path bypasses the filter. Check inherited layer reads apply the
`min(max_version, fork_version)` ceiling.

### MVCC-002: Tombstone semantics

A tombstone at `commit_version = V` means "key deleted at version V." For readers at
`max_version ≥ V`, the key MUST return "not found" — the tombstone shadows all older
versions. For readers at `max_version < V`, the key exists at its previous version. Point
lookups MUST treat tombstones as "not found" (`candidate_into_visible_row` returns None);
history queries MUST include tombstones; scans filter them, with explicit
`scan_*_including_tombstones` variants for callers that need them
(`read_point_or_tombstone` is the tombstone-visible point verb).

**Audit**: In `branch/read.rs`: verify `candidate_into_visible_row` maps a tombstone
candidate to None; verify history includes tombstones; verify the plain scan variants filter
while the `_including_tombstones` variants pass them through.

### MVCC-003: Global version counter monotonicity

The commit-version allocator (`CommitVersionAllocator`, lock-serialized `allocate_next`,
overflow-checked via `checked_next` → `VersionAllocatorOverflow`) MUST hand out unique,
strictly increasing values. No two commits may receive the same version. Version gaps are
acceptable (failed commits burn their allocation — see the version-gap contract in the
commit-runtime scaffold).

Monotonicity is WITHIN a durable lineage: after a crash that legally sheds unsynced
acks (Standard mode), recovery resumes the counter above every DURABLE reference and
may re-issue version numbers the shed acks once carried — by design, since the shed
versions left no durable trace. The reopen publishes `recovered_visible_version`
(the open outcome); any consumer holding pre-crash version handles above it must
rebase on that anchor (#2859 family B: the whole-DB DST's model adopted a
state-matched watermark above the recovered domain and its stale acks collided with
re-issued versions — a phantom `LostAck`; the harness now truncates its model at the
reopen's recovered visible version under lossy families).

**Audit**: Find `CommitVersionAllocator::allocate_next` (`commit/allocator.rs`) and its
overflow guard. Verify recovery restores the counter
to at least the maximum of: the checkpoint watermark, the replayed WAL max (fenced records
included), the restored branch states' max committed version, AND the branch catalog's version
anchors (`descriptor_version_anchor`: `created_at`, fork anchors, deletion watermarks — deleted
descriptors included). The catalog term is load-bearing: lifecycle publishes are durably fenced,
so the catalog can survive a crash that sheds the WAL and every state, and a counter restarted
below its anchors re-issues versions the catalog already attributes to other content (#2850 —
generation fences then eat legitimate commits and fork rebuilds materialize the wrong parent
slice). Re-issue contract: `reopen_version_domain_bound` + the model truncation in
`reconcile_after_reopen` (`testkit/simulation/whole_db.rs`);
`version_domain_truncation_prevents_reissue_poisoning`
(recovery_oracle/verify.rs) pins both directions,
`reopen_version_domain_bound_applies_only_to_lossy_reopens` (simulation/mod.rs) pins
the family gate.

### MVCC-004: Reads snapshot only fully-published state

A read view MUST reflect only commits that are fully applied and published: rows land first,
the visible frontier advances second (monotonic `VisibleVersionTracker::publish_visible`),
and off-lock readers load the frontier (Acquire) BEFORE reading structure — so a reader can
never capture a version whose writes are still in flight. (V1 has no transaction
`start_version`; the per-operation read view plays that role. The exhaustive interleaving
coverage of this ordering is DUR-001/002/003's loom lane — this entry states the MVCC-facing
consequence.)

**Audit**: Verify `publish_visible` (`commit/visibility.rs`) is monotonic and called only
after apply; verify the read path loads the visible frontier before structure
(`branch/read.rs`, the V-before-S order).

### MVCC-005: No read-pinning gap for pruning

Version pruning MUST never drop a row a live read view could still serve. The pruning proof's
`pinned_view_floor` MUST be ≥ `retained_version_floor`, and `retained_version_floor` MUST be
≤ the visible version — enforced as hard proof-validation rules, so a pruning run with any
outstanding older pinned view fails closed rather than pruning under it. (The pre-V1
`gc_safe_version`/active-transaction-drain model is gone; pinned read views are the V1
pinning mechanism, and ARCH-005 holds the full proof-gate law.)

**Audit**: Find `BranchCompactionPruningProof::validate_static` (`branch/pruning.rs`) and
verify both inequalities are rejected on violation. Test:
`row_pruning_proof_pinned_view_below_floor_rejects` (`branch/tests/row_pruning.rs`).

### MVCC-006: TTL expiration does not resurrect old versions

When the newest visible version of a key has expired (TTL elapsed), the read path MUST return
"not found" — it MUST NOT fall through to an older, non-expired version. The expired version
is the authoritative state. (Design decision: expired = gone, not expired = reveal older.)

**Audit**: Find the point lookup path. Verify that when the selected newest candidate is
expired (`row_is_expired_at` via `candidate_into_visible_row`), the function returns `None`
without falling through to older versions. Nuance: this layer evaluates TTL only for
timestamped (as-of) reads; latest-verb TTL is enforced above it.

### MVCC-007: Commit timestamps are monotonically floored

Commit timestamps MUST never regress: generated timestamps clamp UP to the monotonic floor,
an explicit timestamp below the floor is rejected, and recovery catches the floor up to the
recovered maximum. Timeline ordering (DUR-013's derived index) and as-of resolution depend
on this floor.

**Audit**: Find `CommitTimestampGuard` (`commit/allocator.rs`); verify the clamp-up,
below-floor rejection, and recovery catch-up arms. Tests: `commit_timestamp_guard_*`
(`commit/tests/allocator.rs`).

### MVCC-008: The recovered clock resumes above every catalog anchor

After ANY recovery, the version allocator MUST resume strictly above the maximum of: the
checkpoint watermark, the replayed WAL maximum (generation-fenced records included), every
restored branch state's max committed version, AND every catalog descriptor's version
anchors — `created_at`, fork anchors, deletion watermarks — including DELETED descriptors
(`descriptor_version_anchor` over `list_branches(true)`). The catalog term is load-bearing:
lifecycle publishes are durably fenced, so the catalog can survive a crash that sheds the
WAL and every branch state; a counter restarted below its anchors re-issues versions the
catalog already attributes to other content, generation fences then eat legitimate commits,
and fork rebuilds materialize the wrong parent slice (#2850). Promoted from MVCC-003's
recovery prose because it is regressible independently of allocator monotonicity.

**Audit**: Find `catalog_anchor_max` in the recovered-clock computation
(`lifecycle/durable/bootstrap.rs`). The single regression to check for: a
`list_branches(false)` (excluding deleted) at this site. MVCC-003's re-issue contract tests
cover the lossy-domain interplay.

---

## ACID — Atomicity, Consistency, Isolation, Durability Invariants

### ACID-001: Single WAL record per transaction

Each commit MUST produce exactly ONE `WalRecord` containing its complete stamped writeset.
This ensures atomicity of recovery — a commit is either fully replayed or not at all.
Partial (torn) final records are detected at decode and repaired as provably-unacked
(DUR-005 bounds when a repair is legal). Since W3.1c the payload is the user rows only —
the commit stamp is durable on the record itself, not as materialized timeline rows
(DUR-013).

**Audit**: Find `prepare_commit_rows` (`commit/cache.rs`) and `build_wal_record`
(`commit/durable.rs`). Verify all mutations compose into one `WalCommitPayload` → one
`WalRecord`, appended once; verify no path writes multiple records per commit. Tests:
`single_record` (testkit durable contract), `single_record_service`
(`service/wal/tests/corruption.rs`).

### ACID-002: WAL before storage; post-WAL failure fail-closes through the gate

The durable commit protocol MUST order: validate → admit → conflict-check → allocate → WAL
append → storage apply → visible publish. A WAL-append failure returns before any apply.
A storage-apply or publish failure AFTER a durable WAL append is NOT reported as success
and NOT swallowed: it records a `CommitUnresolvedDurable` gate fact and returns a
durability-uncertain error; the global unresolved-durable gate then blocks cross-branch
visible-version advance past the in-flight version until replay reconciles and clears
exactly the matching fact. BS5 write groups preserve the same law with caller-sequenced
durability: `append_deferring_durability` accumulates like Standard, and one covering
`force_durable` must precede any covered ack or visible publish.

**Audit**: Trace `commit/durable.rs`: WAL-failure early return; the durable-but-not-visible
arm recording the gate fact and erroring; the gate check at admission. Verify replay clears
only the exact matching fact (`commit/replay.rs`). For groups: verify the covering-sync
ordering (`api/runtime/commit_group.rs`, DUR-003/DUR-004 hold the adjacent ordering law).

### ACID-003: Per-branch commit guard prevents TOCTOU

Every mutating commit MUST hold the per-branch commit guard from admission through apply and
publish. The guard is a nonblocking fail-fast token (`CommitBranchGuard`, RAII), not a
blocking lock: same-branch contention is a documented fail-fast; retry policy belongs above
the commit runtime. Without the guard, a concurrent commit could mutate the branch between
conflict validation and apply, acting on a stale validation.

**Audit**: Find `admit_mutating_commit` (`commit/branch_registry.rs`) and verify it is
unconditional for every mutating batch, and the RAII `_admission_guard` lives through apply +
publish in both `commit/durable.rs` and `commit/cache.rs`.

### ACID-004: Blind writes are guarded commits that skip only conflict sources

There is NO blind-write fast path in V1: writes without read facts take the same admission,
per-branch guard, allocation, WAL, apply, and publish path as every mutating commit. What a
blind write legitimately skips is conflict-SOURCE capture — with no read/CAS facts there is
nothing to validate against, so the (potentially expensive) conflict source is not built.
Safety needs no special argument: the guard serializes same-branch commits, and MVCC-004
keeps unpublished versions invisible.

**Audit**: Verify `commit_conflict_validation_needs_source` gates only source construction
(`commit/durable.rs`) and that no code path bypasses `admit_mutating_commit` for an
empty-read-set batch. Grep for any resurrected "blind" fast path — its existence would be
the regression.

### ACID-005: Recovery replay is idempotent

WAL replay MUST be idempotent — replaying the same record twice produces the same state as
replaying it once (a crash during recovery can cause re-replay). V1 enforces this by
EXPLICIT duplicate classification, not silent overwrite: `classify_replay_rows` inspects the
target state and maps each record to Absent → apply, byte-identical Exact → AlreadyApplied
(skipped and counted), value Mismatch / Partial overlap → fail closed. Divergent bytes at a
replayed internal key are corruption, never resolved by overwrite.

**Audit**: Find `classify_replay_rows` / `ReplayDuplicateState` (`commit/replay.rs`). Verify
all four arms and that the fail-closed arms are reachable from the recovery path. Tests:
`commit/tests/replay.rs`.

### ACID-006: Always mode is fsync-before-visible

In `Always` mode, the WAL bytes covering a commit MUST be fsynced before the commit's
visibility publishes and before its ack returns; a crash after the ack MUST NOT lose the
commit. Solo path: `force_durable()` runs inline (flush the pending append buffer, THEN
sync — flush-before-fsync is load-bearing), and `require_append_satisfies_policy` rejects an
unforced Always append. Group path: the leader's ONE covering fsync precedes the phase-2
publish for every member (DUR-003/DUR-004 hold the surrounding ordering).

**Audit**: Verify `force_durable` orders flush-then-sync (`service/wal.rs`); verify the
unforced-Always rejection (`commit/durable.rs`); verify the group's covering-sync-before-
publish (`lifecycle/durable/bootstrap.rs`, `api/runtime/commit_group.rs`). Contract tests:
`check_always_success`, `check_unforced_always_rejection` (testkit durable contract).

### ACID-007: Standard mode is deferred durability — shed-on-crash is the contract

`Standard` mode promises NO time-bounded fsync window. Commits publish visibility with no
covering fsync; unsynced acks are LEGALLY shed by a crash. Durability advances only at
explicit events — segment rotation seals, checkpoints, and close all `force_durable` — and
the sync-attested watermark (DUR-005) records exactly what is promised, so recovery restores
a prefix of acknowledged history and never fabricates beyond it. The 500ms append-buffer
trickle-flush is a WRITE (bounding abrupt-kill exposure of buffered bytes), not an fsync,
and promises nothing. There is no interval/batch fsync configuration and no background sync
thread; `DurabilityPolicy` is bare `Standard | Always`.

**Audit**: Verify the Standard group finish captures no sync ticket and publishes without a
covering fsync (`lifecycle/durable/bootstrap.rs`); verify durability events at rotation,
checkpoint, and close (`service/wal.rs`); verify no time-based fsync mechanism exists. The
shed-prefix direction is pinned by the process-crash testkit
(`sigkilled_child_recovers_a_prefix_of_acknowledged_history`).

---

## ARCH — Cross-Cutting Architectural Invariants

### ARCH-001: One version domain for GC

All data-bearing entries (KV pairs, JSON documents, event-log events, vector records, graph
elements, and branch metadata) are stored as branch-aware KV rows stamped with ONE
commit_version domain. Retention floors and pruning operate in that single domain and
protect ALL rows regardless of primitive type.

Event sequence numbers (`EventSequence(u64)`) are payload metadata stored within the row,
not an independent version axis. Pruning by commit_version therefore correctly governs every
primitive.

**Audit**: Verify every engine primitive persists through one `CommitPlan` → one
commit_version (engine `persistence/adapter.rs`, the per-primitive services). Verify
`EventSequence` lives in the key/value payload (engine `data/event/types.rs`), never as a
version axis. Verify pruning floors are commit-version-domain only (`branch/pruning.rs`).

### ARCH-002: One atomic publication boundary

All writes in a transaction share a single commit_version. A reader at `max_version < commit_version`
sees NONE of them. A reader at `max_version ≥ commit_version` sees ALL of them. There is no
intermediate state where some writes are visible and others are not.

The vector index (V1's only secondary index) is updated AFTER storage application, in a
separate commit off the write path — eventually consistent by design; a query through the
index may temporarily miss newly-committed entries. KV rows are always authoritative.

**Audit**: Verify all rows in a commit share one stamp (`prepare_commit_rows`,
`commit/cache.rs`). Verify no reader can observe version V while its apply is in flight
(the guard + MVCC-004's publish ordering). Verify the vector index update is a separate
post-apply commit (engine `data/vector/service.rs`).

### ARCH-003: KV rows are the single source of truth

All persistent state lives in branch-aware KV rows. Derived state (vector index artifacts —
V1's only secondary index) can be fully rebuilt from the rows; losing it MUST NOT lose data —
queries degrade to exact KV scans until rebuild. Source rows are authoritative; derived
state may accelerate retrieval, never replace it (engine charter rule 26).

**Audit**: Verify vector artifacts rebuild fully from KV (engine `data/vector/artifact.rs`)
and index loss degrades to the exact scan path (`data/vector/service.rs`). KNOWN GAP: the
production recovery path does not auto-rebuild vector artifacts (the seal/build primitives
are testkit-wired); data-safety holds via the scan fallback — verify that fallback is
reachable when artifacts are absent.

### ARCH-004: One recovery model with deterministic ordering

Recovery follows one fixed sequence: manifest/assembly → checkpoint install → quarantine
inventory → per-branch table-manifest recovery → flush-watermark validation →
orphaned-delta detection → WAL replay → checkpoint/manifest COMBINE → timeline re-seed →
fork-child re-materialization → recovered-clock restoration. Each step's output MUST NOT
depend on a later step's output. Replay MUST be deterministic (same records → same state)
and idempotent (ACID-005). There is one version domain — `WalRecord` is keyed by
commit_version; no separate txn-id axis exists to diverge.

**Audit**: Trace `LifecycleRecoveryRuntime::recover` (`lifecycle/recovery.rs`) and
`complete_recovery` (`lifecycle/durable/bootstrap.rs`). Verify the step order and the
absence of forward dependencies. The DST whole-DB simulation is the volume lane for
determinism/idempotence.

### ARCH-005: Pruning proceeds only under a complete, fresh safety proof

Below-floor row deletion MUST be gated by a `BranchCompactionPruningProof` that jointly
attests, in ONE validation: `retained_version_floor ≤ visible_version`,
`pinned_view_floor ≥ retained_version_floor` (MVCC-005), no readable inherited layers over
the candidates, candidate tables not shared with any other branch, table-manifest coverage
≥ the floor, healthy recovery state — and a branch-state fingerprint binding the proof to
the actual current contents (a stale proof is refused). This proof-gate model replaces the
pre-V1 global `gc_safe_point`; COW children remain safe because shared tables are excluded
by the not-shared gate and retained by COW-001's reachability.

**Audit**: Find `BranchCompactionPruningProof::{validate_static, validate_for_branch}`
(`branch/pruning.rs`) and the call site before policy execution
(`branch/state/compaction.rs`). Verify a proof missing ANY gate is rejected and the
fingerprint freshness check is load-bearing.

### ARCH-006: RETIRED (2026-08-19) — transaction timeout

Retired: its subject was removed. V1 has no public manual transactions, no long-lived
transaction handles, and no `gc_safe_version` to starve — reads capture short-lived
per-operation read views, and pruning safety is the proof-gate law (ARCH-005/MVCC-005).
The ID is preserved per the no-renumber rule. If long-lived read pins ever return (e.g. a
public snapshot surface), resurrect this entry as a pin-lifetime bound.

### ARCH-007: Durable authorities are enumerated, with declared coupling points

V1's durable authorities are: the `DatabaseManifest` (identity, codec, active WAL segment,
snapshot watermark/id, `flushed_through_commit_id`), per-branch `TableManifest`s, the
`BranchCatalogManifest`, the `PendingReleasesManifest`, and the sync-attested
`meta/wal-watermark` singleton (deliberately OUTSIDE the manifest family — DUR-005). Each
governs its own domain; cross-domain reads are limited to DECLARED coupling points, of which
the load-bearing one is `flushed_through_commit_id`: recovery reads it to detect an orphaned
delta and to run the checkpoint/manifest COMBINE (DUR-010). Undeclared cross-authority reads
are the regression this entry guards against. The `DatabaseManifest` is additionally the
FIRST durable publish of a new store (only lock and temporary objects legally precede it),
and its replace is atomic — so a store holding any WAL/meta/table/snapshot/quarantine object
without a manifest has LOST its authority and MUST refuse to open rather than fabricate a
fresh one (#3015: fabrication silently reset the snapshot-id floor and checkpoint lineage).

**Audit**: Enumerate the authorities (`format/manifest.rs`, `layout/mod.rs` for the
watermark). Verify recovery's cross-domain reads are exactly the declared set
(`lifecycle/recovery.rs` — orphaned-delta detection and the COMBINE); flag any new
durability-manifest field consulted for table-loading decisions or vice versa. For the
creation-ordering law, verify `load_or_create_manifest` (`lifecycle/durable.rs`) refuses on
`durable_residue_exists` and that no creation-path publish precedes `create_initial`.

### ARCH-008: Fork version creates an implicit retention floor for shared tables

When branch B inherits tables from A at fork version V, rows with `commit_version ≤ V` in
those tables MUST remain accessible to B for as long as B's layer references them. The chain:
tables are immutable (LSM-007); A's compaction creates NEW tables; the superseded shared
tables stay retained because B's inherited-layer references keep them reachable
(`SharedTableRegistry` runtime references + COW-001's manifest reachability + ARCH-005's
not-shared pruning gate); B reads the unchanged old tables. DUR-008 covers the adjacent
layer-less-child case where retention rides the SOURCE branch itself.

**Audit**: Verify the chain end to end: `is_runtime_referenced`/`is_reachable`
(`branch/facts.rs`), the reachability snapshot rebuild, and the `candidate_tables_not_shared`
pruning gate. A pruning or sweep path that consults none of these is the regression.

### ARCH-009: Manifest-frontier deletion pin

A table object is physically deletable ONLY when referenced by neither the last
durably-confirmed per-branch manifest nor any in-flight (pending) publication. Deletion is
always a post-publish Quarantine→Purge sweep — never inline with an install (CMP-004's
ordering law generalized to every object class). This is the object-store generalization
that makes crash-at-any-point recovery sound: whatever manifest recovery selects, its listed
objects exist.

**Audit**: Find `manifest_frontier_pinned_objects` (`lifecycle/table_manifest.rs`) and its
consumption in `reclaim_pinned_table_objects` (`lifecycle/durable/maintenance.rs`). Verify
both frontier legs (confirmed + pending) pin, and no deletion path bypasses the sweep.

### ARCH-010: The error-code registry is the single authority for an error's row

An error's public class, retry policy, commit outcome and suggested fix are the registry
row for its code — always, and never a per-site or per-class table beside the registry.
In engine, every `EngineError` constructor resolves its row through one lookup
(`diagnostics/registry.rs::registry_row`) inside one private builder, and the status
constructor is crate-private with that builder as its sole caller (#3280). In executor, the
row is resolved unconditionally at the boundary (`ExecutorError::new` → `resolve_row` →
`status_from_row` in `error.rs`), and a status arriving from a peer — an engine status, an
inference error, a status over IPC from another build — is re-resolved the same way, keeping
only the peer's message, ids, structured `details` and `hints` (#3244). A construction site
therefore owns exactly four things: the code, the message, `details` and site `hints`; a hint
that would restate the row's fix is omitted rather than duplicated (#3241). The regression
this guards against is the class that produced #3237 / #3241 / #3243: a private
`match` on a code or a class that returns a retry policy, a commit outcome or a remedy string,
which then drifts from the registry while `strata agents errors` keeps publishing the row.

**Audit**: (1) `EngineErrorStatus::new` has no caller outside `EngineError::build`
(`diagnostics/error.rs`). (2) No `match` on `EngineErrorClass` / `ErrorClass` / an error
code returns a `RetryPolicy`, a `CommitOutcomeStatus` or a `&'static str` remedy outside
`engine/diagnostics/registry.rs` and `executor/error_registry.rs`; the old names to grep for
are `default_retry_policy`, `default_commit_outcome`, `public_class_for_legacy` and
`suggested_fix_for_class` — all must stay absent. (3) The sweeps still cover every code and
every storage variant: `diagnostics::error::tests::test_constructed_errors_carry_the_registry_row`
and `persistence::adapter::tests::every_storage_error_maps_to_its_registry_row` in engine;
`test_executor_errors_carry_the_whole_registry_row` (`tests/error_contract.rs`),
`from_status_re_derives_the_registry_row_and_keeps_the_site_fields` (`tests/error_and_guards.rs`)
and `error::tests::engine_error_status_re_derives_the_row_and_keeps_the_site_fields` in
executor. A new peer path into the executor that does not go through `from_status` /
`engine_error_status` / `From<InferenceError>` is the regression.

---

## SCALE — Scale-Span Invariants (Pi Zero to Billion-Key Server)

### SCALE-001: One memory budget scales the engine; the default derives from the host

All memory sizing derives from ONE knob: `StorageMemoryBudget` (minimum 1 MiB) splits a
declared total into the seven runtime pools (`StorageRuntimeBudget`), and level sizing is
data-derived (`nonzero_level_targets_from_level_bytes` — no server-scale byte constants).
The budget is a CEILING, never a reservation: the block cache and memtables grow with use,
so an empty database occupies structs, not its budget. When no budget is set, the product
open path (`StorageBudgetPolicy::DerivedFromHost`) derives the default at open time from
the host: 25% of usable memory — the smaller of `MemAvailable` and the cgroup limit, so a
container limit wins over host RAM — clamped to `[1 MiB, 8 GiB]` (#2905: a 384 GiB host
must not derive 96 GiB; a 350 MiB device derives ~87 MiB and opens). A host that reports no
facts (macOS/Windows/wasm today) falls back to the fixed 512 MiB default; the open summary
records the provenance (`StorageBudgetSource`). `StorageBudgetPolicy::Default` stays the
fixed budget so test lanes remain deterministic; `STRATA_HOST_MEMORY_BYTES` overrides the
probe for CI/DST/perf lanes.

**Audit**: `derive_default_budget_bytes` + the probe (`host_memory.rs`) and
`resolve_budget_policy` (`api/runtime/open_close.rs`) — verify the 25%/min/clamp truth
table, the cgroup-wins-over-host case, the no-facts fallback, and that engine's
`apply_memory_budget` selects `DerivedFromHost` only when no explicit budget is given. Verify
no allocation reserves against the ceiling (`TableBlockCache::new` starts empty;
`StorageBudgetLedger` usage is 0 at open).

### SCALE-002: The budget splits into validated pools; durable totals gauge, cache totals cap

The declared total splits into seven pools whose sum MUST validate ≤ total. Enforcement
differs BY MODE, by design: cache mode fail-closes on the total
(`resource_exhausted.storage_api.memory_budget`); durable mode treats the projected total as
a soft observability gauge — `require_projected_mutating_commit_budget` admits over budget
and warns (BS4.5a: durable progress is never wedged by a gauge). Individual pools still
bound their consumers (reader metadata under `table_reader_bytes`, memtables under the
mutable pools).

**Audit**: Verify the pool-sum validation (`lifecycle/budget.rs`), the cache hard-cap test
(`cache_memory_budget_cap_still_fails_closed`, `api/tests/cache.rs`), the durable soft-gauge
arm (`lifecycle/durable/bootstrap.rs`), and the differential soak
(`config_differential_soak_across_seeds`).

### SCALE-003: On-disk format is word-size portable

A database created on a 64-bit server MUST be openable on a 32-bit ARM device, and vice versa.
All on-disk formats (segment files, WAL records, manifests, snapshots) MUST use fixed-width integer
types (`u32`, `u64`), not `usize`. Endianness MUST be explicitly little-endian (`to_le_bytes`),
never native (`to_ne_bytes`).

**Audit**: Find every serialization/deserialization path in segment, WAL, manifest, and snapshot code.
Grep for `usize` in any `to_bytes` / `from_bytes` / serialization function. Grep for `to_ne_bytes`.
Verify all on-disk integers are explicit-width and explicit-endian.

### SCALE-004: AtomicU64 operations are bounded per user operation

On 32-bit ARM (Pi Zero 1 / ARMv6), `AtomicU64` is emulated via a global lock — every `fetch_add`,
`load`, and `store` on any `AtomicU64` contends on the same mutex. The number of `AtomicU64`
operations per read and per write MUST be bounded and small (ideally ≤ 5 per operation).

**Audit**: Trace one point read (`select_ordered_visible_point_candidate`) and one commit
(`commit/cache.rs` execute) through the engine. Count `AtomicU64` touches — the hot ones are
the visible frontier (`api/runtime/background.rs`), the timestamp floor
(`api/runtime/mod.rs`), and the budget gauge (`lifecycle/budget.rs`). No guard currently
bounds the count — flag any new per-row (rather than per-operation) atomic.

### SCALE-005: Write amplification is bounded and documented

LSM write amplification directly impacts SD card write endurance. The theoretical write amplification
for the default configuration MUST be documented. On flash storage, total write amplification
(user writes × LSM amplification × filesystem/FTL amplification) determines device lifetime.

**Audit**: Current shape: L0 trigger 4 (`LEVEL_ZERO_COMPACTION_THRESHOLD`), growth factor 10
(`NONZERO_LEVEL_TARGET_GROWTH_FACTOR`), 8 levels (`DEFAULT_MAX_LEVEL_COUNT`,
`branch/config.rs`). The live regression gate is `SCALED_COMPACTION_AMPLIFICATION_GATE = 4`
(`api/tests/mod.rs`) — verify it still gates. RECORDED GAP (#2906): the endurance derivation
document this entry requires does not exist yet.

### SCALE-006: Maintenance pressure control is deferral + throttle + lanes, not a bandwidth limiter

Compaction/user contention is controlled by three mechanisms, none a byte-rate limiter:
(1) budget-pressure deferral of OPTIONAL maintenance (`defers_optional_maintenance` gates
the optional compaction arms; required admission maintenance never defers — SCALE-011);
(2) graded write admission (`LifecycleWriteThrottlePolicy` delays writers under pressure);
(3) lane caps — compaction lanes and subcompactions each reducible to 1
(`STRATA_COMPACTION_LANES`, `STRATA_SUBCOMPACTIONS`). There is no bandwidth limiter and no
full-pause knob; a "pause compaction" need is met by lane reduction plus deferral.

**Audit**: Verify the three mechanisms at their sites (`lifecycle/budget.rs`,
`lifecycle/config.rs` + the delay site in `api/runtime/mod.rs`,
`lifecycle/durable/maintenance.rs` for the lane env knobs).

### SCALE-007: Thread count is bounded and configurable

On a single-core device, each background thread adds scheduling overhead and stack memory.
The engine's thread count MUST be bounded and configurable: background workers via
`with_background_worker_count` (default 4; durable minimum 1; cache mode runs 0),
subcompactions as bounded ephemeral `thread::scope` teams (`STRATA_SUBCOMPACTIONS`,
default 1). A constrained device runs durable with 1 worker.

**Audit**: Find the worker spawn loop (`lifecycle/background.rs`) and the option surface
(`api/options.rs` — including the durable ≥1 rejection). Grep for any unbounded
`thread::spawn` outside the worker pool and subcompaction scopes.

### SCALE-008: Billion-key table metadata is budget-bounded, disk-resident by default

At billion-key scale with thousands of table objects, reader metadata MUST stay within its
OWN budget pool — `table_reader_bytes` (default 32 MiB) — with table content disk-resident
and block-cached (the BS4.4 disk-resident flip), not pinned in memory. There is no "pinned
10% of block cache" tier; exceeding the reader pool evicts readers rather than failing or
growing unboundedly. Note "pinned" elsewhere in this catalog means GC reachability, not
memory.

**Audit**: Verify the reader pool bound (`DEFAULT_TABLE_READER_BYTES`,
`lifecycle/budget.rs`) and reader eviction under pressure (`service/table.rs`). For a
1B-key/100GB dataset at `DEFAULT_COMPACTION_TARGET_OUTPUT_BYTES` (64 MiB), sanity-check
open-reader metadata against the pool.

### SCALE-009: Merge cursor scales to high source counts

The compaction/read merge cursor MUST not degrade linearly at high source counts: V1's
`MergeTableCursor` switches from linear scan to a `BinaryHeap` at `MERGE_HEAP_THRESHOLD`
(4 sources). The requirement this entry once flagged as a cliff is satisfied — the audit's
job is to keep it satisfied.

**Audit**: Verify the threshold switch (`table/cursor.rs`) and the heap-path test
(`heap_merge_covers_sixteen_sources_with_shared_key_order`, `table/tests/cursor.rs`).

### SCALE-010: Feature degradation is explicit, not OOM (engine/intelligence scope)

Memory-heavy optional features (vector index artifacts, autoembedding model loading) MUST be
absent-by-default and degrade explicitly: vector search without an index falls back to the
exact KV scan (ARCH-003); a missing embedding provider surfaces a typed error
(`inference.missing_api_key` class), never a crash; core KV/JSON/event/graph primitives
never depend on any of them. NOTE: this entry's subject lives in the engine and intelligence
crates — it is the one entry in this catalog audited OUTSIDE crates/storage.

**Audit**: In crates/engine: verify the vector scan fallback (`data/vector/service.rs`) and
that no core primitive imports the vector/intelligence surfaces. In crates/intelligence:
verify the missing-provider typed-error path.

### SCALE-011: Optional maintenance defers under pressure; required maintenance never does

Budget pressure MUST defer only OPTIONAL maintenance (shape-improving compaction); REQUIRED
admission maintenance (whatever unblocks writes or durability) runs regardless — deferring
it would wedge the engine exactly when pressure is highest. The split is the
`defers_optional_maintenance` predicate consulted at the optional arms only.

**Audit**: Verify `defers_optional_maintenance` (`lifecycle/budget.rs`) gates the optional
compaction sites (`lifecycle/compaction.rs`, `lifecycle/durable/maintenance.rs`) and that no
required-admission arm consults it. DUR-009's registry law governs the predicate's
authority.

---

## DUR — V1 Durability, Publish-Ordering, and Scheduling Invariants

Contracts established by the V1 test-coverage program (2026-07). Each is pinned by a live
guard — the Audit instruction names it; a verdict must confirm the guard still exists and
still fails on the inverted contract.

### DUR-001: Off-lock reads bound by V-before-S

The off-lock read protocol loads the visible frontier `V` (Acquire) BEFORE the published
snapshot `S`; a reader observing `V = v` must see every structural change published under
the lock before `v`. Writers publish structure first, frontier second.

**Audit**: `load_published_snapshot` (api/runtime/mod.rs) documents and implements the
ordering; the loom lane (`crates/storage/src/branch/visibility_loom.rs`) explores it
exhaustively on the real structures. Verify the loom job is green and the sabotage twin
(`loom_frontier_published_before_apply_is_caught`) still panics as expected.

### DUR-002: Structural republish precedes the visible-mirror advance

Every structural change on the commit path (commit-triggered rotation; flush and compaction
installs on the maintenance path) republishes the branch snapshot BEFORE the visible
mirror's Release store. A reader at the new frontier must always find a covering snapshot.

**Audit**: every commit path (solo, eager member, deferred wrapper, parallel check-in) calls
`republish_branch_snapshot_after_rotation` before `mirror_visible_and_evaluate_wal_growth`.
The loom twin `loom_rotation_without_republish_is_caught` pins the failure shape.

### DUR-003: Group applies complete before visibility publishes

BS5.4c parallel branch applies are collected by the exchange barrier under the leader's
lock hold; the group's visibility publish happens only after every apply outcome (or the
group goes fatal). No schedule may show a reader a torn batch.

**Audit**: `run_group_applies_parallel` barriers before `publish_commit_group`;
`loom_visibility_publish_after_barrier_forbids_torn_reads` and the 4.3a exchange model
explore it. Verify the loom job covers both models.

### DUR-004: No dependent publish over unsynced WAL bytes

A manifest, snapshot, or table publish must not become durably visible while a WAL segment
holds appended-but-unsynced bytes it depends on. Pending-vs-confirmed semantics: a flagged
publish is exonerated only by a later covering durability event; discarded raced bytes
confirm the violation.

**Audit**: `WriteOrderingWatchdog` (testkit) is the tripwire; `tests/write_ordering.rs`
keeps the strict barrier-free lane; the DST fault/crash lanes run the watchdog stacked over
every trajectory and fail on any CONFIRMED violation.

### DUR-005: The commit-version watermark is sync-attested and monotonic

The durable watermark (`meta/wal-watermark`) is published only AFTER the sync it attests,
never regresses, and recovery refuses any state the watermark proves incomplete (missing
attested segments; sole-segment deletion). Torn final WAL records are provably unacked and
repairable without violating this.

**Audit**: the #2769 watermark tests + `wal_segment_loss` testkit lane + the promoted 4.9a
pins (permanent contracts since the fix). Verify `verify_commit_watermark_recoverable` runs
BEFORE any tail repair mutates bytes.

### DUR-006: Timed waits are safety nets, never load-bearing

Every blocking coordination protocol must reach completion under direct notification alone;
a timed wait may only absorb lost wake-ups, never structural gaps. Under loom (whose
`wait_timeout` never fires) a protocol needing the timeout deadlocks — that detection is
the contract's enforcement.

**Audit**: the per-PR loom job explores every seam-migrated protocol with timeouts
unreachable. Precedent: the #2815 completion-counter condvar was load-bearing and was
replaced by the token queue. New blocking protocols must join the loom seam.

### DUR-007: Degraded data-loss health fail-closes mutating admission

`RecoveryHealth::Degraded { DataLoss }` (lossy recovery) blocks mutating commit admission
non-retryably, with no silent resume path. The engine never opens lossy; the storage-level
lossy surface is read-only after loss by design.

**Audit**: `maintenance_ready_for_recovery_health` + the admission check in
`require_write_admission_recovery_health`. The whole-DB DST records `degraded_read_only`
and ends trajectories there — verify the harness still treats it as terminal, not
retryable.

### DUR-008: Fork sources are retained while recovery-dependent children live

A branch that is the fork source of a live LAYER-LESS child with non-empty fork-visible
rows must not be deleted on the durable live path — that child's recovery re-materializes
from the source (`rebuild_fork_snapshot_rows`). Layered children (durably published
inherited-layer manifests) and empty forks keep their parent deletable. Cache mode (no
recovery) and WAL replay (history re-application) are exempt. The recovery rebuild skips
Deleted sources — sound only because of this refusal.

The layered exemption's premise — a layered child IS durably manifest-covered — is
enforced at fork time (#2855): the durable runtime's COW eligibility requires every
in-fork sealed table to be durably cataloged (`ForkSealedTableDurability`), so a child
layer never references a volatile table (whose manifest publish is guaranteed to fail
best-effort, leaving a child that LOOKS layered without durable coverage). Volatile
sources take the eager path, whose layer-less child this refusal protects.

The rebuild itself is IDEMPOTENT against every earlier re-materializer (#2859): it
runs on EVERY reopen of a layer-less child as the last row-installing recovery step,
and a previous reopen's rebuild may already sit durably in the child's owned tables
(a flush seals the re-materialized memtable). Each snapshot row is elided when its
internal key is already present anywhere in the child's local stack
(`contains_internal_key`: active, frozen, owned — layers excluded); a duplicate that
surfaces past the probe fails recovery loudly. Without the elision, the re-flush
seals a second durable copy of the internal key and the next compaction of the
child's tables fails on the duplicate (drain error or failure-ring entry).

**Audit**: `require_no_recovery_dependent_children` (branch catalog) called from the
durable delete only; `branch_delete_refused_while_layerless_fork_children_live`,
`branch_delete_of_layered_fork_source_stays_allowed`,
`fork_parent_deletion_cannot_brick_recovery` (api tests) pin all three directions;
`branch_dag_model` (engine, cache) pins the cache exemption. The durability gate:
`in_fork_sealed_durable` in `fork_at_retained_version_with_unsealed_builder` with the
table-catalog predicate passed by both durable fork wrappers (`bootstrap.rs`);
`cow_fork_over_a_volatile_source_keeps_inheritance_across_reopen` and
`refork_over_a_deleted_name_does_not_adopt_the_dead_generations_manifest` (api tests)
pin the two failure legs, the layered-delete test above pins the durable-COW keep
direction. Rebuild idempotence: the elision loop in `rebuild_fork_snapshot_rows`
(`lifecycle/durable/bootstrap.rs`) over `contains_internal_key`
(`branch/state.rs`); `durable_layerless_fork_rebuild_elides_rows_already_flushed_durable`
(api/tests/branch.rs) pins the reopen/flush/compact choreography end to end,
`contains_internal_key_probes_the_whole_local_stack` (branch/tests/flush_install.rs)
pins the probe truth table. Origin: #2820; durability gate: #2855 (DST seeds 183,
134); rebuild idempotence: #2859 family A (DST seed 794 canonical, 29 deep-shape
seeds incl. 116/22).

### DUR-009: Enqueue mirrors execution

Every maintenance scheduling site (enqueue, pacing, growth policy) consults the same
structural-deferral predicate registry as the execution-time deferral arms. A task kind
whose scheduling and execution disagree churns (#2792) or hard-fails (#2798).

A branch-scoped task legally races a branch delete (#2859 family F): the target's
descriptor survives with status Deleted. Every consumption point cancels such a task
terminally — Canceled, consumed, ring-silent — never Failed (a legal race is not a
failure), never Deferred (a re-created name shares the branch id under a new
generation, and the stale task must not run against it), never propagated (the old
foreground behavior failed the whole drain). The race has three windows, all covered:
the foreground runner (started after delete), the background starter (started after
delete), and the background publish phase (deleted between the off-lock build and the
install — the built objects are unreferenced and the sweep reclaims them).

**Audit**: `checkpoint_structural_deferral` is the single authority (raw predicates are
private — a divergent consumer must be uncompilable); `scheduling_composition_guard.rs`
pins the shape and the anti-churn oracle. New task kinds add registry variants, never
ad-hoc arms. Deleted-scope cancel: `branch_is_deleted` (`branch_lifecycle.rs`) +
`deleted_scope_canceled_outcome` arms in `run_compaction_maintenance_task`,
`start_background_compaction_task`, `run_flush_maintenance_task`, the background flush
starter, `begin_flush_publish`, and `begin_compaction_publish`
(`lifecycle/durable/maintenance.rs`);
`drain_cancels_branch_scoped_compaction_enqueued_before_the_branch_was_deleted`
(api/tests/branch.rs) pins the drain surface across the race,
`branch_is_deleted_answers_only_for_surviving_deleted_descriptors`
(lifecycle/tests/branch_lifecycle/clear_delete.rs) pins the predicate truth table. The
whole-DB DST deep seeds 24 30 38 88 122 220 237 332 364 are the volume lane.

### DUR-010: Recovery combines a non-seeded checkpoint with a flush-published base

A snapshot MAY legitimately coexist with a non-seeded branch's durable table-manifest
base: the checkpoint-side guard (`non_seeded_branch_has_durable_base`) defers a
checkpoint over an existing base, but it cannot defer a FUTURE flush — a flush that
runs after the checkpoint publishes the branch's manifest and creates the coexistence
(#2847). Recovery MUST therefore COMBINE, never refuse: an occupied non-seeded target
dedups the checkpoint's rows against its manifest-recovered owned levels by internal
key (byte-identical duplicates drop; divergent bytes fail closed), and appends only the
uncovered remainder — Recovery Protocol rule 9 extended beyond the seeded branch. A
leftover row is always strictly newer than any manifest row at its physical key (the
flush covered a superset of the snapshot's rows), so active-first newest-wins holds.

Before any install, checkpoint rows are generation-fenced exactly like WAL records
(#2826/#2833): NO branch — parentless or fork child — installs a row at
`version <= created_at`. A stale pre-delete checkpoint for a re-created name is
indistinguishable from legitimate inheritance by (branch, key, version) alone; the
checkpoint is never the authority for a fork child's inherited content
(`rebuild_fork_snapshot_rows` re-materializes it from the live parent, and covered
children ride their manifest layers), so the whole `<= created_at` band drops.

The checkpoint's delta premise — "owned-table rows may be skipped because a durable
manifest covers them" — is enforced at CAPTURE time (#2863): a VOLATILE owned table
(a snapshot-install L0, no durable catalog entry) is not a base. Its rows are
captured into the snapshot (their only durable home is the snapshot being
superseded), and it contributes to neither `has_durable_rows` nor the recorded
`flushed_through` floor. Without this, a checkpoint over a snapshot-recovered
branch deltas over coverage no manifest holds, the WAL truncates, and the rows are
durably lost — the reopen then correctly refuses the orphaned delta (strict) or
recovers an empty prefix (lossy). Because the structural-deferral guard keeps
non-seeded durable bases out of published snapshots, the durable-only floor is
also exactly the seeded branch's own — restoring the seeded-only orphan
heuristic's single-branch premise.

**Audit**: `combine_non_seeded_checkpoint_rows`, the partition, and the
`record_predates_current_generation` row fence in
`install_non_seeded_checkpoint_rows` (`lifecycle/durable/bootstrap.rs`);
capture-side: `branch_checkpoint_collection` + the durability predicate on
`branch_checkpoint_flush_boundary` (`lifecycle/checkpoint.rs`), threaded from the
table catalog by all three collectors (sync runtime, close drain, background
starter's per-branch durable-identity sets);
`checkpoint_over_a_snapshot_recovered_base_stays_self_contained` (api/tests/branch.rs)
pins the strict-reopen choreography end to end,
`sync_checkpoint_captures_volatile_base_rows_and_records_no_flush_floor`
(lifecycle/tests/durable.rs) pins both capture halves through the foreground lane;
the whole-DB DST seeds 483/1655 are the volume lane;
`checkpoint_then_flush_of_non_seeded_branch_survives_reopen` (api/tests/branch.rs) pins
the choreography end to end, `non_seeded_checkpoint_combine_dedups_appends_and_fails_closed`
(lifecycle/tests/recovery.rs) pins the dedup/append/fail-closed truth table.
`refork_of_a_deleted_name_does_not_resurrect_dead_generation_checkpoint_rows` and
`fork_child_own_rows_survive_reopen_through_the_checkpoint` (api/tests/branch.rs) pin
both directions of the fence. The whole-DB DST sweep (seeds 52 72 76 86 120 150 152
162 176 188; fence seeds 94 142 180) is the volume lane.

### DUR-011: Durable content is authoritative over retained-timeline coverage

After a lossy recovery, flush-published content legally outlives retained-timeline
coverage: rows recover from durable tables while their (version→timestamp) facts shed
with the WAL, and a checkpoint's timeline groups cover only their own watermark — so a
branch's content watermark may permanently exceed its timeline tip. Operations anchored
on CONTENT (fork-current above all) MUST resolve from row-source facts
(`max_commit_version` across active/frozen/owned/inherited), never from timeline bounds
(#2852: the timeline-based resolution silently forked an EMPTY child over a populated
source). Temporal operations (fork-at-version/at-timestamp, at-timestamp reads) may
refuse on missing coverage, but must not deny coverage the index provably retains —
the availability leg, closed by #2853: retained history past the index tip SHRINKS to
the provable prefix, it never vanishes. Three rules enforce it: (1) the index's
bounded lookups CLAMP a view bound above the tip to the tip's prefix (a shed
version's mapping is Unproven, never proven-Absent; queries past the tip's timestamp
keep the after-latest refusal shape); (2) the scan-fallback surfaces re-consult the
index after seeding it (the seed folds observed entries the empty post-elision scan
cannot see); (3) recovery's checkpoint/manifest COMBINE re-seeds the snapshot's
timeline group onto the SURVIVING branch instance — the group is the only durable
carrier of retired (version→timestamp) facts, and the combine's instance swap
previously discarded it, force-completing the index EMPTY.

**Audit**: `current_branch_version` (`api/runtime/mod.rs`, the fork-current anchor)
reads branch read-view facts, not `timeline_view` bounds; the `retained_floor` fallback
maps a degraded timeline to a ZERO floor only where content or prior timeline validation
bounds the fork version. `fork_current_captures_content_that_outlives_timeline_coverage`
(api/tests/branch.rs) pins the choreography;
`fork_current_of_a_rowless_source_stays_a_legitimate_empty_fork` pins the #2521 empty-fork
direction control. Availability leg: `bounded_prefix` clamp + the tip guard in
`timestamp_for_version` (`timeline_index.rs`;
`bound_above_tip_serves_the_clamped_prefix` is the truth table), the post-seed
re-consults in `timeline_view_or_index` / `timeline_version_at_or_before` /
`timeline_timestamp_for_version` (`api/runtime/data.rs`), and the COMBINE-arm
`seed_branch_timeline_from_groups` re-seed (`lifecycle/recovery.rs`);
`fork_at_version_inside_surviving_timeline_coverage_succeeds_after_lossy_crash`
(api/tests/branch.rs) pins the end-to-end choreography with its shed-version refusal
direction control. The whole-DB DST sweep (seeds 83 154 164 178) is the volume lane.

### DUR-012: A checkpoint-attesting manifest requires its WAL chain on disk

A `DatabaseManifest` that attests a published checkpoint proves durable history existed, so
the WAL chain through that watermark MUST exist on disk at open. With zero segment objects,
a fresh empty log would present a gutted store as a healthy empty database — strict open
REFUSES (`recovery corruption`); explicit lossy open proceeds, recovers what the checkpoint
holds, and records `WalCommittedSuffixMissing` (never healthy). A manifest with no
checkpoint attestation (first creation torn early) may recreate freely — nothing
acknowledged can exist yet. Testkit corollary: every checkpoint-attesting fixture must seed
its attested segment (the #2902 class).

**Audit**: Find `checkpoint_attested_wal_chain_missing` and the strict refusal
(`lifecycle/durable.rs`); the lossy fault push (`lifecycle/recovery.rs`). Direction pins:
`strict_open_still_refuses_a_checkpoint_attested_store_with_no_wal`,
`lossy_recovery_degrades_when_checkpoint_attested_wal_chain_is_missing`
(`lifecycle/tests/recovery.rs`). Origin: #2765/#2777.

### DUR-013: The timeline is derived — single observation funnel, provable bounds

Commits materialize NO timeline rows (W3.1c). The retained-timeline index is the only
current timeline source: it observes each commit's stamp at the single apply funnel —
`retained_timeline().observe(...)` in `append_committed_rows_atomically` /
`append_committed_row` (`branch/state/append.rs`), with the batch path observing only after
full-batch success so a rollback leaves no trace. Completeness is a LIFECYCLE-layer mark
(`mark_complete_from_birth` for in-process creation; recovery seeds/marks per DUR-011) —
direct state construction is incomplete-by-default. Consumers materialize views from the
index (`timeline_view_or_index`), re-consult after seeding on the scan fallback, and CLAMP
any bound above the index tip to the tip's provable prefix (`bounded_prefix`) — a bound
beyond the tip is unprovable, never proven-absent. Testkit corollary: harness oracles read
the index, never a timeline-space scan (the #2848/#2896 class).

**Audit**: Verify `prepare_commit_rows` emits no timeline rows (`commit/cache.rs`); verify
`observe` has no caller outside the append funnel; verify the batch-path late observation;
verify `timeline_view_or_index` and both lookup surfaces clamp and re-seed
(`api/runtime/data.rs`, `timeline_index.rs`). DUR-011 holds the content-vs-coverage
authority law this mechanism serves.

### DUR-014: Maintenance failures are classified into the ring, never bare-counted

Every maintenance task failure MUST route through `record_failure_detail` into the bounded
failure ring (`MaintenanceFailureRecord { kind, reason, source_error_code }`, capacity 4) —
the bare `failed` counter is never the only evidence. The ring is what lets test lanes and
operators classify a red instantly (#2763/#2773); a failure path that skips it regresses
triage to archaeology. Legal-race cancellations are ring-SILENT by DUR-009 (Canceled is not
a failure).

**Audit**: Verify every failure arm in the maintenance executor calls
`record_failure_detail` (`lifecycle/durable/maintenance.rs`); verify ring capacity and
shape. Test: `active_task_failure_during_close_drain_records_kind_and_error_code`
(`lifecycle/tests/maintenance.rs`).

### DUR-015: Durable assembly's object listings are exactly the two WAL scans

Durable assembly performs exactly TWO directory listings, both of the WAL prefix: the
segment-loss inventory check (#2690) and the writer-resume reconciliation scan (#2555).
Anything else listing objects during assembly is a side-effect violation — assembly must
not browse the store it has not yet recovered.

**Audit**: The testkit pin (`testkit/lifecycle/durable.rs`, the two-scan listing law in
`check_durable_standard_create`) instruments the backend and fails on any third listing.
Verify the pin still asserts exactly two and the fault-injection CI lane runs it.

## How to Use This Catalog

### After a code change

1. Identify which invariants the change could affect (use the category prefixes: LSM, CMP, COW, MVCC, ACID, ARCH, SCALE, DUR)
2. Execute the **Audit** instruction for each affected invariant against the current codebase
3. If an invariant is violated, fix the code — do not weaken the invariant

### After an architectural change

1. Review all invariants for relevance — some may need updating
2. Add new invariants for new architectural properties
3. Remove invariants for removed architectural properties
4. The invariant IDs are stable — do not renumber (use gaps if removing)

### During code review

Reference invariants by ID: "This change could violate CMP-001 — verify tombstone handling
in the new compaction path."
