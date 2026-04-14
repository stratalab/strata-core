# Branch Primitive - Architecture Reference

## Overview

The Branch primitive manages isolated data namespaces. Each branch is an independent scope containing its own KV, Event, State, JSON, and Vector data. Branches are tracked via a `BranchIndex` that stores metadata in a global namespace outside any branch.

- **Version semantics**: `Version::Counter(u64)` - internal version counter in `BranchMetadata` struct
- **Key construction**: `Key { namespace: global_namespace(), type_tag: TypeTag::Branch (0x05), user_key: branch_name.as_bytes() }`
- **Storage format**: `Value::String(JSON)` wrapping `BranchMetadata { name, branch_id, status, created_at, updated_at, version, ... }`
- **Transactional**: No - branch operations bypass the Session transaction layer; internally use transactions on the global namespace

## Layer Architecture

```
+------------------------------------------------------------------+
|  CLIENT                                                          |
|  Command::BranchCreate { branch_id, metadata }                   |
+------------------------------------------------------------------+
        |
        v
+------------------------------------------------------------------+
|  SESSION (session.rs)                                            |
|  ALWAYS routes to executor (non-transactional)                   |
|  Even if a transaction is active, branch ops bypass it           |
+------------------------------------------------------------------+
        |
        v
+------------------------------------------------------------------+
|  EXECUTOR (executor.rs)                                          |
|  Dispatches to: crate::handlers::branch::branch_create(...)      |
+------------------------------------------------------------------+
        |
        v
+------------------------------------------------------------------+
|  HANDLER (handlers/branch.rs + bridge.rs)                        |
|  1. Generate UUID v4 for anonymous branches (if no name given)   |
|  2. Guard: reject delete of default branch                       |
|  3. Call primitives.branch.create_branch(name)                   |
|  4. Convert BranchMetadata -> BranchInfo for output              |
+------------------------------------------------------------------+
        |
        v
+------------------------------------------------------------------+
|  ENGINE PRIMITIVE (primitives/branch/index.rs - BranchIndex)     |
|  Uses GLOBAL namespace (BranchId all-zeros)                      |
|  1. db.transaction(global_branch_id, |txn| {                     |
|       - Check if branch already exists                            |
|       - Create BranchMetadata::new(name)                         |
|       - Serialize to JSON string                                  |
|       - txn.put(Key::new_branch_with_id(global_ns, name), val)  |
|     })                                                           |
+------------------------------------------------------------------+
        |
        v
+------------------------------------------------------------------+
|  TRANSACTION / STORAGE                                           |
|  Branch metadata stored in global namespace (BranchId [0;16])    |
|  Data isolation achieved by Namespace::for_branch(branch_id)     |
+------------------------------------------------------------------+
```

### Branch ID Resolution

All primitives must convert user-facing branch names to internal `BranchId` (UUID). This happens identically in two places:

```
"default"           ->  BranchId([0u8; 16])           // Nil UUID
Valid UUID string   ->  BranchId(parsed UUID bytes)    // Direct parse
Any other string    ->  BranchId(UUID_v5(NAMESPACE, name))  // Deterministic
```

The resolution function exists in both:
- `crates/executor/src/bridge.rs::to_core_branch_id()` (executor layer)
- `crates/engine/src/primitives/branch/index.rs::resolve_branch_name()` (engine layer)

Both use the same UUID v5 namespace constant: `0x6ba7b810-9dad-11d1-80b4-00c04fd430c8`

## Operation Flows

### BranchCreate

```
Client               Handler             Engine (BranchIndex)  Transaction          Storage
  |                    |                   |                    |                   |
  |-- BranchCreate --->|                   |                    |                   |
  | {branch_id?, meta?}|                   |                    |                   |
  |                    |                   |                    |                   |
  |                    |-- name given? --->|                    |                   |
  |                    |   YES: use it     |                    |                   |
  |                    |   NO: UUID v4     |                    |                   |
  |                    |                   |                    |                   |
  |                    |                   |-- begin txn ------>|                   |
  |                    |                   |   on GLOBAL ns     |                   |
  |                    |                   |   (BranchId [0;16])|                   |
  |                    |                   |                    |                   |
  |                    |                   |-- txn.get -------->|-- read chain ---->|
  |                    |                   |   Key::new_branch  |                   |
  |                    |                   |   (global, name)   |                   |
  |                    |                   |                    |                   |
  |                    |                   |   EXISTS:          |                   |
  |                    |                   |   Err(already      |                   |
  |                    |                   |   exists)          |                   |
  |                    |                   |                    |                   |
  |                    |                   |   NOT EXISTS:      |                   |
  |                    |                   |   BranchMetadata   |                   |
  |                    |                   |   ::new(name)      |                   |
  |                    |                   |   {name, uuid_v4,  |                   |
  |                    |                   |    status:Active,  |                   |
  |                    |                   |    timestamps,     |                   |
  |                    |                   |    version:1}      |                   |
  |                    |                   |                    |                   |
  |                    |                   |-- to_stored_value ->|                  |
  |                    |                   |   JSON serialize   |                   |
  |                    |                   |                    |                   |
  |                    |                   |-- txn.put -------->|-- write_set ----->|
  |                    |                   |                    |                   |
  |                    |                   |-- commit --------->|-- persist ------->|
  |                    |                   |                    |                   |
  |<- BranchWithVer ---|<- metadata_to_ ---|<- Versioned -------|                   |
  |  {info, version}   |  branch_info()    |  <BranchMetadata>  |                   |
```

**Steps:**

1. **Handler**: If `branch_id` is `None`, generates a UUID v4 string. Metadata parameter is MVP-ignored. Calls `primitives.branch.create_branch(name)`.
2. **Engine (BranchIndex)**: Opens transaction on the **global namespace** (`BranchId([0u8; 16])`). Checks if branch already exists. Creates `BranchMetadata::new(name)` which generates a new random UUID as the `branch_id` field, sets `status = Active`, timestamps to now, `version = 1`. Serializes to JSON. Writes to global namespace.
3. **Handler output**: Converts `BranchMetadata` to the executor's `BranchInfo` type for the response.

**Note on dual IDs**: Each branch has two IDs:
- The **user-facing name** (e.g., `"my-branch"`) - stored as the key and in `metadata.name`
- The **internal UUID** (random v4) - stored in `metadata.branch_id`
- The **namespace UUID** (deterministic v5 from name) - used to scope data storage

---

### BranchGet

```
Client               Handler             Engine (BranchIndex)  Transaction          Storage
  |                    |                   |                    |                   |
  |-- BranchGet ------>|                   |                    |                   |
  | {branch}           |                   |                    |                   |
  |                    |                   |                    |                   |
  |                    |                   |-- begin txn ------>|                   |
  |                    |                   |   on GLOBAL ns     |                   |
  |                    |                   |                    |                   |
  |                    |                   |-- txn.get -------->|-- read chain ---->|
  |                    |                   |   Key::new_branch  |                   |
  |                    |                   |   (global, name)   |                   |
  |                    |                   |                    |                   |
  |                    |                   |   NOT FOUND:       |                   |
  |                    |                   |   return None      |                   |
  |                    |                   |                    |                   |
  |                    |                   |   FOUND:           |                   |
  |                    |                   |<- Value::String ---|                   |
  |                    |                   |   (JSON)           |                   |
  |                    |                   |                    |                   |
  |                    |                   |-- deserialize ---->|                   |
  |                    |                   |   BranchMetadata   |                   |
  |                    |                   |   from JSON        |                   |
  |                    |                   |                    |                   |
  |                    |                   |-- into_versioned ->|                   |
  |                    |                   |   Versioned<Meta>  |                   |
  |                    |                   |                    |                   |
  |<- BranchInfo ------|<- convert --------|                    |                   |
  |  or Maybe(None)    |                   |                    |                   |
```

**Steps:**

1. **Handler**: Calls `primitives.branch.get_branch(branch.as_str())`. If found, converts to `VersionedBranchInfo`. If not, returns `Output::Maybe(None)`.
2. **Engine (BranchIndex)**: Opens transaction on global namespace. Reads the branch key. Deserializes `BranchMetadata` from JSON. Wraps in `Versioned` with the metadata's internal version counter and timestamp.

---

### BranchList

```
Client               Handler             Engine (BranchIndex)  Transaction          Storage
  |                    |                   |                    |                   |
  |-- BranchList ----->|                   |                    |                   |
  | {state?, limit?,   |                   |                    |                   |
  |  offset?}          |                   |                    |                   |
  |                    |                   |                    |                   |
  |                    |                   |-- begin txn ------>|                   |
  |                    |                   |   on GLOBAL ns     |                   |
  |                    |                   |                    |                   |
  |                    |                   |-- scan_prefix ---->|-- scan DashMap -->|
  |                    |                   |   Key::new_branch  |   global ns       |
  |                    |                   |   (global, "")     |   TypeTag::Branch  |
  |                    |                   |                    |                   |
  |                    |                   |<- Vec<(Key,Val)> --|                   |
  |                    |                   |                    |                   |
  |                    |                   |-- filter_map ----->|                   |
  |                    |                   |   extract name     |                   |
  |                    |                   |   skip __idx_ keys |                   |
  |                    |                   |                    |                   |
  |                    |<- Vec<String> ----|                    |                   |
  |                    |   (branch names)  |                    |                   |
  |                    |                   |                    |                   |
  |                    |== FOR EACH NAME ==|====================================== |
  |                    |                   |                    |                   |
  |                    |-- get_branch() -->|-- txn.get -------->|-- read chain ---->|
  |                    |   (full metadata) |                    |                   |
  |                    |                   |                    |                   |
  |                    |== END LOOP =======|====================================== |
  |                    |                   |                    |                   |
  |                    |-- apply limit --->|                    |                   |
  |                    |   (state, offset  |                    |                   |
  |                    |    MVP-ignored)   |                    |                   |
  |                    |                   |                    |                   |
  |<-- BranchInfoList -|                   |                    |                   |
  | Vec<VersionedBranchInfo>               |                    |                   |
```

**Steps:**

1. **Handler**: Calls `primitives.branch.list_branches()` to get all branch names. Then calls `get_branch()` for each name to get full metadata. Applies `offset` and `limit` if specified. `state` is currently a no-op because the executor only exposes `Active`.
2. **Engine (BranchIndex)**: Opens transaction on global namespace. Scans all branch keys via `scan_prefix`. Filters out internal `__idx_` keys (legacy data). Returns list of branch name strings.

**Two-phase approach**: List first gets all names (lightweight scan), then fetches full metadata per branch. This is N+1 reads but keeps the scan lock short.

---

### BranchDelete

```
Client               Handler             Engine (BranchIndex)  Transaction          Storage
  |                    |                   |                    |                   |
  |-- BranchDelete --->|                   |                    |                   |
  | {branch}           |                   |                    |                   |
  |                    |                   |                    |                   |
  |                    |-- guard: not ---->|                    |                   |
  |                    |   "default"       |                    |                   |
  |                    |                   |                    |                   |
  |                    |                   |-- get_branch() --->|-- read meta ----->|
  |                    |                   |   (verify exists)  |                   |
  |                    |                   |                    |                   |
  |                    |                   |-- resolve both --->|                   |
  |                    |                   |   IDs:             |                   |
  |                    |                   |   1. executor_id = |                   |
  |                    |                   |      v5(name)      |                   |
  |                    |                   |   2. metadata_id = |                   |
  |                    |                   |      meta.branch_id|                   |
  |                    |                   |                    |                   |
  |                    |                   |== DELETE DATA (executor namespace) ====|
  |                    |                   |                    |                   |
  |                    |                   |-- for each TypeTag:|                   |
  |                    |                   |   KV, Event, State,|                   |
  |                    |                   |   Json, Vector     |                   |
  |                    |                   |                    |                   |
  |                    |                   |-- scan_prefix ---->|-- scan DashMap -->|
  |                    |                   |   (ns + type_tag)  |                   |
  |                    |                   |                    |                   |
  |                    |                   |-- txn.delete each ->|-- tombstone ---->|
  |                    |                   |                    |                   |
  |                    |                   |== END DELETE DATA ======================|
  |                    |                   |                    |                   |
  |                    |                   |== IF metadata_id != executor_id: ======|
  |                    |                   |   repeat data      |                   |
  |                    |                   |   deletion for     |                   |
  |                    |                   |   metadata ns      |                   |
  |                    |                   |== END IF ==============================|
  |                    |                   |                    |                   |
  |                    |                   |== DELETE METADATA =======================|
  |                    |                   |                    |                   |
  |                    |                   |-- begin txn ------>|                   |
  |                    |                   |   on GLOBAL ns     |                   |
  |                    |                   |                    |                   |
  |                    |                   |-- txn.delete ----->|-- tombstone ----->|
  |                    |                   |   meta key         |   in global ns    |
  |                    |                   |                    |                   |
  |                    |                   |-- commit --------->|-- persist ------->|
  |                    |                   |                    |                   |
  |<-- Output::Unit ---|                   |                    |                   |
```

**Steps:**

1. **Handler**: Guards against deleting the default branch (`Error::ConstraintViolation`). Calls `primitives.branch.delete_branch()`.
2. **Engine (BranchIndex)**: Complex multi-phase deletion:
   - **Phase 1**: Get branch metadata to verify existence
   - **Phase 2**: Resolve both the executor's deterministic `BranchId` (UUID v5 from name) and the metadata's random `BranchId` (UUID v4 from creation)
   - **Phase 3**: Delete all data in the executor's namespace by scanning each `TypeTag` (KV, Event, State, Json, Vector) and deleting all matching keys
   - **Phase 4**: If the metadata `BranchId` differs from the executor `BranchId`, also delete data in the metadata namespace (handles edge case of dual-ID system)
   - **Phase 5**: Delete the branch metadata itself from the global namespace

**Cascading delete**: This is the only operation that performs cascading deletion across all primitive types. It deletes ALL data belonging to the branch: KV entries, events, state cells, JSON documents, vector entries, and vector collection configs.

**Transaction count**: Multiple transactions are used (one per TypeTag for data deletion + one for metadata deletion). This is NOT atomic across all types.

## Storage Format

```
TypeTag:           0x05 (Branch)
Key format:        global_namespace() + TypeTag::Branch + branch_name.as_bytes()
Value format:      Value::String(JSON) containing BranchMetadata
Global namespace:  Namespace::for_branch(BranchId([0u8; 16]))
```

### BranchMetadata Struct (stored as JSON)

```
BranchMetadata {
    name:            String             // User-provided branch name
    branch_id:       String             // Random UUID v4 (internal)
    parent_branch:   Option<String>     // Post-MVP: parent branch ref
    status:          BranchStatus       // "active" (only value in MVP)
    created_at:      u64                // Microseconds since epoch
    updated_at:      u64                // Microseconds since epoch
    completed_at:    Option<u64>        // Post-MVP
    error:           Option<String>     // Post-MVP
    version:         u64                // Internal version counter
}
```

### Namespace Isolation Model

```
Global namespace (branch metadata):
  Namespace { tenant: "default", app: "default", agent: "default",
              branch_id: BranchId([0; 16]) }

Branch data namespace (per-branch data):
  Namespace { tenant: "default", app: "default", agent: "default",
              branch_id: BranchId(UUID_v5(name)) }
```

All data for a branch is scoped under its `Namespace::for_branch(branch_id)`. Different branches never share keys because the namespace includes the branch UUID.

## Transaction Behavior

| Aspect | Behavior |
|--------|----------|
| Session transactional | **No** - bypasses Session transaction layer |
| Internal transactions | Yes - on global namespace (`BranchId [0;16]`) |
| Create atomicity | Atomic (single transaction) |
| Delete atomicity | **Not atomic** - multiple transactions across TypeTags |
| Default branch guard | Cannot delete "default" branch |

## Consistency Notes

- Branch is one of two non-transactional primitives (along with Vector). Branch and Vector commands always bypass the Session transaction layer.
- Branch metadata lives in a **global namespace** (nil UUID), not inside any branch. This is unique among all primitives.
- The **dual ID problem**: Each branch has a user-facing name, a deterministic UUID (v5 from name, used for data namespacing), and a random UUID (v4, stored in metadata). The delete operation handles both namespaces to ensure complete cleanup.
- Branch `delete` is the **only cascading operation** in the system - it deletes all data across all primitive types (KV, Event, State, JSON, Vector).
- Branch `delete` is **not atomic** across TypeTags. A crash during deletion could leave partial data. However, the branch metadata is deleted last, so a partial deletion would leave orphaned data but not a corrupt branch index.
- The `list` operation is **two-phase** (scan names, then fetch metadata per name), unlike KV/JSON list which scan and collect in one pass.
- `BranchList` applies `offset` and `limit`. `state` is currently a no-op because the executor only exposes `Active`.
- The handler for `BranchCreate` generates a UUID v4 if no name is provided, making it possible to create anonymous branches.
- `BranchCreate` still accepts a `metadata` field for command parity, but the current executor handler ignores it.
