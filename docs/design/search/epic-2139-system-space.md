# Epic: Add `_system_` Space on Every Branch (#2139)

The foundation for the entire search substrate. Recipes, expansion cache, statistics, and eventually embeddings all live in the `_system_` space on the user's branch.

---

## 1. Background

### How spaces work today

Strata spaces are a namespace layer in the key encoding:

```
InternalKey = branch_id (16B) || space (null-terminated) || type_tag (1B) || user_key (byte-stuffed) || commit_id (8B)
```

Spaces provide logical isolation within a branch. Data in space "alpha" is invisible to prefix scans in space "beta" — the null-terminated space name in the key encoding guarantees this.

### Existing reserved spaces

| Space | Purpose | How it works |
|---|---|---|
| `default` | Implicit default for all operations | Always exists. Never registered. Can't be deleted. Hardcoded in `SpaceIndex.exists()` and `SpaceIndex.list()`. |
| `_graph_` | All graph data | Fixed namespace via `graph_namespace(branch_id)`. Cached in DashMap per branch. |

### Current validation

`validate_space_name()` in `crates/core/src/types.rs:481-512` already rejects names starting with `_system_`:

```rust
if name.starts_with("_system_") {
    return Err("Space names starting with '_system_' are reserved".into());
}
```

User writes to `_system_` are already blocked at the validation layer. We need to add the internal bypass.

---

## 2. Design

### The `_system_` space

A hidden, internal-only space on every branch. Follows the `_graph_` pattern.

| Property | Behavior |
|---|---|
| **Name** | `_system_` (constant) |
| **Exists on every branch** | Implicit, like `default`. Never needs registration. |
| **Hidden from user** | Not returned by `SpaceIndex.list()`, `db.kv.list()`, `db.json.list()`, etc. |
| **User writes rejected** | Already handled by `validate_space_name()`. |
| **Internal writes allowed** | New: internal API bypasses space validation. |
| **Forks via COW** | Automatic — space is part of the key encoding. Fork a branch, `_system_` data inherits via COW. |
| **Can't be deleted** | Enforced in space deletion handler. |

### What will live here (planned, not all in this issue)

| Data | Written by | Read by | Version |
|---|---|---|---|
| Recipes (`recipe:default`, `recipe:{name}`) | `db.set_recipe()` | Substrate `retrieve()` | v0.0 |
| Expansion cache | Intelligence layer | Intelligence layer | v0.2 |
| Statistics | Statistics collector | `db.describe()` | v0.9 |
| Experiment logs | `db.experiment()` | `db.experiment()` | v0.7 |
| Shadow embeddings (future) | Auto-embed hook | Vector search | v0.0-7 |

---

## 3. Implementation

### 3.1 Constants and namespace helper

**File:** New — `crates/engine/src/system_space.rs`

```rust
use std::sync::Arc;
use dashmap::DashMap;
use once_cell::sync::Lazy;
use strata_core::types::{BranchId, Namespace, Key, TypeTag};

pub const SYSTEM_SPACE: &str = "_system_";

static NS_CACHE: Lazy<DashMap<BranchId, Arc<Namespace>>> = Lazy::new(DashMap::new);

/// Get the _system_ namespace for a branch. Cached — one allocation per branch ever.
pub fn system_namespace(branch_id: BranchId) -> Arc<Namespace> {
    NS_CACHE
        .entry(branch_id)
        .or_insert_with(|| Arc::new(Namespace::for_branch_space(branch_id, SYSTEM_SPACE)))
        .clone()
}

/// Construct a KV key in the _system_ space.
pub fn system_kv_key(branch_id: BranchId, key: &str) -> Key {
    Key::new_kv(system_namespace(branch_id), key)
}

/// Construct a JSON key in the _system_ space.
pub fn system_json_key(branch_id: BranchId, doc_id: &str) -> Key {
    Key::new_json(system_namespace(branch_id), doc_id)
}

/// Invalidate cached namespace for a branch (on branch deletion).
pub fn invalidate_cache(branch_id: &BranchId) {
    NS_CACHE.remove(branch_id);
}
```

**Pattern:** Identical to `crates/graph/src/keys.rs:92-109` (`graph_namespace`). Same DashMap cache, same lazy initialization, same Arc<Namespace> return.

### 3.2 Internal write API

The existing write path validates space names. Internal writes need to bypass this.

**Option A: Flag on the write call.**

Add `internal: bool` parameter to the executor's `ensure_space_registered()`:

```rust
// crates/executor/src/executor.rs
fn ensure_space_registered(&self, branch: &BranchId, space: &str, internal: bool) -> Result<()> {
    if space == "default" || (internal && space == SYSTEM_SPACE) {
        return Ok(());
    }
    // ... existing validation ...
}
```

**Option B: Direct Database writes bypassing executor.**

The engine-level `Database` doesn't validate space names — validation happens in the executor layer. Internal code can write directly to the database using `system_namespace()`:

```rust
// Engine level — no space validation
let key = system_kv_key(branch_id, "recipe:default");
db.put_with_version_mode(key, value, VersionMode::Auto)?;
```

**Recommendation: Option B.** Internal writes go through `Database` directly, not the executor. The executor is the user-facing layer with validation. Internal writes bypass it entirely, just like graph writes go through `GraphStore` directly.

This means `set_recipe()`, auto-embed hooks, statistics collection, etc. write to `_system_` space via the engine, not the executor. No changes to executor validation needed.

### 3.3 Hide from user-facing list/scan operations

**SpaceIndex.list()** — `crates/engine/src/primitives/space.rs:74-91`

Current code returns all registered spaces plus "default". Add: exclude `_system_`.

```rust
pub fn list(&self, branch_id: BranchId) -> StrataResult<Vec<String>> {
    self.db.transaction(branch_id, |txn| {
        let prefix = Key::new_space_prefix(branch_id);
        let results = txn.scan_prefix(&prefix)?;

        let mut spaces: Vec<String> = results
            .into_iter()
            .filter_map(|(k, _)| String::from_utf8(k.user_key.to_vec()).ok())
            .filter(|s| s != SYSTEM_SPACE)  // NEW: hide _system_
            .collect();

        if !spaces.contains(&"default".to_string()) {
            spaces.insert(0, "default".to_string());
        }
        Ok(spaces)
    })
}
```

**One line added.** The space is registered (for COW inheritance tracking) but hidden from the user.

**KVStore.list()** — `crates/engine/src/primitives/kv.rs:221-238`

KV list is already space-scoped — `list(branch_id, space, prefix)` only returns keys in the specified space. A user calling `db.kv.list("default", None)` will never see `_system_` data because it's in a different space. **No changes needed.**

**The same applies to JSON, Event, Vector, Graph** — all operations are namespace-scoped. `_system_` space data is invisible to operations targeting other spaces by design.

The only place `_system_` data could leak is `SpaceIndex.list()` (the list of available spaces). The one-line filter above fixes that.

### 3.4 Prevent deletion

**File:** `crates/executor/src/handlers/space.rs:36-39`

Current code prevents deleting "default". Add `_system_`:

```rust
pub fn space_delete(p: &Primitives, branch: &BranchId, space: &str) -> Result<Output> {
    if space == "default" || space == SYSTEM_SPACE {
        return Err(Error::InvalidInput {
            reason: format!("Cannot delete the '{}' space", space),
            hint: None,
        });
    }
    // ... existing deletion logic ...
}
```

**One line changed** (extend the condition).

### 3.5 Exists check

**File:** `crates/engine/src/primitives/space.rs:60-68`

Current code short-circuits for "default". Add `_system_`:

```rust
pub fn exists(&self, branch_id: BranchId, space: &str) -> StrataResult<bool> {
    if space == "default" || space == SYSTEM_SPACE {
        return Ok(true);
    }
    // ... check storage ...
}
```

**One line changed.**

### 3.6 Branch deletion cleanup

When a branch is deleted, invalidate the cached `_system_` namespace:

**File:** `crates/engine/src/branch_ops.rs` (branch deletion path)

```rust
// After branch deletion:
system_space::invalidate_cache(&branch_id);
```

**One line added.** Matches the pattern in `crates/graph/src/keys.rs:107-109`.

---

## 4. What This Does NOT Do

- **Does not store recipes** — that's #2141 (depends on this)
- **Does not move shadow collections** — that's #2145 (depends on this)
- **Does not add any executor commands** — that's #2141
- **Does not change auto-embed** — that's #2145
- **Does not add statistics collection** — that's v0.9

This issue delivers the space infrastructure only. Other issues build on it.

---

## 5. Files Changed

| File | Change | Lines |
|---|---|---|
| **NEW** `crates/engine/src/system_space.rs` | Constants, namespace helper, cache | ~40 |
| `crates/engine/src/lib.rs` | Add `pub mod system_space;` | 1 |
| `crates/engine/src/primitives/space.rs` | Hide from list, exists check | ~5 |
| `crates/executor/src/handlers/space.rs` | Prevent deletion | ~2 |
| `crates/engine/src/branch_ops.rs` | Cache invalidation on branch delete | ~2 |
| **Total** | | **~50** |

Note: the original estimate of ~200 lines included internal write APIs and space filtering across all primitives. The actual implementation is much smaller because:
1. Space isolation is already provided by the key encoding (no filtering needed per primitive)
2. Internal writes bypass the executor entirely (no API changes needed)
3. Only `SpaceIndex.list()` needs filtering (one line)

---

## 6. Testing

### 6.1 Unit tests for system_space.rs

```rust
#[test]
fn system_namespace_cached() {
    let branch = BranchId::new();
    let ns1 = system_namespace(branch);
    let ns2 = system_namespace(branch);
    assert!(Arc::ptr_eq(&ns1, &ns2)); // same allocation
}

#[test]
fn system_namespace_different_branches() {
    let b1 = BranchId::new();
    let b2 = BranchId::new();
    let ns1 = system_namespace(b1);
    let ns2 = system_namespace(b2);
    assert!(!Arc::ptr_eq(&ns1, &ns2)); // different allocations
}

#[test]
fn system_kv_key_creates_correct_key() {
    let branch = BranchId::new();
    let key = system_kv_key(branch, "recipe:default");
    assert_eq!(key.namespace.space, "_system_");
    assert_eq!(key.type_tag, TypeTag::KV);
}
```

### 6.2 Integration tests

```rust
#[test]
fn system_space_hidden_from_list() {
    let db = Database::cache().unwrap();
    let branch = db.default_branch();

    // Write something to _system_ space (via engine, bypassing executor)
    let key = system_kv_key(branch, "test_key");
    db.put(key, Value::String("test".into())).unwrap();

    // Space list should NOT include _system_
    let spaces = db.space.list(branch).unwrap();
    assert!(!spaces.contains(&"_system_".to_string()));
    assert!(spaces.contains(&"default".to_string()));
}

#[test]
fn system_space_user_write_rejected() {
    let db = Database::cache().unwrap();
    // Attempt to write to _system_ space via the user API (executor)
    let result = executor.kv_put("_system_", "test_key", "value");
    assert!(result.is_err()); // rejected by validate_space_name
}

#[test]
fn system_space_internal_write_succeeds() {
    let db = Database::cache().unwrap();
    let branch = db.default_branch();

    let key = system_kv_key(branch, "recipe:default");
    let result = db.put(key, Value::String("{}".into()));
    assert!(result.is_ok());

    // Read it back
    let val = db.get(&system_kv_key(branch, "recipe:default")).unwrap();
    assert!(val.is_some());
}

#[test]
fn system_space_forks_with_branch() {
    let db = Database::cache().unwrap();
    let main = db.default_branch();

    // Write to _system_ on main
    let key = system_kv_key(main, "recipe:default");
    db.put(key, Value::String(r#"{"version":1}"#.into())).unwrap();

    // Fork
    let child = db.branch_create("child", main).unwrap();

    // _system_ data visible on child
    let val = db.get(&system_kv_key(child, "recipe:default")).unwrap();
    assert!(val.is_some());
    assert_eq!(val.unwrap().as_str(), Some(r#"{"version":1}"#));
}

#[test]
fn system_space_override_on_fork() {
    let db = Database::cache().unwrap();
    let main = db.default_branch();

    // Write to _system_ on main
    db.put(system_kv_key(main, "recipe:default"), Value::String("v1".into())).unwrap();

    // Fork
    let child = db.branch_create("child", main).unwrap();

    // Override on child
    db.put(system_kv_key(child, "recipe:default"), Value::String("v2".into())).unwrap();

    // Main unchanged
    let main_val = db.get(&system_kv_key(main, "recipe:default")).unwrap();
    assert_eq!(main_val.unwrap().as_str(), Some("v1"));

    // Child has override
    let child_val = db.get(&system_kv_key(child, "recipe:default")).unwrap();
    assert_eq!(child_val.unwrap().as_str(), Some("v2"));
}

#[test]
fn system_space_invisible_to_kv_list() {
    let db = Database::cache().unwrap();
    let branch = db.default_branch();

    // Write to _system_ space
    db.put(system_kv_key(branch, "secret"), Value::String("hidden".into())).unwrap();

    // Write to default space
    db.kv.put(&branch, "default", "visible", Value::String("shown".into())).unwrap();

    // List on default space should NOT include _system_ entries
    let keys = db.kv.list(&branch, "default", None).unwrap();
    assert!(keys.contains(&"visible".to_string()));
    assert!(!keys.contains(&"secret".to_string()));
}

#[test]
fn system_space_cannot_be_deleted() {
    let result = executor.space_delete("_system_");
    assert!(result.is_err());
}

#[test]
fn system_space_exists_always() {
    let db = Database::cache().unwrap();
    let branch = db.default_branch();
    assert!(db.space.exists(branch, "_system_").unwrap());
}
```

### 6.3 Regression

All existing tests must pass. The `_system_` space adds no new behavior to existing operations — it's invisible to user-facing APIs. The only way existing tests could break is if `SpaceIndex.list()` or `validate_space_name()` changes affect something unexpected. Run the full test suite.

---

## 7. Acceptance Criteria

1. `system_namespace(branch_id)` returns a cached Arc<Namespace> for the `_system_` space
2. `system_kv_key(branch_id, key)` constructs a Key in the `_system_` space
3. Internal writes to `_system_` space via Database succeed
4. User writes to `_system_` space via executor are rejected (already works via validation)
5. `SpaceIndex.list()` does not include `_system_`
6. `SpaceIndex.exists(branch_id, "_system_")` returns true
7. `_system_` space cannot be deleted
8. Fork a branch → `_system_` data inherited via COW
9. Override `_system_` data on fork → parent unaffected
10. `_system_` space data invisible to `db.kv.list()` in other spaces (automatic from key encoding)
11. All existing tests pass
