//! Recipe storage in the `_system_` space.
//!
//! Built-in recipes live on the `_system_` branch (seeded at database creation).
//! User recipes live on the user's branch in `_system_` space.
//! Lookup order: user branch → `_system_` branch fallback.

use crate::primitives::branch::resolve_branch_name;
use crate::search::recipe::{builtin_defaults, get_builtin_recipe, Recipe, BUILTIN_RECIPE_NAMES};
use crate::system_space::system_kv_key;
use crate::SYSTEM_BRANCH;
use crate::{Database, StrataError, StrataResult};
use strata_core::types::BranchId;
use strata_core::value::Value;

/// The branch ID for `_system_` where built-in recipes are stored.
fn system_branch_id() -> BranchId {
    resolve_branch_name(SYSTEM_BRANCH)
}

/// Seed all built-in recipes onto the `_system_` branch.
///
/// Only writes recipes that don't already exist on `_system_`.
/// Safe to call on every open — skips seeding if recipes are present.
pub fn seed_builtin_recipes(db: &Database) -> StrataResult<()> {
    use crate::search::recipe::builtin_recipes;
    let sys_branch = system_branch_id();

    // Quick check: if "default" exists on _system_, assume all built-ins are seeded.
    let check_key = system_kv_key(sys_branch, "recipe:default");
    if db.get_value_direct(&check_key)?.is_some() {
        return Ok(());
    }

    for (name, recipe) in builtin_recipes() {
        let key = system_kv_key(sys_branch, &format!("recipe:{name}"));
        let json = serde_json::to_string(&recipe)?;
        db.transaction(sys_branch, |txn| {
            txn.put(key.clone(), Value::String(json.clone()))
        })?;
    }
    Ok(())
}

/// Store a named recipe on a branch.
///
/// If the target is the `_system_` branch and the name is a built-in,
/// returns an error — built-in recipes are read-only.
pub fn set_recipe(
    db: &Database,
    branch_id: BranchId,
    name: &str,
    recipe: &Recipe,
) -> StrataResult<()> {
    // Protect built-ins on _system_ branch
    if branch_id == system_branch_id() && BUILTIN_RECIPE_NAMES.contains(&name) {
        return Err(StrataError::InvalidInput {
            message: format!("Cannot overwrite built-in recipe '{name}' on _system_ branch"),
        });
    }
    let key = system_kv_key(branch_id, &format!("recipe:{name}"));
    let json = serde_json::to_string(recipe)?;
    db.transaction(branch_id, |txn| {
        txn.put(key.clone(), Value::String(json.clone()))
    })?;
    Ok(())
}

/// Delete a named recipe from a branch.
///
/// If the target is the `_system_` branch and the name is a built-in,
/// returns an error. Deleting a user shadow restores the built-in fallback.
pub fn delete_recipe(db: &Database, branch_id: BranchId, name: &str) -> StrataResult<()> {
    if branch_id == system_branch_id() && BUILTIN_RECIPE_NAMES.contains(&name) {
        return Err(StrataError::InvalidInput {
            message: format!("Cannot delete built-in recipe '{name}' from _system_ branch"),
        });
    }
    let key = system_kv_key(branch_id, &format!("recipe:{name}"));
    db.transaction(branch_id, |txn| txn.delete(key.clone()))?;
    Ok(())
}

/// Retrieve a named recipe. Checks user branch first, falls back to `_system_` branch.
pub fn get_recipe(db: &Database, branch_id: BranchId, name: &str) -> StrataResult<Option<Recipe>> {
    // 1. Check user's branch
    let key = system_kv_key(branch_id, &format!("recipe:{name}"));
    if let Some(Value::String(s)) = db.get_value_direct(&key)? {
        let recipe: Recipe = serde_json::from_str(&s)?;
        return Ok(Some(recipe));
    }

    // 2. Fall back to _system_ branch (if not already on it)
    let sys_branch = system_branch_id();
    if branch_id != sys_branch {
        let sys_key = system_kv_key(sys_branch, &format!("recipe:{name}"));
        if let Some(Value::String(s)) = db.get_value_direct(&sys_key)? {
            let recipe: Recipe = serde_json::from_str(&s)?;
            return Ok(Some(recipe));
        }
    }

    Ok(None)
}

/// Get the default recipe. Always succeeds (built-in "default" is always available).
pub fn get_default_recipe(db: &Database, branch_id: BranchId) -> StrataResult<Recipe> {
    match get_recipe(db, branch_id, "default")? {
        Some(r) => Ok(r),
        // Fallback: if _system_ branch hasn't been seeded yet (e.g., legacy DB),
        // return the in-memory built-in default.
        None => Ok(get_builtin_recipe("default").unwrap_or_else(builtin_defaults)),
    }
}

/// List all recipe names available to a branch (user + system, deduplicated).
pub fn list_recipes(db: &Database, branch_id: BranchId) -> StrataResult<Vec<String>> {
    use std::collections::BTreeSet;
    use strata_core::id::CommitVersion;

    let mut names = BTreeSet::new();

    // User branch recipes
    let prefix = system_kv_key(branch_id, "recipe:");
    let version = CommitVersion(db.storage().version());
    for (k, _) in db.storage().scan_prefix(&prefix, version)? {
        if let Some(n) = k
            .user_key_string()
            .and_then(|s| s.strip_prefix("recipe:").map(|n| n.to_string()))
        {
            names.insert(n);
        }
    }

    // _system_ branch recipes (includes built-ins)
    let sys_branch = system_branch_id();
    if branch_id != sys_branch {
        let sys_prefix = system_kv_key(sys_branch, "recipe:");
        for (k, _) in db.storage().scan_prefix(&sys_prefix, version)? {
            if let Some(n) = k
                .user_key_string()
                .and_then(|s| s.strip_prefix("recipe:").map(|n| n.to_string()))
            {
                names.insert(n);
            }
        }
    }

    Ok(names.into_iter().collect())
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_db() -> (tempfile::TempDir, std::sync::Arc<Database>) {
        let dir = tempfile::TempDir::new().unwrap();
        let db = Database::open(dir.path()).unwrap();
        (dir, db)
    }

    #[test]
    fn test_set_get_recipe_roundtrip() {
        let (_dir, db) = setup_db();
        let bid = BranchId::new();

        let recipe = Recipe {
            version: Some(1),
            retrieve: Some(crate::search::recipe::RetrieveConfig {
                bm25: Some(crate::search::recipe::BM25Config {
                    k1: Some(1.5),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };

        set_recipe(&db, bid, "test", &recipe).unwrap();
        let loaded = get_recipe(&db, bid, "test").unwrap();
        assert_eq!(loaded, Some(recipe));
    }

    #[test]
    fn test_get_recipe_not_found() {
        let (_dir, db) = setup_db();
        let bid = BranchId::new();
        let result = get_recipe(&db, bid, "nonexistent").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_get_default_recipe() {
        let (_dir, db) = setup_db();
        let bid = BranchId::new();

        // Returns built-in default even without seeding
        let r = get_default_recipe(&db, bid).unwrap();
        assert_eq!(r.version, Some(1));
    }

    #[test]
    fn test_seed_and_list_builtins() {
        let (_dir, db) = setup_db();

        // Seed built-ins on _system_ branch
        seed_builtin_recipes(&db).unwrap();

        // List from _system_ branch should include all built-ins
        let sys = system_branch_id();
        let names = list_recipes(&db, sys).unwrap();
        for expected in BUILTIN_RECIPE_NAMES {
            assert!(
                names.contains(&expected.to_string()),
                "Missing built-in: {}",
                expected
            );
        }
    }

    #[test]
    fn test_fallback_to_system_branch() {
        let (_dir, db) = setup_db();

        // Seed built-ins
        seed_builtin_recipes(&db).unwrap();

        // User branch should find "keyword" via fallback
        let user_branch = BranchId::new();
        let recipe = get_recipe(&db, user_branch, "keyword").unwrap();
        assert!(recipe.is_some(), "Should fall back to _system_ branch");
        // keyword recipe has no expansion
        assert!(recipe.unwrap().expansion.is_none());
    }

    #[test]
    fn test_user_shadows_builtin() {
        let (_dir, db) = setup_db();
        seed_builtin_recipes(&db).unwrap();

        let user_branch = BranchId::new();
        let custom = Recipe {
            version: Some(99),
            ..Default::default()
        };
        set_recipe(&db, user_branch, "keyword", &custom).unwrap();

        // User version wins
        let loaded = get_recipe(&db, user_branch, "keyword").unwrap().unwrap();
        assert_eq!(loaded.version, Some(99));
    }

    #[test]
    fn test_delete_shadow_restores_builtin() {
        let (_dir, db) = setup_db();
        seed_builtin_recipes(&db).unwrap();

        let user_branch = BranchId::new();
        let custom = Recipe {
            version: Some(99),
            ..Default::default()
        };
        set_recipe(&db, user_branch, "keyword", &custom).unwrap();

        // Delete shadow
        delete_recipe(&db, user_branch, "keyword").unwrap();

        // Falls back to built-in (keyword has no expansion)
        let loaded = get_recipe(&db, user_branch, "keyword").unwrap().unwrap();
        assert!(
            loaded.expansion.is_none(),
            "Should be the built-in keyword recipe (no expansion)"
        );
    }

    #[test]
    fn test_cannot_overwrite_builtin_on_system() {
        let (_dir, db) = setup_db();
        seed_builtin_recipes(&db).unwrap();

        let sys = system_branch_id();
        let result = set_recipe(&db, sys, "keyword", &Recipe::default());
        assert!(result.is_err());
    }

    #[test]
    fn test_cannot_delete_builtin_on_system() {
        let (_dir, db) = setup_db();
        seed_builtin_recipes(&db).unwrap();

        let sys = system_branch_id();
        let result = delete_recipe(&db, sys, "keyword");
        assert!(result.is_err());
    }

    #[test]
    fn test_list_includes_user_and_system() {
        let (_dir, db) = setup_db();
        seed_builtin_recipes(&db).unwrap();

        let user_branch = BranchId::new();
        set_recipe(&db, user_branch, "my_custom", &Recipe::default()).unwrap();

        let names = list_recipes(&db, user_branch).unwrap();
        assert!(names.contains(&"keyword".to_string())); // from system
        assert!(names.contains(&"my_custom".to_string())); // from user
    }

    #[test]
    fn test_recipe_branch_isolation() {
        let (_dir, db) = setup_db();
        let b1 = BranchId::new();
        let b2 = BranchId::new();

        set_recipe(&db, b1, "private", &builtin_defaults()).unwrap();

        // Not visible on other branch (direct lookup, not via _system_ fallback)
        let key = system_kv_key(b2, "recipe:private");
        assert!(db.get_value_direct(&key).unwrap().is_none());
    }
}
