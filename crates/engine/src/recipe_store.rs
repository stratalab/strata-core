//! Recipe storage in the `_system_` space.
//!
//! Recipes are stored as JSON strings under `recipe:{name}` keys in the
//! per-branch `_system_` space. The "default" recipe is auto-created with
//! built-in defaults on first access.

use crate::search::recipe::{builtin_defaults, Recipe};
use crate::system_space::system_kv_key;
use crate::Database;
use strata_core::types::BranchId;
use strata_core::value::Value;
use strata_core::StrataResult;

/// Store a named recipe on a branch.
pub fn set_recipe(
    db: &Database,
    branch_id: BranchId,
    name: &str,
    recipe: &Recipe,
) -> StrataResult<()> {
    let key = system_kv_key(branch_id, &format!("recipe:{name}"));
    let json = serde_json::to_string(recipe)?;
    db.transaction(branch_id, |txn| {
        txn.put(key.clone(), Value::String(json.clone()))
    })?;
    Ok(())
}

/// Retrieve a named recipe from a branch. Returns `None` if not found.
pub fn get_recipe(db: &Database, branch_id: BranchId, name: &str) -> StrataResult<Option<Recipe>> {
    let key = system_kv_key(branch_id, &format!("recipe:{name}"));
    match db.get_value_direct(&key)? {
        Some(Value::String(s)) => {
            let recipe: Recipe = serde_json::from_str(&s)?;
            Ok(Some(recipe))
        }
        _ => Ok(None),
    }
}

/// Get the default recipe, auto-creating it with built-in defaults if absent.
pub fn get_default_recipe(db: &Database, branch_id: BranchId) -> StrataResult<Recipe> {
    match get_recipe(db, branch_id, "default")? {
        Some(r) => Ok(r),
        None => {
            let defaults = builtin_defaults();
            set_recipe(db, branch_id, "default", &defaults)?;
            Ok(defaults)
        }
    }
}

/// List all recipe names on a branch.
pub fn list_recipes(db: &Database, branch_id: BranchId) -> StrataResult<Vec<String>> {
    use strata_core::traits::Storage;

    let prefix = system_kv_key(branch_id, "recipe:");
    let version = db.storage().version();
    let entries = db.storage().scan_prefix(&prefix, version)?;

    let mut names: Vec<String> = entries
        .into_iter()
        .filter_map(|(k, _)| {
            k.user_key_string()
                .and_then(|s| s.strip_prefix("recipe:").map(|n| n.to_string()))
        })
        .collect();
    names.sort();
    Ok(names)
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
    fn test_get_default_recipe_auto_creates() {
        let (_dir, db) = setup_db();
        let bid = BranchId::new();

        // First call auto-creates
        let r1 = get_default_recipe(&db, bid).unwrap();
        assert_eq!(r1, builtin_defaults());

        // Second call returns the stored one
        let r2 = get_default_recipe(&db, bid).unwrap();
        assert_eq!(r1, r2);
    }

    #[test]
    fn test_list_recipes() {
        let (_dir, db) = setup_db();
        let bid = BranchId::new();

        set_recipe(&db, bid, "alpha", &builtin_defaults()).unwrap();
        set_recipe(&db, bid, "beta", &Recipe::default()).unwrap();

        let names = list_recipes(&db, bid).unwrap();
        assert_eq!(names, vec!["alpha", "beta"]);
    }

    #[test]
    fn test_recipe_branch_isolation() {
        let (_dir, db) = setup_db();
        let b1 = BranchId::new();
        let b2 = BranchId::new();

        set_recipe(&db, b1, "private", &builtin_defaults()).unwrap();

        // Not visible on other branch
        assert!(get_recipe(&db, b2, "private").unwrap().is_none());
    }

    #[test]
    fn test_recipe_overwrite() {
        let (_dir, db) = setup_db();
        let bid = BranchId::new();

        let v1 = Recipe {
            version: Some(1),
            ..Default::default()
        };
        let v2 = Recipe {
            version: Some(2),
            ..Default::default()
        };

        set_recipe(&db, bid, "test", &v1).unwrap();
        set_recipe(&db, bid, "test", &v2).unwrap();

        let loaded = get_recipe(&db, bid, "test").unwrap().unwrap();
        assert_eq!(loaded.version, Some(2), "Last write should win");
    }
}
