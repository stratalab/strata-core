//! Integration tests for Recipe commands.

use crate::{Command, Executor, Output, Value};
use strata_engine::database::search_only_cache_spec;
use strata_engine::Database;
use strata_security::AccessMode;

fn create_test_executor() -> Executor {
    let db = Database::open_runtime(search_only_cache_spec()).unwrap();
    Executor::new(db)
}

#[test]
fn test_recipe_set_get_command() {
    let executor = create_test_executor();

    let recipe_json = r#"{"version":1,"retrieve":{"bm25":{"k1":1.5}}}"#;

    // Set
    let result = executor.execute(Command::RecipeSet {
        branch: None,
        name: "test".to_string(),
        recipe_json: recipe_json.to_string(),
    });
    assert!(matches!(result, Ok(Output::Unit)));

    // Get
    let result = executor.execute(Command::RecipeGet {
        branch: None,
        name: "test".to_string(),
    });
    match result {
        Ok(Output::Maybe(Some(Value::String(json)))) => {
            assert!(json.contains("1.5"), "Should contain k1=1.5, got: {json}");
        }
        other => panic!("Expected Maybe(Some(String)), got: {other:?}"),
    }
}

#[test]
fn test_recipe_get_default_command() {
    let executor = create_test_executor();

    let result = executor.execute(Command::RecipeGetDefault { branch: None });
    match result {
        Ok(Output::Maybe(Some(Value::String(json)))) => {
            // Should contain builtin default values
            assert!(json.contains("0.9"), "Should contain k1=0.9, got: {json}");
            assert!(json.contains("0.4"), "Should contain b=0.4, got: {json}");
        }
        other => panic!("Expected Maybe(Some(String)), got: {other:?}"),
    }
}

#[test]
fn test_recipe_list_command() {
    let executor = create_test_executor();

    // Set two recipes
    executor
        .execute(Command::RecipeSet {
            branch: None,
            name: "alpha".to_string(),
            recipe_json: "{}".to_string(),
        })
        .unwrap();
    executor
        .execute(Command::RecipeSet {
            branch: None,
            name: "beta".to_string(),
            recipe_json: "{}".to_string(),
        })
        .unwrap();

    let result = executor.execute(Command::RecipeList { branch: None });
    match result {
        Ok(Output::Keys(names)) => {
            assert!(names.contains(&"alpha".to_string()));
            assert!(names.contains(&"beta".to_string()));
        }
        other => panic!("Expected Keys, got: {other:?}"),
    }
}

#[test]
fn test_recipe_set_invalid_json() {
    let executor = create_test_executor();

    let result = executor.execute(Command::RecipeSet {
        branch: None,
        name: "bad".to_string(),
        recipe_json: "not valid json".to_string(),
    });
    assert!(result.is_err(), "Should reject invalid JSON");
}

#[test]
fn test_recipe_get_nonexistent() {
    let executor = create_test_executor();

    let result = executor.execute(Command::RecipeGet {
        branch: None,
        name: "nonexistent".to_string(),
    });
    assert!(matches!(result, Ok(Output::Maybe(None))));
}

// ==================== T2-E4: Bootstrap and Recipe Seeding Split ====================

#[test]
fn test_recipe_seed_command() {
    let executor = create_test_executor();

    // Before seeding, list should be empty (open doesn't seed recipes)
    let result = executor.execute(Command::RecipeList { branch: None });
    match result {
        Ok(Output::Keys(names)) => {
            assert!(names.is_empty(), "Recipes should not be seeded on open");
        }
        other => panic!("Expected empty Keys, got: {other:?}"),
    }

    // Seed via command
    let result = executor.execute(Command::RecipeSeed);
    assert!(matches!(result, Ok(Output::Unit)));

    // After seeding, list should contain all 6 built-in recipes
    let result = executor.execute(Command::RecipeList { branch: None });
    match result {
        Ok(Output::Keys(names)) => {
            assert_eq!(names.len(), 6, "Should have 6 built-in recipes");
            assert!(names.contains(&"default".to_string()));
            assert!(names.contains(&"keyword".to_string()));
            assert!(names.contains(&"semantic".to_string()));
            assert!(names.contains(&"hybrid".to_string()));
            assert!(names.contains(&"graph".to_string()));
            assert!(names.contains(&"rag".to_string()));
        }
        other => panic!("Expected Keys with 6 items, got: {other:?}"),
    }
}

#[test]
fn test_seed_builtin_recipes_idempotent() {
    let executor = create_test_executor();

    // Seed twice
    executor.execute(Command::RecipeSeed).unwrap();
    executor.execute(Command::RecipeSeed).unwrap();

    // Should still have exactly 6 recipes
    let result = executor.execute(Command::RecipeList { branch: None });
    match result {
        Ok(Output::Keys(names)) => {
            assert_eq!(names.len(), 6, "Should have exactly 6 built-in recipes");
        }
        other => panic!("Expected Keys with 6 items, got: {other:?}"),
    }
}

#[test]
fn test_engine_open_does_not_seed_recipes() {
    // Engine-level open (Database::open_runtime) does NOT seed recipes.
    // Recipe seeding is product bootstrap, done by Strata::open().
    let executor = create_test_executor(); // Uses search_only_cache_spec()

    // List should be empty - engine open doesn't seed
    let result = executor.execute(Command::RecipeList { branch: None });
    match result {
        Ok(Output::Keys(names)) => {
            assert!(
                names.is_empty(),
                "Engine open should NOT seed recipes, but found: {:?}",
                names
            );
        }
        other => panic!("Expected empty Keys, got: {other:?}"),
    }
}

#[test]
fn test_recipe_seed_blocked_in_read_only_mode() {
    let db = Database::open_runtime(search_only_cache_spec()).unwrap();
    let executor = Executor::new_with_mode(db, AccessMode::ReadOnly);

    let result = executor.execute(Command::RecipeSeed);
    assert!(
        result.is_err(),
        "RecipeSeed should be rejected in read-only mode"
    );

    // Verify error message mentions read-only
    let err = result.unwrap_err();
    let err_str = format!("{}", err);
    assert!(
        err_str.contains("read-only") || err_str.contains("ReadOnly"),
        "Error should mention read-only mode: {}",
        err_str
    );
}

#[test]
fn test_named_recipes_require_seeding() {
    // Without seeding, named recipes (other than "default") are not available.
    // This tests engine-level open which doesn't auto-seed.
    let executor = create_test_executor();

    // Named recipes should return None (not seeded)
    for name in ["keyword", "semantic", "hybrid", "graph", "rag"] {
        let result = executor.execute(Command::RecipeGet {
            branch: None,
            name: name.to_string(),
        });
        assert!(
            matches!(result, Ok(Output::Maybe(None))),
            "Recipe '{}' should not be available without seeding, got: {:?}",
            name,
            result
        );
    }

    // After explicit seeding, all recipes are available
    executor.execute(Command::RecipeSeed).unwrap();

    for name in ["keyword", "semantic", "hybrid", "graph", "rag"] {
        let result = executor.execute(Command::RecipeGet {
            branch: None,
            name: name.to_string(),
        });
        match result {
            Ok(Output::Maybe(Some(Value::String(json)))) => {
                assert!(
                    json.contains("version"),
                    "Recipe '{}' should have version field after seeding, got: {}",
                    name,
                    json
                );
            }
            other => panic!(
                "Expected recipe '{}' to be available after seeding, got: {:?}",
                name, other
            ),
        }
    }
}
