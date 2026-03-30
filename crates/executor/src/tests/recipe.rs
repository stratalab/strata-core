//! Integration tests for Recipe commands.

use crate::{Command, Executor, Output, Value};
use strata_engine::Database;

fn create_test_executor() -> Executor {
    let db = Database::cache().unwrap();
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
