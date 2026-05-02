//! Recipe command handlers.

use std::sync::Arc;

use crate::bridge::{to_core_branch_id, Primitives};
use crate::types::BranchId;
use crate::{Error, Output, Result, Value};

use strata_engine::recipe_store;
use strata_engine::search::recipe::Recipe;

fn recipe_failed(operation: &str, error: impl ToString) -> Error {
    Error::RecipeOperationFailed {
        operation: operation.to_string(),
        reason: error.to_string(),
        hint: None,
    }
}

/// Set a named recipe on a branch.
pub(crate) fn recipe_set(
    p: &Arc<Primitives>,
    branch: BranchId,
    name: String,
    recipe_json: String,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    let recipe: Recipe = serde_json::from_str(&recipe_json).map_err(|e| Error::InvalidInput {
        reason: format!("Invalid recipe JSON: {e}"),
        hint: None,
    })?;
    recipe_store::set_recipe(&p.db, branch_id, &name, &recipe).map_err(|e| {
        if e.to_string().contains("Cannot overwrite built-in") {
            Error::InvalidInput {
                reason: e.to_string(),
                hint: Some("Built-in recipes on _system_ branch are read-only".into()),
            }
        } else {
            recipe_failed("set", e)
        }
    })?;
    Ok(Output::Unit)
}

/// Get a named recipe from a branch.
pub(crate) fn recipe_get(p: &Arc<Primitives>, branch: BranchId, name: String) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    let recipe =
        recipe_store::get_recipe(&p.db, branch_id, &name).map_err(|e| recipe_failed("get", e))?;
    match recipe {
        Some(r) => {
            let json = serde_json::to_string(&r).map_err(|e| Error::Serialization {
                reason: format!("Failed to serialize recipe: {e}"),
            })?;
            Ok(Output::Maybe(Some(Value::String(json))))
        }
        None => Ok(Output::Maybe(None)),
    }
}

/// Get the default recipe, auto-creating with built-in defaults if absent.
pub(crate) fn recipe_get_default(p: &Arc<Primitives>, branch: BranchId) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    let recipe = recipe_store::get_default_recipe(&p.db, branch_id)
        .map_err(|e| recipe_failed("get default", e))?;
    let json = serde_json::to_string(&recipe).map_err(|e| Error::Serialization {
        reason: format!("Failed to serialize recipe: {e}"),
    })?;
    Ok(Output::Maybe(Some(Value::String(json))))
}

/// Delete a named recipe from a branch.
pub(crate) fn recipe_delete(p: &Arc<Primitives>, branch: BranchId, name: String) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    recipe_store::delete_recipe(&p.db, branch_id, &name).map_err(|e| {
        // InvalidInput from recipe_store means built-in protection
        if e.to_string().contains("Cannot delete built-in") {
            Error::InvalidInput {
                reason: e.to_string(),
                hint: Some("Built-in recipes on _system_ branch are read-only".into()),
            }
        } else {
            recipe_failed("delete", e)
        }
    })?;
    Ok(Output::Unit)
}

/// List all recipe names on a branch.
pub(crate) fn recipe_list(p: &Arc<Primitives>, branch: BranchId) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    let names =
        recipe_store::list_recipes(&p.db, branch_id).map_err(|e| recipe_failed("list", e))?;
    Ok(Output::Keys(names))
}

/// Seed built-in recipes to the `_system_` branch.
///
/// Idempotent: safe to call multiple times.
pub(crate) fn recipe_seed(p: &Arc<Primitives>) -> Result<Output> {
    recipe_store::seed_builtin_recipes(&p.db).map_err(|e| recipe_failed("seed", e))?;
    Ok(Output::Unit)
}
