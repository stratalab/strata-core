//! Recipe command handlers.

use std::sync::Arc;

use crate::bridge::{to_core_branch_id, Primitives};
use crate::types::BranchId;
use crate::{Error, Output, Result, Value};

use strata_engine::recipe_store;
use strata_engine::search::recipe::Recipe;

/// Set a named recipe on a branch.
pub fn recipe_set(
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
    recipe_store::set_recipe(&p.db, branch_id, &name, &recipe).map_err(|e| Error::Internal {
        reason: e.to_string(),
        hint: None,
    })?;
    Ok(Output::Unit)
}

/// Get a named recipe from a branch.
pub fn recipe_get(p: &Arc<Primitives>, branch: BranchId, name: String) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    let recipe =
        recipe_store::get_recipe(&p.db, branch_id, &name).map_err(|e| Error::Internal {
            reason: e.to_string(),
            hint: None,
        })?;
    match recipe {
        Some(r) => {
            let json = serde_json::to_string(&r).map_err(|e| Error::Internal {
                reason: e.to_string(),
                hint: None,
            })?;
            Ok(Output::Maybe(Some(Value::String(json.into()))))
        }
        None => Ok(Output::Maybe(None)),
    }
}

/// Get the default recipe, auto-creating with built-in defaults if absent.
pub fn recipe_get_default(p: &Arc<Primitives>, branch: BranchId) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    let recipe =
        recipe_store::get_default_recipe(&p.db, branch_id).map_err(|e| Error::Internal {
            reason: e.to_string(),
            hint: None,
        })?;
    let json = serde_json::to_string(&recipe).map_err(|e| Error::Internal {
        reason: e.to_string(),
        hint: None,
    })?;
    Ok(Output::Maybe(Some(Value::String(json.into()))))
}

/// List all recipe names on a branch.
pub fn recipe_list(p: &Arc<Primitives>, branch: BranchId) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    let names = recipe_store::list_recipes(&p.db, branch_id).map_err(|e| Error::Internal {
        reason: e.to_string(),
        hint: None,
    })?;
    Ok(Output::Keys(names))
}
