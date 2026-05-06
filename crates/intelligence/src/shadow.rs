use std::sync::Arc;

use strata_core::BranchId;
pub use strata_engine::database::{SHADOW_EVENT, SHADOW_JSON, SHADOW_KV};
use strata_engine::{Database, VectorStore};

const SHADOW_KEY_SEP: char = '\x1f';

pub fn maybe_remove_embedding(
    db: &Arc<Database>,
    branch_id: BranchId,
    space: &str,
    shadow_collection: &str,
    key: &str,
) {
    let composite_key = format!("{}{}{}", space, SHADOW_KEY_SEP, key);
    let vector = VectorStore::new(db.clone());
    if let Err(error) = vector.system_delete(branch_id, shadow_collection, &composite_key) {
        if !error.is_not_found() {
            tracing::warn!(
                target: "strata::embed",
                collection = shadow_collection,
                key = composite_key,
                error = %error,
                "Failed to remove shadow embedding"
            );
        }
    }
}

pub fn delete_shadow_embeddings_for_space(
    db: &Arc<Database>,
    branch_id: BranchId,
    target_space: &str,
) -> usize {
    let prefix = format!("{target_space}\x1f");
    let vector = VectorStore::new(db.clone());
    let mut total = 0usize;
    for shadow in [SHADOW_KV, SHADOW_JSON, SHADOW_EVENT] {
        let keys =
            match vector.list_keys(branch_id, strata_engine::system_space::SYSTEM_SPACE, shadow) {
                Ok(keys) => keys,
                Err(error) if error.is_not_found() => continue,
                Err(error) => {
                    tracing::warn!(
                        target: "strata::embed",
                        shadow = shadow,
                        error = %error,
                        "Failed to list shadow keys during space cleanup"
                    );
                    continue;
                }
            };

        for key in keys {
            if !key.starts_with(&prefix) {
                continue;
            }
            match vector.system_delete(branch_id, shadow, &key) {
                Ok(_) => total += 1,
                Err(error) if error.is_not_found() => {}
                Err(error) => {
                    tracing::warn!(
                        target: "strata::embed",
                        shadow = shadow,
                        key = key.as_str(),
                        error = %error,
                        "Failed to delete shadow embedding during space cleanup"
                    );
                }
            }
        }
    }

    total
}
