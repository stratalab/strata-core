pub(crate) mod arrow_import;
pub(crate) mod branch;
pub(crate) mod config;
pub(crate) mod configure_model;
pub(crate) mod database;
pub(crate) mod embed;
pub(crate) mod embed_runtime;
pub(crate) mod event;
pub(crate) mod export;
pub(crate) mod generate;
pub(crate) mod graph;
pub(crate) mod json;
pub(crate) mod kv;
pub(crate) mod maintenance;
pub(crate) mod models;
pub(crate) mod recipe;
pub(crate) mod search;
pub(crate) mod space;
pub(crate) mod space_delete;
pub(crate) mod vector;

use crate::{BranchId, Error, Result};

pub(crate) fn reject_system_branch(branch: &BranchId) -> Result<()> {
    if branch.as_str().starts_with("_system") {
        return Err(Error::InvalidInput {
            reason: format!("Branch '{}' is reserved for system use", branch.as_str()),
            hint: Some(
                "Branches starting with '_system' are internal and cannot be accessed directly."
                    .to_string(),
            ),
        });
    }
    Ok(())
}

pub(crate) fn sample_indices(total: usize, count: usize) -> Vec<usize> {
    if total == 0 || count == 0 {
        return Vec::new();
    }
    if count >= total {
        return (0..total).collect();
    }

    let step = total as f64 / count as f64;
    (0..count)
        .map(|index| (index as f64 * step) as usize)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::sample_indices;

    #[test]
    fn sample_indices_handles_empty_inputs() {
        assert!(sample_indices(0, 5).is_empty());
        assert!(sample_indices(5, 0).is_empty());
    }

    #[test]
    fn sample_indices_covers_small_ranges() {
        assert_eq!(sample_indices(3, 10), vec![0, 1, 2]);
        assert_eq!(sample_indices(5, 5), vec![0, 1, 2, 3, 4]);
    }
}
