//! Model download utility — thin wrapper around strata-inference's ModelRegistry.
//!
//! All four delivery surfaces (CLI, MCP, Python, Node) call [`ensure_model`]
//! to guarantee model files are present before embedding.

use std::path::PathBuf;

use strata_inference::ModelRegistry;

/// Ensure model files are present, downloading if necessary.
///
/// Delegates to `ModelRegistry::pull()` which downloads a GGUF model file
/// from HuggingFace into `~/.strata/models/`. Returns the path to the
/// downloaded model file.
pub fn ensure_model() -> Result<PathBuf, String> {
    let registry = ModelRegistry::new();
    registry
        .pull(super::DEFAULT_MODEL)
        .map_err(|e| format!("Failed to download model: {}", e))
}

/// Returns the system-wide models directory: `~/.strata/models/`.
pub fn system_model_dir() -> PathBuf {
    ModelRegistry::new().models_dir().to_path_buf()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_system_model_dir_under_home() {
        let dir = system_model_dir();
        let s = dir.to_string_lossy();
        assert!(
            s.contains("models"),
            "expected 'models' in path, got: {}",
            s
        );
    }
}
