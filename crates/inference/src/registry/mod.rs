//! Model registry for resolving friendly model names to local GGUF file paths.
//!
//! The registry maps names like `"miniLM"` or `"qwen3:8b"` to HuggingFace GGUF
//! files, manages a local download cache, and provides offline-friendly error
//! messages when models aren't available.
//!
//! # Name Format
//!
//! Names follow `name`, `name:size`, or `name:size:quant`:
//! - `"miniLM"` → catalog entry "miniLM", default quant
//! - `"qwen3:8b"` → catalog entry "qwen3:8b", default quant (q4_k_m)
//! - `"qwen3:8b:q6_k"` → catalog entry "qwen3:8b", variant q6_k
//!
//! # Models Directory
//!
//! Models are stored in (in priority order):
//! 1. `STRATA_MODELS_DIR` environment variable (if set)
//! 2. `~/.strata/models/` (default)

pub mod catalog;

#[cfg(feature = "download")]
pub mod download;

use std::path::{Path, PathBuf};

use crate::error::InferenceError;

/// What a model is designed for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelTask {
    Embed,
    Generate,
}

impl std::fmt::Display for ModelTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ModelTask::Embed => write!(f, "embed"),
            ModelTask::Generate => write!(f, "generate"),
        }
    }
}

/// A quantization variant of a catalog model.
#[derive(Debug, Clone)]
pub struct QuantVariant {
    /// Quant name: "q4_k_m", "q8_0", "f16"
    pub name: &'static str,
    /// Filename on HuggingFace: "Qwen3-8B-Q4_K_M.gguf"
    pub hf_file: &'static str,
    /// Approximate file size in bytes.
    pub size_bytes: u64,
}

/// A model entry in the static catalog.
#[derive(Debug, Clone)]
pub struct CatalogEntry {
    /// Primary name: "qwen3:8b"
    pub name: &'static str,
    /// Alternative names: ["qwen3-8b"]
    pub aliases: &'static [&'static str],
    /// Task type.
    pub task: ModelTask,
    /// HuggingFace repository: "Qwen/Qwen3-8B-GGUF"
    pub hf_repo: &'static str,
    /// Default quant variant: "q4_k_m"
    pub default_quant: &'static str,
    /// Available quant variants.
    pub variants: &'static [QuantVariant],
    /// Model architecture: "qwen3", "llama", "bert"
    pub architecture: &'static str,
    /// Embedding dimension (0 for generation models).
    pub embedding_dim: usize,
}

/// Information about a resolved model.
#[derive(Debug, Clone)]
pub struct ModelInfo {
    pub name: String,
    pub task: ModelTask,
    pub architecture: String,
    pub default_quant: String,
    pub embedding_dim: usize,
    pub is_local: bool,
    pub local_path: Option<PathBuf>,
    pub size_bytes: u64,
    pub hf_repo: String,
}

/// Model registry for resolving names to local GGUF file paths.
pub struct ModelRegistry {
    models_dir: PathBuf,
}

impl ModelRegistry {
    /// Create a registry using the default models directory.
    ///
    /// Resolution order:
    /// 1. `STRATA_MODELS_DIR` environment variable
    /// 2. `~/.strata/models/`
    pub fn new() -> Self {
        let models_dir = if let Ok(dir) = std::env::var("STRATA_MODELS_DIR") {
            PathBuf::from(dir)
        } else {
            dirs_default_models()
        };
        Self { models_dir }
    }

    /// Create a registry with a custom models directory.
    pub fn with_dir(dir: PathBuf) -> Self {
        Self { models_dir: dir }
    }

    /// The directory where models are stored.
    pub fn models_dir(&self) -> &Path {
        &self.models_dir
    }

    /// List all models in the catalog with their local availability.
    pub fn list_available(&self) -> Vec<ModelInfo> {
        catalog::CATALOG
            .iter()
            .map(|entry| self.entry_to_info(entry, entry.default_quant))
            .collect()
    }

    /// List only models that have at least one variant downloaded locally.
    pub fn list_local(&self) -> Vec<ModelInfo> {
        catalog::CATALOG
            .iter()
            .filter_map(|entry| {
                // Find the first locally-present variant
                let local_variant = entry
                    .variants
                    .iter()
                    .find(|v| self.models_dir.join(v.hf_file).exists());
                local_variant.map(|v| self.entry_to_info(entry, v.name))
            })
            .collect()
    }

    /// Resolve a model name to a local file path.
    ///
    /// Returns `Ok(path)` if the model file exists locally.
    /// Returns `Err` with a helpful message including download instructions
    /// if the model is not available locally.
    pub fn resolve(&self, name: &str) -> Result<PathBuf, InferenceError> {
        let (entry, quant) = self.parse_name(name)?;
        let quant_name = quant.unwrap_or(entry.default_quant);

        let variant = entry
            .variants
            .iter()
            .find(|v| v.name.eq_ignore_ascii_case(quant_name))
            .ok_or_else(|| {
                let available: Vec<&str> = entry.variants.iter().map(|v| v.name).collect();
                InferenceError::Registry(format!(
                    "Unknown quant '{}' for model '{}'. Available: {}",
                    quant_name,
                    entry.name,
                    available.join(", ")
                ))
            })?;

        let path = self.models_dir.join(variant.hf_file);

        if path.exists() {
            return Ok(path);
        }

        let size_display = format_size(variant.size_bytes);

        Err(InferenceError::Registry(format!(
            "Model '{}' not found locally.\n\n\
             To download it (requires internet):\n  \
             strata models pull {}\n\n\
             Or manually place the GGUF file at:\n  \
             {}\n\n\
             Expected file: {} ({})\n\
             Source: https://huggingface.co/{}",
            name,
            name,
            path.display(),
            variant.hf_file,
            size_display,
            entry.hf_repo
        )))
    }

    /// Download a model and return its local path.
    #[cfg(feature = "download")]
    pub fn pull(&self, name: &str) -> Result<PathBuf, InferenceError> {
        self.pull_with_progress(name, |_, _| {})
    }

    /// Download a model with a progress callback and return its local path.
    ///
    /// The callback receives `(bytes_downloaded, total_bytes)`.
    #[cfg(feature = "download")]
    pub fn pull_with_progress(
        &self,
        name: &str,
        cb: impl Fn(u64, u64),
    ) -> Result<PathBuf, InferenceError> {
        let (entry, quant) = self.parse_name(name)?;
        let quant_name = quant.unwrap_or(entry.default_quant);

        let variant = entry
            .variants
            .iter()
            .find(|v| v.name.eq_ignore_ascii_case(quant_name))
            .ok_or_else(|| {
                InferenceError::Registry(format!(
                    "Unknown quant '{}' for model '{}'",
                    quant_name, entry.name
                ))
            })?;

        download::download_hf_file(entry.hf_repo, variant.hf_file, &self.models_dir, &cb)?;

        Ok(self.models_dir.join(variant.hf_file))
    }

    /// Parse a model name string into a catalog entry and optional quant override.
    fn parse_name<'a>(
        &self,
        name: &'a str,
    ) -> Result<(&'static CatalogEntry, Option<&'a str>), InferenceError> {
        let parts: Vec<&str> = name.split(':').collect();

        catalog::find_entry_by_parts(&parts).ok_or_else(|| {
            InferenceError::Registry(format!(
                "Unknown model '{}'. Run `strata models list` to see available models.",
                name
            ))
        })
    }

    /// Convert a catalog entry to a ModelInfo with local availability check.
    fn entry_to_info(&self, entry: &CatalogEntry, quant_name: &str) -> ModelInfo {
        let variant = entry
            .variants
            .iter()
            .find(|v| v.name.eq_ignore_ascii_case(quant_name))
            .unwrap_or(&entry.variants[0]);

        let path = self.models_dir.join(variant.hf_file);
        let is_local = path.exists();

        ModelInfo {
            name: entry.name.to_string(),
            task: entry.task,
            architecture: entry.architecture.to_string(),
            default_quant: entry.default_quant.to_string(),
            embedding_dim: entry.embedding_dim,
            is_local,
            local_path: if is_local { Some(path) } else { None },
            size_bytes: variant.size_bytes,
            hf_repo: entry.hf_repo.to_string(),
        }
    }
}

impl Default for ModelRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Format bytes as a human-readable string (e.g., "4.7 GB").
pub fn format_size(bytes: u64) -> String {
    const GB: u64 = 1_000_000_000;
    const MB: u64 = 1_000_000;

    if bytes >= GB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.0} MB", bytes as f64 / MB as f64)
    } else {
        format!("{} bytes", bytes)
    }
}

/// Default models directory: `~/.strata/models/`
fn dirs_default_models() -> PathBuf {
    if let Some(home) = home_dir() {
        home.join(".strata").join("models")
    } else {
        PathBuf::from(".strata/models")
    }
}

/// Get the user's home directory.
fn home_dir() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Mutex to serialize tests that mutate STRATA_MODELS_DIR env var.
    // Required because Rust runs tests in parallel and env vars are global.
    use std::sync::Mutex;
    static ENV_MUTEX: Mutex<()> = Mutex::new(());

    fn test_registry() -> (tempfile::TempDir, ModelRegistry) {
        let dir = tempfile::tempdir().unwrap();
        let registry = ModelRegistry::with_dir(dir.path().to_path_buf());
        (dir, registry)
    }

    // ===== resolve() tests =====

    #[test]
    fn resolve_not_found_gives_helpful_error() {
        let (_dir, registry) = test_registry();

        let err = registry.resolve("miniLM").unwrap_err();
        let msg = format!("{}", err);
        assert!(msg.contains("not found locally"), "Error: {}", msg);
        assert!(msg.contains("strata models pull miniLM"), "Error: {}", msg);
        assert!(msg.contains(".gguf"), "Error: {}", msg);
        assert!(msg.contains("huggingface.co"), "Error: {}", msg);
        assert!(
            msg.contains("MB") || msg.contains("GB"),
            "Should show size: {}",
            msg
        );
    }

    #[test]
    fn resolve_found_when_file_exists() {
        let (dir, registry) = test_registry();

        let entry = catalog::find_entry("miniLM").unwrap();
        let variant = &entry.variants[0];
        std::fs::write(dir.path().join(variant.hf_file), b"fake gguf").unwrap();

        let path = registry.resolve("miniLM").unwrap();
        assert!(path.exists());
        assert_eq!(path.file_name().unwrap(), variant.hf_file);
    }

    #[test]
    fn resolve_with_quant_override() {
        let (dir, registry) = test_registry();

        let entry = catalog::find_entry("tinyllama").unwrap();
        let q8_variant = entry.variants.iter().find(|v| v.name == "q8_0").unwrap();
        std::fs::write(dir.path().join(q8_variant.hf_file), b"fake gguf").unwrap();

        let path = registry.resolve("tinyllama:q8_0").unwrap();
        assert!(path.exists());
        assert_eq!(path.file_name().unwrap(), q8_variant.hf_file);
    }

    #[test]
    fn resolve_case_insensitive_quant() {
        let (dir, registry) = test_registry();

        let entry = catalog::find_entry("tinyllama").unwrap();
        let q8_variant = entry.variants.iter().find(|v| v.name == "q8_0").unwrap();
        std::fs::write(dir.path().join(q8_variant.hf_file), b"fake").unwrap();

        let path = registry.resolve("tinyllama:Q8_0").unwrap();
        assert!(path.exists());
    }

    #[test]
    fn resolve_via_alias() {
        let (dir, registry) = test_registry();

        let entry = catalog::find_entry("nomic-embed").unwrap();
        let variant = &entry.variants[0];
        std::fs::write(dir.path().join(variant.hf_file), b"fake").unwrap();

        let path = registry.resolve("nomic").unwrap();
        assert!(path.exists());
        assert_eq!(path.file_name().unwrap(), variant.hf_file);
    }

    #[test]
    fn resolve_via_alias_case_insensitive() {
        let (dir, registry) = test_registry();

        let entry = catalog::find_entry("tinyllama").unwrap();
        let variant = &entry.variants[0];
        std::fs::write(dir.path().join(variant.hf_file), b"fake").unwrap();

        let path = registry.resolve("TINY-LLAMA").unwrap();
        assert!(path.exists());
    }

    #[test]
    fn resolve_unknown_quant_lists_available() {
        let (_dir, registry) = test_registry();

        let err = registry.resolve("tinyllama:iq2_xs").unwrap_err();
        let msg = format!("{}", err);
        assert!(msg.contains("Unknown quant"), "Error: {}", msg);
        assert!(msg.contains("q4_k_m"), "Should list q4_k_m: {}", msg);
        assert!(msg.contains("q8_0"), "Should list q8_0: {}", msg);
    }

    #[test]
    fn resolve_unknown_model_gives_error() {
        let (_dir, registry) = test_registry();

        let err = registry.resolve("nonexistent-model").unwrap_err();
        let msg = format!("{}", err);
        assert!(msg.contains("Unknown model"), "Error: {}", msg);
    }

    #[test]
    fn resolve_empty_string() {
        let (_dir, registry) = test_registry();

        let err = registry.resolve("").unwrap_err();
        let msg = format!("{}", err);
        assert!(msg.contains("Unknown model"), "Error: {}", msg);
    }

    #[test]
    fn resolve_whitespace_only() {
        let (_dir, registry) = test_registry();

        let err = registry.resolve("  ").unwrap_err();
        let msg = format!("{}", err);
        assert!(msg.contains("Unknown model"), "Error: {}", msg);
    }

    #[test]
    fn resolve_trailing_colon() {
        let (_dir, registry) = test_registry();

        // "qwen3:" — no single entry "qwen3", so Unknown model error
        let err = registry.resolve("qwen3:").unwrap_err();
        let msg = err.to_string();
        assert!(
            matches!(err, InferenceError::Registry(_)),
            "should be Registry error, got: {msg}"
        );
        assert!(
            msg.contains("Unknown model"),
            "trailing colon should give Unknown model error: {msg}"
        );
    }

    #[test]
    fn resolve_trailing_colon_known_single_name() {
        let (dir, registry) = test_registry();

        // "tinyllama:" — trailing colon on a known single-name entry
        // should behave like "tinyllama" (ignoring trailing colon)
        let entry = catalog::find_entry("tinyllama").unwrap();
        let default_variant = entry
            .variants
            .iter()
            .find(|v| v.name == entry.default_quant)
            .unwrap();
        std::fs::write(dir.path().join(default_variant.hf_file), b"fake").unwrap();

        let path = registry.resolve("tinyllama:").unwrap();
        assert!(path.exists());
        assert_eq!(path.file_name().unwrap(), default_variant.hf_file);
    }

    #[test]
    fn resolve_trailing_colon_colon() {
        let (dir, registry) = test_registry();

        // "qwen3:8b:" — trailing colon on a two-part name
        let entry = catalog::find_entry("qwen3:8b").unwrap();
        let default_variant = entry
            .variants
            .iter()
            .find(|v| v.name == entry.default_quant)
            .unwrap();
        std::fs::write(dir.path().join(default_variant.hf_file), b"fake").unwrap();

        let path = registry.resolve("qwen3:8b:").unwrap();
        assert!(path.exists());
        assert_eq!(path.file_name().unwrap(), default_variant.hf_file);
    }

    #[test]
    fn resolve_leading_colon() {
        let (_dir, registry) = test_registry();

        let err = registry.resolve(":miniLM").unwrap_err();
        let msg = err.to_string();
        assert!(
            matches!(err, InferenceError::Registry(_)),
            "should be Registry error, got: {msg}"
        );
        assert!(
            msg.contains("Unknown model"),
            "leading colon should give Unknown model error: {msg}"
        );
    }

    #[test]
    fn resolve_colon_name_with_quant() {
        let (dir, registry) = test_registry();

        let entry = catalog::find_entry("qwen3:8b").unwrap();
        let q6k_variant = entry.variants.iter().find(|v| v.name == "q6_k").unwrap();
        std::fs::write(dir.path().join(q6k_variant.hf_file), b"fake").unwrap();

        let path = registry.resolve("qwen3:8b:q6_k").unwrap();
        assert!(path.exists());
    }

    #[test]
    fn resolve_default_quant_not_found_but_other_exists() {
        let (dir, registry) = test_registry();

        let entry = catalog::find_entry("tinyllama").unwrap();
        assert_eq!(entry.default_quant, "q4_k_m");
        let q8_variant = entry.variants.iter().find(|v| v.name == "q8_0").unwrap();
        std::fs::write(dir.path().join(q8_variant.hf_file), b"fake").unwrap();

        // resolve("tinyllama") without quant override → looks for default (q4_k_m) → not found
        let err = registry.resolve("tinyllama").unwrap_err();
        let msg = format!("{}", err);
        assert!(
            msg.contains("not found locally"),
            "Should fail for default quant: {}",
            msg
        );

        // But explicit q8_0 should work
        let path = registry.resolve("tinyllama:q8_0").unwrap();
        assert!(path.exists());
    }

    // ===== list_available() / list_local() tests =====

    #[test]
    fn list_available_returns_all() {
        let (_dir, registry) = test_registry();

        let available = registry.list_available();
        assert_eq!(available.len(), catalog::CATALOG.len());
        assert!(available.iter().all(|m| !m.is_local));
    }

    #[test]
    fn list_local_empty_dir() {
        let (_dir, registry) = test_registry();

        let local = registry.list_local();
        assert!(local.is_empty());
    }

    #[test]
    fn list_local_with_default_variant() {
        let (dir, registry) = test_registry();

        let entry = catalog::find_entry("miniLM").unwrap();
        let variant = &entry.variants[0];
        std::fs::write(dir.path().join(variant.hf_file), b"fake").unwrap();

        let local = registry.list_local();
        assert_eq!(local.len(), 1);
        assert_eq!(local[0].name, "miniLM");
        assert!(local[0].is_local);
        assert!(local[0].local_path.is_some());
    }

    #[test]
    fn list_local_finds_non_default_variant() {
        let (dir, registry) = test_registry();

        let entry = catalog::find_entry("tinyllama").unwrap();
        assert_eq!(entry.default_quant, "q4_k_m");
        let q8_variant = entry.variants.iter().find(|v| v.name == "q8_0").unwrap();
        std::fs::write(dir.path().join(q8_variant.hf_file), b"fake").unwrap();

        let local = registry.list_local();
        assert_eq!(
            local.len(),
            1,
            "Should find tinyllama via non-default q8_0 variant"
        );
        assert_eq!(local[0].name, "tinyllama");
        assert!(local[0].is_local);
    }

    #[test]
    fn list_local_multiple_models() {
        let (dir, registry) = test_registry();

        let entry1 = catalog::find_entry("miniLM").unwrap();
        std::fs::write(dir.path().join(entry1.variants[0].hf_file), b"fake").unwrap();

        let entry2 = catalog::find_entry("gpt2").unwrap();
        std::fs::write(dir.path().join(entry2.variants[0].hf_file), b"fake").unwrap();

        let local = registry.list_local();
        assert_eq!(local.len(), 2);
        let names: Vec<&str> = local.iter().map(|m| m.name.as_str()).collect();
        assert!(names.contains(&"miniLM"));
        assert!(names.contains(&"gpt2"));
    }

    #[test]
    fn list_local_ignores_unrelated_files() {
        let (dir, registry) = test_registry();

        std::fs::write(dir.path().join("random-model.gguf"), b"fake").unwrap();

        let local = registry.list_local();
        assert!(local.is_empty(), "Should not match random files");
    }

    // ===== parse_name() tests =====

    #[test]
    fn parse_name_single_part() {
        let (_dir, registry) = test_registry();

        let (entry, quant) = registry.parse_name("miniLM").unwrap();
        assert_eq!(entry.name, "miniLM");
        assert!(quant.is_none());
    }

    #[test]
    fn parse_name_two_part_combined() {
        let (_dir, registry) = test_registry();

        let (entry, quant) = registry.parse_name("qwen3:8b").unwrap();
        assert_eq!(entry.name, "qwen3:8b");
        assert!(quant.is_none());
    }

    #[test]
    fn parse_name_three_part() {
        let (_dir, registry) = test_registry();

        let (entry, quant) = registry.parse_name("qwen3:8b:q6_k").unwrap();
        assert_eq!(entry.name, "qwen3:8b");
        assert_eq!(quant, Some("q6_k"));
    }

    #[test]
    fn parse_name_four_parts_fails() {
        let (_dir, registry) = test_registry();
        let err = registry.parse_name("a:b:c:d").unwrap_err();
        let msg = err.to_string();
        assert!(
            matches!(err, InferenceError::Registry(_)),
            "should be Registry error, got: {msg}"
        );
        assert!(
            msg.contains("Unknown model"),
            "four-part name should give Unknown model error: {msg}"
        );
    }

    // ===== ModelInfo fields tests =====

    #[test]
    fn model_info_embed_fields() {
        let (_dir, registry) = test_registry();

        let available = registry.list_available();
        let minilm = available.iter().find(|m| m.name == "miniLM").unwrap();
        assert_eq!(minilm.task, ModelTask::Embed);
        assert_eq!(minilm.embedding_dim, 384);
        assert_eq!(minilm.architecture, "bert");
        assert!(!minilm.is_local);
        assert!(minilm.local_path.is_none());
        assert!(minilm.size_bytes > 0);
        assert!(!minilm.hf_repo.is_empty());
    }

    #[test]
    fn model_info_generate_fields() {
        let (_dir, registry) = test_registry();

        let available = registry.list_available();
        let qwen = available.iter().find(|m| m.name == "qwen3:8b").unwrap();
        assert_eq!(qwen.task, ModelTask::Generate);
        assert_eq!(qwen.embedding_dim, 0);
        assert_eq!(qwen.architecture, "qwen3");
    }

    // ===== ModelRegistry construction tests =====

    #[test]
    fn models_dir_accessor() {
        let (dir, registry) = test_registry();
        assert_eq!(registry.models_dir(), dir.path());
    }

    #[test]
    fn env_var_override() {
        let _lock = ENV_MUTEX.lock().unwrap();
        let key = "STRATA_MODELS_DIR";
        let original = std::env::var(key).ok();

        let unique_path = format!("/tmp/test-strata-models-{}", std::process::id());
        unsafe { std::env::set_var(key, &unique_path) };
        let registry = ModelRegistry::new();
        assert_eq!(registry.models_dir(), Path::new(&unique_path));

        // Restore
        match original {
            Some(val) => unsafe { std::env::set_var(key, val) },
            None => unsafe { std::env::remove_var(key) },
        }
    }

    #[test]
    fn default_models_dir() {
        let _lock = ENV_MUTEX.lock().unwrap();
        let key = "STRATA_MODELS_DIR";
        let original = std::env::var(key).ok();
        unsafe { std::env::remove_var(key) };

        let registry = ModelRegistry::new();
        let dir = registry.models_dir();
        assert!(
            dir.ends_with(".strata/models"),
            "Default dir should end with .strata/models, got: {}",
            dir.display()
        );

        // Restore
        if let Some(val) = original {
            unsafe { std::env::set_var(key, val) };
        }
    }

    #[test]
    fn default_impl_matches_new() {
        let _lock = ENV_MUTEX.lock().unwrap();
        let r1 = ModelRegistry::new();
        let r2 = ModelRegistry::default();
        assert_eq!(r1.models_dir(), r2.models_dir());
    }

    // ===== format_size() tests =====

    #[test]
    fn format_size_bytes() {
        assert_eq!(format_size(0), "0 bytes");
        assert_eq!(format_size(1), "1 bytes");
        assert_eq!(format_size(500), "500 bytes");
        assert_eq!(format_size(999_999), "999999 bytes");
    }

    #[test]
    fn format_size_megabytes() {
        assert_eq!(format_size(1_000_000), "1 MB");
        assert_eq!(format_size(45_000_000), "45 MB");
        assert_eq!(format_size(999_999_999), "1000 MB");
    }

    #[test]
    fn format_size_gigabytes() {
        assert_eq!(format_size(1_000_000_000), "1.0 GB");
        assert_eq!(format_size(4_700_000_000), "4.7 GB");
        assert_eq!(format_size(10_500_000_000), "10.5 GB");
    }

    // ===== ModelTask tests =====

    #[test]
    fn model_task_display() {
        assert_eq!(ModelTask::Embed.to_string(), "embed");
        assert_eq!(ModelTask::Generate.to_string(), "generate");
    }

    #[test]
    fn model_task_equality() {
        assert_eq!(ModelTask::Embed, ModelTask::Embed);
        assert_eq!(ModelTask::Generate, ModelTask::Generate);
        assert_ne!(ModelTask::Embed, ModelTask::Generate);
    }

    #[test]
    fn model_task_copy() {
        let t = ModelTask::Embed;
        let t2 = t; // Copy
        let t3 = t; // Still usable
        assert_eq!(t2, t3);
    }

    // ===== ModelInfo construction tests =====

    #[test]
    fn model_info_is_local_when_file_exists() {
        let (dir, registry) = test_registry();

        let entry = catalog::find_entry("gpt2").unwrap();
        let variant = &entry.variants[0];

        // Before file exists
        let available = registry.list_available();
        let gpt2 = available.iter().find(|m| m.name == "gpt2").unwrap();
        assert!(!gpt2.is_local);
        assert!(gpt2.local_path.is_none());

        // After file exists
        std::fs::write(dir.path().join(variant.hf_file), b"fake").unwrap();
        let available = registry.list_available();
        let gpt2 = available.iter().find(|m| m.name == "gpt2").unwrap();
        assert!(gpt2.is_local);
        assert!(gpt2.local_path.is_some());
    }

    #[test]
    fn model_info_clone() {
        let (_dir, registry) = test_registry();
        let available = registry.list_available();
        let info = &available[0];
        let cloned = info.clone();
        assert_eq!(cloned.name, info.name);
        assert_eq!(cloned.task, info.task);
        assert_eq!(cloned.architecture, info.architecture);
        assert_eq!(cloned.default_quant, info.default_quant);
        assert_eq!(cloned.embedding_dim, info.embedding_dim);
        assert_eq!(cloned.is_local, info.is_local);
        assert_eq!(cloned.local_path, info.local_path);
        assert_eq!(cloned.size_bytes, info.size_bytes);
        assert_eq!(cloned.hf_repo, info.hf_repo);
    }

    // ===== resolve error type tests =====

    #[test]
    fn resolve_errors_are_registry_variant() {
        let (_dir, registry) = test_registry();

        let err = registry.resolve("nonexistent").unwrap_err();
        assert!(
            matches!(err, InferenceError::Registry(_)),
            "should be Registry error, got: {err}"
        );

        let err = registry.resolve("tinyllama:bad_quant").unwrap_err();
        assert!(
            matches!(err, InferenceError::Registry(_)),
            "should be Registry error for bad quant, got: {err}"
        );
    }

    // ===== Additional depth tests =====

    #[test]
    fn resolve_path_is_under_models_dir() {
        let (dir, registry) = test_registry();

        let entry = catalog::find_entry("miniLM").unwrap();
        let variant = &entry.variants[0];
        std::fs::write(dir.path().join(variant.hf_file), b"fake").unwrap();

        let path = registry.resolve("miniLM").unwrap();
        assert!(
            path.starts_with(dir.path()),
            "resolved path should be under models_dir: {}",
            path.display()
        );
    }

    #[test]
    fn list_available_populates_default_quant_and_size() {
        let (_dir, registry) = test_registry();

        let available = registry.list_available();
        for info in &available {
            assert!(
                !info.default_quant.is_empty(),
                "default_quant should not be empty for '{}'",
                info.name
            );
            assert!(
                info.size_bytes > 0,
                "size_bytes should be > 0 for '{}'",
                info.name
            );
            assert!(
                !info.hf_repo.is_empty(),
                "hf_repo should not be empty for '{}'",
                info.name
            );
        }
    }

    #[test]
    fn list_local_returns_found_variants_size_not_default() {
        let (dir, registry) = test_registry();

        // Place the q8_0 variant (non-default) of tinyllama
        let entry = catalog::find_entry("tinyllama").unwrap();
        assert_eq!(entry.default_quant, "q4_k_m");
        let q8_variant = entry.variants.iter().find(|v| v.name == "q8_0").unwrap();
        let q4_variant = entry
            .variants
            .iter()
            .find(|v| v.name == "q4_k_m")
            .unwrap();
        std::fs::write(dir.path().join(q8_variant.hf_file), b"fake").unwrap();

        let local = registry.list_local();
        assert_eq!(local.len(), 1);
        // size_bytes should reflect the q8_0 variant (found first), not q4_k_m default
        assert_eq!(
            local[0].size_bytes, q8_variant.size_bytes,
            "size_bytes should be for q8_0 variant ({}) not q4_k_m ({})",
            q8_variant.size_bytes, q4_variant.size_bytes
        );
    }

    #[test]
    fn list_local_prefers_first_found_variant() {
        let (dir, registry) = test_registry();

        // Place both variants of tinyllama
        let entry = catalog::find_entry("tinyllama").unwrap();
        for v in entry.variants {
            std::fs::write(dir.path().join(v.hf_file), b"fake").unwrap();
        }

        let local = registry.list_local();
        let tinyllama = local.iter().find(|m| m.name == "tinyllama").unwrap();
        // Should find q4_k_m first (it's first in variants list)
        assert_eq!(tinyllama.size_bytes, entry.variants[0].size_bytes);
    }

    #[test]
    fn model_info_local_path_is_correct() {
        let (dir, registry) = test_registry();

        let entry = catalog::find_entry("gpt2").unwrap();
        let variant = &entry.variants[0];
        let file_path = dir.path().join(variant.hf_file);
        std::fs::write(&file_path, b"fake").unwrap();

        let available = registry.list_available();
        let gpt2 = available.iter().find(|m| m.name == "gpt2").unwrap();
        assert_eq!(gpt2.local_path.as_ref().unwrap(), &file_path);
    }

    #[test]
    fn resolve_two_part_name_with_quant_fallback() {
        let (dir, registry) = test_registry();

        // "miniLM:f16" → no combined "miniLM:f16" entry, falls back to miniLM + quant=f16
        let entry = catalog::find_entry("miniLM").unwrap();
        let variant = entry.variants.iter().find(|v| v.name == "f16").unwrap();
        std::fs::write(dir.path().join(variant.hf_file), b"fake").unwrap();

        let path = registry.resolve("miniLM:f16").unwrap();
        assert!(path.exists());
        assert_eq!(path.file_name().unwrap(), variant.hf_file);
    }
}
