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

/// What the filesystem says about the model file at `path`: its metadata,
/// `None` when nothing is there, or the I/O failure that kept it from
/// answering.
///
/// "Not there" and "cannot tell" are different answers. Absence is a fact
/// the caller acts on — pull the model, fix the path. A read that fails for
/// any other reason (a loop in the path, a permission, a dead mount) is not
/// absence, and reporting it as "not downloaded" would send the caller to
/// download a file that may already be there (#3252). A missing parent is
/// absence too: nothing is downloaded under a directory that does not exist,
/// or under a path that is not a directory.
pub(crate) fn probe_model_file(path: &Path) -> Result<Option<std::fs::Metadata>, InferenceError> {
    use std::io::ErrorKind;
    match path.metadata() {
        Ok(meta) => Ok(Some(meta)),
        Err(e) if matches!(e.kind(), ErrorKind::NotFound | ErrorKind::NotADirectory) => Ok(None),
        Err(e) => Err(InferenceError::Io(format!(
            "cannot read {}: {e}",
            path.display()
        ))),
    }
}

/// Whether the model file at `path` is downloaded.
///
/// The one predicate behind every surface that answers that question —
/// `models list`, `models local`, `inference status`, resolution, and the
/// downloader's "already here" check — so they cannot disagree. They did:
/// resolution refused a zero-length file as an interrupted download while
/// the listing called the same file "ready" and the status counted it.
///
/// A filesystem that cannot answer counts as "not downloaded" here: this is
/// the lenient form for listings, which report over the whole catalog and
/// have nowhere to put one file's I/O failure. Resolution asks
/// [`probe_model_file`] directly and refuses.
pub(crate) fn model_file_is_downloaded(path: &Path) -> bool {
    matches!(probe_model_file(path), Ok(Some(meta)) if is_downloaded_model_file(&meta))
}

/// A regular file with at least one byte. A directory or an empty file at the
/// path is a leftover, not a model.
pub(crate) fn is_downloaded_model_file(meta: &std::fs::Metadata) -> bool {
    meta.is_file() && meta.len() > 0
}

/// What a model is designed for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "wire-schemas", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum ModelTask {
    /// Embedding model.
    Embed,
    /// Text generation model.
    Generate,
    /// Passage ranking model.
    Rank,
}

impl std::fmt::Display for ModelTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ModelTask::Embed => write!(f, "embed"),
            ModelTask::Generate => write!(f, "generate"),
            ModelTask::Rank => write!(f, "rank"),
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
    /// Expected SHA-256 hash of the downloaded file (hex-encoded, lowercase).
    /// When `Some`, the download is verified after completion.
    pub sha256: Option<&'static str>,
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
    /// Embedding dimension (0 for generation and ranking models).
    pub embedding_dim: usize,
}

/// Information about a resolved model.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "wire-schemas", derive(schemars::JsonSchema))]
pub struct ModelInfo {
    /// Stable model catalog name.
    pub name: String,
    /// Model task type.
    pub task: ModelTask,
    /// Model architecture family.
    pub architecture: String,
    /// Default quantization variant.
    pub default_quant: String,
    /// Embedding dimension, or zero for non-embedding models.
    pub embedding_dim: usize,
    /// Whether this variant's artifact is downloaded — a non-empty file at
    /// `local_path`, the same test resolution applies. An interrupted
    /// download's zero-length file is not downloaded.
    ///
    /// This is about the *file*, not about whether it can be run: a released
    /// binary reports `is_local: true` for models it cannot load. Check
    /// `runnable` for that (#3124).
    pub is_local: bool,
    /// Whether **this binary** can execute this model.
    ///
    /// Every model in this catalog runs through the local provider, so this is
    /// false in any build without the `local` feature — which is every released
    /// binary. `strata inference install-local` adds that execution; a cloud
    /// model needs none.
    pub runnable: bool,
    /// Local GGUF path when present.
    pub local_path: Option<PathBuf>,
    /// Approximate model artifact size in bytes.
    pub size_bytes: u64,
    /// HuggingFace repository for the model artifact.
    pub hf_repo: String,
}

/// What [`ModelRegistry::lookup`] found for a local model name.
#[derive(Debug, Clone)]
pub(crate) enum CatalogLookup {
    /// A catalogued model and the variant the name selects (the default when
    /// the name carries no quant suffix).
    Found {
        /// The catalog entry the name matched.
        entry: &'static CatalogEntry,
        /// The variant the name selects.
        variant: &'static QuantVariant,
        /// Where the variant's file lives, whether or not it is there.
        path: PathBuf,
        /// Whether that file is present and non-empty
        /// ([`model_file_is_downloaded`]).
        downloaded: bool,
    },
    /// No catalog entry has this name.
    UnknownModel,
    /// The model is catalogued, but it has no variant by the quant the name
    /// asks for.
    UnknownQuant {
        /// The entry the name's model part matched.
        entry: &'static CatalogEntry,
    },
}

/// Delete a catalogued model file whose size says it is not the model.
///
/// Called after a load fails: a file more than 10% off the catalogued size is
/// a truncated or corrupt download, and removing it lets the next
/// `strata inference models pull` fetch a fresh copy instead of tripping over
/// it forever. A file within tolerance is left alone — the load failed for
/// some other reason. Variants with no catalogued size, and files that are
/// already gone, are left alone too.
///
/// Only a local build loads models, so only a local build has a failed load
/// to react to; the tests exercise the predicate in every build.
#[cfg(any(feature = "local", test))]
pub(crate) fn discard_if_corrupt(variant: &QuantVariant, path: &Path) {
    let expected = variant.size_bytes;
    if expected == 0 {
        return;
    }

    let actual = match std::fs::metadata(path) {
        Ok(m) => m.len(),
        Err(_) => return,
    };

    let ratio = actual as f64 / expected as f64;
    if !(0.9..=1.1).contains(&ratio) {
        tracing::warn!(
            file = variant.hf_file,
            expected_bytes = expected,
            actual_bytes = actual,
            "Model file appears corrupted (size mismatch) \u{2014} deleting for re-download"
        );
        if let Err(e) = std::fs::remove_file(path) {
            tracing::warn!(
                file = variant.hf_file,
                path = %path.display(),
                error = %e,
                "Failed to delete corrupted model file"
            );
        }
    }
}

/// Model registry for resolving names to local GGUF file paths.
#[derive(Debug)]
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
                    .find(|v| model_file_is_downloaded(&self.models_dir.join(v.hf_file)));
                local_variant.map(|v| self.entry_to_info(entry, v.name))
            })
            .collect()
    }

    /// What the catalog and the models directory say about a local model name.
    ///
    /// Identity only: the name is matched against the catalog (aliases,
    /// `family:size`, and a case-insensitive quant suffix all count) and the
    /// variant's file is checked for presence. Whether the model can be *used*
    /// — the build, the task, the network — is the resolver's question
    /// (`crate::resolve`), which composes this answer and never re-parses the
    /// name. Before that resolver existed, `resolve`, `resolve_or_pull`,
    /// `info`, `pull` and `check_and_clean_corrupt` each parsed the name for
    /// themselves, and disagreed about what an unknown one meant (#3264).
    ///
    /// Errs only when the models directory cannot be read
    /// ([`probe_model_file`]); an absent file is `downloaded: false`.
    pub(crate) fn lookup(&self, name: &str) -> Result<CatalogLookup, InferenceError> {
        let parts: Vec<&str> = name.split(':').collect();
        let Some((entry, quant)) = catalog::find_entry_by_parts(&parts) else {
            return Ok(CatalogLookup::UnknownModel);
        };
        let quant_name = quant.unwrap_or(entry.default_quant);
        match entry
            .variants
            .iter()
            .find(|v| v.name.eq_ignore_ascii_case(quant_name))
        {
            Some(variant) => {
                let path = self.models_dir.join(variant.hf_file);
                let downloaded =
                    probe_model_file(&path)?.is_some_and(|meta| is_downloaded_model_file(&meta));
                Ok(CatalogLookup::Found {
                    entry,
                    variant,
                    downloaded,
                    path,
                })
            }
            None => Ok(CatalogLookup::UnknownQuant { entry }),
        }
    }

    /// Download one catalogued variant into the models directory and return
    /// its path.
    ///
    /// The callback receives `(bytes_downloaded, total_bytes)`. The caller
    /// has already looked the variant up, so nothing here can fail for a
    /// reason other than the download itself.
    #[cfg(feature = "download")]
    pub(crate) fn pull_variant(
        &self,
        entry: &CatalogEntry,
        variant: &QuantVariant,
        cb: impl Fn(u64, u64),
    ) -> Result<PathBuf, InferenceError> {
        download::download_hf_file_with_size(
            entry.hf_repo,
            variant.hf_file,
            &self.models_dir,
            &cb,
            variant.size_bytes,
            variant.sha256,
        )?;

        Ok(self.models_dir.join(variant.hf_file))
    }

    /// The on-disk path of a catalog model that is already downloaded, for
    /// the ignored smoke tests that load a real model when one is present.
    #[cfg(all(test, feature = "local"))]
    pub(crate) fn downloaded_catalog_path(name: &str) -> Option<PathBuf> {
        match Self::new().lookup(name) {
            Ok(CatalogLookup::Found {
                path,
                downloaded: true,
                ..
            }) => Some(path),
            // A models directory that cannot be read has no downloaded
            // model to smoke-test with, same as one that is empty.
            Ok(
                CatalogLookup::Found { .. }
                | CatalogLookup::UnknownModel
                | CatalogLookup::UnknownQuant { .. },
            )
            | Err(_) => None,
        }
    }

    /// Convert a catalog entry to a ModelInfo with local availability check.
    fn entry_to_info(&self, entry: &CatalogEntry, quant_name: &str) -> ModelInfo {
        let variant = entry
            .variants
            .iter()
            .find(|v| v.name.eq_ignore_ascii_case(quant_name))
            .unwrap_or(&entry.variants[0]);

        let path = self.models_dir.join(variant.hf_file);
        let is_local = model_file_is_downloaded(&path);

        ModelInfo {
            name: entry.name.to_string(),
            task: entry.task,
            architecture: entry.architecture.to_string(),
            default_quant: entry.default_quant.to_string(),
            embedding_dim: entry.embedding_dim,
            is_local,
            // Every catalog model runs through the local provider, so what
            // decides runnability is whether this binary has it compiled in.
            runnable: cfg!(feature = "local"),
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

/// Format a byte count as a human-readable size (e.g., "4.7 GB").
///
/// The one formatter for sizes: the download offer's refusal and the CLI's
/// `models list` table both read it (#3235), and the CLI restates it for its
/// own `|size` cells because inference imports nothing from the workspace
/// (Rule 3) — `size_text_matches_the_inference_registry` keeps the two in
/// step, so a size reads the same wherever the product quotes one (#3335).
pub fn format_size(bytes: u64) -> String {
    // Decimal units, because a byte count a reader is shown is about scale,
    // not about memory pages: 1 kB is 1000 bytes, as every disk and download
    // says. One decimal, trimmed when it adds nothing (`1 kB`, not `1.0 kB`),
    // and whole bytes below a kilobyte.
    const UNITS: [(u64, &str); 3] = [(1_000_000_000, "GB"), (1_000_000, "MB"), (1_000, "kB")];
    for (scale, unit) in UNITS {
        #[allow(clippy::cast_precision_loss)] // A byte count shown to one decimal.
        let scaled = bytes as f64 / scale as f64;
        // Rounding decides the unit, so a count that reads as `1000 MB` after
        // rounding is shown as `1 GB` instead.
        let rounded = (scaled * 10.0).round() / 10.0;
        if rounded >= 1.0 {
            let text = format!("{rounded:.1}");
            return format!("{} {unit}", text.strip_suffix(".0").unwrap_or(&text));
        }
    }
    format!("{bytes} B")
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

    // ===== lookup() tests =====
    //
    // `lookup` answers identity and presence; what a caller may do with the
    // answer, and the error each outcome becomes, is `crate::resolve`'s and
    // is tested there.

    /// The variant and path `lookup` finds for `name`, with whether the file
    /// is downloaded. Panics when the name is not catalogued.
    fn found(registry: &ModelRegistry, name: &str) -> (&'static QuantVariant, PathBuf, bool) {
        match registry.lookup(name).expect("a readable models directory") {
            CatalogLookup::Found {
                variant,
                path,
                downloaded,
                ..
            } => (variant, path, downloaded),
            other => panic!("{name}: expected a catalogued model, got {other:?}"),
        }
    }

    fn assert_unknown_model(registry: &ModelRegistry, name: &str) {
        let lookup = registry.lookup(name).expect("a readable models directory");
        assert!(
            matches!(lookup, CatalogLookup::UnknownModel),
            "{name:?}: expected UnknownModel, got {lookup:?}"
        );
    }

    #[test]
    fn lookup_reports_a_catalogued_model_that_is_not_downloaded() {
        let (dir, registry) = test_registry();

        let entry = catalog::find_entry("miniLM").unwrap();
        let (variant, path, downloaded) = found(&registry, "miniLM");
        assert_eq!(variant.name, entry.default_quant);
        assert_eq!(path, dir.path().join(variant.hf_file));
        assert!(!downloaded);
    }

    #[test]
    fn lookup_found_when_file_exists() {
        let (dir, registry) = test_registry();

        let entry = catalog::find_entry("miniLM").unwrap();
        let variant = &entry.variants[0];
        std::fs::write(dir.path().join(variant.hf_file), b"fake gguf").unwrap();

        let (_, path, downloaded) = found(&registry, "miniLM");
        assert!(downloaded);
        assert!(path.exists());
        assert_eq!(path.file_name().unwrap(), variant.hf_file);
    }

    #[test]
    fn lookup_with_quant_override() {
        let (dir, registry) = test_registry();

        let entry = catalog::find_entry("tinyllama").unwrap();
        let q8_variant = entry.variants.iter().find(|v| v.name == "q8_0").unwrap();
        std::fs::write(dir.path().join(q8_variant.hf_file), b"fake gguf").unwrap();

        let (variant, path, downloaded) = found(&registry, "tinyllama:q8_0");
        assert_eq!(variant.name, "q8_0");
        assert!(downloaded);
        assert_eq!(path.file_name().unwrap(), q8_variant.hf_file);
    }

    #[test]
    fn lookup_case_insensitive_quant() {
        let (dir, registry) = test_registry();

        let entry = catalog::find_entry("tinyllama").unwrap();
        let q8_variant = entry.variants.iter().find(|v| v.name == "q8_0").unwrap();
        std::fs::write(dir.path().join(q8_variant.hf_file), b"fake").unwrap();

        let (variant, _, downloaded) = found(&registry, "tinyllama:Q8_0");
        assert_eq!(variant.name, "q8_0");
        assert!(downloaded);
    }

    #[test]
    fn lookup_via_alias() {
        let (dir, registry) = test_registry();

        let entry = catalog::find_entry("nomic-embed").unwrap();
        let variant = &entry.variants[0];
        std::fs::write(dir.path().join(variant.hf_file), b"fake").unwrap();

        let (_, path, downloaded) = found(&registry, "nomic");
        assert!(downloaded);
        assert_eq!(path.file_name().unwrap(), variant.hf_file);
    }

    #[test]
    fn lookup_via_alias_case_insensitive() {
        let (dir, registry) = test_registry();

        let entry = catalog::find_entry("tinyllama").unwrap();
        let variant = &entry.variants[0];
        std::fs::write(dir.path().join(variant.hf_file), b"fake").unwrap();

        let (_, _, downloaded) = found(&registry, "TINY-LLAMA");
        assert!(downloaded);
    }

    #[test]
    fn lookup_unknown_quant_names_the_entry() {
        let (_dir, registry) = test_registry();

        match registry
            .lookup("tinyllama:iq2_xs")
            .expect("a readable models directory")
        {
            CatalogLookup::UnknownQuant { entry } => assert_eq!(entry.name, "tinyllama"),
            other => panic!("expected UnknownQuant, got {other:?}"),
        }
    }

    #[test]
    fn lookup_unknown_model() {
        let (_dir, registry) = test_registry();
        assert_unknown_model(&registry, "nonexistent-model");
    }

    #[test]
    fn lookup_empty_string() {
        let (_dir, registry) = test_registry();
        assert_unknown_model(&registry, "");
    }

    #[test]
    fn lookup_whitespace_only() {
        let (_dir, registry) = test_registry();
        assert_unknown_model(&registry, "  ");
    }

    #[test]
    fn lookup_trailing_colon() {
        let (_dir, registry) = test_registry();
        // "qwen3:" — no single entry "qwen3".
        assert_unknown_model(&registry, "qwen3:");
    }

    #[test]
    fn lookup_trailing_colon_known_single_name() {
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

        let (variant, _, downloaded) = found(&registry, "tinyllama:");
        assert!(downloaded);
        assert_eq!(variant.hf_file, default_variant.hf_file);
    }

    #[test]
    fn lookup_trailing_colon_colon() {
        let (dir, registry) = test_registry();

        // "qwen3:8b:" — trailing colon on a two-part name
        let entry = catalog::find_entry("qwen3:8b").unwrap();
        let default_variant = entry
            .variants
            .iter()
            .find(|v| v.name == entry.default_quant)
            .unwrap();
        std::fs::write(dir.path().join(default_variant.hf_file), b"fake").unwrap();

        let (variant, _, downloaded) = found(&registry, "qwen3:8b:");
        assert!(downloaded);
        assert_eq!(variant.hf_file, default_variant.hf_file);
    }

    #[test]
    fn lookup_leading_colon() {
        let (_dir, registry) = test_registry();
        assert_unknown_model(&registry, ":miniLM");
    }

    #[test]
    fn lookup_colon_name_with_quant() {
        let (dir, registry) = test_registry();

        let entry = catalog::find_entry("qwen3:8b").unwrap();
        let q6k_variant = entry.variants.iter().find(|v| v.name == "q6_k").unwrap();
        std::fs::write(dir.path().join(q6k_variant.hf_file), b"fake").unwrap();

        let (variant, _, downloaded) = found(&registry, "qwen3:8b:q6_k");
        assert_eq!(variant.name, "q6_k");
        assert!(downloaded);
    }

    #[test]
    fn lookup_default_quant_not_found_but_other_exists() {
        let (dir, registry) = test_registry();

        let entry = catalog::find_entry("tinyllama").unwrap();
        assert_eq!(entry.default_quant, "q4_k_m");
        let q8_variant = entry.variants.iter().find(|v| v.name == "q8_0").unwrap();
        std::fs::write(dir.path().join(q8_variant.hf_file), b"fake").unwrap();

        // "tinyllama" without a quant selects the default (q4_k_m), which is
        // not there; the explicit q8_0 is.
        let (variant, _, downloaded) = found(&registry, "tinyllama");
        assert_eq!(variant.name, "q4_k_m");
        assert!(!downloaded);

        let (variant, _, downloaded) = found(&registry, "tinyllama:q8_0");
        assert_eq!(variant.name, "q8_0");
        assert!(downloaded);
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
    fn list_local_ignores_zero_length_files() {
        let (dir, registry) = test_registry();

        let entry = catalog::find_entry("miniLM").unwrap();
        let variant = &entry.variants[0];
        // Create a zero-length file (simulating interrupted download)
        std::fs::write(dir.path().join(variant.hf_file), b"").unwrap();

        let local = registry.list_local();
        assert!(
            local.is_empty(),
            "Zero-length file should not count as locally available"
        );
    }

    #[test]
    fn lookup_rejects_zero_length_file() {
        let (dir, registry) = test_registry();

        let entry = catalog::find_entry("miniLM").unwrap();
        let variant = &entry.variants[0];
        // Create a zero-length file
        std::fs::write(dir.path().join(variant.hf_file), b"").unwrap();

        let (_, _, downloaded) = found(&registry, "miniLM");
        assert!(!downloaded, "a zero-length file is not a downloaded model");
    }

    /// Every surface that answers "is this model downloaded?" answers the
    /// same way. `list_available` used to call any existing path local —
    /// a zero-length file, even a directory — while `list_local` and
    /// `lookup` refused it, so `models list` said "ready" and
    /// `inference status` counted a model that would not load.
    #[test]
    fn every_surface_agrees_on_what_counts_as_downloaded() {
        let (dir, registry) = test_registry();
        let entry = catalog::find_entry("miniLM").unwrap();
        let default_variant = entry
            .variants
            .iter()
            .find(|v| v.name == entry.default_quant)
            .unwrap();
        let path = dir.path().join(default_variant.hf_file);

        let available_says_local = |registry: &ModelRegistry| {
            registry
                .list_available()
                .into_iter()
                .find(|info| info.name == "miniLM")
                .unwrap()
                .is_local
        };
        let local_lists_it = |registry: &ModelRegistry| {
            registry
                .list_local()
                .iter()
                .any(|info| info.name == "miniLM")
        };

        // An interrupted download's zero-length leftover.
        std::fs::write(&path, b"").unwrap();
        assert!(!model_file_is_downloaded(&path));
        assert!(
            !available_says_local(&registry),
            "zero-length: list_available"
        );
        assert!(!local_lists_it(&registry), "zero-length: list_local");
        assert!(!found(&registry, "miniLM").2, "zero-length: lookup");

        // A directory squatting on the artifact's path.
        std::fs::remove_file(&path).unwrap();
        std::fs::create_dir(&path).unwrap();
        assert!(!model_file_is_downloaded(&path));
        assert!(
            !available_says_local(&registry),
            "directory: list_available"
        );
        assert!(!local_lists_it(&registry), "directory: list_local");
        assert!(!found(&registry, "miniLM").2, "directory: lookup");

        // The real thing.
        std::fs::remove_dir(&path).unwrap();
        std::fs::write(&path, b"gguf").unwrap();
        assert!(model_file_is_downloaded(&path));
        assert!(
            available_says_local(&registry),
            "downloaded: list_available"
        );
        assert!(local_lists_it(&registry), "downloaded: list_local");
        let (_, found_path, downloaded) = found(&registry, "miniLM");
        assert!(downloaded, "downloaded: lookup");
        assert_eq!(found_path, path, "downloaded: lookup path");
    }

    #[test]
    fn list_local_ignores_unrelated_files() {
        let (dir, registry) = test_registry();

        std::fs::write(dir.path().join("random-model.gguf"), b"fake").unwrap();

        let local = registry.list_local();
        assert!(local.is_empty(), "Should not match random files");
    }

    // ===== name-shape tests =====

    #[test]
    fn lookup_single_part_selects_the_default_quant() {
        let (_dir, registry) = test_registry();

        let entry = catalog::find_entry("miniLM").unwrap();
        let (variant, _, _) = found(&registry, "miniLM");
        assert_eq!(variant.name, entry.default_quant);
    }

    #[test]
    fn lookup_two_part_combined_name() {
        let (_dir, registry) = test_registry();

        let entry = catalog::find_entry("qwen3:8b").unwrap();
        let (variant, _, _) = found(&registry, "qwen3:8b");
        assert_eq!(variant.name, entry.default_quant);
    }

    #[test]
    fn lookup_three_part_name() {
        let (_dir, registry) = test_registry();

        let (variant, _, _) = found(&registry, "qwen3:8b:q6_k");
        assert_eq!(variant.name, "q6_k");
    }

    #[test]
    fn lookup_four_parts_is_unknown() {
        let (_dir, registry) = test_registry();
        assert_unknown_model(&registry, "a:b:c:d");
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
    fn format_size_below_a_kilobyte_counts_bytes() {
        assert_eq!(format_size(0), "0 B");
        assert_eq!(format_size(1), "1 B");
        assert_eq!(format_size(500), "500 B");
    }

    #[test]
    fn format_size_scales_by_decimal_units() {
        assert_eq!(format_size(1_000), "1 kB");
        assert_eq!(format_size(1_024), "1 kB");
        assert_eq!(format_size(40_960), "41 kB");
        assert_eq!(format_size(1_000_000), "1 MB");
        assert_eq!(format_size(1_048_576), "1 MB");
        assert_eq!(format_size(45_000_000), "45 MB");
        assert_eq!(format_size(67_108_864), "67.1 MB");
        assert_eq!(format_size(536_870_912), "536.9 MB");
        assert_eq!(format_size(4_700_000_000), "4.7 GB");
        assert_eq!(format_size(10_500_000_000), "10.5 GB");
    }

    /// A count that rounds up to a thousand of its unit is shown in the next
    /// one: `1000 MB` is a size no reader should have to convert.
    #[test]
    fn format_size_promotes_a_unit_when_rounding_fills_it() {
        assert_eq!(format_size(999), "1 kB");
        assert_eq!(format_size(999_999), "1 MB");
        assert_eq!(format_size(999_999_999), "1 GB");
        assert_eq!(format_size(1_000_000_000), "1 GB");
        // Direction control: just below the rounding boundary keeps its unit.
        assert_eq!(format_size(949), "949 B");
        assert_eq!(format_size(949_999), "950 kB");
    }

    // ===== ModelTask tests =====

    #[test]
    fn model_task_display() {
        assert_eq!(ModelTask::Embed.to_string(), "embed");
        assert_eq!(ModelTask::Generate.to_string(), "generate");
        assert_eq!(ModelTask::Rank.to_string(), "rank");
    }

    #[test]
    fn model_task_equality() {
        assert_eq!(ModelTask::Embed, ModelTask::Embed);
        assert_eq!(ModelTask::Generate, ModelTask::Generate);
        assert_eq!(ModelTask::Rank, ModelTask::Rank);
        assert_ne!(ModelTask::Embed, ModelTask::Generate);
        assert_ne!(ModelTask::Embed, ModelTask::Rank);
        assert_ne!(ModelTask::Generate, ModelTask::Rank);
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
        // Use the default variant (what list_available checks)
        let variant = entry
            .variants
            .iter()
            .find(|v| v.name == entry.default_quant)
            .unwrap_or(&entry.variants[0]);

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

    // ===== Additional depth tests =====

    #[test]
    fn lookup_path_is_under_models_dir() {
        let (dir, registry) = test_registry();

        let entry = catalog::find_entry("miniLM").unwrap();
        let variant = &entry.variants[0];
        std::fs::write(dir.path().join(variant.hf_file), b"fake").unwrap();

        let (_, path, _) = found(&registry, "miniLM");
        assert!(
            path.starts_with(dir.path()),
            "looked-up path should be under models_dir: {}",
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
        let q4_variant = entry.variants.iter().find(|v| v.name == "q4_k_m").unwrap();
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
        // Use the default variant (what list_available checks)
        let variant = entry
            .variants
            .iter()
            .find(|v| v.name == entry.default_quant)
            .unwrap_or(&entry.variants[0]);
        let file_path = dir.path().join(variant.hf_file);
        std::fs::write(&file_path, b"fake").unwrap();

        let available = registry.list_available();
        let gpt2 = available.iter().find(|m| m.name == "gpt2").unwrap();
        assert_eq!(gpt2.local_path.as_ref().unwrap(), &file_path);
    }

    #[test]
    fn lookup_two_part_name_with_quant_fallback() {
        let (dir, registry) = test_registry();

        // "miniLM:f16" → no combined "miniLM:f16" entry, falls back to miniLM + quant=f16
        let entry = catalog::find_entry("miniLM").unwrap();
        let variant = entry.variants.iter().find(|v| v.name == "f16").unwrap();
        std::fs::write(dir.path().join(variant.hf_file), b"fake").unwrap();

        let (found_variant, path, downloaded) = found(&registry, "miniLM:f16");
        assert!(downloaded);
        assert_eq!(found_variant.name, "f16");
        assert_eq!(path.file_name().unwrap(), variant.hf_file);
    }

    // ===== discard_if_corrupt() tests =====

    #[test]
    fn discard_deletes_undersized_file() {
        let (dir, _registry) = test_registry();

        let entry = catalog::find_entry("miniLM").unwrap();
        let variant = &entry.variants[0];
        let path = dir.path().join(variant.hf_file);
        // Write a file far smaller than expected (~45MB)
        std::fs::write(&path, b"tiny").unwrap();
        assert!(path.exists());

        discard_if_corrupt(variant, &path);
        assert!(
            !path.exists(),
            "undersized file should be deleted as corrupted"
        );
    }

    #[test]
    fn discard_keeps_correctly_sized_file() {
        let (dir, _registry) = test_registry();

        let entry = catalog::find_entry("miniLM").unwrap();
        let variant = &entry.variants[0];
        let path = dir.path().join(variant.hf_file);
        // Write a file within 10% of expected size
        let fake_data = vec![0u8; variant.size_bytes as usize];
        std::fs::write(&path, &fake_data).unwrap();
        assert!(path.exists());

        discard_if_corrupt(variant, &path);
        assert!(path.exists(), "correctly-sized file should not be deleted");
    }

    #[test]
    fn discard_noop_without_a_catalogued_size() {
        let (dir, _registry) = test_registry();

        let path = dir.path().join("fake.gguf");
        std::fs::write(&path, b"data").unwrap();

        let no_size = QuantVariant {
            name: "q0",
            hf_file: "fake.gguf",
            size_bytes: 0,
            sha256: None,
        };
        discard_if_corrupt(&no_size, &path);
        assert!(path.exists(), "no expected size means no verdict");
    }

    #[test]
    fn discard_noop_for_missing_file() {
        let (dir, _registry) = test_registry();

        let entry = catalog::find_entry("miniLM").unwrap();
        let path = dir.path().join("no-such-file.gguf");
        // Should not panic when file doesn't exist
        discard_if_corrupt(&entry.variants[0], &path);
        assert!(!path.exists());
    }

    #[test]
    fn discard_deletes_zero_length_file() {
        let (dir, _registry) = test_registry();

        let entry = catalog::find_entry("miniLM").unwrap();
        let variant = &entry.variants[0];
        let path = dir.path().join(variant.hf_file);
        std::fs::write(&path, b"").unwrap();

        discard_if_corrupt(variant, &path);
        assert!(
            !path.exists(),
            "zero-length file should be deleted as corrupted"
        );
    }
}
