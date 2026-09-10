//! One answer to "can this model be used here, and if not, why not?"
//!
//! Every runtime entry point — `generate`, `chat`, `tokenize`, `detokenize`,
//! `embed`, `embed_batch`, `rank`, `pull_model`, `capability` — asks
//! [`InferenceRuntime::resolve`] first and acts on the [`ResolvedModel`] it
//! gets back. Nothing downstream parses the spec or consults the catalog
//! again. Before this module existed, six sites each ran their own subset of
//! the checks in their own order, and the code a caller saw depended on which
//! site it happened to reach first (#3255, #3260, #3262, #3263, #3264).
//!
//! [`resolve`] errs only for a malformed spec. Every other outcome is data —
//! an [`Availability`] — so that `capability` can report it, an error can
//! carry it, and [`ResolvedModel::require_ready`] can turn it into exactly
//! one typed [`InferenceError`] by a match the compiler keeps total. No code
//! here is derived from a message; the message is derived from the code.
//!
//! The checks run in a fixed order, identity first: malformed → unknown →
//! wrong task → not built → network → key → not downloaded. The order is
//! contract, pinned by `tests/resolution_matrix.rs`: a spec that fails two
//! checks gets the earlier answer, so nobody is told to install local
//! execution for a model that does not exist.

use std::fmt;
use std::path::{Path, PathBuf};

use crate::registry::{format_size, CatalogEntry, CatalogLookup, ModelRegistry, QuantVariant};
use crate::runtime::{InferenceRuntime, ModelAbilities, LOCAL_UNAVAILABLE_REMEDY};
use crate::{
    api_key_env_var, generation_provider_feature_enabled, parse_model_spec, InferenceError,
    ModelTask, ProviderFailure, ProviderKind, RegistryFailure, UnsupportedKind,
};

/// What a caller is about to do with a model. Decides which checks apply.
///
/// `None` at a call site means the caller only needs to locate the model —
/// `pull`, `capability` — so only identity and presence are checked: the
/// spec's shape, the catalog, and (for a local model) the file. The task,
/// the build, the network and the key are questions a run asks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelUse {
    /// Execute the model for a task.
    Run(ModelTask),
    /// Encode or decode with the model's vocabulary. Needs local execution
    /// and the file, but any catalogued task's model will do.
    Tokenize,
}

impl fmt::Display for ModelUse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Run(task) => write!(f, "{task}"),
            Self::Tokenize => write!(f, "tokenize"),
        }
    }
}

/// A model spec, understood.
///
/// Built once per call by [`InferenceRuntime::resolve`]; everything after it
/// reads these fields instead of re-deriving them.
#[derive(Debug, Clone)]
pub struct ResolvedModel {
    /// The spec as the caller wrote it, trimmed.
    pub spec: String,
    /// Who serves it.
    pub provider: ProviderKind,
    /// The model name without its provider prefix.
    pub name: String,
    /// Where the model comes from.
    pub source: ModelSource,
    /// Whether it can be used right now, and if not, the one reason why.
    pub availability: Availability,
}

/// Where a resolved model comes from.
#[derive(Debug, Clone)]
pub enum ModelSource {
    /// A catalogued local model: the entry, the variant the name selects,
    /// and where that variant's file lives (whether or not it is there).
    Catalog {
        /// The catalog entry.
        entry: &'static CatalogEntry,
        /// The quant variant.
        variant: &'static QuantVariant,
        /// `models_dir/<hf_file>`.
        path: PathBuf,
    },
    /// A GGUF file named by path.
    GgufPath(PathBuf),
    /// A local name the catalog does not know. `entry` is the model the
    /// name's first part matched when only the quant suffix was unknown, so
    /// the refusal can list the quants that exist.
    Uncatalogued {
        /// The entry the name matched without its quant, if any.
        entry: Option<&'static CatalogEntry>,
    },
    /// Served by a cloud provider; nothing lives on disk.
    Cloud,
}

impl ModelSource {
    /// Whether the model is served by a cloud provider rather than a file.
    #[must_use]
    pub const fn is_cloud(&self) -> bool {
        matches!(self, Self::Cloud)
    }
}

/// Why a model can or cannot be used right now. At most one reason, decided
/// in the order the variants are listed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Availability {
    /// Every check passed; a load or a request may still fail on its own.
    Ready,
    /// The name is not in the catalog and is not a file that exists.
    NotInCatalog,
    /// The spec names a file path and there is no file there.
    PathMissing,
    /// The model exists but does not do what the caller asked of it.
    TaskNotSupported {
        /// The use the caller asked for.
        requested: ModelUse,
    },
    /// A local model, in a build without local execution.
    LocalExecutionNotBuilt,
    /// A cloud provider this build was compiled without.
    ProviderNotBuilt,
    /// A cloud model, and the runtime was configured with network access off.
    NetworkDisabled,
    /// A cloud model whose provider key is nowhere the runtime looks.
    KeyMissing {
        /// The environment variable that would hold it.
        env_var: &'static str,
        /// The `strata config` key that would hold it.
        config_key: String,
    },
    /// A catalogued local model whose file is not in the models directory
    /// (absent, or an interrupted download's zero-length file).
    NotDownloaded {
        /// The spec `strata inference models pull` takes to fetch it.
        pull_spec: String,
        /// The download size, from the catalog.
        size_bytes: u64,
    },
}

impl Availability {
    /// The variant alone, for the wire.
    #[must_use]
    pub const fn kind(&self) -> AvailabilityKind {
        match self {
            Self::Ready => AvailabilityKind::Ready,
            Self::NotInCatalog => AvailabilityKind::NotInCatalog,
            Self::PathMissing => AvailabilityKind::PathMissing,
            Self::TaskNotSupported { .. } => AvailabilityKind::TaskNotSupported,
            Self::LocalExecutionNotBuilt => AvailabilityKind::LocalExecutionNotBuilt,
            Self::ProviderNotBuilt => AvailabilityKind::ProviderNotBuilt,
            Self::NetworkDisabled => AvailabilityKind::NetworkDisabled,
            Self::KeyMissing { .. } => AvailabilityKind::KeyMissing,
            Self::NotDownloaded { .. } => AvailabilityKind::NotDownloaded,
        }
    }
}

/// [`Availability`] without its payload: the one word a caller branches on.
///
/// Serializes as the snake_case variant name (`not_downloaded`), which is the
/// value of `details.availability` on every inference refusal.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "wire-schemas", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum AvailabilityKind {
    /// [`Availability::Ready`].
    Ready,
    /// [`Availability::NotInCatalog`].
    NotInCatalog,
    /// [`Availability::PathMissing`].
    PathMissing,
    /// [`Availability::TaskNotSupported`].
    TaskNotSupported,
    /// [`Availability::LocalExecutionNotBuilt`].
    LocalExecutionNotBuilt,
    /// [`Availability::ProviderNotBuilt`].
    ProviderNotBuilt,
    /// [`Availability::NetworkDisabled`].
    NetworkDisabled,
    /// [`Availability::KeyMissing`].
    KeyMissing,
    /// [`Availability::NotDownloaded`].
    NotDownloaded,
}

/// The wire shape of a [`ResolvedModel`]'s answer. This type *is* the
/// definition of `strata.error.details.inference.v1`: every inference
/// refusal that came from resolution carries one, flattened by the executor
/// into the envelope's `details` with these field names as keys, and only
/// the fields that are `Some` appear.
///
/// A caller that wants to act on a refusal reads `availability` here, never
/// the error code: the code says what class of thing went wrong, this says
/// what to do about it (`pull_spec` for a download, `key_env_var` for a key).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "wire-schemas", derive(schemars::JsonSchema))]
#[non_exhaustive]
pub struct AvailabilityDetails {
    /// The model name without its provider prefix, as [`ResolvedModel::name`].
    pub model: String,
    /// Who serves it.
    pub provider: ProviderKind,
    /// Whether it can be used right now, and if not, the one reason why.
    pub availability: AvailabilityKind,
    /// The spec `strata inference models pull` takes to fetch it. Present
    /// only for [`AvailabilityKind::NotDownloaded`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pull_spec: Option<String>,
    /// The download size in bytes. Present only for
    /// [`AvailabilityKind::NotDownloaded`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    /// The environment variable that would hold the provider key. Present
    /// only for [`AvailabilityKind::KeyMissing`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_env_var: Option<String>,
    /// The `strata config` key that would hold the provider key. Present
    /// only for [`AvailabilityKind::KeyMissing`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_key: Option<String>,
    /// Set by the executor when the spec came from a collection record, so
    /// the refusal says which record named the model.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub collection: Option<String>,
}

impl ResolvedModel {
    /// This answer as data, for an error to carry or `capability` to report.
    #[must_use]
    pub fn details(&self) -> AvailabilityDetails {
        let mut details = AvailabilityDetails {
            model: self.name.clone(),
            provider: self.provider,
            availability: self.availability.kind(),
            pull_spec: None,
            size_bytes: None,
            key_env_var: None,
            config_key: None,
            collection: None,
        };
        match &self.availability {
            Availability::KeyMissing {
                env_var,
                config_key,
            } => {
                details.key_env_var = Some((*env_var).to_owned());
                details.config_key = Some(config_key.clone());
            }
            Availability::NotDownloaded {
                pull_spec,
                size_bytes,
            } => {
                details.pull_spec = Some(pull_spec.clone());
                details.size_bytes = Some(*size_bytes);
            }
            Availability::Ready
            | Availability::NotInCatalog
            | Availability::PathMissing
            | Availability::TaskNotSupported { .. }
            | Availability::LocalExecutionNotBuilt
            | Availability::ProviderNotBuilt
            | Availability::NetworkDisabled => {}
        }
        details
    }

    /// The one place an [`Availability`] becomes an error.
    ///
    /// Total over the enum on purpose: a new variant does not compile until
    /// it has a code. The typed error variants carry that code explicitly, so
    /// no wording below is load-bearing — which is what lets the messages
    /// name the spec the caller typed without a stray "provider" or
    /// "download" in it reclassifying the refusal (#3216).
    ///
    /// Every refusal carries [`Self::details`], so the caller gets the same
    /// answer `capability` would have given, with the code beside it.
    pub fn require_ready(&self) -> Result<(), InferenceError> {
        let spec = &self.spec;
        let details = Some(Box::new(self.details()));
        match &self.availability {
            Availability::Ready => Ok(()),
            Availability::NotInCatalog => Err(InferenceError::RegistryFailed {
                kind: RegistryFailure::UnknownModel,
                message: self.not_in_catalog_message(),
                details,
            }),
            Availability::PathMissing => Err(InferenceError::RegistryFailed {
                kind: RegistryFailure::UnknownModel,
                message: format!(
                    "No model file at `{}`. Pass the path of a GGUF file, or a catalog \
                     name from `strata inference models list`.",
                    self.name
                ),
                details,
            }),
            Availability::TaskNotSupported { requested } => Err(InferenceError::Unsupported {
                kind: UnsupportedKind::Operation,
                message: self.task_not_supported_message(*requested),
                details,
            }),
            Availability::LocalExecutionNotBuilt => Err(InferenceError::Unsupported {
                kind: UnsupportedKind::Operation,
                message: format!(
                    "`{spec}` is a local model, and this build has no local model \
                     execution: {LOCAL_UNAVAILABLE_REMEDY} `strata inference status` shows \
                     which of those are ready."
                ),
                details,
            }),
            Availability::ProviderNotBuilt => Err(InferenceError::Unsupported {
                kind: UnsupportedKind::Provider,
                message: format!(
                    "the {} provider is not built into this binary",
                    self.provider
                ),
                details,
            }),
            Availability::NetworkDisabled => Err(InferenceError::Unsupported {
                kind: UnsupportedKind::Operation,
                message: format!(
                    "`{spec}` is a cloud model, and network access is disabled for this \
                     runtime"
                ),
                details,
            }),
            Availability::KeyMissing { env_var, .. } => Err(InferenceError::ProviderFailed {
                kind: ProviderFailure::MissingApiKey,
                message: missing_api_key_message(self.provider, env_var),
                details,
            }),
            Availability::NotDownloaded {
                pull_spec,
                size_bytes,
            } => Err(InferenceError::RegistryFailed {
                kind: RegistryFailure::MissingModel,
                message: self.not_downloaded_message(pull_spec, *size_bytes),
                details,
            }),
        }
    }

    /// The file a local model loads from, whether or not it is there. `None`
    /// for cloud models and names the catalog does not know.
    #[must_use]
    pub fn local_path(&self) -> Option<&Path> {
        match &self.source {
            ModelSource::Catalog { path, .. } | ModelSource::GgufPath(path) => Some(path),
            ModelSource::Uncatalogued { .. } | ModelSource::Cloud => None,
        }
    }

    fn not_in_catalog_message(&self) -> String {
        match &self.source {
            ModelSource::Uncatalogued { entry: Some(entry) } => {
                let available: Vec<&str> = entry.variants.iter().map(|v| v.name).collect();
                format!(
                    "Unknown quant in `{}`: `{}` is available as {}. Run `strata inference \
                     models list` to see every model and quant.",
                    self.name,
                    entry.name,
                    available.join(", ")
                )
            }
            _ => format!(
                "Unknown model `{}`. Run `strata inference models list` to see the \
                 catalog, or pass the path of a GGUF file.",
                self.name
            ),
        }
    }

    fn task_not_supported_message(&self, requested: ModelUse) -> String {
        match &self.source {
            ModelSource::Catalog { entry, .. } => format!(
                "`{}`'s task is `{}`; it cannot {}. Run `strata inference models list` to \
                 see each model's task.",
                self.spec, entry.task, requested
            ),
            _ => format!(
                "`{}` is served by {}, which cannot {}.",
                self.spec, self.provider, requested
            ),
        }
    }

    fn not_downloaded_message(&self, pull_spec: &str, size_bytes: u64) -> String {
        let ModelSource::Catalog {
            entry,
            variant,
            path,
        } = &self.source
        else {
            // Only a catalogued model has a download to be missing; the
            // resolver never pairs this availability with another source.
            unreachable!("NotDownloaded resolved for a model that is not catalogued")
        };
        format!(
            "Model `{}` is not downloaded.\n\n\
             To download it ({}, requires internet):\n  \
             strata inference models pull {}\n\n\
             Or place the GGUF file at:\n  \
             {}\n\n\
             Expected file: {}\n\
             Source: https://huggingface.co/{}",
            self.name,
            format_size(size_bytes),
            pull_spec,
            path.display(),
            variant.hf_file,
            entry.hf_repo
        )
    }
}

/// Resolves a spec against a registry and this build.
///
/// A pure function of its inputs: the spec, the catalog and models
/// directory behind `registry`, the runtime's network setting, whether a
/// provider's key is present, and what this binary was compiled with. It
/// reads no environment of its own — `key_present` is asked, and only when
/// the check before it passed, so a network-disabled runtime never touches
/// the key at all.
///
/// Errs only when the spec is malformed. Everything else is an
/// [`Availability`].
pub(crate) fn resolve(
    registry: &ModelRegistry,
    network_enabled: bool,
    key_present: &dyn Fn(ProviderKind) -> bool,
    spec: &str,
    use_: Option<ModelUse>,
) -> Result<ResolvedModel, InferenceError> {
    let (provider, name) = parse_model_spec(spec)?;
    let (source, availability) = if provider == ProviderKind::Local {
        resolve_local(registry, &name, use_)
    } else {
        resolve_cloud(provider, network_enabled, key_present, use_)
    };
    Ok(ResolvedModel {
        spec: spec.trim().to_owned(),
        provider,
        name,
        source,
        availability,
    })
}

fn resolve_local(
    registry: &ModelRegistry,
    name: &str,
    use_: Option<ModelUse>,
) -> (ModelSource, Availability) {
    if looks_like_path(name) {
        let path = PathBuf::from(name);
        let availability = if !path.is_file() {
            Availability::PathMissing
        } else if use_.is_some() && !cfg!(feature = "local") {
            Availability::LocalExecutionNotBuilt
        } else {
            Availability::Ready
        };
        return (ModelSource::GgufPath(path), availability);
    }

    match registry.lookup(name) {
        CatalogLookup::UnknownModel => (
            ModelSource::Uncatalogued { entry: None },
            Availability::NotInCatalog,
        ),
        CatalogLookup::UnknownQuant { entry } => (
            ModelSource::Uncatalogued { entry: Some(entry) },
            Availability::NotInCatalog,
        ),
        CatalogLookup::Found {
            entry,
            variant,
            path,
            downloaded,
        } => {
            let abilities = ModelAbilities::of(ProviderKind::Local, Some(entry.task));
            let availability = match use_ {
                Some(requested) if !supports(abilities, requested) => {
                    Availability::TaskNotSupported { requested }
                }
                Some(_) if !cfg!(feature = "local") => Availability::LocalExecutionNotBuilt,
                _ if !downloaded => Availability::NotDownloaded {
                    pull_spec: pull_spec(entry, variant),
                    size_bytes: variant.size_bytes,
                },
                _ => Availability::Ready,
            };
            (
                ModelSource::Catalog {
                    entry,
                    variant,
                    path,
                },
                availability,
            )
        }
    }
}

fn resolve_cloud(
    provider: ProviderKind,
    network_enabled: bool,
    key_present: &dyn Fn(ProviderKind) -> bool,
    use_: Option<ModelUse>,
) -> (ModelSource, Availability) {
    let abilities = ModelAbilities::of(provider, None);
    // Locating a cloud model asks nothing of the build, the network, or the
    // key: there is no file to find, and every remaining check is about
    // reaching the provider, which only a run does. `pull` refuses a cloud
    // spec on its source, not on its availability.
    let availability = match use_ {
        None => Availability::Ready,
        Some(requested) if !supports(abilities, requested) => {
            Availability::TaskNotSupported { requested }
        }
        Some(_) if !generation_provider_feature_enabled(provider) => Availability::ProviderNotBuilt,
        Some(_) if !network_enabled => Availability::NetworkDisabled,
        Some(_) if !key_present(provider) => Availability::KeyMissing {
            env_var: api_key_env_var(provider),
            config_key: format!("{provider}.api_key"),
        },
        Some(_) => Availability::Ready,
    };
    (ModelSource::Cloud, availability)
}

/// Whether a model with these abilities can be put to this use. One truth
/// table ([`ModelAbilities::of`]) answers for `capability` and for every
/// verb, so they cannot disagree about what a model does.
fn supports(abilities: ModelAbilities, use_: ModelUse) -> bool {
    match use_ {
        ModelUse::Run(ModelTask::Generate) => abilities.generate,
        ModelUse::Run(ModelTask::Embed) => abilities.embed,
        ModelUse::Run(ModelTask::Rank) => abilities.rank,
        ModelUse::Tokenize => abilities.tokenize,
    }
}

/// The spec `strata inference models pull` takes for this variant: the
/// catalog name alone selects the default quant, otherwise `name:quant`.
fn pull_spec(entry: &CatalogEntry, variant: &QuantVariant) -> String {
    if variant.name.eq_ignore_ascii_case(entry.default_quant) {
        entry.name.to_owned()
    } else {
        format!("{}:{}", entry.name, variant.name)
    }
}

/// A local model name that is a file path rather than a catalog name.
pub(crate) fn looks_like_path(model: &str) -> bool {
    model.ends_with(".gguf")
        || model.contains('/')
        || model.contains('\\')
        || Path::new(model).exists()
}

/// The message for a provider key that is nowhere the runtime looks.
///
/// Shared with `api_key` in the runtime, which reads the key after
/// `require_ready` has established it is there — so the two can only say the
/// same thing.
pub(crate) fn missing_api_key_message(provider: ProviderKind, env_var: &str) -> String {
    match crate::provider_key_info(&provider.to_string()) {
        Some(info) => format!(
            "{env_var} is not set: the {provider} API key is missing. Get a key at {url}, \
             then set it with `strata config set {name}.api_key <KEY>` or by exporting \
             {env_var}.",
            url = info.acquisition_url,
            name = info.provider,
        ),
        None => format!("{env_var} is not set: the {provider} API key is missing."),
    }
}

impl InferenceRuntime {
    /// Resolves a model spec against this runtime's registry, network setting
    /// and the process environment's provider keys. See the module docs for
    /// the order of checks; see [`ResolvedModel::require_ready`] for how an
    /// unavailable model becomes an error.
    pub fn resolve(
        &self,
        spec: &str,
        use_: Option<ModelUse>,
    ) -> Result<ResolvedModel, InferenceError> {
        resolve(
            self.registry(),
            self.network_enabled(),
            &|provider| {
                crate::runtime::env_holds_a_key(
                    std::env::var_os(api_key_env_var(provider)).as_deref(),
                )
            },
            spec,
            use_,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LOCAL_BUILT: bool = cfg!(feature = "local");

    fn registry() -> (tempfile::TempDir, ModelRegistry) {
        let dir = tempfile::tempdir().expect("tempdir");
        let registry = ModelRegistry::with_dir(dir.path().to_path_buf());
        (dir, registry)
    }

    fn resolve_in(
        registry: &ModelRegistry,
        network: bool,
        key: bool,
        spec: &str,
        use_: Option<ModelUse>,
    ) -> ResolvedModel {
        resolve(registry, network, &|_| key, spec, use_).expect("well-formed spec")
    }

    fn plant_minilm(registry: &ModelRegistry) -> PathBuf {
        let CatalogLookup::Found { path, .. } = registry.lookup("miniLM") else {
            panic!("miniLM is catalogued")
        };
        std::fs::write(&path, b"not a real model").expect("plant");
        path
    }

    // --- identity ---------------------------------------------------------

    #[test]
    fn a_malformed_spec_is_the_only_error() {
        let (_dir, registry) = registry();
        for spec in ["", "   ", "openai:", "local:"] {
            let err = resolve(&registry, true, &|_| true, spec, None).expect_err(spec);
            assert_eq!(err.code(), "inference.invalid_request", "{spec:?}");
        }
    }

    #[test]
    fn the_spec_is_kept_trimmed_and_the_name_loses_its_prefix() {
        let (_dir, registry) = registry();
        let resolved = resolve_in(&registry, true, true, "  local:miniLM ", None);
        assert_eq!(resolved.spec, "local:miniLM");
        assert_eq!(resolved.name, "miniLM");
        assert_eq!(resolved.provider, ProviderKind::Local);
        assert!(matches!(resolved.source, ModelSource::Catalog { .. }));
    }

    #[test]
    fn an_unknown_name_is_not_in_the_catalog_before_anything_else_is_checked() {
        let (_dir, registry) = registry();
        // Network off and no key would matter for a cloud spec; a local build
        // would matter for a known one. Neither applies to a name that is
        // nothing.
        for use_ in [
            None,
            Some(ModelUse::Run(ModelTask::Generate)),
            Some(ModelUse::Tokenize),
        ] {
            let resolved = resolve_in(&registry, false, false, "nope:thing", use_);
            assert_eq!(
                resolved.availability,
                Availability::NotInCatalog,
                "{use_:?}"
            );
            assert!(matches!(
                resolved.source,
                ModelSource::Uncatalogued { entry: None }
            ));
            let err = resolved.require_ready().expect_err("not ready");
            assert_eq!(err.code(), "inference.unknown_model");
            let message = err.to_string();
            assert!(message.contains("Unknown model"), "{message}");
            assert!(
                message.contains("strata inference models list"),
                "{message}"
            );
        }
    }

    #[test]
    fn an_unknown_quant_names_the_model_and_lists_its_quants() {
        let (_dir, registry) = registry();
        let resolved = resolve_in(&registry, true, true, "tinyllama:iq2_xs", None);
        assert_eq!(resolved.availability, Availability::NotInCatalog);
        let ModelSource::Uncatalogued { entry: Some(entry) } = resolved.source else {
            panic!("the model part matched: {:?}", resolved.source)
        };
        assert_eq!(entry.name, "tinyllama");
        let err = resolved.require_ready().expect_err("not ready");
        assert_eq!(err.code(), "inference.unknown_model");
        let message = err.to_string();
        assert!(message.contains("Unknown quant"), "{message}");
        assert!(message.contains("q4_k_m"), "{message}");
        assert!(message.contains("q8_0"), "{message}");
    }

    #[test]
    fn a_path_that_is_not_a_file_is_missing_whatever_the_build() {
        let (dir, registry) = registry();
        let absent = dir.path().join("absent.gguf");
        let spec = absent.to_string_lossy().into_owned();
        for use_ in [None, Some(ModelUse::Run(ModelTask::Generate))] {
            let resolved = resolve_in(&registry, true, true, &spec, use_);
            assert_eq!(resolved.availability, Availability::PathMissing, "{use_:?}");
            assert_eq!(resolved.local_path(), Some(absent.as_path()));
            let err = resolved.require_ready().expect_err("not ready");
            assert_eq!(err.code(), "inference.unknown_model");
        }
        // A directory at the path is not a model file either.
        let spec = dir.path().to_string_lossy().into_owned();
        let resolved = resolve_in(&registry, true, true, &spec, None);
        assert_eq!(resolved.availability, Availability::PathMissing);
    }

    #[test]
    fn a_present_path_is_ready_to_locate_and_ready_to_run_only_with_local_execution() {
        let (dir, registry) = registry();
        let present = dir.path().join("present.gguf");
        std::fs::write(&present, b"junk").expect("write");
        let spec = present.to_string_lossy().into_owned();

        let located = resolve_in(&registry, false, false, &spec, None);
        assert_eq!(located.availability, Availability::Ready);
        assert!(matches!(located.source, ModelSource::GgufPath(_)));

        let run = resolve_in(&registry, false, false, &spec, Some(ModelUse::Tokenize));
        let expected = if LOCAL_BUILT {
            Availability::Ready
        } else {
            Availability::LocalExecutionNotBuilt
        };
        assert_eq!(run.availability, expected);
    }

    // --- catalogued local models -------------------------------------------

    #[test]
    fn a_catalogued_model_that_is_not_on_disk_is_not_downloaded() {
        let (_dir, registry) = registry();
        let resolved = resolve_in(&registry, true, true, "miniLM", None);
        let Availability::NotDownloaded {
            pull_spec,
            size_bytes,
        } = &resolved.availability
        else {
            panic!("{:?}", resolved.availability)
        };
        assert_eq!(pull_spec, "miniLM");
        assert!(*size_bytes > 0);
        assert!(resolved.local_path().is_some());

        let err = resolved.require_ready().expect_err("not ready");
        assert_eq!(err.code(), "inference.missing_model");
        let message = err.to_string();
        assert!(message.contains("is not downloaded"), "{message}");
        assert!(
            message.contains("strata inference models pull miniLM"),
            "{message}"
        );
        assert!(message.contains(".gguf"), "{message}");
        assert!(message.contains("huggingface.co"), "{message}");
        assert!(
            message.contains("MB") || message.contains("GB"),
            "{message}"
        );
    }

    #[test]
    fn a_non_default_quant_pulls_by_name_and_quant() {
        let (_dir, registry) = registry();
        let resolved = resolve_in(&registry, true, true, "tinyllama:q8_0", None);
        let Availability::NotDownloaded { pull_spec, .. } = &resolved.availability else {
            panic!("{:?}", resolved.availability)
        };
        assert_eq!(pull_spec, "tinyllama:q8_0");
    }

    #[test]
    fn a_zero_length_file_is_not_downloaded() {
        let (_dir, registry) = registry();
        let path = plant_minilm(&registry);
        std::fs::write(&path, b"").expect("truncate");
        let resolved = resolve_in(&registry, true, true, "miniLM", None);
        assert!(matches!(
            resolved.availability,
            Availability::NotDownloaded { .. }
        ));
    }

    #[test]
    fn a_planted_model_is_ready_to_locate_in_every_build() {
        let (_dir, registry) = registry();
        let path = plant_minilm(&registry);
        let resolved = resolve_in(&registry, false, false, "miniLM", None);
        assert_eq!(resolved.availability, Availability::Ready);
        assert_eq!(resolved.local_path(), Some(path.as_path()));
    }

    #[test]
    fn running_a_planted_model_needs_local_execution() {
        let (_dir, registry) = registry();
        plant_minilm(&registry);
        let resolved = resolve_in(
            &registry,
            true,
            true,
            "miniLM",
            Some(ModelUse::Run(ModelTask::Embed)),
        );
        let expected = if LOCAL_BUILT {
            Availability::Ready
        } else {
            Availability::LocalExecutionNotBuilt
        };
        assert_eq!(resolved.availability, expected);
    }

    #[test]
    fn the_wrong_task_is_refused_before_the_build_and_the_download_are_considered() {
        let (_dir, registry) = registry();
        // miniLM embeds; asking it to generate is refused whether or not the
        // file is there and whether or not this build could run it.
        for planted in [false, true] {
            if planted {
                plant_minilm(&registry);
            }
            let resolved = resolve_in(
                &registry,
                true,
                true,
                "miniLM",
                Some(ModelUse::Run(ModelTask::Generate)),
            );
            assert_eq!(
                resolved.availability,
                Availability::TaskNotSupported {
                    requested: ModelUse::Run(ModelTask::Generate)
                },
                "planted={planted}"
            );
            let err = resolved.require_ready().expect_err("not ready");
            assert_eq!(err.code(), "inference.unsupported_operation");
        }
    }

    /// The refusal says what the model does instead: a catalogued model names
    /// its task and where every model's task is listed; a cloud model names
    /// the provider that lacks the task.
    #[test]
    fn the_wrong_task_refusal_says_what_the_model_does_instead() {
        let (_dir, registry) = registry();
        let err = resolve_in(
            &registry,
            true,
            true,
            "miniLM",
            Some(ModelUse::Run(ModelTask::Generate)),
        )
        .require_ready()
        .expect_err("miniLM embeds");
        assert_eq!(err.code(), "inference.unsupported_operation");
        let message = err.to_string();
        assert!(message.contains("`miniLM`'s task is `embed`"), "{message}");
        assert!(message.contains("cannot generate"), "{message}");
        assert!(
            message.contains("strata inference models list"),
            "{message}"
        );

        let err = resolve_in(
            &registry,
            true,
            true,
            "anthropic:m",
            Some(ModelUse::Run(ModelTask::Embed)),
        )
        .require_ready()
        .expect_err("anthropic has no embedding API");
        assert_eq!(err.code(), "inference.unsupported_operation");
        let message = err.to_string();
        assert!(message.contains("served by anthropic"), "{message}");
        assert!(message.contains("cannot embed"), "{message}");
        assert!(
            !message.contains("strata inference models list"),
            "the catalog does not list cloud models: {message}"
        );
    }

    #[test]
    fn every_catalogued_model_tokenizes_but_only_for_its_own_task_runs() {
        for entry in crate::registry::catalog::CATALOG {
            let abilities = ModelAbilities::of(ProviderKind::Local, Some(entry.task));
            assert!(supports(abilities, ModelUse::Tokenize), "{}", entry.name);
            for task in [ModelTask::Generate, ModelTask::Embed, ModelTask::Rank] {
                assert_eq!(
                    supports(abilities, ModelUse::Run(task)),
                    task == entry.task,
                    "{} asked to {task}",
                    entry.name
                );
            }
        }
    }

    #[test]
    fn not_built_comes_before_not_downloaded() {
        // In a build without local execution, a catalogued model that is not
        // on disk is refused for the build, not for the download: fetching it
        // would not help.
        let (_dir, registry) = registry();
        let resolved = resolve_in(
            &registry,
            true,
            true,
            "miniLM",
            Some(ModelUse::Run(ModelTask::Embed)),
        );
        let expected = if LOCAL_BUILT {
            Availability::NotDownloaded {
                pull_spec: "miniLM".to_owned(),
                size_bytes: match &resolved.source {
                    ModelSource::Catalog { variant, .. } => variant.size_bytes,
                    other => panic!("{other:?}"),
                },
            }
        } else {
            Availability::LocalExecutionNotBuilt
        };
        assert_eq!(resolved.availability, expected);
    }

    #[test]
    fn the_local_execution_refusal_says_what_to_do() {
        let (_dir, registry) = registry();
        plant_minilm(&registry);
        let resolved = ResolvedModel {
            availability: Availability::LocalExecutionNotBuilt,
            ..resolve_in(&registry, true, true, "miniLM", None)
        };
        let err = resolved.require_ready().expect_err("not ready");
        assert_eq!(err.code(), "inference.unsupported_operation");
        let message = err.to_string();
        assert!(
            message.contains("strata inference install-local"),
            "{message}"
        );
        assert!(message.contains("openai:"), "{message}");
        assert!(message.contains("strata inference status"), "{message}");
    }

    // --- cloud models --------------------------------------------------------

    fn built(provider: ProviderKind) -> bool {
        generation_provider_feature_enabled(provider)
    }

    /// Locating asks only what the model is: a cloud spec locates in any
    /// build, with the network off, and with no key — `capability` answers
    /// in all of those, and `pull` refuses on the source instead.
    #[test]
    fn a_cloud_model_is_ready_to_locate_without_build_network_or_key() {
        let (_dir, registry) = registry();
        for provider in [
            ProviderKind::OpenAI,
            ProviderKind::Anthropic,
            ProviderKind::Google,
        ] {
            let spec = format!("{provider}:m");
            let resolved = resolve_in(&registry, false, false, &spec, None);
            assert!(matches!(resolved.source, ModelSource::Cloud));
            assert_eq!(resolved.local_path(), None);
            assert_eq!(resolved.availability, Availability::Ready, "{spec}");
        }
    }

    #[test]
    fn cloud_checks_run_task_then_build_then_network_then_key() {
        let (_dir, registry) = registry();
        let generate = Some(ModelUse::Run(ModelTask::Generate));
        for provider in [
            ProviderKind::OpenAI,
            ProviderKind::Anthropic,
            ProviderKind::Google,
        ] {
            let spec = format!("{provider}:m");
            let expect = |network: bool, key: bool| -> Availability {
                if !built(provider) {
                    Availability::ProviderNotBuilt
                } else if !network {
                    Availability::NetworkDisabled
                } else if !key {
                    Availability::KeyMissing {
                        env_var: api_key_env_var(provider),
                        config_key: format!("{provider}.api_key"),
                    }
                } else {
                    Availability::Ready
                }
            };
            for (network, key) in [(false, false), (false, true), (true, false), (true, true)] {
                let resolved = resolve_in(&registry, network, key, &spec, generate);
                assert_eq!(
                    resolved.availability,
                    expect(network, key),
                    "{spec} network={network} key={key}"
                );
            }
        }
    }

    #[test]
    fn the_key_is_not_consulted_until_the_network_check_passes() {
        let (_dir, registry) = registry();
        let asked = std::cell::Cell::new(false);
        let key_present = |_: ProviderKind| {
            asked.set(true);
            true
        };
        let resolved = resolve(
            &registry,
            false,
            &key_present,
            "openai:m",
            Some(ModelUse::Run(ModelTask::Generate)),
        )
        .expect("well-formed");
        if built(ProviderKind::OpenAI) {
            assert_eq!(resolved.availability, Availability::NetworkDisabled);
        }
        assert!(!asked.get(), "the key was read with the network off");
    }

    #[test]
    fn cloud_refusals_carry_their_codes() {
        let (_dir, registry) = registry();
        let base = resolve_in(&registry, true, true, "openai:m", None);
        let cases = [
            (
                Availability::ProviderNotBuilt,
                "inference.unsupported_provider",
            ),
            (
                Availability::NetworkDisabled,
                "inference.unsupported_operation",
            ),
            (
                Availability::KeyMissing {
                    env_var: "OPENAI_API_KEY",
                    config_key: "openai.api_key".to_owned(),
                },
                "inference.missing_api_key",
            ),
            (
                Availability::TaskNotSupported {
                    requested: ModelUse::Tokenize,
                },
                "inference.unsupported_operation",
            ),
        ];
        for (availability, code) in cases {
            let resolved = ResolvedModel {
                availability: availability.clone(),
                ..base.clone()
            };
            let err = resolved.require_ready().expect_err("not ready");
            assert_eq!(err.code(), code, "{availability:?}");
        }
    }

    #[test]
    fn the_missing_key_message_names_the_variable_and_the_config_key() {
        let (_dir, registry) = registry();
        let resolved = ResolvedModel {
            availability: Availability::KeyMissing {
                env_var: "OPENAI_API_KEY",
                config_key: "openai.api_key".to_owned(),
            },
            ..resolve_in(&registry, true, true, "openai:m", None)
        };
        let message = resolved.require_ready().expect_err("not ready").to_string();
        assert!(message.contains("OPENAI_API_KEY is not set"), "{message}");
        assert!(
            message.contains("strata config set openai.api_key"),
            "{message}"
        );
        assert!(message.contains("platform.openai.com"), "{message}");
    }

    #[test]
    fn cloud_models_do_not_tokenize_or_rank_and_anthropic_does_not_embed() {
        let (_dir, registry) = registry();
        let refused = [
            ("openai:m", ModelUse::Tokenize),
            ("openai:m", ModelUse::Run(ModelTask::Rank)),
            ("google:m", ModelUse::Run(ModelTask::Rank)),
            ("anthropic:m", ModelUse::Run(ModelTask::Embed)),
        ];
        for (spec, use_) in refused {
            let resolved = resolve_in(&registry, true, true, spec, Some(use_));
            assert_eq!(
                resolved.availability,
                Availability::TaskNotSupported { requested: use_ },
                "{spec} {use_}"
            );
        }
        for spec in ["openai:m", "google:m"] {
            let resolved = resolve_in(
                &registry,
                true,
                true,
                spec,
                Some(ModelUse::Run(ModelTask::Embed)),
            );
            assert_ne!(
                resolved.availability,
                Availability::TaskNotSupported {
                    requested: ModelUse::Run(ModelTask::Embed)
                },
                "{spec}"
            );
        }
    }

    // --- details ----------------------------------------------------------------

    /// One `ResolvedModel` per `Availability` variant. `NotDownloaded` is a
    /// real catalogued resolution (its message reads the catalog entry); the
    /// rest are stamped onto a real cloud resolution so `spec`/`provider`/
    /// `name` are the resolver's own.
    fn one_of_each_availability(registry: &ModelRegistry) -> Vec<ResolvedModel> {
        let base = resolve_in(registry, true, true, "openai:m", None);
        let mut models: Vec<ResolvedModel> = [
            Availability::Ready,
            Availability::NotInCatalog,
            Availability::PathMissing,
            Availability::TaskNotSupported {
                requested: ModelUse::Tokenize,
            },
            Availability::LocalExecutionNotBuilt,
            Availability::ProviderNotBuilt,
            Availability::NetworkDisabled,
            Availability::KeyMissing {
                env_var: "OPENAI_API_KEY",
                config_key: "openai.api_key".to_owned(),
            },
        ]
        .into_iter()
        .map(|availability| ResolvedModel {
            availability,
            ..base.clone()
        })
        .collect();
        let not_downloaded = resolve_in(registry, true, true, "miniLM", None);
        assert!(matches!(
            not_downloaded.availability,
            Availability::NotDownloaded { .. }
        ));
        models.push(not_downloaded);
        models
    }

    /// The key set `strata.error.details.inference.v1` promises per
    /// availability: the three constant keys, plus the payload keys of the
    /// variants that have one. This is the schema's only definition, so the
    /// table is spelled out rather than derived.
    #[test]
    fn details_serialize_to_the_promised_key_set_for_every_availability() {
        let (_dir, registry) = registry();
        let models = one_of_each_availability(&registry);
        let mut seen = std::collections::HashSet::new();
        for resolved in &models {
            let value = serde_json::to_value(resolved.details()).expect("serializable");
            let object = value.as_object().expect("a JSON object");
            let mut keys: Vec<&str> = object.keys().map(String::as_str).collect();
            keys.sort_unstable();
            let mut expected = vec!["availability", "model", "provider"];
            match &resolved.availability {
                Availability::KeyMissing { .. } => expected.extend(["config_key", "key_env_var"]),
                Availability::NotDownloaded { .. } => expected.extend(["pull_spec", "size_bytes"]),
                _ => {}
            }
            expected.sort_unstable();
            assert_eq!(keys, expected, "{:?}", resolved.availability);
            assert_eq!(object["model"], resolved.name.as_str());
            assert_eq!(object["provider"], resolved.provider.to_string());
            seen.insert(object["availability"].clone());
        }
        // Every variant has its own wire word, and the set covers the enum.
        assert_eq!(seen.len(), models.len());
        assert!(seen.contains(&serde_json::json!("not_downloaded")));
        assert!(seen.contains(&serde_json::json!("key_missing")));
    }

    #[test]
    fn details_carry_the_payload_of_the_variants_that_have_one() {
        let (_dir, registry) = registry();
        for resolved in one_of_each_availability(&registry) {
            let details = resolved.details();
            assert_eq!(details.availability, resolved.availability.kind());
            match &resolved.availability {
                Availability::KeyMissing {
                    env_var,
                    config_key,
                } => {
                    assert_eq!(details.key_env_var.as_deref(), Some(*env_var));
                    assert_eq!(details.config_key.as_deref(), Some(config_key.as_str()));
                    assert_eq!(details.pull_spec, None);
                    assert_eq!(details.size_bytes, None);
                }
                Availability::NotDownloaded {
                    pull_spec,
                    size_bytes,
                } => {
                    assert_eq!(details.pull_spec.as_deref(), Some(pull_spec.as_str()));
                    assert_eq!(details.size_bytes, Some(*size_bytes));
                    assert_eq!(details.key_env_var, None);
                    assert_eq!(details.config_key, None);
                }
                _ => {
                    assert_eq!(details.pull_spec, None);
                    assert_eq!(details.size_bytes, None);
                    assert_eq!(details.key_env_var, None);
                    assert_eq!(details.config_key, None);
                }
            }
            // The executor sets this; the resolver never knows a collection.
            assert_eq!(details.collection, None);
        }
    }

    #[test]
    fn every_refusal_carries_the_same_details_capability_would_report() {
        let (_dir, registry) = registry();
        for resolved in one_of_each_availability(&registry) {
            match resolved.require_ready() {
                Ok(()) => assert_eq!(resolved.availability, Availability::Ready),
                Err(err) => assert_eq!(
                    err.availability(),
                    Some(&resolved.details()),
                    "{:?}",
                    resolved.availability
                ),
            }
        }
    }

    #[test]
    fn unknown_and_not_downloaded_are_different_codes() {
        // The two halves of what used to be one `missing_model` (#3256): a
        // name the catalog does not know can never be pulled, a catalogued
        // model that is not on disk can.
        let (dir, registry) = registry();
        let absent = dir
            .path()
            .join("absent.gguf")
            .to_string_lossy()
            .into_owned();
        let unknown = [
            resolve_in(&registry, true, true, "nope", None),
            resolve_in(&registry, true, true, "tinyllama:q99", None),
            resolve_in(&registry, true, true, &absent, None),
        ];
        for resolved in &unknown {
            let err = resolved.require_ready().expect_err("not ready");
            assert_eq!(err.code(), "inference.unknown_model", "{}", resolved.spec);
            let details = err.availability().expect("carried");
            assert_eq!(
                details.pull_spec, None,
                "nothing to pull for {}",
                resolved.spec
            );
        }
        let not_downloaded = resolve_in(&registry, true, true, "miniLM", None);
        let err = not_downloaded.require_ready().expect_err("not ready");
        assert_eq!(err.code(), "inference.missing_model");
        let details = err.availability().expect("carried");
        assert_eq!(details.availability, AvailabilityKind::NotDownloaded);
        assert_eq!(details.pull_spec.as_deref(), Some("miniLM"));
    }

    // --- helpers --------------------------------------------------------------

    /// `is_cloud` decides which engine a ready model is handed to; every
    /// source answers it, and only `Cloud` answers yes. (The dispatch it
    /// gates runs only against a real engine or a live provider, so the
    /// decision is pinned here, at the source.)
    #[test]
    fn only_the_cloud_source_is_cloud() {
        let (_dir, registry) = registry();
        let path = plant_minilm(&registry);
        let path_spec = path.to_str().expect("utf-8 tempdir").to_owned();
        let table = [
            ("miniLM", false, true),
            (path_spec.as_str(), false, true),
            ("no-such-model", false, false),
            ("openai:m", true, false),
        ];
        let mut seen = std::collections::HashSet::new();
        for (spec, cloud, has_path) in table {
            let resolved = resolve_in(&registry, true, true, spec, None);
            assert_eq!(resolved.source.is_cloud(), cloud, "{:?}", resolved.source);
            assert_eq!(
                resolved.local_path().is_some(),
                has_path,
                "{:?}",
                resolved.source
            );
            seen.insert(std::mem::discriminant(&resolved.source));
        }
        assert_eq!(seen.len(), 4, "the table covers every source variant");
    }

    #[test]
    fn a_path_is_recognised_by_extension_separator_or_existence() {
        assert!(looks_like_path("model.gguf"));
        assert!(looks_like_path("models/x"));
        assert!(looks_like_path("models\\x"));
        assert!(looks_like_path("."));
        assert!(!looks_like_path("miniLM"));
        assert!(!looks_like_path("qwen3:1.7b:q8_0"));
    }

    #[test]
    fn model_use_displays_its_verb() {
        assert_eq!(ModelUse::Run(ModelTask::Embed).to_string(), "embed");
        assert_eq!(ModelUse::Run(ModelTask::Generate).to_string(), "generate");
        assert_eq!(ModelUse::Run(ModelTask::Rank).to_string(), "rank");
        assert_eq!(ModelUse::Tokenize.to_string(), "tokenize");
    }
}
