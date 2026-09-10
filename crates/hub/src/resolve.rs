//! Hub-URL resolution — the 5-layer precedence chain from stratahub's
//! `strata-cli-hub-resolution-config.md` §2.
//!
//! First source that yields a value wins: explicit flag, environment,
//! per-project `.strata/config.toml` (walking up from the working
//! directory, stopping at a `.git` boundary or the filesystem root),
//! global user config, then the built-in default. A malformed source
//! never falls through silently — it aborts naming the source.
//!
//! Single-surface rule (the §5/Q8 amendment): this module is
//! strata-core's designated defaults surface, and [`DEFAULT_HUB_URL`]
//! is the only place a hub host may appear in source — enforced by the
//! `hub_neutrality` guard test. Every configuration layer overrides it.

use std::error::Error;
use std::fmt;
use std::path::{Path, PathBuf};

use url::Url;

/// The built-in default hub: the official StrataHub instance. Used only
/// when no configuration layer supplies a URL.
pub const DEFAULT_HUB_URL: &str = "https://hub.stratahub.io";

/// Which layer produced the resolved URL (surfaced by `config show`
/// style diagnostics).
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HubUrlSource {
    /// The `--hub` flag (or an explicit per-call override).
    Flag,
    /// The `STRATA_HUB_URL` environment variable.
    Environment,
    /// A per-project `.strata/config.toml`, at the recorded path.
    ProjectConfig(PathBuf),
    /// The global user config, at the recorded path.
    GlobalConfig(PathBuf),
    /// No layer supplied a URL: [`DEFAULT_HUB_URL`].
    Default,
}

impl fmt::Display for HubUrlSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Flag => formatter.write_str("--hub flag"),
            Self::Environment => formatter.write_str("STRATA_HUB_URL"),
            Self::ProjectConfig(path) | Self::GlobalConfig(path) => {
                write!(formatter, "{}", path.display())
            }
            Self::Default => formatter.write_str("built-in default"),
        }
    }
}

/// A resolved hub URL plus the layer it came from.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedHubUrl {
    /// The parsed base URL.
    pub url: Url,
    /// The layer that supplied it.
    pub source: HubUrlSource,
}

/// Resolution failure modes.
#[derive(Debug)]
#[non_exhaustive]
pub enum HubUrlError {
    /// A source supplied a value that does not parse as a URL, or a
    /// config file is malformed. Never falls through to lower layers.
    MalformedSource {
        /// The offending source, by name.
        source: String,
        /// Parse failure detail.
        detail: String,
    },
}

impl fmt::Display for HubUrlError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MalformedSource { source, detail } => {
                write!(formatter, "{source}: {detail}")
            }
        }
    }
}

impl Error for HubUrlError {}

/// Inputs to resolution, parameterized so callers own process state
/// (argv, environment, CWD, platform config paths) and tests inject it.
#[derive(Clone, Debug, Default)]
pub struct HubUrlInputs {
    /// Layer 1: the `--hub` flag value (or per-call override), verbatim.
    pub flag: Option<String>,
    /// Layer 2: the `STRATA_HUB_URL` value, verbatim. Empty string is
    /// treated as unset; whitespace-only is a parse error.
    pub environment: Option<String>,
    /// Layer 3 anchor: the working directory the project-config walk
    /// starts from.
    pub working_dir: Option<PathBuf>,
    /// Layer 4: the platform's global config file path.
    pub global_config: Option<PathBuf>,
}

/// Resolves the hub URL by the §2 precedence chain, falling back to
/// [`DEFAULT_HUB_URL`] when no layer supplies a value.
///
/// # Errors
///
/// [`HubUrlError::MalformedSource`] when the winning source is invalid.
pub fn resolve_hub_url(inputs: &HubUrlInputs) -> Result<ResolvedHubUrl, HubUrlError> {
    if let Some(flag) = &inputs.flag {
        return parse_layer(flag, HubUrlSource::Flag, "--hub");
    }

    if let Some(environment) = &inputs.environment {
        if environment.is_empty() {
            // Empty means unset; fall through.
        } else if environment.trim().is_empty() {
            return Err(HubUrlError::MalformedSource {
                source: "STRATA_HUB_URL".to_owned(),
                detail: "value is whitespace-only".to_owned(),
            });
        } else {
            return parse_layer(environment, HubUrlSource::Environment, "STRATA_HUB_URL");
        }
    }

    if let Some(working_dir) = &inputs.working_dir {
        if let Some(config_path) = find_project_config(working_dir) {
            // A config file without the key is simply unset here; a
            // malformed file still aborts rather than falling through.
            if let Some(url) = read_config_hub_url(&config_path)? {
                return Ok(ResolvedHubUrl {
                    url,
                    source: HubUrlSource::ProjectConfig(config_path),
                });
            }
        }
    }

    if let Some(global) = &inputs.global_config {
        if global.is_file() {
            if let Some(url) = read_config_hub_url(global)? {
                return Ok(ResolvedHubUrl {
                    url,
                    source: HubUrlSource::GlobalConfig(global.clone()),
                });
            }
        }
    }

    Ok(ResolvedHubUrl {
        url: Url::parse(DEFAULT_HUB_URL).expect("the built-in default is a valid URL"),
        source: HubUrlSource::Default,
    })
}

fn parse_layer(
    value: &str,
    source: HubUrlSource,
    source_name: &str,
) -> Result<ResolvedHubUrl, HubUrlError> {
    let url = Url::parse(value).map_err(|error| HubUrlError::MalformedSource {
        source: source_name.to_owned(),
        detail: format!("not a valid URL: {error}"),
    })?;
    Ok(ResolvedHubUrl { url, source })
}

/// Walks up from `working_dir` looking for `.strata/config.toml`,
/// stopping past a `.git` boundary or at the filesystem root.
fn find_project_config(working_dir: &Path) -> Option<PathBuf> {
    let mut current = Some(working_dir);
    while let Some(dir) = current {
        let candidate = dir.join(".strata/config.toml");
        if candidate.is_file() {
            return Some(candidate);
        }
        if dir.join(".git").exists() {
            return None;
        }
        current = dir.parent();
    }
    None
}

/// Reads `hub.url` from a config file. `Ok(None)` when the key is
/// absent (the file may legitimately hold other configuration); any
/// other defect — unreadable, bad TOML, non-string or invalid URL —
/// aborts naming the source.
fn read_config_hub_url(path: &Path) -> Result<Option<Url>, HubUrlError> {
    let source = path.display().to_string();
    let text = std::fs::read_to_string(path).map_err(|error| HubUrlError::MalformedSource {
        source: source.clone(),
        detail: format!("unreadable: {error}"),
    })?;
    let value: toml::Value =
        toml::from_str(&text).map_err(|error| HubUrlError::MalformedSource {
            source: source.clone(),
            detail: format!("malformed TOML: {error}"),
        })?;
    let Some(url) = value.get("hub").and_then(|hub| hub.get("url")) else {
        return Ok(None);
    };
    let url = url.as_str().ok_or_else(|| HubUrlError::MalformedSource {
        source: source.clone(),
        detail: "[hub].url is not a string".to_owned(),
    })?;
    Url::parse(url)
        .map(Some)
        .map_err(|error| HubUrlError::MalformedSource {
            source,
            detail: format!("[hub].url is not a valid URL: {error}"),
        })
}

impl HubUrlInputs {
    /// Gathers resolution inputs from the process environment: the
    /// caller's explicit flag, `STRATA_HUB_URL`, the working directory
    /// (for the project-config walk), and the platform's global config
    /// path. This is the entry frontends use so resolution behavior is
    /// identical everywhere.
    #[must_use]
    pub fn from_process(flag: Option<String>) -> Self {
        Self {
            flag,
            environment: std::env::var("STRATA_HUB_URL").ok(),
            working_dir: std::env::current_dir().ok(),
            global_config: global_config_path(),
        }
    }
}

/// The platform's global strata config file path
/// (`<config dir>/strata/config.toml`), when the platform exposes one.
#[must_use]
pub fn global_config_path() -> Option<PathBuf> {
    dirs::config_dir().map(|dir| dir.join("strata/config.toml"))
}

/// Reads `hub.url` from the global config; `Ok(None)` when the file or
/// key is absent.
///
/// # Errors
///
/// [`HubUrlError::MalformedSource`] when the file exists but is invalid.
pub fn read_global_hub_url() -> Result<Option<Url>, HubUrlError> {
    let Some(path) = global_config_path() else {
        return Ok(None);
    };
    if !path.is_file() {
        return Ok(None);
    }
    read_config_hub_url(&path)
}

/// Writes `hub.url` into the global config, preserving other keys and
/// creating the file (`0600` on Unix) and parent directories on first
/// use. Returns the file path written.
///
/// # Errors
///
/// [`HubUrlError::MalformedSource`] when `url` is not a valid URL or the
/// existing file is unreadable/unwritable.
pub fn write_global_hub_url(url: &str) -> Result<PathBuf, HubUrlError> {
    let parsed = Url::parse(url).map_err(|error| HubUrlError::MalformedSource {
        source: "hub.url".to_owned(),
        detail: format!("not a valid URL: {error}"),
    })?;
    let path = global_config_path().ok_or_else(|| HubUrlError::MalformedSource {
        source: "global config".to_owned(),
        detail: "the platform exposes no user config directory".to_owned(),
    })?;
    edit_global_config(&path, |hub| {
        hub.insert(
            "url".to_owned(),
            toml::Value::String(parsed.as_str().to_owned()),
        );
    })?;
    Ok(path)
}

/// Removes `hub.url` from the global config, leaving other keys. Returns
/// the file path when the file existed.
///
/// # Errors
///
/// [`HubUrlError::MalformedSource`] on unreadable/unwritable state.
pub fn unset_global_hub_url() -> Result<Option<PathBuf>, HubUrlError> {
    let Some(path) = global_config_path() else {
        return Ok(None);
    };
    if !path.is_file() {
        return Ok(None);
    }
    edit_global_config(&path, |hub| {
        hub.remove("url");
    })?;
    Ok(Some(path))
}

// ---------------------------------------------------------------------------
// Provider API keys — `[providers.<name>].api_key` in the *global* config only.
//
// Deliberately global-only (unlike `hub.url`, which has a per-project layer): a
// key in a project's `.strata/config.toml` risks being committed to source
// control. Environment variables still take precedence over these: the
// executor's provider settings ask the environment first and this file second
// (#3221). Provider names are opaque here — the CLI validates them against the
// known cloud providers.
//
// The reader takes the file's path, like `HubUrlInputs` does for `hub.url`, so
// the executor can fix the platform path once and tests can point it at a
// temp file; the writers below resolve the global path themselves because
// only the CLI's `config set/unset` call them.
// ---------------------------------------------------------------------------

/// Reads `[providers.<provider>].api_key` from the config file at `path`;
/// `Ok(None)` when the file, section, or key is absent.
///
/// # Errors
///
/// [`HubUrlError::MalformedSource`] when the file exists but is invalid.
pub fn read_provider_key(path: &Path, provider: &str) -> Result<Option<String>, HubUrlError> {
    if !path.is_file() {
        return Ok(None);
    }
    let source = path.display().to_string();
    let text = std::fs::read_to_string(path).map_err(|error| HubUrlError::MalformedSource {
        source: source.clone(),
        detail: format!("unreadable: {error}"),
    })?;
    let value: toml::Value =
        toml::from_str(&text).map_err(|error| HubUrlError::MalformedSource {
            source: source.clone(),
            detail: format!("malformed TOML: {error}"),
        })?;
    let key = value
        .get("providers")
        .and_then(|providers| providers.get(provider))
        .and_then(|table| table.get("api_key"));
    let Some(key) = key else {
        return Ok(None);
    };
    let key = key.as_str().ok_or_else(|| HubUrlError::MalformedSource {
        source,
        detail: format!("[providers.{provider}].api_key is not a string"),
    })?;
    Ok(Some(key.to_owned()))
}

/// Writes `[providers.<provider>].api_key` into the global config, preserving
/// other keys and creating the file (`0600` on Unix) on first use. Returns the
/// file path written.
///
/// # Errors
///
/// [`HubUrlError::MalformedSource`] when the config directory is unavailable or
/// the existing file is unreadable/unwritable.
pub fn write_global_provider_key(provider: &str, api_key: &str) -> Result<PathBuf, HubUrlError> {
    let path = global_config_path().ok_or_else(|| HubUrlError::MalformedSource {
        source: "global config".to_owned(),
        detail: "the platform exposes no user config directory".to_owned(),
    })?;
    edit_global_providers(&path, provider, |table| {
        table.insert(
            "api_key".to_owned(),
            toml::Value::String(api_key.to_owned()),
        );
    })?;
    Ok(path)
}

/// Removes `[providers.<provider>].api_key` from the global config. Returns the
/// file path when the file existed.
///
/// # Errors
///
/// [`HubUrlError::MalformedSource`] on unreadable/unwritable state.
pub fn unset_global_provider_key(provider: &str) -> Result<Option<PathBuf>, HubUrlError> {
    let Some(path) = global_config_path() else {
        return Ok(None);
    };
    if !path.is_file() {
        return Ok(None);
    }
    edit_global_providers(&path, provider, |table| {
        table.remove("api_key");
    })?;
    Ok(Some(path))
}

/// Edits the `[providers.<provider>]` sub-table of the global config, reading,
/// mutating, and writing back with restrictive permissions. Self-contained to
/// keep the `hub.url` path (`edit_global_config`) untouched.
fn edit_global_providers(
    path: &Path,
    provider: &str,
    edit: impl FnOnce(&mut toml::map::Map<String, toml::Value>),
) -> Result<(), HubUrlError> {
    let source = path.display().to_string();
    let malformed = |detail: String| HubUrlError::MalformedSource {
        source: source.clone(),
        detail,
    };
    let mut root: toml::Value = if path.is_file() {
        let text = std::fs::read_to_string(path)
            .map_err(|error| malformed(format!("unreadable: {error}")))?;
        toml::from_str(&text).map_err(|error| malformed(format!("malformed TOML: {error}")))?
    } else {
        toml::Value::Table(toml::map::Map::new())
    };
    let table = root
        .as_table_mut()
        .ok_or_else(|| malformed("config root is not a table".to_owned()))?;
    let providers = table
        .entry("providers")
        .or_insert_with(|| toml::Value::Table(toml::map::Map::new()));
    let providers = providers
        .as_table_mut()
        .ok_or_else(|| malformed("[providers] is not a table".to_owned()))?;
    let provider_table = providers
        .entry(provider.to_owned())
        .or_insert_with(|| toml::Value::Table(toml::map::Map::new()));
    let provider_table = provider_table
        .as_table_mut()
        .ok_or_else(|| malformed(format!("[providers.{provider}] is not a table")))?;
    edit(provider_table);

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|error| malformed(format!("config directory: {error}")))?;
    }
    let rendered = toml::to_string_pretty(&root)
        .map_err(|error| malformed(format!("config serialization: {error}")))?;
    std::fs::write(path, rendered).map_err(|error| malformed(format!("write failed: {error}")))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
    }
    Ok(())
}

fn edit_global_config(
    path: &Path,
    edit: impl FnOnce(&mut toml::map::Map<String, toml::Value>),
) -> Result<(), HubUrlError> {
    let source = path.display().to_string();
    let malformed = |detail: String| HubUrlError::MalformedSource {
        source: source.clone(),
        detail,
    };
    let mut root: toml::Value = if path.is_file() {
        let text = std::fs::read_to_string(path)
            .map_err(|error| malformed(format!("unreadable: {error}")))?;
        toml::from_str(&text).map_err(|error| malformed(format!("malformed TOML: {error}")))?
    } else {
        toml::Value::Table(toml::map::Map::new())
    };
    let table = root
        .as_table_mut()
        .ok_or_else(|| malformed("config root is not a table".to_owned()))?;
    let hub = table
        .entry("hub")
        .or_insert_with(|| toml::Value::Table(toml::map::Map::new()));
    let hub = hub
        .as_table_mut()
        .ok_or_else(|| malformed("[hub] is not a table".to_owned()))?;
    edit(hub);

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|error| malformed(format!("config directory: {error}")))?;
    }
    let rendered = toml::to_string_pretty(&root)
        .map_err(|error| malformed(format!("config serialization: {error}")))?;
    std::fs::write(path, rendered).map_err(|error| malformed(format!("write failed: {error}")))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
    }
    Ok(())
}
