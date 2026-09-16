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
/// What the user config file is, for a diagnostic that must tell "nothing was
/// ever stored" from "something was stored and cannot be reached".
///
/// Every read path folds a failure into "no value", deliberately: the runtime
/// asks for settings on every status and provider call, and a broken file must
/// not turn `inference status` into an error. The cost is that a key the user
/// did store reads back exactly like one they never set, so `doctor` needs a
/// way to look at the file itself (#3296).
/// Deliberately exhaustive, where Rule 28 makes public *error* enums
/// `#[non_exhaustive]`. A `_` arm at the call site would let a state added
/// later default to "nothing is wrong", and the one caller that matters is
/// `doctor`, which install scripts run as their last step. A new state should
/// break the build until someone decides whether it is a fault.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigFileState {
    /// No file at that path. The default install, and not a fault.
    Absent,
    /// Present, readable, and valid TOML. It may still store nothing.
    Readable,
    /// Present but its bytes could not be read: permissions, or a directory
    /// where a file belongs.
    Unreadable,
    /// Read, but not valid TOML. Anything stored in it is unreachable.
    Malformed,
}

impl ConfigFileState {
    /// The one word this state reports as, on every surface that reports it:
    /// `strata doctor`'s `config_file.state`, and `inference status`'s
    /// (#3423).
    ///
    /// One spelling, so an agent that learns these four words from one
    /// command matches them on the other. The `inference` wire enum is a
    /// separate type — that crate imports nothing from this workspace — and
    /// an executor test holds its serde names to exactly these.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Absent => "absent",
            Self::Readable => "readable",
            Self::Unreadable => "unreadable",
            Self::Malformed => "malformed",
        }
    }
}

/// Looks at the user config file without asking it for any particular value.
///
/// Deliberately not built on [`read_provider_setting`]: that answers "is there
/// a value for this provider", which is `None` both for a file that stores
/// nothing and for one that cannot be parsed. This answers the other question.
pub fn inspect_config(path: &Path) -> ConfigFileState {
    if !path.exists() {
        return ConfigFileState::Absent;
    }
    let Ok(text) = std::fs::read_to_string(path) else {
        return ConfigFileState::Unreadable;
    };
    if toml::from_str::<toml::Value>(&text).is_ok() {
        ConfigFileState::Readable
    } else {
        ConfigFileState::Malformed
    }
}

/// A TOML parse failure, named without quoting the file that failed.
///
/// `toml::de::Error`'s `Display` renders the offending source line as an
/// excerpt. In this file that line can be `api_key = "sk-..."`, so
/// interpolating it put a provider key into an error message, and from there
/// into a terminal, a CI log, or a pasted bug report (#3296, Rule 31).
///
/// `message()` is the cause without the excerpt and `span()` locates it, so a
/// reader still learns what went wrong and where to look, and the file is
/// named separately by `MalformedSource`.
fn malformed_toml_detail(text: &str, error: &toml::de::Error) -> String {
    let cause = error.message();
    match error.span() {
        Some(span) => {
            let (line, column) = line_and_column(text, span.start);
            format!("malformed TOML at line {line}, column {column}: {cause}")
        }
        None => format!("malformed TOML: {cause}"),
    }
}

/// The 1-based line and column of a byte offset, counting columns in
/// characters so a multi-byte line does not report a column past its end.
fn line_and_column(text: &str, offset: usize) -> (usize, usize) {
    // A span from a parse error is a byte offset into this same text, but it
    // is floored to a character boundary rather than trusted: slicing on a
    // mid-character byte panics, and an error message is the worst place to
    // learn that.
    let offset = (0..=offset.min(text.len()))
        .rev()
        .find(|candidate| text.is_char_boundary(*candidate))
        .unwrap_or(0);
    let before = &text[..offset];
    let line = before.matches('\n').count() + 1;
    let column = before
        .rsplit('\n')
        .next()
        .map_or(0, |current| current.chars().count())
        + 1;
    (line, column)
}

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
            detail: malformed_toml_detail(&text, &error),
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
// Reader and writers all take the file's path, like `HubUrlInputs` does for
// `hub.url`: the executor and the CLI fix the platform path once
// (`global_config_path`) and tests point every function at a temp file.
// ---------------------------------------------------------------------------

/// One setting a `[providers.<provider>]` section stores: the field the
/// `strata config` surface names after the provider (`<provider>.api_key`,
/// `<provider>.base_url`) and the inference runtime's settings read back.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderSetting {
    /// The provider's API key (`api_key`). Stored as given.
    ApiKey,
    /// The base URL the provider is reached at instead of its public
    /// endpoint (`base_url`). Must parse as an `http` or `https` URL.
    BaseUrl,
}

impl ProviderSetting {
    /// The TOML field name under `[providers.<provider>]`, which is also the
    /// suffix of the `strata config` key.
    #[must_use]
    pub const fn field(self) -> &'static str {
        match self {
            Self::ApiKey => "api_key",
            Self::BaseUrl => "base_url",
        }
    }

    /// The value as it will be stored, or why it cannot be. A key is opaque
    /// and stored as given. A base URL is checked here, at the one place it
    /// is written, because a value that is not an `http(s)` URL reaches the
    /// user only later, as a provider call that fails to connect — the wrong
    /// moment and the wrong words for a typo. The `Url` parse is a check
    /// only: the value is stored as typed, so `config get` shows what was
    /// set rather than a normalized form with a trailing slash.
    fn validated(self, provider: &str, value: &str) -> Result<String, HubUrlError> {
        match self {
            Self::ApiKey => Ok(value.to_owned()),
            Self::BaseUrl => {
                let malformed = |detail: String| HubUrlError::MalformedSource {
                    source: format!("{provider}.base_url"),
                    detail,
                };
                let parsed = Url::parse(value)
                    .map_err(|error| malformed(format!("not a valid URL: {error}")))?;
                match parsed.scheme() {
                    "http" | "https" => Ok(value.to_owned()),
                    scheme => Err(malformed(format!(
                        "not an http or https URL (scheme is `{scheme}`)"
                    ))),
                }
            }
        }
    }
}

/// Reads `[providers.<provider>].<setting>` from the config file at `path`;
/// `Ok(None)` when the file, section, or field is absent.
///
/// # Errors
///
/// [`HubUrlError::MalformedSource`] when the file exists but is invalid, or
/// the field is not a string.
pub fn read_provider_setting(
    path: &Path,
    provider: &str,
    setting: ProviderSetting,
) -> Result<Option<String>, HubUrlError> {
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
            detail: malformed_toml_detail(&text, &error),
        })?;
    let field = setting.field();
    let stored = value
        .get("providers")
        .and_then(|providers| providers.get(provider))
        .and_then(|table| table.get(field));
    let Some(stored) = stored else {
        return Ok(None);
    };
    let stored = stored
        .as_str()
        .ok_or_else(|| HubUrlError::MalformedSource {
            source,
            detail: format!("[providers.{provider}].{field} is not a string"),
        })?;
    Ok(Some(stored.to_owned()))
}

/// Writes `[providers.<provider>].<setting>` into the config file at `path`,
/// preserving other keys and creating the file (`0600` on Unix) and its
/// directory on first use.
///
/// # Errors
///
/// [`HubUrlError::MalformedSource`] when the value is not valid for the
/// setting (a base URL that is not an `http(s)` URL — nothing is written) or
/// the existing file is unreadable/unwritable.
pub fn write_provider_setting(
    path: &Path,
    provider: &str,
    setting: ProviderSetting,
    value: &str,
) -> Result<(), HubUrlError> {
    let value = setting.validated(provider, value)?;
    edit_global_providers(path, provider, |table| {
        table.insert(setting.field().to_owned(), toml::Value::String(value));
    })
}

/// Removes `[providers.<provider>].<setting>` from the config file at `path`.
/// `Ok(true)` when the file existed and was rewritten without the field;
/// `Ok(false)` when there was no file, so nothing to remove and nothing is
/// created.
///
/// # Errors
///
/// [`HubUrlError::MalformedSource`] on unreadable/unwritable state.
pub fn unset_provider_setting(
    path: &Path,
    provider: &str,
    setting: ProviderSetting,
) -> Result<bool, HubUrlError> {
    if !path.is_file() {
        return Ok(false);
    }
    edit_global_providers(path, provider, |table| {
        table.remove(setting.field());
    })?;
    Ok(true)
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
        toml::from_str(&text).map_err(|error| malformed(malformed_toml_detail(&text, &error)))?
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
        toml::from_str(&text).map_err(|error| malformed(malformed_toml_detail(&text, &error)))?
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

#[cfg(test)]
mod tests {
    use super::*;

    /// The global config write path, asserted in the crate that owns it.
    ///
    /// `strata config set hub.url` round-trips in the CLI suite, but that
    /// suite cannot speak for this code: a mutation lane runs the owning
    /// crate's tests, so `edit_global_config` replaced by `Ok(())` -- a write
    /// that silently does nothing -- survived every test here.
    ///
    /// Against an explicit path, because the public `write_global_hub_url`
    /// resolves the real user config and a test must never touch it.
    #[test]
    fn editing_the_global_config_writes_a_value_a_read_finds() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("nested").join("config.toml");

        edit_global_config(&path, |hub| {
            hub.insert(
                "url".to_owned(),
                toml::Value::String("https://hub.example.com/".to_owned()),
            );
        })
        .expect("write");

        assert!(path.is_file(), "the write created no file");
        assert_eq!(
            read_config_hub_url(&path)
                .expect("read back")
                .map(|url| url.to_string()),
            Some("https://hub.example.com/".to_owned()),
            "the value read back is not the one written"
        );
    }

    /// `line_and_column` is what tells a reader where to look once the excerpt
    /// is gone, so its arithmetic is pinned directly rather than inferred from
    /// one rendered message.
    #[test]
    fn a_byte_offset_becomes_a_one_based_line_and_column() {
        let text = "abc\ndefgh\n";
        assert_eq!(line_and_column(text, 0), (1, 1), "the very start");
        assert_eq!(line_and_column(text, 2), (1, 3), "third character");
        assert_eq!(
            line_and_column(text, 3),
            (1, 4),
            "the newline itself ends line 1"
        );
        assert_eq!(
            line_and_column(text, 4),
            (2, 1),
            "first character of line 2"
        );
        assert_eq!(line_and_column(text, 8), (2, 5));

        // Columns count characters, not bytes: a multi-byte line must not
        // report a column past its own end.
        let wide = "aé\nxé y";
        assert_eq!(
            line_and_column(wide, 3),
            (1, 3),
            "after a two-byte character"
        );
        assert_eq!(
            line_and_column(wide, 7),
            (2, 3),
            "line 2, past its two-byte character"
        );

        // An offset past the end, or inside a character, is floored rather
        // than panicking: an error message is the worst place to learn that
        // slicing a mid-character byte aborts.
        assert_eq!(
            line_and_column(wide, 2),
            (1, 2),
            "mid-character offset floors"
        );
        assert_eq!(line_and_column(text, 9_999), (3, 1), "past the end");
    }

    fn refused(setting: ProviderSetting, value: &str) -> (String, String) {
        match setting.validated("openai", value) {
            Err(HubUrlError::MalformedSource { source, detail }) => (source, detail),
            other => panic!("`{value}` must be refused, got {other:?}"),
        }
    }

    #[test]
    fn a_setting_names_its_field_which_is_the_config_key_suffix() {
        assert_eq!(ProviderSetting::ApiKey.field(), "api_key");
        assert_eq!(ProviderSetting::BaseUrl.field(), "base_url");
    }

    #[test]
    fn a_key_is_stored_as_given() {
        for value in ["sk-anything", "", "not a url at all"] {
            assert_eq!(
                ProviderSetting::ApiKey
                    .validated("openai", value)
                    .expect("a key is opaque"),
                value
            );
        }
    }

    #[test]
    fn a_base_url_is_stored_as_typed_when_it_is_an_http_url() {
        // Stored as typed: no trailing slash appended, no normalization —
        // `config get` shows what was set.
        for value in [
            "http://127.0.0.1:8000/v1",
            "https://proxy.example/openai/v1",
            "http://localhost:11434",
            "https://proxy.example/",
        ] {
            assert_eq!(
                ProviderSetting::BaseUrl
                    .validated("openai", value)
                    .expect("an http(s) URL"),
                value
            );
        }
    }

    #[test]
    fn a_base_url_without_a_scheme_or_with_another_scheme_is_refused_naming_the_key() {
        // The likely typo — a host without a scheme — parses as a URL whose
        // scheme is the host name, so the scheme check is what catches it.
        let (source, detail) = refused(ProviderSetting::BaseUrl, "localhost:8000");
        assert_eq!(source, "openai.base_url");
        assert!(detail.contains("http or https"), "{detail}");

        let (source, detail) = refused(ProviderSetting::BaseUrl, "ftp://proxy.example/v1");
        assert_eq!(source, "openai.base_url");
        assert!(detail.contains("`ftp`"), "{detail}");

        let (source, _) = refused(ProviderSetting::BaseUrl, "not a url");
        assert_eq!(source, "openai.base_url");

        let (source, _) = refused(ProviderSetting::BaseUrl, "");
        assert_eq!(source, "openai.base_url");
    }
}
