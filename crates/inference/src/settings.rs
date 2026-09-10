//! Provider settings the runtime does not own: where a provider's API key
//! and base URL come from (R4).
//!
//! The runtime never reads a key or a URL out of the process environment
//! itself. It asks the [`ProviderSettings`] it was constructed with, and asks
//! it in three places — resolution (`Availability::KeyMissing`), `status`
//! (`key_present`, `key_source`, `base_url`, `base_url_source`) and the
//! provider call — so the three can never disagree about whether a key exists
//! or where a request goes. The one implementation here reads the
//! environment; the executor composes it with the user's config file so every
//! consumer (CLI, SDK, MCP server) sees the same settings (#3221, #3270).
//! Tests inject a fixed answer instead of mutating the environment, which
//! races every other test in a binary.

use std::ffi::OsStr;
use std::fmt;
use std::path::PathBuf;

use crate::{api_key_env_var, ProviderKind};

/// Where a provider setting was found. This is what `status` reports as
/// `key_source` and `base_url_source`: a place, never a value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SettingSource {
    /// The named environment variable.
    Environment(String),
    /// The configuration file at this path (`strata config set
    /// <provider>.api_key` / `<provider>.base_url`).
    ConfigFile(PathBuf),
    /// The embedding application supplied the setting programmatically.
    Application,
}

impl SettingSource {
    /// The one-line form `status` reports: the variable's name, the file's
    /// path, or `application`.
    #[must_use]
    pub fn label(&self) -> String {
        match self {
            Self::Environment(variable) => variable.clone(),
            Self::ConfigFile(path) => path.display().to_string(),
            Self::Application => "application".to_owned(),
        }
    }
}

/// A provider's API key and where it was found.
///
/// `Debug` shows the source and never the value (Rule 31), so a key can sit
/// in a runtime that is dumped into a log or a panic message without leaking.
#[derive(Clone)]
pub struct ProviderKey {
    value: String,
    source: SettingSource,
}

impl ProviderKey {
    /// A key found at `source`.
    pub fn new(value: impl Into<String>, source: SettingSource) -> Self {
        Self {
            value: value.into(),
            source,
        }
    }

    /// Where the key was found.
    #[must_use]
    pub fn source(&self) -> &SettingSource {
        &self.source
    }

    /// The key itself. Handed to a provider client and nowhere else.
    #[must_use]
    pub fn secret(&self) -> &str {
        &self.value
    }
}

impl fmt::Debug for ProviderKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProviderKey")
            .field("value", &"<redacted>")
            .field("source", &self.source)
            .finish()
    }
}

/// A base URL that replaces a provider's public endpoint, and where it was
/// found. Not a secret: `status` reports the URL itself so a caller can see
/// where a request would go.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderBaseUrl {
    url: String,
    source: SettingSource,
}

impl ProviderBaseUrl {
    /// A base URL found at `source`. Trailing slashes are dropped: the
    /// provider clients append their own paths (`/chat/completions`,
    /// `/v1/messages`, …), and `http://host:8000/v1/` would otherwise send
    /// them to `//chat/completions`.
    pub fn new(url: impl Into<String>, source: SettingSource) -> Self {
        let mut url = url.into();
        while url.ends_with('/') {
            url.pop();
        }
        Self { url, source }
    }

    /// The URL, without a trailing slash.
    #[must_use]
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Where the URL was found.
    #[must_use]
    pub fn source(&self) -> &SettingSource {
        &self.source
    }
}

/// Where the runtime finds provider settings it does not own.
///
/// One source per runtime, injected by [`InferenceRuntime::with_settings`]
/// (`InferenceRuntime::new` installs [`EnvProviderSettings`]). Implementations
/// answer for the moment they are asked: a key set after the runtime was built
/// is found on the next call.
///
/// [`InferenceRuntime::with_settings`]: crate::InferenceRuntime::with_settings
pub trait ProviderSettings: Send + Sync {
    /// The key for `provider`, if this source holds one. `None` for a
    /// provider that needs no key, and for a blank value.
    fn key(&self, provider: ProviderKind) -> Option<ProviderKey>;

    /// The base URL to reach `provider` at instead of its public endpoint
    /// (#3270): an OpenAI-compatible server, a proxy, a test double. `None`
    /// sends requests to the public endpoint. The URL has the meaning the
    /// provider's own SDK gives its base-URL setting — see
    /// [`default_base_url`] for what each one is rooted at.
    fn base_url(&self, _provider: ProviderKind) -> Option<ProviderBaseUrl> {
        None
    }
}

/// Where OpenAI requests go when nothing overrides it. Includes the `/v1`
/// segment, as the OpenAI SDK's `OPENAI_BASE_URL` does: the client appends
/// `/chat/completions` and `/embeddings`.
pub(crate) const OPENAI_BASE_URL: &str = "https://api.openai.com/v1";

/// Where Anthropic requests go when nothing overrides it. The origin alone,
/// as the Anthropic SDK's `ANTHROPIC_BASE_URL` is: the client appends
/// `/v1/messages`.
pub(crate) const ANTHROPIC_BASE_URL: &str = "https://api.anthropic.com";

/// Where Google Gemini requests go when nothing overrides it. The origin
/// alone, as the Google GenAI SDK's base-URL option is: the client appends
/// `/v1beta/models/{model}:generateContent` and its embedding siblings.
pub(crate) const GOOGLE_BASE_URL: &str = "https://generativelanguage.googleapis.com";

/// The public endpoint a provider is reached at when no base URL overrides
/// it, rooted where that provider's own SDK roots its base-URL setting so a
/// value written for the SDK works unchanged here. `None` for the local
/// provider, which is not reached over HTTP.
///
/// Not gated on the cloud features, for the same reason as
/// [`api_key_env_var`]: `status` reports every provider.
pub(crate) const fn default_base_url(provider: ProviderKind) -> Option<&'static str> {
    match provider {
        ProviderKind::OpenAI => Some(OPENAI_BASE_URL),
        ProviderKind::Anthropic => Some(ANTHROPIC_BASE_URL),
        ProviderKind::Google => Some(GOOGLE_BASE_URL),
        ProviderKind::Local => None,
    }
}

/// The environment variable that overrides a provider's base URL — the one
/// its own SDK reads, so a shell already set up for the SDK is set up for
/// Strata. `None` for the local provider.
pub(crate) const fn base_url_env_var(provider: ProviderKind) -> Option<&'static str> {
    match provider {
        ProviderKind::OpenAI => Some("OPENAI_BASE_URL"),
        ProviderKind::Anthropic => Some("ANTHROPIC_BASE_URL"),
        ProviderKind::Google => Some("GOOGLE_GEMINI_BASE_URL"),
        ProviderKind::Local => None,
    }
}

/// The process environment: `OPENAI_API_KEY`, `OPENAI_BASE_URL` and their
/// siblings.
#[derive(Clone, Copy, Debug, Default)]
pub struct EnvProviderSettings;

impl ProviderSettings for EnvProviderSettings {
    fn key(&self, provider: ProviderKind) -> Option<ProviderKey> {
        let variable = api_key_env_var(provider)?;
        key_from_variable(variable, std::env::var_os(variable).as_deref())
    }

    fn base_url(&self, provider: ProviderKind) -> Option<ProviderBaseUrl> {
        let variable = base_url_env_var(provider)?;
        base_url_from_variable(variable, std::env::var_os(variable).as_deref())
    }
}

/// The key one environment variable holds.
///
/// A variable that exists but holds nothing is not a key: `KEY=""` is what an
/// unset shell variable expands to in a script, and reporting a key present
/// there sends the caller to a provider that will reject them instead of to
/// the line that sets it. A value that is not UTF-8 is not a key either — it
/// cannot be sent as a header. The local provider has no key variable
/// ([`api_key_env_var`] names none), so it never reaches here.
///
/// A pure function on purpose: the alternative is a test that mutates the
/// process environment. No provider variable is set in CI, so a predicate
/// buried in the environment read would never be reached there — the mutant
/// that drops the emptiness check survived that way once. Decided on a value,
/// it has a truth table; the environment read itself is observed end to end
/// by the resolution matrix's keyed child process.
pub(crate) fn key_from_variable(variable: &str, value: Option<&OsStr>) -> Option<ProviderKey> {
    setting_from_variable(value)
        .map(|value| ProviderKey::new(value, SettingSource::Environment(variable.to_owned())))
}

/// The base URL one environment variable holds. The same line as for keys —
/// an empty or non-UTF-8 value is no setting — for the same reasons; the
/// environment read is observed end to end by the resolution matrix, whose
/// every child points the cloud providers at a closed loopback port.
pub(crate) fn base_url_from_variable(
    variable: &str,
    value: Option<&OsStr>,
) -> Option<ProviderBaseUrl> {
    setting_from_variable(value)
        .map(|value| ProviderBaseUrl::new(value, SettingSource::Environment(variable.to_owned())))
}

/// What one environment variable contributes: its value when it is set,
/// non-empty and UTF-8; nothing otherwise.
fn setting_from_variable(value: Option<&OsStr>) -> Option<&str> {
    value
        .and_then(OsStr::to_str)
        .filter(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    const VARIABLE: &str = "OPENAI_API_KEY";
    const URL_VARIABLE: &str = "OPENAI_BASE_URL";

    /// The truth table for what counts as a key in a variable.
    #[test]
    fn a_variable_holds_a_key_when_set_non_empty_and_utf8() {
        assert!(
            key_from_variable(VARIABLE, None).is_none(),
            "unset is no key"
        );
        assert!(
            key_from_variable(VARIABLE, Some(OsStr::new(""))).is_none(),
            "empty is no key"
        );
        let found =
            key_from_variable(VARIABLE, Some(OsStr::new("sk-abc"))).expect("a value is a key");
        assert_eq!(found.secret(), "sk-abc");
        assert_eq!(
            found.source(),
            &SettingSource::Environment(VARIABLE.to_owned()),
            "the source names the variable read"
        );
        assert!(
            key_from_variable(VARIABLE, Some(OsStr::new(" "))).is_some(),
            "whitespace is a value"
        );
    }

    /// The local provider has no key variable, so the environment never
    /// holds a key for it — whatever the process environment contains.
    #[test]
    fn the_local_provider_never_holds_a_key() {
        assert_eq!(api_key_env_var(ProviderKind::Local), None);
        assert!(EnvProviderSettings.key(ProviderKind::Local).is_none());
        assert!(EnvProviderSettings.base_url(ProviderKind::Local).is_none());
    }

    /// A value that cannot be sent as a header is not a key.
    #[test]
    #[cfg(unix)]
    fn a_non_utf8_value_is_not_a_key() {
        use std::os::unix::ffi::OsStrExt as _;
        let value = OsStr::from_bytes(b"sk-\xff");
        assert!(key_from_variable(VARIABLE, Some(value)).is_none());
        assert!(base_url_from_variable(URL_VARIABLE, Some(value)).is_none());
    }

    /// The reported source is a place, never the value (D11).
    #[test]
    fn a_source_label_is_a_place_never_the_value() {
        const SECRET: &str = "sk-do-not-leak-this-value";
        let from_env = ProviderKey::new(SECRET, SettingSource::Environment(VARIABLE.to_owned()));
        assert_eq!(from_env.source().label(), VARIABLE);

        let path = PathBuf::from("/home/u/.config/strata/config.toml");
        let from_file = ProviderKey::new(SECRET, SettingSource::ConfigFile(path.clone()));
        assert_eq!(from_file.source().label(), path.display().to_string());

        let from_app = ProviderKey::new(SECRET, SettingSource::Application);
        assert_eq!(from_app.source().label(), "application");

        for key in [from_env, from_file, from_app] {
            assert_ne!(key.source().label(), SECRET);
            let dumped = format!("{key:?}");
            assert!(!dumped.contains(SECRET), "Debug must redact: {dumped}");
            assert!(dumped.contains("<redacted>"), "{dumped}");
        }
    }

    /// The truth table for what counts as a base URL in a variable.
    #[test]
    fn a_variable_holds_a_base_url_when_set_and_non_empty() {
        assert!(
            base_url_from_variable(URL_VARIABLE, None).is_none(),
            "unset is no override"
        );
        assert!(
            base_url_from_variable(URL_VARIABLE, Some(OsStr::new(""))).is_none(),
            "empty is no override"
        );
        let found = base_url_from_variable(URL_VARIABLE, Some(OsStr::new("http://127.0.0.1:1")))
            .expect("a value is an override");
        assert_eq!(found.url(), "http://127.0.0.1:1");
        assert_eq!(
            found.source(),
            &SettingSource::Environment(URL_VARIABLE.to_owned()),
            "the source names the variable read"
        );
    }

    /// The clients append their own paths, so a trailing slash is dropped
    /// wherever the URL came from.
    #[test]
    fn a_base_url_loses_its_trailing_slashes() {
        let url = ProviderBaseUrl::new("http://localhost:8000/v1/", SettingSource::Application);
        assert_eq!(url.url(), "http://localhost:8000/v1");
        let url = ProviderBaseUrl::new("http://localhost:8000//", SettingSource::Application);
        assert_eq!(url.url(), "http://localhost:8000");
        let url = ProviderBaseUrl::new("http://localhost:8000", SettingSource::Application);
        assert_eq!(url.url(), "http://localhost:8000", "nothing to drop");
    }

    /// Every cloud provider has a public endpoint and an override variable;
    /// the local provider, reached in-process, has neither. The endpoints are
    /// rooted the way the providers' own SDKs root them.
    #[test]
    fn every_cloud_provider_has_a_default_endpoint_and_an_override_variable() {
        assert_eq!(
            default_base_url(ProviderKind::OpenAI),
            Some("https://api.openai.com/v1")
        );
        assert_eq!(
            default_base_url(ProviderKind::Anthropic),
            Some("https://api.anthropic.com")
        );
        assert_eq!(
            default_base_url(ProviderKind::Google),
            Some("https://generativelanguage.googleapis.com")
        );
        assert_eq!(default_base_url(ProviderKind::Local), None);

        assert_eq!(
            base_url_env_var(ProviderKind::OpenAI),
            Some("OPENAI_BASE_URL")
        );
        assert_eq!(
            base_url_env_var(ProviderKind::Anthropic),
            Some("ANTHROPIC_BASE_URL")
        );
        assert_eq!(
            base_url_env_var(ProviderKind::Google),
            Some("GOOGLE_GEMINI_BASE_URL")
        );
        assert_eq!(base_url_env_var(ProviderKind::Local), None);

        // A default never ends in a slash either: the clients append paths.
        for provider in [
            ProviderKind::OpenAI,
            ProviderKind::Anthropic,
            ProviderKind::Google,
        ] {
            let url = default_base_url(provider).expect("a cloud provider has one");
            assert!(!url.ends_with('/'), "{provider}: {url}");
        }
    }

    /// The environment source never overrides the local provider, whatever
    /// the (nonexistent) variable would say.
    #[test]
    fn the_local_provider_has_no_base_url() {
        assert_eq!(EnvProviderSettings.base_url(ProviderKind::Local), None);
    }
}
