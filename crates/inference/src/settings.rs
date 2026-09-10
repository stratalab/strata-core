//! Provider settings the runtime does not own: where a provider's API key
//! comes from (R4).
//!
//! The runtime never reads a key out of the process environment itself. It
//! asks the [`ProviderSettings`] it was constructed with, and asks it in three
//! places — resolution (`Availability::KeyMissing`), `status` (`key_present`,
//! `key_source`) and the provider call — so the three can never disagree
//! about whether a key exists. The one implementation here reads the
//! environment; the executor composes it with the user's config file so every
//! consumer (CLI, SDK, MCP server) sees the same keys (#3221). Tests inject a
//! fixed answer instead of mutating the environment, which races every other
//! test in a binary.

use std::ffi::OsStr;
use std::fmt;
use std::path::PathBuf;

use crate::{api_key_env_var, ProviderKind};

/// Where a provider's key was found. This is what `status` reports as
/// `key_source`: a place, never a value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KeySource {
    /// The named environment variable.
    Environment(String),
    /// The configuration file at this path (`strata config set
    /// <provider>.api_key`).
    ConfigFile(PathBuf),
    /// The embedding application supplied the key programmatically.
    Application,
}

impl KeySource {
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
    source: KeySource,
}

impl ProviderKey {
    /// A key found at `source`.
    pub fn new(value: impl Into<String>, source: KeySource) -> Self {
        Self {
            value: value.into(),
            source,
        }
    }

    /// Where the key was found.
    #[must_use]
    pub fn source(&self) -> &KeySource {
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

    /// The base URL to reach `provider` at, when it is not the provider's
    /// public endpoint (#3270). Read by no caller yet: S3b wires it into the
    /// provider clients.
    fn api_base(&self, _provider: ProviderKind) -> Option<String> {
        None
    }
}

/// The process environment: `OPENAI_API_KEY` and its siblings.
#[derive(Clone, Copy, Debug, Default)]
pub struct EnvProviderSettings;

impl ProviderSettings for EnvProviderSettings {
    fn key(&self, provider: ProviderKind) -> Option<ProviderKey> {
        let variable = api_key_env_var(provider);
        key_from_variable(provider, variable, std::env::var_os(variable).as_deref())
    }
}

/// The key one environment variable holds for `provider`.
///
/// A variable that exists but holds nothing is not a key: `KEY=""` is what an
/// unset shell variable expands to in a script, and reporting a key present
/// there sends the caller to a provider that will reject them instead of to
/// the line that sets it. A value that is not UTF-8 is not a key either — it
/// cannot be sent as a header. The local provider has no key variable, so it
/// is never one.
///
/// A pure function on purpose: the alternative is a test that mutates the
/// process environment. No provider variable is set in CI, so a predicate
/// buried in the environment read would never be reached there — the mutant
/// that drops the emptiness check survived that way once. Decided on a value,
/// it has a truth table; the environment read itself is observed end to end
/// by the resolution matrix's keyed child process.
pub(crate) fn key_from_variable(
    provider: ProviderKind,
    variable: &str,
    value: Option<&OsStr>,
) -> Option<ProviderKey> {
    if provider == ProviderKind::Local {
        return None;
    }
    value
        .and_then(OsStr::to_str)
        .filter(|value| !value.is_empty())
        .map(|value| ProviderKey::new(value, KeySource::Environment(variable.to_owned())))
}

#[cfg(test)]
mod tests {
    use super::*;

    const VARIABLE: &str = "OPENAI_API_KEY";

    /// The truth table for what counts as a key in a variable.
    #[test]
    fn a_variable_holds_a_key_when_set_non_empty_and_utf8() {
        let openai = ProviderKind::OpenAI;
        assert!(
            key_from_variable(openai, VARIABLE, None).is_none(),
            "unset is no key"
        );
        assert!(
            key_from_variable(openai, VARIABLE, Some(OsStr::new(""))).is_none(),
            "empty is no key"
        );
        let found = key_from_variable(openai, VARIABLE, Some(OsStr::new("sk-abc")))
            .expect("a value is a key");
        assert_eq!(found.secret(), "sk-abc");
        assert_eq!(
            found.source(),
            &KeySource::Environment(VARIABLE.to_owned()),
            "the source names the variable read"
        );
        assert!(
            key_from_variable(openai, VARIABLE, Some(OsStr::new(" "))).is_some(),
            "whitespace is a value"
        );
    }

    /// The local provider has no key, whatever its (unused) variable holds.
    #[test]
    fn the_local_provider_never_holds_a_key() {
        assert!(key_from_variable(
            ProviderKind::Local,
            "STRATA_LOCAL_API_KEY",
            Some(OsStr::new("sk-abc"))
        )
        .is_none());
    }

    /// A value that cannot be sent as a header is not a key.
    #[test]
    #[cfg(unix)]
    fn a_non_utf8_value_is_not_a_key() {
        use std::os::unix::ffi::OsStrExt as _;
        let value = OsStr::from_bytes(b"sk-\xff");
        assert!(key_from_variable(ProviderKind::OpenAI, VARIABLE, Some(value)).is_none());
    }

    /// The reported source is a place, never the value (D11).
    #[test]
    fn a_source_label_is_a_place_never_the_value() {
        const SECRET: &str = "sk-do-not-leak-this-value";
        let from_env = ProviderKey::new(SECRET, KeySource::Environment(VARIABLE.to_owned()));
        assert_eq!(from_env.source().label(), VARIABLE);

        let path = PathBuf::from("/home/u/.config/strata/config.toml");
        let from_file = ProviderKey::new(SECRET, KeySource::ConfigFile(path.clone()));
        assert_eq!(from_file.source().label(), path.display().to_string());

        let from_app = ProviderKey::new(SECRET, KeySource::Application);
        assert_eq!(from_app.source().label(), "application");

        for key in [from_env, from_file, from_app] {
            assert_ne!(key.source().label(), SECRET);
            let dumped = format!("{key:?}");
            assert!(!dumped.contains(SECRET), "Debug must redact: {dumped}");
            assert!(dumped.contains("<redacted>"), "{dumped}");
        }
    }

    /// With no override, every provider is reached at its public endpoint.
    #[test]
    fn the_environment_names_no_base_url_yet() {
        for provider in [
            ProviderKind::OpenAI,
            ProviderKind::Anthropic,
            ProviderKind::Google,
            ProviderKind::Local,
        ] {
            assert_eq!(EnvProviderSettings.api_base(provider), None);
        }
    }
}
