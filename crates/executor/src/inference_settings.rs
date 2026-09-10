//! The executor's provider settings (R4 of #3261): where an inference runtime
//! this crate builds learns a provider's API key from.
//!
//! Two places hold a key — the environment, and the user config file that
//! `strata config set <provider>.api_key` writes — and the environment wins
//! (D5). Until #3221 the CLI bridged the file into the environment at startup
//! and then corrected `inference status` after the fact, so a library or SDK
//! caller opening the same database never saw the stored key, and `doctor`
//! answered from an environment nobody had bridged. Now one
//! [`ProviderSettings`] composes both sources, every runtime the executor
//! builds is constructed with it, and the runtime asks it in the three places
//! that need a key — resolution, `status`, and the provider call — so they
//! cannot disagree and the reported source is where the key actually came
//! from.
//!
//! The file is read per call, not cached: a key set mid-session is seen on
//! the next command. The file's *path* is fixed at construction (the platform
//! config dir does not move while a process runs), which is also what lets a
//! test point the composition at a temp file.

use std::sync::Arc;

#[cfg(feature = "hub")]
use std::path::{Path, PathBuf};

use strata_inference::{EnvProviderSettings, InferenceRuntime, InferenceRuntimeConfig};

#[cfg(feature = "hub")]
use strata_inference::{KeySource, ProviderKey, ProviderKind, ProviderSettings};

/// The inference runtime every executor opens with, and the one `strata
/// doctor` inspects: the default configuration over the executor's provider
/// settings — the environment first, then the user config file when this
/// build carries the `hub` feature that reads it.
#[must_use]
pub fn default_inference_runtime() -> InferenceRuntime {
    #[cfg(feature = "hub")]
    let settings = Arc::new(EnvThenConfig {
        env: EnvProviderSettings,
        config_path: strata_hub::global_config_path(),
    });
    #[cfg(not(feature = "hub"))]
    let settings = Arc::new(EnvProviderSettings);
    InferenceRuntime::with_settings(InferenceRuntimeConfig::default(), settings)
}

/// Environment first, then the user config file.
#[cfg(feature = "hub")]
struct EnvThenConfig<E> {
    env: E,
    /// The global config file (`strata config path`); `None` when the
    /// platform exposes no user config directory.
    config_path: Option<PathBuf>,
}

#[cfg(feature = "hub")]
impl<E: ProviderSettings> ProviderSettings for EnvThenConfig<E> {
    fn key(&self, provider: ProviderKind) -> Option<ProviderKey> {
        self.env.key(provider).or_else(|| {
            let section = config_section(provider)?;
            let path = self.config_path.as_deref()?;
            key_from_config(path, strata_hub::read_provider_key(path, section))
        })
    }
}

/// The `[providers.<section>]` a provider's key is stored under, or `None`
/// for a provider that never uses one — so the file is not read for it.
#[cfg(feature = "hub")]
fn config_section(provider: ProviderKind) -> Option<&'static str> {
    match provider {
        ProviderKind::Local => None,
        ProviderKind::Anthropic => Some("anthropic"),
        ProviderKind::OpenAI => Some("openai"),
        ProviderKind::Google => Some("google"),
    }
}

/// What a read of the config file means for the caller.
///
/// A stored key is a key from the file. An empty string is not a key (it
/// cannot be a bearer credential, and the environment reader draws the same
/// line). A file that cannot be read or parsed yields no key rather than a
/// failure: the runtime asks for keys on every status call and provider
/// call, and a broken config file must not turn `inference status` into an
/// error — the old bridge swallowed the same read error. It is logged so the
/// disappearance is not silent; `doctor` surfacing the broken file is the
/// follow-up. The log names the file and never the error detail, which can
/// quote the offending line — and so the key.
#[cfg(feature = "hub")]
fn key_from_config(
    path: &Path,
    read: Result<Option<String>, strata_hub::HubUrlError>,
) -> Option<ProviderKey> {
    match read {
        Ok(Some(value)) if !value.is_empty() => Some(ProviderKey::new(
            value,
            KeySource::ConfigFile(path.to_path_buf()),
        )),
        Ok(_) => None,
        Err(_) => {
            tracing::warn!(
                path = %path.display(),
                "the user config file could not be read; keys stored in it are unavailable"
            );
            None
        }
    }
}

#[cfg(test)]
#[cfg(feature = "hub")]
mod tests {
    use super::*;

    /// An environment holding a key for every cloud provider.
    struct EnvWithKeys;

    impl ProviderSettings for EnvWithKeys {
        fn key(&self, provider: ProviderKind) -> Option<ProviderKey> {
            (provider != ProviderKind::Local).then(|| {
                ProviderKey::new(
                    "sk-from-env",
                    KeySource::Environment("A_VARIABLE".to_owned()),
                )
            })
        }
    }

    /// An environment holding no keys.
    struct EmptyEnv;

    impl ProviderSettings for EmptyEnv {
        fn key(&self, _provider: ProviderKind) -> Option<ProviderKey> {
            None
        }
    }

    fn config_file(dir: &Path, text: &str) -> PathBuf {
        let path = dir.join("config.toml");
        std::fs::write(&path, text).expect("write config");
        path
    }

    const CLOUD: [ProviderKind; 3] = [
        ProviderKind::OpenAI,
        ProviderKind::Anthropic,
        ProviderKind::Google,
    ];

    #[test]
    fn the_file_fills_only_the_keys_the_environment_lacks() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = config_file(
            dir.path(),
            "[providers.openai]\napi_key = \"sk-from-file\"\n",
        );

        let settings = EnvThenConfig {
            env: EmptyEnv,
            config_path: Some(path.clone()),
        };
        let key = settings
            .key(ProviderKind::OpenAI)
            .expect("the file holds a key");
        assert_eq!(key.secret(), "sk-from-file");
        assert_eq!(*key.source(), KeySource::ConfigFile(path.clone()));
        assert!(
            settings.key(ProviderKind::Anthropic).is_none(),
            "a provider the file does not name has no key"
        );
        assert!(settings.key(ProviderKind::Local).is_none());

        // The environment wins for a provider it holds a key for, even though
        // the file holds one too.
        let settings = EnvThenConfig {
            env: EnvWithKeys,
            config_path: Some(path),
        };
        let key = settings
            .key(ProviderKind::OpenAI)
            .expect("the environment holds a key");
        assert_eq!(key.secret(), "sk-from-env");
        assert_eq!(
            *key.source(),
            KeySource::Environment("A_VARIABLE".to_owned())
        );
    }

    #[test]
    fn a_platform_without_a_config_dir_has_only_the_environment() {
        let settings = EnvThenConfig {
            env: EmptyEnv,
            config_path: None,
        };
        for provider in CLOUD {
            assert!(settings.key(provider).is_none(), "{provider}");
        }
        let settings = EnvThenConfig {
            env: EnvWithKeys,
            config_path: None,
        };
        for provider in CLOUD {
            assert_eq!(
                settings
                    .key(provider)
                    .expect("from the environment")
                    .secret(),
                "sk-from-env",
                "{provider}"
            );
        }
    }

    #[test]
    fn a_stored_key_is_seen_on_the_next_call_not_cached() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = config_file(dir.path(), "[hub]\n");
        let settings = EnvThenConfig {
            env: EmptyEnv,
            config_path: Some(path),
        };
        assert!(settings.key(ProviderKind::Google).is_none());

        config_file(dir.path(), "[providers.google]\napi_key = \"sk-later\"\n");
        assert_eq!(
            settings
                .key(ProviderKind::Google)
                .expect("set after the settings were built")
                .secret(),
            "sk-later"
        );
    }

    #[test]
    fn a_cloud_provider_has_a_section_and_the_local_provider_has_none() {
        assert_eq!(config_section(ProviderKind::OpenAI), Some("openai"));
        assert_eq!(config_section(ProviderKind::Anthropic), Some("anthropic"));
        assert_eq!(config_section(ProviderKind::Google), Some("google"));
        assert_eq!(config_section(ProviderKind::Local), None);
        // The section is the provider's canonical name, so `strata config set
        // <provider>.api_key` and this reader agree on the table.
        for provider in CLOUD {
            let name = provider.to_string();
            assert_eq!(config_section(provider), Some(name.as_str()));
        }
    }

    #[test]
    fn a_read_is_a_key_only_when_it_holds_a_non_empty_string() {
        let path = Path::new("/somewhere/config.toml");
        let malformed = || strata_hub::HubUrlError::MalformedSource {
            source: path.display().to_string(),
            detail: "malformed TOML".to_owned(),
        };

        let key = key_from_config(path, Ok(Some("sk-stored".to_owned()))).expect("a stored key");
        assert_eq!(key.secret(), "sk-stored");
        assert_eq!(*key.source(), KeySource::ConfigFile(path.to_path_buf()));

        assert!(key_from_config(path, Ok(Some(String::new()))).is_none());
        assert!(key_from_config(path, Ok(None)).is_none());
        assert!(key_from_config(path, Err(malformed())).is_none());
    }

    #[test]
    fn a_malformed_file_is_no_key_not_a_failure() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = config_file(dir.path(), "[providers.openai\napi_key = \"sk-broken\"\n");
        let settings = EnvThenConfig {
            env: EmptyEnv,
            config_path: Some(path),
        };
        assert!(settings.key(ProviderKind::OpenAI).is_none());
    }
}
