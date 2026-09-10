//! The executor's provider settings (R4 of #3261): where an inference runtime
//! this crate builds learns a provider's API key and base URL from.
//!
//! Two places hold a setting — the environment, and the user config file that
//! `strata config set <provider>.api_key` / `<provider>.base_url` writes — and
//! the environment wins (D5). Until #3221 the CLI bridged the file into the
//! environment at startup and then corrected `inference status` after the
//! fact, so a library or SDK caller opening the same database never saw the
//! stored key, and `doctor` answered from an environment nobody had bridged.
//! Now one [`ProviderSettings`] composes both sources, every runtime the
//! executor builds is constructed with it, and the runtime asks it in the
//! three places that need a setting — resolution, `status`, and the provider
//! call — so they cannot disagree and the reported source is where the value
//! actually came from. The base URL (#3270) rides the same composition: the
//! provider's own SDK variable first, then `[providers.<name>].base_url`.
//!
//! The file is read per call, not cached: a setting stored mid-session is
//! seen on the next command. The file's *path* is fixed at construction (the
//! platform config dir does not move while a process runs), which is also
//! what lets a test point the composition at a temp file.

use std::sync::Arc;

#[cfg(feature = "hub")]
use std::path::{Path, PathBuf};

use strata_inference::{EnvProviderSettings, InferenceRuntime, InferenceRuntimeConfig};

#[cfg(feature = "hub")]
use strata_hub::ProviderSetting;
#[cfg(feature = "hub")]
use strata_inference::{
    ProviderBaseUrl, ProviderKey, ProviderKind, ProviderSettings, SettingSource,
};

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
            let (path, value) = self.stored(provider, ProviderSetting::ApiKey)?;
            Some(ProviderKey::new(value, SettingSource::ConfigFile(path)))
        })
    }

    fn base_url(&self, provider: ProviderKind) -> Option<ProviderBaseUrl> {
        self.env.base_url(provider).or_else(|| {
            let (path, value) = self.stored(provider, ProviderSetting::BaseUrl)?;
            Some(ProviderBaseUrl::new(value, SettingSource::ConfigFile(path)))
        })
    }
}

#[cfg(feature = "hub")]
impl<E> EnvThenConfig<E> {
    /// The stored value of one setting for a provider, with the file it came
    /// from — `None` when the provider stores nothing, the platform has no
    /// config file, or the file holds no usable value (see
    /// [`setting_from_config`]).
    fn stored(
        &self,
        provider: ProviderKind,
        setting: ProviderSetting,
    ) -> Option<(PathBuf, String)> {
        let section = config_section(provider)?;
        let path = self.config_path.as_deref()?;
        let value = setting_from_config(
            path,
            strata_hub::read_provider_setting(path, section, setting),
        )?;
        Some((path.to_path_buf(), value))
    }
}

/// The `[providers.<section>]` a provider's settings are stored under, or
/// `None` for a provider that never uses one — so the file is not read for it.
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
/// A stored value is the setting from the file. An empty string is not a
/// value (an empty key cannot be a bearer credential, an empty base URL
/// reaches nothing, and the environment reader draws the same line). A file
/// that cannot be read or parsed yields no value rather than a failure: the
/// runtime asks for settings on every status call and provider call, and a
/// broken config file must not turn `inference status` into an error — the
/// old bridge swallowed the same read error. It is logged so the
/// disappearance is not silent; `doctor` surfacing the broken file is the
/// follow-up. The log names the file and never the error detail, which can
/// quote the offending line — and so the key.
#[cfg(feature = "hub")]
fn setting_from_config(
    path: &Path,
    read: Result<Option<String>, strata_hub::HubUrlError>,
) -> Option<String> {
    match read {
        Ok(Some(value)) if !value.is_empty() => Some(value),
        Ok(_) => None,
        Err(_) => {
            tracing::warn!(
                path = %path.display(),
                "the user config file could not be read; settings stored in it are unavailable"
            );
            None
        }
    }
}

#[cfg(test)]
#[cfg(feature = "hub")]
mod tests {
    use super::*;

    /// An environment holding a key and a base URL for every cloud provider.
    struct EnvWithKeys;

    impl ProviderSettings for EnvWithKeys {
        fn key(&self, provider: ProviderKind) -> Option<ProviderKey> {
            (provider != ProviderKind::Local).then(|| {
                ProviderKey::new(
                    "sk-from-env",
                    SettingSource::Environment("A_VARIABLE".to_owned()),
                )
            })
        }

        fn base_url(&self, provider: ProviderKind) -> Option<ProviderBaseUrl> {
            (provider != ProviderKind::Local).then(|| {
                ProviderBaseUrl::new(
                    "http://env.example/v1",
                    SettingSource::Environment("A_URL_VARIABLE".to_owned()),
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
        assert_eq!(*key.source(), SettingSource::ConfigFile(path.clone()));
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
            SettingSource::Environment("A_VARIABLE".to_owned())
        );
    }

    #[test]
    fn the_file_fills_only_the_base_urls_the_environment_lacks() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = config_file(
            dir.path(),
            "[providers.openai]\nbase_url = \"http://127.0.0.1:8000/v1\"\n",
        );

        let settings = EnvThenConfig {
            env: EmptyEnv,
            config_path: Some(path.clone()),
        };
        let base_url = settings
            .base_url(ProviderKind::OpenAI)
            .expect("the file holds a base URL");
        assert_eq!(base_url.url(), "http://127.0.0.1:8000/v1");
        assert_eq!(*base_url.source(), SettingSource::ConfigFile(path.clone()));
        assert!(
            settings.base_url(ProviderKind::Anthropic).is_none(),
            "a provider the file does not name keeps its public endpoint"
        );
        assert!(settings.base_url(ProviderKind::Local).is_none());
        // A base URL stored for a provider says nothing about its key.
        assert!(settings.key(ProviderKind::OpenAI).is_none());

        // The environment wins for a provider it names, even though the file
        // names it too.
        let settings = EnvThenConfig {
            env: EnvWithKeys,
            config_path: Some(path),
        };
        let base_url = settings
            .base_url(ProviderKind::OpenAI)
            .expect("the environment holds a base URL");
        assert_eq!(base_url.url(), "http://env.example/v1");
        assert_eq!(
            *base_url.source(),
            SettingSource::Environment("A_URL_VARIABLE".to_owned())
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
            assert!(settings.base_url(provider).is_none(), "{provider}");
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
            assert_eq!(
                settings
                    .base_url(provider)
                    .expect("from the environment")
                    .url(),
                "http://env.example/v1",
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
        assert!(settings.base_url(ProviderKind::Google).is_none());

        config_file(
            dir.path(),
            "[providers.google]\napi_key = \"sk-later\"\nbase_url = \"http://127.0.0.1:1\"\n",
        );
        assert_eq!(
            settings
                .key(ProviderKind::Google)
                .expect("set after the settings were built")
                .secret(),
            "sk-later"
        );
        assert_eq!(
            settings
                .base_url(ProviderKind::Google)
                .expect("set after the settings were built")
                .url(),
            "http://127.0.0.1:1"
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
    fn a_read_is_a_setting_only_when_it_holds_a_non_empty_string() {
        let path = Path::new("/somewhere/config.toml");
        let malformed = || strata_hub::HubUrlError::MalformedSource {
            source: path.display().to_string(),
            detail: "malformed TOML".to_owned(),
        };

        assert_eq!(
            setting_from_config(path, Ok(Some("sk-stored".to_owned()))),
            Some("sk-stored".to_owned())
        );
        assert!(setting_from_config(path, Ok(Some(String::new()))).is_none());
        assert!(setting_from_config(path, Ok(None)).is_none());
        assert!(setting_from_config(path, Err(malformed())).is_none());
    }

    #[test]
    fn a_malformed_file_is_no_setting_not_a_failure() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = config_file(
            dir.path(),
            "[providers.openai\napi_key = \"sk-broken\"\nbase_url = \"http://127.0.0.1:1\"\n",
        );
        let settings = EnvThenConfig {
            env: EmptyEnv,
            config_path: Some(path),
        };
        assert!(settings.key(ProviderKind::OpenAI).is_none());
        assert!(settings.base_url(ProviderKind::OpenAI).is_none());
    }
}
