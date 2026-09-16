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
    ConfigFileState, ConfigFileStatus, ProviderBaseUrl, ProviderKey, ProviderKind,
    ProviderSettings, SettingSource,
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

/// An inference runtime that reads no provider settings at all — not the
/// environment, not the user config file.
///
/// The state a fresh install is in, and the only one a captured example can
/// reproduce. Capturing against the real sources wrote the capturing
/// developer's own config path into `command-examples.json` — a corpus that
/// feeds the published reference pages — and made the replay test fail
/// permanently on any machine that had run `strata config set` (#3389).
pub(crate) fn isolated_inference_runtime() -> InferenceRuntime {
    InferenceRuntime::with_settings(
        InferenceRuntimeConfig::default(),
        Arc::new(NoProviderSettings),
    )
}

/// Holds nothing, for [`isolated_inference_runtime`]. `base_url` takes the
/// trait's `None` default.
struct NoProviderSettings;

impl strata_inference::ProviderSettings for NoProviderSettings {
    fn key(
        &self,
        _provider: strata_inference::ProviderKind,
    ) -> Option<strata_inference::ProviderKey> {
        None
    }
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

    fn config_file(&self) -> Option<ConfigFileStatus> {
        let path = self.config_path.as_deref()?;
        Some(ConfigFileStatus {
            path: path.to_path_buf(),
            state: wire_config_state(strata_hub::inspect_config(path)),
        })
    }
}

/// The wire spelling of a config-file state.
///
/// Two enums because `strata-inference` imports nothing from this workspace
/// (Rule 3): it owns the vocabulary `status` reports, `strata-hub` owns the
/// looking, and this is the one place they meet. Exhaustive on purpose at
/// both ends — a state added to either must be decided here rather than
/// falling through to a default.
#[cfg(feature = "hub")]
fn wire_config_state(state: strata_hub::ConfigFileState) -> ConfigFileState {
    match state {
        strata_hub::ConfigFileState::Absent => ConfigFileState::Absent,
        strata_hub::ConfigFileState::Readable => ConfigFileState::Readable,
        strata_hub::ConfigFileState::Unreadable => ConfigFileState::Unreadable,
        strata_hub::ConfigFileState::Malformed => ConfigFileState::Malformed,
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

    /// What `inference status` reports about the file, for each state it can
    /// be in — the answer #3423 exists to give.
    #[test]
    fn the_config_file_state_is_reported_for_every_shape_the_file_can_take() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");
        let settings = EnvThenConfig {
            env: EmptyEnv,
            config_path: Some(path.clone()),
        };
        let state = || settings.config_file().expect("a path was configured").state;

        // Nothing there: the default install, and not a fault.
        assert_eq!(state(), ConfigFileState::Absent);

        // Present and parses, whether or not it stores anything.
        config_file(dir.path(), "[hub]\n");
        assert_eq!(state(), ConfigFileState::Readable);
        config_file(
            dir.path(),
            "[providers.openai]\napi_key = \"sk-not-a-real-key\"\n",
        );
        assert_eq!(state(), ConfigFileState::Readable);

        // The case the issue is about: a key IS stored and cannot be reached,
        // so the provider row alone is indistinguishable from never having
        // configured one. Both halves are asserted here, because the second
        // is only interesting given the first.
        config_file(dir.path(), "[providers.openai]\napi_key = [\"sk-\n");
        assert_eq!(state(), ConfigFileState::Malformed);
        assert!(
            settings.key(ProviderKind::OpenAI).is_none(),
            "a malformed file yields no key -- which is why the state must be reported"
        );

        // A directory where the file belongs: readable as a path, not as
        // bytes.
        let as_dir = dir.path().join("nested");
        std::fs::create_dir_all(as_dir.join("config.toml")).expect("directory in the file's place");
        let settings = EnvThenConfig {
            env: EmptyEnv,
            config_path: Some(as_dir.join("config.toml")),
        };
        assert_eq!(
            settings.config_file().expect("a path").state,
            ConfigFileState::Unreadable
        );
    }

    /// The reported path is the file that was consulted, and a runtime that
    /// consults none says so — which is a different answer from a path with
    /// no file at it.
    #[test]
    fn a_settings_source_that_reads_no_file_reports_none() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = config_file(dir.path(), "[hub]\n");
        let reported = EnvThenConfig {
            env: EmptyEnv,
            config_path: Some(path.clone()),
        }
        .config_file()
        .expect("a path was configured");
        assert_eq!(reported.path, path);

        assert_eq!(
            EnvThenConfig {
                env: EmptyEnv,
                config_path: None,
            }
            .config_file(),
            None,
            "no config directory on this platform: no file is consulted"
        );
        assert_eq!(
            NoProviderSettings.config_file(),
            None,
            "the capture runtime reads nothing, so a captured example carries no path (#3389)"
        );
        assert_eq!(
            strata_inference::EnvProviderSettings.config_file(),
            None,
            "the environment is not a file"
        );
    }

    /// The two enums are separate types — `strata-inference` imports nothing
    /// from this workspace (Rule 3) — so nothing in the compiler stops them
    /// spelling a state differently on the wire. An agent that learns these
    /// four words from `strata doctor` matches them on `inference status`,
    /// and this is what keeps that true (#3423).
    #[test]
    fn the_wire_state_spells_every_state_exactly_as_doctor_does() {
        for hub_state in [
            strata_hub::ConfigFileState::Absent,
            strata_hub::ConfigFileState::Readable,
            strata_hub::ConfigFileState::Unreadable,
            strata_hub::ConfigFileState::Malformed,
        ] {
            let wire = serde_json::to_value(wire_config_state(hub_state)).expect("serializes");
            assert_eq!(
                wire.as_str().expect("a JSON string"),
                hub_state.label(),
                "{hub_state:?} is spelled differently by doctor and by the wire"
            );
        }

        // And the mapping is injective: four states in, four distinct words
        // out, so no two states collapse into one answer.
        let words: std::collections::BTreeSet<&str> = [
            strata_hub::ConfigFileState::Absent,
            strata_hub::ConfigFileState::Readable,
            strata_hub::ConfigFileState::Unreadable,
            strata_hub::ConfigFileState::Malformed,
        ]
        .into_iter()
        .map(strata_hub::ConfigFileState::label)
        .collect();
        assert_eq!(words.len(), 4, "two states share a word: {words:?}");
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

#[cfg(test)]
mod isolation_tests {
    use super::{isolated_inference_runtime, InferenceRuntime};
    use strata_inference::{InferenceStatus, ProviderKind};

    fn openai_row(status: &InferenceStatus) -> &strata_inference::ProviderStatus {
        status
            .providers
            .iter()
            .find(|provider| provider.provider == ProviderKind::OpenAI)
            .expect("openai is a catalogued provider")
    }

    /// The capture runtime ignores a provider key the default runtime finds.
    ///
    /// This is the property #3389 rests on, and it is only observable when a
    /// provider setting is actually present — which is why the mutation gate
    /// could replace `isolated_inference_runtime` with `Default::default()`
    /// and survive: on a runner with no key set, an environment-reading
    /// runtime and a no-settings runtime answer identically.
    ///
    /// So a key is set here. The first assertion is what stops the test being
    /// vacuous: it proves the variable actually reached the default runtime,
    /// so the second is a real difference rather than two runtimes agreeing
    /// about nothing.
    ///
    /// The variable's NAME comes from the provider row rather than being
    /// written here: which variable a provider reads is inference's to know,
    /// and `inference_guards` forbids executor sources from naming one.
    #[test]
    fn the_capture_runtime_ignores_an_environment_key_the_default_runtime_finds() {
        let baseline = InferenceRuntime::default().status();
        let variable = openai_row(&baseline)
            .key_env_var
            .clone()
            .expect("openai reads its key from a variable");

        let restore = std::env::var(&variable).ok();
        std::env::set_var(&variable, "sk-not-a-real-key");

        let default = openai_row(&InferenceRuntime::default().status()).key_present;
        let isolated = openai_row(&isolated_inference_runtime().status()).key_present;

        match restore {
            Some(value) => std::env::set_var(&variable, value),
            None => std::env::remove_var(&variable),
        }

        assert!(
            default,
            "{variable} did not reach the default runtime, so this test proves \
             nothing about isolation"
        );
        assert!(
            !isolated,
            "the capture runtime read the environment; a captured example would \
             record the capturing machine's provider state"
        );
    }
}
