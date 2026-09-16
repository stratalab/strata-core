//! `[providers.<name>]` reads and writes against a config file the test
//! places (#3221, #3270): the executor's provider settings read the global
//! file through `read_provider_setting`, so what it answers for each file
//! shape is what `inference status` and a provider call see — for the API key
//! and for the base URL alike; `strata config set/unset` write it through
//! `write_provider_setting` / `unset_provider_setting`, so what those leave in
//! the file is what the next read answers.

use std::path::{Path, PathBuf};

use strata_hub::{
    read_provider_setting, unset_provider_setting, write_provider_setting, HubUrlError,
    ProviderSetting,
};

fn config_file(dir: &Path, text: &str) -> PathBuf {
    let path = dir.join("config.toml");
    std::fs::write(&path, text).expect("write config");
    path
}

#[test]
fn a_stored_setting_is_read_for_its_provider_and_field_only() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = config_file(
        dir.path(),
        "[hub]\nurl = \"https://hub.example\"\n\n\
         [providers.openai]\napi_key = \"sk-file\"\nbase_url = \"http://127.0.0.1:8000/v1\"\n\n\
         [providers.google]\nbase_url = \"http://127.0.0.1:8001\"\n",
    );

    assert_eq!(
        read_provider_setting(&path, "openai", ProviderSetting::ApiKey).expect("readable"),
        Some("sk-file".to_owned())
    );
    assert_eq!(
        read_provider_setting(&path, "openai", ProviderSetting::BaseUrl).expect("readable"),
        Some("http://127.0.0.1:8000/v1".to_owned())
    );
    assert_eq!(
        read_provider_setting(&path, "anthropic", ProviderSetting::ApiKey).expect("readable"),
        None,
        "a provider without a section has no key"
    );
    // The fields are independent: a section holding only a base URL has no
    // key, and one holding only a key has no base URL.
    assert_eq!(
        read_provider_setting(&path, "google", ProviderSetting::ApiKey).expect("readable"),
        None
    );
    let key_only = config_file(dir.path(), "[providers.openai]\napi_key = \"sk-file\"\n");
    assert_eq!(
        read_provider_setting(&key_only, "openai", ProviderSetting::BaseUrl).expect("readable"),
        None
    );
}

#[test]
fn an_absent_file_section_or_field_is_no_setting_not_an_error() {
    let dir = tempfile::tempdir().expect("tempdir");

    let missing = dir.path().join("config.toml");
    assert_eq!(
        read_provider_setting(&missing, "openai", ProviderSetting::ApiKey).expect("no file"),
        None
    );

    let directory = dir.path().to_owned();
    assert_eq!(
        read_provider_setting(&directory, "openai", ProviderSetting::ApiKey)
            .expect("a directory is not a file"),
        None
    );

    let without_section = config_file(dir.path(), "[hub]\nurl = \"https://hub.example\"\n");
    assert_eq!(
        read_provider_setting(&without_section, "openai", ProviderSetting::ApiKey)
            .expect("no section"),
        None
    );

    let without_field = config_file(dir.path(), "[providers.openai]\nmodel = \"gpt\"\n");
    for setting in [ProviderSetting::ApiKey, ProviderSetting::BaseUrl] {
        assert_eq!(
            read_provider_setting(&without_field, "openai", setting).expect("no field"),
            None,
            "{setting:?}"
        );
    }
}

fn malformed_source(error: &HubUrlError) -> &str {
    match error {
        HubUrlError::MalformedSource { source, .. } => source,
        other => panic!("expected a malformed source, got {other:?}"),
    }
}

#[test]
fn a_written_setting_is_what_the_next_read_answers_and_touches_nothing_else() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("nested").join("config.toml");

    // First write creates the directory and the file.
    write_provider_setting(&path, "openai", ProviderSetting::ApiKey, "sk-file").expect("write");
    assert_eq!(
        read_provider_setting(&path, "openai", ProviderSetting::ApiKey).expect("readable"),
        Some("sk-file".to_owned())
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&path)
            .expect("metadata")
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600, "a key file is private to the user");
    }

    // A second setting, another provider, and a re-set of the first coexist
    // with the sibling fields and with an unrelated section.
    write_provider_setting(
        &path,
        "openai",
        ProviderSetting::BaseUrl,
        "http://127.0.0.1:8000/v1/",
    )
    .expect("write");
    write_provider_setting(&path, "google", ProviderSetting::ApiKey, "g-file").expect("write");
    write_provider_setting(&path, "openai", ProviderSetting::ApiKey, "sk-file-2").expect("write");
    let text = std::fs::read_to_string(&path).expect("read");
    let text = format!("[hub]\nurl = \"https://hub.example\"\n\n{text}");
    std::fs::write(&path, text).expect("rewrite");
    write_provider_setting(
        &path,
        "google",
        ProviderSetting::BaseUrl,
        "http://127.0.0.1:8001",
    )
    .expect("write");

    assert_eq!(
        read_provider_setting(&path, "openai", ProviderSetting::ApiKey).expect("readable"),
        Some("sk-file-2".to_owned())
    );
    assert_eq!(
        read_provider_setting(&path, "openai", ProviderSetting::BaseUrl).expect("readable"),
        Some("http://127.0.0.1:8000/v1/".to_owned()),
        "a base URL is stored as typed, trailing slash included"
    );
    assert_eq!(
        read_provider_setting(&path, "google", ProviderSetting::ApiKey).expect("readable"),
        Some("g-file".to_owned())
    );
    assert_eq!(
        read_provider_setting(&path, "google", ProviderSetting::BaseUrl).expect("readable"),
        Some("http://127.0.0.1:8001".to_owned())
    );
    let root: toml::Value = toml::from_str(&std::fs::read_to_string(&path).expect("read"))
        .expect("the file stays valid TOML");
    assert_eq!(
        root.get("hub")
            .and_then(|hub| hub.get("url"))
            .and_then(toml::Value::as_str),
        Some("https://hub.example"),
        "an unrelated section survives every write"
    );
}

#[test]
fn a_refused_base_url_leaves_the_file_exactly_as_it_was() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = config_file(
        dir.path(),
        "[providers.openai]\napi_key = \"sk-file\"\nbase_url = \"http://127.0.0.1:8000/v1\"\n",
    );
    let before = std::fs::read_to_string(&path).expect("read");

    for value in ["localhost:8000", "ftp://proxy.example/v1", "not a url", ""] {
        let error = write_provider_setting(&path, "openai", ProviderSetting::BaseUrl, value)
            .expect_err(value);
        assert_eq!(malformed_source(&error), "openai.base_url", "{value:?}");
    }
    assert_eq!(std::fs::read_to_string(&path).expect("read"), before);

    // No file is created for a refused value either.
    let absent = dir.path().join("absent.toml");
    write_provider_setting(
        &absent,
        "openai",
        ProviderSetting::BaseUrl,
        "localhost:8000",
    )
    .expect_err("refused");
    assert!(!absent.exists(), "a refused write creates nothing");
}

#[test]
fn unset_removes_only_the_named_field_and_says_whether_there_was_a_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = config_file(
        dir.path(),
        "[hub]\nurl = \"https://hub.example\"\n\n\
         [providers.openai]\napi_key = \"sk-file\"\nbase_url = \"http://127.0.0.1:8000/v1\"\n",
    );

    assert!(
        unset_provider_setting(&path, "openai", ProviderSetting::BaseUrl).expect("unset"),
        "the file existed"
    );
    assert_eq!(
        read_provider_setting(&path, "openai", ProviderSetting::BaseUrl).expect("readable"),
        None,
        "the named field is gone"
    );
    assert_eq!(
        read_provider_setting(&path, "openai", ProviderSetting::ApiKey).expect("readable"),
        Some("sk-file".to_owned()),
        "the sibling field stays"
    );
    let root: toml::Value =
        toml::from_str(&std::fs::read_to_string(&path).expect("read")).expect("valid TOML");
    assert_eq!(
        root.get("hub")
            .and_then(|hub| hub.get("url"))
            .and_then(toml::Value::as_str),
        Some("https://hub.example")
    );

    // Unsetting what is already absent is a no-op on an existing file, and
    // reports the file; with no file there is nothing to do and nothing is
    // created.
    assert!(
        unset_provider_setting(&path, "anthropic", ProviderSetting::ApiKey).expect("unset"),
        "an existing file is reported even when the field was absent"
    );
    let absent = dir.path().join("absent.toml");
    assert!(
        !unset_provider_setting(&absent, "openai", ProviderSetting::ApiKey).expect("no file"),
        "no file, nothing removed"
    );
    assert!(!absent.exists(), "unset creates no file");
}

#[test]
fn a_malformed_file_or_non_string_field_is_an_error_naming_the_file() {
    let dir = tempfile::tempdir().expect("tempdir");

    let malformed = config_file(dir.path(), "[providers.openai\napi_key = 1\n");
    let error = read_provider_setting(&malformed, "openai", ProviderSetting::ApiKey)
        .expect_err("malformed TOML");
    assert_eq!(malformed_source(&error), malformed.display().to_string());

    let not_a_string = config_file(
        dir.path(),
        "[providers.openai]\napi_key = 42\nbase_url = 8000\n",
    );
    for setting in [ProviderSetting::ApiKey, ProviderSetting::BaseUrl] {
        let error = read_provider_setting(&not_a_string, "openai", setting)
            .expect_err("a number is not a setting");
        assert_eq!(
            malformed_source(&error),
            not_a_string.display().to_string(),
            "{setting:?}"
        );
    }
}

/// A parse failure may not quote the file it failed on (#3296, Rule 31).
///
/// `toml::de::Error`'s `Display` renders the offending source line as an
/// excerpt. In this file that line can be the stored credential, so
/// interpolating it put a provider key into an error message — and from there
/// into a terminal, a CI log, or a pasted bug report. The reader still needs
/// to know what went wrong and where, so the detail keeps the cause and the
/// position and drops the excerpt.
#[test]
fn a_parse_error_never_quotes_the_line_it_failed_on() {
    const SECRET: &str = "sk-live-thismustneverbeprinted";

    let dir = tempfile::tempdir().expect("tempdir");

    // An unterminated string: the parse fails ON the credential's own line,
    // which is the case that leaks.
    let path = config_file(
        dir.path(),
        &format!("[providers.openai]\napi_key = \"{SECRET}\n"),
    );
    let error = read_provider_setting(&path, "openai", ProviderSetting::ApiKey)
        .expect_err("malformed TOML");
    let rendered = error.to_string();

    assert!(
        !rendered.contains(SECRET),
        "the error quotes the stored key: {rendered}"
    );
    assert!(
        !rendered.contains("sk-"),
        "the error quotes credential-shaped text: {rendered}"
    );
    // Still useful: it names the file and where to look.
    assert!(
        rendered.contains(&path.display().to_string()),
        "the error does not name the file: {rendered}"
    );
    // The position, exactly: a truth table on `line_and_column` proves the
    // arithmetic, and this proves the value reaches the message. The key sits
    // on line 2 and the unterminated string runs to the line's end.
    assert!(
        rendered.contains("at line 2, column 42"),
        "the error does not say where the problem is: {rendered}"
    );
}

/// `doctor` must be able to tell "no config file" from "a config file that
/// cannot be used" (#3296). A read that folds every failure into `None` cannot:
/// a key the user did store reads back absent, `inference status` says
/// `key_present: false`, and a broken install ends green.
///
/// The file format is hub's to know, so the probe lives here rather than
/// having each caller parse TOML for itself.
#[test]
fn the_config_file_state_distinguishes_absent_from_unusable() {
    let dir = tempfile::tempdir().expect("tempdir");

    // Absent is the default install, and is not a fault.
    assert_eq!(
        strata_hub::inspect_config(&dir.path().join("nothing.toml")),
        strata_hub::ConfigFileState::Absent
    );

    let good = config_file(dir.path(), "[providers.openai]\napi_key = \"sk-file\"\n");
    assert_eq!(
        strata_hub::inspect_config(&good),
        strata_hub::ConfigFileState::Readable
    );

    // An empty file is valid TOML and stores nothing: usable, not broken.
    let empty = config_file(dir.path(), "");
    assert_eq!(
        strata_hub::inspect_config(&empty),
        strata_hub::ConfigFileState::Readable
    );

    let malformed = config_file(dir.path(), "not = [toml\n");
    assert_eq!(
        strata_hub::inspect_config(&malformed),
        strata_hub::ConfigFileState::Malformed
    );

    // A directory where a file belongs cannot be read as one.
    let as_dir = dir.path().join("config-dir.toml");
    std::fs::create_dir(&as_dir).expect("create dir");
    assert_eq!(
        strata_hub::inspect_config(&as_dir),
        strata_hub::ConfigFileState::Unreadable
    );
}

/// The four words a state reports as. `strata doctor` prints them and
/// `inference status` puts them on the wire (#3423), so they are a published
/// vocabulary an agent matches on — not display text.
///
/// Pinned here, in the crate that owns the word, because that is the only
/// place a mutation lane can see: the executor test that holds the wire enum
/// to these is behind `strata-executor/inference`, which is not a default
/// feature, so `label` returning `""` for everything survived until this
/// existed.
#[test]
fn every_config_file_state_reports_one_stable_word() {
    for (state, word) in [
        (strata_hub::ConfigFileState::Absent, "absent"),
        (strata_hub::ConfigFileState::Readable, "readable"),
        (strata_hub::ConfigFileState::Unreadable, "unreadable"),
        (strata_hub::ConfigFileState::Malformed, "malformed"),
    ] {
        assert_eq!(state.label(), word);
    }

    // And a state is identified by its word: four states, four words.
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
