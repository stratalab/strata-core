//! `[providers.<name>].api_key` reads from a config file the test places
//! (#3221): the executor's provider settings read the global file through
//! `read_provider_key`, so what it answers for each file shape is what
//! `inference status` and a provider call see.

use std::path::{Path, PathBuf};

use strata_hub::{read_provider_key, HubUrlError};

fn config_file(dir: &Path, text: &str) -> PathBuf {
    let path = dir.join("config.toml");
    std::fs::write(&path, text).expect("write config");
    path
}

#[test]
fn a_stored_key_is_read_for_its_provider_only() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = config_file(
        dir.path(),
        "[hub]\nurl = \"https://hub.example\"\n\n[providers.openai]\napi_key = \"sk-file\"\n",
    );

    assert_eq!(
        read_provider_key(&path, "openai").expect("readable"),
        Some("sk-file".to_owned())
    );
    assert_eq!(
        read_provider_key(&path, "anthropic").expect("readable"),
        None,
        "a provider without a section has no key"
    );
}

#[test]
fn an_absent_file_section_or_key_is_no_key_not_an_error() {
    let dir = tempfile::tempdir().expect("tempdir");

    let missing = dir.path().join("config.toml");
    assert_eq!(
        read_provider_key(&missing, "openai").expect("no file"),
        None
    );

    let directory = dir.path().to_owned();
    assert_eq!(
        read_provider_key(&directory, "openai").expect("a directory is not a file"),
        None
    );

    let without_section = config_file(dir.path(), "[hub]\nurl = \"https://hub.example\"\n");
    assert_eq!(
        read_provider_key(&without_section, "openai").expect("no section"),
        None
    );

    let without_key = config_file(dir.path(), "[providers.openai]\nmodel = \"gpt\"\n");
    assert_eq!(
        read_provider_key(&without_key, "openai").expect("no key"),
        None
    );
}

fn malformed_source(error: &HubUrlError) -> &str {
    match error {
        HubUrlError::MalformedSource { source, .. } => source,
        other => panic!("expected a malformed source, got {other:?}"),
    }
}

#[test]
fn a_malformed_file_or_non_string_key_is_an_error_naming_the_file() {
    let dir = tempfile::tempdir().expect("tempdir");

    let malformed = config_file(dir.path(), "[providers.openai\napi_key = 1\n");
    let error = read_provider_key(&malformed, "openai").expect_err("malformed TOML");
    assert_eq!(malformed_source(&error), malformed.display().to_string());

    let not_a_string = config_file(dir.path(), "[providers.openai]\napi_key = 42\n");
    let error = read_provider_key(&not_a_string, "openai").expect_err("a number is not a key");
    assert_eq!(malformed_source(&error), not_a_string.display().to_string());
}
