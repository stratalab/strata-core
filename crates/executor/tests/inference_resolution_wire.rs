//! Executor-level cells of the model-resolution matrix (design:
//! `docs/design/inference-model-resolution.md` §5, tracker #3261).
//!
//! `crates/inference/tests/resolution_matrix.rs` pins what the resolver
//! answers for every spec form. This file pins that the executor — the path
//! every product surface takes — relays that answer unchanged and dresses it
//! from the registry row. For every spec form × registry directory × network
//! setting × verb it asserts four relations:
//!
//! 1. **Pass-through.** The executor succeeds exactly when the
//!    [`InferenceService`] trait succeeds on an identically configured
//!    runtime, and fails with the same code. Dispatch adds no logic (rule 7).
//! 2. **Row fidelity.** The error's class and retry policy are the registry
//!    row's for that code (`public_error_code_entry`), so no private table
//!    can drift from it again (#3243). A code with no row would surface as
//!    `internal.executor.unregistered_code` and fail relation 1.
//! 3. **Declared.** The code is in the command's declared error set in the
//!    generated IDL index, so an agent reading `agents commands` sees every
//!    code the resolution surface can produce.
//! 4. **Details pass-through.** The resolver's answer on the trait error
//!    (`InferenceError::availability`) is the error's `details` on the wire,
//!    one entry per field of `AvailabilityDetails` and nothing else; an
//!    error with no answer has no details. The answer travels as data, so a
//!    client acts on `availability` and `pull_spec`, never on the message.
//!
//! Nothing here restates the inference matrix's expectations: the spec list
//! is derived from the catalog and the provider table, and the expected code
//! for a cell is observed from a second runtime, never written by hand.
//!
//! Hermetic the same way the inference matrix is: the cells run in a child
//! process with a scrubbed environment (no provider keys, `HOME` and
//! `STRATA_MODELS_DIR` under a tempdir), nothing is downloaded (`pull` never
//! runs with the network on), and every "present" model is a junk file.

#![cfg(feature = "inference")]

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command as Process;

use strata_executor::{public_error_code_entry, Command, Executor, ExecutorError};
use strata_inference::registry::catalog::CATALOG;
use strata_inference::{
    ChatRequest, EmbedInput, EmbeddingsRequest, InferenceError, InferenceRuntime,
    InferenceRuntimeConfig, InferenceService, RankRequest, CLOUD_PROVIDER_KEYS,
};

// ---------------------------------------------------------------------------
// Dimensions. The spec list is derived: every catalog name, alias, default
// variant, `local:` form, lenient spelling and unknown quant; every cloud
// provider's well-formed and malformed forms; the two GGUF paths.
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Dir {
    Empty,
    Present,
}

const DIRS: [Dir; 2] = [Dir::Empty, Dir::Present];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Net {
    Off,
    On,
}

const NETS: [Net; 2] = [Net::Off, Net::On];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Verb {
    Capability,
    Generate,
    Embed,
    Rank,
    Tokenize,
    Detokenize,
    Pull,
}

const VERBS: [Verb; 7] = [
    Verb::Capability,
    Verb::Generate,
    Verb::Embed,
    Verb::Rank,
    Verb::Tokenize,
    Verb::Detokenize,
    Verb::Pull,
];

/// `<tmp>` is replaced by the cell's tempdir.
fn specs() -> Vec<String> {
    let mut specs = vec![
        String::new(),
        "   ".to_owned(),
        "local:".to_owned(),
        "nope".to_owned(),
        "nope:thing".to_owned(),
        "local:nope".to_owned(),
        "a:b:c:d".to_owned(),
        "openai-compatible:ep:m".to_owned(),
        "<tmp>/present.gguf".to_owned(),
        "<tmp>/absent.gguf".to_owned(),
    ];
    for entry in CATALOG {
        specs.push(entry.name.to_owned());
        specs.push(format!("  {}  ", entry.name.to_uppercase()));
        specs.push(format!("local:{}", entry.name));
        specs.push(format!("{}:{}", entry.name, entry.default_quant));
        specs.push(format!("{}:q99", entry.name));
        specs.extend(entry.aliases.iter().map(|alias| (*alias).to_owned()));
    }
    for key in CLOUD_PROVIDER_KEYS {
        specs.push(format!("{}:", key.provider));
        specs.push(format!("{}:model-x", key.provider));
        let mut chars = key.provider.chars();
        let capitalised: String = chars
            .next()
            .map(|first| first.to_uppercase().chain(chars).collect())
            .unwrap_or_default();
        specs.push(format!("{capitalised}:model-x"));
    }
    specs
}

// ---------------------------------------------------------------------------
// Fixture: one executor and one shadow runtime, configured identically.
// ---------------------------------------------------------------------------

/// Bytes that are not a GGUF file.
const JUNK: &[u8; 4096] = &[0x5a; 4096];

struct Fixture {
    _tmp: tempfile::TempDir,
    root: PathBuf,
    executor: Executor,
    shadow: InferenceRuntime,
}

fn runtime(models: PathBuf, net: Net) -> InferenceRuntime {
    InferenceRuntime::new(InferenceRuntimeConfig {
        models_dir: Some(models),
        network_enabled: net == Net::On,
    })
}

fn fixture(dir: Dir, net: Net) -> Fixture {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path().to_path_buf();
    let models = root.join("models");
    fs::create_dir_all(&models).expect("models dir");
    if dir == Dir::Present {
        for entry in CATALOG {
            let variant = entry
                .variants
                .iter()
                .find(|variant| variant.name == entry.default_quant)
                .expect("default variant exists");
            fs::write(models.join(variant.hf_file), JUNK).expect("plant");
        }
    }
    fs::write(root.join("present.gguf"), JUNK).expect("present.gguf");
    let executor = Executor::open_cache()
        .expect("cache-mode executor")
        .with_inference_runtime(runtime(models.clone(), net));
    Fixture {
        _tmp: tmp,
        root,
        executor,
        shadow: runtime(models, net),
    }
}

fn command(verb: Verb, model: &str) -> Command {
    let model = model.to_owned();
    match verb {
        Verb::Capability => Command::InferenceModelCapability { model },
        Verb::Generate => Command::InferenceGenerate {
            model,
            request: ChatRequest {
                prompt: Some("hi".to_owned()),
                max_tokens: Some(1),
                ..ChatRequest::default()
            },
        },
        Verb::Embed => Command::InferenceEmbed {
            model,
            request: embeddings_request(),
        },
        Verb::Rank => Command::InferenceRank {
            model,
            request: rank_request(),
        },
        Verb::Tokenize => Command::InferenceTokenize {
            model,
            text: "hi".to_owned(),
            add_special: true,
        },
        Verb::Detokenize => Command::InferenceDetokenize {
            model,
            ids: vec![1],
        },
        Verb::Pull => Command::InferenceModelsPull { model },
    }
}

fn embeddings_request() -> EmbeddingsRequest {
    EmbeddingsRequest {
        input: EmbedInput::One("hi".to_owned()),
        dimensions: None,
        normalize: None,
        input_type: None,
        instruction: None,
    }
}

fn rank_request() -> RankRequest {
    RankRequest {
        query: "q".to_owned(),
        passages: vec!["p".to_owned()],
    }
}

/// The same cell through the trait, on the shadow runtime.
fn shadow(runtime: &dyn InferenceService, verb: Verb, model: &str) -> Result<(), InferenceError> {
    match verb {
        Verb::Capability => runtime.capability(model).map(drop),
        Verb::Generate => runtime
            .chat(
                model,
                &ChatRequest {
                    prompt: Some("hi".to_owned()),
                    max_tokens: Some(1),
                    ..ChatRequest::default()
                },
            )
            .map(drop),
        Verb::Embed => runtime.embeddings(model, &embeddings_request()).map(drop),
        Verb::Rank => runtime.rank(model, &rank_request()).map(drop),
        Verb::Tokenize => runtime.tokenize(model, "hi", true).map(drop),
        Verb::Detokenize => runtime.detokenize(model, &[1]).map(drop),
        Verb::Pull => runtime.pull_model(model).map(drop),
    }
}

// ---------------------------------------------------------------------------
// The declared error set per command, from the generated IDL index.
// ---------------------------------------------------------------------------

fn declared_errors() -> BTreeMap<String, BTreeSet<String>> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("idl/v1/generated/command-index.json");
    let index: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&path).expect("command index reads"))
            .expect("command index parses");
    index["commands"]
        .as_array()
        .expect("commands array")
        .iter()
        .map(|command| {
            let wire = command["wire"].as_str().expect("wire name").to_owned();
            let errors = command["errors"]
                .as_array()
                .expect("errors array")
                .iter()
                .map(|error| error["code"].as_str().expect("error code").to_owned())
                .collect();
            (wire, errors)
        })
        .collect()
}

// ---------------------------------------------------------------------------
// The child: runs every cell in the scrubbed environment.
// ---------------------------------------------------------------------------

const CHILD_MODE: &str = "STRATA_RESOLUTION_WIRE_CHILD";

/// `AvailabilityDetails` as the wire carries it: its serialized fields, each
/// value a string. The oracle for relation 4.
fn flattened(details: &strata_inference::AvailabilityDetails) -> BTreeMap<String, String> {
    let serde_json::Value::Object(fields) =
        serde_json::to_value(details).expect("details serialize")
    else {
        panic!("details serialize as an object")
    };
    fields
        .into_iter()
        .map(|(key, value)| match value {
            serde_json::Value::String(text) => (key, text),
            other => (key, other.to_string()),
        })
        .collect()
}

fn describe(result: &Result<(), ExecutorError>) -> String {
    match result {
        Ok(()) => "Ok".to_owned(),
        Err(error) => error.code().to_owned(),
    }
}

/// Relations 2–4 for one refusal. Returns whether the resolver answered with
/// details, so the caller can prove relation 4 checked something.
fn check_refusal(
    name: &str,
    wire: &str,
    error: &ExecutorError,
    via_trait: &Result<(), InferenceError>,
    declared: &BTreeMap<String, BTreeSet<String>>,
    failures: &mut Vec<String>,
) -> bool {
    let code = error.code();

    // 2. Row fidelity.
    match public_error_code_entry(code) {
        None => failures.push(format!("{name}: {code} has no registry row")),
        Some(entry) => {
            if error.public_class() != entry.class || error.retry_policy() != entry.retry_policy {
                failures.push(format!(
                    "{name}: {code} rendered as ({:?}, {:?}), registry row says ({:?}, {:?})",
                    error.public_class(),
                    error.retry_policy(),
                    entry.class,
                    entry.retry_policy
                ));
            }
        }
    }

    // 3. Declared.
    let declared_for = declared
        .get(wire)
        .unwrap_or_else(|| panic!("{wire} is in the IDL index"));
    if !declared_for.contains(code) {
        failures.push(format!("{name}: {wire} does not declare {code} in the IDL"));
    }

    // 4. Details pass-through.
    let answered = via_trait
        .as_ref()
        .err()
        .and_then(InferenceError::availability);
    // No resolver answer means no details at all: the wire does not pretend.
    let expected = answered.map_or_else(BTreeMap::new, flattened);
    let observed: BTreeMap<String, String> = error
        .status()
        .details()
        .iter()
        .map(|detail| (detail.key().to_owned(), detail.value().to_owned()))
        .collect();
    if observed != expected {
        failures.push(format!(
            "{name}: {code} carries details {observed:?}, the resolver answered {expected:?}"
        ));
    }
    answered.is_some()
}

#[test]
#[ignore = "subprocess phase: re-invoked by `executor_relays_the_resolver_and_the_registry_row`"]
fn wire_child() {
    if std::env::var_os(CHILD_MODE).is_none() {
        return;
    }
    for key in CLOUD_PROVIDER_KEYS {
        assert!(
            std::env::var_os(key.env_var).is_none(),
            "{} leaked into the child environment",
            key.env_var
        );
    }

    let declared = declared_errors();
    let specs = specs();
    let mut failures = Vec::new();
    let mut executed = 0usize;
    let mut with_details = 0usize;
    let mut codes_seen = BTreeSet::new();

    for dir in DIRS {
        for net in NETS {
            let mut fixture = fixture(dir, net);
            let root = fixture.root.clone();
            for spec in &specs {
                let model = spec.replace("<tmp>", root.to_str().expect("utf-8 tempdir"));
                for verb in VERBS {
                    // `pull` is the one verb that downloads; with the network
                    // on it only ever runs in the inference matrix, on a file
                    // that is already present.
                    if verb == Verb::Pull && net == Net::On {
                        continue;
                    }
                    executed += 1;
                    let name = format!("{spec:?} × {dir:?} × {net:?} × {verb:?}");
                    let command = command(verb, &model);
                    let wire = command.name();
                    let via_executor = fixture.executor.execute(command).map(drop);
                    let via_trait = shadow(&fixture.shadow, verb, &model);

                    // 1. Pass-through.
                    let trait_code = match &via_trait {
                        Ok(()) => None,
                        Err(error) => Some(error.code()),
                    };
                    let executor_code = via_executor.as_ref().err().map(ExecutorError::code);
                    if executor_code != trait_code {
                        failures.push(format!(
                            "{name}: executor answered {}, trait answered {}",
                            describe(&via_executor),
                            trait_code.unwrap_or("Ok")
                        ));
                        continue;
                    }
                    let Err(error) = &via_executor else {
                        continue;
                    };
                    codes_seen.insert(error.code().to_owned());
                    if check_refusal(&name, wire, error, &via_trait, &declared, &mut failures) {
                        with_details += 1;
                    }
                }
            }
        }
    }

    // `inference-local` is the executor's only local feature; it carries
    // `download` with it.
    println!(
        "resolution wire [local={}]: {executed} cells, {with_details} with details, codes seen: {}",
        cfg!(feature = "inference-local"),
        codes_seen
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
            .join(", ")
    );
    assert!(
        failures.is_empty(),
        "resolution wire:\n  {}",
        failures.join("\n  ")
    );
    assert!(
        with_details > 0,
        "no cell carried a resolver answer; relation 4 checked nothing"
    );
}

// ---------------------------------------------------------------------------
// The parent: builds the scrubbed environment and runs the child.
// ---------------------------------------------------------------------------

/// Every executor-level cell: pass-through, row fidelity, and declaration.
#[test]
fn executor_relays_the_resolver_and_the_registry_row() {
    let scrub = tempfile::tempdir().expect("scrub dir");
    let home = scrub.path().join("home");
    let models = scrub.path().join("models");
    fs::create_dir_all(&home).expect("home");
    fs::create_dir_all(&models).expect("models");

    let exe = std::env::current_exe().expect("current test binary");
    let mut command = Process::new(exe);
    command
        .args(["wire_child", "--exact", "--ignored", "--nocapture"])
        .env_clear()
        .env(CHILD_MODE, "1")
        .env("HOME", &home)
        .env("STRATA_MODELS_DIR", &models);
    // Keep the parent's temp-dir convention so the child's tempdirs land in
    // the same place; nothing else from the environment crosses over.
    if let Some(tmpdir) = std::env::var_os("TMPDIR") {
        command.env("TMPDIR", tmpdir);
    }
    let output = command.output().expect("spawn wire child");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "wire child failed:\n{stdout}\n{stderr}"
    );
    assert!(
        stdout.contains("resolution wire ["),
        "wire child did not run the cells:\n{stdout}\n{stderr}"
    );
}
