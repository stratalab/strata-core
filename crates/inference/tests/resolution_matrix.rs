//! The model-resolution contract, as a matrix (design:
//! `docs/design/inference-model-resolution.md` §5, tracker #3261).
//!
//! One question, one answerer: "can this model run here, and if not, why?"
//! This file pins the answer for every spec form × registry directory ×
//! network setting × build × verb, as data:
//!
//! - `SPECS` pins what each spec form **is** (its identity: malformed, a
//!   cloud provider, a catalogued local model, an uncatalogued name, a GGUF
//!   path).
//! - `expected` pins what every verb must answer for it. Its precedence rules
//!   are the contract (§5.4 and the decisions recorded at the top of that fn).
//! - `KNOWN_RED` lists the cells the code answers wrongly today, keyed by
//!   issue and pinning today's wrong answer. It is shrink-only: an entry
//!   whose cells all pass fails this test ("fixed — delete the entry"), an
//!   entry whose cells moved to a different wrong answer fails it, and a red
//!   cell without an entry fails it ("file an issue"). Slices S1 and S2
//!   emptied it; every cell is green, and a regression must be pinned here
//!   under its issue before it can merge.
//!
//! Hermetic by construction. The matrix runs in a child process with a
//! scrubbed environment — no provider keys, `HOME` and `STRATA_MODELS_DIR`
//! under a fresh tempdir — so no cell can read the developer's real models
//! directory or keys (the loaders did exactly that until #3260), and no test
//! mutates the environment of the process `cargo test` runs. Nothing reaches
//! a provider or downloads a model: every provider's base URL is a closed
//! loopback port (`OPENAI_BASE_URL` and its siblings, #3270), so a cloud cell
//! that passes the network gate and the key check sends its request there
//! and observes `provider_unavailable` at once — proof the runtime resolved,
//! keyed, and addressed the call, and that nothing was sent anywhere else;
//! the model hub is a closed loopback port too (`STRATA_HF_ENDPOINT`) so a
//! `pull` that attempts a download observes `download_failed`; and every
//! "present" model is a junk file, so a cell that reaches a loader observes
//! `model_load_failed` — proof the resolver said Ready, never a loaded model.
//!
//! Written to be correct under either feature set: the expectation for a cell
//! is computed from `cfg!`, so the same file grades a `local,download` build
//! (run it with `--features local,download`; no CI lane does today) and the
//! released `native + cloud` shape.

use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use strata_inference::registry::catalog::CATALOG;
use strata_inference::{
    ChatRequest, EmbedInput, EmbeddingsRequest, InferenceCapability, InferenceError,
    InferenceRuntime, InferenceRuntimeConfig, InferenceService, ModelTask, ProviderKind,
    RankRequest, CLOUD_PROVIDER_KEYS,
};

// ---------------------------------------------------------------------------
// Build facts. Every expectation below is a function of these, never of a
// hard-coded lane.
// ---------------------------------------------------------------------------

const LOCAL_BUILT: bool = cfg!(feature = "local");
const DOWNLOAD_BUILT: bool = cfg!(feature = "download");

fn provider_built(provider: ProviderKind) -> bool {
    match provider {
        ProviderKind::Local => LOCAL_BUILT,
        ProviderKind::Anthropic => cfg!(feature = "anthropic"),
        ProviderKind::OpenAI => cfg!(feature = "openai"),
        ProviderKind::Google => cfg!(feature = "google"),
    }
}

// ---------------------------------------------------------------------------
// Codes. Names, not prose: every assertion compares `InferenceError::code()`.
// ---------------------------------------------------------------------------

const INVALID_REQUEST: &str = "inference.invalid_request";
const MISSING_MODEL: &str = "inference.missing_model";
/// R2: the split of `missing_model` ("catalogued, not downloaded") from
/// "not a model this binary knows" (S2, #3256).
const UNKNOWN_MODEL: &str = "inference.unknown_model";
const UNSUPPORTED_OPERATION: &str = "inference.unsupported_operation";
const UNSUPPORTED_PROVIDER: &str = "inference.unsupported_provider";
const MISSING_API_KEY: &str = "inference.missing_api_key";
const PROVIDER_UNAVAILABLE: &str = "inference.provider_unavailable";
const DOWNLOAD_DISABLED: &str = "inference.download_disabled";
const DOWNLOAD_FAILED: &str = "inference.download_failed";
const MODEL_LOAD_FAILED: &str = "inference.model_load_failed";

// ---------------------------------------------------------------------------
// Dimensions.
// ---------------------------------------------------------------------------

/// What a spec form is. The identity is pinned by hand — it is the contract —
/// while the file the registry dimension plants is derived from the catalog.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Identity {
    /// Rejected by the parser: empty, blank, or a provider with no model.
    Malformed,
    /// A cloud provider spec.
    Cloud(ProviderKind),
    /// A catalogued local model. `planted` marks the specs that resolve to the
    /// file the registry dimension plants (miniLM's default variant), so the
    /// directory state is observable through them.
    Catalog { task: ModelTask, planted: bool },
    /// A local name the catalog does not know (including an unknown quant of
    /// a known model, a four-part name, and an `openai-compatible:` prefix —
    /// §5.4 / Q6).
    NotInCatalog,
    /// A GGUF path that exists (as a junk file).
    PathPresent,
    /// A GGUF path that does not exist (Q5).
    PathAbsent,
}

struct SpecRow {
    /// The spec as typed. `<tmp>` is replaced by the cell's tempdir.
    spec: &'static str,
    identity: Identity,
}

const PLANTED_EMBED: Identity = Identity::Catalog {
    task: ModelTask::Embed,
    planted: true,
};
const GENERATE: Identity = Identity::Catalog {
    task: ModelTask::Generate,
    planted: false,
};

/// §5.1 spec forms.
const SPECS: &[SpecRow] = &[
    // Malformed.
    SpecRow {
        spec: "",
        identity: Identity::Malformed,
    },
    SpecRow {
        spec: "   ",
        identity: Identity::Malformed,
    },
    SpecRow {
        spec: "openai:",
        identity: Identity::Malformed,
    },
    SpecRow {
        spec: "local:",
        identity: Identity::Malformed,
    },
    // Catalogued local models, and the lenient spellings of one (Q2).
    SpecRow {
        spec: "miniLM",
        identity: PLANTED_EMBED,
    },
    SpecRow {
        spec: "MINILM",
        identity: PLANTED_EMBED,
    },
    SpecRow {
        spec: "  miniLM  ",
        identity: PLANTED_EMBED,
    },
    SpecRow {
        spec: "local:miniLM",
        identity: PLANTED_EMBED,
    },
    SpecRow {
        spec: "qwen3:1.7b",
        identity: GENERATE,
    },
    SpecRow {
        spec: "qwen3:1.7b:q8_0",
        identity: GENERATE,
    },
    SpecRow {
        spec: "tinyllama:q8_0",
        identity: GENERATE,
    },
    // Not in the catalog.
    SpecRow {
        spec: "tinyllama:q99",
        identity: Identity::NotInCatalog,
    },
    SpecRow {
        spec: "nope",
        identity: Identity::NotInCatalog,
    },
    SpecRow {
        spec: "nope:thing",
        identity: Identity::NotInCatalog,
    },
    SpecRow {
        spec: "local:nope",
        identity: Identity::NotInCatalog,
    },
    SpecRow {
        spec: "a:b:c:d",
        identity: Identity::NotInCatalog,
    },
    SpecRow {
        spec: "openai-compatible:ep:m",
        identity: Identity::NotInCatalog,
    },
    // GGUF paths.
    SpecRow {
        spec: "<tmp>/present.gguf",
        identity: Identity::PathPresent,
    },
    SpecRow {
        spec: "<tmp>/absent.gguf",
        identity: Identity::PathAbsent,
    },
    // Cloud.
    SpecRow {
        spec: "openai:gpt-4o-mini",
        identity: Identity::Cloud(ProviderKind::OpenAI),
    },
    SpecRow {
        spec: "OpenAI:gpt-4o-mini",
        identity: Identity::Cloud(ProviderKind::OpenAI),
    },
    SpecRow {
        spec: "anthropic:claude-x",
        identity: Identity::Cloud(ProviderKind::Anthropic),
    },
    SpecRow {
        spec: "google:x",
        identity: Identity::Cloud(ProviderKind::Google),
    },
];

/// Registry directory state (§5.1).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Dir {
    Empty,
    /// miniLM's default variant present as a non-empty junk file.
    Present,
    /// miniLM's default variant present as a zero-length file — an interrupted
    /// download. Not downloaded (§5.4).
    ZeroLength,
}

const DIRS: [Dir; 3] = [Dir::Empty, Dir::Present, Dir::ZeroLength];

/// `InferenceRuntimeConfig::network_enabled`.
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
    Pull,
}

const VERBS: [Verb; 6] = [
    Verb::Capability,
    Verb::Generate,
    Verb::Embed,
    Verb::Rank,
    Verb::Tokenize,
    Verb::Pull,
];

impl Verb {
    /// Which catalogued task a local verb needs. Every local GGUF tokenizes
    /// (`ModelAbilities::of`).
    fn applies_to(self, task: ModelTask) -> bool {
        match self {
            Verb::Generate => task == ModelTask::Generate,
            Verb::Embed => task == ModelTask::Embed,
            Verb::Rank => task == ModelTask::Rank,
            Verb::Tokenize => true,
            Verb::Capability | Verb::Pull => unreachable!("not a load verb"),
        }
    }
}

// ---------------------------------------------------------------------------
// The contract.
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Expect {
    /// The verb succeeds.
    Ok,
    /// The verb refuses with this code.
    Code(&'static str),
}

/// What every verb must answer. This function *is* §5.4 plus the precedence
/// decisions S0 makes (flagged in the PR as Q9–Q12):
///
/// - **Q9 — identity before capability.** Whether a spec names something is
///   answered before whether this build or network can run it: malformed →
///   unknown (not in catalog / path missing) → wrong task → execution not
///   built → network disabled → key missing → not downloaded. A user is
///   never told to install local execution for a model that does not exist.
/// - **Q10 — `pull` is task-neutral and needs no execution build.** It
///   resolves first and downloads only when a catalogued model is not
///   downloaded; a present file is returned without network; a cloud spec is
///   `unsupported_operation` (R6); a path is returned when present. The
///   download itself is observed, not skipped: the child's hub is a closed
///   loopback port, so the cell that must attempt it answers
///   `download_failed` — a `pull` that did not attempt would answer `Ok` or
///   `download_disabled` instead.
/// - **Q11 — network before key.** With the network off, a cloud verb is
///   refused as `unsupported_operation` (a `NetworkDisabled` availability the
///   design must add) before any key is looked at. With the network on and a
///   key present the request is sent — to the base URL the settings hold,
///   which in this matrix is a closed loopback port (S3b, #3270).
/// - **Q12 — a zero-length file is not downloaded** (§5.4), for `pull` too.
fn expected(identity: Identity, dir: Dir, net: Net, verb: Verb) -> Expect {
    match identity {
        Identity::Malformed => Expect::Code(INVALID_REQUEST),
        Identity::Cloud(provider) => cloud(provider, net, verb),
        Identity::NotInCatalog | Identity::PathAbsent => match verb {
            Verb::Capability => Expect::Ok,
            _ => Expect::Code(UNKNOWN_MODEL),
        },
        Identity::PathPresent => match verb {
            Verb::Capability | Verb::Pull => Expect::Ok,
            _ if LOCAL_BUILT => Expect::Code(MODEL_LOAD_FAILED),
            _ => Expect::Code(UNSUPPORTED_OPERATION),
        },
        Identity::Catalog { task, planted } => {
            let downloaded = planted && dir == Dir::Present;
            match verb {
                Verb::Capability => Expect::Ok,
                Verb::Pull if downloaded => Expect::Ok,
                Verb::Pull if net == Net::Off || !DOWNLOAD_BUILT => Expect::Code(DOWNLOAD_DISABLED),
                Verb::Pull => Expect::Code(DOWNLOAD_FAILED),
                _ if !verb.applies_to(task) => Expect::Code(UNSUPPORTED_OPERATION),
                _ if !LOCAL_BUILT => Expect::Code(UNSUPPORTED_OPERATION),
                _ if downloaded => Expect::Code(MODEL_LOAD_FAILED),
                _ => Expect::Code(MISSING_MODEL),
            }
        }
    }
}

fn cloud(provider: ProviderKind, net: Net, verb: Verb) -> Expect {
    match verb {
        Verb::Capability => Expect::Ok,
        // Local-only verbs (rank, tokenize) and pull (R6).
        Verb::Pull | Verb::Rank | Verb::Tokenize => Expect::Code(UNSUPPORTED_OPERATION),
        // Anthropic has no embedding API: a task the provider lacks, not a
        // provider this build lacks.
        Verb::Embed if provider == ProviderKind::Anthropic => Expect::Code(UNSUPPORTED_OPERATION),
        Verb::Generate | Verb::Embed => {
            if !provider_built(provider) {
                Expect::Code(UNSUPPORTED_PROVIDER)
            } else if net == Net::Off {
                Expect::Code(UNSUPPORTED_OPERATION)
            } else if keys_in_env() {
                Expect::Code(PROVIDER_UNAVAILABLE)
            } else {
                Expect::Code(MISSING_API_KEY)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Known red cells. Shrink-only. Each entry names the cells it covers, the
// issue that owns them, and today's wrong answer.
// ---------------------------------------------------------------------------

/// Which builds an entry applies to. `None` = every build.
#[derive(Clone, Copy)]
struct Lane {
    local: Option<bool>,
    download: Option<bool>,
}

impl Lane {
    fn applies(self) -> bool {
        self.local.is_none_or(|local| local == LOCAL_BUILT)
            && self
                .download
                .is_none_or(|download| download == DOWNLOAD_BUILT)
    }
}

/// Which registry directories an entry covers. `None` = every one.
type Dirs = Option<&'static [Dir]>;
/// Which network settings an entry covers. `None` = both.
type Nets = Option<Net>;

struct KnownRed {
    issue: &'static str,
    why: &'static str,
    lane: Lane,
    verbs: &'static [Verb],
    specs: &'static [&'static str],
    dirs: Dirs,
    nets: Nets,
    today: Expect,
}

impl KnownRed {
    fn covers(&self, cell: &Cell) -> bool {
        self.lane.applies()
            && self.verbs.contains(&cell.verb)
            && self.specs.contains(&cell.row.spec)
            && self.dirs.is_none_or(|dirs| dirs.contains(&cell.dir))
            && self.nets.is_none_or(|net| net == cell.net)
    }
}

/// Empty since S2 (#3256). An entry names the cells it covers, the issue that
/// owns them, and today's wrong answer, e.g. `KnownRed { issue: "#NNNN", why:
/// "…", lane: Lane { local: None, download: None }, verbs: &[Verb::Pull],
/// specs: &["nope"], dirs: None, nets: None, today: Expect::Code(MISSING_MODEL) }`.
const KNOWN_RED: &[KnownRed] = &[];

// ---------------------------------------------------------------------------
// Cells and their execution.
// ---------------------------------------------------------------------------

struct Cell {
    row: &'static SpecRow,
    dir: Dir,
    net: Net,
    verb: Verb,
}

impl Cell {
    fn name(&self) -> String {
        format!(
            "{:?} × {:?} × {:?} × {:?}",
            self.row.spec, self.dir, self.net, self.verb
        )
    }
}

fn cells() -> Vec<Cell> {
    let mut cells = Vec::new();
    for row in SPECS {
        for dir in DIRS {
            for net in NETS {
                for verb in VERBS {
                    cells.push(Cell {
                        row,
                        dir,
                        net,
                        verb,
                    });
                }
            }
        }
    }
    cells
}

#[derive(Debug, PartialEq, Eq)]
enum Observed {
    Ok,
    Code(&'static str),
}

impl Observed {
    fn of<T>(result: Result<T, InferenceError>) -> Self {
        match result {
            Ok(_) => Observed::Ok,
            Err(error) => Observed::Code(error.code()),
        }
    }

    fn matches(&self, expect: Expect) -> bool {
        match (self, expect) {
            (Observed::Ok, Expect::Ok) => true,
            (Observed::Code(observed), Expect::Code(expected)) => *observed == expected,
            _ => false,
        }
    }
}

/// The file the registry dimension plants: miniLM's default variant, named
/// by the catalog, never by hand.
fn planted_file() -> &'static str {
    let entry = CATALOG
        .iter()
        .find(|entry| entry.name == "miniLM")
        .expect("miniLM is catalogued");
    entry
        .variants
        .iter()
        .find(|variant| variant.name == entry.default_quant)
        .expect("default variant exists")
        .hf_file
}

/// Bytes that are not a GGUF file. Large enough that a loader that gets this
/// far fails on content, not on an empty read.
const JUNK: &[u8; 4096] = &[0x5a; 4096];

struct Fixture {
    _tmp: tempfile::TempDir,
    root: PathBuf,
    runtime: InferenceRuntime,
}

fn fixture(dir: Dir, net: Net) -> Fixture {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path().to_path_buf();
    let models = root.join("models");
    fs::create_dir_all(&models).expect("models dir");
    match dir {
        Dir::Empty => {}
        Dir::Present => fs::write(models.join(planted_file()), JUNK).expect("plant"),
        Dir::ZeroLength => fs::write(models.join(planted_file()), b"").expect("plant"),
    }
    fs::write(root.join("present.gguf"), JUNK).expect("present.gguf");
    let runtime = InferenceRuntime::new(InferenceRuntimeConfig {
        models_dir: Some(models),
        network_enabled: net == Net::On,
    });
    Fixture {
        _tmp: tmp,
        root,
        runtime,
    }
}

fn spec_for(row: &SpecRow, root: &Path) -> String {
    row.spec
        .replace("<tmp>", root.to_str().expect("utf-8 tempdir"))
}

/// Runs one cell through the [`InferenceService`] trait — the path the
/// executor dispatches to — never through the inherent single-item helpers.
fn run(cell: &Cell) -> Observed {
    let fixture = fixture(cell.dir, cell.net);
    let runtime: &dyn InferenceService = &fixture.runtime;
    let spec = spec_for(cell.row, &fixture.root);
    match cell.verb {
        Verb::Capability => {
            let result = runtime.capability(&spec);
            if let (Ok(capability), Identity::Cloud(provider)) = (&result, cell.row.identity) {
                assert_eq!(
                    capability.provider,
                    provider,
                    "{}: provider identity",
                    cell.name()
                );
            }
            if let (Ok(capability), Identity::Catalog { .. } | Identity::NotInCatalog) =
                (&result, cell.row.identity)
            {
                assert_eq!(
                    capability.provider,
                    ProviderKind::Local,
                    "{}: provider identity",
                    cell.name()
                );
            }
            if let (
                Ok(capability),
                Identity::Catalog { .. }
                | Identity::NotInCatalog
                | Identity::PathPresent
                | Identity::PathAbsent,
            ) = (&result, cell.row.identity)
            {
                assert_claims_match_the_verbs(cell, capability);
            }
            Observed::of(result)
        }
        Verb::Generate => Observed::of(runtime.chat(
            &spec,
            &ChatRequest {
                prompt: Some("hi".to_owned()),
                max_tokens: Some(1),
                ..ChatRequest::default()
            },
        )),
        Verb::Embed => Observed::of(runtime.embeddings(
            &spec,
            &EmbeddingsRequest {
                input: EmbedInput::One("hi".to_owned()),
                dimensions: None,
                normalize: None,
                input_type: None,
                instruction: None,
            },
        )),
        Verb::Rank => Observed::of(runtime.rank(
            &spec,
            &RankRequest {
                query: "q".to_owned(),
                passages: vec!["p".to_owned()],
            },
        )),
        Verb::Tokenize => Observed::of(runtime.tokenize(&spec, "hi", true)),
        Verb::Pull => Observed::of(runtime.pull_model(&spec)),
    }
}

/// One truth table answers for `capability` and for every verb (#3303): a
/// local spec's `can_<task>` is true exactly when the verb would go on to
/// load the model — refused by neither the identity check nor the task
/// check nor the build check. `expected` already pins what each verb does,
/// so the flags are derived from it rather than pinned a second time.
///
/// A present GGUF path is the cell that made this a law: every verb loads
/// it (`model_load_failed` on junk, in a build with `local`) while
/// `capability` claimed nothing for it, as if it were a name the catalog
/// does not know.
///
/// Cloud cells are exempt: a cloud flag is about the provider feature, and
/// the network and the key are reported beside it (`network_enabled`,
/// `requires_api_key`), not folded into it.
fn assert_claims_match_the_verbs(cell: &Cell, capability: &InferenceCapability) {
    for verb in [Verb::Generate, Verb::Embed, Verb::Rank, Verb::Tokenize] {
        let would_load = !matches!(
            expected(cell.row.identity, cell.dir, cell.net, verb),
            Expect::Code(UNSUPPORTED_OPERATION | UNKNOWN_MODEL)
        );
        let claimed = match verb {
            Verb::Generate => capability.can_generate,
            Verb::Embed => capability.can_embed,
            Verb::Rank => capability.can_rank,
            Verb::Tokenize => capability.can_tokenize,
            Verb::Capability | Verb::Pull => unreachable!("not a load verb"),
        };
        assert_eq!(
            claimed,
            would_load,
            "{}: capability claims {claimed} for {verb:?}, which {}",
            cell.name(),
            if would_load {
                "would load the model"
            } else {
                "is refused before any load"
            }
        );
    }
}

// ---------------------------------------------------------------------------
// The child: runs every cell in the scrubbed environment and grades it.
// ---------------------------------------------------------------------------

const CHILD_MODE: &str = "STRATA_RESOLUTION_MATRIX_CHILD";
const FAKE_KEY: &str = "matrix-fake-key-never-sent";
/// The model hub the registry downloads from. Pointed at a closed loopback
/// port in the child, so a download that is attempted is refused at once and
/// one that is not attempted is the only way a `pull` can succeed.
const HUB_ENDPOINT: &str = "STRATA_HF_ENDPOINT";
const UNREACHABLE_HUB: &str = "http://127.0.0.1:1";
/// Where every cloud provider's requests go in the child: a closed loopback
/// port, set through the variable the provider's own SDK reads (#3270).
const UNREACHABLE_PROVIDER: &str = "http://127.0.0.1:1";

fn keys_in_env() -> bool {
    std::env::var(CHILD_MODE).as_deref() == Ok("keys")
}

/// The environment the parent built, as the surfaces that report it see it:
/// no key unless this is the keys phase, every provider redirected to a
/// closed port, a models directory that is not the developer's, and a hub
/// that cannot be reached.
fn assert_child_environment(mode: &str) {
    for key in CLOUD_PROVIDER_KEYS {
        assert_eq!(
            std::env::var_os(key.env_var).is_some(),
            mode == "keys",
            "{} in the child environment",
            key.env_var
        );
        assert_eq!(
            std::env::var(key.base_url_env_var).as_deref(),
            Ok(UNREACHABLE_PROVIDER),
            "no cell may reach a real provider"
        );
    }
    assert_eq!(
        std::env::var(HUB_ENDPOINT).as_deref(),
        Ok(UNREACHABLE_HUB),
        "no cell may reach the real hub"
    );

    // The key dimension as `status` observes it: presence and the variable
    // that holds it, never the key. The runtime learned both from its
    // provider settings — the environment, the source `InferenceRuntime::new`
    // installs (#3221); the executor's env-then-config composition is pinned
    // by its own tests and the CLI's `config_behavior`.
    let status = fixture(Dir::Empty, Net::On).runtime.status();
    for row in &status.providers {
        if row.requires_api_key {
            assert_eq!(
                row.key_present,
                mode == "keys",
                "{:?} key_present in mode {mode}",
                row.provider
            );
            assert_eq!(
                row.key_source.as_deref(),
                (mode == "keys").then_some(row.key_env_var.as_deref().expect("a cloud variable")),
                "{:?} key_source in mode {mode}",
                row.provider
            );
            // The base-URL dimension: where a request would go and what
            // redirected it — the provider's own variable, in both modes.
            assert_eq!(
                (row.base_url.as_deref(), row.base_url_source.as_deref()),
                (Some(UNREACHABLE_PROVIDER), row.base_url_env_var.as_deref()),
                "{:?} base_url / base_url_source in mode {mode}",
                row.provider
            );
            assert!(
                row.base_url_env_var.is_some(),
                "{:?} names its base URL variable",
                row.provider
            );
        }
    }

    // The directory dimension as observed by the surfaces that list models:
    // a zero-length file is not downloaded.
    for dir in DIRS {
        let fixture = fixture(dir, Net::Off);
        let downloaded = fixture.runtime.status().models_downloaded;
        let listed = fixture.runtime.list_local_models().len();
        let expected = usize::from(dir == Dir::Present);
        assert_eq!(
            (downloaded, listed),
            (expected, expected),
            "{dir:?}: models_downloaded / list_local_models"
        );
    }
}

/// Every cell graded against the contract and against `KNOWN_RED`.
#[derive(Default)]
struct Grade {
    executed: usize,
    /// Cells covered by each `KNOWN_RED` entry, by index.
    covered: Vec<usize>,
    /// Red, no entry: file an issue and add one.
    unexpected_red: Vec<String>,
    /// Red, entry present, but not today's pinned answer.
    moved: Vec<String>,
    /// Green with an entry: shrink `KNOWN_RED`.
    fixed: Vec<String>,
}

fn grade_cells() -> Grade {
    let mut grade = Grade {
        covered: vec![0; KNOWN_RED.len()],
        ..Grade::default()
    };
    for cell in cells() {
        let expect = expected(cell.row.identity, cell.dir, cell.net, cell.verb);
        grade.executed += 1;
        let observed = run(&cell);
        let entry = KNOWN_RED.iter().position(|entry| entry.covers(&cell));
        match (observed.matches(expect), entry) {
            (true, None) => {}
            (true, Some(index)) => grade.fixed.push(format!(
                "{} — {} ({}): passes; delete or narrow the entry",
                cell.name(),
                KNOWN_RED[index].issue,
                KNOWN_RED[index].why
            )),
            (false, None) => grade.unexpected_red.push(format!(
                "{}: expected {expect:?}, observed {observed:?} — file an issue and \
                 add a KNOWN_RED entry",
                cell.name()
            )),
            (false, Some(index)) => {
                grade.covered[index] += 1;
                if !observed.matches(KNOWN_RED[index].today) {
                    grade.moved.push(format!(
                        "{} — {}: pinned today={:?}, observed {observed:?}, contract {expect:?}",
                        cell.name(),
                        KNOWN_RED[index].issue,
                        KNOWN_RED[index].today
                    ));
                }
            }
        }
    }
    grade
}

#[test]
#[ignore = "subprocess phase: re-invoked by `matrix_holds_in_a_scrubbed_environment`"]
fn matrix_child() {
    let Ok(mode) = std::env::var(CHILD_MODE) else {
        return;
    };
    assert!(
        matches!(mode.as_str(), "no-keys" | "keys"),
        "unknown child mode {mode:?}"
    );
    assert_child_environment(&mode);

    let grade = grade_cells();
    let dead: Vec<String> = KNOWN_RED
        .iter()
        .zip(&grade.covered)
        .filter(|(entry, count)| entry.lane.applies() && **count == 0)
        .map(|(entry, _)| format!("{} ({}) covers no executed cell", entry.issue, entry.why))
        .collect();

    println!(
        "resolution matrix [{mode}; local={LOCAL_BUILT} download={DOWNLOAD_BUILT}]: \
         {} executed, {} known red",
        grade.executed,
        grade.covered.iter().sum::<usize>()
    );
    let mut report = String::new();
    for (title, lines) in [
        ("UNEXPECTED RED", &grade.unexpected_red),
        ("MOVED (known red now fails differently)", &grade.moved),
        ("FIXED (shrink KNOWN_RED)", &grade.fixed),
        ("DEAD ENTRIES", &dead),
    ] {
        if !lines.is_empty() {
            // `fmt::Write` for `String` is infallible.
            writeln!(report, "\n{title}:").expect("String never fails to write");
            for line in lines {
                writeln!(report, "  {line}").expect("String never fails to write");
            }
        }
    }
    assert!(report.is_empty(), "resolution matrix [{mode}]:{report}");
}

// ---------------------------------------------------------------------------
// The parent: builds the scrubbed environment and runs the child twice —
// once with no key and once with a fake key in every provider variable. In
// both, every provider's base URL variable points at a closed port.
// ---------------------------------------------------------------------------

fn run_child(mode: &str) {
    let scrub = tempfile::tempdir().expect("scrub dir");
    let home = scrub.path().join("home");
    let models = scrub.path().join("models");
    fs::create_dir_all(&home).expect("home");
    fs::create_dir_all(&models).expect("models");

    let exe = std::env::current_exe().expect("current test binary");
    let mut command = Command::new(exe);
    command
        .args(["matrix_child", "--exact", "--ignored", "--nocapture"])
        .env_clear()
        .env(CHILD_MODE, mode)
        .env("HOME", &home)
        .env("STRATA_MODELS_DIR", &models)
        .env(HUB_ENDPOINT, UNREACHABLE_HUB);
    // Keep the parent's temp-dir convention so the child's tempdirs land in
    // the same place; nothing else from the environment crosses over.
    if let Some(tmpdir) = std::env::var_os("TMPDIR") {
        command.env("TMPDIR", tmpdir);
    }
    for key in CLOUD_PROVIDER_KEYS {
        command.env(key.base_url_env_var, UNREACHABLE_PROVIDER);
        if mode == "keys" {
            command.env(key.env_var, FAKE_KEY);
        }
    }
    let output = command.output().expect("spawn matrix child");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "matrix child [{mode}] failed:\n{stdout}\n{stderr}"
    );
    let summary = stdout
        .lines()
        .find(|line| line.starts_with("resolution matrix ["))
        .unwrap_or_else(|| {
            panic!("matrix child [{mode}] did not run the matrix:\n{stdout}\n{stderr}")
        });
    // Surface the cell counts under `--nocapture`; they are the evidence a
    // slice cites when it shrinks `KNOWN_RED`.
    println!("{summary}");
}

/// Every cell of §5, graded against the contract, with `KNOWN_RED` exact.
#[test]
fn matrix_holds_in_a_scrubbed_environment() {
    run_child("no-keys");
}

/// With a key in the environment nothing changes except what `status`
/// reports and which cloud cells get past the key check: those send their
/// request — to the closed port every provider is redirected to — and
/// observe `provider_unavailable`; with the network off a key is never
/// consulted, and no local cell looks at one.
#[test]
fn a_key_in_the_environment_changes_only_key_cells() {
    run_child("keys");
}
