//! One name per concept: a CLI verb and its engine method must agree (#3165).
//!
//! The CLI vocabulary is the IDL command catalog — `family.verb` ids, which is
//! what a user types and what an agent reads. The engine vocabulary is the
//! public method set on each capability service. When the two disagree, someone
//! reading `strata branch merge` has to know it is `BranchService::promote` to
//! find it, and that lookup is exactly what the surface review kept tripping on.
//!
//! This guard does not demand instant agreement. It demands that every
//! disagreement be **written down with its real other name**, so the list can
//! only shrink: rename the engine method to match the verb and the row here goes
//! stale, which fails the test until the row is deleted.
//!
//! Deliberately out of scope, because a 1:1 verb-to-method mapping is not
//! meaningful for them:
//!
//! - Dotted verbs (`graph.node.add`, `vector.collection.create`). The CLI groups
//!   by sub-noun and the engine flattens (`upsert_node`, `create_collection`),
//!   so the shapes differ by construction rather than by drift.
//! - The `arrow`, `hub`, and `inference` families. They have no capability
//!   service: they are transport, registry, and provider surfaces.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

/// Capability families and the engine service that owns their semantics.
const SERVICES: &[(&str, &str)] = &[
    ("admin", "crates/engine/src/api/admin.rs"),
    ("branch", "crates/engine/src/branch/service.rs"),
    ("event", "crates/engine/src/data/event/service.rs"),
    ("json", "crates/engine/src/data/json/service.rs"),
    ("kv", "crates/engine/src/data/kv/service.rs"),
    ("space", "crates/engine/src/api/space.rs"),
    ("vector", "crates/engine/src/data/vector/service.rs"),
];

/// `(family, cli verb, engine method, why they differ)`.
///
/// **This list may only shrink.** Every row is one concept wearing two names.
/// Fixing one means renaming so the verb and the method agree, then deleting the
/// row — the test below fails while a stale row remains.
const DIVERGENT_NAMES: &[(&str, &str, &str, &str)] = &[
    ("admin", "config_key", "config_value", "key vs value for the same read"),
    (
        "admin",
        "storage",
        "storage_footprint",
        "the CLI names the subject; the engine names the measurement",
    ),
    ("branch", "diff", "compare", "the CLI borrows git's noun; the engine states the operation"),
    (
        "branch",
        "fork",
        "fork_current",
        "the engine qualifies the default fork point; #3147 kept fork_current over create_from_head",
    ),
    (
        "branch",
        "merge",
        "promote",
        "#3148: the product word is merge, the engine word is promote, and the error code is conflict.engine.promotion",
    ),
    ("event", "count", "len", "collection idiom vs product noun"),
    ("event", "range_time", "range_by_time", "abbreviation vs preposition"),
    ("event", "types", "list_types", "bare noun vs list verb"),
    ("json", "batch_set", "batch_set_or_create", "#3153: the library set does not create; the CLI set does"),
    ("json", "history", "get_versions", "product noun vs accessor"),
    ("kv", "batch_delete", "delete_batch", "word order: the engine puts batch last here and first in batch_get"),
    ("kv", "batch_put", "put_batch", "word order: the engine puts batch last here and first in batch_get"),
    ("kv", "history", "get_versions", "product noun vs accessor"),
    ("vector", "keys", "list_keys", "bare noun vs list verb"),
];

/// `(family, cli verb, why the engine does not own it)`.
///
/// These verbs have no engine method because the surface is not engine-owned.
/// A row here is a claim about ownership, not a naming excuse.
const NOT_ENGINE_OWNED: &[(&str, &str, &str)] = &[
    (
        "admin",
        "hub_clone",
        "StrataHub artifact fetch; the engine has no remote surface",
    ),
    (
        "admin",
        "ipc_status",
        "IPC is an executor/CLI transport concern",
    ),
    (
        "admin",
        "ipc_stop",
        "IPC is an executor/CLI transport concern",
    ),
    (
        "admin",
        "remote",
        "StrataHub remote registry; the engine has no remote surface",
    ),
];

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("canonicalize repository root above crates/executor")
}

/// Every `family.verb` id in the IDL command catalog.
fn cli_verbs() -> BTreeSet<(String, String)> {
    let catalog = repo_root().join("crates/executor/idl/v1/commands");
    let mut verbs = BTreeSet::new();
    let entries = std::fs::read_dir(&catalog)
        .unwrap_or_else(|error| panic!("read {}: {error}", catalog.display()));
    for entry in entries {
        let path = entry.expect("catalog dir entry").path();
        if path.extension().is_none_or(|ext| ext != "yaml") {
            continue;
        }
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
        for line in text.lines() {
            if let Some(id) = line.trim().strip_prefix("- id:") {
                let id = id.trim();
                let (family, verb) = id.split_once('.').unwrap_or_else(|| {
                    panic!("command id `{id}` is not `family.verb`");
                });
                verbs.insert((family.to_owned(), verb.to_owned()));
            }
        }
    }
    assert!(
        verbs.len() > 100,
        "parsed only {} command ids — the catalog parser has drifted",
        verbs.len()
    );
    verbs
}

/// Public method names on each capability service, by family.
fn engine_methods() -> BTreeMap<String, BTreeSet<String>> {
    SERVICES
        .iter()
        .map(|(family, relative)| {
            let path = repo_root().join(relative);
            let text = std::fs::read_to_string(&path)
                .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
            let methods: BTreeSet<String> = text
                .lines()
                .filter_map(|line| {
                    let line = line.trim_start();
                    let rest = line
                        .strip_prefix("pub fn ")
                        .or_else(|| line.strip_prefix("pub const fn "))?;
                    let name: String = rest
                        .chars()
                        .take_while(|c| c.is_alphanumeric() || *c == '_')
                        .collect();
                    (!name.is_empty()).then_some(name)
                })
                .collect();
            assert!(
                !methods.is_empty(),
                "no public methods parsed from {}",
                path.display()
            );
            ((*family).to_owned(), methods)
        })
        .collect()
}

/// A flat CLI verb on a capability family names an engine method of the same
/// name, or the difference is written down.
#[test]
fn every_flat_cli_verb_maps_to_an_engine_method() {
    let methods = engine_methods();
    let divergent: BTreeSet<(&str, &str)> = DIVERGENT_NAMES
        .iter()
        .map(|(family, verb, _, _)| (*family, *verb))
        .collect();
    let unowned: BTreeSet<(&str, &str)> = NOT_ENGINE_OWNED
        .iter()
        .map(|(family, verb, _)| (*family, *verb))
        .collect();

    let mut unexplained = Vec::new();
    for (family, verb) in cli_verbs() {
        let Some(service) = methods.get(&family) else {
            continue; // family without a capability service; see the module docs
        };
        if verb.contains('.') || service.contains(&verb) {
            continue;
        }
        let key = (family.as_str(), verb.as_str());
        if !divergent.contains(&key) && !unowned.contains(&key) {
            unexplained.push(format!("{family}.{verb}"));
        }
    }

    assert!(
        unexplained.is_empty(),
        "CLI verbs with no engine method of the same name and no recorded reason: {unexplained:?}\n\
         Name the engine method after the verb, or add a DIVERGENT_NAMES row \
         (with the real engine name) or a NOT_ENGINE_OWNED row."
    );
}

/// Every divergence row still describes a live disagreement. A row goes stale
/// the moment the two names agree — which is what makes the list shrink-only.
#[test]
fn divergence_rows_are_live() {
    let verbs = cli_verbs();
    let methods = engine_methods();

    for (family, verb, engine_method, note) in DIVERGENT_NAMES {
        assert!(!note.is_empty(), "{family}.{verb} needs a reason");
        assert!(
            verbs.contains(&((*family).to_owned(), (*verb).to_owned())),
            "DIVERGENT_NAMES names {family}.{verb}, which the command catalog no longer has — delete the row"
        );
        let service = methods
            .get(*family)
            .unwrap_or_else(|| panic!("{family} has no service in SERVICES"));
        assert!(
            service.contains(*engine_method),
            "DIVERGENT_NAMES claims {family}.{verb} is `{engine_method}` in the engine, but no such method exists — fix or delete the row"
        );
        assert!(
            !service.contains(*verb),
            "{family}.{verb} now has an engine method of the same name: the names agree, so delete this DIVERGENT_NAMES row"
        );
    }
}

/// Every not-engine-owned row still describes a live CLI verb that the engine
/// genuinely does not implement.
#[test]
fn not_engine_owned_rows_are_live() {
    let verbs = cli_verbs();
    let methods = engine_methods();

    for (family, verb, reason) in NOT_ENGINE_OWNED {
        assert!(!reason.is_empty(), "{family}.{verb} needs a reason");
        assert!(
            verbs.contains(&((*family).to_owned(), (*verb).to_owned())),
            "NOT_ENGINE_OWNED names {family}.{verb}, which the command catalog no longer has — delete the row"
        );
        let service = methods
            .get(*family)
            .unwrap_or_else(|| panic!("{family} has no service in SERVICES"));
        assert!(
            !service.contains(*verb),
            "{family}.{verb} now has an engine method: it is engine-owned after all, so delete this NOT_ENGINE_OWNED row"
        );
    }
}

/// The capability vocabulary agrees on `kv` (#3160). This is the row the
/// surface review found inside a single response object, where `key_value` sat
/// beside `json`.
#[test]
fn compared_capability_speaks_the_user_vocabulary() {
    use strata_executor::ComparedCapability;

    let spelled = |capability: ComparedCapability| {
        serde_json::to_value(capability)
            .expect("capability serializes")
            .as_str()
            .expect("capability is a string")
            .to_owned()
    };

    assert_eq!(spelled(ComparedCapability::Kv), "kv");
    assert_eq!(spelled(ComparedCapability::Json), "json");
    assert_eq!(spelled(ComparedCapability::Vector), "vector");
    assert_eq!(spelled(ComparedCapability::Event), "event");
}
