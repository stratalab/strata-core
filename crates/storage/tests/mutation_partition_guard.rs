//! An exclusion must be able to reach the mutants it excludes.
//!
//! The mutation-on-diff gate runs in three lanes, each with its own config
//! file, because cargo-mutants forwards `--features` to the `cargo test` it
//! runs per mutant and feature names are package-local. `--config` REPLACES the
//! default config rather than adding to it, so `.cargo/mutants.toml` — which
//! reads like *the* config file — governs lane A alone.
//!
//! That is a quiet trap. An exclusion for `embed_text_with`, an executor
//! symbol, was written into `.cargo/mutants.toml`; it parsed, it looked right
//! in review, and it did nothing, because lane A excludes `crates/executor/**`
//! and lanes B and C never read that file. The same nine mutants survived two
//! CI cycles before the cause was found. Nothing failed — the exclusion was
//! simply inert.
//!
//! This guard makes that failure loud. For every lane it takes each
//! `exclude_re`, pulls the identifiers out of the pattern, and asks whether any
//! file that lane actually mutates contains them. If a symbol exists in the
//! tree but only outside the lane's reach, the entry is dead where it was
//! written, and the guard says which lane's file it belongs in.
//!
//! It also pins the two structural properties the trap grew out of: no lane may
//! use `--no-config` (that is what made a config file inert in the first
//! place), and lane A's package exclusions must correspond exactly to the lanes
//! that cover those packages.

#![deny(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

/// The workflow whose mutation step defines the lanes.
const WORKFLOW: &str = ".github/workflows/ci.yml";

/// The config a `cargo mutants` invocation reads when it passes no `--config`.
const DEFAULT_CONFIG: &str = ".cargo/mutants.toml";

/// Identifiers shorter than this are mutant-name grammar (`with`, `true`,
/// `stop`) rather than symbol names, and appear in every crate anyway.
const MIN_IDENTIFIER: usize = 5;

fn repo_root() -> PathBuf {
    // CARGO_MANIFEST_DIR = crates/storage.
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root above crates/storage")
        .to_path_buf()
}

fn read(root: &Path, relative: &str) -> String {
    std::fs::read_to_string(root.join(relative))
        .unwrap_or_else(|err| panic!("{relative} unreadable: {err}"))
}

/// One `cargo mutants` invocation in the workflow.
#[derive(Debug)]
struct Lane {
    /// The config file this lane reads, relative to the repository root.
    config: String,
    /// The package it is restricted to, if any. Absent means "the workspace".
    package: Option<String>,
}

/// The lanes, parsed from the workflow rather than restated here — a lane added
/// there is covered here without touching this file.
fn lanes(root: &Path) -> Vec<Lane> {
    let workflow = read(root, WORKFLOW);
    let step = workflow
        .split_once("- name: Mutants on the PR diff")
        .expect("the mutation step exists")
        .1
        .split_once("- name: Upload mutants report")
        .expect("the mutation step ends")
        .0;

    // Join shell line continuations so each invocation is one line.
    let joined = step.replace("\\\n", " ");
    let lanes: Vec<Lane> = joined
        .lines()
        .map(str::trim)
        .filter(|line| line.starts_with("cargo mutants"))
        .map(|line| {
            let words: Vec<&str> = line.split_whitespace().collect();
            let after = |flag: &str| {
                words
                    .iter()
                    .position(|word| *word == flag)
                    .map(|index| words[index + 1].to_owned())
            };
            assert!(
                !words.contains(&"--no-config"),
                "a lane uses --no-config, which is what made an exclusion inert \
                 in the first place: its exclusions can only live as CLI flags, \
                 where the next person will not find them. Give the lane a \
                 config file and pass --config instead.\n  {line}"
            );
            Lane {
                config: after("--config").unwrap_or_else(|| DEFAULT_CONFIG.to_owned()),
                package: after("--package"),
            }
        })
        .collect();

    assert!(
        lanes.len() >= 3,
        "the guard found {} lanes; the mutation step moved and this guard is \
         now checking nothing",
        lanes.len()
    );
    lanes
}

/// `strata-executor` -> `crates/executor`, read from the manifests.
fn package_directories(root: &Path) -> BTreeMap<String, String> {
    let mut directories = BTreeMap::new();
    let entries = std::fs::read_dir(root.join("crates")).expect("crates/ is readable");
    for entry in entries.flatten() {
        let manifest = entry.path().join("Cargo.toml");
        let Ok(text) = std::fs::read_to_string(&manifest) else {
            continue;
        };
        let parsed: toml::Value = text
            .parse()
            .unwrap_or_else(|err| panic!("{} is not valid TOML: {err}", manifest.display()));
        if let Some(name) = parsed
            .get("package")
            .and_then(|package| package.get("name"))
            .and_then(toml::Value::as_str)
        {
            let directory = entry.file_name().to_string_lossy().into_owned();
            directories.insert(name.to_owned(), format!("crates/{directory}"));
        }
    }
    directories
}

/// A `**`/`*` path glob. `**` spans any number of segments, `*` any characters
/// within one.
fn glob_matches(pattern: &str, path: &str) -> bool {
    fn segments(pattern: &[&str], path: &[&str]) -> bool {
        match pattern.split_first() {
            None => path.is_empty(),
            Some((&"**", rest)) => (0..=path.len()).any(|skipped| segments(rest, &path[skipped..])),
            Some((head, rest)) => match path.split_first() {
                Some((first, tail)) if segment_matches(head, first) => segments(rest, tail),
                _ => false,
            },
        }
    }
    fn segment_matches(pattern: &str, segment: &str) -> bool {
        match pattern.split_once('*') {
            None => pattern == segment,
            Some((prefix, suffix)) => {
                segment.len() >= prefix.len() + suffix.len()
                    && segment.starts_with(prefix)
                    && segment.ends_with(suffix)
            }
        }
    }
    segments(
        &pattern.split('/').collect::<Vec<_>>(),
        &path.split('/').collect::<Vec<_>>(),
    )
}

/// Every mutable source file in the workspace, as repo-relative paths.
fn source_files(root: &Path) -> Vec<String> {
    fn walk(directory: &Path, root: &Path, found: &mut Vec<String>) {
        let Ok(entries) = std::fs::read_dir(directory) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, root, found);
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                let relative = path
                    .strip_prefix(root)
                    .expect("paths are built from the root");
                found.push(relative.to_string_lossy().replace('\\', "/"));
            }
        }
    }
    let mut found = Vec::new();
    let entries = std::fs::read_dir(root.join("crates")).expect("crates/ is readable");
    for entry in entries.flatten() {
        walk(&entry.path().join("src"), root, &mut found);
    }
    found.sort();
    assert!(
        found.len() > 100,
        "only {} source files found; the walk is broken",
        found.len()
    );
    found
}

/// The exclusions declared in one lane's config.
fn exclusions(root: &Path, config: &str) -> (Vec<String>, Vec<String>) {
    let parsed: toml::Value = read(root, config)
        .parse()
        .unwrap_or_else(|err| panic!("{config} is not valid TOML: {err}"));
    let list = |key: &str| -> Vec<String> {
        parsed
            .get(key)
            .and_then(toml::Value::as_array)
            .map(|entries| {
                entries
                    .iter()
                    .map(|entry| {
                        entry
                            .as_str()
                            .unwrap_or_else(|| panic!("{config}: {key} holds a non-string"))
                            .to_owned()
                    })
                    .collect()
            })
            .unwrap_or_default()
    };
    (list("exclude_globs"), list("exclude_re"))
}

/// The identifiers a mutant-name regex names, with the regex syntax dropped.
fn identifiers(pattern: &str) -> BTreeSet<String> {
    let mut found = BTreeSet::new();
    let mut current = String::new();
    for character in pattern.chars().chain(std::iter::once(' ')) {
        if character.is_ascii_alphanumeric() || character == '_' {
            current.push(character);
        } else {
            if current.len() >= MIN_IDENTIFIER && !current.chars().all(|c| c.is_ascii_digit()) {
                found.insert(std::mem::take(&mut current));
            }
            current.clear();
        }
    }
    found
}

/// Every lane's config exists and parses, and no lane silently reads none.
#[test]
fn every_lane_reads_a_config_file_that_exists() {
    let root = repo_root();
    for lane in lanes(&root) {
        let path = root.join(&lane.config);
        assert!(
            path.is_file(),
            "lane {:?} reads {} which does not exist",
            lane.package,
            lane.config
        );
        // Parsing is the check: cargo-mutants rejects an unknown key outright,
        // so a typo cannot silently disable an exclusion — but only if the file
        // is real TOML in the first place.
        exclusions(&root, &lane.config);
    }
}

/// Lane A's package exclusions and the featured lanes are the same set.
///
/// The two halves of the partition are written in different files, and each is
/// silently wrong without the other: a package excluded from lane A with no
/// lane of its own is never mutated at all, and a package with its own lane
/// that lane A still mutates is mutated twice, once under the wrong features.
#[test]
fn each_excluded_package_has_a_lane_and_each_lane_is_excluded_from_the_default() {
    let root = repo_root();
    let directories = package_directories(&root);
    let lanes = lanes(&root);

    let featured: BTreeSet<String> = lanes
        .iter()
        .filter_map(|lane| lane.package.as_ref())
        .map(|package| {
            directories
                .get(package)
                .unwrap_or_else(|| panic!("{package} is not a crate in this workspace"))
                .clone()
        })
        .collect();

    let default_lane = lanes
        .iter()
        .find(|lane| lane.config == DEFAULT_CONFIG)
        .expect("one lane reads the default config");
    let (globs, _) = exclusions(&root, &default_lane.config);
    let excluded: BTreeSet<String> = globs
        .iter()
        .filter_map(|glob| glob.strip_suffix("/**"))
        .filter(|prefix| prefix.matches('/').count() == 1)
        .map(str::to_owned)
        .collect();

    assert_eq!(
        excluded, featured,
        "the packages {DEFAULT_CONFIG} excludes and the packages with their own \
         lanes must be the same set; a package in neither is mutated under the \
         wrong features, a package in both halves of the difference is either \
         mutated twice or not at all"
    );
}

/// A glob that matches nothing is rot: it protects a file that has moved.
#[test]
fn every_path_exclusion_matches_a_file_that_exists() {
    let root = repo_root();
    let files = source_files(&root);
    let mut dead = Vec::new();

    for lane in lanes(&root) {
        let (globs, _) = exclusions(&root, &lane.config);
        for glob in globs {
            if !files.iter().any(|file| glob_matches(&glob, file)) {
                dead.push(format!("  {}: {glob}", lane.config));
            }
        }
    }

    assert!(
        dead.is_empty(),
        "these path exclusions match no file — the code moved and the exclusion \
         now protects nothing:\n{}",
        dead.join("\n")
    );
}

/// An exclusion must name something its own lane can actually mutate.
///
/// This is the guard the trap earned. `embed_text_with` in `.cargo/mutants.toml`
/// named an executor symbol from the one lane that excludes the executor, so it
/// could never match; the mutants it was meant to silence went on surviving and
/// nothing said why.
#[test]
fn no_exclusion_names_a_symbol_its_own_lane_cannot_reach() {
    let root = repo_root();
    let files = source_files(&root);
    let directories = package_directories(&root);
    let lanes = lanes(&root);

    // Read each file once; the patterns then search in memory.
    let contents: Vec<(String, String)> = files
        .iter()
        .map(|file| (file.clone(), read(&root, file)))
        .collect();

    // What each lane may mutate: its package if it names one, minus its globs.
    let scopes: Vec<BTreeSet<String>> = lanes
        .iter()
        .map(|lane| {
            let (globs, _) = exclusions(&root, &lane.config);
            let package_prefix = lane.package.as_ref().map(|package| {
                format!(
                    "{}/",
                    directories
                        .get(package)
                        .unwrap_or_else(|| panic!("{package} is not a crate"))
                )
            });
            files
                .iter()
                .filter(|file| {
                    package_prefix
                        .as_ref()
                        .is_none_or(|prefix| file.starts_with(prefix))
                        && !globs.iter().any(|glob| glob_matches(glob, file))
                })
                .cloned()
                .collect()
        })
        .collect();

    let mut misplaced = Vec::new();
    for (lane, scope) in lanes.iter().zip(&scopes) {
        let (_, patterns) = exclusions(&root, &lane.config);
        for pattern in patterns {
            for identifier in identifiers(&pattern) {
                let holders: Vec<&String> = contents
                    .iter()
                    .filter(|(_, text)| text.contains(&identifier))
                    .map(|(file, _)| file)
                    .collect();
                if holders.is_empty() || holders.iter().any(|file| scope.contains(*file)) {
                    continue;
                }
                // The symbol exists, but nowhere this lane mutates. Name the
                // lane that does reach it, so the fix is to move the entry.
                let reachable: Vec<&str> = lanes
                    .iter()
                    .zip(&scopes)
                    .filter(|(_, other)| holders.iter().any(|file| other.contains(*file)))
                    .map(|(other, _)| other.config.as_str())
                    .collect();
                misplaced.push(format!(
                    "  {}: exclusion {pattern:?} names `{identifier}`, which \
                     exists only in {} — a file this lane does not mutate. \
                     {}",
                    lane.config,
                    holders
                        .iter()
                        .take(3)
                        .map(|file| file.as_str())
                        .collect::<Vec<_>>()
                        .join(", "),
                    if reachable.is_empty() {
                        "No lane mutates it, so no exclusion is needed at all.".to_owned()
                    } else {
                        format!("Move it to {}.", reachable.join(" or "))
                    }
                ));
            }
        }
    }

    assert!(
        misplaced.is_empty(),
        "these exclusions cannot match anything in the lane that declares them, \
         so they silently do nothing:\n{}",
        misplaced.join("\n")
    );
}

/// The glob matcher itself, since three tests above trust it.
#[test]
fn the_glob_matcher_handles_the_shapes_the_configs_use() {
    assert!(glob_matches(
        "crates/executor/**",
        "crates/executor/src/a.rs"
    ));
    assert!(!glob_matches(
        "crates/executor/**",
        "crates/engine/src/a.rs"
    ));
    assert!(glob_matches(
        "crates/storage/src/**/*_loom.rs",
        "crates/storage/src/deep/nested/queue_loom.rs"
    ));
    assert!(!glob_matches(
        "crates/storage/src/**/*_loom.rs",
        "crates/storage/src/deep/queue.rs"
    ));
    assert!(glob_matches("a/b.rs", "a/b.rs"));
    assert!(!glob_matches("a/b.rs", "a/b/c.rs"));
    // `**` spans zero segments as well as many.
    assert!(glob_matches("a/**/c.rs", "a/c.rs"));
    // A `*` stays inside its own segment.
    assert!(!glob_matches("a/*.rs", "a/b/c.rs"));
}

/// Identifier extraction, since the trap check is only as good as this.
#[test]
fn identifiers_are_pulled_out_of_regex_syntax() {
    assert_eq!(
        identifiers("replace embed_text_with -> Vec<f32> with vec![]"),
        ["embed_text_with", "replace"]
            .into_iter()
            .map(str::to_owned)
            .collect()
    );
    // Alternation, paths, and escapes are syntax, not symbols.
    assert!(identifiers("in (listener_loop|handle_connection)").contains("handle_connection"));
    assert!(identifiers("replace print_banner with \\(\\)").contains("print_banner"));
    // Grammar words below the length floor are not symbols.
    assert!(!identifiers("delete ! in IpcServer::stop").contains("stop"));
}

/// #3292: cargo-mutants only recognises the LITERAL `#[cfg(test)]` when deciding
/// to skip a module — a module gated `#[cfg(all(test, …))]` is treated as
/// product code, so its non-`#[test]` HELPER functions become mutants (vacuous
/// MISSED when the extra condition is not compiled in the lane, or pure `cargo
/// test` waste when it is). Stack the gates instead — `#[cfg(test)]` first, then
/// the rest — so cargo-mutants reads the `#[cfg(test)]` and skips the whole
/// module. Individual `#[test]` functions are always skipped, so this guards
/// MODULE gates only.
fn declares_module(line: &str) -> bool {
    let mut rest = line.trim_start();
    for prefix in ["pub(crate) ", "pub(super) ", "pub "] {
        if let Some(stripped) = rest.strip_prefix(prefix) {
            rest = stripped.trim_start();
            break;
        }
    }
    rest.starts_with("mod ")
}

#[test]
fn test_only_modules_stack_the_cfg_test_gate() {
    let root = repo_root();
    let mut offenders = Vec::new();
    for relative in source_files(&root) {
        let text = read(&root, &relative);
        let lines: Vec<&str> = text.lines().collect();
        for (index, line) in lines.iter().enumerate() {
            if !line.contains("#[cfg(all(test") {
                continue;
            }
            // The gated item is whatever the next non-attribute line declares.
            let gates_a_module = lines[index + 1..]
                .iter()
                .find(|following| !following.trim_start().starts_with("#["))
                .is_some_and(|following| declares_module(following));
            if gates_a_module {
                offenders.push(format!("{relative}:{}", index + 1));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "a test-only module must stack `#[cfg(test)]` ABOVE the feature/platform gate — \
         cargo-mutants skips only a literal `#[cfg(test)]` module, so a combined \
         `#[cfg(all(test, …))]` leaks its helper functions as mutants (#3292). Rewrite as \
         `#[cfg(test)]` then `#[cfg(<rest>)]`. Offenders:\n  {}",
        offenders.join("\n  ")
    );
}
