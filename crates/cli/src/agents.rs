//! The self-describing agent surface (first-run D3).
//!
//! `strata agents` exposes the machine-readable knowledge the binary already
//! carries — the command catalog, the error registry, and a complete offline
//! usage guide — so an agent that lands anywhere on the surface is one hop
//! from the whole map, without web searches and version-locked to the
//! installed binary. `agents init` plants that pointer inside a repo so every
//! future agent session there starts oriented.

use std::fmt::Write as _;
use std::fs;
use std::path::Path;

use serde_json::{json, Value};
use strata_executor::error_registry::public_error_code_entries;

use crate::options::{AgentsCommand, Format};
use crate::render::render_value;
use crate::{guidance, CliError};

const POINTER_MARKER: &str = "## Strata";

/// Runs one agents command and returns the process exit code.
pub(crate) fn run(command: &AgentsCommand, format: Format) -> Result<i32, CliError> {
    match command {
        AgentsCommand::Guide => {
            // The guide is markdown; it is the format.
            println!("{}", guide_markdown()?);
        }
        AgentsCommand::Commands => render_value(&commands_value()?, format)?,
        AgentsCommand::Errors => render_value(&errors_value(), format)?,
        AgentsCommand::Init { apply } => render_value(&run_repo_init(*apply)?, format)?,
        AgentsCommand::Skill { write: false, .. } => {
            // The skill is markdown; it is the format.
            println!("{}", skill_markdown());
        }
        AgentsCommand::Skill {
            write: true,
            force,
            targets,
        } => {
            render_value(&run_skill_write(*force, targets)?, format)?;
        }
    }
    Ok(0)
}

// ---- guide -----------------------------------------------------------------

pub(crate) fn guide_markdown() -> Result<String, CliError> {
    let catalog = crate::catalog::embedded()?;
    let mut guide = String::new();
    let version = env!("CARGO_PKG_VERSION");

    // write!/writeln! into a String are infallible; results ignored throughout.
    let _ = write!(
        guide,
        "\
# Strata {version} — agent usage guide

Strata is an embedded multi-model database: one binary, one file-backed
database, no server. Six primitives share one storage substrate with
branches and time travel: key-value, JSON documents, vectors, an event log,
graphs, and product spaces. This guide is generated from the installed
binary's own metadata, so it cannot drift from what `strata {version}` does.

## Targeting a database

1. Explicit path or `--db <path>` — always wins: `strata ./my-db kv get k`
2. `STRATA_DB=<path>` — set once per session, used when no path is passed
3. `--cache` — explicit in-memory database (nothing persisted)

One-shot commands with no target refuse with
`invalid_argument.cli.no_database` — Strata never opens the current
directory implicitly. `strata <path>` with no command opens a REPL.

## Quickstart

```
{next_steps}
```

Branches fork cheaply and isolate writes; every primitive is branch-aware:

```
strata ./my-db branch fork default experiment
strata ./my-db kv put city tokyo --branch experiment
strata ./my-db kv get city --branch experiment   # tokyo
strata ./my-db kv get city                       # unchanged on default
```

Time travel: every write receipt carries a commit `timestamp`; pass it back
with `--as-of` on reads (kv/json/vector/event/graph alike):

```
strata --json ./my-db kv put k v1        # note data.commit.timestamp
strata ./my-db kv put k v2
strata ./my-db kv get k --as-of <t1>     # v1
```

## Output contract

- `--json`: one compact envelope per command — `{{\"type\": ..., \"data\": ...}}`.
  KV keys/values and cursors are base64 strings on the wire.
- `--raw`: shell-composable — the declared facts as bare values, one per
  line and tab-separated; no header, hint or summary. `--json` is the one
  unmodified record of what the engine returned.
- default: human-readable; binary values decode to text when valid UTF-8.
- Continuation cursors are opaque base64 tokens: pass the printed cursor
  back verbatim via `--cursor`.
- Raw serialized commands (the programmatic path):
  `strata <db> command run --command-json '{{\"type\":\"kv_get\",\"key\":\"a2V5\"}}'`

## Errors teach

Failures carry a stable code (`<class>.<area>.<detail>`), a one-line hint,
and a per-code ref (`https://stratadb.org/e/<code>`). `--json` failures emit
the full envelope on stderr. Recover by code and class, never by message
text. Full registry: `strata agents errors --json`.

## Diagnostics

`strata doctor [--json] [path]` checks the installation and (optionally) a
database, reporting coded issues with hints; it exits non-zero when
anything needs attention.

## MCP

`strata <db> mcp serve` speaks Model Context Protocol over stdio — ~20
curated tools plus `strata_guide` (this guide) and `strata_command` (any
cataloged command as raw wire JSON). Same envelopes, same error codes.
Client config: {{\"command\":\"strata\",\"args\":[\"<db-path>\",\"mcp\",\"serve\"]}}.

",
        version = version,
        next_steps = guidance::NEXT_STEPS.join("\n"),
    );

    guide.push_str("## Command catalog\n\n");
    let _ = writeln!(
        guide,
        "{} commands carry full metadata today (catalog JSON: `strata agents \
commands --json`); every command documents itself via `--help`.\n",
        catalog.commands().len()
    );
    for family in catalog.families() {
        let _ = writeln!(guide, "### {}", family.id);
        guide.push('\n');
        for id in &family.commands {
            if let Some(entry) = catalog.command_by_id(id) {
                let _ = writeln!(guide, "- `{}` — {}", entry.path_display, entry.summary);
            }
        }
        guide.push('\n');
    }

    guide.push_str(
        "## Repo onboarding\n\n`strata agents init` writes `.strata/AGENTS.md` into the current \
repo; `--apply` also appends a short pointer block to the repo's `AGENTS.md`/`CLAUDE.md` so every \
future agent session here starts oriented. `strata agents skill --write [--for all]` installs \
the condensed Python + CLI playbook for coding agents — Claude Code \
(`.claude/skills/strata/SKILL.md`), Cursor (`.cursor/rules/strata.mdc`), and Codex (a \
marker-delimited section in `AGENTS.md`) — loaded automatically when a session touches Strata.\n\
\n\
The fuller skill set (`strata`, `strata-branching`, `strata-time-travel`) lives in \
stratalab/strata-agent-skills: `npx skills add stratalab/strata-agent-skills`, or \
`/plugin marketplace add stratalab/strata-agent-skills` in Claude Code. The same repo's `init` \
command registers this MCP server with every agent surface in a workspace.\n",
    );

    Ok(guide)
}

// ---- catalogs ---------------------------------------------------------------

fn commands_value() -> Result<Value, CliError> {
    let catalog = crate::catalog::embedded()?;
    let index = serde_json::to_value(catalog.index())?;
    Ok(json!({
        "type": "agents_commands",
        "data": index,
    }))
}

fn errors_value() -> Value {
    let errors = public_error_code_entries()
        .map(|entry| {
            json!({
                "code": entry.code,
                "class": entry.class,
                "retry_policy": entry.retry_policy,
                "commit_outcome": entry.commit_outcome,
                "message": entry.message_template,
                "hint": entry.suggested_fix,
                "ref": format!("https://stratadb.org/e/{}", entry.docs_slug),
            })
        })
        .collect::<Vec<_>>();
    json!({
        "type": "agents_errors",
        "data": {
            "count": errors.len(),
            "errors": errors,
        }
    })
}

// ---- skill ------------------------------------------------------------------

/// Path the skill installs to, relative to the repo root (Claude Code's
/// project-skill location).
const SKILL_PATH: &str = ".claude/skills/strata/SKILL.md";

/// The Claude Code skill, version-stamped. The template is authored at
/// `agents_skill.md` and vendored by the Python SDK (which serves the same
/// text as `stratadb.agents_skill()`), so the two surfaces cannot drift.
pub(crate) fn skill_markdown() -> String {
    include_str!("agents_skill.md").replace("{version}", env!("CARGO_PKG_VERSION"))
}

/// Cursor's rule file (MDC): frontmatter Cursor understands, same body.
const CURSOR_RULE_PATH: &str = ".cursor/rules/strata.mdc";
/// Codex reads the repo root `AGENTS.md`; the skill body lives there between
/// these markers so re-runs can replace exactly the region we own.
const CODEX_AGENTS_PATH: &str = "AGENTS.md";
const CODEX_START: &str = "<!-- strata-skill:start -->";
const CODEX_END: &str = "<!-- strata-skill:end -->";

/// The skill body without the Claude skill frontmatter (the part between and
/// after the `---` fence pair), plus the trigger description on its own.
fn skill_parts() -> (String, String) {
    let skill = skill_markdown();
    if let Some(rest) = skill.strip_prefix("---\n") {
        if let Some(end) = rest.find("\n---\n") {
            let description = rest[..end]
                .lines()
                .find_map(|line| line.strip_prefix("description: "))
                .unwrap_or_default()
                .to_owned();
            let body = rest[end + "\n---\n".len()..].trim_start().to_owned();
            return (description, body);
        }
    }
    (String::new(), skill)
}

/// Cursor MDC rule: agent-attached (not always-on), same trigger text.
fn cursor_rule() -> String {
    let (description, body) = skill_parts();
    format!("---\ndescription: {description}\nalwaysApply: false\n---\n\n{body}")
}

/// Writes a whole file we own: created/unchanged/replaced, refusing to
/// replace differing content unless `force` (state `pending`).
fn write_owned_file(
    path: &Path,
    content: &str,
    force: bool,
) -> Result<(&'static str, Option<String>), CliError> {
    if path.exists() {
        let existing = fs::read_to_string(path)?;
        if existing == content {
            return Ok(("unchanged", None));
        }
        if !force {
            return Ok((
                "pending",
                Some(format!(
                    "an existing {} differs; re-run with --force to replace it",
                    path.display()
                )),
            ));
        }
        fs::write(path, content)?;
        return Ok(("replaced", None));
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, content)?;
    Ok(("created", None))
}

/// Installs the skill body into `AGENTS.md` between the strata markers.
/// We own the marked region, not the file: the region is replaced in place
/// on re-runs (no `--force` needed), and a missing file is created.
fn write_codex_section() -> Result<(&'static str, Option<String>), CliError> {
    let (_, body) = skill_parts();
    let section = format!("{CODEX_START}\n{body}\n{CODEX_END}\n");
    let path = Path::new(CODEX_AGENTS_PATH);
    if !path.exists() {
        fs::write(path, &section)?;
        return Ok(("created", None));
    }
    let existing = fs::read_to_string(path)?;
    if let (Some(start), Some(end)) = (existing.find(CODEX_START), existing.find(CODEX_END)) {
        let current = &existing[start..end + CODEX_END.len()];
        let replacement = section.trim_end();
        if current == replacement {
            return Ok(("unchanged", None));
        }
        let updated = format!(
            "{}{}{}",
            &existing[..start],
            replacement,
            &existing[end + CODEX_END.len()..]
        );
        fs::write(path, updated)?;
        return Ok(("replaced", None));
    }
    let mut updated = existing;
    if !updated.ends_with('\n') {
        updated.push('\n');
    }
    updated.push('\n');
    updated.push_str(&section);
    fs::write(path, updated)?;
    Ok(("appended", None))
}

/// Installs the skill for the requested agents, reporting one result per
/// target file.
fn run_skill_write(
    force: bool,
    targets: &[crate::options::SkillTarget],
) -> Result<Value, CliError> {
    use crate::options::SkillTarget;
    let all = targets.contains(&SkillTarget::All);
    let wants = |t: SkillTarget| all || targets.contains(&t);

    let mut results = Vec::new();
    if wants(SkillTarget::Claude) {
        let (state, next) = write_owned_file(Path::new(SKILL_PATH), &skill_markdown(), force)?;
        results
            .push(json!({ "agent": "claude", "path": SKILL_PATH, "state": state, "next": next }));
    }
    if wants(SkillTarget::Cursor) {
        let (state, next) = write_owned_file(Path::new(CURSOR_RULE_PATH), &cursor_rule(), force)?;
        results.push(
            json!({ "agent": "cursor", "path": CURSOR_RULE_PATH, "state": state, "next": next }),
        );
    }
    if wants(SkillTarget::Codex) {
        let (state, next) = write_codex_section()?;
        results.push(
            json!({ "agent": "codex", "path": CODEX_AGENTS_PATH, "state": state, "next": next }),
        );
    }
    Ok(json!({ "type": "agents_skill", "data": { "written": results } }))
}

// ---- repo onboarding --------------------------------------------------------

/// The ~10-line pointer block planted in repos: a pointer, not a manual.
fn pointer_block() -> String {
    format!(
        "\
{POINTER_MARKER}

This repo uses Strata (embedded database — SQLite-shaped, zero-config).

- Full usage guide: run `strata agents guide` (offline, version-matched)
- Agent skill: `strata agents skill --write --for all` (Claude Code, Cursor, Codex)
- Fuller skill set: `npx skills add stratalab/strata-agent-skills` (usage, branching, time travel)
- Command catalog: `strata agents commands --json`; errors: `strata agents errors --json`
- Database targeting: pass a path or set `STRATA_DB`; never rely on cwd
- Structured output: add `--json` to any command; raw commands via `strata <db> command run --command-json`
- MCP: `strata <db> mcp serve` (stdio; same envelopes and error codes)
"
    )
}

fn run_repo_init(apply: bool) -> Result<Value, CliError> {
    fs::create_dir_all(".strata")?;
    fs::write(Path::new(".strata/AGENTS.md"), pointer_block())?;

    let pointer_target = ["AGENTS.md", "CLAUDE.md"]
        .into_iter()
        .find(|candidate| Path::new(candidate).exists());

    // absent: the repo has no agent instructions file to point from.
    // present: the pointer block is already planted (idempotent re-runs).
    // appended: --apply added the block just now.
    // pending: a target exists; re-run with --apply to plant the pointer.
    let mut pointer_state = "absent";
    if let Some(target) = pointer_target {
        let existing = fs::read_to_string(target)?;
        if existing.contains(POINTER_MARKER) {
            pointer_state = "present";
        } else if apply {
            let mut updated = existing;
            if !updated.ends_with('\n') {
                updated.push('\n');
            }
            updated.push('\n');
            updated.push_str(&pointer_block());
            fs::write(target, updated)?;
            pointer_state = "appended";
        } else {
            pointer_state = "pending";
        }
    }

    Ok(json!({
        "type": "agents_init",
        "data": {
            "written": [".strata/AGENTS.md"],
            "pointer_target": pointer_target,
            "pointer": pointer_state,
            "next": if pointer_state == "pending" {
                Value::String(
                    "run `strata agents init --apply` to append the pointer block".to_owned(),
                )
            } else {
                Value::Null
            },
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The repo pointer block is a pointer, not a manual: it must carry the
    /// marker plus each surface it points at (guide, skill verb, the fuller
    /// skill set, catalogs, database targeting, MCP).
    #[test]
    fn pointer_block_points_at_every_surface() {
        let block = pointer_block();
        assert!(block.starts_with(POINTER_MARKER));
        for needle in [
            "strata agents guide",
            "strata agents skill --write --for all",
            "npx skills add stratalab/strata-agent-skills",
            "strata agents commands --json",
            "STRATA_DB",
            "mcp serve",
        ] {
            assert!(block.contains(needle), "pointer block lost `{needle}`");
        }
    }

    /// Consistency guard: the generated CLI agent guide has a catalog section
    /// for every command family, so a family cannot silently drop out of the
    /// guide an agent reads (mirrors the SDK-side agent-guide coverage guard).
    #[test]
    fn guide_covers_every_catalog_family() {
        let guide = guide_markdown().expect("agent guide renders");
        let catalog = crate::catalog::embedded().expect("embedded catalog resolves");
        for family in catalog.families() {
            assert!(
                guide.contains(&format!("### {}", family.id)),
                "agent guide is missing the section for family `{}`",
                family.id
            );
        }
    }

    /// `strata agents commands` is the embedded catalog under the agents
    /// envelope — the same index the renderer resolves declarations from, not
    /// a second copy — so its command list is the catalog's, wire for wire.
    #[test]
    fn commands_value_carries_the_embedded_catalog_index() {
        let value = commands_value().expect("agents commands renders");
        assert_eq!(value["type"], "agents_commands");
        let listed = value["data"]["commands"]
            .as_array()
            .expect("the payload lists the commands")
            .iter()
            .map(|command| {
                command["wire"]
                    .as_str()
                    .expect("every entry names its wire")
            })
            .collect::<Vec<_>>();
        let catalog = crate::catalog::embedded().expect("embedded catalog resolves");
        let expected = catalog
            .commands()
            .iter()
            .map(|command| command.wire.as_str())
            .collect::<Vec<_>>();
        assert!(!expected.is_empty(), "the embedded catalog has commands");
        assert_eq!(listed, expected);
    }

    /// The skill is a valid Claude Code skill: YAML frontmatter with the
    /// trigger description, version-stamped body, and the canonical Python
    /// entry point — with no unexpanded template placeholder left behind.
    #[test]
    fn skill_renders_frontmatter_version_and_entry_point() {
        let skill = skill_markdown();
        assert!(skill.starts_with("---\nname: strata\ndescription: "));
        assert!(skill.contains(&format!("# StrataDB {}", env!("CARGO_PKG_VERSION"))));
        assert!(skill.contains("stratadb.open("));
        assert!(skill.contains("stratadb.agents_guide()"));
        assert!(!skill.contains("{version}"));
    }

    /// The Cursor rule carries MDC frontmatter (description + alwaysApply)
    /// and the same body; the Codex parts split cleanly (description text,
    /// body starting at the title, no leftover fence).
    #[test]
    fn cursor_and_codex_derivations_share_the_body() {
        let (description, body) = skill_parts();
        assert!(description.starts_with("Use when working with StrataDB"));
        assert!(body.starts_with("# StrataDB"));
        assert!(!body.contains("name: strata"));

        let rule = cursor_rule();
        assert!(rule.starts_with("---\ndescription: Use when working with StrataDB"));
        assert!(rule.contains("alwaysApply: false"));
        assert!(rule.contains("stratadb.open("));
    }
}
