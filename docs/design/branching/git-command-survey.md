# Git Command Survey for Strata

**Date:** 2026-03-18
**Purpose:** Comprehensive mapping of every git command to Strata's embedded database context, identifying which capabilities unlock novel AI-powered workflows

---

## Context: Strata AI is Claude Code for Data

**Strata AI** is an autonomous AI agent — built on the Anthropic Agent SDK and Claude API — that operates on a Strata database the way **Claude Code** operates on a codebase. The architectural parallel is direct:

| Claude Code | Strata AI |
|-------------|-----------|
| Opens a git repo | Opens a Strata database |
| Reads/writes files | Reads/writes KV, JSON, State, Events, Vectors, Graphs |
| Uses git branches to isolate work | Uses Strata branches to isolate work |
| Shows diffs for user review | Shows branch diffs for user review |
| Commits changes | Commits via transactions |
| Can revert, cherry-pick, bisect | Needs these same powers over data |
| Runs tools autonomously | Runs data operations autonomously |
| Uses Anthropic Agent SDK + Claude API | Uses Anthropic Agent SDK + Claude API |

Claude Code's power comes from git. It can branch, experiment, diff, revert, and show its work — all because git provides those primitives over source code. **Strata AI needs the same primitives over data.** Every git capability we build into Strata directly translates into a capability Strata AI can use to operate on a user's database safely, transparently, and reversibly.

The `_system_` branch is Strata AI's private workspace — like Claude Code's internal state. It stores AI memories, conversation context, conflict resolution rules, and operational metadata. The user's data lives on user branches. Strata AI forks, operates, diffs, and merges — exactly like Claude Code does with git.

---

## Reading Guide

Each command is rated:

| Rating | Meaning |
|--------|---------|
| **HAVE** | Already implemented in Strata |
| **BUILD** | High value, should be built |
| **FUTURE** | Valuable but depends on other work landing first |
| **N/A** | Does not apply to an embedded database |

The **AI Angle** column describes how Strata AI could use the capability — framed through the lens of "what would Claude Code do if it operated on data instead of source code?"

---

## 1. Core — The Operations Users Touch Daily

### 1.1 Snapshotting

| Git Command | What It Does | Strata Equivalent | Status | AI Angle |
|------------|-------------|-------------------|--------|----------|
| `git init` | Create empty repository | `Strata::open()` / `strata --db ./path` | **HAVE** | — |
| `git add` | Stage changes for commit | N/A — Strata commits immediately via `put`/`set`/`append` | **N/A** | Strata has no staging area by design. Every write is atomic. |
| `git commit` | Record snapshot | `db.transaction()` — groups writes into a single versioned commit | **HAVE** | AI could "checkpoint" its work at meaningful moments, creating named snapshots it can return to |
| `git status` | Show what changed | `branch diff` (partial) | **BUILD** | AI asks "what changed since I last looked?" to orient itself after context loss |
| `git diff` | Show changes between states | `branch diff` (two-way) / `diff_three_way` (planned) | **HAVE** (partial) | AI diffs its current workspace against a known-good state to detect drift or corruption |
| `git log` | Show commit history | `event_log` on `_system_` branch | **BUILD** | AI replays its own decision history to understand why the database is in its current state |
| `git show` | Display a specific commit/object | `get_versioned()` + `get_history()` | **HAVE** (partial) | AI inspects a specific past version of a key to understand what changed |
| `git rm` | Delete tracked file | `kv_delete` / `txn.delete()` — writes tombstone | **HAVE** | — |
| `git mv` | Rename/move file | No direct equivalent — delete + put under new key | **HAVE** (manual) | AI could implement key migration/renaming as an atomic transaction |

### 1.2 Branching & Merging

| Git Command | What It Does | Strata Equivalent | Status | AI Angle |
|------------|-------------|-------------------|--------|----------|
| `git branch` | List/create/delete branches | `branch create` / `branch list` / `branch del` | **HAVE** | AI creates branches for each experiment, hypothesis, or user session |
| `git checkout` / `git switch` | Switch active branch | `use <branch>` (CLI) / session branch selection | **HAVE** | AI switches context between parallel workstreams |
| `git merge` | Join histories | `branch merge` (two-way, broken) → three-way merge (planned) | **BUILD** | **The killer feature.** AI runs experiments on branches, merges winning results back. See Section 3. |
| `git merge-base` | Find common ancestor | `compute_merge_base()` (planned) | **BUILD** | AI determines where two branches diverged to understand the scope of changes |
| `git rebase` | Reapply commits on new base | No equivalent yet | **FUTURE** | AI "replays" a sequence of operations on top of a different starting state. See Section 3. |
| `git cherry-pick` | Apply single commit to another branch | No equivalent yet | **BUILD** | **High value.** AI selectively picks specific changes from one branch and applies them to another without a full merge |
| `git revert` | Undo a specific commit | No equivalent yet | **BUILD** | **High value.** AI reverses a specific change while preserving everything after it. "Undo that one thing without losing subsequent work" |
| `git stash` | Temporarily shelve changes | Fork to temp branch, work, merge back | **HAVE** (pattern) | AI "parks" work-in-progress when interrupted by a higher-priority task |
| `git tag` | Name a specific point in history | KV on `_system_` branch: `tags:{name} → version` | **BUILD** | AI creates semantic bookmarks: "pre-migration", "hypothesis-3-validated", "user-approved-checkpoint" |
| `git worktree` | Multiple working directories | Multiple branches (already isolated) | **HAVE** | AI works on multiple tasks simultaneously, each in its own branch |

### 1.3 Inspection & Comparison

| Git Command | What It Does | Strata Equivalent | Status | AI Angle |
|------------|-------------|-------------------|--------|----------|
| `git log` | Commit history | Version history via `get_history()` + DAG events | **HAVE** (partial) | AI reviews what happened over time |
| `git blame` | Who changed what, when | Version history per key shows commit_id but not actor | **FUTURE** | AI traces which agent session or user modified a key, for debugging or attribution |
| `git bisect` | Binary search for a bug-introducing commit | No equivalent | **BUILD** | **Novel.** AI binary-searches through database versions to find when data went wrong. See Section 3. |
| `git grep` | Search across files | `kv_list` with prefix / vector search | **HAVE** (partial) | AI searches its own workspace and knowledge base |
| `git shortlog` | Summarized history | Aggregate event log by type/actor | **FUTURE** | AI generates summaries of what happened in a branch |
| `git reflog` | Record of all HEAD changes | Event log on `_system_` branch | **BUILD** | Safety net: AI can recover from mistakes by seeing exactly what operations were performed |
| `git describe` | Human-readable name for a version | Tag lookup for nearest tagged version | **FUTURE** | AI refers to versions by meaningful names instead of opaque numbers |

---

## 2. Infrastructure — What Makes the Magic Possible

### 2.1 DAG & History

| Git Command | What It Does | Strata Equivalent | Status | AI Angle |
|------------|-------------|-------------------|--------|----------|
| `git rev-list` | List commits in topological order | DAG traversal on `_branch_dag` graph | **BUILD** | AI walks the full history of a branch lineage |
| `git rev-parse` | Resolve references to commit IDs | Resolve branch name → version number | **HAVE** | — |
| `git cat-file` | Inspect raw objects | `get_versioned()` — read value at specific version | **HAVE** | AI inspects historical state |
| `git ls-tree` | List contents of a snapshot | `list_by_type_at_version()` (planned) | **BUILD** | AI enumerates all data in a branch at a specific point in time |
| `git show-branch` | Visualize branch divergence | DAG query showing fork/merge topology | **FUTURE** | AI explains to users how their data branches relate |
| `git range-diff` | Compare two commit ranges | Diff version ranges on two branches | **FUTURE** | AI compares what changed in two parallel experiments over the same period |

### 2.2 Maintenance & GC

| Git Command | What It Does | Strata Equivalent | Status | AI Angle |
|------------|-------------|-------------------|--------|----------|
| `git gc` | Garbage collect unreachable objects | Compaction (Epics 22-25) + MVCC GC (future) | **HAVE** (partial) | — |
| `git prune` | Remove unreachable objects | Branch deletion + compaction | **HAVE** | AI cleans up expired experiment branches |
| `git fsck` | Verify object database integrity | No equivalent | **FUTURE** | AI validates database consistency and reports issues |
| `git repack` | Optimize storage packing | Compaction scheduling | **HAVE** | — |
| `git pack-refs` | Optimize reference storage | N/A (branches are already compact) | **N/A** | — |

### 2.3 Remote & Sharing

| Git Command | What It Does | Strata Equivalent | Status | AI Angle |
|------------|-------------|-------------------|--------|----------|
| `git clone` | Copy entire repository | Database file copy / backup | **HAVE** (file-level) | — |
| `git fetch` | Download remote changes | N/A — embedded database, no remotes | **N/A** | — |
| `git pull` | Fetch + merge | N/A | **N/A** | — |
| `git push` | Upload changes | N/A | **N/A** | — |
| `git remote` | Manage remotes | N/A | **N/A** | — |
| `git bundle` | Package repo for offline transfer | Database export | **FUTURE** | AI could package a branch's data for transfer or sharing |
| `git archive` | Create tarball of snapshot | Export branch data at version | **FUTURE** | AI exports a clean snapshot for reporting or handoff |

### 2.4 Patching

| Git Command | What It Does | Strata Equivalent | Status | AI Angle |
|------------|-------------|-------------------|--------|----------|
| `git format-patch` | Create portable patch files | Export diff as serialized changeset | **FUTURE** | AI creates a "patch" — a portable set of changes that can be applied to any branch |
| `git apply` | Apply a patch | Apply serialized changeset to branch | **FUTURE** | AI applies changes from one database to another |
| `git am` | Apply patches from mailbox | N/A | **N/A** | — |
| `git cherry` | Find unapplied changes | Diff branch against target showing what hasn't been merged | **FUTURE** | AI identifies which experiment results haven't been promoted yet |

### 2.5 Configuration & Plumbing

| Git Command | What It Does | Strata Equivalent | Status | AI Angle |
|------------|-------------|-------------------|--------|----------|
| `git config` | Get/set config | `strata.toml` / runtime config | **HAVE** | — |
| `git hook` | Pre/post operation hooks | No equivalent | **FUTURE** | AI registers triggers: "after any write to this key, run this validation" |
| `git notes` | Attach metadata to objects | JSON metadata on `_system_` branch | **BUILD** | AI annotates versions with context: "this was generated by prompt X with model Y" |
| `git replace` | Substitute objects transparently | No equivalent | **N/A** | — |
| `git filter-branch` | Rewrite history | No equivalent | **N/A** | Dangerous for a database. Maybe useful for data migration. |
| `git rerere` | Remember conflict resolutions | No equivalent | **FUTURE** | **Novel.** AI remembers how it resolved a conflict and auto-applies the same resolution next time. See Section 3. |
| `git submodule` | Nested repositories | Nested/linked databases | **FUTURE** | AI manages hierarchical data stores |

---

## 3. The AI-Powered Workflows

**The core thesis:** Claude Code's power comes from git. Strata AI's power comes from git-like database primitives.

Claude Code doesn't just read and write files — it branches, experiments, diffs, reverts, and shows its work. Every git command Claude Code uses translates to a Strata AI capability. The difference is the substrate: source code vs. structured data.

These workflows describe what Strata AI — an agent built on the Anthropic Agent SDK and Claude API, operating autonomously on a user's database — can do when Strata provides the full git mental model.

### 3.1 Speculative Execution (`fork` + `merge`)

**Claude Code parallel:** Claude Code creates a feature branch, makes changes, shows a diff, and the user accepts or rejects.

**Strata AI does the same with data:**
```
User: "Reorganize my notes by topic instead of by date"

Strata AI (via Agent SDK tool calls):
1. fork(default → _ai_reorganize)        # isolated sandbox
2. [AI reads all notes, builds topic clusters, rewrites keys]
3. diff(_ai_reorganize, default)          # show user what changed
4. User approves → merge(_ai_reorganize → default)
5. User rejects → branch del _ai_reorganize   # zero impact
```

**Why this is novel:** Claude Code can do this with files because git provides branching. No database has ever let an AI agent do this with data — make sweeping changes in a sandbox, show the user a clean diff, and atomically apply or discard. Strata AI gets the same safety model that makes Claude Code trustworthy.

### 3.2 Hypothesis Testing (`fork` × N + `diff`)

**Claude Code parallel:** Developer asks Claude Code to try multiple approaches, it can create branches for each.

**Strata AI runs parallel experiments:**
```
User: "Find the best embedding model for my documents"

Strata AI:
1. fork(default → _ai_embed_model_a)
2. fork(default → _ai_embed_model_b)
3. fork(default → _ai_embed_model_c)
4. [Run each model on its own branch, storing vectors + metrics]
5. diff(default, _ai_embed_model_a)   # compare results
6. diff(default, _ai_embed_model_b)
7. diff(default, _ai_embed_model_c)
8. merge(best_branch → default)       # promote winner
9. branch del _ai_embed_model_*       # clean up losers
```

**Why this is novel:** Each hypothesis gets its own universe with full data isolation. The user sees diffs showing exactly what each approach produced. The AI promotes the winner and discards the rest — the same pattern as trying multiple implementations in Claude Code.

### 3.3 Safe Rollback (`revert`)

**Claude Code parallel:** `git revert <commit>` — undo a specific commit without losing subsequent work.

**Strata AI does surgical undo:**
```
User: "That last AI action messed up my data, undo it"

Strata AI:
1. Find the version range of the problematic operation (via event log)
2. For each key modified in that range:
   - Read the value BEFORE the operation (via MVCC)
   - Write it back as the current value
3. Record a "revert" event in the audit log
```

**Why this is novel:** Point-in-time undo of any operation, even if other writes happened after it. Traditional databases can only roll back to the last checkpoint, losing subsequent work. This is the data equivalent of `git revert` — surgical, precise, history-preserving.

### 3.4 Bisect (`bisect`)

**Claude Code parallel:** A developer uses `git bisect` to find which commit introduced a bug. Strata AI does the same with data versions.

**Strata AI debugs data regressions:**
```
User: "Something is wrong with my recommendation scores, they were fine last week"

Strata AI:
1. binary_search(good_version, bad_version):
   a. mid = (good + bad) / 2
   b. Read state at version mid (MVCC snapshot)
   c. Run validation check against that state
   d. If bad → bad = mid; else good = mid
   e. Repeat until converged
2. Report: "Version 4,217 introduced the issue. Here's what changed:"
3. Show diff(version_4216, version_4217)
```

**Why this is novel:** Binary search through database history to find exactly when data went wrong. The AI can even use Claude to evaluate each midpoint ("does this data look correct?"), combining MVCC time-travel with LLM judgment. No database has ever offered this.

### 3.5 Cherry-Pick (selective merge)

**Claude Code parallel:** Cherry-picking a specific commit from one branch to another without merging everything.

**Strata AI picks surgically:**
```
User: "I like the new categories from experiment-A but keep the descriptions from experiment-B"

Strata AI:
1. Identify the specific keys/changes from experiment-A that affect categories
2. Apply ONLY those changes to the target branch
3. Leave everything else untouched
```

**Why this is novel:** Not "merge everything" but "merge exactly these changes." Because Strata AI understands the semantic content (via Claude), it can cherry-pick at a level of abstraction that raw git cherry-pick can't — it understands what "categories" means and selects the right keys.

### 3.6 Time Travel Queries

**Claude Code parallel:** `git show HEAD~5:file.txt` — view any file at any point in history.

**Strata AI navigates temporal references in natural language:**
```
User: "What did my user profile look like before the migration?"

Strata AI:
1. Find the migration's version via event log or tag
2. Read all profile keys at (migration_version - 1)
3. Present the historical snapshot
4. Optionally diff against current state
```

**Why this is novel:** Users don't need to know version numbers. The AI resolves temporal references ("before the migration", "last Tuesday", "when we had 1000 users") to concrete MVCC versions using the event log on `_system_`. Claude Code can `git show` any commit — Strata AI can read any key at any version.

### 3.7 Conflict Resolution Memory (`rerere`)

**Claude Code parallel:** `git rerere` remembers how you resolved a merge conflict and auto-applies the same resolution next time.

**Strata AI learns from human decisions:**
```
Agent session A writes: config:theme = "dark"
Agent session B writes: config:theme = "auto"

First time: conflict → user resolves: "always prefer session A for UI preferences"

Strata AI records resolution rule on _system_ branch:
  rerere:config:theme → { prefer: "session_a", reason: "user preference" }

Next time same conflict occurs → auto-resolved, logged for audit
```

**Why this is novel:** The database learns from human conflict resolution decisions and applies them automatically. Combined with Claude's reasoning, Strata AI can generalize rules ("user always prefers explicit values over auto-detection") rather than just memorizing exact key matches. No merge tool has ever done LLM-informed conflict resolution.

### 3.8 Branch-Per-Conversation

**Claude Code parallel:** This IS Claude Code's model. Every conversation gets its own context. Changes are shown as diffs. The user accepts or rejects.

**Strata AI operates identically, but on data:**
```
User starts conversation with Strata AI:
1. fork(default → _ai_conv_{id})      # AI gets its own sandbox
2. AI reads/writes freely on its branch (via Agent SDK tool calls)
3. Conversation ends:
   a. merge(_ai_conv_{id} → default)   # persist what the AI produced
   b. OR branch del _ai_conv_{id}      # discard if user doesn't want results
4. User can inspect what AI did: diff(default, _ai_conv_{id})
```

**Why this maps perfectly:** Claude Code's safety model — "the AI works in isolation, shows you what it did, and you decide" — is exactly what branch-per-conversation provides for data. The `_system_` branch stores Strata AI's own memories and context across conversations, just like Claude Code maintains its own state between sessions.

### 3.9 Progressive Refinement (`rebase`)

**Claude Code parallel:** Interactive rebase — reorder, squash, or redo specific commits while keeping the rest.

**Strata AI replays from any checkpoint:**
```
User: "Take my draft analysis and improve each section"

Strata AI:
1. Read the original analysis (version V1)
2. For each section, in sequence:
   a. Improve section → write new version
   b. If user says "actually, redo section 3 differently":
      - Read V1 state for section 3 (MVCC time-travel)
      - Rewrite from V1, not from the failed attempt
      - Continue forward from there
```

**Why this is novel:** The AI can "replay" its work from any point, incorporating corrections without losing the rest of its progress. This is the data equivalent of Claude Code re-running a failed edit from a clean state.

### 3.10 Audit Trail & Provenance

**Claude Code parallel:** `git log` + `git blame` — full history of who changed what, when, and why.

**Strata AI provides complete provenance:**
```
User: "Show me every change the AI made to my data this week"

Strata AI:
1. Query event log on _system_ for all operations in date range
2. For each change: version, timestamp, conversation_id, prompt that triggered it
3. Generate report linking every data change to the user request that caused it
```

**Why this is novel:** Every data mutation is version-tracked (MVCC) and every AI operation is logged (Events on `_system_`). Strata AI can produce a full chain: user prompt → Agent SDK tool call → database write → version number. This is the data equivalent of `git blame` — but instead of "who wrote this line," it's "which conversation changed this data and why."

---

## 4. Priority Matrix

### Must Build (Phase 1 — enables three-way merge and fixes #1537)

| Capability | Git Equivalent | Why |
|-----------|---------------|-----|
| Three-way merge | `git merge` | Fixes #1537, enables speculative execution |
| Merge base | `git merge-base` | Required for three-way merge |
| Version-scoped listing | `git ls-tree <commit>` | Required for ancestor reconstruction |
| Fork version tracking | `git commit` parent pointers | Required for merge base |

### Should Build (Phase 2 — unlocks the AI workflows)

| Capability | Git Equivalent | Why |
|-----------|---------------|-----|
| Cherry-pick | `git cherry-pick` | Selective merge for AI experiment promotion |
| Revert | `git revert` | Safe undo of any operation without losing subsequent work |
| Tags | `git tag` | Semantic bookmarks for AI checkpoints |
| Branch audit log | `git log` | Full operation history on `_system_` branch via Events |
| Notes | `git notes` | AI annotates versions with metadata (prompt, model, reasoning) |

### Could Build (Phase 3 — deeper git semantics)

| Capability | Git Equivalent | Why |
|-----------|---------------|-----|
| Bisect | `git bisect` | Automated regression detection in data |
| Reflog | `git reflog` | Safety net for AI mistake recovery |
| Rerere | `git rerere` | Learned conflict resolution |
| Rebase | `git rebase` | Replay operations on new base state |
| Patch export/import | `git format-patch` / `git apply` | Portable changesets between databases |
| Range diff | `git range-diff` | Compare parallel experiments |

### Not Applicable

| Capability | Git Equivalent | Why Not |
|-----------|---------------|---------|
| Staging area | `git add` | Strata writes are immediate; transactions serve this purpose |
| Remotes | `git remote/fetch/push/pull` | Embedded database — no network protocol |
| Submodules | `git submodule` | No nested database concept (yet) |
| Object replacement | `git replace` | No use case for transparent substitution |
| History rewriting | `git filter-branch` | Too dangerous for a database |
| Email patches | `git send-email/am` | N/A |

---

## 5. The `_system_` Branch as the Foundation

All of these capabilities are powered by the `_system_` branch, which serves as:

1. **Registry** — immutable record of everything that happened (branch DAG, event log, tags, rerere rules)
2. **AI Workspace** — Strata AI's private scratchpad for memories, context, and operational state

### Primitive Usage Map

| `_system_` Primitive | What Gets Stored | Used By |
|---------------------|-----------------|---------|
| **Graph** (`_branch_dag`) | Branch topology: nodes, fork events, merge events with versions | Merge base computation, branch visualization |
| **Events** | Append-only audit log: every fork, merge, delete, tag, revert | `log`, `reflog`, audit trail, provenance |
| **KV** | Tags (`tag:{name} → version`), rerere rules, AI config | `tag`, `rerere`, AI workspace |
| **JSON** | Rich metadata: notes on versions, AI annotations, conflict resolution context | `notes`, AI provenance |
| **State** | Operational state: current bisect range, in-progress merge state | `bisect`, long-running operations |

### Namespace Convention

```
_system_/default/kv:
  tag:{name}              → version number
  rerere:{key_hash}       → resolution rule (JSON)
  _ai:memory:{id}         → AI memory entry
  _ai:config:{key}        → AI configuration

_system_/default/events:
  branch.fork             → { parent, child, fork_version, ... }
  branch.merge            → { source, target, merge_version, strategy, ... }
  branch.delete           → { name, deleted_at, ... }
  branch.tag              → { name, version, message, ... }
  branch.revert           → { branch, from_version, to_version, keys_affected, ... }
  ai.action               → { conversation_id, action, ... }

_system_/default/json:
  note:{branch}:{version} → { message, author, timestamp, metadata }

_system_/default/state:
  bisect:{branch}         → { good, bad, current, step }
```

---

## 6. What This Means

Claude Code changed what developers expect from an AI coding tool. It didn't just generate code — it gave the AI the full power of git: branch, experiment, diff, review, merge, revert. The AI operates safely because git provides the safety model.

**Strata AI is the same paradigm shift, applied to data.**

Built on the same foundation — Anthropic Agent SDK, Claude API — Strata AI gives the AI agent the full power of git-like database primitives. The parallel is exact:

| What Claude Code does with git | What Strata AI does with Strata |
|-------------------------------|-------------------------------|
| Branches to isolate experiments | Forks to isolate data experiments |
| Shows diffs for user review | Shows branch diffs for user review |
| Reverts specific commits | Reverts specific data changes |
| Cherry-picks across branches | Cherry-picks across data branches |
| Uses git log for context | Uses `_system_` events for context |
| Stores state in git repo | Stores state on `_system_` branch |

The `_system_` branch is Strata AI's brain — its memories, conflict resolution rules, conversation context, and operational state persist there across sessions. User data lives on user branches. Strata AI forks, operates, diffs, and merges — showing its work at every step.

No database has ever given an AI agent this level of control:

- **Experiment safely** — branch, try things, merge or discard
- **Show its work** — diff reveals exactly what changed
- **Undo surgically** — revert any specific action without losing subsequent work
- **Learn from mistakes** — rerere remembers how conflicts were resolved
- **Debug data issues** — bisect finds when things went wrong
- **Audit everything** — full provenance chain linking every data change to the conversation that caused it

Traditional databases offer transactions and maybe point-in-time recovery. Strata offers the full git mental model — purpose-built for AI agents that need to operate autonomously, safely, and transparently on a user's data.

---

## Sources

- [Git Reference Manual](https://git-scm.com/docs)
- [Git Internals - Plumbing and Porcelain](https://git-scm.com/book/en/v2/Git-Internals-Plumbing-and-Porcelain)
- Strata design docs: `current-state.md`, `git-reference-architecture.md`, `implementation-design.md`
- Strata cookbooks: `agent-state-management.md`, `multi-agent-coordination.md`
