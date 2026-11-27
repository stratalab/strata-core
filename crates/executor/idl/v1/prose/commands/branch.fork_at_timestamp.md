---
summary: Fork a new branch from a retained source commit timestamp.
mcp_description: Use this when the user wants to branch from an existing branch as of an earlier commit, named by its commit timestamp.
---

Forks a new branch anchored at a retained position on the source's logical commit timeline: the `timestamp` from a write receipt or `history` output. It is a per-commit counter, not the `version`, not the wall-clock `committed_at` beside it, and never a calendar date. The engine resolves the timestamp to the covering retained commit; the returned parent lineage records both the fork timestamp and the resolved fork version. A timestamp before the source's first commit fails with `history_unavailable.engine.before_first_commit`, and one after its latest commit with `history_unavailable.engine.after_latest_commit` rather than forking the current state; `history_unavailable.engine.persistence_history` is reserved for history a retention policy pruned.

This command has no dedicated CLI verb: the CLI expresses it as `strata branch fork <SOURCE> <BRANCH> --timestamp <TIMESTAMP>` (one shared `branch fork` verb routes to all three fork commands, so only `branch.fork` owns the CLI path). It remains fully reachable through the generic wire surface — `strata command run`, MCP, and SDKs.
