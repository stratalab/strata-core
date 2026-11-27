---
summary: Read multiple KV values by key.
mcp_description: Use this when the user wants to fetch several KV keys in one request.
---

Reads several KV keys and returns positional item results. Each item records whether the corresponding key was found and includes value metadata when present.

Every key is read from one snapshot, so a commit that lands while the batch runs is visible to every key or to none. With `as_of` (a position on the logical commit timeline) or `as_of_time` (a wall-clock instant), every key is read as of that one position, so a batch can hydrate the entries a version-pinned read named — a graph query at a version, say — without mixing in later state. Keys keep their order, a repeated key is answered again, and a key absent at that position is a miss. An instant outside the branch's recorded history fails the whole batch rather than answering from the latest state: `history_unavailable.engine.before_first_commit` or `history_unavailable.engine.after_latest_commit` names the end it missed, `history_unavailable.engine.undated_history` refuses an `as_of_time` over commits that record no wall-clock time, and `history_unavailable.engine.persistence_history` refuses history a retention policy pruned; setting both clocks at once is refused.
