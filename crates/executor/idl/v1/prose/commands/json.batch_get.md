---
summary: Read multiple JSON values by document and path.
mcp_description: Use this when the user wants to fetch several JSON documents or fields in one request.
---

Reads several document/path entries and returns positional item results. Each item records whether the value was found and includes version metadata when present.

With `as_of` (a position on the logical commit timeline) or `as_of_time` (a wall-clock instant), every entry is read as of that one position, so a batch can hydrate the documents a version-pinned read named — a graph query at a version, say — without mixing in later state. Entries keep their order, a repeated entry is answered again, and a document absent at that position is a miss. An instant outside the branch's recorded history fails the whole batch with `history_unavailable.engine.persistence_history` rather than answering from the latest state; setting both clocks at once is refused.
