---
summary: Read a range of events by sequence number.
mcp_description: Use this when the user wants events between two sequence numbers, optionally filtered by event type, in forward or reverse order.
---

Reads events from the selected branch and space by sequence range. The start sequence is inclusive and the optional end sequence is exclusive; reverse direction returns the same `[start_seq, end_seq)` window in descending order (newest first). An optional event type narrows the results.

This command reads the latest log and takes no `as_of`. To read the log as of an earlier commit, page through `event.list` with `as_of` (or `as_of_time`) and its `after_sequence` cursor; it reads oldest first, and there is no newest-first read as of a commit. Do not bound this command's `end_seq` by an as-of `event.count`: force-deleting a space restarts its sequence numbers, so after that the same numbers name events of the new log.
