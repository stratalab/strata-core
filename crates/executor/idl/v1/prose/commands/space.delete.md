---
summary: Delete a product space from a branch.
mcp_description: Use this when the user wants to remove a space, optionally force-deleting its data.
---

Drops the product space from the branch catalog. The `default` space refuses deletion with `invalid_argument.engine.space_delete_default`. A space that still contains visible data refuses deletion with `failed_precondition.engine.space_not_empty` unless `force: true` is set, which tombstones the visible rows first and reports the count. Deleting a space that does not exist succeeds with `deleted: false`.

A space of any size deletes. One whose rows fit a single commit goes in that commit; a larger one is unregistered by its first commit — gone from `space list` and `space exists`, and a write of the name finishes the sweep and lands in a fresh, empty space rather than the one being deleted — and its rows are then swept in commits the storage budget admits, the catalog row last. A deletion interrupted mid-sweep is finished by the next `space delete` or `space create` of the name, or by the first write that would register it; the interrupted delete never leaves a half-registered space. Reads pinned before the deletion still see the space as it was.
