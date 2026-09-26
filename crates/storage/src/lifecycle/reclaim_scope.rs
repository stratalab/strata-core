//! Space-reclamation contract §3.1 (slice 4, #3597): the open-time
//! reclaim-only scope.
//!
//! Reopening an existing database enqueues the prior session's reclaim
//! backlog (the table-object mark that chains the quarantine sweep and the
//! purge), and `open` arms ONE low-tier wake so that backlog runs even in a
//! session that never commits. Until the session's first commit is applied the
//! runtime is in the reclaim-only scope: a background drain admits the reclaim
//! tier and refuses the upper tier (flush, checkpoint, flush-watermark, WAL
//! truncation, table rewrite), which has nothing a read-only session should
//! start and which DUR-018 keeps out of the open path. The first applied commit
//! ends the scope; explicit foreground drains are the caller's own intent and
//! are never gated.

use super::StorageOpenDisposition;

/// Whether the session has written yet. `Active` from an `OpenedExisting`
/// open until the first commit applies; a freshly created database starts
/// `Inactive` (it has no backlog and no wake).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReclaimOnlyScope {
    Active,
    Inactive,
}

/// The scope a durable runtime starts in, from its open disposition.
pub(crate) const fn reclaim_only_scope_after_open(
    disposition: StorageOpenDisposition,
) -> ReclaimOnlyScope {
    match disposition {
        StorageOpenDisposition::OpenedExisting => ReclaimOnlyScope::Active,
        StorageOpenDisposition::Created => ReclaimOnlyScope::Inactive,
    }
}

/// Whether a background drain round may start upper-tier work (flush,
/// checkpoint, flush-watermark, WAL truncation, table rewrite).
pub(crate) const fn drain_scope_admits_upper_tier(scope: ReclaimOnlyScope) -> bool {
    match scope {
        ReclaimOnlyScope::Active => false,
        ReclaimOnlyScope::Inactive => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reclaim_only_scope_after_open_truth_table() {
        assert_eq!(
            reclaim_only_scope_after_open(StorageOpenDisposition::OpenedExisting),
            ReclaimOnlyScope::Active
        );
        assert_eq!(
            reclaim_only_scope_after_open(StorageOpenDisposition::Created),
            ReclaimOnlyScope::Inactive
        );
    }

    #[test]
    fn drain_scope_admits_upper_tier_truth_table() {
        assert!(!drain_scope_admits_upper_tier(ReclaimOnlyScope::Active));
        assert!(drain_scope_admits_upper_tier(ReclaimOnlyScope::Inactive));
    }
}
