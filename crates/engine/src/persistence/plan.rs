//! Engine-owned persistence commit plans.

use strata_core::{BranchId, CommitVersion};

use super::{PersistenceReadRow, RowAddress, RowMutation};
use crate::diagnostics::EngineError;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CommitPlan {
    branch_id: BranchId,
    mutations: Vec<RowMutation>,
    expected_generation: Option<u64>,
    conditions: Vec<CommitCondition>,
}

/// A row a commit's mutations were derived from, and the version it was
/// read at: the commit lands only if the row is still at that version, and
/// otherwise fails with `conflict.engine.persistence` without applying
/// (#3776). A read-modify-write commit carries the row it read, so two
/// writers that read the same row cannot both commit a value derived from
/// it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CommitCondition {
    address: RowAddress,
    read_version: Option<CommitVersion>,
}

impl CommitCondition {
    /// The row at `address` must be as `read` found it: present at its
    /// version, or absent (never written, or tombstoned) when `read` found
    /// no live row.
    pub(crate) fn unchanged_since(address: RowAddress, read: Option<&PersistenceReadRow>) -> Self {
        Self {
            address,
            read_version: read
                .filter(|row| !row.is_tombstone())
                .map(PersistenceReadRow::commit_version),
        }
    }

    pub(crate) const fn address(&self) -> &RowAddress {
        &self.address
    }

    /// The version the row must still be at, or `None` when it must still be
    /// absent.
    pub(crate) const fn read_version(&self) -> Option<CommitVersion> {
        self.read_version
    }
}

/// How many times a read-modify-write write runs before a lost race is
/// reported (#3776). Each attempt that conflicts lost to a commit that did
/// land, so contention always makes progress; the bound only stops a writer
/// that keeps losing from spinning forever. At 256 a writer racing 15 others
/// that each win as often as it does loses every attempt with odds under
/// 1e-7.
pub(crate) const COMMIT_CONFLICT_ATTEMPTS: usize = 256;

/// Runs `attempt`, a read-modify-write write whose commit carries the rows
/// it read as [`CommitCondition`]s, until it does not lose a race (#3776).
/// A `conflict.engine.persistence` refusal means another commit changed a
/// row the attempt read, and the refused commit applied nothing, so the
/// attempt reads again and retries, at most `attempts` times in all. Any
/// other outcome, success or error, is returned as it is. A conflict on the
/// last attempt is returned to the caller, still retryable.
pub(crate) fn retry_lost_commit_race<T>(
    attempts: usize,
    mut attempt: impl FnMut() -> Result<T, EngineError>,
) -> Result<T, EngineError> {
    let mut tried = 1;
    loop {
        match attempt() {
            Err(error) if error.code() == "conflict.engine.persistence" && tried < attempts => {
                tried += 1;
            }
            outcome => return outcome,
        }
    }
}

impl CommitPlan {
    pub(crate) const fn new(
        branch_id: BranchId,
        mutations: Vec<RowMutation>,
        expected_generation: Option<u64>,
    ) -> Self {
        Self {
            branch_id,
            mutations,
            expected_generation,
            conditions: Vec::new(),
        }
    }

    /// The plan, landing only if every row in `conditions` is still as it
    /// was read (#3776).
    pub(crate) fn with_conditions(mut self, conditions: Vec<CommitCondition>) -> Self {
        self.conditions = conditions;
        self
    }

    pub(crate) fn conditions(&self) -> &[CommitCondition] {
        &self.conditions
    }

    pub(crate) const fn branch_id(&self) -> BranchId {
        self.branch_id
    }

    pub(crate) fn mutations(&self) -> &[RowMutation] {
        &self.mutations
    }

    pub(crate) const fn expected_generation(&self) -> Option<u64> {
        self.expected_generation
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use strata_core::{BranchId, CommitVersion};

    use super::{retry_lost_commit_race, CommitCondition};
    use crate::diagnostics::EngineError;
    use crate::persistence::{PersistenceReadRow, RowAddress, RowClass};

    fn lost_race() -> EngineError {
        EngineError::conflict("conflict.engine.persistence", "lost a commit race")
    }

    /// Runs the retry with an attempt whose `n`th call (from 0) answers
    /// `outcome(n)`, and returns the result and how many attempts ran.
    fn run(
        attempts: usize,
        outcome: impl Fn(usize) -> Result<u8, EngineError>,
    ) -> (Result<u8, EngineError>, usize) {
        let calls = Cell::new(0);
        let result = retry_lost_commit_race(attempts, || {
            let call = calls.get();
            // A retry past its bound fails here at once rather than spinning.
            assert!(
                call < attempts,
                "attempt {call} is past the bound of {attempts}"
            );
            calls.set(call + 1);
            outcome(call)
        });
        (result, calls.get())
    }

    #[test]
    fn retry_lost_commit_race_retries_only_a_lost_race_and_only_up_to_its_bound() {
        // Success first time: one attempt.
        let (result, calls) = run(4, |_| Ok(7));
        assert_eq!(result.expect("success"), 7);
        assert_eq!(calls, 1);

        // A lost race, then success: the second attempt's answer.
        let (result, calls) = run(4, |call| if call == 0 { Err(lost_race()) } else { Ok(8) });
        assert_eq!(result.expect("the retry lands"), 8);
        assert_eq!(calls, 2);

        // Losing every time: exactly `attempts` attempts, then the conflict.
        let (result, calls) = run(4, |_| Err(lost_race()));
        assert_eq!(
            result.expect_err("the race is reported").code(),
            "conflict.engine.persistence"
        );
        assert_eq!(calls, 4);

        // One attempt allowed: no retry at all.
        let (result, calls) = run(1, |call| if call == 0 { Err(lost_race()) } else { Ok(9) });
        assert!(result.is_err());
        assert_eq!(calls, 1);

        // Any other error, even another conflict, is not a lost race.
        for (code, error) in [
            (
                "conflict.engine.branch_generation",
                (|| EngineError::conflict("conflict.engine.branch_generation", "replaced"))
                    as fn() -> EngineError,
            ),
            ("not_found.engine.branch", || {
                EngineError::not_found("not_found.engine.branch", "gone")
            }),
        ] {
            let (result, calls) = run(4, |call| if call == 0 { Err(error()) } else { Ok(10) });
            assert_eq!(result.expect_err("returned as it is").code(), code);
            assert_eq!(calls, 1, "{code} is not retried");
        }
    }

    #[test]
    fn a_conditioned_commit_lands_only_while_the_row_is_as_it_was_read() {
        use crate::branch::catalog::BranchCatalogRecord;
        use crate::branch::BranchName;
        use crate::persistence::{
            CommitPlan, PersistenceOpenTarget, ReadSelector, RowMutation, StoragePersistence,
        };

        let (mut persistence, _) =
            StoragePersistence::open(PersistenceOpenTarget::Cache).expect("cache opens");
        let record =
            BranchCatalogRecord::root(BranchName::new("conditions").expect("valid branch"), 1);
        persistence
            .create_branch(record.storage_branch_id(), record.generation())
            .expect("storage branch created");
        let row = |key: u8| RowAddress::new(record.storage_branch_id(), RowClass::Kv, vec![key]);
        let read = |key: u8| {
            persistence
                .read_row(row(key), ReadSelector::Latest)
                .expect("the row reads")
        };
        let commit = |key: u8, conditions: Vec<CommitCondition>| {
            persistence.commit(
                &CommitPlan::new(
                    record.storage_branch_id(),
                    vec![RowMutation::put(row(key), vec![key])],
                    Some(record.generation()),
                )
                .with_conditions(conditions),
            )
        };
        let refused = |outcome: Result<_, EngineError>| {
            assert_eq!(
                outcome.expect_err("the row moved since it was read").code(),
                "conflict.engine.persistence"
            );
        };

        // Absent as read, still absent: lands.
        let absent = CommitCondition::unchanged_since(row(1), read(1).as_ref());
        commit(1, vec![absent.clone()]).expect("an absent row still absent lands");
        // Absent as read, now present: refused, and nothing applied.
        refused(commit(1, vec![absent]));
        // Present as read, unchanged: lands.
        let first = read(1);
        let present = CommitCondition::unchanged_since(row(1), first.as_ref());
        commit(1, vec![present.clone()]).expect("an unchanged row lands");
        // Present as read, rewritten since: refused, and nothing applied.
        let before_refusal = read(1).expect("present").commit_version();
        refused(commit(1, vec![present]));
        assert_eq!(
            read(1).expect("present").commit_version(),
            before_refusal,
            "a refused commit applies nothing"
        );
        // Every condition must hold: one stale row refuses the commit.
        let fresh = CommitCondition::unchanged_since(row(1), read(1).as_ref());
        let stale_missing = CommitCondition::unchanged_since(row(2), first.as_ref());
        refused(commit(3, vec![fresh, stale_missing]));
        assert!(read(3).is_none(), "the refused commit wrote nothing");
    }

    #[test]
    fn a_condition_expects_a_live_row_at_its_version_and_otherwise_absence() {
        let address = || RowAddress::new(BranchId::from_bytes([7; 16]), RowClass::Kv, vec![1]);
        let live = PersistenceReadRow::for_test(vec![1], Some(vec![2]), false);
        let tombstone = PersistenceReadRow::for_test(vec![1], None, true);

        let present = CommitCondition::unchanged_since(address(), Some(&live));
        assert_eq!(present.read_version(), Some(CommitVersion::new(1)));
        assert_eq!(present.address(), &address());
        assert_eq!(
            CommitCondition::unchanged_since(address(), Some(&tombstone)).read_version(),
            None,
            "a tombstone reads as absent, as storage's conflict check sees it"
        );
        assert_eq!(
            CommitCondition::unchanged_since(address(), None).read_version(),
            None
        );
    }
}
