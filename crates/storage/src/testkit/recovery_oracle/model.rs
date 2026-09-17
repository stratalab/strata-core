//! Recovery-oracle expected-state model.
//!
//! Records every acknowledged (and in-doubt) commit of a durable workload and
//! reconstructs the expected live state at any commit-version watermark, so the
//! verifier can prove a recovered database is a *prefix of acknowledged history*.

use std::collections::BTreeMap;

use strata_core::{BranchId, CommitVersion};

use crate::api::{StorageKey, StorageSpaceId, StorageValue};

/// Durability policy the workload runtime was opened with. Determines how much
/// of an acknowledged history the verifier may tolerate losing (see `verify`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OracleDurability {
    Standard,
    Always,
}

/// One mutation, recorded exactly as submitted.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RecordedMutation {
    Put {
        space: StorageSpaceId,
        key: StorageKey,
        value: StorageValue,
    },
    Delete {
        space: StorageSpaceId,
        key: StorageKey,
    },
}

/// Whether a logged commit's `commit()` returned `Ok` (acknowledged) or was the
/// crash op / returned `Err` (in-doubt: it may or may not have become durable).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AckState {
    Acknowledged,
    #[allow(
        dead_code,
        reason = "constructed by the backend-fault crash mechanism; an in-doubt commit may or may not survive recovery"
    )]
    InDoubt,
}

#[derive(Clone, Debug)]
struct LoggedCommit {
    branch: BranchId,
    version: CommitVersion,
    state: AckState,
    mutations: Vec<RecordedMutation>,
}

/// `(space, key)` map key for reconstructed state.
pub(crate) type StateKey = (StorageSpaceId, StorageKey);
/// `(value, version)` — a `None` value marks a tombstone at that version.
type CellState = (Option<StorageValue>, CommitVersion);

/// Append-only log of acknowledged + in-doubt commits in submission order.
pub(crate) struct ExpectedState {
    #[allow(
        dead_code,
        reason = "retained for future per-mode suffix-tolerance policy; the crash family is currently passed to the verifier explicitly"
    )]
    durability: OracleDurability,
    log: Vec<LoggedCommit>,
}

impl ExpectedState {
    pub(crate) fn new(durability: OracleDurability) -> Self {
        Self {
            durability,
            log: Vec::new(),
        }
    }

    /// Record a commit whose `commit()` returned `Ok(summary)`.
    pub(crate) fn record_ack(
        &mut self,
        branch: BranchId,
        version: CommitVersion,
        mutations: Vec<RecordedMutation>,
    ) {
        self.log.push(LoggedCommit {
            branch,
            version,
            state: AckState::Acknowledged,
            mutations,
        });
    }

    /// Record the crash op (or an op whose `commit()` returned `Err`) as
    /// in-doubt. Assigns the would-be next version (last logged + 1) and returns
    /// it; recovery may legitimately reflect this op or not.
    #[allow(
        dead_code,
        reason = "exercised by unit tests; reserved for the backend-fault crash mechanism"
    )]
    pub(crate) fn record_in_doubt(
        &mut self,
        branch: BranchId,
        mutations: Vec<RecordedMutation>,
    ) -> CommitVersion {
        let version = self
            .log
            .iter()
            .map(|commit| commit.version)
            .max()
            .map_or(CommitVersion::new(1), |last| {
                CommitVersion::new(last.as_u64() + 1)
            });
        self.log.push(LoggedCommit {
            branch,
            version,
            state: AckState::InDoubt,
            mutations,
        });
        version
    }

    /// Greatest version of an *acknowledged* commit on `branch` — the floor the
    /// recovered watermark must reach under a zero-loss crash mechanism.
    pub(crate) fn last_acked_version(&self, branch: BranchId) -> Option<CommitVersion> {
        self.log
            .iter()
            .filter(|commit| commit.branch == branch && commit.state == AckState::Acknowledged)
            .map(|commit| commit.version)
            .max()
    }

    /// Greatest version of any logged commit on `branch` (acked or in-doubt).
    pub(crate) fn max_version(&self, branch: BranchId) -> Option<CommitVersion> {
        self.log
            .iter()
            .filter(|commit| commit.branch == branch)
            .map(|commit| commit.version)
            .max()
    }

    /// Distinct candidate watermarks for `branch` at or below `upper`, plus
    /// `ZERO` (empty state), in descending order — the verifier's search space.
    pub(crate) fn candidate_watermarks(
        &self,
        branch: BranchId,
        upper: CommitVersion,
    ) -> Vec<CommitVersion> {
        let mut versions: Vec<CommitVersion> = self
            .log
            .iter()
            .filter(|commit| commit.branch == branch && commit.version <= upper)
            .map(|commit| commit.version)
            .collect();
        versions.push(CommitVersion::ZERO);
        versions.sort_unstable();
        versions.dedup();
        versions.reverse();
        versions
    }

    /// The mutations of the commit at exactly `version` (for atomicity checks).
    pub(crate) fn mutations_at(
        &self,
        branch: BranchId,
        version: CommitVersion,
    ) -> Option<&[RecordedMutation]> {
        self.log
            .iter()
            .find(|commit| commit.branch == branch && commit.version == version)
            .map(|commit| commit.mutations.as_slice())
    }

    /// Whether the commit at `version` was acknowledged or in-doubt.
    pub(crate) fn ack_state_at(
        &self,
        branch: BranchId,
        version: CommitVersion,
    ) -> Option<AckState> {
        self.log
            .iter()
            .find(|commit| commit.branch == branch && commit.version == version)
            .map(|commit| commit.state)
    }

    /// Expected state at watermark `W`: fold all commits (acked + in-doubt) with
    /// `version <= W` in version order. A `None` value marks a tombstone.
    pub(crate) fn state_at(
        &self,
        branch: BranchId,
        watermark: CommitVersion,
    ) -> BTreeMap<StateKey, CellState> {
        let mut commits: Vec<&LoggedCommit> = self
            .log
            .iter()
            .filter(|commit| commit.branch == branch && commit.version <= watermark)
            .collect();
        commits.sort_by_key(|commit| commit.version);
        let mut state: BTreeMap<StateKey, CellState> = BTreeMap::new();
        for commit in commits {
            for mutation in &commit.mutations {
                match mutation {
                    RecordedMutation::Put { space, key, value } => {
                        state.insert(
                            (space.clone(), key.clone()),
                            (Some(value.clone()), commit.version),
                        );
                    }
                    RecordedMutation::Delete { space, key } => {
                        state.insert((space.clone(), key.clone()), (None, commit.version));
                    }
                }
            }
        }
        state
    }

    /// TCP4.11b: drop every logged commit for `branch` — the model side of a
    /// delete-then-recreate cycle (the recreated branch starts with empty
    /// history; keeping the old log would poison the prefix search).
    // Called only by the whole-db simulation, which additionally requires
    // `fault-injection`; under `testkit` alone the oracle model is compiled
    // for the external harness and this method is not reached.
    #[cfg(all(any(test, feature = "fault-injection"), feature = "localfs"))]
    pub(crate) fn forget_branch(&mut self, branch: BranchId) {
        self.log.retain(|commit| commit.branch != branch);
    }

    /// TCP4.11b: drop `branch`'s logged commits above `watermark` — adopting a
    /// lossy crash's surviving prefix so the next epoch's oracle starts from
    /// what actually recovered.
    pub(crate) fn truncate_branch_above(&mut self, branch: BranchId, watermark: CommitVersion) {
        self.log
            .retain(|commit| commit.branch != branch || commit.version <= watermark);
    }

    /// Live state at `W`: `state_at` with tombstones dropped — exactly what a
    /// `ReadBound::Latest` full scan of the recovered database returns.
    pub(crate) fn live_state_at(
        &self,
        branch: BranchId,
        watermark: CommitVersion,
    ) -> BTreeMap<StateKey, (StorageValue, CommitVersion)> {
        self.state_at(branch, watermark)
            .into_iter()
            .filter_map(|(state_key, (value, version))| {
                value.map(|value| (state_key, (value, version)))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::{ExpectedState, OracleDurability, RecordedMutation};
    use crate::api::{StorageKey, StorageSpaceId, StorageValue};
    use strata_core::{BranchId, CommitVersion};

    fn space() -> StorageSpaceId {
        StorageSpaceId::new(vec![0x20]).expect("space")
    }

    fn key(byte: u8) -> StorageKey {
        StorageKey::new(vec![0x6b, byte]).expect("key")
    }

    fn value(byte: u8) -> StorageValue {
        StorageValue::new(vec![byte])
    }

    fn branch() -> BranchId {
        BranchId::from_bytes([1; BranchId::BYTE_LEN])
    }

    fn put(k: u8, v: u8) -> RecordedMutation {
        RecordedMutation::Put {
            space: space(),
            key: key(k),
            value: value(v),
        }
    }

    fn del(k: u8) -> RecordedMutation {
        RecordedMutation::Delete {
            space: space(),
            key: key(k),
        }
    }

    fn live_value(
        state: &std::collections::BTreeMap<
            (StorageSpaceId, StorageKey),
            (StorageValue, CommitVersion),
        >,
        k: u8,
    ) -> Option<Vec<u8>> {
        state
            .get(&(space(), key(k)))
            .map(|(value, _)| value.as_bytes().to_vec())
    }

    #[test]
    fn state_at_folds_latest_version_per_key() {
        let mut model = ExpectedState::new(OracleDurability::Standard);
        model.record_ack(branch(), CommitVersion::new(1), vec![put(0, 10)]);
        model.record_ack(
            branch(),
            CommitVersion::new(2),
            vec![put(0, 20), put(1, 11)],
        );
        let live = model.live_state_at(branch(), CommitVersion::new(2));
        assert_eq!(live_value(&live, 0), Some(vec![20]));
        assert_eq!(live_value(&live, 1), Some(vec![11]));
    }

    #[test]
    fn state_at_respects_watermark() {
        let mut model = ExpectedState::new(OracleDurability::Standard);
        model.record_ack(branch(), CommitVersion::new(1), vec![put(0, 10)]);
        model.record_ack(branch(), CommitVersion::new(2), vec![put(0, 20)]);
        let live = model.live_state_at(branch(), CommitVersion::new(1));
        assert_eq!(live_value(&live, 0), Some(vec![10]));
    }

    #[test]
    fn delete_tombstones_drop_from_live_state_and_can_resurrect() {
        let mut model = ExpectedState::new(OracleDurability::Standard);
        model.record_ack(branch(), CommitVersion::new(1), vec![put(0, 10)]);
        model.record_ack(branch(), CommitVersion::new(2), vec![del(0)]);
        assert!(model
            .live_state_at(branch(), CommitVersion::new(2))
            .is_empty());
        assert_eq!(
            model
                .state_at(branch(), CommitVersion::new(2))
                .get(&(space(), key(0)))
                .map(|(value, _)| value.clone()),
            Some(None),
        );
        model.record_ack(branch(), CommitVersion::new(3), vec![put(0, 30)]);
        assert_eq!(
            live_value(&model.live_state_at(branch(), CommitVersion::new(3)), 0),
            Some(vec![30]),
        );
    }

    #[test]
    fn in_doubt_excluded_from_last_acked_but_present_at_its_version() {
        let mut model = ExpectedState::new(OracleDurability::Always);
        model.record_ack(branch(), CommitVersion::new(1), vec![put(0, 10)]);
        let version = model.record_in_doubt(branch(), vec![put(1, 11)]);
        assert_eq!(version, CommitVersion::new(2));
        assert_eq!(
            model.last_acked_version(branch()),
            Some(CommitVersion::new(1))
        );
        assert!(!model
            .live_state_at(branch(), CommitVersion::new(1))
            .contains_key(&(space(), key(1))));
        assert!(model
            .live_state_at(branch(), version)
            .contains_key(&(space(), key(1))));
    }

    #[test]
    fn reconstruction_is_deterministic() {
        let build = || {
            let mut model = ExpectedState::new(OracleDurability::Standard);
            model.record_ack(
                branch(),
                CommitVersion::new(1),
                vec![put(0, 10), put(1, 11)],
            );
            model.record_ack(branch(), CommitVersion::new(2), vec![del(0), put(2, 12)]);
            model.live_state_at(branch(), CommitVersion::new(2))
        };
        assert_eq!(build(), build());
    }

    #[test]
    fn candidate_watermarks_are_descending_with_zero_floor() {
        let mut model = ExpectedState::new(OracleDurability::Standard);
        model.record_ack(branch(), CommitVersion::new(1), vec![put(0, 10)]);
        model.record_ack(branch(), CommitVersion::new(3), vec![put(0, 20)]);
        assert_eq!(
            model.candidate_watermarks(branch(), CommitVersion::new(3)),
            vec![
                CommitVersion::new(3),
                CommitVersion::new(1),
                CommitVersion::ZERO
            ],
        );
    }
}
