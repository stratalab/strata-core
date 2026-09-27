//! Write-group join queue (BS5.1).
//!
//! Every durable commit joins the queue. The first joiner while no leader is
//! active becomes the leader: it takes the runtime lock, drains up to
//! [`COMMIT_GROUP_MAX_MEMBERS`] queued requests, and executes them as one
//! write group (one durable-gate span, one covering fsync, one visible
//! publish). Waiting members park on their own joiner's condvar — never on
//! the runtime lock — so the moment the leader publishes their responses they
//! wake, return, and re-join, keeping the next group full. On finish the
//! leader promotes the queue's front joiner as the next leader (or clears the
//! leadership flag when the queue is empty); a timed-wait fallback lets a
//! waiter assume leadership if a promotion was lost, so liveness never
//! depends on a single wake-up.
//!
//! Single-threaded callers (and wasm, per constraint C1) never wait: joining
//! an empty queue with no active leader returns leadership immediately — the
//! exact solo commit path with one uncontended queue-mutex hop.
//!
//! The coordination protocol (queue, joiner state machine, apply exchange,
//! sync chain) is generic over [`GroupProtocol`] and imports its primitives
//! from the [`crate::sync`] seam: the payload types are opaque to every
//! schedule decision, which lets the TCP4.3 loom lane
//! (`commit_group_loom.rs`) explore the REAL protocol code exhaustively with
//! lightweight payloads. Production instantiates it once as the
//! `CommitGroup*` aliases below.

use crate::commit::{CommitBatch, CommitBranchGenerationGuard, CommitOutcome};
use crate::lifecycle::{LifecycleError, LifecycleWalGrowthOutcome, LifecycleWriteAdmissionOutcome};
use crate::sync::{Arc, Condvar, Mutex};
use std::collections::VecDeque;
use std::fmt::Debug;
use std::time::Duration;

/// Maximum members per write group, the leader included (plan D3).
pub(super) const COMMIT_GROUP_MAX_MEMBERS: usize = 16;

/// Fallback poll interval for waiters (lost-promotion safety net only; the
/// normal path is a direct notify).
const WAITER_FALLBACK_POLL: Duration = Duration::from_millis(100);

/// The payload types a group protocol coordinates. Purely marker-level: the
/// queue, joiner, and exchange never inspect payloads — [`Self::apply`] is
/// the single behavioral hook (a member executing its branch's deferred
/// apply). Debug bounds keep the carrying enums debuggable.
pub(super) trait GroupProtocol {
    type Request: Debug;
    type Response: Debug;
    type Work: Debug;
    type Done: Debug;

    /// Execute one unit of handed-off apply work (BS5.4c member side).
    fn apply(work: Self::Work) -> Self::Done;
}

/// The production protocol: durable commit requests/responses with the
/// lifecycle layer's deferred branch applies.
#[derive(Debug)]
pub(super) struct DurableCommitGroupProtocol;

impl GroupProtocol for DurableCommitGroupProtocol {
    type Request = CommitGroupRequest;
    type Response = CommitGroupResponse;
    type Work = crate::lifecycle::DurableGroupApplyWork;
    type Done = crate::lifecycle::DurableGroupApplyDone;

    fn apply(work: Self::Work) -> Self::Done {
        work.apply()
    }
}

pub(super) type CommitGroupQueue = GroupQueue<DurableCommitGroupProtocol>;
pub(super) type CommitGroupJoinerHandle = JoinerHandle<DurableCommitGroupProtocol>;
pub(super) type CommitGroupLeadership<'q> = GroupLeadership<'q, DurableCommitGroupProtocol>;
pub(super) type CommitGroupExchangeHandle = ExchangeHandle<DurableCommitGroupProtocol>;
pub(super) type JoinPath = GroupJoinPath<DurableCommitGroupProtocol>;
pub(super) type WaitOutcome = GroupWaitOutcome<DurableCommitGroupProtocol>;

#[derive(Debug)]
pub(super) struct CommitGroupRequest {
    pub(super) batch: CommitBatch,
    pub(super) generation_guard: CommitBranchGenerationGuard,
}

/// Everything a caller needs after the under-lock section: the outcome plus
/// the same per-caller snapshots the solo path reads under the lock.
#[derive(Debug)]
pub(super) struct CommitGroupResponse {
    pub(super) outcome: Result<CommitOutcome, LifecycleError>,
    pub(super) admission: Option<LifecycleWriteAdmissionOutcome>,
    pub(super) pending_tasks: usize,
    /// Space-reclamation contract §3.1 (slice 8): a table-object sweep is owed
    /// and this quiet period's idle wake is unspent — the commit is the
    /// activity edge that lets a drain round arm the wake.
    pub(super) idle_wake_pending: bool,
    pub(super) wal_growth: Option<LifecycleWalGrowthOutcome>,
    pub(super) throttle_delay_millis: u64,
}

#[derive(Debug)]
enum JoinState<P: GroupProtocol> {
    /// Queued, waiting for a leader (request still inside).
    Pending(P::Request),
    /// A leader took the request and is executing it.
    Taken,
    /// Promoted to lead: the waiter takes its request back and leads.
    Promoted(P::Request),
    /// The leader handed this (taken) member its branch's deferred group
    /// apply (BS5.4c); the waiter runs it lock-free and resumes waiting.
    Apply(Box<GroupApplyHandoff<P>>),
    // Boxed: the response dwarfs the other variants, and each joiner
    // completes at most once.
    Done(Box<P::Response>),
}

#[derive(Debug)]
pub(super) struct GroupJoiner<P: GroupProtocol> {
    state: Mutex<JoinState<P>>,
    ready: Condvar,
}

impl<P: GroupProtocol> GroupJoiner<P> {
    fn new(request: P::Request) -> Self {
        Self {
            state: Mutex::new(JoinState::Pending(request)),
            ready: Condvar::new(),
        }
    }

    /// Leader-side: take the request for execution (`Pending` → `Taken`).
    /// Returns `None` for stale nodes (already taken, promoted, or done).
    fn take_request(&self) -> Option<P::Request> {
        // Rationale: a poisoned joiner mutex means the owning caller panicked;
        // skipping the node fails closed (its caller already returned).
        let Ok(mut state) = self.state.lock() else {
            return None;
        };
        match &*state {
            JoinState::Pending(_) => match std::mem::replace(&mut *state, JoinState::Taken) {
                JoinState::Pending(request) => Some(request),
                // Unreachable: the match arm above proved the state Pending
                // while the lock is held.
                _ => None,
            },
            _ => None,
        }
    }

    /// Leader-side: publish the member's response and wake its caller.
    fn complete(&self, response: P::Response) {
        if let Ok(mut state) = self.state.lock() {
            *state = JoinState::Done(Box::new(response));
        }
        // Rationale: a poisoned joiner mutex means the owning caller panicked
        // and will never read the response; dropping it is the only option.
        self.ready.notify_one();
    }

    /// Promotion: hand leadership to this (still pending) joiner. Returns
    /// false for stale nodes.
    fn promote(&self) -> bool {
        let Ok(mut state) = self.state.lock() else {
            return false;
        };
        if !matches!(&*state, JoinState::Pending(_)) {
            return false;
        }
        let JoinState::Pending(request) = std::mem::replace(&mut *state, JoinState::Taken) else {
            // Unreachable: checked Pending under the same lock hold.
            return false;
        };
        *state = JoinState::Promoted(request);
        drop(state);
        self.ready.notify_one();
        true
    }

    /// Leader-side (BS5.4c): hand this (taken, parked) member its branch's
    /// deferred apply. Returns the work back if the member is not in the
    /// taken state (the leader then applies it itself).
    fn request_apply(&self, work: P::Work, exchange: &ExchangeHandle<P>) -> Result<(), P::Work> {
        let Ok(mut state) = self.state.lock() else {
            return Err(work);
        };
        if !matches!(&*state, JoinState::Taken) {
            return Err(work);
        }
        *state = JoinState::Apply(Box::new(GroupApplyHandoff {
            work,
            exchange: Arc::clone(&exchange.0),
        }));
        drop(state);
        self.ready.notify_one();
        Ok(())
    }
}

/// Shared handle to one queued joiner. Callers outside this module never
/// name the inner `Arc` — the seam's `Arc` differs under loom.
#[derive(Debug)]
pub(super) struct JoinerHandle<P: GroupProtocol>(Arc<GroupJoiner<P>>);

impl<P: GroupProtocol> Clone for JoinerHandle<P> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<P: GroupProtocol> JoinerHandle<P> {
    /// Leader-side: publish the member's response and wake its caller.
    pub(super) fn complete(&self, response: P::Response) {
        self.0.complete(response);
    }

    /// Leader-side (BS5.4c): hand this member its deferred apply through
    /// `exchange`; on `Err` the member cannot take it and the leader
    /// applies the returned work itself.
    pub(super) fn request_apply(
        &self,
        work: P::Work,
        exchange: &ExchangeHandle<P>,
    ) -> Result<(), P::Work> {
        self.0.request_apply(work, exchange)
    }
}

/// Where a joining commit goes: lead now, or wait to be served.
#[derive(Debug)]
pub(super) enum GroupJoinPath<P: GroupProtocol> {
    /// No leader was active: the caller leads immediately (its request never
    /// enters the queue). The guard hands leadership on when dropped.
    Lead(P::Request),
    /// A leader is active: wait on the joiner for a response or a promotion.
    Wait(JoinerHandle<P>),
}

/// Terminal state a waiting caller observes.
#[derive(Debug)]
pub(super) enum GroupWaitOutcome<P: GroupProtocol> {
    /// A leader executed this request; the response is ready.
    Done(Box<P::Response>),
    /// Promoted (or self-promoted after a lost wake-up): lead with the
    /// returned request.
    Lead(P::Request),
    /// The request was taken but never completed. Leaders complete every
    /// taken member before handing off, so this is only reachable if a leader
    /// died mid-group — fail closed, never re-execute.
    Abandoned,
}

#[derive(Debug)]
struct QueueState<P: GroupProtocol> {
    queue: VecDeque<Arc<GroupJoiner<P>>>,
    /// True while some caller owns leadership (executing or promoted). At most
    /// one leader exists at a time; it drains the queue the members wait in.
    leader_active: bool,
}

impl<P: GroupProtocol> Default for QueueState<P> {
    fn default() -> Self {
        Self {
            queue: VecDeque::new(),
            leader_active: false,
        }
    }
}

#[derive(Debug)]
pub(super) struct GroupQueue<P: GroupProtocol> {
    state: Mutex<QueueState<P>>,
}

impl<P: GroupProtocol> Default for GroupQueue<P> {
    fn default() -> Self {
        Self {
            state: Mutex::new(QueueState::default()),
        }
    }
}

impl<P: GroupProtocol> GroupQueue<P> {
    /// Join the write-group protocol: become the leader if none is active,
    /// otherwise enqueue and wait.
    pub(super) fn join(&self, request: P::Request) -> GroupJoinPath<P> {
        // Rationale: queue-mutex poisoning requires a panic inside the tiny
        // sections of this module; leading solo is the fail-closed fallback
        // (the commit still executes with full solo semantics).
        let Ok(mut state) = self.state.lock() else {
            return GroupJoinPath::Lead(request);
        };
        if !state.leader_active {
            state.leader_active = true;
            return GroupJoinPath::Lead(request);
        }
        let joiner = Arc::new(GroupJoiner::new(request));
        state.queue.push_back(Arc::clone(&joiner));
        GroupJoinPath::Wait(JoinerHandle(joiner))
    }

    /// Leader-side: drain up to `max` pending joiners with their requests, in
    /// FIFO order. Stale nodes are discarded without counting.
    pub(super) fn drain_members(&self, max: usize) -> Vec<(JoinerHandle<P>, P::Request)> {
        let mut members = Vec::new();
        let Ok(mut state) = self.state.lock() else {
            return members;
        };
        while members.len() < max {
            let Some(joiner) = state.queue.pop_front() else {
                break;
            };
            if let Some(request) = joiner.take_request() {
                members.push((JoinerHandle(joiner), request));
            }
        }
        members
    }

    /// Leader-side: hand leadership on. Promotes the queue's front pending
    /// joiner as the next leader, or clears the leadership flag when nothing
    /// is queued. Every leader MUST call this exactly once (the API layer
    /// wraps it in a drop guard so a panicking leader still hands off).
    pub(super) fn finish_leadership(&self) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        while let Some(joiner) = state.queue.pop_front() {
            if joiner.promote() {
                // Leadership transfers: the flag stays set for the promotee.
                return;
            }
        }
        state.leader_active = false;
    }

    /// Waiter-side fallback: claim leadership if none is active (covers a
    /// lost promotion). On success the waiter must lead with its own request.
    fn try_assume_leadership(&self) -> bool {
        let Ok(mut state) = self.state.lock() else {
            return false;
        };
        if state.leader_active {
            return false;
        }
        state.leader_active = true;
        true
    }

    /// Waiter-side: block until served or promoted. Uses a timed wait purely
    /// as a lost-wake-up safety net; the normal path is a direct notify from
    /// the leader.
    pub(super) fn await_service(&self, handle: &JoinerHandle<P>) -> GroupWaitOutcome<P> {
        let joiner = &*handle.0;
        let Ok(mut state) = joiner.state.lock() else {
            return GroupWaitOutcome::Abandoned;
        };
        loop {
            match &*state {
                // Pending: no leader has reached us yet. Taken: a leader is
                // executing our request right now (possibly a long fsync) —
                // keep waiting for its completion in both cases.
                JoinState::Pending(_) | JoinState::Taken => {}
                JoinState::Apply(_) => {
                    match std::mem::replace(&mut *state, JoinState::Taken) {
                        JoinState::Apply(handoff) => {
                            // Run the apply OFF the joiner mutex (the work is
                            // owned; the leader may complete us meanwhile).
                            drop(state);
                            let GroupApplyHandoff { work, exchange } = *handoff;
                            exchange.submit(P::apply(work));
                            let Ok(again) = joiner.state.lock() else {
                                return GroupWaitOutcome::Abandoned;
                            };
                            state = again;
                            // Re-match immediately: the response may already
                            // be in.
                            continue;
                        }
                        // Unreachable: matched Apply under this hold.
                        _ => return GroupWaitOutcome::Abandoned,
                    }
                }
                JoinState::Promoted(_) | JoinState::Done(_) => {
                    match std::mem::replace(&mut *state, JoinState::Taken) {
                        JoinState::Promoted(request) => return GroupWaitOutcome::Lead(request),
                        JoinState::Done(response) => return GroupWaitOutcome::Done(response),
                        // Unreachable: matched Promoted/Done under this hold.
                        _ => return GroupWaitOutcome::Abandoned,
                    }
                }
            }
            let Ok((next, timeout)) = joiner.ready.wait_timeout(state, WAITER_FALLBACK_POLL) else {
                return GroupWaitOutcome::Abandoned;
            };
            state = next;
            if timeout.timed_out() && matches!(&*state, JoinState::Pending(_)) {
                // Nobody woke us within the fallback window. If no leader is
                // active (a promotion was lost), take over with our own
                // request; otherwise keep waiting for the active leader.
                drop(state);
                if self.try_assume_leadership() {
                    let Ok(mut reclaimed) = joiner.state.lock() else {
                        // Release the just-claimed leadership before failing
                        // closed, or no later joiner could ever lead.
                        self.finish_leadership();
                        return GroupWaitOutcome::Abandoned;
                    };
                    let taken = std::mem::replace(&mut *reclaimed, JoinState::Taken);
                    drop(reclaimed);
                    let (outcome, keeps_leadership) = reclaimed_outcome::<P>(taken);
                    if !keeps_leadership {
                        self.finish_leadership();
                    }
                    return outcome;
                }
                let Ok(again) = joiner.state.lock() else {
                    return GroupWaitOutcome::Abandoned;
                };
                state = again;
            }
        }
    }
}

/// Resolve the joiner state a self-promoted waiter reclaimed (the
/// lost-promotion fallback): what the caller returns, and whether it keeps
/// the leadership it just claimed. Pure so the decision table is directly
/// testable: still `Pending` → lead with the own request; raced to `Done`
/// (a leader served us in the window between the timeout check and the
/// claim) → hand leadership back and return the response; anything else →
/// hand leadership back and fail closed, never re-execute.
fn reclaimed_outcome<P: GroupProtocol>(taken: JoinState<P>) -> (GroupWaitOutcome<P>, bool) {
    match taken {
        JoinState::Pending(request) => (GroupWaitOutcome::Lead(request), true),
        JoinState::Done(response) => (GroupWaitOutcome::Done(response), false),
        _ => (GroupWaitOutcome::Abandoned, false),
    }
}

/// A ticket whose fsync the covering chain can run or prove covered. The
/// production impl is [`crate::service::WalGroupSyncTicket`]; the loom lane
/// substitutes instrumented tickets.
pub(super) trait SyncTicket {
    type Error;
    fn captured_seq(&self) -> u64;
    fn sync(&self) -> Result<(), Self::Error>;
}

impl SyncTicket for crate::service::WalGroupSyncTicket<'_> {
    type Error = crate::service::WalServiceError;

    fn captured_seq(&self) -> u64 {
        Self::captured_seq(self)
    }

    fn sync(&self) -> Result<(), Self::Error> {
        Self::sync(self)
    }
}

/// The covering-fsync chain (BS5.2): at most ONE sync runs at a time (the
/// device flush is the serial resource — overlapping fsyncs on one file just
/// queue at the device), and every completed sync covers all appends that
/// preceded its ticket's capture. A pipelined group therefore either proves
/// its appends already covered (the durable watermark passed its capture), or
/// queues at the token — the holder's release IS the completion wake-up —
/// and on acquiring either finds itself covered by the finished sync or runs
/// its own fresh-capture sync.
///
/// The token queue replaced a completion-counter condvar (#2815): loom found
/// two distinct schedules where a waiter anchored its counter observation
/// after the holder's final tick and parked on a wake-up that could never
/// come — the 20ms poll was load-bearing, not a safety net. Waiting on the
/// token itself makes a stranded waiter structurally impossible: mutex
/// release always wakes the queue, and a covered acquirer returns after two
/// loads.
#[derive(Debug, Default)]
pub(super) struct WalSyncChain {
    token: Mutex<()>,
}

impl WalSyncChain {
    /// Resolve a group's covering durability WITHOUT the runtime lock.
    /// Returns `None` when the group's appends were proven covered by an
    /// already-completed sync (nothing to redeem), otherwise `Some(result)`
    /// of the sync this caller ran itself. The token holder syncs a FRESH
    /// capture from `refresh` (one brief runtime-lock hold) rather than its
    /// own phase-1 ticket, so one fsync covers every group that appended
    /// while the previous sync ran — otherwise each group's early capture
    /// would cover only itself and the syncs would serialize unbatched.
    pub(super) fn sync_or_wait_covered<T, U>(
        &self,
        durable_seq: &std::sync::atomic::AtomicU64,
        ticket: &T,
        batching_beat: bool,
        refresh: impl Fn() -> Result<U, T::Error>,
    ) -> Option<Result<(), T::Error>>
    where
        T: SyncTicket,
        U: SyncTicket<Error = T::Error>,
    {
        use std::sync::atomic::Ordering;
        if durable_seq.load(Ordering::Acquire) >= ticket.captured_seq() {
            return None;
        }
        // Queue at the token. Rationale: a poisoned chain mutex means
        // another caller panicked mid-sync; degrade to syncing directly —
        // correctness is unchanged (extra fsyncs, never fewer).
        let Ok(_token) = self.token.lock() else {
            return Some(ticket.sync());
        };
        // Re-check after acquiring: the sync we queued behind (or any
        // earlier one) may have covered us.
        if durable_seq.load(Ordering::Acquire) >= ticket.captured_seq() {
            return None;
        }
        if batching_beat {
            // Other commits are mid-pipeline: the cohort served by the
            // previous sync is re-appending RIGHT NOW, ~3% of a device
            // flush away. One beat before capturing folds them into this
            // sync instead of the one after — without it the cohorts
            // alternate and every sync covers half the writers. Never taken
            // solo.
            crate::sync::beat_pause(Duration::from_micros(250));
        }
        // W3.3a: the re-capture flushes the append buffer; a flush failure
        // is the sync chain's failure (nothing was covered) and flows to
        // phase 2 like a failed fsync.
        let result = match refresh() {
            Ok(fresh) => {
                let result = fresh.sync();
                if result.is_ok() {
                    // Publish coverage immediately (the fsync's proof is
                    // lock-independent); phase 2 re-asserts it under the
                    // runtime lock via `complete_group_sync`.
                    durable_seq.fetch_max(fresh.captured_seq(), Ordering::AcqRel);
                }
                result
            }
            Err(error) => Err(error),
        };
        Some(result)
    }
}

/// Drop guard binding a leadership span to a scope: created when a caller
/// becomes leader, hands leadership on even if the leader's execution panics.
#[derive(Debug)]
pub(super) struct GroupLeadership<'q, P: GroupProtocol> {
    queue: &'q GroupQueue<P>,
}

impl<'q, P: GroupProtocol> GroupLeadership<'q, P> {
    pub(super) const fn new(queue: &'q GroupQueue<P>) -> Self {
        Self { queue }
    }
}

impl<P: GroupProtocol> Drop for GroupLeadership<'_, P> {
    fn drop(&mut self) {
        self.queue.finish_leadership();
    }
}

/// The leader→member handoff of one branch's deferred group apply (BS5.4c):
/// the work is fully owned (checked-out state, no locks), the exchange
/// collects the outcome back for the leader's barrier.
#[derive(Debug)]
pub(super) struct GroupApplyHandoff<P: GroupProtocol> {
    work: P::Work,
    exchange: Arc<GroupApplyExchange<P>>,
}

/// Barrier collecting parallel group-apply outcomes (BS5.4c): appliers submit
/// as they finish; the leader waits for the expected count. The timed wait is
/// a lost-wake safety net only.
#[derive(Debug)]
struct GroupApplyExchange<P: GroupProtocol> {
    returns: Mutex<Vec<P::Done>>,
    done: Condvar,
}

impl<P: GroupProtocol> Default for GroupApplyExchange<P> {
    fn default() -> Self {
        Self {
            returns: Mutex::new(Vec::new()),
            done: Condvar::new(),
        }
    }
}

impl<P: GroupProtocol> GroupApplyExchange<P> {
    fn submit(&self, outcome: P::Done) {
        if let Ok(mut returns) = self.returns.lock() {
            returns.push(outcome);
        }
        // Rationale: a poisoned exchange mutex means the leader panicked; the
        // outcome (and its checked-out state) is unrecoverable either way.
        self.done.notify_all();
    }

    /// Wait until `expected` outcomes arrived and drain them. A member thread
    /// dying mid-apply is panic-class (process-fatal by policy); the bounded
    /// deadline turns it into a fail-closed short count instead of a hang —
    /// the leader treats missing outcomes as group-fatal and their branches
    /// stay checked out (every access fails closed until reopen).
    fn wait_for(&self, expected: usize) -> Vec<P::Done> {
        if expected == 0 {
            // Nothing was dispatched (C1 wasm always lands here: groups of 1
            // route the sole apply to the leader) — never touch the clock or
            // the condvar.
            return Vec::new();
        }
        let deadline = crate::sync::Deadline::after(Duration::from_secs(5));
        let Ok(mut returns) = self.returns.lock() else {
            return Vec::new();
        };
        while returns.len() < expected && !deadline.expired() {
            let Ok((next, _timeout)) = self.done.wait_timeout(returns, Duration::from_millis(20))
            else {
                return Vec::new();
            };
            returns = next;
        }
        std::mem::take(&mut *returns)
    }
}

/// Sharable handle to one group's apply exchange: mints handoffs and
/// collects the barrier. Callers never name the inner `Arc`.
#[derive(Debug)]
pub(super) struct ExchangeHandle<P: GroupProtocol>(Arc<GroupApplyExchange<P>>);

impl<P: GroupProtocol> Default for ExchangeHandle<P> {
    fn default() -> Self {
        Self(Arc::new(GroupApplyExchange::default()))
    }
}

impl<P: GroupProtocol> ExchangeHandle<P> {
    /// Leader-side barrier: wait for `expected` submitted outcomes.
    pub(super) fn wait_for(&self, expected: usize) -> Vec<P::Done> {
        self.0.wait_for(expected)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::{
        CommitBatch, CommitBatchOptions, CommitConflictValidationMode, CommitDuplicateKeyPolicy,
        CommitDurabilityMode, CommitExpiry, CommitMutation, CommitOrigin, CommitRetentionHint,
        CommitTimestampPolicy, CommitValidationFacts,
    };
    use crate::row::{PhysicalKey, StorageSpaceId};
    use strata_core::BranchId;

    fn request(tag: u8) -> CommitGroupRequest {
        let branch = BranchId::from_bytes([tag; 16]);
        CommitGroupRequest {
            batch: CommitBatch::mutating(
                branch,
                vec![CommitMutation::put(
                    PhysicalKey::new(
                        branch,
                        "join-queue",
                        StorageSpaceId::engine(0x37).expect("engine storage space"),
                        vec![tag],
                    )
                    .expect("physical key"),
                    vec![tag],
                    CommitExpiry::None,
                    CommitRetentionHint::Append,
                )],
                CommitValidationFacts::empty(),
                CommitBatchOptions::new(
                    CommitDurabilityMode::Standard,
                    CommitConflictValidationMode::Validate,
                    CommitDuplicateKeyPolicy::Reject,
                    CommitTimestampPolicy::RuntimeGenerated,
                    CommitOrigin::StorageRuntime,
                ),
            ),
            generation_guard: CommitBranchGenerationGuard::not_supplied(),
        }
    }

    fn response() -> CommitGroupResponse {
        CommitGroupResponse {
            outcome: Err(LifecycleError::InvalidLifecycleState {
                reason: "join-queue unit test response",
            }),
            admission: None,
            pending_tasks: 0,
            idle_wake_pending: false,
            wal_growth: None,
            throttle_delay_millis: 0,
        }
    }

    fn request_tag(request: &CommitGroupRequest) -> u8 {
        request.batch.branch_id().as_bytes()[0]
    }

    #[test]
    fn first_joiner_leads_and_later_joiners_wait_in_fifo_order() {
        let queue = CommitGroupQueue::default();
        let JoinPath::Lead(leader) = queue.join(request(1)) else {
            panic!("first joiner must lead");
        };
        assert_eq!(request_tag(&leader), 1);

        let mut waiters = Vec::new();
        for tag in 2..=6 {
            match queue.join(request(tag)) {
                JoinPath::Wait(joiner) => waiters.push(joiner),
                JoinPath::Lead(_) => panic!("leader already active"),
            }
        }

        // Leader drains in FIFO order, capped.
        let first = queue.drain_members(3);
        let tags: Vec<_> = first
            .iter()
            .map(|(_, request)| request_tag(request))
            .collect();
        assert_eq!(tags, vec![2, 3, 4]);
        let rest = queue.drain_members(16);
        let tags: Vec<_> = rest
            .iter()
            .map(|(_, request)| request_tag(request))
            .collect();
        assert_eq!(tags, vec![5, 6]);
        assert!(queue.drain_members(16).is_empty());

        // Handing leadership off with an empty queue clears the flag: the
        // next joiner leads again.
        queue.finish_leadership();
        assert!(matches!(queue.join(request(7)), JoinPath::Lead(_)));
    }

    #[test]
    fn finish_leadership_promotes_the_front_waiter() {
        let queue = CommitGroupQueue::default();
        let JoinPath::Lead(_leader) = queue.join(request(1)) else {
            panic!("first joiner must lead");
        };
        let JoinPath::Wait(waiter) = queue.join(request(2)) else {
            panic!("second joiner must wait");
        };

        // The leader finishes without draining the waiter: the waiter is
        // promoted and leads with its own request.
        queue.finish_leadership();
        match queue.await_service(&waiter) {
            WaitOutcome::Lead(request) => assert_eq!(request_tag(&request), 2),
            outcome => panic!("expected promotion, got {outcome:?}"),
        }
        // Leadership stayed active across the promotion: new joiners wait.
        assert!(matches!(queue.join(request(3)), JoinPath::Wait(_)));
    }

    #[test]
    fn served_waiters_get_their_response_and_stale_nodes_are_skipped() {
        let queue = CommitGroupQueue::default();
        let JoinPath::Lead(_leader) = queue.join(request(1)) else {
            panic!("first joiner must lead");
        };
        let JoinPath::Wait(served) = queue.join(request(2)) else {
            panic!("waiter expected");
        };
        let JoinPath::Wait(_other) = queue.join(request(3)) else {
            panic!("waiter expected");
        };

        let members = queue.drain_members(1);
        assert_eq!(members.len(), 1);
        assert_eq!(request_tag(&members[0].1), 2);
        members[0].0.complete(response());
        match queue.await_service(&served) {
            WaitOutcome::Done(response) => assert!(response.outcome.is_err()),
            outcome => panic!("expected served response, got {outcome:?}"),
        }

        // A drained-and-completed node never reappears; the remaining pending
        // node is promoted on handoff.
        queue.finish_leadership();
        assert!(matches!(queue.join(request(4)), JoinPath::Wait(_)));
    }

    /// u32-payload protocol for direction tests on the generic layer (the
    /// std twin of the loom lane's `TagProtocol`).
    #[derive(Debug)]
    struct TagProtocol;

    impl GroupProtocol for TagProtocol {
        type Request = u32;
        type Response = u32;
        type Work = u32;
        type Done = u32;

        fn apply(work: u32) -> u32 {
            work + 1000
        }
    }

    #[test]
    fn reclaimed_state_decision_table() {
        // Still pending: lead with the own request, keep the claimed
        // leadership.
        let (outcome, keeps) = reclaimed_outcome::<TagProtocol>(JoinState::Pending(9));
        assert!(matches!(outcome, GroupWaitOutcome::Lead(9)));
        assert!(keeps);
        // Raced to done: hand leadership back, return the response.
        let (outcome, keeps) = reclaimed_outcome::<TagProtocol>(JoinState::Done(Box::new(70)));
        let GroupWaitOutcome::Done(response) = outcome else {
            panic!("done state returns the response");
        };
        assert_eq!(*response, 70);
        assert!(!keeps);
        // Anything else: hand leadership back and fail closed.
        let (outcome, keeps) = reclaimed_outcome::<TagProtocol>(JoinState::Taken);
        assert!(matches!(outcome, GroupWaitOutcome::Abandoned));
        assert!(!keeps);
        let (outcome, keeps) = reclaimed_outcome::<TagProtocol>(JoinState::Promoted(3));
        assert!(matches!(outcome, GroupWaitOutcome::Abandoned));
        assert!(!keeps);
    }

    #[test]
    fn apply_handoff_is_accepted_only_by_taken_members() {
        let queue = GroupQueue::<TagProtocol>::default();
        let GroupJoinPath::Lead(_own) = queue.join(0) else {
            panic!("first joiner must lead");
        };
        let GroupJoinPath::Wait(_handle) = queue.join(7) else {
            panic!("member must wait behind the leader");
        };
        let exchange = ExchangeHandle::<TagProtocol>::default();

        // Pending (not yet drained): the member cannot take the work.
        let queued = queue.drain_members(0);
        assert!(queued.is_empty());
        // Drained (Taken): the handoff is accepted.
        let members = queue.drain_members(1);
        assert_eq!(members.len(), 1);
        assert!(members[0].0.request_apply(5, &exchange).is_ok());

        // Completed (Done): a further handoff is refused and returned.
        members[0].0.complete(70);
        assert_eq!(members[0].0.request_apply(6, &exchange), Err(6));
    }

    #[test]
    fn leadership_guard_hands_off_on_drop() {
        let queue = CommitGroupQueue::default();
        let JoinPath::Lead(_leader) = queue.join(request(1)) else {
            panic!("first joiner must lead");
        };
        {
            let _guard = CommitGroupLeadership::new(&queue);
            // Guard dropped here without an explicit finish.
        }
        assert!(matches!(queue.join(request(2)), JoinPath::Lead(_)));
    }

    /// Counting ticket for the sync-chain decision tests.
    #[derive(Debug)]
    struct CountingTicket {
        captured: u64,
        syncs: std::sync::atomic::AtomicU64,
    }

    impl SyncTicket for &CountingTicket {
        type Error = std::convert::Infallible;

        fn captured_seq(&self) -> u64 {
            self.captured
        }

        fn sync(&self) -> Result<(), Self::Error> {
            self.syncs.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
    }

    #[test]
    fn sync_chain_proves_covered_tickets_without_syncing() {
        let chain = WalSyncChain::default();
        let durable = std::sync::atomic::AtomicU64::new(5);
        let ticket = CountingTicket {
            captured: 5,
            syncs: std::sync::atomic::AtomicU64::new(0),
        };
        // Covered (durable == captured): proven without a sync.
        assert!(chain
            .sync_or_wait_covered(&durable, &&ticket, false, || Ok(&ticket))
            .is_none());
        assert_eq!(ticket.syncs.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(durable.load(std::sync::atomic::Ordering::SeqCst), 5);
    }

    #[test]
    fn sync_chain_syncs_a_fresh_capture_and_publishes_coverage() {
        let chain = WalSyncChain::default();
        let durable = std::sync::atomic::AtomicU64::new(0);
        let ticket = CountingTicket {
            captured: 3,
            syncs: std::sync::atomic::AtomicU64::new(0),
        };
        let fresh = CountingTicket {
            captured: 7,
            syncs: std::sync::atomic::AtomicU64::new(0),
        };
        // Uncovered: the caller syncs the FRESH capture (not its own
        // phase-1 ticket) and the watermark advances to the fresh seq.
        let outcome = chain
            .sync_or_wait_covered(&durable, &&ticket, false, || Ok(&fresh))
            .expect("uncovered ticket must sync");
        outcome.expect("counting ticket sync is infallible");
        assert_eq!(ticket.syncs.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(fresh.syncs.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(durable.load(std::sync::atomic::Ordering::SeqCst), 7);
        // A follower with captures inside the fresh coverage is now proven
        // without another sync.
        let follower = CountingTicket {
            captured: 6,
            syncs: std::sync::atomic::AtomicU64::new(0),
        };
        assert!(chain
            .sync_or_wait_covered(&durable, &&follower, false, || Ok(&follower))
            .is_none());
        assert_eq!(follower.syncs.load(std::sync::atomic::Ordering::SeqCst), 0);
    }
}
