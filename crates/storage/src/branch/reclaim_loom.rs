//! #3048 — exhaustive schedule exploration of the reader-versus-reclaim
//! liveness protocol behind #3047 (an off-lock reader range-read a table
//! object the table-object sweep had already deleted). Only compiled with
//! `RUSTFLAGS="--cfg loom"`.
//!
//! Two models.
//!
//! **Model A — the protocol on the real publisher.** Three threads:
//!
//! - an off-lock reader: the real [`load_from_registry`] (whose slot load is
//!   the `crate::sync::SwapCell` seam), holding the view across its "range
//!   read" of every table object the view references;
//! - a rewrite (compaction): the durable three-phase publish shape —
//!   install the output into in-memory branch state and register the
//!   pending manifest under one lock hold (`begin_compaction_publish` +
//!   `begin_off_lock_publish`), persist off-lock, then confirm the manifest
//!   and republish under a second hold (`finish_publish_phase`, which runs
//!   the real [`BranchSnapshotPublisher::publish_view`] and therefore the
//!   real `retire_view` prune);
//! - the table-object sweep: the mark under one lock hold
//!   (`start_next_background_quarantine_sweep`: the real
//!   [`BranchSnapshotPublisher::retired_views_alive`] interlock and the real
//!   [`BranchSnapshotPublisher::current_views`] pin, unioned with the
//!   in-memory and manifest-frontier pins), then the deletes off-lock
//!   (`SweepStageInputs::stage`).
//!
//! The oracle is COW-001 / ARCH-009 from the reader's side: no object a
//! reader's view references is deleted while the reader holds that view.
//!
//! What Model A abstracts, precisely: a table object is named by the
//! generation (captured `max_commit_version`) of the branch state that
//! references it — generation `g` references exactly object `g`, so a
//! rewrite consumes its input and a view references what its capture saw.
//! The durable table catalogue's identity-to-object mapping, the backend,
//! and the inventory listing are elided (the mark's candidates are every
//! written object not pinned). The world starts in the deferred-install
//! state the #3047 investigation found: the confirmed manifest lags one
//! generation behind in-memory state, because a compaction that found the
//! branch's publish slot busy installed in memory without registering any
//! manifest (the deterministic twin of this shape against the real runtime
//! is `a_rewrite_consuming_an_unmanifested_output_never_sweeps_the_current_views_table`).
//!
//! **Model B — the `arc_swap` debt protocol.** Under loom the swap cell is
//! a lock, so Model A cannot see the hypothesis #3048 was opened on: that
//! `ArcSwap::load_full` keeps a view alive through a debt slot that
//! `Weak::strong_count` does not count, so the sweep could see a held view
//! as dead. Model B transcribes `arc_swap` 1.9.2's `HybridStrategy` fast
//! path (`attempt` + `into_inner`) and `Debt::pay_all` onto loom atomics
//! and checks the one claim the publisher's `retire_view` comment relies
//! on: once `store` returns, `strong_count` counts every reader that holds
//! or will hold the old value. It holds, because `swap` pays every
//! outstanding debt on the old pointer (converting it to a real count)
//! BEFORE it returns; the sabotage twin that skips the payment shows the
//! oracle would catch the hazard if it existed.

use super::config::BranchRuntimeConfig;
use super::read::BranchReadView;
use super::snapshot::{load_from_registry, BranchSnapshotPublisher, BranchSnapshotRegistry};
use super::state::BranchLocalState;
use crate::row::{PhysicalKey, StorageRow, StorageSpaceId};
use loom::sync::{Arc as LoomArc, Mutex as LoomMutex};
use std::collections::BTreeSet;
use std::sync::Arc;
use strata_core::{BranchId, CommitVersion, Timestamp};

fn branch() -> BranchId {
    BranchId::from_bytes([9; 16])
}

/// One commit that moves the branch to generation `generation`.
fn rows(generation: u64) -> Vec<StorageRow> {
    vec![StorageRow::put(
        PhysicalKey::new(
            branch(),
            "reclaim-loom",
            StorageSpaceId::engine(0x38).expect("engine storage space"),
            b"k".to_vec(),
        )
        .expect("physical key"),
        CommitVersion::new(generation),
        Timestamp::from_micros(generation),
        Timestamp::MAX,
        generation.to_le_bytes().to_vec(),
    )]
}

/// The table object a view references: its captured generation.
fn view_object(view: &BranchReadView) -> u64 {
    view.facts()
        .max_commit_version()
        .expect("every view in the model has a commit")
        .as_u64()
}

/// Which pins the mark applies (the production set, or a sabotage twin).
#[derive(Clone, Copy)]
struct Mark {
    /// #3639: pin every object a CURRENT published view references.
    current_view_pin: bool,
    /// Defer the whole sweep while any retired view is still held.
    retired_view_interlock: bool,
}

const PRODUCTION: Mark = Mark {
    current_view_pin: true,
    retired_view_interlock: true,
};

/// Everything the runtime lock guards.
struct Runtime {
    state: BranchLocalState,
    publisher: BranchSnapshotPublisher,
    /// The object the last durably confirmed manifest lists.
    confirmed_manifest: u64,
    /// The object an in-flight manifest persist lists.
    pending_manifest: Option<u64>,
    /// Every object ever written (the mark's inventory).
    written: BTreeSet<u64>,
}

struct World {
    runtime: LoomMutex<Runtime>,
    registry: Arc<BranchSnapshotRegistry>,
    /// Objects the sweep has deleted from the backend.
    deleted: LoomMutex<BTreeSet<u64>>,
}

/// The deferred-install world: generation 1 is installed and published,
/// but the confirmed manifest still lists generation 0 (a compaction
/// installed generation 1 while another publish held the branch's slot).
fn world() -> LoomArc<World> {
    let mut state = BranchLocalState::new(
        branch(),
        BranchRuntimeConfig::default()
            .with_active_rotation_bytes(usize::MAX / 2)
            .expect("rotation config"),
    )
    .expect("branch state");
    state
        .append_committed_rows_atomically(rows(1))
        .expect("generation 1 applies");
    let mut publisher = BranchSnapshotPublisher::new();
    publisher.publish_view(
        branch(),
        Arc::new(state.capture_snapshot().expect("generation 1 view")),
    );
    let registry = publisher.registry_handle();
    LoomArc::new(World {
        runtime: LoomMutex::new(Runtime {
            state,
            publisher,
            confirmed_manifest: 0,
            pending_manifest: None,
            written: BTreeSet::from([0, 1]),
        }),
        registry,
        deleted: LoomMutex::new(BTreeSet::new()),
    })
}

/// A rewrite consuming the current generation into `next`, in the durable
/// three-phase publish shape.
fn rewrite(world: &World, next: u64) {
    {
        // Begin (one lock hold): the output is written (in-flight pin elided:
        // it hands over to the in-memory pin inside this same hold), the
        // install consumes the input out of in-memory state, and the pending
        // manifest is registered. The view is NOT republished here.
        let mut runtime = world.runtime.lock().expect("runtime lock");
        runtime.written.insert(next);
        runtime
            .state
            .append_committed_rows_atomically(rows(next))
            .expect("rewrite installs");
        runtime.pending_manifest = Some(next);
    }
    // The manifest persists off-lock (no shared state touched).
    let mut guard = world.runtime.lock().expect("runtime lock");
    // Finish (second lock hold): confirm the manifest and republish — the
    // real publish, which retires (and prunes) the superseded view.
    let runtime = &mut *guard;
    runtime.confirmed_manifest = runtime.pending_manifest.take().expect("pending manifest");
    let view = Arc::new(runtime.state.capture_snapshot().expect("post-rewrite view"));
    runtime.publisher.publish_view(branch(), view);
}

/// The table-object sweep: mark under the lock, delete off-lock.
fn sweep(world: &World, mark: Mark) {
    let candidates: Vec<u64> = {
        let mut guard = world.runtime.lock().expect("runtime lock");
        let runtime = &mut *guard;
        let retired_readers_alive = runtime.publisher.retired_views_alive();
        let mut pinned = BTreeSet::from([runtime.confirmed_manifest]);
        pinned.extend(runtime.pending_manifest);
        pinned.insert(
            runtime
                .state
                .facts()
                .expect("in-memory facts")
                .max_commit_version()
                .expect("in-memory generation")
                .as_u64(),
        );
        if mark.current_view_pin {
            pinned.extend(
                runtime
                    .publisher
                    .current_views()
                    .iter()
                    .map(|view| view_object(view)),
            );
        }
        let deleted = world.deleted.lock().expect("deleted lock");
        let candidates: Vec<u64> = runtime
            .written
            .iter()
            .filter(|object| !pinned.contains(object) && !deleted.contains(object))
            .copied()
            .collect();
        if candidates.is_empty() || (mark.retired_view_interlock && retired_readers_alive) {
            return;
        }
        candidates
    };
    // Off-lock stage: each delete is its own backend step.
    for object in candidates {
        world.deleted.lock().expect("deleted lock").insert(object);
    }
}

/// The off-lock reader: load the published view, then range-read what it
/// references while still holding it.
fn read(world: &World) {
    let view = load_from_registry(&world.registry, branch()).expect("branch view published");
    let object = view_object(&view);
    assert!(
        !world
            .deleted
            .lock()
            .expect("deleted lock")
            .contains(&object),
        "reader range-read deleted table object {object}"
    );
    drop(view);
}

fn run(mark: Mark, rewrites: &'static [u64]) {
    // Unbounded: every schedule is explored (a few seconds).
    loom::model(move || {
        let world = world();
        let rewriter = {
            let world = LoomArc::clone(&world);
            loom::thread::spawn(move || {
                for next in rewrites {
                    rewrite(&world, *next);
                }
            })
        };
        let sweeper = {
            let world = LoomArc::clone(&world);
            loom::thread::spawn(move || sweep(&world, mark))
        };
        read(&world);
        rewriter.join().expect("rewriter completes");
        sweeper.join().expect("sweeper completes");
        // Quiesced: whatever the sweep deleted, the published view is whole.
        read(&world);
    });
}

/// The production mark, against the #3047 window: a rewrite consumes an
/// object no manifest lists while the reader may hold (or be about to load)
/// the current view that references it. Every schedule is safe.
#[test]
fn loom_reclaim_never_deletes_what_a_reader_can_reach() {
    run(PRODUCTION, &[2]);
}

/// Two back-to-back rewrites: the second publish's `retire_view` prunes the
/// retired list while the reader may hold either superseded view.
#[test]
fn loom_reclaim_survives_back_to_back_rewrites() {
    run(PRODUCTION, &[2, 3]);
}

/// Sabotage twin (the pre-#3639 mark — the #3047 root cause): without the
/// current-view pin, the object the published view references is pinned by
/// nothing in the rewrite's window, and the sweep deletes it under a reader.
#[test]
#[should_panic(expected = "reader range-read deleted table object")]
fn loom_mark_without_the_current_view_pin_is_caught() {
    run(
        Mark {
            current_view_pin: false,
            retired_view_interlock: true,
        },
        &[2],
    );
}

/// Sabotage twin: without the retired-view interlock, a reader still holding
/// the view a rewrite retired loses its object.
#[test]
#[should_panic(expected = "reader range-read deleted table object")]
fn loom_mark_without_the_retired_view_interlock_is_caught() {
    run(
        Mark {
            current_view_pin: true,
            retired_view_interlock: false,
        },
        &[2],
    );
}

/// Model B: `arc_swap` 1.9.2's debt protocol, transcribed onto loom atomics.
///
/// Loom treats `SeqCst` accesses as `AcqRel` (its README, "Unsupported
/// features"), which would admit the store-buffering outcome real `SeqCst`
/// forbids between the reader's debt-slot store and the writer's slot scan.
/// A `SeqCst` fence (which loom does model) after each side's store restores
/// exactly that ordering and nothing more.
mod debt {
    use loom::sync::atomic::{fence, AtomicBool, AtomicUsize, Ordering::SeqCst};
    use loom::sync::Arc;

    const NONE: usize = 0;
    const OLD: usize = 1;
    const NEW: usize = 2;

    struct Cell {
        storage: AtomicUsize,
        /// The reader thread's fast debt slot.
        slot: AtomicUsize,
        /// Strong counts, indexed by pointer.
        strong: [AtomicUsize; 3],
        /// The sweep's decision for the old value's objects.
        old_deleted: AtomicBool,
    }

    /// `ArcSwap::load_full`: `HybridStrategy::attempt` then
    /// `HybridProtection::into_inner`. The fallback (helping) path is
    /// modelled as a retry; with one writer swap the retry's loads are
    /// stable, so it always completes on the fast path.
    fn load_full(cell: &Cell) -> usize {
        loop {
            let ptr = cell.storage.load(SeqCst);
            cell.slot.store(ptr, SeqCst);
            fence(SeqCst);
            let confirm = cell.storage.load(SeqCst);
            if ptr == confirm {
                // into_inner: take a real count, then return the debt; if a
                // writer already paid it, give the duplicate count back.
                cell.strong[ptr].fetch_add(1, SeqCst);
                if cell
                    .slot
                    .compare_exchange(ptr, NONE, SeqCst, SeqCst)
                    .is_err()
                {
                    cell.strong[ptr].fetch_sub(1, SeqCst);
                }
                return ptr;
            }
            if cell
                .slot
                .compare_exchange(ptr, NONE, SeqCst, SeqCst)
                .is_ok()
            {
                continue;
            }
            // The writer paid this debt: the count it added is ours.
            return ptr;
        }
    }

    /// `publish_view` + `retire_view` + the sweep's `strong_count` check.
    fn publish_and_sweep(cell: &Cell, pay_debts: bool) {
        // `let retired = slot.load()`: the runtime lock excludes every other
        // swap, so this load is a plain count bump.
        cell.strong[OLD].fetch_add(1, SeqCst);
        // `slot.publish(view)` = `ArcSwap::store` = `swap` + `wait_for_readers`.
        let old = cell.storage.swap(NEW, SeqCst);
        fence(SeqCst);
        if pay_debts {
            // Debt::pay_all: pre-pay one count, hand it to each debt found on
            // `old`, drop the unused pre-payment.
            cell.strong[old].fetch_add(1, SeqCst);
            if cell
                .slot
                .compare_exchange(old, NONE, SeqCst, SeqCst)
                .is_ok()
            {
                cell.strong[old].fetch_add(1, SeqCst);
            }
            cell.strong[old].fetch_sub(1, SeqCst);
        }
        // `store` drops the swapped-out storage reference.
        cell.strong[old].fetch_sub(1, SeqCst);
        // `retire_view` downgrades `retired`, which the caller then drops.
        cell.strong[OLD].fetch_sub(1, SeqCst);
        // The sweep: `retired_views_alive` is `strong_count() > 0`.
        if cell.strong[OLD].load(SeqCst) == 0 {
            cell.old_deleted.store(true, SeqCst);
        }
    }

    fn run(pay_debts: bool) {
        loom::model(move || {
            let cell = Arc::new(Cell {
                storage: AtomicUsize::new(OLD),
                slot: AtomicUsize::new(NONE),
                strong: [
                    AtomicUsize::new(0),
                    AtomicUsize::new(1),
                    AtomicUsize::new(1),
                ],
                old_deleted: AtomicBool::new(false),
            });
            let writer = {
                let cell = Arc::clone(&cell);
                loom::thread::spawn(move || publish_and_sweep(&cell, pay_debts))
            };
            let held = load_full(&cell);
            if held == OLD {
                assert!(
                    !cell.old_deleted.load(SeqCst),
                    "sweep judged a view dead while a load_full held it"
                );
            }
            cell.strong[held].fetch_sub(1, SeqCst);
            writer.join().expect("writer completes");
        });
    }

    /// `strong_count` after `store` counts every in-flight `load_full` of
    /// the old value: the #3048 hypothesis does not hold.
    #[test]
    fn loom_strong_count_after_store_counts_in_flight_loads() {
        run(true);
    }

    /// Sabotage twin: a swap that did not pay outstanding debts WOULD let the
    /// sweep see a held view as dead — the oracle catches it.
    #[test]
    #[should_panic(expected = "sweep judged a view dead while a load_full held it")]
    fn loom_swap_without_paying_debts_is_caught() {
        run(false);
    }
}
