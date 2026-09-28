//! Published, refcounted branch read snapshots (BS2.3) + the off-lock registry (BS2.4).
//!
//! A [`BranchReadSlot`] holds the most recently published [`BranchReadView`] for one branch behind
//! an [`ArcSwap`], so a reader can load a consistent snapshot with a single atomic op and serve
//! every read verb off it. A [`BranchSnapshotPublisher`] owns one slot per live branch for a
//! runtime, keyed in an [`ArcSwap`]-wrapped map. The publisher mutates the map only under the
//! runtime lock (publish/remove); a [`registry_handle`](BranchSnapshotPublisher::registry_handle)
//! shares the same map with the runtime slot so reads load a snapshot **off-lock** via
//! [`load_from_registry`].

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

use parking_lot::Mutex as ParkingMutex;

use arc_swap::ArcSwap;
use strata_core::BranchId;

use crate::branch::read::BranchReadView;

/// #3645: how a retired view's release reaches the maintenance scheduler.
///
/// The table-object sweep defers while any retired view is held. If it
/// deferred and reclaim is still owed, the release of the LAST retired view is
/// the event that can unblock it — so each published view carries this shared
/// signal, which counts the retired views still held and, when that count
/// reaches zero, runs the installed waker (which re-arms the debounced idle
/// wake). A retired view nobody held (a republish's predecessor) comes and goes
/// without firing while another retired view is still pinned. The
/// waker only touches atomics and schedules a timer: a view can be dropped
/// while the runtime lock is held, so it must never run a drain inline.
#[derive(Default)]
pub(crate) struct ViewReleaseSignal {
    /// A sweep deferred on a held reader and reclaim is still owed.
    reclaim_waits_on_reader: AtomicBool,
    /// Retired views some handle still holds (the count `retired_views_alive`
    /// tests, kept here so the release path needs no runtime lock).
    retired_alive: AtomicUsize,
    /// Installed by the runtime slot while it has a background scheduler and
    /// removed when the slot shuts it down, so a view that outlives its runtime
    /// holds no scheduler state.
    waker: ParkingMutex<Option<Box<dyn Fn() + Send + Sync>>>,
}

impl std::fmt::Debug for ViewReleaseSignal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ViewReleaseSignal")
            .field(
                "reclaim_waits_on_reader",
                &self.reclaim_waits_on_reader.load(Ordering::Acquire),
            )
            .field("retired_alive", &self.retired_alive.load(Ordering::SeqCst))
            .field("waker_installed", &self.waker.lock().is_some())
            .finish()
    }
}

impl ViewReleaseSignal {
    /// Record whether reclaim is waiting on a held reader (synced by the
    /// runtime after each maintenance round).
    pub(crate) fn set_reclaim_waits_on_reader(&self, waiting: bool) {
        // SeqCst pairs with the release path's load and the round's
        // retired-reader check (a store-then-check vs decrement-then-load pair).
        self.reclaim_waits_on_reader
            .store(waiting, Ordering::SeqCst);
    }

    pub(crate) fn reclaim_waits_on_reader(&self) -> bool {
        self.reclaim_waits_on_reader.load(Ordering::SeqCst)
    }

    /// Install the waker (the runtime slot does, when it has a background
    /// scheduler).
    pub(crate) fn install_waker(&self, waker: Box<dyn Fn() + Send + Sync>) {
        *self.waker.lock() = Some(waker);
    }

    #[cfg(all(test, feature = "localfs"))]
    pub(crate) fn waker_installed_for_test(&self) -> bool {
        self.waker.lock().is_some()
    }

    /// Remove the waker (the runtime slot does, when it shuts its scheduler
    /// down): later releases fire nothing.
    pub(crate) fn uninstall_waker(&self) {
        *self.waker.lock() = None;
    }

    fn view_retired(&self) {
        self.retired_alive.fetch_add(1, Ordering::SeqCst);
    }

    fn retired_view_released(&self) {
        let was_last = self.retired_alive.fetch_sub(1, Ordering::SeqCst) == 1;
        if was_last && self.reclaim_waits_on_reader() {
            if let Some(waker) = self.waker.lock().as_ref() {
                waker();
            }
        }
    }
}

/// #3645: one published view's link to the release signal. Shared by every
/// clone of the view; when the last clone drops after the publisher retired
/// the view, the signal fires.
#[derive(Debug)]
pub(crate) struct ViewRelease {
    signal: Arc<ViewReleaseSignal>,
    retired: AtomicBool,
}

impl ViewRelease {
    pub(crate) fn new(signal: Arc<ViewReleaseSignal>) -> Self {
        Self {
            signal,
            retired: AtomicBool::new(false),
        }
    }

    pub(crate) fn mark_retired(&self) {
        if !self.retired.swap(true, Ordering::AcqRel) {
            self.signal.view_retired();
        }
    }
}

impl Drop for ViewRelease {
    fn drop(&mut self) {
        if self.retired.load(Ordering::Acquire) {
            self.signal.retired_view_released();
        }
    }
}

/// The per-branch published snapshot map. Shared as `Arc<BranchSnapshotRegistry>` so the same map is
/// reachable from both the publish path (under the lock) and the off-lock read path.
pub(crate) type BranchSnapshotRegistry = ArcSwap<HashMap<BranchId, Arc<BranchReadSlot>>>;

/// The per-branch published snapshot cell. Shared as `Arc<BranchReadSlot>` so the same cell is
/// reachable from both the publish path (under the lock) and the off-lock registry.
#[derive(Debug)]
pub(crate) struct BranchReadSlot {
    // TCP4.3b: the coherence-critical cell of the off-lock read protocol
    // (V-before-S) goes through the sync seam so the loom lane explores
    // publish/load schedules on the real slot.
    snapshot: crate::sync::SwapCell<BranchReadView>,
}

impl BranchReadSlot {
    fn new(view: Arc<BranchReadView>) -> Self {
        Self {
            snapshot: crate::sync::SwapCell::new(view),
        }
    }

    /// Publish a freshly captured snapshot, replacing the previous one (release ordering). Old
    /// snapshots stay valid for any reader still holding them and die by `Arc` drop.
    fn publish(&self, view: Arc<BranchReadView>) {
        self.snapshot.store(view);
    }

    /// Load the currently published snapshot with a single atomic refcount bump.
    fn load(&self) -> Arc<BranchReadView> {
        self.snapshot.load_full()
    }
}

/// Owns the per-branch published snapshots for one runtime. The runtime captures a `BranchReadView`
/// (it holds the catalog) and hands the owned view here to store; this type never touches the
/// catalog, so capture and store never overlap a borrow.
///
/// The map lives behind an `ArcSwap`: publishing into an existing branch's slot never rebuilds the
/// map (the hot path), and only first-publish / removal (branch lifecycle events, both rare and
/// under the lock) swap a rebuilt map. The same `Arc` is shared with the runtime slot for off-lock
/// loads.
#[derive(Debug, Default)]
pub(crate) struct BranchSnapshotPublisher {
    read_slots: Arc<BranchSnapshotRegistry>,
    /// Weak handles to superseded (retired) views that an off-lock reader may still hold. Durable
    /// lazy readers are name-addressed range reads with no held file descriptor, so deleting a
    /// superseded table object breaks any reader still pinned to a retired view — the table-object
    /// sweep defers while one is alive. Pruned opportunistically on publish and on query.
    retired_views: Vec<Weak<BranchReadView>>,
    /// #3645: attached to every published view; fires when a retired one is
    /// released.
    release_signal: Arc<ViewReleaseSignal>,
}

impl BranchSnapshotPublisher {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Publish a freshly captured view, creating the branch's slot on first publish. Publishing into
    /// an existing slot is a single `ArcSwap` store with no map rebuild.
    pub(crate) fn publish_view(&mut self, branch_id: BranchId, mut view: Arc<BranchReadView>) {
        // A freshly captured view is uniquely owned here; attach the release
        // signal before any reader can load it.
        if let Some(fresh) = Arc::get_mut(&mut view) {
            fresh.attach_release(Arc::clone(&self.release_signal));
        }
        let map = self.read_slots.load();
        if let Some(slot) = map.get(&branch_id) {
            let retired = slot.load();
            slot.publish(view);
            self.retire_view(&retired);
        } else {
            let mut next = HashMap::clone(&map);
            next.insert(branch_id, Arc::new(BranchReadSlot::new(view)));
            self.read_slots.store(Arc::new(next));
        }
    }

    /// Drop a branch's slot (on delete). A racing reader completes on the snapshot it already holds.
    pub(crate) fn remove(&mut self, branch_id: BranchId) {
        let map = self.read_slots.load();
        if let Some(slot) = map.get(&branch_id) {
            let retired = slot.load();
            let mut next = HashMap::clone(&map);
            next.remove(&branch_id);
            self.read_slots.store(Arc::new(next));
            self.retire_view(&retired);
        }
    }

    /// #3047: every branch's CURRENTLY published view. A reader may load one at any moment, so
    /// the table-object mark must treat every object these reference as live — independent of
    /// the manifests and in-memory state it also pins, which can briefly disagree with what is
    /// published.
    pub(crate) fn current_views(&self) -> Vec<Arc<BranchReadView>> {
        self.read_slots
            .load()
            .values()
            .map(|slot| slot.load())
            .collect()
    }

    /// #3645: the release signal every published view carries.
    pub(crate) fn release_signal(&self) -> &Arc<ViewReleaseSignal> {
        &self.release_signal
    }

    /// A shared handle to the registry so the runtime slot can load snapshots off-lock (BS2.4).
    pub(crate) fn registry_handle(&self) -> Arc<BranchSnapshotRegistry> {
        Arc::clone(&self.read_slots)
    }

    /// Track a superseded view until every off-lock reader drops it. The local `retired` Arc (plus
    /// the registry's, already swapped out) is the only strong handle when no reader holds one, so
    /// the weak goes dead as soon as `retired` drops at the caller.
    fn retire_view(&mut self, retired: &Arc<BranchReadView>) {
        retired.mark_retired();
        self.retired_views.retain(|weak| weak.strong_count() > 0);
        self.retired_views.push(Arc::downgrade(retired));
    }

    /// Whether any retired view is still held by an off-lock reader. The table-object sweep defers
    /// while true: a retired view may reference superseded objects whose deletion would break the
    /// reader's name-addressed block fetches.
    pub(crate) fn retired_views_alive(&mut self) -> bool {
        self.retired_views.retain(|weak| weak.strong_count() > 0);
        !self.retired_views.is_empty()
    }
}

/// Load the published snapshot for `branch_id` directly from a shared registry handle, off-lock:
/// one `ArcSwap` load for the map, one for the slot. `None` means the branch has no published slot
/// (never created, or deleted — the caller maps this to not-found).
pub(crate) fn load_from_registry(
    registry: &BranchSnapshotRegistry,
    branch_id: BranchId,
) -> Option<Arc<BranchReadView>> {
    registry.load().get(&branch_id).map(|slot| slot.load())
}

#[cfg(test)]
mod release_signal_tests {
    use super::ViewReleaseSignal;

    /// #3645: the signal's debug form carries its three facts.
    #[test]
    fn view_release_signal_debug_reports_its_facts() {
        let signal = ViewReleaseSignal::default();
        signal.set_reclaim_waits_on_reader(true);
        let rendered = format!("{signal:?}");
        for fact in [
            "ViewReleaseSignal",
            "reclaim_waits_on_reader: true",
            "retired_alive: 0",
            "waker_installed: false",
        ] {
            assert!(rendered.contains(fact), "{fact} missing from {rendered}");
        }
        signal.install_waker(Box::new(|| {}));
        assert!(format!("{signal:?}").contains("waker_installed: true"));
    }
}
