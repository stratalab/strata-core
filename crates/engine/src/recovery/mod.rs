//! Pluggable recovery + freeze via the `Subsystem` trait.
//!
//! A subsystem is an independent component (vector index, search index,
//! graph adjacency cache, …) that owns runtime state outside
//! `SegmentedStore` and needs to rebuild that state on database open and
//! optionally persist it on drop. Each `Database` holds a
//! `Vec<Box<dyn Subsystem>>` supplied at open time via [`OpenSpec`];
//! the **same ordered list** drives recovery (in registration order) and
//! freeze (in reverse order), so callers cannot accidentally recover one
//! set of subsystems while freezing a different set — the guarantee the
//! lifecycle-unification refactor in stratalab/strata-core#2354 exists to
//! enforce.
//!
//! ## Runtime Composition
//!
//! The engine crate declares the trait and owns the product-open policy. Until
//! graph/vector/search are all engine-owned, product callers pass the current
//! subsystem instances into the engine product-open API as a temporary bridge.
//! Engine-internal tests that do not load the vector crate can install
//! `[SearchSubsystem]` directly via `OpenSpec::with_subsystem`.
//!
//! ## Lifecycle
//!
//! ```text
//! OpenSpec::primary(path)
//!     .with_subsystem(VectorSubsystem)
//!     .with_subsystem(SearchSubsystem)
//!         │
//!         ▼
//! Database::open_runtime(OpenSpec)
//!     1. WAL replay + segment recovery (engine-owned, no subsystem)
//!     2. for subsystem in &subsystems { subsystem.recover(&db)?; }
//!     3. db.set_subsystems(subsystems)    // same list, same order
//!         │
//!         ▼
//!     // ... database handed to caller ...
//!         │
//!         ▼
//! Drop for Database (or Database::shutdown)
//!     4. run_freeze_hooks():
//!            for subsystem in subsystems.iter().rev() {
//!                subsystem.freeze(&db)    // best-effort, logs on failure
//!            }
//! ```
//!
//! See [`Subsystem`] for the trait definition and
//! `crates/engine/src/database/spec.rs` for the OpenSpec API.
//!
//! [`OpenSpec`]: crate::OpenSpec

mod subsystem;

pub use subsystem::Subsystem;
