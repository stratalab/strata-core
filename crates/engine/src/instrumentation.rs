//! Performance instrumentation for optimization
//!
//! Re-exports from `strata_core::instrumentation`. The canonical definitions
//! live in core so that the concurrency crate can also use them without
//! a circular dependency.
//!
//! Feature-gated to avoid overhead in production.
//! Enable with: cargo build --features perf-trace
//!
//! # Usage
//!
//! ```text
//! use strata_engine::instrumentation::PerfTrace;
//!
//! #[cfg(feature = "perf-trace")]
//! let mut trace = PerfTrace::new();
//!
//! // Time a section
//! let result = perf_time!(trace, snapshot_acquire_ns, {
//!     engine.snapshot()
//! });
//!
//! #[cfg(feature = "perf-trace")]
//! println!("{}", trace.summary());
//! ```

pub use strata_core::instrumentation::PerfTrace;

#[cfg(feature = "perf-trace")]
pub use strata_core::instrumentation::{PerfBreakdown, PerfStats};
