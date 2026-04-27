# Strata error architecture: one engine-owned parent, layered beneath

## 1. Executive recommendation

**Strata should keep one canonical parent error type, but it should not live in `core` and it should not be a junk-drawer.** The parent — call it `StrataError` — belongs in **`engine`**, because `engine` is the layer that owns the database kernel, transactions, snapshots, recovery, and the canonical runtime semantics that every higher layer ultimately reasons about. `core` should be reduced to small, type-local validation/parse errors. `storage` should keep `StorageError` and never depend on `StrataError`. `executor` should keep its own boundary error (`executor::Error`), shaped like a stable wire-protocol code set, and convert from `StrataError` at the boundary. `cli` does presentation only and is the only layer permitted to use `anyhow`.

This is the **redb pattern**, hardened with **FoundationDB-style retry and ambiguous-commit semantics** and a **gRPC-canonical-codes** style executor boundary. It rejects both extremes: a single global `StrataError` mega-enum (sqlx-style sprawl, sled's documented anti-pattern) and unstructured proliferation of niche per-function errors.

**Concrete layer rules:**
- `core`: returns `core::Error` (or per-module mini-errors) — only type-local validation. Must not depend on storage/engine.
- `storage`: returns `Result<T, StorageError>`. Owns IO, page, WAL, fsync, checksum, corruption, lock-poisoning. Must not know about transactions.
- `engine`: returns `Result<T, StrataError>` (or narrower domain errors that flow into it). Owns transactions, MVCC/snapshot, conflict, ambiguous commit, schema, recovery. **This is the home of the canonical parent.**
- `executor`: returns `Result<T, executor::Error>`. Public IPC/session/command boundary. Translates `StrataError`/`StorageError` into a small stable code set. Wire contract.
- `cli`: returns `anyhow::Result<()>`. Reporting only.

**Stable codes and retryability classifiers are more important than preserving any single enum.** Strata's parent error must expose `is_retryable()`, `is_maybe_committed()`, `is_corruption()`, and a stable `kind() -> StrataErrorKind` accessor. `Display` strings are not part of the contract.

---

## 2. Rust best practices that bind this design

The Rust ecosystem has converged on a small set of rules for multi-crate library workspaces. The relevant ones for Strata:

**`thiserror` for libraries, `anyhow` for applications — and dtolnay says so directly.** The thiserror README states it is intended for code "where the caller receives exactly the information that you choose," and explicitly notes that "thiserror deliberately does not appear in your public API." The anyhow README scopes anyhow to "easy idiomatic error handling in Rust applications." Issue dtolnay/anyhow#102 records the consensus that `anyhow::Error` is inappropriate as a library's public return type because it erases the caller's ability to match. **Strata's library crates (`core`, `storage`, `engine`, `executor`) should use `thiserror`. Only `cli` should depend on `anyhow`.**

**`Error::source` is for crossing abstraction boundaries.** The std docs say verbatim: "`Error::source()` is generally used when errors cross 'abstraction boundaries'. If one module must report an error that is caused by an error from a lower-level module, it can allow accessing that error via `Error::source()`." The std docs also impose a hard rule: "the underlying error should be either returned by the outer error's `Error::source()`, or rendered by the outer error's `Display` implementation, but not both." Strata must pick one — **`source()` only, never `Display` interpolation of the cause**. With thiserror, this means do not put `{source}` inside `#[error("...")]` messages.

**`#[from]` is dangerous; prefer `#[source]` plus explicit `.map_err`.** matklad and quad have documented at length that bare `#[from]` (a) silently coerces every `?` site into producing your variant — including conversions that are semantically wrong, (b) makes "where did this error come from" un-greppable, (c) discards local context (path, key, page, txn id), and (d) puts the `From` impl into your public API. **For Strata: use `#[from]` only for unambiguous, context-free, lossless lifts; for everything else use `#[source]` and `.map_err(|e| MyError::Foo { source: e, page, txn, … })`.**

**Two valid public shapes for a top-level facade.** The opaque-struct + private-repr pattern (`std::io::Error`, `hyper::Error`, `reqwest::Error`) hides the internal enum behind a public `kind()` accessor returning a `#[non_exhaustive]` enum. The flat-enum pattern (`sqlx::Error`, `rusqlite::Error`) exposes variants directly with `#[non_exhaustive]`. **For Strata's `engine::StrataError`, the flat-enum-with-`#[non_exhaustive]`-and-nested-reasons shape is appropriate** because the variant set is genuinely the public taxonomy databases need (NotFound, Conflict, AmbiguousCommit, Corruption, etc.) and it parallels well-understood SQLSTATE classes. **For `executor::Error`, the small fixed code-shaped enum is the right pattern** because it crosses a serialization boundary.

**Errors must be `Send + Sync + 'static`.** Per Rust API Guidelines C-GOOD-ERR: "Typically `Error + Send + Sync + 'static` will be the most useful for callers. The addition of `'static` allows the trait object to be used with `Error::downcast_ref`." Strata has background threads (compaction, flush, possibly IPC); this is non-negotiable.

**`#[non_exhaustive]` on every public error and every public kind enum.** RFC 2008 cites errors as the prototypical use case. Internal/private repr enums should *not* be `#[non_exhaustive]` — exhaustiveness inside the crate is a compile-time correctness check.

**Display is not a contract.** `hyper::Error` documents this verbatim: "The contents of the formatted error message of this specific Error type is unspecified. You must not depend on it." Strata must adopt the same disclaimer. Programmatic discrimination is via variants/kind/codes; `Display` is for humans.

**Pertinent precedents in the embedded-DB space:**
- **sled** (sled.rs/errors.html) explicitly warns against unified error enums: "make the global Error enum specifically only hold errors that should cause the overall system to halt … Keep errors which relate to separate concerns in totally separate error types." Sled keeps a small `Error` for fatal cases and uses separate `ConflictableTransactionError` / `UnabortableTransactionError` for transactional concerns.
- **redb** (the closest analogue to Strata) explicitly **split a previously unified Error into eight typed errors**: `StorageError`, `TableError`, `TransactionError`, `CommitError`, `SavepointError`, `CompactionError`, `DatabaseError`, `UpgradeError`, plus a convenience `Error` superset with `From` impls. Lower-layer errors bubble up via dedicated variants like `TableError::Storage(StorageError)`. The CHANGELOG is unambiguous: "Improve errors to be more granular. Error has been split into multiple different enums, which can all be implicitly converted back to Error for convenience."
- **rust-rocksdb** is the **anti-pattern** here: it flattens RocksDB's rich `Status` (Code + SubCode + Severity + retryable + data_loss + scope) into a stringly `ErrorKind` parsed from the C++ message. Strata must not lose structure when wrapping.

---

## 3. Database best practices that bind this design

**Mature systems treat error codes as the public contract; messages are not.** PostgreSQL Appendix A is explicit: "Applications that need to know which error condition has occurred should usually test the error code, rather than looking at the textual error message. The error codes are less likely to change across PostgreSQL releases, and also are not subject to change due to localization." SQLite makes the same statement and exposes both a primary code and an extended code with the rule that `extended & 0xFF == primary` so clients that only understand the primary class still work.

**SQLSTATE classes are a stable taxonomy.** Postgres uses a 5-character `class(2)+subclass(3)` code where the first two characters identify the class. The classes most relevant to Strata are: **08** connection exception, **22** data exception, **23** integrity constraint violation, **25** invalid transaction state, **40** transaction rollback (40001 serialization_failure, 40P01 deadlock_detected, 40003 statement_completion_unknown), **42** syntax/access, **53** insufficient resources, **57** operator intervention, **58** system error, **XX** internal error (XX001 data_corrupted, XX002 index_corrupted). The deliberate split between **XX** (engine-bug class), **58** (OS-told-us-no), and **53** (resource exhaustion) is the right granularity for Strata's own taxonomy.

**SQLite's "primary + extended" structure maps cleanly onto a Rust enum-of-enums.** SQLite has ~31 primary codes (`SQLITE_BUSY`, `SQLITE_LOCKED`, `SQLITE_CORRUPT`, `SQLITE_IOERR`, `SQLITE_CONSTRAINT`, `SQLITE_FULL`, `SQLITE_MISUSE`, etc.) and ~74 extended codes (e.g. `SQLITE_CONSTRAINT_UNIQUE`, `SQLITE_CORRUPT_INDEX` — "suggests that the problem might be resolved by running the REINDEX command" — `SQLITE_BUSY_SNAPSHOT`, `SQLITE_IOERR_FSYNC`, `SQLITE_IOERR_DATA` for checksum failure). The split between `SQLITE_BUSY` (cross-process retryable contention) and `SQLITE_LOCKED` (same-connection reentrancy) is a doctrinal distinction Strata should preserve even in-process.

**Postgres's primary/detail/hint message model is worth copying verbatim.** Per the Error Message Style Guide: primary is short, lowercase, no trailing punctuation; detail is factual amplification (which syscall, which page); hint is suggestion ("run REINDEX"). Object identifiers (`PG_DIAG_TABLE_NAME`, `PG_DIAG_CONSTRAINT_NAME`, etc.) are **structured fields** beside the message, not embedded in it — so applications never regex error text. **Strata should do the same: error variants carry structured fields (table, key, page, txn_id), and `Display` formats only the primary; CLI renders detail/hint separately.**

**FoundationDB and CockroachDB universally treat "may have committed" as a first-class outcome.** This is the most important transactional lesson. FDB has a dedicated error code `commit_unknown_result (1021)`, **structurally distinct** from `not_committed (1020)` which means "definitely failed (conflict)." FDB further exposes three predicates as the public retry contract: `FDB_ERROR_PREDICATE_RETRYABLE`, `FDB_ERROR_PREDICATE_MAYBE_COMMITTED`, `FDB_ERROR_PREDICATE_RETRYABLE_NOT_COMMITTED`. The developer guide guarantees: "if it didn't commit, it will never commit in the future. … if your transaction is idempotent you can simply retry." CockroachDB exposes the same dichotomy as SQLSTATE `40001` (retry conflict, "restart transaction") vs `40003 statement_completion_unknown` ("result is ambiguous"), with a dedicated `AmbiguousCommitError` Go type. The contract is identical: ambiguity occurs only at the commit boundary; if a connection drops before commit was attempted, the txn is "definitely aborted."

**FDB also distinguishes Timeout from AmbiguousCommit and warns that Timeout is *worse*.** From the FDB developer guide: "these errors lack the one guarantee `commit_unknown_result` still gives to the user: if the commit has already been sent to the database, the transaction could get committed at a later point in time." Strata must keep `Timeout` and `AmbiguousCommit` as distinct variants and document this guarantee precisely.

**Retryability is best exposed as an explicit predicate, not inferred.** FDB's `is_retryable() / is_maybe_committed() / is_retryable_not_committed()` API is the cleanest in the industry. Postgres clients infer retryability from class 40 by convention; SQLite from `SQLITE_BUSY/SQLITE_LOCKED`. **Strata should do both: encode retryability in the variant choice AND expose explicit predicate methods.**

**gRPC canonical codes are the right model for the executor boundary.** Sixteen status codes, retryability baked into the taxonomy: UNAVAILABLE means "retry the call," ABORTED means "retry at a higher level (read-modify-write)," FAILED_PRECONDITION means "do not retry until system state changes." etcd implements this with ~50 named errors each pinned to one of the 16 codes plus a free-form message. The HTTP gateway exposes `google.rpc.Status` (code, message, repeated `details: Any`). Strata's executor boundary should mirror this.

**Don't expose subsystem internals to clients.** Postgres carries source file/line/function in libpq diagnostic fields, separate from SQLSTATE. They are a debugging channel; codes are the contract. Strata's executor boundary must not leak storage-layer internals (page IDs, fsync errnos) into client-facing codes — those belong in `details` payloads or trace logs.

---

## 4. Recommended error architecture for Strata

The candidate model in the brief is essentially correct, with three sharpenings.

**By-layer ownership:**

| Layer | Owns | Returns | Depends on |
|---|---|---|---|
| `core` | `core::Error` (small) — encoding, schema-key parsing, type validation | `Result<T, core::Error>` per module, or per-call mini-errors | nothing |
| `storage` | `StorageError`, `StorageResult<T>` | `Result<T, StorageError>` | `core` |
| `engine` | **`StrataError`, `StrataResult<T>` (canonical parent)**; nested reasons (`ConflictReason`, `RecoveryReason`, `CorruptionDetail`, `CommitPhase`) | `Result<T, StrataError>` | `core`, `storage` |
| `executor` | `executor::Error`, `Code`, `Status`, `Details` | `Result<T, executor::Error>` | `engine` (and transitively storage/core) |
| `cli` | nothing taxonomic; presentation, exit codes, JSON shaping | `anyhow::Result<()>` | all |

**Three sharpenings versus the brief's candidate:**

1. **`StrataError` is a flat `#[non_exhaustive]` enum at engine level, with nested reason types as payloads — it is not "a wrapper around smaller domain enums."** Wrappers are right when each domain is independent (redb's split). For Strata, the domains *are* the database's own vocabulary (Conflict, NotFound, Corruption, etc.); flattening them into one variant set is what makes `kind()` and `is_retryable()` cheap and stable. Heavy variants carry nested reason structs (e.g. `Conflict { reason: ConflictReason, … }` where `ConflictReason` is itself a `#[non_exhaustive]` enum).

2. **`StorageError` does not bubble through `StrataError` as `Storage(StorageError)` for *every* failure.** That is the redb shape, and it works there because redb's StorageError is small (`Io`, `Corrupted`, `ValueTooLarge`, `PreviousIo`, `LockPoisoned`). For Strata, the engine should **interpret** storage failures: a storage `Corrupted` becomes `StrataError::Corruption { kind, detail, source: storage_err }`; a storage `Io` becomes `StrataError::Io { op, source }` or, where it pollutes a commit, `StrataError::AmbiguousCommit { phase: CommitPhase::WalFsync, source }`. **The engine takes responsibility for assigning database-semantic meaning to storage failures.** The raw storage error is preserved in `source()` for diagnostics.

3. **Stable codes (`StrataErrorCode`) are a deliberate, separate construct** alongside the variant taxonomy — not redundant with it. Codes are u32 (low byte = primary class, high bytes = extended subtype, SQLite-style). They exist because (a) the executor boundary is serialization-bound and needs stable wire integers, (b) telemetry/dashboards want a small enumerable set, (c) clients in other languages need a code-based contract. Internal Rust callers use variants and `kind()`; wire/IPC clients use `code`.

**Top-level decisions, answered directly:**

- **Should `StrataError` be flat, layered, or a wrapper around domain enums?** Flat enum at engine level, with `#[non_exhaustive]`, with heavy variants carrying nested `*Reason` structs. Not a wrapper-of-wrappers.
- **Stable error codes like SQLSTATE / SQLite extended codes?** Yes. A `StrataErrorCode(u32)` with primary-class low byte and extended subtype high bytes. Mirrored to a `StrataErrorClass` enum for ergonomic matching.
- **Encode retryability explicitly?** Yes. `is_retryable()`, `is_maybe_committed()`, `is_definitely_not_committed()` as inherent methods on `StrataError`. Modeled directly on FDB's three-predicate API.
- **Ambiguous commit a first-class class?** Yes. Top-level `StrataError::AmbiguousCommit { phase: CommitPhase, source }` with the explicit guarantee "no in-flight commit remains." Distinct from `Timeout` (no such guarantee) and from `Conflict` (definitely failed).
- **Top-level classes:** Yes to `NotFound`, `InvalidArgument` (split from `Schema` and `Constraint` because the SQLSTATE precedent does), `Conflict`, `AmbiguousCommit`, `Timeout`, `Cancelled`, `Storage` (raw escalation), `Corruption`, `Io`, `Resource` (disk full / OOM), `Unsupported`, `TxState` (no active txn / in failed txn), `Internal`, `Misuse`. **No** to `"DatabaseError"` (means nothing), `"GenericError"`, or per-subsystem variants like `"WalReplay"` — those are nested reasons.
- **What stays nested?** `ConflictReason` (WriteWrite, ReadWrite, Deadlock, SerializationFailure), `CorruptionDetail` (PageChecksum, IndexInvariant, WalCorrupt, HeaderInvalid, FreelistCorrupt — copying SQLite's split), `CommitPhase` (WalFsync, ManifestRename, GroupCommitNotify), `RecoveryReason`, `IoOp`, `ResourceKind` (DiskFull, OutOfMemory, FdsExhausted), `TxStateKind` (NoActiveTx, AbortedTx, ReadOnlyTx, NestedTx). These are `#[non_exhaustive]` enums that the engine can grow without bumping `StrataError`'s variant set.

---

## 5. Concrete design proposal

### 5.1 `strata-storage::StorageError`

Storage owns disk, paging, WAL bytes, fsync, locks. It does **not** know about transactions or schema.

```rust
// strata-storage/src/error.rs
use std::{io, path::PathBuf};
use thiserror::Error;

pub type StorageResult<T> = Result<T, StorageError>;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum StorageError {
    #[error("io error during {op:?} on {path:?}")]
    Io {
        op: IoOp,
        path: Option<PathBuf>,
        #[source]
        source: io::Error,
    },

    #[error("checksum mismatch at page {page_id}")]
    ChecksumMismatch { page_id: u64, expected: u32, found: u32 },

    #[error("structural corruption: {detail:?}")]
    Corruption { detail: CorruptionDetail, hint: Option<&'static str> },

    #[error("write rejected: previous io error has poisoned this handle")]
    PoisonedAfterIo,

    #[error("lock poisoned at {location}")]
    LockPoisoned { location: &'static std::panic::Location<'static> },

    #[error("out of space: {kind:?}")]
    OutOfSpace { kind: SpaceKind },

    #[error("value too large ({len} bytes; max {max})")]
    ValueTooLarge { len: usize, max: usize },

    #[error("unsupported storage operation: {0}")]
    Unsupported(&'static str),
}

#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub enum IoOp { Read, Write, Fsync, FsyncDir, Truncate, Rename, Open, Lock }

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum CorruptionDetail {
    PageHeader { page_id: u64 },
    PageChecksum { page_id: u64 },
    IndexInvariant { index: String, reason: &'static str },
    WalRecord { offset: u64 },
    Manifest { reason: &'static str },
    Freelist,
}

#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub enum SpaceKind { Disk, FileSystemQuota, FdLimit }

impl StorageError {
    pub fn is_corruption(&self) -> bool { matches!(self, Self::Corruption { .. } | Self::ChecksumMismatch { .. }) }
    pub fn is_io(&self) -> bool { matches!(self, Self::Io { .. }) }
}
```

Note: no `From<io::Error>` impl. Every `io::Error` site must be wrapped with `IoOp` and (where known) the path. This is the matklad/quad rule.

### 5.2 `strata-engine::StrataError` (the canonical parent)

```rust
// strata-engine/src/error.rs
use strata_storage::StorageError;
use thiserror::Error;

pub type StrataResult<T> = Result<T, StrataError>;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum StrataError {
    // ---- input / contract ----
    #[error("invalid argument: {what}")]
    InvalidArgument { what: String, hint: Option<String> },

    #[error("not found: {entity}")]
    NotFound { entity: EntityRef },

    #[error("already exists: {entity}")]
    AlreadyExists { entity: EntityRef },

    #[error("unsupported: {0}")]
    Unsupported(&'static str),

    // ---- transactions ----
    #[error("transaction conflict: {reason:?}")]
    Conflict { reason: ConflictReason },

    #[error("transaction state error: {kind:?}")]
    TxState { kind: TxStateKind },

    #[error("transaction timed out after {elapsed_ms}ms")]
    Timeout { elapsed_ms: u64, phase: TxPhase },

    #[error("operation cancelled")]
    Cancelled,

    // ---- the load-bearing variant ----
    /// The transaction MAY have committed durably. Strata guarantees no in-flight
    /// commit remains: the txn is either durable or will never become durable.
    /// Retry is safe only if the operation is idempotent.
    #[error("ambiguous commit at phase {phase:?}; outcome unknown")]
    AmbiguousCommit {
        phase: CommitPhase,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    },

    // ---- data integrity ----
    #[error("data corruption detected: {detail:?}")]
    Corruption {
        detail: CorruptionDetail,
        #[source]
        source: Option<StorageError>,
    },

    // ---- resource / environment ----
    #[error("resource exhausted: {kind:?}")]
    Resource { kind: ResourceKind },

    #[error("io error during {op:?}")]
    Io {
        op: IoOp,
        #[source]
        source: StorageError,
    },

    // ---- raw escalation when we can't classify ----
    #[error(transparent)]
    Storage(#[from] StorageError),

    // ---- engine-bug invariants ----
    #[error("internal engine error: {message}")]
    Internal {
        message: String,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    },

    #[error("api misuse: {0}")]
    Misuse(&'static str),
}

// Re-export selected nested types from storage to avoid double-naming.
pub use strata_storage::{CorruptionDetail, IoOp};

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum EntityRef {
    Table(String),
    Key { table: String, key: Vec<u8> },
    Snapshot(u64),
    Schema(String),
    Index(String),
}

#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub enum ConflictReason {
    WriteWrite,
    ReadWrite,
    Serialization,
    Deadlock,
    WriteTooOld,
    ReadUncertainty,
}

#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub enum TxStateKind {
    NoActiveTransaction,
    AlreadyInTransaction,
    InAbortedTransaction,
    ReadOnlyViolated,
    SavepointInvalid,
}

#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub enum CommitPhase { WalAppend, WalFsync, GroupCommitNotify, ManifestUpdate, IpcAck }

#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub enum TxPhase { Read, Write, Commit, Abort }

#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub enum ResourceKind { DiskFull, OutOfMemory, FdsExhausted, TooManyTransactions, BackpressureThrottled }
```

The retry/ambiguity contract methods — modeled directly on FDB:

```rust
impl StrataError {
    pub fn kind(&self) -> StrataErrorKind { /* derive from variant */ }
    pub fn code(&self) -> StrataErrorCode { /* stable u32 */ }

    /// Safe to retry without idempotency consideration.
    pub fn is_retryable_not_committed(&self) -> bool {
        matches!(self, Self::Conflict { .. })
            || matches!(self, Self::Resource { kind: ResourceKind::BackpressureThrottled, .. })
    }

    /// May or may not have committed durably. Retry only if idempotent.
    pub fn is_maybe_committed(&self) -> bool {
        matches!(self, Self::AmbiguousCommit { .. })
    }

    /// Composite (FDB convention).
    pub fn is_retryable(&self) -> bool {
        self.is_retryable_not_committed() || self.is_maybe_committed()
    }

    pub fn is_corruption(&self) -> bool {
        matches!(self, Self::Corruption { .. })
            || matches!(&self, Self::Storage(s) if s.is_corruption())
    }
}
```

The stable code:

```rust
/// Low byte: primary class. High bytes: extended subtype.
/// Codes are STABLE. New codes may be added but existing codes never change.
#[derive(Copy, Clone, Eq, PartialEq, Debug, Hash)]
pub struct StrataErrorCode(pub u32);

impl StrataErrorCode {
    pub const fn primary(self) -> u8 { (self.0 & 0xFF) as u8 }
    pub const fn class(self) -> StrataErrorClass {
        StrataErrorClass::from_primary(self.primary())
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
#[non_exhaustive]
#[repr(u8)]
pub enum StrataErrorClass {
    Ok            = 0x00,
    InvalidArg    = 0x01,
    NotFound      = 0x02,
    AlreadyExists = 0x03,
    Conflict      = 0x10, // retryable_not_committed
    TxState       = 0x11,
    Timeout       = 0x12,
    Cancelled     = 0x13,
    AmbiguousCommit = 0x14, // maybe_committed
    Resource      = 0x20,
    Io            = 0x21,
    Corruption    = 0x30,
    Internal      = 0x40,
    Misuse        = 0x41,
    Unsupported   = 0x50,
}

// Examples of extended codes:
// Conflict + Deadlock      = 0x0001_0010
// Conflict + Serialization = 0x0002_0010
// AmbiguousCommit + WalFsync = 0x0001_0014
// Corruption + IndexInvariant = 0x0003_0030
```

### 5.3 `strata-executor::Error` (boundary)

The executor is the wire/IPC boundary. It exposes a small fixed taxonomy modeled on the gRPC subset that applies to a single-node embedded DB, plus a structured `details` payload (etcd-style).

```rust
// strata-executor/src/error.rs
use serde::{Serialize, Deserialize};
use thiserror::Error;

pub type ExecutorResult<T> = Result<T, Error>;

#[derive(Debug, Error)]
#[error("{code:?}: {message}")]
pub struct Error {
    pub code: Code,
    pub message: String,
    pub details: Option<Details>,
    /// Server-side richness; never serialized to clients.
    #[source]
    pub source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
}

#[derive(Copy, Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
#[non_exhaustive]
#[repr(u8)]
pub enum Code {
    Ok                = 0,
    InvalidArgument   = 3,
    DeadlineExceeded  = 4,
    NotFound          = 5,
    AlreadyExists     = 6,
    PermissionDenied  = 7,
    ResourceExhausted = 8,
    FailedPrecondition = 9, // do not retry until state changes
    Aborted           = 10, // retry the whole txn
    OutOfRange        = 11,
    Unimplemented     = 12,
    Internal          = 13,
    Unavailable       = 14, // retry the call
    DataLoss          = 15, // corruption
    Unauthenticated   = 16,
    Cancelled         = 1,
    AmbiguousCommit   = 100, // Strata-specific: wire-stable extension
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Details {
    pub strata_code: u32,         // the rich StrataErrorCode for diagnostics
    pub retryable: Retryability,
    pub entity: Option<String>,
    pub hint: Option<String>,
}

#[derive(Copy, Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub enum Retryability {
    /// Safe to retry without idempotency.
    SafeRetry,
    /// Retry only if the operation is idempotent (commit ambiguity).
    IdempotentRetry,
    /// Do not retry until external state changes.
    DoNotRetry,
}
```

The boundary conversion (engine → executor) is a single, hand-written `From` — never `#[from]` — so the mapping is explicit and reviewable:

```rust
impl From<strata_engine::StrataError> for Error {
    fn from(e: strata_engine::StrataError) -> Self {
        use strata_engine::StrataError as E;
        let (code, retryability) = match &e {
            E::InvalidArgument { .. }   => (Code::InvalidArgument, Retryability::DoNotRetry),
            E::NotFound { .. }          => (Code::NotFound, Retryability::DoNotRetry),
            E::AlreadyExists { .. }     => (Code::AlreadyExists, Retryability::DoNotRetry),
            E::Unsupported(_)           => (Code::Unimplemented, Retryability::DoNotRetry),
            E::Conflict { .. }          => (Code::Aborted, Retryability::SafeRetry),
            E::TxState { .. }           => (Code::FailedPrecondition, Retryability::DoNotRetry),
            E::Timeout { .. }           => (Code::DeadlineExceeded, Retryability::DoNotRetry),
            E::Cancelled                => (Code::Cancelled, Retryability::DoNotRetry),
            E::AmbiguousCommit { .. }   => (Code::AmbiguousCommit, Retryability::IdempotentRetry),
            E::Resource { .. }          => (Code::ResourceExhausted, Retryability::SafeRetry),
            E::Io { .. } | E::Storage(_) => (Code::Unavailable, Retryability::SafeRetry),
            E::Corruption { .. }        => (Code::DataLoss, Retryability::DoNotRetry),
            E::Internal { .. } | E::Misuse(_) => (Code::Internal, Retryability::DoNotRetry),
        };
        Error {
            code,
            message: e.to_string(),
            details: Some(Details {
                strata_code: e.code().0,
                retryable: retryability,
                entity: None, // populated by call site if relevant
                hint: None,
            }),
            source: Some(Box::new(e)),
        }
    }
}
```

### 5.4 `strata-core` minimum

`core` should not own `StrataError`. It owns small, type-local validation:

```rust
// strata-core/src/key.rs
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum KeyParseError {
    #[error("key too long: {len} > {max}")]
    TooLong { len: usize, max: usize },
    #[error("invalid utf-8 in key")]
    InvalidUtf8,
    #[error("empty key")]
    Empty,
}
```

These are converted to `StrataError::InvalidArgument` at the engine boundary, with explicit `.map_err(|e| StrataError::InvalidArgument { what: e.to_string(), hint: None })`. **No global `From<core::KeyParseError> for StrataError`** — the conversion site is where the schema/operation context exists.

### 5.5 Trait/impl conventions

- `Debug + Display + std::error::Error + Send + Sync + 'static` on every public error.
- `#[non_exhaustive]` on every public error enum and every public reason enum.
- No `Clone`, no `PartialEq`, no `Hash` on error types (would force `Box<dyn Error>` payloads to support traits they cannot).
- Display: short, lowercase, no trailing punctuation, no embedded newlines, no `{source}` interpolation. Detail and hint live in dedicated fields. Postgres style.
- Backtrace: capture at the leaf (storage, on `IoError` and `Corruption` construction) using stable `std::backtrace::Backtrace::capture()`. Expose via inherent `fn backtrace(&self) -> Option<&Backtrace>`. Do not depend on nightly `provide()`.
- `source()`: the chain must reach the original `io::Error` or external cause. Engine wrapping must use `#[source]`, not Display interpolation.
- Error codes are stable; Display strings are not. Document this on `StrataError`'s rustdoc.

### 5.6 `#[from]` policy (concrete rules)

- **Allowed:** `StrataError::Storage(#[from] StorageError)` — explicit raw escalation variant for storage errors that the engine genuinely cannot classify further. This is the only blanket conversion permitted.
- **Forbidden:** `From<io::Error>` for `StorageError`. Every IO site must wrap with `IoOp` and path.
- **Forbidden:** `From<StorageError>` for `executor::Error` — the boundary must go through `StrataError` first, so that storage errors get database-semantic interpretation.
- **Forbidden:** `From<engine::StrataError>` via `#[from]` on `executor::Error` — must be the explicit hand-written `From` impl shown in §5.3, so that retryability classification stays reviewable.
- **Forbidden everywhere:** `From<anyhow::Error>`, `From<Box<dyn Error>>` into any library error type.

---

## 6. Migration strategy for CO3

The migration must avoid two specific failure modes: (a) breaking the executor's public boundary mid-transit, and (b) creating a "dual-monolith" period where both the legacy `core::StrataError` and a new `engine::StrataError` exist with overlapping responsibilities. The phased plan:

**Phase 0 — Freeze and audit (week 1).** Freeze the legacy `core::StrataError` enum: no new variants, no new `From` impls. Tag every existing `StrataResult` use site with `#[allow(deprecated)]` or a tracking attribute. Run a workspace-wide audit: `rg -t rust 'StrataResult|StrataError'` per crate, classify each occurrence as (a) genuinely-engine-domain, (b) actually storage-domain, (c) actually executor-boundary, (d) actually validation. The output of this audit is the migration backlog.

**Phase 1 — Re-home the parent (week 2).** Create `strata-engine::StrataError` with the design in §5.2. Have it re-export `strata_core::StrataError` *as a temporary type alias* so existing callers compile: `pub use strata_core::StrataError;` inside engine, with a `#[deprecated]` note pointing to the new variants. Critically: do not yet move any variants. The engine crate now *owns* the symbol publicly even though the definition still lives in core.

**Phase 2 — Move the definition (weeks 3–4).** Move the enum definition from `core` to `engine`. `core` now re-exports it for backwards compat: `pub use strata_engine::StrataError;` — this is a temporary inversion that lets old callers compile without churn. Add new variants needed by the design (`AmbiguousCommit`, `Conflict { reason }`, etc.) and the `is_retryable*`/`code()` methods. At this point the engine is the source of truth; core is a forwarding shim.

**Phase 3 — Boundary hardening (weeks 4–5).** Define `executor::Error` per §5.3 and the explicit `From<StrataError> for executor::Error` mapping. Audit the executor's public surface: every public function whose return type was `StrataResult<T>` becomes `executor::Result<T>`. **This is the only public-API break and must be done in one atomic PR per public command.** Add a feature flag `legacy-strata-error` if external consumers need a temporary bridge — but plan to remove it within one minor version.

**Phase 4 — Storage rationalization (weeks 5–6).** Audit `StorageError` against the design in §5.1. Remove any `From<StrataError>`-for-storage impls (storage cannot depend on engine). Ensure storage carries `IoOp` and path on every IO variant. Where leaf crates have `RecoveryError`, `VectorError`, `CommitError`: collapse `RecoveryError` and `CommitError` into nested reason types under `StrataError::Corruption` / `StrataError::AmbiguousCommit`; keep `VectorError` only if it's genuinely a public type-local validation error in `core`, otherwise fold it into `StrataError::InvalidArgument { what }`.

**Phase 5 — Drop the core forwarder (week 7).** Delete `core::StrataError`, `core::StrataResult`. The compiler will catch every remaining caller. Replace with an explicit import from `engine`. Remove the `#[deprecated]` markers added in Phase 1.

**Phase 6 — Stable codes published (week 8).** Document `StrataErrorCode` and `StrataErrorClass` as stable. Add tests asserting numeric values (golden files). From this point, codes are SemVer-stable; new codes may be added but existing codes are immutable.

**Audit script (concrete, runnable):**
```bash
for crate in core storage engine executor cli; do
  echo "=== $crate ==="
  rg -t rust -l 'StrataResult|StrataError|crate::Error' "crates/$crate" \
    | xargs -I{} rg -c 'StrataResult|StrataError' {} | sort -t: -k2 -nr
done
```
The Phase 0 deliverable is this audit output annotated with "stays / move-to-engine / move-to-storage / move-to-executor / fold-into-reason."

**Rules to forbid during migration:**
- No new `StrataResult` outside `engine`.
- No new `From` impls on the legacy core enum.
- No new public functions returning `core::StrataError`.
- No silent `?`-coercion across layer boundaries (use `.map_err`).

---

## 7. Decision matrix

| Strategy | Benefits | Costs | Failure modes | Ergonomics | Long-term architecture | Public API stability |
|---|---|---|---|---|---|---|
| **A. One giant `core::StrataError`** | trivial `?` everywhere; one type to learn; minimal up-front design | every layer transitively depends on every concern; documentation lies (every fn appears to fail every way); cannot extract `storage` or `engine` as standalone crates; new failure mode forces touching every layer | **junk-drawer drift** — sled author's documented experience: "dozens and dozens of bugs … `?` accidentally pushing a local concern into a caller that can't deal with it"; retryability semantics get diluted across unrelated variants; corruption errors race with parse errors in the same `match` | great initially, degrades sharply as the variant count grows past ~15 | actively hostile to the planned `core/storage/engine/executor/cli` re-layering — the central type *is* the layer violation | poor — every variant addition is a potential breaking change; `#[non_exhaustive]` mitigates `match` breakage but cannot prevent semantic drift; conflicts in CI between unrelated subsystems |
| **B. No parent, only crate-local errors** | maximum locality; each crate independently versionable; clearest layer boundaries; easy to extract a crate into its own repository | every layer must define `From` or `.map_err` for every cross-layer call; users must learn N error types; no canonical retry/ambiguity surface; each crate re-invents `is_retryable` differently or not at all; generic retry middleware impossible to write; testing assertions fragment per crate | **proliferation drift** — `RecoveryError`, `VectorError`, `CommitError`, `WalError`, `IndexError` all become public surfaces with their own evolution and inconsistent retryability; clients write nested `match`-of-`match`-of-`match`; ambiguous-commit semantics cannot be expressed cleanly because no one type owns "the database failed" | high friction at every boundary; verbose call sites; encourages workarounds like `Box<dyn Error>` that defeat the typed-error premise | excellent per-crate, terrible aggregate — clients depend on the *combination*, which is the volatile surface |
| **C. Engine-owned `StrataError` + crate-local layer errors beneath** *(recommended)* | one canonical retry/ambiguity/code surface; `core` and `storage` independently extractable; redb-validated multi-type layout beneath; FDB-style predicates available at engine; small stable executor boundary; obvious home for new failure modes; matches the planned re-layering 1:1 | requires explicit `From` at the engine boundary and explicit `From` at the executor boundary (no `#[from]` shortcuts); requires discipline to keep `StorageError` from leaking past engine; up-front cost in the migration | **classification drift** — engineers add a variant to `StrataError` that should have been a nested `*Reason`; mitigated by code review checklists and the rule that "if the variant carries no extra structured context, it's probably a Reason on an existing variant" | excellent for callers (one `match`), good for engine authors (clear ownership), some friction at boundaries (explicit conversion) — but the friction *is the point*: it forces semantic interpretation | maps directly onto core/storage/engine/executor/cli; supports later extraction of any layer; supports adding new layers (e.g. a clustering layer above engine) without reshaping the parent | excellent — engine variants and executor codes are independently SemVer'd; storage is internal-only and can churn; codes are stable across versions |

**Verdict:** C wins on every long-term axis. A wins only on near-term inertia. B wins only on isolation but loses retry/ambiguity coherence — which for a database is unacceptable.

---

## 8. Final recommendation

**Adopt Strategy C: an engine-owned `StrataError` parent with a crate-local layer-error beneath it, modeled jointly on redb (multi-type layering), FoundationDB (retry predicates and ambiguous-commit), Postgres (code classes and primary/detail/hint), SQLite (primary + extended code encoding), and gRPC (small stable boundary code set).**

**Why this wins:**

1. **It rejects both failure modes named in the brief simultaneously.** No core-owned mega-monolith (storage and core stay decoupled from engine semantics). No proliferation chaos (one canonical parent at engine, with named ownership rules for what stays nested). The architecture has a single answer to "where does this error go?" for every plausible failure mode in a database.

2. **It encodes the database-specific invariants the Rust ecosystem mostly ignores.** Retryability, idempotent retry vs unconditional retry, ambiguous commit, corruption-vs-IO, recovery-vs-conflict — these are first-class in `StrataError` and absent from sled, redb, sqlx, rusqlite. Every transactional database eventually needs them; designing them in now is cheaper than retrofitting.

3. **It splits the public contract from the internal taxonomy at the right place.** `StrataError`'s variants can evolve with engine internals; the executor's `Code` and `StrataErrorCode(u32)` are SemVer-stable wire contracts that the executor boundary translates into. This is exactly the Postgres/SQLite split: codes for clients, structured fields for diagnostics.

4. **It scales with the planned re-layering.** When a future layer is added above engine (e.g. a replication or clustering layer), it adds its own error type and an explicit `From<StrataError>`. When a layer is extracted (e.g. `storage` becomes its own crate), nothing in `StrataError` needs to change.

### Recommended Strata rules (concrete, adoptable)

**Ownership rules:**
- `StrataError` lives in `strata-engine`. Period. `core` may not define a parent error type.
- `StorageError` lives in `strata-storage` and never depends on `engine` or higher.
- `executor::Error` lives in `strata-executor` and is the only error type exposed to wire/IPC clients.
- `cli` uses `anyhow::Result` and only `anyhow::Result`. No other crate may depend on `anyhow`.

**Type-shape rules:**
- Every public error enum is `#[derive(Debug, thiserror::Error)] #[non_exhaustive]`.
- Every public reason enum is `#[non_exhaustive]`.
- Every public error type is `Send + Sync + 'static`.
- No `Clone`, `PartialEq`, or `Hash` on error types.
- `Display` is lowercase, no trailing punctuation, no `{source}` interpolation, no embedded newlines.
- `source()` reaches the original cause.
- Backtrace captured at storage leaves; exposed via inherent accessor; no nightly `provide()`.

**Conversion rules:**
- `#[from]` is permitted **only** for `StrataError::Storage(#[from] StorageError)`. All other cross-layer conversions are explicit `.map_err`.
- No `From<io::Error>` for any Strata error; every IO site wraps with `IoOp` + path.
- `executor::Error` is constructed via the explicit hand-written `From<StrataError>` in `executor`, never via `#[from]`.
- No `From<anyhow::Error>` or `From<Box<dyn Error>>` into any library error type.

**Retry/ambiguity rules:**
- `StrataError::AmbiguousCommit` is a top-level variant with the documented invariant "no in-flight commit remains."
- `StrataError::Timeout` is distinct and carries no such invariant; this distinction is documented per the FDB precedent.
- `is_retryable()`, `is_maybe_committed()`, `is_retryable_not_committed()` are the public retry contract; clients should not pattern-match on variants for retry decisions.
- A canonical `retry_loop` helper is provided in `engine` (not user-supplied) that respects these predicates with bounded backoff. It does *not* auto-retry `is_maybe_committed()` errors unless the caller has flagged the closure as idempotent.

**Code rules:**
- `StrataErrorCode(u32)` is stable: low byte = primary class, high bytes = extended subtype.
- Primary classes are an `#[non_exhaustive]` enum; new classes may be added but existing values never change.
- Golden tests assert numeric stability of every published code.
- Display strings are explicitly *not* part of the contract; rustdoc says so.

### Things Strata must forbid

- A `StrataError` definition in `core`.
- A `From` impl that lifts a leaf error (`io::Error`, `serde_json::Error`, `bincode::Error`, etc.) into `StrataError` without going through `StorageError` or explicit engine-level wrapping.
- An "Other(String)" or "Generic(String)" variant on any public error type.
- Pattern-matching on `Display` output, anywhere, ever.
- New error variants whose name is the name of a subsystem (`WalError`, `LsmError`) — these are nested reasons, not top-level variants.
- Returning `anyhow::Result<T>` from any function in `core`, `storage`, `engine`, or `executor`.
- Re-exporting `thiserror::Error` from a public Strata API (it's an implementation detail).
- Adding a new top-level `StrataError` variant without simultaneously assigning it a stable `StrataErrorCode` and a retryability classification.
- Letting storage-layer details (page IDs, offsets, fsync errnos) appear in the executor's wire `Code` or `message`. They belong in `Details` payloads or trace logs only.

The single most important rule: **the engine owns the database's vocabulary of failure. Every layer above translates into that vocabulary; every layer below feeds raw conditions into it. The parent error is not a junk drawer — it is the codified semantics of the database itself.**