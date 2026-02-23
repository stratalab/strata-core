# Strata Document Ingestion

**Status:** Proposal
**Author:** Design Team
**Date:** 2026-02-23

---

## 1. Problem Statement

AI agents produce markdown files on disk as their primary output — thinking traces, research notes, generated documentation. Users maintain knowledge bases of markdown: personal notes, meeting transcripts, project docs. Today, getting this content into Strata for search requires explicit `kv.put()` calls for every document, which no agent or workflow does automatically.

This creates three concrete gaps:

- **No automatic ingestion** — A user with 500 markdown files in `~/notes` must write code to read each file and call `kv.put()`. There is no "watch this folder" primitive.
- **No chunking support** — A 10,000-word document stored as one KV entry produces one BM25 entry and one vector embedding. `score_top_k()` (`crates/engine/src/search/index.rs:464`) returns the whole document when only one paragraph is relevant. Fine-grained retrieval is impossible.
- **No change tracking** — If a file changes on disk, Strata doesn't know. The user must re-ingest manually. There is no diffing, no hash comparison, no file watching.

### Concrete Scenario

```
~/notes/ contains 200 markdown files totaling 1.2M words.

Without document ingestion:
  - User writes a script to read all 200 files → 200 kv.put() calls
  - Each file stored as single KV entry → 200 BM25 documents
  - Search for "authentication flow" returns entire 8,000-word design doc
  - User edits 3 files → must manually re-run script
  - No way to know which files changed without external tooling

With document ingestion:
  - db.watch("~/notes", "notes", "**/*.md")
  - 200 files → ~2,400 chunks → 2,400 BM25 documents + 2,400 embeddings
  - Search for "authentication flow" returns the 512-token chunk about auth
  - User edits 3 files → SHA-256 detects changes → only 3 files re-chunked
  - File watcher catches changes within 500ms
```

### What Already Works

The building blocks exist. `batch_put()` (`crates/engine/src/primitives/kv.rs:227`) provides atomic multi-key writes. The auto-embed hook (`crates/executor/src/handlers/embed_hook.rs`) fires on every KV write, extracting text and submitting embeddings via `BackgroundScheduler` at `TaskPriority::Normal`. BM25 indexing happens inline during `put()`. `sha2` is already a dependency (`crates/engine/Cargo.toml:30`). `tokenize()` (`crates/engine/src/search/tokenizer.rs:53`) provides consistent token counting. We need ingestion, chunking, and file watching — not new search or embedding infrastructure.

---

## 2. Inspiration / Industry Context

### QMD (tobi/qmd)

QMD is a purpose-built markdown ingestion system for AI agents. Key design choices:

- **Collections with glob patterns**: Named groups of watched paths, each with a glob filter (e.g., `**/*.md`)
- **SHA-256 change detection**: Content-addressable storage; skip files whose hash hasn't changed
- **Scored break-point chunking**: Heading = 100, code fence = 80, paragraph = 20. Pick the highest-scored break point within a look-back window from the target size
- **Chunk parameters**: 900 tokens per chunk, 15% overlap between adjacent chunks
- **Heading context propagation**: Prepend ancestor headings to each chunk so search results have structural context

QMD's chunking algorithm is the most directly applicable prior art. Its scored break-point approach respects markdown structure (never splits mid-heading, mid-code-block) while keeping chunks close to target size.

### Obsidian / Notion / Apple Notes

All three watch local files or sync folders automatically:

- **Obsidian**: Watches a vault directory, detects changes via filesystem events, re-indexes on change. No chunking — indexes whole files for link resolution and search.
- **Notion**: Syncs blocks (their unit of content) in real-time. Each block is independently searchable.
- **Apple Notes**: Watches for changes via CloudKit, indexes content for Spotlight integration.

The common pattern: users point the tool at a folder, and it handles the rest. No manual import step.

### LlamaIndex / LangChain

Both provide document loaders with configurable chunking:

- **LlamaIndex**: `MarkdownNodeParser` splits on headings, `SentenceSplitter` for general text. Chunk size and overlap configurable.
- **LangChain**: `MarkdownHeaderTextSplitter` splits on heading hierarchy, `RecursiveCharacterTextSplitter` for fallback. Metadata propagation from headings to chunks.

Key insight from both: heading-based splitting produces more semantically coherent chunks than fixed-size splitting, but needs a fallback for long sections without subheadings.

---

## 3. Design

### Collections

A **collection** is a named group of watched filesystem paths. Collections map directly to KV spaces — the collection name becomes the space name, so all documents in a collection are co-located for search.

**Configuration** (`strata.toml`):

```toml
[[documents.collections]]
name = "notes"
path = "~/notes"
pattern = "**/*.md"
context = "Personal notes and meeting transcripts"

[[documents.collections]]
name = "docs"
path = "~/projects/myapp/docs"
pattern = "**/*.md"
context = "Project documentation"
```

**Programmatic API**:

```rust
// Watch a directory as a named collection
db.watch("~/notes", "notes", "**/*.md")?;

// One-shot ingestion of a single file
db.ingest_document("notes", path, content)?;
```

Each collection field:

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `name` | `String` | Yes | — | Collection name, becomes KV space name |
| `path` | `String` | Yes | — | Root directory to watch |
| `pattern` | `String` | No | `"**/*.md"` | Glob pattern for file filtering |
| `context` | `String` | No | — | Human-readable description (stored in metadata) |

### Storage Model

Documents, chunks, and metadata all live as KV entries in the collection's space. This reuses the existing KV → BM25 → auto-embed pipeline with zero new indexing code.

**Key schema**:

| Entry | KV Space | KV Key | Value |
|-------|----------|--------|-------|
| Full document | `{collection}` | `{relative_path}` | Raw markdown content |
| Chunk N | `{collection}` | `{relative_path}::chunk::{seq}` | Chunk text (with heading context) |
| Metadata | `{collection}` | `{relative_path}::meta` | JSON: hash, mtime, chunk_count, title |

**Example** for file `notes/meeting-2026-02-20.md` in collection `"notes"`:

```
Space: "notes"
Keys:
  "meeting-2026-02-20.md"              → full markdown content
  "meeting-2026-02-20.md::chunk::0"    → "# Meeting Notes\n## Attendees\n..."
  "meeting-2026-02-20.md::chunk::1"    → "## Action Items\n- Fix auth bug..."
  "meeting-2026-02-20.md::chunk::2"    → "## Discussion\n### Performance\n..."
  "meeting-2026-02-20.md::meta"        → {"hash":"ab3f...","mtime":1708646400,"chunk_count":3,"title":"Meeting Notes"}
```

**Why this model works**: Each chunk KV write triggers the existing auto-embed hook (`embed_hook.rs:79-88`) and BM25 `index_document()` automatically. No new embedding or indexing code needed — chunks are just KV entries.

**Retrieval path**: Search returns a chunk → strip `::chunk::N` suffix → retrieve parent document by base key. The `::meta` entry provides chunk count for iteration.

### Chunking Algorithm

QMD-inspired scored break-point chunking with markdown structure awareness.

**Break point scores**:

| Markdown Element | Score | Rationale |
|-----------------|-------|-----------|
| `# H1` | 100 | Strongest structural boundary |
| `## H2` | 90 | Major section break |
| `### H3` | 80 | Subsection break |
| `#### H4` | 70 | Minor section |
| `##### H5` | 60 | Rare, but still structural |
| `###### H6` | 50 | Rare, but still structural |
| Code fence (` ``` `) | 80 | Code blocks are self-contained units |
| Horizontal rule (`---`) | 60 | Explicit section divider |
| Paragraph break (blank line) | 20 | Natural text boundary |
| List item (`- `, `* `, `1. `) | 5 | Weak boundary within lists |
| Newline | 1 | Last-resort split point |

**Algorithm**:

1. Scan document, counting tokens using `tokenize()` (`crates/engine/src/search/tokenizer.rs:53`) for consistency with BM25 token counting
2. When accumulated tokens exceed target chunk size (default 512 tokens), enter look-back window
3. Within the look-back window, find all break points and their scores
4. Select the break point with the highest score, using squared-distance decay to favor proximity to the target size: `effective_score = score / (1 + (distance / window_size)²)`
5. Emit chunk, advance past break point, continue

**Code fence protection**: Track open/close of ` ``` ` blocks. Never emit a break point inside an open code fence — the entire fenced block stays in one chunk (even if it exceeds target size).

**Heading context propagation**: Prepend the nearest ancestor heading(s) to each chunk. If a chunk falls under `## API Reference` → `### Authentication`, prepend those headings so the chunk is self-contained for search:

```
## API Reference
### Authentication

The auth endpoint accepts a Bearer token in the Authorization header...
```

This ensures BM25 and embedding both capture the structural context, not just the leaf text.

**Overlap**: 15% of target chunk size (default: ~77 tokens). The overlap region is appended from the end of the previous chunk to the start of the next, ensuring no context is lost at boundaries.

### File Watching

**Crate**: `notify` (cross-platform: kqueue on macOS, inotify on Linux, ReadDirectoryChangesW on Windows).

**Architecture**:

```
notify::RecommendedWatcher
    → OS filesystem events (create, modify, delete, rename)
    → strata-fswatcher thread (dedicated, named)
    → debounce buffer (500ms window)
    → BackgroundScheduler (TaskPriority::Normal)
    → ingestion task
```

**Debouncing**: Editors perform atomic saves as delete + create (or write + rename). A 500ms debounce window coalesces these into a single "modified" event. The debounce buffer follows the `EmbedBuffer` pattern (`embed_hook.rs:77-108`): accumulate events in a `Vec`, flush when the timer fires or the buffer exceeds a size threshold.

**Event batching**: After the debounce window, submit one `BackgroundScheduler` task per batch of changed files rather than one task per file. This amortizes scheduler overhead for bulk changes (e.g., `git checkout` touching many files).

**Watcher thread lifecycle**:
1. Spawned during `Database::open()` if any document collections are configured
2. Named `strata-fswatcher` (follows `strata-maint` naming convention from maintenance design)
3. Runs until `Database::close()` sets the shutdown flag
4. On shutdown: stop `notify` watcher, drain pending debounce buffer, submit final batch

### Change Detection

Two-level check to minimize I/O:

1. **mtime fast-path**: Compare file's `mtime` against the value stored in `::meta`. If unchanged, skip entirely. This avoids reading the file at all.
2. **SHA-256 correctness**: If `mtime` changed, read the file, compute SHA-256 (`sha2` crate, already in `crates/engine/Cargo.toml:30`), compare against stored hash. If hash matches (mtime changed but content didn't — e.g., `touch`), skip re-ingestion.

This two-level approach is the same strategy QMD uses. The mtime check handles 90%+ of "nothing changed" cases with zero I/O; SHA-256 handles the remaining edge cases correctly.

### Update Semantics

| Event | Action |
|-------|--------|
| **Create** | Read file → compute SHA-256 → chunk with `MarkdownChunker` → `batch_put()` doc + chunks + meta → auto-embed triggers on each chunk |
| **Modify** | Read file → compute SHA-256 → if hash unchanged, skip → delete old chunk KVs → re-chunk → `batch_put()` new chunks + update meta |
| **Delete** | Delete doc KV + all chunk KVs + meta KV → `remove_document()` fires for BM25 cleanup → auto-embed delete path cleans vectors |
| **Rename** | Treat as delete old path + create new path (simplest, avoids complex rename tracking) |

**Modify detail**: On update, the chunk count may change (e.g., adding a section increases chunks). The old chunk KVs are deleted first (using `chunk_count` from `::meta`), then new chunks are written. This ensures no orphaned chunks remain.

**Batch efficiency**: On initial directory scan (startup or new collection), use `batch_put()` (`crates/engine/src/primitives/kv.rs:227`) for all chunks across all files. This produces a single atomic transaction per batch rather than one transaction per chunk.

### Graceful Degradation

Ingestion is **best-effort** — following the same philosophy as the maintenance coordinator design:

- **File read errors** (permissions, encoding): Log `tracing::warn!`, skip file, retry on next scan or filesystem event
- **Watcher errors** (too many watches, OS limit): Fall back to periodic full scan at `scan_interval_secs` (default 300s)
- **Chunking errors** (malformed markdown): Treat entire file as a single chunk — worst case is reduced retrieval granularity, not data loss
- **Scheduler backpressure** (`BackpressureError`): Log warning, skip batch, retry on next debounce flush (same pattern as `embed_hook.rs:170`)

Ingestion failures never propagate to the user as errors. The system degrades gracefully from real-time file watching → periodic scanning → manual ingestion.

---

## 4. Architecture

### Key Structs

```rust
/// Database extension that owns the file watcher and collection configs.
/// Created during Database::open() if any document collections are configured.
pub struct DocumentWatcher {
    collections: Vec<DocumentCollection>,
    watcher: Option<notify::RecommendedWatcher>,
    debounce_buffer: Arc<Mutex<Vec<FsEvent>>>,
    shutdown: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
    db: Arc<Database>,
}

/// Configuration for a single watched collection.
pub struct DocumentCollection {
    pub name: String,           // Collection name = KV space name
    pub path: PathBuf,          // Root directory to watch
    pub pattern: String,        // Glob pattern (default "**/*.md")
    pub context: Option<String>, // Human-readable description
}

/// Per-document metadata stored as JSON in the ::meta KV entry.
#[derive(Serialize, Deserialize)]
pub struct DocumentMeta {
    pub hash: String,           // SHA-256 hex digest
    pub mtime: u64,             // File mtime (epoch seconds)
    pub chunk_count: usize,     // Number of chunks produced
    pub title: Option<String>,  // First H1, if present
}

/// Stateless markdown chunking engine.
/// Scored break points, code fence protection, heading context propagation.
pub struct MarkdownChunker {
    pub target_tokens: usize,   // Target chunk size (default 512)
    pub overlap_pct: f32,       // Overlap percentage (default 0.15)
}
```

### Data Flow

```
filesystem event
    → notify::RecommendedWatcher (OS-level: kqueue/inotify)
    → strata-fswatcher thread
    → debounce buffer (500ms coalesce window)
    → BackgroundScheduler::submit(TaskPriority::Normal, ingestion_task)
    → ingestion task executes:
        1. Read file from disk
        2. Compute SHA-256, compare to stored hash in ::meta KV
        3. If unchanged → skip (fast path)
        4. If changed → MarkdownChunker::chunk(content)
        5. Delete old chunk KVs if updating (using ::meta chunk_count)
        6. batch_put() new doc + chunks + meta KV entries
        7. Auto-embed hook fires on each chunk KV write (embed_hook.rs)
        8. BM25 index_document() fires on each chunk KV write (inline)
```

### Search Integration

No changes to the search path. Chunks are normal KV entries, so they appear in `score_top_k()` results like any other document. The caller resolves chunk results to parent documents:

```rust
// After search returns a ScoredDocId pointing to "design.md::chunk::3"
let key = scored.entity_ref.key;
if let Some(base) = key.split("::chunk::").next() {
    // base = "design.md" — fetch full document or adjacent chunks
    let full_doc = kv.get(space, base)?;
    let meta: DocumentMeta = kv.get(space, &format!("{base}::meta"))?;
}
```

### Lifecycle

1. **`Database::open()`**: Read `[documents]` config from `strata.toml`. If any collections are defined, create `DocumentWatcher`.
2. **Startup scan**: Walk all configured paths, compare mtime/hash against stored metadata, ingest new or changed files.
3. **Steady state**: `notify` watcher delivers filesystem events → debounce → ingest.
4. **`Database::close()`**: Set shutdown flag → stop `notify` watcher → drain debounce buffer → submit final ingestion batch → `BackgroundScheduler::drain()` handles in-flight tasks.

---

## 5. Configuration

### DocumentsConfig

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentsConfig {
    /// Target tokens per chunk.
    #[serde(default = "default_chunk_size")]
    pub chunk_size_tokens: usize,       // 512

    /// Overlap between adjacent chunks as percentage of chunk_size_tokens.
    #[serde(default = "default_overlap")]
    pub chunk_overlap_pct: f32,         // 0.15

    /// Debounce window for filesystem events (milliseconds).
    #[serde(default = "default_debounce")]
    pub debounce_ms: u64,               // 500

    /// Fallback periodic scan interval when watcher is unavailable (seconds).
    #[serde(default = "default_scan_interval")]
    pub scan_interval_secs: u64,        // 300

    /// Watched collections.
    #[serde(default)]
    pub collections: Vec<DocumentCollectionConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentCollectionConfig {
    pub name: String,
    pub path: String,
    #[serde(default = "default_pattern")]
    pub pattern: String,                // "**/*.md"
    #[serde(default)]
    pub context: Option<String>,
}
```

### strata.toml Integration

Nested under `[documents]` in the existing config, alongside `[maintenance]`:

```toml
durability = "standard"
auto_embed = false

[documents]
chunk_size_tokens = 512
chunk_overlap_pct = 0.15
debounce_ms = 500
scan_interval_secs = 300

[[documents.collections]]
name = "notes"
path = "~/notes"
pattern = "**/*.md"
context = "Personal notes and meeting transcripts"

[[documents.collections]]
name = "docs"
path = "~/projects/myapp/docs"
pattern = "**/*.md"
context = "Project documentation"
```

### StrataConfig Changes

Add `DocumentsConfig` as an optional field in `StrataConfig` (`crates/engine/src/database/config.rs:66`):

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrataConfig {
    #[serde(default = "default_durability_str")]
    pub durability: String,
    #[serde(default)]
    pub auto_embed: bool,
    // ... existing fields ...

    /// Document ingestion configuration. Empty collections = disabled.
    #[serde(default)]
    pub documents: DocumentsConfig,
}
```

No collections configured = no watcher spawned = zero overhead for users who don't use this feature.

### Default Justification

| Parameter | Default | Rationale |
|-----------|---------|-----------|
| `chunk_size_tokens` | 512 | QMD uses 900; 512 aligns with common embedding model context windows (512-1024 tokens) and produces more granular retrieval |
| `chunk_overlap_pct` | 0.15 | QMD uses 15%; standard in LlamaIndex/LangChain. Prevents context loss at chunk boundaries |
| `debounce_ms` | 500 | Covers atomic-save patterns (delete + create). VS Code, Vim, and Emacs all complete saves within 100ms; 500ms provides 5x margin |
| `scan_interval_secs` | 300 | 5-minute fallback matches maintenance coordinator's GC interval. Frequent enough for missed events, rare enough to be negligible overhead |

---

## 6. Implementation Roadmap

### Phase 1: Core (Chunking + Storage)

**Goal:** Implement the `MarkdownChunker` and document ingestion pipeline. No file watching yet — manual ingestion via API.

**Tasks:**
1. Implement `MarkdownChunker` with scored break-point algorithm
   - Break point scoring (headings, code fences, paragraphs, list items)
   - Code fence protection (never split inside ` ``` ` blocks)
   - Target token size with look-back window and decay-weighted selection
   - Token counting via `tokenize()` for BM25 consistency
2. Implement `DocumentMeta` with SHA-256 hashing and serde
3. Implement document ingestion: read → hash → chunk → `batch_put()` as KV entries
   - Full document stored at `{path}` key
   - Chunks stored at `{path}::chunk::{seq}` keys
   - Metadata stored at `{path}::meta` key
4. Implement update semantics: create, modify (delete old chunks + re-chunk), delete (remove all entries)
5. API: `db.ingest_document(collection, path, content)` for programmatic use
6. Add `DocumentsConfig` and `DocumentCollectionConfig` to `StrataConfig`
7. Tests: chunking correctness (headings, code fences, overlap, long sections), storage model (key schema, metadata), update semantics (create/modify/delete idempotency)

### Phase 2: File Watching

**Goal:** Automatic ingestion via filesystem watching. Users configure collections and Strata handles the rest.

**Tasks:**
1. Add `notify` crate dependency (behind `document-watch` feature flag)
2. Implement `DocumentWatcher` with debounced event handling
   - Dedicated `strata-fswatcher` thread
   - 500ms debounce window following `EmbedBuffer` accumulate-flush pattern
   - Event batching: one scheduler task per debounce window, not per file
3. Implement change detection: mtime fast-path → SHA-256 correctness check
4. Implement initial startup scan: walk configured paths, ingest new/changed files
5. Wire `DocumentWatcher` into `Database::open()` (create if collections configured) and `Database::close()` (shutdown + drain)
6. Implement `db.watch(path, name, pattern)` for runtime collection creation
7. Add `[documents]` section to default `strata.toml` template (commented out)
8. Tests: watch create/modify/delete events, debouncing coalesces rapid writes, graceful error handling (permission denied, missing directory), shutdown drains pending work

### Phase 3: Polish

**Goal:** Production readiness — heading context, fallback scanning, observability.

**Tasks:**
1. Heading context propagation: prepend ancestor headings to each chunk
2. Periodic fallback scan: when `notify` watcher fails, fall back to `scan_interval_secs` polling
3. `DocumentStatus` command: collections watched, files tracked, chunks indexed, last scan time, errors
   - Follows `EmbedStatusInfo` pattern (`embed_hook.rs`)
   - Exposed via `Command::DocumentStatus` → `Output::DocumentStatus`
4. Performance: parallel chunking for large directories (submit per-file chunking tasks to `BackgroundScheduler`)
5. Chunk result resolution helper: `resolve_chunk_to_document(entity_ref)` → parent document + context
