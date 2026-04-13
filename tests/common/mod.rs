//! Shared test utilities for all integration test suites.
//!
//! Consolidated from per-milestone test_utils.rs files.
//! Import via `mod common;` from any test's main.rs.

#![allow(dead_code)]
#![allow(unused_imports)]

use std::collections::{BTreeMap, HashMap};
use std::fs::{self, OpenOptions};
use std::hash::{Hash, Hasher};
use std::io::{Seek, SeekFrom, Write as IoWrite};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
pub use strata_core::{BranchId, JsonPath, JsonValue, Value, Version};
pub use strata_engine::{
    BranchIndex, Database, DatabaseBuilder, EventLog, JsonStore, KVStore, SearchSubsystem,
    StrataConfig,
};
pub use strata_graph::GraphStore;
pub use strata_vector::{DistanceMetric, StorageDtype, VectorConfig, VectorStore, VectorSubsystem};
use tempfile::TempDir;

/// Fresh `DatabaseBuilder` wired with the production subsystems
/// (`GraphSubsystem` + `VectorSubsystem` + `SearchSubsystem`), used by
/// all test-helper open paths so integration tests exercise the same
/// recovery pipeline the executor uses in production.
///
/// `GraphSubsystem` must be first because its `initialize()` method
/// registers the per-database graph merge handler and DAG hook.
fn test_db_builder() -> DatabaseBuilder {
    DatabaseBuilder::new()
        .with_subsystem(strata_graph::GraphSubsystem)
        .with_subsystem(VectorSubsystem)
        .with_subsystem(SearchSubsystem)
}

/// Open a test database at the given path with production subsystems.
/// Used for recovery tests that need to reopen at a specific path.
pub fn test_db_open(path: &std::path::Path) -> Arc<Database> {
    test_db_builder()
        .open(path)
        .expect("Failed to open test database")
}

/// Create a StrataConfig with always durability mode.
pub fn always_config() -> StrataConfig {
    StrataConfig {
        durability: "always".to_string(),
        ..StrataConfig::default()
    }
}

static COUNTER: AtomicU64 = AtomicU64::new(0);

// ============================================================================
// TestDb - Full-featured test database wrapper
// ============================================================================

/// Test database wrapper with access to all primitives and durability support.
pub struct TestDb {
    pub db: Arc<Database>,
    pub dir: TempDir,
    pub branch_id: BranchId,
}

impl TestDb {
    /// Create a new test database with standard durability (default for tests).
    pub fn new() -> Self {
        let dir = tempfile::tempdir().expect("Failed to create temp dir");
        let db = test_db_builder()
            .open(dir.path())
            .expect("Failed to create test database");
        let branch_id = BranchId::new();
        TestDb { db, dir, branch_id }
    }

    /// Create a test database with always durability.
    pub fn new_strict() -> Self {
        let dir = tempfile::tempdir().expect("Failed to create temp dir");
        let config = always_config();
        let db = test_db_builder()
            .open_with_config(dir.path(), config)
            .expect("Failed to create test database");
        let branch_id = BranchId::new();
        TestDb { db, dir, branch_id }
    }

    /// Create an in-memory test database.
    pub fn new_in_memory() -> Self {
        let dir = tempfile::tempdir().expect("Failed to create temp dir");
        let db = test_db_builder()
            .cache()
            .expect("Failed to create test database");
        let branch_id = BranchId::new();
        TestDb { db, dir, branch_id }
    }

    /// Get all 6 primitives.
    pub fn all_primitives(&self) -> AllPrimitives {
        AllPrimitives {
            kv: KVStore::new(self.db.clone()),
            json: JsonStore::new(self.db.clone()),
            event: EventLog::new(self.db.clone()),
            branch: BranchIndex::new(self.db.clone()),
            vector: VectorStore::new(self.db.clone()),
            graph: GraphStore::new(self.db.clone()),
        }
    }

    pub fn kv(&self) -> KVStore {
        KVStore::new(self.db.clone())
    }

    pub fn json(&self) -> JsonStore {
        JsonStore::new(self.db.clone())
    }

    pub fn event(&self) -> EventLog {
        EventLog::new(self.db.clone())
    }

    pub fn branch_index(&self) -> BranchIndex {
        BranchIndex::new(self.db.clone())
    }

    pub fn vector(&self) -> VectorStore {
        VectorStore::new(self.db.clone())
    }

    pub fn db_path(&self) -> &Path {
        self.dir.path()
    }

    /// Get the WAL file path (first segment).
    pub fn wal_path(&self) -> PathBuf {
        self.dir.path().join("wal").join("wal-000001.seg")
    }

    /// Get the WAL directory path.
    pub fn wal_dir(&self) -> PathBuf {
        self.dir.path().join("wal")
    }

    /// Get the snapshots directory path.
    pub fn snapshot_dir(&self) -> PathBuf {
        self.dir.path().join("snapshots")
    }

    /// Reopen the database (simulates restart).
    pub fn reopen(&mut self) {
        // Drop the old database first so the global registry clears the path entry
        let path = self.dir.path().to_path_buf();
        drop(std::mem::replace(
            &mut self.db,
            Database::cache().expect("temporary cache for swap"),
        ));
        // Now open fresh from the same path (reads strata.toml)
        self.db = test_db_builder()
            .open(&path)
            .expect("Failed to reopen database");
    }

    /// Create a new branch ID for this test.
    pub fn new_branch(&mut self) -> BranchId {
        self.branch_id = BranchId::new();
        self.branch_id
    }
}

impl Default for TestDb {
    fn default() -> Self {
        Self::new()
    }
}

/// Container for all 6 primitives.
pub struct AllPrimitives {
    pub kv: KVStore,
    pub json: JsonStore,
    pub event: EventLog,
    pub branch: BranchIndex,
    pub vector: VectorStore,
    pub graph: GraphStore,
}

// ============================================================================
// Database Creation Helpers
// ============================================================================

/// Create an in-memory test database (fastest, no persistence).
pub fn create_test_db() -> Arc<Database> {
    test_db_builder()
        .cache()
        .expect("Failed to create test database")
}

/// Create a persistent database at the given path.
pub fn create_persistent_db(path: &Path) -> Arc<Database> {
    test_db_builder()
        .open(path)
        .expect("Failed to create persistent database")
}

/// Create in-memory, standard, and always databases for cross-mode testing.
fn all_mode_databases() -> Vec<(&'static str, Arc<Database>, Option<TempDir>)> {
    let standard_dir = tempfile::tempdir().expect("Failed to create temp dir for standard db");
    let standard_db = test_db_builder()
        .open(standard_dir.path())
        .expect("standard db");
    let always_dir = tempfile::tempdir().expect("Failed to create temp dir for always db");
    let always_db = test_db_builder()
        .open_with_config(always_dir.path(), always_config())
        .expect("always db");
    vec![
        ("in_memory", create_test_db(), None),
        ("standard", standard_db, Some(standard_dir)),
        ("always", always_db, Some(always_dir)),
    ]
}

/// Run a test workload across all durability modes.
pub fn test_across_modes<F, T>(test_name: &str, workload: F)
where
    F: Fn(Arc<Database>) -> T,
    T: PartialEq + std::fmt::Debug,
{
    let dbs = all_mode_databases();
    let mut results: Vec<(&str, T)> = Vec::new();

    for (name, db, _dir) in dbs {
        let result = workload(db);
        results.push((name, result));
    }

    let (first_mode, first_result) = &results[0];
    for (mode, result) in &results[1..] {
        assert_eq!(
            first_result, result,
            "SEMANTIC DRIFT in '{}': {:?} produced {:?}, but {:?} produced {:?}",
            test_name, first_mode, first_result, mode, result
        );
    }
}

// ============================================================================
// Unique Key Generation
// ============================================================================

/// Generate a unique key string.
pub fn unique_key() -> String {
    let count = COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("key_{}", count)
}

/// Generate a unique key with a prefix.
pub fn unique_key_with_prefix(prefix: &str) -> String {
    let count = COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("{}_{}", prefix, count)
}

// ============================================================================
// Vector Configuration Helpers
// ============================================================================

/// 3-dimension config for quick tests.
pub fn config_small() -> VectorConfig {
    VectorConfig {
        dimension: 3,
        metric: DistanceMetric::Cosine,
        storage_dtype: StorageDtype::F32,
    }
}

/// 384-dimension MiniLM-style config.
pub fn config_standard() -> VectorConfig {
    VectorConfig {
        dimension: 384,
        metric: DistanceMetric::Cosine,
        storage_dtype: StorageDtype::F32,
    }
}

/// Custom dimension + metric config.
pub fn config_custom(dimension: usize, metric: DistanceMetric) -> VectorConfig {
    VectorConfig {
        dimension,
        metric,
        storage_dtype: StorageDtype::F32,
    }
}

/// Euclidean config (384 dimensions).
pub fn config_euclidean() -> VectorConfig {
    VectorConfig {
        dimension: 384,
        metric: DistanceMetric::Euclidean,
        storage_dtype: StorageDtype::F32,
    }
}

/// DotProduct config (384 dimensions).
pub fn config_dotproduct() -> VectorConfig {
    VectorConfig {
        dimension: 384,
        metric: DistanceMetric::DotProduct,
        storage_dtype: StorageDtype::F32,
    }
}

// ============================================================================
// Random Data Generation
// ============================================================================

/// Generate a seeded random vector.
pub fn seeded_vector(dimension: usize, seed: u64) -> Vec<f32> {
    use std::collections::hash_map::DefaultHasher;

    let mut hasher = DefaultHasher::new();
    (0..dimension)
        .map(|i| {
            (i as u64 ^ seed).hash(&mut hasher);
            let h = hasher.finish();
            (h as f32 / u64::MAX as f32) * 2.0 - 1.0
        })
        .collect()
}

/// Generate a random vector (counter-seeded).
pub fn random_vector(dimension: usize) -> Vec<f32> {
    let seed = COUNTER.fetch_add(1, Ordering::SeqCst);
    seeded_vector(dimension, seed)
}

/// Generate a zero vector.
pub fn zero_vector(dimension: usize) -> Vec<f32> {
    vec![0.0; dimension]
}

/// Generate a unit vector (first component = 1.0).
pub fn unit_vector(dimension: usize) -> Vec<f32> {
    let mut v = vec![0.0; dimension];
    if dimension > 0 {
        v[0] = 1.0;
    }
    v
}

// ============================================================================
// Event Payload Helpers
// ============================================================================

/// Create an empty object payload for EventLog.
pub fn empty_payload() -> Value {
    Value::object(std::collections::HashMap::new())
}

/// Create an object payload wrapping an integer.
pub fn int_payload(v: i64) -> Value {
    Value::object(HashMap::from([("value".to_string(), Value::Int(v))]))
}

/// Create an object payload wrapping a string.
pub fn string_payload(s: &str) -> Value {
    Value::object(HashMap::from([(
        "data".to_string(),
        Value::String(s.into()),
    )]))
}

// ============================================================================
// Value Constructors
// ============================================================================

pub mod values {
    use super::*;

    pub fn int(n: i64) -> Value {
        Value::Int(n)
    }
    pub fn float(f: f64) -> Value {
        Value::Float(f)
    }
    pub fn string(s: &str) -> Value {
        Value::String(s.to_string())
    }
    pub fn bytes(data: &[u8]) -> Value {
        Value::Bytes(data.to_vec())
    }
    pub fn bool_val(b: bool) -> Value {
        Value::Bool(b)
    }
    pub fn null() -> Value {
        Value::Null
    }
    pub fn array(items: Vec<Value>) -> Value {
        Value::array(items)
    }
    pub fn map(pairs: Vec<(&str, Value)>) -> Value {
        let mut m = std::collections::HashMap::new();
        for (k, v) in pairs {
            m.insert(k.to_string(), v);
        }
        Value::object(m)
    }

    /// Event payload wrapping a value as `{"data": value}`.
    pub fn event_payload(value: Value) -> Value {
        let mut m = std::collections::HashMap::new();
        m.insert("data".to_string(), value);
        Value::object(m)
    }

    /// Empty event payload.
    pub fn empty_event_payload() -> Value {
        Value::object(std::collections::HashMap::new())
    }

    /// Large bytes value for stress tests.
    pub fn large_bytes(size_kb: usize) -> Value {
        Value::Bytes(vec![0xAB; size_kb * 1024])
    }

    /// Sized string value.
    pub fn sized_string(size: usize) -> Value {
        Value::String("x".repeat(size))
    }
}

// ============================================================================
// JSON Helpers
// ============================================================================

/// Generate a test JSON value.
pub fn test_json_value(index: usize) -> JsonValue {
    JsonValue::from(serde_json::json!({
        "id": index,
        "name": format!("document_{}", index),
        "tags": ["test", "generated"],
        "nested": {
            "value": index * 10,
            "active": true
        }
    }))
}

/// Generate a deeply nested JSON value.
pub fn deep_json_value(depth: usize) -> JsonValue {
    let mut doc = serde_json::json!({ "value": depth });
    for i in (0..depth).rev() {
        doc = serde_json::json!({ "level": i, "child": doc });
    }
    JsonValue::from(doc)
}

/// Generate a large JSON document of specified byte size.
pub fn large_json_doc(size_bytes: usize) -> JsonValue {
    let padding = "x".repeat(size_bytes);
    JsonValue::from(serde_json::json!({ "data": padding }))
}

/// Generate a JSON array with specified element count.
pub fn large_array_json(element_count: usize) -> JsonValue {
    let arr: Vec<serde_json::Value> = (0..element_count)
        .map(|i| serde_json::json!({"index": i}))
        .collect();
    JsonValue::from(serde_json::json!({"array": arr}))
}

/// Helper to create a JsonValue from serde_json::Value.
pub fn json_value(v: serde_json::Value) -> JsonValue {
    JsonValue::from(v)
}

/// Helper to create a new unique doc ID string.
pub fn new_doc_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// Parse a JsonPath from string.
pub fn path(s: &str) -> JsonPath {
    s.parse().expect("Invalid path")
}

/// Root JsonPath.
pub fn root() -> JsonPath {
    JsonPath::root()
}

/// Create an empty JSON object.
pub fn empty_object() -> JsonValue {
    JsonValue::object()
}

/// Create an empty JSON array.
pub fn empty_array() -> JsonValue {
    JsonValue::array()
}

/// Create a test document in a store, returning its ID string.
pub fn create_doc(store: &JsonStore, branch_id: &BranchId, value: JsonValue) -> String {
    let doc_id = new_doc_id();
    store
        .create(branch_id, "default", &doc_id, value)
        .expect("Failed to create document");
    doc_id
}

/// Generate a random JSON tree with controlled depth.
pub fn random_json_tree(depth: usize, seed: u64) -> JsonValue {
    fn hash_seed(seed: u64, salt: u64) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        seed.hash(&mut hasher);
        salt.hash(&mut hasher);
        hasher.finish()
    }

    fn generate(depth: usize, seed: u64) -> JsonValue {
        if depth == 0 {
            match seed % 4 {
                0 => JsonValue::from(seed as i64),
                1 => JsonValue::from(format!("str_{}", seed)),
                2 => JsonValue::from(seed.is_multiple_of(2)),
                _ => JsonValue::null(),
            }
        } else if seed.is_multiple_of(2) {
            let mut map = serde_json::Map::new();
            let count = (seed % 5) as usize + 1;
            for i in 0..count {
                let key = format!("key_{}", i);
                let child = generate(depth - 1, hash_seed(seed, i as u64));
                map.insert(key, child.into_inner());
            }
            JsonValue::from(serde_json::Value::Object(map))
        } else {
            let count = (seed % 5) as usize + 1;
            let arr: Vec<serde_json::Value> = (0..count)
                .map(|i| generate(depth - 1, hash_seed(seed, i as u64)).into_inner())
                .collect();
            JsonValue::from(serde_json::Value::Array(arr))
        }
    }

    generate(depth, seed)
}

/// Collect all valid paths in a JSON tree.
pub fn collect_paths(value: &JsonValue) -> Vec<JsonPath> {
    fn collect(value: &serde_json::Value, current: JsonPath, paths: &mut Vec<JsonPath>) {
        paths.push(current.clone());
        match value {
            serde_json::Value::Object(obj) => {
                for (key, child) in obj {
                    let child_path = current.clone().key(key);
                    collect(child, child_path, paths);
                }
            }
            serde_json::Value::Array(arr) => {
                for (idx, child) in arr.iter().enumerate() {
                    let child_path = current.clone().index(idx);
                    collect(child, child_path, paths);
                }
            }
            _ => {}
        }
    }

    let mut paths = Vec::new();
    collect(value.as_inner(), JsonPath::root(), &mut paths);
    paths
}

// ============================================================================
// Distance Calculation Helpers
// ============================================================================

/// Cosine similarity between two vectors.
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(a.len(), b.len());
    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm_a == 0.0 || norm_b == 0.0 {
        0.0
    } else {
        dot / (norm_a * norm_b)
    }
}

/// Euclidean distance between two vectors.
pub fn euclidean_distance(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(a.len(), b.len());
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| (x - y).powi(2))
        .sum::<f32>()
        .sqrt()
}

/// Dot product between two vectors.
pub fn dot_product(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(a.len(), b.len());
    a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
}

// ============================================================================
// WAL / Snapshot Manipulation Helpers
// ============================================================================

/// Corrupt a file at a specific offset.
pub fn corrupt_file_at_offset(path: &Path, offset: u64, bytes: &[u8]) {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .expect("Failed to open file for corruption");
    file.seek(SeekFrom::Start(offset))
        .expect("Failed to seek in file");
    file.write_all(bytes)
        .expect("Failed to write corruption bytes");
}

/// Corrupt a file by flipping bits near the middle.
pub fn corrupt_file_random(path: &Path) {
    let metadata = fs::metadata(path).expect("Failed to get file metadata");
    let size = metadata.len();
    if size > 100 {
        corrupt_file_at_offset(path, size / 2, &[0xFF, 0xFF, 0xFF, 0xFF]);
    }
}

/// Truncate a file to a specific size.
pub fn truncate_file(path: &Path, new_size: u64) {
    let file = OpenOptions::new()
        .write(true)
        .open(path)
        .expect("Failed to open file for truncation");
    file.set_len(new_size).expect("Failed to truncate file");
}

/// Get file size (0 if not found).
pub fn file_size(path: &Path) -> u64 {
    fs::metadata(path).map(|m| m.len()).unwrap_or(0)
}

/// Create a partial WAL entry (simulating crash during write).
pub fn create_partial_wal_entry(path: &Path, entry_bytes: &[u8], fraction: f64) {
    let partial_len = ((entry_bytes.len() as f64) * fraction) as usize;
    let partial = &entry_bytes[..partial_len];
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .expect("Failed to open WAL for partial write");
    file.write_all(partial)
        .expect("Failed to write partial entry");
}

/// Count snapshot files in a directory.
pub fn count_snapshots(dir: &Path) -> usize {
    if !dir.exists() {
        return 0;
    }
    fs::read_dir(dir)
        .map(|entries| {
            entries
                .filter_map(Result::ok)
                .filter(|e| {
                    e.path()
                        .extension()
                        .map(|ext| ext == "snap")
                        .unwrap_or(false)
                })
                .count()
        })
        .unwrap_or(0)
}

/// List all snapshot files in a directory.
pub fn list_snapshots(dir: &Path) -> Vec<PathBuf> {
    if !dir.exists() {
        return Vec::new();
    }
    fs::read_dir(dir)
        .map(|entries| {
            entries
                .filter_map(Result::ok)
                .filter(|e| {
                    e.path()
                        .extension()
                        .map(|ext| ext == "snap")
                        .unwrap_or(false)
                })
                .map(|e| e.path())
                .collect()
        })
        .unwrap_or_default()
}

/// Delete all snapshot files in a directory.
pub fn delete_snapshots(dir: &Path) {
    if !dir.exists() {
        return;
    }
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.filter_map(Result::ok) {
            let path = entry.path();
            if path.extension().map(|ext| ext == "snap").unwrap_or(false) {
                let _ = fs::remove_file(path);
            }
        }
    }
}

// ============================================================================
// State Capture for Comparison
// ============================================================================

/// Captured KV state of a database for comparison.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapturedState {
    pub kv_entries: HashMap<String, String>,
    pub hash: u64,
}

impl CapturedState {
    pub fn capture(db: &Arc<Database>, branch_id: &BranchId) -> Self {
        let kv = KVStore::new(db.clone());
        let mut kv_entries = HashMap::new();

        if let Ok(keys) = kv.list(branch_id, "default", None) {
            for key in keys {
                if let Ok(Some(value)) = kv.get(branch_id, "default", &key) {
                    kv_entries.insert(key.to_string(), format!("{:?}", value));
                }
            }
        }

        let hash = Self::compute_hash(&kv_entries);
        CapturedState { kv_entries, hash }
    }

    fn compute_hash(entries: &HashMap<String, String>) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        let mut hasher = DefaultHasher::new();
        let mut sorted: Vec<_> = entries.iter().collect();
        sorted.sort_by_key(|(k, _)| *k);
        for (k, v) in sorted {
            k.hash(&mut hasher);
            v.hash(&mut hasher);
        }
        hasher.finish()
    }
}

/// Captured state of a vector collection for comparison.
#[derive(Debug, Clone, PartialEq)]
pub struct CapturedVectorState {
    pub vectors: BTreeMap<String, (strata_vector::VectorId, Vec<f32>, Option<serde_json::Value>)>,
    pub count: usize,
    pub max_id: u64,
}

impl CapturedVectorState {
    /// Capture state by probing keys matching `key_0`, `key_1`, etc.
    pub fn capture(vector_store: &VectorStore, branch_id: BranchId, collection: &str) -> Self {
        let mut vectors = BTreeMap::new();
        let mut max_id = 0u64;

        for i in 0..1000 {
            for key in [format!("key_{}", i), format!("key_{:02}", i)] {
                if let Ok(Some(entry)) = vector_store.get(branch_id, "default", collection, &key) {
                    let vid = entry.value.vector_id();
                    if vid.as_u64() > max_id {
                        max_id = vid.as_u64();
                    }
                    vectors.insert(
                        key,
                        (
                            vid,
                            entry.value.embedding.clone(),
                            entry.value.metadata.clone(),
                        ),
                    );
                }
            }
        }

        let count = vectors.len();
        CapturedVectorState {
            vectors,
            count,
            max_id,
        }
    }

    /// Capture state using a known set of keys.
    pub fn capture_keys(
        vector_store: &VectorStore,
        branch_id: BranchId,
        collection: &str,
        keys: &[String],
    ) -> Self {
        let mut vectors = BTreeMap::new();
        let mut max_id = 0u64;

        for key in keys {
            if let Ok(Some(entry)) = vector_store.get(branch_id, "default", collection, key) {
                let vid = entry.value.vector_id();
                if vid.as_u64() > max_id {
                    max_id = vid.as_u64();
                }
                vectors.insert(
                    key.to_string(),
                    (
                        vid,
                        entry.value.embedding.clone(),
                        entry.value.metadata.clone(),
                    ),
                );
            }
        }

        let count = vectors.len();
        CapturedVectorState {
            vectors,
            count,
            max_id,
        }
    }
}

// ============================================================================
// Assertion Helpers
// ============================================================================

/// Assert a database is healthy (can write + read back).
pub fn assert_db_healthy(db: &Arc<Database>, branch_id: &BranchId) {
    let kv = KVStore::new(db.clone());
    let key = unique_key();
    kv.put(branch_id, "default", &key, Value::String("test".into()))
        .expect("Database should be able to write");
    let value = kv
        .get(branch_id, "default", &key)
        .expect("Database should be able to read");
    assert_eq!(
        value,
        Some(Value::String("test".into())),
        "Database should return written value"
    );
}

/// Assert all 6 primitives can perform basic operations.
pub fn assert_all_primitives_healthy(test_db: &TestDb) {
    let p = test_db.all_primitives();
    let branch_id = test_db.branch_id;

    // KV
    let key = unique_key();
    p.kv.put(&branch_id, "default", &key, Value::String("kv_test".into()))
        .expect("KV should write");
    assert_eq!(
        p.kv.get(&branch_id, "default", &key).expect("KV read"),
        Some(Value::String("kv_test".into()))
    );

    // JSON
    let doc_id = new_doc_id();
    p.json
        .create(&branch_id, "default", &doc_id, test_json_value(0))
        .expect("JSON should create");
    assert!(p
        .json
        .get(&branch_id, "default", &doc_id, &JsonPath::root())
        .expect("JSON read")
        .is_some());

    // Event
    p.event
        .append(&branch_id, "default", "test_event", empty_payload())
        .expect("Event should append");

    // Vector
    let collection = unique_key();
    p.vector
        .create_collection(branch_id, "default", &collection, config_small())
        .expect("Vector should create collection");
    let vec_key = unique_key();
    p.vector
        .insert(
            branch_id,
            "default",
            &collection,
            &vec_key,
            &[1.0, 0.0, 0.0],
            None,
        )
        .expect("Vector should insert");
}

/// Assert two captured states are equal.
pub fn assert_states_equal(state1: &CapturedState, state2: &CapturedState, msg: &str) {
    assert_eq!(state1.hash, state2.hash, "{}: State hashes differ", msg);
    assert_eq!(
        state1.kv_entries, state2.kv_entries,
        "{}: KV entries differ",
        msg
    );
}

/// Assert two vector states are identical.
pub fn assert_vector_states_equal(
    state1: &CapturedVectorState,
    state2: &CapturedVectorState,
    msg: &str,
) {
    assert_eq!(state1.count, state2.count, "{}: Vector counts differ", msg);
    assert_eq!(
        state1.vectors.len(),
        state2.vectors.len(),
        "{}: Vector map sizes differ",
        msg
    );

    for (key, (id1, emb1, meta1)) in &state1.vectors {
        let (id2, emb2, meta2) = state2
            .vectors
            .get(key)
            .unwrap_or_else(|| panic!("{}: Missing key {} in second state", msg, key));

        assert_eq!(id1, id2, "{}: VectorId differs for key {}", msg, key);
        assert_eq!(
            emb1.len(),
            emb2.len(),
            "{}: Embedding length differs for key {}",
            msg,
            key
        );

        for (i, (v1, v2)) in emb1.iter().zip(emb2.iter()).enumerate() {
            assert!(
                (v1 - v2).abs() < 1e-6,
                "{}: Embedding value differs at index {} for key {}",
                msg,
                i,
                key
            );
        }

        assert_eq!(meta1, meta2, "{}: Metadata differs for key {}", msg, key);
    }
}

/// Assert a vector collection is healthy.
pub fn assert_vector_collection_healthy(
    vector_store: &VectorStore,
    branch_id: BranchId,
    collection: &str,
    dimension: usize,
) {
    let test_key = unique_key();
    let test_embedding = random_vector(dimension);

    vector_store
        .insert(
            branch_id,
            "default",
            collection,
            &test_key,
            &test_embedding,
            None,
        )
        .expect("Vector store should be able to insert");

    let entry = vector_store
        .get(branch_id, "default", collection, &test_key)
        .expect("Vector store should be able to get");
    assert!(entry.is_some(), "Vector store should return inserted entry");

    let results = vector_store
        .search(branch_id, "default", collection, &test_embedding, 1, None)
        .expect("Vector store should be able to search");
    assert!(!results.is_empty(), "Search should return results");
}

pub mod assert_helpers {
    use strata_core::StrataError;

    pub fn assert_conflict<T: std::fmt::Debug>(result: Result<T, StrataError>) {
        match result {
            Err(e) if e.is_conflict() => {}
            Err(e) => panic!("Expected conflict error, got: {:?}", e),
            Ok(v) => panic!("Expected conflict error, got Ok({:?})", v),
        }
    }

    pub fn assert_invalid_state<T: std::fmt::Debug>(result: Result<T, StrataError>) {
        match result {
            Err(_) => {} // Any error is acceptable
            Ok(v) => panic!("Expected error, got Ok({:?})", v),
        }
    }

    pub fn assert_error<T: std::fmt::Debug>(result: Result<T, StrataError>) {
        match result {
            Err(_) => {}
            Ok(v) => panic!("Expected error, got Ok({:?})", v),
        }
    }
}

// ============================================================================
// Concurrency Helpers
// ============================================================================

pub mod concurrent {
    use super::*;

    /// Run multiple threads that start at the same time.
    pub fn run_concurrent<F, T>(num_threads: usize, f: F) -> Vec<T>
    where
        F: Fn(usize) -> T + Send + Sync + 'static,
        T: Send + 'static,
    {
        let barrier = Arc::new(Barrier::new(num_threads));
        let f = Arc::new(f);

        let handles: Vec<JoinHandle<T>> = (0..num_threads)
            .map(|i| {
                let barrier = Arc::clone(&barrier);
                let f = Arc::clone(&f);
                thread::spawn(move || {
                    barrier.wait();
                    f(i)
                })
            })
            .collect();

        handles
            .into_iter()
            .map(|h| h.join().expect("Thread panicked"))
            .collect()
    }

    /// Run threads with shared state.
    pub fn run_with_shared<S, F, T>(num_threads: usize, shared: S, f: F) -> Vec<T>
    where
        S: Send + Sync + 'static,
        F: Fn(usize, &S) -> T + Send + Sync + 'static,
        T: Send + 'static,
    {
        let barrier = Arc::new(Barrier::new(num_threads));
        let shared = Arc::new(shared);
        let f = Arc::new(f);

        let handles: Vec<JoinHandle<T>> = (0..num_threads)
            .map(|i| {
                let barrier = Arc::clone(&barrier);
                let shared = Arc::clone(&shared);
                let f = Arc::clone(&f);
                thread::spawn(move || {
                    barrier.wait();
                    f(i, &shared)
                })
            })
            .collect();

        handles
            .into_iter()
            .map(|h| h.join().expect("Thread panicked"))
            .collect()
    }
}

// ============================================================================
// Timing Helpers
// ============================================================================

pub mod timing {
    use super::*;

    /// Measure execution time of a closure.
    pub fn measure<F, T>(f: F) -> (T, Duration)
    where
        F: FnOnce() -> T,
    {
        let start = Instant::now();
        let result = f();
        (result, start.elapsed())
    }

    /// Assert that an operation completes within a timeout.
    pub fn assert_completes_within<F, T>(timeout: Duration, f: F) -> T
    where
        F: FnOnce() -> T,
    {
        let (result, elapsed) = measure(f);
        assert!(
            elapsed < timeout,
            "Operation took {:?}, expected < {:?}",
            elapsed,
            timeout
        );
        result
    }
}

/// Run a function with a timeout.
pub fn with_timeout<F, T>(timeout: Duration, f: F) -> Option<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    use std::sync::mpsc;
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let result = f();
        let _ = tx.send(result);
    });
    rx.recv_timeout(timeout).ok()
}

// ============================================================================
// Search Helpers (for intelligence tests)
// ============================================================================

pub mod search {
    use std::sync::Arc;
    use strata_core::PrimitiveType;
    use strata_engine::search::recipe::get_builtin_recipe;
    use strata_engine::search::SearchHit;
    use strata_engine::{Database, SearchRequest, SearchResponse};
    use strata_search::substrate::{self, RetrievalRequest, RetrievalResponse};

    /// Search via the unified retrieval substrate.
    ///
    /// Converts a `SearchRequest` into a `RetrievalRequest` using the builtin
    /// recipe defaults, calls `substrate::retrieve()`, and returns the response.
    /// This is the canonical way to run cross-primitive search in tests.
    pub fn substrate_search(db: &Arc<Database>, req: &SearchRequest) -> RetrievalResponse {
        let mut recipe = get_builtin_recipe("keyword").expect("keyword recipe must exist");
        // Apply k from the SearchRequest to the recipe limit.
        recipe.transform = Some(strata_engine::search::recipe::TransformConfig {
            limit: Some(req.k),
            ..Default::default()
        });

        let retrieval_req = RetrievalRequest {
            query: req.query.clone(),
            branch_id: req.branch_id,
            space: req.space.clone(),
            recipe,
            embedding: req.precomputed_embedding.clone(),
            time_range: req.time_range,
            primitive_filter: req.primitive_filter.clone(),
            as_of: None,
            budget_ms: None,
        };

        substrate::retrieve(db, &retrieval_req).expect("substrate::retrieve failed")
    }

    /// Assert all hits are from a specific primitive.
    pub fn assert_all_from_primitive(response: &SearchResponse, kind: PrimitiveType) {
        for hit in &response.hits {
            assert_eq!(
                hit.doc_ref.primitive_type(),
                kind,
                "Expected all hits from {:?}, found {:?}",
                kind,
                hit.doc_ref.primitive_type()
            );
        }
    }

    /// Verify search results are deterministic (via substrate).
    pub fn verify_deterministic(db: &Arc<Database>, req: &SearchRequest) {
        let r1 = substrate_search(db, req);
        let r2 = substrate_search(db, req);

        assert_eq!(r1.hits.len(), r2.hits.len());
        for (h1, h2) in r1.hits.iter().zip(r2.hits.iter()) {
            assert_eq!(h1.doc_ref, h2.doc_ref);
            assert_eq!(h1.rank, h2.rank);
            assert!((h1.score - h2.score).abs() < 0.0001);
        }
    }

    /// Verify scores are monotonically decreasing.
    pub fn verify_scores_decreasing(response: &SearchResponse) {
        if response.hits.len() >= 2 {
            for i in 1..response.hits.len() {
                assert!(
                    response.hits[i - 1].score >= response.hits[i].score,
                    "Scores should be monotonically decreasing: {} vs {}",
                    response.hits[i - 1].score,
                    response.hits[i].score
                );
            }
        }
    }

    /// Verify substrate hit scores are monotonically decreasing.
    pub fn verify_substrate_scores_decreasing(hits: &[SearchHit]) {
        if hits.len() >= 2 {
            for i in 1..hits.len() {
                assert!(
                    hits[i - 1].score >= hits[i].score,
                    "Scores should be monotonically decreasing: {} vs {}",
                    hits[i - 1].score,
                    hits[i].score
                );
            }
        }
    }

    /// Verify ranks are sequential starting from 1.
    pub fn verify_ranks_sequential(response: &SearchResponse) {
        for (i, hit) in response.hits.iter().enumerate() {
            assert_eq!(
                hit.rank as usize,
                i + 1,
                "Ranks should be sequential starting from 1"
            );
        }
    }

    /// Verify substrate hit ranks are sequential starting from 1.
    pub fn verify_substrate_ranks_sequential(hits: &[SearchHit]) {
        for (i, hit) in hits.iter().enumerate() {
            assert_eq!(
                hit.rank as usize,
                i + 1,
                "Ranks should be sequential starting from 1"
            );
        }
    }
}

// ============================================================================
// M9 Core Type Helpers
// ============================================================================

pub mod core_types {
    use strata_core::{
        BranchId, BranchName, EntityRef, PrimitiveType, Timestamp, Version, Versioned,
    };

    pub fn test_branch_id() -> BranchId {
        BranchId::new()
    }

    pub fn test_branch_name(name: &str) -> BranchName {
        BranchName::new(name.to_string()).expect("valid test branch name")
    }

    pub fn test_timestamp(micros: u64) -> Timestamp {
        Timestamp::from_micros(micros)
    }

    pub fn test_txn_version(id: u64) -> Version {
        Version::txn(id)
    }

    pub fn test_seq_version(n: u64) -> Version {
        Version::seq(n)
    }

    pub fn test_counter_version(n: u64) -> Version {
        Version::counter(n)
    }

    pub fn test_versioned<T>(value: T, version: Version) -> Versioned<T> {
        Versioned::new(value, version)
    }

    pub fn all_entity_refs(branch_id: BranchId) -> Vec<EntityRef> {
        vec![
            EntityRef::kv(branch_id, "default", "test_key"),
            EntityRef::event(branch_id, "default", 1),
            EntityRef::branch(branch_id),
            EntityRef::json(branch_id, "default", "test_doc"),
            EntityRef::vector(branch_id, "default", "test_collection", "test_vector"),
            EntityRef::graph(branch_id, "default", "test_graph/n/test_node"),
        ]
    }

    pub fn all_primitive_types() -> Vec<PrimitiveType> {
        vec![
            PrimitiveType::Kv,
            PrimitiveType::Event,
            PrimitiveType::Branch,
            PrimitiveType::Json,
            PrimitiveType::Vector,
            PrimitiveType::Graph,
        ]
    }
}

// ============================================================================
// Vector Population Helpers
// ============================================================================

/// Populate a vector collection with test data.
pub fn populate_vector_collection(
    vector_store: &VectorStore,
    branch_id: BranchId,
    collection: &str,
    count: usize,
    dimension: usize,
) -> Vec<(String, Vec<f32>)> {
    let mut entries = Vec::new();
    for i in 0..count {
        let key = format!("key_{}", i);
        let embedding = seeded_vector(dimension, i as u64);
        vector_store
            .insert(branch_id, "default", collection, &key, &embedding, None)
            .expect("Failed to insert vector");
        entries.push((key, embedding));
    }
    entries
}

/// Populate a vector collection with metadata.
pub fn populate_vector_collection_with_metadata(
    vector_store: &VectorStore,
    branch_id: BranchId,
    collection: &str,
    count: usize,
    dimension: usize,
) -> Vec<(String, Vec<f32>, serde_json::Value)> {
    let mut entries = Vec::new();
    for i in 0..count {
        let key = format!("key_{}", i);
        let embedding = seeded_vector(dimension, i as u64);
        let metadata = serde_json::json!({
            "index": i,
            "category": format!("cat_{}", i % 5),
            "value": i as f64 * 0.1
        });
        vector_store
            .insert(
                branch_id,
                "default",
                collection,
                &key,
                &embedding,
                Some(metadata.clone()),
            )
            .expect("Failed to insert vector");
        entries.push((key, embedding, metadata));
    }
    entries
}

/// Write KV test data.
pub fn populate_test_data(db: &Arc<Database>, branch_id: &BranchId, count: usize) {
    let kv = KVStore::new(db.clone());
    for i in 0..count {
        kv.put(
            branch_id,
            "default",
            &format!("key_{}", i),
            Value::String(format!("value_{}", i)),
        )
        .expect("Failed to write test data");
    }
}

/// Verify specific keys exist.
pub fn verify_keys_exist(db: &Arc<Database>, branch_id: &BranchId, keys: &[&str]) {
    let kv = KVStore::new(db.clone());
    for key in keys {
        let value = kv
            .get(branch_id, "default", key)
            .expect("Failed to read key");
        assert!(value.is_some(), "Key {} should exist", key);
    }
}

/// Verify specific keys do NOT exist.
pub fn verify_keys_absent(db: &Arc<Database>, branch_id: &BranchId, keys: &[&str]) {
    let kv = KVStore::new(db.clone());
    for key in keys {
        let value = kv
            .get(branch_id, "default", key)
            .expect("Failed to read key");
        assert!(value.is_none(), "Key {} should NOT exist", key);
    }
}

/// Searchable trait check helper.
pub fn can_search_as_searchable<T: strata_engine::Searchable>(_: &T) -> bool {
    true
}

/// Test helper macro for checking that a type implements required traits.
#[macro_export]
macro_rules! assert_traits {
    ($type:ty: $($trait:ident),+ $(,)?) => {
        fn _assert_traits<T: $($trait +)+>() {}
        _assert_traits::<$type>();
    };
}
