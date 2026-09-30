//! Clone orchestration behavior (Ask 6, slice `HB7a`): the full
//! `hub.clone` flow against an in-process transport, the pre-download
//! compatibility gate, and failure hygiene.

use std::cell::RefCell;
use std::collections::HashMap;
use std::path::Path;

use strata_engine::{
    BranchName as EngineBranchName, Database, DurableLocalOpenOptions, KvKey, KvValue, ProductSpace,
};
use strata_hub::{
    clone_dataset, read_remote_tracking_ref, CloneError, CloneProgress, CloneRequest,
    EngineExportOptions, HubTransport, StrataCoreEngine,
};
use stratahub_protocol::{BranchName, DatasetName, Hash, Manifest};

/// In-process hub: one dataset, one branch, backed by a real exported
/// bundle. Counts object fetches so tests can assert the compatibility
/// gate fires before any download.
struct FakeHub {
    dataset: DatasetName,
    branch: BranchName,
    manifest_hash: Hash,
    manifest: Manifest,
    objects: HashMap<Hash, Vec<u8>>,
    object_fetches: RefCell<u64>,
}

impl FakeHub {
    fn from_database(path: &Path) -> Self {
        let mut engine = StrataCoreEngine::open(path).expect("open");
        let output = engine
            .export_bundle(&EngineExportOptions::default())
            .expect("export");
        let manifest_hash = stratahub_protocol::hash_bytes(&output.manifest_canonical_bytes);
        Self {
            dataset: DatasetName::parse("titanic").expect("dataset"),
            branch: BranchName::parse("default").expect("branch"),
            manifest_hash,
            manifest: output.manifest,
            objects: output
                .objects
                .into_iter()
                .map(|object| (object.hash, object.bytes))
                .collect(),
            object_fetches: RefCell::new(0),
        }
    }
}

impl HubTransport for FakeHub {
    fn hub_url(&self) -> String {
        "https://hub.example.com".to_owned()
    }

    fn default_branch(&self, dataset: &DatasetName) -> Result<BranchName, CloneError> {
        assert_eq!(dataset, &self.dataset);
        Ok(self.branch.clone())
    }

    fn resolve_ref(&self, dataset: &DatasetName, branch: &BranchName) -> Result<Hash, CloneError> {
        assert_eq!(dataset, &self.dataset);
        assert_eq!(branch, &self.branch);
        Ok(self.manifest_hash.clone())
    }

    fn get_manifest(&self, hash: &Hash) -> Result<Manifest, CloneError> {
        assert_eq!(hash, &self.manifest_hash);
        Ok(self.manifest.clone())
    }

    fn get_object(&self, hash: &Hash) -> Result<Vec<u8>, CloneError> {
        *self.object_fetches.borrow_mut() += 1;
        self.objects
            .get(hash)
            .cloned()
            .ok_or_else(|| CloneError::Transport {
                detail: format!("object {} not found", hash.as_str()),
            })
    }
}

fn build_source() -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = Database::open_local(dir.path(), DurableLocalOpenOptions::new())
        .expect("opens")
        .into_database();
    let mut kv = db
        .kv(
            EngineBranchName::new("default").expect("branch"),
            ProductSpace::new("default").expect("space"),
        )
        .expect("kv");
    kv.put(
        KvKey::new("user:ada").expect("key"),
        KvValue::new(b"engineer".to_vec()),
    )
    .expect("put");
    dir
}

#[test]
fn clone_flow_fetches_imports_and_records_origin() {
    let source = build_source();
    let hub = FakeHub::from_database(source.path());
    let workdir = tempfile::tempdir().expect("workdir");
    let dest = workdir.path().join("titanic.strata");

    let mut events = Vec::new();
    let outcome = clone_dataset(
        &hub,
        &CloneRequest {
            dataset: hub.dataset.clone(),
            branch: None, // exercises default-branch discovery
            dest: dest.clone(),
        },
        &mut |event| events.push(event),
    )
    .expect("clone succeeds");

    assert_eq!(outcome.branch.as_str(), "default");
    assert_eq!(outcome.manifest_hash, hub.manifest_hash);
    assert_eq!(outcome.object_count, hub.manifest.object_count);

    // Progress narrates the §3.6 flow in order.
    assert!(matches!(events[0], CloneProgress::Resolved { .. }));
    assert!(matches!(events[1], CloneProgress::ManifestFetched { .. }));
    assert!(matches!(events.last(), Some(CloneProgress::Done)));
    let fetched = events
        .iter()
        .filter(|event| matches!(event, CloneProgress::ObjectFetched { .. }))
        .count() as u64;
    assert_eq!(fetched, hub.manifest.object_count);

    // The clone is queryable and its origin is recorded.
    let mut db = Database::open_local(&dest, DurableLocalOpenOptions::new())
        .expect("clone opens")
        .into_database();
    let mut kv = db
        .kv(
            EngineBranchName::new("default").expect("branch"),
            ProductSpace::new("default").expect("space"),
        )
        .expect("kv");
    assert!(kv
        .get(&KvKey::new("user:ada").expect("key"))
        .expect("get")
        .is_some());
    drop(kv);
    // Close, not drop: a dropped handle may leave a detached worker holding
    // the writer lock past the reopen below (#2837, #3694).
    db.close().expect("clean close");

    let tracking_ref = read_remote_tracking_ref(&dest)
        .expect("read ref")
        .expect("recorded");
    assert_eq!(tracking_ref.hub_url, "https://hub.example.com");
    assert_eq!(tracking_ref.dataset.as_str(), "titanic");
    assert_eq!(tracking_ref.manifest_hash, hub.manifest_hash);
}

#[test]
fn incompatible_bundles_refuse_before_any_object_download() {
    let source = build_source();
    let mut hub = FakeHub::from_database(source.path());
    hub.manifest.engine_compatibility.required_engine_version = ">=99.0.0".to_owned();

    let workdir = tempfile::tempdir().expect("workdir");
    let dest = workdir.path().join("titanic.strata");
    let Err(error) = clone_dataset(
        &hub,
        &CloneRequest {
            dataset: hub.dataset.clone(),
            branch: Some(hub.branch.clone()),
            dest: dest.clone(),
        },
        &mut |_| {},
    ) else {
        panic!("incompatible bundle must refuse");
    };
    assert!(matches!(error, CloneError::EngineIncompatible { .. }));
    assert_eq!(
        *hub.object_fetches.borrow(),
        0,
        "no bandwidth spent on an incompatible bundle"
    );
    assert!(!dest.exists(), "no destination state");
}

#[test]
fn transport_failures_leave_no_destination_state() {
    let source = build_source();
    let mut hub = FakeHub::from_database(source.path());
    // Drop one object from the hub: the fetch loop fails mid-download.
    let victim = hub.manifest.objects.last().expect("objects").hash.clone();
    hub.objects.remove(&victim);

    let workdir = tempfile::tempdir().expect("workdir");
    let dest = workdir.path().join("titanic.strata");
    let Err(error) = clone_dataset(
        &hub,
        &CloneRequest {
            dataset: hub.dataset.clone(),
            branch: Some(hub.branch.clone()),
            dest: dest.clone(),
        },
        &mut |_| {},
    ) else {
        panic!("missing object must refuse");
    };
    assert!(matches!(error, CloneError::Transport { .. }));
    assert!(!dest.exists(), "no destination state");
}
