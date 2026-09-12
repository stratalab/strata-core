//! Branch-local vector index manifest records.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use strata_core::{BranchId, CommitVersion};

use crate::data::kv::ProductSpace;
use crate::diagnostics::{EngineError, EngineResult};

use super::{VectorCollectionName, VectorDistanceMetric};

const MANIFEST_FORMAT_VERSION: u8 = 1;
const MANIFEST_POLICY_VERSION: u16 = 1;
const MANIFEST_MAX_BYTES: usize = 64 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct VectorIndexManifest {
    manifest_generation: u64,
    branch_id: BranchId,
    branch_generation: u64,
    space: ProductSpace,
    collection: VectorCollectionName,
    collection_generation: u64,
    policy_version: u16,
    artifact_refs: Vec<VectorArtifactRef>,
    active_delta_count: u64,
}

impl VectorIndexManifest {
    #[cfg_attr(not(any(test, feature = "testkit")), allow(dead_code))]
    pub(crate) fn empty(
        branch_id: BranchId,
        branch_generation: u64,
        space: ProductSpace,
        collection: VectorCollectionName,
        collection_generation: u64,
    ) -> Self {
        Self {
            manifest_generation: 1,
            branch_id,
            branch_generation,
            space,
            collection,
            collection_generation,
            policy_version: MANIFEST_POLICY_VERSION,
            artifact_refs: Vec::new(),
            active_delta_count: 0,
        }
    }

    pub(crate) const fn manifest_generation(&self) -> u64 {
        self.manifest_generation
    }

    pub(crate) fn artifact_refs(&self) -> &[VectorArtifactRef] {
        &self.artifact_refs
    }

    pub(crate) const fn active_delta_count(&self) -> u64 {
        self.active_delta_count
    }

    pub(crate) fn matches_branch_key(
        &self,
        branch_id: BranchId,
        branch_generation: u64,
        space: &ProductSpace,
        collection: &VectorCollectionName,
    ) -> bool {
        self.branch_id == branch_id
            && self.branch_generation == branch_generation
            && &self.space == space
            && &self.collection == collection
    }

    pub(crate) fn with_artifact_refs(
        mut self,
        artifact_refs: Vec<VectorArtifactRef>,
        active_delta_count: u64,
    ) -> Self {
        self.artifact_refs = artifact_refs;
        self.active_delta_count = active_delta_count;
        self
    }

    #[cfg(any(test, feature = "testkit"))]
    pub(crate) fn with_artifact_refs_for_test(
        self,
        artifact_refs: Vec<VectorArtifactRef>,
        active_delta_count: u64,
    ) -> Self {
        self.with_artifact_refs(artifact_refs, active_delta_count)
    }

    pub(crate) fn matches_branch_collection(
        &self,
        branch_id: BranchId,
        branch_generation: u64,
        space: &ProductSpace,
        collection: &VectorCollectionName,
        collection_generation: u64,
    ) -> bool {
        self.branch_id == branch_id
            && self.branch_generation == branch_generation
            && &self.space == space
            && &self.collection == collection
            && self.collection_generation == collection_generation
    }

    pub(crate) fn artifact_refs_match_config(
        &self,
        dimension: usize,
        metric: VectorDistanceMetric,
    ) -> bool {
        self.artifact_refs
            .iter()
            .all(|artifact_ref| artifact_ref.matches_config(dimension, metric))
    }

    pub(crate) fn materialize_for_child_fork(
        &self,
        child_branch_id: BranchId,
        child_branch_generation: u64,
        fork_version_cap: CommitVersion,
    ) -> Self {
        let artifact_refs = self
            .artifact_refs
            .iter()
            .map(|artifact_ref| artifact_ref.with_fork_version_cap(fork_version_cap))
            .collect();
        Self::empty(
            child_branch_id,
            child_branch_generation,
            self.space.clone(),
            self.collection.clone(),
            self.collection_generation,
        )
        .with_artifact_refs(artifact_refs, self.active_delta_count)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct VectorArtifactRef {
    artifact_id: String,
    source_id: String,
    source_branch_id: BranchId,
    source_generation: u64,
    covered_commit_version: u64,
    fork_version_cap: Option<CommitVersion>,
    index_kind: VectorArtifactKind,
    vector_dimension: usize,
    metric: VectorDistanceMetric,
    vector_count: u64,
    derived_bytes: u64,
    checksum: u64,
}

impl VectorArtifactRef {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        artifact_id: impl Into<String>,
        source_id: impl Into<String>,
        source_branch_id: BranchId,
        source_generation: u64,
        covered_commit_version: u64,
        fork_version_cap: Option<CommitVersion>,
        index_kind: VectorArtifactKind,
        vector_dimension: usize,
        metric: VectorDistanceMetric,
        vector_count: u64,
        derived_bytes: u64,
        checksum: u64,
    ) -> Result<Self, EngineError> {
        let reference = Self {
            artifact_id: artifact_id.into(),
            source_id: source_id.into(),
            source_branch_id,
            source_generation,
            covered_commit_version,
            fork_version_cap,
            index_kind,
            vector_dimension,
            metric,
            vector_count,
            derived_bytes,
            checksum,
        };
        reference.validate_identity()?;
        Ok(reference)
    }

    #[cfg(any(test, feature = "testkit"))]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn for_test(
        artifact_id: impl Into<String>,
        source_id: impl Into<String>,
        source_branch_id: BranchId,
        source_generation: u64,
        covered_commit_version: u64,
        fork_version_cap: Option<CommitVersion>,
        index_kind: VectorArtifactKind,
        vector_dimension: usize,
        metric: VectorDistanceMetric,
        vector_count: u64,
        derived_bytes: u64,
        checksum: u64,
    ) -> Self {
        Self::new(
            artifact_id,
            source_id,
            source_branch_id,
            source_generation,
            covered_commit_version,
            fork_version_cap,
            index_kind,
            vector_dimension,
            metric,
            vector_count,
            derived_bytes,
            checksum,
        )
        .expect("valid artifact ref for test")
    }

    #[allow(dead_code)]
    pub(crate) fn artifact_id(&self) -> &str {
        &self.artifact_id
    }

    #[allow(dead_code)]
    pub(crate) fn source_id(&self) -> &str {
        &self.source_id
    }

    pub(crate) const fn source_branch_id(&self) -> BranchId {
        self.source_branch_id
    }

    #[allow(dead_code)]
    pub(crate) const fn source_generation(&self) -> u64 {
        self.source_generation
    }

    #[allow(dead_code)]
    pub(crate) const fn covered_commit_version(&self) -> CommitVersion {
        CommitVersion::new(self.covered_commit_version)
    }

    #[allow(dead_code)]
    pub(crate) const fn fork_version_cap(&self) -> Option<CommitVersion> {
        self.fork_version_cap
    }

    #[allow(dead_code)]
    pub(crate) const fn index_kind(&self) -> VectorArtifactKind {
        self.index_kind
    }

    #[allow(dead_code)]
    pub(crate) const fn vector_dimension(&self) -> usize {
        self.vector_dimension
    }

    #[allow(dead_code)]
    pub(crate) const fn metric(&self) -> VectorDistanceMetric {
        self.metric
    }

    #[allow(dead_code)]
    pub(crate) const fn vector_count(&self) -> u64 {
        self.vector_count
    }

    #[allow(dead_code)]
    pub(crate) const fn derived_bytes(&self) -> u64 {
        self.derived_bytes
    }

    #[allow(dead_code)]
    pub(crate) const fn checksum(&self) -> u64 {
        self.checksum
    }

    fn validate_identity(&self) -> Result<(), EngineError> {
        if self.artifact_id.is_empty() || self.source_id.is_empty() {
            return Err(EngineError::corruption(
                "data_loss.engine.vector_index_manifest",
                "stored vector index manifest contains an empty artifact identity",
            ));
        }
        if self.vector_dimension == 0 {
            return Err(EngineError::corruption(
                "data_loss.engine.vector_index_manifest",
                "stored vector index manifest contains an invalid dimension",
            ));
        }
        Ok(())
    }

    fn matches_config(&self, dimension: usize, metric: VectorDistanceMetric) -> bool {
        self.vector_dimension == dimension && self.metric == metric
    }

    fn with_fork_version_cap(&self, fork_version_cap: CommitVersion) -> Self {
        let fork_version_cap = Some(match self.fork_version_cap {
            Some(existing) => existing.min(fork_version_cap),
            None => fork_version_cap,
        });
        Self {
            artifact_id: self.artifact_id.clone(),
            source_id: self.source_id.clone(),
            source_branch_id: self.source_branch_id,
            source_generation: self.source_generation,
            covered_commit_version: self.covered_commit_version,
            fork_version_cap,
            index_kind: self.index_kind,
            vector_dimension: self.vector_dimension,
            metric: self.metric,
            vector_count: self.vector_count,
            derived_bytes: self.derived_bytes,
            checksum: self.checksum,
        }
    }
}

impl StoredArtifactRef {
    fn from_artifact_ref(reference: &VectorArtifactRef) -> Self {
        Self {
            artifact_id: reference.artifact_id.clone(),
            source_id: reference.source_id.clone(),
            source_branch_id: reference.source_branch_id,
            source_generation: reference.source_generation,
            covered_commit_version: reference.covered_commit_version,
            fork_version_cap: reference.fork_version_cap,
            index_kind: reference.index_kind,
            vector_dimension: reference.vector_dimension,
            metric: reference.metric,
            vector_count: reference.vector_count,
            derived_bytes: reference.derived_bytes,
            checksum: reference.checksum,
        }
    }

    fn into_artifact_ref(self) -> Result<VectorArtifactRef, EngineError> {
        VectorArtifactRef::new(
            self.artifact_id,
            self.source_id,
            self.source_branch_id,
            self.source_generation,
            self.covered_commit_version,
            self.fork_version_cap,
            self.index_kind,
            self.vector_dimension,
            self.metric,
            self.vector_count,
            self.derived_bytes,
            self.checksum,
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum VectorArtifactKind {
    Flat,
    Hnsw,
}

#[derive(Serialize, Deserialize)]
struct StoredManifestEnvelope {
    checksum: u64,
    body: StoredManifestBody,
}

#[derive(Serialize, Deserialize)]
struct StoredManifestBody {
    manifest_generation: u64,
    branch_id: BranchId,
    branch_generation: u64,
    space: ProductSpace,
    collection: VectorCollectionName,
    collection_generation: u64,
    policy_version: u16,
    artifact_refs: Vec<StoredArtifactRef>,
    active_delta_count: u64,
}

#[derive(Serialize, Deserialize)]
struct StoredArtifactRef {
    artifact_id: String,
    source_id: String,
    source_branch_id: BranchId,
    source_generation: u64,
    #[serde(default)]
    covered_commit_version: u64,
    fork_version_cap: Option<CommitVersion>,
    index_kind: VectorArtifactKind,
    vector_dimension: usize,
    metric: VectorDistanceMetric,
    vector_count: u64,
    derived_bytes: u64,
    checksum: u64,
}

#[cfg_attr(not(any(test, feature = "testkit")), allow(dead_code))]
pub(crate) fn encode_vector_index_manifest(
    manifest: &VectorIndexManifest,
) -> Result<Vec<u8>, EngineError> {
    let body = StoredManifestBody::from_manifest(manifest);
    let body_bytes = serde_json::to_vec(&body).map_err(|error| {
        EngineError::invalid_input(
            "invalid_argument.engine.vector_index_manifest",
            format!("vector index manifest cannot be encoded: {error}"),
        )
    })?;
    let envelope = StoredManifestEnvelope {
        checksum: manifest_checksum(&body_bytes),
        body,
    };
    let mut bytes = vec![MANIFEST_FORMAT_VERSION];
    bytes.extend(serde_json::to_vec(&envelope).map_err(|error| {
        EngineError::invalid_input(
            "invalid_argument.engine.vector_index_manifest",
            format!("vector index manifest cannot be encoded: {error}"),
        )
    })?);
    if bytes.len() > MANIFEST_MAX_BYTES {
        return Err(EngineError::invalid_input(
            "invalid_argument.engine.vector_index_manifest",
            "vector index manifest is too large",
        ));
    }
    Ok(bytes)
}

pub(crate) fn decode_vector_index_manifest(
    bytes: &[u8],
) -> Result<VectorIndexManifest, EngineError> {
    if bytes.len() > MANIFEST_MAX_BYTES {
        return Err(EngineError::corruption(
            "data_loss.engine.vector_index_manifest",
            "stored vector index manifest is too large",
        ));
    }
    if bytes.first().copied() != Some(MANIFEST_FORMAT_VERSION) {
        return Err(EngineError::corruption(
            "data_loss.engine.vector_index_manifest",
            "stored vector index manifest has an unknown format version",
        ));
    }
    let envelope =
        serde_json::from_slice::<StoredManifestEnvelope>(&bytes[1..]).map_err(|error| {
            EngineError::corruption(
                "data_loss.engine.vector_index_manifest",
                format!("stored vector index manifest cannot be decoded: {error}"),
            )
        })?;
    let body_bytes = serde_json::to_vec(&envelope.body).map_err(|error| {
        EngineError::corruption(
            "data_loss.engine.vector_index_manifest",
            format!("stored vector index manifest cannot be checked: {error}"),
        )
    })?;
    if manifest_checksum(&body_bytes) != envelope.checksum {
        return Err(EngineError::corruption(
            "data_loss.engine.vector_index_manifest",
            "stored vector index manifest checksum does not match",
        ));
    }
    envelope.body.into_manifest()
}

impl StoredManifestBody {
    fn from_manifest(manifest: &VectorIndexManifest) -> Self {
        Self {
            manifest_generation: manifest.manifest_generation,
            branch_id: manifest.branch_id,
            branch_generation: manifest.branch_generation,
            space: manifest.space.clone(),
            collection: manifest.collection.clone(),
            collection_generation: manifest.collection_generation,
            policy_version: manifest.policy_version,
            artifact_refs: manifest
                .artifact_refs
                .iter()
                .map(StoredArtifactRef::from_artifact_ref)
                .collect(),
            active_delta_count: manifest.active_delta_count,
        }
    }

    fn into_manifest(self) -> Result<VectorIndexManifest, EngineError> {
        if self.policy_version != MANIFEST_POLICY_VERSION {
            return Err(EngineError::incompatible_layout(
                "failed_precondition.engine.vector_index_manifest",
                "vector index manifest policy version is not supported",
            ));
        }
        let artifact_refs = self
            .artifact_refs
            .into_iter()
            .map(StoredArtifactRef::into_artifact_ref)
            .collect::<EngineResult<Vec<_>>>()?;
        reject_duplicate_artifact_ids(&artifact_refs)?;
        Ok(VectorIndexManifest {
            manifest_generation: self.manifest_generation,
            branch_id: self.branch_id,
            branch_generation: self.branch_generation,
            space: self.space,
            collection: self.collection,
            collection_generation: self.collection_generation,
            policy_version: self.policy_version,
            artifact_refs,
            active_delta_count: self.active_delta_count,
        })
    }
}

fn reject_duplicate_artifact_ids(artifact_refs: &[VectorArtifactRef]) -> Result<(), EngineError> {
    let mut ids = BTreeSet::new();
    for artifact_ref in artifact_refs {
        if !ids.insert(artifact_ref.artifact_id.as_str()) {
            return Err(EngineError::corruption(
                "data_loss.engine.vector_index_manifest",
                "stored vector index manifest contains duplicate artifact refs",
            ));
        }
    }
    Ok(())
}

fn manifest_checksum(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

#[cfg(test)]
mod tests {
    use strata_core::BranchId;

    use super::{
        decode_vector_index_manifest, encode_vector_index_manifest, VectorArtifactKind,
        VectorArtifactRef, VectorIndexManifest, MANIFEST_MAX_BYTES, MANIFEST_POLICY_VERSION,
    };
    use crate::data::kv::ProductSpace;
    use crate::data::vector::{VectorCollectionName, VectorDistanceMetric};
    use crate::diagnostics::EngineErrorClass;

    #[test]
    fn vector_index_manifest_round_trips_empty_refs() {
        let branch = BranchId::from_bytes([7; BranchId::BYTE_LEN]);
        let space = ProductSpace::new("default").expect("valid space");
        let collection = VectorCollectionName::new("docs").expect("valid collection");
        let manifest = VectorIndexManifest::empty(branch, 3, space.clone(), collection.clone(), 11);

        let encoded = encode_vector_index_manifest(&manifest).expect("manifest encodes");
        let decoded = decode_vector_index_manifest(&encoded).expect("manifest decodes");

        assert_eq!(decoded, manifest);
        assert!(decoded.matches_branch_collection(branch, 3, &space, &collection, 11));
    }

    #[test]
    fn vector_index_manifest_rejects_unknown_version_checksum_and_oversized_rows() {
        let branch = BranchId::from_bytes([8; BranchId::BYTE_LEN]);
        let manifest = VectorIndexManifest::empty(
            branch,
            1,
            ProductSpace::new("default").expect("valid space"),
            VectorCollectionName::new("docs").expect("valid collection"),
            1,
        );
        let mut encoded = encode_vector_index_manifest(&manifest).expect("manifest encodes");

        let mut unknown_version = encoded.clone();
        unknown_version[0] = 2;
        let error =
            decode_vector_index_manifest(&unknown_version).expect_err("unknown version must fail");
        assert_eq!(error.class(), EngineErrorClass::Corruption);

        let last = encoded.len() - 1;
        encoded[last] ^= 0x01;
        let error = decode_vector_index_manifest(&encoded).expect_err("checksum must fail");
        assert_eq!(error.class(), EngineErrorClass::Corruption);

        let oversized = vec![1; MANIFEST_MAX_BYTES + 1];
        let error = decode_vector_index_manifest(&oversized).expect_err("oversized must fail");
        assert_eq!(error.class(), EngineErrorClass::Corruption);
    }

    #[test]
    fn vector_index_manifest_rejects_duplicate_and_invalid_artifact_refs() {
        let branch = BranchId::from_bytes([9; BranchId::BYTE_LEN]);
        let base = VectorIndexManifest::empty(
            branch,
            1,
            ProductSpace::new("default").expect("valid space"),
            VectorCollectionName::new("docs").expect("valid collection"),
            1,
        );
        let duplicate = base.clone().with_artifact_refs_for_test(
            vec![
                artifact_ref("dup", "source-a", branch, 2),
                artifact_ref("dup", "source-b", branch, 2),
            ],
            0,
        );
        let error = decode_vector_index_manifest(
            &encode_vector_index_manifest(&duplicate).expect("manifest encodes"),
        )
        .expect_err("duplicate refs must fail");
        assert_eq!(error.class(), EngineErrorClass::Corruption);

        let invalid_dimension = encode_stored_manifest_with_invalid_ref(&base);
        let error = decode_vector_index_manifest(&invalid_dimension)
            .expect_err("zero dimension ref must fail");
        assert_eq!(error.class(), EngineErrorClass::Corruption);
    }

    fn artifact_ref(
        artifact_id: &str,
        source_id: &str,
        source_branch_id: BranchId,
        vector_dimension: usize,
    ) -> VectorArtifactRef {
        VectorArtifactRef::for_test(
            artifact_id,
            source_id,
            source_branch_id,
            1,
            1,
            None,
            VectorArtifactKind::Flat,
            vector_dimension,
            VectorDistanceMetric::Cosine,
            10,
            1024,
            99,
        )
    }

    fn encode_stored_manifest_with_invalid_ref(manifest: &VectorIndexManifest) -> Vec<u8> {
        let body = super::StoredManifestBody {
            manifest_generation: manifest.manifest_generation,
            branch_id: manifest.branch_id,
            branch_generation: manifest.branch_generation,
            space: manifest.space.clone(),
            collection: manifest.collection.clone(),
            collection_generation: manifest.collection_generation,
            policy_version: manifest.policy_version,
            artifact_refs: vec![super::StoredArtifactRef {
                artifact_id: "artifact".to_owned(),
                source_id: "source".to_owned(),
                source_branch_id: manifest.branch_id,
                source_generation: 1,
                covered_commit_version: 1,
                fork_version_cap: None,
                index_kind: VectorArtifactKind::Flat,
                vector_dimension: 0,
                metric: VectorDistanceMetric::Cosine,
                vector_count: 10,
                derived_bytes: 1024,
                checksum: 99,
            }],
            active_delta_count: 0,
        };
        let body_bytes = serde_json::to_vec(&body).expect("body encodes");
        let envelope = super::StoredManifestEnvelope {
            checksum: super::manifest_checksum(&body_bytes),
            body,
        };
        let mut bytes = vec![super::MANIFEST_FORMAT_VERSION];
        bytes.extend(serde_json::to_vec(&envelope).expect("envelope encodes"));
        bytes
    }

    // --- corruption-code assertions (TCP3.15c) ---------------------------
    //
    // The runtime swallows these decode errors into a "corrupt" manifest
    // status (source rows stay authoritative — rule 26), so the corruption
    // codes never propagate to a public caller. These decoder unit tests are
    // the only place the codes are assertable, and the corruption detector's
    // contract is exactly to return them.

    fn sample_manifest(seed: u8) -> VectorIndexManifest {
        VectorIndexManifest::empty(
            BranchId::from_bytes([seed; BranchId::BYTE_LEN]),
            3,
            ProductSpace::new("default").expect("valid space"),
            VectorCollectionName::new("docs").expect("valid collection"),
            1,
        )
    }

    #[test]
    fn decode_reports_the_data_loss_code_for_malformed_manifest_bytes() {
        let mut encoded =
            encode_vector_index_manifest(&sample_manifest(1)).expect("manifest encodes");
        encoded[0] = 0xFF; // wrong leading format-version byte (valid is 1)
        let error =
            decode_vector_index_manifest(&encoded).expect_err("malformed manifest is rejected");
        assert_eq!(error.code(), "data_loss.engine.vector_index_manifest");
        assert_eq!(error.class(), EngineErrorClass::Corruption);
    }

    #[test]
    fn decode_reports_the_precondition_code_for_an_unsupported_policy_version() {
        // Bytes that decode cleanly (valid format byte, JSON, and checksum) but
        // carry a policy version this build does not support. Arbitrary garbage
        // cannot reach this path — the checksum covers the policy field — so the
        // body is rebuilt with a matching checksum.
        let bytes =
            encode_manifest_with_policy_version(&sample_manifest(2), MANIFEST_POLICY_VERSION + 1);
        let error =
            decode_vector_index_manifest(&bytes).expect_err("unsupported policy is rejected");
        assert_eq!(
            error.code(),
            "failed_precondition.engine.vector_index_manifest"
        );
    }

    fn encode_manifest_with_policy_version(
        base: &VectorIndexManifest,
        policy_version: u16,
    ) -> Vec<u8> {
        let body = super::StoredManifestBody {
            manifest_generation: base.manifest_generation,
            branch_id: base.branch_id,
            branch_generation: base.branch_generation,
            space: base.space.clone(),
            collection: base.collection.clone(),
            collection_generation: base.collection_generation,
            policy_version,
            artifact_refs: Vec::new(),
            active_delta_count: 0,
        };
        let body_bytes = serde_json::to_vec(&body).expect("body encodes");
        let envelope = super::StoredManifestEnvelope {
            checksum: super::manifest_checksum(&body_bytes),
            body,
        };
        let mut bytes = vec![super::MANIFEST_FORMAT_VERSION];
        bytes.extend(serde_json::to_vec(&envelope).expect("envelope encodes"));
        bytes
    }
}
