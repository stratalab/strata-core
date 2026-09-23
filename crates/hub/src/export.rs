//! `StrataCoreEngine`: bundle export over the engine's artifact payloads.
//!
//! Maps engine-owned SAP1 payload sections (HB2) to the StrataHub bundle
//! transport: content-addressed objects with reconstitution paths, plus the
//! JCS-canonical `Manifest`. All seven output invariants from the
//! coordination doc §3.3 are honored and pinned by tests.
//!
//! # Object layout
//!
//! ```text
//! control/bundle.json                                  bundle control document
//! branches/<branch>/<space>/<model>/<nnnn>.rows        SAP1 chunk (kv/json/event)
//! branches/<branch>/<space>/<model>/<name>/<nnnn>.rows SAP1 chunk (vector/graph)
//! ```
//!
//! Chunks split section bytes at fixed offsets; import concatenates chunks
//! in path order before decoding, so records may straddle chunk borders.
//!
//! # Determinism
//!
//! Manifest bytes are a pure function of source content: `created` derives
//! from the maximum exported row timestamp (never the wall clock), branch
//! `head_commit` hashes the branch's canonical control entry, and object
//! bytes inherit HB2's payload determinism.

use std::fs;
use std::path::{Path, PathBuf};

use serde_json::json;
use stratahub_protocol::{
    BranchEntry, BranchName, EngineCompatibility, Hash, HashAlgorithm, Manifest, ManifestObject,
    ObjectPath, MANIFEST_FORMAT_VERSION,
};
use time::OffsetDateTime;

use strata_engine::artifact::{ArtifactSection, BranchArtifact};
use strata_engine::{Database, DurableLocalOpenOptions, EngineErrorClass};

use crate::error::BundleExportError;
use crate::info::{engine_info, CAPABILITY_REGISTRY_VERSION};

/// Chunk target: sections split into 64 MiB objects (well under the 512 MB
/// transport cap; large enough to keep manifests far from their 1 MB cap).
const CHUNK_BYTES: usize = 64 * 1024 * 1024;

/// Semver range exported bundles require for reconstitution.
const REQUIRED_ENGINE_VERSION: &str = ">=1.0.0, <2.0.0";

/// Runtime state the source database never contains portable content for.
const NON_PORTABLE_TOP_LEVEL: &[&str] = &["locks"];

/// Export options (M8E2 `EngineExportOptions` shape).
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct EngineExportOptions {
    /// Emit schema + preview auxiliary blobs. Defaults to `true` per the
    /// M8E2 spec.
    pub emit_schema_preview: bool,
    /// Branches to export; empty means the `default` branch.
    pub branches: Vec<BranchName>,
}

impl Default for EngineExportOptions {
    fn default() -> Self {
        Self {
            emit_schema_preview: true,
            branches: Vec::new(),
        }
    }
}

/// blake3 hashes of the auxiliary blobs (M8E3 shape).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AuxiliaryHashes {
    /// Hash of the schema blob when present.
    pub schema: Option<Hash>,
    /// Hash of the preview blob when present.
    pub preview: Option<Hash>,
}

/// One content-addressed bundle object, fully materialized.
///
/// V1 buffers object bodies (they originate from buffered engine payload
/// sections); the HB5 trait impl adapts these into the async object stream
/// the ingest pipeline consumes.
#[derive(Clone, Debug)]
pub struct BundleObject {
    /// Content hash of `bytes`.
    pub hash: Hash,
    /// Reconstitution path within the bundle.
    pub path: ObjectPath,
    /// Exact byte length of `bytes`.
    pub size_bytes: u64,
    /// MIME hint; `None` means `application/octet-stream`.
    pub content_type: Option<String>,
    /// The object body.
    pub bytes: Vec<u8>,
}

/// Bundle export output (M8E2 `EngineExportOutput` shape, sync V1 form).
#[derive(Debug)]
pub struct EngineExportOutput {
    /// JCS-canonical manifest bytes; hash these to get the bundle id.
    pub manifest_canonical_bytes: Vec<u8>,
    /// The same manifest, typed.
    pub manifest: Manifest,
    /// Objects backing the manifest, in manifest order.
    pub objects: Vec<BundleObject>,
    /// Schema blob (HB4; `None` until schema generation lands).
    pub schema_blob: Option<Vec<u8>>,
    /// Preview blob (HB4; `None` until preview generation lands).
    pub preview_blob: Option<Vec<u8>>,
    /// Hashes of the auxiliary blobs when present.
    pub auxiliary_hashes: AuxiliaryHashes,
}

/// Export handle over a read-only source database.
///
/// `open` copies the source into a scratch directory (excluding
/// non-portable runtime state) and operates on the copy, so the source is
/// never mutated — not even lock files (coordination doc §3.3 invariant 7).
pub struct StrataCoreEngine {
    database: Database,
    source_path: PathBuf,
    _scratch: tempfile::TempDir,
}

impl StrataCoreEngine {
    /// Opens the Strata database at `source_path` for export.
    ///
    /// # Errors
    ///
    /// [`BundleExportError::NotAStrataDb`] when the directory is missing or
    /// does not hold a V1 database; [`BundleExportError::Io`] on copy
    /// failures; [`BundleExportError::Internal`] on engine open failures.
    pub fn open(source_path: &Path) -> Result<Self, BundleExportError> {
        if !source_path.is_dir() {
            return Err(BundleExportError::NotAStrataDb(source_path.to_owned()));
        }
        let scratch = tempfile::tempdir()?;
        copy_database(source_path, scratch.path())?;

        let database = match Database::open_local(scratch.path(), DurableLocalOpenOptions::new()) {
            Ok(outcome) => {
                if outcome.summary().created() {
                    // The open would have CREATED a fresh layout — the
                    // source held no database. Export never fabricates.
                    return Err(BundleExportError::NotAStrataDb(source_path.to_owned()));
                }
                outcome.into_database()
            }
            Err(error) => {
                return Err(match error.class() {
                    EngineErrorClass::IncompatibleLayout => {
                        BundleExportError::NotAStrataDb(source_path.to_owned())
                    }
                    _ => BundleExportError::Internal {
                        detail: format!("source database failed to open: {}", error.code()),
                    },
                });
            }
        };
        Ok(Self {
            database,
            source_path: source_path.to_owned(),
            _scratch: scratch,
        })
    }

    /// The source path this handle exports from.
    #[must_use]
    pub fn source_path(&self) -> &Path {
        &self.source_path
    }

    /// Reports the engine's version and capabilities (M8E2 shape).
    #[must_use]
    pub fn engine_info(&self) -> crate::EngineInfo {
        engine_info()
    }

    /// Exports the requested branches as a StrataHub bundle.
    ///
    /// # Errors
    ///
    /// [`BundleExportError::BranchNotFound`] for unknown branches;
    /// [`BundleExportError::Internal`] on engine or manifest failures.
    pub fn export_bundle(
        &mut self,
        options: &EngineExportOptions,
    ) -> Result<EngineExportOutput, BundleExportError> {
        let branches = resolve_branches(options)?;

        let mut objects = Vec::new();
        let mut branch_entries = Vec::new();
        let mut control_branches = Vec::new();
        let mut max_micros: Option<u64> = None;
        let mut used_models = std::collections::BTreeSet::new();

        let mut schema_preview = None;
        let mut branch_samples = Vec::new();
        for (position, branch) in branches.iter().enumerate() {
            let artifact = self.export_branch(branch)?;
            branch_samples.push(stratahub_protocol::wire::BranchSampleEntry {
                name: branch.clone(),
                created: content_derived_created(
                    artifact
                        .max_row_timestamp()
                        .map(strata_core::Timestamp::as_micros),
                )?,
                is_default: position == 0,
            });
            if position == 0 && options.emit_schema_preview {
                schema_preview = Some(crate::schema_preview::generate(&artifact)?);
            }
            if let Some(timestamp) = artifact.max_row_timestamp() {
                max_micros = Some(
                    max_micros.map_or(timestamp.as_micros(), |max| max.max(timestamp.as_micros())),
                );
            }

            let mut section_summaries = Vec::new();
            for section in artifact.sections() {
                used_models.insert(section.model().as_str());
                let chunk_hashes =
                    chunk_section(branch, section, &mut objects, &mut section_summaries)?;
                debug_assert!(!chunk_hashes.is_empty());
            }

            let control_entry = json!({
                "name": branch.as_str(),
                "spaces": artifact
                    .spaces()
                    .iter()
                    .map(|space| space.as_str().to_owned())
                    .collect::<Vec<_>>(),
                "sections": section_summaries,
                "max_row_timestamp_micros": artifact.max_row_timestamp().map(strata_core::Timestamp::as_micros),
            });
            let head_commit = hash_canonical(&control_entry)?;
            branch_entries.push(BranchEntry {
                name: branch.clone(),
                head_commit: head_commit.as_str().to_owned(),
            });
            control_branches.push(control_entry);
        }

        let control = json!({
            "bundle_control_version": 1,
            "branches": control_branches,
        });
        let control_bytes = serde_jcs::to_vec(&control)
            .map_err(|error| internal(format!("control document canonicalization: {error}")))?;
        let control_object = bundle_object(
            "control/bundle.json",
            control_bytes,
            Some("application/json".to_owned()),
        )?;
        objects.insert(0, control_object);

        let (schema_blob, preview_blob, auxiliary_hashes) =
            auxiliary_blobs(schema_preview, branch_samples)?;

        let mut manifest = Self::build_manifest(
            &branches,
            branch_entries,
            &objects,
            max_micros,
            &used_models,
        )?;
        manifest.schema_hash.clone_from(&auxiliary_hashes.schema);
        manifest.preview_hash.clone_from(&auxiliary_hashes.preview);
        manifest
            .validate()
            .map_err(|error| internal(format!("exported manifest failed validation: {error}")))?;
        let manifest_canonical_bytes = manifest
            .canonical_bytes()
            .map_err(|error| internal(format!("manifest canonicalization: {error}")))?;

        Ok(EngineExportOutput {
            manifest_canonical_bytes,
            manifest,
            objects,
            schema_blob,
            preview_blob,
            auxiliary_hashes,
        })
    }

    fn export_branch(&mut self, branch: &BranchName) -> Result<BranchArtifact, BundleExportError> {
        let engine_branch = strata_engine::BranchName::new(branch.as_str())
            .map_err(|_| BundleExportError::BranchNotFound(branch.as_str().to_owned()))?;
        self.database
            .export_branch_artifact(&engine_branch)
            .map_err(|error| match error.class() {
                EngineErrorClass::NotFound => {
                    BundleExportError::BranchNotFound(branch.as_str().to_owned())
                }
                _ => internal(format!("branch export failed: {}", error.code())),
            })
    }

    fn build_manifest(
        branches: &[BranchName],
        branch_entries: Vec<BranchEntry>,
        objects: &[BundleObject],
        max_micros: Option<u64>,
        used_models: &std::collections::BTreeSet<&'static str>,
    ) -> Result<Manifest, BundleExportError> {
        // #3198: capabilities are derived from the data models present (and
        // `branches` for a multi-branch bundle). A dataset clone is
        // point-in-time — it carries current values, not version history — so
        // `time_travel` is deliberately never among them. A history-carrying
        // export would add it here; see the dataset-clone-artifact contract.
        let mut required_capabilities: Vec<String> = used_models
            .iter()
            .map(|model| capability_for(model))
            .collect();
        if branches.len() > 1 {
            required_capabilities.push("branches".to_owned());
        }
        required_capabilities.sort();
        required_capabilities.dedup();

        Ok(Manifest {
            manifest_format_version: MANIFEST_FORMAT_VERSION,
            bundle_hash_algorithm: HashAlgorithm::Blake3,
            created: content_derived_created(max_micros)?,
            engine_compatibility: EngineCompatibility {
                required_engine_version: REQUIRED_ENGINE_VERSION.to_owned(),
                capability_registry_version: CAPABILITY_REGISTRY_VERSION,
                required_capabilities,
            },
            default_branch: branches[0].clone(),
            branches: branch_entries,
            total_size_bytes: objects.iter().map(|object| object.size_bytes).sum(),
            object_count: objects.len() as u64,
            objects: objects
                .iter()
                .map(|object| ManifestObject {
                    hash: object.hash.clone(),
                    path: object.path.clone(),
                    size_bytes: object.size_bytes,
                    content_type: object.content_type.clone(),
                })
                .collect(),
            schema_hash: None,
            preview_hash: None,
            base_manifest_hash: None,
            subject: None,
        })
    }
}

type AuxiliaryBlobs = (Option<Vec<u8>>, Option<Vec<u8>>, AuxiliaryHashes);

/// Canonicalizes and hashes the schema + preview blobs when generated.
fn auxiliary_blobs(
    schema_preview: Option<(
        stratahub_protocol::wire::DatasetSchema,
        stratahub_protocol::wire::SamplePreview,
    )>,
    branch_samples: Vec<stratahub_protocol::wire::BranchSampleEntry>,
) -> Result<AuxiliaryBlobs, BundleExportError> {
    let Some((schema, mut preview)) = schema_preview else {
        return Ok((None, None, AuxiliaryHashes::default()));
    };
    preview.branches = Some(branch_samples);
    let schema_blob = serde_jcs::to_vec(&schema)
        .map_err(|error| internal(format!("schema canonicalization: {error}")))?;
    let preview_blob = serde_jcs::to_vec(&preview)
        .map_err(|error| internal(format!("preview canonicalization: {error}")))?;
    let hashes = AuxiliaryHashes {
        schema: Some(stratahub_protocol::hash_bytes(&schema_blob)),
        preview: Some(stratahub_protocol::hash_bytes(&preview_blob)),
    };
    Ok((Some(schema_blob), Some(preview_blob), hashes))
}

fn resolve_branches(options: &EngineExportOptions) -> Result<Vec<BranchName>, BundleExportError> {
    if options.branches.is_empty() {
        let default = BranchName::parse("default")
            .map_err(|error| internal(format!("default branch name: {error}")))?;
        return Ok(vec![default]);
    }
    Ok(options.branches.clone())
}

/// Splits one payload section into chunk objects; returns the chunk hashes
/// and records a section summary for the control document.
fn chunk_section(
    branch: &BranchName,
    section: &ArtifactSection,
    objects: &mut Vec<BundleObject>,
    section_summaries: &mut Vec<serde_json::Value>,
) -> Result<Vec<Hash>, BundleExportError> {
    let mut chunk_hashes = Vec::new();
    let chunks: Vec<&[u8]> = if section.bytes().is_empty() {
        Vec::new()
    } else {
        section.bytes().chunks(CHUNK_BYTES).collect()
    };
    for (index, chunk) in chunks.iter().enumerate() {
        let path = section_chunk_path(branch, section, index);
        let object = bundle_object(&path, chunk.to_vec(), None)?;
        chunk_hashes.push(object.hash.clone());
        objects.push(object);
    }
    section_summaries.push(json!({
        "space": section.space().as_str(),
        "model": section.model().as_str(),
        "qualifier": section.qualifier(),
        "record_count": section.record_count(),
        "chunks": chunk_hashes
            .iter()
            .map(|hash| hash.as_str().to_owned())
            .collect::<Vec<_>>(),
    }));
    Ok(chunk_hashes)
}

fn section_chunk_path(branch: &BranchName, section: &ArtifactSection, index: usize) -> String {
    let mut path = format!(
        "branches/{}/{}/{}",
        branch.as_str(),
        section.space().as_str(),
        section.model().as_str()
    );
    if let Some(qualifier) = section.qualifier() {
        path.push('/');
        path.push_str(qualifier);
    }
    let _ = std::fmt::Write::write_fmt(&mut path, format_args!("/{index:04}.rows"));
    path
}

fn bundle_object(
    path: &str,
    bytes: Vec<u8>,
    content_type: Option<String>,
) -> Result<BundleObject, BundleExportError> {
    let path = ObjectPath::parse(path)
        .map_err(|error| internal(format!("object path `{path}`: {error}")))?;
    let hash = stratahub_protocol::hash_bytes(&bytes);
    let size_bytes = bytes.len() as u64;
    Ok(BundleObject {
        hash,
        path,
        size_bytes,
        content_type,
        bytes,
    })
}

fn hash_canonical(value: &serde_json::Value) -> Result<Hash, BundleExportError> {
    let bytes = serde_jcs::to_vec(value)
        .map_err(|error| internal(format!("control entry canonicalization: {error}")))?;
    Ok(stratahub_protocol::hash_bytes(&bytes))
}

/// `created` derives from content (max exported row timestamp), never the
/// wall clock — the round-trip conformance property (export → import →
/// re-export yields the identical manifest hash) forbids wall-clock fields.
/// An empty bundle uses the Unix epoch.
fn content_derived_created(max_micros: Option<u64>) -> Result<OffsetDateTime, BundleExportError> {
    let micros = i128::from(max_micros.unwrap_or(0));
    OffsetDateTime::from_unix_timestamp_nanos(micros * 1_000)
        .map_err(|error| internal(format!("created timestamp out of range: {error}")))
}

fn capability_for(model: &str) -> String {
    // Engine artifact models map onto the protocol's primitive vocabulary
    // (`PrimitiveType`'s lowercase wire strings). `graph` gained its variant
    // in stratahub#11 and falls through `other` at the same spelling.
    match model {
        "event" => "events".to_owned(),
        "vector" => "vectors".to_owned(),
        other => other.to_owned(),
    }
}

fn internal(detail: String) -> BundleExportError {
    BundleExportError::Internal { detail }
}

/// Copies the source database into the scratch directory, excluding
/// non-portable top-level runtime state (lock files must not travel: a
/// copied lock could wedge the scratch open, and locks are not content).
fn copy_database(source: &Path, target: &Path) -> Result<(), BundleExportError> {
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let name = entry.file_name();
        if NON_PORTABLE_TOP_LEVEL
            .iter()
            .any(|excluded| name.to_str() == Some(excluded))
        {
            continue;
        }
        let source_path = entry.path();
        let target_path = target.join(&name);
        if entry.file_type()?.is_dir() {
            fs::create_dir_all(&target_path)?;
            copy_database(&source_path, &target_path)?;
        } else {
            fs::copy(&source_path, &target_path)?;
        }
    }
    Ok(())
}
