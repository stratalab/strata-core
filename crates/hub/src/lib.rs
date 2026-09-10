//! StrataHub adapter: bundle export/import and hub orchestration over the
//! Strata engine.
//!
//! This crate is the single home for hub-facing core features: the bundle
//! export adapter consumed by StrataHub's ingest pipeline, bundle import
//! for clone reconstitution, remote-tracking metadata, and (behind the
//! executor `hub.*` commands) clone orchestration. The engine never sees
//! StrataHub wire types; this crate maps engine-owned artifact payloads to
//! the StrataHub transport contract.
//!
//! Contract sources: stratahub `strata-core-requirements-for-stratahub-v1.md`
//! §3 and `stratahub-v1-bundle-format.md` §3-§5; strata-core
//! `docs/architecture/engine/dataset-clone-artifact-contract.md`; the local
//! slice plan in `docs/design/hub-bundle-adapter-plan.md`.
//!
//! [`IngestEngine`] implements `stratahub-ingest`'s `Engine` trait by pure
//! delegation over [`StrataCoreEngine`], and [`ClientTransport`] binds
//! `stratahub-client` to the clone orchestration's transport seam.

#![deny(unsafe_code)]

mod clone;
mod error;
mod export;
mod import;
mod info;
#[cfg(feature = "ingest")]
mod ingest_adapter;
mod remote;
mod resolve;
mod schema_preview;
mod transport;

pub use stratahub_client::{ClientError, DatasetFilter, ListPageReq, SortKey};
pub use stratahub_protocol;

pub use clone::{
    clone_dataset, CloneError, CloneOutcome, CloneProgress, CloneRequest, HubTransport,
};
pub use error::BundleExportError;
pub use export::{
    AuxiliaryHashes, BundleObject, EngineExportOptions, EngineExportOutput, StrataCoreEngine,
};
pub use import::{import_bundle, BundleImportError};
pub use info::{engine_info, EngineInfo, CAPABILITY_REGISTRY_VERSION};
#[cfg(feature = "ingest")]
pub use ingest_adapter::IngestEngine;
pub use remote::{
    read_remote_tracking_ref, write_remote_tracking_ref, RemoteRefError, RemoteTrackingRef,
};
pub use resolve::{
    global_config_path, read_global_hub_url, read_provider_key, resolve_hub_url,
    unset_global_hub_url, unset_global_provider_key, write_global_hub_url,
    write_global_provider_key, HubUrlError, HubUrlInputs, HubUrlSource, ResolvedHubUrl,
    DEFAULT_HUB_URL,
};
pub use transport::ClientTransport;
