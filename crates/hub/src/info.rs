//! Engine version + capability advertisement (coordination doc §3.8).

use stratahub_protocol::wire::PrimitiveType;

/// Capability registry version 1: kv, json, vectors, events, branches.
///
/// Registry versions are monotonic supersets — a later version never
/// removes a capability (coordination doc §3.8 invariant 2).
///
/// `PrimitiveType` can now carry `graph` (stratahub#11), so this engine can
/// *read* a graph-bearing dataset card. Advertising graph here is the
/// separate half: it grows the registry, which is a version bump stratahub
/// validates `required_capabilities` against, so it is coordinated rather
/// than appended (#3449).
pub const CAPABILITY_REGISTRY_VERSION: u32 = 1;

/// What the engine reports about itself (M8E2 `EngineInfo` shape).
///
/// `version` parses with `semver::Version::parse` and is constant for a
/// given binary; `capability_registry_version` matches the value exported
/// manifests declare in `engine_compatibility.capability_registry_version`;
/// `supported_primitives` is the full supported set, of which any bundle's
/// `required_capabilities` must be a subset.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EngineInfo {
    /// Semver version of the engine implementation.
    pub version: String,
    /// Capability-registry version the engine implements.
    pub capability_registry_version: u32,
    /// The primitives the engine supports.
    pub supported_primitives: Vec<PrimitiveType>,
}

/// Reports the engine's version and capabilities.
///
/// Callable repeatedly without side effects; the returned value is
/// constant for a given binary (coordination doc §3.8 invariant 1).
#[must_use]
pub fn engine_info() -> EngineInfo {
    EngineInfo {
        version: env!("CARGO_PKG_VERSION").to_owned(),
        capability_registry_version: CAPABILITY_REGISTRY_VERSION,
        supported_primitives: vec![
            PrimitiveType::Kv,
            PrimitiveType::Json,
            PrimitiveType::Vectors,
            PrimitiveType::Events,
            PrimitiveType::Branches,
        ],
    }
}
