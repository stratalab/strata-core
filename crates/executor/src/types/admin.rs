use super::{Deserialize, Serialize, VectorDistanceMetric};

/// Database open target exposed in admin outputs.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum AdminOpenTarget {
    /// Volatile cache-backed database.
    Cache,
    /// Durable local filesystem-backed database.
    DurableLocal,
}

/// Health status exposed in admin outputs.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum AdminHealthStatus {
    /// All required facts are healthy.
    Healthy,
    /// Database is available with warnings.
    Degraded,
    /// A required subsystem is missing, corrupt, unavailable, or closed.
    Unhealthy,
}

/// Control-plane status exposed in admin health outputs.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum AdminControlStatus {
    /// Required facts are healthy.
    Healthy,
    /// Requested facts are missing.
    Missing,
    /// Required facts are corrupt.
    Corrupt,
    /// Required facts are unavailable.
    Unavailable,
}

/// Liveness probe output.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct AdminPing {
    /// Engine package version.
    pub version: String,
}

/// Multi-process IPC stop output.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct AdminIpcStop {
    /// True when a running host was stopped; false when this process was not
    /// hosting a socket (nothing to stop).
    pub stopped: bool,
}

/// Multi-process IPC status output.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct AdminIpcStatus {
    /// True when this process owns the store (holds the writer lock); false
    /// when it is a client of another same-machine owner.
    pub is_owner: bool,
    /// True when this process is hosting a broker socket for other processes.
    pub hosting: bool,
    /// The hosted (or connected) socket path, when one exists.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub socket_path: Option<String>,
    /// The hosting owner's process id, when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_pid: Option<u64>,
    /// Number of clients currently connected to the host (0 when not hosting).
    pub client_count: u64,
    /// The connected clients, as their hellos introduced them; a protocol-1
    /// (pre-hello) connection appears anonymous. Empty when not hosting.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub clients: Vec<AdminIpcClient>,
}

/// One connected IPC client, as reported by `ipc_status`. Display identity
/// only — the socket's same-user permission check is the security boundary.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct AdminIpcClient {
    /// Client-reported product name (`strata-vscode`, `strata`), if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Client-reported version, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    /// Client-reported process id, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pid: Option<u64>,
    /// The session access this connection was granted.
    pub access: crate::SessionAccess,
    /// The negotiated wire protocol revision (1 = legacy, no hello).
    pub protocol: u32,
}

/// Database information output.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct AdminDatabaseInfo {
    /// Engine package version.
    pub version: String,
    /// Open target.
    pub target: AdminOpenTarget,
    /// True when this open created a new database.
    pub created: bool,
    /// True when storage is durable.
    pub durable: bool,
    /// Default product branch.
    pub default_branch: String,
    /// Active branch count.
    pub branch_count: u64,
    /// Registered space count for the selected branch.
    pub space_count: u64,
    /// Storage memory budget: its total and where it came from (#2905).
    pub memory_budget: AdminMemoryBudget,
    /// True while the database handle is open.
    pub open: bool,
}

/// Storage memory budget provenance and total for `admin.info`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct AdminMemoryBudget {
    /// The resolved storage memory budget total, in bytes.
    pub total_bytes: u64,
    /// Where the budget came from.
    pub source: AdminMemoryBudgetSource,
    /// The usable host memory the derivation started from, in bytes — present
    /// only when `source` is `derived_from_host`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usable_host_bytes: Option<u64>,
}

/// How an opened database's storage memory budget was chosen.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum AdminMemoryBudgetSource {
    /// The caller set a memory budget explicitly.
    Explicit,
    /// Derived at open from host memory (25% of usable memory, clamped to `[1 MiB, 8 GiB]`).
    DerivedFromHost,
    /// The fixed built-in default (the host reported no memory facts, or a deterministic open).
    FixedDefault,
}

/// Health output.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct AdminHealth {
    /// Worst health status.
    pub status: AdminHealthStatus,
    /// Database identity status.
    pub identity: AdminControlStatus,
    /// Registry status.
    pub registry: AdminControlStatus,
    /// Branch catalog status.
    pub branch_catalog: AdminControlStatus,
    /// Optional branch-local space catalog status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub space_catalog: Option<AdminControlStatus>,
    /// Default product branch.
    pub default_branch: String,
    /// Active branch count.
    pub branch_count: u64,
}

/// Metrics output.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct AdminMetrics {
    /// Open target.
    pub target: AdminOpenTarget,
    /// True when storage is durable.
    pub durable: bool,
    /// True while the database handle is open.
    pub open: bool,
    /// Active branch count.
    pub branch_count: u64,
    /// Registered space count for the selected branch.
    pub space_count: u64,
    /// Control-plane health status.
    pub control_status: AdminHealthStatus,
}

/// Sanitized config output.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct AdminConfig {
    /// Open target.
    pub target: AdminOpenTarget,
    /// True when this open created a new database.
    pub created: bool,
    /// True when storage is durable.
    pub durable: bool,
    /// Default product branch.
    pub default_branch: String,
}

/// Capability flags in describe output.
#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct AdminCapabilities {
    /// KV primitive is available.
    pub kv: bool,
    /// JSON primitive is available.
    pub json: bool,
    /// Event primitive is available.
    pub event: bool,
    /// Vector primitive is available.
    pub vector: bool,
    /// Vector index query path is available.
    pub vector_index: bool,
    /// Graph core primitive is available.
    pub graph_core: bool,
    /// Arrow command surface is available.
    pub arrow: bool,
    /// Inference command surface is available.
    pub inference: bool,
}

/// Vector collection summary in describe output.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct AdminVectorCollection {
    /// Collection name.
    pub name: String,
    /// Embedding dimension.
    pub dimension: usize,
    /// Distance metric.
    pub metric: VectorDistanceMetric,
    /// Visible vector count.
    pub count: u64,
}

/// Graph summary in describe output.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct AdminGraph {
    /// Graph name.
    pub name: String,
    /// Visible node count.
    pub node_count: u64,
    /// Visible edge count.
    pub edge_count: u64,
}

/// Primitive summaries in describe output.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct AdminPrimitives {
    /// Visible KV row count in the described space.
    pub kv_count: u64,
    /// Visible JSON document count in the described space.
    pub json_count: u64,
    /// Visible event count in the described space.
    pub event_count: u64,
    /// Vector collection summaries.
    pub vector_collections: Vec<AdminVectorCollection>,
    /// Graph summaries.
    pub graphs: Vec<AdminGraph>,
}

/// Database describe output.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct AdminDescribe {
    /// Engine package version.
    pub version: String,
    /// Open target.
    pub target: AdminOpenTarget,
    /// Default product branch.
    pub default_branch: String,
    /// Described branch.
    pub branch: String,
    /// Active branches.
    pub branches: Vec<String>,
    /// Registered product spaces on the described branch.
    pub spaces: Vec<String>,
    /// Primitive summaries.
    pub primitives: AdminPrimitives,
    /// Sanitized config.
    pub config: AdminConfig,
    /// Available rebuilt capabilities.
    pub capabilities: AdminCapabilities,
}
