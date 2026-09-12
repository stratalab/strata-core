//! Safe database administration and introspection API.

use crate::branch::BranchName;
use crate::control::{space as control_space, ControlPlane};
use crate::data::event::EventService;
use crate::data::graph::GraphService;
use crate::data::json::JsonService;
use crate::data::kv::{KvService, ProductSpace};
use crate::data::vector::{
    VectorArtifactStore, VectorCollectionInfo, VectorDistanceMetric, VectorService,
};
use crate::diagnostics::EngineError;
use crate::persistence::StoragePersistence;

use super::{
    ControlHealthStatus, DatabaseOpenSummary, DatabaseOpenTarget, MemoryBudgetSource,
    SpaceCatalogDiagnostics,
};

/// Health status reported by admin commands.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminHealthStatus {
    /// All required facts are healthy.
    Healthy,
    /// The control plane is unavailable but can still answer reads and
    /// inspection; writes need recovery.
    Degraded,
    /// A required diagnostic area is missing or corrupt.
    Unhealthy,
}

/// Ping summary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminPingSummary {
    /// Engine package version.
    pub version: String,
}

/// Database information summary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminDatabaseInfo {
    /// Engine package version.
    pub version: String,
    /// Open target.
    pub target: DatabaseOpenTarget,
    /// True when this open created a new database.
    pub created: bool,
    /// True when storage is durable.
    pub durable: bool,
    /// Default product branch.
    pub default_branch: BranchName,
    /// Active branch count.
    pub branch_count: u64,
    /// Registered space count for the selected branch.
    pub space_count: u64,
    /// Provenance and total of the storage memory budget (#2905).
    pub memory_budget: MemoryBudgetSource,
    /// True while the database handle is open.
    pub open: bool,
}

/// Health summary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminHealthSummary {
    /// Worst health status.
    pub status: AdminHealthStatus,
    /// Database identity/control status.
    pub identity: ControlHealthStatus,
    /// Registry status.
    pub registry: ControlHealthStatus,
    /// Branch catalog status.
    pub branch_catalog: ControlHealthStatus,
    /// Optional branch-local space catalog status.
    pub space_catalog: Option<ControlHealthStatus>,
    /// Default product branch.
    pub default_branch: BranchName,
    /// Active branch count.
    pub branch_count: u64,
}

/// Metrics summary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminMetricsSummary {
    /// Open target.
    pub target: DatabaseOpenTarget,
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

/// Sanitized config summary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminConfigSummary {
    /// Open target.
    pub target: DatabaseOpenTarget,
    /// True when this open created a new database.
    pub created: bool,
    /// True when storage is durable.
    pub durable: bool,
    /// Default product branch.
    pub default_branch: BranchName,
}

/// Primitive capability flags.
#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminCapabilitySummary {
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
    /// Arrow command surface is available in executor builds that enable it.
    pub arrow: bool,
    /// Inference command surface is available in executor builds that enable it.
    pub inference: bool,
}

/// Vector collection summary used by describe.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminVectorCollectionSummary {
    /// Collection name.
    pub name: String,
    /// Embedding dimension.
    pub dimension: usize,
    /// Distance metric.
    pub metric: VectorDistanceMetric,
    /// Visible vector count.
    pub count: u64,
}

impl From<&VectorCollectionInfo> for AdminVectorCollectionSummary {
    fn from(value: &VectorCollectionInfo) -> Self {
        Self {
            name: value.name().as_str().to_owned(),
            dimension: value.config().dimension(),
            metric: value.config().metric(),
            count: value.count(),
        }
    }
}

/// Graph summary used by describe.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminGraphSummary {
    /// Graph name.
    pub name: String,
    /// Visible node count.
    pub node_count: u64,
    /// Visible edge count.
    pub edge_count: u64,
}

/// Primitive counts and summaries used by describe.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminPrimitiveSummary {
    /// Visible KV row count in the described space.
    pub kv_count: u64,
    /// Visible JSON document count in the described space.
    pub json_count: u64,
    /// Visible event count in the described space.
    pub event_count: u64,
    /// Vector collections in the described space.
    pub vector_collections: Vec<AdminVectorCollectionSummary>,
    /// Graphs in the described space.
    pub graphs: Vec<AdminGraphSummary>,
}

/// Database describe snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminDescribeSummary {
    /// Engine package version.
    pub version: String,
    /// Open target.
    pub target: DatabaseOpenTarget,
    /// Default product branch.
    pub default_branch: BranchName,
    /// Described branch.
    pub branch: BranchName,
    /// Active branch names.
    pub branches: Vec<BranchName>,
    /// Registered spaces on the described branch.
    pub spaces: Vec<ProductSpace>,
    /// Primitive summaries for the default space.
    pub primitives: AdminPrimitiveSummary,
    /// Sanitized config summary.
    pub config: AdminConfigSummary,
    /// Available rebuilt capabilities.
    pub capabilities: AdminCapabilitySummary,
}

/// Read-only database administration service.
pub struct AdminService<'a> {
    persistence: &'a mut StoragePersistence,
    control: &'a mut ControlPlane,
    vector_artifacts: &'a mut VectorArtifactStore,
    summary: DatabaseOpenSummary,
    open: bool,
}

impl<'a> AdminService<'a> {
    pub(crate) const fn new(
        persistence: &'a mut StoragePersistence,
        control: &'a mut ControlPlane,
        vector_artifacts: &'a mut VectorArtifactStore,
        summary: DatabaseOpenSummary,
        open: bool,
    ) -> Self {
        Self {
            persistence,
            control,
            vector_artifacts,
            summary,
            open,
        }
    }

    /// Returns a lightweight liveness response.
    pub fn ping(&self) -> AdminPingSummary {
        AdminPingSummary {
            version: env!("CARGO_PKG_VERSION").to_owned(),
        }
    }

    /// Returns stable database facts.
    pub fn info(&mut self, branch: Option<&BranchName>) -> Result<AdminDatabaseInfo, EngineError> {
        let branch = self.resolved_branch(branch).clone();
        let space_count = self.space_count(&branch)?;
        Ok(AdminDatabaseInfo {
            version: env!("CARGO_PKG_VERSION").to_owned(),
            target: self.summary.target(),
            created: self.summary.created(),
            durable: self.summary.durable(),
            default_branch: self.control.default_branch().clone(),
            branch_count: u64::try_from(self.control.active_branch_count()).unwrap_or(u64::MAX),
            space_count,
            memory_budget: self.summary.memory_budget_source(),
            open: self.open,
        })
    }

    /// Returns control-plane health facts.
    pub fn health(&mut self, branch: Option<&BranchName>) -> AdminHealthSummary {
        let diagnostics = self.control.diagnostics(self.persistence, branch);
        let space_catalog = diagnostics
            .space_catalog()
            .map(SpaceCatalogDiagnostics::status);
        let status = worst_status([
            diagnostics.identity_status(),
            diagnostics.registry_status(),
            diagnostics.branch_catalog_status(),
            space_catalog.unwrap_or(ControlHealthStatus::Healthy),
        ]);
        AdminHealthSummary {
            status,
            identity: diagnostics.identity_status(),
            registry: diagnostics.registry_status(),
            branch_catalog: diagnostics.branch_catalog_status(),
            space_catalog,
            default_branch: diagnostics.default_branch().clone(),
            branch_count: u64::try_from(diagnostics.active_branch_count()).unwrap_or(u64::MAX),
        }
    }

    /// Returns compact metrics facts.
    pub fn metrics(
        &mut self,
        branch: Option<&BranchName>,
    ) -> Result<AdminMetricsSummary, EngineError> {
        let branch = self.resolved_branch(branch).clone();
        let health = self.health(Some(&branch));
        Ok(AdminMetricsSummary {
            target: self.summary.target(),
            durable: self.summary.durable(),
            open: self.open,
            branch_count: u64::try_from(self.control.active_branch_count()).unwrap_or(u64::MAX),
            space_count: self.space_count(&branch)?,
            control_status: health.status,
        })
    }

    /// Returns a structured database snapshot for agent/CLI introspection.
    pub fn describe(
        &mut self,
        branch: Option<&BranchName>,
    ) -> Result<AdminDescribeSummary, EngineError> {
        self.control.require_healthy()?;
        let branch = self.resolved_branch(branch).clone();
        let space = ProductSpace::new(control_space::DEFAULT_SPACE)?;
        let branches = self
            .control
            .list_branches()
            .into_iter()
            .map(|record| record.name().clone())
            .collect();
        let record = self.branch_record(&branch)?;
        let spaces = control_space::registered_spaces(self.persistence, &record)?;
        let primitives = self.primitive_summary(&branch, &space)?;
        Ok(AdminDescribeSummary {
            version: env!("CARGO_PKG_VERSION").to_owned(),
            target: self.summary.target(),
            default_branch: self.control.default_branch().clone(),
            branch,
            branches,
            spaces,
            primitives,
            config: self.config(),
            capabilities: AdminCapabilitySummary {
                kv: true,
                json: true,
                event: true,
                vector: true,
                vector_index: true,
                graph_core: true,
                arrow: false,
                inference: false,
            },
        })
    }

    /// Returns sanitized config facts.
    pub fn config(&self) -> AdminConfigSummary {
        AdminConfigSummary {
            target: self.summary.target(),
            created: self.summary.created(),
            durable: self.summary.durable(),
            default_branch: self.control.default_branch().clone(),
        }
    }

    /// Returns one sanitized config value from the allowlist.
    pub fn config_value(&self, key: &str) -> Result<Option<String>, EngineError> {
        match key.trim().to_ascii_lowercase().as_str() {
            "target" => Ok(Some(match self.summary.target() {
                DatabaseOpenTarget::Cache => "cache".to_owned(),
                DatabaseOpenTarget::DurableLocal => "durable_local".to_owned(),
            })),
            "created" => Ok(Some(self.summary.created().to_string())),
            "durable" => Ok(Some(self.summary.durable().to_string())),
            "default_branch" => Ok(Some(self.control.default_branch().as_str().to_owned())),
            "" => Err(EngineError::invalid_input(
                "invalid_argument.engine.config_key",
                "config key must not be empty",
            )),
            _ => Ok(None),
        }
    }

    fn primitive_summary(
        &mut self,
        branch: &BranchName,
        space: &ProductSpace,
    ) -> Result<AdminPrimitiveSummary, EngineError> {
        let kv_count = {
            let mut service = KvService::new(
                self.persistence,
                self.control,
                branch.clone(),
                space.clone(),
            );
            service.count(None)?
        };
        let json_count = {
            let mut service = JsonService::new(
                self.persistence,
                self.control,
                branch.clone(),
                space.clone(),
            );
            service.count(None)?
        };
        let event_count = {
            let mut service = EventService::new(
                self.persistence,
                self.control,
                branch.clone(),
                space.clone(),
            );
            service.len()?.count()
        };
        let vector_collections = {
            let mut service = VectorService::new(
                self.persistence,
                self.control,
                self.vector_artifacts,
                branch.clone(),
                space.clone(),
            );
            service
                .list_collections()?
                .iter()
                .map(AdminVectorCollectionSummary::from)
                .collect()
        };
        let graphs = {
            let mut service = GraphService::new(
                self.persistence,
                self.control,
                branch.clone(),
                space.clone(),
            );
            let page = service.list_graphs(None, usize::MAX)?;
            let mut summaries = Vec::with_capacity(page.graphs().len());
            for graph in page.graphs() {
                if let Some(info) = service.graph_info(graph)? {
                    summaries.push(AdminGraphSummary {
                        name: info.name().as_str().to_owned(),
                        node_count: info.node_count(),
                        edge_count: info.edge_count(),
                    });
                }
            }
            summaries
        };
        Ok(AdminPrimitiveSummary {
            kv_count,
            json_count,
            event_count,
            vector_collections,
            graphs,
        })
    }

    fn resolved_branch<'b>(&'b self, branch: Option<&'b BranchName>) -> &'b BranchName {
        branch.unwrap_or_else(|| self.control.default_branch())
    }

    fn branch_record(
        &self,
        branch: &BranchName,
    ) -> Result<crate::branch::catalog::BranchCatalogRecord, EngineError> {
        self.control.lookup_branch(branch).cloned().ok_or_else(|| {
            EngineError::not_found(
                "not_found.engine.branch",
                format!("branch `{branch}` does not exist"),
            )
        })
    }

    fn space_count(&mut self, branch: &BranchName) -> Result<u64, EngineError> {
        let record = self.branch_record(branch)?;
        let count = control_space::registered_spaces(self.persistence, &record)?.len();
        Ok(u64::try_from(count).unwrap_or(u64::MAX))
    }
}

fn worst_status(statuses: impl IntoIterator<Item = ControlHealthStatus>) -> AdminHealthStatus {
    statuses
        .into_iter()
        .map(admin_status)
        .max_by_key(|status| match status {
            AdminHealthStatus::Healthy => 0,
            AdminHealthStatus::Degraded => 1,
            AdminHealthStatus::Unhealthy => 2,
        })
        .unwrap_or(AdminHealthStatus::Healthy)
}

const fn admin_status(status: ControlHealthStatus) -> AdminHealthStatus {
    match status {
        ControlHealthStatus::Healthy => AdminHealthStatus::Healthy,
        // An unavailable (fail-closed) control plane can still answer reads and
        // inspection; writes need recovery. That is degraded, not fully down.
        ControlHealthStatus::Unavailable => AdminHealthStatus::Degraded,
        ControlHealthStatus::Missing | ControlHealthStatus::Corrupt => AdminHealthStatus::Unhealthy,
    }
}

#[cfg(test)]
mod tests {
    use super::{admin_status, AdminHealthStatus, ControlHealthStatus};

    #[test]
    fn unavailable_control_plane_is_degraded_not_unhealthy() {
        assert_eq!(
            admin_status(ControlHealthStatus::Healthy),
            AdminHealthStatus::Healthy
        );
        assert_eq!(
            admin_status(ControlHealthStatus::Unavailable),
            AdminHealthStatus::Degraded
        );
        assert_eq!(
            admin_status(ControlHealthStatus::Missing),
            AdminHealthStatus::Unhealthy
        );
        assert_eq!(
            admin_status(ControlHealthStatus::Corrupt),
            AdminHealthStatus::Unhealthy
        );
    }
}
