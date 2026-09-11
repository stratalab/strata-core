use super::{
    branch_cleanup_item, branch_comparison, branch_item, branch_name, branch_preview,
    branch_promotion, delete_effect, engine_promotion_strategy, output_admin_config,
    output_admin_describe, output_admin_health, output_admin_info, output_admin_metrics,
    output_space_create, output_space_delete, product_space, BranchStateSelector, CommitVersion,
    Executor, ExecutorResult, Output, PageInfo, PromotionStrategy, Timestamp, DEFAULT_BRANCH,
};

impl Executor {
    pub(super) fn execute_ping(&mut self) -> ExecutorResult<Output> {
        let summary = self.database.admin()?.ping();
        Ok(Output::Pong(crate::types::AdminPing {
            version: summary.version,
        }))
    }

    // Infallible: it only reads the injected transport state, so it returns an
    // `Output` directly (the dispatch arm wraps it) rather than an always-`Ok`
    // result.
    pub(super) fn execute_ipc_status(&self) -> Output {
        // Transport state, not engine state: read the host handle the owning
        // `Connection` injected (absent on a client/cache/off open). `is_owner`
        // is always true here — the responding executor owns the store; a
        // remote client's `Connection` flips it to false on the way back.
        let status = match self.ipc_host_state() {
            Some(state) => crate::types::AdminIpcStatus {
                is_owner: true,
                hosting: true,
                socket_path: Some(state.socket_path().display().to_string()),
                owner_pid: Some(u64::from(state.owner_pid())),
                client_count: state.client_count(),
                clients: state
                    .clients()
                    .into_iter()
                    .map(|entry| crate::types::AdminIpcClient {
                        name: entry.name,
                        version: entry.version,
                        pid: entry.pid,
                        access: entry.access,
                        protocol: entry.protocol,
                    })
                    .collect(),
            },
            None => crate::types::AdminIpcStatus {
                is_owner: true,
                hosting: false,
                socket_path: None,
                owner_pid: None,
                client_count: 0,
                clients: Vec::new(),
            },
        };
        Output::IpcStatus(status)
    }

    // Infallible: stops the injected host (if any) and returns whether it did.
    // The dispatch arm wraps it; a remote client's request lands here on the
    // owner's executor, so a client's `ipc_stop` stops the owner's socket.
    pub(super) fn execute_ipc_stop(&mut self) -> Output {
        Output::IpcStop(crate::types::AdminIpcStop {
            stopped: self.stop_ipc_hosting(),
        })
    }

    pub(super) fn execute_remote_get(&mut self) -> ExecutorResult<Output> {
        let origin = self.database.remote_origin()?;
        Ok(Output::RemoteOriginResult {
            origin: origin.map(|origin| crate::RemoteOriginInfo {
                remote_url: origin.remote_url().to_owned(),
                dataset: origin.dataset().to_owned(),
                branch: origin.branch().to_owned(),
                manifest_hash: origin.manifest_hash().to_owned(),
                fetched_at_micros: origin.fetched_at_micros(),
                base_frontier: origin
                    .base_frontier()
                    .iter()
                    .map(|entry| crate::RemoteOriginFrontierInfo {
                        branch: entry.branch().to_owned(),
                        base: entry.base().to_owned(),
                        local_version: entry.local_version(),
                    })
                    .collect(),
            }),
        })
    }

    pub(super) fn execute_info(&mut self, branch: Option<&str>) -> ExecutorResult<Output> {
        let branch = branch_name(branch, &self.default_branch)?;
        let mut admin = self.database.admin()?;
        let summary = admin.info(Some(&branch))?;
        Ok(Output::DatabaseInfo(output_admin_info(&summary)))
    }

    pub(super) fn execute_health(&mut self, branch: Option<&str>) -> ExecutorResult<Output> {
        let branch = branch_name(branch, &self.default_branch)?;
        let mut admin = self.database.admin()?;
        let summary = admin.health(Some(&branch));
        Ok(Output::Health(output_admin_health(&summary)))
    }

    pub(super) fn execute_metrics(&mut self, branch: Option<&str>) -> ExecutorResult<Output> {
        let branch = branch_name(branch, &self.default_branch)?;
        let mut admin = self.database.admin()?;
        let summary = admin.metrics(Some(&branch))?;
        Ok(Output::Metrics(output_admin_metrics(&summary)))
    }

    pub(super) fn execute_describe(&mut self, branch: Option<&str>) -> ExecutorResult<Output> {
        let branch = branch_name(branch, &self.default_branch)?;
        let mut admin = self.database.admin()?;
        let summary = admin.describe(Some(&branch))?;
        Ok(Output::Described(output_admin_describe(&summary)))
    }

    pub(super) fn execute_config_get(&mut self) -> ExecutorResult<Output> {
        let admin = self.database.admin()?;
        Ok(Output::Config(output_admin_config(&admin.config())))
    }

    pub(super) fn execute_configure_get_key(&mut self, key: &str) -> ExecutorResult<Output> {
        let admin = self.database.admin()?;
        Ok(Output::ConfigValue(admin.config_value(key)?))
    }

    pub(super) fn execute_space_list(&mut self, branch: Option<&str>) -> ExecutorResult<Output> {
        let branch = branch_name(branch, &self.default_branch)?;
        let mut spaces = self.database.spaces(branch)?;
        Ok(Output::SpaceList {
            items: spaces
                .list()?
                .iter()
                .map(|space| space.as_str().to_owned())
                .collect(),
            page: PageInfo::terminal(),
        })
    }

    pub(super) fn execute_space_create(
        &mut self,
        branch: Option<&str>,
        space: &str,
    ) -> ExecutorResult<Output> {
        let branch = branch_name(branch, &self.default_branch)?;
        let space = product_space(Some(space), &self.default_space)?;
        let mut spaces = self.database.spaces(branch)?;
        let outcome = spaces.create(space)?;
        Ok(output_space_create(&outcome))
    }

    pub(super) fn execute_space_exists(
        &mut self,
        branch: Option<&str>,
        space: &str,
    ) -> ExecutorResult<Output> {
        let branch = branch_name(branch, &self.default_branch)?;
        let space = product_space(Some(space), &self.default_space)?;
        let mut spaces = self.database.spaces(branch)?;
        Ok(Output::Bool(spaces.exists(&space)?))
    }

    pub(super) fn execute_space_delete(
        &mut self,
        branch: Option<&str>,
        space: &str,
        force: bool,
    ) -> ExecutorResult<Output> {
        let branch = branch_name(branch, &self.default_branch)?;
        let space = product_space(Some(space), &self.default_space)?;
        let mut spaces = self.database.spaces(branch)?;
        let outcome = spaces.delete(&space, force)?;
        Ok(output_space_delete(&outcome))
    }

    pub(super) fn execute_branch_list(&mut self) -> ExecutorResult<Output> {
        let branches = self
            .database
            .branches()?
            .list()?
            .iter()
            .map(branch_item)
            .collect();
        Ok(Output::Branches {
            items: branches,
            page: PageInfo::terminal(),
        })
    }

    pub(super) fn execute_branch_get(&mut self, branch: &str) -> ExecutorResult<Output> {
        let branch = branch_name(Some(branch), DEFAULT_BRANCH)?;
        let summary = self.database.branches()?.get(&branch)?;
        Ok(Output::Branch(branch_item(&summary)))
    }

    pub(super) fn execute_branch_diff(
        &mut self,
        branch_a: &str,
        branch_b: &str,
        at_timestamp: Option<u64>,
    ) -> ExecutorResult<Output> {
        let branch_a = branch_name(Some(branch_a), DEFAULT_BRANCH)?;
        let branch_b = branch_name(Some(branch_b), DEFAULT_BRANCH)?;
        let selector = match at_timestamp {
            None => BranchStateSelector::Current,
            Some(micros) => BranchStateSelector::AtTimestamp(Timestamp::from_micros(micros)),
        };
        let comparison = self
            .database
            .branches()?
            .compare(&branch_a, &branch_b, selector)?;
        Ok(Output::BranchComparison(branch_comparison(&comparison)))
    }

    pub(super) fn execute_branch_merge(
        &mut self,
        source: &str,
        target: &str,
        strategy: PromotionStrategy,
    ) -> ExecutorResult<Output> {
        let source = branch_name(Some(source), DEFAULT_BRANCH)?;
        let target = branch_name(Some(target), DEFAULT_BRANCH)?;
        let outcome = self.database.branches()?.promote(
            &source,
            &target,
            engine_promotion_strategy(strategy),
        )?;
        Ok(Output::BranchMerge(branch_promotion(&outcome)))
    }

    pub(super) fn execute_branch_preview(
        &mut self,
        source: &str,
        target: &str,
        strategy: PromotionStrategy,
    ) -> ExecutorResult<Output> {
        let source = branch_name(Some(source), DEFAULT_BRANCH)?;
        let target = branch_name(Some(target), DEFAULT_BRANCH)?;
        let preview = self.database.branches()?.preview(
            &source,
            &target,
            engine_promotion_strategy(strategy),
        )?;
        Ok(Output::BranchPreview(branch_preview(&preview)))
    }

    pub(super) fn execute_branch_create(&mut self, branch: &str) -> ExecutorResult<Output> {
        let branch = branch_name(Some(branch), DEFAULT_BRANCH)?;
        let outcome = self.database.branches()?.create(branch)?;
        Ok(Output::Branch(branch_item(outcome.branch())))
    }

    pub(super) fn execute_branch_fork_current(
        &mut self,
        source: &str,
        branch: &str,
    ) -> ExecutorResult<Output> {
        let source = branch_name(Some(source), DEFAULT_BRANCH)?;
        let branch = branch_name(Some(branch), DEFAULT_BRANCH)?;
        let outcome = self.database.branches()?.fork_current(&source, branch)?;
        Ok(Output::Branch(branch_item(outcome.branch())))
    }

    pub(super) fn execute_branch_fork_at_version(
        &mut self,
        source: &str,
        branch: &str,
        version: u64,
    ) -> ExecutorResult<Output> {
        let source = branch_name(Some(source), DEFAULT_BRANCH)?;
        let branch = branch_name(Some(branch), DEFAULT_BRANCH)?;
        let outcome = self.database.branches()?.fork_at_version(
            &source,
            branch,
            CommitVersion::new(version),
        )?;
        Ok(Output::Branch(branch_item(outcome.branch())))
    }

    pub(super) fn execute_branch_fork_at_timestamp(
        &mut self,
        source: &str,
        branch: &str,
        timestamp: u64,
    ) -> ExecutorResult<Output> {
        let source = branch_name(Some(source), DEFAULT_BRANCH)?;
        let branch = branch_name(Some(branch), DEFAULT_BRANCH)?;
        let outcome = self.database.branches()?.fork_at_timestamp(
            &source,
            branch,
            Timestamp::from_micros(timestamp),
        )?;
        Ok(Output::Branch(branch_item(outcome.branch())))
    }

    pub(super) fn execute_branch_delete(&mut self, branch: &str) -> ExecutorResult<Output> {
        let branch = branch_name(Some(branch), DEFAULT_BRANCH)?;
        let outcome = self.database.branches()?.delete(&branch)?;
        Ok(Output::BranchDeleteResult {
            deleted: true,
            effect: delete_effect(true),
            branch: branch_item(outcome.branch()),
            generation_before: outcome.generation_before(),
            generation_after: outcome.generation_after(),
            cleanup: outcome.cleanup().map(branch_cleanup_item),
        })
    }
}
