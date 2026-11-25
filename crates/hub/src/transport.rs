//! The real transport: `stratahub-client` bound to [`HubTransport`]
//! (slice `HB7b`).
//!
//! The client is async; clone orchestration is synchronous. Each call
//! bridges through a current-thread runtime owned by the transport: the
//! calling thread drives I/O and timers, so a transport starts no worker
//! pool and I/O readiness stays on the calling thread (a cross-thread
//! handoff is what the thread sanitizer misreports, #3308). Object
//! downloads go through the client's range-resumable `download_object`
//! (retries + `Range:` resume per its retry policy — the §3.6
//! resumability invariant).

use stratahub_client::{
    download_object, Client, ClientConfig, DatasetFilter, ListPageReq, RetryPolicy,
};
use stratahub_protocol::wire::{
    DatasetCard, DatasetSummary, InfoResponse, PaginationEnvelope, RefList, YankedList,
};
use stratahub_protocol::{BranchName, DatasetName, Hash, Manifest};
use time::OffsetDateTime;
use url::Url;

use crate::clone::{CloneError, HubTransport};

/// [`HubTransport`] over a real hub via `stratahub-client`.
pub struct ClientTransport {
    client: Client,
    runtime: tokio::runtime::Runtime,
    retry: RetryPolicy,
    base_url: Url,
}

impl ClientTransport {
    /// Builds a transport for the hub at `base_url` (as produced by
    /// [`crate::resolve_hub_url`]).
    ///
    /// # Errors
    ///
    /// [`CloneError::Transport`] when the URL or HTTP stack refuses
    /// configuration.
    pub fn new(base_url: Url) -> Result<Self, CloneError> {
        let config = client_config(std::env::var("STRATA_HUB_TIMEOUT_SECONDS").ok().as_deref());
        let client =
            Client::new(base_url.clone(), config).map_err(|error| transport_error(&error))?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| CloneError::Transport {
                detail: format!("async runtime failed to start: {error}"),
            })?;
        Ok(Self {
            client,
            runtime,
            retry: RetryPolicy::default(),
            base_url,
        })
    }

    /// Reads the hub capability advertisement.
    pub fn info(&self) -> Result<InfoResponse, stratahub_client::ClientError> {
        self.runtime.block_on(self.client.info())
    }

    /// Lists datasets with V1 filter and pagination parameters.
    pub fn list_datasets(
        &self,
        filter: &DatasetFilter,
        page: ListPageReq,
    ) -> Result<PaginationEnvelope<DatasetSummary>, stratahub_client::ClientError> {
        self.runtime
            .block_on(self.client.list_datasets(filter, page))
    }

    /// Reads one full dataset card.
    pub fn get_dataset(
        &self,
        dataset: &DatasetName,
    ) -> Result<DatasetCard, stratahub_client::ClientError> {
        self.runtime.block_on(self.client.get_dataset(dataset))
    }

    /// Lists the dataset's live refs.
    pub fn list_refs(
        &self,
        dataset: &DatasetName,
    ) -> Result<RefList, stratahub_client::ClientError> {
        self.runtime.block_on(self.client.list_refs(dataset))
    }

    /// Lists the hub yank deny-list, optionally after an RFC 3339 timestamp.
    pub fn yanked(
        &self,
        since: Option<OffsetDateTime>,
    ) -> Result<YankedList, stratahub_client::ClientError> {
        self.runtime.block_on(self.client.yanked(since))
    }
}

/// The client configuration, with the test-facing timeout override
/// (`STRATA_HUB_TIMEOUT_SECONDS`) applied to both wall-clock budgets: the
/// per-request timeout and the TCP connect timeout. Sanitizer CI legs run
/// 5-15x slower than native (#2803), and the thread sanitizer can stall every
/// thread for tens of seconds while it symbolizes a (suppressed) report,
/// which outlasts the 10s connect default on a healthy loopback connect
/// (#3308).
/// Unset or unparsable values keep the defaults; production callers never
/// set this.
fn client_config(timeout_override: Option<&str>) -> ClientConfig {
    let mut config = ClientConfig::default();
    if let Some(timeout) = timeout_override
        .and_then(|value| value.parse::<u64>().ok())
        .map(std::time::Duration::from_secs)
    {
        config.request_timeout = timeout;
        config.connect_timeout = timeout;
    }
    config
}

impl HubTransport for ClientTransport {
    fn hub_url(&self) -> String {
        self.base_url.as_str().trim_end_matches('/').to_owned()
    }

    fn default_branch(&self, dataset: &DatasetName) -> Result<BranchName, CloneError> {
        let card = self
            .runtime
            .block_on(self.client.get_dataset(dataset))
            .map_err(|error| transport_error(&error))?;
        Ok(card.summary.default_branch)
    }

    fn resolve_ref(&self, dataset: &DatasetName, branch: &BranchName) -> Result<Hash, CloneError> {
        let reference = self
            .runtime
            .block_on(self.client.resolve_ref(dataset, branch))
            .map_err(|error| transport_error(&error))?;
        Ok(reference.manifest_hash)
    }

    fn get_manifest(&self, hash: &Hash) -> Result<Manifest, CloneError> {
        // The client verifies the bytes hash to `hash` before parsing.
        let (_bytes, manifest) = self
            .runtime
            .block_on(self.client.get_manifest(hash))
            .map_err(|error| transport_error(&error))?;
        Ok(manifest)
    }

    fn get_object(&self, hash: &Hash) -> Result<Vec<u8>, CloneError> {
        let mut body = Vec::new();
        self.runtime
            .block_on(download_object(&self.client, &self.retry, hash, &mut body))
            .map_err(|error| transport_error(&error))?;
        Ok(body)
    }
}

fn transport_error(error: &stratahub_client::ClientError) -> CloneError {
    CloneError::Transport {
        detail: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::client_config;
    use std::time::Duration;
    use stratahub_client::ClientConfig;

    #[test]
    fn timeout_override_sets_both_request_and_connect_budgets() {
        let defaults = ClientConfig::default();
        let budgets = |config: ClientConfig| (config.request_timeout, config.connect_timeout);
        let default_budgets = (defaults.request_timeout, defaults.connect_timeout);

        assert_eq!(budgets(client_config(None)), default_budgets);
        assert_eq!(
            budgets(client_config(Some("180"))),
            (Duration::from_secs(180), Duration::from_secs(180))
        );
        assert_eq!(
            budgets(client_config(Some("not-a-number"))),
            default_budgets
        );
    }
}
