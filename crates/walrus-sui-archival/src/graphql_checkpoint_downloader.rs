// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::Result;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use in_memory_checkpoint_holder::InMemoryCheckpointHolder;
use sui_storage::blob::Blob;
use sui_types::{
    effects::TransactionEffects,
    full_checkpoint_content::{CheckpointData, CheckpointTransaction},
    messages_checkpoint::{
        CertifiedCheckpointSummary,
        CheckpointContents,
        CheckpointSequenceNumber,
    },
    transaction::Transaction,
};
use tokio::{fs, sync, task, time};

use crate::{
    checkpoint_downloader::CheckpointInfo,
    config::GraphqlCheckpointDownloaderConfig,
    metrics::Metrics,
};

/// Guard that decrements active worker count when dropped.
struct WorkerGuard {
    metrics: Arc<Metrics>,
}

impl Drop for WorkerGuard {
    fn drop(&mut self) {
        self.metrics.active_download_workers.dec();
    }
}

/// Internal struct holding raw checkpoint data fetched from GraphQL.
#[derive(Debug, Clone)]
pub struct GraphqlCheckpointData {
    pub sequence_number: u64,
    pub epoch: u64,
    pub timestamp_ms: u64,
    pub summary_bcs: Vec<u8>,
    pub content_bcs: Vec<u8>,
    pub transactions: Vec<GraphqlTransactionData>,
}

/// Internal struct holding raw transaction data fetched from GraphQL.
#[derive(Debug, Clone)]
pub struct GraphqlTransactionData {
    pub transaction_bcs: Vec<u8>,
    pub effects_bcs: Vec<u8>,
}

/// GraphQL query for fetching checkpoint data with transactions.
const CHECKPOINTS_QUERY: &str = r#"
query($after: Int!, $first: Int!) {
  checkpoints(first: $first, filter: {afterCheckpoint: $after}) {
    pageInfo {
      hasNextPage
      endCursor
    }
    nodes {
      sequenceNumber
      epoch {
        epochId
      }
      timestamp
      summaryBcs
      contentBcs
      transactions(first: 50) {
        pageInfo {
          hasNextPage
          endCursor
        }
        nodes {
          transactionBcs
          effects {
            effectsBcs
          }
        }
      }
    }
  }
}
"#;

/// GraphQL query for paginating through remaining transactions in a checkpoint.
const CHECKPOINT_TRANSACTIONS_QUERY: &str = r#"
query($seq: Int!, $first: Int!, $after: String!) {
  checkpoint(id: {sequenceNumber: $seq}) {
    transactions(first: $first, after: $after) {
      pageInfo {
        hasNextPage
        endCursor
      }
      nodes {
        transactionBcs
        effects {
          effectsBcs
        }
      }
    }
  }
}
"#;

/// Maximum number of checkpoints the driver can be ahead of the watermark.
const MAX_AHEAD_OF_WATERMARK: u64 = 1000;

pub struct GraphqlCheckpointDownloadWorker {
    worker_id: usize,
    rx: async_channel::Receiver<GraphqlCheckpointData>,
    tx: sync::mpsc::Sender<CheckpointInfo>,
    config: GraphqlCheckpointDownloaderConfig,
    metrics: Arc<Metrics>,
    in_memory_holder: Option<InMemoryCheckpointHolder>,
}

impl GraphqlCheckpointDownloadWorker {
    pub fn new(
        worker_id: usize,
        rx: async_channel::Receiver<GraphqlCheckpointData>,
        tx: sync::mpsc::Sender<CheckpointInfo>,
        config: GraphqlCheckpointDownloaderConfig,
        metrics: Arc<Metrics>,
        in_memory_holder: Option<InMemoryCheckpointHolder>,
    ) -> Self {
        Self {
            worker_id,
            rx,
            tx,
            config,
            metrics,
            in_memory_holder,
        }
    }

    pub async fn start(self) {
        tracing::debug!("graphql worker {} started", self.worker_id);

        self.metrics.active_download_workers.inc();
        let _worker_guard = WorkerGuard {
            metrics: self.metrics.clone(),
        };

        while let Ok(graphql_data) = self.rx.recv().await {
            let checkpoint_number = graphql_data.sequence_number;

            tracing::debug!(
                "graphql worker {} processing checkpoint {}",
                self.worker_id,
                checkpoint_number
            );

            match self.process_checkpoint(graphql_data).await {
                Ok(checkpoint_info) => {
                    if let Err(e) = self.tx.send(checkpoint_info).await {
                        tracing::debug!(
                            "graphql worker {} failed to send result: {}",
                            self.worker_id,
                            e
                        );
                        break;
                    }
                }
                Err(e) => {
                    tracing::error!(
                        "graphql worker {} failed to process checkpoint {}: {}",
                        self.worker_id,
                        checkpoint_number,
                        e
                    );
                }
            }
        }

        tracing::debug!("graphql worker {} stopped", self.worker_id);
    }

    async fn process_checkpoint(&self, data: GraphqlCheckpointData) -> Result<CheckpointInfo> {
        let checkpoint_number = data.sequence_number;

        // Deserialize BCS data into typed structs.
        let checkpoint_summary: CertifiedCheckpointSummary = bcs::from_bytes(&data.summary_bcs)?;
        let checkpoint_contents: CheckpointContents = bcs::from_bytes(&data.content_bcs)?;

        // Reconstruct transactions from BCS data.
        let mut transactions = Vec::with_capacity(data.transactions.len());
        for tx_data in &data.transactions {
            let transaction: Transaction = bcs::from_bytes(&tx_data.transaction_bcs)?;
            let effects: TransactionEffects = bcs::from_bytes(&tx_data.effects_bcs)?;
            transactions.push(CheckpointTransaction {
                transaction,
                effects,
                events: None,
                input_objects: vec![],
                output_objects: vec![],
            });
        }

        let checkpoint_data = CheckpointData {
            checkpoint_summary,
            checkpoint_contents,
            transactions,
        };

        // Serialize to the standard blob format.
        let bytes =
            Blob::encode(&checkpoint_data, sui_storage::blob::BlobEncoding::Bcs)?.to_bytes();

        let checkpoint_info = CheckpointInfo {
            checkpoint_number,
            epoch: checkpoint_data.checkpoint_summary.epoch,
            is_end_of_epoch: checkpoint_data
                .checkpoint_summary
                .end_of_epoch_data
                .is_some(),
            timestamp_ms: checkpoint_data.checkpoint_summary.timestamp_ms,
            checkpoint_byte_size: bytes.len(),
        };

        // Store checkpoint either in memory or on disk.
        if let Some(ref holder) = self.in_memory_holder {
            holder.store(checkpoint_number, bytes.to_vec()).await;
            tracing::debug!(checkpoint_number, "checkpoint stored in memory");
        } else {
            let checkpoint_file = self
                .config
                .downloaded_checkpoint_dir
                .join(format!("{checkpoint_number}"));
            let temp_file = self
                .config
                .downloaded_checkpoint_dir
                .join(format!("{checkpoint_number}.tmp"));

            fs::write(&temp_file, &bytes).await?;
            fs::rename(&temp_file, &checkpoint_file).await?;

            tracing::debug!(checkpoint_number, "checkpoint written to disk");
        }

        self.metrics.total_downloaded_checkpoints.inc();

        tracing::debug!(checkpoint_number, "checkpoint processing successful");
        Ok(checkpoint_info)
    }
}

pub struct GraphqlCheckpointDownloader {
    num_workers: usize,
    worker_handles: Vec<task::JoinHandle<()>>,
    config: GraphqlCheckpointDownloaderConfig,
    metrics: Arc<Metrics>,
    in_memory_holder: Option<InMemoryCheckpointHolder>,
}

impl GraphqlCheckpointDownloader {
    pub fn new(
        config: GraphqlCheckpointDownloaderConfig,
        metrics: Arc<Metrics>,
        in_memory_holder: Option<InMemoryCheckpointHolder>,
    ) -> Self {
        Self {
            num_workers: config.num_workers,
            worker_handles: Vec::new(),
            config,
            metrics,
            in_memory_holder,
        }
    }

    async fn cleanup_temp_files(&self) -> Result<()> {
        let mut cleaned_count = 0u64;
        let mut dir_entries = fs::read_dir(&self.config.downloaded_checkpoint_dir).await?;
        while let Some(entry) = dir_entries.next_entry().await? {
            let path = entry.path();
            if let Some(name) = path.file_name()
                && let Some(name_str) = name.to_str()
                && name_str.ends_with(".tmp")
            {
                tracing::debug!("cleaning up leftover temp file: {}", path.display());
                if let Err(e) = fs::remove_file(&path).await {
                    tracing::warn!("failed to remove temp file {}: {}", path.display(), e);
                } else {
                    cleaned_count += 1;
                }
            }
        }

        if cleaned_count > 0 {
            self.metrics.temp_files_cleaned.inc_by(cleaned_count);
        }

        Ok(())
    }

    pub async fn start(
        mut self,
        initial_checkpoint: CheckpointSequenceNumber,
    ) -> Result<(
        sync::mpsc::Receiver<CheckpointInfo>,
        sync::mpsc::UnboundedSender<(&'static str, CheckpointSequenceNumber)>,
        task::JoinHandle<()>,
    )> {
        tracing::info!(
            "starting graphql checkpoint downloader from checkpoint {} with {} workers",
            initial_checkpoint,
            self.num_workers
        );

        // Create the directory if it doesn't exist.
        fs::create_dir_all(&self.config.downloaded_checkpoint_dir).await?;

        // Clean up any leftover temporary files from previous runs.
        self.cleanup_temp_files().await?;

        // Create channels for worker communication.
        let (download_tx, download_rx) =
            async_channel::bounded::<GraphqlCheckpointData>(self.num_workers * 2);
        let (result_tx, result_rx) = sync::mpsc::channel::<CheckpointInfo>(100);

        // Create the watermark channel for backpressure.
        let (watermark_tx, mut watermark_rx) =
            sync::mpsc::unbounded_channel::<(&'static str, CheckpointSequenceNumber)>();

        // Shared watermark value for the driver to read.
        let watermark = Arc::new(AtomicU64::new(initial_checkpoint));
        let watermark_clone = watermark.clone();

        // Task to update watermark from the channel.
        let watermark_updater_handle = tokio::spawn(async move {
            while let Some((_name, seq)) = watermark_rx.recv().await {
                watermark_clone.store(seq, Ordering::Release);
            }
        });

        // Set initial watermark to prevent downloading too many checkpoints at beginning.
        watermark_tx.send(("checkpoint_monitor", initial_checkpoint))?;

        // Start workers to process checkpoints.
        for worker_id in 0..self.num_workers {
            let worker_rx = download_rx.clone();
            let worker_tx = result_tx.clone();
            let metrics = self.metrics.clone();
            let in_memory_holder = self.in_memory_holder.clone();

            let worker = GraphqlCheckpointDownloadWorker::new(
                worker_id,
                worker_rx,
                worker_tx,
                self.config.clone(),
                metrics,
                in_memory_holder,
            );

            let handle = tokio::spawn(async move {
                worker.start().await;
            });
            self.worker_handles.push(handle);
        }

        // Start the driver task to fetch checkpoints via GraphQL.
        let config = self.config.clone();
        let metrics = self.metrics.clone();
        let driver_handle = tokio::spawn(async move {
            if let Err(e) =
                checkpoint_driver(config, metrics, download_tx, watermark, initial_checkpoint).await
            {
                tracing::error!("graphql checkpoint driver failed: {}", e);
            }
        });

        // Create a joined handle that waits for any task to complete.
        let joined_handle = tokio::spawn(async move {
            tokio::select! {
                result = driver_handle => {
                    tracing::info!("graphql driver task completed: {:?}", result);
                    if let Err(e) = result {
                        tracing::error!("graphql driver task failed: {}", e);
                    }
                }
                result = watermark_updater_handle => {
                    tracing::info!("watermark updater task completed: {:?}", result);
                    if let Err(e) = result {
                        tracing::error!("watermark updater task failed: {}", e);
                    }
                }
            }
        });

        Ok((result_rx, watermark_tx, joined_handle))
    }
}

/// Driver task that fetches checkpoints from the GraphQL API and sends them to workers.
async fn checkpoint_driver(
    config: GraphqlCheckpointDownloaderConfig,
    metrics: Arc<Metrics>,
    download_tx: async_channel::Sender<GraphqlCheckpointData>,
    watermark: Arc<AtomicU64>,
    initial_checkpoint: CheckpointSequenceNumber,
) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(config.request_timeout)
        .build()?;

    let mut current_checkpoint = initial_checkpoint;

    loop {
        // Backpressure: wait if too far ahead of watermark.
        loop {
            let wm = watermark.load(Ordering::Acquire);
            if current_checkpoint > wm + MAX_AHEAD_OF_WATERMARK {
                tracing::debug!(
                    current_checkpoint,
                    watermark = wm,
                    "graphql driver waiting for watermark to advance"
                );
                time::sleep(Duration::from_millis(100)).await;
            } else {
                break;
            }
        }

        // Fetch a batch of checkpoints.
        let batch = fetch_checkpoint_batch(&client, &config, &metrics, current_checkpoint).await;

        match batch {
            Ok(checkpoints) => {
                if checkpoints.is_empty() {
                    // No new checkpoints available yet, wait and retry.
                    tracing::debug!(current_checkpoint, "no new checkpoints available, waiting");
                    time::sleep(Duration::from_secs(1)).await;
                    continue;
                }

                let batch_end = checkpoints
                    .last()
                    .map(|c| c.sequence_number)
                    .unwrap_or(current_checkpoint);

                for checkpoint in checkpoints {
                    if let Err(e) = download_tx.send(checkpoint).await {
                        tracing::debug!("failed to send checkpoint to workers: {}", e);
                        return Ok(());
                    }
                }

                current_checkpoint = batch_end + 1;
            }
            Err(e) => {
                tracing::warn!(
                    current_checkpoint,
                    "failed to fetch checkpoint batch: {}",
                    e
                );
                // Error tracked inside fetch_checkpoint_batch, just continue retry loop.
            }
        }
    }
}

/// Fetches a batch of checkpoints from the GraphQL API with retry logic.
async fn fetch_checkpoint_batch(
    client: &reqwest::Client,
    config: &GraphqlCheckpointDownloaderConfig,
    metrics: &Metrics,
    after_checkpoint: CheckpointSequenceNumber,
) -> Result<Vec<GraphqlCheckpointData>> {
    let mut retry_count = 0u32;
    let mut wait_duration = config.min_retry_wait;

    loop {
        match try_fetch_checkpoint_batch(client, config, after_checkpoint).await {
            Ok(checkpoints) => return Ok(checkpoints),
            Err(e) => {
                retry_count += 1;
                tracing::warn!(
                    after_checkpoint,
                    retry_count,
                    wait_duration_ms = wait_duration.as_millis(),
                    "failed to fetch checkpoint batch, retrying: {}",
                    e
                );

                let status_label = if let Some(reqwest_err) = e.downcast_ref::<reqwest::Error>() {
                    reqwest_err
                        .status()
                        .map(|s| s.as_str().to_string())
                        .unwrap_or_else(|| "other".to_string())
                } else {
                    "other".to_string()
                };
                metrics
                    .download_failures
                    .with_label_values(&[&status_label])
                    .inc();

                time::sleep(wait_duration).await;
                wait_duration = std::cmp::min(wait_duration * 2, config.max_retry_wait);
            }
        }
    }
}

/// Attempts to fetch a batch of checkpoints from the GraphQL API.
async fn try_fetch_checkpoint_batch(
    client: &reqwest::Client,
    config: &GraphqlCheckpointDownloaderConfig,
    after_checkpoint: CheckpointSequenceNumber,
) -> Result<Vec<GraphqlCheckpointData>> {
    // The GraphQL filter `afterCheckpoint` is exclusive, so we pass
    // after_checkpoint - 1 to include after_checkpoint itself.
    let after_value = if after_checkpoint > 0 {
        after_checkpoint as i64 - 1
    } else {
        -1
    };

    let body = serde_json::json!({
        "query": CHECKPOINTS_QUERY,
        "variables": {
            "after": after_value,
            "first": config.batch_size,
        }
    });

    let response = client
        .post(config.graphql_url.as_str())
        .json(&body)
        .send()
        .await?
        .error_for_status()?;

    let json: serde_json::Value = response.json().await?;

    // Check for GraphQL errors.
    if let Some(errors) = json.get("errors") {
        return Err(anyhow::anyhow!("GraphQL errors: {}", errors));
    }

    let nodes = json
        .get("data")
        .and_then(|d| d.get("checkpoints"))
        .and_then(|c| c.get("nodes"))
        .and_then(|n| n.as_array())
        .ok_or_else(|| anyhow::anyhow!("unexpected GraphQL response structure"))?;

    let mut result = Vec::with_capacity(nodes.len());

    for node in nodes {
        let checkpoint_data = parse_checkpoint_node(client, config, node).await?;
        result.push(checkpoint_data);
    }

    Ok(result)
}

/// Parses a checkpoint node from the GraphQL response, fetching additional
/// transaction pages if needed.
async fn parse_checkpoint_node(
    client: &reqwest::Client,
    config: &GraphqlCheckpointDownloaderConfig,
    node: &serde_json::Value,
) -> Result<GraphqlCheckpointData> {
    let sequence_number = node
        .get("sequenceNumber")
        .and_then(|v| v.as_u64())
        .ok_or_else(|| anyhow::anyhow!("missing sequenceNumber"))?;

    let epoch = node
        .get("epoch")
        .and_then(|e| e.get("epochId"))
        .and_then(|v| v.as_u64())
        .ok_or_else(|| anyhow::anyhow!("missing epoch.epochId"))?;

    // Parse timestamp: the GraphQL API returns an ISO 8601 string.
    let timestamp_str = node
        .get("timestamp")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing timestamp"))?;
    let timestamp_ms = parse_timestamp_to_ms(timestamp_str)?;

    let summary_bcs_b64 = node
        .get("summaryBcs")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing summaryBcs"))?;
    let summary_bcs = BASE64.decode(summary_bcs_b64)?;

    let content_bcs_b64 = node
        .get("contentBcs")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing contentBcs"))?;
    let content_bcs = BASE64.decode(content_bcs_b64)?;

    // Parse initial transactions page.
    let tx_obj = node
        .get("transactions")
        .ok_or_else(|| anyhow::anyhow!("missing transactions"))?;

    let mut transactions = parse_transaction_nodes(tx_obj)?;

    // Paginate through remaining transactions if needed.
    let page_info = tx_obj
        .get("pageInfo")
        .ok_or_else(|| anyhow::anyhow!("missing transactions.pageInfo"))?;

    let mut has_next_page = page_info
        .get("hasNextPage")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let mut end_cursor = page_info
        .get("endCursor")
        .and_then(|v| v.as_str())
        .map(String::from);

    while has_next_page {
        let cursor = end_cursor
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("hasNextPage=true but no endCursor"))?;

        let (more_txs, next_page_info) =
            fetch_checkpoint_transactions(client, config, sequence_number, cursor).await?;

        transactions.extend(more_txs);

        has_next_page = next_page_info
            .get("hasNextPage")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        end_cursor = next_page_info
            .get("endCursor")
            .and_then(|v| v.as_str())
            .map(String::from);
    }

    Ok(GraphqlCheckpointData {
        sequence_number,
        epoch,
        timestamp_ms,
        summary_bcs,
        content_bcs,
        transactions,
    })
}

/// Parses transaction nodes from the GraphQL response.
fn parse_transaction_nodes(tx_obj: &serde_json::Value) -> Result<Vec<GraphqlTransactionData>> {
    let nodes = tx_obj
        .get("nodes")
        .and_then(|n| n.as_array())
        .ok_or_else(|| anyhow::anyhow!("missing transactions.nodes"))?;

    let mut transactions = Vec::with_capacity(nodes.len());

    for tx_node in nodes {
        let tx_bcs_b64 = tx_node
            .get("transactionBcs")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("missing transactionBcs"))?;
        let tx_bcs = BASE64.decode(tx_bcs_b64)?;

        let effects_bcs_b64 = tx_node
            .get("effects")
            .and_then(|e| e.get("effectsBcs"))
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("missing effects.effectsBcs"))?;
        let effects_bcs = BASE64.decode(effects_bcs_b64)?;

        transactions.push(GraphqlTransactionData {
            transaction_bcs: tx_bcs,
            effects_bcs,
        });
    }

    Ok(transactions)
}

/// Fetches additional transaction pages for a checkpoint.
async fn fetch_checkpoint_transactions(
    client: &reqwest::Client,
    config: &GraphqlCheckpointDownloaderConfig,
    sequence_number: u64,
    after_cursor: &str,
) -> Result<(Vec<GraphqlTransactionData>, serde_json::Value)> {
    let body = serde_json::json!({
        "query": CHECKPOINT_TRANSACTIONS_QUERY,
        "variables": {
            "seq": sequence_number,
            "first": 50,
            "after": after_cursor,
        }
    });

    let response = client
        .post(config.graphql_url.as_str())
        .json(&body)
        .send()
        .await?
        .error_for_status()?;

    let json: serde_json::Value = response.json().await?;

    if let Some(errors) = json.get("errors") {
        return Err(anyhow::anyhow!("GraphQL errors: {}", errors));
    }

    let tx_obj = json
        .get("data")
        .and_then(|d| d.get("checkpoint"))
        .and_then(|c| c.get("transactions"))
        .ok_or_else(|| anyhow::anyhow!("unexpected GraphQL response for transaction pagination"))?;

    let transactions = parse_transaction_nodes(tx_obj)?;

    let page_info = tx_obj
        .get("pageInfo")
        .cloned()
        .unwrap_or(serde_json::json!({"hasNextPage": false}));

    Ok((transactions, page_info))
}

/// Parses an ISO 8601 timestamp string to milliseconds since Unix epoch.
fn parse_timestamp_to_ms(timestamp_str: &str) -> Result<u64> {
    let dt = chrono::DateTime::parse_from_rfc3339(timestamp_str)
        .or_else(|_| {
            // Try parsing without timezone (some APIs return UTC without Z).
            chrono::NaiveDateTime::parse_from_str(timestamp_str, "%Y-%m-%dT%H:%M:%S%.f")
                .map(|naive| naive.and_utc().fixed_offset())
        })
        .map_err(|e| anyhow::anyhow!("failed to parse timestamp '{}': {}", timestamp_str, e))?;

    Ok(dt.timestamp_millis() as u64)
}

#[cfg(test)]
mod tests {
    use prometheus::Registry;
    use tempfile::TempDir;

    use super::*;

    fn create_test_metrics() -> Arc<Metrics> {
        let registry = Registry::new();
        Arc::new(Metrics::new(&registry))
    }

    #[test]
    fn test_parse_timestamp_to_ms() {
        // RFC 3339 with Z suffix.
        let ms = parse_timestamp_to_ms("2024-01-01T00:00:00Z").unwrap();
        assert_eq!(ms, 1704067200000);

        // RFC 3339 with timezone offset.
        let ms = parse_timestamp_to_ms("2024-01-01T00:00:00+00:00").unwrap();
        assert_eq!(ms, 1704067200000);

        // With fractional seconds.
        let ms = parse_timestamp_to_ms("2024-01-01T00:00:00.123Z").unwrap();
        assert_eq!(ms, 1704067200123);
    }

    #[test]
    fn test_parse_transaction_nodes_empty() {
        let tx_obj = serde_json::json!({
            "nodes": [],
            "pageInfo": {
                "hasNextPage": false,
                "endCursor": null
            }
        });
        let txs = parse_transaction_nodes(&tx_obj).unwrap();
        assert!(txs.is_empty());
    }

    #[test]
    fn test_parse_transaction_nodes_with_data() {
        let tx_bcs = BASE64.encode(b"test_tx_data");
        let effects_bcs = BASE64.encode(b"test_effects_data");

        let tx_obj = serde_json::json!({
            "nodes": [
                {
                    "transactionBcs": tx_bcs,
                    "effects": {
                        "effectsBcs": effects_bcs
                    }
                }
            ],
            "pageInfo": {
                "hasNextPage": false,
                "endCursor": null
            }
        });

        let txs = parse_transaction_nodes(&tx_obj).unwrap();
        assert_eq!(txs.len(), 1);
        assert_eq!(txs[0].transaction_bcs, b"test_tx_data");
        assert_eq!(txs[0].effects_bcs, b"test_effects_data");
    }

    #[test]
    fn test_graphql_config_defaults() {
        let yaml = r#"
graphql_url: "https://graphql.mainnet.sui.io/graphql"
"#;
        let config: GraphqlCheckpointDownloaderConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(
            config.graphql_url.as_str(),
            "https://graphql.mainnet.sui.io/graphql"
        );
        assert_eq!(config.num_workers, 20);
        assert_eq!(config.batch_size, 10);
        assert_eq!(config.request_timeout, Duration::from_secs(30));
        assert_eq!(config.min_retry_wait, Duration::from_secs(1));
        assert_eq!(config.max_retry_wait, Duration::from_secs(60));
    }

    #[test]
    fn test_graphql_config_custom_values() {
        let yaml = r#"
graphql_url: "https://custom.endpoint/graphql"
num_workers: 5
batch_size: 20
request_timeout: 60s
min_retry_wait: 2s
max_retry_wait: 120s
downloaded_checkpoint_dir: "/custom/dir"
"#;
        let config: GraphqlCheckpointDownloaderConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(
            config.graphql_url.as_str(),
            "https://custom.endpoint/graphql"
        );
        assert_eq!(config.num_workers, 5);
        assert_eq!(config.batch_size, 20);
        assert_eq!(config.request_timeout, Duration::from_secs(60));
        assert_eq!(config.min_retry_wait, Duration::from_secs(2));
        assert_eq!(config.max_retry_wait, Duration::from_secs(120));
    }

    #[test]
    fn test_checkpoint_downloader_type_graphql_serde() {
        use crate::config::CheckpointDownloaderType;

        let yaml = r#"
type: graphql
graphql_url: "https://graphql.mainnet.sui.io/graphql"
"#;
        let downloader_type: CheckpointDownloaderType = serde_yaml::from_str(yaml).unwrap();
        match downloader_type {
            CheckpointDownloaderType::GraphQL(config) => {
                assert_eq!(
                    config.graphql_url.as_str(),
                    "https://graphql.mainnet.sui.io/graphql"
                );
            }
            _ => panic!("expected GraphQL variant"),
        }
    }

    #[tokio::test]
    async fn test_graphql_checkpoint_downloader_initialization() {
        let temp_dir = TempDir::new().unwrap();
        let config = GraphqlCheckpointDownloaderConfig {
            graphql_url: reqwest::Url::parse("https://graphql.mainnet.sui.io/graphql").unwrap(),
            num_workers: 4,
            downloaded_checkpoint_dir: temp_dir.path().to_path_buf(),
            batch_size: 10,
            request_timeout: Duration::from_secs(30),
            min_retry_wait: Duration::from_secs(1),
            max_retry_wait: Duration::from_secs(60),
        };

        let downloader =
            GraphqlCheckpointDownloader::new(config.clone(), create_test_metrics(), None);

        assert_eq!(downloader.num_workers, 4);
        assert_eq!(
            downloader.config.downloaded_checkpoint_dir,
            temp_dir.path().to_path_buf()
        );
    }

    #[test]
    fn test_graphql_response_parsing() {
        let summary_bcs = BASE64.encode(b"summary_data");
        let content_bcs = BASE64.encode(b"content_data");
        let tx_bcs = BASE64.encode(b"tx_data");
        let effects_bcs = BASE64.encode(b"effects_data");

        let response = serde_json::json!({
            "data": {
                "checkpoints": {
                    "pageInfo": {
                        "hasNextPage": false,
                        "endCursor": null
                    },
                    "nodes": [
                        {
                            "sequenceNumber": 42,
                            "epoch": { "epochId": 5 },
                            "timestamp": "2024-01-01T00:00:00Z",
                            "summaryBcs": summary_bcs,
                            "contentBcs": content_bcs,
                            "transactions": {
                                "pageInfo": {
                                    "hasNextPage": false,
                                    "endCursor": null
                                },
                                "nodes": [
                                    {
                                        "transactionBcs": tx_bcs,
                                        "effects": {
                                            "effectsBcs": effects_bcs
                                        }
                                    }
                                ]
                            }
                        }
                    ]
                }
            }
        });

        // Verify we can parse the structure.
        let nodes = response
            .get("data")
            .and_then(|d| d.get("checkpoints"))
            .and_then(|c| c.get("nodes"))
            .and_then(|n| n.as_array())
            .unwrap();

        assert_eq!(nodes.len(), 1);

        let node = &nodes[0];
        assert_eq!(node.get("sequenceNumber").unwrap().as_u64().unwrap(), 42);
        assert_eq!(
            node.get("epoch")
                .unwrap()
                .get("epochId")
                .unwrap()
                .as_u64()
                .unwrap(),
            5
        );

        // Verify Base64 decoding.
        let summary = BASE64
            .decode(node.get("summaryBcs").unwrap().as_str().unwrap())
            .unwrap();
        assert_eq!(summary, b"summary_data");

        let content = BASE64
            .decode(node.get("contentBcs").unwrap().as_str().unwrap())
            .unwrap();
        assert_eq!(content, b"content_data");

        // Verify transaction parsing.
        let tx_obj = node.get("transactions").unwrap();
        let txs = parse_transaction_nodes(tx_obj).unwrap();
        assert_eq!(txs.len(), 1);
        assert_eq!(txs[0].transaction_bcs, b"tx_data");
        assert_eq!(txs[0].effects_bcs, b"effects_data");
    }
}
