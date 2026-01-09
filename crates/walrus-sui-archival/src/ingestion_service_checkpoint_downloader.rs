// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use anyhow::Result;
use async_channel::Receiver;
use in_memory_checkpoint_holder::InMemoryCheckpointHolder;
use sui_indexer_alt_framework::ingestion::IngestionService;
use sui_storage::blob::Blob;
use sui_types::{
    full_checkpoint_content::{Checkpoint, CheckpointData},
    messages_checkpoint::CheckpointSequenceNumber,
};
use tokio::{fs, select, sync, task};
use tokio_util::sync::CancellationToken;

use crate::{
    checkpoint_downloader::CheckpointInfo,
    config::IngestionServiceCheckpointDownloaderConfig,
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

pub struct IngestionServiceCheckpointDownloadWorker {
    worker_id: usize,
    rx: Receiver<Arc<Checkpoint>>,
    tx: sync::mpsc::Sender<CheckpointInfo>,
    config: IngestionServiceCheckpointDownloaderConfig,
    metrics: Arc<Metrics>,
    in_memory_holder: Option<InMemoryCheckpointHolder>,
}

impl IngestionServiceCheckpointDownloadWorker {
    pub fn new(
        worker_id: usize,
        rx: Receiver<Arc<Checkpoint>>,
        tx: sync::mpsc::Sender<CheckpointInfo>,
        config: IngestionServiceCheckpointDownloaderConfig,
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
        tracing::debug!("worker {} started", self.worker_id);

        // Track the number of active workers.
        self.metrics.active_download_workers.inc();
        let _worker_guard = WorkerGuard {
            metrics: self.metrics.clone(),
        };

        while let Ok(checkpoint) = self.rx.recv().await {
            let checkpoint_number = checkpoint.summary.sequence_number;

            tracing::debug!(
                "worker {} processing checkpoint {}",
                self.worker_id,
                checkpoint_number
            );

            // TODO: fix this bug:
            // // If the checkpoint file already exists, skip it.
            // let checkpoint_file = self
            //     .config
            //     .downloaded_checkpoint_dir
            //     .join(format!("{checkpoint_number}"));
            // if checkpoint_file.exists() {
            //     tracing::info!(
            //         "worker {} skipping checkpoint {}, file already exists",
            //         self.worker_id,
            //         checkpoint_number
            //     );

            //     // Still need to send checkpoint info even if file exists.
            //     let checkpoint_info = CheckpointInfo {
            //         checkpoint_number,
            //         epoch: checkpoint.summary.epoch,
            //         is_end_of_epoch: checkpoint_data
            //             .summary
            //             .end_of_epoch_data
            //             .is_some(),
            //         timestamp_ms: checkpoint.summary.timestamp_ms,
            //         checkpoint_byte_size: 0, // We don't know the size if we skipped it.
            //     };

            //     if let Err(e) = self.tx.send(checkpoint_info).await {
            //         tracing::debug!("worker {} failed to send result: {}", self.worker_id, e);
            //         break;
            //     }
            //     continue;
            // }

            // Convert Checkpoint to CheckpointData for serialization.
            let checkpoint_data = CheckpointData::from((*checkpoint).clone());

            match self
                .write_checkpoint_to_disk(checkpoint_number, checkpoint_data)
                .await
            {
                Ok(checkpoint_info) => {
                    if let Err(e) = self.tx.send(checkpoint_info).await {
                        tracing::debug!("worker {} failed to send result: {}", self.worker_id, e);
                        break;
                    }
                }
                Err(e) => {
                    tracing::error!(
                        "worker {} failed to write checkpoint {} to disk: {}",
                        self.worker_id,
                        checkpoint_number,
                        e
                    );
                }
            }
        }

        tracing::debug!("worker {} stopped", self.worker_id);
    }

    async fn write_checkpoint_to_disk(
        &self,
        checkpoint_number: CheckpointSequenceNumber,
        checkpoint_data: CheckpointData,
    ) -> Result<CheckpointInfo> {
        // Serialize checkpoint data to bytes.
        let bytes =
            Blob::encode(&checkpoint_data, sui_storage::blob::BlobEncoding::Bcs)?.to_bytes();

        // Create checkpoint info with all values.
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
            // Store in memory.
            holder.store(checkpoint_number, bytes.to_vec()).await;
            tracing::debug!(checkpoint_number, "checkpoint stored in memory");
        } else {
            // Write checkpoint to disk atomically.
            // First write to a temporary file, then rename to final name.
            let checkpoint_file = self
                .config
                .downloaded_checkpoint_dir
                .join(format!("{checkpoint_number}"));
            let temp_file = self
                .config
                .downloaded_checkpoint_dir
                .join(format!("{checkpoint_number}.tmp"));

            // Write to temporary file.
            fs::write(&temp_file, &bytes).await?;

            // Atomically rename to final file.
            // This ensures the file is either fully written or not present at all.
            fs::rename(&temp_file, &checkpoint_file).await?;

            tracing::debug!(checkpoint_number, "checkpoint written to disk");
        }

        // Update metrics.
        self.metrics.total_downloaded_checkpoints.inc();

        tracing::debug!(checkpoint_number, "checkpoint write successful");
        Ok(checkpoint_info)
    }
}

pub struct IngestionServiceCheckpointDownloader {
    num_workers: usize,
    worker_handles: Vec<task::JoinHandle<()>>,
    config: IngestionServiceCheckpointDownloaderConfig,
    metrics: Arc<Metrics>,
    in_memory_holder: Option<InMemoryCheckpointHolder>,
}

impl IngestionServiceCheckpointDownloader {
    pub fn new(
        config: IngestionServiceCheckpointDownloaderConfig,
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
        // Track the number of temp files cleaned up.
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
            "starting ingestion service checkpoint downloader from checkpoint {} with {} workers",
            initial_checkpoint,
            self.num_workers
        );

        // Create the directory if it doesn't exist.
        fs::create_dir_all(&self.config.downloaded_checkpoint_dir).await?;

        // Clean up any leftover temporary files from previous runs.
        self.cleanup_temp_files().await?;

        // Create the IngestionService.
        let cancel = CancellationToken::new();
        let mut ingestion_service = IngestionService::new(
            self.config.to_client_args(),
            self.config.ingestion_config.clone(),
            None,
            &self.metrics.registry,
            cancel.clone(),
        )?;

        // Subscribe to the ingestion service.
        let (checkpoint_rx, watermark_tx) = ingestion_service.subscribe();

        // Set initial watermark prevent downloading too many checkpoints at beginning.
        watermark_tx.send(("checkpoint_monitor", initial_checkpoint))?;

        // Create channels for worker communication.
        // TODO: apply this to the other downloader if needed.
        let (download_tx, download_rx) = async_channel::bounded::<Arc<Checkpoint>>(10);
        let (result_tx, result_rx) = sync::mpsc::channel::<CheckpointInfo>(100);

        // Start the ingestion service.
        let ingestion_service_handle = ingestion_service.run(initial_checkpoint.., None).await?;

        // Start workers to process checkpoints.
        for worker_id in 0..self.num_workers {
            let worker_rx = download_rx.clone();
            let worker_tx = result_tx.clone();
            let metrics = self.metrics.clone();
            let in_memory_holder = self.in_memory_holder.clone();

            let worker = IngestionServiceCheckpointDownloadWorker::new(
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

        // Start the driver task to forward checkpoints from ingestion service to workers.
        let driver_handle = tokio::spawn(async move {
            if let Err(e) = Self::checkpoint_driver(checkpoint_rx, download_tx).await {
                tracing::error!("checkpoint driver failed: {}", e);
            }
        });

        // Create a joined handle for the ingestion service, and the drive task.
        // This waits for any task to complete, allowing fast failure detection.
        let joined_handle = tokio::spawn(async move {
            select! {
                result = ingestion_service_handle => {
                    tracing::info!("ingestion service task completed: {:?}", result);
                    if let Err(e) = result {
                        tracing::error!("ingestion service task failed: {}", e);
                    }
                }
                result = driver_handle => {
                    tracing::info!("driver task completed: {:?}", result);
                    if let Err(e) = result {
                        tracing::error!("driver task failed: {}", e);
                    }
                }
            }
        });

        // Return the receiver for the CheckpointMonitor to consume, watermark sender, and the join handle.
        Ok((result_rx, watermark_tx, joined_handle))
    }

    async fn checkpoint_driver(
        mut checkpoint_rx: sync::mpsc::Receiver<Arc<Checkpoint>>,
        download_tx: async_channel::Sender<Arc<Checkpoint>>,
    ) -> Result<()> {
        loop {
            // Receive checkpoint from ingestion service.
            match checkpoint_rx.recv().await {
                Some(checkpoint_data) => {
                    if let Err(e) = download_tx.send(checkpoint_data).await {
                        tracing::debug!("failed to send checkpoint to workers: {}", e);
                        break;
                    }
                }
                None => {
                    tracing::info!("ingestion service checkpoint stream closed");
                    break;
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use prometheus::Registry;
    use reqwest::Url;
    use sui_indexer_alt_framework::ingestion::IngestionConfig;
    use tempfile::TempDir;

    use super::*;

    fn create_test_metrics() -> Arc<Metrics> {
        let registry = Registry::new();
        Arc::new(Metrics::new(&registry))
    }

    #[tokio::test]
    async fn test_ingestion_service_checkpoint_downloader_initialization() {
        tracing_subscriber::fmt::init();

        let temp_dir = TempDir::new().unwrap();
        let config = IngestionServiceCheckpointDownloaderConfig {
            num_workers: 4,
            downloaded_checkpoint_dir: temp_dir.path().to_path_buf(),
            remote_store_url: Url::parse("https://example.com/bucket/").unwrap(),
            ingestion_config: IngestionConfig::default(),
        };
        let downloader =
            IngestionServiceCheckpointDownloader::new(config.clone(), create_test_metrics(), None);

        assert_eq!(downloader.num_workers, 4);
        assert_eq!(
            downloader.config.downloaded_checkpoint_dir,
            temp_dir.path().to_path_buf()
        );
    }
}
