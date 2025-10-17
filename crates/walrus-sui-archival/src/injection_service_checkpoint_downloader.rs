// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use anyhow::Result;
use async_channel::Receiver;
use sui_indexer_alt_framework::ingestion::IngestionService;
use sui_storage::blob::Blob;
use sui_types::{
    full_checkpoint_content::CheckpointData,
    messages_checkpoint::CheckpointSequenceNumber,
};
use tokio::{fs, select, sync, task};
use tokio_util::sync::CancellationToken;

use crate::{
    checkpoint_downloader::CheckpointInfo,
    config::InjectionServiceCheckpointDownloaderConfig,
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

pub struct InjectionServiceCheckpointDownloadWorker {
    worker_id: usize,
    rx: Receiver<Arc<CheckpointData>>,
    tx: sync::mpsc::Sender<CheckpointInfo>,
    config: InjectionServiceCheckpointDownloaderConfig,
    metrics: Arc<Metrics>,
}

impl InjectionServiceCheckpointDownloadWorker {
    pub fn new(
        worker_id: usize,
        rx: Receiver<Arc<CheckpointData>>,
        tx: sync::mpsc::Sender<CheckpointInfo>,
        config: InjectionServiceCheckpointDownloaderConfig,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            worker_id,
            rx,
            tx,
            config,
            metrics,
        }
    }

    pub async fn start(self) {
        tracing::debug!("worker {} started", self.worker_id);

        // Track the number of active workers.
        self.metrics.active_download_workers.inc();
        let _worker_guard = WorkerGuard {
            metrics: self.metrics.clone(),
        };

        while let Ok(checkpoint_data) = self.rx.recv().await {
            let checkpoint_number = checkpoint_data.checkpoint_summary.sequence_number;

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
            //         epoch: checkpoint_data.checkpoint_summary.epoch,
            //         is_end_of_epoch: checkpoint_data
            //             .checkpoint_summary
            //             .end_of_epoch_data
            //             .is_some(),
            //         timestamp_ms: checkpoint_data.checkpoint_summary.timestamp_ms,
            //         checkpoint_byte_size: 0, // We don't know the size if we skipped it.
            //     };

            //     if let Err(e) = self.tx.send(checkpoint_info).await {
            //         tracing::debug!("worker {} failed to send result: {}", self.worker_id, e);
            //         break;
            //     }
            //     continue;
            // }

            match self
                .write_checkpoint_to_disk(checkpoint_number, checkpoint_data.clone())
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
        checkpoint_data: Arc<CheckpointData>,
    ) -> Result<CheckpointInfo> {
        // Serialize checkpoint data to bytes.
        let bytes =
            Blob::encode(&*checkpoint_data, sui_storage::blob::BlobEncoding::Bcs)?.to_bytes();

        // Create checkpoint info.
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

        // Update metrics.
        self.metrics.total_downloaded_checkpoints.inc();

        tracing::debug!(checkpoint_number, "checkpoint write to disk successful");
        Ok(checkpoint_info)
    }
}

pub struct InjectionServiceCheckpointDownloader {
    num_workers: usize,
    worker_handles: Vec<task::JoinHandle<()>>,
    config: InjectionServiceCheckpointDownloaderConfig,
    metrics: Arc<Metrics>,
}

impl InjectionServiceCheckpointDownloader {
    pub fn new(config: InjectionServiceCheckpointDownloaderConfig, metrics: Arc<Metrics>) -> Self {
        Self {
            num_workers: config.num_workers,
            worker_handles: Vec::new(),
            config,
            metrics,
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
            "starting injection service checkpoint downloader from checkpoint {} with {} workers",
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
            self.metrics.indexer_metrics.clone(),
            cancel.clone(),
        )?;

        // Subscribe to the ingestion service.
        let (checkpoint_rx, watermark_tx) = ingestion_service.subscribe();

        // Set initial watermark prevent downloading too many checkpoints at beginning.
        watermark_tx.send(("checkpoint_monitor", initial_checkpoint))?;

        // Create channels for worker communication.
        let (download_tx, download_rx) = async_channel::bounded::<Arc<CheckpointData>>(100);
        let (result_tx, result_rx) = sync::mpsc::channel::<CheckpointInfo>(100);

        // Start the ingestion service.
        let (regulator_handle, broadcaster_handle) =
            ingestion_service.run(initial_checkpoint..).await?;

        // Start workers to process checkpoints.
        for worker_id in 0..self.num_workers {
            let worker_rx = download_rx.clone();
            let worker_tx = result_tx.clone();
            let metrics = self.metrics.clone();

            let worker = InjectionServiceCheckpointDownloadWorker::new(
                worker_id,
                worker_rx,
                worker_tx,
                self.config.clone(),
                metrics,
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

        // Create a joined handle for the regulator, broadcaster, and the drive task.
        // This waits for any task to complete, allowing fast failure detection.
        let joined_handle = tokio::spawn(async move {
            select! {
                result = regulator_handle => {
                    tracing::info!("regulator task completed: {:?}", result);
                    if let Err(e) = result {
                        tracing::error!("regulator task failed: {}", e);
                    }
                }
                result = broadcaster_handle => {
                    tracing::info!("broadcaster task completed: {:?}", result);
                    if let Err(e) = result {
                        tracing::error!("broadcaster task failed: {}", e);
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
        mut checkpoint_rx: sync::mpsc::Receiver<Arc<CheckpointData>>,
        download_tx: async_channel::Sender<Arc<CheckpointData>>,
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
    async fn test_injection_service_checkpoint_downloader_initialization() {
        tracing_subscriber::fmt::init();

        let temp_dir = TempDir::new().unwrap();
        let config = InjectionServiceCheckpointDownloaderConfig {
            num_workers: 4,
            downloaded_checkpoint_dir: temp_dir.path().to_path_buf(),
            remote_store_url: Url::parse("https://example.com/bucket/").unwrap(),
            ingestion_config: IngestionConfig::default(),
        };
        let downloader =
            InjectionServiceCheckpointDownloader::new(config.clone(), create_test_metrics());

        assert_eq!(downloader.num_workers, 4);
        assert_eq!(
            downloader.config.downloaded_checkpoint_dir,
            temp_dir.path().to_path_buf()
        );
    }
}
