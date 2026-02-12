// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{sync::Arc, time::Duration};

use anyhow::Result;
use async_channel::Receiver;
use in_memory_checkpoint_holder::InMemoryCheckpointHolder;
use reqwest::Url;
use sui_types::messages_checkpoint::CheckpointSequenceNumber;
use tokio::{fs, sync, task, time};

use crate::{
    checkpoint_downloader::CheckpointInfo,
    checkpoint_proto,
    config::GcsProtoCheckpointDownloaderConfig,
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

pub struct GcsProtoCheckpointDownloadWorker {
    worker_id: usize,
    rx: Receiver<CheckpointSequenceNumber>,
    tx: sync::mpsc::Sender<CheckpointInfo>,
    bucket_url: Url,
    config: GcsProtoCheckpointDownloaderConfig,
    client: reqwest::Client,
    metrics: Arc<Metrics>,
    in_memory_holder: Option<InMemoryCheckpointHolder>,
}

impl GcsProtoCheckpointDownloadWorker {
    pub fn new(
        worker_id: usize,
        rx: Receiver<CheckpointSequenceNumber>,
        tx: sync::mpsc::Sender<CheckpointInfo>,
        bucket_url: Url,
        config: GcsProtoCheckpointDownloaderConfig,
        metrics: Arc<Metrics>,
        in_memory_holder: Option<InMemoryCheckpointHolder>,
    ) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("should be able to build reqwest client");
        Self {
            worker_id,
            rx,
            tx,
            bucket_url,
            config,
            client,
            metrics,
            in_memory_holder,
        }
    }

    pub async fn start(self) {
        tracing::debug!("gcs-proto worker {} started", self.worker_id);

        self.metrics.active_download_workers.inc();
        let _worker_guard = WorkerGuard {
            metrics: self.metrics.clone(),
        };

        while let Ok(checkpoint_number) = self.rx.recv().await {
            tracing::debug!(
                "gcs-proto worker {} downloading checkpoint {}",
                self.worker_id,
                checkpoint_number
            );

            match self.download_checkpoint(checkpoint_number).await {
                Ok(checkpoint_info) => {
                    if let Err(e) = self.tx.send(checkpoint_info).await {
                        tracing::debug!(
                            "gcs-proto worker {} failed to send result: {}",
                            self.worker_id,
                            e
                        );
                        break;
                    }
                }
                Err(e) => {
                    tracing::debug!(
                        "gcs-proto worker {} failed to download checkpoint {}: {}",
                        self.worker_id,
                        checkpoint_number,
                        e
                    );
                }
            }
        }

        tracing::debug!("gcs-proto worker {} stopped", self.worker_id);
    }

    async fn download_checkpoint(
        &self,
        checkpoint_number: CheckpointSequenceNumber,
    ) -> Result<CheckpointInfo> {
        let url = self
            .bucket_url
            .join(&format!("{}.binpd.zst", checkpoint_number))?;

        let mut retry_count = 0;
        let mut wait_duration = self.config.min_download_retry_wait;

        loop {
            tracing::debug!(
                %url,
                retry_count,
                "downloading proto checkpoint from GCS"
            );

            match self.try_download(&url).await {
                Ok(compressed_bytes) => {
                    // Extract metadata from the compressed protobuf.
                    let checkpoint_info =
                        checkpoint_proto::extract_checkpoint_info(&compressed_bytes)?;

                    // Store checkpoint either in memory or on disk.
                    if let Some(ref holder) = self.in_memory_holder {
                        holder.store(checkpoint_number, compressed_bytes).await;
                        tracing::debug!(checkpoint_number, "proto checkpoint stored in memory");
                    } else {
                        // Write raw .zst bytes to disk atomically.
                        let checkpoint_file = self
                            .config
                            .downloaded_checkpoint_dir
                            .join(format!("{checkpoint_number}"));
                        let temp_file = self
                            .config
                            .downloaded_checkpoint_dir
                            .join(format!("{checkpoint_number}.tmp"));

                        fs::write(&temp_file, &compressed_bytes).await?;
                        fs::rename(&temp_file, &checkpoint_file).await?;

                        tracing::debug!(checkpoint_number, "proto checkpoint written to disk");
                    }

                    self.metrics.total_downloaded_checkpoints.inc();

                    tracing::debug!(
                        checkpoint_number,
                        "proto checkpoint download and save successful"
                    );
                    return Ok(checkpoint_info);
                }
                Err(e) => {
                    retry_count += 1;
                    tracing::warn!(
                        checkpoint_number,
                        retry_count,
                        wait_duration_ms = wait_duration.as_millis(),
                        "failed to download proto checkpoint, retrying after wait: {}",
                        e
                    );

                    let status_label = if let Some(reqwest_err) = e.downcast_ref::<reqwest::Error>()
                    {
                        if let Some(status) = reqwest_err.status() {
                            status.as_str().to_string()
                        } else {
                            "other".to_string()
                        }
                    } else {
                        "other".to_string()
                    };
                    self.metrics
                        .download_failures
                        .with_label_values(&[&status_label])
                        .inc();

                    time::sleep(wait_duration).await;

                    wait_duration =
                        std::cmp::min(wait_duration * 2, self.config.max_download_retry_wait);
                }
            }
        }
    }

    async fn try_download(&self, url: &Url) -> Result<Vec<u8>> {
        let response = self
            .client
            .get(url.clone())
            .send()
            .await?
            .error_for_status()?;
        let bytes = response.bytes().await?;
        Ok(bytes.to_vec())
    }
}

pub struct GcsProtoCheckpointDownloader {
    num_workers: usize,
    worker_handles: Vec<task::JoinHandle<()>>,
    bucket_url: Url,
    config: GcsProtoCheckpointDownloaderConfig,
    metrics: Arc<Metrics>,
    in_memory_holder: Option<InMemoryCheckpointHolder>,
}

impl GcsProtoCheckpointDownloader {
    pub fn new(
        config: GcsProtoCheckpointDownloaderConfig,
        metrics: Arc<Metrics>,
        in_memory_holder: Option<InMemoryCheckpointHolder>,
    ) -> Self {
        Self {
            num_workers: config.num_workers,
            worker_handles: Vec::new(),
            bucket_url: Url::parse(&config.bucket_url).expect("invalid GCS bucket URL"),
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
        sync::mpsc::Sender<bool>,
        task::JoinHandle<()>,
    )> {
        tracing::info!(
            "starting GCS proto checkpoint downloader from checkpoint {} with {} workers",
            initial_checkpoint,
            self.num_workers
        );

        fs::create_dir_all(&self.config.downloaded_checkpoint_dir).await?;
        self.cleanup_temp_files().await?;

        let (download_tx, download_rx) = async_channel::bounded::<CheckpointSequenceNumber>(100);
        let (result_tx, result_rx) = sync::mpsc::channel::<CheckpointInfo>(100);
        let (pause_tx, pause_rx) = sync::mpsc::channel::<bool>(10);

        for worker_id in 0..self.num_workers {
            let worker_rx = download_rx.clone();
            let worker_tx = result_tx.clone();
            let bucket_url = self.bucket_url.clone();
            let metrics = self.metrics.clone();
            let in_memory_holder = self.in_memory_holder.clone();

            let worker = GcsProtoCheckpointDownloadWorker::new(
                worker_id,
                worker_rx,
                worker_tx,
                bucket_url,
                self.config.clone(),
                metrics,
                in_memory_holder,
            );

            let handle = tokio::spawn(async move {
                worker.start().await;
            });
            self.worker_handles.push(handle);
        }

        let driver_handle = tokio::spawn(async move {
            if let Err(e) = self
                .download_checkpoint_driver(download_tx, pause_rx, initial_checkpoint)
                .await
            {
                tracing::error!("GCS proto checkpoint driver failed: {}", e);
            }
        });

        Ok((result_rx, pause_tx, driver_handle))
    }

    async fn download_checkpoint_driver(
        &self,
        download_tx: async_channel::Sender<CheckpointSequenceNumber>,
        mut pause_rx: sync::mpsc::Receiver<bool>,
        initial_checkpoint: CheckpointSequenceNumber,
    ) -> Result<()> {
        let mut current_checkpoint = initial_checkpoint;
        let mut is_paused = false;

        loop {
            while let Ok(should_pause) = pause_rx.try_recv() {
                if should_pause && !is_paused {
                    tracing::info!(
                        "GCS proto checkpoint downloader paused at checkpoint {} due to backpressure",
                        current_checkpoint
                    );
                    is_paused = true;
                } else if !should_pause && is_paused {
                    tracing::info!(
                        "GCS proto checkpoint downloader resumed at checkpoint {}",
                        current_checkpoint
                    );
                    is_paused = false;
                }
            }

            if is_paused {
                time::sleep(time::Duration::from_millis(100)).await;
                continue;
            }

            if let Err(e) = download_tx.send(current_checkpoint).await {
                tracing::debug!(
                    "failed to send checkpoint number {}: {}",
                    current_checkpoint,
                    e
                );
                break;
            }
            current_checkpoint += 1;
        }

        Ok(())
    }
}
