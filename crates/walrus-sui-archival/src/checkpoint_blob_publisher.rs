// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{num::NonZeroU16, path::PathBuf, sync::Arc};

use anyhow::{Context, Result};
use blob_bundle::{BlobBundleBuildResult, BlobBundleBuilder};
use sui_types::messages_checkpoint::CheckpointSequenceNumber;
use tokio::{fs, sync::mpsc};
use walrus_sdk::{
    client::{StoreArgs, WalrusNodeClient},
    sui::client::{BlobPersistence, SuiContractClient},
};

use crate::{
    archival_state::ArchivalState,
    config::CheckpointBlobPublisherConfig,
    metrics::Metrics,
};

/// Message sent from CheckpointMonitor to CheckpointBlobPublisher.
#[derive(Debug, Clone)]
pub struct BlobBuildRequest {
    /// First checkpoint number in the range.
    pub start_checkpoint: CheckpointSequenceNumber,
    /// Last checkpoint number in the range (inclusive).
    pub end_checkpoint: CheckpointSequenceNumber,
    /// Weather this checkpoint contains the end-of-epoch transaction.
    pub end_of_epoch: bool,
}

/// A long-running service that builds blob files from checkpoint ranges.
pub struct CheckpointBlobPublisher {
    archival_state: Arc<ArchivalState>,
    walrus_client: Arc<WalrusNodeClient<SuiContractClient>>,
    n_shards: NonZeroU16,
    config: CheckpointBlobPublisherConfig,
    downloaded_checkpoint_dir: PathBuf,
    metrics: Arc<Metrics>,
}

impl CheckpointBlobPublisher {
    pub async fn new(
        archival_state: Arc<ArchivalState>,
        walrus_client: Arc<WalrusNodeClient<SuiContractClient>>,
        config: CheckpointBlobPublisherConfig,
        downloaded_checkpoint_dir: PathBuf,
        metrics: Arc<Metrics>,
    ) -> Result<Self> {
        let n_shards = walrus_client.get_committees().await?.n_shards();
        Ok(Self {
            archival_state,
            walrus_client,
            n_shards,
            config,
            downloaded_checkpoint_dir,
            metrics,
        })
    }

    /// Start the blob publisher service that listens for build requests.
    pub async fn start(self, mut request_rx: mpsc::Receiver<BlobBuildRequest>) -> Result<()> {
        tracing::info!(
            "starting checkpoint blob publisher, storing blobs in {}",
            self.config.checkpoint_blobs_dir.display()
        );

        // Clear the blob directory.
        fs::create_dir_all(&self.config.checkpoint_blobs_dir).await?;

        // Remove all files in the blob directory if there are any.
        for entry in std::fs::read_dir(&self.config.checkpoint_blobs_dir)? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                std::fs::remove_file(entry.path())?;
            }
        }

        while let Some(request) = request_rx.recv().await {
            tracing::info!(
                "received blob build request for checkpoints {} to {}",
                request.start_checkpoint,
                request.end_checkpoint
            );

            if let Err(e) = self.build_and_upload_blob(request).await {
                tracing::error!(
                    "failed to build blob: {}, stopping checkpoint blob publisher",
                    e
                );
                return Err(e);
            }
        }

        tracing::info!("checkpoint blob publisher stopped");
        Ok(())
    }

    async fn build_and_upload_blob(&self, request: BlobBuildRequest) -> Result<()> {
        // Track the latency of building blobs.
        let build_timer = self.metrics.blob_build_latency_seconds.start_timer();
        let start_checkpoint = request.start_checkpoint;
        let end_checkpoint = request.end_checkpoint;

        tracing::info!(
            "building blob for checkpoints {} to {}",
            start_checkpoint,
            end_checkpoint
        );

        // Collect checkpoint file paths.
        let mut file_paths = Vec::new();
        for checkpoint_num in start_checkpoint..=end_checkpoint {
            let checkpoint_file = self
                .downloaded_checkpoint_dir
                .join(format!("{checkpoint_num}"));

            // Check if the checkpoint file exists.
            if !checkpoint_file.exists() {
                return Err(anyhow::anyhow!(
                    "checkpoint file {} does not exist",
                    checkpoint_file.display()
                ));
            }

            file_paths.push(checkpoint_file);
        }

        if file_paths.is_empty() {
            tracing::warn!("no checkpoint files to bundle");
            return Ok(());
        }

        // Create the blob bundle.
        let builder = BlobBundleBuilder::new(self.n_shards);

        // Generate output filename.
        let blob_filename = format!(
            "checkpoint_blob_{}_{}.blob",
            start_checkpoint, end_checkpoint
        );
        let output_path = self.config.checkpoint_blobs_dir.join(&blob_filename);

        // Build the blob bundle.
        let result = builder.build(&file_paths, &output_path)?;

        tracing::info!(
            "successfully built blob {} with {} checkpoints, total size {} bytes",
            blob_filename,
            file_paths.len(),
            result.total_size
        );

        // Track blob size metrics.
        let blob_size = result.total_size as i64;
        self.metrics.blob_size_bytes.observe(blob_size as f64);
        self.metrics.latest_blob_size_bytes.set(blob_size);

        // Stop the build timer before starting upload.
        build_timer.observe_duration();

        self.upload_blob_to_walrus(&request, &result).await?;

        // Track the latest checkpoint included in uploaded blob.
        self.metrics
            .latest_uploaded_checkpoint
            .set(request.end_checkpoint as i64);

        // Clean up the downloaded checkpoints and uploaded blobs.
        self.clean_up_downloaded_checkpoints_and_uploaded_blobs(&request, &result)
            .await?;

        tracing::info!(
            "successfully cleaned up downloaded checkpoints and uploaded blobs for checkpoints {} to {}",
            request.start_checkpoint,
            request.end_checkpoint
        );

        Ok(())
    }

    async fn upload_blob_to_walrus(
        &self,
        request: &BlobBuildRequest,
        result: &BlobBundleBuildResult,
    ) -> Result<()> {
        // Track the latency of uploading blobs.
        let upload_timer = self.metrics.blob_upload_latency_seconds.start_timer();

        let (blob_id, end_epoch) = {
            let blob = fs::read(&result.file_path)
                .await
                .context("read blob file")?;

            let mut store_args = StoreArgs::default_with_epochs(self.config.store_epoch_length);
            store_args.persistence = BlobPersistence::Permanent;

            // Infinite retry with exponential backoff.
            let mut retry_delay = self.config.min_retry_duration;
            let max_retry_delay = self.config.max_retry_duration;

            loop {
                match self
                    .walrus_client
                    .reserve_and_store_blobs_retry_committees(&[blob.as_slice()], &[], &store_args)
                    .await
                {
                    Ok(results) => {
                        if let Some(blob_store_result) = results.first() {
                            // Check if the blob was successfully stored.
                            if blob_store_result.is_not_stored() {
                                // Track upload failures with not_stored status.
                                self.metrics.blobs_uploaded_not_stored.inc();

                                tracing::error!(
                                    "blob upload failed with is_not_stored status, retrying in {:?}",
                                    retry_delay
                                );
                                tokio::time::sleep(retry_delay).await;
                                // Exponential backoff.
                                retry_delay = std::cmp::min(retry_delay * 2, max_retry_delay);
                                continue;
                            }

                            // Track successful uploads.
                            self.metrics.blobs_uploaded_success.inc();
                            upload_timer.observe_duration();

                            // Successfully stored.
                            break (
                                blob_store_result
                                    .blob_id()
                                    .expect("blob id should be present"),
                                blob_store_result
                                    .end_epoch()
                                    .expect("end epoch should be present"),
                            );
                        } else {
                            tracing::error!(
                                "blob upload returned empty results, retrying in {:?}",
                                retry_delay
                            );
                            tokio::time::sleep(retry_delay).await;
                            // Exponential backoff.
                            retry_delay = std::cmp::min(retry_delay * 2, max_retry_delay);
                        }
                    }
                    Err(e) => {
                        // Track upload failures.
                        self.metrics.blobs_uploaded_failed.inc();

                        tracing::error!("blob upload failed: {}, retrying in {:?}", e, retry_delay);
                        tokio::time::sleep(retry_delay).await;
                        // Exponential backoff.
                        retry_delay = std::cmp::min(retry_delay * 2, max_retry_delay);
                    }
                }
            }
        };

        // Log the index map for debugging.
        tracing::debug!("blob index map:");
        for (id, (offset, length)) in &result.index_map {
            tracing::debug!("  {} -> offset: {}, length: {} bytes", id, offset, length);
        }

        self.archival_state.create_new_checkpoint_blob(
            request.start_checkpoint,
            request.end_checkpoint,
            &result.index_map,
            blob_id,
            end_epoch,
            request.end_of_epoch,
        )?;
        Ok(())
    }

    async fn clean_up_downloaded_checkpoints_and_uploaded_blobs(
        &self,
        request: &BlobBuildRequest,
        result: &BlobBundleBuildResult,
    ) -> Result<()> {
        tracing::info!(
            "cleaning up downloaded checkpoints and uploaded blobs for checkpoints {} to {}",
            request.start_checkpoint,
            request.end_checkpoint
        );

        // Track checkpoints being cleaned up.
        let checkpoints_count = request.end_checkpoint - request.start_checkpoint + 1;
        self.metrics.checkpoints_cleaned.inc_by(checkpoints_count);

        for checkpoint_num in request.start_checkpoint..=request.end_checkpoint {
            let checkpoint_file = self
                .downloaded_checkpoint_dir
                .join(format!("{checkpoint_num}"));

            if let Err(e) = std::fs::remove_file(&checkpoint_file) {
                // Do not stop if file removal fails.
                tracing::warn!(
                    "failed to remove checkpoint file {}: {}",
                    checkpoint_file.display(),
                    e
                );
            }
        }

        // Track latest checkpoint cleaned up.
        self.metrics
            .latest_cleaned_checkpoint
            .set(request.end_checkpoint as i64);

        // Remove the uploaded blob.
        if let Err(e) = std::fs::remove_file(&result.file_path) {
            // Do not stop if file removal fails.
            tracing::warn!(
                "failed to remove uploaded blob {}: {}",
                result.file_path.display(),
                e
            );
        } else {
            self.metrics.local_blobs_removed.inc();
            tracing::info!("removed uploaded blob: {}", result.file_path.display());
        }

        Ok(())
    }
}
