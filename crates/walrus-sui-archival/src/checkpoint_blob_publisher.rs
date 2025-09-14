use std::{num::NonZeroU16, path::PathBuf, sync::Arc};

use anyhow::Result;
use blob_bundle::{BlobBundleBuildResult, BlobBundleBuilder};
use sui_types::messages_checkpoint::CheckpointSequenceNumber;
use tokio::{fs, sync::mpsc};
use walrus_core::BlobId;

use crate::{archival_state::ArchivalState, config::CheckpointBlobPublisherConfig};

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
    config: CheckpointBlobPublisherConfig,
    downloaded_checkpoint_dir: PathBuf,
}

impl CheckpointBlobPublisher {
    pub fn new(
        archival_state: Arc<ArchivalState>,
        config: CheckpointBlobPublisherConfig,
        downloaded_checkpoint_dir: PathBuf,
    ) -> Result<Self> {
        Ok(Self {
            archival_state,
            config,
            downloaded_checkpoint_dir,
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

        // TODO(zhe): get this info from Walrus.
        let n_shards = NonZeroU16::new(self.config.n_shards)
            .ok_or_else(|| anyhow::anyhow!("n_shards must be non-zero"))?;

        // Create the blob bundle.
        let builder = BlobBundleBuilder::new(n_shards);

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

        self.upload_blob_to_walrus(&request, &result).await?;

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
        // TODO(zhe): Upload the blob to Walrus.
        let blob_id = BlobId::ZERO;

        // Log the index map for debugging.
        tracing::debug!("blob index map:");
        for (id, (offset, length)) in &result.index_map {
            tracing::debug!("  {} -> offset: {}, length: {} bytes", id, offset, length);
        }

        // TODO(zhe): Calculate the blob expiration epoch. This is currently incorrect. Need to
        // first get the current epoch from Walrus.
        let blob_expiration_epoch = self.config.store_epoch_length;

        self.archival_state.create_new_checkpoint_blob(
            request.start_checkpoint,
            request.end_checkpoint,
            &result.index_map,
            blob_id,
            blob_expiration_epoch,
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

        // Remove the uploaded blob.
        if let Err(e) = std::fs::remove_file(&result.file_path) {
            // Do not stop if file removal fails.
            tracing::warn!(
                "failed to remove uploaded blob {}: {}",
                result.file_path.display(),
                e
            );
        }

        Ok(())
    }
}
