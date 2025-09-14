use std::{num::NonZeroU16, path::PathBuf, sync::Arc};

use anyhow::Result;
use blob_bundle::BlobBundleBuilder;
use sui_types::messages_checkpoint::CheckpointSequenceNumber;
use tokio::{fs, sync::mpsc};
use walrus_core::BlobId;

use crate::{archival_state::ArchivalState, config::CheckpointBlobBuilderConfig};

/// Message sent from CheckpointMonitor to CheckpointBlobBuilder.
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
pub struct CheckpointBlobBuilder {
    archival_state: Arc<ArchivalState>,
    checkpoint_blobs_dir: PathBuf,
    downloaded_checkpoint_dir: PathBuf,
    n_shards: NonZeroU16,
}

impl CheckpointBlobBuilder {
    pub fn new(
        archival_state: Arc<ArchivalState>,
        config: CheckpointBlobBuilderConfig,
        downloaded_checkpoint_dir: PathBuf,
    ) -> Result<Self> {
        let n_shards = NonZeroU16::new(config.n_shards)
            .ok_or_else(|| anyhow::anyhow!("n_shards must be non-zero"))?;

        Ok(Self {
            archival_state,
            checkpoint_blobs_dir: PathBuf::from(&config.checkpoint_blobs_dir),
            downloaded_checkpoint_dir,
            n_shards,
        })
    }

    /// Start the blob builder service that listens for build requests.
    pub async fn start(self, mut request_rx: mpsc::Receiver<BlobBuildRequest>) -> Result<()> {
        tracing::info!(
            "starting checkpoint blob builder, storing blobs in {}",
            self.checkpoint_blobs_dir.display()
        );

        // Clear the blob directory.
        fs::create_dir_all(&self.checkpoint_blobs_dir).await?;

        // Remove all files in the blob directory.
        for entry in std::fs::read_dir(&self.checkpoint_blobs_dir)? {
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

            if let Err(e) = self.build_blob(request).await {
                tracing::error!(
                    "failed to build blob: {}, stopping checkpoint blob builder",
                    e
                );
                return Err(e);
            }
        }

        tracing::info!("checkpoint blob builder stopped");
        Ok(())
    }

    async fn build_blob(&self, request: BlobBuildRequest) -> Result<()> {
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
        let output_path = self.checkpoint_blobs_dir.join(&blob_filename);

        // Build the blob bundle.
        let result = builder.build(&file_paths, &output_path)?;

        tracing::info!(
            "successfully built blob {} with {} checkpoints, total size {} bytes",
            blob_filename,
            file_paths.len(),
            result.total_size
        );

        // Log the index map for debugging.
        tracing::debug!("blob index map:");
        for (id, (offset, length)) in &result.index_map {
            tracing::debug!("  {} -> offset: {}, length: {} bytes", id, offset, length);
        }

        // TODO: Upload the blob to Walrus.

        let blob_id = BlobId::ZERO;

        self.archival_state.create_new_checkpoint_blob(
            request.start_checkpoint,
            request.end_checkpoint,
            &result.index_map,
            blob_id,
            100,
            request.end_of_epoch,
        )?;

        Ok(())
    }
}
