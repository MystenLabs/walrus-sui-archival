// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use anyhow::{Result, anyhow};
use tokio::time;
use walrus_sdk::{
    ObjectID,
    client::WalrusNodeClient,
    sui::client::{ReadClient, SuiContractClient},
};

use crate::{
    archival_state::ArchivalState,
    config::CheckpointBlobExtenderConfig,
    metrics::Metrics,
};

/// Service that periodically checks and extends blob expiration epochs.
pub struct CheckpointBlobExtender {
    archival_state: Arc<ArchivalState>,
    walrus_client: Arc<WalrusNodeClient<SuiContractClient>>,
    config: CheckpointBlobExtenderConfig,
    metrics: Arc<Metrics>,
}

impl CheckpointBlobExtender {
    pub fn new(
        archival_state: Arc<ArchivalState>,
        walrus_client: Arc<WalrusNodeClient<SuiContractClient>>,
        config: CheckpointBlobExtenderConfig,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            archival_state,
            walrus_client,
            config,
            metrics,
        }
    }

    /// Start the background process that periodically checks and extends blobs.
    pub async fn start(self) -> Result<()> {
        tracing::info!("starting checkpoint blob extender service");

        let mut interval = time::interval(self.config.check_interval);

        loop {
            interval.tick().await;

            if let Err(e) = self.check_and_extend_blobs().await {
                tracing::error!("failed to check and extend blobs: {}", e);
                // Continue running despite errors.
            }
        }
    }

    /// Check all blobs and extend those that are expiring soon.
    async fn check_and_extend_blobs(&self) -> Result<()> {
        tracing::info!("checking blobs for expiration");

        // Get current Walrus epoch.
        let current_epoch = self.walrus_client.get_committees().await?.epoch();
        tracing::info!("current walrus epoch: {}", current_epoch);

        // Get all blobs from the archival state.
        let blobs = self.archival_state.list_all_blobs()?;
        tracing::info!("found {} blobs to check", blobs.len());

        // Store tuple of (object_id, start_checkpoint, blob_id) for blobs to extend.
        let mut blobs_to_extend = Vec::new();

        for blob_info in &blobs {
            let blob_end_epoch = blob_info.blob_expiration_epoch;

            // Check if the blob is expiring within 2 epochs.
            if blob_end_epoch <= current_epoch + 2 {
                // Parse the object ID.
                let object_id = ObjectID::from_bytes(&blob_info.object_id)
                    .map_err(|e| anyhow!("failed to parse object ID: {}", e))?;

                // Parse the blob ID.
                let blob_id_str = String::from_utf8_lossy(&blob_info.blob_id).to_string();
                let blob_id: walrus_core::BlobId = blob_id_str
                    .parse()
                    .map_err(|e| anyhow!("failed to parse blob ID: {}", e))?;

                tracing::info!(
                    "blob {} (object {}) expires at epoch {}, will extend",
                    blob_id,
                    object_id,
                    blob_end_epoch
                );

                blobs_to_extend.push((object_id, blob_info.start_checkpoint, blob_id));
            }
        }

        if blobs_to_extend.is_empty() {
            tracing::info!("no blobs need extension");
            return Ok(());
        }

        tracing::info!("extending {} blobs", blobs_to_extend.len());

        // Extend each blob.
        for (object_id, start_checkpoint, blob_id) in blobs_to_extend {
            if let Err(e) = self.extend_blob(object_id).await {
                tracing::error!("failed to extend blob {}: {}", object_id, e);
                // Continue with other blobs even if one fails.
                continue;
            }

            // Get the updated blob object to get the new expiration epoch with infinite retry.
            let mut retry_delay = self.config.min_transaction_retry_duration;
            let max_retry_delay = self.config.max_transaction_retry_duration;

            let blob = loop {
                match self
                    .walrus_client
                    .sui_client()
                    .get_blob_by_object_id(&object_id)
                    .await
                {
                    Ok(blob) => {
                        tracing::info!(
                            "successfully retrieved extended blob {} with new end_epoch: {}",
                            object_id,
                            blob.blob.storage.end_epoch
                        );
                        break blob;
                    }
                    Err(e) => {
                        tracing::error!(
                            "failed to get blob {} after extension: {}, retrying in {:?}",
                            object_id,
                            e,
                            retry_delay
                        );

                        // Sleep before retrying.
                        tokio::time::sleep(retry_delay).await;

                        // Exponential backoff.
                        retry_delay = std::cmp::min(retry_delay * 2, max_retry_delay);
                    }
                }
            };

            // Update the archival state with the new expiration epoch.
            if let Err(e) = self.archival_state.update_blob_expiration_epoch(
                start_checkpoint,
                &blob_id,
                &object_id,
                blob.blob.storage.end_epoch,
            ) {
                tracing::error!(
                    "failed to update archival state for blob {}: {}",
                    object_id,
                    e
                );
            }
        }

        Ok(())
    }

    /// Extend a single blob with retry logic.
    async fn extend_blob(&self, object_id: ObjectID) -> Result<()> {
        // Increment the attempted counter.
        self.metrics.blob_extensions_attempted.inc();

        let mut retry_delay = self.config.min_transaction_retry_duration;
        let max_retry_delay = self.config.max_transaction_retry_duration;

        loop {
            tracing::info!(
                "attempting to extend blob {} by {} epochs",
                object_id,
                self.config.extend_epoch_length
            );

            match self
                .walrus_client
                .sui_client()
                .extend_blob(object_id, self.config.extend_epoch_length)
                .await
            {
                Ok(_) => {
                    tracing::info!("successfully extended blob {}", object_id);
                    // Increment the success counter.
                    self.metrics.blob_extensions_succeeded.inc();
                    return Ok(());
                }
                Err(e) => {
                    tracing::error!(
                        "failed to extend blob {}: {}, retrying in {:?}",
                        object_id,
                        e,
                        retry_delay
                    );

                    // Sleep before retrying.
                    tokio::time::sleep(retry_delay).await;

                    // Exponential backoff.
                    retry_delay = std::cmp::min(retry_delay * 2, max_retry_delay);
                }
            }
        }
    }
}
