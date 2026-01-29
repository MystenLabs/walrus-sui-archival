// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{sync::Arc, time::Duration};

use anyhow::{Result, anyhow};
use sui_types::{
    Identifier,
    base_types::ObjectID,
    object::Owner,
    programmable_transaction_builder::ProgrammableTransactionBuilder,
    transaction::{ObjectArg, SharedObjectMutability, TransactionData, TransactionKind},
};
use tokio::time;
use walrus_sdk::{SuiReadClient, sui::client::ReadClient};

use crate::{
    archival_state::ArchivalState,
    config::CheckpointBlobExtenderConfig,
    metrics::Metrics,
    sui_interactive_client::SuiInteractiveClient,
    util::execute_transaction_and_check_status,
};

/// Service that periodically checks and extends blob expiration epochs.
pub struct CheckpointBlobExtender {
    archival_state: Arc<ArchivalState>,
    sui_interactive_client: SuiInteractiveClient,
    config: CheckpointBlobExtenderConfig,
    metrics: Arc<Metrics>,
    contract_package_id: ObjectID,
    system_object_id: ObjectID,
    wal_token_package_id: ObjectID,
}

impl CheckpointBlobExtender {
    pub fn new(
        archival_state: Arc<ArchivalState>,
        sui_interactive_client: SuiInteractiveClient,
        config: CheckpointBlobExtenderConfig,
        metrics: Arc<Metrics>,
        contract_package_id: ObjectID,
        system_object_id: ObjectID,
        wal_token_package_id: ObjectID,
    ) -> Self {
        Self {
            archival_state,
            sui_interactive_client,
            config,
            metrics,
            contract_package_id,
            system_object_id,
            wal_token_package_id,
        }
    }

    /// Start the background process that periodically checks and extends blobs.
    pub async fn start(self) -> Result<()> {
        tracing::info!("starting checkpoint blob extender service");

        // Spawn a background task for syncing blob expiration epochs.
        let sync_self = self.clone();
        tokio::spawn(async move {
            sync_self.sync_blob_expiration_epochs_loop().await;
        });

        // Main loop for checking and extending blobs.
        let mut extend_interval = time::interval(self.config.check_interval);

        // Initial delay to avoid overloading initial startup process.
        tokio::time::sleep(Duration::from_secs(1800)).await;

        loop {
            extend_interval.tick().await;
            if let Err(e) = self.check_and_extend_blobs().await {
                tracing::error!("failed to check and extend blobs: {}", e);
                // Continue running despite errors.
            }
        }
    }

    /// Background loop for syncing blob expiration epochs.
    async fn sync_blob_expiration_epochs_loop(&self) {
        // Run sync every hour.
        // We need relatively fresh if others are extending blobs.
        let mut sync_interval = time::interval(Duration::from_secs(3600));

        // Initial delay to avoid overloading initial startup process.
        tokio::time::sleep(Duration::from_secs(60)).await;

        loop {
            sync_interval.tick().await;
            tracing::info!("starting scheduled blob expiration epoch sync");

            // Run both syncs concurrently.
            let (rocksdb_result, postgres_result) = tokio::join!(
                self.sync_all_blob_expiration_epochs(),
                self.sync_all_blob_expiration_epochs_to_postgres()
            );

            if let Err(e) = rocksdb_result {
                tracing::error!("failed to sync blob expiration epochs to RocksDB: {}", e);
                // Continue running despite errors.
            }

            if let Err(e) = postgres_result {
                tracing::error!("failed to sync blob expiration epochs to PostgreSQL: {}", e);
                // Continue running despite errors.
            }
        }
    }

    /// Check all blobs and extend those that are expiring soon.
    async fn check_and_extend_blobs(&self) -> Result<()> {
        tracing::info!("checking blobs for expiration");

        // Get current Walrus epoch.
        let current_epoch = self
            .sui_interactive_client
            .with_walrus_client_async(|client| {
                Box::pin(async move {
                    let committees = client.get_committees().await?;
                    Ok(committees.epoch())
                })
            })
            .await?;
        tracing::info!("current walrus epoch: {}", current_epoch);

        // Store tuple of (object_id, start_checkpoint, blob_id) for regular and shared blobs separately.
        let mut regular_blobs_to_extend = Vec::new();
        let mut shared_blobs_to_extend = Vec::new();

        {
            // Get all blobs from the archival state.
            // We need to drop blobs after iterating through them to avoid long memory usage.
            let blobs = self.archival_state.list_all_blobs(false, true)?;
            tracing::info!("found {} blobs to check", blobs.len());

            // We need to release blobs after iterating through them to avoid holding the lock for too long.
            for blob_info in blobs.into_iter() {
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

                    tracing::debug!(
                        "blob {} (object {}) expires at epoch {}, will extend (shared: {})",
                        blob_id,
                        object_id,
                        blob_end_epoch,
                        blob_info.is_shared_blob
                    );

                    let blob_tuple = (object_id, blob_info.start_checkpoint, blob_id);

                    if blob_info.is_shared_blob {
                        shared_blobs_to_extend.push(blob_tuple);
                    } else {
                        regular_blobs_to_extend.push(blob_tuple);
                    }
                }
            }
        }

        if regular_blobs_to_extend.is_empty() && shared_blobs_to_extend.is_empty() {
            tracing::info!("no blobs need extension");
            return Ok(());
        }

        tracing::info!(
            "extending {} regular blobs and {} shared blobs",
            regular_blobs_to_extend.len(),
            shared_blobs_to_extend.len()
        );

        // For reading blobs, get sui_read_client without holding the lock.
        let sui_read_client = self.sui_interactive_client.get_sui_read_client().await;

        // Extend regular blobs one by one.
        for (object_id, start_checkpoint, blob_id) in regular_blobs_to_extend {
            if let Err(e) = self.extend_regular_blob(object_id).await {
                tracing::error!("failed to extend regular blob {}: {}", object_id, e);
                // Continue with other blobs even if one fails.
                continue;
            }

            let new_end_epoch = self
                .get_blob_expiration_epoch(object_id, false, sui_read_client.clone())
                .await;

            // Update the archival state with the new expiration epoch.
            if let Err(e) = self.archival_state.update_blob_expiration_epoch(
                start_checkpoint,
                &blob_id,
                &object_id,
                new_end_epoch,
            ) {
                tracing::error!(
                    "failed to update archival state for blob {}: {}",
                    object_id,
                    e
                );
            }
        }

        let mut succeeded_count = 0;
        let mut succeeded_transactions = 0;

        // Extend shared blobs in batches of 100.
        for chunk in shared_blobs_to_extend.chunks(100) {
            //TODO: make the timeout configurable.
            tokio::time::sleep(Duration::from_secs(10)).await;

            if let Err(e) = self.extend_shared_blobs_batch(chunk).await {
                tracing::error!(
                    "failed to extend batch of {} shared blobs: {}",
                    chunk.len(),
                    e
                );
                // Continue with other batches even if one fails.
                continue;
            }

            // Update the archival state with the new expiration epochs.
            for (object_id, start_checkpoint, blob_id) in chunk {
                let new_end_epoch = self
                    .get_blob_expiration_epoch(*object_id, true, sui_read_client.clone())
                    .await;

                if let Err(e) = self.archival_state.update_blob_expiration_epoch(
                    *start_checkpoint,
                    blob_id,
                    object_id,
                    new_end_epoch,
                ) {
                    tracing::error!(
                        "failed to update archival state for blob {}: {}",
                        object_id,
                        e
                    );
                }
            }

            succeeded_count += chunk.len();
            succeeded_transactions += 1;
        }

        tracing::info!(
            "successfully extended {} shared blobs in {} transactions",
            succeeded_count,
            succeeded_transactions
        );

        Ok(())
    }

    /// Periodically sync all blob expiration epochs from on-chain state.
    /// Uses batch fetching with multi_get_object_with_options for efficiency.
    async fn sync_all_blob_expiration_epochs(&self) -> Result<()> {
        tracing::info!("syncing blob expiration epochs from on-chain state");

        // Get all blobs from the archival state.
        let blobs = self.archival_state.list_all_blobs(true, true)?;
        tracing::info!("found {} blobs to sync", blobs.len());

        let mut synced_count = 0;
        let mut error_count = 0;
        let mut total_processed = 0;

        // Get a read client for reading blob epoch.
        let sui_read_client = self.sui_interactive_client.get_sui_read_client().await;

        // Process blobs in batches.
        let batch_size = 50; // Sui RPC typically supports up to 50 objects per multi_get call.

        for batch in blobs.chunks(batch_size) {
            tracing::info!(
                "processing batch of {} blobs (total processed so far: {})",
                batch.len(),
                total_processed
            );

            // Parse all object IDs and blob IDs for this batch.
            let mut object_ids = Vec::with_capacity(batch.len());
            let mut valid_blobs = Vec::with_capacity(batch.len());

            for blob_info in batch {
                // Parse the object ID.
                let object_id = match ObjectID::from_bytes(&blob_info.object_id) {
                    Ok(id) => id,
                    Err(e) => {
                        tracing::error!("failed to parse object ID: {}", e);
                        error_count += 1;
                        continue;
                    }
                };

                // Parse the blob ID.
                let blob_id_str = String::from_utf8_lossy(&blob_info.blob_id).to_string();
                let blob_id: walrus_core::BlobId = match blob_id_str.parse() {
                    Ok(id) => id,
                    Err(e) => {
                        tracing::error!("failed to parse blob ID: {}", e);
                        error_count += 1;
                        continue;
                    }
                };

                object_ids.push(object_id);
                valid_blobs.push((blob_info, blob_id, object_id));
            }

            if object_ids.is_empty() {
                continue;
            }

            // Batch fetch all objects from on-chain.
            let objects = match sui_read_client
                .retriable_sui_client()
                .multi_get_object_with_options(
                    &object_ids,
                    sui_sdk::rpc_types::SuiObjectDataOptions::new().with_content(),
                )
                .await
            {
                Ok(objs) => objs,
                Err(e) => {
                    tracing::error!("failed to batch fetch objects from on-chain: {}", e);
                    error_count += valid_blobs.len();
                    total_processed += valid_blobs.len();
                    // Wait before retrying next batch.
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
            };

            // Process each object and compare epochs.
            for (i, obj_response) in objects.into_iter().enumerate() {
                total_processed += 1;
                let (blob_info, blob_id, object_id) = &valid_blobs[i];

                // Extract expiration epoch from object.
                let on_chain_epoch = match self
                    .extract_expiration_epoch_from_object(&obj_response, blob_info.is_shared_blob)
                {
                    Ok(epoch) => epoch,
                    Err(e) => {
                        tracing::error!(
                            "failed to extract expiration epoch for object {}: {}",
                            object_id,
                            e
                        );
                        error_count += 1;
                        continue;
                    }
                };

                // Compare with stored epoch.
                if on_chain_epoch != blob_info.blob_expiration_epoch {
                    tracing::info!(
                        "blob {} (checkpoint {}) expiration epoch mismatch: db={}, on-chain={}, updating db",
                        blob_id,
                        blob_info.start_checkpoint,
                        blob_info.blob_expiration_epoch,
                        on_chain_epoch
                    );

                    // Update the database.
                    if let Err(e) = self.archival_state.update_blob_expiration_epoch(
                        blob_info.start_checkpoint,
                        blob_id,
                        object_id,
                        on_chain_epoch,
                    ) {
                        tracing::error!(
                            "failed to update blob expiration epoch in db for blob {}: {}",
                            object_id,
                            e
                        );
                        error_count += 1;
                    } else {
                        synced_count += 1;
                    }
                }
            }

            // Small delay between batches to avoid overwhelming the RPC node.
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        tracing::info!(
            "blob expiration epoch sync completed: {} total blobs, {} updated, {} errors",
            total_processed,
            synced_count,
            error_count
        );

        Ok(())
    }

    /// Sync all blob expiration epochs from on-chain state to PostgreSQL.
    /// This reads blobs from PostgreSQL and compares with on-chain state,
    /// updating PostgreSQL if they don't match.
    /// Uses batch fetching with multi_get_object_with_options for efficiency.
    async fn sync_all_blob_expiration_epochs_to_postgres(&self) -> Result<()> {
        let pg_pool = match self.archival_state.postgres_pool() {
            Some(pool) => pool.clone(),
            None => {
                tracing::info!("PostgreSQL not configured, skipping postgres sync");
                return Ok(());
            }
        };

        tracing::info!("syncing blob expiration epochs from on-chain state to PostgreSQL");

        let mut synced_count = 0;
        let mut error_count = 0;
        let mut total_processed = 0;

        // Get a read client for reading blob epochs from on-chain.
        let sui_read_client = self.sui_interactive_client.get_sui_read_client().await;

        // Use cursor-based pagination to loop through all blobs.
        // Batch size for PostgreSQL query and on-chain fetch.
        let batch_size = 50; // Sui RPC typically supports up to 50 objects per multi_get call.
        let mut cursor: Option<i64> = None;

        loop {
            // Get a batch of blobs from PostgreSQL.
            let pg_blobs = pg_pool
                .list_all_blobs(false, Some(batch_size), cursor)
                .await?;

            if pg_blobs.is_empty() {
                break;
            }

            tracing::info!(
                "processing batch of {} blobs (total processed so far: {})",
                pg_blobs.len(),
                total_processed
            );

            // Update cursor for next iteration.
            cursor = pg_blobs.last().map(|b| b.start_checkpoint);

            // Parse all object IDs for this batch.
            let mut object_ids = Vec::with_capacity(pg_blobs.len());
            let mut valid_blobs = Vec::with_capacity(pg_blobs.len());

            for pg_blob in pg_blobs {
                match ObjectID::from_hex_literal(&pg_blob.object_id) {
                    Ok(id) => {
                        object_ids.push(id);
                        valid_blobs.push(pg_blob);
                    }
                    Err(e) => {
                        tracing::error!(
                            "failed to parse object ID {} from PostgreSQL: {}",
                            pg_blob.object_id,
                            e
                        );
                        error_count += 1;
                    }
                }
            }

            if object_ids.is_empty() {
                total_processed += valid_blobs.len();
                continue;
            }

            // Batch fetch all objects from on-chain.
            let objects = match sui_read_client
                .retriable_sui_client()
                .multi_get_object_with_options(
                    &object_ids,
                    sui_sdk::rpc_types::SuiObjectDataOptions::new().with_content(),
                )
                .await
            {
                Ok(objs) => objs,
                Err(e) => {
                    tracing::error!("failed to batch fetch objects from on-chain: {}", e);
                    error_count += valid_blobs.len();
                    total_processed += valid_blobs.len();
                    // Wait before retrying next batch.
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
            };

            // Process each object and compare epochs.
            for (i, obj_response) in objects.into_iter().enumerate() {
                total_processed += 1;
                let pg_blob = &valid_blobs[i];
                let object_id = object_ids[i];

                // Extract expiration epoch from object.
                let on_chain_epoch = match self
                    .extract_expiration_epoch_from_object(&obj_response, pg_blob.is_shared_blob)
                {
                    Ok(epoch) => epoch,
                    Err(e) => {
                        tracing::error!(
                            "failed to extract expiration epoch for object {}: {}",
                            object_id,
                            e
                        );
                        error_count += 1;
                        continue;
                    }
                };

                let pg_epoch = pg_blob.blob_expiration_epoch as u32;

                // Compare epochs.
                if on_chain_epoch != pg_epoch {
                    tracing::info!(
                        "blob {} (start_checkpoint {}) expiration epoch mismatch: pg={}, on-chain={}, updating pg",
                        object_id,
                        pg_blob.start_checkpoint,
                        pg_epoch,
                        on_chain_epoch
                    );

                    // Update PostgreSQL with on-chain value.
                    if let Err(e) = pg_pool
                        .update_blob_expiration_epoch(
                            pg_blob.start_checkpoint,
                            on_chain_epoch as i32,
                        )
                        .await
                    {
                        tracing::error!(
                            "failed to update PostgreSQL blob expiration epoch for start_checkpoint {}: {}",
                            pg_blob.start_checkpoint,
                            e
                        );
                        error_count += 1;
                    } else {
                        synced_count += 1;
                    }
                }
            }

            // Small delay between batches to avoid overwhelming the RPC node.
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        tracing::info!(
            "PostgreSQL blob expiration epoch sync completed: {} total blobs, {} updated, {} errors",
            total_processed,
            synced_count,
            error_count
        );

        Ok(())
    }

    /// Extract expiration epoch from a fetched object response.
    fn extract_expiration_epoch_from_object(
        &self,
        obj_response: &sui_sdk::rpc_types::SuiObjectResponse,
        is_shared_blob: bool,
    ) -> Result<u32> {
        let obj_data = obj_response
            .data
            .as_ref()
            .ok_or_else(|| anyhow!("object data not found"))?;

        let content = obj_data
            .content
            .as_ref()
            .ok_or_else(|| anyhow!("object content not found"))?;

        let fields = match content {
            sui_sdk::rpc_types::SuiParsedData::MoveObject(obj) => {
                obj.fields.clone().to_json_value()
            }
            _ => return Err(anyhow!("unexpected object type")),
        };

        if is_shared_blob {
            // SharedArchivalBlob structure: { id, blob: Blob { storage: { end_epoch } } }
            fields
                .get("blob")
                .ok_or_else(|| anyhow!("blob field not found"))?
                .get("storage")
                .ok_or_else(|| anyhow!("blob.storage not found"))?
                .get("end_epoch")
                .ok_or_else(|| anyhow!("blob.storage.end_epoch not found"))?
                .as_u64()
                .map(|v| v as u32)
                .ok_or_else(|| anyhow!("end_epoch is not a valid u64"))
        } else {
            // Regular Blob structure: { storage: { end_epoch } }
            fields
                .get("storage")
                .ok_or_else(|| anyhow!("storage not found"))?
                .get("end_epoch")
                .ok_or_else(|| anyhow!("storage.end_epoch not found"))?
                .as_u64()
                .map(|v| v as u32)
                .ok_or_else(|| anyhow!("end_epoch is not a valid u64"))
        }
    }

    /// Get the updated blob expiration epoch with infinite retry.
    async fn get_blob_expiration_epoch(
        &self,
        object_id: ObjectID,
        is_shared_blob: bool,
        sui_read_client: Arc<SuiReadClient>,
    ) -> u32 {
        // Get the updated blob expiration epoch with infinite retry.
        let mut retry_delay = self.config.min_transaction_retry_duration;
        let max_retry_delay = self.config.max_transaction_retry_duration;
        loop {
            let result = if is_shared_blob {
                // For shared blobs, parse the object to get expiration epoch.
                self.get_shared_blob_expiration_epoch(object_id, sui_read_client.clone())
                    .await
            } else {
                async {
                    let blob = sui_read_client
                        .get_blob_by_object_id(&object_id)
                        .await
                        .map_err(|e| anyhow!("failed to get blob: {}", e))?;
                    Ok(blob.blob.storage.end_epoch)
                }
                .await
            };

            match result {
                Ok(end_epoch) => {
                    tracing::debug!(
                        "successfully retrieved extended {} blob {} with new end_epoch: {}",
                        if is_shared_blob { "shared" } else { "regular" },
                        object_id,
                        end_epoch
                    );
                    break end_epoch;
                }
                Err(e) => {
                    tracing::error!(
                        "failed to get {} blob {} expiration epoch: {}, retrying in {:?}",
                        if is_shared_blob { "shared" } else { "regular" },
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

    /// Extend a single blob with retry logic.
    async fn extend_regular_blob(&self, object_id: ObjectID) -> Result<()> {
        // Increment the attempted counter.
        self.metrics.blob_extensions_attempted.inc();

        let mut retry_delay = self.config.min_transaction_retry_duration;
        let max_retry_delay = self.config.max_transaction_retry_duration;

        loop {
            tracing::info!(
                "attempting to extend regular blob {} by {} epochs",
                object_id,
                self.config.extend_epoch_length
            );

            let result = {
                let extend_epoch_length = self.config.extend_epoch_length;
                self.sui_interactive_client
                    .with_walrus_client_async(|client| {
                        Box::pin(async move {
                            client
                                .sui_client()
                                .extend_blob(object_id, extend_epoch_length)
                                .await
                                .map_err(|e| anyhow!("failed to extend blob: {}", e))
                        })
                    })
                    .await
            };

            match result {
                Ok(_) => {
                    tracing::info!("successfully extended regular blob {}", object_id);
                    // Increment the success counter.
                    self.metrics.blob_extensions_succeeded.inc();
                    return Ok(());
                }
                Err(e) => {
                    tracing::error!(
                        "failed to extend regular blob {}: {}, retrying in {:?}",
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

    /// Get the expiration epoch of a shared blob by parsing the object.
    async fn get_shared_blob_expiration_epoch(
        &self,
        shared_blob_id: ObjectID,
        sui_read_client: Arc<SuiReadClient>,
    ) -> Result<u32> {
        // Fetch shared blob object with content.
        let shared_blob_obj = sui_read_client
            .retriable_sui_client()
            .get_object_with_options(
                shared_blob_id,
                sui_sdk::rpc_types::SuiObjectDataOptions::new().with_content(),
            )
            .await?;

        let shared_blob_data = shared_blob_obj
            .data
            .ok_or_else(|| anyhow!("shared blob object data not found"))?;

        // Parse the content to get the blob field.
        let content = shared_blob_data
            .content
            .ok_or_else(|| anyhow!("shared blob object content not found"))?;

        // Extract fields from the Move object.
        let fields = match content {
            sui_sdk::rpc_types::SuiParsedData::MoveObject(obj) => obj.fields.to_json_value(),
            _ => return Err(anyhow!("unexpected object type")),
        };

        // Navigate to blob.storage.end_epoch.
        // Structure: SharedArchivalBlob { id, blob: Blob { ... } }
        // Blob contains a storage field with end_epoch.
        // TODO: use a more generic and robust way to parse the onchain object.
        let end_epoch = fields
            .get("blob")
            .ok_or_else(|| anyhow!("blob field not found"))?
            .get("storage")
            .ok_or_else(|| anyhow!("blob.storage not found"))?
            .get("end_epoch")
            .ok_or_else(|| anyhow!("blob.storage.end_epoch not found"))?
            .as_u64()
            .ok_or_else(|| anyhow!("end_epoch is not a number"))? as u32;

        Ok(end_epoch)
    }

    /// Extend multiple shared blobs in a batch using a single PTB transaction.
    async fn extend_shared_blobs_batch(
        &self,
        blobs: &[(ObjectID, u64, walrus_core::BlobId)],
    ) -> Result<()> {
        if blobs.is_empty() {
            return Ok(());
        }

        // Increment the attempted counter for each blob.
        self.metrics
            .blob_extensions_attempted
            .inc_by(blobs.len() as u64);

        let package_id = self.contract_package_id;
        let extend_epoch_length = self.config.extend_epoch_length;
        let system_object_id = self.system_object_id;
        let wal_token_package_id = self.wal_token_package_id;

        // Extract just the object IDs for logging.
        let blob_ids: Vec<ObjectID> = blobs.iter().map(|(id, _, _)| *id).collect();

        tracing::info!(
            "extending batch of {} shared blobs by {} epochs",
            blobs.len(),
            extend_epoch_length
        );

        self.sui_interactive_client
            .with_wallet_mut_async(|wallet| {
                let blob_ids = blob_ids.clone();
                Box::pin(async move {
                    let sui_client = wallet.get_client().await?;
                    let active_address = wallet.active_address()?;

                    // Fetch System object to get initial shared version.
                    let system_obj = sui_client
                        .read_api()
                        .get_object_with_options(
                            system_object_id,
                            sui_sdk::rpc_types::SuiObjectDataOptions::new()
                                .with_owner()
                                .with_previous_transaction(),
                        )
                        .await?;

                    let system_data = system_obj
                        .data
                        .ok_or_else(|| anyhow!("system object data not found"))?;

                    let system_initial_shared_version = match system_data.owner {
                        Some(Owner::Shared {
                            initial_shared_version,
                        }) => initial_shared_version,
                        _ => return Err(anyhow!("system object is not a shared object")),
                    };

                    // Construct WAL coin type from package ID.
                    let wal_coin_type = format!("{}::wal::WAL", wal_token_package_id);

                    // Get WAL coins for payment.
                    let wal_coins = sui_client
                        .coin_read_api()
                        .get_coins(active_address, Some(wal_coin_type), None, None)
                        .await?;

                    if wal_coins.data.is_empty() {
                        return Err(anyhow!(
                            "no WAL coins available for address {}",
                            active_address
                        ));
                    }

                    // Get SUI coins for gas.
                    let sui_coins = sui_client
                        .coin_read_api()
                        .get_coins(active_address, None, None, None)
                        .await?;

                    if sui_coins.data.is_empty() {
                        return Err(anyhow!(
                            "no SUI coins available for address {}",
                            active_address
                        ));
                    }

                    // Build programmable transaction.
                    let mut ptb = ProgrammableTransactionBuilder::new();

                    // Create system argument (shared once for all calls).
                    let system_arg = ptb.obj(ObjectArg::SharedObject {
                        id: system_object_id,
                        initial_shared_version: system_initial_shared_version,
                        mutability: SharedObjectMutability::Mutable,
                    })?;

                    // Create payment coin argument (shared once for all calls).
                    let payment_coin_ref = wal_coins.data[0].object_ref();
                    let payment_arg = ptb.obj(ObjectArg::ImmOrOwnedObject(payment_coin_ref))?;

                    // Create extend_epochs argument (shared once for all calls).
                    let extend_epochs_arg = ptb.pure(extend_epoch_length)?;

                    // For each blob, fetch its initial shared version and add a move call.
                    for shared_blob_id in &blob_ids {
                        // Fetch shared blob object to get initial shared version.
                        let shared_blob_obj = sui_client
                            .read_api()
                            .get_object_with_options(
                                *shared_blob_id,
                                sui_sdk::rpc_types::SuiObjectDataOptions::new()
                                    .with_owner()
                                    .with_previous_transaction(),
                            )
                            .await?;

                        let shared_blob_data = shared_blob_obj
                            .data
                            .ok_or_else(|| anyhow!("shared blob object data not found"))?;

                        let shared_blob_initial_shared_version = match shared_blob_data.owner {
                            Some(Owner::Shared {
                                initial_shared_version,
                            }) => initial_shared_version,
                            _ => {
                                return Err(anyhow!(
                                    "shared blob object {} is not a shared object",
                                    shared_blob_id
                                ))
                            }
                        };

                        // Create shared blob argument for this call.
                        let shared_blob_arg = ptb.obj(ObjectArg::SharedObject {
                            id: *shared_blob_id,
                            initial_shared_version: shared_blob_initial_shared_version,
                            mutability: SharedObjectMutability::Mutable,
                        })?;

                        // Call extend_shared_blob_using_token function for this blob.
                        ptb.programmable_move_call(
                            package_id,
                            Identifier::new("archival_blob")?,
                            Identifier::new("extend_shared_blob_using_token")?,
                            vec![],
                            vec![system_arg, shared_blob_arg, extend_epochs_arg, payment_arg],
                        );
                    }

                    let pt = ptb.finish();

                    tracing::info!(
                        "executing batch extend transaction for {} shared blobs - package: {}, epochs: {}",
                        blob_ids.len(),
                        package_id,
                        extend_epoch_length
                    );

                    // Create transaction data.
                    let gas_budget = 1_000_000_000; // 1 SUI for batch operations.
                    let gas_price = sui_client.read_api().get_reference_gas_price().await?;

                    // Use the first SUI coin for gas.
                    let gas_coin = &sui_coins.data[0];

                    let tx_data = TransactionData::new(
                        TransactionKind::ProgrammableTransaction(pt),
                        active_address,
                        gas_coin.object_ref(),
                        gas_budget,
                        gas_price,
                    );

                    let response = execute_transaction_and_check_status(wallet, tx_data).await?;

                    tracing::info!(
                        "successfully extended batch of {} shared blobs, tx digest: {:?}",
                        blob_ids.len(),
                        response.digest
                    );

                    Ok(())
                })
            })
            .await?;

        // Increment the success counter for each blob in the batch.
        self.metrics
            .blob_extensions_succeeded
            .inc_by(blobs.len() as u64);

        Ok(())
    }
}

impl Clone for CheckpointBlobExtender {
    fn clone(&self) -> Self {
        Self {
            archival_state: self.archival_state.clone(),
            sui_interactive_client: self.sui_interactive_client.clone(),
            config: self.config.clone(),
            metrics: self.metrics.clone(),
            contract_package_id: self.contract_package_id,
            system_object_id: self.system_object_id,
            wal_token_package_id: self.wal_token_package_id,
        }
    }
}
