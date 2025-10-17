// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use anyhow::{Result, anyhow};
use sui_types::{
    Identifier,
    base_types::ObjectID,
    object::Owner,
    programmable_transaction_builder::ProgrammableTransactionBuilder,
    transaction::{ObjectArg, TransactionData, TransactionKind},
};
use tokio::time;
use walrus_sdk::sui::client::ReadClient;

use crate::{
    archival_state::ArchivalState,
    config::CheckpointBlobExtenderConfig,
    metrics::Metrics,
    sui_interactive_client::SuiInteractiveClient,
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

        // Get all blobs from the archival state.
        let blobs = self.archival_state.list_all_blobs()?;
        tracing::info!("found {} blobs to check", blobs.len());

        // Store tuple of (object_id, start_checkpoint, blob_id, is_shared_blob) for blobs to extend.
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
                    "blob {} (object {}) expires at epoch {}, will extend (shared: {})",
                    blob_id,
                    object_id,
                    blob_end_epoch,
                    blob_info.is_shared_blob
                );

                blobs_to_extend.push((
                    object_id,
                    blob_info.start_checkpoint,
                    blob_id,
                    blob_info.is_shared_blob,
                ));
            }
        }

        if blobs_to_extend.is_empty() {
            tracing::info!("no blobs need extension");
            return Ok(());
        }

        tracing::info!("extending {} blobs", blobs_to_extend.len());

        // Extend each blob.
        for (object_id, start_checkpoint, blob_id, is_shared_blob) in blobs_to_extend {
            if let Err(e) = self.extend_blob(object_id, is_shared_blob).await {
                tracing::error!("failed to extend blob {}: {}", object_id, e);
                // Continue with other blobs even if one fails.
                continue;
            }

            // Get the updated blob object to get the new expiration epoch with infinite retry.
            let mut retry_delay = self.config.min_transaction_retry_duration;
            let max_retry_delay = self.config.max_transaction_retry_duration;

            let blob = loop {
                let result = self
                    .sui_interactive_client
                    .with_walrus_client_async(|client| {
                        Box::pin(async move {
                            client
                                .sui_client()
                                .get_blob_by_object_id(&object_id)
                                .await
                                .map_err(|e| anyhow!("failed to get blob: {}", e))
                        })
                    })
                    .await;

                match result {
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
    async fn extend_blob(&self, object_id: ObjectID, is_shared_blob: bool) -> Result<()> {
        // Increment the attempted counter.
        self.metrics.blob_extensions_attempted.inc();

        let mut retry_delay = self.config.min_transaction_retry_duration;
        let max_retry_delay = self.config.max_transaction_retry_duration;

        loop {
            tracing::info!(
                "attempting to extend {} blob {} by {} epochs",
                if is_shared_blob { "shared" } else { "regular" },
                object_id,
                self.config.extend_epoch_length
            );

            let result = if is_shared_blob {
                self.extend_shared_blob_using_contract(object_id).await
            } else {
                self.extend_regular_blob(object_id).await
            };

            match result {
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

    /// Extend a regular blob using the Walrus SDK.
    async fn extend_regular_blob(&self, object_id: ObjectID) -> Result<()> {
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
    }

    /// Extend a shared blob using the Move contract call.
    async fn extend_shared_blob_using_contract(&self, shared_blob_id: ObjectID) -> Result<()> {
        let package_id = self.contract_package_id;
        let extend_epoch_length = self.config.extend_epoch_length;
        let system_object_id = self.system_object_id;
        let wal_token_package_id = self.wal_token_package_id;

        self.sui_interactive_client
            .with_wallet_mut_async(|wallet| {
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

                    // Fetch shared blob object to get initial shared version.
                    let shared_blob_obj = sui_client
                        .read_api()
                        .get_object_with_options(
                            shared_blob_id,
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
                        _ => return Err(anyhow!("shared blob object is not a shared object")),
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

                    // Create arguments for the function call.
                    let system_arg = ptb.obj(ObjectArg::SharedObject {
                        id: system_object_id,
                        initial_shared_version: system_initial_shared_version,
                        mutable: true,
                    })?;
                    let shared_blob_arg = ptb.obj(ObjectArg::SharedObject {
                        id: shared_blob_id,
                        initial_shared_version: shared_blob_initial_shared_version,
                        mutable: true,
                    })?;
                    let extend_epochs_arg = ptb.pure(extend_epoch_length)?;

                    // Create a mutable payment coin argument using WAL tokens.
                    // We'll use the first WAL coin and make it mutable.
                    let payment_coin_ref = wal_coins.data[0].object_ref();
                    let payment_arg = ptb.obj(ObjectArg::ImmOrOwnedObject(payment_coin_ref))?;

                    // Call extend_shared_blob_using_token function.
                    ptb.programmable_move_call(
                        package_id,
                        Identifier::new("archival_blob")?,
                        Identifier::new("extend_shared_blob_using_token")?,
                        vec![],
                        vec![system_arg, shared_blob_arg, extend_epochs_arg, payment_arg],
                    );

                    let pt = ptb.finish();

                    tracing::info!(
                        "executing extend_shared_blob_using_token transaction - package: {}, shared_blob: {}, epochs: {}",
                        package_id,
                        shared_blob_id,
                        extend_epoch_length
                    );

                    // Create transaction data.
                    let gas_budget = 500_000_000; // 0.5 SUI.
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

                    let signed_tx = wallet.sign_transaction(&tx_data).await;
                    let response = wallet.execute_transaction_may_fail(signed_tx).await?;

                    tracing::info!(
                        "successfully extended shared blob {}, tx digest: {:?}",
                        shared_blob_id,
                        response.digest
                    );

                    Ok(())
                })
            })
            .await
    }
}
