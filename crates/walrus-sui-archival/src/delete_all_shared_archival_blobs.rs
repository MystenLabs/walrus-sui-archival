// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::path::{Path, PathBuf};

use anyhow::Result;
use prost::Message;
use rocksdb::IteratorMode;
use sui_sdk::wallet_context::WalletContext;
use sui_types::{
    Identifier,
    base_types::ObjectID,
    programmable_transaction_builder::ProgrammableTransactionBuilder,
    transaction::{ObjectArg, TransactionData, TransactionKind},
};
use tokio::fs;
use walrus_sdk::config::ClientConfig;

use crate::{
    archival_state::{ArchivalState, CF_CHECKPOINT_BLOB_INFO, proto::CheckpointBlobInfo},
    config::Config,
};

pub async fn delete_all_shared_archival_blobs(
    config_path: impl AsRef<Path>,
    blob_list_file: Option<PathBuf>,
) -> Result<()> {
    let config = Config::from_file(config_path)?;
    let (client_config, _) =
        ClientConfig::load_from_multi_config(config.client_config_path, Some(&config.context))?;
    let mut wallet = WalletContext::new(
        client_config
            .wallet_config
            .ok_or_else(|| anyhow::anyhow!("wallet config is required"))?
            .path()
            .ok_or_else(|| anyhow::anyhow!("wallet config path is required"))?,
    )?;
    let sui_client = wallet.get_client().await?;
    let active_address = wallet.active_address()?;

    tracing::info!("deleting all shared archival blobs");

    // Collect all shared blob object IDs.
    let shared_blob_ids = if let Some(file_path) = blob_list_file {
        // Read blob IDs from file.
        tracing::info!("reading blob IDs from file: {}", file_path.display());
        let file_content = fs::read_to_string(&file_path).await?;
        let mut blob_ids = Vec::new();

        for (line_num, line) in file_content.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                // Skip empty lines and comments.
                continue;
            }

            match line.parse::<ObjectID>() {
                Ok(object_id) => blob_ids.push(object_id),
                Err(e) => {
                    tracing::warn!(
                        "failed to parse blob ID at line {}: '{}', error: {}",
                        line_num + 1,
                        line,
                        e
                    );
                }
            }
        }

        tracing::info!("loaded {} blob IDs from file", blob_ids.len());
        blob_ids
    } else {
        // Read from database.
        tracing::info!("reading blob IDs from database");
        let archival_state = ArchivalState::open(&config.db_path, true)?;
        let db = archival_state.get_db();
        let cf_handle = db
            .cf_handle(CF_CHECKPOINT_BLOB_INFO)
            .ok_or_else(|| anyhow::anyhow!("failed to get column family handle"))?;

        let mut blob_ids = Vec::new();
        let iter = db.iterator_cf(&cf_handle, IteratorMode::Start);
        for item in iter {
            let (_key, value) = item?;
            let checkpoint_blob_info = CheckpointBlobInfo::decode(value.as_ref())?;

            if checkpoint_blob_info.is_shared_blob {
                let object_id_bytes = checkpoint_blob_info.object_id;
                if object_id_bytes.len() == 32 {
                    let mut array = [0u8; 32];
                    array.copy_from_slice(&object_id_bytes);
                    let object_id = ObjectID::from_bytes(array)?;
                    blob_ids.push(object_id);
                }
            }
        }

        tracing::info!("loaded {} blob IDs from database", blob_ids.len());
        blob_ids
    };

    tracing::info!("found {} shared blobs to delete", shared_blob_ids.len());

    if shared_blob_ids.is_empty() {
        tracing::info!("no shared blobs to delete");
        return Ok(());
    }

    let package_id = config.archival_state_snapshot.contract_package_id;
    let admin_cap_id = config.archival_state_snapshot.admin_cap_object_id;

    // Batch size for deleting blobs in a single transaction.
    const BATCH_SIZE: usize = 100;

    // Process blobs in batches.
    let total_batches = (shared_blob_ids.len() + BATCH_SIZE - 1) / BATCH_SIZE;
    let mut total_deleted = 0;

    for (batch_index, batch) in shared_blob_ids.chunks(BATCH_SIZE).enumerate() {
        tracing::info!(
            "processing batch {} of {} ({} blobs)",
            batch_index + 1,
            total_batches,
            batch.len()
        );

        // Fetch admin cap object to get version and digest.
        let admin_cap_obj = sui_client
            .read_api()
            .get_object_with_options(
                admin_cap_id,
                sui_sdk::rpc_types::SuiObjectDataOptions::default(),
            )
            .await?;
        let admin_cap_ref = admin_cap_obj
            .object_ref_if_exists()
            .ok_or_else(|| anyhow::anyhow!("admin cap object not found"))?;

        // Fetch all shared blob objects in this batch.
        let mut blob_data = Vec::new();
        for shared_blob_id in batch {
            let shared_blob_obj_result = sui_client
                .read_api()
                .get_object_with_options(
                    *shared_blob_id,
                    sui_sdk::rpc_types::SuiObjectDataOptions::new().with_owner(),
                )
                .await;

            match shared_blob_obj_result {
                Ok(shared_blob_obj) => {
                    if let Some(data) = shared_blob_obj.data {
                        match data.owner {
                            Some(sui_types::object::Owner::Shared {
                                initial_shared_version,
                            }) => {
                                blob_data.push((*shared_blob_id, initial_shared_version));
                            }
                            _ => {
                                tracing::warn!(
                                    "blob {} is not a shared object, skipping",
                                    shared_blob_id
                                );
                            }
                        }
                    } else {
                        tracing::warn!("blob {} has no data, skipping", shared_blob_id);
                    }
                }
                Err(e) => {
                    tracing::warn!("failed to get blob {}: {}, skipping", shared_blob_id, e);
                }
            }
        }

        if blob_data.is_empty() {
            tracing::warn!("batch {} has no valid blobs to delete", batch_index + 1);
            continue;
        }

        // Build programmable transaction with multiple delete calls.
        let mut ptb = ProgrammableTransactionBuilder::new();

        // Create admin cap argument (shared across all delete calls).
        let admin_cap_arg = ptb.obj(ObjectArg::ImmOrOwnedObject(admin_cap_ref))?;

        // Add delete call for each blob.
        for (blob_id, initial_shared_version) in &blob_data {
            let shared_blob_arg = ptb.obj(ObjectArg::SharedObject {
                id: *blob_id,
                initial_shared_version: *initial_shared_version,
                mutable: true,
            })?;

            // Call delete_shared_blob function.
            ptb.programmable_move_call(
                package_id,
                Identifier::new("archival_blob")?,
                Identifier::new("delete_shared_blob")?,
                vec![],
                vec![admin_cap_arg, shared_blob_arg],
            );
        }

        let pt = ptb.finish();

        tracing::info!(
            "executing batch delete transaction for {} blobs",
            blob_data.len()
        );

        // Get gas payment object.
        let coins = sui_client
            .coin_read_api()
            .get_coins(active_address, None, None, None)
            .await?;

        if coins.data.is_empty() {
            return Err(anyhow::anyhow!(
                "no gas coins available for address {}",
                active_address
            ));
        }

        let gas_coin = &coins.data[0];

        // Create transaction data with larger gas budget for batch.
        let gas_budget = 500_000_000; // 0.5 SUI for batch operations.
        let gas_price = sui_client.read_api().get_reference_gas_price().await?;

        let tx_data = TransactionData::new(
            TransactionKind::ProgrammableTransaction(pt),
            active_address,
            gas_coin.object_ref(),
            gas_budget,
            gas_price,
        );

        let signed_tx = wallet.sign_transaction(&tx_data).await;
        let response = wallet.execute_transaction_may_fail(signed_tx).await?;

        total_deleted += blob_data.len();

        tracing::info!(
            "successfully deleted batch {} ({} blobs), tx digest: {:?}",
            batch_index + 1,
            blob_data.len(),
            response.digest
        );
    }

    println!(
        "successfully deleted {} shared archival blobs (out of {} total)",
        total_deleted,
        shared_blob_ids.len()
    );

    Ok(())
}
