// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::path::Path;

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
use walrus_sdk::config::ClientConfig;

use crate::{
    archival_state::{ArchivalState, CF_CHECKPOINT_BLOB_INFO, proto::CheckpointBlobInfo},
    config::Config,
};

pub async fn delete_all_shared_archival_blobs(config_path: impl AsRef<Path>) -> Result<()> {
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

    // Open the database to read checkpoint blob info.
    let archival_state = ArchivalState::open(&config.db_path, true)?;
    let db = archival_state.get_db();
    let cf_handle = db
        .cf_handle(CF_CHECKPOINT_BLOB_INFO)
        .ok_or_else(|| anyhow::anyhow!("failed to get column family handle"))?;

    // Collect all shared blob object IDs.
    let mut shared_blob_ids = Vec::new();
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
                shared_blob_ids.push(object_id);
            }
        }
    }

    tracing::info!("found {} shared blobs to delete", shared_blob_ids.len());

    if shared_blob_ids.is_empty() {
        tracing::info!("no shared blobs to delete");
        return Ok(());
    }

    let package_id = config.archival_state_snapshot.contract_package_id;
    let admin_cap_id = config.archival_state_snapshot.admin_cap_object_id;

    // Delete each shared blob.
    for (index, shared_blob_id) in shared_blob_ids.iter().enumerate() {
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

        tracing::info!(
            "deleting shared blob {} of {}: {}",
            index + 1,
            shared_blob_ids.len(),
            shared_blob_id
        );

        // Fetch shared blob object to get initial shared version.
        let shared_blob_obj_result = sui_client
            .read_api()
            .get_object_with_options(
                *shared_blob_id,
                sui_sdk::rpc_types::SuiObjectDataOptions::new().with_owner(),
            )
            .await;

        if let Err(e) = shared_blob_obj_result {
            tracing::error!("failed to get shared blob object: {}", e);
            continue;
        }

        let shared_blob_obj = shared_blob_obj_result.unwrap();

        let shared_blob_data_result = shared_blob_obj.data.ok_or_else(|| {
            anyhow::anyhow!("shared blob object data not found: {}", shared_blob_id)
        });

        if let Err(e) = shared_blob_data_result {
            tracing::error!("failed to get shared blob object data: {}", e);
            continue;
        }

        let shared_blob_data = shared_blob_data_result.unwrap();

        let shared_blob_initial_shared_version = match shared_blob_data.owner {
            Some(sui_types::object::Owner::Shared {
                initial_shared_version,
            }) => initial_shared_version,
            _ => {
                return Err(anyhow::anyhow!(
                    "shared blob object is not a shared object: {}",
                    shared_blob_id
                ));
            }
        };

        // Build programmable transaction.
        let mut ptb = ProgrammableTransactionBuilder::new();

        // Create arguments for the function call.
        let admin_cap_arg = ptb.obj(ObjectArg::ImmOrOwnedObject(admin_cap_ref))?;
        let shared_blob_arg = ptb.obj(ObjectArg::SharedObject {
            id: *shared_blob_id,
            initial_shared_version: shared_blob_initial_shared_version,
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

        let pt = ptb.finish();

        tracing::info!(
            "executing delete shared blob transaction - package: {}, shared_blob: {}",
            package_id,
            shared_blob_id
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

        // Create transaction data.
        let gas_budget = 100_000_000; // 0.1 SUI.
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

        tracing::info!(
            "successfully deleted shared blob {}, tx digest: {:?}",
            shared_blob_id,
            response.digest
        );
    }

    println!(
        "successfully deleted {} shared archival blobs",
        shared_blob_ids.len()
    );

    Ok(())
}
