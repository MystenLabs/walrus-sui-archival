// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::path::Path;

use anyhow::{Result, anyhow};
use sui_sdk::wallet_context::WalletContext;
use sui_types::{
    Identifier,
    base_types::ObjectID,
    object::Owner,
    programmable_transaction_builder::ProgrammableTransactionBuilder,
    transaction::{ObjectArg, SharedObjectMutability, TransactionData, TransactionKind},
};
use walrus_sdk::config::ClientConfig;

use crate::config::Config;

/// Extend a shared blob's storage period using the caller's own WAL tokens.
pub async fn extend_shared_blob(
    config_path: impl AsRef<Path>,
    shared_blob_id: ObjectID,
    extend_epochs: u32,
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

    tracing::info!(
        "extending shared blob {} by {} epochs",
        shared_blob_id,
        extend_epochs
    );

    let package_id = config.archival_state_snapshot.contract_package_id;
    let system_object_id = client_config.contract_config.system_object;
    let wal_token_package_id = config.archival_state_snapshot.wal_token_package_id;

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
        mutability: SharedObjectMutability::Mutable,
    })?;
    let shared_blob_arg = ptb.obj(ObjectArg::SharedObject {
        id: shared_blob_id,
        initial_shared_version: shared_blob_initial_shared_version,
        mutability: SharedObjectMutability::Mutable,
    })?;
    let extend_epochs_arg = ptb.pure(extend_epochs)?;

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
        extend_epochs
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

    println!(
        "successfully extended shared blob {} by {} epochs",
        shared_blob_id, extend_epochs
    );
    println!("transaction digest: {:?}", response.digest);

    Ok(())
}
