// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::path::Path;

use anyhow::Result;
use sui_sdk::wallet_context::WalletContext;
use sui_types::{
    Identifier,
    TypeTag,
    base_types::ObjectID,
    effects::TransactionEffectsAPI,
    programmable_transaction_builder::ProgrammableTransactionBuilder,
    transaction::{ObjectArg, SharedObjectMutability, TransactionData, TransactionKind},
};
use walrus_sdk::config::ClientConfig;

use crate::{config::Config, util::execute_transaction_and_check_status};

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
    let system_initial_shared_version =
        crate::util::get_initial_shared_version(&wallet, system_object_id).await?;

    // Fetch shared blob object to get initial shared version.
    let shared_blob_initial_shared_version =
        crate::util::get_initial_shared_version(&wallet, shared_blob_id).await?;

    // Construct WAL coin type from package ID.
    let wal_coin_type: TypeTag = format!("{}::wal::WAL", wal_token_package_id).parse()?;

    // Get a WAL coin for payment.
    let payment_coin_ref =
        crate::util::get_one_coin_ref_of_type(&wallet, active_address, wal_coin_type).await?;

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
    let gas_coin_ref = crate::util::get_gas_coin_ref(&wallet, active_address, gas_budget).await?;
    let gas_price = wallet.get_reference_gas_price().await?;

    let tx_data = TransactionData::new(
        TransactionKind::ProgrammableTransaction(pt),
        active_address,
        gas_coin_ref,
        gas_budget,
        gas_price,
    );

    let response = execute_transaction_and_check_status(&wallet, tx_data).await?;

    tracing::info!(
        "successfully extended shared blob {}, tx digest: {:?}",
        shared_blob_id,
        response.effects.transaction_digest()
    );

    println!(
        "successfully extended shared blob {} by {} epochs",
        shared_blob_id, extend_epochs
    );
    println!(
        "transaction digest: {:?}",
        response.effects.transaction_digest()
    );

    Ok(())
}
