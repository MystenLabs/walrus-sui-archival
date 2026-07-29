// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::path::Path;

use anyhow::Result;
use sui_sdk::wallet_context::WalletContext;
use sui_types::{
    Identifier,
    effects::TransactionEffectsAPI,
    programmable_transaction_builder::ProgrammableTransactionBuilder,
    transaction::{ObjectArg, SharedObjectMutability, TransactionData, TransactionKind},
};
use walrus_sdk::config::ClientConfig;

use crate::{config::Config, util::execute_transaction_and_check_status};

pub async fn clear_metadata_blob_id(config_path: impl AsRef<Path>) -> Result<()> {
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

    tracing::info!("clearing metadata blob id for pointer object");

    let package_id = config.archival_state_snapshot.contract_package_id;
    let pointer_id = config.archival_state_snapshot.metadata_pointer_object_id;
    let admin_cap_id = config.archival_state_snapshot.admin_cap_object_id;

    // fetch admin cap object to get version and digest.
    let admin_cap_ref = crate::util::get_object_ref(&wallet, admin_cap_id).await?;

    // build programmable transaction.
    let mut ptb = ProgrammableTransactionBuilder::new();

    // create arguments for the function call.
    let admin_cap_arg = ptb.obj(ObjectArg::ImmOrOwnedObject(admin_cap_ref))?;
    let pointer_id_arg = ptb.obj(ObjectArg::SharedObject {
        id: pointer_id,
        initial_shared_version: config
            .archival_state_snapshot
            .metadata_pointer_initial_shared_version,
        mutability: SharedObjectMutability::Mutable,
    })?;

    // call clear_metadata_blob_pointer function.
    ptb.programmable_move_call(
        package_id,
        Identifier::new("archival_metadata")?,
        Identifier::new("clear_metadata_blob_pointer")?,
        vec![],
        vec![admin_cap_arg, pointer_id_arg],
    );

    let pt = ptb.finish();

    tracing::info!(
        "executing clear metadata pointer transaction - package: {}, pointer: {}, admin_cap: {}",
        package_id,
        pointer_id,
        admin_cap_id
    );

    // create transaction data.
    let gas_budget = 100_000_000; // 0.1 SUI.
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
        "successfully cleared metadata pointer, tx digest: {:?}",
        response.effects.transaction_digest()
    );

    println!(
        "transaction digest: {:?}",
        response.effects.transaction_digest()
    );
    println!("successfully cleared blob ID from metadata pointer");

    Ok(())
}
