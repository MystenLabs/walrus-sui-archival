// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::path::PathBuf;

use anyhow::{Context, Result};
use walrus_sdk::{
    config::ClientConfig,
    sui::{client::ExpirySelectionPolicy, types::Blob},
};

/// Burns all blobs owned by the wallet in the client config.
/// This is a development tool for cleaning up test blobs.
pub async fn burn_all_blobs(client_config: PathBuf) -> Result<()> {
    tracing::warn!(
        "starting burn all blobs operation - THIS WILL DELETE ALL BLOBS OWNED BY YOUR WALLET!"
    );

    // Load the client configuration.
    let (client_config, _) = ClientConfig::load_from_multi_config(&client_config, Some("testnet"))
        .context("failed to load client config")?;

    // Create the Sui client with wallet.
    let sui_client = client_config
        .new_contract_client_with_wallet_in_config(None)
        .await
        .context("failed to create Sui client")?;

    // Get all blobs owned by the wallet.
    tracing::info!("fetching all blobs owned by wallet...");
    let owned_blobs: Vec<Blob> = sui_client
        .owned_blobs(None, ExpirySelectionPolicy::All)
        .await
        .context("failed to fetch owned blobs")?;

    if owned_blobs.is_empty() {
        tracing::info!("no blobs found to burn");
        return Ok(());
    }

    tracing::info!("found {} blobs to burn", owned_blobs.len());

    // Burn the blob objects.
    let object_ids = owned_blobs.iter().map(|blob| blob.id).collect::<Vec<_>>();
    let burn_blobs_result = sui_client.burn_blobs(&object_ids).await;
    tracing::info!(
        "burn blobs result: {:?}, burned count: {}",
        burn_blobs_result,
        owned_blobs.len(),
    );

    Ok(())
}
