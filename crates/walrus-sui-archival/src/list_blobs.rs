// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::path::PathBuf;

use anyhow::{Context, Result};
use walrus_sdk::{
    config::ClientConfig,
    sui::{client::ExpirySelectionPolicy, types::Blob},
};

/// Lists all blobs owned by the wallet in the client config.
pub async fn list_owned_blobs(client_config: PathBuf, context: &str) -> Result<()> {
    tracing::info!("listing all blobs owned by wallet...");

    // Load the client configuration.
    let (client_config, _) = ClientConfig::load_from_multi_config(&client_config, Some(context))
        .context("failed to load client config")?;

    // Create the Sui client with wallet.
    let sui_client = client_config
        .new_contract_client_with_wallet_in_config(None)
        .await
        .context("failed to create Sui client")?;

    // Get all blobs owned by the wallet.
    let owned_blobs: Vec<Blob> = sui_client
        .owned_blobs(None, ExpirySelectionPolicy::All)
        .await
        .context("failed to fetch owned blobs")?;

    if owned_blobs.is_empty() {
        println!("No blobs found.");
        return Ok(());
    }

    println!("Found {} blob(s):\n", owned_blobs.len());
    println!(
        "{:<70} {:<70} {:<15} {:<10} {:<10}",
        "Blob ID", "Object ID", "Size (bytes)", "Encoding", "Expiry Epoch"
    );
    println!("{}", "-".repeat(145));

    for blob in owned_blobs {
        let blob_id = blob.blob_id;
        let object_id = blob.id;
        let size = blob.size;
        let encoding = format!("{:?}", blob.encoding_type);
        let expiry_epoch = blob.storage.end_epoch;

        println!(
            "{:<70} {:<70} {:<15} {:<10} {:<10}",
            blob_id, object_id, size, encoding, expiry_epoch
        );
    }

    Ok(())
}
