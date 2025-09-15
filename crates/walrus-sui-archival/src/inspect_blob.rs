// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{path::PathBuf, str::FromStr};

use anyhow::{Context, Result};
use blob_bundle::BlobBundleReader;
use bytes::Bytes;
use sui_storage::blob::Blob;
use sui_types::full_checkpoint_content::CheckpointData;
use walrus_core::{BlobId, encoding::Primary};
use walrus_sdk::{client::WalrusNodeClient, config::ClientConfig};

/// Inspect a blob - either from a local file or by fetching from Walrus.
///
/// # Arguments
/// * `path` - Optional path to the blob file
/// * `blob_id` - Optional blob ID to fetch from Walrus
/// * `client_config` - Optional path to client config (required if using blob_id)
/// * `index` - Optional index to inspect a specific entry
/// * `offset` - Optional offset to read from (used when index is not specified)
/// * `length` - Optional length to read (used when index is not specified)
/// * `full` - Print full deserialized CheckpointData
pub async fn inspect_blob(
    path: Option<PathBuf>,
    blob_id: Option<String>,
    client_config: Option<PathBuf>,
    index: Option<usize>,
    offset: Option<u64>,
    length: Option<u64>,
    full: bool,
) -> Result<()> {
    // Get the blob data either from file or Walrus.
    let bytes = match (path, blob_id) {
        (Some(path), None) => {
            // Read from local file.
            tracing::info!("reading blob from local file: {}", path.display());
            let data = std::fs::read(&path)?;
            Bytes::from(data)
        }
        (None, Some(blob_id_str)) => {
            // Fetch from Walrus.
            tracing::info!("fetching blob from walrus: {}", blob_id_str);

            let client_config_path = client_config
                .ok_or_else(|| anyhow::anyhow!("client config required when using blob ID"))?;

            // Parse the blob ID.
            let blob_id = BlobId::from_str(&blob_id_str).context("failed to parse blob ID")?;

            // Fetch the blob from Walrus.
            fetch_blob_from_walrus(blob_id, client_config_path).await?
        }
        (Some(_), Some(_)) => {
            return Err(anyhow::anyhow!("cannot specify both path and blob ID"));
        }
        (None, None) => {
            return Err(anyhow::anyhow!("must specify either path or blob ID"));
        }
    };

    // Open the blob bundle for reading.
    let reader = BlobBundleReader::new(bytes)?;

    if let Some(idx) = index {
        // Inspect a specific entry by index.
        inspect_by_index(&reader, idx, full)?;
    } else if let (Some(offset), Some(length)) = (offset, length) {
        // Read raw data by offset and length.
        inspect_by_range(&reader, offset, length, full)?;
    } else {
        // List all entries.
        list_all_entries(&reader)?;
    }

    Ok(())
}

/// Inspect a specific entry by index.
fn inspect_by_index(reader: &BlobBundleReader, idx: usize, full: bool) -> Result<()> {
    // Get list of all IDs in the blob.
    let ids = reader.list_ids()?;
    println!("blob metadata:");
    println!("  entry count: {}", ids.len());
    println!("  total size: {} bytes", reader.total_size());
    println!();
    if idx >= ids.len() {
        return Err(anyhow::anyhow!(
            "index {} out of range, blob has {} entries",
            idx,
            ids.len()
        ));
    }

    let id = &ids[idx];
    println!("entry at index {}:", idx);
    println!("  id: {}", id);

    // Get entry metadata.
    if let Some(entry) = reader.get_entry(id)? {
        println!("  offset: {} bytes", entry.offset);
        println!("  size: {} bytes", entry.length);
    }
    println!();

    // Read and display the entry data.
    let data = reader.get(id)?;
    println!("checkpoint data ({} bytes):", data.len());

    // Try to deserialize as checkpoint.
    match Blob::from_bytes::<CheckpointData>(&data) {
        Ok(checkpoint_data) => {
            if full {
                // Print full deserialized CheckpointData.
                println!("full checkpoint data:");
                println!("{:#?}", checkpoint_data);
            } else {
                // Print summary only.
                println!(
                    "  checkpoint number: {}",
                    checkpoint_data.checkpoint_summary.sequence_number
                );
                println!("  epoch: {}", checkpoint_data.checkpoint_summary.epoch);
                println!(
                    "  timestamp: {} ms",
                    checkpoint_data.checkpoint_summary.timestamp_ms
                );
                println!(
                    "  network total transactions: {}",
                    checkpoint_data
                        .checkpoint_summary
                        .network_total_transactions
                );
                println!(
                    "  transaction count: {}",
                    checkpoint_data.transactions.len()
                );
                if checkpoint_data
                    .checkpoint_summary
                    .end_of_epoch_data
                    .is_some()
                {
                    println!("  end of epoch: yes");
                } else {
                    println!("  end of epoch: no");
                }
            }
        }
        Err(e) => {
            println!("  failed to deserialize checkpoint data: {}", e);
            println!("  raw data (first 256 bytes):");
            let display_len = std::cmp::min(256, data.len());
            println!("  {:?}", &data[..display_len]);
        }
    }

    Ok(())
}

/// Read raw data by offset and length.
fn inspect_by_range(reader: &BlobBundleReader, offset: u64, length: u64, full: bool) -> Result<()> {
    println!("reading raw data:");
    println!("  offset: {} bytes", offset);
    println!("  length: {} bytes", length);
    println!();

    // Read the raw data using read_range.
    let data = reader.read_range(offset, length)?;
    println!("raw data ({} bytes):", data.len());

    // Try to deserialize as checkpoint if it looks like one.
    match Blob::from_bytes::<CheckpointData>(&data) {
        Ok(checkpoint_data) => {
            println!("successfully deserialized as checkpoint:");
            if full {
                // Print full deserialized CheckpointData.
                println!("full checkpoint data:");
                println!("{:#?}", checkpoint_data);
            } else {
                // Print summary only.
                println!(
                    "  checkpoint number: {}",
                    checkpoint_data.checkpoint_summary.sequence_number
                );
                println!("  epoch: {}", checkpoint_data.checkpoint_summary.epoch);
                println!(
                    "  timestamp: {} ms",
                    checkpoint_data.checkpoint_summary.timestamp_ms
                );
                println!(
                    "  network total transactions: {}",
                    checkpoint_data
                        .checkpoint_summary
                        .network_total_transactions
                );
                println!(
                    "  transaction count: {}",
                    checkpoint_data.transactions.len()
                );
                if checkpoint_data
                    .checkpoint_summary
                    .end_of_epoch_data
                    .is_some()
                {
                    println!("  end of epoch: yes");
                } else {
                    println!("  end of epoch: no");
                }
            }
        }
        Err(_) => {
            // Not a checkpoint, show raw data.
            println!("raw bytes (first 256 bytes):");
            let display_len = std::cmp::min(256, data.len());

            // Display as hex dump.
            for i in (0..display_len).step_by(16) {
                print!("  {:08x}: ", offset as usize + i);

                // Hex bytes.
                for j in 0..16 {
                    if i + j < display_len {
                        print!("{:02x} ", data[i + j]);
                    } else {
                        print!("   ");
                    }
                    if j == 7 {
                        print!(" ");
                    }
                }

                print!(" |");

                // ASCII representation.
                for j in 0..16 {
                    if i + j < display_len {
                        let byte = data[i + j];
                        if byte.is_ascii_graphic() || byte == b' ' {
                            print!("{}", byte as char);
                        } else {
                            print!(".");
                        }
                    }
                }

                println!("|");
            }

            if data.len() > 256 {
                println!("  ... ({} more bytes)", data.len() - 256);
            }
        }
    }

    Ok(())
}

/// Fetch a blob from Walrus by its ID.
async fn fetch_blob_from_walrus(blob_id: BlobId, client_config_path: PathBuf) -> Result<Bytes> {
    // Load the client configuration.
    let (client_config, _) =
        ClientConfig::load_from_multi_config(&client_config_path, Some("testnet"))
            .context("failed to load client config")?;

    // Create the Sui client.
    let sui_client = client_config
        .new_contract_client_with_wallet_in_config(None)
        .await
        .context("failed to create Sui client")?;

    // Create the Walrus client.
    let walrus_client =
        WalrusNodeClient::new_contract_client_with_refresher(client_config, sui_client)
            .await
            .context("failed to create Walrus client")?;

    // Fetch the blob.
    tracing::info!("downloading blob {} from walrus...", blob_id);
    let data = walrus_client
        .read_blob_retry_committees::<Primary>(&blob_id)
        .await
        .context("failed to fetch blob from Walrus")?;

    tracing::info!("successfully fetched blob ({} bytes)", data.len());
    Ok(Bytes::from(data))
}

/// List all entries in the blob.
fn list_all_entries(reader: &BlobBundleReader) -> Result<()> {
    // Get list of all IDs in the blob.
    let ids = reader.list_ids()?;
    println!("blob metadata:");
    println!("  entry count: {}", ids.len());
    println!("  total size: {} bytes", reader.total_size());
    println!();

    println!("blob entries:");
    for (i, id) in ids.iter().enumerate() {
        let entry = reader.get_entry(id)?;
        if let Some(entry) = entry {
            println!(
                "  [{}] id: {}, offset: {}, size: {} bytes",
                i, id, entry.offset, entry.length
            );
        } else {
            println!("  [{}] id: {} (entry not found)", i, id);
            continue;
        }

        // Try to get checkpoint metadata.
        let data = reader.get(id)?;
        match Blob::from_bytes::<CheckpointData>(&data) {
            Ok(checkpoint_data) => {
                println!(
                    "      checkpoint: {}, epoch: {}, timestamp: {} ms",
                    checkpoint_data.checkpoint_summary.sequence_number,
                    checkpoint_data.checkpoint_summary.epoch,
                    checkpoint_data.checkpoint_summary.timestamp_ms
                );
            }
            Err(_) => {
                println!("      (not a valid checkpoint)");
            }
        }
    }

    Ok(())
}
