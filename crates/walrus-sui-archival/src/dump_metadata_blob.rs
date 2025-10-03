// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::path::Path;

use anyhow::Result;
use sui_types::base_types::ObjectID;

use crate::{
    config::Config,
    util::{fetch_metadata_blob_from_walrus, parse_checkpoint_blob_infos_from_parquet},
};

/// Dump the content of the metadata blob.
///
/// This function reads the on-chain metadata pointer, fetches the blob from Walrus,
/// parses the parquet file, and dumps all CheckpointBlobInfo entries.
pub async fn dump_metadata_blob(config_path: impl AsRef<Path>) -> Result<()> {
    tracing::info!("dumping metadata blob content");

    let config = Config::from_file(config_path)?;
    let pointer_id = config.archival_state_snapshot.metadata_pointer_object_id;

    // Fetch and download the metadata blob.
    let (blob_id, blob_data) = match fetch_metadata_blob_from_walrus(
        &config.client_config_path,
        pointer_id,
        &config.context,
    )
    .await
    {
        Ok(result) => result,
        Err(e) => {
            if e.to_string().contains("no metadata blob ID found") {
                println!("No metadata blob ID found in the on-chain pointer.");
                return Ok(());
            }
            return Err(e);
        }
    };

    println!("Metadata blob ID: {}", blob_id);
    println!("Downloaded {} bytes", blob_data.len());

    // Parse the parquet file.
    println!("\nParsing parquet file...");
    let checkpoint_blob_infos = parse_checkpoint_blob_infos_from_parquet(&blob_data)?;

    println!("Total entries: {}\n", checkpoint_blob_infos.len());

    // Display all entries.
    for (idx, checkpoint_blob_info) in checkpoint_blob_infos.iter().enumerate() {
        let blob_id = String::from_utf8_lossy(&checkpoint_blob_info.blob_id);
        let object_id =
            ObjectID::from_bytes(&checkpoint_blob_info.object_id).unwrap_or(ObjectID::ZERO);

        println!("--- Entry {} ---", idx + 1);
        println!("  Blob ID: {}", blob_id);
        println!(
            "  Start checkpoint: {}",
            checkpoint_blob_info.start_checkpoint
        );
        println!("  End checkpoint: {}", checkpoint_blob_info.end_checkpoint);
        println!("  End of epoch: {}", checkpoint_blob_info.end_of_epoch);
        println!(
            "  Blob expiration epoch: {}",
            checkpoint_blob_info.blob_expiration_epoch
        );
        println!("  Object ID: {}", object_id);
        println!(
            "  Index entries: {} (cleared in snapshot)",
            checkpoint_blob_info.index_entries.len()
        );
        println!();
    }

    println!("Metadata blob dump completed successfully.");

    Ok(())
}
