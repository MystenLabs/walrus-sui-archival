// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::path::Path;

use anyhow::Result;
use parquet::file::reader::{FileReader, SerializedFileReader};
use prost::Message;
use walrus_core::encoding::Primary;
use walrus_sdk::{ObjectID, client::WalrusNodeClient};

use crate::{
    archival_state::proto::CheckpointBlobInfo,
    config::Config,
    util::fetch_metadata_blob_id,
};

/// Dump the content of the metadata blob.
///
/// This function reads the on-chain metadata pointer, fetches the blob from Walrus,
/// parses the parquet file, and dumps all CheckpointBlobInfo entries.
pub async fn dump_metadata_blob(config_path: impl AsRef<Path>) -> Result<()> {
    tracing::info!("dumping metadata blob content");

    let config = Config::from_file(config_path)?;
    let pointer_id = config.archival_state_snapshot.metadata_pointer_object_id;

    // Fetch the blob ID from the on-chain metadata pointer.
    let blob_id_opt = fetch_metadata_blob_id(&config.client_config_path, pointer_id).await?;

    let blob_id = match blob_id_opt {
        Some(id) => {
            println!("Metadata blob ID: {}", id);
            id
        }
        None => {
            println!("No metadata blob ID found in the on-chain pointer.");
            return Ok(());
        }
    };

    // Initialize Walrus client to fetch the blob.
    let (client_config, _) = walrus_sdk::config::ClientConfig::load_from_multi_config(
        &config.client_config_path,
        Some("testnet"),
    )?;
    let sui_client = client_config
        .new_contract_client_with_wallet_in_config(None)
        .await?;
    let read_client = sui_client.read_client().clone();
    let walrus_read_client =
        WalrusNodeClient::new_read_client_with_refresher(client_config, read_client).await?;

    // Download the blob from Walrus.
    println!("Downloading blob from Walrus...");
    let blob_data = walrus_read_client.read_blob::<Primary>(&blob_id).await?;
    println!("Downloaded {} bytes", blob_data.len());

    // Write blob data to a temporary file for parquet parsing.
    let temp_file = std::env::temp_dir().join(format!("metadata_blob_{}.parquet", blob_id));
    std::fs::write(&temp_file, &blob_data)?;

    // Parse the parquet file.
    println!("Parsing parquet file...");
    let file = std::fs::File::open(&temp_file)?;
    let reader = SerializedFileReader::new(file)?;

    let metadata = reader.metadata();
    println!(
        "Parquet file schema: {:?}",
        metadata.file_metadata().schema()
    );
    println!("Number of row groups: {}", metadata.num_row_groups());

    let mut total_entries = 0;

    // Iterate through all row groups.
    for i in 0..metadata.num_row_groups() {
        let row_group_reader = reader.get_row_group(i)?;
        println!(
            "\nRow group {}: {} rows",
            i,
            row_group_reader.metadata().num_rows()
        );

        // Get the column reader for checkpoint_blob_info.
        let mut column_reader = row_group_reader.get_column_reader(0)?;

        if let parquet::column::reader::ColumnReader::ByteArrayColumnReader(ref mut reader) =
            column_reader
        {
            let num_rows = row_group_reader.metadata().num_rows() as usize;
            let mut values = Vec::with_capacity(num_rows);
            let mut def_levels = Vec::with_capacity(num_rows);

            let (num_read, _, _) =
                reader.read_records(num_rows, Some(&mut def_levels), None, &mut values)?;

            println!("Read {} entries from row group {}", num_read, i);

            for (idx, value) in values.iter().enumerate() {
                let checkpoint_blob_info = CheckpointBlobInfo::decode(value.data())?;
                total_entries += 1;

                let blob_id = String::from_utf8_lossy(&checkpoint_blob_info.blob_id);
                let object_id =
                    ObjectID::from_bytes(&checkpoint_blob_info.object_id).unwrap_or(ObjectID::ZERO);

                println!(
                    "\n--- Entry {} (Row group {}, Index {}) ---",
                    total_entries, i, idx
                );
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
            }
        }
    }

    // Clean up temporary file.
    std::fs::remove_file(&temp_file)?;

    println!("\nTotal entries: {}", total_entries);
    println!("Metadata blob dump completed successfully.");

    Ok(())
}
