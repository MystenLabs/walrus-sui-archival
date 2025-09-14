use std::path::PathBuf;

use anyhow::Result;
use blob_bundle::BlobBundleReader;
use bytes::Bytes;
use sui_storage::blob::Blob;
use sui_types::full_checkpoint_content::CheckpointData;

/// Inspect a blob file.
///
/// # Arguments
/// * `path` - Path to the blob file
/// * `index` - Optional index to inspect a specific entry
/// * `offset` - Optional offset to read from (used when index is not specified)
/// * `length` - Optional length to read (used when index is not specified)
pub fn inspect_blob(
    path: PathBuf,
    index: Option<usize>,
    offset: Option<u64>,
    length: Option<u64>,
) -> Result<()> {
    // Read the blob file.
    let data = std::fs::read(&path)?;
    let bytes = Bytes::from(data);

    // Open the blob bundle for reading.
    let reader = BlobBundleReader::new(bytes)?;

    if let Some(idx) = index {
        // Inspect a specific entry by index.
        inspect_by_index(&reader, idx)?;
    } else if let (Some(offset), Some(length)) = (offset, length) {
        // Read raw data by offset and length.
        inspect_by_range(&reader, offset, length)?;
    } else {
        // List all entries.
        list_all_entries(&reader)?;
    }

    Ok(())
}

/// Inspect a specific entry by index.
fn inspect_by_index(reader: &BlobBundleReader, idx: usize) -> Result<()> {
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
fn inspect_by_range(reader: &BlobBundleReader, offset: u64, length: u64) -> Result<()> {
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
