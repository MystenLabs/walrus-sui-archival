// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::path::PathBuf;

use anyhow::Result;
use clap::Subcommand;
use sui_types::{base_types::ObjectID, messages_checkpoint::CheckpointSequenceNumber};

use crate::archival_state::ArchivalState;

#[derive(Subcommand, Debug)]
pub enum InspectDbCommand {
    /// Get checkpoint blob info for a specific checkpoint.
    GetCheckpointBlobInfo {
        /// Checkpoint number to look up.
        #[arg(short, long)]
        checkpoint: u64,
    },
    /// Get the latest stored checkpoint number.
    GetLatestCheckpoint,
    /// List all blobs tracked in the database.
    ListBlobs,
}

/// Execute the inspect database command.
pub async fn execute_inspect_db(db_path: PathBuf, command: InspectDbCommand) -> Result<()> {
    // Open the archival state database.
    let state = ArchivalState::open(&db_path, true)?;
    tracing::info!("opened archival database at {}", db_path.display());

    match command {
        InspectDbCommand::GetCheckpointBlobInfo { checkpoint } => {
            get_checkpoint_blob_info(&state, checkpoint).await
        }
        InspectDbCommand::GetLatestCheckpoint => get_latest_checkpoint(&state),
        InspectDbCommand::ListBlobs => list_blobs(&state),
    }
}

/// Get and display checkpoint blob info for a specific checkpoint.
async fn get_checkpoint_blob_info(state: &ArchivalState, checkpoint: u64) -> Result<()> {
    let checkpoint_seq = CheckpointSequenceNumber::from(checkpoint);

    match state.get_checkpoint_blob_info(checkpoint_seq).await {
        Ok(blob_info) => {
            let index_position = checkpoint_seq - blob_info.start_checkpoint;
            println!("checkpoint blob info for checkpoint {}:", checkpoint);
            println!("  blob id: {}", String::from_utf8_lossy(&blob_info.blob_id));
            let object_id =
                ObjectID::from_bytes(&blob_info.object_id).expect("Failed to parse object ID");
            println!("  object id: {}", object_id);
            println!("  index position: {}", index_position);
            println!(
                "  offset in blob: {}",
                blob_info.index_entries[index_position as usize].offset
            );
            println!(
                "  length in blob: {}",
                blob_info.index_entries[index_position as usize].length
            );
            println!("  start checkpoint: {}", blob_info.start_checkpoint);
            println!("  end checkpoint: {}", blob_info.end_checkpoint);
            println!("  end of epoch: {}", blob_info.end_of_epoch);
            println!(
                "  blob expiration epoch: {}",
                blob_info.blob_expiration_epoch
            );
            println!("  index entries count: {}", blob_info.index_entries.len());

            // Calculate total size.
            let total_size: u64 = blob_info.index_entries.iter().map(|e| e.length).sum();
            println!("\n  total blob data size: {} bytes", total_size);
        }
        Err(e) => {
            eprintln!(
                "error: failed to get checkpoint blob info for checkpoint {}: {}",
                checkpoint, e
            );
            return Err(e);
        }
    }

    Ok(())
}

/// Get and display the latest stored checkpoint number.
fn get_latest_checkpoint(state: &ArchivalState) -> Result<()> {
    match state.get_latest_stored_checkpoint()? {
        Some(checkpoint) => {
            println!("latest stored checkpoint: {}", checkpoint);
        }
        None => {
            println!("no checkpoints stored in the database");
        }
    }

    Ok(())
}

/// List all blobs tracked in the database.
fn list_blobs(state: &ArchivalState) -> Result<()> {
    let blobs = state.list_all_blobs(true)?;

    if blobs.is_empty() {
        println!("no blobs tracked in the database");
        return Ok(());
    }

    println!("found {} blob(s) in the database:\n", blobs.len());
    println!(
        "{:<70} {:<70} {:<15} {:<15} {:<10} {:<15} {:<10}",
        "Blob ID", "Object ID", "Start CP", "End CP", "EOE", "Expiry Epoch", "Entries"
    );
    println!("{}", "-".repeat(215));

    let mut total_entries = 0;
    let mut total_size = 0u64;

    for blob_info in &blobs {
        let blob_id = String::from_utf8_lossy(&blob_info.blob_id);
        let object_id =
            ObjectID::from_bytes(&blob_info.object_id).expect("Failed to parse object ID");
        let entries_count = blob_info.index_entries.len();
        let blob_size: u64 = blob_info.index_entries.iter().map(|e| e.length).sum();

        total_entries += entries_count;
        total_size += blob_size;

        println!(
            "{:<70} {:<70} {:<15} {:<15} {:<10} {:<15} {:<10}",
            blob_id,
            object_id,
            blob_info.start_checkpoint,
            blob_info.end_checkpoint,
            if blob_info.end_of_epoch { "Yes" } else { "No" },
            blob_info.blob_expiration_epoch,
            entries_count
        );
    }

    println!("\nsummary:");
    println!("  total blobs: {}", blobs.len());
    println!("  total checkpoint entries: {}", total_entries);
    println!(
        "  total data size: {} bytes ({:.2} GB)",
        total_size,
        total_size as f64 / (1024.0 * 1024.0 * 1024.0)
    );

    Ok(())
}
