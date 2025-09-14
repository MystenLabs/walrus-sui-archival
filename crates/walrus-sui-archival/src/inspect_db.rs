use std::path::PathBuf;

use anyhow::Result;
use clap::Subcommand;
use sui_types::messages_checkpoint::CheckpointSequenceNumber;

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
}

/// Execute the inspect database command.
pub fn execute_inspect_db(db_path: PathBuf, command: InspectDbCommand) -> Result<()> {
    // Open the archival state database.
    let state = ArchivalState::open(&db_path, true)?;
    tracing::info!("opened archival database at {}", db_path.display());

    match command {
        InspectDbCommand::GetCheckpointBlobInfo { checkpoint } => {
            get_checkpoint_blob_info(&state, checkpoint)
        }
        InspectDbCommand::GetLatestCheckpoint => get_latest_checkpoint(&state),
    }
}

/// Get and display checkpoint blob info for a specific checkpoint.
fn get_checkpoint_blob_info(state: &ArchivalState, checkpoint: u64) -> Result<()> {
    let checkpoint_seq = CheckpointSequenceNumber::from(checkpoint);

    match state.get_checkpoint_blob_info(checkpoint_seq) {
        Ok(blob_info) => {
            let index_position = checkpoint_seq - blob_info.start_checkpoint;
            println!("checkpoint blob info for checkpoint {}:", checkpoint);
            println!("  blob id: {}", String::from_utf8_lossy(&blob_info.blob_id));
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
