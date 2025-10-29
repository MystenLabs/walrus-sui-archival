// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::path::Path;

use anyhow::Result;
use sui_types::messages_checkpoint::CheckpointSequenceNumber;

use crate::archival_state::ArchivalState;

/// Remove all checkpoint blob entries from the database with start_checkpoint >= from_checkpoint.
pub fn remove_metadata_from_db(
    db_path: impl AsRef<Path>,
    from_checkpoint: CheckpointSequenceNumber,
) -> Result<()> {
    let db_path = db_path.as_ref();

    tracing::info!(
        "opening database at {} to remove entries >= checkpoint {}",
        db_path.display(),
        from_checkpoint
    );

    // Open the database in read-write mode.
    let archival_state = ArchivalState::open(db_path, false)?;

    // Remove entries.
    let num_removed = archival_state.remove_entries_from_checkpoint(from_checkpoint)?;

    tracing::info!("successfully removed {} entries from database", num_removed);

    println!(
        "removed {} checkpoint blob entries with start_checkpoint >= {}",
        num_removed, from_checkpoint
    );
    println!("database path: {}", db_path.display());

    Ok(())
}
