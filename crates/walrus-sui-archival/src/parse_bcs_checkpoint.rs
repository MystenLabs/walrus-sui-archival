// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::path::PathBuf;

use anyhow::Result;
use sui_storage::blob::Blob;
use sui_types::full_checkpoint_content::CheckpointData;

/// Parse and print a BCS-encoded CheckpointData file.
pub fn parse_bcs_checkpoint(file_path: PathBuf) -> Result<()> {
    // Read the file.
    let bcs_data = std::fs::read(&file_path)?;

    tracing::info!(
        "read {} bytes from file: {}",
        bcs_data.len(),
        file_path.display()
    );

    // Deserialize using Blob::from_bytes.
    let checkpoint_data = Blob::from_bytes::<CheckpointData>(&bcs_data)?;

    tracing::info!("successfully parsed BCS checkpoint data");

    // Convert to JSON for pretty printing.
    let json_output = serde_json::to_string_pretty(&checkpoint_data)?;

    println!("{}", json_output);

    Ok(())
}
