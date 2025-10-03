// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::path::Path;

use anyhow::Result;

use crate::{config::Config, util::fetch_metadata_blob_id};

pub async fn get_metadata_blob_id(config_path: impl AsRef<Path>) -> Result<()> {
    let config = Config::from_file(config_path)?;

    let pointer_id = config.archival_state_snapshot.metadata_pointer_object_id;

    println!("metadata pointer object ID: {}", pointer_id);

    let blob_id_opt = fetch_metadata_blob_id(&config.client_config_path, pointer_id).await?;

    if let Some(blob_id) = blob_id_opt {
        println!("blob ID: {}", blob_id);
    } else {
        println!("blob ID: none (not set)");
    }

    Ok(())
}
