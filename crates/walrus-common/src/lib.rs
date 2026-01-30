// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Common utilities and helper functions shared across walrus-sui-archival crates.

use anyhow::Result;
use serde::Deserialize;
use sui_sdk::{SuiClient, types::base_types::ObjectID as SuiObjectID};
use sui_storage::blob::Blob;
use sui_types::full_checkpoint_content::CheckpointData;
use walrus_core::BlobId;

#[derive(Deserialize)]
#[allow(dead_code)]
struct MetadataBlobPointer {
    id: SuiObjectID,
    blob_id: Option<Vec<u8>>,
}

/// Fetch checkpoint content from aggregator.
pub async fn fetch_checkpoint_content(
    blob_id: &str,
    offset: u64,
    length: u64,
) -> Result<CheckpointData> {
    let url = format!(
        "https://aggregator.walrus-mainnet.walrus.space/v1/blobs/{}/byte-range?start={}&length={}",
        blob_id, offset, length
    );

    tracing::info!("fetching checkpoint content from: {}", url);

    // Fetch the data from the aggregator.
    let client = reqwest::Client::new();
    let response = client
        .get(&url)
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("failed to fetch from aggregator: {}", e))?;

    if !response.status().is_success() {
        return Err(anyhow::anyhow!(
            "aggregator returned error status: {}",
            response.status()
        ));
    }

    let bcs_data = response
        .bytes()
        .await
        .map_err(|e| anyhow::anyhow!("failed to read response body: {}", e))?;

    // Decode using BCS.
    let checkpoint_data = Blob::from_bytes::<CheckpointData>(&bcs_data)
        .map_err(|e| anyhow::anyhow!("failed to decode checkpoint data: {}", e))?;

    Ok(checkpoint_data)
}

/// Fetches the blob ID from the metadata pointer object on-chain using a Sui client.
///
/// Returns the BlobId if it exists, or None if the pointer is not set.
pub async fn fetch_metadata_blob_id_from_sui_client(
    sui_client: &SuiClient,
    metadata_pointer_object_id: SuiObjectID,
) -> Result<Option<BlobId>> {
    // read the metadata pointer object.
    let object_response = sui_client
        .read_api()
        .get_object_with_options(
            metadata_pointer_object_id,
            sui_sdk::rpc_types::SuiObjectDataOptions::new().with_bcs(),
        )
        .await?;

    let object_data = object_response
        .data
        .ok_or_else(|| anyhow::anyhow!("metadata pointer object not found"))?;

    // extract blob_id from the object.
    if let Some(bcs_data) = object_data.bcs
        && let sui_sdk::rpc_types::SuiRawData::MoveObject(move_obj) = bcs_data
    {
        // decode BCS to extract the Option<vector<u8>> blob_id field.
        let pointer: MetadataBlobPointer = bcs::from_bytes(&move_obj.bcs_bytes)?;

        if let Some(blob_id_bytes) = pointer.blob_id {
            // convert Vec<u8> to BlobId.
            if blob_id_bytes.len() == 32 {
                let mut array = [0u8; 32];
                array.copy_from_slice(&blob_id_bytes);
                return Ok(Some(BlobId(array)));
            } else {
                return Err(anyhow::anyhow!(
                    "invalid blob_id length: expected 32 bytes, got {}",
                    blob_id_bytes.len()
                ));
            }
        } else {
            return Ok(None);
        }
    }

    Err(anyhow::anyhow!(
        "failed to extract blob_id from metadata pointer object"
    ))
}
