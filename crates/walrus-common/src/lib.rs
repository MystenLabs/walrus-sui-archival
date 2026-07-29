// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Common utilities and helper functions shared across walrus-sui-archival crates.

use anyhow::{Context, Result};
use prost_014::Message as _;
use serde::Deserialize;
use sui_rpc::proto::sui::rpc::v2::Checkpoint;
use sui_sdk::types::base_types::ObjectID as SuiObjectID;
use sui_storage::blob::Blob;
use sui_types::full_checkpoint_content::{self, CheckpointData};
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

/// Fetch checkpoint content from aggregator, decoding zstd-compressed protobuf format.
///
/// Converts the proto checkpoint to `CheckpointData` (same as the BCS format), which includes
/// full transaction data with input_objects and output_objects per transaction.
pub async fn fetch_checkpoint_content_proto(
    blob_id: &str,
    offset: u64,
    length: u64,
) -> Result<serde_json::Value> {
    let url = format!(
        "https://aggregator.walrus-mainnet.walrus.space/v1/blobs/{}/byte-range?start={}&length={}",
        blob_id, offset, length
    );

    tracing::info!("fetching proto checkpoint content from: {}", url);

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

    let compressed = response
        .bytes()
        .await
        .map_err(|e| anyhow::anyhow!("failed to read response body: {}", e))?;

    // 1. Decompress zstd.
    let proto_bytes =
        zstd::decode_all(compressed.as_ref()).context("failed to zstd-decompress checkpoint")?;

    // 2. Decode protobuf Checkpoint message.
    let proto_checkpoint = Checkpoint::decode(proto_bytes.as_slice())
        .context("failed to decode protobuf checkpoint")?;

    // 3. Convert proto Checkpoint → sui-types Checkpoint → CheckpointData.
    // This reconstructs per-transaction input_objects and output_objects from the
    // checkpoint-level deduped ObjectSet, matching the BCS format exactly.
    let sui_checkpoint = full_checkpoint_content::Checkpoint::try_from(&proto_checkpoint)
        .map_err(|e| anyhow::anyhow!("failed to convert proto to sui-types checkpoint: {}", e))?;
    let checkpoint_data = CheckpointData::from(sui_checkpoint);

    // 4. Serialize to JSON.
    let value = serde_json::to_value(checkpoint_data)
        .context("failed to serialize CheckpointData to JSON")?;

    Ok(value)
}

/// Fetches the blob ID from the metadata pointer object on-chain using a Sui grpc client.
///
/// Returns the BlobId if it exists, or None if the pointer is not set.
pub async fn fetch_metadata_blob_id_from_grpc(
    grpc_client: &mut sui_rpc_api::Client,
    metadata_pointer_object_id: SuiObjectID,
) -> Result<Option<BlobId>> {
    // read the metadata pointer object.
    let object = grpc_client
        .get_object(metadata_pointer_object_id)
        .await
        .context("failed to fetch metadata pointer object")?;

    let move_object = object
        .data
        .try_as_move()
        .ok_or_else(|| anyhow::anyhow!("metadata pointer object is not a move object"))?;

    // decode BCS to extract the Option<vector<u8>> blob_id field.
    let pointer: MetadataBlobPointer = bcs::from_bytes(move_object.contents())?;

    if let Some(blob_id_bytes) = pointer.blob_id {
        // convert Vec<u8> to BlobId.
        if blob_id_bytes.len() == 32 {
            let mut array = [0u8; 32];
            array.copy_from_slice(&blob_id_bytes);
            Ok(Some(BlobId(array)))
        } else {
            Err(anyhow::anyhow!(
                "invalid blob_id length: expected 32 bytes, got {}",
                blob_id_bytes.len()
            ))
        }
    } else {
        Ok(None)
    }
}
