// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Common utilities and helper functions shared across walrus-sui-archival crates.

use anyhow::{Context, Result};
use prost_014::Message as _;
use serde::Deserialize;
use sui_rpc::proto::sui::rpc::v2::Checkpoint;
use sui_sdk::{SuiClient, types::base_types::ObjectID as SuiObjectID};
use sui_storage::blob::Blob;
use sui_types::{full_checkpoint_content::CheckpointData, messages_checkpoint::CheckpointSummary};
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
/// Returns a `serde_json::Value` with key checkpoint fields extracted from the proto message.
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

    let sequence_number = proto_checkpoint.sequence_number.unwrap_or_default();

    // 3. BCS-decode the CheckpointSummary from the proto summary to get rich metadata.
    let summary_json = if let Some(summary) = proto_checkpoint.summary.as_ref() {
        if let Some(bcs_field) = summary.bcs.as_ref() {
            if let Some(bcs_bytes) = bcs_field.value.as_ref() {
                match bcs::from_bytes::<CheckpointSummary>(bcs_bytes) {
                    Ok(cs) => serde_json::json!({
                        "epoch": cs.epoch,
                        "sequence_number": cs.sequence_number,
                        "network_total_transactions": cs.network_total_transactions,
                        "timestamp_ms": cs.timestamp_ms,
                        "previous_digest": cs.previous_digest.map(|d| d.to_string()),
                        "end_of_epoch_data": cs.end_of_epoch_data.is_some(),
                        "content_digest": cs.content_digest.to_string(),
                        "epoch_rolling_gas_cost_summary": {
                            "computation_cost": cs.epoch_rolling_gas_cost_summary.computation_cost,
                            "storage_cost": cs.epoch_rolling_gas_cost_summary.storage_cost,
                            "storage_rebate": cs.epoch_rolling_gas_cost_summary.storage_rebate,
                            "non_refundable_storage_fee": cs.epoch_rolling_gas_cost_summary.non_refundable_storage_fee,
                        },
                    }),
                    Err(e) => {
                        tracing::warn!("failed to BCS-decode CheckpointSummary: {}", e);
                        serde_json::Value::Null
                    }
                }
            } else {
                serde_json::Value::Null
            }
        } else {
            serde_json::Value::Null
        }
    } else {
        serde_json::Value::Null
    };

    // 4. Extract transaction info from the proto checkpoint.
    let transactions: Vec<serde_json::Value> = proto_checkpoint
        .transactions
        .iter()
        .map(|tx| {
            let digest = tx
                .digest
                .clone()
                .or_else(|| {
                    tx.effects
                        .as_ref()
                        .and_then(|e| e.transaction_digest.clone())
                })
                .unwrap_or_default();

            let status = tx
                .effects
                .as_ref()
                .and_then(|e| e.status.as_ref())
                .map(|s| s.success.unwrap_or(false));

            let gas_used = tx.effects.as_ref().and_then(|e| {
                e.gas_used.as_ref().map(|g| {
                    serde_json::json!({
                        "computation_cost": g.computation_cost.unwrap_or(0),
                        "storage_cost": g.storage_cost.unwrap_or(0),
                        "storage_rebate": g.storage_rebate.unwrap_or(0),
                        "non_refundable_storage_fee": g.non_refundable_storage_fee.unwrap_or(0),
                    })
                })
            });

            serde_json::json!({
                "digest": digest,
                "success": status,
                "gas_used": gas_used,
            })
        })
        .collect();

    let result = serde_json::json!({
        "sequence_number": sequence_number,
        "summary": summary_json,
        "transaction_count": transactions.len(),
        "transactions": transactions,
    });

    Ok(result)
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
