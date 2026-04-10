// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use anyhow::{Context, Result, anyhow};
use prost_014::Message as _;
use sui_rpc::proto::sui::rpc::v2::Checkpoint;
use sui_types::messages_checkpoint::CheckpointSummary;

use crate::checkpoint_downloader::CheckpointInfo;

/// Extract lightweight checkpoint metadata from zstd-compressed protobuf bytes.
///
/// The compressed bytes are the raw `.zst` file from the GCS bucket. We decompress,
/// decode the protobuf `Checkpoint` message, then BCS-decode the summary to get
/// epoch, timestamp, and end-of-epoch information.
pub fn extract_checkpoint_info(compressed: &[u8]) -> Result<CheckpointInfo> {
    // 1. Decompress zstd.
    let proto_bytes =
        zstd::decode_all(compressed).context("failed to zstd-decompress checkpoint")?;

    // 2. Decode protobuf Checkpoint message.
    let proto_checkpoint = Checkpoint::decode(proto_bytes.as_slice())
        .context("failed to decode protobuf checkpoint")?;

    let sequence_number = proto_checkpoint
        .sequence_number
        .ok_or_else(|| anyhow!("checkpoint missing sequence_number"))?;

    // 3. BCS-decode the summary to get epoch, timestamp, end_of_epoch_data.
    let summary = proto_checkpoint
        .summary
        .as_ref()
        .ok_or_else(|| anyhow!("checkpoint missing summary"))?;
    let bcs_field = summary
        .bcs
        .as_ref()
        .ok_or_else(|| anyhow!("checkpoint summary missing bcs"))?;
    let bcs_bytes = bcs_field
        .value
        .as_ref()
        .ok_or_else(|| anyhow!("checkpoint summary bcs missing value"))?;

    let checkpoint_summary: CheckpointSummary =
        bcs::from_bytes(bcs_bytes).context("failed to BCS-decode CheckpointSummary")?;

    Ok(CheckpointInfo {
        checkpoint_number: sequence_number,
        epoch: checkpoint_summary.epoch,
        is_end_of_epoch: checkpoint_summary.end_of_epoch_data.is_some(),
        timestamp_ms: checkpoint_summary.timestamp_ms,
        checkpoint_byte_size: compressed.len(),
    })
}
