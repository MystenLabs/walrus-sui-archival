// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use chrono::{DateTime, Utc};
use diesel::prelude::*;

use crate::schema::{checkpoint_blob_info, checkpoint_index_entry};

/// Queryable model for checkpoint_blob_info table.
#[derive(Debug, Clone, Queryable, Selectable, Identifiable)]
#[diesel(table_name = checkpoint_blob_info)]
#[diesel(primary_key(start_checkpoint))]
pub struct CheckpointBlobInfoRow {
    pub start_checkpoint: i64,
    pub end_checkpoint: i64,
    pub blob_id: String,
    pub object_id: String,
    pub end_of_epoch: bool,
    pub blob_expiration_epoch: i32,
    pub is_shared_blob: bool,
    pub version: i32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub blob_size: Option<i64>,
}

/// Insertable model for checkpoint_blob_info table.
#[derive(Debug, Clone, Insertable)]
#[diesel(table_name = checkpoint_blob_info)]
pub struct NewCheckpointBlobInfo {
    pub start_checkpoint: i64,
    pub end_checkpoint: i64,
    pub blob_id: String,
    pub object_id: String,
    pub end_of_epoch: bool,
    pub blob_expiration_epoch: i32,
    pub is_shared_blob: bool,
    pub version: i32,
    pub blob_size: Option<i64>,
}

/// AsChangeset model for updating checkpoint_blob_info.
#[derive(Debug, Clone, AsChangeset)]
#[diesel(table_name = checkpoint_blob_info)]
pub struct UpdateCheckpointBlobInfo {
    pub blob_expiration_epoch: Option<i32>,
}

/// Queryable model for checkpoint_index_entry table.
#[derive(Debug, Clone, Queryable, Selectable, Identifiable, Associations)]
#[diesel(table_name = checkpoint_index_entry)]
#[diesel(primary_key(checkpoint_number))]
#[diesel(belongs_to(CheckpointBlobInfoRow, foreign_key = start_checkpoint))]
pub struct CheckpointIndexEntryRow {
    pub checkpoint_number: i64,
    pub start_checkpoint: i64,
    pub offset_bytes: i64,
    pub length_bytes: i64,
}

/// Insertable model for checkpoint_index_entry table.
#[derive(Debug, Clone, Insertable)]
#[diesel(table_name = checkpoint_index_entry)]
pub struct NewCheckpointIndexEntry {
    pub checkpoint_number: i64,
    pub start_checkpoint: i64,
    pub offset_bytes: i64,
    pub length_bytes: i64,
}

impl NewCheckpointBlobInfo {
    /// Create a new checkpoint blob info record from protobuf data.
    #[allow(clippy::too_many_arguments)]
    pub fn from_proto(
        start_checkpoint: u64,
        end_checkpoint: u64,
        blob_id: &str,
        object_id: &str,
        end_of_epoch: bool,
        blob_expiration_epoch: u32,
        is_shared_blob: bool,
        version: u32,
        blob_size: Option<u64>,
    ) -> Self {
        Self {
            start_checkpoint: start_checkpoint as i64,
            end_checkpoint: end_checkpoint as i64,
            blob_id: blob_id.to_string(),
            object_id: object_id.to_string(),
            end_of_epoch,
            blob_expiration_epoch: blob_expiration_epoch as i32,
            is_shared_blob,
            version: version as i32,
            blob_size: blob_size.map(|s| s as i64),
        }
    }
}

impl NewCheckpointIndexEntry {
    /// Create a new index entry record.
    pub fn new(checkpoint_number: u64, start_checkpoint: u64, offset: u64, length: u64) -> Self {
        Self {
            checkpoint_number: checkpoint_number as i64,
            start_checkpoint: start_checkpoint as i64,
            offset_bytes: offset as i64,
            length_bytes: length as i64,
        }
    }
}
