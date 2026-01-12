// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! One-time backfill tool for migrating checkpoint blob metadata from RocksDB to PostgreSQL.
//!
//! This module provides a CLI tool that reads all checkpoint blob info records
//! from RocksDB and writes them to PostgreSQL.

use std::{collections::HashSet, path::Path};

use anyhow::Result;
use prost::Message;
use rocksdb::{ColumnFamilyDescriptor, DB, Options as RocksOptions};

use crate::{
    archival_state::{CF_CHECKPOINT_BLOB_INFO, be_fix_int_deser, proto::CheckpointBlobInfo},
    postgres::{NewCheckpointBlobInfo, NewCheckpointIndexEntry, PostgresPool},
};

/// Convert a RocksDB blob info to PostgreSQL models.
fn convert_to_pg_models(
    blob_info: &CheckpointBlobInfo,
) -> (NewCheckpointBlobInfo, Vec<NewCheckpointIndexEntry>) {
    let blob_id_str = String::from_utf8_lossy(&blob_info.blob_id).to_string();
    let object_id_bytes = &blob_info.object_id;
    let object_id_str = if object_id_bytes.len() == 32 {
        format!("0x{}", hex::encode(object_id_bytes))
    } else {
        hex::encode(object_id_bytes)
    };

    let pg_blob_info = NewCheckpointBlobInfo::from_proto(
        blob_info.start_checkpoint,
        blob_info.end_checkpoint,
        &blob_id_str,
        &object_id_str,
        blob_info.end_of_epoch,
        blob_info.blob_expiration_epoch,
        blob_info.is_shared_blob,
        blob_info.version,
    );

    let pg_index_entries: Vec<NewCheckpointIndexEntry> = blob_info
        .index_entries
        .iter()
        .map(|entry| {
            NewCheckpointIndexEntry::new(
                entry.checkpoint_number,
                blob_info.start_checkpoint,
                entry.offset,
                entry.length,
            )
        })
        .collect();

    (pg_blob_info, pg_index_entries)
}

/// Run the backfill process from RocksDB to PostgreSQL.
///
/// This function reads all checkpoint blob info records from RocksDB and writes them
/// to PostgreSQL in batches. It also verifies and reinserts any missing entries.
pub async fn run_backfill(db_path: impl AsRef<Path>, batch_size: usize) -> Result<()> {
    tracing::info!("starting RocksDB to PostgreSQL backfill");

    // Initialize PostgreSQL connection pool from DATABASE_URL.
    let postgres_pool = PostgresPool::from_env()?;

    // Run migrations first.
    postgres_pool.run_migrations().await?;

    // Open RocksDB in read-only mode.
    let db_path = db_path.as_ref();
    tracing::info!("opening RocksDB at {} in read-only mode", db_path.display());

    let cf_descriptors = vec![ColumnFamilyDescriptor::new(
        CF_CHECKPOINT_BLOB_INFO,
        RocksOptions::default(),
    )];

    let db = DB::open_cf_descriptors_read_only(
        &RocksOptions::default(),
        db_path,
        cf_descriptors,
        false,
    )?;

    let cf = db
        .cf_handle(CF_CHECKPOINT_BLOB_INFO)
        .expect("column family must exist");

    // Collect all start_checkpoints from RocksDB.
    tracing::info!("collecting all start_checkpoints from RocksDB...");
    let mut rocks_checkpoints: Vec<u64> = Vec::new();
    for item in db.iterator_cf(&cf, rocksdb::IteratorMode::Start) {
        let (key_bytes, _) = item?;
        let start_checkpoint: u64 = be_fix_int_deser(&key_bytes)?;
        rocks_checkpoints.push(start_checkpoint);
    }
    let total_count = rocks_checkpoints.len();
    tracing::info!("found {} total records in RocksDB", total_count);

    // Get existing checkpoints from PostgreSQL.
    let pg_checkpoints: HashSet<i64> = postgres_pool
        .get_all_start_checkpoints()
        .await?
        .into_iter()
        .collect();
    tracing::info!(
        "found {} existing records in PostgreSQL",
        pg_checkpoints.len()
    );

    // Find checkpoints that need to be inserted.
    let missing_checkpoints: Vec<u64> = rocks_checkpoints
        .iter()
        .filter(|&&c| !pg_checkpoints.contains(&(c as i64)))
        .copied()
        .collect();

    if missing_checkpoints.is_empty() {
        tracing::info!("all records already exist in PostgreSQL, nothing to backfill");
    } else {
        tracing::info!(
            "{} records need to be inserted into PostgreSQL",
            missing_checkpoints.len()
        );

        // Process missing checkpoints in batches.
        let mut processed_count = 0;
        for chunk in missing_checkpoints.chunks(batch_size) {
            let mut batch_records = Vec::with_capacity(chunk.len());

            for &start_checkpoint in chunk {
                // Read the blob info from RocksDB.
                let key = crate::archival_state::be_fix_int_ser(&start_checkpoint)?;
                if let Some(value) = db.get_cf(&cf, &key)? {
                    let blob_info = CheckpointBlobInfo::decode(value.as_ref())?;
                    let (pg_blob_info, pg_index_entries) = convert_to_pg_models(&blob_info);
                    batch_records.push((pg_blob_info, pg_index_entries));
                }
            }

            // Batch insert into PostgreSQL.
            postgres_pool
                .batch_insert_checkpoint_blob_info(batch_records)
                .await?;

            processed_count += chunk.len();
            tracing::info!(
                "processed {}/{} missing records",
                processed_count,
                missing_checkpoints.len()
            );
        }

        tracing::info!(
            "backfill complete: inserted {} records",
            missing_checkpoints.len()
        );
    }

    // Verify counts match.
    let pg_count = postgres_pool.count_checkpoint_blobs().await?;
    tracing::info!(
        "verification: RocksDB has {} records, PostgreSQL has {} records",
        total_count,
        pg_count
    );

    if pg_count as usize == total_count {
        tracing::info!("backfill verification passed: record counts match");
    } else {
        tracing::warn!(
            "backfill verification warning: record counts don't match (RocksDB: {}, PostgreSQL: {}). Run backfill again to insert missing entries.",
            total_count,
            pg_count
        );
    }

    Ok(())
}
