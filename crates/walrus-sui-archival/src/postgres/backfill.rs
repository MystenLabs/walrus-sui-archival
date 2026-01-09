// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! One-time backfill tool for migrating checkpoint blob metadata from RocksDB to PostgreSQL.
//!
//! This module provides a CLI tool that reads all checkpoint blob info records
//! from RocksDB and writes them to PostgreSQL.

use std::path::Path;

use anyhow::Result;
use prost::Message;
use rocksdb::{ColumnFamilyDescriptor, DB, Options as RocksOptions};

use crate::{
    archival_state::{CF_CHECKPOINT_BLOB_INFO, be_fix_int_deser, proto::CheckpointBlobInfo},
    postgres::{NewCheckpointBlobInfo, NewCheckpointIndexEntry, PostgresPool},
};

/// Run the backfill process from RocksDB to PostgreSQL.
///
/// This function reads all checkpoint blob info records from RocksDB and writes them
/// to PostgreSQL. It's designed as a one-time migration tool.
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

    // Count total records first.
    let total_count = db.iterator_cf(&cf, rocksdb::IteratorMode::Start).count();
    tracing::info!("found {} total records in RocksDB", total_count);

    // Get the latest checkpoint in PostgreSQL to resume from.
    let pg_latest = postgres_pool.get_latest_stored_checkpoint().await?;
    let start_from = pg_latest.map(|c| (c + 1) as u64).unwrap_or(0);

    if let Some(latest) = pg_latest {
        tracing::info!(
            "PostgreSQL already has data up to checkpoint {}, resuming from {}",
            latest,
            start_from
        );
    } else {
        tracing::info!("PostgreSQL is empty, starting from beginning");
    }

    // Create iterator starting from the resume point.
    let seek_key = crate::archival_state::be_fix_int_ser(&start_from)?;
    let iter = db.iterator_cf(
        &cf,
        rocksdb::IteratorMode::From(&seek_key, rocksdb::Direction::Forward),
    );

    let mut processed_count = 0;
    let mut batch_records = Vec::new();

    for item in iter {
        let (key_bytes, value_bytes) = item?;

        let start_checkpoint: u64 = be_fix_int_deser(&key_bytes)?;
        let blob_info = CheckpointBlobInfo::decode(value_bytes.as_ref())?;

        // Convert to PostgreSQL models.
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
                    blob_info.start_checkpoint,
                    entry.checkpoint_number,
                    entry.offset,
                    entry.length,
                )
            })
            .collect();

        batch_records.push((pg_blob_info, pg_index_entries));
        processed_count += 1;

        // Process batch when it reaches the batch size.
        if batch_records.len() >= batch_size {
            for (blob_info, index_entries) in batch_records.drain(..) {
                postgres_pool
                    .insert_checkpoint_blob_info(blob_info, index_entries)
                    .await?;
            }

            tracing::info!(
                "processed {}/{} records (checkpoint {})",
                processed_count,
                total_count,
                start_checkpoint
            );
        }
    }

    // Process remaining records.
    for (blob_info, index_entries) in batch_records {
        postgres_pool
            .insert_checkpoint_blob_info(blob_info, index_entries)
            .await?;
    }

    tracing::info!(
        "backfill complete: processed {} records total",
        processed_count
    );

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
            "backfill verification warning: record counts don't match (RocksDB: {}, PostgreSQL: {})",
            total_count,
            pg_count
        );
    }

    Ok(())
}
