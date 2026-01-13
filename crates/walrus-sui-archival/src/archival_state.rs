// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{path::Path, sync::Arc, time::Duration};

use anyhow::Result;
use bincode::Options;
use blob_bundle::BlobBundleReader;
use bytes::Bytes;
use prost::Message;
use rocksdb::{ColumnFamilyDescriptor, DB, Options as RocksOptions};
use sui_types::{base_types::ObjectID, messages_checkpoint::CheckpointSequenceNumber};
use tokio::task::JoinHandle;
use walrus_core::{BlobId, Epoch, encoding::Primary};
use walrus_sdk::{SuiReadClient, client::WalrusNodeClient};

use crate::postgres::{NewCheckpointBlobInfo, NewCheckpointIndexEntry, SharedPostgresPool};

// Include the generated protobuf code.
pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/archival.rs"));
}

use proto::{CheckpointBlobInfo, IndexEntry};

/// Column family names.
pub const CF_CHECKPOINT_BLOB_INFO: &str = "checkpoint_blob_info";

/// Current version of the CheckpointBlobInfo format.
const CHECKPOINT_BLOB_INFO_VERSION: u32 = 1;

/// Archival state that manages the RocksDB database and optional PostgreSQL.
pub struct ArchivalState {
    db: Arc<DB>,
    read_only: bool,
    walrus_read_client: Option<Arc<WalrusNodeClient<SuiReadClient>>>,
    /// Optional PostgreSQL connection pool for distributed metadata storage.
    postgres_pool: Option<SharedPostgresPool>,
}

impl ArchivalState {
    /// Create a new ArchivalState with RocksDB database.
    pub fn open<P: AsRef<Path>>(db_path: P, read_only: bool) -> Result<Self> {
        let db_path = db_path.as_ref();
        tracing::info!(
            "opening archival database at {} (read_only: {})",
            db_path.display(),
            read_only
        );

        // Define column families.
        let cf_descriptors = vec![ColumnFamilyDescriptor::new(
            CF_CHECKPOINT_BLOB_INFO,
            RocksOptions::default(),
        )];

        let db = if read_only {
            // Open database in read-only mode.
            DB::open_cf_descriptors_read_only(
                &RocksOptions::default(),
                db_path,
                cf_descriptors,
                false, // error_if_log_file_exist
            )?
        } else {
            // Create database options for read-write mode.
            let mut db_opts = RocksOptions::default();
            db_opts.create_if_missing(true);
            db_opts.create_missing_column_families(true);

            // Open database with column families.
            DB::open_cf_descriptors(&db_opts, db_path, cf_descriptors)?
        };

        Ok(Self {
            db: Arc::new(db),
            read_only,
            walrus_read_client: None,
            postgres_pool: None,
        })
    }

    /// Set the Walrus read client for fetching blob data.
    pub fn set_walrus_read_client(&mut self, client: Arc<WalrusNodeClient<SuiReadClient>>) {
        self.walrus_read_client = Some(client);
    }

    /// Set the PostgreSQL connection pool for distributed metadata storage.
    pub fn set_postgres_pool(&mut self, pool: SharedPostgresPool) {
        self.postgres_pool = Some(pool);
    }

    /// Get a reference to the PostgreSQL pool if configured.
    pub fn postgres_pool(&self) -> Option<&SharedPostgresPool> {
        self.postgres_pool.as_ref()
    }

    /// Write checkpoint blob info to PostgreSQL if configured.
    /// This is a fire-and-forget operation - errors are logged but don't fail the main operation.
    #[allow(clippy::too_many_arguments)]
    fn write_to_postgres(
        &self,
        start_checkpoint: CheckpointSequenceNumber,
        end_checkpoint: CheckpointSequenceNumber,
        blob_id: &BlobId,
        object_id: &ObjectID,
        blob_expiration_epoch: Epoch,
        end_of_epoch: bool,
        is_shared_blob: bool,
        index_entries: &[IndexEntry],
    ) {
        if let Some(pg_pool) = &self.postgres_pool {
            let blob_id_str = blob_id.to_string();
            let object_id_str = object_id.to_string();

            // Calculate blob_size as sum of all length_bytes from index entries
            let blob_size: u64 = index_entries.iter().map(|e| e.length).sum();

            let pg_blob_info = NewCheckpointBlobInfo::from_proto(
                start_checkpoint,
                end_checkpoint,
                &blob_id_str,
                &object_id_str,
                end_of_epoch,
                blob_expiration_epoch,
                is_shared_blob,
                CHECKPOINT_BLOB_INFO_VERSION,
                Some(blob_size),
            );

            let pg_index_entries: Vec<NewCheckpointIndexEntry> = index_entries
                .iter()
                .map(|entry| {
                    NewCheckpointIndexEntry::new(
                        entry.checkpoint_number,
                        start_checkpoint,
                        entry.offset,
                        entry.length,
                    )
                })
                .collect();

            let pool = pg_pool.clone();
            // Spawn a task to write to PostgreSQL asynchronously.
            // This ensures RocksDB writes are not blocked by PostgreSQL operations.
            tokio::spawn(async move {
                if let Err(e) = pool
                    .insert_checkpoint_blob_info(pg_blob_info, pg_index_entries)
                    .await
                {
                    tracing::error!(
                        "failed to write checkpoint blob info to PostgreSQL (start={}): {}",
                        start_checkpoint,
                        e
                    );
                } else {
                    tracing::debug!(
                        "wrote checkpoint blob info to PostgreSQL: start={}, end={}",
                        start_checkpoint,
                        end_checkpoint
                    );
                }
            });
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create_new_checkpoint_blob(
        &self,
        start_checkpoint: CheckpointSequenceNumber,
        end_checkpoint: CheckpointSequenceNumber,
        index_map: &[(String, (u64, u64))],
        blob_id: BlobId,
        object_id: ObjectID,
        blob_expiration_epoch: Epoch,
        end_of_epoch: bool,
        is_shared_blob: bool,
    ) -> Result<()> {
        if self.read_only {
            return Err(anyhow::anyhow!(
                "cannot create checkpoint blob in read-only mode"
            ));
        }
        // Convert index_map to protobuf IndexEntry messages.
        let index_entries: Vec<IndexEntry> = index_map
            .iter()
            .map(|(id, (offset, length))| {
                let checkpoint_number = id.parse::<u64>().unwrap_or_else(|_| {
                    // Try to parse without the .chk extension if present.
                    id.trim_end_matches(".chk").parse::<u64>().unwrap_or(0)
                });
                IndexEntry {
                    checkpoint_number,
                    offset: *offset,
                    length: *length,
                }
            })
            .collect();

        // Create the protobuf message.
        let blob_info = CheckpointBlobInfo {
            version: CHECKPOINT_BLOB_INFO_VERSION,
            blob_id: blob_id.to_string().into_bytes(),
            object_id: object_id.to_vec(),
            start_checkpoint,
            end_checkpoint,
            end_of_epoch,
            blob_expiration_epoch,
            index_entries: index_entries.clone(),
            is_shared_blob,
        };

        // Serialize to protobuf bytes.
        let blob_info_bytes = blob_info.encode_to_vec();

        // Store in RocksDB with start_checkpoint as key.
        self.db.put_cf(
            &self
                .db
                .cf_handle(CF_CHECKPOINT_BLOB_INFO)
                .expect("column family must exist"),
            be_fix_int_ser(&start_checkpoint)?,
            blob_info_bytes,
        )?;

        // Also write to PostgreSQL if configured (fire-and-forget).
        self.write_to_postgres(
            start_checkpoint,
            end_checkpoint,
            &blob_id,
            &object_id,
            blob_expiration_epoch,
            end_of_epoch,
            is_shared_blob,
            &index_entries,
        );

        Ok(())
    }

    /// Get checkpoint blob info that contains the given checkpoint number.
    ///
    /// This function searches for a blob where start_checkpoint <= checkpoint <= end_checkpoint.
    /// Returns an error if no blob contains the requested checkpoint.
    pub async fn get_checkpoint_blob_info(
        &self,
        checkpoint: CheckpointSequenceNumber,
    ) -> Result<CheckpointBlobInfo> {
        // First, find the blob_info synchronously without holding cf across await.
        let (start_checkpoint, mut blob_info) = {
            let cf = self
                .db
                .cf_handle(CF_CHECKPOINT_BLOB_INFO)
                .expect("column family must exist");

            // Create an iterator and seek to the position at or before the target checkpoint.
            let seek_key = be_fix_int_ser(&checkpoint)?;
            let iter = self.db.iterator_cf(
                &cf,
                rocksdb::IteratorMode::From(&seek_key, rocksdb::Direction::Reverse),
            );

            let mut found: Option<(CheckpointSequenceNumber, CheckpointBlobInfo)> = None;

            // Iterate backwards to find a blob that might contain this checkpoint.
            for item in iter {
                let (key_bytes, value_bytes) = item?;

                // Deserialize the key to get the start checkpoint.
                let start_checkpoint: CheckpointSequenceNumber = be_fix_int_deser(&key_bytes)?;

                // Deserialize the blob info.
                let blob_info = CheckpointBlobInfo::decode(value_bytes.as_ref())?;

                // Check if this blob contains our checkpoint.
                // The blob contains checkpoints from start_checkpoint to end_checkpoint (inclusive).
                if start_checkpoint <= checkpoint && checkpoint <= blob_info.end_checkpoint {
                    found = Some((start_checkpoint, blob_info));
                    break;
                }

                // If we've gone past where the checkpoint could be, stop searching.
                if blob_info.end_checkpoint < checkpoint {
                    break;
                }
            }

            found.ok_or_else(|| {
                anyhow::anyhow!("no blob found containing checkpoint {}", checkpoint)
            })?
        };

        // Now handle async operations outside the scope where cf was held.
        // If index_entries is empty, fetch from Walrus blob.
        if blob_info.index_entries.is_empty() {
            tracing::info!(
                "index entries empty for checkpoint {}, fetching from walrus blob",
                checkpoint
            );
            blob_info = self
                .fetch_and_populate_index_entries(start_checkpoint, blob_info)
                .await?;
        }

        Ok(blob_info)
    }

    /// Fetch blob from Walrus and populate index entries.
    async fn fetch_and_populate_index_entries(
        &self,
        start_checkpoint: CheckpointSequenceNumber,
        mut blob_info: CheckpointBlobInfo,
    ) -> Result<CheckpointBlobInfo> {
        if self.walrus_read_client.is_none() {
            tracing::info!("walrus read client not set, cannot fetch blob index");
            return Ok(blob_info);
        }

        let walrus_client = self
            .walrus_read_client
            .as_ref()
            .expect("walrus read client must be set");

        let blob_id_str = String::from_utf8_lossy(&blob_info.blob_id).to_string();
        let blob_id: walrus_core::BlobId = blob_id_str
            .parse()
            .map_err(|e| anyhow::anyhow!("failed to parse blob ID: {}", e))?;

        tracing::info!(
            "fetching blob {} from walrus to extract index entries",
            blob_id
        );

        // Download the blob from Walrus.
        let blob_data = walrus_client.read_blob::<Primary>(&blob_id).await?;

        tracing::info!(
            "downloaded {} bytes for blob {}, parsing blob bundle",
            blob_data.len(),
            blob_id
        );

        // Parse the blob as a BlobBundle.
        let blob_bundle = BlobBundleReader::new(Bytes::from(blob_data))?;

        // Convert blob bundle index to CheckpointBlobInfo index entries.
        let index_entries: Vec<IndexEntry> = blob_bundle
            .entries()?
            .iter()
            .map(|(checkpoint_id, entry)| {
                let checkpoint_number = checkpoint_id
                    .parse::<u64>()
                    .expect("invalid checkpoint number");
                IndexEntry {
                    checkpoint_number,
                    offset: entry.offset,
                    length: entry.length,
                }
            })
            .collect();

        blob_info.index_entries = index_entries;

        tracing::info!(
            "populated {} index entries for blob {}",
            blob_info.index_entries.len(),
            blob_id
        );

        // Write the updated blob_info back to the database.
        if !self.read_only {
            let cf = self
                .db
                .cf_handle(CF_CHECKPOINT_BLOB_INFO)
                .expect("column family must exist");

            let key = be_fix_int_ser(&start_checkpoint)?;
            let value = blob_info.encode_to_vec();
            self.db.put_cf(&cf, key, value)?;

            tracing::info!(
                "updated database with populated index entries for checkpoint {}",
                start_checkpoint
            );
        } else {
            tracing::warn!(
                "database is read-only, skipping index entries persistence for checkpoint {}",
                start_checkpoint
            );
        }

        Ok(blob_info)
    }

    /// Get the latest stored checkpoint number.
    ///
    /// This function finds the last CheckpointBlobInfo and returns the checkpoint number
    /// of its last index entry. Returns None if no checkpoints are stored.
    pub fn get_latest_stored_checkpoint(&self) -> Result<Option<CheckpointSequenceNumber>> {
        let cf = self
            .db
            .cf_handle(CF_CHECKPOINT_BLOB_INFO)
            .expect("column family must exist");

        // Create an iterator starting from the end (reverse order).
        let iter = self.db.iterator_cf(&cf, rocksdb::IteratorMode::End);

        // Get the last (highest) entry.
        if let Some(item) = iter.into_iter().next() {
            let (_key_bytes, value_bytes) = item?;

            // Deserialize the blob info.
            let blob_info = CheckpointBlobInfo::decode(value_bytes.as_ref())?;

            // Return the last checkpoint number in the blob.
            Ok(Some(blob_info.end_checkpoint))
        } else {
            // No blobs stored yet.
            Ok(None)
        }
    }

    /// Count the number of checkpoint blob records in the database.
    pub fn count_checkpoint_blobs(&self) -> Result<usize> {
        let cf = self
            .db
            .cf_handle(CF_CHECKPOINT_BLOB_INFO)
            .expect("column family must exist");

        let iter = self.db.iterator_cf(&cf, rocksdb::IteratorMode::Start);
        let count = iter.count();

        Ok(count)
    }

    /// List all checkpoint blobs in the database.
    /// Returns blobs in decreasing order of start_checkpoint (newest first).
    /// If `omit_index_entries` is true, the index entries will be omitted from the blob info.
    /// This is useful when we only need to list the blobs and not the index entries (which can be
    /// large).
    pub fn list_all_blobs(
        &self,
        reverse: bool,
        omit_index_entries: bool,
    ) -> Result<Vec<CheckpointBlobInfo>> {
        tracing::info!(
            "listing all checkpoint blobs in the databas, reverse: {}, omit_index_entries: {}",
            reverse,
            omit_index_entries
        );
        let cf = self
            .db
            .cf_handle(CF_CHECKPOINT_BLOB_INFO)
            .expect("column family must exist");

        // Create an iterator in reverse order (from end to start).
        let iter = self.db.iterator_cf(
            &cf,
            if reverse {
                rocksdb::IteratorMode::End
            } else {
                rocksdb::IteratorMode::Start
            },
        );

        let mut blobs = Vec::new();
        for item in iter {
            let (_key_bytes, value_bytes) = item?;
            let mut blob_info = CheckpointBlobInfo::decode(value_bytes.as_ref())?;
            if omit_index_entries {
                blob_info.index_entries.clear();
            }
            blobs.push(blob_info);
        }

        tracing::info!("listed {} checkpoint blobs in the database", blobs.len());

        Ok(blobs)
    }

    /// Populate the database from a list of checkpoint blob info records.
    /// This is typically used to restore from a metadata blob snapshot.
    pub fn populate_from_checkpoint_blob_infos(
        &self,
        blob_infos: Vec<CheckpointBlobInfo>,
    ) -> Result<()> {
        if self.read_only {
            return Err(anyhow::anyhow!(
                "cannot populate database in read-only mode"
            ));
        }

        tracing::info!(
            "populating database with {} checkpoint blob records",
            blob_infos.len()
        );

        let cf = self
            .db
            .cf_handle(CF_CHECKPOINT_BLOB_INFO)
            .expect("column family must exist");

        for blob_info in blob_infos {
            let key = be_fix_int_ser(&blob_info.start_checkpoint)?;
            let value = blob_info.encode_to_vec();
            self.db.put_cf(&cf, key, value)?;
        }

        tracing::info!("database population completed successfully");
        Ok(())
    }

    /// Get the database handle for direct access.
    pub fn get_db(&self) -> &Arc<DB> {
        &self.db
    }

    /// Update the expiration epoch of an existing blob.
    pub fn update_blob_expiration_epoch(
        &self,
        start_checkpoint: CheckpointSequenceNumber,
        blob_id: &BlobId,
        object_id: &ObjectID,
        new_expiration_epoch: Epoch,
    ) -> Result<()> {
        if self.read_only {
            return Err(anyhow::anyhow!(
                "cannot update blob expiration epoch in read-only mode"
            ));
        }

        let cf = self
            .db
            .cf_handle(CF_CHECKPOINT_BLOB_INFO)
            .expect("column family must exist");

        // Use start_checkpoint as key for point read.
        let key = be_fix_int_ser(&start_checkpoint)?;

        // Get the blob info for this start_checkpoint.
        let value = self.db.get_cf(&cf, &key)?.ok_or_else(|| {
            anyhow::anyhow!("no blob found with start_checkpoint {}", start_checkpoint)
        })?;

        let mut blob_info = CheckpointBlobInfo::decode(value.as_ref())?;

        // Verify blob_id matches.
        let stored_blob_id_str = String::from_utf8_lossy(&blob_info.blob_id);
        if stored_blob_id_str != blob_id.to_string() {
            return Err(anyhow::anyhow!(
                "blob_id mismatch: expected {}, found {}",
                blob_id,
                stored_blob_id_str
            ));
        }

        // Verify object_id matches.
        let stored_object_id = ObjectID::from_bytes(&blob_info.object_id)
            .map_err(|e| anyhow::anyhow!("failed to parse object ID: {}", e))?;
        if stored_object_id != *object_id {
            return Err(anyhow::anyhow!(
                "object_id mismatch: expected {}, found {}",
                object_id,
                stored_object_id
            ));
        }

        // Update the expiration epoch.
        let old_epoch = blob_info.blob_expiration_epoch;
        blob_info.blob_expiration_epoch = new_expiration_epoch;

        // Serialize and store the updated blob info.
        let updated_blob_info_bytes = blob_info.encode_to_vec();
        self.db.put_cf(&cf, key, updated_blob_info_bytes)?;

        tracing::info!(
            "updated blob expiration epoch for start_checkpoint {} (object {}) from epoch {} to epoch {}",
            start_checkpoint,
            object_id,
            old_epoch,
            new_expiration_epoch
        );

        Ok(())
    }

    /// Remove all checkpoint blob entries with start_checkpoint >= the specified checkpoint.
    /// Returns the number of entries removed.
    pub fn remove_entries_from_checkpoint(
        &self,
        from_checkpoint: CheckpointSequenceNumber,
    ) -> Result<usize> {
        if self.read_only {
            return Err(anyhow::anyhow!("cannot remove entries in read-only mode"));
        }

        tracing::info!(
            "removing all checkpoint blob entries with start_checkpoint >= {}",
            from_checkpoint
        );

        let cf = self
            .db
            .cf_handle(CF_CHECKPOINT_BLOB_INFO)
            .expect("column family must exist");

        // Create an iterator starting from the target checkpoint.
        let seek_key = be_fix_int_ser(&from_checkpoint)?;
        let iter = self.db.iterator_cf(
            &cf,
            rocksdb::IteratorMode::From(&seek_key, rocksdb::Direction::Forward),
        );

        // Collect all keys to delete.
        let mut keys_to_delete = Vec::new();
        for item in iter {
            let (key_bytes, _value_bytes) = item?;
            keys_to_delete.push(key_bytes.to_vec());
        }

        let num_entries = keys_to_delete.len();

        // Delete all collected keys.
        for key in keys_to_delete {
            self.db.delete_cf(&cf, key)?;
        }

        tracing::info!(
            "removed {} checkpoint blob entries with start_checkpoint >= {}",
            num_entries,
            from_checkpoint
        );

        Ok(num_entries)
    }

    /// Check the consistency of the checkpoint blob database by ensuring there are no gaps.
    /// Logs warnings if any gaps are detected in the checkpoint sequence.
    pub fn check_consistency(&self) -> Result<()> {
        tracing::info!("starting database consistency check");

        let cf = self
            .db
            .cf_handle(CF_CHECKPOINT_BLOB_INFO)
            .expect("column family must exist");

        // Create an iterator in forward order (from start to end).
        let iter = self.db.iterator_cf(&cf, rocksdb::IteratorMode::Start);

        let mut blobs = Vec::new();
        for item in iter {
            let (_key_bytes, value_bytes) = item?;
            let blob_info = CheckpointBlobInfo::decode(value_bytes.as_ref())?;
            blobs.push(blob_info);
        }

        if blobs.is_empty() {
            tracing::info!("no blobs found in database, consistency check passed");
            return Ok(());
        }

        tracing::info!("checking consistency for {} blobs", blobs.len());

        let mut has_gaps = false;
        let mut expected_next_checkpoint = None;

        for blob in &blobs {
            if let Some(expected) = expected_next_checkpoint
                && blob.start_checkpoint != expected
            {
                tracing::warn!(
                    "gap detected: expected start_checkpoint {}, found {}",
                    expected,
                    blob.start_checkpoint
                );
                has_gaps = true;
            }

            expected_next_checkpoint = Some(blob.end_checkpoint + 1);
        }

        if has_gaps {
            tracing::warn!("database consistency check completed with gaps detected");
        } else {
            tracing::info!("database consistency check passed, no gaps found");
        }

        Ok(())
    }

    /// Start a background task that periodically checks database consistency.
    /// This task runs once per hour and logs any gaps found in the checkpoint sequence.
    pub fn start_consistency_checker(self: Arc<Self>) -> JoinHandle<Result<()>> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(3600)); // 1 hour.
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                interval.tick().await;

                tracing::info!("running periodic consistency check");

                if let Err(e) = self.check_consistency() {
                    tracing::error!("consistency check failed: {}", e);
                    return Err(e);
                }
            }
        })
    }
}

#[inline]
pub fn be_fix_int_ser<S>(t: &S) -> Result<Vec<u8>>
where
    S: ?Sized + serde::Serialize,
{
    bincode::DefaultOptions::new()
        .with_big_endian()
        .with_fixint_encoding()
        .serialize(t)
        .map_err(|e| anyhow::anyhow!("failed to serialize: {}", e))
}

#[inline]
pub fn be_fix_int_deser<'a, T>(bytes: &'a [u8]) -> Result<T>
where
    T: serde::Deserialize<'a>,
{
    bincode::DefaultOptions::new()
        .with_big_endian()
        .with_fixint_encoding()
        .deserialize(bytes)
        .map_err(|e| anyhow::anyhow!("failed to deserialize: {}", e))
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use tempfile::TempDir;

    use super::*;

    #[allow(clippy::type_complexity)]
    fn create_test_blob_info(
        start: u64,
        end: u64,
        blob_id: &str,
    ) -> (Vec<(String, (u64, u64))>, BlobId, Epoch) {
        // Create index map with checkpoint entries.
        let mut index_map = Vec::new();
        let mut offset = 0u64;

        for checkpoint_num in start..=end {
            let id = format!("{}", checkpoint_num);
            let length = 1000 + (checkpoint_num % 100) * 10; // Vary the size.
            index_map.push((id, (offset, length)));
            offset += length;
        }

        let blob_id = BlobId::from_str(blob_id).unwrap_or(BlobId::ZERO);
        let epoch = 1000u32;

        (index_map, blob_id, epoch)
    }

    #[test]
    fn test_archival_state_open() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let db_path = temp_dir.path().join("test_db");

        // Test opening a new database.
        let state = ArchivalState::open(&db_path, false);
        assert!(state.is_ok(), "Should successfully open database");

        // Drop the first instance before opening again.
        drop(state);

        // Test opening an existing database.
        let state2 = ArchivalState::open(&db_path, false);
        assert!(state2.is_ok(), "Should successfully open existing database");
    }

    #[tokio::test]
    async fn test_create_and_get_checkpoint_blob() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let db_path = temp_dir.path().join("test_db");
        let state = ArchivalState::open(&db_path, false).expect("Failed to open database");

        // Create a blob with checkpoints 100-199.
        let start_checkpoint = 100u64;
        let end_checkpoint = 199u64;
        let (index_map, blob_id, epoch) = create_test_blob_info(100, 199, "test_blob_1");

        // Store the blob info.
        let result = state.create_new_checkpoint_blob(
            start_checkpoint,
            end_checkpoint,
            &index_map,
            blob_id,
            ObjectID::random(),
            epoch,
            false,
            false,
        );
        assert!(result.is_ok(), "Should successfully create checkpoint blob");

        // Test getting checkpoint blob by various checkpoint numbers.

        // Test with start checkpoint.
        let blob_info = state.get_checkpoint_blob_info(100u64).await;
        assert!(blob_info.is_ok(), "Should find blob for checkpoint 100");
        let blob_info = blob_info.unwrap();
        assert_eq!(blob_info.start_checkpoint, 100);
        assert_eq!(blob_info.end_checkpoint, 199);
        assert!(!blob_info.end_of_epoch);
        assert_eq!(blob_info.index_entries.len(), 100);

        // Test with middle checkpoint.
        let blob_info = state.get_checkpoint_blob_info(150u64).await;
        assert!(blob_info.is_ok(), "Should find blob for checkpoint 150");
        let blob_info = blob_info.unwrap();
        assert_eq!(blob_info.start_checkpoint, 100);
        assert_eq!(blob_info.end_checkpoint, 199);

        // Test with end checkpoint.
        let blob_info = state.get_checkpoint_blob_info(199u64).await;
        assert!(blob_info.is_ok(), "Should find blob for checkpoint 199");
        let blob_info = blob_info.unwrap();
        assert_eq!(blob_info.start_checkpoint, 100);
        assert_eq!(blob_info.end_checkpoint, 199);

        // Test with checkpoint before range.
        let blob_info = state.get_checkpoint_blob_info(99u64).await;
        assert!(blob_info.is_err(), "Should not find blob for checkpoint 99");

        // Test with checkpoint after range.
        let blob_info = state.get_checkpoint_blob_info(200u64).await;
        assert!(
            blob_info.is_err(),
            "Should not find blob for checkpoint 200"
        );
    }

    #[tokio::test]
    async fn test_multiple_blobs() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let db_path = temp_dir.path().join("test_db");
        let state = ArchivalState::open(&db_path, false).expect("Failed to open database");

        // Create multiple blobs.
        let blobs = vec![
            (100u64, 199u64, "blob_1", false),
            (200u64, 299u64, "blob_2", false),
            (300u64, 399u64, "blob_3", true), // End of epoch.
            (400u64, 499u64, "blob_4", false),
        ];

        for (start, end, blob_id, end_of_epoch) in &blobs {
            let (index_map, blob_id, epoch) = create_test_blob_info(*start, *end, blob_id);
            state
                .create_new_checkpoint_blob(
                    *start,
                    *end,
                    &index_map,
                    blob_id,
                    ObjectID::random(),
                    epoch,
                    *end_of_epoch,
                    false,
                )
                .expect("Failed to create blob");
        }

        // Test finding blobs for various checkpoints.
        let test_cases = vec![
            (150u64, 100u64, 199u64),
            (250u64, 200u64, 299u64),
            (350u64, 300u64, 399u64),
            (450u64, 400u64, 499u64),
        ];

        for (checkpoint, expected_start, expected_end) in test_cases {
            let blob_info = state
                .get_checkpoint_blob_info(checkpoint)
                .await
                .unwrap_or_else(|_| panic!("Should find blob for checkpoint {}", checkpoint));
            assert_eq!(blob_info.start_checkpoint, expected_start);
            assert_eq!(blob_info.end_checkpoint, expected_end);
        }

        // Test end of epoch flag.
        let blob_info = state
            .get_checkpoint_blob_info(350u64)
            .await
            .expect("Should find blob");
        assert!(blob_info.end_of_epoch);
    }

    #[test]
    fn test_get_latest_stored_checkpoint() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let db_path = temp_dir.path().join("test_db");
        let state = ArchivalState::open(&db_path, false).expect("Failed to open database");

        // Initially, no checkpoints stored.
        let latest = state
            .get_latest_stored_checkpoint()
            .expect("Should not error");
        assert_eq!(latest, None);

        // Add first blob.
        let (index_map, blob_id, epoch) = create_test_blob_info(100, 199, "blob_1");
        state
            .create_new_checkpoint_blob(
                100u64,
                199u64,
                &index_map,
                blob_id,
                ObjectID::random(),
                epoch,
                false,
                false,
            )
            .expect("Failed to create blob");

        let latest = state
            .get_latest_stored_checkpoint()
            .expect("Should not error");
        assert_eq!(latest, Some(199u64));

        // Add second blob with higher checkpoints.
        let (index_map, blob_id, epoch) = create_test_blob_info(200, 299, "blob_2");
        state
            .create_new_checkpoint_blob(
                200u64,
                299u64,
                &index_map,
                blob_id,
                ObjectID::random(),
                epoch,
                false,
                false,
            )
            .expect("Failed to create blob");

        let latest = state
            .get_latest_stored_checkpoint()
            .expect("Should not error");
        assert_eq!(latest, Some(299u64));

        // Add third blob.
        let (index_map, blob_id, epoch) = create_test_blob_info(300, 399, "blob_3");
        state
            .create_new_checkpoint_blob(
                300u64,
                399u64,
                &index_map,
                blob_id,
                ObjectID::random(),
                epoch,
                false,
                false,
            )
            .expect("Failed to create blob");

        let latest = state
            .get_latest_stored_checkpoint()
            .expect("Should not error");
        assert_eq!(latest, Some(399u64));
    }

    #[tokio::test]
    async fn test_blob_with_gaps() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let db_path = temp_dir.path().join("test_db");
        let state = ArchivalState::open(&db_path, false).expect("Failed to open database");

        // Create blobs with gaps.
        let (index_map, blob_id, epoch) = create_test_blob_info(100, 199, "blob_1");
        state
            .create_new_checkpoint_blob(
                100u64,
                199u64,
                &index_map,
                blob_id,
                ObjectID::random(),
                epoch,
                false,
                false,
            )
            .expect("Failed to create blob");

        // Gap from 200-299.

        let (index_map, blob_id, epoch) = create_test_blob_info(300, 399, "blob_2");
        state
            .create_new_checkpoint_blob(
                300u64,
                399u64,
                &index_map,
                blob_id,
                ObjectID::random(),
                epoch,
                false,
                false,
            )
            .expect("Failed to create blob");

        // Test checkpoints in the gap.
        let result = state.get_checkpoint_blob_info(250u64).await;
        assert!(
            result.is_err(),
            "Should not find blob for checkpoint in gap"
        );
        assert!(result.unwrap_err().to_string().contains("no blob found"));

        // Test checkpoints in stored ranges.
        assert!(state.get_checkpoint_blob_info(150u64).await.is_ok());
        assert!(state.get_checkpoint_blob_info(350u64).await.is_ok());
    }

    #[tokio::test]
    async fn test_empty_index_entries() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let db_path = temp_dir.path().join("test_db");
        let state = ArchivalState::open(&db_path, false).expect("Failed to open database");

        // Create a blob with empty index entries.
        let empty_index_map: Vec<(String, (u64, u64))> = Vec::new();
        let blob_id = BlobId::from_str("empty_blob").unwrap_or(BlobId::ZERO);
        let epoch = 1000u32;

        state
            .create_new_checkpoint_blob(
                500u64,
                599u64,
                &empty_index_map,
                blob_id,
                ObjectID::random(),
                epoch,
                false,
                false,
            )
            .expect("Failed to create blob");

        // Should still be able to find the blob.
        let blob_info = state
            .get_checkpoint_blob_info(550u64)
            .await
            .expect("Should find blob");
        assert_eq!(blob_info.start_checkpoint, 500);
        assert_eq!(blob_info.end_checkpoint, 599);
        assert_eq!(blob_info.index_entries.len(), 0);

        // get_latest_stored_checkpoint should use end_checkpoint as fallback.
        let latest = state
            .get_latest_stored_checkpoint()
            .expect("Should not error");
        assert_eq!(latest, Some(599u64));
    }

    #[tokio::test]
    async fn test_index_entry_parsing() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let db_path = temp_dir.path().join("test_db");
        let state = ArchivalState::open(&db_path, false).expect("Failed to open database");

        // Create index map with various ID formats.
        let index_map = vec![
            ("1000".to_string(), (0u64, 100u64)),
            ("1001.chk".to_string(), (100u64, 200u64)), // With .chk extension.
            ("1002".to_string(), (300u64, 150u64)),
        ];

        let blob_id = BlobId::from_str("test_blob").unwrap_or(BlobId::ZERO);
        let epoch = 1000u32;

        state
            .create_new_checkpoint_blob(
                1000u64,
                1002u64,
                &index_map,
                blob_id,
                ObjectID::random(),
                epoch,
                false,
                false,
            )
            .expect("Failed to create blob");

        let blob_info = state
            .get_checkpoint_blob_info(1001u64)
            .await
            .expect("Should find blob");

        // Check that checkpoint numbers were parsed correctly.
        assert_eq!(blob_info.index_entries[0].checkpoint_number, 1000);
        assert_eq!(blob_info.index_entries[1].checkpoint_number, 1001); // .chk stripped.
        assert_eq!(blob_info.index_entries[2].checkpoint_number, 1002);
    }

    #[tokio::test]
    async fn test_read_only_mode() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let db_path = temp_dir.path().join("test_db");

        // First create database in read-write mode and add some data.
        {
            let state = ArchivalState::open(&db_path, false).expect("Failed to open database");
            let index_map = vec![("1000".to_string(), (0u64, 100u64))];
            let blob_id = BlobId::from_str("test_blob").unwrap_or(BlobId::ZERO);
            let epoch = 1000u32;

            state
                .create_new_checkpoint_blob(
                    1000u64,
                    1000u64,
                    &index_map,
                    blob_id,
                    ObjectID::random(),
                    epoch,
                    false,
                    false,
                )
                .expect("Failed to create blob");
        }

        // Now open in read-only mode.
        let state =
            ArchivalState::open(&db_path, true).expect("Failed to open database in read-only mode");

        // Reading should work.
        let blob_info = state
            .get_checkpoint_blob_info(1000u64)
            .await
            .expect("Should be able to read in read-only mode");
        assert_eq!(blob_info.start_checkpoint, 1000);

        // Writing should fail.
        let index_map = vec![("2000".to_string(), (0u64, 100u64))];
        let blob_id = BlobId::from_str("test_blob2").unwrap_or(BlobId::ZERO);
        let epoch = 2000u32;

        let result = state.create_new_checkpoint_blob(
            2000u64,
            2000u64,
            &index_map,
            blob_id,
            ObjectID::random(),
            epoch,
            false,
            false,
        );

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("cannot create checkpoint blob in read-only mode")
        );
    }

    #[tokio::test]
    async fn test_update_blob_expiration_epoch() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let db_path = temp_dir.path().join("test_db");
        let state = ArchivalState::open(&db_path, false).expect("Failed to open database");

        // Create a blob first.
        let start_checkpoint = 100u64;
        let end_checkpoint = 199u64;
        let (index_map, blob_id, initial_epoch) = create_test_blob_info(100, 199, "test_blob_1");
        let object_id = ObjectID::random();

        // Store the blob info.
        state
            .create_new_checkpoint_blob(
                start_checkpoint,
                end_checkpoint,
                &index_map,
                blob_id,
                object_id,
                initial_epoch,
                false,
                false,
            )
            .expect("Failed to create blob");

        // Verify initial expiration epoch.
        let blob_info = state
            .get_checkpoint_blob_info(150u64)
            .await
            .expect("Should find blob");
        assert_eq!(blob_info.blob_expiration_epoch, initial_epoch);

        // Update the expiration epoch.
        let new_epoch = 2000u32;
        state
            .update_blob_expiration_epoch(start_checkpoint, &blob_id, &object_id, new_epoch)
            .expect("Should successfully update expiration epoch");

        // Verify the epoch was updated.
        let updated_blob_info = state
            .get_checkpoint_blob_info(150u64)
            .await
            .expect("Should find blob");
        assert_eq!(updated_blob_info.blob_expiration_epoch, new_epoch);

        // Verify other fields remain unchanged.
        assert_eq!(updated_blob_info.start_checkpoint, start_checkpoint);
        assert_eq!(updated_blob_info.end_checkpoint, end_checkpoint);
        assert_eq!(updated_blob_info.index_entries.len(), 100);
        assert!(!updated_blob_info.end_of_epoch);
    }

    #[test]
    fn test_update_blob_expiration_epoch_wrong_blob_id() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let db_path = temp_dir.path().join("test_db");
        let state = ArchivalState::open(&db_path, false).expect("Failed to open database");

        // Create a blob with BlobId::ZERO.
        let start_checkpoint = 100u64;
        let blob_id = BlobId::ZERO;
        let mut index_map = Vec::new();
        for checkpoint_num in 100..=199 {
            let id = format!("{}", checkpoint_num);
            index_map.push((id, ((checkpoint_num - 100) * 1000, 1000)));
        }
        let epoch = 1000u32;
        let object_id = ObjectID::random();

        state
            .create_new_checkpoint_blob(
                start_checkpoint,
                199u64,
                &index_map,
                blob_id,
                object_id,
                epoch,
                false,
                false,
            )
            .expect("Failed to create blob");

        // Try to update with wrong blob_id - create from different bytes.
        let wrong_blob_bytes = [1u8; 32];
        let wrong_blob_id = BlobId(wrong_blob_bytes);

        // Ensure they're actually different.
        assert_ne!(blob_id, wrong_blob_id, "Blob IDs should be different");

        let result =
            state.update_blob_expiration_epoch(start_checkpoint, &wrong_blob_id, &object_id, 2000);

        assert!(
            result.is_err(),
            "Expected error for wrong blob_id, but got Ok"
        );
        assert!(result.unwrap_err().to_string().contains("blob_id mismatch"));
    }

    #[test]
    fn test_update_blob_expiration_epoch_wrong_object_id() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let db_path = temp_dir.path().join("test_db");
        let state = ArchivalState::open(&db_path, false).expect("Failed to open database");

        // Create a blob.
        let start_checkpoint = 100u64;
        let (index_map, blob_id, epoch) = create_test_blob_info(100, 199, "test_blob_1");
        let object_id = ObjectID::random();

        state
            .create_new_checkpoint_blob(
                start_checkpoint,
                199u64,
                &index_map,
                blob_id,
                object_id,
                epoch,
                false,
                false,
            )
            .expect("Failed to create blob");

        // Try to update with wrong object_id.
        let wrong_object_id = ObjectID::random();
        let result =
            state.update_blob_expiration_epoch(start_checkpoint, &blob_id, &wrong_object_id, 2000);

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("object_id mismatch")
        );
    }

    #[test]
    fn test_update_blob_expiration_epoch_nonexistent_checkpoint() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let db_path = temp_dir.path().join("test_db");
        let state = ArchivalState::open(&db_path, false).expect("Failed to open database");

        // Try to update a non-existent blob.
        let blob_id = BlobId::from_str("test_blob").unwrap_or(BlobId::ZERO);
        let object_id = ObjectID::random();
        let result = state.update_blob_expiration_epoch(999u64, &blob_id, &object_id, 2000);

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("no blob found with start_checkpoint 999")
        );
    }

    #[test]
    fn test_update_blob_expiration_epoch_read_only_mode() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let db_path = temp_dir.path().join("test_db");

        // First create a blob in read-write mode.
        {
            let state = ArchivalState::open(&db_path, false).expect("Failed to open database");
            let (index_map, blob_id, epoch) = create_test_blob_info(100, 199, "test_blob");
            let object_id = ObjectID::random();

            state
                .create_new_checkpoint_blob(
                    100u64, 199u64, &index_map, blob_id, object_id, epoch, false, false,
                )
                .expect("Failed to create blob");
        }

        // Now open in read-only mode and try to update.
        let state =
            ArchivalState::open(&db_path, true).expect("Failed to open database in read-only mode");
        let blob_id = BlobId::from_str("test_blob").unwrap_or(BlobId::ZERO);
        let object_id = ObjectID::random();

        let result = state.update_blob_expiration_epoch(100u64, &blob_id, &object_id, 2000);

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("cannot update blob expiration epoch in read-only mode")
        );
    }

    #[tokio::test]
    async fn test_update_multiple_blobs_expiration_epochs() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let db_path = temp_dir.path().join("test_db");
        let state = ArchivalState::open(&db_path, false).expect("Failed to open database");

        // Create multiple blobs.
        let blobs = vec![
            (100u64, 199u64, "blob_1", ObjectID::random(), 1000u32),
            (200u64, 299u64, "blob_2", ObjectID::random(), 1100u32),
            (300u64, 399u64, "blob_3", ObjectID::random(), 1200u32),
        ];

        // Create all blobs.
        for (start, end, blob_id_str, object_id, epoch) in &blobs {
            let (index_map, blob_id, _) = create_test_blob_info(*start, *end, blob_id_str);
            state
                .create_new_checkpoint_blob(
                    *start, *end, &index_map, blob_id, *object_id, *epoch, false, false,
                )
                .expect("Failed to create blob");
        }

        // Update each blob's expiration epoch.
        for (start, _, blob_id_str, object_id, old_epoch) in &blobs {
            let blob_id = BlobId::from_str(blob_id_str).unwrap_or(BlobId::ZERO);
            let new_epoch = old_epoch + 500;

            state
                .update_blob_expiration_epoch(*start, &blob_id, object_id, new_epoch)
                .expect("Should successfully update expiration epoch");

            // Verify the update.
            let blob_info = state
                .get_checkpoint_blob_info(*start)
                .await
                .expect("Should find blob");
            assert_eq!(blob_info.blob_expiration_epoch, new_epoch);
        }

        // Verify all blobs have been updated correctly.
        let all_blobs = state
            .list_all_blobs(false, false)
            .expect("Should list all blobs");
        assert_eq!(all_blobs.len(), 3);

        for (i, blob_info) in all_blobs.iter().enumerate() {
            let expected_new_epoch = blobs[i].4 + 500;
            assert_eq!(blob_info.blob_expiration_epoch, expected_new_epoch);
        }
    }

    #[test]
    fn test_consistency_checker_no_gaps() {
        let _ = tracing_subscriber::fmt::try_init();

        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let db_path = temp_dir.path().join("test_db");
        let state = ArchivalState::open(&db_path, false).expect("Failed to open database");

        // Create continuous blobs without gaps.
        let blobs = vec![
            (0u64, 99u64, "blob_1"),
            (100u64, 199u64, "blob_2"),
            (200u64, 299u64, "blob_3"),
            (300u64, 399u64, "blob_4"),
        ];

        for (start, end, blob_id_str) in &blobs {
            let (index_map, blob_id, epoch) = create_test_blob_info(*start, *end, blob_id_str);
            state
                .create_new_checkpoint_blob(
                    *start,
                    *end,
                    &index_map,
                    blob_id,
                    ObjectID::random(),
                    epoch,
                    false,
                    false,
                )
                .expect("Failed to create blob");
        }

        // Run consistency check - should pass without gaps.
        let result = state.check_consistency();
        assert!(result.is_ok(), "Consistency check should pass");
    }

    #[test]
    fn test_consistency_checker_with_gaps() {
        let _ = tracing_subscriber::fmt::try_init();

        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let db_path = temp_dir.path().join("test_db");
        let state = ArchivalState::open(&db_path, false).expect("Failed to open database");

        // Create blobs with gaps.
        let blobs = vec![
            (0u64, 99u64, "blob_1"),
            (100u64, 199u64, "blob_2"),
            // Gap from 200-299.
            (300u64, 399u64, "blob_3"),
            // Gap from 400-499.
            (500u64, 599u64, "blob_4"),
        ];

        for (start, end, blob_id_str) in &blobs {
            let (index_map, blob_id, epoch) = create_test_blob_info(*start, *end, blob_id_str);
            state
                .create_new_checkpoint_blob(
                    *start,
                    *end,
                    &index_map,
                    blob_id,
                    ObjectID::random(),
                    epoch,
                    false,
                    false,
                )
                .expect("Failed to create blob");
        }

        // Run consistency check - should complete successfully but log warnings.
        let result = state.check_consistency();
        assert!(result.is_ok(), "Consistency check should complete");
        // Note: The test will log warnings about gaps, which is expected behavior.
    }

    #[test]
    fn test_consistency_checker_empty_database() {
        let _ = tracing_subscriber::fmt::try_init();

        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let db_path = temp_dir.path().join("test_db");
        let state = ArchivalState::open(&db_path, false).expect("Failed to open database");

        // Run consistency check on empty database.
        let result = state.check_consistency();
        assert!(
            result.is_ok(),
            "Consistency check should pass on empty database"
        );
    }

    #[test]
    fn test_remove_entries_from_checkpoint() {
        let _ = tracing_subscriber::fmt::try_init();

        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let db_path = temp_dir.path().join("test_db");
        let state = ArchivalState::open(&db_path, false).expect("Failed to open database");

        // Create multiple blobs.
        let blobs = vec![
            (0u64, 99u64, "blob_1"),
            (100u64, 199u64, "blob_2"),
            (200u64, 299u64, "blob_3"),
            (300u64, 399u64, "blob_4"),
            (400u64, 499u64, "blob_5"),
        ];

        for (start, end, blob_id_str) in &blobs {
            let (index_map, blob_id, epoch) = create_test_blob_info(*start, *end, blob_id_str);
            state
                .create_new_checkpoint_blob(
                    *start,
                    *end,
                    &index_map,
                    blob_id,
                    ObjectID::random(),
                    epoch,
                    false,
                    false,
                )
                .expect("Failed to create blob");
        }

        // Verify all 5 blobs exist.
        let count = state.count_checkpoint_blobs().expect("should count blobs");
        assert_eq!(count, 5);

        // Remove entries with start_checkpoint >= 200.
        let removed = state
            .remove_entries_from_checkpoint(200)
            .expect("should remove entries");
        assert_eq!(removed, 3); // blob_3, blob_4, blob_5.

        // Verify only 2 blobs remain.
        let count = state.count_checkpoint_blobs().expect("should count blobs");
        assert_eq!(count, 2);

        // Verify we can still find the remaining blobs.
        let all_blobs = state
            .list_all_blobs(true, false)
            .expect("should list blobs");
        assert_eq!(all_blobs.len(), 2);
        assert_eq!(all_blobs[0].start_checkpoint, 100);
        assert_eq!(all_blobs[1].start_checkpoint, 0);
    }

    #[test]
    fn test_remove_entries_from_checkpoint_boundary() {
        let _ = tracing_subscriber::fmt::try_init();

        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let db_path = temp_dir.path().join("test_db");
        let state = ArchivalState::open(&db_path, false).expect("Failed to open database");

        // Create blobs.
        let blobs = vec![
            (0u64, 99u64, "blob_1"),
            (100u64, 199u64, "blob_2"),
            (200u64, 299u64, "blob_3"),
        ];

        for (start, end, blob_id_str) in &blobs {
            let (index_map, blob_id, epoch) = create_test_blob_info(*start, *end, blob_id_str);
            state
                .create_new_checkpoint_blob(
                    *start,
                    *end,
                    &index_map,
                    blob_id,
                    ObjectID::random(),
                    epoch,
                    false,
                    false,
                )
                .expect("Failed to create blob");
        }

        // Remove from checkpoint 0 - should remove everything.
        let removed = state
            .remove_entries_from_checkpoint(0)
            .expect("should remove entries");
        assert_eq!(removed, 3);

        // Verify database is empty.
        let count = state.count_checkpoint_blobs().expect("should count blobs");
        assert_eq!(count, 0);
    }

    #[test]
    fn test_remove_entries_from_checkpoint_none_match() {
        let _ = tracing_subscriber::fmt::try_init();

        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let db_path = temp_dir.path().join("test_db");
        let state = ArchivalState::open(&db_path, false).expect("Failed to open database");

        // Create blobs.
        let blobs = vec![(0u64, 99u64, "blob_1"), (100u64, 199u64, "blob_2")];

        for (start, end, blob_id_str) in &blobs {
            let (index_map, blob_id, epoch) = create_test_blob_info(*start, *end, blob_id_str);
            state
                .create_new_checkpoint_blob(
                    *start,
                    *end,
                    &index_map,
                    blob_id,
                    ObjectID::random(),
                    epoch,
                    false,
                    false,
                )
                .expect("Failed to create blob");
        }

        // Remove from checkpoint 300 - should remove nothing.
        let removed = state
            .remove_entries_from_checkpoint(300)
            .expect("should remove entries");
        assert_eq!(removed, 0);

        // Verify all blobs still exist.
        let count = state.count_checkpoint_blobs().expect("should count blobs");
        assert_eq!(count, 2);
    }

    #[test]
    fn test_remove_entries_read_only_mode() {
        let _ = tracing_subscriber::fmt::try_init();

        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let db_path = temp_dir.path().join("test_db");

        // Create a blob in read-write mode.
        {
            let state = ArchivalState::open(&db_path, false).expect("Failed to open database");
            let (index_map, blob_id, epoch) = create_test_blob_info(0, 99, "blob_1");
            state
                .create_new_checkpoint_blob(
                    0,
                    99,
                    &index_map,
                    blob_id,
                    ObjectID::random(),
                    epoch,
                    false,
                    false,
                )
                .expect("Failed to create blob");
        }

        // Open in read-only mode and try to remove.
        let state =
            ArchivalState::open(&db_path, true).expect("Failed to open database in read-only mode");
        let result = state.remove_entries_from_checkpoint(0);

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("cannot remove entries in read-only mode")
        );
    }

    #[test]
    fn test_list_all_blobs_omit_index_entries() {
        tracing_subscriber::fmt::init();

        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let db_path = temp_dir.path().join("test_db");
        let state = ArchivalState::open(&db_path, false).expect("Failed to open database");

        // Create multiple blobs with different checkpoint ranges.
        let blobs = vec![
            (0u64, 99u64, "blob_1"),
            (100u64, 199u64, "blob_2"),
            (200u64, 299u64, "blob_3"),
        ];

        for (start, end, blob_id_str) in &blobs {
            let (index_map, blob_id, epoch) = create_test_blob_info(*start, *end, blob_id_str);
            state
                .create_new_checkpoint_blob(
                    *start,
                    *end,
                    &index_map,
                    blob_id,
                    ObjectID::random(),
                    epoch,
                    false,
                    false,
                )
                .expect("Failed to create blob");
        }

        // Test with omit_index_entries = false (should include index entries).
        let blobs_with_index = state
            .list_all_blobs(false, false)
            .expect("should list all blobs");
        assert_eq!(blobs_with_index.len(), 3);

        // Each blob should have 100 index entries (0-99, 100-199, 200-299).
        for blob_info in &blobs_with_index {
            assert_eq!(
                blob_info.index_entries.len(),
                100,
                "blob should have 100 index entries"
            );
        }

        // Test with omit_index_entries = true (should omit index entries).
        let blobs_without_index = state
            .list_all_blobs(false, true)
            .expect("should list all blobs");
        assert_eq!(blobs_without_index.len(), 3);

        // Each blob should have 0 index entries.
        for blob_info in &blobs_without_index {
            assert_eq!(
                blob_info.index_entries.len(),
                0,
                "blob should have 0 index entries when omitted"
            );
        }

        // Verify that other fields are still present when index entries are omitted.
        assert_eq!(blobs_without_index[0].start_checkpoint, 0);
        assert_eq!(blobs_without_index[0].end_checkpoint, 99);
        assert_eq!(blobs_without_index[1].start_checkpoint, 100);
        assert_eq!(blobs_without_index[1].end_checkpoint, 199);
        assert_eq!(blobs_without_index[2].start_checkpoint, 200);
        assert_eq!(blobs_without_index[2].end_checkpoint, 299);

        // Test with reverse = true and omit_index_entries = true.
        let blobs_reversed = state
            .list_all_blobs(true, true)
            .expect("should list all blobs in reverse");
        assert_eq!(blobs_reversed.len(), 3);

        // Verify reverse order.
        assert_eq!(blobs_reversed[0].start_checkpoint, 200);
        assert_eq!(blobs_reversed[1].start_checkpoint, 100);
        assert_eq!(blobs_reversed[2].start_checkpoint, 0);

        // All should have 0 index entries.
        for blob_info in &blobs_reversed {
            assert_eq!(blob_info.index_entries.len(), 0);
        }
    }
}
