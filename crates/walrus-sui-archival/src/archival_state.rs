use std::{path::Path, sync::Arc};

use anyhow::Result;
use bincode::Options;
use prost::Message;
use rocksdb::{ColumnFamilyDescriptor, DB, Options as RocksOptions};
use sui_types::messages_checkpoint::CheckpointSequenceNumber;
use walrus_core::{BlobId, Epoch};

// Include the generated protobuf code.
pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/archival.rs"));
}

use proto::{CheckpointBlobInfo, IndexEntry};

/// Column family names.
const CF_CHECKPOINT_BLOB_INFO: &str = "checkpoint_blob_info";

/// Current version of the CheckpointBlobInfo format.
const CHECKPOINT_BLOB_INFO_VERSION: u32 = 1;

/// Archival state that manages the RocksDB database.
pub struct ArchivalState {
    db: Arc<DB>,
    read_only: bool,
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
        })
    }

    pub fn create_new_checkpoint_blob(
        &self,
        start_checkpoint: CheckpointSequenceNumber,
        end_checkpoint: CheckpointSequenceNumber,
        index_map: &[(String, (u64, u64))],
        blob_id: BlobId,
        blob_expiration_epoch: Epoch,
        end_of_epoch: bool,
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
            start_checkpoint,
            end_checkpoint,
            end_of_epoch,
            blob_expiration_epoch,
            index_entries,
        };

        // Serialize to protobuf bytes.
        let blob_info_bytes = blob_info.encode_to_vec();

        // Store in database with start_checkpoint as key.
        self.db.put_cf(
            &self
                .db
                .cf_handle(CF_CHECKPOINT_BLOB_INFO)
                .expect("column family must exist"),
            be_fix_int_ser(&start_checkpoint)?,
            blob_info_bytes,
        )?;

        Ok(())
    }

    /// Get checkpoint blob info that contains the given checkpoint number.
    ///
    /// This function searches for a blob where start_checkpoint <= checkpoint <= end_checkpoint.
    /// Returns an error if no blob contains the requested checkpoint.
    pub fn get_checkpoint_blob_info(
        &self,
        checkpoint: CheckpointSequenceNumber,
    ) -> Result<CheckpointBlobInfo> {
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
                return Ok(blob_info);
            }

            // If we've gone past where the checkpoint could be, stop searching.
            if blob_info.end_checkpoint < checkpoint {
                break;
            }
        }

        Err(anyhow::anyhow!(
            "no blob found containing checkpoint {}",
            checkpoint
        ))
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

    /// List all checkpoint blobs in the database.
    pub fn list_all_blobs(&self) -> Result<Vec<CheckpointBlobInfo>> {
        let cf = self
            .db
            .cf_handle(CF_CHECKPOINT_BLOB_INFO)
            .expect("column family must exist");

        // Create an iterator in forward order.
        let iter = self.db.iterator_cf(&cf, rocksdb::IteratorMode::Start);

        let mut blobs = Vec::new();
        for item in iter {
            let (_key_bytes, value_bytes) = item?;
            let blob_info = CheckpointBlobInfo::decode(value_bytes.as_ref())?;
            blobs.push(blob_info);
        }

        Ok(blobs)
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

    #[test]
    fn test_create_and_get_checkpoint_blob() {
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
            epoch,
            false,
        );
        assert!(result.is_ok(), "Should successfully create checkpoint blob");

        // Test getting checkpoint blob by various checkpoint numbers.

        // Test with start checkpoint.
        let blob_info = state.get_checkpoint_blob_info(100u64);
        assert!(blob_info.is_ok(), "Should find blob for checkpoint 100");
        let blob_info = blob_info.unwrap();
        assert_eq!(blob_info.start_checkpoint, 100);
        assert_eq!(blob_info.end_checkpoint, 199);
        assert!(!blob_info.end_of_epoch);
        assert_eq!(blob_info.index_entries.len(), 100);

        // Test with middle checkpoint.
        let blob_info = state.get_checkpoint_blob_info(150u64);
        assert!(blob_info.is_ok(), "Should find blob for checkpoint 150");
        let blob_info = blob_info.unwrap();
        assert_eq!(blob_info.start_checkpoint, 100);
        assert_eq!(blob_info.end_checkpoint, 199);

        // Test with end checkpoint.
        let blob_info = state.get_checkpoint_blob_info(199u64);
        assert!(blob_info.is_ok(), "Should find blob for checkpoint 199");
        let blob_info = blob_info.unwrap();
        assert_eq!(blob_info.start_checkpoint, 100);
        assert_eq!(blob_info.end_checkpoint, 199);

        // Test with checkpoint before range.
        let blob_info = state.get_checkpoint_blob_info(99u64);
        assert!(blob_info.is_err(), "Should not find blob for checkpoint 99");

        // Test with checkpoint after range.
        let blob_info = state.get_checkpoint_blob_info(200u64);
        assert!(
            blob_info.is_err(),
            "Should not find blob for checkpoint 200"
        );
    }

    #[test]
    fn test_multiple_blobs() {
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
                .create_new_checkpoint_blob(*start, *end, &index_map, blob_id, epoch, *end_of_epoch)
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
                .unwrap_or_else(|_| panic!("Should find blob for checkpoint {}", checkpoint));
            assert_eq!(blob_info.start_checkpoint, expected_start);
            assert_eq!(blob_info.end_checkpoint, expected_end);
        }

        // Test end of epoch flag.
        let blob_info = state
            .get_checkpoint_blob_info(350u64)
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
            .create_new_checkpoint_blob(100u64, 199u64, &index_map, blob_id, epoch, false)
            .expect("Failed to create blob");

        let latest = state
            .get_latest_stored_checkpoint()
            .expect("Should not error");
        assert_eq!(latest, Some(199u64));

        // Add second blob with higher checkpoints.
        let (index_map, blob_id, epoch) = create_test_blob_info(200, 299, "blob_2");
        state
            .create_new_checkpoint_blob(200u64, 299u64, &index_map, blob_id, epoch, false)
            .expect("Failed to create blob");

        let latest = state
            .get_latest_stored_checkpoint()
            .expect("Should not error");
        assert_eq!(latest, Some(299u64));

        // Add third blob.
        let (index_map, blob_id, epoch) = create_test_blob_info(300, 399, "blob_3");
        state
            .create_new_checkpoint_blob(300u64, 399u64, &index_map, blob_id, epoch, false)
            .expect("Failed to create blob");

        let latest = state
            .get_latest_stored_checkpoint()
            .expect("Should not error");
        assert_eq!(latest, Some(399u64));
    }

    #[test]
    fn test_blob_with_gaps() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let db_path = temp_dir.path().join("test_db");
        let state = ArchivalState::open(&db_path, false).expect("Failed to open database");

        // Create blobs with gaps.
        let (index_map, blob_id, epoch) = create_test_blob_info(100, 199, "blob_1");
        state
            .create_new_checkpoint_blob(100u64, 199u64, &index_map, blob_id, epoch, false)
            .expect("Failed to create blob");

        // Gap from 200-299.

        let (index_map, blob_id, epoch) = create_test_blob_info(300, 399, "blob_2");
        state
            .create_new_checkpoint_blob(300u64, 399u64, &index_map, blob_id, epoch, false)
            .expect("Failed to create blob");

        // Test checkpoints in the gap.
        let result = state.get_checkpoint_blob_info(250u64);
        assert!(
            result.is_err(),
            "Should not find blob for checkpoint in gap"
        );
        assert!(result.unwrap_err().to_string().contains("no blob found"));

        // Test checkpoints in stored ranges.
        assert!(state.get_checkpoint_blob_info(150u64).is_ok());
        assert!(state.get_checkpoint_blob_info(350u64).is_ok());
    }

    #[test]
    fn test_empty_index_entries() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let db_path = temp_dir.path().join("test_db");
        let state = ArchivalState::open(&db_path, false).expect("Failed to open database");

        // Create a blob with empty index entries.
        let empty_index_map: Vec<(String, (u64, u64))> = Vec::new();
        let blob_id = BlobId::from_str("empty_blob").unwrap_or(BlobId::ZERO);
        let epoch = 1000u32;

        state
            .create_new_checkpoint_blob(500u64, 599u64, &empty_index_map, blob_id, epoch, false)
            .expect("Failed to create blob");

        // Should still be able to find the blob.
        let blob_info = state
            .get_checkpoint_blob_info(550u64)
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

    #[test]
    fn test_index_entry_parsing() {
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
            .create_new_checkpoint_blob(1000u64, 1002u64, &index_map, blob_id, epoch, false)
            .expect("Failed to create blob");

        let blob_info = state
            .get_checkpoint_blob_info(1001u64)
            .expect("Should find blob");

        // Check that checkpoint numbers were parsed correctly.
        assert_eq!(blob_info.index_entries[0].checkpoint_number, 1000);
        assert_eq!(blob_info.index_entries[1].checkpoint_number, 1001); // .chk stripped.
        assert_eq!(blob_info.index_entries[2].checkpoint_number, 1002);
    }

    #[test]
    fn test_read_only_mode() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let db_path = temp_dir.path().join("test_db");

        // First create database in read-write mode and add some data.
        {
            let state = ArchivalState::open(&db_path, false).expect("Failed to open database");
            let index_map = vec![("1000".to_string(), (0u64, 100u64))];
            let blob_id = BlobId::from_str("test_blob").unwrap_or(BlobId::ZERO);
            let epoch = 1000u32;

            state
                .create_new_checkpoint_blob(1000u64, 1000u64, &index_map, blob_id, epoch, false)
                .expect("Failed to create blob");
        }

        // Now open in read-only mode.
        let state =
            ArchivalState::open(&db_path, true).expect("Failed to open database in read-only mode");

        // Reading should work.
        let blob_info = state
            .get_checkpoint_blob_info(1000u64)
            .expect("Should be able to read in read-only mode");
        assert_eq!(blob_info.start_checkpoint, 1000);

        // Writing should fail.
        let index_map = vec![("2000".to_string(), (0u64, 100u64))];
        let blob_id = BlobId::from_str("test_blob2").unwrap_or(BlobId::ZERO);
        let epoch = 2000u32;

        let result =
            state.create_new_checkpoint_blob(2000u64, 2000u64, &index_map, blob_id, epoch, false);

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("cannot create checkpoint blob in read-only mode")
        );
    }
}
