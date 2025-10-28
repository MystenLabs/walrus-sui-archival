// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::Result;
use arrow::{
    array::BinaryArray,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use prost::Message;
use rocksdb::IteratorMode;
use sui_types::{
    Identifier,
    base_types::SuiAddress,
    programmable_transaction_builder::ProgrammableTransactionBuilder,
    transaction::{ObjectArg, TransactionData, TransactionKind},
};
use tokio::time;
use walrus_core::BlobId;

use crate::{
    archival_state::{ArchivalState, CF_CHECKPOINT_BLOB_INFO, proto::CheckpointBlobInfo},
    config::ArchivalStateSnapshotConfig,
    metrics::Metrics,
    sui_interactive_client::SuiInteractiveClient,
    util::upload_blob_to_walrus_with_retry,
};

pub struct ArchivalStateSnapshotCreator {
    archival_state: Arc<ArchivalState>,
    sui_interactive_client: SuiInteractiveClient,
    active_address: SuiAddress,
    config: ArchivalStateSnapshotConfig,
    metrics: Arc<Metrics>,
}

impl ArchivalStateSnapshotCreator {
    pub async fn new(
        archival_state: Arc<ArchivalState>,
        sui_interactive_client: SuiInteractiveClient,
        config: ArchivalStateSnapshotConfig,
        metrics: Arc<Metrics>,
    ) -> Result<Self> {
        let active_address = sui_interactive_client
            .with_wallet_mut(|wallet| wallet.active_address())
            .await?;
        Ok(Self {
            archival_state,
            sui_interactive_client,
            active_address,
            config,
            metrics,
        })
    }

    pub async fn run(&self) -> Result<()> {
        tracing::info!("starting archival state snapshot creator");

        let mut interval = time::interval(self.config.snapshot_state_to_walrus_interval);

        loop {
            interval.tick().await;

            if let Err(e) = self.create_and_upload_snapshot().await {
                self.metrics.snapshots_created_failed.inc();
                tracing::error!("failed to create and upload snapshot: {}", e);
            }
        }
    }

    async fn create_and_upload_snapshot(&self) -> Result<()> {
        tracing::info!("creating archival state snapshot");

        let creation_timer = self.metrics.snapshot_creation_latency_seconds.start_timer();

        // ensure snapshot directory exists.
        fs::create_dir_all(&self.config.snapshot_temp_dir)?;

        // delete any existing files under the snapshot directory.
        for entry in fs::read_dir(&self.config.snapshot_temp_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                fs::remove_file(&path)?;
            }
        }

        // generate snapshot file path.
        let snapshot_file = self.config.snapshot_temp_dir.join(format!(
            "archival_snapshot_{}.parquet",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        ));

        // create parquet file from rocksdb data.
        let records_count = self.create_parquet_file(&snapshot_file)?;
        self.metrics
            .snapshot_records_total
            .set(records_count as i64);

        creation_timer.observe_duration();

        // upload to walrus.
        let upload_timer = self.metrics.snapshot_upload_latency_seconds.start_timer();
        let blob_id = self.upload_to_walrus(&snapshot_file).await?;
        upload_timer.observe_duration();

        // update on-chain metadata pointer if configured.
        let package_id = self.config.contract_package_id;
        let pointer_id = self.config.metadata_pointer_object_id;
        let pointer_initial_shared_version = self.config.metadata_pointer_initial_shared_version;
        let active_address = self.active_address;
        let admin_cap_id = self.config.admin_cap_object_id;

        let result = self
            .sui_interactive_client
            .with_wallet_mut_async(|wallet| {
                Box::pin(async move {
                    let sui_client = wallet.get_client().await?;

                    tracing::info!("updating on-chain metadata pointer");

                    // fetch admin cap object to get version and digest.
                    let admin_cap_obj = sui_client
                        .read_api()
                        .get_object_with_options(
                            admin_cap_id,
                            sui_sdk::rpc_types::SuiObjectDataOptions::default(),
                        )
                        .await?;

                    let admin_cap_ref = admin_cap_obj
                        .object_ref_if_exists()
                        .expect("admin cap object must exist");

                    // convert blob_id to 32-byte vector.
                    let blob_id_bytes = blob_id.0.to_vec();

                    // build programmable transaction.
                    let mut ptb = ProgrammableTransactionBuilder::new();

                    // create arguments for the function call.
                    let admin_cap_arg = ptb.obj(ObjectArg::ImmOrOwnedObject(admin_cap_ref))?;
                    let pointer_id_arg = ptb.obj(ObjectArg::SharedObject {
                        id: pointer_id,
                        initial_shared_version: pointer_initial_shared_version,
                        mutable: true,
                    })?;
                    let blob_id_arg = ptb.pure(blob_id_bytes)?;

                    // call update_metadata_blob_pointer function.
                    ptb.programmable_move_call(
                        package_id,
                        Identifier::new("archival_metadata")?,
                        Identifier::new("update_metadata_blob_pointer")?,
                        vec![],
                        vec![admin_cap_arg, pointer_id_arg, blob_id_arg],
                    );

                    let pt = ptb.finish();

                    tracing::info!(
                        "executing metadata pointer update transaction - package: {}, blob_id: {}",
                        package_id,
                        blob_id
                    );

                    // get gas payment object.
                    let coins = sui_client
                        .coin_read_api()
                        .get_coins(active_address, None, None, None)
                        .await?;

                    if coins.data.is_empty() {
                        return Err(anyhow::anyhow!(
                            "no gas coins available for address {}",
                            active_address
                        ));
                    }

                    let gas_coin = &coins.data[0];

                    // create transaction data.
                    let gas_budget = 100_000_000; // 0.1 SUI.
                    let gas_price = sui_client.read_api().get_reference_gas_price().await?;

                    let tx_data = TransactionData::new(
                        TransactionKind::ProgrammableTransaction(pt),
                        active_address,
                        gas_coin.object_ref(),
                        gas_budget,
                        gas_price,
                    );

                    let signed_tx = wallet.sign_transaction(&tx_data).await;
                    let response = wallet.execute_transaction_may_fail(signed_tx).await?;

                    tracing::info!(
                        "successfully updated metadata pointer for blob_id: {}, tx digest: {:?}",
                        blob_id,
                        response.digest
                    );

                    Ok(())
                })
            })
            .await;

        // Track metadata update result.
        match result {
            Ok(_) => {
                self.metrics.metadata_updates_success.inc();
                tracing::info!("successfully updated on-chain metadata pointer");
            }
            Err(e) => {
                self.metrics.metadata_updates_failed.inc();
                tracing::error!("failed to update on-chain metadata pointer: {}", e);
                // Continue even if metadata update fails - we can retry in the next snapshot.
            }
        }

        // clean up temporary file.
        fs::remove_file(&snapshot_file)?;

        self.metrics.snapshots_created_success.inc();
        tracing::info!(
            "snapshot created and uploaded successfully with blob_id: {}",
            blob_id
        );

        Ok(())
    }

    fn create_parquet_file(&self, file_path: &PathBuf) -> Result<usize> {
        tracing::info!("creating parquet file from rocksdb data");

        // define schema for parquet file.
        // value is a serialized proto of CheckpointBlobInfo proto struct.
        let schema = Arc::new(Schema::new(vec![Field::new(
            "checkpoint_blob_info",
            DataType::Binary,
            false,
        )]));

        // create parquet writer.
        let file = fs::File::create(file_path)?;
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;

        // get database handle.
        let db = self.archival_state.get_db();

        // iterate over all column families.
        let cf_handle: Arc<rocksdb::BoundColumnFamily<'_>> =
            db.cf_handle(CF_CHECKPOINT_BLOB_INFO).ok_or_else(|| {
                anyhow::anyhow!(
                    "failed to get column family handle for {}",
                    CF_CHECKPOINT_BLOB_INFO
                )
            })?;

        // collect data from this column family.
        let mut values = Vec::new();
        let mut total_records = 0;

        let iter = db.iterator_cf(&cf_handle, IteratorMode::Start);
        for item in iter {
            let (_key, value) = item?;
            let mut checkpoint_blob_info = CheckpointBlobInfo::decode(value.as_ref())?;

            // Remove index entries to reduce the size of the snapshot. The index entries are stored
            // in checkpoint blob and can be reconstructed from the blob.
            checkpoint_blob_info.index_entries.clear();
            values.push(checkpoint_blob_info.encode_to_vec());
            total_records += 1;

            // write batch when we have enough records.
            if values.len() >= 1000 {
                self.write_batch_to_parquet(&mut writer, &values, &schema)?;
                values.clear();
            }
        }

        // write remaining records.
        if !values.is_empty() {
            self.write_batch_to_parquet(&mut writer, &values, &schema)?;
        }

        writer.close()?;
        tracing::info!(
            "parquet file created successfully with {} records",
            total_records
        );

        Ok(total_records)
    }

    fn write_batch_to_parquet(
        &self,
        writer: &mut ArrowWriter<fs::File>,
        values: &[Vec<u8>],
        schema: &Arc<Schema>,
    ) -> Result<()> {
        let value_array =
            BinaryArray::from(values.iter().map(|v| v.as_slice()).collect::<Vec<_>>());

        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(value_array)])?;

        writer.write(&batch)?;
        Ok(())
    }

    async fn upload_to_walrus(&self, file_path: &Path) -> Result<BlobId> {
        tracing::info!("uploading snapshot to walrus");

        let upload_timer = self.metrics.blob_upload_latency_seconds.start_timer();

        let blob_data = fs::read(file_path)?;
        let min_retry_duration = self.config.min_retry_duration;
        let max_retry_duration = self.config.max_retry_duration;
        let store_epoch_length = self.config.store_epoch_length;
        let metrics = self.metrics.clone();

        let (blob_id, _object_id, _end_epoch) = self
            .sui_interactive_client
            .with_walrus_client_async(|client| {
                Box::pin(async move {
                    upload_blob_to_walrus_with_retry(
                        "archival_state_snapshot_creator",
                        client,
                        None,
                        blob_data,
                        min_retry_duration,
                        max_retry_duration,
                        store_epoch_length,
                        true,
                        &metrics,
                    )
                    .await
                })
            })
            .await?;

        upload_timer.observe_duration();

        tracing::info!("snapshot uploaded to walrus with blob_id: {}", blob_id);

        Ok(blob_id)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::TempDir;

    use super::*;
    use crate::archival_state::proto;

    #[test]
    fn test_create_and_parse_parquet_file() {
        tracing_subscriber::fmt::init();

        // Create a temporary directory for the test database.
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db");

        // Create archival state and populate with test data.
        let archival_state = Arc::new(ArchivalState::open(&db_path, false).unwrap());

        // Create test checkpoint blob info records.
        let test_records = vec![
            proto::CheckpointBlobInfo {
                version: 1,
                blob_id: b"test_blob_id_1_padded_to_32byte".to_vec(),
                start_checkpoint: 100,
                end_checkpoint: 199,
                end_of_epoch: false,
                blob_expiration_epoch: 1000,
                object_id: vec![1u8; 32],
                index_entries: vec![
                    proto::IndexEntry {
                        checkpoint_number: 100,
                        offset: 0,
                        length: 1024,
                    },
                    proto::IndexEntry {
                        checkpoint_number: 150,
                        offset: 1024,
                        length: 2048,
                    },
                ],
                is_shared_blob: false,
            },
            proto::CheckpointBlobInfo {
                version: 1,
                blob_id: b"test_blob_id_2_padded_to_32byte".to_vec(),
                start_checkpoint: 200,
                end_checkpoint: 299,
                end_of_epoch: true,
                blob_expiration_epoch: 2000,
                object_id: vec![2u8; 32],
                index_entries: vec![proto::IndexEntry {
                    checkpoint_number: 200,
                    offset: 0,
                    length: 512,
                }],
                is_shared_blob: false,
            },
            proto::CheckpointBlobInfo {
                version: 1,
                blob_id: b"test_blob_id_3_padded_to_32byte".to_vec(),
                start_checkpoint: 300,
                end_checkpoint: 399,
                end_of_epoch: false,
                blob_expiration_epoch: 3000,
                object_id: vec![3u8; 32],
                index_entries: vec![],
                is_shared_blob: false,
            },
        ];

        // Populate the database with test records.
        archival_state
            .populate_from_checkpoint_blob_infos(test_records.clone())
            .unwrap();

        // Verify the database has the correct number of records.
        let count = archival_state.count_checkpoint_blobs().unwrap();
        assert_eq!(count, 3, "database should contain 3 records");

        // Create a mock ArchivalStateSnapshotCreator to access create_parquet_file.
        let temp_snapshot_dir = TempDir::new().unwrap();
        let snapshot_file = temp_snapshot_dir.path().join("test_snapshot.parquet");

        // Create the parquet file using a simpler approach.
        let db = archival_state.get_db();
        let cf_handle = db
            .cf_handle(crate::archival_state::CF_CHECKPOINT_BLOB_INFO)
            .unwrap();

        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new(
                "checkpoint_blob_info",
                arrow::datatypes::DataType::Binary,
                false,
            ),
        ]));

        let file = std::fs::File::create(&snapshot_file).unwrap();
        let props = parquet::file::properties::WriterProperties::builder().build();
        let mut writer =
            parquet::arrow::ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();

        let mut values = Vec::new();
        let iter = db.iterator_cf(&cf_handle, rocksdb::IteratorMode::Start);
        for item in iter {
            let (_key, value) = item.unwrap();
            let mut checkpoint_blob_info =
                proto::CheckpointBlobInfo::decode(value.as_ref()).unwrap();
            checkpoint_blob_info.index_entries.clear();
            values.push(checkpoint_blob_info.encode_to_vec());
        }

        let value_array = arrow::array::BinaryArray::from(
            values.iter().map(|v| v.as_slice()).collect::<Vec<_>>(),
        );
        let batch =
            arrow::record_batch::RecordBatch::try_new(schema.clone(), vec![Arc::new(value_array)])
                .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        // Read the parquet file data.
        let parquet_data = std::fs::read(&snapshot_file).unwrap();

        // Parse the parquet file using the utility function.
        let parsed_records =
            crate::util::parse_checkpoint_blob_infos_from_parquet(&parquet_data).unwrap();

        // Verify the parsed records match the original data.
        assert_eq!(
            parsed_records.len(),
            test_records.len(),
            "parsed records count should match original"
        );

        for (i, parsed) in parsed_records.iter().enumerate() {
            let original = &test_records[i];
            assert_eq!(
                parsed.blob_id, original.blob_id,
                "blob_id should match for record {}",
                i
            );
            assert_eq!(
                parsed.start_checkpoint, original.start_checkpoint,
                "start_checkpoint should match for record {}",
                i
            );
            assert_eq!(
                parsed.end_checkpoint, original.end_checkpoint,
                "end_checkpoint should match for record {}",
                i
            );
            assert_eq!(
                parsed.end_of_epoch, original.end_of_epoch,
                "end_of_epoch should match for record {}",
                i
            );
            assert_eq!(
                parsed.blob_expiration_epoch, original.blob_expiration_epoch,
                "blob_expiration_epoch should match for record {}",
                i
            );
            assert_eq!(
                parsed.object_id, original.object_id,
                "object_id should match for record {}",
                i
            );
            // Index entries should be empty (cleared during snapshot creation).
            assert_eq!(
                parsed.index_entries.len(),
                0,
                "index_entries should be cleared for record {}",
                i
            );
        }

        println!(
            "test passed: created parquet file with {} records and successfully parsed them back",
            parsed_records.len()
        );
    }
}
