// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{fs, path::PathBuf, sync::Arc};

use anyhow::Result;
use arrow::{
    array::BinaryArray,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use prost::Message;
use rocksdb::IteratorMode;
use sui_sdk::{SuiClient, wallet_context::WalletContext};
use sui_types::{
    Identifier,
    base_types::{ObjectID, SequenceNumber, SuiAddress},
    programmable_transaction_builder::ProgrammableTransactionBuilder,
    transaction::{ObjectArg, TransactionData, TransactionKind},
};
use tokio::time;
use walrus_core::BlobId;
use walrus_sdk::{client::WalrusNodeClient, sui::client::SuiContractClient};

use crate::{
    archival_state::{ArchivalState, CF_CHECKPOINT_BLOB_INFO, proto::CheckpointBlobInfo},
    config::ArchivalStateSnapshotConfig,
    metrics::Metrics,
    util::upload_blob_to_walrus_with_retry,
};

pub struct ArchivalStateSnapshotCreator {
    archival_state: Arc<ArchivalState>,
    walrus_client: Arc<WalrusNodeClient<SuiContractClient>>,
    wallet: WalletContext,
    sui_client: SuiClient,
    active_address: SuiAddress,
    config: ArchivalStateSnapshotConfig,
    metrics: Arc<Metrics>,
}

impl ArchivalStateSnapshotCreator {
    pub async fn new(
        archival_state: Arc<ArchivalState>,
        walrus_client: Arc<WalrusNodeClient<SuiContractClient>>,
        mut wallet: WalletContext,
        config: ArchivalStateSnapshotConfig,
        metrics: Arc<Metrics>,
    ) -> Result<Self> {
        let sui_client = wallet.get_client().await?;
        let active_address = wallet.active_address()?;
        Ok(Self {
            archival_state,
            walrus_client,
            wallet,
            sui_client,
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
                tracing::error!("failed to create and upload snapshot: {}", e);
            }
        }
    }

    async fn create_and_upload_snapshot(&self) -> Result<()> {
        tracing::info!("creating archival state snapshot");

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
        self.create_parquet_file(&snapshot_file)?;

        // upload to walrus.
        let blob_id = self.upload_to_walrus(&snapshot_file).await?;

        // update on-chain metadata pointer if configured.
        let result = self
            .update_metadata_pointer(
                &self.config.metadata_package_id,
                &self.config.metadata_pointer_object_id,
                self.config.metadata_pointer_initial_shared_version,
                blob_id,
            )
            .await;

        // Ignore the error if the onchain update fails. We can always retry in the next snapshot.
        // TODO(zhe): add metrics to track the number of failed onchain updates.
        // TODO(zhe): eventually we need to get an alert on this.
        if let Err(e) = result {
            tracing::error!("failed to update on-chain metadata pointer: {}", e);
        }

        // clean up temporary file.
        fs::remove_file(&snapshot_file)?;

        tracing::info!(
            "snapshot created and uploaded successfully with blob_id: {}",
            blob_id
        );

        Ok(())
    }

    fn create_parquet_file(&self, file_path: &PathBuf) -> Result<()> {
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

        let iter = db.iterator_cf(&cf_handle, IteratorMode::Start);
        for item in iter {
            let (_key, value) = item?;
            let mut checkpoint_blob_info = CheckpointBlobInfo::decode(value.as_ref())?;

            // TODO(zhe): add metrics to track the number of rows written.

            // Remove index entries to reduce the size of the snapshot. The index entries are stored
            // in checkpoint blob and can be reconstructed from the blob.
            checkpoint_blob_info.index_entries.clear();
            values.push(checkpoint_blob_info.encode_to_vec());

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
        tracing::info!("parquet file created successfully");

        Ok(())
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

    async fn upload_to_walrus(&self, file_path: &PathBuf) -> Result<BlobId> {
        tracing::info!("uploading snapshot to walrus");

        let upload_timer = self.metrics.blob_upload_latency_seconds.start_timer();

        let (blob_id, _object_id, _end_epoch) = upload_blob_to_walrus_with_retry(
            &self.walrus_client,
            file_path,
            self.config.min_retry_duration,
            self.config.max_retry_duration,
            self.config.store_epoch_length,
            true,
            &self.metrics,
        )
        .await?;

        upload_timer.observe_duration();

        tracing::info!("snapshot uploaded to walrus with blob_id: {}", blob_id);

        Ok(blob_id)
    }

    async fn update_metadata_pointer(
        &self,
        package_id: &ObjectID,
        pointer_id: &ObjectID,
        pointer_initial_shared_version: SequenceNumber,
        blob_id: BlobId,
    ) -> Result<()> {
        tracing::info!("updating on-chain metadata pointer");

        // get admin cap from config.
        let admin_cap_id = &self.config.admin_cap_object_id;

        // fetch admin cap object to get version and digest.
        let admin_cap_obj = self
            .sui_client
            .read_api()
            .get_object_with_options(
                admin_cap_id.clone(),
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
            id: pointer_id.clone(),
            initial_shared_version: pointer_initial_shared_version,
            mutable: true,
        })?;
        let blob_id_arg = ptb.pure(blob_id_bytes)?;

        // call create_metadata_blob_pointer function.
        ptb.programmable_move_call(
            (*package_id).into(),
            Identifier::new("metadata")?,
            Identifier::new("update_metadata_blob_pointer")?,
            vec![],
            vec![admin_cap_arg, pointer_id_arg, blob_id_arg],
        );

        let pt = ptb.finish();

        tracing::info!(
            "executing metadata pointer creation transaction - package: {}, blob_id: {}, admin_cap: {}",
            package_id,
            blob_id,
            admin_cap_id
        );

        // get gas payment object.
        let coins = self
            .sui_client
            .coin_read_api()
            .get_coins(self.active_address, None, None, None)
            .await?;

        if coins.data.is_empty() {
            return Err(anyhow::anyhow!(
                "no gas coins available for address {}",
                self.active_address
            ));
        }

        let gas_coin = &coins.data[0];

        // create transaction data.
        let gas_budget = 10_000_000; // 0.01 SUI.
        let gas_price = self.sui_client.read_api().get_reference_gas_price().await?;

        let tx_data = TransactionData::new(
            TransactionKind::ProgrammableTransaction(pt),
            self.active_address,
            gas_coin.object_ref(),
            gas_budget,
            gas_price,
        );

        let signed_tx = self.wallet.sign_transaction(&tx_data).await;
        let response = self.wallet.execute_transaction_may_fail(signed_tx).await?;

        tracing::info!(
            "successfully created metadata pointer for blob_id: {}, tx digest: {:?}",
            blob_id,
            response.digest
        );

        Ok(())
    }
}
