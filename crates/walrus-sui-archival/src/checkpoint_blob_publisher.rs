// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{num::NonZeroU16, path::PathBuf, sync::Arc, time::Duration};

use anyhow::Result;
use blob_bundle::{BlobBundleBuildResult, BlobBundleBuilder};
use sui_types::{
    Identifier,
    base_types::ObjectID,
    messages_checkpoint::CheckpointSequenceNumber,
    programmable_transaction_builder::ProgrammableTransactionBuilder,
    transaction::{ObjectArg, TransactionData, TransactionKind},
};
use tokio::{fs, sync::mpsc, task::JoinSet};

use crate::{
    archival_state::ArchivalState,
    config::CheckpointBlobPublisherConfig,
    metrics::Metrics,
    sui_interactive_client::SuiInteractiveClient,
    util::upload_blob_to_walrus_with_retry,
};

// Global semaphore to limit concurrent blob builds to 1.
// This prevents disk I/O contention when building multiple blobs concurrently.
static BLOB_BUILD_SEMAPHORE: std::sync::OnceLock<tokio::sync::Semaphore> =
    std::sync::OnceLock::new();

fn get_blob_build_semaphore() -> &'static tokio::sync::Semaphore {
    BLOB_BUILD_SEMAPHORE.get_or_init(|| tokio::sync::Semaphore::new(1))
}

/// Message sent from CheckpointMonitor to CheckpointBlobPublisher.
#[derive(Debug, Clone)]
pub struct BlobBuildRequest {
    /// First checkpoint number in the range.
    pub start_checkpoint: CheckpointSequenceNumber,
    /// Last checkpoint number in the range (inclusive).
    pub end_checkpoint: CheckpointSequenceNumber,
    /// Weather this checkpoint contains the end-of-epoch transaction.
    pub end_of_epoch: bool,
}

/// A long-running service that builds blob files from checkpoint ranges.
pub struct CheckpointBlobPublisher {
    archival_state: Arc<ArchivalState>,
    sui_interactive_client: SuiInteractiveClient,
    uploader_interactive_clients: Vec<SuiInteractiveClient>,
    n_shards: NonZeroU16,
    config: CheckpointBlobPublisherConfig,
    downloaded_checkpoint_dir: PathBuf,
    metrics: Arc<Metrics>,
    contract_package_id: ObjectID,
    admin_cap_object_id: ObjectID,
}

impl CheckpointBlobPublisher {
    pub async fn new(
        archival_state: Arc<ArchivalState>,
        sui_interactive_client: SuiInteractiveClient,
        uploader_interactive_clients: Vec<SuiInteractiveClient>,
        config: CheckpointBlobPublisherConfig,
        downloaded_checkpoint_dir: PathBuf,
        metrics: Arc<Metrics>,
        contract_package_id: ObjectID,
        admin_cap_object_id: ObjectID,
    ) -> Result<Self> {
        let n_shards = sui_interactive_client
            .with_walrus_client_async(|client| {
                Box::pin(async move {
                    let committees = client.get_committees().await?;
                    Ok(committees.n_shards())
                })
            })
            .await?;
        Ok(Self {
            archival_state,
            sui_interactive_client,
            uploader_interactive_clients,
            n_shards,
            config,
            downloaded_checkpoint_dir,
            metrics,
            contract_package_id,
            admin_cap_object_id,
        })
    }

    /// Start the blob publisher service that listens for build requests.
    pub async fn start(self, mut request_rx: mpsc::Receiver<BlobBuildRequest>) -> Result<()> {
        tracing::info!(
            "starting checkpoint blob publisher with {} concurrent upload slots, storing blobs in {}",
            self.config.concurrent_publishing_tasks,
            self.config.checkpoint_blobs_dir.display()
        );

        // Clear the blob directory.
        fs::create_dir_all(&self.config.checkpoint_blobs_dir).await?;

        // Remove all files in the blob directory if there are any.
        for entry in std::fs::read_dir(&self.config.checkpoint_blobs_dir)? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                std::fs::remove_file(entry.path())?;
            }
        }

        // Use a semaphore to limit concurrent uploads.
        let semaphore = Arc::new(tokio::sync::Semaphore::new(
            self.config.concurrent_publishing_tasks,
        ));

        // Wrap self in Arc to share across tasks.
        let self_arc = Arc::new(self);

        // Use JoinSet to track running tasks.
        let mut tasks = JoinSet::new();

        loop {
            tokio::select! {
                // Check if any task has completed.
                Some(result) = tasks.join_next() => {
                    match result {
                        Ok(task_result) => {
                            // If the task returned an error, stop immediately.
                            if let Err(e) = task_result {
                                tracing::error!(
                                    "failed to build blob: {}, stopping checkpoint blob publisher",
                                    e
                                );
                                return Err(e);
                            }
                        }
                        Err(e) => {
                            tracing::error!("task join error: {}", e);
                            return Err(anyhow::anyhow!("task join error: {}", e));
                        }
                    }
                }

                // Receive new requests.
                Some(request) = request_rx.recv() => {
                    tracing::info!(
                        "received blob build request for checkpoints {} to {}",
                        request.start_checkpoint,
                        request.end_checkpoint
                    );

                    // Acquire a permit (wait if all slots are busy).
                    let permit = semaphore.clone().acquire_owned().await.unwrap();
                    let self_clone = self_arc.clone();

                    // Spawn task in background.
                    tasks.spawn(async move {
                        let result = Self::build_and_upload_blob(
                            "checkpoint_blob_publisher",
                            request,
                            &self_clone.archival_state,
                            self_clone.sui_interactive_client.clone(),
                            self_clone.sui_interactive_client.clone(),
                            self_clone.n_shards,
                            &self_clone.config,
                            &self_clone.downloaded_checkpoint_dir,
                            &self_clone.metrics,
                            self_clone.contract_package_id,
                            self_clone.admin_cap_object_id,
                        )
                        .await;

                        // Release the permit.
                        drop(permit);

                        result
                    });
                }

                // All requests processed and no tasks running.
                else => {
                    break;
                }
            }
        }

        tracing::info!("checkpoint blob publisher stopped");
        Ok(())
    }

    /// Start the blob publisher service with dedicated workers per uploader client.
    /// If uploader_interactive_clients is non-empty, creates one worker per client.
    /// Otherwise, creates a single worker using sui_interactive_client.
    /// All workers share a single async_channel with buffer size 1.
    pub async fn start_v2(self, mut request_rx: mpsc::Receiver<BlobBuildRequest>) -> Result<()> {
        let num_workers = if self.uploader_interactive_clients.is_empty() {
            1
        } else {
            self.uploader_interactive_clients.len()
        };

        tracing::info!(
            "starting checkpoint blob publisher v2 with {} workers, storing blobs in {}",
            num_workers,
            self.config.checkpoint_blobs_dir.display()
        );

        // Clear the blob directory.
        fs::create_dir_all(&self.config.checkpoint_blobs_dir).await?;

        // Remove all files in the blob directory if there are any.
        for entry in std::fs::read_dir(&self.config.checkpoint_blobs_dir)? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                std::fs::remove_file(entry.path())?;
            }
        }

        // Create a single shared channel with buffer size 1 for all workers.
        let (shared_tx, shared_rx) = async_channel::bounded::<BlobBuildRequest>(1);

        // Spawn workers that all listen on the same channel.
        let mut worker_handles = JoinSet::new();

        for i in 0..num_workers {
            // Determine which client to use for this worker.
            let client = if self.uploader_interactive_clients.is_empty() {
                self.sui_interactive_client.clone()
            } else {
                self.uploader_interactive_clients[i].clone()
            };

            // Clone necessary data for the worker.
            let archival_state = self.archival_state.clone();
            let config = self.config.clone();
            let downloaded_checkpoint_dir = self.downloaded_checkpoint_dir.clone();
            let metrics = self.metrics.clone();
            let contract_package_id = self.contract_package_id;
            let admin_cap_object_id = self.admin_cap_object_id;
            let n_shards = self.n_shards;
            let worker_rx = shared_rx.clone();
            let main_sui_interactive_client = self.sui_interactive_client.clone();

            // Spawn worker task.
            worker_handles.spawn(async move {
                tracing::info!("worker {} started", i);

                // Process messages one at a time from the shared channel.
                while let Ok(request) = worker_rx.recv().await {
                    tracing::info!(
                        "worker {} processing checkpoints {} to {}",
                        i,
                        request.start_checkpoint,
                        request.end_checkpoint
                    );

                    // Build and upload blob.
                    let result = Self::build_and_upload_blob(
                        &format!("worker_{}", i),
                        request,
                        &archival_state,
                        main_sui_interactive_client.clone(),
                        client.clone(),
                        n_shards,
                        &config,
                        &downloaded_checkpoint_dir,
                        &metrics,
                        contract_package_id,
                        admin_cap_object_id,
                    )
                    .await;

                    // If error occurs, return immediately.
                    if let Err(e) = result {
                        tracing::error!("worker {} failed to build and upload blob: {}", i, e);
                        return Err(e);
                    }
                }

                tracing::info!("worker {} stopped", i);
                Ok::<(), anyhow::Error>(())
            });
        }

        // Forward requests from mpsc channel to shared async_channel.
        // Monitor worker exits and return immediately on error.
        let forward_result: Result<()> = async {
            loop {
                tokio::select! {
                    // Monitor workers and exit immediately if any fails.
                    Some(result) = worker_handles.join_next() => {
                        // A worker has exited.
                        match result {
                            Ok(worker_result) => {
                                match worker_result {
                                    Ok(()) => {
                                        // Worker exited successfully (channel closed).
                                        tracing::info!("worker exited successfully");
                                    }
                                    Err(e) => {
                                        tracing::error!(
                                            "worker failed: {}, stopping checkpoint blob publisher",
                                            e
                                        );
                                        return Err(e);
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::error!("worker join error: {}", e);
                                return Err(anyhow::anyhow!("worker join error: {}", e));
                            }
                        }
                    }

                    // Receive new requests and forward to shared channel.
                    Some(request) = request_rx.recv() => {
                        tracing::info!(
                            "received blob build request for checkpoints {} to {}",
                            request.start_checkpoint,
                            request.end_checkpoint,
                        );

                        // Send to shared channel (this will block if buffer is full).
                        shared_tx.send(request).await?;
                    }

                    // All requests processed.
                    else => {
                        break;
                    }
                }
            }
            Ok(())
        }
        .await;

        // Close the shared channel.
        shared_tx.close();

        // If forwarding failed due to worker error, return immediately.
        forward_result?;

        // Wait for all remaining workers to finish.
        while let Some(result) = worker_handles.join_next().await {
            result??;
        }

        tracing::info!("checkpoint blob publisher v2 stopped");
        Ok(())
    }

    async fn build_and_upload_blob(
        worker_name: &str,
        request: BlobBuildRequest,
        archival_state: &Arc<ArchivalState>,
        main_sui_interactive_client: SuiInteractiveClient,
        sui_interactive_client: SuiInteractiveClient,
        n_shards: NonZeroU16,
        config: &CheckpointBlobPublisherConfig,
        downloaded_checkpoint_dir: &PathBuf,
        metrics: &Arc<Metrics>,
        contract_package_id: ObjectID,
        admin_cap_object_id: ObjectID,
    ) -> Result<()> {
        // Track the latency of building blobs.
        let build_timer = metrics.blob_build_latency_seconds.start_timer();
        let start_checkpoint = request.start_checkpoint;
        let end_checkpoint = request.end_checkpoint;

        tracing::info!(
            "{} building blob for checkpoints {} to {}",
            worker_name,
            start_checkpoint,
            end_checkpoint
        );

        // Collect checkpoint file paths.
        let mut file_paths = Vec::new();
        for checkpoint_num in start_checkpoint..=end_checkpoint {
            let checkpoint_file = downloaded_checkpoint_dir.join(format!("{checkpoint_num}"));

            // Check if the checkpoint file exists.
            if !checkpoint_file.exists() {
                return Err(anyhow::anyhow!(
                    "checkpoint file {} does not exist",
                    checkpoint_file.display()
                ));
            }

            file_paths.push(checkpoint_file);
        }

        if file_paths.is_empty() {
            tracing::warn!("no checkpoint files to bundle");
            return Ok(());
        }

        // Create the blob bundle.
        let builder = BlobBundleBuilder::new(n_shards);

        // Generate output filename.
        let blob_filename = format!(
            "checkpoint_blob_{}_{}.blob",
            start_checkpoint, end_checkpoint
        );

        tracing::info!(
            "{} bundling {} checkpoint files into a blob: {}",
            worker_name,
            file_paths.len(),
            blob_filename
        );

        let output_path = config.checkpoint_blobs_dir.join(&blob_filename);
        let file_num = file_paths.len();

        // Acquire blob build semaphore to prevent concurrent builds (disk I/O contention).
        tracing::info!("{} waiting for blob build semaphore", worker_name);
        let _build_permit = get_blob_build_semaphore().acquire().await.unwrap();
        tracing::info!("{} acquired blob build semaphore", worker_name);

        // Build the blob bundle in a blocking thread.
        let build_result =
            tokio::task::spawn_blocking(move || builder.build(&file_paths, &output_path)).await;

        // Release the semaphore immediately after build task completes (success or failure).
        drop(_build_permit);
        tracing::info!("{} released blob build semaphore", worker_name);

        // Now handle the result.
        let result =
            build_result.map_err(|e| anyhow::anyhow!("blob build task failed: {}", e))??;

        tracing::info!(
            "{} successfully built blob {} with {} checkpoints, total size {} bytes",
            worker_name,
            blob_filename,
            file_num,
            result.total_size
        );

        // Track blob size metrics.
        let blob_size = result.total_size as i64;
        metrics.blob_size_bytes.observe(blob_size as f64);
        metrics.latest_blob_size_bytes.set(blob_size);

        // Stop the build timer before starting upload.
        build_timer.observe_duration();

        Self::upload_blob_to_walrus(
            worker_name,
            &request,
            &result,
            main_sui_interactive_client,
            sui_interactive_client,
            archival_state,
            config,
            metrics,
            contract_package_id,
            admin_cap_object_id,
        )
        .await?;

        // Track the latest checkpoint included in uploaded blob.
        metrics
            .latest_uploaded_checkpoint
            .set(request.end_checkpoint as i64);

        // Clean up the downloaded checkpoints and uploaded blobs.
        Self::clean_up_downloaded_checkpoints_and_uploaded_blobs(
            &request,
            &result,
            downloaded_checkpoint_dir,
            metrics,
        )
        .await?;

        tracing::info!(
            "{} successfully cleaned up downloaded checkpoints and uploaded blobs for checkpoints {} to {}",
            worker_name,
            request.start_checkpoint,
            request.end_checkpoint
        );

        Ok(())
    }

    async fn upload_blob_to_walrus(
        worker_name: &str,
        request: &BlobBuildRequest,
        result: &BlobBundleBuildResult,
        main_sui_interactive_client: SuiInteractiveClient,
        sui_interactive_client: SuiInteractiveClient,
        archival_state: &Arc<ArchivalState>,
        config: &CheckpointBlobPublisherConfig,
        metrics: &Arc<Metrics>,
        contract_package_id: ObjectID,
        admin_cap_object_id: ObjectID,
    ) -> Result<()> {
        // Increment active uploads counter on entry.
        metrics.active_blob_uploads.inc();

        // Execute the upload and ensure we decrement on exit (both success and error).
        let result = Self::upload_blob_to_walrus_inner(
            worker_name,
            request,
            result,
            main_sui_interactive_client,
            sui_interactive_client,
            archival_state,
            config,
            metrics,
            contract_package_id,
            admin_cap_object_id,
        )
        .await;

        // Decrement active uploads counter on exit.
        metrics.active_blob_uploads.dec();

        result
    }

    async fn upload_blob_to_walrus_inner(
        worker_name: &str,
        request: &BlobBuildRequest,
        result: &BlobBundleBuildResult,
        main_sui_interactive_client: SuiInteractiveClient,
        sui_interactive_client: SuiInteractiveClient,
        archival_state: &Arc<ArchivalState>,
        config: &CheckpointBlobPublisherConfig,
        metrics: &Arc<Metrics>,
        contract_package_id: ObjectID,
        admin_cap_object_id: ObjectID,
    ) -> Result<()> {
        // Track the latency of uploading blobs.
        let upload_timer = metrics.blob_upload_latency_seconds.start_timer();

        let blob_file_path = result.file_path.clone();
        let min_retry_duration = config.min_retry_duration;
        let max_retry_duration = config.max_retry_duration;
        let store_epoch_length = config.store_epoch_length;
        let metrics_clone = metrics.clone();
        let worker_name_clone = worker_name.to_string();

        let transfer_to_address =
            main_sui_interactive_client.active_address != sui_interactive_client.active_address;

        let (blob_id, object_id, end_epoch) = sui_interactive_client
            .with_walrus_client_async(|client| {
                Box::pin(async move {
                    upload_blob_to_walrus_with_retry(
                        &worker_name_clone,
                        client,
                        if transfer_to_address {
                            Some(main_sui_interactive_client.active_address)
                        } else {
                            None
                        },
                        &blob_file_path,
                        min_retry_duration,
                        max_retry_duration,
                        store_epoch_length,
                        false,
                        &metrics_clone,
                    )
                    .await
                })
            })
            .await?;

        upload_timer.observe_duration();

        // Log the index map for debugging.
        tracing::debug!("blob index map:");
        for (id, (offset, length)) in &result.index_map {
            tracing::debug!("  {} -> offset: {}, length: {} bytes", id, offset, length);
        }

        // Optionally create a shared blob if configured.
        let (final_object_id, is_shared_blob) = if config.create_shared_blobs {
            tracing::info!(
                "{} creating shared blob for blob_id: {}",
                worker_name,
                blob_id
            );

            let shared_blob_id = main_sui_interactive_client
                .with_wallet_mut_async(|wallet| {
                    let package_id = contract_package_id;
                    let admin_cap_object_id_clone = admin_cap_object_id;
                    let blob_object_id = object_id;
                    let worker_name_clone = worker_name.to_string();

                    Box::pin(async move {
                        let sui_client = wallet.get_client().await?;
                        let active_address = wallet.active_address()?;

                        // Fetch AdminCap object to get version and digest.
                        let fetched_objects = sui_client
                            .read_api()
                            .multi_get_object_with_options(
                                vec![admin_cap_object_id_clone, blob_object_id],
                                sui_sdk::rpc_types::SuiObjectDataOptions::default(),
                            )
                            .await?;

                        let admin_cap_obj = fetched_objects[0].clone();
                        let blob_obj = fetched_objects[1].clone();

                        let admin_cap_ref = admin_cap_obj
                            .object_ref_if_exists()
                            .ok_or_else(|| anyhow::anyhow!("admin cap object not found"))?;

                        let blob_ref = blob_obj
                            .object_ref_if_exists()
                            .ok_or_else(|| anyhow::anyhow!("blob object not found"))?;

                        // Build programmable transaction.
                        let mut ptb = ProgrammableTransactionBuilder::new();

                        // Create arguments for the function call.
                        let admin_cap_arg = ptb.obj(ObjectArg::ImmOrOwnedObject(admin_cap_ref))?;
                        let blob_arg = ptb.obj(ObjectArg::ImmOrOwnedObject(blob_ref))?;

                        // Call create_shared_blob function.
                        ptb.programmable_move_call(
                            package_id,
                            Identifier::new("archival_blob")?,
                            Identifier::new("create_shared_blob")?,
                            vec![],
                            vec![admin_cap_arg, blob_arg],
                        );

                        let pt = ptb.finish();

                        tracing::info!(
                            "{} executing create_shared_blob transaction - package: {}, blob: {}",
                            &worker_name_clone,
                            package_id,
                            blob_object_id
                        );

                        // Get gas payment object.
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

                        // Create transaction data.
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

                        // Extract the SharedArchivalBlob object ID from object changes.
                        let object_changes = response.object_changes.as_ref().ok_or_else(|| {
                            anyhow::anyhow!("transaction object changes not found")
                        })?;

                        // Find the created SharedArchivalBlob object by type.
                        let shared_blob_id = object_changes
                            .iter()
                            .find_map(|change| {
                                if let sui_sdk::rpc_types::ObjectChange::Created {
                                    object_id,
                                    object_type,
                                    ..
                                } = change
                                {
                                    // Check if the object type ends with "::archival_blob::SharedArchivalBlob".
                                    if object_type
                                        .to_string()
                                        .ends_with("::archival_blob::SharedArchivalBlob")
                                    {
                                        return Some(*object_id);
                                    }
                                }
                                None
                            })
                            .ok_or_else(|| {
                                anyhow::anyhow!(
                                    "failed to find SharedArchivalBlob in created objects"
                                )
                            })?;

                        tracing::info!(
                            "{} successfully created shared blob, tx digest: {:?}, shared_blob_id: {}",
                            &worker_name_clone,
                            response.digest,
                            shared_blob_id
                        );

                        Ok(shared_blob_id)
                    })
                })
                .await?;

            (shared_blob_id, true)
        } else {
            (object_id, false)
        };

        // Wait until all prior checkpoint blobs are created.
        let start_wait_time = std::time::Instant::now();
        loop {
            let latest_checkpoint = archival_state.get_latest_stored_checkpoint()?;
            if latest_checkpoint.is_none() {
                // First checkpoint blob, just create it.
                break;
            }
            if latest_checkpoint.unwrap() + 1 == request.start_checkpoint {
                // We are the next checkpoint blob, just create it.
                break;
            }
            if start_wait_time.elapsed() > Duration::from_secs(1200) {
                return Err(anyhow::anyhow!(
                    "{} timed out waiting for prior checkpoint blobs to be created, latest checkpoint: {}, current start checkpoint: {}",
                    worker_name,
                    latest_checkpoint.unwrap(),
                    request.start_checkpoint
                ));
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        archival_state.create_new_checkpoint_blob(
            request.start_checkpoint,
            request.end_checkpoint,
            &result.index_map,
            blob_id,
            final_object_id,
            end_epoch,
            request.end_of_epoch,
            is_shared_blob,
        )?;

        Ok(())
    }

    async fn clean_up_downloaded_checkpoints_and_uploaded_blobs(
        request: &BlobBuildRequest,
        result: &BlobBundleBuildResult,
        downloaded_checkpoint_dir: &PathBuf,
        metrics: &Arc<Metrics>,
    ) -> Result<()> {
        tracing::info!(
            "cleaning up downloaded checkpoints and uploaded blobs for checkpoints {} to {}",
            request.start_checkpoint,
            request.end_checkpoint
        );

        // Track checkpoints being cleaned up.
        let checkpoints_count = request.end_checkpoint - request.start_checkpoint + 1;
        metrics.checkpoints_cleaned.inc_by(checkpoints_count);

        for checkpoint_num in request.start_checkpoint..=request.end_checkpoint {
            let checkpoint_file = downloaded_checkpoint_dir.join(format!("{checkpoint_num}"));

            if let Err(e) = std::fs::remove_file(&checkpoint_file) {
                // Do not stop if file removal fails.
                tracing::warn!(
                    "failed to remove checkpoint file {}: {}",
                    checkpoint_file.display(),
                    e
                );
            }
        }

        // Track latest checkpoint cleaned up.
        metrics
            .latest_cleaned_checkpoint
            .set(request.end_checkpoint as i64);

        // Remove the uploaded blob.
        if let Err(e) = std::fs::remove_file(&result.file_path) {
            // Do not stop if file removal fails.
            tracing::warn!(
                "failed to remove uploaded blob {}: {}",
                result.file_path.display(),
                e
            );
        } else {
            metrics.local_blobs_removed.inc();
            tracing::info!("removed uploaded blob: {}", result.file_path.display());
        }

        Ok(())
    }
}
