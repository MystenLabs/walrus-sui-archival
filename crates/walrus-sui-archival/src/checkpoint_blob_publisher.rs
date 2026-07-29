// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::BTreeMap,
    num::NonZeroU16,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result};
use blob_bundle::{BlobBundleBuilder, BlobBundleBuilderTrait};
use in_memory_checkpoint_holder::InMemoryCheckpointHolder;
use sui_types::{
    Identifier,
    base_types::ObjectID,
    messages_checkpoint::CheckpointSequenceNumber,
    programmable_transaction_builder::ProgrammableTransactionBuilder,
    transaction::{ObjectArg, TransactionData, TransactionKind},
};
use tokio::{fs, sync::mpsc, task::JoinSet};
use walrus_core::{BlobId, Epoch};

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

static BLOB_UPLOAD_SEMAPHORE: std::sync::OnceLock<tokio::sync::Semaphore> =
    std::sync::OnceLock::new();

fn get_blob_upload_semaphore(
    max_concurrent_blob_uploads: usize,
) -> &'static tokio::sync::Semaphore {
    BLOB_UPLOAD_SEMAPHORE.get_or_init(|| tokio::sync::Semaphore::new(max_concurrent_blob_uploads))
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

/// Message sent from sub-workers to the blob finalizer worker after a successful upload.
struct BlobUploadResult {
    worker_name: String,
    start_checkpoint: CheckpointSequenceNumber,
    end_checkpoint: CheckpointSequenceNumber,
    end_of_epoch: bool,
    blob_id: BlobId,
    object_id: ObjectID,
    end_epoch: Epoch,
    index_map: Vec<(String, (u64, u64))>,
}

/// A finalized blob ready to be committed to archival state.
struct FinalizedBlob {
    start_checkpoint: CheckpointSequenceNumber,
    end_checkpoint: CheckpointSequenceNumber,
    end_of_epoch: bool,
    blob_id: BlobId,
    final_object_id: ObjectID,
    end_epoch: Epoch,
    index_map: Vec<(String, (u64, u64))>,
    is_shared_blob: bool,
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
    in_memory_checkpoint_holder: Option<InMemoryCheckpointHolder>,
}

impl CheckpointBlobPublisher {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        archival_state: Arc<ArchivalState>,
        sui_interactive_client: SuiInteractiveClient,
        uploader_interactive_clients: Vec<SuiInteractiveClient>,
        config: CheckpointBlobPublisherConfig,
        downloaded_checkpoint_dir: PathBuf,
        metrics: Arc<Metrics>,
        contract_package_id: ObjectID,
        admin_cap_object_id: ObjectID,
        in_memory_checkpoint_holder: Option<InMemoryCheckpointHolder>,
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
            in_memory_checkpoint_holder,
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

        // Create the finalizer channel and spawn the finalizer worker.
        let (finalizer_tx, finalizer_rx) =
            mpsc::channel::<BlobUploadResult>(self.config.concurrent_publishing_tasks);

        // Wrap self in Arc to share across tasks.
        let self_arc = Arc::new(self);

        // Use JoinSet to track upload tasks (not the finalizer).
        let mut upload_tasks = JoinSet::new();

        // Spawn the blob finalizer worker separately.
        let finalizer_handle = {
            let archival_state = self_arc.archival_state.clone();
            let main_sui_interactive_client = self_arc.sui_interactive_client.clone();
            let config = self_arc.config.clone();
            let metrics = self_arc.metrics.clone();
            let contract_package_id = self_arc.contract_package_id;
            let admin_cap_object_id = self_arc.admin_cap_object_id;

            tokio::spawn(async move {
                Self::blob_finalizer_worker(
                    finalizer_rx,
                    &archival_state,
                    main_sui_interactive_client,
                    &config,
                    &metrics,
                    contract_package_id,
                    admin_cap_object_id,
                )
                .await
            })
        };

        let result: Result<()> = async {
            loop {
                tokio::select! {
                    // Check if any upload task has completed.
                    Some(result) = upload_tasks.join_next() => {
                        match result {
                            Ok(task_result) => {
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
                        let finalizer_tx = finalizer_tx.clone();

                        // Spawn task in background.
                        upload_tasks.spawn(async move {
                            let result = Self::build_and_upload_blob(
                                "checkpoint_blob_publisher",
                                request,
                                self_clone.sui_interactive_client.clone(),
                                self_clone.sui_interactive_client.clone(),
                                self_clone.n_shards,
                                &self_clone.config,
                                &self_clone.downloaded_checkpoint_dir,
                                &self_clone.metrics,
                                self_clone.in_memory_checkpoint_holder.clone(),
                                &finalizer_tx,
                            )
                            .await;

                            // Release the permit.
                            drop(permit);

                            result
                        });
                    }

                    // All requests processed and no upload tasks running.
                    else => {
                        break;
                    }
                }
            }
            Ok(())
        }
        .await;

        // Drop the original sender so the finalizer will see channel close
        // after all upload tasks (which hold clones) have completed.
        drop(finalizer_tx);

        // Wait for remaining upload tasks.
        while let Some(join_result) = upload_tasks.join_next().await {
            join_result??;
        }

        // Wait for the finalizer to finish processing remaining items.
        finalizer_handle.await??;

        // Propagate any error from the main loop.
        result?;

        tracing::info!("checkpoint blob publisher stopped");
        Ok(())
    }

    /// Start the blob publisher service with dedicated workers per uploader client.
    /// If uploader_interactive_clients is non-empty, creates one worker per client.
    /// Otherwise, creates a single worker using sui_interactive_client.
    /// All workers share a single async_channel with buffer size 1.
    ///
    /// A dedicated blob finalizer worker receives upload results from all sub-workers
    /// and handles `create_shared_blob` transactions and archival state updates sequentially.
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

        // Create a channel for sub-workers to send upload results to the finalizer.
        // Buffer size = num_workers so sub-workers don't block waiting for the finalizer.
        let (finalizer_tx, finalizer_rx) = mpsc::channel::<BlobUploadResult>(num_workers);

        // Spawn the blob finalizer worker separately (different lifecycle from upload workers).
        let finalizer_handle = {
            let archival_state = self.archival_state.clone();
            let main_sui_interactive_client = self.sui_interactive_client.clone();
            let config = self.config.clone();
            let metrics = self.metrics.clone();
            let contract_package_id = self.contract_package_id;
            let admin_cap_object_id = self.admin_cap_object_id;

            tokio::spawn(async move {
                Self::blob_finalizer_worker(
                    finalizer_rx,
                    &archival_state,
                    main_sui_interactive_client,
                    &config,
                    &metrics,
                    contract_package_id,
                    admin_cap_object_id,
                )
                .await
            })
        };

        // Spawn upload workers that all listen on the same channel.
        let mut worker_handles = JoinSet::new();
        for i in 0..num_workers {
            // Determine which client to use for this worker.
            let client = if self.uploader_interactive_clients.is_empty() {
                self.sui_interactive_client.clone()
            } else {
                self.uploader_interactive_clients[i].clone()
            };

            // Clone necessary data for the worker.
            let config = self.config.clone();
            let downloaded_checkpoint_dir = self.downloaded_checkpoint_dir.clone();
            let metrics = self.metrics.clone();
            let n_shards = self.n_shards;
            let worker_rx = shared_rx.clone();
            let main_sui_interactive_client = self.sui_interactive_client.clone();
            let in_memory_checkpoint_holder = self.in_memory_checkpoint_holder.clone();
            let finalizer_tx = finalizer_tx.clone();

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

                    // Build and upload blob, then send result to finalizer.
                    Self::build_and_upload_blob(
                        &format!("worker_{}", i),
                        request,
                        main_sui_interactive_client.clone(),
                        client.clone(),
                        n_shards,
                        &config,
                        &downloaded_checkpoint_dir,
                        &metrics,
                        in_memory_checkpoint_holder.clone(),
                        &finalizer_tx,
                    )
                    .await?;
                }

                tracing::info!("worker {} stopped", i);
                Ok::<(), anyhow::Error>(())
            });
        }

        // Drop the original finalizer_tx so the finalizer will exit when all workers are done.
        drop(finalizer_tx);

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

                        // Send to shared channel while continuing to monitor workers.
                        // This ensures we detect worker failures even if send blocks.
                        let send_future = shared_tx.send(request);
                        tokio::pin!(send_future);

                        loop {
                            tokio::select! {
                                // Continue monitoring workers during send.
                                Some(result) = worker_handles.join_next() => {
                                    match result {
                                        Ok(worker_result) => {
                                            match worker_result {
                                                Ok(()) => {
                                                    tracing::info!("worker exited successfully");
                                                }
                                                Err(e) => {
                                                    tracing::error!(
                                                        "worker failed during send: {}, stopping checkpoint blob publisher",
                                                        e
                                                    );
                                                    return Err(e);
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            tracing::error!("worker join error during send: {}", e);
                                            return Err(anyhow::anyhow!("worker join error: {}", e));
                                        }
                                    }
                                }

                                // Complete the send operation.
                                result = &mut send_future => {
                                    result?;
                                    break;
                                }
                            }
                        }
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

        // Close the shared channel so workers will exit.
        shared_tx.close();

        // If forwarding failed due to worker error, return immediately.
        forward_result?;

        // Wait for all remaining upload workers to finish.
        while let Some(result) = worker_handles.join_next().await {
            result??;
        }

        // All upload workers are done — their finalizer_tx clones are dropped.
        // Wait for the finalizer to finish processing remaining items.
        finalizer_handle.await??;

        tracing::info!("checkpoint blob publisher v2 stopped");
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn build_and_upload_blob(
        worker_name: &str,
        request: BlobBuildRequest,
        main_sui_interactive_client: SuiInteractiveClient,
        sui_interactive_client: SuiInteractiveClient,
        n_shards: NonZeroU16,
        config: &CheckpointBlobPublisherConfig,
        downloaded_checkpoint_dir: &Path,
        metrics: &Arc<Metrics>,
        in_memory_checkpoint_holder: Option<InMemoryCheckpointHolder>,
        finalizer_tx: &mpsc::Sender<BlobUploadResult>,
    ) -> Result<()> {
        // Track the latency of building blobs.
        let build_timer = metrics.blob_build_latency_seconds.start_timer();
        let start_checkpoint = request.start_checkpoint;
        let end_checkpoint = request.end_checkpoint;
        let read_checkpoints_from_memory = in_memory_checkpoint_holder.is_some();

        tracing::info!(
            "{} building blob for checkpoints {} to {}",
            worker_name,
            start_checkpoint,
            end_checkpoint
        );

        // Collect checkpoint file paths.
        let mut file_paths = Vec::new();
        if !read_checkpoints_from_memory {
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
        }

        // Create the blob bundle.
        let builder = BlobBundleBuilder::new(n_shards);

        // Generate output filename.
        let blob_filename = format!(
            "checkpoint_blob_{}_{}.blob",
            start_checkpoint, end_checkpoint
        );

        let file_num = end_checkpoint - start_checkpoint + 1;

        tracing::info!(
            "{} bundling {} checkpoint files into a blob: {}",
            worker_name,
            file_num,
            blob_filename
        );

        let output_path = config.checkpoint_blobs_dir.join(&blob_filename);

        // Acquire blob build semaphore to prevent concurrent builds (disk I/O contention).
        tracing::info!("{} waiting for blob build semaphore", worker_name);
        let _build_permit = get_blob_build_semaphore().acquire().await.unwrap();
        tracing::info!("{} acquired blob build semaphore", worker_name);

        // Build the blob bundle and extract common data.
        let (total_size, index_map, data) =
            if let Some(ref in_memory_checkpoint_holder) = in_memory_checkpoint_holder {
                let build_result = builder
                    .build_in_memory_from_holder(
                        in_memory_checkpoint_holder,
                        start_checkpoint,
                        end_checkpoint,
                    )
                    .await
                    .context("build blob bundle in memory from holder");

                // Release the semaphore immediately after build completes.
                drop(_build_permit);
                tracing::info!("{} released in memory blob build semaphore", worker_name);
                let result = build_result?;
                let total_size = result.get_total_size();
                let index_map = result.get_index_map();
                let data = result.get_data().await?;
                (total_size, index_map, data)
            } else if config.in_memory_build {
                let build_result = builder
                    .build_in_memory(&file_paths)
                    .context("build blob bundle in memory");

                // Release the semaphore immediately after build completes.
                drop(_build_permit);
                tracing::info!("{} released in memory blob build semaphore", worker_name);

                let result = build_result?;
                let total_size = result.get_total_size();
                let index_map = result.get_index_map();
                let data = result.get_data().await?;
                (total_size, index_map, data)
            } else {
                let build_result = builder
                    .build(&file_paths, &output_path)
                    .context("build blob bundle on disk");

                // Release the semaphore immediately after build completes.
                drop(_build_permit);
                tracing::info!("{} released on disk blob build semaphore", worker_name);

                let result = build_result?;
                let total_size = result.get_total_size();
                let index_map = result.get_index_map();
                let data = result.get_data().await?;
                (total_size, index_map, data)
            };

        tracing::info!(
            "{} successfully built blob {} with {} checkpoints, total size {} bytes",
            worker_name,
            blob_filename,
            file_num,
            total_size
        );

        // Track blob size metrics.
        let blob_size = total_size as i64;
        metrics.blob_size_bytes.observe(blob_size as f64);
        metrics.latest_blob_size_bytes.set(blob_size);

        // Stop the build timer before starting upload.
        build_timer.observe_duration();

        let upload_permit_size = if total_size > 1024 * 1024 * 1024 * 15 / 10 {
            2
        } else {
            1
        };

        let _upload_permit = get_blob_upload_semaphore(config.max_concurrent_blob_uploads)
            .acquire_many(upload_permit_size)
            .await
            .unwrap();

        tracing::info!(
            "{} acquired blob upload semaphore with size {}",
            worker_name,
            upload_permit_size
        );

        let result = Self::upload_blob_to_walrus(
            worker_name,
            &request,
            index_map,
            data,
            main_sui_interactive_client,
            sui_interactive_client,
            config,
            metrics,
            finalizer_tx,
        )
        .await;

        drop(_upload_permit);
        tracing::info!("{} released blob upload semaphore", worker_name);

        result?;

        // Clean up the downloaded checkpoints and uploaded blobs.
        Self::clean_up_downloaded_checkpoints_and_uploaded_blobs(
            &request,
            if !config.in_memory_build {
                Some(&output_path)
            } else {
                None
            },
            downloaded_checkpoint_dir,
            metrics,
            in_memory_checkpoint_holder.clone(),
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

    #[allow(clippy::too_many_arguments)]
    async fn upload_blob_to_walrus(
        worker_name: &str,
        request: &BlobBuildRequest,
        index_map: Vec<(String, (u64, u64))>,
        data: Vec<u8>,
        main_sui_interactive_client: SuiInteractiveClient,
        sui_interactive_client: SuiInteractiveClient,
        config: &CheckpointBlobPublisherConfig,
        metrics: &Arc<Metrics>,
        finalizer_tx: &mpsc::Sender<BlobUploadResult>,
    ) -> Result<()> {
        // Increment active uploads counter on entry.
        metrics.active_blob_uploads.inc();

        // Execute the upload and ensure we decrement on exit (both success and error).
        let result = Self::upload_blob_to_walrus_inner(
            worker_name,
            request,
            index_map,
            data,
            main_sui_interactive_client,
            sui_interactive_client,
            config,
            metrics,
            finalizer_tx,
        )
        .await;

        // Decrement active uploads counter on exit.
        metrics.active_blob_uploads.dec();

        result
    }

    #[allow(clippy::too_many_arguments)]
    async fn upload_blob_to_walrus_inner(
        worker_name: &str,
        request: &BlobBuildRequest,
        index_map: Vec<(String, (u64, u64))>,
        data: Vec<u8>,
        main_sui_interactive_client: SuiInteractiveClient,
        sui_interactive_client: SuiInteractiveClient,
        config: &CheckpointBlobPublisherConfig,
        metrics: &Arc<Metrics>,
        finalizer_tx: &mpsc::Sender<BlobUploadResult>,
    ) -> Result<()> {
        // Track the latency of uploading blobs.
        let upload_timer = metrics.blob_upload_latency_seconds.start_timer();

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
                        data,
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
        for (id, (offset, length)) in &index_map {
            tracing::debug!("  {} -> offset: {}, length: {} bytes", id, offset, length);
        }

        // Send the upload result to the finalizer worker for sequential processing
        // (ownership polling, create_shared_blob tx, and archival state update).
        tracing::info!(
            "{} sending upload result to finalizer for checkpoints {} to {}",
            worker_name,
            request.start_checkpoint,
            request.end_checkpoint,
        );

        finalizer_tx
            .send(BlobUploadResult {
                worker_name: worker_name.to_string(),
                start_checkpoint: request.start_checkpoint,
                end_checkpoint: request.end_checkpoint,
                end_of_epoch: request.end_of_epoch,
                blob_id,
                object_id,
                end_epoch,
                index_map,
            })
            .await
            .map_err(|_| anyhow::anyhow!("finalizer channel closed unexpectedly"))?;

        Ok(())
    }

    /// Dedicated worker that processes blob upload results.
    /// Handles ownership polling, `create_shared_blob` transactions, and archival state updates.
    /// This ensures no concurrent main wallet transactions.
    ///
    /// Results may arrive out of order from parallel sub-workers. The finalizer buffers
    /// finalized blobs and commits them to archival state in checkpoint order.
    #[allow(clippy::too_many_arguments)]
    async fn blob_finalizer_worker(
        mut rx: mpsc::Receiver<BlobUploadResult>,
        archival_state: &Arc<ArchivalState>,
        main_sui_interactive_client: SuiInteractiveClient,
        config: &CheckpointBlobPublisherConfig,
        metrics: &Arc<Metrics>,
        contract_package_id: ObjectID,
        admin_cap_object_id: ObjectID,
    ) -> Result<()> {
        tracing::info!("blob finalizer worker started");

        // Buffer for out-of-order finalized blobs, keyed by start_checkpoint.
        let mut pending_commits: BTreeMap<CheckpointSequenceNumber, FinalizedBlob> =
            BTreeMap::new();

        while let Some(result) = rx.recv().await {
            let BlobUploadResult {
                worker_name,
                start_checkpoint,
                end_checkpoint,
                end_of_epoch,
                blob_id,
                object_id,
                end_epoch,
                index_map,
            } = result;

            tracing::info!(
                "finalizer processing checkpoints {} to {} from {}",
                start_checkpoint,
                end_checkpoint,
                worker_name,
            );

            // Optionally create a shared blob if configured.
            let (final_object_id, is_shared_blob) = if config.create_shared_blobs {
                tracing::info!(
                    "finalizer creating shared blob for blob_id: {} (from {})",
                    blob_id,
                    worker_name,
                );

                // Poll until the main wallet's RPC confirms ownership of the blob.
                let main_wallet_address = main_sui_interactive_client.active_address;
                let max_attempts = 30;
                let poll_interval = Duration::from_secs(2);
                let mut confirmed = false;

                for attempt in 1..=max_attempts {
                    let owner_result = main_sui_interactive_client
                        .with_wallet_async(|wallet| {
                            let blob_oid = object_id;
                            Box::pin(async move {
                                let sui_client =
                                    crate::util::build_sui_client_from_wallet(wallet).await?;
                                let resp = sui_client
                                    .read_api()
                                    .get_object_with_options(
                                        blob_oid,
                                        sui_sdk::rpc_types::SuiObjectDataOptions::new()
                                            .with_owner(),
                                    )
                                    .await?;
                                Ok(resp.owner().and_then(|o| o.get_owner_address().ok()))
                            })
                        })
                        .await?;

                    if owner_result == Some(main_wallet_address) {
                        confirmed = true;
                        break;
                    }

                    tracing::info!(
                        "finalizer: blob {} owned by {}, waiting for transfer to {} (attempt {}/{})",
                        object_id,
                        owner_result.map_or("unknown".to_string(), |a| a.to_string()),
                        main_wallet_address,
                        attempt,
                        max_attempts,
                    );
                    tokio::time::sleep(poll_interval).await;
                }

                if !confirmed {
                    return Err(anyhow::anyhow!(
                        "blob {} not owned by {} after {} attempts",
                        object_id,
                        main_wallet_address,
                        max_attempts,
                    ));
                }

                // Retry the entire transaction (re-fetching object refs each time)
                // to handle stale object versions from concurrent wallet usage.
                let mut last_error = None;
                let mut shared_blob_id = None;

                for attempt in 1..=5 {
                    let result = main_sui_interactive_client
                        .with_wallet_mut_async(|wallet| {
                            let package_id = contract_package_id;
                            let admin_cap_object_id_clone = admin_cap_object_id;
                            let blob_object_id = object_id;

                            Box::pin(async move {
                                let sui_client = crate::util::build_sui_client_from_wallet(wallet).await?;
                                let active_address = wallet.active_address()?;

                                // Fetch AdminCap object to get version and digest.
                                // Retry a few times with delay in case of RPC lag.
                                let mut admin_cap_ref = None;
                                for fetch_attempt in 1..=5u64 {
                                    let admin_cap_obj = sui_client
                                        .read_api()
                                        .get_object_with_options(
                                            admin_cap_object_id_clone,
                                            sui_sdk::rpc_types::SuiObjectDataOptions::default(),
                                        )
                                        .await?;
                                    if let Some(r) = admin_cap_obj.object_ref_if_exists() {
                                        admin_cap_ref = Some(r);
                                        break;
                                    }
                                    tracing::warn!(
                                        "finalizer: admin cap object {} not found (attempt {}/5), retrying after delay",
                                        admin_cap_object_id_clone,
                                        fetch_attempt,
                                    );
                                    tokio::time::sleep(std::time::Duration::from_secs(
                                        fetch_attempt * 2,
                                    ))
                                    .await;
                                }
                                let admin_cap_ref = admin_cap_ref.ok_or_else(|| {
                                    anyhow::anyhow!("admin cap object not found after 5 attempts")
                                })?;

                                // Fetch blob ref - ownership already confirmed above.
                                // Retry a few times with delay in case of RPC lag.
                                let mut blob_ref = None;
                                for fetch_attempt in 1..=5u64 {
                                    let blob_obj = sui_client
                                        .read_api()
                                        .get_object_with_options(
                                            blob_object_id,
                                            sui_sdk::rpc_types::SuiObjectDataOptions::default(),
                                        )
                                        .await?;
                                    if let Some(r) = blob_obj.object_ref_if_exists() {
                                        blob_ref = Some(r);
                                        break;
                                    }
                                    tracing::warn!(
                                        "finalizer: blob object {} not found (attempt {}/5), retrying after delay",
                                        blob_object_id,
                                        fetch_attempt,
                                    );
                                    tokio::time::sleep(std::time::Duration::from_secs(
                                        fetch_attempt * 2,
                                    ))
                                    .await;
                                }
                                let blob_ref = blob_ref.ok_or_else(|| {
                                    anyhow::anyhow!("blob object {} not found after 5 attempts", blob_object_id)
                                })?;

                                // Build programmable transaction.
                                let mut ptb = ProgrammableTransactionBuilder::new();

                                let admin_cap_arg =
                                    ptb.obj(ObjectArg::ImmOrOwnedObject(admin_cap_ref))?;
                                let blob_arg =
                                    ptb.obj(ObjectArg::ImmOrOwnedObject(blob_ref))?;

                                ptb.programmable_move_call(
                                    package_id,
                                    Identifier::new("archival_blob")?,
                                    Identifier::new("create_shared_blob")?,
                                    vec![],
                                    vec![admin_cap_arg, blob_arg],
                                );

                                let pt = ptb.finish();

                                tracing::info!(
                                    "finalizer executing create_shared_blob transaction - package: {}, blob: {}",
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

                                let gas_budget = 100_000_000; // 0.1 SUI.
                                let gas_price =
                                    sui_client.read_api().get_reference_gas_price().await?;

                                let tx_data = TransactionData::new(
                                    TransactionKind::ProgrammableTransaction(pt),
                                    active_address,
                                    gas_coin.object_ref(),
                                    gas_budget,
                                    gas_price,
                                );

                                let signed_tx = wallet.sign_transaction(&tx_data).await;
                                let response = sui_client
                                    .quorum_driver_api()
                                    .execute_transaction_block(
                                        signed_tx,
                                        sui_sdk::rpc_types::SuiTransactionBlockResponseOptions::new()
                                            .with_effects()
                                            .with_object_changes(),
                                        Some(
                                            sui_types::transaction_driver_types::ExecuteTransactionRequestType::WaitForLocalExecution,
                                        ),
                                    )
                                    .await?;

                                // If object_changes is missing from the response, re-query the
                                // transaction to get them. The tx already succeeded on-chain so
                                // we must not retry the tx itself — only retry the query.
                                let object_changes = if let Some(changes) = response.object_changes {
                                    changes
                                } else {
                                    tracing::warn!(
                                        "finalizer: object_changes missing from tx response, re-querying tx {}",
                                        response.digest
                                    );
                                    let mut re_query_result = None;
                                    for re_query_attempt in 1..=5 {
                                        tokio::time::sleep(std::time::Duration::from_secs(
                                            re_query_attempt * 2,
                                        ))
                                        .await;
                                        tracing::info!(
                                            "finalizer: re-query attempt {}/5 for tx {}",
                                            re_query_attempt,
                                            response.digest
                                        );
                                        match sui_client
                                            .read_api()
                                            .get_transaction_with_options(
                                                response.digest,
                                                sui_sdk::rpc_types::SuiTransactionBlockResponseOptions::new()
                                                    .with_object_changes(),
                                            )
                                            .await
                                        {
                                            Ok(re_queried)
                                                if re_queried.object_changes.is_some() =>
                                            {
                                                re_query_result =
                                                    Some(re_queried.object_changes.unwrap());
                                                break;
                                            }
                                            Ok(_) => {
                                                tracing::warn!(
                                                    "finalizer: re-query attempt {}/5 returned no object_changes for tx {}",
                                                    re_query_attempt,
                                                    response.digest
                                                );
                                            }
                                            Err(e) => {
                                                tracing::warn!(
                                                    "finalizer: re-query attempt {}/5 failed for tx {}: {}",
                                                    re_query_attempt,
                                                    response.digest,
                                                    e
                                                );
                                            }
                                        }
                                    }
                                    re_query_result.ok_or_else(|| {
                                        anyhow::anyhow!(
                                            "object_changes still missing after 5 re-query attempts for tx {}",
                                            response.digest
                                        )
                                    })?
                                };

                                let created_id = object_changes
                                    .iter()
                                    .find_map(|change| {
                                        if let sui_sdk::rpc_types::ObjectChange::Created {
                                            object_id,
                                            object_type,
                                            ..
                                        } = change
                                            && object_type
                                                .to_string()
                                                .ends_with(
                                                    "::archival_blob::SharedArchivalBlob",
                                                )
                                        {
                                            return Some(*object_id);
                                        }
                                        None
                                    })
                                    .ok_or_else(|| {
                                        anyhow::anyhow!(
                                            "failed to find SharedArchivalBlob in created objects"
                                        )
                                    })?;

                                tracing::info!(
                                    "finalizer successfully created shared blob, tx digest: {:?}, shared_blob_id: {}",
                                    response.digest,
                                    created_id
                                );

                                Ok(created_id)
                            })
                        })
                        .await;

                    match result {
                        Ok(id) => {
                            shared_blob_id = Some(id);
                            break;
                        }
                        Err(e) => {
                            tracing::warn!(
                                "finalizer create_shared_blob attempt {} failed: {}",
                                attempt,
                                e
                            );
                            last_error = Some(e);
                            if attempt < 5 {
                                tokio::time::sleep(Duration::from_secs(attempt * 2)).await;
                            }
                        }
                    }
                }

                let shared_blob_id = shared_blob_id.ok_or_else(|| {
                    anyhow::anyhow!(
                        "finalizer create_shared_blob failed after 3 attempts: {:?}",
                        last_error
                    )
                })?;

                (shared_blob_id, true)
            } else {
                (object_id, false)
            };

            // Buffer this finalized blob.
            pending_commits.insert(
                start_checkpoint,
                FinalizedBlob {
                    start_checkpoint,
                    end_checkpoint,
                    end_of_epoch,
                    blob_id,
                    final_object_id,
                    end_epoch,
                    index_map,
                    is_shared_blob,
                },
            );

            // Flush all consecutive blobs that are ready to commit.
            Self::flush_pending_commits(&mut pending_commits, archival_state, metrics)?;
        }

        // Flush any remaining buffered blobs on channel close.
        if !pending_commits.is_empty() {
            tracing::info!(
                "finalizer draining {} remaining buffered blobs",
                pending_commits.len()
            );
            Self::flush_pending_commits(&mut pending_commits, archival_state, metrics)?;

            if !pending_commits.is_empty() {
                return Err(anyhow::anyhow!(
                    "finalizer exiting with {} uncommitted blobs (gap in checkpoint sequence)",
                    pending_commits.len()
                ));
            }
        }

        tracing::info!("blob finalizer worker stopped");
        Ok(())
    }

    /// Flush all consecutive finalized blobs from the buffer into archival state.
    /// Commits blobs whose start_checkpoint matches the next expected checkpoint.
    fn flush_pending_commits(
        pending_commits: &mut BTreeMap<CheckpointSequenceNumber, FinalizedBlob>,
        archival_state: &Arc<ArchivalState>,
        metrics: &Arc<Metrics>,
    ) -> Result<()> {
        loop {
            // Determine the next expected start checkpoint.
            let next_start = match archival_state.get_latest_stored_checkpoint()? {
                Some(latest) => latest + 1,
                None => {
                    // No blobs committed yet — the first blob in the buffer is acceptable.
                    match pending_commits.keys().next() {
                        Some(&first_key) => first_key,
                        None => return Ok(()),
                    }
                }
            };

            // Check if the next expected blob is in the buffer.
            let blob = match pending_commits.remove(&next_start) {
                Some(b) => b,
                None => return Ok(()),
            };

            tracing::info!(
                "finalizer committing checkpoints {} to {}",
                blob.start_checkpoint,
                blob.end_checkpoint,
            );

            archival_state.create_new_checkpoint_blob(
                blob.start_checkpoint,
                blob.end_checkpoint,
                &blob.index_map,
                blob.blob_id,
                blob.final_object_id,
                blob.end_epoch,
                blob.end_of_epoch,
                blob.is_shared_blob,
            )?;

            metrics
                .latest_uploaded_checkpoint
                .set(blob.end_checkpoint as i64);
        }
    }

    async fn clean_up_downloaded_checkpoints_and_uploaded_blobs(
        request: &BlobBuildRequest,
        file_path: Option<&PathBuf>,
        downloaded_checkpoint_dir: &Path,
        metrics: &Arc<Metrics>,
        in_memory_checkpoint_holder: Option<InMemoryCheckpointHolder>,
    ) -> Result<()> {
        tracing::info!(
            "cleaning up downloaded checkpoints and uploaded blobs for checkpoints {} to {}",
            request.start_checkpoint,
            request.end_checkpoint
        );

        let checkpoints_count = request.end_checkpoint - request.start_checkpoint + 1;
        if let Some(in_memory_checkpoint_holder) = in_memory_checkpoint_holder {
            metrics
                .checkpoints_cleaned_in_memory
                .inc_by(checkpoints_count);
            for checkpoint_num in request.start_checkpoint..=request.end_checkpoint {
                in_memory_checkpoint_holder.remove(checkpoint_num).await;
            }
        } else {
            // Track checkpoints being cleaned up.
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
        }

        // Track latest checkpoint cleaned up.
        metrics
            .latest_cleaned_checkpoint
            .set(request.end_checkpoint as i64);

        if let Some(file_path) = file_path {
            // Remove the uploaded blob.
            if let Err(e) = std::fs::remove_file(file_path) {
                // Do not stop if file removal fails.
                tracing::warn!(
                    "failed to remove uploaded blob {}: {}",
                    file_path.display(),
                    e
                );
            } else {
                metrics.local_blobs_removed.inc();
                tracing::info!("removed uploaded blob: {}", file_path.display());
            }
        }

        Ok(())
    }
}
