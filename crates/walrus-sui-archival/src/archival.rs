// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{path::PathBuf, sync::Arc};

use anyhow::Result;
use sui_sdk::wallet_context::WalletContext;
use sui_types::messages_checkpoint::CheckpointSequenceNumber;
use tokio::{select, sync::mpsc};
use walrus_sdk::{
    SuiReadClient,
    client::WalrusNodeClient,
    config::ClientConfig,
    sui::client::SuiContractClient,
};

use crate::{
    archival_state::ArchivalState,
    archival_state_snapshot_creator::ArchivalStateSnapshotCreator,
    checkpoint_blob_extender::CheckpointBlobExtender,
    checkpoint_blob_publisher::{self, BlobBuildRequest},
    checkpoint_downloader,
    checkpoint_monitor,
    config::{CheckpointDownloaderType, Config},
    injection_service_checkpoint_downloader,
    metrics::Metrics,
    rest_api::RestApiServer,
    sui_interactive_client::SuiInteractiveClient,
};

pub fn run_sui_archival(config: Config, version: &'static str) -> Result<()> {
    tracing::info!("starting sui archival process...");

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(config.thread_pool_size)
        .enable_all()
        .build()?;

    runtime.block_on(async { run_application_logic(config, version).await })
}

async fn run_application_logic(config: Config, version: &'static str) -> Result<()> {
    tracing::info!("starting application logic in multi-thread runtime");

    let registry_service = mysten_metrics::start_prometheus_server(config.metrics_address);
    let registry = registry_service.default_registry();

    let registry_clone = registry.clone();
    tokio::spawn(async move {
        registry_clone
            .register(mysten_metrics::uptime_metric(
                "walrus-sui-archival",
                version,
                "sui-walrus",
            ))
            .expect("metrics defined at compile time must be valid");
    });

    // Create metrics.
    let metrics = Arc::new(Metrics::new(&registry));

    // Initialize the archival state with RocksDB.
    let mut archival_state = ArchivalState::open(&config.db_path, false)?;
    tracing::info!(
        "initialized archival state with database at {:?}",
        config.db_path
    );

    // Check if the database is empty and populate from metadata blob if needed.
    let record_count = archival_state.count_checkpoint_blobs()?;
    tracing::info!("database contains {} checkpoint blob records", record_count);

    if record_count == 0 {
        tracing::info!("database is empty, attempting to populate from metadata blob");

        match crate::util::load_checkpoint_blob_infos_from_metadata(
            &config.client_config_path,
            config.archival_state_snapshot.metadata_pointer_object_id,
            &config.context,
        )
        .await
        {
            Ok(blob_infos) => {
                tracing::info!("loaded {} records from metadata blob", blob_infos.len());
                archival_state.populate_from_checkpoint_blob_infos(blob_infos)?;
                tracing::info!("database populated successfully");
            }
            Err(e) => {
                tracing::warn!(
                    "failed to load metadata blob, starting with empty database: {}",
                    e
                );
            }
        }
    }

    let (client_config, _) = ClientConfig::load_from_multi_config(
        config.client_config_path.clone(),
        Some(&config.context),
    )?;

    // Initialize walrus client.
    let walrus_client = initialize_walrus_client(client_config.clone()).await?;
    let walrus_read_client =
        Arc::new(initialize_walrus_read_client(client_config.clone(), &walrus_client).await?);

    // Set walrus read client on archival state for lazy index fetching.
    archival_state.set_walrus_read_client(walrus_read_client.clone());

    let archival_state = Arc::new(archival_state);

    let wallet = WalletContext::new(
        client_config
            .wallet_config
            .expect("wallet config is required")
            .path()
            .expect("wallet config path is required"),
    )?;
    let sui_interactive_client = SuiInteractiveClient::new(walrus_client, wallet);

    // TODO(zhe): remove testing initial checkpoint.
    let initial_checkpoint = archival_state
        .get_latest_stored_checkpoint()?
        .unwrap_or(CheckpointSequenceNumber::from(244999999u64))
        + 1;

    // Cleanup all the old downloaded checkpoints before initial_checkpoint.
    cleanup_orphaned_downloaded_checkpoints_and_uploaded_blobs(
        initial_checkpoint,
        config
            .checkpoint_downloader
            .downloaded_checkpoint_dir()
            .clone(),
    )
    .await?;

    tracing::info!("initial checkpoint: {}", initial_checkpoint);

    // Create channel for blob build requests.
    let (blob_publisher_tx, blob_publisher_rx) = mpsc::channel::<BlobBuildRequest>(1);

    // Start the checkpoint blob publisher.
    let blob_publisher = checkpoint_blob_publisher::CheckpointBlobPublisher::new(
        archival_state.clone(),
        sui_interactive_client.clone(),
        config.checkpoint_blob_publisher.clone(),
        config
            .checkpoint_downloader
            .downloaded_checkpoint_dir()
            .clone(),
        metrics.clone(),
        config.archival_state_snapshot.contract_package_id,
    )
    .await?;
    let blob_publisher_handle =
        tokio::spawn(async move { blob_publisher.start(blob_publisher_rx).await });

    // Start the checkpoint downloader based on the configured type.
    let (
        checkpoint_receiver,
        downloader_pause_tx,
        watermark_tx,
        checkpoint_downloading_driver_handle,
    ) = match &config.checkpoint_downloader {
        CheckpointDownloaderType::Bucket(downloader_config) => {
            let downloader = checkpoint_downloader::CheckpointDownloader::new(
                downloader_config.clone(),
                metrics.clone(),
            );
            let (receiver, pause_tx, handle) = downloader.start(initial_checkpoint).await?;
            (receiver, Some(pause_tx), None, handle)
        }
        CheckpointDownloaderType::InjectionService(downloader_config) => {
            let downloader =
                injection_service_checkpoint_downloader::InjectionServiceCheckpointDownloader::new(
                    downloader_config.clone(),
                    metrics.clone(),
                );
            let (receiver, watermark_tx, handle) = downloader.start(initial_checkpoint).await?;
            (receiver, None, Some(watermark_tx), handle)
        }
    };

    // Start the checkpoint monitor with the receiver.
    let mut monitor = checkpoint_monitor::CheckpointMonitor::new(
        config.checkpoint_monitor.clone(),
        blob_publisher_tx,
        metrics.clone(),
    );
    // Wire the backpressure channel from monitor to downloader.
    if let Some(downloader_pause_tx) = downloader_pause_tx {
        monitor.set_downloader_pause_channel(downloader_pause_tx);
    }
    // Wire the watermark channel from monitor to ingestion service.
    if let Some(watermark_tx) = watermark_tx {
        monitor.set_watermark_channel(watermark_tx);
    }
    let monitor_handle = monitor.start(initial_checkpoint, checkpoint_receiver);

    // Start the REST API server.
    let rest_api_server = RestApiServer::new(
        config.rest_api_address,
        archival_state.clone(),
        walrus_read_client.clone(),
        Some(config.archival_state_snapshot.clone()),
        config.client_config_path.clone(),
        config.context.clone(),
    );
    let rest_api_handle = tokio::spawn(async move { rest_api_server.start().await });

    // Start the checkpoint blob extender.
    let system_object_id = client_config.contract_config.system_object;
    let blob_extender = CheckpointBlobExtender::new(
        archival_state.clone(),
        sui_interactive_client.clone(),
        config.checkpoint_blob_extender.clone(),
        metrics.clone(),
        config.archival_state_snapshot.contract_package_id,
        system_object_id,
        config.archival_state_snapshot.wal_token_package_id,
    );
    let blob_extender_handle = tokio::spawn(async move { blob_extender.start().await });

    // Start the archival state snapshot creator if configured.
    let snapshot_creator_handle = {
        tracing::info!("starting archival state snapshot creator");

        let snapshot_creator = ArchivalStateSnapshotCreator::new(
            archival_state.clone(),
            sui_interactive_client.clone(),
            config.archival_state_snapshot.clone(),
            metrics.clone(),
        )
        .await?;

        tokio::spawn(async move { snapshot_creator.run().await })
    };

    select! {
        checkpoint_downloading_driver_result = checkpoint_downloading_driver_handle => {
            tracing::info!("checkpoint downloading driver stopped: {:?}", checkpoint_downloading_driver_result);
            if let Err(e) = checkpoint_downloading_driver_result {
                tracing::error!("checkpoint downloading driver failed: {}", e);
                return Err(anyhow::anyhow!("checkpoint downloading driver failed: {}", e));
            }
        }
        monitor_result = monitor_handle => {
            tracing::info!("checkpoint monitor stopped: {:?}", monitor_result);
            if let Err(e) = monitor_result {
                tracing::error!("checkpoint monitor failed: {}", e);
                return Err(anyhow::anyhow!("checkpoint monitor failed: {}", e));
            }
        }
        blob_publisher_result = blob_publisher_handle => {
            tracing::info!("checkpoint blob publisher stopped: {:?}", blob_publisher_result);
            if let Err(e) = blob_publisher_result {
                tracing::error!("checkpoint blob publisher failed: {}", e);
                return Err(anyhow::anyhow!("checkpoint blob publisher failed: {}", e));
            }
        }
        rest_api_result = rest_api_handle => {
            tracing::info!("REST API server stopped: {:?}", rest_api_result);
            if let Err(e) = rest_api_result {
                tracing::error!("REST API server failed: {}", e);
                return Err(anyhow::anyhow!("REST API server failed: {}", e));
            }
        }
        blob_extender_result = blob_extender_handle => {
            tracing::info!("checkpoint blob extender stopped: {:?}", blob_extender_result);
            if let Err(e) = blob_extender_result {
                tracing::error!("checkpoint blob extender failed: {}", e);
                return Err(anyhow::anyhow!("checkpoint blob extender failed: {}", e));
            }
        }
        snapshot_creator_result = snapshot_creator_handle => {
            tracing::info!("archival state snapshot creator stopped: {:?}", snapshot_creator_result);
            if let Err(e) = snapshot_creator_result {
                tracing::error!("archival state snapshot creator failed: {}", e);
                return Err(anyhow::anyhow!("archival state snapshot creator failed: {}", e));
            }
        }
    }

    Ok(())
}

async fn initialize_walrus_client(
    client_config: ClientConfig,
) -> Result<WalrusNodeClient<SuiContractClient>> {
    let sui_client = client_config
        .new_contract_client_with_wallet_in_config(None)
        .await?;
    let walrus_client =
        WalrusNodeClient::new_contract_client_with_refresher(client_config, sui_client).await?;
    Ok(walrus_client)
}

async fn initialize_walrus_read_client(
    client_config: ClientConfig,
    walrus_client: &WalrusNodeClient<SuiContractClient>,
) -> Result<WalrusNodeClient<SuiReadClient>> {
    let read_client = walrus_client.sui_client().read_client().clone();
    let walrus_read_client =
        WalrusNodeClient::new_read_client_with_refresher(client_config, read_client).await?;
    Ok(walrus_read_client)
}

async fn cleanup_orphaned_downloaded_checkpoints_and_uploaded_blobs(
    initial_checkpoint: CheckpointSequenceNumber,
    downloaded_checkpoint_dir: PathBuf,
) -> Result<()> {
    tracing::info!(
        "cleaning up orphaned downloaded checkpoints before checkpoint {}",
        initial_checkpoint
    );

    // Read all files in the downloaded checkpoint directory.
    let entries = match tokio::fs::read_dir(&downloaded_checkpoint_dir).await {
        Ok(entries) => entries,
        Err(e) => {
            tracing::warn!(
                "failed to read downloaded checkpoint directory {}: {}",
                downloaded_checkpoint_dir.display(),
                e
            );
            return Ok(());
        }
    };

    let mut entries = entries;
    let mut removed_count = 0;

    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();

        // Skip if not a file.
        if !path.is_file() {
            continue;
        }

        // Get the file name.
        let file_name = match path.file_name() {
            Some(name) => name.to_string_lossy(),
            None => continue,
        };

        // Try to parse the file name as a checkpoint number.
        let checkpoint_num = match file_name.parse::<u64>() {
            Ok(num) => num,
            Err(_) => {
                tracing::warn!("failed to parse checkpoint file name as u64: {}", file_name);
                continue;
            }
        };

        // Remove the file if it's before the initial checkpoint.
        if checkpoint_num < initial_checkpoint {
            match tokio::fs::remove_file(&path).await {
                Ok(_) => {
                    removed_count += 1;
                    tracing::debug!("removed orphaned checkpoint file: {}", path.display());
                }
                Err(e) => {
                    tracing::error!(
                        "failed to remove orphaned checkpoint file {}: {}",
                        path.display(),
                        e
                    );
                }
            }
        }
    }

    if removed_count > 0 {
        tracing::info!(
            "removed {} orphaned checkpoint files before checkpoint {}",
            removed_count,
            initial_checkpoint
        );
    }

    Ok(())
}
