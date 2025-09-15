// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{path::PathBuf, sync::Arc};

use anyhow::Result;
use sui_types::messages_checkpoint::CheckpointSequenceNumber;
use tokio::{select, sync::mpsc};
use walrus_sdk::{client::WalrusNodeClient, config::ClientConfig, sui::client::SuiContractClient};

use crate::{
    archival_state::ArchivalState,
    checkpoint_blob_publisher::{self, BlobBuildRequest},
    checkpoint_downloader,
    checkpoint_monitor,
    config::Config,
};

pub fn run_sui_archival(config: Config) -> Result<()> {
    tracing::info!("starting sui archival process...");

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(config.thread_pool_size)
        .enable_all()
        .build()?;

    runtime.block_on(async { run_application_logic(config).await })
}

async fn run_application_logic(config: Config) -> Result<()> {
    tracing::info!("starting application logic in multi-thread runtime");

    // Initialize the archival state with RocksDB.
    let archival_state = std::sync::Arc::new(ArchivalState::open(&config.db_path, false)?);
    tracing::info!(
        "initialized archival state with database at {:?}",
        config.db_path
    );

    // Initialize walrus client.
    let walrus_client = Arc::new(initialize_walrus_client(config.clone()).await?);

    // TODO(zhe): remove testing initial checkpoint.
    let initial_checkpoint = archival_state
        .get_latest_stored_checkpoint()?
        .unwrap_or(CheckpointSequenceNumber::from(239999999u64))
        + 1;

    // Cleanup all the old downloaded checkpoints before initial_checkpoint.
    cleanup_orphaned_downloaded_checkpoints_and_uploaded_blobs(
        initial_checkpoint,
        config
            .checkpoint_downloader
            .downloaded_checkpoint_dir
            .clone(),
    )
    .await?;

    tracing::info!("initial checkpoint: {}", initial_checkpoint);

    // Create channel for blob build requests.
    let (blob_publisher_tx, blob_publisher_rx) = mpsc::channel::<BlobBuildRequest>(100);

    // Start the checkpoint blob publisher.
    let blob_publisher = checkpoint_blob_publisher::CheckpointBlobPublisher::new(
        archival_state.clone(),
        walrus_client.clone(),
        config.checkpoint_blob_publisher.clone(),
        config
            .checkpoint_downloader
            .downloaded_checkpoint_dir
            .clone(),
    )
    .await?;
    let blob_publisher_handle =
        tokio::spawn(async move { blob_publisher.start(blob_publisher_rx).await });

    // Start the checkpoint downloader and get the receiver.
    let downloader =
        checkpoint_downloader::CheckpointDownloader::new(config.checkpoint_downloader.clone());
    let (checkpoint_receiver, checkpoint_downloading_driver_handle) =
        downloader.start(initial_checkpoint).await?;

    // Start the checkpoint monitor with the receiver.
    let monitor = checkpoint_monitor::CheckpointMonitor::new(
        config.checkpoint_monitor.clone(),
        blob_publisher_tx,
    );
    let monitor_handle = monitor.start(initial_checkpoint, checkpoint_receiver);

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
    }

    Ok(())
}

async fn initialize_walrus_client(config: Config) -> Result<WalrusNodeClient<SuiContractClient>> {
    let (client_config, _) =
        ClientConfig::load_from_multi_config(config.client_config_path, Some("testnet"))?;
    let sui_client = client_config
        .new_contract_client_with_wallet_in_config(None)
        .await?;
    let walrus_client =
        WalrusNodeClient::new_contract_client_with_refresher(client_config, sui_client).await?;
    Ok(walrus_client)
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
