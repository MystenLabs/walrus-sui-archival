use anyhow::Result;
use sui_types::messages_checkpoint::CheckpointSequenceNumber;
use tokio::{select, sync::mpsc};

use crate::{
    checkpoint_blob_builder::{self, BlobBuildRequest},
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

    let initial_checkpoint = CheckpointSequenceNumber::from(240000000u64);

    // Create channel for blob build requests.
    let (blob_builder_tx, blob_builder_rx) = mpsc::channel::<BlobBuildRequest>(100);

    // Start the checkpoint blob builder.
    let blob_builder = checkpoint_blob_builder::CheckpointBlobBuilder::new(
        config.checkpoint_blob_builder.clone(),
        config
            .checkpoint_downloader
            .downloaded_checkpoint_dir
            .clone()
            .into(),
    )?;
    let blob_builder_handle =
        tokio::spawn(async move { blob_builder.start(blob_builder_rx).await });

    // Start the checkpoint downloader and get the receiver.
    let downloader =
        checkpoint_downloader::CheckpointDownloader::new(config.checkpoint_downloader.clone());
    let (checkpoint_receiver, checkpoint_downloading_driver_handle) =
        downloader.start(initial_checkpoint).await?;

    // Start the checkpoint monitor with the receiver.
    let monitor = checkpoint_monitor::CheckpointMonitor::new(
        config.checkpoint_monitor.clone(),
        blob_builder_tx,
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
        blob_builder_result = blob_builder_handle => {
            tracing::info!("checkpoint blob builder stopped: {:?}", blob_builder_result);
            if let Err(e) = blob_builder_result {
                tracing::error!("checkpoint blob builder failed: {}", e);
                return Err(anyhow::anyhow!("checkpoint blob builder failed: {}", e));
            }
        }
    }

    Ok(())
}
