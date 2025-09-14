use anyhow::Result;
use sui_types::messages_checkpoint::CheckpointSequenceNumber;
use tokio::select;

use crate::{checkpoint_downloader, checkpoint_monitor, config::Config};

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

    let initial_checkpoint = CheckpointSequenceNumber::from(0u64);

    // Start the checkpoint downloader and get the receiver.
    let downloader =
        checkpoint_downloader::CheckpointDownloader::new(config.checkpoint_downloader.clone());
    let (checkpoint_receiver, driver_handle) = downloader.start(initial_checkpoint).await?;

    // Start the checkpoint monitor with the receiver.
    let monitor = checkpoint_monitor::CheckpointMonitor::new(config.checkpoint_monitor.clone());
    let monitor_handle = monitor.start(initial_checkpoint, checkpoint_receiver);

    select! {
        driver_result = driver_handle => {
            tracing::info!("checkpoint driver stopped: {:?}", driver_result);
            if let Err(e) = driver_result {
                tracing::error!("checkpoint driver failed: {}", e);
                return Err(anyhow::anyhow!("checkpoint driver failed: {}", e));
            }
        }
        monitor_result = monitor_handle => {
            tracing::info!("checkpoint monitor stopped: {:?}", monitor_result);
            if let Err(e) = monitor_result {
                tracing::error!("checkpoint monitor failed: {}", e);
                return Err(anyhow::anyhow!("checkpoint monitor failed: {}", e));
            }
        }
    }

    Ok(())
}
