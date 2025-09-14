use crate::{checkpoint_downloader, config::Config};
use anyhow::Result;
use sui_types::messages_checkpoint::CheckpointSequenceNumber;

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

    let downloader = checkpoint_downloader::CheckpointDownloader::new(
        config.checkpoint_downloader.clone(),
    );
    downloader.start(initial_checkpoint).await?;

    Ok(())
}
