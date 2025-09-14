use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Total number of threads for the tokio runtime thread pool.
    #[serde(default = "default_thread_pool_size")]
    pub thread_pool_size: usize,

    /// Configuration for the checkpoint downloader.
    pub checkpoint_downloader: CheckpointDownloaderConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointDownloaderConfig {
    /// Base URL for the S3 bucket containing checkpoints.
    pub bucket_base_url: String,

    /// Number of worker threads for downloading checkpoints.
    #[serde(default = "default_num_workers")]
    pub num_workers: usize,

    /// Directory to store downloaded checkpoint data.
    #[serde(default = "default_downloaded_checkpoint_dir")]
    pub downloaded_checkpoint_dir: String,

    /// Minimum wait time for download retry.
    #[serde(default = "default_min_download_retry_wait", with = "humantime_serde")]
    pub min_download_retry_wait: Duration,

    /// Maximum wait time for download retry.
    #[serde(default = "default_max_download_retry_wait", with = "humantime_serde")]
    pub max_download_retry_wait: Duration,
}

impl Config {
    /// Load configuration from a file.
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        tracing::info!("loading config from {}", path.as_ref().display());
        let contents = fs::read_to_string(path)?;
        let config: Config = serde_yaml::from_str(&contents)?;
        Ok(config)
    }
}

fn default_thread_pool_size() -> usize {
    4
}

fn default_num_workers() -> usize {
    4
}

fn default_downloaded_checkpoint_dir() -> String {
    "downloaded_checkpoint_dir".to_string()
}

fn default_min_download_retry_wait() -> Duration {
    Duration::from_secs(1)
}

fn default_max_download_retry_wait() -> Duration {
    Duration::from_secs(60)
}
