use std::{fs, path::Path, time::Duration};

use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Path to the RocksDB database directory.
    #[serde(default = "default_db_path")]
    pub db_path: String,

    /// Total number of threads for the tokio runtime thread pool.
    #[serde(default = "default_thread_pool_size")]
    pub thread_pool_size: usize,

    /// Configuration for the checkpoint downloader.
    pub checkpoint_downloader: CheckpointDownloaderConfig,

    /// Configuration for the checkpoint monitor.
    #[serde(default)]
    pub checkpoint_monitor: CheckpointMonitorConfig,

    /// Configuration for the checkpoint blob builder.
    #[serde(default)]
    pub checkpoint_blob_builder: CheckpointBlobBuilderConfig,
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

fn default_db_path() -> String {
    "archival_db".to_string()
}

fn default_thread_pool_size() -> usize {
    4
}

fn default_num_workers() -> usize {
    20
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointMonitorConfig {
    /// Maximum time duration for accumulating checkpoints before creating a blob.
    /// Default: 1 hour.
    #[serde(
        default = "default_max_accumulation_duration",
        with = "humantime_serde"
    )]
    pub max_accumulation_duration: Duration,

    /// Maximum size in bytes for accumulated checkpoints before creating a blob.
    /// Default: 10 GB.
    #[serde(default = "default_max_accumulation_size_bytes")]
    pub max_accumulation_size_bytes: u64,
}

impl Default for CheckpointMonitorConfig {
    fn default() -> Self {
        Self {
            max_accumulation_duration: default_max_accumulation_duration(),
            max_accumulation_size_bytes: default_max_accumulation_size_bytes(),
        }
    }
}

fn default_max_accumulation_duration() -> Duration {
    // Duration::from_secs(3600) // 1 hour.
    Duration::from_secs(60) // 1 minute.
}

fn default_max_accumulation_size_bytes() -> u64 {
    5 * 1024 * 1024 * 1024 // 5 GB.
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointBlobBuilderConfig {
    /// Directory to store checkpoint blob files.
    /// Default: "checkpoint_blobs".
    #[serde(default = "default_checkpoint_blobs_dir")]
    pub checkpoint_blobs_dir: String,

    /// Number of shards for Walrus blob encoding.
    /// Default: 1000.
    #[serde(default = "default_n_shards")]
    pub n_shards: u16,
}

impl Default for CheckpointBlobBuilderConfig {
    fn default() -> Self {
        Self {
            checkpoint_blobs_dir: default_checkpoint_blobs_dir(),
            n_shards: default_n_shards(),
        }
    }
}

fn default_checkpoint_blobs_dir() -> String {
    "checkpoint_blobs".to_string()
}

fn default_n_shards() -> u16 {
    1000
}
