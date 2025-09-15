use std::{
    fs,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::Result;
use serde::{Deserialize, Serialize};
use walrus_core::EpochCount;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Path to the client config file.
    #[serde(default = "default_client_config_path")]
    pub client_config_path: PathBuf,

    /// Path to the RocksDB database directory.
    #[serde(default = "default_db_path")]
    pub db_path: PathBuf,

    /// Total number of threads for the tokio runtime thread pool.
    #[serde(default = "default_thread_pool_size")]
    pub thread_pool_size: usize,

    /// Configuration for the checkpoint downloader.
    pub checkpoint_downloader: CheckpointDownloaderConfig,

    /// Configuration for the checkpoint monitor.
    #[serde(default)]
    pub checkpoint_monitor: CheckpointMonitorConfig,

    /// Configuration for the checkpoint blob publisher.
    #[serde(default)]
    pub checkpoint_blob_publisher: CheckpointBlobPublisherConfig,
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
    pub downloaded_checkpoint_dir: PathBuf,

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

fn default_client_config_path() -> PathBuf {
    ["./", "config", "wallet", "client_config.yaml"]
        .iter()
        .collect()
}

fn default_db_path() -> PathBuf {
    ["./", "archival_db"].iter().collect()
}

fn default_thread_pool_size() -> usize {
    4
}

fn default_num_workers() -> usize {
    20
}

fn default_downloaded_checkpoint_dir() -> PathBuf {
    ["./", "downloaded_checkpoint_dir"].iter().collect()
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
pub struct CheckpointBlobPublisherConfig {
    /// Directory to store checkpoint blob files.
    /// Default: "checkpoint_blobs".
    #[serde(default = "default_checkpoint_blobs_dir")]
    pub checkpoint_blobs_dir: PathBuf,

    /// Number of shards for Walrus blob encoding.
    /// Default: 1000.
    #[serde(default = "default_n_shards")]
    pub n_shards: u16,

    /// Number of epochs to store in the database.
    #[serde(default = "default_store_epoch_length")]
    pub store_epoch_length: EpochCount,

    /// Minimum retry duration for blob upload.
    #[serde(default = "default_min_retry_duration", with = "humantime_serde")]
    pub min_retry_duration: Duration,

    /// Maximum retry duration for blob upload.
    #[serde(default = "default_max_retry_duration", with = "humantime_serde")]
    pub max_retry_duration: Duration,
}

impl Default for CheckpointBlobPublisherConfig {
    fn default() -> Self {
        Self {
            checkpoint_blobs_dir: default_checkpoint_blobs_dir(),
            n_shards: default_n_shards(),
            store_epoch_length: default_store_epoch_length(),
            min_retry_duration: default_min_retry_duration(),
            max_retry_duration: default_max_retry_duration(),
        }
    }
}

fn default_checkpoint_blobs_dir() -> PathBuf {
    ["./", "checkpoint_blobs"].iter().collect()
}

fn default_n_shards() -> u16 {
    1000
}

fn default_store_epoch_length() -> EpochCount {
    5
}

fn default_min_retry_duration() -> Duration {
    Duration::from_secs(30)
}

fn default_max_retry_duration() -> Duration {
    Duration::from_secs(300)
}
