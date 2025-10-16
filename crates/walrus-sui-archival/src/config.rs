// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::Result;
use reqwest::Url;
use serde::{Deserialize, Serialize};
use sui_indexer_alt_framework::ingestion::{ClientArgs, IngestionConfig};
use sui_types::base_types::{ObjectID, SequenceNumber};
use walrus_core::EpochCount;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Context to use for the client config.
    pub context: String,

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
    pub checkpoint_downloader: CheckpointDownloaderType,

    /// Configuration for the checkpoint monitor.
    #[serde(default)]
    pub checkpoint_monitor: CheckpointMonitorConfig,

    /// Configuration for the checkpoint blob publisher.
    #[serde(default)]
    pub checkpoint_blob_publisher: CheckpointBlobPublisherConfig,

    /// REST API server address.
    #[serde(default = "default_rest_api_address")]
    pub rest_api_address: SocketAddr,

    /// Metrics server address.
    #[serde(default = "default_metrics_address")]
    pub metrics_address: SocketAddr,

    /// Configuration for the checkpoint blob extender.
    #[serde(default)]
    pub checkpoint_blob_extender: CheckpointBlobExtenderConfig,

    /// Configuration for the archival state snapshot.
    pub archival_state_snapshot: ArchivalStateSnapshotConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CheckpointDownloaderType {
    /// Use the original checkpoint downloader that downloads from a bucket URL.
    Bucket(CheckpointDownloaderConfig),
    /// Use the injection service checkpoint downloader.
    InjectionService(InjectionServiceCheckpointDownloaderConfig),
}

impl CheckpointDownloaderType {
    /// Get the downloaded checkpoint directory from the config.
    pub fn downloaded_checkpoint_dir(&self) -> &PathBuf {
        match self {
            CheckpointDownloaderType::Bucket(config) => &config.downloaded_checkpoint_dir,
            CheckpointDownloaderType::InjectionService(config) => &config.downloaded_checkpoint_dir,
        }
    }
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InjectionServiceCheckpointDownloaderConfig {
    /// Number of worker threads for processing checkpoints.
    #[serde(default = "default_num_workers")]
    pub num_workers: usize,

    /// Directory to store downloaded checkpoint data.
    #[serde(default = "default_downloaded_checkpoint_dir")]
    pub downloaded_checkpoint_dir: PathBuf,

    /// Remote Store to fetch checkpoints from.
    pub remote_store_url: Url,

    /// Configuration for the ingestion service.
    #[serde(default)]
    pub ingestion_config: IngestionConfig,
}

impl InjectionServiceCheckpointDownloaderConfig {
    /// Convert this config into ClientArgs for the ingestion service.
    pub fn to_client_args(&self) -> ClientArgs {
        ClientArgs {
            remote_store_url: Some(self.remote_store_url.clone()),
            local_ingestion_path: None,
            rpc_api_url: None,
            rpc_username: None,
            rpc_password: None,
        }
    }
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
    ["./", "config", "wallet", "local_client_config.yaml"]
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
            store_epoch_length: default_store_epoch_length(),
            min_retry_duration: default_min_retry_duration(),
            max_retry_duration: default_max_retry_duration(),
        }
    }
}

fn default_checkpoint_blobs_dir() -> PathBuf {
    ["./", "checkpoint_blobs"].iter().collect()
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

fn default_rest_api_address() -> SocketAddr {
    "0.0.0.0:9185".parse().unwrap()
}

fn default_metrics_address() -> SocketAddr {
    "0.0.0.0:9184".parse().unwrap()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointBlobExtenderConfig {
    /// Number of epochs to extend blobs by.
    #[serde(default = "default_extend_epoch_length")]
    pub extend_epoch_length: EpochCount,

    /// Minimum retry duration for transaction submission.
    #[serde(
        default = "default_min_transaction_retry_duration",
        with = "humantime_serde"
    )]
    pub min_transaction_retry_duration: Duration,

    /// Maximum retry duration for transaction submission.
    #[serde(
        default = "default_max_transaction_retry_duration",
        with = "humantime_serde"
    )]
    pub max_transaction_retry_duration: Duration,

    /// Interval to check for blobs to extend.
    #[serde(default = "default_check_interval", with = "humantime_serde")]
    pub check_interval: Duration,
}

impl Default for CheckpointBlobExtenderConfig {
    fn default() -> Self {
        Self {
            extend_epoch_length: default_extend_epoch_length(),
            min_transaction_retry_duration: default_min_transaction_retry_duration(),
            max_transaction_retry_duration: default_max_transaction_retry_duration(),
            check_interval: default_check_interval(),
        }
    }
}

fn default_extend_epoch_length() -> EpochCount {
    5
}

fn default_min_transaction_retry_duration() -> Duration {
    Duration::from_secs(10)
}

fn default_max_transaction_retry_duration() -> Duration {
    Duration::from_secs(120)
}

fn default_check_interval() -> Duration {
    Duration::from_secs(30 * 60)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchivalStateSnapshotConfig {
    /// Interval between creating snapshots and uploading to Walrus.
    #[serde(
        default = "default_snapshot_state_to_walrus_interval",
        with = "humantime_serde"
    )]
    pub snapshot_state_to_walrus_interval: Duration,

    /// Directory to store temporary snapshot files.
    #[serde(default = "default_snapshot_temp_dir")]
    pub snapshot_temp_dir: PathBuf,

    /// Package ID of the metadata contract.
    pub metadata_package_id: ObjectID,

    /// Object ID of the AdminCap capability object.
    pub admin_cap_object_id: ObjectID,

    /// Object ID of the MetadataBlobPointer shared object.
    pub metadata_pointer_object_id: ObjectID,

    /// Initial shared version of the MetadataBlobPointer shared object.
    pub metadata_pointer_initial_shared_version: SequenceNumber,

    /// Number of epochs to store in the database.
    #[serde(default = "default_snapshot_store_epoch_length")]
    pub store_epoch_length: EpochCount,

    /// Minimum retry duration for blob upload.
    #[serde(default = "default_min_retry_duration", with = "humantime_serde")]
    pub min_retry_duration: Duration,

    /// Maximum retry duration for blob upload.
    #[serde(default = "default_max_retry_duration", with = "humantime_serde")]
    pub max_retry_duration: Duration,
}

fn default_snapshot_state_to_walrus_interval() -> Duration {
    Duration::from_secs(3600) // 1 hour.
}

fn default_snapshot_store_epoch_length() -> EpochCount {
    2
}

fn default_snapshot_temp_dir() -> PathBuf {
    ["./", "archival_snapshots"].iter().collect()
}
