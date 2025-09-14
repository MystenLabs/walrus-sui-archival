use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use async_channel::Receiver;
use reqwest::Url;
use sui_storage::blob::Blob;
use sui_types::full_checkpoint_content::CheckpointData;
use sui_types::messages_checkpoint::CheckpointSequenceNumber;
use tokio::{fs, sync, task, time};

use crate::config::CheckpointDownloaderConfig;

#[derive(Debug, Clone)]
pub struct CheckpointInfo {
    pub checkpoint_number: CheckpointSequenceNumber,
    pub epoch: u64,
    pub is_end_of_epoch: bool,
    pub timestamp_ms: u64,
    pub checkpoint_byte_size: usize,
}

pub struct CheckpointDownloadWorker {
    worker_id: usize,
    rx: Receiver<CheckpointSequenceNumber>,
    tx: sync::mpsc::Sender<CheckpointInfo>,
    bucket_base_url: Url,
    downloaded_checkpoint_dir: PathBuf,
    client: reqwest::Client,
    min_retry_wait: Duration,
    max_retry_wait: Duration,
}

impl CheckpointDownloadWorker {
    pub fn new(
        worker_id: usize,
        rx: Receiver<CheckpointSequenceNumber>,
        tx: sync::mpsc::Sender<CheckpointInfo>,
        bucket_base_url: Url,
        downloaded_checkpoint_dir: PathBuf,
        min_retry_wait: Duration,
        max_retry_wait: Duration,
    ) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("should be able to build reqwest client");
        Self {
            worker_id,
            rx,
            tx,
            bucket_base_url,
            downloaded_checkpoint_dir,
            client,
            min_retry_wait,
            max_retry_wait,
        }
    }

    pub async fn start(self) {
        tracing::debug!("worker {} started", self.worker_id);

        while let Ok(checkpoint_number) = self.rx.recv().await {
            tracing::debug!(
                "worker {} downloading checkpoint {}",
                self.worker_id,
                checkpoint_number
            );

            match self.download_checkpoint(checkpoint_number).await {
                Ok(checkpoint_info) => {
                    if let Err(e) = self.tx.send(checkpoint_info).await {
                        tracing::debug!("worker {} failed to send result: {}", self.worker_id, e);
                        break;
                    }
                }
                Err(e) => {
                    tracing::debug!(
                        "worker {} failed to download checkpoint {}: {}",
                        self.worker_id,
                        checkpoint_number,
                        e
                    );
                }
            }
        }

        tracing::debug!("worker {} stopped", self.worker_id);
    }

    async fn download_checkpoint(
        &self,
        checkpoint_number: CheckpointSequenceNumber,
    ) -> Result<CheckpointInfo> {
        let url = self
            .bucket_base_url
            .join(&format!("{checkpoint_number}.chk"))?;

        let mut retry_count = 0;
        let mut wait_duration = self.min_retry_wait;

        loop {
            tracing::debug!(
                %url,
                retry_count,
                "downloading checkpoint from bucket"
            );

            // We are doing unlimited retries here since we cannot miss any checkpoint.
            match self.try_download_checkpoint(&url).await {
                Ok((bytes, checkpoint)) => {
                    // Create checkpoint info.
                    let checkpoint_info = CheckpointInfo {
                        checkpoint_number,
                        epoch: checkpoint.checkpoint_summary.epoch,
                        is_end_of_epoch: checkpoint.checkpoint_summary.end_of_epoch_data.is_some(),
                        timestamp_ms: checkpoint.checkpoint_summary.timestamp_ms,
                        checkpoint_byte_size: bytes.len(),
                    };

                    // Write checkpoint to disk.
                    let checkpoint_file = self
                        .downloaded_checkpoint_dir
                        .join(format!("{}.chk", checkpoint_number));
                    fs::write(&checkpoint_file, &bytes).await?;

                    tracing::debug!(checkpoint_number, "checkpoint download and save successful");
                    return Ok(checkpoint_info);
                }
                Err(e) => {
                    retry_count += 1;
                    tracing::debug!(
                        checkpoint_number,
                        retry_count,
                        wait_duration_ms = wait_duration.as_millis(),
                        "failed to download checkpoint, retrying after wait: {}",
                        e
                    );

                    // Wait before retrying.
                    time::sleep(wait_duration).await;

                    // Exponential backoff with max cap.
                    wait_duration = std::cmp::min(wait_duration * 2, self.max_retry_wait);
                }
            }
        }
    }

    async fn try_download_checkpoint(&self, url: &Url) -> Result<(Vec<u8>, CheckpointData)> {
        let response = self
            .client
            .get(url.clone())
            .send()
            .await?
            .error_for_status()?;
        let bytes = response.bytes().await?;
        let bytes_vec = bytes.to_vec();
        let checkpoint = Blob::from_bytes::<CheckpointData>(&bytes_vec)
            .map_err(|e| anyhow::anyhow!("failed to deserialize checkpoint: {}", e))?;
        Ok((bytes_vec, checkpoint))
    }
}

pub struct CheckpointDownloader {
    num_workers: usize,
    worker_handles: Vec<task::JoinHandle<()>>,
    bucket_base_url: Url,
    downloaded_checkpoint_dir: PathBuf,
    min_retry_wait: Duration,
    max_retry_wait: Duration,
}

impl CheckpointDownloader {
    pub fn new(config: CheckpointDownloaderConfig) -> Self {
        Self {
            num_workers: config.num_workers,
            worker_handles: Vec::new(),
            bucket_base_url: Url::parse(&config.bucket_base_url).expect("invalid bucket base URL"),
            downloaded_checkpoint_dir: PathBuf::from(config.downloaded_checkpoint_dir),
            min_retry_wait: config.min_download_retry_wait,
            max_retry_wait: config.max_download_retry_wait,
        }
    }

    pub async fn start(mut self, initial_checkpoint: CheckpointSequenceNumber) -> Result<()> {
        tracing::info!(
            "starting checkpoint downloader from checkpoint {} with {} workers",
            initial_checkpoint,
            self.num_workers
        );

        // Create the directory if it doesn't exist.
        fs::create_dir_all(&self.downloaded_checkpoint_dir).await?;

        let (download_tx, download_rx) = async_channel::bounded::<CheckpointSequenceNumber>(100);
        let (result_tx, result_rx) = sync::mpsc::channel::<CheckpointInfo>(100);

        for worker_id in 0..self.num_workers {
            let worker_rx = download_rx.clone();
            let worker_tx = result_tx.clone();
            let bucket_base_url = self.bucket_base_url.clone();
            let downloaded_checkpoint_dir = self.downloaded_checkpoint_dir.clone();
            let min_retry_wait = self.min_retry_wait;
            let max_retry_wait = self.max_retry_wait;

            let worker = CheckpointDownloadWorker::new(
                worker_id,
                worker_rx,
                worker_tx,
                bucket_base_url,
                downloaded_checkpoint_dir,
                min_retry_wait,
                max_retry_wait,
            );

            let handle = tokio::spawn(async move {
                worker.start().await;
            });
            self.worker_handles.push(handle);
        }

        let driver_task = self.download_checkpoint_driver(download_tx, initial_checkpoint);
        let receiver_task = self.download_checkpoint_receiver(result_rx);

        tokio::select! {
            driver_result = driver_task => {
                tracing::info!("driver task stopped with result {:?}", driver_result);
                if let Err(e) = driver_result {
                    tracing::error!("driver task failed with error: {}", e);
                    return Err(e);
                }
            }
            receiver_result = receiver_task => {
                tracing::info!("receiver task stopped with result {:?}", receiver_result);
                if let Err(e) = receiver_result {
                    tracing::error!("receiver task failed with error: {}", e);
                    return Err(e);
                }
            }
        }

        tracing::info!("checkpoint downloader stopped");
        Ok(())
    }

    async fn download_checkpoint_driver(
        &self,
        download_tx: async_channel::Sender<CheckpointSequenceNumber>,
        initial_checkpoint: CheckpointSequenceNumber,
    ) -> Result<()> {
        tokio::spawn(async move {
            let mut current_checkpoint = initial_checkpoint;
            loop {
                if let Err(e) = download_tx.send(current_checkpoint).await {
                    tracing::debug!(
                        "failed to send checkpoint number {}: {}",
                        current_checkpoint,
                        e
                    );
                    break;
                }
                current_checkpoint = current_checkpoint + 1;
                time::sleep(time::Duration::from_millis(100)).await;
            }
        })
        .await?;
        Ok(())
    }

    async fn download_checkpoint_receiver(
        &self,
        mut result_rx: sync::mpsc::Receiver<CheckpointInfo>,
    ) -> Result<()> {
        while let Some(checkpoint_info) = result_rx.recv().await {
            tracing::debug!(
                "received checkpoint {} timestamp {} epoch {} is end of epoch {} size {}",
                checkpoint_info.checkpoint_number,
                checkpoint_info.timestamp_ms,
                checkpoint_info.epoch,
                checkpoint_info.is_end_of_epoch,
                checkpoint_info.checkpoint_byte_size,
            );

            if checkpoint_info.checkpoint_number > 5 {
                break;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mockito::Server;
    use sui_storage::blob::BlobEncoding;
    use tempfile::TempDir;

    fn create_test_checkpoint_data(checkpoint_number: u64) -> Vec<u8> {
        // Load real checkpoint data from Sui testnet and modify the checkpoint number.
        // This ensures we're testing with valid checkpoint data structure.
        let test_data_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("test_data")
            .join("checkpoint_1.chk");

        let checkpoint_bytes = std::fs::read(test_data_path).expect(
            "Failed to read test checkpoint data. Make sure test_data/checkpoint_1.chk exists",
        );

        // Deserialize, modify checkpoint number, and re-serialize
        let mut checkpoint_data = Blob::from_bytes::<CheckpointData>(&checkpoint_bytes)
            .expect("Failed to deserialize test checkpoint data");

        // Modify the checkpoint number in the summary
        // We need to access the inner data of the envelope
        let summary = checkpoint_data.checkpoint_summary.data_mut_for_testing();
        summary.sequence_number = checkpoint_number;

        // Serialize back to bytes
        Blob::encode(&checkpoint_data, BlobEncoding::Bcs)
            .expect("Failed to serialize modified checkpoint data")
            .to_bytes()
    }

    #[tokio::test]
    async fn test_checkpoint_download_worker_successful_download() {
        tracing_subscriber::fmt::init();
        // Setup test environment.
        let temp_dir = TempDir::new().unwrap();
        let checkpoint_dir = temp_dir.path().to_path_buf();

        // Create channels for communication.
        let (tx, rx) = async_channel::bounded(10);
        let (result_tx, mut result_rx) = sync::mpsc::channel(10);

        // Create test checkpoint data.
        let checkpoint_number = 42;
        let checkpoint_bytes = create_test_checkpoint_data(checkpoint_number);

        // Setup mock server.
        let mut server = Server::new_async().await;
        let _m = server
            .mock("GET", format!("/{}.chk", checkpoint_number).as_str())
            .with_status(200)
            .with_body(&checkpoint_bytes)
            .create();

        // Create and start worker.
        let worker = CheckpointDownloadWorker::new(
            0,
            rx,
            result_tx,
            Url::parse(&server.url()).unwrap(),
            checkpoint_dir.clone(),
            Duration::from_millis(100),
            Duration::from_secs(1),
        );

        // Send checkpoint number to download.
        tx.send(checkpoint_number).await.unwrap();
        drop(tx); // Close channel to stop worker.

        // Start worker in background.
        let worker_handle = tokio::spawn(async move {
            worker.start().await;
        });

        // Verify result.
        let checkpoint_info = result_rx.recv().await.unwrap();
        assert_eq!(checkpoint_info.checkpoint_number, checkpoint_number);
        // The actual epoch and timestamp will come from the real checkpoint data
        // Just verify the timestamp is reasonable
        assert!(checkpoint_info.timestamp_ms > 0);
        assert_eq!(checkpoint_info.checkpoint_byte_size, checkpoint_bytes.len());

        // Verify file was written.
        let checkpoint_file = checkpoint_dir.join(format!("{}.chk", checkpoint_number));
        assert!(checkpoint_file.exists());
        let saved_bytes = fs::read(&checkpoint_file).await.unwrap();
        assert_eq!(saved_bytes, checkpoint_bytes);

        worker_handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_checkpoint_download_worker_retry_on_failure() {
        tracing_subscriber::fmt::init();
        // Setup test environment.
        let temp_dir = TempDir::new().unwrap();
        let checkpoint_dir = temp_dir.path().to_path_buf();

        // Create channels for communication.
        let (tx, rx) = async_channel::bounded(10);
        let (result_tx, mut result_rx) = sync::mpsc::channel(10);

        // Create test checkpoint data.
        let checkpoint_number = 42;
        let checkpoint_bytes = create_test_checkpoint_data(checkpoint_number);

        // Setup mock server with initial failures.
        let mut server = Server::new_async().await;
        let _m1 = server
            .mock("GET", format!("/{}.chk", checkpoint_number).as_str())
            .with_status(500)
            .expect(2) // Expect 2 failures.
            .create();

        let _m2 = server
            .mock("GET", format!("/{}.chk", checkpoint_number).as_str())
            .with_status(200)
            .with_body(&checkpoint_bytes)
            .expect(1) // Then succeed.
            .create();

        // Create and start worker with short retry intervals.
        let worker = CheckpointDownloadWorker::new(
            0,
            rx,
            result_tx,
            Url::parse(&server.url()).unwrap(),
            checkpoint_dir.clone(),
            Duration::from_millis(10), // Short retry for testing.
            Duration::from_millis(100),
        );

        // Send checkpoint number to download.
        tx.send(checkpoint_number).await.unwrap();
        drop(tx); // Close channel to stop worker.

        // Start worker in background.
        let worker_handle = tokio::spawn(async move {
            worker.start().await;
        });

        // Verify result after retries.
        let checkpoint_info = result_rx.recv().await.unwrap();
        assert_eq!(checkpoint_info.checkpoint_number, checkpoint_number);

        // Verify file was written.
        let checkpoint_file = checkpoint_dir.join(format!("{}.chk", checkpoint_number));
        assert!(checkpoint_file.exists());

        worker_handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_checkpoint_download_worker_multiple_checkpoints() {
        tracing_subscriber::fmt::init();
        // Setup test environment.
        let temp_dir = TempDir::new().unwrap();
        let checkpoint_dir = temp_dir.path().to_path_buf();

        // Create channels for communication.
        let (tx, rx) = async_channel::bounded(10);
        let (result_tx, mut result_rx) = sync::mpsc::channel(10);

        // Setup mock server for multiple checkpoints.
        let mut server = Server::new_async().await;
        let checkpoints = vec![1, 2, 3];
        let mut mocks = Vec::new();

        for &checkpoint_number in &checkpoints {
            let checkpoint_bytes = create_test_checkpoint_data(checkpoint_number);

            let m = server
                .mock("GET", format!("/{}.chk", checkpoint_number).as_str())
                .with_status(200)
                .with_body(checkpoint_bytes)
                .create();
            mocks.push(m);
        }

        // Create and start worker.
        let worker = CheckpointDownloadWorker::new(
            0,
            rx,
            result_tx,
            Url::parse(&server.url()).unwrap(),
            checkpoint_dir.clone(),
            Duration::from_millis(100),
            Duration::from_secs(1),
        );

        // Send multiple checkpoint numbers.
        for &checkpoint_number in &checkpoints {
            tx.send(checkpoint_number).await.unwrap();
        }
        drop(tx); // Close channel to stop worker.

        // Start worker in background.
        let worker_handle = tokio::spawn(async move {
            worker.start().await;
        });

        // Verify all results.
        for &expected_checkpoint in &checkpoints {
            let checkpoint_info = result_rx.recv().await.unwrap();
            assert_eq!(checkpoint_info.checkpoint_number, expected_checkpoint);

            // Verify file was written.
            let checkpoint_file = checkpoint_dir.join(format!("{}.chk", expected_checkpoint));
            assert!(checkpoint_file.exists());
        }

        worker_handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_checkpoint_downloader_initialization() {
        tracing_subscriber::fmt::init();
        let config = CheckpointDownloaderConfig {
            num_workers: 4,
            bucket_base_url: "https://example.com/bucket/".to_string(),
            downloaded_checkpoint_dir: "/tmp/checkpoints".to_string(),
            min_download_retry_wait: Duration::from_millis(100),
            max_download_retry_wait: Duration::from_secs(10),
        };
        let downloader = CheckpointDownloader::new(config);

        assert_eq!(downloader.num_workers, 4);
        assert_eq!(
            downloader.bucket_base_url.as_str(),
            "https://example.com/bucket/"
        );
        assert_eq!(
            downloader.downloaded_checkpoint_dir,
            PathBuf::from("/tmp/checkpoints")
        );
        assert_eq!(downloader.min_retry_wait, Duration::from_millis(100));
        assert_eq!(downloader.max_retry_wait, Duration::from_secs(10));
    }

    #[tokio::test]
    async fn test_checkpoint_downloader_with_multiple_workers() {
        tracing_subscriber::fmt::init();
        // Setup test environment.
        let temp_dir = TempDir::new().unwrap();
        let checkpoint_dir = temp_dir.path().to_path_buf();

        // Setup mock server for checkpoints 0-6.
        let mut server = Server::new_async().await;
        let mut mocks = Vec::new();
        for checkpoint_number in 0..=6 {
            let checkpoint_bytes = create_test_checkpoint_data(checkpoint_number);

            let m = server
                .mock("GET", format!("/{}.chk", checkpoint_number).as_str())
                .with_status(200)
                .with_body(checkpoint_bytes)
                .create();
            mocks.push(m);
        }

        // Create and start downloader.
        let config = CheckpointDownloaderConfig {
            num_workers: 2,
            bucket_base_url: server.url(),
            downloaded_checkpoint_dir: checkpoint_dir.to_str().unwrap().to_string(),
            min_download_retry_wait: Duration::from_millis(10),
            max_download_retry_wait: Duration::from_millis(100),
        };
        let downloader = CheckpointDownloader::new(config);

        // Start downloader (it will stop after checkpoint 5 based on the receiver logic).
        let result = downloader.start(0).await;
        assert!(result.is_ok());

        // Verify files were written.
        for checkpoint_number in 0..=5 {
            let checkpoint_file = checkpoint_dir.join(format!("{}.chk", checkpoint_number));
            assert!(checkpoint_file.exists());
        }
    }

    #[tokio::test]
    async fn test_checkpoint_info_creation() {
        tracing_subscriber::fmt::init();
        let checkpoint_info = CheckpointInfo {
            checkpoint_number: 100,
            epoch: 5,
            is_end_of_epoch: true,
            timestamp_ms: 1234567890,
            checkpoint_byte_size: 4096,
        };

        assert_eq!(checkpoint_info.checkpoint_number, 100);
        assert_eq!(checkpoint_info.epoch, 5);
        assert!(checkpoint_info.is_end_of_epoch);
        assert_eq!(checkpoint_info.timestamp_ms, 1234567890);
        assert_eq!(checkpoint_info.checkpoint_byte_size, 4096);
    }

    #[tokio::test]
    async fn test_checkpoint_download_worker_invalid_checkpoint_data() {
        tracing_subscriber::fmt::init();

        // Setup test environment.
        let temp_dir = TempDir::new().unwrap();
        let checkpoint_dir = temp_dir.path().to_path_buf();

        // Create channels for communication.
        let (tx, rx) = async_channel::bounded(10);
        let (result_tx, mut result_rx) = sync::mpsc::channel(10);

        let checkpoint_number = 42;

        // Setup mock server with invalid data.
        let mut server = Server::new_async().await;
        let _m = server
            .mock("GET", format!("/{}.chk", checkpoint_number).as_str())
            .with_status(200)
            .with_body(b"invalid checkpoint data")
            .create();

        // Create and start worker with very short retry intervals.
        let worker = CheckpointDownloadWorker::new(
            0,
            rx,
            result_tx,
            Url::parse(&server.url()).unwrap(),
            checkpoint_dir.clone(),
            Duration::from_millis(1), // Very short for testing.
            Duration::from_millis(2),
        );

        // Send checkpoint number to download.
        tx.send(checkpoint_number).await.unwrap();

        // Start worker in background with timeout.
        let worker_handle = tokio::spawn(async move {
            worker.start().await;
        });

        // Give it some time to retry a few times.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Drop tx to stop the worker.
        worker_handle.abort();

        // Worker should keep retrying but we stop it.
        // Verify no successful result was sent.
        assert!(result_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_checkpoint_downloader_creates_directory() {
        tracing_subscriber::fmt::init();

        // Use a temporary directory that doesn't exist yet.
        let temp_dir = TempDir::new().unwrap();
        let checkpoint_dir = temp_dir.path().join("new_checkpoint_dir");

        // Ensure directory doesn't exist.
        assert!(!checkpoint_dir.exists());

        // Setup mock server.
        let mut server = Server::new_async().await;
        let mut mocks = Vec::new();
        for checkpoint_number in 0..=6 {
            let checkpoint_bytes = create_test_checkpoint_data(checkpoint_number);

            let m = server
                .mock("GET", format!("/{}.chk", checkpoint_number).as_str())
                .with_status(200)
                .with_body(checkpoint_bytes)
                .create();
            mocks.push(m);
        }

        // Create and start downloader.
        let config = CheckpointDownloaderConfig {
            num_workers: 1,
            bucket_base_url: server.url(),
            downloaded_checkpoint_dir: checkpoint_dir.to_str().unwrap().to_string(),
            min_download_retry_wait: Duration::from_millis(10),
            max_download_retry_wait: Duration::from_millis(100),
        };
        let downloader = CheckpointDownloader::new(config);

        // Start downloader.
        let result = downloader.start(0).await;
        assert!(result.is_ok());

        // Verify directory was created.
        assert!(checkpoint_dir.exists());
        assert!(checkpoint_dir.is_dir());
    }
}
