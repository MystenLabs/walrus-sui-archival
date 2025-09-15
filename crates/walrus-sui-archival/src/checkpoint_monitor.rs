// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use anyhow::{Result, anyhow};
use sui_types::messages_checkpoint::CheckpointSequenceNumber;
use tokio::sync::mpsc;

use crate::{
    checkpoint_blob_publisher::BlobBuildRequest,
    checkpoint_downloader::CheckpointInfo,
    config::CheckpointMonitorConfig,
    metrics::Metrics,
};

/// Criteria for when to build a new blob.
#[derive(Debug, Clone, PartialEq)]
pub enum BlobBuildTrigger {
    /// Time duration threshold exceeded.
    TimeDurationExceeded,
    /// Size threshold exceeded.
    SizeExceeded,
    /// End of epoch checkpoint encountered.
    EndOfEpoch,
}

/// Accumulator for checkpoints that will be bundled into a blob.
#[derive(Debug)]
struct CheckpointAccumulator {
    /// List of checkpoint info accumulated so far.
    checkpoints: Vec<CheckpointInfo>,
    /// Total size of accumulated checkpoints in bytes.
    total_size_bytes: u64,
    /// Timestamp of the first checkpoint in milliseconds.
    first_checkpoint_timestamp_ms: Option<u64>,
}

impl CheckpointAccumulator {
    fn new() -> Self {
        Self {
            checkpoints: Vec::new(),
            total_size_bytes: 0,
            first_checkpoint_timestamp_ms: None,
        }
    }

    fn add(&mut self, checkpoint: CheckpointInfo) {
        if self.first_checkpoint_timestamp_ms.is_none() {
            self.first_checkpoint_timestamp_ms = Some(checkpoint.timestamp_ms);
        }
        self.total_size_bytes += checkpoint.checkpoint_byte_size as u64;
        self.checkpoints.push(checkpoint);
    }

    fn clear(&mut self) {
        self.checkpoints.clear();
        self.total_size_bytes = 0;
        self.first_checkpoint_timestamp_ms = None;
    }

    fn is_empty(&self) -> bool {
        self.checkpoints.is_empty()
    }

    /// Get the last checkpoint info if any.
    fn last(&self) -> Option<&CheckpointInfo> {
        self.checkpoints.last()
    }

    /// Check if the accumulator should trigger blob building based on the given config.
    fn should_build_blob(
        &self,
        config: &CheckpointMonitorConfig,
        current_checkpoint: &CheckpointInfo,
    ) -> Option<BlobBuildTrigger> {
        let last_checkpoint = self.last();
        // Check if this is an end of epoch checkpoint.
        if last_checkpoint.map(|c| c.is_end_of_epoch).unwrap_or(false) {
            return Some(BlobBuildTrigger::EndOfEpoch);
        }

        // Check size threshold.
        if self.total_size_bytes + current_checkpoint.checkpoint_byte_size as u64
            > config.max_accumulation_size_bytes
        {
            return Some(BlobBuildTrigger::SizeExceeded);
        }

        // Check time duration threshold.
        if let Some(first_timestamp) = self.first_checkpoint_timestamp_ms {
            let duration_ms = current_checkpoint
                .timestamp_ms
                .saturating_sub(first_timestamp);
            if duration_ms > config.max_accumulation_duration.as_millis() as u64 {
                return Some(BlobBuildTrigger::TimeDurationExceeded);
            }
        }

        None
    }
}

/// Monitors checkpoints and determines when to build blobs.
pub struct CheckpointMonitor {
    config: CheckpointMonitorConfig,
    accumulator: CheckpointAccumulator,
    /// Buffer for out-of-order checkpoints.
    pending_checkpoints: BTreeMap<CheckpointSequenceNumber, CheckpointInfo>,
    /// Next expected checkpoint number.
    next_checkpoint_number: CheckpointSequenceNumber,
    /// Last checkpoint number that was updated.
    last_updated_checkpoint_number: tokio::time::Instant,
    /// Last time we emitted a stall warning.
    last_stall_warning: tokio::time::Instant,
    /// Channel to send blob build requests to CheckpointBlobPublisher.
    blob_publisher_tx: mpsc::Sender<BlobBuildRequest>,
    metrics: Arc<Metrics>,
}

impl CheckpointMonitor {
    pub fn new(
        config: CheckpointMonitorConfig,
        blob_publisher_tx: mpsc::Sender<BlobBuildRequest>,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            config,
            accumulator: CheckpointAccumulator::new(),
            pending_checkpoints: BTreeMap::new(),
            next_checkpoint_number: CheckpointSequenceNumber::from(0u64),
            last_updated_checkpoint_number: tokio::time::Instant::now(),
            last_stall_warning: tokio::time::Instant::now(),
            blob_publisher_tx,
            metrics,
        }
    }

    /// Start monitoring checkpoints from the receiver.
    pub async fn start(
        mut self,
        initial_checkpoint: CheckpointSequenceNumber,
        mut result_rx: mpsc::Receiver<CheckpointInfo>,
    ) -> Result<()> {
        tracing::info!(
            "starting checkpoint monitor from checkpoint {}",
            initial_checkpoint
        );

        self.next_checkpoint_number = initial_checkpoint;
        self.last_updated_checkpoint_number = tokio::time::Instant::now();

        while let Some(checkpoint_info) = result_rx.recv().await {
            tracing::debug!(
                "received checkpoint {} timestamp {} epoch {} is end of epoch {} size {}",
                checkpoint_info.checkpoint_number,
                checkpoint_info.timestamp_ms,
                checkpoint_info.epoch,
                checkpoint_info.is_end_of_epoch,
                checkpoint_info.checkpoint_byte_size,
            );

            // Check if this is the expected checkpoint.
            if checkpoint_info.checkpoint_number == self.next_checkpoint_number {
                // Process this checkpoint and any pending consecutive ones.
                self.process_checkpoint(checkpoint_info).await?;

                // Process any pending checkpoints that are now consecutive.
                while let Some(entry) = self.pending_checkpoints.first_entry() {
                    if *entry.key() == self.next_checkpoint_number {
                        let (_, checkpoint) = entry.remove_entry();
                        self.process_checkpoint(checkpoint).await?;
                    } else {
                        break;
                    }
                }
            } else if checkpoint_info.checkpoint_number > self.next_checkpoint_number {
                // This checkpoint is ahead, buffer it.
                tracing::debug!(
                    "buffering out-of-order checkpoint {}, expecting {}",
                    checkpoint_info.checkpoint_number,
                    self.next_checkpoint_number
                );
                self.pending_checkpoints
                    .insert(checkpoint_info.checkpoint_number, checkpoint_info);

                // Check for potential holes - if we have too many pending, there might be a problem.
                if self.pending_checkpoints.len() > 100 {
                    tracing::warn!(
                        "detected potential hole: large number of pending checkpoints: {}, waiting for checkpoint {}",
                        self.pending_checkpoints.len(),
                        self.next_checkpoint_number
                    );
                }
            } else {
                // This checkpoint is behind what we expect - this is an error.
                return Err(anyhow!(
                    "received checkpoint {} but already processed up to {}",
                    checkpoint_info.checkpoint_number,
                    self.next_checkpoint_number - 1
                ));
            }

            // Emit a warning if we have not received the next checkpoint in a while.
            // Only emit this warning at most once per minute.
            if self.last_updated_checkpoint_number.elapsed() > Duration::from_secs(60) {
                let should_warn = self.last_stall_warning.elapsed() > Duration::from_secs(60);

                if should_warn {
                    tracing::warn!(
                        "checkpoint stalled at {}, last updated at {:?}, latest downloaded checkpoint {}",
                        self.next_checkpoint_number,
                        self.last_updated_checkpoint_number,
                        self.pending_checkpoints
                            .last_key_value()
                            .map(|(k, _)| *k)
                            .unwrap_or(0u64)
                    );
                    self.last_stall_warning = tokio::time::Instant::now();
                }
            }
        }

        // Check if there are still pending checkpoints - this would indicate missing data.
        if !self.pending_checkpoints.is_empty() {
            tracing::error!(
                "checkpoint monitor stopped with {} pending checkpoints, missing checkpoint {}",
                self.pending_checkpoints.len(),
                self.next_checkpoint_number
            );
            return Err(anyhow!(
                "checkpoint sequence incomplete: missing checkpoint {} with {} pending",
                self.next_checkpoint_number,
                self.pending_checkpoints.len()
            ));
        }

        tracing::info!("checkpoint monitor stopped");
        Ok(())
    }

    /// Process a checkpoint that is in the correct order.
    async fn process_checkpoint(&mut self, checkpoint_info: CheckpointInfo) -> Result<()> {
        // Verify this is the expected checkpoint.
        if checkpoint_info.checkpoint_number != self.next_checkpoint_number {
            return Err(anyhow!(
                "invariant violation: processing checkpoint {} but expecting {}",
                checkpoint_info.checkpoint_number,
                self.next_checkpoint_number
            ));
        }

        // Check if we should build a blob before adding this checkpoint.
        if let Some(trigger) = self
            .accumulator
            .should_build_blob(&self.config, &checkpoint_info)
            && !self.accumulator.is_empty()
        {
            tracing::info!(
                "building blob due to {:?} with {} checkpoints, total size {} bytes",
                trigger,
                self.accumulator.checkpoints.len(),
                self.accumulator.total_size_bytes
            );

            // Build the blob.
            self.build_blob().await?;

            // Clear the accumulator for the next batch.
            self.accumulator.clear();
        }

        // Add the checkpoint to the accumulator.
        self.accumulator.add(checkpoint_info);

        // Update metrics.
        self.metrics
            .latest_processed_checkpoint
            .set(self.next_checkpoint_number as i64);

        // Update the next expected checkpoint number.
        self.next_checkpoint_number += 1;
        self.last_updated_checkpoint_number = tokio::time::Instant::now();

        Ok(())
    }

    /// Build a Walrus blob from the accumulated checkpoints.
    async fn build_blob(&self) -> Result<()> {
        if self.accumulator.checkpoints.is_empty() {
            return Ok(());
        }

        let start_checkpoint = self
            .accumulator
            .checkpoints
            .first()
            .map(|c| c.checkpoint_number)
            .unwrap_or(0);
        let end_checkpoint = self
            .accumulator
            .checkpoints
            .last()
            .map(|c| c.checkpoint_number)
            .unwrap_or(0);

        let end_of_epoch = self
            .accumulator
            .checkpoints
            .last()
            .map(|c| c.is_end_of_epoch)
            .unwrap_or(false);

        tracing::info!(
            "sending blob build request for checkpoints {} to {}",
            start_checkpoint,
            end_checkpoint
        );

        // Send request to CheckpointBlobPublisher.
        let request = BlobBuildRequest {
            start_checkpoint,
            end_checkpoint,
            end_of_epoch,
        };

        // Track the total number of blobs started to build.
        self.metrics.blob_build_requests_sent.inc();

        if let Err(e) = self.blob_publisher_tx.send(request).await {
            tracing::error!("failed to send blob build request: {}", e);
            return Err(anyhow!("failed to send blob build request: {}", e));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use prometheus::Registry;

    use super::*;

    fn create_test_metrics() -> Arc<Metrics> {
        let registry = Registry::new();
        Arc::new(Metrics::new(&registry))
    }

    fn create_test_checkpoint_info(
        checkpoint_number: u64,
        timestamp_ms: u64,
        size_bytes: usize,
        is_end_of_epoch: bool,
    ) -> CheckpointInfo {
        CheckpointInfo {
            checkpoint_number: CheckpointSequenceNumber::from(checkpoint_number),
            epoch: 1,
            is_end_of_epoch,
            timestamp_ms,
            checkpoint_byte_size: size_bytes,
        }
    }

    #[test]
    fn test_accumulator_should_build_blob_time_exceeded() {
        tracing_subscriber::fmt::init();

        let config = CheckpointMonitorConfig {
            max_accumulation_duration: Duration::from_secs(3600), // 1 hour.
            max_accumulation_size_bytes: 10 * 1024 * 1024 * 1024, // 10 GB.
        };

        let mut accumulator = CheckpointAccumulator::new();

        // Add first checkpoint.
        let checkpoint1 = create_test_checkpoint_info(1, 1000, 1024, false);
        accumulator.add(checkpoint1);

        // Check with a checkpoint that's within the time limit.
        let checkpoint2 = create_test_checkpoint_info(2, 1000 + 1800 * 1000, 1024, false); // 30 minutes later.
        assert_eq!(accumulator.should_build_blob(&config, &checkpoint2), None);

        // Check with a checkpoint that exceeds the time limit.
        let checkpoint3 = create_test_checkpoint_info(3, 1000 + 3700 * 1000, 1024, false); // 61+ minutes later.
        assert_eq!(
            accumulator.should_build_blob(&config, &checkpoint3),
            Some(BlobBuildTrigger::TimeDurationExceeded)
        );
    }

    #[test]
    fn test_accumulator_should_build_blob_size_exceeded() {
        tracing_subscriber::fmt::init();

        let config = CheckpointMonitorConfig {
            max_accumulation_duration: Duration::from_secs(3600),
            max_accumulation_size_bytes: 1024 * 1024, // 1 MB for testing.
        };

        let mut accumulator = CheckpointAccumulator::new();

        // Add checkpoint that's almost at the limit.
        let checkpoint1 = create_test_checkpoint_info(1, 1000, 1024 * 1000, false); // 1000 KB.
        accumulator.add(checkpoint1);

        // Check with a checkpoint that's within the size limit.
        let checkpoint2 = create_test_checkpoint_info(2, 2000, 1024, false); // 1 KB.
        assert_eq!(accumulator.should_build_blob(&config, &checkpoint2), None);

        // Check with a checkpoint that exceeds the size limit.
        let checkpoint3 = create_test_checkpoint_info(3, 3000, 1024 * 100, false); // 100 KB.
        assert_eq!(
            accumulator.should_build_blob(&config, &checkpoint3),
            Some(BlobBuildTrigger::SizeExceeded)
        );
    }

    #[test]
    fn test_accumulator_should_build_blob_end_of_epoch() {
        tracing_subscriber::fmt::init();

        let config = CheckpointMonitorConfig::default();

        let mut accumulator = CheckpointAccumulator::new();

        // Check with an end of epoch checkpoint.
        let checkpoint = create_test_checkpoint_info(100, 5000, 1024, true);
        // Should not build blob since it has not been added to the accumulator.
        assert_eq!(accumulator.should_build_blob(&config, &checkpoint), None);

        accumulator.add(checkpoint.clone());
        assert_eq!(
            accumulator.should_build_blob(&config, &checkpoint),
            Some(BlobBuildTrigger::EndOfEpoch)
        );
    }

    #[test]
    fn test_accumulator_operations() {
        tracing_subscriber::fmt::init();

        let mut accumulator = CheckpointAccumulator::new();

        assert!(accumulator.is_empty());
        assert_eq!(accumulator.total_size_bytes, 0);
        assert_eq!(accumulator.first_checkpoint_timestamp_ms, None);

        // Add checkpoints.
        let checkpoint1 = create_test_checkpoint_info(1, 1000, 500, false);
        accumulator.add(checkpoint1);

        assert!(!accumulator.is_empty());
        assert_eq!(accumulator.total_size_bytes, 500);
        assert_eq!(accumulator.first_checkpoint_timestamp_ms, Some(1000));

        let checkpoint2 = create_test_checkpoint_info(2, 2000, 300, false);
        accumulator.add(checkpoint2);

        assert_eq!(accumulator.checkpoints.len(), 2);
        assert_eq!(accumulator.total_size_bytes, 800);
        assert_eq!(accumulator.first_checkpoint_timestamp_ms, Some(1000));

        // Clear accumulator.
        accumulator.clear();

        assert!(accumulator.is_empty());
        assert_eq!(accumulator.total_size_bytes, 0);
        assert_eq!(accumulator.first_checkpoint_timestamp_ms, None);
    }

    #[tokio::test]
    async fn test_checkpoint_monitor_ordering() {
        tracing_subscriber::fmt::init();

        let config = CheckpointMonitorConfig::default();
        let (blob_tx, _blob_rx) = mpsc::channel(10);
        let monitor = CheckpointMonitor::new(config, blob_tx, create_test_metrics());

        let (tx, rx) = mpsc::channel(10);

        // Send checkpoints out of order.
        tx.send(create_test_checkpoint_info(2, 2000, 1024, false))
            .await
            .unwrap();
        tx.send(create_test_checkpoint_info(0, 0, 1024, false))
            .await
            .unwrap();
        tx.send(create_test_checkpoint_info(1, 1000, 1024, false))
            .await
            .unwrap();
        tx.send(create_test_checkpoint_info(3, 3000, 1024, false))
            .await
            .unwrap();

        drop(tx); // Close the channel.

        // Monitor should process them in order.
        let result = monitor.start(0, rx).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_checkpoint_monitor_detects_holes() {
        tracing_subscriber::fmt::init();

        let config = CheckpointMonitorConfig::default();
        let (blob_tx, _blob_rx) = mpsc::channel(10);
        let monitor = CheckpointMonitor::new(config, blob_tx, create_test_metrics());

        let (tx, rx) = mpsc::channel(1000);

        // Send checkpoints with a hole (missing checkpoint 1).
        tx.send(create_test_checkpoint_info(0, 0, 1024, false))
            .await
            .unwrap();
        tx.send(create_test_checkpoint_info(2, 2000, 1024, false))
            .await
            .unwrap();

        // Send many more to trigger the gap detection.
        for i in 102..202 {
            tracing::info!("sending checkpoint {}", i);
            tx.send(create_test_checkpoint_info(i, i * 1000, 1024, false))
                .await
                .unwrap();
        }

        drop(tx); // Close the channel.

        // Monitor should detect the hole and return an error.
        let result = monitor.start(0, rx).await;
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(e.to_string().contains("missing checkpoint 1"));
        }
    }

    #[tokio::test]
    async fn test_checkpoint_monitor_rejects_duplicate() {
        tracing_subscriber::fmt::init();

        let config = CheckpointMonitorConfig::default();
        let (blob_tx, _blob_rx) = mpsc::channel(10);
        let monitor = CheckpointMonitor::new(config, blob_tx, create_test_metrics());

        let (tx, rx) = mpsc::channel(10);

        // Send checkpoints with a duplicate.
        tx.send(create_test_checkpoint_info(0, 0, 1024, false))
            .await
            .unwrap();
        tx.send(create_test_checkpoint_info(1, 1000, 1024, false))
            .await
            .unwrap();
        tx.send(create_test_checkpoint_info(0, 0, 1024, false)) // Duplicate.
            .await
            .unwrap();

        drop(tx); // Close the channel.

        // Monitor should reject the duplicate.
        let result = monitor.start(0, rx).await;
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(e.to_string().contains("already processed"));
        }
    }
}
