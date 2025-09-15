// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use prometheus::{Histogram, HistogramOpts, IntCounter, IntCounterVec, IntGauge, Opts, Registry};

/// Metrics for the walrus-sui-archival service.
pub struct Metrics {
    // Checkpoint downloader metrics.
    /// Total number of checkpoints downloaded.
    pub total_downloaded_checkpoints: IntCounter,
    /// Latest successfully processed checkpoint number.
    pub latest_processed_checkpoint: IntGauge,
    /// Number of active download workers.
    pub active_download_workers: IntGauge,
    /// Download failures by status code.
    pub download_failures: IntCounterVec,
    /// Number of temp files cleaned up.
    pub temp_files_cleaned: IntCounter,

    // Checkpoint monitor metrics.
    /// Total number of blob build requests sent.
    pub blob_build_requests_sent: IntCounter,

    // Checkpoint blob publisher metrics.
    /// Blob build latency histogram.
    pub blob_build_latency_seconds: Histogram,
    /// Blob upload latency histogram.
    pub blob_upload_latency_seconds: Histogram,
    /// Blob size histogram.
    pub blob_size_bytes: Histogram,
    /// Latest uploaded blob size.
    pub latest_blob_size_bytes: IntGauge,
    /// Latest checkpoint included in uploaded blob.
    pub latest_uploaded_checkpoint: IntGauge,
    /// Total blobs uploaded successfully.
    pub blobs_uploaded_success: IntCounter,
    /// Total blobs upload failed.
    pub blobs_uploaded_failed: IntCounter,
    /// Total blobs upload failed with not_stored status.
    pub blobs_uploaded_not_stored: IntCounter,
    /// Total checkpoints cleaned up.
    pub checkpoints_cleaned: IntCounter,
    /// Latest checkpoint cleaned up.
    pub latest_cleaned_checkpoint: IntGauge,
    /// Total local blobs removed.
    pub local_blobs_removed: IntCounter,
}

impl Metrics {
    /// Create and register metrics with the provided registry.
    pub fn new(registry: &Registry) -> Self {
        // Checkpoint downloader metrics.
        let total_downloaded_checkpoints = IntCounter::new(
            "total_downloaded_checkpoints",
            "Total number of checkpoints downloaded",
        )
        .expect("metrics defined at compile time must be valid");

        let latest_processed_checkpoint = IntGauge::new(
            "latest_processed_checkpoint",
            "Latest successfully processed checkpoint number",
        )
        .expect("metrics defined at compile time must be valid");

        let active_download_workers = IntGauge::new(
            "active_download_workers",
            "Number of active download workers",
        )
        .expect("metrics defined at compile time must be valid");

        let download_failures = IntCounterVec::new(
            Opts::new("download_failures", "Download failures by status code"),
            &["status_code"],
        )
        .expect("metrics defined at compile time must be valid");

        let temp_files_cleaned =
            IntCounter::new("temp_files_cleaned", "Number of temp files cleaned up")
                .expect("metrics defined at compile time must be valid");

        // Checkpoint monitor metrics.
        let blob_build_requests_sent = IntCounter::new(
            "blob_build_requests_sent",
            "Total number of blob build requests sent",
        )
        .expect("metrics defined at compile time must be valid");

        // Checkpoint blob publisher metrics.
        let blob_build_latency_seconds = Histogram::with_opts(HistogramOpts::new(
            "blob_build_latency_seconds",
            "Blob build latency in seconds",
        ))
        .expect("metrics defined at compile time must be valid");

        let blob_upload_latency_seconds = Histogram::with_opts(HistogramOpts::new(
            "blob_upload_latency_seconds",
            "Blob upload latency in seconds",
        ))
        .expect("metrics defined at compile time must be valid");

        let blob_size_bytes =
            Histogram::with_opts(HistogramOpts::new("blob_size_bytes", "Blob size in bytes"))
                .expect("metrics defined at compile time must be valid");

        let latest_blob_size_bytes = IntGauge::new(
            "latest_blob_size_bytes",
            "Size of the latest uploaded blob in bytes",
        )
        .expect("metrics defined at compile time must be valid");

        let latest_uploaded_checkpoint = IntGauge::new(
            "latest_uploaded_checkpoint",
            "Latest checkpoint included in uploaded blob",
        )
        .expect("metrics defined at compile time must be valid");

        let blobs_uploaded_success = IntCounter::new(
            "blobs_uploaded_success",
            "Total blobs uploaded successfully",
        )
        .expect("metrics defined at compile time must be valid");

        let blobs_uploaded_failed =
            IntCounter::new("blobs_uploaded_failed", "Total blobs upload failed")
                .expect("metrics defined at compile time must be valid");

        let blobs_uploaded_not_stored = IntCounter::new(
            "blobs_uploaded_not_stored",
            "Total blobs upload failed with not_stored status",
        )
        .expect("metrics defined at compile time must be valid");

        let checkpoints_cleaned =
            IntCounter::new("checkpoints_cleaned", "Total checkpoints cleaned up")
                .expect("metrics defined at compile time must be valid");

        let latest_cleaned_checkpoint =
            IntGauge::new("latest_cleaned_checkpoint", "Latest checkpoint cleaned up")
                .expect("metrics defined at compile time must be valid");

        let local_blobs_removed =
            IntCounter::new("local_blobs_removed", "Total local blobs removed")
                .expect("metrics defined at compile time must be valid");

        // Register all metrics.
        registry
            .register(Box::new(total_downloaded_checkpoints.clone()))
            .expect("metrics defined at compile time must be valid");
        registry
            .register(Box::new(latest_processed_checkpoint.clone()))
            .expect("metrics defined at compile time must be valid");
        registry
            .register(Box::new(active_download_workers.clone()))
            .expect("metrics defined at compile time must be valid");
        registry
            .register(Box::new(download_failures.clone()))
            .expect("metrics defined at compile time must be valid");
        registry
            .register(Box::new(temp_files_cleaned.clone()))
            .expect("metrics defined at compile time must be valid");
        registry
            .register(Box::new(blob_build_requests_sent.clone()))
            .expect("metrics defined at compile time must be valid");
        registry
            .register(Box::new(blob_build_latency_seconds.clone()))
            .expect("metrics defined at compile time must be valid");
        registry
            .register(Box::new(blob_upload_latency_seconds.clone()))
            .expect("metrics defined at compile time must be valid");
        registry
            .register(Box::new(blob_size_bytes.clone()))
            .expect("metrics defined at compile time must be valid");
        registry
            .register(Box::new(latest_blob_size_bytes.clone()))
            .expect("metrics defined at compile time must be valid");
        registry
            .register(Box::new(latest_uploaded_checkpoint.clone()))
            .expect("metrics defined at compile time must be valid");
        registry
            .register(Box::new(blobs_uploaded_success.clone()))
            .expect("metrics defined at compile time must be valid");
        registry
            .register(Box::new(blobs_uploaded_failed.clone()))
            .expect("metrics defined at compile time must be valid");
        registry
            .register(Box::new(blobs_uploaded_not_stored.clone()))
            .expect("metrics defined at compile time must be valid");
        registry
            .register(Box::new(checkpoints_cleaned.clone()))
            .expect("metrics defined at compile time must be valid");
        registry
            .register(Box::new(latest_cleaned_checkpoint.clone()))
            .expect("metrics defined at compile time must be valid");
        registry
            .register(Box::new(local_blobs_removed.clone()))
            .expect("metrics defined at compile time must be valid");

        Self {
            total_downloaded_checkpoints,
            latest_processed_checkpoint,
            active_download_workers,
            download_failures,
            temp_files_cleaned,
            blob_build_requests_sent,
            blob_build_latency_seconds,
            blob_upload_latency_seconds,
            blob_size_bytes,
            latest_blob_size_bytes,
            latest_uploaded_checkpoint,
            blobs_uploaded_success,
            blobs_uploaded_failed,
            blobs_uploaded_not_stored,
            checkpoints_cleaned,
            latest_cleaned_checkpoint,
            local_blobs_removed,
        }
    }
}
