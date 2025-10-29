// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use prometheus::{Histogram, HistogramOpts, IntCounter, IntCounterVec, IntGauge, Opts, Registry};
use sui_indexer_alt_framework::metrics::IndexerMetrics;
use walrus_sdk::{client::metrics::ClientMetrics, core_utils::metrics::Registry as WalrusRegistry};

/// Metrics for the walrus-sui-archival service.
pub struct Metrics {
    // Indexer metrics for ingestion service.
    pub indexer_metrics: Arc<IndexerMetrics>,

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
    /// Number of active blob uploads in progress.
    pub active_blob_uploads: IntGauge,

    // Blob extension metrics.
    /// Total blob extensions attempted.
    pub blob_extensions_attempted: IntCounter,
    /// Total blob extensions succeeded.
    pub blob_extensions_succeeded: IntCounter,
    /// Total blob extensions failed.
    pub blob_extensions_failed: IntCounter,

    // Archival state snapshot metrics.
    /// Snapshot creation latency histogram.
    pub snapshot_creation_latency_seconds: Histogram,
    /// Snapshot upload latency histogram.
    pub snapshot_upload_latency_seconds: Histogram,
    /// Total number of records in the latest snapshot.
    pub snapshot_records_total: IntGauge,
    /// Total snapshots created successfully.
    pub snapshots_created_success: IntCounter,
    /// Total snapshots creation failed.
    pub snapshots_created_failed: IntCounter,
    /// Total on-chain metadata updates succeeded.
    pub metadata_updates_success: IntCounter,
    /// Total on-chain metadata updates failed.
    pub metadata_updates_failed: IntCounter,

    // Walrus client metrics.
    pub walrus_sdk_registry: WalrusRegistry,
    /// Metrics for the Walrus client.
    pub walrus_client_metrics: Arc<ClientMetrics>,
}

impl Metrics {
    /// Create and register metrics with the provided registry.
    pub fn new(registry: &Registry) -> Self {
        // Indexer metrics for ingestion service.
        let indexer_metrics = IndexerMetrics::new(None, registry);

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
        let blob_build_latency_seconds = Histogram::with_opts(
            HistogramOpts::new(
                "blob_build_latency_seconds",
                "Blob build latency in seconds",
            )
            .buckets(vec![
                0.1, 0.5, 1.0, 5.0, 10.0, 20.0, 50.0, 100.0, 200.0, 300.0,
            ]),
        )
        .expect("metrics defined at compile time must be valid");

        let blob_upload_latency_seconds = Histogram::with_opts(
            HistogramOpts::new(
                "blob_upload_latency_seconds",
                "Blob upload latency in seconds",
            )
            .buckets(vec![
                1.0, 5.0, 10.0, 20.0, 40.0, 60.0, 120.0, 240.0, 300.0, 360.0, 420.0, 480.0, 540.0,
                600.0, 900.0, 1200.0,
            ]),
        )
        .expect("metrics defined at compile time must be valid");

        let blob_size_bytes = Histogram::with_opts(
            HistogramOpts::new("blob_size_bytes", "Blob size in bytes").buckets(vec![
                1024.0,       // 1 KB
                10240.0,      // 10 KB
                102400.0,     // 100 KB
                1048576.0,    // 1 MB
                10485760.0,   // 10 MB
                104857600.0,  // 100 MB
                1073741824.0, // 1 GB
                2147483648.0, // 2 GB
                3221225472.0, // 3 GB
            ]),
        )
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

        let active_blob_uploads = IntGauge::new(
            "active_blob_uploads",
            "Number of active blob uploads in progress",
        )
        .expect("metrics defined at compile time must be valid");

        // Blob extension metrics.
        let blob_extensions_attempted = IntCounter::new(
            "blob_extensions_attempted",
            "Total blob extensions attempted",
        )
        .expect("metrics defined at compile time must be valid");

        let blob_extensions_succeeded = IntCounter::new(
            "blob_extensions_succeeded",
            "Total blob extensions succeeded",
        )
        .expect("metrics defined at compile time must be valid");

        let blob_extensions_failed =
            IntCounter::new("blob_extensions_failed", "Total blob extensions failed")
                .expect("metrics defined at compile time must be valid");

        // Archival state snapshot metrics.
        let snapshot_creation_latency_seconds = Histogram::with_opts(
            HistogramOpts::new(
                "snapshot_creation_latency_seconds",
                "Snapshot creation latency in seconds",
            )
            .buckets(vec![
                0.1, 0.5, 1.0, 5.0, 10.0, 20.0, 50.0, 100.0, 200.0, 300.0,
            ]),
        )
        .expect("metrics defined at compile time must be valid");

        let snapshot_upload_latency_seconds = Histogram::with_opts(
            HistogramOpts::new(
                "snapshot_upload_latency_seconds",
                "Snapshot upload latency in seconds",
            )
            .buckets(vec![
                1.0, 5.0, 10.0, 20.0, 40.0, 60.0, 120.0, 240.0, 300.0, 360.0, 420.0, 480.0, 540.0,
                600.0, 900.0, 1200.0,
            ]),
        )
        .expect("metrics defined at compile time must be valid");

        let snapshot_records_total = IntGauge::new(
            "snapshot_records_total",
            "Total number of records in the latest snapshot",
        )
        .expect("metrics defined at compile time must be valid");

        let snapshots_created_success = IntCounter::new(
            "snapshots_created_success",
            "Total snapshots created successfully",
        )
        .expect("metrics defined at compile time must be valid");

        let snapshots_created_failed = IntCounter::new(
            "snapshots_created_failed",
            "Total snapshots creation failed",
        )
        .expect("metrics defined at compile time must be valid");

        let metadata_updates_success = IntCounter::new(
            "metadata_updates_success",
            "Total on-chain metadata updates succeeded",
        )
        .expect("metrics defined at compile time must be valid");

        let metadata_updates_failed = IntCounter::new(
            "metadata_updates_failed",
            "Total on-chain metadata updates failed",
        )
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
        registry
            .register(Box::new(active_blob_uploads.clone()))
            .expect("metrics defined at compile time must be valid");
        registry
            .register(Box::new(blob_extensions_attempted.clone()))
            .expect("metrics defined at compile time must be valid");
        registry
            .register(Box::new(blob_extensions_succeeded.clone()))
            .expect("metrics defined at compile time must be valid");
        registry
            .register(Box::new(blob_extensions_failed.clone()))
            .expect("metrics defined at compile time must be valid");
        registry
            .register(Box::new(snapshot_creation_latency_seconds.clone()))
            .expect("metrics defined at compile time must be valid");
        registry
            .register(Box::new(snapshot_upload_latency_seconds.clone()))
            .expect("metrics defined at compile time must be valid");
        registry
            .register(Box::new(snapshot_records_total.clone()))
            .expect("metrics defined at compile time must be valid");
        registry
            .register(Box::new(snapshots_created_success.clone()))
            .expect("metrics defined at compile time must be valid");
        registry
            .register(Box::new(snapshots_created_failed.clone()))
            .expect("metrics defined at compile time must be valid");
        registry
            .register(Box::new(metadata_updates_success.clone()))
            .expect("metrics defined at compile time must be valid");
        registry
            .register(Box::new(metadata_updates_failed.clone()))
            .expect("metrics defined at compile time must be valid");

        let walrus_sdk_registry = WalrusRegistry::new(registry.clone());
        let walrus_client_metrics = Arc::new(ClientMetrics::new(&walrus_sdk_registry));

        Self {
            indexer_metrics,
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
            active_blob_uploads,
            blob_extensions_attempted,
            blob_extensions_succeeded,
            blob_extensions_failed,
            snapshot_creation_latency_seconds,
            snapshot_upload_latency_seconds,
            snapshot_records_total,
            snapshots_created_success,
            snapshots_created_failed,
            metadata_updates_success,
            metadata_updates_failed,
            walrus_sdk_registry,
            walrus_client_metrics,
        }
    }
}
