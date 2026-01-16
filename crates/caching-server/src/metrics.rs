// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use prometheus::{
    Histogram,
    HistogramOpts,
    HistogramVec,
    IntCounter,
    IntCounterVec,
    Opts,
    Registry,
    TextEncoder,
};

/// Metrics for the caching server.
#[derive(Clone)]
pub struct Metrics {
    /// Total HTTP requests by endpoint, source, and status.
    pub http_requests_total: IntCounterVec,
    /// HTTP request latency by endpoint and source.
    pub http_request_latency_seconds: HistogramVec,
    /// Cache hits.
    pub cache_hits_total: IntCounter,
    /// Cache misses.
    pub cache_misses_total: IntCounter,
    /// Stale cache serves (when backend fails).
    pub cache_stale_serves_total: IntCounter,
    /// PostgreSQL query total by operation.
    pub postgres_queries_total: IntCounterVec,
    /// PostgreSQL query failures by operation.
    pub postgres_query_failures: IntCounterVec,
    /// PostgreSQL query latency by operation.
    pub postgres_query_latency_seconds: HistogramVec,
    /// Backend proxy requests total.
    pub backend_requests_total: IntCounter,
    /// Backend proxy request failures.
    pub backend_request_failures: IntCounter,
    /// Backend request latency.
    pub backend_request_latency_seconds: Histogram,
    /// Cache refresh total.
    pub cache_refresh_total: IntCounter,
    /// Cache refresh failures.
    pub cache_refresh_failures: IntCounter,
    /// The prometheus registry.
    pub registry: Registry,
}

impl Metrics {
    /// Create and register metrics with a new registry.
    pub fn new() -> Self {
        let registry = Registry::new();
        Self::new_with_registry(&registry)
    }

    /// Create and register metrics with the provided registry.
    pub fn new_with_registry(registry: &Registry) -> Self {
        let http_requests_total = IntCounterVec::new(
            Opts::new(
                "http_requests_total",
                "Total HTTP requests by endpoint, source, and status",
            ),
            &["endpoint", "source", "status"],
        )
        .expect("metrics defined at compile time must be valid");

        let http_request_latency_seconds = HistogramVec::new(
            HistogramOpts::new(
                "http_request_latency_seconds",
                "HTTP request latency in seconds by endpoint and source",
            )
            .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0]),
            &["endpoint", "source"],
        )
        .expect("metrics defined at compile time must be valid");

        let cache_hits_total = IntCounter::new("cache_hits_total", "Total cache hits")
            .expect("metrics defined at compile time must be valid");

        let cache_misses_total = IntCounter::new("cache_misses_total", "Total cache misses")
            .expect("metrics defined at compile time must be valid");

        let cache_stale_serves_total = IntCounter::new(
            "cache_stale_serves_total",
            "Total stale cache serves when backend fails",
        )
        .expect("metrics defined at compile time must be valid");

        let postgres_queries_total = IntCounterVec::new(
            Opts::new(
                "postgres_queries_total",
                "Total PostgreSQL queries by operation",
            ),
            &["operation"],
        )
        .expect("metrics defined at compile time must be valid");

        let postgres_query_failures = IntCounterVec::new(
            Opts::new(
                "postgres_query_failures",
                "Total PostgreSQL query failures by operation",
            ),
            &["operation"],
        )
        .expect("metrics defined at compile time must be valid");

        let postgres_query_latency_seconds = HistogramVec::new(
            HistogramOpts::new(
                "postgres_query_latency_seconds",
                "PostgreSQL query latency in seconds by operation",
            )
            .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0]),
            &["operation"],
        )
        .expect("metrics defined at compile time must be valid");

        let backend_requests_total =
            IntCounter::new("backend_requests_total", "Total backend proxy requests")
                .expect("metrics defined at compile time must be valid");

        let backend_request_failures = IntCounter::new(
            "backend_request_failures",
            "Total backend proxy request failures",
        )
        .expect("metrics defined at compile time must be valid");

        let backend_request_latency_seconds = Histogram::with_opts(
            HistogramOpts::new(
                "backend_request_latency_seconds",
                "Backend proxy request latency in seconds",
            )
            .buckets(vec![0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0]),
        )
        .expect("metrics defined at compile time must be valid");

        let cache_refresh_total =
            IntCounter::new("cache_refresh_total", "Total cache refresh operations")
                .expect("metrics defined at compile time must be valid");

        let cache_refresh_failures =
            IntCounter::new("cache_refresh_failures", "Total cache refresh failures")
                .expect("metrics defined at compile time must be valid");

        // Register all metrics.
        registry
            .register(Box::new(http_requests_total.clone()))
            .expect("metrics defined at compile time must be valid");
        registry
            .register(Box::new(http_request_latency_seconds.clone()))
            .expect("metrics defined at compile time must be valid");
        registry
            .register(Box::new(cache_hits_total.clone()))
            .expect("metrics defined at compile time must be valid");
        registry
            .register(Box::new(cache_misses_total.clone()))
            .expect("metrics defined at compile time must be valid");
        registry
            .register(Box::new(cache_stale_serves_total.clone()))
            .expect("metrics defined at compile time must be valid");
        registry
            .register(Box::new(postgres_queries_total.clone()))
            .expect("metrics defined at compile time must be valid");
        registry
            .register(Box::new(postgres_query_failures.clone()))
            .expect("metrics defined at compile time must be valid");
        registry
            .register(Box::new(postgres_query_latency_seconds.clone()))
            .expect("metrics defined at compile time must be valid");
        registry
            .register(Box::new(backend_requests_total.clone()))
            .expect("metrics defined at compile time must be valid");
        registry
            .register(Box::new(backend_request_failures.clone()))
            .expect("metrics defined at compile time must be valid");
        registry
            .register(Box::new(backend_request_latency_seconds.clone()))
            .expect("metrics defined at compile time must be valid");
        registry
            .register(Box::new(cache_refresh_total.clone()))
            .expect("metrics defined at compile time must be valid");
        registry
            .register(Box::new(cache_refresh_failures.clone()))
            .expect("metrics defined at compile time must be valid");

        Self {
            http_requests_total,
            http_request_latency_seconds,
            cache_hits_total,
            cache_misses_total,
            cache_stale_serves_total,
            postgres_queries_total,
            postgres_query_failures,
            postgres_query_latency_seconds,
            backend_requests_total,
            backend_request_failures,
            backend_request_latency_seconds,
            cache_refresh_total,
            cache_refresh_failures,
            registry: registry.clone(),
        }
    }

    /// Encode all metrics to Prometheus text format.
    pub fn encode(&self) -> String {
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        encoder
            .encode_to_string(&metric_families)
            .unwrap_or_default()
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}
