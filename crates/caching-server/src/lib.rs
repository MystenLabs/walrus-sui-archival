// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

pub mod metrics;

use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Result;
use axum::{
    Json,
    Router,
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use metrics::Metrics;
use postgres_store::{PostgresPool, SharedPostgresPool};
use rand::Rng;
use serde::{Deserialize, Serialize};
use sui_storage::blob::Blob;
use sui_types::full_checkpoint_content::CheckpointData;
use tokio::sync::RwLock;
use tower_http::cors::CorsLayer;

/// Configuration for the caching server.
#[derive(Debug, Clone)]
pub struct Config {
    /// The backend URL to proxy requests to (used as fallback).
    pub backend_url: String,
    /// The address to bind the server to.
    pub bind_address: SocketAddr,
    /// The address to bind the Prometheus metrics server to.
    /// Metrics will be available at /metrics on this server.
    pub metrics_address: SocketAddr,
    /// Cache freshness duration in seconds.
    pub cache_freshness_secs: u64,
    /// Cache refresh interval in seconds.
    pub cache_refresh_interval_secs: u64,
    /// PostgreSQL database URL for direct queries.
    pub database_url: Option<String>,
}

impl Config {
    /// Create a new config from environment string.
    pub fn new(
        env: &str,
        bind_address: SocketAddr,
        metrics_address: SocketAddr,
        cache_freshness_secs: u64,
        cache_refresh_interval_secs: u64,
        database_url: Option<String>,
    ) -> Self {
        let backend_url = match env {
            "mainnet" => "https://walrus-sui-archival.mainnet.walrus.space".to_string(),
            "testnet" => "https://walrus-sui-archival.testnet.walrus.space".to_string(),
            "localnet" => "http://localhost:9185".to_string(),
            custom => custom.to_string(),
        };

        Self {
            backend_url,
            bind_address,
            metrics_address,
            cache_freshness_secs,
            cache_refresh_interval_secs,
            database_url,
        }
    }
}

/// Helper function to convert Vec<u8> fields in a JSON value to base64 strings.
/// This recursively processes JSON arrays and objects.
fn convert_bytes_to_base64(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(arr) => {
            // Check if this is a byte array (array of numbers 0-255).
            if arr.iter().all(|v| {
                if let serde_json::Value::Number(n) = v {
                    n.as_u64().is_some_and(|num| num <= 255)
                } else {
                    false
                }
            }) && !arr.is_empty()
            {
                // Convert to base64 string.
                let bytes: Vec<u8> = arr
                    .iter()
                    .filter_map(|v| v.as_u64().map(|n| n as u8))
                    .collect();
                serde_json::Value::String(BASE64.encode(&bytes))
            } else {
                // Recursively process array elements.
                serde_json::Value::Array(arr.into_iter().map(convert_bytes_to_base64).collect())
            }
        }
        serde_json::Value::Object(map) => {
            // Recursively process object fields.
            serde_json::Value::Object(
                map.into_iter()
                    .map(|(k, v)| (k, convert_bytes_to_base64(v)))
                    .collect(),
            )
        }
        // Leave other types unchanged.
        other => other,
    }
}

/// Cache entry for storing responses.
#[derive(Clone)]
struct CacheEntry {
    data: String,
    timestamp: Instant,
}

/// Application state.
#[derive(Clone)]
struct AppState {
    backend_url: String,
    http_client: reqwest::Client,
    cache: Arc<RwLock<std::collections::HashMap<String, CacheEntry>>>,
    cache_refresh_interval: Duration,
    cache_freshness_duration: Duration,
    /// PostgreSQL connection pool for direct database queries.
    postgres_pool: Option<SharedPostgresPool>,
    /// Metrics for tracking server operations.
    metrics: Arc<Metrics>,
}

impl AppState {
    /// Query homepage info directly from PostgreSQL.
    /// Uses aggregate query for efficient stats retrieval.
    async fn query_homepage_info_from_postgres(&self) -> Option<HomepageInfo> {
        let pool = self.postgres_pool.as_ref()?;

        // Get aggregate stats in a single query.
        let stats = match pool.get_blob_stats().await {
            Ok(stats) => stats,
            Err(e) => {
                tracing::error!("failed to get blob stats from postgres: {}", e);
                return None;
            }
        };

        let total_checkpoints = if stats.latest_checkpoint >= stats.earliest_checkpoint {
            (stats.latest_checkpoint - stats.earliest_checkpoint + 1) as u64
        } else {
            0
        };

        Some(HomepageInfo {
            blob_count: stats.blob_count as usize,
            total_checkpoints,
            earliest_checkpoint: stats.earliest_checkpoint as u64,
            latest_checkpoint: stats.latest_checkpoint as u64,
            total_size: stats.total_size as u64,
            metadata_info: None, // Metadata info requires on-chain query.
        })
    }

    /// Query blobs directly from PostgreSQL with pagination.
    async fn query_blobs_from_postgres(
        &self,
        limit: Option<i64>,
        cursor: Option<i64>,
    ) -> Option<AppBlobsResponse> {
        let pool = self.postgres_pool.as_ref()?;

        // Request one extra to determine if there are more results.
        let request_limit = limit.unwrap_or(50);
        let blobs = match pool
            .list_all_blobs(true, Some(request_limit + 1), cursor)
            .await
        {
            Ok(blobs) => blobs,
            Err(e) => {
                tracing::error!("failed to list blobs from postgres: {}", e);
                return None;
            }
        };

        let has_more = blobs.len() > request_limit as usize;
        let blobs_to_return: Vec<_> = blobs.into_iter().take(request_limit as usize).collect();

        let next_cursor = if has_more {
            blobs_to_return.last().map(|b| b.start_checkpoint)
        } else {
            None
        };

        let result: Vec<AppBlobInfo> = blobs_to_return
            .into_iter()
            .map(|blob| {
                // Calculate entries_count from checkpoint range.
                let entries_count =
                    (blob.end_checkpoint - blob.start_checkpoint + 1).max(0) as usize;
                // Use blob_size directly from the database.
                let total_size = blob.blob_size.unwrap_or(0) as u64;

                AppBlobInfo {
                    blob_id: blob.blob_id,
                    object_id: blob.object_id,
                    start_checkpoint: blob.start_checkpoint as u64,
                    end_checkpoint: blob.end_checkpoint as u64,
                    end_of_epoch: blob.end_of_epoch,
                    expiry_epoch: blob.blob_expiration_epoch as u32,
                    is_shared_blob: blob.is_shared_blob,
                    entries_count,
                    total_size,
                }
            })
            .collect();

        Some(AppBlobsResponse {
            blobs: result,
            next_cursor,
        })
    }

    /// Query blobs expired before a given epoch from PostgreSQL.
    /// Uses the blob_expiration_epoch index for efficient querying.
    async fn query_blobs_expired_before_epoch_from_postgres(
        &self,
        epoch: u32,
    ) -> Option<Vec<ExpiredBlobInfo>> {
        let pool = self.postgres_pool.as_ref()?;

        let blobs = match pool.list_blobs_expiring_before(epoch as i32).await {
            Ok(blobs) => blobs,
            Err(e) => {
                tracing::error!("failed to query expired blobs from postgres: {}", e);
                return None;
            }
        };

        let result: Vec<ExpiredBlobInfo> = blobs
            .into_iter()
            .map(|b| ExpiredBlobInfo {
                blob_id: b.blob_id,
                object_id: b.object_id,
                end_epoch: b.blob_expiration_epoch as u32,
            })
            .collect();

        Some(result)
    }

    /// Query a specific checkpoint from PostgreSQL.
    async fn query_checkpoint_from_postgres(&self, checkpoint: u64) -> Option<AppCheckpointInfo> {
        let pool = self.postgres_pool.as_ref()?;

        // Find the blob that contains this checkpoint.
        let blob = match pool
            .find_blob_containing_checkpoint(checkpoint as i64)
            .await
        {
            Ok(Some(blob)) => blob,
            Ok(None) => {
                tracing::warn!("checkpoint {} not found in postgres", checkpoint);
                return None;
            }
            Err(e) => {
                tracing::error!("failed to find blob for checkpoint {}: {}", checkpoint, e);
                return None;
            }
        };

        // Get index entries for this blob.
        let entries = match pool.get_index_entries(blob.start_checkpoint).await {
            Ok(entries) => entries,
            Err(e) => {
                tracing::error!(
                    "failed to get index entries for checkpoint {}: {}",
                    checkpoint,
                    e
                );
                return None;
            }
        };

        // Find the specific entry for this checkpoint.
        let index = (checkpoint as i64 - blob.start_checkpoint) as usize;
        let entry = entries
            .iter()
            .find(|e| e.checkpoint_number == checkpoint as i64)?;

        Some(AppCheckpointInfo {
            checkpoint_number: checkpoint,
            blob_id: blob.blob_id,
            object_id: blob.object_id,
            index,
            offset: entry.offset_bytes as u64,
            length: entry.length_bytes as u64,
            content: None, // Content requires fetching from Walrus.
        })
    }
}

/// Query parameters for blobs expired before epoch endpoint.
#[derive(Deserialize)]
struct BlobsExpiredQuery {
    epoch: u32,
}

/// Response structure for expired blobs.
#[derive(Serialize, Deserialize)]
struct ExpiredBlobInfo {
    blob_id: String,
    object_id: String,
    end_epoch: u32,
}

/// Response structure for homepage info.
#[derive(Serialize, Deserialize)]
struct HomepageInfo {
    blob_count: usize,
    total_checkpoints: u64,
    earliest_checkpoint: u64,
    latest_checkpoint: u64,
    total_size: u64,
    metadata_info: Option<MetadataInfo>,
}

/// Metadata information structure.
#[derive(Serialize, Deserialize)]
struct MetadataInfo {
    metadata_pointer_object_id: String,
    contract_package_id: String,
    current_metadata_blob_id: Option<String>,
}

/// Response structure for app blobs endpoint.
#[derive(Serialize, Deserialize)]
struct AppBlobInfo {
    blob_id: String,
    object_id: String,
    start_checkpoint: u64,
    end_checkpoint: u64,
    end_of_epoch: bool,
    expiry_epoch: u32,
    is_shared_blob: bool,
    entries_count: usize,
    total_size: u64,
}

/// Response structure for paginated app blobs endpoint.
#[derive(Serialize, Deserialize)]
struct AppBlobsResponse {
    blobs: Vec<AppBlobInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_cursor: Option<i64>,
}

/// Query parameters for app blobs endpoint.
#[derive(Deserialize)]
struct AppBlobsQuery {
    #[serde(default)]
    limit: Option<i64>,
    #[serde(default)]
    cursor: Option<i64>,
}

/// Response structure for app checkpoint endpoint.
#[derive(Serialize, Deserialize)]
struct AppCheckpointInfo {
    checkpoint_number: u64,
    blob_id: String,
    object_id: String,
    index: usize,
    offset: u64,
    length: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    content: Option<serde_json::Value>,
}

/// Query parameters for app checkpoint endpoint.
#[derive(Deserialize)]
struct AppCheckpointQuery {
    checkpoint: u64,
    #[serde(default)]
    show_content: bool,
}

/// Request structure for refresh blob end epoch endpoint.
#[derive(Deserialize, Serialize)]
struct RefreshBlobEndEpochRequest {
    object_ids: Vec<String>,
}

/// Response structure for refresh blob end epoch endpoint.
#[derive(Serialize, Deserialize)]
struct RefreshBlobEndEpochResponse {
    message: String,
    count: usize,
}

/// Start the caching server.
pub async fn start_server(config: Config, version: &'static str) -> Result<()> {
    // Initialize metrics with Prometheus server.
    let registry_service = mysten_metrics::start_prometheus_server(config.metrics_address);
    let registry = registry_service.default_registry();

    // Register uptime metric.
    let registry_clone = registry.clone();
    tokio::spawn(async move {
        registry_clone
            .register(mysten_metrics::uptime_metric(
                "caching-server",
                version,
                "walrus-sui-archival",
            ))
            .expect("metrics defined at compile time must be valid");
    });

    tracing::info!(
        "Prometheus metrics server started on {}",
        config.metrics_address
    );
    let metrics = Arc::new(Metrics::new_with_registry(&registry));

    // Initialize PostgreSQL pool if database URL is provided.
    // If DATABASE_URL is set, use PostgreSQL exclusively; otherwise use backend proxy.
    let postgres_pool = if let Some(ref db_url) = config.database_url {
        let pool = PostgresPool::new(db_url)?;
        tracing::info!("PostgreSQL mode: using database directly");
        Some(Arc::new(pool))
    } else {
        tracing::info!(
            "Backend proxy mode: forwarding requests to {}",
            config.backend_url
        );
        None
    };

    let app_state = AppState {
        backend_url: config.backend_url.clone(),
        http_client: reqwest::Client::new(),
        cache: Arc::new(RwLock::new(std::collections::HashMap::new())),
        cache_refresh_interval: Duration::from_secs(config.cache_refresh_interval_secs),
        cache_freshness_duration: Duration::from_secs(config.cache_freshness_secs),
        postgres_pool,
        metrics,
    };

    // Start background cache refresh task.
    let cache_state = app_state.clone();
    tokio::spawn(async move {
        background_cache_refresh(cache_state).await;
    });

    let app = Router::new()
        .route(
            "/v1/app_blobs_expired_before_epoch",
            get(proxy_blobs_expired_before_epoch),
        )
        .route(
            "/v1/app_info_for_homepage",
            get(proxy_app_info_for_homepage),
        )
        .route("/v1/app_blobs", get(proxy_app_blobs))
        .route("/v1/app_checkpoint", get(proxy_app_checkpoint))
        .route(
            "/v1/app_refresh_blob_end_epoch",
            post(proxy_refresh_blob_end_epoch),
        )
        .route("/v1/health", get(health_check))
        .route("/metrics", get(metrics_handler))
        .with_state(app_state)
        .layer(CorsLayer::permissive());

    tracing::info!(
        "starting caching server on {} with backend {}",
        config.bind_address,
        config.backend_url
    );

    let listener = tokio::net::TcpListener::bind(config.bind_address)
        .await
        .map_err(|e| anyhow::anyhow!("failed to bind to {}: {}", config.bind_address, e))?;

    axum::serve(listener, app)
        .await
        .map_err(|e| anyhow::anyhow!("server error: {}", e))?;

    Ok(())
}

/// Background task to periodically refresh cache.
async fn background_cache_refresh(state: AppState) {
    loop {
        tracing::info!("starting background cache refresh");

        // If PostgreSQL is available, refresh from there; otherwise use backend proxy.
        if state.postgres_pool.is_some() {
            refresh_homepage_cache_from_postgres(&state).await;
            refresh_blobs_cache_from_postgres(&state).await;
        } else {
            refresh_homepage_cache(&state).await;
            refresh_blobs_cache(&state).await;
        }

        tracing::info!("background cache refresh completed");

        // Wait for the cache freshness duration before refreshing.
        // Random jitter: 10% shorter to 30% longer than the base interval.
        let base = state.cache_refresh_interval;
        let factor: f64 = rand::rng().random_range(0.9..1.3);
        let sleep_ms = (base.as_millis() as f64 * factor).max(0.0) as u64;
        tracing::info!("sleeping for {} ms before next refresh", sleep_ms);
        tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
    }
}

/// Refresh homepage info cache.
async fn refresh_homepage_cache(state: &AppState) {
    let url = format!("{}/v1/app_info_for_homepage", state.backend_url);
    let cache_key = "v1/app_info_for_homepage?".to_string();

    match state.http_client.get(&url).send().await {
        Ok(response) => {
            if response.status().is_success() {
                match response.text().await {
                    Ok(text) => {
                        let mut cache = state.cache.write().await;
                        cache.insert(
                            cache_key,
                            CacheEntry {
                                data: text,
                                timestamp: Instant::now(),
                            },
                        );
                        tracing::info!("refreshed cache for /v1/app_info_for_homepage");
                    }
                    Err(e) => {
                        tracing::error!("failed to read homepage response: {}", e);
                    }
                }
            } else {
                tracing::error!("backend returned error for homepage: {}", response.status());
            }
        }
        Err(e) => {
            tracing::error!("failed to fetch homepage from backend: {}", e);
        }
    }
}

/// Refresh blobs cache.
async fn refresh_blobs_cache(state: &AppState) {
    let url = format!("{}/v1/app_blobs", state.backend_url);
    let cache_key = "v1/app_blobs?".to_string();

    match state.http_client.get(&url).send().await {
        Ok(response) => {
            if response.status().is_success() {
                match response.text().await {
                    Ok(text) => {
                        let mut cache = state.cache.write().await;
                        cache.insert(
                            cache_key,
                            CacheEntry {
                                data: text,
                                timestamp: Instant::now(),
                            },
                        );
                        tracing::info!("refreshed cache for /v1/app_blobs");
                    }
                    Err(e) => {
                        tracing::error!("failed to read blobs response: {}", e);
                    }
                }
            } else {
                tracing::error!("backend returned error for blobs: {}", response.status());
            }
        }
        Err(e) => {
            tracing::error!("failed to fetch blobs from backend: {}", e);
        }
    }
}

/// Refresh homepage info cache from PostgreSQL.
async fn refresh_homepage_cache_from_postgres(state: &AppState) {
    let cache_key = "v1/app_info_for_homepage?".to_string();

    match state.query_homepage_info_from_postgres().await {
        Some(info) => match serde_json::to_string(&info) {
            Ok(text) => {
                let mut cache = state.cache.write().await;
                cache.insert(
                    cache_key,
                    CacheEntry {
                        data: text,
                        timestamp: Instant::now(),
                    },
                );
                tracing::info!("refreshed cache for /v1/app_info_for_homepage from postgres");
            }
            Err(e) => {
                tracing::error!("failed to serialize homepage info: {}", e);
            }
        },
        None => {
            tracing::error!("failed to query homepage info from postgres");
        }
    }
}

/// Refresh blobs cache from PostgreSQL.
/// Caches the first 500 blobs so UI can get the first 10 pages quickly.
async fn refresh_blobs_cache_from_postgres(state: &AppState) {
    let cache_key = "v1/app_blobs?".to_string();

    match state.query_blobs_from_postgres(Some(500), None).await {
        Some(response) => match serde_json::to_string(&response) {
            Ok(text) => {
                let mut cache = state.cache.write().await;
                cache.insert(
                    cache_key,
                    CacheEntry {
                        data: text,
                        timestamp: Instant::now(),
                    },
                );
                tracing::info!("refreshed cache for /v1/app_blobs from postgres");
            }
            Err(e) => {
                tracing::error!("failed to serialize blobs: {}", e);
            }
        },
        None => {
            tracing::error!("failed to query blobs from postgres");
        }
    }
}

/// Generic proxy handler that caches responses.
async fn proxy_with_cache<T>(
    state: &AppState,
    endpoint: &str,
    query_string: &str,
) -> Result<Json<T>, StatusCode>
where
    T: for<'de> Deserialize<'de> + Serialize,
{
    let cache_key = format!("{}?{}", endpoint, query_string);
    let url = format!("{}/{}?{}", state.backend_url, endpoint, query_string);

    // Check if we have fresh cached data.
    {
        let cache = state.cache.read().await;
        if let Some(entry) = cache.get(&cache_key) {
            let age = entry.timestamp.elapsed();
            if age < state.cache_freshness_duration {
                // Cache is fresh, return it directly without backend request.
                match serde_json::from_str::<T>(&entry.data) {
                    Ok(data) => {
                        tracing::info!(
                            "serving fresh cached response for: {} (age: {:?})",
                            endpoint,
                            age
                        );
                        return Ok(Json(data));
                    }
                    Err(e) => {
                        tracing::error!("failed to parse cached data: {}", e);
                        // Fall through to try backend.
                    }
                }
            }
        }
    }

    // Try to fetch from backend.
    match state.http_client.get(&url).send().await {
        Ok(response) => {
            if response.status().is_success() {
                match response.text().await {
                    Ok(text) => {
                        // Cache the response.
                        {
                            let mut cache = state.cache.write().await;
                            cache.insert(
                                cache_key.clone(),
                                CacheEntry {
                                    data: text.clone(),
                                    timestamp: Instant::now(),
                                },
                            );
                        }

                        // Parse and return.
                        match serde_json::from_str::<T>(&text) {
                            Ok(data) => {
                                tracing::info!("successfully fetched and cached: {}", endpoint);
                                return Ok(Json(data));
                            }
                            Err(e) => {
                                tracing::error!("failed to parse response from backend: {}", e);
                                // Fall through to cache check.
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!("failed to read response body: {}", e);
                        // Fall through to cache check.
                    }
                }
            } else {
                tracing::warn!("backend returned error status: {}", response.status());
                // Fall through to cache check.
            }
        }
        Err(e) => {
            tracing::error!("failed to contact backend: {}", e);
            // Fall through to cache check.
        }
    }

    // Try to serve from cache (even if stale).
    let cache = state.cache.read().await;
    if let Some(entry) = cache.get(&cache_key) {
        match serde_json::from_str::<T>(&entry.data) {
            Ok(data) => {
                tracing::info!(
                    "serving stale cached response for: {} (age: {:?})",
                    endpoint,
                    entry.timestamp.elapsed()
                );
                return Ok(Json(data));
            }
            Err(e) => {
                tracing::error!("failed to parse cached data: {}", e);
            }
        }
    }

    tracing::error!("no cached data available for: {}", endpoint);
    Err(StatusCode::SERVICE_UNAVAILABLE)
}

/// Handler for /v1/app_blobs_expired_before_epoch.
async fn proxy_blobs_expired_before_epoch(
    State(state): State<AppState>,
    Query(params): Query<BlobsExpiredQuery>,
) -> impl IntoResponse {
    let start_time = Instant::now();
    let endpoint = "app_blobs_expired_before_epoch";
    let source = if state.postgres_pool.is_some() {
        "postgres"
    } else {
        "backend"
    };

    // PostgreSQL mode: query database directly.
    if state.postgres_pool.is_some() {
        let result = match state
            .query_blobs_expired_before_epoch_from_postgres(params.epoch)
            .await
        {
            Some(result) => {
                state
                    .metrics
                    .http_requests_total
                    .with_label_values(&[endpoint, source, "200"])
                    .inc();
                Ok(Json(result))
            }
            None => {
                state
                    .metrics
                    .http_requests_total
                    .with_label_values(&[endpoint, source, "500"])
                    .inc();
                Err(StatusCode::INTERNAL_SERVER_ERROR)
            }
        };
        state
            .metrics
            .http_request_latency_seconds
            .with_label_values(&[endpoint, source])
            .observe(start_time.elapsed().as_secs_f64());
        return result;
    }

    // Backend proxy mode.
    let query_string = format!("epoch={}", params.epoch);
    let result = proxy_with_cache::<Vec<ExpiredBlobInfo>>(
        &state,
        "v1/app_blobs_expired_before_epoch",
        &query_string,
    )
    .await;

    let status = if result.is_ok() { "200" } else { "error" };
    state
        .metrics
        .http_requests_total
        .with_label_values(&[endpoint, source, status])
        .inc();
    state
        .metrics
        .http_request_latency_seconds
        .with_label_values(&[endpoint, source])
        .observe(start_time.elapsed().as_secs_f64());

    result
}

/// Handler for /v1/app_info_for_homepage.
async fn proxy_app_info_for_homepage(State(state): State<AppState>) -> impl IntoResponse {
    let start_time = Instant::now();
    let endpoint = "app_info_for_homepage";
    let source = if state.postgres_pool.is_some() {
        "postgres"
    } else {
        "backend"
    };

    // PostgreSQL mode: query database directly.
    if state.postgres_pool.is_some() {
        let result = match state.query_homepage_info_from_postgres().await {
            Some(result) => {
                state
                    .metrics
                    .http_requests_total
                    .with_label_values(&[endpoint, source, "200"])
                    .inc();
                Ok(Json(result))
            }
            None => {
                state
                    .metrics
                    .http_requests_total
                    .with_label_values(&[endpoint, source, "500"])
                    .inc();
                Err(StatusCode::INTERNAL_SERVER_ERROR)
            }
        };
        state
            .metrics
            .http_request_latency_seconds
            .with_label_values(&[endpoint, source])
            .observe(start_time.elapsed().as_secs_f64());
        return result;
    }

    // Backend proxy mode.
    let result = proxy_with_cache::<HomepageInfo>(&state, "v1/app_info_for_homepage", "").await;

    let status = if result.is_ok() { "200" } else { "error" };
    state
        .metrics
        .http_requests_total
        .with_label_values(&[endpoint, source, status])
        .inc();
    state
        .metrics
        .http_request_latency_seconds
        .with_label_values(&[endpoint, source])
        .observe(start_time.elapsed().as_secs_f64());

    result
}

/// Handler for /v1/app_blobs.
async fn proxy_app_blobs(
    State(state): State<AppState>,
    Query(params): Query<AppBlobsQuery>,
) -> impl IntoResponse {
    let start_time = Instant::now();
    let endpoint = "app_blobs";
    let source = if state.postgres_pool.is_some() {
        "postgres"
    } else {
        "backend"
    };

    // PostgreSQL mode: query database directly with pagination.
    if state.postgres_pool.is_some() {
        let result = match state
            .query_blobs_from_postgres(params.limit, params.cursor)
            .await
        {
            Some(result) => {
                state
                    .metrics
                    .http_requests_total
                    .with_label_values(&[endpoint, source, "200"])
                    .inc();
                Ok(Json(result))
            }
            None => {
                state
                    .metrics
                    .http_requests_total
                    .with_label_values(&[endpoint, source, "500"])
                    .inc();
                Err(StatusCode::INTERNAL_SERVER_ERROR)
            }
        };
        state
            .metrics
            .http_request_latency_seconds
            .with_label_values(&[endpoint, source])
            .observe(start_time.elapsed().as_secs_f64());
        return result;
    }

    // Backend proxy mode - wrap response in AppBlobsResponse for consistency.
    let query_string = match (params.limit, params.cursor) {
        (Some(l), Some(c)) => format!("limit={}&cursor={}", l, c),
        (Some(l), None) => format!("limit={}", l),
        (None, Some(c)) => format!("cursor={}", c),
        (None, None) => String::new(),
    };

    let result =
        match proxy_with_cache::<Vec<AppBlobInfo>>(&state, "v1/app_blobs", &query_string).await {
            Ok(Json(blobs)) => {
                state
                    .metrics
                    .http_requests_total
                    .with_label_values(&[endpoint, source, "200"])
                    .inc();
                Ok(Json(AppBlobsResponse {
                    blobs,
                    next_cursor: None,
                }))
            }
            Err(e) => {
                state
                    .metrics
                    .http_requests_total
                    .with_label_values(&[endpoint, source, "error"])
                    .inc();
                Err(e)
            }
        };
    state
        .metrics
        .http_request_latency_seconds
        .with_label_values(&[endpoint, source])
        .observe(start_time.elapsed().as_secs_f64());

    result
}

/// Fetch checkpoint content from aggregator.
// TODO: move this to a helper crate that can be shared with the archival service.
async fn fetch_checkpoint_content(
    blob_id: &str,
    offset: u64,
    length: u64,
) -> Result<CheckpointData> {
    let url = format!(
        "https://aggregator.walrus-mainnet.walrus.space/v1/blobs/{}/byte-range?start={}&length={}",
        blob_id, offset, length
    );

    tracing::info!("fetching checkpoint content from: {}", url);

    // Fetch the data from the aggregator.
    let client = reqwest::Client::new();
    let response = client
        .get(&url)
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("failed to fetch from aggregator: {}", e))?;

    if !response.status().is_success() {
        return Err(anyhow::anyhow!(
            "aggregator returned error status: {}",
            response.status()
        ));
    }

    let bcs_data = response
        .bytes()
        .await
        .map_err(|e| anyhow::anyhow!("failed to read response body: {}", e))?;

    // Decode using BCS.
    let checkpoint_data = Blob::from_bytes::<CheckpointData>(&bcs_data)
        .map_err(|e| anyhow::anyhow!("failed to decode checkpoint data: {}", e))?;

    Ok(checkpoint_data)
}

/// Handler for /v1/app_checkpoint.
async fn proxy_app_checkpoint(
    State(state): State<AppState>,
    Query(params): Query<AppCheckpointQuery>,
) -> impl IntoResponse {
    let start_time = Instant::now();
    let endpoint = "app_checkpoint";
    let source = if state.postgres_pool.is_some() {
        "postgres"
    } else {
        "backend"
    };

    // PostgreSQL mode: query database directly.
    // Note: show_content requires Walrus, so we forward to backend in that case.
    if state.postgres_pool.is_some() {
        let checkpoint_info = match state
            .query_checkpoint_from_postgres(params.checkpoint)
            .await
        {
            Some(result) => result,
            None => {
                state
                    .metrics
                    .http_requests_total
                    .with_label_values(&[endpoint, source, "404"])
                    .inc();
                state
                    .metrics
                    .http_request_latency_seconds
                    .with_label_values(&[endpoint, source])
                    .observe(start_time.elapsed().as_secs_f64());
                return Err(StatusCode::NOT_FOUND);
            }
        };

        if !params.show_content {
            state
                .metrics
                .http_requests_total
                .with_label_values(&[endpoint, source, "200"])
                .inc();
            state
                .metrics
                .http_request_latency_seconds
                .with_label_values(&[endpoint, source])
                .observe(start_time.elapsed().as_secs_f64());
            return Ok(Json(checkpoint_info));
        }

        let checkpoint_data = match fetch_checkpoint_content(
            &checkpoint_info.blob_id,
            checkpoint_info.offset,
            checkpoint_info.length,
        )
        .await
        {
            Ok(data) => data,
            Err(e) => {
                tracing::error!("failed to fetch checkpoint content: {}", e);
                state
                    .metrics
                    .http_requests_total
                    .with_label_values(&[endpoint, source, "500"])
                    .inc();
                state
                    .metrics
                    .http_request_latency_seconds
                    .with_label_values(&[endpoint, source])
                    .observe(start_time.elapsed().as_secs_f64());
                return Err(StatusCode::INTERNAL_SERVER_ERROR);
            }
        };

        // Serialize checkpoint data and convert Vec<u8> fields to base64.
        let checkpoint_value = match serde_json::to_value(checkpoint_data) {
            Ok(v) => v,
            Err(e) => {
                tracing::error!("failed to serialize checkpoint data: {}", e);
                state
                    .metrics
                    .http_requests_total
                    .with_label_values(&[endpoint, source, "500"])
                    .inc();
                state
                    .metrics
                    .http_request_latency_seconds
                    .with_label_values(&[endpoint, source])
                    .observe(start_time.elapsed().as_secs_f64());
                return Err(StatusCode::INTERNAL_SERVER_ERROR);
            }
        };
        let checkpoint_value_with_base64 = convert_bytes_to_base64(checkpoint_value);

        state
            .metrics
            .http_requests_total
            .with_label_values(&[endpoint, source, "200"])
            .inc();
        state
            .metrics
            .http_request_latency_seconds
            .with_label_values(&[endpoint, source])
            .observe(start_time.elapsed().as_secs_f64());

        Ok(Json(AppCheckpointInfo {
            checkpoint_number: checkpoint_info.checkpoint_number,
            blob_id: checkpoint_info.blob_id,
            object_id: checkpoint_info.object_id,
            index: checkpoint_info.index,
            offset: checkpoint_info.offset,
            length: checkpoint_info.length,
            content: Some(checkpoint_value_with_base64),
        }))
    } else {
        // Backend proxy mode.
        let query_string = if params.show_content {
            format!("checkpoint={}&show_content=true", params.checkpoint)
        } else {
            format!("checkpoint={}", params.checkpoint)
        };
        let result =
            proxy_with_cache::<AppCheckpointInfo>(&state, "v1/app_checkpoint", &query_string).await;

        let status = if result.is_ok() { "200" } else { "error" };
        state
            .metrics
            .http_requests_total
            .with_label_values(&[endpoint, source, status])
            .inc();
        state
            .metrics
            .http_request_latency_seconds
            .with_label_values(&[endpoint, source])
            .observe(start_time.elapsed().as_secs_f64());

        result
    }
}

/// Handler for /v1/app_refresh_blob_end_epoch.
/// This endpoint always forwards to backend without caching.
async fn proxy_refresh_blob_end_epoch(
    State(state): State<AppState>,
    Json(request): Json<RefreshBlobEndEpochRequest>,
) -> impl IntoResponse {
    let start_time = Instant::now();
    let endpoint = "app_refresh_blob_end_epoch";
    let source = "backend"; // Always goes to backend

    state.metrics.backend_requests_total.inc();

    let url = format!("{}/v1/app_refresh_blob_end_epoch", state.backend_url);

    // Always forward to backend without caching.
    let result = match state.http_client.post(&url).json(&request).send().await {
        Ok(response) => {
            if response.status().is_success() {
                match response.json::<RefreshBlobEndEpochResponse>().await {
                    Ok(data) => {
                        tracing::info!("successfully forwarded refresh blob end epoch request");
                        state
                            .metrics
                            .http_requests_total
                            .with_label_values(&[endpoint, source, "200"])
                            .inc();
                        Ok(Json(data))
                    }
                    Err(e) => {
                        tracing::error!("failed to parse response from backend: {}", e);
                        state.metrics.backend_request_failures.inc();
                        state
                            .metrics
                            .http_requests_total
                            .with_label_values(&[endpoint, source, "500"])
                            .inc();
                        Err(StatusCode::INTERNAL_SERVER_ERROR)
                    }
                }
            } else {
                tracing::error!("backend returned error status: {}", response.status());
                state.metrics.backend_request_failures.inc();
                state
                    .metrics
                    .http_requests_total
                    .with_label_values(&[endpoint, source, &response.status().as_u16().to_string()])
                    .inc();
                Err(response.status())
            }
        }
        Err(e) => {
            tracing::error!("failed to contact backend: {}", e);
            state.metrics.backend_request_failures.inc();
            state
                .metrics
                .http_requests_total
                .with_label_values(&[endpoint, source, "502"])
                .inc();
            Err(StatusCode::BAD_GATEWAY)
        }
    };

    state
        .metrics
        .http_request_latency_seconds
        .with_label_values(&[endpoint, source])
        .observe(start_time.elapsed().as_secs_f64());
    state
        .metrics
        .backend_request_latency_seconds
        .observe(start_time.elapsed().as_secs_f64());

    result
}

/// Handler for /v1/health endpoint.
async fn health_check() -> StatusCode {
    StatusCode::OK
}

/// Handler for /metrics endpoint.
async fn metrics_handler(State(state): State<AppState>) -> impl IntoResponse {
    state.metrics.encode()
}
