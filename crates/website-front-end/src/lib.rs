// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

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
use postgres_store::{PostgresPool, SharedPostgresPool};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tower_http::cors::CorsLayer;

/// Configuration for the website front-end.
#[derive(Debug, Clone)]
pub struct Config {
    /// The backend URL to proxy requests to (used as fallback).
    pub backend_url: String,
    /// The address to bind the server to.
    pub bind_address: SocketAddr,
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
            cache_freshness_secs,
            cache_refresh_interval_secs,
            database_url,
        }
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
}

impl AppState {
    /// Query homepage info directly from PostgreSQL.
    async fn query_homepage_info_from_postgres(&self) -> Option<HomepageInfo> {
        let pool = self.postgres_pool.as_ref()?;

        // Get blob count and stats.
        let blobs = match pool.list_all_blobs(false).await {
            Ok(blobs) => blobs,
            Err(e) => {
                tracing::error!("failed to list blobs from postgres: {}", e);
                return None;
            }
        };

        if blobs.is_empty() {
            return Some(HomepageInfo {
                blob_count: 0,
                total_checkpoints: 0,
                earliest_checkpoint: 0,
                latest_checkpoint: 0,
                total_size: 0,
                metadata_info: None,
            });
        }

        let blob_count = blobs.len();
        let earliest_checkpoint = blobs
            .first()
            .map(|b| b.start_checkpoint as u64)
            .unwrap_or(0);
        let latest_checkpoint = blobs.last().map(|b| b.end_checkpoint as u64).unwrap_or(0);
        let total_checkpoints = if latest_checkpoint >= earliest_checkpoint {
            latest_checkpoint - earliest_checkpoint + 1
        } else {
            0
        };

        // Calculate total size by summing index entries.
        // For simplicity, we estimate based on checkpoint count * average size.
        // A more accurate approach would query total size from index entries.
        let total_size = 0u64; // We don't have size info in the blob table directly.

        Some(HomepageInfo {
            blob_count,
            total_checkpoints,
            earliest_checkpoint,
            latest_checkpoint,
            total_size,
            metadata_info: None, // Metadata info requires on-chain query.
        })
    }

    /// Query all blobs directly from PostgreSQL.
    async fn query_blobs_from_postgres(&self) -> Option<Vec<AppBlobInfo>> {
        let pool = self.postgres_pool.as_ref()?;

        let blobs = match pool.list_all_blobs(true).await {
            Ok(blobs) => blobs,
            Err(e) => {
                tracing::error!("failed to list blobs from postgres: {}", e);
                return None;
            }
        };

        let mut result = Vec::with_capacity(blobs.len());
        for blob in blobs {
            // Get index entries for this blob to calculate entries_count and total_size.
            let (entries_count, total_size) =
                match pool.get_index_entries(blob.start_checkpoint).await {
                    Ok(entries) => {
                        let count = entries.len();
                        let size: i64 = entries.iter().map(|e| e.length_bytes).sum();
                        (count, size as u64)
                    }
                    Err(_) => (0, 0),
                };

            result.push(AppBlobInfo {
                blob_id: blob.blob_id,
                object_id: blob.object_id,
                start_checkpoint: blob.start_checkpoint as u64,
                end_checkpoint: blob.end_checkpoint as u64,
                end_of_epoch: blob.end_of_epoch,
                expiry_epoch: blob.blob_expiration_epoch as u32,
                is_shared_blob: blob.is_shared_blob,
                entries_count,
                total_size,
            });
        }

        Some(result)
    }

    /// Query blobs expired before a given epoch from PostgreSQL.
    async fn query_blobs_expired_before_epoch_from_postgres(
        &self,
        epoch: u32,
    ) -> Option<Vec<ExpiredBlobInfo>> {
        let pool = self.postgres_pool.as_ref()?;

        let blobs = match pool.list_all_blobs(false).await {
            Ok(blobs) => blobs,
            Err(e) => {
                tracing::error!("failed to list blobs from postgres: {}", e);
                return None;
            }
        };

        let result: Vec<ExpiredBlobInfo> = blobs
            .into_iter()
            .filter(|b| (b.blob_expiration_epoch as u32) < epoch)
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

/// Start the website front-end server.
pub async fn start_server(config: Config) -> Result<()> {
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
        .with_state(app_state)
        .layer(CorsLayer::permissive());

    tracing::info!(
        "starting website front-end server on {} with backend {}",
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
        tokio::time::sleep(state.cache_refresh_interval).await;
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
async fn refresh_blobs_cache_from_postgres(state: &AppState) {
    let cache_key = "v1/app_blobs?".to_string();

    match state.query_blobs_from_postgres().await {
        Some(blobs) => match serde_json::to_string(&blobs) {
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
    // PostgreSQL mode: query database directly.
    if state.postgres_pool.is_some() {
        return match state
            .query_blobs_expired_before_epoch_from_postgres(params.epoch)
            .await
        {
            Some(result) => Ok(Json(result)),
            None => Err(StatusCode::INTERNAL_SERVER_ERROR),
        };
    }

    // Backend proxy mode.
    let query_string = format!("epoch={}", params.epoch);
    proxy_with_cache::<Vec<ExpiredBlobInfo>>(
        &state,
        "v1/app_blobs_expired_before_epoch",
        &query_string,
    )
    .await
}

/// Handler for /v1/app_info_for_homepage.
async fn proxy_app_info_for_homepage(State(state): State<AppState>) -> impl IntoResponse {
    // PostgreSQL mode: query database directly.
    if state.postgres_pool.is_some() {
        return match state.query_homepage_info_from_postgres().await {
            Some(result) => Ok(Json(result)),
            None => Err(StatusCode::INTERNAL_SERVER_ERROR),
        };
    }

    // Backend proxy mode.
    proxy_with_cache::<HomepageInfo>(&state, "v1/app_info_for_homepage", "").await
}

/// Handler for /v1/app_blobs.
async fn proxy_app_blobs(State(state): State<AppState>) -> impl IntoResponse {
    // PostgreSQL mode: query database directly.
    if state.postgres_pool.is_some() {
        return match state.query_blobs_from_postgres().await {
            Some(result) => Ok(Json(result)),
            None => Err(StatusCode::INTERNAL_SERVER_ERROR),
        };
    }

    // Backend proxy mode.
    proxy_with_cache::<Vec<AppBlobInfo>>(&state, "v1/app_blobs", "").await
}

/// Handler for /v1/app_checkpoint.
async fn proxy_app_checkpoint(
    State(state): State<AppState>,
    Query(params): Query<AppCheckpointQuery>,
) -> impl IntoResponse {
    // PostgreSQL mode: query database directly.
    // Note: show_content requires Walrus, so we forward to backend in that case.
    if state.postgres_pool.is_some() && !params.show_content {
        return match state
            .query_checkpoint_from_postgres(params.checkpoint)
            .await
        {
            Some(result) => Ok(Json(result)),
            None => Err(StatusCode::NOT_FOUND),
        };
    }

    // Backend proxy mode (or show_content requested).
    let query_string = if params.show_content {
        format!("checkpoint={}&show_content=true", params.checkpoint)
    } else {
        format!("checkpoint={}", params.checkpoint)
    };
    proxy_with_cache::<AppCheckpointInfo>(&state, "v1/app_checkpoint", &query_string).await
}

/// Handler for /v1/app_refresh_blob_end_epoch.
/// This endpoint always forwards to backend without caching.
async fn proxy_refresh_blob_end_epoch(
    State(state): State<AppState>,
    Json(request): Json<RefreshBlobEndEpochRequest>,
) -> impl IntoResponse {
    let url = format!("{}/v1/app_refresh_blob_end_epoch", state.backend_url);

    // Always forward to backend without caching.
    match state.http_client.post(&url).json(&request).send().await {
        Ok(response) => {
            if response.status().is_success() {
                match response.json::<RefreshBlobEndEpochResponse>().await {
                    Ok(data) => {
                        tracing::info!("successfully forwarded refresh blob end epoch request");
                        Ok(Json(data))
                    }
                    Err(e) => {
                        tracing::error!("failed to parse response from backend: {}", e);
                        Err(StatusCode::INTERNAL_SERVER_ERROR)
                    }
                }
            } else {
                tracing::error!("backend returned error status: {}", response.status());
                Err(response.status())
            }
        }
        Err(e) => {
            tracing::error!("failed to contact backend: {}", e);
            Err(StatusCode::BAD_GATEWAY)
        }
    }
}

/// Handler for /v1/health endpoint.
async fn health_check() -> StatusCode {
    StatusCode::OK
}
