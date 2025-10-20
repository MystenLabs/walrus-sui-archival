// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use anyhow::Result;
use axum::{
    Json,
    Router,
    body::Body,
    extract::{Query, State},
    http::{StatusCode, header},
    response::{Html, IntoResponse, Response},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use sui_storage::blob::Blob;
use sui_types::{
    full_checkpoint_content::CheckpointData,
    messages_checkpoint::CheckpointSequenceNumber,
};
use tower_http::cors::CorsLayer;
use walrus_core::{BlobId, encoding::Primary};
use walrus_sdk::{ObjectID, SuiReadClient, client::WalrusNodeClient};

use crate::{
    archival_state::{ArchivalState, proto},
    config::ArchivalStateSnapshotConfig,
    util,
};

/// Format size in bytes to human-readable format.
fn format_size(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    const TB: u64 = GB * 1024;

    if bytes >= TB {
        format!("{:.2} TB", bytes as f64 / TB as f64)
    } else if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

/// REST API server for the archival system.
pub struct RestApiServer {
    address: SocketAddr,
    archival_state: Arc<ArchivalState>,
    walrus_client: Arc<WalrusNodeClient<SuiReadClient>>,
    snapshot_config: Option<ArchivalStateSnapshotConfig>,
    client_config_path: PathBuf,
    context: String,
}

impl RestApiServer {
    /// Create a new REST API server.
    pub fn new(
        address: SocketAddr,
        archival_state: Arc<ArchivalState>,
        walrus_client: Arc<WalrusNodeClient<SuiReadClient>>,
        snapshot_config: Option<ArchivalStateSnapshotConfig>,
        client_config_path: PathBuf,
        context: String,
    ) -> Self {
        Self {
            address,
            archival_state,
            walrus_client,
            snapshot_config,
            client_config_path,
            context,
        }
    }

    /// Start the REST API server.
    pub async fn start(self) -> Result<()> {
        let app_state = AppState {
            archival_state: self.archival_state,
            walrus_client: self.walrus_client,
            snapshot_config: self.snapshot_config,
            client_config_path: self.client_config_path,
            context: self.context,
        };

        let app = Router::new()
            .route("/", get(home_page))
            .route("/v1/blobs", get(list_all_blobs))
            .route(
                "/v1/app_blobs_expired_before_epoch",
                get(get_blobs_expired_before_epoch),
            )
            .route("/v1/app_info_for_homepage", get(get_app_info_for_homepage))
            .route("/v1/app_blobs", get(get_app_blobs))
            .route("/v1/app_checkpoint", get(get_app_checkpoint))
            .route(
                "/v1/app_refresh_blob_end_epoch",
                post(refresh_blob_end_epoch),
            )
            .route("/v1/health", get(health_check))
            .route("/v1/checkpoint", get(get_checkpoint))
            .route("/resources/walrus_image.png", get(serve_walrus_image))
            .with_state(app_state)
            .layer(CorsLayer::permissive()); // Allow all CORS requests.

        tracing::info!("starting REST API server on {}", self.address);

        let listener = tokio::net::TcpListener::bind(self.address)
            .await
            .map_err(|e| anyhow::anyhow!("failed to bind to {}: {}", self.address, e))?;

        axum::serve(listener, app)
            .await
            .map_err(|e| anyhow::anyhow!("REST API server error: {}", e))?;

        Ok(())
    }
}

#[derive(Clone)]
struct AppState {
    archival_state: Arc<ArchivalState>,
    walrus_client: Arc<WalrusNodeClient<SuiReadClient>>,
    snapshot_config: Option<ArchivalStateSnapshotConfig>,
    client_config_path: PathBuf,
    context: String,
}

/// Handler for listing all blobs.
async fn list_all_blobs(State(app_state): State<AppState>) -> Result<Html<String>, StatusCode> {
    let archival_state = app_state.archival_state;
    // Get all blobs from the database.
    let blobs = archival_state.list_all_blobs().map_err(|e| {
        tracing::error!("failed to list blobs: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    // Build HTML response similar to inspect_db output.
    let mut html = String::new();
    html.push_str("<!DOCTYPE html>\n");
    html.push_str("<html>\n");
    html.push_str("<head>\n");
    html.push_str("<title>Walrus Sui Archival - All Blobs</title>\n");
    html.push_str("<style>\n");
    html.push_str("body { font-family: monospace; margin: 20px; }\n");
    html.push_str("h1 { color: #333; }\n");
    html.push_str("table { border-collapse: collapse; width: 100%; margin-top: 20px; }\n");
    html.push_str("th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }\n");
    html.push_str("th { background-color: #f2f2f2; font-weight: bold; }\n");
    html.push_str("tr:hover { background-color: #f5f5f5; }\n");
    html.push_str(".summary { margin-top: 20px; padding: 10px; background-color: #f9f9f9; border: 1px solid #ddd; }\n");
    html.push_str(".home-link { display: inline-block; margin-bottom: 20px; color: #667eea; text-decoration: none; font-size: 1.1em; }\n");
    html.push_str(".home-link:hover { text-decoration: underline; }\n");
    html.push_str("</style>\n");
    html.push_str("</head>\n");
    html.push_str("<body>\n");
    html.push_str("<a href=\"/\" class=\"home-link\">← Back to Home</a>\n");
    html.push_str("<h1>Walrus Sui Archival - Blob List</h1>\n");

    if blobs.is_empty() {
        html.push_str("<p>No blobs tracked in the database.</p>\n");
    } else {
        html.push_str(&format!(
            "<p>Found {} blob(s) in the database:</p>\n",
            blobs.len()
        ));

        // Create table.
        html.push_str("<table>\n");
        html.push_str("<tr>\n");
        html.push_str("<th>Blob ID</th>\n");
        html.push_str("<th>Object ID</th>\n");
        html.push_str("<th>Start Checkpoint</th>\n");
        html.push_str("<th>End Checkpoint</th>\n");
        html.push_str("<th>End of Epoch</th>\n");
        html.push_str("<th>Expiry Epoch</th>\n");
        html.push_str("<th>Shared Blob</th>\n");
        html.push_str("<th>Entries</th>\n");
        html.push_str("<th>Size</th>\n");
        html.push_str("</tr>\n");

        let mut total_entries = 0;
        let mut total_size = 0u64;

        for blob_info in &blobs {
            let blob_id = String::from_utf8_lossy(&blob_info.blob_id);
            let object_id = ObjectID::from_bytes(&blob_info.object_id).unwrap_or(ObjectID::ZERO);
            let entries_count = blob_info.index_entries.len();
            let blob_size: u64 = blob_info.index_entries.iter().map(|e| e.length).sum();

            total_entries += entries_count;
            total_size += blob_size;

            html.push_str("<tr>\n");
            html.push_str(&format!("<td>{}</td>\n", blob_id));
            html.push_str(&format!("<td>{}</td>\n", object_id));
            html.push_str(&format!("<td>{}</td>\n", blob_info.start_checkpoint));
            html.push_str(&format!("<td>{}</td>\n", blob_info.end_checkpoint));
            html.push_str(&format!(
                "<td>{}</td>\n",
                if blob_info.end_of_epoch { "Yes" } else { "No" }
            ));
            html.push_str(&format!("<td>{}</td>\n", blob_info.blob_expiration_epoch));
            html.push_str(&format!(
                "<td>{}</td>\n",
                if blob_info.is_shared_blob {
                    "Yes"
                } else {
                    "No"
                }
            ));
            html.push_str(&format!("<td>{}</td>\n", entries_count));
            html.push_str(&format!("<td>{}</td>\n", format_size(blob_size)));
            html.push_str("</tr>\n");
        }

        html.push_str("</table>\n");

        // Add summary.
        html.push_str("<div class=\"summary\">\n");
        html.push_str("<h2>Summary</h2>\n");
        html.push_str(&format!("<p>Total blobs: {}</p>\n", blobs.len()));
        html.push_str(&format!(
            "<p>Total checkpoint entries: {}</p>\n",
            total_entries
        ));
        html.push_str(&format!(
            "<p>Total data size: {}</p>\n",
            format_size(total_size)
        ));
        html.push_str("</div>\n");
    }

    html.push_str("</body>\n");
    html.push_str("</html>\n");

    Ok(Html(html))
}

/// Query parameters for blobs expired before epoch endpoint.
#[derive(Deserialize)]
struct BlobsExpiredQuery {
    epoch: u32,
}

/// Response structure for expired blobs.
#[derive(Serialize)]
struct ExpiredBlobInfo {
    blob_id: String,
    object_id: String,
    end_epoch: u32,
}

/// Response structure for homepage info.
#[derive(Serialize)]
struct HomepageInfo {
    blob_count: usize,
    total_checkpoints: u64,
    earliest_checkpoint: u64,
    latest_checkpoint: u64,
    total_size: u64,
    metadata_info: Option<MetadataInfo>,
}

/// Metadata information structure.
#[derive(Serialize)]
struct MetadataInfo {
    metadata_pointer_object_id: String,
    contract_package_id: String,
    current_metadata_blob_id: Option<String>,
}

/// Response structure for app blobs endpoint.
#[derive(Serialize)]
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
#[derive(Serialize)]
struct AppCheckpointInfo {
    checkpoint_number: u64,
    blob_id: String,
    object_id: String,
    index: usize,
    offset: u64,
    length: u64,
}

/// Request structure for refresh blob end epoch endpoint.
#[derive(Deserialize)]
struct RefreshBlobEndEpochRequest {
    object_ids: Vec<String>,
}

/// Response structure for refresh blob end epoch endpoint.
#[derive(Serialize)]
struct RefreshBlobEndEpochResponse {
    message: String,
    count: usize,
}

/// Handler for getting blobs that expire before a given epoch.
async fn get_blobs_expired_before_epoch(
    State(app_state): State<AppState>,
    Query(params): Query<BlobsExpiredQuery>,
) -> Result<Json<Vec<ExpiredBlobInfo>>, StatusCode> {
    let archival_state = app_state.archival_state;

    // Get all blobs from the database.
    let blobs = archival_state.list_all_blobs().map_err(|e| {
        tracing::error!("failed to list blobs: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    // Filter blobs that expire before or at the given epoch.
    let mut expired_blobs: Vec<ExpiredBlobInfo> = blobs
        .iter()
        .filter(|blob| blob.blob_expiration_epoch <= params.epoch)
        .map(|blob| {
            let object_id = ObjectID::from_bytes(&blob.object_id)
                .map(|id| id.to_string())
                .expect("failed to parse object ID");

            ExpiredBlobInfo {
                blob_id: String::from_utf8_lossy(&blob.blob_id).to_string(),
                object_id,
                end_epoch: blob.blob_expiration_epoch,
            }
        })
        .collect();

    // Sort by end_epoch in ascending order.
    expired_blobs.sort_by_key(|blob| blob.end_epoch);

    tracing::info!(
        "found {} blobs expiring before or at epoch {}",
        expired_blobs.len(),
        params.epoch
    );

    Ok(Json(expired_blobs))
}

/// Handler for getting app homepage information as JSON.
async fn get_app_info_for_homepage(
    State(app_state): State<AppState>,
) -> Result<Json<HomepageInfo>, StatusCode> {
    let archival_state = app_state.archival_state;

    // Get statistics from the database.
    let total_blobs = archival_state.list_all_blobs().map_err(|e| {
        tracing::error!("failed to list blobs: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let blob_count = total_blobs.len();
    let latest_checkpoint = archival_state
        .get_latest_stored_checkpoint()
        .map_err(|e| {
            tracing::error!("failed to get latest checkpoint: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .unwrap_or(0);

    // Calculate total checkpoints and size.
    let mut total_checkpoints = 0u64;
    let mut total_size = 0u64;
    let mut earliest_checkpoint = u64::MAX;

    for blob in &total_blobs {
        let checkpoint_count = blob.end_checkpoint - blob.start_checkpoint + 1;
        total_checkpoints += checkpoint_count;

        if blob.start_checkpoint < earliest_checkpoint {
            earliest_checkpoint = blob.start_checkpoint;
        }

        let blob_size: u64 = blob.index_entries.iter().map(|e| e.length).sum();
        total_size += blob_size;
    }

    if earliest_checkpoint == u64::MAX {
        earliest_checkpoint = 0;
    }

    // Fetch metadata blob ID if snapshot config is available.
    let metadata_info = if let Some(config) = app_state.snapshot_config.as_ref() {
        let blob_id = util::fetch_metadata_blob_id(
            &app_state.client_config_path,
            config.metadata_pointer_object_id,
            &app_state.context,
        )
        .await
        .ok()
        .flatten();

        Some(MetadataInfo {
            metadata_pointer_object_id: config.metadata_pointer_object_id.to_string(),
            contract_package_id: config.contract_package_id.to_string(),
            current_metadata_blob_id: blob_id.map(|id| id.to_string()),
        })
    } else {
        None
    };

    let response = HomepageInfo {
        blob_count,
        total_checkpoints,
        earliest_checkpoint,
        latest_checkpoint,
        total_size,
        metadata_info,
    };

    Ok(Json(response))
}

/// Handler for getting all blobs as JSON.
async fn get_app_blobs(
    State(app_state): State<AppState>,
) -> Result<Json<Vec<AppBlobInfo>>, StatusCode> {
    let archival_state = app_state.archival_state;

    // Get all blobs from the database.
    let blobs = archival_state.list_all_blobs().map_err(|e| {
        tracing::error!("failed to list blobs: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    // Convert to app blob info format.
    let app_blobs: Vec<AppBlobInfo> = blobs
        .iter()
        .map(|blob_info| {
            let blob_id = String::from_utf8_lossy(&blob_info.blob_id).to_string();
            let object_id = ObjectID::from_bytes(&blob_info.object_id)
                .map(|id| id.to_string())
                .unwrap_or_else(|_| ObjectID::ZERO.to_string());
            let entries_count = blob_info.index_entries.len();
            let total_size: u64 = blob_info.index_entries.iter().map(|e| e.length).sum();

            AppBlobInfo {
                blob_id,
                object_id,
                start_checkpoint: blob_info.start_checkpoint,
                end_checkpoint: blob_info.end_checkpoint,
                end_of_epoch: blob_info.end_of_epoch,
                expiry_epoch: blob_info.blob_expiration_epoch,
                is_shared_blob: blob_info.is_shared_blob,
                entries_count,
                total_size,
            }
        })
        .collect();

    Ok(Json(app_blobs))
}

/// Query parameters for app checkpoint endpoint.
#[derive(Deserialize)]
struct AppCheckpointQuery {
    checkpoint: u64,
}

/// Handler for getting checkpoint information as JSON.
async fn get_app_checkpoint(
    State(app_state): State<AppState>,
    Query(params): Query<AppCheckpointQuery>,
) -> Result<Json<AppCheckpointInfo>, StatusCode> {
    let checkpoint_num = CheckpointSequenceNumber::from(params.checkpoint);

    // Get checkpoint blob info from database.
    let blob_info = app_state
        .archival_state
        .get_checkpoint_blob_info(checkpoint_num)
        .await
        .map_err(|e| {
            tracing::error!("failed to get checkpoint blob info: {}", e);
            StatusCode::NOT_FOUND
        })?;

    // Find the index entry for this checkpoint.
    let (index, entry) = blob_info
        .index_entries
        .iter()
        .enumerate()
        .find(|(_, e)| e.checkpoint_number == checkpoint_num)
        .ok_or_else(|| {
            tracing::error!("checkpoint {} not found in blob index", checkpoint_num);
            StatusCode::NOT_FOUND
        })?;

    let blob_id = String::from_utf8_lossy(&blob_info.blob_id).to_string();
    let object_id = ObjectID::from_bytes(&blob_info.object_id)
        .map(|id| id.to_string())
        .unwrap_or_else(|_| ObjectID::ZERO.to_string());

    let response = AppCheckpointInfo {
        checkpoint_number: params.checkpoint,
        blob_id,
        object_id,
        index,
        offset: entry.offset,
        length: entry.length,
    };

    Ok(Json(response))
}

/// Handler for refreshing blob end epochs.
async fn refresh_blob_end_epoch(
    State(app_state): State<AppState>,
    Json(request): Json<RefreshBlobEndEpochRequest>,
) -> Result<Json<RefreshBlobEndEpochResponse>, StatusCode> {
    let object_ids = request.object_ids;
    let count = object_ids.len();

    tracing::info!("received request to refresh {} blob end epochs", count);

    // Parse object IDs.
    let mut parsed_object_ids = Vec::new();
    for object_id_str in &object_ids {
        match ObjectID::from_hex_literal(object_id_str) {
            Ok(object_id) => parsed_object_ids.push(object_id),
            Err(e) => {
                tracing::error!("failed to parse object ID {}: {}", object_id_str, e);
                return Err(StatusCode::BAD_REQUEST);
            }
        }
    }

    // Clone the necessary state for the background task.
    let archival_state = app_state.archival_state.clone();
    let walrus_client = app_state.walrus_client.clone();

    // Spawn a background task to refresh the blob end epochs.
    tokio::spawn(async move {
        tracing::info!(
            "starting background task to refresh {} blob end epochs",
            parsed_object_ids.len()
        );

        // Get all blobs from the database once.
        let all_blobs = match archival_state.list_all_blobs() {
            Ok(blobs) => blobs,
            Err(e) => {
                tracing::error!("failed to list blobs: {}", e);
                return;
            }
        };

        for object_id in parsed_object_ids {
            match refresh_single_blob_end_epoch(
                &archival_state,
                &walrus_client,
                &all_blobs,
                &object_id,
            )
            .await
            {
                Ok(new_epoch) => {
                    tracing::info!(
                        "refreshed blob end epoch for object {}: new epoch {}",
                        object_id,
                        new_epoch
                    );
                }
                Err(e) => {
                    tracing::error!(
                        "failed to refresh blob end epoch for object {}: {}",
                        object_id,
                        e
                    );
                }
            }
        }

        tracing::info!("background task completed: refreshed blob end epochs");
    });

    let response = RefreshBlobEndEpochResponse {
        message: "blob end epoch refresh task started".to_string(),
        count,
    };

    Ok(Json(response))
}

/// Refresh the end epoch for a single blob.
async fn refresh_single_blob_end_epoch(
    archival_state: &Arc<ArchivalState>,
    walrus_client: &Arc<WalrusNodeClient<SuiReadClient>>,
    all_blobs: &[proto::CheckpointBlobInfo],
    object_id: &ObjectID,
) -> Result<u32> {
    // Get the blob from the blockchain.
    let blob_response = walrus_client
        .get_blob_by_object_id(object_id)
        .await
        .map_err(|e| anyhow::anyhow!("failed to get blob: {}", e))?;

    let new_end_epoch = blob_response.blob.storage.end_epoch;
    let blob_id = blob_response.blob.blob_id;

    // Find the blob in the database by object_id.
    let blob_info = all_blobs
        .iter()
        .find(|b| {
            ObjectID::from_bytes(&b.object_id)
                .map(|id| id == *object_id)
                .unwrap_or(false)
        })
        .ok_or_else(|| {
            anyhow::anyhow!("blob with object_id {} not found in database", object_id)
        })?;

    // Update the blob's expiration epoch.
    archival_state.update_blob_expiration_epoch(
        blob_info.start_checkpoint,
        &blob_id,
        object_id,
        new_end_epoch,
    )?;

    Ok(new_end_epoch)
}

/// Handler for health check endpoint.
async fn health_check() -> StatusCode {
    StatusCode::OK
}

/// Handler for serving the walrus image.
async fn serve_walrus_image() -> impl IntoResponse {
    let image_path = std::path::Path::new("/opt/walrus/resources/walrus_image.png");
    match tokio::fs::read(image_path).await {
        Ok(contents) => Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "image/png")
            .body(Body::from(contents))
            .unwrap(),
        Err(_) => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::from("Image not found"))
            .unwrap(),
    }
}

/// Handler for home page.
async fn home_page(State(app_state): State<AppState>) -> Result<Html<String>, StatusCode> {
    let archival_state = app_state.archival_state;

    // Get statistics from the database.
    let total_blobs = archival_state.list_all_blobs().map_err(|e| {
        tracing::error!("failed to list blobs: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let blob_count = total_blobs.len();
    let latest_checkpoint = archival_state.get_latest_stored_checkpoint().map_err(|e| {
        tracing::error!("failed to get latest checkpoint: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    // Calculate total checkpoints and size.
    let mut total_checkpoints = 0u64;
    let mut total_size = 0u64;
    let mut earliest_checkpoint = u64::MAX;

    for blob in &total_blobs {
        let checkpoint_count = blob.end_checkpoint - blob.start_checkpoint + 1;
        total_checkpoints += checkpoint_count;

        if blob.start_checkpoint < earliest_checkpoint {
            earliest_checkpoint = blob.start_checkpoint;
        }

        let blob_size: u64 = blob.index_entries.iter().map(|e| e.length).sum();
        total_size += blob_size;
    }

    if earliest_checkpoint == u64::MAX {
        earliest_checkpoint = 0;
    }

    // Fetch metadata blob ID if snapshot config is available.
    let metadata_info = if let Some(config) = app_state.snapshot_config.as_ref() {
        let blob_id = util::fetch_metadata_blob_id(
            &app_state.client_config_path,
            config.metadata_pointer_object_id,
            &app_state.context,
        )
        .await
        .ok()
        .flatten();

        Some((
            config.metadata_pointer_object_id,
            config.contract_package_id,
            blob_id,
        ))
    } else {
        None
    };

    let html = build_home_page(
        blob_count,
        total_checkpoints,
        earliest_checkpoint,
        latest_checkpoint.unwrap_or(0),
        total_size,
        metadata_info,
    );

    Ok(Html(html))
}

/// Query parameters for checkpoint endpoint.
#[derive(Deserialize)]
struct CheckpointQuery {
    #[serde(default)]
    checkpoint: Option<u64>,
    #[serde(default)]
    show_content: bool,
}

/// Handler for getting checkpoint information.
async fn get_checkpoint(
    State(app_state): State<AppState>,
    Query(params): Query<CheckpointQuery>,
) -> Result<Html<String>, StatusCode> {
    // If no checkpoint provided, show the input form.
    let Some(checkpoint_num_u64) = params.checkpoint else {
        return Ok(Html(build_checkpoint_form(None, None, false)));
    };
    let checkpoint_num = CheckpointSequenceNumber::from(checkpoint_num_u64);

    // Get checkpoint blob info from database.
    let blob_info = match app_state
        .archival_state
        .get_checkpoint_blob_info(checkpoint_num)
        .await
    {
        Ok(info) => info,
        Err(e) => {
            // Check if it's a not found error.
            let error_msg = if e
                .to_string()
                .contains("no blob found containing checkpoint")
            {
                tracing::debug!("checkpoint {} not found", checkpoint_num);
                format!("checkpoint {} not found", checkpoint_num_u64)
            } else {
                tracing::error!("failed to get checkpoint blob info: {}", e);
                format!("internal server error: {}", e)
            };
            return Ok(Html(build_checkpoint_form(
                Some(checkpoint_num_u64),
                Some(&error_msg),
                params.show_content,
            )));
        }
    };

    // Find the index entry for this checkpoint.
    let entry = blob_info
        .index_entries
        .iter()
        .enumerate()
        .find(|(_, e)| e.checkpoint_number == checkpoint_num);

    let (index, entry) = match entry {
        Some(e) => e,
        None => {
            tracing::error!("checkpoint {} not found in blob index", checkpoint_num);
            return Ok(Html(build_checkpoint_form(
                Some(checkpoint_num_u64),
                Some(&format!(
                    "checkpoint {} not found in blob index",
                    checkpoint_num_u64
                )),
                params.show_content,
            )));
        }
    };

    let blob_id = String::from_utf8_lossy(&blob_info.blob_id).to_string();
    let object_id = ObjectID::from_bytes(&blob_info.object_id)
        .map(|id| id.to_string())
        .unwrap_or_else(|_| ObjectID::ZERO.to_string());

    // Only fetch checkpoint data if show_content is true.
    let checkpoint_json = if params.show_content {
        // Fetch the checkpoint data from Walrus.
        tracing::info!(
            "fetching checkpoint {} from walrus blob {}",
            checkpoint_num,
            blob_id
        );

        // Parse the blob ID.
        let blob_id_parsed: BlobId = match blob_id.parse() {
            Ok(id) => id,
            Err(e) => {
                tracing::error!("failed to parse blob id: {}", e);
                return Ok(Html(build_checkpoint_form(
                    Some(checkpoint_num_u64),
                    Some(&format!("failed to parse blob id: {}", e)),
                    params.show_content,
                )));
            }
        };

        // Download the specific range from the blob.
        let blob_data = match app_state
            .walrus_client
            .read_blob::<Primary>(&blob_id_parsed)
            .await
        {
            Ok(data) => data,
            Err(e) => {
                tracing::error!("failed to retrieve blob from walrus: {}", e);
                return Ok(Html(build_checkpoint_form(
                    Some(checkpoint_num_u64),
                    Some(&format!("failed to retrieve blob from walrus: {}", e)),
                    params.show_content,
                )));
            }
        };

        // Extract the checkpoint data from the blob.
        let start = entry.offset as usize;
        let end = (entry.offset + entry.length) as usize;
        let checkpoint_bytes = match blob_data.get(start..end) {
            Some(bytes) => bytes,
            None => {
                tracing::error!("invalid offset/length for checkpoint data");
                return Ok(Html(build_checkpoint_form(
                    Some(checkpoint_num_u64),
                    Some("invalid offset/length for checkpoint data"),
                    params.show_content,
                )));
            }
        };

        // Deserialize the checkpoint using Blob::from_bytes.
        let checkpoint = match Blob::from_bytes::<CheckpointData>(checkpoint_bytes) {
            Ok(cp) => cp,
            Err(e) => {
                tracing::error!("failed to deserialize checkpoint: {}", e);
                return Ok(Html(build_checkpoint_form(
                    Some(checkpoint_num_u64),
                    Some(&format!("failed to deserialize checkpoint: {}", e)),
                    params.show_content,
                )));
            }
        };

        // Convert to JSON for display.
        match serde_json::to_string_pretty(&checkpoint) {
            Ok(json) => Some(json),
            Err(e) => {
                tracing::error!("failed to convert checkpoint to json: {}", e);
                return Ok(Html(build_checkpoint_form(
                    Some(checkpoint_num_u64),
                    Some(&format!("failed to convert checkpoint to json: {}", e)),
                    params.show_content,
                )));
            }
        }
    } else {
        None
    };

    // Build response HTML with checkpoint information.
    let html = build_checkpoint_result_page(
        checkpoint_num_u64,
        &blob_id,
        &object_id,
        index,
        entry.offset,
        entry.length,
        checkpoint_json.as_deref(),
    );

    Ok(Html(html))
}

/// Build the checkpoint input form HTML.
fn build_checkpoint_form(
    checkpoint: Option<u64>,
    error: Option<&str>,
    show_content: bool,
) -> String {
    let checkpoint_value = checkpoint.map(|c| c.to_string()).unwrap_or_default();
    let error_html = error
        .map(|e| format!("<p style=\"color: red;\">Error: {}</p>", e))
        .unwrap_or_default();
    let checked = if show_content { "checked" } else { "" };

    format!(
        r#"
<!DOCTYPE html>
<html>
<head>
    <title>Walrus Sui Archival - Checkpoint Lookup</title>
    <style>
        body {{ font-family: monospace; margin: 20px; }}
        h1 {{ color: #333; }}
        .form-container {{ margin-top: 20px; }}
        input[type="number"] {{ padding: 8px; width: 300px; font-size: 16px; }}
        button {{ padding: 8px 16px; font-size: 16px; cursor: pointer; }}
        .error {{ color: red; margin-top: 10px; }}
        .checkbox-container {{ margin: 10px 0; }}
        .home-link {{ display: inline-block; margin-bottom: 20px; color: #667eea; text-decoration: none; font-size: 1.1em; }}
        .home-link:hover {{ text-decoration: underline; }}
    </style>
</head>
<body>
    <a href="/" class="home-link">← Back to Home</a>
    <h1>Walrus Sui Archival - Checkpoint Lookup</h1>
    <div class="form-container">
        <form action="/v1/checkpoint" method="get">
            <label for="checkpoint">Checkpoint Number:</label><br>
            <input type="number" id="checkpoint" name="checkpoint" value="{}" min="0" required><br>
            <div class="checkbox-container">
                <input type="checkbox" id="show_content" name="show_content" value="true" {}>
                <label for="show_content">Show checkpoint data content</label>
                <div style="margin-left: 25px; margin-top: 5px; font-size: 0.9em; color: #666;">
                    <em>Note: This option is for testing purposes only and will be removed later. The proper way to read individual checkpoints is to query the Walrus blob directly.</em>
                </div>
            </div>
            <button type="submit">Lookup Checkpoint</button>
        </form>
        {}
    </div>
</body>
</html>
"#,
        checkpoint_value, checked, error_html
    )
}

/// Build the checkpoint result page HTML.
fn build_checkpoint_result_page(
    checkpoint: u64,
    blob_id: &str,
    object_id: &str,
    index: usize,
    offset: u64,
    length: u64,
    checkpoint_data: Option<&str>,
) -> String {
    let data_section = if let Some(data) = checkpoint_data {
        format!(
            r#"
    <h2>Checkpoint Data</h2>
    <div class="data-container">
        <pre>{}</pre>
    </div>
"#,
            data
        )
    } else {
        String::new()
    };

    format!(
        r#"
<!DOCTYPE html>
<html>
<head>
    <title>Checkpoint {} - Walrus Sui Archival</title>
    <style>
        body {{ font-family: monospace; margin: 20px; }}
        h1 {{ color: #333; }}
        .info-table {{ border-collapse: collapse; margin-top: 20px; }}
        .info-table th, .info-table td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        .info-table th {{ background-color: #f2f2f2; font-weight: bold; }}
        .data-container {{ margin-top: 20px; padding: 10px; background-color: #f9f9f9; border: 1px solid #ddd; overflow-x: auto; }}
        pre {{ white-space: pre-wrap; word-wrap: break-word; }}
        .back-link {{ margin-bottom: 20px; }}
        .home-link {{ display: inline-block; margin-bottom: 20px; color: #667eea; text-decoration: none; font-size: 1.1em; }}
        .home-link:hover {{ text-decoration: underline; }}
    </style>
</head>
<body>
    <a href="/" class="home-link">← Back to Home</a>
    <div class="back-link">
        <a href="/v1/checkpoint">← Back to Lookup</a>
    </div>

    <h1>Checkpoint {}</h1>

    <table class="info-table">
        <tr>
            <th>Field</th>
            <th>Value</th>
        </tr>
        <tr>
            <td>Checkpoint Number</td>
            <td>{}</td>
        </tr>
        <tr>
            <td>Blob ID</td>
            <td>{}</td>
        </tr>
        <tr>
            <td>Object ID</td>
            <td>{}</td>
        </tr>
        <tr>
            <td>Index</td>
            <td>{}</td>
        </tr>
        <tr>
            <td>Offset</td>
            <td>{}</td>
        </tr>
        <tr>
            <td>Length</td>
            <td>{}</td>
        </tr>
    </table>
{}
</body>
</html>
"#,
        checkpoint, checkpoint, checkpoint, blob_id, object_id, index, offset, length, data_section
    )
}

/// Build the home page HTML.
fn build_home_page(
    blob_count: usize,
    total_checkpoints: u64,
    earliest_checkpoint: u64,
    latest_checkpoint: u64,
    total_size: u64,
    metadata_info: Option<(ObjectID, ObjectID, Option<BlobId>)>,
) -> String {
    let size_gb = total_size as f64 / (1024.0 * 1024.0 * 1024.0);

    let (metadata_section, _metadata_stats) =
        if let Some((pointer_id, package_id, blob_id_opt)) = metadata_info {
            let blob_id_display = if let Some(blob_id) = blob_id_opt {
                format!("<code>{}</code>", blob_id)
            } else {
                "<em style=\"color: #999;\">Not set</em>".to_string()
            };

            (
                format!(
                    r#"
        <div class="metadata-section">
            <h2>📋 Metadata Tracking</h2>
            <div class="metadata-info">
                <p><strong>On-Chain Metadata Pointer:</strong> <code>{}</code></p>
                <p><strong>Metadata Package ID:</strong> <code>{}</code></p>
                <p><strong>Current Metadata Blob ID:</strong> {}</p>
                <p class="metadata-description">
                    The archival system maintains an on-chain metadata blob that contains a snapshot
                    of all checkpoint blob information. This enables disaster recovery and quick
                    bootstrapping of new archival nodes.
                </p>
            </div>
        </div>
"#,
                    pointer_id, package_id, blob_id_display
                ),
                String::new(), // Can add metadata stats here if needed
            )
        } else {
            (String::new(), String::new())
        };

    format!(
        r#"
<!DOCTYPE html>
<html>
<head>
    <title>Walrus Sui Archival</title>
    <style>
        body {{
            font-family: monospace;
            margin: 0;
            padding: 20px;
            background: linear-gradient(135deg, {{{{#667eea}}}} 0%, {{{{#764ba2}}}} 100%);
            min-height: 100vh;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            border-radius: 10px;
            padding: 40px;
            box-shadow: 0 10px 40px rgba(0,0,0,0.3);
        }}
        h1 {{
            color: {{{{#333}}}};
            text-align: center;
            margin-bottom: 10px;
            font-size: 2.5em;
        }}
        .subtitle {{
            text-align: center;
            color: {{{{#666}}}};
            margin-bottom: 40px;
            font-size: 1.1em;
        }}
        .walrus-container {{
            text-align: center;
            margin: 30px 0;
        }}
        .walrus-image {{
            max-width: 400px;
            width: 100%;
            height: auto;
            border-radius: 10px;
        }}
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin: 40px 0;
        }}
        .stat-card {{
            background: {{{{#f8f9fa}}}};
            padding: 20px;
            border-radius: 8px;
            border: 1px solid {{{{#dee2e6}}}};
            text-align: center;
        }}
        .stat-value {{
            font-size: 2em;
            font-weight: bold;
            color: {{{{#667eea}}}};
            margin: 10px 0;
        }}
        .stat-label {{
            color: {{{{#666}}}};
            font-size: 0.9em;
            text-transform: uppercase;
            letter-spacing: 1px;
        }}
        .links-section {{
            margin-top: 40px;
            padding-top: 40px;
            border-top: 2px solid {{{{#dee2e6}}}};
        }}
        .links-title {{
            font-size: 1.5em;
            color: {{{{#333}}}};
            margin-bottom: 20px;
        }}
        .link-card {{
            display: block;
            background: {{{{#f8f9fa}}}};
            padding: 20px;
            margin: 15px 0;
            border-radius: 8px;
            border: 1px solid {{{{#dee2e6}}}};
            text-decoration: none;
            color: {{{{#333}}}};
            transition: all 0.3s ease;
        }}
        .link-card:hover {{
            background: {{{{#667eea}}}};
            color: white;
            transform: translateY(-2px);
            box-shadow: 0 4px 12px rgba(102, 126, 234, 0.4);
        }}
        .link-title {{
            font-size: 1.2em;
            font-weight: bold;
            margin-bottom: 5px;
        }}
        .link-description {{
            font-size: 0.9em;
            opacity: 0.8;
        }}
        .metadata-section {{
            margin: 40px 0;
            padding: 20px;
            background: {{{{#f8f9fa}}}};
            border-radius: 8px;
            border: 1px solid {{{{#dee2e6}}}};
        }}
        .metadata-section h2 {{
            color: {{{{#333}}}};
            margin-top: 0;
            margin-bottom: 15px;
        }}
        .metadata-info p {{
            margin: 10px 0;
        }}
        .metadata-description {{
            margin-top: 15px;
            font-style: italic;
            color: {{{{#666}}}};
        }}
        code {{
            background: {{{{#e9ecef}}}};
            padding: 2px 6px;
            border-radius: 3px;
            font-family: monospace;
            font-size: 0.9em;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>🦭 Walrus Sui Archival</h1>
        <div class="subtitle">Decentralized Checkpoint Archival System</div>

        <div class="walrus-container">
            <img src="/resources/walrus_image.png" alt="Walrus" class="walrus-image">
        </div>

        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-label">Total Blobs</div>
                <div class="stat-value">{}</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Total Checkpoints</div>
                <div class="stat-value">{}</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Checkpoint Range</div>
                <div class="stat-value">{} - {}</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Total Size</div>
                <div class="stat-value">{:.2} GB</div>
            </div>
        </div>

        {}

        <div class="links-section">
            <div class="links-title">Quick Links</div>

            <a href="/v1/blobs" class="link-card">
                <div class="link-title">📦 View All Blobs</div>
                <div class="link-description">Browse all archived checkpoint blobs and their metadata</div>
            </a>

            <a href="/v1/checkpoint" class="link-card">
                <div class="link-title">🔍 Query Checkpoint</div>
                <div class="link-description">Look up a specific checkpoint by number</div>
            </a>
        </div>
    </div>
</body>
</html>
"#,
        blob_count,
        total_checkpoints,
        earliest_checkpoint,
        latest_checkpoint,
        size_gb,
        metadata_section
    )
}
