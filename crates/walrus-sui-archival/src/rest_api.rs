// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{net::SocketAddr, sync::Arc};

use anyhow::Result;
use axum::{
    Router,
    extract::{Query, State},
    http::StatusCode,
    response::{Html, Json},
    routing::get,
};
use serde::{Deserialize, Serialize};
use sui_storage::blob::Blob;
use sui_types::{
    full_checkpoint_content::CheckpointData,
    messages_checkpoint::CheckpointSequenceNumber,
};
use walrus_core::{BlobId, encoding::Primary};
use walrus_sdk::{ObjectID, SuiReadClient, client::WalrusNodeClient};

use crate::archival_state::ArchivalState;

/// REST API server for the archival system.
pub struct RestApiServer {
    address: SocketAddr,
    archival_state: Arc<ArchivalState>,
    walrus_client: Arc<WalrusNodeClient<SuiReadClient>>,
}

impl RestApiServer {
    /// Create a new REST API server.
    pub fn new(
        address: SocketAddr,
        archival_state: Arc<ArchivalState>,
        walrus_client: Arc<WalrusNodeClient<SuiReadClient>>,
    ) -> Self {
        Self {
            address,
            archival_state,
            walrus_client,
        }
    }

    /// Start the REST API server.
    pub async fn start(self) -> Result<()> {
        let app_state = AppState {
            archival_state: self.archival_state,
            walrus_client: self.walrus_client,
        };

        let app = Router::new()
            .route("/v1/blobs", get(list_all_blobs))
            .route("/v1/health", get(health_check))
            .route("/v1/checkpoint", get(get_checkpoint))
            .with_state(app_state);

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
    html.push_str("</style>\n");
    html.push_str("</head>\n");
    html.push_str("<body>\n");
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
        html.push_str("<th>Entries</th>\n");
        html.push_str("<th>Size (bytes)</th>\n");
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
            html.push_str(&format!("<td>{}</td>\n", entries_count));
            html.push_str(&format!("<td>{}</td>\n", blob_size));
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
            "<p>Total data size: {} bytes ({:.2} GB)</p>\n",
            total_size,
            total_size as f64 / (1024.0 * 1024.0 * 1024.0)
        ));
        html.push_str("</div>\n");
    }

    html.push_str("</body>\n");
    html.push_str("</html>\n");

    Ok(Html(html))
}

/// Handler for health check endpoint.
async fn health_check() -> StatusCode {
    StatusCode::OK
}

/// Query parameters for checkpoint endpoint.
#[derive(Deserialize)]
struct CheckpointQuery {
    checkpoint: u64,
    #[serde(default)]
    content: bool,
}

/// Response for checkpoint endpoint without content.
#[derive(Serialize)]
struct CheckpointLocationResponse {
    blob_id: String,
    object_id: String,
    index: usize,
    offset: u64,
    length: u64,
}

/// Response for checkpoint endpoint with content.
#[derive(Serialize)]
struct CheckpointContentResponse {
    blob_id: String,
    object_id: String,
    index: usize,
    offset: u64,
    length: u64,
    checkpoint_data: serde_json::Value,
}

/// Handler for getting checkpoint information.
async fn get_checkpoint(
    State(app_state): State<AppState>,
    Query(params): Query<CheckpointQuery>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let checkpoint_num = CheckpointSequenceNumber::from(params.checkpoint);

    // Get checkpoint blob info from database.
    let blob_info = match app_state
        .archival_state
        .get_checkpoint_blob_info(checkpoint_num)
    {
        Ok(info) => info,
        Err(e) => {
            // Check if it's a not found error.
            if e.to_string()
                .contains("no blob found containing checkpoint")
            {
                tracing::debug!("checkpoint {} not found", checkpoint_num);
                return Err(StatusCode::NOT_FOUND);
            } else {
                tracing::error!("failed to get checkpoint blob info: {}", e);
                return Err(StatusCode::INTERNAL_SERVER_ERROR);
            }
        }
    };

    // Find the index entry for this checkpoint.
    let entry = blob_info
        .index_entries
        .iter()
        .enumerate()
        .find(|(_, e)| e.checkpoint_number == checkpoint_num)
        .ok_or_else(|| {
            tracing::error!("checkpoint {} not found in blob index", checkpoint_num);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let (index, entry) = entry;
    let blob_id = String::from_utf8_lossy(&blob_info.blob_id).to_string();
    let object_id = ObjectID::from_bytes(&blob_info.object_id)
        .map(|id| id.to_string())
        .unwrap_or_else(|_| ObjectID::ZERO.to_string());

    if !params.content {
        // Return just the location info.
        let response = CheckpointLocationResponse {
            blob_id,
            object_id,
            index,
            offset: entry.offset,
            length: entry.length,
        };
        return Ok(Json(serde_json::to_value(response).unwrap()));
    }

    // Fetch the checkpoint data from Walrus.
    tracing::info!(
        "fetching checkpoint {} from walrus blob {}",
        checkpoint_num,
        blob_id
    );

    // Parse the blob ID.
    let blob_id_parsed: BlobId = blob_id.parse().map_err(|e| {
        tracing::error!("failed to parse blob id: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    // Download the specific range from the blob.
    let blob_data = app_state
        .walrus_client
        .read_blob::<Primary>(&blob_id_parsed)
        .await
        .map_err(|e| {
            tracing::error!("failed to retrieve blob from walrus: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    // Extract the checkpoint data from the blob.
    let start = entry.offset as usize;
    let end = (entry.offset + entry.length) as usize;
    let checkpoint_bytes = blob_data.get(start..end).ok_or_else(|| {
        tracing::error!("invalid offset/length for checkpoint data");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    // Deserialize the checkpoint using Blob::from_bytes.
    let checkpoint = Blob::from_bytes::<CheckpointData>(checkpoint_bytes).map_err(|e| {
        tracing::error!("failed to deserialize checkpoint: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    // Convert to JSON.
    let checkpoint_json = serde_json::to_value(&checkpoint).map_err(|e| {
        tracing::error!("failed to convert checkpoint to json: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let response = CheckpointContentResponse {
        blob_id,
        object_id,
        index,
        offset: entry.offset,
        length: entry.length,
        checkpoint_data: checkpoint_json,
    };

    Ok(Json(serde_json::to_value(response).unwrap()))
}
