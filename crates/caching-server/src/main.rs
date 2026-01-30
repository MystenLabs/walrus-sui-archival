// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::net::SocketAddr;

use caching_server::Config;
use clap::Parser;
use sui_sdk::SUI_MAINNET_URL;
use sui_types::base_types::ObjectID;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Parser, Debug)]
#[command(name = "caching-server")]
#[command(about = "Caching server for walrus-sui-archival API")]
#[command(version = VERSION)]
struct Args {
    /// Backend environment: mainnet, testnet, localnet, or custom URL.
    #[arg(long, default_value = "mainnet")]
    backend: String,

    /// Address to bind the server to.
    #[arg(long, default_value = "0.0.0.0:9185")]
    bind_address: String,

    /// Address to bind the Prometheus metrics server to.
    /// Metrics will be available at /metrics on this server.
    #[arg(long, default_value = "0.0.0.0:9184")]
    metrics_address: String,

    /// Cache freshness duration in seconds.
    #[arg(long, default_value = "300")]
    cache_freshness_secs: u64,

    /// Cache refresh interval in seconds.
    #[arg(long, default_value = "60")]
    cache_refresh_interval_secs: u64,

    /// PostgreSQL database URL for direct queries (optional).
    /// If not provided, will fall back to proxying requests to the backend.
    /// Can also be set via DATABASE_URL environment variable.
    #[arg(long, env = "DATABASE_URL")]
    database_url: Option<String>,

    /// Metadata pointer object ID (required).
    #[arg(long)]
    metadata_pointer_object_id: ObjectID,

    /// Sui RPC URL (required).
    #[arg(long, default_value = SUI_MAINNET_URL)]
    sui_rpc_url: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing.
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    // Parse bind address.
    let bind_address: SocketAddr = args
        .bind_address
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid bind address: {}", e))?;

    // Parse metrics address.
    let metrics_address: SocketAddr = args
        .metrics_address
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid metrics address: {}", e))?;

    // Create config.
    let config = Config::new(
        &args.backend,
        bind_address,
        metrics_address,
        args.cache_freshness_secs,
        args.cache_refresh_interval_secs,
        args.database_url,
        args.metadata_pointer_object_id,
        args.sui_rpc_url,
    );

    tracing::info!(
        "starting caching server with backend: {}, cache freshness: {}s",
        config.backend_url,
        config.cache_freshness_secs
    );

    tracing::info!(
        "Prometheus metrics server will be started on {}",
        config.metrics_address
    );

    // Start server.
    caching_server::start_server(config, VERSION).await?;

    Ok(())
}
