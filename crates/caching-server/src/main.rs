// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::net::SocketAddr;

use caching_server::Config;
use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "caching-server")]
#[command(about = "Caching server for walrus-sui-archival API")]
struct Args {
    /// Backend environment: mainnet, testnet, localnet, or custom URL.
    #[arg(long, default_value = "mainnet")]
    backend: String,

    /// Address to bind the server to.
    #[arg(long, default_value = "0.0.0.0:9185")]
    bind_address: String,

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

    // Create config.
    let config = Config::new(
        &args.backend,
        bind_address,
        args.cache_freshness_secs,
        args.cache_refresh_interval_secs,
        args.database_url,
    );

    tracing::info!(
        "starting caching server with backend: {}, cache freshness: {}s",
        config.backend_url,
        config.cache_freshness_secs
    );

    // Start server.
    caching_server::start_server(config).await?;

    Ok(())
}
