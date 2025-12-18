// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::net::SocketAddr;

use clap::Parser;
use website_front_end::Config;

#[derive(Parser, Debug)]
#[command(name = "website-front-end")]
#[command(about = "Website front-end caching proxy for walrus-sui-archival API")]
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
    );

    tracing::info!(
        "starting website front-end with backend: {}, cache freshness: {}s",
        config.backend_url,
        config.cache_freshness_secs
    );

    // Start server.
    website_front_end::start_server(config).await?;

    Ok(())
}
