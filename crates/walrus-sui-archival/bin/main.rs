// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};
use walrus_sui_archival::{
    archival::run_sui_archival,
    burn_blobs::burn_all_blobs,
    config::Config,
    inspect_blob::inspect_blob,
    inspect_db::{InspectDbCommand, execute_inspect_db},
    list_blobs::list_owned_blobs,
};

#[derive(Parser, Debug)]
#[command(name = "walrus-sui-archival")]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value = "info")]
    log_level: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Run the main archival process.
    Run {
        /// Path to configuration file.
        #[arg(short, long, default_value = "config/testnet_config.yaml")]
        config: PathBuf,
    },
    /// Inspect the database.
    InspectDb {
        /// Path to the database.
        #[arg(short, long, default_value = "archival_db")]
        db_path: PathBuf,
        /// Subcommand for database inspection.
        #[command(subcommand)]
        command: InspectDbCommand,
    },
    /// Inspect a blob file or fetch from Walrus.
    InspectBlob {
        /// Path to the blob file (mutually exclusive with blob-id).
        #[arg(short, long, conflicts_with = "blob_id")]
        path: Option<PathBuf>,
        /// Blob ID to fetch from Walrus (mutually exclusive with path).
        #[arg(short, long, conflicts_with = "path")]
        blob_id: Option<String>,
        /// Path to client configuration file (required when using blob-id).
        #[arg(short, long, default_value = "config/client_config.yaml")]
        client_config: Option<PathBuf>,
        /// Optional index to inspect a specific entry.
        #[arg(short, long)]
        index: Option<usize>,
        /// Optional offset to read from (used when index is not specified).
        #[arg(short, long)]
        offset: Option<u64>,
        /// Optional length to read (used when index is not specified).
        #[arg(short = 'l', long)]
        length: Option<u64>,
        /// Print full deserialized CheckpointData (verbose output).
        #[arg(short = 'f', long)]
        full: bool,
    },
    /// List all blobs owned by the wallet.
    ListOwnedBlobs {
        /// Path to client configuration file.
        #[arg(short, long, default_value = "config/client_config.yaml")]
        client_config: PathBuf,
    },
    /// [DEV TOOL] Burn all blobs owned by the wallet (hidden from help).
    #[command(hide = true)]
    BurnAllBlobs {
        /// Path to client configuration file.
        #[arg(short, long, default_value = "config/client_config.yaml")]
        client_config: PathBuf,
    },
}

fn main() -> Result<()> {
    let args = Args::parse();

    match args.command {
        Commands::Run { config } => {
            tracing::info!("starting walrus-sui-archival run command...");
            let config = Config::from_file(&config)?;
            run_sui_archival(config)?;
        }
        Commands::InspectDb { db_path, command } => {
            tracing::info!("starting database inspection...");
            execute_inspect_db(db_path, command)?;
        }
        Commands::InspectBlob {
            path,
            blob_id,
            client_config,
            index,
            offset,
            length,
            full,
        } => {
            match (&path, &blob_id) {
                (Some(p), None) => tracing::info!("inspecting blob file: {}", p.display()),
                (None, Some(id)) => tracing::info!("inspecting blob from walrus: {}", id),
                _ => {}
            }
            // Create tokio runtime for async operation.
            let runtime = tokio::runtime::Runtime::new()?;
            runtime.block_on(inspect_blob(
                path,
                blob_id,
                client_config,
                index,
                offset,
                length,
                full,
            ))?;
        }
        Commands::ListOwnedBlobs { client_config } => {
            tracing::info!("starting list owned blobs command...");
            // Create tokio runtime for async operation.
            let runtime = tokio::runtime::Runtime::new()?;
            runtime.block_on(list_owned_blobs(client_config))?;
        }
        Commands::BurnAllBlobs { client_config } => {
            tracing::warn!("starting burn all blobs command (dev tool)...");
            // Create tokio runtime for async operation.
            let runtime = tokio::runtime::Runtime::new()?;
            runtime.block_on(burn_all_blobs(client_config))?;
        }
    }

    Ok(())
}
