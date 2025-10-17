// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};
use walrus_sui_archival::{
    archival::run_sui_archival,
    burn_blobs::burn_all_blobs,
    clear_metadata_blob_id::clear_metadata_blob_id,
    config::Config,
    delete_all_shared_archival_blobs::delete_all_shared_archival_blobs,
    dump_metadata_blob::dump_metadata_blob,
    get_metadata_blob_id::get_metadata_blob_id,
    inspect_blob::inspect_blob,
    inspect_db::{InspectDbCommand, execute_inspect_db},
    list_blobs::list_owned_blobs,
};

// Define version constants.
walrus_sui_archival::bin_version!();

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
        #[arg(short, long, default_value = "config/testnet_local_config.yaml")]
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
        #[arg(short, long, default_value = "config/local_client_config.yaml")]
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
        /// Context to use for the client config.
        #[arg(short, long, default_value = "testnet")]
        context: Option<String>,
        /// Print full deserialized CheckpointData (verbose output).
        #[arg(short = 'f', long)]
        full: bool,
    },
    /// List all blobs owned by the wallet.
    ListOwnedBlobs {
        /// Path to client configuration file.
        #[arg(short, long, default_value = "config/local_client_config.yaml")]
        client_config: PathBuf,
        /// Context to use for the client config.
        #[arg(short, long, default_value = "testnet")]
        context: String,
    },
    /// [DEV TOOL] Burn all blobs owned by the wallet (hidden from help).
    #[command(hide = true)]
    BurnAllBlobs {
        /// Path to client configuration file.
        #[arg(short, long, default_value = "config/local_client_config.yaml")]
        client_config: PathBuf,
        /// Context to use for the client config.
        #[arg(short, long, default_value = "testnet")]
        context: String,
    },
    /// Get the blob ID from the metadata pointer object.
    GetMetadataBlobId {
        /// Path to configuration file.
        #[arg(short, long, default_value = "config/testnet_local_config.yaml")]
        config: PathBuf,
    },
    /// Clear the blob ID from the metadata pointer object.
    ClearMetadataBlobId {
        /// Path to configuration file.
        #[arg(short, long, default_value = "config/testnet_local_config.yaml")]
        config: PathBuf,
    },
    /// Dump the content of the metadata blob.
    DumpMetadataBlob {
        /// Path to configuration file.
        #[arg(short, long, default_value = "config/testnet_local_config.yaml")]
        config: PathBuf,
    },
    /// Delete all shared archival blobs from the database.
    DeleteAllSharedArchivalBlobs {
        /// Path to configuration file.
        #[arg(short, long, default_value = "config/testnet_local_config.yaml")]
        config: PathBuf,
    },
}

fn main() -> Result<()> {
    let args = Args::parse();

    // Initialize telemetry for all commands to enable logging.
    // If we need span latency, we need to add the prom_registry to the telemetry config.
    let (_telemetry_guards, _tracing_handle) = telemetry_subscribers::TelemetryConfig::new()
        .with_env()
        .with_json()
        .init();

    match args.command {
        Commands::Run { config } => {
            tracing::info!("starting walrus-sui-archival run command...");
            let config = Config::from_file(&config)?;
            run_sui_archival(config, VERSION)?;
        }
        Commands::InspectDb { db_path, command } => {
            tracing::info!("starting database inspection...");
            let runtime = tokio::runtime::Runtime::new()?;
            runtime.block_on(execute_inspect_db(db_path, command))?;
        }
        Commands::InspectBlob {
            path,
            blob_id,
            client_config,
            index,
            offset,
            length,
            context,
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
                context.as_deref(),
                full,
            ))?;
        }
        Commands::ListOwnedBlobs {
            client_config,
            context,
        } => {
            tracing::info!("starting list owned blobs command...");
            // Create tokio runtime for async operation.
            let runtime = tokio::runtime::Runtime::new()?;
            runtime.block_on(list_owned_blobs(client_config, &context))?;
        }
        Commands::BurnAllBlobs {
            client_config,
            context,
        } => {
            tracing::warn!("starting burn all blobs command (dev tool)...");
            // Create tokio runtime for async operation.
            let runtime = tokio::runtime::Runtime::new()?;
            runtime.block_on(burn_all_blobs(client_config, &context))?;
        }
        Commands::GetMetadataBlobId { config } => {
            tracing::info!("reading metadata blob id from on-chain pointer...");
            // Create tokio runtime for async operation.
            let runtime = tokio::runtime::Runtime::new()?;
            runtime.block_on(get_metadata_blob_id(config))?;
        }
        Commands::ClearMetadataBlobId { config } => {
            tracing::info!("clearing metadata blob id from on-chain pointer...");
            // Create tokio runtime for async operation.
            let runtime = tokio::runtime::Runtime::new()?;
            runtime.block_on(clear_metadata_blob_id(config))?;
        }
        Commands::DumpMetadataBlob { config } => {
            tracing::info!("dumping metadata blob content...");
            // Create tokio runtime for async operation.
            let runtime = tokio::runtime::Runtime::new()?;
            runtime.block_on(dump_metadata_blob(config))?;
        }
        Commands::DeleteAllSharedArchivalBlobs { config } => {
            tracing::info!("deleting all shared archival blobs...");
            // Create tokio runtime for async operation.
            let runtime = tokio::runtime::Runtime::new()?;
            runtime.block_on(delete_all_shared_archival_blobs(config))?;
        }
    }

    Ok(())
}
