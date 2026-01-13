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
    extend_shared_blob::extend_shared_blob,
    get_metadata_blob_id::get_metadata_blob_id,
    inspect_blob::inspect_blob,
    inspect_db::{InspectDbCommand, execute_inspect_db},
    list_blobs::list_owned_blobs,
    parse_bcs_checkpoint::parse_bcs_checkpoint,
    postgres::{PostgresPool, run_backfill},
    remove_metadata_from_db::remove_metadata_from_db,
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
        #[arg(short, long, default_value = "config/local_testnet_client_config.yaml")]
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
        #[arg(short, long, default_value = "config/local_testnet_client_config.yaml")]
        client_config: PathBuf,
        /// Context to use for the client config.
        #[arg(short, long, default_value = "testnet")]
        context: String,
    },
    /// [DEV TOOL] Burn all blobs owned by the wallet (hidden from help).
    #[command(hide = true)]
    BurnAllBlobs {
        /// Path to client configuration file.
        #[arg(short, long, default_value = "config/local_testnet_client_config.yaml")]
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
        /// Optional file containing blob IDs to delete (one per line).
        /// If provided, blob IDs will be read from this file instead of the database.
        #[arg(short = 'f', long)]
        blob_list_file: Option<PathBuf>,
    },
    /// Extend a shared archival blob's storage period using the caller's own WAL tokens.
    ExtendSharedArchivalBlob {
        /// Path to configuration file.
        #[arg(short, long, default_value = "config/testnet_local_config.yaml")]
        config: PathBuf,
        /// Object ID of the shared archival blob to extend.
        #[arg(short, long)]
        shared_blob_id: String,
        /// Number of epochs to extend.
        #[arg(short, long)]
        epochs: u32,
    },
    /// Remove metadata entries from the database with checkpoint >= specified value.
    RemoveMetadataFromDb {
        /// Path to the database.
        #[arg(short, long, default_value = "archival_db")]
        db_path: PathBuf,
        /// Checkpoint number - all entries with start_checkpoint >= this value will be removed.
        #[arg(short, long)]
        from_checkpoint: u64,
    },
    /// Parse and print a BCS-encoded CheckpointData file.
    ParseBcsCheckpoint {
        /// Path to the BCS-encoded checkpoint file.
        #[arg(short, long)]
        file: PathBuf,
    },
    /// Backfill checkpoint metadata from RocksDB to PostgreSQL.
    /// Requires DATABASE_URL environment variable to be set.
    BackfillToPostgres {
        /// Path to the RocksDB database.
        #[arg(short, long, default_value = "archival_db")]
        db_path: PathBuf,
        /// Number of records to process in each batch.
        #[arg(short, long, default_value = "100")]
        batch_size: usize,
    },
    /// Backfill blob_size column in checkpoint_blob_info table.
    /// blob_size = sum of length_bytes from all checkpoint_index_entry records.
    /// Requires DATABASE_URL environment variable to be set.
    BackfillBlobSize {
        /// Database URL (can also be set via DATABASE_URL environment variable).
        #[arg(short, long, env = "DATABASE_URL")]
        database_url: String,
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
        Commands::DeleteAllSharedArchivalBlobs {
            config,
            blob_list_file,
        } => {
            if let Some(ref file) = blob_list_file {
                tracing::info!(
                    "deleting shared archival blobs from file: {}...",
                    file.display()
                );
            } else {
                tracing::info!("deleting all shared archival blobs from database...");
            }
            // Create tokio runtime for async operation.
            let runtime = tokio::runtime::Runtime::new()?;
            runtime.block_on(delete_all_shared_archival_blobs(config, blob_list_file))?;
        }
        Commands::ExtendSharedArchivalBlob {
            config,
            shared_blob_id,
            epochs,
        } => {
            tracing::info!(
                "extending shared blob {} by {} epochs...",
                shared_blob_id,
                epochs
            );
            // Parse the object ID.
            let shared_blob_id = shared_blob_id
                .parse()
                .map_err(|e| anyhow::anyhow!("failed to parse shared blob object ID: {}", e))?;
            // Create tokio runtime for async operation.
            let runtime = tokio::runtime::Runtime::new()?;
            runtime.block_on(extend_shared_blob(config, shared_blob_id, epochs))?;
        }
        Commands::RemoveMetadataFromDb {
            db_path,
            from_checkpoint,
        } => {
            tracing::info!(
                "removing metadata entries from database {} with checkpoint >= {}",
                db_path.display(),
                from_checkpoint
            );
            remove_metadata_from_db(db_path, from_checkpoint)?;
        }
        Commands::ParseBcsCheckpoint { file } => {
            tracing::info!("parsing BCS checkpoint file: {}", file.display());
            parse_bcs_checkpoint(file)?;
        }
        Commands::BackfillToPostgres {
            db_path,
            batch_size,
        } => {
            tracing::info!(
                "starting backfill from RocksDB ({}) to PostgreSQL with batch size {}",
                db_path.display(),
                batch_size
            );
            let runtime = tokio::runtime::Runtime::new()?;
            runtime.block_on(run_backfill(db_path, batch_size))?;
        }
        Commands::BackfillBlobSize { database_url } => {
            tracing::info!("starting blob_size backfill...");
            let runtime = tokio::runtime::Runtime::new()?;
            runtime.block_on(async {
                let pool = PostgresPool::new(&database_url)?;

                // Run migrations first to ensure the blob_size column exists
                pool.run_migrations().await?;

                // Get count of blobs to backfill
                let blobs_without_size = pool.get_blobs_without_size().await?;
                let total = blobs_without_size.len();

                if total == 0 {
                    tracing::info!(
                        "all blobs already have blob_size set, running full backfill to update..."
                    );
                }

                // Run the backfill
                let updated = pool.backfill_blob_sizes().await?;
                tracing::info!("blob_size backfill complete: {} records updated", updated);

                Ok::<_, anyhow::Error>(())
            })?;
        }
    }

    Ok(())
}
