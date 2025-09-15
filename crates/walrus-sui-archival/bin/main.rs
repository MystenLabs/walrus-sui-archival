use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;
use walrus_sui_archival::{
    archival::run_sui_archival,
    burn_blobs::burn_all_blobs,
    config::Config,
    inspect_blob::inspect_blob,
    inspect_db::{InspectDbCommand, execute_inspect_db},
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
    /// Inspect a blob file.
    InspectBlob {
        /// Path to the blob file.
        #[arg(short, long)]
        path: PathBuf,
        /// Optional index to inspect a specific entry.
        #[arg(short, long)]
        index: Option<usize>,
        /// Optional offset to read from (used when index is not specified).
        #[arg(short, long)]
        offset: Option<u64>,
        /// Optional length to read (used when index is not specified).
        #[arg(short = 'l', long)]
        length: Option<u64>,
    },
    /// [DEV TOOL] Burn all blobs owned by the wallet (hidden from help).
    #[command(hide = true)]
    BurnAllBlobs {
        /// Path to client configuration file.
        #[arg(short, long, default_value = "config/client_config.yaml")]
        config: PathBuf,
    },
}

fn main() -> Result<()> {
    let args = Args::parse();

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&args.log_level)),
        )
        .init();

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
            index,
            offset,
            length,
        } => {
            tracing::info!("inspecting blob file: {}", path.display());
            inspect_blob(path, index, offset, length)?;
        }
        Commands::BurnAllBlobs { config } => {
            tracing::warn!("starting burn all blobs command (dev tool)...");
            // Create tokio runtime for async operation.
            let runtime = tokio::runtime::Runtime::new()?;
            runtime.block_on(burn_all_blobs(config))?;
        }
    }

    Ok(())
}
