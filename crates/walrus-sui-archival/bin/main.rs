use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;
use walrus_sui_archival::{archival::run_sui_archival, config::Config, inspect_blob::inspect_blob};

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
        config: String,
    },
    /// Inspect the database.
    DbInspection,
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
        Commands::DbInspection => {
            tracing::info!("starting database inspection...");
            println!("db inspection command not yet implemented");
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
    }

    Ok(())
}
