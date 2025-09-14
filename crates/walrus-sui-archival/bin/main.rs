use anyhow::Result;
use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;
use walrus_sui_archival::archival::run_sui_archival;
use walrus_sui_archival::config::Config;

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
    }

    Ok(())
}
