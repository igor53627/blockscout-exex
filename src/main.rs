//! Blockscout ExEx - Reth Execution Extension for Blockscout indexing
//!
//! This crate provides a sidecar index database for Blockscout-compatible queries.
//!
//! # Usage
//!
//! 1. Run backfill to index historical data
//! 2. Run API server to serve queries
//! 3. (Optional) Run as ExEx for live updates (requires reth feature)

use clap::{Parser, Subcommand};
use eyre::Result;

mod api;
mod index_db;
mod transform;

#[derive(Parser)]
#[command(name = "blockscout-exex")]
#[command(about = "Blockscout-compatible indexer for Reth")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the API server
    Api {
        /// Index database path
        #[arg(long, default_value = "./blockscout-index")]
        index_path: String,

        /// API server port
        #[arg(long, default_value = "3000")]
        port: u16,
    },

    /// Show index database stats
    Stats {
        /// Index database path
        #[arg(long, default_value = "./blockscout-index")]
        index_path: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Api { index_path, port } => {
            let index_db = index_db::IndexDb::open(&index_path)?;
            let state = std::sync::Arc::new(api::ApiState { index_db });
            let router = api::create_router(state);

            let addr = format!("0.0.0.0:{}", port);
            tracing::info!("Starting API server on {}", addr);

            let listener = tokio::net::TcpListener::bind(&addr).await?;
            axum::serve(listener, router).await?;
        }

        Commands::Stats { index_path } => {
            let index_db = index_db::IndexDb::open(&index_path)?;
            let last_block = index_db.last_indexed_block()?;
            println!("Last indexed block: {:?}", last_block);
        }
    }

    Ok(())
}
