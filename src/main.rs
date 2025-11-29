//! Blockscout ExEx - Reth Execution Extension for Blockscout indexing
//!
//! This crate provides a sidecar index database for Blockscout-compatible queries.
//! Uses FoundationDB for distributed, scalable indexed data storage.
//!
//! # Usage
//!
//! 1. Run backfill to index historical data
//! 2. Run API server to serve queries
//! 3. (Optional) Run as ExEx for live updates (requires reth feature)

use std::sync::Arc;

use clap::{Parser, Subcommand};
use eyre::Result;

mod api;
mod fdb_index;
#[cfg(feature = "reth")]
mod reth_reader;
mod transform;

#[derive(Parser)]
#[command(name = "blockscout-exex")]
#[command(about = "Blockscout-compatible indexer backed by FoundationDB")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the API server
    Api {
        /// FoundationDB cluster file path (uses default if not specified)
        #[arg(long)]
        cluster_file: Option<String>,

        /// Reth database path (enables direct DB reads)
        #[arg(long)]
        reth_db: Option<String>,

        /// Reth static files path (required if reth_db is set)
        #[arg(long)]
        reth_static_files: Option<String>,

        /// API server port
        #[arg(long, default_value = "3000")]
        port: u16,
    },

    /// Show index database stats
    Stats {
        /// FoundationDB cluster file path (uses default if not specified)
        #[arg(long)]
        cluster_file: Option<String>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    // Initialize FDB network - must be done once before any FDB operations
    let _network = unsafe { fdb_index::init_fdb_network() };

    let cli = Cli::parse();

    match cli.command {
        Commands::Api {
            cluster_file,
            #[cfg(feature = "reth")]
            reth_db,
            #[cfg(feature = "reth")]
            reth_static_files,
            #[cfg(not(feature = "reth"))]
            reth_db: _,
            #[cfg(not(feature = "reth"))]
            reth_static_files: _,
            port,
        } => {
            let index = match cluster_file {
                Some(path) => {
                    tracing::info!("Using cluster file: {}", path);
                    Arc::new(fdb_index::FdbIndex::open(&path)?)
                }
                None => {
                    tracing::info!("Using default cluster file");
                    Arc::new(fdb_index::FdbIndex::open_default()?)
                }
            };

            #[cfg(feature = "reth")]
            let reth = if let (Some(db_path), Some(static_path)) = (reth_db, reth_static_files) {
                tracing::info!("Opening reth DB at {} with static files at {}", db_path, static_path);
                Some(reth_reader::RethReader::open(&db_path, &static_path)?)
            } else {
                None
            };

            let state = Arc::new(api::ApiState {
                index,
                #[cfg(feature = "reth")]
                reth,
            });
            let router = api::create_router(state);

            let addr = format!("0.0.0.0:{}", port);
            tracing::info!("Starting API server on {}", addr);

            let listener = tokio::net::TcpListener::bind(&addr).await?;
            axum::serve(listener, router).await?;
        }

        Commands::Stats { cluster_file } => {
            let index = match cluster_file {
                Some(path) => fdb_index::FdbIndex::open(&path)?,
                None => fdb_index::FdbIndex::open_default()?,
            };
            let last_block = index.last_indexed_block().await?;
            println!("Last indexed block: {:?}", last_block);
        }
    }

    Ok(())
}
