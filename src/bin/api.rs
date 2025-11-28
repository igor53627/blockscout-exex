//! Standalone API server with Unix socket support for SSR

use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use eyre::Result;

use blockscout_exex::api::{create_router, ApiState};
use blockscout_exex::index_db::IndexDb;

#[derive(Parser)]
#[command(name = "blockscout-api")]
#[command(about = "Blockscout-compatible API server")]
struct Args {
    /// Index database path
    #[arg(long, default_value = "./blockscout-index")]
    index_path: PathBuf,

    /// API server port (ignored if --socket is set)
    #[arg(long, default_value = "3000")]
    port: u16,

    /// API server host (ignored if --socket is set)
    #[arg(long, default_value = "0.0.0.0")]
    host: String,

    /// Unix socket path (for SSR, faster than TCP)
    #[arg(long)]
    socket: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    tracing::info!("Opening index database at {:?}", args.index_path);
    let index_db = IndexDb::open(&args.index_path)?;

    let last_block = index_db.last_indexed_block()?;
    tracing::info!("Last indexed block: {:?}", last_block);

    let state = Arc::new(ApiState { index_db });
    let router = create_router(state);

    if let Some(socket_path) = args.socket {
        // Remove existing socket file
        let _ = std::fs::remove_file(&socket_path);

        tracing::info!("Starting API server on unix://{}", socket_path.display());

        let listener = tokio::net::UnixListener::bind(&socket_path)?;

        // Accept connections
        loop {
            let (stream, _) = listener.accept().await?;
            let router = router.clone();

            tokio::spawn(async move {
                let io = hyper_util::rt::TokioIo::new(stream);
                let service = hyper::service::service_fn(move |req| {
                    let router = router.clone();
                    async move {
                        use tower::ServiceExt;
                        router.oneshot(req).await
                    }
                });

                if let Err(e) = hyper::server::conn::http1::Builder::new()
                    .serve_connection(io, service)
                    .await
                {
                    tracing::error!("Error serving connection: {}", e);
                }
            });
        }
    } else {
        let addr = format!("{}:{}", args.host, args.port);
        tracing::info!("Starting API server on {}", addr);

        let listener = tokio::net::TcpListener::bind(&addr).await?;
        axum::serve(listener, router).await?;
    }

    Ok(())
}
