//! Standalone API server with Unix socket support for SSR

use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use eyre::Result;
use serde_json::json;

use blockscout_exex::api::{create_router, ApiState};
#[cfg(feature = "fdb")]
use blockscout_exex::fdb_index::FdbIndex;
use blockscout_exex::IndexDatabase;
#[cfg(feature = "mdbx")]
use blockscout_exex::mdbx_index::MdbxIndex;
use blockscout_exex::meili::SearchClient;
use blockscout_exex::rpc_executor::RpcExecutor;
use blockscout_exex::websocket::{create_broadcaster, BroadcastMessage, Broadcaster};

#[derive(Parser)]
#[command(name = "blockscout-api")]
#[command(about = "Blockscout-compatible API server backed by FoundationDB or MDBX")]
struct Args {
    /// FoundationDB cluster file path (uses default if not specified)
    #[arg(long)]
    cluster_file: Option<PathBuf>,

    /// MDBX database path (mutually exclusive with --cluster-file)
    #[cfg(feature = "mdbx")]
    #[arg(long, conflicts_with = "cluster_file")]
    mdbx_path: Option<PathBuf>,

    /// API server port (ignored if --socket is set)
    #[arg(long, default_value = "3000")]
    port: u16,

    /// API server host (ignored if --socket is set)
    #[arg(long, default_value = "0.0.0.0")]
    host: String,

    /// Unix socket path (for SSR, faster than TCP)
    #[arg(long)]
    socket: Option<PathBuf>,

    /// Reth RPC URL for fetching new blocks (e.g., http://127.0.0.1:8545)
    #[arg(long)]
    reth_rpc: Option<String>,

    /// Meilisearch URL (optional, enables full-text search)
    #[arg(long)]
    meili_url: Option<String>,

    /// Meilisearch API key (optional)
    #[arg(long)]
    meili_key: Option<String>,

    /// Chain name for search filtering
    #[arg(long, default_value = "sepolia")]
    chain: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    // Backend selection: MDBX or FDB
    let index: Arc<dyn IndexDatabase> = {
        #[cfg(feature = "mdbx")]
        if let Some(mdbx_path) = &args.mdbx_path {
            tracing::info!("Using MDBX backend (read-only) at: {:?}", mdbx_path);
            Arc::new(MdbxIndex::open_readonly(mdbx_path)?)
        } else {
            #[cfg(feature = "fdb")]
            {
                // Initialize FDB network - must be done once before any FDB operations
                // Safety: We ensure the network guard is held until program exit
                let _network = unsafe { blockscout_exex::fdb_index::init_fdb_network() };

                tracing::info!("Using FoundationDB backend");
                match &args.cluster_file {
                    Some(path) => {
                        tracing::info!("Using cluster file: {:?}", path);
                        Arc::new(FdbIndex::open(path)?)
                    }
                    None => {
                        tracing::info!("Using default cluster file");
                        Arc::new(FdbIndex::open_default()?)
                    }
                }
            }
            #[cfg(not(feature = "fdb"))]
            {
                eyre::bail!("No backend configured. Build with --features fdb or --features mdbx")
            }
        }

        #[cfg(not(feature = "mdbx"))]
        {
            #[cfg(feature = "fdb")]
            {
                // Initialize FDB network - must be done once before any FDB operations
                // Safety: We ensure the network guard is held until program exit
                let _network = unsafe { blockscout_exex::fdb_index::init_fdb_network() };

                tracing::info!("Using FoundationDB backend");
                match &args.cluster_file {
                    Some(path) => {
                        tracing::info!("Using cluster file: {:?}", path);
                        Arc::new(FdbIndex::open(path)?)
                    }
                    None => {
                        tracing::info!("Using default cluster file");
                        Arc::new(FdbIndex::open_default()?)
                    }
                }
            }
            #[cfg(not(feature = "fdb"))]
            {
                eyre::bail!("No backend configured. Build with --features fdb or --features mdbx")
            }
        }
    };

    let last_block = index.last_indexed_block().await?;
    tracing::info!("Last indexed block: {:?}", last_block);

    // Note: Address count refresh is backend-specific and handled internally by each backend

    let broadcaster = create_broadcaster();

    // Spawn block watcher if reth RPC is configured
    if let Some(reth_rpc) = args.reth_rpc.clone() {
        let tx = broadcaster.clone();
        tokio::spawn(async move {
            block_watcher(reth_rpc, tx).await;
        });
    }

    // Initialize Meilisearch if configured
    let search = if let Some(ref meili_url) = args.meili_url {
        tracing::info!("Connecting to Meilisearch at {}", meili_url);
        let client = SearchClient::new(meili_url, args.meili_key.as_deref(), &args.chain);
        if let Err(e) = client.ensure_indexes().await {
            tracing::warn!("Failed to configure Meilisearch indexes: {}", e);
        }
        Some(client)
    } else {
        None
    };

    // Initialize RPC executor with dedicated thread pools
    let rpc_executor = if args.reth_rpc.is_some() {
        tracing::info!("Initializing RPC executor with thread pools (light:4, heavy:8, trace:2)");
        match RpcExecutor::new(4, 8, 2) {
            Ok(executor) => Some(Arc::new(executor)),
            Err(e) => {
                tracing::warn!("Failed to create RPC executor: {}", e);
                None
            }
        }
    } else {
        None
    };

    let state = Arc::new(ApiState {
        index,
        #[cfg(feature = "reth")]
        reth: None,
        broadcaster,
        rpc_url: args.reth_rpc.clone(),
        search,
        rpc_executor,
        chain: args.chain.clone(),
        gas_price_cache: Arc::new(parking_lot::RwLock::new(None)),
        coin_price_cache: Arc::new(parking_lot::RwLock::new(None)),
    });
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

async fn block_watcher(reth_rpc: String, tx: Broadcaster) {
    let client = reqwest::Client::new();
    let mut last_block: Option<u64> = None;

    tracing::info!("Starting block watcher with RPC: {}", reth_rpc);

    loop {
        match fetch_latest_block(&client, &reth_rpc).await {
            Ok(block) => {
                let block_num = block["number"]
                    .as_str()
                    .and_then(|s| u64::from_str_radix(s.trim_start_matches("0x"), 16).ok());

                if let Some(num) = block_num {
                    if last_block != Some(num) {
                        let prev = last_block.unwrap_or(num.saturating_sub(1));
                        last_block = Some(num);

                        // Process new blocks
                        for block_height in (prev + 1)..=num {
                            if let Ok(blk) = fetch_block_by_number(&client, &reth_rpc, block_height).await {
                                let txs = blk["transactions"].as_array();
                                let tx_count = txs.map(|t| t.len()).unwrap_or(0);
                                
                                tracing::info!("New block: {} ({} txs)", block_height, tx_count);

                                // Broadcast block
                                let block_payload = format_block_for_ws(&blk);
                                let _ = tx.send(BroadcastMessage {
                                    topic: "blocks:new_block".to_string(),
                                    event: "new_block".to_string(),
                                    payload: block_payload,
                                });

                                // Broadcast transactions
                                if let Some(transactions) = txs {
                                    for (idx, tx_obj) in transactions.iter().enumerate() {
                                        let tx_payload = format_tx_for_ws(tx_obj, block_height, idx);
                                        let _ = tx.send(BroadcastMessage {
                                            topic: "transactions:new_transaction".to_string(),
                                            event: "new_transaction".to_string(),
                                            payload: tx_payload,
                                        });
                                    }
                                }
                            }
                        }

                        // Send indexing status
                        let _ = tx.send(BroadcastMessage {
                            topic: "blocks:indexing".to_string(),
                            event: "index_status".to_string(),
                            payload: json!({
                                "finished_indexing": true,
                                "indexed_blocks_ratio": "1.00",
                                "indexed_internal_transactions_ratio": "1.00"
                            }),
                        });
                    }
                }
            }
            Err(e) => {
                tracing::warn!("Failed to fetch block: {}", e);
            }
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    }
}

async fn fetch_latest_block(
    client: &reqwest::Client,
    reth_rpc: &str,
) -> Result<serde_json::Value, reqwest::Error> {
    let resp = client
        .post(reth_rpc)
        .json(&json!({
            "jsonrpc": "2.0",
            "method": "eth_getBlockByNumber",
            "params": ["latest", true],
            "id": 1
        }))
        .send()
        .await?
        .json::<serde_json::Value>()
        .await?;

    Ok(resp["result"].clone())
}

async fn fetch_block_by_number(
    client: &reqwest::Client,
    reth_rpc: &str,
    number: u64,
) -> Result<serde_json::Value, reqwest::Error> {
    let resp = client
        .post(reth_rpc)
        .json(&json!({
            "jsonrpc": "2.0",
            "method": "eth_getBlockByNumber",
            "params": [format!("0x{:x}", number), true],
            "id": 1
        }))
        .send()
        .await?
        .json::<serde_json::Value>()
        .await?;

    Ok(resp["result"].clone())
}

fn format_block_for_ws(block: &serde_json::Value) -> serde_json::Value {
    let hash = block["hash"].as_str().unwrap_or("0x0");
    let number = block["number"].as_str().unwrap_or("0x0");
    let number_dec = u64::from_str_radix(number.trim_start_matches("0x"), 16).unwrap_or(0);
    let timestamp = block["timestamp"].as_str().unwrap_or("0x0");
    let timestamp_dec = u64::from_str_radix(timestamp.trim_start_matches("0x"), 16).unwrap_or(0);
    let miner = block["miner"].as_str().unwrap_or("0x0000000000000000000000000000000000000000");
    let gas_used = block["gasUsed"].as_str().unwrap_or("0x0");
    let gas_limit = block["gasLimit"].as_str().unwrap_or("0x0");
    let tx_count = block["transactions"].as_array().map(|a| a.len()).unwrap_or(0);

    let ts = chrono::DateTime::from_timestamp(timestamp_dec as i64, 0)
        .map(|dt| dt.to_rfc3339())
        .unwrap_or_else(|| "1970-01-01T00:00:00Z".to_string());

    json!({
        "average_block_time": "12.0",
        "block": {
            "hash": hash,
            "height": number_dec,
            "timestamp": ts,
            "tx_count": tx_count,
            "miner": {
                "hash": miner,
                "implementations": null,
                "is_contract": false,
                "is_verified": false,
                "name": null,
                "ens_domain_name": null
            },
            "size": 0,
            "nonce": "0x0000000000000000",
            "rewards": [],
            "gas_limit": gas_limit,
            "gas_used": gas_used,
            "burnt_fees": null,
            "priority_fee": null,
            "base_fee_per_gas": block["baseFeePerGas"].as_str().unwrap_or("0"),
            "parent_hash": block["parentHash"].as_str().unwrap_or("0x0"),
            "type": "block"
        }
    })
}

fn format_tx_for_ws(tx: &serde_json::Value, block_num: u64, idx: usize) -> serde_json::Value {
    let hash = tx["hash"].as_str().unwrap_or("0x0");
    let from = tx["from"].as_str().unwrap_or("0x0");
    let to = tx["to"].as_str();

    json!({
        "transaction": {
            "hash": hash,
            "from": {
                "hash": from,
                "is_contract": false,
                "is_verified": false,
                "name": null,
                "implementations": null,
                "ens_domain_name": null
            },
            "to": to.map(|a| json!({
                "hash": a,
                "is_contract": false,
                "is_verified": false,
                "name": null,
                "implementations": null,
                "ens_domain_name": null
            })),
            "block_number": block_num,
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "status": "ok",
            "method": null,
            "transaction_types": [],
            "exchange_rate": null,
            "fee": {
                "type": "actual",
                "value": "0"
            },
            "tx_types": [],
            "priority": idx
        }
    })
}
