//! Live block subscription - keeps index updated with new blocks
//!
//! Subscribes to new blocks via WebSocket and indexes them in real-time.

use std::path::PathBuf;
use std::time::Duration;

use alloy_primitives::{Address, Log, TxHash};
use clap::Parser;
use eyre::Result;
use futures::StreamExt;
use serde::Deserialize;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};

use blockscout_exex::fdb_index::{FdbIndex, TokenTransfer};
use blockscout_exex::transform::decode_erc20_transfer;

#[derive(Parser)]
#[command(name = "blockscout-subscriber")]
#[command(about = "Subscribe to new blocks and index them")]
struct Args {
    /// WebSocket endpoint URL
    #[arg(long, default_value = "ws://localhost:8546")]
    ws_url: String,

    /// JSON-RPC endpoint URL (for fetching block details)
    #[arg(long, default_value = "http://localhost:8545")]
    rpc_url: String,

    /// FoundationDB cluster file path (uses default if not specified)
    #[arg(long)]
    cluster_file: Option<PathBuf>,

    /// Reconnect delay on disconnect (seconds)
    #[arg(long, default_value = "5")]
    reconnect_delay: u64,
}

#[derive(Debug, Deserialize)]
struct WsResponse {
    method: Option<String>,
    params: Option<WsParams>,
}

#[derive(Debug, Deserialize)]
struct WsParams {
    result: NewHeadResult,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct NewHeadResult {
    number: String,
    hash: String,
}

#[derive(Debug, Deserialize)]
struct RpcBlock {
    #[allow(dead_code)]
    number: String,
    timestamp: String,
    transactions: Vec<RpcTransaction>,
}

#[derive(Debug, Deserialize)]
struct RpcTransaction {
    hash: String,
    from: String,
    to: Option<String>,
    #[serde(rename = "transactionIndex")]
    transaction_index: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RpcReceipt {
    #[serde(rename = "transactionHash")]
    transaction_hash: String,
    logs: Vec<RpcLog>,
}

#[derive(Debug, Deserialize)]
struct RpcLog {
    address: String,
    topics: Vec<String>,
    data: String,
}

struct RpcClient {
    url: String,
    client: reqwest::Client,
}

impl RpcClient {
    fn new(url: &str) -> Self {
        Self {
            url: url.to_string(),
            client: reqwest::Client::new(),
        }
    }

    async fn call<T: for<'de> Deserialize<'de>>(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<T> {
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": 1
        });

        let resp: serde_json::Value = self
            .client
            .post(&self.url)
            .json(&body)
            .send()
            .await?
            .json()
            .await?;

        if let Some(error) = resp.get("error") {
            return Err(eyre::eyre!("RPC error: {}", error));
        }

        let result = resp
            .get("result")
            .ok_or_else(|| eyre::eyre!("No result in response"))?;

        Ok(serde_json::from_value(result.clone())?)
    }

    async fn get_block(&self, hash: &str) -> Result<Option<RpcBlock>> {
        self.call("eth_getBlockByHash", serde_json::json!([hash, true]))
            .await
    }

    async fn get_block_receipts(&self, number: u64) -> Result<Vec<RpcReceipt>> {
        let hex = format!("0x{:x}", number);
        self.call("eth_getBlockReceipts", serde_json::json!([hex]))
            .await
    }
}

fn parse_log(rpc_log: &RpcLog) -> Option<Log> {
    let address: Address = rpc_log.address.parse().ok()?;
    let topics: Vec<_> = rpc_log
        .topics
        .iter()
        .filter_map(|t| t.parse().ok())
        .collect();
    let data = hex::decode(rpc_log.data.trim_start_matches("0x")).ok()?;

    Some(Log::new(address, topics, data.into()).unwrap())
}

fn parse_tx_index(tx: &RpcTransaction) -> u32 {
    tx.transaction_index
        .as_ref()
        .and_then(|s| u32::from_str_radix(s.trim_start_matches("0x"), 16).ok())
        .unwrap_or(0)
}

async fn index_block(
    index: &FdbIndex,
    rpc: &RpcClient,
    block_hash: &str,
    block_number: u64,
) -> Result<()> {
    let Some(block) = rpc.get_block(block_hash).await? else {
        warn!("Block {} not found", block_hash);
        return Ok(());
    };

    let receipts = rpc.get_block_receipts(block_number).await?;
    let timestamp = u64::from_str_radix(block.timestamp.trim_start_matches("0x"), 16)?;

    let receipt_map: std::collections::HashMap<_, _> = receipts
        .into_iter()
        .map(|r| (r.transaction_hash.clone(), r))
        .collect();

    let mut batch = index.write_batch();

    for tx in block.transactions {
        let tx_hash: TxHash = tx.hash.parse()?;
        let from: Address = tx.from.parse()?;
        let tx_idx = parse_tx_index(&tx);

        batch.insert_address_tx(from, tx_hash, block_number, tx_idx);

        if let Some(to_str) = &tx.to {
            if let Ok(to) = to_str.parse::<Address>() {
                batch.insert_address_tx(to, tx_hash, block_number, tx_idx);
            }
        }

        batch.insert_tx_block(tx_hash, block_number);

        if let Some(receipt) = receipt_map.get(&tx.hash) {
            for (log_index, rpc_log) in receipt.logs.iter().enumerate() {
                if let Some(log) = parse_log(rpc_log) {
                    if let Some((token, from, to, value)) = decode_erc20_transfer(&log) {
                        let transfer = TokenTransfer::new(
                            tx_hash,
                            log_index as u64,
                            token,
                            from,
                            to,
                            value,
                            block_number,
                            timestamp,
                        );
                        batch.insert_transfer(transfer);
                    }
                }
            }
        }
    }

    batch.commit(block_number).await?;
    info!(block = block_number, "Indexed block");

    Ok(())
}

async fn subscribe_loop(args: &Args, index: &FdbIndex) -> Result<()> {
    let rpc = RpcClient::new(&args.rpc_url);

    info!("Connecting to {}", args.ws_url);

    let (ws_stream, _) = connect_async(&args.ws_url).await?;
    let (mut write, mut read) = ws_stream.split();

    // Subscribe to new heads
    let subscribe_msg = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_subscribe",
        "params": ["newHeads"]
    });

    use futures::SinkExt;
    write
        .send(Message::Text(subscribe_msg.to_string().into()))
        .await?;

    info!("Subscribed to newHeads");

    while let Some(msg) = read.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                debug!("Received: {}", text);

                let response: WsResponse = match serde_json::from_str(&text) {
                    Ok(r) => r,
                    Err(e) => {
                        debug!("Failed to parse message: {}", e);
                        continue;
                    }
                };

                if response.method.as_deref() == Some("eth_subscription") {
                    if let Some(params) = response.params {
                        let block_number = u64::from_str_radix(
                            params.result.number.trim_start_matches("0x"),
                            16,
                        )?;

                        if let Err(e) =
                            index_block(index, &rpc, &params.result.hash, block_number).await
                        {
                            error!("Failed to index block {}: {}", block_number, e);
                        }
                    }
                }
            }
            Ok(Message::Ping(data)) => {
                use futures::SinkExt;
                let _ = write.send(Message::Pong(data)).await;
            }
            Ok(Message::Close(_)) => {
                warn!("WebSocket closed");
                break;
            }
            Err(e) => {
                error!("WebSocket error: {}", e);
                break;
            }
            _ => {}
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    // Initialize FDB network
    let _network = unsafe { blockscout_exex::fdb_index::init_fdb_network() };

    info!("Connecting to FoundationDB...");
    let index = match &args.cluster_file {
        Some(path) => {
            info!("Using cluster file: {:?}", path);
            FdbIndex::open(path)?
        }
        None => {
            info!("Using default cluster file");
            FdbIndex::open_default()?
        }
    };

    loop {
        match subscribe_loop(&args, &index).await {
            Ok(_) => {
                info!("Subscription ended, reconnecting...");
            }
            Err(e) => {
                error!("Subscription error: {}, reconnecting...", e);
            }
        }

        tokio::time::sleep(Duration::from_secs(args.reconnect_delay)).await;
    }
}
