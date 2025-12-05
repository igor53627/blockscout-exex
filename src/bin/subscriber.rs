//! Live block subscription - keeps index updated with new blocks
//!
//! Subscribes to new blocks via WebSocket and indexes them in real-time.
//! Also catches up on any missed blocks between last_indexed and chain head.

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
use blockscout_exex::transform::{decode_token_transfer, TokenType};

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

    /// Maximum blocks to catch up per batch
    #[arg(long, default_value = "100")]
    catchup_batch_size: u64,
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

    async fn get_block_number(&self) -> Result<u64> {
        let hex: String = self.call("eth_blockNumber", serde_json::json!([])).await?;
        Ok(u64::from_str_radix(hex.trim_start_matches("0x"), 16)?)
    }

    async fn get_block(&self, hash: &str) -> Result<Option<RpcBlock>> {
        self.call("eth_getBlockByHash", serde_json::json!([hash, true]))
            .await
    }

    async fn get_block_by_number(&self, number: u64) -> Result<Option<RpcBlock>> {
        let hex = format!("0x{:x}", number);
        self.call("eth_getBlockByNumber", serde_json::json!([hex, true]))
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
    block_number: u64,
    block_hash: Option<&str>,
) -> Result<()> {
    let block = if let Some(hash) = block_hash {
        rpc.get_block(hash).await?
    } else {
        rpc.get_block_by_number(block_number).await?
    };

    let Some(block) = block else {
        warn!("Block {} not found", block_number);
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
                    // Use unified decoder for ERC-20, ERC-721, ERC-1155
                    let decoded_transfers = decode_token_transfer(&log);
                    for decoded in decoded_transfers {
                        let transfer = match decoded.token_type {
                            TokenType::Erc721 => TokenTransfer::new_erc721(
                                tx_hash,
                                log_index as u64,
                                decoded.token_address,
                                decoded.from,
                                decoded.to,
                                decoded.token_id.unwrap(),
                                block_number,
                                timestamp,
                            ),
                            TokenType::Erc1155 => TokenTransfer::new_erc1155(
                                tx_hash,
                                log_index as u64,
                                decoded.token_address,
                                decoded.from,
                                decoded.to,
                                decoded.token_id.unwrap(),
                                decoded.value,
                                block_number,
                                timestamp,
                            ),
                            TokenType::Erc20 => TokenTransfer::new(
                                tx_hash,
                                log_index as u64,
                                decoded.token_address,
                                decoded.from,
                                decoded.to,
                                decoded.value,
                                block_number,
                                timestamp,
                            ),
                        };
                        batch.insert_transfer(transfer);
                    }
                }
            }
        }
    }

    batch.commit(block_number).await?;
    Ok(())
}

/// Catch up on any missed blocks between last_indexed and chain head
async fn catchup_missed_blocks(
    index: &FdbIndex,
    rpc: &RpcClient,
    batch_size: u64,
) -> Result<u64> {
    let last_indexed = index.last_indexed_block().await?.unwrap_or(0);
    let chain_head = rpc.get_block_number().await?;

    if last_indexed >= chain_head {
        info!(
            last_indexed = last_indexed,
            chain_head = chain_head,
            "Index is up to date"
        );
        return Ok(last_indexed);
    }

    let gap = chain_head - last_indexed;
    info!(
        last_indexed = last_indexed,
        chain_head = chain_head,
        gap = gap,
        "Catching up on missed blocks"
    );

    let mut current = last_indexed + 1;
    while current <= chain_head {
        let batch_end = (current + batch_size - 1).min(chain_head);

        for block_num in current..=batch_end {
            if let Err(e) = index_block(index, rpc, block_num, None).await {
                error!("Failed to index block {}: {}", block_num, e);
                // Continue with next block, don't fail the whole catchup
            }
        }

        info!(
            from = current,
            to = batch_end,
            remaining = chain_head - batch_end,
            "Caught up batch"
        );

        current = batch_end + 1;
    }

    info!(
        blocks_indexed = chain_head - last_indexed,
        "Catchup complete"
    );
    Ok(chain_head)
}

async fn subscribe_loop(args: &Args, index: &FdbIndex) -> Result<()> {
    let rpc = RpcClient::new(&args.rpc_url);

    // First, catch up on any missed blocks
    let last_block = catchup_missed_blocks(index, &rpc, args.catchup_batch_size).await?;
    info!(last_block = last_block, "Starting live subscription");

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

    let mut expected_block = last_block + 1;

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

                        // Check for gaps - if we missed blocks, catch up
                        if block_number > expected_block {
                            warn!(
                                expected = expected_block,
                                received = block_number,
                                gap = block_number - expected_block,
                                "Gap detected, catching up"
                            );
                            for missed in expected_block..block_number {
                                if let Err(e) = index_block(index, &rpc, missed, None).await {
                                    error!("Failed to index missed block {}: {}", missed, e);
                                } else {
                                    info!(block = missed, "Indexed missed block");
                                }
                            }
                        }

                        if let Err(e) =
                            index_block(index, &rpc, block_number, Some(&params.result.hash)).await
                        {
                            error!("Failed to index block {}: {}", block_number, e);
                        } else {
                            info!(block = block_number, "Indexed block");
                        }

                        expected_block = block_number + 1;
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
