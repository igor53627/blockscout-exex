//! Backfill tool - reads from JSON-RPC and indexes into FoundationDB
//!
//! This is a standalone tool that doesn't require linking against reth.
//! It uses JSON-RPC to fetch blocks and receipts.

use std::path::PathBuf;

use alloy_primitives::{Address, Log, TxHash};
use clap::Parser;
use eyre::Result;
use serde::Deserialize;
use tracing::{debug, info};

use blockscout_exex::fdb_index::{FdbIndex, TokenTransfer};
use blockscout_exex::transform::decode_erc20_transfer;

#[derive(Parser)]
#[command(name = "blockscout-backfill")]
#[command(about = "Backfill FoundationDB index from JSON-RPC")]
struct Args {
    /// JSON-RPC endpoint URL
    #[arg(long, default_value = "http://localhost:8545")]
    rpc_url: String,

    /// FoundationDB cluster file path (uses default if not specified)
    #[arg(long)]
    cluster_file: Option<PathBuf>,

    /// Starting block number (0 = from beginning)
    #[arg(long, default_value = "0")]
    from_block: u64,

    /// Ending block number (0 = latest)
    #[arg(long, default_value = "0")]
    to_block: u64,

    /// Batch size for commits
    #[arg(long, default_value = "1000")]
    batch_size: u64,
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

    async fn get_block(&self, number: u64) -> Result<Option<RpcBlock>> {
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

    let rpc = RpcClient::new(&args.rpc_url);

    // Determine block range
    let from_block = if args.from_block == 0 {
        index.last_indexed_block().await?.map(|b| b + 1).unwrap_or(0)
    } else {
        args.from_block
    };

    let to_block = if args.to_block == 0 {
        rpc.get_block_number().await?
    } else {
        args.to_block
    };

    info!(from = from_block, to = to_block, "Starting backfill");

    let total = to_block - from_block + 1;
    let mut processed = 0u64;

    for batch_start in (from_block..=to_block).step_by(args.batch_size as usize) {
        let batch_end = (batch_start + args.batch_size - 1).min(to_block);

        let mut batch = index.write_batch();

        for block_num in batch_start..=batch_end {
            let Some(block) = rpc.get_block(block_num).await? else {
                continue;
            };

            let receipts = rpc.get_block_receipts(block_num).await?;
            let timestamp = u64::from_str_radix(block.timestamp.trim_start_matches("0x"), 16)?;

            // Create receipt lookup by tx hash
            let receipt_map: std::collections::HashMap<_, _> = receipts
                .into_iter()
                .map(|r| (r.transaction_hash.clone(), r))
                .collect();

            for tx in block.transactions {
                let tx_hash: TxHash = tx.hash.parse()?;
                let from: Address = tx.from.parse()?;
                let tx_idx = parse_tx_index(&tx);

                // Index by sender
                batch.insert_address_tx(from, tx_hash, block_num, tx_idx);

                // Index by receiver
                if let Some(to_str) = &tx.to {
                    if let Ok(to) = to_str.parse::<Address>() {
                        batch.insert_address_tx(to, tx_hash, block_num, tx_idx);
                    }
                }

                // Index tx → block
                batch.insert_tx_block(tx_hash, block_num);

                // Process logs for ERC20 transfers
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
                                    block_num,
                                    timestamp,
                                );
                                batch.insert_transfer(transfer);
                            }
                        }
                    }
                }
            }

            processed += 1;
            if processed % 1000 == 0 {
                let percent = (processed as f64 / total as f64) * 100.0;
                info!(
                    block = block_num,
                    progress = format!("{:.2}%", percent),
                    "Progress"
                );
            }
        }

        batch.commit(batch_end).await?;
        debug!(from = batch_start, to = batch_end, "Committed batch");
    }

    info!("Backfill complete");
    Ok(())
}
