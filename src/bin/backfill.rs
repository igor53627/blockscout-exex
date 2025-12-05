//! Backfill tool - indexes blockchain data into FoundationDB or MDBX
//!
//! Supports multiple modes:
//! - HTTP JSON-RPC mode (default): fetches via HTTP
//! - IPC mode (--ipc-path): fetches via Unix socket, faster than HTTP
//! - Direct MDBX mode (--reth-db): reads reth database directly, fastest
//!
//! Target database:
//! - FoundationDB (default): uses --cluster-file
//! - MDBX index: uses --mdbx-path

use std::collections::HashSet;
use std::path::PathBuf;

use alloy_primitives::{Address, Log, TxHash};
use clap::Parser;
use eyre::Result;
use serde::Deserialize;
use tracing::{debug, info, warn};

#[cfg(feature = "fdb")]
use blockscout_exex::fdb_index::{AddressWrapper, FdbIndex, TokenTransfer};
use blockscout_exex::IndexDatabase;
#[cfg(feature = "mdbx")]
use blockscout_exex::mdbx_index::MdbxIndex;
use blockscout_exex::meili::{SearchClient, TokenDocument};
use blockscout_exex::transform::{decode_token_transfer, TokenType};
use hyperloglogplus::HyperLogLog;

#[derive(Parser)]
#[command(name = "blockscout-backfill")]
#[command(about = "Backfill FoundationDB or MDBX index from reth")]
struct Args {
    /// JSON-RPC endpoint URL (used when --ipc-path is not set)
    #[arg(long, default_value = "http://localhost:8545")]
    rpc_url: String,

    /// Path to IPC socket (e.g., /mnt/sepolia/data/reth.ipc)
    #[arg(long)]
    ipc_path: Option<PathBuf>,

    /// Path to reth database directory (enables direct MDBX reading for source data)
    #[cfg(feature = "reth")]
    #[arg(long)]
    reth_db: Option<PathBuf>,

    /// Path to reth static files directory (required with --reth-db)
    #[cfg(feature = "reth")]
    #[arg(long)]
    reth_static_files: Option<PathBuf>,

    /// Chain name: mainnet or sepolia
    #[cfg(feature = "reth")]
    #[arg(long, default_value = "sepolia")]
    chain: String,

    /// FoundationDB cluster file path (uses default if not specified)
    #[arg(long)]
    cluster_file: Option<PathBuf>,

    /// MDBX index database path (mutually exclusive with --cluster-file)
    #[cfg(feature = "mdbx")]
    #[arg(long, conflicts_with = "cluster_file")]
    mdbx_path: Option<PathBuf>,

    /// Starting block number (0 = from last indexed + 1)
    #[arg(long, default_value = "0")]
    from_block: u64,

    /// Ending block number (0 = latest)
    #[arg(long, default_value = "0")]
    to_block: u64,

    /// Batch size for commits
    #[arg(long, default_value = "1000")]
    batch_size: u64,

    /// Meilisearch URL (optional, enables search indexing)
    #[arg(long)]
    meili_url: Option<String>,

    /// Meilisearch API key (optional)
    #[arg(long)]
    meili_key: Option<String>,
}

// ============ JSON-RPC Types and Client ============

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

enum RpcTransport {
    Http { url: String, client: reqwest::Client },
    Ipc { path: PathBuf },
}

struct RpcClient {
    transport: RpcTransport,
}

impl RpcClient {
    fn new_http(url: &str) -> Self {
        Self {
            transport: RpcTransport::Http {
                url: url.to_string(),
                client: reqwest::Client::new(),
            },
        }
    }

    fn new_ipc(path: PathBuf) -> Self {
        Self {
            transport: RpcTransport::Ipc { path },
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

        let resp: serde_json::Value = match &self.transport {
            RpcTransport::Http { url, client } => {
                client
                    .post(url)
                    .json(&body)
                    .send()
                    .await?
                    .json()
                    .await?
            }
            RpcTransport::Ipc { path } => {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                use tokio::net::UnixStream;

                let mut stream = UnixStream::connect(path).await?;
                let request = serde_json::to_string(&body)?;
                stream.write_all(request.as_bytes()).await?;
                stream.flush().await?;

                // Read response - IPC returns raw JSON without newlines
                let mut response = Vec::new();
                let mut buf = [0u8; 4096];
                loop {
                    let n = stream.read(&mut buf).await?;
                    if n == 0 {
                        break;
                    }
                    response.extend_from_slice(&buf[..n]);
                    // Try to parse - if successful, we have the complete response
                    if serde_json::from_slice::<serde_json::Value>(&response).is_ok() {
                        break;
                    }
                }
                serde_json::from_slice(&response)?
            }
        };

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

    async fn call_contract(&self, to: &str, data: &str) -> Result<String> {
        self.call(
            "eth_call",
            serde_json::json!([{"to": to, "data": data}, "latest"]),
        )
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

// ============ Direct MDBX Mode ============

#[cfg(feature = "reth")]
mod direct {
    use super::*;
    use alloy_primitives::Log;
    use blockscout_exex::meili::{SearchClient, TokenDocument};
    use blockscout_exex::reth_reader::RethReader;

    pub struct DirectBackfillConfig<'a> {
        pub reader: &'a RethReader,
        pub index: &'a FdbIndex,
        pub search: Option<&'a SearchClient>,
        pub chain: &'a str,
        pub from_block: u64,
        pub to_block: u64,
        pub batch_size: u64,
    }

    pub async fn run_direct_backfill(config: DirectBackfillConfig<'_>) -> Result<()> {
        let DirectBackfillConfig {
            reader,
            index,
            search,
            chain,
            from_block,
            to_block,
            batch_size,
        } = config;

        let total = to_block - from_block + 1;
        let mut processed = 0u64;
        let mut seen_tokens: HashSet<Address> = HashSet::new();

        // Create in-memory HLL for address counting (we just save the count periodically)
        let mut address_hll = FdbIndex::new_address_hll();
        let prev_count = index.load_address_hll_count().await.unwrap_or(0);
        info!("Starting address HLL (previous count: ~{})", prev_count);
        let mut hll_save_counter = 0u64;
        const HLL_SAVE_INTERVAL: u64 = 10000; // Save HLL count every 10k blocks

        for batch_start in (from_block..=to_block).step_by(batch_size as usize) {
            let batch_end = (batch_start + batch_size - 1).min(to_block);

            let mut batch = index.write_batch();
            let mut new_tokens: Vec<(Address, TokenType)> = Vec::new();

            for block_num in batch_start..=batch_end {
                let Some(block) = reader.block_by_number(block_num)? else {
                    continue;
                };

                let receipts = reader.receipts_by_block(block_num)?.unwrap_or_default();
                let timestamp = block.header().timestamp;

                let mut block_tx_count = 0i64;
                let mut block_transfer_count = 0i64;

                for (tx_idx, tx) in block.transactions_with_sender().enumerate() {
                    let (from, tx) = tx;
                    let tx_hash = *tx.tx_hash();

                    // Index by sender
                    batch.insert_address_tx(*from, tx_hash, block_num, tx_idx as u32);

                    // Index by receiver - use alloy_consensus Transaction trait
                    use alloy_consensus::transaction::Transaction as TxTrait;
                    if let Some(to) = TxTrait::to(tx) {
                        batch.insert_address_tx(to, tx_hash, block_num, tx_idx as u32);
                    }

                    // Index tx → block
                    batch.insert_tx_block(tx_hash, block_num);
                    block_tx_count += 1;

                    // Process logs for token transfers (ERC-20, ERC-721, ERC-1155)
                    if let Some(receipt) = receipts.get(tx_idx) {
                        for (log_index, log) in receipt.logs.iter().enumerate() {
                            let alloy_log = Log::new(log.address, log.topics().to_vec(), log.data.data.clone()).unwrap();
                            let decoded_transfers = decode_token_transfer(&alloy_log);
                            for decoded in decoded_transfers {
                                let transfer = match decoded.token_type {
                                    TokenType::Erc721 => TokenTransfer::new_erc721(
                                        tx_hash,
                                        log_index as u64,
                                        decoded.token_address,
                                        decoded.from,
                                        decoded.to,
                                        decoded.token_id.unwrap(),
                                        block_num,
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
                                        block_num,
                                        timestamp,
                                    ),
                                    TokenType::Erc20 => TokenTransfer::new(
                                        tx_hash,
                                        log_index as u64,
                                        decoded.token_address,
                                        decoded.from,
                                        decoded.to,
                                        decoded.value,
                                        block_num,
                                        timestamp,
                                    ),
                                };
                                batch.insert_transfer(transfer);
                                block_transfer_count += 1;

                                // Track new tokens for Meilisearch indexing
                                if !seen_tokens.contains(&decoded.token_address) {
                                    seen_tokens.insert(decoded.token_address);
                                    new_tokens.push((decoded.token_address, decoded.token_type));
                                }
                            }
                        }
                    }
                }

                // Record daily metrics for this block
                batch.record_block_timestamp(timestamp, block_tx_count, block_transfer_count);

                processed += 1;
                
                // Commit when batch is getting large (avoid FDB 10MB limit)
                if batch.is_large() {
                    // Update HLL with addresses from this batch before commit
                    for addr in batch.collect_addresses() {
                        address_hll.insert(&AddressWrapper(addr));
                    }
                    batch.commit(block_num).await?;
                    batch = index.write_batch();
                }

                if processed % 1000 == 0 {
                    let percent = (processed as f64 / total as f64) * 100.0;
                    info!(
                        block = block_num,
                        progress = format!("{:.2}%", percent),
                        "Progress"
                    );
                }
            }

            // Update HLL with addresses from this batch before commit
            for addr in batch.collect_addresses() {
                address_hll.insert(&AddressWrapper(addr));
            }

            // Commit remaining batch
            batch.commit(batch_end).await?;
            debug!(from = batch_start, to = batch_end, "Batch complete");

            // Save HLL count periodically
            hll_save_counter += batch_size;
            if hll_save_counter >= HLL_SAVE_INTERVAL {
                let count = address_hll.count() as u64;
                index.save_address_hll_count(count).await?;
                info!("Saved address HLL count (~{} unique addresses)", count);
                hll_save_counter = 0;
            }

            // Index new tokens to Meilisearch
            if let Some(search) = search {
                if !new_tokens.is_empty() {
                    let token_docs = fetch_token_docs_direct(reader, chain, &new_tokens);
                    if !token_docs.is_empty() {
                        if let Err(e) = search.index_tokens(&token_docs).await {
                            warn!("Failed to index tokens to Meilisearch: {}", e);
                        } else {
                            debug!(count = token_docs.len(), "Indexed tokens to Meilisearch");
                        }
                    }
                }
            }
        }

        // Final HLL count save
        let final_count = address_hll.count() as u64;
        index.save_address_hll_count(final_count).await?;
        info!("Final address count: ~{} unique addresses", final_count);

        Ok(())
    }

    fn fetch_token_docs_direct(
        _reader: &RethReader,
        chain: &str,
        tokens: &[(Address, TokenType)],
    ) -> Vec<TokenDocument> {
        let mut docs = Vec::new();

        for (token, token_type) in tokens {
            let addr = format!("{:?}", token);
            let type_str = token_type.as_str();

            docs.push(TokenDocument::new(
                chain,
                &addr,
                None,
                None,
                None,
                type_str,
            ));
        }

        docs
    }
}

// ============ JSON-RPC Mode ============

async fn run_rpc_backfill(
    rpc: &RpcClient,
    index: &FdbIndex,
    search: Option<&SearchClient>,
    chain: &str,
    from_block: u64,
    to_block: u64,
    batch_size: u64,
) -> Result<()> {
    let total = to_block - from_block + 1;
    let mut processed = 0u64;
    let mut seen_tokens: HashSet<Address> = HashSet::new();

    // Create in-memory HLL for address counting (we just save the count periodically)
    let mut address_hll = FdbIndex::new_address_hll();
    let prev_count = index.load_address_hll_count().await.unwrap_or(0);
    info!("Starting address HLL (previous count: ~{})", prev_count);
    let mut hll_save_counter = 0u64;
    const HLL_SAVE_INTERVAL: u64 = 10000; // Save HLL count every 10k blocks

    for batch_start in (from_block..=to_block).step_by(batch_size as usize) {
        let batch_end = (batch_start + batch_size - 1).min(to_block);

        let mut batch = index.write_batch();
        let mut new_tokens: Vec<(Address, TokenType)> = Vec::new();

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

            let mut block_tx_count = 0i64;
            let mut block_transfer_count = 0i64;

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
                block_tx_count += 1;

                // Process logs for token transfers (ERC-20, ERC-721, ERC-1155)
                if let Some(receipt) = receipt_map.get(&tx.hash) {
                    for (log_index, rpc_log) in receipt.logs.iter().enumerate() {
                        if let Some(log) = parse_log(rpc_log) {
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
                                        block_num,
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
                                        block_num,
                                        timestamp,
                                    ),
                                    TokenType::Erc20 => TokenTransfer::new(
                                        tx_hash,
                                        log_index as u64,
                                        decoded.token_address,
                                        decoded.from,
                                        decoded.to,
                                        decoded.value,
                                        block_num,
                                        timestamp,
                                    ),
                                };
                                batch.insert_transfer(transfer);
                                block_transfer_count += 1;

                                // Track new tokens for Meilisearch indexing
                                if !seen_tokens.contains(&decoded.token_address) {
                                    seen_tokens.insert(decoded.token_address);
                                    new_tokens.push((decoded.token_address, decoded.token_type));
                                }
                            }
                        }
                    }
                }
            }

            // Record daily metrics for this block
            batch.record_block_timestamp(timestamp, block_tx_count, block_transfer_count);

            processed += 1;
            
            // Commit when batch is getting large (avoid FDB 10MB limit)
            if batch.is_large() {
                // Update HLL with addresses from this batch before commit
                for addr in batch.collect_addresses() {
                    address_hll.insert(&AddressWrapper(addr));
                }
                batch.commit(block_num).await?;
                batch = index.write_batch();
            }

            if processed % 1000 == 0 {
                let percent = (processed as f64 / total as f64) * 100.0;
                info!(
                    block = block_num,
                    progress = format!("{:.2}%", percent),
                    "Progress"
                );
            }
        }

        // Update HLL with addresses from this batch before commit
        for addr in batch.collect_addresses() {
            address_hll.insert(&AddressWrapper(addr));
        }

        // Commit remaining batch
        batch.commit(batch_end).await?;
        debug!(from = batch_start, to = batch_end, "Batch complete");

        // Save HLL count periodically
        hll_save_counter += batch_size;
        if hll_save_counter >= HLL_SAVE_INTERVAL {
            let count = address_hll.count() as u64;
            index.save_address_hll_count(count).await?;
            info!("Saved address HLL count (~{} unique addresses)", count);
            hll_save_counter = 0;
        }

        // Index new tokens to Meilisearch
        if let Some(search) = search {
            if !new_tokens.is_empty() {
                let token_docs = fetch_token_docs(rpc, chain, &new_tokens).await;
                if !token_docs.is_empty() {
                    if let Err(e) = search.index_tokens(&token_docs).await {
                        warn!("Failed to index tokens to Meilisearch: {}", e);
                    } else {
                        debug!(count = token_docs.len(), "Indexed tokens to Meilisearch");
                    }
                }
            }
        }
    }

    // Final HLL count save
    let final_count = address_hll.count() as u64;
    index.save_address_hll_count(final_count).await?;
    info!("Final address count: ~{} unique addresses", final_count);

    Ok(())
}

async fn fetch_token_docs(rpc: &RpcClient, chain: &str, tokens: &[(Address, TokenType)]) -> Vec<TokenDocument> {
    let mut docs = Vec::new();
    
    for (token, token_type) in tokens {
        let addr = format!("{:?}", token);
        let type_str = token_type.as_str();
        
        // Only fetch name/symbol/decimals for ERC-20 tokens
        // NFTs often don't have decimals and may have different metadata patterns
        let (name, symbol, decimals) = if *token_type == TokenType::Erc20 {
            fetch_token_metadata_rpc(rpc, &addr).await
        } else {
            // For NFTs, try to fetch name and symbol but skip decimals
            let name = rpc
                .call_contract(&addr, "0x06fdde03") // name()
                .await
                .ok()
                .and_then(|r| decode_string_result(&r));
            let symbol = rpc
                .call_contract(&addr, "0x95d89b41") // symbol()
                .await
                .ok()
                .and_then(|r| decode_string_result(&r));
            (name, symbol, None)
        };
        
        docs.push(TokenDocument::new(
            chain,
            &addr,
            name,
            symbol,
            decimals.and_then(|d| d.parse().ok()),
            type_str,
        ));
    }
    
    docs
}

async fn fetch_token_metadata_rpc(
    rpc: &RpcClient,
    token_addr: &str,
) -> (Option<String>, Option<String>, Option<String>) {
    let name = rpc
        .call_contract(token_addr, "0x06fdde03") // name()
        .await
        .ok()
        .and_then(|r| decode_string_result(&r));

    let symbol = rpc
        .call_contract(token_addr, "0x95d89b41") // symbol()
        .await
        .ok()
        .and_then(|r| decode_string_result(&r));

    let decimals = rpc
        .call_contract(token_addr, "0x313ce567") // decimals()
        .await
        .ok()
        .map(|r| {
            let hex = r.trim_start_matches("0x");
            u64::from_str_radix(hex, 16)
                .map(|d| d.to_string())
                .unwrap_or_default()
        })
        .filter(|s| !s.is_empty());

    (name, symbol, decimals)
}

fn decode_string_result(hex: &str) -> Option<String> {
    let bytes = hex::decode(hex.trim_start_matches("0x")).ok()?;
    if bytes.len() < 64 {
        return None;
    }
    let offset = usize::from_be_bytes(bytes[24..32].try_into().ok()?);
    if offset + 32 > bytes.len() {
        return None;
    }
    let len = usize::from_be_bytes(bytes[offset + 24..offset + 32].try_into().ok()?);
    if offset + 32 + len > bytes.len() {
        return None;
    }
    String::from_utf8(bytes[offset + 32..offset + 32 + len].to_vec()).ok()
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    // Backend selection note:
    // The backfill tool currently uses FdbIndex concrete type throughout the codebase.
    // Full trait-based abstraction would require refactoring run_rpc_backfill and direct module.
    // For now, we support MDBX via separate code paths (TODO: refactor to use IndexDatabase trait)

    #[cfg(feature = "mdbx")]
    if args.mdbx_path.is_some() {
        eyre::bail!("MDBX backend for backfill not yet fully implemented. Use FDB for now. See Task 9 for migration tool.");
    }

    // Initialize FDB network
    #[cfg(feature = "fdb")]
    let _network = unsafe { blockscout_exex::fdb_index::init_fdb_network() };

    #[cfg(feature = "fdb")]
    info!("Connecting to FoundationDB...");
    #[cfg(feature = "fdb")]
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

    #[cfg(not(feature = "fdb"))]
    {
        eyre::bail!("Backfill requires FDB feature. Build with --features fdb");
    }

    // Check if using direct MDBX mode
    #[cfg(feature = "reth")]
    if let Some(reth_db) = &args.reth_db {
        let static_files = args
            .reth_static_files
            .as_ref()
            .map(|p| p.clone())
            .unwrap_or_else(|| reth_db.join("static_files"));

        info!("Opening reth database at {:?}", reth_db);
        info!("Static files at {:?}", static_files);

        let chain = &args.chain;
        let reader = if chain == "sepolia" {
            blockscout_exex::reth_reader::RethReader::open_sepolia(reth_db, &static_files)?
        } else {
            blockscout_exex::reth_reader::RethReader::open_mainnet(reth_db, &static_files)?
        };

        let from_block = if args.from_block == 0 {
            index.last_indexed_block().await?.map(|b| b + 1).unwrap_or(0)
        } else {
            args.from_block
        };

        let to_block = if args.to_block == 0 {
            reader.last_block_number()?.unwrap_or(0)
        } else {
            args.to_block
        };

        // Initialize Meilisearch client if configured
        let search = if let Some(ref meili_url) = args.meili_url {
            info!("Connecting to Meilisearch at {}", meili_url);
            let client = SearchClient::new(meili_url, args.meili_key.as_deref(), chain);
            if let Err(e) = client.ensure_indexes().await {
                warn!("Failed to configure Meilisearch indexes: {}", e);
            }
            Some(client)
        } else {
            None
        };

        info!(from = from_block, to = to_block, mode = "direct MDBX", "Starting backfill");

        direct::run_direct_backfill(direct::DirectBackfillConfig {
            reader: &reader,
            index: &index,
            search: search.as_ref(),
            chain,
            from_block,
            to_block,
            batch_size: args.batch_size,
        }).await?;

        info!("Backfill complete");
        return Ok(());
    }

    // Initialize Meilisearch client if configured
    let search = if let Some(ref meili_url) = args.meili_url {
        info!("Connecting to Meilisearch at {}", meili_url);
        let client = SearchClient::new(meili_url, args.meili_key.as_deref(), "sepolia");
        if let Err(e) = client.ensure_indexes().await {
            warn!("Failed to configure Meilisearch indexes: {}", e);
        }
        Some(client)
    } else {
        None
    };

    // IPC or HTTP JSON-RPC mode
    let rpc = if let Some(ipc_path) = &args.ipc_path {
        info!(mode = "IPC", path = %ipc_path.display(), "Starting backfill");
        RpcClient::new_ipc(ipc_path.clone())
    } else {
        info!(mode = "HTTP", url = %args.rpc_url, "Starting backfill");
        RpcClient::new_http(&args.rpc_url)
    };

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

    info!(from = from_block, to = to_block, "Block range");

    run_rpc_backfill(
        &rpc,
        &index,
        search.as_ref(),
        "sepolia",
        from_block,
        to_block,
        args.batch_size,
    )
    .await?;

    info!("Backfill complete");
    Ok(())
}
