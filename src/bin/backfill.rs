//! Backfill tool - indexes blockchain data into MDBX
//!
//! Reads directly from reth database and writes to MDBX index.
//!
//! Usage:
//!   blockscout-backfill --mdbx-path /path/to/mdbx --reth-db /path/to/reth/db

use std::collections::HashSet;
use std::path::PathBuf;

use alloy_primitives::{Address, Log};
use clap::Parser;
use eyre::Result;
use tracing::{debug, info, warn};

use blockscout_exex::IndexDatabase;
#[cfg(feature = "mdbx")]
use blockscout_exex::mdbx_index::MdbxIndex;
use blockscout_exex::meili::SearchClient;
use blockscout_exex::transform::{decode_token_transfer, TokenType};

#[derive(Parser)]
#[command(name = "blockscout-backfill")]
#[command(about = "Backfill MDBX index from reth database")]
struct Args {
    /// Path to reth database directory
    #[cfg(feature = "reth")]
    #[arg(long)]
    reth_db: PathBuf,

    /// Path to reth static files directory (required with --reth-db)
    #[cfg(feature = "reth")]
    #[arg(long)]
    reth_static_files: Option<PathBuf>,

    /// Chain name: mainnet or sepolia
    #[cfg(feature = "reth")]
    #[arg(long, default_value = "sepolia")]
    chain: String,

    /// MDBX index database path
    #[cfg(feature = "mdbx")]
    #[arg(long)]
    mdbx_path: PathBuf,

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

// ============ Direct MDBX Backfill ============

#[cfg(all(feature = "reth", feature = "mdbx"))]
mod direct_mdbx {
    use super::*;
    use alloy_primitives::Log;
    use blockscout_exex::mdbx_index::{MdbxIndex, AddressWrapper, TokenTransfer as MdbxTokenTransfer};
    use blockscout_exex::reth_reader::RethReader;
    use hyperloglogplus::HyperLogLog;

    pub struct DirectMdbxBackfillConfig<'a> {
        pub reader: &'a RethReader,
        pub index: &'a MdbxIndex,
        pub search: Option<&'a SearchClient>,
        pub chain: &'a str,
        pub from_block: u64,
        pub to_block: u64,
        pub batch_size: u64,
    }

    pub fn run_direct_mdbx_backfill(config: DirectMdbxBackfillConfig<'_>) -> Result<()> {
        let DirectMdbxBackfillConfig {
            reader,
            index,
            search: _search,
            chain: _chain,
            from_block,
            to_block,
            batch_size,
        } = config;

        let total = to_block - from_block + 1;
        let mut processed = 0u64;
        let mut seen_tokens: HashSet<Address> = HashSet::new();

        // Create in-memory HLL for address counting
        let mut address_hll = MdbxIndex::new_address_hll();
        let prev_count = index.load_address_hll_count().unwrap_or(0);
        info!("Starting address HLL (previous count: ~{})", prev_count);
        info!("Total blocks to process: {}", total);
        info!("Commit frequency: every 100 blocks");
        let mut hll_save_counter = 0u64;
        const HLL_SAVE_INTERVAL: u64 = 10000;

        for batch_start in (from_block..=to_block).step_by(batch_size as usize) {
            let batch_end = (batch_start + batch_size - 1).min(to_block);

            let mut batch = index.write_batch();

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

                    // Index by receiver
                    use alloy_consensus::transaction::Transaction as TxTrait;
                    if let Some(to) = TxTrait::to(tx) {
                        batch.insert_address_tx(to, tx_hash, block_num, tx_idx as u32);
                    }

                    // Index tx → block
                    batch.insert_tx_block(tx_hash, block_num);
                    block_tx_count += 1;

                    // Process logs for token transfers
                    if let Some(receipt) = receipts.get(tx_idx) {
                        for (log_index, log) in receipt.logs.iter().enumerate() {
                            let alloy_log = Log::new(log.address, log.topics().to_vec(), log.data.data.clone()).unwrap();
                            let decoded_transfers = decode_token_transfer(&alloy_log);
                            for decoded in decoded_transfers {
                                let transfer = match decoded.token_type {
                                    TokenType::Erc721 => MdbxTokenTransfer::new_erc721(
                                        tx_hash,
                                        log_index as u64,
                                        decoded.token_address,
                                        decoded.from,
                                        decoded.to,
                                        decoded.token_id.unwrap(),
                                        block_num,
                                        timestamp,
                                    ),
                                    TokenType::Erc1155 => MdbxTokenTransfer::new_erc1155(
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
                                    TokenType::Erc20 => MdbxTokenTransfer::new(
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

                                if !seen_tokens.contains(&decoded.token_address) {
                                    seen_tokens.insert(decoded.token_address);
                                }
                            }
                        }
                    }
                }

                // Record daily metrics
                batch.record_block_timestamp(timestamp, block_tx_count, block_transfer_count);

                processed += 1;

                // Commit every 100 blocks or when batch is large to avoid memory buildup
                let should_commit = processed % 100 == 0 || batch.is_large();
                if should_commit {
                    for addr in batch.collect_addresses() {
                        address_hll.insert(&AddressWrapper(addr));
                    }
                    batch.commit(block_num)?;
                    batch = index.write_batch();
                }

                if processed % 100 == 0 {
                    let percent = (processed as f64 / total as f64) * 100.0;
                    info!(
                        block = block_num,
                        progress = format!("{:.2}%", percent),
                        "Progress"
                    );
                }
            }

            // Update HLL before commit
            for addr in batch.collect_addresses() {
                address_hll.insert(&AddressWrapper(addr));
            }

            // Commit remaining batch
            batch.commit(batch_end)?;
            debug!(from = batch_start, to = batch_end, "Batch complete");

            // Save HLL count periodically
            hll_save_counter += batch_size;
            if hll_save_counter >= HLL_SAVE_INTERVAL {
                let count = address_hll.count() as u64;
                index.save_address_hll_count(count)?;
                info!("Saved address HLL count (~{} unique addresses)", count);
                hll_save_counter = 0;
            }
        }

        // Final HLL count save
        let final_count = address_hll.count() as u64;
        index.save_address_hll_count(final_count)?;
        info!("Final address count: ~{} unique addresses", final_count);

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    #[cfg(all(feature = "mdbx", feature = "reth"))]
    {
        let static_files = args
            .reth_static_files
            .as_ref()
            .cloned()
            .unwrap_or_else(|| args.reth_db.join("static_files"));

        info!("Opening reth database at {:?}", args.reth_db);
        info!("Static files at {:?}", static_files);
        info!("Target MDBX index at {:?}", args.mdbx_path);

        let chain = &args.chain;
        let reader = if chain == "sepolia" {
            blockscout_exex::reth_reader::RethReader::open_sepolia(&args.reth_db, &static_files)?
        } else {
            blockscout_exex::reth_reader::RethReader::open_mainnet(&args.reth_db, &static_files)?
        };

        let index = MdbxIndex::open(&args.mdbx_path)?;

        let from_block = if args.from_block == 0 {
            index.get_last_indexed_block()?.map(|b| b + 1).unwrap_or(0)
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
            Some(client)
        } else {
            None
        };

        info!(from = from_block, to = to_block, mode = "direct MDBX", "Starting backfill");

        direct_mdbx::run_direct_mdbx_backfill(direct_mdbx::DirectMdbxBackfillConfig {
            reader: &reader,
            index: &index,
            search: search.as_ref(),
            chain,
            from_block,
            to_block,
            batch_size: args.batch_size,
        })?;

        info!("MDBX backfill complete");
        return Ok(());
    }

    #[cfg(not(all(feature = "mdbx", feature = "reth")))]
    {
        eyre::bail!("Backfill requires MDBX and reth features. Build with --features mdbx,reth or --features reth");
    }
}
