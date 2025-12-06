//! Counter update tool - computes and updates counters after backfill
//!
//! Run this after backfill completes to populate:
//! - total_blocks (from IndexingStatus)
//! - total_txs (count from TxBlocks table)
//! - total_addresses (count from AddressCounters table - O(1) via stat)
//! - total_transfers (count from TokenTransfers table)
//!
//! Uses db.stat() for O(1) entry counts instead of iterating.

use std::path::PathBuf;
use clap::Parser;
use eyre::Result;
use tracing::info;

#[derive(Parser)]
#[command(name = "update-counters")]
#[command(about = "Update MDBX counters after backfill")]
struct Args {
    /// MDBX database path
    #[arg(long)]
    mdbx_path: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    info!("Opening MDBX database at {:?}", args.mdbx_path);

    #[cfg(feature = "mdbx")]
    {
        use reth_libmdbx::{Environment, EnvironmentFlags, Geometry, Mode, PageSize, SyncMode, DatabaseFlags, WriteFlags};

        // Open in read-write mode with longer timeout
        let env = Environment::builder()
            .set_geometry(Geometry {
                size: Some(0..(2 * 1024 * 1024 * 1024 * 1024)), // 2TB max
                growth_step: Some(1024 * 1024 * 1024),          // 1GB growth
                shrink_threshold: None,
                page_size: Some(PageSize::Set(4096)),
            })
            .set_flags(EnvironmentFlags {
                mode: Mode::ReadWrite { sync_mode: SyncMode::Durable },
                no_rdahead: true,
                coalesce: true,
                ..Default::default()
            })
            .set_max_dbs(20)
            .open(&args.mdbx_path)?;

        info!("Getting entry counts via stat() - O(1) operation...");

        let txn = env.begin_ro_txn()?;

        // Use stat() for O(1) entry counts
        let tx_blocks_db = txn.open_db(Some("TxBlocks"))?;
        let tx_stat = txn.db_stat(&tx_blocks_db)?;
        let tx_count = tx_stat.entries() as u64;
        info!("Total transactions (TxBlocks entries): {}", tx_count);

        // Count unique addresses from AddressTxs in batches to avoid transaction timeout
        // Keys are: address[20] + block[8] + tx_idx[4], sorted by address
        let addr_txs_db = txn.open_db(Some("AddressTxs"))?;
        let addr_txs_stat = txn.db_stat(&addr_txs_db)?;
        info!("AddressTxs has {} entries, counting unique addresses in batches...", addr_txs_stat.entries());
        drop(txn); // Release transaction before batch counting

        let mut addr_count = 0u64;
        let mut last_addr: Option<[u8; 20]> = None;
        let mut start_key: Option<Vec<u8>> = None;
        let batch_size = 10_000_000u64; // 10M entries per batch

        loop {
            let txn = env.begin_ro_txn()?;
            let addr_txs_db = txn.open_db(Some("AddressTxs"))?;
            let mut cursor = txn.cursor(&addr_txs_db)?;
            let mut batch_count = 0u64;

            // Position cursor
            let started = if let Some(ref key) = start_key {
                cursor.set_range::<(), ()>(key)?.is_some()
            } else {
                cursor.first::<(), ()>()?.is_some()
            };

            if !started {
                break;
            }

            loop {
                if let Some((key, _)) = cursor.get_current::<Vec<u8>, Vec<u8>>()? {
                    if key.len() >= 20 {
                        let addr: [u8; 20] = key[0..20].try_into().unwrap_or([0u8; 20]);
                        if last_addr.as_ref() != Some(&addr) {
                            addr_count += 1;
                            last_addr = Some(addr);
                            if addr_count % 500_000 == 0 {
                                info!("  Counted {} unique addresses...", addr_count);
                            }
                        }
                    }
                    start_key = Some(key.clone());
                }

                batch_count += 1;
                if batch_count >= batch_size {
                    // Move to next key before ending batch
                    if cursor.next::<(), ()>()?.is_none() {
                        start_key = None;
                    } else if let Some((key, _)) = cursor.get_current::<Vec<u8>, Vec<u8>>()? {
                        start_key = Some(key);
                    }
                    break;
                }

                if cursor.next::<(), ()>()?.is_none() {
                    start_key = None;
                    break;
                }
            }

            drop(cursor);
            drop(txn);

            if start_key.is_none() {
                break;
            }
            info!("  Batch complete, continuing from next position...");
        }
        info!("Total unique addresses: {}", addr_count);

        // Reopen transaction for remaining operations
        let txn = env.begin_ro_txn()?;

        // TokenTransfers count
        let transfers_db = txn.open_db(Some("TokenTransfers"))?;
        let transfer_stat = txn.db_stat(&transfers_db)?;
        let transfer_count = transfer_stat.entries() as u64;
        info!("Total transfers (TokenTransfers entries): {}", transfer_count);

        // Get last indexed block from Metadata
        let meta_db = txn.open_db(Some("Metadata"))?;
        let last_block: u64 = match txn.get::<Vec<u8>>(meta_db.dbi(), b"last_block") {
            Ok(Some(v)) if v.len() >= 8 => {
                u64::from_be_bytes(v[0..8].try_into().unwrap_or([0u8; 8]))
            }
            _ => 0,
        };
        info!("Last indexed block: {}", last_block);

        drop(txn);

        // Now write counters to Counters table (i64 little-endian, matching get_counter())
        info!("Writing counters to Counters table...");
        let txn = env.begin_rw_txn()?;
        let counters_db = txn.create_db(Some("Counters"), DatabaseFlags::default())?;

        // Write as i64 little-endian to match get_counter() expectations
        txn.put(counters_db.dbi(), b"total_txs", &(tx_count as i64).to_le_bytes(), WriteFlags::UPSERT)?;
        txn.put(counters_db.dbi(), b"total_addresses", &(addr_count as i64).to_le_bytes(), WriteFlags::UPSERT)?;
        txn.put(counters_db.dbi(), b"total_transfers", &(transfer_count as i64).to_le_bytes(), WriteFlags::UPSERT)?;
        // Also store last_block in Counters as fallback (in case Metadata read fails)
        txn.put(counters_db.dbi(), b"last_block", &(last_block as i64).to_le_bytes(), WriteFlags::UPSERT)?;

        txn.commit()?;

        info!("Counters updated successfully!");
        info!("  total_txs: {}", tx_count);
        info!("  total_addresses: {}", addr_count);
        info!("  total_transfers: {}", transfer_count);
        info!("  last_indexed_block: {}", last_block);

        Ok(())
    }

    #[cfg(not(feature = "mdbx"))]
    {
        Err(eyre::eyre!("MDBX support not enabled. Compile with --features mdbx"))
    }
}
