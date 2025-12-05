//! Counter update tool - computes and updates counters after backfill
//!
//! Run this after backfill completes to populate:
//! - total_blocks (from IndexingStatus)
//! - total_txs (count from TxBlocks table)
//! - total_addresses (count unique addresses)
//! - total_transfers (count from TokenTransfers)

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
        use blockscout_exex::mdbx_index::MdbxIndex;
        use reth_libmdbx::{Environment, EnvironmentFlags, Geometry, Mode, PageSize, SyncMode};

        // Open in read-write mode
        let env = Environment::builder()
            .set_geometry(Geometry {
                size: Some(0..(1024 * 1024 * 1024 * 1024)), // 1TB max
                growth_step: Some(1024 * 1024 * 1024),      // 1GB growth
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

        info!("Counting entries in tables...");

        let txn = env.begin_ro_txn()?;

        // Count TxBlocks entries (total transactions)
        let tx_blocks_db = txn.open_db(Some("TxBlocks"))?;
        let mut tx_count: u64 = 0;
        {
            let mut cursor = txn.cursor(&tx_blocks_db)?;
            while cursor.next::<(), ()>()?.is_some() {
                tx_count += 1;
                if tx_count % 1_000_000 == 0 {
                    info!("  Counted {} txs...", tx_count);
                }
            }
        }
        info!("Total transactions: {}", tx_count);

        // Count unique addresses from AddressTxs
        let addr_txs_db = txn.open_db(Some("AddressTxs"))?;
        let mut last_addr: Option<[u8; 20]> = None;
        let mut addr_count: u64 = 0;
        {
            let mut cursor = txn.cursor(&addr_txs_db)?;
            while let Some((key, _)) = cursor.next::<[u8], ()>()? {
                if key.len() >= 20 {
                    let addr: [u8; 20] = key[0..20].try_into().unwrap_or([0u8; 20]);
                    if last_addr.as_ref() != Some(&addr) {
                        addr_count += 1;
                        last_addr = Some(addr);
                        if addr_count % 100_000 == 0 {
                            info!("  Counted {} addresses...", addr_count);
                        }
                    }
                }
            }
        }
        info!("Total addresses: {}", addr_count);

        // Count transfers
        let transfers_db = txn.open_db(Some("TokenTransfers"))?;
        let mut transfer_count: u64 = 0;
        {
            let mut cursor = txn.cursor(&transfers_db)?;
            while cursor.next::<(), ()>()?.is_some() {
                transfer_count += 1;
                if transfer_count % 1_000_000 == 0 {
                    info!("  Counted {} transfers...", transfer_count);
                }
            }
        }
        info!("Total transfers: {}", transfer_count);

        // Get last indexed block from IndexingStatus
        let meta_db = txn.open_db(Some("Metadata"))?;
        let last_block: u64 = txn.get(&meta_db, b"last_indexed_block")?
            .map(|v: &[u8]| {
                if v.len() >= 8 {
                    u64::from_be_bytes(v[0..8].try_into().unwrap_or([0u8; 8]))
                } else {
                    0
                }
            })
            .unwrap_or(0);
        info!("Last indexed block: {}", last_block);

        drop(txn);

        // Now write counters
        info!("Writing counters...");
        let txn = env.begin_rw_txn()?;
        let meta_db = txn.open_db(Some("Metadata"))?;

        txn.put(&meta_db, b"total_txs", &tx_count.to_be_bytes(), reth_libmdbx::WriteFlags::UPSERT)?;
        txn.put(&meta_db, b"total_addresses", &addr_count.to_be_bytes(), reth_libmdbx::WriteFlags::UPSERT)?;
        txn.put(&meta_db, b"total_transfers", &transfer_count.to_be_bytes(), reth_libmdbx::WriteFlags::UPSERT)?;

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
