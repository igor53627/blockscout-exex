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

        // AddressCounters has one entry per unique address
        let addr_counters_db = txn.open_db(Some("AddressCounters"))?;
        let addr_stat = txn.db_stat(&addr_counters_db)?;
        let addr_count = addr_stat.entries() as u64;
        info!("Total addresses (AddressCounters entries): {}", addr_count);

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
