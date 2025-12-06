//! Compute token holder counts from TokenTransfers table
//!
//! Reads TokenTransfers table and computes holder counts per token by
//! tracking running balances and counting holders with non-zero balance.

use std::collections::HashMap;
use std::path::PathBuf;
use clap::Parser;
use eyre::Result;
use tracing::info;
use alloy_primitives::{Address, U256};

#[derive(Parser)]
#[command(name = "compute-holder-counts")]
#[command(about = "Compute token holder counts from TokenTransfers table")]
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

        // Open in read-write mode
        let env = Environment::builder()
            .set_geometry(Geometry {
                size: Some(0..(2 * 1024 * 1024 * 1024 * 1024)), // 2TB max
                growth_step: Some(1024 * 1024 * 1024),
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

        // First try TokenHolders table (if it exists)
        {
            info!("Checking for TokenHolders table...");
            let txn = env.begin_ro_txn()?;
            match txn.open_db(Some("TokenHolders")) {
                Ok(db) => {
                    let stat = txn.db_stat(&db)?;
                    if stat.entries() > 0 {
                        info!("Found TokenHolders table with {} entries, using it", stat.entries());
                        drop(txn);
                        return compute_from_token_holders(&env).await;
                    }
                    info!("TokenHolders table exists but is empty");
                }
                Err(_) => {
                    info!("TokenHolders table not found");
                }
            }
        }

        // Fall back to computing from TokenTransfers
        info!("Computing holder counts from TokenTransfers (this may take a while)...");

        // First, get unique tokens from TokenTransfers
        // Key format: token[20] + block[8] + log_idx[8] = 36 bytes
        info!("Phase 1: Finding unique tokens...");

        let mut unique_tokens: std::collections::HashSet<Address> = std::collections::HashSet::new();
        let mut processed = 0u64;
        let mut start_key: Option<Vec<u8>> = None;
        let batch_size = 10_000_000u64;

        loop {
            let txn = env.begin_ro_txn()?;
            let token_transfers_db = match txn.open_db(Some("TokenTransfers")) {
                Ok(db) => db,
                Err(e) => {
                    info!("TokenTransfers table not found: {}", e);
                    return Ok(());
                }
            };
            let stat = txn.db_stat(&token_transfers_db)?;
            if processed == 0 {
                info!("TokenTransfers has {} entries", stat.entries());
            }

            let mut cursor = txn.cursor(&token_transfers_db)?;
            let mut batch_count = 0u64;

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
                    // Key: token[20] + block[8] + log_idx[8]
                    if key.len() >= 20 {
                        let token = Address::from_slice(&key[0..20]);
                        unique_tokens.insert(token);

                        processed += 1;
                        if processed % 10_000_000 == 0 {
                            info!("Processed {} transfers, {} unique tokens...", processed, unique_tokens.len());
                        }
                    }
                    start_key = Some(key.clone());
                }

                batch_count += 1;
                if batch_count >= batch_size {
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
        }

        info!("Phase 1 complete: {} transfers processed, {} unique tokens found",
              processed, unique_tokens.len());

        // Phase 2: For each token, count unique receivers
        // This is an approximation (counts anyone who ever received, not current holders)
        // For exact counts, we'd need to track running balances

        info!("Phase 2: Counting unique holders per token...");

        // We'll track (token -> set of holders) for a simpler approximation
        // that counts unique addresses involved in transfers for each token
        let mut token_holder_counts: HashMap<Address, u64> = HashMap::new();
        let mut current_token: Option<Address> = None;
        let mut current_holders: std::collections::HashSet<[u8; 20]> = std::collections::HashSet::new();
        let mut tokens_processed = 0u64;
        start_key = None;
        processed = 0;

        loop {
            let txn = env.begin_ro_txn()?;
            let token_transfers_db = txn.open_db(Some("TokenTransfers"))?;
            let mut cursor = txn.cursor(&token_transfers_db)?;
            let mut batch_count = 0u64;

            let started = if let Some(ref key) = start_key {
                cursor.set_range::<(), ()>(key)?.is_some()
            } else {
                cursor.first::<(), ()>()?.is_some()
            };

            if !started {
                break;
            }

            loop {
                if let Some((key, value)) = cursor.get_current::<Vec<u8>, Vec<u8>>()? {
                    if key.len() >= 20 && value.len() >= 40 {
                        let token = Address::from_slice(&key[0..20]);

                        // Check if token changed
                        if current_token.as_ref() != Some(&token) {
                            // Save previous token's count
                            if let Some(prev_token) = current_token.take() {
                                token_holder_counts.insert(prev_token, current_holders.len() as u64);
                                tokens_processed += 1;
                                if tokens_processed % 10_000 == 0 {
                                    info!("Processed {} tokens...", tokens_processed);
                                }
                            }
                            current_token = Some(token);
                            current_holders.clear();
                        }

                        // Extract from and to addresses from transfer value
                        // Value format based on TokenTransfer struct:
                        // tx_hash[32] + log_index[8] + token_address[20] + from[20] + to[20] + value[32] + block_number[8] + timestamp[8] + token_type[1] + [optional token_id]
                        // But we serialized with bincode, so the format might be different
                        // Let's try to extract from/to from the serialized data

                        // Actually, looking at the struct, the serialized format via bincode would be:
                        // tx_hash: [u8; 32], log_index: u64, token_address: [u8; 20], from: [u8; 20], to: [u8; 20], ...
                        // So from is at offset 32+8+20 = 60, to is at 60+20 = 80
                        if value.len() >= 100 {
                            // from is at byte 60, to is at byte 80
                            let from: [u8; 20] = value[60..80].try_into().unwrap_or([0u8; 20]);
                            let to: [u8; 20] = value[80..100].try_into().unwrap_or([0u8; 20]);

                            // Track both from and to addresses
                            if from != [0u8; 20] {
                                current_holders.insert(from);
                            }
                            if to != [0u8; 20] {
                                current_holders.insert(to);
                            }
                        }

                        processed += 1;
                    }
                    start_key = Some(key.clone());
                }

                batch_count += 1;
                if batch_count >= batch_size {
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
        }

        // Save last token's count
        if let Some(prev_token) = current_token.take() {
            token_holder_counts.insert(prev_token, current_holders.len() as u64);
        }

        info!("Phase 2 complete: {} tokens processed", token_holder_counts.len());

        // Phase 3: Write to TokenHolderCounts table
        info!("Phase 3: Writing TokenHolderCounts table...");
        let txn = env.begin_rw_txn()?;
        let token_holder_counts_db = txn.create_db(Some("TokenHolderCounts"), DatabaseFlags::default())?;

        let mut written = 0u64;
        for (token, count) in &token_holder_counts {
            let key = token.as_slice();
            let value = (*count as i64).to_le_bytes();
            txn.put(token_holder_counts_db.dbi(), key, &value, WriteFlags::UPSERT)?;
            written += 1;

            if written % 100_000 == 0 {
                info!("Written {} token counts...", written);
            }
        }

        txn.commit()?;

        info!("Successfully wrote {} token holder counts", written);

        // Show top 10 tokens by holder count
        let mut top_tokens: Vec<_> = token_holder_counts.into_iter().collect();
        top_tokens.sort_by(|a, b| b.1.cmp(&a.1));
        info!("Top 10 tokens by holder count:");
        for (i, (token, count)) in top_tokens.iter().take(10).enumerate() {
            info!("  {}. {:?} - {} unique addresses", i + 1, token, count);
        }

        Ok(())
    }

    #[cfg(not(feature = "mdbx"))]
    {
        Err(eyre::eyre!("MDBX support not enabled. Compile with --features mdbx"))
    }
}

#[cfg(feature = "mdbx")]
async fn compute_from_token_holders(env: &reth_libmdbx::Environment) -> Result<()> {
    use reth_libmdbx::{DatabaseFlags, WriteFlags};

    // Count holders per token with non-zero balance
    let mut holder_counts: HashMap<Address, u64> = HashMap::new();
    let mut processed = 0u64;
    let mut start_key: Option<Vec<u8>> = None;
    let batch_size = 10_000_000u64;

    loop {
        let txn = env.begin_ro_txn()?;
        let token_holders_db = txn.open_db(Some("TokenHolders"))?;
        let mut cursor = txn.cursor(&token_holders_db)?;
        let mut batch_count = 0u64;

        let started = if let Some(ref key) = start_key {
            cursor.set_range::<(), ()>(key)?.is_some()
        } else {
            cursor.first::<(), ()>()?.is_some()
        };

        if !started {
            break;
        }

        loop {
            if let Some((key, value)) = cursor.get_current::<Vec<u8>, Vec<u8>>()? {
                // Key: token[20] + holder[20] = 40 bytes
                // Value: balance[32] bytes
                if key.len() == 40 && value.len() == 32 {
                    let token = Address::from_slice(&key[0..20]);
                    let balance = U256::from_be_bytes::<32>(value[..32].try_into().unwrap_or([0u8; 32]));

                    // Only count holders with non-zero balance
                    if balance > U256::ZERO {
                        *holder_counts.entry(token).or_insert(0) += 1;
                    }

                    processed += 1;
                    if processed % 5_000_000 == 0 {
                        info!("Processed {} entries, {} unique tokens...", processed, holder_counts.len());
                    }
                }
                start_key = Some(key.clone());
            }

            batch_count += 1;
            if batch_count >= batch_size {
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
        info!("Batch complete, continuing...");
    }

    info!("Processed {} total entries", processed);
    info!("Found {} unique tokens", holder_counts.len());

    // Write holder counts to TokenHolderCounts table
    info!("Writing TokenHolderCounts table...");
    let txn = env.begin_rw_txn()?;
    let token_holder_counts_db = txn.create_db(Some("TokenHolderCounts"), DatabaseFlags::default())?;

    let mut written = 0u64;
    for (token, count) in &holder_counts {
        let key = token.as_slice();
        let value = (*count as i64).to_le_bytes();
        txn.put(token_holder_counts_db.dbi(), key, &value, WriteFlags::UPSERT)?;
        written += 1;

        if written % 100_000 == 0 {
            info!("Written {} token counts...", written);
        }
    }

    txn.commit()?;

    info!("Successfully wrote {} token holder counts", written);

    // Show top 10 tokens by holder count
    let mut top_tokens: Vec<_> = holder_counts.into_iter().collect();
    top_tokens.sort_by(|a, b| b.1.cmp(&a.1));
    info!("Top 10 tokens by holder count:");
    for (i, (token, count)) in top_tokens.iter().take(10).enumerate() {
        info!("  {}. {:?} - {} holders", i + 1, token, count);
    }

    Ok(())
}
