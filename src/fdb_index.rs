//! FoundationDB-based index storage for Blockscout data.
//!
//! Key schema:
//! - `\x01<address:20>/<block:8><tx_idx:4>` → tx_hash:32 (address_txs)
//! - `\x02<address:20>/<block:8><log_idx:8>` → transfer (address_transfers)
//! - `\x03<token:20>/<holder:20>` → balance:32 (token_holders)
//! - `\x04<tx_hash:32>` → block_num:8 (tx_block)
//! - `\x05last_block` → u64 (meta)

use std::path::Path;

use alloy_primitives::{Address, TxHash, U256};
use eyre::Result;
use foundationdb::{Database, RangeOption};
use serde::{Deserialize, Serialize};

// Key prefixes for different tables
const PREFIX_ADDRESS_TXS: u8 = 0x01;
const PREFIX_ADDRESS_TRANSFERS: u8 = 0x02;
const PREFIX_TOKEN_HOLDERS: u8 = 0x03;
const PREFIX_TX_BLOCK: u8 = 0x04;
const PREFIX_META: u8 = 0x05;

const META_LAST_BLOCK: &[u8] = b"last_block";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenTransfer {
    pub tx_hash: [u8; 32],
    pub log_index: u64,
    pub token_address: [u8; 20],
    pub from: [u8; 20],
    pub to: [u8; 20],
    pub value: [u8; 32],
    pub block_number: u64,
    pub timestamp: u64,
}

impl TokenTransfer {
    pub fn new(
        tx_hash: TxHash,
        log_index: u64,
        token_address: Address,
        from: Address,
        to: Address,
        value: U256,
        block_number: u64,
        timestamp: u64,
    ) -> Self {
        Self {
            tx_hash: tx_hash.0,
            log_index,
            token_address: token_address.0 .0,
            from: from.0 .0,
            to: to.0 .0,
            value: value.to_be_bytes(),
            block_number,
            timestamp,
        }
    }
}

pub struct FdbIndex {
    db: Database,
}

impl FdbIndex {
    /// Open a connection to FoundationDB using the default cluster file.
    /// On most systems this is `/usr/local/etc/foundationdb/fdb.cluster`
    pub fn open_default() -> Result<Self> {
        let db = Database::default()?;
        Ok(Self { db })
    }

    /// Open a connection to FoundationDB using a specific cluster file.
    pub fn open(cluster_file: impl AsRef<Path>) -> Result<Self> {
        let db = Database::from_path(cluster_file.as_ref().to_str().unwrap())?;
        Ok(Self { db })
    }

    /// Get the database handle for advanced operations
    pub fn db(&self) -> &Database {
        &self.db
    }

    // Key builders

    fn address_tx_key(address: &Address, block: u64, tx_idx: u32) -> Vec<u8> {
        let mut key = Vec::with_capacity(1 + 20 + 8 + 4);
        key.push(PREFIX_ADDRESS_TXS);
        key.extend_from_slice(address.as_slice());
        key.extend_from_slice(&block.to_be_bytes());
        key.extend_from_slice(&tx_idx.to_be_bytes());
        key
    }

    fn address_tx_prefix(address: &Address) -> Vec<u8> {
        let mut key = Vec::with_capacity(1 + 20);
        key.push(PREFIX_ADDRESS_TXS);
        key.extend_from_slice(address.as_slice());
        key
    }

    fn address_transfer_key(address: &Address, block: u64, log_idx: u64) -> Vec<u8> {
        let mut key = Vec::with_capacity(1 + 20 + 8 + 8);
        key.push(PREFIX_ADDRESS_TRANSFERS);
        key.extend_from_slice(address.as_slice());
        key.extend_from_slice(&block.to_be_bytes());
        key.extend_from_slice(&log_idx.to_be_bytes());
        key
    }

    fn address_transfer_prefix(address: &Address) -> Vec<u8> {
        let mut key = Vec::with_capacity(1 + 20);
        key.push(PREFIX_ADDRESS_TRANSFERS);
        key.extend_from_slice(address.as_slice());
        key
    }

    fn token_holder_key(token: &Address, holder: &Address) -> Vec<u8> {
        let mut key = Vec::with_capacity(1 + 20 + 20);
        key.push(PREFIX_TOKEN_HOLDERS);
        key.extend_from_slice(token.as_slice());
        key.extend_from_slice(holder.as_slice());
        key
    }

    fn token_holder_prefix(token: &Address) -> Vec<u8> {
        let mut key = Vec::with_capacity(1 + 20);
        key.push(PREFIX_TOKEN_HOLDERS);
        key.extend_from_slice(token.as_slice());
        key
    }

    fn tx_block_key(tx_hash: &TxHash) -> Vec<u8> {
        let mut key = Vec::with_capacity(1 + 32);
        key.push(PREFIX_TX_BLOCK);
        key.extend_from_slice(tx_hash.as_slice());
        key
    }

    fn meta_key(name: &[u8]) -> Vec<u8> {
        let mut key = Vec::with_capacity(1 + name.len());
        key.push(PREFIX_META);
        key.extend_from_slice(name);
        key
    }

    // Meta operations

    pub async fn last_indexed_block(&self) -> Result<Option<u64>> {
        let key = Self::meta_key(META_LAST_BLOCK);
        let result = self
            .db
            .run(|trx, _| {
                let key = key.clone();
                async move { Ok(trx.get(&key, false).await?) }
            })
            .await?;

        match result {
            Some(value) => {
                let bytes: [u8; 8] = value.as_ref().try_into()?;
                Ok(Some(u64::from_be_bytes(bytes)))
            }
            None => Ok(None),
        }
    }

    pub async fn set_last_indexed_block(&self, block: u64) -> Result<()> {
        let key = Self::meta_key(META_LAST_BLOCK);
        let value = block.to_be_bytes();
        self.db
            .run(|trx, _| {
                let key = key.clone();
                async move {
                    trx.set(&key, &value);
                    Ok(())
                }
            })
            .await?;
        Ok(())
    }

    // Read operations

    pub async fn get_address_txs(
        &self,
        address: &Address,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<TxHash>> {
        let prefix = Self::address_tx_prefix(address);
        let mut end_key = prefix.clone();
        end_key.push(0xFF);

        let range = RangeOption {
            limit: Some((limit + offset) as usize),
            reverse: true, // Most recent first
            ..RangeOption::from(prefix.as_slice()..end_key.as_slice())
        };

        let results = self
            .db
            .run(|trx, _| {
                let range = range.clone();
                async move {
                    let kvs = trx.get_range(&range, 0, false).await?;
                    Ok(kvs.into_iter().collect::<Vec<_>>())
                }
            })
            .await?;

        let txs: Vec<TxHash> = results
            .into_iter()
            .skip(offset)
            .take(limit)
            .filter_map(|kv| {
                let value = kv.value();
                if value.len() == 32 {
                    let mut hash = [0u8; 32];
                    hash.copy_from_slice(value);
                    Some(TxHash::from(hash))
                } else {
                    None
                }
            })
            .collect();

        Ok(txs)
    }

    pub async fn get_address_transfers(
        &self,
        address: &Address,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<TokenTransfer>> {
        let prefix = Self::address_transfer_prefix(address);
        let mut end_key = prefix.clone();
        end_key.push(0xFF);

        let range = RangeOption {
            limit: Some((limit + offset) as usize),
            reverse: true,
            ..RangeOption::from(prefix.as_slice()..end_key.as_slice())
        };

        let results = self
            .db
            .run(|trx, _| {
                let range = range.clone();
                async move {
                    let kvs = trx.get_range(&range, 0, false).await?;
                    Ok(kvs.into_iter().collect::<Vec<_>>())
                }
            })
            .await?;

        let transfers: Vec<TokenTransfer> = results
            .into_iter()
            .skip(offset)
            .take(limit)
            .filter_map(|kv| bincode::deserialize(kv.value()).ok())
            .collect();

        Ok(transfers)
    }

    pub async fn get_token_holders(
        &self,
        token: &Address,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<(Address, U256)>> {
        let prefix = Self::token_holder_prefix(token);
        let mut end_key = prefix.clone();
        end_key.push(0xFF);

        let range = RangeOption {
            limit: Some((limit + offset) as usize),
            ..RangeOption::from(prefix.as_slice()..end_key.as_slice())
        };

        let results = self
            .db
            .run(|trx, _| {
                let range = range.clone();
                async move {
                    let kvs = trx.get_range(&range, 0, false).await?;
                    Ok(kvs.into_iter().collect::<Vec<_>>())
                }
            })
            .await?;

        let holders: Vec<(Address, U256)> = results
            .into_iter()
            .skip(offset)
            .take(limit)
            .filter_map(|kv| {
                let key = kv.key();
                let value = kv.value();
                // Key format: prefix(1) + token(20) + holder(20)
                if key.len() == 41 && value.len() == 32 {
                    let mut holder_bytes = [0u8; 20];
                    holder_bytes.copy_from_slice(&key[21..41]);
                    let mut balance_bytes = [0u8; 32];
                    balance_bytes.copy_from_slice(value);
                    Some((
                        Address::from(holder_bytes),
                        U256::from_be_bytes(balance_bytes),
                    ))
                } else {
                    None
                }
            })
            .collect();

        Ok(holders)
    }

    pub async fn get_tx_block(&self, tx_hash: &TxHash) -> Result<Option<u64>> {
        let key = Self::tx_block_key(tx_hash);
        let result = self
            .db
            .run(|trx, _| {
                let key = key.clone();
                async move { Ok(trx.get(&key, false).await?) }
            })
            .await?;

        match result {
            Some(value) => {
                let bytes: [u8; 8] = value.as_ref().try_into()?;
                Ok(Some(u64::from_be_bytes(bytes)))
            }
            None => Ok(None),
        }
    }

    // Write batch for bulk operations
    pub fn write_batch(&self) -> WriteBatch<'_> {
        WriteBatch {
            db: &self.db,
            address_txs: Vec::new(),
            tx_blocks: Vec::new(),
            transfers: Vec::new(),
            holder_updates: Vec::new(),
        }
    }
}

pub struct WriteBatch<'a> {
    db: &'a Database,
    address_txs: Vec<(Address, TxHash, u64, u32)>, // (address, tx_hash, block, tx_idx)
    tx_blocks: Vec<(TxHash, u64)>,
    transfers: Vec<TokenTransfer>,
    holder_updates: Vec<(Address, Address, U256)>, // (token, holder, balance)
}

impl<'a> WriteBatch<'a> {
    pub fn insert_address_tx(&mut self, address: Address, tx_hash: TxHash, block: u64, tx_idx: u32) {
        self.address_txs.push((address, tx_hash, block, tx_idx));
    }

    pub fn insert_tx_block(&mut self, tx_hash: TxHash, block_number: u64) {
        self.tx_blocks.push((tx_hash, block_number));
    }

    pub fn insert_transfer(&mut self, transfer: TokenTransfer) {
        self.transfers.push(transfer);
    }

    pub fn update_holder_balance(&mut self, token: Address, holder: Address, balance: U256) {
        self.holder_updates.push((token, holder, balance));
    }

    pub async fn commit(self, last_block: u64) -> Result<()> {
        let address_txs = self.address_txs;
        let tx_blocks = self.tx_blocks;
        let transfers = self.transfers;
        let holder_updates = self.holder_updates;

        self.db
            .run(|trx, _| {
                let address_txs = address_txs.clone();
                let tx_blocks = tx_blocks.clone();
                let transfers = transfers.clone();
                let holder_updates = holder_updates.clone();

                async move {
                    // Write address → tx mappings
                    for (addr, tx_hash, block, tx_idx) in &address_txs {
                        let key = FdbIndex::address_tx_key(addr, *block, *tx_idx);
                        trx.set(&key, tx_hash.as_slice());
                    }

                    // Write tx → block mappings
                    for (tx_hash, block_num) in &tx_blocks {
                        let key = FdbIndex::tx_block_key(tx_hash);
                        trx.set(&key, &block_num.to_be_bytes());
                    }

                    // Write transfers (indexed by both from and to address)
                    for transfer in &transfers {
                        let value = bincode::serialize(transfer).unwrap();

                        // Index by 'from' address
                        let from_addr = Address::from(transfer.from);
                        let from_key = FdbIndex::address_transfer_key(
                            &from_addr,
                            transfer.block_number,
                            transfer.log_index,
                        );
                        trx.set(&from_key, &value);

                        // Index by 'to' address
                        let to_addr = Address::from(transfer.to);
                        let to_key = FdbIndex::address_transfer_key(
                            &to_addr,
                            transfer.block_number,
                            transfer.log_index,
                        );
                        trx.set(&to_key, &value);
                    }

                    // Write holder balance updates
                    for (token, holder, balance) in &holder_updates {
                        let key = FdbIndex::token_holder_key(token, holder);
                        trx.set(&key, &balance.to_be_bytes::<32>());
                    }

                    // Update last indexed block
                    let meta_key = FdbIndex::meta_key(META_LAST_BLOCK);
                    trx.set(&meta_key, &last_block.to_be_bytes());

                    Ok(())
                }
            })
            .await?;

        Ok(())
    }
}

/// Initialize the FDB network. Must be called once before using FdbIndex.
/// Returns a guard that should be kept alive for the duration of the program.
/// 
/// # Safety
/// This is marked unsafe because the network must be shut down before the
/// program exits. The returned guard handles this automatically when dropped.
pub unsafe fn init_fdb_network() -> foundationdb::api::NetworkAutoStop {
    foundationdb::boot()
}
