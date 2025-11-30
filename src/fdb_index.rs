//! FoundationDB-based index storage for Blockscout data.
//!
//! Key schema:
//! - `\x01<address:20>/<block:8><tx_idx:4>` → tx_hash:32 (address_txs)
//! - `\x02<address:20>/<block:8><log_idx:8>` → transfer (address_transfers)
//! - `\x03<token:20>/<holder:20>` → balance:32 (token_holders)
//! - `\x04<tx_hash:32>` → block_num:8 (tx_block)
//! - `\x05last_block` → u64 (meta)
//! - `\x06<counter_name>` → i64_le (counters: total_txs, total_addresses, total_transfers)
//! - `\x08<token:20>/<block:8><log_idx:8>` → transfer (token_transfers)
//! - `\x09<address:20><kind:1>` → i64_le (address_counters: 0=tx, 1=token_transfer)
//! - `\x0A<token:20>` → i64_le (token_holder_counts)
//! - `\x0B<yyyy:2><mm:1><dd:1><metric:1>` → i64_le (daily_metrics: 0=txs, 1=transfers)

use std::path::Path;

use alloy_primitives::{Address, TxHash, U256};
use eyre::Result;
use foundationdb::options::MutationType;
use foundationdb::{Database, RangeOption};
use serde::{Deserialize, Serialize};

// Key prefixes for different tables
const PREFIX_ADDRESS_TXS: u8 = 0x01;
const PREFIX_ADDRESS_TRANSFERS: u8 = 0x02;
const PREFIX_TOKEN_HOLDERS: u8 = 0x03;
const PREFIX_TX_BLOCK: u8 = 0x04;
const PREFIX_META: u8 = 0x05;
const PREFIX_COUNTERS: u8 = 0x06;
const PREFIX_TOKEN_TRANSFERS: u8 = 0x08;
const PREFIX_ADDRESS_COUNTERS: u8 = 0x09;
const PREFIX_TOKEN_HOLDER_COUNTS: u8 = 0x0A;
const PREFIX_DAILY_METRICS: u8 = 0x0B;

const META_LAST_BLOCK: &[u8] = b"last_block";
const COUNTER_TOTAL_TXS: &[u8] = b"total_txs";
const COUNTER_TOTAL_ADDRESSES: &[u8] = b"total_addresses";
const COUNTER_TOTAL_TRANSFERS: &[u8] = b"total_transfers";

// Address counter kinds
const ADDR_COUNTER_TX: u8 = 0;
const ADDR_COUNTER_TOKEN_TRANSFER: u8 = 1;

// Daily metric kinds
const METRIC_TXS: u8 = 0;
const METRIC_TRANSFERS: u8 = 1;

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
    #[serde(default)]
    pub token_type: u8,  // 0=ERC-20, 1=ERC-721, 2=ERC-1155
    #[serde(default)]
    pub token_id: Option<[u8; 32]>,  // For ERC-721/ERC-1155
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
            token_type: 0, // ERC-20
            token_id: None,
        }
    }
    
    pub fn new_erc721(
        tx_hash: TxHash,
        log_index: u64,
        token_address: Address,
        from: Address,
        to: Address,
        token_id: U256,
        block_number: u64,
        timestamp: u64,
    ) -> Self {
        Self {
            tx_hash: tx_hash.0,
            log_index,
            token_address: token_address.0 .0,
            from: from.0 .0,
            to: to.0 .0,
            value: U256::from(1).to_be_bytes(), // ERC-721 transfers exactly 1 token
            block_number,
            timestamp,
            token_type: 1, // ERC-721
            token_id: Some(token_id.to_be_bytes()),
        }
    }
    
    pub fn new_erc1155(
        tx_hash: TxHash,
        log_index: u64,
        token_address: Address,
        from: Address,
        to: Address,
        token_id: U256,
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
            token_type: 2, // ERC-1155
            token_id: Some(token_id.to_be_bytes()),
        }
    }
    
    pub fn token_type_str(&self) -> &'static str {
        match self.token_type {
            1 => "ERC-721",
            2 => "ERC-1155",
            _ => "ERC-20",
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

    fn token_transfer_key(token: &Address, block: u64, log_idx: u64) -> Vec<u8> {
        let mut key = Vec::with_capacity(1 + 20 + 8 + 8);
        key.push(PREFIX_TOKEN_TRANSFERS);
        key.extend_from_slice(token.as_slice());
        key.extend_from_slice(&block.to_be_bytes());
        key.extend_from_slice(&log_idx.to_be_bytes());
        key
    }

    fn token_transfer_prefix(token: &Address) -> Vec<u8> {
        let mut key = Vec::with_capacity(1 + 20);
        key.push(PREFIX_TOKEN_TRANSFERS);
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

    fn counter_key(name: &[u8]) -> Vec<u8> {
        let mut key = Vec::with_capacity(1 + name.len());
        key.push(PREFIX_COUNTERS);
        key.extend_from_slice(name);
        key
    }

    fn address_counter_key(address: &Address, kind: u8) -> Vec<u8> {
        let mut key = Vec::with_capacity(1 + 20 + 1);
        key.push(PREFIX_ADDRESS_COUNTERS);
        key.extend_from_slice(address.as_slice());
        key.push(kind);
        key
    }

    fn token_holder_count_key(token: &Address) -> Vec<u8> {
        let mut key = Vec::with_capacity(1 + 20);
        key.push(PREFIX_TOKEN_HOLDER_COUNTS);
        key.extend_from_slice(token.as_slice());
        key
    }

    fn daily_metric_key(year: u16, month: u8, day: u8, metric: u8) -> Vec<u8> {
        let mut key = Vec::with_capacity(1 + 2 + 1 + 1 + 1);
        key.push(PREFIX_DAILY_METRICS);
        key.extend_from_slice(&year.to_be_bytes());
        key.push(month);
        key.push(day);
        key.push(metric);
        key
    }

    fn daily_metric_prefix() -> Vec<u8> {
        vec![PREFIX_DAILY_METRICS]
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

    // Counter operations

    async fn get_counter(&self, name: &[u8]) -> Result<u64> {
        let key = Self::counter_key(name);
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
                Ok(u64::from_be_bytes(bytes))
            }
            None => Ok(0),
        }
    }

    pub async fn get_total_txs(&self) -> Result<u64> {
        self.get_counter(COUNTER_TOTAL_TXS).await
    }

    pub async fn get_total_addresses(&self) -> Result<u64> {
        self.get_counter(COUNTER_TOTAL_ADDRESSES).await
    }

    pub async fn get_total_transfers(&self) -> Result<u64> {
        self.get_counter(COUNTER_TOTAL_TRANSFERS).await
    }

    pub async fn get_address_tx_count(&self, address: &Address) -> Result<u64> {
        let key = Self::address_counter_key(address, ADDR_COUNTER_TX);
        let result = self
            .db
            .run(|trx, _| {
                let key = key.clone();
                async move { Ok(trx.get(&key, false).await?) }
            })
            .await?;

        match result {
            Some(value) if value.len() == 8 => {
                let bytes: [u8; 8] = value.as_ref().try_into()?;
                Ok(i64::from_le_bytes(bytes) as u64)
            }
            _ => Ok(0),
        }
    }

    pub async fn get_address_token_transfer_count(&self, address: &Address) -> Result<u64> {
        let key = Self::address_counter_key(address, ADDR_COUNTER_TOKEN_TRANSFER);
        let result = self
            .db
            .run(|trx, _| {
                let key = key.clone();
                async move { Ok(trx.get(&key, false).await?) }
            })
            .await?;

        match result {
            Some(value) if value.len() == 8 => {
                let bytes: [u8; 8] = value.as_ref().try_into()?;
                Ok(i64::from_le_bytes(bytes) as u64)
            }
            _ => Ok(0),
        }
    }

    pub async fn get_token_holder_count(&self, token: &Address) -> Result<u64> {
        let key = Self::token_holder_count_key(token);
        let result = self
            .db
            .run(|trx, _| {
                let key = key.clone();
                async move { Ok(trx.get(&key, false).await?) }
            })
            .await?;

        match result {
            Some(value) if value.len() == 8 => {
                let bytes: [u8; 8] = value.as_ref().try_into()?;
                Ok(i64::from_le_bytes(bytes) as u64)
            }
            _ => Ok(0),
        }
    }

    pub async fn get_daily_tx_metrics(&self, days: usize) -> Result<Vec<(String, u64)>> {
        let prefix = Self::daily_metric_prefix();
        let mut end_key = prefix.clone();
        end_key.push(0xFF);

        let range = RangeOption {
            limit: Some(days * 2), // We store txs and transfers separately
            reverse: true,
            ..RangeOption::from(prefix.as_slice()..end_key.as_slice())
        };

        let results = self
            .db
            .run(|trx, _| {
                let range = range.clone();
                async move {
                    let kvs = trx.get_range(&range, 1, false).await?;
                    Ok(kvs.into_iter().collect::<Vec<_>>())
                }
            })
            .await?;

        let mut metrics: Vec<(String, u64)> = Vec::new();
        for kv in results {
            let key = kv.key();
            let value = kv.value();
            // Key format: prefix(1) + year(2) + month(1) + day(1) + metric(1)
            if key.len() == 6 && value.len() == 8 {
                let metric_type = key[5];
                if metric_type == METRIC_TXS {
                    let year = u16::from_be_bytes([key[1], key[2]]);
                    let month = key[3];
                    let day = key[4];
                    let count = i64::from_le_bytes(value.try_into().unwrap()) as u64;
                    let date = format!("{:04}-{:02}-{:02}", year, month, day);
                    metrics.push((date, count));
                }
            }
        }
        metrics.truncate(days);
        Ok(metrics)
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

    /// Count address transactions (capped at max_count for performance)
    pub async fn count_address_txs(&self, address: &Address, max_count: usize) -> Result<usize> {
        let prefix = Self::address_tx_prefix(address);
        let mut end_key = prefix.clone();
        end_key.push(0xFF);

        let range = RangeOption {
            limit: Some(max_count),
            ..RangeOption::from(prefix.as_slice()..end_key.as_slice())
        };

        let count = self
            .db
            .run(|trx, _| {
                let range = range.clone();
                async move {
                    let kvs = trx.get_range(&range, 0, false).await?;
                    Ok(kvs.len())
                }
            })
            .await?;

        Ok(count)
    }

    /// Count address token transfers (capped at max_count for performance)
    pub async fn count_address_transfers(&self, address: &Address, max_count: usize) -> Result<usize> {
        let prefix = Self::address_transfer_prefix(address);
        let mut end_key = prefix.clone();
        end_key.push(0xFF);

        let range = RangeOption {
            limit: Some(max_count),
            ..RangeOption::from(prefix.as_slice()..end_key.as_slice())
        };

        let count = self
            .db
            .run(|trx, _| {
                let range = range.clone();
                async move {
                    let kvs = trx.get_range(&range, 0, false).await?;
                    Ok(kvs.len())
                }
            })
            .await?;

        Ok(count)
    }

    /// Count token holders (capped at max_count for performance)
    pub async fn count_token_holders(&self, token: &Address, max_count: usize) -> Result<usize> {
        let prefix = Self::token_holder_prefix(token);
        let mut end_key = prefix.clone();
        end_key.push(0xFF);

        let range = RangeOption {
            limit: Some(max_count),
            ..RangeOption::from(prefix.as_slice()..end_key.as_slice())
        };

        let count = self
            .db
            .run(|trx, _| {
                let range = range.clone();
                async move {
                    let kvs = trx.get_range(&range, 0, false).await?;
                    Ok(kvs.len())
                }
            })
            .await?;

        Ok(count)
    }

    /// Get token transfers for a specific token (sorted by block desc)
    pub async fn get_token_transfers(
        &self,
        token: &Address,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<TokenTransfer>> {
        let prefix = Self::token_transfer_prefix(token);
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

    /// Count token transfers (capped at max_count for performance)
    pub async fn count_token_transfers(&self, token: &Address, max_count: usize) -> Result<usize> {
        let prefix = Self::token_transfer_prefix(token);
        let mut end_key = prefix.clone();
        end_key.push(0xFF);

        let range = RangeOption {
            limit: Some(max_count),
            ..RangeOption::from(prefix.as_slice()..end_key.as_slice())
        };

        let count = self
            .db
            .run(|trx, _| {
                let range = range.clone();
                async move {
                    let kvs = trx.get_range(&range, 0, false).await?;
                    Ok(kvs.len())
                }
            })
            .await?;

        Ok(count)
    }

    // Write batch for bulk operations
    pub fn write_batch(&self) -> WriteBatch<'_> {
        WriteBatch {
            db: &self.db,
            address_txs: Vec::new(),
            tx_blocks: Vec::new(),
            transfers: Vec::new(),
            holder_updates: Vec::new(),
            daily_tx_counts: std::collections::HashMap::new(),
            daily_transfer_counts: std::collections::HashMap::new(),
            address_tx_increments: std::collections::HashMap::new(),
            address_transfer_increments: std::collections::HashMap::new(),
        }
    }
}

pub struct WriteBatch<'a> {
    db: &'a Database,
    address_txs: Vec<(Address, TxHash, u64, u32)>, // (address, tx_hash, block, tx_idx)
    tx_blocks: Vec<(TxHash, u64)>,
    transfers: Vec<TokenTransfer>,
    holder_updates: Vec<(Address, Address, U256, Option<U256>)>, // (token, holder, new_balance, old_balance)
    daily_tx_counts: std::collections::HashMap<(u16, u8, u8), i64>, // (year, month, day) -> count
    daily_transfer_counts: std::collections::HashMap<(u16, u8, u8), i64>,
    address_tx_increments: std::collections::HashMap<Address, i64>,
    address_transfer_increments: std::collections::HashMap<Address, i64>,
}

impl<'a> WriteBatch<'a> {
    pub fn insert_address_tx(&mut self, address: Address, tx_hash: TxHash, block: u64, tx_idx: u32) {
        self.address_txs.push((address, tx_hash, block, tx_idx));
        *self.address_tx_increments.entry(address).or_insert(0) += 1;
    }

    pub fn insert_tx_block(&mut self, tx_hash: TxHash, block_number: u64) {
        self.tx_blocks.push((tx_hash, block_number));
    }

    pub fn insert_transfer(&mut self, transfer: TokenTransfer) {
        let from_addr = Address::from(transfer.from);
        let to_addr = Address::from(transfer.to);
        *self.address_transfer_increments.entry(from_addr).or_insert(0) += 1;
        if from_addr != to_addr {
            *self.address_transfer_increments.entry(to_addr).or_insert(0) += 1;
        }
        self.transfers.push(transfer);
    }

    pub fn update_holder_balance(&mut self, token: Address, holder: Address, new_balance: U256, old_balance: Option<U256>) {
        self.holder_updates.push((token, holder, new_balance, old_balance));
    }

    pub fn record_block_timestamp(&mut self, timestamp: u64, tx_count: i64, transfer_count: i64) {
        let dt = chrono::DateTime::from_timestamp(timestamp as i64, 0)
            .unwrap_or_else(|| chrono::DateTime::from_timestamp(0, 0).unwrap());
        let year = dt.format("%Y").to_string().parse::<u16>().unwrap_or(2024);
        let month = dt.format("%m").to_string().parse::<u8>().unwrap_or(1);
        let day = dt.format("%d").to_string().parse::<u8>().unwrap_or(1);
        let key = (year, month, day);
        *self.daily_tx_counts.entry(key).or_insert(0) += tx_count;
        *self.daily_transfer_counts.entry(key).or_insert(0) += transfer_count;
    }

    pub async fn commit(self, last_block: u64) -> Result<()> {
        let address_txs = self.address_txs;
        let tx_blocks = self.tx_blocks;
        let transfers = self.transfers;
        let holder_updates = self.holder_updates;
        let daily_tx_counts: Vec<_> = self.daily_tx_counts.into_iter().collect();
        let daily_transfer_counts: Vec<_> = self.daily_transfer_counts.into_iter().collect();
        let address_tx_increments: Vec<_> = self.address_tx_increments.into_iter().collect();
        let address_transfer_increments: Vec<_> = self.address_transfer_increments.into_iter().collect();

        self.db
            .run(|trx, _| {
                let address_txs = address_txs.clone();
                let tx_blocks = tx_blocks.clone();
                let transfers = transfers.clone();
                let holder_updates = holder_updates.clone();
                let daily_tx_counts = daily_tx_counts.clone();
                let daily_transfer_counts = daily_transfer_counts.clone();
                let address_tx_increments = address_tx_increments.clone();
                let address_transfer_increments = address_transfer_increments.clone();

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

                    // Write transfers (indexed by from, to, and token address)
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

                        // Index by token address
                        let token_addr = Address::from(transfer.token_address);
                        let token_key = FdbIndex::token_transfer_key(
                            &token_addr,
                            transfer.block_number,
                            transfer.log_index,
                        );
                        trx.set(&token_key, &value);
                    }

                    // Write holder balance updates and track holder count changes
                    let mut holder_count_deltas: std::collections::HashMap<Address, i64> = std::collections::HashMap::new();
                    for (token, holder, new_balance, old_balance) in &holder_updates {
                        let key = FdbIndex::token_holder_key(token, holder);
                        trx.set(&key, &new_balance.to_be_bytes::<32>());
                        
                        // Track zero transitions for holder counts
                        let was_zero = old_balance.map(|b| b.is_zero()).unwrap_or(true);
                        let is_zero = new_balance.is_zero();
                        
                        if was_zero && !is_zero {
                            // 0 → >0: new holder
                            *holder_count_deltas.entry(*token).or_insert(0) += 1;
                        } else if !was_zero && is_zero {
                            // >0 → 0: holder left
                            *holder_count_deltas.entry(*token).or_insert(0) -= 1;
                        }
                    }
                    
                    // Apply holder count deltas
                    for (token, delta) in &holder_count_deltas {
                        if *delta != 0 {
                            let key = FdbIndex::token_holder_count_key(token);
                            trx.atomic_op(&key, &delta.to_le_bytes(), MutationType::Add);
                        }
                    }

                    // Update last indexed block
                    let meta_key = FdbIndex::meta_key(META_LAST_BLOCK);
                    trx.set(&meta_key, &last_block.to_be_bytes());

                    // Atomically increment global counters
                    let tx_count = tx_blocks.len() as i64;
                    let transfer_count = transfers.len() as i64;

                    if tx_count > 0 {
                        let key = FdbIndex::counter_key(COUNTER_TOTAL_TXS);
                        trx.atomic_op(&key, &tx_count.to_le_bytes(), MutationType::Add);
                    }

                    if transfer_count > 0 {
                        let key = FdbIndex::counter_key(COUNTER_TOTAL_TRANSFERS);
                        trx.atomic_op(&key, &transfer_count.to_le_bytes(), MutationType::Add);
                    }

                    // Atomically increment per-address tx counters
                    for (addr, count) in &address_tx_increments {
                        let key = FdbIndex::address_counter_key(addr, ADDR_COUNTER_TX);
                        trx.atomic_op(&key, &count.to_le_bytes(), MutationType::Add);
                    }

                    // Atomically increment per-address token transfer counters
                    for (addr, count) in &address_transfer_increments {
                        let key = FdbIndex::address_counter_key(addr, ADDR_COUNTER_TOKEN_TRANSFER);
                        trx.atomic_op(&key, &count.to_le_bytes(), MutationType::Add);
                    }

                    // Atomically increment daily tx metrics
                    for ((year, month, day), count) in &daily_tx_counts {
                        let key = FdbIndex::daily_metric_key(*year, *month, *day, METRIC_TXS);
                        trx.atomic_op(&key, &count.to_le_bytes(), MutationType::Add);
                    }

                    // Atomically increment daily transfer metrics
                    for ((year, month, day), count) in &daily_transfer_counts {
                        let key = FdbIndex::daily_metric_key(*year, *month, *day, METRIC_TRANSFERS);
                        trx.atomic_op(&key, &count.to_le_bytes(), MutationType::Add);
                    }

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
