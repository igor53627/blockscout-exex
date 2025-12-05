//! MDBX-based index storage for Blockscout data.
//!
//! This module provides MDBX (libmdbx) database implementation as a replacement
//! for FoundationDB, using reth's database abstractions for compatibility.
//!
//! Schema matches FDB implementation for data compatibility:
//! - AddressTxs: (address:20, block:8, tx_idx:4) -> tx_hash:32
//! - AddressTransfers: (address:20, block:8, log_idx:8) -> TokenTransfer
//! - TokenHolders: (token:20, holder:20) -> balance:32
//! - TxBlocks: tx_hash:32 -> block_number:8
//! - TokenTransfers: (token:20, block:8, log_idx:8) -> TokenTransfer
//! - Metadata: key -> value (version, last_block, etc.)
//! - Counters: name -> i64_le
//! - AddressCounters: (address:20, kind:1) -> i64_le
//! - TokenHolderCounts: token:20 -> i64_le
//! - DailyMetrics: (year:2, month:1, day:1, metric:1) -> i64_le

#[cfg(feature = "reth")]
use reth_db::mdbx::{DatabaseEnv, DatabaseEnvKind, DatabaseArguments};
#[cfg(feature = "reth")]
use reth_db_api::{database::Database, models::ClientVersion, transaction::DbTx};
#[cfg(feature = "reth")]
use reth_libmdbx::{WriteFlags, DatabaseFlags};

// Table name constants for our custom index schema
#[cfg(feature = "reth")]
mod tables {
    pub const ADDRESS_TXS: &str = "AddressTxs";
    pub const TX_BLOCKS: &str = "TxBlocks";
    pub const ADDRESS_TRANSFERS: &str = "AddressTransfers";
    pub const TOKEN_TRANSFERS: &str = "TokenTransfers";
    pub const TOKEN_HOLDERS: &str = "TokenHolders";
    pub const COUNTERS: &str = "Counters";
    pub const ADDRESS_COUNTERS: &str = "AddressCounters";
    pub const TOKEN_HOLDER_COUNTS: &str = "TokenHolderCounts";
    pub const DAILY_METRICS: &str = "DailyMetrics";
    pub const METADATA: &str = "Metadata";
}

use std::path::Path;
use std::sync::Arc;
use std::hash::Hash;
use eyre::Result;
use alloy_primitives::Address;

// Re-export TokenTransfer from index_trait for compatibility
pub use crate::index_trait::TokenTransfer;

use hyperloglogplus::HyperLogLogPlus;
use std::collections::hash_map::RandomState;

/// Type alias for HyperLogLog used for address counting
pub type AddressHll = HyperLogLogPlus<AddressWrapper, RandomState>;

/// Wrapper for Address to implement Hash trait for HyperLogLog
#[derive(Clone, Copy)]
pub struct AddressWrapper(pub Address);

impl Hash for AddressWrapper {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.as_slice().hash(state);
    }
}

const SCHEMA_VERSION: u32 = 1;
const META_SCHEMA_VERSION: &[u8] = b"schema_version";

// ============================================================================
// Composite Key Types with Encode/Decode (Subtask 1.2)
// ============================================================================

/// Key for address -> transaction mappings: (address, block, tx_idx)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddressTxKey {
    address: Address,
    block: u64,
    tx_idx: u32,
}

/// Key for token -> transfer mappings: (token, block, log_idx)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenTransferKey {
    token: Address,
    block: u64,
    log_idx: u64,
}

/// Key for address counter mappings: (address, kind)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AddressCounterKey {
    address: Address,
    kind: u8,
}

/// Key for daily metrics: (year, month, day, metric)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DailyMetricKey {
    year: u16,
    month: u8,
    day: u8,
    metric: u8,
}

impl AddressTxKey {
    pub fn new(address: Address, block: u64, tx_idx: u32) -> Self {
        Self {
            address,
            block,
            tx_idx,
        }
    }

    pub fn address(&self) -> Address {
        self.address
    }

    pub fn block(&self) -> u64 {
        self.block
    }

    pub fn tx_idx(&self) -> u32 {
        self.tx_idx
    }

    /// Encode key to bytes (big-endian for lexicographic ordering)
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(20 + 8 + 4);
        bytes.extend_from_slice(self.address.as_slice());
        bytes.extend_from_slice(&self.block.to_be_bytes());
        bytes.extend_from_slice(&self.tx_idx.to_be_bytes());
        bytes
    }

    /// Decode key from bytes
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != 32 {
            return Err(eyre::eyre!("Invalid AddressTxKey length: {}", bytes.len()));
        }
        let address = Address::from_slice(&bytes[0..20]);
        let block = u64::from_be_bytes(bytes[20..28].try_into()?);
        let tx_idx = u32::from_be_bytes(bytes[28..32].try_into()?);
        Ok(Self {
            address,
            block,
            tx_idx,
        })
    }
}

/// Key for address -> transfer mappings: (address, block, log_idx)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddressTransferKey {
    address: Address,
    block: u64,
    log_idx: u64,
}

impl AddressTransferKey {
    pub fn new(address: Address, block: u64, log_idx: u64) -> Self {
        Self {
            address,
            block,
            log_idx,
        }
    }

    pub fn address(&self) -> Address {
        self.address
    }

    pub fn block(&self) -> u64 {
        self.block
    }

    pub fn log_idx(&self) -> u64 {
        self.log_idx
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(20 + 8 + 8);
        bytes.extend_from_slice(self.address.as_slice());
        bytes.extend_from_slice(&self.block.to_be_bytes());
        bytes.extend_from_slice(&self.log_idx.to_be_bytes());
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != 36 {
            return Err(eyre::eyre!(
                "Invalid AddressTransferKey length: {}",
                bytes.len()
            ));
        }
        let address = Address::from_slice(&bytes[0..20]);
        let block = u64::from_be_bytes(bytes[20..28].try_into()?);
        let log_idx = u64::from_be_bytes(bytes[28..36].try_into()?);
        Ok(Self {
            address,
            block,
            log_idx,
        })
    }
}

/// Key for token holder mappings: (token, holder)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenHolderKey {
    token: Address,
    holder: Address,
}

impl TokenHolderKey {
    pub fn new(token: Address, holder: Address) -> Self {
        Self { token, holder }
    }

    pub fn token(&self) -> Address {
        self.token
    }

    pub fn holder(&self) -> Address {
        self.holder
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(40);
        bytes.extend_from_slice(self.token.as_slice());
        bytes.extend_from_slice(self.holder.as_slice());
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != 40 {
            return Err(eyre::eyre!(
                "Invalid TokenHolderKey length: {}",
                bytes.len()
            ));
        }
        let token = Address::from_slice(&bytes[0..20]);
        let holder = Address::from_slice(&bytes[20..40]);
        Ok(Self { token, holder })
    }
}

impl TokenTransferKey {
    pub fn new(token: Address, block: u64, log_idx: u64) -> Self {
        Self { token, block, log_idx }
    }

    pub fn token(&self) -> Address {
        self.token
    }

    pub fn block(&self) -> u64 {
        self.block
    }

    pub fn log_idx(&self) -> u64 {
        self.log_idx
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(20 + 8 + 8);
        bytes.extend_from_slice(self.token.as_slice());
        bytes.extend_from_slice(&self.block.to_be_bytes());
        bytes.extend_from_slice(&self.log_idx.to_be_bytes());
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != 36 {
            return Err(eyre::eyre!("Invalid TokenTransferKey length: {}", bytes.len()));
        }
        let token = Address::from_slice(&bytes[0..20]);
        let block = u64::from_be_bytes(bytes[20..28].try_into()?);
        let log_idx = u64::from_be_bytes(bytes[28..36].try_into()?);
        Ok(Self { token, block, log_idx })
    }
}

impl AddressCounterKey {
    pub fn new(address: Address, kind: u8) -> Self {
        Self { address, kind }
    }

    pub fn address(&self) -> Address {
        self.address
    }

    pub fn kind(&self) -> u8 {
        self.kind
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(21);
        bytes.extend_from_slice(self.address.as_slice());
        bytes.push(self.kind);
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != 21 {
            return Err(eyre::eyre!("Invalid AddressCounterKey length: {}", bytes.len()));
        }
        let address = Address::from_slice(&bytes[0..20]);
        let kind = bytes[20];
        Ok(Self { address, kind })
    }
}

impl DailyMetricKey {
    pub fn new(year: u16, month: u8, day: u8, metric: u8) -> Self {
        Self { year, month, day, metric }
    }

    pub fn year(&self) -> u16 {
        self.year
    }

    pub fn month(&self) -> u8 {
        self.month
    }

    pub fn day(&self) -> u8 {
        self.day
    }

    pub fn metric(&self) -> u8 {
        self.metric
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(5);
        bytes.extend_from_slice(&self.year.to_be_bytes());
        bytes.push(self.month);
        bytes.push(self.day);
        bytes.push(self.metric);
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != 5 {
            return Err(eyre::eyre!("Invalid DailyMetricKey length: {}", bytes.len()));
        }
        let year = u16::from_be_bytes([bytes[0], bytes[1]]);
        let month = bytes[2];
        let day = bytes[3];
        let metric = bytes[4];
        Ok(Self { year, month, day, metric })
    }
}

// ============================================================================
// MDBX Environment and Core Structures (Subtask 1.1)
// ============================================================================

/// MDBX-based index for Blockscout data
///
/// Configuration:
/// - Map size: 256GB initial (auto-grows as needed)
/// - Max readers: 256 concurrent read transactions
/// - SafeNoSync mode for better write performance
pub struct MdbxIndex {
    #[cfg(feature = "reth")]
    env: Arc<DatabaseEnv>,
    #[cfg(not(feature = "reth"))]
    _phantom: std::marker::PhantomData<()>,
}

impl MdbxIndex {
    /// Open or create an MDBX database at the specified path
    ///
    /// Configuration:
    /// - 256GB max map size with auto-grow
    /// - 256 max concurrent readers
    /// - SafeNoSync for performance
    #[cfg(feature = "reth")]
    pub fn open(path: &Path) -> Result<Self> {
        use reth_db::mdbx::SyncMode;

        const GIGABYTE: usize = 1024 * 1024 * 1024;

        // Create database arguments with custom geometry
        let args = DatabaseArguments::new(ClientVersion::default())
            .with_max_readers(Some(256))
            .with_geometry_max_size(Some(256 * GIGABYTE))
            .with_sync_mode(Some(SyncMode::SafeNoSync));

        let env = DatabaseEnv::open(path, DatabaseEnvKind::RW, args)?;

        Ok(Self {
            env: Arc::new(env),
        })
    }

    #[cfg(not(feature = "reth"))]
    pub fn open(_path: &Path) -> Result<Self> {
        Err(eyre::eyre!(
            "MDBX support requires 'reth' feature to be enabled"
        ))
    }

    /// Get schema version from metadata
    /// Returns 1 for new databases or stored version
    #[cfg(feature = "reth")]
    pub fn get_schema_version(&self) -> Result<u32> {
        use reth_db_api::table::Table;
        use reth_db_api::cursor::DbCursorRO;

        let tx = self.env.tx()?;

        // Use reth's Table trait to access metadata table
        // We'll store schema_version as a simple key-value pair
        // For simplicity, using a direct get approach
        // In a real implementation, we'd define proper table types

        // For now, return default version until we set it
        Ok(SCHEMA_VERSION)
    }

    #[cfg(not(feature = "reth"))]
    pub fn get_schema_version(&self) -> Result<u32> {
        Err(eyre::eyre!("MDBX support requires 'reth' feature"))
    }

    /// Set schema version in metadata
    #[cfg(feature = "reth")]
    pub fn set_schema_version(&self, version: u32) -> Result<()> {
        let tx = self.env.tx_mut()?;

        // Store version in metadata
        // For now, just commit the transaction
        // In production, we'd write to a proper metadata table

        tx.commit()?;
        Ok(())
    }

    #[cfg(not(feature = "reth"))]
    pub fn set_schema_version(&self, _version: u32) -> Result<()> {
        Err(eyre::eyre!("MDBX support requires 'reth' feature"))
    }

    /// Get last indexed block from metadata
    #[cfg(feature = "reth")]
    pub fn get_last_indexed_block(&self) -> Result<Option<u64>> {
        // For now, return None until we implement proper metadata storage
        Ok(None)
    }

    #[cfg(not(feature = "reth"))]
    pub fn get_last_indexed_block(&self) -> Result<Option<u64>> {
        Err(eyre::eyre!("MDBX support requires 'reth' feature"))
    }

    /// Set last indexed block in metadata
    #[cfg(feature = "reth")]
    pub fn set_last_indexed_block(&self, _block: u64) -> Result<()> {
        let tx = self.env.tx_mut()?;
        tx.commit()?;
        Ok(())
    }

    #[cfg(not(feature = "reth"))]
    pub fn set_last_indexed_block(&self, _block: u64) -> Result<()> {
        Err(eyre::eyre!("MDBX support requires 'reth' feature"))
    }

    /// Begin a read-only transaction
    #[cfg(feature = "reth")]
    pub fn begin_read(&self) -> Result<MdbxReadTransaction> {
        let tx = self.env.tx()?;
        Ok(MdbxReadTransaction { _tx: tx })
    }

    #[cfg(not(feature = "reth"))]
    pub fn begin_read(&self) -> Result<MdbxReadTransaction> {
        Err(eyre::eyre!("MDBX support requires 'reth' feature"))
    }

    /// Begin a read-write transaction
    #[cfg(feature = "reth")]
    pub fn begin_write(&self) -> Result<MdbxWriteTransaction> {
        let tx = self.env.tx_mut()?;
        Ok(MdbxWriteTransaction { _tx: tx })
    }

    #[cfg(not(feature = "reth"))]
    pub fn begin_write(&self) -> Result<MdbxWriteTransaction> {
        Err(eyre::eyre!("MDBX support requires 'reth' feature"))
    }

    // ============================================================================
    // Read Operations (Subtask 1.4)
    // ============================================================================

    /// Get address transactions with pagination
    #[cfg(feature = "reth")]
    pub fn get_address_txs(
        &self,
        _address: &Address,
        _limit: usize,
        _offset: usize,
    ) -> Result<Vec<alloy_primitives::TxHash>> {
        // For now, return empty vec until we implement proper table access
        Ok(Vec::new())
    }

    #[cfg(not(feature = "reth"))]
    pub fn get_address_txs(
        &self,
        _address: &Address,
        _limit: usize,
        _offset: usize,
    ) -> Result<Vec<alloy_primitives::TxHash>> {
        Err(eyre::eyre!("MDBX support requires 'reth' feature"))
    }

    /// Get address transfers with pagination
    #[cfg(feature = "reth")]
    pub fn get_address_transfers(
        &self,
        _address: &Address,
        _limit: usize,
        _offset: usize,
    ) -> Result<Vec<TokenTransfer>> {
        Ok(Vec::new())
    }

    #[cfg(not(feature = "reth"))]
    pub fn get_address_transfers(
        &self,
        _address: &Address,
        _limit: usize,
        _offset: usize,
    ) -> Result<Vec<TokenTransfer>> {
        Err(eyre::eyre!("MDBX support requires 'reth' feature"))
    }

    /// Get token holders with pagination
    #[cfg(feature = "reth")]
    pub fn get_token_holders(
        &self,
        _token: &Address,
        _limit: usize,
        _offset: usize,
    ) -> Result<Vec<(Address, alloy_primitives::U256)>> {
        Ok(Vec::new())
    }

    #[cfg(not(feature = "reth"))]
    pub fn get_token_holders(
        &self,
        _token: &Address,
        _limit: usize,
        _offset: usize,
    ) -> Result<Vec<(Address, alloy_primitives::U256)>> {
        Err(eyre::eyre!("MDBX support requires 'reth' feature"))
    }

    /// Get token transfers with pagination
    #[cfg(feature = "reth")]
    pub fn get_token_transfers(
        &self,
        _token: &Address,
        _limit: usize,
        _offset: usize,
    ) -> Result<Vec<TokenTransfer>> {
        Ok(Vec::new())
    }

    #[cfg(not(feature = "reth"))]
    pub fn get_token_transfers(
        &self,
        _token: &Address,
        _limit: usize,
        _offset: usize,
    ) -> Result<Vec<TokenTransfer>> {
        Err(eyre::eyre!("MDBX support requires 'reth' feature"))
    }

    /// Get block number for a transaction hash
    #[cfg(feature = "reth")]
    pub fn get_tx_block(&self, _tx_hash: &alloy_primitives::TxHash) -> Result<Option<u64>> {
        Ok(None)
    }

    #[cfg(not(feature = "reth"))]
    pub fn get_tx_block(&self, _tx_hash: &alloy_primitives::TxHash) -> Result<Option<u64>> {
        Err(eyre::eyre!("MDBX support requires 'reth' feature"))
    }

    /// Get total transaction count
    #[cfg(feature = "reth")]
    pub fn get_total_txs(&self) -> Result<u64> {
        Ok(0)
    }

    #[cfg(not(feature = "reth"))]
    pub fn get_total_txs(&self) -> Result<u64> {
        Err(eyre::eyre!("MDBX support requires 'reth' feature"))
    }

    /// Get total transfer count
    #[cfg(feature = "reth")]
    pub fn get_total_transfers(&self) -> Result<u64> {
        Ok(0)
    }

    #[cfg(not(feature = "reth"))]
    pub fn get_total_transfers(&self) -> Result<u64> {
        Err(eyre::eyre!("MDBX support requires 'reth' feature"))
    }

    /// Get total unique address count
    #[cfg(feature = "reth")]
    pub fn get_total_addresses(&self) -> Result<u64> {
        Ok(0)
    }

    #[cfg(not(feature = "reth"))]
    pub fn get_total_addresses(&self) -> Result<u64> {
        Err(eyre::eyre!("MDBX support requires 'reth' feature"))
    }

    // ============================================================================
    // Write Operations (Subtask 1.5)
    // ============================================================================

    /// Create a new write batch
    #[cfg(feature = "reth")]
    pub fn write_batch(&self) -> MdbxWriteBatch {
        MdbxWriteBatch {
            env: Arc::clone(&self.env),
            address_txs: Vec::new(),
            tx_blocks: Vec::new(),
            transfers: Vec::new(),
            holder_updates: Vec::new(),
            counter_deltas: std::collections::HashMap::new(),
            address_counter_deltas: std::collections::HashMap::new(),
            daily_metrics: Vec::new(),
        }
    }

    #[cfg(not(feature = "reth"))]
    pub fn write_batch(&self) -> MdbxWriteBatch {
        MdbxWriteBatch {
            _phantom: std::marker::PhantomData,
        }
    }

    /// Get counter value
    #[cfg(feature = "reth")]
    pub fn get_counter(&self, _name: &[u8]) -> Result<i64> {
        // Placeholder implementation
        Ok(0)
    }

    #[cfg(not(feature = "reth"))]
    pub fn get_counter(&self, _name: &[u8]) -> Result<i64> {
        Err(eyre::eyre!("MDBX support requires 'reth' feature"))
    }

    /// Get address counter
    #[cfg(feature = "reth")]
    pub fn get_address_counter(&self, _address: &Address, _kind: u8) -> Result<i64> {
        // Placeholder implementation
        Ok(0)
    }

    #[cfg(not(feature = "reth"))]
    pub fn get_address_counter(&self, _address: &Address, _kind: u8) -> Result<i64> {
        Err(eyre::eyre!("MDBX support requires 'reth' feature"))
    }

    /// Get token holder count
    #[cfg(feature = "reth")]
    pub fn get_token_holder_count(&self, _token: &Address) -> Result<i64> {
        // Placeholder implementation
        Ok(0)
    }

    #[cfg(not(feature = "reth"))]
    pub fn get_token_holder_count(&self, _token: &Address) -> Result<i64> {
        Err(eyre::eyre!("MDBX support requires 'reth' feature"))
    }

    /// Get daily metric
    #[cfg(feature = "reth")]
    pub fn get_daily_metric(&self, _year: u16, _month: u8, _day: u8, _metric: u8) -> Result<i64> {
        // Placeholder implementation
        Ok(0)
    }

    #[cfg(not(feature = "reth"))]
    pub fn get_daily_metric(&self, _year: u16, _month: u8, _day: u8, _metric: u8) -> Result<i64> {
        Err(eyre::eyre!("MDBX support requires 'reth' feature"))
    }

    // ============================================================================
    // HyperLogLog Support (for address counting)
    // ============================================================================

    /// Create a new empty HyperLogLog for address counting
    /// Uses same precision as FDB (14 bits = ~16K registers)
    pub fn new_address_hll() -> AddressHll {
        use hyperloglogplus::HyperLogLogPlus;
        use std::collections::hash_map::RandomState;
        const HLL_PRECISION: u8 = 14;
        HyperLogLogPlus::new(HLL_PRECISION, RandomState::new()).unwrap()
    }

    /// Load the address HLL count from MDBX (we store just the count, not full HLL)
    #[cfg(feature = "reth")]
    pub fn load_address_hll_count(&self) -> Result<u64> {
        // For now, return 0 - will be stored in metadata table
        Ok(0)
    }

    #[cfg(not(feature = "reth"))]
    pub fn load_address_hll_count(&self) -> Result<u64> {
        Err(eyre::eyre!("MDBX support requires 'reth' feature"))
    }

    /// Save the address HLL count to MDBX
    #[cfg(feature = "reth")]
    pub fn save_address_hll_count(&self, _count: u64) -> Result<()> {
        // For now, no-op - will store in metadata table
        Ok(())
    }

    #[cfg(not(feature = "reth"))]
    pub fn save_address_hll_count(&self, _count: u64) -> Result<()> {
        Err(eyre::eyre!("MDBX support requires 'reth' feature"))
    }
}

// Transaction wrapper types
pub struct MdbxReadTransaction {
    #[cfg(feature = "reth")]
    _tx: <DatabaseEnv as Database>::TX,
    #[cfg(not(feature = "reth"))]
    _phantom: std::marker::PhantomData<()>,
}

pub struct MdbxWriteTransaction {
    #[cfg(feature = "reth")]
    _tx: <DatabaseEnv as Database>::TXMut,
    #[cfg(not(feature = "reth"))]
    _phantom: std::marker::PhantomData<()>,
}

// ============================================================================
// Table Type Markers (Subtask 1.2)
// ============================================================================

/// Marker type for MDBX tables
pub struct MdbxTable<T> {
    _phantom: std::marker::PhantomData<T>,
}

/// Table: AddressTxs - (address:20, block:8, tx_idx:4) -> tx_hash:32
pub struct AddressTxs;

/// Table: AddressTransfers - (address:20, block:8, log_idx:8) -> TokenTransfer
pub struct AddressTransfers;

/// Table: TokenHolders - (token:20, holder:20) -> balance:32
pub struct TokenHolders;

/// Table: TxBlocks - tx_hash:32 -> block_number:8
pub struct TxBlocks;

/// Table: TokenTransfers - (token:20, block:8, log_idx:8) -> TokenTransfer
pub struct TokenTransfers;

/// Table: Metadata - key -> value
pub struct Metadata;

/// Table: Counters - name -> i64_le
pub struct Counters;

/// Table: AddressCounters - (address:20, kind:1) -> i64_le
pub struct AddressCounters;

/// Table: TokenHolderCounts - token:20 -> i64_le
pub struct TokenHolderCounts;

/// Table: DailyMetrics - (year:2, month:1, day:1, metric:1) -> i64_le
pub struct DailyMetrics;

// ============================================================================
// Write Batch Implementation (Subtask 1.5)
// ============================================================================

/// Batch writer for atomic write operations
pub struct MdbxWriteBatch {
    #[cfg(feature = "reth")]
    env: Arc<DatabaseEnv>,
    #[cfg(feature = "reth")]
    address_txs: Vec<(AddressTxKey, alloy_primitives::TxHash)>,
    #[cfg(feature = "reth")]
    tx_blocks: Vec<(alloy_primitives::TxHash, u64)>,
    #[cfg(feature = "reth")]
    transfers: Vec<TokenTransfer>,
    #[cfg(feature = "reth")]
    holder_updates: Vec<(TokenHolderKey, alloy_primitives::U256, Option<alloy_primitives::U256>)>,
    #[cfg(feature = "reth")]
    counter_deltas: std::collections::HashMap<Vec<u8>, i64>,
    #[cfg(feature = "reth")]
    address_counter_deltas: std::collections::HashMap<AddressCounterKey, i64>,
    #[cfg(feature = "reth")]
    daily_metrics: Vec<(DailyMetricKey, i64)>,
    #[cfg(not(feature = "reth"))]
    _phantom: std::marker::PhantomData<()>,
}

#[cfg(feature = "reth")]
impl MdbxWriteBatch {
    /// Insert an address transaction mapping
    pub fn insert_address_tx(
        &mut self,
        address: Address,
        tx_hash: alloy_primitives::TxHash,
        block: u64,
        tx_idx: u32,
    ) {
        let key = AddressTxKey::new(address, block, tx_idx);
        self.address_txs.push((key, tx_hash));
    }

    /// Insert a transaction -> block mapping
    pub fn insert_tx_block(&mut self, tx_hash: alloy_primitives::TxHash, block: u64) {
        self.tx_blocks.push((tx_hash, block));
    }

    /// Insert a token transfer
    pub fn insert_transfer(&mut self, transfer: TokenTransfer) {
        self.transfers.push(transfer);
    }

    /// Update a holder's balance
    pub fn update_holder_balance(
        &mut self,
        token: Address,
        holder: Address,
        new_balance: alloy_primitives::U256,
        old_balance: Option<alloy_primitives::U256>,
    ) {
        let key = TokenHolderKey::new(token, holder);
        self.holder_updates.push((key, new_balance, old_balance));
    }

    /// Increment a counter
    pub fn increment_counter(&mut self, name: &[u8], delta: i64) {
        *self.counter_deltas.entry(name.to_vec()).or_insert(0) += delta;
    }

    /// Increment an address-specific counter
    pub fn increment_address_counter(&mut self, address: Address, kind: u8, delta: i64) {
        let key = AddressCounterKey::new(address, kind);
        *self.address_counter_deltas.entry(key).or_insert(0) += delta;
    }

    /// Record a daily metric
    pub fn record_daily_metric(&mut self, year: u16, month: u8, day: u8, metric: u8, value: i64) {
        let key = DailyMetricKey::new(year, month, day, metric);
        self.daily_metrics.push((key, value));
    }

    /// Record metrics for a block timestamp (extracts date and increments daily counters)
    pub fn record_block_timestamp(&mut self, timestamp: u64, tx_count: i64, transfer_count: i64) {
        let dt = chrono::DateTime::from_timestamp(timestamp as i64, 0)
            .unwrap_or_else(|| chrono::DateTime::from_timestamp(0, 0).unwrap());
        let year = dt.format("%Y").to_string().parse::<u16>().unwrap_or(2024);
        let month = dt.format("%m").to_string().parse::<u8>().unwrap_or(1);
        let day = dt.format("%d").to_string().parse::<u8>().unwrap_or(1);
        // Metric type 0 = TX count, 1 = transfer count
        self.record_daily_metric(year, month, day, 0, tx_count);
        self.record_daily_metric(year, month, day, 1, transfer_count);
    }

    /// Collect all unique addresses from this batch for HLL insertion
    pub fn collect_addresses(&self) -> impl Iterator<Item = Address> + '_ {
        // Addresses from transactions
        let tx_addrs = self.address_txs.iter().map(|(key, _)| key.address());

        // Addresses from transfers (from + to)
        let transfer_addrs = self.transfers.iter().flat_map(|t| {
            [Address::from(t.from), Address::from(t.to)]
        });

        tx_addrs.chain(transfer_addrs)
    }

    /// Check if batch is getting large (for memory efficiency, not hard limit like FDB)
    pub fn is_large(&self) -> bool {
        // MDBX doesn't have FDB's 10MB transaction limit, but we still want to
        // commit periodically for memory efficiency and progress tracking
        let total_entries = self.address_txs.len() + self.tx_blocks.len() + self.transfers.len();
        total_entries > 50_000 // More generous than FDB since no hard limit
    }

    /// Commit the batch atomically
    pub fn commit(self, last_block: u64) -> Result<()> {
        use reth_db_api::transaction::DbTxMut;

        // Get raw mutable transaction
        let tx = self.env.tx_mut()?;

        // Open or create all our tables, getting the DBI handles
        let addr_txs_dbi = tx.inner.create_db(Some(tables::ADDRESS_TXS), DatabaseFlags::default())?.dbi();
        let tx_blocks_dbi = tx.inner.create_db(Some(tables::TX_BLOCKS), DatabaseFlags::default())?.dbi();
        let addr_transfers_dbi = tx.inner.create_db(Some(tables::ADDRESS_TRANSFERS), DatabaseFlags::default())?.dbi();
        let token_transfers_dbi = tx.inner.create_db(Some(tables::TOKEN_TRANSFERS), DatabaseFlags::default())?.dbi();
        let counters_dbi = tx.inner.create_db(Some(tables::COUNTERS), DatabaseFlags::default())?.dbi();
        let addr_counters_dbi = tx.inner.create_db(Some(tables::ADDRESS_COUNTERS), DatabaseFlags::default())?.dbi();
        let daily_metrics_dbi = tx.inner.create_db(Some(tables::DAILY_METRICS), DatabaseFlags::default())?.dbi();
        let metadata_dbi = tx.inner.create_db(Some(tables::METADATA), DatabaseFlags::default())?.dbi();

        // Write address -> tx mappings
        for (key, tx_hash) in &self.address_txs {
            tx.inner.put(addr_txs_dbi, key.encode(), tx_hash.as_slice(), WriteFlags::UPSERT)?;
        }

        // Write tx -> block mappings
        for (tx_hash, block) in &self.tx_blocks {
            tx.inner.put(tx_blocks_dbi, tx_hash.as_slice(), &block.to_be_bytes(), WriteFlags::UPSERT)?;
        }

        // Write transfers - indexed by both address and token
        for transfer in &self.transfers {
            // Serialize transfer
            let transfer_bytes = bincode::serialize(transfer)
                .map_err(|e| eyre::eyre!("Failed to serialize transfer: {}", e))?;

            // Index by from address
            let from_key = AddressTransferKey::new(
                Address::from(transfer.from),
                transfer.block_number,
                transfer.log_index,
            );
            tx.inner.put(addr_transfers_dbi, from_key.encode(), &transfer_bytes, WriteFlags::UPSERT)?;

            // Index by to address (if different)
            if transfer.from != transfer.to {
                let to_key = AddressTransferKey::new(
                    Address::from(transfer.to),
                    transfer.block_number,
                    transfer.log_index,
                );
                tx.inner.put(addr_transfers_dbi, to_key.encode(), &transfer_bytes, WriteFlags::UPSERT)?;
            }

            // Index by token address
            let token_key = TokenTransferKey::new(
                Address::from(transfer.token_address),
                transfer.block_number,
                transfer.log_index,
            );
            tx.inner.put(token_transfers_dbi, token_key.encode(), &transfer_bytes, WriteFlags::UPSERT)?;
        }

        // Update counters
        for (name, delta) in &self.counter_deltas {
            // Read current value, add delta, write back
            let current: i64 = match tx.inner.get::<Vec<u8>>(counters_dbi, name.as_slice()) {
                Ok(Some(bytes)) if bytes.len() >= 8 => {
                    i64::from_le_bytes(bytes[..8].try_into().unwrap_or([0u8; 8]))
                }
                _ => 0,
            };
            let new_value = current + delta;
            tx.inner.put(counters_dbi, name.as_slice(), &new_value.to_le_bytes(), WriteFlags::UPSERT)?;
        }

        // Update address-specific counters
        for (key, delta) in &self.address_counter_deltas {
            let key_bytes = key.encode();
            let current: i64 = match tx.inner.get::<Vec<u8>>(addr_counters_dbi, key_bytes.as_slice()) {
                Ok(Some(bytes)) if bytes.len() >= 8 => {
                    i64::from_le_bytes(bytes[..8].try_into().unwrap_or([0u8; 8]))
                }
                _ => 0,
            };
            let new_value = current + delta;
            tx.inner.put(addr_counters_dbi, key_bytes.as_slice(), &new_value.to_le_bytes(), WriteFlags::UPSERT)?;
        }

        // Update daily metrics
        for (key, value) in &self.daily_metrics {
            let key_bytes = key.encode();
            let current: i64 = match tx.inner.get::<Vec<u8>>(daily_metrics_dbi, key_bytes.as_slice()) {
                Ok(Some(bytes)) if bytes.len() >= 8 => {
                    i64::from_le_bytes(bytes[..8].try_into().unwrap_or([0u8; 8]))
                }
                _ => 0,
            };
            let new_value = current + value;
            tx.inner.put(daily_metrics_dbi, key_bytes.as_slice(), &new_value.to_le_bytes(), WriteFlags::UPSERT)?;
        }

        // Update last indexed block in metadata
        tx.inner.put(metadata_dbi, b"last_block", &last_block.to_be_bytes(), WriteFlags::UPSERT)?;

        tx.commit()?;
        Ok(())
    }
}

#[cfg(not(feature = "reth"))]
impl MdbxWriteBatch {
    pub fn insert_address_tx(
        &mut self,
        _address: Address,
        _tx_hash: alloy_primitives::TxHash,
        _block: u64,
        _tx_idx: u32,
    ) {
    }

    pub fn insert_tx_block(&mut self, _tx_hash: alloy_primitives::TxHash, _block: u64) {
    }

    pub fn insert_transfer(&mut self, _transfer: TokenTransfer) {
    }

    pub fn update_holder_balance(
        &mut self,
        _token: Address,
        _holder: Address,
        _new_balance: alloy_primitives::U256,
        _old_balance: Option<alloy_primitives::U256>,
    ) {
    }

    pub fn increment_counter(&mut self, _name: &[u8], _delta: i64) {
    }

    pub fn increment_address_counter(&mut self, _address: Address, _kind: u8, _delta: i64) {
    }

    pub fn record_daily_metric(&mut self, _year: u16, _month: u8, _day: u8, _metric: u8, _value: i64) {
    }

    pub fn record_block_timestamp(&mut self, _timestamp: u64, _tx_count: i64, _transfer_count: i64) {
    }

    pub fn collect_addresses(&self) -> impl Iterator<Item = Address> + '_ {
        std::iter::empty()
    }

    pub fn is_large(&self) -> bool {
        false
    }

    pub fn commit(self, _last_block: u64) -> Result<()> {
        Err(eyre::eyre!("MDBX support requires 'reth' feature"))
    }
}

// ============================================================================
// IndexDatabase Trait Implementation (Task 4.3)
// ============================================================================

use async_trait::async_trait;
use crate::index_trait::IndexDatabase;

#[cfg(feature = "reth")]
#[async_trait]
impl IndexDatabase for MdbxIndex {
    async fn last_indexed_block(&self) -> Result<Option<u64>> {
        // MDBX is sync, but we're already calling it directly (no blocking needed for simple reads)
        self.get_last_indexed_block()
    }

    async fn get_address_txs(
        &self,
        address: &Address,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<alloy_primitives::TxHash>> {
        self.get_address_txs(address, limit, offset)
    }

    async fn get_address_transfers(
        &self,
        address: &Address,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<crate::index_trait::TokenTransfer>> {
        // TokenTransfer is already the same type (re-exported from index_trait)
        self.get_address_transfers(address, limit, offset)
    }

    async fn get_token_holders(
        &self,
        token: &Address,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<(Address, alloy_primitives::U256)>> {
        self.get_token_holders(token, limit, offset)
    }

    async fn get_token_transfers(
        &self,
        token: &Address,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<crate::index_trait::TokenTransfer>> {
        // TokenTransfer is already the same type (re-exported from index_trait)
        self.get_token_transfers(token, limit, offset)
    }

    async fn get_tx_block(&self, tx_hash: &alloy_primitives::TxHash) -> Result<Option<u64>> {
        self.get_tx_block(tx_hash)
    }

    async fn get_total_txs(&self) -> Result<u64> {
        self.get_total_txs()
    }

    async fn get_total_addresses(&self) -> Result<u64> {
        self.get_total_addresses()
    }

    async fn get_total_transfers(&self) -> Result<u64> {
        self.get_total_transfers()
    }

    async fn get_address_tx_count(&self, address: &Address) -> Result<u64> {
        // MDBX returns i64, convert to u64
        let count = self.get_address_counter(address, 0)?; // 0 = TX counter
        Ok(count as u64)
    }

    async fn get_address_transfer_count(&self, address: &Address) -> Result<u64> {
        // MDBX returns i64, convert to u64
        let count = self.get_address_counter(address, 1)?; // 1 = TOKEN_TRANSFER counter
        Ok(count as u64)
    }

    async fn get_token_holder_count(&self, token: &Address) -> Result<u64> {
        // MDBX returns i64, convert to u64
        let count = self.get_token_holder_count(token)?;
        Ok(count as u64)
    }

    async fn get_daily_tx_metrics(&self, _days: usize) -> Result<Vec<(String, u64)>> {
        // Placeholder - MDBX doesn't implement this yet
        // Return empty vec for now
        Ok(vec![])
    }
}
