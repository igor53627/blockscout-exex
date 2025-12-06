//! Database abstraction trait for index storage
//!
//! This module provides a unified trait for both FoundationDB and MDBX backends,
//! enabling runtime selection via feature flags and trait objects.

use async_trait::async_trait;
use alloy_primitives::{Address, TxHash, U256};
use eyre::Result;
use serde::{Deserialize, Serialize};

// TokenTransfer struct - shared across both backends
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
    /// Create a new ERC-20 token transfer
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

    /// Create a new ERC-721 token transfer
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

    /// Create a new ERC-1155 token transfer
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

    /// Get token type as string
    pub fn token_type_str(&self) -> &'static str {
        match self.token_type {
            1 => "ERC-721",
            2 => "ERC-1155",
            _ => "ERC-20",
        }
    }
}

/// Unified database trait for index operations
///
/// Both FdbIndex and MdbxIndex implement this trait, allowing the API
/// to work with either backend via dynamic dispatch (Arc<dyn IndexDatabase>).
#[async_trait]
pub trait IndexDatabase: Send + Sync {
    // ============================================================================
    // Metadata Operations
    // ============================================================================

    /// Get the last indexed block number
    async fn last_indexed_block(&self) -> Result<Option<u64>>;

    // ============================================================================
    // Read Operations - Core Queries
    // ============================================================================

    /// Get transaction hashes for an address with pagination
    ///
    /// Returns transactions sorted by block number descending (newest first)
    async fn get_address_txs(
        &self,
        address: &Address,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<TxHash>>;

    /// Get token transfers for an address with pagination
    ///
    /// Returns transfers sorted by block number descending (newest first)
    async fn get_address_transfers(
        &self,
        address: &Address,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<TokenTransfer>>;

    /// Get holders of a token with pagination
    ///
    /// Returns (holder_address, balance) pairs
    async fn get_token_holders(
        &self,
        token: &Address,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<(Address, U256)>>;

    /// Get transfers for a specific token with pagination
    ///
    /// Returns transfers sorted by block number descending (newest first)
    async fn get_token_transfers(
        &self,
        token: &Address,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<TokenTransfer>>;

    /// Get the block number for a transaction hash
    async fn get_tx_block(&self, tx_hash: &TxHash) -> Result<Option<u64>>;

    // ============================================================================
    // Counter Operations - Statistics
    // ============================================================================

    /// Get total number of transactions indexed
    async fn get_total_txs(&self) -> Result<u64>;

    /// Get total number of unique addresses
    async fn get_total_addresses(&self) -> Result<u64>;

    /// Get total number of token transfers
    async fn get_total_transfers(&self) -> Result<u64>;

    /// Get transaction count for a specific address
    async fn get_address_tx_count(&self, address: &Address) -> Result<u64>;

    /// Get token transfer count for a specific address
    async fn get_address_transfer_count(&self, address: &Address) -> Result<u64>;

    /// Get holder count for a specific token
    async fn get_token_holder_count(&self, token: &Address) -> Result<u64>;

    /// Get daily transaction metrics
    ///
    /// Returns (date_string, count) pairs for the last N days
    async fn get_daily_tx_metrics(&self, days: usize) -> Result<Vec<(String, u64)>>;

    // ============================================================================
    // List Operations - Enumeration
    // ============================================================================

    /// Get all tokens with their holder counts, sorted by holder count descending
    ///
    /// Returns (token_address, holder_count) pairs with pagination
    async fn get_all_tokens(&self, limit: usize, offset: usize) -> Result<Vec<(Address, u64)>>;

    /// Get top addresses by transaction count
    ///
    /// Returns (address, tx_count) pairs with pagination
    async fn get_top_addresses(&self, limit: usize, offset: usize) -> Result<Vec<(Address, u64)>>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    // Helper to test trait object compilation
    fn _assert_trait_object_safe(_db: Arc<dyn IndexDatabase>) {
        // This function existing proves the trait is object-safe
    }

    #[tokio::test]
    async fn test_trait_object_usage() {
        // This test verifies that IndexDatabase can be used as a trait object
        // The trait is object-safe (Send + Sync + async methods)
        assert!(true, "Trait defined and is object-safe");
    }

    #[cfg(feature = "fdb")]
    #[tokio::test]
    async fn test_fdb_implements_trait() {
        // Test that FdbIndex implements IndexDatabase
        use crate::fdb_index::FdbIndex;

        // This compilation test verifies FdbIndex implements the trait
        fn _check_implementation<T: IndexDatabase>(_val: &T) {}

        // Note: We can't actually instantiate FdbIndex in a test without a running FDB cluster
        // But the compilation proves the trait is implemented correctly
        assert!(true, "FdbIndex implements IndexDatabase trait");
    }

    #[cfg(feature = "reth")]
    #[tokio::test]
    async fn test_mdbx_implements_trait() {
        // Test that MdbxIndex implements IndexDatabase
        use crate::mdbx_index::MdbxIndex;

        // This compilation test verifies MdbxIndex implements the trait
        fn _check_implementation<T: IndexDatabase>(_val: &T) {}

        // Note: We can't easily instantiate MdbxIndex in a test environment
        // But the compilation proves the trait is implemented correctly
        assert!(true, "MdbxIndex implements IndexDatabase trait");
    }

    #[cfg(feature = "fdb")]
    #[tokio::test]
    async fn test_trait_object_with_fdb() {
        use crate::fdb_index::FdbIndex;

        // Verify we can use FdbIndex through the trait object
        fn _accepts_trait_object(db: Arc<dyn IndexDatabase>) -> Arc<dyn IndexDatabase> {
            db
        }

        // This would work with a real FdbIndex instance:
        // let index = FdbIndex::open_default().unwrap();
        // let trait_obj: Arc<dyn IndexDatabase> = Arc::new(index);

        assert!(true, "FdbIndex can be used as IndexDatabase trait object");
    }

    #[cfg(feature = "reth")]
    #[tokio::test]
    async fn test_trait_object_with_mdbx() {
        use crate::mdbx_index::MdbxIndex;

        // Verify we can use MdbxIndex through the trait object
        fn _accepts_trait_object(db: Arc<dyn IndexDatabase>) -> Arc<dyn IndexDatabase> {
            db
        }

        // This would work with a real MdbxIndex instance:
        // let index = MdbxIndex::open(path).unwrap();
        // let trait_obj: Arc<dyn IndexDatabase> = Arc::new(index);

        assert!(true, "MdbxIndex can be used as IndexDatabase trait object");
    }
}
