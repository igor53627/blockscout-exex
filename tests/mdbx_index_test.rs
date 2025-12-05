//! Integration tests for MDBX index module
//!
//! Following TDD approach: RED -> GREEN -> REFACTOR

use blockscout_exex::fdb_index::TokenTransfer;
use alloy_primitives::{Address, TxHash, U256};
use std::path::PathBuf;
use tempfile::TempDir;

// ============================================================================
// SUBTASK 1.1: Database Environment and Configuration Tests
// ============================================================================

#[test]
fn test_environment_open_close() {
    // RED PHASE: This should fail because MdbxIndex doesn't exist yet
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.mdbx");

    // Should be able to open a new database
    let result = blockscout_exex::mdbx_index::MdbxIndex::open(&db_path);
    assert!(result.is_ok(), "Should open new MDBX database");

    let index = result.unwrap();

    // Should be able to close cleanly (via Drop)
    drop(index);

    // Should be able to reopen existing database
    let result = blockscout_exex::mdbx_index::MdbxIndex::open(&db_path);
    assert!(result.is_ok(), "Should reopen existing MDBX database");
}

#[test]
fn test_schema_version_detection() {
    // RED PHASE: Test schema versioning system
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.mdbx");

    let index = blockscout_exex::mdbx_index::MdbxIndex::open(&db_path).unwrap();

    // New database should have version 1
    let version = index.get_schema_version();
    assert!(version.is_ok(), "Should retrieve schema version");
    assert_eq!(version.unwrap(), 1, "New database should be schema version 1");

    // Should be able to set version
    let result = index.set_schema_version(2);
    assert!(result.is_ok(), "Should be able to set schema version");

    // Version should persist after reopen
    drop(index);
    let index = blockscout_exex::mdbx_index::MdbxIndex::open(&db_path).unwrap();
    let version = index.get_schema_version().unwrap();
    assert_eq!(version, 2, "Schema version should persist");
}

#[test]
fn test_transaction_modes() {
    // RED PHASE: Test read-only and read-write transaction creation
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.mdbx");

    let index = blockscout_exex::mdbx_index::MdbxIndex::open(&db_path).unwrap();

    // Should create read transaction
    let read_tx = index.begin_read();
    assert!(read_tx.is_ok(), "Should create read transaction");
    drop(read_tx);

    // Should create write transaction
    let write_tx = index.begin_write();
    assert!(write_tx.is_ok(), "Should create write transaction");
    drop(write_tx);

    // Should support multiple concurrent read transactions
    let read_tx1 = index.begin_read().unwrap();
    let read_tx2 = index.begin_read().unwrap();
    let read_tx3 = index.begin_read().unwrap();

    drop(read_tx1);
    drop(read_tx2);
    drop(read_tx3);
}

#[test]
fn test_key_encoding_roundtrip() {
    // RED PHASE: Test composite key encoding/decoding
    use blockscout_exex::mdbx_index::{AddressTxKey, AddressTransferKey, TokenHolderKey};

    let address = Address::from([0x42; 20]);
    let block = 12345678u64;
    let tx_idx = 99u32;

    // Test AddressTxKey encoding/decoding
    let key = AddressTxKey::new(address, block, tx_idx);
    let encoded = key.encode();
    let decoded = AddressTxKey::decode(&encoded).unwrap();
    assert_eq!(decoded.address(), address, "Address should match");
    assert_eq!(decoded.block(), block, "Block should match");
    assert_eq!(decoded.tx_idx(), tx_idx, "Tx index should match");

    // Test AddressTransferKey encoding/decoding
    let log_idx = 5u64;
    let key = AddressTransferKey::new(address, block, log_idx);
    let encoded = key.encode();
    let decoded = AddressTransferKey::decode(&encoded).unwrap();
    assert_eq!(decoded.address(), address, "Address should match");
    assert_eq!(decoded.block(), block, "Block should match");
    assert_eq!(decoded.log_idx(), log_idx, "Log index should match");

    // Test TokenHolderKey encoding/decoding
    let token = Address::from([0x11; 20]);
    let holder = Address::from([0x22; 20]);
    let key = TokenHolderKey::new(token, holder);
    let encoded = key.encode();
    let decoded = TokenHolderKey::decode(&encoded).unwrap();
    assert_eq!(decoded.token(), token, "Token should match");
    assert_eq!(decoded.holder(), holder, "Holder should match");
}

#[test]
fn test_value_serialization() {
    // RED PHASE: Test TokenTransfer serialization with bincode
    let tx_hash = TxHash::from([0x33; 32]);
    let token = Address::from([0x11; 20]);
    let from = Address::from([0x22; 20]);
    let to = Address::from([0x44; 20]);
    let value = U256::from(1000000u64);
    let block = 12345u64;
    let timestamp = 1234567890u64;
    let log_index = 5u64;

    let transfer = TokenTransfer::new(
        tx_hash,
        log_index,
        token,
        from,
        to,
        value,
        block,
        timestamp,
    );

    // Should serialize
    let serialized = bincode::serialize(&transfer);
    assert!(serialized.is_ok(), "Should serialize TokenTransfer");

    // Should deserialize
    let bytes = serialized.unwrap();
    let deserialized: Result<TokenTransfer, _> = bincode::deserialize(&bytes);
    assert!(deserialized.is_ok(), "Should deserialize TokenTransfer");

    // Values should match
    let transfer2 = deserialized.unwrap();
    assert_eq!(transfer2.tx_hash, transfer.tx_hash, "Tx hash should match");
    assert_eq!(transfer2.token_address, transfer.token_address, "Token should match");
    assert_eq!(transfer2.from, transfer.from, "From should match");
    assert_eq!(transfer2.to, transfer.to, "To should match");
    assert_eq!(transfer2.value, transfer.value, "Value should match");
    assert_eq!(transfer2.block_number, transfer.block_number, "Block should match");
}

// ============================================================================
// SUBTASK 1.2: MDBX Table Definitions Tests
// ============================================================================

#[test]
fn test_table_definitions_exist() {
    // RED PHASE: Test that all 10 table types are defined
    use blockscout_exex::mdbx_index::*;

    // Should be able to reference all table types
    let _t1: Option<MdbxTable<AddressTxs>> = None;
    let _t2: Option<MdbxTable<AddressTransfers>> = None;
    let _t3: Option<MdbxTable<TokenHolders>> = None;
    let _t4: Option<MdbxTable<TxBlocks>> = None;
    let _t5: Option<MdbxTable<TokenTransfers>> = None;
    let _t6: Option<MdbxTable<Metadata>> = None;
    let _t7: Option<MdbxTable<Counters>> = None;
    let _t8: Option<MdbxTable<AddressCounters>> = None;
    let _t9: Option<MdbxTable<TokenHolderCounts>> = None;
    let _t10: Option<MdbxTable<DailyMetrics>> = None;
}

#[test]
fn test_additional_composite_keys() {
    // RED PHASE: Test remaining composite key types
    use blockscout_exex::mdbx_index::{TokenTransferKey, AddressCounterKey, DailyMetricKey};

    let token = Address::from([0x11; 20]);
    let block = 12345u64;
    let log_idx = 99u64;

    // TokenTransferKey: (token:20, block:8, log_idx:8)
    let key = TokenTransferKey::new(token, block, log_idx);
    let encoded = key.encode();
    let decoded = TokenTransferKey::decode(&encoded).unwrap();
    assert_eq!(decoded.token(), token);
    assert_eq!(decoded.block(), block);
    assert_eq!(decoded.log_idx(), log_idx);

    // AddressCounterKey: (address:20, kind:1)
    let address = Address::from([0x42; 20]);
    let kind = 1u8;
    let key = AddressCounterKey::new(address, kind);
    let encoded = key.encode();
    let decoded = AddressCounterKey::decode(&encoded).unwrap();
    assert_eq!(decoded.address(), address);
    assert_eq!(decoded.kind(), kind);

    // DailyMetricKey: (year:2, month:1, day:1, metric:1)
    let year = 2024u16;
    let month = 12u8;
    let day = 5u8;
    let metric = 0u8;
    let key = DailyMetricKey::new(year, month, day, metric);
    let encoded = key.encode();
    let decoded = DailyMetricKey::decode(&encoded).unwrap();
    assert_eq!(decoded.year(), year);
    assert_eq!(decoded.month(), month);
    assert_eq!(decoded.day(), day);
    assert_eq!(decoded.metric(), metric);
}

// ============================================================================
// SUBTASK 1.3: Schema Versioning and Metadata Tests
// ============================================================================

#[test]
fn test_metadata_operations() {
    // RED PHASE: Test metadata get/set operations
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.mdbx");

    let index = blockscout_exex::mdbx_index::MdbxIndex::open(&db_path).unwrap();

    // Schema version should default to 1
    let version = index.get_schema_version().unwrap();
    assert_eq!(version, 1);

    // Should be able to update schema version
    index.set_schema_version(2).unwrap();
    let version = index.get_schema_version().unwrap();
    assert_eq!(version, 2);

    // Last indexed block should be None initially
    let last_block = index.get_last_indexed_block().unwrap();
    assert_eq!(last_block, None);

    // Should be able to set last indexed block
    index.set_last_indexed_block(12345).unwrap();
    let last_block = index.get_last_indexed_block().unwrap();
    assert_eq!(last_block, Some(12345));

    // Should persist after reopen
    drop(index);
    let index = blockscout_exex::mdbx_index::MdbxIndex::open(&db_path).unwrap();
    assert_eq!(index.get_schema_version().unwrap(), 2);
    assert_eq!(index.get_last_indexed_block().unwrap(), Some(12345));
}

// ============================================================================
// SUBTASK 1.4: Read Transaction Operations Tests
// ============================================================================

#[test]
fn test_read_operations() {
    // RED PHASE: Test read operations for all tables
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.mdbx");

    let index = blockscout_exex::mdbx_index::MdbxIndex::open(&db_path).unwrap();

    let address = Address::from([0x42; 20]);
    let token = Address::from([0x11; 20]);
    let tx_hash = TxHash::from([0x33; 32]);

    // All reads should return empty results on new database
    let txs = index.get_address_txs(&address, 10, 0).unwrap();
    assert_eq!(txs.len(), 0, "New db should have no address txs");

    let transfers = index.get_address_transfers(&address, 10, 0).unwrap();
    assert_eq!(transfers.len(), 0, "New db should have no address transfers");

    let holders = index.get_token_holders(&token, 10, 0).unwrap();
    assert_eq!(holders.len(), 0, "New db should have no token holders");

    let token_transfers = index.get_token_transfers(&token, 10, 0).unwrap();
    assert_eq!(token_transfers.len(), 0, "New db should have no token transfers");

    let block = index.get_tx_block(&tx_hash).unwrap();
    assert_eq!(block, None, "New db should have no tx blocks");

    let total_txs = index.get_total_txs().unwrap();
    assert_eq!(total_txs, 0, "New db should have 0 total txs");

    let total_transfers = index.get_total_transfers().unwrap();
    assert_eq!(total_transfers, 0, "New db should have 0 total transfers");

    let total_addresses = index.get_total_addresses().unwrap();
    assert_eq!(total_addresses, 0, "New db should have 0 total addresses");
}

// ============================================================================
// SUBTASK 1.5: Write Transaction Operations Tests
// ============================================================================

#[test]
fn test_write_batch_operations() {
    // RED PHASE: Test MdbxWriteBatch write and commit
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.mdbx");

    let index = blockscout_exex::mdbx_index::MdbxIndex::open(&db_path).unwrap();

    let address = Address::from([0x42; 20]);
    let tx_hash = TxHash::from([0x33; 32]);
    let token = Address::from([0x11; 20]);
    let from = Address::from([0x22; 20]);
    let to = Address::from([0x44; 20]);
    let value = U256::from(1000000u64);
    let block = 12345u64;
    let timestamp = 1234567890u64;

    // Create a write batch
    let mut batch = index.write_batch();

    // Insert address tx
    batch.insert_address_tx(address, tx_hash, block, 0);

    // Insert transfer
    let transfer = TokenTransfer::new(tx_hash, 0, token, from, to, value, block, timestamp);
    batch.insert_transfer(transfer);

    // Update holder balance
    batch.update_holder_balance(token, to, value, None);

    // Increment counter
    batch.increment_counter(b"total_txs", 1);

    // Commit batch
    let result = batch.commit(block);
    assert!(result.is_ok(), "Batch commit should succeed");

    // Verify data was written
    let txs = index.get_address_txs(&address, 10, 0).unwrap();
    assert_eq!(txs.len(), 1, "Should have 1 address tx");
    assert_eq!(txs[0], tx_hash, "Tx hash should match");

    let transfers = index.get_address_transfers(&from, 10, 0).unwrap();
    assert_eq!(transfers.len(), 1, "Should have 1 transfer");

    let holders = index.get_token_holders(&token, 10, 0).unwrap();
    assert_eq!(holders.len(), 1, "Should have 1 holder");
    assert_eq!(holders[0].0, to, "Holder address should match");
    assert_eq!(holders[0].1, value, "Holder balance should match");

    let total_txs = index.get_total_txs().unwrap();
    assert_eq!(total_txs, 1, "Should have 1 total tx");

    let last_block = index.get_last_indexed_block().unwrap();
    assert_eq!(last_block, Some(block), "Last indexed block should be set");
}

#[test]
fn test_write_batch_atomicity() {
    // RED PHASE: Test that write batch commits atomically
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.mdbx");

    let index = blockscout_exex::mdbx_index::MdbxIndex::open(&db_path).unwrap();

    let mut batch = index.write_batch();

    // Insert multiple operations
    for i in 0..10 {
        let address = Address::from([i as u8; 20]);
        let tx_hash = TxHash::from([i as u8; 32]);
        batch.insert_address_tx(address, tx_hash, 100 + i as u64, i);
        batch.increment_counter(b"total_txs", 1);
    }

    // Commit should be atomic
    batch.commit(110).unwrap();

    // All operations should be visible
    let total_txs = index.get_total_txs().unwrap();
    assert_eq!(total_txs, 10, "All tx increments should be visible");

    let last_block = index.get_last_indexed_block().unwrap();
    assert_eq!(last_block, Some(110), "Last block should be committed");
}

// ============================================================================
// TASK 2: MDBX Write Operations with Batch Commit (TDD)
// ============================================================================

// ============================================================================
// SUBTASK 2.1: Enhanced MdbxWriteBatch Structure Tests
// ============================================================================

#[test]
fn test_write_batch_creation() {
    // RED PHASE: Test enhanced MdbxWriteBatch structure
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.mdbx");

    let index = blockscout_exex::mdbx_index::MdbxIndex::open(&db_path).unwrap();

    // Should create empty write batch
    let batch = index.write_batch();

    // Batch should be committable even when empty
    let result = batch.commit(0);
    assert!(result.is_ok(), "Empty batch should commit successfully");
}

#[test]
fn test_write_batch_deferred_operations() {
    // RED PHASE: Test that write batch accumulates operations without writing
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.mdbx");

    let index = blockscout_exex::mdbx_index::MdbxIndex::open(&db_path).unwrap();

    let mut batch = index.write_batch();

    let address = Address::from([0x42; 20]);
    let tx_hash = TxHash::from([0x33; 32]);

    // Insert operations should not write immediately
    batch.insert_address_tx(address, tx_hash, 100, 0);
    batch.insert_tx_block(tx_hash, 100);
    batch.increment_counter(b"total_txs", 1);

    // Data should not be visible before commit
    let txs = index.get_address_txs(&address, 10, 0).unwrap();
    assert_eq!(txs.len(), 0, "Data should not be visible before commit");

    let total_txs = index.get_total_txs().unwrap();
    assert_eq!(total_txs, 0, "Counter should not be updated before commit");

    // After commit, data should be visible
    batch.commit(100).unwrap();

    let txs = index.get_address_txs(&address, 10, 0).unwrap();
    assert_eq!(txs.len(), 1, "Data should be visible after commit");

    let total_txs = index.get_total_txs().unwrap();
    assert_eq!(total_txs, 1, "Counter should be updated after commit");
}

// ============================================================================
// SUBTASK 2.2: Insert Methods for Address and Transaction Mappings
// ============================================================================

#[test]
fn test_insert_address_tx() {
    // RED PHASE: Test insert_address_tx with multiple transactions
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.mdbx");

    let index = blockscout_exex::mdbx_index::MdbxIndex::open(&db_path).unwrap();

    let address = Address::from([0x42; 20]);
    let mut batch = index.write_batch();

    // Insert multiple transactions for same address
    for i in 0..5 {
        let tx_hash = TxHash::from([i as u8; 32]);
        batch.insert_address_tx(address, tx_hash, 100 + i as u64, i);
    }

    batch.commit(104).unwrap();

    // Should retrieve all transactions in order
    let txs = index.get_address_txs(&address, 10, 0).unwrap();
    assert_eq!(txs.len(), 5, "Should have 5 transactions");

    // Verify ordering by block and tx_idx
    for i in 0..5 {
        assert_eq!(txs[i], TxHash::from([i as u8; 32]));
    }
}

#[test]
fn test_insert_tx_block() {
    // RED PHASE: Test insert_tx_block mapping
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.mdbx");

    let index = blockscout_exex::mdbx_index::MdbxIndex::open(&db_path).unwrap();

    let mut batch = index.write_batch();

    // Insert tx->block mappings
    for i in 0..10 {
        let tx_hash = TxHash::from([i as u8; 32]);
        batch.insert_tx_block(tx_hash, 1000 + i as u64);
    }

    batch.commit(1009).unwrap();

    // Verify mappings
    for i in 0..10 {
        let tx_hash = TxHash::from([i as u8; 32]);
        let block = index.get_tx_block(&tx_hash).unwrap();
        assert_eq!(block, Some(1000 + i as u64), "Block number should match");
    }
}

// ============================================================================
// SUBTASK 2.3: Insert Transfer with Multi-Token Type Support
// ============================================================================

#[test]
fn test_insert_transfer_erc20() {
    // RED PHASE: Test ERC-20 token transfer insertion
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.mdbx");

    let index = blockscout_exex::mdbx_index::MdbxIndex::open(&db_path).unwrap();

    let token = Address::from([0x11; 20]);
    let from = Address::from([0x22; 20]);
    let to = Address::from([0x44; 20]);
    let tx_hash = TxHash::from([0x33; 32]);
    let value = U256::from(1000000u64);

    let mut batch = index.write_batch();

    // Create ERC-20 transfer (no token_id)
    let transfer = TokenTransfer::new(
        tx_hash,
        0,
        token,
        from,
        to,
        value,
        12345,
        1234567890,
    );

    batch.insert_transfer(transfer.clone());
    batch.commit(12345).unwrap();

    // Verify transfer is stored in both address and token indexes
    let from_transfers = index.get_address_transfers(&from, 10, 0).unwrap();
    assert_eq!(from_transfers.len(), 1, "From address should have 1 transfer");
    assert_eq!(from_transfers[0].token_type, 0, "Should be ERC-20");
    assert_eq!(from_transfers[0].token_id, None, "ERC-20 should have no token_id");

    let to_transfers = index.get_address_transfers(&to, 10, 0).unwrap();
    assert_eq!(to_transfers.len(), 1, "To address should have 1 transfer");

    let token_transfers = index.get_token_transfers(&token, 10, 0).unwrap();
    assert_eq!(token_transfers.len(), 1, "Token should have 1 transfer");
}

#[test]
fn test_insert_transfer_erc721() {
    // RED PHASE: Test ERC-721 NFT transfer insertion
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.mdbx");

    let index = blockscout_exex::mdbx_index::MdbxIndex::open(&db_path).unwrap();

    let token = Address::from([0x11; 20]);
    let from = Address::from([0x22; 20]);
    let to = Address::from([0x44; 20]);
    let tx_hash = TxHash::from([0x33; 32]);
    let token_id = U256::from(42u64);

    let mut batch = index.write_batch();

    // Create ERC-721 transfer (value=1, with token_id)
    let transfer = TokenTransfer::new_erc721(
        tx_hash,
        0,
        token,
        from,
        to,
        token_id,
        12345,
        1234567890,
    );

    batch.insert_transfer(transfer);
    batch.commit(12345).unwrap();

    // Verify transfer
    let token_transfers = index.get_token_transfers(&token, 10, 0).unwrap();
    assert_eq!(token_transfers.len(), 1, "Token should have 1 transfer");
    assert_eq!(token_transfers[0].token_type, 1, "Should be ERC-721");
    assert!(token_transfers[0].token_id.is_some(), "ERC-721 should have token_id");
    assert_eq!(
        U256::from_be_bytes(token_transfers[0].value),
        U256::from(1),
        "ERC-721 value should be 1"
    );
}

#[test]
fn test_insert_transfer_erc1155() {
    // RED PHASE: Test ERC-1155 multi-token transfer insertion
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.mdbx");

    let index = blockscout_exex::mdbx_index::MdbxIndex::open(&db_path).unwrap();

    let token = Address::from([0x11; 20]);
    let from = Address::from([0x22; 20]);
    let to = Address::from([0x44; 20]);
    let tx_hash = TxHash::from([0x33; 32]);
    let token_id = U256::from(100u64);
    let value = U256::from(50u64);

    let mut batch = index.write_batch();

    // Create ERC-1155 transfer (custom value, with token_id)
    let transfer = TokenTransfer::new_erc1155(
        tx_hash,
        0,
        token,
        from,
        to,
        token_id,
        value,
        12345,
        1234567890,
    );

    batch.insert_transfer(transfer);
    batch.commit(12345).unwrap();

    // Verify transfer
    let token_transfers = index.get_token_transfers(&token, 10, 0).unwrap();
    assert_eq!(token_transfers.len(), 1, "Token should have 1 transfer");
    assert_eq!(token_transfers[0].token_type, 2, "Should be ERC-1155");
    assert!(token_transfers[0].token_id.is_some(), "ERC-1155 should have token_id");
    assert_eq!(
        U256::from_be_bytes(token_transfers[0].value),
        value,
        "ERC-1155 value should match"
    );
}

// ============================================================================
// SUBTASK 2.4: Holder Balance Tracking and Counter Operations
// ============================================================================

#[test]
fn test_update_holder_balance() {
    // RED PHASE: Test holder balance updates
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.mdbx");

    let index = blockscout_exex::mdbx_index::MdbxIndex::open(&db_path).unwrap();

    let token = Address::from([0x11; 20]);
    let holder = Address::from([0x22; 20]);
    let initial_balance = U256::from(1000u64);
    let new_balance = U256::from(2000u64);

    let mut batch = index.write_batch();

    // Initial balance (0 -> 1000)
    batch.update_holder_balance(token, holder, initial_balance, None);
    batch.commit(100).unwrap();

    // Verify initial balance
    let holders = index.get_token_holders(&token, 10, 0).unwrap();
    assert_eq!(holders.len(), 1, "Should have 1 holder");
    assert_eq!(holders[0].0, holder, "Holder address should match");
    assert_eq!(holders[0].1, initial_balance, "Balance should match");

    // Update balance (1000 -> 2000)
    let mut batch = index.write_batch();
    batch.update_holder_balance(token, holder, new_balance, Some(initial_balance));
    batch.commit(101).unwrap();

    // Verify updated balance
    let holders = index.get_token_holders(&token, 10, 0).unwrap();
    assert_eq!(holders.len(), 1, "Should still have 1 holder");
    assert_eq!(holders[0].1, new_balance, "Balance should be updated");
}

#[test]
fn test_holder_count_tracking() {
    // RED PHASE: Test holder count tracking with 0->1 and 1->0 transitions
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.mdbx");

    let index = blockscout_exex::mdbx_index::MdbxIndex::open(&db_path).unwrap();

    let token = Address::from([0x11; 20]);
    let holder1 = Address::from([0x22; 20]);
    let holder2 = Address::from([0x33; 20]);

    // Add first holder (0 -> positive: count +1)
    let mut batch = index.write_batch();
    batch.update_holder_balance(token, holder1, U256::from(1000u64), None);
    batch.commit(100).unwrap();

    let count = index.get_token_holder_count(&token).unwrap();
    assert_eq!(count, 1, "Should have 1 holder after first addition");

    // Add second holder (0 -> positive: count +1)
    let mut batch = index.write_batch();
    batch.update_holder_balance(token, holder2, U256::from(500u64), None);
    batch.commit(101).unwrap();

    let count = index.get_token_holder_count(&token).unwrap();
    assert_eq!(count, 2, "Should have 2 holders");

    // Remove first holder (positive -> 0: count -1)
    let mut batch = index.write_batch();
    batch.update_holder_balance(token, holder1, U256::ZERO, Some(U256::from(1000u64)));
    batch.commit(102).unwrap();

    let count = index.get_token_holder_count(&token).unwrap();
    assert_eq!(count, 1, "Should have 1 holder after removal");

    // Update second holder balance (positive -> positive: no count change)
    let mut batch = index.write_batch();
    batch.update_holder_balance(token, holder2, U256::from(1500u64), Some(U256::from(500u64)));
    batch.commit(103).unwrap();

    let count = index.get_token_holder_count(&token).unwrap();
    assert_eq!(count, 1, "Count should remain 1 after balance update");
}

#[test]
fn test_increment_counter() {
    // RED PHASE: Test atomic counter increment operations
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.mdbx");

    let index = blockscout_exex::mdbx_index::MdbxIndex::open(&db_path).unwrap();

    // Increment counter multiple times
    for i in 1..=10 {
        let mut batch = index.write_batch();
        batch.increment_counter(b"test_counter", 1);
        batch.commit(i).unwrap();
    }

    let count = index.get_counter(b"test_counter").unwrap();
    assert_eq!(count, 10, "Counter should be 10 after 10 increments");

    // Test negative increment (decrement)
    let mut batch = index.write_batch();
    batch.increment_counter(b"test_counter", -3);
    batch.commit(11).unwrap();

    let count = index.get_counter(b"test_counter").unwrap();
    assert_eq!(count, 7, "Counter should be 7 after decrement by 3");
}

#[test]
fn test_increment_address_counter() {
    // RED PHASE: Test per-address counter increments
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.mdbx");

    let index = blockscout_exex::mdbx_index::MdbxIndex::open(&db_path).unwrap();

    let address = Address::from([0x42; 20]);

    // Increment tx counter
    let mut batch = index.write_batch();
    batch.increment_address_counter(address, 0, 5); // kind=0 for txs
    batch.commit(100).unwrap();

    let count = index.get_address_counter(&address, 0).unwrap();
    assert_eq!(count, 5, "Address tx counter should be 5");

    // Increment transfer counter
    let mut batch = index.write_batch();
    batch.increment_address_counter(address, 1, 3); // kind=1 for transfers
    batch.commit(101).unwrap();

    let count = index.get_address_counter(&address, 1).unwrap();
    assert_eq!(count, 3, "Address transfer counter should be 3");
}

#[test]
fn test_record_daily_metric() {
    // RED PHASE: Test daily metrics recording
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.mdbx");

    let index = blockscout_exex::mdbx_index::MdbxIndex::open(&db_path).unwrap();

    let mut batch = index.write_batch();

    // Record daily metrics for 2024-12-05
    batch.record_daily_metric(2024, 12, 5, 0, 100); // metric=0 for txs
    batch.record_daily_metric(2024, 12, 5, 1, 50);  // metric=1 for transfers
    batch.commit(100).unwrap();

    let tx_count = index.get_daily_metric(2024, 12, 5, 0).unwrap();
    assert_eq!(tx_count, 100, "Daily tx count should be 100");

    let transfer_count = index.get_daily_metric(2024, 12, 5, 1).unwrap();
    assert_eq!(transfer_count, 50, "Daily transfer count should be 50");
}

// ============================================================================
// SUBTASK 2.5: Atomic Commit with MDBX Transactions
// ============================================================================

#[test]
fn test_batch_commit_atomicity() {
    // RED PHASE: Test all-or-nothing atomic commit
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.mdbx");

    let index = blockscout_exex::mdbx_index::MdbxIndex::open(&db_path).unwrap();

    let mut batch = index.write_batch();

    // Add multiple types of operations
    let address = Address::from([0x42; 20]);
    let tx_hash = TxHash::from([0x33; 32]);
    let token = Address::from([0x11; 20]);
    let from = Address::from([0x22; 20]);
    let to = Address::from([0x44; 20]);

    batch.insert_address_tx(address, tx_hash, 100, 0);
    batch.insert_tx_block(tx_hash, 100);

    let transfer = TokenTransfer::new(
        tx_hash,
        0,
        token,
        from,
        to,
        U256::from(1000u64),
        100,
        1234567890,
    );
    batch.insert_transfer(transfer);

    batch.update_holder_balance(token, to, U256::from(1000u64), None);
    batch.increment_counter(b"total_txs", 1);
    batch.increment_counter(b"total_transfers", 1);

    // Commit should be atomic
    batch.commit(100).unwrap();

    // All operations should be visible or none
    let txs = index.get_address_txs(&address, 10, 0).unwrap();
    assert_eq!(txs.len(), 1, "Tx should be committed");

    let block = index.get_tx_block(&tx_hash).unwrap();
    assert_eq!(block, Some(100), "Tx block should be committed");

    let transfers = index.get_token_transfers(&token, 10, 0).unwrap();
    assert_eq!(transfers.len(), 1, "Transfer should be committed");

    let holders = index.get_token_holders(&token, 10, 0).unwrap();
    assert_eq!(holders.len(), 1, "Holder balance should be committed");

    let total_txs = index.get_total_txs().unwrap();
    assert_eq!(total_txs, 1, "Counter should be committed");
}

#[test]
fn test_large_batch_commit() {
    // RED PHASE: Test large batch with 1000+ operations
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.mdbx");

    let index = blockscout_exex::mdbx_index::MdbxIndex::open(&db_path).unwrap();

    let mut batch = index.write_batch();

    // Insert 1000 transfers
    let token = Address::from([0x11; 20]);
    for i in 0..1000 {
        let from = Address::from([(i % 256) as u8; 20]);
        let to = Address::from([((i + 1) % 256) as u8; 20]);
        let tx_hash = TxHash::from([(i % 256) as u8; 32]);

        let transfer = TokenTransfer::new(
            tx_hash,
            i as u64,
            token,
            from,
            to,
            U256::from(1000u64),
            100 + (i / 10) as u64,
            1234567890 + i as u64,
        );

        batch.insert_transfer(transfer);
        batch.insert_address_tx(from, tx_hash, 100 + (i / 10) as u64, (i % 10) as u32);
        batch.increment_counter(b"total_transfers", 1);
    }

    // Should handle large batch
    let result = batch.commit(199);
    assert!(result.is_ok(), "Large batch should commit successfully");

    // Verify data
    let transfers = index.get_token_transfers(&token, 2000, 0).unwrap();
    assert_eq!(transfers.len(), 1000, "Should have 1000 transfers");

    let total = index.get_total_transfers().unwrap();
    assert_eq!(total, 1000, "Counter should be 1000");
}

#[test]
fn test_last_indexed_block_update() {
    // RED PHASE: Test that last_indexed_block is updated on commit
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.mdbx");

    let index = blockscout_exex::mdbx_index::MdbxIndex::open(&db_path).unwrap();

    // Initial state
    let last_block = index.get_last_indexed_block().unwrap();
    assert_eq!(last_block, None, "Initial last block should be None");

    // Commit batch with block 100
    let mut batch = index.write_batch();
    batch.increment_counter(b"test", 1);
    batch.commit(100).unwrap();

    let last_block = index.get_last_indexed_block().unwrap();
    assert_eq!(last_block, Some(100), "Last block should be 100");

    // Commit batch with block 200
    let mut batch = index.write_batch();
    batch.increment_counter(b"test", 1);
    batch.commit(200).unwrap();

    let last_block = index.get_last_indexed_block().unwrap();
    assert_eq!(last_block, Some(200), "Last block should be 200");
}

// ============================================================================
// TASK 3: MDBX Read Operations with Pagination (TDD)
// ============================================================================

// ============================================================================
// SUBTASK 3.1: Metadata Query Methods Tests
// ============================================================================

#[test]
fn test_last_indexed_block_read() {
    // RED PHASE: Test reading last indexed block
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.mdbx");

    let index = blockscout_exex::mdbx_index::MdbxIndex::open(&db_path).unwrap();

    // Initially should be None
    let last_block = index.get_last_indexed_block().unwrap();
    assert_eq!(last_block, None, "New db should have no last indexed block");

    // After writing, should return the block
    index.set_last_indexed_block(12345).unwrap();
    let last_block = index.get_last_indexed_block().unwrap();
    assert_eq!(last_block, Some(12345), "Should return last indexed block");

    // Should persist across reopens
    drop(index);
    let index = blockscout_exex::mdbx_index::MdbxIndex::open(&db_path).unwrap();
    let last_block = index.get_last_indexed_block().unwrap();
    assert_eq!(last_block, Some(12345), "Last block should persist");
}

#[test]
fn test_get_schema_version_read() {
    // RED PHASE: Test reading schema version
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.mdbx");

    let index = blockscout_exex::mdbx_index::MdbxIndex::open(&db_path).unwrap();

    // Should default to version 1
    let version = index.get_schema_version().unwrap();
    assert_eq!(version, 1, "New db should have schema version 1");

    // After setting, should return new version
    index.set_schema_version(2).unwrap();
    let version = index.get_schema_version().unwrap();
    assert_eq!(version, 2, "Should return updated schema version");
}

#[test]
fn test_get_total_txs_read() {
    // RED PHASE: Test reading total transaction count
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.mdbx");

    let index = blockscout_exex::mdbx_index::MdbxIndex::open(&db_path).unwrap();

    // Initially should be 0
    let total = index.get_total_txs().unwrap();
    assert_eq!(total, 0, "New db should have 0 txs");

    // After incrementing counter
    let mut batch = index.write_batch();
    batch.increment_counter(b"total_txs", 100);
    batch.commit(100).unwrap();

    let total = index.get_total_txs().unwrap();
    assert_eq!(total, 100, "Should return total txs");

    // After more increments
    let mut batch = index.write_batch();
    batch.increment_counter(b"total_txs", 50);
    batch.commit(101).unwrap();

    let total = index.get_total_txs().unwrap();
    assert_eq!(total, 150, "Should return updated total");
}

#[test]
fn test_get_total_transfers_read() {
    // RED PHASE: Test reading total transfer count
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.mdbx");

    let index = blockscout_exex::mdbx_index::MdbxIndex::open(&db_path).unwrap();

    // Initially should be 0
    let total = index.get_total_transfers().unwrap();
    assert_eq!(total, 0, "New db should have 0 transfers");

    // After incrementing
    let mut batch = index.write_batch();
    batch.increment_counter(b"total_transfers", 75);
    batch.commit(100).unwrap();

    let total = index.get_total_transfers().unwrap();
    assert_eq!(total, 75, "Should return total transfers");
}

#[test]
fn test_get_total_addresses_read() {
    // RED PHASE: Test reading total unique address count
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.mdbx");

    let index = blockscout_exex::mdbx_index::MdbxIndex::open(&db_path).unwrap();

    // Initially should be 0
    let total = index.get_total_addresses().unwrap();
    assert_eq!(total, 0, "New db should have 0 addresses");

    // After incrementing
    let mut batch = index.write_batch();
    batch.increment_counter(b"total_addresses", 42);
    batch.commit(100).unwrap();

    let total = index.get_total_addresses().unwrap();
    assert_eq!(total, 42, "Should return total addresses");
}
