use std::path::Path;

use alloy_primitives::{Address, TxHash, U256};
use eyre::Result;
use redb::{Database, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};

// Table definitions
const ADDRESS_TXS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("address_txs");
const ADDRESS_TRANSFERS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("address_transfers");
const TX_BLOCK: TableDefinition<&[u8], u64> = TableDefinition::new("tx_block");
const TOKEN_HOLDERS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("token_holders");
const META: TableDefinition<&str, u64> = TableDefinition::new("meta");

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenTransfer {
    pub tx_hash: [u8; 32],
    pub log_index: u64,
    pub token_address: [u8; 20],
    pub from: [u8; 20],
    pub to: [u8; 20],
    pub value: [u8; 32], // U256 as bytes
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

pub struct IndexDb {
    db: Database,
}

impl IndexDb {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let db = Database::create(path)?;

        // Create tables
        let write_txn = db.begin_write()?;
        {
            let _ = write_txn.open_table(ADDRESS_TXS)?;
            let _ = write_txn.open_table(ADDRESS_TRANSFERS)?;
            let _ = write_txn.open_table(TX_BLOCK)?;
            let _ = write_txn.open_table(TOKEN_HOLDERS)?;
            let _ = write_txn.open_table(META)?;
        }
        write_txn.commit()?;

        Ok(Self { db })
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let db = Database::open(path)?;
        Ok(Self { db })
    }

    // Meta operations
    pub fn last_indexed_block(&self) -> Result<Option<u64>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(META)?;

        match table.get("last_block")? {
            Some(value) => Ok(Some(value.value())),
            None => Ok(None),
        }
    }

    pub fn set_last_indexed_block(&self, block: u64) -> Result<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(META)?;
            table.insert("last_block", block)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    // Write batch for bulk operations
    pub fn write_batch(&self) -> Result<WriteBatch<'_>> {
        Ok(WriteBatch {
            db: &self.db,
            address_txs: Vec::new(),
            tx_blocks: Vec::new(),
            transfers: Vec::new(),
        })
    }

    // Read operations
    pub fn get_address_txs(
        &self,
        address: &Address,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<TxHash>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(ADDRESS_TXS)?;

        match table.get(address.as_slice())? {
            Some(value) => {
                let hashes: Vec<[u8; 32]> = bincode::deserialize(value.value())?;
                Ok(hashes
                    .into_iter()
                    .skip(offset)
                    .take(limit)
                    .map(|h| TxHash::from(h))
                    .collect())
            }
            None => Ok(vec![]),
        }
    }

    pub fn get_address_transfers(
        &self,
        address: &Address,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<TokenTransfer>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(ADDRESS_TRANSFERS)?;

        match table.get(address.as_slice())? {
            Some(value) => {
                let transfers: Vec<TokenTransfer> = bincode::deserialize(value.value())?;
                Ok(transfers.into_iter().skip(offset).take(limit).collect())
            }
            None => Ok(vec![]),
        }
    }

    pub fn get_token_holders(
        &self,
        token: &Address,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<(Address, U256)>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(TOKEN_HOLDERS)?;

        match table.get(token.as_slice())? {
            Some(value) => {
                let holders: Vec<([u8; 20], [u8; 32])> = bincode::deserialize(value.value())?;
                Ok(holders
                    .into_iter()
                    .skip(offset)
                    .take(limit)
                    .map(|(addr, bal)| {
                        (
                            Address::from(addr),
                            U256::from_be_bytes(bal),
                        )
                    })
                    .collect())
            }
            None => Ok(vec![]),
        }
    }

    pub fn get_tx_block(&self, tx_hash: &TxHash) -> Result<Option<u64>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(TX_BLOCK)?;

        match table.get(tx_hash.as_slice())? {
            Some(value) => Ok(Some(value.value())),
            None => Ok(None),
        }
    }
}

pub struct WriteBatch<'a> {
    db: &'a Database,
    address_txs: Vec<(Address, TxHash)>,
    tx_blocks: Vec<(TxHash, u64)>,
    transfers: Vec<TokenTransfer>,
}

impl<'a> WriteBatch<'a> {
    pub fn insert_address_tx(&mut self, address: Address, tx_hash: TxHash) {
        self.address_txs.push((address, tx_hash));
    }

    pub fn insert_tx_block(&mut self, tx_hash: TxHash, block_number: u64) {
        self.tx_blocks.push((tx_hash, block_number));
    }

    pub fn insert_transfer(&mut self, transfer: TokenTransfer) {
        self.transfers.push(transfer);
    }

    pub fn set_last_indexed_block(self, block: u64) -> Result<()> {
        let write_txn = self.db.begin_write()?;
        
        // Write address → txs
        {
            let mut table = write_txn.open_table(ADDRESS_TXS)?;
            
            // Group by address
            let mut grouped: std::collections::HashMap<Address, Vec<[u8; 32]>> =
                std::collections::HashMap::new();
            
            for (addr, tx) in &self.address_txs {
                grouped.entry(*addr).or_default().push(tx.0);
            }

            for (addr, new_txs) in grouped {
                let existing: Vec<[u8; 32]> = match table.get(addr.as_slice())? {
                    Some(v) => bincode::deserialize(v.value())?,
                    None => vec![],
                };
                
                let mut all_txs = existing;
                all_txs.extend(new_txs);
                
                let bytes = bincode::serialize(&all_txs)?;
                table.insert(addr.as_slice(), bytes.as_slice())?;
            }
        }

        // Write tx → block
        {
            let mut table = write_txn.open_table(TX_BLOCK)?;
            for (tx_hash, block_num) in &self.tx_blocks {
                table.insert(tx_hash.as_slice(), *block_num)?;
            }
        }

        // Write transfers (grouped by from/to address)
        {
            let mut table = write_txn.open_table(ADDRESS_TRANSFERS)?;
            
            let mut grouped: std::collections::HashMap<[u8; 20], Vec<TokenTransfer>> =
                std::collections::HashMap::new();
            
            for transfer in &self.transfers {
                grouped.entry(transfer.from).or_default().push(transfer.clone());
                grouped.entry(transfer.to).or_default().push(transfer.clone());
            }

            for (addr, new_transfers) in grouped {
                let existing: Vec<TokenTransfer> = match table.get(addr.as_slice())? {
                    Some(v) => bincode::deserialize(v.value())?,
                    None => vec![],
                };
                
                let mut all_transfers = existing;
                all_transfers.extend(new_transfers);
                
                let bytes = bincode::serialize(&all_transfers)?;
                table.insert(addr.as_slice(), bytes.as_slice())?;
            }
        }

        // Update last block
        {
            let mut table = write_txn.open_table(META)?;
            table.insert("last_block", block)?;
        }

        write_txn.commit()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{address, b256};

    #[test]
    fn test_create_and_query() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let db_path = dir.path().join("test.redb");

        let db = IndexDb::create(&db_path)?;

        // Insert some data
        let addr = address!("0000000000000000000000000000000000000001");
        let tx_hash = b256!("0000000000000000000000000000000000000000000000000000000000000001");

        let mut batch = db.write_batch()?;
        batch.insert_address_tx(addr, tx_hash);
        batch.insert_tx_block(tx_hash, 12345);
        batch.set_last_indexed_block(12345)?;

        // Query
        let txs = db.get_address_txs(&addr, 10, 0)?;
        assert_eq!(txs.len(), 1);
        assert_eq!(txs[0], tx_hash);

        let block = db.get_tx_block(&tx_hash)?;
        assert_eq!(block, Some(12345));

        let last = db.last_indexed_block()?;
        assert_eq!(last, Some(12345));

        Ok(())
    }
}
