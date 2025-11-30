//! Direct read-only access to Reth's MDBX database.
//!
//! This module provides zero-copy access to blocks, transactions, and receipts
//! without any network overhead.

use std::path::Path;
use std::sync::Arc;

use alloy_primitives::{Address, B256, U256};
use eyre::Result;
use reth_chainspec::{MAINNET, SEPOLIA};
use reth_db::{open_db_read_only, DatabaseEnv};
use reth_node_ethereum::EthereumNode;
use reth_node_types::NodeTypesWithDBAdapter;
use alloy_eips::BlockHashOrNumber;
use reth_provider::{
    providers::StaticFileProvider, BlockNumReader, BlockReader, ProviderFactory,
    ReceiptProvider, StateProvider, TransactionVariant, TransactionsProvider,
};

type NodeTypes = NodeTypesWithDBAdapter<EthereumNode, Arc<DatabaseEnv>>;

pub struct RethReader {
    factory: ProviderFactory<NodeTypes>,
}

impl RethReader {
    pub fn open_mainnet(
        db_path: impl AsRef<Path>,
        static_files_path: impl AsRef<Path>,
    ) -> Result<Self> {
        let db = Arc::new(open_db_read_only(db_path.as_ref(), Default::default())?);
        let static_files = StaticFileProvider::read_only(static_files_path.as_ref(), false)?;
        let factory = ProviderFactory::<NodeTypes>::new(db, MAINNET.clone(), static_files);
        Ok(Self { factory })
    }

    pub fn open_sepolia(
        db_path: impl AsRef<Path>,
        static_files_path: impl AsRef<Path>,
    ) -> Result<Self> {
        let db = Arc::new(open_db_read_only(db_path.as_ref(), Default::default())?);
        let static_files = StaticFileProvider::read_only(static_files_path.as_ref(), false)?;
        let factory = ProviderFactory::<NodeTypes>::new(db, SEPOLIA.clone(), static_files);
        Ok(Self { factory })
    }

    pub fn last_block_number(&self) -> Result<Option<u64>> {
        let provider = self.factory.provider()?;
        Ok(provider.last_block_number().ok())
    }

    pub fn block_by_number(
        &self,
        number: u64,
    ) -> Result<Option<reth_primitives::RecoveredBlock<reth_primitives::Block>>> {
        let provider = self.factory.provider()?;
        Ok(provider.recovered_block(
            BlockHashOrNumber::Number(number),
            TransactionVariant::WithHash,
        )?)
    }

    pub fn block_by_hash(
        &self,
        hash: B256,
    ) -> Result<Option<reth_primitives::RecoveredBlock<reth_primitives::Block>>> {
        let provider = self.factory.provider()?;
        Ok(provider.recovered_block(
            BlockHashOrNumber::Hash(hash),
            TransactionVariant::WithHash,
        )?)
    }

    pub fn transaction_by_hash(
        &self,
        hash: B256,
    ) -> Result<Option<reth_primitives::TransactionSigned>> {
        let provider = self.factory.provider()?;
        Ok(provider.transaction_by_hash(hash)?)
    }

    pub fn transaction_block_number(&self, hash: B256) -> Result<Option<u64>> {
        let provider = self.factory.provider()?;
        if let Some(meta) = provider.transaction_by_hash_with_meta(hash)? {
            Ok(Some(meta.1.block_number))
        } else {
            Ok(None)
        }
    }

    pub fn receipts_by_block(
        &self,
        number: u64,
    ) -> Result<Option<Vec<reth_primitives::Receipt>>> {
        let provider = self.factory.provider()?;
        Ok(provider.receipts_by_block(number.into())?)
    }

    pub fn receipt_by_hash(&self, hash: B256) -> Result<Option<reth_primitives::Receipt>> {
        let provider = self.factory.provider()?;
        Ok(provider.receipt_by_hash(hash)?)
    }

    pub fn account_balance(&self, address: Address) -> Result<U256> {
        let provider = self.factory.latest()?;
        Ok(provider.account_balance(&address)?.unwrap_or_default())
    }

    pub fn account_nonce(&self, address: Address) -> Result<u64> {
        let provider = self.factory.latest()?;
        Ok(provider.account_nonce(&address)?.unwrap_or_default())
    }

    pub fn is_contract(&self, address: Address) -> Result<bool> {
        let provider = self.factory.latest()?;
        Ok(provider.account_code(&address)?.is_some())
    }
}
