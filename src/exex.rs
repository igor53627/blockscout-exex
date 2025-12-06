//! Reth Execution Extension (ExEx) for live MDBX indexing
//!
//! This module provides a Reth ExEx that processes chain notifications in real-time,
//! writing indexed data directly to MDBX without RPC overhead.
//!
//! # Architecture
//!
//! The ExEx runs inside the Reth process and receives notifications for:
//! - ChainCommitted: New blocks have been finalized
//! - ChainReorged: Chain reorganization occurred
//! - ChainReverted: Blocks have been reverted
//!
//! # Usage
//!
//! ```ignore
//! use blockscout_exex::exex::blockscout_exex;
//! use std::path::PathBuf;
//!
//! // In your Reth node builder:
//! let mdbx_path = PathBuf::from("/path/to/mdbx");
//! node.install_exex("blockscout", move |ctx| async move {
//!     blockscout_exex(ctx, &mdbx_path).await
//! });
//! ```

#[cfg(feature = "exex")]
mod inner {
    use std::path::Path;
    use std::sync::Arc;

    use alloy_consensus::transaction::SignerRecoverable;
    use alloy_consensus::Transaction as AlloyTx;
    use alloy_primitives::B256;
    use eyre::Result;
    use futures::StreamExt;
    use reth_exex::{ExExContext, ExExEvent, ExExNotification};
    use reth_node_api::FullNodeComponents;
    use reth_primitives::EthPrimitives;
    use tracing::{debug, error, info, warn};

    use crate::index_trait::{IndexDatabase, TokenTransfer};
    use crate::mdbx_index::MdbxIndex;
    use crate::transform::{decode_token_transfer, DecodedTransfer, TokenType};

    /// Blockscout ExEx for live MDBX indexing
    pub struct BlockscoutExEx<Node: FullNodeComponents> {
        /// ExEx context for receiving notifications
        ctx: ExExContext<Node>,
        /// MDBX index for writing data
        index: Arc<MdbxIndex>,
        /// Last successfully processed block
        #[allow(dead_code)]
        last_block: Option<u64>,
    }

    impl<Node> BlockscoutExEx<Node>
    where
        Node: FullNodeComponents<Types: reth_node_types::NodeTypes<Primitives = EthPrimitives>>,
    {
        /// Create a new BlockscoutExEx
        pub async fn new(ctx: ExExContext<Node>, mdbx_path: &Path) -> Result<Self> {
            info!("Initializing BlockscoutExEx with MDBX at {:?}", mdbx_path);

            let index = Arc::new(MdbxIndex::open(mdbx_path)?);
            let last_block = index.last_indexed_block().await?;

            info!("BlockscoutExEx initialized, last indexed block: {:?}", last_block);

            Ok(Self {
                ctx,
                index,
                last_block,
            })
        }

        /// Run the ExEx main loop
        pub async fn run(mut self) -> Result<()> {
            info!("BlockscoutExEx starting main loop");

            while let Some(notification) = self.ctx.notifications.next().await {
                match notification {
                    Ok(ExExNotification::ChainCommitted { new }) => {
                        let tip = new.tip();
                        let block_num = tip.number;
                        let block_hash = tip.hash();

                        debug!(block = block_num, hash = ?block_hash, "Processing ChainCommitted");

                        if let Err(e) = self.process_committed_chain(&new).await {
                            error!(block = block_num, error = ?e, "Failed to process committed chain");
                            continue;
                        }

                        // Notify reth that we've processed up to this height
                        if let Err(e) = self.ctx.events.send(ExExEvent::FinishedHeight(tip.num_hash())) {
                            error!(error = ?e, "Failed to send FinishedHeight event");
                        }

                        info!(block = block_num, "Indexed block via ExEx");
                    }

                    Ok(ExExNotification::ChainReorged { old, new }) => {
                        let old_tip = old.tip().number;
                        let new_tip = new.tip().number;

                        warn!(
                            old_tip = old_tip,
                            new_tip = new_tip,
                            "Chain reorg detected"
                        );

                        // Revert old blocks (log only for now)
                        if let Err(e) = self.process_reverted_chain(&old).await {
                            error!(error = ?e, "Failed to revert old chain");
                        }

                        // Process new blocks
                        if let Err(e) = self.process_committed_chain(&new).await {
                            error!(error = ?e, "Failed to process new chain after reorg");
                        }

                        let tip = new.tip();
                        if let Err(e) = self.ctx.events.send(ExExEvent::FinishedHeight(tip.num_hash())) {
                            error!(error = ?e, "Failed to send FinishedHeight event");
                        }
                    }

                    Ok(ExExNotification::ChainReverted { old }) => {
                        let tip = old.tip().number;
                        warn!(block = tip, "Chain reverted");

                        if let Err(e) = self.process_reverted_chain(&old).await {
                            error!(error = ?e, "Failed to revert chain");
                        }
                    }

                    Err(e) => {
                        error!(error = ?e, "Error receiving notification");
                        continue;
                    }
                }
            }

            info!("BlockscoutExEx shutting down");
            Ok(())
        }

        /// Process a committed chain of blocks
        async fn process_committed_chain(
            &self,
            chain: &reth_execution_types::Chain<EthPrimitives>,
        ) -> Result<()> {
            let mut batch = self.index.write_batch();
            let mut total_txs = 0u64;
            let mut total_transfers = 0u64;

            for (block, receipts) in chain.blocks_and_receipts() {
                let header = block.header();
                let block_num = header.number;
                let timestamp = header.timestamp;
                let body = block.body();

                // Process each transaction
                for (tx_idx, tx) in body.transactions.iter().enumerate() {
                    let tx_hash = *tx.tx_hash();

                    // Recover signer from signed transaction
                    let from = match tx.recover_signer() {
                        Ok(addr) => addr,
                        Err(_) => continue, // Skip if we can't recover signer
                    };

                    // Get 'to' address from transaction
                    let to = AlloyTx::to(tx);

                    // Index address -> tx mappings
                    batch.insert_address_tx(from, tx_hash, block_num, tx_idx as u32);
                    batch.insert_tx_block(tx_hash, block_num);

                    if let Some(to_addr) = to {
                        batch.insert_address_tx(to_addr, tx_hash, block_num, tx_idx as u32);
                    }

                    total_txs += 1;
                }

                // Process receipts for token transfers
                for (receipt_idx, receipt) in receipts.iter().enumerate() {
                    for (log_idx, log) in receipt.logs.iter().enumerate() {
                        let transfers = decode_token_transfer(log);

                        for transfer in transfers {
                            let global_log_idx = (receipt_idx * 100 + log_idx) as u64;

                            // Get tx_hash from the transaction
                            let tx_hash = if receipt_idx < body.transactions.len() {
                                *body.transactions[receipt_idx].tx_hash()
                            } else {
                                B256::ZERO
                            };

                            Self::index_transfer(
                                &mut batch,
                                &transfer,
                                tx_hash,
                                block_num,
                                global_log_idx,
                                timestamp,
                            );

                            total_transfers += 1;
                        }
                    }
                }
            }

            // Commit the batch with last indexed block
            let tip = chain.tip().number;
            batch.commit(tip)?;

            debug!(
                block = tip,
                txs = total_txs,
                transfers = total_transfers,
                "Committed batch"
            );

            Ok(())
        }

        /// Process a reverted chain of blocks (undo operations)
        async fn process_reverted_chain(
            &self,
            _chain: &reth_execution_types::Chain<EthPrimitives>,
        ) -> Result<()> {
            // For now, log the revert but don't undo data
            // MDBX doesn't support efficient deletes in the same way
            // A full implementation would need to track reversible operations
            warn!("Chain revert detected - data may be stale until next full reindex");
            Ok(())
        }

        /// Index a single token transfer
        fn index_transfer(
            batch: &mut crate::mdbx_index::MdbxWriteBatch,
            transfer: &DecodedTransfer,
            tx_hash: B256,
            block_num: u64,
            log_idx: u64,
            timestamp: u64,
        ) {
            use alloy_primitives::TxHash;

            // Convert B256 to TxHash
            let tx_hash = TxHash::from(tx_hash.0);

            // Create TokenTransfer using the appropriate constructor
            let token_transfer = match transfer.token_type {
                TokenType::Erc20 => TokenTransfer::new(
                    tx_hash,
                    log_idx,
                    transfer.token_address,
                    transfer.from,
                    transfer.to,
                    transfer.value,
                    block_num,
                    timestamp,
                ),
                TokenType::Erc721 => TokenTransfer::new_erc721(
                    tx_hash,
                    log_idx,
                    transfer.token_address,
                    transfer.from,
                    transfer.to,
                    transfer.token_id.unwrap_or_default(),
                    block_num,
                    timestamp,
                ),
                TokenType::Erc1155 => TokenTransfer::new_erc1155(
                    tx_hash,
                    log_idx,
                    transfer.token_address,
                    transfer.from,
                    transfer.to,
                    transfer.token_id.unwrap_or_default(),
                    transfer.value,
                    block_num,
                    timestamp,
                ),
            };

            // Use the unified insert_transfer API which handles all indexing
            batch.insert_transfer(token_transfer);
        }
    }

    /// ExEx factory function for use with reth node builder
    ///
    /// # Example
    ///
    /// ```ignore
    /// use std::path::PathBuf;
    /// use blockscout_exex::exex::blockscout_exex;
    ///
    /// let mdbx_path = PathBuf::from("/path/to/mdbx");
    /// node.install_exex("blockscout", move |ctx| {
    ///     let path = mdbx_path.clone();
    ///     async move { blockscout_exex(ctx, &path).await }
    /// });
    /// ```
    pub async fn blockscout_exex<Node>(
        ctx: ExExContext<Node>,
        mdbx_path: &Path,
    ) -> Result<()>
    where
        Node: FullNodeComponents<Types: reth_node_types::NodeTypes<Primitives = EthPrimitives>>,
    {
        BlockscoutExEx::new(ctx, mdbx_path).await?.run().await
    }
}

#[cfg(feature = "exex")]
pub use inner::{blockscout_exex, BlockscoutExEx};

#[cfg(not(feature = "exex"))]
pub fn blockscout_exex() {
    panic!("ExEx feature not enabled. Build with --features exex");
}
