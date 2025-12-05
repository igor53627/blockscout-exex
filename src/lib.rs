//! Blockscout ExEx - Reth Execution Extension for Blockscout indexing
//!
//! This crate provides a sidecar index database for Blockscout-compatible queries.
//! Uses FoundationDB for distributed, scalable indexed data storage.

pub mod api;
pub mod buffer_pool;
pub mod cache;
#[cfg(feature = "fdb")]
pub mod fdb_index;
pub mod index_trait;
pub mod meili;
#[cfg(feature = "reth")]
pub mod mdbx_index;
#[cfg(feature = "reth")]
pub mod reth_reader;
pub mod rpc_executor;
pub mod transform;
pub mod websocket;

// Re-export commonly used types
pub use cache::{CacheEntry, CacheStats, LruCache, new_json_cache, new_bytes_cache, DEFAULT_MAX_SIZE};
#[cfg(feature = "fdb")]
pub use fdb_index::{FdbIndex, WriteBatch};
pub use index_trait::{IndexDatabase, TokenTransfer};
pub use rpc_executor::{RpcExecutor, RpcRequest, RpcResponse};
pub use transform::{decode_token_transfer, DecodedTransfer, TokenType};
