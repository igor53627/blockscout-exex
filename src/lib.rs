//! Blockscout ExEx - Reth Execution Extension for Blockscout indexing
//!
//! This crate provides a sidecar index database for Blockscout-compatible queries.
//! Uses MDBX for fast, embedded key-value storage.

pub mod api;
pub mod buffer_pool;
pub mod cache;
#[cfg(feature = "exex")]
pub mod exex;
pub mod index_trait;
pub mod meili;
#[cfg(feature = "mdbx")]
pub mod mdbx_index;
#[cfg(feature = "reth")]
pub mod reth_reader;
pub mod rpc_executor;
pub mod transform;
pub mod websocket;

// Re-export commonly used types
pub use cache::{CacheEntry, CacheStats, LruCache, new_json_cache, new_bytes_cache, DEFAULT_MAX_SIZE};
pub use index_trait::{IndexDatabase, TokenTransfer};
#[cfg(feature = "mdbx")]
pub use mdbx_index::MdbxIndex;
pub use rpc_executor::{RpcExecutor, RpcRequest, RpcResponse};
pub use transform::{decode_token_transfer, DecodedTransfer, TokenType};
