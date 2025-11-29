//! Blockscout ExEx - Reth Execution Extension for Blockscout indexing
//!
//! This crate provides a sidecar index database for Blockscout-compatible queries.
//! Uses FoundationDB for distributed, scalable indexed data storage.

pub mod api;
pub mod fdb_index;
#[cfg(feature = "reth")]
pub mod reth_reader;
pub mod transform;

// Re-export commonly used types
pub use fdb_index::{FdbIndex, TokenTransfer, WriteBatch};
