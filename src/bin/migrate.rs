//! Migration tool - migrates data from FoundationDB to MDBX
//!
//! Features:
//! - Stream data from FDB in batches
//! - Write to MDBX using batch operations
//! - Checkpoint progress after each batch
//! - Resume from last checkpoint
//! - Validate data integrity after migration

use std::path::PathBuf;
use clap::Parser;
use eyre::Result;
use tracing::{info, warn};

#[cfg(feature = "mdbx")]
use blockscout_exex::MdbxIndex;

#[derive(Parser, Debug)]
#[command(name = "blockscout-migrate")]
#[command(about = "Migrate data from FoundationDB to MDBX")]
pub struct Args {
    /// FDB cluster file path (uses default if not specified)
    #[arg(long)]
    pub fdb_cluster_file: Option<PathBuf>,

    /// MDBX database path (destination)
    #[arg(long)]
    pub mdbx_path: PathBuf,

    /// Batch size for streaming (entries per transaction)
    #[arg(long, default_value = "10000")]
    pub batch_size: usize,

    /// Resume from last checkpoint
    #[arg(long)]
    pub resume: bool,

    /// Validate data after migration
    #[arg(long)]
    pub validate: bool,
}

// ============================================================================
// Checkpoint System (Subtask 9.2)
// ============================================================================

/// Migration checkpoint for resume capability
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationCheckpoint {
    pub table_name: String,
    pub last_key: Vec<u8>,
    pub rows_migrated: u64,
}

impl MigrationCheckpoint {
    /// Create a new checkpoint
    pub fn new(table_name: String, last_key: Vec<u8>, rows_migrated: u64) -> Self {
        Self {
            table_name,
            last_key,
            rows_migrated,
        }
    }

    /// Encode checkpoint to string format: "table_name:hex_last_key:rows_migrated"
    pub fn encode(&self) -> String {
        format!(
            "{}:{}:{}",
            self.table_name,
            hex::encode(&self.last_key),
            self.rows_migrated
        )
    }

    /// Decode checkpoint from string format
    pub fn decode(s: &str) -> Result<Self> {
        let parts: Vec<&str> = s.split(':').collect();
        if parts.len() != 3 {
            return Err(eyre::eyre!("Invalid checkpoint format: expected 3 parts, got {}", parts.len()));
        }

        let table_name = parts[0].to_string();
        let last_key = hex::decode(parts[1])?;
        let rows_migrated = parts[2].parse::<u64>()?;

        Ok(Self {
            table_name,
            last_key,
            rows_migrated,
        })
    }
}

const CHECKPOINT_KEY: &[u8] = b"migration_checkpoint";

// ============================================================================
// Table Migration Logic (Subtask 9.3)
// ============================================================================

/// Table names in migration order (dependencies first)
const MIGRATION_TABLES: &[&str] = &[
    "Metadata",          // Schema version, counters
    "TxBlocks",          // tx_hash -> block (no deps)
    "AddressTxs",        // address -> txs (depends on TxBlocks)
    "AddressTransfers",  // address -> transfers (depends on TxBlocks)
    "TokenTransfers",    // token -> transfers (depends on TxBlocks)
    "TokenHolders",      // token -> holders (depends on TokenTransfers)
    "AddressCounters",   // per-address counters
    "TokenHolderCounts", // per-token holder counts
    "DailyMetrics",      // time-series metrics
];

/// Progress report for migration
#[derive(Debug, Clone)]
pub struct MigrationProgress {
    pub table_name: String,
    pub rows_migrated: u64,
    pub total_rows: Option<u64>, // None if unknown
}

impl MigrationProgress {
    pub fn new(table_name: String, rows_migrated: u64) -> Self {
        Self {
            table_name,
            rows_migrated,
            total_rows: None,
        }
    }

    pub fn percentage(&self) -> Option<f64> {
        self.total_rows.map(|total| {
            if total == 0 {
                100.0
            } else {
                (self.rows_migrated as f64 / total as f64) * 100.0
            }
        })
    }
}

/// Save checkpoint to MDBX metadata table
#[cfg(feature = "mdbx")]
pub fn save_checkpoint(_mdbx: &MdbxIndex, checkpoint: &MigrationCheckpoint) -> Result<()> {
    // TODO: Implement actual MDBX write in future iteration
    // For now, just validate encoding/decoding works
    let _encoded = checkpoint.encode();
    Ok(())
}

#[cfg(not(feature = "mdbx"))]
pub fn save_checkpoint(_mdbx: &(), _checkpoint: &MigrationCheckpoint) -> Result<()> {
    Ok(())
}

/// Load checkpoint from MDBX metadata table
#[cfg(feature = "mdbx")]
pub fn load_checkpoint(_mdbx: &MdbxIndex) -> Result<Option<MigrationCheckpoint>> {
    // TODO: Implement actual MDBX read in future iteration
    // For now, return None (no checkpoint)
    Ok(None)
}

#[cfg(not(feature = "mdbx"))]
pub fn load_checkpoint(_mdbx: &()) -> Result<Option<MigrationCheckpoint>> {
    Ok(None)
}

/// Clear checkpoint from MDBX metadata table
#[cfg(feature = "mdbx")]
pub fn clear_checkpoint(_mdbx: &MdbxIndex) -> Result<()> {
    // TODO: Implement actual MDBX delete in future iteration
    Ok(())
}

#[cfg(not(feature = "mdbx"))]
pub fn clear_checkpoint(_mdbx: &()) -> Result<()> {
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"))
        )
        .init();

    let args = Args::parse();

    info!("Starting migration from FoundationDB to MDBX");
    info!("  FDB cluster file: {:?}", args.fdb_cluster_file);
    info!("  MDBX path: {:?}", args.mdbx_path);
    info!("  Batch size: {}", args.batch_size);
    info!("  Resume: {}", args.resume);
    info!("  Validate: {}", args.validate);

    // Validate MDBX path exists and is writable
    if !args.mdbx_path.exists() {
        warn!("MDBX path does not exist, will be created: {:?}", args.mdbx_path);
    }

    #[cfg(feature = "fdb")]
    {
        // Initialize FDB network
        let _network = unsafe { blockscout_exex::fdb_index::init_fdb_network() };

        // Open FDB connection
        let fdb = if let Some(ref cluster_file) = args.fdb_cluster_file {
            info!("Opening FDB with cluster file: {:?}", cluster_file);
            blockscout_exex::FdbIndex::open(cluster_file)?
        } else {
            info!("Opening FDB with default cluster file");
            blockscout_exex::FdbIndex::open_default()?
        };

        info!("FDB connection established");

        // Open MDBX connection
        #[cfg(feature = "mdbx")]
        {
            let mdbx = blockscout_exex::MdbxIndex::open(&args.mdbx_path)?;
            info!("MDBX connection established");

            // TODO: Implement migration logic in subsequent subtasks
            info!("Migration logic not yet implemented (see subtask 9.2-9.5)");

            Ok(())
        }

        #[cfg(not(feature = "mdbx"))]
        {
            Err(eyre::eyre!("MDBX support not enabled. Compile with --features mdbx"))
        }
    }

    #[cfg(not(feature = "fdb"))]
    {
        Err(eyre::eyre!("FDB support not enabled. Compile with --features fdb"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_args_parsing_minimal() {
        // Test minimal required arguments
        let args = Args::try_parse_from(&[
            "migrate",
            "--mdbx-path", "/tmp/test.mdbx"
        ]).unwrap();

        assert_eq!(args.mdbx_path, PathBuf::from("/tmp/test.mdbx"));
        assert_eq!(args.batch_size, 10000);
        assert!(!args.resume);
        assert!(!args.validate);
        assert!(args.fdb_cluster_file.is_none());
    }

    #[test]
    fn test_args_parsing_full() {
        // Test all arguments
        let args = Args::try_parse_from(&[
            "migrate",
            "--fdb-cluster-file", "/etc/foundationdb/fdb.cluster",
            "--mdbx-path", "/data/blockscout.mdbx",
            "--batch-size", "5000",
            "--resume",
            "--validate"
        ]).unwrap();

        assert_eq!(args.fdb_cluster_file, Some(PathBuf::from("/etc/foundationdb/fdb.cluster")));
        assert_eq!(args.mdbx_path, PathBuf::from("/data/blockscout.mdbx"));
        assert_eq!(args.batch_size, 5000);
        assert!(args.resume);
        assert!(args.validate);
    }

    #[test]
    fn test_args_parsing_missing_mdbx_path() {
        // Test that mdbx-path is required
        let result = Args::try_parse_from(&[
            "migrate"
        ]);

        assert!(result.is_err());
    }

    #[test]
    fn test_args_parsing_default_batch_size() {
        // Test default batch size
        let args = Args::try_parse_from(&[
            "migrate",
            "--mdbx-path", "/tmp/test.mdbx"
        ]).unwrap();

        assert_eq!(args.batch_size, 10000);
    }

    #[test]
    fn test_args_parsing_custom_batch_size() {
        // Test custom batch size
        let args = Args::try_parse_from(&[
            "migrate",
            "--mdbx-path", "/tmp/test.mdbx",
            "--batch-size", "1000"
        ]).unwrap();

        assert_eq!(args.batch_size, 1000);
    }

    // ========================================================================
    // Checkpoint System Tests (Subtask 9.2)
    // ========================================================================

    #[test]
    fn test_checkpoint_encode_decode() {
        // Test encoding and decoding checkpoint
        let checkpoint = MigrationCheckpoint::new(
            "AddressTxs".to_string(),
            vec![0x01, 0x02, 0x03, 0x04],
            12345,
        );

        let encoded = checkpoint.encode();
        assert_eq!(encoded, "AddressTxs:01020304:12345");

        let decoded = MigrationCheckpoint::decode(&encoded).unwrap();
        assert_eq!(decoded, checkpoint);
    }

    #[test]
    fn test_checkpoint_decode_empty_key() {
        // Test decoding checkpoint with empty key
        let encoded = "TokenHolders::999";
        let decoded = MigrationCheckpoint::decode(encoded).unwrap();

        assert_eq!(decoded.table_name, "TokenHolders");
        assert_eq!(decoded.last_key, Vec::<u8>::new());
        assert_eq!(decoded.rows_migrated, 999);
    }

    #[test]
    fn test_checkpoint_decode_long_key() {
        // Test decoding checkpoint with long key (address + block + log_idx)
        let long_key = vec![0xAB; 36]; // 36 bytes for AddressTransferKey
        let checkpoint = MigrationCheckpoint::new(
            "AddressTransfers".to_string(),
            long_key.clone(),
            5000,
        );

        let encoded = checkpoint.encode();
        let decoded = MigrationCheckpoint::decode(&encoded).unwrap();

        assert_eq!(decoded.table_name, "AddressTransfers");
        assert_eq!(decoded.last_key, long_key);
        assert_eq!(decoded.rows_migrated, 5000);
    }

    #[test]
    fn test_checkpoint_decode_invalid_format() {
        // Test decoding with too few parts
        let result = MigrationCheckpoint::decode("invalid");
        assert!(result.is_err());

        // Test decoding with too many parts
        let result = MigrationCheckpoint::decode("table:key:123:extra");
        assert!(result.is_err());
    }

    #[test]
    fn test_checkpoint_decode_invalid_hex() {
        // Test decoding with invalid hex
        let result = MigrationCheckpoint::decode("table:ZZZZ:123");
        assert!(result.is_err());
    }

    #[test]
    fn test_checkpoint_decode_invalid_count() {
        // Test decoding with invalid row count
        let result = MigrationCheckpoint::decode("table:abcd:not_a_number");
        assert!(result.is_err());
    }

    #[test]
    fn test_save_load_checkpoint_no_mdbx() {
        // Test save/load without MDBX (should not panic)
        let checkpoint = MigrationCheckpoint::new(
            "TestTable".to_string(),
            vec![1, 2, 3],
            100,
        );

        #[cfg(not(feature = "mdbx"))]
        {
            let mdbx = ();
            save_checkpoint(&mdbx, &checkpoint).unwrap();
            let loaded = load_checkpoint(&mdbx).unwrap();
            assert!(loaded.is_none());
        }
    }

    // ========================================================================
    // Migration Logic Tests (Subtask 9.3)
    // ========================================================================

    #[test]
    fn test_migration_tables_order() {
        // Test that tables are in correct dependency order
        assert_eq!(MIGRATION_TABLES[0], "Metadata");
        assert_eq!(MIGRATION_TABLES[1], "TxBlocks");
        assert_eq!(MIGRATION_TABLES[2], "AddressTxs");
        assert_eq!(MIGRATION_TABLES.len(), 9);
    }

    #[test]
    fn test_migration_progress_new() {
        let progress = MigrationProgress::new("TestTable".to_string(), 1000);
        assert_eq!(progress.table_name, "TestTable");
        assert_eq!(progress.rows_migrated, 1000);
        assert_eq!(progress.total_rows, None);
        assert_eq!(progress.percentage(), None);
    }

    #[test]
    fn test_migration_progress_percentage() {
        let mut progress = MigrationProgress::new("TestTable".to_string(), 5000);
        progress.total_rows = Some(10000);

        let pct = progress.percentage().unwrap();
        assert!((pct - 50.0).abs() < 0.01); // 50%
    }

    #[test]
    fn test_migration_progress_percentage_zero_total() {
        let mut progress = MigrationProgress::new("TestTable".to_string(), 0);
        progress.total_rows = Some(0);

        let pct = progress.percentage().unwrap();
        assert_eq!(pct, 100.0); // Empty table = 100% complete
    }

    #[test]
    fn test_migration_progress_percentage_complete() {
        let mut progress = MigrationProgress::new("TestTable".to_string(), 10000);
        progress.total_rows = Some(10000);

        let pct = progress.percentage().unwrap();
        assert_eq!(pct, 100.0);
    }
}
