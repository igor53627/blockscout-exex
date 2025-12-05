//! Comprehensive benchmarks comparing FoundationDB vs MDBX performance
//!
//! This benchmark suite validates the performance improvements of MDBX over FDB:
//! - Backfill speed: Target 100+ blocks/sec vs 11 blocks/sec baseline
//! - API latency: Target p99 < 50ms
//! - Concurrent reads: Test parallel query performance
//! - Memory usage: Track RSS during operations
//!
//! Run with: cargo bench --bench fdb_vs_mdbx

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, black_box};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

use alloy_primitives::{Address, TxHash, U256};
use blockscout_exex::index_trait::{IndexDatabase, TokenTransfer};

#[cfg(feature = "fdb")]
use blockscout_exex::fdb_index::FdbIndex;

#[cfg(feature = "reth")]
use blockscout_exex::mdbx_index::MdbxIndex;

// ============================================================================
// Helper Functions
// ============================================================================

/// Generate deterministic test data for a block
fn generate_block_data(block_num: u64, txs_per_block: usize) -> Vec<(TxHash, Address, u32)> {
    (0..txs_per_block)
        .map(|tx_idx| {
            let tx_hash_bytes = [
                (block_num >> 24) as u8,
                (block_num >> 16) as u8,
                (block_num >> 8) as u8,
                block_num as u8,
                (tx_idx >> 24) as u8,
                (tx_idx >> 16) as u8,
                (tx_idx >> 8) as u8,
                tx_idx as u8,
            ];
            let mut full_hash = [0u8; 32];
            full_hash[..8].copy_from_slice(&tx_hash_bytes);
            let tx_hash = TxHash::from(full_hash);

            // Generate address from block and tx_idx
            let mut addr_bytes = [0u8; 20];
            addr_bytes[0] = (block_num % 100) as u8; // Reuse addresses
            addr_bytes[1] = (tx_idx % 256) as u8;
            let address = Address::from(addr_bytes);

            (tx_hash, address, tx_idx as u32)
        })
        .collect()
}

/// Generate token transfer data
fn generate_transfer_data(
    block_num: u64,
    transfers_per_block: usize,
) -> Vec<TokenTransfer> {
    (0..transfers_per_block)
        .map(|log_idx| {
            let mut tx_hash = [0u8; 32];
            tx_hash[0] = (block_num >> 8) as u8;
            tx_hash[1] = block_num as u8;
            tx_hash[2] = (log_idx >> 8) as u8;
            tx_hash[3] = log_idx as u8;

            let mut token = [0u8; 20];
            token[0] = (block_num % 10) as u8; // 10 different tokens

            let mut from = [0u8; 20];
            from[0] = (log_idx % 50) as u8;

            let mut to = [0u8; 20];
            to[0] = ((log_idx + 1) % 50) as u8;

            let mut value = [0u8; 32];
            value[31] = 1; // Transfer 1 wei

            TokenTransfer {
                tx_hash,
                log_index: log_idx as u64,
                token_address: token,
                from,
                to,
                value,
                block_number: block_num,
                timestamp: 1700000000 + block_num * 12,
                token_type: 0, // ERC-20
                token_id: None,
            }
        })
        .collect()
}

// ============================================================================
// FDB Benchmarks (Baseline)
// ============================================================================

#[cfg(feature = "fdb")]
async fn setup_fdb() -> Arc<FdbIndex> {
    let cluster_file = std::env::var("FDB_CLUSTER_FILE")
        .unwrap_or_else(|_| "/etc/foundationdb/fdb.cluster".to_string());

    Arc::new(FdbIndex::new(&cluster_file).await.expect("Failed to create FDB index"))
}

#[cfg(feature = "fdb")]
fn bench_fdb_backfill_speed(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let index = rt.block_on(setup_fdb());

    let mut group = c.benchmark_group("backfill_speed");
    group.measurement_time(Duration::from_secs(30));

    // Benchmark: Index 100 blocks with varying transaction counts
    for txs_per_block in [10, 50, 100] {
        group.bench_with_input(
            BenchmarkId::new("fdb", txs_per_block),
            &txs_per_block,
            |b, &txs| {
                b.iter(|| {
                    rt.block_on(async {
                        let start_block = 1_000_000u64;
                        for block_num in start_block..start_block + 100 {
                            let txs_data = generate_block_data(block_num, txs);
                            for (tx_hash, address, tx_idx) in txs_data {
                                index.index_address_tx(
                                    black_box(address),
                                    black_box(block_num),
                                    black_box(tx_idx),
                                    black_box(tx_hash),
                                ).await.expect("Failed to index tx");
                            }
                        }
                    });
                });
            },
        );
    }
    group.finish();
}

#[cfg(feature = "fdb")]
fn bench_fdb_api_latency(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let index = rt.block_on(setup_fdb());

    // Pre-populate with test data
    rt.block_on(async {
        let test_addr = Address::from([1u8; 20]);
        for block in 0..1000 {
            for tx_idx in 0..10 {
                let tx_hash = TxHash::from([block as u8; 32]);
                index.index_address_tx(test_addr, block, tx_idx, tx_hash)
                    .await
                    .expect("Failed to index");
            }
        }
    });

    let mut group = c.benchmark_group("api_latency");
    group.measurement_time(Duration::from_secs(20));

    let test_addr = Address::from([1u8; 20]);

    group.bench_function("fdb_get_address_txs", |b| {
        b.iter(|| {
            rt.block_on(async {
                index.get_address_txs(black_box(test_addr), black_box(50), black_box(0))
                    .await
                    .expect("Failed to get txs")
            })
        });
    });

    group.finish();
}

#[cfg(feature = "fdb")]
fn bench_fdb_concurrent_reads(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let index = rt.block_on(setup_fdb());

    // Pre-populate with test data for 100 addresses
    rt.block_on(async {
        for addr_id in 0..100 {
            let mut addr_bytes = [0u8; 20];
            addr_bytes[0] = addr_id;
            let test_addr = Address::from(addr_bytes);

            for block in 0..100 {
                let tx_hash = TxHash::from([addr_id; 32]);
                index.index_address_tx(test_addr, block, 0, tx_hash)
                    .await
                    .expect("Failed to index");
            }
        }
    });

    let mut group = c.benchmark_group("concurrent_reads");
    group.measurement_time(Duration::from_secs(20));

    group.bench_function("fdb_100_parallel_queries", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut handles = Vec::new();

                for addr_id in 0..100 {
                    let index = Arc::clone(&index);
                    let handle = tokio::spawn(async move {
                        let mut addr_bytes = [0u8; 20];
                        addr_bytes[0] = addr_id;
                        let addr = Address::from(addr_bytes);

                        index.get_address_txs(addr, 10, 0)
                            .await
                            .expect("Failed to get txs")
                    });
                    handles.push(handle);
                }

                for handle in handles {
                    handle.await.expect("Task failed");
                }
            })
        });
    });

    group.finish();
}

// ============================================================================
// MDBX Benchmarks (Target)
// ============================================================================

#[cfg(feature = "reth")]
async fn setup_mdbx() -> Arc<MdbxIndex> {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    Arc::new(MdbxIndex::new(temp_dir.path()).await.expect("Failed to create MDBX index"))
}

#[cfg(feature = "reth")]
fn bench_mdbx_backfill_speed(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("backfill_speed");
    group.measurement_time(Duration::from_secs(30));

    // Benchmark: Index 100 blocks with varying transaction counts
    for txs_per_block in [10, 50, 100] {
        group.bench_with_input(
            BenchmarkId::new("mdbx", txs_per_block),
            &txs_per_block,
            |b, &txs| {
                b.iter(|| {
                    // Create fresh index for each iteration
                    let index = rt.block_on(setup_mdbx());

                    rt.block_on(async {
                        let start_block = 1_000_000u64;
                        for block_num in start_block..start_block + 100 {
                            let txs_data = generate_block_data(block_num, txs);
                            for (tx_hash, address, tx_idx) in txs_data {
                                index.index_address_tx(
                                    black_box(address),
                                    black_box(block_num),
                                    black_box(tx_idx),
                                    black_box(tx_hash),
                                ).await.expect("Failed to index tx");
                            }
                        }
                    });
                });
            },
        );
    }
    group.finish();
}

#[cfg(feature = "reth")]
fn bench_mdbx_api_latency(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let index = rt.block_on(setup_mdbx());

    // Pre-populate with test data
    rt.block_on(async {
        let test_addr = Address::from([1u8; 20]);
        for block in 0..1000 {
            for tx_idx in 0..10 {
                let tx_hash = TxHash::from([block as u8; 32]);
                index.index_address_tx(test_addr, block, tx_idx, tx_hash)
                    .await
                    .expect("Failed to index");
            }
        }
    });

    let mut group = c.benchmark_group("api_latency");
    group.measurement_time(Duration::from_secs(20));

    let test_addr = Address::from([1u8; 20]);

    group.bench_function("mdbx_get_address_txs", |b| {
        b.iter(|| {
            rt.block_on(async {
                index.get_address_txs(black_box(test_addr), black_box(50), black_box(0))
                    .await
                    .expect("Failed to get txs")
            })
        });
    });

    group.finish();
}

#[cfg(feature = "reth")]
fn bench_mdbx_concurrent_reads(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let index = rt.block_on(setup_mdbx());

    // Pre-populate with test data for 100 addresses
    rt.block_on(async {
        for addr_id in 0..100 {
            let mut addr_bytes = [0u8; 20];
            addr_bytes[0] = addr_id;
            let test_addr = Address::from(addr_bytes);

            for block in 0..100 {
                let tx_hash = TxHash::from([addr_id; 32]);
                index.index_address_tx(test_addr, block, 0, tx_hash)
                    .await
                    .expect("Failed to index");
            }
        }
    });

    let mut group = c.benchmark_group("concurrent_reads");
    group.measurement_time(Duration::from_secs(20));

    group.bench_function("mdbx_100_parallel_queries", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut handles = Vec::new();

                for addr_id in 0..100 {
                    let index = Arc::clone(&index);
                    let handle = tokio::spawn(async move {
                        let mut addr_bytes = [0u8; 20];
                        addr_bytes[0] = addr_id;
                        let addr = Address::from(addr_bytes);

                        index.get_address_txs(addr, 10, 0)
                            .await
                            .expect("Failed to get txs")
                    });
                    handles.push(handle);
                }

                for handle in handles {
                    handle.await.expect("Task failed");
                }
            })
        });
    });

    group.finish();
}

#[cfg(feature = "reth")]
fn bench_mdbx_write_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("write_throughput");
    group.measurement_time(Duration::from_secs(30));

    group.bench_function("mdbx_token_transfers_1000_blocks", |b| {
        b.iter(|| {
            let index = rt.block_on(setup_mdbx());

            rt.block_on(async {
                for block_num in 0..1000 {
                    let transfers = generate_transfer_data(block_num, 50);
                    for transfer in transfers {
                        index.index_token_transfer(black_box(transfer))
                            .await
                            .expect("Failed to index transfer");
                    }
                }
            });
        });
    });

    group.finish();
}

// ============================================================================
// Memory Usage Tracking
// ============================================================================

#[cfg(all(feature = "reth", target_os = "linux"))]
fn get_rss_kb() -> usize {
    let pid = std::process::id();
    let status_path = format!("/proc/{}/status", pid);

    if let Ok(content) = std::fs::read_to_string(&status_path) {
        for line in content.lines() {
            if line.starts_with("VmRSS:") {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() >= 2 {
                    return parts[1].parse().unwrap_or(0);
                }
            }
        }
    }
    0
}

#[cfg(all(feature = "reth", target_os = "linux"))]
fn bench_memory_usage(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("memory_usage");
    group.measurement_time(Duration::from_secs(30));

    group.bench_function("mdbx_memory_10k_blocks", |b| {
        b.iter(|| {
            let index = rt.block_on(setup_mdbx());
            let rss_before = get_rss_kb();

            rt.block_on(async {
                for block_num in 0..10_000 {
                    let txs = generate_block_data(block_num, 20);
                    for (tx_hash, address, tx_idx) in txs {
                        index.index_address_tx(address, block_num, tx_idx, tx_hash)
                            .await
                            .expect("Failed to index");
                    }
                }
            });

            let rss_after = get_rss_kb();
            let rss_delta_mb = (rss_after.saturating_sub(rss_before)) as f64 / 1024.0;
            println!("Memory delta: {:.2} MB for 10k blocks", rss_delta_mb);
        });
    });

    group.finish();
}

// ============================================================================
// Benchmark Groups
// ============================================================================

#[cfg(feature = "fdb")]
criterion_group!(
    fdb_benches,
    bench_fdb_backfill_speed,
    bench_fdb_api_latency,
    bench_fdb_concurrent_reads
);

#[cfg(feature = "reth")]
criterion_group!(
    mdbx_benches,
    bench_mdbx_backfill_speed,
    bench_mdbx_api_latency,
    bench_mdbx_concurrent_reads,
    bench_mdbx_write_throughput
);

#[cfg(all(feature = "reth", target_os = "linux"))]
criterion_group!(memory_benches, bench_memory_usage);

// ============================================================================
// Main Entry Point
// ============================================================================

#[cfg(all(feature = "fdb", feature = "reth"))]
criterion_main!(fdb_benches, mdbx_benches);

#[cfg(all(feature = "fdb", not(feature = "reth")))]
criterion_main!(fdb_benches);

#[cfg(all(not(feature = "fdb"), feature = "reth", not(target_os = "linux")))]
criterion_main!(mdbx_benches);

#[cfg(all(not(feature = "fdb"), feature = "reth", target_os = "linux"))]
criterion_main!(mdbx_benches, memory_benches);

#[cfg(not(any(feature = "fdb", feature = "reth")))]
fn main() {
    println!("No database backend enabled. Enable 'fdb' or 'reth' feature to run benchmarks.");
}
