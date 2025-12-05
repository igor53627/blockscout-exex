# Benchmarking Guide

## Overview

This document describes the comprehensive benchmarking suite for comparing FoundationDB (FDB) and MDBX performance in the Blockscout indexer.

## Running Benchmarks

### Prerequisites

```bash
# Install Criterion benchmarking tool
cargo install cargo-criterion

# Ensure you have the required features enabled
cargo build --release --features mdbx
```

### Quick Start

```bash
# Run all benchmarks (FDB + MDBX)
cargo bench --bench fdb_vs_mdbx

# Run only MDBX benchmarks
cargo bench --bench fdb_vs_mdbx --no-default-features --features reth

# Run only FDB benchmarks
cargo bench --bench fdb_vs_mdbx --features fdb --no-default-features

# Run specific benchmark group
cargo bench --bench fdb_vs_mdbx -- backfill_speed
cargo bench --bench fdb_vs_mdbx -- api_latency
cargo bench --bench fdb_vs_mdbx -- concurrent_reads
cargo bench --bench fdb_vs_mdbx -- memory_usage
```

## Benchmark Categories

### 1. Backfill Speed

**What it measures**: Time to index 100 blocks with varying transaction counts per block.

**Command**:
```bash
cargo bench --bench fdb_vs_mdbx -- backfill_speed
```

**Metrics**:
- Blocks indexed per second
- Total time for 100 blocks
- Variation across different transaction densities (10, 50, 100 txs/block)

**Expected Results**:
| Database | Txs/Block | Speed | Improvement |
|----------|-----------|-------|-------------|
| FDB      | 10        | ~11 blocks/sec | Baseline |
| MDBX     | 10        | ~100 blocks/sec | **9.1x** |
| FDB      | 50        | ~8 blocks/sec  | Baseline |
| MDBX     | 50        | ~85 blocks/sec | **10.6x** |
| FDB      | 100       | ~5 blocks/sec  | Baseline |
| MDBX     | 100       | ~70 blocks/sec | **14x** |

### 2. API Latency

**What it measures**: Response time for `get_address_txs` queries.

**Command**:
```bash
cargo bench --bench fdb_vs_mdbx -- api_latency
```

**Metrics**:
- P50, P95, P99 latency
- Average response time
- Latency distribution

**Expected Results**:
| Database | P50 | P95 | P99 | Target |
|----------|-----|-----|-----|--------|
| FDB      | TBD | TBD | TBD | N/A    |
| MDBX     | < 10ms | < 30ms | **< 50ms** | **✓** |

### 3. Concurrent Reads

**What it measures**: Performance under parallel query load (100 simultaneous queries).

**Command**:
```bash
cargo bench --bench fdb_vs_mdbx -- concurrent_reads
```

**Metrics**:
- Total time for 100 parallel queries
- Queries per second (QPS)
- Throughput under load

**Expected Results**:
| Database | Total Time | QPS | Improvement |
|----------|-----------|-----|-------------|
| FDB      | TBD       | TBD | Baseline    |
| MDBX     | < 500ms   | > 200 | TBD       |

### 4. Write Throughput

**What it measures**: Token transfer indexing speed over 1000 blocks.

**Command**:
```bash
cargo bench --bench fdb_vs_mdbx -- write_throughput
```

**Metrics**:
- Transfers indexed per second
- Batch write performance
- Sustained write throughput

### 5. Memory Usage (Linux only)

**What it measures**: RSS (Resident Set Size) during 10k block indexing.

**Command**:
```bash
# Only available on Linux
cargo bench --bench fdb_vs_mdbx -- memory_usage
```

**Metrics**:
- Memory delta before/after indexing
- Peak memory usage
- Memory efficiency (MB per 1000 blocks)

## Interpreting Results

### Criterion Output

Criterion provides detailed statistical analysis:

```
backfill_speed/mdbx/50  time:   [1.1234 s 1.1567 s 1.1923 s]
                        change: [-89.432% -88.901% -88.345%] (p = 0.00 < 0.05)
                        Performance has improved.
```

**What this means**:
- **time**: [lower bound, estimate, upper bound] with 95% confidence
- **change**: Performance change vs. previous run (negative = improvement)
- **p-value**: Statistical significance (< 0.05 = significant)

### Performance Targets

| Metric | Target | Rationale |
|--------|--------|-----------|
| Backfill Speed | 100+ blocks/sec | Full mainnet sync in ~55 hours (vs. 20+ days with FDB) |
| API Latency P99 | < 50ms | Acceptable UX for block explorer |
| Concurrent QPS | > 200 | Support moderate production traffic |
| Memory Overhead | < 1GB | Efficient resource usage |

## Advanced Benchmarking

### Custom Benchmark Runs

#### Adjust Block Count

Edit `benches/fdb_vs_mdbx.rs`:

```rust
// Change this:
for block_num in start_block..start_block + 100 {

// To benchmark more blocks:
for block_num in start_block..start_block + 1000 {
```

#### Adjust Transaction Density

```rust
// Modify transaction counts per block
for txs_per_block in [10, 50, 100, 200, 500] {
```

#### Save Results for Comparison

```bash
# Baseline run
cargo bench --bench fdb_vs_mdbx --features fdb --no-default-features -- --save-baseline fdb-baseline

# MDBX comparison run
cargo bench --bench fdb_vs_mdbx --features reth --no-default-features -- --baseline fdb-baseline
```

### Profiling with Flamegraphs

Install flamegraph:
```bash
cargo install flamegraph
```

Profile specific benchmark:
```bash
cargo flamegraph --bench fdb_vs_mdbx -- --bench backfill_speed
```

### Export Results

Criterion stores results in `target/criterion/`. To export:

```bash
# Generate HTML reports
open target/criterion/report/index.html

# Export CSV data
cargo criterion --message-format=json > benchmark-results.json
```

## Production Load Testing

### Real-World Scenarios

#### Scenario 1: Full Mainnet Backfill

```bash
# Time a full backfill from block 0 to latest
time ./target/release/blockscout-backfill \
    --mdbx-path /tmp/test-mdbx \
    --rpc-url https://eth.llamarpc.com \
    --from-block 0 \
    --to-block 21000000 \
    --batch-size 100
```

**Expected Duration** (MDBX):
- 21M blocks ÷ 100 blocks/sec = 58.3 hours
- With network latency: ~72 hours

#### Scenario 2: API Load Test

Use `hey` for HTTP load testing:

```bash
# Install hey
go install github.com/rakyll/hey@latest

# Test address transactions endpoint
hey -n 10000 -c 100 -m GET \
    "http://localhost:4000/api/v2/addresses/0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb/transactions"

# Expected results (MDBX):
# - Success rate: 100%
# - P99 latency: < 50ms
# - Throughput: > 1000 RPS
```

#### Scenario 3: Sustained Write Load

```bash
# Simulate continuous indexing
while true; do
    ./target/release/blockscout-backfill \
        --mdbx-path /var/lib/blockscout/mdbx \
        --rpc-url http://localhost:8545 \
        --from-block $START_BLOCK \
        --batch-size 100
    START_BLOCK=$((START_BLOCK + 1000))
    sleep 1
done
```

Monitor with:
```bash
# Watch database size growth
watch -n 5 'du -sh /var/lib/blockscout/mdbx'

# Monitor resource usage
htop
```

## CI/CD Integration

### GitHub Actions Example

```yaml
name: Benchmarks

on:
  push:
    branches: [main]
  pull_request:

jobs:
  benchmark:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2

      - name: Install Rust
        uses: actions-rs/toolchain@v1
        with:
          profile: minimal
          toolchain: stable

      - name: Run benchmarks
        run: cargo bench --bench fdb_vs_mdbx --no-default-features --features reth

      - name: Store benchmark results
        uses: benchmark-action/github-action-benchmark@v1
        with:
          tool: 'criterion'
          output-file-path: target/criterion/output.txt
```

## Troubleshooting

### Benchmark Failures

#### "Database connection failed"

FDB benchmarks require FoundationDB to be running:

```bash
# Check FDB status
fdbcli --exec status

# If not running, skip FDB benchmarks
cargo bench --bench fdb_vs_mdbx --no-default-features --features reth
```

#### "Temporary directory permission denied"

MDBX benchmarks create temp directories:

```bash
# Ensure /tmp is writable
sudo chmod 1777 /tmp
```

#### High variance in results

System load affects benchmarks:

```bash
# Close unnecessary applications
# Disable CPU frequency scaling (Linux)
sudo cpupower frequency-set --governor performance

# Run benchmarks with higher sample size
cargo bench --bench fdb_vs_mdbx -- --sample-size 100
```

## Performance Regression Detection

### Set Performance Thresholds

Create `.cargo/config.toml`:

```toml
[bench]
criterion = { version = "0.5", features = ["html_reports"] }

[bench.thresholds]
backfill_speed = { max_regression = 0.10 }  # Fail if > 10% slower
api_latency = { max_regression = 0.05 }     # Fail if > 5% slower
```

### Automated Checks

```bash
#!/bin/bash
# benchmark-check.sh

THRESHOLD=0.10  # 10% regression threshold

# Run benchmarks and save baseline
cargo bench --bench fdb_vs_mdbx -- --save-baseline current

# Compare with previous baseline
cargo bench --bench fdb_vs_mdbx -- --baseline previous

# Check for regressions
if [ $? -ne 0 ]; then
    echo "Performance regression detected!"
    exit 1
fi
```

## Reference

### Benchmark Configuration

Criterion settings in `benches/fdb_vs_mdbx.rs`:

```rust
let mut group = c.benchmark_group("backfill_speed");
group.measurement_time(Duration::from_secs(30));  // 30s per benchmark
group.sample_size(10);                             // 10 samples
group.warm_up_time(Duration::from_secs(5));       // 5s warmup
```

### Environment Variables

```bash
# Increase measurement time for more stable results
CRITERION_MEASUREMENT_TIME=60 cargo bench

# Save baseline with custom name
CRITERION_BASELINE=my-baseline cargo bench

# Export detailed CSV results
CRITERION_CSV=1 cargo bench
```

---

**Last Updated**: 2024-12-05
**Version**: 1.0
**Related**: [DEPLOYMENT.md](DEPLOYMENT.md), [README.md](README.md)
