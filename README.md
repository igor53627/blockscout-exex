# blockscout-exex

A Blockscout-compatible indexer that creates a sidecar index database for fast address-based queries.

## Architecture

```
┌─────────────────────────────────────────────────────┐
│              Ethereum Node (Reth/Geth)              │
│                      JSON-RPC                       │
└───────────────────────┬─────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────┐
│              blockscout-backfill                    │
│         (indexes historical blocks)                 │
└───────────────────────┬─────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────┐
│                  Index DB (redb)                    │
│                                                     │
│  address → txs     │  address → transfers          │
│  token → holders   │  tx → block                   │
└───────────────────────┬─────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────┐
│               blockscout-api                        │
│        (Blockscout-compatible endpoints)            │
└─────────────────────────────────────────────────────┘
```

## Features

- **Standalone**: No reth dependency required (uses JSON-RPC)
- **Fast backfill**: Indexes blocks in batches
- **redb storage**: Pure Rust embedded database (no FFI)
- **Blockscout-compatible API**: Drop-in replacement for indexed endpoints

## Indexed Data

| Table | Description |
|-------|-------------|
| `address_txs` | All transactions involving an address |
| `address_transfers` | ERC20/721 transfers for an address |
| `token_holders` | Holder balances per token |
| `tx_block` | Transaction → block number mapping |

## Usage

### 1. Build

```bash
cargo build --release
```

### 2. Backfill from JSON-RPC

```bash
# Index from block 0 to latest
./target/release/blockscout-backfill \
  --rpc-url http://localhost:8545 \
  --index-path ./blockscout-index

# Resume from last indexed block
./target/release/blockscout-backfill \
  --rpc-url http://localhost:8545 \
  --index-path ./blockscout-index

# Index specific range
./target/release/blockscout-backfill \
  --rpc-url http://localhost:8545 \
  --index-path ./blockscout-index \
  --from-block 1000000 \
  --to-block 2000000
```

### 3. Start API Server

```bash
./target/release/blockscout-api \
  --index-path ./blockscout-index \
  --port 3000
```

Or use the combined CLI:

```bash
./target/release/blockscout-exex api \
  --index-path ./blockscout-index \
  --port 3000
```

## API Endpoints

```
GET /api/v2/addresses/:hash/transactions
GET /api/v2/addresses/:hash/token-transfers  
GET /api/v2/tokens/:hash/holders
GET /api/v2/main-page/indexing-status
GET /health
```

### Query Parameters

- `limit` (default: 50): Number of items per page
- `offset` (default: 0): Pagination offset

### Example

```bash
curl "http://localhost:3000/api/v2/addresses/0x.../transactions?limit=10"
```

## Performance

### Backfill Speed

Speed depends on:
- JSON-RPC latency (local node is fastest)
- Network bandwidth
- Disk I/O

| Setup | Blocks/sec |
|-------|------------|
| Local Reth node | ~500-1000 |
| Remote RPC | ~50-200 |

### Tips

1. **Use local node**: Much faster than remote RPC
2. **Increase batch size**: `--batch-size 5000` for faster commits
3. **SSD/NVMe**: Recommended for index database
4. **Parallel instances**: Run multiple instances for different block ranges

## Configuration

| Flag | Description | Default |
|------|-------------|---------|
| `--rpc-url` | Ethereum JSON-RPC endpoint | `http://localhost:8545` |
| `--index-path` | Path to index database | `./blockscout-index` |
| `--from-block` | Starting block (0 = resume) | `0` |
| `--to-block` | Ending block (0 = latest) | `0` |
| `--batch-size` | Blocks per commit | `1000` |
| `--port` | API server port | `3000` |
| `--host` | API server host | `0.0.0.0` |

## Development

```bash
# Run tests
cargo test

# Check formatting
cargo fmt --check

# Run clippy
cargo clippy

# Build release
cargo build --release
```

## Future Work

- [ ] Live updates via WebSocket subscription
- [ ] Reth ExEx integration for in-process indexing
- [ ] Internal transactions (requires debug_traceBlock)
- [ ] Contract verification storage
- [ ] Search functionality

## License

MIT
