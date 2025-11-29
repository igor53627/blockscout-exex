# blockscout-exex

A high-performance Blockscout-compatible API server that reads directly from Reth's database — no JSON-RPC overhead. Uses FoundationDB for scalable indexed data storage.

## Architecture

```
┌─────────────────┐     ┌───────────────────────────────────────────────┐
│   Blockscout    │────▶│           blockscout-exex API                 │
│    Frontend     │     │                                               │
│    (Next.js)    │     │  ┌─────────────────┐    ┌──────────────────┐  │
└─────────────────┘     │  │  FoundationDB   │    │    reth MDBX     │  │
                        │  │                 │    │                  │  │
                        │  │  • addr → txs   │    │  • blocks        │  │
                        │  │  • addr → xfers │    │  • transactions  │  │
                        │  │  • token holders│    │  • balances      │  │
                        │  │  • tx → block   │    │  • state         │  │
                        │  └─────────────────┘    └──────────────────┘  │
                        └───────────────────────────────────────────────┘
                                    │                      │
                              TCP localhost           direct mmap
                              (no Unix socket)        (zero-copy)
```

## Current Status

| Component | Status | Notes |
|-----------|--------|-------|
| **API Server** | ✅ Working | Serves Blockscout v2 API |
| **FDB Integration** | ✅ Working | Replaced redb, TCP connection to fdbserver |
| **Backfill Tool** | ✅ Working | Indexes from JSON-RPC |
| **Subscriber** | ⚠️ Untested | Live block subscription via WebSocket |
| **Reth Direct Reads** | ✅ Working | Optional, requires `--features reth` |

### Why FoundationDB?

- **Multi-node scaling**: Can distribute indexed data across nodes
- **ACID transactions**: Strict serializability 
- **Battle-tested**: Used by Apple CloudKit, Snowflake
- **Ordered key-value**: Natural fit for blockchain data patterns

### Connection Details

- FDB client connects via **TCP** to `127.0.0.1:4500` (configurable in cluster file)
- Unix sockets not supported by FDB ([feature request exists](https://forums.foundationdb.org/t/feature-request-support-unix-domain-sockets-for-client-local-server-access/1071))
- For same-machine deployments, loopback TCP adds ~1-2μs overhead

## Prerequisites

### Install FoundationDB

**macOS:**
```bash
brew install foundationdb
```

**Ubuntu/Debian:**
```bash
curl -LO https://github.com/apple/foundationdb/releases/download/7.3.43/foundationdb-clients_7.3.43-1_amd64.deb
curl -LO https://github.com/apple/foundationdb/releases/download/7.3.43/foundationdb-server_7.3.43-1_amd64.deb
sudo dpkg -i foundationdb-clients_7.3.43-1_amd64.deb
sudo dpkg -i foundationdb-server_7.3.43-1_amd64.deb
```

Cluster file location: `/usr/local/etc/foundationdb/fdb.cluster` (macOS) or `/etc/foundationdb/fdb.cluster` (Linux)

## Quick Start

### 1. Build

```bash
# Without reth integration
cargo build --release

# With direct reth DB access (recommended for production)
cargo build --release --features reth
```

### 2. Index Historical Data

```bash
./target/release/blockscout-backfill \
  --rpc-url https://eth.llamarpc.com \
  --from-block 21270000 \
  --to-block 21275000
```

### 3. Start API Server

```bash
# Basic (uses default FDB cluster file)
./target/release/blockscout-exex api --port 3000

# With reth direct reads
./target/release/blockscout-exex api \
  --reth-db ~/.local/share/reth/mainnet/db \
  --reth-static-files ~/.local/share/reth/mainnet/static_files \
  --port 3000
```

### 4. Connect Frontend

Create `.env.local` in blockscout-frontend:

```env
NEXT_PUBLIC_API_HOST=localhost:3000
NEXT_PUBLIC_API_PROTOCOL=http
NEXT_PUBLIC_NETWORK_NAME=Ethereum
NEXT_PUBLIC_NETWORK_ID=1
```

## FDB Key Schema

```
Prefix  Key Layout                              Value
──────  ──────────────────────────────────────  ─────────────────
0x01    <address:20>/<block:8>/<tx_idx:4>       tx_hash:32
0x02    <address:20>/<block:8>/<log_idx:8>      TokenTransfer (bincode)
0x03    <token:20>/<holder:20>                  balance:32
0x04    <tx_hash:32>                            block_num:8
0x05    "last_block"                            u64
```

Keys are ordered, enabling efficient range scans for pagination.

## API Endpoints

### Implemented

| Endpoint | Source |
|----------|--------|
| `GET /api/v2/addresses/:hash/transactions` | FDB |
| `GET /api/v2/addresses/:hash/token-transfers` | FDB |
| `GET /api/v2/tokens/:hash/holders` | FDB |
| `GET /api/v2/blocks` | reth (or stub) |
| `GET /api/v2/blocks/:id` | reth (or stub) |
| `GET /api/v2/transactions/:hash` | reth (or stub) |
| `GET /api/v2/addresses/:hash` | reth (or stub) |
| `GET /api/v2/stats` | reth + FDB |

### Stubbed (frontend compatibility)

`/api/v2/transactions`, `/api/v2/tokens`, `/api/v2/search`, `/api/v2/stats/charts/*`

## CLI Reference

### blockscout-exex

```
blockscout-exex api [OPTIONS]
    --cluster-file <PATH>         FDB cluster file
    --reth-db <PATH>              Reth database path
    --reth-static-files <PATH>    Reth static files path  
    --port <PORT>                 [default: 3000]

blockscout-exex stats [OPTIONS]
    --cluster-file <PATH>         FDB cluster file
```

### blockscout-backfill

```
blockscout-backfill [OPTIONS]
    --rpc-url <URL>           [default: http://localhost:8545]
    --cluster-file <PATH>     FDB cluster file
    --from-block <NUM>        [default: 0 = resume]
    --to-block <NUM>          [default: 0 = latest]
    --batch-size <NUM>        [default: 1000]
```

### blockscout-api (standalone)

```
blockscout-api [OPTIONS]
    --cluster-file <PATH>     FDB cluster file
    --port <PORT>             [default: 3000]
    --host <HOST>             [default: 0.0.0.0]
    --socket <PATH>           Unix socket for SSR
```

### blockscout-subscriber

```
blockscout-subscriber [OPTIONS]
    --ws-url <URL>            [default: ws://localhost:8546]
    --rpc-url <URL>           [default: http://localhost:8545]
    --cluster-file <PATH>     FDB cluster file
    --reconnect-delay <SECS>  [default: 5]
```

## Multi-Node Deployment

```
┌──────────────┐   ┌──────────────┐   ┌──────────────┐
│    Node 1    │   │    Node 2    │   │    Node 3    │
│              │   │              │   │              │
│  fdbserver   │◄─►│  fdbserver   │◄─►│  fdbserver   │
│  exex-api    │   │  exex-api    │   │  backfill    │
└──────────────┘   └──────────────┘   └──────────────┘
       │                  │                  │
       └──────────────────┴──────────────────┘
                  shared cluster file
```

All nodes share the same `fdb.cluster` file. FDB handles replication and distribution.

## Performance

| Operation | Latency |
|-----------|---------|
| Block by number (reth) | ~0.1ms |
| Address balance (reth) | ~0.2ms |
| Address transactions (FDB) | ~1ms |
| Token holders (FDB) | ~1ms |

## Roadmap

- [x] Direct reth MDBX reads
- [x] Blockscout frontend compatibility  
- [x] Address transaction indexing
- [x] ERC20 transfer indexing
- [x] FoundationDB storage
- [ ] Live block subscription (subscriber binary exists, untested)
- [ ] Internal transactions (debug_traceBlock)
- [ ] Contract verification
- [ ] Full-text search

## License

MIT
