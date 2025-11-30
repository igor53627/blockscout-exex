# blockscout-exex

A high-performance Blockscout-compatible API server that reads directly from Reth's database — no JSON-RPC overhead. Uses FoundationDB for scalable indexed data storage and Meilisearch for full-text search.

## Architecture

```
┌─────────────────┐     ┌───────────────────────────────────────────────────────┐
│   Blockscout    │────▶│              blockscout-exex API                      │
│    Frontend     │     │                                                       │
│    (Next.js)    │     │  ┌─────────────────┐  ┌────────────┐  ┌────────────┐  │
└─────────────────┘     │  │  FoundationDB   │  │ Meilisearch│  │ Reth/RPC   │  │
                        │  │                 │  │            │  │            │  │
                        │  │  • addr → txs   │  │  • tokens  │  │  • blocks  │  │
                        │  │  • addr → xfers │  │  • addrs   │  │  • txs     │  │
                        │  │  • token holders│  │  • search  │  │  • traces  │  │
                        │  │  • token xfers  │  │            │  │  • state   │  │
                        │  │  • counters     │  │            │  │            │  │
                        │  └─────────────────┘  └────────────┘  └────────────┘  │
                        └───────────────────────────────────────────────────────┘
```

## Current Status

| Component | Status | Notes |
|-----------|--------|-------|
| **API Server** | ✅ Working | Serves Blockscout v2 API |
| **FDB Integration** | ✅ Working | TCP connection to fdbserver |
| **Meilisearch** | ✅ Working | Full-text search for tokens/addresses |
| **Backfill Tool** | ✅ Working | Indexes from JSON-RPC + populates Meilisearch |
| **WebSocket** | ✅ Working | Phoenix Channels compatible for live updates |
| **Reth Direct Reads** | ✅ Working | Optional, requires `--features reth` |

## FDB Key Schema

```
Prefix  Key Layout                              Value
──────  ──────────────────────────────────────  ─────────────────
0x01    <address:20>/<block:8>/<tx_idx:4>       tx_hash:32
0x02    <address:20>/<block:8>/<log_idx:8>      TokenTransfer (bincode)
0x03    <token:20>/<holder:20>                  balance:32
0x04    <tx_hash:32>                            block_num:8
0x05    "last_block"                            u64
0x06    <counter_name>                          u64 (total_txs, total_transfers)
0x08    <token:20>/<block:8>/<log_idx:8>        TokenTransfer (bincode)
```

## Meilisearch Indexes

| Index | Searchable Fields | Filterable |
|-------|-------------------|------------|
| `tokens` | name, symbol, address | chain, token_type |
| `addresses` | address, ens_name, label | chain, is_contract |

## API Endpoints

### Implemented (Real Data)

| Endpoint | Source |
|----------|--------|
| `GET /api/v2/addresses/:hash/transactions` | FDB |
| `GET /api/v2/addresses/:hash/token-transfers` | FDB |
| `GET /api/v2/addresses/:hash/counters` | FDB |
| `GET /api/v2/addresses/:hash/tabs-counters` | FDB |
| `GET /api/v2/tokens/:hash` | RPC eth_call |
| `GET /api/v2/tokens/:hash/holders` | FDB |
| `GET /api/v2/tokens/:hash/transfers` | FDB |
| `GET /api/v2/tokens/:hash/counters` | FDB |
| `GET /api/v2/transactions/:hash/logs` | RPC receipt |
| `GET /api/v2/transactions/:hash/token-transfers` | RPC receipt |
| `GET /api/v2/transactions/:hash/internal-transactions` | RPC trace |
| `GET /api/v2/blocks`, `/blocks/:id`, `/blocks/:id/transactions` | RPC |
| `GET /api/v2/search`, `/search/quick` | Meilisearch + FDB |
| `GET /api/v2/stats` | FDB counters |
| `GET /socket/v2/websocket` | Phoenix Channels |

### Stubbed (Frontend Compatibility)

`/api/v2/stats/charts/*`, `/api/v2/smart-contracts/*`

## CLI Reference

### blockscout-api

```bash
blockscout-api [OPTIONS]
    --cluster-file <PATH>     FDB cluster file
    --port <PORT>             [default: 3000]
    --host <HOST>             [default: 0.0.0.0]
    --socket <PATH>           Unix socket for SSR
    --reth-rpc <URL>          Reth RPC for live blocks
    --meili-url <URL>         Meilisearch URL
    --meili-key <KEY>         Meilisearch API key
    --chain <NAME>            Chain name for search [default: sepolia]
```

### blockscout-backfill

```bash
blockscout-backfill [OPTIONS]
    --rpc-url <URL>           [default: http://localhost:8545]
    --ipc-path <PATH>         Unix socket (faster than HTTP)
    --cluster-file <PATH>     FDB cluster file
    --from-block <NUM>        [default: 0 = resume]
    --to-block <NUM>          [default: 0 = latest]
    --batch-size <NUM>        [default: 1000]
    --meili-url <URL>         Meilisearch URL (enables token indexing)
    --meili-key <KEY>         Meilisearch API key
```

## Quick Start

### 1. Prerequisites

**FoundationDB:**
```bash
# macOS
brew install foundationdb

# Ubuntu/Debian
curl -LO https://github.com/apple/foundationdb/releases/download/7.3.43/foundationdb-clients_7.3.43-1_amd64.deb
curl -LO https://github.com/apple/foundationdb/releases/download/7.3.43/foundationdb-server_7.3.43-1_amd64.deb
sudo dpkg -i foundationdb-*.deb
```

**Meilisearch:**
```bash
docker run -d --name meilisearch -p 7700:7700 \
  -v ./meilisearch:/meili_data \
  getmeili/meilisearch:latest
```

### 2. Build

```bash
cargo build --release
```

### 3. Backfill Index

```bash
./target/release/blockscout-backfill \
  --rpc-url https://eth-sepolia.g.alchemy.com/v2/YOUR_KEY \
  --meili-url http://localhost:7700 \
  --from-block 0
```

### 4. Start API

```bash
./target/release/blockscout-api \
  --port 4000 \
  --reth-rpc http://localhost:8545 \
  --meili-url http://localhost:7700 \
  --chain sepolia
```

## Performance

| Operation | Latency |
|-----------|---------|
| Block by number (RPC) | ~5-10ms |
| Address transactions (FDB) | ~1ms |
| Token holders (FDB) | ~1ms |
| Search (Meilisearch) | ~5ms |
| Counter read (FDB) | ~0.5ms |

## License

MIT
