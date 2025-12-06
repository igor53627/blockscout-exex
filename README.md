# blockscout-exex

A high-performance Blockscout-compatible API server that reads directly from Reth's database — no JSON-RPC overhead. Uses MDBX for fast indexed data storage and Meilisearch for full-text search.

## Architecture

```
┌─────────────────┐     ┌───────────────────────────────────────────────────────┐
│   Blockscout    │────▶│              blockscout-exex API                      │
│    Frontend     │     │                                                       │
│    (Next.js)    │     │  ┌─────────────────┐  ┌────────────┐  ┌────────────┐  │
└─────────────────┘     │  │      MDBX       │  │ Meilisearch│  │ Reth/RPC   │  │
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
| **MDBX Integration** | ✅ Working | Embedded key-value database |
| **Meilisearch** | ✅ Working | Full-text search for tokens/addresses |
| **Backfill Tool** | ✅ Working | Indexes from Reth DB + populates Meilisearch |
| **WebSocket** | ✅ Working | Phoenix Channels compatible for live updates |
| **Reth Direct Reads** | ✅ Working | Requires `--features reth` |

## MDBX Key Schema

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
| `GET /api/v2/addresses/:hash/transactions` | MDBX |
| `GET /api/v2/addresses/:hash/token-transfers` | MDBX |
| `GET /api/v2/addresses/:hash/counters` | MDBX |
| `GET /api/v2/addresses/:hash/tabs-counters` | MDBX |
| `GET /api/v2/tokens/:hash` | RPC eth_call |
| `GET /api/v2/tokens/:hash/holders` | MDBX |
| `GET /api/v2/tokens/:hash/transfers` | MDBX |
| `GET /api/v2/tokens/:hash/counters` | MDBX |
| `GET /api/v2/transactions/:hash/logs` | RPC receipt |
| `GET /api/v2/transactions/:hash/token-transfers` | RPC receipt |
| `GET /api/v2/transactions/:hash/internal-transactions` | RPC trace |
| `GET /api/v2/blocks`, `/blocks/:id`, `/blocks/:id/transactions` | RPC |
| `GET /api/v2/search`, `/search/quick` | Meilisearch + MDBX |
| `GET /api/v2/stats` | MDBX counters |
| `GET /socket/v2/websocket` | Phoenix Channels |

### Stubbed (Frontend Compatibility)

`/api/v2/stats/charts/*`, `/api/v2/smart-contracts/*`

## CLI Reference

### blockscout-api

```bash
blockscout-api [OPTIONS]
    --mdbx-path <PATH>        MDBX database path
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
    --reth-db <PATH>          Reth database path
    --reth-static-files <PATH> Reth static files path
    --mdbx-path <PATH>        MDBX database path
    --from-block <NUM>        [default: 0 = resume]
    --to-block <NUM>          [default: 0 = latest]
    --batch-size <NUM>        [default: 1000]
    --chain <NAME>            Chain name [default: sepolia]
    --meili-url <URL>         Meilisearch URL (enables token indexing)
    --meili-key <KEY>         Meilisearch API key
```

## Quick Start

### 1. Prerequisites

**Reth Node:**
Requires a running Reth node with accessible database for backfill.

**Meilisearch:**
```bash
docker run -d --name meilisearch -p 7700:7700 \
  -v ./meilisearch:/meili_data \
  getmeili/meilisearch:latest
```

### 2. Build

```bash
cargo build --release --features reth
```

### 3. Backfill Index

```bash
./target/release/blockscout-backfill \
  --reth-db /path/to/reth/db \
  --reth-static-files /path/to/reth/static_files \
  --mdbx-path /path/to/mdbx \
  --chain sepolia \
  --meili-url http://localhost:7700
```

### 4. Start API

```bash
./target/release/blockscout-api \
  --mdbx-path /path/to/mdbx \
  --port 4000 \
  --reth-rpc http://localhost:8545 \
  --meili-url http://localhost:7700 \
  --chain sepolia
```

## Performance

| Operation | Latency |
|-----------|---------|
| Block by number (RPC) | ~5-10ms |
| Address transactions (MDBX) | ~1ms |
| Token holders (MDBX) | ~1ms |
| Search (Meilisearch) | ~5ms |
| Counter read (MDBX) | ~0.5ms |
| Backfill speed | ~100 blocks/sec |

## License

MIT
