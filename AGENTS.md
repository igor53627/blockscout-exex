# Agent Instructions

## API Development

When modifying any API endpoints, always cross-check the response format with the real Blockscout API:
- **Sepolia**: `https://eth-sepolia.blockscout.com/api/v2/`
- **Mainnet**: `https://eth.blockscout.com/api/v2/`

Example: To check block transactions format:
```bash
curl -s 'https://eth-sepolia.blockscout.com/api/v2/blocks/3575610/transactions' | jq '.items[0]'
```

Compare field names, types (null vs [], string vs number), and structure before deploying.

## Build & Deploy

### API Server
```bash
# Build on server (FDB required)
ssh root@aya "cd /mnt/sepolia/blockscout-exex/src && cargo build --release --bin blockscout-api"

# Deploy
ssh root@aya "systemctl stop blockscout-api && cp /mnt/sepolia/blockscout-exex/src/target/release/blockscout-api /mnt/sepolia/blockscout-exex/bin/ && systemctl start blockscout-api"
```

### Backfill
```bash
# Build
ssh root@aya "cd /mnt/sepolia/blockscout-exex/src && cargo build --release --bin blockscout-backfill"

# Run with Meilisearch indexing
ssh root@aya "/mnt/sepolia/blockscout-exex/bin/blockscout-backfill \
  --rpc-url http://127.0.0.1:9090/queries/sepolia \
  --meili-url http://127.0.0.1:7700 \
  --from-block 0"
```

### Frontend (Custom SSR Fork)
```bash
# Deploy from ~/dev/blockscout/blockscout-frontend
cd ~/dev/blockscout/blockscout-frontend && ./deploy.sh
```

**Important**: Always use our custom frontend fork (~/dev/blockscout/blockscout-frontend), never the upstream ghcr.io/blockscout/frontend image. Our fork includes SSR optimizations.

### Caddy + Nodecore Proxy
```bash
# Deploy from ~/dev/blockscout/blockscout-proxy
rsync -av ~/dev/blockscout/blockscout-proxy/ root@aya:/mnt/sepolia/blockscout-proxy/
ssh root@aya "cd /mnt/sepolia/blockscout-proxy && podman-compose down && podman-compose up -d"
```

## Server Details

- Server: `root@aya` (104.204.142.45)
- Public URL: http://104.204.142.45

### Services

| Service | Port | Type | Notes |
|---------|------|------|-------|
| Caddy | 80 | podman | Public entry point |
| Frontend | 3000 | podman | Next.js SSR |
| API | 4000 | systemd | blockscout-api |
| Meilisearch | 7700 | podman | Full-text search |
| Nodecore | 9090 | podman | RPC cache |
| Reth RPC | 8545 | systemd | Ethereum node |
| FDB | 4500 | systemd | FoundationDB |

### Data Locations

| Data | Path |
|------|------|
| FDB data | `/mnt/sepolia/foundationdb/data/` |
| Meilisearch data | `/mnt/sepolia/meilisearch/` |
| API binary | `/mnt/sepolia/blockscout-exex/bin/` |
| Source code | `/mnt/sepolia/blockscout-exex/src/` |

## Architecture

```
                         ┌─────────────────────────────────────────┐
                         │              Server (aya)               │
                         │                                         │
User ──▶ :80 Caddy ──────┼──▶ :3000 Next.js frontend (SSR)        │
                         │       │                                 │
                         │       ▼                                 │
                         │   :4000 API server ◀────────────────────┤
                         │       │                                 │
                         │       ├──▶ :7700 Meilisearch (search)   │
                         │       │                                 │
                         │       ├──▶ :4500 FoundationDB (index)   │
                         │       │                                 │
                         │       └──▶ :9090 Nodecore (RPC cache)   │
                         │               │                         │
                         │               ▼                         │
                         │           :8545 Reth (Ethereum)         │
                         │                                         │
                         └─────────────────────────────────────────┘
```

## Service Management

```bash
# API
systemctl status blockscout-api
systemctl restart blockscout-api
journalctl -u blockscout-api -f

# Meilisearch
podman ps | grep meilisearch
podman logs meilisearch -f
podman restart meilisearch

# Check Meilisearch health
curl http://localhost:7700/health

# Check Meilisearch indexes
curl http://localhost:7700/indexes
```

## Nodecore Configuration

When something doesn't work with Nodecore, **fix the Nodecore config** - don't bypass it by switching to Reth directly.

Key Nodecore settings:
- Config path: `/mnt/sepolia/blockscout-proxy/nodecore.yml`
- Must set `NODECORE_CONFIG_PATH=/nodecore.yml` environment variable in docker-compose
- Chain must be `sepolia` (matches chains.yaml from drpcorg/public)
- Request path is `/queries/sepolia` (not just `/` or `/sepolia`)
- Use `127.0.0.1:8545` not `host.containers.internal` when using `network_mode: host`

Restart after config changes:
```bash
cd /mnt/sepolia/blockscout-proxy && podman-compose restart nodecore
```

Check logs for "State of SEPOLIA" to confirm correct chain detection:
```bash
podman logs blockscout-proxy_nodecore_1 2>&1 | grep -i "state of"
```

## FDB Key Schema

```
Prefix  Description                 Key Format
──────  ──────────────────────────  ────────────────────────────────────
0x01    Address transactions        <address:20>/<block:8>/<tx_idx:4>
0x02    Address token transfers     <address:20>/<block:8>/<log_idx:8>
0x03    Token holders               <token:20>/<holder:20>
0x04    Transaction → block         <tx_hash:32>
0x05    Metadata                    "last_block", "address_hll"
0x06    Global counters             "total_txs", "total_transfers"
0x08    Token transfers             <token:20>/<block:8>/<log_idx:8>
0x09    Address counters            <address:20><kind:1> (0=tx, 1=transfer)
0x0A    Token holder counts         <token:20>
0x0B    Daily metrics               <year:2><month:1><day:1><metric:1>
```

## Address Counting (HyperLogLog)

The unique address count uses HyperLogLog for fast approximate counting:
- **In-memory HLL** during backfill (p=14, ~0.8% error, ~12KB RAM)
- **Count only** stored in FDB at `0x05address_hll` (8 bytes)
- Saved every 10k blocks during backfill
- API reads count directly via `get_address_count_hll()`

Note: HLL cannot be serialized (RandomState issue), so we only persist the count.
If backfill restarts from scratch, HLL rebuilds from zero.

## Meilisearch Indexes

| Index | Documents | Searchable | Filterable |
|-------|-----------|------------|------------|
| tokens | ERC-20 tokens | name, symbol, address | chain, token_type |
| addresses | Addresses | address, ens_name, label | chain, is_contract |
