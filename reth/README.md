# Reth 10k Block Window

Run a pruned Ethereum mainnet node that keeps only the last 10,000 blocks with full archive data.

## Requirements

- ~50-100GB disk space
- 16GB+ RAM recommended
- Good internet connection

## Quick Start

### Option 1: Docker Compose (recommended)

```bash
cd reth
docker compose up -d

# Watch logs
docker compose logs -f reth
docker compose logs -f lighthouse
```

### Option 2: Native (requires reth + lighthouse installed)

Terminal 1 - Reth:
```bash
./run-reth.sh
```

Terminal 2 - Lighthouse (consensus client, lightweight):
```bash
lighthouse bn \
  --network mainnet \
  --execution-endpoint http://localhost:8551 \
  --execution-jwt ~/.local/share/reth-10k/jwt.hex \
  --checkpoint-sync-url https://beaconstate.ethstaker.cc \
  --disable-deposit-contract-sync \
  --reconstruct-historic-states false \
  --prune-payloads true \
  --prune-blobs true \
  --genesis-backfill false \
  --http
```

## Sync Time

| Phase | Time |
|-------|------|
| Checkpoint sync (beacon) | ~10-30 min |
| State sync (execution) | ~2-4 hours |
| **Total** | **~3-5 hours** |

## Endpoints

| Service | URL |
|---------|-----|
| HTTP RPC | http://localhost:8545 |
| WebSocket | ws://localhost:8546 |
| Metrics | http://localhost:9001 |
| Beacon API | http://localhost:5052 |

## Verify Sync

```bash
# Check sync status
curl -s http://localhost:8545 \
  -X POST \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"eth_syncing","params":[],"id":1}' | jq .

# Get latest block
curl -s http://localhost:8545 \
  -X POST \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}' | jq -r '.result' | xargs printf "%d\n"
```

## Use with blockscout-exex

Once synced, run the API with direct DB access:

```bash
# Docker data location
RETH_DB="/var/lib/docker/volumes/reth_reth-data/_data/mainnet/db"
RETH_STATIC="/var/lib/docker/volumes/reth_reth-data/_data/mainnet/static_files"

# Native data location
RETH_DB="$HOME/.local/share/reth-10k/mainnet/db"
RETH_STATIC="$HOME/.local/share/reth-10k/mainnet/static_files"

# Run API
./target/release/blockscout-exex api \
  --index-path ./blockscout-index \
  --reth-db "$RETH_DB" \
  --reth-static-files "$RETH_STATIC" \
  --port 3000
```

## Pruning Config

The `reth.toml` keeps last 10,000 blocks for:
- Transaction sender recovery
- Transaction lookup index
- Receipts (logs, events)
- Account history (balance changes)
- Storage history (contract state changes)

Older data is pruned every 100 blocks.

## Disk Usage

| Data | Size |
|------|------|
| Reth (10k window) | ~50-80GB |
| Lighthouse (lightweight) | ~5-10GB |
| **Total** | **~55-90GB** |

Note: Lighthouse with `--genesis-backfill=false` and pruning keeps only recent data.

## Stop

```bash
# Docker
docker compose down

# Native
pkill reth
pkill lighthouse
```

## Reset

```bash
# Docker - remove all data
docker compose down -v

# Native
rm -rf ~/.local/share/reth-10k
rm -rf ~/.lighthouse/mainnet
```
