#!/bin/bash
# Run reth with 10k block window on mainnet
#
# Requirements:
#   - reth installed: cargo install --git https://github.com/paradigmxyz/reth.git reth
#   - ~50-100GB disk space
#   - Good internet connection
#
# Sync time: ~2-4 hours (checkpoint sync)

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DATA_DIR="${RETH_DATA_DIR:-$HOME/.local/share/reth-10k}"

echo "=== Reth 10k Block Window ==="
echo "Data directory: $DATA_DIR"
echo "Config: $SCRIPT_DIR/reth.toml"
echo ""

# Create data directory
mkdir -p "$DATA_DIR"

# Run reth with checkpoint sync and pruning
exec reth node \
  --chain mainnet \
  --datadir "$DATA_DIR" \
  --config "$SCRIPT_DIR/reth.toml" \
  --http \
  --http.addr 0.0.0.0 \
  --http.port 8545 \
  --http.api eth,net,web3,debug,trace \
  --ws \
  --ws.addr 0.0.0.0 \
  --ws.port 8546 \
  --authrpc.addr 0.0.0.0 \
  --authrpc.port 8551 \
  --metrics 0.0.0.0:9001 \
  --log.stdout.format terminal \
  "$@"
