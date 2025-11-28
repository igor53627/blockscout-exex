# Blockscout API Endpoints Priority

## Priority 1: Core Pages (Homepage, Block, Tx, Address)

| Endpoint | Usage Count | Source | Status |
|----------|-------------|--------|--------|
| `GET /api/v2/stats` | 12 | Index + Reth | 🔴 TODO |
| `GET /api/v2/addresses/:hash` | 9 | Reth state | 🔴 TODO |
| `GET /api/v2/smart-contracts/:hash` | 6 | Mock/Verify | 🔴 TODO |
| `GET /api/v2/transactions/:hash` | 3 | Reth | 🔴 TODO |
| `GET /api/v2/tokens/:hash` | 3 | Index | 🔴 TODO |
| `GET /api/v2/stats/charts/market` | 3 | Mock/External | 🔴 TODO |
| `GET /api/v2/main-page/transactions` | 3 | Reth + Index | 🔴 TODO |
| `GET /api/v2/main-page/indexing-status` | 2 | Index | ✅ Done |
| `GET /api/v2/main-page/blocks` | 2 | Reth | 🔴 TODO |
| `GET /api/v2/blocks/:height` | 2 | Reth | 🔴 TODO |
| `GET /api/v2/search/check-redirect` | 2 | Index | 🔴 TODO |

## Priority 2: Address Page Tabs

| Endpoint | Usage Count | Source | Status |
|----------|-------------|--------|--------|
| `GET /api/v2/addresses/:hash/transactions` | 3 | Index | ✅ Done |
| `GET /api/v2/addresses/:hash/token-transfers` | 2 | Index | ✅ Done |
| `GET /api/v2/addresses/:hash/internal-transactions` | 2 | Index/Trace | 🔴 TODO |
| `GET /api/v2/addresses/:hash/tokens` | 2 | Index | 🔴 TODO |
| `GET /api/v2/addresses/:hash/nft` | 2 | Index | 🔴 TODO |
| `GET /api/v2/addresses/:hash/nft/collections` | 2 | Index | 🔴 TODO |

## Priority 3: Token Pages

| Endpoint | Usage Count | Source | Status |
|----------|-------------|--------|--------|
| `GET /api/v2/tokens` | 2 | Index | 🔴 TODO |
| `GET /api/v2/tokens/:hash/holders` | - | Index | ✅ Done |
| `GET /api/v2/tokens/:hash/transfers` | 2 | Index | 🔴 TODO |
| `GET /api/v2/token-transfers` | 2 | Index | 🔴 TODO |
| `GET /api/v2/tokens/:hash/instances` | 2 | Index | 🔴 TODO |

## Priority 4: Block/Transaction Pages

| Endpoint | Usage Count | Source | Status |
|----------|-------------|--------|--------|
| `GET /api/v2/blocks` | 2 | Reth | 🔴 TODO |
| `GET /api/v2/blocks/:height/transactions` | 2 | Reth | 🔴 TODO |
| `GET /api/v2/transactions` | 2 | Reth + Index | 🔴 TODO |
| `GET /api/v2/transactions/:hash/token-transfers` | 2 | Index | 🔴 TODO |
| `GET /api/v2/transactions/:hash/logs` | - | Reth | 🔴 TODO |
| `GET /api/v2/transactions/:hash/internal-transactions` | 2 | Trace | 🔴 TODO |

## Priority 5: Search

| Endpoint | Usage Count | Source | Status |
|----------|-------------|--------|--------|
| `GET /api/v2/search` | 2 | Index (search) | 🔴 TODO |
| `GET /api/v2/advanced-filter` | 2 | Index | 🔴 TODO |
| `GET /api/v2/advanced-filter/methods` | 2 | Index | 🔴 TODO |

## Data Sources

### From Reth DB (direct read)
- Blocks (headers, bodies, transactions)
- Transactions (by hash)
- Receipts (logs, status)
- Account state (balance, nonce, code)
- Storage

### From Index DB (our sidecar)
- Address → transactions mapping
- Address → token transfers
- Token → holders
- Token metadata cache
- Search index

### Needs Mocking
- Contract verification/source code
- Market charts/prices
- Gas oracle
- Third-party integrations (ENS, etc.)

### Needs Tracing (debug_traceBlock)
- Internal transactions
- State changes
- Call traces

## Implementation Order

1. **Phase 1**: Stats + Homepage (blocks, txs)
2. **Phase 2**: Address page (balance, code, full tx list)
3. **Phase 3**: Block/Tx detail pages
4. **Phase 4**: Token pages
5. **Phase 5**: Search
6. **Phase 6**: Contract verification (mock or integrate)
