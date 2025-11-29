use std::sync::Arc;

use alloy_primitives::Address;
#[cfg(feature = "reth")]
use alloy_primitives::B256;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tower_http::cors::CorsLayer;

use crate::fdb_index::FdbIndex;
#[cfg(feature = "reth")]
use crate::reth_reader::RethReader;

pub struct ApiState {
    pub index: Arc<FdbIndex>,
    #[cfg(feature = "reth")]
    pub reth: Option<RethReader>,
}

#[derive(Debug, Deserialize)]
pub struct PaginationParams {
    #[serde(default = "default_limit")]
    pub limit: usize,
    #[serde(default)]
    pub offset: usize,
}

fn default_limit() -> usize {
    50
}

#[derive(Debug, Serialize)]
pub struct ApiResponse<T> {
    pub items: T,
    pub next_page_params: Option<NextPageParams>,
}

#[derive(Debug, Serialize)]
pub struct NextPageParams {
    pub offset: usize,
    pub limit: usize,
}

pub fn create_router(state: Arc<ApiState>) -> Router {
    Router::new()
        // Existing real endpoints
        .route("/api/v2/addresses/:hash/transactions", get(get_address_txs))
        .route(
            "/api/v2/addresses/:hash/token-transfers",
            get(get_address_transfers),
        )
        .route("/api/v2/tokens/:hash/holders", get(get_token_holders))
        .route("/api/v2/main-page/indexing-status", get(indexing_status))
        .route("/health", get(health))
        // Stub endpoints for frontend compatibility
        .route("/api/v2/config/backend-version", get(backend_version))
        .route("/api/v2/stats", get(stats))
        .route("/api/v2/stats/charts/transactions", get(stats_charts_txs))
        .route("/api/v2/stats/charts/market", get(stats_charts_market))
        .route("/api/v2/blocks", get(blocks))
        .route("/api/v2/blocks/:height_or_hash", get(block_by_id))
        .route("/api/v2/blocks/:height_or_hash/transactions", get(block_txs))
        .route("/api/v2/transactions", get(transactions))
        .route("/api/v2/transactions/:hash", get(transaction_by_hash))
        .route("/api/v2/transactions/:hash/token-transfers", get(tx_token_transfers))
        .route("/api/v2/transactions/:hash/logs", get(tx_logs))
        .route("/api/v2/transactions/:hash/internal-transactions", get(tx_internal_txs))
        .route("/api/v2/addresses/:hash", get(address_by_hash))
        .route("/api/v2/addresses/:hash/counters", get(address_counters))
        .route("/api/v2/addresses/:hash/tabs-counters", get(address_tabs_counters))
        .route("/api/v2/tokens/:hash", get(token_by_hash))
        .route("/api/v2/tokens/:hash/counters", get(token_counters))
        .route("/api/v2/tokens", get(tokens_list))
        .route("/api/v2/main-page/blocks", get(homepage_blocks))
        .route("/api/v2/main-page/transactions", get(homepage_txs))
        .route("/api/v2/search", get(search))
        .route("/api/v2/search/quick", get(search_quick))
        .layer(CorsLayer::permissive())
        .with_state(state)
}

// ============ STUB ENDPOINTS ============

async fn backend_version() -> impl IntoResponse {
    Json(json!({
        "backend_version": "6.10.0-exex-fdb"
    }))
}

async fn stats(State(state): State<Arc<ApiState>>) -> impl IntoResponse {
    #[cfg(feature = "reth")]
    let last_block = state
        .reth
        .as_ref()
        .and_then(|r| r.last_block_number().ok().flatten())
        .unwrap_or(0);
    #[cfg(not(feature = "reth"))]
    let last_block = state.index.last_indexed_block().await.unwrap_or(None).unwrap_or(0);

    Json(json!({
        "total_blocks": last_block.to_string(),
        "total_addresses": "0",
        "total_transactions": "0",
        "average_block_time": 12000.0,
        "coin_price": null,
        "coin_price_change_percentage": null,
        "total_gas_used": "0",
        "transactions_today": null,
        "gas_used_today": "0",
        "gas_prices": null,
        "gas_price_updated_at": null,
        "gas_prices_update_in": 0,
        "static_gas_price": null,
        "market_cap": null,
        "network_utilization_percentage": 0.0,
        "tvl": null
    }))
}

async fn stats_charts_txs() -> impl IntoResponse {
    Json(json!({
        "chart_data": [],
        "available_resolutions": ["DAY"]
    }))
}

async fn stats_charts_market() -> impl IntoResponse {
    Json(json!({
        "chart_data": [],
        "available_resolutions": ["DAY"]
    }))
}

async fn blocks(State(state): State<Arc<ApiState>>) -> impl IntoResponse {
    #[cfg(feature = "reth")]
    let last_block = state
        .reth
        .as_ref()
        .and_then(|r| r.last_block_number().ok().flatten())
        .unwrap_or(0);
    #[cfg(not(feature = "reth"))]
    let last_block = state.index.last_indexed_block().await.unwrap_or(None).unwrap_or(0);

    #[cfg(feature = "reth")]
    let items: Vec<Value> = (0..10)
        .filter_map(|i| {
            let height = last_block.checked_sub(i)?;
            if let Some(ref reth) = state.reth {
                if let Ok(Some(block)) = reth.block_by_number(height) {
                    return Some(block_to_json(&block, height));
                }
            }
            Some(stub_block(height))
        })
        .collect();

    #[cfg(not(feature = "reth"))]
    let items: Vec<Value> = (0..10)
        .filter_map(|i| {
            let height = last_block.checked_sub(i)?;
            Some(stub_block(height))
        })
        .collect();

    Json(json!({
        "items": items,
        "next_page_params": null
    }))
}

async fn block_by_id(
    State(state): State<Arc<ApiState>>,
    Path(height_or_hash): Path<String>,
) -> impl IntoResponse {
    #[cfg(feature = "reth")]
    if let Some(ref reth) = state.reth {
        // Try as block number first
        if let Ok(height) = height_or_hash.parse::<u64>() {
            if let Ok(Some(block)) = reth.block_by_number(height) {
                return Json(block_to_json(&block, height));
            }
        }
        // Try as hash
        if let Ok(hash) = height_or_hash.parse::<B256>() {
            if let Ok(Some(block)) = reth.block_by_hash(hash) {
                let height = block.header.number;
                return Json(block_to_json(&block, height));
            }
        }
    }

    let height = height_or_hash.parse::<u64>().unwrap_or(0);
    let _ = state; // silence unused warning when reth feature disabled
    Json(stub_block(height))
}

async fn block_txs(Path(_height_or_hash): Path<String>) -> impl IntoResponse {
    Json(json!({
        "items": [],
        "next_page_params": null
    }))
}

async fn transactions() -> impl IntoResponse {
    Json(json!({
        "items": [],
        "next_page_params": null
    }))
}

async fn transaction_by_hash(
    State(state): State<Arc<ApiState>>,
    Path(hash): Path<String>,
) -> impl IntoResponse {
    #[cfg(feature = "reth")]
    if let Some(ref reth) = state.reth {
        if let Ok(tx_hash) = hash.parse::<B256>() {
            if let Ok(Some(tx)) = reth.transaction_by_hash(tx_hash) {
                let block_num = reth.transaction_block_number(tx_hash).ok().flatten();
                let receipt = reth.receipt_by_hash(tx_hash).ok().flatten();
                return Json(tx_to_json(&tx, block_num, receipt.as_ref()));
            }
        }
    }

    let _ = state;
    Json(stub_transaction(&hash))
}

async fn tx_token_transfers(Path(_hash): Path<String>) -> impl IntoResponse {
    Json(json!({
        "items": [],
        "next_page_params": null
    }))
}

async fn tx_logs(Path(_hash): Path<String>) -> impl IntoResponse {
    Json(json!({
        "items": [],
        "next_page_params": null
    }))
}

async fn tx_internal_txs(Path(_hash): Path<String>) -> impl IntoResponse {
    Json(json!({
        "items": [],
        "next_page_params": null
    }))
}

async fn address_by_hash(
    State(state): State<Arc<ApiState>>,
    Path(hash): Path<String>,
) -> impl IntoResponse {
    #[cfg(feature = "reth")]
    if let Some(ref reth) = state.reth {
        if let Ok(addr) = hash.parse::<Address>() {
            let balance = reth.account_balance(addr).unwrap_or_default();
            let is_contract = reth.is_contract(addr).unwrap_or(false);
            return Json(json!({
                "hash": hash,
                "block_number_balance_updated_at": null,
                "coin_balance": balance.to_string(),
                "creator_address_hash": null,
                "creation_transaction_hash": null,
                "creation_status": null,
                "exchange_rate": null,
                "ens_domain_name": null,
                "has_logs": false,
                "has_token_transfers": false,
                "has_tokens": false,
                "has_validated_blocks": false,
                "implementations": null,
                "is_contract": is_contract,
                "is_verified": false,
                "name": null,
                "token": null,
                "watchlist_address_id": null
            }));
        }
    }

    let _ = state;
    Json(stub_address(&hash))
}

async fn address_counters(Path(_hash): Path<String>) -> impl IntoResponse {
    Json(json!({
        "transactions_count": "0",
        "token_transfers_count": "0",
        "gas_usage_count": null,
        "validations_count": null
    }))
}

async fn address_tabs_counters(Path(_hash): Path<String>) -> impl IntoResponse {
    Json(json!({
        "internal_transactions_count": 0,
        "logs_count": 0,
        "token_balances_count": 0,
        "token_transfers_count": 0,
        "transactions_count": 0,
        "validations_count": 0,
        "withdrawals_count": 0
    }))
}

async fn token_by_hash(Path(hash): Path<String>) -> impl IntoResponse {
    Json(stub_token(&hash))
}

async fn token_counters(Path(_hash): Path<String>) -> impl IntoResponse {
    Json(json!({
        "token_holders_count": "0",
        "transfers_count": "0"
    }))
}

async fn tokens_list() -> impl IntoResponse {
    Json(json!({
        "items": [],
        "next_page_params": null
    }))
}

async fn homepage_blocks(State(state): State<Arc<ApiState>>) -> impl IntoResponse {
    let last_block = state.index.last_indexed_block().await.unwrap_or(None).unwrap_or(0);
    let items: Vec<Value> = (0..6)
        .filter_map(|i| {
            let height = last_block.checked_sub(i)?;
            Some(stub_block(height))
        })
        .collect();
    
    Json(items)
}

async fn homepage_txs() -> impl IntoResponse {
    Json(json!([]))
}

async fn search(Query(_params): Query<SearchParams>) -> impl IntoResponse {
    Json(json!({
        "items": [],
        "next_page_params": null
    }))
}

async fn search_quick(Query(_params): Query<SearchParams>) -> impl IntoResponse {
    Json(json!([]))
}

#[derive(Debug, Deserialize)]
pub struct SearchParams {
    #[serde(default)]
    #[allow(dead_code)]
    pub q: String,
}

// ============ RETH DATA CONVERTERS ============

#[cfg(feature = "reth")]
fn block_to_json(block: &reth_primitives::Block, height: u64) -> Value {
    use reth_primitives::BlockBody;
    
    let header = &block.header;
    let body: &BlockBody = &block.body;
    let timestamp = chrono::DateTime::from_timestamp(header.timestamp as i64, 0)
        .map(|dt| dt.to_rfc3339())
        .unwrap_or_default();

    json!({
        "height": height,
        "timestamp": timestamp,
        "transactions_count": body.transactions.len(),
        "internal_transactions_count": 0,
        "miner": stub_address_param(&format!("{:?}", header.beneficiary)),
        "size": header.size(),
        "hash": format!("{:?}", block.hash_slow()),
        "parent_hash": format!("{:?}", header.parent_hash),
        "difficulty": header.difficulty.to_string(),
        "total_difficulty": null,
        "gas_used": header.gas_used.to_string(),
        "gas_limit": header.gas_limit.to_string(),
        "nonce": format!("{:?}", header.nonce),
        "base_fee_per_gas": header.base_fee_per_gas.map(|f| f.to_string()),
        "burnt_fees": null,
        "priority_fee": null,
        "extra_data": format!("{:?}", header.extra_data),
        "state_root": format!("{:?}", header.state_root),
        "rewards": [],
        "gas_target_percentage": null,
        "gas_used_percentage": ((header.gas_used as f64 / header.gas_limit as f64) * 100.0),
        "burnt_fees_percentage": null,
        "type": "block",
        "transaction_fees": null,
        "uncles_hashes": body.ommers.iter().map(|o| format!("{:?}", o.hash_slow())).collect::<Vec<_>>(),
        "blob_gas_used": header.blob_gas_used.map(|g| g.to_string()),
        "excess_blob_gas": header.excess_blob_gas.map(|g| g.to_string())
    })
}

#[cfg(feature = "reth")]
fn tx_to_json(
    tx: &reth_primitives::TransactionSigned,
    block_num: Option<u64>,
    receipt: Option<&reth_primitives::Receipt>,
) -> Value {
    use reth_primitives::Transaction;
    
    let inner = &tx.transaction;
    let timestamp = chrono::Utc::now().to_rfc3339();

    let status = receipt.map(|r| if r.success { "ok" } else { "error" });
    let gas_used = receipt.map(|r| r.cumulative_gas_used.to_string());
    
    let from = tx.recover_signer()
        .map(|a| format!("{:?}", a))
        .unwrap_or_else(|| "0x0".to_string());

    fn tx_kind_to_addr(kind: alloy_primitives::TxKind) -> Option<String> {
        match kind {
            alloy_primitives::TxKind::Call(addr) => Some(format!("{:?}", addr)),
            alloy_primitives::TxKind::Create => None,
        }
    }

    let (to, value, gas_limit, nonce, input, gas_price, max_fee, max_priority_fee, tx_type) = match inner {
        Transaction::Legacy(t) => (
            tx_kind_to_addr(t.to),
            t.value.to_string(),
            t.gas_limit.to_string(),
            t.nonce,
            format!("{:?}", t.input),
            Some(t.gas_price.to_string()),
            None,
            None,
            0u8,
        ),
        Transaction::Eip2930(t) => (
            tx_kind_to_addr(t.to),
            t.value.to_string(),
            t.gas_limit.to_string(),
            t.nonce,
            format!("{:?}", t.input),
            Some(t.gas_price.to_string()),
            None,
            None,
            1u8,
        ),
        Transaction::Eip1559(t) => (
            tx_kind_to_addr(t.to),
            t.value.to_string(),
            t.gas_limit.to_string(),
            t.nonce,
            format!("{:?}", t.input),
            None,
            Some(t.max_fee_per_gas.to_string()),
            Some(t.max_priority_fee_per_gas.to_string()),
            2u8,
        ),
        Transaction::Eip4844(t) => (
            Some(format!("{:?}", t.to)),
            t.value.to_string(),
            t.gas_limit.to_string(),
            t.nonce,
            format!("{:?}", t.input),
            None,
            Some(t.max_fee_per_gas.to_string()),
            Some(t.max_priority_fee_per_gas.to_string()),
            3u8,
        ),
        Transaction::Eip7702(t) => (
            Some(format!("{:?}", t.to)),
            t.value.to_string(),
            t.gas_limit.to_string(),
            t.nonce,
            format!("{:?}", t.input),
            None,
            Some(t.max_fee_per_gas.to_string()),
            Some(t.max_priority_fee_per_gas.to_string()),
            4u8,
        ),
    };

    json!({
        "hash": format!("{:?}", tx.hash()),
        "to": to.as_ref().map(|a| stub_address_param(a)),
        "created_contract": null,
        "result": if status == Some("ok") { "success" } else { "error" },
        "confirmations": 1,
        "status": status,
        "block_number": block_num,
        "timestamp": timestamp,
        "confirmation_duration": null,
        "from": stub_address_param(&from),
        "value": value,
        "fee": { "type": "actual", "value": "0" },
        "gas_price": gas_price,
        "type": tx_type,
        "gas_used": gas_used,
        "gas_limit": gas_limit,
        "max_fee_per_gas": max_fee,
        "max_priority_fee_per_gas": max_priority_fee,
        "priority_fee": null,
        "base_fee_per_gas": null,
        "transaction_burnt_fee": null,
        "nonce": nonce,
        "position": 0,
        "revert_reason": null,
        "raw_input": input,
        "decoded_input": null,
        "token_transfers": null,
        "token_transfers_overflow": false,
        "exchange_rate": null,
        "method": null,
        "transaction_types": [],
        "transaction_tag": null,
        "actions": [],
        "has_error_in_internal_transactions": null
    })
}

// ============ STUB DATA HELPERS ============

fn stub_block(height: u64) -> Value {
    let timestamp = chrono::Utc::now().to_rfc3339();
    json!({
        "height": height,
        "timestamp": timestamp,
        "transactions_count": 0,
        "internal_transactions_count": 0,
        "miner": stub_address_param("0x0000000000000000000000000000000000000000"),
        "size": 0,
        "hash": format!("0x{:064x}", height),
        "parent_hash": format!("0x{:064x}", height.saturating_sub(1)),
        "difficulty": "0",
        "total_difficulty": null,
        "gas_used": "0",
        "gas_limit": "30000000",
        "nonce": "0x0000000000000000",
        "base_fee_per_gas": null,
        "burnt_fees": null,
        "priority_fee": null,
        "extra_data": null,
        "state_root": null,
        "rewards": [],
        "gas_target_percentage": null,
        "gas_used_percentage": null,
        "burnt_fees_percentage": null,
        "type": "block",
        "transaction_fees": null,
        "uncles_hashes": []
    })
}

fn stub_transaction(hash: &str) -> Value {
    let timestamp = chrono::Utc::now().to_rfc3339();
    json!({
        "hash": hash,
        "to": null,
        "created_contract": null,
        "result": "success",
        "confirmations": 1,
        "status": "ok",
        "block_number": 0,
        "timestamp": timestamp,
        "confirmation_duration": null,
        "from": stub_address_param("0x0000000000000000000000000000000000000000"),
        "value": "0",
        "fee": { "type": "actual", "value": "0" },
        "gas_price": "0",
        "type": 2,
        "gas_used": "0",
        "gas_limit": "21000",
        "max_fee_per_gas": null,
        "max_priority_fee_per_gas": null,
        "priority_fee": null,
        "base_fee_per_gas": null,
        "transaction_burnt_fee": null,
        "nonce": 0,
        "position": 0,
        "revert_reason": null,
        "raw_input": "0x",
        "decoded_input": null,
        "token_transfers": null,
        "token_transfers_overflow": false,
        "exchange_rate": null,
        "method": null,
        "transaction_types": [],
        "transaction_tag": null,
        "actions": [],
        "has_error_in_internal_transactions": null
    })
}

fn stub_address(hash: &str) -> Value {
    json!({
        "hash": hash,
        "block_number_balance_updated_at": null,
        "coin_balance": "0",
        "creator_address_hash": null,
        "creation_transaction_hash": null,
        "creation_status": null,
        "exchange_rate": null,
        "ens_domain_name": null,
        "has_logs": false,
        "has_token_transfers": false,
        "has_tokens": false,
        "has_validated_blocks": false,
        "implementations": null,
        "is_contract": false,
        "is_verified": false,
        "name": null,
        "token": null,
        "watchlist_address_id": null
    })
}

fn stub_address_param(hash: &str) -> Value {
    json!({
        "hash": hash,
        "implementations": null,
        "is_contract": false,
        "is_verified": false,
        "name": null,
        "ens_domain_name": null
    })
}

fn stub_token(hash: &str) -> Value {
    json!({
        "address": hash,
        "circulating_market_cap": null,
        "decimals": "18",
        "exchange_rate": null,
        "holders": "0",
        "icon_url": null,
        "name": "Unknown Token",
        "symbol": "???",
        "total_supply": "0",
        "type": "ERC-20"
    })
}

// ============ REAL ENDPOINTS ============

async fn health() -> impl IntoResponse {
    Json(serde_json::json!({ "status": "ok" }))
}

async fn indexing_status(State(state): State<Arc<ApiState>>) -> impl IntoResponse {
    let last_block = state.index.last_indexed_block().await.unwrap_or(None);
    Json(serde_json::json!({
        "finished_indexing": true,
        "indexed_blocks_ratio": "1.00",
        "last_indexed_block": last_block
    }))
}

async fn get_address_txs(
    State(state): State<Arc<ApiState>>,
    Path(hash): Path<String>,
    Query(params): Query<PaginationParams>,
) -> Result<impl IntoResponse, ApiError> {
    let address: Address = hash.parse().map_err(|_| ApiError::InvalidAddress)?;

    let txs = state
        .index
        .get_address_txs(&address, params.limit + 1, params.offset)
        .await
        .map_err(|_| ApiError::DatabaseError)?;

    let has_more = txs.len() > params.limit;
    let items: Vec<_> = txs
        .into_iter()
        .take(params.limit)
        .map(|h| format!("{:?}", h))
        .collect();

    let next_page_params = if has_more {
        Some(NextPageParams {
            offset: params.offset + params.limit,
            limit: params.limit,
        })
    } else {
        None
    };

    Ok(Json(ApiResponse {
        items,
        next_page_params,
    }))
}

async fn get_address_transfers(
    State(state): State<Arc<ApiState>>,
    Path(hash): Path<String>,
    Query(params): Query<PaginationParams>,
) -> Result<impl IntoResponse, ApiError> {
    let address: Address = hash.parse().map_err(|_| ApiError::InvalidAddress)?;

    let transfers = state
        .index
        .get_address_transfers(&address, params.limit + 1, params.offset)
        .await
        .map_err(|_| ApiError::DatabaseError)?;

    let has_more = transfers.len() > params.limit;
    let items: Vec<_> = transfers.into_iter().take(params.limit).collect();

    let next_page_params = if has_more {
        Some(NextPageParams {
            offset: params.offset + params.limit,
            limit: params.limit,
        })
    } else {
        None
    };

    Ok(Json(ApiResponse {
        items,
        next_page_params,
    }))
}

async fn get_token_holders(
    State(state): State<Arc<ApiState>>,
    Path(hash): Path<String>,
    Query(params): Query<PaginationParams>,
) -> Result<impl IntoResponse, ApiError> {
    let token: Address = hash.parse().map_err(|_| ApiError::InvalidAddress)?;

    let holders = state
        .index
        .get_token_holders(&token, params.limit + 1, params.offset)
        .await
        .map_err(|_| ApiError::DatabaseError)?;

    let has_more = holders.len() > params.limit;
    let items: Vec<_> = holders
        .into_iter()
        .take(params.limit)
        .map(|(addr, bal)| {
            serde_json::json!({
                "address": format!("{:?}", addr),
                "balance": bal.to_string(),
            })
        })
        .collect();

    let next_page_params = if has_more {
        Some(NextPageParams {
            offset: params.offset + params.limit,
            limit: params.limit,
        })
    } else {
        None
    };

    Ok(Json(ApiResponse {
        items,
        next_page_params,
    }))
}

#[derive(Debug)]
pub enum ApiError {
    InvalidAddress,
    DatabaseError,
    #[allow(dead_code)]
    NotFound,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        let (status, message) = match self {
            ApiError::InvalidAddress => (StatusCode::BAD_REQUEST, "Invalid address"),
            ApiError::DatabaseError => (StatusCode::INTERNAL_SERVER_ERROR, "Database error"),
            ApiError::NotFound => (StatusCode::NOT_FOUND, "Not found"),
        };

        (status, Json(serde_json::json!({ "error": message }))).into_response()
    }
}
