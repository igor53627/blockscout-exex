use std::sync::Arc;

use alloy_primitives::{Address, U256 as AlloyU256};
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
pub use crate::meili::SearchClient;
#[cfg(feature = "reth")]
use crate::reth_reader::RethReader;
use crate::websocket::{websocket_handler, Broadcaster};

pub struct ApiState {
    pub index: Arc<FdbIndex>,
    #[cfg(feature = "reth")]
    pub reth: Option<RethReader>,
    pub broadcaster: Broadcaster,
    pub rpc_url: Option<String>,
    pub search: Option<SearchClient>,
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
        // WebSocket endpoint for Phoenix Channels compatibility
        .route("/socket/v2/websocket", get(websocket_handler))
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
        .route("/api/v2/transactions/:hash/state-changes", get(tx_state_changes))
        .route("/api/v2/transactions/:hash/raw-trace", get(tx_raw_trace))
        .route("/api/v2/addresses/:hash", get(address_by_hash))
        .route("/api/v2/addresses/:hash/counters", get(address_counters))
        .route("/api/v2/addresses/:hash/tabs-counters", get(address_tabs_counters))
        .route("/api/v2/tokens/:hash", get(token_by_hash))
        .route("/api/v2/tokens/:hash/counters", get(token_counters))
        .route("/api/v2/tokens/:hash/transfers", get(token_transfers))
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

    let total_txs = state.index.get_total_txs().await.unwrap_or(0);
    let total_transfers = state.index.get_total_transfers().await.unwrap_or(0);

    Json(json!({
        "total_blocks": last_block.to_string(),
        "total_addresses": "0",
        "total_transactions": total_txs.to_string(),
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
        "tvl": null,
        "total_token_transfers": total_transfers.to_string()
    }))
}

async fn stats_charts_txs(State(state): State<Arc<ApiState>>) -> impl IntoResponse {
    match state.index.get_daily_tx_metrics(31).await {
        Ok(metrics) => {
            tracing::info!("Daily metrics returned {} entries", metrics.len());
            let chart_data: Vec<Value> = metrics
                .into_iter()
                .map(|(date, count)| {
                    json!({
                        "date": date,
                        "transactions_count": count
                    })
                })
                .collect();
            Json(json!({
                "chart_data": chart_data
            }))
        }
        Err(e) => {
            tracing::error!("Failed to get daily metrics: {:?}", e);
            Json(json!({
                "chart_data": []
            }))
        }
    }
}

async fn stats_charts_market() -> impl IntoResponse {
    Json(json!({
        "chart_data": [],
        "available_resolutions": ["DAY"]
    }))
}

async fn blocks(State(state): State<Arc<ApiState>>) -> impl IntoResponse {
    // Try RPC first for latest blocks
    if let Some(ref rpc_url) = state.rpc_url {
        if let Ok(block) = fetch_block_rpc(rpc_url, "latest").await {
            if let Some(num_hex) = block["number"].as_str() {
                let last_block = hex_to_u64(num_hex);
                let mut items = Vec::new();
                
                for i in 0..10 {
                    if let Some(height) = last_block.checked_sub(i) {
                        if let Ok(blk) = fetch_block_rpc(rpc_url, &format!("0x{:x}", height)).await {
                            items.push(rpc_block_to_json(&blk, height));
                        }
                    }
                }
                
                return Json(json!({
                    "items": items,
                    "next_page_params": null
                }));
            }
        }
    }

    // Fallback to FDB index
    let last_block = state.index.last_indexed_block().await.unwrap_or(None).unwrap_or(0);
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

async fn block_txs(
    State(state): State<Arc<ApiState>>,
    Path(height_or_hash): Path<String>,
) -> impl IntoResponse {
    if let Some(ref rpc_url) = state.rpc_url {
        let block_param = if height_or_hash.starts_with("0x") {
            height_or_hash.clone()
        } else if let Ok(num) = height_or_hash.parse::<u64>() {
            format!("0x{:x}", num)
        } else {
            height_or_hash.clone()
        };

        if let Ok(block) = fetch_block_rpc(rpc_url, &block_param).await {
            if let Some(txs) = block["transactions"].as_array() {
                let block_num = block["number"]
                    .as_str()
                    .and_then(|s| u64::from_str_radix(s.trim_start_matches("0x"), 16).ok())
                    .unwrap_or(0);
                let timestamp = block["timestamp"]
                    .as_str()
                    .and_then(|s| u64::from_str_radix(s.trim_start_matches("0x"), 16).ok())
                    .unwrap_or(0);
                let base_fee = block["baseFeePerGas"]
                    .as_str()
                    .map(|s| hex_to_u128(s));
                let ts = chrono::DateTime::from_timestamp(timestamp as i64, 0)
                    .map(|dt| dt.to_rfc3339())
                    .unwrap_or_default();

                // Fetch receipts for gas_used
                let receipts = fetch_block_receipts_rpc(rpc_url, &block_param)
                    .await
                    .ok()
                    .and_then(|r| r.as_array().cloned());

                let items: Vec<Value> = txs
                    .iter()
                    .enumerate()
                    .map(|(i, tx)| {
                        let receipt = receipts.as_ref().and_then(|r| r.get(i));
                        rpc_tx_to_json(tx, block_num, &ts, base_fee, receipt)
                    })
                    .collect();

                return Json(json!({
                    "items": items,
                    "next_page_params": null
                }));
            }
        }
    }

    Json(json!({
        "items": [],
        "next_page_params": null
    }))
}

async fn transactions(
    State(state): State<Arc<ApiState>>,
    Query(params): Query<PaginationParams>,
) -> impl IntoResponse {
    // Get recent transactions from the last few blocks via RPC
    if let Some(ref rpc_url) = state.rpc_url {
        // Get latest block number
        if let Ok(latest_block) = fetch_block_rpc(rpc_url, "latest").await {
            let latest_num = latest_block["number"]
                .as_str()
                .and_then(|s| u64::from_str_radix(s.trim_start_matches("0x"), 16).ok())
                .unwrap_or(0);

            let mut all_txs: Vec<Value> = Vec::new();
            let limit = params.limit.min(50);
            
            // Fetch transactions from recent blocks until we have enough
            let mut current_block = latest_num;
            let min_block = latest_num.saturating_sub(20); // Look back max 20 blocks
            
            while all_txs.len() < limit && current_block > min_block {
                let block_param = format!("0x{:x}", current_block);
                if let Ok(block) = fetch_block_rpc(rpc_url, &block_param).await {
                    if let Some(txs) = block["transactions"].as_array() {
                        let timestamp = block["timestamp"]
                            .as_str()
                            .and_then(|s| u64::from_str_radix(s.trim_start_matches("0x"), 16).ok())
                            .unwrap_or(0);
                        let base_fee = block["baseFeePerGas"]
                            .as_str()
                            .map(|s| hex_to_u128(s));
                        let ts = chrono::DateTime::from_timestamp(timestamp as i64, 0)
                            .map(|dt| dt.to_rfc3339())
                            .unwrap_or_default();

                        // Fetch receipts
                        let receipts = fetch_block_receipts_rpc(rpc_url, &block_param)
                            .await
                            .ok()
                            .and_then(|r| r.as_array().cloned());

                        for (i, tx) in txs.iter().enumerate() {
                            if all_txs.len() >= limit {
                                break;
                            }
                            let receipt = receipts.as_ref().and_then(|r| r.get(i));
                            all_txs.push(rpc_tx_to_json(tx, current_block, &ts, base_fee, receipt));
                        }
                    }
                }
                current_block = current_block.saturating_sub(1);
            }

            return Json(json!({
                "items": all_txs,
                "next_page_params": null
            }));
        }
    }

    Json(json!({
        "items": [],
        "next_page_params": null
    }))
}

async fn transaction_by_hash(
    State(state): State<Arc<ApiState>>,
    Path(hash): Path<String>,
) -> impl IntoResponse {
    // Try RPC first
    if let Some(ref rpc_url) = state.rpc_url {
        if let Ok(tx) = fetch_tx_rpc(rpc_url, &hash).await {
            if !tx.is_null() {
                let receipt = fetch_tx_receipt_rpc(rpc_url, &hash).await.ok();
                let block_num = tx["blockNumber"]
                    .as_str()
                    .map(|s| hex_to_u64(s));
                let base_fee = if let Some(bn) = block_num {
                    if let Ok(block) = fetch_block_rpc(rpc_url, &format!("0x{:x}", bn)).await {
                        block["baseFeePerGas"].as_str().map(|s| hex_to_u128(s))
                    } else {
                        None
                    }
                } else {
                    None
                };
                let timestamp = if let Some(bn) = block_num {
                    if let Ok(block) = fetch_block_rpc(rpc_url, &format!("0x{:x}", bn)).await {
                        let ts = block["timestamp"].as_str().map(|s| hex_to_u64(s)).unwrap_or(0);
                        chrono::DateTime::from_timestamp(ts as i64, 0)
                            .map(|dt| dt.to_rfc3339())
                            .unwrap_or_default()
                    } else {
                        chrono::Utc::now().to_rfc3339()
                    }
                } else {
                    chrono::Utc::now().to_rfc3339()
                };
                return Json(rpc_tx_to_json(&tx, block_num.unwrap_or(0), &timestamp, base_fee, receipt.as_ref().filter(|r| !r.is_null())));
            }
        }
    }

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

async fn tx_token_transfers(
    State(state): State<Arc<ApiState>>,
    Path(hash): Path<String>,
) -> impl IntoResponse {
    if let Some(ref rpc_url) = state.rpc_url {
        // Get transaction receipt for logs
        if let Ok(receipt) = fetch_tx_receipt_rpc(rpc_url, &hash).await {
            if !receipt.is_null() {
                let block_num = receipt["blockNumber"]
                    .as_str()
                    .map(|s| hex_to_u64(s))
                    .unwrap_or(0);
                let block_hash = receipt["blockHash"].as_str().unwrap_or_default();
                
                let mut items = Vec::new();
                
                // Event signatures
                const TRANSFER_SIG: &str = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
                const TRANSFER_SINGLE_SIG: &str = "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62";
                const TRANSFER_BATCH_SIG: &str = "0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb";
                
                if let Some(logs) = receipt["logs"].as_array() {
                    for log in logs {
                        if let Some(topics) = log["topics"].as_array() {
                            let topic0 = topics.first().and_then(|t| t.as_str()).unwrap_or_default();
                            let token_addr = log["address"].as_str().unwrap_or_default();
                            let log_index = log["logIndex"].as_str().map(|s| hex_to_u64(s)).unwrap_or(0);
                            
                            // ERC-721 Transfer (4 topics)
                            if topic0 == TRANSFER_SIG && topics.len() == 4 {
                                let from = format!("0x{}", &topics[1].as_str().unwrap_or_default()[26..]);
                                let to = format!("0x{}", &topics[2].as_str().unwrap_or_default()[26..]);
                                let token_id = hex_to_u256(topics[3].as_str().unwrap_or("0x0"));
                                
                                let transfer_type = if from == "0x0000000000000000000000000000000000000000" {
                                    "token_minting"
                                } else if to == "0x0000000000000000000000000000000000000000" {
                                    "token_burning"
                                } else {
                                    "token_transfer"
                                };
                                
                                let (name, symbol, _) = fetch_token_metadata(rpc_url, token_addr).await;
                                
                                items.push(json!({
                                    "block_hash": block_hash,
                                    "block_number": block_num,
                                    "from": stub_address_param(&from),
                                    "to": stub_address_param(&to),
                                    "log_index": log_index,
                                    "method": null,
                                    "timestamp": null,
                                    "token": {
                                        "address_hash": token_addr,
                                        "circulating_market_cap": null,
                                        "decimals": null,
                                        "exchange_rate": null,
                                        "holders_count": null,
                                        "icon_url": null,
                                        "name": name,
                                        "reputation": "ok",
                                        "symbol": symbol,
                                        "total_supply": null,
                                        "type": "ERC-721",
                                        "volume_24h": null
                                    },
                                    "token_type": "ERC-721",
                                    "total": {
                                        "token_id": token_id,
                                        "token_instance": null
                                    },
                                    "transaction_hash": hash,
                                    "type": transfer_type
                                }));
                            }
                            // ERC-20 Transfer (3 topics)
                            else if topic0 == TRANSFER_SIG && topics.len() == 3 {
                                let from = format!("0x{}", &topics[1].as_str().unwrap_or_default()[26..]);
                                let to = format!("0x{}", &topics[2].as_str().unwrap_or_default()[26..]);
                                let data = log["data"].as_str().unwrap_or("0x0");
                                let value = hex_to_u256(data);
                                
                                let transfer_type = if from == "0x0000000000000000000000000000000000000000" {
                                    "token_minting"
                                } else if to == "0x0000000000000000000000000000000000000000" {
                                    "token_burning"
                                } else {
                                    "token_transfer"
                                };
                                
                                let (name, symbol, decimals) = fetch_token_metadata(rpc_url, token_addr).await;
                                
                                items.push(json!({
                                    "block_hash": block_hash,
                                    "block_number": block_num,
                                    "from": stub_address_param(&from),
                                    "to": stub_address_param(&to),
                                    "log_index": log_index,
                                    "method": null,
                                    "timestamp": null,
                                    "token": {
                                        "address_hash": token_addr,
                                        "circulating_market_cap": null,
                                        "decimals": decimals,
                                        "exchange_rate": null,
                                        "holders_count": null,
                                        "icon_url": null,
                                        "name": name,
                                        "reputation": "ok",
                                        "symbol": symbol,
                                        "total_supply": null,
                                        "type": "ERC-20",
                                        "volume_24h": null
                                    },
                                    "token_type": "ERC-20",
                                    "total": {
                                        "decimals": decimals,
                                        "value": value
                                    },
                                    "transaction_hash": hash,
                                    "type": transfer_type
                                }));
                            }
                            // ERC-1155 TransferSingle (4 topics + data)
                            else if topic0 == TRANSFER_SINGLE_SIG && topics.len() == 4 {
                                let from = format!("0x{}", &topics[2].as_str().unwrap_or_default()[26..]);
                                let to = format!("0x{}", &topics[3].as_str().unwrap_or_default()[26..]);
                                let data = log["data"].as_str().unwrap_or("0x");
                                let data_bytes = hex::decode(data.trim_start_matches("0x")).unwrap_or_default();
                                let (token_id, value) = if data_bytes.len() >= 64 {
                                    (hex_to_u256(&format!("0x{}", hex::encode(&data_bytes[0..32]))),
                                     hex_to_u256(&format!("0x{}", hex::encode(&data_bytes[32..64]))))
                                } else {
                                    ("0".to_string(), "0".to_string())
                                };
                                
                                let transfer_type = if from == "0x0000000000000000000000000000000000000000" {
                                    "token_minting"
                                } else if to == "0x0000000000000000000000000000000000000000" {
                                    "token_burning"
                                } else {
                                    "token_transfer"
                                };
                                
                                let (name, symbol, _) = fetch_token_metadata(rpc_url, token_addr).await;
                                
                                items.push(json!({
                                    "block_hash": block_hash,
                                    "block_number": block_num,
                                    "from": stub_address_param(&from),
                                    "to": stub_address_param(&to),
                                    "log_index": log_index,
                                    "method": null,
                                    "timestamp": null,
                                    "token": {
                                        "address_hash": token_addr,
                                        "circulating_market_cap": null,
                                        "decimals": null,
                                        "exchange_rate": null,
                                        "holders_count": null,
                                        "icon_url": null,
                                        "name": name,
                                        "reputation": "ok",
                                        "symbol": symbol,
                                        "total_supply": null,
                                        "type": "ERC-1155",
                                        "volume_24h": null
                                    },
                                    "token_type": "ERC-1155",
                                    "total": {
                                        "token_id": token_id,
                                        "value": value,
                                        "token_instance": null
                                    },
                                    "transaction_hash": hash,
                                    "type": transfer_type
                                }));
                            }
                            // ERC-1155 TransferBatch - simplified, just note it exists
                            else if topic0 == TRANSFER_BATCH_SIG && topics.len() == 4 {
                                let from = format!("0x{}", &topics[2].as_str().unwrap_or_default()[26..]);
                                let to = format!("0x{}", &topics[3].as_str().unwrap_or_default()[26..]);
                                
                                let transfer_type = if from == "0x0000000000000000000000000000000000000000" {
                                    "token_minting"
                                } else if to == "0x0000000000000000000000000000000000000000" {
                                    "token_burning"
                                } else {
                                    "token_transfer"
                                };
                                
                                let (name, symbol, _) = fetch_token_metadata(rpc_url, token_addr).await;
                                
                                items.push(json!({
                                    "block_hash": block_hash,
                                    "block_number": block_num,
                                    "from": stub_address_param(&from),
                                    "to": stub_address_param(&to),
                                    "log_index": log_index,
                                    "method": null,
                                    "timestamp": null,
                                    "token": {
                                        "address_hash": token_addr,
                                        "circulating_market_cap": null,
                                        "decimals": null,
                                        "exchange_rate": null,
                                        "holders_count": null,
                                        "icon_url": null,
                                        "name": name,
                                        "reputation": "ok",
                                        "symbol": symbol,
                                        "total_supply": null,
                                        "type": "ERC-1155",
                                        "volume_24h": null
                                    },
                                    "token_type": "ERC-1155",
                                    "total": {
                                        "token_id": "batch",
                                        "value": "batch",
                                        "token_instance": null
                                    },
                                    "transaction_hash": hash,
                                    "type": transfer_type
                                }));
                            }
                        }
                    }
                }
                
                return Json(json!({
                    "items": items,
                    "next_page_params": null
                }));
            }
        }
    }
    
    Json(json!({
        "items": [],
        "next_page_params": null
    }))
}

async fn tx_logs(
    State(state): State<Arc<ApiState>>,
    Path(hash): Path<String>,
) -> impl IntoResponse {
    if let Some(ref rpc_url) = state.rpc_url {
        // Get transaction receipt for logs
        if let Ok(receipt) = fetch_tx_receipt_rpc(rpc_url, &hash).await {
            if !receipt.is_null() {
                let block_num = receipt["blockNumber"]
                    .as_str()
                    .map(|s| hex_to_u64(s))
                    .unwrap_or(0);
                let block_hash = receipt["blockHash"].as_str().unwrap_or_default();
                
                let mut items = Vec::new();
                
                if let Some(logs) = receipt["logs"].as_array() {
                    for log in logs {
                        let addr = log["address"].as_str().unwrap_or_default();
                        let data = log["data"].as_str().unwrap_or("0x");
                        let log_index = log["logIndex"].as_str().map(|s| hex_to_u64(s)).unwrap_or(0);
                        
                        // Convert topics to array of strings (with null padding to 4)
                        let topics: Vec<Value> = log["topics"]
                            .as_array()
                            .map(|arr| {
                                let mut result: Vec<Value> = arr.iter()
                                    .map(|t| json!(t.as_str().unwrap_or_default()))
                                    .collect();
                                // Pad to 4 topics with null
                                while result.len() < 4 {
                                    result.push(json!(null));
                                }
                                result
                            })
                            .unwrap_or_else(|| vec![json!(null); 4]);
                        
                        items.push(json!({
                            "address": stub_address_param(addr),
                            "block_hash": block_hash,
                            "block_number": block_num,
                            "data": data,
                            "decoded": null,
                            "index": log_index,
                            "smart_contract": null,
                            "topics": topics,
                            "transaction_hash": hash
                        }));
                    }
                }
                
                return Json(json!({
                    "items": items,
                    "next_page_params": null
                }));
            }
        }
    }
    
    Json(json!({
        "items": [],
        "next_page_params": null
    }))
}

async fn tx_internal_txs(
    State(state): State<Arc<ApiState>>,
    Path(hash): Path<String>,
) -> impl IntoResponse {
    if let Some(ref rpc_url) = state.rpc_url {
        // First get tx info for block number and tx index
        if let Ok(tx) = fetch_tx_rpc(rpc_url, &hash).await {
            if tx.is_null() {
                return Json(json!({ "items": [], "next_page_params": null }));
            }
            
            let block_num = tx["blockNumber"]
                .as_str()
                .map(|s| hex_to_u64(s))
                .unwrap_or(0);
            let tx_index = tx["transactionIndex"]
                .as_str()
                .map(|s| hex_to_u64(s))
                .unwrap_or(0);
            
            // Get block timestamp
            let timestamp = if let Ok(block) = fetch_block_rpc(rpc_url, &format!("0x{:x}", block_num)).await {
                let ts = block["timestamp"].as_str().map(|s| hex_to_u64(s)).unwrap_or(0);
                chrono::DateTime::from_timestamp(ts as i64, 0)
                    .map(|dt| dt.to_rfc3339())
                    .unwrap_or_default()
            } else {
                chrono::Utc::now().to_rfc3339()
            };
            
            // Try trace_transaction first
            let client = reqwest::Client::new();
            let resp = client
                .post(rpc_url)
                .json(&json!({
                    "jsonrpc": "2.0",
                    "method": "trace_transaction",
                    "params": [hash],
                    "id": 1
                }))
                .send()
                .await;
            
            if let Ok(resp) = resp {
                if let Ok(body) = resp.json::<Value>().await {
                    if let Some(traces) = body.get("result").and_then(|r| r.as_array()) {
                        let items: Vec<Value> = traces.iter().enumerate().map(|(i, trace)| {
                            let action = &trace["action"];
                            let result = &trace["result"];
                            let trace_type = trace["type"].as_str().unwrap_or("call");
                            let call_type = action["callType"].as_str().unwrap_or(trace_type);
                            
                            json!({
                                "block_index": i,
                                "block_number": block_num,
                                "created_contract": if trace_type == "create" { 
                                    result["address"].as_str().map(|a| stub_address_param(a))
                                } else { None::<Value> },
                                "error": trace["error"].as_str(),
                                "from": stub_address_param(action["from"].as_str().unwrap_or_default()),
                                "gas_limit": action["gas"].as_str().map(|s| hex_to_u64(s).to_string()),
                                "index": i + 1,
                                "success": trace["error"].is_null(),
                                "timestamp": timestamp,
                                "to": stub_address_param(action["to"].as_str().unwrap_or_default()),
                                "transaction_hash": hash,
                                "transaction_index": tx_index,
                                "type": call_type,
                                "value": action["value"].as_str().map(|s| hex_to_u128(s).to_string()).unwrap_or_else(|| "0".to_string())
                            })
                        }).collect();
                        
                        return Json(json!({
                            "items": items,
                            "next_page_params": null
                        }));
                    }
                }
            }
            
            // Fallback to debug_traceTransaction with callTracer
            let resp = client
                .post(rpc_url)
                .json(&json!({
                    "jsonrpc": "2.0",
                    "method": "debug_traceTransaction",
                    "params": [hash, {"tracer": "callTracer"}],
                    "id": 1
                }))
                .send()
                .await;
            
            if let Ok(resp) = resp {
                if let Ok(body) = resp.json::<Value>().await {
                    if let Some(result) = body.get("result") {
                        if !result.is_null() {
                            let items = convert_call_trace_to_internal_txs(result, block_num, tx_index, &timestamp, &hash);
                            return Json(json!({
                                "items": items,
                                "next_page_params": null
                            }));
                        }
                    }
                }
            }
        }
    }
    
    Json(json!({
        "items": [],
        "next_page_params": null
    }))
}

// Convert callTracer output to internal transactions format
fn convert_call_trace_to_internal_txs(
    call_trace: &Value,
    block_num: u64,
    tx_index: u64,
    timestamp: &str,
    tx_hash: &str,
) -> Vec<Value> {
    let mut items = Vec::new();
    let mut block_index = 0;
    flatten_call_trace_to_internal_txs(call_trace, &mut items, &mut block_index, block_num, tx_index, timestamp, tx_hash);
    items
}

fn flatten_call_trace_to_internal_txs(
    trace: &Value,
    output: &mut Vec<Value>,
    block_index: &mut usize,
    block_num: u64,
    tx_index: u64,
    timestamp: &str,
    tx_hash: &str,
) {
    let call_type = trace["type"].as_str().unwrap_or("call").to_lowercase();
    let from = trace["from"].as_str().unwrap_or_default();
    let to = trace["to"].as_str().unwrap_or_default();
    let value = trace["value"].as_str().map(|s| hex_to_u128(s)).unwrap_or(0);
    let gas = trace["gas"].as_str().map(|s| hex_to_u64(s)).unwrap_or(0);
    let error = trace["error"].as_str();
    
    *block_index += 1;
    let current_index = *block_index;
    
    output.push(json!({
        "block_index": current_index - 1,
        "block_number": block_num,
        "created_contract": null,
        "error": error,
        "from": stub_address_param(from),
        "gas_limit": gas.to_string(),
        "index": current_index,
        "success": error.is_none(),
        "timestamp": timestamp,
        "to": stub_address_param(to),
        "transaction_hash": tx_hash,
        "transaction_index": tx_index,
        "type": call_type,
        "value": value.to_string()
    }));
    
    // Process subcalls
    if let Some(calls) = trace["calls"].as_array() {
        for subcall in calls {
            flatten_call_trace_to_internal_txs(subcall, output, block_index, block_num, tx_index, timestamp, tx_hash);
        }
    }
}

async fn tx_state_changes(
    State(state): State<Arc<ApiState>>,
    Path(hash): Path<String>,
) -> impl IntoResponse {
    // Use debug_traceTransaction with prestateTracer to get state changes
    if let Some(ref rpc_url) = state.rpc_url {
        // First get the transaction to find the block and involved addresses
        if let Ok(tx) = fetch_tx_rpc(rpc_url, &hash).await {
            if tx.is_null() {
                return Json(json!({ "items": [], "next_page_params": null }));
            }
            
            let block_num = tx["blockNumber"]
                .as_str()
                .and_then(|s| u64::from_str_radix(s.trim_start_matches("0x"), 16).ok());
            
            if block_num.is_none() {
                return Json(json!({ "items": [], "next_page_params": null }));
            }
            
            // Try to get state diff using debug_traceTransaction
            let trace_result = trace_tx_state_diff(rpc_url, &hash).await;
            
            if let Ok(state_diff) = trace_result {
                let items = parse_state_diff(&state_diff, &tx);
                return Json(json!({
                    "items": items,
                    "next_page_params": null
                }));
            }
            
            // Fallback: return basic state changes from tx sender/receiver
            let from = tx["from"].as_str().unwrap_or_default();
            let to = tx["to"].as_str();
            let value = tx["value"].as_str().map(|s| hex_to_u128(s)).unwrap_or(0);
            
            let mut items = Vec::new();
            
            // Add sender (value decrease)
            if !from.is_empty() && value > 0 {
                items.push(json!({
                    "address": {
                        "hash": from,
                        "is_contract": false,
                        "is_verified": false,
                        "is_scam": false,
                        "name": null,
                        "ens_domain_name": null,
                        "implementations": [],
                        "private_tags": [],
                        "public_tags": [],
                        "watchlist_names": [],
                        "proxy_type": null,
                        "metadata": null,
                        "reputation": "ok"
                    },
                    "balance_before": null,
                    "balance_after": null,
                    "change": format!("-{}", value),
                    "is_miner": false,
                    "token": null,
                    "token_id": null,
                    "type": "coin"
                }));
            }
            
            // Add receiver (value increase)
            if let Some(to_addr) = to {
                if value > 0 {
                    items.push(json!({
                        "address": {
                            "hash": to_addr,
                            "is_contract": false,
                            "is_verified": false,
                            "is_scam": false,
                            "name": null,
                            "ens_domain_name": null,
                            "implementations": [],
                            "private_tags": [],
                            "public_tags": [],
                            "watchlist_names": [],
                            "proxy_type": null,
                            "metadata": null,
                            "reputation": "ok"
                        },
                        "balance_before": null,
                        "balance_after": null,
                        "change": value.to_string(),
                        "is_miner": false,
                        "token": null,
                        "token_id": null,
                        "type": "coin"
                    }));
                }
            }
            
            return Json(json!({
                "items": items,
                "next_page_params": null
            }));
        }
    }
    
    Json(json!({
        "items": [],
        "next_page_params": null
    }))
}

// Helper to trace transaction state diff
async fn trace_tx_state_diff(rpc_url: &str, tx_hash: &str) -> Result<Value, ()> {
    let client = reqwest::Client::new();
    let resp = client
        .post(rpc_url)
        .json(&json!({
            "jsonrpc": "2.0",
            "method": "debug_traceTransaction",
            "params": [tx_hash, {"tracer": "prestateTracer", "tracerConfig": {"diffMode": true}}],
            "id": 1
        }))
        .send()
        .await
        .map_err(|_| ())?;
    
    let body: Value = resp.json().await.map_err(|_| ())?;
    Ok(body["result"].clone())
}

// Parse state diff into Blockscout format
fn parse_state_diff(state_diff: &Value, _tx: &Value) -> Vec<Value> {
    let mut items = Vec::new();
    
    // state_diff has "pre" and "post" objects with address -> { balance, nonce, ... }
    if let (Some(pre), Some(post)) = (state_diff["pre"].as_object(), state_diff["post"].as_object()) {
        // Collect all addresses from both pre and post
        let mut all_addrs: std::collections::HashSet<&str> = std::collections::HashSet::new();
        for addr in pre.keys() {
            all_addrs.insert(addr);
        }
        for addr in post.keys() {
            all_addrs.insert(addr);
        }
        
        for addr in all_addrs {
            let pre_balance = pre.get(addr)
                .and_then(|v| v["balance"].as_str())
                .map(|s| hex_to_u128(s))
                .unwrap_or(0);
            let post_balance = post.get(addr)
                .and_then(|v| v["balance"].as_str())
                .map(|s| hex_to_u128(s))
                .unwrap_or(0);
            
            if pre_balance != post_balance {
                let change = post_balance as i128 - pre_balance as i128;
                items.push(json!({
                    "address": {
                        "hash": addr,
                        "is_contract": false,
                        "is_verified": false,
                        "is_scam": false,
                        "name": null,
                        "ens_domain_name": null,
                        "implementations": [],
                        "private_tags": [],
                        "public_tags": [],
                        "watchlist_names": [],
                        "proxy_type": null,
                        "metadata": null,
                        "reputation": "ok"
                    },
                    "balance_before": pre_balance.to_string(),
                    "balance_after": post_balance.to_string(),
                    "change": change.to_string(),
                    "is_miner": false,
                    "token": null,
                    "token_id": null,
                    "type": "coin"
                }));
            }
        }
    }
    
    items
}

async fn tx_raw_trace(
    State(state): State<Arc<ApiState>>,
    Path(hash): Path<String>,
) -> impl IntoResponse {
    // Use trace_transaction to get the raw trace
    if let Some(ref rpc_url) = state.rpc_url {
        let client = reqwest::Client::new();
        let resp = client
            .post(rpc_url)
            .json(&json!({
                "jsonrpc": "2.0",
                "method": "trace_transaction",
                "params": [hash],
                "id": 1
            }))
            .send()
            .await;
        
        if let Ok(resp) = resp {
            if let Ok(body) = resp.json::<Value>().await {
                if let Some(result) = body.get("result") {
                    if !result.is_null() {
                        return Json(result.clone());
                    }
                }
            }
        }
        
        // Fallback: try debug_traceTransaction with callTracer
        let resp = client
            .post(rpc_url)
            .json(&json!({
                "jsonrpc": "2.0",
                "method": "debug_traceTransaction",
                "params": [hash, {"tracer": "callTracer"}],
                "id": 1
            }))
            .send()
            .await;
        
        if let Ok(resp) = resp {
            if let Ok(body) = resp.json::<Value>().await {
                if let Some(result) = body.get("result") {
                    if !result.is_null() {
                        // Convert callTracer format to trace_transaction format
                        let traces = convert_call_trace_to_trace_format(result);
                        return Json(json!(traces));
                    }
                }
            }
        }
    }
    
    Json(json!([]))
}

// Convert callTracer output to trace_transaction format
fn convert_call_trace_to_trace_format(call_trace: &Value) -> Vec<Value> {
    let mut traces = Vec::new();
    flatten_call_trace(call_trace, &mut traces, vec![]);
    traces
}

fn flatten_call_trace(trace: &Value, output: &mut Vec<Value>, trace_address: Vec<usize>) {
    let call_type = trace["type"].as_str().unwrap_or("call").to_lowercase();
    let from = trace["from"].as_str().unwrap_or_default();
    let to = trace["to"].as_str().unwrap_or_default();
    let value = trace["value"].as_str().unwrap_or("0x0");
    let gas = trace["gas"].as_str().unwrap_or("0x0");
    let gas_used = trace["gasUsed"].as_str().unwrap_or("0x0");
    let input = trace["input"].as_str().unwrap_or("0x");
    let output_data = trace["output"].as_str().unwrap_or("0x");
    
    output.push(json!({
        "action": {
            "callType": call_type,
            "from": from,
            "to": to,
            "value": value,
            "gas": gas,
            "input": input
        },
        "result": {
            "gasUsed": gas_used,
            "output": output_data
        },
        "subtraces": trace["calls"].as_array().map(|c| c.len()).unwrap_or(0),
        "traceAddress": trace_address,
        "type": "call"
    }));
    
    // Process subcalls
    if let Some(calls) = trace["calls"].as_array() {
        for (i, subcall) in calls.iter().enumerate() {
            let mut sub_address = trace_address.clone();
            sub_address.push(i);
            flatten_call_trace(subcall, output, sub_address);
        }
    }
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

async fn address_counters(
    State(state): State<Arc<ApiState>>,
    Path(hash): Path<String>,
) -> impl IntoResponse {
    let addr: Address = match hash.parse() {
        Ok(a) => a,
        Err(_) => return Json(json!({
            "transactions_count": "0",
            "token_transfers_count": "0",
            "gas_usage_count": null,
            "validations_count": "0"
        })),
    };
    
    // Use atomic counters from FDB (O(1) lookup instead of range scan)
    let tx_count = state.index.get_address_tx_count(&addr).await.unwrap_or(0);
    let transfer_count = state.index.get_address_token_transfer_count(&addr).await.unwrap_or(0);
    
    Json(json!({
        "transactions_count": tx_count.to_string(),
        "token_transfers_count": transfer_count.to_string(),
        "gas_usage_count": null,
        "validations_count": "0"
    }))
}

async fn address_tabs_counters(
    State(state): State<Arc<ApiState>>,
    Path(hash): Path<String>,
) -> impl IntoResponse {
    let addr: Address = match hash.parse() {
        Ok(a) => a,
        Err(_) => return Json(json!({
            "internal_transactions_count": 0,
            "logs_count": 0,
            "token_balances_count": 0,
            "token_transfers_count": 0,
            "transactions_count": 0,
            "validations_count": 0,
            "withdrawals_count": 0,
            "beacon_deposits_count": 0
        })),
    };
    
    // Use atomic counters from FDB (O(1) lookup instead of range scan)
    let tx_count = state.index.get_address_tx_count(&addr).await.unwrap_or(0);
    let transfer_count = state.index.get_address_token_transfer_count(&addr).await.unwrap_or(0);
    
    Json(json!({
        "internal_transactions_count": 0,
        "logs_count": 0,
        "token_balances_count": 0,
        "token_transfers_count": transfer_count,
        "transactions_count": tx_count,
        "validations_count": 0,
        "withdrawals_count": 0,
        "beacon_deposits_count": 0
    }))
}

const MAX_COUNT_CAP: usize = 10000;

async fn token_by_hash(
    State(state): State<Arc<ApiState>>,
    Path(hash): Path<String>,
) -> impl IntoResponse {
    // Fetch token metadata via RPC
    if let Some(ref rpc_url) = state.rpc_url {
        let (name, symbol, decimals) = fetch_token_metadata(rpc_url, &hash).await;
        
        // Fetch total supply
        let total_supply = fetch_token_total_supply(rpc_url, &hash).await;
        
        // Try atomic holder count first, fall back to range scan
        let holders_count = if let Ok(addr) = hash.parse::<Address>() {
            let count = state.index.get_token_holder_count(&addr).await.unwrap_or(0);
            if count > 0 {
                count as usize
            } else {
                // Fallback to range scan for data indexed before Phase 4
                state.index.count_token_holders(&addr, MAX_COUNT_CAP).await.unwrap_or(0)
            }
        } else {
            0
        };
        
        return Json(json!({
            "address_hash": hash,
            "circulating_market_cap": null,
            "decimals": decimals,
            "exchange_rate": null,
            "holders_count": holders_count.to_string(),
            "icon_url": null,
            "name": name,
            "reputation": "ok",
            "symbol": symbol,
            "total_supply": total_supply,
            "type": "ERC-20",
            "volume_24h": null
        }));
    }
    
    Json(stub_token(&hash))
}

async fn fetch_token_total_supply(rpc_url: &str, token_addr: &str) -> Option<String> {
    let client = reqwest::Client::new();
    let total_supply_selector = "0x18160ddd"; // totalSupply()
    
    let resp = client
        .post(rpc_url)
        .json(&json!({
            "jsonrpc": "2.0",
            "method": "eth_call",
            "params": [{"to": token_addr, "data": total_supply_selector}, "latest"],
            "id": 1
        }))
        .send()
        .await
        .ok()?;
    
    let body: Value = resp.json().await.ok()?;
    let result = body["result"].as_str()?;
    
    // Convert hex to decimal string
    Some(hex_to_u256(result))
}

async fn token_transfers(
    State(state): State<Arc<ApiState>>,
    Path(hash): Path<String>,
    Query(params): Query<PaginationParams>,
) -> impl IntoResponse {
    let token: Address = match hash.parse() {
        Ok(addr) => addr,
        Err(_) => return Json(json!({ "items": [], "next_page_params": null })),
    };

    let transfers = state
        .index
        .get_token_transfers(&token, params.limit + 1, params.offset)
        .await
        .unwrap_or_default();

    let has_more = transfers.len() > params.limit;
    let transfers: Vec<_> = transfers.into_iter().take(params.limit).collect();

    let items: Vec<Value> = futures::future::join_all(
        transfers.iter().map(|t| {
            let rpc_url = state.rpc_url.clone();
            async move {
                let tx_hash = format!("0x{}", hex::encode(t.tx_hash));
                let token_addr = format!("0x{}", hex::encode(t.token_address));
                let from = format!("0x{}", hex::encode(t.from));
                let to = format!("0x{}", hex::encode(t.to));
                let value = alloy_primitives::U256::from_be_bytes(t.value);

                let transfer_type = if t.from == [0u8; 20] {
                    "token_minting"
                } else if t.to == [0u8; 20] {
                    "token_burning"
                } else {
                    "token_transfer"
                };

                let timestamp = chrono::DateTime::from_timestamp(t.timestamp as i64, 0)
                    .map(|dt| dt.format("%Y-%m-%dT%H:%M:%S.000000Z").to_string())
                    .unwrap_or_default();

                let (name, symbol, decimals) = if let Some(ref url) = rpc_url {
                    let (n, s, d) = fetch_token_metadata(url, &token_addr).await;
                    (n.unwrap_or_else(|| "Unknown".to_string()), 
                     s.unwrap_or_else(|| "???".to_string()), 
                     d.unwrap_or_else(|| "18".to_string()))
                } else {
                    ("Unknown".to_string(), "???".to_string(), "18".to_string())
                };

                json!({
                    "block_hash": null,
                    "block_number": t.block_number,
                    "from": stub_address_param(&from),
                    "to": stub_address_param(&to),
                    "log_index": t.log_index,
                    "method": "transfer",
                    "timestamp": timestamp,
                    "token": {
                        "address_hash": token_addr,
                        "circulating_market_cap": null,
                        "decimals": decimals,
                        "exchange_rate": null,
                        "holders_count": null,
                        "icon_url": null,
                        "name": name,
                        "reputation": "ok",
                        "symbol": symbol,
                        "total_supply": null,
                        "type": "ERC-20",
                        "volume_24h": null
                    },
                    "token_type": "ERC-20",
                    "total": {
                        "decimals": decimals,
                        "value": value.to_string()
                    },
                    "transaction_hash": tx_hash,
                    "type": transfer_type
                })
            }
        })
    ).await;

    let next_page_params = if has_more {
        Some(json!({
            "index": params.offset + params.limit,
            "items_count": params.limit
        }))
    } else {
        None
    };

    Json(json!({
        "items": items,
        "next_page_params": next_page_params
    }))
}

async fn token_counters(
    State(state): State<Arc<ApiState>>,
    Path(hash): Path<String>,
) -> impl IntoResponse {
    let (holders_count, transfers_count) = if let Ok(addr) = hash.parse::<Address>() {
        let holders = state.index.count_token_holders(&addr, MAX_COUNT_CAP).await.unwrap_or(0);
        let transfers = state.index.count_token_transfers(&addr, MAX_COUNT_CAP).await.unwrap_or(0);
        (holders, transfers)
    } else {
        (0, 0)
    };
    
    Json(json!({
        "token_holders_count": holders_count.to_string(),
        "transfers_count": transfers_count.to_string()
    }))
}

async fn tokens_list() -> impl IntoResponse {
    Json(json!({
        "items": [],
        "next_page_params": null
    }))
}

async fn homepage_blocks(State(state): State<Arc<ApiState>>) -> impl IntoResponse {
    if let Some(ref rpc_url) = state.rpc_url {
        if let Ok(block) = fetch_block_rpc(rpc_url, "latest").await {
            if let Some(num_hex) = block["number"].as_str() {
                let last_block = hex_to_u64(num_hex);
                let mut items = Vec::new();
                
                for i in 0..6 {
                    if let Some(height) = last_block.checked_sub(i) {
                        if let Ok(blk) = fetch_block_rpc(rpc_url, &format!("0x{:x}", height)).await {
                            items.push(rpc_block_to_homepage_json(&blk, height));
                        }
                    }
                }
                
                return Json(items);
            }
        }
    }

    let last_block = state.index.last_indexed_block().await.unwrap_or(None).unwrap_or(0);
    let items: Vec<Value> = (0..6)
        .filter_map(|i| {
            let height = last_block.checked_sub(i)?;
            Some(stub_block(height))
        })
        .collect();
    
    Json(items)
}

async fn homepage_txs(State(state): State<Arc<ApiState>>) -> impl IntoResponse {
    if let Some(ref rpc_url) = state.rpc_url {
        if let Ok(block) = fetch_block_rpc(rpc_url, "latest").await {
            if let Some(txs) = block["transactions"].as_array() {
                let block_num = block["number"]
                    .as_str()
                    .and_then(|s| u64::from_str_radix(s.trim_start_matches("0x"), 16).ok())
                    .unwrap_or(0);
                let timestamp = block["timestamp"]
                    .as_str()
                    .and_then(|s| u64::from_str_radix(s.trim_start_matches("0x"), 16).ok())
                    .unwrap_or(0);
                let base_fee = block["baseFeePerGas"]
                    .as_str()
                    .map(|s| hex_to_u128(s));
                let ts = chrono::DateTime::from_timestamp(timestamp as i64, 0)
                    .map(|dt| dt.to_rfc3339())
                    .unwrap_or_default();

                let block_param = format!("0x{:x}", block_num);
                let receipts = fetch_block_receipts_rpc(rpc_url, &block_param)
                    .await
                    .ok()
                    .and_then(|r| r.as_array().cloned());

                let items: Vec<Value> = txs
                    .iter()
                    .rev()
                    .take(6)
                    .enumerate()
                    .map(|(_, tx)| {
                        let tx_hash = tx["hash"].as_str().unwrap_or("");
                        let receipt = receipts.as_ref().and_then(|rs| {
                            rs.iter().find(|r| r["transactionHash"].as_str() == Some(tx_hash))
                        });
                        rpc_tx_to_json(tx, block_num, &ts, base_fee, receipt)
                    })
                    .collect();

                return Json(items);
            }
        }
    }
    Json(Vec::<Value>::new())
}

async fn search(
    State(state): State<Arc<ApiState>>,
    Query(params): Query<SearchParams>,
) -> impl IntoResponse {
    let query = params.q.trim();
    if query.is_empty() {
        return Json(json!({ "items": [], "next_page_params": null }));
    }

    // Check if it's a tx hash (66 chars with 0x prefix)
    if query.len() == 66 && query.starts_with("0x") {
        if let Some(ref rpc_url) = state.rpc_url {
            if let Ok(tx) = fetch_tx_rpc(rpc_url, query).await {
                if !tx.is_null() {
                    return Json(json!({
                        "items": [search_result_tx(query)],
                        "next_page_params": null
                    }));
                }
            }
        }
    }

    // Check if it's a block number
    if let Ok(block_num) = query.parse::<u64>() {
        if let Some(ref rpc_url) = state.rpc_url {
            if let Ok(block) = fetch_block_rpc(rpc_url, &format!("0x{:x}", block_num)).await {
                if !block.is_null() {
                    return Json(json!({
                        "items": [search_result_block(block_num)],
                        "next_page_params": null
                    }));
                }
            }
        }
    }

    // Check if it's an address
    if query.len() == 42 && query.starts_with("0x") {
        if let Ok(_addr) = query.parse::<Address>() {
            return Json(json!({
                "items": [search_result_address(query)],
                "next_page_params": null
            }));
        }
    }

    // Search Meilisearch for tokens/addresses
    if let Some(ref search) = state.search {
        if let Ok(results) = search.search(query, 10).await {
            let items: Vec<Value> = results
                .into_iter()
                .map(|r| {
                    if r.r#type == "token" {
                        json!({
                            "address_hash": r.address,
                            "address_url": format!("/address/{}", r.address),
                            "certified": false,
                            "circulating_market_cap": null,
                            "exchange_rate": null,
                            "icon_url": null,
                            "is_smart_contract_verified": false,
                            "is_verified_via_admin_panel": false,
                            "name": r.name,
                            "priority": 2,
                            "reputation": "ok",
                            "symbol": r.symbol,
                            "token_type": "ERC-20",
                            "token_url": format!("/token/{}", r.address),
                            "total_supply": null,
                            "type": "token"
                        })
                    } else {
                        json!({
                            "address_hash": r.address,
                            "certified": false,
                            "ens_info": r.ens_name.map(|e| json!({"name": e})),
                            "is_smart_contract_verified": r.is_contract,
                            "name": r.name,
                            "priority": 0,
                            "reputation": "ok",
                            "type": "address",
                            "url": format!("/address/{}", r.address)
                        })
                    }
                })
                .collect();

            return Json(json!({
                "items": items,
                "next_page_params": null
            }));
        }
    }

    Json(json!({ "items": [], "next_page_params": null }))
}

async fn search_quick(
    State(state): State<Arc<ApiState>>,
    Query(params): Query<SearchParams>,
) -> impl IntoResponse {
    let query = params.q.trim();
    if query.is_empty() {
        return Json(json!([]));
    }

    // Check if it's a tx hash
    if query.len() == 66 && query.starts_with("0x") {
        if let Some(ref rpc_url) = state.rpc_url {
            if let Ok(tx) = fetch_tx_rpc(rpc_url, query).await {
                if !tx.is_null() {
                    return Json(json!([search_result_tx(query)]));
                }
            }
        }
    }

    // Check if it's a block number
    if let Ok(block_num) = query.parse::<u64>() {
        return Json(json!([search_result_block(block_num)]));
    }

    // Check if it's an address
    if query.len() == 42 && query.starts_with("0x") {
        if let Ok(_addr) = query.parse::<Address>() {
            return Json(json!([search_result_address(query)]));
        }
    }

    // Search Meilisearch
    if let Some(ref search) = state.search {
        if let Ok(results) = search.search_quick(query, 5).await {
            let items: Vec<Value> = results
                .into_iter()
                .map(|r| {
                    if r.r#type == "token" {
                        json!({
                            "address_hash": r.address,
                            "address_url": format!("/address/{}", r.address),
                            "certified": false,
                            "circulating_market_cap": null,
                            "exchange_rate": null,
                            "icon_url": null,
                            "is_smart_contract_verified": false,
                            "is_verified_via_admin_panel": false,
                            "name": r.name,
                            "priority": 2,
                            "reputation": "ok",
                            "symbol": r.symbol,
                            "token_type": "ERC-20",
                            "token_url": format!("/token/{}", r.address),
                            "total_supply": null,
                            "type": "token"
                        })
                    } else {
                        json!({
                            "address_hash": r.address,
                            "certified": false,
                            "ens_info": r.ens_name.map(|e| json!({"name": e})),
                            "is_smart_contract_verified": r.is_contract,
                            "name": r.name,
                            "priority": 0,
                            "reputation": "ok",
                            "type": "address",
                            "url": format!("/address/{}", r.address)
                        })
                    }
                })
                .collect();

            return Json(json!(items));
        }
    }

    Json(json!([]))
}

fn search_result_tx(hash: &str) -> Value {
    json!({
        "tx_hash": hash,
        "type": "transaction",
        "url": format!("/tx/{}", hash)
    })
}

fn search_result_block(number: u64) -> Value {
    json!({
        "block_number": number,
        "type": "block",
        "url": format!("/block/{}", number)
    })
}

fn search_result_address(address: &str) -> Value {
    json!({
        "address_hash": address,
        "certified": false,
        "ens_info": null,
        "is_smart_contract_verified": false,
        "name": null,
        "priority": 0,
        "reputation": "ok",
        "type": "address",
        "url": format!("/address/{}", address)
    })
}

#[derive(Debug, Deserialize)]
pub struct SearchParams {
    #[serde(default)]
    pub q: String,
}

// ============ RPC HELPERS ============

async fn fetch_block_rpc(rpc_url: &str, block_param: &str) -> Result<Value, reqwest::Error> {
    let client = reqwest::Client::new();
    let resp = client
        .post(rpc_url)
        .json(&json!({
            "jsonrpc": "2.0",
            "method": "eth_getBlockByNumber",
            "params": [block_param, true],
            "id": 1
        }))
        .send()
        .await?
        .json::<Value>()
        .await?;
    Ok(resp["result"].clone())
}

async fn fetch_tx_rpc(rpc_url: &str, tx_hash: &str) -> Result<Value, reqwest::Error> {
    let client = reqwest::Client::new();
    let resp = client
        .post(rpc_url)
        .json(&json!({
            "jsonrpc": "2.0",
            "method": "eth_getTransactionByHash",
            "params": [tx_hash],
            "id": 1
        }))
        .send()
        .await?
        .json::<Value>()
        .await?;
    Ok(resp["result"].clone())
}

async fn fetch_tx_receipt_rpc(rpc_url: &str, tx_hash: &str) -> Result<Value, reqwest::Error> {
    let client = reqwest::Client::new();
    let resp = client
        .post(rpc_url)
        .json(&json!({
            "jsonrpc": "2.0",
            "method": "eth_getTransactionReceipt",
            "params": [tx_hash],
            "id": 1
        }))
        .send()
        .await?
        .json::<Value>()
        .await?;
    Ok(resp["result"].clone())
}

async fn fetch_block_receipts_rpc(rpc_url: &str, block_param: &str) -> Result<Value, reqwest::Error> {
    let client = reqwest::Client::new();
    let resp = client
        .post(rpc_url)
        .json(&json!({
            "jsonrpc": "2.0",
            "method": "eth_getBlockReceipts",
            "params": [block_param],
            "id": 1
        }))
        .send()
        .await?
        .json::<Value>()
        .await?;
    Ok(resp["result"].clone())
}

fn hex_to_u128(hex: &str) -> u128 {
    u128::from_str_radix(hex.trim_start_matches("0x"), 16).unwrap_or(0)
}

fn hex_to_u64(hex: &str) -> u64 {
    u64::from_str_radix(hex.trim_start_matches("0x"), 16).unwrap_or(0)
}

fn hex_to_u256(hex: &str) -> String {
    let hex = hex.trim_start_matches("0x");
    if hex.is_empty() || hex == "0" {
        return "0".to_string();
    }
    
    // Parse as U256 and return decimal string
    AlloyU256::from_str_radix(hex, 16)
        .map(|n| n.to_string())
        .unwrap_or_else(|_| "0".to_string())
}

async fn fetch_token_metadata(rpc_url: &str, token_addr: &str) -> (Option<String>, Option<String>, Option<String>) {
    let client = reqwest::Client::new();
    
    // ERC20 function selectors
    let name_selector = "0x06fdde03"; // name()
    let symbol_selector = "0x95d89b41"; // symbol()
    let decimals_selector = "0x313ce567"; // decimals()
    
    let mut name = None;
    let mut symbol = None;
    let mut decimals = None;
    
    // Fetch name
    if let Ok(resp) = client
        .post(rpc_url)
        .json(&json!({
            "jsonrpc": "2.0",
            "method": "eth_call",
            "params": [{"to": token_addr, "data": name_selector}, "latest"],
            "id": 1
        }))
        .send()
        .await
    {
        if let Ok(body) = resp.json::<Value>().await {
            if let Some(result) = body["result"].as_str() {
                name = decode_string_result(result);
            }
        }
    }
    
    // Fetch symbol
    if let Ok(resp) = client
        .post(rpc_url)
        .json(&json!({
            "jsonrpc": "2.0",
            "method": "eth_call",
            "params": [{"to": token_addr, "data": symbol_selector}, "latest"],
            "id": 1
        }))
        .send()
        .await
    {
        if let Ok(body) = resp.json::<Value>().await {
            if let Some(result) = body["result"].as_str() {
                symbol = decode_string_result(result);
            }
        }
    }
    
    // Fetch decimals
    if let Ok(resp) = client
        .post(rpc_url)
        .json(&json!({
            "jsonrpc": "2.0",
            "method": "eth_call",
            "params": [{"to": token_addr, "data": decimals_selector}, "latest"],
            "id": 1
        }))
        .send()
        .await
    {
        if let Ok(body) = resp.json::<Value>().await {
            if let Some(result) = body["result"].as_str() {
                let dec = hex_to_u64(result);
                if dec <= 77 { // sanity check for decimals
                    decimals = Some(dec.to_string());
                }
            }
        }
    }
    
    (name, symbol, decimals)
}

fn decode_string_result(hex: &str) -> Option<String> {
    let hex = hex.trim_start_matches("0x");
    if hex.len() < 128 {
        // Try direct bytes32 decoding (some tokens return this way)
        let bytes = hex::decode(hex).ok()?;
        let s: String = bytes.iter()
            .take_while(|&&b| b != 0)
            .map(|&b| b as char)
            .collect();
        if !s.is_empty() && s.chars().all(|c| c.is_ascii_graphic() || c == ' ') {
            return Some(s);
        }
        return None;
    }
    
    // Standard ABI string decoding
    // First 32 bytes: offset (usually 0x20)
    // Next 32 bytes: length
    // Rest: string data
    let bytes = hex::decode(hex).ok()?;
    if bytes.len() < 64 {
        return None;
    }
    
    // Get length from bytes 32-64
    let len_bytes = &bytes[32..64];
    let len = u64::from_be_bytes(len_bytes[24..32].try_into().ok()?) as usize;
    
    if bytes.len() < 64 + len {
        return None;
    }
    
    let string_bytes = &bytes[64..64 + len];
    String::from_utf8(string_bytes.to_vec()).ok()
}

fn rpc_block_to_json(block: &Value, height: u64) -> Value {
    let hash = block["hash"].as_str().unwrap_or("0x0");
    let timestamp = block["timestamp"].as_str().unwrap_or("0x0");
    let timestamp_dec = hex_to_u64(timestamp);
    let miner = block["miner"].as_str().unwrap_or("0x0");
    let gas_used = block["gasUsed"].as_str().unwrap_or("0x0");
    let gas_limit = block["gasLimit"].as_str().unwrap_or("0x0");
    let tx_count = block["transactions"].as_array().map(|a| a.len()).unwrap_or(0);
    let base_fee = block["baseFeePerGas"].as_str();
    let parent_hash = block["parentHash"].as_str().unwrap_or("0x0");

    let ts = chrono::DateTime::from_timestamp(timestamp_dec as i64, 0)
        .map(|dt| dt.to_rfc3339())
        .unwrap_or_default();

    json!({
        "height": height,
        "timestamp": ts,
        "tx_count": tx_count,
        "miner": stub_address_param(miner),
        "size": 0,
        "hash": hash,
        "parent_hash": parent_hash,
        "difficulty": "0",
        "total_difficulty": null,
        "gas_used": hex_to_u64(gas_used).to_string(),
        "gas_limit": hex_to_u64(gas_limit).to_string(),
        "nonce": "0x0000000000000000",
        "base_fee_per_gas": base_fee.map(|s| hex_to_u128(s).to_string()),
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

fn rpc_block_to_homepage_json(block: &Value, height: u64) -> Value {
    let hash = block["hash"].as_str().unwrap_or("0x0");
    let timestamp = block["timestamp"].as_str().unwrap_or("0x0");
    let timestamp_dec = hex_to_u64(timestamp);
    let miner = block["miner"].as_str().unwrap_or("0x0");
    let gas_used = block["gasUsed"].as_str().unwrap_or("0x0");
    let gas_limit = block["gasLimit"].as_str().unwrap_or("0x0");
    let tx_count = block["transactions"].as_array().map(|a| a.len()).unwrap_or(0);
    let base_fee = block["baseFeePerGas"].as_str();
    let parent_hash = block["parentHash"].as_str().unwrap_or("0x0");
    let blob_gas_used = block["blobGasUsed"].as_str();
    let excess_blob_gas = block["excessBlobGas"].as_str();

    let blob_tx_count = block["transactions"]
        .as_array()
        .map(|txs| txs.iter().filter(|tx| tx["type"].as_str() == Some("0x3")).count())
        .unwrap_or(0);

    let ts = chrono::DateTime::from_timestamp(timestamp_dec as i64, 0)
        .map(|dt| dt.to_rfc3339())
        .unwrap_or_default();

    let gas_used_val = hex_to_u64(gas_used);
    let gas_limit_val = hex_to_u64(gas_limit);
    let gas_used_percentage = if gas_limit_val > 0 {
        Some((gas_used_val as f64 / gas_limit_val as f64) * 100.0)
    } else {
        None
    };

    json!({
        "base_fee_per_gas": base_fee.map(|s| hex_to_u128(s).to_string()),
        "beacon_deposits_count": null,
        "blob_gas_used": blob_gas_used.map(|s| hex_to_u64(s).to_string()),
        "blob_transactions_count": blob_tx_count,
        "burnt_fees": null,
        "burnt_fees_percentage": null,
        "difficulty": "0",
        "excess_blob_gas": excess_blob_gas.map(|s| hex_to_u64(s).to_string()),
        "gas_limit": gas_limit_val.to_string(),
        "gas_target_percentage": null,
        "gas_used": gas_used_val.to_string(),
        "gas_used_percentage": gas_used_percentage,
        "hash": hash,
        "height": height,
        "internal_transactions_count": null,
        "is_pending_update": false,
        "miner": stub_address_param(miner),
        "nonce": "0x0000000000000000",
        "parent_hash": parent_hash,
        "priority_fee": null,
        "rewards": [],
        "size": 0,
        "timestamp": ts,
        "total_difficulty": null,
        "transaction_fees": null,
        "transactions_count": tx_count,
        "type": "block",
        "uncles_hashes": [],
        "withdrawals_count": null
    })
}

fn rpc_tx_to_json(
    tx: &Value,
    block_num: u64,
    timestamp: &str,
    base_fee: Option<u128>,
    receipt: Option<&Value>,
) -> Value {
    let hash = tx["hash"].as_str().unwrap_or("0x0");
    let from = tx["from"].as_str().unwrap_or("0x0");
    let to = tx["to"].as_str();
    let value = tx["value"].as_str().unwrap_or("0x0");
    let gas = tx["gas"].as_str().unwrap_or("0x0");
    let gas_price = tx["gasPrice"].as_str().unwrap_or("0x0");
    let nonce = tx["nonce"].as_str().unwrap_or("0x0");
    let tx_index = tx["transactionIndex"].as_str().unwrap_or("0x0");
    let input = tx["input"].as_str().unwrap_or("0x");
    let tx_type = tx["type"].as_str().unwrap_or("0x0");
    let max_fee = tx["maxFeePerGas"].as_str();
    let max_priority_fee = tx["maxPriorityFeePerGas"].as_str();

    let gas_used = receipt
        .and_then(|r| r["gasUsed"].as_str())
        .map(|s| hex_to_u64(s).to_string());
    let status = receipt
        .and_then(|r| r["status"].as_str())
        .map(|s| if s == "0x1" { "ok" } else { "error" })
        .unwrap_or("ok");
    let result = if status == "ok" { "success" } else { "reverted" };

    let gas_price_val = hex_to_u128(gas_price);
    let gas_used_val = gas_used.as_ref().and_then(|s| s.parse::<u128>().ok()).unwrap_or(0);
    let fee_value = gas_price_val * gas_used_val;

    json!({
        "hash": hash,
        "to": to.map(|a| stub_address_param(a)),
        "created_contract": if to.is_none() { Some(json!(null)) } else { None },
        "result": result,
        "confirmations": 1,
        "status": status,
        "block_number": block_num,
        "timestamp": timestamp,
        "confirmation_duration": [0, 12000],
        "from": stub_address_param(from),
        "value": hex_to_u128(value).to_string(),
        "fee": { "type": "actual", "value": fee_value.to_string() },
        "gas_price": gas_price_val.to_string(),
        "type": hex_to_u64(tx_type) as u8,
        "gas_used": gas_used,
        "gas_limit": hex_to_u64(gas).to_string(),
        "max_fee_per_gas": max_fee.map(|s| hex_to_u128(s).to_string()),
        "max_priority_fee_per_gas": max_priority_fee.map(|s| hex_to_u128(s).to_string()),
        "priority_fee": null,
        "base_fee_per_gas": base_fee.map(|f| f.to_string()),
        "transaction_burnt_fee": null,
        "nonce": hex_to_u64(nonce),
        "position": hex_to_u64(tx_index),
        "revert_reason": null,
        "raw_input": input,
        "decoded_input": null,
        "token_transfers": null,
        "token_transfers_overflow": null,
        "exchange_rate": null,
        "method": if input.len() >= 10 { Some(&input[..10]) } else { None::<&str> },
        "transaction_types": if input == "0x" { vec!["coin_transfer"] } else { vec!["contract_call"] },
        "transaction_tag": null,
        "actions": [],
        "has_error_in_internal_transactions": false
    })
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
    use alloy_consensus::transaction::SignerRecoverable;
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
        "implementations": [],
        "is_contract": false,
        "is_verified": false,
        "is_scam": false,
        "name": null,
        "ens_domain_name": null,
        "private_tags": [],
        "public_tags": [],
        "watchlist_names": []
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
