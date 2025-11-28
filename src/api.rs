use std::sync::Arc;

use alloy_primitives::Address;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use tower_http::cors::CorsLayer;

use crate::index_db::IndexDb;

pub struct ApiState {
    pub index_db: IndexDb,
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
        .route("/api/v2/addresses/:hash/transactions", get(get_address_txs))
        .route(
            "/api/v2/addresses/:hash/token-transfers",
            get(get_address_transfers),
        )
        .route("/api/v2/tokens/:hash/holders", get(get_token_holders))
        .route("/api/v2/main-page/indexing-status", get(indexing_status))
        .route("/health", get(health))
        .layer(CorsLayer::permissive())
        .with_state(state)
}

async fn health() -> impl IntoResponse {
    Json(serde_json::json!({ "status": "ok" }))
}

async fn indexing_status(State(state): State<Arc<ApiState>>) -> impl IntoResponse {
    let last_block = state.index_db.last_indexed_block().unwrap_or(None);
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
        .index_db
        .get_address_txs(&address, params.limit + 1, params.offset)
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
        .index_db
        .get_address_transfers(&address, params.limit + 1, params.offset)
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
        .index_db
        .get_token_holders(&token, params.limit + 1, params.offset)
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
