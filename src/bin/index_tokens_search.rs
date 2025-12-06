//! Index tokens to Meilisearch from MDBX
//!
//! Reads all tokens from TOKEN_HOLDER_COUNTS and indexes them to Meilisearch
//! with their holder counts. Optionally fetches metadata via RPC.

use std::path::PathBuf;
use clap::Parser;
use eyre::Result;
use tracing::{info, warn};
use alloy_primitives::Address;

#[derive(Parser)]
#[command(name = "index-tokens-search")]
#[command(about = "Index tokens from MDBX to Meilisearch")]
struct Args {
    /// MDBX database path
    #[arg(long)]
    mdbx_path: PathBuf,

    /// Meilisearch URL
    #[arg(long, default_value = "http://localhost:7700")]
    meili_url: String,

    /// Meilisearch API key
    #[arg(long)]
    meili_key: Option<String>,

    /// Chain name (e.g., sepolia)
    #[arg(long, default_value = "sepolia")]
    chain: String,

    /// RPC URL for fetching token metadata
    #[arg(long)]
    rpc_url: Option<String>,

    /// Batch size for indexing
    #[arg(long, default_value = "100")]
    batch_size: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    info!("Opening MDBX database at {:?}", args.mdbx_path);

    #[cfg(feature = "mdbx")]
    {
        use reth_libmdbx::{Environment, EnvironmentFlags, Geometry, Mode, PageSize};
        use blockscout_exex::meili::{SearchClient, TokenDocument};

        // Open MDBX in read-only mode
        let env = Environment::builder()
            .set_geometry(Geometry {
                size: Some(0..(2 * 1024 * 1024 * 1024 * 1024)), // 2TB max
                growth_step: Some(1024 * 1024 * 1024),
                shrink_threshold: None,
                page_size: Some(PageSize::Set(4096)),
            })
            .set_flags(EnvironmentFlags {
                mode: Mode::ReadOnly,
                no_rdahead: true,
                coalesce: true,
                ..Default::default()
            })
            .set_max_dbs(20)
            .open(&args.mdbx_path)?;

        // Initialize Meilisearch client
        let search = SearchClient::new(
            &args.meili_url,
            args.meili_key.as_deref(),
            &args.chain,
        );

        // Ensure indexes exist with correct settings
        search.ensure_indexes().await?;
        info!("Meilisearch indexes configured");

        // Read all tokens from TOKEN_HOLDER_COUNTS
        let txn = env.begin_ro_txn()?;
        let token_counts_db = txn.open_db(Some("TokenHolderCounts"))?;
        let mut cursor = txn.cursor(&token_counts_db)?;

        let mut tokens: Vec<(Address, u64)> = Vec::new();

        for result in cursor.iter::<Vec<u8>, Vec<u8>>() {
            let (key, value) = result?;
            if key.len() == 20 && value.len() == 8 {
                let addr = Address::from_slice(&key);
                let count = i64::from_le_bytes(value[..8].try_into().unwrap_or([0u8; 8])) as u64;
                tokens.push((addr, count));
            }
        }

        drop(cursor);
        drop(txn);

        info!("Found {} tokens in MDBX", tokens.len());

        // Process in batches
        let mut indexed = 0;
        for batch in tokens.chunks(args.batch_size) {
            let mut docs: Vec<TokenDocument> = Vec::new();

            for (addr, holder_count) in batch {
                let addr_str = format!("{:?}", addr);

                // Fetch metadata if RPC available
                let (name, symbol, decimals) = if let Some(ref rpc_url) = args.rpc_url {
                    fetch_token_metadata(rpc_url, &addr_str).await
                } else {
                    (None, None, None)
                };

                let doc = TokenDocument::new(
                    &args.chain,
                    &addr_str,
                    name,
                    symbol,
                    decimals,
                    "ERC-20", // Default, could detect from token type
                ).with_holder_count(*holder_count);

                docs.push(doc);
            }

            if let Err(e) = search.index_tokens(&docs).await {
                warn!("Failed to index batch: {}", e);
            } else {
                indexed += docs.len();
                if indexed % 1000 == 0 {
                    info!("Indexed {} tokens...", indexed);
                }
            }
        }

        info!("Successfully indexed {} tokens to Meilisearch", indexed);

        Ok(())
    }

    #[cfg(not(feature = "mdbx"))]
    {
        Err(eyre::eyre!("MDBX support not enabled. Compile with --features mdbx"))
    }
}

async fn fetch_token_metadata(
    rpc_url: &str,
    token_address: &str,
) -> (Option<String>, Option<String>, Option<u8>) {
    let client = reqwest::Client::new();

    // eth_call for name()
    let name = call_token_method(&client, rpc_url, token_address, "0x06fdde03").await
        .and_then(|data| decode_string(&data));

    // eth_call for symbol()
    let symbol = call_token_method(&client, rpc_url, token_address, "0x95d89b41").await
        .and_then(|data| decode_string(&data));

    // eth_call for decimals()
    let decimals = call_token_method(&client, rpc_url, token_address, "0x313ce567").await
        .and_then(|data| decode_u8(&data));

    (name, symbol, decimals)
}

async fn call_token_method(
    client: &reqwest::Client,
    rpc_url: &str,
    token_address: &str,
    data: &str,
) -> Option<String> {
    let payload = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "eth_call",
        "params": [{
            "to": token_address,
            "data": data
        }, "latest"],
        "id": 1
    });

    let resp = client.post(rpc_url)
        .json(&payload)
        .timeout(std::time::Duration::from_secs(5))
        .send()
        .await
        .ok()?;

    let json: serde_json::Value = resp.json().await.ok()?;
    json["result"].as_str().map(|s| s.to_string())
}

fn decode_string(hex: &str) -> Option<String> {
    let hex = hex.strip_prefix("0x").unwrap_or(hex);
    if hex.len() < 128 {
        return None;
    }

    // ABI string: offset (32 bytes) + length (32 bytes) + data
    let offset = usize::from_str_radix(&hex[0..64], 16).ok()?;
    let len_start = offset * 2;
    if hex.len() < len_start + 64 {
        return None;
    }

    let len = usize::from_str_radix(&hex[len_start..len_start + 64], 16).ok()?;
    let data_start = len_start + 64;
    let data_end = data_start + len * 2;

    if hex.len() < data_end {
        return None;
    }

    let bytes = hex::decode(&hex[data_start..data_end]).ok()?;
    String::from_utf8(bytes).ok()
}

fn decode_u8(hex: &str) -> Option<u8> {
    let hex = hex.strip_prefix("0x").unwrap_or(hex);
    if hex.len() < 64 {
        return None;
    }
    u8::from_str_radix(&hex[62..64], 16).ok()
}
