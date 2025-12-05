//! Meilisearch integration for full-text search across chains.
//!
//! Indexes:
//! - `tokens` - ERC20/721/1155 tokens with name, symbol, address
//! - `addresses` - Addresses with optional ENS names and labels

use eyre::Result;
use meilisearch_sdk::client::Client;
use meilisearch_sdk::indexes::Index;
use meilisearch_sdk::settings::Settings;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenDocument {
    pub id: String,
    pub chain: String,
    pub address: String,
    pub name: Option<String>,
    pub symbol: Option<String>,
    pub decimals: Option<u8>,
    pub token_type: String,
}

impl TokenDocument {
    pub fn new(
        chain: &str,
        address: &str,
        name: Option<String>,
        symbol: Option<String>,
        decimals: Option<u8>,
        token_type: &str,
    ) -> Self {
        Self {
            id: format!("{}_{}", chain, address.to_lowercase()),
            chain: chain.to_string(),
            address: address.to_lowercase(),
            name,
            symbol,
            decimals,
            token_type: token_type.to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddressDocument {
    pub id: String,
    pub chain: String,
    pub address: String,
    pub ens_name: Option<String>,
    pub label: Option<String>,
    pub is_contract: bool,
}

impl AddressDocument {
    pub fn new(chain: &str, address: &str, is_contract: bool) -> Self {
        Self {
            id: format!("{}_{}", chain, address.to_lowercase()),
            chain: chain.to_string(),
            address: address.to_lowercase(),
            ens_name: None,
            label: None,
            is_contract,
        }
    }

    pub fn with_ens(mut self, ens_name: String) -> Self {
        self.ens_name = Some(ens_name);
        self
    }

    pub fn with_label(mut self, label: String) -> Self {
        self.label = Some(label);
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResult {
    pub r#type: String,
    pub address: String,
    pub name: Option<String>,
    pub symbol: Option<String>,
    pub ens_name: Option<String>,
    pub is_contract: bool,
}

pub struct SearchClient {
    client: Client,
    chain: String,
}

impl SearchClient {
    pub fn new(url: &str, api_key: Option<&str>, chain: &str) -> Self {
        let client = Client::new(url, api_key).expect("Failed to create Meilisearch client");
        Self {
            client,
            chain: chain.to_string(),
        }
    }

    pub async fn ensure_indexes(&self) -> Result<()> {
        let tokens_settings = Settings::new()
            .with_searchable_attributes(["name", "symbol", "address"])
            .with_filterable_attributes(["chain", "token_type"])
            .with_sortable_attributes(["name"]);

        let addresses_settings = Settings::new()
            .with_searchable_attributes(["address", "ens_name", "label"])
            .with_filterable_attributes(["chain", "is_contract"])
            .with_sortable_attributes(["address"]);

        self.client
            .index("tokens")
            .set_settings(&tokens_settings)
            .await?;

        self.client
            .index("addresses")
            .set_settings(&addresses_settings)
            .await?;

        Ok(())
    }

    fn tokens_index(&self) -> Index {
        self.client.index("tokens")
    }

    fn addresses_index(&self) -> Index {
        self.client.index("addresses")
    }

    pub async fn index_token(&self, token: TokenDocument) -> Result<()> {
        self.tokens_index()
            .add_documents(&[token], Some("id"))
            .await?;
        Ok(())
    }

    pub async fn index_tokens(&self, tokens: &[TokenDocument]) -> Result<()> {
        if tokens.is_empty() {
            return Ok(());
        }
        self.tokens_index()
            .add_documents(tokens, Some("id"))
            .await?;
        Ok(())
    }

    pub async fn index_address(&self, address: AddressDocument) -> Result<()> {
        self.addresses_index()
            .add_documents(&[address], Some("id"))
            .await?;
        Ok(())
    }

    pub async fn index_addresses(&self, addresses: &[AddressDocument]) -> Result<()> {
        if addresses.is_empty() {
            return Ok(());
        }
        self.addresses_index()
            .add_documents(addresses, Some("id"))
            .await?;
        Ok(())
    }

    pub async fn search(&self, query: &str, limit: usize) -> Result<Vec<SearchResult>> {
        let mut results = Vec::new();
        let chain_filter = format!("chain = \"{}\"", self.chain);

        // Search tokens
        let token_results = self
            .tokens_index()
            .search()
            .with_query(query)
            .with_filter(&chain_filter)
            .with_limit(limit)
            .execute::<TokenDocument>()
            .await?;

        for hit in token_results.hits {
            results.push(SearchResult {
                r#type: "token".to_string(),
                address: hit.result.address,
                name: hit.result.name,
                symbol: hit.result.symbol,
                ens_name: None,
                is_contract: true,
            });
        }

        // Search addresses
        let address_results = self
            .addresses_index()
            .search()
            .with_query(query)
            .with_filter(&chain_filter)
            .with_limit(limit)
            .execute::<AddressDocument>()
            .await?;

        for hit in address_results.hits {
            results.push(SearchResult {
                r#type: if hit.result.is_contract {
                    "contract".to_string()
                } else {
                    "address".to_string()
                },
                address: hit.result.address,
                name: hit.result.label,
                symbol: None,
                ens_name: hit.result.ens_name,
                is_contract: hit.result.is_contract,
            });
        }

        Ok(results)
    }

    pub async fn search_quick(&self, query: &str, limit: usize) -> Result<Vec<SearchResult>> {
        self.search(query, limit).await
    }
}
