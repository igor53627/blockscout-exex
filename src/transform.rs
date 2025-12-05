use alloy_primitives::{Address, Log, U256};
use alloy_sol_types::{sol, SolEvent};

// ERC20/ERC721 Transfer event (same signature, different topic count)
sol! {
    event Transfer(address indexed from, address indexed to, uint256 value);
}

// ERC1155 events
sol! {
    event TransferSingle(
        address indexed operator,
        address indexed from,
        address indexed to,
        uint256 id,
        uint256 value
    );
    event TransferBatch(
        address indexed operator,
        address indexed from,
        address indexed to,
        uint256[] ids,
        uint256[] values
    );
}

/// Token type for transfers
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenType {
    Erc20,
    Erc721,
    Erc1155,
}

impl TokenType {
    pub fn as_str(&self) -> &'static str {
        match self {
            TokenType::Erc20 => "ERC-20",
            TokenType::Erc721 => "ERC-721",
            TokenType::Erc1155 => "ERC-1155",
        }
    }
    
    pub fn as_u8(&self) -> u8 {
        match self {
            TokenType::Erc20 => 0,
            TokenType::Erc721 => 1,
            TokenType::Erc1155 => 2,
        }
    }
    
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(TokenType::Erc20),
            1 => Some(TokenType::Erc721),
            2 => Some(TokenType::Erc1155),
            _ => None,
        }
    }
}

/// Decoded token transfer with type info
#[derive(Debug, Clone)]
pub struct DecodedTransfer {
    pub token_address: Address,
    pub from: Address,
    pub to: Address,
    pub value: U256,        // For ERC-20/ERC-1155, this is amount. For ERC-721 always 1.
    pub token_id: Option<U256>, // For ERC-721/ERC-1155
    pub token_type: TokenType,
}

/// Decode ERC20 Transfer event from log
/// Returns (token_address, from, to, value)
pub fn decode_erc20_transfer(log: &Log) -> Option<(Address, Address, Address, U256)> {
    // Check topic count - ERC20 Transfer has 3 topics
    if log.topics().len() != 3 {
        return None;
    }

    // Check event signature
    if log.topics()[0] != Transfer::SIGNATURE_HASH {
        return None;
    }

    // Decode the event
    let decoded = Transfer::decode_log(log).ok()?;

    Some((log.address, decoded.from, decoded.to, decoded.value))
}

/// Decode ERC721 Transfer (same signature as ERC20, but tokenId as 3rd indexed topic)
pub fn decode_erc721_transfer(log: &Log) -> Option<(Address, Address, Address, U256)> {
    // ERC721 has tokenId as indexed (4 topics total including signature)
    if log.topics().len() != 4 {
        return None;
    }

    if log.topics()[0] != Transfer::SIGNATURE_HASH {
        return None;
    }

    // For ERC721, the third topic is the tokenId
    let from = Address::from_slice(&log.topics()[1].as_slice()[12..]);
    let to = Address::from_slice(&log.topics()[2].as_slice()[12..]);
    let token_id = U256::from_be_bytes(log.topics()[3].0);

    Some((log.address, from, to, token_id))
}

/// Decode ERC1155 TransferSingle event
/// Returns (token_address, from, to, token_id, value)
pub fn decode_erc1155_transfer_single(log: &Log) -> Option<(Address, Address, Address, U256, U256)> {
    // TransferSingle has 4 topics (signature + 3 indexed) + data (id, value)
    if log.topics().len() != 4 {
        return None;
    }

    if log.topics()[0] != TransferSingle::SIGNATURE_HASH {
        return None;
    }

    let decoded = TransferSingle::decode_log(log).ok()?;
    Some((log.address, decoded.from, decoded.to, decoded.id, decoded.value))
}

/// Decode ERC1155 TransferBatch event
/// Returns (token_address, from, to, vec of (token_id, value))
pub fn decode_erc1155_transfer_batch(log: &Log) -> Option<(Address, Address, Address, Vec<(U256, U256)>)> {
    // TransferBatch has 4 topics (signature + 3 indexed) + data (ids[], values[])
    if log.topics().len() != 4 {
        return None;
    }

    if log.topics()[0] != TransferBatch::SIGNATURE_HASH {
        return None;
    }

    let decoded = TransferBatch::decode_log(log).ok()?;
    let pairs: Vec<_> = decoded.ids.iter().copied()
        .zip(decoded.values.iter().copied())
        .collect();
    Some((log.address, decoded.from, decoded.to, pairs))
}

/// Decode any token transfer from a log, returning structured transfer info
pub fn decode_token_transfer(log: &Log) -> Vec<DecodedTransfer> {
    let mut transfers = Vec::new();
    
    // Try ERC-721 first (4 topics with Transfer signature)
    if let Some((token, from, to, token_id)) = decode_erc721_transfer(log) {
        transfers.push(DecodedTransfer {
            token_address: token,
            from,
            to,
            value: U256::from(1),
            token_id: Some(token_id),
            token_type: TokenType::Erc721,
        });
        return transfers;
    }
    
    // Try ERC-20 (3 topics with Transfer signature)
    if let Some((token, from, to, value)) = decode_erc20_transfer(log) {
        transfers.push(DecodedTransfer {
            token_address: token,
            from,
            to,
            value,
            token_id: None,
            token_type: TokenType::Erc20,
        });
        return transfers;
    }
    
    // Try ERC-1155 TransferSingle
    if let Some((token, from, to, id, value)) = decode_erc1155_transfer_single(log) {
        transfers.push(DecodedTransfer {
            token_address: token,
            from,
            to,
            value,
            token_id: Some(id),
            token_type: TokenType::Erc1155,
        });
        return transfers;
    }
    
    // Try ERC-1155 TransferBatch
    if let Some((token, from, to, pairs)) = decode_erc1155_transfer_batch(log) {
        for (id, value) in pairs {
            transfers.push(DecodedTransfer {
                token_address: token,
                from,
                to,
                value,
                token_id: Some(id),
                token_type: TokenType::Erc1155,
            });
        }
        return transfers;
    }
    
    transfers
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::b256;

    #[test]
    fn test_transfer_signature() {
        // keccak256("Transfer(address,address,uint256)")
        let expected = b256!("ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef");
        assert_eq!(Transfer::SIGNATURE_HASH, expected);
    }
}
