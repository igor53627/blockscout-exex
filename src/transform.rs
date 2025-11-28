use alloy_primitives::{Address, Log, U256};
use alloy_sol_types::{sol, SolEvent};

// ERC20 Transfer event
sol! {
    event Transfer(address indexed from, address indexed to, uint256 value);
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
    let decoded = Transfer::decode_log(log, true).ok()?;

    Some((log.address, decoded.from, decoded.to, decoded.value))
}

/// Decode ERC721 Transfer (same signature as ERC20, but tokenId in data vs indexed)
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
