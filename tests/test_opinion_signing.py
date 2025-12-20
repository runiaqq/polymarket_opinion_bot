import pytest
from eth_account import Account

try:  # eth-account >=0.10 renamed helpers
    from eth_account.messages import encode_structured_data
except ImportError:  # pragma: no cover - fallback for newer eth-account
    from eth_account.messages import encode_typed_data as encode_structured_data


# Deterministic offline EIP-712 signing check (no network, no real keys)
TEST_PRIV = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
TEST_ADDR = "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266"


def _typed_message():
    return {
        "types": {
            "EIP712Domain": [
                {"name": "name", "type": "string"},
                {"name": "version", "type": "string"},
                {"name": "chainId", "type": "uint256"},
                {"name": "verifyingContract", "type": "address"},
            ],
            "Order": [
                {"name": "maker", "type": "address"},
                {"name": "tokenId", "type": "uint256"},
                {"name": "price", "type": "uint256"},
                {"name": "salt", "type": "uint256"},
            ],
        },
        "primaryType": "Order",
        "domain": {
            "name": "OpinionCLOB",
            "version": "1",
            "chainId": 56,
            "verifyingContract": "0x0000000000000000000000000000000000000000",
        },
        "message": {
            "maker": TEST_ADDR,
            "tokenId": 123456,
            "price": 550000000000000000,  # 0.55 * 1e18
            "salt": 42,
        },
    }


def _encode_message(msg):
    try:
        return encode_structured_data(msg)
    except Exception:
        return encode_structured_data(full_message=msg)


def test_opinion_eip712_sign_verify():
    msg = _typed_message()
    encoded = _encode_message(msg)
    signed = Account.sign_message(encoded, private_key=TEST_PRIV)
    recovered = Account.recover_message(encoded, signature=signed.signature)
    assert recovered.lower() == TEST_ADDR.lower()


