import pytest

from core.models import AccountCredentials, ExchangeName


@pytest.fixture
def make_accounts():
    def _build(count: int = 10, tokens_per_sec: float = 5.0, burst: int = 5):
        accounts = []
        for idx in range(count):
            exchange = ExchangeName.OPINION if idx % 2 == 0 else ExchangeName.POLYMARKET
            accounts.append(
                AccountCredentials(
                    account_id=f"acct-{idx}",
                    exchange=exchange,
                    api_key=f"key-{idx}",
                    secret_key=f"sec-{idx}",
                    weight=1.0,
                    tokens_per_sec=tokens_per_sec,
                    burst=burst,
                )
            )
        return accounts

    return _build

