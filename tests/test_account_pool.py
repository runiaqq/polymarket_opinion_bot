import asyncio
from typing import List

import pytest

from core.models import AccountCredentials, ExchangeName
from utils.account_pool import AccountPool
from utils.token_bucket import AsyncTokenBucket


class DummySession:
    pass


async def fake_create_session(account: AccountCredentials):
    return DummySession()


async def fake_health_check(account: AccountCredentials, session) -> bool:
    return not account.metadata.get("bad")


@pytest.mark.asyncio
async def test_account_pool_health_and_assignment():
    accounts: List[AccountCredentials] = []
    for idx in range(5):
        accounts.append(
            AccountCredentials(
                account_id=f"acc-{idx}",
                exchange=ExchangeName.OPINION,
                api_key="k",
                secret_key="s",
                weight=1.0,
                tokens_per_sec=10,
                burst=5,
            )
        )
    accounts[1].metadata["bad"] = "true"

    pool = AccountPool(accounts, fake_create_session, fake_health_check)
    await pool._run_health_checks()
    worker = await pool.acquire_worker("round_robin")
    assert worker
    assert worker.credentials.account_id != "acc-1"


@pytest.mark.asyncio
async def test_token_bucket_limits():
    bucket = AsyncTokenBucket(tokens_per_second=5, burst=5)
    assert await bucket.try_acquire(5)
    assert not await bucket.try_acquire(1)
    await asyncio.sleep(0.3)
    assert await bucket.try_acquire(1)







