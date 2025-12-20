import asyncio
import time
from collections import defaultdict

import pytest

from core.models import OrderSide
from core.scheduler import TaskScheduler
from utils.account_pool import AccountPool
from utils.mock_exchange import MockExchange

pytestmark = pytest.mark.stress


class MockReconciler:
    def __init__(self):
        self.processed = set()
        self.duplicates = 0

    async def submit(self, fill_id: str):
        if fill_id in self.processed:
            self.duplicates += 1
            return False
        self.processed.add(fill_id)
        return True


@pytest.mark.asyncio
async def test_stress_multi_account(make_accounts):
    account_count = 100
    accounts = make_accounts(count=account_count, tokens_per_sec=5.0, burst=5)
    exchanges = {acc.account_id: MockExchange(acc.exchange) for acc in accounts}

    async def create_session(account):
        return exchanges[account.account_id]

    async def health_check(account, session):
        return True

    pool = AccountPool(accounts, create_session, health_check, health_interval=0.1)
    scheduler = TaskScheduler(pool, policy="round_robin")

    reconciler = MockReconciler()
    usage_counts = defaultdict(int)
    latency_sums = defaultdict(float)
    latency_counts = defaultdict(int)
    rate_limited = 0
    rate_lock = asyncio.Lock()

    tasks_total = 40
    ops_per_task = 25

    async def worker_task(task_id: int):
        nonlocal rate_limited
        for op in range(ops_per_task):
            worker = await scheduler.assign()
            retries = 0
            while worker is None:
                retries += 1
                async with rate_lock:
                    rate_limited += 1
                await asyncio.sleep(0)
                worker = await scheduler.assign()
            account_id = worker.credentials.account_id
            usage_counts[account_id] += 1
            start = time.perf_counter()
            exchange = exchanges[account_id]
            side = OrderSide.BUY if (task_id + op) % 2 == 0 else OrderSide.SELL
            await exchange.place_limit_order(
                market_id="stress",
                side=side,
                price=0.5,
                size=1.0,
                client_order_id=f"{account_id}-{task_id}-{op}",
            )
            duration = time.perf_counter() - start
            latency_sums[account_id] += duration
            latency_counts[account_id] += 1
            fill_id = f"{account_id}-{task_id}-{op}"
            await reconciler.submit(fill_id)
            await reconciler.submit(fill_id)  # duplicate intentionally
            await scheduler.release(worker)

    workers = [asyncio.create_task(worker_task(idx)) for idx in range(tasks_total)]
    await asyncio.gather(*workers)

    total_ops = sum(latency_counts.values())
    assert total_ops == tasks_total * ops_per_task
    assert reconciler.duplicates == total_ops

    active_accounts = sum(1 for cnt in usage_counts.values() if cnt > 0)
    assert active_accounts > account_count // 2

    avg_latencies = {
        acc: (latency_sums[acc] / latency_counts[acc]) * 1000.0 for acc in latency_counts
    }
    summary = {
        "tasks_completed": total_ops,
        "rate_limited_events": rate_limited,
        "active_accounts": active_accounts,
        "avg_latency_ms": sum(avg_latencies.values()) / max(len(avg_latencies), 1),
    }
    print("stress summary:", summary)

