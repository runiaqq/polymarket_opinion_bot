import pytest

from core.order_fsm import OrderFSMEvent, OrderFSMState, OrderStateMachine
from core.models import OrderStatus


class DummyDB:
    def __init__(self):
        self.statuses = []

    async def update_order_status(self, order_id, status: OrderStatus):
        self.statuses.append((order_id, status))


@pytest.mark.asyncio
async def test_fsm_transitions_and_persistence():
    db = DummyDB()
    fsm = OrderStateMachine("ord-1", db)

    await fsm.transition(OrderFSMEvent.PLACE)
    assert fsm.current_state == OrderFSMState.PLACED
    assert db.statuses[-1][1] == OrderStatus.OPEN

    await fsm.transition(OrderFSMEvent.FILL_PARTIAL)
    assert fsm.current_state == OrderFSMState.PARTIALLY_FILLED
    assert db.statuses[-1][1] == OrderStatus.PARTIALLY_FILLED

    await fsm.transition(OrderFSMEvent.FILL_FULL)
    assert fsm.current_state == OrderFSMState.FILLED
    assert db.statuses[-1][1] == OrderStatus.FILLED


@pytest.mark.asyncio
async def test_fsm_idempotent_events():
    db = DummyDB()
    fsm = OrderStateMachine("ord-2", db)
    await fsm.transition(OrderFSMEvent.PLACE, event_id="evt-1")
    assert len(db.statuses) == 1
    await fsm.transition(OrderFSMEvent.PLACE, event_id="evt-1")
    assert len(db.statuses) == 1  # duplicate ignored


@pytest.mark.asyncio
async def test_fsm_callbacks_invoked():
    db = DummyDB()
    fsm = OrderStateMachine("ord-3", db)
    called = []

    async def on_filled(state, payload):
        called.append((state, payload))

    fsm.on_enter(OrderFSMState.FILLED, on_filled)
    await fsm.transition(OrderFSMEvent.PLACE)
    await fsm.transition(OrderFSMEvent.FILL_FULL, payload={"price": 1.0})

    assert called and called[0][0] == OrderFSMState.FILLED

