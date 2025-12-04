from __future__ import annotations

from enum import Enum
from typing import Awaitable, Callable, Dict, List, Optional

from core.models import OrderStatus
from utils.logger import BotLogger


class OrderFSMState(Enum):
    NEW = "NEW"
    PLACED = "PLACED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELLING = "CANCELLING"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"


class OrderFSMEvent(Enum):
    PLACE = "PLACE"
    ACK = "ACK"
    FILL_PARTIAL = "FILL_PARTIAL"
    FILL_FULL = "FILL_FULL"
    CANCEL_REQUEST = "CANCEL_REQUEST"
    CANCEL_ACK = "CANCEL_ACK"
    ERROR = "ERROR"


Callback = Callable[[OrderFSMState, Optional[object]], Awaitable[None]]


class OrderStateMachine:
    """Deterministic FSM for order lifecycle."""

    _TRANSITIONS: Dict[OrderFSMState, Dict[OrderFSMEvent, OrderFSMState]] = {
        OrderFSMState.NEW: {
            OrderFSMEvent.PLACE: OrderFSMState.PLACED,
            OrderFSMEvent.ACK: OrderFSMState.PLACED,
            OrderFSMEvent.ERROR: OrderFSMState.FAILED,
        },
        OrderFSMState.PLACED: {
            OrderFSMEvent.FILL_PARTIAL: OrderFSMState.PARTIALLY_FILLED,
            OrderFSMEvent.FILL_FULL: OrderFSMState.FILLED,
            OrderFSMEvent.CANCEL_REQUEST: OrderFSMState.CANCELLING,
            OrderFSMEvent.ERROR: OrderFSMState.FAILED,
        },
        OrderFSMState.PARTIALLY_FILLED: {
            OrderFSMEvent.FILL_PARTIAL: OrderFSMState.PARTIALLY_FILLED,
            OrderFSMEvent.FILL_FULL: OrderFSMState.FILLED,
            OrderFSMEvent.CANCEL_REQUEST: OrderFSMState.CANCELLING,
            OrderFSMEvent.ERROR: OrderFSMState.FAILED,
        },
        OrderFSMState.CANCELLING: {
            OrderFSMEvent.CANCEL_ACK: OrderFSMState.CANCELLED,
            OrderFSMEvent.FILL_FULL: OrderFSMState.FILLED,
            OrderFSMEvent.ERROR: OrderFSMState.FAILED,
        },
        OrderFSMState.CANCELLED: {},
        OrderFSMState.FAILED: {},
        OrderFSMState.FILLED: {},
    }

    _STATUS_MAP: Dict[OrderFSMState, OrderStatus] = {
        OrderFSMState.NEW: OrderStatus.PENDING,
        OrderFSMState.PLACED: OrderStatus.OPEN,
        OrderFSMState.PARTIALLY_FILLED: OrderStatus.PARTIALLY_FILLED,
        OrderFSMState.FILLED: OrderStatus.FILLED,
        OrderFSMState.CANCELLING: OrderStatus.CANCELED,
        OrderFSMState.CANCELLED: OrderStatus.CANCELED,
        OrderFSMState.FAILED: OrderStatus.REJECTED,
    }

    def __init__(
        self,
        order_id: str,
        database,
        initial_state: OrderFSMState = OrderFSMState.NEW,
        logger: BotLogger | None = None,
    ):
        self.order_id = order_id
        self.db = database
        self.current_state = initial_state
        self.logger = logger or BotLogger(__name__)
        self._callbacks: Dict[OrderFSMState, List[Callback]] = {}
        self._last_event_id: Optional[str] = None

    def on_enter(self, state: OrderFSMState, callback: Callback) -> None:
        self._callbacks.setdefault(state, []).append(callback)

    async def transition(
        self,
        event: OrderFSMEvent,
        payload: Optional[object] = None,
        event_id: Optional[str] = None,
    ) -> OrderFSMState:
        if event_id and event_id == self._last_event_id:
            return self.current_state

        next_state = self._next_state(event)
        if next_state == self.current_state:
            return self.current_state

        self.current_state = next_state
        self._last_event_id = event_id
        await self._persist_state()
        await self._run_callbacks(next_state, payload)
        return self.current_state

    def _next_state(self, event: OrderFSMEvent) -> OrderFSMState:
        transitions = self._TRANSITIONS.get(self.current_state, {})
        next_state = transitions.get(event, self.current_state)
        if next_state == self.current_state:
            self.logger.debug(
                "fsm noop transition",
                order_id=self.order_id,
                state=self.current_state.value,
                event=event.value,
            )
        return next_state

    async def _persist_state(self) -> None:
        status = self._STATUS_MAP.get(self.current_state)
        if not status:
            return
        await self.db.update_order_status(self.order_id, status)

    async def _run_callbacks(self, state: OrderFSMState, payload: Optional[object]) -> None:
        callbacks = self._callbacks.get(state, [])
        for cb in callbacks:
            await cb(state, payload)

