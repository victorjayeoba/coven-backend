"""
Minimal in-process async pub/sub.

Producers publish events, consumers subscribe to event types.
No external dependencies, no network — just asyncio and a dict of lists.
"""

import asyncio
from collections import defaultdict
from typing import Any, Awaitable, Callable

Handler = Callable[[dict], Awaitable[None]]


class EventBus:
    def __init__(self) -> None:
        self._handlers: dict[str, list[Handler]] = defaultdict(list)

    def subscribe(self, event_type: str, handler: Handler) -> None:
        self._handlers[event_type].append(handler)

    def unsubscribe(self, event_type: str, handler: Handler) -> None:
        try:
            self._handlers.get(event_type, []).remove(handler)
        except ValueError:
            pass

    async def publish(self, event_type: str, payload: dict) -> None:
        handlers = list(self._handlers.get(event_type, ()))
        if not handlers:
            return
        # Fan out in parallel; swallow handler errors so one bad consumer
        # can't kill the producer.
        results = await asyncio.gather(
            *[h(payload) for h in handlers],
            return_exceptions=True,
        )
        for r in results:
            if isinstance(r, Exception):
                print(f"[event_bus] handler error on '{event_type}': {r}")


# Singleton for the app
bus = EventBus()


# Event type constants
SWAP_EVENT = "swap"
SIGNAL_FIRED = "signal.fired"
SIGNAL_SCORED = "signal.scored"
TRADE_OPENED = "trade.opened"
TRADE_CLOSED = "trade.closed"
PRICE_UPDATE = "price.update"
BOT_TRADE_OPENED = "bot.trade.opened"
BOT_TRADE_CLOSED = "bot.trade.closed"
BOT_UPDATED = "bot.updated"
