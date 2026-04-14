from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


TradeStatus = Literal["open", "closed", "failed"]
ExitReason = Literal[
    "cluster_distribution",
    "risk_spike",
    "manual",
    "price_stop",
    "take_profit",
    "none",
]


class TradeEntry(BaseModel):
    price_usd: float
    size_usd: float
    amount_tokens: float | None = None
    timestamp: datetime


class TradeExit(BaseModel):
    price_usd: float
    amount_tokens: float | None = None
    timestamp: datetime
    reason: ExitReason = "none"


class Trade(BaseModel):
    # identity
    user_id: str = "demo"           # single-user demo for hackathon
    signal_id: str | None = None     # reference to source signal
    token_id: str
    chain: str
    symbol: str | None = None
    cluster_id: int | None = None

    # trigger context — snapshot of what fired the entry
    entry_wallets: list[str]         # cluster wallets that triggered entry
    conviction_score: int = 0
    risk_score: int = 0

    # execution
    entry: TradeEntry
    exit: TradeExit | None = None
    current_price_usd: float | None = None

    # result
    pnl_usd: float | None = None
    pnl_pct: float | None = None

    # state
    status: TradeStatus = "open"
    is_paper: bool = True
    opened_at: datetime = Field(default_factory=datetime.utcnow)
    closed_at: datetime | None = None
    last_updated: datetime = Field(default_factory=datetime.utcnow)
