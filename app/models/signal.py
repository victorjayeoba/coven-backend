from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


SignalStatus = Literal["watch", "partial", "exec"]
SignalType = Literal["cluster", "alpha"]


class ContagionSignal(BaseModel):
    """A detected cluster movement on a token."""
    signal_type: SignalType = "cluster"

    token_id: str
    chain: str
    symbol: str | None = None

    # Cluster context (always present; for alpha signals cluster_id may be None)
    cluster_id: int | None = None
    wallets_involved: list[str]
    cluster_size_total: int = 0
    cluster_active_count: int = 0

    # Alpha context
    wallet_alpha_scores: list[float] = Field(default_factory=list)
    avg_alpha_score: float = 0.0

    # Timing
    time_window_seconds: int = 0
    first_entry_at: datetime
    last_entry_at: datetime

    # Filled by enricher (Phase 4)
    conviction_score: float = 0.0
    momentum_score: float | None = None
    status: SignalStatus = "watch"

    detected_at: datetime = Field(default_factory=datetime.utcnow)
