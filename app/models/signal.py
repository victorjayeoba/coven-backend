from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


SignalStatus = Literal["watch", "partial", "exec"]


class ContagionSignal(BaseModel):
    """A detected cluster movement on a token."""
    token_id: str
    chain: str
    symbol: str | None = None
    cluster_id: int | None = None
    wallets_involved: list[str]
    cluster_size_total: int
    cluster_active_count: int       # how many cluster wallets entered in the window
    time_window_seconds: int
    first_entry_at: datetime
    last_entry_at: datetime
    conviction_score: float = 0.0   # filled by Phase 4
    status: SignalStatus = "watch"  # filled by Phase 4
    detected_at: datetime = Field(default_factory=datetime.utcnow)
