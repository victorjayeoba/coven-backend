from datetime import datetime

from pydantic import BaseModel, Field


class TokenTracked(BaseModel):
    """A token we've pulled top100 data for."""
    token_id: str
    chain: str
    symbol: str | None = None
    seed_topic: str | None = None
    last_updated: datetime = Field(default_factory=datetime.utcnow)
