from datetime import datetime

from pydantic import BaseModel, Field


class Cluster(BaseModel):
    """A group of wallets that consistently appear together across tokens."""
    cluster_id: int
    chain: str
    wallet_addresses: list[str]
    shared_tokens: list[str]
    avg_shared_count: float
    size: int
    created_at: datetime = Field(default_factory=datetime.utcnow)
    last_updated: datetime = Field(default_factory=datetime.utcnow)
