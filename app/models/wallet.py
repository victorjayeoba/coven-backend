from datetime import datetime

from pydantic import BaseModel, Field


class TokenAppearance(BaseModel):
    """A single appearance of a wallet in a token's top100."""
    token_id: str
    symbol: str | None = None
    chain: str | None = None
    balance_ratio: float | None = None
    realized_profit: float | None = None
    unrealized_profit: float | None = None


class WalletNode(BaseModel):
    """Represents a wallet in the graph."""
    address: str
    chain: str
    alpha_score: float = 0.0
    tokens: list[TokenAppearance] = Field(default_factory=list)
    first_seen: datetime = Field(default_factory=datetime.utcnow)
    last_updated: datetime = Field(default_factory=datetime.utcnow)


class WalletEdge(BaseModel):
    """A connection between two wallets based on shared token history."""
    wallet_a: str
    wallet_b: str
    shared_tokens: list[str]
    shared_count: int
    chain: str
