from datetime import datetime
from typing import Literal

from pydantic import BaseModel, EmailStr, Field


class UserPreferences(BaseModel):
    mode: Literal["paper", "live"] = "paper"
    conviction_threshold: int = 70
    max_position_usd: float = 200.0
    chains: list[str] = Field(default_factory=lambda: ["solana", "bsc"])
    auto_exit: bool = True


class UserCreate(BaseModel):
    email: EmailStr
    password: str = Field(min_length=8, max_length=128)


class UserLogin(BaseModel):
    email: EmailStr
    password: str


class UserOut(BaseModel):
    id: str
    email: EmailStr
    created_at: datetime
    preferences: UserPreferences


class UserInDB(BaseModel):
    id: str
    email: EmailStr
    password_hash: str
    created_at: datetime
    preferences: UserPreferences = Field(default_factory=UserPreferences)
