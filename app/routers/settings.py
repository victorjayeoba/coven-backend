from datetime import datetime

from bson import ObjectId
from fastapi import APIRouter, Depends
from pydantic import BaseModel

from app.auth.dependencies import get_current_user
from app.db import mongo
from app.models.user import UserPreferences

router = APIRouter(prefix="/api/settings", tags=["settings"])


class PreferencesUpdate(BaseModel):
    mode: str | None = None
    conviction_threshold: int | None = None
    max_position_usd: float | None = None
    chains: list[str] | None = None
    auto_exit: bool | None = None


@router.get("")
async def get_settings(user: dict = Depends(get_current_user)) -> dict:
    return {
        "preferences": user.get("preferences") or UserPreferences().model_dump(),
    }


@router.patch("")
async def update_settings(
    payload: PreferencesUpdate,
    user: dict = Depends(get_current_user),
):
    db = mongo.db()
    current = user.get("preferences") or UserPreferences().model_dump()
    updates = {k: v for k, v in payload.model_dump(exclude_none=True).items()}
    current.update(updates)
    # Validate via pydantic
    validated = UserPreferences(**current).model_dump()

    await db.users.update_one(
        {"_id": ObjectId(user["id"])},
        {"$set": {"preferences": validated, "updated_at": datetime.utcnow()}},
    )
    return {"preferences": validated}
