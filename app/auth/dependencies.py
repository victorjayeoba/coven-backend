from bson import ObjectId
from fastapi import HTTPException, Request, status

from app.auth.jwt_handler import decode_token
from app.config import settings
from app.db import mongo


def _unauthorized() -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Not authenticated",
    )


async def get_current_user(request: Request) -> dict:
    """Read JWT from cookie, load user from Mongo, return user dict (without hash)."""
    token = request.cookies.get(settings.cookie_name)
    if not token:
        raise _unauthorized()

    payload = decode_token(token)
    if not payload or not payload.get("sub"):
        raise _unauthorized()

    user_id = payload["sub"]
    db = mongo.db()
    try:
        user = await db.users.find_one({"_id": ObjectId(user_id)})
    except Exception:
        raise _unauthorized()
    if not user:
        raise _unauthorized()

    user["id"] = str(user.pop("_id"))
    user.pop("password_hash", None)
    return user
