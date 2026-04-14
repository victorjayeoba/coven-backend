from datetime import datetime

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Response, status

from app.auth.dependencies import get_current_user
from app.auth.hashing import hash_password, verify_password
from app.auth.jwt_handler import create_token
from app.config import settings
from app.db import mongo
from app.models.user import UserCreate, UserLogin, UserOut, UserPreferences

router = APIRouter(prefix="/api/auth", tags=["auth"])


def _set_cookie(response: Response, token: str) -> None:
    response.set_cookie(
        key=settings.cookie_name,
        value=token,
        httponly=True,
        secure=settings.cookie_secure,
        samesite=settings.cookie_samesite,
        max_age=settings.jwt_expire_minutes * 60,
        path="/",
    )


def _serialize(user: dict) -> dict:
    return {
        "id": str(user.get("_id") or user.get("id")),
        "email": user["email"],
        "created_at": user["created_at"],
        "preferences": user.get("preferences") or UserPreferences().model_dump(),
    }


@router.post("/signup", response_model=UserOut, status_code=201)
async def signup(payload: UserCreate, response: Response):
    db = mongo.db()
    existing = await db.users.find_one({"email": payload.email.lower()})
    if existing:
        raise HTTPException(status_code=409, detail="Email already registered")

    doc = {
        "email": payload.email.lower(),
        "password_hash": hash_password(payload.password),
        "created_at": datetime.utcnow(),
        "preferences": UserPreferences().model_dump(),
    }
    result = await db.users.insert_one(doc)
    doc["_id"] = result.inserted_id

    token = create_token(subject=str(result.inserted_id))
    _set_cookie(response, token)
    return _serialize(doc)


@router.post("/signin", response_model=UserOut)
async def signin(payload: UserLogin, response: Response):
    db = mongo.db()
    user = await db.users.find_one({"email": payload.email.lower()})
    if not user or not verify_password(payload.password, user["password_hash"]):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    token = create_token(subject=str(user["_id"]))
    _set_cookie(response, token)
    return _serialize(user)


@router.post("/logout")
async def logout(response: Response):
    response.delete_cookie(settings.cookie_name, path="/")
    return {"ok": True}


@router.get("/me", response_model=UserOut)
async def me(user: dict = Depends(get_current_user)):
    return _serialize(user)
