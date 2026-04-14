from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

from app.config import settings

_client: AsyncIOMotorClient | None = None


def connect() -> None:
    global _client
    _client = AsyncIOMotorClient(settings.mongo_uri)


async def disconnect() -> None:
    if _client is not None:
        _client.close()


def db() -> AsyncIOMotorDatabase:
    if _client is None:
        raise RuntimeError("Mongo client not initialized. Call connect() first.")
    return _client[settings.mongo_db]


async def ping() -> bool:
    if _client is None:
        return False
    try:
        await _client.admin.command("ping")
        return True
    except Exception:
        return False
