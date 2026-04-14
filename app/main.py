import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.db import mongo
from app.jobs import tx_poller, ws_listener
from app.routers import health
from app.services import contagion_detector, signal_enricher


@asynccontextmanager
async def lifespan(_: FastAPI):
    # Startup
    mongo.connect()
    index_stats = await contagion_detector.load_graph_index()
    print(f"[startup] graph index loaded: {index_stats}")
    contagion_detector.register()
    signal_enricher.register()

    ws_task = asyncio.create_task(ws_listener.run(), name="ws_listener")
    tx_task = None
    if settings.enable_tx_poller:
        tx_task = asyncio.create_task(tx_poller.run(), name="tx_poller")
    else:
        print("[startup] tx_poller disabled (WSS is primary). "
              "Set ENABLE_TX_POLLER=true to enable REST fallback.")

    try:
        yield
    finally:
        # Shutdown
        ws_listener.stop()
        if tx_task is not None:
            tx_poller.stop()
        await asyncio.gather(
            ws_task,
            tx_task if tx_task else asyncio.sleep(0),
            return_exceptions=True,
        )
        await mongo.disconnect()


app = FastAPI(
    title="Coven API",
    description="See the circle move before the spell is cast. "
                "Behavioural cluster detection on on-chain smart money.",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origin_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health.router)


@app.get("/")
async def root():
    return {"name": "Coven", "docs": "/docs", "health": "/health"}
