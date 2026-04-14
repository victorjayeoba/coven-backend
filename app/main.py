import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.db import mongo
from app.jobs import position_monitor, tx_poller, ws_listener
from app.routers import (
    auth,
    backtests,
    clusters,
    health,
    settings as settings_router,
    signals,
    tokens,
    trades,
    wallets,
)
from app.services import (
    contagion_detector,
    execution_engine,
    exit_monitor,
    signal_enricher,
)


@asynccontextmanager
async def lifespan(_: FastAPI):
    # Startup
    mongo.connect()
    index_stats = await contagion_detector.load_graph_index()
    print(f"[startup] graph index loaded: {index_stats}")
    contagion_detector.register()
    signal_enricher.register()
    execution_engine.register()
    exit_monitor.register()

    ws_task = asyncio.create_task(ws_listener.run(), name="ws_listener")
    pos_task = asyncio.create_task(position_monitor.run(), name="position_monitor")
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
        position_monitor.stop()
        if tx_task is not None:
            tx_poller.stop()
        await asyncio.gather(
            ws_task,
            pos_task,
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
app.include_router(auth.router)
app.include_router(signals.router)
app.include_router(wallets.router)
app.include_router(clusters.router)
app.include_router(tokens.router)
app.include_router(trades.router)
app.include_router(backtests.router)
app.include_router(settings_router.router)


@app.get("/")
async def root():
    return {"name": "Coven", "docs": "/docs", "health": "/health"}
