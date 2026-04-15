import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.db import mongo
from app.jobs import (
    bot_position_monitor,
    helius_stream,
    position_monitor,
    price_listener,
    rank_poller,
    telegram_poller,
    tx_poller,
    wallet_poller,
    wallet_watcher,
    ws_listener,
)
from app.routers import (
    auth,
    backtests,
    balance,
    bots,
    clusters,
    health,
    settings as settings_router,
    signals,
    stream,
    telegram,
    tokens,
    trades,
    wallets,
)
from app.services import (
    bot_runner,
    contagion_detector,
    execution_engine,
    exit_monitor,
    signal_dedupe,
    signal_enricher,
    telegram_dispatcher,
)


@asynccontextmanager
async def lifespan(_: FastAPI):
    # Startup
    mongo.connect()
    index_stats = await contagion_detector.load_graph_index()
    print(f"[startup] graph index loaded: {index_stats}")
    try:
        dedupe_stats = await signal_dedupe.run()
        if dedupe_stats.get("deleted"):
            print(f"[startup] signal dedupe: {dedupe_stats}")
    except Exception as e:
        print(f"[startup] signal dedupe failed: {e}")
    contagion_detector.register()
    signal_enricher.register()
    execution_engine.register()
    exit_monitor.register()
    bot_runner.register()
    telegram_dispatcher.register()

    ws_task = asyncio.create_task(ws_listener.run(), name="ws_listener")
    price_task = asyncio.create_task(price_listener.run(), name="price_listener")
    wallet_task = asyncio.create_task(wallet_watcher.run(), name="wallet_watcher")
    poller_task = asyncio.create_task(wallet_poller.run(), name="wallet_poller")
    helius_task = asyncio.create_task(helius_stream.run(), name="helius_stream")
    pos_task = asyncio.create_task(position_monitor.run(), name="position_monitor")
    bot_pos_task = asyncio.create_task(
        bot_position_monitor.run(), name="bot_position_monitor"
    )
    tg_task = asyncio.create_task(telegram_poller.run(), name="telegram_poller")
    rank_task = asyncio.create_task(rank_poller.run(), name="rank_poller")
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
        price_listener.stop()
        wallet_watcher.stop()
        wallet_poller.stop()
        helius_stream.stop()
        position_monitor.stop()
        bot_position_monitor.stop()
        telegram_poller.stop()
        rank_poller.stop()
        if tx_task is not None:
            tx_poller.stop()
        await asyncio.gather(
            ws_task,
            price_task,
            wallet_task,
            poller_task,
            helius_task,
            pos_task,
            bot_pos_task,
            tg_task,
            rank_task,
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
app.include_router(stream.router)
app.include_router(settings_router.router)
app.include_router(bots.router)
app.include_router(telegram.router)
app.include_router(balance.router)


@app.get("/")
async def root():
    return {"name": "Coven", "docs": "/docs", "health": "/health"}
