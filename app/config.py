from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    ave_api_key: str
    ave_base_url: str = "https://prod.ave-api.com/v2"
    ave_wss_url: str = "wss://wss.ave-api.xyz"

    # When WSS is active, the REST poller is redundant and causes rate limits.
    # Set ENABLE_TX_POLLER=true in .env to force it on (e.g. if WSS is down).
    enable_tx_poller: bool = False

    # Auth / JWT
    jwt_secret: str = "dev-change-me-please-super-secret-key"
    jwt_algorithm: str = "HS256"
    jwt_expire_minutes: int = 60 * 24  # 24h

    cookie_name: str = "coven_session"
    cookie_secure: bool = False
    cookie_samesite: str = "lax"

    mongo_uri: str = "mongodb://localhost:27017"
    mongo_db: str = "contagion"

    cors_origins: str = "http://localhost:3000"

    # Helius (Solana wallet-level tx indexing — used by wallet_poller
    # for copy-trading coverage that AVE's multi_tx doesn't give us).
    helius_api_key: str | None = None
    helius_base_url: str = "https://api.helius.xyz"

    # Etherscan V2 (BSCScan consolidated here in 2024 — one key, 60+ chains).
    # For BSC we pass chainid=56.
    bscscan_api_key: str | None = None
    bscscan_base_url: str = "https://api.etherscan.io/v2/api"
    bscscan_chain_id: int = 56

    # How often to poll each watched wallet (seconds). Lower = faster copy, more CU.
    wallet_poll_interval_seconds: int = 10

    # When Helius WSS is running, the REST poller is redundant. Keep it off by
    # default and only enable as fallback if WSS is unavailable. BSC uses the
    # poller regardless (no WSS equivalent wired up yet).
    enable_wallet_poller: bool = False

    # Telegram bot — DM @BotFather on Telegram, run /newbot, paste token here.
    # telegram_bot_username is the handle without the '@' (e.g. "CovenAlphaBot"),
    # used to build the deep link t.me/<username>?start=<code>
    telegram_bot_token: str | None = None
    telegram_bot_username: str | None = None

    @property
    def cors_origin_list(self) -> list[str]:
        return [o.strip() for o in self.cors_origins.split(",") if o.strip()]


settings = Settings()
