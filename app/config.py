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

    @property
    def cors_origin_list(self) -> list[str]:
        return [o.strip() for o in self.cors_origins.split(",") if o.strip()]


settings = Settings()
