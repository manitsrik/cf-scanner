from functools import lru_cache
from hashlib import sha256
import secrets

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = "CF Scanner"
    symbols: list[str] = Field(default_factory=lambda: ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT"])
    timeframes: list[str] = Field(default_factory=lambda: ["15m", "30m", "1h"])
    auto_watchlist_enabled: bool = True
    auto_watchlist_size: int = 8
    watchlist_refresh_seconds: int = 3600
    rest_refresh_seconds: int = 3600
    rest_concurrency: int = 1
    rest_backoff_seconds: int = 7200
    kline_limit: int = 250
    binance_rest_url: str = "https://fapi.binance.com"
    binance_ws_url: str = "wss://fstream.binance.com/market/stream"
    telegram_bot_token: str | None = None
    telegram_chat_id: str | None = None
    signal_db_path: str = "data/signals.db"
    signal_limit: int = 100
    signal_cooldown_minutes: int = 120
    system_alert_cooldown_minutes: int = 30
    near_cross_threshold_pct: float = 0.15
    near_volume_ratio_min: float = 0.8
    dashboard_password: str | None = None
    session_secret: str | None = None
    session_cookie_name: str = "cf_scanner_session"
    session_cookie_secure: bool = True

    def model_post_init(self, __context: object) -> None:
        if self.session_secret:
            return
        if self.dashboard_password:
            seed = f"cf-scanner-session:{self.dashboard_password}"
            self.session_secret = sha256(seed.encode()).hexdigest()
            return
        self.session_secret = secrets.token_urlsafe(32)


@lru_cache
def get_settings() -> Settings:
    return Settings()
