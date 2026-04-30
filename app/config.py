from functools import lru_cache
import secrets

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = "CF Scanner"
    symbols: list[str] = Field(default_factory=lambda: ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT"])
    timeframes: list[str] = Field(default_factory=lambda: ["15m", "30m", "1h"])
    auto_watchlist_enabled: bool = True
    auto_watchlist_size: int = 20
    watchlist_refresh_seconds: int = 900
    kline_limit: int = 250
    binance_rest_url: str = "https://fapi.binance.com"
    binance_ws_url: str = "wss://fstream.binance.com/stream"
    telegram_bot_token: str | None = None
    telegram_chat_id: str | None = None
    signal_limit: int = 100
    signal_cooldown_minutes: int = 120
    near_cross_threshold_pct: float = 0.15
    near_volume_ratio_min: float = 0.8
    dashboard_password: str | None = None
    session_secret: str = Field(default_factory=lambda: secrets.token_urlsafe(32))
    session_cookie_name: str = "cf_scanner_session"
    session_cookie_secure: bool = True


@lru_cache
def get_settings() -> Settings:
    return Settings()
