from functools import lru_cache
from hashlib import sha256
import os
from pathlib import Path
import secrets
from typing import Any

from pydantic import Field
from pydantic_settings import BaseSettings, DotEnvSettingsSource, EnvSettingsSource, SettingsConfigDict


class _CommaSeparatedListMixin:
    comma_separated_fields = {"symbols", "timeframes", "telegram_timeframes"}

    def prepare_field_value(self, field_name: str, field: Any, value: Any, value_is_complex: bool) -> Any:
        if field_name in self.comma_separated_fields and isinstance(value, str):
            raw_value = value.strip()
            if raw_value and not raw_value.startswith("["):
                return [item.strip() for item in raw_value.split(",") if item.strip()]

        return super().prepare_field_value(field_name, field, value, value_is_complex)


class CommaSeparatedEnvSettingsSource(_CommaSeparatedListMixin, EnvSettingsSource):
    pass


class CommaSeparatedDotEnvSettingsSource(_CommaSeparatedListMixin, DotEnvSettingsSource):
    pass


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = "CF Scanner"
    symbols: list[str] = Field(default_factory=lambda: ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT"])
    timeframes: list[str] = Field(default_factory=lambda: ["5m", "15m", "30m", "1h"])
    auto_watchlist_enabled: bool = True
    auto_watchlist_size: int = 20
    watchlist_refresh_seconds: int = 3600
    rest_refresh_seconds: int = 3600
    rest_concurrency: int = 1
    rest_backoff_seconds: int = 7200
    news_refresh_seconds: int = 600
    news_item_limit: int = 12
    overview_timeframe: str = "15m"
    kline_limit: int = 320
    binance_rest_url: str = "https://fapi.binance.com"
    binance_ws_url: str = "wss://fstream.binance.com/market/stream"
    dashboard_url: str = "https://cf-scanner-production.up.railway.app/"
    telegram_bot_token: str | None = None
    telegram_chat_id: str | None = None
    # Signals on every scanned timeframe are stored and shown on the dashboard, but
    # only these are pushed to Telegram (5m is left out because it is the noisiest).
    telegram_timeframes: list[str] = Field(default_factory=lambda: ["15m", "30m", "1h"])
    signal_db_path: str = "data/signals.db"
    railway_volume_mount_path: str | None = None
    signal_limit: int = 100
    signal_cooldown_minutes: int = 120
    system_alert_cooldown_minutes: int = 30
    near_cross_threshold_pct: float = 0.15
    near_volume_ratio_min: float = 0.8
    dashboard_password: str | None = None
    session_secret: str | None = None
    session_cookie_name: str = "cf_scanner_session"
    session_cookie_secure: bool = True

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings,
        env_settings,
        dotenv_settings,
        file_secret_settings,
    ):
        return (
            init_settings,
            CommaSeparatedEnvSettingsSource(settings_cls),
            CommaSeparatedDotEnvSettingsSource(
                settings_cls,
                env_file=settings_cls.model_config.get("env_file"),
                env_file_encoding=settings_cls.model_config.get("env_file_encoding"),
            ),
            file_secret_settings,
        )

    def model_post_init(self, __context: object) -> None:
        if self.railway_volume_mount_path and "SIGNAL_DB_PATH" not in os.environ:
            self.signal_db_path = str(Path(self.railway_volume_mount_path) / "signals.db")

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
