from functools import lru_cache
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = "CF Scanner"
    symbols: list[str] = Field(default_factory=lambda: ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT"])
    timeframes: list[str] = Field(default_factory=lambda: ["15m", "30m"])
    kline_limit: int = 250
    binance_rest_url: str = "https://fapi.binance.com"
    binance_ws_url: str = "wss://fstream.binance.com/stream"
    telegram_bot_token: str | None = None
    telegram_chat_id: str | None = None
    signal_limit: int = 100


@lru_cache
def get_settings() -> Settings:
    return Settings()

