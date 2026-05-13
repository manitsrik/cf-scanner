from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


SignalType = Literal["LONG", "SHORT"]


class Signal(BaseModel):
    id: str
    symbol: str
    timeframe: str
    signal_type: SignalType
    price: float
    rsi: float
    volume: float
    volume_average_20: float
    volume_status: str
    reasons: list[str]
    indicators: dict[str, float]
    quality_score: float | None = None
    quality_label: str | None = None
    quality_reasons: list[str] = Field(default_factory=list)
    trade_plan: dict = Field(default_factory=dict)
    backtest: dict = Field(default_factory=dict)
    status: dict = Field(default_factory=dict)
    trader_summary: str | None = None
    news_context: dict | None = None
    created_at: datetime
    tradingview_url: str


class SymbolInfo(BaseModel):
    symbols: list[str]
    timeframes: list[str]
