from datetime import datetime
from typing import Literal

from pydantic import BaseModel


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
    created_at: datetime
    tradingview_url: str


class SymbolInfo(BaseModel):
    symbols: list[str]
    timeframes: list[str]
