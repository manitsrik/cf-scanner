import asyncio
import json
import logging
from datetime import datetime, timezone

import httpx
import pandas as pd
import websockets

from app.config import Settings
from app.indicators import add_indicators
from app.models import Signal
from app.store import SignalStore
from app.telegram import TelegramAlerter

logger = logging.getLogger(__name__)


class FuturesScanner:
    def __init__(self, settings: Settings, store: SignalStore, alerter: TelegramAlerter) -> None:
        self.settings = settings
        self.store = store
        self.alerter = alerter
        self._candles: dict[tuple[str, str], pd.DataFrame] = {}
        self._stop_event = asyncio.Event()

    async def start(self) -> None:
        await self._load_initial_candles()
        await self._run_websocket_loop()

    async def stop(self) -> None:
        self._stop_event.set()

    async def _load_initial_candles(self) -> None:
        async with httpx.AsyncClient(base_url=self.settings.binance_rest_url, timeout=15) as client:
            tasks = [
                self._fetch_klines(client, symbol, timeframe)
                for symbol in self.settings.symbols
                for timeframe in self.settings.timeframes
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                logger.error("Initial candle load failed: %s", result)
                continue
            symbol, timeframe, candles = result
            self._candles[(symbol, timeframe)] = candles
            self._detect_signal(symbol, timeframe)

    async def _fetch_klines(
        self, client: httpx.AsyncClient, symbol: str, timeframe: str
    ) -> tuple[str, str, pd.DataFrame]:
        response = await client.get(
            "/fapi/v1/klines",
            params={"symbol": symbol, "interval": timeframe, "limit": self.settings.kline_limit},
        )
        response.raise_for_status()
        return symbol, timeframe, self._klines_to_dataframe(response.json())

    async def _run_websocket_loop(self) -> None:
        backoff_seconds = 1
        while not self._stop_event.is_set():
            try:
                streams = "/".join(
                    f"{symbol.lower()}@kline_{timeframe}"
                    for symbol in self.settings.symbols
                    for timeframe in self.settings.timeframes
                )
                ws_url = f"{self.settings.binance_ws_url}?streams={streams}"
                logger.info("Connecting to Binance websocket.")
                async with websockets.connect(ws_url, ping_interval=20, ping_timeout=20) as websocket:
                    backoff_seconds = 1
                    async for message in websocket:
                        if self._stop_event.is_set():
                            break
                        await self._handle_ws_message(message)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Websocket connection failed. Reconnecting in %s seconds.", backoff_seconds)
                await asyncio.sleep(backoff_seconds)
                backoff_seconds = min(backoff_seconds * 2, 60)

    async def _handle_ws_message(self, message: str) -> None:
        try:
            payload = json.loads(message)
            kline = payload["data"]["k"]
            if not kline["x"]:
                return

            symbol = kline["s"]
            timeframe = kline["i"]
            row = {
                "open_time": pd.to_datetime(kline["t"], unit="ms", utc=True),
                "open": float(kline["o"]),
                "high": float(kline["h"]),
                "low": float(kline["l"]),
                "close": float(kline["c"]),
                "volume": float(kline["v"]),
                "close_time": pd.to_datetime(kline["T"], unit="ms", utc=True),
            }
            self._upsert_candle(symbol, timeframe, row)
            signal = self._detect_signal(symbol, timeframe)
            if signal and self.store.add_if_new(signal):
                logger.info("New %s signal for %s %s.", signal.signal_type, symbol, timeframe)
                await self.alerter.send_signal(signal)
        except (KeyError, ValueError, TypeError, json.JSONDecodeError):
            logger.exception("Failed to parse websocket message.")

    def _upsert_candle(self, symbol: str, timeframe: str, row: dict) -> None:
        key = (symbol, timeframe)
        current = self._candles.get(key, pd.DataFrame())
        incoming = pd.DataFrame([row])
        if current.empty or "open_time" not in current.columns:
            updated = incoming
        else:
            updated = pd.concat([current[current["open_time"] != row["open_time"]], incoming], ignore_index=True)
        updated = updated.sort_values("open_time").tail(self.settings.kline_limit).reset_index(drop=True)
        self._candles[key] = updated

    def _detect_signal(self, symbol: str, timeframe: str) -> Signal | None:
        candles = self._candles.get((symbol, timeframe))
        if candles is None or len(candles) < 200:
            return None

        df = add_indicators(candles)
        latest = df.iloc[-1]
        previous = df.iloc[-2]
        required = ["ema_9", "ema_21", "ema_200", "rsi_14", "volume_avg_20"]
        if latest[required].isna().any() or previous[["ema_9", "ema_21"]].isna().any():
            return None

        price = float(latest["close"])
        rsi = float(latest["rsi_14"])
        volume = float(latest["volume"])
        volume_average = float(latest["volume_avg_20"])
        has_volume = volume > volume_average

        long_signal = (
            price > float(latest["ema_200"])
            and float(previous["ema_9"]) <= float(previous["ema_21"])
            and float(latest["ema_9"]) > float(latest["ema_21"])
            and rsi > 50
            and has_volume
        )
        short_signal = (
            price < float(latest["ema_200"])
            and float(previous["ema_9"]) >= float(previous["ema_21"])
            and float(latest["ema_9"]) < float(latest["ema_21"])
            and rsi < 50
            and has_volume
        )

        signal_type = "LONG" if long_signal else "SHORT" if short_signal else None
        if not signal_type:
            return None

        close_time = latest["close_time"].to_pydatetime()
        signal_id = f"{symbol}:{timeframe}:{int(close_time.timestamp())}:{signal_type}"
        return Signal(
            id=signal_id,
            symbol=symbol,
            timeframe=timeframe,
            signal_type=signal_type,
            price=price,
            rsi=rsi,
            volume=volume,
            volume_average_20=volume_average,
            volume_status="above average",
            created_at=datetime.now(timezone.utc),
            tradingview_url=f"https://www.tradingview.com/chart/?symbol=BINANCE:{symbol}.P",
        )

    @staticmethod
    def _klines_to_dataframe(klines: list[list]) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "open_time": pd.to_datetime(item[0], unit="ms", utc=True),
                    "open": float(item[1]),
                    "high": float(item[2]),
                    "low": float(item[3]),
                    "close": float(item[4]),
                    "volume": float(item[5]),
                    "close_time": pd.to_datetime(item[6], unit="ms", utc=True),
                }
                for item in klines
            ]
        )
