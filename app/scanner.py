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
        self._active_symbols: list[str] = list(settings.symbols)
        self._candles: dict[tuple[str, str], pd.DataFrame] = {}
        self._stop_event = asyncio.Event()
        self._started_at: datetime | None = None
        self._initial_load_completed_at: datetime | None = None
        self._last_message_at: datetime | None = None
        self._last_rest_refresh_at: datetime | None = None
        self._watchlist_updated_at: datetime | None = None
        self._last_error: str | None = None
        self._websocket_connected = False
        self._watchlist_version = 0

    async def start(self) -> None:
        self._started_at = datetime.now(timezone.utc)
        await asyncio.gather(self._refresh_candles_loop(), self._run_websocket_loop())

    async def stop(self) -> None:
        self._stop_event.set()

    async def _refresh_candles_loop(self) -> None:
        while not self._stop_event.is_set():
            await self._refresh_watchlist()
            await self._load_initial_candles()
            await asyncio.sleep(60)

    async def _load_initial_candles(self) -> None:
        async with httpx.AsyncClient(base_url=self.settings.binance_rest_url, timeout=15) as client:
            tasks = [
                self._fetch_klines(client, symbol, timeframe)
                for symbol in self._active_symbols
                for timeframe in self.settings.timeframes
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                self._last_error = f"Initial candle load failed: {result}"
                logger.error("Initial candle load failed: %s", result)
                continue
            symbol, timeframe, candles = result
            self._candles[(symbol, timeframe)] = candles
            signal = self._detect_signal(symbol, timeframe)
            if signal and self.store.add_if_new(signal):
                logger.info("New %s signal for %s %s from REST refresh.", signal.signal_type, symbol, timeframe)
                await self.alerter.send_signal(signal)
        self._initial_load_completed_at = datetime.now(timezone.utc)
        self._last_rest_refresh_at = self._initial_load_completed_at

    async def _refresh_watchlist(self) -> None:
        if not self.settings.auto_watchlist_enabled:
            return
        if self._watchlist_updated_at:
            age = (datetime.now(timezone.utc) - self._watchlist_updated_at).total_seconds()
            if age < self.settings.watchlist_refresh_seconds:
                return

        try:
            async with httpx.AsyncClient(base_url=self.settings.binance_rest_url, timeout=15) as client:
                exchange_info_response, tickers_response = await asyncio.gather(
                    client.get("/fapi/v1/exchangeInfo"),
                    client.get("/fapi/v1/ticker/24hr"),
                )
                exchange_info_response.raise_for_status()
                tickers_response.raise_for_status()

            tradable = {
                item["symbol"]
                for item in exchange_info_response.json()["symbols"]
                if item.get("quoteAsset") == "USDT"
                and item.get("contractType") == "PERPETUAL"
                and item.get("status") == "TRADING"
            }
            tickers = [
                item
                for item in tickers_response.json()
                if item.get("symbol") in tradable and float(item.get("quoteVolume", 0) or 0) > 0
            ]
            tickers.sort(key=lambda item: float(item.get("quoteVolume", 0) or 0), reverse=True)
            symbols = [item["symbol"] for item in tickers[: self.settings.auto_watchlist_size]]
            if not symbols:
                return

            if symbols != self._active_symbols:
                old_pairs = set((symbol, timeframe) for symbol in symbols for timeframe in self.settings.timeframes)
                self._candles = {key: value for key, value in self._candles.items() if key in old_pairs}
                self._active_symbols = symbols
                self._watchlist_version += 1
                logger.info("Updated auto watchlist: %s", ", ".join(symbols))

            self._watchlist_updated_at = datetime.now(timezone.utc)
            self._last_error = None
        except Exception as exc:
            self._last_error = f"Auto watchlist refresh failed: {exc}"
            logger.exception("Auto watchlist refresh failed.")

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
                websocket_version = self._watchlist_version
                streams = "/".join(
                    f"{symbol.lower()}@kline_{timeframe}"
                    for symbol in self._active_symbols
                    for timeframe in self.settings.timeframes
                )
                ws_url = f"{self.settings.binance_ws_url}?streams={streams}"
                logger.info("Connecting to Binance websocket.")
                async with websockets.connect(ws_url, ping_interval=20, ping_timeout=20) as websocket:
                    self._websocket_connected = True
                    self._last_error = None
                    backoff_seconds = 1
                    while not self._stop_event.is_set():
                        if websocket_version != self._watchlist_version:
                            logger.info("Watchlist changed. Reconnecting websocket.")
                            break
                        message = await asyncio.wait_for(websocket.recv(), timeout=60)
                        if self._stop_event.is_set():
                            break
                        await self._handle_ws_message(message)
            except TimeoutError:
                logger.info("Websocket receive timeout. Reconnecting.")
            except asyncio.CancelledError:
                raise
            except Exception:
                self._websocket_connected = False
                self._last_error = f"Websocket connection failed. Reconnecting in {backoff_seconds} seconds."
                logger.exception("Websocket connection failed. Reconnecting in %s seconds.", backoff_seconds)
                await asyncio.sleep(backoff_seconds)
                backoff_seconds = min(backoff_seconds * 2, 60)
            finally:
                self._websocket_connected = False

    async def _handle_ws_message(self, message: str) -> None:
        try:
            self._last_message_at = datetime.now(timezone.utc)
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
            self._last_error = "Failed to parse websocket message."
            logger.exception("Failed to parse websocket message.")

    def status(self) -> dict:
        pairs = []
        for symbol in self._active_symbols:
            for timeframe in self.settings.timeframes:
                candles = self._candles.get((symbol, timeframe))
                latest = None
                count = 0
                price = None
                if candles is not None and not candles.empty:
                    count = len(candles)
                    latest_row = candles.iloc[-1]
                    latest = latest_row["close_time"].to_pydatetime().isoformat()
                    price = float(latest_row["close"])

                pairs.append(
                    {
                        "symbol": symbol,
                        "timeframe": timeframe,
                        "candle_count": count,
                        "last_closed_candle_at": latest,
                        "last_close_price": price,
                    }
                )

        return {
            "running": self._started_at is not None and not self._stop_event.is_set(),
            "websocket_connected": self._websocket_connected,
            "telegram_enabled": self.alerter.enabled,
            "auto_watchlist_enabled": self.settings.auto_watchlist_enabled,
            "watchlist_size": len(self._active_symbols),
            "active_symbols": self._active_symbols,
            "started_at": self._started_at.isoformat() if self._started_at else None,
            "initial_load_completed_at": self._initial_load_completed_at.isoformat()
            if self._initial_load_completed_at
            else None,
            "last_message_at": self._last_message_at.isoformat() if self._last_message_at else None,
            "last_rest_refresh_at": self._last_rest_refresh_at.isoformat() if self._last_rest_refresh_at else None,
            "watchlist_updated_at": self._watchlist_updated_at.isoformat() if self._watchlist_updated_at else None,
            "last_error": self._last_error,
            "pairs": pairs,
        }

    def symbols(self) -> list[str]:
        return list(self._active_symbols)

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
        ema_9 = float(latest["ema_9"])
        ema_21 = float(latest["ema_21"])
        ema_200 = float(latest["ema_200"])
        previous_ema_9 = float(previous["ema_9"])
        previous_ema_21 = float(previous["ema_21"])
        volume_ratio = volume / volume_average if volume_average > 0 else 0
        has_volume = volume > volume_average

        long_signal = (
            price > ema_200
            and previous_ema_9 <= previous_ema_21
            and ema_9 > ema_21
            and rsi > 50
            and has_volume
        )
        short_signal = (
            price < ema_200
            and previous_ema_9 >= previous_ema_21
            and ema_9 < ema_21
            and rsi < 50
            and has_volume
        )

        signal_type = "LONG" if long_signal else "SHORT" if short_signal else None
        if not signal_type:
            return None

        if signal_type == "LONG":
            reasons = [
                f"Price {price:.4f} is above EMA200 {ema_200:.4f}",
                f"EMA9 crossed above EMA21 ({previous_ema_9:.4f}/{previous_ema_21:.4f} -> {ema_9:.4f}/{ema_21:.4f})",
                f"RSI14 is bullish at {rsi:.2f}",
                f"Volume is {volume_ratio:.2f}x average 20",
            ]
        else:
            reasons = [
                f"Price {price:.4f} is below EMA200 {ema_200:.4f}",
                f"EMA9 crossed below EMA21 ({previous_ema_9:.4f}/{previous_ema_21:.4f} -> {ema_9:.4f}/{ema_21:.4f})",
                f"RSI14 is bearish at {rsi:.2f}",
                f"Volume is {volume_ratio:.2f}x average 20",
            ]

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
            volume_status=f"{volume_ratio:.2f}x avg20",
            reasons=reasons,
            indicators={
                "ema_9": ema_9,
                "ema_21": ema_21,
                "ema_200": ema_200,
                "rsi_14": rsi,
                "volume_ratio": volume_ratio,
            },
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
