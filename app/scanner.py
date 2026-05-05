import asyncio
import json
import logging
from collections import deque
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


class MarketDataError(Exception):
    def __init__(self, symbol: str, timeframe: str, status_code: int, reason: str) -> None:
        self.symbol = symbol
        self.timeframe = timeframe
        self.status_code = status_code
        self.reason = reason
        super().__init__(f"{symbol} {timeframe}: HTTP {status_code} {reason}")

    def dashboard_message(self) -> str:
        if self.status_code == 418:
            return (
                f"Binance REST blocked this server IP with HTTP 418 while loading {self.symbol} {self.timeframe}. "
                "Reduce REST refreshes or move to a fresh/persistent outbound IP."
            )
        if self.status_code == 429:
            return (
                f"Binance REST rate limit hit with HTTP 429 while loading {self.symbol} {self.timeframe}. "
                "Scanner will retry on the next REST refresh."
            )
        return f"Binance REST HTTP {self.status_code} while loading {self.symbol} {self.timeframe}."


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
        self._events: deque[dict] = deque(maxlen=50)
        self._last_alert_at: dict[str, datetime] = {}
        self._websocket_connected = False
        self._watchlist_version = 0

    async def start(self) -> None:
        self._started_at = datetime.now(timezone.utc)
        self._record_event("info", "Scanner started.")
        await asyncio.gather(self._refresh_candles_loop(), self._run_websocket_loop())

    async def stop(self) -> None:
        self._stop_event.set()

    async def _refresh_candles_loop(self) -> None:
        while not self._stop_event.is_set():
            await self._refresh_watchlist()
            await self._load_initial_candles()
            await asyncio.sleep(self.settings.rest_refresh_seconds)

    async def _load_initial_candles(self) -> None:
        semaphore = asyncio.Semaphore(self.settings.rest_concurrency)

        async def fetch_with_limit(symbol: str, timeframe: str) -> tuple[str, str, pd.DataFrame]:
            async with semaphore:
                return await self._fetch_klines(client, symbol, timeframe)

        async with httpx.AsyncClient(base_url=self.settings.binance_rest_url, timeout=15) as client:
            tasks = [
                fetch_with_limit(symbol, timeframe)
                for symbol in self._active_symbols
                for timeframe in self.settings.timeframes
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        had_error = False
        for result in results:
            if isinstance(result, Exception):
                had_error = True
                if isinstance(result, MarketDataError):
                    self._last_error = result.dashboard_message()
                    self._record_event("error", self._last_error)
                    await self._send_system_alert(f"market-data:{result.status_code}", self._last_error)
                else:
                    self._last_error = f"Initial candle load failed: {result}"
                    self._record_event("error", self._last_error)
                    await self._send_system_alert("market-data:load-failed", self._last_error)
                logger.error("Initial candle load failed: %s", result)
                continue
            symbol, timeframe, candles = result
            self._candles[(symbol, timeframe)] = candles
            signal = self._detect_signal(symbol, timeframe)
            if signal and self.store.add_if_new(signal, self.settings.signal_cooldown_minutes):
                logger.info("New %s signal for %s %s from REST refresh.", signal.signal_type, symbol, timeframe)
                await self.alerter.send_signal(signal)
        if not had_error:
            if self._last_error:
                self._record_event("info", "Binance REST candle refresh recovered.")
            self._last_error = None
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
                self._record_event("info", f"Watchlist updated: {len(symbols)} symbols.")
                logger.info("Updated auto watchlist: %s", ", ".join(symbols))

            self._watchlist_updated_at = datetime.now(timezone.utc)
            self._last_error = None
        except Exception as exc:
            self._last_error = f"Auto watchlist refresh failed: {exc}"
            self._record_event("error", self._last_error)
            await self._send_system_alert("watchlist:refresh-failed", self._last_error)
            logger.exception("Auto watchlist refresh failed.")

    async def _fetch_klines(
        self, client: httpx.AsyncClient, symbol: str, timeframe: str
    ) -> tuple[str, str, pd.DataFrame]:
        response = await client.get(
            "/fapi/v1/klines",
            params={"symbol": symbol, "interval": timeframe, "limit": self.settings.kline_limit},
        )
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise MarketDataError(symbol, timeframe, exc.response.status_code, exc.response.reason_phrase) from exc
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
                    self._record_event("info", "Binance websocket connected.")
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
                self._record_event("warning", "Websocket receive timeout. Reconnecting.")
                logger.info("Websocket receive timeout. Reconnecting.")
            except asyncio.CancelledError:
                raise
            except Exception:
                self._websocket_connected = False
                self._last_error = f"Websocket connection failed. Reconnecting in {backoff_seconds} seconds."
                self._record_event("error", self._last_error)
                await self._send_system_alert("websocket:connection-failed", self._last_error)
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
            if signal and self.store.add_if_new(signal, self.settings.signal_cooldown_minutes):
                logger.info("New %s signal for %s %s.", signal.signal_type, symbol, timeframe)
                await self.alerter.send_signal(signal)
        except (KeyError, ValueError, TypeError, json.JSONDecodeError):
            self._last_error = "Failed to parse websocket message."
            self._record_event("warning", self._last_error)
            logger.exception("Failed to parse websocket message.")

    def status(self) -> dict:
        pairs = []
        near_setups = []
        now = datetime.now(timezone.utc)
        total_pair_count = len(self._active_symbols) * len(self.settings.timeframes)
        loaded_pair_count = 0
        stale_pair_count = 0
        latest_closed_candle_at = None
        for symbol in self._active_symbols:
            for timeframe in self.settings.timeframes:
                candles = self._candles.get((symbol, timeframe))
                latest = None
                count = 0
                price = None
                indicators = None
                near_setup = None
                if candles is not None and not candles.empty:
                    count = len(candles)
                    latest_row = candles.iloc[-1]
                    latest_time = latest_row["close_time"].to_pydatetime()
                    if latest_time.tzinfo is None:
                        latest_time = latest_time.replace(tzinfo=timezone.utc)
                    latest = latest_time.isoformat()
                    loaded_pair_count += 1
                    if latest_closed_candle_at is None or latest_time > latest_closed_candle_at:
                        latest_closed_candle_at = latest_time
                    if (now - latest_time).total_seconds() > self._stale_after_seconds(timeframe):
                        stale_pair_count += 1
                    price = float(latest_row["close"])
                    indicators = self._indicator_snapshot(candles)
                    if indicators:
                        near_setup = self._near_setup(symbol, timeframe, indicators)
                        if near_setup:
                            near_setups.append(near_setup)

                pairs.append(
                    {
                        "symbol": symbol,
                        "timeframe": timeframe,
                        "candle_count": count,
                        "last_closed_candle_at": latest,
                        "last_close_price": price,
                        "indicators": indicators,
                        "near_setup": near_setup["setup_type"] if near_setup else None,
                    }
                )

        if self._last_error:
            market_data_status = "Error"
        elif loaded_pair_count == 0:
            market_data_status = "Loading"
        elif stale_pair_count > 0:
            market_data_status = "Stale"
        else:
            market_data_status = "OK"

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
            "signal_cooldown_minutes": self.settings.signal_cooldown_minutes,
            "market_data_status": market_data_status,
            "total_pair_count": total_pair_count,
            "loaded_pair_count": loaded_pair_count,
            "stale_pair_count": stale_pair_count,
            "latest_closed_candle_at": latest_closed_candle_at.isoformat() if latest_closed_candle_at else None,
            "events": list(self._events),
            "near_setups": near_setups,
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

    def _indicator_snapshot(self, candles: pd.DataFrame) -> dict | None:
        if len(candles) < 200:
            return None

        df = add_indicators(candles)
        latest = df.iloc[-1]
        required = ["ema_9", "ema_21", "ema_200", "rsi_14", "volume_avg_20"]
        if latest[required].isna().any():
            return None

        price = float(latest["close"])
        volume = float(latest["volume"])
        volume_average = float(latest["volume_avg_20"])
        ema_9 = float(latest["ema_9"])
        ema_21 = float(latest["ema_21"])
        ema_gap_pct = abs(ema_9 - ema_21) / price * 100 if price > 0 else 0
        volume_ratio = volume / volume_average if volume_average > 0 else 0
        return {
            "price": price,
            "ema_9": ema_9,
            "ema_21": ema_21,
            "ema_200": float(latest["ema_200"]),
            "rsi_14": float(latest["rsi_14"]),
            "volume_ratio": volume_ratio,
            "ema_gap_pct": ema_gap_pct,
        }

    def _near_setup(self, symbol: str, timeframe: str, indicators: dict) -> dict | None:
        price = indicators["price"]
        ema_9 = indicators["ema_9"]
        ema_21 = indicators["ema_21"]
        ema_200 = indicators["ema_200"]
        rsi = indicators["rsi_14"]
        volume_ratio = indicators["volume_ratio"]
        ema_gap_pct = indicators["ema_gap_pct"]
        close_enough = ema_gap_pct <= self.settings.near_cross_threshold_pct
        enough_volume = volume_ratio >= self.settings.near_volume_ratio_min

        setup_type = None
        if price > ema_200 and ema_9 <= ema_21 and rsi >= 48 and close_enough and enough_volume:
            setup_type = "LONG"
        elif price < ema_200 and ema_9 >= ema_21 and rsi <= 52 and close_enough and enough_volume:
            setup_type = "SHORT"

        if not setup_type:
            return None

        return {
            "symbol": symbol,
            "timeframe": timeframe,
            "setup_type": setup_type,
            "price": price,
            "rsi": rsi,
            "volume_ratio": volume_ratio,
            "ema_gap_pct": ema_gap_pct,
            "tradingview_url": f"https://www.tradingview.com/chart/?symbol=BINANCE:{symbol}.P",
        }

    @staticmethod
    def _stale_after_seconds(timeframe: str) -> int:
        if timeframe.endswith("m"):
            return int(timeframe.removesuffix("m")) * 60 * 3
        if timeframe.endswith("h"):
            return int(timeframe.removesuffix("h")) * 60 * 60 * 3
        return 60 * 60 * 3

    def _record_event(self, level: str, message: str) -> None:
        self._events.appendleft(
            {
                "level": level,
                "message": message,
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
        )

    async def _send_system_alert(self, key: str, message: str) -> None:
        if not self.alerter.enabled:
            return

        now = datetime.now(timezone.utc)
        last_alert_at = self._last_alert_at.get(key)
        cooldown_seconds = self.settings.system_alert_cooldown_minutes * 60
        if last_alert_at and (now - last_alert_at).total_seconds() < cooldown_seconds:
            return

        self._last_alert_at[key] = now
        await self.alerter.send_text(f"CF Scanner system alert\n{message}")

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
