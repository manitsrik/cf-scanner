import asyncio
import json
import logging
from collections import deque
from datetime import datetime, timedelta, timezone

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
        self._rest_backoff_until: datetime | None = None
        self._watchlist_updated_at: datetime | None = None
        self._last_error: str | None = None
        self._events: deque[dict] = deque(maxlen=50)
        self._last_alert_at: dict[str, datetime] = {}
        self._last_market_bias_label: str | None = None
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
            if self._rest_backoff_until:
                now = datetime.now(timezone.utc)
                if now < self._rest_backoff_until:
                    wait_seconds = (self._rest_backoff_until - now).total_seconds()
                    self._last_error = (
                        "Binance REST is cooling down after a rate-limit response. "
                        f"Next REST refresh after {self._rest_backoff_until.isoformat()}."
                    )
                    self._record_event("warning", self._last_error)
                    await asyncio.sleep(min(wait_seconds, self.settings.rest_refresh_seconds))
                    continue
                self._rest_backoff_until = None

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
                    self._schedule_rest_backoff(result.status_code)
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
        await self._maybe_send_market_bias_alert()

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
            await self._maybe_send_market_bias_alert()
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

        market_overview = self._market_overview()
        best_setups = self._best_setups(market_overview, near_setups)

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
            "rest_backoff_until": self._rest_backoff_until.isoformat() if self._rest_backoff_until else None,
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
            "market_overview": market_overview,
            "best_setups": best_setups,
            "pairs": pairs,
        }

    def symbols(self) -> list[str]:
        return list(self._active_symbols)

    def indicator_series(self, symbol: str, timeframe: str, limit: int = 120) -> dict:
        candles = self._candles.get((symbol, timeframe))
        if candles is None or candles.empty:
            return {"symbol": symbol, "timeframe": timeframe, "points": []}

        df = add_indicators(candles).tail(limit)
        points = []
        for _, row in df.iterrows():
            points.append(
                {
                    "time": row["close_time"].to_pydatetime().isoformat(),
                    "close": self._clean_number(row["close"]),
                    "rsi_14": self._clean_number(row["rsi_14"]),
                    "macd": self._clean_number(row["macd"]),
                    "macd_signal": self._clean_number(row["macd_signal"]),
                    "macd_diff": self._clean_number(row["macd_diff"]),
                }
            )

        return {"symbol": symbol, "timeframe": timeframe, "points": points}

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
        quality = self._signal_quality(signal_type, price, ema_9, ema_21, ema_200, rsi, volume_ratio)
        trade_plan = self._trade_plan(signal_type, df, len(df) - 1)
        backtest = self._signal_backtest(df, signal_type)
        status = self._signal_status(signal_type, trade_plan, df, close_time)
        trader_summary = self._trader_summary(
            signal_type=signal_type,
            quality_label=quality["label"],
            quality_score=quality["score"],
            rsi=rsi,
            volume_ratio=volume_ratio,
            trade_plan=trade_plan,
            backtest=backtest,
            status=status,
        )
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
            quality_score=quality["score"],
            quality_label=quality["label"],
            quality_reasons=quality["reasons"],
            trade_plan=trade_plan,
            backtest=backtest,
            status=status,
            trader_summary=trader_summary,
            created_at=datetime.now(timezone.utc),
            tradingview_url=f"https://www.tradingview.com/chart/?symbol=BINANCE:{symbol}.P",
        )

    def enrich_signal_context(self, signal: Signal) -> dict:
        candles = self._candles.get((signal.symbol, signal.timeframe))
        if candles is None or len(candles) < 30:
            return {
                "trade_plan": signal.trade_plan or {},
                "backtest": signal.backtest or {},
                "status": signal.status or {"state": "waiting", "label": "Waiting for market data"},
            }

        df = add_indicators(candles)
        signal_time = self._signal_close_time(signal)
        signal_index = self._signal_index(df, signal_time)
        trade_plan = signal.trade_plan or self._trade_plan(signal.signal_type, df, signal_index)
        backtest = signal.backtest or self._signal_backtest(df, signal.signal_type)
        status = self._signal_status(signal.signal_type, trade_plan, df, signal_time)
        trader_summary = signal.trader_summary or self._trader_summary(
            signal_type=signal.signal_type,
            quality_label=signal.quality_label,
            quality_score=signal.quality_score,
            rsi=float(signal.rsi or 0),
            volume_ratio=float((signal.indicators or {}).get("volume_ratio") or 0),
            trade_plan=trade_plan,
            backtest=backtest,
            status=status,
        )
        return {"trade_plan": trade_plan, "backtest": backtest, "status": status, "trader_summary": trader_summary}

    def _trade_plan(self, signal_type: str, df: pd.DataFrame, index: int | None = None) -> dict:
        if df.empty:
            return {}

        index = len(df) - 1 if index is None else max(0, min(index, len(df) - 1))
        row = df.iloc[index]
        entry = float(row["close"])
        atr = self._atr(df, index)
        lookback = df.iloc[max(0, index - 12) : index + 1]
        if lookback.empty or entry <= 0:
            return {}

        buffer = max(atr * 0.25, entry * 0.0015)
        if signal_type == "LONG":
            structure_stop = float(lookback["low"].min()) - buffer
            atr_stop = entry - atr * 1.4
            stop_loss = min(structure_stop, atr_stop)
            risk = entry - stop_loss
            take_profit_1 = entry + risk * 1.5
            take_profit_2 = entry + risk * 2.4
            invalidation = "Close back below EMA21 or stop loss is touched"
        else:
            structure_stop = float(lookback["high"].max()) + buffer
            atr_stop = entry + atr * 1.4
            stop_loss = max(structure_stop, atr_stop)
            risk = stop_loss - entry
            take_profit_1 = entry - risk * 1.5
            take_profit_2 = entry - risk * 2.4
            invalidation = "Close back above EMA21 or stop loss is touched"

        if risk <= 0:
            risk = max(atr, entry * 0.005)
            stop_loss = entry - risk if signal_type == "LONG" else entry + risk
            take_profit_1 = entry + risk * 1.5 if signal_type == "LONG" else entry - risk * 1.5
            take_profit_2 = entry + risk * 2.4 if signal_type == "LONG" else entry - risk * 2.4

        risk_pct = risk / entry * 100 if entry > 0 else 0
        zone_a = entry * 0.998
        zone_b = entry * 1.002
        return {
            "entry": entry,
            "entry_zone_low": min(zone_a, zone_b),
            "entry_zone_high": max(zone_a, zone_b),
            "stop_loss": stop_loss,
            "take_profit_1": take_profit_1,
            "take_profit_2": take_profit_2,
            "risk_reward_1": 1.5,
            "risk_reward_2": 2.4,
            "risk_pct": risk_pct,
            "atr_14": atr,
            "invalidation": invalidation,
            "note": "Position size should be based on stop distance, not on signal confidence.",
        }

    def _signal_backtest(self, df: pd.DataFrame, signal_type: str, lookahead: int = 12) -> dict:
        if len(df) < 220:
            return {"sample_size": 0, "summary": "Need at least 220 candles for local backtest"}

        results = []
        max_start = len(df) - lookahead - 1
        for index in range(201, max_start + 1):
            if not self._historical_signal_matches(df, index, signal_type):
                continue
            plan = self._trade_plan(signal_type, df, index)
            if not plan:
                continue
            outcome = self._simulate_plan(signal_type, plan, df.iloc[index + 1 : index + 1 + lookahead])
            results.append(outcome)

        if not results:
            return {
                "sample_size": 0,
                "lookahead_candles": lookahead,
                "summary": "No similar setups found in current candle window",
            }

        wins = sum(1 for result in results if result["outcome"] == "tp1")
        losses = sum(1 for result in results if result["outcome"] == "sl")
        expired = len(results) - wins - losses
        avg_move = sum(result["max_favorable_pct"] for result in results) / len(results)
        avg_drawdown = sum(result["max_adverse_pct"] for result in results) / len(results)
        return {
            "sample_size": len(results),
            "lookahead_candles": lookahead,
            "tp1_hits": wins,
            "stop_hits": losses,
            "expired": expired,
            "win_rate": wins / len(results) * 100,
            "average_favorable_pct": avg_move,
            "average_adverse_pct": avg_drawdown,
            "summary": f"{wins}/{len(results)} hit TP1 before SL over the next {lookahead} candles",
        }

    def _signal_status(self, signal_type: str, plan: dict, df: pd.DataFrame, signal_time: datetime | None) -> dict:
        if not plan or df.empty:
            return {"state": "waiting", "label": "Waiting for plan"}

        signal_index = self._signal_index(df, signal_time)
        after = df.iloc[signal_index + 1 :]
        if after.empty:
            return {"state": "active", "label": "Active", "detail": "Waiting for the next closed candle"}

        outcome = self._simulate_plan(signal_type, plan, after)
        current = float(df.iloc[-1]["close"])
        entry = float(plan.get("entry") or 0)
        move_pct = self._directional_move_pct(signal_type, entry, current)
        latest_time = df.iloc[-1]["close_time"].to_pydatetime().isoformat()

        if outcome["outcome"] == "tp1":
            return {
                "state": "tp1",
                "label": "TP1 hit",
                "detail": "Price reached take profit 1 before stop loss",
                "current_move_pct": move_pct,
                "updated_at": latest_time,
            }
        if outcome["outcome"] == "sl":
            return {
                "state": "stopped",
                "label": "Stopped",
                "detail": "Price touched stop loss before TP1",
                "current_move_pct": move_pct,
                "updated_at": latest_time,
            }
        if len(after) >= 12:
            return {
                "state": "expired",
                "label": "Expired",
                "detail": "No TP1 or stop hit within 12 closed candles",
                "current_move_pct": move_pct,
                "updated_at": latest_time,
            }
        return {
            "state": "active",
            "label": "Active",
            "detail": f"{len(after)} closed candle(s) since signal",
            "current_move_pct": move_pct,
            "updated_at": latest_time,
        }

    def _trader_summary(
        self,
        signal_type: str,
        quality_label: str | None,
        quality_score: float | None,
        rsi: float,
        volume_ratio: float,
        trade_plan: dict,
        backtest: dict,
        status: dict,
        news_context: dict | None = None,
    ) -> str:
        side_thai = "Long" if signal_type == "LONG" else "Short"
        score = float(quality_score or 0)
        risk_pct = float(trade_plan.get("risk_pct") or 0)
        win_rate = float(backtest.get("win_rate") or 0)
        sample_size = int(backtest.get("sample_size") or 0)
        state = status.get("state")
        relation = (news_context or {}).get("relation")

        strengths = []
        cautions = []

        if score >= 85:
            strengths.append(f"สัญญาณ {side_thai} แข็งแรงมากจาก quality {quality_label or ''} {score:.0f}")
        elif score >= 70:
            strengths.append(f"สัญญาณ {side_thai} ค่อนข้างดี quality {quality_label or ''} {score:.0f}")
        elif score >= 55:
            cautions.append("คุณภาพสัญญาณยังระดับต้องระวัง ควรรอ confirmation เพิ่ม")
        else:
            cautions.append("คุณภาพสัญญาณอ่อน ยังไม่เหมาะกับการรีบเข้าไม้")

        if volume_ratio >= 2:
            strengths.append(f"volume หนุนชัด {volume_ratio:.2f}x avg20")
        elif volume_ratio >= 1:
            strengths.append(f"volume ผ่านเกณฑ์ {volume_ratio:.2f}x avg20")

        if signal_type == "LONG" and rsi >= 68:
            cautions.append(f"RSI {rsi:.2f} เริ่มสูง ระวังไล่ราคา")
        if signal_type == "SHORT" and rsi <= 32:
            cautions.append(f"RSI {rsi:.2f} เริ่มต่ำ ระวังขายตามปลายทาง")

        if risk_pct >= 6:
            cautions.append(f"stop ค่อนข้างไกล {risk_pct:.2f}% ควรลดขนาดไม้")
        elif 0 < risk_pct <= 3:
            strengths.append(f"risk ต่อไม้คุมง่ายประมาณ {risk_pct:.2f}%")

        if sample_size >= 5:
            if win_rate >= 55:
                strengths.append(f"backtest ระยะสั้นหนุน win rate {win_rate:.0f}% จาก {sample_size} ตัวอย่าง")
            elif win_rate <= 40:
                cautions.append(f"backtest ระยะสั้นยังไม่สวย win rate {win_rate:.0f}% จาก {sample_size} ตัวอย่าง")
        else:
            cautions.append("backtest ยังมีตัวอย่างน้อย ใช้เป็นบริบทเท่านั้น")

        if relation == "conflicts":
            cautions.append("มีข่าวขัดกับทิศทางสัญญาณ ควรรอความผันผวนสงบหรือใช้ size เล็ก")
        elif relation == "supports":
            strengths.append("ข่าวล่าสุดหนุนทิศทางสัญญาณ")

        if state == "tp1":
            action = "สัญญาณนี้ถึง TP1 แล้ว ไม่ควรไล่เข้าใหม่ถ้าไม่มี setup ใหม่ ให้เน้นจัดการกำไรหรือรอย่อ"
        elif state == "stopped":
            action = "สัญญาณนี้โดน stop แล้ว ควรตัดออกจากแผนและรอสัญญาณใหม่"
        elif state == "expired":
            action = "สัญญาณหมดอายุแล้ว เพราะไม่ไปถึง TP1 หรือ SL ในกรอบเวลาที่กำหนด ควรรอ setup ใหม่"
        elif score >= 85 and risk_pct <= 6 and relation != "conflicts":
            action = "แผนที่เหมาะคือรอราคาอยู่ใน entry zone แล้วค่อยพิจารณาเข้า ไม่ไล่ราคา และวาง stop ตามแผน"
        elif score >= 70:
            action = "ใช้เป็น setup เฝ้าดูได้ แต่ควรรอแท่งยืนยันหรือจังหวะย่อเข้าใกล้ entry zone ก่อน"
        else:
            action = "ยังไม่ควรรีบเข้าไม้ ให้รอ confirmation เพิ่มหรือเลือก setup ที่คุณภาพสูงกว่า"

        strength_text = " จุดแข็ง: " + "; ".join(strengths[:3]) + "." if strengths else ""
        caution_text = " ข้อควรระวัง: " + "; ".join(cautions[:3]) + "." if cautions else ""
        return f"สรุปแบบเทรดเดอร์: {action}{strength_text}{caution_text}"

    def _historical_signal_matches(self, df: pd.DataFrame, index: int, signal_type: str) -> bool:
        latest = df.iloc[index]
        previous = df.iloc[index - 1]
        required = ["ema_9", "ema_21", "ema_200", "rsi_14", "volume_avg_20"]
        if latest[required].isna().any() or previous[["ema_9", "ema_21"]].isna().any():
            return False
        price = float(latest["close"])
        volume_average = float(latest["volume_avg_20"])
        if volume_average <= 0 or float(latest["volume"]) <= volume_average:
            return False
        if signal_type == "LONG":
            return (
                price > float(latest["ema_200"])
                and float(previous["ema_9"]) <= float(previous["ema_21"])
                and float(latest["ema_9"]) > float(latest["ema_21"])
                and float(latest["rsi_14"]) > 50
            )
        return (
            price < float(latest["ema_200"])
            and float(previous["ema_9"]) >= float(previous["ema_21"])
            and float(latest["ema_9"]) < float(latest["ema_21"])
            and float(latest["rsi_14"]) < 50
        )

    def _simulate_plan(self, signal_type: str, plan: dict, future: pd.DataFrame) -> dict:
        entry = float(plan.get("entry") or 0)
        stop = float(plan.get("stop_loss") or 0)
        tp1 = float(plan.get("take_profit_1") or 0)
        max_favorable_pct = 0.0
        max_adverse_pct = 0.0
        for _, row in future.iterrows():
            high = float(row["high"])
            low = float(row["low"])
            if signal_type == "LONG":
                max_favorable_pct = max(max_favorable_pct, self._directional_move_pct(signal_type, entry, high))
                max_adverse_pct = min(max_adverse_pct, self._directional_move_pct(signal_type, entry, low))
                if low <= stop:
                    return {"outcome": "sl", "max_favorable_pct": max_favorable_pct, "max_adverse_pct": max_adverse_pct}
                if high >= tp1:
                    return {"outcome": "tp1", "max_favorable_pct": max_favorable_pct, "max_adverse_pct": max_adverse_pct}
            else:
                max_favorable_pct = max(max_favorable_pct, self._directional_move_pct(signal_type, entry, low))
                max_adverse_pct = min(max_adverse_pct, self._directional_move_pct(signal_type, entry, high))
                if high >= stop:
                    return {"outcome": "sl", "max_favorable_pct": max_favorable_pct, "max_adverse_pct": max_adverse_pct}
                if low <= tp1:
                    return {"outcome": "tp1", "max_favorable_pct": max_favorable_pct, "max_adverse_pct": max_adverse_pct}
        return {"outcome": "open", "max_favorable_pct": max_favorable_pct, "max_adverse_pct": max_adverse_pct}

    def _atr(self, df: pd.DataFrame, index: int, period: int = 14) -> float:
        start = max(1, index - period + 1)
        window = df.iloc[start : index + 1]
        previous_close = df["close"].shift(1).iloc[start : index + 1]
        true_range = pd.concat(
            [
                window["high"] - window["low"],
                (window["high"] - previous_close).abs(),
                (window["low"] - previous_close).abs(),
            ],
            axis=1,
        ).max(axis=1)
        atr = float(true_range.mean()) if not true_range.empty else 0
        price = float(df.iloc[index]["close"])
        return atr if atr > 0 else price * 0.006

    def _signal_close_time(self, signal: Signal) -> datetime | None:
        parts = signal.id.split(":")
        if len(parts) >= 3 and parts[2].isdigit():
            return datetime.fromtimestamp(int(parts[2]), tz=timezone.utc)
        return signal.created_at

    @staticmethod
    def _signal_index(df: pd.DataFrame, signal_time: datetime | None) -> int:
        if signal_time is None:
            return len(df) - 1
        if signal_time.tzinfo is None:
            signal_time = signal_time.replace(tzinfo=timezone.utc)
        matches = df.index[df["close_time"] >= pd.Timestamp(signal_time)]
        if len(matches):
            return int(matches[0])
        return len(df) - 1

    @staticmethod
    def _directional_move_pct(signal_type: str, entry: float, price: float) -> float:
        if entry <= 0:
            return 0.0
        if signal_type == "LONG":
            return (price - entry) / entry * 100
        return (entry - price) / entry * 100

    def _signal_quality(
        self,
        signal_type: str,
        price: float,
        ema_9: float,
        ema_21: float,
        ema_200: float,
        rsi: float,
        volume_ratio: float,
    ) -> dict:
        trend_distance_pct = abs(price - ema_200) / price * 100 if price > 0 else 0
        cross_gap_pct = abs(ema_9 - ema_21) / price * 100 if price > 0 else 0
        trend_score = self._clamp(trend_distance_pct / 1.2 * 100, 35, 100)
        cross_score = self._clamp(cross_gap_pct / 0.35 * 100, 35, 100)
        volume_score = self._clamp(volume_ratio / 1.8 * 100, 35, 100)
        rsi_score = self._rsi_quality_score(signal_type, rsi)
        score = self._clamp(
            trend_score * 0.28 + cross_score * 0.2 + rsi_score * 0.24 + volume_score * 0.28,
            0,
            100,
        )
        if score >= 85:
            label = "Excellent"
        elif score >= 70:
            label = "Strong"
        elif score >= 55:
            label = "Caution"
        else:
            label = "Weak"

        reasons = [
            f"Trend distance {trend_distance_pct:.2f}% from EMA200",
            f"EMA cross gap {cross_gap_pct:.3f}%",
            f"RSI quality {rsi_score:.0f}/100",
            f"Volume strength {volume_ratio:.2f}x avg20",
        ]
        return {"score": score, "label": label, "reasons": reasons}

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

    def _market_overview(self) -> dict:
        timeframe = self.settings.timeframes[0] if self.settings.timeframes else "15m"
        timeframe_seconds = self._timeframe_seconds(timeframe)
        lookback_periods = max(1, round((24 * 60 * 60) / timeframe_seconds))
        movers = []

        for symbol in self._active_symbols:
            candles = self._candles.get((symbol, timeframe))
            if candles is None or candles.empty:
                continue

            latest = candles.iloc[-1]
            reference_index = max(0, len(candles) - lookback_periods - 1)
            reference = candles.iloc[reference_index]
            latest_close = float(latest["close"])
            reference_close = float(reference["close"])
            if reference_close <= 0:
                continue

            change_pct = (latest_close - reference_close) / reference_close * 100
            movers.append(
                {
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "price": latest_close,
                    "change_pct": change_pct,
                    "direction": "up" if change_pct > 0 else "down" if change_pct < 0 else "flat",
                    "latest_closed_candle_at": latest["close_time"].to_pydatetime().isoformat(),
                    "reference_closed_candle_at": reference["close_time"].to_pydatetime().isoformat(),
                }
            )

        gainers = sum(1 for item in movers if item["change_pct"] > 0)
        losers = sum(1 for item in movers if item["change_pct"] < 0)
        average_change_pct = sum(item["change_pct"] for item in movers) / len(movers) if movers else 0
        breadth_pct = gainers / len(movers) * 100 if movers else 0
        bias_score = max(0, min(100, (breadth_pct * 0.7) + ((average_change_pct + 5) / 10 * 100 * 0.3)))
        if not movers:
            direction = "Loading"
            bias_label = "Loading"
        elif average_change_pct > 0 and gainers >= losers:
            direction = "Up"
            bias_label = "Long Bias" if bias_score >= 60 else "Neutral"
        elif average_change_pct < 0 and losers > gainers:
            direction = "Down"
            bias_label = "Short Bias" if bias_score <= 40 else "Neutral"
        else:
            direction = "Mixed"
            bias_label = "Neutral"

        return {
            "timeframe": timeframe,
            "lookback_hours": 24,
            "direction": direction,
            "bias_label": bias_label,
            "bias_score": bias_score,
            "average_change_pct": average_change_pct,
            "breadth_pct": breadth_pct,
            "gainers": gainers,
            "losers": losers,
            "flat": len(movers) - gainers - losers,
            "movers": sorted(movers, key=lambda item: item["change_pct"], reverse=True),
            "risk": self._market_risk(direction, bias_label, breadth_pct, average_change_pct, movers),
        }

    def _best_setups(self, overview: dict, near_setups: list[dict]) -> list[dict]:
        timeframe = overview.get("timeframe") or (self.settings.timeframes[0] if self.settings.timeframes else "15m")
        movers = {item["symbol"]: item for item in overview.get("movers", [])}
        near_by_symbol = {item["symbol"]: item for item in near_setups if item.get("timeframe") == timeframe}
        bias_label = overview.get("bias_label", "Neutral")
        opportunities = []

        for rank, symbol in enumerate(self._active_symbols):
            candles = self._candles.get((symbol, timeframe))
            if candles is None or len(candles) < 200 or symbol not in movers:
                continue
            indicators = self._indicator_snapshot(candles)
            if not indicators:
                continue

            mover = movers[symbol]
            change_pct = float(mover.get("change_pct", 0))
            setup = near_by_symbol.get(symbol)
            side = self._preferred_side(bias_label, change_pct, indicators)
            trend_aligned = (
                side == "LONG" and indicators["price"] > indicators["ema_200"]
            ) or (
                side == "SHORT" and indicators["price"] < indicators["ema_200"]
            )
            momentum_score = self._clamp(abs(change_pct) / 4 * 100, 0, 100)
            if side == "LONG" and change_pct < 0:
                momentum_score *= 0.35
            if side == "SHORT" and change_pct > 0:
                momentum_score *= 0.35
            rsi_score = self._rsi_quality_score(side, indicators["rsi_14"])
            volume_score = self._clamp(indicators["volume_ratio"] / 1.5 * 100, 0, 100)
            liquidity_score = 100 - (rank / max(1, len(self._active_symbols) - 1) * 30)
            setup_bonus = 20 if setup and setup.get("setup_type") == side else 0
            trend_bonus = 12 if trend_aligned else -10
            score = self._clamp(
                momentum_score * 0.28
                + rsi_score * 0.2
                + volume_score * 0.16
                + liquidity_score * 0.16
                + setup_bonus
                + trend_bonus,
                0,
                100,
            )
            reasons = []
            if setup and setup.get("setup_type") == side:
                reasons.append("near setup")
            if trend_aligned:
                reasons.append("trend aligned")
            if indicators["volume_ratio"] >= self.settings.near_volume_ratio_min:
                reasons.append(f"volume {indicators['volume_ratio']:.2f}x")
            reasons.append(f"24h {change_pct:+.2f}%")

            opportunities.append(
                {
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "side": side,
                    "score": score,
                    "change_pct": change_pct,
                    "rsi": indicators["rsi_14"],
                    "volume_ratio": indicators["volume_ratio"],
                    "trend_aligned": trend_aligned,
                    "near_setup": setup.get("setup_type") if setup else None,
                    "reasons": reasons,
                    "tradingview_url": f"https://www.tradingview.com/chart/?symbol=BINANCE:{symbol}.P",
                }
            )

        return sorted(opportunities, key=lambda item: item["score"], reverse=True)[:10]

    def _market_risk(
        self,
        direction: str,
        bias_label: str,
        breadth_pct: float,
        average_change_pct: float,
        movers: list[dict],
    ) -> dict:
        flags = []
        level = "Low"
        if bias_label == "Neutral":
            flags.append("Bias ยังเป็นกลาง: ลดขนาดไม้และรอ confirmation ก่อนเข้าเทรด")
            level = "Moderate"
        if 45 <= breadth_pct <= 55:
            flags.append("Breadth ก้ำกึ่ง: เหรียญขึ้นลงใกล้เคียงกัน อย่าฝืนเลือกทาง")
            level = "Moderate"
        if movers:
            max_abs_move = max(abs(float(item.get("change_pct", 0))) for item in movers)
            if max_abs_move >= 8:
                flags.append("มีเหรียญที่วิ่งไกลแล้ว: ระวังการไล่ราคาและรอจังหวะย่อ")
                level = "High"
        btc = next((item for item in movers if item.get("symbol") == "BTCUSDT"), None)
        eth = next((item for item in movers if item.get("symbol") == "ETHUSDT"), None)
        if btc and eth and float(btc["change_pct"]) * float(eth["change_pct"]) < 0:
            flags.append("BTC กับ ETH สวนทางกัน: altcoins อาจแกว่งและหลอกทิศทางง่าย")
            level = "High" if level == "Moderate" else "Moderate"
        if direction == "Up" and average_change_pct < 0.15:
            flags.append("ตลาดเอียงขึ้น แต่ average gain ยังบาง: แรงซื้อยังไม่กว้าง")
            level = "Moderate"
        if not flags:
            flags.append("ความเสี่ยงปกติสำหรับ watchlist ปัจจุบัน")
        return {"level": level, "flags": flags}

    @staticmethod
    def _preferred_side(bias_label: str, change_pct: float, indicators: dict) -> str:
        if bias_label == "Long Bias":
            return "LONG"
        if bias_label == "Short Bias":
            return "SHORT"
        if change_pct > 0 and indicators["price"] > indicators["ema_200"]:
            return "LONG"
        if change_pct < 0 and indicators["price"] < indicators["ema_200"]:
            return "SHORT"
        return "LONG" if change_pct >= 0 else "SHORT"

    @staticmethod
    def _rsi_quality_score(side: str, rsi: float) -> float:
        if side == "LONG":
            if 50 <= rsi <= 68:
                return 100
            if 45 <= rsi < 50:
                return 70
            if 68 < rsi <= 75:
                return 65
            return 35
        if 32 <= rsi <= 50:
            return 100
        if 50 < rsi <= 55:
            return 70
        if 25 <= rsi < 32:
            return 65
        return 35

    def _thai_market_brief(self, overview: dict, best_setups: list[dict] | None = None) -> str:
        movers = overview.get("movers", [])
        if not movers:
            return "กำลังสรุปภาพรวมตลาด: ยังรอข้อมูล candles ให้ครบก่อน"

        gainers = int(overview.get("gainers", 0) or 0)
        losers = int(overview.get("losers", 0) or 0)
        flat = int(overview.get("flat", 0) or 0)
        total = gainers + losers + flat or len(movers)
        average = float(overview.get("average_change_pct", 0) or 0)
        breadth = float(overview.get("breadth_pct", 0) or 0)
        bias_score = float(overview.get("bias_score", 0) or 0)
        top_gainer = movers[0]
        top_loser = movers[-1]
        risk_flags = overview.get("risk", {}).get("flags", [])
        risk_text = risk_flags[0] if risk_flags else "ความเสี่ยงปกติสำหรับ watchlist ปัจจุบัน"
        setups = (best_setups or [])[:3]
        setup_text = (
            ", ".join(
                f"{setup['symbol']} ({self._thai_side_text(setup.get('side'))} {float(setup.get('score', 0)):.0f})"
                for setup in setups
            )
            if setups
            else "ยังไม่มีเหรียญเด่นจากคะแนนรวม"
        )
        action_text = self._thai_action_text(overview.get("bias_label"))

        return (
            "Thai Market Brief\n"
            f"ภาพรวม: ตลาดตอนนี้{self._thai_bias_text(overview.get('bias_label'))} "
            f"คะแนน {bias_score:.0f}/100 โดยมีเหรียญบวก {gainers}/{total} ตัว, "
            f"breadth {breadth:.0f}% และ average gain {self._format_percent(average)}\n"
            f"เหรียญน่าดู: {setup_text}\n"
            f"แรงนำ/แรงอ่อน: {top_gainer['symbol']} {self._format_percent(top_gainer.get('change_pct', 0))} เด่นสุด "
            f"ส่วน {top_loser['symbol']} {self._format_percent(top_loser.get('change_pct', 0))} อ่อนสุด\n"
            f"ความเสี่ยง: {risk_text}\n"
            f"แผน: {action_text}"
        )

    @staticmethod
    def _format_percent(value: float) -> str:
        number = float(value or 0)
        sign = "+" if number > 0 else ""
        return f"{sign}{number:.2f}%"

    @staticmethod
    def _thai_bias_text(label: str | None) -> str:
        if label == "Long Bias":
            return "เอนเอียงฝั่ง Long"
        if label == "Short Bias":
            return "เอนเอียงฝั่ง Short"
        if label == "Neutral":
            return "เป็นกลาง"
        return "กำลังโหลด"

    @staticmethod
    def _thai_side_text(side: str | None) -> str:
        if side == "LONG":
            return "Long"
        if side == "SHORT":
            return "Short"
        return "ดูกราฟ"

    @staticmethod
    def _thai_action_text(label: str | None) -> str:
        if label == "Long Bias":
            return "เน้นหา Long จาก Best Setups หรือ Near Setup และหลีกเลี่ยงการไล่ราคา"
        if label == "Short Bias":
            return "เน้นหา Short จากเหรียญที่อ่อนกว่าตลาด และรอ confirmation ก่อนเข้า"
        return "รอ confirmation ลดขนาดไม้ และเลือกเทรดเฉพาะ setup ที่ชัด"

    async def _maybe_send_market_bias_alert(self) -> None:
        overview = self._market_overview()
        bias_label = overview.get("bias_label")
        if bias_label in {None, "Loading"}:
            return
        if self._last_market_bias_label is None:
            self._last_market_bias_label = bias_label
            return
        if bias_label == self._last_market_bias_label:
            return

        previous = self._last_market_bias_label
        self._last_market_bias_label = bias_label
        best_setups = self._best_setups(overview, [])
        thai_brief = self._thai_market_brief(overview, best_setups)
        previous_badge = self._bias_badge(previous)
        current_badge = self._bias_badge(bias_label)
        message = (
            f"{current_badge} CF Scanner market bias changed\n"
            f"{previous_badge} {previous} -> {current_badge} {bias_label}\n"
            f"Score: {overview.get('bias_score', 0):.0f}/100\n"
            f"Breadth: {overview.get('breadth_pct', 0):.0f}% up\n"
            f"Average gain: {overview.get('average_change_pct', 0):+.2f}%\n\n"
            f"{thai_brief}"
        )
        self._record_event("info", message)
        await self._send_system_alert("market-bias", message)

    @staticmethod
    def _bias_badge(label: str | None) -> str:
        if label == "Long Bias":
            return "🟢"
        if label == "Short Bias":
            return "🔴"
        if label == "Neutral":
            return "🟡"
        return "⚪"

    @staticmethod
    def _timeframe_seconds(timeframe: str) -> int:
        if timeframe.endswith("m"):
            return int(timeframe.removesuffix("m")) * 60
        if timeframe.endswith("h"):
            return int(timeframe.removesuffix("h")) * 60 * 60
        return 15 * 60

    @staticmethod
    def _stale_after_seconds(timeframe: str) -> int:
        return FuturesScanner._timeframe_seconds(timeframe) * 3

    @staticmethod
    def _clamp(value: float, minimum: float, maximum: float) -> float:
        return max(minimum, min(maximum, value))

    def _record_event(self, level: str, message: str) -> None:
        self._events.appendleft(
            {
                "level": level,
                "message": message,
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
        )

    def _schedule_rest_backoff(self, status_code: int) -> None:
        if status_code not in {418, 429}:
            return
        backoff_until = datetime.now(timezone.utc) + timedelta(seconds=self.settings.rest_backoff_seconds)
        if self._rest_backoff_until is None or backoff_until > self._rest_backoff_until:
            self._rest_backoff_until = backoff_until

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
    def _clean_number(value) -> float | None:
        if pd.isna(value):
            return None
        return float(value)

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
