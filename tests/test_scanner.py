import asyncio
from datetime import datetime, timedelta, timezone

import httpx
import pandas as pd
import pytest

from app.config import Settings
from app.models import Signal
from app.scanner import FuturesScanner
from app.store import SignalStore
from app.telegram import TelegramAlerter

CANDLE = timedelta(minutes=15)


class CaptureAlerter(TelegramAlerter):
    def __init__(self) -> None:
        super().__init__("1:token", "42")
        self.messages: list[str] = []

    def notify(self, text: str) -> None:
        self.messages.append(text)


def make_scanner(
    store: SignalStore | None = None, alerter: TelegramAlerter | None = None, **overrides
) -> FuturesScanner:
    options = {
        "auto_watchlist_enabled": False,
        "telegram_bot_token": None,
        "telegram_chat_id": None,
        **overrides,
    }
    settings = Settings(_env_file=None, **options)
    return FuturesScanner(
        settings,
        store or SignalStore(limit=100, db_path=None),
        alerter or TelegramAlerter(None, None),
    )


def flat_candles(count: int, start: datetime, price: float = 100.0) -> pd.DataFrame:
    open_times = [start + CANDLE * index for index in range(count)]
    return pd.DataFrame(
        {
            "open_time": [pd.Timestamp(value) for value in open_times],
            "open": price,
            "high": price + 0.1,
            "low": price - 0.1,
            "close": price,
            "volume": 100.0,
            # Binance close times end 1 ms before the next candle opens.
            "close_time": [pd.Timestamp(value + CANDLE - timedelta(milliseconds=1)) for value in open_times],
        }
    )


def mock_binance(monkeypatch, handler) -> None:
    real_client = httpx.AsyncClient

    def mock_client(*args, **kwargs):
        return real_client(*args, transport=httpx.MockTransport(handler), **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", mock_client)


def to_klines(candles: pd.DataFrame) -> list[list]:
    return [
        [
            int(row.open_time.timestamp() * 1000),
            str(row.open),
            str(row.high),
            str(row.low),
            str(row.close),
            str(row.volume),
            int(row.close_time.timestamp() * 1000),
        ]
        for row in candles.itertuples()
    ]


def make_signal(close_time: datetime, trade_plan: dict) -> Signal:
    return Signal(
        id=f"BTCUSDT:15m:{int(close_time.timestamp())}:LONG",
        symbol="BTCUSDT",
        timeframe="15m",
        signal_type="LONG",
        price=100,
        rsi=55,
        volume=150,
        volume_average_20=100,
        volume_status="1.50x avg20",
        reasons=[],
        indicators={"volume_ratio": 1.5},
        trade_plan=trade_plan,
        status={"state": "active", "label": "Active"},
        created_at=close_time,
        tradingview_url="",
    )


PLAN = {"entry": 100.0, "stop_loss": 90.0, "take_profit_1": 110.0, "take_profit_2": 120.0, "risk_pct": 10.0}


def test_rest_klines_drop_the_unclosed_candle():
    now_ms = 1_700_000_000_000
    closed = [now_ms - 900_000, "1", "2", "0.5", "1.5", "10", now_ms - 1]
    forming = [now_ms, "1.5", "2", "1", "1.8", "3", now_ms + 899_999]

    frame = FuturesScanner._klines_to_dataframe([closed, forming], now_ms=now_ms)

    assert len(frame) == 1
    assert frame.iloc[-1]["close"] == 1.5


def test_rate_limit_stops_remaining_rest_requests(monkeypatch):
    requests = []

    def rate_limited(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(429, request=request)

    mock_binance(monkeypatch, rate_limited)
    scanner = make_scanner(symbols=["AAAUSDT", "BBBUSDT", "CCCUSDT"], timeframes=["5m", "15m"], rest_concurrency=1)

    asyncio.run(scanner._load_initial_candles())

    assert len(requests) == 1
    assert scanner._rest_backoff_active()
    assert scanner.status()["market_data_status"] == "Error"


def test_rest_refresh_keeps_candle_closed_over_websocket_mid_refresh(monkeypatch):
    candles = flat_candles(320, datetime.now(timezone.utc) - CANDLE * 400)
    snapshot, closed_mid_refresh = candles.iloc[:-1], candles.iloc[-1]
    scanner = make_scanner(symbols=["BTCUSDT"], timeframes=["15m"])
    scanner._set_candles("BTCUSDT", "15m", snapshot)

    def klines(request: httpx.Request) -> httpx.Response:
        # The websocket delivers a newly closed candle after this pair was fetched
        # but before the REST snapshot is applied.
        scanner._upsert_candle("BTCUSDT", "15m", closed_mid_refresh.to_dict())
        return httpx.Response(200, json=to_klines(snapshot), request=request)

    mock_binance(monkeypatch, klines)

    asyncio.run(scanner._load_initial_candles())

    loaded = scanner._candles[("BTCUSDT", "15m")]
    assert len(loaded) == 320
    assert loaded.iloc[-1]["open_time"] == closed_mid_refresh["open_time"]


@pytest.mark.parametrize(
    ("spike_offset", "expected_state"),
    [(5, "tp1"), (20, "expired")],
)
def test_status_only_tracks_the_signal_window(spike_offset, expected_state):
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    candles = flat_candles(320, start)
    signal_index = 250
    candles.loc[signal_index + spike_offset, "high"] = 125.0
    scanner = make_scanner()
    scanner._set_candles("BTCUSDT", "15m", candles)
    signal = make_signal(candles.iloc[signal_index]["close_time"].to_pydatetime(), PLAN)

    status = scanner.enrich_signal_context(signal)["status"]

    assert status["state"] == expected_state


def test_signal_older_than_loaded_candles_is_not_tracked():
    start = datetime(2026, 1, 10, tzinfo=timezone.utc)
    candles = flat_candles(320, start)
    # Without the window check this TP hit would be credited to the old signal.
    candles.loc[5, "high"] = 125.0
    scanner = make_scanner()
    scanner._set_candles("BTCUSDT", "15m", candles)
    signal = make_signal(start - timedelta(days=5), PLAN)

    status = scanner.enrich_signal_context(signal)["status"]

    assert status["state"] == "untracked"


@pytest.mark.parametrize(
    ("age", "expected_state"),
    [(timedelta(days=1), "untracked"), (timedelta(minutes=30), "active")],
)
def test_signal_without_candles_is_untracked_after_tracking_window(age, expected_state):
    # e.g. the symbol dropped out of the auto watchlist, so no candles are loaded.
    signal = make_signal(datetime.now(timezone.utc) - age, PLAN)

    status = make_scanner().enrich_signal_context(signal)["status"]

    assert status["state"] == expected_state


def test_signal_on_first_loaded_candle_is_still_tracked():
    start = datetime(2026, 1, 10, tzinfo=timezone.utc)
    candles = flat_candles(320, start)
    scanner = make_scanner()
    scanner._set_candles("BTCUSDT", "15m", candles)
    signal = make_signal(candles.iloc[0]["close_time"].to_pydatetime(), PLAN)

    status = scanner.enrich_signal_context(signal)["status"]

    assert status["state"] == "expired"


def test_final_status_is_saved_and_frozen(tmp_path):
    store = SignalStore(limit=100, db_path=str(tmp_path / "signals.db"))
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    candles = flat_candles(320, start)
    candles.loc[255, "high"] = 125.0
    signal = make_signal(candles.iloc[250]["close_time"].to_pydatetime(), PLAN)
    store.add_if_new(signal)
    scanner = make_scanner(store=store)
    scanner._set_candles("BTCUSDT", "15m", candles)

    scanner.enrich_signal_context(store.list()[0])

    saved = store.list()[0]
    assert saved.status["state"] == "tp1"
    # A fresh scanner with no candles (e.g. after a restart) keeps the saved outcome.
    assert make_scanner(store=store).enrich_signal_context(saved)["status"]["state"] == "tp1"


def observe_bias(scanner: FuturesScanner, label: str, minutes_later: int = 0) -> None:
    if minutes_later and scanner._bias_candidate:
        candidate, since = scanner._bias_candidate
        scanner._bias_candidate = (candidate, since - timedelta(minutes=minutes_later))
    overview = {"bias_label": label, "timeframe": "15m", "movers": [{"symbol": "BTCUSDT", "change_pct": 1.0}]}
    scanner._market_overview = lambda: overview
    scanner._maybe_send_market_bias_alert()


def test_bias_reversal_is_announced_after_confirmation():
    alerter = CaptureAlerter()
    scanner = make_scanner(alerter=alerter, timeframes=["15m"])

    observe_bias(scanner, "Neutral")
    observe_bias(scanner, "Long Bias")
    assert alerter.messages == []
    observe_bias(scanner, "Long Bias", minutes_later=16)
    # Previously a reversal inside the 30 minute cooldown was never announced.
    observe_bias(scanner, "Neutral", minutes_later=5)
    observe_bias(scanner, "Neutral", minutes_later=16)

    assert len(alerter.messages) == 2
    assert "Neutral -> \U0001F7E2 Long Bias" in alerter.messages[0]
    assert "Long Bias -> \U0001F7E1 Neutral" in alerter.messages[1]
    assert "system alert" not in alerter.messages[1]


def test_short_bias_flip_is_not_announced():
    alerter = CaptureAlerter()
    scanner = make_scanner(alerter=alerter, timeframes=["15m"])

    observe_bias(scanner, "Neutral")
    observe_bias(scanner, "Long Bias")
    observe_bias(scanner, "Neutral", minutes_later=5)
    observe_bias(scanner, "Long Bias", minutes_later=20)
    observe_bias(scanner, "Long Bias", minutes_later=5)

    assert alerter.messages == []


def test_recovery_is_sent_once_and_only_after_an_alert():
    alerter = CaptureAlerter()
    scanner = make_scanner(alerter=alerter)

    scanner._resolve_incident("websocket", "Binance websocket reconnected.")
    scanner._send_system_alert("websocket:connection-failed", "Websocket connection failed.")
    scanner._resolve_incident("websocket", "Binance websocket reconnected.")
    scanner._resolve_incident("websocket", "Binance websocket reconnected.")

    assert len(alerter.messages) == 2
    assert alerter.messages[0].startswith("⚠️ CF Scanner system alert")
    assert alerter.messages[1] == "✅ CF Scanner recovered\nBinance websocket reconnected."


def test_only_telegram_timeframes_are_sent_to_telegram():
    alerter = CaptureAlerter()
    scanner = make_scanner(alerter=alerter)
    signal = make_signal(datetime(2026, 1, 1, tzinfo=timezone.utc), PLAN)

    scanner._alert_signal(signal.model_copy(update={"timeframe": "5m"}))
    scanner._alert_signal(signal.model_copy(update={"timeframe": "15m"}))

    assert len(alerter.messages) == 1
    assert "· 15m" in alerter.messages[0]


def test_telegram_timeframes_can_be_set_from_env(monkeypatch):
    monkeypatch.setenv("TELEGRAM_TIMEFRAMES", "5m, 1h")

    assert Settings(_env_file=None).telegram_timeframes == ["5m", "1h"]
