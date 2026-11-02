import asyncio
import json
import logging
from datetime import datetime, timezone

import httpx

from app.models import Signal
from app.telegram import MAX_ATTEMPTS, TelegramAlerter

TOKEN = "123456:SECRET_BOT_TOKEN"


def mock_telegram(monkeypatch, handler) -> list[dict]:
    payloads = []

    def recording_handler(request: httpx.Request) -> httpx.Response:
        payloads.append(json.loads(request.content))
        return handler(request, len(payloads))

    real_client = httpx.AsyncClient

    def mock_client(*args, **kwargs):
        return real_client(*args, transport=httpx.MockTransport(recording_handler), **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", mock_client)
    return payloads


def fast_alerter() -> TelegramAlerter:
    return TelegramAlerter(TOKEN, "42", send_interval_seconds=0, retry_base_seconds=0)


def deliver(alerter: TelegramAlerter, *messages: str) -> None:
    async def scenario():
        alerter.start()
        for message in messages:
            alerter.notify(message)
        await alerter.stop()

    asyncio.run(scenario())


def test_failed_send_does_not_log_the_bot_token(monkeypatch, caplog):
    def bad_request(request, _):
        return httpx.Response(400, json={"ok": False, "description": "Bad Request: chat not found"}, request=request)

    mock_telegram(monkeypatch, bad_request)
    alerter = TelegramAlerter(TOKEN, "42")

    with caplog.at_level(logging.ERROR, logger="app.telegram"):
        sent = asyncio.run(alerter.send_text("hello"))

    assert sent is False
    assert "HTTP 400" in caplog.text
    assert "chat not found" in caplog.text
    assert "SECRET_BOT_TOKEN" not in caplog.text


def test_httpx_request_log_does_not_contain_the_bot_token(monkeypatch, caplog):
    mock_telegram(monkeypatch, lambda request, _: httpx.Response(200, json={"ok": True}, request=request))
    alerter = TelegramAlerter(TOKEN, "42")

    with caplog.at_level(logging.INFO, logger="httpx"):
        asyncio.run(alerter.send_text("hello"))

    assert "api.telegram.org/bot<redacted>/sendMessage" in caplog.text
    assert "SECRET_BOT_TOKEN" not in caplog.text


def test_queued_alert_is_retried_after_rate_limit(monkeypatch):
    def rate_limited_once(request, call):
        if call == 1:
            return httpx.Response(429, json={"ok": False, "parameters": {"retry_after": 0}}, request=request)
        return httpx.Response(200, json={"ok": True}, request=request)

    payloads = mock_telegram(monkeypatch, rate_limited_once)
    alerter = fast_alerter()

    deliver(alerter, "signal")

    assert len(payloads) == 2
    status = alerter.status()
    assert status["last_error"] is None
    assert status["failed_count"] == 0
    assert status["last_sent_at"] is not None


def test_permanent_error_is_not_retried_and_is_reported(monkeypatch):
    def chat_not_found(request, _):
        return httpx.Response(400, json={"ok": False, "description": "Bad Request: chat not found"}, request=request)

    payloads = mock_telegram(monkeypatch, chat_not_found)
    alerter = fast_alerter()

    deliver(alerter, "signal")

    assert len(payloads) == 1
    status = alerter.status()
    assert status["failed_count"] == 1
    assert "chat not found" in status["last_error"]


def test_network_errors_are_retried_then_reported_without_token(monkeypatch):
    def unreachable(request, _):
        raise httpx.ConnectError(f"cannot reach {request.url}", request=request)

    payloads = mock_telegram(monkeypatch, unreachable)
    alerter = fast_alerter()

    deliver(alerter, "signal")

    assert len(payloads) == MAX_ATTEMPTS
    status = alerter.status()
    assert status["failed_count"] == 1
    assert "ConnectError" in status["last_error"]
    assert "SECRET_BOT_TOKEN" not in status["last_error"]


def test_signal_message_format(monkeypatch):
    payloads = mock_telegram(monkeypatch, lambda request, _: httpx.Response(200, json={"ok": True}, request=request))
    alerter = fast_alerter()
    signal = Signal(
        id="AKEUSDT:15m:1793603699:LONG",
        symbol="AKEUSDT",
        timeframe="15m",
        signal_type="LONG",
        price=0.01283,
        rsi=58.61,
        volume=150,
        volume_average_20=100,
        volume_status="1.50x avg20",
        reasons=[],
        indicators={"volume_ratio": 1.5},
        quality_score=62,
        quality_label="Caution",
        trade_plan={
            "entry_zone_low": 0.0128043,
            "entry_zone_high": 0.0128557,
            "stop_loss": 0.012521,
            "take_profit_1": 0.0132935,
            "take_profit_2": 0.0135716,
            "risk_pct": 2.41,
        },
        status={"state": "active", "label": "Active"},
        trader_summary="สรุปแบบเทรดเดอร์: ทดสอบ",
        created_at=datetime(2026, 11, 2, 7, 15, tzinfo=timezone.utc),
        tradingview_url="https://www.tradingview.com/chart/?symbol=BINANCE:AKEUSDT.P",
    )

    async def scenario():
        alerter.start()
        # Binance close time for the 14:00-14:15 Bangkok candle.
        alerter.notify_signal(signal, datetime(2026, 11, 2, 7, 14, 59, 999000, tzinfo=timezone.utc))
        await alerter.stop()

    asyncio.run(scenario())

    payload = payloads[0]
    text = payload["text"]
    assert payload["link_preview_options"] == {"is_disabled": True}
    assert text.splitlines()[0] == "\U0001F7E2 LONG AKEUSDT · 15m"
    assert "แท่งปิด 02/11 14:15 (เวลาไทย)" in text
    assert "Price 0.01283" in text
    assert "SL 0.012521 (-2.41%)" in text
    assert "Status:" not in text
    # The trade plan comes before the long Thai summary.
    assert text.index("Entry 0.0128043") < text.index("สรุปแบบเทรดเดอร์")
    assert "Chart: https://www.tradingview.com/" in text
    assert text.rstrip().endswith("Dashboard: https://cf-scanner-production.up.railway.app/")


def test_disabled_alerter_ignores_notifications():
    alerter = TelegramAlerter(None, None)

    alerter.notify("hello")

    assert alerter.status()["queued"] == 0
