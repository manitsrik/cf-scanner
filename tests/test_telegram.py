import asyncio
import logging

import httpx

from app.telegram import TelegramAlerter

TOKEN = "123456:SECRET_BOT_TOKEN"


def test_failed_send_does_not_log_the_bot_token(monkeypatch, caplog):
    def bad_request(request: httpx.Request) -> httpx.Response:
        return httpx.Response(400, json={"ok": False, "description": "Bad Request: chat not found"}, request=request)

    real_client = httpx.AsyncClient

    def mock_client(*args, **kwargs):
        return real_client(*args, transport=httpx.MockTransport(bad_request), **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", mock_client)
    alerter = TelegramAlerter(TOKEN, "42")

    with caplog.at_level(logging.ERROR, logger="app.telegram"):
        sent = asyncio.run(alerter.send_text("hello"))

    assert sent is False
    assert "HTTP 400" in caplog.text
    assert "chat not found" in caplog.text
    assert "SECRET_BOT_TOKEN" not in caplog.text
