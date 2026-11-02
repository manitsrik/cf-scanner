import asyncio
import logging
import re
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import httpx

from app.formatting import format_price
from app.models import Signal

logger = logging.getLogger(__name__)

DEFAULT_DASHBOARD_URL = "https://cf-scanner-production.up.railway.app/"
# Thailand has no daylight saving time, so a fixed offset avoids needing tzdata.
BANGKOK_TZ = timezone(timedelta(hours=7), "ICT")
QUEUE_LIMIT = 200
MAX_ATTEMPTS = 4
MAX_RETRY_AFTER_SECONDS = 60
DRAIN_TIMEOUT_SECONDS = 5
BOT_TOKEN_IN_URL = re.compile(r"(api\.telegram\.org/bot)[^/\s]+")


class _RedactBotToken(logging.Filter):
    """httpx logs every request URL at INFO level, and Telegram URLs carry the bot token."""

    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        if "api.telegram.org/bot" in message:
            record.msg = BOT_TOKEN_IN_URL.sub(r"\1<redacted>", message)
            record.args = ()
        return True


logging.getLogger("httpx").addFilter(_RedactBotToken())


@dataclass
class _Attempt:
    sent: bool
    error: str | None = None
    # Seconds to wait before retrying; None means the error is permanent.
    retry_after: float | None = None


class TelegramAlerter:
    """Sends Telegram messages from a background queue.

    Alerts are queued with ``notify`` so a slow or rate-limited Telegram API never
    blocks candle processing, and failed sends are retried before being dropped.
    """

    def __init__(
        self,
        bot_token: str | None,
        chat_id: str | None,
        dashboard_url: str | None = None,
        send_interval_seconds: float = 1.0,
        retry_base_seconds: float = 2.0,
    ) -> None:
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.dashboard_url = dashboard_url or DEFAULT_DASHBOARD_URL
        # Telegram allows roughly one message per second per chat.
        self.send_interval_seconds = send_interval_seconds
        self.retry_base_seconds = retry_base_seconds
        self._queue: asyncio.Queue[str] = asyncio.Queue(maxsize=QUEUE_LIMIT)
        self._worker: asyncio.Task | None = None
        self._last_sent_at: datetime | None = None
        self._last_error: str | None = None
        self._last_error_at: datetime | None = None
        self._failed_count = 0

    @property
    def enabled(self) -> bool:
        return bool(self.bot_token and self.chat_id)

    def start(self) -> None:
        if self.enabled and (self._worker is None or self._worker.done()):
            self._worker = asyncio.create_task(self._run())

    async def stop(self) -> None:
        if self._worker is None:
            return
        # Give queued alerts a moment to go out before shutdown (e.g. a redeploy).
        with suppress(TimeoutError):
            await asyncio.wait_for(self._queue.join(), timeout=DRAIN_TIMEOUT_SECONDS)
        self._worker.cancel()
        with suppress(asyncio.CancelledError):
            await self._worker
        self._worker = None

    def status(self) -> dict:
        return {
            "enabled": self.enabled,
            "queued": self._queue.qsize(),
            "failed_count": self._failed_count,
            "last_sent_at": self._last_sent_at.isoformat() if self._last_sent_at else None,
            "last_error": self._last_error,
            "last_error_at": self._last_error_at.isoformat() if self._last_error_at else None,
        }

    def notify(self, text: str) -> None:
        """Queue a message for background delivery."""
        if not self.enabled:
            return
        try:
            self._queue.put_nowait(text)
        except asyncio.QueueFull:
            self._record_failure("Telegram queue is full; dropped an alert.")

    def notify_signal(self, signal: Signal, candle_closed_at: datetime | None = None) -> None:
        self.notify(self.format_signal(signal, candle_closed_at))

    async def send_text(self, text: str) -> bool:
        """Send one message immediately without retries, e.g. for the dashboard test button."""
        if not self.enabled:
            logger.info("Telegram alert skipped because TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID is missing.")
            return False
        async with httpx.AsyncClient(timeout=10) as client:
            attempt = await self._attempt(client, text)
        self._record(attempt)
        return attempt.sent

    @staticmethod
    def format_signal(signal: Signal, candle_closed_at: datetime | None = None) -> str:
        marker = "\U0001F7E2" if signal.signal_type == "LONG" else "\U0001F534"
        volume_ratio = float((signal.indicators or {}).get("volume_ratio") or 0)
        plan = signal.trade_plan or {}
        summary = signal.trader_summary or "รอข้อมูลเพิ่มเติมก่อนสรุปแผนเทรด"
        quality = (
            f"{signal.quality_label} {signal.quality_score:.0f}"
            if signal.quality_label and signal.quality_score is not None
            else "N/A"
        )
        lines = [f"{marker} {signal.signal_type} {signal.symbol} · {signal.timeframe}"]
        if candle_closed_at is not None:
            if candle_closed_at.tzinfo is None:
                candle_closed_at = candle_closed_at.replace(tzinfo=timezone.utc)
            # Close times end 1 ms before the next candle, so round up to the minute.
            closed = (candle_closed_at + timedelta(seconds=1)).astimezone(BANGKOK_TZ)
            lines.append(f"แท่งปิด {closed:%d/%m %H:%M} (เวลาไทย) · Quality: {quality}")
        else:
            lines.append(f"Quality: {quality}")
        lines.append(f"Price {format_price(signal.price)} · RSI {signal.rsi:.1f} · Vol {volume_ratio:.2f}x")

        if plan:
            risk_pct = plan.get("risk_pct")
            risk_text = f" (-{float(risk_pct):.2f}%)" if risk_pct is not None else ""
            lines += [
                "",
                f"Entry {format_price(plan.get('entry_zone_low'))} – {format_price(plan.get('entry_zone_high'))}",
                f"SL {format_price(plan.get('stop_loss'))}{risk_text}",
                f"TP1 {format_price(plan.get('take_profit_1'))} · TP2 {format_price(plan.get('take_profit_2'))}",
            ]

        lines += ["", summary, "", f"Chart: {signal.tradingview_url}"]
        return "\n".join(lines)

    async def _run(self) -> None:
        async with httpx.AsyncClient(timeout=10) as client:
            while True:
                text = await self._queue.get()
                try:
                    await self._deliver(client, text)
                except Exception as exc:
                    # Keep the worker alive whatever happens to a single message.
                    self._record_failure(f"Unexpected Telegram error: {type(exc).__name__}")
                finally:
                    self._queue.task_done()
                await asyncio.sleep(self.send_interval_seconds)

    async def _deliver(self, client: httpx.AsyncClient, text: str) -> bool:
        for attempt_number in range(1, MAX_ATTEMPTS + 1):
            attempt = await self._attempt(client, text)
            if attempt.sent or attempt.retry_after is None or attempt_number == MAX_ATTEMPTS:
                self._record(attempt)
                return attempt.sent
            logger.warning("%s Retrying in %.0f seconds.", attempt.error, attempt.retry_after)
            await asyncio.sleep(attempt.retry_after)
        return False

    async def _attempt(self, client: httpx.AsyncClient, text: str) -> _Attempt:
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": self._with_dashboard(text),
            "link_preview_options": {"is_disabled": True},
        }
        # httpx error messages embed the request URL, which contains the bot token,
        # so never log or store the raw exception.
        try:
            response = await client.post(url, json=payload)
        except httpx.HTTPError as exc:
            error = f"Telegram request failed: {type(exc).__name__} {self._redact(str(exc))}".strip()
            return _Attempt(sent=False, error=error, retry_after=self.retry_base_seconds)

        if response.is_success:
            return _Attempt(sent=True)

        description = self._redact(self._description(response))
        error = f"Telegram API returned HTTP {response.status_code}: {description}"
        if response.status_code == 429:
            return _Attempt(sent=False, error=error, retry_after=self._retry_after(response))
        if response.status_code >= 500:
            return _Attempt(sent=False, error=error, retry_after=self.retry_base_seconds)
        # Other 4xx errors (bad token, unknown chat, bot blocked) will not fix themselves.
        return _Attempt(sent=False, error=error)

    def _record(self, attempt: _Attempt) -> None:
        if attempt.sent:
            self._last_sent_at = datetime.now(timezone.utc)
            self._last_error = None
        else:
            self._record_failure(attempt.error or "Telegram send failed.")

    def _record_failure(self, error: str) -> None:
        logger.error("Failed to send Telegram alert: %s", error)
        self._failed_count += 1
        self._last_error = error
        self._last_error_at = datetime.now(timezone.utc)

    def _with_dashboard(self, text: str) -> str:
        if self.dashboard_url and self.dashboard_url not in text:
            return f"{text}\n\nDashboard: {self.dashboard_url}"
        return text

    @staticmethod
    def _description(response: httpx.Response) -> str:
        try:
            return str(response.json().get("description") or response.reason_phrase)
        except ValueError:
            return response.text[:200] or response.reason_phrase

    def _retry_after(self, response: httpx.Response) -> float:
        try:
            seconds = float(response.json()["parameters"]["retry_after"])
        except (ValueError, KeyError, TypeError):
            seconds = self.retry_base_seconds
        return min(max(seconds, 0), MAX_RETRY_AFTER_SECONDS)

    def _redact(self, value: str) -> str:
        return value.replace(self.bot_token, "<redacted>") if self.bot_token else value
