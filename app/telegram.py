import logging

import httpx

from app.models import Signal

logger = logging.getLogger(__name__)


class TelegramAlerter:
    def __init__(self, bot_token: str | None, chat_id: str | None) -> None:
        self.bot_token = bot_token
        self.chat_id = chat_id

    @property
    def enabled(self) -> bool:
        return bool(self.bot_token and self.chat_id)

    async def send_signal(self, signal: Signal) -> bool:
        direction = "Bullish" if signal.signal_type == "LONG" else "Bearish"
        marker = "\U0001F7E2" if signal.signal_type == "LONG" else "\U0001F534"
        volume_ratio = signal.indicators.get("volume_ratio", 0)
        plan = signal.trade_plan or {}
        summary = signal.trader_summary or "รอข้อมูลเพิ่มเติมก่อนสรุปแผนเทรด"
        quality = (
            f"{signal.quality_label} {signal.quality_score:.0f}"
            if signal.quality_label and signal.quality_score is not None
            else "N/A"
        )
        status = (signal.status or {}).get("label") or "Active"
        text = (
            f"{marker} {direction} {signal.signal_type} signal\n"
            f"{signal.symbol} | {signal.timeframe}\n"
            f"Price: {signal.price:.4f}\n"
            f"RSI 14: {signal.rsi:.2f}\n"
            f"Volume: {volume_ratio:.2f}x avg20\n"
            f"Quality: {quality}\n"
            f"Status: {status}\n\n"
            f"{summary}\n\n"
            "Trade plan:\n"
            f"- Entry: {self._price(plan.get('entry_zone_low'))} - {self._price(plan.get('entry_zone_high'))}\n"
            f"- SL: {self._price(plan.get('stop_loss'))} ({self._pct(plan.get('risk_pct'))} risk)\n"
            f"- TP1: {self._price(plan.get('take_profit_1'))} | TP2: {self._price(plan.get('take_profit_2'))}\n\n"
            "Checks:\n"
            + "\n".join(f"- {reason}" for reason in signal.reasons[:3])
            + "\n\n"
            f"{signal.tradingview_url}"
        )
        return await self.send_text(text)

    @staticmethod
    def _price(value: object) -> str:
        try:
            return f"{float(value):.6g}"
        except (TypeError, ValueError):
            return "-"

    @staticmethod
    def _pct(value: object) -> str:
        try:
            return f"{float(value):.2f}%"
        except (TypeError, ValueError):
            return "-"

    async def send_text(self, text: str) -> bool:
        if not self.enabled:
            logger.info("Telegram alert skipped because TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID is missing.")
            return False

        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"

        try:
            async with httpx.AsyncClient(timeout=10) as client:
                response = await client.post(url, json={"chat_id": self.chat_id, "text": text})
                response.raise_for_status()
                return True
        except httpx.HTTPError:
            logger.exception("Failed to send Telegram alert.")
            return False
