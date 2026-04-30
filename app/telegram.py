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

    async def send_signal(self, signal: Signal) -> None:
        if not self.enabled:
            logger.info("Telegram alert skipped because TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID is missing.")
            return

        text = (
            f"{signal.signal_type} signal: {signal.symbol} {signal.timeframe}\n"
            f"Price: {signal.price:.4f}\n"
            f"RSI 14: {signal.rsi:.2f}\n"
            f"Volume: {signal.volume:.2f} > avg20 {signal.volume_average_20:.2f}\n"
            f"{signal.tradingview_url}"
        )
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"

        try:
            async with httpx.AsyncClient(timeout=10) as client:
                response = await client.post(url, json={"chat_id": self.chat_id, "text": text})
                response.raise_for_status()
        except httpx.HTTPError:
            logger.exception("Failed to send Telegram alert.")

