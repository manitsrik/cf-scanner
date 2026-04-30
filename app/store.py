from collections import deque
from datetime import datetime, timedelta, timezone
from threading import Lock

from app.models import Signal


class SignalStore:
    def __init__(self, limit: int = 100) -> None:
        self._signals: deque[Signal] = deque(maxlen=limit)
        self._seen: set[str] = set()
        self._last_signal_at: dict[tuple[str, str, str], datetime] = {}
        self._lock = Lock()

    def add_if_new(self, signal: Signal, cooldown_minutes: int = 0) -> bool:
        with self._lock:
            if signal.id in self._seen:
                return False

            key = (signal.symbol, signal.timeframe, signal.signal_type)
            now = datetime.now(timezone.utc)
            last_signal_at = self._last_signal_at.get(key)
            if cooldown_minutes > 0 and last_signal_at:
                if now - last_signal_at < timedelta(minutes=cooldown_minutes):
                    return False

            self._seen.add(signal.id)
            self._last_signal_at[key] = now
            self._signals.appendleft(signal)
            return True

    def list(self) -> list[Signal]:
        with self._lock:
            return list(self._signals)
