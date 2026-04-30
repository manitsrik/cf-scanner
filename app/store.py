from collections import deque
from threading import Lock

from app.models import Signal


class SignalStore:
    def __init__(self, limit: int = 100) -> None:
        self._signals: deque[Signal] = deque(maxlen=limit)
        self._seen: set[str] = set()
        self._lock = Lock()

    def add_if_new(self, signal: Signal) -> bool:
        with self._lock:
            if signal.id in self._seen:
                return False
            self._seen.add(signal.id)
            self._signals.appendleft(signal)
            return True

    def list(self) -> list[Signal]:
        with self._lock:
            return list(self._signals)

