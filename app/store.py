from collections import deque
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sqlite3
from threading import Lock

from app.models import Signal


class SignalStore:
    def __init__(self, limit: int = 100, db_path: str | None = None) -> None:
        self.limit = limit
        self.db_path = Path(db_path) if db_path else None
        self._signals: deque[Signal] = deque(maxlen=limit)
        self._seen: set[str] = set()
        self._last_signal_at: dict[tuple[str, str, str], datetime] = {}
        self._lock = Lock()
        self._initialize_db()
        self._load_saved_signals()

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
            self._save_signal(signal)
            return True

    def list(self) -> list[Signal]:
        with self._lock:
            return list(self._signals)

    def _initialize_db(self) -> None:
        if not self.db_path:
            return

        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with closing(sqlite3.connect(self.db_path)) as connection:
            with connection:
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS signals (
                        id TEXT PRIMARY KEY,
                        symbol TEXT NOT NULL,
                        timeframe TEXT NOT NULL,
                        signal_type TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        payload TEXT NOT NULL
                    )
                    """
                )
                connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_signals_created_at
                    ON signals (created_at DESC)
                    """
                )

    def _load_saved_signals(self) -> None:
        if not self.db_path or not self.db_path.exists():
            return

        with closing(sqlite3.connect(self.db_path)) as connection:
            rows = connection.execute(
                """
                SELECT payload
                FROM signals
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (self.limit,),
            ).fetchall()

        for (payload,) in rows:
            signal = Signal.model_validate_json(payload)
            self._signals.append(signal)
            self._seen.add(signal.id)

            key = (signal.symbol, signal.timeframe, signal.signal_type)
            previous = self._last_signal_at.get(key)
            signal_time = signal.created_at
            if signal_time.tzinfo is None:
                signal_time = signal_time.replace(tzinfo=timezone.utc)
            if previous is None or signal_time > previous:
                self._last_signal_at[key] = signal_time

    def _save_signal(self, signal: Signal) -> None:
        if not self.db_path:
            return

        with closing(sqlite3.connect(self.db_path)) as connection:
            with connection:
                connection.execute(
                    """
                    INSERT OR IGNORE INTO signals (id, symbol, timeframe, signal_type, created_at, payload)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        signal.id,
                        signal.symbol,
                        signal.timeframe,
                        signal.signal_type,
                        signal.created_at.isoformat(),
                        signal.model_dump_json(),
                    ),
                )
