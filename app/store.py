from __future__ import annotations

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
            self._trim_saved_signals()
            return True

    def list(self) -> list[Signal]:
        if self.db_path:
            return self._list_saved_signals()

        with self._lock:
            return list(self._signals)

    def count(self) -> int:
        if not self.db_path or not self.db_path.exists():
            with self._lock:
                return len(self._signals)

        with closing(sqlite3.connect(self.db_path)) as connection:
            row = connection.execute("SELECT COUNT(*) FROM signals").fetchone()
        return int(row[0] if row else 0)

    def latest_created_at(self) -> datetime | None:
        latest = self.list()[:1]
        return latest[0].created_at if latest else None

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

        for signal in self._list_saved_signals():
            self._signals.append(signal)
            self._seen.add(signal.id)
            self._remember_last_signal(signal)

    def _list_saved_signals(self) -> list[Signal]:
        if not self.db_path or not self.db_path.exists():
            return []

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

        return [Signal.model_validate_json(payload) for (payload,) in rows]

    def _remember_last_signal(self, signal: Signal) -> None:
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

    def _trim_saved_signals(self) -> None:
        if not self.db_path:
            return

        with closing(sqlite3.connect(self.db_path)) as connection:
            with connection:
                connection.execute(
                    """
                    DELETE FROM signals
                    WHERE id NOT IN (
                        SELECT id
                        FROM signals
                        ORDER BY created_at DESC
                        LIMIT ?
                    )
                    """,
                    (self.limit,),
                )
