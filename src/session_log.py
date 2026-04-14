"""Local SQLite session log for Genie API calls.

Records one row per question asked through GenieClient. Used by the
cost-attribution layer to map time windows back to billing data.
"""

from __future__ import annotations

import os
import sqlite3
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Optional

DEFAULT_DB_PATH = Path(os.environ.get("GENIE_SESSION_DB", ".genie_session.sqlite"))

TABLE_DDL = """
CREATE TABLE IF NOT EXISTS calls (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_start_utc REAL NOT NULL,
    ts_end_utc REAL NOT NULL,
    latency_s REAL NOT NULL,
    space_id TEXT NOT NULL,
    warehouse_id TEXT,
    conversation_id TEXT,
    message_id TEXT,
    question TEXT NOT NULL,
    status TEXT NOT NULL,
    row_count INTEGER NOT NULL DEFAULT 0,
    error TEXT,
    statement_id TEXT
);
CREATE INDEX IF NOT EXISTS idx_calls_ts ON calls(ts_start_utc);
"""

MIGRATIONS = [
    "ALTER TABLE calls ADD COLUMN statement_id TEXT",
]

POST_MIGRATION_DDL = [
    "CREATE INDEX IF NOT EXISTS idx_calls_stmt ON calls(statement_id)",
]


@dataclass
class CallRecord:
    ts_start_utc: float
    ts_end_utc: float
    latency_s: float
    space_id: str
    warehouse_id: Optional[str]
    conversation_id: Optional[str]
    message_id: Optional[str]
    question: str
    status: str
    row_count: int
    error: Optional[str] = None
    statement_id: Optional[str] = None


class SessionLog:
    def __init__(self, db_path: Path | str = DEFAULT_DB_PATH):
        self.db_path = Path(db_path)
        self._init()

    def _init(self) -> None:
        with self._conn() as c:
            c.executescript(TABLE_DDL)
            for stmt in MIGRATIONS:
                try:
                    c.execute(stmt)
                except sqlite3.OperationalError:
                    pass  # already applied
            for stmt in POST_MIGRATION_DDL:
                c.execute(stmt)

    @contextmanager
    def _conn(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.db_path)
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def record(self, rec: CallRecord) -> int:
        with self._conn() as c:
            cur = c.execute(
                """INSERT INTO calls(ts_start_utc, ts_end_utc, latency_s, space_id,
                   warehouse_id, conversation_id, message_id, question, status,
                   row_count, error, statement_id)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    rec.ts_start_utc, rec.ts_end_utc, rec.latency_s, rec.space_id,
                    rec.warehouse_id, rec.conversation_id, rec.message_id,
                    rec.question, rec.status, rec.row_count, rec.error,
                    rec.statement_id,
                ),
            )
            return int(cur.lastrowid or 0)

    def recent(self, limit: int = 20) -> list[dict]:
        with self._conn() as c:
            c.row_factory = sqlite3.Row
            rows = c.execute(
                "SELECT * FROM calls ORDER BY ts_start_utc DESC LIMIT ?", (limit,)
            ).fetchall()
            return [dict(r) for r in rows]

    def summary(self, since_utc: Optional[float] = None) -> dict:
        since = since_utc or 0.0
        with self._conn() as c:
            row = c.execute(
                """SELECT COUNT(*) AS n, COALESCE(SUM(latency_s),0) AS total_latency_s,
                          COALESCE(AVG(latency_s),0) AS avg_latency_s,
                          COALESCE(SUM(row_count),0) AS total_rows,
                          SUM(CASE WHEN status LIKE '%COMPLETED%' THEN 1 ELSE 0 END) AS completed,
                          SUM(CASE WHEN error IS NOT NULL THEN 1 ELSE 0 END) AS errors
                   FROM calls WHERE ts_start_utc >= ?""",
                (since,),
            ).fetchone()
            return dict(
                zip(
                    ("n", "total_latency_s", "avg_latency_s", "total_rows", "completed", "errors"),
                    row,
                )
            )

    def statement_ids(self, since_utc: Optional[float] = None) -> list[str]:
        since = since_utc or 0.0
        with self._conn() as c:
            rows = c.execute(
                """SELECT statement_id FROM calls
                   WHERE ts_start_utc >= ? AND statement_id IS NOT NULL""",
                (since,),
            ).fetchall()
            return [r[0] for r in rows if r[0]]

    def time_windows(self, since_utc: Optional[float] = None) -> list[tuple[float, float]]:
        """Return (start, end) epoch pairs for every completed call."""
        since = since_utc or 0.0
        with self._conn() as c:
            rows = c.execute(
                """SELECT ts_start_utc, ts_end_utc FROM calls
                   WHERE ts_start_utc >= ? AND status LIKE '%COMPLETED%'""",
                (since,),
            ).fetchall()
            return [(float(a), float(b)) for a, b in rows]


def now_utc() -> float:
    return time.time()
