"""SQLite database operations for aicontext."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone

from aicontext.records import ActivityRecord

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS activity (
    id        INTEGER PRIMARY KEY,
    timestamp TEXT NOT NULL,
    source    TEXT NOT NULL,
    service   TEXT NOT NULL,
    action    TEXT NOT NULL,
    title     TEXT NOT NULL,
    extra     TEXT,
    ref_type  TEXT,
    ref_id    TEXT
);

CREATE INDEX IF NOT EXISTS idx_timestamp ON activity(timestamp);
CREATE INDEX IF NOT EXISTS idx_source ON activity(source);
CREATE INDEX IF NOT EXISTS idx_service ON activity(service);
CREATE INDEX IF NOT EXISTS idx_source_svc ON activity(source, service);
CREATE INDEX IF NOT EXISTS idx_service_ts ON activity(service, timestamp);
CREATE INDEX IF NOT EXISTS idx_action ON activity(action);
CREATE INDEX IF NOT EXISTS idx_ref ON activity(ref_type, ref_id);

CREATE TABLE IF NOT EXISTS metadata (
    key   TEXT PRIMARY KEY,
    value TEXT
);
"""


def create_database(db_path: str) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(_SCHEMA_SQL)
        now = datetime.now(timezone.utc).isoformat()
        conn.execute("INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)", ("schema_version", "1"))
        conn.execute("INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)", ("created_timestamp", now))
        conn.commit()
    finally:
        conn.close()


def _extra_to_json(extra: dict | None) -> str | None:
    if extra is None:
        return None
    return json.dumps(extra, ensure_ascii=False)


def insert_records(db_path: str, records: list[ActivityRecord]) -> int:
    if not records:
        return 0
    rows = [
        (r.timestamp, r.source, r.service, r.action, r.title,
         _extra_to_json(r.extra), r.ref_type, r.ref_id)
        for r in records
    ]
    conn = sqlite3.connect(db_path)
    try:
        conn.executemany(
            "INSERT INTO activity (timestamp, source, service, action, title, extra, ref_type, ref_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        conn.commit()
        return len(rows)
    finally:
        conn.close()


def update_record(db_path: str, record_id: int, record: ActivityRecord) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "UPDATE activity SET timestamp=?, source=?, service=?, action=?, title=?, "
            "extra=?, ref_type=?, ref_id=? WHERE id=?",
            (record.timestamp, record.source, record.service, record.action, record.title,
             _extra_to_json(record.extra), record.ref_type, record.ref_id, record_id),
        )
        conn.commit()
    finally:
        conn.close()


def load_all_records(db_path: str) -> list[tuple[int, ActivityRecord]]:
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.execute(
            "SELECT id, timestamp, source, service, action, title, extra, ref_type, ref_id FROM activity"
        )
        results = []
        for row in cursor:
            extra = json.loads(row[6]) if row[6] else None
            rec = ActivityRecord(
                timestamp=row[1], source=row[2], service=row[3], action=row[4],
                title=row[5], extra=extra, ref_type=row[7], ref_id=row[8],
            )
            results.append((row[0], rec))
        return results
    finally:
        conn.close()


def get_record_count(db_path: str) -> int:
    conn = sqlite3.connect(db_path)
    try:
        return conn.execute("SELECT COUNT(*) FROM activity").fetchone()[0]
    finally:
        conn.close()
