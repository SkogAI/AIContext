"""Skill merge: CRDT-based cross-device sync for aicontext skills.

Merges a remote skill directory into the local skill. Designed for P2P sync
where devices can be on/off and sync in any order. The merge is:
  - Commutative:  merge(A, B) = merge(B, A)
  - Associative:  merge(merge(A, B), C) = merge(A, merge(B, C))
  - Idempotent:   merge(A, A) = A

Data types and their CRDT semantics:
  - Activity records: Map CRDT keyed by dedup_key. Older timestamp wins.
  - Reference files:  Register CRDT keyed by path. Larger file wins, hash tiebreak.
  - Table data:       Map CRDT keyed by primary key. INSERT OR IGNORE.
"""

import json
import logging
import os
import shutil
import sqlite3
from dataclasses import dataclass, field

from aicontext.database import (
    insert_records, load_all_records, update_record,
)
from aicontext.dedup import (
    compute_default_dedup_key, content_hash_json, pick_older_record, records_equal,
)

logger = logging.getLogger(__name__)

# Tables that are part of the core schema (not source-provided).
_CORE_TABLES = frozenset({"activity", "metadata", "sqlite_sequence"})


@dataclass
class MergeResult:
    """Statistics from a skill merge operation."""
    activity_inserted: int = 0
    activity_updated: int = 0
    activity_skipped: int = 0
    activity_by_source: dict[str, dict[str, int]] = field(default_factory=dict)
    refs_copied: int = 0
    refs_updated: int = 0
    refs_skipped: int = 0
    refs_by_source: dict[str, dict[str, int]] = field(default_factory=dict)
    table_rows_merged: int = 0
    tables_merged: list[str] = field(default_factory=list)

    def _track(self, by_source: dict, source: str, action: str) -> None:
        entry = by_source.setdefault(source, {"inserted": 0, "updated": 0, "skipped": 0, "copied": 0})
        entry[action] += 1


def merge_skill(local_skill_dir: str, remote_skill_dir: str) -> MergeResult:
    """Merge a remote skill into the local skill. Convergent (CRDT).

    Order: reference files -> table data -> activity records.
    Reference files must be merged first so that local ref paths exist
    when activity records with ref_type="local" are inserted.
    """
    result = MergeResult()
    local_data = os.path.join(local_skill_dir, "data")
    remote_data = os.path.join(remote_skill_dir, "data")
    local_db = os.path.join(local_data, "activity.db")
    remote_db = os.path.join(remote_data, "activity.db")

    # Step 1: Reference files
    _merge_references(local_data, remote_data, result)

    # Step 2: Table data
    if os.path.exists(remote_db) and os.path.exists(local_db):
        _merge_tables(local_db, remote_db, result)

    # Step 3: Activity records
    if os.path.exists(remote_db) and os.path.exists(local_db):
        _merge_activity(local_db, remote_db, result)

    def _log_breakdown(label, by_source):
        if not by_source:
            return
        logger.info("  %s:", label)
        for source, counts in sorted(by_source.items()):
            parts = [f"{v} {k}" for k, v in counts.items() if v > 0]
            if parts:
                logger.info("    %s: %s", source, ", ".join(parts))

    logger.info("Merge complete")
    logger.info("  Activity: %d inserted, %d updated, %d skipped",
                result.activity_inserted, result.activity_updated, result.activity_skipped)
    _log_breakdown("Activity by source", result.activity_by_source)
    logger.info("  References: %d copied, %d updated, %d skipped",
                result.refs_copied, result.refs_updated, result.refs_skipped)
    _log_breakdown("References by source", result.refs_by_source)
    if result.tables_merged:
        logger.info("  Tables: %d rows across %s",
                    result.table_rows_merged, ", ".join(result.tables_merged))
    return result


def _load_meta(data_dir: str) -> dict:
    """Load reference_data/_meta.json."""
    meta_path = os.path.join(data_dir, "reference_data", "_meta.json")
    if os.path.exists(meta_path):
        try:
            with open(meta_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def _save_meta(data_dir: str, meta: dict) -> None:
    """Write reference_data/_meta.json."""
    meta_path = os.path.join(data_dir, "reference_data", "_meta.json")
    os.makedirs(os.path.dirname(meta_path), exist_ok=True)
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False)


def _ref_wins(remote_hash: str, remote_size: int,
              local_hash: str, local_size: int) -> bool:
    """Return True if the remote reference file should replace the local one.

    CRDT resolution: larger file wins (more data). Hash tiebreak.
    """
    if remote_size != local_size:
        return remote_size > local_size
    return remote_hash > local_hash


def _merge_references(local_data: str, remote_data: str, result: MergeResult) -> None:
    """Merge reference files from remote into local."""
    remote_ref_dir = os.path.join(remote_data, "reference_data")
    local_ref_dir = os.path.join(local_data, "reference_data")
    if not os.path.isdir(remote_ref_dir):
        return

    remote_meta = _load_meta(remote_data)
    local_meta = _load_meta(local_data)
    meta_changed = False

    for rel_path, remote_entry in remote_meta.items():
        if rel_path == "_meta.json":
            continue
        if not isinstance(remote_entry, dict):
            continue

        remote_hash = remote_entry.get("content_hash", "")
        remote_size = remote_entry.get("size", 0)

        local_entry = local_meta.get(rel_path)
        if isinstance(local_entry, dict):
            local_hash = local_entry.get("content_hash", "")
            local_size = local_entry.get("size", 0)

            ref_source = rel_path.split("/", 1)[0]

            if local_hash == remote_hash:
                result.refs_skipped += 1
                result._track(result.refs_by_source, ref_source, "skipped")
                continue

            if not _ref_wins(remote_hash, remote_size, local_hash, local_size):
                result.refs_skipped += 1
                result._track(result.refs_by_source, ref_source, "skipped")
                continue

            result.refs_updated += 1
            result._track(result.refs_by_source, ref_source, "updated")
        else:
            ref_source = rel_path.split("/", 1)[0]
            result.refs_copied += 1
            result._track(result.refs_by_source, ref_source, "copied")

        # Copy the file
        src = os.path.join(remote_ref_dir, rel_path)
        dst = os.path.join(local_ref_dir, rel_path)
        if not os.path.exists(src):
            continue
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        shutil.copy2(src, dst)
        local_meta[rel_path] = {"content_hash": remote_hash, "size": remote_size}
        meta_changed = True

    if meta_changed:
        _save_meta(local_data, local_meta)


def _merge_tables(local_db: str, remote_db: str, result: MergeResult) -> None:
    """Merge non-core tables from remote DB into local DB."""
    conn = sqlite3.connect(remote_db)
    try:
        tables = [row[0] for row in
                  conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
                  if row[0] not in _CORE_TABLES]
    finally:
        conn.close()

    if not tables:
        return

    local_conn = sqlite3.connect(local_db)
    remote_conn = sqlite3.connect(remote_db)
    try:
        for table in tables:
            # Get CREATE TABLE statement from remote
            create_sql = remote_conn.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)
            ).fetchone()
            if not create_sql or not create_sql[0]:
                continue

            # Ensure table exists locally (CREATE IF NOT EXISTS)
            local_conn.execute(create_sql[0].replace(
                f"CREATE TABLE {table}",
                f"CREATE TABLE IF NOT EXISTS {table}",
            ))

            # Get all rows from remote
            rows = remote_conn.execute(f"SELECT * FROM {table}").fetchall()
            if not rows:
                continue

            # Get column count for placeholders
            col_count = len(rows[0])
            placeholders = ",".join("?" * col_count)

            # INSERT OR IGNORE — existing rows (by primary key) are kept
            merged = 0
            for row in rows:
                before = local_conn.total_changes
                local_conn.execute(
                    f"INSERT OR IGNORE INTO {table} VALUES ({placeholders})", row
                )
                if local_conn.total_changes > before:
                    merged += 1

            result.table_rows_merged += merged
            result.tables_merged.append(table)

        local_conn.commit()
    finally:
        local_conn.close()
        remote_conn.close()


def _merge_activity(local_db: str, remote_db: str, result: MergeResult) -> None:
    """Merge activity records from remote DB into local DB."""
    # Build local dedup map
    local_rows = load_all_records(local_db)
    local_map: dict[str, tuple[int, object]] = {}
    for row_id, rec in local_rows:
        key = compute_default_dedup_key(rec.title, rec.service, rec.action, rec.timestamp)
        existing = local_map.get(key)
        if existing is None or pick_older_record(rec, existing[1]) is rec:
            local_map[key] = (row_id, rec)

    # Load remote records
    remote_rows = load_all_records(remote_db)

    to_insert = []
    to_update = []

    for _, remote_rec in remote_rows:
        key = compute_default_dedup_key(
            remote_rec.title, remote_rec.service, remote_rec.action, remote_rec.timestamp
        )

        if key not in local_map:
            to_insert.append(remote_rec)
            result.activity_inserted += 1
            result._track(result.activity_by_source, remote_rec.source, "inserted")
            continue

        local_id, local_rec = local_map[key]
        if records_equal(local_rec, remote_rec):
            result.activity_skipped += 1
            result._track(result.activity_by_source, remote_rec.source, "skipped")
            continue

        winner = pick_older_record(local_rec, remote_rec)
        if winner is remote_rec:
            to_update.append((local_id, remote_rec))
            result.activity_updated += 1
            result._track(result.activity_by_source, remote_rec.source, "updated")
        else:
            result.activity_skipped += 1
            result._track(result.activity_by_source, remote_rec.source, "skipped")

    if to_insert:
        insert_records(local_db, to_insert)
    for row_id, rec in to_update:
        update_record(local_db, row_id, rec)
