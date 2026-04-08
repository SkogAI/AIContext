"""Codex session data source."""

from __future__ import annotations

import json
import logging
import os

from aicontext.sources.base import DataSource
from aicontext.records import ActivityRecord, ReferenceFile
from aicontext.timestamps import parse_iso_utc

logger = logging.getLogger(__name__)


def _relative_path(cwd):
    home = os.path.expanduser("~")
    if cwd and cwd.startswith(home):
        rel = cwd[len(home):]
        if rel.startswith(os.sep):
            rel = rel[1:]
        return rel if rel else "~"
    return cwd or ""


def _iter_session_files(source_path: str):
    if not os.path.isdir(source_path):
        return
    for dirpath, _, filenames in os.walk(source_path):
        for fname in sorted(filenames):
            if fname.endswith(".jsonl"):
                yield os.path.join(dirpath, fname)


def _parse_session(filepath: str) -> dict:
    events = []
    session_meta = None
    turn_contexts: dict[str, dict] = {}
    session_id = None
    first_user_text = None
    timestamps = []
    messages = []
    project_path = ""
    active_turn_id = None
    role_ordinals: dict[tuple[str, str], int] = {}

    with open(filepath, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue

            events.append(rec)
            payload = rec.get("payload") or {}
            rec_type = rec.get("type")
            ts_str = (rec.get("timestamp") or "").strip()
            ts = None
            if ts_str:
                try:
                    ts = parse_iso_utc(ts_str)
                    timestamps.append(ts)
                except Exception:
                    ts = None

            if rec_type == "session_meta":
                session_meta = payload
                session_id = payload.get("id") or session_id
                if not project_path:
                    project_path = _relative_path(payload.get("cwd"))
                continue

            if rec_type == "turn_context":
                turn_id = payload.get("turn_id")
                if turn_id:
                    turn_contexts[turn_id] = payload
                    if not project_path:
                        project_path = _relative_path(payload.get("cwd"))
                continue

            if rec_type != "event_msg":
                continue

            event_type = payload.get("type")
            if event_type == "task_started":
                active_turn_id = payload.get("turn_id")
                continue
            if event_type == "task_complete":
                active_turn_id = None
                continue
            if event_type not in ("user_message", "agent_message"):
                continue
            if ts is None or active_turn_id is None:
                continue

            turn_ctx = turn_contexts.get(active_turn_id, {})
            msg_text = (payload.get("message") or "").strip()
            if not msg_text:
                continue

            role = "user" if event_type == "user_message" else "assistant"
            ordinal_key = (active_turn_id, role)
            ordinal = role_ordinals.get(ordinal_key, 0)
            role_ordinals[ordinal_key] = ordinal + 1
            message_id = f"turn:{active_turn_id}:{role}:{ordinal}"

            if role == "user" and first_user_text is None:
                first_user_text = msg_text

            msg_entry = {
                "message_id": message_id,
                "role": role,
                "ts": ts,
                "text": msg_text,
                "event_type": event_type,
            }

            record = None
            if role == "user" or payload.get("phase") == "final_answer":
                record = ActivityRecord(
                    timestamp=ts,
                    source="codex",
                    service="codex",
                    action="prompted" if role == "user" else "received",
                    title=msg_text,
                    extra={"project_path": _relative_path(turn_ctx.get("cwd")) or project_path}
                          if (_relative_path(turn_ctx.get("cwd")) or project_path) else None,
                    ref_type="local",
                    ref_id=f"codex/{session_id}.json#msg:{message_id}",
                )

            messages.append({"record": record, "message": msg_entry})

    if not events:
        return {}
    if session_id is None:
        session_id = os.path.splitext(os.path.basename(filepath))[0]

    return {
        "session_id": session_id,
        "summary": (first_user_text or "")[:100],
        "created_at": min(timestamps) if timestamps else None,
        "modified_at": max(timestamps) if timestamps else None,
        "project_path": project_path or None,
        "session_meta": session_meta or {},
        "turns": [{"turn_id": tid, "context": ctx} for tid, ctx in turn_contexts.items()],
        "events": events,
        "messages": messages,
    }


class CodexSource(DataSource):

    @property
    def name(self) -> str:
        return "Codex"

    @property
    def source_key(self) -> str:
        return "codex"

    def ingest_activity(self, source_path: str, source_config: dict) -> list[ActivityRecord]:
        records = []
        for filepath in _iter_session_files(source_path):
            session = _parse_session(filepath)
            if not session:
                continue
            records.extend(msg["record"] for msg in session["messages"] if msg["record"] is not None)
        return records

    def ingest_reference(self, source_path: str, source_config: dict,
                         db_path: str | None = None) -> list[ReferenceFile] | None:
        ref_files = []
        for filepath in _iter_session_files(source_path):
            session = _parse_session(filepath)
            if not session:
                continue
            ref_files.append(ReferenceFile(
                path=f"codex/{session['session_id']}.json",
                data={
                    "summary": session["summary"],
                    "created_at": session["created_at"],
                    "modified_at": session["modified_at"],
                    "project_path": session["project_path"],
                    "messages": [msg["message"] for msg in session["messages"]],
                    "turns": session["turns"],
                    "session_meta": session["session_meta"],
                    "events": session["events"],
                },
            ))
        return ref_files or None

    def get_reference_doc(self) -> str:
        return """# Codex Reference

Codex CLI session history. Timeline records include user prompts and final assistant answers.

## Services
| Service | Description |
|---------|-------------|
| codex | Codex coding sessions |

## Actions
| Action | Meaning |
|--------|---------|
| prompted | User sent a prompt |
| received | Assistant sent a final answer |

## Extra Fields
| Field | Type | Description |
|-------|------|-------------|
| project_path | string | Project directory (home-relative) |

## Reference Data
Each session stored as `data/reference_data/codex/<session_id>.json`.

## Query Examples
```sql
-- Recent Codex messages
SELECT timestamp, action, SUBSTR(title, 1, 100) as message
FROM activity WHERE service='codex'
ORDER BY timestamp DESC LIMIT 20;
```
"""
