"""Claude Code session data source."""

from __future__ import annotations

import json
import logging
import os
import re

from aicontext.sources.base import DataSource
from aicontext.records import ActivityRecord, ReferenceFile
from aicontext.timestamps import parse_iso_utc

logger = logging.getLogger(__name__)

_SYSTEM_TAG_RE = re.compile(r'^<[a-zA-Z]')


def _is_real_user_prompt(rec):
    if rec.get("type") != "user":
        return False
    content = rec.get("message", {}).get("content", "")
    if not isinstance(content, str):
        return False
    text = content.strip()
    if not text:
        return False
    if text == "[Request interrupted by user]":
        return False
    if _SYSTEM_TAG_RE.match(text) and not text.startswith("<http"):
        return False
    return True


def _relative_path(cwd):
    home = os.path.expanduser("~")
    if cwd and cwd.startswith(home):
        rel = cwd[len(home):]
        if rel.startswith(os.sep):
            rel = rel[1:]
        return rel if rel else "~"
    return cwd or ""


def _message_id(rec):
    msg_id = rec.get("uuid")
    return f"msg:{msg_id}" if msg_id else None


def _iter_sessions(source_path):
    if not os.path.isdir(source_path):
        return

    jsonl_files = []
    for dirpath, dirnames, filenames in os.walk(source_path):
        dirnames[:] = [d for d in dirnames if d != "subagents"]
        for fname in filenames:
            if fname.endswith(".jsonl"):
                jsonl_files.append(os.path.join(dirpath, fname))

    for filepath in sorted(jsonl_files):
        if "/subagents/" in filepath:
            continue

        try:
            with open(filepath, encoding="utf-8") as fh:
                lines = fh.readlines()
        except Exception:
            continue

        parsed_records = []
        for line in lines:
            line = line.strip()
            if not line:
                continue
            try:
                parsed_records.append(json.loads(line))
            except json.JSONDecodeError:
                continue

        if not parsed_records:
            continue

        session_id = None
        cwd = None
        git_branch = None
        is_sidechain = False

        for rec in parsed_records:
            if session_id is None and rec.get("sessionId"):
                session_id = rec["sessionId"]
            if cwd is None and rec.get("cwd"):
                cwd = rec["cwd"]
            if git_branch is None and rec.get("gitBranch"):
                git_branch = rec["gitBranch"]
            if rec.get("isSidechain"):
                is_sidechain = True
                break

        if is_sidechain:
            continue
        if session_id is None:
            session_id = os.path.splitext(os.path.basename(filepath))[0]

        yield session_id, _relative_path(cwd), git_branch, parsed_records, filepath


class ClaudeCodeSource(DataSource):

    @property
    def name(self) -> str:
        return "Claude Code"

    @property
    def source_key(self) -> str:
        return "claude_code"

    def ingest_activity(self, source_path: str, source_config: dict) -> list[ActivityRecord]:
        records = []
        for session_id, relative_cwd, git_branch, parsed_records, _filepath in _iter_sessions(source_path):
            for rec in parsed_records:
                if rec.get("type") == "file-history-snapshot":
                    continue

                rec_type = rec.get("type")
                if rec_type not in ("user", "assistant"):
                    continue

                timestamp_str = (rec.get("timestamp") or "").strip()
                if not timestamp_str:
                    continue
                try:
                    ts = parse_iso_utc(timestamp_str)
                except Exception:
                    continue

                if rec_type == "user":
                    if not _is_real_user_prompt(rec):
                        continue
                    text = rec.get("message", {}).get("content", "").strip()
                    action = "prompted"
                else:
                    content = rec.get("message", {}).get("content", [])
                    if isinstance(content, list):
                        text_parts = []
                        for block in content:
                            if isinstance(block, dict) and block.get("type") == "text":
                                text_parts.append(block.get("text", ""))
                        text = "\n".join(text_parts).strip()
                    elif isinstance(content, str):
                        text = content.strip()
                    else:
                        continue
                    action = "received"

                if not text:
                    continue

                extra = {}
                if relative_cwd:
                    extra["project_path"] = relative_cwd
                if git_branch:
                    extra["git_branch"] = git_branch

                message_id = _message_id(rec)
                if message_id is None:
                    continue

                records.append(ActivityRecord(
                    timestamp=ts,
                    source="claude_code",
                    service="claude_code",
                    action=action,
                    title=text,
                    extra=extra or None,
                    ref_type="local",
                    ref_id=f"claude_code/{session_id}.json#{message_id}",
                ))
        return records

    def ingest_reference(self, source_path: str, source_config: dict,
                         db_path: str | None = None) -> list[ReferenceFile] | None:
        ref_files = []
        for session_id, relative_cwd, git_branch, parsed_records, filepath in _iter_sessions(source_path):
            messages = []
            timestamps = []
            first_prompt = None

            for rec in parsed_records:
                if rec.get("type") == "file-history-snapshot":
                    continue

                timestamp_str = (rec.get("timestamp") or "").strip()
                if not timestamp_str:
                    continue
                try:
                    ts = parse_iso_utc(timestamp_str)
                except Exception:
                    continue
                timestamps.append(ts)

                rec_type = rec.get("type")
                if rec_type == "user":
                    message_id = _message_id(rec)
                    if message_id is None:
                        continue
                    content = rec.get("message", {}).get("content", "")
                    if isinstance(content, str) and content.strip():
                        if _is_real_user_prompt(rec) and first_prompt is None:
                            first_prompt = content.strip()
                        messages.append({
                            "message_id": message_id,
                            "role": "user",
                            "text": content.strip(),
                            "ts": ts,
                        })
                    elif isinstance(content, list):
                        messages.append({
                            "message_id": message_id,
                            "role": "user",
                            "content": content,
                            "ts": ts,
                        })
                elif rec_type == "assistant":
                    message_id = _message_id(rec)
                    if message_id is None:
                        continue
                    content = rec.get("message", {}).get("content", [])
                    if isinstance(content, list):
                        messages.append({
                            "message_id": message_id,
                            "role": "assistant",
                            "content": content,
                            "ts": ts,
                        })
                    elif isinstance(content, str) and content.strip():
                        messages.append({
                            "message_id": message_id,
                            "role": "assistant",
                            "content": [{"type": "text", "text": content.strip()}],
                            "ts": ts,
                        })

            if not messages:
                continue

            entry = {
                "summary": (first_prompt or "")[:100],
                "messages": messages,
                "project_path": relative_cwd,
            }
            if git_branch:
                entry["git_branch"] = git_branch
            if timestamps:
                entry["created_at"] = min(timestamps)
                entry["modified_at"] = max(timestamps)

            ref_files.append(ReferenceFile(
                path=f"claude_code/{session_id}.json",
                data=entry,
            ))

        return ref_files or None

    def get_reference_doc(self) -> str:
        return """# Claude Code Reference

Claude Code session history. Timeline records include both user prompts and assistant responses, with project path and git branch metadata. Full sessions are in reference data.

## Services
| Service | Description |
|---------|-------------|
| claude_code | Claude Code coding sessions |

## Actions
| Action | Meaning |
|--------|---------|
| prompted | User sent a prompt |
| received | Assistant response |

## Extra Fields
| Field | Type | Description |
|-------|------|-------------|
| project_path | string | Project directory (home-relative) |
| git_branch | string | Git branch at time of prompt |

## Reference Data
Each session stored as `{DATA_DIR}/reference_data/claude_code/<session_id>.json`.

Timeline rows link to specific messages via `ref_id='claude_code/<session_id>.json#msg:<message_id>'`.

## Query Examples
```sql
-- Recent session messages
SELECT timestamp, action, SUBSTR(title, 1, 80) as message,
       json_extract(extra, '$.project_path') as project
FROM activity WHERE service='claude_code'
ORDER BY timestamp DESC LIMIT 20;

-- Messages by project
SELECT json_extract(extra, '$.project_path') as project, COUNT(*) as n
FROM activity WHERE service='claude_code'
GROUP BY project ORDER BY n DESC;
```
"""
