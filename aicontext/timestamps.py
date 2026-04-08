"""Timestamp parsing utilities for aicontext.

All parse functions return ISO 8601 strings with local timezone offset.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

_tz_name: str | None = None
_tz: ZoneInfo | None = None

_TZ_ABBREV_OFFSETS = {
    "PDT": -7, "PST": -8, "EDT": -4, "EST": -5,
    "CDT": -5, "CST": -6, "MDT": -6, "MST": -7,
    "UTC": 0, "GMT": 0,
}

_ISO_TS_RE = re.compile(
    r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}([+-]\d{2}:\d{2})?$'
)


def set_timezone(tz_name: str) -> None:
    global _tz_name, _tz
    _tz_name = tz_name
    _tz = ZoneInfo(tz_name)


def get_timezone() -> str:
    if _tz_name is None:
        raise RuntimeError("Timezone not set. Call set_timezone() first.")
    return _tz_name


def _ensure_tz() -> ZoneInfo:
    if _tz is None:
        raise RuntimeError("Timezone not set. Call set_timezone() first.")
    return _tz


def to_local_iso(dt_utc: datetime) -> str:
    tz = _ensure_tz()
    if dt_utc.tzinfo is None:
        dt_utc = dt_utc.replace(tzinfo=timezone.utc)
    local_dt = dt_utc.astimezone(tz)
    local_dt = local_dt.replace(microsecond=0)
    return local_dt.isoformat()


def validate_iso_timestamp(ts: str) -> bool:
    if not ts:
        return False
    return _ISO_TS_RE.match(ts) is not None


def parse_iso_utc(iso_str: str) -> str:
    """Parse ISO 8601 with Z or +00:00. Truncate fractional seconds."""
    _ensure_tz()
    s = iso_str.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dot_idx = s.find(".")
    if dot_idx != -1:
        plus_idx = s.find("+", dot_idx)
        minus_idx = s.find("-", dot_idx)
        offset_idx = -1
        if plus_idx != -1 and minus_idx != -1:
            offset_idx = min(plus_idx, minus_idx)
        elif plus_idx != -1:
            offset_idx = plus_idx
        elif minus_idx != -1:
            offset_idx = minus_idx
        if offset_idx != -1:
            s = s[:dot_idx] + s[offset_idx:]
        else:
            s = s[:dot_idx]
    dt = datetime.fromisoformat(s)
    return to_local_iso(dt)


def parse_chrome_epoch(chrome_usec: int) -> str:
    _ensure_tz()
    unix_sec = (chrome_usec / 1_000_000) - 11644473600
    dt_utc = datetime.fromtimestamp(unix_sec, tz=timezone.utc)
    return to_local_iso(dt_utc)


def parse_mac_absolute(mac_sec: float) -> str:
    _ensure_tz()
    unix_sec = mac_sec + 978307200
    dt_utc = datetime.fromtimestamp(unix_sec, tz=timezone.utc)
    return to_local_iso(dt_utc)
