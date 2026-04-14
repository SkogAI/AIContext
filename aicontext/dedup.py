"""Deduplication utilities for aicontext."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from urllib.parse import urlparse, parse_qs, urlencode, unquote, urlunparse

_TRACKING_PARAMS = frozenset({
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "fbclid", "gclid", "rlz", "sourceid", "ie", "oq", "gs_lcrp", "aqs",
    "sxsrf", "biw", "bih", "ved", "uact", "sclient", "ei", "sca_esv",
    "ref_", "num", "newwindow", "sca_upv",
})

_DEFAULT_PORTS = {"http": 80, "https": 443}


def normalize_for_dedup(text: str) -> str:
    if text is None:
        return ""
    s = text.strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def normalize_url(url: str) -> str:
    if not url:
        return ""
    try:
        s = url.strip().lower()
        s = unquote(s)
        parsed = urlparse(s)
        netloc = parsed.hostname or ""
        port = parsed.port
        scheme = parsed.scheme or "http"
        if port and _DEFAULT_PORTS.get(scheme) == port:
            port = None
        if port:
            netloc = f"{netloc}:{port}"
        if netloc.startswith("www."):
            netloc = netloc[4:]
        params = parse_qs(parsed.query, keep_blank_values=True)
        filtered = {k: sorted(v) for k, v in params.items() if k not in _TRACKING_PARAMS}
        sorted_query = urlencode(filtered, doseq=True)
        path = parsed.path.rstrip("/")
        result = netloc + path
        if sorted_query:
            result += "?" + sorted_query
        return result
    except Exception:
        return url.strip().lower()


def round_timestamp(ts_str: str, window_sec: int = 10) -> str:
    if not ts_str:
        return ""
    try:
        dt = datetime.fromisoformat(ts_str)
        epoch = dt.timestamp()
        rounded = round(epoch / window_sec) * window_sec
        return str(int(rounded))
    except (ValueError, TypeError):
        return ts_str


def collapse_consecutive(records: list, key_fn=None) -> list:
    if not records:
        return records
    if key_fn is None:
        key_fn = lambda r: normalize_for_dedup(r.title)
    sorted_records = sorted(records, key=lambda r: r.timestamp)
    result = [sorted_records[0]]
    prev_key = key_fn(sorted_records[0])
    for rec in sorted_records[1:]:
        k = key_fn(rec)
        if k != prev_key:
            result.append(rec)
            prev_key = k
    return result


def content_hash_json(data) -> str:
    canonical = json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _stable_json(value: dict | None) -> str:
    if value is None:
        return ""
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def record_order_key(rec) -> tuple:
    return (
        rec.timestamp, rec.source, rec.service, rec.action, rec.title,
        _stable_json(rec.extra), rec.ref_type or "", rec.ref_id or "",
    )


def records_equal(a, b) -> bool:
    return (a.timestamp == b.timestamp and a.source == b.source and a.service == b.service
            and a.action == b.action and a.title == b.title
            and a.extra == b.extra and a.ref_type == b.ref_type and a.ref_id == b.ref_id)


def pick_older_record(a, b):
    return a if record_order_key(a) <= record_order_key(b) else b


def compute_default_dedup_key(title: str, service: str, action: str, ts: str) -> str:
    rounded = round_timestamp(ts)
    raw = service + "|" + action + "|" + normalize_for_dedup(title) + "|" + rounded
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def should_replace_reference(new_hash: str, new_size: int,
                             local_hash: str, local_size: int) -> bool:
    """CRDT resolution for reference files: larger file wins, hash tiebreak."""
    if new_size != local_size:
        return new_size > local_size
    return new_hash > local_hash
