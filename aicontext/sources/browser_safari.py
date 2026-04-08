"""Safari local browser data source."""

import logging
import os
import shutil
import sqlite3
import tempfile

from aicontext.sources.base import DataSource
from aicontext.records import ActivityRecord
from aicontext.timestamps import parse_mac_absolute

logger = logging.getLogger(__name__)


class BrowserSafariSource(DataSource):

    @property
    def name(self) -> str:
        return "Safari Browser"

    @property
    def source_key(self) -> str:
        return "browser_safari"

    def ingest_activity(self, source_path: str, source_config: dict) -> list[ActivityRecord]:
        if not os.path.exists(source_path):
            return []

        tmp_path = None
        conn = None
        try:
            tmp_fd, tmp_path = tempfile.mkstemp(suffix=".sqlite")
            os.close(tmp_fd)
            shutil.copy2(source_path, tmp_path)
            conn = sqlite3.connect(f"file:{tmp_path}?mode=ro", uri=True)
            conn.row_factory = sqlite3.Row
            rows = conn.execute("""
                SELECT hv.visit_time, hv.title, hi.url
                FROM history_visits hv
                JOIN history_items hi ON hi.id = hv.history_item
            """).fetchall()
        except (PermissionError, sqlite3.DatabaseError) as e:
            logger.warning("Failed to read Safari history: %s", e)
            return []
        finally:
            if conn:
                conn.close()
            if tmp_path and os.path.exists(tmp_path):
                os.unlink(tmp_path)

        records = []
        for row in rows:
            title = row["title"]
            if not title:
                continue
            try:
                ts = parse_mac_absolute(row["visit_time"])
            except Exception:
                continue

            records.append(ActivityRecord(
                timestamp=ts, source="safari", service="safari", action="visited",
                title=title, ref_type="url", ref_id=row["url"],
            ))

        return records

    def get_reference_doc(self) -> str:
        return """# Safari Browser Reference

Local Safari browser history.

## Services
| Service | Description |
|---------|-------------|
| safari | Local Safari browser history |

## Actions
| Action | Meaning |
|--------|---------|
| visited | Page visit |

## Query Examples
```sql
SELECT timestamp, title, ref_id as url FROM activity
WHERE source='safari' ORDER BY timestamp DESC LIMIT 20;
```
"""
