"""Sync: export/import skills via a shared exchange directory.

Designed for use with Syncthing or similar P2P file sync tools.
Each device writes to exchange/<device_id>/ and reads from other devices'
subdirectories, then merges via the CRDT merge module.

Exchange directory layout:
    exchange/
      <device-a>/
        activity.db
        reference_data/
          _meta.json
          claude_code/...
      <device-b>/
        ...
"""

import logging
import os
import shutil

from aicontext.merge import merge_skill, MergeResult

logger = logging.getLogger(__name__)


def _files_match(src: str, dst: str) -> bool:
    """Check if two files have the same size and mtime."""
    try:
        s = os.stat(src)
        d = os.stat(dst)
        return s.st_size == d.st_size and int(s.st_mtime) == int(d.st_mtime)
    except OSError:
        return False


def export_skill(skill_dir: str, exchange_dir: str, device_id: str) -> None:
    """Export local skill data to exchange/<device_id>/.

    Copies activity.db and reference_data/ (full export).
    Skips if the exported data is already up to date.
    """
    data_dir = os.path.join(skill_dir, "data")
    dest = os.path.join(exchange_dir, device_id)

    src_db = os.path.join(data_dir, "activity.db")
    dst_db = os.path.join(dest, "activity.db")
    src_meta = os.path.join(data_dir, "reference_data", "_meta.json")
    dst_meta = os.path.join(dest, "reference_data", "_meta.json")

    if _files_match(src_db, dst_db) and _files_match(src_meta, dst_meta):
        logger.info("Export skipped (no changes)")
        return

    os.makedirs(dest, exist_ok=True)

    # activity.db
    if os.path.exists(src_db):
        shutil.copy2(src_db, dst_db)

    # reference_data/
    src_ref = os.path.join(data_dir, "reference_data")
    dst_ref = os.path.join(dest, "reference_data")
    if os.path.isdir(src_ref):
        if os.path.exists(dst_ref):
            shutil.rmtree(dst_ref)
        shutil.copytree(src_ref, dst_ref)

    logger.info("Exported skill to %s", dest)


def import_skills(skill_dir: str, exchange_dir: str, device_id: str) -> list[MergeResult]:
    """Import and merge skills from all other devices in the exchange directory.

    Returns list of MergeResult (one per remote device that was merged).
    """
    if not os.path.isdir(exchange_dir):
        return []

    results = []
    for entry in sorted(os.listdir(exchange_dir)):
        if entry == device_id:
            continue
        remote_dir = os.path.join(exchange_dir, entry)
        remote_db = os.path.join(remote_dir, "activity.db")
        if not os.path.isfile(remote_db):
            continue

        logger.info("Merging from device: %s", entry)
        # merge_skill expects skill dirs with data/ subdirectory.
        # The exchange layout is flat (activity.db + reference_data/ directly),
        # so we wrap it to match the expected structure.
        result = _merge_from_exchange(skill_dir, remote_dir)
        results.append(result)

    return results


def _merge_from_exchange(skill_dir: str, remote_dir: str) -> MergeResult:
    """Merge a remote exchange directory into the local skill.

    The exchange layout is flat: exchange/<device>/activity.db + reference_data/.
    merge_skill expects: <dir>/data/activity.db + data/reference_data/.
    We create a temporary wrapper structure to bridge the difference.
    """
    # Build a virtual skill dir that points to the exchange data.
    # Rather than copying, create a minimal wrapper with symlinks.
    import tempfile
    wrapper = tempfile.mkdtemp(prefix="aicontext_sync_")
    wrapper_data = os.path.join(wrapper, "data")
    os.makedirs(wrapper_data)

    # Symlink activity.db and reference_data into wrapper/data/
    remote_db = os.path.join(remote_dir, "activity.db")
    remote_ref = os.path.join(remote_dir, "reference_data")

    os.symlink(remote_db, os.path.join(wrapper_data, "activity.db"))
    if os.path.isdir(remote_ref):
        os.symlink(remote_ref, os.path.join(wrapper_data, "reference_data"))

    try:
        return merge_skill(skill_dir, wrapper)
    finally:
        shutil.rmtree(wrapper)
