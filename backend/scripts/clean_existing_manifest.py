#!/usr/bin/env python3
"""
One-off scrub of ``dealers.json``: keep only rows whose ``name`` matches OEM/franchise tokens
(see :func:`backend.discovery.overture_discovery.is_franchised_dealer`).

Writes an unscrubbed backup to ``workspace/dealers_backup.json`` before replacing the manifest.

Close editors and pause cloud sync on ``dealers.json`` while this runs; concurrent writes can corrupt the file.

Default manifest path:
  ``<repo>/local_data/dealers.json`` if that file exists, else ``<repo>/dealers.json``
  (scanner canonical path — there is no dealers path in ``backend/config/`` today).

Examples::

  python backend/scripts/clean_existing_manifest.py
  python backend/scripts/clean_existing_manifest.py --manifest /path/to/dealers.json
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import warnings
from pathlib import Path

warnings.filterwarnings(
    "ignore",
    message=r".*doesn't match a supported version.*",
)

# Repo root of this checkout (file is …/backend/scripts/<this>.py).
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

try:
    from backend.utils.project_env import load_project_dotenv

    load_project_dotenv()
except ImportError:
    pass


def _default_manifest_path(repo_root: Path) -> Path:
    local = repo_root / "local_data" / "dealers.json"
    if local.is_file():
        return local.resolve()
    return (repo_root / "dealers.json").resolve()


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    log = logging.getLogger("clean_existing_manifest")

    p = argparse.ArgumentParser(description="Remove non-franchised dealers from dealers.json (OEM name filter).")
    p.add_argument(
        "--manifest",
        type=Path,
        default=None,
        help=f"Path to dealers.json (default: local_data/dealers.json if present, else {_REPO_ROOT}/dealers.json)",
    )
    p.add_argument(
        "--backup",
        type=Path,
        default=None,
        help=f"Backup path for pre-scrub JSON (default: {_REPO_ROOT}/workspace/dealers_backup.json)",
    )
    args = p.parse_args(argv)

    from backend.dev.dealers import load_dealers, validate_dealers
    from backend.discovery.overture_discovery import is_franchised_dealer

    manifest_path = (args.manifest.expanduser().resolve() if args.manifest else _default_manifest_path(_REPO_ROOT))
    backup_path = (
        args.backup.expanduser().resolve()
        if args.backup
        else (_REPO_ROOT / "workspace" / "dealers_backup.json").resolve()
    )

    if not manifest_path.is_file():
        log.error("Manifest not found: %s", manifest_path)
        return 1

    rows = load_dealers(manifest_path)
    starting = len(rows)
    log.info("Starting dealership count: %s (%s)", starting, manifest_path)

    filtered = [r for r in rows if is_franchised_dealer(str(r.get("name") or ""))]
    removed = starting - len(filtered)
    final_count = len(filtered)
    log.info("Removing non-franchised / independent rows: %s", removed)
    log.info("Final franchised dealership count: %s", final_count)

    backup_path.parent.mkdir(parents=True, exist_ok=True)
    with backup_path.open("w", encoding="utf-8") as bf:
        json.dump(rows, bf, ensure_ascii=False, separators=(",", ":"))
        bf.write("\n")
        bf.flush()
        os.fsync(bf.fileno())
    log.info("Wrote unscrubbed backup: %s", backup_path)

    validated = validate_dealers(filtered)
    pending = manifest_path.with_name(manifest_path.name + ".scrub_pending")
    try:
        with pending.open("w", encoding="utf-8") as wf:
            json.dump(validated, wf, ensure_ascii=False, separators=(",", ":"))
            wf.write("\n")
            wf.flush()
            os.fsync(wf.fileno())
        loaded_check = json.loads(pending.read_text(encoding="utf-8"))
    except OSError as e:
        log.error("Failed writing scrub pending file %s: %s", pending, e)
        pending.unlink(missing_ok=True)
        return 1
    except json.JSONDecodeError as e:
        log.error("Scrub pending file is not valid JSON: %s", e)
        pending.unlink(missing_ok=True)
        return 1

    if not isinstance(loaded_check, list):
        log.error("Scrub pending JSON must be an array")
        pending.unlink(missing_ok=True)
        return 1
    leaked = [r for r in loaded_check if not is_franchised_dealer(str(r.get("name") or ""))]
    if leaked:
        log.error(
            "Pre-replace verification failed: %s non-franchised row(s) (example name=%r)",
            len(leaked),
            (leaked[0].get("name") or "")[:80],
        )
        pending.unlink(missing_ok=True)
        return 1
    if len(loaded_check) != len(validated):
        log.error("Pre-replace verification failed: row count mismatch")
        pending.unlink(missing_ok=True)
        return 1

    os.replace(pending, manifest_path)
    log.info("Wrote scrubbed manifest (compact JSON): %s", manifest_path)

    try:
        post = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        log.error(
            "Post-replace read failed (possible concurrent writer): %s. Restore from %s if needed.",
            e,
            backup_path,
        )
        return 1
    if len(post) != final_count:
        log.error(
            "Post-replace verification failed: disk has %s rows, expected %s — restore from %s if needed",
            len(post),
            final_count,
            backup_path,
        )
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
