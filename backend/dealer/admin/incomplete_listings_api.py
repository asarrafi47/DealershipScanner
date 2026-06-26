"""Incomplete listing payloads and mutations for site-admin and legacy ``/dev`` APIs."""

from __future__ import annotations

import json
import logging
import re
import sqlite3
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from flask import jsonify

from backend.db.incomplete_listings_db import get_incomplete_listings_count
from backend.db.inventory_db import get_conn, get_incomplete_cars
from backend.utils.car_serialize import serialize_car_for_listings_grid
from backend.utils.listing_completeness import INCOMPLETE_FIELD_LABELS, summarize_incomplete_missing_fields

_log = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
INCOMPLETE_ISSUE_CODES_ALLOWED = frozenset(INCOMPLETE_FIELD_LABELS.keys())


def incomplete_listings_count() -> int:
    return get_incomplete_listings_count()


def build_incomplete_cars_payload() -> dict[str, Any]:
    cars = get_incomplete_cars()
    safe = []
    for c in cars:
        row = dict(c)
        missing = row.pop("incomplete_missing_fields", None) or []
        payload = serialize_car_for_listings_grid(row)
        payload["listing_trim_display"] = payload.get("trim")
        payload["listing_model_display"] = payload.get("model")
        payload["incomplete_missing_fields"] = missing
        safe.append(payload)
    return {
        "ok": True,
        "cars": safe,
        "count": len(safe),
        "issues_summary": summarize_incomplete_missing_fields(safe),
    }


def incomplete_cars_response():
    return jsonify(build_incomplete_cars_payload())


def _sqlite_car_row_json_safe(row: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in row.items():
        if isinstance(v, bytes):
            out[k] = f"<binary len={len(v)}>"
        elif isinstance(v, memoryview):
            out[k] = f"<memoryview len={len(v)}>"
        else:
            out[k] = v
    return out


def _incomplete_export_text_body(*, issue: str, ordered_rows: list[dict[str, Any]]) -> str:
    header = [
        "# Sarrafi Collection — incomplete listing export (SQLite ``cars`` columns)",
        f"# missing_field_code: {issue}",
        f"# row_count: {len(ordered_rows)}",
        f"# exported_at_utc: {datetime.now(timezone.utc).isoformat()}",
        "",
    ]
    chunks: list[str] = list(header)
    for i, r in enumerate(ordered_rows):
        safe = _sqlite_car_row_json_safe(r)
        cid = safe.get("id")
        vin = safe.get("vin")
        chunks.append("=" * 80)
        chunks.append(f"CAR {i + 1} of {len(ordered_rows)}  id={cid!r}  vin={vin!r}")
        chunks.append("=" * 80)
        for k in sorted(safe.keys()):
            chunks.append(f"{k}\t{safe[k]!r}")
        chunks.append("")
    return "\n".join(chunks) + "\n"


def _reveal_path_in_os_fs(path: Path) -> None:
    try:
        if sys.platform == "darwin":
            subprocess.run(["open", str(path.parent)], check=False, timeout=20)
        elif sys.platform == "win32":
            subprocess.run(
                ["explorer", f"/select,{path.resolve()}"],
                check=False,
                timeout=20,
            )
        elif sys.platform.startswith("linux"):
            subprocess.run(["xdg-open", str(path.parent)], check=False, timeout=20)
    except Exception:
        _log.debug("Could not open export folder in file manager", exc_info=True)


def export_incomplete_issue(issue: str):
    if not issue:
        return jsonify({"ok": False, "error": "issue code required"}), 400
    if issue not in INCOMPLETE_ISSUE_CODES_ALLOWED:
        return jsonify({"ok": False, "error": "invalid issue code"}), 400

    cars = get_incomplete_cars()
    ordered_ids: list[int] = []
    seen: set[int] = set()
    for c in cars:
        fields = c.get("incomplete_missing_fields")
        if not isinstance(fields, list) or issue not in fields:
            continue
        try:
            cid = int(c["id"])
        except (TypeError, ValueError, KeyError):
            continue
        if cid <= 0 or cid in seen:
            continue
        seen.add(cid)
        ordered_ids.append(cid)

    if not ordered_ids:
        _log.info("incomplete export: no cars match issue=%r", issue)
        return jsonify({"ok": True, "matched": 0, "issue": issue})

    conn = get_conn()
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    ph = ",".join("?" * len(ordered_ids))
    cur.execute(f"SELECT * FROM cars WHERE id IN ({ph})", ordered_ids)
    by_id = {int(dict(r)["id"]): dict(r) for r in cur.fetchall()}
    conn.close()

    ordered_rows = [by_id[i] for i in ordered_ids if i in by_id]

    slug = re.sub(r"[^a-zA-Z0-9_-]+", "_", issue).strip("_")[:80] or "issue"
    export_dir = PROJECT_ROOT / "workspace" / "incomplete_exports"
    export_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    fname = f"incomplete_{slug}_{stamp}.txt"
    out_path = export_dir / fname
    out_path.write_text(_incomplete_export_text_body(issue=issue, ordered_rows=ordered_rows), encoding="utf-8")

    rel_dir = export_dir.relative_to(PROJECT_ROOT).as_posix()
    rel_file = out_path.relative_to(PROJECT_ROOT).as_posix()

    _log.info(
        "Wrote %d incomplete car row(s) to %s for issue=%r",
        len(ordered_rows),
        rel_file,
        issue,
    )
    _reveal_path_in_os_fs(out_path)

    return jsonify(
        {
            "ok": True,
            "matched": len(ordered_rows),
            "issue": issue,
            "export_file": fname,
            "export_dir": rel_dir,
            "export_path": rel_file,
        }
    )


def delete_incomplete_car(car_id: int):
    conn = get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM saved_cars WHERE car_id = ?", (car_id,))
    except sqlite3.Error:
        _log.debug("saved_cars cleanup skipped for car_id=%s", car_id)
    cursor.execute("DELETE FROM cars WHERE id = ?", (car_id,))
    deleted = cursor.rowcount
    conn.commit()
    conn.close()
    if deleted:
        try:
            from backend.db import incomplete_listings_db as ild

            ild.delete_incomplete_record(car_id)
        except Exception:
            _log.exception("incomplete_listings cleanup after car delete failed")
        return jsonify({"ok": True})
    return jsonify({"ok": False, "error": "not found"}), 404
