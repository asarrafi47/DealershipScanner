"""
Structured JSONL scan log — one line per vehicle, one summary line per dealer.

Written to workspace/scanlogs/<timestamp>.jsonl alongside normal stdout logging.
Thread-safe; no-ops if init_scan_log() was never called.

Line types:
  {"type": "vehicle", ...}      — one per captured vehicle
  {"type": "dealer_summary", ...} — one per dealer at run end
  {"type": "scan_start", ...}   — emitted once when the scan begins
"""
from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_lock = threading.Lock()
_log_file: Path | None = None
_scan_ts: str = ""


def init_scan_log(log_path: Path) -> None:
    global _log_file, _scan_ts
    log_path.parent.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).isoformat()
    with _lock:
        _log_file = log_path
        _scan_ts = ts
    _write({"type": "scan_start", "scan_ts": ts, "log_file": str(log_path)})


def active_log_path() -> Path | None:
    return _log_file


def _write(record: dict[str, Any]) -> None:
    if _log_file is None:
        return
    line = json.dumps(record, ensure_ascii=False, separators=(",", ":"), default=str) + "\n"
    with _lock:
        with _log_file.open("a", encoding="utf-8") as fh:
            fh.write(line)


def log_vehicles(
    dealer_id: str,
    dealer_name: str,
    provider: str,
    vehicles: list[dict[str, Any]],
) -> None:
    if _log_file is None or not vehicles:
        return
    ts = datetime.now(timezone.utc).isoformat()
    for v in vehicles:
        _write({
            "type": "vehicle",
            "scan_ts": ts,
            "dealer_id": dealer_id,
            "dealer_name": dealer_name,
            "provider": provider,
            "vin": v.get("vin"),
            "year": v.get("year"),
            "make": v.get("make"),
            "model": v.get("model"),
            "trim": v.get("trim"),
            "price": v.get("price"),
            "mileage": v.get("mileage"),
            "exterior_color": v.get("exterior_color"),
            "interior_color": v.get("interior_color"),
            "stock_number": v.get("stock_number"),
            "condition": v.get("condition"),
        })


def read_scan_log(
    path: Path,
    *,
    dealer_id: str | None = None,
    vin: str | None = None,
    record_type: str | None = None,
) -> list[dict[str, Any]]:
    """
    Read a scan JSONL log back, optionally filtered.

    ``record_type`` filters to one line type ("vehicle", "dealer_summary",
    "scan_start"); ``dealer_id`` / ``vin`` filter on those fields when present
    on a record. Malformed lines are skipped. Returns [] if ``path`` doesn't exist.
    """
    p = Path(path)
    if not p.is_file():
        return []
    rows: list[dict[str, Any]] = []
    with p.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except (json.JSONDecodeError, ValueError):
                continue
            if not isinstance(rec, dict):
                continue
            if record_type and rec.get("type") != record_type:
                continue
            if dealer_id and str(rec.get("dealer_id") or "") != str(dealer_id):
                continue
            if vin and str(rec.get("vin") or "") != str(vin):
                continue
            rows.append(rec)
    return rows


def failed_dealer_ids(path: Path) -> list[str]:
    """
    dealer_id values whose ``dealer_summary`` line carried an error, in the
    order they appear in the log. Used to build a retry list for a prior run.
    """
    seen: dict[str, None] = {}
    for rec in read_scan_log(path, record_type="dealer_summary"):
        if not rec.get("error"):
            continue
        did = str(rec.get("dealer_id") or "").strip()
        if did:
            seen[did] = None
    return list(seen.keys())


def log_dealer_summary(result: dict[str, Any], provider: str) -> None:
    if _log_file is None:
        return
    err = result.get("error")
    _write({
        "type": "dealer_summary",
        "scan_ts": _scan_ts,
        "dealer_id": result.get("dealer_id"),
        "dealer_name": result.get("dealer_name"),
        "provider": provider,
        "upserted": result.get("upserted"),
        "inventory_rows": result.get("inventory_rows"),
        "deduped_rows": result.get("deduped_rows"),
        "vdps_visited": result.get("vdps_visited"),
        "vehicles_vdp_enriched": result.get("vehicles_vdp_enriched"),
        "seconds": round(float(result.get("seconds") or 0.0), 2),
        "error": str(err)[:400] if err else None,
        "recovery_strategy": (result.get("inventory_recovery") or {}).get("winning_strategy"),
        "phase_secs": result.get("phase_secs"),
    })
