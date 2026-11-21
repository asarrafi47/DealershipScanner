"""Unrostered-rooftop detector (N8.13, D-N4 as answered 2026-10-16).

The rooftop gate refuses cars a group feed stamps with a store that is not the one being
scanned. Most of those stores are on the roster and get scanned in their own right. Some are
not (Mtn View Dalton/Bristol, Sam Pack's other stores, autosavvy's 22 rooftops): their cars
are held, never filed under a sister, and D-N4 wants each one profiled and scanned.

This module turns the refusals of one dealer's run into one *candidate* per unrostered
rooftop and keeps them in ``workspace/rooftop_candidates.json`` (``ROOFTOP_CANDIDATES_PATH``
overrides). The fleet summary and ``backend/scripts/rooftop_candidates.py`` list the ``new``
ones; the dealer-discovery workflow works the queue and marks each ``profiled``.

Recording is opt-in per process (``SCANNER_ROOFTOP_CANDIDATES=1``, set by ``fleet_scan``),
so tests and ad-hoc runs never write the shared file. Nothing here changes what the gate
keeps or refuses.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from backend.attribution import rooftop as _rt

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_PATH = ROOT / "workspace" / "rooftop_candidates.json"
# Refusals that say nothing about a named rooftop.
_NO_ROOFTOP_REASONS = frozenset({"unstamped_row_in_group_feed"})


def enabled() -> bool:
    return (os.environ.get("SCANNER_ROOFTOP_CANDIDATES") or "").strip().lower() in ("1", "true", "yes", "on")


def store_path() -> Path:
    return Path(os.environ.get("ROOFTOP_CANDIDATES_PATH") or DEFAULT_PATH)


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None).isoformat() + "Z"


def roster_entries() -> list[dict[str, str]]:
    """Active roster stores as ``{id, name, street_key, host}`` (best effort; [] when unreadable)."""
    try:
        from backend.attribution.place import host_key
        from backend.db.inventory_db import db_conn

        with db_conn() as conn:
            rows = conn.execute(
                "SELECT id, name, street_address, city, state, website_url FROM dealerships"
                " WHERE is_active = 1 AND duplicate_of_id IS NULL"
            ).fetchall()
        return [
            {
                "id": str(r[0]), "name": _rt._nrm(r[1]), "street_key": _rt._street_key(r[2]),
                "city_state": _rt._nrm(r[3]) + _rt._nrm(r[4]), "host": host_key(r[5]),
            }
            for r in rows
        ]
    except Exception as exc:  # noqa: BLE001 - the detector must never block a scan
        logger.debug("rooftop candidates: roster unreadable: %s", exc)
        return []


def _on_roster(rooftop: dict, roster: Iterable[dict[str, str]]) -> bool:
    """True when the rooftop is a rostered store: same normalised name, same street, or its slug."""
    name = _rt._nrm(_rt._rooftop_name(rooftop) or rooftop.get("name"))
    slug = _rt._rooftop_slug(rooftop)
    street_key = _rt._rooftop_place(rooftop)[1]
    for e in roster:
        if name and e["name"] and name == e["name"]:
            return True
        if street_key and e["street_key"] and street_key == e["street_key"]:
            return True
        if slug and slug in (_rt._nrm(e["id"]), e["name"]):
            return True
    return False


def _host_of(rooftop: dict, rows: list[dict]) -> str:
    for cand in (rooftop.get("url"), *(r.get("source_url") or r.get("_detail_url") for r in rows)):
        h = _rt._host(cand)
        if h:
            return h
    return ""


def candidates_from_refused(
    refused: list[dict], roster: list[dict[str, str]], scanned_dealer: str = "",
) -> list[dict[str, Any]]:
    """One candidate per unrostered rooftop named by the refused rows of one run."""
    groups: dict[str, tuple[dict, list[dict]]] = {}
    for row in refused or []:
        if str(row.get("_rooftop_reject") or "") in _NO_ROOFTOP_REASONS:
            continue
        rooftop = _rt._rooftop_of(row)
        identity = _rt._rooftop_identity(rooftop) if rooftop else ""
        if not identity:
            continue
        groups.setdefault(identity, (rooftop, []))[1].append(row)
    now = _now()
    out: list[dict[str, Any]] = []
    for identity, (rooftop, rows) in groups.items():
        if _on_roster(rooftop, roster):
            continue
        street, _key, _cs, postal = _rt._rooftop_place(rooftop)
        lines = _rt._label_lines(rooftop.get("key")) or _rt._label_lines(rooftop.get("name"))
        out.append({
            "id": identity,
            "name": _rt._rooftop_name(rooftop) or str(rooftop.get("name") or "").strip(),
            "address": ", ".join(p for p in (
                str(rooftop.get("address") or street or "").strip(),
                str(rooftop.get("city") or "").strip(),
                (str(rooftop.get("state") or "").strip() + " " + str(rooftop.get("zip") or postal or "").strip()).strip(),
            ) if p) or (lines[0] if lines and _rt._looks_like_address(lines[0]) else ""),
            "store_id": _rt._rooftop_slug(rooftop),
            "host": _host_of(rooftop, rows),
            "car_count": len({str(r.get("vin") or id(r)) for r in rows}),
            "first_seen": now,
            "last_seen": now,
            "seen_via": [scanned_dealer] if scanned_dealer else [],
            "status": "new",
        })
    return out


def merge(store: dict[str, Any], found: list[dict[str, Any]]) -> dict[str, Any]:
    """Fold ``found`` into the stored queue (first_seen kept, car_count = largest sighting)."""
    items = store.setdefault("candidates", {})
    for c in found:
        cur = items.get(c["id"])
        if cur is None:
            items[c["id"]] = dict(c)
            continue
        cur["last_seen"] = c["last_seen"]
        cur["car_count"] = max(int(cur.get("car_count") or 0), int(c["car_count"]))
        for k in ("name", "address", "store_id", "host"):
            if not cur.get(k) and c.get(k):
                cur[k] = c[k]
        via = list(cur.get("seen_via") or [])
        for d in c.get("seen_via") or []:
            if d not in via:
                via.append(d)
        cur["seen_via"] = via
    return store


def load(path: Path | None = None) -> dict[str, Any]:
    try:
        data = json.loads((path or store_path()).read_text())
        return data if isinstance(data, dict) else {"candidates": {}}
    except (OSError, ValueError):
        return {"candidates": {}}


def record(found: list[dict[str, Any]], path: Path | None = None) -> int:
    """Merge ``found`` into the queue file under an advisory lock (shards write concurrently)."""
    if not found:
        return 0
    p = path or store_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    lock = p.with_name(p.name + ".lock")
    with open(lock, "a+") as fh:
        try:
            import fcntl

            fcntl.flock(fh, fcntl.LOCK_EX)
        except ImportError:  # pragma: no cover - non-POSIX
            pass
        store = merge(load(p), found)
        tmp = p.with_name(p.name + ".tmp")
        tmp.write_text(json.dumps(store, indent=1, sort_keys=True))
        os.replace(tmp, p)
    return len(found)


def new_candidates(path: Path | None = None) -> list[dict[str, Any]]:
    items = (load(path).get("candidates") or {}).values()
    return sorted((c for c in items if c.get("status", "new") == "new"), key=lambda c: (-int(c.get("car_count") or 0), c.get("name") or ""))


def note_refusals(run_refused: list[dict], dealer_id: str, roster: list[dict[str, str]] | None = None) -> list[dict[str, Any]]:
    """Detect and, when enabled, record the unrostered rooftops among a run's refusals."""
    if not run_refused:
        return []
    found = candidates_from_refused(run_refused, roster if roster is not None else roster_entries(), dealer_id)
    if found and enabled():
        record(found)
    return found
