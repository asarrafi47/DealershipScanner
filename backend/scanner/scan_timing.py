"""
Per-dealer scan timing as part of the dealer fingerprint (``scan_hints["timing"]``).

Owner's request (2026-09-28): "take a note of dealerships that don't finish,
have issues, or are extremely long; make the time window part of the
dealership's fingerprint." A ``scan_runs`` row already carries the facts
(duration, error, ``summary_json.vdp_prefetch.http_first`` stats, phase
seconds); this module turns one row into a compact timing entry with flags,
keeps the last few entries per dealer, and derives the HTTP-first window the
dealer actually needs so the next run is neither cut short nor left to burn
the batch timeout.

Pure functions, no DB: the pipeline (``backend/scripts/dealer_pipeline.py``)
and the backfill (``backend/scripts/fingerprint_timing.py``) do the reading
and writing; ``backend/scanner/vdp/prefetch.py`` reads the recommendation.

Stored block (``dealer_recipes.scan_hints["timing"]``)::

    {"runs": [entry, ...],           # newest first, at most ``keep``
     "flags": [...],                 # flags of the latest run
     "vdp_http_first_max_sec": 1140, # None when the latest run was not capped
     "pages_needed": 1667,           # max candidates+skipped_cap over the runs
     "updated_at": "<finished_at of the latest run>"}
"""
from __future__ import annotations

import json
import math
from datetime import datetime
from typing import Any

# Defaults the recommendation is measured against (mirrors prefetch.py).
HTTP_FIRST_DEFAULT_SEC = 300
HTTP_FIRST_MAX_SEC = 1800
SLOW_SEC = 600
VERY_SLOW_SEC = 1200
UPSERT_SLOW_SEC = 200
FORBIDDEN_403_MIN = 5

# Deterministic flag order — every consumer (logs, triage, errors_index) prints them as stored.
FLAG_ORDER = (
    "error", "timed_out", "cap_hit", "host_exhausted", "slowed_host",
    "slow", "very_slow", "upsert_slow", "403s",
)
# Flags that put a dealer on the slow_dealers list / errors_index.
ATTENTION_FLAGS = frozenset({"error", "timed_out", "cap_hit", "very_slow", "host_exhausted"})


def _num(v: Any, default: float = 0.0) -> float:
    try:
        return float(v) if v is not None and str(v).strip() != "" else default
    except (TypeError, ValueError):
        return default


def _int(v: Any, default: int = 0) -> int:
    return int(round(_num(v, default)))


def _iso(v: Any) -> str | None:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.isoformat()
    s = str(v).strip()
    return s or None


def _parse_summary(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if raw is None:
        return {}
    try:
        parsed = json.loads(raw)
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def timing_entry(run: dict[str, Any]) -> dict[str, Any]:
    """One compact timing record from a ``scan_runs`` row.

    ``run`` needs ``id``, ``finished_at``, ``duration_seconds``, ``error`` and
    ``summary_json`` (JSON text or an already-parsed dict).
    """
    s = _parse_summary(run.get("summary_json"))
    hf = (s.get("vdp_prefetch") or {}).get("http_first") or {}
    if not isinstance(hf, dict):
        hf = {}
    phases = s.get("phase_secs") if isinstance(s.get("phase_secs"), dict) else {}
    statuses = hf.get("statuses") if isinstance(hf.get("statuses"), dict) else {}

    duration_s = round(_num(run.get("duration_seconds")), 1)
    upsert_raw = phases.get("upsert")
    upsert_s = round(_num(upsert_raw), 1) if upsert_raw is not None else None
    error = str(run.get("error")).strip() if run.get("error") else None

    prefetch = {
        "candidates": _int(hf.get("candidates")),
        "fetched": _int(hf.get("fetched")),
        "skipped_cap": _int(hf.get("skipped_cap")),
        "retried": _int(hf.get("retried")),
        "statuses": {str(k): _int(v) for k, v in statuses.items()},
        "wall_clock_hit": bool(hf.get("wall_clock_hit")),
        # The window the pass actually ran under (prefetch records it since
        # 2026-09-28); None on older rows, where the default is assumed.
        "wall_clock_sec": _int(hf.get("wall_clock_sec")) or None,
        "host_exhausted": hf.get("host_exhausted") or None,
        "slowed_host": hf.get("slowed_host") or None,
    }

    err_l = (error or "").lower()
    timed_out = bool(s.get("vdp_phase_timed_out")) or "timeout" in err_l or "timed out" in err_l
    cap_hit = prefetch["wall_clock_hit"] or (
        prefetch["skipped_cap"] > 0 and prefetch["fetched"] < prefetch["candidates"]
    )
    conditions = {
        "error": bool(error),
        "timed_out": timed_out,
        "cap_hit": cap_hit,
        "host_exhausted": bool(prefetch["host_exhausted"]),
        "slowed_host": bool(prefetch["slowed_host"]),
        "slow": duration_s > SLOW_SEC,
        "very_slow": duration_s > VERY_SLOW_SEC,
        "upsert_slow": (upsert_s or 0) > UPSERT_SLOW_SEC,
        "403s": prefetch["statuses"].get("403", 0) >= FORBIDDEN_403_MIN,
    }
    flags = [f for f in FLAG_ORDER if conditions[f]]

    return {
        "run_id": run.get("id"),
        "finished_at": _iso(run.get("finished_at")),
        "duration_s": duration_s,
        "upsert_s": upsert_s,
        "prefetch": prefetch,
        "error": error[:200] if error else None,
        "flags": flags,
    }


def _sort_key(e: dict[str, Any]) -> tuple[str, int]:
    return (str(e.get("finished_at") or ""), _int(e.get("run_id")))


def latest_entry(entries: list[dict[str, Any]]) -> dict[str, Any] | None:
    return max(entries, key=_sort_key) if entries else None


def _pages_wanted(e: dict[str, Any]) -> int:
    p = e.get("prefetch") or {}
    return _int(p.get("candidates")) + _int(p.get("skipped_cap"))


def run_window_sec(entry: dict[str, Any]) -> int:
    """The HTTP-first window a run actually used: its recorded
    ``prefetch.wall_clock_sec``, else the default (rows written before the
    stat existed)."""
    p = entry.get("prefetch") or {}
    used = _int(p.get("wall_clock_sec"))
    return used if used > 0 else HTTP_FIRST_DEFAULT_SEC


def recommend_windows(entries: list[dict[str, Any]]) -> dict[str, Any]:
    """HTTP-first window the dealer needs, from its recent timing entries.

    ``vdp_http_first_max_sec``: None when the latest run was not capped; else
    the window that run actually used (``run_window_sec``) scaled by pages
    wanted / pages fetched, rounded up to the minute, floored at both the
    default and the window that just hit the cap, and capped at
    ``HTTP_FIRST_MAX_SEC``. Scaling off the run's own window (not the
    default) keeps a hinted dealer from oscillating: a capped 1140 s pass
    used to be re-scaled from 300 s and recommend less than it just used.
    The scale is exact for ``wall_clock_hit`` and conservative when the page
    cap ended the pass earlier.

    ``pages_needed``: the most pages any of the runs wanted
    (candidates + skipped_cap); None when no run carried prefetch stats.
    """
    out: dict[str, Any] = {"vdp_http_first_max_sec": None, "pages_needed": None}
    latest = latest_entry(entries)
    if latest is None:
        return out
    wanted_all = [_pages_wanted(e) for e in entries]
    if any(wanted_all):
        out["pages_needed"] = max(wanted_all)
    if "cap_hit" not in (latest.get("flags") or []):
        return out
    p = latest.get("prefetch") or {}
    wanted = _pages_wanted(latest)
    fetched = max(_int(p.get("fetched")), 1)
    used = run_window_sec(latest)
    scaled = used * wanted / fetched
    minutes = math.ceil(scaled / 60.0)
    sec = 60 * minutes
    out["vdp_http_first_max_sec"] = int(min(HTTP_FIRST_MAX_SEC, max(HTTP_FIRST_DEFAULT_SEC, used, sec)))
    return out


def merge_timing(existing_hints: dict[str, Any] | None, entry: dict[str, Any], keep: int = 5) -> dict[str, Any]:
    """New ``timing`` block: ``entry`` folded onto the runs already stored.

    Runs are deduped by ``run_id`` (a re-assessed run replaces its older
    record), kept newest first, at most ``keep``. Flags are the latest run's.
    """
    prev_block = (existing_hints or {}).get("timing") if isinstance(existing_hints, dict) else None
    prev_runs = prev_block.get("runs") if isinstance(prev_block, dict) else None
    runs: list[dict[str, Any]] = [entry]
    seen = {entry.get("run_id")}
    for r in prev_runs or []:
        if not isinstance(r, dict) or r.get("run_id") in seen:
            continue
        seen.add(r.get("run_id"))
        runs.append(r)
    runs.sort(key=_sort_key, reverse=True)
    runs = runs[: max(1, int(keep))]
    latest = runs[0]
    rec = recommend_windows(runs)
    return {
        "runs": runs,
        "flags": list(latest.get("flags") or []),
        "vdp_http_first_max_sec": rec["vdp_http_first_max_sec"],
        "pages_needed": rec["pages_needed"],
        "updated_at": latest.get("finished_at"),
    }


def needs_attention(flags: list[str] | None) -> bool:
    return any(f in ATTENTION_FLAGS for f in (flags or []))


def minutes(entry: dict[str, Any]) -> float:
    return round(_num(entry.get("duration_s")) / 60.0, 1)
