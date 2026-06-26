"""
Track listing price changes across inventory scans in ``cars.price_provenance_json``.

Stored shape (JSON object):
  {
    "version": 1,
    "history": [{"date": "...", "price": 45000, "source": "inventory_scan", ...}, ...],
    "latest_change": {"date": "...", "previous_price": 46000, "new_price": 45000, "delta": -1000, "direction": "down"}
  }

``latest_change`` is set whenever the price moves; ``direction`` is ``down`` when the newer scan is lower.
"""
from __future__ import annotations

import json
from typing import Any

PRICE_PROVENANCE_VERSION = 1
_MAX_HISTORY = 64
_SCAN_SOURCE = "inventory_scan"


def _norm_price(value: Any) -> int | None:
    if value is None:
        return None
    try:
        p = int(round(float(value)))
    except (TypeError, ValueError):
        return None
    return p if p > 0 else None


def parse_price_provenance(raw: Any) -> dict[str, Any]:
    """Parse ``price_provenance_json`` column into a normalized dict."""
    out: dict[str, Any] = {"version": PRICE_PROVENANCE_VERSION, "history": []}
    if raw is None or raw == "" or str(raw).strip() in ("{}", "[]", "null"):
        return out
    try:
        parsed = json.loads(raw) if isinstance(raw, str) else raw
    except (json.JSONDecodeError, TypeError, ValueError):
        return out
    if isinstance(parsed, list):
        out["history"] = [_normalize_history_event(item) for item in parsed if isinstance(item, dict)]
        out["history"] = [e for e in out["history"] if e]
        return out
    if not isinstance(parsed, dict):
        return out
    hist = parsed.get("history") or parsed.get("sweeps") or parsed.get("price_history") or []
    if isinstance(hist, list):
        out["history"] = [_normalize_history_event(item) for item in hist if isinstance(item, dict)]
        out["history"] = [e for e in out["history"] if e]
    lc = parsed.get("latest_change")
    if isinstance(lc, dict):
        out["latest_change"] = lc
    return out


def _normalize_history_event(item: dict[str, Any]) -> dict[str, Any] | None:
    when = item.get("date") or item.get("recorded_at") or item.get("scraped_at")
    amt = _norm_price(item.get("price"))
    if when is None or amt is None:
        return None
    evt: dict[str, Any] = {
        "date": str(when),
        "price": amt,
        "source": str(item.get("source") or _SCAN_SOURCE)[:40],
    }
    if item.get("event"):
        evt["event"] = str(item["event"])[:32]
    if item.get("delta") is not None:
        try:
            evt["delta"] = int(round(float(item["delta"])))
        except (TypeError, ValueError):
            pass
    if item.get("direction") in ("down", "up", "flat"):
        evt["direction"] = item["direction"]
    return evt


def price_dropped_on_scan(previous_price: Any, new_price: Any) -> bool:
    """True when *new_price* is a positive amount strictly lower than *previous_price*."""
    prev = _norm_price(previous_price)
    new = _norm_price(new_price)
    return prev is not None and new is not None and new < prev


def build_price_change_event(
    *,
    previous_price: int,
    new_price: int,
    recorded_at: str,
    source: str = _SCAN_SOURCE,
) -> dict[str, Any]:
    delta = int(new_price) - int(previous_price)
    direction = "down" if delta < 0 else "up" if delta > 0 else "flat"
    return {
        "date": recorded_at,
        "previous_price": int(previous_price),
        "new_price": int(new_price),
        "delta": delta,
        "direction": direction,
        "source": source[:40],
    }


def latest_price_drop(provenance_raw: Any) -> dict[str, Any] | None:
    """
    Return the most recent downward price move, if any.

    Prefers ``latest_change`` when ``direction`` is ``down``; otherwise scans history
    newest-first for a down event.
    """
    blob = parse_price_provenance(provenance_raw)
    lc = blob.get("latest_change")
    if isinstance(lc, dict) and lc.get("direction") == "down":
        prev = _norm_price(lc.get("previous_price"))
        new = _norm_price(lc.get("new_price"))
        if prev is not None and new is not None and new < prev:
            return {
                "date": lc.get("date"),
                "previous_price": prev,
                "new_price": new,
                "delta": int(lc.get("delta") if lc.get("delta") is not None else new - prev),
                "source": lc.get("source") or _SCAN_SOURCE,
            }
    history = list(blob.get("history") or [])
    for evt in reversed(history):
        if evt.get("direction") != "down":
            continue
        prev = _norm_price(evt.get("previous_price"))
        new = _norm_price(evt.get("price"))
        if prev is not None and new is not None and new < prev:
            return {
                "date": evt.get("date"),
                "previous_price": prev,
                "new_price": new,
                "delta": int(evt.get("delta") if evt.get("delta") is not None else new - prev),
                "source": evt.get("source") or _SCAN_SOURCE,
            }
        if new is not None and evt.get("delta") is not None:
            try:
                delta = int(evt["delta"])
            except (TypeError, ValueError):
                continue
            if delta < 0:
                return {
                    "date": evt.get("date"),
                    "previous_price": new - delta,
                    "new_price": new,
                    "delta": delta,
                    "source": evt.get("source") or _SCAN_SOURCE,
                }
    return None


def merge_price_provenance_for_upsert(
    *,
    existing_provenance_json: Any,
    existing_price: Any,
    existing_first_seen_at: str | None,
    incoming_price: Any,
    recorded_at: str,
    is_new_row: bool,
    source: str = _SCAN_SOURCE,
) -> str | None:
    """
    Build updated ``price_provenance_json`` for an inventory upsert.

    - New row with price: seed history with a ``listed`` event.
    - Existing row: append when incoming price differs from stored price.
    - Returns None when there is nothing to store.
    """
    incoming = _norm_price(incoming_price)
    stored = _norm_price(existing_price)
    blob = parse_price_provenance(existing_provenance_json)
    history: list[dict[str, Any]] = list(blob.get("history") or [])

    if is_new_row:
        if incoming is None:
            return None
        history = [
            {
                "date": recorded_at,
                "price": incoming,
                "source": source,
                "event": "listed",
            }
        ]
        return _dump_provenance(history, latest_change=None)

    if incoming is None:
        return _dump_provenance(history, latest_change=_latest_change_from_blob(blob))

    anchor_date = (existing_first_seen_at or recorded_at).strip()
    if not history and stored is not None:
        history.append(
            {
                "date": anchor_date,
                "price": stored,
                "source": source,
                "event": "listed",
            }
        )

    if stored is None:
        history.append(
            {
                "date": recorded_at,
                "price": incoming,
                "source": source,
                "event": "listed",
            }
        )
        return _dump_provenance(history, latest_change=None)

    if incoming == stored:
        return _dump_provenance(history, latest_change=_latest_change_from_blob(blob))

    last_price = _norm_price(history[-1].get("price")) if history else None
    if last_price == incoming:
        return _dump_provenance(history, latest_change=_latest_change_from_blob(blob))

    change = build_price_change_event(
        previous_price=stored,
        new_price=incoming,
        recorded_at=recorded_at,
        source=source,
    )
    history.append(
        {
            "date": recorded_at,
            "price": incoming,
            "source": source,
            "event": "price_change",
            "delta": change["delta"],
            "direction": change["direction"],
            "previous_price": stored,
        }
    )
    history = history[-_MAX_HISTORY:]
    return _dump_provenance(history, latest_change=change)


def _dump_provenance(
    history: list[dict[str, Any]],
    *,
    latest_change: dict[str, Any] | None,
) -> str | None:
    if not history:
        return None
    payload: dict[str, Any] = {
        "version": PRICE_PROVENANCE_VERSION,
        "history": history[-_MAX_HISTORY:],
    }
    if latest_change:
        payload["latest_change"] = latest_change
    return json.dumps(payload, ensure_ascii=False)


def _latest_change_from_blob(blob: dict[str, Any]) -> dict[str, Any] | None:
    lc = blob.get("latest_change")
    return lc if isinstance(lc, dict) else None


def price_history_events_for_vdp(provenance_raw: Any) -> list[dict[str, Any]]:
    """Public VDP timeline: [{date, price}, ...] newest first."""
    blob = parse_price_provenance(provenance_raw)
    events: list[dict[str, Any]] = []
    for item in blob.get("history") or []:
        if not isinstance(item, dict):
            continue
        when = item.get("date")
        amt = _norm_price(item.get("price"))
        if when is None or amt is None:
            continue
        events.append({"date": str(when), "price": float(amt)})
    events.sort(key=lambda e: str(e.get("date") or ""), reverse=True)
    return events
