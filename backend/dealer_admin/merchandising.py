"""
Merchandising / listing-quality rules and stale-pricing heuristics (pure functions + row dict in).

Rules return stable string codes for dashboards, exports, and tests. Copy labels in templates only.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any

# HTTPS gallery URLs counted as "photos" for merchandising thresholds.
_MIN_PHOTOS_DEFAULT = 3
_STALE_PRICE_DAYS_DEFAULT = 45


def min_photo_threshold() -> int:
    try:
        return max(1, int(os.environ.get("STORE_ADMIN_MIN_PHOTOS", str(_MIN_PHOTOS_DEFAULT))))
    except (TypeError, ValueError):
        return _MIN_PHOTOS_DEFAULT


def stale_price_days() -> int:
    try:
        return max(7, int(os.environ.get("STORE_ADMIN_STALE_PRICE_DAYS", str(_STALE_PRICE_DAYS_DEFAULT))))
    except (TypeError, ValueError):
        return _STALE_PRICE_DAYS_DEFAULT


def https_gallery_count(row: dict[str, Any]) -> int:
    raw = row.get("gallery")
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except (json.JSONDecodeError, TypeError, ValueError):
            raw = []
    if not isinstance(raw, list):
        return 0
    n = 0
    for u in raw:
        if isinstance(u, str) and u.strip().lower().startswith("https"):
            n += 1
    return n


def _price(row: dict[str, Any]) -> float | None:
    p = row.get("price")
    if p is None or str(p).strip() == "":
        return None
    try:
        return float(p)
    except (TypeError, ValueError):
        return None


def merchandising_issue_codes(row: dict[str, Any]) -> list[str]:
    """Ordered issue codes for one ``cars`` row (inventory DB)."""
    issues: list[str] = []
    pr = _price(row)
    if pr is None:
        issues.append("no_price")
    elif pr <= 0:
        issues.append("price_zero")

    nphotos = https_gallery_count(row)
    minp = min_photo_threshold()
    if nphotos == 0:
        issues.append("empty_gallery")
    elif nphotos < minp:
        issues.append("low_photo_count")

    def _missing(field: str) -> bool:
        v = row.get(field)
        return v is None or str(v).strip() == ""

    if _missing("trim"):
        issues.append("missing_trim")
    if _missing("exterior_color"):
        issues.append("missing_exterior_color")
    if _missing("interior_color"):
        issues.append("missing_interior_color")

    desc = (row.get("description") or "").strip()
    pkg = row.get("packages")
    pkg_empty = pkg is None or str(pkg).strip() in ("", "{}", "[]", "null")
    if desc and pkg_empty:
        issues.append("missing_packages_with_description")

    return issues


def _parse_ts(val: Any) -> datetime | None:
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return None


def is_stale_price(row: dict[str, Any]) -> bool:
    """
    True when list price has not changed for ``STORE_ADMIN_STALE_PRICE_DAYS`` or more.

    Uses ``last_price_change_at`` when set, else ``first_seen_at``, else ``scraped_at``.
    Requires a positive list price (zero / call-for-price excluded).
    """
    pr = _price(row)
    if pr is None or pr <= 0:
        return False
    anchor = row.get("last_price_change_at") or row.get("first_seen_at") or row.get("scraped_at")
    dt = _parse_ts(anchor)
    if not dt:
        return False
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    age = (datetime.now(timezone.utc) - dt).total_seconds() / 86400.0
    return age >= float(stale_price_days())


def customer_ready_codes(row: dict[str, Any]) -> dict[str, bool]:
    """
    Simple checklist for lead readiness (auto from data; conservative — no fabricated signals).
    """
    issues = set(merchandising_issue_codes(row))
    return {
        "has_price": "no_price" not in issues and "price_zero" not in issues,
        "has_photos": "empty_gallery" not in issues and "low_photo_count" not in issues,
        "has_trim": "missing_trim" not in issues,
        "has_colors": "missing_exterior_color" not in issues
        and "missing_interior_color" not in issues,
        "has_packages_or_no_desc": "missing_packages_with_description" not in issues,
        "not_stale_price": not is_stale_price(row),
    }


def issue_label(code: str) -> str:
    return {
        "no_price": "No list price",
        "price_zero": "List price is zero",
        "empty_gallery": "No HTTPS gallery images",
        "low_photo_count": "Fewer than minimum photos",
        "missing_trim": "Missing trim",
        "missing_exterior_color": "Missing exterior color",
        "missing_interior_color": "Missing interior color",
        "missing_packages_with_description": "Description present but no packages JSON",
    }.get(code, code)


def derive_price_provenance(row: dict[str, Any]) -> list[dict[str, Any]]:
    """
    Read-only hints from stored JSON (never fabricates). MSRP + list price + optional VDP hint.
    """
    out: list[dict[str, Any]] = []
    pr = _price(row)
    if pr is not None and pr > 0:
        out.append({"label": "Internet / list price", "value": pr, "source": "inventory_feed"})
    msrp = row.get("msrp")
    if msrp is not None:
        try:
            m = float(msrp)
            if m > 0:
                out.append({"label": "MSRP", "value": m, "source": "feed_or_vdp"})
        except (TypeError, ValueError):
            pass
    raw = row.get("spec_source_json")
    if raw and str(raw).strip():
        try:
            sj = json.loads(raw) if isinstance(raw, str) else raw
        except (json.JSONDecodeError, TypeError, ValueError):
            sj = None
        if isinstance(sj, dict):
            vp = sj.get("vdp_price")
            if isinstance(vp, dict):
                amt = vp.get("amount") or vp.get("value")
                if amt is not None:
                    try:
                        out.append(
                            {
                                "label": "VDP-captured price hint",
                                "value": float(amt),
                                "source": "vdp_merge",
                                "note": str(vp.get("note") or "")[:200] or None,
                            }
                        )
                    except (TypeError, ValueError):
                        pass
    return out


def days_in_inventory(row: dict[str, Any]) -> float | None:
    """Approximate days since ``first_seen_at`` (or ``scraped_at``)."""
    anchor = row.get("first_seen_at") or row.get("scraped_at")
    dt = _parse_ts(anchor)
    if not dt:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return round((datetime.now(timezone.utc) - dt).total_seconds() / 86400.0, 1)


def kpi_counts_for_rows(rows: list[dict[str, Any]]) -> dict[str, int]:
    """Aggregate counts for admin dashboard widgets."""
    rule_hits: dict[str, int] = {}
    stale = 0
    for row in rows:
        for code in merchandising_issue_codes(row):
            rule_hits[code] = rule_hits.get(code, 0) + 1
        if is_stale_price(row):
            stale += 1
    return {"stale_price_units": stale, "rule_hits": rule_hits}
