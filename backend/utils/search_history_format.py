"""Presentation helpers for stored listings filter objects (recent + saved searches).

A stored filter object is the cleaned listings query-param shape
(``_clean_saved_search_filters`` in backend/routes/listings_api.py): scalar or
list-of-scalar values keyed by the /listings query parameter names main.js
writes in ``syncUrl()``. These helpers turn one back into a ``/listings?...`` URL
(the "Run again" link), a one-line human summary, and a relative timestamp for
the account profile. Pure functions; no Flask context needed.
"""
from __future__ import annotations

from datetime import datetime, timezone
from urllib.parse import urlencode

from backend.db.repositories.search_history_repo import parse_created_at

LISTINGS_PATH = "/listings"

# Keys that are never part of a re-runnable search.
_SKIP_URL_KEYS = frozenset({"page"})
# Legacy/alternate spellings normalised to the query-param name the page reads.
_URL_KEY_ALIASES = {
    "dealer_registry_ids": "dealer_registry_id",
    "search": "q",
    "zip": "zip_code",
    "engine_displacement_l_min": "engine_l_min",
    "engine_displacement_l_max": "engine_l_max",
}
_MAX_LIST_VALUES = 50


def _scalar_str(v) -> str:
    if v is None or isinstance(v, bool):
        return "1" if v is True else ""
    if isinstance(v, float) and v.is_integer():
        return str(int(v))
    return str(v).strip()


def normalize_filters_for_url(filters) -> list[tuple[str, str]]:
    """Ordered (key, value) pairs for ``urlencode``; empties, ``page`` and aliases resolved."""
    if not isinstance(filters, dict):
        return []
    pairs: list[tuple[str, str]] = []
    for raw_key in sorted(filters.keys(), key=str):
        key = _URL_KEY_ALIASES.get(str(raw_key).strip(), str(raw_key).strip())
        if not key or key in _SKIP_URL_KEYS:
            continue
        v = filters[raw_key]
        values = v if isinstance(v, (list, tuple)) else [v]
        for x in list(values)[:_MAX_LIST_VALUES]:
            if isinstance(x, (dict, list, tuple)):
                continue
            s = _scalar_str(x)
            if s:
                pairs.append((key, s))
    return pairs


def listings_url_for_filters(filters, path: str = LISTINGS_PATH) -> str:
    """``/listings?make=Honda&model=Civic...`` for a stored filter object ("Run again")."""
    pairs = normalize_filters_for_url(filters)
    if not pairs:
        return path
    return f"{path}?{urlencode(pairs)}"


def _first_value(filters: dict, *keys):
    for k in keys:
        v = filters.get(k)
        if isinstance(v, (list, tuple)):
            v = [x for x in v if _scalar_str(x)]
            if v:
                return v
        elif _scalar_str(v):
            return v
    return None


def _join(v) -> str:
    if isinstance(v, (list, tuple)):
        return "/".join(_scalar_str(x) for x in v if _scalar_str(x))
    return _scalar_str(v)


def _as_number(v):
    if isinstance(v, (list, tuple)):
        v = v[0] if v else None
    try:
        return float(str(v).replace(",", "").replace("$", ""))
    except (TypeError, ValueError):
        return None


def _usd(n: float) -> str:
    return f"${int(round(n)):,}"


def _int_text(n: float) -> str:
    return f"{int(round(n)):,}"


def describe_search_filters(filters, query_text: str | None = None) -> str:
    """One-line summary: make/model/trim, year range, price range, mileage, condition,
    zip+radius and the query text. Falls back to "All listings"."""
    f = filters if isinstance(filters, dict) else {}
    bits: list[str] = []

    vehicle = [
        _join(v)
        for v in (_first_value(f, "make"), _first_value(f, "model"), _first_value(f, "trim"))
        if v
    ]
    if vehicle:
        bits.append(" ".join(vehicle))

    y_min = _as_number(_first_value(f, "min_year", "year_min"))
    y_max = _as_number(_first_value(f, "max_year", "year_max"))
    if y_min is not None and y_max is not None:
        bits.append(f"{int(y_min)}–{int(y_max)}" if y_min != y_max else str(int(y_min)))
    elif y_min is not None:
        bits.append(f"{int(y_min)} or newer")
    elif y_max is not None:
        bits.append(f"{int(y_max)} or older")

    p_min = _as_number(_first_value(f, "min_price"))
    p_max = _as_number(_first_value(f, "max_price"))
    if p_min is not None and p_max is not None:
        bits.append(f"{_usd(p_min)}–{_usd(p_max)}")
    elif p_max is not None:
        bits.append(f"under {_usd(p_max)}")
    elif p_min is not None:
        bits.append(f"over {_usd(p_min)}")

    m_max = _as_number(_first_value(f, "max_mileage"))
    if m_max is not None:
        bits.append(f"under {_int_text(m_max)} mi")

    cond = _scalar_str(_first_value(f, "inventory_condition")).lower()
    cpo = _scalar_str(_first_value(f, "cpo_only")).lower()
    if cpo in ("1", "true", "yes", "on") or cond == "cpo":
        bits.append("Certified pre-owned")
    elif cond in ("new", "used"):
        bits.append(cond.capitalize())

    for key, label in (
        ("fuel_type", None),
        ("body_style", None),
        ("drivetrain", None),
        ("transmission", None),
        ("exterior_color", "exterior"),
        ("interior_color", "interior"),
        ("package", None),
    ):
        v = _first_value(f, key)
        if v:
            text = _join(v)
            bits.append(f"{text} {label}" if label else text)

    zip_code = _scalar_str(_first_value(f, "zip_code"))
    radius = _as_number(_first_value(f, "radius"))
    if zip_code and radius:
        bits.append(f"within {_int_text(radius)} mi of {zip_code}")
    elif zip_code:
        bits.append(f"near {zip_code}")

    q = _scalar_str(query_text) or _scalar_str(_first_value(f, "q"))
    if q:
        bits.append(f"“{q}”")

    return " · ".join(bits) if bits else "All listings"


def humanize_when(created_at, now: datetime | None = None) -> str:
    """'just now' / '5 minutes ago' / 'yesterday' / 'Sep 12' / 'Sep 12, 2025'."""
    dt = parse_created_at(created_at)
    if dt is None:
        return ""
    ref = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    seconds = (ref - dt).total_seconds()
    if seconds < 0:
        seconds = 0
    if seconds < 60:
        return "just now"
    minutes = int(seconds // 60)
    if minutes < 60:
        return f"{minutes} minute{'s' if minutes != 1 else ''} ago"
    hours = int(seconds // 3600)
    if hours < 24:
        return f"{hours} hour{'s' if hours != 1 else ''} ago"
    days = int(seconds // 86400)
    if days == 1:
        return "yesterday"
    if days < 7:
        return f"{days} days ago"
    if dt.year == ref.year:
        return dt.strftime("%b %d").replace(" 0", " ")
    return dt.strftime("%b %d, %Y").replace(" 0", " ")


def decorate_search_rows(rows: list[dict], now: datetime | None = None) -> list[dict]:
    """Add ``label``, ``url`` and ``when`` to repo rows (recent or saved searches)."""
    out: list[dict] = []
    for r in rows or []:
        filters = r.get("filters") if isinstance(r.get("filters"), dict) else {}
        item = dict(r)
        item["filters"] = filters
        item["label"] = describe_search_filters(filters, r.get("query_text"))
        item["url"] = listings_url_for_filters(filters)
        item["when"] = humanize_when(r.get("created_at"), now=now)
        out.append(item)
    return out
