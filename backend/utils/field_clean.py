"""
Normalize scraped / legacy placeholder strings to None for SQLite + prompts.

Single source of truth for "empty" text so parsers, upserts, embeddings, and LLM
context do not propagate N/A poison.
"""
from __future__ import annotations

import json
import re
from typing import Any

# Lowercased set of junk tokens → NULL in DB / omit from embeddings
# Dealer DMS / VDP boilerplate (same idea as ``car_serialize._MANUFACTURER_SPEC_RE``).
_MANUFACTURER_SPEC_JUNK_RE = re.compile(
    r"see\s+manufacturer|manufacturer\s+specifications|refer\s+to\s+manufacturer",
    re.IGNORECASE,
)


_PLACEHOLDER_LOWER: frozenset[str] = frozenset(
    {
        "",
        "n/a",
        "na",
        "null",
        "none",
        "unknown",
        "undefined",
        "-",
        "—",
        "--",
        "---",
        "tbd",
        "not specified",
        "unspecified",
    }
)


def is_effectively_empty(val: Any) -> bool:
    """True for None, whitespace-only, or known placeholder strings."""
    if val is None:
        return True
    if isinstance(val, (int, float)):
        return False
    s = str(val).strip()
    if not s:
        return True
    return s.lower() in _PLACEHOLDER_LOWER


def is_spec_overlay_junk(val: Any) -> bool:
    """True for empty/placeholder strings or manufacturer-spec boilerplate (not a real listing value)."""
    if is_effectively_empty(val):
        return True
    s = str(val).strip()
    return bool(_MANUFACTURER_SPEC_JUNK_RE.search(s))


def normalize_optional_str(val: Any, *, max_len: int | None = None) -> str | None:
    """
    Return a clean string or None. Never returns placeholder tokens.
    """
    if is_effectively_empty(val):
        return None
    s = str(val).strip()
    if is_spec_overlay_junk(s):
        return None
    if max_len is not None and len(s) > max_len:
        s = s[:max_len]
    return s


def normalize_optional_url(val: Any) -> str | None:
    """HTTP(S) URLs only; empty or placeholders → None."""
    u = normalize_optional_str(val)
    if not u:
        return None
    low = u.lower()
    if low.startswith("http://") or low.startswith("https://"):
        return u
    return None


def clean_car_row_dict(d: dict[str, Any]) -> dict[str, Any]:
    """
    Apply normalization to typical cars.* string columns in-place copy.
    Used before SQLite upsert and before semantic indexing.
    """
    out = dict(d)
    string_cols = (
        "trim",
        "zip_code",
        "transmission",
        "drivetrain",
        "exterior_color",
        "interior_color",
        "fuel_type",
        "dealer_url",
        "carfax_url",
        "title",
        "make",
        "model",
        "dealer_name",
        "stock_number",
        "dealer_id",
        "image_url",
        "source_url",
        "body_style",
        "engine_description",
        "condition",
        "description",
        "model_full_raw",
    )
    for k in string_cols:
        if k not in out:
            continue
        if k in ("dealer_url", "carfax_url", "image_url", "source_url"):
            out[k] = normalize_optional_url(out.get(k))
        else:
            out[k] = normalize_optional_str(out.get(k))

    # Some feeds store boolean-ish junk in ``condition`` (not a real listing value).
    _cv = out.get("condition")
    if _cv is not None and str(_cv).strip().lower() in ("0", "false", "no", "off"):
        out["condition"] = None

    for num_key in ("mpg_city", "mpg_highway", "cylinders"):
        if num_key not in out:
            continue
        v = out.get(num_key)
        if v is None or v == "":
            out[num_key] = None
            continue
        try:
            n = int(float(str(v).replace(",", "").strip()))
        except (TypeError, ValueError):
            out[num_key] = None
            continue
        if num_key == "cylinders":
            out[num_key] = n if n >= 0 else None
        else:
            out[num_key] = n if n > 0 else None

    pkg = out.get("packages")
    if pkg is not None:
        if isinstance(pkg, dict):
            try:
                out["packages"] = json.dumps(pkg, ensure_ascii=False)
            except (TypeError, ValueError):
                out["packages"] = None
        elif isinstance(pkg, str):
            s = pkg.strip()
            if not s or s.lower() in ("{}", "[]", "null"):
                out["packages"] = None
            else:
                out["packages"] = s[:800000]
        else:
            out["packages"] = None
    return out


def display_str(val: Any, *, fallback: str = "unknown") -> str:
    """For LLM prompts: never show N/A; use fallback for missing."""
    if is_effectively_empty(val):
        return fallback
    return str(val).strip()


def build_inventory_chroma_document(car: dict[str, Any]) -> str:
    """
    Human-readable summary for embedding; skips null/junk fields.
    """
    c = clean_car_row_dict(car)
    parts: list[str] = []
    for label, key in (
        ("VIN", "vin"),
        ("Year", "year"),
        ("Make", "make"),
        ("Model", "model"),
        ("Trim", "trim"),
        ("Title", "title"),
        ("Dealer", "dealer_name"),
        ("ZIP", "zip_code"),
        ("Transmission", "transmission"),
        ("Drivetrain", "drivetrain"),
        ("Fuel", "fuel_type"),
        ("Exterior", "exterior_color"),
        ("Interior", "interior_color"),
        ("Body", "body_style"),
        ("Engine", "engine_description"),
        ("Condition", "condition"),
    ):
        v = c.get(key)
        if v is None or (isinstance(v, str) and not v.strip()):
            continue
        parts.append(f"{label}: {v}")
    price = c.get("price")
    if price is not None:
        try:
            p = float(price)
            if p > 0:
                parts.append(f"Price: {int(round(p))}")
        except (TypeError, ValueError):
            pass
    mil = c.get("mileage")
    if mil is not None:
        try:
            mi = int(mil)
            if mi > 0:
                parts.append(f"Mileage: {mi}")
        except (TypeError, ValueError):
            pass
    cyl = c.get("cylinders")
    if cyl is not None:
        try:
            if int(cyl) > 0:
                parts.append(f"Cylinders: {int(cyl)}")
        except (TypeError, ValueError):
            pass
    eng = c.get("engine_l")
    if eng is not None and str(eng).strip() and not is_effectively_empty(eng):
        parts.append(f"Engine L: {eng}")
    desc = c.get("description")
    if desc and len(str(desc)) > 20:
        parts.append(str(desc)[:500])
    pkg = c.get("packages")
    if pkg and str(pkg).strip() not in ("{}", "[]", "null"):
        try:
            if isinstance(pkg, str):
                pj = json.loads(pkg)
            else:
                pj = pkg
            if isinstance(pj, dict):
                obs = pj.get("observed_features") or []
                if isinstance(obs, list) and obs:
                    parts.append("Observed: " + "; ".join(str(x) for x in obs[:8]))
        except (json.JSONDecodeError, TypeError):
            pass
    text = " ".join(parts)
    return text[:8000] if text else "vehicle"


def format_mpg_city_highway_display(mpg_city: Any, mpg_highway: Any) -> str | None:
    """Build ``19 city / 26 highway MPG`` from DB integers when EPA aggregate string is absent."""
    try:
        c = int(mpg_city) if mpg_city is not None and str(mpg_city).strip() != "" else None
    except (TypeError, ValueError):
        c = None
    try:
        h = int(mpg_highway) if mpg_highway is not None and str(mpg_highway).strip() != "" else None
    except (TypeError, ValueError):
        h = None
    if c is not None and h is not None and c > 0 and h > 0:
        return f"{c} City / {h} Hwy"
    if c is not None and c > 0:
        return f"{c} City MPG"
    if h is not None and h > 0:
        return f"{h} Hwy MPG"
    return None


def compute_data_quality_score(car: dict[str, Any]) -> float:
    """
    0–100 heuristic: completeness + no junk strings + image + price.
    """
    c = clean_car_row_dict(car)
    pts = 0.0
    max_pts = 0.0

    def add(w: float, ok: bool) -> None:
        nonlocal pts, max_pts
        max_pts += w
        if ok:
            pts += w

    add(12, bool(c.get("vin")) and not str(c.get("vin", "")).lower().startswith("unknown"))
    add(10, bool(c.get("title")))
    add(8, c.get("year") is not None)
    add(8, bool(c.get("make")))
    add(8, bool(c.get("model")))
    add(6, bool(c.get("trim")))
    add(8, bool(c.get("price")) and float(c.get("price") or 0) > 0)
    add(5, c.get("mileage") is not None and int(c.get("mileage") or 0) >= 0)
    add(6, bool(c.get("transmission")))
    add(6, bool(c.get("drivetrain")))
    add(5, bool(c.get("fuel_type")))
    add(4, bool(c.get("exterior_color")))
    add(4, bool(c.get("interior_color")))
    add(5, bool(c.get("zip_code")))
    add(5, _has_real_image(c))
    add(4, bool(c.get("engine_l")) or c.get("cylinders") not in (None, 0))
    add(4, c.get("mpg_city") is not None and c.get("mpg_highway") is not None)
    if max_pts <= 0:
        return 0.0
    return round(100.0 * pts / max_pts, 2)


def _has_real_image(car: dict) -> bool:
    u = car.get("image_url")
    if u and str(u).strip().startswith("http"):
        return True
    g = car.get("gallery")
    if isinstance(g, list):
        return any(isinstance(x, str) and x.startswith("http") for x in g)
    if isinstance(g, str) and "http" in g:
        return True
    return False


_JUNK_URL = re.compile(r"^(n/?a|none|null|unknown|[-—]+)$", re.IGNORECASE)


def clean_url_for_db(val: Any) -> str | None:
    u = normalize_optional_str(val)
    if not u:
        return None
    if _JUNK_URL.match(u):
        return None
    return normalize_optional_url(u)
