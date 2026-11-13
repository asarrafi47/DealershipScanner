"""Inventory rarity score — "how unusual is this car among the cars we track?"

Scores 0-10 from scarcity WITHIN our own active inventory (never a market-wide
claim): model scarcity, trim share within the model, paint-family share within
the model, special-order/visible-option evidence from the vision scan
(BMW Individual plaques, premium-audio speaker grilles, carbon trim, wraps),
and sticker option richness. Read-time only — mirrors the reference-store
rule: nothing here is ever written onto cars.

The score is honest about its frame: the label says "in our inventory", and a
car only earns points a shopper could verify (a badge in a photo, a printed
sticker line, an actual count of siblings on our lots).
"""
from __future__ import annotations

import json
import logging
import threading
import time
from collections import Counter
from typing import Any

from backend.utils.swr_refresh import RETRY_AFTER_S, BackgroundRefresher

logger = logging.getLogger(__name__)

_CACHE_TTL_S = 600.0

# Evidence keywords scanned against the vision summary's equipment/packages/
# notes text. Grouped by how much genuine scarcity each class signals.
_SPECIAL_ORDER_TERMS = (
    "bmw individual", "individual manufaktur", "manufaktur", "paint to sample",
    "pts ", "porsche exclusive", "audi exclusive", "frozen ", "matte paint",
    "frozen paint",
)
_PREMIUM_AUDIO_TERMS = (
    "harman", "burmester", "bang & oluf", "bang and oluf", "bowers", "b&w",
    "mark levinson", "meridian", "mcintosh", "focal", "naim", "revel",
    "els studio", "krell", "lexicon",
)
_CARBON_TERMS = ("carbon fiber", "carbon-fiber", "carbon roof", "carbon trim")
_WRAP_TERMS = ("aftermarket wrap", "vinyl wrap", "wrapped", "ppf")


def _norm(s: Any) -> str:
    return " ".join(str(s or "").split()).lower()


def _color_family(color: Any, make: Any) -> str | None:
    """Paint-family bucket so spelling variants share one scarcity pool."""
    try:
        from backend.utils.interior_color_buckets import infer_paint_color_buckets

        buckets = infer_paint_color_buckets(str(color or "") or None, str(make or "") or None)
        return buckets[0] if buckets else None
    except Exception:
        return None


# One row per distinct (make, model, trim, exterior_color): the database counts, Python
# only normalizes the ~50k groups. The old loop fetched every active car (214k rows) and
# held the interpreter for 700 to 900 ms in a background thread, which stalled the request
# threads for the same time (web speed P6, server-causes 4.7).
_COUNTS_SQL = """
    SELECT make, model, trim, exterior_color, COUNT(*)
    FROM cars
    WHERE COALESCE(listing_active, 1) = 1
      AND TRIM(COALESCE(make, '')) != ''
      AND TRIM(COALESCE(model, '')) != ''
    GROUP BY make, model, trim, exterior_color
"""


def _counts_from_groups(groups) -> dict[str, Counter]:
    """Frequency maps from ``(make, model, trim, exterior_color, n)`` rows.

    The same four Counters the per-car loop produced: a group of ``n`` identical
    cars adds ``n`` where the loop added 1 ``n`` times.
    """
    model_c: Counter = Counter()
    trim_c: Counter = Counter()
    color_c: Counter = Counter()
    color_known_c: Counter = Counter()
    for make, model, trim, color, n in groups:
        n = int(n or 0)
        if n <= 0:
            continue
        mk_md = (_norm(make), _norm(model))
        model_c[mk_md] += n
        t = _norm(trim)
        if t:
            trim_c[(*mk_md, t)] += n
        fam = _color_family(color, make)
        if fam:
            color_known_c[mk_md] += n
            color_c[(*mk_md, fam)] += n
    return {
        "model": model_c,
        "trim": trim_c,
        "color": color_c,
        "color_known": color_known_c,
    }


def _load_inventory_counts() -> dict[str, Counter]:
    """Counted in SQL, then normalized per group -> the frequency maps every score reads."""
    from backend.db.inventory_db import db_conn

    with db_conn() as conn:
        groups = conn.execute(_COUNTS_SQL).fetchall()
    return _counts_from_groups(groups)


# (loaded_at on the monotonic clock, counts) or None before the first load. Read without
# a lock (one reference); written under ``_counts_lock``.
_counts_cache: tuple[float, dict[str, Counter]] | None = None
_counts_lock = threading.Lock()
_counts_load_lock = threading.Lock()  # held for the whole cold load
_counts_refresher = BackgroundRefresher("rarity-counts")


def _store_counts(counts: dict[str, Counter]) -> None:
    global _counts_cache
    with _counts_lock:
        _counts_cache = (time.monotonic(), counts)


def _refresh_counts_in_background() -> None:
    global _counts_cache
    try:
        counts = _load_inventory_counts()
    except Exception:
        # Keep serving the old counts; try again after RETRY_AFTER_S, not on every call.
        with _counts_lock:
            hit = _counts_cache
            if hit is not None:
                _counts_cache = (time.monotonic() - _CACHE_TTL_S + RETRY_AFTER_S, hit[1])
        raise
    _store_counts(counts)


def get_inventory_counts() -> dict[str, Counter]:
    """Rarity frequency maps; never waits for a reload once a value exists.

    Cold: loaded inline under a lock (concurrent first callers share one load). After
    ``_CACHE_TTL_S`` the old maps are returned and ONE background reload starts.
    """
    hit = _counts_cache
    if hit is None:
        with _counts_load_lock:
            hit = _counts_cache
            if hit is None:
                counts = _load_inventory_counts()
                _store_counts(counts)
                return counts
        return hit[1]
    if (time.monotonic() - hit[0]) >= _CACHE_TTL_S:
        _counts_refresher.kick(_refresh_counts_in_background)
    return hit[1]


def warm_inventory_counts() -> None:
    """Load the maps now (process start); a no-op when they are already loaded."""
    if _counts_cache is None:
        get_inventory_counts()


def _vision_blob(vision_summary: Any) -> str:
    """Equipment + packages + notes from a car_image_text summary as one string."""
    if not isinstance(vision_summary, dict):
        return ""
    parts: list[str] = []
    for key in ("equipment", "packages", "notes", "exterior_color_seen"):
        v = vision_summary.get(key)
        if isinstance(v, str):
            parts.append(v)
        elif isinstance(v, list):
            parts.extend(str(x) for x in v)
    return _norm(" ".join(parts))


def _special_signals(blob: str) -> tuple[float, list[str]]:
    """(points 0-3, reasons) from vision-scan evidence."""
    pts = 0.0
    reasons: list[str] = []
    if any(t in blob for t in _SPECIAL_ORDER_TERMS):
        pts += 1.5
        reasons.append("Special-order paint/trim evidence in photos")
    if any(t in blob for t in _PREMIUM_AUDIO_TERMS):
        pts += 0.5
        reasons.append("Branded premium audio visible in photos")
    if any(t in blob for t in _CARBON_TERMS):
        pts += 0.5
        reasons.append("Carbon-fiber parts visible in photos")
    if any(t in blob for t in _WRAP_TERMS):
        pts += 0.5
        reasons.append("Aftermarket wrap/PPF noted (one-off appearance)")
    return min(pts, 3.0), reasons


def _option_richness(vision_summary: Any) -> tuple[float, list[str]]:
    """(points 0-1, reasons) from the sticker's priced options, when read."""
    if not isinstance(vision_summary, dict):
        return 0.0, []
    priced = vision_summary.get("priced_options")
    if isinstance(priced, str):
        try:
            priced = json.loads(priced)
        except (TypeError, ValueError):
            priced = None
    if not isinstance(priced, list) or not priced:
        return 0.0, []
    n = len(priced)
    if n >= 8:
        return 1.0, [f"Heavily optioned per window sticker ({n} priced options)"]
    if n >= 4:
        return 0.5, [f"Well optioned per window sticker ({n} priced options)"]
    return 0.0, []


def rarity_for_car(
    car: dict[str, Any],
    *,
    vision_summary: Any = None,
    counts: dict[str, Counter] | None = None,
) -> dict[str, Any] | None:
    """Score this car's rarity within our active inventory.

    Returns {score, label, reasons, model_count} or None when the car cannot
    be scored (no make/model). ``counts`` is injectable for tests.
    """
    mk, md = _norm(car.get("make")), _norm(car.get("model"))
    if not mk or not md:
        return None
    c = counts if counts is not None else get_inventory_counts()
    mk_md = (mk, md)
    n_model = int(c["model"].get(mk_md, 0))
    if n_model <= 0:
        # This car isn't in the active index (sold/new row) — count it as 1.
        n_model = 1

    score = 0.0
    reasons: list[str] = []

    # 1. Model scarcity (0-5.5): how many of this nameplate do we track at
    #    all? A one-of-one model is the strongest single rarity fact we can
    #    state, so it alone must clear the display threshold.
    if n_model <= 1:
        score += 5.5
        reasons.append("Only one of this model in our inventory")
    elif n_model <= 3:
        score += 4.0
        reasons.append(f"One of just {n_model} of this model in our inventory")
    elif n_model <= 10:
        score += 2.5
        reasons.append(f"Only {n_model} of this model in our inventory")
    elif n_model <= 30:
        score += 1.0

    # 2. Trim share within the model (0-2), only when the pool is big enough
    #    for a share to mean anything.
    t = _norm(car.get("trim"))
    if t and n_model >= 5:
        n_trim = int(c["trim"].get((*mk_md, t), 0)) or 1
        share = n_trim / n_model
        if share <= 0.05:
            score += 2.0
            reasons.append(f"Rarest trim: {n_trim} of {n_model} {str(car.get('model') or '').strip()}s")
        elif share <= 0.10:
            score += 1.5
            reasons.append(f"Rare trim: {n_trim} of {n_model} in our inventory")
        elif share <= 0.20:
            score += 1.0
        elif share <= 0.35:
            score += 0.5

    # 3. Paint-family share within the model (0-2), same pool-size guard.
    fam = _color_family(car.get("exterior_color"), car.get("make"))
    n_colored = int(c["color_known"].get(mk_md, 0))
    if fam and n_colored >= 10:
        n_fam = int(c["color"].get((*mk_md, fam), 0)) or 1
        share = n_fam / n_colored
        if share <= 0.03:
            score += 2.0
            reasons.append(f"Rarest color family: {n_fam} of {n_colored} with a known color")
        elif share <= 0.07:
            score += 1.5
            reasons.append("Rare color for this model in our inventory")
        elif share <= 0.15:
            score += 1.0
        elif share <= 0.25:
            score += 0.5

    # 4. Vision-scan evidence (0-3) + 5. sticker option richness (0-1).
    sp, sp_reasons = _special_signals(_vision_blob(vision_summary))
    score += sp
    reasons.extend(sp_reasons)
    orich, orich_reasons = _option_richness(vision_summary)
    score += orich
    reasons.extend(orich_reasons)

    score = round(min(score, 10.0), 1)
    if score >= 8:
        label = "Exceptionally rare in our inventory"
    elif score >= 5.5:
        label = "Very rare in our inventory"
    elif score >= 3.5:
        label = "Uncommon in our inventory"
    elif score >= 2:
        label = "Somewhat uncommon"
    else:
        label = "Common"
    return {
        "score": score,
        "label": label,
        "reasons": reasons,
        "model_count": n_model,
    }


def vision_summary_for_car(car_id: Any) -> dict[str, Any] | None:
    """This car's vision-scan summary row (version 100), or None."""
    try:
        cid = int(car_id)
    except (TypeError, ValueError):
        return None
    try:
        from backend.db.inventory_db import db_conn

        with db_conn() as conn:
            row = conn.execute(
                "SELECT summary FROM car_image_text WHERE car_id = ? AND version = 100",
                (cid,),
            ).fetchone()
        if not row or not row[0]:
            return None
        s = row[0]
        if isinstance(s, str):
            s = json.loads(s)
        return s if isinstance(s, dict) else None
    except Exception as exc:
        logger.debug("vision summary lookup failed for car %s: %s", car_id, exc)
        return None
