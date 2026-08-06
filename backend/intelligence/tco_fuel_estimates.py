"""
TCO fuel helpers: city/highway MPG average and fuel-tank capacity lookup.

The tank size returned here is multiplied by a live fuel price and shown to a
shopper as the cost of a fill-up, so :func:`resolve_fuel_tank_gallons` returns a
number quoted from a page we can name, or ``None`` — never an estimate, and
never a value one of our own scripts derived. See the block comment below for
what it used to return instead and how much of the column that was.
"""

from __future__ import annotations

import json
from functools import lru_cache
from typing import Any

# NOT sourced, and still live. These two fall back to a flat constant when the
# listing carries no EPA economy figure, and they feed the 5-year fuel/energy
# projection on the car page. 14,926 of the 72,504 active listings (measured
# 2026-07-31) have neither an EPA city nor an EPA highway MPG, so they are
# costed at a flat 25 MPG that describes no particular car. Fixing that the same
# way the tank was fixed means returning None here too, which only changes what
# a shopper sees once the template and car_page.js stop substituting their own
# 25 — see `data-car-mpg="{{ car.tco_avg_mpg if car.tco_avg_mpg else 25 }}"` in
# frontend/templates/car.html and resolveTcoAvgMpg() in frontend/static/car_page.js.
# Left alone deliberately rather than half-changed.
_TCO_DEFAULT_MPG = 25.0
_EPA_KWH_PER_GALLON = 33.7
_TCO_DEFAULT_EV_KWH_PER_100 = 33.7

#: Band a US-market fuel tank has to fall in to be believable; outside it the
#: stored value is a unit or parse error rather than a tank. Deliberately wide —
#: it is a sanity floor/ceiling, not a spec — and out-of-band values are dropped,
#: never clamped to the edge.
_MIN_PLAUSIBLE_TANK_GAL = 5.0
_MAX_PLAUSIBLE_TANK_GAL = 60.0


# ---------------------------------------------------------------------------
# Read-time catalog join, restricted to values we can quote
#
# The fuel-tank figure is read from ``epa_extended_specs`` at read time. Nothing
# is copied into ``cars``; catalog facts stay in the catalog.
#
# ``backend.intelligence.ev_range_estimates`` used to share this lookup for
# ``ev_range_miles`` and no longer reads the table at all — see the removal note
# there. This is the table's last shopper-facing reader in this package.
#
# WHY THIS DOES NOT GO THROUGH ``lookup_epa_extended_specs``
# (``backend.enrichment.knowledge_engine``), which is what it used until
# 2026-07-31: that function, and its ``…_by_master_id`` sibling, both finish by
# calling ``_merge_ai_model_specs`` (knowledge_engine.py:829 and :602). That
# fills still-missing keys from ``ai_model_specs``, and ``fuel_tank_gal`` is in
# its ``_AI_SPEC_COLUMNS`` list. Every row of ``ai_model_specs`` carries
# ``source_host='ai-research'`` (2,147 rows, 314 with a tank) and every row of
# ``ai_engine_specs`` carries ``source_host='ai-engine-research'`` (888 rows,
# 847 with a tank) — measured 2026-07-31. A function named
# ``sourced_extended_specs`` was returning those as sourced numbers. There is no
# way to tell afterwards which key the AI merge supplied, because it fills by
# ``setdefault`` into the same flat dict, so the fix is to stop calling it and
# read the table directly.
#
# WHY THE TABLE ALONE IS STILL NOT ENOUGH. ``epa_extended_specs.fuel_tank_gal``
# is populated 99.9% by our own derivation, not by the scrape. Counted over all
# 48,658 rows that carry a tank (2026-07-31), by the ``specs_json ->>
# 'fuel_tank_source'`` flag the writer left behind:
#
#     body_style      31,977   a body-class lookup table — 8 distinct values
#                              across all 31,977 rows
#     model_defaults   9,025   a per-nameplate default — 33 distinct values
#     class_default    7,023   one single number, 15.5, for every one of them
#     trim_adds          383   parsed out of the LLM ``adds_by_trim`` bullets,
#                              i.e. the exact text the provenance gate exists
#                              to keep off the page
#     (no flag)          250   not attributed to any of the fillers; 59 of them
#                              have the number in a page extraction
#
# A ``source_url`` on the row does NOT vouch for the tank: 38,525 of those rows
# have one, because the row WAS scraped — for horsepower, curb weight and 0-60.
# The tank was written over the top of it afterwards. Only 59 rows in the whole
# table have a tank in ``specs_json['pages'][*]['extracted']['fuel_tank_gal']``,
# which is the one place that records "this page said this", and in all 59 the
# extraction agrees with the stored column, so nothing is lost to the match
# below — the scrape simply never read tank sizes.
#
# Hence :func:`quoted_extended_spec`: a value is admitted only when it appears
# in a page extraction whose URL we can print. Everything else — the AI tables,
# the body-class table, the flat 15.5, the trim-adds parse — is silence.
# ---------------------------------------------------------------------------

#: The only field this module will quote.
#:
#: ``ev_range_miles`` is deliberately NOT here. It passes the quote test — 14,503
#: of the 14,884 rows carrying one have the number in a page extraction — and it
#: is still unusable, because the number those pages print is a TOTAL driving
#: range: 400 miles of "all-electric range" for a Jeep Wrangler 4xe, 550 for a
#: BMW 530e, 402 for a hydrogen Mirai. Being quotable is necessary, not
#: sufficient. See the removal note in ``backend.intelligence.ev_range_estimates``
#: for the before/after count.
_QUOTABLE_SPEC_FIELDS = ("fuel_tank_gal",)

#: Two figures count as the same quote when they round to the same tenth. The
#: page extraction and the stored column are the same number written twice
#: (float round-trip through JSON), not two independent measurements.
_QUOTE_MATCH_TOLERANCE = 0.05


def quoted_extended_spec(
    car: dict[str, Any] | None, field: str
) -> tuple[float, str] | None:
    """
    ``(value, source_url)`` for *field* when the number is quoted from a page.

    Returns ``None`` — meaning "render nothing" — unless ALL of this holds:

    * an ``epa_extended_specs`` row resolves for this car (resolved catalog link
      ``cars.epa_master_id`` first, an exact FK; then exact
      (year, make, model, trim); then any row for the (year, make, model));
    * that row's stored column is non-null;
    * and the same number appears in ``specs_json['pages'][*]['extracted'][field]``
      under a page URL, so the claim can be attributed to a document we hold.

    The AI spec tables are not consulted at all — this reads
    ``epa_extended_specs`` directly rather than through
    ``knowledge_engine.lookup_epa_extended_specs``, which merges them (see the
    block comment above).

    What this does NOT establish: that the page is about THIS car. The rows are
    MODEL-level scrapes of nameplate pages, so a 2005 Silverado 2500HD row
    quotes ``caranddriver.com/chevrolet/silverado``, a page about the 2026
    Silverado EV. Callers must keep their own drivetrain and physical-bound
    checks — see :func:`resolve_fuel_tank_gallons` — because this function only
    proves the number was read somewhere, not that it is true here.
    """
    if not car or field not in _QUOTABLE_SPEC_FIELDS:
        return None
    quoted = _quoted_specs_for_car(car)
    hit = quoted.get(field)
    if hit is None:
        return None
    return hit


def _quoted_specs_for_car(car: dict[str, Any]) -> dict[str, tuple[float, str]]:
    """
    Every quotable field this car's extended-specs row can attribute to a page.

    The FK row is tried first and the (year, make, model[, trim]) lookup is tried
    when it yields no quote — not only when it yields no row at all, which is
    where the old lookup stopped. Falling through on "no quote" reaches strictly
    more rows, so nothing is hidden by the FK happening to point at a row whose
    tank was written by a filler.
    """
    try:
        linked_id = car.get("epa_master_id")
        try:
            mid = int(linked_id) if linked_id not in (None, "") else 0
        except (TypeError, ValueError):
            mid = 0
        if mid > 0:
            found = _quoted_specs_by_master_id(mid)
            if found:
                return dict(found)
        year = _parse_year(car.get("year"))
        make = str(car.get("make") or "").strip()
        model = str(car.get("model") or "").strip()
        if year is None or not make or not model:
            return {}
        trim = str(car.get("trim") or "").strip()
        return dict(_quoted_specs_by_ymmt(year, make, model, trim))
    except Exception:
        return {}


# Cache sizes are set against the live key counts so a full pass cannot thrash:
# the 72,504 active listings carry 5,564 distinct ``epa_master_id`` values and
# 13,340 distinct (year, make, model, trim) combinations (measured 2026-07-31).


@lru_cache(maxsize=8192)
def _quoted_specs_by_master_id(epa_master_id: int) -> tuple[tuple[str, tuple[float, str]], ...]:
    return _quoted_specs_from_query(
        "WHERE epa_master_id=?",
        (epa_master_id,),
    )


@lru_cache(maxsize=16384)
def _quoted_specs_by_ymmt(
    year: int, make: str, model: str, trim: str
) -> tuple[tuple[str, tuple[float, str]], ...]:
    if trim:
        found = _quoted_specs_from_query(
            "WHERE year=? AND lower(make)=lower(?) AND lower(model)=lower(?) AND lower(trim)=lower(?)",
            (year, make, model, trim),
        )
        if found:
            return found
    return _quoted_specs_from_query(
        "WHERE year=? AND lower(make)=lower(?) AND lower(model)=lower(?)",
        (year, make, model),
    )


def _quoted_specs_from_query(
    where: str, params: tuple[Any, ...]
) -> tuple[tuple[str, tuple[float, str]], ...]:
    cols = ", ".join(_QUOTABLE_SPEC_FIELDS)
    conn = None
    try:
        from backend.db.inventory_db import get_conn

        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            f"SELECT {cols}, specs_json FROM epa_extended_specs {where} LIMIT 1",
            params,
        )
        row = cur.fetchone()
    except Exception:
        return ()
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
    if not row:
        return ()
    stored = dict(zip(_QUOTABLE_SPEC_FIELDS, row))
    return _quotable_from_specs_json(stored, row[len(_QUOTABLE_SPEC_FIELDS)])


def _quotable_from_specs_json(
    stored: dict[str, Any], specs_json: Any
) -> tuple[tuple[str, tuple[float, str]], ...]:
    """
    Pair each stored value with the page URL that reported the same number.

    Only ``pages[*].extracted`` counts. The duplicate copy of each field at the
    top level of ``specs_json`` does not: that is where the default-fillers
    wrote, so a top-level ``fuel_tank_gal`` of 15.5 sits next to a ``pages``
    array whose extractions never mention a tank.
    """
    payload = _load_specs_json(specs_json)
    pages = payload.get("pages")
    if not isinstance(pages, list):
        return ()
    out: dict[str, tuple[float, str]] = {}
    for page in pages:
        if not isinstance(page, dict):
            continue
        url = str(page.get("url") or "").strip()
        extracted = page.get("extracted")
        if not url or not isinstance(extracted, dict):
            continue
        for field, value in stored.items():
            if field in out or value is None:
                continue
            quoted = _positive_float(extracted.get(field))
            kept = _positive_float(value)
            if quoted is None or kept is None:
                continue
            if abs(quoted - kept) <= _QUOTE_MATCH_TOLERANCE:
                out[field] = (kept, url)
    return tuple(sorted(out.items()))


def _load_specs_json(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, (str, bytes)):
        try:
            parsed = json.loads(value)
        except (ValueError, TypeError):
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def clear_quoted_extended_spec_cache() -> None:
    """Drop the memoized row lookups (tests, and after a catalog reload)."""
    _quoted_specs_by_master_id.cache_clear()
    _quoted_specs_by_ymmt.cache_clear()


def average_mpg_city_highway(mpg_city: Any, mpg_highway: Any) -> float | None:
    """
    Arithmetic mean of EPA city and highway MPG when both are present.

    If only one is available, returns that value. Returns ``None`` when neither is usable.
    """
    city = _positive_float(mpg_city)
    highway = _positive_float(mpg_highway)
    if city is not None and highway is not None:
        return (city + highway) / 2.0
    if city is not None:
        return city
    if highway is not None:
        return highway
    return None


def resolve_tco_avg_mpg(car: dict[str, Any] | None, *, default: float = _TCO_DEFAULT_MPG) -> float:
    """MPG for TCO and fill-up range copy; falls back to ``default`` when EPA values are missing."""
    if not car:
        return default
    avg = average_mpg_city_highway(car.get("mpg_city"), car.get("mpg_highway"))
    if avg is not None and avg > 0:
        return round(avg, 1)
    combined = _positive_float(car.get("mpg_combined"))
    if combined is not None:
        return round(combined, 1)
    return default


def resolve_tco_ev_efficiency(
    car: dict[str, Any] | None,
    *,
    default: float = _TCO_DEFAULT_EV_KWH_PER_100,
) -> float:
    """
    Estimated residential charging intensity in kWh per 100 miles for pure EVs.

    Uses EPA MPGe (stored in ``mpg_city`` / ``mpg_highway`` for electric listings)
    converted via 33.7 kWh per gasoline-gallon equivalent.
    """
    if not car:
        return default
    avg_mpge = average_mpg_city_highway(car.get("mpg_city"), car.get("mpg_highway"))
    if avg_mpge is None:
        avg_mpge = _positive_float(car.get("mpg_combined"))
    if avg_mpge is None or avg_mpge <= 0:
        return default
    kwh_per_100 = (_EPA_KWH_PER_GALLON * 100.0) / float(avg_mpge)
    return round(kwh_per_100, 1)


def resolve_fuel_tank_gallons(car: dict[str, Any] | None) -> float | None:
    """
    US fuel-tank capacity in gallons, quoted from a page, or ``None``.

    ``None`` means "we do not have a tank size for this car we can attribute to
    a document", and the caller must render nothing — not a class average, not a
    body-style guess. The shopper multiplies this by a live fuel price to get a
    cost-per-fill-up, so a number nobody can trace is worse than a blank.

    The value has to come back from :func:`quoted_extended_spec`, which admits it
    only when a named page URL in ``specs_json['pages']`` reported that same
    number. That excludes the AI spec tables and it excludes the body-class /
    per-model / flat-15.5 defaults that fill 99.9% of the ``fuel_tank_gal``
    column — see the block comment above for the counts.

    Suppressed as well as unquoted:

    * pure battery-electric cars, which have no fuel tank at all. The extended
      spec rows are matched at MODEL level, so a gas Kona can pull the Kona
      Electric's row and vice versa;
    * anything outside 5-60 gal, which is a unit or parse error rather than a
      tank.

    The read-time plausibility guard in ``knowledge_engine_specs`` is NOT applied
    here, and it is not an oversight: ``implausible_extended_spec_fields``
    inspects ``curb_weight_lb`` and ``zero_to_60_sec`` only (verified
    2026-07-31), so on a dict holding a tank size it is a no-op that would only
    make this look better-checked than it is. The 5-60 gal band below is the
    check that actually runs. Nor is the nameplate-wide-constant fingerprint
    that gives away a contaminated ``zero_to_60_sec`` usable on a tank: a fuel
    tank legitimately IS the same across every trim and most model years of a
    nameplate, so "identical across the family" carries no signal. Detecting a
    wrong-but-quoted tank would need a second source to disagree with, which we
    do not have.
    """
    if not car:
        return None
    # ``_is_battery_electric`` reads ``fuel_type``, which ``epa_extended_specs``
    # does not have a column for — the second argument was always empty on this
    # path, so the listing's own fuel type is the whole test.
    if _is_pure_battery_electric(car, {}):
        return None
    quoted = quoted_extended_spec(car, "fuel_tank_gal")
    if quoted is None:
        return None
    return _plausible_tank_gallons(quoted[0])


def compute_fill_up_cost_usd(
    fuel_price_per_gallon: float | None,
    tank_gallons: float | None,
) -> float | None:
    """Full-tank cost at the given price per gallon; ``None`` when either input is missing."""
    if not _positive_float(fuel_price_per_gallon) or not _positive_float(tank_gallons):
        return None
    return round(float(tank_gallons) * float(fuel_price_per_gallon), 2)


def miles_per_tank(avg_mpg: float | None, tank_gallons: float | None) -> int | None:
    if not _positive_float(avg_mpg) or not _positive_float(tank_gallons):
        return None
    return int(round(float(avg_mpg) * float(tank_gallons)))


def _positive_float(value: Any) -> float | None:
    try:
        n = float(value)
    except (TypeError, ValueError):
        return None
    return n if n > 0 else None


def _parse_year(value: Any) -> int | None:
    try:
        y = int(value)
    except (TypeError, ValueError):
        return None
    return y if 1900 <= y <= 2100 else None


def _plausible_tank_gallons(value: Any) -> float | None:
    gal = _positive_float(value)
    if gal is None:
        return None
    if not (_MIN_PLAUSIBLE_TANK_GAL <= gal <= _MAX_PLAUSIBLE_TANK_GAL):
        return None
    return round(gal, 1)


def _is_pure_battery_electric(car: dict[str, Any], specs: dict[str, Any]) -> bool:
    """
    Pure BEV (a plug-in hybrid does NOT count — it has a tank).

    Delegates to the one implementation in ``knowledge_engine_specs`` rather than
    restating the fuel-type rule, so the two cannot disagree about a PHEV.
    """
    try:
        from backend.enrichment.knowledge_engine_specs import _is_battery_electric
    except Exception:
        return False
    return _is_battery_electric(car, specs or {})
