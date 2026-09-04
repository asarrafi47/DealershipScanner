"""Generate a synthesized window-sticker / build sheet for ANY scanned car.

Read-time only. The sheet is composed from the listing row (``cars``) plus
``verified_specs`` (which already merges EPA + extended specs at read time) plus
a best-effort catalog lookup for factory options & packages with prices, plus
what a vision agent read off this car's own photographed window sticker
(``car_image_text`` — see the sticker-photo block below). Nothing
here is persisted — it is a presentation-layer assembly, consistent with the
reference-store rule that catalog facts are never written back onto listing rows.

Unlike the OEM window-sticker path (which needs a per-VIN sticker PDF and only
covers a handful of eligible cars), this build sheet works for *every* car: it
degrades gracefully, emitting only the rows it has real data for.

Nothing on the "Performance & capacity" or "Fuel economy" sections is read off
``verified_specs`` any more. That dict carries AI-generated numbers and
nameplate-level scrapes for every figure in them; see the two block comments
below (:func:`_sourced_fuel_tank_gallons` and "Performance & capacity") for the
measured composition of each column and what replaced it.
"""
from __future__ import annotations

import json
import re
import time
from functools import lru_cache
from typing import Any

# Presentation caps so a pathological catalog row can't flood the panel.
_MAX_CATALOG_TRIM_ROWS = 8
_MAX_CATALOG_PACKAGES = 24
_MAX_CATALOG_OPTIONS = 60
_MAX_PACKAGE_FEATURES = 20


def _clean(v: Any) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    if not s or s in ("—", "-", "N/A", "n/a", "NA", "None", "null"):
        return None
    return s


def _int_or_none(v: Any) -> int | None:
    if v is None or v == "":
        return None
    try:
        return int(round(float(v)))
    except (TypeError, ValueError):
        return None


def _num_or_none(v: Any) -> float | None:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _fmt_price(v: Any) -> str | None:
    n = _num_or_none(v)
    if n is None:
        return None
    n = int(round(n))
    if n < 0:
        return f"${abs(n):,} credit"
    return f"${n:,}"


def _row(
    label: str,
    value: Any,
    *,
    source: str | None = None,
    source_url: str | None = None,
    derived: bool = False,
) -> dict[str, Any] | None:
    """One spec row, carrying where its value came from.

    ``source`` names the store or document the value was read out of — it is not
    decoration. A row with ``derived=True`` is something this code computed
    (arithmetic over listing fields), not something a manufacturer published,
    and consumers must not present it as a specification. Rows built from a
    quoted catalog page also carry ``source_url``, the page that printed the
    number, so a reader can check it.
    """
    v = _clean(value)
    if v is None:
        return None
    row: dict[str, Any] = {"label": label, "value": v, "derived": bool(derived)}
    if source:
        row["source"] = source
    if source_url:
        row["source_url"] = source_url
    return row


def _format_mpg(city: int | None, hwy: int | None) -> str | None:
    if city and hwy:
        return f"{city} city / {hwy} hwy mpg"
    if city:
        return f"{city} city mpg"
    if hwy:
        return f"{hwy} hwy mpg"
    return None


def _epa_mpg_display(vs: dict[str, Any]) -> str | None:
    """EPA's own city/highway figures, from ``epa_master`` via the EPA dataset.

    A non-null ``epa_city08`` / ``epa_highway08`` is what makes the row EPA. The
    pre-formatted ``fuel_economy_display`` is used only as the wording for that
    same EPA row (it carries MPGe for electrics and the window sticker's figures
    for a BEV) and is never accepted on its own: ``merge_verified_specs`` falls
    back to ``format_mpg_city_highway_display(car.mpg_city, car.mpg_highway)``
    when EPA has nothing (knowledge_engine_specs.py:741), so a bare display
    string can be the dealer's own number wearing the EPA's name. Measured on a
    500-car random sample of active listings (2026-08-02): 481 had an EPA
    figure, 2 had a display string with no EPA figure — and in both the dealer's
    own ``mpg_city`` was the number in it. Those 2 now render as "as listed".
    """
    if vs.get("epa_city08") is None and vs.get("epa_highway08") is None:
        return None
    return _clean(vs.get("fuel_economy_display")) or _format_mpg(
        _int_or_none(vs.get("epa_city08")), _int_or_none(vs.get("epa_highway08"))
    )


def _listed_mpg_display(car: dict[str, Any]) -> str | None:
    """Fuel economy as the dealer stated it on the listing.

    A real, attributable number — the listing said it — but it is not an EPA
    rating and the row that shows it must not be labelled as one. This used to
    be merged into the "Fuel economy (EPA)" row with an ``or``, which put the
    EPA's name on whatever the dealer typed.
    """
    return _format_mpg(_int_or_none(car.get("mpg_city")), _int_or_none(car.get("mpg_highway")))


def _engine_display(vs: dict[str, Any], car: dict[str, Any]) -> str | None:
    for cand in (
        car.get("engine_description"),
        vs.get("epa_engine_description"),
        vs.get("master_engine_string"),
    ):
        c = _clean(cand)
        if c:
            return c
    disp = _clean(vs.get("epa_displacement")) or _clean(car.get("engine_l"))
    cyl = _int_or_none(vs.get("cylinders")) or _int_or_none(car.get("cylinders"))
    parts = [p for p in (disp, f"{cyl}-cyl" if cyl else None) if p]
    return " ".join(parts) if parts else None


# ---------------------------------------------------------------------------
# The two figures on this sheet a shopper reads as money or as usable range
#
# "Fuel tank" and "Electric range" are the rows here that turn into a
# running cost or a trip radius, and both used to be read straight off
# ``verified_specs``. That dict is built by ``merge_verified_specs``, whose
# extended-spec half comes from ``lookup_epa_extended_specs`` /
# ``…_by_master_id`` — and both of those finish by merging the AI-researched
# ``ai_model_specs`` table into the result. So the sheet was printing generated
# numbers as facts, on the third render path, after the other two were fixed.
#
# Both now go through the same resolvers the car-page serializer uses, which
# return ``None`` — meaning render nothing — for anything they cannot attribute
# to a document. Neither reads ``verified_specs`` and neither touches the AI
# tables. Routing both through the same two functions is the point: the build
# sheet and the spec list are the same claim about the same car and must not be
# able to print different numbers.
# ---------------------------------------------------------------------------


def _sourced_fuel_tank_gallons(car: dict[str, Any]) -> float | None:
    """Tank size quoted from a named page, or ``None``. Never an estimate."""
    try:
        from backend.intelligence.tco_fuel_estimates import resolve_fuel_tank_gallons

        return resolve_fuel_tank_gallons(car)
    except Exception:
        return None


def _sourced_ev_range_miles(car: dict[str, Any]) -> int | None:
    """EPA published all-electric range, or ``None``. Never an estimate."""
    try:
        from backend.intelligence.ev_range_estimates import resolve_factory_epa_range

        return resolve_factory_epa_range(car)
    except Exception:
        return None


def _can_be_plugged_in(car: dict[str, Any], vs: dict[str, Any]) -> bool:
    """
    True only for a car with a battery-only range: a BEV or a plug-in hybrid.

    A conventional hybrid is excluded. It has no battery-only range to quote —
    a Camry Hybrid's battery moves it across a parking lot — so an "Electric
    range" row on one can only be showing something else's number, which is what
    the old ``"hybrid" in fuel_blob`` gate here was doing.

    Judged on the LISTING's own ``fuel_type``, never on ``verified_specs``. The
    extended-spec rows are matched at MODEL level, so a gas Kona resolves the
    Kona Electric's row; letting the resolved specs vote on whether the car
    plugs in would let the bad match authorise itself. *vs* is accepted and
    deliberately unused so callers cannot pass it in expecting it to widen this.

    Delegates to ``ev_range_estimates._is_electrified_car`` rather than
    restating the fuel-type rule, so this sheet and the range resolver cannot
    disagree about a PHEV. If that import fails the answer is False: a missing
    row is always acceptable, a fabricated one never is.
    """
    del vs
    try:
        from backend.intelligence.ev_range_estimates import _is_electrified_car

        return _is_electrified_car(car)
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Performance & capacity: what ``epa_extended_specs`` can and cannot attribute
#
# Horsepower, torque, 0-60, curb weight, tow capacity and battery capacity used
# to be read straight off ``verified_specs``. Two separate problems:
#
# 1. AI-GENERATED VALUES. ``merge_verified_specs`` fills those keys from
#    ``lookup_epa_extended_specs`` / ``…_by_master_id``, and both finish by
#    calling ``_merge_ai_model_specs`` (knowledge_engine.py:829 and :602), which
#    setdefaults from ``ai_model_specs`` — 2,147 rows, every one
#    ``source_host='ai-research'``. ``_AI_SPEC_COLUMNS`` covers horsepower,
#    torque, curb weight, 0-60 and tow capacity. Because the merge fills into
#    the same flat dict, nothing downstream can tell afterwards which key the AI
#    supplied. The only fix is to stop calling those functions, which is what
#    :func:`_attributable_extended_specs` does — it reads
#    ``epa_extended_specs`` directly.
#
# 2. NAMEPLATE-LEVEL CONTAMINATION. The table was filled by scraping MODEL-level
#    review pages, so one page's numbers were written onto every trim/year row
#    of the nameplate. Counted 2026-08-02 over the whole table:
#
#      field             stored   in a page extraction   nameplate-constant*
#      horsepower        28,893          28,882              24,605
#      torque_lb_ft      27,527          27,527              22,929
#      curb_weight_lb    23,803          23,803              21,743
#      zero_to_60_sec    22,763          22,744              20,857
#      tow_capacity_lb    9,864           9,403               8,818
#      battery_kwh          427             427                 423
#      (* rows in a (make, model) family of >=4 rows holding ONE distinct value)
#
#    So "the number appears in a page extraction" — the test that works for the
#    fuel tank — passes for ~100% of these and proves almost nothing here. The
#    worst case in the table: the 2005 Chevrolet TrailBlazer 2WD row (a
#    body-on-frame SUV with a 4.2L I6) carries 137 hp / 1,000 lb tow / 380 mi
#    "EV range", extracted from a page titled "2026 Chevrolet Trailblazer
#    Review" — a different vehicle with the same name, 21 model years later.
#
# THE GATE. A figure renders only when all of it holds:
#   (a) an ``epa_extended_specs`` row resolves for this car;
#   (b) the stored column is non-null and physically possible;
#   (c) the same number appears under ``specs_json['pages'][*]['extracted']``,
#       so it can be attributed to a page we hold;
#   (d) that page's TITLE names this car's model year. The URL is NOT accepted
#       as evidence of a year: we built those URLs from our own key
#       (``carwow.co.uk/ford/f150/1985/specifications``), so reading the year
#       back out of one is checking the store against its own bookkeeping. 14,392
#       of those carwow fetches landed on a brand home page — title "Ford UK |
#       New Ford Cars, Prices & Reviews | Carwow" — and were parsed anyway;
#   (e) the value is not shared by two or more trims of that (year, make, model)
#       in the table. A number that cannot tell a Laredo from an SRT describes at
#       most one of them and is not attributable to either. This is the failure
#       that put "360 horsepower" on a 290 hp 2011 Grand Cherokee Laredo.
#   (f) ``implausible_extended_spec_fields`` does not condemn it. That guard
#       inspects ``curb_weight_lb`` and ``zero_to_60_sec`` and no other key, so
#       on this sheet — which carries no curb weight — the only field it can act
#       on is 0-60; (b)'s bounds are what run for horsepower, torque and battery.
#
# What survives is small. Over all 73,389 active listings, the fields this sheet
# will render reach: horsepower 2,589 cars (3.5%), torque 2,460 (3.4%), 0-60
# 2,366 (3.2%), battery 0. Before the gate, ``verified_specs`` offered a
# horsepower for 442 of a 1,000-car random sample (44%) — and 221 of those 442,
# exactly half, were supplied by ``ai_model_specs``, measured by instrumenting
# ``_merge_ai_model_specs`` rather than inferring it. That gap is the honest size
# of what this store can say about a particular car; a blank row is the correct
# output for the rest.
# ---------------------------------------------------------------------------

# CURB WEIGHT AND TOW CAPACITY ARE NOT ON THIS SHEET, and that is not an
# oversight. They cleared the gate below and were still wrong, because in this
# store those two columns are frequently the same figure and frequently neither
# of the things they are named after. Measured over every active listing config
# that cleared the rest of the gate (2026-08-02):
#
#   * 118 of the 132 admitted tow ratings — 89% — were identical to the same
#     row's curb weight, and so were 118 of the 288 admitted curb weights. One
#     number parsed into two columns: a 2026 Jeep Gladiator storing 7,700 lb for
#     both is quoting its tow rating; it weighs about 5,050 lb.
#   * Dropping those collisions and flooring the weight at 2,000 lb (the
#     lightest vehicle sold in the US is a 2,095 lb Mitsubishi Mirage) still left
#     310 configs / 1,280 active cars, and 30 of those configs were an SUV or a
#     truck under 3,000 lb. Every 2026 Mazda CX-50 in inventory carried a
#     2,000 lb "curb weight" — which is the CX-50's TOW rating; it weighs about
#     3,700. The collision test cannot catch that one, because the tow column is
#     null on those rows.
#   * Of the four nameplates still holding a tow rating afterwards, two were
#     wrong: 1,500 lb on an Audi Q3 (2,205) and on a Mazda CX-5 (2,000). Right
#     on a Nissan Murano and a Lexus GX 550.
#
# Nothing in the data separates a curb weight from a tow rating here, so both
# stay off the page until the column is re-sourced. Horsepower, torque and 0-60
# are kept: hand-checked against the manufacturers' published figures at the
# extremes of the admitted set (Mirage 78 hp, Lamborghini Urus SE 789 hp, BMW XM
# Label 644 hp, Ford EcoSport 9.8 s, Nissan Versa 9.5 s, Toyota Crown Signia
# 243 hp) they were right, with the exception below, and every row prints the URL
# it came from so a reader can check it.
#
# KNOWN EXCEPTION, unfixed: the 2025 VW ID. Buzz is admitted at 99 lb-ft; VW
# publishes 413. It clears every check here — quoted, this model year, distinct
# across trims, in band — which is the reminder that this gate proves the number
# was READ somewhere about this car, not that the scraper read it correctly.

#: Extended-spec columns this sheet will consider. ``fuel_tank_gal`` and
#: ``ev_range_miles`` are absent on purpose — they have their own resolvers in
#: ``backend.intelligence`` (see above), and routing them twice would let the
#: build sheet and the car page print different numbers for the same car.
#: ``curb_weight_lb`` and ``tow_capacity_lb`` are absent for the reason above.
_ATTRIBUTABLE_SPEC_FIELDS: tuple[str, ...] = (
    "horsepower",
    "torque_lb_ft",
    "zero_to_60_sec",
    "battery_kwh",
)

#: Sanity floor/ceiling per field. Outside the band the stored number is a unit
#: or parse error rather than a specification, and it is dropped, never clamped.
#: Deliberately wide: this is a bounds check, not a spec. The horsepower and
#: torque bands match the ones ``knowledge_engine`` already applies so the two
#: readers of this table cannot disagree about what is physically possible.
_SPEC_BOUNDS: dict[str, tuple[float, float]] = {
    "horsepower": (60.0, 1600.0),
    "torque_lb_ft": (40.0, 1500.0),
    "zero_to_60_sec": (1.5, 30.0),
    "battery_kwh": (5.0, 250.0),
}

#: The page extraction and the stored column are the same number written twice
#: (a float round-trip through JSON), not two measurements — so the match is
#: exact to within a rounding step, scaled to the magnitude of the field.
_QUOTE_REL_TOLERANCE = 0.005

_TITLE_YEAR_RE = re.compile(r"(?<!\d)(?:19|20)\d{2}(?!\d)")


def _page_title_years(title: Any) -> set[int]:
    """Model years named in a page title ("2026 Chevrolet Trailblazer Review")."""
    if not isinstance(title, str) or not title.strip():
        return set()
    return {int(m.group(0)) for m in _TITLE_YEAR_RE.finditer(title)}


def _parse_year(value: Any) -> int | None:
    try:
        y = int(float(value))
    except (TypeError, ValueError):
        return None
    return y if 1900 <= y <= 2100 else None


def _load_specs_json(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, (str, bytes)):
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError):
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _within_bounds(field: str, value: float) -> bool:
    lo, hi = _SPEC_BOUNDS[field]
    return lo <= value <= hi


def _quote_matches(stored: float, extracted: float) -> bool:
    return abs(stored - extracted) <= max(abs(stored), 1.0) * _QUOTE_REL_TOLERANCE


def _extended_row_for_car(car: dict[str, Any]) -> tuple[Any, ...] | None:
    """The ``epa_extended_specs`` row for this car, or ``None``.

    Resolved catalog link (``cars.epa_master_id``, an exact FK) first, then exact
    (year, make, model, trim), then any row for the (year, make, model). Read
    straight off the table: ``knowledge_engine.lookup_epa_extended_specs`` is
    deliberately not used because it merges ``ai_model_specs``.
    """
    mid = _int_or_none(car.get("epa_master_id"))
    if mid and mid > 0:
        row = _extended_row_by_master_id(mid)
        if row is not None:
            return row
    year = _parse_year(car.get("year"))
    make = _clean(car.get("make"))
    model = _clean(car.get("model"))
    if year is None or not make or not model:
        return None
    return _extended_row_by_ymmt(year, make, model, _clean(car.get("trim")) or "")


def _extended_select() -> str:
    return ", ".join((*_ATTRIBUTABLE_SPEC_FIELDS, "year", "make", "model", "specs_json"))


def _fetch_extended_row(where: str, params: tuple[Any, ...]) -> tuple[Any, ...] | None:
    conn = None
    try:
        from backend.db.inventory_db import get_conn

        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            f"SELECT {_extended_select()} FROM epa_extended_specs {where} LIMIT 1", params
        )
        return cur.fetchone()
    except Exception:
        return None
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


@lru_cache(maxsize=8192)
def _extended_row_by_master_id(epa_master_id: int) -> tuple[Any, ...] | None:
    return _fetch_extended_row("WHERE epa_master_id=?", (epa_master_id,))


@lru_cache(maxsize=16384)
def _extended_row_by_ymmt(
    year: int, make: str, model: str, trim: str
) -> tuple[Any, ...] | None:
    if trim:
        row = _fetch_extended_row(
            "WHERE year=? AND lower(make)=lower(?) AND lower(model)=lower(?) "
            "AND lower(trim)=lower(?)",
            (year, make, model, trim),
        )
        if row is not None:
            return row
    return _fetch_extended_row(
        "WHERE year=? AND lower(make)=lower(?) AND lower(model)=lower(?)",
        (year, make, model),
    )


@lru_cache(maxsize=16384)
def _fields_shared_across_trims(year: int, make: str, model: str) -> frozenset[str]:
    """Fields whose stored value is the SAME on two or more trims of this config.

    Such a value cannot tell those trims apart, so it describes at most one of
    them and is not attributable to any particular car. Configs with a single
    trim row are not evidence either way and contribute nothing here.
    """
    cols = ", ".join(
        f"count(DISTINCT trim) FILTER (WHERE {f} IS NOT NULL), "
        f"count(DISTINCT {f})"
        for f in _ATTRIBUTABLE_SPEC_FIELDS
    )
    conn = None
    try:
        from backend.db.inventory_db import get_conn

        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            f"SELECT {cols} FROM epa_extended_specs "
            "WHERE year=? AND lower(make)=lower(?) AND lower(model)=lower(?)",
            (year, make, model),
        )
        row = cur.fetchone()
    except Exception:
        # Unknown spread is not permission to render: a value we cannot show is
        # trim-specific stays off the page.
        return frozenset(_ATTRIBUTABLE_SPEC_FIELDS)
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
    if not row:
        return frozenset()
    shared: set[str] = set()
    for idx, field in enumerate(_ATTRIBUTABLE_SPEC_FIELDS):
        trims = _int_or_none(row[idx * 2]) or 0
        values = _int_or_none(row[idx * 2 + 1]) or 0
        if trims >= 2 and values <= 1:
            shared.add(field)
    return frozenset(shared)


def clear_attributable_spec_cache() -> None:
    """Drop the memoized catalog reads (tests, and after a catalog reload)."""
    _extended_row_by_master_id.cache_clear()
    _extended_row_by_ymmt.cache_clear()
    _fields_shared_across_trims.cache_clear()


def _attributable_extended_specs(car: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Extended specs this car's own model-year page printed, with the page URL.

    Returns ``{field: {"value": float, "source_url": str, "page_title": str}}``,
    empty for everything that fails the gate in the block comment above. An
    empty result means "render nothing" — never a family average, never the AI
    table's answer.
    """
    if not car:
        return {}
    car_year = _parse_year(car.get("year"))
    if car_year is None:
        return {}
    row = _extended_row_for_car(car)
    if not row:
        return {}

    n = len(_ATTRIBUTABLE_SPEC_FIELDS)
    stored = {f: row[i] for i, f in enumerate(_ATTRIBUTABLE_SPEC_FIELDS)}
    row_year, row_make, row_model = row[n], row[n + 1], row[n + 2]
    payload = _load_specs_json(row[n + 3])

    candidates: dict[str, float] = {}
    for field, raw in stored.items():
        val = _num_or_none(raw)
        if val is not None and _within_bounds(field, val):
            candidates[field] = val
    if not candidates:
        return {}

    # (c) + (d): the number has to come back from a page extraction whose TITLE
    # names this car's model year. Not the URL — we built the URL.
    found: dict[str, dict[str, Any]] = {}
    pages = payload.get("pages")
    if not isinstance(pages, list):
        return {}
    for page in pages:
        if not isinstance(page, dict):
            continue
        url = str(page.get("url") or "").strip()
        title = str(page.get("title") or "").strip()
        extracted = page.get("extracted")
        if not url or not isinstance(extracted, dict):
            continue
        if car_year not in _page_title_years(title):
            continue
        for field, val in candidates.items():
            if field in found:
                continue
            quoted = _num_or_none(extracted.get(field))
            if quoted is None or not _quote_matches(val, quoted):
                continue
            found[field] = {"value": val, "source_url": url, "page_title": title}
    if not found:
        return {}

    # (e): drop anything the table gives to two or more trims of this config.
    fam_year = _parse_year(row_year) or car_year
    shared = _fields_shared_across_trims(
        fam_year, str(row_make or "").strip(), str(row_model or "").strip()
    )
    found = {f: v for f, v in found.items() if f not in shared}
    if not found:
        return {}

    # (f) the shared read-time guard. It inspects curb_weight_lb and
    # zero_to_60_sec only (verified 2026-08-02 in
    # ``knowledge_engine_specs.implausible_extended_spec_fields``), and this
    # sheet never carries a curb weight — so the only field it can act on here is
    # zero_to_60_sec, where it catches the nameplate-wide Ram 1500 4.9 s and the
    # sub-4-second-without-a-badge cases. On horsepower, torque and battery it is
    # a no-op, and the bounds check above is what actually runs for those. Called
    # anyway so this sheet and the car page apply the same rule to 0-60.
    try:
        from backend.enrichment.knowledge_engine_specs import (
            implausible_extended_spec_fields,
        )

        bad = implausible_extended_spec_fields(
            car, {f: v["value"] for f, v in found.items()}
        )
    except Exception:
        bad = set()
    return {f: v for f, v in found.items() if f not in bad}


def _consistent_cylinders(
    vs: dict[str, Any], car: dict[str, Any], engine_display: str | None
) -> int | None:
    """Cylinder count, or ``None`` when it contradicts the engine on the same sheet.

    A 2017 Grand Cherokee Limited renders "Engine: 3.6L V6" one line above
    "Cylinders: 8" — the resolver had no answer, so the row fell through to the
    dealer's own ``cars.cylinders``, which disagrees with the dealer's own engine
    string. Two rows of the same panel cannot describe different engines: when
    they conflict the count is dropped and the engine line, which carries the
    displacement and the layout, is left to speak.

    Delegates to ``backend.utils.engine_consistency`` rather than restating the
    layout rule, so this sheet cannot disagree with the rest of the codebase
    about what "V6" implies. If that import fails the count is dropped: a missing
    row is acceptable, a contradictory pair is not.
    """
    count = _int_or_none(vs.get("cylinders"))
    if count is None:
        count = _int_or_none(car.get("cylinders"))
    if count is None:
        return None
    engine_text = " ".join(
        str(x)
        for x in (engine_display, car.get("engine_description"), vs.get("master_engine_string"))
        if x
    )
    fuel = _clean(vs.get("epa_fuel_type")) or _clean(car.get("fuel_type"))
    try:
        from backend.utils.engine_consistency import cylinders_conflicts_with_engine_text
        from backend.utils.fuel_label_plausibility import (
            PLAUSIBLE,
            assess_electric_claim,
            is_bare_electric_label,
        )

        # A battery-electric row with a junk positive count: judged from the
        # ROW's own text and nameplate, not from *engine_display* — that string
        # can itself be derived from the junk count ("4-cyl" on a Tesla), so
        # letting it corroborate the count would be circular. When the electric
        # claim is implausible (gas GX 550 stored "Electric"), the count is the
        # evidence and survives to the layout check below.
        if count > 0 and is_bare_electric_label(fuel):
            if assess_electric_claim(car).verdict == PLAUSIBLE:
                return None
        if cylinders_conflicts_with_engine_text(count, engine_text, fuel):
            return None
    except Exception:
        return None
    return count


def _described_package_names(car: dict[str, Any]) -> list[str]:
    """Package names this listing itself names (from its parsed description).

    Reads the per-car ``cars.packages`` blob: ``factory_packages`` (listing
    package names) and any ``packages_normalized`` titles. These are the
    packages the dealer says the car has \u2014 the ones a generated sticker should
    price. Standard-equipment ``features`` are intentionally excluded.
    """
    raw = car.get("packages")
    if not raw or str(raw).strip() in ("{}", "[]", "null", ""):
        return []
    try:
        d = json.loads(raw) if isinstance(raw, str) else raw
    except (TypeError, ValueError):
        return []
    if not isinstance(d, dict):
        return []
    names: list[str] = []
    seen: set[str] = set()

    def _add(name: Any) -> None:
        s = _clean(name)
        if not s:
            return
        key = s.lower()
        if key not in seen:
            seen.add(key)
            names.append(s)

    for p in d.get("factory_packages") or []:
        _add(p)
    for entry in d.get("packages_normalized") or []:
        if isinstance(entry, dict):
            _add(entry.get("name") or entry.get("canonical_name") or entry.get("name_verbatim"))
        else:
            _add(entry)
    return names


def _catalog_equipment(car: dict[str, Any]) -> dict[str, Any]:
    """Price the packages THIS listing names \u2014 a generated sticker's line items.

    For a car with no OEM window sticker but whose listing names packages, we
    look up each package's price in the package-value registry. Every price in
    that registry was observed on a real OEM window sticker (2,376 priced rows,
    all ``msrp_source='oem_sticker'``, measured 2026-08-02) — none is estimated.

    A price is PRINTED only when the sticker it came from was for this car's
    model year and trim. ``price_for_package`` also answers with observations
    from other years and trims of the nameplate, and it used to hand those back
    with a "sticker" badge and no caveat: 331 of the 1,547 priced
    (make, model, name) groups span more than one model year and 163 hold more
    than one distinct price for the same name, up to a $114,500-$198,300 spread
    inside a single group. Those now come back on the entry as
    ``observed_price`` / ``observed_year`` / ``observed_trim`` with
    ``price_basis='other_config'`` — the fact is kept, the false attribution is
    not. Packages we have never seen priced are still listed, without a price.
    See ``backend.enrichment.package_registry``.
    """
    make = _clean(car.get("make"))
    model = _clean(car.get("model"))
    empty = {"packages": [], "options": [], "priced_total": None, "priced_total_display": None}
    if not (make and model):
        return empty

    names = _described_package_names(car)
    if not names:
        return empty
    try:
        from backend.enrichment.package_registry import price_for_package
    except Exception:
        return empty

    miss = {
        "price": None,
        "source": None,
        "from_sticker": False,
        "observed_year": None,
        "observed_trim": None,
        "exact_config": False,
    }
    year = car.get("year")
    trim = car.get("trim")
    packages: list[dict[str, Any]] = []
    for name in names[:_MAX_CATALOG_PACKAGES]:
        try:
            p = price_for_package(make, model, year, trim, name)
        except Exception:
            p = miss
        observed = _int_or_none(p.get("price"))
        exact = bool(p.get("exact_config")) and observed is not None
        price = observed if exact else None
        packages.append(
            {
                "name": name,
                "code": None,
                "price": price,
                "price_display": _fmt_price(price),
                "category": None,
                # True only for a sticker observed on THIS year and trim; the
                # template renders this as a "sticker" badge next to the price.
                "from_sticker": exact and bool(p.get("from_sticker")),
                "price_basis": (
                    "exact_config" if exact else ("other_config" if observed else None)
                ),
                "price_source": p.get("source"),
                "observed_price": observed,
                "observed_price_display": _fmt_price(observed),
                "observed_year": p.get("observed_year"),
                "observed_trim": p.get("observed_trim"),
            }
        )
    priced_total = sum(e["price"] for e in packages if e["price"] and e["price"] > 0)
    return {
        "packages": packages,
        "options": [],
        "priced_total": priced_total or None,
        "priced_total_display": _fmt_price(priced_total) if priced_total else None,
        # A sum this code computed over the observed line items above, not a
        # figure any manufacturer or dealer published.
        "priced_total_derived": priced_total > 0,
        "priced_total_basis": "sum of exact-config observed sticker prices",
    }


# ---------------------------------------------------------------------------
# Window-sticker PHOTOS: what a vision agent read off this car's own gallery
#
# ``car_image_text`` holds what agents read off photographed Monroneys and
# Vehicle Highlights slides: ``summary.equipment[]``, ``summary.packages[]``
# and ``summary.priced_options[]`` (each ``{name, price}``). Those prices were
# gate-checked at record time — fees, never-priced trials and column-drift skew
# are filtered in ``backend/scripts/image_batch.py`` ``cmd_record``, and an
# itemization whose parts exceed the sticker's own printed total is refused
# whole — so they are taken here as recorded.
#
# Authority mirrors the package registry's ladder
# (``package_registry._SOURCE_AUTHORITY``): a parsed OEM sticker PDF (3)
# outranks a sticker photo (2), because a photo shot at an angle can shift the
# price column against the label column. So a photo price only FILLS a line
# that has no price; it never replaces one, and every photo-sourced line says
# where it came from (``source_label``) so the template can badge it.
# ---------------------------------------------------------------------------

#: Rows below this version are the local OCR passes (Apple Vision / qwen,
#: version 1–99), which mis-pair option prices across columns; only agent-vision
#: rows are read here. Mirrors ``image_batch.AGENT_VISION_VERSION`` (= 100) —
#: not imported, so rendering a VDP never imports a batch script.
_STICKER_PHOTO_MIN_VERSION = 100

#: Wording for photo-sourced lines; the template renders it as a badge.
_STICKER_PHOTO_LABEL = "seen on window sticker photo"


_NON_ALNUM_KEY_RE = re.compile(r"[^a-z0-9]+")
#: Agents write the same package twice — "Premium Package" in ``packages`` and
#: "Premium Package (Harman Kardon sound, HUD, …)" in ``priced_options`` — so a
#: parenthesized span is contents-of, not identity, and is ignored for matching.
_PARENTHETICAL_RE = re.compile(r"\([^)]*\)")


def _photo_dedup_key(name: Any) -> str:
    """Case/punctuation-insensitive key for "is this line already shown".

    Parenthesized spans are dropped first (see above), then the registry's
    ``normalize_name`` runs (it also drops package/option nouns, so "Premium
    Package" == "Premium Pkg") when available; a name made entirely of those
    nouns falls back to a bare lowercase-alnum key rather than colliding with
    every other such name on the empty string.
    """
    base = _PARENTHETICAL_RE.sub(" ", str(name or ""))
    fallback = " ".join(_NON_ALNUM_KEY_RE.sub(" ", base.lower()).split())
    try:
        from backend.enrichment.package_registry import normalize_name

        return normalize_name(base) or fallback
    except Exception:
        return fallback


#: How long one memoized photo read is trusted. ``image_batch`` records new
#: sticker reads from ANOTHER process, so the web worker never sees the write
#: and nothing ever calls ``clear_sticker_photo_cache`` for it; rotating the
#: cache key on this coarse clock is what lets fresh rows show up at all.
_STICKER_PHOTO_TTL_SECONDS = 300


@lru_cache(maxsize=8192)
def _sticker_photo_summary_cached(
    car_id: int, time_bucket: int
) -> tuple[tuple[str, ...], tuple[str, ...], tuple[tuple[str, int], ...]]:
    """The memoized DB read behind :func:`_sticker_photo_summary`.

    One PRIMARY-KEY fetch: ``car_image_text.car_id`` is the table's PK and its
    only per-car index (the others cover ``version`` and ``has_sticker``), and
    this sheet renders on every VDP, so the query must never scan. All three
    lists come back from the single ``summary`` read and are memoized per
    ``(car, time_bucket)`` — the bucket does nothing but rotate the key every
    ``_STICKER_PHOTO_TTL_SECONDS``. A DB error propagates: ``lru_cache`` never
    stores a raise, so one blip can't blank a car for the process lifetime.
    """
    conn = None
    try:
        from backend.db.inventory_db import get_conn

        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "SELECT summary FROM car_image_text "
            "WHERE car_id=? AND version >= ? AND summary IS NOT NULL",
            (car_id, _STICKER_PHOTO_MIN_VERSION),
        )
        row = cur.fetchone()
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
    if not row:
        return ((), (), ())
    summary = _load_specs_json(row[0])
    equipment = tuple(
        s for s in (_clean(x) for x in summary.get("equipment") or []) if s
    )
    packages = tuple(
        s for s in (_clean(x) for x in summary.get("packages") or []) if s
    )
    priced: list[tuple[str, int]] = []
    for opt in summary.get("priced_options") or []:
        if not isinstance(opt, dict):
            continue
        name = _clean(opt.get("name"))
        price = _int_or_none(opt.get("price"))
        if name and price is not None:
            priced.append((name, price))
    return (equipment, packages, tuple(priced))


def _sticker_photo_summary(
    car_id: int,
) -> tuple[tuple[str, ...], tuple[str, ...], tuple[tuple[str, int], ...]]:
    """``(equipment, packages, priced_options)`` a vision agent read for this car.

    Empty tuples on any miss or error — a missing row renders nothing — but the
    error path is never cached (see :func:`_sticker_photo_summary_cached`), so
    the next render retries.
    """
    try:
        return _sticker_photo_summary_cached(
            car_id, int(time.time()) // _STICKER_PHOTO_TTL_SECONDS
        )
    except Exception:
        return ((), (), ())


def clear_sticker_photo_cache() -> None:
    """Drop the memoized photo reads (tests, and after a new agent wave lands)."""
    _sticker_photo_summary_cached.cache_clear()


def _photo_entry(name: str, price: int | None = None) -> dict[str, Any]:
    """A catalog entry for a line seen only on the sticker photo."""
    return {
        "name": name,
        "code": None,
        "price": price,
        "price_display": _fmt_price(price),
        "category": None,
        # Not a parsed-PDF sticker price; the "sticker" badge stays off.
        "from_sticker": False,
        "price_basis": "sticker_photo" if price is not None else None,
        "price_source": "sticker_photo" if price is not None else None,
        "source_label": _STICKER_PHOTO_LABEL,
        "observed_price": price,
        "observed_price_display": _fmt_price(price),
        "observed_year": None,
        "observed_trim": None,
    }


def _merge_sticker_photo_findings(catalog: dict[str, Any], car: dict[str, Any]) -> None:
    """Fold this car's photographed-sticker findings into the catalog section.

    Mutates ``catalog`` in place. Equipment and package names land as unpriced
    lines, deduped case/punct-insensitively against everything already shown.
    A priced option whose name is already listed only donates its price when
    the existing line has none — the listed lines are priced from parsed OEM
    stickers (authority 3 on the registry ladder), which a photo (2) must not
    overwrite. A priced option not listed at all is added with its price.
    """
    car_id = _int_or_none(car.get("id"))
    if not car_id or car_id <= 0:
        return
    equipment, packages, priced = _sticker_photo_summary(car_id)
    if not (equipment or packages or priced):
        return

    pkg_entries: list[dict[str, Any]] = catalog["packages"]
    opt_entries: list[dict[str, Any]] = catalog["options"]
    shown: dict[str, dict[str, Any]] = {}
    for entry in pkg_entries + opt_entries:
        key = _photo_dedup_key(entry.get("name"))
        if key:
            shown.setdefault(key, entry)

    for name, price in priced:
        key = _photo_dedup_key(name)
        if not key:
            continue
        entry = shown.get(key)
        if entry is None:
            # Same routing rule as the registry: package-shaped names under
            # "Packages" (where the subtotal lives), the rest as options.
            try:
                from backend.enrichment.package_registry import classify_kind

                is_pkg = classify_kind(name) == "package"
            except Exception:
                is_pkg = False
            target, cap = (
                (pkg_entries, _MAX_CATALOG_PACKAGES)
                if is_pkg
                else (opt_entries, _MAX_CATALOG_OPTIONS)
            )
            if len(target) < cap:
                new = _photo_entry(name, price)
                target.append(new)
                shown[key] = new
            continue
        if entry.get("price") is None:
            # Fill, never overwrite: the line was named but not priced. The
            # entry's observed_* fields (a real sticker price from ANOTHER
            # config, when present) are left alone — they describe what the
            # registry saw, this price describes what this car's photo says.
            entry["price"] = price
            entry["price_display"] = _fmt_price(price)
            entry["price_basis"] = "sticker_photo"
            entry["price_source"] = "sticker_photo"
            entry["source_label"] = _STICKER_PHOTO_LABEL

    for name in packages:
        key = _photo_dedup_key(name)
        if not key or key in shown or len(pkg_entries) >= _MAX_CATALOG_PACKAGES:
            continue
        new = _photo_entry(name)
        pkg_entries.append(new)
        shown[key] = new

    for name in equipment:
        key = _photo_dedup_key(name)
        if not key or key in shown or len(opt_entries) >= _MAX_CATALOG_OPTIONS:
            continue
        new = _photo_entry(name)
        opt_entries.append(new)
        shown[key] = new

    # The subtotal sums PACKAGE lines; a photo price filled into one changes
    # it, and the stated basis must change with the composition.
    if any(e.get("price_basis") == "sticker_photo" for e in pkg_entries):
        priced_total = sum(e["price"] for e in pkg_entries if e["price"] and e["price"] > 0)
        catalog["priced_total"] = priced_total or None
        catalog["priced_total_display"] = _fmt_price(priced_total) if priced_total else None
        catalog["priced_total_derived"] = priced_total > 0
        catalog["priced_total_basis"] = (
            "sum of observed sticker prices (parsed stickers and sticker photos)"
        )


_REGISTRY_SOURCE_LABELS = {
    "oem_sticker": "seen on this trim's window sticker",
    "sticker_photo": "seen on this trim's window sticker photo",
    "brochure": "listed in the manufacturer brochure for this trim",
    "estimate": "estimated for this trim",
}


def _registry_entry(e: dict[str, Any]) -> dict[str, Any]:
    price = _int_or_none(e.get("msrp"))
    source = e.get("msrp_source")
    return {
        "name": e.get("name"),
        "code": e.get("code"),
        "price": price,
        "price_display": _fmt_price(price),
        "category": e.get("category"),
        "from_sticker": False,
        "from_registry": True,
        "price_basis": source,
        "price_source": source,
        "source_label": _REGISTRY_SOURCE_LABELS.get(source, source),
        "observed_price": price,
        "observed_price_display": _fmt_price(price),
        "observed_year": None,
        "observed_trim": None,
    }


def _merge_registry_offerings(catalog: dict[str, Any], car: dict[str, Any]) -> None:
    """Add priced trim-level packages/options this car's OWN listing never named.

    ``_catalog_equipment`` only prices packages the listing text already
    states; ``_merge_sticker_photo_findings`` only adds what THIS car's own
    photographed sticker shows. Neither surfaces a ``package_values`` row for
    this exact (year, make, model, trim) that came from a DIFFERENT car of
    the same trim — an OEM sticker, a brochure read, or a sticker photo —
    unless this car's own listing happens to name it too. That left every
    trim-level fact fed into the registry (including the whole brochure
    pipeline) unreachable from the frontend. This fills the gap, clearly
    labeled as trim-level catalog knowledge and never mistaken for a fact
    about this specific VIN (``from_registry`` stays False on every other
    entry). Only priced rows are added — an unpriced name with nothing else
    backing it is too noisy to show without evidence.
    """
    year = _int_or_none(car.get("year"))
    make = _clean(car.get("make"))
    model = _clean(car.get("model"))
    trim = _clean(car.get("trim"))
    if not (make and model and trim):
        return
    try:
        from backend.enrichment.package_registry import lookup_package_values
    except Exception:
        return
    try:
        found = lookup_package_values(year, make, model, trim)
    except Exception:
        return
    if not (found.get("packages") or found.get("options")):
        return

    pkg_entries: list[dict[str, Any]] = catalog["packages"]
    opt_entries: list[dict[str, Any]] = catalog["options"]
    shown: set[str] = set()
    for entry in pkg_entries + opt_entries:
        key = _photo_dedup_key(entry.get("name"))
        if key:
            shown.add(key)

    for e in found.get("packages") or []:
        if _int_or_none(e.get("msrp")) is None:
            continue
        key = _photo_dedup_key(e.get("name"))
        if not key or key in shown or len(pkg_entries) >= _MAX_CATALOG_PACKAGES:
            continue
        pkg_entries.append(_registry_entry(e))
        shown.add(key)

    for e in found.get("options") or []:
        if _int_or_none(e.get("msrp")) is None:
            continue
        key = _photo_dedup_key(e.get("name"))
        if not key or key in shown or len(opt_entries) >= _MAX_CATALOG_OPTIONS:
            continue
        opt_entries.append(_registry_entry(e))
        shown.add(key)

    priced_total = sum(e["price"] for e in pkg_entries if e.get("price") and e["price"] > 0)
    if priced_total:
        catalog["priced_total"] = priced_total
        catalog["priced_total_display"] = _fmt_price(priced_total)
        catalog["priced_total_derived"] = True
        catalog["priced_total_basis"] = (
            "sum of observed sticker prices (parsed stickers, sticker photos, "
            "and this trim's catalog)"
        )


def build_generated_spec_sheet(
    car: dict[str, Any], verified_specs: dict[str, Any] | None = None
) -> dict[str, Any] | None:
    """Assemble a window-sticker-style build sheet for a single car.

    ``car`` is the raw listing row (``cars`` table dict). ``verified_specs`` is
    the output of ``merge_verified_specs`` (EPA + extended specs already merged);
    pass it in to avoid a second lookup. Returns ``None`` only when the row lacks
    even basic identity — every real car gets a sheet.
    """
    if not car:
        return None
    vs = verified_specs or {}

    year = _int_or_none(car.get("year"))
    make = _clean(car.get("make"))
    model = _clean(car.get("model"))
    if not (make and model):
        return None
    trim = _clean(car.get("trim"))

    identity = [
        r
        for r in (
            _row("Year", year),
            _row("Make", make),
            _row("Model", model),
            _row("Trim", trim),
            _row("Body style", _clean(vs.get("body_style_display")) or car.get("body_style")),
            _row("VIN", car.get("vin")),
            _row("Stock #", car.get("stock_number")),
        )
        if r
    ]

    engine = _engine_display(vs, car)
    powertrain = [
        r
        for r in (
            _row("Engine", engine, source="listing / EPA engine description"),
            _row(
                "Cylinders",
                _consistent_cylinders(vs, car, engine),
                source="trim decoder / EPA dataset / listing",
            ),
            _row("Forced induction", car.get("forced_induction"), source="listing"),
            _row(
                "Transmission",
                _clean(vs.get("transmission_display")) or car.get("transmission"),
                source="trim decoder / EPA dataset / listing",
            ),
            _row(
                "Drivetrain",
                _clean(vs.get("drivetrain_display")) or car.get("drivetrain"),
                source="trim decoder / EPA dataset / listing",
            ),
            _row(
                "Fuel type",
                _clean(vs.get("epa_fuel_type")) or car.get("fuel_type"),
                source="EPA dataset / listing",
            ),
        )
        if r
    ]

    # Electric range and battery capacity only exist on a car you can plug in.
    # ``_can_be_plugged_in`` is the gate, NOT "the fuel string says hybrid": a
    # Camry Hybrid has no battery-only range to quote, and the extended-spec rows
    # are matched at MODEL level, so a gas Kona can pull the Kona Electric's row.
    plug_in = _can_be_plugged_in(car, vs)
    # ``resolve_factory_epa_range`` — EPA's own published all-electric range, or
    # None. NOT ``vs["ev_range_miles"]``, which is a TOTAL driving range scraped
    # off a nameplate review page and mislabelled (598 mi of "electric range" on
    # a hybrid); see the removal note in ``backend.intelligence.ev_range_estimates``.
    ev_range = _sourced_ev_range_miles(car) if plug_in else None

    # Every extended-spec figure below and in the performance section is quoted
    # from a page about THIS car's model year, or absent. Nothing reads ``vs`` —
    # see the "Performance & capacity" block comment for what ``vs`` carries.
    attributed = _attributable_extended_specs(car)

    def _spec_row(label: str, field: str, fmt) -> dict[str, Any] | None:
        hit = attributed.get(field)
        if not hit:
            return None
        return _row(
            label,
            fmt(hit["value"]),
            source="epa_extended_specs page extraction",
            source_url=hit["source_url"],
        )

    # "Fuel economy (EPA)" said EPA whatever the number was, including when it
    # came off ``cars.mpg_city`` / ``mpg_highway`` — figures the dealer typed
    # into the listing. The label now follows the source.
    mpg_epa = _epa_mpg_display(vs)
    mpg_listed = None if mpg_epa else _listed_mpg_display(car)
    battery = attributed.get("battery_kwh") if plug_in else None
    economy = [
        r
        for r in (
            _row("Fuel economy (EPA)", mpg_epa, source="epa_master (EPA dataset)"),
            _row(
                "Fuel economy (as listed)",
                mpg_listed,
                source="dealer listing (cars.mpg_city / cars.mpg_highway)",
            ),
            _row(
                "Electric range",
                f"{ev_range} mi" if ev_range else None,
                source="EPA published all-electric range",
            ),
            _row(
                "Battery",
                f"{battery['value']:g} kWh" if battery else None,
                source="epa_extended_specs page extraction",
                source_url=battery["source_url"] if battery else None,
            ),
            # ``resolve_fuel_tank_gallons`` — a tank quoted from a named page, or
            # None. NOT ``vs["fuel_tank_gal"]``: ``merge_verified_specs`` builds
            # that through ``lookup_epa_extended_specs``, which ends in
            # ``_merge_ai_model_specs``, and even the non-AI value is 99.9%
            # body-class / per-model / flat-15.5 defaults. See the block comment
            # in ``backend.intelligence.tco_fuel_estimates``.
            _row(
                "Fuel tank",
                (lambda g: f"{g:g} gal" if g else None)(_sourced_fuel_tank_gallons(car)),
                source="epa_extended_specs page extraction",
            ),
        )
        if r
    ]

    performance = [
        r
        for r in (
            _spec_row("Horsepower", "horsepower", lambda v: f"{int(round(v))} hp"),
            _spec_row("Torque", "torque_lb_ft", lambda v: f"{int(round(v))} lb-ft"),
            _spec_row("0–60 mph", "zero_to_60_sec", lambda v: f"{v:g} sec"),
            # No "Curb weight" and no "Towing capacity" row: see the block
            # comment above _ATTRIBUTABLE_SPEC_FIELDS for what those two columns
            # actually hold and the counts behind removing them.
        )
        if r
    ]

    color = [
        r
        for r in (
            _row("Exterior", car.get("exterior_color")),
            _row("Interior", car.get("interior_color")),
        )
        if r
    ]

    # The MSRP on this sheet is NOT ``cars.msrp``. That column is the dealer
    # feed's verbatim value, and on used inventory it is a marketing "was"
    # price: 9,621 of the 18,350 active listings that carry one hold EXACTLY
    # the asking price, which is what put "MSRP $48,990 / LISTED PRICE $48,990"
    # on a 2024 X3 M40i, and 2,439 hold LESS than the asking price. This sheet
    # used to print that number and then subtract it from the price and label
    # the remainder "Below MSRP" — inventing a $2,202 discount on a 2016 M4
    # whose own window sticker, which we hold, reads $86,600.
    #
    # ``resolve_display_msrp`` is the single read-time gate, shared with the car
    # page serializer so the two cannot print different MSRPs for the same car.
    # It prefers the sticker we hold for this VIN, rejects a feed msrp on
    # anything pre-owned, and never returns a figure at or below the price — so
    # "Below MSRP" can no longer be zero, negative, or measured against the
    # price itself. See ``backend/utils/msrp_trust.py``.
    from backend.utils.msrp_trust import resolve_display_msrp

    resolved = resolve_display_msrp(car)
    msrp = resolved["msrp"]
    price = _num_or_none(car.get("price"))
    savings = resolved["savings"]
    pricing = {
        "msrp": msrp,
        "msrp_display": _fmt_price(msrp),
        # "MSRP" on new inventory; "Original MSRP" on a used car, where the
        # sticker total is what the car cost new and not today's benchmark.
        "msrp_label": resolved["label"],
        "msrp_source": resolved["source"],
        "msrp_from_sticker": resolved["from_sticker"],
        "price": price,
        "price_display": _fmt_price(price),
        "price_source": "dealer listing (cars.price)" if price else None,
        "savings": savings,
        "savings_display": _fmt_price(savings) if savings else None,
        # Arithmetic over two figures, not a manufacturer or lender figure.
        # Flagged in the data so no consumer can present it as one.
        "savings_derived": savings is not None,
        "savings_basis": resolved["savings_basis"],
    }
    has_pricing = bool(pricing["msrp_display"] or pricing["price_display"])

    catalog = _catalog_equipment(car)
    # Fold in what a vision agent read off this car's own photographed sticker
    # (car_image_text). Photo lines carry their own provenance label and never
    # outrank a parsed price — see _merge_sticker_photo_findings.
    _merge_sticker_photo_findings(catalog, car)
    # Trim-level catalog knowledge (brochure/oem-sticker/sticker-photo reads
    # from OTHER cars of this same trim) — never a fact about this VIN.
    _merge_registry_offerings(catalog, car)
    has_catalog = bool(catalog["packages"] or catalog["options"])
    catalog_from_sticker = any(
        e.get("from_sticker") for e in catalog["packages"] + catalog["options"]
    )
    catalog_from_sticker_photo = any(
        e.get("source_label") == _STICKER_PHOTO_LABEL
        for e in catalog["packages"] + catalog["options"]
    )
    catalog_from_registry = any(
        e.get("from_registry") for e in catalog["packages"] + catalog["options"]
    )

    sections = [
        {"key": "identity", "title": "Vehicle", "rows": identity},
        {"key": "powertrain", "title": "Mechanical", "rows": powertrain},
        {"key": "economy", "title": "Fuel economy", "rows": economy},
        {"key": "performance", "title": "Performance & capacity", "rows": performance},
        {"key": "color", "title": "Color", "rows": color},
    ]
    sections = [s for s in sections if s["rows"]]

    subtitle_parts = [str(p) for p in (year, make, model, trim) if p]
    return {
        "title": " ".join(subtitle_parts) or "Vehicle build sheet",
        "vin": _clean(car.get("vin")),
        "sections": sections,
        "pricing": pricing,
        "has_pricing": has_pricing,
        "catalog": catalog,
        "has_catalog": has_catalog,
        "catalog_from_sticker": catalog_from_sticker,
        "catalog_from_sticker_photo": catalog_from_sticker_photo,
        "catalog_from_registry": catalog_from_registry,
    }


# ---------------------------------------------------------------------------
# Options tab unification
#
# The VDP used to render up to seven separate equipment lists for one car —
# window-sticker options, this module's own catalog, Monroney factory
# options, Monroney standard equipment, packages named in the listing
# description, photo-analysis guesses, and "observed features" — with no
# dedup between them. Because ``_catalog_equipment`` above is itself built
# from ``_described_package_names`` (the listing's own package text), the
# catalog and "packages from listing" almost always name the same things
# twice; a Monroney "Sunroof" and a photo-analysis "Sunroof" would show a
# third and fourth time. ``build_unified_options_list`` folds every source
# into ONE list, keyed by :func:`_photo_dedup_key` (the same key the catalog
# already dedupes packages/options with), so each real piece of equipment is
# printed once, tagged with the best source that named it.
# ---------------------------------------------------------------------------

#: Priority order = confidence order. A name already added under an earlier
#: tier is skipped, never repeated, under a later one.
_UNIFIED_TIER_ORDER = (
    "sticker",
    "catalog",
    "monroney_factory",
    "monroney_standard",
    "listing",
    "photo",
)


def _unified_add(
    seen: dict[str, dict[str, Any]],
    order: list[str],
    name: Any,
    *,
    tier: str,
    badge_label: str,
    price: Any = None,
) -> None:
    cleaned = _clean(name)
    if not cleaned:
        return
    key = _photo_dedup_key(cleaned)
    if not key or key in seen:
        return
    price_num = _num_or_none(price)
    seen[key] = {
        "name": cleaned,
        "key": key,
        "tier": tier,
        "badge_label": badge_label,
        "price": price_num,
        "price_display": _fmt_price(price_num) if price_num is not None else None,
    }
    order.append(key)


def _unified_add_sticker_entry(
    seen: dict[str, dict[str, Any]], order: list[str], entry: Any
) -> None:
    if isinstance(entry, dict):
        name = entry.get("name") or entry.get("label")
        price = entry.get("price")
        _unified_add(seen, order, name, tier="sticker", badge_label="Window sticker", price=price)
        for feat in entry.get("features") or []:
            fname = feat.get("name") if isinstance(feat, dict) else feat
            _unified_add(seen, order, fname, tier="sticker", badge_label="Window sticker")
    else:
        _unified_add(seen, order, entry, tier="sticker", badge_label="Window sticker")


def build_unified_options_list(
    *,
    catalog: dict[str, Any] | None = None,
    sticker_option_sections: dict[str, Any] | None = None,
    sticker_option_groups: list[dict[str, Any]] | None = None,
    sticker_options: list[Any] | None = None,
    monroney_options: list[str] | None = None,
    monroney_standard: list[str] | None = None,
    packages_sections: list[dict[str, Any]] | None = None,
    photo_detected_equipment: list[str] | None = None,
    possible_packages: list[str] | None = None,
    observed_features: list[str] | None = None,
    standalone_features: list[str] | None = None,
) -> list[dict[str, Any]]:
    """One de-duplicated, provenance-badged equipment list for the Options tab.

    Every argument is one of the VDP's existing equipment sources — pass
    whatever the car has; missing ones are simply skipped. Returns a flat list
    of ``{name, tier, badge_label, price, price_display}`` in priority order
    (highest-confidence source first), each name appearing exactly once.
    """
    seen: dict[str, dict[str, Any]] = {}
    order: list[str] = []

    # 1. A real, parsed OEM window sticker — the single most authoritative
    # source, when this car has one. ``build_generated_spec_sheet`` (and so
    # ``catalog``) is only ever computed when this car has NO real sticker
    # (see backend/routes/cars_pages.py), so this tier and "catalog" below
    # are mutually exclusive in practice, never actually competing.
    if sticker_option_sections and isinstance(sticker_option_sections, dict):
        for entry in sticker_option_sections.get("packages") or []:
            _unified_add_sticker_entry(seen, order, entry)
        for entry in sticker_option_sections.get("options") or []:
            _unified_add_sticker_entry(seen, order, entry)
        base = sticker_option_sections.get("base")
        if isinstance(base, dict):
            for feat in base.get("features") or []:
                fname = feat.get("name") if isinstance(feat, dict) else feat
                _unified_add(seen, order, fname, tier="sticker", badge_label="Window sticker")
    elif sticker_option_groups:
        for entry in sticker_option_groups:
            _unified_add_sticker_entry(seen, order, entry)
    elif sticker_options:
        for entry in sticker_options:
            _unified_add_sticker_entry(seen, order, entry)

    # 2. This module's own synthesized catalog — already deduped internally
    # and each entry already carries its own provenance (OEM sticker / trim
    # catalog / sticker photo).
    if catalog:
        for entry in (catalog.get("packages") or []) + (catalog.get("options") or []):
            if entry.get("from_sticker"):
                label = "Window sticker"
            elif entry.get("from_registry"):
                label = "Trim catalog"
            else:
                label = entry.get("source_label") or "Build sheet"
            _unified_add(
                seen,
                order,
                entry.get("name"),
                tier="catalog",
                badge_label=label,
                price=entry.get("price"),
            )

    # 3. Monroney factory data quoted in the listing (not a parsed sticker
    # PDF — the dealer's own printed factory-option and standard-equipment
    # text).
    for name in monroney_options or []:
        _unified_add(seen, order, name, tier="monroney_factory", badge_label="Factory option")
    for name in monroney_standard or []:
        _unified_add(
            seen, order, name, tier="monroney_standard", badge_label="Standard equipment"
        )

    # 4. Packages named in the listing's own description text.
    for pkg in packages_sections or []:
        name = pkg.get("name") if isinstance(pkg, dict) else pkg
        _unified_add(seen, order, name, tier="listing", badge_label="Listing description")
        if isinstance(pkg, dict):
            for feat in pkg.get("features") or []:
                _unified_add(seen, order, feat, tier="listing", badge_label="Listing description")

    # 5. Lowest confidence: equipment guessed from listing photos.
    for name in (photo_detected_equipment or possible_packages or []):
        _unified_add(seen, order, name, tier="photo", badge_label="Photo analysis")
    for name in observed_features or []:
        _unified_add(seen, order, name, tier="photo", badge_label="Photo analysis")
    for name in standalone_features or []:
        _unified_add(seen, order, name, tier="listing", badge_label="Listing description")

    return [seen[k] for k in order]
