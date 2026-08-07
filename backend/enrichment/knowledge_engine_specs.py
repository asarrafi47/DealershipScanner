"""Verified-spec merge and car-detail context assembly.

Mechanically split out of :mod:`backend.enrichment.knowledge_engine`.
All public names remain importable from that module via re-export.
No logic changed; the only addition is a lazy import of the sibling
lookup/format helpers that still live in ``knowledge_engine``.
"""

from __future__ import annotations

import re
from typing import Any


def _sticker_specs_from_packages(car: dict[str, Any]) -> dict[str, Any]:
    """Read merged Monroney fields from ``cars.packages`` JSON."""
    import json

    out: dict[str, Any] = {}
    raw = car.get("packages")
    if not raw or str(raw).strip() in ("{}", "[]", "null"):
        return out
    try:
        parsed = json.loads(raw) if isinstance(raw, str) else raw
    except (TypeError, ValueError, json.JSONDecodeError):
        return out
    if not isinstance(parsed, dict):
        return out

    from backend.scanner.window_sticker import sticker_engine_display_is_valid

    disp = parsed.get("sticker_engine_display")
    if isinstance(disp, str) and sticker_engine_display_is_valid(disp):
        from backend.scanner.window_sticker import upgrade_engine_display_for_etorque

        out["engine_display"] = upgrade_engine_display_for_etorque(disp.strip(), car)
    ft = parsed.get("sticker_fuel_type")
    if isinstance(ft, str) and ft.strip():
        out["fuel_type"] = ft.strip()
    trans = parsed.get("sticker_transmission")
    if isinstance(trans, str) and sticker_engine_display_is_valid(trans):
        out["transmission"] = trans.strip()
    for key, pkg_key in (("mpg_city", "sticker_mpg_city"), ("mpg_highway", "sticker_mpg_highway")):
        val = parsed.get(pkg_key)
        if val is not None:
            try:
                out[key] = int(val)
            except (TypeError, ValueError):
                pass
    return out


# ---------------------------------------------------------------------------
# Extended-spec plausibility guard
#
# ``epa_extended_specs`` was populated by scraping MODEL-level review pages
# (motortrend / caranddriver). A model page headlines whichever variant the
# editors drove, so one set of numbers got written onto every trim/year row of
# the nameplate: all 158 Ram 1500 rows (2013-2026, 2WD through TRX) carry the
# identical 305 hp / 271 lb-ft / 11,580 lb "curb weight" / 4.9 s 0-60.
#
# These checks run at READ time and only ever SUPPRESS a value (render unknown)
# — nothing is written back into ``cars``, per the reference-store rule that
# catalog facts are joined, never copied. A wrong number shown as fact is worse
# than a blank.
# ---------------------------------------------------------------------------

#: Badges that mark a genuine sub-4-second factory performance variant. Matched
#: against trim + title + engine text so a Hellcat, a TRX, a Plaid or an M3
#: keeps its real (very quick) 0-60 instead of being suppressed as impossible.
_PERFORMANCE_VARIANT_RE = re.compile(
    r"(?<![A-Za-z0-9])("
    r"TRX|RHO|HELLCAT|RED\s*EYE|REDEYE|DEMON|SRT|SCAT\s*PACK|TRACKHAWK|"
    r"RAPTOR|SHELBY|GT\s*500|GT\s*350|COBRA|"
    r"AMG|JOHN\s*COOPER\s*WORKS|JCW|TYPE\s*R|NISMO|"
    r"Z06|ZR1|ZL1|BLACKWING|V-?SERIES|QUADRIFOGLIO|"
    r"PLAID|SAPPHIRE|PERFORMANTE|SVJ|SVR|SPECIALE|"
    # "Turbo S" must not be assembled out of "... 3.0L I6 Turbo" + "S-Class",
    # which badged every Mercedes S 500 as a Porsche-grade performance car.
    # BMW's XM is an M-division-only nameplate, so the badge identifies the car
    # outright — and it is the one plug-in hybrid that really does run sub-4.
    r"TURBO\s+S(?![-A-Za-z0-9])|GT2|GT3|GT4|RS[3-9]?|M[2-8]|XM|///M|COMPETITION"
    r")(?![A-Za-z0-9])",
    re.IGNORECASE,
)

#: Nameplates whose curb weight legitimately exceeds the light-duty ceiling:
#: heavy-duty pickups/chassis cabs, full-size vans, and the 9,000 lb-plus
#: battery-electric trucks (Hummer EV, Silverado EV, Escalade IQ).
#:
#: Matched against the MODEL designation only — see :func:`_is_heavy_duty_vehicle`.
_HEAVY_DUTY_MODEL_RE = re.compile(
    r"(?<![A-Za-z0-9])("
    r"[2-7]500(?:\s*HD)?|HD|[EF]-?[2-7]50|SUPER\s*DUTY|CHASSIS|CAB\s*CHASSIS|LOW\s*CAB|"
    r"SPRINTER|PROMASTER|TRANSIT|SAVANA|EXPRESS|NV\s*[23]500|"
    r"HUMMER"
    r")(?![A-Za-z0-9])",
    re.IGNORECASE,
)

#: HD designators that also legitimately live in the TRIM ("Silverado" with trim
#: "2500HD", "F-350" with trim "Super Duty"). Deliberately narrower than the
#: nameplate list above: only tokens that cannot mean anything but a heavy-duty
#: truck. A van nameplate must NOT be here — "Express" is a Chevrolet van *and*
#: a Ram 1500 trim, and a bare "HD" is a trim-level appearance token on plenty of
#: light-duty vehicles.
_HEAVY_DUTY_TRIM_RE = re.compile(
    r"(?<![A-Za-z0-9])("
    r"[2-7]500(?:\s*HD)?|[EF]-?[2-7]50|SUPER\s*DUTY|CAB\s*CHASSIS|CHASSIS\s*CAB"
    r")(?![A-Za-z0-9])",
    re.IGNORECASE,
)

#: Above this, a half-ton pickup / SUV / car curb weight is a parse error, not a
#: vehicle. The heaviest light-duty things on sale (Rivian R1T ~7,150 lb, Ford
#: Lightning ~6,900 lb, G-Class ~5,900 lb) all sit well under it.
_LIGHT_DUTY_CURB_CEILING_LB = 8000

#: A production vehicle needs a reason to break 4 seconds: a performance badge,
#: a nameplate that is quick in base form, or an electric drivetrain. Anything
#: else claiming it is quoting some other trim's number.
_SUB_FOUR_SECOND_SEC = 4.0

#: Nameplates that run sub-4 in ordinary factory form, where a quick number is
#: not evidence of a mis-scrape. Matched on model + trim only: a dealer title is
#: free text and would turn stray tokens into exemptions.
_NATURALLY_QUICK_MODEL_RE = re.compile(
    r"(?<![A-Za-z0-9])("
    # R8 is spelled out because the performance-badge regex matches Audi's RS
    # cars, not its one mid-engine V10 — every R8 ever built runs sub-4.6.
    r"911|718|BOXSTER|CAYMAN|CORVETTE|GT-?R|NSX|VIPER|SUPRA|R8|"
    r"MC20|EMIRA|EVORA|HURACAN|AVENTADOR|REVUELTO|URUS|"
    r"VANTAGE|DBS|DB\d{1,2}|CONTINENTAL\s*GT|TAYCAN"
    r")(?![A-Za-z0-9])",
    re.IGNORECASE,
)

#: "Mercedes-Benz", "Mercedes", "Mercedes Benz" — dealers use all three.
_MERCEDES_MAKE_RE = re.compile(r"^MERCEDES", re.IGNORECASE)

#: AMG model numbers. Deliberately only the four that AMG owns outright: 43, 45,
#: 53 and 63 name no non-AMG Mercedes, whereas 350/500/580 are the ordinary
#: range and would badge an S 500 as a performance car.
_AMG_NUMERIC_BADGE_RE = re.compile(
    r"(?<![A-Za-z0-9])(43|45|53|63)(?![A-Za-z0-9])",
    re.IGNORECASE,
)

#: Marques with no slow models. Cheaper and safer than enumerating their ranges.
_NATURALLY_QUICK_MAKES = frozenset(
    {
        "PORSCHE", "FERRARI", "LAMBORGHINI", "MCLAREN", "BUGATTI",
        "KOENIGSEGG", "PAGANI", "RIMAC", "LOTUS", "ASTON MARTIN", "LUCID",
    }
)

#: A naturally aspirated pickup / SUV / van does not reach 60 in five seconds.
#: The quickest unbadged ones are well clear of it (Silverado 6.2 V8 ~5.4 s,
#: Yukon Denali 6.2 ~5.9 s, Tundra 5.7 V8 ~6.6 s), so nothing real is lost —
#: but a 305 hp 3.6 V6 half-ton pickup claiming 4.9 s is caught without needing
#: a horsepower column the row no longer carries.
_NA_HEAVY_BODY_FLOOR_SEC = 5.0

_FORCED_INDUCTION_RE = re.compile(
    r"TURBO|SUPERCHARG|BI-?TURBO|ECOBOOST|HURRICANE|TFSI|TSI|TDI|SKYACTIV-X",
    re.IGNORECASE,
)

#: Nameplates whose single stored 0-60 has been traced to one specific variant,
#: with the value that proves it. ``epa_extended_specs`` keeps ONE 0-60 per
#: nameplate: all 152 Ram 1500 rows — 2013 to 2026, 2WD, 4WD, Classic and TRX —
#: read 4.9 s, the 702 hp TRX's figure. A number that cannot distinguish a
#: Tradesman from a TRX describes at most one of them, so it is not attributable
#: to any particular car and never renders. Where a real cohort figure is known,
#: ``_CURATED_ZERO_TO_60`` below supplies it. Keyed on the value as well as the
#: nameplate so that re-scraping the table silently retires the entry.
#: The model matcher is a prefix pattern, not an exact name: dealers ship the
#: cab and box in the model field ("1500 TRX Crew Cab 4X4"), and those rows link
#: to the same contaminated nameplate row as a plain "1500".
_MISATTRIBUTED_ZERO_TO_60: tuple[dict[str, Any], ...] = (
    {
        "make": "RAM",
        "model_re": re.compile(r"^1500(?![0-9])", re.IGNORECASE),
        "value": 4.9,
    },
)

#: Body types that cannot reach 60 in under four seconds without a badge that
#: says so. Sports cars are deliberately absent: a plain 911 Carrera or a base
#: Corvette runs 3.x from the factory, and many of those rows carry no
#: horsepower to check against, so a bare "is it quick?" test would erase real
#: numbers. Two tons of pickup is the case that is never innocent.
_HEAVY_BODY_RE = re.compile(
    r"(?<![A-Za-z0-9])("
    r"TRUCK|PICKUP|CREW\s*CAB|QUAD\s*CAB|EXT(?:ENDED)?\s*CAB|"
    r"VAN|MINIVAN|CARGO|SUV|SPORT\s*UTILITY|CHASSIS"
    r")(?![A-Za-z0-9])",
    re.IGNORECASE,
)


def _spec_text_blob(car: dict[str, Any]) -> str:
    """
    Trim + title + model — the naming fields where a performance badge lives.

    The engine description is deliberately excluded. Stellantis calls the plain
    6.4 V8 an "SRT HEMI" engine, so a Durango R/T read as an SRT and kept the
    Hellcat's 3.4 s. A badge names the car, not its engine.
    """
    parts = (car.get("trim"), car.get("title"), car.get("model"))
    return " ".join(str(p) for p in parts if p)


def is_performance_variant(car: dict[str, Any]) -> bool:
    """True when this car's badging identifies a factory high-performance model."""
    blob = _spec_text_blob(car)
    if _PERFORMANCE_VARIANT_RE.search(blob):
        return True
    # Hyundai's performance line is badged with a bare "N" ("Elantra N",
    # "IONIQ 5 N") — too generic to put in the shared regex, but unambiguous
    # once the make is known. "N Line" is the appearance package, not the car.
    if str(car.get("make") or "").strip().upper() == "HYUNDAI":
        trim = str(car.get("trim") or "").strip().upper()
        if trim == "N" or (trim.endswith(" N") and "N LINE" not in trim):
            return True
    # Mercedes badges its AMG cars with a number, and dealers routinely drop the
    # "AMG" word ("E 63 S", "GLE 53"). Those four numbers are AMG-only, unlike
    # the ordinary range (S 500, E 350, S 580e), so they identify the car by
    # themselves once the make is known.
    if _MERCEDES_MAKE_RE.match(str(car.get("make") or "").strip()):
        if _AMG_NUMERIC_BADGE_RE.search(_spec_text_blob(car)):
            return True
    return False


def _is_heavy_duty_vehicle(car: dict[str, Any]) -> bool:
    """
    True for HD pickups / chassis cabs / full-size vans / heavy BEV trucks.

    Reads the MODEL designation, then a narrow set of HD tokens in the trim —
    never a blob of concatenated free text. Concatenating model + trim + title +
    body_style exempted light-duty trucks from the curb-weight ceiling two ways:
    a normal dealer title ending in a bed length ("... 4x4 Crew Cab 5'7 Box")
    ran into ``body_style`` "Truck" and read as BOX TRUCK, and every Ram 1500
    **Express** matched the Chevrolet Express van. 144 Ram 1500s kept an 11,580
    lb "curb weight" that way.
    """
    if _HEAVY_DUTY_MODEL_RE.search(str(car.get("model") or "")):
        return True
    return bool(_HEAVY_DUTY_TRIM_RE.search(str(car.get("trim") or "")))


def _is_heavy_body(car: dict[str, Any]) -> bool:
    """True for pickups / vans / SUVs — bodies that need a badge to be quick."""
    text = " ".join(
        str(car.get(k) or "")
        for k in ("body_style", "body_style_detail", "model", "title", "trim")
    )
    return bool(_HEAVY_BODY_RE.search(text))


def _is_naturally_quick_nameplate(car: dict[str, Any]) -> bool:
    """True for cars that run sub-4 without any performance badge to say so."""
    if str(car.get("make") or "").strip().upper() in _NATURALLY_QUICK_MAKES:
        return True
    text = " ".join(str(car.get(k) or "") for k in ("model", "trim"))
    return bool(_NATURALLY_QUICK_MODEL_RE.search(text))


def _has_forced_induction(car: dict[str, Any]) -> bool:
    """True when the engine is turbocharged / supercharged."""
    fi = car.get("forced_induction")
    if fi not in (None, "") and str(fi).strip().lower() not in ("0", "false", "none", "na"):
        return True
    text = " ".join(
        str(car.get(k) or "")
        for k in ("engine_description", "engine_display", "trim", "title")
    )
    return bool(_FORCED_INDUCTION_RE.search(text))


def _zero_to_60_is_misattributed(car: dict[str, Any], zero_60: float) -> bool:
    """True when this exact figure is the nameplate-wide value from a model scrape."""
    make = str(car.get("make") or "").strip().upper()
    model = re.sub(r"\s+", " ", str(car.get("model") or "").strip().upper())
    if not make or not model:
        return False
    for entry in _MISATTRIBUTED_ZERO_TO_60:
        if make == entry["make"] and entry["model_re"].match(model):
            if abs(zero_60 - float(entry["value"])) < 0.05:
                return True
    return False


def _is_battery_electric(car: dict[str, Any], specs: dict[str, Any]) -> bool:
    """
    True only for a PURE battery-electric drivetrain.

    A plug-in hybrid deliberately does NOT count. It is an ordinary engine with
    an assist motor and is usually the heaviest car in its range — an S 580e is
    4.4 s, a 750e 4.9 s — so "it plugs in" is no reason to believe a sub-4
    figure. Both of those inherited the S-Class / 7 Series nameplate value
    (3.9 s / 3.5 s) and were the last cars still quoting it. The plug-in hybrids
    that genuinely are this quick carry a badge that says so (AMG S 63 E,
    911 Turbo S E-Hybrid, BMW XM) and pass on that instead.
    """
    fuel = str(car.get("fuel_type") or specs.get("fuel_type") or "").strip().lower()
    if not fuel:
        return False
    if "plug" in fuel:
        return False
    return "electric" in fuel and "gas" not in fuel and "hybrid" not in fuel


def implausible_extended_spec_fields(
    car: dict[str, Any], specs: dict[str, Any]
) -> set[str]:
    """
    Names of extended-spec keys in *specs* that cannot be true for *car*.

    Callers suppress those keys (drop them, or set them to ``None``) so the page
    renders "unknown" instead of a fabricated number.
    """
    bad: set[str] = set()

    def _num(key: str) -> float | None:
        val = specs.get(key)
        if val is None or val == "":
            return None
        try:
            return float(val)
        except (TypeError, ValueError):
            return None

    curb = _num("curb_weight_lb")
    zero_60 = _num("zero_to_60_sec")

    # A curb weight identical to the tow rating is the fingerprint of a towing
    # figure parsed into the weight field (Ram 1500: 11,580 for both). It is NOT
    # acted on by itself: the collision says the row is a model-level scrape, not
    # which column is wrong, and an Audi Q7 stores 4,400 for both where 4,400 is
    # a believable curb weight. Condemning the row wholesale erased a plausible
    # curb weight AND a correct 0-60 on every Q7, 3 Series, Yukon, Ranger and
    # Mustang. The ceiling below is what actually catches a towing figure, and it
    # catches the Ram either way (11,580 > 8,000).
    if curb is not None and curb > _LIGHT_DUTY_CURB_CEILING_LB and not _is_heavy_duty_vehicle(car):
        bad.add("curb_weight_lb")
        if specs.get("curb_weight_kg") is not None:
            # One fact in two units; never leave half of it on the page.
            bad.add("curb_weight_kg")

    if zero_60 is not None and _zero_to_60_implausible(car, specs, zero_60):
        bad.add("zero_to_60_sec")

    return bad


def _zero_to_60_implausible(
    car: dict[str, Any], specs: dict[str, Any], zero_60: float
) -> bool:
    """True when *zero_60* cannot describe *car*."""
    # The figure is the nameplate-wide constant traced to one variant — it says
    # nothing about this car regardless of how quick the car is.
    if _zero_to_60_is_misattributed(car, zero_60):
        return True

    is_bev = _is_battery_electric(car, specs)
    # A badge, a marque or a nameplate that is quick from the factory.
    if is_performance_variant(car) or _is_naturally_quick_nameplate(car):
        return False

    if zero_60 < _SUB_FOUR_SECOND_SEC:
        # Horsepower is NOT consulted. It sits in the same row, scraped in the
        # same pass, so it is not independent evidence: a BMW 740i inherited the
        # 760i's 536 hp and its 3.5 s together and the pair vouched for each
        # other, and most contaminated rows have no hp at all because
        # ``knowledge_engine`` strips a family-wide value. A BEV is the one
        # body-agnostic way to be this quick without a badge, so it keeps the
        # benefit of the doubt; everything else is quoting another trim (2016
        # Accord I4 CVT at 3.0 s, 2023 330i at 3.7 s, S 500 at 3.9 s).
        return not is_bev

    if (
        zero_60 < _NA_HEAVY_BODY_FLOOR_SEC
        and not is_bev
        and _is_heavy_body(car)
        and not _has_forced_induction(car)
    ):
        return True

    return False


def apply_spec_plausibility_guard(
    car: dict[str, Any], specs: dict[str, Any]
) -> dict[str, Any]:
    """Copy of *specs* with the physically impossible values dropped."""
    bad = implausible_extended_spec_fields(car, specs)
    if not bad:
        return specs
    return {k: v for k, v in specs.items() if k not in bad}


#: Hand-verified 0-60 figures for engine cohorts whose stored value provably
#: belongs to a different trim. Keyed on make/model/year-range/engine so the fix
#: covers the whole cohort rather than one VIN. Keep this table small: it exists
#: for cases where the scraped source is known-wrong, not as a spec catalog.
_CURATED_ZERO_TO_60: tuple[dict[str, Any], ...] = (
    {
        # Every Ram 1500 row carries the 702 hp TRX's 4.9 s. The 5.7L HEMI V8
        # (eTorque and not) is a ~6.4 s truck: Car and Driver instrumented the
        # 2019 Ram 1500 Laramie 5.7 eTorque 4x4 at 6.3 s and the non-eTorque
        # 2WD at 6.5 s; the 2013-2018 DS-generation 5.7 tested in the same band.
        "make": "RAM",
        "models": ("1500", "1500 CLASSIC"),
        "years": (2013, 2027),
        "engine_re": re.compile(r"5[.,]7", re.IGNORECASE),
        "cylinders": 8,
        "value": 6.4,
    },
)


def curated_zero_to_60_sec(car: dict[str, Any]) -> float | None:
    """Hand-verified 0-60 for this car's engine cohort, or None when uncurated."""
    make = str(car.get("make") or "").strip().upper()
    model = re.sub(r"\s+", " ", str(car.get("model") or "").strip().upper())
    if not make or not model:
        return None
    try:
        year = int(car.get("year"))
    except (TypeError, ValueError):
        return None
    engine_text = " ".join(
        str(car.get(k) or "")
        for k in ("engine_description", "engine_display", "trim", "title")
    )
    try:
        cyl = int(car.get("cylinders")) if car.get("cylinders") not in (None, "") else None
    except (TypeError, ValueError):
        cyl = None

    for entry in _CURATED_ZERO_TO_60:
        if make != entry["make"] or model not in entry["models"]:
            continue
        lo, hi = entry["years"]
        if not (lo <= year <= hi):
            continue
        if not entry["engine_re"].search(engine_text):
            continue
        want_cyl = entry.get("cylinders")
        # Cylinders confirm the engine when the row has them; a missing count
        # must not veto a displacement match the engine text already made.
        if want_cyl is not None and cyl is not None and cyl != want_cyl:
            continue
        return float(entry["value"])
    return None


# ===========================================================================
# WHICH EXTENDED SPECS MAY REACH A SHOPPER  (2026-08-02)
#
# ``merge_verified_specs`` used to populate eight extended-spec keys straight
# out of ``lookup_epa_extended_specs`` / ``…_by_master_id``. Both of those ended
# in ``_merge_ai_model_specs``, which setdefaulted from ``ai_model_specs`` —
# 2,147 rows, ONE distinct ``source_host`` ('ai-research'), counted 2026-08-02
# against the live database. So a number a model wrote was rendered under a
# heading that says "Horsepower".
#
# Those keys have TWO shopper-visible consumers, not one, and fixing only the
# first is how this defect survived a previous pass:
#   1. ``serialize_car_for_api`` -> ``car.horsepower`` etc. in the car.html spec
#      list (frontend/templates/car.html:570-645) and every API payload built
#      from it.
#   2. ``prepare_car_detail_context(car)["verified_specs"]`` -> the AI chat
#      agent, which json.dumps this dict verbatim into the model's prompt as
#      block (4) "verified specs" (backend/intelligence/ai/agent.py:876 and
#      :1213, via ``_verified_without_dealer_conflicts``). That path reads the
#      merge output DIRECTLY and never touches the serializer, so a suppression
#      written only in ``car_serialize`` does not cover it.
# The gate therefore lives HERE, at the merge, where both consumers see it.
#
# ---------------------------------------------------------------------------
# WHY A SCRAPED VALUE IS STILL NOT ENOUGH
#
# ``epa_extended_specs`` is a MODEL-level scrape: one review page per nameplate,
# its numbers written across every trim and model-year row. Measured 2026-08-02
# over its 49,912 rows:
#   * 7,767 rows carry ``curb_weight_lb == tow_capacity_lb``;
#   * 4,630 of the 22,763 rows with a 0-60 are under 4.0 s.
# So the admission test cannot be "the column is not null". It is the same test
# the generated build sheet already applies (see the block comment above
# ``generated_spec_sheet._ATTRIBUTABLE_SPEC_FIELDS``): the value has to come
# back from a page extraction stored in that row's ``specs_json`` whose TITLE
# names this car's model year, has to be inside a physical-plausibility band,
# has to differ between the trims of its config, and has to survive
# ``implausible_extended_spec_fields``. This module deliberately calls THAT
# function rather than growing a second copy, so the car page and the build
# sheet cannot print different numbers for the same car.
#
# ---------------------------------------------------------------------------
# WHY CURB WEIGHT AND TOW CAPACITY RENDER NOTHING AT ALL
#
# Both were re-measured here rather than inherited (2026-08-02, live database):
#
#   tow_capacity_lb — 9,864 rows carry one, spread over 2,037 distinct
#   (year, make, model) groups, and the number of those groups holding more than
#   one distinct tow value is ZERO. Every nameplate-year stores a single towing
#   figure for all of its trims, so the value cannot tell a Tradesman from a
#   Limited and is not attributable to either. There is no configuration of the
#   quote gate that admits it.
#
#   curb_weight_lb — 23,803 rows carry one; 2,134 are under 2,500 lb and 2,042
#   under 2,100 lb, i.e. lighter than the lightest vehicle sold in the US (a
#   2,095 lb Mirage). The cause is visible: every Mazda CX-50 row from 2023 to
#   2026 stores curb_weight_lb = 2000 AND tow_capacity_lb = 2000 — 2,000 lb is
#   the CX-50's tow rating; it weighs about 3,700. The page extraction carries
#   the same swapped number, so the quote gate cannot catch it either, and
#   nothing in the row says which column is the wrong one.
#
# A blank row beats an invented number, so both are ``None`` unconditionally.
# ``curb_weight_kg`` goes with ``curb_weight_lb``: one fact in two units.
# ===========================================================================

#: The only extended-spec measurements that may render, and the only keys
#: ``sourced_extended_specs`` returns.
SOURCED_EXTENDED_SPEC_FIELDS: tuple[str, ...] = (
    "horsepower",
    "torque_lb_ft",
    "zero_to_60_sec",
)

#: Extended-spec keys ``merge_verified_specs`` always reports as ``None``.
#: ``fuel_tank_gal`` and ``ev_range_miles`` are absent from this tuple because
#: they are not blank — they are re-resolved from their own document-backed
#: resolvers in ``backend.intelligence`` (see ``merge_verified_specs``).
BLANK_EXTENDED_SPEC_FIELDS: tuple[str, ...] = (
    "curb_weight_lb",
    "curb_weight_kg",
    "tow_capacity_lb",
    "battery_kwh",
)


def sourced_extended_specs(car: dict[str, Any]) -> dict[str, float]:
    """
    hp / torque / 0-60 that a named page for THIS car's model year printed.

    ``{}`` — meaning "render nothing" — whenever the gate is not met. Never a
    family average, never an ``ai_model_specs`` value, never an estimate.

    Delegates to ``generated_spec_sheet._attributable_extended_specs``, which is
    the shipped implementation of that gate and the one the build sheet uses. It
    is private only because it was written for a single caller; routing the car
    page through a second copy is how the two would drift apart. A failure to
    import or query returns ``{}``: unknown provenance is not permission.
    """
    if not car:
        return {}
    try:
        from backend.enrichment.generated_spec_sheet import _attributable_extended_specs

        attributed = _attributable_extended_specs(car)
    except Exception:
        return {}
    out: dict[str, float] = {}
    for field in SOURCED_EXTENDED_SPEC_FIELDS:
        entry = attributed.get(field)
        if isinstance(entry, dict) and entry.get("value") is not None:
            try:
                out[field] = float(entry["value"])
            except (TypeError, ValueError):
                continue
    return out


def _document_backed_fuel_tank_gal(car: dict[str, Any]) -> float | None:
    """The tank figure the car page and the build sheet already show, or None."""
    try:
        from backend.intelligence.tco_fuel_estimates import resolve_fuel_tank_gallons

        gal = resolve_fuel_tank_gallons(car)
    except Exception:
        return None
    return round(gal, 1) if gal is not None else None


def _document_backed_factory_range(car: dict[str, Any]) -> int | None:
    """The factory EPA electric range the car page already shows, or None."""
    try:
        from backend.intelligence.ev_range_estimates import resolve_factory_epa_range

        return resolve_factory_epa_range(car)
    except Exception:
        return None


def merge_verified_specs(
    car: dict[str, Any], *, include_extended_specs: bool = True
) -> dict[str, Any]:
    """
    Combine dealer row with regex decoder + EPA lookup.
    Prefer: regex (brand trim) > EPA aggregate > dealer fields.
    When dealer omits or sends N/A, show inferred values as verified.

    *include_extended_specs* (default True) controls the document-backed
    resolvers that populate horsepower / torque / 0-60 / fuel tank / EV range.
    Those keys feed display only; nothing about the spec-sheet gap detection
    (engine / transmission / drivetrain / cylinders / fuel / colors) reads them.
    Batch callers that don't need them (the incomplete-listings index rebuild)
    pass False to skip the per-car reference queries — the returned dict is
    identical except those keys are ``None``.

    ``curb_weight_lb``, ``tow_capacity_lb`` and ``battery_kwh`` are ``None``
    either way: no per-trim source for them exists. See the block comment above
    :data:`SOURCED_EXTENDED_SPEC_FIELDS` for the measurements behind that.
    """
    from backend.enrichment.knowledge_engine import (
        _drivetrain_ui_label,
        _is_na_spec,
        _transmission_has_gear_detail,
        build_master_engine_string,
        decode_trim_logic,
        format_fuel_economy_display,
        format_transmission_display,
        lookup_epa_aggregate,
        lookup_epa_by_trim,
        lookup_epa_master_by_id,
        lookup_vpic_from_cache,
    )
    from backend.utils.field_clean import clean_car_row_dict

    car = clean_car_row_dict(dict(car))
    make = car.get("make") or ""
    model = car.get("model") or ""
    trim = car.get("trim") or ""
    title = car.get("title") or ""
    year = car.get("year")
    try:
        y = int(year) if year is not None else None
    except (TypeError, ValueError):
        y = None

    def _int_or_none(v: Any) -> int | None:
        if v is None or v == "":
            return None
        try:
            return int(v)
        except (TypeError, ValueError):
            return None

    dealer_cyl = car.get("cylinders")
    dealer_drive = (car.get("drivetrain") or "").strip()
    dealer_trans = (car.get("transmission") or "").strip()

    title_for_decode = (title or "").strip()
    dealer_ft = (car.get("fuel_type") or "").strip()
    if (make or "").strip().upper() == "BMW" and dealer_ft:
        title_for_decode = f"{title_for_decode} {dealer_ft}".strip()
    regex = decode_trim_logic(make, model, trim, title_for_decode)
    # Resolved catalog link first (cars.epa_master_id, written by the one
    # resolver in backend.catalog): exact row + exact extended-specs FK, no
    # fuzzy re-matching. Fuzzy per-trim/aggregate lookups remain the fallback
    # for unlinked cars.
    linked_id = car.get("epa_master_id")
    try:
        from backend.catalog.generations import generation_for

        _generation = generation_for(make, model, y)
    except Exception:
        _generation = None
    epa_trim = lookup_epa_master_by_id(linked_id) if linked_id else {}
    linked_exact = bool(epa_trim)  # True only when the by-id row actually resolved
    if not epa_trim:
        # Per-trim lookup (exact match from build_epa_master.py data), then aggregate fallback
        epa_trim = lookup_epa_by_trim(y, make, model, trim) if trim else {}
    epa = lookup_epa_aggregate(
        y, make, model, title=title_for_decode, trim=trim,
        prefer_cylinders=_int_or_none(dealer_cyl),
    )
    # Merge: per-trim values win over aggregate for any key they provide
    epa = {**epa, **{k: v for k, v in epa_trim.items() if v is not None}}
    if include_extended_specs:
        # Only the figures a named page for THIS car's model year actually
        # printed — see the "WHICH EXTENDED SPECS MAY REACH A SHOPPER" block
        # above. ``lookup_epa_extended_specs`` is deliberately NOT called here
        # any more: its rows are a model-level scrape, and a raw column read is
        # what put 185 hp / 200 lb-ft / 3,000 lb on a 2001 SLK Kompressor.
        sourced = sourced_extended_specs(car)
        curated_060 = curated_zero_to_60_sec(car)
        tank_gal = _document_backed_fuel_tank_gal(car)
        factory_range = _document_backed_factory_range(car)
    else:
        sourced = {}
        curated_060 = None
        tank_gal = None
        factory_range = None

    from backend.enrichment.model_specs_dictionary import lookup_model_specs_dictionary

    dict_specs = lookup_model_specs_dictionary(make, model)
    vpic = lookup_vpic_from_cache(car.get("vin"))

    # Resolved catalog row beats the regex trim decoder for cylinders — the
    # decoder is era-blind ("E 350" decodes to the modern turbo-four) while the
    # link was scored against this car's own engine data. Only when the by-id
    # row actually resolved: a fuzzy fallback must not inherit this authority.
    cyl_ver = _int_or_none(epa_trim.get("cylinders")) if linked_exact else None
    if cyl_ver is None:
        cyl_ver = regex.get("cylinders")
    if cyl_ver is None:
        cyl_ver = epa.get("cylinders")
    if cyl_ver is None:
        di = _int_or_none(dealer_cyl)
        if di is not None:
            cyl_ver = di
    if cyl_ver is None and dict_specs and dict_specs.get("cylinders") is not None:
        try:
            cyl_ver = int(dict_specs["cylinders"])
        except (TypeError, ValueError):
            cyl_ver = None
    if cyl_ver is None and vpic.get("cylinders") is not None:
        cyl_ver = vpic["cylinders"]
    if cyl_ver is None:
        # Last resort: the answer is often sitting in the listing's own text —
        # e.g. an Infiniti Q70L whose trim reads "Sedan V-6 cyl". Read the
        # layout token from trim/engine text only (skipping BEVs, which have
        # no cylinders and never carry a V-N badge).
        #
        # The TITLE is deliberately excluded: it carries the model designator,
        # and BMW "i8"/"i4"/"i7" and Hummer "H2"/"H3" collide with the V/I/H/W
        # layout pattern ("i8" -> 8 cyl, "H2" -> 2 cyl). A genuine cylinder
        # badge lives in the trim or engine description, not the model name.
        from backend.utils.engine_consistency import cylinders_from_engine_text, is_bev_fuel

        if not is_bev_fuel(dealer_ft or epa.get("fuel_type")):
            cyl_ver = cylinders_from_engine_text(
                " ".join(x for x in (trim, car.get("engine_description")) if x)
            )

    drive_ver = regex.get("drivetrain") or epa.get("drivetrain")
    if not drive_ver and dealer_drive and not _is_na_spec(dealer_drive):
        drive_ver = dealer_drive
    if not drive_ver and dict_specs and dict_specs.get("drivetrain"):
        drive_ver = str(dict_specs["drivetrain"]).strip()
    if not drive_ver and vpic.get("drivetrain"):
        drive_ver = vpic["drivetrain"]

    gears_ver = regex.get("gears") or epa.get("gears")
    # Priority: trim-decoder year-aware hint > EPA > vPIC > model_specs > dealer.
    # transmission_hint encodes year logic (e.g. Transit pre-/post-2020); must beat model_specs.
    _trans_hint = regex.get("transmission_hint")
    if _trans_hint and not epa.get("transmission") and not (dealer_trans and not _is_na_spec(dealer_trans)):
        trans_raw = _trans_hint
    else:
        epa_trany = epa.get("transmission")
        trans_raw = epa_trany or (
            dealer_trans if dealer_trans and not _is_na_spec(dealer_trans) else None
        )
        # EPA aggregate is often generic "Automatic" while the VDP lists "8-Speed Automatic".
        dealer_ok = dealer_trans and not _is_na_spec(dealer_trans)
        if (
            dealer_ok
            and _transmission_has_gear_detail(dealer_trans)
            and not _transmission_has_gear_detail(epa_trany or "")
        ):
            trans_raw = dealer_trans
        if not trans_raw and vpic.get("transmission"):
            trans_raw = vpic["transmission"]
        if not trans_raw and dict_specs and dict_specs.get("transmission"):
            trans_raw = str(dict_specs["transmission"]).strip()
    trans_ver = format_transmission_display(trans_raw) or trans_raw

    # Trim decoder often knows "8-Speed Automatic" while EPA row is generic "Automatic".
    _dec_hint = regex.get("transmission_hint")
    if (
        _dec_hint
        and _transmission_has_gear_detail(_dec_hint)
        and not _transmission_has_gear_detail(str(trans_ver or ""))
    ):
        trans_ver = format_transmission_display(_dec_hint) or _dec_hint
    elif gears_ver is not None and str(trans_ver or "").strip().lower() in ("automatic", "auto"):
        try:
            _ng = int(gears_ver)
            if _ng > 0:
                trans_ver = f"{_ng}-Speed Automatic"
        except (TypeError, ValueError):
            pass

    dealer_cyl_i = _int_or_none(dealer_cyl)

    blob_full = f"{title} {trim} {model}".strip()
    # Dealer-supplied fuel_type "Electric" / "Electricity" (pure BEV, not PHEV/hybrid)
    _dealer_ft_lower = (car.get("fuel_type") or "").strip().lower()
    _dealer_is_pure_ev = (
        "electric" in _dealer_ft_lower
        and not any(x in _dealer_ft_lower for x in ("gas", "gasoline", "hybrid", "plug"))
    )
    if _dealer_is_pure_ev:
        # Dealer "Electric" labels have landed on gas/hybrid cars (GX 550 gas
        # V6 ×25). Forcing cylinders_display=0 off that label made the engine
        # line read "Electric" too — the bad label erased its own disproof. Only
        # let the dealer label force BEV treatment when the row's own evidence
        # does not contradict it; catalog/sticker/decoder BEV signals below are
        # unaffected.
        try:
            from backend.utils.fuel_label_plausibility import (
                PLAUSIBLE,
                assess_electric_claim,
            )

            if assess_electric_claim(car).verdict != PLAUSIBLE:
                _dealer_is_pure_ev = False
        except Exception:
            pass
    # Fuel-cell vehicles (Mirai, NEXO) are electric-drive: no cylinders, MPGe.
    # EPA dual-fuel strings ("Premium Gasoline / Electricity" = PHEV) must not count.
    _epa_fuel_lower = (epa.get("fuel_type") or "").lower()
    is_bev = (
        regex.get("cylinders") == 0
        or (regex.get("fuel_type_hint") or "").strip().lower() == "electric"
        or (epa.get("atv_type") or "").strip().upper() in ("EV", "FCV")
        or ("electric" in _epa_fuel_lower and "gas" not in _epa_fuel_lower)
        or "hydrogen" in _dealer_ft_lower
        or _dealer_is_pure_ev
    )
    try:
        from backend.scanner.window_sticker import dodge_charger_daytona_is_bev

        if dodge_charger_daytona_is_bev(car):
            is_bev = True
    except Exception:
        pass

    sticker_pkg = _sticker_specs_from_packages(car)
    if sticker_pkg.get("fuel_type") == "Electric":
        is_bev = True
    if is_bev:
        cyl_ver = 0
        display_cyl = 0
    elif dealer_cyl_i is not None and dealer_cyl_i > 0:
        display_cyl = dealer_cyl_i
    elif cyl_ver is not None:
        display_cyl = cyl_ver
    else:
        display_cyl = dealer_cyl_i

    cylinders_verified = bool(
        display_cyl is not None
        and (_is_na_spec(dealer_cyl) or dealer_cyl_i in (None, 0))
        and (regex.get("cylinders") is not None or epa.get("cylinders") is not None)
    )

    # Regex/EPA first so xDrive/4MATIC in title wins over dealer placeholders.
    if drive_ver:
        display_drive = drive_ver
    elif not _is_na_spec(dealer_drive):
        display_drive = dealer_drive
    else:
        display_drive = ""

    drivetrain_verified = bool(
        drive_ver
        and (display_drive or "").strip().upper() == (drive_ver or "").strip().upper()
        and _is_na_spec(dealer_drive)
    )

    blob_full = f"{title} {trim} {model}".strip()
    display_drive_ui = _drivetrain_ui_label(display_drive, make, blob_full)

    if is_bev and not trans_ver and _is_na_spec(dealer_trans):
        trans_ver = sticker_pkg.get("transmission") or "Single-speed automatic"

    body_style_display = None
    from backend.utils.field_clean import is_jeep_wrangler_car, normalize_body_style_for_car

    if is_jeep_wrangler_car(make, model, trim, title):
        body_style_display = "SUV"
    elif _is_na_spec(car.get("body_style")) and regex.get("body_style_hint"):
        body_style_display = regex["body_style_hint"]
    if not body_style_display and _is_na_spec(car.get("body_style")) and vpic.get("body_style"):
        body_style_display = vpic["body_style"]
    if not is_jeep_wrangler_car(make, model, trim, title):
        corrected_bs = normalize_body_style_for_car(
            car.get("body_style") if not _is_na_spec(car.get("body_style")) else body_style_display,
            make=make,
            model=model,
            trim=trim,
            title=title,
        )
        if corrected_bs:
            body_style_display = corrected_bs
    fuel_economy_display = format_fuel_economy_display(epa, is_bev)
    if is_bev and sticker_pkg.get("mpg_city") and sticker_pkg.get("mpg_highway"):
        fuel_economy_display = format_fuel_economy_display(
            {
                "city08": sticker_pkg.get("mpg_city"),
                "highway08": sticker_pkg.get("mpg_highway"),
            },
            True,
        )
    if not fuel_economy_display and not is_bev:
        from backend.utils.field_clean import format_mpg_city_highway_display

        fuel_economy_display = format_mpg_city_highway_display(
            car.get("mpg_city"), car.get("mpg_highway")
        )
    master_engine_string = build_master_engine_string(make, model, trim, title, regex, epa)

    sources = []
    if (
        regex.get("cylinders") is not None
        or regex.get("drivetrain")
        or regex.get("fuel_type_hint")
        or regex.get("body_style_hint")
        or regex.get("transmission_hint")
    ):
        sources.append("Trim decoder")
    if epa.get("cylinders") is not None or epa.get("drivetrain") or epa.get("gears") or epa.get("transmission"):
        sources.append("EPA dataset")
    if dict_specs:
        sources.append("Model specs dictionary")

    # Normalize display string only when it's a raw shorthand (no speed count already present).
    # normalize_transmission_standard is a bucket classifier; applying it to strings that already
    # contain speed info ("10-Speed Automatic") would strip the count to just "Automatic".
    if trans_ver and not _is_na_spec(str(trans_ver)) and not re.search(r"\b\d+[-\s]?speed\b", trans_ver, re.I):
        from backend.utils.transmission_normalize import normalize_transmission_standard
        vin_raw = car.get("vin")
        vin_s = str(vin_raw).strip() if vin_raw not in (None, "") else None
        norm_t, _weak = normalize_transmission_standard(
            trans_ver,
            make=make,
            model=model,
            trim=trim,
            title=title_for_decode,
            year=y,
            vin=vin_s,
        )
        if norm_t:
            trans_ver = norm_t

    return {
        "cylinders": cyl_ver,
        "cylinders_display": display_cyl,
        "cylinders_verified": cylinders_verified,
        "drivetrain": drive_ver,
        "drivetrain_display": display_drive_ui,
        "drivetrain_verified": drivetrain_verified,
        "gears": gears_ver,
        "transmission_display": trans_ver,
        "fuel_type_hint": regex.get("fuel_type_hint"),
        "sources": sources,
        "dealer_cylinders": dealer_cyl,
        "master_engine_string": master_engine_string,
        "fuel_economy_display": fuel_economy_display,
        "epa_displacement": epa.get("displacement") or vpic.get("engine_l"),
        "body_style_display": body_style_display or epa.get("body_style"),
        # Per-trim EPA fields (populated from DICTIONARY via build_epa_master.py)
        "epa_fuel_type": epa.get("fuel_type"),
        "epa_engine_description": epa_trim.get("engine_description"),
        "epa_city08": epa.get("city08"),
        "epa_highway08": epa.get("highway08"),
        # Extended specs. Every key below is either quoted from a page this row
        # names, or None. See the block comment above SOURCED_EXTENDED_SPEC_FIELDS.
        "horsepower": sourced.get("horsepower"),
        "torque_lb_ft": sourced.get("torque_lb_ft"),
        # Curated cohort figure wins outright: it is only present where the
        # stored value is known to belong to a different trim.
        "zero_to_60_sec": (
            curated_060 if curated_060 is not None else sourced.get("zero_to_60_sec")
        ),
        "fuel_tank_gal": tank_gal,
        "ev_range_miles": factory_range,
        # BLANK_EXTENDED_SPEC_FIELDS — no per-trim source exists for any of
        # these, so they are blank rather than approximate.
        "curb_weight_lb": None,
        "battery_kwh": None,
        "tow_capacity_lb": None,
        # Catalog link + generation (backend.catalog; see docs/data_architecture_plan.md)
        "epa_master_id": linked_id,
        "generation_code": _generation.get("generation") if _generation else None,
        "generation_years": (
            f"{_generation['year_start']}–{_generation['year_end'] or 'present'}" if _generation else None
        ),
        "generation_notes": _generation.get("notes") if _generation else None,
    }


def prepare_car_detail_context(car: dict[str, Any]) -> dict[str, Any]:
    """Attach verified_specs + normalized gallery list for templates."""
    import json

    if not car:
        return {}
    g = car.get("gallery")
    if isinstance(g, str):
        try:
            g = json.loads(g)
        except (TypeError, ValueError):
            g = []
    if not isinstance(g, list):
        g = []
    from backend.vision.url_heuristics import (
        filter_public_gallery_urls,
        heuristic_listing_gallery_fluff_url,
        prefer_full_gallery_url,
    )

    urls = filter_public_gallery_urls([u for u in g if u and isinstance(u, str)])
    if not urls and car.get("image_url"):
        iu = car.get("image_url")
        if isinstance(iu, str) and iu.strip() and not heuristic_listing_gallery_fluff_url(iu):
            urls = [prefer_full_gallery_url(iu.strip())]
    verified = merge_verified_specs(car)

    listing_packages_sections: list[dict[str, Any]] = []
    listing_standalone_features: list[str] = []
    listing_observed_features: list[str] = []
    listing_monroney_options: list[str] = []
    listing_monroney_standard: list[str] = []
    listing_sticker_options: list[str] = []
    listing_sticker_option_groups: list[dict[str, Any]] = []
    listing_sticker_option_sections: dict[str, Any] = {}
    listing_possible_packages: list[str] = []
    listing_photo_detected_equipment: list[str] = []
    sticker_exterior_color: str | None = None
    sticker_interior_color: str | None = None
    sticker_interior_material: str | None = None
    sticker_spec_lines: list[dict[str, str]] = []
    interior_from_listing_description = False
    interior_from_llava_vision = False
    llava_interior_section: dict[str, Any] | None = None
    hide_photo_analysis = False

    def _normalized_pkg_title(entry: dict[str, Any]) -> str:
        for key in ("name", "canonical_name", "name_verbatim"):
            v = entry.get(key)
            if v is not None and str(v).strip():
                return str(v).strip()[:200]
        return ""

    pkg_raw = car.get("packages")
    pj: dict[str, Any] | None = None
    if pkg_raw and str(pkg_raw).strip() not in ("{}", "[]", "null"):
        try:
            parsed = json.loads(pkg_raw) if isinstance(pkg_raw, str) else pkg_raw
        except (TypeError, ValueError, json.JSONDecodeError):
            parsed = None
        if isinstance(parsed, dict):
            pj = parsed

    try:
        from backend.enrichment.window_sticker_service import should_skip_photo_package_analysis

        hide_photo_analysis = should_skip_photo_package_analysis(car, pj)
    except Exception:
        hide_photo_analysis = False
    if not hide_photo_analysis:
        try:
            from backend.scanner.window_sticker import oem_hide_photo_analysis

            hide_photo_analysis = oem_hide_photo_analysis(
                str(car.get("vin") or ""),
                str(car.get("make") or ""),
            )
        except Exception:
            hide_photo_analysis = False

    if pj is not None:
        liv = pj.get("llava_interior_cabin")
        if isinstance(liv, dict) and liv and not hide_photo_analysis:
            ib = liv.get("interior_buckets") or []
            bucket_list = [str(x).strip() for x in ib if str(x).strip()][:24]
            llava_interior_section = {
                "guess": str(liv.get("interior_guess_text") or "").strip()[:240],
                "buckets": bucket_list,
                "evidence": str(liv.get("evidence") or "").strip()[:400],
                "confidence": liv.get("confidence"),
            }
        trim_low = str(car.get("trim") or "").strip().lower()
        seen_titles: set[str] = set()
        for entry in pj.get("packages_normalized") or []:
            if not isinstance(entry, dict):
                continue
            title = _normalized_pkg_title(entry)
            if not title:
                continue
            low = title.lower()
            if low in seen_titles:
                continue
            feats = entry.get("features") or []
            feat_list = [str(x).strip() for x in feats if isinstance(x, str) and str(x).strip()]
            evidence = entry.get("evidence_spans") or []
            evidence_list = [
                str(x).strip() for x in evidence if isinstance(x, str) and str(x).strip()
            ][:8]
            # Trim names are not option packages — skip empty accordions that only repeat trim.
            if trim_low and low == trim_low and not feat_list and not evidence_list:
                continue
            seen_titles.add(low)
            listing_packages_sections.append(
                {
                    "name": title,
                    "features": feat_list[:30],
                    "evidence": evidence_list,
                    "source": "listing",
                    "from_listing_description": True,
                    "from_vision": False,
                }
            )

        mo = pj.get("monroney_options")
        if isinstance(mo, list):
            try:
                from backend.scanner.window_sticker import is_sticker_boilerplate_line
            except ImportError:
                is_sticker_boilerplate_line = None
            for x in mo:
                s = str(x).strip()[:500]
                if not s:
                    continue
                if is_sticker_boilerplate_line and is_sticker_boilerplate_line(s):
                    continue
                try:
                    from backend.scanner.window_sticker import _is_sticker_factory_code_noise
                except ImportError:
                    _is_sticker_factory_code_noise = None
                if _is_sticker_factory_code_noise and _is_sticker_factory_code_noise(s):
                    continue
                listing_monroney_options.append(s)
        listing_monroney_options = listing_monroney_options[:60]

        mstd = pj.get("monroney_standard_highlights")
        if isinstance(mstd, list):
            try:
                from backend.scanner.window_sticker import is_sticker_boilerplate_line as _is_boilerplate
            except ImportError:
                _is_boilerplate = None
            for x in mstd:
                s = str(x).strip()[:500]
                if not s:
                    continue
                if _is_boilerplate and _is_boilerplate(s):
                    continue
                listing_monroney_standard.append(s)
        listing_monroney_standard = listing_monroney_standard[:40]

        so = pj.get("sticker_options")
        listing_sticker_option_groups: list[dict[str, Any]] = []
        listing_sticker_option_sections: dict[str, Any] = {}
        try:
            from backend.scanner.window_sticker import (
                group_sticker_options_for_display,
                sticker_option_sections_for_display,
                sticker_options_for_display,
            )

            listing_sticker_options = sticker_options_for_display(pj)
            listing_sticker_option_groups = group_sticker_options_for_display(listing_sticker_options)
            listing_sticker_option_sections = sticker_option_sections_for_display(pj)
        except Exception:
            listing_sticker_options = []
            listing_sticker_option_groups = []
            listing_sticker_option_sections = {}
        if not listing_sticker_options and isinstance(so, list):
            seen_so: set[str] = set()
            for x in so:
                s = str(x).strip()[:200]
                if not s:
                    continue
                k = s.lower()
                if k in seen_so:
                    continue
                seen_so.add(k)
                listing_sticker_options.append({"name": s, "price": None, "label": s})
        listing_sticker_options = listing_sticker_options[:80]

        sec = pj.get("sticker_exterior_color")
        if isinstance(sec, str) and sec.strip():
            sticker_exterior_color = re.sub(r"^:\s*", "", sec.strip())[:120]
        sic = pj.get("sticker_interior_color")
        if isinstance(sic, str) and sic.strip():
            sticker_interior_color = re.sub(r"^:\s*", "", sic.strip())[:120]
        sim = pj.get("sticker_interior_material")
        if isinstance(sim, str) and sim.strip():
            sticker_interior_material = sim.strip()[:120]

        ss = pj.get("sticker_specs")
        if isinstance(ss, dict):
            for label, val in ss.items():
                if not isinstance(label, str) or not isinstance(val, str):
                    continue
                s = re.sub(r"^:\s*", "", val.strip())
                if not s:
                    continue
                sticker_spec_lines.append({"label": label.strip()[:40], "value": s[:160]})
        if not sticker_spec_lines:
            for label, key in (
                ("Engine", "sticker_engine_display"),
                ("Transmission", "sticker_transmission"),
                ("Drivetrain", "sticker_drivetrain"),
                ("Doors", "sticker_doors"),
                ("Seating", "sticker_seating"),
                ("Tires", "sticker_tires"),
                ("Wheels", "sticker_wheels"),
            ):
                val = pj.get(key)
                if isinstance(val, str) and val.strip():
                    v = re.sub(r"^:\s*", "", val.strip())
                    if v:
                        sticker_spec_lines.append({"label": label, "value": v[:160]})

        _mk = str(car.get("make") or "").strip().lower()
        _hide_possible = hide_photo_analysis and _mk in {"jeep", "chrysler", "dodge", "ram"}
        if not _hide_possible and not listing_packages_sections:
            for raw in pj.get("possible_packages") or []:
                if not isinstance(raw, str):
                    continue
                label = raw.strip()[:200]
                if not label:
                    continue
                low = label.lower()
                if low in seen_titles:
                    continue
                seen_titles.add(low)
                listing_possible_packages.append(label)
        else:
            listing_possible_packages = []

        seen_sf: set[str] = set()
        sf = pj.get("standalone_features_from_description")
        if isinstance(sf, list):
            for x in sf:
                s = str(x).strip()[:200]
                if not s:
                    continue
                k = s.lower()
                if k in seen_sf:
                    continue
                seen_sf.add(k)
                listing_standalone_features.append(s)
        listing_standalone_features = listing_standalone_features[:40]

        if listing_sticker_options:
            try:
                from backend.scanner.window_sticker import (
                    build_sticker_option_dedupe_keys,
                    normalize_option_dedupe_key,
                    option_name_overlaps_sticker,
                )

                sticker_keys = build_sticker_option_dedupe_keys(listing_sticker_options)
                base_sec = (listing_sticker_option_sections or {}).get("base") or {}
                for feat in base_sec.get("features") or []:
                    if isinstance(feat, dict):
                        fn = str(feat.get("name") or "").strip()
                        if fn:
                            sticker_keys.add(normalize_option_dedupe_key(fn))
                if sticker_keys:
                    filtered_sections: list[dict[str, Any]] = []
                    for sec in listing_packages_sections:
                        title = str(sec.get("name") or "").strip()
                        if title and option_name_overlaps_sticker(title, sticker_keys):
                            continue
                        feats = [
                            f
                            for f in (sec.get("features") or [])
                            if isinstance(f, str)
                            and not option_name_overlaps_sticker(f, sticker_keys)
                        ]
                        evidence = [
                            e
                            for e in (sec.get("evidence") or [])
                            if isinstance(e, str) and str(e).strip()
                        ]
                        if not feats and not evidence:
                            continue
                        sec_copy = dict(sec)
                        sec_copy["features"] = feats[:30]
                        sec_copy["evidence"] = evidence[:8]
                        filtered_sections.append(sec_copy)
                    listing_packages_sections = filtered_sections
                    listing_monroney_options = [
                        x
                        for x in listing_monroney_options
                        if not option_name_overlaps_sticker(x, sticker_keys)
                    ]
                    listing_standalone_features = [
                        x
                        for x in listing_standalone_features
                        if not option_name_overlaps_sticker(x, sticker_keys)
                    ]
                    listing_monroney_standard = [
                        x
                        for x in listing_monroney_standard
                        if not option_name_overlaps_sticker(x, sticker_keys)
                    ]
            except Exception:
                pass

        seen_obs: set[str] = set()
        obs = pj.get("observed_features")
        if isinstance(obs, list) and not hide_photo_analysis:
            for x in obs:
                s = str(x).strip()[:200]
                if not s:
                    continue
                k = s.lower()
                if k in seen_obs:
                    continue
                seen_obs.add(k)
                listing_observed_features.append(s)
        listing_observed_features = listing_observed_features[:40]

        try:
            from backend.vision.equipment_vision import collect_photo_detected_equipment

            photo_only = [] if hide_photo_analysis else collect_photo_detected_equipment(pj)
        except Exception:
            photo_only = [] if hide_photo_analysis else list(dict.fromkeys(
                listing_observed_features + listing_possible_packages
            ))[:48]
        try:
            from backend.utils.listing_description_extract import collect_equipment_options_from_packages

            listing_photo_detected_equipment = collect_equipment_options_from_packages(
                pj,
                photo_equipment=photo_only,
                include_vision=not hide_photo_analysis,
            )
        except Exception:
            listing_photo_detected_equipment = photo_only
    spec_raw = car.get("spec_source_json")
    if spec_raw and str(spec_raw).strip():
        try:
            sj = json.loads(spec_raw) if isinstance(spec_raw, str) else spec_raw
        except (TypeError, ValueError, json.JSONDecodeError):
            sj = None
        if isinstance(sj, dict):
            ic = sj.get("interior_color")
            if isinstance(ic, dict) and str(ic.get("source") or "").strip().lower() == "listing_description":
                interior_from_listing_description = True
            if isinstance(ic, dict) and str(ic.get("source") or "").strip().lower() == "llava_vision":
                interior_from_llava_vision = True
            icv = sj.get("interior_cabin_vision")
            if isinstance(icv, dict) and str(icv.get("source") or "").strip().lower() == "llava_vision":
                interior_from_llava_vision = True

    packages_panel_has_content = bool(
        listing_packages_sections
        or listing_standalone_features
        or listing_observed_features
        or listing_photo_detected_equipment
        or listing_monroney_options
        or listing_monroney_standard
        or listing_sticker_options
        or listing_possible_packages
        or sticker_exterior_color
        or sticker_interior_color
        or sticker_interior_material
        or sticker_spec_lines
        or llava_interior_section
    )

    return {
        "gallery_images": urls,
        "verified_specs": verified,
        "listing_packages_sections": listing_packages_sections,
        "listing_standalone_features": listing_standalone_features,
        "listing_observed_features": listing_observed_features,
        "listing_monroney_options": listing_monroney_options,
        "listing_monroney_standard": listing_monroney_standard,
        "listing_sticker_options": listing_sticker_options,
        "listing_sticker_option_groups": listing_sticker_option_groups,
        "listing_sticker_option_sections": listing_sticker_option_sections,
        "listing_possible_packages": listing_possible_packages,
        "listing_photo_detected_equipment": listing_photo_detected_equipment,
        "sticker_exterior_color": sticker_exterior_color,
        "sticker_interior_color": sticker_interior_color,
        "sticker_interior_material": sticker_interior_material,
        "sticker_spec_lines": sticker_spec_lines,
        "interior_from_listing_description": interior_from_listing_description,
        "interior_from_llava_vision": interior_from_llava_vision,
        "packages_panel_has_content": packages_panel_has_content,
        "llava_interior_section": llava_interior_section,
        "hide_photo_analysis": hide_photo_analysis,
    }
