"""Resolve original EPA all-electric range (miles) for EV / PHEV listings."""

from __future__ import annotations

import csv
import io
import json
import logging
import re
import urllib.request
from functools import lru_cache
from pathlib import Path
from typing import Any

from backend.enrichment.knowledge_engine import lookup_epa_by_trim

logger = logging.getLogger(__name__)

_RANGE_TOKEN_STOP = frozenset(
    {
        "awd",
        "rwd",
        "fwd",
        "with",
        "the",
        "and",
        "for",
        "suv",
        "sedan",
        "hatchback",
        "coupe",
    }
)

_EPA_VEHICLES_CSV_URL = "https://fueleconomy.gov/feg/epadata/vehicles.csv"
_EPA_RANGE_CACHE_PATH = (
    Path(__file__).resolve().parents[1] / "dictionary" / "derived" / "epa_ev_range_miles.json"
)
_ELECTRIFIED_FUEL_TYPES = frozenset({"electric", "ev", "electricity", "plug-in hybrid", "plug in hybrid", "phev"})

# ---------------------------------------------------------------------------
# Three removed sources, and why none of them is coming back
#
# 1. Overlay bullet text. ``_range_miles_from_trim_adds`` read
#    ``backend/dictionary/derived/trim_adds_by_year/*.json`` directly and regex'd
#    a mileage out of ``adds_by_trim``, bypassing ``load_brochure_trim_overlay``
#    and the provenance gate behind it. Six overlay files yield a range and none
#    of them has an admissible source (3 ``brochure_llm``, 3
#    ``brochure_llm_web``). Measured 2026-07-31: 6 active cars were showing a
#    range that came from a bullet nothing can account for, and where EPA has a
#    row for the same car the two disagree badly — the 2021 Mustang Mach-E file
#    says 270 mi against EPA's 211, the 2021 Taycan file says 274 against 199.
#
# 2. The listing's own free text (description / title / trim / model / packages).
#    That is dealer marketing copy, which the project rules exclude as a source
#    for a rendered claim, and a regex over it cannot tell "330 miles of range"
#    about THIS car from the same sentence about the trim above it. Measured over
#    all 72,504 active listings on 2026-07-31: this path resolved a range for
#    exactly 0 cars, so removing it costs nothing and closes the hole before a
#    feed starts writing that sentence.
#
# 3. ``epa_extended_specs.ev_range_miles`` (removed 2026-07-31). It survived the
#    first pass because it is a scrape rather than prose, and because it sat
#    last in the order — behind EPA's own file. Both defences fail on measurement:
#
#    * It is the wrong FIELD, not merely the wrong trim. The column is filled
#      from nameplate review pages, and what those pages print is a TOTAL
#      driving range. Serializing all 72,504 active listings before and after
#      the removal, 67 cars lose a figure and no car's figure changes; every one
#      of the 67 is a plug-in hybrid, a hydrogen Mirai or a single Audi Q4
#      e-tron, and the numbers they lose are 400 miles of "all-electric range"
#      for a Jeep Wrangler 4xe, 550 for a BMW 530e, 600 for a Mercedes-AMG C 63
#      S E Performance and 402 for a 2018 Toyota Mirai — a hydrogen car with no
#      plug. None of those is a battery range; each is roughly what the car does
#      on a full tank. (Resolving the row the way this module now would, rather
#      than the way the old lookup did, the same source answers for 171 of the
#      4,748 active electrified listings — so 67 is the floor, not the ceiling.)
#    * The page it quotes is about a different model year. 14,503 of the 14,884
#      rows carrying a range do have the number in a page extraction, so they
#      pass a quote test; only 965 of those pages carry the row's own year in
#      their title. A 2023 EQS 580 quotes ``caranddriver.com/mercedes-benz/eqs``
#      for 400 miles where EPA says 285.
#
#    The disagreement is not rare, either: on 2,681 of the 3,500 active
#    electrified listings that DO have an EPA row, this column disagrees with
#    EPA by more than 5 miles. It was only invisible because EPA won.
#
# What is left is EPA's own published range (fueleconomy.gov vehicles.csv), and
# nothing else. Plug-in hybrids are not in that file — the cache holds 1,451
# rows, all ``atvType=EV`` — so a PHEV now shows no factory range at all, which
# is the correct answer to "we do not know it" for the number a battery-health
# readout is measured against.
# ---------------------------------------------------------------------------

#: Band an EPA all-electric range has to fall in to be believable. Deliberately
#: wide — a sanity floor/ceiling, not a spec — and out-of-band values are
#: dropped, never clamped to the edge. These are the same bounds this module has
#: always applied; they were inline in ``_positive_range_miles``.
_MIN_PLAUSIBLE_RANGE_MI = 40
_MAX_PLAUSIBLE_RANGE_MI = 600


def _normalize_token(raw: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", str(raw or "").strip().lower())


def _positive_int(value: Any) -> int | None:
    try:
        n = int(round(float(value)))
    except (TypeError, ValueError):
        return None
    return n if n > 0 else None


def _positive_range_miles(value: Any) -> int | None:
    miles = _positive_int(value)
    if miles is None:
        return None
    return miles if _MIN_PLAUSIBLE_RANGE_MI <= miles <= _MAX_PLAUSIBLE_RANGE_MI else None


def _parse_year(value: Any) -> int | None:
    year = _positive_int(value)
    return year if year is not None and 1900 <= year <= 2100 else None


def _is_electrified_car(car: dict[str, Any] | None) -> bool:
    if not car:
        return False
    fuel = str(car.get("fuel_type") or "").strip().lower()
    if fuel in _ELECTRIFIED_FUEL_TYPES:
        return True
    if "electric" in fuel or "plug" in fuel:
        return True
    return False


def _listing_range_tokens(*parts: Any) -> set[str]:
    tokens: set[str] = set()
    for part in parts:
        for raw in re.split(r"[\s/+-]+", str(part or "").lower()):
            tok = re.sub(r"[^a-z0-9]", "", raw)
            if not tok:
                continue
            if tok.isdigit():
                tokens.add(tok)
                continue
            if len(tok) < 3 or tok in _RANGE_TOKEN_STOP:
                continue
            tokens.add(tok)
    return tokens


def _score_epa_model_match(
    *,
    listing_model: str,
    listing_trim: str,
    epa_model: str,
    epa_trim: str | None = None,
) -> int:
    model_n = _normalize_token(listing_model)
    trim_n = _normalize_token(listing_trim)
    epa_model_n = _normalize_token(epa_model)
    epa_trim_n = _normalize_token(epa_trim or "")
    blob = f"{listing_model} {listing_trim}".strip().lower()
    epa_blob = f"{epa_model} {epa_trim or ''}".strip().lower()
    score = 0
    if model_n and model_n in epa_model_n:
        score += 20
    if trim_n and trim_n in epa_model_n:
        score += 30
    if trim_n and epa_trim_n and (trim_n in epa_trim_n or epa_trim_n in trim_n):
        score += 25
    tokens = [t for t in re.split(r"[\s/+-]+", listing_trim.lower()) if len(t) > 2]
    if tokens and all(tok in epa_blob for tok in tokens):
        score += 15
    if blob and blob in epa_blob:
        score += 40

    listing_tokens = _listing_range_tokens(listing_model, listing_trim)
    epa_tokens = _listing_range_tokens(epa_model, epa_trim or "")
    overlap = listing_tokens & epa_tokens
    for tok in overlap:
        if tok.isdigit():
            score += 18
        else:
            score += 10
    if len(overlap) >= 3:
        score += 20
    elif len(overlap) >= 2:
        score += 10
    return score


def _name_tokens(raw: Any) -> frozenset[str]:
    """Alphanumeric runs of a model name, lowercased (``Mach-E`` -> mach, e)."""
    return frozenset(t for t in re.split(r"[^a-z0-9]+", str(raw or "").lower()) if t)


def _head_token(raw: Any) -> str:
    """The leading alphanumeric run — the nameplate proper (``Q4``, ``Model``)."""
    for tok in re.split(r"[^a-z0-9]+", str(raw or "").lower()):
        if tok:
            return tok
    return ""


def _same_nameplate(listing_model: Any, listing_trim: Any, epa_model: Any) -> bool:
    """
    True when an EPA row is about the same nameplate as the listing.

    ``_score_epa_model_match`` alone is not enough to decide this, and the gap is
    not theoretical — every one of these was rendering on the car page before
    this gate existed (measured 2026-07-31, live data):

    * a 2021 Tesla Model S "Long Range AWD" scored ``Model 3 Long Range AWD``
      (95) and ``Model Y Long Range AWD`` (95) ABOVE ``Model S Long Range`` (70),
      because the trim string matches the other cars word for word, and showed
      the Model 3's 353 mi;
    * a 2026 Kia EV6 GT-Line took ``EV9 Long Range AWD GT-Line`` (55) over every
      real EV6 row (30) and showed the EV9's 280 mi, a number no 2026 EV6 prints;
    * a 2023 Audi Q5 55 TFSI e — a plug-in hybrid with roughly 23 miles of
      electric range — matched ``e-tron quattro`` and showed 226 mi, the range of
      a different, battery-electric SUV;
    * a 2023 Mercedes-Benz EQE matched ``EQS`` rows.

    The trim half of the score is doing the damage: "Long Range AWD" and
    "GT-Line" are shared across a maker's whole EV lineup, so a trim that matches
    perfectly can outweigh a model that does not match at all. Rather than
    re-tune the weights — which only moves the line — a candidate must first be
    the same car, and the score then only chooses between that nameplate's rows.

    Compared as token sets rather than as substrings, because EPA reorders the
    words: ``Q4 e-tron Sportback`` is filed as ``Q4 Sportback 55 e-tron
    quattro``. Two conditions, both required:

    * each side's LEADING token appears in the other side's tokens. The leading
      token is the nameplate proper (``Q4``, ``EV6``, ``Model``), and checking it
      in both directions is what separates an Audi ``e-tron`` from a ``Q4
      e-tron``: neither name contains the other's head;
    * and one side's tokens are a subset of the other's, since the two sources
      split model from trim differently — the listing may carry the longer name
      (``C40 Recharge Pure Electric`` vs EPA's ``C40 Recharge``) or the shorter
      one (``EQE`` vs EPA's ``EQE 350 4matic``). The listing's trim counts toward
      its tokens, which is what lets EPA's fuller name be recognised.

    This is a nameplate test, not a trim test. It is happy to admit every trim of
    the right car and leaves choosing between them to the score.
    """
    listing_tokens = _name_tokens(listing_model)
    epa_tokens = _name_tokens(epa_model)
    if not listing_tokens or not epa_tokens:
        return False
    with_trim = listing_tokens | _name_tokens(listing_trim)
    if _head_token(listing_model) not in epa_tokens:
        return False
    if _head_token(epa_model) not in with_trim:
        return False
    return listing_tokens <= epa_tokens or epa_tokens <= with_trim


def _fetch_epa_vehicle_rows() -> list[dict[str, Any]]:
    request = urllib.request.Request(
        _EPA_VEHICLES_CSV_URL,
        headers={"User-Agent": "DealershipScanner/1.0"},
    )
    with urllib.request.urlopen(request, timeout=120) as response:
        payload = response.read().decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(payload))
    rows: list[dict[str, Any]] = []
    for row in reader:
        atv = str(row.get("atvType") or "").strip().upper()
        fuel = str(row.get("fuelType1") or "").strip().lower()
        if atv not in {"EV", "PHEV"} and "electric" not in fuel and "plug" not in fuel:
            continue
        year = _parse_year(row.get("year"))
        make = str(row.get("make") or "").strip()
        model = str(row.get("model") or "").strip()
        if year is None or not make or not model:
            continue
        range_miles = _positive_range_miles(row.get("range"))
        if range_miles is None:
            range_miles = _positive_range_miles(row.get("rangeA"))
        if range_miles is None:
            continue
        rows.append(
            {
                "year": year,
                "make": make,
                "make_n": _normalize_token(make),
                "model": model,
                "range_miles": range_miles,
                "atv_type": atv or None,
            }
        )
    return rows


def build_epa_ev_range_cache() -> dict[str, Any]:
    rows = _fetch_epa_vehicle_rows()
    return {
        "source": "fueleconomy.gov/vehicles.csv",
        "rows": rows,
    }


def write_epa_ev_range_cache(payload: dict[str, Any] | None = None) -> Path:
    data = payload or build_epa_ev_range_cache()
    path = _EPA_RANGE_CACHE_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)
    return path


@lru_cache(maxsize=1)
def _load_epa_ev_range_rows() -> tuple[dict[str, Any], ...]:
    path = _EPA_RANGE_CACHE_PATH
    if path.is_file():
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            rows = payload.get("rows")
            if isinstance(rows, list) and rows:
                return tuple(rows)
        except (OSError, json.JSONDecodeError) as exc:
            logger.warning("epa_ev_range_miles.json unreadable: %s", exc)
    try:
        write_epa_ev_range_cache()
        payload = json.loads(_EPA_RANGE_CACHE_PATH.read_text(encoding="utf-8"))
        rows = payload.get("rows") or []
        return tuple(rows if isinstance(rows, list) else [])
    except Exception as exc:
        logger.warning("Unable to build EPA EV range cache: %s", exc)
        return ()


def lookup_epa_range_miles(
    year: Any,
    make: Any,
    model: Any,
    trim: Any = None,
) -> int | None:
    """
    EPA's published all-electric range for this car, or ``None``.

    A candidate row must be the same make, the same nameplate
    (:func:`_same_nameplate`) and the same model year — or one either side of it,
    which is how the file names a car sold across a model-year boundary. Within
    that set, :func:`_score_epa_model_match` picks the closest trim and the
    result is only accepted at a score of 15 or better.

    ``None`` when nothing clears those bars, which the caller must render as
    nothing. It is emphatically NOT the nearest available number: the make-wide
    candidate list this used to score over is what handed a Q5 plug-in hybrid an
    e-tron's range.
    """
    year_val = _parse_year(year)
    make_raw = str(make or "").strip()
    model_raw = str(model or "").strip()
    trim_raw = str(trim or "").strip()
    if year_val is None or not make_raw or not model_raw:
        return None

    make_n = _normalize_token(make_raw)
    rows = _load_epa_ev_range_rows()

    for year_try in (year_val, year_val - 1, year_val + 1):
        # Same make, same year, AND the same nameplate. The nameplate test is a
        # hard filter rather than another term in the score: see
        # :func:`_same_nameplate` for the four families this was getting wrong.
        candidates = [
            row
            for row in rows
            if row.get("year") == year_try
            and row.get("make_n") == make_n
            and _same_nameplate(model_raw, trim_raw, row.get("model"))
        ]
        if not candidates:
            continue

        epa_trim = lookup_epa_by_trim(year_try, make_raw, model_raw, trim_raw or None)
        epa_trim_name = str(epa_trim.get("trim") or "").strip() or None

        best_range: int | None = None
        best_score = 0
        for row in candidates:
            score = _score_epa_model_match(
                listing_model=model_raw,
                listing_trim=trim_raw,
                epa_model=str(row.get("model") or ""),
                epa_trim=epa_trim_name,
            )
            if score > best_score:
                best_score = score
                best_range = _positive_range_miles(row.get("range_miles"))
        if best_range is not None and best_score >= 15:
            return best_range
    return None


def resolve_factory_epa_range(car: dict[str, Any] | None) -> int | None:
    """
    Original EPA all-electric range in miles for EV / PHEV listings, or ``None``.

    ``None`` means "no sourced range for this car" and the caller must render
    nothing. This figure anchors the battery-degradation readout a shopper reads
    as the car's real-world range, so a number nobody can trace is worse than a
    blank.

    Resolution order — one source, plus whatever the caller already resolved:

    1. the row's own ``factory_range`` / ``epa_range`` / ``epa_range_miles``
       value, if the caller supplied one;
    2. EPA's published range from ``fueleconomy.gov`` ``vehicles.csv``, cached in
       ``backend/dictionary/derived/epa_ev_range_miles.json``.

    There is no third step any more; ``epa_extended_specs.ev_range_miles`` was
    removed on 2026-07-31 and the block comment above records what it was
    handing out and to how many cars.

    Gated on the car being electrified in the first place, and on a 40-600 mile
    plausibility band, because the EPA file is matched on year/make/model with a
    scored model-name comparison and will otherwise hand a gas Kona the Kona
    Electric's row.
    """
    if not car or not _is_electrified_car(car):
        return None

    for key in ("factory_range", "epa_range", "epa_range_miles"):
        explicit = _positive_range_miles(car.get(key))
        if explicit is not None:
            return explicit

    # Re-banded here as well as inside the lookup: the band is part of THIS
    # function's contract, and it must hold for whatever source sits behind the
    # call rather than depending on the current one policing itself.
    return _positive_range_miles(
        lookup_epa_range_miles(
            car.get("year"),
            car.get("make"),
            car.get("model"),
            car.get("trim"),
        )
    )
