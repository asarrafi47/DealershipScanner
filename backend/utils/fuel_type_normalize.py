"""
Present 48V mild-hybrid (belt-starter-generator) drivetrains as gasoline.

Dealer feeds label these rows ``Hybrid`` because the OEM build data flags an
electrified drivetrain. That is technically true and practically misleading: a
Ram 1500 eTorque has no plug, no EV-only mode and no hybrid-specific fuel — it
burns the same pump gasoline as the non-eTorque HEMI (89 octane recommended, 87
acceptable). Shoppers who filter for "Hybrid" are looking for Prius-class fuel
economy, so the buyer-facing FUEL TYPE must read as gas. The ENGINE line is left
alone on purpose: "5.7L V8 Mild Hybrid" is an accurate description of the
hardware and is useful to a shopper.

Scope is deliberately narrow. Only the (make, model, year, engine) combinations
listed in :data:`MILD_HYBRID_FAMILIES` are corrected, and only when the row's own
label is a non-plug-in hybrid. Real hybrids (Prius, RAV4 Hybrid), plug-in hybrids
(RAV4 Prime, Wrangler 4xe) and BEVs are never touched — a new family is added by
appending one row to the table, never by adding a branch.

FILTERS READ THE STORED COLUMN, so a display-only correction is not enough: the
facet cascade is normalized through this module, but ``search_cars`` (smart
search, package filters, nearby-dealer counts) builds ``WHERE fuel_type IN (...)``
straight off ``cars.fuel_type``. A read-time-only fix therefore makes the card and
the filter disagree, and a one-off column backfill decays — the scanner re-stores
the fed "Hybrid" on the next pass of that dealer. Measured, not theorised: an
earlier backfill of this rule claimed to have taken active Ram 1500 "Hybrid" to
zero, and 349 such rows were stored "Hybrid" again when it was next checked.

So the correction is applied on BOTH sides:

* WRITE — :func:`normalize_fuel_type_for_storage` runs in
  ``scanner/database.py::upsert_vehicles`` (right after ``clean_car_row_dict``,
  on every scanned row) and in ``db/repositories/cars_repo.py::
  update_car_row_partial`` (the path the post-scan ``gap_fill`` / window-sticker
  enrichers write through). This is a correction of a LISTING field at ingest,
  not a catalog fact being copied into ``cars``.
* READ — :func:`fill_normalized_fuel_type_for_display` and the facet cascade in
  ``listings_repo`` still correct on the way out, so rows written before the
  write-path hook (or by any writer that bypasses both entry points) still show
  the right chip.

See ``backend/tests/test_fuel_type_normalize.py`` for the ingest round-trip test.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from backend.utils.fuel_label_plausibility import (
    corrected_label_for_electric_claim,
    is_bare_electric_label,
)

# Fleet-dominant gasoline label (~48k active rows already display this). The
# linked catalog row says "Midgrade Gasoline" for the Ram 1500 eTorque, but the
# octane grade belongs in the fuel-requirement copy, not in the fuel-type chip —
# showing one truck as "Midgrade Gasoline" next to a wall of "Gasoline" reads as
# a data bug.
GASOLINE_DISPLAY = "Gasoline"

_DISPLAY_EMPTY = ("", "-", "—", "n/a", "na", "none", "null")

# Anything that can be plugged in, or that is battery-only: never correctable.
_PLUG_IN_RE = re.compile(
    r"plug[\s\-]?in|\bphev\b|\b4xe\b|\brecharge\b|\bere?v\b|\bbev\b|\bprime\b",
    re.I,
)
# Labels a mild hybrid can plausibly be wearing: "Hybrid", "MHEV",
# "Gasoline / Electric", "Electric / Gasoline".
_HYBRID_LABEL_RE = re.compile(r"\bhybrid\b|\bmhev\b|\bhev\b", re.I)
_GAS_ELECTRIC_LABEL_RE = re.compile(
    r"gas\w*\s*[/&+]\s*electric|electric\s*[/&+]\s*gas\w*", re.I
)

# EPA ``epa_master.fuel_type`` vocabulary ("Regular Gasoline", "Midgrade
# Gasoline", "Premium Gasoline", "Regular Gasoline / E85", ...).
_CATALOG_GASOLINE_RE = re.compile(r"gasoline|\bgas\b|\be85\b|ethanol", re.I)
_CATALOG_ELECTRIC_RE = re.compile(r"electric", re.I)

# Engine evidence that rules a family match OUT even when the nameplate fits: a
# compression-ignition or battery-only drivetrain is not a 48V BSG gas engine.
_ENGINE_CONTRADICTION_RE = re.compile(
    r"diesel|ecodiesel|\bcummins\b|battery\s*electric|\bbev\b|\bfuel\s*cell\b", re.I
)


@dataclass(frozen=True)
class MildHybridFamily:
    """One 48V BSG drivetrain family that must present as gasoline."""

    system: str  # OEM marketing name, e.g. "eTorque"
    make_re: re.Pattern[str]
    model_re: re.Pattern[str]  # matched against ``model``, then ``title``
    year_min: int
    year_max: int | None  # None = still in production
    engine_re: re.Pattern[str]  # matched against the row's combined engine text
    exclude_re: re.Pattern[str] | None  # nameplate variants that are NOT this system
    recommended_octane: int | None = None
    minimum_octane: int | None = None
    # When False, a row whose engine text is missing or unrecognisable still
    # matches on nameplate + year + label. Only set that where the nameplate has
    # no electrified variant other than this system, because then the label gate
    # has already done the identification work — see the Ram entry below.
    engine_required: bool = True


MILD_HYBRID_FAMILIES: tuple[MildHybridFamily, ...] = (
    # Ram 1500 eTorque (DT, 2019+, and the DS "Classic" that ran alongside it).
    # Both the 3.6L Pentastar and the 5.7L HEMI were offered with the 48V BSG;
    # every electrified-labelled Ram 1500 in inventory is one of the two. The
    # Ram 1500 REV (BEV) and 1500 Ramcharger (EREV) are separate nameplates and
    # are excluded by name as well as by the plug-in label gate.
    MildHybridFamily(
        system="eTorque",
        make_re=re.compile(r"^\s*ram\b", re.I),
        model_re=re.compile(r"^\s*(?:ram\s+)?1500\b", re.I),
        year_min=2019,
        year_max=None,
        engine_re=re.compile(
            r"e[\s\-]?torque|mild\s*hybrid|\bbsg\b"
            r"|\bhemi\b|\bpentastar\b"
            r"|\b5\.7\s*l?\b|\b3\.6\s*l?\b",
            re.I,
        ),
        exclude_re=re.compile(r"ramcharger|\brev\b|\b4xe\b", re.I),
        recommended_octane=89,
        minimum_octane=87,
        # Feeds send junk engine text ("6 Cyl - 6 L" on a 2022 Big Horn whose VIN
        # engine code is the 3.6L eTorque), and the facet cascade projects no
        # engine column at all. Neither should strand a truck on the "Hybrid"
        # facet the grid cannot fill. Nothing is lost by relaxing it here: a
        # 2019+ Ram 1500 wearing a non-plug-in hybrid label is an eTorque, the
        # plug-in nameplates (REV, Ramcharger) are excluded by name and by label,
        # and a contradicting engine (diesel) still blocks the match.
        engine_required=False,
    ),
)


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _is_blank_display(value: Any) -> bool:
    return _clean(value).lower() in _DISPLAY_EMPTY


def is_correctable_hybrid_label(fuel_type: Any) -> bool:
    """
    True when *fuel_type* is a non-plug-in hybrid label that a mild hybrid could
    wrongly be wearing.

    This is the safety gate that keeps real plug-in hybrids and BEVs out: any
    plug-in cue, or a bare "Electric", returns False.
    """
    s = _clean(fuel_type)
    if not s or _is_blank_display(s):
        return False
    if _PLUG_IN_RE.search(s):
        return False
    return bool(_HYBRID_LABEL_RE.search(s) or _GAS_ELECTRIC_LABEL_RE.search(s))


def _engine_text_for_car(car: dict[str, Any], engine_text: str | None = None) -> str:
    """Every field that can carry the engine identity, joined for one regex pass."""
    parts = [
        engine_text,
        car.get("engine_description"),
        car.get("engine_display"),
        car.get("engine"),
        car.get("engine_l"),
        car.get("master_engine_string"),
    ]
    return " ".join(_clean(p) for p in parts if not _is_blank_display(p))


def _nameplate_text_for_car(car: dict[str, Any]) -> str:
    return " ".join(
        _clean(car.get(k)) for k in ("make", "model", "trim", "title", "series")
    )


def match_mild_hybrid_family(
    car: dict[str, Any],
    *,
    engine_text: str | None = None,
) -> MildHybridFamily | None:
    """Return the :class:`MildHybridFamily` this row belongs to, or None."""
    make = _clean(car.get("make"))
    model = _clean(car.get("model"))
    title = _clean(car.get("title"))
    if not make:
        return None

    try:
        year = int(str(car.get("year")).strip())
    except (TypeError, ValueError):
        return None

    nameplate = _nameplate_text_for_car(car)
    engine_blob = _engine_text_for_car(car, engine_text)
    if _ENGINE_CONTRADICTION_RE.search(engine_blob):
        return None

    for fam in MILD_HYBRID_FAMILIES:
        if not fam.make_re.search(make):
            continue
        if not (fam.model_re.search(model) or fam.model_re.search(title)):
            continue
        if year < fam.year_min or (fam.year_max is not None and year > fam.year_max):
            continue
        if fam.exclude_re is not None and fam.exclude_re.search(nameplate):
            continue
        if not fam.engine_re.search(engine_blob) and fam.engine_required:
            continue
        return fam
    return None


def catalog_fuel_type_for_car(car: dict[str, Any]) -> str | None:
    """
    ``epa_master.fuel_type`` for a row with a resolved ``cars.epa_master_id``.

    Only consulted after a family match, so the (lru-cached) catalog read costs
    nothing on the 99% of rows that are not mild hybrids.
    """
    mid = car.get("epa_master_id")
    if mid in (None, "", 0):
        return None
    try:
        from backend.enrichment.knowledge_engine import lookup_epa_master_by_id

        row = lookup_epa_master_by_id(mid)
    except Exception:
        return None
    ft = _clean((row or {}).get("fuel_type"))
    return ft or None


_LOOKUP_CATALOG = object()


def normalize_fuel_type_for_display(
    car: dict[str, Any],
    *,
    fuel_type: Any = None,
    engine_text: str | None = None,
    catalog_fuel_type: Any = _LOOKUP_CATALOG,
) -> str | None:
    """
    Corrected buyer-facing fuel type, or None to leave the current value alone.

    *fuel_type* defaults to ``car["fuel_type"]``; pass the already-derived display
    value when the caller has one. *catalog_fuel_type* defaults to a lookup of the
    linked ``epa_master`` row; pass it explicitly (including ``None``) to skip the
    DB read.
    """
    ft = fuel_type if fuel_type is not None else car.get("fuel_type")

    # Bare "Electric" on a row whose own evidence says combustion (gas GX 550,
    # ES 350h hybrid, 330e plug-in stored as "Electric"): route the claim to the
    # evidence-backed label. Same call sites as the mild-hybrid rule — card
    # serializer, facet cascade and the storage normalizer — so the chip and the
    # fuel filter never disagree about which bucket the car belongs to.
    if is_bare_electric_label(ft):
        fixed = corrected_label_for_electric_claim(
            car, fuel_type=ft, engine_text=_engine_text_for_car(car, engine_text)
        )
        if not fixed:
            return None
        catalog = (
            catalog_fuel_type_for_car(car)
            if catalog_fuel_type is _LOOKUP_CATALOG
            else catalog_fuel_type
        )
        # A linked catalog row that says electric-ONLY outranks the heuristics:
        # abstain rather than relabel something the cited source calls a BEV.
        # Dual-fuel catalog strings ("Premium Gasoline / Electricity" = PHEV)
        # do not protect a bare-electric label.
        if (
            catalog
            and _CATALOG_ELECTRIC_RE.search(catalog)
            and not _CATALOG_GASOLINE_RE.search(catalog)
        ):
            return None
        return fixed

    if not is_correctable_hybrid_label(ft):
        return None

    fam = match_mild_hybrid_family(car, engine_text=engine_text)
    if fam is None:
        return None

    catalog = (
        catalog_fuel_type_for_car(car)
        if catalog_fuel_type is _LOOKUP_CATALOG
        else catalog_fuel_type
    )
    if catalog:
        # The catalog is the cited source and it outranks the family table. If it
        # says the linked trim burns electricity, the row is not the mild hybrid
        # we think it is (bad resolver link, or a nameplate variant the exclude
        # pattern missed) — abstain rather than relabel a real electrified car.
        if _CATALOG_ELECTRIC_RE.search(catalog):
            return None
        if _CATALOG_GASOLINE_RE.search(catalog):
            return GASOLINE_DISPLAY

    return GASOLINE_DISPLAY


def fill_normalized_fuel_type_for_display(
    car: dict[str, Any], out: dict[str, Any]
) -> None:
    """
    Mutate *out* ``fuel_type`` in place when the row is a mild hybrid.

    *out* is the partially built serialized dict, so ``engine_display`` (which
    may say "Mild Hybrid" where the raw column does not) is used as engine
    evidence and the already-derived ``fuel_type`` is used as the current label.
    """
    current = out.get("fuel_type")
    if _is_blank_display(current):
        current = car.get("fuel_type")

    fixed = normalize_fuel_type_for_display(
        car,
        fuel_type=current,
        engine_text=_clean(out.get("engine_display")),
    )
    if fixed:
        out["fuel_type"] = fixed


def normalize_fuel_type_for_storage(car: dict[str, Any]) -> str | None:
    """
    Corrected ``cars.fuel_type`` to PERSIST for a mild-hybrid row, or None to keep
    the fed value.

    Write-path twin of :func:`normalize_fuel_type_for_display`, shaped like
    ``car_serialize.normalize_condition_for_storage``. Callers:
    ``scanner/database.py::upsert_vehicles`` (every scanned row),
    ``scanner/database.py::apply_model_specs_corrections`` (the empty-fuel filler)
    and ``db/repositories/cars_repo.py::update_car_row_partial`` (post-scan
    enrichers). It exists because the display correction alone cannot fix the fuel
    FILTERS — see the module docstring — and the only durable fix is to store the
    corrected label.

    Never reads the catalog: this runs per row inside the upsert loop, where no
    ``epa_master_id`` is resolved yet and a per-row DB hit would be a scan-time
    regression (the same reason ``car_serialize/condition.py`` splits storage from
    display). The family table plus the row's own nameplate / year / label is the
    whole rule, so it reaches the same answer the display path does on a freshly
    parsed row.
    """
    return normalize_fuel_type_for_display(car, catalog_fuel_type=None)
