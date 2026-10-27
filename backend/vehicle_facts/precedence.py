"""
Write-time fact precedence: who may overwrite whom, per (field, source).

Pure: no database, no network. P8A.3 (the apply helper) and the writers call
:func:`decide` / :func:`plan_updates` and persist what they return.

Owner rules (CLAUDE.md, D-PR1/4/6/7):

* Ranking is per (field, source), never one global rank. NHTSA vPIC wins only on
  driven wheels, electrification / diesel and cylinders. vPIC-synthesized
  engine text, transmission and body are fill-only and lose to dealer text.
* The EPA catalog never outranks the dealer's own engine text or the VIN decode.
* vPIC "4x2" is a constraint {FWD, RWD, 2WD}, never RWD, never AWD (D-PR1).
* A less specific feed value (2WD, 4x2, Other, blank) never replaces a stored
  specific end. The catalog tiebreak follows D-PR4 and a 4MATIC / xDrive trim
  veto. Several signals contradicting vPIC: vPIC wins and the row is flagged
  ``cannot_assess`` (D-PR6). vPIC cylinders count only when they do not
  contradict the engine text (D-PR7).
* Stale provenance: a stored entry's rank counts only while its ``value`` still
  normalizes to the column value; otherwise the source is ``unknown``.

Provenance entries live in ``spec_source_json[<field>]`` as
``{source, value, fetched_at[, discrepancy, cannot_assess]}``.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping

from backend.utils.engine_consistency import cylinders_conflicts_with_engine_text
from backend.vehicle_facts.drivetrain import (
    drivetrain_storage_value,
    is_two_wheel_unknown,
    normalize_drivetrain,
    same_drive_wheels,
)
from backend.vehicle_facts.electrification import (
    BEV,
    FCEV,
    HEV,
    PHEV,
    PLUG_IN,
    electrification,
    fuel_label_electrification,
)

# --- sources ---------------------------------------------------------------
NHTSA_VPIC = "nhtsa_vpic"
STICKER = "oem_window_sticker"
STICKER_UNVERIFIED = "oem_window_sticker_unverified"
DEALER_FEED = "dealer_feed"
DEALER_VDP = "dealer_vdp"
DEALER_NORMALIZED = "dealer_normalized"
DEALER_TEXT = "dealer_text_heuristic"
HTML_JSONLD = "html_jsonld"
RECIPE_HEAL = "recipe_heal"
EPA_CATALOG = "epa_catalog"
EPA_TIEBREAK = "epa_catalog_tiebreak"
WEB_SEARCH = "web_search"
MODEL_SPECS = "model_specs"
TRIM_HEURISTIC = "trim_heuristic"
LLM = "llm"
UNKNOWN = "unknown"

SOURCES: tuple[str, ...] = (
    NHTSA_VPIC, STICKER, STICKER_UNVERIFIED, DEALER_FEED, DEALER_VDP, DEALER_NORMALIZED,
    DEALER_TEXT, HTML_JSONLD, RECIPE_HEAL, EPA_CATALOG, EPA_TIEBREAK, WEB_SEARCH,
    MODEL_SPECS, TRIM_HEURISTIC, LLM, UNKNOWN,
)

# Labels found in stored provenance. Mixed-source legacy tags are unknown.
_ALIASES: dict[str, str] = {
    "inventory_repair": UNKNOWN,
    "listing_gap_fill": UNKNOWN,
    "nhtsa_vpic_heal": NHTSA_VPIC,
    "ev_cylinders_heal": NHTSA_VPIC,
    "epa_master": EPA_CATALOG,
    "epa_dictionary": EPA_CATALOG,
}

# --- rank tables -----------------------------------------------------------
# Higher wins. A source absent from a field's table never writes that field.
# Sources in FILL_ONLY[field] write only into a blank / junk column.
_R = {
    # drivetrain / fuel / cylinders: vPIC decides (with the guards in decide()).
    "drivetrain": {NHTSA_VPIC: 100, STICKER: 80, DEALER_FEED: 60, DEALER_VDP: 60,
                   DEALER_NORMALIZED: 55, HTML_JSONLD: 55, RECIPE_HEAL: 45,
                   DEALER_TEXT: 40, EPA_TIEBREAK: 35, EPA_CATALOG: 30,
                   TRIM_HEURISTIC: 20, MODEL_SPECS: 10, WEB_SEARCH: 5, UNKNOWN: 1},
    "fuel_type": {NHTSA_VPIC: 100, STICKER: 80, DEALER_FEED: 60, DEALER_VDP: 60,
                  DEALER_NORMALIZED: 55, HTML_JSONLD: 55, RECIPE_HEAL: 45,
                  DEALER_TEXT: 40, EPA_CATALOG: 30, MODEL_SPECS: 10, WEB_SEARCH: 5, UNKNOWN: 1},
    "cylinders": {NHTSA_VPIC: 100, STICKER: 80, DEALER_FEED: 60, DEALER_VDP: 60,
                  DEALER_NORMALIZED: 55, HTML_JSONLD: 55, RECIPE_HEAL: 45,
                  DEALER_TEXT: 40, EPA_CATALOG: 30, MODEL_SPECS: 10, WEB_SEARCH: 5, UNKNOWN: 1},
    # Text the dealer or the sticker wrote outranks everything synthesized.
    "engine_description": {STICKER: 90, DEALER_FEED: 80, DEALER_VDP: 80, DEALER_NORMALIZED: 70,
                           HTML_JSONLD: 70, RECIPE_HEAL: 60, DEALER_TEXT: 50, NHTSA_VPIC: 20,
                           EPA_CATALOG: 15, MODEL_SPECS: 10, WEB_SEARCH: 5, UNKNOWN: 1},
    "engine_l": {STICKER: 90, DEALER_FEED: 80, DEALER_VDP: 80, DEALER_NORMALIZED: 70,
                 HTML_JSONLD: 70, RECIPE_HEAL: 60, DEALER_TEXT: 50, NHTSA_VPIC: 20,
                 EPA_CATALOG: 15, MODEL_SPECS: 10, WEB_SEARCH: 5, UNKNOWN: 1},
    # sticker > dealer > vPIC fill > catalog fill
    "transmission": {STICKER: 90, DEALER_FEED: 80, DEALER_VDP: 80, DEALER_NORMALIZED: 70,
                     HTML_JSONLD: 70, RECIPE_HEAL: 60, DEALER_TEXT: 50, NHTSA_VPIC: 20,
                     EPA_CATALOG: 15, MODEL_SPECS: 10, WEB_SEARCH: 5, UNKNOWN: 1},
    "body_type": {STICKER: 90, DEALER_FEED: 80, DEALER_VDP: 80, DEALER_NORMALIZED: 70,
                  HTML_JSONLD: 70, RECIPE_HEAL: 60, NHTSA_VPIC: 20, EPA_CATALOG: 15,
                  MODEL_SPECS: 10, UNKNOWN: 1},
    # sticker > dealer; vPIC, catalog and model_specs never write colors.
    "exterior_color": {STICKER: 90, DEALER_FEED: 80, DEALER_VDP: 80, DEALER_NORMALIZED: 70,
                       HTML_JSONLD: 70, RECIPE_HEAL: 60, UNKNOWN: 1},
    "interior_color": {STICKER: 90, DEALER_FEED: 80, DEALER_VDP: 80, DEALER_NORMALIZED: 70,
                       HTML_JSONLD: 70, RECIPE_HEAL: 60, UNKNOWN: 1},
    # Placeholder until forced_induction.resolve_car_forced_induction (P10C.2).
    "forced_induction": {STICKER: 90, DEALER_FEED: 80, DEALER_VDP: 80, DEALER_TEXT: 50,
                         NHTSA_VPIC: 40, EPA_CATALOG: 30, TRIM_HEURISTIC: 10, UNKNOWN: 1},
}
for _t in _R.values():
    _t[STICKER_UNVERIFIED] = 5  # fill-only: ranks last, never overwrites
RANKS: dict[str, dict[str, int]] = _R
FIELDS: tuple[str, ...] = tuple(_R)

_FILL_ONLY_COMMON = frozenset({STICKER_UNVERIFIED})
FILL_ONLY: dict[str, frozenset[str]] = {
    "drivetrain": _FILL_ONLY_COMMON,
    "fuel_type": _FILL_ONLY_COMMON,
    "cylinders": _FILL_ONLY_COMMON,
    "engine_description": _FILL_ONLY_COMMON | {NHTSA_VPIC, EPA_CATALOG, MODEL_SPECS, WEB_SEARCH},
    "engine_l": _FILL_ONLY_COMMON | {NHTSA_VPIC, EPA_CATALOG, MODEL_SPECS, WEB_SEARCH},
    "transmission": _FILL_ONLY_COMMON | {NHTSA_VPIC, EPA_CATALOG, MODEL_SPECS, WEB_SEARCH},
    "body_type": frozenset(SOURCES),  # body is fill-only for every source
    "exterior_color": _FILL_ONLY_COMMON,
    "interior_color": _FILL_ONLY_COMMON,
    "forced_induction": frozenset(SOURCES),  # fill-only until P10C.2
}

_JUNK = frozenset({"", "other", "n/a", "na", "none", "null", "unknown", "-", "--", "tbd", "0"})


def canonical_source(label: Any) -> str:
    s = str(label or "").strip().lower()
    s = _ALIASES.get(s, s)
    return s if s in SOURCES else UNKNOWN


def is_blank(field: str, value: Any) -> bool:
    if value is None:
        return True
    if field == "cylinders":
        try:
            return int(value) <= 0
        except (TypeError, ValueError):
            return True
    return str(value).strip().lower() in _JUNK


def _norm(field: str, value: Any) -> Any:
    """Comparable form of *value*; AWD and 4WD compare equal for drivetrain."""
    if value is None:
        return None
    if field == "drivetrain":
        n = normalize_drivetrain(value, "dealer")
        if n in ("AWD", "4WD"):
            return "AWD"
        return n or str(value).strip().upper()
    if field == "cylinders":
        try:
            return int(value)
        except (TypeError, ValueError):
            return None
    return " ".join(str(value).split()).casefold()


def _same(field: str, a: Any, b: Any) -> bool:
    return _norm(field, a) == _norm(field, b)


def _prov_entries(prov: Any) -> dict[str, Any]:
    if isinstance(prov, str):
        try:
            prov = json.loads(prov)
        except (json.JSONDecodeError, TypeError, ValueError):
            return {}
    return prov if isinstance(prov, dict) else {}


def field_source(field: str, column_value: Any, provenance: Any) -> str:
    """Source whose rank counts for the stored *column_value*.

    ``unknown`` when there is no entry, the entry carries no ``value``, or the
    value no longer matches the column (stale provenance rule).
    """
    if is_blank(field, column_value):
        return UNKNOWN
    entry = _prov_entries(provenance).get(field)
    if isinstance(entry, str):
        return UNKNOWN  # a bare label carries no value to check
    if not isinstance(entry, dict) or "value" not in entry:
        return UNKNOWN
    if not _same(field, entry.get("value"), column_value):
        return UNKNOWN
    return canonical_source(entry.get("source"))


def rank_of(field: str, source: str) -> int | None:
    """Rank of *source* for *field*, None when it may never write it."""
    return RANKS.get(field, {}).get(canonical_source(source))


@dataclass(frozen=True)
class Decision:
    write: bool
    field: str
    value: Any = None
    source: str = UNKNOWN
    reason: str = ""
    discrepancy: dict | None = None   # {source, value, seen_at}: the losing signal
    cannot_assess: bool = False


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _no(field: str, source: str, reason: str, stored: Any = None, cand: Any = None,
        *, record: bool = True, cannot_assess: bool = False) -> Decision:
    disc = None
    if record and cand is not None and not is_blank(field, cand) and not _same(field, stored, cand):
        disc = {"source": source, "value": cand, "seen_at": _now()}
    return Decision(False, field, None, source, reason, disc, cannot_assess)


def _yes(field: str, value: Any, source: str, reason: str, cannot_assess: bool = False) -> Decision:
    return Decision(True, field, value, source, reason, None, cannot_assess)


def decide(field: str, stored: Any, provenance: Any, candidate: Any, source: str,
           ctx: Mapping[str, Any] | None = None) -> Decision:
    """May *candidate* from *source* replace the stored column value?

    *stored* is the column value, *provenance* the row's ``spec_source_json``
    (dict or JSON text). *ctx* carries evidence from the rest of the row and
    the decode: ``vpic_drive`` (raw vPIC drive text), ``vpic`` (a vPIC dict for
    electrification), ``engine_description``, ``fuel_type``, ``trim_drive``
    (canonical drive read from the trim text), ``corroborated`` (catalog
    engine correction, D-PR3).
    """
    ctx = ctx or {}
    src = canonical_source(source)
    if field not in RANKS:
        return _no(field, src, "unknown field")
    if src == LLM:
        return _no(field, src, "llm never writes fact columns", stored, candidate)
    cand_rank = rank_of(field, src)
    if cand_rank is None:
        return _no(field, src, f"{src} never writes {field}", stored, candidate)
    if candidate is None or str(candidate).strip() == "":
        return _no(field, src, "blank candidate", record=False)

    handler = _FIELD_RULES.get(field)
    if handler:
        early = handler(stored, provenance, candidate, src, ctx)
        if early is not None:
            return early

    cand = candidate
    if is_blank(field, cand):
        return _no(field, src, "junk candidate", stored, cand, record=False)
    if is_blank(field, stored):
        return _yes(field, cand, src, "fills blank")
    if _same(field, stored, cand):
        return _no(field, src, "unchanged", record=False)
    if (field in ("engine_description", "engine_l") and src == EPA_CATALOG
            and ctx.get("corroborated")):
        return _yes(field, cand, src, "catalog correction with corroboration (D-PR3)")
    if src in FILL_ONLY.get(field, ()):
        return _no(field, src, "fill-only source, column already set", stored, cand)
    stored_src = field_source(field, stored, provenance)
    stored_rank = rank_of(field, stored_src) or 0
    if cand_rank > stored_rank:
        return _yes(field, cand, src, f"{src} outranks {stored_src}")
    return _no(field, src, f"{src} does not outrank {stored_src}", stored, cand)


# --- per-field guards ------------------------------------------------------
def _drive_rule(stored, provenance, candidate, src, ctx):
    field = "drivetrain"
    vpic_raw = ctx.get("vpic_drive")
    vpic_end = normalize_drivetrain(vpic_raw, "vpic") if vpic_raw else None
    vpic_two = bool(vpic_raw) and is_two_wheel_unknown(vpic_raw)
    cand_n = normalize_drivetrain(candidate, "vpic" if src == NHTSA_VPIC else "dealer")
    cand_two = is_two_wheel_unknown(candidate)
    stored_n = normalize_drivetrain(stored, "dealer")
    stored_src = field_source(field, stored, provenance)

    if src == NHTSA_VPIC:
        if cand_n:
            if stored_n and same_drive_wheels(stored_n, cand_n) and stored_n in ("AWD", "4WD"):
                return _no(field, src, "AWD and 4WD are the same wheels", record=False)
            if stored_n == cand_n:
                return _no(field, src, "unchanged", record=False)
            return _yes(field, cand_n, src, "vPIC decides driven wheels")
        if cand_two:
            # Constraint {FWD, RWD, 2WD}: never mapped to an end.
            if stored_n in ("FWD", "RWD"):
                return _no(field, src, "stored end satisfies the vPIC 4x2 constraint", record=False)
            if stored_n in ("AWD", "4WD"):
                return _yes(field, "2WD", src, "vPIC 4x2 contradicts the stored all-wheel claim",
                            cannot_assess=True)
            if is_blank("drivetrain", stored):
                return _yes(field, "2WD", src, "fills blank with the constraint")
            return _no(field, src, "unchanged", record=False)
        return _no(field, src, "vPIC drive not decisive", record=False)

    # Less specific than a stored specific end: never replaces it.
    if cand_n is None:
        if cand_two or str(candidate).strip().lower() in _JUNK:
            if stored_n:
                return _no(field, src, "less specific value never replaces a stored end", stored, candidate)
            if cand_two and is_blank(field, stored):
                return _yes(field, drivetrain_storage_value(candidate), src, "fills blank with 2WD")
        return _no(field, src, "unrecognised drivetrain", record=False)

    # vPIC specific end/wheels outrank every non-vPIC signal.
    if vpic_end and not same_drive_wheels(vpic_end, cand_n):
        return _no(field, src, "contradicts vPIC", vpic_end, cand_n, cannot_assess=_signals_against(
            vpic_end, stored_n, cand_n) >= 2)
    if vpic_two and cand_n in ("AWD", "4WD"):
        return _no(field, src, "vPIC 4x2 forbids all-wheel", "2WD", cand_n,
                   cannot_assess=_signals_against("2WD", stored_n, cand_n) >= 2)

    if src == EPA_TIEBREAK:
        if vpic_end:
            return _no(field, src, "vPIC already decided the drive", record=False)
        tdrv = ctx.get("trim_drive")
        if tdrv and not same_drive_wheels(tdrv, cand_n):
            return _no(field, src, "trim decoder veto (4MATIC / xDrive / AWD badge)", stored, cand_n)
        if stored_n is None or stored_n == cand_n:
            return _yes(field, cand_n, src, "tiebreak fills 2WD / blank") if stored_n is None \
                else _no(field, src, "unchanged", record=False)
        # D-PR4 sub-question: a dealer FWD/RWD loses only when vPIC is silent or
        # says 4x2 and the trim decoder agrees with the catalog.
        if stored_n in ("FWD", "RWD") and cand_n in ("FWD", "RWD") and tdrv == cand_n:
            return _yes(field, cand_n, src, "tiebreak corrected by the trim decoder")
        return _no(field, src, "tiebreak never replaces a stored specific end", stored, cand_n)

    # Other sources: a less specific stored value (2WD / blank) is simply filled.
    if stored_n is None:
        return _yes(field, cand_n, src, "fills blank / 2WD")
    if src in FILL_ONLY["drivetrain"]:
        return _no(field, src, "fill-only source, column already set", stored, cand_n)
    if same_drive_wheels(stored_n, cand_n):
        return _no(field, src, "unchanged", record=False)
    cr = rank_of(field, src) or 0
    if cr > (rank_of(field, stored_src) or 0):
        return _yes(field, cand_n, src, f"{src} outranks {stored_src}")
    return _no(field, src, f"{src} does not outrank {stored_src}", stored, cand_n)


def _signals_against(vpic_end, stored_n, cand_n) -> int:
    """How many listing signals (stored value, candidate) contradict vPIC (D-PR6)."""
    n = 0
    for s in (stored_n, cand_n):
        if s and (
            (vpic_end == "2WD" and s in ("AWD", "4WD")) or
            (vpic_end != "2WD" and not same_drive_wheels(vpic_end, s))
        ):
            n += 1
    return n


def _fuel_rule(stored, provenance, candidate, src, ctx):
    field = "fuel_type"
    text = str(candidate).strip()
    cand_e = fuel_label_electrification(text)
    stored_e = fuel_label_electrification(stored)
    vpic = ctx.get("vpic")
    if src == NHTSA_VPIC:
        low = text.lower()
        decisive = cand_e in (BEV, PHEV, FCEV, HEV) or "diesel" in low
        if low == "gasoline" and not decisive:
            # N15.5: plain Gasoline only fills a blank / not-a-fuel / abbreviation
            # of gasoline ("F", "G", "Direct Injection"); it never replaces a
            # stated hybrid, diesel or electric label.
            from backend.utils.field_clean import coerce_fuel_type_stored

            stored_c = coerce_fuel_type_stored(stored)
            if stored_c is None or stored_c == "Gasoline":
                if str(stored or "").strip() == text:
                    return _no(field, src, "unchanged", record=False)
                return _yes(field, text, src, "vPIC gasoline fills a blank or non-canonical fuel (N15.5)")
            return _no(field, src, "plain-gasoline decode never replaces a stated fuel", record=False)
        if "mild" in low or not decisive:
            return _no(field, src, "mild hybrid / silent / plain-gasoline decode changes nothing",
                       record=False)
        if _same(field, stored, text):
            return _no(field, src, "unchanged", record=False)
        return _yes(field, text, src, "vPIC electrification / diesel overrides the feed")
    if cand_e == HEV and stored_e in PLUG_IN:
        return _no(field, src, "a Hybrid label never downgrades PHEV / BEV", stored, text)
    if vpic is not None and cand_e is not None:
        decided = electrification({"fuel_type": text}, vpic)
        if decided != cand_e:
            return _no(field, src, "contradicts the vPIC decode", stored, text)
    return None


def _cyl_rule(stored, provenance, candidate, src, ctx):
    field = "cylinders"
    try:
        n = int(candidate)
    except (TypeError, ValueError):
        return _no(field, src, "non-numeric cylinders", record=False)
    fuel = ctx.get("fuel_type")
    if n > 0 and cylinders_conflicts_with_engine_text(n, ctx.get("engine_description"), fuel):
        return _no(field, src, "contradicts the engine text / EV", stored, n)
    return None


_FIELD_RULES = {"drivetrain": _drive_rule, "fuel_type": _fuel_rule, "cylinders": _cyl_rule}


# --- batch helpers ---------------------------------------------------------
def provenance_patch(decision: Decision, *, fetched_at: str | None = None) -> dict[str, dict]:
    """``spec_source_json`` patch for one decision ({} when it has nothing to say)."""
    if decision.write:
        entry: dict[str, Any] = {"source": decision.source, "value": decision.value,
                                 "fetched_at": fetched_at or _now()}
        if decision.cannot_assess:
            entry["cannot_assess"] = True
        return {decision.field: entry}
    if decision.discrepancy:
        return {decision.field: {"discrepancy": dict(decision.discrepancy),
                                 "cannot_assess": bool(decision.cannot_assess) or None}}
    return {}


def plan_updates(row: Mapping[str, Any], proposals: list[tuple[str, Any, str]],
                 ctx: Mapping[str, Any] | None = None) -> tuple[dict[str, Any], dict[str, dict], list[Decision]]:
    """Decide ordered *proposals* ``(field, value, source)`` against *row*.

    Returns ``(column_changes, provenance_patch, decisions)``. Later proposals
    see earlier winners (so the result does not depend on a second pass), and
    running the plan on its own output proposes nothing (idempotent). The patch
    keeps the stored entry fields (value / source) when only a discrepancy is added.
    """
    state = {f: row.get(f) for f in FIELDS}
    prov = dict(_prov_entries(row.get("spec_source_json")))
    changes: dict[str, Any] = {}
    patch: dict[str, dict] = {}
    decisions: list[Decision] = []
    base = dict(ctx or {})
    for field, value, source in proposals:
        c = dict(base)
        c.setdefault("engine_description", state.get("engine_description"))
        c.setdefault("fuel_type", state.get("fuel_type"))
        d = decide(field, state.get(field), prov, value, source, c)
        decisions.append(d)
        p = provenance_patch(d)
        if d.write:
            state[field] = d.value
            changes[field] = d.value
            prov[field] = p[field]
            patch[field] = p[field]
        elif p:
            merged = dict(prov.get(field) if isinstance(prov.get(field), dict) else {})
            merged.update({k: v for k, v in p[field].items() if v is not None})
            prov[field] = merged
            patch[field] = merged
    return changes, patch, decisions
