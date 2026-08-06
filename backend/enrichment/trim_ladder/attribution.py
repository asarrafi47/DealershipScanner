"""Attribute an observed trim/spec row to a rung. Fuzzy scoring is revoked."""
from __future__ import annotations

import csv
import logging
from typing import Any

logger = logging.getLogger(__name__)

from ._common import (
    _extract_trim_from_cell,
    _find_complete_options_csv,
    _norm_token,
)
from .claims import (
    _drivetrain_neutral_forms,
    _rung_claim_forms,
)

def _exact_rung_match(
    listing_trim: str,
    step_name: str,
    aliases: list[str],
    *,
    make: str,
    model: str | None = None,
    tier: int = 1,
) -> bool:
    """True only when ``listing_trim`` IS this rung. Near-misses are not matches.

    Sameness is spelling equality under ``_rung_claim_forms``, never
    similarity. Prefix, suffix, word-boundary and substring — every relation
    the revoked scorer graded between 60 and 85 — are rejected, and there is no
    threshold to retune:

        "LE"             is not "XLE Premium"   (substring)
        "Sport"          is not "Sport Prestige"(prefix)
        "SX-Prestige"    is     "SX Prestige"   (punctuation only)
        "Limited"        is not "Limited Platinum"

    ``aliases`` IS ACCEPTED AND IGNORED — see below. The parameter stays in the
    signature only because ``backend/scripts/trim_coverage_report.py`` and the
    tests call this positionally; passing a non-empty list changes nothing.

    ALIASES WERE A MATCHING ROUTE AND ARE REVOKED (2026-08-02). The previous
    version compared the listing trim against the rung's declared aliases as
    well as its name, and this docstring asserted "a neighbouring trim is never
    an alias, so aliases cannot re-open the door". THAT ASSERTION WAS FALSE, and
    it is corrected here together with the code. The ladder definitions were
    read — not recalled — and every list below is copied out of
    ``trim_ladders.json`` on 2026-08-02:

        "Raptor R"                 aliased under the Ford F-150 "Raptor" rung
                                   (different truck: 700 hp V8 vs 450 hp V6)
        "Premium Luxury Platinum"  aliased under the Cadillac Escalade
                                   "Premium Luxury" rung
        "AT4 Ultimate", "AT4X"     aliased under the GMC Yukon "AT4" rung
        "Standard Range Plus"      aliased under the Tesla Model 3
                                   "Standard Range" rung
        "Scat Pack Plus"           aliased under the Dodge Charger
                                   "Scat Pack" rung
        "328i", "335i"             BOTH aliased under the BMW 3 Series "330i"
                                   rung — a 240 hp turbo four and a 300 hp
                                   turbo six declared to be one 248 hp trim

    An alias table is OUR OWN mapping. Nothing in it is quoted from a document,
    no part of the pipeline checks an entry against one, and the entries above
    are wrong. It therefore cannot carry the claim "this car IS that trim" —
    the same objection ``_exact_inventory_rung_hits`` already records against
    using aliases to justify a rung NAME, for the same reason.

    Measured on the live fleet (active cars, model year 2015+, 13,849
    year/make/model/trim combos): the alias route was the sole support for the
    "This vehicle" badge on 550 cars in 43 combos. 31 of those, in 4 combos,
    are now carried by the rung name instead, because the enumerated door-count
    noise rule was repaired at the same time (see ``_CLAIM_NOISE_RES``). The
    other 519 cars in 39 combos lose the badge. The cost is stated rather than
    hidden, and the two largest pieces of it are NOT the same case:

      * 421 Ram 1500 cars listed as "Big Horn/Lone Star". This reads like a
        harmless double badge, and it is not: the rendered 2026 Ram 1500 ladder
        is ``[Tungsten, Limited, Laramie, Rebel, Lone Star, Big Horn,
        Tradesman]`` — verified by rendering /car/148924, not assumed — so the
        ladder itself treats Lone Star and Big Horn as two different rungs, and
        the alias route was silently picking one of them. Withholding is the
        only defensible answer here.
      * 18 Toyota Tacoma cars listed as "Tacoma TRD Off-Road", where the label
        is the rung name with the model name in front of it. That one probably
        IS one trim, but establishing it needs a model-name-prefix rule with its
        own evidence, not a hand-kept list that also contains "335i".

    Until such a rule exists with its evidence written down, silence is the
    correct output.

    An empty trim string matches nothing. It used to match everything: the old
    scorer normalised a missing trim to ``""``, and ``"" in cn`` is true for
    every rung, which is how 205 active cars with no trim string at all were
    stamped "This vehicle" on a rung.

    ``tier`` distinguishes the two strengths the caller has to keep apart:

      1 — the labels agree as written, bar case, punctuation and the
          enumerated noise. This is the answer whenever it exists.
      2 — they agree only after a FUSED drivetrain is dropped from the
          listing ("sDrive40i" → "40i"), and is offered only for a rung whose
          own name is drivetrain-neutral, so the badge cannot print a
          drivetrain the car has not got. ``_build_ladder_result`` consults
          tier 2 only when no rung answered at tier 1 — otherwise a BMW X5
          listed as "xDrive40i" would tie its own "xDrive40i" rung against the
          "sDrive40i" rung and the ambiguity guard would throw away a claim
          that was literally correct. Measured on the live fleet 2026-08-02:
          that tie silenced 687 cars across 28 combos whose trim string is
          character-for-character a rung name, every one of them a BMW X3/X5
          sDrive/xDrive pair. With the tiers in place, tier 2 adds 0 claims on
          a rung the listing does not name literally.
    """
    lt_forms = _rung_claim_forms(listing_trim, make, model)
    if not lt_forms:
        return False
    # The rung's NAME is the only label a claim may rest on. ``aliases`` is
    # deliberately not read here — see the docstring.
    rung_labels = [str(step_name or "")]
    if tier == 1:
        rung_forms: set[str] = set()
        for label in rung_labels:
            rung_forms |= _rung_claim_forms(label, make, model)
        return bool(lt_forms & rung_forms)

    lt_loose = _drivetrain_neutral_forms(listing_trim, make, model)
    for label in rung_labels:
        label_forms = _rung_claim_forms(label, make, model)
        if not label_forms:
            continue
        # The rung name must not itself name a drivetrain.
        if not (_drivetrain_neutral_forms(label, make, model) <= label_forms):
            continue
        if lt_loose & label_forms:
            return True
    return False


def _label_value_bullet(label: str, value: str) -> str:
    """``"Engine Options" + "Engine Options: LV3 …"`` → one label, not two.

    The CSV cell often already repeats its own column heading; prefixing it
    again is what put "Engine Options: Engine Options: LV3 EcoTec3 4.3 L V6" on
    the 2020 GMC Sierra Base rung.
    """
    from backend.enrichment.trim_ladder_knowledge import collapse_repeated_label_prefix

    return collapse_repeated_label_prefix(f"{label.strip()}: {value.strip()}")[:240]


def _row_trim_adds(row: dict[str, str]) -> list[str]:
    """Extract display-worthy feature bullets from a Complete_Options CSV row."""
    from backend.enrichment.trim_ladder_knowledge import is_generic_trim_add

    out: list[str] = []
    pkg = (row.get("Packages") or "").strip()
    pkg_d = (row.get("packageDetails") or "").strip()
    opt = (row.get("Options") or "").strip()
    opt_d = (row.get("optionDetails") or "").strip()
    if pkg and pkg_d and len(pkg_d) >= 20 and not is_generic_trim_add(pkg_d):
        out.append(_label_value_bullet(pkg, pkg_d))
    elif pkg and len(pkg) >= 12 and not is_generic_trim_add(pkg):
        out.append(pkg[:240])
    if opt and opt_d and len(opt_d) >= 20 and not is_generic_trim_add(opt_d):
        out.append(_label_value_bullet(opt, opt_d))
    elif opt and len(opt) >= 12 and not is_generic_trim_add(opt):
        out.append(opt[:240])
    return out[:4]


def _dictionary_adds_by_trim(make: str, model: str, year: Any) -> dict[str, list[str]]:
    """Load trim→feature bullets from the best Complete_Options CSV for this vehicle."""
    csv_path = _find_complete_options_csv(make, model, year)
    if not csv_path:
        return {}
    try:
        with csv_path.open(encoding="utf-8", newline="") as fh:
            rows = list(csv.DictReader(fh))
    except OSError:
        return {}
    out: dict[str, list[str]] = {}
    for row in rows:
        trim_raw = (row.get("Trim") or "").strip()
        if not trim_raw:
            continue
        name = _extract_trim_from_cell(trim_raw, make, model)
        if not name:
            continue
        adds = _row_trim_adds(row)
        if not adds:
            continue
        key = _norm_token(name)
        if key not in out:
            out[key] = adds
    return out


def _merge_trim_spec_values(left: str, right: str) -> str:
    """Combine two spec values for the same label (e.g. RWD + xDrive engine options)."""
    a = (left or "").strip()
    b = (right or "").strip()
    if not a:
        return b
    if not b or a.lower() == b.lower():
        return a
    if b.lower() in a.lower():
        return a
    if a.lower() in b.lower():
        return b
    return f"{a}; {b}"


def _raw_trim_specs(
    trim_name: str,
    *,
    make: str,
    model: str,
    year: Any,
    ladder_id: str,
    adds: list[str],
) -> list[dict[str, str]]:
    """``lookup_trim_specs`` without its lossy sanitize pass.

    ``trim_spec_sheets.lookup_trim_specs`` runs every value through
    ``sanitize_trim_specs``, which splits on ``;`` regardless of brackets and
    then rejoins the survivors — so "Hybrid 2.5L I4 (SIDI & PFI; Hybrid)" is
    already truncated to "Hybrid 2.5L I4 (SIDI & PFI" by the time it is
    returned. Reading the same two sources directly keeps the values intact so
    the bracket-aware split in ``_bullet_display_parts`` can do the cutting.
    """
    from backend.enrichment.trim_spec_sheets import _parse_trim_specs, _pick_sheet
    from backend.enrichment.trim_spec_extractor import extract_trim_specs

    name = (trim_name or "").strip()
    sheet = _pick_sheet(make=make, model=model, year=year, ladder_id=ladder_id)
    if sheet and name:
        trims = sheet.get("trims") or {}
        if isinstance(trims, dict):
            raw = trims.get(name)
            if raw is None:
                key = _norm_token(name)
                for k, v in trims.items():
                    if _norm_token(str(k)) == key:
                        raw = v
                        break
            parsed = _parse_trim_specs(raw)
            if parsed:
                return list(parsed)

    adds_key = "\n".join(str(a).strip() for a in (adds or []) if str(a).strip())
    return [
        dict(row)
        for row in extract_trim_specs(
            name,
            make=make,
            model=model,
            year=year,
            adds_key=adds_key,
        )
    ]


def _lookup_merged_trim_specs(
    step_name: str,
    aliases: list[str],
    *,
    make: str,
    model: str,
    year: Any,
    ladder_id: str,
    adds: list[str],
    dict_adds: dict[str, list[str]],
) -> list[dict[str, str]]:
    variant_names = [step_name] + [str(a).strip() for a in aliases if str(a).strip()]
    seen_labels: dict[str, dict[str, str]] = {}
    for i, variant in enumerate(variant_names):
        variant_adds = list(adds) if i == 0 else list(dict_adds.get(_norm_token(variant), []))
        part = _raw_trim_specs(
            variant,
            make=make,
            model=model,
            year=year,
            ladder_id=ladder_id,
            adds=variant_adds,
        )
        for row in part:
            label = str(row.get("label") or "").strip()
            value = str(row.get("value") or "").strip()
            if not label or not value:
                continue
            if label not in seen_labels:
                seen_labels[label] = {"label": label, "value": value}
            else:
                seen_labels[label]["value"] = _merge_trim_spec_values(
                    seen_labels[label]["value"],
                    value,
                )
    return list(seen_labels.values())


def _trim_spec_rows_to_bullets(
    rows: list[dict[str, str]] | None,
    *,
    trim_name: str = "",
    max_items: int = 8,
) -> list[str]:
    """Structured ``{label, value}`` spec rows → bullet candidates.

    Stands in for ``trim_spec_extractor.trim_specs_to_bullets`` so the split
    happens at bracket depth 0 (see ``_bullet_display_parts``); the shared
    helper cuts inside parentheses and hands back orphan halves.
    """
    from backend.enrichment.trim_ladder_knowledge import split_outside_brackets

    out: list[str] = []
    seen: set[str] = set()
    for row in rows or []:
        value = str((row or {}).get("value") or "").strip()
        if not value:
            continue
        for part in split_outside_brackets(value, separators=";|"):
            key = part.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(part)
    return out[:max_items]


def _curated_adds_for_step(step: dict[str, Any], year: Any) -> list[str]:
    """Return ladder-step adds, optionally overridden by ``adds_from_year`` thresholds."""
    base = [str(a).strip() for a in (step.get("adds") or []) if str(a).strip()]
    by_year = step.get("adds_from_year")
    if not isinstance(by_year, dict):
        return base
    try:
        y = int(year)
    except (TypeError, ValueError):
        return base
    thresholds: list[int] = []
    for key in by_year:
        try:
            thresholds.append(int(key))
        except (TypeError, ValueError):
            continue
    for threshold in sorted(thresholds, reverse=True):
        if y >= threshold:
            vals = by_year.get(str(threshold), by_year.get(threshold))
            if isinstance(vals, list):
                picked = [str(a).strip() for a in vals if str(a).strip()]
                if picked:
                    return picked
    return base
