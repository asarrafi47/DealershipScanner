"""Pick the ladder definition for a car and resolve it (public entry point)."""
from __future__ import annotations

import csv
import logging
from functools import lru_cache
from typing import Any

logger = logging.getLogger(__name__)
from backend.enrichment.dictionary_catalog import iter_dt_options_paths

from ._common import (
    _MIN_TRIM_LADDER_YEAR,
    _extract_trim_from_cell,
    _find_complete_options_csv,
    _find_epa_csv,
    _is_valid_trim_name,
    _norm_make,
    _norm_model,
    _normalize_listing_model,
)
from .adds_filter import (
    _only_marketing_brochure_placeholders,
    _trim_ladder_should_display,
)
from .build import (
    _build_ladder_result,
)
from .csv_ladders import (
    _ladder_from_complete_options_csv,
    _ladder_from_dt_csv,
    _ladder_from_epa_csv,
)
from .document_order import (
    _apply_document_order,
    _document_rung_order,
    _step_trim_keys,
)
from .inventory import (
    _ladder_from_inventory,
)
from .loaders import (
    _load_generated_ladders,
    _load_json_ladders,
    _load_merged_ladders,
)
from .plausibility import (
    _complete_options_ladder_is_junk,
    _finalize_ladder_steps,
    _ladder_steps_plausible_for_model,
    _ladder_steps_usable,
)
from .steps import (
    _filter_ladder_steps_for_year,
    _find_ladder_step,
    _index_ladder_steps_by_trim,
    _step_applies_to_year,
)

@lru_cache(maxsize=1)
def _all_ladder_defs() -> tuple[dict[str, Any], ...]:
    seen_ids: set[str] = set()
    merged: list[dict[str, Any]] = []
    loaders = [_load_json_ladders, _load_generated_ladders]
    for loader in loaders:
        for ladder in loader():
            if not isinstance(ladder, dict):
                continue
            lid = str(ladder.get("id") or "")
            if lid and lid not in seen_ids:
                seen_ids.add(lid)
                merged.append(ladder)
    for ladder in _load_dt_csv_ladders():
        if not isinstance(ladder, dict):
            continue
        lid = str(ladder.get("id") or "")
        if lid and lid not in seen_ids:
            seen_ids.add(lid)
            merged.append(ladder)
    return tuple(merged)


def _load_dt_csv_ladders() -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for path in sorted(iter_dt_options_paths()):
        ladder = _ladder_from_dt_csv(path)
        if ladder:
            out.append(ladder)
    return out


def _year_in_range(year: Any, ladder: dict[str, Any]) -> bool:
    try:
        y = int(year)
    except (TypeError, ValueError):
        return True
    ymin = int(ladder.get("year_min") or 0)
    ymax = int(ladder.get("year_max") or 9999)
    return ymin <= y <= ymax


def _ladder_matches_car(ladder: dict[str, Any], make: str, model: str, year: Any) -> bool:
    from backend.enrichment.dictionary_catalog import catalog_key as _catalog_key_for_car

    try:
        y = int(year)
        ladder_ck = ladder.get("catalog_key")
        if ladder_ck and _catalog_key_for_car(y, make, model) == str(ladder_ck):
            return True
    except (TypeError, ValueError):
        pass
    if _norm_make(ladder.get("make") or "") != _norm_make(make):
        return False
    if not _year_in_range(year, ladder):
        return False
    car_model = _norm_model(model)
    if not car_model:
        return False
    for m in ladder.get("models") or []:
        lm = _norm_model(str(m))
        if not lm:
            continue
        if car_model == lm:
            return True
        # Listing model may be more specific than ladder key (e.g. "1500" vs "1500 Classic").
        if len(lm) >= 3 and car_model.startswith(lm):
            return True
    return False


def _pick_curated_ladder(make: str, model: str, year: Any) -> dict[str, Any] | None:
    matches = [lad for lad in _all_ladder_defs() if _ladder_matches_car(lad, make, model, year)]
    if not matches:
        return None
    try:
        y = int(year)
    except (TypeError, ValueError):
        return matches[0]

    def year_dist(lad: dict[str, Any]) -> int:
        ymin = int(lad.get("year_min") or 0)
        ymax = int(lad.get("year_max") or 9999)
        if ymin <= y <= ymax:
            return 0
        return min(abs(y - ymin), abs(y - ymax))

    def source_rank(lad: dict[str, Any]) -> int:
        src = str(lad.get("source") or "").lower()
        if src == "curated":
            return 0
        if src == "brochure":
            return 9
        if src in {"epa", "epa_fueleconomy", "epa csv", "epa_fueleconomy.gov"}:
            return 2
        if src == "inventory":
            return 4
        return 3

    def year_mid_dist(lad: dict[str, Any]) -> int:
        """Among equal-rank ladders covering the query year, prefer the narrowest/closest range."""
        ymin = int(lad.get("year_min") or 0)
        ymax = int(lad.get("year_max") or 9999)
        mid = (ymin + ymax) // 2 if ymin and ymax < 9999 else y
        return abs(y - mid)

    # Prefer curated OEM ladders over per-year brochure auto-ladders when both match.
    # Secondary tiebreaker: prefer year-range midpoint closest to query year so the
    # most-specific ladder (e.g. 2015 EPA vs 2013 EPA for year=2015) wins.
    matches.sort(key=lambda lad: (source_rank(lad), year_dist(lad), year_mid_dist(lad)))
    return matches[0]


def _generic_trim_ladder_def(make: str, model: str, year: Any) -> dict[str, Any]:
    from backend.enrichment.trim_ladder_knowledge import generic_fallback_steps

    try:
        y = int(year)
        ymin, ymax = y - 5, y + 2
    except (TypeError, ValueError):
        ymin, ymax = 0, 9999
    return {
        "id": f"generic_{_norm_make(make)}_{_norm_model(model)}",
        "make": make,
        "models": [model, _norm_model(model)],
        "year_min": ymin,
        "year_max": ymax,
        "label": f"{make} {model} trim lineup",
        "source": "oem_knowledge",
        "steps": generic_fallback_steps(make, model),
    }


def _pick_brochure_ladder(make: str, model: str, year: Any) -> dict[str, Any] | None:
    from backend.enrichment.brochure_extract import load_brochure_trim_overlay
    from backend.enrichment.dictionary_catalog import catalog_key as _ck

    # Hand-reviewed overlay drives steps; avoid auto brochure ladder superseding EPA/options base.
    if load_brochure_trim_overlay(year, make, model):
        return None

    try:
        y = int(year)
    except (TypeError, ValueError):
        return None
    target = _ck(y, make, model)
    for lad in _load_merged_ladders():
        if str(lad.get("catalog_key") or "") != target:
            continue
        if str(lad.get("source") or "").lower() != "brochure":
            continue
        from backend.enrichment.brochure_trim_candidates import filter_spurious_brochure_trims
        from backend.enrichment.trim_ladder_knowledge import normalize_ladder_steps

        finalized = _finalize_ladder_steps(lad, make, model)
        names = filter_spurious_brochure_trims(
            [str(s.get("name") or "").strip() for s in finalized.get("steps") or []],
            make=make,
            model=model or "",
        )
        if len(names) < 2:
            return None
        seen: set[str] = set()
        ordered: list[dict[str, Any]] = []
        for n in names:
            key = n.lower()
            if key in seen:
                continue
            seen.add(key)
            ordered.append({"name": n, "aliases": [], "adds": []})
        finalized = {
            **finalized,
            "source": "brochure",
            "steps": normalize_ladder_steps(ordered, make, model=model or ""),
        }
        if _ladder_steps_plausible_for_model(finalized.get("steps") or [], make, model):
            return finalized
    return None


def _pick_ladder_def(make: str, model: str, year: Any) -> dict[str, Any] | None:
    from backend.enrichment.brochure_extract import load_brochure_trim_overlay

    has_overlay = bool(load_brochure_trim_overlay(year, make, model))
    curated = _pick_curated_ladder(make, model, year)
    if curated:
        src = str(curated.get("source") or "").lower()
        if src == "brochure" and has_overlay:
            curated = None
        else:
            finalized = _finalize_ladder_steps(curated, make, model)
            if _ladder_steps_plausible_for_model(finalized.get("steps") or [], make, model):
                return finalized

    epa_path = _find_epa_csv(make, model, year)
    if epa_path:
        epa_ladder = _ladder_from_epa_csv(epa_path, make, model, year)
        if epa_ladder and _ladder_steps_plausible_for_model(epa_ladder.get("steps") or [], make, model):
            return epa_ladder

    brochure = _pick_brochure_ladder(make, model, year)
    if brochure and len(brochure.get("steps") or []) >= 3:
        return brochure

    csv_path = _find_complete_options_csv(make, model, year)
    if csv_path:
        try:
            csv_blob = csv_path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            csv_blob = ""
        ladder = _ladder_from_complete_options_csv(csv_path, make, model, year)
        if ladder and _ladder_matches_car(ladder, make, model, year):
            finalized = _finalize_ladder_steps(ladder, make, model)
            if (
                _ladder_steps_usable(finalized.get("steps") or [], make, model=model)
                and _ladder_steps_plausible_for_model(finalized.get("steps") or [], make, model)
                and not _complete_options_ladder_is_junk(
                    finalized.get("steps") or [], make, model, source_blob=csv_blob
                )
            ):
                return finalized
        blob = csv_blob
        from backend.enrichment.trim_ladder_knowledge import extract_trims_from_text, merge_trim_names

        col_trims = []
        try:
            with csv_path.open(encoding="utf-8", newline="") as fh:
                for row in csv.DictReader(fh):
                    raw = (row.get("Trim") or "").strip()
                    if not raw:
                        continue
                    name = _extract_trim_from_cell(raw, make, model)
                    if name and _is_valid_trim_name(name, make=make, model=model):
                        col_trims.append(name)
        except OSError:
            pass
        merged = merge_trim_names(
            col_trims,
            extract_trims_from_text(make, blob, model=model),
            make=make,
            model=model,
        )
        if len(merged) >= 3:
            ladder = {
                "id": csv_path.stem.lower(),
                "make": make,
                "models": [model],
                "year_min": int(year) - 2 if str(year).isdigit() else 0,
                "year_max": int(year) + 2 if str(year).isdigit() else 9999,
                "label": f"{make} {model} trim lineup",
                "source": csv_path.name,
                "steps": [
                    {
                        "name": n,
                        "aliases": [],
                        "adds": [],
                    }
                    for n in merged
                ],
            }
            finalized = _finalize_ladder_steps(ladder, make, model)
            if (
                _ladder_steps_usable(finalized.get("steps") or [], make, model=model)
                and _ladder_steps_plausible_for_model(finalized.get("steps") or [], make, model)
                and not _complete_options_ladder_is_junk(
                    finalized.get("steps") or [], make, model, source_blob=blob
                )
            ):
                return finalized

    generic = _generic_trim_ladder_def(make, model, year)
    generic_steps = generic.get("steps") or []

    inventory = _ladder_from_inventory(make, model, year)
    inv_steps = inventory.get("steps") or [] if inventory else []
    if (
        len(inv_steps) >= 3
        and inventory
        and _ladder_steps_usable(inv_steps, make, model=model)
        and _ladder_steps_plausible_for_model(inv_steps, make, model)
    ):
        return inventory

    if len(generic_steps) >= 2:
        return generic

    if (
        inventory
        and len(inv_steps) >= 2
        and _ladder_steps_usable(inv_steps, make, model=model)
        and _ladder_steps_plausible_for_model(inv_steps, make, model)
    ):
        return inventory

    return generic if generic_steps else None


def resolve_trim_ladder(
    *,
    make: str | None,
    model: str | None,
    year: Any = None,
    trim: str | None,
) -> dict[str, Any] | None:
    """
    Return trim ladder context for a listing, or None if no ladder applies.
    """
    if not make or not model:
        return None
    try:
        model_year = int(year)
    except (TypeError, ValueError):
        model_year = 0
    if model_year and model_year < _MIN_TRIM_LADDER_YEAR:
        return None
    model = _normalize_listing_model(make, model)
    ladder_def = _pick_ladder_def(make, model, year)
    if not ladder_def:
        ladder_def = _generic_trim_ladder_def(make, model, year)

    steps_def = ladder_def.get("steps") or []
    if len(steps_def) < 2:
        ladder_def = _generic_trim_ladder_def(make, model, year)
        steps_def = ladder_def.get("steps") or []

    from backend.enrichment.brochure_extract import (
        load_brochure_trim_overlay,
        overlay_citable_for_year,
    )

    brochure_overlay = load_brochure_trim_overlay(year, make, model)
    # NEIGHBOUR-YEAR DRIFT. ``load_brochure_trim_overlay`` walks (0, -1, +1, -2,
    # +2), so a book from an adjacent model year was still deciding WHICH RUNGS
    # ARE LISTED — reordering the ladder, adding trims the ladder def did not
    # have, and dropping trims it did — long after the same overlay had been
    # blocked from contributing a single bullet. Measured on the live fleet:
    # 20,637 active cars got an overlay for their own model year and 11,674 got
    # one from a neighbouring year. Trims change between years, so only this
    # car's own book may shape the rung list. The overlay object is still passed
    # to ``_build_ladder_result`` below: its bullets are year-gated there, and
    # its mere presence is what stops Complete_Options CSV lines refilling the
    # rungs it emptied.
    overlay_for_year = (
        brochure_overlay if overlay_citable_for_year(brochure_overlay, year) else None
    )
    curated_ladder = str(ladder_def.get("source") or "").lower() == "curated"
    preserve_all_steps = bool(ladder_def.get("preserve_all_steps"))
    skip_trim_year_windows = preserve_all_steps
    # THE ORDER, and the evidence for it. Top first, matching the step list.
    # Empty whenever this car's own book states nothing about the sequence, and
    # every branch below then leaves ``luxury_rank`` in charge with the rungs
    # labelled ``unproven`` — the hand-typed table is the last resort, never the
    # thing that overrules a page of the brochure.
    doc_order, doc_basis = _document_rung_order(
        overlay_for_year, year=year, make=make, model=model
    )
    if overlay_for_year:
        from backend.enrichment.trim_ladder_knowledge import luxury_rank

        preserve_all_steps = False
        brochure_trims = set(overlay_for_year.get("trims_available") or [])
        brochure_trims.update((overlay_for_year.get("adds_by_trim") or {}).keys())
        steps_before_brochure = list(steps_def)

        if curated_ladder and brochure_trims:
            # Curated ladders: keep curated step list but apply year-window filtering.
            # Trims confirmed by the overlay's trims_available are force-included even if
            # their knowledge-base year window disagrees (overlay is authoritative for the year).
            overlay_confirmed = {str(t).strip() for t in (overlay_for_year.get("trims_available") or [])}
            filtered_steps: list[dict[str, Any]] = []
            for step in steps_def:
                name = str(step.get("name") or "").strip()
                if name in overlay_confirmed:
                    filtered_steps.append(step)
                elif _step_applies_to_year(step, year, make=make, model=model, skip_trim_year_windows=False):
                    filtered_steps.append(step)
            # A curated ladder's step ORDER is hand-typed too. When the car's own
            # book states the sequence, that outranks it (rule D: the curated
            # list is not evidence and may not override a document-derived edge).
            steps_def = _apply_document_order(
                filtered_steps, doc_order, doc_basis, make=make, model=model
            )
            skip_trim_year_windows = True  # year filtering already applied above
        else:
            skip_trim_year_windows = True
            if len(brochure_trims) >= 2:
                order = list(overlay_for_year.get("trims_available") or [])
                if not order:
                    order = list((overlay_for_year.get("adds_by_trim") or {}).keys())
                order = sorted(
                    [str(t).strip() for t in order if str(t).strip()],
                    key=lambda t: luxury_rank(t, make, model),
                )
                existing_index = _index_ladder_steps_by_trim(steps_def, make=make, model=model)
                steps_def = []
                seen_step_keys: set[str] = set()
                for t in order:
                    if t not in brochure_trims:
                        continue
                    step = _find_ladder_step(existing_index, t, make=make, model=model) or {
                        "name": t,
                        "aliases": [],
                        "adds": [],
                    }
                    steps_def.append(step)
                    seen_step_keys |= _step_trim_keys(step, make=make, model=model)
                if str(ladder_def.get("source") or "").lower() == "inventory":
                    for step in steps_before_brochure:
                        step_keys = _step_trim_keys(step, make=make, model=model)
                        if step_keys & seen_step_keys:
                            continue
                        steps_def.append(step)
                        seen_step_keys |= step_keys
                steps_def.sort(
                    key=lambda s: luxury_rank(str(s.get("name") or ""), make, model),
                )
                # ``luxury_rank`` above is the fallback, applied first so the
                # rungs the document does not place still land somewhere
                # sensible relative to each other. The document then overrules
                # it for every rung it DOES place, and stamps each step with
                # what put it where it is.
                steps_def = _apply_document_order(
                    steps_def, doc_order, doc_basis, make=make, model=model
                )
    steps_def = _filter_ladder_steps_for_year(
        steps_def,
        year,
        make=make,
        model=model,
        skip_trim_year_windows=skip_trim_year_windows,
    )
    if len(steps_def) < 2:
        ladder_def = _generic_trim_ladder_def(make, model, year)
        steps_def = _filter_ladder_steps_for_year(
            ladder_def.get("steps") or [],
            year,
            make=make,
            model=model,
            skip_trim_year_windows=bool(ladder_def.get("preserve_all_steps")),
        )

    result = _build_ladder_result(
        {**ladder_def, "steps": steps_def},
        make=make,
        model=model,
        year=year,
        trim=trim,
        brochure_overlay=brochure_overlay,
    )
    if len(result.get("steps") or []) < 2:
        fallback_def = _generic_trim_ladder_def(make, model, year)
        fallback_steps = _filter_ladder_steps_for_year(
            fallback_def.get("steps") or [],
            year,
            make=make,
            model=model,
        )
        if len(fallback_steps) >= 2:
            result = _build_ladder_result(
                {**fallback_def, "steps": fallback_steps},
                make=make,
                model=model,
                year=year,
                trim=trim,
                brochure_overlay=None,
            )

    if len(result.get("steps") or []) < 2:
        return None
    if _only_marketing_brochure_placeholders(result):
        return None
    step_dicts = [{"name": s.get("name")} for s in result.get("steps") or []]
    source = str(result.get("source") or "").lower()
    plausible_ok = _ladder_steps_plausible_for_model(step_dicts, make, model)
    is_generic_oem = str(result.get("id") or "").startswith("generic_")
    if not plausible_ok and source not in {"oem_knowledge", "inventory", "brochure"}:
        return None
    if not plausible_ok and is_generic_oem:
        return None
    if not _trim_ladder_should_display(result, make, model):
        # Premium VDP: still show OEM fallback ladder when quality heuristics are conservative.
        if str(result.get("source") or "").lower() in {
            "oem_knowledge",
            "inventory",
            "brochure",
        } and not is_generic_oem:
            result["quality"] = "medium"
            return result
        fallback_def = _generic_trim_ladder_def(make, model, year)
        # Skip trim-specific year windows so generic steps aren't over-filtered.
        fallback_steps = _filter_ladder_steps_for_year(
            fallback_def.get("steps") or [],
            year,
            make=make,
            model=model,
            skip_trim_year_windows=True,
        )
        if len(fallback_steps) >= 2:
            result = _build_ladder_result(
                {**fallback_def, "steps": fallback_steps},
                make=make,
                model=model,
                year=year,
                trim=trim,
                brochure_overlay=None,
            )
            if len(result.get("steps") or []) >= 2:
                result["quality"] = "medium"
                return result
        # Absolute last resort: return unfiltered generic steps so premium VDP always
        # shows a trim ladder even for unusual makes/years.
        all_steps = fallback_def.get("steps") or []
        if len(all_steps) >= 2:
            result = _build_ladder_result(
                {**fallback_def, "steps": all_steps},
                make=make,
                model=model,
                year=year,
                trim=trim,
                brochure_overlay=None,
            )
            if len(result.get("steps") or []) >= 2:
                result["quality"] = "low"
                return result
        return None
    return result
