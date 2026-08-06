"""
Resolve OEM trim ladder position for a listing (premium VDP feature).

Where the ORDER and the alias tables come from (first match wins):
1. ``trim_ladders.json`` curated ladders
2. ``trim_ladders_generated.json`` (built from dictionary CSVs)
3. ``*_DT_Complete_Options.csv`` dictionary files
4. Year-specific ``*_Make_Model_Complete_Options.csv`` (+ text extraction)
5. Active inventory for the same make/model (price-ordered trims)
6. Make-specific OEM trim knowledge (generic fallback)

WHICH RUNGS ARE LISTED is decided separately and afterwards, by
``_justified_ladder_steps``: a rung renders only if its displayed name is
character-for-character (case- and punctuation-insensitive, see
``_rung_evidence_key``) a trim string carried by an active listing of this exact
year/make/model, or the heading of a verified brochure citation for this model
year. None of the six producers above can put a rung name on the page on its own
— the sixth used to guarantee a ladder for every car, and that guarantee is
gone. Fewer than two justified rungs → no ladder.

Bullets are gated separately again, by ``brochure_extract.LADDER_BULLET_STORES``.

The ALIAS tables those producers ship no longer decide anything a shopper reads.
They are our own mapping, they are not checked against any document, and they
were found to contain neighbouring trims outright ("335i" filed under the "330i"
rung, "Raptor R" under "Raptor"), so as of 2026-08-02 neither the "This vehicle"
claim (``_exact_rung_match``) nor the brochure-section binding
(``_lookup_brochure_adds_key``) reads them. They survive only as display data on
the rendered step.

Historically one ~3900-line module. Split by responsibility into submodules
(``_common``, ``steps``, ``document_order``, ``bullets``, ``claims``,
``attribution``, ``citations``, ``evidence``, ``build``, ``engine_steps``,
``adds_filter``, ``loaders``, ``epa``, ``csv_ladders``, ``inventory``,
``plausibility``, ``selection``). This package re-exports every previously-public
name, so ``from backend.enrichment.trim_ladder import X`` keeps working unchanged.

Seams that tests reach into (``_inventory_rung_evidence`` and the cached
inventory readers in ``evidence``; ``_pick_ladder_def`` in ``selection``) must be
patched on the submodule that defines them -- rebinding the name on this facade
does not reach the caller.
"""
from __future__ import annotations

import csv
import json
import logging
import re
from functools import lru_cache
from pathlib import Path
from typing import Any
from backend.utils.spec_field_normalize import normalize_trim_text

logger = logging.getLogger(__name__)
from backend.enrichment.dictionary_catalog import find_complete_options_csv as _catalog_find_co
from backend.enrichment.dictionary_catalog import find_epa_csv as _catalog_find_epa
from backend.enrichment.dictionary_catalog import iter_dt_options_paths
from backend.enrichment.dictionary_paths import (
    trim_ladders_curated_path,
    trim_ladders_epa_path,
    trim_ladders_generated_path,
    trim_ladders_merged_path,
)

from ._common import (
    _JUNK_TRIM_TOKENS,
    _LADDERS_EPA_JSON,
    _LADDERS_GENERATED_JSON,
    _LADDERS_JSON,
    _LADDERS_MERGED_JSON,
    _MIN_TRIM_LADDER_YEAR,
    _NON_TRIM_RE,
    _TRIM_NOISE_RE,
    _YEAR_MIN_SUFFIX_RE,
    _YEAR_RANGE_SUFFIX_RE,
    _YEAR_SUFFIX_RE,
    _clean_trim_label,
    _extract_trim_from_cell,
    _filename_token,
    _find_complete_options_csv,
    _find_epa_csv,
    _is_valid_trim_name,
    _norm_make,
    _norm_model,
    _norm_token,
    _normalize_listing_model,
    _provenance_gate_on,
)
from .steps import (
    _clean_trim_step_display_name,
    _filter_ladder_steps_for_year,
    _find_ladder_step,
    _index_ladder_steps_by_trim,
    _lookup_brochure_adds_key,
    _step_applies_to_year,
    _trim_identity_keys,
)
from .document_order import (
    UNPROVEN_ORDER_BASIS,
    _apply_document_order,
    _document_rung_order,
    _step_trim_keys,
)
from .bullets import (
    _bullet_display_parts,
    _inventory_listing_price_fallback,
    _is_mechanical_trim_bullet,
    _merge_trim_display_bullets,
    _strip_inventory_price_adds,
)
from .claims import (
    _CLAIM_NOISE_RES,
    _drivetrain_neutral_forms,
    _rung_claim_forms,
    _strip_claim_noise,
)
from .attribution import (
    _curated_adds_for_step,
    _dictionary_adds_by_trim,
    _exact_rung_match,
    _label_value_bullet,
    _lookup_merged_trim_specs,
    _merge_trim_spec_values,
    _raw_trim_specs,
    _row_trim_adds,
    _trim_spec_rows_to_bullets,
)
from .citations import (
    _CITATION_KEY_RE,
    _CitationRegister,
    _citation_key,
    _epa_engine_citations,
    _register_epa_citations,
    _register_overlay_citations,
)
from .evidence import (
    _active_make_spellings,
    _active_trims_by_make_year,
    _exact_inventory_rung_hits,
    _inventory_rung_evidence,
    _justified_ladder_steps,
    _rung_evidence_key,
)
from .build import (
    _build_ladder_result,
)
from .engine_steps import (
    _BROCHURE_ADDS_TO_RE,
    _BROCHURE_ENGINE_ITEM_RE,
    _BROCHURE_PAGE_MARKER_RE,
    _TRADEMARK_RE,
    _brochure_engine_bullet_by_step,
    _brochure_engine_step_ups,
)
from .adds_filter import (
    _ADD_STOPWORDS,
    _ADD_TOKEN_RE,
    _GRID_MARK_RUN_RE,
    _GRID_MATCH_COVERAGE,
    _MIN_ADD_TOKENS_FOR_GRID_MATCH,
    _add_tokens,
    _lineup_standard_features,
    _norm_add_line,
    _only_marketing_brochure_placeholders,
    _suppress_non_adds,
    _trim_ladder_quality,
    _trim_ladder_should_display,
)
from .loaders import (
    _load_epa_ladders,
    _load_generated_ladders,
    _load_generated_ladders_raw,
    _load_json_ladders,
    _load_merged_ladders,
    _read_ladders_file,
    _steps_from_csv_rows,
)
from .epa import (
    _epa_models_match,
    _normalize_epa_trim,
)
from .csv_ladders import (
    _ladder_from_complete_options_csv,
    _ladder_from_dt_csv,
    _ladder_from_epa_csv,
)
from .inventory import (
    _inventory_trim_rows,
    _ladder_from_inventory,
)
from .selection import (
    _all_ladder_defs,
    _generic_trim_ladder_def,
    _ladder_matches_car,
    _load_dt_csv_ladders,
    _pick_brochure_ladder,
    _pick_curated_ladder,
    _pick_ladder_def,
    _year_in_range,
    resolve_trim_ladder,
)
from .plausibility import (
    _complete_options_ladder_is_junk,
    _finalize_ladder_steps,
    _ladder_steps_plausible_for_model,
    _ladder_steps_usable,
    _preserve_curated_ladder_steps,
    options_trim_cell_is_plausible,
)

__all__ = [
    "Any",
    "Path",
    "UNPROVEN_ORDER_BASIS",
    "_ADD_STOPWORDS",
    "_ADD_TOKEN_RE",
    "_BROCHURE_ADDS_TO_RE",
    "_BROCHURE_ENGINE_ITEM_RE",
    "_BROCHURE_PAGE_MARKER_RE",
    "_CITATION_KEY_RE",
    "_CLAIM_NOISE_RES",
    "_CitationRegister",
    "_GRID_MARK_RUN_RE",
    "_GRID_MATCH_COVERAGE",
    "_JUNK_TRIM_TOKENS",
    "_LADDERS_EPA_JSON",
    "_LADDERS_GENERATED_JSON",
    "_LADDERS_JSON",
    "_LADDERS_MERGED_JSON",
    "_MIN_ADD_TOKENS_FOR_GRID_MATCH",
    "_MIN_TRIM_LADDER_YEAR",
    "_NON_TRIM_RE",
    "_TRADEMARK_RE",
    "_TRIM_NOISE_RE",
    "_YEAR_MIN_SUFFIX_RE",
    "_YEAR_RANGE_SUFFIX_RE",
    "_YEAR_SUFFIX_RE",
    "_active_make_spellings",
    "_active_trims_by_make_year",
    "_add_tokens",
    "_all_ladder_defs",
    "_apply_document_order",
    "_brochure_engine_bullet_by_step",
    "_brochure_engine_step_ups",
    "_build_ladder_result",
    "_bullet_display_parts",
    "_catalog_find_co",
    "_catalog_find_epa",
    "_citation_key",
    "_clean_trim_label",
    "_clean_trim_step_display_name",
    "_complete_options_ladder_is_junk",
    "_curated_adds_for_step",
    "_dictionary_adds_by_trim",
    "_document_rung_order",
    "_drivetrain_neutral_forms",
    "_epa_engine_citations",
    "_epa_models_match",
    "_exact_inventory_rung_hits",
    "_exact_rung_match",
    "_extract_trim_from_cell",
    "_filename_token",
    "_filter_ladder_steps_for_year",
    "_finalize_ladder_steps",
    "_find_complete_options_csv",
    "_find_epa_csv",
    "_find_ladder_step",
    "_generic_trim_ladder_def",
    "_index_ladder_steps_by_trim",
    "_inventory_listing_price_fallback",
    "_inventory_rung_evidence",
    "_inventory_trim_rows",
    "_is_mechanical_trim_bullet",
    "_is_valid_trim_name",
    "_justified_ladder_steps",
    "_label_value_bullet",
    "_ladder_from_complete_options_csv",
    "_ladder_from_dt_csv",
    "_ladder_from_epa_csv",
    "_ladder_from_inventory",
    "_ladder_matches_car",
    "_ladder_steps_plausible_for_model",
    "_ladder_steps_usable",
    "_lineup_standard_features",
    "_load_dt_csv_ladders",
    "_load_epa_ladders",
    "_load_generated_ladders",
    "_load_generated_ladders_raw",
    "_load_json_ladders",
    "_load_merged_ladders",
    "_lookup_brochure_adds_key",
    "_lookup_merged_trim_specs",
    "_merge_trim_display_bullets",
    "_merge_trim_spec_values",
    "_norm_add_line",
    "_norm_make",
    "_norm_model",
    "_norm_token",
    "_normalize_epa_trim",
    "_normalize_listing_model",
    "_only_marketing_brochure_placeholders",
    "_pick_brochure_ladder",
    "_pick_curated_ladder",
    "_pick_ladder_def",
    "_preserve_curated_ladder_steps",
    "_provenance_gate_on",
    "_raw_trim_specs",
    "_read_ladders_file",
    "_register_epa_citations",
    "_register_overlay_citations",
    "_row_trim_adds",
    "_rung_claim_forms",
    "_rung_evidence_key",
    "_step_applies_to_year",
    "_step_trim_keys",
    "_steps_from_csv_rows",
    "_strip_claim_noise",
    "_strip_inventory_price_adds",
    "_suppress_non_adds",
    "_trim_identity_keys",
    "_trim_ladder_quality",
    "_trim_ladder_should_display",
    "_trim_spec_rows_to_bullets",
    "_year_in_range",
    "annotations",
    "csv",
    "iter_dt_options_paths",
    "json",
    "logger",
    "logging",
    "lru_cache",
    "normalize_trim_text",
    "options_trim_cell_is_plausible",
    "re",
    "resolve_trim_ladder",
    "trim_ladders_curated_path",
    "trim_ladders_epa_path",
    "trim_ladders_generated_path",
    "trim_ladders_merged_path",
]
