"""Extract trim-level standard equipment from OEM brochure PDFs.

Facade: the code lives in ``backend/enrichment/brochure_pdf/`` (archive, pdf_text,
matrix_parse, overlay_store) and ``provenance/ladder_stores.py``; every name is
re-exported here so importers keep resolving. A test or caller that patches a
name must patch the submodule that owns it (the functions look their globals up
there), not this facade.
"""

from __future__ import annotations

import logging

from backend.enrichment.dictionary_catalog import catalog_key  # noqa: F401
from backend.enrichment.dictionary_paths import (  # noqa: F401
    BROCHURE_FACTS_DIR,
    BROCHURE_TEXT_DIR,
    BROCHURES_DIR,
    CANONICAL_CSV_COLUMNS,
    OPTIONS_RAW_DIR,
    TRIM_ADDS_BY_YEAR_DIR,
)
from backend.enrichment.brochure_pdf.archive import *  # noqa: F401,F403
from backend.enrichment.brochure_pdf.pdf_text import *  # noqa: F401,F403
from backend.enrichment.brochure_pdf.matrix_parse import *  # noqa: F401,F403
from backend.enrichment.brochure_pdf.overlay_store import *  # noqa: F401,F403

logger = logging.getLogger(__name__)

# Provenance policy lives in provenance/ladder_stores.py; re-exported so every
# importer (and every test patch target for these names) keeps resolving here.
from backend.enrichment.provenance.ladder_stores import (  # noqa: F401
    ADMISSIBLE_LADDER_BULLET_STORES,
    ADMISSIBLE_LADDER_ORDER_STORES,
    ADMISSIBLE_LADDER_RUNG_STORES,
    ADMISSIBLE_OVERLAY_SOURCES,
    CITATION_VERIFICATION_KEY,
    CITATION_VERIFIED_KEY,
    LADDER_BULLET_STORES,
    LADDER_ORDER_STORES,
    LADDER_RUNG_STORES,
    ORDER_BEARING_OVERLAY_SOURCES,
    PER_BULLET_PROVENANCE_SOURCES,
    PROVEN_LADDER_ORDER_STORES,
    TRIM_ADDS_PROVENANCE_ENV,
    TRIM_RUNGS_PROVENANCE_ENV,
    VERIFIED_LADDER_BULLET_STORES,
    _BULLET_PRIVATE_USE_AREA,
    _OFF_VALUES,
    _ORDER_BASIS_STORES,
    _OVERLAY_SOURCE_STORES,
    _PUA_BLOCKS,
    _cited_bullet_texts,
    _order_basis_entry_is_citable,
    _overlay_year,
    admissible_overlay_adds,
    apply_overlay_provenance_gate,
    brochure_overlay_is_usable,
    bullet_text_is_readable,
    ladder_bullet_store_admissible,
    ladder_bullet_store_for_source,
    ladder_order_store_admissible,
    ladder_rung_store_admissible,
    order_basis_store,
    overlay_citable_for_year,
    overlay_rung_order,
    overlay_source_admissible,
    trim_adds_provenance_required,
    trim_rungs_provenance_required,
    verified_overlay_trim_names,
)

