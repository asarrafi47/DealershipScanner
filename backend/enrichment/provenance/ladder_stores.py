"""Provenance policy for brochure-derived trim ladders: which stores may put a
bullet, a rung or an order on a ladder, and the overlay gates built on them.

Moved verbatim out of ``backend/enrichment/brochure_extract.py`` (audit
enrich.md, P17C.3); ``brochure_extract`` re-exports every name. This module
must stay free of PDF libraries and archive I/O, and its trim_ladder imports
stay lazy (trim_ladder imports brochure_extract stores).
"""

from __future__ import annotations

import os
import re
from typing import Any


# --- provenance gate: no citation, no bullet -----------------------------
#
# The overlays under ``derived/trim_adds_by_year`` were written by several
# generations of extractor. Only some of them can say WHERE a bullet came from:
#
#   brochure_text_quoted    per-bullet {text, source file, page} in adds_provenance
#   manual_brochure_review / brochure_llm / brochure_llm_web /
#   brochure_llm_knowledge / promoted_brochure_auto
#                           no citation of any kind is recorded, per bullet or
#                           per file (see the note on ADMISSIBLE_OVERLAY_SOURCES)
#
# A shopper cannot tell those apart on the page, so the uncited ones do not
# render. ``ADMISSIBLE_OVERLAY_SOURCES`` is the list of overlay sources allowed
# through, and it is derived from ``LADDER_BULLET_STORES`` below — that table is
# the single place a later phase widens the policy, for overlays and for every
# other store that can put a bullet on a rung.

_OFF_VALUES = ("0", "false", "no", "off")

# --- every store that can put a bullet on a rung, and the rule it must satisfy ---
#
# The overlays are not the only producer. A rung's ``adds`` are assembled in
# ``trim_ladder._build_ladder_result`` from all of these:
#
#   brochure_text_quoted   overlay bullets carrying {text, source file, page}
#   brochure_trim_walk     an engine sentence BUILT around a line off a brochure
#                          "<TRIM> / Adds to <LOWER>" page (see the revocation
#                          note below — it composes, so it is uncited now)
#   epa_csv                backend/dictionary/epa/**/*_EPA.csv — addressable as
#                          file + row, but the cell we would quote is a sentence
#                          OUR importer composed (see the revocation note below)
#   manual_brochure_review /
#   brochure_llm           overlay bullets with no citation (see the note below)
#   curated_trim_ladder    hand-typed adds in curated/trim_ladders.json
#   generated_trim_ladder  curated/trim_ladders_generated.json, machine-written
#                          from the Complete_Options CSVs
#   complete_options_csv   options/raw/**/*_Complete_Options.csv
#   trim_spec_sheet        derived/trim_spec_sheets/*.json
#   oem_knowledge_prose    trim_ladder_knowledge position copy ("Mid-level trim
#                          with added convenience features…")
#   inventory_prose        price notes derived from our own active listings
#
# ``LADDER_BULLET_STORES`` is the ONE place that says which of those a shopper
# may see. Everything else — the overlay gate, the ladder-def step adds, the
# spec sheets, the CSV scrapes — reads its verdict from here, so widening the
# policy later is a single-row edit rather than a hunt through call sites.
LADDER_BULLET_STORES: dict[str, str] = {
    "brochure_text_quoted": "verified_per_bullet_citation",
    # Revoked 2026-07-31 — see the note below. Was "per_bullet_citation".
    "brochure_trim_walk": "uncited",
    # Revoked 2026-07-31 — see the two grounds below. Was "per_bullet_citation".
    "epa_csv": "uncited",
    "manual_brochure_review": "uncited",
    "brochure_llm": "uncited",
    "curated_trim_ladder": "uncited",
    "generated_trim_ladder": "uncited",
    "complete_options_csv": "uncited",
    "trim_spec_sheet": "uncited",
    "oem_knowledge_prose": "uncited",
    "inventory_prose": "uncited",
}

ADMISSIBLE_LADDER_BULLET_STORES = frozenset(
    store for store, rule in LADDER_BULLET_STORES.items() if rule != "uncited"
)

# Stores whose bullets additionally have to be signed off by
# ``backend/scripts/verify_trim_citations.py`` BEFORE a request can render them.
# The script re-opens the source PDF and checks the bullet is printed there; the
# render path then reads its verdict out of the data instead of re-deciding.
VERIFIED_LADDER_BULLET_STORES = frozenset(
    store
    for store, rule in LADDER_BULLET_STORES.items()
    if rule == "verified_per_bullet_citation"
)

# ``brochure_trim_walk`` WAS admitted on the reasoning that a brochure page
# headed "<TRIM> / Adds to <LOWER>" states the engine step-up itself. Revoked
# 2026-07-31: the store is the same defect class as ``epa_csv``, on two counts.
#
#  1. IT COMPOSES ITS BULLET. ``trim_ladder._brochure_engine_bullet_by_step``
#     builds ``f"{engine} — added over the {base_name}"``. Only ``engine`` is
#     printed in the brochure; the em dash, the words "added over the" and
#     ``base_name`` (taken from OUR ladder definition, not from the page) are
#     ours. The composed string was then handed to the citation register
#     (``citations.add_exact(engine_claim[0], …)``), so the gate was certifying
#     text no document contains — the register said yes because we had just told
#     it to.
#  2. THE TRIM IS INFERRED POSITIONALLY, NOT READ FROM A LABEL.
#     ``_brochure_engine_step_ups`` scans back up to 3 lines from the "Adds to
#     <LOWER>" marker and takes the first non-marker line as the trim name. That
#     is a guess about page layout, and it misreads: it is how a heading, a
#     footnote or a stray caption becomes the trim a claim is attributed to.
#
# Re-admitting it needs BOTH fixed: a bullet that is one printed line quoted
# whole, and a trim name read off a labelled field. It would then also have to
# pass ``verify_trim_citations.py`` like every other admitted store.

# Why ``complete_options_csv`` is blocked rather than cited like the EPA rows:
# backend/dictionary/README.md describes options/raw as "Wikipedia/NHTSA/brochure"
# and nothing in the row records WHICH of the three a given cell came from. A
# citation that cannot name the document is not a citation, and Wikipedia is not
# an admissible source here in any case. ``generated_trim_ladder`` is machine
# output of the same CSVs (its ``source`` is the CSV filename) and inherits the
# same problem, on top of re-ingesting its own prior output — the 2025 Tonale
# Premium rung reads "Upgrades Engine Options: Upgrades Engine Options: L GME T4
# turbo I4 … (was Layout Front-engine, front-wheel-drive …)".
#
# ``trim_spec_sheet`` is derived FROM the two blocked stores (see
# ``trim_spec_extractor.build_spec_sheet``), so admitting it would reintroduce
# them one level down; 1,259 of the 1,260 sheets are marked "generated" and none
# of the 1,260 carries a provenance field of any kind.
#
# ``epa_csv`` WAS admitted, on the reasoning that an EPA row is our own catalog
# data rather than invented prose and can be named as file + row. That reasoning
# was wrong on two independent grounds and the store is revoked (2026-07-31).
# Do not re-admit it on either of them:
#
#  1. THE CELL IS A SENTENCE WE COMPOSED, NOT ONE WE QUOTE.
#     ``backend/scripts/import_epa_to_dictionary.py:build_engine_desc`` BUILDS
#     the ``engineOptions`` string out of separate FuelEconomy.gov columns:
#     ``parts.append(f"{displ}L")``, ``parts.append("I%d"/"V%d" % cyl)``,
#     ``parts.append(f"({eng_dscr})")``. The 2021 Jeep Grand Cherokee Trackhawk
#     row reads "6.2L V8 (Hellcat engine)" — three of our own concatenations
#     around one quoted fragment. Citing it cites our own synthesis, which is
#     the circularity this gate exists to stop, and the V/I bank letter is a
#     guess from the cylinder count (the 2026 Ram 2500 Laramie row pairs
#     engineOptions "6.7L Cummins Turbo Diesel I6" with engineDisplay
#     "6.7L V6 Turbo").
#
#  2. THE FILE IS NOT ALWAYS THIS VEHICLE'S FILE. ``find_epa_csv`` used to fall
#     back to a sibling model's CSV. Measured 2026-07-31 over the 5,020 active
#     (year, make, model) groups that resolved to an EPA file: 1,439 groups /
#     11,121 active cars resolved to a file whose ``Model`` column is a
#     different model (2025 Chevrolet "Blazer EV" → the gas 2025 Blazer file,
#     2026 Ford "F-250SD" → ``1999_Ford_F250_EPA.csv``). That fallback is now
#     removed in ``dictionary_catalog.find_epa_csv``, but a store may not depend
#     on a resolver being right about which vehicle it opened.
#
# Cost of the revocation, measured on the live fleet the same day by resolving
# the ladder for all 14,465 active (year, make, model, trim) groups: cars showing
# at least one ladder bullet fall 20,851 → 2,118 of 72,504 active cars, because
# 18,699 of those cars had nothing but EPA-cited bullets. Silence is the correct
# output for them.
#
# Two rules that were recorded here while the store was admitted still hold for
# anyone re-reading the EPA rows for a different purpose: ``drivetrainOptions``
# is not a trim-ladder add (the row describes the one configuration it was filed
# for, so "All-Wheel Drive" as something a rung ADDS is our inference —
# ``trim_spec_extractor._extract_epa_trim_fields`` refuses it for the same
# reason), and ``engineDisplay`` is our synthesized label, not source text.
#
# --- 2026-07-31, second pass: narrow to one store and verify it at build time --
#
# Three more defects were found live with the gate ON, and all three are fixed
# above. Measured the same day by resolving the ladder for all 14,465 active
# (year, make, model, trim) groups, on 72,504 active cars, one change at a time:
#
#   stage                                       groups   cars ≥1 bullet  bullets
#   gate on, all three defects live                177          2,119      2,698
#   + brochure_trim_walk revoked                   155          2,032      2,658
#   + citations must be the car's OWN model year   130          1,969      2,340
#   + citations verified against the PDF            57          1,760      1,105
#
# so the three cost 87 / 63 / 209 cars and 40 / 318 / 1,235 bullets respectively.
# 895 active cars had been quoting a brochure from a NEIGHBOURING model year —
# 154 of them a 2026 RAV4 book read as if it described a 2024 car.
#
# The build-time run behind the last row (``verify_trim_citations.py``, which
# re-opens the PDF and re-extracts the cited page) checked all 614 provenance
# entries in the 26 admissible overlays:
#
#   verified                        322   (12 brochures we still hold)
#   source_pdf_not_held             287   (14 brochures lost off disk)
#   text_not_printed_on_cited_page    5
#
# The 5 are real: "Front tow hooks (4x only)" where the 2022 Titan XD brochure
# prints "(4x4 only)", and four cases where a footnote marker was dropped out of
# the middle of a printed line. Re-checked with a second, independent PDF reader
# (PyMuPDF rather than pdfplumber): it agrees on all 327 checkable entries.
#
# The number is meant to be small. Growing it means ACQUIRING BROCHURES — the
# 287 lost citations come back the moment their PDFs are on disk again and the
# verifier is re-run — not widening this table.

# Overlay ``source`` strings live in their own namespace; map them onto the
# stores above so the admissibility verdict still comes from one table.
_OVERLAY_SOURCE_STORES: dict[str, str] = {
    "brochure_text_quoted": "brochure_text_quoted",
    "manual_brochure_review": "manual_brochure_review",
    "brochure_llm": "brochure_llm",
    "brochure_llm_web": "brochure_llm",
    "brochure_llm_knowledge": "brochure_llm",
    "promoted_brochure_auto": "brochure_llm",
}

ADMISSIBLE_OVERLAY_SOURCES = frozenset(
    source
    for source, store in _OVERLAY_SOURCE_STORES.items()
    if store in ADMISSIBLE_LADDER_BULLET_STORES
)


def ladder_bullet_store_admissible(store: Any) -> bool:
    """True when bullets from this store may be shown, gate permitting."""
    return str(store or "").strip().lower() in ADMISSIBLE_LADDER_BULLET_STORES


def ladder_bullet_store_for_source(source: Any) -> str:
    """
    A ladder definition's ``source`` string → the store id its step ``adds`` came from.

    Ladder defs label themselves inconsistently: ``"curated"``, ``"inventory"``,
    ``"oem_knowledge"``, the literal ``"EPA CSV"``, or a bare filename
    (``"2022_Toyota_RAV4_EPA.csv"``, ``"2022_Jeep_Grand_Cherokee_Complete_Options.csv"``).
    Anything unrecognised maps to an uncited store, so a new producer is silent
    until it is added to :data:`LADDER_BULLET_STORES` on purpose.
    """
    text = str(source or "").strip().lower()
    if text == "curated":
        return "curated_trim_ladder"
    if text == "inventory":
        return "inventory_prose"
    if text == "oem_knowledge":
        return "oem_knowledge_prose"
    if text == "brochure":
        return "brochure_llm"
    if text == "epa csv" or text.endswith("_epa.csv") or text.endswith("_epa"):
        return "epa_csv"
    if "complete_options" in text:
        return "complete_options_csv"
    return "generated_trim_ladder"


# --- every store that can put a RUNG NAME on the ladder, and its rule ---------
#
# ``LADDER_BULLET_STORES`` above governs the BULLETS. It said nothing about the
# card they sit on. The rung names — which trims exist for this vehicle, and in
# what order — were asserted with no provenance at all: a 2023 Ram 1500 Limited
# listed nine rungs (TRX, Limited, Laramie Longhorn, Laramie, Rebel, Lone Star…)
# and zero bullets, and a shopper reads that list as "these are the trims of this
# vehicle". The producers were the hand-written ``curated/trim_ladders.json``,
# the machine-written ``trim_ladders_generated.json``, the Complete_Options and
# EPA CSVs, LLM brochure overlays, and — when nothing else matched — a
# hard-coded ``generic_fallback_steps`` table in ``trim_ladder_knowledge``.
#
# Same discipline as the bullets: a rung renders only if we can point at
# something that is not our own synthesis.
#
#   active_inventory        at least one ACTIVE row in our own ``cars`` table,
#                           same make + model + EXACT model year, whose ``trim``
#                           string EQUALS the rung's displayed name once both
#                           are case-folded and stripped of non-alphanumerics
#                           ("Tremor®" == "Tremor"; "Sport Utility" != "Sport").
#                           The claim this makes is narrow and checkable:
#                           "dealers are listing this trim, spelled this way,
#                           for this year of this vehicle." ``name_provenance``
#                           carries the literal spellings, so we can name the
#                           rows.
#   brochure_text_quoted    a rung of a ``brochure_text_quoted`` overlay FOR THIS
#                           MODEL YEAR that carries at least one bullet the
#                           build-time verifier confirmed is printed in the PDF
#                           (``verify_trim_citations.py``). The trim heading is
#                           the label that bullet was filed under in a document
#                           we re-opened, and it is matched to the rung name by
#                           the same exact rule.
#
# NEITHER STORE MAY BE MATCHED FUZZILY, and that is the whole point rather than
# an implementation detail. The first version of the gate scored inventory trims
# against rung names and admitted anything >= 80, which is a prefix match: the 3
# active "Sport Utility" listings of the 2026 Ford Explorer justified a rung
# named "Sport" — a string that appears on none of the 13 distinct trim values
# on active 2026 Explorer rows and under no verified 2026 citation — and the
# gate stamped it ``active_inventory``, so the provenance label asserted an
# observation that had never been made. Measured 2026-08-01 across the 14,343
# active (year, make, model, trim) groups: 3,703 groups rendered at least one
# rung whose name matched nothing we hold, 29,777 car-by-rung impressions.
# A rung with no provenance is unsupported; a fabricated rung
# carrying a provenance label is laundered, which is strictly worse. Matching is
# therefore exact equality on ``trim_ladder._rung_evidence_key``, name against
# name, aliases excluded (an alias table is our own synthesis, so resolving
# an observed "Ltd" to a printed "Limited" would be us renaming the evidence).
#
# Everything else is uncited by the same reasoning already recorded above for the
# bullets, and additionally:
#
#   epa_csv                 ``epa_master.trim`` holds body/drive descriptors
#                           ("Pickup 4WD", "RAPTOR R 4WD"), not marketing trims,
#                           and the file a vehicle resolves to has been the wrong
#                           vehicle's file (measured 2026-07-31: 1,439 groups).
#   oem_knowledge_generic   ``generic_fallback_steps`` is a hand-typed table. It
#                           is the source of the "always shows a ladder" promise
#                           in this module's docstring, which is exactly the
#                           promise that had to go.
#   brochure_neighbour_year a ±1/±2 model-year overlay. Trims change between
#                           years; a 2022 book is not evidence about a 2024 car.
LADDER_RUNG_STORES: dict[str, str] = {
    "active_inventory": "observed_active_listing",
    "brochure_text_quoted": "verified_per_bullet_citation",
    "epa_csv": "uncited",
    "curated_trim_ladder": "uncited",
    "generated_trim_ladder": "uncited",
    "complete_options_csv": "uncited",
    "brochure_llm": "uncited",
    "manual_brochure_review": "uncited",
    "brochure_neighbour_year": "uncited",
    "oem_knowledge_generic": "uncited",
    "inventory_prose": "uncited",
}

ADMISSIBLE_LADDER_RUNG_STORES = frozenset(
    store for store, rule in LADDER_RUNG_STORES.items() if rule != "uncited"
)

TRIM_RUNGS_PROVENANCE_ENV = "TRIM_RUNGS_REQUIRE_PROVENANCE"


def ladder_rung_store_admissible(store: Any) -> bool:
    """True when a rung NAME justified by this store may be listed."""
    return str(store or "").strip().lower() in ADMISSIBLE_LADDER_RUNG_STORES


# --- every store that can assert the ORDER of the rungs, and its rule --------
#
# ``LADDER_BULLET_STORES`` governs the bullets, ``LADDER_RUNG_STORES`` the rung
# names. Neither said anything about the SEQUENCE, and the ladder is drawn as a
# hierarchy — a shopper reads the top card as the better vehicle. Until
# 2026-08-02 that sequence came from ``trim_ladder_knowledge.luxury_rank``, a
# hand-maintained table, applied by ``resolve_trim_ladder`` ON TOP OF a brochure
# order that had already been derived from the document and then discarded.
# Measured live by the main thread on 2026-08-01: the 2020 Durango rendered
# Citadel above R/T above GT, and the 2026 RAV4 rendered XLE Premium above XSE.
# Both contradict the vehicles' own books.
#
#   brochure_adds_to_edge      a page of the brochure headed "<TRIM> / Adds to
#                              <LOWER>" (or "<TRIM> INCLUDES <LOWER> EQUIPMENT
#                              PLUS"). The OEM states the relation itself, in
#                              its own words, on a page we hold and can name.
#                              This is the only store that PROVES an order.
#   brochure_printed_sequence  the order the document printed the grades in —
#                              successive trim-walk pages, or the columns of one
#                              equipment grid. The sequence is the document's,
#                              but "printed earlier means lower" is a habit of
#                              the format rather than a statement, and for a
#                              grid the direction is read off mark counts by
#                              ``brochure_trim_candidates._cell_grid_direction``
#                              — our inference. CORROBORATING ONLY: it may order
#                              rungs the edges do not reach and break ties the
#                              edges leave, and it may never move a rung an edge
#                              placed.
#   luxury_rank_table          ``trim_ladder_knowledge.luxury_rank`` / the
#                              per-model ordered trim lists it reads. Hand
#                              typed, no document behind any entry. It is still
#                              the last resort when the document says nothing,
#                              and every rung it places is labelled
#                              ``unproven`` so the caller can decline to draw a
#                              hierarchy at all.
#   curated_trim_ladder        the step order in ``curated/trim_ladders.json``.
#                              Hand typed, same objection.
#   inventory                  the order rungs happened to arrive in from our
#                              own ``cars`` rows. Not evidence of anything.
LADDER_ORDER_STORES: dict[str, str] = {
    "brochure_adds_to_edge": "quoted_edge_with_page",
    "brochure_printed_sequence": "printed_sequence_in_document",
    "luxury_rank_table": "uncited",
    "curated_trim_ladder": "uncited",
    "inventory": "uncited",
}

#: Stores whose ordering may be shown as a hierarchy at all.
ADMISSIBLE_LADDER_ORDER_STORES = frozenset(
    store for store, rule in LADDER_ORDER_STORES.items() if rule != "uncited"
)

#: The subset that PROVES a position rather than corroborating one.
PROVEN_LADDER_ORDER_STORES = frozenset(
    store for store, rule in LADDER_ORDER_STORES.items() if rule == "quoted_edge_with_page"
)

#: ``brochure_trim_candidates`` basis vocabulary -> the store it belongs to.
_ORDER_BASIS_STORES: dict[str, str] = {
    "adds_to_edge": "brochure_adds_to_edge",
    "page_sequence": "brochure_printed_sequence",
    "printed_sequence": "brochure_printed_sequence",
}

#: Overlay sources that record ordering evidence at all. Every other generation
#: of overlay writer stored a bare trim list with nothing behind its sequence.
ORDER_BEARING_OVERLAY_SOURCES = frozenset({"brochure_text_quoted"})


def ladder_order_store_admissible(store: Any) -> bool:
    """True when an order attributed to this store may be drawn as a hierarchy."""
    return str(store or "").strip().lower() in ADMISSIBLE_LADDER_ORDER_STORES


def order_basis_store(basis: Any) -> str:
    """The store a ``brochure_trim_candidates`` order basis belongs to ('' if none)."""
    return _ORDER_BASIS_STORES.get(str(basis or "").strip().lower(), "")


def _order_basis_entry_is_citable(entry: Any) -> bool:
    """True when one ``order_basis`` entry names where its claim can be checked.

    Same shape of test the bullets get: an ordering claim renders only if it can
    point at a file and a page. An ``adds_to_edge`` additionally has to carry the
    two printed lines it was read off and the trim it names, so a reviewer can
    re-open that page and find the sentence.
    """
    if not isinstance(entry, dict):
        return False
    basis = str(entry.get("basis") or "").strip().lower()
    store = order_basis_store(basis)
    if not store or not ladder_order_store_admissible(store):
        return False
    if basis == "adds_to_edge":
        edge = entry.get("edge")
        if not isinstance(edge, dict):
            return False
        try:
            page = int(edge.get("page") or 0)
        except (TypeError, ValueError):
            return False
        return bool(
            page > 0
            and str(edge.get("source") or "").strip()
            and str(edge.get("trim") or "").strip()
            and str(edge.get("below") or "").strip()
            and str(edge.get("trim_quote") or "").strip()
            and str(edge.get("below_quote") or "").strip()
        )
    if basis == "page_sequence":
        try:
            return int(entry.get("page") or 0) > 0
        except (TypeError, ValueError):
            return False
    if basis == "printed_sequence":
        pages = entry.get("pages")
        return bool(
            str(entry.get("source") or "").strip()
            and isinstance(pages, list)
            and any(isinstance(p, int) and p > 0 for p in pages)
        )
    return False


def overlay_rung_order(
    overlay: dict[str, Any] | None,
    *,
    for_year: Any = None,
) -> tuple[list[str], dict[str, dict[str, Any]]]:
    """The base→top rung order this overlay can account for, and why.

    Returns ``([], {})`` unless every one of these holds:

    * the overlay's source is one that records ordering evidence;
    * ``for_year`` is given and the overlay IS that model year's book — a
      neighbouring year's brochure is not evidence about this car's lineup, the
      same rule ``overlay_citable_for_year`` applies to the bullets;
    * at least two rungs carry an ``order_basis`` entry that names a file and a
      page.

    Rungs whose basis cannot be checked are dropped from the returned order
    rather than carried along unexplained.
    """
    if not isinstance(overlay, dict):
        return [], {}
    if str(overlay.get("source") or "").strip().lower() not in ORDER_BEARING_OVERLAY_SOURCES:
        return [], {}
    if not overlay_citable_for_year(overlay, for_year):
        return [], {}
    basis_raw = overlay.get("order_basis")
    if not isinstance(basis_raw, dict):
        return [], {}
    order = [
        str(t).strip()
        for t in (overlay.get("rung_order") or overlay.get("trims_available") or [])
        if str(t).strip()
    ]
    kept: list[str] = []
    basis: dict[str, dict[str, Any]] = {}
    for trim in order:
        entry = basis_raw.get(trim)
        if not _order_basis_entry_is_citable(entry):
            continue
        kept.append(trim)
        basis[trim] = {
            **entry,
            "store": order_basis_store(entry.get("basis")),
            "proven": order_basis_store(entry.get("basis")) in PROVEN_LADDER_ORDER_STORES,
        }
    if len(kept) < 2:
        return [], {}
    return kept, basis


def trim_rungs_provenance_required() -> bool:
    """
    Kill switch for the rung-name gate (see ``trim_ladder._justified_ladder_steps``).

    Default ON. ``TRIM_RUNGS_REQUIRE_PROVENANCE=0`` restores the previous
    behaviour of listing whatever rungs a ladder definition happened to carry.
    Read per call, not cached, so it can be flipped without a restart.
    """
    return (
        os.environ.get(TRIM_RUNGS_PROVENANCE_ENV) or "1"
    ).strip().lower() not in _OFF_VALUES


# ``manual_brochure_review`` is deliberately NOT in that set, though its name
# claims a human read the brochure. Measured 2026-07-31 by looking every overlay
# bullet up in the brochure text we still hold for that same catalog key
# (punctuation/case/whitespace-insensitive substring):
#
#   brochure_text_quoted     266 / 270    verbatim  (98.5%; the 4 misses are a ™
#                                         ligature and a mid-phrase line wrap)
#   brochure_llm          19,714 / 57,442 verbatim  (34.3%)
#   manual_brochure_review   100 / 2,018  verbatim  (5.0%)
#
# The 2011 Jeep Grand Cherokee Laredo rung that reported "3.6L Pentastar V6
# engine with 360 horsepower" — the 2011 Pentastar makes 290 (our own
# epa_extended_specs says 293); 360 is the 5.7 HEMI V8 — is a
# manual_brochure_review file. The label records that someone looked at the file,
# not that the bullets are the brochure's words, and none of the 125 such files
# carries a citation of any kind. Re-admit the source by flipping its row in
# ``LADDER_BULLET_STORES`` only once those files quote something.

# Sources whose bullets are checked one at a time: a bullet from one of these
# files renders only if adds_provenance names a file AND a page for that exact
# bullet text. An admissible source that is NOT listed here would be admitted
# whole-file; today every admissible source is per-bullet, so the two sets match.
PER_BULLET_PROVENANCE_SOURCES = frozenset({"brochure_text_quoted"})

TRIM_ADDS_PROVENANCE_ENV = "TRIM_ADDS_REQUIRE_PROVENANCE"


def trim_adds_provenance_required() -> bool:
    """
    Kill switch for the overlay provenance gate (see :func:`admissible_overlay_adds`).

    Default ON. Set ``TRIM_ADDS_REQUIRE_PROVENANCE=0`` to restore the previous
    behaviour of rendering every overlay bullet regardless of citation.
    """
    return (
        os.environ.get(TRIM_ADDS_PROVENANCE_ENV) or "1"
    ).strip().lower() not in _OFF_VALUES


# --- the pre-verified flag the render path trusts -------------------------
#
# ``backend/scripts/verify_trim_citations.py`` re-opens the source PDF named by
# each provenance entry, re-extracts the cited page, and stamps the entry with
# ``verified: true`` only when the bullet text is printed on that page. It is a
# build step; nothing at render time re-derives that verdict.
#
# FAIL CLOSED, TWICE OVER: an entry with no ``verified`` key at all (an overlay
# written before the verifier existed, or one the verifier could not check
# because we no longer hold the PDF) is NOT renderable. Only the literal value
# ``True`` passes. That is why the key is read with ``is True`` rather than
# truth-tested — a string "false" is truthy.
CITATION_VERIFIED_KEY = "verified"

#: Where ``verify_trim_citations.py`` records the run that produced those flags.
CITATION_VERIFICATION_KEY = "citation_verification"


#: The three Unicode Private Use Area blocks, duplicated from
#: ``brochure_sources._PRIVATE_USE_AREA`` because that module imports from this
#: one and the dependency may not run the other way.
#:
#: A codepoint in these blocks means whatever the PDF's own embedded font says
#: it means and nothing outside that font, so a bullet holding one is text we
#: cannot read being shown as if we could. ``brochure_sources`` refuses a whole
#: DOCUMENT when a character CLASS was remapped
#: (``MAX_PRIVATE_USE_CODEPOINTS``); this refuses a single BULLET for any PUA
#: character at all, which is the residue that gate deliberately leaves behind.
#:
#: WHY BOTH. Measured 2026-08-02 over ``derived/brochure_text``, before the
#: document gate swept anything: 122 live files held at least one PUA character,
#: and only 12 of them had had a class remapped. The other 110 are sound
#: documents with a dingbat or a punctuation glyph in a subsetted font, and are
#: still live — ``2012__cadillac__cts`` prints
#: ``DIRECT<U+F6BA>INJECTION``, ``2012__jaguar__xf`` prints ``18<U+E044>`` for an
#: 18-inch wheel, ``2020__buick__encoregx`` glues a tick mark onto the next
#: token as ``<U+F0A2>20``. Refusing those documents would throw away 110 books
#: over a hyphen. Refusing the individual bullet costs only the bullet, and a
#: bullet is exactly the unit that reaches a shopper.
#:
#: MEASURED COST at the time of writing: **zero renderable bullets**. None of
#: the 26 live ``brochure_text_quoted`` overlays carries a PUA character in any
#: bullet text. Four live overlays do — ``2013__bmw__5series``,
#: ``2015__bmw__7series`` (``promoted_brochure_auto``), ``2014__bmw__5series``
#: (``brochure_llm``) — and all four are already refused a step earlier by
#: ``LADDER_BULLET_STORES``. This check is what stops that text if any of those
#: stores is ever re-admitted, and what stops it arriving from the corpus-wide
#: extraction run this store is about to be filled by.
#: Built from ordinals rather than written as a literal character class. A raw
#: ``[-...]`` in source is invisible to a reviewer and survives
#: copy/paste badly — the first draft of this constant lost its BMP block in
#: transit and silently matched nothing but the two supplementary planes, which
#: is a gate that passes everything. ``test_bullet_pua_pattern_covers_bmp_block``
#: pins it.
_PUA_BLOCKS: tuple[tuple[int, int], ...] = (
    (0xE000, 0xF8FF),      # BMP Private Use Area — where subsetted fonts land
    (0xF0000, 0xFFFFD),    # Supplementary Private Use Area-A
    (0x100000, 0x10FFFD),  # Supplementary Private Use Area-B
)
_BULLET_PRIVATE_USE_AREA = re.compile(
    "[" + "".join(f"{chr(lo)}-{chr(hi)}" for lo, hi in _PUA_BLOCKS) + "]"
)


def bullet_text_is_readable(text: Any) -> bool:
    """
    False when a bullet holds a character we cannot actually read.

    Today that means a Unicode Private Use Area codepoint (see
    :data:`_BULLET_PRIVATE_USE_AREA`) or a U+FFFD replacement character — both
    mean a font's glyph did not resolve to a character, so the string is not the
    document's words even though it was copied out of the document.

    Fails closed: a bullet that cannot be read is not shown, and the rung simply
    has one less thing to say.
    """
    string = str(text or "")
    if "�" in string:
        return False
    return not _BULLET_PRIVATE_USE_AREA.search(string)


def _cited_bullet_texts(entries: Any, *, require_verified: bool) -> set[str]:
    """
    Bullet texts in one trim's ``adds_provenance`` that this overlay can account for.

    A text counts only when its entry names a document AND a page inside it, and
    — for stores in :data:`VERIFIED_LADDER_BULLET_STORES` — only when the
    build-time verifier has confirmed the text is printed there.
    """
    out: set[str] = set()
    if not isinstance(entries, list):
        return out
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        text = str(entry.get("text") or "").strip()
        source = str(entry.get("source") or "").strip()
        page = entry.get("page")
        if not text or not source or page is None:
            continue
        if not bullet_text_is_readable(text):
            # The citation may be perfectly sound — the text really is printed
            # on that page — but what we extracted from it is not the words. See
            # :data:`_BULLET_PRIVATE_USE_AREA`.
            continue
        if require_verified and entry.get(CITATION_VERIFIED_KEY) is not True:
            continue
        out.add(text)
    return out


def overlay_source_admissible(source: Any) -> bool:
    """True when this overlay's ``source`` may put bullets in front of a shopper."""
    return str(source or "").strip().lower() in ADMISSIBLE_OVERLAY_SOURCES


def _overlay_year(overlay: dict[str, Any] | None) -> int | None:
    try:
        return int((overlay or {}).get("year"))
    except (TypeError, ValueError):
        return None


def overlay_citable_for_year(overlay: dict[str, Any] | None, year: Any) -> bool:
    """
    True when this overlay is evidence about ``year`` — i.e. it IS that year's book.

    ``load_brochure_trim_overlay`` still reaches ±2 model years to learn which
    rungs a lineup had, because a trim list moves slowly. A CITATION may not: a
    2019 brochure does not say what a 2022 car has, and equipment moves between
    trims every refresh. Measured 2026-07-31 over the 26 ``brochure_text_quoted``
    overlays, the ±2 reach was answering for years those books never covered.
    Asked with ``year=None`` this returns False, so a caller that cannot say
    which car it is rendering gets no citations rather than the benefit of the
    doubt.
    """
    y = _overlay_year(overlay)
    if y is None:
        return False
    try:
        want = int(year)
    except (TypeError, ValueError):
        return False
    return y == want


def admissible_overlay_adds(
    overlay: dict[str, Any],
    *,
    for_year: Any = None,
) -> dict[str, list[str]]:
    """
    ``adds_by_trim`` reduced to the bullets this overlay can account for.

    Uncited source → ``{}``. Per-bullet-citation source → only the bullets whose
    exact text appears in ``adds_provenance`` with a file and a page, and (for a
    ``verified_per_bullet_citation`` store) only those the build-time verifier
    confirmed against the document. Idempotent, so it is safe to re-apply
    anywhere an overlay is consumed. With the kill switch off it returns
    ``adds_by_trim`` untouched.

    ``for_year`` is the model year of the car being rendered. When it is given,
    an overlay for a different year keeps its ``trims_available`` but contributes
    no bullets (see :func:`overlay_citable_for_year`). It defaults to None —
    meaning "not being rendered against a particular car", as when a script
    audits an overlay file — and in that case the year check does not apply;
    :func:`load_brochure_trim_overlay` always passes the real year.
    """
    raw = (overlay or {}).get("adds_by_trim") or {}
    if not isinstance(raw, dict):
        return {}
    # ``bullet_text_is_readable`` is applied HERE, before the provenance gate,
    # rather than only inside ``_cited_bullet_texts``. Two returns below leave
    # this function without consulting a citation at all — the kill switch, and
    # an admissible source that is not per-bullet — and an unreadable bullet is
    # wrong on every one of those paths, not only on the cited one.
    normalized = {
        str(k).strip(): [
            str(a).strip()
            for a in v
            if str(a).strip() and bullet_text_is_readable(a)
        ]
        for k, v in raw.items()
        if isinstance(v, list)
    }
    if not trim_adds_provenance_required():
        return normalized
    source = str((overlay or {}).get("source") or "").strip().lower()
    if not overlay_source_admissible(source):
        return {}
    if for_year is not None and not overlay_citable_for_year(overlay, for_year):
        return {}
    if source not in PER_BULLET_PROVENANCE_SOURCES:
        return normalized
    provenance = (overlay or {}).get("adds_provenance") or {}
    if not isinstance(provenance, dict):
        return {}
    require_verified = _OVERLAY_SOURCE_STORES.get(source) in VERIFIED_LADDER_BULLET_STORES
    out: dict[str, list[str]] = {}
    for trim, bullets in normalized.items():
        cited = _cited_bullet_texts(
            provenance.get(trim), require_verified=require_verified
        )
        kept = [b for b in bullets if b in cited]
        if kept:
            out[trim] = kept
    return out


def verified_overlay_trim_names(
    overlay: dict[str, Any] | None,
    *,
    for_year: Any,
) -> list[str]:
    """
    Trim names this overlay can justify LISTING as rungs — not merely name.

    A name qualifies only when the overlay is this car's own model year, its
    source is admissible for rung names, and the trim carries at least one
    ``adds_provenance`` entry the build-time verifier stamped ``verified: true``
    — i.e. we re-opened that PDF, re-extracted that page, and found the bullet
    printed on it under this trim.

    ``trims_available`` on its own is NOT enough and is deliberately ignored
    here: on an LLM-written overlay it is a list nobody checked, and even on a
    ``brochure_text_quoted`` overlay it is the extractor's reading of the lineup
    rather than a location in the document. Returns [] whenever any of that is
    missing, so the failure mode is a shorter ladder.
    """
    if not isinstance(overlay, dict):
        return []
    if not overlay_citable_for_year(overlay, for_year):
        return []
    source = str(overlay.get("source") or "").strip().lower()
    store = _OVERLAY_SOURCE_STORES.get(source)
    if not store or not ladder_rung_store_admissible(store):
        return []
    provenance = overlay.get("adds_provenance") or {}
    if not isinstance(provenance, dict):
        return []
    require_verified = store in VERIFIED_LADDER_BULLET_STORES
    out: list[str] = []
    for trim, entries in provenance.items():
        name = str(trim or "").strip()
        if not name:
            continue
        if _cited_bullet_texts(entries, require_verified=require_verified):
            out.append(name)
    return out


def apply_overlay_provenance_gate(
    overlay: dict[str, Any] | None,
    *,
    for_year: Any = None,
) -> dict[str, Any] | None:
    """
    Copy of ``overlay`` whose ``adds_by_trim`` holds only admissible bullets.

    The overlay object itself is kept even when every bullet is dropped: its
    ``trims_available`` still tells the ladder which rungs this model year had,
    and its presence is what stops the ladder substituting Complete_Options CSV
    lines or generated position copy for the bullets just removed. A rung with
    nothing left to say renders no bullets at all.
    """
    if not isinstance(overlay, dict):
        return overlay
    return {
        **overlay,
        "adds_by_trim": admissible_overlay_adds(overlay, for_year=for_year),
    }


def brochure_overlay_is_usable(data: dict[str, Any]) -> bool:
    """Reject auto-promoted overlays with junk trim tokens or brochure footer lines."""
    if not isinstance(data, dict):
        return False
    make = str(data.get("make") or "")
    model = str(data.get("model") or "")
    src = str(data.get("source") or "").lower()
    if "manual" in src or "curated" in src:
        return bool(data.get("adds_by_trim"))

    if src == "brochure_text_quoted":
        # Every bullet in this overlay is quoted from a named page of a named
        # brochure and carries that citation in ``adds_provenance``. The overlay
        # deliberately lists the whole lineup while claiming adds only for the
        # rungs the brochure's layout could be read for, so the "adds on half
        # the listed trims" rule below does not apply — two cited rungs is a
        # trim walk.
        quoted = data.get("trims_quoted") or [
            t for t, v in (data.get("adds_by_trim") or {}).items() if v
        ]
        provenance = data.get("adds_provenance") or {}
        cited = [
            t
            for t in quoted
            if (data.get("adds_by_trim") or {}).get(t) and provenance.get(t)
        ]
        return len(cited) >= 2

    from backend.enrichment.trim_ladder import _is_valid_trim_name
    from backend.enrichment.trim_ladder_knowledge import is_generic_trim_add

    trims = list(data.get("trims_available") or [])
    adds = data.get("adds_by_trim") or {}
    if not isinstance(adds, dict):
        return False
    valid_trims = [
        str(t).strip()
        for t in trims
        if str(t).strip() and _is_valid_trim_name(str(t).strip(), make=make, model=model)
    ]
    if len(valid_trims) < 2:
        return False

    substantive_trims = 0
    for trim_name, bullets in adds.items():
        if not _is_valid_trim_name(str(trim_name).strip(), make=make, model=model):
            continue
        if any(
            str(b).strip()
            and not is_generic_trim_add(str(b), str(trim_name))
            for b in (bullets or [])
        ):
            substantive_trims += 1
    return substantive_trims >= max(1, len(valid_trims) // 2)
