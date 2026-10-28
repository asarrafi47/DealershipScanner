"""
Surface golden for ``backend/enrichment/brochure_extract.py``.

Written before the module is split (audit enrich.md, P17C.3/P17C.4) and kept
passing across the moves: every public and underscore name, each function's
signature, each class's members and each module constant's value are what the
25 importers and the tests patch. A name that moves out must be re-exported
from ``brochure_extract`` unchanged. Also pins that the provenance policy module
loads without pdfplumber.

Offline: nothing here reads a PDF, a database or the network.
"""
from __future__ import annotations

import inspect
import re
import subprocess
import sys
from pathlib import Path

import pytest

import backend.enrichment.brochure_extract as be

REPO = Path(__file__).resolve().parents[2]

FUNCTION_SIGNATURES = {'_archive_make_dir': "(ymm: 'BrochureYMM | None') -> 'str'",
 '_brochure_text_file': "(catalog_key_value: 'str') -> 'Path'",
 '_brochure_text_path': "(ymm: 'BrochureYMM') -> 'Path'",
 '_canonical_trim_name': "(raw: 'str', *, make: 'str', model: 'str') -> 'str'",
 '_capture_status': "(result: 'BrochureTextResult') -> 'str'",
 '_cited_bullet_texts': "(entries: 'Any', *, require_verified: 'bool') -> 'set[str]'",
 '_compute_adds_over_lower': "(ladder_order: 'list[str]', standard_by_trim: 'dict[str, "
                             "list[str]]') -> 'dict[str, list[str]]'",
 '_detect_trim_columns': "(table: 'list[list[Any]]') -> 'list[tuple[int, str]]'",
 '_extract_equipment_matrix': "(path: 'Path', ymm: 'BrochureYMM', *, ladder_order: 'list[str] | "
                              "None') -> 'tuple[dict[str, list[str]], list[int], list[str]]'",
 '_extract_pdf_text': "(path: 'Path') -> 'tuple[str, list[int]]'",
 '_facts_path': "(ymm: 'BrochureYMM') -> 'Path'",
 '_feature_column_index': "(row: 'list[Any]', trim_cols: 'list[tuple[int, str]]') -> 'int'",
 '_feature_key': "(feature: 'str') -> 'str'",
 '_find_trim_sections': "(text: 'str', *, allowed_trims: 'set[str] | None' = None, make: 'str' = "
                        "'', model: 'str' = '') -> 'list[tuple[int, str, str]]'",
 '_is_interleaved_standard_header': "(text: 'str', start: 'int', end: 'int') -> 'bool'",
 '_ladder_trim_order': "(ymm: 'BrochureYMM') -> 'tuple[str | None, list[str]]'",
 '_looks_like_trim_column': "(cell: 'str') -> 'bool'",
 '_map_extracted_to_ladder': "(extracted: 'dict[str, list[str]]', ladder_order: 'list[str]', *, "
                             "make: 'str', model: 'str') -> 'dict[str, list[str]]'",
 '_marker_is_standard': "(marker: 'str') -> 'bool'",
 '_normalize_feature': "(raw: 'str') -> 'str'",
 '_order_basis_entry_is_citable': "(entry: 'Any') -> 'bool'",
 '_overlay_path': "(ymm: 'BrochureYMM') -> 'Path'",
 '_overlay_year': "(overlay: 'dict[str, Any] | None') -> 'int | None'",
 '_page_layout_lines': "(page: 'Any') -> 'list[dict[str, Any]]'",
 '_page_table_lines': "(page: 'Any') -> 'list[str]'",
 '_parse_matrix_table': "(table: 'list[list[Any]]', *, make: 'str', model: 'str', acc: 'dict[str, "
                        "list[str]]') -> 'None'",
 '_raw_trim_to_canonical': "(raw: 'str') -> 'str'",
 '_read_brochure_overlay_file': "(path: 'Path') -> 'dict[str, Any] | None'",
 '_rung_order_from_brochure_text': "(catalog_key_value: 'str', make: 'str', model: 'str', year: "
                                   "'int') -> 'tuple[tuple[str, ...], str]'",
 '_split_features': "(block: 'str') -> 'list[str]'",
 '_write_archive_meta': "(target: 'Path', origin: 'Path', ymm: 'BrochureYMM | None', digest: "
                        "'str', size: 'int', source_url: 'str') -> 'None'",
 '_write_marketing_trim_csv_rows': "(result: 'BrochureExtractResult') -> 'None'",
 'admissible_overlay_adds': "(overlay: 'dict[str, Any]', *, for_year: 'Any' = None) -> 'dict[str, "
                            "list[str]]'",
 'apply_overlay_provenance_gate': "(overlay: 'dict[str, Any] | None', *, for_year: 'Any' = None) "
                                  "-> 'dict[str, Any] | None'",
 'archive_source_pdf': "(path: 'Path', *, ymm: 'BrochureYMM | None' = None, source_url: 'str' = "
                       "'', dry_run: 'bool' = False) -> 'Path | None'",
 'archived_pdf_path': "(path: 'Path', *, ymm: 'BrochureYMM | None' = None) -> 'Path'",
 'attach_rung_order': "(overlay: 'dict[str, Any]') -> 'dict[str, Any]'",
 'backfill_overlay_rung_order': "(*, dry_run: 'bool' = False) -> 'dict[str, int]'",
 'brochure_archive_dir': "() -> 'Path'",
 'brochure_overlay_is_usable': "(data: 'dict[str, Any]') -> 'bool'",
 'brochure_text_payload': "(result: 'BrochureTextResult') -> 'dict[str, Any]'",
 'brochure_text_rejection': "(result: 'BrochureTextResult') -> 'str'",
 'bullet_text_is_readable': "(text: 'Any') -> 'bool'",
 'dumps_brochure_text': "(payload: 'dict[str, Any]') -> 'str'",
 'extract_brochure_pdf': "(path: 'Path') -> 'BrochureExtractResult | None'",
 'extract_brochure_text_pdf': "(path: 'Path', *, all_pages: 'bool' = True, capture_layout: 'bool' "
                              "= True) -> 'BrochureTextResult | None'",
 'iter_brochure_pdfs': "(directory: 'Path | None' = None) -> 'list[Path]'",
 'ladder_bullet_store_admissible': "(store: 'Any') -> 'bool'",
 'ladder_bullet_store_for_source': "(source: 'Any') -> 'str'",
 'ladder_order_store_admissible': "(store: 'Any') -> 'bool'",
 'ladder_rung_store_admissible': "(store: 'Any') -> 'bool'",
 'load_brochure_trim_overlay': "(year: 'Any', make: 'str', model: 'str') -> 'dict[str, Any] | "
                               "None'",
 'order_basis_store': "(basis: 'Any') -> 'str'",
 'overlay_citable_for_year': "(overlay: 'dict[str, Any] | None', year: 'Any') -> 'bool'",
 'overlay_rung_order': "(overlay: 'dict[str, Any] | None', *, for_year: 'Any' = None) -> "
                       "'tuple[list[str], dict[str, dict[str, Any]]]'",
 'overlay_source_admissible': "(source: 'Any') -> 'bool'",
 'parse_brochure_filename': "(path: 'Path') -> 'BrochureYMM | None'",
 'persist_brochure_extract': "(result: 'BrochureExtractResult') -> 'Path | None'",
 'persist_brochure_text': "(result: 'BrochureTextResult') -> 'Path'",
 'process_brochure_file': "(path: 'Path', *, delete_after: 'bool' = False, dry_run: 'bool' = "
                          "False) -> 'BrochureExtractResult | None'",
 'sha256_file': "(path: 'Path', *, chunk: 'int' = 1048576) -> 'str'",
 'trim_adds_provenance_required': "() -> 'bool'",
 'trim_rungs_provenance_required': "() -> 'bool'",
 'verified_overlay_trim_names': "(overlay: 'dict[str, Any] | None', *, for_year: 'Any') -> "
                                "'list[str]'"}

#: names wrapped in functools.lru_cache (the cache moves with its function)
LRU_CACHED = ['_rung_order_from_brochure_text']

CLASS_MEMBERS = {'BrochureExtractResult': ['ladder_id', 'source_pdf'],
 'BrochurePageText': ['has_text_layer', 'height', 'trim_hint', 'width'],
 'BrochureTextResult': ['all_pages',
                        'combined_trim_pages_text',
                        'layout_captured',
                        'page_count',
                        'source_pdf_bytes',
                        'source_pdf_sha256'],
 'BrochureYMM': ['catalog_key']}

CONSTANTS = {'ADMISSIBLE_LADDER_BULLET_STORES': ('set', ["'brochure_text_quoted'"]),
 'ADMISSIBLE_LADDER_ORDER_STORES': ('set',
                                    ["'brochure_adds_to_edge'", "'brochure_printed_sequence'"]),
 'ADMISSIBLE_LADDER_RUNG_STORES': ('set', ["'active_inventory'", "'brochure_text_quoted'"]),
 'ADMISSIBLE_OVERLAY_SOURCES': ('set', ["'brochure_text_quoted'"]),
 'BROCHURES_DIR': ('path', 'backend/data/brochures'),
 'BROCHURE_ARCHIVE_ENV': 'BROCHURE_ARCHIVE_DIR',
 'BROCHURE_FACTS_DIR': ('path', 'backend/dictionary/derived/brochure_facts'),
 'BROCHURE_TEXT_DIR': ('path', 'backend/dictionary/derived/brochure_text'),
 'BROCHURE_TEXT_SCHEMA_VERSION': 2,
 'CANONICAL_CSV_COLUMNS': ('Year',
                           'Make',
                           'Model',
                           'Trim',
                           'engineOptions',
                           'engineDisplay',
                           'forcedInduction',
                           'transmissionOptions',
                           'drivetrainOptions',
                           'fuelType',
                           'bodyStyle',
                           'cylinders',
                           'displacement',
                           'mpg_city',
                           'mpg_highway',
                           'mpg_combined',
                           'exteriorColors',
                           'Packages',
                           'packageDetails',
                           'Options',
                           'optionDetails'),
 'CITATION_VERIFICATION_KEY': 'citation_verification',
 'CITATION_VERIFIED_KEY': 'verified',
 'DEFAULT_BROCHURE_ARCHIVE_DIR': ('path', 'backend/data/brochure_archive'),
 'LADDER_BULLET_STORES': {'brochure_llm': 'uncited',
                          'brochure_text_quoted': 'verified_per_bullet_citation',
                          'brochure_trim_walk': 'uncited',
                          'complete_options_csv': 'uncited',
                          'curated_trim_ladder': 'uncited',
                          'epa_csv': 'uncited',
                          'generated_trim_ladder': 'uncited',
                          'inventory_prose': 'uncited',
                          'manual_brochure_review': 'uncited',
                          'oem_knowledge_prose': 'uncited',
                          'trim_spec_sheet': 'uncited'},
 'LADDER_ORDER_STORES': {'brochure_adds_to_edge': 'quoted_edge_with_page',
                         'brochure_printed_sequence': 'printed_sequence_in_document',
                         'curated_trim_ladder': 'uncited',
                         'inventory': 'uncited',
                         'luxury_rank_table': 'uncited'},
 'LADDER_RUNG_STORES': {'active_inventory': 'observed_active_listing',
                        'brochure_llm': 'uncited',
                        'brochure_neighbour_year': 'uncited',
                        'brochure_text_quoted': 'verified_per_bullet_citation',
                        'complete_options_csv': 'uncited',
                        'curated_trim_ladder': 'uncited',
                        'epa_csv': 'uncited',
                        'generated_trim_ladder': 'uncited',
                        'inventory_prose': 'uncited',
                        'manual_brochure_review': 'uncited',
                        'oem_knowledge_generic': 'uncited'},
 'OPTIONS_RAW_DIR': ('path', 'backend/dictionary/options/raw'),
 'ORDER_BEARING_OVERLAY_SOURCES': ('set', ["'brochure_text_quoted'"]),
 'PER_BULLET_PROVENANCE_SOURCES': ('set', ["'brochure_text_quoted'"]),
 'PROVEN_LADDER_ORDER_STORES': ('set', ["'brochure_adds_to_edge'"]),
 'TRIM_ADDS_BY_YEAR_DIR': ('path', 'backend/dictionary/derived/trim_adds_by_year'),
 'TRIM_ADDS_PROVENANCE_ENV': 'TRIM_ADDS_REQUIRE_PROVENANCE',
 'TRIM_RUNGS_PROVENANCE_ENV': 'TRIM_RUNGS_REQUIRE_PROVENANCE',
 'VERIFIED_LADDER_BULLET_STORES': ('set', ["'brochure_text_quoted'"]),
 '_ARCHIVE_GITIGNORE': '# Durable OEM brochure archive: source documents, never deleted, never '
                       'committed.\n'
                       '*\n',
 '_BULLET_PRIVATE_USE_AREA': ('re',
                              '[\ue000-\uf8ff\U000f0000-\U000ffffd\U00100000-\U0010fffd]',
                              32),
 '_BULLET_SPLIT': ('re', '\\s+[–—]\\s+', 32),
 '_BUYERS_GUIDE_TRIM_START': ('re',
                              '(?P<raw>G\\s*R\\s*A\\s*N\\s*D\\s*C\\s*H\\s*E\\s*R\\s*O\\s*K\\s*E\\s*E\\s*S\\s*U\\s*M\\s*M\\s*I\\s*T|H\\s*I\\s*G\\s*H\\s*A\\s*L\\s*T\\s*I\\s*T\\s*U\\s*D\\s*E|O\\s*V\\s*E\\s*R\\s*L\\s*A\\s*N\\s*D|T\\s*R\\s*A\\s*I\\s*L\\s*H\\s*A\\s*W\\s*K|L\\s*I\\s*M\\s*I\\s*T\\s*E\\s*D\\s*X|L\\s*I\\s*M\\s*I\\s*T\\s*E\\s*D|L\\s*A\\s*R\\s*E\\s*D\\s*O|T\\s*R\\s*A\\s*C\\s*K\\s*H\\s*A\\s*W\\s*K|S\\s*R\\s*T\\s*8|S\\s*R\\s*T)\\s*®?\\s*STANDARD\\s*[-–]\\s*',
                              34),
 '_EQUIPMENT_PAGE': ('re', 'Standard\\s+equipment|Specifications|Features\\s+\\w+\\s+\\w+', 34),
 '_FILENAME_YMM': ('re', '^(\\d{4})_([^_]+)_(.+?)(?:_Brochure)?$', 34),
 '_GENERIC_TRIM_STANDARD': ('re',
                            "(?P<raw>[A-Z][A-Za-z0-9®™'\\-/]{1,28}(?:\\s+[A-Z][A-Za-z0-9®™'\\-/]{1,28}){0,3})\\s+STANDARD\\s*[-–]\\s*",
                            32),
 '_LAYOUT_GAP_CHAR_MULTIPLE': 1.2,
 '_LAYOUT_MIN_CELL_GAP': 4.0,
 '_LAYOUT_ROW_TOLERANCE': 2.5,
 '_NOISE_LINE': ('re',
                 '^(?:J\\s*E\\s*E\\s*P|BUYER.?S\\s*GUIDE|©20|WARRANTY|facebook\\.com|Actual '
                 'mileage|GO MOBILE|JEEP SOCIAL|disclaimer)',
                 34),
 '_OFF_VALUES': ('0', 'false', 'no', 'off'),
 '_OPTIONAL_GROUP': ('re', '\\b(?:GROUP\\s+[IVX\\d]+|AVAILABLE\\s+[A-Z]|\\bOPTIONAL\\b)\\s*:', 34),
 '_ORDER_BASIS_STORES': {'adds_to_edge': 'brochure_adds_to_edge',
                         'page_sequence': 'brochure_printed_sequence',
                         'printed_sequence': 'brochure_printed_sequence'},
 '_OVERLAY_SOURCE_STORES': {'brochure_llm': 'brochure_llm',
                            'brochure_llm_knowledge': 'brochure_llm',
                            'brochure_llm_web': 'brochure_llm',
                            'brochure_text_quoted': 'brochure_text_quoted',
                            'manual_brochure_review': 'manual_brochure_review',
                            'promoted_brochure_auto': 'brochure_llm'},
 '_PUA_BLOCKS': ((57344, 63743), (983040, 1048573), (1048576, 1114109)),
 '_SECTION_HEADER_ROW': ('re',
                         '^(?:performance|handling|audio|interior|comfort|chassis|technical|fuel|weight|tires|brakes|bmw '
                         'services|stand-alone)\\b',
                         34),
 '_STANDARD_MARKERS': ('set', ["'p'", "'s'"]),
 '_TRIM_COLUMN_HEADER': ('re', '^[A-Za-z0-9][A-Za-z0-9®™\\.\\-\\s]{0,28}$', 32),
 '_TRIM_PAGE_HINT': ('re',
                     'BUYER.?S\\s*GUIDE|STANDARD\\s*[-–]|CHOOSE\\s+YOUR\\s+TRIM|CHOOSE\\s+YOUR\\s+PACKAGE|ADDS\\s+TO|ADDS\\s+FROM|INCLUDES\\s+\\w+\\s+EQUIPMENT\\s+PLUS|Trim\\s+Levels?|>\\s*0\\d\\s+TRIMS|06\\s+TRIMS|FEATURES\\s*&\\s*TRIMS|Features\\s+&\\s+Trims|STYLES|TRIM\\s+LEVEL|MODELS\\s+TO\\s+CHOOSE|Four\\s+models\\s+to\\s+choose|NINE\\s+MODELS\\s+TO\\s+CHOOSE|Standard\\s+equipment|Specifications',
                     34),
 '_TRIM_RAW_TO_CANONICAL': {'grandcherokeesummit': 'Summit',
                            'highaltitude': 'High Altitude',
                            'laredo': 'Laredo',
                            'limited': 'Limited',
                            'limitedx': 'Limited X',
                            'overland': 'Overland',
                            'srt': 'SRT',
                            'srt8': 'SRT',
                            'summit': 'Summit',
                            'trackhawk': 'Trackhawk',
                            'trailhawk': 'Trailhawk'}}



def _norm(v):
    if isinstance(v, re.Pattern):
        return ("re", v.pattern, v.flags)
    if isinstance(v, (set, frozenset)):
        return ("set", sorted(map(repr, v)))
    if isinstance(v, Path):
        return ("path", str(v.relative_to(be._REPO_ROOT)))
    if isinstance(v, dict):
        return {k: _norm(x) for k, x in v.items()}
    if isinstance(v, (tuple, list)):
        return type(v)(_norm(x) for x in v)
    return v


@pytest.mark.parametrize("name", sorted(FUNCTION_SIGNATURES))
def test_function_signature(name):
    fn = getattr(be, name)
    fn = getattr(fn, "__wrapped__", fn)
    assert str(inspect.signature(fn)) == FUNCTION_SIGNATURES[name]


@pytest.mark.parametrize("name", LRU_CACHED)
def test_lru_cache_kept(name):
    assert hasattr(getattr(be, name), "cache_info")


@pytest.mark.parametrize("name", sorted(CLASS_MEMBERS))
def test_class_members(name):
    cls = getattr(be, name)
    assert sorted(n for n in vars(cls) if not n.startswith("__")) == CLASS_MEMBERS[name]


@pytest.mark.parametrize("name", sorted(CONSTANTS))
def test_constant_value(name):
    assert _norm(getattr(be, name)) == CONSTANTS[name]


def test_ladder_stores_does_not_load_pdfplumber():
    code = (
        "import sys; sys.modules['pdfplumber'] = None; "
        "import backend.enrichment.provenance.ladder_stores as m; "
        "assert 'pdfplumber' not in [k for k, v in sys.modules.items() if v is not None]; "
        "assert 'backend.enrichment.brochure_extract' not in sys.modules"
    )
    r = subprocess.run([sys.executable, "-c", code], cwd=REPO, capture_output=True, text=True, timeout=120)
    assert r.returncode == 0, r.stderr
