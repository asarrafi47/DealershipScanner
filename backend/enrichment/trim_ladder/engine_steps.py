"""Brochure engine step-up parsing (attribution revoked 2026-07-31)."""
from __future__ import annotations

import json
import logging
import re
from functools import lru_cache
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

from ._common import (
    _norm_token,
)

# --- engine step-up: REVOKED 2026-07-31, kept only as a parser ------------
#
# This section used to be the one engine comparison we were willing to print: a
# brochure trim-walk page headed "<TRIM> / Adds to <LOWER TRIM>" whose bullet
# list contains an engine. The reasoning was that the page gives us the claim,
# its direction and its baseline together, with no inference of ours in between.
#
# That reasoning does not survive reading the code below. TWO of the three parts
# are ours, not the brochure's:
#
#   * the bullet is COMPOSED. ``_brochure_engine_bullet_by_step`` emits
#     ``f"{engine} — added over the {base_name}"``. The brochure prints
#     ``engine``. It does not print the em dash, the phrase "added over the", or
#     ``base_name`` — that comes out of OUR ladder definition via
#     ``steps_def[lo]["name"]``, not off the page. Handing that string to
#     ``_CitationRegister.add_exact`` made the register agree with a sentence
#     nobody had written down, which is the circular self-certification the gate
#     exists to prevent.
#   * the trim is INFERRED FROM POSITION. ``_brochure_engine_step_ups`` walks
#     back up to three lines from the "Adds to <LOWER>" marker and takes the
#     first non-marker line as the trim being described. Nothing labels that line
#     as a trim name.
#
# ``brochure_trim_walk`` is therefore ``uncited`` in ``LADDER_BULLET_STORES``,
# and ``_brochure_engine_bullet_by_step`` returns nothing while it stays that
# way. The parser is left in place because it is the honest half of the job and
# a future phase can build on it — but re-admitting the store needs the bullet to
# BE one printed line, and the trim to come from a labelled field.
#
# Everything below still fails closed — a layout we cannot parse yields no bullet.

_BROCHURE_ADDS_TO_RE = re.compile(r"^Adds to ([A-Za-z0-9][A-Za-z0-9 /®+.\-]{0,24})$", re.I)
_BROCHURE_ENGINE_ITEM_RE = re.compile(
    r"^\d\.\d\s?L\b[^•]{0,64}\b(?:engine|V-?\d|I-?\d)\b[^•]{0,16}$", re.I
)
_TRADEMARK_RE = re.compile(r"[®™]")


_BROCHURE_PAGE_MARKER_RE = re.compile(r"^---\s*page\s+(\d+)\s*---$", re.I)


@lru_cache(maxsize=128)
def _brochure_engine_step_ups(
    make: str, model: str, year: Any
) -> tuple[tuple[str, str, str, str, int], ...]:
    """
    ``(trim, lower trim, engine text, brochure text file, page)`` the brochure states outright.

    The file and the page travel with the claim because the bullet built from it
    has to clear the same citation check as everything else on the rung; the
    slim brochure text carries ``--- page N ---`` markers, so the page is read
    off the document rather than guessed.
    """
    from backend.enrichment.dictionary_catalog import catalog_key
    from backend.enrichment.dictionary_paths import BROCHURE_TEXT_SLIM_DIR

    try:
        y = int(year)
    except (TypeError, ValueError):
        return ()
    key = catalog_key(y, make or "", model or "")
    path = Path(BROCHURE_TEXT_SLIM_DIR) / f"{key.replace('|', '__')}.json"
    if not path.is_file():
        return ()
    try:
        blob = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return ()
    lines = [ln.strip() for ln in str((blob or {}).get("combined_trim_pages_text") or "").split("\n")]
    page_at: list[int] = []
    current_page = 0
    for ln in lines:
        marker = _BROCHURE_PAGE_MARKER_RE.match(ln)
        if marker:
            current_page = int(marker.group(1))
        page_at.append(current_page)

    out: list[tuple[str, str, str, str, int]] = []
    for i, line in enumerate(lines):
        m = _BROCHURE_ADDS_TO_RE.match(line)
        if not m:
            continue
        lower = m.group(1).strip()
        # Multi-column trim-walk tables flatten into one line and interleave
        # several trims' text; those never yield a single attributable claim.
        if re.search(r"\breplaces\b|\bofferedon\b|\boffered on\b", line, re.I):
            continue
        trim = ""
        for j in range(i - 1, max(-1, i - 4), -1):
            cand = lines[j]
            if cand and not cand.startswith("---"):
                trim = cand
                break
        if not trim or len(trim) > 30 or "•" in trim:
            continue
        for k in range(i + 1, min(len(lines), i + 40)):
            item = lines[k]
            if item.startswith("---") or item.upper().startswith("OPTIONS"):
                break
            if not item.startswith("•"):
                continue
            body = item.lstrip("• ").strip()
            if _BROCHURE_ENGINE_ITEM_RE.match(body):
                out.append(
                    (
                        trim,
                        lower,
                        _TRADEMARK_RE.sub("", body).strip(" .,;:—–-"),
                        path.name,
                        page_at[k],
                    )
                )
                break
    return tuple(out)


def _brochure_engine_bullet_by_step(
    steps_def: list[dict[str, Any]],
    make: str,
    model: str,
    year: Any,
) -> dict[int, tuple[str, dict[str, Any]]]:
    """
    step index → ``(engine bullet, citation)``. EMPTY while ``brochure_trim_walk``
    is revoked, which it is as of 2026-07-31.

    The bullet this builds is not a quotation — see the section header above. The
    store table is the policy, so flipping ``brochure_trim_walk`` back to a
    citable rule in ``LADDER_BULLET_STORES`` is what turns this function on
    again; that must not happen before the composition is removed.
    """
    from backend.enrichment.brochure_extract import ladder_bullet_store_admissible

    if not ladder_bullet_store_admissible("brochure_trim_walk"):
        return {}
    try:
        claims = _brochure_engine_step_ups(make, model, year)
    except Exception as e:  # pragma: no cover - never blocks the ladder
        logger.debug("brochure engine step-up lookup failed: %s", e)
        return {}
    if not claims:
        return {}
    index_by_key: dict[str, int] = {}
    for i, step in enumerate(steps_def):
        # NAME ONLY. This maps a trim name read off a brochure page onto a rung,
        # which is the same "these two strings are one trim" claim
        # ``_exact_rung_match`` makes, so it gets the same answer about aliases:
        # our own mapping table cannot license it. Unreachable today —
        # ``brochure_trim_walk`` is revoked above — and tightened now so
        # re-admitting the store does not re-open the alias route with it.
        key = _norm_token(str(step.get("name") or ""))
        if key:
            index_by_key.setdefault(key, i)
    out: dict[int, tuple[str, dict[str, Any]]] = {}
    for trim, lower, engine, source_file, page in claims:
        hi = index_by_key.get(_norm_token(trim))
        lo = index_by_key.get(_norm_token(lower))
        # Steps are luxury-first, so the baseline must sit at a LARGER index.
        if hi is None or lo is None or lo <= hi or hi in out:
            continue
        base_name = str(steps_def[lo].get("name") or "").strip()
        if not base_name:
            continue
        out[hi] = (
            f"{engine} — added over the {base_name}",
            {"store": "brochure_trim_walk", "source": source_file, "page": page},
        )
    return out
