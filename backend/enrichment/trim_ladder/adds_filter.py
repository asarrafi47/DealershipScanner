""""This trim adds X" has to actually be an ADD: suppression and quality gates."""
from __future__ import annotations

import json
import logging
import re
from functools import lru_cache
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

from ._common import (
    _norm_make,
)
from .plausibility import (
    _ladder_steps_plausible_for_model,
)

# --- "this trim adds X" has to actually be an ADD -------------------------
#
# The LLM-derived brochure lists routinely staple lineup-wide standard equipment
# onto the top rung. Live example: the 2020 Dodge Durango R/T rung claimed
# "Three-Zone Automatic Temperature Control with infrared sensors" as an add,
# while the same brochure's own equipment grid marks that row standard in all
# five columns — i.e. an SXT has it too. Both guards below only ever DELETE a
# bullet, so the failure mode is a missing line, never a false one.

_ADD_TOKEN_RE = re.compile(r"[a-z0-9]+")
_ADD_STOPWORDS = frozenset(
    {
        "and", "the", "with", "for", "includes", "included", "including", "features",
        "system", "systems", "available", "std", "standard", "optional", "package",
        "packaged", "group", "plus", "new", "all", "only", "when", "your", "you",
    }
)
# Feature marks in an OEM equipment grid: included / optional / package.
_GRID_MARK_RUN_RE = re.compile(r"((?:(?:[•OP](?:/[•OP])?)[\s|]+)*(?:[•OP](?:/[•OP])?))\s*$")
_MIN_ADD_TOKENS_FOR_GRID_MATCH = 4
_GRID_MATCH_COVERAGE = 0.8


def _add_tokens(text: str) -> frozenset[str]:
    return frozenset(
        t
        for t in _ADD_TOKEN_RE.findall(str(text or "").lower())
        if len(t) >= 3 and t not in _ADD_STOPWORDS
    )


def _norm_add_line(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(text or "").lower()).strip()


@lru_cache(maxsize=128)
def _lineup_standard_features(make: str, model: str, year: Any) -> tuple[frozenset[str], ...]:
    """Token sets for equipment the brochure grid marks standard on EVERY trim.

    Returns () whenever the grid cannot be read unambiguously — including when
    two different column counts are equally common, because guessing the column
    count wrong would suppress genuine trim-specific content.
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
    text = str((blob or {}).get("combined_trim_pages_text") or "")
    if not text:
        return ()

    rows: list[tuple[list[str], str]] = []
    widths: dict[int, int] = {}
    for raw in text.split("\n"):
        line = raw.strip()
        if not line or line.startswith("---"):
            continue
        m = _GRID_MARK_RUN_RE.search(line)
        if not m:
            continue
        marks = [t for t in re.split(r"[\s|]+", m.group(1)) if t]
        feature = line[: m.start(1)].strip(" .|–—-")
        if len(marks) < 2 or len(feature) < 6:
            continue
        widths[len(marks)] = widths.get(len(marks), 0) + 1
        rows.append((marks, feature))
    if not widths:
        return ()
    ordered = sorted(widths.items(), key=lambda kv: (-kv[1], -kv[0]))
    if len(ordered) > 1 and ordered[0][1] == ordered[1][1]:
        # Ambiguous column count — a wrong guess suppresses real adds.
        return ()
    n_cols = ordered[0][0]
    out: list[frozenset[str]] = []
    for marks, feature in rows:
        if len(marks) != n_cols or any(mk != "•" for mk in marks):
            continue
        toks = _add_tokens(feature)
        if len(toks) >= _MIN_ADD_TOKENS_FOR_GRID_MATCH:
            out.append(toks)
    return tuple(out)


def _suppress_non_adds(
    steps_out: list[dict[str, Any]],
    *,
    make: str,
    model: str,
    year: Any,
) -> None:
    """Strip bullets that are not adds for their rung. Mutates ``steps_out``.

    1. A bullet repeated verbatim on a LOWER rung is not something this rung
       adds — keep it only on the lowest rung that carries it.
    2. A bullet the brochure grid marks standard across the whole lineup is not
       an add on any rung above the base one. The base rung keeps it: there its
       list reads as "what you get", not "what it adds".
    """
    if not steps_out:
        return
    try:
        grid = _lineup_standard_features(make, model, year)
    except Exception as e:  # pragma: no cover - defensive, never blocks the ladder
        logger.debug("lineup-standard lookup failed: %s", e)
        grid = ()

    # Steps are luxury-first, so "lower rung" == larger index.
    seen_below: set[str] = set()
    for idx in range(len(steps_out) - 1, -1, -1):
        step = steps_out[idx]
        is_base = idx == len(steps_out) - 1
        kept: list[str] = []
        mine: set[str] = set()
        for line in step.get("adds") or []:
            norm = _norm_add_line(line)
            if not norm:
                continue
            if norm in seen_below:
                continue
            if not is_base and grid:
                toks = _add_tokens(line)
                if len(toks) >= _MIN_ADD_TOKENS_FOR_GRID_MATCH and any(
                    len(toks & row) / len(toks) >= _GRID_MATCH_COVERAGE for row in grid
                ):
                    continue
            mine.add(norm)
            kept.append(line)
        step["adds"] = kept
        seen_below |= mine


def _trim_ladder_quality(result: dict[str, Any], make: str, model: str) -> str:
    """high | medium | low — used for display gating and UI confidence."""
    source = str(result.get("source") or "").lower()
    matched = bool(result.get("matched"))
    names = {str(s.get("name") or "").lower() for s in result.get("steps") or [] if s.get("name")}
    generic_cross_make = frozenset({"platinum", "limited", "premium", "touring", "xle", "sport", "base"})
    if _norm_make(make) not in {"toyota", "lexus", "scion"} and names and names <= generic_cross_make:
        return "low"
    if not matched:
        if source == "curated":
            return "medium"
        if "complete_options" in source or "epa" in source or source.endswith(".csv"):
            return "medium"
        return "low"
    if source == "curated":
        return "high"
    if source in {"inventory", "oem_knowledge"}:
        return "medium"
    if "epa" in source or source.endswith("_epa.csv"):
        return "medium"
    return "medium"


def _only_marketing_brochure_placeholders(result: dict[str, Any]) -> bool:
    steps = result.get("steps") or []
    saw_add = False
    for step in steps:
        for line in step.get("adds") or []:
            saw_add = True
            if not re.search(
                r"\bmarketing trim level from oem brochure\b",
                str(line),
                re.I,
            ):
                return False
    return saw_add


def _trim_ladder_should_display(result: dict[str, Any], make: str, model: str) -> bool:
    steps = result.get("steps") or []
    if len(steps) < 2:
        return False
    step_dicts = [{"name": s.get("name")} for s in steps]
    if not _ladder_steps_plausible_for_model(step_dicts, make, model):
        return False
    if _trim_ladder_quality(result, make, model) == "low":
        return False
    if _only_marketing_brochure_placeholders(result):
        return False
    if not result.get("matched"):
        names = {str(s.get("name") or "").lower() for s in steps}
        if names <= {"sport", "base"} or names <= {"sport", "base", "s3 sportback"}:
            return False
    return True
