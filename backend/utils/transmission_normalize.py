"""
Normalize dealer / EPA transmission strings to buyer-facing buckets: Automatic, Manual, CVT.

Used by verified-spec merge, API serialization, and maintenance scripts.
"""
from __future__ import annotations

import logging
import re
from typing import Any

from backend.enrichment.knowledge_engine import _is_na_spec

logger = logging.getLogger(__name__)

_STRICT_AUTO_TOKENS = re.compile(
    r"\b(?:"
    r"automatic|auto\b|a/t|at\b|tiptronic|steptronic|powershift|"
    r"dual\s+clutch|\bdct\b|direct\s+shift|"
    r"dual\s+shift\s+mode|transmission\s+w/\s*dual\s+shift\s+mode"
    r")\b",
    re.I,
)
_MANUAL_TOKENS = re.compile(
    r"\b(?:manual|m/t|stick\s*shift|stick-shift)\b|"
    r"(?:^|\s)(?:6|5|7)[-\s]*speed\s+manual\b",
    re.I,
)
_CVT_TOKENS = re.compile(
    r"\b(?:"
    r"continuously\s+variable(?:\s+transmission)?|"
    r"continuous\s+variable\s+transmission|"
    r"\bcvt\b|e[-\s]?cvt|ecvt|"
    r"power[-\s]?split|multistage\s+hybrid"
    r")\b",
    re.I,
)
# EV / fixed-ratio phrases stored as Automatic (inventory convention).
_EV_AUTOMATIC_TOKENS = re.compile(
    r"\b(?:"
    r"single[-\s]?speed(?:\s+fixed(?:\s+gear)?)?|"
    r"1[-\s]speed|one[-\s]speed|"
    r"fixed\s+gear|direct\s+drive"
    r")\b",
    re.I,
)
_CVT_VAR_OR_CONTINUOUS = re.compile(r"\b(?:variable|continuous)\b", re.I)
_PERF_HINT = re.compile(
    r"\b(?:"
    r"type\s*r|typer|\bsti\b|hellcat|trackhawk|trx\b|"
    r"gt350|gt500|zr1|z06|grand\s+sport|"
    r"\bm[23458]\b|amg\b|rs\s*[368]\b|"
    r"corvette\s+z|911\s+gt|camaro\s+zl1"
    r")\b",
    re.I,
)


def normalize_transmission_standard(
    raw: Any,
    *,
    make: str | None = None,
    model: str | None = None,
    trim: str | None = None,
    title: str | None = None,
    year: int | None = None,
    vin: str | None = None,
    log_weak: bool = True,
) -> tuple[str | None, bool]:
    """
    Map *raw* transmission text to ``Automatic``, ``Manual``, or ``CVT``.

    Returns ``(label, weak_match)``. *weak_match* is True when classification used the
    generic fallback (default Automatic) so operators can audit logs.

    Set *log_weak* False for bulk DB scripts to emit one summary instead of per-row noise.

    ``None`` / placeholders yield ``(None, False)``.
    """
    if raw is None:
        return None, False
    s = str(raw).strip()
    if not s or _is_na_spec(s):
        return None, False

    u = s

    # Dual-clutch / automated gearboxes (not "manual" for buyer-facing bucket).
    if re.search(r"\bautomated\s+manual\b", u, re.I) or re.search(r"\bsemi[-\s]?automatic\b", u, re.I):
        return "Automatic", False

    # Manual before CVT so mixed phrases resolve to Manual when the word appears.
    if re.fullmatch(r"m\.?", s.strip(), re.I):
        return "Manual", False
    if _MANUAL_TOKENS.search(u):
        return "Manual", False

    # CVT phrases / keywords (before speed-based automatic and EV fixed-ratio rules).
    if _CVT_TOKENS.search(u) or _CVT_VAR_OR_CONTINUOUS.search(u):
        return "CVT", False
    if re.search(r"\bvariable\s+transmission\b", u, re.I):
        return "CVT", False

    # Dealer / data-feed single-letter automatic shorthand.
    if re.fullmatch(r"a\.?", s.strip(), re.I):
        return "Automatic", False

    # Fixed-ratio EV / hybrid gearing → Automatic bucket (after explicit CVT cues).
    if _EV_AUTOMATIC_TOKENS.search(u):
        return "Automatic", False

    # Explicit automatic tokens (incl. "Single-speed automatic", etc.).
    if _STRICT_AUTO_TOKENS.search(u):
        return "Automatic", False

    # Contains "Speed" and not Manual → geared automatic (e.g. "8-Speed", "1-Speed Automatic").
    if re.search(r"\bspeed\b", u, re.I) and not _MANUAL_TOKENS.search(u):
        return "Automatic", False

    # --- Fallback: assume automatic for typical US inventory; log for review ---
    blob = f"{title or ''} {trim or ''} {model or ''} {make or ''}"
    perf = bool(_PERF_HINT.search(blob))
    y = year if isinstance(year, int) else None
    if log_weak:
        logger.warning(
            "transmission_weak_match_default_automatic vin=%r raw=%r make=%r model=%r "
            "year=%s performance_hint=%s",
            (vin or "").strip() or None,
            s,
            (make or "").strip() or None,
            (model or "").strip() or None,
            y,
            perf,
        )
    return "Automatic", True
