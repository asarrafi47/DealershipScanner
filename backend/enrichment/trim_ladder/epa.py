"""EPA model/trim name matching."""
from __future__ import annotations

import logging
import re

logger = logging.getLogger(__name__)

from ._common import (
    _extract_trim_from_cell,
    _norm_make,
    _norm_model,
)

def _epa_models_match(car_model: str, row_model: str) -> bool:
    """True when an EPA CSV row model corresponds to the listing model."""
    cm = _norm_model(car_model)
    rm = _norm_model(row_model)
    if not cm or not rm:
        return False
    if cm == rm:
        return True
    if cm.startswith(rm) or rm.startswith(cm):
        return True
    if cm.endswith("class") and cm[:-5] == rm:
        return True
    if rm.endswith("class") and rm[:-5] == cm:
        return True
    if cm.replace("hybrid", "") == rm.replace("hybrid", ""):
        return True
    if cm.startswith("r8") and rm == "r8":
        return True
    if cm.startswith("r8spyder") and rm in {"r8", "r8spyder"}:
        return True
    if cm.startswith("rs6") and rm in {"rs6", "rs"}:
        return True
    return False


def _normalize_epa_trim(trim_raw: str, make: str, model: str) -> str | None:
    """Extract a display trim from EPA CSV cells (often verbose)."""
    from backend.enrichment.trim_ladder_knowledge import preserve_trim_label

    raw = str(trim_raw or "").strip()
    if not raw:
        return None

    preserved = preserve_trim_label(raw, make, model)
    if preserved:
        return preserved

    t = re.sub(r"\s*\([^)]*\)\s*", " ", raw).strip()
    t = re.sub(r"\s+", " ", t)
    if not t or t.startswith("("):
        return None

    if _norm_model(t) == _norm_model(model):
        preserved = preserve_trim_label(raw, make, model)
        if not preserved:
            return None
        t = preserved

    if _norm_make(make) == "jeep" and t.upper() in {"2WD", "4WD"}:
        return None
    if _norm_make(make) == "cadillac" and _norm_model(model) in {"escalade", "xt4"} and t.upper() in {
        "2WD",
        "4WD",
        "FWD",
        "AWD",
    }:
        return None
    if re.fullmatch(r"V\s*AWD", t, re.I) and _norm_make(make) == "cadillac":
        return "V-Series"
    if re.fullmatch(r"L\s*\dWD", t, re.I):
        return None

    preserved = preserve_trim_label(t, make, model)
    if preserved:
        return preserved

    name = _extract_trim_from_cell(t, make, model)
    return name
