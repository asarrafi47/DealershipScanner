"""
Shared constants and low-level display primitives for car serialization.

Split out of the former monolith ``backend/utils/car_serialize.py`` and
re-exported from the package facade so the public import surface is unchanged.
"""
from __future__ import annotations

import logging
import math
import re
from typing import Any

from backend.utils.field_clean import (
    is_effectively_empty,
    is_spec_overlay_junk,
)

logger = logging.getLogger(__name__)

DISPLAY_DASH = "—"

# Omitted from public JSON and redacted in /dev debug endpoints (SEC-087).
SENSITIVE_CAR_ROW_KEYS = frozenset(
    {
        "internal_notes",
        "marked_for_review",
        "price_provenance_json",
        "spec_source_json",
        "recovery_notes",
        "recovery_source",
        "recovery_status",
        "recoverability_score",
        "missing_field_count",
    }
)


def redact_sensitive_car_row(row: dict[str, Any]) -> dict[str, Any]:
    """Copy car row dict without operator-only / provenance columns."""
    return {k: v for k, v in row.items() if k not in SENSITIVE_CAR_ROW_KEYS}


# Dealer DMS boilerplate → treat as missing in UI/API
_MANUFACTURER_SPEC_RE = re.compile(
    r"see\s+manufacturer|manufacturer\s+specifications|refer\s+to\s+manufacturer",
    re.IGNORECASE,
)


def format_display_value(value: Any, *, dash: str = DISPLAY_DASH) -> str:
    """
    Human-facing string for a spec field.
    None / null / N/A / 'None' / manufacturer boilerplate → em dash (—).
    """
    if value is None:
        return dash
    if isinstance(value, bool):
        return "Yes" if value else "No"
    if isinstance(value, float) and math.isnan(value):
        return dash
    if isinstance(value, (int, float)):
        if isinstance(value, float) and value == int(value):
            return str(int(value))
        return str(value)
    s = str(value).strip()
    if not s:
        return dash
    low = s.lower()
    if low in ("none", "null", "undefined"):
        return dash
    if is_effectively_empty(s):
        return dash
    if _MANUFACTURER_SPEC_RE.search(s):
        return dash
    return s


def _format_mpg_city_highway(mpg_city: Any, mpg_highway: Any) -> str | None:
    """Delegate to field_clean (shared with knowledge_engine verified_specs)."""
    from backend.utils.field_clean import format_mpg_city_highway_display

    return format_mpg_city_highway_display(mpg_city, mpg_highway)


def _dealer_spec_wins(dealer_val: Any) -> bool:
    """True when DB/dealer column has a real value (VDP/listing) vs EPA placeholder."""
    if dealer_val is None:
        return False
    if isinstance(dealer_val, bool):
        return True
    if isinstance(dealer_val, (int, float)):
        if isinstance(dealer_val, float) and math.isnan(dealer_val):
            return False
        return True
    if is_effectively_empty(dealer_val):
        return False
    s = str(dealer_val).strip()
    if not s or is_spec_overlay_junk(s):
        return False
    return True


_EPA_MODE_AGGREGATE_RE = re.compile(
    r"\(?\s*EPA\s+mode\s+aggregate\s*\)?",
    re.IGNORECASE,
)


def _strip_epa_aggregate_label(s: str) -> str:
    if not s:
        return s
    return _EPA_MODE_AGGREGATE_RE.sub("", s).strip().strip(",").strip()


def _transmission_line_has_gear_count(s: Any) -> bool:
    if s is None:
        return False
    raw = str(s).strip()
    if not raw:
        return False
    if re.search(r"\b\d+[-\s]?speed\b", raw, re.I):
        return True
    if re.match(r"Auto(?:matic)?\s*\(\s*S\d+\s*\)", raw, re.I):
        return True
    if re.match(r"Auto(?:matic)?\s*\(\s*AM-S\d+\s*\)", raw, re.I):
        return True
    if re.match(r"Auto(?:matic)?\s*\(\s*A\d+\s*\)", raw, re.I):
        return True
    return False


def _transmission_phrase_prefer_detail(td_src: Any, td_norm: str | None) -> Any:
    """
    If the source string names a gear count (e.g. '8-Speed Automatic'), show that phrase
    instead of collapsing to the bucket label 'Automatic' / 'Manual'.
    """
    if td_src is None:
        return td_norm if td_norm else td_src
    raw = str(td_src).strip()
    if not raw:
        return td_norm if td_norm else td_src
    if td_norm in ("Automatic", "Manual") and re.search(r"\b\d+[-\s]?speed\b", raw, re.I):
        return raw
    return td_norm if td_norm else td_src
