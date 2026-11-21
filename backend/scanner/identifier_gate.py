"""Write-time identifier gate (N6.9, D-N17) and model-year gate (FP17).

A ``cars`` primary key must be a real 17-character VIN. Order numbers (Metro
Honda, 410 rows) and 8-character dealer codes (Koons, 152 rows) are refused at
parse and again at upsert. 17-character IDs holding I, O or Q are NOT refused
here: D-N17b decides them in N21.

Model year 0 or NULL (FP17, 1,019 rows) is first recovered from the VIN (10th
character, 7th character picks the 1980-2009 or 2010-2039 cycle); a row whose
year cannot be recovered is refused.
"""
from __future__ import annotations

import logging
from datetime import date

logger = logging.getLogger(__name__)

VIN_LENGTH = 17
_YEAR_CODES = "ABCDEFGHJKLMNPRSTVWXY123456789"  # 1980/2010 .. 2009/2039
MIN_MODEL_YEAR = 1981


def vin_length_ok(vin: object) -> bool:
    return isinstance(vin, str) and len(vin.strip()) == VIN_LENGTH


def year_from_vin(vin: object) -> int | None:
    """Model year from a 17-character VIN, or None when it does not decode to a plausible year."""
    if not vin_length_ok(vin):
        return None
    s = str(vin).strip().upper()
    idx = _YEAR_CODES.find(s[9])
    if idx < 0:
        return None
    # North American position 7: a letter means the 2010+ cycle, a digit the 1980-2009 one.
    year = (2010 if s[6].isalpha() else 1980) + idx
    return year if MIN_MODEL_YEAR <= year <= date.today().year + 2 else None


def _year_int(v: object) -> int:
    try:
        return int(float(str(v).strip())) if v not in (None, "") else 0
    except (ValueError, TypeError):
        return 0


def gate_reason(v: dict) -> str | None:
    """Refusal reason for one vehicle dict, or None to keep it. Fills a recoverable year in place."""
    if not vin_length_ok(v.get("vin")):
        return "vin_length_not_17"
    year = _year_int(v.get("year"))
    if year < MIN_MODEL_YEAR:
        recovered = year_from_vin(v.get("vin"))
        if recovered is None:
            return "model_year_missing"
        v["year"] = recovered
    return None


def drop_ungated_vehicles(vehicles: list[dict]) -> tuple[list[dict], dict[str, int]]:
    """Split ``vehicles`` into rows that pass the gate and a {reason: count} of the refused."""
    kept: list[dict] = []
    reasons: dict[str, int] = {}
    for v in vehicles:
        why = gate_reason(v)
        if why:
            reasons[why] = reasons.get(why, 0) + 1
        else:
            kept.append(v)
    if reasons:
        logger.warning(
            "upsert_vehicles: refused %d row(s) at the identifier gate (%s)",
            sum(reasons.values()),
            ", ".join(f"{k}={n}" for k, n in sorted(reasons.items())),
        )
    return kept, reasons
