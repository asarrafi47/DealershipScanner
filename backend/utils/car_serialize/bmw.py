"""
BMW-specific display rules: condition (CPO/Used/New) and model/trim splitting.

Split out of the former monolith ``backend/utils/car_serialize.py`` and
re-exported from the package facade so the public import surface is unchanged.
"""
from __future__ import annotations

import logging
import os
import re
from typing import Any

from backend.utils.field_clean import is_effectively_empty

from ._common import DISPLAY_DASH, format_display_value

logger = logging.getLogger(__name__)

_BMW_SRC_CPO_MARKERS = (
    "certified-inventory",
    "certified_inventory",
    "/certified/",
    "/cpo/",
    "certifiedused",
    "bmw-certified",
    "-cpo-",
    "cpo-inventory",
)


def _bmw_trace_vin_enabled(vin: str | None) -> bool:
    raw = (os.environ.get("BMW_TRACE_VINS") or "").strip().upper()
    if not raw or not vin:
        return False
    v = str(vin).strip().upper()[:17]
    return v in {x.strip()[:17] for x in raw.split(",") if x.strip()}


def _bmw_resolve_condition_for_display(
    c: dict[str, Any],
    out: dict[str, Any],
    *,
    title_lower: str,
) -> None:
    """
    BMW-only display rules for *condition* (does not invent odometer-based used).

    Priority: explicit Used/New in DB → keep; specific dealer condition strings → keep;
    then ``is_cpo`` / title CPO phrases / certified inventory URL / title Used|New prefix;
    generic DB value ``certified`` → *Certified Pre-Owned*; else leave ``out`` unchanged.
    """
    if (c.get("make") or "").strip().upper() != "BMW":
        return

    dash = DISPLAY_DASH
    raw_cond = c.get("condition")
    rl = str(raw_cond).strip().lower() if raw_cond else ""

    if rl in ("used", "new"):
        return
    if rl and rl not in ("certified",) and "certif" not in rl:
        if out.get("condition") != dash:
            return

    vin_key = str(c.get("vin") or "")[:17]
    trace = _bmw_trace_vin_enabled(vin_key)
    rule: str | None = None

    if c.get("is_cpo") in (1, True, "1"):
        out["condition"] = "Certified Pre-Owned"
        rule = "is_cpo"
    elif title_lower and (
        "bmw certified" in title_lower
        or "bmw cpo" in title_lower
        or "certified pre-owned" in title_lower
        or "certified preowned" in title_lower
    ):
        out["condition"] = "Certified Pre-Owned"
        rule = "title_cpo"
    else:
        su = (c.get("source_url") or "").lower()
        if any(m in su for m in _BMW_SRC_CPO_MARKERS) or ("certified" in su and "inventory" in su):
            out["condition"] = "Certified Pre-Owned"
            rule = "source_url_cpo"
        elif title_lower:
            if title_lower.startswith("used "):
                out["condition"] = "Used"
                rule = "title_used_prefix"
            elif title_lower.startswith("new "):
                out["condition"] = "New"
                rule = "title_new_prefix"

    if rule is None and rl == "certified":
        out["condition"] = "Certified Pre-Owned"
        rule = "db_certified_token"

    if trace:
        logger.info(
            "BMW condition display trace vin=%s rule=%r raw_db_condition=%r is_cpo=%r title=%r source_url=%r out_condition=%r",
            vin_key,
            rule,
            raw_cond,
            c.get("is_cpo"),
            (c.get("title") or "")[:120],
            (c.get("source_url") or "")[:160],
            out.get("condition"),
        )


def _bmw_series_trim_from_motor_model(model_raw: str) -> tuple[str | None, str | None]:
    """
    Single-token BMW motor models: 330i -> (3 Series, 330i); M340i -> (3 Series, M340i).
    Does not split X3, i4, or multi-word model strings.
    """
    s = (model_raw or "").strip()
    if not s or " " in s:
        return None, None
    su = s.upper()
    if su.startswith("X") and re.match(r"^X\d", su):
        return None, None
    if su.startswith("Z") and re.match(r"^Z\d", su):
        return None, None
    if re.match(r"^[iI][Xx\d]", s):
        return None, None
    m = re.match(r"^M(\d)(\d{2})([iI])$", s)
    if m:
        return f"{m.group(1)} Series", s
    m2 = re.match(r"^([2-8])(\d{2})([eEiI]+)$", s)
    if m2:
        return f"{m2.group(1)} Series", s
    return None, None


# Strips the series-prefix digit from standard BMW motor trims:
#   330i -> 30i,  540i xDrive -> 40i xDrive,  M340i -> unchanged,  xDrive30i -> unchanged
_BMW_MOTOR_TRIM_PREFIX_RE = re.compile(r"^([2-9])(\d{2}[eEiI]\S*(?:\s+.*)?)$")


def _strip_bmw_trim_series_prefix(trim: str) -> str:
    """330i -> 30i; 540i xDrive -> 40i xDrive. Leaves M340i, xDrive30i, 30i untouched."""
    m = _BMW_MOTOR_TRIM_PREFIX_RE.match(trim.strip())
    return m.group(2) if m else trim


_BMW_MODEL_TAIL = re.compile(
    r"""
    ^(?P<base>
        X\d[A-Za-z]?              # X3, X5M
      | [iI][Xx\d]+               # i4, iX, i7
      | Z\d                       # Z4
      | \d{3,4}[eE]?              # 330i, 530e, 760i
      | \d\s+Series               # 5 Series, 3 Series
    )
    \s+(?P<tail>.+)$
    """,
    re.VERBOSE | re.IGNORECASE,
)


def _bmw_title_suffix_trim(title: str, model: str) -> str | None:
    """If title contains ``… {model} {trim}``, return trim tail (conservative)."""
    if not title or not model:
        return None
    t = title.strip()
    m = model.strip()
    if len(m) < 2:
        return None
    idx = t.upper().rfind(m.upper())
    if idx < 0:
        return None
    rest = t[idx + len(m) :].strip()
    if len(rest) < 2:
        return None
    if not re.search(r"(?i)(s?drive|xdrive|m\s+sport|competition|pure\s+impulse|gran\s+coupe)", rest):
        if not re.match(r"^[A-Za-z0-9][A-Za-z0-9\s\-]{1,60}$", rest):
            return None
    return rest[:80]


def apply_bmw_model_trim_display(car: dict[str, Any]) -> tuple[str, str]:
    """
    Return (model_display, trim_display) for BMW rows only. Does not mutate *car*.
    Conservative: only fills trim from title/model split when trim is missing.
    """
    make = (car.get("make") or "").strip()
    if make.upper() != "BMW":
        return format_display_value(car.get("model")), format_display_value(car.get("trim"))

    model_raw = (car.get("model") or "").strip()
    trim_raw = car.get("trim")
    title = (car.get("title") or "").strip()

    model_base = model_raw
    trim_extra: str | None = None
    series_from_motor, motor_trim = _bmw_series_trim_from_motor_model(model_raw)
    if series_from_motor:
        model_base = series_from_motor

    mm = _BMW_MODEL_TAIL.match(model_raw)
    if mm:
        model_base = mm.group("base").strip()
        trim_extra = mm.group("tail").strip()

    trim_out = trim_raw if isinstance(trim_raw, str) and trim_raw.strip() else None
    if trim_extra:
        trim_out = trim_extra if not trim_out else f"{trim_out} / {trim_extra}"

    if motor_trim and (not trim_out or is_effectively_empty(trim_out)):
        trim_out = motor_trim

    if not trim_out or is_effectively_empty(trim_out):
        from_title = _bmw_title_suffix_trim(title, model_base)
        if from_title:
            trim_out = from_title

    # NOTE: the series-prefix strip (330i → 30i) is deliberately NOT applied.
    # It was meant to avoid reading "5 Series 535i" as redundant, but "35i" is
    # not a designation BMW uses or a shopper recognises — the badge on the car
    # says 535i, where 5 is the series and 35 the engine class, and splitting
    # them leaves a label that matches nothing. Observed on
    # WBA5B3C5XED539263: stored trim "535i xDrive", spec panel rendered
    # "35i xDrive", while the build sheet (which does not use this normalizer)
    # correctly showed "535i xDrive" on the same page.
    # ``_strip_bmw_trim_series_prefix`` is kept and still exported because it is
    # part of the module's public surface; it simply is not used for display.
    return format_display_value(model_base), format_display_value(trim_out)
