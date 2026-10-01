"""
The one drivetrain normalizer.

Before 2026-10-01 there were five (``knowledge_engine._VPIC_DRIVE_MAP`` /
``_norm_drive_epa``, ``catalog.resolver._drive_bucket``,
``nhtsa_vpic._normalize_drivetrain``, ``field_clean.coerce_drivetrain_stored``,
``spec_field_normalize.drivetrain_from_blob``, ``analytics_ep._norm_drivetrain``)
and they gave three different answers for "4x2": None / "2WD" / "FWD". They now
all call :func:`normalize_drivetrain`.

Rules (owner rules first):

* "4x2", "2WD", "2-Wheel Drive" say two driven wheels, NOT which end. vPIC
  stamps "4x2" on FWD Camrys and Pilots as much as on RWD trucks (91k rows of
  the local decode cache). It is never RWD and never FWD: the answer is None and
  the dealer feed / catalog tiebreak decide. An explicit end next to it
  ("2WD RWD", "2 Wheel Drive - Rear", "2WD Front") is honoured.
* "4x4" is four-wheel drive (vPIC spells its 4WD value "4WD/4-Wheel Drive/4x4").
* Ambiguous values name two different answers ("2WD/4WD", "FWD / AWD") and
  return None. AWD and 4WD together ("4-Wheel or All-Wheel Drive", EPA) are the
  same wheels driven and resolve to 4WD (the storage vocabulary's choice).
* "Dual Rear Wheels" is a dually axle, not rear-wheel drive.

``source`` selects how *raw* is read:

* ``"vpic"``, ``"epa"``, ``"dealer"`` -- a structured drivetrain value (a column,
  a decode field, a feed attribute). Matching is on the value with punctuation
  removed, so "4MATIC®", "FOUR_WHEEL_DRIVE" and "FourWheelDrive" all read.
  ``"dealer"`` also accepts the one-letter feed codes A / F / R.
* ``"text"`` -- free text (a title or trim). Word-bounded tokens only, first
  rule wins, so "Silverado 1500 LT 4WD" reads and "Allure" does not.
"""
from __future__ import annotations

import re
from typing import Any

CANONICAL_DRIVETRAINS: tuple[str, ...] = ("FWD", "RWD", "AWD", "4WD")

SOURCES: tuple[str, ...] = ("vpic", "epa", "dealer", "text")

_SCHEMA_ORG_RE = re.compile(r"(?:https?://)?schema\.org/([A-Za-z0-9-]+)\b", re.I)
_SCHEMA_ORG: dict[str, str] = {
    "allwheeldriveconfiguration": "AWD",
    "fourwheeldriveconfiguration": "4WD",
    "frontwheeldriveconfiguration": "FWD",
    "rearwheeldriveconfiguration": "RWD",
}
_ONE_LETTER: dict[str, str] = {"A": "AWD", "F": "FWD", "R": "RWD"}

# Structured values, matched on the upper-cased value with every non-alphanumeric
# removed ("4WD/4-Wheel Drive/4x4" -> "4WD4WHEELDRIVE4X4").
_AWD_RE = re.compile(
    r"AWD|ALLWHEEL(?!S)|4MATIC|XDRIVE|QUATTRO|ALL4|4MOTION|HTRAC|4ORCE|SYMMETRICAL"
)
_4WD_RE = re.compile(r"4WD|4X4|FOURWHEEL(?!S)|4WHEEL(?!S)")
_FWD_RE = re.compile(r"FWD|FRONTWHEEL(?!S)|FRONTTRAK")
_RWD_RE = re.compile(r"RWD|(?<!DUAL)REARWHEEL(?!S)|SDRIVE")  # "Dual Rear Wheel(s)" is a dually axle
_TWO_WHEEL_RE = re.compile(r"4X2|2WD|2WHEEL(?!S)|TWOWHEEL(?!S)")
# An end named next to a two-wheel token ("2 Wheel Drive - Rear", "2WD Front"):
# only read when a two-wheel token is present, so a bare "Rear" means nothing.
_TWO_WHEEL_REAR_RE = re.compile(r"(?<!DUAL)REAR(?!WHEELS)")
_TWO_WHEEL_FRONT_RE = re.compile(r"FRONT")

# Free text: ordered, word-bounded (the old ``drivetrain_from_blob`` order, with
# 4X2 no longer read as FWD and 4X4 read as 4WD).
_TEXT_RULES: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"\bFOUR[\s-]WHEEL[\s-]DRIVE\b"), "4WD"),
    (re.compile(r"\b4WD\b"), "4WD"),
    (re.compile(r"\b4X4\b"), "4WD"),
    (re.compile(r"\bALL[\s-]WHEEL[\s-]DRIVE\b"), "AWD"),
    (re.compile(r"\bAWD\b"), "AWD"),
    # Brand AWD badges, also when fused to a model code ("xDrive30i", "4MATIC+").
    (re.compile(r"\b(?:XDRIVE|4MATIC|QUATTRO|SH-AWD|ALL4|4MOTION)"), "AWD"),
    # Two-wheel drive with its end spelled out ("2 Wheel Drive - Rear", "2WD Front").
    (re.compile(r"\b(?:2WD|4X2|2[\s-]?WHEEL[\s-]DRIVE)[\s-]*(?:\(\s*)?REAR\b"), "RWD"),
    (re.compile(r"\b(?:2WD|4X2|2[\s-]?WHEEL[\s-]DRIVE)[\s-]*(?:\(\s*)?FRONT\b"), "FWD"),
    (re.compile(r"\bFRONT[\s-]WHEEL[\s-]DRIVE\b"), "FWD"),
    (re.compile(r"\bFWD\b"), "FWD"),
    (re.compile(r"\bREAR[\s-]WHEEL[\s-]DRIVE\b"), "RWD"),
    (re.compile(r"\bRWD\b"), "RWD"),
    (re.compile(r"\bSDRIVE\d*"), "RWD"),  # BMW sDrive (scanner._infer_drivetrain_from_trim)
)


def _compact(s: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", s.upper())


def _two_wheel_end(c: str) -> tuple[bool, bool]:
    """(front, rear) named next to a two-wheel token in compact value *c*."""
    if not _TWO_WHEEL_RE.search(c):
        return False, False
    return bool(_TWO_WHEEL_FRONT_RE.search(c)), bool(_TWO_WHEEL_REAR_RE.search(c))


def is_two_wheel_unknown(raw: Any) -> bool:
    """True when *raw* says "two driven wheels" and names no end (4x2 / 2WD)."""
    if raw is None:
        return False
    c = _compact(str(raw))
    if not c or not _TWO_WHEEL_RE.search(c):
        return False
    if any(_two_wheel_end(c)):
        return False
    return not (_FWD_RE.search(c) or _RWD_RE.search(c) or _AWD_RE.search(c) or _4WD_RE.search(c))


def _structured(s: str) -> str | None:
    c = _compact(s)
    if not c:
        return None
    awd = bool(_AWD_RE.search(c))
    four = bool(_4WD_RE.search(c))
    front_end, rear_end = _two_wheel_end(c)
    fwd = bool(_FWD_RE.search(c)) or front_end
    rwd = bool(_RWD_RE.search(c)) or rear_end
    two = bool(_TWO_WHEEL_RE.search(c))
    all_four = awd or four
    two_ends = fwd or rwd
    if all_four and (two_ends or two):
        return None  # "FWD / AWD", "2WD/4WD": two different answers
    if fwd and rwd:
        return None
    if awd and four:
        return "4WD"  # EPA "4-Wheel or All-Wheel Drive": same wheels driven; storage keeps 4WD
    if awd:
        return "AWD"
    if four:
        return "4WD"
    if fwd:
        return "FWD"
    if rwd:
        return "RWD"
    return None  # bare 4x2 / 2WD, or nothing recognisable


def _text(s: str) -> str | None:
    u = s.upper()
    for rx, val in _TEXT_RULES:
        if rx.search(u):
            return val
    return None


def normalize_drivetrain(raw: Any, source: str = "dealer") -> str | None:
    """Canonical ``FWD`` / ``RWD`` / ``AWD`` / ``4WD`` for *raw*, else ``None``.

    None means "no drivetrain fact here": empty, placeholder, two-wheel-drive of
    unknown end (4x2 / 2WD), ambiguous, or unrecognised.
    """
    if raw is None:
        return None
    if source not in SOURCES:
        raise ValueError(f"unknown drivetrain source {source!r}")
    s = str(raw).strip()
    if not s:
        return None
    if source == "text":
        return _text(s)
    if source == "dealer" and s.upper() in _ONE_LETTER:
        return _ONE_LETTER[s.upper()]
    if "schema.org" in s.lower():
        m = _SCHEMA_ORG_RE.search(s)
        return _SCHEMA_ORG.get(re.sub(r"[^a-z0-9]", "", m.group(1).lower())) if m else None
    return _structured(s)


def drivetrain_storage_value(raw: Any) -> str | None:
    """The value ``cars.drivetrain`` stores for a feed value *raw*.

    The canonical answer when there is one. A two-wheel-drive-of-unknown-end
    claim is kept as the literal ``"2WD"``: it is not a drivetrain verdict
    (:func:`normalize_drivetrain` reads it as None everywhere a decision is
    made) but it is the dealer's statement that the car is not 4WD/AWD, and the
    vPIC heal settles the end with the catalog. Anything unrecognised is kept
    as sent (stripped), as the storage canonicalizer always did. Callers handle
    empty / placeholder values before calling.
    """
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    n = normalize_drivetrain(s, "dealer")
    if n:
        return n
    if "schema.org" in s.lower():
        return None  # an unknown schema.org type is markup, not a value
    if is_two_wheel_unknown(s):
        return "2WD"
    return s


def same_drive_wheels(a: str | None, b: str | None) -> bool:
    """True when two canonical drivetrains do not contradict (blank never does;
    AWD and 4WD are the same wheels driven)."""
    if not a or not b:
        return True
    return a == b or {a, b} <= {"AWD", "4WD"}
