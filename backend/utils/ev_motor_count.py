"""
Infer EV motor count from listing fields.

Returns 1, 2, or 3 for BEVs when inferable; None when unknown.
"""
from __future__ import annotations

import re
from typing import Any

_TRI_MOTOR_RE = re.compile(r"\b(?:tri|three)[- ]?motor(?:s)?\b", re.I)
_TWIN_DUAL_MOTOR_RE = re.compile(
    r"\b(?:twin|dual|two)[- ]?motor(?:s)?\b|\b(?:twin|dual)[- ]?drive\b",
    re.I,
)
_SINGLE_MOTOR_RE = re.compile(r"\b(?:single|one)[- ]?motor(?:s)?\b", re.I)

_KNOWN_EV_BRANDS = frozenset(
    {
        "tesla",
        "rivian",
        "lucid",
        "polestar",
        "fisker",
        "vinfast",
        "canoo",
    }
)

_EV_MODEL_RE = re.compile(
    r"\b(?:"
    r"model\s*[s3xy]|cybertruck|"
    r"eq[a-z]\d*|eqs|eqe|eqb|eqc|"
    r"e-tron|etron|"
    r"i[457x]\b|ix\b|"
    r"ex30|ex90|ex40|ec40|em90|"
    r"taycan|"
    r"ioniq\s*[56]|ev6|ev9|"
    r"niro\s*ev|kona\s*electric|"
    r"id\.?\s*\d|id\.?\s*buzz|"
    r"leaf|"
    r"mach[- ]?e|"
    r"f[- ]?150\s*lightning|"
    r"lyriq|celestiq|optiq|"
    r"blazer\s*ev|silverado\s*ev|sierra\s*ev|"
    r"hummer\s*ev|"
    r"air\b|"
    r"r1t|r1s"
    r")\b",
    re.I,
)


def _norm(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip()).lower()


def _norm_drivetrain(drivetrain: str | None) -> str | None:
    d = _norm(drivetrain)
    if not d:
        return None
    if d in {"awd", "a", "all wheel drive", "all-wheel drive", "4wd", "4x4"}:
        return "AWD"
    if d in {"rwd", "r", "rear wheel drive", "rear-wheel drive"}:
        return "RWD"
    if d in {"fwd", "f", "front wheel drive", "front-wheel drive", "4x2"}:
        return "FWD"
    if "all" in d and "wheel" in d and "drive" in d:
        return "AWD"
    if "rear" in d and "wheel" in d and "drive" in d:
        return "RWD"
    if "front" in d and "wheel" in d and "drive" in d:
        return "FWD"
    if re.fullmatch(r"a|awd|4wd", d, re.I):
        return "AWD"
    if re.fullmatch(r"r|rwd", d, re.I):
        return "RWD"
    return drivetrain.strip().upper() if drivetrain else None


def _is_pure_electric_fuel(fuel_type: str | None) -> bool:
    ft = _norm(fuel_type)
    if not ft:
        return False
    if "electric" not in ft and ft not in {"electricity", "ev", "bev"}:
        return False
    if "plug" in ft or "hybrid" in ft or "phev" in ft:
        return False
    return True


def _is_known_ev_brand(make: str | None) -> bool:
    return _norm(make) in _KNOWN_EV_BRANDS


def _is_known_ev_model(
    make: str | None,
    model: str | None,
    *,
    title: str | None = None,
    trim: str | None = None,
    description: str | None = None,
) -> bool:
    blob = " ".join(
        part for part in (_norm(make), _norm(model), _norm(title), _norm(trim), _norm(description)) if part
    )
    if not blob:
        return False
    if _EV_MODEL_RE.search(blob):
        return True
    mk = _norm(make)
    mo = _norm(model)
    if mk == "volvo" and re.search(r"\b(ex30|ex90|ex40|ec40|em90|c40)\b", mo):
        return True
    if mk == "mercedes-benz" or mk == "mercedes":
        return bool(re.search(r"\beq[a-z0-9]+\b", mo))
    return False


def _is_clearly_electric(
    *,
    make: str | None,
    model: str | None,
    fuel_type: str | None,
    title: str | None = None,
    trim: str | None = None,
    description: str | None = None,
) -> bool:
    if _is_pure_electric_fuel(fuel_type):
        return True
    text_blob = " ".join(
        part for part in (_norm(title), _norm(trim), _norm(description)) if part
    )
    if re.search(r"\belectric(?:ity)?\b|\bbev\b|\bbattery electric\b", text_blob):
        if "plug" not in text_blob and "hybrid" not in text_blob:
            return True
    if _is_known_ev_brand(make):
        return True
    return _is_known_ev_model(make, model, title=title, trim=trim, description=description)


def _explicit_motor_count(text_blob: str) -> int | None:
    if _TRI_MOTOR_RE.search(text_blob):
        return 3
    if _TWIN_DUAL_MOTOR_RE.search(text_blob):
        return 2
    if _SINGLE_MOTOR_RE.search(text_blob):
        return 1
    return None


def _brand_heuristic_motor_count(
    *,
    make: str | None,
    model: str | None,
    trim: str | None,
    title: str | None,
    drivetrain: str | None,
) -> int | None:
    mk = _norm(make)
    mo = _norm(model)
    tr = _norm(trim)
    ti = _norm(title)
    blob = f"{mo} {tr} {ti}"
    drive = _norm_drivetrain(drivetrain)

    if mk == "tesla":
        if drive == "RWD":
            return 1
        if drive == "AWD" and (
            re.search(r"\blong range\b", blob) or re.search(r"\bperformance\b", blob)
        ):
            return 2
        return None

    if mk == "volvo":
        if re.search(r"\bex30\b", mo) and re.search(r"\btwin motor\b", blob):
            return 2
        if re.search(r"\bex90\b", mo):
            if re.search(r"\btwin motor\b", blob):
                return 2
            if re.search(r"\b(?:rwd plus|plus rwd)\b", blob) or (
                drive == "RWD" and re.search(r"\bplus\b", blob)
            ):
                return 1
        return None

    if mk in {"mercedes-benz", "mercedes"} and re.search(r"\beq[a-z0-9]+\b", mo):
        if _SINGLE_MOTOR_RE.search(blob):
            return 1
        if drive == "AWD":
            return 2
        return None

    return None


def _drivetrain_fallback_motor_count(
    *,
    make: str | None,
    fuel_type: str | None,
    drivetrain: str | None,
) -> int | None:
    if not (_is_pure_electric_fuel(fuel_type) or _is_known_ev_brand(make)):
        return None
    drive = _norm_drivetrain(drivetrain)
    if drive == "AWD":
        return 2
    if drive == "RWD":
        return 1
    return None


def infer_ev_motor_count(
    *,
    make: str | None = None,
    model: str | None = None,
    trim: str | None = None,
    title: str | None = None,
    description: str | None = None,
    drivetrain: str | None = None,
    fuel_type: str | None = None,
) -> int | None:
    """
    Infer BEV motor count (1, 2, or 3) from listing metadata.

    Returns None when the vehicle is not clearly electric or motor count is unknown.
    """
    if not _is_clearly_electric(
        make=make,
        model=model,
        fuel_type=fuel_type,
        title=title,
        trim=trim,
        description=description,
    ):
        return None

    text_blob = " ".join(
        part for part in (_norm(trim), _norm(title), _norm(description)) if part
    )

    explicit = _explicit_motor_count(text_blob)
    if explicit is not None:
        return explicit

    brand = _brand_heuristic_motor_count(
        make=make,
        model=model,
        trim=trim,
        title=title,
        drivetrain=drivetrain,
    )
    if brand is not None:
        return brand

    return _drivetrain_fallback_motor_count(
        make=make,
        fuel_type=fuel_type,
        drivetrain=drivetrain,
    )
