"""
Lookup transmission / drivetrain / cylinders from the ``model_specs`` table plus static fallbacks.

Used when EPA ``epa_master`` is empty and dealer columns are blank — keeps listings and
completeness checks aligned with the project's spec dictionary.
"""
from __future__ import annotations

import os
import re
from typing import Any

from backend.db.inventory_db import get_conn

# Static fallbacks keyed by (make_lower, model_lower). Drivetrain uses short tokens
# (FWD/RWD/AWD/4WD) consistent with ``decode_trim_logic`` / EPA merge.
_MODEL_FALLBACK: dict[tuple[str, str], dict[str, Any]] = {
    # BMW — dealer often omits trans/drive on performance / niche codes
    ("bmw", "228i"): {"cylinders": 4, "transmission": "8-Speed Automatic", "drivetrain": "RWD"},
    ("bmw", "m2"): {"cylinders": 6, "transmission": "8-Speed Automatic", "drivetrain": "RWD"},
    ("bmw", "m3"): {"cylinders": 6, "transmission": "8-Speed Automatic", "drivetrain": "RWD"},
    ("bmw", "m4"): {"cylinders": 6, "transmission": "8-Speed Automatic", "drivetrain": "RWD"},
    ("bmw", "m5"): {"cylinders": 8, "transmission": "8-Speed Automatic", "drivetrain": "RWD"},
    ("bmw", "m6"): {"cylinders": 8, "transmission": "7-Speed Automatic", "drivetrain": "RWD"},
    ("bmw", "m8"): {"cylinders": 8, "transmission": "8-Speed Automatic", "drivetrain": "RWD"},
    ("bmw", "m340"): {"cylinders": 6, "transmission": "8-Speed Automatic", "drivetrain": "RWD"},
    ("bmw", "550e"): {"cylinders": 6, "transmission": "8-Speed Automatic", "drivetrain": "RWD"},
    ("bmw", "750e"): {"cylinders": 6, "transmission": "8-Speed Automatic", "drivetrain": "RWD"},
    ("bmw", "x2"): {"cylinders": 4, "transmission": "8-Speed Automatic", "drivetrain": "AWD"},
    ("bmw", "z4"): {"cylinders": 4, "transmission": "8-Speed Automatic", "drivetrain": "RWD"},
    ("bmw", "x3 m"): {"cylinders": 6, "transmission": "8-Speed Automatic", "drivetrain": "AWD"},
    ("bmw", "x5 m"): {"cylinders": 8, "transmission": "8-Speed Automatic", "drivetrain": "AWD"},
    # Hyundai — inventory uses full model names not in small MODEL_SPECS seed
    ("hyundai", "elantra"): {
        "cylinders": 4,
        "transmission": "Continuously Variable Transmission (CVT)",
        "drivetrain": "FWD",
    },
    ("hyundai", "elantra hybrid"): {
        "cylinders": 4,
        "transmission": "6-Speed EcoShift Dual-Clutch",
        "drivetrain": "FWD",
    },
    ("hyundai", "palisade"): {
        "cylinders": 6,
        "transmission": "8-Speed Automatic",
        "drivetrain": "AWD",
    },
    ("hyundai", "santa cruz"): {
        "cylinders": 4,
        "transmission": "8-Speed Automatic",
        "drivetrain": "AWD",
    },
    ("hyundai", "santa fe"): {
        "cylinders": 4,
        "transmission": "8-Speed Dual-Clutch",
        "drivetrain": "AWD",
    },
    ("hyundai", "santa fe hybrid"): {
        "cylinders": 4,
        "transmission": "6-Speed Automatic",
        "drivetrain": "AWD",
    },
    # Jeep — PHEV trim name not in seed table
    ("jeep", "wrangler 4xe"): {
        "cylinders": 4,
        "transmission": "8-Speed Automatic",
        "drivetrain": "4WD",
    },
    # Audi EV
    ("audi", "e-tron"): {
        "cylinders": 0,
        "transmission": "Single-speed automatic",
        "drivetrain": "AWD",
        "fuel_type": "Electric",
    },
    # VW EV
    ("volkswagen", "id. buzz"): {
        "cylinders": 0,
        "transmission": "1-Speed Automatic",
        "drivetrain": "RWD",
        "fuel_type": "Electric",
    },
    # Hyundai EV / PHEV
    ("hyundai", "ioniq 6"): {
        "cylinders": 0,
        "transmission": "Single-speed automatic",
        "drivetrain": "RWD",
        "fuel_type": "Electric",
    },
    ("hyundai", "ioniq 5"): {
        "cylinders": 0,
        "transmission": "Single-speed automatic",
        "drivetrain": "RWD",
        "fuel_type": "Electric",
    },
    ("hyundai", "tucson"): {
        "cylinders": 4,
        "transmission": "8-Speed Automatic",
        "drivetrain": "AWD",
    },
    ("hyundai", "tucson plug-in hybrid"): {
        "cylinders": 4,
        "transmission": "6-Speed Automatic",
        "drivetrain": "AWD",
        "fuel_type": "Plug-In Hybrid",
    },
    # Kia
    ("kia", "seltos"): {
        "cylinders": 4,
        "transmission": "8-Speed Dual-Clutch",
        "drivetrain": "AWD",
    },
    ("kia", "optima plug-in hybrid"): {
        "cylinders": 4,
        "transmission": "6-Speed Automatic",
        "drivetrain": "FWD",
        "fuel_type": "Plug-In Hybrid",
    },
    # Toyota
    ("toyota", "rav4"): {
        "cylinders": 4,
        "transmission": "8-Speed Automatic",
        "drivetrain": "AWD",
    },
    ("toyota", "tacoma 4wd"): {
        "cylinders": 4,
        "transmission": "8-Speed Automatic",
        "drivetrain": "4WD",
    },
    ("toyota", "tacoma"): {
        "cylinders": 4,
        "transmission": "8-Speed Automatic",
        "drivetrain": "4WD",
    },
    # Lexus
    ("lexus", "rx 350"): {
        "cylinders": 6,
        "transmission": "8-Speed Automatic",
        "drivetrain": "AWD",
    },
    # Mercedes-Benz
    ("mercedes-benz", "gle"): {
        "cylinders": 4,
        "transmission": "9-Speed Automatic",
        "drivetrain": "AWD",
    },
    ("mercedes-benz", "glb"): {
        "cylinders": 4,
        "transmission": "8-Speed Automatic",
        "drivetrain": "AWD",
    },
    # GMC
    ("gmc", "canyon"): {
        "cylinders": 4,
        "transmission": "8-Speed Automatic",
        "drivetrain": "4WD",
    },
}

_VARIANT_SUFFIX_RES = (
    re.compile(r"\s+(plug[\s-]?in\s+)?hybrid\s*$", re.I),
    re.compile(r"\s+plug[\s-]?in\s+hybrid\s*$", re.I),
    re.compile(r"\s+4xe\s*$", re.I),
    re.compile(r"\s+phev\s*$", re.I),
    re.compile(r"\s+electric\s*$", re.I),
)


def iter_model_lookup_variants(model: str) -> list[str]:
    """Original model plus stripped suffixes (e.g. Elantra Hybrid → Elantra)."""
    m = (model or "").strip()
    if not m:
        return []
    seen: set[str] = set()
    out: list[str] = []
    for candidate in (m, m.lower(), m.title()):
        c = candidate.strip()
        if c and c.lower() not in seen:
            seen.add(c.lower())
            out.append(c)
    lower = m.lower()
    cur = lower
    for rx in _VARIANT_SUFFIX_RES:
        nxt = rx.sub("", cur).strip()
        if nxt and nxt != cur:
            for candidate in (nxt, nxt.title()):
                if candidate.lower() not in seen:
                    seen.add(candidate.lower())
                    out.append(candidate)
            cur = nxt
    return out


def _query_sqlite_row(canonical_make: str, model_variant: str) -> dict[str, Any] | None:
    conn = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT cylinders, transmission, drivetrain, body_style, fuel_type
            FROM model_specs
            WHERE lower(make) = lower(?) AND lower(model) = lower(?)
            LIMIT 1
            """,
            (canonical_make.strip(), model_variant.strip()),
        )
        row = cur.fetchone()
        if not row:
            return None
        cyl, trans, drv, body, fuel = row
        d: dict[str, Any] = {}
        if cyl is not None:
            d["cylinders"] = int(cyl)
        if trans and str(trans).strip():
            d["transmission"] = str(trans).strip()
        if drv and str(drv).strip():
            # Stored values may be long-form; merge layer accepts short codes — normalize
            d["drivetrain"] = _short_drivetrain(str(drv).strip())
        if body and str(body).strip():
            d["body_style"] = str(body).strip()
        if fuel and str(fuel).strip():
            d["fuel_type"] = str(fuel).strip()
        return d if d else None
    except Exception:
        return None
    finally:
        if conn is not None:
            conn.close()


_SHORT_DRIVE_MAP = {
    "fwd": "FWD",
    "front-wheel drive": "FWD",
    "rwd": "RWD",
    "rear-wheel drive": "RWD",
    "awd": "AWD",
    "all-wheel drive": "AWD",
    "4wd": "4WD",
    "four-wheel drive": "4WD",
}


def _short_drivetrain(s: str) -> str:
    k = s.strip().lower()
    return _SHORT_DRIVE_MAP.get(k, s)


def lookup_model_specs_dictionary(make: str | None, model: str | None) -> dict[str, Any] | None:
    """
    Return specs dict with optional keys: cylinders, transmission, drivetrain (short code),
    body_style, fuel_type. None if nothing found.
    """
    mk = (make or "").strip()
    md = (model or "").strip()
    if not mk or not md:
        return None

    try:
        from backend.scanner.database import _resolve_canonical_make

        canon = _resolve_canonical_make(mk, md)
    except Exception:
        canon = mk

    for variant in iter_model_lookup_variants(md):
        row = _query_sqlite_row(canon, variant)
        if row:
            return row

    mk_l = mk.lower()
    for variant in iter_model_lookup_variants(md):
        fb = _MODEL_FALLBACK.get((mk_l, variant.lower()))
        if fb:
            out = dict(fb)
            return out

    # Alias: Jeep Wrangler 4xe → wrangler 4xe already in FALLBACK; Elantra Hybrid variants
    merged = f"{md}".lower()
    if mk_l == "hyundai" and "elantra" in merged and "hybrid" in merged:
        return dict(_MODEL_FALLBACK[("hyundai", "elantra hybrid")])

    return None
