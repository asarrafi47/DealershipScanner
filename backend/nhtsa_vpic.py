"""
NHTSA vPIC ``DecodeVinValuesExtended`` (official US VIN decode).

Used only to fill **missing or placeholder** SQLite ``cars`` columns; provenance is merged
into existing ``spec_source_json`` (same pattern as ``backend.spec_backfill``).

API: https://vpic.nhtsa.dot.gov/api/vehicles/decodevinvaluesextended/{vin}?format=json
"""
from __future__ import annotations

import json
import logging
import re
import urllib.error
import urllib.request
from typing import Any, Callable

log = logging.getLogger(__name__)

_VIN17 = re.compile(r"^[A-HJ-NPR-Z0-9]{17}$", re.IGNORECASE)
_VPIC_BASE = "https://vpic.nhtsa.dot.gov/api/vehicles/decodevinvaluesextended/{vin}?format=json"

# vPIC returns these when NHTSA has no value — do not treat as real data.
_VPIC_EMPTY_TOKENS = frozenset(
    {
        "",
        "not applicable",
        "0",
        "null",
        "undefined",
    }
)


def looks_like_decode_vin(vin: str | None) -> bool:
    """True for a 17-char VIN suitable for vPIC (excludes UNKNOWN* placeholders)."""
    v = (vin or "").strip().upper()
    if not v or v.startswith("UNKNOWN"):
        return False
    return bool(_VIN17.match(v))


def _vpic_scalar_empty(val: Any) -> bool:
    if val is None:
        return True
    s = str(val).strip()
    if not s:
        return True
    return s.strip().lower() in _VPIC_EMPTY_TOKENS


def _pretty_make_model(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return s
    u = s.upper()
    known = {
        "BMW": "BMW",
        "GMC": "GMC",
        "RAM": "Ram",
        "VW": "Volkswagen",
        "MINI": "MINI",
        "JEEP": "Jeep",
        "GMC TRUCK": "GMC",
    }
    if u in known:
        return known[u]
    return s.title()


def _normalize_drivetrain(raw: str) -> str | None:
    if _vpic_scalar_empty(raw):
        return None
    u = raw.strip().upper()
    if "AWD" in u or "ALL-WHEEL" in u:
        return "AWD"
    if "4WD" in u or "4-WHEEL" in u or "4X4" in u:
        return "4WD"
    if "FWD" in u or "FRONT-WHEEL" in u:
        return "FWD"
    if "RWD" in u or "REAR-WHEEL" in u:
        return "RWD"
    return raw.strip()


def _build_engine_description(flat: dict[str, str]) -> str | None:
    parts: list[str] = []
    disp = flat.get("DisplacementL") or flat.get("DisplacementCI") or ""
    if not _vpic_scalar_empty(disp):
        parts.append(str(disp).strip())
    eng = flat.get("EngineModel") or ""
    if not _vpic_scalar_empty(eng):
        parts.append(str(eng).strip())
    cfg = flat.get("EngineConfiguration") or ""
    if not _vpic_scalar_empty(cfg):
        parts.append(str(cfg).strip())
    turbo = (flat.get("Turbo") or "").strip().upper()
    if turbo in ("Y", "YES", "1", "TRUE"):
        parts.append("Turbo")
    if not parts:
        return None
    return " ".join(parts)[:240]


def flat_vpic_result_to_car_patch(flat: dict[str, str]) -> dict[str, Any]:
    """
    Map vPIC ``Results[0]`` string dict to ``cars`` column names (values only; caller filters fillable).
    """
    out: dict[str, Any] = {}
    my = flat.get("ModelYear") or ""
    if not _vpic_scalar_empty(my):
        try:
            y = int(float(str(my).strip()))
            if 1900 <= y <= 2100:
                out["year"] = y
        except (TypeError, ValueError):
            pass
    mk = flat.get("Make") or ""
    if not _vpic_scalar_empty(mk):
        out["make"] = _pretty_make_model(mk)
    md = flat.get("Model") or ""
    if not _vpic_scalar_empty(md):
        out["model"] = _pretty_make_model(md)
    tr = (flat.get("Trim") or "").strip()
    tr2 = (flat.get("Trim2") or "").strip()
    trim_parts = [p for p in (tr, tr2) if p and not _vpic_scalar_empty(p)]
    if trim_parts:
        out["trim"] = " ".join(trim_parts)[:160]
    ftp = flat.get("FuelTypePrimary") or ""
    fts = flat.get("FuelTypeSecondary") or ""
    if not _vpic_scalar_empty(ftp):
        if not _vpic_scalar_empty(fts):
            out["fuel_type"] = f"{ftp.strip()} / {fts.strip()}"[:120]
        else:
            out["fuel_type"] = ftp.strip()[:120]
    dt = flat.get("DriveType") or ""
    nd = _normalize_drivetrain(dt)
    if nd:
        out["drivetrain"] = nd
    ts = flat.get("TransmissionStyle") or ""
    tsp = flat.get("TransmissionSpeeds") or ""
    if not _vpic_scalar_empty(ts):
        if not _vpic_scalar_empty(tsp):
            out["transmission"] = f"{tsp.strip()}-Speed {ts.strip()}"[:160]
        else:
            out["transmission"] = ts.strip()[:160]
    ec = flat.get("EngineCylinders") or ""
    if not _vpic_scalar_empty(ec):
        try:
            c = int(float(str(ec).strip()))
            if c >= 0:
                out["cylinders"] = c
        except (TypeError, ValueError):
            pass
    bc = flat.get("BodyClass") or ""
    if not _vpic_scalar_empty(bc):
        out["body_style"] = bc.strip()[:120]
    ed = _build_engine_description(flat)
    if ed:
        out["engine_description"] = ed
    return out


def decode_vpic_http_response(body: dict[str, Any]) -> dict[str, str] | None:
    """Return the first ``Results`` row as str→str, or None if unusable."""
    results = body.get("Results")
    if not isinstance(results, list) or not results:
        return None
    row0 = results[0]
    if not isinstance(row0, dict):
        return None
    return {str(k): str(v) if v is not None else "" for k, v in row0.items()}


def fetch_decode_vin_values_extended(
    vin: str,
    *,
    get_json: Callable[[str], dict[str, Any]] | None = None,
    timeout_s: float = 25.0,
) -> tuple[dict[str, Any] | None, dict[str, str] | None, str | None]:
    """
    Return ``(raw_api_body, flat_first_row, error_message)``.

    *raw_api_body* is the parsed JSON dict from vPIC (suitable for SQLite cache); *flat_first_row*
    is ``Results[0]`` as str→str. *error_message* is None only when *flat_first_row* is usable.
    """
    v = (vin or "").strip().upper()
    if not looks_like_decode_vin(v):
        return None, None, "invalid_vin"

    def _default_get(url: str) -> dict[str, Any]:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "DealershipScanner-structured-spec-backfill/1.0"},
            method="GET",
        )
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
        return json.loads(raw)

    getter = get_json or _default_get
    url = _VPIC_BASE.format(vin=v)
    try:
        body = getter(url)
    except urllib.error.HTTPError as e:
        log.warning("vPIC HTTP %s for vin=%s", e.code, v[:8])
        return None, None, f"http_{e.code}"
    except urllib.error.URLError as e:
        log.warning("vPIC URL error vin=%s: %s", v[:8], e)
        return None, None, "url_error"
    except (json.JSONDecodeError, TypeError, ValueError) as e:
        log.warning("vPIC JSON error vin=%s: %s", v[:8], e)
        return None, None, "json_error"
    except Exception as e:
        log.warning("vPIC error vin=%s: %s", v[:8], e)
        return None, None, type(e).__name__

    if not isinstance(body, dict):
        return None, None, "invalid_body"

    flat = decode_vpic_http_response(body)
    if not flat:
        return body, None, "no_results"
    err = (flat.get("ErrorText") or "").strip()
    if err and not any(
        not _vpic_scalar_empty(flat.get(k)) for k in ("Make", "Model", "ModelYear")
    ):
        return body, None, "decode_error_no_make"
    return body, flat, None
