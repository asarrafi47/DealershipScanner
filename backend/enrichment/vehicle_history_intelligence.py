"""
Free government vehicle-history intelligence (NHTSA recalls + vPIC + listing highlights).

Premium car-detail surface only — no Carfax scraping.
"""
from __future__ import annotations

import json
import logging
import re
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from backend.enrichment.knowledge_engine import decode_trim_logic, merge_verified_specs
from backend.enrichment.nhtsa_vpic import (
    fetch_decode_vin_values_extended,
    looks_like_decode_vin,
)

log = logging.getLogger(__name__)

_RECALLS_YMM_URL = (
    "https://api.nhtsa.gov/recalls/recallsByVehicle"
    "?make={make}&model={model}&modelYear={year}&format=json"
)
_TITLE_FLAG_KEYS = (
    ("salvageHistory", "Salvage title history"),
    ("salvage", "Salvage"),
    ("titleIssues", "Title issues"),
    ("lemonHistory", "Lemon history"),
    ("accidentCount", "Accidents reported"),
    ("accidents", "Accidents"),
    ("frameRepairs", "Frame damage"),
    ("ownerCount", "Owner count"),
    ("numberOfOwners", "Owner count"),
)


def _parse_recall_rows(body: Any) -> list[dict[str, Any]]:
    if not isinstance(body, dict):
        return []
    results = body.get("results") or body.get("Results") or []
    if not isinstance(results, list):
        return []
    out: list[dict[str, Any]] = []
    for row in results[:12]:
        if not isinstance(row, dict):
            continue
        component = str(row.get("Component") or row.get("component") or "").strip()
        summary = str(row.get("Summary") or row.get("summary") or "").strip()
        campaign = str(row.get("NHTSACampaignNumber") or row.get("CampaignNumber") or "").strip()
        out.append(
            {
                "campaign": campaign,
                "component": component,
                "summary": summary[:280] if summary else "",
            }
        )
    return out


def _parse_nhtsa_recall_http_payload(raw: str) -> tuple[list[dict[str, Any]], str | None]:
    """Parse NHTSA recallsByVehicle JSON; None error means payload was understood."""
    try:
        body = json.loads(raw)
    except json.JSONDecodeError:
        return [], "invalid_response"
    if not isinstance(body, dict):
        return [], "invalid_response"
    if "results" in body or "Results" in body or "Message" in body or "Count" in body:
        return _parse_recall_rows(body), None
    return [], "invalid_response"


def _fetch_recalls_by_ymm(
    make: str,
    model: str,
    year: int | str,
    *,
    timeout_s: float = 20.0,
) -> tuple[list[dict[str, Any]], str | None]:
    mk = (make or "").strip()
    md = (model or "").strip()
    try:
        yr = int(str(year or "").strip())
    except ValueError:
        return [], "invalid_year"
    if not mk or not md or yr < 1980 or yr > 2100:
        return [], "invalid_ymm"
    url = _RECALLS_YMM_URL.format(
        make=urllib.parse.quote(mk.lower(), safe=""),
        model=urllib.parse.quote(md.lower(), safe=""),
        year=urllib.parse.quote(str(yr), safe=""),
    )
    try:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "SarrafiCollection-vehicle-history/1.0"},
            method="GET",
        )
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        raw = ""
        try:
            raw = e.read().decode("utf-8", errors="replace")
        except Exception:
            pass
        recalls, parse_err = _parse_nhtsa_recall_http_payload(raw)
        if parse_err is None:
            return recalls, None
        log.warning("NHTSA recalls HTTP %s ymm=%s|%s|%s", e.code, mk[:12], md[:12], yr)
        return [], f"http_{e.code}"
    except Exception as e:
        log.warning("NHTSA recalls error ymm=%s|%s|%s: %s", mk[:12], md[:12], yr, e)
        return [], type(e).__name__
    return _parse_nhtsa_recall_http_payload(raw)


def fetch_nhtsa_recalls(
    vin: str,
    *,
    make: str | None = None,
    model: str | None = None,
    year: int | str | None = None,
    timeout_s: float = 20.0,
) -> tuple[list[dict[str, Any]], str | None]:
    """NHTSA recalls for this vehicle configuration (make/model/year).

    The federal API does not expose per-VIN repair status. When a VIN is present we prefer vPIC
    make/model/year because listing titles (e.g. "V60 Cross Country") often do not match NHTSA tokens.
    """
    vin_norm = (vin or "").strip().upper()
    ymm_make = (make or "").strip() or None
    ymm_model = (model or "").strip() or None
    ymm_year = year
    if looks_like_decode_vin(vin_norm):
        _body, flat, dec_err = fetch_decode_vin_values_extended(vin_norm, timeout_s=timeout_s)
        if flat and not dec_err:
            ymm_make = (flat.get("Make") or "").strip() or ymm_make
            ymm_model = (flat.get("Model") or "").strip() or ymm_model
            ymm_year = (flat.get("ModelYear") or "").strip() or ymm_year
        elif not (ymm_make and ymm_model and ymm_year):
            return [], dec_err or "decode_failed"
    elif not (ymm_make and ymm_model and ymm_year):
        return [], "invalid_vin"
    return _fetch_recalls_by_ymm(ymm_make or "", ymm_model or "", ymm_year or "", timeout_s=timeout_s)


def _history_highlight_flags(highlights: Any) -> list[dict[str, str]]:
    flags: list[dict[str, str]] = []
    if not highlights:
        return flags

    def _add(label: str, value: str, *, severity: str = "info") -> None:
        v = (value or "").strip()
        if not v or v.lower() in ("n/a", "na", "none", "unknown", "0", "—", "-"):
            return
        flags.append({"label": label, "value": v, "severity": severity})

    items: list[Any]
    if isinstance(highlights, str):
        try:
            items = json.loads(highlights)
        except (json.JSONDecodeError, TypeError):
            items = [highlights]
    elif isinstance(highlights, list):
        items = highlights
    else:
        return flags

    for item in items:
        if isinstance(item, dict):
            for key, label in _TITLE_FLAG_KEYS:
                if key in item:
                    val = item[key]
                    sev = "warning" if key in ("salvageHistory", "titleIssues", "lemonHistory", "frameRepairs") else "info"
                    _add(label, str(val), severity=sev)
            if "label" in item and "value" in item:
                lbl = str(item.get("label") or "").strip()
                val = str(item.get("value") or "").strip()
                if lbl and val:
                    low = f"{lbl} {val}".lower()
                    sev = "warning" if any(x in low for x in ("salvage", "title", "lemon", "frame", "accident")) else "info"
                    _add(lbl, val, severity=sev)
        elif isinstance(item, str):
            s = item.strip()
            if not s:
                continue
            m = re.match(r"^([^:]+):\s*(.+)$", s)
            if m:
                lbl, val = m.group(1).strip(), m.group(2).strip()
                low = f"{lbl} {val}".lower()
                sev = "warning" if any(x in low for x in ("salvage", "title", "lemon", "frame")) else "info"
                _add(lbl, val, severity=sev)

    return flags


def build_vehicle_history_intelligence(car: dict[str, Any]) -> dict[str, Any]:
    """
    Aggregate listing history highlights, NHTSA open recalls, and vPIC/knowledge-engine validation.
  """
    vin = (car.get("vin") or "").strip().upper()
    out: dict[str, Any] = {
        "ok": False,
        "vin": vin,
        "flags": [],
        "recalls": [],
        "government_validation": {},
        "error": None,
    }
    if not looks_like_decode_vin(vin):
        out["error"] = "invalid_vin"
        return out

    highlight_flags = _history_highlight_flags(car.get("history_highlights"))
    recalls, recall_err = fetch_nhtsa_recalls(vin)
    out["recalls"] = recalls

    _, vpic_flat, vpic_err = fetch_decode_vin_values_extended(vin)
    verified = merge_verified_specs(car)
    regex = decode_trim_logic(
        car.get("make"),
        car.get("model"),
        car.get("trim"),
        car.get("title"),
    )

    gov: dict[str, Any] = {
        "vpic_make": (vpic_flat or {}).get("Make"),
        "vpic_model": (vpic_flat or {}).get("Model"),
        "vpic_model_year": (vpic_flat or {}).get("ModelYear"),
        "vpic_vehicle_type": (vpic_flat or {}).get("VehicleType"),
        "vpic_body_class": (vpic_flat or {}).get("BodyClass"),
        "verified_drivetrain": verified.get("drivetrain"),
        "verified_fuel_type": verified.get("fuel_type"),
        "trim_decoder_drivetrain": regex.get("drivetrain"),
    }
    out["government_validation"] = {k: v for k, v in gov.items() if v not in (None, "", "—")}

    flags: list[dict[str, str]] = list(highlight_flags)
    if recalls:
        flags.insert(
            0,
            {
                "label": "NHTSA open recalls",
                "value": f"{len(recalls)} campaign{'s' if len(recalls) != 1 else ''}",
                "severity": "warning",
            },
        )
    elif recall_err:
        flags.append(
            {
                "label": "NHTSA recalls lookup",
                "value": "Temporarily unavailable",
                "severity": "info",
            }
        )
    else:
        flags.append(
            {
                "label": "NHTSA open recalls",
                "value": "None reported for this VIN",
                "severity": "info",
            }
        )

    if vpic_flat and not vpic_err:
        ymm = " ".join(
            x
            for x in (
                str(gov.get("vpic_model_year") or "").strip(),
                str(gov.get("vpic_make") or "").strip(),
                str(gov.get("vpic_model") or "").strip(),
            )
            if x
        )
        if ymm:
            flags.append({"label": "NHTSA VIN decode", "value": ymm, "severity": "info"})
    elif vpic_err:
        flags.append(
            {
                "label": "NHTSA VIN decode",
                "value": "Unavailable",
                "severity": "info",
            }
        )

    listing_make = (car.get("make") or "").strip()
    listing_model = (car.get("model") or "").strip()
    vpic_make = str(gov.get("vpic_make") or "").strip()
    vpic_model = str(gov.get("vpic_model") or "").strip()
    if listing_make and vpic_make and listing_make.lower() != vpic_make.lower():
        flags.append(
            {
                "label": "Listing vs NHTSA make",
                "value": f"Listing: {listing_make} · NHTSA: {vpic_make}",
                "severity": "warning",
            }
        )
    if listing_model and vpic_model and listing_model.lower() != vpic_model.lower():
        flags.append(
            {
                "label": "Listing vs NHTSA model",
                "value": f"Listing: {listing_model} · NHTSA: {vpic_model}",
                "severity": "warning",
            }
        )

    out["flags"] = flags
    out["ok"] = True
    return out
