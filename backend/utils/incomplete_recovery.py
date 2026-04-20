"""
Incomplete listing recovery: scoring, Mazda-safe deterministic rules, and merge policy.

Source order (caller): inventory embedded JSON → VDP extraction → deterministic inference.
Never invent colors or images; only merge HTTP image URLs from inventory/VDP.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

from backend.parsers import parse as parse_inventory
from backend.parsers.vdp_urls import (
    dealer_style_vdp_url_candidates,
    looks_like_real_vin,
    suggest_dealer_style_vdp_url,
)
from backend.utils.field_clean import clean_car_row_dict, is_effectively_empty, normalize_optional_str

logger = logging.getLogger(__name__)

# Fields we try to repair (medium-priority gaps typical of “incomplete but good skeleton” rows).
RECOVERY_VALUE_FIELDS: tuple[str, ...] = (
    "engine_description",
    "drivetrain",
    "transmission",
    "condition",
    "cylinders",
    "mpg_city",
    "mpg_highway",
    "exterior_color",
    "interior_color",
    "body_style",
)

INVENTORY_INDEX_PATHS: tuple[str, ...] = (
    "/used-inventory/index.htm",
    "/new-inventory/index.htm",
    "/certified-inventory/index.htm",
)

_PLACEHOLDER_IMG = "/static/placeholder.svg"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def has_real_vehicle_image(car: dict[str, Any]) -> bool:
    """True when at least one non-placeholder HTTP(S) image exists."""
    img = car.get("image_url")
    if isinstance(img, str) and img.strip().startswith("http"):
        return True
    g = car.get("gallery")
    if isinstance(g, list):
        for u in g:
            if isinstance(u, str) and u.strip().startswith("http"):
                return True
    if isinstance(g, str) and "http" in g:
        try:
            arr = json.loads(g)
            if isinstance(arr, list):
                for u in arr:
                    if isinstance(u, str) and u.strip().startswith("http"):
                        return True
        except (json.JSONDecodeError, TypeError):
            pass
    return False


def _missing_recovery_field(val: Any, key: str) -> bool:
    if key == "body_style":
        return val is None or is_effectively_empty(val)
    if key in ("mpg_city", "mpg_highway", "cylinders"):
        if val is None:
            return True
        try:
            if int(val) <= 0 and key != "cylinders":
                return True
            if key == "cylinders" and int(val) <= 0:
                return True
        except (TypeError, ValueError):
            return True
        return False
    return val is None or is_effectively_empty(val)


def count_recovery_missing(car: dict[str, Any]) -> int:
    n = 0
    for k in RECOVERY_VALUE_FIELDS:
        if _missing_recovery_field(car.get(k), k):
            n += 1
    if not has_real_vehicle_image(car):
        n += 1
    return n


def compute_recovery_metrics(car: dict[str, Any]) -> dict[str, Any]:
    vin = (car.get("vin") or "").strip().upper()
    stock = (car.get("stock_number") or "").strip()
    du = (car.get("source_url") or "").strip()
    base = (car.get("dealer_url") or "").strip()
    has_detail_url = bool(du.startswith("http")) or (
        bool(base.startswith("http")) and looks_like_real_vin(vin)
    )
    mm = (car.get("make") or "").strip() and (car.get("model") or "").strip()
    trim_ok = bool((car.get("trim") or "").strip())
    has_mmt = bool(mm and trim_ok)

    missing = count_recovery_missing(car)
    # 0–100 heuristic: identifiers + skeleton quality − missing density
    score = 0.0
    if looks_like_real_vin(vin):
        score += 30.0
    if stock:
        score += 18.0
    if base.startswith("http"):
        score += 15.0
    if has_detail_url:
        score += 12.0
    if mm:
        score += 15.0
    if trim_ok:
        score += 5.0
    score -= min(45.0, float(missing) * 4.5)
    score = max(0.0, min(100.0, score))

    return {
        "has_vin": looks_like_real_vin(vin),
        "has_stock": bool(stock),
        "has_detail_url": has_detail_url,
        "has_make_model_trim": has_mmt,
        "missing_field_count": missing,
        "recoverability_score": round(score, 2),
    }


def promotion_eligible(car: dict[str, Any]) -> bool:
    """
    After repair: eligible for “normal” listings if core commerce fields exist,
    price > 0, and at least one real image URL (not placeholder-only).
    """
    if is_effectively_empty(car.get("title")):
        return False
    if car.get("year") in (None, 0):
        return False
    if is_effectively_empty(car.get("make")) or is_effectively_empty(car.get("model")):
        return False
    try:
        p = float(car.get("price") or 0)
    except (TypeError, ValueError):
        p = 0.0
    if p <= 0:
        return False
    if not has_real_vehicle_image(car):
        return False
    return True


def _better_str(old: Any, new: Any) -> bool:
    o = normalize_optional_str(old) if old is not None else None
    n = normalize_optional_str(new) if new is not None else None
    if not n:
        return False
    if not o:
        return True
    return len(n) > len(o)


def merge_recovery_patch(
    current: dict[str, Any],
    incoming: dict[str, Any],
    *,
    source_tag: str,
    notes: list[str],
) -> dict[str, Any]:
    """
    Return a dict of fields to apply (only higher-signal updates).
    Does not mutate *current*.
    """
    out: dict[str, Any] = {}
    for k in RECOVERY_VALUE_FIELDS:
        if k not in incoming:
            continue
        nv = incoming.get(k)
        if nv is None:
            continue
        if k == "body_style":
            s = normalize_optional_str(nv) if isinstance(nv, str) else nv
            if s and is_effectively_empty(current.get("body_style")):
                out[k] = s
                notes.append(f"{source_tag}:body_style")
            continue
        if k in ("mpg_city", "mpg_highway", "cylinders"):
            try:
                iv = int(nv)
            except (TypeError, ValueError):
                continue
            if iv <= 0 and k != "cylinders":
                continue
            if k == "cylinders" and iv <= 0:
                continue
            if _missing_recovery_field(current.get(k), k):
                out[k] = iv
                notes.append(f"{source_tag}:{k}={iv}")
        else:
            s = normalize_optional_str(nv) if isinstance(nv, str) else nv
            if s is None or is_effectively_empty(s):
                continue
            if _better_str(current.get(k), s):
                out[k] = s
                notes.append(f"{source_tag}:{k}")

    # Images: only HTTP URLs from incoming
    for img_key in ("image_url",):
        v = incoming.get(img_key)
        if isinstance(v, str) and v.strip().startswith("http"):
            cur = current.get(img_key)
            if not isinstance(cur, str) or not cur.strip().startswith("http"):
                out[img_key] = v.strip()
                notes.append(f"{source_tag}:image_url")
    gl = incoming.get("gallery")
    if isinstance(gl, list) and gl:
        http_urls = [u for u in gl if isinstance(u, str) and u.strip().startswith("http")]
        if http_urls:
            cur_g = current.get("gallery")
            cur_http = False
            if isinstance(cur_g, list):
                cur_http = any(isinstance(u, str) and u.startswith("http") for u in cur_g)
            elif isinstance(cur_g, str):
                cur_http = "http" in cur_g
            if not cur_http:
                out["gallery"] = http_urls[:24]
                if "image_url" not in out:
                    out["image_url"] = http_urls[0]
                notes.append(f"{source_tag}:gallery")

    # Price / title when clearly better
    if incoming.get("price"):
        try:
            ip = float(incoming["price"])
            cp = float(current.get("price") or 0)
            if ip > 0 and cp <= 0:
                out["price"] = int(round(ip))
                notes.append(f"{source_tag}:price")
        except (TypeError, ValueError):
            pass
    if incoming.get("title") and _better_str(current.get("title"), incoming.get("title")):
        out["title"] = normalize_optional_str(incoming.get("title"))
        notes.append(f"{source_tag}:title")

    for ek in ("zip_code", "fuel_type"):
        if ek not in incoming:
            continue
        ev = incoming.get(ek)
        if ek == "zip_code":
            zs = normalize_optional_str(ev)
            if zs and is_effectively_empty(current.get("zip_code")):
                out["zip_code"] = zs
                notes.append(f"{source_tag}:{ek}")
            continue
        if isinstance(ev, str) and normalize_optional_str(ev) and is_effectively_empty(current.get(ek)):
            out[ek] = normalize_optional_str(ev)
            notes.append(f"{source_tag}:{ek}")

    return out


def mazda_deterministic_patch(car: dict[str, Any], notes: list[str]) -> dict[str, Any]:
    """Conservative Mazda-only rules; never fabricate colors/images."""
    make = (car.get("make") or "").strip().upper()
    if make != "MAZDA":
        return {}
    patch: dict[str, Any] = {}
    model_l = (car.get("model") or "").lower().strip()
    trim = (car.get("trim") or "") + " " + (car.get("title") or "")
    trim_l = trim.lower()
    title_l = (car.get("title") or "").lower()
    src_l = ((car.get("source_url") or "") + " " + (car.get("dealer_url") or "")).lower()

    # Body style
    if ("cx-5" in model_l or "cx5" in model_l.replace(" ", "")) and is_effectively_empty(car.get("body_style")):
        patch["body_style"] = "SUV"
        notes.append("mazda:cx5_body_suv")

    # Drivetrain from trim
    if is_effectively_empty(car.get("drivetrain")):
        if re.search(r"\bawd\b", trim_l) or "i-active awd" in trim_l:
            patch["drivetrain"] = "AWD"
            notes.append("mazda:trim_awd")
        elif re.search(r"\bfwd\b", trim_l) or "front-wheel" in trim_l:
            patch["drivetrain"] = "FWD"
            notes.append("mazda:trim_fwd")

    # Condition: Certified / CPO
    cond_in = (car.get("condition") or "").strip()
    if is_effectively_empty(cond_in):
        if "certified" in trim_l or "cpo" in trim_l or "mazda certified" in title_l:
            patch["condition"] = "Certified Pre-Owned"
            notes.append("mazda:condition_cpo_title_trim")
        elif title_l.startswith("used "):
            patch["condition"] = "Used"
            notes.append("mazda:condition_used_title_prefix")
        elif "/used-inventory/" in src_l:
            patch["condition"] = "Used"
            notes.append("mazda:condition_used_inventory_path")
        elif "/new-inventory/" in src_l:
            patch["condition"] = "New"
            notes.append("mazda:condition_new_inventory_path")

    # Engine + cylinders: Skyactiv-G 2.5 non-turbo CX-5 gasoline
    fuel_l = (car.get("fuel_type") or "").lower()
    gas = "gas" in fuel_l or "unlead" in fuel_l or "petrol" in fuel_l
    if ("cx-5" in model_l or "cx5" in model_l.replace(" ", "")) and gas:
        if "turbo" not in trim_l and "2.5" in trim_l:
            if is_effectively_empty(car.get("engine_description")):
                patch["engine_description"] = "2.5L SKYACTIV-G I4"
                notes.append("mazda:engine_cx5_25")
            if car.get("cylinders") in (None, 0):
                patch["cylinders"] = 4
                notes.append("mazda:cylinders_cx5_25_family")

    return patch


def try_fetch_inventory_vehicle(car: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    """
    Fetch Dealer.com-style inventory index pages and parse embedded JSON / HTML.
    Returns (merged_vehicle_dict_or_empty, note_lines).
    """
    notes: list[str] = []
    base = (car.get("dealer_url") or "").strip().rstrip("/")
    if not base.startswith("http"):
        return {}, notes
    want_vin = (car.get("vin") or "").strip().upper()
    if not looks_like_real_vin(want_vin):
        return {}, notes

    dealer_id = (car.get("dealer_id") or "").strip() or "recovery"
    dealer_name = (car.get("dealer_name") or "").strip() or "Dealer"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    for path in INVENTORY_INDEX_PATHS:
        url = base + path
        try:
            r = requests.get(url, headers=headers, timeout=35)
        except requests.RequestException as e:
            notes.append(f"inventory_http_error:{path}:{str(e)[:180]}")
            continue
        if r.status_code != 200:
            notes.append(f"inventory_status:{path}:{r.status_code}")
            continue
        try:
            vehicles = parse_inventory(
                "dealer_dot_com",
                r.text,
                base_url=base,
                dealer_id=dealer_id,
                dealer_name=dealer_name,
                dealer_url=base,
            )
        except Exception as e:
            notes.append(f"inventory_parse_error:{path}:{e!s}"[:200])
            continue
        for v in vehicles:
            if not isinstance(v, dict):
                continue
            vvin = (v.get("vin") or "").strip().upper()
            if vvin == want_vin:
                notes.append(f"inventory_match:{path}")
                return v, notes
    notes.append("inventory_no_vin_match")
    return {}, notes


def prepare_vdp_urls(vehicle: dict[str, Any]) -> tuple[str | None, list[str]]:
    """Primary VDP URL + alternates (Dealer.com-style)."""
    vin = (vehicle.get("vin") or "").strip().upper()
    base = (vehicle.get("dealer_url") or "").strip().rstrip("/")
    if not looks_like_real_vin(vin) or not base.startswith("http"):
        return None, []
    su = (vehicle.get("source_url") or "").strip()
    if su.startswith("http"):
        primary = su
    else:
        primary = suggest_dealer_style_vdp_url(base, vin, vehicle)
    alts = [u for u in dealer_style_vdp_url_candidates(base, vin, vehicle) if u and u != primary]
    return primary, alts[:12]


async def recover_vehicle_vdp_async(page: Any, vehicle: dict[str, Any], dealer_name: str) -> dict[str, Any]:
    """Run scanner VDP extraction for one row (mutates *vehicle*)."""
    root = Path(__file__).resolve().parents[2]
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    from scanner_vdp import _vdp_visit_one  # noqa: WPS433 — runtime path for project root

    vin = (vehicle.get("vin") or "").strip().upper()
    primary, alts = prepare_vdp_urls(vehicle)
    if not primary or not primary.startswith("http"):
        return {"enriched": False, "reason": "no_vdp_url", "visited": 0}
    vehicle["_detail_url_alternates"] = alts
    preview_lock = asyncio.Lock()
    preview_budget = [2]
    return await _vdp_visit_one(page, dealer_name, vehicle, primary, vin, preview_lock, preview_budget)


def finalize_recovery_status(
    before_missing: int,
    after_missing: int,
    any_updates: bool,
) -> tuple[str, str]:
    """Return (recovery_status, short summary)."""
    if not any_updates:
        return "unrecoverable", "no_field_updates"
    if after_missing == 0:
        return "recovered", f"missing {before_missing}->{after_missing}"
    if after_missing < before_missing:
        return "partially_recovered", f"missing {before_missing}->{after_missing}"
    return "unrecoverable", "no_reduction_in_missing_fields"


def apply_patches_to_dict(car: dict[str, Any], patches: dict[str, Any]) -> None:
    """Apply non-None patch entries to car dict (in memory)."""
    for k, v in patches.items():
        if v is None:
            continue
        car[k] = v


def row_after_cleanup(car: dict[str, Any]) -> dict[str, Any]:
    return clean_car_row_dict(dict(car))


def preserve_placeholder_image_if_no_http(before: dict[str, Any], clean: dict[str, Any]) -> None:
    """
    ``clean_car_row_dict`` maps non-HTTP ``image_url`` to None. If we did not recover a real URL,
    keep the listing placeholder so the row does not regress.
    """
    bimg = before.get("image_url")
    cimg = clean.get("image_url")
    if isinstance(bimg, str) and bimg.startswith("/static/"):
        if not (isinstance(cimg, str) and cimg.startswith("http")):
            clean["image_url"] = bimg
    g_clean = clean.get("gallery")
    has_http_g = isinstance(g_clean, list) and any(
        isinstance(u, str) and u.startswith("http") for u in g_clean
    )
    g0 = before.get("gallery")
    if not has_http_g and isinstance(g0, list) and g0:
        clean["gallery"] = g0
