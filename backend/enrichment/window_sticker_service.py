"""
Fetch OEM window sticker PDFs, store locally, parse options, and merge into ``cars.packages``.

Premium car detail pages embed the stored PDF and show consolidated package data.
"""
from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backend.db.inventory_db import get_car_by_id, update_car_row_partial
from backend.utils.field_clean import is_effectively_empty
from backend.utils.spec_provenance import merge_spec_source_json

logger = logging.getLogger(__name__)

_ROOT = Path(__file__).resolve().parents[2]
CAR_WINDOW_STICKERS_DIR = _ROOT / "car_window_stickers"

# Bump when render settings change so cached PNGs regenerate.
_STICKER_PREVIEW_RENDER_VERSION = 2
_DEFAULT_STICKER_PREVIEW_DPI = 200

_VIN_RE = re.compile(r"^[A-HJ-NPR-Z0-9]{17}$", re.I)


def _sticker_storage_key(
    *,
    dealer_id: str | int | None = None,
    dealership_registry_id: int | None = None,
) -> str:
    """Stable folder name under ``car_window_stickers/`` (``dealer_id`` is often a slug)."""
    if dealership_registry_id is not None:
        try:
            rid = int(dealership_registry_id)
            if rid > 0:
                return str(rid)
        except (TypeError, ValueError):
            pass
    if dealer_id is not None:
        s = str(dealer_id).strip()
        if s:
            try:
                n = int(s)
                if n > 0:
                    return str(n)
            except ValueError:
                pass
            safe = re.sub(r"[^\w\-.]", "_", s)[:120]
            if safe:
                return safe
    return "0"


def _normalize_vin(vin: str | None) -> str | None:
    s = re.sub(r"\s+", "", (vin or "").strip().upper())
    if _VIN_RE.match(s):
        return s
    return None


def window_sticker_local_path(
    vin: str | None,
    *,
    dealer_id: str | int | None = None,
    dealership_registry_id: int | None = None,
) -> Path | None:
    vnorm = _normalize_vin(vin)
    if not vnorm:
        return None
    keys: list[str] = []
    primary = _sticker_storage_key(
        dealer_id=dealer_id,
        dealership_registry_id=dealership_registry_id,
    )
    keys.append(primary)
    if primary != "0":
        keys.append("0")
    for key in keys:
        p = CAR_WINDOW_STICKERS_DIR / key / vnorm / "window_sticker.pdf"
        if p.is_file() and p.stat().st_size > 500:
            return p
    if CAR_WINDOW_STICKERS_DIR.is_dir():
        for p in CAR_WINDOW_STICKERS_DIR.glob(f"*/{vnorm}/window_sticker.pdf"):
            if p.is_file() and p.stat().st_size > 500:
                return p
    return None


def window_sticker_preview_path(local_pdf: Path) -> Path:
    return local_pdf.parent / f"window_sticker_preview_v{_STICKER_PREVIEW_RENDER_VERSION}.png"


def sticker_preview_dpi() -> int:
    """DPI for Monroney PNG previews (120–300). 200 ≈ retina-sharp at ~720px display width."""
    raw = (os.environ.get("WINDOW_STICKER_PREVIEW_DPI") or "").strip()
    try:
        dpi = int(raw) if raw else _DEFAULT_STICKER_PREVIEW_DPI
    except ValueError:
        dpi = _DEFAULT_STICKER_PREVIEW_DPI
    return max(120, min(300, dpi))


def ensure_sticker_preview_png(local_pdf: Path, *, dpi: int | None = None) -> Path | None:
    """Render page 1 of the stored Monroney PDF to PNG for in-page display."""
    if not local_pdf.is_file():
        return None
    png_path = window_sticker_preview_path(local_pdf)
    render_dpi = dpi if dpi is not None else sticker_preview_dpi()
    try:
        pdf_mtime = local_pdf.stat().st_mtime
    except OSError:
        return None
    if png_path.is_file() and png_path.stat().st_mtime >= pdf_mtime:
        return png_path
    try:
        import fitz
    except ImportError:
        logger.warning(
            "pymupdf not installed — window sticker preview images disabled. "
            "pip install pymupdf"
        )
        return None
    try:
        doc = fitz.open(str(local_pdf))
        try:
            page = doc[0]
            pix = page.get_pixmap(dpi=render_dpi, alpha=False)
            png_path.write_bytes(pix.tobytes("png"))
        finally:
            doc.close()
    except Exception as e:
        logger.debug("Window sticker PNG render failed for %s: %s", local_pdf, e)
        return None
    return png_path if png_path.is_file() else None


def window_sticker_available(car: dict[str, Any]) -> bool:
    return window_sticker_local_path(
        car.get("vin"),
        dealer_id=car.get("dealer_id"),
        dealership_registry_id=car.get("dealership_registry_id"),
    ) is not None


def car_sticker_packages_need_analysis(car: dict[str, Any]) -> bool:
    """True when a stored PDF exists but ``sticker_options`` were never merged into ``packages``."""
    if not window_sticker_available(car):
        return False
    raw = car.get("packages")
    if not raw or is_effectively_empty(raw):
        return True
    try:
        parsed = json.loads(raw) if isinstance(raw, str) else raw
    except (json.JSONDecodeError, TypeError):
        return True
    if not isinstance(parsed, dict):
        return True
    opts = parsed.get("sticker_options")
    if not (isinstance(opts, list) and len(opts) > 0):
        return True
    if not parsed.get("sticker_specs") and not parsed.get("sticker_engine_display"):
        return True
    return False


def _apply_parsed_sticker_to_patch(
    pkg_patch: dict[str, Any],
    parsed: dict[str, Any],
) -> None:
    """Merge regex-parsed Monroney fields into packages JSON patch."""
    if parsed.get("options"):
        pkg_patch["sticker_options"] = parsed["options"]
    if parsed.get("standard_highlights"):
        pkg_patch["monroney_standard_highlights"] = parsed["standard_highlights"]
    if parsed.get("exterior_color"):
        pkg_patch["sticker_exterior_color"] = str(parsed["exterior_color"]).strip()
    if parsed.get("interior_color"):
        pkg_patch["sticker_interior_color"] = str(parsed["interior_color"]).strip()
    if parsed.get("interior_material"):
        pkg_patch["sticker_interior_material"] = str(parsed["interior_material"]).strip()
    if parsed.get("sticker_specs"):
        pkg_patch["sticker_specs"] = parsed["sticker_specs"]
    if parsed.get("engine_display"):
        pkg_patch["sticker_engine_display"] = str(parsed["engine_display"]).strip()
    if parsed.get("transmission"):
        pkg_patch["sticker_transmission"] = str(parsed["transmission"]).strip()
    if parsed.get("drivetrain"):
        pkg_patch["sticker_drivetrain"] = str(parsed["drivetrain"]).strip()
    if parsed.get("tires"):
        pkg_patch["sticker_tires"] = str(parsed["tires"]).strip()
    if parsed.get("wheels"):
        pkg_patch["sticker_wheels"] = str(parsed["wheels"]).strip()
    if parsed.get("doors"):
        pkg_patch["sticker_doors"] = str(parsed["doors"]).strip()
    if parsed.get("seating"):
        pkg_patch["sticker_seating"] = str(parsed["seating"]).strip()


def _car_fields_from_parsed_sticker(
    car: dict[str, Any],
    parsed: dict[str, Any],
) -> dict[str, Any]:
    """Backfill listing columns when sticker data is authoritative."""
    fields: dict[str, Any] = {}
    vin = str(car.get("vin") or "")

    if parsed.get("engine_l") is not None:
        fields["engine_l"] = _format_sticker_engine_l(float(parsed["engine_l"]))
    if parsed.get("cylinders") is not None:
        fields["cylinders"] = int(parsed["cylinders"])
    if parsed.get("engine_raw"):
        fields["engine_description"] = str(parsed["engine_raw"]).strip()[:200]
    elif parsed.get("engine_display"):
        fields["engine_description"] = str(parsed["engine_display"]).strip()[:200]

    if parsed.get("transmission") and is_effectively_empty(car.get("transmission")):
        fields["transmission"] = str(parsed["transmission"]).strip()[:120]
    if parsed.get("drivetrain") and is_effectively_empty(car.get("drivetrain")):
        fields["drivetrain"] = str(parsed["drivetrain"]).strip()[:40]
    if parsed.get("exterior_color") and is_effectively_empty(car.get("exterior_color")):
        fields["exterior_color"] = str(parsed["exterior_color"]).strip()[:120]
    if parsed.get("interior_color") and is_effectively_empty(car.get("interior_color")):
        fields["interior_color"] = str(parsed["interior_color"]).strip()[:120]

    if vin and parsed.get("engine_display"):
        fields.setdefault("engine_description", str(parsed["engine_display"]).strip()[:200])
    return fields


def _format_sticker_engine_l(lit: float) -> str:
    return f"{lit:.1f}".rstrip("0").rstrip(".") if lit == int(lit) else f"{lit:.1f}"


def _analyze_local_sticker_pdf(
    car_id: int,
    car: dict[str, Any],
    local: Path,
    *,
    sticker_url: str = "",
) -> bool:
    """Parse stored PDF and merge options/colors into the car row. Returns True if packages updated."""
    from backend.scanner.window_sticker import (
        _extract_pdf_text,
        _parse_msrp_from_sticker_text,
        known_oem_engine_from_car,
        oem_sticker_parsing_skip_claude,
        parse_sticker_from_text,
    )

    try:
        content = local.read_bytes()
    except OSError as e:
        logger.debug("Could not read local sticker %s: %s", local, e)
        return False
    if len(content) < 500 or content[:4] != b"%PDF":
        return False

    raw_text = _extract_pdf_text(content)
    pkg_patch: dict[str, Any] = {
        "sticker_url": sticker_url or car.get("window_sticker_url") or "",
        "sticker_fetched_at": datetime.now(timezone.utc).isoformat(),
    }
    vnorm = str(car.get("vin") or "").strip().upper()
    skip_claude = oem_sticker_parsing_skip_claude(vnorm)
    parsed = parse_sticker_from_text(raw_text) if raw_text else {}
    if not parsed.get("engine_display"):
        fallback = known_oem_engine_from_car(car)
        if fallback:
            for key, val in fallback.items():
                if val and not parsed.get(key):
                    parsed[key] = val
    if parsed:
        _apply_parsed_sticker_to_patch(pkg_patch, parsed)

    claude = None
    if raw_text and not skip_claude and not pkg_patch.get("sticker_options"):
        claude = _analyze_sticker_text_with_claude(raw_text, car)
        if claude:
            if claude.get("options"):
                pkg_patch["sticker_options"] = claude["options"]
            if claude.get("standard_equipment"):
                pkg_patch["monroney_standard_highlights"] = claude["standard_equipment"]
            if claude.get("exterior_color"):
                pkg_patch["sticker_exterior_color"] = str(claude["exterior_color"]).strip()
            if claude.get("interior_color"):
                pkg_patch["sticker_interior_color"] = str(claude["interior_color"]).strip()

    if not pkg_patch.get("sticker_options") and not claude and not pkg_patch.get(
        "monroney_standard_highlights"
    ) and not pkg_patch.get("sticker_specs"):
        return False

    merged_packages = _merge_packages(car.get("packages"), pkg_patch)
    fields: dict[str, Any] = {
        "packages": merged_packages,
        "window_sticker_url": sticker_url or car.get("window_sticker_url") or "",
    }
    fields.update(_car_fields_from_parsed_sticker(car, parsed))
    msrp = _parse_msrp_from_sticker_text(raw_text)
    if msrp is None and claude and claude.get("msrp") is not None:
        try:
            msrp = int(claude["msrp"])
        except (TypeError, ValueError):
            msrp = None
    if msrp and is_effectively_empty(car.get("msrp")):
        fields["msrp"] = msrp

    prov = merge_spec_source_json(
        car.get("spec_source_json"),
        {
            "window_sticker_pdf": {
                "source": "oem_window_sticker",
                "url": sticker_url or car.get("window_sticker_url") or "",
                "detail": str(local.relative_to(_ROOT)),
                "fetched_at": datetime.now(timezone.utc).isoformat(),
            }
        },
    )
    fields["spec_source_json"] = prov
    update_car_row_partial(int(car_id), fields)
    return True


def _merge_packages(existing_raw: str | None, patch: dict[str, Any]) -> str:
    base: dict[str, Any] = {}
    if existing_raw and str(existing_raw).strip() and not is_effectively_empty(existing_raw):
        try:
            parsed = json.loads(existing_raw)
            if isinstance(parsed, dict):
                base = dict(parsed)
        except (json.JSONDecodeError, TypeError):
            base = {"legacy_text": str(existing_raw).strip()}

    for k, v in patch.items():
        if v is None:
            continue
        if isinstance(v, list):
            cur = base.get(k)
            if not isinstance(cur, list):
                cur = []
            seen = {str(x).strip().lower() for x in cur if x}
            for item in v:
                s = str(item).strip()
                if s and s.lower() not in seen:
                    cur.append(s)
                    seen.add(s.lower())
            base[k] = cur
        else:
            base[k] = v
    return json.dumps(base, ensure_ascii=False)


def _analyze_sticker_text_with_claude(raw_text: str, car: dict[str, Any]) -> dict[str, Any] | None:
    api_key = (os.environ.get("ANTHROPIC_API_KEY") or "").strip()
    if not api_key or not (raw_text or "").strip():
        return None
    snippet = raw_text.strip()[:12000]
    prompt = (
        f"Vehicle: {car.get('year')} {car.get('make')} {car.get('model')} {car.get('trim') or ''}\n"
        f"VIN: {car.get('vin')}\n\n"
        "Extract window sticker / Monroney label data from the text below.\n"
        "Return ONLY valid JSON:\n"
        '{"options": string[], "standard_equipment": string[], '
        '"exterior_color": string|null, "interior_color": string|null, "msrp": number|null, '
        '"base_price": number|null, "total_price": number|null}\n'
        "options: paid optional equipment and packages only (e.g. Customer Preferred Package 2BE ($2,595) "
        "and its sub-features as separate strings). Exclude standard equipment and safety bullet lists.\n"
        "standard_equipment: up to 25 major standard feature lines if listed.\n"
        "msrp: total/sticker price if shown; base_price if only base is clear.\n\n"
        f"--- STICKER TEXT ---\n{snippet}"
    )
    try:
        import anthropic

        client = anthropic.Anthropic(api_key=api_key)
        resp = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=1024,
            system="You output JSON only. No markdown.",
            messages=[{"role": "user", "content": prompt}],
        )
        text = (resp.content[0].text or "").strip()
        if text.startswith("```"):
            text = re.sub(r"^```(?:json)?\n?", "", text)
            text = re.sub(r"\n?```$", "", text)
        data = json.loads(text)
        return data if isinstance(data, dict) else None
    except Exception as e:
        logger.debug("Claude sticker text analysis failed: %s", e)
        return None


def _vision_fallback_packages(car: dict[str, Any]) -> dict[str, Any] | None:
    """When no OEM PDF, analyze listing photos for sticker/options (existing Haiku vision)."""
    try:
        from backend.enrichment.service import (
            _merge_vision_observations,
            _vision_analyze_car,
        )

        vision = _vision_analyze_car(car)
        if not vision:
            return None
        merged_json = _merge_vision_observations(car.get("packages"), vision)
        return json.loads(merged_json) if merged_json else None
    except Exception as e:
        logger.debug("Vision fallback for packages failed: %s", e)
        return None


def sticker_panel_payload(
    ctx: dict[str, Any],
    car: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """JSON-friendly sticker summary for car detail / packages ensure API."""
    car = car or {}
    title_bits = [str(car.get("year") or "").strip(), str(car.get("make") or "").strip()]
    model = str(car.get("model") or "").strip()
    trim = str(car.get("trim") or "").strip()
    if model:
        title_bits.append(model)
    if trim:
        title_bits.append(trim)
    ymm = " ".join(x for x in title_bits if x).strip()
    msrp = car.get("msrp")
    try:
        msrp_n = int(msrp) if msrp is not None else None
    except (TypeError, ValueError):
        msrp_n = None
    return {
        "vehicle_title": ymm,
        "listing_sticker_options": ctx.get("listing_sticker_options") or [],
        "listing_monroney_standard": (ctx.get("listing_monroney_standard") or [])[:20],
        "sticker_exterior_color": ctx.get("sticker_exterior_color"),
        "sticker_interior_color": ctx.get("sticker_interior_color"),
        "sticker_interior_material": ctx.get("sticker_interior_material"),
        "sticker_spec_lines": ctx.get("sticker_spec_lines") or [],
        "sticker_msrp": msrp_n,
        "packages_panel_has_content": bool(ctx.get("packages_panel_has_content")),
    }


def ensure_window_sticker_for_car(car_id: int, *, allow_vision_fallback: bool = True) -> dict[str, Any]:
    """
    Fetch OEM sticker PDF when possible, store under ``car_window_stickers/``, merge packages.
    Returns a status dict for API / templates.
    """
    car = get_car_by_id(int(car_id), include_inactive=True)
    if not car:
        return {"ok": False, "error": "not_found"}

    vnorm = _normalize_vin(car.get("vin"))
    out: dict[str, Any] = {
        "ok": True,
        "car_id": int(car_id),
        "vin": vnorm,
        "stored": False,
        "analyzed": False,
        "vision_fallback": False,
        "window_sticker_available": False,
    }

    local = window_sticker_local_path(
        vnorm,
        dealer_id=car.get("dealer_id"),
        dealership_registry_id=car.get("dealership_registry_id"),
    )
    if local:
        out["window_sticker_available"] = True
        out["stored"] = True
        from backend.scanner.window_sticker import get_window_sticker_url

        sticker_url = (car.get("window_sticker_url") or "").strip() or (
            get_window_sticker_url(vnorm) if vnorm else ""
        )
        if car_sticker_packages_need_analysis(car):
            if _analyze_local_sticker_pdf(
                int(car_id), car, local, sticker_url=sticker_url
            ):
                out["analyzed"] = True
        ensure_sticker_preview_png(local)
        return out

    if not vnorm:
        if allow_vision_fallback:
            pkg = _vision_fallback_packages(car)
            if pkg:
                update_car_row_partial(int(car_id), {"packages": json.dumps(pkg, ensure_ascii=False)})
                out["vision_fallback"] = True
                out["analyzed"] = True
        return out

    from backend.scanner.window_sticker import (
        fetch_window_sticker_pdf,
        get_window_sticker_url,
        known_oem_engine_from_car,
        oem_sticker_parsing_skip_claude,
        parse_sticker_from_text,
    )

    sticker_url = (car.get("window_sticker_url") or "").strip() or get_window_sticker_url(vnorm)
    fetched = fetch_window_sticker_pdf(vnorm)
    if not fetched:
        out["fetch_error"] = "oem_fetch_failed"
        if allow_vision_fallback:
            pkg = _vision_fallback_packages(car)
            if pkg:
                update_car_row_partial(int(car_id), {"packages": json.dumps(pkg, ensure_ascii=False)})
                out["vision_fallback"] = True
                out["analyzed"] = True
        return out

    content = fetched.get("content") or b""
    if len(content) < 500 or content[:4] != b"%PDF":
        out["fetch_error"] = "not_a_pdf"
        if allow_vision_fallback:
            pkg = _vision_fallback_packages(car)
            if pkg:
                update_car_row_partial(int(car_id), {"packages": json.dumps(pkg, ensure_ascii=False)})
                out["vision_fallback"] = True
                out["analyzed"] = True
        return out

    storage_key = _sticker_storage_key(
        dealer_id=car.get("dealer_id"),
        dealership_registry_id=car.get("dealership_registry_id"),
    )
    dest_dir = CAR_WINDOW_STICKERS_DIR / storage_key / vnorm
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / "window_sticker.pdf"
    dest.write_bytes(content)
    out["stored"] = True
    out["window_sticker_available"] = True
    ensure_sticker_preview_png(dest)

    pkg_patch: dict[str, Any] = {
        "sticker_url": fetched.get("url") or sticker_url,
        "sticker_fetched_at": datetime.now(timezone.utc).isoformat(),
    }
    raw_text = fetched.get("raw_text") or ""
    skip_claude = oem_sticker_parsing_skip_claude(vnorm)
    parsed = parse_sticker_from_text(raw_text) if raw_text else {}
    if not parsed.get("engine_display"):
        fallback = known_oem_engine_from_car(car)
        if fallback:
            for key, val in fallback.items():
                if val and not parsed.get(key):
                    parsed[key] = val
    if not parsed and fetched.get("options"):
        parsed = {"options": fetched.get("options") or []}
    if parsed:
        _apply_parsed_sticker_to_patch(pkg_patch, parsed)
        out["analyzed"] = True

    claude = None
    if raw_text and not skip_claude and not pkg_patch.get("sticker_options"):
        claude = _analyze_sticker_text_with_claude(raw_text, car)
        if claude:
            out["analyzed"] = True
            if claude.get("options"):
                pkg_patch["sticker_options"] = claude["options"]
            if claude.get("standard_equipment"):
                pkg_patch["monroney_standard_highlights"] = claude["standard_equipment"]
            if claude.get("exterior_color"):
                pkg_patch["sticker_exterior_color"] = str(claude["exterior_color"]).strip()
            if claude.get("interior_color"):
                pkg_patch["sticker_interior_color"] = str(claude["interior_color"]).strip()

    merged_packages = _merge_packages(car.get("packages"), pkg_patch)
    fields: dict[str, Any] = {
        "packages": merged_packages,
        "window_sticker_url": fetched.get("url") or sticker_url,
    }
    fields.update(_car_fields_from_parsed_sticker(car, parsed))
    msrp = fetched.get("msrp") or parsed.get("msrp")
    if msrp is None and claude and claude.get("msrp") is not None:
        try:
            msrp = int(claude["msrp"])
        except (TypeError, ValueError):
            msrp = None
    if msrp and is_effectively_empty(car.get("msrp")):
        fields["msrp"] = msrp

    prov = merge_spec_source_json(
        car.get("spec_source_json"),
        {
            "window_sticker_pdf": {
                "source": "oem_window_sticker",
                "url": fetched.get("url") or sticker_url,
                "detail": str(dest.relative_to(_ROOT)),
                "fetched_at": datetime.now(timezone.utc).isoformat(),
            }
        },
    )
    fields["spec_source_json"] = prov
    update_car_row_partial(int(car_id), fields)
    return out
