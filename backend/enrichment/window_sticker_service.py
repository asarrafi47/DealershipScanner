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
        for ext in (".jpg", ".jpeg", ".png", ".webp"):
            p_img = CAR_WINDOW_STICKERS_DIR / key / vnorm / f"window_sticker{ext}"
            if p_img.is_file() and p_img.stat().st_size > 20000:
                return p_img
        p_txt = CAR_WINDOW_STICKERS_DIR / key / vnorm / "window_sticker.txt"
        if p_txt.is_file() and p_txt.stat().st_size > 200:
            return p_txt
    if CAR_WINDOW_STICKERS_DIR.is_dir():
        for p in CAR_WINDOW_STICKERS_DIR.glob(f"*/{vnorm}/window_sticker.pdf"):
            if p.is_file() and p.stat().st_size > 500:
                return p
        for p in CAR_WINDOW_STICKERS_DIR.glob(f"*/{vnorm}/window_sticker.*"):
            if p.suffix.lower() in {".jpg", ".jpeg", ".png", ".webp"} and p.stat().st_size > 20000:
                return p
        for p in CAR_WINDOW_STICKERS_DIR.glob(f"*/{vnorm}/window_sticker.txt"):
            if p.is_file() and p.stat().st_size > 200:
                return p
    return None


def window_sticker_visual_local_path(
    vin: str | None,
    *,
    dealer_id: str | int | None = None,
    dealership_registry_id: int | None = None,
) -> Path | None:
    """Local PDF or image suitable for in-page preview (not plain-text extracts)."""
    path = window_sticker_local_path(
        vin,
        dealer_id=dealer_id,
        dealership_registry_id=dealership_registry_id,
    )
    if not path:
        return None
    if path.suffix.lower() in {".pdf", ".jpg", ".jpeg", ".png", ".webp"}:
        return path
    return None


def window_sticker_has_visual(car: dict[str, Any]) -> bool:
    return window_sticker_visual_local_path(
        car.get("vin"),
        dealer_id=car.get("dealer_id"),
        dealership_registry_id=car.get("dealership_registry_id"),
    ) is not None


def _try_fetch_and_store_oem_pdf(
    car_id: int,
    car: dict[str, Any],
    vnorm: str,
    *,
    out: dict[str, Any] | None = None,
) -> Path | None:
    """Fetch Stellantis/Ford OEM PDF and store under ``car_window_stickers/``."""
    from backend.scanner.window_sticker import fetch_window_sticker_pdf, get_window_sticker_url

    fetched = fetch_window_sticker_pdf(vnorm)
    if not fetched:
        return None
    content = fetched.get("content") or b""
    if len(content) < 500 or content[:4] != b"%PDF":
        return None

    storage_key = _sticker_storage_key(
        dealer_id=car.get("dealer_id"),
        dealership_registry_id=car.get("dealership_registry_id"),
    )
    dest_dir = CAR_WINDOW_STICKERS_DIR / storage_key / vnorm
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / "window_sticker.pdf"
    dest.write_bytes(content)
    ensure_sticker_preview_png(dest)

    sticker_url = (car.get("window_sticker_url") or "").strip() or get_window_sticker_url(vnorm)
    if car_sticker_packages_need_analysis(car):
        _analyze_local_sticker_pdf(int(car_id), car, dest, sticker_url=sticker_url or "")

    if out is not None:
        out["stored"] = True
        out["window_sticker_available"] = True
        out["analyzed"] = True
    return dest


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
    suffix = local_pdf.suffix.lower()
    if suffix == ".txt":
        return None
    if suffix in {".jpg", ".jpeg", ".png", ".webp"}:
        return local_pdf
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


def packages_has_sticker_options(packages: dict[str, Any] | None) -> bool:
    if not isinstance(packages, dict):
        return False
    priced = packages.get("sticker_options_priced")
    if isinstance(priced, list) and priced:
        return True
    opts = packages.get("sticker_options")
    return isinstance(opts, list) and len(opts) > 0


def packages_has_parsed_description(packages: dict[str, Any] | None) -> bool:
    if not isinstance(packages, dict):
        return False
    if packages.get("packages_normalized"):
        return True
    sf = packages.get("standalone_features_from_description")
    if isinstance(sf, list) and sf:
        return True
    return bool(str(packages.get("listing_description_parser_version") or "").strip())


def should_skip_photo_package_analysis(
    car: dict[str, Any],
    packages: dict[str, Any] | None = None,
) -> bool:
    """
    Skip Claude/vision photo analysis when window sticker or parsed listing description
    already supplies equipment data.
    """
    if window_sticker_available(car):
        return True
    if packages is None:
        raw = car.get("packages")
        if raw and not is_effectively_empty(raw):
            try:
                packages = json.loads(raw) if isinstance(raw, str) else raw
            except (json.JSONDecodeError, TypeError):
                packages = None
    if packages_has_sticker_options(packages if isinstance(packages, dict) else None):
        return True
    if packages_has_parsed_description(packages if isinstance(packages, dict) else None):
        return True
    return False


def _packages_have_priced_sticker_options(parsed: dict[str, Any]) -> bool:
    """True when packages JSON already has structured options with at least one price."""
    priced = parsed.get("sticker_options_priced")
    if isinstance(priced, list):
        for raw in priced:
            if isinstance(raw, dict) and raw.get("price") is not None:
                return True
            if isinstance(raw, str):
                from backend.scanner.window_sticker import _parse_dollar_amount

                if _parse_dollar_amount(raw) is not None:
                    return True
    opts = parsed.get("sticker_options")
    if isinstance(opts, list):
        from backend.scanner.window_sticker import _parse_dollar_amount

        for raw in opts:
            if isinstance(raw, dict) and raw.get("price") is not None:
                return True
            if isinstance(raw, str) and _parse_dollar_amount(raw) is not None:
                return True
    return False


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
    if not _packages_have_priced_sticker_options(parsed):
        return True
    if not parsed.get("sticker_specs") and not parsed.get("sticker_engine_display"):
        return True
    disp = parsed.get("sticker_engine_display")
    if isinstance(disp, str) and disp.strip():
        from backend.scanner.window_sticker import sticker_engine_display_is_valid

        if not sticker_engine_display_is_valid(disp):
            return True
        pkg_blob = json.dumps(parsed, ensure_ascii=False).lower()
        if "etorque" in pkg_blob and "mild hybrid" not in disp.lower():
            return True
        if "mild hybrid" not in disp.lower():
            from backend.scanner.window_sticker import car_has_etorque_signal

            if car_has_etorque_signal(car):
                return True
    return False


def _apply_parsed_sticker_to_patch(
    pkg_patch: dict[str, Any],
    parsed: dict[str, Any],
) -> None:
    """Merge regex-parsed Monroney fields into packages JSON patch."""
    option_items = parsed.get("option_items")
    if isinstance(option_items, list) and option_items:
        pkg_patch["sticker_options_priced"] = option_items
        pkg_patch["sticker_options"] = [
            str(x.get("label") or x.get("name") or "").strip()
            for x in option_items
            if isinstance(x, dict) and str(x.get("label") or x.get("name") or "").strip()
        ]
    elif parsed.get("options"):
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
    if parsed.get("fuel_type"):
        pkg_patch["sticker_fuel_type"] = str(parsed["fuel_type"]).strip()
    if parsed.get("mpg_city") is not None:
        pkg_patch["sticker_mpg_city"] = int(parsed["mpg_city"])
    if parsed.get("mpg_highway") is not None:
        pkg_patch["sticker_mpg_highway"] = int(parsed["mpg_highway"])
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
    from backend.scanner.window_sticker import (
        oem_sticker_parsing_skip_claude,
        sticker_engine_display_is_valid,
    )

    fields: dict[str, Any] = {}
    vin = str(car.get("vin") or "")
    authoritative = oem_sticker_parsing_skip_claude(vin)
    is_ev = str(parsed.get("fuel_type") or "").strip().lower() == "electric"

    def _set(key: str, val: Any, *, force: bool = False) -> None:
        if val is None:
            return
        if force or is_effectively_empty(car.get(key)):
            fields[key] = val

    if is_ev:
        _set("fuel_type", "Electric", force=authoritative)
        _set("cylinders", 0, force=authoritative)
        _set("engine_l", "Electric", force=authoritative)
    elif str(parsed.get("fuel_type") or "").strip().lower() == "hybrid":
        _set("fuel_type", "Hybrid", force=authoritative)
    elif parsed.get("engine_l") is not None:
        _set(
            "engine_l",
            _format_sticker_engine_l(float(parsed["engine_l"])),
            force=authoritative,
        )
    if parsed.get("cylinders") is not None and not is_ev:
        _set("cylinders", int(parsed["cylinders"]), force=authoritative)

    engine_disp = parsed.get("engine_display")
    if isinstance(engine_disp, str) and sticker_engine_display_is_valid(engine_disp):
        _set("engine_description", engine_disp.strip()[:200], force=authoritative)
    elif parsed.get("engine_raw") and sticker_engine_display_is_valid(parsed.get("engine_raw")):
        _set("engine_description", str(parsed["engine_raw"]).strip()[:200], force=authoritative)

    if parsed.get("transmission"):
        _set("transmission", str(parsed["transmission"]).strip()[:120], force=authoritative)
    if parsed.get("drivetrain"):
        _set("drivetrain", str(parsed["drivetrain"]).strip()[:40], force=authoritative)
    if parsed.get("exterior_color"):
        _set("exterior_color", str(parsed["exterior_color"]).strip()[:120], force=authoritative)
    if parsed.get("interior_color"):
        _set("interior_color", str(parsed["interior_color"]).strip()[:120], force=authoritative)

    if is_ev and authoritative:
        if parsed.get("mpg_city") is not None:
            fields["mpg_city"] = int(parsed["mpg_city"])
        if parsed.get("mpg_highway") is not None:
            fields["mpg_highway"] = int(parsed["mpg_highway"])
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
    """Parse stored PDF or text sticker and merge options/colors into the car row."""
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

    raw_text = ""
    if local.suffix.lower() == ".txt":
        raw_text = content.decode("utf-8", errors="replace")
    elif len(content) >= 500 and content[:4] == b"%PDF":
        raw_text = _extract_pdf_text(content)
    else:
        return False

    return _analyze_sticker_text_and_merge(
        car_id,
        car,
        raw_text,
        sticker_url=sticker_url or car.get("window_sticker_url") or "",
        local_detail=str(local.relative_to(_ROOT)) if local.is_file() else "",
        source_label="listing_window_sticker" if local.suffix.lower() == ".txt" else "oem_window_sticker",
    )


def _analyze_sticker_text_and_merge(
    car_id: int,
    car: dict[str, Any],
    raw_text: str,
    *,
    sticker_url: str = "",
    local_detail: str = "",
    source_label: str = "oem_window_sticker",
) -> bool:
    """Parse sticker text (PDF extract or iPacket MSRP doc) and merge into packages."""
    from backend.scanner.window_sticker import (
        _parse_msrp_from_sticker_text,
        known_oem_engine_from_car,
        oem_sticker_parsing_skip_claude,
        parse_sticker_from_text,
    )

    if not (raw_text or "").strip():
        return False

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
    has_sticker_opts = bool(
        pkg_patch.get("sticker_options_priced") or pkg_patch.get("sticker_options")
    )
    if raw_text and not skip_claude and not has_sticker_opts:
        claude = _analyze_sticker_text_with_claude(raw_text, car)
        if claude:
            if claude.get("options"):
                _apply_claude_sticker_options(pkg_patch, claude["options"])
            if claude.get("standard_equipment"):
                pkg_patch["monroney_standard_highlights"] = claude["standard_equipment"]
            if claude.get("exterior_color"):
                pkg_patch["sticker_exterior_color"] = str(claude["exterior_color"]).strip()
            if claude.get("interior_color"):
                pkg_patch["sticker_interior_color"] = str(claude["interior_color"]).strip()

    if (
        not pkg_patch.get("sticker_options")
        and not pkg_patch.get("sticker_options_priced")
        and not claude
        and not pkg_patch.get("monroney_standard_highlights")
        and not pkg_patch.get("sticker_specs")
    ):
        return False

    # Self-improving loop: fold this sticker's priced options into the package-
    # value registry. Real OEM prices are ground truth and teach the value of
    # each package/option for every future car of the same configuration.
    try:
        _priced = pkg_patch.get("sticker_options_priced")
        if _priced:
            from backend.enrichment.package_registry import (
                classify_kind,
                record_package_observations,
            )

            _items = [
                {
                    "kind": classify_kind(e.get("name")),
                    "name": str(e.get("name") or "").strip(),
                    "code": e.get("code"),
                    "price": e.get("price") if isinstance(e.get("price"), (int, float)) else None,
                }
                for e in _priced
                if isinstance(e, dict) and str(e.get("name") or "").strip()
            ]
            if _items:
                record_package_observations(car, "oem_sticker", _items)
    except Exception:
        pass

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
                "source": source_label,
                "url": sticker_url or car.get("window_sticker_url") or "",
                "detail": local_detail,
                "fetched_at": datetime.now(timezone.utc).isoformat(),
            }
        },
    )
    fields["spec_source_json"] = prov
    update_car_row_partial(int(car_id), fields)
    return True


def _listing_sticker_urls_for_car(car: dict[str, Any]) -> list[str]:
    from backend.scanner.window_sticker import (
        extract_listing_sticker_urls_from_html,
        listing_sticker_url_candidates,
    )

    urls = listing_sticker_url_candidates(car)
    if urls:
        return urls

    listing_url = str(
        car.get("source_url") or car.get("_detail_url") or car.get("listing_vdp_url") or ""
    ).strip()
    if not listing_url.lower().startswith("http"):
        return []
    try:
        from backend.scanner.listing_gap_fill import fetch_listing_html

        html = fetch_listing_html(listing_url)
    except Exception as e:
        logger.debug("Listing HTML fetch for sticker discovery failed: %s", e)
        html = None
    if not html:
        return []
    discovered = extract_listing_sticker_urls_from_html(html, vin=str(car.get("vin") or ""), car=car)
    if not discovered:
        vnorm = re.sub(r"\s+", "", str(car.get("vin") or "").strip().upper())
        if len(vnorm) == 17:
            from backend.scanner.dealer_sticker_provider import (
                record_ipacket_probe_result,
                should_try_ipacket_website_plugin,
            )
            from backend.scanner.window_sticker import _ipacket_sticker_urls_from_website_plugin

            if should_try_ipacket_website_plugin(car, html):
                discovered = _ipacket_sticker_urls_from_website_plugin(vnorm)
                record_ipacket_probe_result(car, success=bool(discovered))
            elif "autoipacket" in html.lower():
                record_ipacket_probe_result(car, success=False)
    if not discovered:
        return []
    probe = dict(car)
    probe["window_sticker_url"] = discovered[0]
    ranked = listing_sticker_url_candidates(probe)
    for u in discovered:
        if u not in ranked:
            ranked.append(u)
    return ranked[:8]


def _analyze_sticker_image_with_claude(image_bytes: bytes, car: dict[str, Any]) -> dict[str, Any] | None:
    """Extract Monroney options from a listing sticker image (JPG/PNG)."""
    api_key = (os.environ.get("ANTHROPIC_API_KEY") or "").strip()
    if not api_key or not image_bytes or len(image_bytes) < 8000:
        return None
    import base64

    b64 = base64.standard_b64encode(image_bytes).decode("ascii")
    media_type = "image/jpeg"
    if image_bytes[:8] == b"\x89PNG\r\n\x1a\n":
        media_type = "image/png"
    elif image_bytes[:4] == b"RIFF" and len(image_bytes) > 12 and image_bytes[8:12] == b"WEBP":
        media_type = "image/webp"
    prompt = (
        f"Vehicle: {car.get('year')} {car.get('make')} {car.get('model')} {car.get('trim') or ''}\n"
        f"VIN: {car.get('vin')}\n\n"
        "This image is a window sticker / Monroney label. Extract ALL factory options and packages.\n"
        "Return ONLY valid JSON:\n"
        '{"options": [{"name": string, "price": number|null, "code": string|null}], '
        '"standard_equipment": string[], '
        '"exterior_color": string|null, "interior_color": string|null, "msrp": number|null}\n'
        "options: paid packages and optional equipment only. Include MSRP add-on price when visible.\n"
    )
    try:
        import anthropic

        client = anthropic.Anthropic(api_key=api_key)
        resp = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=1536,
            system="You output JSON only. No markdown.",
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image",
                            "source": {"type": "base64", "media_type": media_type, "data": b64},
                        },
                        {"type": "text", "text": prompt},
                    ],
                }
            ],
        )
        text = (resp.content[0].text or "").strip()
        if text.startswith("```"):
            text = re.sub(r"^```(?:json)?\n?", "", text)
            text = re.sub(r"\n?```$", "", text)
        data = json.loads(text)
        return data if isinstance(data, dict) else None
    except Exception as e:
        logger.debug("Claude sticker image analysis failed: %s", e)
        return None


def _try_fetch_listing_sticker_for_car(
    car_id: int,
    car: dict[str, Any],
    out: dict[str, Any],
) -> bool:
    """Fetch iPacket / dealer Monroney URL from the listing when OEM PDF is unavailable."""
    from backend.scanner.window_sticker import (
        _is_valid_listing_sticker_payload,
        fetch_listing_sticker,
    )

    vnorm = _normalize_vin(car.get("vin"))
    if not vnorm:
        return False

    sticker_urls = _listing_sticker_urls_for_car(car)
    if not sticker_urls:
        return False

    fetched = None
    sticker_url = ""
    for url in sticker_urls:
        try:
            candidate = fetch_listing_sticker(url, vnorm)
        except Exception as e:
            logger.debug("Listing sticker fetch failed %s: %s", url[:80], e)
            continue
        if candidate and _is_valid_listing_sticker_payload(candidate):
            fetched = candidate
            sticker_url = url
            break

    if not fetched:
        out["fetch_error"] = out.get("fetch_error") or "listing_sticker_fetch_failed"
        return False

    content = fetched.get("content") or b""
    raw_text = str(fetched.get("raw_text") or "")
    storage_key = _sticker_storage_key(
        dealer_id=car.get("dealer_id"),
        dealership_registry_id=car.get("dealership_registry_id"),
    )
    dest_dir = CAR_WINDOW_STICKERS_DIR / storage_key / vnorm
    dest_dir.mkdir(parents=True, exist_ok=True)

    is_pdf = content[:4] == b"%PDF" and len(content) >= 500
    is_image = not is_pdf and len(content) >= 20000 and bool(
        re.search(r"image/|jpe?g|png|webp", str(fetched.get("content_type") or ""), re.I)
    )

    if is_pdf:
        dest = dest_dir / "window_sticker.pdf"
        dest.write_bytes(content)
        ensure_sticker_preview_png(dest)
    elif is_image:
        ext = ".jpg"
        if content[:8] == b"\x89PNG\r\n\x1a\n":
            ext = ".png"
        dest = dest_dir / f"window_sticker{ext}"
        dest.write_bytes(content)
    else:
        dest = dest_dir / "window_sticker.txt"
        dest.write_text(raw_text or content.decode("utf-8", errors="replace"), encoding="utf-8")

    out["stored"] = True
    out["window_sticker_available"] = True
    out["listing_sticker"] = True

    if "autoipacket.com" in (sticker_url or "").lower():
        from backend.scanner.dealer_sticker_provider import note_dealer_sticker_provider

        note_dealer_sticker_provider(
            car.get("dealership_registry_id"),
            "ipacket",
            source="listing_sticker_fetch",
        )
    elif sticker_url:
        from backend.scanner.dealer_sticker_provider import note_dealer_sticker_provider

        note_dealer_sticker_provider(
            car.get("dealership_registry_id"),
            "listing_embed",
            source="listing_sticker_fetch",
        )

    analyzed = False
    if raw_text.strip():
        analyzed = _analyze_sticker_text_and_merge(
            int(car_id),
            car,
            raw_text,
            sticker_url=sticker_url or fetched.get("url") or "",
            local_detail=str(dest.relative_to(_ROOT)),
            source_label="listing_window_sticker",
        )
    if not analyzed and is_image:
        logger.debug(
            "Skipping Claude sticker image analysis for car_id=%s (stored listing sticker image)",
            car_id,
        )

    if analyzed:
        out["analyzed"] = True

    if sticker_url and is_effectively_empty(car.get("window_sticker_url")):
        update_car_row_partial(int(car_id), {"window_sticker_url": sticker_url})

    return True


def _apply_vision_fallback_merged(car_id: int, car: dict[str, Any], out: dict[str, Any]) -> bool:
    """Merge vision-detected equipment into existing packages (never replace)."""
    if should_skip_photo_package_analysis(car):
        return False
    pkg = _vision_fallback_packages(car)
    if not pkg:
        return False
    merged = _merge_packages(car.get("packages"), pkg if isinstance(pkg, dict) else {})
    update_car_row_partial(int(car_id), {"packages": merged})
    out["vision_fallback"] = True
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
            if k == "sticker_options_priced":
                if v:
                    base[k] = list(v)
                continue
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


def _apply_claude_sticker_options(pkg_patch: dict[str, Any], claude_opts: Any) -> None:
    from backend.scanner.window_sticker import coerce_sticker_option_item

    if not isinstance(claude_opts, list):
        return
    items: list[dict[str, Any]] = []
    for raw in claude_opts:
        if isinstance(raw, dict):
            name = str(raw.get("name") or raw.get("label") or "").strip()
            if not name:
                continue
            price = raw.get("price")
            if price is not None:
                try:
                    price = int(price)
                except (TypeError, ValueError):
                    price = None
            code = str(raw.get("code") or "").strip() or None
            item = coerce_sticker_option_item(
                {"name": name, "price": price, "code": code, "label": raw.get("label")}
            )
            if item:
                items.append(item)
        else:
            item = coerce_sticker_option_item(raw)
            if item:
                items.append(item)
    if not items:
        return
    pkg_patch["sticker_options_priced"] = items
    pkg_patch["sticker_options"] = [str(x["label"]) for x in items if x.get("label")]


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
        '{"options": [{"name": string, "price": number|null, "code": string|null}], '
        '"standard_equipment": string[], '
        '"exterior_color": string|null, "interior_color": string|null, "msrp": number|null, '
        '"base_price": number|null, "total_price": number|null}\n'
        "options: paid optional equipment/packages only. Include MSRP add-on price in dollars when listed "
        "(negative for credits). Use code when the sticker shows a factory code (e.g. DG3, DC1).\n"
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
    """Analyze listing photos for visible equipment/options (multi-image Claude vision)."""
    try:
        from backend.enrichment.service import (
            _all_gallery_urls_ordered,
            _merge_vision_observations,
            _vision_analyze_car,
        )
        from backend.vision.equipment_vision import analyze_car_equipment_from_gallery

        def _analyze(row_in: dict[str, Any], url: str) -> dict[str, Any] | None:
            return _vision_analyze_car(row_in, url_override=url)

        merged_json, stats = analyze_car_equipment_from_gallery(
            car,
            gallery_urls=_all_gallery_urls_ordered(car),
            analyze_url=_analyze,
            merge_observations=_merge_vision_observations,
        )
        if not merged_json or not stats.get("urls_analyzed"):
            return None
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
        "listing_sticker_option_groups": ctx.get("listing_sticker_option_groups") or [],
        "listing_sticker_option_sections": ctx.get("listing_sticker_option_sections") or {},
        "listing_packages_sections": ctx.get("listing_packages_sections") or [],
        "listing_observed_features": ctx.get("listing_observed_features") or [],
        "listing_possible_packages": ctx.get("listing_possible_packages") or [],
        "listing_photo_detected_equipment": ctx.get("listing_photo_detected_equipment") or [],
        "listing_standalone_features": ctx.get("listing_standalone_features") or [],
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

    from backend.scanner.window_sticker import (
        get_window_sticker_url,
        should_auto_fetch_oem_window_sticker,
    )

    local = window_sticker_local_path(
        vnorm,
        dealer_id=car.get("dealer_id"),
        dealership_registry_id=car.get("dealership_registry_id"),
    )
    if local and local.suffix.lower() == ".txt" and should_auto_fetch_oem_window_sticker(car):
        upgraded = _try_fetch_and_store_oem_pdf(int(car_id), car, vnorm, out=out)
        if upgraded:
            local = upgraded

    if local:
        out["window_sticker_available"] = True
        out["stored"] = True

        sticker_url = (car.get("window_sticker_url") or "").strip() or (
            get_window_sticker_url(vnorm) if vnorm else ""
        )
        if car_sticker_packages_need_analysis(car):
            if _analyze_local_sticker_pdf(
                int(car_id), car, local, sticker_url=sticker_url
            ):
                out["analyzed"] = True
        visual = window_sticker_visual_local_path(
            vnorm,
            dealer_id=car.get("dealer_id"),
            dealership_registry_id=car.get("dealership_registry_id"),
        )
        if visual and visual.suffix.lower() == ".pdf":
            ensure_sticker_preview_png(visual)
        if allow_vision_fallback:
            _apply_vision_fallback_merged(int(car_id), car, out)
        return out

    if not vnorm:
        if allow_vision_fallback:
            _apply_vision_fallback_merged(int(car_id), car, out)
        return out

    if not should_auto_fetch_oem_window_sticker(car):
        if _try_fetch_listing_sticker_for_car(int(car_id), car, out):
            return out
        if allow_vision_fallback:
            _apply_vision_fallback_merged(int(car_id), car, out)
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
        if _try_fetch_listing_sticker_for_car(int(car_id), car, out):
            return out
        if allow_vision_fallback:
            _apply_vision_fallback_merged(int(car_id), car, out)
        return out

    content = fetched.get("content") or b""
    if len(content) < 500 or content[:4] != b"%PDF":
        out["fetch_error"] = "not_a_pdf"
        if _try_fetch_listing_sticker_for_car(int(car_id), car, out):
            return out
        if allow_vision_fallback:
            _apply_vision_fallback_merged(int(car_id), car, out)
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
    has_sticker_opts = bool(
        pkg_patch.get("sticker_options_priced") or pkg_patch.get("sticker_options")
    )
    if raw_text and not skip_claude and not has_sticker_opts:
        claude = _analyze_sticker_text_with_claude(raw_text, car)
        if claude:
            out["analyzed"] = True
            if claude.get("options"):
                _apply_claude_sticker_options(pkg_patch, claude["options"])
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
    if allow_vision_fallback:
        car = get_car_by_id(int(car_id), include_inactive=True) or car
        _apply_vision_fallback_merged(int(car_id), car, out)
    return out
