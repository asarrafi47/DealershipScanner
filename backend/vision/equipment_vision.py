"""Multi-image gallery equipment vision — detect options visible in listing photos."""
from __future__ import annotations

import json
import logging
import os
import re
import time
from typing import Any, Callable

logger = logging.getLogger(__name__)

_GENERIC_VISION_FEATURE_PATTERNS = (
    r"\balloy wheels?\b",
    r"\bchrome grille\b",
    r"\b(grille|grill) trim\b",
    r"\bled headlights?\b",
    r"\bprojector[- ]style headlights?\b",
    r"\bchrome mirror\b",
    r"\bpower mirrors?\b",
    r"\bpower windows?\b",
    r"\bpower locks?\b",
    r"\bbody side molding\b",
    r"\bfog lights?\b",
    r"\broof spoiler\b",
    r"\bchrome accents?\b",
    r"\bfloor mats?\b",
    r"\bcarpet(ed|ing)? floor(ing)?\b",
    r"\bsteering wheel[- ]mounted controls?\b",
    r"\bmulti[- ]function steering wheel\b",
    r"\bcup holders?\b",
    r"\bair vents?\b",
    r"\bdoor panel\b",
    r"\bcenter console\b",
    r"\bblack interior trim\b",
    r"\bdashboard air vents?\b",
    r"\b(digital|analog) instrument cluster\b",
    r"\b(dual[- ]zone )?climate control\b",
    r"\b(perforated )?leather[- ]wrapped steering wheel\b",
    r"\bmanual transmission\b",
    r"\bautomatic transmission\b",
    r"\bgear shift(er)?\b",
    r"\bpower side mirrors?\b",
    r"\bdual exhaust\b",
    r"\bled tail lights?\b",
    r"\btwo[- ]tone (exterior )?paint\b",
    r"\b(sel|titanium|sport|limited) (or|/|trim)\b",
    r"\btrim (level|package)\b",
    r"\bcomfort/convenience package\b",
    r"\bpremium leather package\b",
)

_MEANINGFUL_VISION_HINTS = (
    "aftermarket",
    "tow",
    "hitch",
    "trailer",
    "panoramic",
    "sunroof",
    "moonroof",
    "navigation",
    "premium audio",
    "bang & olufsen",
    "bose",
    "harman",
    "burmester",
    "meridian",
    "jbl",
    "sony",
    "heated",
    "ventilated",
    "massage",
    "memory seat",
    "adaptive cruise",
    "lane keep",
    "lane departure",
    "blind spot",
    "360",
    "surround",
    "heads-up",
    "head-up",
    "hud",
    "night vision",
    "parking sensor",
    "red brake",
    "performance package",
    "sport package",
    "cold weather",
    "technology package",
    "driver assistance",
    "lift kit",
    "running board",
    "side step",
    "nerf bar",
    "roof rack",
    "cross bar",
    "tonneau",
    "bed liner",
    "bull bar",
    "brush guard",
    "light bar",
)


def is_meaningful_vision_feature(label: str) -> bool:
    """Keep upgraded/optional equipment; drop generic base-trim guesses."""
    s = str(label or "").strip()
    if not s or len(s) < 4:
        return False
    low = s.lower()
    if any(re.search(pat, low) for pat in _GENERIC_VISION_FEATURE_PATTERNS):
        return False
    if any(hint in low for hint in _MEANINGFUL_VISION_HINTS):
        return True
    # Likely optional if it names a package or clearly non-standard exterior add-on.
    if re.search(r"\bpackage\b", low):
        return True
    if re.search(r"\b(rails?|rack|spoiler|tint(ed|ing)?|lift|steps?|boards?)\b", low):
        return True
    return False


def filter_vision_feature_list(items: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in items:
        s = str(raw or "").strip()[:200]
        if not s or not is_meaningful_vision_feature(s):
            continue
        k = s.lower()
        if k in seen:
            continue
        seen.add(k)
        out.append(s)
    return out[:24]


PHOTO_EQUIPMENT_SCAN_VERSION = 4

_INTERIOR_URL_TOKENS = (
    "interior",
    "cabin",
    "inside",
    "dashboard",
    "dash",
    "cockpit",
    "seat",
    "console",
    "infotainment",
    "screen",
    "interiorview",
    "interior_",
    "_interior",
    "/int/",
    "-int-",
)

_EXTERIOR_DETAIL_TOKENS = (
    "wheel",
    "brake",
    "caliper",
    "exhaust",
    "hitch",
    "tow",
    "trailer",
    "rear",
    "tail",
    "bumper",
    "grille",
    "mirror",
    "speaker",
    "audio",
    "badge",
    "emblem",
    "side",
    "profile",
    "step",
    "rail",
    "running",
    "board",
    "nerf",
    "rock",
    "slider",
    "bed",
    "tonneau",
    "rack",
    "roof",
)


def equipment_vision_max_images() -> int:
    try:
        return max(1, min(12, int((os.environ.get("EQUIPMENT_VISION_MAX_IMAGES") or "6").strip())))
    except ValueError:
        return 6


def _url_lower(url: str) -> str:
    return url.strip().lower()


def _url_has_any_token(url: str, tokens: tuple[str, ...]) -> bool:
    ul = _url_lower(url)
    return any(t in ul for t in tokens)


def pick_equipment_vision_urls(urls: list[str], *, max_urls: int | None = None) -> list[str]:
    """
    Pick a diverse subset of gallery URLs for equipment/options vision.

    Prefers: hero exterior, interior cabin/dash, wheels/brakes, rear/exhaust/hitch,
    and any URL whose path hints at interior or detail shots.
    """
    cap = max_urls if max_urls is not None else equipment_vision_max_images()
    ordered: list[str] = []
    seen: set[str] = set()
    for u in urls:
        if not isinstance(u, str):
            continue
        s = u.strip()
        if not s.lower().startswith("http") or s in seen:
            continue
        seen.add(s)
        ordered.append(s)
    if not ordered:
        return []

    picks: list[str] = []
    pick_set: set[str] = set()

    def _add(url: str | None) -> None:
        if not url or url in pick_set or len(picks) >= cap:
            return
        picks.append(url)
        pick_set.add(url)

    # Hero / sticker first
    sticker = next((u for u in ordered if _is_sticker_url(u)), None)
    _add(sticker or ordered[0])

    # Explicit interior URLs
    for u in ordered:
        if _url_has_any_token(u, _INTERIOR_URL_TOKENS):
            _add(u)

    # Dealer sets: interior often starts around index 3
    for idx in (3, 4, 5, 2, 6, 7, 8):
        if idx < len(ordered):
            _add(ordered[idx])

    # Detail / exterior feature shots
    for u in ordered:
        if _url_has_any_token(u, _EXTERIOR_DETAIL_TOKENS):
            _add(u)

    # Fill remaining slots evenly across the gallery
    if len(picks) < cap and len(ordered) > 1:
        step = max(1, len(ordered) // cap)
        for i in range(0, len(ordered), step):
            _add(ordered[i])

    for u in ordered:
        _add(u)

    return picks[:cap]


def _is_sticker_url(url: str) -> bool:
    ul = url.lower()
    return any(n in ul for n in ("sticker", "monroney", "label"))


def packages_lacks_photo_equipment(packages_raw: Any) -> bool:
    """True when packages JSON has no meaningful photo-detected equipment yet."""
    if not packages_raw or not str(packages_raw).strip():
        return True
    try:
        pj = json.loads(packages_raw) if isinstance(packages_raw, str) else packages_raw
    except (TypeError, ValueError, json.JSONDecodeError):
        return True
    if not isinstance(pj, dict):
        return True
    try:
        scan_ver = int(pj.get("photo_equipment_scan_version") or 0)
    except (TypeError, ValueError):
        scan_ver = 0
    if scan_ver < PHOTO_EQUIPMENT_SCAN_VERSION:
        return True
    if pj.get("photo_equipment_scanned_at"):
        return False

    def _has_items(key: str) -> bool:
        v = pj.get(key)
        return isinstance(v, list) and any(str(x).strip() for x in v)

    if _has_items("observed_features") or _has_items("possible_packages") or _has_items("detected_adas"):
        return False
    if _has_items("sticker_options"):
        return False
    return True


def analyze_car_equipment_from_gallery(
    row: dict[str, Any],
    *,
    gallery_urls: list[str],
    analyze_url: Callable[[dict[str, Any], str], dict[str, Any] | None],
    merge_observations: Callable[[str | None, dict[str, Any]], str],
    force: bool = False,
) -> tuple[str | None, dict[str, Any]]:
    """
    Run equipment vision across selected gallery images and merge into packages JSON.

    *analyze_url* receives ``(row, url)`` and returns parsed vision JSON.
    """
    if not force and not packages_lacks_photo_equipment(row.get("packages")):
        return None, {"skipped": "already_scanned"}

    selected = pick_equipment_vision_urls(gallery_urls)
    if not selected:
        return None, {"skipped": "no_images"}

    merged_json: str | None = row.get("packages")
    stats: dict[str, Any] = {
        "urls_selected": len(selected),
        "urls_analyzed": 0,
        "features_added": 0,
    }

    batch_ok = (os.environ.get("EQUIPMENT_VISION_BATCH") or "1").strip().lower() not in (
        "0",
        "false",
        "no",
        "off",
    )
    if batch_ok:
        try:
            from backend.vision.claude_vision import analyze_equipment_from_image_urls

            referer = str(row.get("source_url") or row.get("_detail_url") or "").strip() or None
            vis = analyze_equipment_from_image_urls(
                selected,
                page_referer=referer if referer and referer.startswith("http") else None,
                max_images=equipment_vision_max_images(),
            )
            if vis:
                stats["urls_analyzed"] = len(selected)
                before = merged_json
                merged_json = merge_observations(merged_json, vis)
                if before != merged_json:
                    stats["features_added"] += 1
        except Exception as e:
            logger.debug("Equipment vision batch failed, falling back per-url: %s", e)
            batch_ok = False

    if not batch_ok or stats["urls_analyzed"] == 0:
        for url in selected:
            try:
                vis = analyze_url(row, url)
            except Exception as e:
                logger.debug("Equipment vision failed for url=%s: %s", url[:80], e)
                continue
            if not vis:
                continue
            stats["urls_analyzed"] += 1
            before = merged_json
            merged_json = merge_observations(merged_json, vis)
            if before != merged_json:
                stats["features_added"] += 1

    if not merged_json or stats["urls_analyzed"] == 0:
        return None, stats

    try:
        pj = json.loads(merged_json)
        if isinstance(pj, dict):
            pj["photo_equipment_scanned_at"] = int(time.time())
            pj["photo_equipment_scan_version"] = PHOTO_EQUIPMENT_SCAN_VERSION
            merged_json = json.dumps(pj, ensure_ascii=False)
    except (TypeError, ValueError, json.JSONDecodeError):
        pass

    return merged_json[:8000], stats


_ADAS_LABELS: dict[str, str] = {
    "acc": "Adaptive cruise control",
    "adaptive_cruise": "Adaptive cruise control",
    "adaptive_cruise_control": "Adaptive cruise control",
    "lka": "Lane keep assist",
    "lane_keep": "Lane keep assist",
    "lane_departure": "Lane departure warning",
    "hud": "Heads-up display",
    "360_cameras": "360° surround-view cameras",
    "surround_view": "360° surround-view cameras",
    "surround_view_camera": "360° surround-view cameras",
    "night_vision": "Night vision camera",
    "blind_spot": "Blind spot monitoring",
    "blind_spot_monitoring": "Blind spot monitoring",
    "parking_sensors": "Parking sensors",
    "front_parking_sensors": "Front parking sensors",
    "rear_parking_sensors": "Rear parking sensors",
}


def humanize_adas_token(raw: str) -> str:
    s = str(raw or "").strip()
    if not s:
        return ""
    key = re.sub(r"[\s\-]+", "_", s.lower())
    key = re.sub(r"[^a-z0-9_]", "", key)
    if key in _ADAS_LABELS:
        return _ADAS_LABELS[key]
    if key.endswith("_cameras") and "360" in key:
        return "360° surround-view cameras"
    return s.replace("_", " ").strip().title()


def collect_photo_detected_equipment(pj: dict[str, Any]) -> list[str]:
    """Merge observed features, possible packages, and ADAS into one deduped display list."""
    out: list[str] = []
    seen: set[str] = set()

    def _add(label: str) -> None:
        s = str(label or "").strip()[:200]
        if not s:
            return
        k = s.lower()
        if k in seen:
            return
        seen.add(k)
        out.append(s)

    for key in ("observed_features", "possible_packages"):
        v = pj.get(key)
        if isinstance(v, list):
            for item in v:
                _add(str(item))

    adas = pj.get("detected_adas")
    if isinstance(adas, list):
        for item in adas:
            _add(humanize_adas_token(str(item)))

    return filter_vision_feature_list(out)
