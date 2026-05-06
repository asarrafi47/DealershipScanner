"""
Ollama LLaVA (vision) client for cabin / interior inference and listing-gallery relevance.

Environment (document for operators):

- ``OLLAMA_HOST`` — base URL, default ``http://127.0.0.1:11434`` (same as enrichment).
- ``OLLAMA_VISION_MODEL`` — vision model id, default ``llava:13b``. Run ``ollama pull llava:13b``
  for the recommended default image.
- ``OLLAMA_INTERIOR_VISION_TIMEOUT_S`` — HTTP timeout seconds (default ``120``), used for
  interior analysis and listing-image classification.
- ``SCANNER_GALLERY_VISION_KEEP_UNFETCHABLE`` — when ``1``/``true``, a gallery URL is **kept** if
  the image could not be downloaded/encoded to JPEG (old behavior). When unset, those URLs are
  **dropped** so the UI list matches loadable car photos.
- ``SCANNER_GALLERY_VISION_FALLBACK_ON_EMPTY`` — when ``1``/``true``, if every image is classified as
  non-keep, **fall back** to the original URL list (heuristic lot order). **Default: ``0``** — allow
  an empty gallery rather than re-injecting junk. Set to ``1`` if you prefer any photo over none.
  Explicit non-keep categories and OEM/badge URL heuristics are excluded from re-injection.
- ``SCANNER_GALLERY_VISION_PASSTHROUGH_TRUSTED_CDN`` — when ``1``/``true`` (default), skip LLaVA for
  high-confidence dealer inventory image URLs (``/inv/``, ``vehicle-phot`` paths, VIN in path, etc.):
  keeps the full photo carousel as collected from the VDP without vision dropping real lot frames.
  Set to ``0`` to classify every URL (slower; may over-drop).

Persistence / merge thresholds live on the merge layer: ``INTERIOR_VISION_CONFIDENCE``,
``INTERIOR_VISION_OVERWRITE`` (see ``backend.vision.interior_vision_merge``). When no cabin
photo exists, ``backend.scanner.post_pipeline`` can still send the hero frame with
``inference_context=through_windows`` (see ``INTERIOR_VISION_FALLBACK_THROUGH_WINDOWS``).

The chat API returns JSON only (enforced in prompt). Interior buckets must be from the fixed
allowlist (see ``INTERIOR_BUCKET_ALLOWLIST`` below — keep aligned with
``backend.utils.interior_color_buckets``).
"""
from __future__ import annotations

import base64
import json
import logging
import os
import re
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any
from urllib.parse import unquote, urlparse

import requests

from backend.enrichment.service import _fetch_image_b64_optimized

logger = logging.getLogger(__name__)

# If truthy, URLs that fail HTTP decode or that look like blank/1×1 icons are *kept* in the gallery
# (old conservative behavior; bad for “grey box” / broken images in the UI).
# Default: unset → drop them.
def _env_keep_broken_gallery_images() -> bool:
    v = (os.environ.get("SCANNER_GALLERY_VISION_KEEP_UNFETCHABLE") or "").strip().lower()
    return v in ("1", "true", "yes", "on")


def _env_gallery_vision_fallback_on_empty() -> bool:
    v = (os.environ.get("SCANNER_GALLERY_VISION_FALLBACK_ON_EMPTY") or "0").strip().lower()
    return v in ("1", "true", "yes", "on")


def _env_gallery_vision_passthrough_trusted_cdn() -> bool:
    v = (os.environ.get("SCANNER_GALLERY_VISION_PASSTHROUGH_TRUSTED_CDN") or "1").strip().lower()
    return v in ("1", "true", "yes", "on")


# If every row failed before/during model scoring, do not re-inject original URLs in fallback.
# ``marketing_strip`` = auto-detected bar-shaped pixels. ``warranty_flyer_page`` = from the model, not
# heuristics (pixel-based triage misfired on real portrait lot photos while missing landscape promos).
# All explicit non-keep labels from the model should be here so we never re-add F&I / tile / stock art.
_GALLERY_NO_FALLBACK_CATEGORIES: frozenset[str] = frozenset(
    {
        "unfetchable",
        "blank_or_trivial",
        "vision_unavailable",
        "json_parse_failed",
        "marketing_strip",
        "warranty_flyer_page",
        "heuristic_dropped",
        "url_veto",
        "not_vehicle",
        "marketing",
        "dealer_badge",
        "legal_document",
        "warranty",
        "warranty_flyer",
        "oem_brochure",
        "stock_photo",
        "brand_graphic",
        "kbb",
        "fluff",
        "abstract",
    }
)

OLLAMA_HOST = os.environ.get("OLLAMA_HOST", "http://127.0.0.1:11434").rstrip("/")
OLLAMA_VISION_MODEL = os.environ.get("OLLAMA_VISION_MODEL", "llava:13b")
OLLAMA_INTERIOR_VISION_TIMEOUT_S = float(os.environ.get("OLLAMA_INTERIOR_VISION_TIMEOUT_S", "120"))

# Last request diagnostics for operator-facing CLIs (best-effort).
_LAST_OLLAMA_ERROR: str | None = None
_LAST_OLLAMA_RAW_SNIPPET: str | None = None


def last_ollama_diagnostics() -> dict[str, Any]:
    """Best-effort diagnostics from the last Ollama chat attempt (for CLI output)."""
    return {
        "last_error": _LAST_OLLAMA_ERROR,
        "last_raw_snippet": _LAST_OLLAMA_RAW_SNIPPET,
        "host": OLLAMA_HOST,
        "model": OLLAMA_VISION_MODEL,
        "timeout_s": OLLAMA_INTERIOR_VISION_TIMEOUT_S,
    }

# Fixed allowlist for model output (subset of lexicon buckets; "other" catches remainder).
INTERIOR_BUCKET_ALLOWLIST: tuple[str, ...] = (
    "black",
    "white",
    "gray",
    "silver",
    "red",
    "blue",
    "green",
    "brown",
    "tan",
    "beige",
    "orange",
    "yellow",
    "other",
)

EXTERIOR_BUCKET_ALLOWLIST: tuple[str, ...] = INTERIOR_BUCKET_ALLOWLIST

_SYSTEM_PROMPT = (
    "You analyze a vehicle interior/cabin photo. Prioritize **seat upholstery color** as the primary "
    "signal for the interior color (dash/trim can be secondary). Reply with STRICT JSON only, no markdown, "
    "no prose outside JSON. Keys: interior_buckets (array of strings from this fixed set only: "
    + ", ".join(INTERIOR_BUCKET_ALLOWLIST)
    + "), interior_guess_text (short human label INCLUDING a color, e.g. 'Black leather' or 'Tan/Black'), confidence (0.0-1.0 float), "
    "evidence (one short phrase; mention the seats if visible). If unsure, use interior_buckets [\"other\"] and low confidence."
)

_SYSTEM_PROMPT_THROUGH_WINDOWS = (
    "You infer **passenger cabin** colors from a vehicle listing image that may be an **exterior** "
    "shot. Use only what is visible **through side or rear windows or windshield** (seats, dash, "
    "door panels, headliner). Prioritize **seat upholstery color** if seats are visible. "
    "Do **not** guess from body paint, wheels, or reflections you cannot "
    "resolve as interior. Reply with STRICT JSON only, no markdown, no prose outside JSON. Keys: "
    "interior_buckets (array of strings from this fixed set only: "
    + ", ".join(INTERIOR_BUCKET_ALLOWLIST)
    + "), interior_guess_text (short human label INCLUDING a color), confidence (0.0-1.0 float), "
    "evidence (one short phrase; mention the seats if visible). If little or no interior is visible, use interior_buckets "
    '[\"other\"], low confidence, and evidence stating visibility limits.'
)

_USER_INTERIOR_CABIN = (
    "Analyze this vehicle interior/cabin image. Focus on the **seat upholstery color** first."
)

_USER_INTERIOR_THROUGH_WINDOWS = (
    "This may be an outside view of the vehicle. Describe **interior upholstery and trim colors** "
    "visible **only through the glass**. Focus on the **seat upholstery color** if you can see seats. "
    "If you cannot see inside, say so and use low confidence."
)

_EXTERIOR_SYSTEM_PROMPT = (
    "You analyze a vehicle photo to identify the **exterior body paint color**. "
    "Ignore the interior, wheels, trim strips, and window glass. "
    "Reply with STRICT JSON only, no markdown, no prose outside JSON. "
    "Keys: exterior_buckets (array of strings from this fixed set only: "
    + ", ".join(EXTERIOR_BUCKET_ALLOWLIST)
    + "), exterior_guess_text (short human label INCLUDING a color, e.g. 'Pearl White' or 'Midnight Blue'), "
    "confidence (0.0-1.0 float), evidence (one short phrase describing the visible paint). "
    "If the image is an interior-only shot with no body paint visible, use exterior_buckets [\"other\"] and confidence 0.1."
)

_USER_EXTERIOR = "Identify the **exterior body paint color** of this vehicle."


def _guess_text_from_buckets(buckets: list[str]) -> str:
    out = [b for b in buckets if isinstance(b, str) and b and b.lower() != "other"]
    if not out:
        return ""
    # Keep stable order; title-case for display.
    return " / ".join(str(b).strip().title() for b in out)


_GENERIC_GUESS_TOKENS: frozenset[str] = frozenset(
    {
        "leather",
        "cloth",
        "suede",
        "alcantara",
        "vinyl",
        "upholstery",
        "seats",
        "seat",
        "interior",
        "trim",
    }
)

_MATERIAL_TOKENS: tuple[str, ...] = ("leather", "cloth", "suede", "alcantara", "vinyl")

_LISTING_IMAGE_SYSTEM_PROMPT = (
    "You triage one image from a vehicle listing photo gallery. Reply with STRICT JSON only, no markdown.\n"
    "Default: **keep=false**. Set **keep=true** only when the image’s main subject is unambiguously one of:\n"
    "  (1) The **exterior of a car** (body, paint, glass, wheels/tires, lights — a real vehicle in frame).\n"
    "  (2) The **interior of a car** (cabin, seats, dashboard, center stack, door cards, headliner, cargo area — inside the vehicle).\n"
    "  (3) A **close-up of a car interior part** that is clearly automotive (e.g. steering wheel, shifter, seat, vent, screen, door controls) with vehicle context.\n"
    "  (4) **Engine bay, trunk, or under-vehicle** of a real car (not a generic product photo or parts diagram).\n"
    "  (5) A **Monroney / window sticker** (legible label with VIN, price, or equipment lines).\n"
    "If the image is **not** one of the above — for example: abstract patterns, brand stripes/logos with no car, KBB/Carfax/emblems, "
    "warranty or legal pages, **F&I / GAP / Vehicle Protection Plan** style promotional strips or badges (text about coverage, 24/7, "
    "warranty, service contract) with no car as the main subject, maps, people, building, floor only, color swatches, boxes of accessories, "
    "OEM brochure white-sweep, a **full-page** F&I / **Vehicle Protection Plan** / **PLAN OVERVIEW** style document (mostly text, bulleted coverage levels, warranty marketing copy) with little or no real car as the main subject, "
    "or anything where you are not sure it is a real car or real car interior — you **must** set keep=false.\n"
    "For **category** when keep=true, prefer these exact labels: exterior, interior, cabin, engine_bay, "
    "trunk, window_sticker (or interior_detail for close-ups). You may also use vehicle, car, or lot_photo "
    "for real on-lot car photos; they are treated as exterior. For junk use not_vehicle, marketing, "
    "brand_graphic, oem_brochure, or similar.\n"
    "Required keys: keep (boolean), category (string), confidence (0.0-1.0). **keep must be false unless the subject is clearly a car exterior, car interior, car interior component, under-hood/trunk of a car, or Monroney.**"
)

# Only these categories, with explicit keep=true from the model, count as a gallery keep.
_LISTING_GOOD_CATEGORIES: frozenset[str] = frozenset(
    {
        "exterior",
        "interior",
        "interior_detail",
        "cabin",
        "window_sticker",
        "sticker",
        "monroney",
        "monroney_label",
        "window_label",
        "engine_bay",
        "under_hood",
        "trunk",
        "cargo",
        "wheel",
        "tire",
    }
)
_LISTING_BAD_CATEGORIES: frozenset[str] = frozenset(
    {
        "not_vehicle",
        "irrelevant",
        "dealership",
        "logo",
        "logo_only",
        "dealer_badge",
        "sign",
        "map",
        "building",
        "staff",
        "marketing",
        "legal",
        "legal_document",
        "waiver",
        "warranty",
        "warranty_flyer",
        "fi",
        "document",
        "recall",
        "promo",
        "unreadable",
        "placeholder",
        "oem_brochure",
        "oem_studio",
        "press_photo",
        "brochure",
        "stock_photo",
        "catalog",
        "brand_graphic",
        "brand_pattern",
        "m_livery",
        "racing_stripes",
        "vpp",
        "vehicle_protection",
        "protection_plan",
        "f_and_i",
        "kbb",
        "guide_badge",
        "abstract",
        "pattern",
        "fluff",
        "icon",
        "lifestyle",
        "scenery",
        "warranty_flyer_page",
    }
)


# Third-party badge / guide hosts — never a vehicle photo, skip vision
_GALLERY_HOST_DROP_SUBSTR: frozenset[str] = frozenset(
    {
        "kbb.com",
        "kbb-",
        "kelleybluebook",
    }
)
_VIN_LIKE_IN_URL_RE = re.compile(r"[^a-z0-9]([a-hj-npr-z0-9]{11,17})[^a-z0-9]", re.I)
# Do not mark these host substrings as fluff (dealer DMS / lot photography CDNs)
_DEALER_CDN_KEEP_HINTS: frozenset[str] = frozenset(
    {
        "pictures.dealer",
        "dealerinspire",
        "cstatic-images",
        "getvehicleimage",
        "invphoto",
        "invphotos",
        "inventoryphoto",
    }
)


def heuristic_listing_gallery_fluff_url(url: str) -> bool:
    """
    True if the URL is very unlikely to be a real **dealer lot** inventory image (OEM stock, F&I
    tiles, KBB, CARFAX badges, iPacket, BMW press/configurator, etc.).

    Used: (1) before calling LLaVA, (2) after a keep to veto OEM URLs the model sometimes mislabels.
    """
    if not url or not isinstance(url, str):
        return True
    s = url.strip()
    sl = s.lower()
    if not sl.startswith("http"):
        return True
    if any(keep in sl for keep in _DEALER_CDN_KEEP_HINTS):
        if any(
            x in sl
            for x in ("/inv/", "vehicle-phot", "vdp-phot", "getvehicleimage", "invphoto", "invphotos")
        ):
            return False
    try:
        p = urlparse(s)
        h = (p.netloc or "").lower()
        path = unquote((p.path or "").lower())
    except Exception as e:
        logger.debug("heuristic_listing_gallery_fluff_url: urlparse failed for %r: %s", s[:300], e)
        return True
    for needle in _GALLERY_HOST_DROP_SUBSTR:
        if needle in h or needle in path:
            return True
    # Spyne 3D renders are synthetic CG models — not real photos, always drop
    if "spyne" in h:
        fname = path.rsplit("/", 1)[-1].split("?")[0]
        if fname.startswith("3d_renders_"):
            return True
    if "autotrader.com" in h and ("/kbb" in sl or "kelley" in sl):
        return True
    if h.endswith("carfax.com") and ("/img/" in sl or "brand" in sl or "badge" in sl or "1-owner" in sl or "1owner" in sl):
        return True
    if "cfximg" in h and "carfax" in h:
        return True
    if "ipacket" in sl or "i-packet" in sl or "i_packet" in sl:
        return True
    # BMW / global OEM: marketing, press, build-your-own — not the dealer's camera roll.
    if "media.bmw.com" in h or "buildyour.bmw" in h or "configurator" in sl:
        return True
    if h.endswith("bmw.com") and ("binaries" in sl or "/content/dam" in sl):
        return True
    if "static.bmwusa.com" in h or "cache.bmwusa.com" in h:
        if not any(
            x in sl
            for x in (
                "vehicle-phot",
                "vdp-phot",
                "inventory",
                "dealer",
                "invphot",
            )
        ):
            return True
    # BMW / M brand slop (stripes, roundel mark, lifestyle packs) — not vehicle listing photos
    if not any(
        x in sl
        for x in ("/inv/", "vehicle-phot", "vdp-phot", "getvehicleimage", "invphot", "dealerinspire/inv")
    ):
        for frag in (
            "m-stripe",
            "m_stripes",
            "mperformance",
            "m-performance",
            "m-composition",
            "lifestyle-",
            "/lifestyle/",
            "brand-hero",
            "oempack",
            "roundel-",
            "m-badge",
            "m_series_hero",
            "mpower-",
        ):
            if frag in sl:
                return True
    if "edmunds.com" in h and ("/g/" in sl or "/static/img" in sl or "kbb" in sl or "icon" in sl):
        return True
    _fi_path_needles: tuple[str, ...] = (
        "/vpp/",
        "vpp-",
        "-vpp-",
        "vpp_",
        "vehicleprotection",
        "vehicle_protection",
        "vehicle-protection",
        "vehicleprotectionplan",
        "protectionplan",
        "protection_plan",
        "warrantybadge",
        "warranty_badge",
        "fi_badge",
        "f&i_",
        "f&i-",
        "f&i.",
        "fandibadge",
        "gap_logo",
        "plan_overview",
        "planoverview",
        "plan-overview",
        "coveragelevel",
        "covered_care",
        "ipocket",
    )
    q = unquote((p.query or "").lower())
    pq = f"{path}?{q}"
    for nd in _fi_path_needles:
        if nd in pq:
            return True
    return False


def heuristic_drop_gallery_listing_url(url: str) -> bool:
    """
    True = drop the URL without calling the vision model (see :func:`heuristic_listing_gallery_fluff_url`).
    """
    return heuristic_listing_gallery_fluff_url(url)


def dealer_lot_photo_score(url: str) -> int:
    """
    Higher = more likely a dealer-photographed listing asset (for hero / sort order). Heuristic only.
    """
    if not url:
        return 0
    u = unquote(url).lower()
    n = 0
    for token in (
        "/inv",
        "/inventory",
        "vehicle-phot",
        "vdp-phot",
        "in_transit",
        "stock_",
        "getvehicleimage",
        "vehicleimage",
    ):
        if token in u:
            n += 40
    if _VIN_LIKE_IN_URL_RE.search(u) or re.search(
        r"vin[_\s=%\-][a-hj-npr-z0-9]{6,20}", u, re.I
    ):
        n += 28
    for token in (
        "dealerinspire",
        "cstatic-images",
        "cloudinary",
        "s3.amazonaws.com",
        "cloudfront",
        "azureedge",
    ):
        if token in u:
            n += 8
    # Spyne bg-removed photos are clean listing shots — lift above plain originals
    if "spyne" in u:
        fname = u.rsplit("/", 1)[-1].split("?")[0]
        if fname.startswith("car_replace_bg_"):
            n += 20
    for token in ("buildyour", "configurator", "bmgusa", "bimmerpost"):
        if token in u:
            n -= 22
    if "media.bmw.com" in u or (".bmw.com" in u and "binaries" in u):
        n -= 16
    # De-prioritize common F&I / plan-artifact paths (still classified by vision, but fall behind true lot
    # shots for hero and empty-Gallery fallback). Broad tokens only where inventory URLs rarely use them.
    for token in (
        "vpp-",
        "/vpp/",
        "-vpp-",
        "vpp_",
        "vehicleprotect",
        "protectionplan",
        "warranty_badge",
        "warrantybadge",
        "fandi",
        "fi_badge",
        "plan_overview",
        "planoverview",
        "plan-overview",
        "coveragelevel",
    ):
        if token in u:
            n -= 32
    return n


def dealer_listing_gallery_passthrough_vision(url: str) -> bool:
    """
    True when the URL is very likely a real **inventory** photo (not a main-page F&I tile) and
    **safe to keep without** running listing LLaVA — preserves full VDP carousels on trusted CDNs.

    Requires a strong lot signal from :func:`dealer_lot_photo_score` and not a
    :func:`heuristic_listing_gallery_fluff_url` match.
    """
    if not url or not isinstance(url, str):
        return False
    if heuristic_listing_gallery_fluff_url(url):
        return False
    if dealer_lot_photo_score(url) < 40:
        return False
    return True


def _category_hero_priority(category: str) -> int:
    c = (category or "").strip().lower().replace(" ", "_").replace("-", "_")
    return {
        "exterior": 48,
        "interior": 32,
        "cabin": 32,
        "listing_cdn_passthrough": 40,
        "window_sticker": 22,
        "sticker": 22,
        "monroney": 24,
        "monroney_label": 24,
        "window_label": 24,
    }.get(c, 4)


def _extract_json_object(text: str) -> dict[str, Any] | None:
    if not text:
        return None
    t = text.strip()
    try:
        return json.loads(t)
    except json.JSONDecodeError:
        pass
    m = re.search(r"\{[\s\S]*\}", t)
    if not m:
        return None
    try:
        return json.loads(m.group(0))
    except json.JSONDecodeError:
        return None


def _ollama_vision_chat_json(
    *,
    system: str,
    user_text: str,
    image_b64_jpeg: str,
    timeout_s: float | None = None,
) -> str | None:
    global _LAST_OLLAMA_ERROR, _LAST_OLLAMA_RAW_SNIPPET
    payload = {
        "model": OLLAMA_VISION_MODEL,
        "stream": False,
        "messages": [
            {"role": "system", "content": system},
            {
                "role": "user",
                "content": user_text,
                "images": [image_b64_jpeg],
            },
        ],
    }
    t = float(OLLAMA_INTERIOR_VISION_TIMEOUT_S if timeout_s is None else timeout_s)
    url = f"{OLLAMA_HOST}/api/chat"
    _LAST_OLLAMA_ERROR = None
    _LAST_OLLAMA_RAW_SNIPPET = None
    # One retry helps with transient 5xx / connection churn.
    for attempt in (1, 2):
        try:
            resp = requests.post(url, json=payload, timeout=t)
            resp.raise_for_status()
            data = resp.json()
            break
        except Exception as e:
            _LAST_OLLAMA_ERROR = f"{type(e).__name__}: {e}"
            if attempt >= 2:
                logger.warning("Ollama LLaVA request failed: %s", e)
                return None
    msg = (data.get("message") or {}) if isinstance(data, dict) else {}
    content = msg.get("content") if isinstance(msg, dict) else None
    if not isinstance(content, str):
        _LAST_OLLAMA_ERROR = "Ollama response missing message.content"
        return None
    _LAST_OLLAMA_RAW_SNIPPET = content[:300]
    return content


def analyze_interior_from_image_url(
    image_url: str,
    *,
    inference_context: str = "cabin",
) -> dict[str, Any] | None:
    """
    Call Ollama chat with images for *image_url*. Returns parsed dict or ``None``.

    ``inference_context``: ``\"cabin\"`` (direct interior photo) or ``\"through_windows\"``
    (exterior / hero shot — model is asked to read cabin only through glass).

    On failure logs and returns ``None`` (callers decide whether to persist).
    """
    b64 = _fetch_image_b64_optimized(image_url)
    if not b64:
        return None
    return analyze_interior_from_image_b64(b64, inference_context=inference_context)


def analyze_interior_from_image_b64(
    image_b64_jpeg: str,
    *,
    inference_context: str = "cabin",
) -> dict[str, Any] | None:
    ctx = (inference_context or "cabin").strip().lower().replace("-", "_")
    if ctx == "through_windows":
        system = _SYSTEM_PROMPT_THROUGH_WINDOWS
        user_text = _USER_INTERIOR_THROUGH_WINDOWS
    else:
        system = _SYSTEM_PROMPT
        user_text = _USER_INTERIOR_CABIN
    content = _ollama_vision_chat_json(
        system=system,
        user_text=user_text,
        image_b64_jpeg=image_b64_jpeg,
    )
    if not content:
        return None
    parsed = _extract_json_object(content)
    if not isinstance(parsed, dict):
        # Model sometimes violates the "STRICT JSON only" instruction, especially on
        # through-windows exterior shots when it cannot see seats. Return a safe low-confidence
        # payload instead of None so CLIs can show a consistent schema and callers can decide.
        low = content.strip().lower()
        allow = set(INTERIOR_BUCKET_ALLOWLIST)
        found: list[str] = []
        for b in INTERIOR_BUCKET_ALLOWLIST:
            if b == "other":
                continue
            if re.search(rf"\b{re.escape(b)}\b", low) and b in allow and b not in found:
                found.append(b)
        buckets = found if found else ["other"]
        guess_s = _guess_text_from_buckets(buckets)
        material: str | None = None
        for m in _MATERIAL_TOKENS:
            if re.search(rf"\b{re.escape(m)}\b", low):
                material = m
                break
        if guess_s and material:
            guess_s = f"{guess_s} {material}".strip()
        # Keep this low: the model didn't follow the schema, but we can still salvage signal.
        confidence = 0.12 if buckets != ["other"] else 0.05

        ev = content.strip().replace("\n", " ")[:160]
        if not ev:
            ev = "model returned non-JSON output"
        return {
            "interior_buckets": buckets,
            "interior_guess_text": guess_s,
            "confidence": confidence,
            "evidence": ev,
            "model": OLLAMA_VISION_MODEL,
            "image_b64_len": len(image_b64_jpeg),
            "inference_context": "through_windows" if ctx == "through_windows" else "cabin",
            "parse_error": "non_json",
        }
    raw_buckets = parsed.get("interior_buckets")
    buckets: list[str] = []
    if isinstance(raw_buckets, list):
        allow = set(INTERIOR_BUCKET_ALLOWLIST)
        for x in raw_buckets:
            b = str(x).strip().lower()
            if b in allow and b not in buckets:
                buckets.append(b)
    if not buckets:
        buckets = ["other"]
    guess = parsed.get("interior_guess_text")
    evidence = parsed.get("evidence")
    conf_raw = parsed.get("confidence")
    try:
        confidence = float(conf_raw)
    except (TypeError, ValueError):
        confidence = 0.0
    confidence = max(0.0, min(1.0, confidence))
    guess_s = str(guess).strip() if guess is not None else ""
    # LLaVA sometimes answers material only (e.g. "leather") — force a color label from buckets.
    if guess_s and guess_s.strip().lower() in _GENERIC_GUESS_TOKENS:
        label = _guess_text_from_buckets(buckets)
        if label:
            guess_s = f"{label} {guess_s}".strip()
    elif not guess_s:
        label = _guess_text_from_buckets(buckets)
        if label:
            guess_s = label
    return {
        "interior_buckets": buckets,
        "interior_guess_text": guess_s,
        "confidence": confidence,
        "evidence": str(evidence).strip() if evidence is not None else "",
        "model": OLLAMA_VISION_MODEL,
        "image_b64_len": len(image_b64_jpeg),
        "inference_context": "through_windows" if ctx == "through_windows" else "cabin",
    }


def analyze_exterior_color_from_image_url(image_url: str) -> dict[str, Any] | None:
    """Analyze vehicle body paint color from image URL."""
    b64 = _fetch_image_b64_optimized(image_url)
    if not b64:
        return None
    return analyze_exterior_color_from_image_b64(b64)


def analyze_exterior_color_from_image_b64(image_b64_jpeg: str) -> dict[str, Any] | None:
    """Analyze vehicle body paint color from base64 JPEG."""
    content = _ollama_vision_chat_json(
        system=_EXTERIOR_SYSTEM_PROMPT,
        user_text=_USER_EXTERIOR,
        image_b64_jpeg=image_b64_jpeg,
    )
    if not content:
        return None
    parsed = _extract_json_object(content)
    if not isinstance(parsed, dict):
        # salvage low-confidence from unstructured text
        low = content.strip().lower()
        allow = set(EXTERIOR_BUCKET_ALLOWLIST)
        found = [b for b in EXTERIOR_BUCKET_ALLOWLIST if b != "other" and re.search(rf"\b{re.escape(b)}\b", low)]
        buckets = found if found else ["other"]
        return {
            "exterior_buckets": buckets,
            "exterior_guess_text": " / ".join(b.title() for b in buckets if b != "other"),
            "confidence": 0.12 if buckets != ["other"] else 0.05,
            "evidence": content.strip()[:160],
            "model": OLLAMA_VISION_MODEL,
            "parse_error": "non_json",
        }
    raw_buckets = parsed.get("exterior_buckets")
    buckets = []
    if isinstance(raw_buckets, list):
        allow = set(EXTERIOR_BUCKET_ALLOWLIST)
        buckets = [b for b in raw_buckets if isinstance(b, str) and b in allow]
    if not buckets:
        buckets = ["other"]
    guess = (parsed.get("exterior_guess_text") or "").strip()
    if not guess:
        guess = " / ".join(b.title() for b in buckets if b != "other")
    conf = float(parsed.get("confidence", 0.0))
    conf = max(0.0, min(1.0, conf))
    return {
        "exterior_buckets": buckets,
        "exterior_guess_text": guess,
        "confidence": conf,
        "evidence": (parsed.get("evidence") or "").strip(),
        "model": OLLAMA_VISION_MODEL,
        "image_b64_len": len(image_b64_jpeg),
    }


def _coerce_parsed_keep(val: Any) -> bool | None:
    """
    None = missing/invalid; callers treat as **not** a keep. Explicit false drops the image.
    """
    if val is None:
        return None
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)):
        if val == 1:
            return True
        if val == 0:
            return False
        return None
    if isinstance(val, str):
        s = val.strip().lower()
        if s in ("true", "1", "yes", "y", "on"):
            return True
        if s in ("false", "0", "no", "n", "off"):
            return False
    return None


def _category_allows_gallery_keep(cat_resolved: str) -> bool:
    if cat_resolved in _LISTING_GOOD_CATEGORIES:
        return True
    if any(x in cat_resolved for x in ("monroney", "sticker", "window_label", "window_sticker")):
        return True
    return False


# When the model says keep=true but uses a near-synonym label, map to a canonical _LISTING_GOOD_CATEGORIES.
_LISTING_CATEGORY_SYNONYMS: dict[str, str] = {
    "vehicle": "exterior",
    "car": "exterior",
    "suv": "exterior",
    "truck": "exterior",
    "automobile": "exterior",
    "exterior_shot": "exterior",
    "exterior_view": "exterior",
    "outside": "exterior",
    "body": "exterior",
    "dealer_lot": "exterior",
    "lot_photo": "exterior",
    "listing_photo": "exterior",
    "vdp": "exterior",
    "vdp_photo": "exterior",
    "dealer_photo": "exterior",
    "showroom": "exterior",
    "inside": "interior",
    "insides": "interior",
    "dash": "interior_detail",
    "dashboard": "interior_detail",
    "seats": "interior",
    "cockpit": "interior",
    "hood": "engine_bay",
    "bonnet": "engine_bay",
    "engine": "engine_bay",
    "undercarriage": "exterior",
}


def _map_category_synonym_to_canonical(cat_raw: str) -> str:
    c = str(cat_raw or "").strip().lower().replace(" ", "_").replace("-", "_")
    if not c:
        return c
    if c in _LISTING_CATEGORY_SYNONYMS:
        return _LISTING_CATEGORY_SYNONYMS[c]
    if c in _LISTING_GOOD_CATEGORIES or c in _LISTING_BAD_CATEGORIES:
        return c
    return c


def _listing_image_keep_from_parsed(parsed: dict[str, Any] | None) -> bool:
    """
    Keep only when the model explicitly sets keep=true **and** the category is a real vehicle
    or sticker class. (Previously, ``exterior``/``interior`` alone forced a keep and mislabeled
    fluff could slip through.) Near-synonyms (e.g. ``vehicle``, ``car``) map to canonical types.
    """
    if not isinstance(parsed, dict):
        return False
    cat_raw = str(parsed.get("category") or "").strip().lower().replace(" ", "_").replace("-", "_")
    if cat_raw in _LISTING_BAD_CATEGORIES:
        return False
    if _coerce_parsed_keep(parsed.get("keep")) is not True:
        return False
    resolved = _map_category_synonym_to_canonical(cat_raw)
    if resolved in _LISTING_BAD_CATEGORIES:
        return False
    return _category_allows_gallery_keep(resolved)


def _is_trivially_blank_or_icon_b64(b64_jpeg: str) -> bool:
    """
    Drop without calling the LLM: tiny/icon canvas, or nearly solid-color (placeholders, failed loads).
    """
    try:
        from PIL import Image  # type: ignore[import-untyped]

        raw = base64.b64decode(b64_jpeg)
    except (ValueError, OSError) as e:
        logger.debug("Trivially-blank: base64 decode failed: %s", e)
        return True
    if len(raw) < 120:
        return True
    try:
        im = Image.open(BytesIO(raw))
    except OSError as e:
        logger.debug("Trivially-blank: PIL open failed: %s", e)
        return True
    w, h = im.size
    if w * h < 1:
        return True
    if max(w, h) < 60:
        return True
    iml = im.convert("L")
    w2, h2 = min(96, w), min(96, h)
    iml = iml.resize((w2, h2))
    seq = list(iml.getdata())
    n = len(seq)
    if n < 1:
        return True
    m = sum(float(p) for p in seq) / n
    var = sum((float(p) - m) ** 2 for p in seq) / n
    st = var**0.5
    # Solid-color / empty CDN placeholders: real photos (even night shots) have higher edge/texture.
    if st < 0.5:
        return True
    return False


def _b64_looks_like_marketing_strip(b64_jpeg: str) -> bool:
    """
    True for very short vertical or horizontal *strip* images typical of F&I / VPP / warranty banners
    (often misread by vision as a vehicle). Normal listing photos and 16:9/4:3 shots return False.
    On decode/PIL errors, return False (do not drop real photos on uncertainty).
    """
    try:
        from PIL import Image  # type: ignore[import-untyped]

        raw = base64.b64decode(b64_jpeg)
    except (ValueError, OSError):
        return False
    if len(raw) < 200:
        return False
    try:
        im = Image.open(BytesIO(raw))
    except OSError:
        return False
    w, h = im.size
    if w < 32 or h < 32:
        return False
    a, b = (w, h) if w <= h else (h, w)
    if a < 1:
        return False
    # Shortest side small + bar-shaped (not phone portrait / 16:9 in reasonable pixel height).
    if a <= 220 and b >= 2.5 * a:
        return True
    return False


def _b64_gallery_fluff_category(b64_jpeg: str) -> str | None:
    # Only *thin* bar-shaped assets. Do not auto-classify “portrait” pages: many real lot photos are
    # 3:4/4:5, while a lot of F&I / VPP artwork is 16:9/landscape (missed) — rely on the LLM + URL rules.
    if _b64_looks_like_marketing_strip(b64_jpeg):
        return "marketing_strip"
    return None


def classify_listing_image_from_url(image_url: str, *, page_referer: str | None = None) -> dict[str, Any] | None:
    """
    Returns ``{"keep": bool, "category": str, "confidence": float, ...}`` or, when
    ``SCANNER_GALLERY_VISION_KEEP_UNFETCHABLE=1`` and the image could not be fetched, ``None``
    (gallery filter keeps the URL in that mode only).

    When the model returns ``keep: true`` but the URL still matches
    :func:`heuristic_listing_gallery_fluff_url` (OEM/press, F&I paths, etc.), the result is
    coerced to ``keep: false`` and ``category: "url_veto"``.

    ``page_referer`` should be the vehicle listing (VDP) URL when available so inventory CDN images
    fetch with a browser-like Referer (many dealers block hotlinked requests without it).
    """
    b64 = _fetch_image_b64_optimized(image_url, referer=page_referer)
    if not b64:
        if _env_keep_broken_gallery_images():
            return None
        return {
            "keep": False,
            "category": "unfetchable",
            "confidence": 0.0,
            "model": OLLAMA_VISION_MODEL,
            "image_b64_len": 0,
        }
    if _is_trivially_blank_or_icon_b64(b64):
        if _env_keep_broken_gallery_images():
            return None
        return {
            "keep": False,
            "category": "blank_or_trivial",
            "confidence": 0.0,
            "model": OLLAMA_VISION_MODEL,
            "image_b64_len": len(b64),
        }
    fluff = _b64_gallery_fluff_category(b64)
    if fluff:
        if _env_keep_broken_gallery_images():
            return None
        return {
            "keep": False,
            "category": fluff,
            "confidence": 0.0,
            "model": OLLAMA_VISION_MODEL,
            "image_b64_len": len(b64),
        }
    out = classify_listing_image_from_image_b64(b64)
    if not isinstance(out, dict):
        return out
    if out.get("keep") and heuristic_listing_gallery_fluff_url(image_url):
        try:
            conf = float(out.get("confidence") or 0.0)
        except (TypeError, ValueError):
            conf = 0.0
        return {
            **out,
            "keep": False,
            "category": "url_veto",
            "confidence": min(conf, 0.4),
        }
    return out


def classify_listing_image_from_image_b64(image_b64_jpeg: str) -> dict[str, Any] | None:
    if _is_trivially_blank_or_icon_b64(image_b64_jpeg):
        return {
            "keep": False,
            "category": "blank_or_trivial",
            "confidence": 0.0,
            "model": OLLAMA_VISION_MODEL,
            "image_b64_len": len(image_b64_jpeg),
        }
    content = _ollama_vision_chat_json(
        system=_LISTING_IMAGE_SYSTEM_PROMPT,
        user_text=(
            "Triage: keep=true only for a **real** vehicle photo: exterior, interior, engine bay/trunk, "
            "or Monroney. keep=false for **any** F&I / GAP / Vehicle Protection / warranty **marketing** asset: "
            "wide **landscape** banners, **full-page** plan documents with headings like PLAN OVERVIEW, "
            "text bullet lists, coverage level graphics, 24/7 or protection-plan badges, or stock-people in "
            "a corner of a text-heavy slide — even if a car is partly visible. For those, category=marketing, "
            "warranty, or warranty_flyer_page and keep=false. Output JSON: keep, category, confidence."
        ),
        image_b64_jpeg=image_b64_jpeg,
    )
    if not content:
        return {
            "keep": False,
            "category": "vision_unavailable",
            "confidence": 0.0,
            "model": OLLAMA_VISION_MODEL,
            "image_b64_len": len(image_b64_jpeg),
        }
    parsed = _extract_json_object(content)
    if not isinstance(parsed, dict):
        return {
            "keep": False,
            "category": "json_parse_failed",
            "confidence": 0.0,
            "model": OLLAMA_VISION_MODEL,
            "image_b64_len": len(image_b64_jpeg),
        }
    keep = _listing_image_keep_from_parsed(parsed)
    fluff = _b64_gallery_fluff_category(image_b64_jpeg)
    if keep and fluff:
        # Model sometimes labels F&I / warranty artwork as a vehicle; pixels do not lie.
        keep = False
        cat = fluff
    else:
        cat = str(parsed.get("category") or "").strip()[:64]
    conf_raw = parsed.get("confidence")
    try:
        confidence = float(conf_raw)
    except (TypeError, ValueError):
        confidence = 0.0
    confidence = max(0.0, min(1.0, confidence))
    return {
        "keep": keep,
        "category": cat,
        "confidence": confidence,
        "model": OLLAMA_VISION_MODEL,
        "image_b64_len": len(image_b64_jpeg),
    }


def _classify_gallery_indexed(
    pair: tuple[int, str],
    *,
    page_referer: str | None = None,
) -> tuple[int, str, bool, str, float]:
    i, u = pair
    parsed = classify_listing_image_from_url(u, page_referer=page_referer)
    if parsed is None:
        return i, u, True, "unknown", 0.0
    try:
        conf = float(parsed.get("confidence") or 0.0)
    except (TypeError, ValueError):
        conf = 0.0
    conf = max(0.0, min(1.0, conf))
    return (
        i,
        u,
        bool(parsed.get("keep", False)),
        str(parsed.get("category") or "unknown"),
        conf,
    )


def _sort_gallery_tuples(
    items: list[tuple[int, str, bool, str, float]],
) -> list[str]:
    kept = [t for t in items if t[2]]
    if not kept:
        return []

    def key_fn(t: tuple[int, str, bool, str, float]) -> tuple:
        i, u, _ok, cat, conf = t
        pri = dealer_lot_photo_score(u) + _category_hero_priority(cat) + int(conf * 10.0)
        return (-pri, i)

    kept.sort(key=key_fn)
    return [t[1] for t in kept]


def filter_gallery_urls_for_vehicle_listing(
    urls: list[str],
    *,
    max_workers: int = 1,
    page_referer: str | None = None,
) -> list[str]:
    """
    Deduplicate URLs, drop known third-party guide/badge hosts, then either **passthrough** trusted
    dealer-inventory CDNs (no LLaVA) or classify with LLaVA, then **sort** kept images with dealer
    lot / exterior shots first (heuristic URL score + category + model confidence). Duplicate URLs
    are skipped. If ``classify`` returns ``None`` (only when
    ``SCANNER_GALLERY_VISION_KEEP_UNFETCHABLE=1`` and fetch/encode failed), the URL is **kept** for
    legacy CDN-flake tolerance (low hero priority). If no image is kept and
    ``SCANNER_GALLERY_VISION_FALLBACK_ON_EMPTY=1``, returns a subset of the original **loadable**
    candidates (excluding known marketing/OEM heuristics) ordered by dealer-lot score. Default: empty
    gallery rather than re-injecting junk.

    ``page_referer`` should be the VDP / listing page URL; it is sent as the HTTP ``Referer`` when
    downloading each gallery image (many CDNs return 403 without it).
    """
    seen: set[str] = set()
    ordered: list[str] = []
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
    passthrough_pairs: list[tuple[int, str]] = []
    vision_indexed: list[tuple[int, str]] = []
    for i, u in enumerate(ordered):
        if heuristic_drop_gallery_listing_url(u):
            continue
        if _env_gallery_vision_passthrough_trusted_cdn() and dealer_listing_gallery_passthrough_vision(
            u
        ):
            passthrough_pairs.append((i, u))
        else:
            vision_indexed.append((i, u))
    if not passthrough_pairs and not vision_indexed:
        return []
    all_candidates: list[tuple[int, str]] = passthrough_pairs + vision_indexed

    def _fallback_urls_if_all_dropped(
        rows_in: list[tuple[int, str, bool, str, float]],
    ) -> list[str]:
        out = _sort_gallery_tuples(rows_in)
        if out or not _env_gallery_vision_fallback_on_empty():
            return out
        by_url: dict[str, tuple[int, str, bool, str, float]] = {t[1]: t for t in rows_in}
        restore: list[tuple[int, str]] = []
        for i, u in all_candidates:
            t = by_url.get(u)
            if not t:
                continue
            cat = str(t[3] or "").strip().lower()
            if cat in _GALLERY_NO_FALLBACK_CATEGORIES:
                continue
            restore.append((i, u))
        if not restore:
            return []
        logger.warning(
            "Gallery vision: 0 of %d image(s) kept after scoring; using lot-heuristic URL fallback for "
            "%d loadable URL(s) (set SCANNER_GALLERY_VISION_FALLBACK_ON_EMPTY=0 to allow empty).",
            len(all_candidates),
            len(restore),
        )
        return [u for _, u in sorted(restore, key=lambda t: (-dealer_lot_photo_score(t[1]), t[0]))]

    p_rows: list[tuple[int, str, bool, str, float]] = [
        (i, u, True, "listing_cdn_passthrough", 0.78) for i, u in passthrough_pairs
    ]
    mw = max(1, int(max_workers))
    if mw <= 1:
        v_rows = [_classify_gallery_indexed(pair, page_referer=page_referer) for pair in vision_indexed]
        return _fallback_urls_if_all_dropped(p_rows + v_rows)

    v_rows: list[tuple[int, str, bool, str, float]] = []
    with ThreadPoolExecutor(max_workers=mw, thread_name_prefix="LlavaGallery") as pool:

        def _run(p: tuple[int, str]) -> tuple[int, str, bool, str, float]:
            return _classify_gallery_indexed(p, page_referer=page_referer)

        futs = {pool.submit(_run, p): p for p in vision_indexed}
        for fut in as_completed(futs):
            v_rows.append(fut.result())
    return _fallback_urls_if_all_dropped(p_rows + v_rows)


_MONRONEY_STICKER_SYSTEM = (
    "You read a vehicle window sticker (Monroney) photo. Reply with STRICT JSON only, no markdown, "
    "no prose outside JSON.\n"
    "Keys: optional_packages (string[] of optional equipment lines you can read), "
    "standard_equipment_summary (string[] short bullets of major standard equipment if legible), "
    "engine_description (string|null), transmission (string|null), drivetrain (string|null), "
    "fuel_type (string|null), cylinders (integer|null), mpg_city (integer|null), mpg_highway (integer|null), "
    "msrp (number|null), total_vehicle_price (number|null), vin_visible (string|null), "
    "confidence (0.0-1.0 number). Use null or [] when not visible. Do not invent unreadable lines."
)

_MONRONEY_TEXT_SYSTEM = (
    "You read text copied from a dealership vehicle listing page; it may include Monroney / window "
    "sticker lines. Reply with STRICT JSON only, same key schema as for a sticker photo: "
    "optional_packages, standard_equipment_summary, engine_description, transmission, drivetrain, "
    "fuel_type, cylinders, mpg_city, mpg_highway, msrp, total_vehicle_price, vin_visible, confidence. "
    "Use null or [] when unknown."
)


def _ollama_chat_text_only(*, system: str, user_text: str) -> str | None:
    payload = {
        "model": OLLAMA_VISION_MODEL,
        "stream": False,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user_text},
        ],
    }
    url = f"{OLLAMA_HOST}/api/chat"
    try:
        resp = requests.post(url, json=payload, timeout=float(OLLAMA_INTERIOR_VISION_TIMEOUT_S))
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.warning("Ollama text chat failed: %s", e)
        return None
    msg = (data.get("message") or {}) if isinstance(data, dict) else {}
    content = msg.get("content") if isinstance(msg, dict) else None
    return content if isinstance(content, str) else None


def is_probable_sticker_image_url(url: str) -> bool:
    """Heuristic URL match for Monroney / window-sticker assets (CDK and similar CDNs)."""
    ul = (url or "").lower()
    if not ul.startswith("http"):
        return False
    needles = (
        "sticker",
        "monroney",
        "monrone",
        "windowsticker",
        "window-sticker",
        "window_sticker",
        "label-g",
        "options.jpg",
        "equipment-label",
        "equiplabel",
        "monroneylabel",
    )
    return any(n in ul for n in needles)


def analyze_monroney_sticker_from_image_url(image_url: str) -> dict[str, Any] | None:
    b64 = _fetch_image_b64_optimized(image_url)
    if not b64:
        return None
    return analyze_monroney_sticker_from_image_b64(b64)


def analyze_monroney_sticker_from_image_b64(image_b64_jpeg: str) -> dict[str, Any] | None:
    content = _ollama_vision_chat_json(
        system=_MONRONEY_STICKER_SYSTEM,
        user_text="Read this window sticker / Monroney image.",
        image_b64_jpeg=image_b64_jpeg,
    )
    if not content:
        return None
    parsed = _extract_json_object(content)
    if not isinstance(parsed, dict):
        return None
    parsed["vision_model"] = OLLAMA_VISION_MODEL
    parsed["source"] = "sticker_image"
    return parsed


def analyze_monroney_from_page_texts(text_blocks: list[str]) -> dict[str, Any] | None:
    """Parse combined listing-page sticker text (no image) via the same Ollama model."""
    parts = [t.strip() for t in text_blocks if isinstance(t, str) and t.strip()]
    if not parts:
        return None
    blob = "\n---\n".join(parts)[:8000]
    user = "Listing page excerpt(s) follow:\n" + blob
    content = _ollama_chat_text_only(system=_MONRONEY_TEXT_SYSTEM, user_text=user)
    if not content:
        return None
    parsed = _extract_json_object(content)
    if not isinstance(parsed, dict):
        return None
    parsed["vision_model"] = OLLAMA_VISION_MODEL
    parsed["source"] = "page_text"
    return parsed


def image_bytes_to_b64_jpeg(image_bytes: bytes) -> str | None:
    """Encode raw image bytes as base64 JPEG for Ollama (no resize — caller may resize)."""
    if not image_bytes:
        return None
    try:
        return base64.b64encode(image_bytes).decode("ascii")
    except Exception:
        return None
