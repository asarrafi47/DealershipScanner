"""
VDP (vehicle detail page) enrichment for the Playwright scanner (scanner.py).

This module does not start Playwright; the ``page`` passed in is the same as the main scanner
browser, which (when ``playwright_stealth`` is installed) is created via
``Stealth().use_async(async_playwright())`` in ``scanner.py``.

Runs during the main scan when SCANNER_VDP_EP_MAX > 0: visits up to N unique VDP URLs
per dealer, extracts analytics ep.*, network JSON, JSON-LD, inline JSON, and DOM heuristics,
then merges into vehicle rows via merge_analytics_ep_into_vehicle (conservative fallback).
Gallery URLs from network JSON and in-page extraction are merged separately (see
backend.utils.gallery_merge.merge_vdp_gallery_into_vehicle) after EP merge.

Gallery interaction (after load + ``PAGE_EXTRACT_JS`` settle): ``GALLERY_COLLECT_URLS_JS`` runs
on the main document and in each frame (SpinCar/Impel iframes, etc.; cross-origin frames are
skipped) in a loop while advancing the carousel (ArrowRight → next/chevron locators → thumbnails)
until no new unique HTTPS URLs appear for ``SCANNER_VDP_GALLERY_IDLE_ROUNDS`` rounds or
``SCANNER_VDP_GALLERY_MAX_ROUNDS``. A short randomized pointer move runs before the gallery loop
to nudge React/lazy clients. Network image URLs use ``Content-Type`` (e.g. ``image/webp``) not only
URL extensions; JSON bodies are parsed when ``Content-Type`` is JSON-like or ``text/plain`` (e.g. GraphQL).
Image ``response`` URLs merge with DOM harvest.

Local VDP image download (default **on**): gallery bytes are saved under
``SCANNER_VDP_IMAGE_DOWNLOAD_DIR`` (default ``vdp_images`` in the process cwd), keyed by VIN with
optional ``SCANNER_VDP_IMAGE_DOWNLOAD_KEY=vin|stock`` (default ``vin``). A ``manifest.json`` is
written per vehicle folder; a compact summary is merged into ``spec_source_json`` (``vdp_gallery_local``).
Set ``SCANNER_VDP_DOWNLOAD_IMAGES=0`` to disable. Reuses ``SCANNER_VDP_NAV_TIMEOUT_MS``,
``SCANNER_VDP_SETTLE_MS``, ``SCANNER_MAX_VDP_CONCURRENCY``.

VDP price hints (JSON-LD ``offers``, ``dataLayer`` keys like ``internetPrice`` / ``salePrice``, light
DOM) merge into ``price`` only when the listing has no positive price; provenance is stored under
``spec_source_json`` key ``vdp_price`` when applied (see ``backend.database.upsert_vehicles``).
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import random
import re
from collections import Counter
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from backend.parsers.base import harvest_image_urls_from_json, inventory_gallery_max
from backend.utils.analytics_ep import (
    log_exterior_downgrade_skip,
    merge_analytics_ep_into_vehicle,
    normalize_ep_field_aliases,
)
from backend.utils.gallery_merge import (
    gallery_https_bin_histogram,
    merge_vdp_gallery_into_vehicle,
)
from backend.utils.spec_provenance import merge_spec_source_json
from backend.utils.vdp_gallery_urls import merge_https_url_batches
from backend.utils.vdp_price_merge import (
    merge_vdp_price_into_vehicle,
    pick_vdp_price_from_hints,
)

log = logging.getLogger("scanner.vdp")

MAX_JSON_BYTES = 2 * 1024 * 1024

_GENERIC_VHR_VIN_ONLY = re.compile(
    r"^https?://vhr\.carfax\.com/main\?vin=[0-9a-z]+(&format=\w+)?$",
    re.I,
)


def _is_generic_vhr_vin_only_url(url: str) -> bool:
    u = (url or "").strip()
    return bool(u) and bool(_GENERIC_VHR_VIN_ONLY.match(u))


def _pick_best_vehicle_history_url(candidates: list[Any]) -> str | None:
    """
    Prefer dealer-provided Carfax / AutoCheck / partner URLs (absolute https) from DOM or JSON.
    """
    good: list[str] = []
    seen: set[str] = set()
    for raw in candidates:
        if not isinstance(raw, str):
            continue
        s = raw.strip()
        if not s.lower().startswith("http"):
            continue
        if "javascript:" in s.lower():
            continue
        low = s.lower()
        if "carfax" not in low and "autocheck" not in low:
            continue
        if s in seen:
            continue
        seen.add(s)
        good.append(s[:900])
    if not good:
        return None

    def score(u: str) -> tuple[int, int]:
        low = u.lower()
        sc = 0
        if "partner" in low or "dealer" in low or "token" in low or "pid=" in low or "otp=" in low:
            sc += 6
        if "vhr.carfax.com" in low and not _is_generic_vhr_vin_only_url(u):
            sc += 4
        if "report" in low or "vehiclehistory" in low or "displayhistory" in low:
            sc += 2
        if _is_generic_vhr_vin_only_url(u):
            sc -= 3
        return (sc, len(u))

    good.sort(key=lambda u: score(u), reverse=True)
    return good[0]


def _merge_vdp_vehicle_history_url(vehicle: dict[str, Any], dom_urls: list[Any]) -> bool:
    """Set ``carfax_url`` from *dom_urls* when it improves on the listing JSON link. Returns True if updated."""
    picked = _pick_best_vehicle_history_url(dom_urls)
    if not picked:
        return False
    cur = str(vehicle.get("carfax_url") or "").strip()
    if not cur.lower().startswith("http"):
        vehicle["carfax_url"] = picked
        return True
    if _is_generic_vhr_vin_only_url(cur) and not _is_generic_vhr_vin_only_url(picked):
        vehicle["carfax_url"] = picked
        return True
    if len(picked) > len(cur) + 12 and ("partner" in picked.lower() or "token" in picked.lower()):
        vehicle["carfax_url"] = picked
        return True
    return False


def _detach_response_handler(page: Any, handler: Any) -> None:
    """Playwright Python builds differ; detach without assuming a specific API name."""
    for meth_name in ("off", "remove_listener", "removeListener"):
        meth = getattr(page, meth_name, None)
        if callable(meth):
            try:
                meth("response", handler)
                return
            except Exception:
                continue


MAX_NETWORK_ROWS = 45

VEHICLE_SIGNAL_KEYS = frozenset(
    {
        "vin",
        "vinnumber",
        "transmission",
        "transmissiontype",
        "drivetrain",
        "drive_train",
        "drivetype",
        "engine",
        "engine_description",
        "interior_color",
        "exterior_color",
        "fuel_type",
        "fueltype",
        "mpg",
        "city_fuel_economy",
        "highway_fuel_economy",
        "options",
        "features",
        "vehicleid",
        "vehicle_id",
        "chromestyleid",
        "stock_id",
        "stocknumber",
        "mf_year",
        "vehicle_make",
        "vehicle_model",
        "body_style",
        "inventory_type",
        "certified",
        "trim",
        "make",
        "model",
        "year",
        "driveline",
        "enginedescription",
        "cityfuelefficiency",
        "highwayfuelefficiency",
        "exteriorcolor",
        "vehicletransmission",
    }
)

PRIORITY = {
    "dataLayer": 100,
    "dataLayer_flat": 99,
    "inline_ep": 97,
    "network_ep": 85,
    "network_vehicle_json": 55,
    "ld_json": 42,
    "inline_json": 28,
    "dom": 18,
}


def _vdp_field_gap_score(vehicle: dict[str, Any]) -> int:
    """Prefer VDP visits for rows missing many dealer fields (CPO/EV listing gaps)."""
    keys = (
        "transmission",
        "drivetrain",
        "body_style",
        "condition",
        "interior_color",
        "engine_description",
    )
    n = 0
    for k in keys:
        val = vehicle.get(k)
        if val is None or (isinstance(val, str) and not str(val).strip()):
            n += 1
    return n


def _vdp_max_per_dealer() -> int:
    raw = (os.environ.get("SCANNER_VDP_EP_MAX") or "10").strip()
    try:
        return max(0, int(raw))
    except ValueError:
        return 10


def _nav_timeout_ms() -> int:
    raw = (os.environ.get("SCANNER_VDP_NAV_TIMEOUT_MS") or "32000").strip()
    try:
        return max(5000, int(raw))
    except ValueError:
        return 32000


def _settle_ms() -> int:
    raw = (os.environ.get("SCANNER_VDP_SETTLE_MS") or "2200").strip()
    try:
        return max(200, int(raw))
    except ValueError:
        return 2200


def _max_vdp_concurrency() -> int:
    raw = (os.environ.get("SCANNER_MAX_VDP_CONCURRENCY") or "2").strip()
    try:
        return max(1, int(raw))
    except ValueError:
        return 2


def _vdp_gallery_min_https() -> int:
    try:
        return max(1, int((os.environ.get("SCANNER_VDP_GALLERY_MIN_HTTPS") or "3").strip()))
    except ValueError:
        return 3


def _vdp_gallery_priority_enabled() -> bool:
    return (os.environ.get("SCANNER_VDP_GALLERY_PRIORITY") or "1").strip().lower() not in (
        "0",
        "false",
        "no",
        "off",
    )


def _gallery_max_rounds() -> int:
    try:
        return max(4, min(120, int((os.environ.get("SCANNER_VDP_GALLERY_MAX_ROUNDS") or "80").strip())))
    except ValueError:
        return 80


def _gallery_idle_rounds() -> int:
    try:
        return max(1, min(20, int((os.environ.get("SCANNER_VDP_GALLERY_IDLE_ROUNDS") or "3").strip())))
    except ValueError:
        return 3


def _vdp_download_images_enabled() -> bool:
    """Default: download VDP gallery images to disk; set ``SCANNER_VDP_DOWNLOAD_IMAGES=0`` to skip."""
    raw = (os.environ.get("SCANNER_VDP_DOWNLOAD_IMAGES") or "1").strip().lower()
    return raw not in ("0", "false", "no", "off", "")


def _vdp_image_download_dir() -> Path:
    raw = (os.environ.get("SCANNER_VDP_IMAGE_DOWNLOAD_DIR") or "vdp_images").strip()
    return Path(raw).expanduser()


def _response_maybe_gallery_image_url(url: str, content_type: str) -> bool:
    """
    True when a response is likely a vehicle-gallery image. Prefer ``Content-Type`` (many CDNs
    serve ``?fmt=webp`` and similar with no file extension in the path).
    """
    u = (url or "").strip()
    if not u.lower().startswith("https://"):
        return False
    ct = (content_type or "").lower().split(";")[0].strip()
    if ct in (
        "image/jpeg",
        "image/jpg",
        "image/pjpeg",
        "image/png",
        "image/webp",
        "image/avif",
        "image/gif",
    ):
        return True
    if ct.startswith("image/") and "svg" not in ct and "x-icon" not in ct and "vnd" not in ct:
        return True
    low = u.lower()
    if re.search(r"\.(jpe?g|png|webp|gif|avif)(\?|#|$)", low):
        return True
    for frag in (
        "/image/",
        "/images/",
        "/photos/",
        "/media/",
        "/inventory/",
        "cloudinary",
        "dealerinspire",
        "dealer.com",
        "carsforsale",
        "inventoryphoto",
        "vehiclephoto",
    ):
        if frag in low:
            return True
    return False


def _vdp_wants_json_network_capture(content_type: str) -> bool:
    """True when a response body may be JSON (including GraphQL with ``text/plain``)."""
    c = (content_type or "").strip().lower()
    if not c:
        return False
    if c.startswith("image/") or c.startswith("video/") or c.startswith("audio/"):
        return False
    if c.startswith("text/css"):
        return False
    if c.startswith("text/html") and "json" not in c:
        return False
    if c.startswith("text/javascript") or "text/javascript" in c:
        return False
    if c == "application/javascript" or c.startswith("application/x-javascript"):
        return False
    if c.startswith("text/plain"):
        return True
    if "json" in c or "+json" in c:
        return True
    return False


def _response_origin(url: str) -> str:
    try:
        p = urlparse(url or "")
        if p.scheme and p.netloc:
            return f"{p.scheme}://{p.netloc}/"
    except (ValueError, TypeError):
        pass
    return "https:///"


def _count_https_gallery_urls(vehicle: dict[str, Any]) -> int:
    seen: set[str] = set()
    n = 0
    g = vehicle.get("gallery")
    if isinstance(g, list):
        for u in g:
            if isinstance(u, str) and u.strip().lower().startswith("https://") and u not in seen:
                seen.add(u)
                n += 1
    iu = vehicle.get("image_url")
    if isinstance(iu, str) and iu.strip().lower().startswith("https://") and iu not in seen:
        n += 1
    return n


def _vdp_gallery_thin_boost(vehicle: dict[str, Any]) -> int:
    """Higher score → higher priority for limited VDP budget when gallery is thin."""
    if not _vdp_gallery_priority_enabled():
        return 0
    have = _count_https_gallery_urls(vehicle)
    need = _vdp_gallery_min_https()
    if have >= need:
        return 0
    return (need - have) * 5


def _vdp_visit_priority_tuple(vehicle: dict[str, Any]) -> tuple[int, int]:
    """Sort key: gallery-thin boost first, then EP field-gap score."""
    return (_vdp_gallery_thin_boost(vehicle) + _vdp_field_gap_score(vehicle), _vdp_field_gap_score(vehicle))


def _vdp_rotation_enabled() -> bool:
    return (os.environ.get("SCANNER_VDP_ROTATION") or "1").strip().lower() not in (
        "0",
        "false",
        "no",
        "off",
    )


def _vdp_rotation_seed(dealer_id: str) -> str:
    explicit = (os.environ.get("SCANNER_VDP_ROTATION_SEED") or "").strip()
    if explicit:
        return explicit
    from datetime import datetime, timezone

    day = datetime.now(timezone.utc).date().isoformat()
    return f"{day}|{(dealer_id or '').strip()}"


def _vdp_rotation_tie_hash(vehicle: dict[str, Any], seed: str) -> int:
    vin = (vehicle.get("vin") or "").strip().upper()
    digest = hashlib.blake2b(f"{seed}\0{vin}".encode(), digest_size=6, usedforsecurity=False).digest()
    return int.from_bytes(digest, "big")


def _vdp_queue_sort_key(vehicle: dict[str, Any], seed: str, *, rotation: bool) -> tuple[Any, ...]:
    """Descending priority: larger thin+gap first; tie-break by rotation hash or VIN."""
    t = _vdp_visit_priority_tuple(vehicle)
    if rotation:
        return (-t[0], -t[1], _vdp_rotation_tie_hash(vehicle, seed))
    return (-t[0], -t[1], (vehicle.get("vin") or "").strip().upper())


def _looks_like_vin17(v: str) -> bool:
    s = (v or "").strip().upper()
    return bool(re.match(r"^[A-HJ-NPR-Z0-9]{17}$", s))


def _vdp_image_download_key(vehicle: dict[str, Any]) -> str:
    mode = (os.environ.get("SCANNER_VDP_IMAGE_DOWNLOAD_KEY") or "vin").strip().lower()
    if mode == "stock":
        s = (vehicle.get("stock_number") or "").strip()
        if s:
            return re.sub(r"[^\w.\-]+", "_", s)[:80]
    vin = (vehicle.get("vin") or "").strip().upper()
    if _looks_like_vin17(vin):
        return vin
    s = (vehicle.get("stock_number") or "").strip()
    return re.sub(r"[^\w.\-]+", "_", s)[:80] if s else "unknown"


def _analyze_json_signals(obj: Any, depth: int = 0) -> tuple[float, list[str], list[dict[str, Any]]]:
    score = 0.0
    hits: list[str] = []
    eps: list[dict[str, Any]] = []
    if obj is None or depth > 18:
        return score, hits, eps
    if isinstance(obj, dict):
        if isinstance(obj.get("ep"), dict):
            eps.append(obj["ep"])
            score += 25
        for k, val in obj.items():
            lk = str(k).replace(" ", "_").lower()
            if lk in VEHICLE_SIGNAL_KEYS:
                if val not in (None, "", [], {}):
                    score += 8
                    hits.append(str(k))
            if isinstance(val, (dict, list)):
                s2, h2, e2 = _analyze_json_signals(val, depth + 1)
                score += s2 * 0.35
                hits.extend(h2)
                eps.extend(e2)
    elif isinstance(obj, list):
        for x in obj:
            s2, h2, e2 = _analyze_json_signals(x, depth + 1)
            score += s2
            hits.extend(h2)
            eps.extend(e2)
    return round(score, 2), hits[:30], eps


def _pick_vehicle_like_object(root: Any, depth: int = 0) -> dict[str, Any] | None:
    if root is None or depth > 14:
        return None
    if isinstance(root, list):
        for x in root:
            p = _pick_vehicle_like_object(x, depth + 1)
            if p:
                return p
        return None
    if not isinstance(root, dict):
        return None
    v = root.get("vin") or root.get("VIN")
    if _looks_like_vin17(str(v or "")):
        return root
    for key in (
        "vehicle",
        "vehicles",
        "inventory",
        "inventoryItem",
        "inventoryItems",
        "vehicleDetail",
        "vehicleDetails",
        "listing",
        "listings",
        "data",
        "result",
        "results",
        "pageData",
        "payload",
    ):
        child = root.get(key)
        if isinstance(child, list) and child:
            p = _pick_vehicle_like_object(child[0], depth + 1)
            if p:
                return p
        elif isinstance(child, dict):
            p = _pick_vehicle_like_object(child, depth + 1)
            if p:
                return p
    keys = list(root.keys())
    lowered = {str(k).replace(" ", "_").lower() for k in keys}
    if len(lowered & VEHICLE_SIGNAL_KEYS) >= 2 and (root.get("vin") or root.get("VIN")) and len(keys) < 120:
        return root
    if len(lowered & VEHICLE_SIGNAL_KEYS) >= 3 and len(keys) < 120:
        return root
    for val in root.values():
        if isinstance(val, (dict, list)):
            p = _pick_vehicle_like_object(val, depth + 1)
            if p:
                return p
    return None


def _string_quality(val: Any) -> float:
    if val is None:
        return 0.0
    if isinstance(val, bool):
        return 5.0
    if isinstance(val, (int, float)):
        return 10.0
    s = str(val).strip()
    if not s or s.lower() in ("na", "n/a", "null"):
        return 0.0
    q = float(min(40, len(s)))
    if len(s.split()) > 1:
        q += 15
    if re.search(r"metallic|pearl|tri-?coat", s, re.I):
        q += 20
    return q


def _combine_ep_fragments(
    fragments: list[tuple[str, dict[str, Any], float]],
    expected_vin: str,
) -> dict[str, Any]:
    """Merge fragment dicts; higher priority wins per field when quality improves."""
    pv = expected_vin.strip().upper()
    merged: dict[str, Any] = {}
    prov: dict[str, str] = {}

    def pri_source(src: str) -> float:
        return float(PRIORITY.get(src.split(":")[0], 10))

    ordered = sorted(
        fragments,
        key=lambda x: (-pri_source(x[0]), -x[2], -_string_quality(next(iter(x[1].values()), ""))),
    )

    for source, ep, score in ordered:
        if not ep:
            continue
        ev = str(ep.get("vin") or ep.get("VIN") or "").strip().upper()
        if pv and ev and ev != pv:
            continue
        for k, val in ep.items():
            if val is None or val == "":
                continue
            prev = merged.get(k)
            pq = _string_quality(prev) if prev is not None else 0.0
            nq = _string_quality(val)
            if prev is None or nq > pq or (nq == pq and pri_source(source) > pri_source(prov.get(k, source))):
                merged[k] = val
                prov[k] = f"{source}({score:.0f})"
    return merged


PAGE_EXTRACT_JS = r"""
() => {
  const result = {
    dataLayerEps: [],
    dataLayerFlatVehicle: [],
    dataLayerRows: 0,
    ldJsonVehicle: [],
    inlineJsonHits: [],
    inlineEpObjects: [],
    domSpecs: {},
    domFeatures: [],
    domBadges: [],
    domGalleryUrls: [],
    jsonGalleryUrls: [],
    domVehicleHistoryUrls: [],
    domMonroneyTextSnippets: [],
    vdpPriceHints: [],
    scriptSrcSample: [],
    metaGenerator: "",
    galleryExtractDebug: { photosTabClicked: false, domImgSample: 0 },
    extractDebug: {
      dataLayerLength: 0,
      dataLayerRowTopKeys: [],
      dataLayerEvents: [],
      dataLayerEpCount: 0,
      dataLayerFlatCount: 0,
      inlineEpParseCount: 0,
      inlineKeySamples: [],
      analyticsEventKeys: []
    }
  };
  const epSeen = new Set();
  function pushEp(ep) {
    if (!ep || typeof ep !== "object" || Array.isArray(ep)) return;
    if (epSeen.has(ep)) return;
    epSeen.add(ep);
    result.dataLayerEps.push(ep);
  }
  function walkForEp(obj, depth, seen) {
    if (depth > 18 || !obj || typeof obj !== "object") return;
    if (seen.has(obj)) return;
    seen.add(obj);
    if (obj.ep && typeof obj.ep === "object" && !Array.isArray(obj.ep)) {
      pushEp(obj.ep);
    }
    for (const k of Object.keys(obj)) {
      const v = obj[k];
      if (!v || typeof v !== "object") continue;
      if (Array.isArray(v)) {
        for (const it of v) walkForEp(it, depth + 1, seen);
      } else {
        walkForEp(v, depth + 1, seen);
      }
    }
  }
  function hasKeyHint(keys, hint) {
    const h = hint.replace(/_/g, "").toLowerCase();
    for (const raw of keys) {
      const k = String(raw).replace(/_/g, "").toLowerCase();
      if (k === h) return true;
      if (h.length >= 6 && (k.indexOf(h) >= 0 || h.indexOf(k) >= 0)) return true;
    }
    return false;
  }
  function vehicleLikePrimitiveScore(o) {
    const keys = Object.keys(o);
    let sc = 0;
    const hints = [
      "transmission", "drivetrain", "drivetype", "drive_train", "driveline", "fueltype", "fuel",
      "engine", "bodystyle", "body_style", "vehiclemodel", "vehicle_model",
      "exteriorcolor", "exterior_color", "interiorcolor", "interior_color",
      "cityfueleconomy", "city_fuel_economy", "highwayfueleconomy", "highway_fuel_economy",
      "cityfuelefficiency", "highwayfuelefficiency",
      "inventorytype", "inventory_type", "certified", "trim", "stockid", "stock_id"
    ];
    for (const h of hints) {
      if (hasKeyHint(keys, h)) sc++;
    }
    for (const k of keys) {
      const lk = String(k).replace(/_/g, "").toLowerCase();
      if (lk === "vin" || lk === "vinnumber") sc += 2;
    }
    return sc;
  }
  function parseJsonFromBrace(txt, openBraceIdx) {
    let depth = 0;
    let start = -1;
    const lim = Math.min(openBraceIdx + 90000, txt.length);
    for (let i = openBraceIdx; i < lim; i++) {
      const c = txt[i];
      if (c === "{") {
        if (depth === 0) start = i;
        depth++;
      } else if (c === "}") {
        depth--;
        if (depth === 0 && start >= 0) {
          const chunk = txt.slice(start, i + 1);
          try {
            return JSON.parse(chunk);
          } catch (e) {
            return null;
          }
        }
      }
    }
    return null;
  }
  const flatSig = new Set();
  function extractVehicleFlat(obj, depth, seen, out) {
    if (depth > 16 || !obj || typeof obj !== "object") return;
    if (seen.has(obj)) return;
    seen.add(obj);
    const prims = {};
    let primCount = 0;
    for (const [k, v] of Object.entries(obj)) {
      if (v === null || typeof v === "string" || typeof v === "number" || typeof v === "boolean") {
        if (String(k).length < 120 && (typeof v !== "string" || v.length < 2000)) {
          prims[k] = v;
          primCount++;
        }
      }
    }
    const sc = vehicleLikePrimitiveScore(prims);
    const vin = prims.vin || prims.VIN || prims.vinNumber;
    const hasVin = vin && String(vin).replace(/\s/g, "").length === 17;
    const good =
      (sc >= 3 && primCount >= 3) ||
      (hasVin && sc >= 2 && primCount >= 3) ||
      (sc >= 4 && primCount >= 2);
    if (good) {
      const sig = JSON.stringify(prims);
      if (!flatSig.has(sig) && out.length < 22) {
        flatSig.add(sig);
        out.push(prims);
      }
    }
    for (const k of Object.keys(obj)) {
      const v = obj[k];
      if (!v || typeof v !== "object") continue;
      if (k === "ep") continue;
      if (Array.isArray(v)) {
        for (const it of v) extractVehicleFlat(it, depth + 1, seen, out);
      } else {
        extractVehicleFlat(v, depth + 1, seen, out);
      }
    }
  }
  try {
    if (window.dataLayer && Array.isArray(window.dataLayer)) {
      result.dataLayerRows = window.dataLayer.length;
      result.extractDebug.dataLayerLength = result.dataLayerRows;
      const rowSeen = new WeakSet();
      const flatSeen = new WeakSet();
      for (let i = 0; i < window.dataLayer.length; i++) {
        const row = window.dataLayer[i];
        walkForEp(row, 0, rowSeen);
        extractVehicleFlat(row, 0, flatSeen, result.dataLayerFlatVehicle);
        if (i < 24) {
          try {
            const ev = row && typeof row === "object" && row.event != null ? String(row.event) : "";
            if (ev) result.extractDebug.dataLayerEvents.push(ev.slice(0, 120));
            if (row && typeof row === "object") {
              const ks = Object.keys(row).slice(0, 50);
              result.extractDebug.dataLayerRowTopKeys.push(ks);
              result.extractDebug.analyticsEventKeys.push(
                (ev || "?").slice(0, 90) + " | " + ks.slice(0, 28).join(", ")
              );
            } else {
              result.extractDebug.dataLayerRowTopKeys.push([]);
            }
          } catch (e2) {}
        }
      }
    }
  } catch (e) {}
  result.extractDebug.dataLayerEpCount = result.dataLayerEps.length;
  result.extractDebug.dataLayerFlatCount = result.dataLayerFlatVehicle.length;
  const lds = document.querySelectorAll('script[type="application/ld+json"]');
  for (const s of lds) {
    try {
      const j = JSON.parse(s.textContent || "{}");
      const stack = Array.isArray(j) ? j : [j];
      for (const node of stack) {
        if (!node || typeof node !== "object") continue;
        const t = [].concat(node["@type"] || []);
        const ts = t.map((x) => String(x).toLowerCase());
        const hasVin = !!(node.vehicleIdentificationNumber || node.vin || node.VIN);
        if (
          ts.some((x) => /vehicle|car|automobile/.test(x)) ||
          (hasVin && ts.some((x) => x === "product"))
        ) {
          result.ldJsonVehicle.push(node);
        }
      }
    } catch (e) {}
  }
  const mg = document.querySelector('meta[name="generator"]');
  if (mg && mg.getAttribute("content")) result.metaGenerator = mg.getAttribute("content").slice(0, 200);
  const sscripts = document.querySelectorAll("script[src]");
  for (let i = 0; i < Math.min(sscripts.length, 35); i++) {
    const src = sscripts[i].getAttribute("src") || "";
    if (src) result.scriptSrcSample.push(src.slice(0, 220));
  }
  const inlineScripts = document.querySelectorAll("script:not([src])");
  const inlineKeySamples = new Set();
  const epLiteral = /["']ep["']\s*:\s*\{/g;
  for (const sc of inlineScripts) {
    const txt = (sc.textContent || "").slice(0, 220000);
    if (txt.length < 40) continue;
    if (!/["']ep["']\s*:|ep\.|vehicle_model|drive_train|driveLine|driveline|fuel_type|transmission|drivetrain|fueltype|bodystyle|inventory_type|cityFuelEfficiency|highwayFuelEfficiency|engineDescription/i.test(txt)) continue;
    let em;
    epLiteral.lastIndex = 0;
    while ((em = epLiteral.exec(txt)) !== null && result.inlineEpObjects.length < 14) {
      const braceIdx = txt.indexOf("{", em.index);
      if (braceIdx < 0) continue;
      const parsedEp = parseJsonFromBrace(txt, braceIdx);
      if (parsedEp && typeof parsedEp === "object" && !Array.isArray(parsedEp)) {
        result.inlineEpObjects.push(parsedEp);
        result.extractDebug.inlineEpParseCount++;
        Object.keys(parsedEp).slice(0, 55).forEach((k) => inlineKeySamples.add(k));
      }
    }
  }
  result.extractDebug.inlineKeySamples = Array.from(inlineKeySamples).slice(0, 70);
  for (const sc of inlineScripts) {
    const txt = (sc.textContent || "").slice(0, 120000);
    if (txt.length < 80) continue;
    if (!/vin|vehicle|inventory|drivetrain|driveline|driveLine|transmission|["']ep["']/i.test(txt)) continue;
    let parsed = null;
    try {
      const m = txt.match(/\\{\\s*"vin"\\s*:\\s*"[^"]+"/i);
      if (m && m.index != null) {
        let depth = 0;
        let start = -1;
        for (let i = m.index; i < Math.min(m.index + 25000, txt.length); i++) {
          const c = txt[i];
          if (c === "{") {
            if (depth === 0) start = i;
            depth++;
          } else if (c === "}") {
            depth--;
            if (depth === 0 && start >= 0) {
              const chunk = txt.slice(start, i + 1);
              try {
                parsed = JSON.parse(chunk);
                break;
              } catch (e) {
                parsed = null;
              }
            }
          }
        }
      }
    } catch (e) {}
    if (parsed && typeof parsed === "object") {
      result.inlineJsonHits.push(parsed);
      if (result.inlineJsonHits.length >= 5) break;
    }
  }
  const specSelectors = ["dl", "dl.vehicle-specs", ".vehicle-specs", ".specifications", "table.specs", ".vdp-specs"];
  for (const sel of specSelectors) {
    try {
      const el = document.querySelector(sel);
      if (!el) continue;
      const rows = el.querySelectorAll("tr, dt");
      rows.forEach((row) => {
        const label = (row.querySelector("th, dt, .label, .name") || row.cells?.[0])?.textContent?.trim();
        const val = (row.querySelector("td, dd, .value") || row.cells?.[1])?.textContent?.trim();
        if (label && val && label.length < 80 && val.length < 400) {
          const lk = label.toLowerCase();
          if (/trans|drive|exterior|interior|engine|fuel|mpg|vin|stock|body/i.test(lk)) {
            result.domSpecs[label.slice(0, 60)] = val.slice(0, 300);
          }
        }
      });
    } catch (e) {}
  }
  document.querySelectorAll("[class*='feature'], .features li, ul.features li").forEach((el, idx) => {
    if (idx > 60) return;
    const t = (el.textContent || "").trim();
    if (t && t.length < 200) result.domFeatures.push(t);
  });
  document.querySelectorAll(".badge, [class*='badge'], .label-pill").forEach((el, idx) => {
    if (idx > 25) return;
    const t = (el.textContent || "").trim();
    if (t && t.length < 120) result.domBadges.push(t);
  });
  try {
    const tabCands = Array.from(
      document.querySelectorAll("a, button, [role='tab'], [data-tab], [data-toggle]")
    ).filter((el) => {
      const t = (el.textContent || "").trim().toLowerCase();
      if (!t || t.length > 28) return false;
      return (
        t === "photos" ||
        t === "pictures" ||
        t === "images" ||
        t === "gallery" ||
        /^photo(s)?$/i.test(t)
      );
    });
    const visible = tabCands.filter((el) => {
      try {
        const st = window.getComputedStyle(el);
        return st.display !== "none" && st.visibility !== "hidden" && el.offsetParent !== null;
      } catch (e) {
        return false;
      }
    });
    if (visible.length === 1) {
      visible[0].click();
      result.galleryExtractDebug.photosTabClicked = true;
    }
  } catch (e) {}
  try {
    window.scrollTo(0, Math.min(3200, (document.body && document.body.scrollHeight) || 0));
  } catch (e) {}
  const imgSeen = new Set();
  const imgSelectors = [
    ".vehicle-image-gallery img",
    ".gallery img",
    "[class*='photo-gallery'] img",
    "[class*='vehicle-photo'] img",
    "[class*='image-gallery'] img",
    "img[src*='.jpg']",
    "img[src*='.jpeg']",
    "img[src*='.png']",
    "img[src*='.webp']",
  ];
  for (const sel of imgSelectors) {
    try {
      document.querySelectorAll(sel).forEach((el, idx) => {
        if (idx > 90 || result.domGalleryUrls.length >= 48) return;
        const s =
          el.getAttribute("src") ||
          el.getAttribute("data-src") ||
          el.getAttribute("data-lazy-src") ||
          el.getAttribute("data-original") ||
          "";
        const t = (s || "").trim();
        if (!/^https?:\/\//i.test(t)) return;
        if (!/\.(jpe?g|png|webp|gif)(\?|$)/i.test(t)) return;
        if (imgSeen.has(t)) return;
        imgSeen.add(t);
        result.domGalleryUrls.push(t.slice(0, 900));
      });
    } catch (e) {}
    if (result.domGalleryUrls.length >= 36) break;
  }
  result.galleryExtractDebug.domImgSample = result.domGalleryUrls.length;
  const jSeen = new Set();
  function pushJsonImg(s) {
    if (!s || typeof s !== "string") return;
    const t = s.trim();
    if (!/^https?:\/\//i.test(t)) return;
    if (!/\.(jpe?g|png|webp|gif)(\?|$)/i.test(t)) return;
    if (jSeen.size >= 55) return;
    if (jSeen.has(t)) return;
    jSeen.add(t);
    result.jsonGalleryUrls.push(t.slice(0, 900));
  }
  function walkJsonImg(o, d) {
    if (d > 14 || !o || typeof o !== "object") return;
    if (Array.isArray(o)) {
      for (const x of o) walkJsonImg(x, d + 1);
      return;
    }
    for (const [k, v] of Object.entries(o)) {
      const lk = String(k).toLowerCase().replace(/_/g, "");
      if (
        /photo|image|media|gallery|spin|thumb|picture|carousel|viewer|asset/i.test(lk) &&
        (typeof v === "string" || Array.isArray(v) || (v && typeof v === "object"))
      ) {
        if (typeof v === "string") pushJsonImg(v);
        else if (Array.isArray(v)) {
          for (const it of v) {
            if (typeof it === "string") pushJsonImg(it);
            else if (it && typeof it === "object") {
              const u =
                it.url || it.URL || it.uri || it.src || it.href || it.large || it.full || it.xlarge;
              if (typeof u === "string") pushJsonImg(u);
            }
          }
        } else if (v && typeof v === "object") {
          const u = v.url || v.URL || v.uri || v.src;
          if (typeof u === "string") pushJsonImg(u);
        }
      }
      if (v && typeof v === "object") walkJsonImg(v, d + 1);
    }
  }
  try {
    if (window.dataLayer && Array.isArray(window.dataLayer)) {
      for (let i = 0; i < Math.min(14, window.dataLayer.length); i++) {
        walkJsonImg(window.dataLayer[i], 0);
      }
    }
  } catch (e) {}
  for (const node of result.inlineJsonHits || []) {
    walkJsonImg(node, 0);
  }
  for (const node of result.ldJsonVehicle || []) {
    walkJsonImg(node, 0);
  }
  function pushPriceHint(raw, source) {
    if (raw === null || raw === undefined) return;
    let num = null;
    if (typeof raw === "number" && isFinite(raw)) {
      num = raw;
    } else if (typeof raw === "string") {
      const t = raw.replace(/[$,]/g, "").trim();
      if (!t || /call|contact|request|quote/i.test(t)) return;
      const m = t.match(/(\\d{3,7})(?:\\.\\d{2})?/);
      if (m) num = parseFloat(m[1]);
    }
    if (num === null || !isFinite(num) || num < 500 || num > 2500000) return;
    result.vdpPriceHints.push({ value: num, raw: String(raw).slice(0, 60), source: String(source || "?") });
  }
  function walkDataLayerPrice(obj, depth, seen) {
    if (depth > 14 || !obj || typeof obj !== "object" || seen.has(obj)) return;
    seen.add(obj);
    const keys = [
      "internetPrice",
      "InternetPrice",
      "salePrice",
      "SalePrice",
      "price",
      "Price",
      "vehiclePrice",
      "askingPrice",
      "listPrice",
      "retailPrice",
      "msrp",
      "MSRP",
    ];
    for (const k of keys) {
      if (Object.prototype.hasOwnProperty.call(obj, k)) pushPriceHint(obj[k], "dataLayer:" + k);
    }
    for (const v of Object.values(obj)) {
      if (v && typeof v === "object") walkDataLayerPrice(v, depth + 1, seen);
    }
  }
  try {
    if (window.dataLayer && Array.isArray(window.dataLayer)) {
      const seen = new WeakSet();
      for (let i = 0; i < Math.min(40, window.dataLayer.length); i++) {
        walkDataLayerPrice(window.dataLayer[i], 0, seen);
      }
    }
  } catch (e3) {}
  function offersFromLd(node) {
    const out = [];
    if (!node || typeof node !== "object") return out;
    const o = node.offers || node.offer;
    if (!o) return out;
    return [].concat(o);
  }
  for (const node of result.ldJsonVehicle || []) {
    for (const off of offersFromLd(node)) {
      if (!off || typeof off !== "object") continue;
      const p = off.price || off.Price || (off.priceSpecification && off.priceSpecification.price);
      pushPriceHint(p, "json_ld_offer");
    }
    const p2 = node.price || node.Price;
    if (p2) pushPriceHint(p2, "json_ld_product");
  }
  try {
    document.querySelectorAll('[itemprop="price"],[itemprop=price]').forEach((el, idx) => {
      if (idx > 12) return;
      const c = el.getAttribute("content");
      if (c) pushPriceHint(c, "dom_itemprop");
      else pushPriceHint((el.textContent || "").trim(), "dom_itemprop");
    });
  } catch (e4) {}
  const priceSelectors = [
    ".vehicle-price",
    ".internetPrice",
    ".sale-price",
    ".price-value",
    ".pricing-price",
    "[class*='vehicle-price']",
    "[data-vehicle-price]",
    "[data-price]",
  ];
  for (const sel of priceSelectors) {
    try {
      const el = document.querySelector(sel);
      if (!el) continue;
      const t = (el.textContent || "").trim();
      if (t && t.length < 80) pushPriceHint(t, "dom_dealer:" + sel.slice(0, 40));
    } catch (e5) {}
  }
  result.domVehicleHistoryUrls = [];
  result.domMonroneyTextSnippets = [];
  function absUrl(href) {
    try {
      if (!href || typeof href !== "string") return "";
      const t = href.trim();
      if (!t || t.toLowerCase().indexOf("javascript:") === 0) return "";
      const u = new URL(t, document.baseURI);
      return u.href;
    } catch (eAbs) {
      return "";
    }
  }
  try {
    document
      .querySelectorAll(
        'a[href*="carfax"], a[href*="CARFAX"], a[href*="vhr.carfax"], a[href*="autocheck"], a[href*="AutoCheck"], area[href]'
      )
      .forEach((a, idx) => {
        if (idx > 70 || result.domVehicleHistoryUrls.length >= 16) return;
        const h = absUrl(a.getAttribute("href") || "");
        if (!/^https?:\\/\\//i.test(h)) return;
        const low = h.toLowerCase();
        if (low.indexOf("carfax") < 0 && low.indexOf("autocheck") < 0) return;
        if (result.domVehicleHistoryUrls.includes(h)) return;
        result.domVehicleHistoryUrls.push(h.slice(0, 900));
      });
  } catch (eVhr) {}
  try {
    document.querySelectorAll("[data-carfax-url], [data-carfax-href], [data-vhr-url]").forEach((el, idx) => {
      if (idx > 30 || result.domVehicleHistoryUrls.length >= 18) return;
      const raw =
        el.getAttribute("data-carfax-url") ||
        el.getAttribute("data-carfax-href") ||
        el.getAttribute("data-vhr-url") ||
        "";
      const h = absUrl(raw);
      if (!/^https?:\\/\\//i.test(h)) return;
      if (result.domVehicleHistoryUrls.includes(h)) return;
      result.domVehicleHistoryUrls.push(h.slice(0, 900));
    });
  } catch (eDa) {}
  let monoBudget = 0;
  const monoSelectors = [
    "[class*='monroney']",
    "[class*='Monroney']",
    "[class*='window-sticker']",
    "[class*='windowSticker']",
    "[class*='WindowSticker']",
    "[id*='monroney']",
    "[id*='Monroney']",
    "[data-widget*='sticker']",
  ];
  for (const sel of monoSelectors) {
    try {
      document.querySelectorAll(sel).forEach((el) => {
        if (monoBudget >= 5 || result.domMonroneyTextSnippets.length >= 5) return;
        const t = (el.textContent || "").trim().replace(/\\s+/g, " ");
        if (t.length < 50 || t.length > 3200) return;
        if (!/engine|trans|equip|option|msrp|vin|standard|included|warranty|drivetrain|fuel/i.test(t)) return;
        result.domMonroneyTextSnippets.push(t.slice(0, 2400));
        monoBudget++;
      });
    } catch (eM) {}
  }
  return result;
}
"""


GALLERY_COLLECT_URLS_JS = r"""
() => {
  const out = [];
  const seen = new Set();
  const bgRe = /url\\(\\s*['"]?([^'")\\s>]+)['"]?\\s*\\)/gi;
  const cdnQParam = /[?&](fmt|format|f_auto|w_auto|fit|q|w|h)=/i;
  function mightBeRasterUrl(low) {
    if (/\\.(jpe?g|png|webp|gif|avif)(\\?|#|$)/i.test(low)) return true;
    if (cdnQParam.test(low) && /(image|photo|media|cdn|dealer|inventory|vehicle|res\\.cloudinary|imgix|akamai|spin|impel|cfassets|photobucket)/i.test(low))
      return true;
    if (/(\\/image\\/|\\/images\\/|\\/photos\\/|\\/media\\/|cloudinary|dealerinspire|dealer\\.com|inventoryphoto|resizable)/i.test(low)) return true;
    return false;
  }
  function push(u) {
    if (!u || typeof u !== "string") return;
    let t = u.trim();
    if (t.startsWith("//")) t = "https:" + t;
    if (t.startsWith("http://")) t = "https://" + t.slice(7);
    if (!/^https:\\/\\//i.test(t)) return;
    const low = t.toLowerCase();
    if (!mightBeRasterUrl(low)) return;
    if (seen.has(t)) return;
    seen.add(t);
    if (out.length < 220) out.push(t.slice(0, 900));
  }
  function fromSrcset(ss) {
    if (!ss || typeof ss !== "string") return;
    for (const part of ss.split(",")) {
      const p = part.trim().split(/\\s+/)[0];
      if (p) push(p);
    }
  }
  function fromBackgroundString(bg) {
    if (!bg || typeof bg !== "string") return;
    const s = bg.trim();
    if (!s || /^none$|^initial$|^inherit$/i.test(s)) return;
    let m;
    const r = new RegExp(bgRe.source, "gi");
    while ((m = r.exec(s)) !== null) {
      if (m[1]) push(m[1].replace(/^["']|["']$/g, ""));
    }
  }
  try {
    document.querySelectorAll("img").forEach((img, idx) => {
      if (idx > 220) return;
      try {
        if (img.currentSrc) push(img.currentSrc);
      } catch (e0) {}
      push(img.getAttribute("src"));
      fromSrcset(img.getAttribute("srcset"));
      const lazy = [
        "data-src",
        "data-lazy-src",
        "data-original",
        "data-lazy",
        "data-image",
        "data-zoom-src",
        "data-fullsrc",
      ];
      for (const a of lazy) push(img.getAttribute(a));
    });
  } catch (e1) {}
  try {
    document.querySelectorAll("picture source[srcset], picture source[src]").forEach((src, idx) => {
      if (idx > 80) return;
      fromSrcset(src.getAttribute("srcset"));
      push(src.getAttribute("src"));
    });
  } catch (e2) {}
  try {
    const bsel =
      "div,span,section,article,li,a,button,p,figure,header,footer,main,aside," +
      "[style*='background'],[style*='Background']";
    document.querySelectorAll(bsel).forEach((el, idx) => {
      if (idx > 520) return;
      try {
        const st = el.getAttribute("style");
        if (st && /background\\s*:|background-image\\s*:/i.test(st)) {
          fromBackgroundString(st);
        }
        if (window.getComputedStyle) {
          const cbg = window.getComputedStyle(el).backgroundImage;
          if (cbg && cbg !== "none" && cbg !== "initial") {
            fromBackgroundString(cbg);
          }
        }
      } catch (eBg) {}
    });
  } catch (e3) {}
  return out;
}
"""


def _dom_specs_to_ep(dom_specs: dict[str, str]) -> dict[str, Any]:
    flat: dict[str, Any] = {}
    for label, val in dom_specs.items():
        lk = label.lower()
        if re.search(r"vin", lk):
            flat["vin"] = val
        elif re.search(r"trans", lk):
            flat["transmission"] = val
        elif re.search(r"drive|drivetrain|driveline|wheel\s*drive", lk):
            flat["drive_train"] = val
        elif re.search(r"exterior|ext\.?\s*color", lk):
            flat["exterior_color"] = val
        elif re.search(r"interior|int\.?\s*color", lk):
            flat["interior_color"] = val
        elif re.search(r"engine", lk):
            flat["engine"] = val
        elif re.search(r"fuel", lk):
            flat["fuel_type"] = val
        elif re.search(r"mpg|fuel economy", lk):
            m = re.search(r"(\d+)\s*[/|]\s*(\d+)", val)
            if m:
                flat["city_fuel_economy"] = m.group(1)
                flat["highway_fuel_economy"] = m.group(2)
    return flat


def _ld_to_ep(node: dict[str, Any]) -> dict[str, Any]:
    flat: dict[str, Any] = {}
    if node.get("name") or node.get("model"):
        flat["vehicle_model"] = str(node.get("name") or node.get("model") or "")[:200]
    if node.get("vehicleIdentificationNumber"):
        flat["vin"] = str(node["vehicleIdentificationNumber"])
    elif node.get("vin"):
        flat["vin"] = str(node["vin"])
    if node.get("vehicleInteriorColor"):
        flat["interior_color"] = str(node["vehicleInteriorColor"])
    if node.get("color"):
        flat.setdefault("exterior_color", str(node["color"])[:120])
    if node.get("bodyType"):
        flat["body_style"] = str(node["bodyType"])
    vt = node.get("vehicleTransmission") or node.get("transmission")
    if vt:
        if isinstance(vt, dict):
            t = vt.get("name") or vt.get("value")
            if t:
                flat["transmission"] = str(t)[:120]
        else:
            flat["transmission"] = str(vt)[:120]
    dw = node.get("driveWheelConfiguration")
    if dw:
        if isinstance(dw, dict) and dw.get("name"):
            flat["drive_train"] = str(dw["name"])[:120]
        else:
            flat["drive_train"] = str(dw)[:120]
    fts = node.get("fuelType")
    if fts:
        if isinstance(fts, dict) and fts.get("name"):
            flat["fuel_type"] = str(fts["name"])[:80]
        else:
            flat["fuel_type"] = str(fts)[:80]
    eng = node.get("vehicleEngine")
    if isinstance(eng, dict):
        nm = eng.get("name") or eng.get("description")
        if nm:
            flat["engine"] = str(nm)[:500]
    elif isinstance(eng, str) and eng.strip():
        flat["engine"] = eng[:500]
    return flat


def _build_fragments_from_vdp_capture(
    network_rows: list[dict[str, Any]],
    bundle: dict[str, Any] | None,
    expected_vin: str,
) -> tuple[list[tuple[str, dict[str, Any], float]], list[str], str | None]:
    fragments: list[tuple[str, dict[str, Any], float]] = []
    extractor_hits: list[str] = []
    bundle_err = (bundle or {}).get("error") if isinstance(bundle, dict) else None

    for row in network_rows:
        sc = float(row.get("score") or 0)
        for ep in row.get("ep_objects") or []:
            if isinstance(ep, dict):
                fragments.append(("network_ep", ep, sc))
        parsed = row.get("parsed")
        if parsed and sc >= 15:
            sub = _pick_vehicle_like_object(parsed)
            if sub:
                eff_sc = float(sc) if not row.get("ep_objects") else min(float(sc), 72.0)
                fragments.append(("network_vehicle_json", sub, eff_sc))

    if network_rows:
        extractor_hits.append("network")

    dle = (bundle or {}).get("dataLayerEps") or []
    if dle:
        extractor_hits.append("analytics_ep")
        log.info("VDP: analytics_ep hit for VIN %s (%d ep fragment(s))", expected_vin[:17], len(dle))
    for ep in dle:
        if isinstance(ep, dict):
            fragments.append(("dataLayer", ep, 95.0))

    for flat in (bundle or {}).get("dataLayerFlatVehicle") or []:
        if isinstance(flat, dict):
            fragments.append(("dataLayer_flat", flat, 99.0))
    if (bundle or {}).get("dataLayerFlatVehicle"):
        extractor_hits.append("dataLayer_flat")

    for ep_inline in (bundle or {}).get("inlineEpObjects") or []:
        if isinstance(ep_inline, dict):
            fragments.append(("inline_ep", ep_inline, 97.0))
    if (bundle or {}).get("inlineEpObjects"):
        extractor_hits.append("inline_ep")

    for node in (bundle or {}).get("ldJsonVehicle") or []:
        if isinstance(node, dict):
            fe = _ld_to_ep(node)
            if fe:
                fragments.append(("ld_json", fe, 40.0))
    if (bundle or {}).get("ldJsonVehicle"):
        extractor_hits.append("ld_json")

    for hit in (bundle or {}).get("inlineJsonHits") or []:
        if isinstance(hit, dict) and not hit.get("_rawSnippet"):
            fragments.append(("inline_json", hit, 25.0))
    if (bundle or {}).get("inlineJsonHits"):
        extractor_hits.append("inline_json")

    ds = (bundle or {}).get("domSpecs") or {}
    if isinstance(ds, dict) and ds:
        dom_ep = _dom_specs_to_ep({str(k): str(v) for k, v in ds.items()})
        if dom_ep:
            fragments.append(("dom", dom_ep, 18.0))
            extractor_hits.append("dom")

    return fragments, list(dict.fromkeys(extractor_hits)), bundle_err


def _vdp_count_gallery_signals(
    network_rows: list[dict[str, Any]],
    bundle: dict[str, Any] | None,
) -> int:
    n = 0
    for row in network_rows:
        n += len(row.get("image_urls") or [])
    if isinstance(bundle, dict):
        n += len(bundle.get("domGalleryUrls") or [])
        n += len(bundle.get("jsonGalleryUrls") or [])
    return n


async def _drain_pending_tasks(pending: list[asyncio.Task[Any]]) -> None:
    if not pending:
        return
    await asyncio.gather(*pending, return_exceptions=True)
    pending.clear()


async def _vdp_gallery_step_advance(wp: Any, thumb_rot: list[int]) -> None:
    for sel in (
        ".vehicle-image-gallery",
        ".vehicle-photos",
        ".photo-gallery",
        ".gallery",
        "[class*='photo-gallery']",
        "[class*='image-gallery']",
    ):
        try:
            loc = wp.locator(sel).first
            await loc.click(timeout=500)
            await wp.keyboard.press("ArrowRight")
            await asyncio.sleep(0.05)
            return
        except Exception:
            continue
    try:
        await wp.keyboard.press("ArrowRight")
        await asyncio.sleep(0.05)
    except Exception:
        pass
    next_selectors = [
        'button[aria-label*="next" i]',
        'a[aria-label*="next" i]',
        '[class*="gallery"] button:has-text("Next")',
        ".gallery-next",
        ".swiper-button-next",
        "[class*='chevron-right'][role='button']",
    ]
    for s in next_selectors:
        try:
            loc = wp.locator(s).first
            if await loc.count() > 0:
                await loc.click(timeout=900)
                return
        except Exception:
            continue
    try:
        thumbs = wp.locator(
            ".thumbnail, .thumbnails button, [data-gallery-thumb], "
            ".swiper-slide:not(.swiper-slide-duplicate), li.swiper-slide"
        )
        n = await thumbs.count()
        if n > 1:
            idx = thumb_rot[0] % n
            thumb_rot[0] += 1
            await thumbs.nth(idx).click(timeout=1200)
    except Exception:
        pass


async def _vdp_evaluate_gallery_all_frames(wp: Any) -> list[str]:
    """
    Run ``GALLERY_COLLECT_URLS_JS`` in the main document and in each child frame. Same-origin
    gallery iframes (e.g. some 360 / embed hosts) are included; cross-origin frames raise and are
    skipped.
    """
    merged: list[str] = []
    frames = list(getattr(wp, "frames", None) or [])
    for fr in frames:
        try:
            raw = await fr.evaluate(GALLERY_COLLECT_URLS_JS)
        except Exception:
            continue
        if not isinstance(raw, list):
            continue
        for x in raw:
            if isinstance(x, str) and x.strip():
                merged.append(x)
    return merged


async def _vdp_mouse_jitter(wp: Any) -> None:
    """Small random pointer moves to nudge lazy galleries and client-side anti-bot heuristics."""
    try:
        view = await wp.evaluate(
            "() => ({ w: Math.max(0, window.innerWidth), h: Math.max(0, window.innerHeight) })"
        )
    except Exception:
        view = {"w": 0, "h": 0}
    wv = int(view.get("w") or 0)
    hv = int(view.get("h") or 0)
    if wv < 2 or hv < 2:
        wv, hv = 800, 600
    for _ in range(2):
        x = random.randint(1, max(1, wv - 1))
        y = random.randint(1, max(1, hv - 1))
        try:
            await wp.mouse.move(x, y, steps=max(1, min(8, 2 + int(random.random() * 5))))
        except Exception:
            break
        await asyncio.sleep(0.03 + random.random() * 0.05)


async def _vdp_gallery_interaction_loop(
    wp: Any,
    *,
    settle_ms: int,
    response_image_urls: list[str],
    pending: list[asyncio.Task[Any]],
) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    stall = 0
    thumb_rot = [0]
    cap = max(inventory_gallery_max(), 160)
    settle_sleep = min(1200, max(240, int(settle_ms // 4)))
    for _ in range(_gallery_max_rounds()):
        snap = list(response_image_urls)
        try:
            dom_batch = await _vdp_evaluate_gallery_all_frames(wp)
        except Exception:
            dom_batch = []
        if not isinstance(dom_batch, list):
            dom_batch = []
        n1 = merge_https_url_batches(ordered, seen, dom_batch, max_total=cap)
        n2 = merge_https_url_batches(ordered, seen, snap, max_total=cap)
        if n1 + n2 == 0:
            stall += 1
            if stall >= _gallery_idle_rounds():
                break
        else:
            stall = 0
        await _vdp_gallery_step_advance(wp, thumb_rot)
        await asyncio.sleep(settle_sleep / 1000.0)
        await _drain_pending_tasks(pending)
    return ordered


def _apply_vdp_price_hints(
    vehicle: dict[str, Any],
    bundle: dict[str, Any] | None,
    detail_url: str,
) -> dict[str, Any]:
    if not isinstance(bundle, dict):
        return {"updated": False}
    hints = [h for h in (bundle.get("vdpPriceHints") or []) if isinstance(h, dict)]
    picked, meta = pick_vdp_price_from_hints(hints)
    src = (meta or {}).get("source") or "vdp"
    diag = merge_vdp_price_into_vehicle(vehicle, picked, provenance_source=str(src), detail_url=detail_url or "")
    if diag.get("updated"):
        vehicle["spec_source_json"] = merge_spec_source_json(
            vehicle.get("spec_source_json"),
            {
                "vdp_price": {
                    "source": "vdp_scan",
                    "origin": str(src)[:120],
                    "value": diag.get("value"),
                    "url": (detail_url or "")[:500],
                }
            },
        )
    return diag


async def _download_vdp_gallery_images(wp: Any, vehicle: dict[str, Any], urls: list[str]) -> dict[str, Any] | None:
    if not urls or not _vdp_download_images_enabled():
        return None
    dest = _vdp_image_download_dir() / _vdp_image_download_key(vehicle)
    dest.mkdir(parents=True, exist_ok=True)
    manifest: dict[str, Any] = {"files": [], "errors": []}
    req = wp.context.request
    tmo = _nav_timeout_ms()
    cap = inventory_gallery_max()
    for i, url in enumerate(urls[:cap]):
        if not isinstance(url, str) or not url.lower().startswith("https://"):
            continue
        try:
            resp = await req.get(url, timeout=tmo)
            if resp.status != 200:
                manifest["errors"].append({"url": url[:220], "status": int(resp.status)})
                continue
            ct = (resp.headers.get("content-type") or "").lower()
            if "image/" not in ct and not _response_maybe_gallery_image_url(url, ct):
                manifest["errors"].append({"url": url[:220], "note": "skipped_non_image"})
                continue
            body = await resp.body()
            if not body or len(body) < 80:
                manifest["errors"].append({"url": url[:220], "note": "empty_body"})
                continue
            path = urlparse(url).path or ""
            suf = Path(path).suffix.lower()
            if suf not in (".jpg", ".jpeg", ".png", ".webp", ".gif", ".avif"):
                suf = ".jpg"
            name = f"{i:03d}_{hashlib.sha256(url.encode()).hexdigest()[:14]}{suf}"
            fp = dest / name[:160]
            fp.write_bytes(body)
            manifest["files"].append({"url": url[:900], "path": str(fp)})
        except Exception as e:
            manifest["errors"].append({"url": url[:220], "err": str(e)[:160]})
    try:
        man_path = dest / "manifest.json"
        man_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    except OSError as e:
        log.warning("VDP: manifest write failed: %s", e)
    vehicle["spec_source_json"] = merge_spec_source_json(
        vehicle.get("spec_source_json"),
        {
            "vdp_gallery_local": {
                "source": "vdp_download",
                "dir": str(dest.resolve()),
                "saved": len(manifest.get("files") or []),
                "errors": len(manifest.get("errors") or []),
            }
        },
    )
    return manifest


async def _vdp_visit_one(
    wp: Any,
    dealer_name: str,
    v: dict[str, Any],
    u: str,
    vin: str,
    preview_lock: asyncio.Lock,
    preview_budget: list[int],
) -> dict[str, Any]:
    """
    Visit one VDP URL on *wp*, merge analytics into *v*. Isolated per page (safe for parallel workers).
    """
    out: dict[str, Any] = {
        "visited": 0,
        "enriched": False,
        "filled": [],
        "skipped": [],
        "gallery_added": 0,
        "price_updated": False,
    }
    visit_epoch = [0]
    network_rows: list[dict[str, Any]] = []
    response_image_urls: list[str] = []
    pending: list[asyncio.Task[Any]] = []

    async def capture_response(response) -> None:
        my_epoch = visit_epoch[0]
        try:
            if response.status != 200:
                return
            ct = (response.headers.get("content-type") or "").lower()
            url = (response.url or "").strip()
            if _response_maybe_gallery_image_url(url, ct):
                if visit_epoch[0] != my_epoch:
                    return
                if url.lower().startswith("https://"):
                    response_image_urls.append(url[:900])
                return
            if not _vdp_wants_json_network_capture(ct):
                return
            text = await response.text()
            if visit_epoch[0] != my_epoch:
                return
            if not text or len(text) > MAX_JSON_BYTES:
                return
            img_hint = bool(
                re.search(
                    r"vehiclePhotos|vehiclephotos|\"images\"\s*:\s*\[|imageUrls|imageurls|photoList|"
                    r"spin|gallery|carousel|cdn\..*\.(jpe?g|png|webp)",
                    text,
                    re.I,
                )
            )
            if '"ep"' not in text and '"vin"' not in text.lower():
                if not re.search(
                    r"driveLine|drive_train|drivetrain|driveline|transmission|fuelType|cityFuelEfficiency",
                    text,
                    re.I,
                ):
                    if not img_hint:
                        return
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError:
                return
            score, key_hits, ep_objs = _analyze_json_signals(parsed)
            origin = _response_origin(response.url or "")
            urls_from_images = harvest_image_urls_from_json(parsed, origin, max_urls=96)
            if score < 6 and not ep_objs and len(urls_from_images) < 3:
                return
            network_rows.append(
                {
                    "url": (response.url or "")[:500],
                    "score": score,
                    "key_hits": key_hits[:20],
                    "ep_objects": ep_objs,
                    "parsed": parsed,
                    "image_urls": urls_from_images,
                }
            )
            if len(network_rows) > MAX_NETWORK_ROWS:
                network_rows.pop(0)
        except Exception:
            return

    def on_response(response: Any) -> None:
        pending.append(asyncio.create_task(capture_response(response)))

    wp.on("response", on_response)
    urls_to_try: list[str] = [u]
    alts = v.get("_detail_url_alternates")
    if isinstance(alts, list):
        for a in alts:
            if isinstance(a, str):
                au = a.strip()
                if au.startswith("http") and au not in urls_to_try:
                    urls_to_try.append(au)
            if len(urls_to_try) >= 6:
                break

    try:
        combined_ep: dict[str, Any] = {}
        success_u = u
        last_bundle: dict[str, Any] = {}
        extra_loop_gallery: list[str] = []
        for try_url in urls_to_try:
            visit_epoch[0] += 1
            network_rows.clear()
            response_image_urls.clear()
            out["visited"] = int(out.get("visited") or 0) + 1

            log.info("VDP: %s — visiting %s", dealer_name, try_url[:200])

            nav_err = None
            try:
                await wp.goto(try_url, wait_until="domcontentloaded", timeout=_nav_timeout_ms())
            except Exception as e:
                nav_err = str(e)
            await asyncio.sleep(_settle_ms() / 1000.0)
            await _drain_pending_tasks(pending)

            if nav_err:
                log.warning("VDP: %s — navigation issue: %s", dealer_name, nav_err[:120])

            try:
                bundle = await wp.evaluate(PAGE_EXTRACT_JS)
            except Exception as e:
                bundle = {"error": str(e)}
            last_bundle = bundle if isinstance(bundle, dict) else {}
            await _drain_pending_tasks(pending)
            try:
                await _vdp_mouse_jitter(wp)
            except Exception:
                pass
            try:
                extra_loop_gallery = await _vdp_gallery_interaction_loop(
                    wp,
                    settle_ms=_settle_ms(),
                    response_image_urls=response_image_urls,
                    pending=pending,
                )
            except Exception as e:
                log.warning("VDP: %s — gallery interaction loop: %s", dealer_name, str(e)[:160])
            await _drain_pending_tasks(pending)

            async with preview_lock:
                if preview_budget[0] > 0 and isinstance(bundle, dict):
                    preview_budget[0] -= 1
                    idx = 2 - preview_budget[0]
                    ed = bundle.get("extractDebug") or {}
                    log.info(
                        "VDP: %s — extract raw preview (%d/2) dataLayer_len=%s nested_ep=%s flat_vehicle=%s inline_ep_JSON=%s",
                        dealer_name,
                        idx,
                        ed.get("dataLayerLength"),
                        ed.get("dataLayerEpCount"),
                        ed.get("dataLayerFlatCount"),
                        ed.get("inlineEpParseCount"),
                    )
                    log.info(
                        "VDP: %s — dataLayer event names (sample): %s",
                        dealer_name,
                        (ed.get("dataLayerEvents") or [])[:14],
                    )
                    log.info(
                        "VDP: %s — analytics rows (event | keys): %s",
                        dealer_name,
                        (ed.get("analyticsEventKeys") or [])[:8],
                    )
                    log.info(
                        "VDP: %s — dataLayer top-level key samples (first rows): %s",
                        dealer_name,
                        (ed.get("dataLayerRowTopKeys") or [])[:4],
                    )
                    log.info(
                        "VDP: %s — inline script ep key candidates: %s",
                        dealer_name,
                        ed.get("inlineKeySamples"),
                    )

            frags, hits, _berr = _build_fragments_from_vdp_capture(network_rows, bundle, vin)
            if hits:
                log.info("VDP: %s — extractors with data: %s", dealer_name, ", ".join(hits))

            combined_try = normalize_ep_field_aliases(_combine_ep_fragments(frags, vin))
            gsig = _vdp_count_gallery_signals(network_rows, last_bundle) + len(extra_loop_gallery)
            log.info(
                "VDP: %s — combined EP keys for VIN %s: %s (gallery_signal=%d)",
                dealer_name,
                vin[:17],
                sorted(combined_try.keys()),
                gsig,
            )
            if combined_try:
                combined_ep = combined_try
                success_u = try_url
                break
            if gsig >= 4:
                combined_ep = {}
                success_u = try_url
                log.info(
                    "VDP: %s — gallery-rich capture without EP fields on %s (signal=%d)",
                    dealer_name,
                    try_url[:120],
                    gsig,
                )
                break
            log.info(
                "VDP: %s — no extractable ep/vehicle fields from %s (trying alternate URL if any)",
                dealer_name,
                try_url[:120],
            )

        img_net = len(
            {u for u in response_image_urls if isinstance(u, str) and u.lower().startswith("https://")}
        )
        g_total = (
            _vdp_count_gallery_signals(network_rows, last_bundle) + len(extra_loop_gallery) + img_net
        )
        dom_vhr: list[Any] = []
        dom_mono: list[Any] = []
        if isinstance(last_bundle, dict):
            dom_vhr = last_bundle.get("domVehicleHistoryUrls") or []
            dom_mono = last_bundle.get("domMonroneyTextSnippets") or []
        has_dom_history = isinstance(dom_vhr, list) and any(
            isinstance(x, str) and x.strip().lower().startswith("http") for x in dom_vhr
        )
        has_mono_text = isinstance(dom_mono, list) and any(
            isinstance(x, str) and len(x.strip()) >= 50 for x in dom_mono
        )
        price_hints_n = 0
        if isinstance(last_bundle, dict):
            price_hints_n = len([h for h in (last_bundle.get("vdpPriceHints") or []) if isinstance(h, dict)])
        if (
            not combined_ep
            and g_total < 2
            and not has_dom_history
            and not has_mono_text
            and price_hints_n == 0
        ):
            log.info(
                "VDP: %s — no extractable ep/vehicle fields and minimal gallery signals after %d URL attempt(s)",
                dealer_name,
                len(urls_to_try),
            )
            return out

        filled: list[str] = []
        if combined_ep:
            log_exterior_downgrade_skip(v, combined_ep, log, "vdp_combined")
            diag: dict[str, Any] = {}
            filled = merge_analytics_ep_into_vehicle(v, combined_ep, diagnostics=diag)
            out["filled"] = list(filled)
            out["skipped"] = list(diag.get("skipped") or [])
            log.info(
                "VDP: %s — merge diagnostics VIN %s | ep_keys=%s | filled=%s | eligible=%s | skipped=%s",
                dealer_name,
                vin[:17],
                diag.get("ep_keys"),
                diag.get("filled"),
                diag.get("eligible"),
                diag.get("skipped"),
            )
        else:
            out["filled"] = []
            out["skipped"] = []

        if not v.get("source_url"):
            v["source_url"] = success_u

        cand_gallery: list[str] = []
        gseen: set[str] = set()
        mx_cap = max(inventory_gallery_max(), 200)

        def _push_g(batch: list[str]) -> None:
            merge_https_url_batches(cand_gallery, gseen, batch, max_total=mx_cap)

        if isinstance(last_bundle, dict):
            for key in ("domGalleryUrls", "jsonGalleryUrls"):
                _push_g([u2 for u2 in (last_bundle.get(key) or []) if isinstance(u2, str)])
        for row in network_rows:
            _push_g([u2 for u2 in (row.get("image_urls") or []) if isinstance(u2, str)])
        _push_g(extra_loop_gallery)
        _push_g(list(response_image_urls))

        gmerge = merge_vdp_gallery_into_vehicle(
            v,
            cand_gallery,
            max_gallery=inventory_gallery_max(),
        )
        out["gallery_added"] = int(gmerge.get("added") or 0)
        out["gallery_merge_action"] = gmerge.get("action")

        dom_carfax_updated = False
        if isinstance(last_bundle, dict):
            vhr_dom = last_bundle.get("domVehicleHistoryUrls") or []
            dom_carfax_updated = _merge_vdp_vehicle_history_url(v, vhr_dom)
            if dom_carfax_updated:
                filled.append("carfax_url")
                out["filled"] = list(filled)
            snips = last_bundle.get("domMonroneyTextSnippets") or []
            if isinstance(snips, list):
                clean_snips = [s for s in snips if isinstance(s, str) and s.strip()]
                if clean_snips:
                    prev_txt = v.get("_monroney_page_texts")
                    if not isinstance(prev_txt, list):
                        prev_txt = []
                    v["_monroney_page_texts"] = (prev_txt + clean_snips)[:8]

        pdiag = _apply_vdp_price_hints(v, last_bundle, success_u)
        out["price_updated"] = bool(pdiag.get("updated"))

        if _vdp_download_images_enabled():
            try:
                await _download_vdp_gallery_images(wp, v, v.get("gallery") if isinstance(v.get("gallery"), list) else [])
            except Exception as e:
                log.warning("VDP: %s — image download: %s", dealer_name, str(e)[:160])

        if filled:
            log.info(
                "VDP: %s — filled %s for VIN %s",
                dealer_name,
                filled,
                vin[:17],
            )
        elif combined_ep:
            log.info(
                "VDP: %s — merge did not add fields (already populated or no gap) for VIN %s",
                dealer_name,
                vin[:17],
            )

        if int(out.get("gallery_added") or 0) > 0 or gmerge.get("action") in ("replace", "extend"):
            log.info(
                "VDP: %s — gallery merge VIN %s action=%s added=%s final_len=%s",
                dealer_name,
                vin[:17],
                gmerge.get("action"),
                gmerge.get("added"),
                gmerge.get("final_len"),
            )
        if filled or int(out.get("gallery_added") or 0) > 0 or out.get("price_updated") or dom_carfax_updated:
            out["enriched"] = True
        return out
    except Exception as e:
        log.warning("VDP: %s — visit failed for %s: %s", dealer_name, u[:120], e)
        return out
    finally:
        _detach_response_handler(wp, on_response)


async def enrich_vehicles_vdp(
    page,
    vehicles: list[dict[str, Any]],
    dealer_name: str,
    *,
    dealer_id: str = "",
) -> dict[str, Any]:
    """
    Mutates vehicles in place: runs VDP extraction and merge_analytics_ep_into_vehicle.
    Expects vehicles deduped by VIN; uses _detail_url when present.

    Returns stats for scanner timing: vdps_visited, vehicles_enriched, rows_inventory (input len).
    """
    stats: dict[str, Any] = {
        "vdps_visited": 0,
        "vehicles_enriched": 0,
        "gallery_vdp_urls_added": 0,
        "inventory_rows": len(vehicles),
        "skipped_no_detail_url": False,
        "gallery_phase_bins": {},
    }
    max_v = _vdp_max_per_dealer()
    if max_v == 0:
        log.info("VDP: %s — enrichment skipped (SCANNER_VDP_EP_MAX=0)", dealer_name)
        return stats

    bins_before = gallery_https_bin_histogram(vehicles)
    seed = _vdp_rotation_seed(dealer_id)
    rot = _vdp_rotation_enabled()
    vehicles.sort(key=lambda v: _vdp_queue_sort_key(v, seed, rotation=rot))

    log.info(
        "VDP: %s — enrichment enabled (max %d VDP visit(s) per dealer; set SCANNER_VDP_EP_MAX=0 to disable; "
        "rotation=%s)",
        dealer_name,
        max_v,
        rot,
    )

    if not any(str(v.get("_detail_url") or "").strip().startswith("http") for v in vehicles):
        log.info(
            "VDP: %s — no _detail_url on inventory rows; skipping VDP visits (listing JSON may omit VDP links)",
            dealer_name,
        )
        stats["skipped_no_detail_url"] = True
        return stats

    work: list[tuple[dict[str, Any], str, str]] = []
    seen_urls: set[str] = set()
    for v in vehicles:
        if len(work) >= max_v:
            break
        u = (v.get("_detail_url") or "").strip()
        if not u.startswith("http"):
            continue
        if u in seen_urls:
            continue
        seen_urls.add(u)
        vin = (v.get("vin") or "").strip().upper()
        if not _looks_like_vin17(vin):
            continue
        work.append((v, u, vin))

    if not work:
        return stats

    conc = min(_max_vdp_concurrency(), len(work))
    preview_lock = asyncio.Lock()
    preview_budget = [2]
    field_fill_counter: Counter[str] = Counter()
    skip_reason_counter: Counter[str] = Counter()
    vehicles_enriched = 0
    visited = 0
    gallery_urls_added_total = 0

    log.info("VDP pool: %s — %d concurrent worker page(s) (%d visit(s) queued)", dealer_name, conc, len(work))

    async def aggregate_one(r: dict[str, Any]) -> None:
        nonlocal visited, vehicles_enriched, gallery_urls_added_total
        visited += int(r.get("visited", 0))
        if r.get("enriched"):
            vehicles_enriched += 1
        gallery_urls_added_total += int(r.get("gallery_added") or 0)
        for fn in r.get("filled") or []:
            field_fill_counter[fn] += 1
        if r.get("price_updated"):
            field_fill_counter["price"] += 1
        for sk in r.get("skipped") or []:
            head = sk.split(":", 1)[0].strip() if ":" in str(sk) else str(sk).strip()
            if head:
                skip_reason_counter[head] += 1

    worker_pages: list[Any] = []

    try:
        if conc <= 1:
            for v, u, vin in work:
                try:
                    r = await _vdp_visit_one(page, dealer_name, v, u, vin, preview_lock, preview_budget)
                    await aggregate_one(r)
                except Exception as e:
                    log.warning("VDP: %s — visit error (continuing): %s", dealer_name, e)
        else:
            ctx = page.context
            worker_pages = [await ctx.new_page() for _ in range(conc)]
            pool: asyncio.Queue[Any] = asyncio.Queue()
            for wp in worker_pages:
                await pool.put(wp)

            async def run_item(item: tuple[dict[str, Any], str, str]) -> None:
                v, u, vin = item
                wp = await pool.get()
                try:
                    r = await _vdp_visit_one(wp, dealer_name, v, u, vin, preview_lock, preview_budget)
                    await aggregate_one(r)
                except Exception as e:
                    log.warning("VDP: %s — visit error (continuing): %s", dealer_name, e)
                finally:
                    await pool.put(wp)

            results = await asyncio.gather(*[run_item(w) for w in work], return_exceptions=True)
            for res in results:
                if isinstance(res, Exception):
                    log.warning("VDP: %s — worker task failed: %s", dealer_name, res)

        stats["vdps_visited"] = visited
        stats["vehicles_enriched"] = vehicles_enriched
        stats["gallery_vdp_urls_added"] = gallery_urls_added_total
        stats["gallery_phase_bins"] = {
            "before_vdp": bins_before,
            "after_vdp": gallery_https_bin_histogram(vehicles),
        }
        log.info(
            "VDP: %s — phase summary: visits=%d vehicles_enriched=%d gallery_urls_added=%d "
            "top_fields_filled=%s common_skip_reasons=%s gallery_bins_after=%s",
            dealer_name,
            visited,
            vehicles_enriched,
            gallery_urls_added_total,
            field_fill_counter.most_common(14),
            skip_reason_counter.most_common(10),
            stats["gallery_phase_bins"].get("after_vdp"),
        )
    finally:
        for wp in worker_pages:
            try:
                await wp.close()
            except Exception:
                pass

    return stats
