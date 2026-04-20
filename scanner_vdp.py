"""
VDP (vehicle detail page) enrichment for the Playwright scanner (scanner.py).

Runs during the main scan when SCANNER_VDP_EP_MAX > 0: visits up to N unique VDP URLs
per dealer, extracts analytics ep.*, network JSON, JSON-LD, inline JSON, and DOM heuristics,
then merges into vehicle rows via merge_analytics_ep_into_vehicle (conservative fallback).
Gallery URLs from network JSON and in-page extraction are merged separately (see
backend.utils.gallery_merge.merge_vdp_gallery_into_vehicle) after EP merge.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
from collections import Counter
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

log = logging.getLogger("scanner.vdp")

MAX_JSON_BYTES = 2 * 1024 * 1024


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
  return result;
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
    }
    visit_epoch = [0]
    network_rows: list[dict[str, Any]] = []
    pending: list[asyncio.Task[Any]] = []

    async def capture_response(response) -> None:
        my_epoch = visit_epoch[0]
        try:
            if response.status != 200:
                return
            ct = (response.headers.get("content-type") or "").lower()
            if "application/json" not in ct:
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
        for try_url in urls_to_try:
            visit_epoch[0] += 1
            network_rows.clear()
            out["visited"] = int(out.get("visited") or 0) + 1

            log.info("VDP: %s — visiting %s", dealer_name, try_url[:200])

            nav_err = None
            try:
                await wp.goto(try_url, wait_until="domcontentloaded", timeout=_nav_timeout_ms())
            except Exception as e:
                nav_err = str(e)
            await asyncio.sleep(_settle_ms() / 1000.0)
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
                pending.clear()

            if nav_err:
                log.warning("VDP: %s — navigation issue: %s", dealer_name, nav_err[:120])

            try:
                bundle = await wp.evaluate(PAGE_EXTRACT_JS)
            except Exception as e:
                bundle = {"error": str(e)}
            last_bundle = bundle if isinstance(bundle, dict) else {}

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
            gsig = _vdp_count_gallery_signals(network_rows, last_bundle)
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

        g_total = _vdp_count_gallery_signals(network_rows, last_bundle)
        if not combined_ep and g_total < 2:
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

        origin = _response_origin(success_u)
        cand_gallery: list[str] = []
        if isinstance(last_bundle, dict):
            for key in ("domGalleryUrls", "jsonGalleryUrls"):
                for u2 in last_bundle.get(key) or []:
                    if isinstance(u2, str):
                        cand_gallery.append(u2)
        for row in network_rows:
            for u2 in row.get("image_urls") or []:
                if isinstance(u2, str):
                    cand_gallery.append(u2)

        gmerge = merge_vdp_gallery_into_vehicle(
            v,
            cand_gallery,
            max_gallery=inventory_gallery_max(),
        )
        out["gallery_added"] = int(gmerge.get("added") or 0)
        out["gallery_merge_action"] = gmerge.get("action")

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
        if filled or int(out.get("gallery_added") or 0) > 0:
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
