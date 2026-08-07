"""
VDP capture analysis and EP-fragment extraction.

Pure functions that turn captured network JSON, ``PAGE_EXTRACT_JS`` bundles, JSON-LD and
DOM specs into merged EP fragments, plus response-classification helpers and the
Carfax / window-sticker URL pickers. Leaf module — no intra-package imports.
"""
from __future__ import annotations

import logging
import re
from typing import Any
from urllib.parse import urlparse

log = logging.getLogger("scanner.vdp")

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


def _pick_best_sticker_url(dom_urls: list[Any], vin: str | None = None) -> str | None:
    from backend.scanner.post_scan.window_sticker import pick_best_listing_sticker_url

    urls = [str(u).strip() for u in dom_urls if isinstance(u, str) and str(u).strip().startswith("http")]
    return pick_best_listing_sticker_url(urls, vin=vin)


def _merge_vdp_sticker_url(vehicle: dict[str, Any], dom_urls: list[Any]) -> bool:
    """Set ``window_sticker_url`` from VDP iPacket / Monroney links when missing or improved."""
    picked = _pick_best_sticker_url(dom_urls, str(vehicle.get("vin") or ""))
    if not picked:
        return False
    cur = str(vehicle.get("window_sticker_url") or "").strip()
    if not cur.lower().startswith("http"):
        vehicle["window_sticker_url"] = picked
        return True
    cur_low = cur.lower()
    picked_low = picked.lower()
    if "sticker-puller" in picked_low and "sticker-puller" not in cur_low:
        vehicle["window_sticker_url"] = picked
        return True
    if "token=" in picked_low and "token=" not in cur_low:
        vehicle["window_sticker_url"] = picked
        return True
    return False


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
            else:
                m1 = re.search(r"(\d{1,2})", val)
                if m1 and re.search(r"city", lk):
                    flat["city_fuel_economy"] = m1.group(1)
                elif m1 and re.search(r"highway|hwy", lk):
                    flat["highway_fuel_economy"] = m1.group(1)
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
