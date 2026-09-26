"""Per-vehicle endpoint recipes: the JSON a dealer's VDP page requests for ONE car,
captured while the browser is there anyway, templated by VIN / stock number, and
replayed over plain HTTP for every other car on the next scan.

Why: the inventory recipes (``backend.scanner.recipes``) only cover the list feed.
Everything the VDP page pulls per car (Dealer Inspire's vehicle payload, DealerOn's
gallery call, Team Velocity's image endpoint, analytics ``ep`` objects) was seen by
the browser on every visit and discarded, so the same page had to be rendered again
next time. A dealer whose per-car JSON is known needs no browser at all.

Lifecycle
  browser visit  -> ``record_candidate``  (visit.capture_response, per JSON response
                    whose URL or body names the car)
  dealer done    -> ``promote_candidates`` (dispatch, after the VDP pool)
  next scan      -> ``apply_vdp_recipes``  (prefetch, before the HTML http-first pass)

Storage: ``workspace/recipes/vdp/<dealer>.json`` (file only for now; the list
recipes' Postgres mirror does not carry these yet).
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlparse

log = logging.getLogger("scanner.vdp")

VDP_RECIPES_DIR = Path(__file__).resolve().parents[3] / "workspace" / "recipes" / "vdp"

_MAX_RECIPES_PER_DEALER = 6
_MAX_FAILS_BEFORE_STALE = 3
_REPLAY_TIMEOUT_S = 15.0
_REPLAY_CONCURRENCY = 6
_POLITE_DELAY_S = 0.15
_MIN_SCORE = 15.0  # same bar the browser uses for network_vehicle_json fragments
_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)
_SKIP_HOST_PARTS = (
    "google", "doubleclick", "facebook", "gstatic", "cloudflare", "hotjar", "clarity.ms",
    "newrelic", "sentry", "segment.io", "adsrvr", "criteo", "bing.com", "tiktok",
)


def replay_enabled() -> bool:
    return (os.environ.get("SCANNER_VDP_RECIPE_REPLAY") or "1").strip().lower() not in ("0", "false", "no", "off")


def capture_enabled() -> bool:
    return (os.environ.get("SCANNER_VDP_RECIPE_CAPTURE") or "1").strip().lower() not in ("0", "false", "no", "off")


@dataclass
class VdpRecipe:
    dealer_id: str
    url_template: str
    method: str = "GET"
    post_template: str | None = None
    auth_headers: dict[str, str] = field(default_factory=dict)
    provider_hint: str = ""
    hits: int = 0          # browser visits where this endpoint answered for the car
    ep_hits: int = 0       # ... and carried spec/price fields
    image_hits: int = 0    # ... and carried gallery URLs
    saved_at: float = 0.0
    last_ok_at: float = 0.0
    fails: int = 0
    stale: bool = False
    stale_reason: str = ""

    def key(self) -> tuple[str, str]:
        p = urlparse(self.url_template)
        return (self.method, f"{(p.hostname or '').lower()}{p.path}")


# --------------------------------------------------------------------------
# templating
# --------------------------------------------------------------------------

def templatize(text: str | None, vin: str, stock: str | None) -> tuple[str | None, bool]:
    """Replace the car's VIN (and stock number when distinctive) with placeholders.
    Returns (templated text, whether anything was substituted)."""
    if not text:
        return text, False
    out = text
    hit = False
    v = (vin or "").strip()
    if len(v) == 17:
        new = re.sub(re.escape(v), "{vin}", out, flags=re.I)
        if new != out:
            out, hit = new, True
    s = (stock or "").strip()
    if len(s) >= 5 and not s.isdigit() or len(s) >= 7:
        new = re.sub(re.escape(s), "{stock}", out, flags=re.I)
        if new != out:
            out, hit = new, True
    return out, hit


def _fill(template: str | None, v: dict[str, Any]) -> str | None:
    if template is None:
        return None
    vin = str(v.get("vin") or "").strip().upper()
    stock = str(v.get("stock_number") or "").strip()
    if "{vin}" in template and len(vin) != 17:
        return None
    if "{stock}" in template and not stock:
        return None
    return template.replace("{vin}", vin).replace("{stock}", stock)


# --------------------------------------------------------------------------
# capture side (browser)
# --------------------------------------------------------------------------

_CANDIDATES: dict[str, dict[tuple[str, str], VdpRecipe]] = {}


def _skip_host(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower()
    return any(part in host for part in _SKIP_HOST_PARTS)


def record_candidate(
    dealer_name: str,
    *,
    url: str,
    method: str,
    post_data: str | None,
    headers: dict[str, str] | None,
    vin: str,
    stock: str | None,
    score: float,
    ep_count: int,
    image_count: int,
) -> bool:
    """Called from the browser's response capture for JSON that scored as vehicle
    data. Keeps it only when the request itself names the car (VIN or stock in the
    URL or POST body), which is what makes it replayable for a different car."""
    if not capture_enabled() or not url.startswith("http") or _skip_host(url):
        return False
    if score < _MIN_SCORE and ep_count == 0 and image_count < 3:
        return False
    url_t, hit_u = templatize(url, vin, stock)
    post_t, hit_p = templatize(post_data, vin, stock)
    if not (hit_u or hit_p):
        return False
    from backend.scanner.network_observer import extract_auth_headers

    rec = VdpRecipe(
        dealer_id="",
        url_template=url_t or url,
        method=(method or "GET").upper(),
        post_template=post_t if (method or "GET").upper() != "GET" else None,
        auth_headers=extract_auth_headers(dict(headers or {})),
        hits=1,
        ep_hits=1 if ep_count else 0,
        image_hits=1 if image_count >= 3 else 0,
    )
    bucket = _CANDIDATES.setdefault(dealer_name, {})
    cur = bucket.get(rec.key())
    if cur is None:
        bucket[rec.key()] = rec
    else:
        cur.hits += rec.hits
        cur.ep_hits += rec.ep_hits
        cur.image_hits += rec.image_hits
        if not cur.auth_headers and rec.auth_headers:
            cur.auth_headers = rec.auth_headers
    return True


def promote_candidates(dealer_id: str, dealer_name: str, provider: str = "") -> int:
    """Merge this run's candidates into the dealer's VDP recipe file. Returns the
    number of recipes kept."""
    cands = _CANDIDATES.pop(dealer_name, None) or {}
    if not cands:
        return 0
    now = time.time()
    merged: dict[tuple[str, str], VdpRecipe] = {r.key(): r for r in load_vdp_recipes(dealer_id)}
    for k, c in cands.items():
        c.dealer_id = dealer_id
        c.provider_hint = provider or ""
        c.saved_at = now
        cur = merged.get(k)
        if cur is not None:
            c.hits += cur.hits
            c.ep_hits += cur.ep_hits
            c.image_hits += cur.image_hits
            c.last_ok_at = cur.last_ok_at
            if not c.auth_headers and cur.auth_headers:
                c.auth_headers = dict(cur.auth_headers)
        merged[k] = c  # a fresh capture clears a stale flag
    ranked = sorted(merged.values(), key=lambda r: (r.stale, -r.ep_hits, -r.image_hits, -r.hits))
    keep = ranked[:_MAX_RECIPES_PER_DEALER]
    save_vdp_recipes(dealer_id, keep)
    log.info(
        "VDP recipes [%s]: %d endpoint(s) kept (%d captured this run): %s",
        dealer_name, len(keep), len(cands),
        ", ".join(f"{urlparse(r.url_template).path[:50]} ep={r.ep_hits} img={r.image_hits}" for r in keep[:4]),
    )
    return len(keep)


# --------------------------------------------------------------------------
# storage
# --------------------------------------------------------------------------

def _path(dealer_id: str) -> Path:
    from backend.scanner.recipes import _recipe_slug

    return VDP_RECIPES_DIR / f"{_recipe_slug(dealer_id)}.json"


def load_vdp_recipes(dealer_id: str) -> list[VdpRecipe]:
    p = _path(dealer_id)
    if not p.exists():
        return []
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return []
    out: list[VdpRecipe] = []
    for row in raw if isinstance(raw, list) else []:
        try:
            out.append(VdpRecipe(**{k: v for k, v in row.items() if k in VdpRecipe.__dataclass_fields__}))
        except TypeError:
            continue
    return out


def save_vdp_recipes(dealer_id: str, recipes: list[VdpRecipe]) -> None:
    VDP_RECIPES_DIR.mkdir(parents=True, exist_ok=True)
    _path(dealer_id).write_text(json.dumps([asdict(r) for r in recipes], indent=1), encoding="utf-8")


# --------------------------------------------------------------------------
# replay side (HTTP)
# --------------------------------------------------------------------------

def _fetch_json(recipe: VdpRecipe, url: str, payload: str | None, base_url: str) -> tuple[int, Any | None]:
    headers = {
        "User-Agent": _UA,
        "Accept": "application/json, text/plain, */*",
        "Origin": base_url.rstrip("/"),
        "Referer": base_url.rstrip("/") + "/",
        **recipe.auth_headers,
    }
    if recipe.method != "GET":
        headers["Content-Type"] = "application/json"
    from backend.scanner.http_fetch import proxy_url

    proxies = {"http": proxy_url(), "https": proxy_url()} if proxy_url() else None
    try:
        from curl_cffi import requests as cffi_requests

        resp = cffi_requests.request(
            recipe.method, url, headers=headers, data=payload, impersonate="chrome",
            timeout=_REPLAY_TIMEOUT_S, proxies=proxies,
        )
        status, text = resp.status_code, resp.text
    except ImportError:
        import requests

        try:
            resp = requests.request(recipe.method, url, headers=headers, data=payload, timeout=_REPLAY_TIMEOUT_S, proxies=proxies)
            status, text = resp.status_code, resp.text
        except Exception:  # noqa: BLE001
            return 0, None
    except Exception:  # noqa: BLE001
        return 0, None
    if status != 200 or not text:
        return status, None
    try:
        parsed = json.loads(text)
    except ValueError:
        return status, None
    return status, parsed if isinstance(parsed, (dict, list)) else None


def _row_from_parsed(url: str, parsed: Any) -> dict[str, Any]:
    """Same shape ``visit.capture_response`` appends to ``network_rows``."""
    from backend.parsers.base import harvest_image_urls_from_json
    from backend.scanner.vdp.extract import _analyze_json_signals, _response_origin

    score, key_hits, ep_objs = _analyze_json_signals(parsed)
    images = harvest_image_urls_from_json(parsed, _response_origin(url), max_urls=96)
    return {
        "url": url[:500], "score": score, "key_hits": key_hits[:20],
        "ep_objects": ep_objs, "parsed": parsed, "image_urls": images,
    }


def apply_row_to_vehicle(v: dict[str, Any], row: dict[str, Any], *, gallery_min: int) -> tuple[list[str], int]:
    """Merge one replayed JSON row into the vehicle the way a browser visit would.
    Returns (fields filled, gallery URLs added)."""
    from backend.scanner.vdp.extract import _build_fragments_from_vdp_capture, _combine_ep_fragments
    from backend.scanner.vdp.queue import _count_https_gallery_urls
    from backend.utils.analytics_ep import merge_analytics_ep_into_vehicle, normalize_ep_field_aliases

    vin = str(v.get("vin") or "").strip().upper()
    frags, _hits, _err = _build_fragments_from_vdp_capture([row], None, vin)
    combined = normalize_ep_field_aliases(_combine_ep_fragments(frags, vin)) if frags else {}
    filled: list[str] = []
    if combined:
        filled = list(merge_analytics_ep_into_vehicle(v, combined) or [])
    added = 0
    if _count_https_gallery_urls(v) < gallery_min:
        cur = v.get("gallery") if isinstance(v.get("gallery"), list) else []
        seen = {u for u in cur if isinstance(u, str)}
        merged = list(cur)
        for u in row.get("image_urls") or []:
            if isinstance(u, str) and u.lower().startswith("https://") and u not in seen:
                seen.add(u)
                merged.append(u)
        added = len(merged) - len(cur)
        if added:
            v["gallery"] = merged
            v.setdefault("_gallery_source", "vdp_recipe")
    return filled, added


async def apply_vdp_recipes(
    vehicles: list[dict[str, Any]],
    dealer_id: str,
    *,
    wants: Callable[[dict[str, Any]], bool],
    gallery_min: int,
    base_url: str = "",
) -> dict[str, Any]:
    """Replay the dealer's VDP recipes for every vehicle that still has a gap.
    Mutates vehicles in place; returns stats."""
    stats: dict[str, Any] = {"recipes": 0, "candidates": 0, "fetched": 0, "fields_filled": 0, "galleries_extended": 0, "stale_marked": 0}
    if not replay_enabled():
        return stats
    recipes = [r for r in load_vdp_recipes(dealer_id) if not r.stale]
    stats["recipes"] = len(recipes)
    if not recipes:
        return stats
    todo = [v for v in vehicles if wants(v)]
    stats["candidates"] = len(todo)
    if not todo:
        return stats
    if not base_url:
        u = str(todo[0].get("_detail_url") or todo[0].get("source_url") or "")
        p = urlparse(u)
        base_url = f"{p.scheme}://{p.netloc}" if p.netloc else ""
    sem = asyncio.Semaphore(_REPLAY_CONCURRENCY)
    fails: dict[tuple[str, str], int] = {r.key(): 0 for r in recipes}
    oks: dict[tuple[str, str], int] = {r.key(): 0 for r in recipes}

    async def _one(v: dict[str, Any]) -> None:
        for r in recipes:
            if fails[r.key()] >= _MAX_FAILS_BEFORE_STALE and oks[r.key()] == 0:
                continue
            url = _fill(r.url_template, v)
            payload = _fill(r.post_template, v)
            if not url:
                continue
            async with sem:
                status, parsed = await asyncio.to_thread(_fetch_json, r, url, payload, base_url)
                await asyncio.sleep(_POLITE_DELAY_S)
            if parsed is None:
                if status in (401, 403, 404, 410, 429) or status == 0:
                    fails[r.key()] += 1
                continue
            oks[r.key()] += 1
            stats["fetched"] += 1
            row = _row_from_parsed(url, parsed)
            filled, added = apply_row_to_vehicle(v, row, gallery_min=gallery_min)
            stats["fields_filled"] += len(filled)
            if added:
                stats["galleries_extended"] += 1

    try:
        await asyncio.wait_for(asyncio.gather(*(_one(v) for v in todo)), timeout=float(os.environ.get("SCANNER_VDP_RECIPE_MAX_SEC") or 300))
    except asyncio.TimeoutError:
        stats["wall_clock_hit"] = True
        log.warning("VDP recipes [%s]: wall clock reached after %d/%d car(s); keeping what was filled", dealer_id, stats["fetched"], stats["candidates"])
    now = time.time()
    changed = False
    for r in recipes:
        k = r.key()
        if oks[k]:
            r.last_ok_at, r.fails, changed = now, 0, True
        elif fails[k] >= _MAX_FAILS_BEFORE_STALE:
            r.fails += fails[k]
            r.stale, r.stale_reason, changed = True, f"{fails[k]} consecutive replay failures", True
            stats["stale_marked"] += 1
    if changed:
        save_vdp_recipes(dealer_id, _merge_state(dealer_id, recipes))
    return stats


def _merge_state(dealer_id: str, updated: list[VdpRecipe]) -> list[VdpRecipe]:
    cur = {r.key(): r for r in load_vdp_recipes(dealer_id)}
    for r in updated:
        cur[r.key()] = r
    return list(cur.values())
