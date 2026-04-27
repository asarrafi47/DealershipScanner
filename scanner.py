#!/usr/bin/env python3
"""
Manifest-driven dealership inventory scanner. Uses playwright + stealth,
session warmup to avoid 403, network interception for JSON, HTML fallback, and
optional ``__NEXT_DATA__`` (Next.js) extraction when the raw HTML shell has no rows.

Gallery / images (inventory + VDP):
  SCANNER_INVENTORY_GALLERY_MAX — max URLs stored per vehicle after dedupe (default: 48).
  Inventory parsers deep-walk intercepted JSON for nested media keys (see backend/parsers/base.py).

Optional VDP (detail page) enrichment (see scanner_vdp):
  SCANNER_VDP_EP_MAX — max VDP **page navigations** per dealer (default: 10; set 0 to disable).
    This single budget covers both EP-style spec extraction and gallery harvesting; there is no
    separate paid API. To “do more VDP for images”, raise this value (full dealer runs: keep
    moderate to avoid opening thousands of VDPs).
  SCANNER_VDP_GALLERY_MIN_HTTPS — rows with fewer than this many unique https gallery URLs get
    extra priority in the VDP visit queue when SCANNER_VDP_GALLERY_PRIORITY is enabled (default: 3).
  SCANNER_VDP_GALLERY_PRIORITY — truthy (default): thin-gallery vehicles compete for VDP slots.
  SCANNER_VDP_ROTATION — truthy (default): among equal-priority rows, rotate which VINs get VDP visits
    using SCANNER_VDP_ROTATION_SEED or the UTC date + dealer_id (long-tail coverage when EP_MAX is low).
  SCANNER_VDP_NAV_TIMEOUT_MS — navigation timeout per VDP (default: 32000).
  SCANNER_VDP_SETTLE_MS — wait after load for analytics/XHR (default: 2200).
  SCANNER_VDP_GALLERY_MAX_ROUNDS — max carousel-advance iterations per VDP (default: 80).
  SCANNER_VDP_GALLERY_IDLE_ROUNDS — stop gallery loop after this many rounds with no new HTTPS URL (default: 3).
  SCANNER_VDP_DOWNLOAD_IMAGES — default on: persist gallery bytes under SCANNER_VDP_IMAGE_DOWNLOAD_DIR
    (default ``vdp_images``) keyed by VIN or stock (``SCANNER_VDP_IMAGE_DOWNLOAD_KEY=vin|stock``); manifest +
    ``spec_source_json.vdp_gallery_local`` updated on the vehicle row (see backend.database upsert). Set to ``0``
    to disable.
  VDP price (JSON-LD / dataLayer / DOM) fills ``price`` only when the scraped row has no positive price;
    provenance is stored under ``spec_source_json.vdp_price``.
  SCANNER_GALLERY_MERGE_REPLACE_IF_BELOW — VDP merge replaces whole gallery when existing https
    count is below this (default: 3); otherwise VDP URLs extend listing gallery (see gallery_merge).
  SCANNER_MAX_DEALER_CONCURRENCY — parallel dealer scans (default: 2; set 1 for sequential).
  SCANNER_MAX_VDP_CONCURRENCY — parallel VDP page visits per dealer (default: 2).

Inventory JSON intercept gating (drops third-party vehicle-shaped JSON, e.g. payment widgets):
  SCANNER_INTERCEPT_URL_ALLOW — comma-separated URL substrings that always allow (lowercased match).
  SCANNER_INTERCEPT_URL_DENY — comma-separated substrings that always deny (e.g. carnow.com,payments).
  Default: same registrable host / subdomain as the dealer base URL from dealers.json, OR substrings
  from config/scanner_intercept_policy.json (mirrored in scanner.js), after the deny list is applied.

Inventory page timing:
  SCANNER_INVENTORY_WAIT_MS — max milliseconds for ``page.wait_for_event("response", ...)`` after
    each inventory navigation (default 18000). Uses the same URL gate as the predicate; vehicle-list
    validation still runs in the response handler.
  SCANNER_PAGINATION_RESPONSE_WAIT_MS — after each Next/Load-more click, max ms to wait for a matching
    JSON response (default 8000, capped by SCANNER_INVENTORY_WAIT_MS), then a short fixed tail sleep.

Warmup (first dealer base URL load):
  SCANNER_WARMUP_POST_GOTO_SEC — max seconds to wait after domcontentloaded (default 4; set 0 to skip
    the idle cap arm). Races against SCANNER_WARMUP_SIGNAL_TIMEOUT_MS for the first JSON response that
    passes the inventory URL gate — whichever finishes first ends the wait early.
  SCANNER_WARMUP_SIGNAL_TIMEOUT_MS — max ms to wait for that first qualifying ``response`` (default 12000).
  SCANNER_WARMUP_SCROLL_SEC — seconds after mid-page scroll (default 1; set 0 to skip).
  SCANNER_WARMUP_DOM_SELECTORS — optional comma-separated CSS selectors tried briefly before scroll
    (defaults include ``[data-vehicle]``, ``[data-vin]``, ``.vehicle-card``, …).

Navigation resilience:
  SCANNER_GOTO_MAX_ATTEMPTS — retries per ``page.goto`` with exponential backoff + jitter (default 3).
  SCANNER_GOTO_403_EXTRA_ATTEMPTS — extra tries after HTTP 403/429 with longer backoff (default 2).

Browser:
  SCANNER_USER_AGENT — optional; when unset, Playwright’s default Chromium UA is used (recommended).
    Set only when you need a pinned string for testing.

Post-scan pipeline (SQLite, same run):

  By default, after all dealers finish, rows for VINs touched in this scan are repaired
  (``backend.utils.inventory_repair``: placeholders, ``merge_verified_specs`` backfill, condition).

  SCANNER_POST_REPAIR — ``0`` / ``false`` / ``off`` disables repair (default: on).

  By default, each touched row's ``description`` is parsed into structured ``packages`` JSON
  (``packages_normalized``, OEM catalog hints, optional interior from text). Set
  SCANNER_POST_LISTING_DESCRIPTION=0 or use ``--no-post-listing-description`` to skip.
  Optional LLM tier: LISTING_DESC_PARSE_USE_LLM=1 (local Ollama).

  Optional enrichment (EPA catalog in Postgres + Ollama vision), after repair + listing parse:

  --post-enrich / SCANNER_POST_ENRICH=1 — ``InventoryEnricher`` for scanned VINs only (needs catalog).

  --post-enrich-vision-only / SCANNER_POST_ENRICH_VISION=1 — vision pass only (catalog not required).

  Per-dealer inventory reconcile (after each successful upsert): soft-unlist DB rows for that
  dealer whose VIN is no longer in the scraped feed (``listing_active=0``). Disabled when
  SCANNER_RECONCILE=0. Skipped when ``deduped_rows`` < SCANNER_RECONCILE_MIN_ROWS (default 8) or
  when there are no valid normalized VINs in the scrape.

Vision passes (default **on** for ``python scanner.py``; requires local Ollama, default
``OLLAMA_VISION_MODEL=llava:13b``):

  **Gallery** — each HTTPS gallery / hero URL is classified with LLaVA; non-vehicle images are
  removed. Opt out: ``SCANNER_GALLERY_VISION_FILTER=0`` or ``--no-gallery-vision-filter``.
  ``SCANNER_GALLERY_VISION_MAX_WORKERS`` (default ``1``) sets parallel Ollama calls per vehicle.

  **Monroney** — after gallery cleanup, LLaVA reads sticker-like URLs and VDP Monroney text into
  ``packages`` / empty specs. Opt out: ``SCANNER_MONRONEY_VISION=0`` or ``--no-monroney-vision``.

  **Post-scan interior** — LLaVA cabin inference for touched VINs (see ``SCANNER_POST_INTERIOR_VISION``).
  Default on; opt out: ``SCANNER_POST_INTERIOR_VISION=0`` or ``--no-post-interior-vision``.
  A cabin-appropriate image is selected via URL heuristics + LLaVA gallery classification
  (not only the first hero; avoids reading exterior as cabin). Tuning: ``INTERIOR_VISION_MAX_GALLERY_CLASSIFY``,
  ``INTERIOR_VISION_CONFIDENCE``, ``INTERIOR_VISION_OVERWRITE``, and optional legacy
  ``INTERIOR_VISION_FALLBACK_HERO=1`` to analyze the first HTTPS image if no cabin shot is found.

  **Post-scan KBB (optional)** — licensed Kelley Blue Book IDWS values for touched VINs
  (``--post-kbb`` or ``SCANNER_POST_KBB=1``). Requires ``KBB_API_KEY`` and usually a ZIP
  on each row or ``KBB_DEFAULT_ZIP``. See ``backend/kbb_idws.py``.

Run from project root: python scanner.py
"""
import argparse
import asyncio
import contextlib
import json
import logging
import os
import random
import sys
import time
from pathlib import Path
from typing import Any

# Project root = directory containing this file
ROOT = Path(__file__).resolve().parent
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))

try:
    from backend.utils.project_env import load_project_dotenv

    load_project_dotenv()
except ImportError:
    pass

# Import BMW enhancement
from bmw_enhancer import bmw_optimized_browsing, is_bmw_dealership, enhance_scraping_for_bmw_dealerships

from backend.database import upsert_vehicles
from backend.parsers import parse
from backend.parsers.base import find_vehicle_list
from backend.utils.gallery_merge import gallery_https_bin_histogram
from scanner_vdp import enrich_vehicles_vdp, _max_vdp_concurrency

from backend.scrapers.inventory_vin_merge import merge_inventory_rows_same_vin
from backend.scrapers.next_data_inventory import fetch_next_data_json_from_page, parse_next_data_json_from_html
from backend.scrapers.scanner_intercept_filter import (
    intercept_url_allowed,
    pick_total_count_from_intercepts,
)
from backend.scanner_post_pipeline import (
    aggregate_vins_from_dealer_results,
    gallery_vision_filter_env_enabled,
    monroney_vision_env_enabled,
    post_enrich_env_enabled,
    post_enrich_vision_env_enabled,
    post_interior_vision_env_enabled,
    post_listing_description_env_enabled,
    post_kbb_env_enabled,
    post_repair_env_enabled,
    run_post_scan,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("scanner")


def _log_gallery_bins(dealer_name: str, phase: str, vehicles: list[dict[str, Any]]) -> None:
    h = gallery_https_bin_histogram(vehicles)
    logger.info(
        "Gallery bins [%s] %s: https 0=%d 1=%d 2-4=%d 5+=%d (vehicles=%d)",
        dealer_name,
        phase,
        h["0"],
        h["1"],
        h["2_4"],
        h["5p"],
        len(vehicles),
    )


MANIFEST_PATH = ROOT / "dealers.json"
DEBUG_DIR = ROOT / "debug"
# Exhaustive category search: all three for every dealer
INVENTORY_PATHS = [
    "/new-inventory/index.htm",
    "/used-inventory/index.htm",
    "/certified-inventory/index.htm",
]
NEXT_SELECTORS = [
    'button:has-text("Next")',
    'button:has-text("Load More")',
    'a:has-text("Next")',
    '[data-action="next"]',
    '.pagination-next',
    '.load-more',
    'a:has-text("Load More")',
]
MAX_PAGINATION_CLICKS = 15


def _inventory_wait_ms() -> int:
    raw = (os.environ.get("SCANNER_INVENTORY_WAIT_MS") or "18000").strip()
    try:
        return max(1000, int(raw))
    except ValueError:
        return 18000


def _pagination_response_wait_ms() -> int:
    raw = (os.environ.get("SCANNER_PAGINATION_RESPONSE_WAIT_MS") or "8000").strip()
    try:
        return max(300, min(_inventory_wait_ms(), int(raw)))
    except ValueError:
        return min(_inventory_wait_ms(), 8000)


def _warmup_delays() -> tuple[float, float]:
    try:
        post = float((os.environ.get("SCANNER_WARMUP_POST_GOTO_SEC") or "4").strip())
    except ValueError:
        post = 4.0
    try:
        scroll = float((os.environ.get("SCANNER_WARMUP_SCROLL_SEC") or "1").strip())
    except ValueError:
        scroll = 1.0
    return max(0.0, post), max(0.0, scroll)


def _warmup_signal_timeout_ms() -> int:
    raw = (os.environ.get("SCANNER_WARMUP_SIGNAL_TIMEOUT_MS") or "12000").strip()
    try:
        return max(0, int(raw))
    except ValueError:
        return 12000


def _goto_403_extra_attempts() -> int:
    raw = (os.environ.get("SCANNER_GOTO_403_EXTRA_ATTEMPTS") or "2").strip()
    try:
        return max(0, int(raw))
    except ValueError:
        return 2


def _goto_max_attempts() -> int:
    raw = (os.environ.get("SCANNER_GOTO_MAX_ATTEMPTS") or "3").strip()
    try:
        return max(1, int(raw))
    except ValueError:
        return 3


async def _goto_with_retries(page: Any, target: str, *, log_label: str, timeout_ms: int = 30000) -> None:
    base_attempts = _goto_max_attempts()
    extra_403 = _goto_403_extra_attempts()
    max_total = base_attempts + extra_403
    last_exc: BaseException | None = None
    for i in range(max_total):
        try:
            resp = await page.goto(target, wait_until="domcontentloaded", timeout=timeout_ms)
        except BaseException as e:
            last_exc = e
            if i >= max_total - 1:
                break
            delay = 0.6 * (2 ** min(i, 8)) + random.random() * 0.35
            logger.warning(
                "%s: goto retry %s/%s (%s, sleep %.2fs)",
                log_label,
                i + 1,
                max_total,
                type(e).__name__,
                delay,
            )
            await asyncio.sleep(delay)
            continue

        if resp is not None and resp.status in (403, 429):
            delay = min(60.0, 2.8 * (2 ** min(i, 6)) + random.random() * 0.45)
            logger.warning(
                "%s: HTTP %s — backoff retry %s/%s (sleep %.2fs)",
                log_label,
                resp.status,
                i + 1,
                max_total,
                delay,
            )
            await asyncio.sleep(delay)
            if i >= max_total - 1:
                raise RuntimeError(
                    f"{log_label}: navigation blocked (HTTP {resp.status}) after {max_total} attempt(s)"
                )
            continue
        return

    if last_exc is not None:
        raise last_exc
    raise RuntimeError("goto failed with no exception captured")


async def _warmup_settle_after_base_goto(
    page: Any,
    pred: Any,
    *,
    dealer_name: str,
    max_idle_sec: float,
    scroll_sec: float,
) -> None:
    """
    Race inventory-shaped JSON ``response`` vs a max idle cap, then best-effort DOM signals,
    mid-page scroll, and optional scroll sleep.
    """
    signal_ms = _warmup_signal_timeout_ms()
    max_idle_sec = max(0.0, max_idle_sec)
    scroll_sec = max(0.0, scroll_sec)

    async def arm_response() -> None:
        if signal_ms <= 0:
            return
        try:
            await page.wait_for_event("response", pred, timeout=signal_ms)
        except BaseException:
            pass

    async def arm_cap() -> None:
        if max_idle_sec > 0:
            await asyncio.sleep(max_idle_sec)

    if max_idle_sec > 0 and signal_ms > 0:
        t1 = asyncio.create_task(arm_response())
        t2 = asyncio.create_task(arm_cap())
        done, pending = await asyncio.wait({t1, t2}, return_when=asyncio.FIRST_COMPLETED)
        for p in pending:
            p.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await p
        for d in done:
            with contextlib.suppress(asyncio.CancelledError):
                await d
    elif signal_ms > 0:
        await arm_response()
    elif max_idle_sec > 0:
        await arm_cap()

    dom_csv = (os.environ.get("SCANNER_WARMUP_DOM_SELECTORS") or "").strip()
    default_dom = (
        "[data-vehicle],[data-vin],[data-vehicle-id],.vehicle-card,.inventory-vehicle,"
        "a[href*='/inventory/']"
    )
    selectors = [s.strip() for s in (dom_csv or default_dom).split(",") if s.strip()][:8]
    for sel in selectors:
        try:
            await page.locator(sel).first.wait_for(state="attached", timeout=1200)
            logger.info("Warmup: %s — DOM signal matched selector %s", dealer_name, sel[:72])
            break
        except BaseException:
            continue

    await page.evaluate("window.scrollTo(0, document.body.scrollHeight / 2)")
    if scroll_sec:
        await asyncio.sleep(scroll_sec)


def _truncate_url(u: str, max_len: int = 120) -> str:
    s = (u or "").strip()
    if len(s) <= max_len:
        return s
    return s[: max_len - 3] + "..."


class _DealerResponseErrorBudget:
    """DEBUG logs for response-handler failures without per-response spam."""

    def __init__(self, *, first_n: int = 12, then_every: int = 40) -> None:
        self._first_n = max(0, first_n)
        self._then_every = max(1, then_every)
        self._n = 0

    def should_log(self) -> bool:
        self._n += 1
        if self._n <= self._first_n:
            return True
        return (self._n - self._first_n) % self._then_every == 1


def load_manifest():
    with open(MANIFEST_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def filter_manifest_by_dealer_id(dealers: list, dealer_id: str) -> list:
    """Return rows whose dealer_id matches (exact string after strip)."""
    want = (dealer_id or "").strip()
    if not want:
        return []
    return [d for d in dealers if (d.get("dealer_id") or "").strip() == want]


def _is_valid_vehicle_list(body) -> bool:
    """True if JSON contains a list with at least 3 dicts that have vin/VIN (same as parser logic)."""
    return find_vehicle_list(body) is not None


def _playwright_inventory_json_predicate(dealer_base_url: str):
    """Sync predicate for ``page.wait_for_event("response", ...)`` — URL gate + JSON content-type only."""

    def _pred(resp: Any) -> bool:
        try:
            ct = (resp.headers.get("content-type") or "").lower()
            if "application/json" not in ct:
                return False
            u = str(getattr(resp, "url", "") or "")
            return intercept_url_allowed(u, dealer_base_url)
        except Exception:
            return False

    return _pred


def _is_playwright_shutdown_error(exc: BaseException) -> bool:
    """True when the browser was closed mid-operation (e.g. Ctrl+C / task cancellation)."""
    name = type(exc).__name__
    msg = str(exc).lower()
    if name == "TargetClosedError":
        return True
    if "targetclosed" in name.lower():
        return True
    if "target page, context or browser has been closed" in msg:
        return True
    if "browser has been closed" in msg:
        return True
    if "context or browser has been closed" in msg:
        return True
    return False


async def _safe_close_context(context) -> None:
    if context is not None:
        try:
            await context.close()
        except BaseException:
            pass


def _max_dealer_concurrency() -> int:
    raw = (os.environ.get("SCANNER_MAX_DEALER_CONCURRENCY") or "2").strip()
    try:
        return max(1, int(raw))
    except ValueError:
        return 2


async def _upsert_vehicles_serialized(write_lock: asyncio.Lock, vehicles: list[dict]) -> int:
    """One SQLite writer at a time; run blocking upsert in a thread."""
    if not vehicles:
        return 0
    async with write_lock:
        return await asyncio.to_thread(upsert_vehicles, vehicles)


def _apply_gallery_vision_filter_to_vehicles(vehicles: list[dict[str, Any]]) -> dict[str, int]:
    """
    Mutate each vehicle's ``gallery`` and ``image_url`` to drop images LLaVA classifies as not
    vehicle exterior, interior, or window sticker. Requires Ollama (``OLLAMA_HOST``) and a vision
    model (default ``llava:13b`` via ``OLLAMA_VISION_MODEL``).
    """
    from backend.vision import ollama_llava as _llv

    raw = (os.environ.get("SCANNER_GALLERY_VISION_MAX_WORKERS") or "1").strip()
    try:
        max_w = max(1, int(raw))
    except ValueError:
        max_w = 1
    total_before = 0
    total_after = 0
    for v in vehicles:
        hero = v.get("image_url")
        raw_g = v.get("gallery")
        g_list = raw_g if isinstance(raw_g, list) else []
        urls: list[str] = []
        if isinstance(hero, str) and hero.strip().lower().startswith("http"):
            urls.append(hero.strip())
        for u in g_list:
            if isinstance(u, str) and u.strip().lower().startswith("http"):
                urls.append(u.strip())
        seen_u: set[str] = set()
        n_before = 0
        for u in urls:
            if u not in seen_u:
                seen_u.add(u)
                n_before += 1
        total_before += n_before
        filtered = _llv.filter_gallery_urls_for_vehicle_listing(urls, max_workers=max_w)
        seen_f: set[str] = set()
        n_after = 0
        for u in filtered:
            if u not in seen_f:
                seen_f.add(u)
                n_after += 1
        total_after += n_after
        v["gallery"] = filtered
        v["image_url"] = filtered[0] if filtered else ""
    dropped = max(0, total_before - total_after)
    return {
        "gallery_vision_unique_before": total_before,
        "gallery_vision_unique_after": total_after,
        "gallery_vision_unique_dropped": dropped,
    }


def _apply_monroney_vision_to_vehicles(vehicles: list[dict[str, Any]]) -> dict[str, Any]:
    """
    LLaVA read of window-sticker images (URL heuristics) plus VDP ``_monroney_page_texts`` snippets.
    Mutates vehicles; pops ``_monroney_page_texts``. Requires Ollama (``OLLAMA_VISION_MODEL``).
    """
    from backend.vision import ollama_llava as ol
    from backend.vision.monroney_merge import merge_monroney_parsed_into_vehicle

    stats: dict[str, Any] = {"rows_touched": 0, "sticker_image_calls": 0, "page_text_calls": 0}
    for v in vehicles:
        touched = False
        texts = v.pop("_monroney_page_texts", None)
        if isinstance(texts, list) and texts:
            tp = ol.analyze_monroney_from_page_texts(texts)
            stats["page_text_calls"] += 1
            if isinstance(tp, dict) and merge_monroney_parsed_into_vehicle(v, tp):
                touched = True
        hero = v.get("image_url")
        g = v.get("gallery") if isinstance(v.get("gallery"), list) else []
        urls_dedup: list[str] = []
        seen_u: set[str] = set()
        seq: list[str] = []
        if isinstance(hero, str) and hero.strip().lower().startswith("http"):
            seq.append(hero.strip())
        for x in g:
            if isinstance(x, str):
                seq.append(x.strip())
        for u in seq:
            if not u.lower().startswith("http") or u in seen_u:
                continue
            seen_u.add(u)
            urls_dedup.append(u)
        sticker_urls = [u for u in urls_dedup if ol.is_probable_sticker_image_url(u)][:2]
        merged: dict[str, Any] = {}
        for su in sticker_urls:
            sp = ol.analyze_monroney_sticker_from_image_url(su)
            stats["sticker_image_calls"] += 1
            if not isinstance(sp, dict):
                continue
            for k, val in sp.items():
                if k in ("vision_model", "source"):
                    continue
                if isinstance(val, list) and val:
                    cur = merged.setdefault(k, [])
                    if not isinstance(cur, list):
                        cur = []
                        merged[k] = cur
                    seenn = {str(x).strip().lower() for x in cur}
                    for it in val:
                        ss = str(it).strip()
                        if ss and ss.lower() not in seenn:
                            cur.append(ss)
                            seenn.add(ss.lower())
                elif val not in (None, "", []):
                    if merged.get(k) in (None, "", []):
                        merged[k] = val
        if merged and merge_monroney_parsed_into_vehicle(v, merged):
            touched = True
        if touched:
            stats["rows_touched"] += 1
    return stats


def _emit_dealer_run_summary(result: dict[str, Any]) -> None:
    """One parseable INFO line per dealer (JSON), capped ~2KB for log pipelines."""
    payload: dict[str, Any] = {
        "dealer_id": result.get("dealer_id"),
        "intercept_count": result.get("intercept_count"),
        "filtered_count": result.get("filtered_count"),
        "inventory_rows": result.get("inventory_rows"),
        "deduped_rows": result.get("deduped_rows"),
        "vdps_visited": result.get("vdps_visited"),
        "gallery_bins": result.get("gallery_bins"),
        "gallery_vision": result.get("gallery_vision"),
        "monroney_vision": result.get("monroney_vision"),
        "seconds": round(float(result.get("seconds") or 0.0), 2),
        "upserted": result.get("upserted"),
    }
    err = result.get("error")
    if err:
        payload["error"] = str(err)[:400]
    rec = result.get("reconcile")
    if isinstance(rec, dict):
        payload["reconcile"] = {
            "ran": rec.get("ran"),
            "scraped_candidates": rec.get("scraped_candidates"),
            "marked_inactive": rec.get("marked_inactive"),
            "skipped_reason": rec.get("skipped_reason"),
        }
    line = json.dumps(payload, separators=(",", ":"), default=str, ensure_ascii=False)
    if len(line) > 2048:
        line = line[:2045] + "..."
    logger.info("dealer_run_summary %s", line)


async def run_dealer(
    browser: Any,
    dealer: dict,
    write_lock: asyncio.Lock,
    *,
    gallery_vision_filter: bool = True,
    monroney_vision: bool = True,
) -> dict[str, Any]:
    name = dealer.get("name", "")
    url = dealer.get("url", "").rstrip("/")
    provider = dealer.get("provider", "dealer_dot_com")
    dealer_id = dealer.get("dealer_id", "")
    result: dict[str, Any] = {
        "dealer_id": dealer_id,
        "dealer_name": name,
        "upserted": 0,
        "inventory_rows": 0,
        "deduped_rows": 0,
        "vdps_visited": 0,
        "vehicles_vdp_enriched": 0,
        "gallery_vdp_urls_added": 0,
        "intercept_count": 0,
        "filtered_count": 0,
        "gallery_bins": None,
        "seconds": 0.0,
        "error": None,
        "vins": [],
        "reconcile": None,
        "gallery_vision": None,
        "monroney_vision": None,
    }
    t0 = time.perf_counter()
    if not url or not dealer_id:
        logger.warning("Skipping dealer missing url or dealer_id: %s", dealer)
        result["seconds"] = time.perf_counter() - t0
        _emit_dealer_run_summary(result)
        return result

    logger.info("Dealer start: %s", name)
    logger.info("Warmup: %s — navigating to base URL", name)
    context = None
    intercept_records: list[tuple[str, Any]] = []
    body_parse_cache: dict[int, list[dict[str, Any]]] = {}
    gate_stats = {"url_denied": 0}
    found_data = {"value": False}
    resp_err_budget = _DealerResponseErrorBudget()

    async def handle_response(response):
        try:
            ct = response.headers.get("content-type") or ""
            if "application/json" not in ct:
                return
            body = await response.json()
            if not _is_valid_vehicle_list(body):
                return
            rurl = str(getattr(response, "url", "") or "")
            if not intercept_url_allowed(rurl, url):
                gate_stats["url_denied"] += 1
                logger.debug(
                    "Intercept URL denied [%s]: %s",
                    name,
                    _truncate_url(rurl),
                )
                return
            intercept_records.append((rurl, body))
            found_data["value"] = True
            logger.info(
                "Intercepting: %s — got valid vehicle list (%s)",
                name,
                _truncate_url(rurl, 80),
            )
        except Exception as e:
            if resp_err_budget.should_log():
                rurl = _truncate_url(str(getattr(response, "url", "") or ""))
                logger.debug(
                    "Intercept handler failure [%s] url=%s type=%s msg=%s",
                    name,
                    rurl,
                    type(e).__name__,
                    str(e)[:200],
                )

    try:
        ctx_opts: dict[str, Any] = {"viewport": {"width": 1920, "height": 1080}}
        _ua = (os.environ.get("SCANNER_USER_AGENT") or "").strip()
        if _ua:
            ctx_opts["user_agent"] = _ua
        context = await browser.new_context(**ctx_opts)
        page = await context.new_page()
        warm_pred = _playwright_inventory_json_predicate(url)
        await _goto_with_retries(page, url, log_label=f"Warmup:{name}", timeout_ms=30000)
        w_post, w_scroll = _warmup_delays()
        await _warmup_settle_after_base_goto(
            page,
            warm_pred,
            dealer_name=name,
            max_idle_sec=w_post,
            scroll_sec=w_scroll,
        )
        logger.info("Warmup: %s — done (signal race cap=%.1fs + scroll %.1fs)", name, w_post, w_scroll)

        page.on("response", handle_response)
        # Accumulate intercepted JSON across all three category URLs
        intercept_records.clear()
        body_parse_cache.clear()
        gate_stats["url_denied"] = 0

        def _vehicles_for_body(body: Any) -> list[dict[str, Any]]:
            bid = id(body)
            cached = body_parse_cache.get(bid)
            if cached is not None:
                return cached
            vehicles = list(
                parse(provider, body, base_url=url, dealer_id=dealer_id, dealer_name=name, dealer_url=url)
            )
            for v in vehicles:
                v.setdefault("dealer_name", name)
                v.setdefault("dealer_url", url)
            body_parse_cache[bid] = vehicles
            return vehicles

        def _vin_count_from_intercepts() -> tuple[int, dict[str, dict[str, Any]]]:
            by_vin: dict[str, dict[str, Any]] = {}
            for _ru, body in intercept_records:
                for v in _vehicles_for_body(body):
                    vin = (v.get("vin") or "").strip()
                    if vin:
                        by_vin[vin] = v
            return len(by_vin), by_vin

        inv_wait_ms = _inventory_wait_ms()
        pag_wait_ms = _pagination_response_wait_ms()
        pred = _playwright_inventory_json_predicate(url)

        for inv_path in INVENTORY_PATHS:
            found_data["value"] = False
            full_url = url + inv_path
            logger.info("Navigating: %s — %s", name, full_url)
            try:
                await _goto_with_retries(page, full_url, log_label=f"Nav:{name}", timeout_ms=20000)
                try:
                    await page.wait_for_event("response", pred, timeout=inv_wait_ms)
                except Exception:
                    pass
                idle_cap = 3 if found_data["value"] else 18
                for _ in range(idle_cap):
                    await asyncio.sleep(1)
                    if found_data["value"]:
                        logger.info("Intercepting: %s — stopping wait (data found)", name)
                        break
                await asyncio.sleep(1)

                # Multi-page scrolling: trigger infinite scroll
                logger.info("Scrolling: %s — 5x scroll to bottom (2s between)", name)
                for _ in range(5):
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await asyncio.sleep(2)

                # API pagination: click Next/Load More until we have totalCount
                for _ in range(MAX_PAGINATION_CLICKS):
                    total_count = pick_total_count_from_intercepts(intercept_records, url)
                    current_count, _ = _vin_count_from_intercepts()
                    if total_count is not None and total_count > current_count:
                        clicked = False
                        for sel in NEXT_SELECTORS:
                            try:
                                loc = page.locator(sel)
                                if await loc.count() > 0:
                                    first = loc.first
                                    if await first.is_visible():
                                        await first.click()
                                        logger.info("Pagination: %s — clicked %s", name, sel)
                                        try:
                                            await page.wait_for_event("response", pred, timeout=pag_wait_ms)
                                        except Exception:
                                            pass
                                        await asyncio.sleep(0.35)
                                        clicked = True
                                        break
                            except Exception:
                                continue
                        if not clicked:
                            break
                    else:
                        break
            except Exception as e:
                logger.warning("Navigating: %s — %s: %s", name, full_url, e)
                continue

        # Parse all accumulated payloads and merge by VIN (upsert_vehicles dedupes).
        # Parser sets carfax_url from explicit feed links first, then vhr.carfax.com. VDP capture
        # overwrites weak links with anchor/data URLs scraped from the live detail page when better.
        all_vehicles: list[dict[str, Any]] = []
        for _resp_url, body in intercept_records:
            all_vehicles.extend(_vehicles_for_body(body))

        result["inventory_rows"] = len(all_vehicles)

        if not all_vehicles:
            logger.info(
                "Extraction backup: %s — no vehicles from %d JSON intercept(s); trying page.content() HTML",
                name,
                len(intercept_records),
            )
            html = await page.content()
            all_vehicles = list(
                parse(provider, html, base_url=url, dealer_id=dealer_id, dealer_name=name, dealer_url=url)
            )
            for v in all_vehicles:
                v.setdefault("dealer_name", name)
                v.setdefault("dealer_url", url)
            result["inventory_rows"] = len(all_vehicles)
            if not all_vehicles:
                nd = parse_next_data_json_from_html(html or "")
                if nd is None:
                    nd = await fetch_next_data_json_from_page(page)
                if nd is not None:
                    all_vehicles = list(
                        parse(provider, nd, base_url=url, dealer_id=dealer_id, dealer_name=name, dealer_url=url)
                    )
                    for v in all_vehicles:
                        v.setdefault("dealer_name", name)
                        v.setdefault("dealer_url", url)
                    result["inventory_rows"] = len(all_vehicles)
                if all_vehicles:
                    logger.info(
                        "HTML fallback: %s — recovered %d vehicle row(s) from __NEXT_DATA__",
                        name,
                        len(all_vehicles),
                    )
                else:
                    logger.info(
                        "HTML fallback: %s — 0 vehicles (SPA shell or unsupported HTML; %d chars)",
                        name,
                        len(html or ""),
                    )
            else:
                logger.info("HTML fallback: %s — recovered %d vehicle row(s) from HTML", name, len(all_vehicles))

        if all_vehicles:
            # One row per VIN for downstream VDP enrichment (listing payloads may repeat VINs).
            by_vin: dict[str, dict] = {}
            for v in all_vehicles:
                vin = (v.get("vin") or "").strip()
                if vin:
                    if vin in by_vin:
                        merge_inventory_rows_same_vin(by_vin[vin], v)
                    else:
                        by_vin[vin] = v
            all_vehicles = list(by_vin.values())
            result["deduped_rows"] = len(all_vehicles)
            result["vins"] = sorted({(v.get("vin") or "").strip() for v in all_vehicles if (v.get("vin") or "").strip()})
            _log_gallery_bins(name, "after_inventory_merge", all_vehicles)

            vdp_stats: dict[str, Any] = {}
            try:
                vdp_stats = await enrich_vehicles_vdp(page, all_vehicles, name, dealer_id=dealer_id)
            except Exception as e:
                logger.warning("VDP enrichment failed for %s (continuing with listing data only): %s", name, e)
            result["vdps_visited"] = int(vdp_stats.get("vdps_visited") or 0)
            result["vehicles_vdp_enriched"] = int(vdp_stats.get("vehicles_enriched") or 0)
            result["gallery_vdp_urls_added"] = int(vdp_stats.get("gallery_vdp_urls_added") or 0)
            _log_gallery_bins(name, "after_vdp", all_vehicles)
            result["gallery_bins"] = gallery_https_bin_histogram(all_vehicles)
            if vdp_stats.get("gallery_phase_bins"):
                logger.info("Gallery phase bins [%s]: %s", name, vdp_stats.get("gallery_phase_bins"))

            # Ensure gallery is always a list for DB (stored as json.dumps(gallery) in database.py)
            for v in all_vehicles:
                g = v.get("gallery")
                if not isinstance(g, list):
                    g = []
                hero = v.get("image_url")
                if (
                    not any(isinstance(x, str) and x.strip().lower().startswith("http") for x in g)
                    and isinstance(hero, str)
                    and hero.strip().lower().startswith("http")
                ):
                    g = [hero.strip()]
                v["gallery"] = g
            if gallery_vision_filter:
                try:
                    gv = await asyncio.to_thread(_apply_gallery_vision_filter_to_vehicles, all_vehicles)
                    result["gallery_vision"] = gv
                    logger.info(
                        "Gallery vision filter [%s]: dropped %s of %s unique HTTPS image URLs (LLaVA)",
                        name,
                        gv.get("gallery_vision_unique_dropped"),
                        gv.get("gallery_vision_unique_before"),
                    )
                except Exception as e:
                    logger.warning(
                        "Gallery vision filter failed for %s (saving unfiltered images): %s",
                        name,
                        e,
                    )
                    result["gallery_vision"] = {"error": str(e)[:200]}
            if monroney_vision:
                try:
                    mv = await asyncio.to_thread(_apply_monroney_vision_to_vehicles, all_vehicles)
                    result["monroney_vision"] = mv
                    logger.info(
                        "Monroney vision [%s]: rows_touched=%s sticker_image_calls=%s page_text_calls=%s",
                        name,
                        mv.get("rows_touched"),
                        mv.get("sticker_image_calls"),
                        mv.get("page_text_calls"),
                    )
                except Exception as e:
                    logger.warning("Monroney vision failed for %s: %s", name, e)
                    result["monroney_vision"] = {"error": str(e)[:200]}
            count = await _upsert_vehicles_serialized(write_lock, all_vehicles)
            result["upserted"] = count
            try:
                from backend.scanner_inventory_reconcile import (
                    normalized_vin_set_from_vehicles,
                    reconcile_dealer_inventory_after_scan,
                )

                scraped_norm = normalized_vin_set_from_vehicles(all_vehicles)
                result["reconcile"] = await asyncio.to_thread(
                    reconcile_dealer_inventory_after_scan,
                    dealer_id,
                    url,
                    scraped_norm,
                    result,
                )
            except Exception as e:
                logger.warning(
                    "Inventory reconcile failed for %s (inventory already saved): %s",
                    name,
                    e,
                )
                result["reconcile"] = {
                    "ran": False,
                    "scraped_candidates": 0,
                    "marked_inactive": 0,
                    "skipped_reason": "exception",
                    "error": str(e)[:200],
                }
            result["seconds"] = time.perf_counter() - t0
            logger.info(
                "Parsing: %s — extracted %d vehicles (deduped by VIN), upserted %d",
                name,
                len(all_vehicles),
                count,
            )
            logger.info(
                "Dealer complete: %s (%d inventory rows, %d deduped, %d VDP visited, %d VDP-enriched, "
                "%d upserted, %.1fs)",
                name,
                result["inventory_rows"],
                result["deduped_rows"],
                result["vdps_visited"],
                result["vehicles_vdp_enriched"],
                count,
                result["seconds"],
            )
            return result
        # No path returned vehicles
        logger.warning("Parsing: %s — no vehicles from any inventory path", name)
        DEBUG_DIR.mkdir(parents=True, exist_ok=True)
        screenshot_path = DEBUG_DIR / f"fail_{dealer_id}.png"
        await page.screenshot(path=str(screenshot_path))
        logger.info("Debug: saved screenshot to %s", screenshot_path)
        result["seconds"] = time.perf_counter() - t0
        logger.info(
            "Dealer complete: %s (%d inventory rows, %d deduped, %d VDP visited, %d VDP-enriched, %d upserted, %.1fs)",
            name,
            result["inventory_rows"],
            result["deduped_rows"],
            result["vdps_visited"],
            result["vehicles_vdp_enriched"],
            result["upserted"],
            result["seconds"],
        )
        return result
    except asyncio.CancelledError:
        raise
    except Exception as e:
        result["error"] = str(e)
        result["seconds"] = time.perf_counter() - t0
        if _is_playwright_shutdown_error(e):
            return result
        logger.exception("Dealer %s failed: %s", dealer_id, e)
        logger.info(
            "Dealer complete: %s (%d inventory rows, %d deduped, %d VDP visited, %d VDP-enriched, %d upserted, %.1fs) [error]",
            name,
            result["inventory_rows"],
            result["deduped_rows"],
            result["vdps_visited"],
            result["vehicles_vdp_enriched"],
            result["upserted"],
            result["seconds"],
        )
        return result
    finally:
        result["intercept_count"] = len(intercept_records)
        result["filtered_count"] = gate_stats["url_denied"]
        if result.get("gallery_bins") is None:
            result["gallery_bins"] = {}
        result["seconds"] = time.perf_counter() - t0
        _emit_dealer_run_summary(result)
        await _safe_close_context(context)


async def main(
    dealers: list | None = None,
    *,
    post_repair: bool = True,
    post_listing_description: bool = True,
    post_interior_vision: bool = True,
    post_enrich: bool = False,
    post_enrich_vision_only: bool = False,
    post_kbb: bool = False,
    enrichment_max_workers: int | None = None,
    gallery_vision_filter: bool = True,
    monroney_vision: bool = True,
):
    if dealers is None:
        dealers = load_manifest()
    logger.info("Loading manifest: %s", MANIFEST_PATH)
    if not dealers:
        logger.error("No dealers to scan (manifest empty or filter matched nothing).")
        return
    logger.info("Found %d dealer(s) to run", len(dealers))

    dealer_conc = _max_dealer_concurrency()
    vdp_conc = _max_vdp_concurrency()
    logger.info("Scanner: dealer concurrency = %d", dealer_conc)
    logger.info("Scanner: VDP concurrency = %d", vdp_conc)

    bmw_enhanced_dealers = enhance_scraping_for_bmw_dealerships(dealers)
    write_lock = asyncio.Lock()
    scan_t0 = time.perf_counter()
    total_upserted = 0
    sem = asyncio.Semaphore(dealer_conc)
    outcomes: list[Any] = []

    async def run_dealers_with_browser(p) -> list[Any]:
        browser = await p.chromium.launch(headless=True)

        async def one_dealer(dealer: dict) -> dict[str, Any]:
            did = dealer.get("dealer_id", "")
            if dealer.get("optimize_for") == "bmw":
                logger.info("Applying BMW-specific optimization for %s", dealer.get("name"))
            try:
                return await run_dealer(
                    browser,
                    dealer,
                    write_lock,
                    gallery_vision_filter=gallery_vision_filter,
                    monroney_vision=monroney_vision,
                )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.exception("Dealer %s failed: %s", did, e)
                DEBUG_DIR.mkdir(parents=True, exist_ok=True)
                try:
                    pg = await browser.new_page()
                    try:
                        await pg.goto(dealer.get("url", "about:blank"), timeout=10000)
                        await pg.screenshot(path=str(DEBUG_DIR / f"fail_{did or 'unknown'}.png"))
                    finally:
                        await pg.close()
                except Exception:
                    pass
                return {
                    "dealer_id": did,
                    "dealer_name": dealer.get("name", ""),
                    "upserted": 0,
                    "error": str(e),
                }

        async def bounded(dealer: dict) -> dict[str, Any]:
            async with sem:
                return await one_dealer(dealer)

        try:
            return await asyncio.gather(
                *[bounded(d) for d in bmw_enhanced_dealers],
                return_exceptions=True,
            )
        finally:
            await browser.close()

    try:
        from playwright_stealth import Stealth
        from playwright.async_api import async_playwright

        async with Stealth().use_async(async_playwright()) as p:
            outcomes = await run_dealers_with_browser(p)
    except ImportError:
        logger.warning("playwright_stealth not found, using plain playwright")
        from playwright.async_api import async_playwright

        async with async_playwright() as p:
            outcomes = await run_dealers_with_browser(p)

    for o in outcomes:
        if isinstance(o, BaseException):
            logger.error("Dealer task ended with exception: %s", o)
            continue
        if isinstance(o, dict):
            total_upserted += int(o.get("upserted") or 0)

    elapsed = time.perf_counter() - scan_t0
    logger.info(
        "Scanner finished — total runtime %.1fs, dealer_concurrency=%d, vdp_concurrency=%d, total vehicles upserted=%d",
        elapsed,
        dealer_conc,
        vdp_conc,
        total_upserted,
    )

    scanned_vins = aggregate_vins_from_dealer_results(outcomes)
    try:
        from datetime import datetime, timezone

        from backend.db.inventory_db import record_scan_outcomes

        record_scan_outcomes(outcomes, finished_at=datetime.now(timezone.utc).isoformat())
    except Exception:
        logger.exception("record_scan_outcomes failed (inventory.db scan_runs)")

    if (
        post_repair
        or post_listing_description
        or post_interior_vision
        or post_enrich
        or post_enrich_vision_only
        or post_kbb
    ):
        try:
            post_summary = run_post_scan(
                scanned_vins,
                post_repair=post_repair,
                post_listing_description=post_listing_description,
                post_interior_vision=post_interior_vision,
                post_enrich=post_enrich,
                post_enrich_vision_only=post_enrich_vision_only,
                post_kbb=post_kbb,
                enrichment_max_workers=enrichment_max_workers,
            )
            logger.info("Post-scan summary: %s", json.dumps(post_summary, default=str)[:1800])
        except Exception:
            logger.exception("Post-scan pipeline failed (inventory already saved)")

    # --- model_specs dictionary correction (full pass) ---
    # Per-VIN corrections already run inside upsert_vehicles; this final pass
    # catches any rows that were updated by post_repair after the initial upsert.
    try:
        from backend.database import apply_model_specs_corrections
        corrected = await asyncio.to_thread(apply_model_specs_corrections)
        if corrected:
            logger.info("model_specs final pass: %d rows corrected", corrected)
    except Exception:
        logger.exception("model_specs final correction pass failed")

    # --- Resync incomplete_listings.db ---
    try:
        from backend.db.incomplete_listings_db import fast_rebuild_incomplete_listings_index
        n_incomplete = await asyncio.to_thread(fast_rebuild_incomplete_listings_index)
        logger.info("incomplete_listings resynced: %d incomplete listings", n_incomplete)
    except Exception:
        logger.exception("incomplete_listings resync failed")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        description="Manifest-driven dealership inventory scanner (Playwright + stealth)."
    )
    ap.add_argument(
        "--dealer-id",
        metavar="ID",
        default=None,
        help="Scan only this dealer_id from dealers.json (e.g. from the /dev console).",
    )
    ap.add_argument(
        "--no-post-repair",
        action="store_true",
        help="Skip SQLite repair for VINs touched in this run (see SCANNER_POST_REPAIR).",
    )
    ap.add_argument(
        "--no-post-listing-description",
        action="store_true",
        help="Skip parsing each listing description into packages JSON (see SCANNER_POST_LISTING_DESCRIPTION).",
    )
    ap.add_argument(
        "--post-enrich",
        action="store_true",
        help="After repair + listing parse, run InventoryEnricher for scanned rows (requires indexed EPA catalog).",
    )
    ap.add_argument(
        "--post-enrich-vision-only",
        action="store_true",
        help="After repair + listing parse, vision-only enrichment for scanned rows (Ollama; catalog not required).",
    )
    ap.add_argument(
        "--no-post-interior-vision",
        action="store_true",
        help="Skip post-scan Ollama LLaVA interior/cabin pass (default is on; see SCANNER_POST_INTERIOR_VISION).",
    )
    ap.add_argument(
        "--no-gallery-vision-filter",
        action="store_true",
        help="Skip LLaVA gallery cleanup before upsert (default is on; see SCANNER_GALLERY_VISION_FILTER).",
    )
    ap.add_argument(
        "--no-monroney-vision",
        action="store_true",
        help="Skip LLaVA Monroney / sticker pass before upsert (default is on; see SCANNER_MONRONEY_VISION).",
    )
    ap.add_argument(
        "--enrichment-workers",
        type=int,
        default=None,
        help="Worker threads for post-scan enrichment (default: ENRICHMENT_MAX_WORKERS or enricher default).",
    )
    ap.add_argument(
        "--post-kbb",
        action="store_true",
        help="After scan, refresh KBB IDWS values for touched VINs (needs KBB_API_KEY; see SCANNER_POST_KBB).",
    )
    args = ap.parse_args()
    to_run = load_manifest()
    if args.dealer_id:
        to_run = filter_manifest_by_dealer_id(to_run, args.dealer_id)
        if not to_run:
            logger.error(
                "No dealer with dealer_id %r in %s — save the dealer in /dev first.",
                args.dealer_id.strip(),
                MANIFEST_PATH,
            )
            sys.exit(1)

    do_repair = not args.no_post_repair and post_repair_env_enabled()
    do_listing = not args.no_post_listing_description and post_listing_description_env_enabled()
    do_vision = bool(args.post_enrich_vision_only) or post_enrich_vision_env_enabled()
    do_enrich = bool(args.post_enrich) or post_enrich_env_enabled()
    do_interior_vision = (not args.no_post_interior_vision) and post_interior_vision_env_enabled()
    do_gallery_vision = (not args.no_gallery_vision_filter) and gallery_vision_filter_env_enabled()
    do_monroney = (not args.no_monroney_vision) and monroney_vision_env_enabled()
    do_kbb = bool(args.post_kbb) or post_kbb_env_enabled()

    try:
        asyncio.run(
            main(
                to_run,
                post_repair=do_repair,
                post_listing_description=do_listing,
                post_interior_vision=do_interior_vision,
                post_enrich=do_enrich and not do_vision,
                post_enrich_vision_only=do_vision,
                post_kbb=do_kbb,
                enrichment_max_workers=args.enrichment_workers,
                gallery_vision_filter=do_gallery_vision,
                monroney_vision=do_monroney,
            )
        )
    except KeyboardInterrupt:
        logger.info("Scanner gracefully stopped by user.")
        sys.exit(0)
    except asyncio.CancelledError:
        logger.info("Scanner gracefully stopped by user.")
        sys.exit(0)
