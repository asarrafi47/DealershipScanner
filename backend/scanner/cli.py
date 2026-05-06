#!/usr/bin/env python3
"""
Manifest-driven dealership inventory scanner. Uses playwright + stealth,
session warmup to avoid 403, network interception for JSON, HTML fallback, and
optional ``__NEXT_DATA__`` (Next.js) extraction when the raw HTML shell has no rows.

Gallery / images (inventory + VDP):
  SCANNER_INVENTORY_GALLERY_MAX — max URLs stored per vehicle after dedupe (default: 48).
  Inventory parsers deep-walk intercepted JSON for nested media keys (see backend/parsers/base.py).

Optional VDP (detail page) enrichment (see scanner_vdp):
  SCANNER_VDP_EP_MAX — max unique listing URLs for EP / gallery harvest per dealer (default: 10; 0 = skip this phase).
  SCANNER_VDP_PRICE_MAX — extra unique listing URLs only for rows still missing price after inventory JSON
    (default: 400; max 5000; 0 = skip). Mirrors Node ``scanner.js`` VDP price phase.
  SCANNER_VDP_GALLERY_MIN_HTTPS — rows with fewer than this many unique https gallery URLs get
    extra priority in the VDP visit queue when SCANNER_VDP_GALLERY_PRIORITY is enabled (default: 3).
  SCANNER_VDP_GALLERY_PRIORITY — truthy (default): thin-gallery vehicles compete for VDP slots.
  SCANNER_VDP_ROTATION — truthy (default): among equal-priority rows, rotate which VINs get VDP visits
    using SCANNER_VDP_ROTATION_SEED or the UTC date + dealer_id (long-tail coverage when EP_MAX is low).
  SCANNER_VDP_NAV_TIMEOUT_MS — navigation timeout per VDP (default: 32000).
  SCANNER_VDP_SETTLE_MS — wait after load for analytics/XHR (default: 2200).
  SCANNER_VDP_GALLERY_MAX_ROUNDS — max carousel-advance iterations per VDP (default: 80).
  SCANNER_VDP_GALLERY_IDLE_ROUNDS — stop gallery loop after this many rounds with no new HTTPS URL (default: 3).
  SCANNER_VDP_GALLERY_OPEN_LIGHTBOX — when truthy (default), try to open the dealer photo modal before DOM gallery harvest; set 0 to collect from the full page like before.
  SCANNER_VDP_DOWNLOAD_IMAGES — default on: persist gallery bytes under SCANNER_VDP_IMAGE_DOWNLOAD_DIR; set 0 to disable.
    (default ``vdp_images``) keyed by VIN or stock (``SCANNER_VDP_IMAGE_DOWNLOAD_KEY=vin|stock``); manifest +
    ``spec_source_json.vdp_gallery_local`` updated on the vehicle row (see ``backend.scanner.database`` upsert). Set to ``0``
    to disable.
  VDP price (JSON-LD / dataLayer / DOM) fills ``price`` only when the scraped row has no positive price;
    provenance is stored under ``spec_source_json.vdp_price``.
  SCANNER_GALLERY_MERGE_REPLACE_IF_BELOW — VDP merge replaces whole gallery when existing https
    count is below this (default: 3); otherwise VDP URLs extend listing gallery (see gallery_merge).
  SCANNER_MAX_DEALER_CONCURRENCY — parallel dealer scans (default: 3; set 1 for sequential).
  SCANNER_MAX_VDP_CONCURRENCY — parallel VDP page visits per dealer (default: 12; max 64).

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

Structured JSON interception (SPA / JSON APIs):
  Responses with JSON content-types are retained when the payload matches ``payload_qualifies_for_inventory_intercept``
  (classic ≥3 VIN rows, or inventory-shaped keys + ≥1 VIN row). When any path captures qualifying JSON,
  that path skips saving HTML (JSON is the source of truth for extraction).
  SCANNER_HYDRATION_TIMEOUT_MS — wait for ``[data-vin]`` or ``a[href*="/inventory/"]`` after each
    inventory navigation (default 30000 ms; set 0 to skip).

Infinite scroll:
  When no Next/Load-more control is visible, the scanner runs a scroll-to-bottom loop (stable-height stop)
  and briefly waits for qualifying JSON responses between scrolls.

Failure diagnostics:
  SCANNER_SHARD_COUNT — optional; split manifest across parallel workers (with SCANNER_SHARD_INDEX or
  Kubernetes JOB_COMPLETION_INDEX from an Indexed Job). Same manifest order on every worker.

  SCANNER_FAILURE_HAR — when truthy (default), zero-vehicle runs for ``dealer_dot_com`` / ``dealer_on``
  dealers write ``workspace/debug/fail_<dealer_id>_<epoch>.har`` (may contain cookies / auth headers;
    keep out of git — see SEC-066).

Warmup (first dealer base URL load):
  SCANNER_WARMUP_POST_GOTO_SEC — max seconds to wait after domcontentloaded (default 2; set 0 to skip
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

  Optional listing gap fill (after repair + listing parse): EPA/vPIC structured pass again, then
  fetch each vehicle's ``source_url`` (HTTP then Playwright if needed), parse **condition** only
  from that HTML; optional DuckDuckGo Instant Answer for residual transmission/drivetrain/fuel.
  ``--post-listing-gap-fill`` or ``SCANNER_POST_LISTING_GAP_FILL=1``; cap via
  ``SCANNER_POST_LISTING_GAP_FILL_MAX`` (default 400); disable DDG tier with ``LISTING_GAP_FILL_ALLOW_DDG=0``.

  Optional enrichment (EPA catalog in Postgres + Ollama vision), after repair + listing parse:

  --post-enrich / SCANNER_POST_ENRICH=1 — ``InventoryEnricher`` for scanned VINs only (needs catalog).

  --post-enrich-vision-only / SCANNER_POST_ENRICH_VISION=1 — vision pass only (catalog not required).

  Per-dealer inventory reconcile (after each successful upsert): soft-unlist DB rows for that
  dealer whose VIN is no longer in the scraped feed (``listing_active=0``). Disabled when
  SCANNER_RECONCILE=0. Skipped when ``deduped_rows`` < SCANNER_RECONCILE_MIN_ROWS (default 8) or
  when there are no valid normalized VINs in the scrape.

Vision passes (default **on** for ``python scanner.py``; requires local Ollama, default
``OLLAMA_VISION_MODEL=llava:13b``):

  **Gallery** — HTTPS gallery / hero URLs are de-junked (KBB, CARFAX, OEM, etc.); for proven dealer
  inventory image URLs, LLaVA can be **skipped** so the full VDP photo carousel is preserved
  (``SCANNER_GALLERY_VISION_PASSTHROUGH_TRUSTED_CDN=1`` default; set ``0`` to run vision on every URL).
  Remaining images are LLaVA-triaged. Opt out entirely: ``SCANNER_GALLERY_VISION_FILTER=0`` or
  ``--no-gallery-vision-filter``. ``SCANNER_GALLERY_VISION_MAX_WORKERS`` (default ``1``) sets parallel
  Ollama calls per vehicle. ``SCANNER_GALLERY_VISION_KEEP_UNFETCHABLE=1`` preserves URLs when the image
  could not be fetched (legacy: avoid dropping on CDN flakes; default is to **drop** broken/blank
  fetches from the list).

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
import signal
import sys
import time
from pathlib import Path
from typing import Any

try:
    from fake_useragent import UserAgent as _FakeUA
    _fake_ua_pool: _FakeUA | None = _FakeUA()
except Exception:
    _fake_ua_pool = None

def _get_rotating_ua() -> str:
    """Random modern Chrome UA; falls back to a static Chrome string."""
    if _fake_ua_pool is not None:
        try:
            return str(_fake_ua_pool.chrome)
        except Exception:
            pass
    return (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )

# Repo root (contains dealers.json, scanner.py shim); this file lives in backend/scanner/
ROOT = Path(__file__).resolve().parents[2]
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))

try:
    from backend.utils.project_env import load_project_dotenv

    load_project_dotenv()
except ImportError:
    pass

from backend.scanner.bmw_enhancer import (
    bmw_optimized_browsing,
    enhance_scraping_for_bmw_dealerships,
    is_bmw_dealership,
)
from backend.scanner.dealer_site_url import dealer_inventory_base_url
from backend.scanner.vdp import _max_vdp_concurrency, enrich_vehicles_vdp

from backend.db.inventory_pg import is_inventory_postgres
from backend.scanner.database import upsert_vehicles
from backend.parsers import parse
from backend.utils.gallery_merge import gallery_https_bin_histogram

from backend.scanner.scrapers.inventory_vin_merge import merge_inventory_rows_same_vin
from backend.scanner.scrapers.next_data_inventory import fetch_next_data_json_from_page, parse_next_data_json_from_html
from backend.scanner.scrapers.scanner_intercept_filter import (
    intercept_url_allowed,
    payload_qualifies_for_inventory_intercept,
    pick_total_count_from_intercepts,
    response_content_type_looks_json,
)
from backend.scanner.post_pipeline import (
    aggregate_vins_from_dealer_results,
    gallery_vision_filter_env_enabled,
    monroney_vision_env_enabled,
    post_dict_enrich_env_enabled,
    post_enrich_env_enabled,
    post_enrich_vision_env_enabled,
    post_interior_vision_env_enabled,
    post_listing_description_env_enabled,
    post_listing_gap_fill_env_enabled,
    post_kbb_env_enabled,
    post_repair_env_enabled,
    run_dictionary_enrich_for_vins,
    run_listing_gap_fill_stage,
    run_post_scan,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("scanner")


OEM_MANUFACTURER_DOMAINS = frozenset({
    "www.chevrolet.com",
    "www.chrysler.com",
    "www.ford.com",
    "www.gm.com",
    "www.nissan.com",
    "nissan-global.com",
    "www.honda.com",
    "automobiles.honda.com",
    "www.toyota.com",
    "www.hyundai.com",
    "www.kia.com",
    "www.bmw.com",
    "www.audi.com",
    "www.volkswagen.com",
    "www.mercedes.com",
    "www.jeep.com",
    "www.ram.com",
    "www.dodge.com",
})


def _is_oem_manufacturer_url(url: str) -> bool:
    """Check if URL points to an OEM manufacturer website rather than a dealership."""
    if not url:
        return False
    url_lower = url.lower()
    return any(domain in url_lower for domain in OEM_MANUFACTURER_DOMAINS)


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
WORKSPACE_DEBUG_DIR = ROOT / "workspace" / "debug"

SCANNER_HYDRATION_SELECTORS = '[data-vin], a[href*="/inventory/"]'

KNOWN_HAR_PROVIDERS = frozenset({"dealer_dot_com", "dealer_on"})
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


def _hydration_timeout_ms() -> int:
    raw = (os.environ.get("SCANNER_HYDRATION_TIMEOUT_MS") or "30000").strip()
    try:
        return max(0, int(raw))
    except ValueError:
        return 30000


def _failure_har_enabled() -> bool:
    return (os.environ.get("SCANNER_FAILURE_HAR") or "1").strip().lower() not in (
        "0",
        "false",
        "off",
        "no",
    )


def _infinite_scroll_max_rounds() -> int:
    raw = (os.environ.get("SCANNER_INFINITE_SCROLL_MAX_ROUNDS") or "18").strip()
    try:
        return max(0, int(raw))
    except ValueError:
        return 18


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
        post = float((os.environ.get("SCANNER_WARMUP_POST_GOTO_SEC") or "2").strip())
    except ValueError:
        post = 2.0
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


async def _await_inventory_hydration(
    page: Any,
    dealer_name: str,
    label: str,
    *,
    timeout_ms: int,
    json_pred: Any = None,
) -> None:
    """Wait for SPA/listing shell signals before relying on intercepts or HTML extraction."""
    if timeout_ms <= 0:
        return

    async def _dom_wait() -> str:
        await page.locator(SCANNER_HYDRATION_SELECTORS).first.wait_for(
            state="attached", timeout=timeout_ms
        )
        return "dom"

    async def _json_wait() -> str:
        if json_pred is None:
            await asyncio.sleep(timeout_ms / 1000)
            return "timeout"
        await page.wait_for_event("response", json_pred, timeout=timeout_ms)
        return "json"

    try:
        done, pending = await asyncio.wait(
            [asyncio.create_task(_dom_wait()), asyncio.create_task(_json_wait())],
            return_when=asyncio.FIRST_COMPLETED,
            timeout=timeout_ms / 1000 + 1,
        )
        signal_result = next((t.result() for t in done if not t.exception()), "timeout")
        for t in pending:
            t.cancel()
        logger.info("Hydration: %s%s — signal=%s", dealer_name, label, signal_result)
    except BaseException:
        logger.info(
            "Hydration: %s%s — no signal within %d ms (continuing)",
            dealer_name,
            label,
            timeout_ms,
        )


async def _any_next_control_visible(page: Any) -> bool:
    for sel in NEXT_SELECTORS:
        try:
            loc = page.locator(sel)
            if await loc.count() == 0:
                continue
            first = loc.first
            if await first.is_visible():
                return True
        except BaseException:
            continue
    return False


async def _infinite_scroll_lazy_batches(
    page: Any,
    dealer_name: str,
    path: str,
    pred: Any,
    *,
    pag_wait_ms: int,
) -> None:
    """Scroll to bottom repeatedly when listing relies on lazy XHR (no Next control)."""
    max_rounds = _infinite_scroll_max_rounds()
    if max_rounds <= 0:
        return
    stable = 0
    prev_h: float | int = -1
    rounds_ran = 0
    for i in range(max_rounds):
        try:
            h = await page.evaluate("() => (document.body ? document.body.scrollHeight : 0)")
        except BaseException:
            break
        try:
            await page.evaluate(
                "() => { const t = document.body ? document.body.scrollHeight : 0; window.scrollTo(0, t); }"
            )
        except BaseException:
            break
        try:
            await page.wait_for_event("response", pred, timeout=min(3500, max(300, pag_wait_ms)))
        except BaseException:
            pass
        await asyncio.sleep(1.25)
        rounds_ran = i + 1
        if h == prev_h:
            stable += 1
            if stable >= 4:
                break
        else:
            stable = 0
        prev_h = h
    logger.info(
        "Lazy-scroll: %s%s — rounds=%d (stable_stop=%s)",
        dealer_name,
        path,
        rounds_ran,
        stable >= 4,
    )


async def _capture_scanner_failure_har(browser: Any, base_url: str, dealer_id: str, dealer_name: str) -> str | None:
    """Second-pass browser context with HAR recording for operator diagnosis (treat file as sensitive)."""
    if not _failure_har_enabled():
        return None
    WORKSPACE_DEBUG_DIR.mkdir(parents=True, exist_ok=True)
    har_path = WORKSPACE_DEBUG_DIR / f"fail_{dealer_id}_{int(time.time())}.har"
    ctx_opts: dict[str, Any] = {
        "viewport": {"width": 1920, "height": 1080},
        "record_har_path": str(har_path),
        "record_har_url_filter": "**/*",
    }
    _ua = (os.environ.get("SCANNER_USER_AGENT") or "").strip() or _get_rotating_ua()
    ctx_opts["user_agent"] = _ua
    ctx = await browser.new_context(**ctx_opts)
    try:
        page = await ctx.new_page()
        warm_pred = _playwright_inventory_json_predicate(base_url)
        await _goto_with_retries(page, base_url, log_label=f"HAR-warmup:{dealer_name}", timeout_ms=30000)
        w_post, w_scroll = _warmup_delays()
        await _warmup_settle_after_base_goto(
            page,
            warm_pred,
            dealer_name=dealer_name,
            max_idle_sec=w_post,
            scroll_sec=w_scroll,
        )
        full_inv = base_url + INVENTORY_PATHS[0]
        await _goto_with_retries(page, full_inv, log_label=f"HAR-inv:{dealer_name}", timeout_ms=25000)
        pred = _playwright_inventory_json_predicate(base_url)
        await _await_inventory_hydration(
            page,
            dealer_name,
            f" HAR({INVENTORY_PATHS[0]})",
            timeout_ms=_hydration_timeout_ms(),
            json_pred=pred,
        )
        try:
            await page.wait_for_event("response", pred, timeout=_inventory_wait_ms())
        except BaseException:
            pass
        await asyncio.sleep(6.0)
        if not await _any_next_control_visible(page):
            await _infinite_scroll_lazy_batches(
                page,
                dealer_name,
                " HAR-scroll",
                pred,
                pag_wait_ms=_pagination_response_wait_ms(),
            )
    except BaseException as e:
        logger.warning("HAR capture failed [%s]: %s", dealer_name, e)
        return None
    finally:
        try:
            await ctx.close()
        except BaseException:
            pass
    try:
        if har_path.is_file() and har_path.stat().st_size > 0:
            logger.info("Debug: saved HAR trace to %s", har_path)
            return str(har_path)
    except OSError:
        pass
    return None


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

    SPA hydration (``SCANNER_HYDRATION_TIMEOUT_MS``) runs on inventory paths only — not here —
    so the homepage load stays bounded when it has no listing shell.
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


def filter_manifest_by_shard(
    dealers: list[Any], shard_index: int, shard_count: int
) -> list[Any]:
    """Split *dealers* into *shard_count* disjoint slices using stable list order.

    Dealer at manifest position ``i`` runs on shard ``i % shard_count``.
    Use the same manifest and shard parameters on every worker so partitions do not overlap.
    """
    if shard_count <= 1:
        return list(dealers)
    if shard_index < 0 or shard_index >= shard_count:
        raise ValueError(f"shard_index must satisfy 0 <= index < {shard_count}, got {shard_index}")
    return [d for i, d in enumerate(dealers) if i % shard_count == shard_index]


def _resolve_shard_cli_and_env(args: argparse.Namespace) -> tuple[int, int]:
    """Return ``(shard_index, shard_count)``. ``shard_count == 1`` means no shard filter."""

    cli_idx = getattr(args, "shard_index", None)
    cli_cnt = getattr(args, "shard_count", None)
    if cli_idx is not None or cli_cnt is not None:
        if cli_idx is None or cli_cnt is None:
            logger.error("--shard-index and --shard-count must be provided together.")
            sys.exit(2)
        if cli_cnt < 1:
            logger.error("--shard-count must be >= 1 (got %s).", cli_cnt)
            sys.exit(2)
        if cli_idx < 0 or cli_idx >= cli_cnt:
            logger.error(
                "--shard-index must satisfy 0 <= index < --shard-count (got index=%s count=%s).",
                cli_idx,
                cli_cnt,
            )
            sys.exit(2)
        return (cli_idx, cli_cnt)

    raw_cnt = (os.environ.get("SCANNER_SHARD_COUNT") or "").strip()
    if not raw_cnt:
        return (0, 1)
    try:
        shard_count = int(raw_cnt)
    except ValueError:
        logger.error("SCANNER_SHARD_COUNT must be an integer (got %r).", raw_cnt)
        sys.exit(2)
    if shard_count < 1:
        logger.error("SCANNER_SHARD_COUNT must be >= 1 (got %s).", shard_count)
        sys.exit(2)
    if shard_count == 1:
        return (0, 1)

    raw_idx = (
        os.environ.get("SCANNER_SHARD_INDEX") or os.environ.get("JOB_COMPLETION_INDEX") or ""
    ).strip()
    if not raw_idx:
        logger.error(
            "SCANNER_SHARD_COUNT=%s requires SCANNER_SHARD_INDEX or JOB_COMPLETION_INDEX (e.g. Indexed Job).",
            shard_count,
        )
        sys.exit(2)
    try:
        shard_index = int(raw_idx)
    except ValueError:
        logger.error(
            "Shard index must be an integer (SCANNER_SHARD_INDEX / JOB_COMPLETION_INDEX got %r).",
            raw_idx,
        )
        sys.exit(2)
    if shard_index < 0 or shard_index >= shard_count:
        logger.error(
            "Shard index %s out of range for SCANNER_SHARD_COUNT=%s (expected 0 .. %s).",
            shard_index,
            shard_count,
            shard_count - 1,
        )
        sys.exit(2)
    return (shard_index, shard_count)


def _playwright_inventory_json_predicate(dealer_base_url: str):
    """Sync predicate for ``page.wait_for_event("response", ...)`` — URL gate + JSON-ish content-type."""

    def _pred(resp: Any) -> bool:
        try:
            ct = resp.headers.get("content-type") or ""
            if not response_content_type_looks_json(ct):
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
    raw = (os.environ.get("SCANNER_MAX_DEALER_CONCURRENCY") or "3").strip()
    try:
        return max(1, int(raw))
    except ValueError:
        return 3


async def _upsert_vehicles_serialized(write_lock: asyncio.Lock, vehicles: list[dict]) -> int:
    """One SQLite writer at a time; run blocking upsert in a thread."""
    if not vehicles:
        return 0
    async with write_lock:
        return await asyncio.to_thread(upsert_vehicles, vehicles)


async def _scrape_inventory_path(
    context: Any,
    path: str,
    base_url: str,
    provider: str,
    dealer_id: str,
    dealer_name: str,
    inv_wait_ms: int,
    pag_wait_ms: int,
) -> tuple[list[tuple[str, Any]], str | None, int]:
    """
    Scrape one inventory path on a dedicated page within ``context``.
    Returns ``(intercept_records, page_html, url_denied_count)``.
    Opens and closes its own page; does not touch the warmup/VDP page.
    Structured JSON interception is primary: qualifying payloads skip saving HTML for this path.
    When no Next/Load-more control is visible, a scroll-to-bottom loop pulls lazy-loaded batches.
    """
    local_records: list[tuple[str, Any]] = []
    found_data = {"value": False}
    url_denied = 0
    resp_err = _DealerResponseErrorBudget()

    page = await context.new_page()

    async def handle_response(response: Any) -> None:
        nonlocal url_denied
        try:
            ct = response.headers.get("content-type") or ""
            if not response_content_type_looks_json(ct):
                return
            rurl = str(getattr(response, "url", "") or "")
            if not intercept_url_allowed(rurl, base_url):
                url_denied += 1
                logger.debug("Intercept URL denied [%s] path=%s: %s", dealer_name, path, _truncate_url(rurl))
                return
            body = await response.json()
            if not payload_qualifies_for_inventory_intercept(body):
                return
            local_records.append((rurl, body))
            found_data["value"] = True
            logger.info(
                "Intercepting: %s%s — structured inventory JSON (%s)",
                dealer_name,
                path,
                _truncate_url(rurl, 80),
            )
        except Exception as e:
            if resp_err.should_log():
                logger.debug(
                    "Intercept handler [%s] path=%s: %s %s",
                    dealer_name,
                    path,
                    type(e).__name__,
                    str(e)[:200],
                )

    page.on("response", handle_response)
    html: str | None = None
    full_url = base_url + path
    try:
        logger.info("Navigating: %s — %s", dealer_name, full_url)
        await _goto_with_retries(page, full_url, log_label=f"Nav:{dealer_name}", timeout_ms=20000)
        pred = _playwright_inventory_json_predicate(base_url)
        await _await_inventory_hydration(
            page,
            dealer_name,
            path,
            timeout_ms=_hydration_timeout_ms(),
            json_pred=pred,
        )
        try:
            await page.wait_for_event("response", pred, timeout=inv_wait_ms)
        except Exception:
            pass

        # Short idle: 3 s when data found, up to 12 s otherwise
        idle_cap = 3 if found_data["value"] else 12
        for _ in range(idle_cap):
            await asyncio.sleep(1)
            if found_data["value"]:
                break
        await asyncio.sleep(0.5)

        # Quick viewport ping when JSON hasn't landed — avoids dead wait when API-backed payloads are slow
        if not found_data["value"]:
            logger.info("Scrolling: %s%s — no JSON yet, initial scroll pulses", dealer_name, path)
            for _ in range(5):
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(2)

        if not await _any_next_control_visible(page):
            await _infinite_scroll_lazy_batches(
                page,
                dealer_name,
                path,
                pred,
                pag_wait_ms=pag_wait_ms,
            )

        # Pagination loop — uses only this path's own intercept records
        body_parse_cache: dict[int, list[dict[str, Any]]] = {}

        def _vehicles_for_body(body: Any) -> list[dict[str, Any]]:
            bid = id(body)
            cached = body_parse_cache.get(bid)
            if cached is not None:
                return cached
            vehicles = list(
                parse(provider, body, base_url=base_url, dealer_id=dealer_id, dealer_name=dealer_name, dealer_url=base_url)
            )
            for v in vehicles:
                v.setdefault("dealer_name", dealer_name)
                v.setdefault("dealer_url", base_url)
            body_parse_cache[bid] = vehicles
            return vehicles

        for _ in range(MAX_PAGINATION_CLICKS):
            total_count = pick_total_count_from_intercepts(local_records, base_url)
            by_vin: dict[str, dict[str, Any]] = {}
            for _ru, body in local_records:
                for v in _vehicles_for_body(body):
                    vin = (v.get("vin") or "").strip()
                    if vin:
                        by_vin[vin] = v
            if total_count is not None and total_count > len(by_vin):
                clicked = False
                for sel in NEXT_SELECTORS:
                    try:
                        loc = page.locator(sel)
                        if await loc.count() > 0:
                            first = loc.first
                            if await first.is_visible():
                                await first.click()
                                logger.info("Pagination: %s%s — clicked %s", dealer_name, path, sel)
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

        if not await _any_next_control_visible(page):
            await _infinite_scroll_lazy_batches(
                page,
                dealer_name,
                path,
                pred,
                pag_wait_ms=pag_wait_ms,
            )

        if found_data["value"]:
            html = None
        else:
            html = await page.content()
    except Exception as e:
        logger.warning("Path scrape failed [%s] %s: %s", dealer_name, full_url, e)
    finally:
        try:
            await page.close()
        except Exception:
            pass
    return local_records, html, url_denied


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
        ref = str(v.get("_detail_url") or v.get("detail_url") or "").strip()
        page_referer = ref if ref.lower().startswith("http") else None
        filtered = _llv.filter_gallery_urls_for_vehicle_listing(
            urls, max_workers=max_w, page_referer=page_referer
        )
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
    raw_manifest_url = (dealer.get("url") or "").strip()
    url, inv_doc_normalized = dealer_inventory_base_url(raw_manifest_url)
    url = url.rstrip("/")
    if inv_doc_normalized:
        logger.info(
            "Inventory base URL: %s — using site origin (manifest pointed at document: %s)",
            url,
            raw_manifest_url,
        )
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
    gate_stats = {"url_denied": 0}

    try:
        ctx_opts: dict[str, Any] = {"viewport": {"width": 1920, "height": 1080}}
        _ua = (os.environ.get("SCANNER_USER_AGENT") or "").strip() or _get_rotating_ua()
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

        inv_wait_ms = _inventory_wait_ms()
        pag_wait_ms = _pagination_response_wait_ms()

        # Scrape all three inventory paths in parallel — each on its own page within the
        # same browser context so session cookies from warmup are shared automatically.
        logger.info("Inventory paths: %s — launching %d parallel scrapers", name, len(INVENTORY_PATHS))
        path_results = await asyncio.gather(
            *[
                _scrape_inventory_path(
                    context,
                    path,
                    url,
                    provider,
                    dealer_id,
                    name,
                    inv_wait_ms,
                    pag_wait_ms,
                )
                for path in INVENTORY_PATHS
            ],
            return_exceptions=False,
        )

        # Merge results from all paths
        path_htmls: list[str | None] = []
        for path_records, path_html, path_denied in path_results:
            intercept_records.extend(path_records)
            gate_stats["url_denied"] += path_denied
            path_htmls.append(path_html)

        body_parse_cache: dict[int, list[dict[str, Any]]] = {}

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

        # Parse all accumulated payloads and merge by VIN (upsert_vehicles dedupes).
        # Parser sets carfax_url from explicit feed links first, then vhr.carfax.com. VDP capture
        # overwrites weak links with anchor/data URLs scraped from the live detail page when better.
        all_vehicles: list[dict[str, Any]] = []
        for _resp_url, body in intercept_records:
            all_vehicles.extend(_vehicles_for_body(body))

        result["inventory_rows"] = len(all_vehicles)

        if not all_vehicles:
            logger.info(
                "Extraction backup: %s — no vehicles from %d JSON intercept(s); trying HTML from path pages",
                name,
                len(intercept_records),
            )
            # Try each path's saved HTML in order; stop at first one that yields vehicles
            for path_html in path_htmls:
                if not path_html:
                    continue
                all_vehicles = list(
                    parse(provider, path_html, base_url=url, dealer_id=dealer_id, dealer_name=name, dealer_url=url)
                )
                for v in all_vehicles:
                    v.setdefault("dealer_name", name)
                    v.setdefault("dealer_url", url)
                if all_vehicles:
                    logger.info("HTML fallback: %s — recovered %d vehicle row(s) from HTML", name, len(all_vehicles))
                    break
                # Try __NEXT_DATA__ from the same HTML snapshot before moving on
                nd = parse_next_data_json_from_html(path_html)
                if nd is not None:
                    all_vehicles = list(
                        parse(provider, nd, base_url=url, dealer_id=dealer_id, dealer_name=name, dealer_url=url)
                    )
                    for v in all_vehicles:
                        v.setdefault("dealer_name", name)
                        v.setdefault("dealer_url", url)
                    if all_vehicles:
                        logger.info(
                            "HTML fallback: %s — recovered %d vehicle row(s) from __NEXT_DATA__",
                            name,
                            len(all_vehicles),
                        )
                        break

            # Last resort: live page fetch via the warmup page
            if not all_vehicles:
                nd = await fetch_next_data_json_from_page(page)
                if nd is not None:
                    all_vehicles = list(
                        parse(provider, nd, base_url=url, dealer_id=dealer_id, dealer_name=name, dealer_url=url)
                    )
                    for v in all_vehicles:
                        v.setdefault("dealer_name", name)
                        v.setdefault("dealer_url", url)
                if all_vehicles:
                    logger.info(
                        "HTML fallback: %s — recovered %d vehicle row(s) from live __NEXT_DATA__",
                        name,
                        len(all_vehicles),
                    )
                else:
                    logger.info("HTML fallback: %s — 0 vehicles (SPA shell or unsupported)", name)

            result["inventory_rows"] = len(all_vehicles)

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
                from backend.scanner.inventory_reconcile import (
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
        if provider in KNOWN_HAR_PROVIDERS:
            await _capture_scanner_failure_har(browser, url, dealer_id, name)
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
    post_listing_gap_fill: bool = False,
    enrichment_max_workers: int | None = None,
    gallery_vision_filter: bool = True,
    monroney_vision: bool = True,
):
    if dealers is None:
        dealers = load_manifest()
    logger.info("Loading manifest: %s (resolved %s)", MANIFEST_PATH, MANIFEST_PATH.resolve())

    # Filter out OEM manufacturer websites (not dealerships)
    original_count = len(dealers) if dealers else 0
    dealers = [d for d in (dealers or []) if not _is_oem_manufacturer_url(d.get("url", ""))]
    oem_filtered = original_count - len(dealers)
    if oem_filtered > 0:
        logger.info("Filtered out %d OEM manufacturer URL(s)", oem_filtered)

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
        browser = await p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"],
        )

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
                await asyncio.sleep(random.uniform(0.5, 2.5))
                return await one_dealer(dealer)

        try:
            loop = asyncio.get_running_loop()
            for _sig in (signal.SIGTERM,):
                with contextlib.suppress(Exception):
                    loop.add_signal_handler(_sig, loop.stop)
            return await asyncio.gather(
                *[bounded(d) for d in bmw_enhanced_dealers],
                return_exceptions=True,
            )
        finally:
            with contextlib.suppress(Exception):
                await asyncio.shield(asyncio.wait_for(browser.close(), timeout=6.0))

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

    if post_listing_gap_fill and scanned_vins:
        try:
            gap_summary = await asyncio.to_thread(run_listing_gap_fill_stage, scanned_vins)
            logger.info(
                "Listing gap fill (dictionary → listing page → DDG mechanical): %s",
                json.dumps(gap_summary, default=str)[:1600],
            )
        except Exception:
            logger.exception("Listing gap fill failed (inventory already saved)")

    # --- model_specs dictionary correction (full pass) ---
    # Per-VIN corrections already run inside upsert_vehicles; this final pass
    # catches any rows that were updated by post_repair after the initial upsert.
    try:
        from backend.scanner.database import apply_model_specs_corrections
        corrected = await asyncio.to_thread(apply_model_specs_corrections)
        if corrected:
            logger.info("model_specs final pass: %d rows corrected", corrected)
    except Exception:
        logger.exception("model_specs final correction pass failed")

    # --- EPA DICTIONARY enrichment (fill spec gaps from DICTIONARY/ CSV files) ---
    if scanned_vins and post_dict_enrich_env_enabled():
        try:
            dict_stats = await asyncio.to_thread(run_dictionary_enrich_for_vins, scanned_vins)
            if dict_stats.get("updated"):
                logger.info(
                    "EPA dictionary enrichment: %d/%d VINs updated (set SCANNER_POST_DICT_ENRICH=0 to skip)",
                    dict_stats["updated"],
                    dict_stats["vins"],
                )
        except Exception:
            logger.exception("EPA dictionary enrichment failed (inventory already saved)")

    # --- Resync incomplete_listings.db ---
    try:
        from backend.db.incomplete_listings_db import fast_rebuild_incomplete_listings_index
        n_incomplete = await asyncio.to_thread(fast_rebuild_incomplete_listings_index)
        logger.info("incomplete_listings resynced: %d incomplete listings", n_incomplete)
    except Exception:
        logger.exception("incomplete_listings resync failed")


def run_cli_entry() -> None:
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
        "--limit",
        type=int,
        default=None,
        help="Limit the number of dealerships to scan.",
    )
    ap.add_argument(
        "--shard-index",
        type=int,
        default=None,
        metavar="N",
        help=(
            "0-based shard for parallel scans (requires --shard-count). "
            "Dealer at manifest position i runs on shard (i %% count) == N."
        ),
    )
    ap.add_argument(
        "--shard-count",
        type=int,
        default=None,
        metavar="M",
        help="Number of shards / parallel workers (requires --shard-index when using CLI).",
    )
    ap.add_argument(
        "--provider",
        type=str,
        default=None,
        help="Only scan dealerships matching this provider (e.g., dealer_dot_com).",
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
        "--enable-interior-vision",
        action="store_true",
        help="Enable post-scan Ollama LLaVA interior/cabin analysis (default is off; run separately via image_analyzer.py).",
    )
    ap.add_argument(
        "--enable-gallery-vision",
        action="store_true",
        help="Enable LLaVA gallery cleanup before upsert (default is off; run separately via image_analyzer.py).",
    )
    ap.add_argument(
        "--enable-monroney-vision",
        action="store_true",
        help="Enable LLaVA Monroney / sticker pass before upsert (default is off; run separately via image_analyzer.py).",
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
    ap.add_argument(
        "--post-listing-gap-fill",
        action="store_true",
        help="After post-scan repair: backfill missing specs (EPA/vPIC → listing-page HTML via Playwright → DDG for mechanical only). Condition from listing sites only. Or set SCANNER_POST_LISTING_GAP_FILL=1.",
    )
    args = ap.parse_args()
    if is_inventory_postgres():
        from backend.db.inventory_db import init_inventory_db

        init_inventory_db()
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

    if args.provider is not None:
        prov = args.provider.strip()
        if not prov:
            logger.error("--provider is empty after stripping whitespace.")
            sys.exit(1)
        before = len(to_run)
        to_run = [d for d in to_run if d.get("provider") == prov]
        logger.info("Provider filter %r: %d of %d dealer(s) in manifest.", prov, len(to_run), before)
        if not to_run:
            logger.error(
                "No dealers with provider %r in %s — check manifest provider values.",
                prov,
                MANIFEST_PATH,
            )
            sys.exit(1)

    if args.limit is not None:
        if args.limit < 0:
            logger.error("--limit must be >= 0 (got %s).", args.limit)
            sys.exit(1)
        before = len(to_run)
        to_run = to_run[: args.limit]
        logger.info("Limit %d: scanning %d of %d dealer(s).", args.limit, len(to_run), before)

    shard_index, shard_count = _resolve_shard_cli_and_env(args)
    if shard_count > 1:
        before_shard = len(to_run)
        to_run = filter_manifest_by_shard(to_run, shard_index, shard_count)
        logger.info(
            "Shard %d/%d: %d of %d dealer(s) after prior filters.",
            shard_index,
            shard_count,
            len(to_run),
            before_shard,
        )
        if not to_run:
            logger.warning(
                "Shard %d/%d selected zero dealers — check manifest size vs shard count.",
                shard_index,
                shard_count,
            )

    do_repair = not args.no_post_repair and post_repair_env_enabled()
    do_listing = not args.no_post_listing_description and post_listing_description_env_enabled()
    do_vision = bool(args.post_enrich_vision_only) or post_enrich_vision_env_enabled()
    do_enrich = bool(args.post_enrich) or post_enrich_env_enabled()
    # Vision passes disabled by default (run separately via image_analyzer.py)
    # Enable with: --enable-gallery-vision, --enable-interior-vision, --enable-monroney-vision
    do_interior_vision = args.enable_interior_vision and post_interior_vision_env_enabled()
    do_gallery_vision = args.enable_gallery_vision and gallery_vision_filter_env_enabled()
    do_monroney = args.enable_monroney_vision and monroney_vision_env_enabled()
    do_kbb = bool(args.post_kbb) or post_kbb_env_enabled()
    do_listing_gap_fill = bool(args.post_listing_gap_fill) or post_listing_gap_fill_env_enabled()

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
                post_listing_gap_fill=do_listing_gap_fill,
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


if __name__ == "__main__":
    run_cli_entry()


__all__ = [
    "DEBUG_DIR",
    "MANIFEST_PATH",
    "filter_manifest_by_dealer_id",
    "filter_manifest_by_shard",
    "load_manifest",
    "main",
    "run_cli_entry",
    "_apply_gallery_vision_filter_to_vehicles",
    "_apply_monroney_vision_to_vehicles",
]
