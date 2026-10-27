"""Playwright navigation, warmup, and inventory path scraping helpers."""
from __future__ import annotations

import asyncio
import logging
import os
import random
import re
from typing import Any

from backend.scanner.constants import (
    DEBUG_DIR,
    KNOWN_HAR_PROVIDERS,
    MAX_PAGINATION_CLICKS,
    NEXT_SELECTORS,
    SCANNER_HYDRATION_SELECTORS,
)
from backend.scanner.scan_efficiency import (
    inventory_idle_loop_sec,
    inventory_json_wait_ms,
)
from backend.scanner.scrapers.scanner_intercept_filter import (
    effective_lot_total_from_intercepts,
    intercept_url_allowed,
    payload_qualifies_for_inventory_intercept,
    response_content_type_looks_json,
)

logger = logging.getLogger("scanner")

def get_rotating_ua() -> str:
    """Random modern Chrome UA; falls back to a static Chrome string."""
    try:
        from fake_useragent import UserAgent as _FakeUA
        pool = _FakeUA()
        return str(pool.chrome)
    except Exception:
        pass
    return (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )

def pagination_debug_enabled() -> bool:
    return (os.environ.get("SCANNER_PAGINATION_DEBUG") or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def hydration_timeout_ms() -> int:
    raw = (os.environ.get("SCANNER_HYDRATION_TIMEOUT_MS") or "30000").strip()
    try:
        return max(0, int(raw))
    except ValueError:
        return 30000


def _infinite_scroll_max_rounds() -> int:
    raw = (os.environ.get("SCANNER_INFINITE_SCROLL_MAX_ROUNDS") or "18").strip()
    try:
        return max(0, int(raw))
    except ValueError:
        return 18


def inventory_wait_ms() -> int:
    raw = (os.environ.get("SCANNER_INVENTORY_WAIT_MS") or "18000").strip()
    try:
        return max(1000, int(raw))
    except ValueError:
        return 18000


def pagination_response_wait_ms() -> int:
    raw = (os.environ.get("SCANNER_PAGINATION_RESPONSE_WAIT_MS") or "8000").strip()
    try:
        return max(300, min(inventory_wait_ms(), int(raw)))
    except ValueError:
        return min(inventory_wait_ms(), 8000)


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


async def goto_with_retries(page: Any, target: str, *, log_label: str, timeout_ms: int = 30000) -> None:
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
                "%s: goto retry %s/%s (%s: %s, sleep %.2fs)",
                log_label,
                i + 1,
                max_total,
                type(e).__name__,
                str(e).split("\n")[0][:160],
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


async def await_inventory_hydration(
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


async def any_next_control_visible(page: Any) -> bool:
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


async def infinite_scroll_lazy_batches(
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


class DealerResponseErrorBudget:
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


def playwright_inventory_json_predicate(dealer_base_url: str):
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


def is_playwright_shutdown_error(exc: BaseException) -> bool:
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


async def safe_close_context(context) -> None:
    if context is not None:
        try:
            await context.close()
        except BaseException:
            pass


async def try_apply_location_filter(
    page: Any,
    dealer_name: str,
    pred: Any,
    pag_wait_ms: int,
    *,
    dealer_city: str = "",
    dealer_state: str = "",
) -> bool:
    """
    Detect a Location checkbox filter on pooled-inventory sites and click only the entry
    matching this dealer so sister-store vehicles are excluded at the source.
    Returns True if a filter was applied.
    """
    from difflib import SequenceMatcher

    def _name_sim(a: str, b: str) -> float:
        a, b = a.lower().strip(), b.lower().strip()
        return SequenceMatcher(None, a, b).ratio()

    _MAKE_TOKENS = frozenset(
        {
            "hyundai",
            "toyota",
            "honda",
            "bmw",
            "mercedes",
            "benz",
            "lexus",
            "kia",
            "nissan",
            "mazda",
            "subaru",
            "volkswagen",
            "audi",
            "jeep",
            "ram",
            "dodge",
            "chrysler",
            "ford",
            "chevrolet",
            "gmc",
            "cadillac",
            "acura",
            "infiniti",
            "volvo",
            "porsche",
            "genesis",
            "mitsubishi",
        }
    )

    def _dealer_signature_tokens(name: str) -> list[str]:
        """Distinctive tokens (not make-only) so we do not click model trim filters."""
        return [
            tok
            for tok in re.split(r"[^\w]+", (name or "").lower())
            if len(tok) >= 4
            and tok not in _MAKE_TOKENS
            and tok not in {"auto", "motor", "motors", "national", "superstore", "store"}
        ]

    try:
        # Expand the Location filter panel if collapsed.
        expand_selectors = [
            "button:has-text('Location')",
            "[aria-label*='Location']",
            ".filter-header:has-text('Location')",
            ".accordion-header:has-text('Location')",
            "h3:has-text('Location')",
            "h4:has-text('Location')",
            "legend:has-text('Location')",
            "[data-filter-name='location']",
            "[data-facet='location']",
        ]
        for sel in expand_selectors:
            try:
                loc = page.locator(sel)
                if await loc.count() > 0 and await loc.first.is_visible():
                    aria = await loc.first.get_attribute("aria-expanded")
                    if aria == "false" or aria is None:
                        await loc.first.click()
                        await asyncio.sleep(0.6)
                    break
            except Exception:
                continue

        # Collect checkbox labels from location filter sections.
        candidates: list[tuple[str, Any]] = []
        label_selectors = [
            "[class*='location'] label",
            "[class*='location'] [class*='label']",
            "[data-facet='location'] label",
            "[id*='location'] label",
            ".location-filter label",
        ]
        for sel in label_selectors:
            try:
                els = page.locator(sel)
                count = await els.count()
                if count > 1:
                    for i in range(min(count, 60)):
                        txt = (await els.nth(i).inner_text()).strip()
                        if txt and len(txt) >= 3:
                            candidates.append((txt, els.nth(i)))
                    if candidates:
                        break
            except Exception:
                continue

        if not candidates:
            return False

        def _strip_count(s: str) -> str:
            return re.sub(r"\s+\d+\s*$", "", s).strip()

        def _norm_city(s: str) -> str:
            return re.sub(r"\s+", " ", (s or "").strip().lower())

        def _norm_state(s: str) -> str:
            st = re.sub(r"[^A-Za-z]", "", (s or "").strip().upper())
            return st[:2] if len(st) >= 2 else ""

        sig_tokens = _dealer_signature_tokens(dealer_name)
        city_n = _norm_city(dealer_city)
        state_n = _norm_state(dealer_state)

        best_label, best_el, best_sim, best_reason = None, None, 0.0, "name"

        for raw_txt, el in candidates:
            clean = _strip_count(raw_txt)
            sim = _name_sim(dealer_name, clean)
            reason = "name"
            if sim > best_sim:
                best_sim, best_label, best_el, best_reason = sim, clean, el, reason

        if (best_sim < 0.45 or best_el is None) and city_n:
            city_state_pat = (
                f"{city_n}, {state_n.lower()}" if state_n else city_n
            )
            for raw_txt, el in candidates:
                clean = _strip_count(raw_txt)
                clean_low = clean.lower()
                city_hit = city_n in clean_low
                state_hit = (not state_n) or state_n.lower() in clean_low or f", {state_n.lower()}" in clean_low
                if city_hit and state_hit:
                    sim = 0.82 if city_state_pat in clean_low else 0.75
                    if sim > best_sim:
                        best_sim, best_label, best_el, best_reason = sim, clean, el, "city"
                elif city_hit and not state_n:
                    sim = 0.7
                    if sim > best_sim:
                        best_sim, best_label, best_el, best_reason = sim, clean, el, "city_only"

        if best_sim < 0.45 or best_el is None:
            logger.debug(
                "Location filter [%s]: no close match (best='%s' sim=%.2f city=%s)",
                dealer_name,
                best_label,
                best_sim,
                city_n or "?",
            )
            return False

        label_low = (best_label or "").lower()
        if (
            best_reason == "name"
            and sig_tokens
            and not any(tok in label_low for tok in sig_tokens)
            and best_sim < 0.72
        ):
            logger.debug(
                "Location filter [%s]: rejected '%s' (sim=%.2f, no signature token in %s)",
                dealer_name,
                best_label,
                best_sim,
                sig_tokens,
            )
            return False

        logger.info(
            "Location filter [%s]: clicking '%s' (sim=%.2f, match=%s)",
            dealer_name,
            best_label,
            best_sim,
            best_reason,
        )
        await best_el.click()
        try:
            await page.wait_for_event("response", pred, timeout=pag_wait_ms)
        except Exception:
            pass
        await asyncio.sleep(1.0)
        return True

    except Exception as e:
        logger.debug("Location filter attempt skipped for %s: %s", dealer_name, e)
        return False

