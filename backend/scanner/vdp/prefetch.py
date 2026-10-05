"""
Pre-VDP field prefetch: fill vehicle rows from what we already know before
spending browser time. Two layers, both empty-slot-only:

1. DB merge — carry forward ONLY immutable-for-a-VIN enrichment fields from
   the previous scan's row (colors, engine, transmission, drivetrain, body
   style, fuel type, trim, condition, description, packages, mpg, cylinders)
   plus a gallery union. Price and mileage are NEVER merged: they change and
   must be re-acquired fresh every scan.
2. HTTP prefetch — plain GET of the car's detail page, parsing structured
   data only (JSON-LD/meta price, labeled color rows, spec rows) with the
   same conservative parsers the heal layer uses.

The browser VDP queue is built AFTER this runs, from the same field-emptiness
gates as before — so a car this module can't satisfy still gets the browser
visit exactly as it always did. Disable with SCANNER_VDP_DB_MERGE=0 /
SCANNER_VDP_HTTP_FIRST=0.
"""
from __future__ import annotations

import asyncio
import logging
import os
from typing import Any

logger = logging.getLogger("scanner")

# Facts that cannot change for a given VIN (or that dealers effectively never
# edit) — safe to carry forward from the prior scan when the fresh row is empty.
_DB_MERGE_TEXT_FIELDS: tuple[str, ...] = (
    "exterior_color",
    "interior_color",
    "engine_description",
    "engine_l",
    "transmission",
    "transmission_type",
    "drivetrain",
    "body_style",
    "fuel_type",
    "forced_induction",
    "trim",
    "condition",
    "description",
    "packages",
    "carfax_url",
)
_DB_MERGE_INT_FIELDS: tuple[str, ...] = ("cylinders", "mpg_city", "mpg_highway")

_HTTP_FETCH_TIMEOUT_S = 15.0
_HTTP_CONCURRENCY = 6
_HTTP_POLITE_DELAY_S = 0.15  # per worker, between fetches; sustained bursts soft-block Team Velocity
# Hosts that answered 403 in this process get one worker and a longer gap; Dealer
# Inspire's Cloudflare 403'd 202 of 400 Culver City fetches at 6 x 150 ms.
_SLOW_HOST_DELAY_S = 1.5   # measured 2026-09-23: 1 req / 1.5 s never 403s on DI; 3 workers at 0.5 s still 17%
_SLOW_HOST_COOLDOWN_S = 15.0  # a tripped WAF keeps refusing for a while; wait before the first serialized retry
_SLOW_HOSTS: set[str] = set()
_HOST_LOCKS: dict[str, asyncio.Lock] = {}
_HOST_COOLDOWN_UNTIL: dict[str, float] = {}
_DEAD_HOST_SILENT_MAX = 3  # attempts that return neither a status nor a body before the host is skipped for this run
_SLOW_HOST_MAX_FAILS = 3  # serialized 403s in one run (not consecutive: a hot host answers at random) -> stop for this run


_WALL_CLOCK_MIN_SEC = 30.0
_WALL_CLOCK_MAX_SEC = 1800.0
_HTTP_FIRST_PAGES_MAX = 5000


def _dealer_timing(dealer_id: str) -> dict[str, Any]:
    """``scan_hints["timing"]`` for the dealer, written by the pipeline's
    assess step (backend/scanner/scan_timing.py): one synchronous Postgres
    read. Best-effort: no dealer, no DB, no block -> {} and the env defaults
    apply. Imported lazily so the module keeps working in DB-less tests.
    ``http_prefetch_missing_fields`` reads it once per pass off the event
    loop (``asyncio.to_thread``) and hands the block to the window helpers."""
    if not dealer_id:
        return {}
    try:
        from backend.scanner.recipe_store import get_scan_hints

        timing = (get_scan_hints(dealer_id) or {}).get("timing") or {}
        return timing if isinstance(timing, dict) else {}
    except Exception:  # noqa: BLE001 - a hint lookup must never block the scan
        return {}


def _dealer_timing_hint(dealer_id: str, key: str, timing: dict[str, Any] | None = None) -> float | None:
    """``timing[key]`` as a float, or None. ``timing`` is the block already
    read for this pass; when it is None the block is read now (sync callers,
    tests)."""
    if timing is None:
        timing = _dealer_timing(dealer_id)
    v = timing.get(key) if isinstance(timing, dict) else None
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def _http_first_wall_clock_sec(dealer_id: str = "", timing: dict[str, Any] | None = None) -> float:
    """Hard cap on the whole HTTP-first pass per dealer (default 300 s). Whatever was
    filled by then is kept and the dealer proceeds to its upsert; a throttled host
    can no longer hold a dealer past the pipeline's batch timeout (mblaguna-com
    2026-09-24: 1,026 feed rows lost when the batch was killed at 30 min).

    A dealer whose fingerprint carries ``timing.vdp_http_first_max_sec`` (the
    window its last capped run said it needs, 2026-09-28) gets that instead,
    clamped to 30..1800 s."""
    try:
        default = max(_WALL_CLOCK_MIN_SEC, float((os.environ.get("SCANNER_VDP_HTTP_FIRST_MAX_SEC") or "300").strip()))
    except ValueError:
        default = 300.0
    hinted = _dealer_timing_hint(dealer_id, "vdp_http_first_max_sec", timing)
    if hinted is None or hinted <= 0:
        return default
    window = min(_WALL_CLOCK_MAX_SEC, max(_WALL_CLOCK_MIN_SEC, hinted))
    logger.info("http_first [%s]: per-dealer wall clock %.0fs from scan_hints.timing (default %.0fs)", dealer_id, window, default)
    return window


def _slow_host_budget() -> int:
    """Detail fetches per run on a serialized (WAF-throttled) host, default 120:
    ~3 min at 1.5 s. A hot Dealer Inspire host answered 403 to half of 525 serialized
    fetches and the cool-downs stretched one dealer past 35 min. Rows filled this run
    stop being candidates, so the next run reaches the next 120."""
    try:
        return max(0, int((os.environ.get("SCANNER_VDP_HTTP_FIRST_SLOW_HOST_MAX") or "60").strip()))
    except ValueError:
        return 60
_HTTP_DESCRIPTION_MIN_CHARS = 40  # same floor the browser queue uses
_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)


def _flag(name: str, default: str = "1") -> bool:
    return (os.environ.get(name) or default).strip().lower() not in ("0", "false", "no", "off")


def vdp_db_merge_enabled() -> bool:
    return _flag("SCANNER_VDP_DB_MERGE")


def vdp_http_first_enabled() -> bool:
    return _flag("SCANNER_VDP_HTTP_FIRST")


def _http_first_max(dealer_id: str = "", timing: dict[str, Any] | None = None) -> int:
    """Detail pages fetched per dealer per run (default 800, hard max 5000).

    A dealer whose fingerprint carries ``timing.pages_needed`` (the most pages
    its recent runs wanted, candidates + skipped_cap) gets that cap when it is
    HIGHER than the env default — the hint only ever widens the pass, since
    lowering it would skip pages the next lot could need."""
    try:
        default = max(0, min(_HTTP_FIRST_PAGES_MAX, int((os.environ.get("SCANNER_VDP_HTTP_FIRST_MAX") or "800").strip())))
    except ValueError:
        default = 800
    hinted = _dealer_timing_hint(dealer_id, "pages_needed", timing)
    if hinted is None or hinted <= default:
        return default
    cap = int(min(_HTTP_FIRST_PAGES_MAX, hinted))
    logger.info("http_first [%s]: per-dealer page cap %d from scan_hints.timing (default %d)", dealer_id, cap, default)
    return cap


def _http_first_gallery_min() -> int:
    """Fetch the VDP over HTTP when the feed gave fewer HTTPS photos than this;
    dealer.com and DealerOn pages carry 6-20 more in static HTML/JSON."""
    try:
        return max(0, int((os.environ.get("SCANNER_VDP_HTTP_FIRST_GALLERY_MIN") or "12").strip()))
    except ValueError:
        return 12


def _empty(v: Any) -> bool:
    if v is None:
        return True
    if isinstance(v, str) and not v.strip():
        return True
    return False


def _load_prior_rows_by_vin(dealer_id: str, vins: list[str]) -> dict[str, dict[str, Any]]:
    """Previous-scan rows for this dealer's VINs (dealer-scoped: VINs can recur
    across dealers after trades)."""
    if not vins:
        return {}
    import sqlite3

    from backend.db.inventory_db import _parse_car_gallery, db_conn

    out: dict[str, dict[str, Any]] = {}
    with db_conn(row_factory=sqlite3.Row) as conn:
        cursor = conn.cursor()
        ph = ",".join("?" * len(vins))
        cursor.execute(
            f"SELECT * FROM cars WHERE dealer_id = ? AND vin IN ({ph})",
            [dealer_id, *vins],
        )
        for row in cursor.fetchall():
            d = dict(row)
            _parse_car_gallery(d)
            vin = str(d.get("vin") or "").strip().upper()
            if vin:
                out[vin] = d
    return out


def merge_known_fields_from_db(
    vehicles: list[dict[str, Any]],
    dealer_id: str,
) -> dict[str, int]:
    """Fill empty immutable fields on scraped rows from prior DB rows by VIN."""
    from backend.utils.gallery_merge import merge_inventory_row_galleries

    vins = sorted({
        str(v.get("vin") or "").strip().upper()
        for v in vehicles
        if len(str(v.get("vin") or "").strip()) == 17
    })
    prior = _load_prior_rows_by_vin(dealer_id, vins)
    stats = {"vins_known": len(prior), "fields_filled": 0, "vehicles_touched": 0}
    if not prior:
        return stats

    for v in vehicles:
        vin = str(v.get("vin") or "").strip().upper()
        row = prior.get(vin)
        if not row:
            continue
        touched = False
        for k in _DB_MERGE_TEXT_FIELDS:
            if _empty(v.get(k)) and not _empty(row.get(k)):
                v[k] = row[k]
                stats["fields_filled"] += 1
                touched = True
        for k in _DB_MERGE_INT_FIELDS:
            cur = v.get(k)
            try:
                cur_n = int(cur) if cur is not None and str(cur).strip() != "" else None
            except (TypeError, ValueError):
                cur_n = None
            if cur_n is not None and cur_n > 0:
                continue
            src = row.get(k)
            try:
                src_n = int(src) if src is not None and str(src).strip() != "" else None
            except (TypeError, ValueError):
                src_n = None
            if src_n is not None and src_n > 0:
                if k == "cylinders":
                    # A prior row's count can be enrichment-written and wrong
                    # (8 on "2.7L I4" Silverados). Never carry it onto a fresh
                    # scrape whose own engine text says otherwise.
                    try:
                        from backend.utils.engine_consistency import cylinders_conflicts_with_engine_text

                        if cylinders_conflicts_with_engine_text(
                            src_n, v.get("engine_description"), v.get("fuel_type")
                        ):
                            continue
                    except Exception:
                        pass
                v[k] = src_n
                stats["fields_filled"] += 1
                touched = True
        prior_gallery = row.get("gallery")
        if isinstance(prior_gallery, list) and prior_gallery:
            before = len(v.get("gallery") or []) if isinstance(v.get("gallery"), list) else 0
            merge_inventory_row_galleries(v, {"gallery": prior_gallery, "image_url": row.get("image_url")})
            after = len(v.get("gallery") or []) if isinstance(v.get("gallery"), list) else 0
            if after > before:
                stats["fields_filled"] += 1
                touched = True
        if touched:
            stats["vehicles_touched"] += 1
    return stats


def _gallery_thin(v: dict[str, Any]) -> bool:
    from backend.scanner.vdp.queue import _count_https_gallery_urls

    return _count_https_gallery_urls(v) < _http_first_gallery_min()


def _description_short(v: dict[str, Any]) -> bool:
    return len(str(v.get("description") or "").strip()) < _HTTP_DESCRIPTION_MIN_CHARS


def _extras_missing(v: dict[str, Any]) -> bool:
    """MSRP (new cars), stock number, history link: only the detail page has them
    on Dealer Inspire and dealer.com templates."""
    cond = str(v.get("condition") or "").strip().lower()
    if cond.startswith("new") and _empty(v.get("msrp")):
        return True
    return _empty(v.get("stock_number")) or _empty(v.get("carfax_url"))


def _vehicle_wants_http_prefetch(v: dict[str, Any]) -> bool:
    """Every gap the browser queue would otherwise visit for: price, spec fields,
    colours, and (since 2026-09-22) description and a thin gallery. Those last two
    were 92% of the browser visits and the HTML already carries them on
    dealer.com / DealerOn templates. Since 2026-09-23 also MSRP / stock / history
    link, which the same inline JSON carries."""
    from backend.scanner.utils.vdp_price_merge import listing_price_is_empty
    from backend.scanner.vdp.core import _vehicle_needs_spec_gap_vdp

    if listing_price_is_empty(v):
        return True
    if _vehicle_needs_spec_gap_vdp(v):
        return True
    if _empty(v.get("exterior_color")) or _empty(v.get("interior_color")):
        return True
    return _description_short(v) or _gallery_thin(v) or _extras_missing(v)


_LAST_STATUS: dict[str, int] = {}  # url -> last HTTP status, for the prefetch stats


_LAST_ERROR: dict[str, str] = {}  # url -> exception class + head of message, for the stats


def _note_error(url: str, exc: BaseException) -> None:
    if len(_LAST_ERROR) > 5000:
        _LAST_ERROR.clear()
    _LAST_ERROR[url] = f"{type(exc).__name__}: {str(exc)[:80]}"


def _note_status(url: str, status: int) -> None:
    if len(_LAST_STATUS) > 5000:
        _LAST_STATUS.clear()
    _LAST_STATUS[url] = status


def _fetch_html(url: str) -> str | None:
    """Fetch a VDP with a browser TLS fingerprint first (curl_cffi); Dealer Inspire's
    Cloudflare returns 403 to plain requests on every rooftop. Falls back to
    requests when curl_cffi is missing or errors.

    No challenge detection here (unlike synth's fetchers): a 200 HTML challenge page
    is returned as the VDP. Pinned by test_scanner_http_characterization (audit F-18)."""
    from backend.scanner.net import client as net_client

    from urllib.parse import urlparse as _up

    _p = _up(url)
    _origin = f"{_p.scheme}://{_p.netloc}/" if _p.netloc else ""
    # Same-site Referer: Cloudflare rules on Dealer eProcess / Dealer Inspire sites serve a
    # managed challenge to detail-page requests that arrive without one (Honda of El
    # Cajon: 0/4 without, 7/7 with).
    headers = {"User-Agent": _UA, "Accept": "text/html,application/xhtml+xml", "Referer": _origin,
               "Sec-Fetch-Site": "same-origin", "Sec-Fetch-Mode": "navigate", "Sec-Fetch-Dest": "document"}
    proxies = net_client.scanner_proxies()
    try:
        cffi_requests = net_client.import_curl_cffi()

        resp = net_client.send(
            cffi_requests, "GET", url, via_get=True,
            headers=headers, impersonate="chrome", timeout=_HTTP_FETCH_TIMEOUT_S, proxies=proxies,
        )
        _note_status(url, int(resp.status_code))
        if resp.status_code == 200 and "html" in (resp.headers.get("content-type") or ""):
            return resp.text
        if net_client.is_fingerprint_block(resp.status_code, net_client.VDP_FINGERPRINT_BLOCK_STATUSES):
            return None  # a fingerprint block; plain requests will not do better
    except ImportError:
        pass
    except Exception as exc:  # noqa: BLE001 - fall through to plain requests, but remember why
        _note_error(url, exc)
    requests = net_client.import_requests()

    try:
        resp = net_client.send(
            requests, "GET", url, via_get=True,
            headers=headers, timeout=_HTTP_FETCH_TIMEOUT_S, proxies=proxies,
        )
        _note_status(url, int(resp.status_code))
        if resp.status_code != 200 or "html" not in (resp.headers.get("content-type") or ""):
            return None
        return resp.text
    except Exception as exc:  # noqa: BLE001
        _note_error(url, exc)
        _note_status(url, 0)
        return None


def _apply_page_fields(v: dict[str, Any], html: str, url: str = "") -> int:
    """Fill empty fields from structured page data. Returns fields filled."""
    from backend.scanner.utils.vdp_price_merge import listing_price_is_empty
    from backend.scanner.utils.vdp_spec_parse import (
        parse_color_from_listing_html,
        parse_html_for_vehicle_specs,
        parse_price_from_listing_html,
    )

    filled = 0
    filled += _apply_description_and_gallery(v, html, url)
    filled += _apply_extras(v, html)
    if listing_price_is_empty(v):
        price = parse_price_from_listing_html(html)
        if price:
            v["price"] = int(round(price))
            v.setdefault("_price_source", "vdp_http_prefetch")
            filled += 1
    if _empty(v.get("exterior_color")) or _empty(v.get("interior_color")):
        colors = parse_color_from_listing_html(html)
        if _empty(v.get("exterior_color")) and colors.get("exterior_color"):
            v["exterior_color"] = str(colors["exterior_color"])[:120]
            filled += 1
        if _empty(v.get("interior_color")) and colors.get("interior_color"):
            v["interior_color"] = str(colors["interior_color"])[:120]
            filled += 1
    specs = parse_html_for_vehicle_specs(html)
    for k in ("transmission", "drivetrain", "fuel_type", "body_style", "engine_description"):
        if _empty(v.get(k)) and specs.get(k):
            v[k] = str(specs[k])[:200]
            filled += 1
    for k in ("cylinders", "mpg_city", "mpg_highway"):
        if specs.get(k) is None:
            continue
        cur = v.get(k)
        try:
            has = cur is not None and int(cur) > 0
        except (TypeError, ValueError):
            has = False
        if not has:
            try:
                v[k] = int(specs[k])
                filled += 1
            except (TypeError, ValueError):
                pass
    return filled


def _apply_extras(v: dict[str, Any], html: str) -> int:
    """MSRP, stock, history link, colours, drivetrain, trim from the page's inline
    vehicle JSON. Fills blanks only; never overwrites a feed value."""
    from backend.scanner.utils.vdp_extras_parse import parse_vdp_extras_from_html

    try:
        extras = parse_vdp_extras_from_html(html, str(v.get("vin") or ""))
    except Exception:  # noqa: BLE001
        return 0
    filled = 0
    for k in ("msrp", "stock_number", "carfax_url", "exterior_color", "interior_color", "drivetrain",
              "trim", "fuel_type", "engine_description", "transmission", "make", "model", "mileage", "price"):
        val = extras.get(k)
        if val is None or not _empty(v.get(k)):
            continue
        v[k] = val
        v.setdefault(f"_{k}_source", "vdp_http_prefetch")
        filled += 1
    return filled


def _apply_description_and_gallery(v: dict[str, Any], html: str, url: str) -> int:
    """Description paragraph and static gallery URLs from the fetched HTML, the two
    fields that drove most browser visits. Gallery only ever grows."""
    from backend.scanner.vdp.html_recovery import (
        extract_description_from_html,
        harvest_gallery_urls_from_html,
    )

    filled = 0
    if _description_short(v):
        desc = extract_description_from_html(html) or ""
        if len(desc.strip()) >= _HTTP_DESCRIPTION_MIN_CHARS:
            v["description"] = desc.strip()[:4000]
            v.setdefault("_description_source", "vdp_http_prefetch")
            filled += 1
    if _gallery_thin(v):
        try:
            from backend.scanner.scan_efficiency import vdp_gallery_url_max

            cap = vdp_gallery_url_max()
        except Exception:  # noqa: BLE001
            cap = None
        found = harvest_gallery_urls_from_html(html, url or str(v.get("_detail_url") or ""), max_urls=cap)
        if found:
            cur = v.get("gallery") if isinstance(v.get("gallery"), list) else []
            seen = {u for u in cur if isinstance(u, str)}
            merged = list(cur)
            for u in found:
                if isinstance(u, str) and u.lower().startswith("https://") and u not in seen:
                    seen.add(u)
                    merged.append(u)
            if len(merged) > len(cur):
                # The feed seeds ["/static/placeholder.svg"]; once a real photo
                # exists the placeholder and any non-https entry go, and the hero
                # follows the first real photo (F03, 2026-09-28: 22,340 rows kept
                # image_url = placeholder next to a full gallery).
                from backend.parsers.base import _is_placeholder_url

                def _real(urls: list[Any]) -> list[str]:
                    return [
                        u for u in urls
                        if isinstance(u, str) and u.lower().startswith("https://") and not _is_placeholder_url(u)
                    ]

                real_before = len(_real(cur))
                real = _real(merged)
                if real:
                    merged = real
                v["gallery"] = merged if cap is None else merged[:cap]
                # Count real photos gained, not list length: dropping the seeded
                # placeholder / widget logo made len(merged) - len(cur) 0 or
                # negative (and a negative still counted as "extended").
                added = max(0, len(_real(v["gallery"])) - real_before)
                if added:
                    v["_gallery_http_prefetch_added"] = added
                    filled += 1
                hero = str(v.get("image_url") or "")
                if real and not hero.lower().startswith("http"):
                    v["image_url"] = real[0]
    return filled


_SHARED_GALLERY_MIN_OTHERS = 3


def drop_shared_gallery_urls(vehicles: list[dict[str, Any]], *, min_others: int = _SHARED_GALLERY_MIN_OTHERS) -> dict[str, int]:
    """Drop gallery URLs that also appear on ``min_others``+ other vehicles of
    the same dealer batch: library / stock art, never a photo of this car.

    autoWALL stores serve stock art from the CDN path Team Velocity uses for
    real photos (assets.cai-media-management.com/resize/WxH/common-vehicle-media/
    <uuid>.jpg, both platforms, same resize sizes), so no URL rule tells them
    apart; the only difference is that a real photo belongs to one car while
    library art carries the same UUID across many. "common-vehicle-media" was
    a fluff signal for exactly that reason and dropped every Team Velocity
    photo (F03, 2026-09-28); this batch-level check replaces it. Only https
    entries count (placeholders are shared by design and handled elsewhere).
    A hero that was dropped follows the first photo that remains.
    """
    stats = {"urls_dropped": 0, "vehicles_touched": 0, "removals": 0}
    if len(vehicles) <= min_others:
        return stats
    seen_on: dict[str, int] = {}
    per_vehicle: list[set[str]] = []
    for v in vehicles:
        gal = v.get("gallery") if isinstance(v.get("gallery"), list) else []
        urls = {u for u in gal if isinstance(u, str) and u.lower().startswith("https://")}
        per_vehicle.append(urls)
        for u in urls:
            seen_on[u] = seen_on.get(u, 0) + 1
    shared = {u for u, n in seen_on.items() if n > min_others}
    if not shared:
        return stats
    stats["urls_dropped"] = len(shared)
    for v, urls in zip(vehicles, per_vehicle):
        hit = urls & shared
        if not hit:
            continue
        gal = [u for u in v["gallery"] if not (isinstance(u, str) and u in shared)]
        stats["removals"] += len(v["gallery"]) - len(gal)
        stats["vehicles_touched"] += 1
        v["gallery"] = gal
        hero = str(v.get("image_url") or "")
        if hero in shared:
            nxt = next((u for u in gal if isinstance(u, str) and u.lower().startswith("https://")), None)
            if nxt:
                v["image_url"] = nxt
    return stats


async def http_prefetch_missing_fields(
    vehicles: list[dict[str, Any]], dealer_id: str = "", timing: dict[str, Any] | None = None,
) -> dict[str, int]:
    """Concurrent HTTP prefetch for vehicles still missing queue-driving fields.

    ``dealer_id`` lets the page cap and wall clock come from the dealer's
    fingerprint (``scan_hints.timing``) when it has one; the block is read
    once here, in a worker thread, unless ``timing`` was already read."""
    if timing is None:
        timing = await asyncio.to_thread(_dealer_timing, dealer_id) if dealer_id else {}
    candidates: list[dict[str, Any]] = []
    for v in vehicles:
        url = str(v.get("_detail_url") or v.get("source_url") or "").strip()
        if url.startswith("http") and _vehicle_wants_http_prefetch(v):
            candidates.append(v)
    cap = _http_first_max(dealer_id, timing)
    skipped_cap = max(0, len(candidates) - cap)
    candidates = candidates[:cap]
    stats = {
        "candidates": len(candidates), "fetched": 0, "fields_filled": 0, "skipped_cap": skipped_cap,
        "descriptions_filled": 0, "galleries_extended": 0, "statuses": {},
    }
    if not candidates:
        return stats

    sem = asyncio.Semaphore(_HTTP_CONCURRENCY)

    def _host(u: str) -> str:
        from urllib.parse import urlparse

        return (urlparse(u).hostname or "").lower()

    slow_used: dict[str, int] = {}
    slow_budget = _slow_host_budget()
    fail_total: dict[str, int] = {}
    exhausted: set[str] = set()
    silent: dict[str, int] = {}
    fetched_ok: dict[str, int] = {}
    dead: set[str] = set()

    async def _fetch_paced(url: str) -> tuple[str | None, int | None]:
        import time as _time

        host = _host(url)
        if host in _SLOW_HOSTS:
            if host in exhausted or slow_used.get(host, 0) >= slow_budget:
                stats["slow_host_skipped"] = stats.get("slow_host_skipped", 0) + 1
                return None, None
            slow_used[host] = slow_used.get(host, 0) + 1
            lock = _HOST_LOCKS.setdefault(host, asyncio.Lock())
            async with lock:  # one in flight per slow host
                if host in dead:  # decided while this task queued on the lock
                    stats["dead_host_skipped"] = stats.get("dead_host_skipped", 0) + 1
                    return None, None
                wait = _HOST_COOLDOWN_UNTIL.get(host, 0.0) - _time.monotonic()
                if wait > 0:
                    await asyncio.sleep(wait)
                html = await asyncio.to_thread(_fetch_html, url)
                await asyncio.sleep(_SLOW_HOST_DELAY_S)
        else:
            async with sem:
                if host in dead:  # every task queued behind the semaphore sees the verdict
                    stats["dead_host_skipped"] = stats.get("dead_host_skipped", 0) + 1
                    return None, None
                html = await asyncio.to_thread(_fetch_html, url)
                await asyncio.sleep(_HTTP_POLITE_DELAY_S)
        st = _LAST_STATUS.pop(url, None)
        # A host that answers nothing at all (no status: connect/read timeouts,
        # resets) is a tarpit for this IP. Serialized at 15 s per attempt it
        # burned the whole 300 s wall clock on 18 dealers of the 2026-09-24 fleet
        # run with 0 pages fetched (Bill Luke 0/800, BMW of Ontario 0/530,
        # O'Brien Toyota 0/94). Three silent attempts with no success: stop.
        if st is None and html is None:
            silent[host] = silent.get(host, 0) + 1
            if silent[host] >= _DEAD_HOST_SILENT_MAX and not fetched_ok.get(host):
                if host not in dead:
                    dead.add(host)
                    stats["dead_host"] = host
        elif html is not None:
            fetched_ok[host] = fetched_ok.get(host, 0) + 1
        if st in (403, 429):
            was_slow = host in _SLOW_HOSTS
            if not was_slow:
                _SLOW_HOSTS.add(host)
                stats["slowed_host"] = host
            # every 403 restarts the cool-down: the WAF window is longer than one request
            _HOST_COOLDOWN_UNTIL[host] = _time.monotonic() + _SLOW_HOST_COOLDOWN_S
            if was_slow:
                fail_total[host] = fail_total.get(host, 0) + 1
                if fail_total[host] >= _SLOW_HOST_MAX_FAILS:
                    exhausted.add(host)
                    stats["host_exhausted"] = host  # serialized and still refused: hot for this IP, stop here
        return html, st

    async def _one(v: dict[str, Any]) -> None:
        url = str(v.get("_detail_url") or v.get("source_url") or "").strip()
        html, st = await _fetch_paced(url)
        attempts = 0
        while not html and st in (403, 429) and attempts < 2:
            attempts += 1
            html, st = await _fetch_paced(url)  # serialized, after the host cool-down
            stats["retried"] = stats.get("retried", 0) + 1
        if st is not None:
            key = str(st)
            stats["statuses"][key] = stats["statuses"].get(key, 0) + 1
        err = _LAST_ERROR.pop(url, None)
        if err and not html:
            errs = stats.setdefault("errors", {})
            k = err.split(":")[0]
            errs[k] = errs.get(k, 0) + 1
            stats.setdefault("error_sample", err)
        if not html:
            return
        stats["fetched"] += 1
        had_desc = not _description_short(v)
        stats["fields_filled"] += _apply_page_fields(v, html, url)
        if not had_desc and not _description_short(v):
            stats["descriptions_filled"] += 1
        if v.pop("_gallery_http_prefetch_added", 0):
            stats["galleries_extended"] += 1

    wall_clock = _http_first_wall_clock_sec(dealer_id, timing)
    stats["wall_clock_sec"] = int(wall_clock)
    try:
        await asyncio.wait_for(asyncio.gather(*(_one(v) for v in candidates)), timeout=wall_clock)
    except asyncio.TimeoutError:
        stats["wall_clock_hit"] = True
        logger.warning(
            "http_first: wall clock %.0fs reached after %d/%d page(s); keeping what was filled",
            wall_clock, stats["fetched"], stats["candidates"],
        )
    return stats


def _premark_slow_hosts(vehicles: list[dict[str, Any]], dealer_id: str) -> None:
    """Dealer Inspire rooftops (their inventory recipe hits the carscommerce search
    API) 403 any parallel detail-page traffic: 6 workers = 70% blocked, and a
    tripped host stays hot. Serialize them from the first request instead of
    learning it from the first 403 with six requests already in flight."""
    try:
        from urllib.parse import urlparse

        from backend.scanner.recipes import load_recipes

        if not any("carscommerce" in (urlparse(r.url).hostname or "") for r in load_recipes(dealer_id)):
            return
        for v in vehicles:
            u = str(v.get("_detail_url") or v.get("source_url") or "")
            host = (urlparse(u).hostname or "").lower()
            if host:
                _SLOW_HOSTS.add(host)
                break
    except Exception:  # noqa: BLE001
        return


async def prefetch_before_vdp(
    vehicles: list[dict[str, Any]],
    dealer_id: str,
    dealer_name: str,
) -> dict[str, Any] | None:
    """Run both prefetch layers; the browser queue is built afterwards from the
    unchanged emptiness gates, so this can only shrink it, never mis-skip."""
    if not vehicles:
        return None
    out: dict[str, Any] = {}
    if vdp_db_merge_enabled():
        db_stats = await asyncio.to_thread(merge_known_fields_from_db, vehicles, dealer_id)
        out["db_merge"] = db_stats
    if vdp_http_first_enabled():
        _premark_slow_hosts(vehicles, dealer_id)
        # Per-car JSON endpoints captured on earlier browser visits come first:
        # cheaper than the HTML and they carry what the page's JS would have.
        try:
            from backend.scanner.vdp.vdp_recipes import apply_vdp_recipes

            rec_stats = await apply_vdp_recipes(
                vehicles, dealer_id, wants=_vehicle_wants_http_prefetch, gallery_min=_http_first_gallery_min()
            )
            out["vdp_recipes"] = rec_stats
            if rec_stats.get("recipes"):
                logger.info(
                    "VDP recipes [%s]: %d recipe(s), %d/%d car(s) answered, %d field(s), %d gallery(ies) extended, %d marked stale",
                    dealer_name, rec_stats["recipes"], rec_stats["fetched"], rec_stats["candidates"],
                    rec_stats["fields_filled"], rec_stats["galleries_extended"], rec_stats["stale_marked"],
                )
        except Exception as exc:  # noqa: BLE001 - replay must never block the scan
            logger.warning("VDP recipes [%s]: replay failed: %s", dealer_name, str(exc)[:160])
        http_stats = await http_prefetch_missing_fields(vehicles, dealer_id)
        out["http_first"] = http_stats
    shared = drop_shared_gallery_urls(vehicles)
    if shared["removals"]:
        out["gallery_shared"] = shared
        logger.info(
            "VDP prefetch [%s]: dropped %d gallery URL(s) shared by 4+ cars (%d removal(s) on %d car(s))",
            dealer_name, shared["urls_dropped"], shared["removals"], shared["vehicles_touched"],
        )
    if not out:
        return None
    logger.info(
        "VDP prefetch [%s]: db_merge filled %d field(s) on %d car(s) (%d known VINs); "
        "http_first fetched %d/%d page(s), filled %d field(s), %d description(s), %d gallery(ies) extended, statuses=%s errors=%s dead_host=%s",
        dealer_name,
        out.get("db_merge", {}).get("fields_filled", 0),
        out.get("db_merge", {}).get("vehicles_touched", 0),
        out.get("db_merge", {}).get("vins_known", 0),
        out.get("http_first", {}).get("fetched", 0),
        out.get("http_first", {}).get("candidates", 0),
        out.get("http_first", {}).get("fields_filled", 0),
        out.get("http_first", {}).get("descriptions_filled", 0),
        out.get("http_first", {}).get("galleries_extended", 0),
        out.get("http_first", {}).get("statuses", {}),
        out.get("http_first", {}).get("errors", {}),
        out.get("http_first", {}).get("dead_host"),
    )
    return out
