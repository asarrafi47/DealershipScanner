"""Dealer.com ws-inv-data POST bulk inventory fetch (pageSize bump + start offset pages)."""
from __future__ import annotations

import copy
import json
import logging
import os
from typing import Any

from backend.parsers.base import find_vehicle_list, get_total_count

logger = logging.getLogger("scanner")

_FETCH_JS = """async ({url, body}) => {
  try {
    const r = await fetch(url, {
      method: "POST",
      credentials: "include",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify(body),
    });
    if (!r.ok) return { ok: false, status: r.status, data: null };
    return { ok: true, status: r.status, data: await r.json() };
  } catch (e) {
    return { ok: false, status: 0, data: null, error: String(e) };
  }
}"""

_MAX_EXTRA_PAGES = 50


def dealer_com_bulk_fetch_enabled() -> bool:
    raw = (os.environ.get("SCANNER_DEALER_COM_BULK_FETCH") or "1").strip().lower()
    return raw not in ("0", "false", "no", "off")


def dealer_com_bulk_page_size() -> int:
    raw = (os.environ.get("INVENTORY_PAGE_SIZE") or "500").strip()
    try:
        n = int(raw)
    except ValueError:
        n = 500
    return max(1, min(500, n))


def is_dealer_com_inventory_post_url(url: str) -> bool:
    u = (url or "").lower()
    return "ws-inv-data/getinventory" in u and u.endswith("getinventory")


def prepare_bulk_post_body(template: dict[str, Any], *, start: int, page_size: int) -> dict[str, Any]:
    body = copy.deepcopy(template)
    prefs = body.setdefault("preferences", {})
    if not isinstance(prefs, dict):
        prefs = {}
        body["preferences"] = prefs
    prefs["pageSize"] = str(page_size)
    inv = body.setdefault("inventoryParameters", {})
    if not isinstance(inv, dict):
        inv = {}
        body["inventoryParameters"] = inv
    inv["start"] = [str(max(0, start))]
    return body


def next_start_offset(
    *,
    start: int,
    page_size: int,
    batch_count: int,
    total_count: int | None,
    unique_so_far: int,
) -> int | None:
    """Return the next start offset, or None when paging is complete."""
    if batch_count <= 0:
        return None
    if batch_count < page_size:
        return None
    if total_count is not None and unique_so_far >= total_count:
        return None
    nxt = start + batch_count
    if total_count is not None and nxt >= total_count:
        return None
    if start // max(page_size, 1) >= _MAX_EXTRA_PAGES:
        return None
    return nxt


def merge_post_template(existing: dict[str, Any] | None, incoming: dict[str, Any]) -> dict[str, Any]:
    """Prefer getInventory bodies; keep the latest capture."""
    if existing is None:
        return incoming
    return incoming


async def fetch_dealer_com_inventory_bulk(
    page: Any,
    api_url: str,
    post_template: dict[str, Any],
    *,
    dealer_name: str = "",
    path: str = "",
) -> list[dict[str, Any]]:
    """
    Re-issue Dealer.com getInventory POSTs with large pageSize and inventoryParameters.start offsets.
    Returns one JSON body per fetched page (deduped by identical vehicle lists).
    """
    page_size = dealer_com_bulk_page_size()
    bodies: list[dict[str, Any]] = []
    seen_vins: set[str] = set()
    start = 0
    total_count: int | None = None

    for _ in range(_MAX_EXTRA_PAGES + 1):
        body = prepare_bulk_post_body(post_template, start=start, page_size=page_size)
        raw = await page.evaluate(_FETCH_JS, {"url": api_url, "body": body})
        if not isinstance(raw, dict) or not raw.get("ok") or not isinstance(raw.get("data"), dict):
            status = raw.get("status") if isinstance(raw, dict) else "?"
            logger.debug(
                "Dealer.com bulk fetch failed [%s]%s start=%s status=%s",
                dealer_name,
                path,
                start,
                status,
            )
            break

        data: dict[str, Any] = raw["data"]
        if total_count is None:
            total_count = get_total_count(data)
            if total_count is None:
                pi = data.get("pageInfo") or {}
                try:
                    tc = pi.get("totalCount")
                    total_count = int(tc) if tc is not None else None
                except (TypeError, ValueError):
                    total_count = None

        vehicles = find_vehicle_list(data) or []
        batch_vins: list[str] = []
        for v in vehicles:
            if not isinstance(v, dict):
                continue
            vin = (v.get("vin") or "").strip().upper()
            if vin:
                batch_vins.append(vin)

        new_vins = [v for v in batch_vins if v not in seen_vins]
        if not batch_vins:
            break

        pi = data.get("pageInfo") or {}
        try:
            effective_page_size = int(pi.get("pageSize") or 0)
        except (TypeError, ValueError):
            effective_page_size = 0
        if effective_page_size <= 0:
            effective_page_size = len(batch_vins)

        bodies.append(data)
        seen_vins.update(batch_vins)

        logger.info(
            "Dealer.com bulk fetch [%s]%s start=%s batch=%d new=%d total=%s cum=%d",
            dealer_name,
            path,
            start,
            len(batch_vins),
            len(new_vins),
            total_count,
            len(seen_vins),
        )

        nxt = next_start_offset(
            start=start,
            page_size=effective_page_size,
            batch_count=len(batch_vins),
            total_count=total_count,
            unique_so_far=len(seen_vins),
        )
        if nxt is None:
            break
        start = nxt

    return bodies


def parse_post_template(post_data: str | None) -> dict[str, Any] | None:
    if not post_data:
        return None
    try:
        parsed = json.loads(post_data)
    except (json.JSONDecodeError, TypeError, ValueError):
        return None
    return parsed if isinstance(parsed, dict) else None


async def nudge_dealer_com_inventory_api(
    page: Any,
    *,
    dealer_name: str = "",
    path: str = "",
) -> None:
    """
    Dealer.com sometimes SSR-renders the first SRP batch without firing getInventory until
    pagination interaction. Click Next once to force the widget POST.
    """
    nudge_selectors = (
        ".pagination-next",
        'button:has-text("Next")',
        'a:has-text("Next")',
        ".pagination a",
    )
    for sel in nudge_selectors:
        try:
            loc = page.locator(sel).first
            if await loc.count() == 0 or not await loc.is_visible():
                continue
            await loc.click(timeout=5000)
            logger.info(
                "Dealer.com template nudge [%s]%s — clicked %s",
                dealer_name,
                path,
                sel,
            )
            return
        except Exception:
            continue


def default_dealer_com_inventory_api_url(page_url: str) -> str:
    try:
        from urllib.parse import urlparse

        parsed = urlparse(page_url or "")
        if parsed.scheme and parsed.netloc:
            return f"{parsed.scheme}://{parsed.netloc}/api/widget/ws-inv-data/getInventory"
    except Exception:
        pass
    return "https://www.irvinebmw.com/api/widget/ws-inv-data/getInventory"


__all__ = [
    "dealer_com_bulk_fetch_enabled",
    "dealer_com_bulk_page_size",
    "default_dealer_com_inventory_api_url",
    "fetch_dealer_com_inventory_bulk",
    "is_dealer_com_inventory_post_url",
    "merge_post_template",
    "next_start_offset",
    "nudge_dealer_com_inventory_api",
    "parse_post_template",
    "prepare_bulk_post_body",
]
