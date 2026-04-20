"""
Official search API for spec backfill tier (last resort).

Uses Google Programmable Search JSON API only — no HTML scraping of SERP pages.
Env (never commit secrets): ``GOOGLE_CSE_API_KEY``, ``GOOGLE_CSE_ID`` (cx).
Optional comma-separated hostnames: ``SPEC_SEARCH_EXTRA_ALLOWED_HOSTS`` for
follow-up HTTP GET of result pages (still allowlist-only).
"""
from __future__ import annotations

import logging
import os
import re
from typing import Any
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)

_DEFAULT_ALLOWED_SUFFIXES = ("fueleconomy.gov", "epa.gov")


def build_spec_search_query(
    year: int | None,
    make: str | None,
    model: str | None,
    trim: str | None,
    *,
    intent: str = "mpg",
) -> str:
    """Human + site-biased query for Programmable Search (unit-tested)."""
    parts: list[str] = []
    if year is not None:
        parts.append(str(year))
    if make:
        parts.append(str(make).strip())
    if model:
        parts.append(str(model).strip())
    if trim and str(trim).strip():
        parts.append(str(trim).strip())
    if intent == "cylinders":
        parts.extend(["EPA", "engine", "cylinders"])
    else:
        parts.extend(["EPA", "fuel economy", "MPG"])
    parts.append("site:fueleconomy.gov")
    return " ".join(parts)


def _extra_allowed_hosts() -> set[str]:
    raw = (os.environ.get("SPEC_SEARCH_EXTRA_ALLOWED_HOSTS") or "").strip()
    if not raw:
        return set()
    return {h.strip().lower() for h in raw.split(",") if h.strip()}


def is_allowed_spec_result_url(url: str) -> bool:
    try:
        host = (urlparse(url).hostname or "").lower()
    except ValueError:
        return False
    if not host:
        return False
    for suf in _DEFAULT_ALLOWED_SUFFIXES:
        if host == suf or host.endswith("." + suf):
            return True
    return host in _extra_allowed_hosts()


def google_custom_search_links(query: str, *, num: int = 5) -> list[dict[str, str]]:
    """
    Returns ``[{"url": "...", "title": "..."}, ...]`` or [] if keys missing / error.
    """
    key = (os.environ.get("GOOGLE_CSE_API_KEY") or "").strip()
    cx = (os.environ.get("GOOGLE_CSE_ID") or "").strip()
    if not key or not cx:
        return []
    params: dict[str, str | int] = {
        "key": key,
        "cx": cx,
        "q": query,
        "num": max(1, min(int(num), 10)),
    }
    try:
        r = requests.get(
            "https://www.googleapis.com/customsearch/v1",
            params=params,
            timeout=22,
            headers={"User-Agent": "DealershipScannerSpecBackfill/1.0"},
        )
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        log.info("Google CSE request failed: %s", e)
        return []
    items = data.get("items") if isinstance(data, dict) else None
    if not isinstance(items, list):
        return []
    out: list[dict[str, str]] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        link = str(it.get("link") or "").strip()
        if not link.startswith("http"):
            continue
        if not is_allowed_spec_result_url(link):
            continue
        out.append({"url": link, "title": str(it.get("title") or "")[:200]})
    return out


def parse_fueleconomy_gov_html(html: str) -> dict[str, Any]:
    """
    Extract city/highway MPG and cylinder count from fueleconomy.gov markup (best-effort).
    """
    out: dict[str, Any] = {}
    if not html:
        return out
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)
    text = re.sub(r"\s+", " ", text)

    mc = re.search(
        r"(?:City|CTY)\s*(?:MPG|mpg|Fuel)[^\d]{0,12}(\d{1,2})\b",
        text,
        re.I,
    )
    mh = re.search(
        r"(?:Highway|Hwy|HW)\s*(?:MPG|mpg|Fuel)[^\d]{0,12}(\d{1,2})\b",
        text,
        re.I,
    )
    if mc:
        try:
            v = int(mc.group(1))
            if 8 <= v <= 90:
                out["mpg_city"] = v
        except ValueError:
            pass
    if mh:
        try:
            v = int(mh.group(1))
            if 8 <= v <= 90:
                out["mpg_highway"] = v
        except ValueError:
            pass
    if "mpg_city" not in out or "mpg_highway" not in out:
        m2 = re.search(
            r"(?:Combined|comb\.?)\s*(?:MPG|mpg)[^\d]{0,14}(\d{1,2})\s+(\d{1,2})",
            text,
            re.I,
        )
        if m2:
            try:
                a, b = int(m2.group(1)), int(m2.group(2))
                if 8 <= a <= 90 and 8 <= b <= 90:
                    out.setdefault("mpg_city", min(a, b))
                    out.setdefault("mpg_highway", max(a, b))
            except ValueError:
                pass
    cyl_m = re.search(
        r"(?:^|[\s,;])(\d)\s*[-\s]*(?:cyl|cylinders?)\b",
        text,
        re.I,
    )
    if cyl_m:
        try:
            out["cylinders"] = int(cyl_m.group(1))
        except ValueError:
            pass
    return out


def fetch_url_html(url: str, *, timeout: int = 25) -> str | None:
    if not is_allowed_spec_result_url(url):
        return None
    try:
        r = requests.get(
            url,
            timeout=timeout,
            headers={"User-Agent": "DealershipScannerSpecBackfill/1.0"},
        )
        r.raise_for_status()
        return r.text
    except Exception as e:
        log.debug("fetch_url_html failed %s: %s", url[:100], e)
        return None
