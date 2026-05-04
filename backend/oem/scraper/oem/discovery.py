"""Discover structured data URLs from OEM locator HTML and JS bundles."""
from __future__ import annotations

import json
import re
from typing import Any
from urllib.parse import urljoin

_JSON_IN_SCRIPT = re.compile(
    r'<script[^>]+(?:type="application/json"|id="__NEXT_DATA__")[^>]*>([\s\S]*?)</script>',
    re.I,
)
_SCRIPT_SRC = re.compile(r'<script[^>]+src=["\']([^"\']+)["\']', re.I)
_URL_LIKE = re.compile(
    r"https?://[a-zA-Z0-9][a-zA-Z0-9\-._~:/?#\[\]@!$&'()*+,;=%]+"
)


def extract_embedded_json_blobs(html: str) -> list[tuple[str, Any]]:
    """Parse __NEXT_DATA__ / application/json script tags."""
    out: list[tuple[str, Any]] = []
    for m in _JSON_IN_SCRIPT.finditer(html):
        raw = m.group(1).strip()
        if not raw:
            continue
        try:
            out.append(("embedded_script_tag", json.loads(raw)))
        except json.JSONDecodeError:
            continue
    return out


def collect_script_src(html: str, base_url: str) -> list[str]:
    urls: list[str] = []
    for m in _SCRIPT_SRC.finditer(html):
        src = m.group(1).strip()
        if not src or src.startswith("data:"):
            continue
        urls.append(urljoin(base_url, src))
    return urls


def find_urls_in_text(text: str, *, host_substrings: tuple[str, ...]) -> list[str]:
    found: list[str] = []
    for m in _URL_LIKE.finditer(text):
        u = m.group(0).rstrip("\\\"')")
        low = u.lower()
        if not any(h in low for h in host_substrings):
            continue
        if any(
            x in low
            for x in (
                ".json",
                "/json",
                "graphql",
                "/api/",
                "/bin/",
                "/services/",
                "dealer",
                "locator",
                "outlet",
            )
        ):
            found.append(u.split("?")[0] if "?" in u else u)
    return list(dict.fromkeys(found))


def mine_urls_from_js_bundle(
    bundle_text: str,
    *,
    host_substrings: tuple[str, ...] = ("bmwusa.com", "bmw.com", "cloudfront.net"),
) -> list[str]:
    """Heuristic extraction of API/asset URLs from minified JS."""
    return find_urls_in_text(bundle_text, host_substrings=host_substrings)
