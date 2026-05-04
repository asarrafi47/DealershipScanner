"""Extract Next.js ``__NEXT_DATA__`` payload for inventory parsing fallbacks."""
from __future__ import annotations

import json
import re
from typing import Any


_NEXT_DATA_RE = re.compile(
    r'<script[^>]*\bid\s*=\s*["\']__NEXT_DATA__["\'][^>]*>(?P<body>.*?)</script>',
    re.IGNORECASE | re.DOTALL,
)


def parse_next_data_json_from_html(html: str) -> Any | None:
    """Return parsed JSON from ``__NEXT_DATA__`` script tag, or None."""
    if not html or "__NEXT_DATA__" not in html:
        return None
    m = _NEXT_DATA_RE.search(html)
    if not m:
        return None
    raw = (m.group("body") or "").strip()
    if not raw:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


async def fetch_next_data_json_from_page(page: Any) -> Any | None:
    """DOM read of ``#__NEXT_DATA__`` (handles HTML entities vs regex on ``page.content()``)."""
    try:
        return await page.evaluate(
            """() => {
                const el = document.getElementById('__NEXT_DATA__');
                if (!el) return null;
                const t = el.textContent;
                if (!t || !t.trim()) return null;
                try { return JSON.parse(t); } catch (e) { return null; }
            }"""
        )
    except Exception:
        return None
