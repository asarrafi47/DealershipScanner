"""
Pure helpers for merging HTTPS gallery URL batches (VDP harvest rounds).

Used by scanner_vdp gallery interaction loop tests and dedupe between DOM / network.
"""
from __future__ import annotations

from typing import Callable

from backend.parsers.base import normalize_image_url_https


def merge_https_url_batches(
    ordered: list[str],
    seen: set[str],
    batch: list[str],
    *,
    normalize: Callable[[str], str] | None = None,
    max_total: int | None = None,
) -> int:
    """
    Append normalized HTTPS URLs from *batch* into *ordered* if not in *seen*.
    Mutates *ordered* and *seen*. Returns count of newly added URLs.
    """
    norm_fn = normalize or normalize_image_url_https
    added = 0
    for raw in batch:
        if not isinstance(raw, str):
            continue
        u = norm_fn(raw.strip())
        if not u.startswith("https://"):
            continue
        if u in seen:
            continue
        if max_total is not None and len(ordered) >= max_total:
            break
        seen.add(u)
        ordered.append(u)
        added += 1
    return added
