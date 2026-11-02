"""One shared test for "this image is a placeholder, not a photo of the car" (N6.8, D-N15).

Used by the parser URL filter (``backend/parsers/base.py``), by listing completeness
(a placeholder-only car counts as "no photo") and by the per-dealer photo-coverage alarm.

Two signals:
* URL tokens that dealers and platforms use for "photo unavailable" art.
* The same image on many cars of one dealer (``dealer_context``): a real photo belongs
  to one car, a stand-in is shared by design.
"""
from __future__ import annotations

import json
from collections import Counter
from typing import Any, Iterable, Mapping

# Lower-case substrings. ``/static/placeholder.svg`` is our own stand-in; the others are
# dealer/platform art measured on the fresh scan (unavailable_stockphoto 984 cars at 52
# dealers, photo_unavailable 931, Chapman /default.jpg 416, photo-not-available 75).
PLACEHOLDER_TOKENS: tuple[str, ...] = (
    "photo_unavailable",
    "unavailable_stockphoto",
    "photo-not-available",
    "photo_not_available",
    "coming_soon",
    "comingsoon",
    "default.jpg",
    "/static/placeholder.svg",
)

# One URL on this many cars of a dealer is a stand-in (matches the batch rule in
# scanner/vdp/prefetch.drop_shared_gallery_urls: 3 others + itself).
SHARED_IMAGE_MIN_CARS = 4

# Per-dealer alarm: a dealer with at least this many active cars whose placeholder-only
# or photo-less share is at or above the fraction.
ALARM_MIN_CARS = 10
ALARM_MIN_FRACTION = 0.5


def has_placeholder_token(url: Any) -> bool:
    if not url or not isinstance(url, str):
        return False
    u = url.lower()
    return any(tok in u for tok in PLACEHOLDER_TOKENS)


def shared_image_context(rows: Iterable[Mapping[str, Any]], min_cars: int = SHARED_IMAGE_MIN_CARS) -> dict[str, Any]:
    """Build the ``dealer_context`` for one dealer: how many cars carry each image URL."""
    counts: Counter[str] = Counter()
    for r in rows:
        counts.update(set(_row_image_urls(r)))
    return {"counts": counts, "min_cars": min_cars}


def is_placeholder_image(url: Any, dealer_context: Mapping[str, Any] | None = None) -> bool:
    """True when ``url`` is not a photo of the car.

    ``dealer_context`` (from :func:`shared_image_context`) adds the shared-image test.
    Without it only the token test runs.
    """
    if not url or not isinstance(url, str) or not url.strip():
        return False
    if has_placeholder_token(url):
        return True
    if dealer_context:
        counts = dealer_context.get("counts") or {}
        if counts.get(url.strip(), 0) >= int(dealer_context.get("min_cars") or SHARED_IMAGE_MIN_CARS):
            return True
    return False


def _gallery_list(g: Any) -> list[str]:
    if isinstance(g, str) and g.strip().startswith("["):
        try:
            g = json.loads(g)
        except (TypeError, ValueError):
            return []
    return [u for u in g if isinstance(u, str)] if isinstance(g, list) else []


def _row_image_urls(row: Mapping[str, Any]) -> list[str]:
    urls = []
    img = row.get("image_url")
    if isinstance(img, str) and img.strip():
        urls.append(img.strip())
    urls.extend(u.strip() for u in _gallery_list(row.get("gallery")) if u.strip())
    return urls


def has_real_photo(row: Mapping[str, Any], dealer_context: Mapping[str, Any] | None = None) -> bool:
    """True when the row has at least one http(s) or local car-image URL that is not a placeholder."""
    for u in _row_image_urls(row):
        if not (u.startswith("http") or u.startswith("/car-images/")):
            continue
        if not is_placeholder_image(u, dealer_context):
            return True
    return False


def dealer_photo_alarm(
    rows: Iterable[Mapping[str, Any]],
    *,
    min_cars: int = ALARM_MIN_CARS,
    min_fraction: float = ALARM_MIN_FRACTION,
) -> dict[str, Any]:
    """Photo coverage for one dealer's active cars.

    Returns ``{"cars", "no_real_photo", "placeholder_only", "fraction", "alarm"}``.
    ``placeholder_only`` counts cars that have images but all are placeholders; the
    alarm trips when the no-real-photo share reaches ``min_fraction`` on ``min_cars``+ cars.
    """
    rows = list(rows)
    ctx = shared_image_context(rows)
    none = ph_only = 0
    for r in rows:
        if has_real_photo(r, ctx):
            continue
        none += 1
        if _row_image_urls(r):
            ph_only += 1
    n = len(rows)
    frac = (none / n) if n else 0.0
    return {
        "cars": n,
        "no_real_photo": none,
        "placeholder_only": ph_only,
        "fraction": round(frac, 4),
        "alarm": n >= min_cars and frac >= min_fraction,
    }


def photo_alarms_by_dealer(rows: Iterable[Mapping[str, Any]], **kw: Any) -> dict[str, dict[str, Any]]:
    """Group rows by ``dealer_id`` and return the alarm record of each dealer."""
    groups: dict[str, list[Mapping[str, Any]]] = {}
    for r in rows:
        groups.setdefault(str(r.get("dealer_id") or "?"), []).append(r)
    return {d: dealer_photo_alarm(g, **kw) for d, g in groups.items()}
