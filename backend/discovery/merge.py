"""
Merge and deduplicate candidates across DMV / OSM / web tiers.

Uses the same fuzzy threshold as ``dealerships_db.deduplicate_dealerships`` (88).
"""
from __future__ import annotations

import re
from typing import Sequence

from thefuzz import fuzz

from backend.discovery.candidate import DealerCandidate
from backend.discovery.normalize import normalize_url, normalize_zip

# Keep aligned with backend.db.dealerships_db._DEDUPE_THRESHOLD
_DEDUPE_THRESHOLD = 88


def _norm_key(name: str, url: str, city: str, state: str) -> str:
    u = re.sub(r"^https?://(www\.)?", "", (url or "").lower()).rstrip("/")
    return " ".join(
        [
            (name or "").lower().strip(),
            u,
            (city or "").lower().strip(),
            (state or "").upper().strip(),
        ]
    )


def _pick_zip(a: str, b: str) -> str:
    za, zb = normalize_zip(a), normalize_zip(b)
    if za and zb and za == zb:
        return za
    if za:
        return za
    if zb:
        return zb
    return za or zb or ""


def _merge_two(a: DealerCandidate, b: DealerCandidate) -> DealerCandidate:
    """Prefer DMV address parts; OSM coordinates + osm_id; best HTTPS URL."""
    dmv_first = b if b.source_dmv and not a.source_dmv else (a if a.source_dmv else None)
    osm_first = b if b.source_osm and not a.source_osm else (a if a.source_osm else None)
    base = dmv_first or osm_first or a
    other = b if base is a else a

    street = (base.street_address or other.street_address or "").strip()
    if base.source_dmv and base.street_address:
        street = base.street_address.strip()
    elif other.source_dmv and other.street_address:
        street = other.street_address.strip()

    z = _pick_zip(base.zip_code, other.zip_code)
    if base.source_dmv and normalize_zip(base.zip_code):
        z = normalize_zip(base.zip_code) or z
    elif other.source_dmv and normalize_zip(other.zip_code):
        z = normalize_zip(other.zip_code) or z
    else:
        z = normalize_zip(z) or z or ""

    city = (base.city or other.city or "").strip()
    state = (base.state or other.state or "").strip().upper()[:2]

    lat = base.latitude if base.latitude is not None else other.latitude
    lon = base.longitude if base.longitude is not None else other.longitude
    if osm_first:
        if osm_first.latitude is not None:
            lat = osm_first.latitude
        if osm_first.longitude is not None:
            lon = osm_first.longitude

    url_a = normalize_url(a.dealer_website_url or a.website_url)
    url_b = normalize_url(b.dealer_website_url or b.website_url)
    url = url_a or url_b or ""
    if a.source_web and url_a:
        url = url_a
    elif b.source_web and url_b:
        url = url_b

    osm_id = a.osm_id or b.osm_id

    name = base.name.strip() or other.name.strip()

    return DealerCandidate(
        name=name,
        city=city,
        state=state,
        street_address=street,
        zip_code=z,
        latitude=lat,
        longitude=lon,
        dealer_website_url=url,
        website_url=url,
        osm_id=osm_id,
        source_dmv=a.source_dmv or b.source_dmv,
        source_osm=a.source_osm or b.source_osm,
        source_web=a.source_web or b.source_web,
    )


def merge_and_dedupe(candidates: Sequence[DealerCandidate]) -> list[DealerCandidate]:
    """Greedy clustering on fuzzy name+url+city+state and matching ``osm_id``."""
    items = list(candidates)
    n = len(items)
    if n <= 1:
        return list(items)

    parent = list(range(n))

    def find(i: int) -> int:
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    def union(i: int, j: int) -> None:
        ri, rj = find(i), find(j)
        if ri != rj:
            parent[rj] = ri

    # Union by osm_id
    osm_map: dict[str, int] = {}
    for i, c in enumerate(items):
        if c.osm_id:
            oid = c.osm_id
            if oid in osm_map:
                union(i, osm_map[oid])
            else:
                osm_map[oid] = i

    for i in range(n):
        for j in range(i + 1, n):
            a, b = items[i], items[j]
            if a.osm_id and b.osm_id and a.osm_id == b.osm_id:
                union(i, j)
                continue
            if (a.state or "").upper() != (b.state or "").upper():
                continue
            ka = _norm_key(
                a.name, a.dealer_website_url or a.website_url, a.city, a.state
            )
            kb = _norm_key(
                b.name, b.dealer_website_url or b.website_url, b.city, b.state
            )
            if fuzz.token_set_ratio(ka, kb) >= _DEDUPE_THRESHOLD:
                union(i, j)

    clusters: dict[int, list[int]] = {}
    for i in range(n):
        r = find(i)
        clusters.setdefault(r, []).append(i)

    merged: list[DealerCandidate] = []
    for _root, idxs in clusters.items():
        acc = items[idxs[0]]
        for k in idxs[1:]:
            acc = _merge_two(acc, items[k])
        merged.append(acc)
    return merged
