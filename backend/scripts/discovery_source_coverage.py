#!/usr/bin/env python3
"""
Measure how much of the live dealership registry each KEYLESS discovery source would
have found on its own — metro by metro, bounded, and checkpointed after every metro.

Sources measured
----------------
overture : Overture Maps Places (GeoParquet on S3, read with DuckDB). No key, no quota.
osm      : OpenStreetMap via Overpass. No key.
dmv      : State DMV licensed-dealer registries (``backend/discovery/dmv``). No key.

Why: ``backend/discovery/pipeline.py`` defaults to Google Places, which needs a billable
API key. This script answers whether the keyless tiers cover enough of the registry —
and specifically enough of the rows that carry ACTIVE INVENTORY — for Google to be
dropped outright.

Bounded by construction
-----------------------
Every source is queried per metro inside a bounding box. Overture uses the ``bbox``
struct column so DuckDB prunes Parquet row groups instead of scanning the national
extract. Overpass gets one bounded bbox per metro with the module's own pacing.
The output JSON is rewritten after each metro, so an interrupted run still leaves a
usable partial measurement on disk.

Nothing here writes to the database. It reads ``dealerships`` + ``cars`` only.

Provenance: every candidate carries ``source`` + a re-checkable locator (Overture id,
OSM ``n/123``, DMV file#row). Candidates without a locator are dropped, never guessed at.

Usage::

    .venv/bin/python -m backend.scripts.discovery_source_coverage
    .venv/bin/python -m backend.scripts.discovery_source_coverage --metros OC,CHA
    .venv/bin/python -m backend.scripts.discovery_source_coverage --sources overture
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import os
import re
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Sequence
from urllib.parse import urlparse

logger = logging.getLogger("discovery_source_coverage")

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUT = REPO_ROOT / "workspace" / "discovery_coverage.json"

EARTH_RADIUS_MI = 3958.7613

# A source row may only match a registry row on name when the two points are this
# close. Website-host equality is accepted regardless of distance within the metro.
NAME_MATCH_MAX_MILES = 3.0
# Pad the fetch bbox beyond the metro circle so edge dealers are not clipped.
BBOX_PAD_MILES = 2.0
# Courtesy pause between metros when hitting Overpass.
OVERPASS_SLEEP_S = 3.0


# --------------------------------------------------------------------------- #
# metros
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class Metro:
    key: str
    label: str
    state: str
    lat: float
    lon: float
    radius_mi: float = 20.0

    def bbox(self, pad_mi: float = BBOX_PAD_MILES) -> tuple[float, float, float, float]:
        """(west, south, east, north) covering the circle plus *pad_mi*."""
        r = self.radius_mi + pad_mi
        dlat = r / 69.172
        cos_lat = max(abs(math.cos(math.radians(self.lat))), 0.2)
        dlon = r / (69.172 * cos_lat)
        return (self.lon - dlon, self.lat - dlat, self.lon + dlon, self.lat + dlat)


# Centres come from a greedy 30-mile clustering of the 591 registry rows that carry
# coordinates (1 of 592 has none), snapped to the conventional metro centre. One
# 20-mile radius everywhere keeps each Overpass bbox inside what the public instances
# answer in a single request. Chattanooga TN and Orange County CA are included because
# they are the two metros with ground truth. ``--derive-metros`` re-runs the clustering
# and prints it, so this list can be re-checked rather than trusted.
METROS: tuple[Metro, ...] = (
    Metro("OC", "Orange County", "CA", 33.7455, -117.8677),
    Metro("CHA", "Chattanooga", "TN", 35.0456, -85.3097),
    Metro("CLT", "Charlotte", "NC", 35.2271, -80.8431),
    Metro("PHX", "Phoenix / Scottsdale", "AZ", 33.5039, -112.0051),
    Metro("SD", "San Diego", "CA", 32.8649, -117.1108),
    Metro("DFW", "Dallas / Fort Worth", "TX", 32.7792, -97.1013),
    Metro("HSV", "Huntsville", "AL", 34.7448, -86.6701),
    Metro("SEA", "Seattle / Bellevue", "WA", 47.6300, -122.2538),
    Metro("AUS", "Austin", "TX", 30.2672, -97.7431),
    Metro("SAT", "San Antonio", "TX", 29.4241, -98.4936),
)


# --------------------------------------------------------------------------- #
# geo
# --------------------------------------------------------------------------- #
def haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = p2 - p1
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * EARTH_RADIUS_MI * math.asin(math.sqrt(min(1.0, a)))


# --------------------------------------------------------------------------- #
# website helpers — thin wrappers over backend.discovery.normalize
# --------------------------------------------------------------------------- #
def host_of(url: str | None) -> str:
    """Lowercased hostname without ``www.``; '' when unusable or an aggregator."""
    from backend.discovery.normalize import normalize_url

    nu = normalize_url(url)
    if not nu:
        return ""
    try:
        host = urlparse(nu).netloc.lower()
    except ValueError:
        return ""
    return host.split("@")[-1].split(":")[0].removeprefix("www.")


def usable_website(url: str | None) -> str | None:
    """A URL the scanner could actually be pointed at, or None."""
    from backend.discovery.normalize import looks_like_dealer_website, normalize_url

    nu = normalize_url(url)
    if not nu or not looks_like_dealer_website(nu):
        return None
    return nu


# --------------------------------------------------------------------------- #
# registry
# --------------------------------------------------------------------------- #
@dataclass
class RegistryRow:
    id: int
    name: str
    website: str | None
    host: str
    city: str
    state: str
    lat: float | None
    lon: float | None
    has_active_inventory: bool


def _inventory_dsn() -> str:
    dsn = (os.environ.get("INVENTORY_DATABASE_URL") or "").strip()
    if dsn:
        return dsn
    env_path = REPO_ROOT / ".env"
    if env_path.is_file():
        m = re.search(r"^INVENTORY_DATABASE_URL=(.+)$", env_path.read_text(), re.M)
        if m:
            return m.group(1).strip().strip("'\"")
    raise RuntimeError("INVENTORY_DATABASE_URL is not set and .env has no entry for it")


def load_registry() -> list[RegistryRow]:
    """Every ``dealerships`` row, flagged with whether it currently holds live listings."""
    import psycopg

    with psycopg.connect(_inventory_dsn()) as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, name, website_url, dealer_website_url, city, state, latitude, longitude "
            "FROM dealerships"
        )
        raw = cur.fetchall()
        cur.execute(
            "SELECT DISTINCT dealership_registry_id FROM cars "
            "WHERE listing_active = 1 AND dealership_registry_id IS NOT NULL"
        )
        active_ids = {int(r[0]) for r in cur.fetchall()}

    rows: list[RegistryRow] = []
    for (rid, name, wurl, dwurl, city, state, lat, lon) in raw:
        site = (dwurl or wurl or "").strip() or None
        rows.append(
            RegistryRow(
                id=int(rid),
                name=(name or "").strip(),
                website=site,
                host=host_of(site),
                city=(city or "").strip(),
                state=(state or "").strip().upper()[:2],
                lat=float(lat) if lat is not None else None,
                lon=float(lon) if lon is not None else None,
                has_active_inventory=int(rid) in active_ids,
            )
        )
    return rows


def registry_in_metro(rows: Sequence[RegistryRow], metro: Metro) -> list[RegistryRow]:
    out = []
    for r in rows:
        if r.lat is None or r.lon is None:
            continue
        if haversine_miles(metro.lat, metro.lon, r.lat, r.lon) <= metro.radius_mi:
            out.append(r)
    return out


def derive_metros(rows: Sequence[RegistryRow], radius_mi: float = 30.0, min_size: int = 4) -> list[dict[str, Any]]:
    """Greedy clustering of registry coordinates — used to justify/re-check ``METROS``."""
    pts = [(r.lat, r.lon, r.city, r.state) for r in rows if r.lat is not None and r.lon is not None]
    used = [False] * len(pts)
    clusters: list[dict[str, Any]] = []
    for i, p in enumerate(pts):
        if used[i]:
            continue
        members = [j for j, q in enumerate(pts) if not used[j] and haversine_miles(p[0], p[1], q[0], q[1]) <= radius_mi]
        if len(members) < min_size:
            continue
        for j in members:
            used[j] = True
        clusters.append(
            {
                "n": len(members),
                "lat": round(sum(pts[j][0] for j in members) / len(members), 4),
                "lon": round(sum(pts[j][1] for j in members) / len(members), 4),
                "seed_city": pts[members[0]][2],
                "seed_state": pts[members[0]][3],
            }
        )
    clusters.sort(key=lambda c: -c["n"])
    return clusters


# --------------------------------------------------------------------------- #
# candidates
# --------------------------------------------------------------------------- #
@dataclass
class Candidate:
    source: str          # "overture" | "osm" | "dmv"
    locator: str         # re-checkable id — Overture id, OSM n/123, DMV file#row
    name: str
    website: str | None
    city: str
    state: str
    lat: float | None
    lon: float | None
    extra: dict[str, Any] = field(default_factory=dict)

    def as_json(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "locator": self.locator,
            "name": self.name,
            "website": self.website,
            "city": self.city,
            "state": self.state,
            "latitude": self.lat,
            "longitude": self.lon,
        }


# --------------------------------------------------------------------------- #
# source: Overture (bounded by bbox)
# --------------------------------------------------------------------------- #
def fetch_overture(con, release_id: str, metro: Metro, *, category_mode: str = "car_dealer") -> list[Candidate]:
    from backend.discovery.overture_discovery import BBox, fetch_car_dealers_in_bbox

    west, south, east, north = metro.bbox()
    _rid, raw = fetch_car_dealers_in_bbox(
        con, BBox(west, south, east, north), release_id=release_id, category_mode=category_mode
    )

    out: list[Candidate] = []
    seen: set[str] = set()
    for r in raw:
        oid = str(r.get("overture_id") or "").strip()
        name = (r.get("name") or "").strip()
        lat, lon = r.get("latitude"), r.get("longitude")
        if not oid or not name or lat is None or lon is None:
            continue  # no provenance / no name / no position -> never invent a row
        if oid in seen:
            continue
        if haversine_miles(metro.lat, metro.lon, float(lat), float(lon)) > metro.radius_mi:
            continue
        seen.add(oid)
        out.append(
            Candidate(
                source="overture",
                locator=oid,
                name=name,
                website=(r.get("website") or None),
                city=(r.get("city") or "").strip(),
                state=(r.get("state") or "").strip().upper()[:2],
                lat=float(lat),
                lon=float(lon),
                extra={"category_primary": r.get("category_primary"), "confidence": r.get("confidence")},
            )
        )
    return out


# --------------------------------------------------------------------------- #
# source: OSM / Overpass (one bounded bbox per metro)
# --------------------------------------------------------------------------- #
def fetch_osm(metro: Metro, *, timeout_s: float = 180.0) -> list[Candidate]:
    from backend.discovery.osm import fetch_osm_dealerships

    found = fetch_osm_dealerships(
        metro.lat, metro.lon, metro.radius_mi, timeout_s=timeout_s, attempts=2
    )
    out: list[Candidate] = []
    seen: set[str] = set()
    for c in found:
        if not c.osm_id or not (c.name or "").strip() or c.osm_id in seen:
            continue
        seen.add(c.osm_id)
        out.append(
            Candidate(
                source="osm",
                locator=c.osm_id,
                name=c.name.strip(),
                website=(c.dealer_website_url or c.website_url or None),
                city=(c.city or "").strip(),
                state=(c.state or "").strip().upper()[:2],
                lat=c.latitude,
                lon=c.longitude,
            )
        )
    return out


# --------------------------------------------------------------------------- #
# source: DMV
# --------------------------------------------------------------------------- #
def fetch_dmv(metro: Metro) -> tuple[list[Candidate], str]:
    """
    DMV tier for the metro's state. Returns ``(candidates, status)``.

    ``status`` distinguishes "no loader implemented for this state" from "loader exists
    but its bulk file is not present" from an actual row count — zero rows for a missing
    loader is not a coverage result and must not be reported as one.
    """
    from backend.discovery.dmv import STATE_LOADERS, fetch_dmv_records

    st = metro.state
    if st not in STATE_LOADERS:
        return [], f"no_loader_implemented:{st}"
    recs = fetch_dmv_records(st, REPO_ROOT)
    if not recs:
        return [], f"loader_present_but_no_local_bulk_file:{st}"

    out: list[Candidate] = []
    for n, rec in enumerate(recs):
        name = (getattr(rec, "business_name", "") or "").strip()
        if not name:
            continue
        out.append(
            Candidate(
                source="dmv",
                locator=f"dmv:{st}#{n}",
                name=name,
                website=getattr(rec, "website", None),
                city=(getattr(rec, "city", "") or "").strip(),
                state=(getattr(rec, "state", "") or st).strip().upper()[:2],
                lat=None,
                lon=None,  # DMV bulk files carry addresses, not coordinates
            )
        )
    return out, f"loaded:{st}:{len(out)}"


# --------------------------------------------------------------------------- #
# matching — the house rule, not a new one
# --------------------------------------------------------------------------- #
def match_to_registry(
    cands: Sequence[Candidate],
    registry: Sequence[RegistryRow],
) -> tuple[dict[int, list[Candidate]], list[Candidate], dict[str, int]]:
    """
    Match candidates onto registry rows.

    Order, strongest first:
      1. ``host``      — normalized website hosts are equal. ``normalize.normalize_url``
                         has already rejected aggregators/social, so a surviving host is
                         the dealer's own domain and is the highest-signal key we have.
      2. ``geo_name``  — ``merge._norm_key`` + ``thefuzz.token_set_ratio`` clears
                         ``merge._DEDUPE_THRESHOLD`` (88, the same threshold
                         ``dealerships_db.deduplicate_dealerships`` uses) AND the two
                         points are within ``NAME_MATCH_MAX_MILES``.
      3. ``city_name`` — same fuzzy threshold with same city+state, for DMV rows which
                         have no coordinates at all.

    Returns ``(registry_id -> candidates, unmatched, method counts)``.
    """
    from thefuzz import fuzz

    from backend.discovery.merge import _DEDUPE_THRESHOLD, _norm_key

    by_host: dict[str, list[RegistryRow]] = {}
    for r in registry:
        if r.host:
            by_host.setdefault(r.host, []).append(r)

    with_coords = [r for r in registry if r.lat is not None and r.lon is not None]
    reg_key = {r.id: _norm_key(r.name, r.website or "", r.city, r.state) for r in registry}

    matched: dict[int, list[Candidate]] = {}
    unmatched: list[Candidate] = []
    methods = {"host": 0, "geo_name": 0, "city_name": 0}

    for c in cands:
        hit: RegistryRow | None = None
        method = ""

        ch = host_of(c.website)
        if ch and ch in by_host:
            hit = by_host[ch][0]
            method = "host"

        ckey = _norm_key(c.name, c.website or "", c.city, c.state)

        if hit is None and c.lat is not None and c.lon is not None:
            best: tuple[int, float, RegistryRow] | None = None
            for r in with_coords:
                d = haversine_miles(c.lat, c.lon, r.lat, r.lon)  # type: ignore[arg-type]
                if d > NAME_MATCH_MAX_MILES:
                    continue
                score = int(fuzz.token_set_ratio(ckey, reg_key[r.id]))
                if score >= _DEDUPE_THRESHOLD and (best is None or score > best[0] or (score == best[0] and d < best[1])):
                    best = (score, d, r)
            if best is not None:
                hit, method = best[2], "geo_name"

        if hit is None and c.lat is None and c.city:
            ccity = c.city.strip().lower()
            for r in registry:
                if r.city.strip().lower() != ccity or r.state != c.state:
                    continue
                if int(fuzz.token_set_ratio(ckey, reg_key[r.id])) >= _DEDUPE_THRESHOLD:
                    hit, method = r, "city_name"
                    break

        if hit is None:
            unmatched.append(c)
        else:
            matched.setdefault(hit.id, []).append(c)
            methods[method] += 1

    return matched, unmatched, methods


# --------------------------------------------------------------------------- #
# per-source scoring
# --------------------------------------------------------------------------- #
def summarize_source(
    source: str,
    cands: Sequence[Candidate],
    registry: Sequence[RegistryRow],
) -> dict[str, Any]:
    from backend.discovery.non_dealer_filter import is_probable_non_dealer
    from backend.discovery.overture_discovery import is_franchised_dealer

    matched, unmatched, methods = match_to_registry(cands, registry)

    active_ids = {r.id for r in registry if r.has_active_inventory}
    matched_active = {rid for rid in matched if rid in active_ids}

    # "found WITH a usable website": at least one matching candidate carries a
    # scanner-usable URL. That is the figure that decides whether a source can
    # actually feed the scanner — a dealer with no website is not actionable.
    matched_with_site = {rid for rid, cs in matched.items() if any(usable_website(c.website) for c in cs)}
    matched_active_with_site = matched_with_site & active_ids

    junk = [c for c in unmatched if is_probable_non_dealer(c.name, c.website)]
    junk_locators = {c.locator for c in junk}
    new_plausible = [c for c in unmatched if c.locator not in junk_locators]
    new_plausible_with_site = [c for c in new_plausible if usable_website(c.website)]
    # The registry is a franchise-dealer roster, so the OEM-branded slice of the "new"
    # pool is the part actually comparable to it; the rest is mostly independent lots.
    new_franchise = [c for c in new_plausible if is_franchised_dealer(c.name)]
    new_franchise_with_site = [c for c in new_franchise if usable_website(c.website)]

    with_site_all = [c for c in cands if usable_website(c.website)]
    n_reg = len(registry)

    def pct(num: int, den: int) -> float | None:
        return round(100.0 * num / den, 1) if den else None

    return {
        "source": source,
        "candidates_fetched": len(cands),
        "candidates_with_usable_website": len(with_site_all),
        "candidate_website_yield_pct": pct(len(with_site_all), len(cands)),
        # (a) registry coverage
        "registry_total": n_reg,
        "registry_matched": len(matched),
        "registry_matched_pct": pct(len(matched), n_reg),
        # (b) the commercially relevant subset
        "active_total": len(active_ids),
        "active_matched": len(matched_active),
        "active_matched_pct": pct(len(matched_active), len(active_ids)),
        # (e) website coverage on what it found
        "registry_matched_with_usable_website": len(matched_with_site),
        "registry_matched_with_usable_website_pct": pct(len(matched_with_site), n_reg),
        "active_matched_with_usable_website": len(matched_active_with_site),
        "active_matched_with_usable_website_pct": pct(len(matched_active_with_site), len(active_ids)),
        # (c) new candidates
        "new_plausible": len(new_plausible),
        "new_plausible_with_usable_website": len(new_plausible_with_site),
        "new_franchise_branded": len(new_franchise),
        "new_franchise_branded_with_usable_website": len(new_franchise_with_site),
        # (d) junk
        "junk_filtered": len(junk),
        "match_methods": methods,
        "_matched_ids": sorted(matched),
        "_matched_with_site_ids": sorted(matched_with_site),
        "missed_active_registry_ids": sorted(active_ids - set(matched)),
        "sample_new_franchise": [c.as_json() for c in new_franchise_with_site[:10]],
        "sample_new_plausible": [c.as_json() for c in new_plausible_with_site[:10]],
        "sample_junk": [c.as_json() for c in junk[:10]],
    }


def combine(per_source: dict[str, dict[str, Any]], registry: Sequence[RegistryRow]) -> dict[str, Any]:
    """Union of registry ids found by any source that actually returned rows."""
    active_ids = {r.id for r in registry if r.has_active_inventory}
    union: set[int] = set()
    union_site: set[int] = set()
    for b in per_source.values():
        union.update(b.get("_matched_ids") or [])
        union_site.update(b.get("_matched_with_site_ids") or [])

    def pct(num: int, den: int) -> float | None:
        return round(100.0 * num / den, 1) if den else None

    return {
        "registry_total": len(registry),
        "registry_matched": len(union),
        "registry_matched_pct": pct(len(union), len(registry)),
        "registry_matched_with_usable_website": len(union_site),
        "registry_matched_with_usable_website_pct": pct(len(union_site), len(registry)),
        "active_total": len(active_ids),
        "active_matched": len(union & active_ids),
        "active_matched_pct": pct(len(union & active_ids), len(active_ids)),
        "active_matched_with_usable_website": len(union_site & active_ids),
        "active_matched_with_usable_website_pct": pct(len(union_site & active_ids), len(active_ids)),
        "missed_active_registry_ids": sorted(active_ids - union),
    }


# --------------------------------------------------------------------------- #
# totals across metros
# --------------------------------------------------------------------------- #
_SUM_KEYS = (
    "candidates_fetched",
    "candidates_with_usable_website",
    "registry_total",
    "registry_matched",
    "registry_matched_with_usable_website",
    "active_total",
    "active_matched",
    "active_matched_with_usable_website",
    "new_plausible",
    "new_plausible_with_usable_website",
    "new_franchise_branded",
    "new_franchise_branded_with_usable_website",
    "junk_filtered",
)


def totals(doc: dict[str, Any]) -> dict[str, Any]:
    """Sum per-metro blocks. Metros are disjoint circles, so registry rows do not double-count."""
    metros = doc.get("metros") or {}
    reg_total = sum(m["registry_in_metro"] for m in metros.values())
    act_total = sum(m["registry_active_in_metro"] for m in metros.values())

    def pct(num: int, den: int) -> float | None:
        return round(100.0 * num / den, 1) if den else None

    out: dict[str, Any] = {
        "metros_measured": len(metros),
        "registry_in_sampled_metros": reg_total,
        "registry_active_in_sampled_metros": act_total,
        "per_source": {},
    }
    for src in ("overture", "overture_wide", "osm", "dmv"):
        agg = dict.fromkeys(_SUM_KEYS, 0)
        metros_with_data = 0
        for m in metros.values():
            b = (m.get("sources") or {}).get(src) or {}
            if "registry_matched" not in b:
                continue
            metros_with_data += 1
            for k in _SUM_KEYS:
                agg[k] += int(b.get(k) or 0)
        if not metros_with_data:
            continue
        agg["metros_with_data"] = metros_with_data
        agg["registry_matched_pct"] = pct(agg["registry_matched"], reg_total)
        agg["active_matched_pct"] = pct(agg["active_matched"], act_total)
        agg["registry_matched_with_usable_website_pct"] = pct(agg["registry_matched_with_usable_website"], reg_total)
        agg["active_matched_with_usable_website_pct"] = pct(agg["active_matched_with_usable_website"], act_total)
        out["per_source"][src] = agg

    c = dict.fromkeys(
        ("registry_matched", "registry_matched_with_usable_website", "active_matched", "active_matched_with_usable_website"), 0
    )
    for m in metros.values():
        b = m.get("combined") or {}
        for k in c:
            c[k] += int(b.get(k) or 0)
    c["registry_matched_pct"] = pct(c["registry_matched"], reg_total)
    c["active_matched_pct"] = pct(c["active_matched"], act_total)
    c["registry_matched_with_usable_website_pct"] = pct(c["registry_matched_with_usable_website"], reg_total)
    c["active_matched_with_usable_website_pct"] = pct(c["active_matched_with_usable_website"], act_total)
    out["combined"] = c
    return out


# --------------------------------------------------------------------------- #
# runner
# --------------------------------------------------------------------------- #
def _checkpoint(path: Path, doc: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(doc, indent=2))
    tmp.replace(path)


def run(
    *,
    out_path: Path,
    metros: Sequence[Metro],
    sources: set[str],
    release_pin: str | None = None,
    metro_budget_s: float = 300.0,
    overture_category_mode: str = "car_dealer",
) -> dict[str, Any]:
    registry = load_registry()
    active_n = sum(1 for r in registry if r.has_active_inventory)
    logger.info("registry=%d rows, %d with active inventory", len(registry), active_n)

    con = None
    release_id = release_pin
    if "overture" in sources:
        from backend.discovery.overture_discovery import connect_overture_duckdb, fetch_latest_release_id

        con = connect_overture_duckdb()
        if not release_id:
            release_id = fetch_latest_release_id(con)
        logger.info("Overture release (from STAC catalog %s): %s", "pinned" if release_pin else "latest", release_id)

    doc: dict[str, Any] = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "overture_release": release_id,
        "overture_category_mode": overture_category_mode,
        "sources_requested": sorted(sources),
        "match_rule": (
            "host equality via normalize.normalize_url, else merge._norm_key + "
            f"thefuzz.token_set_ratio >= merge._DEDUPE_THRESHOLD(88) within {NAME_MATCH_MAX_MILES}mi "
            "(city+state instead of distance for coordinate-less DMV rows)"
        ),
        "registry_totals": {
            "rows": len(registry),
            "with_coordinates": sum(1 for r in registry if r.lat is not None and r.lon is not None),
            "with_usable_website": sum(1 for r in registry if usable_website(r.website)),
            "with_active_inventory": active_n,
        },
        "metros": {},
    }
    _checkpoint(out_path, doc)

    for i, metro in enumerate(metros):
        t0 = time.time()
        reg_rows = registry_in_metro(registry, metro)
        block: dict[str, Any] = {
            "label": f"{metro.label}, {metro.state}",
            "center": [metro.lat, metro.lon],
            "radius_miles": metro.radius_mi,
            "registry_in_metro": len(reg_rows),
            "registry_active_in_metro": sum(1 for r in reg_rows if r.has_active_inventory),
            "sources": {},
        }
        logger.info(
            "=== %s %s: registry=%d active=%d",
            metro.key, metro.label, block["registry_in_metro"], block["registry_active_in_metro"],
        )

        if "overture" in sources and con is not None:
            try:
                t = time.time()
                cands = fetch_overture(con, release_id or "", metro, category_mode=overture_category_mode)
                block["sources"]["overture"] = summarize_source("overture", cands, reg_rows)
                block["sources"]["overture"]["elapsed_s"] = round(time.time() - t, 1)
            except Exception as exc:  # noqa: BLE001 — record and continue; a dead metro must not lose the run
                logger.warning("%s overture failed: %s", metro.key, exc)
                block["sources"]["overture"] = {"error": str(exc)}
            # checkpoint mid-metro too: Overpass is the slow half
            doc["metros"][metro.key] = block
            _checkpoint(out_path, doc)

        if "osm" in sources:
            if time.time() - t0 > metro_budget_s:
                block["sources"]["osm"] = {"error": f"skipped: metro budget {metro_budget_s}s already spent"}
                logger.warning("%s: budget exceeded before OSM, skipping", metro.key)
            else:
                try:
                    t = time.time()
                    cands = fetch_osm(metro)
                    block["sources"]["osm"] = summarize_source("osm", cands, reg_rows)
                    block["sources"]["osm"]["elapsed_s"] = round(time.time() - t, 1)
                except Exception as exc:  # noqa: BLE001
                    logger.warning("%s osm failed: %s", metro.key, exc)
                    block["sources"]["osm"] = {"error": str(exc)}

        if "dmv" in sources:
            dmv_cands, dmv_status = fetch_dmv(metro)
            if dmv_cands:
                block["sources"]["dmv"] = summarize_source("dmv", dmv_cands, reg_rows)
            block["sources"].setdefault("dmv", {})["status"] = dmv_status

        block["combined"] = combine(
            {k: v for k, v in block["sources"].items() if "_matched_ids" in v}, reg_rows
        )
        block["elapsed_s"] = round(time.time() - t0, 1)
        doc["metros"][metro.key] = block
        doc["totals"] = totals(doc)
        _checkpoint(out_path, doc)
        logger.info("%s done in %.1fs -> %s", metro.key, block["elapsed_s"], out_path)

        if "osm" in sources and i < len(metros) - 1 and OVERPASS_SLEEP_S > 0:
            time.sleep(OVERPASS_SLEEP_S)

    doc["totals"] = totals(doc)
    _checkpoint(out_path, doc)
    return doc


def fmt_table(doc: dict[str, Any]) -> str:
    t = doc.get("totals") or {}
    hdr = (
        f"{'source':<10} {'fetched':>8} {'reg hit':>10} {'reg+site':>10} "
        f"{'active hit':>11} {'act+site':>10} {'new':>6} {'new+site':>9} "
        f"{'newOEM':>7} {'junk':>6}"
    )
    lines = [hdr, "-" * len(hdr)]
    rows = dict(t.get("per_source") or {})
    if t.get("combined"):
        rows["COMBINED"] = t["combined"]
    reg_total = t.get("registry_in_sampled_metros", 0)
    act_total = t.get("registry_active_in_sampled_metros", 0)
    for name, s in rows.items():
        fetched = "-" if "candidates_fetched" not in s else str(s["candidates_fetched"])
        lines.append(
            f"{name:<10} {fetched:>8} "
            f"{s.get('registry_matched', 0):>4}/{reg_total:<5} "
            f"{s.get('registry_matched_with_usable_website', 0):>4}/{reg_total:<5} "
            f"{s.get('active_matched', 0):>5}/{act_total:<5} "
            f"{s.get('active_matched_with_usable_website', 0):>4}/{act_total:<5} "
            f"{s.get('new_plausible', 0):>6} {s.get('new_plausible_with_usable_website', 0):>9} "
            f"{s.get('new_franchise_branded', 0):>7} {s.get('junk_filtered', 0):>6}"
        )
    return "\n".join(lines)


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--out", default=str(DEFAULT_OUT))
    p.add_argument("--metros", default="", help=f"Comma list of metro keys (default all: {[m.key for m in METROS]})")
    p.add_argument("--sources", default="overture,osm,dmv")
    p.add_argument("--release", default=None, help="Pin an Overture release id (default: STAC latest)")
    p.add_argument("--metro-budget-s", type=float, default=300.0)
    p.add_argument("--overture-categories", default="car_dealer",
                   choices=("car_dealer", "vehicle_dealer"),
                   help="Overture row filter: legacy car_dealer value, or the wider vehicle_dealer taxonomy branch")
    p.add_argument("--derive-metros", action="store_true", help="Print registry clustering and exit")
    p.add_argument("-v", "--verbose", action="store_true")
    return p.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    if args.derive_metros:
        for c in derive_metros(load_registry()):
            print(c)
        return 0

    wanted = {k.strip().upper() for k in args.metros.split(",") if k.strip()}
    metros = [m for m in METROS if not wanted or m.key in wanted]
    if not metros:
        raise SystemExit(f"No metros matched {args.metros!r}; known: {[m.key for m in METROS]}")
    sources = {s.strip().lower() for s in args.sources.split(",") if s.strip()}

    doc = run(
        out_path=Path(args.out),
        metros=metros,
        sources=sources,
        release_pin=args.release,
        metro_budget_s=args.metro_budget_s,
        overture_category_mode=args.overture_categories,
    )
    print(fmt_table(doc))
    print(f"\nwrote {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
