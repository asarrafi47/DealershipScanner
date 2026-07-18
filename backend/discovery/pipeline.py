"""
Tiered discovery orchestration: DMV → Google Places → DDG URL gap-fill.
"""
from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any

import requests

from backend.db.dealerships_db import geocode_city_state, upsert_discovery_row
from backend.db.geo import haversine, zip_to_coords
from backend.discovery.candidate import DealerCandidate
from backend.discovery.dmv import fetch_dmv_records
from backend.discovery.dmv.schema import DMVRecord
from backend.discovery.coordinate_enrich import enrich_candidate_location_fields
from backend.discovery.google_places import fetch_google_places_dealerships
from backend.discovery.merge import merge_and_dedupe
from backend.discovery.non_dealer_filter import is_probable_non_dealer
from backend.discovery.normalize import (
    looks_like_dealer_website,
    normalize_address,
    normalize_url,
    normalize_zip,
    state_code_for_geocode,
)
from backend.discovery.web import ddg_find_dealer_url
from backend.discovery.zcta_gazetteer import resolve_zip_center

logger = logging.getLogger(__name__)


def _validate_zip(zip_code: str) -> str:
    z = normalize_zip(zip_code)
    if not z:
        raise ValueError("zip_code must be a 5-digit US ZIP")
    return z


def _validate_radius(radius_miles: float) -> float:
    if radius_miles <= 0 or radius_miles > 500:
        raise ValueError("radius_miles must be in (0, 500]")
    return float(radius_miles)


def dmv_records_to_candidates(
    records: list[DMVRecord],
    center_lat: float,
    center_lon: float,
    radius_miles: float,
) -> list[DealerCandidate]:
    """Filter DMV rows by distance; fill coords via ZIP centroid or city/state geocode."""
    out: list[DealerCandidate] = []
    for r in records:
        st = (r.state or "").strip().upper()[:2]
        city = (r.city or "").strip()
        if len(st) != 2 or not city:
            continue
        z = normalize_zip(r.zip_code) or ""
        lat: float | None = None
        lon: float | None = None
        if z:
            cc = zip_to_coords(z)
            if cc:
                lat, lon = cc
        if lat is None:
            gc = geocode_city_state(city, st)
            if gc:
                lat, lon = gc
        if lat is None or lon is None:
            continue
        if haversine(center_lat, center_lon, lat, lon) > radius_miles:
            continue
        addr = normalize_address(r.street_address) or ""
        w = (r.website or "").strip()
        nu = normalize_url(w) if w else ""
        out.append(
            DealerCandidate(
                name=r.business_name.strip(),
                city=city,
                state=st,
                street_address=addr,
                zip_code=z,
                latitude=lat,
                longitude=lon,
                dealer_website_url=nu or "",
                website_url=nu or "",
                source_dmv=True,
            )
        )
    return out


def run_discovery(
    zip_code: str,
    radius_miles: float,
    *,
    dmv_state: str | None = None,
    persist: bool = False,
    fill_urls_via_ddg: bool = True,
    project_root: Path | None = None,
    gazetteer_path: Path | None = None,
    within_seed_zip_only: bool = False,
    ddg_timeout_s: float = 15.0,
    session: Any = None,
) -> list[DealerCandidate]:
    """
    Discover dealerships near ``zip_code`` within ``radius_miles``.

    - Optional ``dmv_state``: 2-letter code with a registered loader (e.g. ``NC``).
    - Google Places tier runs when ``GOOGLE_MAPS_API_KEY`` is set.
    - ``fill_urls_via_ddg``: query DDG for HTTPS URLs when still missing after merge.
    - ``persist``: upsert each merged row via ``upsert_discovery_row``.
    - ``gazetteer_path``: optional ZCTA gazetteer file; default picks ``backend/ZIPs/*.txt``
      or ``DISCOVERY_ZCTA_GAZETTEER``. Centroids (INTPTLAT/LONG) override pgeocode when found.
    - ``within_seed_zip_only``: after enrichment, keep rows whose ZIP matches the seed ZCTA.
    """
    z = _validate_zip(zip_code)
    radius_miles = _validate_radius(radius_miles)

    lat0, lon0, center_src = resolve_zip_center(
        z,
        project_root=project_root,
        gazetteer_path=gazetteer_path,
    )
    logger.info("Discovery center for %s from %s", z, center_src)

    sess = session if session is not None else requests.Session()

    combined: list[DealerCandidate] = []
    n_dmv = 0

    if dmv_state:
        dmv_state_u = dmv_state.strip().upper()
        records = fetch_dmv_records(dmv_state_u, project_root)
        combined.extend(dmv_records_to_candidates(records, lat0, lon0, radius_miles))
        n_dmv = len(combined)
        logger.info("DMV tier %s: %s candidates in radius", dmv_state_u, n_dmv)

    gp_list = fetch_google_places_dealerships(lat0, lon0, radius_miles, session=sess)
    n_gp = len(gp_list)
    combined.extend(gp_list)
    logger.info("Google Places tier: %d dealers in radius", n_gp)

    merged = merge_and_dedupe(combined)

    # Drop Google-Places-mislabeled non-dealers (salvage/u-pull yards, car rental,
    # RV dealers, restaurants, D2C brands with no franchise dealers, auction/wholesale)
    # so they don't dilute franchise-dealer coverage numbers.
    before_nd = len(merged)
    kept: list[DealerCandidate] = []
    dropped_nd: list[str] = []
    for c in merged:
        if is_probable_non_dealer(c.name, c.dealer_website_url or c.website_url):
            c.is_dealer = False
            dropped_nd.append(c.name)
        else:
            kept.append(c)
    if dropped_nd:
        logger.info(
            "Non-dealer filter: dropped %d of %d candidates (%s)",
            len(dropped_nd),
            before_nd,
            ", ".join(dropped_nd[:12]) + ("…" if len(dropped_nd) > 12 else ""),
        )
    merged = kept

    for c in merged:
        enrich_candidate_location_fields(c)

    for c in merged:
        if (c.latitude is None or c.longitude is None) and c.city:
            st = state_code_for_geocode(c.state)
            if len(st) == 2:
                gc = geocode_city_state(c.city, st)
                if gc:
                    c.latitude, c.longitude = gc
        nz = normalize_zip(c.zip_code)
        if nz:
            c.zip_code = nz

    if within_seed_zip_only:
        before = len(merged)
        merged = [c for c in merged if normalize_zip(c.zip_code) == z]
        logger.info(
            "Seed ZIP scope %s: %d → %d candidates (addr/ZIP must match ZCTA after enrichment)",
            z,
            before,
            len(merged),
        )

    if fill_urls_via_ddg:
        _ddg_calls = 0
        for c in merged:
            existing = normalize_url(c.dealer_website_url or c.website_url)
            if existing and looks_like_dealer_website(existing):
                c.dealer_website_url = existing
                c.website_url = existing
                continue
            if _ddg_calls > 0:
                time.sleep(1.2)
            _ddg_calls += 1
            u = ddg_find_dealer_url(
                c.name, c.city, c.state, timeout_s=ddg_timeout_s, session=sess
            )
            if u and looks_like_dealer_website(u):
                c.dealer_website_url = u
                c.website_url = u
                c.source_web = True

    def _candidate_has_url(c: DealerCandidate) -> bool:
        raw = (c.dealer_website_url or c.website_url or "").strip()
        return bool(raw)

    # Final radius pass (merged rows may only have city geocode)
    final: list[DealerCandidate] = []
    for c in merged:
        if c.latitude is not None and c.longitude is not None:
            if haversine(lat0, lon0, c.latitude, c.longitude) > radius_miles:
                continue
        final.append(c)

    n_with_url = sum(1 for c in final if _candidate_has_url(c))
    n_missing_url = len(final) - n_with_url
    logger.info(
        "Discovery tiers done (DMV=%s, GooglePlaces=%d): %d candidates in output radius, "
        "%d with a URL string after DMV+GooglePlaces+%s.",
        n_dmv if dmv_state else 0,
        n_gp,
        len(final),
        n_with_url,
        "DDG" if fill_urls_via_ddg else "no DDG",
    )
    if n_missing_url:
        logger.info(
            "%d candidates still have no URL after gap-fill. "
            "dealers.json merge will skip those rows until a URL exists.",
            n_missing_url,
        )

    if persist:
        for c in final:
            upsert_discovery_row(c.to_db_dict())

    return final
