"""
Fill missing city/state/ZIP from coordinates using USPS ZIP centroids (pgeocode GeoNames).

When ``addr:postcode`` is present and plausible vs geometry, **state/city** follow that ZIP's
canonical row — not the geographically nearest ZIP centroid (avoids state-line snaps).
"""
from __future__ import annotations

import logging

from backend.db.geo import haversine, nearest_us_postal_meta, us_postal_meta_for_zip, zip_to_coords
from backend.discovery.candidate import DealerCandidate
from backend.discovery.normalize import normalize_zip

logger = logging.getLogger(__name__)

# Tagged ZIP centroid within this distance → trust ZIP-derived state/city over nearest-neighbor lookup.
_TAGGED_ZIP_TRUST_MI = 22.0

# If tagged ZIP centroid is farther than this from the POI but nearest-ZIP is much closer, prefer nearest.
_TAGGED_ZIP_IMPLAUSIBLE_MI = 28.0
_PREFER_NEAREST_IF_CLOSER_MI = 18.0


def enrich_candidate_location_fields(c: DealerCandidate) -> None:
    """Mutate *c* in place when lat/lon are known."""
    lat, lon = c.latitude, c.longitude
    if lat is None or lon is None:
        return

    tagged_z = normalize_zip(c.zip_code)
    meta_near = nearest_us_postal_meta(lat, lon)

    tagged_meta = us_postal_meta_for_zip(tagged_z) if tagged_z else None
    tagged_plausible = False
    if tagged_z and tagged_meta:
        cc = zip_to_coords(tagged_z)
        if cc:
            d_tag = haversine(lat, lon, cc[0], cc[1])
            tagged_plausible = d_tag <= _TAGGED_ZIP_TRUST_MI

    if tagged_plausible and tagged_meta:
        c.state = tagged_meta["state_code"]
        if not (c.city or "").strip() and tagged_meta.get("place_name"):
            c.city = tagged_meta["place_name"].strip()
    else:
        if not (c.state or "").strip() and meta_near:
            st = (meta_near.get("state_code") or "").strip().upper()[:2]
            if len(st) == 2:
                c.state = st
        if not (c.city or "").strip() and meta_near:
            ph = (meta_near.get("place_name") or "").strip()
            if ph:
                c.city = ph

    nz = normalize_zip(meta_near.get("postal_code")) if meta_near else None

    if not tagged_z and nz:
        c.zip_code = nz
        rm = us_postal_meta_for_zip(nz)
        if rm:
            c.state = rm["state_code"]
            if not (c.city or "").strip() and rm.get("place_name"):
                c.city = rm["place_name"].strip()
        return

    if tagged_z and nz and meta_near:
        c_tag = zip_to_coords(tagged_z)
        c_near = zip_to_coords(nz)
        if c_tag and c_near:
            d_tagged = haversine(lat, lon, c_tag[0], c_tag[1])
            d_near = haversine(lat, lon, c_near[0], c_near[1])
            if d_tagged >= _TAGGED_ZIP_IMPLAUSIBLE_MI and d_near < min(
                d_tagged, _PREFER_NEAREST_IF_CLOSER_MI
            ):
                logger.debug(
                    "Reconciling ZIP %s → %s for %r (tagged centroid ~%.1f mi, nearest ~%.1f mi)",
                    tagged_z,
                    nz,
                    c.name,
                    d_tagged,
                    d_near,
                )
                c.zip_code = nz
                rm = us_postal_meta_for_zip(nz)
                if rm:
                    c.state = rm["state_code"]
                    if not (c.city or "").strip() and rm.get("place_name"):
                        c.city = rm["place_name"].strip()
