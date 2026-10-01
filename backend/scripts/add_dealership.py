"""
Register a rooftop we know exists but have never scanned.

Photo OCR surfaces sister rooftops: 43 cars filed under Hiley VW of Huntsville carry
audihuntsville.com in their own images, but Audi Huntsville is not among the 591
dealerships we know, so those cars had nowhere to be moved to. This adds the rooftop so
backend.scripts.resolve_photo_attribution can move them.

    .venv/bin/python -m backend.scripts.add_dealership \
        --name "Audi Huntsville" --website https://www.audihuntsville.com \
        --brand Audi --near-website https://www.hileyvwhuntsville.com

Coordinates matter more than they look. Radius search is driven by the dealer geocode,
so a rooftop with no latitude/longitude silently drops its entire inventory out of
browse -- moving cars to an ungeocoded dealer would hide them rather than fix them.

``--near-website`` copies the coordinates of an existing rooftop and records the geocode
source as ``inherited_sibling_approx``. That is honest about what it is: good enough to
keep the cars in the right city and in radius results, explicitly not a real geocode, and
easy to find later with a WHERE clause once a working Places key is available. Prior
experience here is that a *name-only* geocode lookup put 15 of 135 dealers in the wrong
city, one of them 2,497 miles out, so inheriting a verified neighbour's point is the
safer approximation -- but only ever use it for a rooftop you know shares the location.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from backend.db.connect import connect as db_connect  # noqa: E402

_log = logging.getLogger("add_dealership")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--name", required=True)
    ap.add_argument("--website", required=True, help="canonical https://www.<host> URL")
    ap.add_argument("--brand", help="oem_brand, e.g. Audi")
    ap.add_argument("--lat", type=float)
    ap.add_argument("--lon", type=float)
    ap.add_argument("--city")
    ap.add_argument("--state")
    ap.add_argument("--zip", dest="zip_code")
    ap.add_argument(
        "--near-website",
        help="copy coordinates from this existing rooftop (same complex only)",
    )
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    conn = db_connect(autocommit=True)
    cur = conn.cursor()

    lat, lon = args.lat, args.lon
    city, state, zip_code = args.city, args.state, args.zip_code
    source = "manual"

    if args.near_website and (lat is None or lon is None):
        cur.execute(
            "SELECT latitude, longitude FROM dealerships WHERE website_url = %s",
            (args.near_website,),
        )
        row = cur.fetchone()
        if not row or row[0] is None:
            _log.error("--near-website %s has no coordinates to copy", args.near_website)
            return 2
        lat, lon = row[0], row[1]
        source = "inherited_sibling_approx"
        cur.execute(
            "SELECT city, state, zip_code FROM dealer_geopoints WHERE dealer_url = %s",
            (args.near_website,),
        )
        g = cur.fetchone()
        if g:
            city = city or g[0]
            state = state or g[1]
            zip_code = zip_code or g[2]
        _log.info("inherited coordinates %.6f, %.6f from %s", lat, lon, args.near_website)

    if lat is None or lon is None:
        _log.error(
            "refusing to add a rooftop with no coordinates: radius search would hide "
            "its entire inventory. Pass --lat/--lon or --near-website."
        )
        return 2

    cur.execute("SELECT id FROM dealerships WHERE website_url = %s", (args.website,))
    row = cur.fetchone()
    if row:
        dealership_id = row[0]
        _log.info("dealership already present (id=%s); leaving it as-is", dealership_id)
    else:
        cur.execute(
            """
            INSERT INTO dealerships (name, website_url, dealer_website_url,
                                     latitude, longitude, city, state, zip_code,
                                     oem_brand, source_web)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 1)
            RETURNING id
            """,
            (args.name, args.website, args.website, lat, lon, city, state, zip_code, args.brand),
        )
        dealership_id = cur.fetchone()[0]
        _log.info("created dealership id=%s  %s", dealership_id, args.name)

    cur.execute("SELECT 1 FROM dealer_geopoints WHERE dealer_url = %s", (args.website,))
    if cur.fetchone():
        _log.info("geopoint already present")
    else:
        cur.execute(
            """
            INSERT INTO dealer_geopoints
                (dealer_url, dealer_name, lat, lon, zip_code, city, state, geocode_source, geocoded_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
            """,
            (args.website, args.name, lat, lon, zip_code, city, state, source),
        )
        _log.info("geopoint added (source=%s)", source)

    if source == "inherited_sibling_approx":
        _log.warning(
            "coordinates are APPROXIMATE. Re-geocode with: "
            "SELECT * FROM dealer_geopoints WHERE geocode_source = 'inherited_sibling_approx'"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
