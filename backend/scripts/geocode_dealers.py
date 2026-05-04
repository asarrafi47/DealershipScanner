#!/usr/bin/env python3
"""
Geocode each unique dealership using OSM Nominatim, with domain-based fallback.

Pass 1: Nominatim by dealer name + location hint from domain.
Pass 2: pgeocode zip centroid using domain → zip mapping for known fails.

Results stored in dealer_geopoints table. Re-running skips cached entries
unless --force is passed.

Usage:
  python -m backend.scripts.geocode_dealers
  python -m backend.scripts.geocode_dealers --force
  python -m backend.scripts.geocode_dealers --dry-run
"""
from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
import time
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_REPO_ROOT))

from backend.db.inventory_db import DB_PATH

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("geocode_dealers")

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
USER_AGENT = "DealershipScanner/1.0 (arman@khash.com)"
RATE_LIMIT_SEC = 1.1  # OSM Nominatim: max 1 req/sec

# Domain-fragment → (city, state, zip) for dealers Nominatim can't find.
# Keyed by lowercase domain with dots/dashes stripped.
_DOMAIN_ZIP_FALLBACK: dict[str, tuple[str, str, str]] = {
    # Hawaii - Honolulu
    "cutterfordsales":        ("Honolulu",  "HI", "96819"),
    "cutterchevy":            ("Honolulu",  "HI", "96814"),
    "cuttermitsubishi":       ("Honolulu",  "HI", "96814"),
    "kingcdjr":               ("Pearl City","HI", "96782"),
    "kingwindwardnissan":     ("Kaneohe",   "HI", "96744"),
    "alohakiaairport":        ("Honolulu",  "HI", "96819"),
    "alohakialeeward":        ("Waipahu",   "HI", "96797"),
    "jerryvhonoluluhyundai":  ("Honolulu",  "HI", "96814"),
    # Hawaii - Maui
    "jimfalkmazdaofmaui":     ("Kahului",   "HI", "96732"),
    "jimfalkhyundaiofmaui":   ("Kahului",   "HI", "96732"),
    "alohakiamaui":           ("Kahului",   "HI", "96732"),
    "mauitoyota":             ("Kahului",   "HI", "96732"),
    # Hawaii - Kauai
    "alohakiakauai":          ("Lihue",     "HI", "96766"),
    # Hawaii - Big Island
    "alohakiahilo":           ("Hilo",      "HI", "96720"),
    "alohakiakona":           ("Kailua-Kona","HI","96740"),
    # Charlotte, NC area
    "townandcountrytoyota":   ("Charlotte", "NC", "28213"),
    "newcitynissan":          ("Charlotte", "NC", "28262"),
    "kefferjeep":             ("Matthews",  "NC", "28105"),
    "hendrickchryslerdodgejeepramofconcord": ("Concord", "NC", "28027"),
    # Irvine/Long Beach, CA
    "irvinebmw":              ("Irvine",    "CA", "92612"),
    # Houston / South Texas
    "gillmanchevygmc":        ("Houston",   "TX", "77082"),
    "gillmanhondasanbenito":  ("San Benito","TX", "78586"),
    "sameslaredochevrolet":   ("Laredo",    "TX", "78045"),
    "escamillaford":          ("Laredo",    "TX", "78043"),
    "southtexasgmc":          ("Houston",   "TX", "77041"),
}

# Overrides for dealers whose Nominatim results landed in the wrong city
# (e.g. "Ford Motor Company" → Milwaukee instead of Hawaii).
_KNOWN_BAD_GEOCODES: set[str] = {
    "https://www.cutterfordsales.com",
    "https://www.kingcdjr.com",
    "https://www.newcitynissan.com",
    "https://www.townandcountryford.com",
    "https://www.kingwindwardnissan.com",
}


def _ensure_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS dealer_geopoints (
            dealer_url    TEXT PRIMARY KEY,
            dealer_name   TEXT,
            lat           REAL,
            lon           REAL,
            zip_code      TEXT,
            city          TEXT,
            state         TEXT,
            geocode_source TEXT,
            geocoded_at   TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_dgp_dealer_url ON dealer_geopoints(dealer_url)"
    )
    conn.commit()


def _nominatim_search(query: str) -> dict | None:
    import urllib.request, urllib.parse, json
    params = urllib.parse.urlencode({
        "q": query, "format": "json", "limit": 1,
        "countrycodes": "us", "addressdetails": 1,
    })
    req = urllib.request.Request(
        f"{NOMINATIM_URL}?{params}", headers={"User-Agent": USER_AGENT}
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
        return data[0] if data else None
    except Exception as exc:
        log.warning("Nominatim error for %r: %s", query, exc)
        return None


def _extract_addr(result: dict) -> tuple[str | None, str | None, str | None]:
    """Return (zip_code, city, state_2letter) from Nominatim address block."""
    _states = {
        "alabama":"AL","alaska":"AK","arizona":"AZ","arkansas":"AR","california":"CA",
        "colorado":"CO","connecticut":"CT","delaware":"DE","florida":"FL","georgia":"GA",
        "hawaii":"HI","idaho":"ID","illinois":"IL","indiana":"IN","iowa":"IA","kansas":"KS",
        "kentucky":"KY","louisiana":"LA","maine":"ME","maryland":"MD","massachusetts":"MA",
        "michigan":"MI","minnesota":"MN","mississippi":"MS","missouri":"MO","montana":"MT",
        "nebraska":"NE","nevada":"NV","new hampshire":"NH","new jersey":"NJ",
        "new mexico":"NM","new york":"NY","north carolina":"NC","north dakota":"ND",
        "ohio":"OH","oklahoma":"OK","oregon":"OR","pennsylvania":"PA","rhode island":"RI",
        "south carolina":"SC","south dakota":"SD","tennessee":"TN","texas":"TX","utah":"UT",
        "vermont":"VT","virginia":"VA","washington":"WA","west virginia":"WV",
        "wisconsin":"WI","wyoming":"WY",
    }
    addr = result.get("address") or {}
    zc = (addr.get("postcode") or "").strip()[:10] or None
    city = (addr.get("city") or addr.get("town") or addr.get("village")
            or addr.get("county") or "").strip() or None
    st = (addr.get("state_code") or addr.get("state") or "").strip()
    if st and len(st) > 2:
        st = _states.get(st.lower(), st[:2].upper())
    return zc, city, st.upper() if st else None


def _pgeocode_fallback(zip_code: str, city: str, state: str) -> dict | None:
    """Use pgeocode zip centroid as fallback coordinates."""
    from backend.db.geo import zip_to_coords
    coords = zip_to_coords(zip_code)
    if not coords:
        return None
    import math
    if math.isnan(coords[0]) or math.isnan(coords[1]):
        return None
    return {
        "lat": float(coords[0]),
        "lon": float(coords[1]),
        "zip_code": zip_code,
        "city": city,
        "state": state,
        "source": "pgeocode_zip",
    }


def _domain_key(dealer_url: str) -> str:
    import urllib.parse, re
    domain = urllib.parse.urlparse(dealer_url).hostname or dealer_url
    domain = domain.replace("www.", "").lower()
    # Strip TLD
    domain = re.sub(r"\.(com|net|org|io)$", "", domain)
    # Strip separators
    return re.sub(r"[-.]", "", domain)


def _location_hint(dealer_url: str) -> str | None:
    """Return 'City, ST' hint based on domain keywords."""
    _hints = {
        "honolulu": "Honolulu, HI",
        "maui":     "Maui, HI",
        "kauai":    "Kauai, HI",
        "hilo":     "Hilo, HI",
        "kona":     "Kailua-Kona, HI",
        "leeward":  "Waipahu, HI",
        "airport":  "Honolulu, HI",
        "windward": "Kaneohe, HI",
        "cutter":   "Honolulu, HI",
        "charlotte": "Charlotte, NC",
        "concord":  "Concord, NC",
        "fortmill": "Fort Mill, SC",
        "longbeach": "Long Beach, CA",
        "irvine":   "Irvine, CA",
        "tuttleclick": "Irvine, CA",
        "gillman":  "Houston, TX",
        "sanbenito": "San Benito, TX",
        "laredo":   "Laredo, TX",
        "escamilla": "Laredo, TX",
        "southtexas": "Houston, TX",
        "kingwindward": "Kaneohe, HI",
        "kingcdjr": "Pearl City, HI",
    }
    import urllib.parse, re
    domain = (urllib.parse.urlparse(dealer_url).hostname or "").replace("www.", "").lower()
    domain_flat = re.sub(r"[-.]", "", domain)
    for kw, loc in _hints.items():
        if kw in domain_flat:
            return loc
    return None


def geocode_dealer(dealer_name: str, dealer_url: str) -> dict | None:
    hint = _location_hint(dealer_url)
    dom_key = _domain_key(dealer_url)

    # Build Nominatim queries: hint-scoped first, then bare name
    queries: list[str] = []
    if hint:
        queries.append(f"{dealer_name}, {hint}")
    queries.append(dealer_name)

    for query in queries:
        log.info("  Nominatim: %r", query)
        result = _nominatim_search(query)
        time.sleep(RATE_LIMIT_SEC)
        if result:
            try:
                lat, lon = float(result["lat"]), float(result["lon"])
            except (KeyError, TypeError, ValueError):
                continue
            zc, city, state = _extract_addr(result)
            return {"lat": lat, "lon": lon, "zip_code": zc,
                    "city": city, "state": state, "source": "nominatim"}

    # Nominatim failed — try domain-based zip fallback
    fb = _DOMAIN_ZIP_FALLBACK.get(dom_key)
    if fb:
        city, state, zip_code = fb
        log.info("  Domain fallback: %s, %s %s", city, state, zip_code)
        return _pgeocode_fallback(zip_code, city, state)

    return None


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--force", action="store_true",
                        help="Re-geocode all, including already-cached entries")
    parser.add_argument("--fix-bad", action="store_true",
                        help="Re-geocode known bad Nominatim results using domain fallback")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    conn = sqlite3.connect(DB_PATH)
    _ensure_table(conn)

    rows = conn.execute("""
        SELECT dealer_url, dealer_name, COUNT(*) as cnt
        FROM cars
        WHERE COALESCE(listing_active, 1) = 1 AND dealer_url IS NOT NULL
        GROUP BY dealer_url
        ORDER BY cnt DESC
    """).fetchall()
    log.info("Found %d unique active dealers", len(rows))

    existing = {
        r[0]: r[1]
        for r in conn.execute(
            "SELECT dealer_url, geocode_source FROM dealer_geopoints"
        ).fetchall()
    }

    inserted = updated = skipped = failed = 0

    for dealer_url, dealer_name, car_count in rows:
        is_known_bad = dealer_url in _KNOWN_BAD_GEOCODES
        already_done = dealer_url in existing and existing[dealer_url] not in ("failed", None)

        if not args.force:
            if already_done and not (args.fix_bad and is_known_bad):
                log.info("SKIP  %s", dealer_name)
                skipped += 1
                continue
            if already_done and args.fix_bad and is_known_bad:
                log.info("REFIX %s (known bad geocode)", dealer_name)
            elif existing.get(dealer_url) == "failed":
                log.info("RETRY %s (previously failed)", dealer_name)

        log.info("Geocoding [%d cars]: %s", car_count, dealer_name)

        # For known-bad Nominatim results: skip Nominatim, use domain fallback directly
        if (args.fix_bad or args.force) and is_known_bad:
            dom_key = _domain_key(dealer_url)
            fb = _DOMAIN_ZIP_FALLBACK.get(dom_key)
            if fb:
                city, state, zip_code = fb
                log.info("  Domain fallback (forced): %s, %s %s", city, state, zip_code)
                geo = _pgeocode_fallback(zip_code, city, state)
            else:
                geo = geocode_dealer(dealer_name or "", dealer_url or "")
        else:
            geo = geocode_dealer(dealer_name or "", dealer_url or "")

        if not geo:
            log.warning("FAIL  %s", dealer_name)
            failed += 1
            if not args.dry_run:
                conn.execute("""
                    INSERT OR REPLACE INTO dealer_geopoints
                        (dealer_url, dealer_name, lat, lon, zip_code, city, state, geocode_source)
                    VALUES (?, ?, NULL, NULL, NULL, NULL, NULL, 'failed')
                """, (dealer_url, dealer_name))
                conn.commit()
            continue

        log.info("OK    %s → (%.4f, %.4f) %s %s %s",
                 dealer_name, geo["lat"], geo["lon"],
                 geo.get("city",""), geo.get("state",""), geo.get("zip_code",""))

        if not args.dry_run:
            conn.execute("""
                INSERT OR REPLACE INTO dealer_geopoints
                    (dealer_url, dealer_name, lat, lon, zip_code, city, state, geocode_source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (dealer_url, dealer_name, geo["lat"], geo["lon"],
                  geo.get("zip_code"), geo.get("city"), geo.get("state"), geo["source"]))
            conn.commit()
        if dealer_url in existing:
            updated += 1
        else:
            inserted += 1

    conn.close()
    prefix = "Would " if args.dry_run else ""
    log.info("")
    log.info("=== Summary ===")
    log.info("%sInserted: %d  Updated: %d  Skipped: %d  Failed: %d",
             prefix, inserted, updated, skipped, failed)


if __name__ == "__main__":
    main()
