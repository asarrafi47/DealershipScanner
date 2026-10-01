"""
Decide which rooftop a car belongs to using the dealer branding in its own photos.

Reads the OCR cache (car_image_text, populated by run_image_text_extraction) and, for
each car whose photographs carry a dealer domain other than the one it is filed under,
records the finding in car_photo_attribution. With --apply it also moves the car.

    .venv/bin/python -m backend.scripts.resolve_photo_attribution            # report
    .venv/bin/python -m backend.scripts.resolve_photo_attribution --apply    # move them
    .venv/bin/python -m backend.scripts.resolve_photo_attribution --revert   # undo

Why a car can be filed wrong at all: a dealer group's feed serves the whole group from
every rooftop's hostname, so the scanner attributes 337 cars to Hiley VW when a share of
them are the group's Audi and Mazda stores.

Two rules keep this from replacing one wrong answer with another:

  * A move requires the photo domain to resolve to a rooftop we already know. An
    unrecognised domain is recorded, never acted on -- otherwise a photo vendor's
    watermark or a manufacturer URL would invent a dealership.
  * The original dealer_id is stored before anything is written, because the scanner
    rewrites cars.dealer_id on every pass and an unrecorded move cannot be undone.

Note the limit of the evidence: a photograph proves where the car was *photographed*.
For a single-rooftop dealer that is the same thing; for a group that shares a photo
booth it may not be. Cars are moved only between rooftops of the same group in practice,
because that is the only case where a sister domain appears at all.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections import Counter
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from backend.db.connect import connect as db_connect  # noqa: E402

_log = logging.getLogger("photo_attr")

# Domains that appear in dealer photography without identifying the selling rooftop:
# photo vendors, syndication partners, manufacturer sites, social links.
_NON_DEALER_DOMAINS = {
    "carfax.com",
    "autocheck.com",
    "kbb.com",
    "edmunds.com",
    "cars.com",
    "cargurus.com",
    "autotrader.com",
    "facebook.com",
    "instagram.com",
    "youtube.com",
    "twitter.com",
    "google.com",
    "spyne.ai",
    "homenetiol.com",
    "dealerinspire.com",
    "dealer.com",
    "carsforsale.com",
    "monroneylabels.com",
    "windowsticker.com",
    # Manufacturer marketing URLs printed on window stickers and in-car displays.
    # A Ford sticker naming ford.com says nothing about which rooftop holds the car;
    # these were the most common "foreign domain" in the first real run.
    "ford.com",
    "nissanusa.com",
    "onstar.com",
    "lincolnafs.com",
    "broncooffroadeo.com",
    "toyota.com",
    "honda.com",
    "chevrolet.com",
    "mazdausa.com",
    "vw.com",
    "bmwusa.com",
    "mbusa.com",
    "audiusa.com",
    "kia.com",
    "hyundaiusa.com",
    "subaru.com",
    "jeep.com",
    "ramtrucks.com",
    "dodge.com",
    "chrysler.com",
    "sirius.com",
    "siriusxm.com",
    "fueleconomy.gov",
    "safercar.gov",
}


# OCR reads a domain off signage far less cleanly than off a printed slide. A first run
# produced "normreeveshuntingtonbe1ch.com" for normreeveshuntingtonbeach, "subarg.com"
# for subaru and "edmartn.com" for edmartin -- every one of them the car's OWN dealer,
# misread, and every one reported as evidence the car was filed wrong. So a domain close
# enough to the current rooftop is treated as that rooftop, and a destination has to be a
# near-exact match to a known host before it is allowed to attract a car.
_SAME_DEALER_RATIO = 0.82
_RESOLVE_RATIO = 0.90


def _plausible_domain(domain: str) -> bool:
    """
    Reject domains that OCR clearly fragmented.

    Signage is read in pieces, so a run produced "for.com", "busa.com" and "ecula.com"
    (the tail of "temecula"). A real dealer hostname is long; these are debris, and at
    one car apiece they would otherwise each look like a distinct unknown rooftop.
    """
    label = (domain or "").strip().lower().split(".")[0]
    return len(label) >= 8


def _similar(a: str, b: str) -> float:
    from difflib import SequenceMatcher

    return SequenceMatcher(None, a, b).ratio()


def _resolve_host(key: str, index: dict[str, str]) -> str | None:
    """Known rooftop for a (possibly misread) host key, or None."""
    exact = index.get(key)
    if exact:
        return exact
    best: str | None = None
    best_ratio = 0.0
    for known_key, dealer_id in index.items():
        ratio = _similar(key, known_key)
        if ratio > best_ratio:
            best, best_ratio = dealer_id, ratio
    return best if best_ratio >= _RESOLVE_RATIO else None


def _host_key(host: str) -> str:
    """Normalise a hostname to the dealer_id convention: strip www, dots to dashes."""
    h = (host or "").strip().lower()
    if h.startswith("www."):
        h = h[4:]
    return h.replace(".", "-")


def build_dealer_index(cur) -> dict[str, str]:
    """
    Map normalised hostname -> dealer_id for rooftops a car may be moved TO.

    Registry-backed only, deliberately. An earlier version also accepted any dealer_id
    appearing on a car, which let group-level landing domains act as destinations:
    "fletcherjones-com" carries 33 cars and is in no registry, and cars were duly moved
    to it out of Mercedes-Benz of Ontario and Audi Costa Mesa -- both real, specific
    rooftops. That trades a precise wrong answer for a vaguer one.

    A destination has to be a rooftop we have actually registered. Where a car is filed
    now is not constrained this way; only where it is allowed to go.
    """
    index: dict[str, str] = {}
    cur.execute(
        "SELECT COALESCE(website_url, dealer_website_url) FROM dealerships "
        "WHERE COALESCE(website_url, dealer_website_url) IS NOT NULL "
        "  AND COALESCE(is_active, 1) = 1 "
        "  AND duplicate_of_id IS NULL"
    )
    for (url,) in cur.fetchall():
        host = urlparse(url if "://" in url else f"https://{url}").netloc
        key = _host_key(host)
        if key:
            index.setdefault(key, key)
    return index


def find_candidates(cur, index: dict[str, str]) -> list[dict[str, Any]]:
    """Cars whose photographs name a dealer domain other than the one they are filed under."""
    cur.execute(
        """
        SELECT t.car_id, t.vin, c.dealer_id, t.summary
        FROM car_image_text t
        JOIN cars c ON c.id = t.car_id
        WHERE c.listing_removed_at IS NULL
          AND t.summary IS NOT NULL
        """
    )
    out: list[dict[str, Any]] = []
    for car_id, vin, dealer_id, summary in cur.fetchall():
        s = summary if isinstance(summary, dict) else json.loads(summary or "{}")
        domains = [
            d for d in (s.get("dealer_domains") or [])
            if d not in _NON_DEALER_DOMAINS and _plausible_domain(d)
        ]
        if not domains:
            continue

        current_key = (dealer_id or "").strip().lower()
        # A misread of the car's own domain is not evidence of anything.
        foreign = [
            d for d in domains
            if _host_key(d) != current_key
            and _similar(_host_key(d), current_key) < _SAME_DEALER_RATIO
        ]
        if not foreign:
            continue

        # Most-seen foreign domain wins; ties resolve to the first, which is stable
        # because summarize_gallery sorts them.
        domain = Counter(foreign).most_common(1)[0][0]
        resolved = _resolve_host(_host_key(domain), index)
        if resolved and resolved.strip().lower() == current_key:
            continue

        out.append(
            {
                "car_id": car_id,
                "vin": vin,
                "original_dealer_id": dealer_id,
                "photo_domain": domain,
                "resolved_dealer_id": resolved,
                "evidence_images": len([d for d in domains if d == domain]) or 1,
                "evidence_image_url": (s.get("sticker_image_urls") or [None])[0],
            }
        )
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true", help="write the moves to cars.dealer_id")
    ap.add_argument("--revert", action="store_true", help="undo every applied move")
    ap.add_argument("--min-evidence", type=int, default=1, help="images that must name the domain")
    ap.add_argument(
        "--min-cluster", type=int, default=5,
        help="cars that must share an unrecognised domain before it counts as a real rooftop",
    )
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    conn = db_connect(autocommit=True)
    cur = conn.cursor()

    if args.revert:
        cur.execute(
            """
            UPDATE cars c SET dealer_id = a.original_dealer_id
            FROM car_photo_attribution a
            WHERE a.car_id = c.id AND a.applied_at IS NOT NULL
            """
        )
        moved = cur.rowcount
        cur.execute("UPDATE car_photo_attribution SET applied_at = NULL WHERE applied_at IS NOT NULL")
        _log.info("reverted %d car(s) to their original dealer", moved)
        return 0

    index = build_dealer_index(cur)
    _log.info("known rooftops: %d", len(index))

    candidates = [c for c in find_candidates(cur, index) if c["evidence_images"] >= args.min_evidence]
    movable = [c for c in candidates if c["resolved_dealer_id"]]

    # An unrecognised domain is a real rooftop only if many cars name it. A sister store
    # like Audi Huntsville shows up on dozens of listings; an OCR misreading is unique to
    # the image that produced it. Frequency is what separates the two, so singletons are
    # dropped rather than reported as dealerships worth adding.
    unresolved = [c for c in candidates if not c["resolved_dealer_id"]]
    domain_counts = Counter(c["photo_domain"] for c in unresolved)
    unknown = [c for c in unresolved if domain_counts[c["photo_domain"]] >= args.min_cluster]
    dropped = len(unresolved) - len(unknown)
    if dropped:
        _log.info("discarded %d one-off unrecognised domain(s) as OCR noise", dropped)

    candidates = movable + unknown

    for c in candidates:
        cur.execute(
            """
            INSERT INTO car_photo_attribution (
                car_id, vin, original_dealer_id, photo_domain,
                evidence_images, evidence_image_url, resolved_dealer_id
            ) VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (car_id) DO UPDATE SET
                photo_domain = EXCLUDED.photo_domain,
                evidence_images = EXCLUDED.evidence_images,
                resolved_dealer_id = EXCLUDED.resolved_dealer_id,
                detected_at = NOW()
            """,
            (
                c["car_id"], c["vin"], c["original_dealer_id"], c["photo_domain"],
                c["evidence_images"], c["evidence_image_url"], c["resolved_dealer_id"],
            ),
        )

    _log.info("photo evidence disagrees with the filed rooftop on %d car(s)", len(candidates))
    _log.info("  movable (destination is a known rooftop): %d", len(movable))
    _log.info("  no known destination -- rooftop not in registry: %d", len(unknown))

    by_move = Counter((c["original_dealer_id"], c["resolved_dealer_id"]) for c in movable)
    for (src, dst), n in by_move.most_common(12):
        _log.info("    %5d  %s  ->  %s", n, src, dst)

    by_unknown = Counter((c["original_dealer_id"], c["photo_domain"]) for c in unknown)
    for (src, dom), n in by_unknown.most_common(12):
        _log.info("    %5d  %s  ->  %s  (NOT a known rooftop -- consider adding it)", n, src, dom)

    if not args.apply:
        _log.info("report only; re-run with --apply to move the movable ones")
        return 0

    applied = 0
    for c in movable:
        cur.execute(
            "UPDATE cars SET dealer_id = %s WHERE id = %s AND dealer_id = %s",
            (c["resolved_dealer_id"], c["car_id"], c["original_dealer_id"]),
        )
        if cur.rowcount:
            cur.execute(
                "UPDATE car_photo_attribution SET applied_at = NOW() WHERE car_id = %s", (c["car_id"],)
            )
            applied += 1
    _log.info("moved %d car(s). Undo with --revert", applied)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
