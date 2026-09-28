"""Per-dealership RESEARCH page (additive, read-only).

New surface at ``GET /dealership/<dealer_key>``. This is intentionally separate
from the car SEARCH / listings flow and must not affect it. It shows one
dealer's header, inventory grid, Leaflet location map, contact block, and
navigate-there deep links, plus an empty placeholder section for a later
"Current Deals & Specials" phase.

``dealer_key`` accepts either the hostname-derived ``cars.dealer_id`` (e.g.
``tustintoyota-com``) or the numeric ``dealerships.id``. The ``dealerships`` <->
``cars`` join is by host: ``dealerships.website_url`` host (www-stripped, dots
turned to dashes) matches ``cars.dealer_id``.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import threading
import time
from collections import OrderedDict
from urllib.parse import urlparse

from flask import abort, jsonify, make_response, render_template, request, session

# Cards server-rendered into the HTML for first paint. The rest of the dealer's
# inventory arrives from ``/api/dealership/<key>/cars`` and main.js takes over —
# same bootstrap-then-hydrate split /listings uses, for the same reason: a big
# dealer has 4,800 listings and none of them belong in the document.
_GRID_BOOTSTRAP = 24

# main.js gates EVERY filter render on a valid 5-digit ZIP
# (``listingsZipRenderStale`` → ``listingsHasValidZip``), because on /listings the
# result set is defined by ZIP + radius. A dealership page has exactly one
# location and no radius control, so we hand the shared code the dealer's own ZIP
# instead of prompting for one; with no radius <select> on the page, main.js's
# geo/radius branch is never entered. See ``_dealer_filter_zip``.
_UNKNOWN_FILTER_ZIP = "00000"

_ZIP5_RE = re.compile(r"^\d{5}$")

# ``dealer_key`` is either a numeric ``dealerships.id`` or a hostname-derived
# ``cars.dealer_id`` (every one of the 216 in the table matches ``[a-z0-9-]+``).
# Anything outside a hostname's own alphabet cannot name a dealer, and letting it
# through reaches code that puts it in a response header: a key containing CR/LF
# made ``resp.headers["ETag"] = ...`` raise, so the endpoint answered 500 with a
# stack trace on input that should simply not resolve. Validate at the door.
_DEALER_KEY_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


def _etag_token(value) -> str:
    """Reduce a string to characters that are legal inside a quoted ETag."""
    return re.sub(r"[^A-Za-z0-9._-]", "", "" if value is None else str(value))


def _clean_dealer_key(dealer_key) -> str | None:
    """The key as a safe lookup token, or ``None`` if it cannot name a dealer."""
    key = ("" if dealer_key is None else str(dealer_key)).strip()
    if not key or not _DEALER_KEY_RE.match(key):
        return None
    return key


def _host_to_dealer_id(url: str | None) -> str | None:
    """``https://www.longotoyota.com/`` -> ``longotoyota-com`` (matches cars.dealer_id)."""
    if not url:
        return None
    host = urlparse(url if "://" in url else "http://" + url).netloc.lower().strip()
    if host.startswith("www."):
        host = host[4:]
    host = host.split(":")[0].strip()
    if not host:
        return None
    return host.replace(".", "-")


def _fetch_dealership_by_id(dealership_id: int) -> dict | None:
    from backend.db.inventory_db import get_conn

    conn = get_conn()
    try:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, name, website_url, oem_brand, platform,
                   google_rating, google_review_count,
                   street_address, city, state, zip_code,
                   latitude, longitude, phone
            FROM dealerships WHERE id = ?
            """,
            (dealership_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def _find_dealership_by_dealer_id(dealer_id: str) -> dict | None:
    """Match a dealerships row to a cars.dealer_id via website_url host."""
    from backend.db.inventory_db import get_conn

    conn = get_conn()
    try:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, name, website_url, oem_brand, platform,
                   google_rating, google_review_count,
                   street_address, city, state, zip_code,
                   latitude, longitude, phone
            FROM dealerships
            WHERE website_url IS NOT NULL AND website_url <> ''
            """
        )
        for row in cur.fetchall():
            if _host_to_dealer_id(row["website_url"]) == dealer_id:
                return dict(row)
        return None
    finally:
        conn.close()


# One page view reads the dealer's cards three times (the page itself, then
# ``/cars`` and ``/filter-options`` from the browser). Keep the resolved cards per
# (dealer_id, data token) for a short TTL so a view costs one ``cards_for_dealer``.
_DEALER_CARDS_TTL_S = 30.0
_DEALER_CARDS_MAX_ENTRIES = 128
_dealer_cards_cache: "OrderedDict[str, tuple[float, str, list[str]]]" = OrderedDict()
_dealer_cards_lock = threading.Lock()


def clear_dealer_cards_cache() -> None:
    with _dealer_cards_lock:
        _dealer_cards_cache.clear()


def _dealer_cards_token(dealer_id: str) -> str:
    """Validator for one dealer's cards WITHOUT building them.

    ``grid_scope_token`` moves with every input a card depends on (the ``cars`` write
    fingerprint, the day, the serializer revision, market bands, attribution
    verdicts, the include-incomplete switch) and ``store_generation`` with the
    background refresher, so an unchanged token means the same cards -- to the
    token's own 60 s resample on Postgres, the same bound /api/listings/cars has."""
    from backend.db.repositories.grid_cards_repo import grid_scope_token, store_generation

    raw = repr(((dealer_id or "").strip(), grid_scope_token(), store_generation()))
    return hashlib.blake2b(raw.encode("utf-8", "surrogatepass"), digest_size=10).hexdigest()


def _dealer_grid_cards_json(dealer_id: str) -> list[str]:
    """Stored listing-card JSON strings for one dealer, in grid order.

    Same cards /listings serves (the persisted ``listings_grid_cards`` store, see
    ``grid_cards_repo``), so a facet value derived here selects the same cars there
    -- main.js filters the SERIALIZED card fields. A dealer-scoped query: this page
    never touches the rest of the fleet (it used to slice the whole-fleet grid).
    Cached per (dealer, :func:`_dealer_cards_token`) for ``_DEALER_CARDS_TTL_S``.
    """
    from backend.db.repositories.grid_cards_repo import cards_for_dealer

    key = (dealer_id or "").strip()
    if not key:
        return []
    token = _dealer_cards_token(key)
    now = time.monotonic()
    with _dealer_cards_lock:
        hit = _dealer_cards_cache.get(key)
        if hit is not None and hit[1] == token and (now - hit[0]) < _DEALER_CARDS_TTL_S:
            _dealer_cards_cache.move_to_end(key)
            return hit[2]
    cards = cards_for_dealer(key).cards_json()
    with _dealer_cards_lock:
        _dealer_cards_cache[key] = (now, token, cards)
        _dealer_cards_cache.move_to_end(key)
        while len(_dealer_cards_cache) > _DEALER_CARDS_MAX_ENTRIES:
            _dealer_cards_cache.popitem(last=False)
    return cards


def _dealer_grid_cars(dealer_id: str) -> list[dict]:
    """Parsed listing cards for one dealer (see :func:`_dealer_grid_cards_json`)."""
    return [json.loads(c) for c in _dealer_grid_cards_json(dealer_id)]


_epa_makes_cache: frozenset[str] | None = None

# Marques the EPA catalogue has no rows for at all (McLaren/INEOS are there under a
# longer legal name, which the suffix strip below handles; Wagoneer is Jeep's
# spun-off brand and simply predates nothing in the file).
_EXTRA_KNOWN_MAKES = frozenset({"wagoneer"})


def _known_catalog_makes() -> frozenset[str]:
    """Lower-cased marque names from the ``epa_master`` catalogue (cached per process).

    ``_facet_make_valid`` gates on ``MAKE_TO_COUNTRY``, a hand-kept dict in
    search_repo, and every marque missing from it disappears from the Make facet.
    Measured 2026-07-30 over the 72,272 active rows: 150 cars behind 35 dropped make
    strings — Lucid, McLaren, Rivian, Polestar, INEOS, Scion, Karma, Aston Martin,
    Saturn, Pontiac, Suzuki, smart, Rolls-Royce. That is not an EV-lot edge case, it
    is a hand-maintained list going stale, so this reads the marque names out of the
    vehicle catalogue instead (153 names). It keeps rejecting the feed junk the hand
    list was there to reject, because none of that is a catalogue make either: after
    the change 18 cars across 15 strings are still dropped, and every one of them is
    junk ("Audi A3 premium", "2022", "Land", "Flat Trailer") or a non-car brand
    (Freightliner, Harley-Davidson, Yamaha, Keystone, Forest River, RawMaxx…).
    """
    global _epa_makes_cache
    if _epa_makes_cache is not None:
        return _epa_makes_cache
    names: set[str] = set(_EXTRA_KNOWN_MAKES)
    try:
        from backend.db.inventory_db import get_conn

        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute("SELECT DISTINCT make FROM epa_master WHERE make IS NOT NULL")
            for (mk,) in cur.fetchall():
                m = str(mk or "").strip().lower()
                if not m:
                    continue
                names.add(m)
                # "mclaren automotive" / "ineos automotive" are the catalogue's legal
                # names; dealers list the marque.
                for suffix in (" automotive", " motors", " cars"):
                    if m.endswith(suffix) and len(m) > len(suffix) + 1:
                        names.add(m[: -len(suffix)].strip())
        finally:
            conn.close()
    except Exception:
        # No catalogue (or no DB) → fall back to _facet_make_valid alone, which is
        # exactly the behaviour before this function existed.
        return frozenset(_EXTRA_KNOWN_MAKES)
    _epa_makes_cache = frozenset(names)
    return _epa_makes_cache


def _dealer_facet_make_valid(make: str, *, base_valid) -> bool:
    """``_facet_make_valid`` plus the marques its country dict has never heard of."""
    if base_valid(make):
        return True
    m = str(make or "").strip().lower()
    if not m:
        return False
    return m in _known_catalog_makes()


def _dealer_facets(cars: list[dict]) -> dict:
    """Facet option lists for ONE dealer, derived from that dealer's serialized cards.

    Same reason as :func:`_dealer_grid_cars`: build the checkboxes from the exact
    values the client-side filter compares against and every option is guaranteed to
    match at least one car. ``forced_induction`` is intentionally absent — the grid
    serializer does not emit it, so such a facet can only ever empty the grid.
    """
    # Same label canonicalisation /listings uses, so "CADILLAC"/"Cadillac" and
    # "LARIAT"/"Lariat" collapse to one option and feed-junk makes ("Audi A3
    # premium") stay out of the list. Every comparison downstream is
    # case-insensitive, so the label choice never changes what a filter matches.
    from backend.db.repositories.listings_repo import (
        _canonical_facet_label,
        _facet_make_valid,
        _facet_transmission_sane,
        _normalize_facet_key,
        _normalize_make_capitalization,
    )
    from backend.db.repositories.search_repo import _lookup_make_country
    from backend.utils.field_clean import (
        is_effectively_empty,
        sort_body_style_presets,
        sort_fuel_type_presets,
    )
    from backend.utils.interior_color_buckets import sort_paint_family_ids

    def _txt(val) -> str:
        """Card field as facet text — placeholders read as absent, not as a value.

        These lists are built from the SERIALIZED cards, so they also see the
        serializer's null placeholder (``DISPLAY_DASH``/"—", plus N/A, Unknown,
        None…). Offering that as a checkbox advertises "—" as a drivetrain and as a
        trim; ``is_effectively_empty`` is the same test the rest of the codebase
        uses for "this field has no value".
        """
        if is_effectively_empty(val):
            return ""
        return str(val).strip()

    make_variants: dict[str, list[str]] = {}
    model_variants: dict[tuple[str, str], list[str]] = {}
    trim_variants: dict[tuple[str, str, str], list[str]] = {}
    fuels: dict[str, str] = {}
    transmissions: dict[str, str] = {}
    drivetrains: dict[str, str] = {}
    body_styles: dict[str, str] = {}
    packages: dict[str, str] = {}
    cylinders: set[int] = set()
    ext_families: set[str] = set()
    int_families: set[str] = set()
    car_rows: list[dict] = []
    seen_rows: set[tuple] = set()
    pkg_keys: set[tuple[str, str, str]] = set()

    for c in cars:
        make, model, trim = _txt(c.get("make")), _txt(c.get("model")), _txt(c.get("trim"))
        fuel = _txt(c.get("fuel_type"))
        drive = _txt(c.get("drivetrain"))
        body = _txt(c.get("body_style"))
        trans = _txt(c.get("transmission"))
        mk = md = ""
        if make and _dealer_facet_make_valid(make, base_valid=_facet_make_valid):
            make = _normalize_make_capitalization(make)
            mk = _normalize_facet_key(make)
            if make not in make_variants.setdefault(mk, []):
                make_variants[mk].append(make)
            if model:
                md = _normalize_facet_key(model)
                if model not in model_variants.setdefault((mk, md), []):
                    model_variants[(mk, md)].append(model)
                if trim:
                    tk = _normalize_facet_key(trim)
                    if trim not in trim_variants.setdefault((mk, md, tk), []):
                        trim_variants[(mk, md, tk)].append(trim)
        if fuel:
            fuels.setdefault(fuel.lower(), fuel)
        # Same sanity gate /listings applies, so a stray cylinder count in the
        # transmission column does not become a "2" checkbox.
        if trans and _facet_transmission_sane(trans):
            transmissions.setdefault(trans.lower(), trans)
        if drive:
            drivetrains.setdefault(drive.lower(), drive)
        if body:
            body_styles.setdefault(body.lower(), body)
        try:
            cyl = int(c.get("cylinders"))
        except (TypeError, ValueError):
            cyl = None
        else:
            cylinders.add(cyl)
        for fam in c.get("exterior_color_families") or []:
            if fam:
                ext_families.add(str(fam))
        for fam in c.get("interior_color_families") or []:
            if fam:
                int_families.add(str(fam))
        for name in c.get("package_names") or []:
            n = _txt(name)
            if n:
                packages.setdefault(n.lower(), n)
                # (make, model, name) rows behind main.js's cascadePackages — without
                # them PACKAGE_ROWS is empty and the Packages list never narrows to
                # the checked make/model. Cars whose make did not survive the facet
                # test keep a row under "" so their package stays selectable while no
                # make/model is checked (and correctly disappears once one is).
                pkg_keys.add((mk, md, n.lower()))

        # Cascade table (make → model → trim → …). main.js rebuilds this from the
        # full car payload once it lands; this copy only has to be right for the
        # frames rendered before that.
        row = (make or None, model or None, trim or None, fuel or None, cyl,
               drive or None, body or None, None)
        if row not in seen_rows:
            seen_rows.add(row)
            car_rows.append({
                "make": row[0], "model": row[1], "trim": row[2], "fuel": row[3],
                "cyl": row[4], "drive": row[5], "body_style": row[6], "induction": row[7],
            })

    make_label: dict[str, str] = {
        mk: _canonical_facet_label("", variants=v) for mk, v in make_variants.items()
    }
    model_label: dict[tuple[str, str], str] = {
        k: _canonical_facet_label("", variants=v) for k, v in model_variants.items()
    }
    # Package cascade rows, keyed to the same canonical labels the Make/Model
    # checkboxes carry (cascadePackages compares them case-insensitively).
    package_rows = sorted(
        (
            {
                "make": make_label.get(mk, ""),
                "model": model_label.get((mk, md), ""),
                "name": packages[nl],
            }
            for mk, md, nl in pkg_keys
            if nl in packages
        ),
        key=lambda r: (r["name"].lower(), r["make"].lower(), r["model"].lower()),
    )

    make_labels = sorted(make_label.values(), key=str.lower)
    country_to_makes: dict[str, list[str]] = {}
    for m in make_labels:
        country = _lookup_make_country(m) or _lookup_make_country(m.lower())
        if country:
            country_to_makes.setdefault(country, []).append(m)

    return {
        "makes": make_labels,
        # (make, model) / (make, model, trim) so the cascade can hide models that
        # do not belong to a checked make — same shape /listings ships.
        "model_rows": [
            (make_label[mk], model_label[(mk, md)]) for (mk, md) in sorted(model_variants)
        ],
        "trim_rows": [
            (make_label[mk], model_label[(mk, md)], _canonical_facet_label("", variants=v))
            for (mk, md, _tk), v in sorted(trim_variants.items())
        ],
        "fuel_types": sort_fuel_type_presets(list(fuels.values())),
        "cylinders": sorted(cylinders),
        "transmissions": sorted(transmissions.values(), key=str.lower),
        "drivetrains": sorted(drivetrains.values(), key=str.lower),
        "body_styles": sort_body_style_presets(list(body_styles.values())),
        "exterior_colors": sort_paint_family_ids(ext_families),
        "interior_colors": sort_paint_family_ids(int_families),
        "all_package_names": sorted(packages.values(), key=str.lower),
        "package_rows": package_rows,
        # Country is noise on a single-franchise rooftop; only worth a section when
        # the lot actually spans more than one.
        "countries": sorted(country_to_makes) if len(country_to_makes) > 1 else [],
        "country_to_makes": country_to_makes if len(country_to_makes) > 1 else {},
        "car_rows": car_rows,
    }


def _dealer_inventory(dealer_id: str) -> dict:
    """Counts, first-paint cards and dealer-scoped facet lists for one dealer."""
    cars = _dealer_grid_cars(dealer_id)
    total = len(cars)
    new_count = sum(
        1 for c in cars if str(c.get("condition") or "").strip().lower() == "new"
    )
    # Cards the photo-attribution overlay says may not be on THIS lot (the grid
    # serializer sets location_confirmed=False; see cars_repo.car_attribution_states).
    # They stay in the list and in every count -- the listing is real and the shopper
    # can still act on it -- but the page stops implying the whole number is on the
    # lot here. Two of these rooftops list a whole ownership group's feed, so the
    # headline count is the single most over-confident number on the page.
    unconfirmed_count = sum(1 for c in cars if c.get("location_confirmed") is False)

    dealer_name_from_car = None
    dealer_url_from_car = None
    for card in cars:
        if not dealer_name_from_car and card.get("dealer_name"):
            dealer_name_from_car = card.get("dealer_name")
        if not dealer_url_from_car and card.get("dealer_url"):
            dealer_url_from_car = card.get("dealer_url")
        if dealer_name_from_car and dealer_url_from_car:
            break

    return {
        "total": total,
        "new_count": new_count,
        "used_count": max(total - new_count, 0),
        "unconfirmed_count": unconfirmed_count,
        "confirmed_total": max(total - unconfirmed_count, 0),
        "cars": cars[:_GRID_BOOTSTRAP],
        "facets": _dealer_facets(cars),
        "dealer_name_from_car": dealer_name_from_car,
        "dealer_url_from_car": dealer_url_from_car,
    }


def _resolve_dealer(dealer_key: str) -> tuple[dict | None, str | None]:
    """(dealership_row | None, dealer_id | None) from a numeric id or a dealer_id string."""
    key = _clean_dealer_key(dealer_key)
    if not key:
        return None, None
    if key.isdigit():
        try:
            row = _fetch_dealership_by_id(int(key))
        except (TypeError, ValueError, OverflowError):
            return None, None
        if not row:
            return None, None
        return row, _host_to_dealer_id(row.get("website_url"))
    # Treat as cars.dealer_id (hostname-derived); find its dealerships row by host.
    return _find_dealership_by_dealer_id(key), key


def _dealer_id_for_key(dealer_key: str) -> str | None:
    """``dealer_key`` -> ``cars.dealer_id`` without the registry scan when possible.

    :func:`_resolve_dealer` reads every ``dealerships`` row to match by host; the
    JSON endpoints only need the ``cars.dealer_id``, which a non-numeric key already
    is, so they skip that.
    """
    key = _clean_dealer_key(dealer_key)
    if not key:
        return None
    if key.isdigit():
        try:
            row = _fetch_dealership_by_id(int(key))
        except (TypeError, ValueError, OverflowError):
            return None
        return _host_to_dealer_id(row.get("website_url")) if row else None
    return key


def _dealer_filter_zip(dealership: dict | None, lat: float | None, lon: float | None) -> str:
    """A 5-digit ZIP for this dealer, used only to satisfy main.js's filter gate.

    Registry ZIP first, then the nearest ZIP to the rooftop's coordinates. Dealers we
    only know from their own listings (no registry row, no geo) fall back to a
    non-ZIP sentinel: it keeps the filters live and resolves to no coordinates, which
    is exactly the truth about where that dealer is.
    """
    raw = str((dealership or {}).get("zip_code") or "").strip()[:5]
    if _ZIP5_RE.match(raw):
        return raw
    if lat is not None and lon is not None:
        try:
            from backend.db.geo import nearest_us_postal_meta

            meta = nearest_us_postal_meta(lat, lon) or {}
            near = str(meta.get("postal_code") or "").strip()
            if _ZIP5_RE.match(near):
                return near
        except Exception:
            pass
    return _UNKNOWN_FILTER_ZIP


def _attach_lease_matches(conn, dealer_id: str, offers: list[dict]) -> None:
    """Attach parsed lease terms + qualifying cars to each lease offer.

    Matching is deterministic (regex over the offer's fine print + a plain
    inventory join — no model, no network), so it's cheap enough to run on view.
    A ``lease_offer_matches`` cache keyed by the content-derived ``offer_hash``
    is used as the fast path (stale offers self-invalidate); anything missing is
    computed on first view and cached. Set ``DEALERSHIP_LEASE_MATCH_ON_VIEW=0``
    to read cache-only (e.g. if population is handled entirely out of band).
    """
    import os

    lease_offers = [o for o in offers if (o.get("type") or "").lower() == "lease"]
    if not lease_offers:
        return

    compute_on_view = (os.environ.get("DEALERSHIP_LEASE_MATCH_ON_VIEW") or "1").strip().lower() in (
        "1", "true", "yes", "on",
    )
    try:
        from backend.scanner.specials.lease_matches_store import get_matches_for_dealer

        cached = get_matches_for_dealer(conn, dealer_id)
    except Exception:
        cached = {}

    for off in lease_offers:
        h = off.get("offer_hash")
        row = cached.get(h) if h else None
        if row is None and compute_on_view:
            try:
                from backend.intelligence.lease_matcher import compute_offer_matches
                from backend.scanner.specials.lease_matches_store import upsert_offer_match

                res = compute_offer_matches(conn, dealer_id, off)
                if h:
                    upsert_offer_match(
                        conn, dealer_id, h, offer_id=off.get("id"),
                        extracted=res["extracted"], summary=res["summary"],
                        matches=res["matches"], confidence=res["confidence"],
                    )
                row = res
            except Exception:
                row = None
        if row:
            off["lease_summary"] = row.get("summary")
            off["lease_matches"] = row.get("matches") or []
            off["lease_extracted"] = row.get("extracted")


def _dealer_specials(dealer_id: str) -> dict:
    """Read stored specials for a dealer and group them by offer type for render."""
    from backend.db.inventory_db import get_conn
    from backend.scanner.specials.store import get_specials_for_dealer

    conn = get_conn()
    try:
        offers = get_specials_for_dealer(conn, dealer_id)
        _attach_lease_matches(conn, dealer_id, offers)
    except Exception:
        offers = []
    finally:
        conn.close()

    order = ["lease", "finance", "manager", "cash", "other"]
    labels = {
        "lease": "Lease Specials",
        "finance": "Finance & APR Offers",
        "manager": "Manager Specials",
        "cash": "Cash & Rebates",
        "other": "Other Offers",
    }
    buckets: dict[str, list] = {}
    for off in offers:
        key = (off.get("type") or "other").lower()
        if key not in labels:
            key = "other"
        buckets.setdefault(key, []).append(off)

    groups = [
        {"type": t, "label": labels[t], "offers": buckets[t]}
        for t in order
        if buckets.get(t)
    ]
    scraped_at = offers[0].get("scraped_at") if offers else None
    return {"total": len(offers), "groups": groups, "scraped_at": scraped_at}


def _dealer_reviews(dealer_id: str) -> dict:
    """Read published reviews + aggregate summary for a dealer (defensive)."""
    from backend.db.inventory_db import get_conn
    from backend.reviews.store import (
        ensure_reviews_table,
        get_reviews_for_dealer,
        review_summary,
    )

    empty = {
        "summary": {"count": 0, "avg_rating": None, "addon_fee_count": 0, "addon_fees": []},
        "reviews": [],
    }
    conn = get_conn()
    try:
        # Idempotent + additive; guarantees reads run against an existing table
        # (no scanner creates it) and avoids an aborted-transaction cascade.
        try:
            cur = conn.cursor()
            ensure_reviews_table(cur)
            conn.commit()
        except Exception:
            conn.rollback()
        summary = review_summary(conn, dealer_id)
        reviews = get_reviews_for_dealer(conn, dealer_id, limit=100)
        return {"summary": summary, "reviews": reviews}
    except Exception:
        return empty
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _active_facet_selection() -> dict:
    """URL-provided filter state, in the shape ``dealership.html`` renders from.

    Only the CHECKED options are server-rendered (the rest are hydrated from
    ``/api/dealership/<key>/filter-options``), so this has to be right for the very
    first frame to filter correctly on a shared/bookmarked link.
    """
    g = request.args.getlist

    def scalar(key: str) -> str:
        vals = [v.strip() for v in request.args.getlist(key) if v.strip()]
        return vals[-1] if vals else ""

    return {
        "make": g("make"),
        "model": g("model"),
        "trim": g("trim"),
        "fuel_type": g("fuel_type"),
        "cylinders": g("cylinders"),
        "transmission": g("transmission"),
        "drivetrain": g("drivetrain"),
        "body_style": g("body_style"),
        "exterior_color": g("exterior_color"),
        "interior_color": g("interior_color"),
        "country": g("country"),
        "package": g("package"),
        "max_price": scalar("max_price"),
        "max_mileage": scalar("max_mileage"),
        "inventory_condition": scalar("inventory_condition"),
    }


def api_dealership_cars(dealer_key: str):
    """Every active listing for one dealer, in listings-grid card shape.

    This is the dealer-scoped stand-in for ``/api/listings/cars``: the dealership
    page publishes it as ``__DS_listingsCarsPrefetchPromise`` so main.js hydrates
    from ~one rooftop instead of the whole 71k-row fleet.
    """
    dealer_id = _dealer_id_for_key(dealer_key)
    if not dealer_id:
        return jsonify({"ok": False, "error": "not_found"}), 404

    # The validator is the data token (see _dealer_cards_token), computed WITHOUT
    # building the cards, so a matching If-None-Match is a 304 with no card work. The
    # dealer part is reduced to etag-safe characters -- for a numeric key it derives
    # from a registry website_url.
    etag = f'W/"{_etag_token(dealer_id)}-{_dealer_cards_token(dealer_id)}"'
    if (request.headers.get("If-None-Match") or "").strip() == etag:
        resp = make_response("", 304)
    else:
        cards = _dealer_grid_cards_json(dealer_id)
        body = '{"ok":true,"cars":[' + ",".join(cards) + "]}"
        resp = make_response(body)
        resp.headers["Content-Type"] = "application/json"
    resp.headers["ETag"] = etag
    resp.headers["Cache-Control"] = "private, no-cache"
    return resp


def api_dealership_filter_options(dealer_key: str):
    """Dealer-scoped facet lists, in the ``/api/listings/filter-options`` shape."""
    dealer_id = _dealer_id_for_key(dealer_key)
    if not dealer_id:
        return jsonify({"ok": False, "error": "not_found"}), 404
    facets = _dealer_facets(_dealer_grid_cars(dealer_id))
    facets.pop("car_rows", None)
    resp = make_response(jsonify({"ok": True, "forced_inductions": [], **facets}))
    resp.headers["Cache-Control"] = "private, max-age=60"
    return resp


def dealership_research_page(dealer_key: str):
    dealership, dealer_id = _resolve_dealer(dealer_key)

    inventory = _dealer_inventory(dealer_id) if dealer_id else None
    has_inventory = bool(inventory and inventory["total"])

    specials = _dealer_specials(dealer_id) if dealer_id else {"total": 0, "groups": [], "scraped_at": None}

    try:
        reviews_data = (
            _dealer_reviews(dealer_id)
            if dealer_id
            else {"summary": {"count": 0, "avg_rating": None, "addon_fee_count": 0, "addon_fees": []}, "reviews": []}
        )
    except Exception:
        reviews_data = {"summary": {"count": 0, "avg_rating": None, "addon_fee_count": 0, "addon_fees": []}, "reviews": []}

    # Require *something* to show: a registry row or real inventory. Otherwise 404
    # rather than render an empty shell.
    if not dealership and not has_inventory:
        abort(404)

    # Header/contact fields: prefer the registry row, fall back to values derived
    # from the dealer's own car listings.
    name = (dealership or {}).get("name") or (inventory or {}).get("dealer_name_from_car") or dealer_id
    from backend.utils.field_clean import normalize_optional_url

    website_url = normalize_optional_url(
        (dealership or {}).get("website_url") or (inventory or {}).get("dealer_url_from_car")
    )

    lat = (dealership or {}).get("latitude")
    lon = (dealership or {}).get("longitude")
    try:
        lat = float(lat) if lat is not None else None
        lon = float(lon) if lon is not None else None
    except (TypeError, ValueError):
        lat = lon = None
    has_geo = lat is not None and lon is not None

    address_parts = [
        (dealership or {}).get("street_address"),
        (dealership or {}).get("city"),
        (dealership or {}).get("state"),
        (dealership or {}).get("zip_code"),
    ]
    full_address = ", ".join(p for p in address_parts if p) or None

    # Navigate-there deep links: prefer precise geo, fall back to the address text.
    if has_geo:
        nav_dest = f"{lat},{lon}"
        gmaps_url = f"https://www.google.com/maps/dir/?api=1&destination={nav_dest}"
        apple_url = f"https://maps.apple.com/?daddr={nav_dest}"
        waze_url = f"https://waze.com/ul?ll={nav_dest}&navigate=yes"
    elif full_address:
        from urllib.parse import quote_plus

        enc = quote_plus(full_address)
        gmaps_url = f"https://www.google.com/maps/dir/?api=1&destination={enc}"
        apple_url = f"https://maps.apple.com/?daddr={enc}"
        waze_url = f"https://waze.com/ul?q={enc}&navigate=yes"
    else:
        gmaps_url = apple_url = waze_url = None

    saved_car_ids: list[int] = []
    is_hidden = False
    uid = session.get("user_id")
    if uid is not None:
        try:
            from backend.db.inventory_db import get_saved_car_ids

            saved_car_ids = get_saved_car_ids(int(uid))
        except (TypeError, ValueError):
            saved_car_ids = []
        if dealer_id:
            try:
                from backend.db.inventory_db import is_dealer_hidden

                is_hidden = bool(is_dealer_hidden(int(uid), dealer_id))
            except Exception:
                is_hidden = False

    from backend.listings.routes import pack_car_rows

    facets = (inventory or {}).get("facets") or {}

    return render_template(
        "dealership.html",
        dealer_key=dealer_key,
        dealer_id=dealer_id,
        facets=facets,
        car_rows_packed=pack_car_rows(facets.get("car_rows") or []),
        active=_active_facet_selection(),
        saved_car_ids=saved_car_ids,
        is_hidden=is_hidden,
        filter_zip=_dealer_filter_zip(dealership, lat, lon),
        dealership=dealership,
        name=name,
        website_url=website_url,
        phone=(dealership or {}).get("phone"),
        oem_brand=(dealership or {}).get("oem_brand"),
        google_rating=(dealership or {}).get("google_rating"),
        google_review_count=(dealership or {}).get("google_review_count"),
        full_address=full_address,
        lat=lat,
        lon=lon,
        has_geo=has_geo,
        nav_gmaps_url=gmaps_url,
        nav_apple_url=apple_url,
        nav_waze_url=waze_url,
        inventory=inventory
        or {"total": 0, "new_count": 0, "used_count": 0, "unconfirmed_count": 0, "cars": []},
        specials=specials,
        review_summary=reviews_data["summary"],
        reviews=reviews_data["reviews"],
        logged_in=bool(session.get("user_id")),
        review_error=request.args.get("review_error"),
    )


def register(app) -> None:
    """Attach the dealership research page route (additive; bare endpoint name)."""
    app.add_url_rule(
        "/dealership/<dealer_key>",
        view_func=dealership_research_page,
        methods=["GET"],
    )
    app.add_url_rule(
        "/api/dealership/<dealer_key>/cars",
        view_func=api_dealership_cars,
        methods=["GET"],
    )
    app.add_url_rule(
        "/api/dealership/<dealer_key>/filter-options",
        view_func=api_dealership_filter_options,
        methods=["GET"],
    )
