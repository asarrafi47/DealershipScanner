"""
Premium listings: nearby dealerships with active inventory, capped or search-scoped.
"""
from __future__ import annotations

import logging
import os
import re
from datetime import datetime, timezone
from typing import Any

_logger = logging.getLogger(__name__)

_DEFAULT_CAP = max(1, int(os.environ.get("NEARBY_DEALER_LIST_CAP", "10")))


_OFF_VALUES = ("0", "false", "no", "off")


def _autoregister_enabled() -> bool:
    """Kill switch for creating registry rows from scanned rooftops.

    Gates the *offline* pass only (:func:`register_unregistered_rooftops`, reached
    from ``repair_mis_stamped_registry_ids``). Nothing on the request path calls
    :func:`_register_rooftops` any more — see that function's docstring.
    """
    return (
        os.environ.get("NEARBY_DEALERS_AUTOREGISTER") or "1"
    ).strip().lower() not in _OFF_VALUES


def _stamp_repair_enabled() -> bool:
    """Kill switch for the substring-collision stamp repair (see :func:`_mis_stamped_pairs`).

    Off suppresses both halves: the in-memory correction the picker applies to the
    rooftops it just read, and the ``UPDATE cars`` the offline pass performs.
    """
    return (
        os.environ.get("NEARBY_DEALERS_REPAIR_STAMPS") or "1"
    ).strip().lower() not in _OFF_VALUES


def _active_listing_counts(registry_ids: list[int]) -> dict[int, int]:
    """
    Active inventory per registry id (missing ids → omitted; use ``.get(id, 0)``).

    Resolved through :func:`_rooftop_slices` rather than read straight off
    ``cars.dealership_registry_id``, so a store whose cars the feed left unattributed
    still counts its own storefront's stock.
    """
    ids = _parse_registry_ids(registry_ids)
    if not ids:
        return {}

    from backend.db.inventory_db import get_conn
    from backend.listings.dealer_registry_match import registry_id_by_dealer_host

    conn = get_conn()
    try:
        rooftops = _rooftop_inventory(conn)
        host_to_registry = registry_id_by_dealer_host(conn)
        registry_rows = _registry_rows_by_id(conn)
    finally:
        conn.close()

    _correct_mis_stamped_rooftops(rooftops, registry_rows)
    counts, _unregistered = _rooftop_slices(rooftops, host_to_registry, registry_rows)
    return {rid: counts[rid] for rid in ids if counts.get(rid)}


def _active_listing_counts_by_dealer_id(dealer_ids: list[str]) -> dict[str, int]:
    """Active inventory count per scanner ``dealer_id`` slug."""
    dids = sorted({(d or "").strip().lower() for d in dealer_ids if (d or "").strip()})
    if not dids:
        return {}

    from backend.db.inventory_db import get_conn

    placeholders = ",".join("?" * len(dids))
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT lower(trim(dealer_id)) AS did, COUNT(*) AS n
        FROM cars
        WHERE COALESCE(listing_active, 1) = 1
          AND dealer_id IS NOT NULL
          AND trim(dealer_id) != ''
          AND lower(trim(dealer_id)) IN ({placeholders})
        GROUP BY lower(trim(dealer_id))
        """,
        dids,
    )
    rows = cur.fetchall()
    conn.close()
    out: dict[str, int] = {}
    for did_raw, count_raw in rows:
        did = (did_raw or "").strip().lower()
        if not did:
            continue
        try:
            out[did] = int(count_raw)
        except (TypeError, ValueError):
            continue
    return out


def attach_listing_counts(dealers: list[dict[str, Any]]) -> None:
    """
    Set ``listing_count`` from the registry id, falling back to the website slug.

    Registry id wins outright rather than taking the larger of the two. Taking the max
    let a group storefront claim its siblings' stock: Nissan of Costa Mesa's slug covers
    2,080 cars but only 304 of them are its own, and 2,080 is a number no dealer filter
    can ever deliver.
    """
    from backend.dev.dealers import slug_from_url

    registry_ids = [
        int(d["registry_id"])
        for d in dealers
        if d.get("registry_id") is not None
    ]
    dealer_ids: list[str] = []
    for d in dealers:
        url = (d.get("website_url") or "").strip()
        if url:
            slug = slug_from_url(url)
            if slug and slug != "dealer":
                dealer_ids.append(slug)

    by_registry = _active_listing_counts(registry_ids)
    by_dealer_id = _active_listing_counts_by_dealer_id(dealer_ids)

    for d in dealers:
        count = 0
        rid = d.get("registry_id")
        registered = False
        if rid is not None:
            try:
                count = by_registry.get(int(rid), 0)
                registered = True
            except (TypeError, ValueError):
                pass
        if not registered:
            # Google-only rows have no registry id; the site slug is all we have.
            url = (d.get("website_url") or "").strip()
            if url:
                did = slug_from_url(url)
                if did:
                    count = by_dealer_id.get(did, 0)
        d["listing_count"] = count


def _parse_registry_ids(registry_ids: list[int]) -> list[int]:
    ids: list[int] = []
    for raw in registry_ids:
        try:
            rid = int(raw)
        except (TypeError, ValueError):
            continue
        if rid > 0:
            ids.append(rid)
    return ids


def _last_inventory_scraped_at(registry_ids: list[int]) -> dict[int, str]:
    """Latest ``cars.scraped_at`` per registry id (any listing, active or not)."""
    ids = _parse_registry_ids(registry_ids)
    if not ids:
        return {}

    from backend.db.inventory_db import get_conn

    placeholders = ",".join("?" * len(ids))
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT CAST(dealership_registry_id AS INTEGER) AS rid, MAX(scraped_at) AS last_scraped_at
        FROM cars
        WHERE dealership_registry_id IS NOT NULL
          AND CAST(dealership_registry_id AS INTEGER) IN ({placeholders})
        GROUP BY CAST(dealership_registry_id AS INTEGER)
        """,
        ids,
    )
    rows = cur.fetchall()
    conn.close()
    out: dict[int, str] = {}
    for rid_raw, ts in rows:
        if not ts:
            continue
        try:
            out[int(rid_raw)] = str(ts)
        except (TypeError, ValueError):
            continue
    return out


def _last_inventory_scraped_at_by_dealer_id(dealer_ids: list[str]) -> dict[str, str]:
    """Latest ``cars.scraped_at`` per scanner ``dealer_id`` slug."""
    dids = sorted({(d or "").strip().lower() for d in dealer_ids if (d or "").strip()})
    if not dids:
        return {}

    from backend.db.inventory_db import get_conn

    placeholders = ",".join("?" * len(dids))
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT lower(trim(dealer_id)) AS did, MAX(scraped_at) AS last_scraped_at
        FROM cars
        WHERE dealer_id IS NOT NULL
          AND trim(dealer_id) != ''
          AND lower(trim(dealer_id)) IN ({placeholders})
        GROUP BY lower(trim(dealer_id))
        """,
        dids,
    )
    rows = cur.fetchall()
    conn.close()
    out: dict[str, str] = {}
    for did_raw, ts in rows:
        did = (did_raw or "").strip().lower()
        if not did or not ts:
            continue
        out[did] = str(ts)
    return out


def _last_catalog_scan_at(
    *,
    registry_ids: list[int],
    dealer_ids: list[str],
) -> tuple[dict[int, str], dict[str, str]]:
    """``dealer_catalog.last_scan_at`` keyed by registry id and dealer slug (Postgres only)."""
    reg_ids = _parse_registry_ids(registry_ids)
    dids = sorted({(d or "").strip().lower() for d in dealer_ids if (d or "").strip()})
    if not reg_ids and not dids:
        return {}, {}

    from backend.db.inventory_pg import is_inventory_postgres, pg_connect

    if not is_inventory_postgres():
        return {}, {}

    conn = pg_connect()
    try:
        cur = conn.cursor()
        clauses: list[str] = []
        params: list[Any] = []
        if reg_ids:
            clauses.append("registry_id = ANY(%s)")
            params.append(reg_ids)
        if dids:
            clauses.append("dealer_id = ANY(%s)")
            params.append(dids)
        cur.execute(
            f"""
            SELECT registry_id, dealer_id, last_scan_at
            FROM dealer_catalog
            WHERE last_scan_at IS NOT NULL AND ({' OR '.join(clauses)})
            """,
            tuple(params),
        )
        by_registry: dict[int, str] = {}
        by_dealer_id: dict[str, str] = {}
        for reg_raw, did_raw, ts in cur.fetchall():
            if not ts:
                continue
            ts_s = str(ts)
            if reg_raw is not None:
                try:
                    by_registry[int(reg_raw)] = ts_s
                except (TypeError, ValueError):
                    pass
            did = (did_raw or "").strip().lower()
            if did:
                by_dealer_id[did] = ts_s
        return by_registry, by_dealer_id
    finally:
        conn.close()


def _max_iso_timestamp(*values: str | None) -> str | None:
    best: str | None = None
    for raw in values:
        ts = (raw or "").strip()
        if not ts:
            continue
        if best is None or ts > best:
            best = ts
    return best


def attach_last_synced_at(dealers: list[dict[str, Any]]) -> None:
    """Set ``last_synced_at`` on each dealer row (inventory scrape vs catalog scan, latest wins)."""
    from backend.dev.dealers import slug_from_url

    registry_ids = [
        int(d["registry_id"])
        for d in dealers
        if d.get("registry_id") is not None
    ]
    dealer_ids: list[str] = []
    for d in dealers:
        url = (d.get("website_url") or "").strip()
        if url:
            slug = slug_from_url(url)
            if slug and slug != "dealer":
                dealer_ids.append(slug)

    inv_ts = _last_inventory_scraped_at(registry_ids)
    inv_by_did = _last_inventory_scraped_at_by_dealer_id(dealer_ids)
    cat_by_reg, cat_by_did = _last_catalog_scan_at(
        registry_ids=registry_ids,
        dealer_ids=dealer_ids,
    )

    for d in dealers:
        candidates: list[str | None] = []
        rid = d.get("registry_id")
        if rid is not None:
            try:
                rid_i = int(rid)
            except (TypeError, ValueError):
                rid_i = None
            if rid_i:
                candidates.append(inv_ts.get(rid_i))
                candidates.append(cat_by_reg.get(rid_i))
        url = (d.get("website_url") or "").strip()
        if url:
            did = slug_from_url(url)
            if did:
                candidates.append(cat_by_did.get(did))
                candidates.append(inv_by_did.get(did))
        d["last_synced_at"] = _max_iso_timestamp(*candidates)


def _rooftop_inventory(conn: Any) -> dict[str, dict[str, Any]]:
    """
    Active listings per scanned site, keyed by normalized ``dealer_url`` host.

    ``listing_count`` is everything the site serves; ``by_registry`` splits it by
    ``cars.dealership_registry_id`` (0 = unattributed). The split matters because a
    storefront is not always one store: of the 2,080 active cars on
    ``nissanofcostamesa.com`` only 304 are Costa Mesa's — the feed files the rest under
    five sibling rooftops, which ``backend.scripts.attribute_feed_rooftops`` verified
    against Google Places and wrote into that column. Counting the whole site as one
    dealer is what made the picker promise 2,080 and the search deliver 304.
    """
    from backend.db.dealer_geo import normalize_dealer_host

    cur = conn.cursor()
    cur.execute(
        """
        SELECT dealer_url, dealership_registry_id, MAX(dealer_name) AS dealer_name, COUNT(*) AS n
        FROM cars
        WHERE COALESCE(listing_active, 1) = 1
          AND TRIM(COALESCE(dealer_url, '')) != ''
        GROUP BY dealer_url, dealership_registry_id
        """
    )
    out: dict[str, dict[str, Any]] = {}
    for dealer_url, registry_raw, dealer_name, count_raw in cur.fetchall():
        host = normalize_dealer_host(str(dealer_url or ""))
        if not host:
            continue
        try:
            listing_count = int(count_raw or 0)
        except (TypeError, ValueError):
            continue
        if listing_count <= 0:
            continue
        try:
            stamped = int(registry_raw or 0)
        except (TypeError, ValueError):
            stamped = 0
        entry = out.setdefault(
            host,
            {
                "host": host,
                "dealer_url": str(dealer_url or "").strip(),
                "name": "",
                "listing_count": 0,
                "by_registry": {},
            },
        )
        entry["listing_count"] += listing_count
        by_registry = entry["by_registry"]
        key = stamped if stamped > 0 else 0
        by_registry[key] = by_registry.get(key, 0) + listing_count
        if not entry["name"]:
            entry["name"] = str(dealer_name or "").strip()
    return out


def _rooftop_slices(
    rooftops: dict[str, dict[str, Any]],
    host_to_registry: dict[str, int],
    registry_rows: dict[int, dict[str, Any]],
) -> tuple[dict[int, int], dict[str, int]]:
    """
    Split scanned inventory into ``(count per registry id, count per unregistered host)``.

    This is *the* definition of "this dealer's cars", and it is deliberately the same
    one ``dealer_registry_sql_filter`` applies in SQL and ``carDealershipRegistryId``
    applies in the browser: the feed's per-vehicle rooftop when it named one, otherwise
    the storefront we scraped it from. Anything else and the number on a picker
    checkbox stops matching the grid you get after ticking it.
    """
    by_registry: dict[int, int] = {}
    unregistered: dict[str, int] = {}
    for host, rooftop in rooftops.items():
        own_id = int(host_to_registry.get(host) or 0)
        for stamped, count in (rooftop.get("by_registry") or {}).items():
            # An id we cannot resolve to a live registry row cannot be filtered on
            # either, so those cars fall back to their storefront.
            rid = stamped if stamped > 0 and stamped in registry_rows else own_id
            if rid > 0:
                by_registry[rid] = by_registry.get(rid, 0) + count
            else:
                unregistered[host] = unregistered.get(host, 0) + count
    return by_registry, unregistered


def _rooftop_geopoints(conn: Any) -> dict[str, dict[str, Any]]:
    """
    Normalized dealer host → ``{lat, lon, city, state}`` from ``dealer_geopoints``.

    This, not the ``dealerships`` registry, is the geo source of record for rooftops we
    scan: the registry was populated by area discovery and covers only 14 of the 214
    hosts that currently have live inventory, which is why a registry-keyed radius
    search reported zero dealers everywhere.
    """
    from backend.db.dealer_geo import TRUSTED_LOCALITY_SOURCES, normalize_dealer_host

    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT dealer_url, lat, lon, city, state, geocode_source
            FROM dealer_geopoints
            WHERE lat IS NOT NULL AND lon IS NOT NULL
            """
        )
        rows = cur.fetchall()
    except Exception:
        return {}

    out: dict[str, dict[str, Any]] = {}
    for dealer_url, lat, lon, city, state, source in rows:
        host = normalize_dealer_host(str(dealer_url or ""))
        if not host:
            continue
        try:
            point = {"lat": float(lat), "lon": float(lon)}
        except (TypeError, ValueError):
            continue
        # Same rule as dealer_locality_for_url: a name-matched geocode can land in a
        # neighbouring town, so only trusted sources are allowed to label the city.
        trusted = str(source or "") in TRUSTED_LOCALITY_SOURCES
        point["city"] = (city or "").strip() if trusted else ""
        point["state"] = (state or "").strip().upper() if trusted else ""
        prev = out.get(host)
        if prev is not None and (prev.get("city") or not point["city"]):
            continue
        out[host] = point
    return out


def _registry_rows_by_id(conn: Any) -> dict[int, dict[str, Any]]:
    """
    Active, non-duplicate registry rows keyed by id (one query, no per-dealer connections).

    ``hosts`` is the set of normalized hosts the row itself claims — empty for a rooftop
    that has no site of its own (Carson Nissan, reached only through a group feed), which
    is what :func:`_mis_stamped_pairs` uses to tell attribution from a bad stamp.
    """
    from backend.db.dealer_geo import normalize_dealer_host

    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, name, city, state, latitude, longitude, website_url, dealer_website_url
        FROM dealerships
        WHERE is_active = 1 AND duplicate_of_id IS NULL
        """
    )
    out: dict[int, dict[str, Any]] = {}
    for rid_raw, name, city, state, lat, lon, website_url, dealer_website_url in cur.fetchall():
        try:
            rid = int(rid_raw)
        except (TypeError, ValueError):
            continue
        if rid <= 0:
            continue
        hosts = {
            normalize_dealer_host(str(url or ""))
            for url in (website_url, dealer_website_url)
        }
        hosts.discard("")
        out[rid] = {
            "name": (name or "").strip(),
            "city": (city or "").strip(),
            "state": (state or "").strip(),
            "latitude": lat,
            "longitude": lon,
            "hosts": hosts,
        }
    return out


def _mis_stamped_pairs(
    rooftops: dict[str, dict[str, Any]],
    registry_rows: dict[int, dict[str, Any]],
) -> list[tuple[str, int, int]]:
    """
    ``(host, stamped id, car count)`` for stamps only an unanchored host match explains.

    ``cars.dealership_registry_id`` is normally either the storefront's own registry row
    or a sibling rooftop the feed named, and a stamp that merely disagrees with the
    storefront is not evidence of a bug: most cross-attributed pairs are group feeds
    (``audifletcherjones.com`` serving Fletcher Jones Motorcars cars, and so on), which
    ``backend.scripts.attribute_feed_rooftops`` is what writes.

    What this looks for is narrower and has one cause: the stamped row claims a site of
    its own, and that site's host is a *substring* of the host the cars were scraped
    from. Nothing but an unanchored ``LIKE '%host%'`` match produces that, which is what
    the old backfill used — registry 170 is "Subaru of America" at ``subaru.com``, so it
    stamped ``sutherlinsubaru.com``'s cars and put a Camden NJ dealer 0.54 miles from ZIP
    08103 holding a Tennessee store's inventory.

    Rows the storefront can no longer be told apart from (a stamped row with no site of
    its own) are deliberately left alone: clearing those would destroy the attribution
    that splits a group feed.
    """
    pairs: list[tuple[str, int, int]] = []
    for host, rooftop in rooftops.items():
        if not host:
            continue
        for stamped, count in (rooftop.get("by_registry") or {}).items():
            if stamped <= 0 or stamped not in registry_rows:
                continue
            reg_hosts = registry_rows[stamped].get("hosts") or set()
            if not reg_hosts or host in reg_hosts:
                continue
            if any(rh and rh != host and rh in host for rh in reg_hosts):
                pairs.append((host, int(stamped), int(count)))
    return pairs


def _apply_stamp_corrections(
    rooftops: dict[str, dict[str, Any]],
    pairs: list[tuple[str, int, int]],
) -> None:
    """Move mis-stamped counts back to the unattributed bucket in the loaded rooftops."""
    for host, rid, _count in pairs:
        rooftop = rooftops.get(host)
        if not rooftop:
            continue
        by_registry = rooftop.get("by_registry") or {}
        moved = by_registry.pop(rid, 0)
        if moved:
            by_registry[0] = by_registry.get(0, 0) + moved


def _clear_mis_stamped_registry_ids(pairs: list[tuple[str, int, int]]) -> int:
    """
    Null out the stamps in :func:`_mis_stamped_pairs` so the rooftop can be re-attributed.

    Clearing rather than re-pointing is what makes the row self-heal: it is the state
    ``backfill_dealership_registry_ids`` (``IS NULL OR <= 0``) and the host fallback in
    ``dealer_registry_sql_filter`` both act on, so once the rooftop has a registry row of
    its own — from :func:`register_unregistered_rooftops`, or from discovery — the next
    backfill stamps it correctly with the anchored patterns. Returns rows updated.

    **Offline only.** Its one caller is :func:`repair_mis_stamped_registry_ids`; the
    picker applies the same correction in memory without writing.
    """
    from backend.db.inventory_db import get_conn
    from backend.listings.dealer_registry_match import dealer_url_like_patterns

    if not pairs:
        return 0
    try:
        conn = get_conn()
    except Exception:
        return 0
    total = 0
    try:
        cur = conn.cursor()
        for host, rid, _count in pairs:
            patterns = dealer_url_like_patterns(host)
            if not patterns:
                continue
            where = " OR ".join("LOWER(IFNULL(dealer_url, '')) LIKE ?" for _ in patterns)
            cur.execute(
                f"""
                UPDATE cars
                SET dealership_registry_id = NULL
                WHERE CAST(dealership_registry_id AS INTEGER) = ?
                  AND ({where})
                """,
                (rid, *patterns),
            )
            total += int(cur.rowcount or 0)
        conn.commit()
    except Exception:
        # A read-only picker request must never fail because the repair write did.
        try:
            conn.rollback()
        except Exception:
            pass
        return 0
    finally:
        conn.close()
    return total


def _correct_mis_stamped_rooftops(
    rooftops: dict[str, dict[str, Any]],
    registry_rows: dict[int, dict[str, Any]],
) -> list[tuple[str, int, int]]:
    """
    Fold mis-stamped counts back into their storefront **in memory only**; returns the pairs.

    Both callers — :func:`_active_listing_counts` and
    :func:`_dealers_with_inventory_near` — run inside a GET, so this must not write.
    It used to: it called :func:`_clear_mis_stamped_registry_ids`, which issues
    ``UPDATE cars SET dealership_registry_id = NULL``. The DB half now happens only
    in :func:`repair_mis_stamped_registry_ids`, offline. The counts a request
    returns are identical either way — the fold is applied to the rooftops the
    request already read.
    """
    if not _stamp_repair_enabled():
        return []
    pairs = _mis_stamped_pairs(rooftops, registry_rows)
    if not pairs:
        return []
    _apply_stamp_corrections(rooftops, pairs)
    return pairs


def register_unregistered_rooftops(
    *, dry_run: bool = False
) -> tuple[int, list[tuple[str, str, int]]]:
    """
    Whole-table offline pass: give every scanned rooftop a ``dealerships`` row.

    This is where rooftop registration lives now. It used to run inside
    ``GET /api/nearby-dealers``, radius-scoped to the request — an unauthenticated
    read endpoint that INSERTed into ``dealerships``, so probe traffic alone created
    rows (three of them during a verification pass). The scope is the only thing
    that changed by moving it: offline it registers every unregistered rooftop that
    has active inventory and a geopoint, not just the ones near one caller's ZIP.

    Returns ``(rows registered, [(host, name, active car count)] considered)``. With
    ``dry_run`` the candidate list is computed and nothing is written, which is what
    the count in the caller's log line is measured from.
    """
    from backend.db.inventory_db import get_conn
    from backend.listings.dealer_registry_match import registry_id_by_dealer_host

    conn = get_conn()
    try:
        rooftops = _rooftop_inventory(conn)
        geopoints = _rooftop_geopoints(conn)
        host_to_registry = registry_id_by_dealer_host(conn)
        registry_rows = _registry_rows_by_id(conn)
    finally:
        conn.close()

    _correct_mis_stamped_rooftops(rooftops, registry_rows)
    _counts, unregistered = _rooftop_slices(rooftops, host_to_registry, registry_rows)

    pending: list[tuple[dict[str, Any], dict[str, Any] | None]] = []
    considered: list[tuple[str, str, int]] = []
    for host, count in sorted(unregistered.items(), key=lambda kv: -kv[1]):
        point = geopoints.get(host)
        if not point:
            # No verified coordinates means we cannot place the rooftop, and a
            # registry row we cannot place is a row no radius search will return.
            continue
        rooftop = rooftops[host]
        considered.append((host, str(rooftop.get("name") or ""), int(count)))
        pending.append((rooftop, point))

    if dry_run or not pending:
        return 0, considered
    registered = _register_rooftops(pending, registry_rows, set(host_to_registry.values()))
    return len(registered), considered


def repair_mis_stamped_registry_ids() -> tuple[int, list[tuple[str, int, int]]]:
    """
    Offline registry-maintenance pass: ``(rows cleared, mis-stamped pairs found)``.

    Entry point for ``backend/scripts/backfill_dealership_registry.py``, which runs
    it immediately before ``backfill_dealership_registry_ids()``. It does two writes,
    in the order that script depends on:

    1. clears the substring-collision stamps (:func:`_mis_stamped_pairs`), freeing
       those cars to be re-stamped;
    2. registers scanned rooftops that have no ``dealerships`` row at all — moved
       here off ``GET /api/nearby-dealers``. The backfill that follows is what then
       stamps their cars with the new ids.

    The return value covers step 1 only, because that is what the script prints; the
    step 2 count goes to the module logger. (That script is out of scope for this
    change, so its printed summary does not yet mention registrations.)
    """
    from backend.db.inventory_db import get_conn

    conn = get_conn()
    try:
        rooftops = _rooftop_inventory(conn)
        registry_rows = _registry_rows_by_id(conn)
    finally:
        conn.close()
    pairs = _mis_stamped_pairs(rooftops, registry_rows) if _stamp_repair_enabled() else []
    cleared = _clear_mis_stamped_registry_ids(pairs) if pairs else 0

    if _autoregister_enabled():
        registered, considered = register_unregistered_rooftops()
        _logger.info(
            "rooftop registry sync: registered %d of %d unregistered rooftop(s) with a geopoint",
            registered,
            len(considered),
        )
    return cleared, pairs


def _dealer_name_key(name: str) -> str:
    """Punctuation-insensitive dealer name key (``Mtn. View Ford`` → ``mtn view ford``)."""
    return " ".join(re.sub(r"[^a-z0-9]+", " ", (name or "").lower()).split())


def _match_unlinked_registry_row(
    rooftop: dict[str, Any],
    point: dict[str, Any] | None,
    registry_rows: dict[int, dict[str, Any]],
    taken: set[int],
) -> int:
    """
    Registry row that is unmistakably this rooftop but carries no URL, else 0.

    ``upsert_discovery_row``'s fuzzy dedupe is deliberately not used here: at its 88
    threshold it folds sibling-brand rooftops of one dealer group together (it maps
    "Shottenkirk Acura Huntsville" onto the existing "Shottenkirk Honda Huntsville"
    row, and "Hiley Volkswagen of Huntsville" onto "Hiley Mazda of Huntsville"), which
    would hand two different stores the same registry id and merge their inventory.
    Exact name + state, within a mile of the registry's own coordinates, does not.
    """
    key = _dealer_name_key(rooftop.get("name") or "")
    if not key:
        return 0
    state = ((point or {}).get("state") or "").strip().upper()
    coords = None
    if point:
        coords = (point["lat"], point["lon"])

    from backend.db.geo import haversine

    for rid, row in registry_rows.items():
        if rid in taken:
            continue
        if _dealer_name_key(row.get("name") or "") != key:
            continue
        if state and (row.get("state") or "").strip().upper() != state:
            continue
        if coords and row.get("latitude") is not None:
            try:
                if haversine(coords[0], coords[1], float(row["latitude"]), float(row["longitude"])) > 1.0:
                    continue
            except (TypeError, ValueError):
                continue
        return rid
    return 0


def _register_rooftops(
    pending: list[tuple[dict[str, Any], dict[str, Any] | None]],
    registry_rows: dict[int, dict[str, Any]],
    taken: set[int],
) -> dict[str, int]:
    """
    Give scanned rooftops a ``dealerships`` id, adopting an unlinked row or inserting one.

    Every dealer filter downstream (the picker checkboxes, ``dealer_registry_id`` in
    ``search_cars``) is keyed on ``dealerships.id``, so a rooftop with no registry row
    is invisible no matter how much inventory it has — and the registry, built by area
    discovery, only ever covered 14 of the 214 hosts we actively scan. Returns
    ``host → registry id`` for whatever was resolved; failures are simply omitted.

    **Offline only.** Its one caller is :func:`register_unregistered_rooftops`. It
    used to be called from :func:`_dealers_with_inventory_near`, i.e. from inside
    ``GET /api/nearby-dealers``, which made an unauthenticated read endpoint INSERT
    into ``dealerships``.
    """
    from backend.db.inventory_db import get_conn
    from backend.db.inventory_pg import is_inventory_postgres

    out: dict[str, int] = {}
    if not pending:
        return out

    now = datetime.now(timezone.utc).isoformat()
    postgres = is_inventory_postgres()
    try:
        conn = get_conn()
    except Exception:
        return out
    try:
        cur = conn.cursor()
        for rooftop, point in pending:
            host = rooftop.get("host") or ""
            url = (rooftop.get("dealer_url") or "").strip()
            name = (rooftop.get("name") or "").strip()
            if not host or not url or not name:
                continue
            city = ((point or {}).get("city") or "").strip()
            state = ((point or {}).get("state") or "").strip().upper()
            lat = (point or {}).get("lat")
            lon = (point or {}).get("lon")

            rid = _match_unlinked_registry_row(rooftop, point, registry_rows, taken)
            if rid > 0:
                cur.execute(
                    """
                    UPDATE dealerships
                    SET website_url = ?,
                        dealer_website_url = ?,
                        latitude = COALESCE(latitude, ?),
                        longitude = COALESCE(longitude, ?),
                        source_web = 1
                    WHERE id = ?
                      AND TRIM(COALESCE(website_url, '')) = ''
                      AND TRIM(COALESCE(dealer_website_url, '')) = ''
                    """,
                    (url, url, lat, lon, rid),
                )
                if cur.rowcount:
                    out[host] = rid
                    taken.add(rid)
                    continue

            columns = (
                "name, website_url, city, state, latitude, longitude, created_at, "
                "dealer_website_url, source_web, is_active"
            )
            params = (name, url, city, state, lat, lon, now, url)
            if postgres:
                cur.execute(
                    f"INSERT INTO dealerships ({columns}) VALUES (?,?,?,?,?,?,?,?,1,1) RETURNING id",
                    params,
                )
                row = cur.fetchone()
                new_id = int(row[0]) if row else 0
            else:
                cur.execute(
                    f"INSERT INTO dealerships ({columns}) VALUES (?,?,?,?,?,?,?,?,1,1)",
                    params,
                )
                new_id = int(cur.lastrowid or 0)
            if new_id > 0:
                out[host] = new_id
                taken.add(new_id)
                registry_rows[new_id] = {
                    "name": name,
                    "city": city,
                    "state": state,
                    "latitude": lat,
                    "longitude": lon,
                }
        conn.commit()
    except Exception:
        # A read-only picker request must never fail because the registry write did.
        try:
            conn.rollback()
        except Exception:
            pass
        return {}
    finally:
        conn.close()
    return out


def _dealers_with_inventory_near(
    lat: float,
    lon: float,
    radius_miles: float,
) -> list[dict[str, Any]]:
    """
    Dealers that have active inventory within *radius_miles* of (lat, lon).

    Built rooftop-first: split active listings by the rooftop that actually holds them
    (see :func:`_rooftop_slices`), place each one with its own ``dealer_geopoints``
    coordinates (registry coords only as a fallback), then attach a registry id —
    creating one when the rooftop is missing from the registry, since the caller's
    checkboxes filter by registry id.

    A sibling rooftop reached only through a group feed (Carson Nissan, whose cars we
    scrape from ``nissanofcostamesa.com``) has no site of its own, so it is placed by
    the coordinates the attribution pass geocoded for it — 25 miles from the storefront,
    which is where those cars really are.

    **Reads only.** A rooftop that has no ``dealerships`` row is left out of the
    result rather than given one here: the picker's checkboxes filter by registry id,
    and minting an id is a write. :func:`register_unregistered_rooftops` mints them
    offline; until it runs, such a rooftop is not offerable as a filter.
    """
    from backend.db.geo import haversine
    from backend.db.inventory_db import get_conn
    from backend.listings.dealer_registry_match import registry_id_by_dealer_host

    conn = get_conn()
    try:
        rooftops = _rooftop_inventory(conn)
        geopoints = _rooftop_geopoints(conn)
        host_to_registry = registry_id_by_dealer_host(conn)
        registry_rows = _registry_rows_by_id(conn)
    finally:
        conn.close()

    # A stamp that only an unanchored host match explains is folded back into the
    # storefront that served the cars, so the dealer is not parked in the stamped
    # row's metro. In memory only — the matching UPDATE runs offline.
    _correct_mis_stamped_rooftops(rooftops, registry_rows)

    counts, _leftover = _rooftop_slices(rooftops, host_to_registry, registry_rows)

    # A registry row's own scanned site is the better place to put it: dealer_geopoints
    # is verified per host, registry coordinates are whatever discovery found.
    point_by_registry: dict[int, dict[str, Any]] = {}
    name_by_registry: dict[int, str] = {}
    for host, rid_raw in host_to_registry.items():
        rid = int(rid_raw or 0)
        if rid <= 0 or host not in rooftops:
            continue
        point = geopoints.get(host)
        if point and rid not in point_by_registry:
            point_by_registry[rid] = point
        name = (rooftops[host].get("name") or "").strip()
        if name and rid not in name_by_registry:
            name_by_registry[rid] = name

    out: list[dict[str, Any]] = []
    for reg_id, listing_count in counts.items():
        if listing_count <= 0:
            continue
        reg_row = registry_rows.get(reg_id) or {}
        point = point_by_registry.get(reg_id)
        dest: tuple[float, float] | None = None
        if point:
            dest = (point["lat"], point["lon"])
        elif reg_row.get("latitude") is not None:
            try:
                dest = (float(reg_row["latitude"]), float(reg_row["longitude"]))
            except (TypeError, ValueError):
                dest = None
        if not dest:
            continue
        dist = haversine(lat, lon, dest[0], dest[1])
        if dist > radius_miles:
            continue
        out.append(
            {
                "id": reg_id,
                "name": name_by_registry.get(reg_id) or reg_row.get("name") or "",
                "city": (point or {}).get("city") or reg_row.get("city") or "",
                "state": (point or {}).get("state") or reg_row.get("state") or "",
                "distance_miles": round(dist, 2),
                "listing_count": listing_count,
            }
        )

    out.sort(key=lambda d: (float(d["distance_miles"]), -int(d["listing_count"])))
    return out


def _cars_matching_search_in_radius(
    zip_code: str,
    radius_miles: float,
    search_query: str,
) -> list[dict[str, Any]]:
    """Inventory rows for *search_query* within ZIP/radius (aligned with listings geo)."""
    from backend.db.inventory_db import search_cars
    from backend.utils.hybrid_search import (
        _has_structured_filters,
        filters_dict_to_search_cars_kwargs,
        hybrid_search_with_kwargs,
    )
    from backend.utils.query_parser import parse_natural_query

    sql_kwargs: dict[str, Any] = {
        "zip_code": zip_code,
        "radius_miles": float(radius_miles),
        "include_incomplete": False,
    }
    parsed = parse_natural_query(search_query)
    if _has_structured_filters(parsed):
        merged = {**sql_kwargs, **filters_dict_to_search_cars_kwargs(parsed)}
        return search_cars(**merged)
    try:
        cars, _meta = hybrid_search_with_kwargs(
            search_query, sql_kwargs, vector_top_k=150
        )
        return cars
    except Exception:
        return search_cars(**sql_kwargs)


def resolve_nearby_dealers_for_listings(
    *,
    zip_code: str,
    radius_miles: float,
    search_query: str | None = None,
    cap: int | None = None,
) -> dict[str, Any]:
    """
    Dealers within radius that have ≥1 active listing.

    - **Broad** (no search query): sort by listing count desc, distance asc; cap at ``cap`` (default 10).
    - **Search** (non-empty ``search_query``): all dealers in radius that have matching
      inventory for the hybrid search (same ZIP/radius + query), uncapped.
    """
    from backend.db.dealerships_db import search_dealerships_by_radius
    from backend.db.geo import zip_to_coords

    limit = cap if cap is not None else _DEFAULT_CAP
    zip_code = (zip_code or "").strip()
    q = (search_query or "").strip()
    search_mode = len(q) >= 2

    coords = zip_to_coords(zip_code)
    if not coords or radius_miles <= 0:
        return {
            "ok": False,
            "dealers": [],
            "mode": "search" if search_mode else "broad",
            "capped": False,
            "shown": 0,
            "total_with_inventory": 0,
            "total_in_radius": 0,
        }

    lat, lon = coords
    radius_f = float(radius_miles)
    with_stock = _dealers_with_inventory_near(lat, lon, radius_f)
    total_with_inventory = len(with_stock)
    # Registry radius search alone under-counts: a rooftop is placed by its own
    # dealer_geopoints coordinates here, and those can disagree with the registry
    # coordinates search_dealerships_by_radius filters on.
    in_radius_ids = {
        int(row["id"])
        for row in search_dealerships_by_radius(lat, lon, radius_f)
        if str(row.get("id") or "").strip().isdigit()
    }
    in_radius_ids.update(int(d["id"]) for d in with_stock)
    total_in_radius = len(in_radius_ids)
    if not with_stock:
        return {
            "ok": True,
            "dealers": [],
            "mode": "search" if search_mode else "broad",
            "capped": False,
            "shown": 0,
            "total_with_inventory": 0,
            "total_in_radius": total_in_radius,
        }

    if search_mode:
        from backend.db.inventory_db import get_conn as inv_get_conn
        from backend.listings.dealer_registry_match import (
            collect_registry_ids_from_cars,
            registry_id_by_dealer_host,
        )

        cars = _cars_matching_search_in_radius(zip_code, radius_f, q)
        conn = inv_get_conn()
        try:
            host_to_registry = registry_id_by_dealer_host(conn)
        finally:
            conn.close()
        match_ids = collect_registry_ids_from_cars(cars, host_to_registry)
        filtered = [d for d in with_stock if d["id"] in match_ids]
        filtered.sort(
            key=lambda d: (-int(d["listing_count"]), float(d.get("distance_miles") or 9999)),
        )
        return {
            "ok": True,
            "dealers": filtered,
            "mode": "search",
            "capped": False,
            "shown": len(filtered),
            "total_with_inventory": total_with_inventory,
            "total_in_radius": total_in_radius,
            "search_query": q,
        }

    with_stock.sort(
        key=lambda d: (-int(d["listing_count"]), float(d.get("distance_miles") or 9999)),
    )
    capped = total_with_inventory > limit
    shown = with_stock[:limit] if capped else with_stock
    return {
        "ok": True,
        "dealers": shown,
        "mode": "broad",
        "capped": capped,
        "shown": len(shown),
        "total_with_inventory": total_with_inventory,
        "total_in_radius": total_in_radius,
        "cap": limit,
    }
