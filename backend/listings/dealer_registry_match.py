"""
Resolve ``dealership_registry_id`` from listing rows (column or ``dealer_url`` host).

Used by nearby-dealer picker, ``search_cars`` dealer filters, and listings geo backfill.
"""
from __future__ import annotations

from typing import Any

from backend.db.dealer_geo import normalize_dealer_host


def registry_id_by_dealer_host(conn: Any) -> dict[str, int]:
    """Map normalized dealer host → active registry id (first wins per host)."""
    out: dict[str, int] = {}
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, website_url, dealer_website_url
        FROM dealerships
        WHERE is_active = 1 AND duplicate_of_id IS NULL
        """
    )
    for row in cur.fetchall():
        # Row factory varies by caller: the picker passes a plain tuple-row connection,
        # search_cars passes a dict-row one. Unpacking a dict row yields column *names*,
        # so every row used to fail int() and the map came back empty on Postgres —
        # which silently disabled the host fallback below (Irvine Subaru's 41 unstamped
        # cars matched the picker's count but returned 0 search results).
        if isinstance(row, dict):
            rid_raw = row.get("id")
            website_url = row.get("website_url")
            dealer_website_url = row.get("dealer_website_url")
        else:
            rid_raw, website_url, dealer_website_url = row[0], row[1], row[2]
        try:
            rid = int(rid_raw)
        except (TypeError, ValueError):
            continue
        if rid <= 0:
            continue
        for url in (website_url, dealer_website_url):
            host = normalize_dealer_host(str(url or ""))
            if host and host not in out:
                out[host] = rid
    return out


def hosts_for_registry_ids(
    registry_ids: list[int],
    host_to_registry: dict[str, int],
) -> list[str]:
    """Hosts that map to any of *registry_ids* (for SQL ``dealer_url`` LIKE filters)."""
    want = {int(x) for x in registry_ids if int(x) > 0}
    if not want:
        return []
    hosts: list[str] = []
    seen: set[str] = set()
    for host, rid in host_to_registry.items():
        if rid in want and host not in seen:
            seen.add(host)
            hosts.append(host)
    return hosts


def resolve_car_dealership_registry_id(
    car: dict[str, Any],
    *,
    host_to_registry: dict[str, int] | None = None,
) -> int:
    """Registry id from column, else from ``dealer_url`` host lookup."""
    try:
        reg = int(car.get("dealership_registry_id") or 0)
    except (TypeError, ValueError):
        reg = 0
    if reg > 0:
        return reg
    host = normalize_dealer_host(str(car.get("dealer_url") or ""))
    if not host:
        return 0
    if host_to_registry is None:
        from backend.db.inventory_db import get_conn

        conn = get_conn()
        try:
            host_to_registry = registry_id_by_dealer_host(conn)
        finally:
            conn.close()
    return int(host_to_registry.get(host) or 0)


def collect_registry_ids_from_cars(
    cars: list[dict[str, Any]],
    host_to_registry: dict[str, int],
) -> set[int]:
    out: set[int] = set()
    for car in cars:
        rid = resolve_car_dealership_registry_id(car, host_to_registry=host_to_registry)
        if rid > 0:
            out.add(rid)
    return out


def dealer_url_like_patterns(host: str) -> list[str]:
    """
    LIKE patterns that match ``dealer_url`` only when its host really *is* *host*.

    Anchored on ``//`` (and the ``www.`` that :func:`normalize_dealer_host` strips)
    because a bare ``%host%`` is a substring test, and registry rows carry bare brand
    domains: id 170 is "Subaru of America" at ``subaru.com``, which ``%subaru.com%``
    matches for every ``*subaru.com`` rooftop we scan — eleven of them today. The
    picker compares normalized hosts for equality, so this must too.
    """
    h = (host or "").strip().lower()
    if not h:
        return []
    return [f"%//{h}", f"%//{h}/%", f"%//www.{h}", f"%//www.{h}/%"]


def dealer_registry_sql_filter(
    registry_ids: list[int],
    host_to_registry: dict[str, int],
    *,
    placeholders_fn,
) -> tuple[str, list[Any]]:
    """
    SQL fragment: linked registry id OR unlinked row whose ``dealer_url`` host matches.
    """
    valid = []
    for x in registry_ids:
        try:
            i = int(x)
            if i > 0:
                valid.append(i)
        except (TypeError, ValueError):
            continue
    if not valid:
        return "", []

    ph = placeholders_fn(valid)
    parts = [f"CAST(dealership_registry_id AS INTEGER) IN ({ph})"]
    params: list[Any] = list(valid)

    hosts = hosts_for_registry_ids(valid, host_to_registry)
    if hosts:
        host_clauses = []
        for host in hosts:
            for pattern in dealer_url_like_patterns(host):
                host_clauses.append("LOWER(IFNULL(dealer_url, '')) LIKE ?")
                params.append(pattern)
        parts.append(
            "("
            "(dealership_registry_id IS NULL OR CAST(dealership_registry_id AS INTEGER) <= 0)"
            f" AND ({' OR '.join(host_clauses)})"
            ")"
        )
    return f" AND ({' OR '.join(parts)})", params
