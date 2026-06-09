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
    for rid_raw, website_url, dealer_website_url in cur.fetchall():
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
            host_clauses.append("LOWER(IFNULL(dealer_url, '')) LIKE ?")
            params.append(f"%{host.lower()}%")
        parts.append(
            "("
            "(dealership_registry_id IS NULL OR CAST(dealership_registry_id AS INTEGER) <= 0)"
            f" AND ({' OR '.join(host_clauses)})"
            ")"
        )
    return f" AND ({' OR '.join(parts)})", params
