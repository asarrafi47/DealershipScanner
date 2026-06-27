"""
Resolve ``dealership_registry_id`` from listing rows (column or ``dealer_url`` host).

Used by nearby-dealer picker, ``search_cars`` dealer filters, and listings geo backfill.
"""
from __future__ import annotations

from typing import Any

from backend.db.dealer_geo import normalize_dealer_host


def _registry_id_from_dealer_id(dealer_id: Any) -> int:
    """``db-{id}`` scanner slug → registry primary key."""
    s = str(dealer_id or "").strip()
    if s.startswith("db-"):
        try:
            return int(s[3:])
        except ValueError:
            return 0
    return 0


def registry_id_by_dealer_slug(host_to_registry: dict[str, int]) -> dict[str, int]:
    """Map ``dealers.json`` ``dealer_id`` slug → registry id via shared URL host."""
    out: dict[str, int] = {}
    try:
        import json

        from backend.scanner.constants import MANIFEST_PATH

        if not MANIFEST_PATH.is_file():
            return out
        with open(MANIFEST_PATH, encoding="utf-8") as f:
            dealers = json.load(f)
        if not isinstance(dealers, list):
            return out
        for row in dealers:
            if not isinstance(row, dict):
                continue
            slug = str(row.get("dealer_id") or "").strip()
            if not slug:
                continue
            host = normalize_dealer_host(str(row.get("url") or ""))
            rid = host_to_registry.get(host)
            if rid and slug not in out:
                out[slug] = int(rid)
    except OSError:
        pass
    return out


def registry_id_by_dealer_host(conn: Any) -> dict[str, int]:
    """Map normalized dealer host → active registry id (first wins per host)."""
    out: dict[str, int] = {}
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, normalized_host, website_url, dealer_website_url
        FROM dealerships
        WHERE is_active = 1 AND duplicate_of_id IS NULL
        """
    )
    for rid_raw, normalized_host, website_url, dealer_website_url in cur.fetchall():
        try:
            rid = int(rid_raw)
        except (TypeError, ValueError):
            continue
        if rid <= 0:
            continue
        host = str(normalized_host or "").strip().lower()
        if not host:
            for url in (website_url, dealer_website_url):
                host = normalize_dealer_host(str(url or ""))
                if host:
                    break
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
    slug_to_registry: dict[str, int] | None = None,
) -> int:
    """Registry id from column, ``db-{id}`` slug, ``dealer_url`` host, or manifest ``dealer_id``."""
    try:
        reg = int(car.get("dealership_registry_id") or 0)
    except (TypeError, ValueError):
        reg = 0
    if reg > 0:
        return reg

    reg = _registry_id_from_dealer_id(car.get("dealer_id"))
    if reg > 0:
        return reg

    if host_to_registry is None:
        from backend.db.inventory_db import get_conn

        conn = get_conn()
        try:
            host_to_registry = registry_id_by_dealer_host(conn)
        finally:
            conn.close()

    host = normalize_dealer_host(str(car.get("dealer_url") or ""))
    if host:
        hit = int(host_to_registry.get(host) or 0)
        if hit > 0:
            return hit
        # Path/query variants (e.g. CarMax store URLs) when netloc match missed.
        url_lower = str(car.get("dealer_url") or "").lower()
        if url_lower:
            for reg_host, rid in host_to_registry.items():
                if reg_host and reg_host in url_lower:
                    return int(rid)

    slug = str(car.get("dealer_id") or "").strip()
    if slug:
        if slug_to_registry is None:
            slug_to_registry = registry_id_by_dealer_slug(host_to_registry)
        hit = int(slug_to_registry.get(slug) or 0)
        if hit > 0:
            return hit

    return 0


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
