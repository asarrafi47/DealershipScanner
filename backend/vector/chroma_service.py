"""
Chroma persistent collections aligned with project SQLite databases:

- inventory_cars      → inventory.db table cars
- inventory_dealers   → inventory.db table dealerships (if present)
- bmw_normalized      → data/oem/bmw/bmw_intake.db bmw_normalized_dealer
- bmw_partial         → bmw_partial_staging

users.db is intentionally not indexed (credentials / PII).

Run: python -m backend.vector reindex
"""

from __future__ import annotations

import hashlib
import sqlite3
from pathlib import Path
from typing import Any

from SCRAPING.paths import ROOT

from backend.db.inventory_db import DB_PATH as INVENTORY_DB_PATH, get_conn as inventory_get_conn
from oem_intake.paths import BMW_DB_PATH
from oem_intake.sqlite_store import connect as bmw_connect
from oem_intake.sqlite_store import init_schema as bmw_init_schema


def get_persist_dir() -> Path:
    d = ROOT / "data" / "chroma"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _client():
    import chromadb
    from chromadb.config import Settings

    return chromadb.PersistentClient(
        path=str(get_persist_dir()),
        settings=Settings(anonymized_telemetry=False),
    )


def _chunked(items: list[tuple], size: int = 400):
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _clear_collection(col: Any) -> None:
    """Remove all points from a collection (Chroma API varies by version)."""
    try:
        res = col.get(limit=500_000)
        ids = list(res.get("ids") or [])
        for i in range(0, len(ids), 500):
            col.delete(ids=ids[i : i + 500])
    except Exception:
        pass


def reindex_inventory_cars(client: Any) -> int:
    col = client.get_or_create_collection(
        "inventory_cars",
        metadata={"sqlite": "inventory.db", "table": "cars"},
    )
    conn = inventory_get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, vin, title, year, make, model, trim, dealer_name, zip_code, price, mileage
        FROM cars
        ORDER BY id
        """
    )
    rows = cur.fetchall()
    conn.close()
    if not rows:
        _clear_collection(col)
        return 0

    _clear_collection(col)
    n = 0
    for batch in _chunked(rows, 400):
        ids = [f"car-{r[0]}" for r in batch]
        docs: list[str] = []
        metas: list[dict[str, Any]] = []
        for r in batch:
            parts = [str(x) for x in r[1:] if x is not None and str(x).strip()]
            docs.append(" ".join(parts)[:8000])
            metas.append(
                {
                    "car_id": int(r[0]),
                    "vin": (r[1] or "")[:32],
                    "year": r[3],
                    "make": (r[4] or "")[:64],
                    "model": (r[5] or "")[:64],
                }
            )
        col.upsert(ids=ids, documents=docs, metadatas=metas)
        n += len(batch)
    return n


def reindex_inventory_dealerships(client: Any) -> int:
    col = client.get_or_create_collection(
        "inventory_dealers",
        metadata={"sqlite": "inventory.db", "table": "dealerships"},
    )
    conn = inventory_get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT id, name, website_url, city, state, latitude, longitude FROM dealerships ORDER BY id"
        )
        rows = cur.fetchall()
    except sqlite3.Error:
        rows = []
    conn.close()
    _clear_collection(col)
    if not rows:
        return 0
    n = 0
    for batch in _chunked(rows, 300):
        ids = [f"dealer-{r[0]}" for r in batch]
        docs = []
        metas = []
        for r in batch:
            parts = [str(x) for x in r[1:] if x is not None and str(x).strip()]
            docs.append(" ".join(parts)[:8000])
            metas.append({"dealer_id": int(r[0]), "state": (r[5] or "")[:8]})
        col.upsert(ids=ids, documents=docs, metadatas=metas)
        n += len(batch)
    return n


def reindex_bmw_normalized(client: Any) -> int:
    col = client.get_or_create_collection(
        "bmw_normalized",
        metadata={"sqlite": str(BMW_DB_PATH), "table": "bmw_normalized_dealer"},
    )
    if not BMW_DB_PATH.is_file():
        _clear_collection(col)
        return 0
    conn = bmw_connect()
    bmw_init_schema(conn)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, dealer_name, street, city, state, zip, phone, root_website,
               normalized_root_domain, row_quality
        FROM bmw_normalized_dealer
        ORDER BY id
        """
    )
    rows = cur.fetchall()
    conn.close()
    _clear_collection(col)
    if not rows:
        return 0
    n = 0
    for batch in _chunked(rows, 300):
        ids = [f"bmw-n-{r[0]}" for r in batch]
        docs = []
        metas = []
        for r in batch:
            parts = [str(x) for x in r[1:] if x is not None and str(x).strip()]
            docs.append(" ".join(parts)[:8000])
            metas.append(
                {
                    "bmw_id": int(r[0]),
                    "row_quality": (r[9] or "")[:32],
                    "domain": (r[8] or "")[:128],
                }
            )
        col.upsert(ids=ids, documents=docs, metadatas=metas)
        n += len(batch)
    return n


def reindex_bmw_partial(client: Any) -> int:
    col = client.get_or_create_collection(
        "bmw_partial",
        metadata={"sqlite": str(BMW_DB_PATH), "table": "bmw_partial_staging"},
    )
    if not BMW_DB_PATH.is_file():
        _clear_collection(col)
        return 0
    conn = bmw_connect()
    bmw_init_schema(conn)
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT partial_group_key, dealer_name, street, city, state, zip, phone,
                   root_website, row_quality
            FROM bmw_partial_staging
            """
        )
        rows = cur.fetchall()
    except sqlite3.Error:
        rows = []
    conn.close()
    _clear_collection(col)
    if not rows:
        return 0
    n = 0
    for batch in _chunked(rows, 300):
        ids = []
        docs = []
        metas = []
        for r in batch:
            key = (r[0] or "unknown")[:500]
            key_hash = hashlib.sha256(key.encode("utf-8")).hexdigest()[:40]
            ids.append(f"bmw-p-{key_hash}")
            parts = [str(x) for x in r[1:] if x is not None and str(x).strip()]
            docs.append(" ".join(parts)[:8000])
            metas.append({"partial_group_key": key[:120], "row_quality": (r[8] or "")[:32]})
        col.upsert(ids=ids, documents=docs, metadatas=metas)
        n += len(batch)
    return n


def query_cars(query_text: str, n_results: int = 20) -> list[int]:
    """
    Embed ``query_text`` and query the ``inventory_cars`` collection.
    Returns SQLite ``cars.id`` values in similarity order (deduped).
    """
    q = (query_text or "").strip()
    if not q:
        return []
    n = max(1, min(int(n_results), 200))
    client = _client()
    col = client.get_or_create_collection(
        "inventory_cars",
        metadata={"sqlite": "inventory.db", "table": "cars"},
    )
    raw = col.query(query_texts=[q], n_results=n)
    metas = raw.get("metadatas") or []
    batch = metas[0] if metas else []
    out: list[int] = []
    seen: set[int] = set()
    for m in batch:
        if not m or not isinstance(m, dict):
            continue
        cid = m.get("car_id")
        if cid is None:
            continue
        try:
            i = int(cid)
        except (TypeError, ValueError):
            continue
        if i > 0 and i not in seen:
            seen.add(i)
            out.append(i)
    return out


def reindex_all() -> dict[str, int]:
    """Rebuild all Chroma collections from SQLite sources.

    Only inventory + BMW OEM tables are indexed. Public users and
    admin_users (dev dashboard) live in users.db and are never read
    here; keep credentials out of Chroma (bcrypt + optional SQLCipher
    via backend.db.users_sqlite).
    """
    client = _client()
    out = {
        "inventory_cars": reindex_inventory_cars(client),
        "inventory_dealers": reindex_inventory_dealerships(client),
        "bmw_normalized": reindex_bmw_normalized(client),
        "bmw_partial": reindex_bmw_partial(client),
    }
    return out
