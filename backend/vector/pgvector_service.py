"""
Postgres + pgvector for all semantic indexes (listings, dealers, BMW OEM, car knowledge, EPA master catalog).

Requires ``PGVECTOR_URL`` or ``DATABASE_URL`` and PostgreSQL with the ``vector`` extension
(``CREATE EXTENSION vector;`` once per database).

Embeddings use ``sentence-transformers`` (default ``all-MiniLM-L6-v2``, 384 dims) unless overridden.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import sqlite3
from pathlib import Path
from typing import Any

from SCRAPING.paths import ROOT

from backend.db.inventory_db import get_conn as inventory_get_conn
from backend.utils.field_clean import is_effectively_empty
from backend.vector.listings_semantic import build_semantic_listing_document
from oem_intake.paths import BMW_DB_PATH
from oem_intake.sqlite_store import connect as bmw_connect
from oem_intake.sqlite_store import init_schema as bmw_init_schema

logger = logging.getLogger(__name__)

_DEFAULT_MODEL = "all-MiniLM-L6-v2"

T_LISTINGS = "listing_semantic_embeddings"
T_DEALERS = "dealer_semantic_embeddings"
T_BMW_NORM = "bmw_normalized_embeddings"
T_BMW_PARTIAL = "bmw_partial_embeddings"
T_CAR_KNOWLEDGE = "car_knowledge_embeddings"
T_MASTER_SPEC = "master_spec_embeddings"

_st_model: Any | None = None

_KNOWLEDGE_THRESHOLD = 0.40


def vector_data_dir() -> Path:
    """Legacy-friendly path for non-DB vector artifacts (replaces Chroma persist dir)."""
    d = ROOT / "data" / "vectors"
    d.mkdir(parents=True, exist_ok=True)
    return d


def get_persist_dir() -> Path:
    """Deprecated alias: was Chroma persist directory; use ``vector_data_dir()``."""
    return vector_data_dir()


def _model_name() -> str:
    return (os.environ.get("LISTING_EMBEDDING_MODEL") or _DEFAULT_MODEL).strip() or _DEFAULT_MODEL


def _pg_url() -> str:
    return (os.environ.get("PGVECTOR_URL") or os.environ.get("DATABASE_URL") or "").strip()


def _encode_model():
    global _st_model
    if _st_model is None:
        from sentence_transformers import SentenceTransformer

        _st_model = SentenceTransformer(_model_name())
    return _st_model


def _resolve_embedding_dim() -> int:
    forced = (os.environ.get("LISTING_EMBEDDING_DIM") or "").strip()
    if forced.isdigit():
        return max(8, int(forced))
    m = _encode_model()
    try:
        return int(m.get_sentence_embedding_dimension())
    except Exception:
        return len(m.encode("x", show_progress_bar=False).tolist())


def _embed_texts(texts: list[str]) -> list[list[float]]:
    if not texts:
        return []
    model = _encode_model()
    vecs = model.encode(
        texts,
        batch_size=min(64, max(8, len(texts))),
        show_progress_bar=False,
        convert_to_numpy=True,
    )
    return [v.tolist() for v in vecs]


def _connect():
    import psycopg

    url = _pg_url()
    if not url:
        raise RuntimeError("PGVECTOR_URL or DATABASE_URL must be set for pgvector indexes")
    conn = psycopg.connect(url)
    try:
        from pgvector.psycopg import register_vector

        register_vector(conn)
    except ImportError as e:
        conn.close()
        raise RuntimeError("pip install pgvector psycopg[binary]") from e
    return conn


def ensure_schema(conn) -> None:
    dim = _resolve_embedding_dim()
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
        for ddl in (
            f"""
            CREATE TABLE IF NOT EXISTS {T_LISTINGS} (
                car_id INTEGER PRIMARY KEY,
                document_text TEXT NOT NULL,
                embedding vector({dim}) NOT NULL,
                embedding_model TEXT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {T_DEALERS} (
                dealer_id INTEGER PRIMARY KEY,
                document_text TEXT NOT NULL,
                embedding vector({dim}) NOT NULL,
                meta JSONB,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {T_BMW_NORM} (
                bmw_id INTEGER PRIMARY KEY,
                document_text TEXT NOT NULL,
                embedding vector({dim}) NOT NULL,
                meta JSONB,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {T_BMW_PARTIAL} (
                doc_key TEXT PRIMARY KEY,
                document_text TEXT NOT NULL,
                embedding vector({dim}) NOT NULL,
                meta JSONB,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {T_CAR_KNOWLEDGE} (
                doc_id TEXT PRIMARY KEY,
                year TEXT NOT NULL,
                make TEXT NOT NULL,
                model TEXT NOT NULL,
                trim TEXT NOT NULL DEFAULT '',
                document_text TEXT NOT NULL,
                source_url TEXT NOT NULL DEFAULT '',
                embedding vector({dim}) NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {T_MASTER_SPEC} (
                spec_id TEXT PRIMARY KEY,
                document_text TEXT NOT NULL,
                embedding vector({dim}) NOT NULL,
                metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """,
        ):
            cur.execute(ddl)
        for ix in (
            f"CREATE INDEX IF NOT EXISTS ix_{T_LISTINGS}_hnsw ON {T_LISTINGS} USING hnsw (embedding vector_cosine_ops)",
            f"CREATE INDEX IF NOT EXISTS ix_{T_DEALERS}_hnsw ON {T_DEALERS} USING hnsw (embedding vector_cosine_ops)",
            f"CREATE INDEX IF NOT EXISTS ix_{T_BMW_NORM}_hnsw ON {T_BMW_NORM} USING hnsw (embedding vector_cosine_ops)",
            f"CREATE INDEX IF NOT EXISTS ix_{T_BMW_PARTIAL}_hnsw ON {T_BMW_PARTIAL} USING hnsw (embedding vector_cosine_ops)",
            f"CREATE INDEX IF NOT EXISTS ix_{T_CAR_KNOWLEDGE}_hnsw ON {T_CAR_KNOWLEDGE} USING hnsw (embedding vector_cosine_ops)",
            f"CREATE INDEX IF NOT EXISTS ix_{T_MASTER_SPEC}_hnsw ON {T_MASTER_SPEC} USING hnsw (embedding vector_cosine_ops)",
        ):
            try:
                cur.execute(ix)
            except Exception as e:
                logger.debug("index create (may exist): %s", e)
    conn.commit()


def _chunked(items: list, size: int):
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _mval(x: Any, mx: int) -> str:
    if x is None or is_effectively_empty(x):
        return ""
    return str(x)[:mx]


# ── listings (hybrid search) ────────────────────────────────────────────────


def query_cars(query_text: str, n_results: int = 100) -> list[int]:
    """Return SQLite ``cars.id`` in cosine-similarity order for free-text recall."""
    q = (query_text or "").strip()
    if not q:
        return []
    n = max(1, min(int(n_results), 300))
    conn = _connect()
    try:
        ensure_schema(conn)
        qv = _embed_texts([q])[0]
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT car_id FROM {T_LISTINGS}
                ORDER BY embedding <=> %s::vector
                LIMIT %s
                """,
                (qv, n),
            )
            rows = cur.fetchall()
        out: list[int] = []
        seen: set[int] = set()
        for (cid,) in rows:
            try:
                i = int(cid)
            except (TypeError, ValueError):
                continue
            if i > 0 and i not in seen:
                seen.add(i)
                out.append(i)
        return out
    finally:
        conn.close()


def _reindex_listings(conn) -> int:
    conn_sql = inventory_get_conn()
    conn_sql.row_factory = sqlite3.Row
    cur_sql = conn_sql.cursor()
    try:
        cur_sql.execute(
            """
            SELECT c.*, d.city AS _dealer_city, d.state AS _dealer_state
            FROM cars c
            LEFT JOIN dealerships d ON c.dealership_registry_id = d.id
            ORDER BY c.id
            """
        )
        rows = cur_sql.fetchall()
    except sqlite3.Error:
        cur_sql.execute("SELECT * FROM cars ORDER BY id")
        rows = cur_sql.fetchall()
    conn_sql.close()

    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE {T_LISTINGS}")
    conn.commit()

    if not rows:
        return 0

    model = _model_name()
    batch_docs: list[str] = []
    batch_ids: list[int] = []
    n = 0
    for row in rows:
        rd = dict(row)
        deal_city = rd.pop("_dealer_city", None)
        deal_state = rd.pop("_dealer_state", None)
        cid = int(rd["id"])
        doc = build_semantic_listing_document(
            rd,
            dealer_city=str(deal_city).strip() if deal_city else None,
            dealer_state=str(deal_state).strip() if deal_state else None,
        )
        batch_ids.append(cid)
        batch_docs.append(doc)
        if len(batch_docs) >= 128:
            vecs = _embed_texts(batch_docs)
            with conn.cursor() as cur:
                for i, d, e in zip(batch_ids, batch_docs, vecs, strict=True):
                    cur.execute(
                        f"""
                        INSERT INTO {T_LISTINGS} (car_id, document_text, embedding, embedding_model)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (car_id) DO UPDATE SET
                          document_text = EXCLUDED.document_text,
                          embedding = EXCLUDED.embedding,
                          embedding_model = EXCLUDED.embedding_model,
                          updated_at = NOW()
                        """,
                        (i, d, e, model),
                    )
            n += len(batch_ids)
            batch_ids.clear()
            batch_docs.clear()
            conn.commit()
    if batch_docs:
        vecs = _embed_texts(batch_docs)
        with conn.cursor() as cur:
            for i, d, e in zip(batch_ids, batch_docs, vecs, strict=True):
                cur.execute(
                    f"""
                    INSERT INTO {T_LISTINGS} (car_id, document_text, embedding, embedding_model)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (car_id) DO UPDATE SET
                      document_text = EXCLUDED.document_text,
                      embedding = EXCLUDED.embedding,
                      embedding_model = EXCLUDED.embedding_model,
                      updated_at = NOW()
                    """,
                    (i, d, e, model),
                )
        n += len(batch_ids)
        conn.commit()
    return n


def _reindex_dealers(conn) -> int:
    conn_sql = inventory_get_conn()
    cur_sql = conn_sql.cursor()
    try:
        cur_sql.execute(
            "SELECT id, name, website_url, city, state, latitude, longitude FROM dealerships ORDER BY id"
        )
        rows = cur_sql.fetchall()
    except sqlite3.Error:
        rows = []
    conn_sql.close()

    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE {T_DEALERS}")
    conn.commit()
    if not rows:
        return 0

    model = _model_name()
    n = 0
    for batch in _chunked(rows, 300):
        docs = []
        metas = []
        ids = []
        for r in batch:
            parts = [str(x) for x in r[1:] if x is not None and str(x).strip()]
            docs.append(" ".join(parts)[:8000])
            metas.append({"dealer_id": int(r[0]), "state": (r[5] or "")[:8]})
            ids.append(int(r[0]))
        vecs = _embed_texts(docs)
        from psycopg.types.json import Json

        with conn.cursor() as cur:
            for did, doc, emb, meta in zip(ids, docs, vecs, metas, strict=True):
                cur.execute(
                    f"""
                    INSERT INTO {T_DEALERS} (dealer_id, document_text, embedding, meta)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (dealer_id) DO UPDATE SET
                      document_text = EXCLUDED.document_text,
                      embedding = EXCLUDED.embedding,
                      meta = EXCLUDED.meta,
                      updated_at = NOW()
                    """,
                    (did, doc, emb, Json(meta)),
                )
        n += len(batch)
        conn.commit()
    return n


def _reindex_bmw_norm(conn) -> int:
    if not BMW_DB_PATH.is_file():
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE {T_BMW_NORM}")
        conn.commit()
        return 0
    bconn = bmw_connect()
    bmw_init_schema(bconn)
    cur = bconn.cursor()
    cur.execute(
        """
        SELECT id, dealer_name, street, city, state, zip, phone, root_website,
               normalized_root_domain, row_quality
        FROM bmw_normalized_dealer
        ORDER BY id
        """
    )
    rows = cur.fetchall()
    bconn.close()

    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE {T_BMW_NORM}")
    conn.commit()
    if not rows:
        return 0

    from psycopg.types.json import Json

    n = 0
    for batch in _chunked(rows, 300):
        docs, metas, ids = [], [], []
        for r in batch:
            parts = [str(x) for x in r[1:] if x is not None and str(x).strip()]
            docs.append(" ".join(parts)[:8000])
            metas.append(
                Json(
                    {
                        "bmw_id": int(r[0]),
                        "row_quality": (r[9] or "")[:32],
                        "domain": (r[8] or "")[:128],
                    }
                )
            )
            ids.append(int(r[0]))
        vecs = _embed_texts(docs)
        with conn.cursor() as cur:
            for bid, doc, emb, meta in zip(ids, docs, vecs, metas, strict=True):
                cur.execute(
                    f"""
                    INSERT INTO {T_BMW_NORM} (bmw_id, document_text, embedding, meta)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (bmw_id) DO UPDATE SET
                      document_text = EXCLUDED.document_text,
                      embedding = EXCLUDED.embedding,
                      meta = EXCLUDED.meta,
                      updated_at = NOW()
                    """,
                    (bid, doc, emb, meta),
                )
        n += len(batch)
        conn.commit()
    return n


def _reindex_bmw_partial(conn) -> int:
    if not BMW_DB_PATH.is_file():
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE {T_BMW_PARTIAL}")
        conn.commit()
        return 0
    bconn = bmw_connect()
    bmw_init_schema(bconn)
    cur = bconn.cursor()
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
    bconn.close()

    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE {T_BMW_PARTIAL}")
    conn.commit()
    if not rows:
        return 0

    from psycopg.types.json import Json

    n = 0
    for batch in _chunked(rows, 300):
        keys, docs, metas = [], [], []
        for r in batch:
            key = (r[0] or "unknown")[:500]
            key_hash = hashlib.sha256(key.encode("utf-8")).hexdigest()[:40]
            keys.append(key_hash)
            parts = [str(x) for x in r[1:] if x is not None and str(x).strip()]
            docs.append(" ".join(parts)[:8000])
            metas.append(Json({"partial_group_key": key[:120], "row_quality": (r[8] or "")[:32]}))
        vecs = _embed_texts(docs)
        with conn.cursor() as cur:
            for dk, doc, emb, meta in zip(keys, docs, vecs, metas, strict=True):
                cur.execute(
                    f"""
                    INSERT INTO {T_BMW_PARTIAL} (doc_key, document_text, embedding, meta)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (doc_key) DO UPDATE SET
                      document_text = EXCLUDED.document_text,
                      embedding = EXCLUDED.embedding,
                      meta = EXCLUDED.meta,
                      updated_at = NOW()
                    """,
                    (dk, doc, emb, meta),
                )
        n += len(batch)
        conn.commit()
    return n


def reindex_all() -> dict[str, int]:
    """Rebuild all pgvector tables from SQLite / BMW sources."""
    conn = _connect()
    try:
        ensure_schema(conn)
        return {
            "inventory_cars": _reindex_listings(conn),
            "inventory_dealers": _reindex_dealers(conn),
            "bmw_normalized": _reindex_bmw_norm(conn),
            "bmw_partial": _reindex_bmw_partial(conn),
        }
    finally:
        conn.close()


def reindex_inventory_only() -> dict[str, int]:
    """Rebuild listing + dealer embeddings only (skip BMW OEM tables)."""
    conn = _connect()
    try:
        ensure_schema(conn)
        return {
            "inventory_cars": _reindex_listings(conn),
            "inventory_dealers": _reindex_dealers(conn),
        }
    finally:
        conn.close()


# ── car knowledge cache ─────────────────────────────────────────────────────


def _knowledge_doc_id(year: Any, make: str, model: str, trim: str = "") -> str:
    parts = [str(year or ""), make or "", model or "", trim or ""]
    slug = "_".join(
        re.sub(r"[^\w]", "-", p.strip().lower()).strip("-")
        for p in parts
        if p.strip()
    )
    return f"know-{slug}"[:200]


def get_model_knowledge(year: Any, make: str, model: str) -> tuple[str, str] | tuple[None, None]:
    try:
        q = f"{year} {make} {model} engine specs reliability powertrain review"
        conn = _connect()
        try:
            ensure_schema(conn)
            qv = _embed_texts([q])[0]
            y = str(year)
            mk = (make or "").strip().lower()
            md = (model or "").strip().lower()
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT document_text, source_url, embedding <=> %s::vector AS dist
                    FROM {T_CAR_KNOWLEDGE}
                    WHERE year = %s AND lower(make) = %s AND lower(model) = %s
                    ORDER BY embedding <=> %s::vector
                    LIMIT 3
                    """,
                    (qv, y, mk, md, qv),
                )
                row = cur.fetchone()
            if not row:
                return None, None
            doc, url, dist = row[0], row[1] or "", float(row[2])
            if dist <= _KNOWLEDGE_THRESHOLD and len(doc or "") >= 80:
                return doc, url
            return None, None
        finally:
            conn.close()
    except Exception as exc:
        logger.warning("[pgvector] get_model_knowledge failed: %s", exc)
        return None, None


def add_model_knowledge(
    year: Any,
    make: str,
    model: str,
    text: str,
    source_url: str,
    trim: str = "",
) -> bool:
    try:
        if not (text or "").strip():
            return False
        doc_id = _knowledge_doc_id(year, make, model, trim)
        conn = _connect()
        try:
            ensure_schema(conn)
            emb = _embed_texts([text[:8000]])[0]
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {T_CAR_KNOWLEDGE}
                        (doc_id, year, make, model, trim, document_text, source_url, embedding)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (doc_id) DO UPDATE SET
                        document_text = EXCLUDED.document_text,
                        source_url = EXCLUDED.source_url,
                        embedding = EXCLUDED.embedding,
                        updated_at = NOW()
                    """,
                    (
                        doc_id,
                        str(year),
                        (make or "").strip().lower(),
                        (model or "").strip().lower(),
                        (trim or "").strip().lower(),
                        text[:8000],
                        source_url or "",
                        emb,
                    ),
                )
            conn.commit()
            return True
        finally:
            conn.close()
    except Exception as exc:
        logger.warning("[pgvector] add_model_knowledge failed: %s", exc)
        return False


# ── EPA master spec catalog (ingest_master_specs + MasterCatalog) ───────────


def master_spec_catalog_nonempty() -> bool:
    try:
        conn = _connect()
        try:
            with conn.cursor() as cur:
                cur.execute(f"SELECT 1 FROM {T_MASTER_SPEC} LIMIT 1")
                return cur.fetchone() is not None
        finally:
            conn.close()
    except Exception:
        return False


def master_spec_truncate() -> None:
    conn = _connect()
    try:
        ensure_schema(conn)
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE {T_MASTER_SPEC}")
        conn.commit()
    finally:
        conn.close()


def master_spec_upsert_batch(
    ids: list[str],
    documents: list[str],
    metadatas: list[dict[str, Any]],
    embeddings: list[list[float]],
) -> None:
    from psycopg.types.json import Json

    conn = _connect()
    try:
        ensure_schema(conn)
        with conn.cursor() as cur:
            for sid, doc, meta, emb in zip(ids, documents, metadatas, embeddings, strict=True):
                cur.execute(
                    f"""
                    INSERT INTO {T_MASTER_SPEC} (spec_id, document_text, embedding, metadata)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (spec_id) DO UPDATE SET
                        document_text = EXCLUDED.document_text,
                        embedding = EXCLUDED.embedding,
                        metadata = EXCLUDED.metadata,
                        updated_at = NOW()
                    """,
                    (sid, doc, emb, Json(meta)),
                )
        conn.commit()
    finally:
        conn.close()


def master_catalog_query(
    query_embedding: list[float],
    *,
    n_results: int = 5,
) -> tuple[list[str], list[dict[str, Any]], list[float], list[str]]:
    """Returns (ids, metadatas_as_dict, distances, documents)."""
    conn = _connect()
    try:
        ensure_schema(conn)
        n = max(1, min(n_results, 50))
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT spec_id, metadata, embedding <=> %s::vector, document_text
                FROM {T_MASTER_SPEC}
                ORDER BY embedding <=> %s::vector
                LIMIT %s
                """,
                (query_embedding, query_embedding, n),
            )
            rows = cur.fetchall()
        ids: list[str] = []
        metas: list[dict[str, Any]] = []
        dists: list[float] = []
        docs: list[str] = []
        for sid, meta, dist, doc in rows:
            ids.append(str(sid))
            if isinstance(meta, dict):
                metas.append(meta)
            else:
                metas.append(dict(meta) if meta else {})
            dists.append(float(dist))
            docs.append(doc or "")
        return ids, metas, dists, docs
    finally:
        conn.close()
