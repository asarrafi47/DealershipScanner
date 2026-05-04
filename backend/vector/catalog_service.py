"""Semantic lookup against the local EPA-derived Master Spec Catalog (Postgres + pgvector)."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Human-readable label for logs (table name is ``master_spec_embeddings`` in ``pgvector_service``).
MASTER_SPEC_CATALOG_LABEL = "master_spec_catalog"
_MODEL_NAME = "all-MiniLM-L6-v2"
_KNOWN_PACKAGES_PATH = Path(__file__).resolve().parent / "known_packages.json"


def master_catalog_persist_dir() -> Path:
    from backend.vector.pgvector_service import vector_data_dir

    return vector_data_dir()


class MasterCatalog:
    """
    Ground-truth spec lookup using the same pgvector table as ``ingest_master_specs``.

    ``lookup_car`` builds a short natural-language query from partial scraper fields and
    returns the best EPA row's engine, transmission, drivetrain, and MPG (plus metadata).
    """

    def __init__(self) -> None:
        self._st_model: Any | None = None

    def _model(self):
        if self._st_model is None:
            from sentence_transformers import SentenceTransformer

            self._st_model = SentenceTransformer(_MODEL_NAME)
        return self._st_model

    def collection_exists(self) -> bool:
        from backend.vector.pgvector_service import master_spec_catalog_nonempty

        return master_spec_catalog_nonempty()

    def lookup_car(
        self,
        partial_data: dict[str, Any],
        *,
        n_results: int = 5,
    ) -> dict[str, Any]:
        """
        ``partial_data`` may include: make, model, trim, year (any subset).

        Returns a dict with ``ok``, ``query``, ``best`` (top match summary), and ``hits``
        (raw rows for debugging).
        """
        from backend.vector.pgvector_service import master_catalog_query

        parts: list[str] = []
        for key in ("year", "make", "model", "trim"):
            v = partial_data.get(key)
            if v is not None and str(v).strip():
                parts.append(str(v).strip())
        if not parts:
            return {
                "ok": False,
                "error": "empty_query",
                "message": "Provide at least one of: year, make, model, trim.",
            }

        query = (
            " ".join(parts)
            + ". Passenger vehicle specifications, engine displacement, cylinders, "
            "transmission, drivetrain, EPA fuel economy MPG."
        )

        if not self.collection_exists():
            logger.warning("Master catalog table is empty (run ingest_master_specs --reindex)")
            return {
                "ok": False,
                "error": "collection_missing",
                "message": (
                    "Master Spec Catalog is not indexed. Run: "
                    "python -m backend.vector.ingest_master_specs --reindex"
                ),
                "query": query,
            }

        emb = self._model().encode(query, show_progress_bar=False)
        if hasattr(emb, "tolist"):
            qe = emb.tolist()
        else:
            qe = list(emb)

        try:
            ids, metas, dists, docs = master_catalog_query(qe, n_results=max(1, min(n_results, 50)))
        except Exception as e:
            logger.warning("Master catalog query failed: %s", e)
            return {"ok": False, "error": "query_failed", "message": str(e), "query": query}

        hits: list[dict[str, Any]] = []
        for i, rid in enumerate(ids):
            meta = metas[i] if i < len(metas) else {}
            hits.append(
                {
                    "id": rid,
                    "distance": dists[i] if i < len(dists) else None,
                    "document": docs[i] if i < len(docs) else "",
                    "metadata": meta,
                }
            )

        if not hits:
            return {"ok": False, "error": "no_results", "query": query, "hits": []}

        top = hits[0]["metadata"] or {}

        def _f(key: str) -> Any:
            v = top.get(key)
            if v is None:
                return None
            if isinstance(v, str) and key in ("city08", "highway08", "displ", "cylinders") and v.strip():
                try:
                    if key in ("city08", "highway08", "cylinders"):
                        return int(float(v))
                    return float(v)
                except ValueError:
                    return v
            return v

        best = {
            "make": top.get("make"),
            "model": top.get("model"),
            "base_model": top.get("base_model"),
            "trim_hint": top.get("trim_hint"),
            "year": _f("year"),
            "engine_l": _f("displ"),
            "cylinders": _f("cylinders"),
            "transmission": top.get("trany"),
            "drivetrain": top.get("drive"),
            "mpg_city": _f("city08"),
            "mpg_highway": _f("highway08"),
            "fuel_type": top.get("fuel_type1"),
            "vehicle_class": top.get("vehicle_class"),
            "known_packages": top.get("known_packages"),
        }

        return {
            "ok": True,
            "query": query,
            "best": best,
            "hits": hits,
        }


def load_known_packages_config() -> dict[str, Any]:
    if not _KNOWN_PACKAGES_PATH.is_file():
        return {"by_make": {}, "by_make_model": {}}
    with open(_KNOWN_PACKAGES_PATH, encoding="utf-8") as f:
        return json.load(f)


def known_packages_for_row(make: str, base_model: str, model: str) -> str:
    """
    Return a single human-readable line listing optional OEM packages for vector text
    (colors are not in EPA data; packages are synthetic / manual).
    """
    cfg = load_known_packages_config()
    by_make = cfg.get("by_make") or {}
    by_mm = cfg.get("by_make_model") or {}
    make_key = (make or "").strip()
    base = (base_model or "").strip()
    full_model = (model or "").strip()
    out: list[str] = []
    for p in by_make.get(make_key, []) or []:
        ps = str(p).strip()
        if ps and ps not in out:
            out.append(ps)
    mm_key = f"{make_key}|{base}" if base else f"{make_key}|{full_model}"
    for p in by_mm.get(mm_key, []) or []:
        ps = str(p).strip()
        if ps and ps not in out:
            out.append(ps)
    if not out:
        return ""
    return "Manufacturer option packages (reference): " + "; ".join(out) + "."
