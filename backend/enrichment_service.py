"""
Inventory enrichment: EPA Master Catalog (Chroma) + optional Ollama vision (llama3.2-vision).

Power mode (high-RAM / M4 Max): parallel ``ThreadPoolExecutor`` workers, larger vision payloads
(1920px, JPEG 95, up to 2 images), larger Ollama ``num_ctx``. Each worker uses its own SQLite
connection via ``get_conn()`` per operation. Catalog embedding uses a process-wide lock because
sentence-transformers is not reliably thread-safe.

CLI:
  python -m backend.enrichment_service --all --limit 10 --workers 4
  python -m backend.enrichment_service --all
  python -m backend.enrichment_service --vision-only --limit 5
"""

from __future__ import annotations

import argparse
import base64
import json
import logging
import os
import re
import sqlite3
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO
from typing import Any

import requests

from backend.db.inventory_db import get_conn, get_car_by_id
from backend.vector.catalog_service import MasterCatalog

logger = logging.getLogger(__name__)

OLLAMA_VISION_MODEL = os.environ.get("OLLAMA_VISION_MODEL", "llama3.2-vision")
OLLAMA_HOST = os.environ.get("OLLAMA_HOST", "http://127.0.0.1:11434")
DEFAULT_MAX_WORKERS = int(os.environ.get("ENRICHMENT_MAX_WORKERS", "4"))

_ENRICHMENT_COLUMNS: tuple[tuple[str, str], ...] = (
    ("engine_l", "REAL"),
    ("mpg_city", "INTEGER"),
    ("mpg_highway", "INTEGER"),
    ("packages", "TEXT"),
)


def _is_missing(val: Any) -> bool:
    if val is None:
        return True
    s = str(val).strip()
    return s == "" or s == "---"


def ensure_enrichment_columns(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(cars)")
    existing = {row[1] for row in cur.fetchall()}
    for col, ctype in _ENRICHMENT_COLUMNS:
        if col not in existing:
            cur.execute(f"ALTER TABLE cars ADD COLUMN {col} {ctype}")
    conn.commit()


def _row_has_any_image(row: dict[str, Any]) -> bool:
    if row.get("image_url") and str(row["image_url"]).strip().startswith("http"):
        return True
    g = row.get("gallery")
    if isinstance(g, list):
        for u in g:
            if isinstance(u, str) and u.strip().startswith("http"):
                return True
    if isinstance(g, str) and "http" in g:
        return True
    return False


def _all_gallery_urls_ordered(row: dict[str, Any]) -> list[str]:
    """All HTTP(S) image URLs in stable order (hero first, then gallery)."""
    urls: list[str] = []
    main = row.get("image_url")
    if isinstance(main, str) and main.strip().startswith("http"):
        urls.append(main.strip())
    g = row.get("gallery")
    if isinstance(g, list):
        for u in g:
            if isinstance(u, str) and u.strip().startswith("http") and u.strip() not in urls:
                urls.append(u.strip())
    elif isinstance(g, str):
        try:
            arr = json.loads(g)
            if isinstance(arr, list):
                for u in arr:
                    if isinstance(u, str) and u.strip().startswith("http") and u.strip() not in urls:
                        urls.append(u.strip())
        except (json.JSONDecodeError, TypeError):
            pass
    return urls


def _is_sticker_url(url: str) -> bool:
    ul = url.lower()
    return any(n in ul for n in ("sticker", "monroney", "label"))


def _pick_vision_urls_up_to_two(row: dict[str, Any]) -> list[str]:
    """
    Up to two images for one Ollama call: prefer one exterior + one sticker/Monroney.
    """
    urls = _all_gallery_urls_ordered(row)
    if not urls:
        return []
    sticker = next((u for u in urls if _is_sticker_url(u)), None)
    exterior = next((u for u in urls if not _is_sticker_url(u)), None)
    if exterior is None:
        exterior = urls[0]
    out: list[str] = [exterior]
    if sticker and sticker != exterior:
        out.append(sticker)
    return out[:2]


VISION_MAX_PX = 1920
VISION_JPEG_QUALITY = 95
OLLAMA_VISION_OPTIONS: dict[str, Any] = {"num_ctx": 4096, "num_thread": 8}


def _downscale_to_jpeg_b64(raw: bytes, *, max_dim: int = VISION_MAX_PX, quality: int = VISION_JPEG_QUALITY) -> str | None:
    try:
        from PIL import Image

        im = Image.open(BytesIO(raw))
        im = im.convert("RGB")
        try:
            resample = Image.Resampling.LANCZOS
        except AttributeError:
            resample = Image.LANCZOS  # type: ignore[attr-defined]
        im.thumbnail((max_dim, max_dim), resample)
        buf = BytesIO()
        im.save(buf, format="JPEG", quality=quality, optimize=True)
        return base64.b64encode(buf.getvalue()).decode("ascii")
    except Exception as e:
        logger.debug("Image downscale failed: %s", e)
        return None


def _extract_json_object(text: str) -> str | None:
    """
    Extract a JSON object from model output (handles leading/trailing prose).
    Uses brace matching with basic double-quoted string awareness.
    """
    if not text or not text.strip():
        return None
    start = text.find("{")
    if start < 0:
        return None
    depth = 0
    in_string = False
    escape = False
    quote = ""
    for i in range(start, len(text)):
        ch = text[i]
        if in_string:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == quote:
                in_string = False
            continue
        if ch == '"':
            in_string = True
            quote = '"'
            continue
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return text[start : i + 1]
    return None


def _parse_vision_json_response(content: str) -> dict[str, Any] | None:
    """Parse vision model text into a dict; tolerant of markdown fences and chatter."""
    raw = (content or "").strip()
    if not raw:
        return None
    raw = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.IGNORECASE).strip()
    raw = re.sub(r"\s*```\s*$", "", raw).strip()

    blob = _extract_json_object(raw)
    if blob:
        try:
            out = json.loads(blob)
            return out if isinstance(out, dict) else None
        except json.JSONDecodeError:
            pass

    m = re.search(r"\{[\s\S]*\}", raw)
    if m:
        try:
            out = json.loads(m.group(0))
            return out if isinstance(out, dict) else None
        except json.JSONDecodeError:
            pass
    return None


def _needs_catalog(row: dict[str, Any]) -> bool:
    if _is_missing(row.get("transmission")):
        return True
    if _is_missing(row.get("drivetrain")):
        return True
    cyl = row.get("cylinders")
    if cyl is None or (isinstance(cyl, (int, float)) and int(cyl) <= 0):
        return True
    eng = row.get("engine_l")
    if eng is None or (isinstance(eng, (int, float)) and float(eng) <= 0):
        return True
    if row.get("mpg_city") is None or row.get("mpg_highway") is None:
        return True
    return False


def _needs_vision(row: dict[str, Any]) -> bool:
    if not _row_has_any_image(row):
        return False
    if _is_missing(row.get("exterior_color")):
        return True
    if _is_missing(row.get("packages")):
        return True
    return False


def _mechanical_complete(row: dict[str, Any]) -> bool:
    return not _needs_catalog(row)


def _sql_catalog_incomplete() -> str:
    return """
        (COALESCE(TRIM(transmission), '') = '' OR TRIM(transmission) = '---')
        OR (COALESCE(TRIM(drivetrain), '') = '' OR TRIM(drivetrain) = '---')
        OR cylinders IS NULL OR cylinders <= 0
        OR engine_l IS NULL OR engine_l <= 0
        OR mpg_city IS NULL OR mpg_highway IS NULL
    """


def fetch_enrichment_candidate_ids(
    conn: sqlite3.Connection,
    *,
    vision_only: bool,
    limit: int | None,
) -> list[int]:
    cur = conn.cursor()
    lim = "LIMIT ?" if limit is not None else ""
    if vision_only:
        sql = f"""
            SELECT id FROM cars
            WHERE
                NOT ({_sql_catalog_incomplete().strip()})
                AND (
                    exterior_color IS NULL OR TRIM(exterior_color) = '' OR TRIM(exterior_color) = '---'
                    OR packages IS NULL OR TRIM(packages) = '' OR TRIM(packages) = '---'
                )
                AND (
                    (image_url IS NOT NULL AND TRIM(image_url) LIKE 'http%')
                    OR (gallery IS NOT NULL AND gallery LIKE '%http%')
                )
            ORDER BY id
            {lim}
        """
        params: tuple[Any, ...] = (limit,) if limit is not None else ()
    else:
        sql = f"""
            SELECT id FROM cars
            WHERE
                ({_sql_catalog_incomplete().strip()})
                OR (
                    NOT ({_sql_catalog_incomplete().strip()})
                    AND (
                        exterior_color IS NULL OR TRIM(exterior_color) = '' OR TRIM(exterior_color) = '---'
                        OR packages IS NULL OR TRIM(packages) = '' OR TRIM(packages) = '---'
                    )
                    AND (
                        (image_url IS NOT NULL AND TRIM(image_url) LIKE 'http%')
                        OR (gallery IS NOT NULL AND gallery LIKE '%http%')
                    )
                )
            ORDER BY id
            {lim}
        """
        params = (limit,) if limit is not None else ()
    cur.execute(sql, params)
    return [int(r[0]) for r in cur.fetchall()]


def _log_vision_skipped(exc: BaseException, *, context: str = "") -> None:
    suffix = f" ({context})" if context else ""
    logger.warning("Vision Skipped - Memory/Format error%s: %s", suffix, exc)


def _fetch_image_b64_optimized(url: str, timeout: int = 25) -> str | None:
    try:
        r = requests.get(url, timeout=timeout, headers={"User-Agent": "DealershipScanner/1.0"})
        r.raise_for_status()
        return _downscale_to_jpeg_b64(r.content)
    except Exception as e:
        logger.debug("Image fetch failed %s: %s", url, e)
        return None


def _vision_analyze_car(row: dict[str, Any]) -> dict[str, Any] | None:
    """Up to two images (exterior + sticker) for Ollama vision."""
    urls = _pick_vision_urls_up_to_two(row)
    if not urls:
        return None
    images_b64: list[str] = []
    for u in urls:
        b64 = _fetch_image_b64_optimized(u)
        if b64:
            images_b64.append(b64)
    if not images_b64:
        return None

    prompt = (
        "Identify the exact exterior color name, and check the car for these features: "
        "sunroof/panoramic roof, wheel size/style, and any visible badges (like ST, Titanium, etc.). "
        "Return as JSON only with keys: "
        '{"exterior_color": string, "features": string[], "badges": string[], "notes": string}'
    )

    try:
        import ollama

        client = ollama.Client(host=OLLAMA_HOST)
        resp = client.chat(
            model=OLLAMA_VISION_MODEL,
            messages=[
                {
                    "role": "user",
                    "content": prompt,
                    "images": images_b64,
                }
            ],
            options=OLLAMA_VISION_OPTIONS,
        )
    except Exception as e:
        _log_vision_skipped(e)
        return None

    content = (resp.get("message") or {}).get("content") or ""
    parsed = _parse_vision_json_response(content)
    if not parsed:
        _log_vision_skipped(
            ValueError("model returned non-JSON"),
            context=(content[:200] if content else "empty"),
        )
    return parsed


def _merge_packages(existing: str | None, vision: dict[str, Any]) -> str:
    base: dict[str, Any] = {"features": [], "badges": [], "vision_notes": ""}
    if existing and str(existing).strip() and not _is_missing(existing):
        try:
            parsed = json.loads(existing)
            if isinstance(parsed, dict):
                base.update(parsed)
        except (json.JSONDecodeError, TypeError):
            base["legacy_text"] = str(existing).strip()
    if vision.get("exterior_color"):
        base["vision_exterior_color"] = vision.get("exterior_color")
    for k in ("features", "badges"):
        v = vision.get(k)
        if isinstance(v, list):
            merged = list(base.get(k) or [])
            for item in v:
                s = str(item).strip()
                if s and s not in merged:
                    merged.append(s)
            base[k] = merged
    if vision.get("notes"):
        base["vision_notes"] = str(vision.get("notes") or "").strip()
    return json.dumps(base, ensure_ascii=False)


def _worker_tag() -> str:
    return threading.current_thread().name


class InventoryEnricher:
    def __init__(self, catalog: MasterCatalog | None = None) -> None:
        self.catalog = catalog or MasterCatalog()
        self._catalog_lock = threading.Lock()
        conn = get_conn()
        try:
            ensure_enrichment_columns(conn)
        finally:
            conn.close()

    def save_enriched_data(self, vehicle_id: int, data: dict[str, Any]) -> list[str]:
        allowed = {
            "cylinders",
            "transmission",
            "drivetrain",
            "fuel_type",
            "engine_l",
            "mpg_city",
            "mpg_highway",
            "exterior_color",
            "packages",
        }
        updates = {k: v for k, v in data.items() if k in allowed and v is not None}
        if not updates:
            return []

        conn = get_conn()
        try:
            cols = ", ".join(f"{k} = ?" for k in updates)
            vals = list(updates.values()) + [vehicle_id]
            conn.execute(f"UPDATE cars SET {cols} WHERE id = ?", vals)
            conn.commit()
        finally:
            conn.close()
        return list(updates.keys())

    def apply_catalog_best(self, row: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
        out: dict[str, Any] = {}
        heal: list[str] = []
        if not _needs_catalog(row):
            return out, heal

        year = row.get("year")
        with self._catalog_lock:
            lk = self.catalog.lookup_car(
                {
                    "year": year,
                    "make": row.get("make"),
                    "model": row.get("model"),
                    "trim": row.get("trim"),
                },
                n_results=5,
            )
        if not lk.get("ok"):
            return out, heal
        best = lk.get("best") or {}

        if best.get("cylinders") is not None and (row.get("cylinders") is None or row.get("cylinders") == 0):
            out["cylinders"] = int(best["cylinders"])
            heal.append(f"{best['cylinders']} cyl")
        if best.get("engine_l") is not None and (
            row.get("engine_l") is None or float(row.get("engine_l") or 0) <= 0
        ):
            out["engine_l"] = float(best["engine_l"])
            heal.append(f"{best['engine_l']}L engine")
        if best.get("transmission") and _is_missing(row.get("transmission")):
            out["transmission"] = str(best["transmission"])[:200]
            heal.append("transmission")
        if best.get("drivetrain") and _is_missing(row.get("drivetrain")):
            out["drivetrain"] = str(best["drivetrain"])[:120]
            heal.append("drivetrain")
        if best.get("mpg_city") is not None and row.get("mpg_city") is None:
            out["mpg_city"] = int(best["mpg_city"])
            heal.append(f"{best['mpg_city']} MPG city")
        if best.get("mpg_highway") is not None and row.get("mpg_highway") is None:
            out["mpg_highway"] = int(best["mpg_highway"])
            heal.append(f"{best['mpg_highway']} MPG hwy")
        if best.get("fuel_type") and _is_missing(row.get("fuel_type")):
            out["fuel_type"] = str(best["fuel_type"])[:80]
            heal.append("fuel type")
        return out, heal

    def apply_vision(self, row: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
        if not _needs_vision(row):
            return {}, []
        try:
            vis = _vision_analyze_car(row)
        except Exception as e:
            _log_vision_skipped(e, context="apply_vision")
            return {}, []
        if not vis:
            return {}, []
        out: dict[str, Any] = {}
        heal: list[str] = []
        if vis.get("exterior_color") and _is_missing(row.get("exterior_color")):
            out["exterior_color"] = str(vis["exterior_color"])[:120]
            heal.append(f"exterior color ({out['exterior_color']})")
        pkg = _merge_packages(row.get("packages"), vis)
        if _is_missing(row.get("packages")) or vis.get("features") or vis.get("badges"):
            out["packages"] = pkg[:8000]
            heal.append("packages/features (vision)")
        return out, heal

    def enrich_one(self, car_id: int, *, vision_only: bool = False) -> dict[str, Any]:
        wt = _worker_tag()
        row = get_car_by_id(car_id)
        if not row:
            return {"ok": False, "error": "not_found", "id": car_id}

        all_heals: list[str] = []
        if vision_only and not _mechanical_complete(row):
            return {
                "ok": False,
                "error": "mechanical_incomplete",
                "id": car_id,
                "message": "Use full enrichment first; vision-only requires MPG/engine/trans/drive filled.",
            }

        if not vision_only:
            cat_updates, cat_heal = self.apply_catalog_best(row)
            if cat_updates:
                self.save_enriched_data(car_id, cat_updates)
                all_heals.extend(cat_heal)
                y = row.get("year")
                title = f"{y} {row.get('make')} {row.get('model')}".strip()
                logger.info(
                    "[%s] Healed %s (id=%s): %s via Master Catalog",
                    wt,
                    title,
                    car_id,
                    ", ".join(cat_heal) if cat_heal else "catalog",
                )
            row = get_car_by_id(car_id) or row

        if vision_only or _needs_vision(row):
            try:
                vis_updates, vis_heal = self.apply_vision(row)
                if vis_updates:
                    self.save_enriched_data(car_id, vis_updates)
                    y = row.get("year")
                    title = f"{y} {row.get('make')} {row.get('model')}".strip()
                    logger.info(
                        "[%s] Healed %s (id=%s): %s via Vision",
                        wt,
                        title,
                        car_id,
                        ", ".join(vis_heal) if vis_heal else "vision",
                    )
                    all_heals.extend(vis_heal)
            except Exception as e:
                _log_vision_skipped(e, context="enrich_one")

        return {"ok": True, "id": car_id, "healed": all_heals}

    def _enrich_one_job(self, car_id: int, vision_only: bool) -> dict[str, Any]:
        """Worker entry: one SQLite connection stack per thread (via get_conn in helpers)."""
        try:
            return self.enrich_one(car_id, vision_only=vision_only)
        except Exception:
            logger.exception("[%s] Enrichment failed for car id=%s", _worker_tag(), car_id)
            return {"ok": False, "error": "exception", "id": car_id}

    def run_all(
        self,
        *,
        limit: int | None = None,
        vision_only: bool = False,
        max_workers: int = DEFAULT_MAX_WORKERS,
    ) -> dict[str, Any]:
        conn = get_conn()
        try:
            ensure_enrichment_columns(conn)
            ids = fetch_enrichment_candidate_ids(conn, vision_only=vision_only, limit=limit)
        finally:
            conn.close()

        workers = max(1, min(int(max_workers), max(1, len(ids))))
        stats: dict[str, Any] = {
            "processed": 0,
            "ok": 0,
            "errors": 0,
            "ids": ids,
            "max_workers": workers,
            "elapsed_seconds": 0.0,
        }
        t0 = time.perf_counter()
        if not ids:
            stats["elapsed_seconds"] = round(time.perf_counter() - t0, 3)
            logger.info("Enrichment run finished (no candidates): %s", stats)
            return stats

        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="EnrichWorker") as pool:
            future_map = {
                pool.submit(self._enrich_one_job, cid, vision_only): cid for cid in ids
            }
            for fut in as_completed(future_map):
                cid = future_map[fut]
                stats["processed"] += 1
                try:
                    r = fut.result()
                    if r.get("ok"):
                        stats["ok"] += 1
                    else:
                        stats["errors"] += 1
                except Exception:
                    logger.exception("Enrichment future failed for car id=%s", cid)
                    stats["errors"] += 1

        stats["elapsed_seconds"] = round(time.perf_counter() - t0, 3)
        logger.info("Enrichment run finished in %.3fs: %s", stats["elapsed_seconds"], stats)
        return stats


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    p = argparse.ArgumentParser(description="Enrich inventory.db from EPA catalog + optional vision.")
    p.add_argument("--all", action="store_true", help="Process enrichment candidates.")
    p.add_argument(
        "--vision-only",
        action="store_true",
        help="Only run Llama 3.2-Vision on cars that already have mechanical + MPG filled.",
    )
    p.add_argument("--limit", type=int, default=None, help="Max cars to process (e.g. 5 or 10).")
    p.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_MAX_WORKERS,
        help=f"Parallel worker threads (default {DEFAULT_MAX_WORKERS}, env ENRICHMENT_MAX_WORKERS).",
    )
    args = p.parse_args(argv)

    if not args.all:
        print("Pass --all to run (optionally with --limit N or --vision-only).", file=sys.stderr)
        return 2

    enricher = InventoryEnricher()
    if not enricher.catalog.collection_exists() and not args.vision_only:
        print(
            "Master catalog collection not found. Index first:\n"
            "  python -m backend.vector.ingest_master_specs --reindex",
            file=sys.stderr,
        )
        return 1

    stats = enricher.run_all(
        limit=args.limit,
        vision_only=args.vision_only,
        max_workers=max(1, args.workers),
    )
    print(json.dumps(stats, indent=2))
    return 0 if stats["errors"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
