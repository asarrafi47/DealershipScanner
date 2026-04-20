"""
Inventory enrichment — Nitro Mode (M4 Max / 48GB RAM).

**Throughput target: 1,000 cars in ≤ 15 min.**

* 16 ``ThreadPoolExecutor`` workers matching M4 Max core count (``OLLAMA_NUM_PARALLEL=16``).
* Vision images resized to ``VISION_MAX_DIM=1600px`` on the longest edge — sticker-readable
  without the 4K transfer penalty.
* ``num_ctx=4096`` / ``num_predict=4096`` — full-context generation prevents truncation.
* Only **one** image per car (sticker/Monroney preferred, else hero shot) — halves GPU load.
* SQLite writes are batched: rows accumulate in ``_write_buffer`` and flush every
  ``BATCH_COMMIT_SIZE=50`` vehicles, eliminating per-car I/O stalls.
* A ``_PrefetchCache`` (8 download threads) keeps the next ``PREFETCH_AHEAD=20`` images
  in memory while the GPU processes the current wave.
* ``keep_alive="5m"`` on every Ollama call — model never unloads between waves.
* Logging is one INFO line per vehicle; warnings/errors are kept.

CLI::

    OLLAMA_NUM_PARALLEL=16 python -m backend.enrichment_service --all --workers 16
    python -m backend.enrichment_service --all
    python -m backend.enrichment_service --vision-only --limit 50
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
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from io import BytesIO
from typing import Any

import requests

from backend.db.inventory_db import get_conn, get_car_by_id
from backend.utils.field_clean import is_effectively_empty, normalize_optional_str
from backend.vector.catalog_service import MasterCatalog

logger = logging.getLogger(__name__)

OLLAMA_VISION_MODEL = os.environ.get("OLLAMA_VISION_MODEL", "llama3.2-vision")
OLLAMA_HOST = os.environ.get("OLLAMA_HOST", "http://127.0.0.1:11434")
DEFAULT_MAX_WORKERS = int(os.environ.get("ENRICHMENT_MAX_WORKERS", "16"))
BATCH_COMMIT_SIZE = 50       # flush SQLite every N vehicles
PREFETCH_AHEAD = 20          # images to pre-download ahead of GPU
VISION_MAX_DIM = 1600        # longest-edge cap for vision images

_ENRICHMENT_COLUMNS: tuple[tuple[str, str], ...] = (
    ("engine_l", "TEXT"),
    ("mpg_city", "INTEGER"),
    ("mpg_highway", "INTEGER"),
    ("packages", "TEXT"),
)


_EV_LABELS = {"electric", "phev"}


def _is_ev_label(val: Any) -> bool:
    return isinstance(val, str) and val.strip().lower() in _EV_LABELS


def _safe_float(val: Any, default: float = 0.0) -> float:
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def _is_field_empty(val: Any) -> bool:
    if val is None:
        return True
    s = str(val).strip()
    return s in ("", "---", "0", "0.0")


def _is_missing(val: Any) -> bool:
    return is_effectively_empty(val)


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


def _pick_vision_url(row: dict[str, Any]) -> str | None:
    """Single image per car: prefer sticker/Monroney label, else first available URL."""
    urls = _all_gallery_urls_ordered(row)
    if not urls:
        return None
    sticker = next((u for u in urls if _is_sticker_url(u)), None)
    return sticker or urls[0]


VISION_JPEG_QUALITY = 100
OLLAMA_VISION_OPTIONS: dict[str, Any] = {
    "num_ctx": 4096,
    "num_thread": 8,
    "num_predict": 4096,   # match full context — never truncate package lists
    "temperature": 0.1,
}
_DB_RETRY_ATTEMPTS = 3
_DB_RETRY_BASE_DELAY = 0.5  # seconds; exponential backoff: 0.5, 1.0, 2.0


def _image_to_jpeg_b64(raw: bytes, *, quality: int = VISION_JPEG_QUALITY) -> str | None:
    """Encode to RGB JPEG, capping the longest edge at VISION_MAX_DIM for speed."""
    try:
        from PIL import Image

        im = Image.open(BytesIO(raw))
        im = im.convert("RGB")
        w, h = im.size
        if max(w, h) > VISION_MAX_DIM:
            scale = VISION_MAX_DIM / max(w, h)
            im = im.resize((int(w * scale), int(h * scale)), Image.LANCZOS)
        buf = BytesIO()
        im.save(buf, format="JPEG", quality=quality, optimize=False)
        return base64.b64encode(buf.getvalue()).decode("ascii")
    except Exception as e:
        logger.debug("Image encode failed: %s", e)
        return None


_VISION_REFUSAL_PHRASES: tuple[str, ...] = (
    "i'm happy to help",
    "i am happy to help",
    "however,",
    "does not contain text",
    "i cannot",
    "i'm unable",
    "i am unable",
    "i apologize",
    "unfortunately",
)


def _is_vision_refusal(text: str) -> bool:
    lower = text.lower()
    return any(phrase in lower for phrase in _VISION_REFUSAL_PHRASES)


def _extract_json_substring(text: str) -> str | None:
    """Find the first '{' and the last '}' and return that substring.

    This slices past any conversational preamble ('Here is the JSON:'),
    markdown fences, or trailing prose the model may add.
    If no closing '}' exists (hard truncation), returns from the first '{'
    to end-of-string so the repair layer can close brackets.
    """
    if not text:
        return None
    first = text.find("{")
    if first < 0:
        return None
    last = text.rfind("}")
    if last > first:
        return text[first : last + 1]
    # No closing brace at all — hand the tail to the repair layer
    return text[first:]


def repair_truncated_json(raw_str: str) -> dict[str, Any] | None:
    """Best-effort repair for JSON truncated mid-value (e.g. cut at 'Head-').

    Strips the trailing incomplete key/value after the last comma, closes
    any unmatched ``[`` / ``{`` brackets, and re-parses.  Returns the partial
    dict (saving the 5 packages found even when the 6th was cut off) or None.
    """
    if not raw_str or "{" not in raw_str:
        return None
    # Close any dangling quoted string
    if raw_str.count('"') % 2 != 0:
        raw_str += '"'
    # Strip trailing incomplete entry after the last comma
    candidate = re.sub(r',\s*"[^"]*"?\s*:?\s*"?[^"]*$', "", raw_str)
    # Close remaining open brackets
    open_braces = candidate.count("{") - candidate.count("}")
    open_brackets = candidate.count("[") - candidate.count("]")
    candidate += "]" * max(0, open_brackets) + "}" * max(0, open_braces)
    try:
        out = json.loads(candidate)
        return out if isinstance(out, dict) else None
    except json.JSONDecodeError:
        return None


def _parse_vision_json_response(content: str) -> dict[str, Any] | None:
    """Parse vision model text into a dict.

    Pipeline:
    1. Refuse conversational responses outright (return empty feature set).
    2. Strip markdown fences.
    3. Locate first '{' to last '}', parse that slice.
    4. If parse fails (or no closing '}' found), run ``repair_truncated_json``.
    """
    raw = (content or "").strip()
    if not raw:
        return None

    if _is_vision_refusal(raw):
        logger.warning("Vision refusal detected, returning {}. Snippet: %.50s", raw)
        return {}

    # Strip markdown code-fences
    raw = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.IGNORECASE).strip()
    raw = re.sub(r"\s*```\s*$", "", raw).strip()

    blob = _extract_json_substring(raw)
    if not blob:
        return None

    # Happy path: well-formed JSON
    try:
        out = json.loads(blob)
        if isinstance(out, dict):
            return out
    except json.JSONDecodeError:
        pass

    # Truncation repair path (e.g. id=4179 cut mid-list)
    repaired = repair_truncated_json(blob)
    if repaired is not None:
        logger.warning("Parsed vision JSON after truncation repair. Snippet: %.50s", raw)
        return repaired

    return None


def _needs_catalog(row: dict[str, Any]) -> bool:
    if _is_missing(row.get("transmission")):
        return True
    if _is_missing(row.get("drivetrain")):
        return True
    cyl = row.get("cylinders")
    if cyl is None or (isinstance(cyl, (int, float)) and int(cyl) <= 0):
        # 0 cylinders with an EV label is intentional, not missing
        if not _is_ev_label(row.get("engine_l")):
            return True
    eng = row.get("engine_l")
    if _is_ev_label(eng):
        pass  # already resolved — don't require a numeric displacement
    elif eng is None or _safe_float(eng) <= 0:
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
        OR (cylinders IS NULL OR (cylinders <= 0 AND LOWER(COALESCE(TRIM(engine_l), '')) NOT IN ('electric', 'phev')))
        OR (
            LOWER(COALESCE(TRIM(engine_l), '')) NOT IN ('electric', 'phev')
            AND (engine_l IS NULL OR CAST(engine_l AS REAL) <= 0)
        )
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


def _log_vision_skipped(exc: BaseException, *, context: str = "", raw_response: str = "") -> None:
    suffix = f" ({context})" if context else ""
    snippet = f" | raw[:50]={raw_response[:50]!r}" if raw_response else ""
    logger.warning("Vision Skipped%s: %s%s", suffix, exc, snippet)


def _fetch_image_b64_optimized(url: str, timeout: int = 25) -> str | None:
    try:
        r = requests.get(url, timeout=timeout, headers={"User-Agent": "DealershipScanner/1.0"})
        r.raise_for_status()
        return _image_to_jpeg_b64(r.content)
    except Exception as e:
        logger.debug("Image fetch failed %s: %s", url, e)
        return None


_PREFETCH_POOL_WORKERS = 8


class _PrefetchCache:
    """Background image downloader.

    ``warm(car_id, url)`` immediately submits a download future.
    ``fetch(car_id, url)`` blocks on the future (or falls back to a live download).
    Keeps the next ``PREFETCH_AHEAD`` images resident in memory while the GPU
    is busy processing the current wave of 16 workers.
    """

    def __init__(self, max_workers: int = _PREFETCH_POOL_WORKERS) -> None:
        self._pool = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="ImgPrefetch")
        self._cache: dict[int, Future[str | None]] = {}
        self._lock = threading.Lock()

    def warm(self, car_id: int, url: str) -> None:
        with self._lock:
            if car_id not in self._cache:
                self._cache[car_id] = self._pool.submit(_fetch_image_b64_optimized, url)

    def fetch(self, car_id: int, url: str) -> str | None:
        with self._lock:
            fut = self._cache.pop(car_id, None)
        if fut is not None:
            try:
                return fut.result(timeout=30.0)
            except Exception:
                return None
        return _fetch_image_b64_optimized(url)

    def shutdown(self) -> None:
        self._pool.shutdown(wait=False)


def _fetch_vision_urls_for_ids(conn: sqlite3.Connection, ids: list[int]) -> dict[int, str]:
    """One SQL round-trip: returns {car_id: best_vision_url} for prefetch warming."""
    if not ids:
        return {}
    placeholders = ",".join("?" * len(ids))
    cur = conn.cursor()
    cur.execute(f"SELECT id, image_url, gallery FROM cars WHERE id IN ({placeholders})", ids)
    result: dict[int, str] = {}
    for row_id, image_url, gallery in cur.fetchall():
        url = _pick_vision_url({"id": row_id, "image_url": image_url, "gallery": gallery})
        if url:
            result[int(row_id)] = url
    return result


def _vision_analyze_car(row: dict[str, Any], prefetch_cache: _PrefetchCache | None = None) -> dict[str, Any] | None:
    """One image per car via Nitro Mode: uses pre-fetched b64 when available."""
    url = _pick_vision_url(row)
    if not url:
        return None

    car_id = int(row.get("id") or 0)
    if prefetch_cache is not None:
        b64 = prefetch_cache.fetch(car_id, url)
    else:
        b64 = _fetch_image_b64_optimized(url)
    if not b64:
        return None

    prompt = (
        "You only report what is visibly present in this photo. Do not claim definitive factory "
        "packages or trim levels. possible_packages is for uncertain sticker/badge hints only.\n"
        "exterior_color must be null unless the paint is clearly readable; if you set exterior_color, "
        "set confidence.exterior_color to \"high\" only when certain, else null and omit exterior_color.\n"
        "Output raw JSON only with exactly these keys:\n"
        '{"observed_features": string[], "observed_badges": string[], "possible_packages": string[], '
        '"confidence": object, '
        '"exterior_color": string|null, "interior_color_hint": string|null, '
        '"vin": string|null, "msrp": number|null}\n'
        "confidence keys may include exterior_color (low|medium|high). Use null/[] when unknown. "
        "Omit vision_notes."
    )

    try:
        import ollama

        client = ollama.Client(host=OLLAMA_HOST)
        resp = client.chat(
            model=OLLAMA_VISION_MODEL,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a JSON-only response engine. "
                        "Never output text other than a valid JSON object. "
                        "If you are unsure about a field, return null. "
                        "Do not explain yourself."
                    ),
                },
                {
                    "role": "user",
                    "content": prompt,
                    "images": [b64],
                },
            ],
            options=OLLAMA_VISION_OPTIONS,
            keep_alive="5m",
        )
    except Exception as e:
        _log_vision_skipped(e)
        return None

    content = (resp.get("message") or {}).get("content") or ""
    parsed = _parse_vision_json_response(content)
    if not parsed:
        _log_vision_skipped(
            ValueError("model returned non-JSON"),
            context=f"id={car_id}",
            raw_response=content,
        )
    return parsed


def _merge_vision_observations(existing: str | None, vision: dict[str, Any]) -> str:
    """Merge vision output into packages JSON without treating vision as ground truth."""
    base: dict[str, Any] = {}
    if existing and str(existing).strip() and not _is_missing(existing):
        try:
            parsed = json.loads(existing)
            if isinstance(parsed, dict):
                base = dict(parsed)
        except (json.JSONDecodeError, TypeError):
            base = {"legacy_text": str(existing).strip()}

    for k in ("observed_features", "observed_badges", "possible_packages"):
        cur = base.get(k)
        if not isinstance(cur, list):
            # Migrate legacy keys into v2 shape
            legacy_map = {
                "observed_features": ("features", "observed_features"),
                "observed_badges": ("badges", "observed_badges"),
                "possible_packages": ("optional_packages", "possible_packages"),
            }
            legacy_a, legacy_b = legacy_map[k]
            merged: list[str] = []
            for src_key in (legacy_a, legacy_b):
                v = base.get(src_key)
                if isinstance(v, list):
                    for item in v:
                        s = str(item).strip()
                        if s and s not in merged:
                            merged.append(s)
            base[k] = merged
        else:
            base[k] = list(cur)

    if not isinstance(base.get("confidence"), dict):
        base["confidence"] = {}

    lv = base.get("legacy_vision")
    if not isinstance(lv, dict):
        lv = {}
    if vision.get("vin"):
        lv["vin_seen"] = str(vision.get("vin") or "").strip()[:32]
    if vision.get("msrp") is not None:
        try:
            lv["msrp_seen"] = float(vision["msrp"])
        except (TypeError, ValueError):
            pass
    base["legacy_vision"] = lv

    def _extend_list(key: str, items: Any) -> None:
        if not isinstance(items, list):
            return
        seen = {str(x).strip().lower() for x in base[key] if x}
        for item in items:
            s = str(item).strip()
            if s and s.lower() not in seen:
                base[key].append(s)
                seen.add(s.lower())

    _extend_list("observed_features", vision.get("observed_features") or vision.get("features"))
    _extend_list("observed_badges", vision.get("observed_badges") or vision.get("badges"))
    _extend_list("possible_packages", vision.get("possible_packages") or vision.get("optional_packages"))

    note = (vision.get("vision_notes") or vision.get("notes") or "").strip()
    if note:
        prev = (base.get("vision_notes") or "").strip()
        base["vision_notes"] = f"{prev}; {note}".strip("; ") if prev else note

    conf_in = vision.get("confidence")
    if isinstance(conf_in, dict):
        base["confidence"].update(conf_in)

    for req in ("observed_features", "observed_badges", "possible_packages"):
        if req not in base or not isinstance(base.get(req), list):
            base[req] = []
    if not isinstance(base.get("confidence"), dict):
        base["confidence"] = {}

    if vision.get("interior_color_hint"):
        h = str(vision["interior_color_hint"]).strip()[:120]
        if h:
            base["interior_color_hint_vision"] = h

    return json.dumps(base, ensure_ascii=False)


def _worker_tag() -> str:
    return threading.current_thread().name


class InventoryEnricher:
    def __init__(self, catalog: MasterCatalog | None = None) -> None:
        self.catalog = catalog or MasterCatalog()
        # Eagerly load the sentence-transformer model before workers start
        # so the lazy-init inside MasterCatalog._model() never races.
        if hasattr(self.catalog, "_model"):
            self.catalog._model()
        self._catalog_lock = threading.Lock()
        self._db_write_lock = threading.Lock()
        self._write_buffer: list[tuple[int, dict[str, Any]]] = []
        self._prefetch_cache: _PrefetchCache | None = None
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

        str_cols = ("transmission", "drivetrain", "fuel_type", "exterior_color")
        for k in str_cols:
            if k in updates:
                updates[k] = normalize_optional_str(updates[k])
                if updates[k] is None:
                    del updates[k]
        if "packages" in updates and isinstance(updates["packages"], str):
            ps = updates["packages"].strip()
            if ps:
                updates["packages"] = ps
            else:
                del updates["packages"]
        if not updates:
            return []

        # EV/PHEV identity: a vehicle with 0 cylinders is electric or plug-in hybrid —
        # store a human-readable label instead of the meaningless "0.0L" float.
        cyl = updates.get("cylinders")
        if cyl is not None and int(cyl) == 0:
            ft = str(updates.get("fuel_type") or "").lower()
            if "plug" in ft or "phev" in ft or "hybrid" in ft:
                updates["engine_l"] = "PHEV"
            else:
                updates["engine_l"] = "Electric"

        flush_batch: list[tuple[int, dict[str, Any]]] | None = None
        with self._db_write_lock:
            self._write_buffer.append((vehicle_id, updates))
            if len(self._write_buffer) >= BATCH_COMMIT_SIZE:
                flush_batch = self._write_buffer[:]
                self._write_buffer.clear()

        if flush_batch is not None:
            self._do_batch_write(flush_batch)
        return list(updates.keys())

    def _do_batch_write(self, batch: list[tuple[int, dict[str, Any]]]) -> None:
        for attempt in range(1, _DB_RETRY_ATTEMPTS + 1):
            conn = get_conn()
            try:
                for vehicle_id, updates in batch:
                    cols = ", ".join(f"{k} = ?" for k in updates)
                    vals = list(updates.values()) + [vehicle_id]
                    conn.execute(f"UPDATE cars SET {cols} WHERE id = ?", vals)
                conn.commit()
                return
            except sqlite3.OperationalError as e:
                if "locked" in str(e).lower() and attempt < _DB_RETRY_ATTEMPTS:
                    delay = _DB_RETRY_BASE_DELAY * (2 ** (attempt - 1))
                    logger.warning("DB locked on batch write (attempt %d/%d), retrying in %.1fs",
                                   attempt, _DB_RETRY_ATTEMPTS, delay)
                    time.sleep(delay)
                else:
                    raise
            finally:
                conn.close()

    def _flush_write_buffer(self) -> None:
        with self._db_write_lock:
            batch = self._write_buffer[:]
            self._write_buffer.clear()
        if batch:
            self._do_batch_write(batch)

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
            if not _is_ev_label(row.get("engine_l")):
                out["cylinders"] = int(best["cylinders"])
                heal.append(f"{best['cylinders']} cyl")
        if _is_ev_label(row.get("engine_l")):
            pass  # never overwrite a resolved EV/PHEV label with a numeric displacement
        elif best.get("engine_l") is not None and _safe_float(row.get("engine_l")) <= 0:
            out["engine_l"] = _safe_float(best["engine_l"], default=0.1)
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
            vis = _vision_analyze_car(row, self._prefetch_cache)
        except Exception as e:
            _log_vision_skipped(e, context="apply_vision")
            return {}, []
        if not vis:
            return {}, []
        out: dict[str, Any] = {}
        heal: list[str] = []
        conf = vis.get("confidence") if isinstance(vis.get("confidence"), dict) else {}
        ext_conf = str(conf.get("exterior_color", "") or "").strip().lower()
        if (
            vis.get("exterior_color")
            and _is_missing(row.get("exterior_color"))
            and ext_conf == "high"
        ):
            out["exterior_color"] = str(vis["exterior_color"])[:120]
            heal.append(f"exterior color ({out['exterior_color']})")
        pkg = _merge_vision_observations(row.get("packages"), vis)
        out["packages"] = pkg[:8000]
        heal.append("vision_observations (packages JSON)")
        return out, heal

    def enrich_one(self, car_id: int, *, vision_only: bool = False) -> dict[str, Any]:
        wt = _worker_tag()
        catalog_ms: float | None = None
        vision_ms: float | None = None
        row = get_car_by_id(car_id)
        if not row:
            return {"ok": False, "error": "not_found", "id": car_id}

        needs_cat = not vision_only and _needs_catalog(row)
        needs_vis = _needs_vision(row)

        if not needs_cat and not needs_vis:
            logger.info("[%s] id=%s skip (already complete)", wt, car_id)
            return {"ok": True, "id": car_id, "healed": []}

        all_heals: list[str] = []
        if vision_only and not _mechanical_complete(row):
            return {
                "ok": False,
                "error": "mechanical_incomplete",
                "id": car_id,
                "message": "Use full enrichment first; vision-only requires MPG/engine/trans/drive filled.",
            }

        if needs_cat:
            t0 = time.perf_counter()
            cat_updates, cat_heal = self.apply_catalog_best(row)
            catalog_ms = (time.perf_counter() - t0) * 1000.0
            if cat_updates:
                self.save_enriched_data(car_id, cat_updates)
                all_heals.extend(cat_heal)
            row = get_car_by_id(car_id) or row

        if vision_only or _needs_vision(row):
            try:
                t1 = time.perf_counter()
                vis_updates, vis_heal = self.apply_vision(row)
                vision_ms = (time.perf_counter() - t1) * 1000.0
                if vis_updates:
                    self.save_enriched_data(car_id, vis_updates)
                    all_heals.extend(vis_heal)
            except Exception as e:
                _log_vision_skipped(e, context="enrich_one")

        logger.info("[%s] id=%s healed=%s", wt, car_id, all_heals or "—")
        out: dict[str, Any] = {"ok": True, "id": car_id, "healed": all_heals}
        if catalog_ms is not None:
            out["catalog_ms"] = round(catalog_ms, 2)
        if vision_ms is not None:
            out["vision_ms"] = round(vision_ms, 2)
        return out

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
            url_map = _fetch_vision_urls_for_ids(conn, ids)
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
            "vehicles_per_minute": 0.0,
        }
        t0 = time.perf_counter()
        if not ids:
            stats["elapsed_seconds"] = round(time.perf_counter() - t0, 3)
            logger.info("Nitro Mode: no candidates found.")
            return stats

        # Spin up prefetch pool and warm the first window of images before GPU starts.
        prefetch = _PrefetchCache()
        self._prefetch_cache = prefetch
        warm_idx = 0
        for cid in ids[:PREFETCH_AHEAD]:
            url = url_map.get(cid)
            if url:
                prefetch.warm(cid, url)
        warm_idx = min(PREFETCH_AHEAD, len(ids))

        try:
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
                    # Roll the prefetch window forward as each job completes.
                    if warm_idx < len(ids):
                        next_url = url_map.get(ids[warm_idx])
                        if next_url:
                            prefetch.warm(ids[warm_idx], next_url)
                        warm_idx += 1
        finally:
            self._flush_write_buffer()
            prefetch.shutdown()
            self._prefetch_cache = None

        elapsed = time.perf_counter() - t0
        stats["elapsed_seconds"] = round(elapsed, 3)
        if elapsed > 0 and stats["processed"] > 0:
            stats["vehicles_per_minute"] = round((stats["processed"] / elapsed) * 60.0, 2)
        else:
            stats["vehicles_per_minute"] = 0.0
        logger.info(
            "Nitro Mode finished in %.1fs | VPM=%.1f | ok=%s errors=%s",
            stats["elapsed_seconds"],
            stats["vehicles_per_minute"],
            stats["ok"],
            stats["errors"],
        )
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
        help=(
            f"Parallel worker threads (default {DEFAULT_MAX_WORKERS}, env ENRICHMENT_MAX_WORKERS). "
            "Match OLLAMA_NUM_PARALLEL on the Ollama server."
        ),
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
