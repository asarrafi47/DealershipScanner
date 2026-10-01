"""
Local-LLM car-photo vision extraction -- the sibling of local_brochure_vision_extract.py,
same architecture, pointed at car gallery photos instead of brochure PDFs.

Candidates are exactly the images a cheap prior pass has already flagged: run_image_text_extraction.py
(Apple Vision OCR, see backend/vision/image_text.py) classifies every gallery photo and records
sticker_image_urls in car_image_text.summary. This script reads ONLY those flagged images with a
vision model running on this machine (LM Studio) -- the same "cheap text pass first, expensive model
only where it found something" hybrid used for brochures (local_brochure_text_cache.py then
local_brochure_vision_extract.py), just inverted: here the cheap pass IS the candidate filter, not a
separate cache.

Why this exists: the cloud vision-scan campaign (Sonnet agents reading galleries via Workflow,
car_image_text.version = 100) hits the same account usage/rate limits documented for the brochure
pipeline. This lane has no such ceiling.

What it deliberately does NOT do: write into car_image_text or claim these facts describe the
specific VIN they were read from. car_image_text.version = 100 is a reviewed cloud pass and the only
writer generated_spec_sheet.py trusts as "this car's own photo says X" (see _merge_sticker_photo_findings).
A local, unreviewed read has no business overwriting that. Facts land in car_image_vision_facts with
review_verdict=NULL; only an independent review pass promotes to 'confirmed', and only confirmed rows
feed the shared package_values registry (feed_package_registry_from_car_image_vision.py), trim-level,
via the existing 'sticker_photo' source tier -- exactly the "trim catalog" badge generated_spec_sheet.py
already renders for brochure-sourced registry entries.

Usage:
    .venv/bin/python -m backend.scripts.local_car_image_vision_extract
    .venv/bin/python -m backend.scripts.local_car_image_vision_extract --limit 20
"""
from __future__ import annotations

import argparse
import base64
import json
import logging
import re
import sys
import time
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from backend.db.connect import connect as db_connect, inventory_dsn  # noqa: E402

_log = logging.getLogger("local_car_image_vision")

LM_STUDIO_URL = "http://localhost:1234/v1/chat/completions"
MODEL_ID = "qwen/qwen3.8-27b"
EXTRACTED_BY_TAG = "qwen3.8-27b-local"

IMAGE_CACHE_DIR = _REPO_ROOT / "backend" / "data" / "car_image_local_cache"

MAX_TOKENS = 16000
REQUEST_TIMEOUT_S = 900
MAX_ATTEMPTS = 3

_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0 Safari/537.36"
)

PROMPT_TEMPLATE = (
    "This is a photo from a car dealer's listing gallery for a {vehicle}, flagged by a prior OCR pass "
    "as likely showing a Monroney window sticker or a 'vehicle highlights' equipment slide. Extract every "
    "concrete, priced-or-named fact this image states: option/package names and prices, standard/optional "
    "equipment, trim name, MSRP or total price if the sticker shows one. For every fact, include quoted_text: "
    "the exact words visible in the image that support it -- if you cannot point to exact supporting text, do "
    "not report the fact at all. If this image is not actually a window sticker or equipment slide (e.g. an "
    "exterior/interior photo the OCR pass misclassified), return an empty facts array. Do not guess or infer "
    "beyond what the image literally shows. Respond with ONLY a JSON object, no other text: "
    '{{"facts": [{{"fact_type": "spec|option|package|package_feature|trim_add", "name": "...", '
    '"value_text": "...", "price": null, "trim": null, "quoted_text": "..."}}]}}'
)


def _candidate_images(cur, limit: int | None) -> list[dict[str, Any]]:
    """
    Every (car, sticker image url) pair a prior OCR pass flagged, minus:
      - cars the cloud (Sonnet) vision pass already fully covers (version = 100)
      - images this lane already wrote a fact row for, or exhausted retries on

    car_image_text.summary->'sticker_image_urls' is a JSON array; expand it in
    Python rather than a jsonb_array_elements join so a car with zero flagged
    images (the overwhelming majority) never touches the array path at all.
    """
    cur.execute(
        """
        SELECT c.id, c.vin, c.year, c.make, c.model, c.trim, t.summary->'sticker_image_urls'
        FROM car_image_text t
        JOIN cars c ON c.id = t.car_id
        WHERE t.version BETWEEN 1 AND 99
          AND t.has_sticker
          AND NOT EXISTS (
              SELECT 1 FROM car_image_text t2
              WHERE t2.car_id = t.car_id AND t2.version = 100
          )
        """
    )
    rows = cur.fetchall()

    cur.execute("SELECT DISTINCT car_id, image_url FROM car_image_vision_facts")
    fact_done = {(r[0], r[1]) for r in cur.fetchall()}
    cur.execute(
        "SELECT DISTINCT car_id, image_url FROM car_image_vision_extraction_failures WHERE attempts >= %s",
        (MAX_ATTEMPTS,),
    )
    exhausted = {(r[0], r[1]) for r in cur.fetchall()}
    done = fact_done | exhausted

    out: list[dict[str, Any]] = []
    for car_id, vin, year, make, model, trim, urls in rows:
        if not isinstance(urls, list):
            continue
        for url in urls:
            if not isinstance(url, str) or not url:
                continue
            if (car_id, url) in done:
                continue
            out.append({
                "car_id": car_id, "vin": vin, "year": year, "make": make,
                "model": model, "trim": trim, "url": url,
            })
    out.sort(key=lambda d: (d["car_id"], d["url"]))
    if limit:
        out = out[:limit]
    return out


def _cache_path(car_id: int, url: str) -> Path:
    import hashlib

    digest = hashlib.sha256(url.encode()).hexdigest()[:16]
    ext = Path(url.split("?")[0]).suffix or ".jpg"
    if len(ext) > 5:
        ext = ".jpg"
    out_dir = IMAGE_CACHE_DIR / str(car_id)
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / f"{digest}{ext}"


def _fetch_image(url: str, dest: Path) -> bool:
    import urllib.parse
    import urllib.request

    if dest.exists():
        return True
    try:
        safe_url = urllib.parse.quote(url, safe=":/?#[]@!$&'()*+,;=%~")
        req = urllib.request.Request(safe_url, headers={"User-Agent": _UA, "Accept": "image/*"})
        with urllib.request.urlopen(req, timeout=20) as resp:
            if resp.status != 200:
                return False
            blob = resp.read(12 * 1024 * 1024)
    except Exception as exc:  # noqa: BLE001
        _log.debug("fetch failed for %s: %s", url, exc)
        return False
    if not blob:
        return False
    dest.write_bytes(blob)
    return True


def _extract_image(image_path: Path, vehicle: str) -> dict[str, Any] | None:
    """Returns {"facts": [...]} on success, {"_error": ...} on exhausted retries."""
    import requests

    b64 = base64.b64encode(image_path.read_bytes()).decode()
    mime = "image/png" if image_path.suffix.lower() == ".png" else "image/jpeg"
    payload = {
        "model": MODEL_ID,
        "messages": [{
            "role": "user",
            "content": [
                {"type": "text", "text": PROMPT_TEMPLATE.format(vehicle=vehicle)},
                {"type": "image_url", "image_url": {"url": f"data:{mime};base64,{b64}"}},
            ],
        }],
        "temperature": 0.1,
        "max_tokens": MAX_TOKENS,
    }
    last_err = "unknown"
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            r = requests.post(LM_STUDIO_URL, json=payload, timeout=REQUEST_TIMEOUT_S)
        except Exception as exc:  # noqa: BLE001
            last_err = f"request error: {exc}"
            _log.warning("attempt %d/%d: %s", attempt, MAX_ATTEMPTS, last_err)
            continue
        if r.status_code != 200:
            last_err = f"HTTP {r.status_code}: {r.text[:200]}"
            _log.warning("attempt %d/%d: %s", attempt, MAX_ATTEMPTS, last_err)
            continue
        try:
            content = r.json()["choices"][0]["message"]["content"]
        except Exception as exc:  # noqa: BLE001
            last_err = f"malformed response: {exc}"
            continue
        cleaned = re.sub(r"^```(json)?", "", content.strip())
        cleaned = re.sub(r"```$", "", cleaned.strip())
        try:
            parsed = json.loads(cleaned)
            if "facts" not in parsed or not isinstance(parsed["facts"], list):
                raise ValueError("no 'facts' array")
            return parsed
        except Exception as exc:  # noqa: BLE001
            last_err = f"JSON parse failed: {exc}"
            _log.warning("attempt %d/%d: %s", attempt, MAX_ATTEMPTS, last_err)
    _log.error("giving up after %d attempts: %s", MAX_ATTEMPTS, last_err)
    return {"_error": last_err}


def _record_failure(cur, car_id: int, url: str, stage: str, reason: str) -> None:
    cur.execute(
        """
        INSERT INTO car_image_vision_extraction_failures (car_id, image_url, stage, reason)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (car_id, image_url, stage) DO UPDATE SET
            attempts = car_image_vision_extraction_failures.attempts + 1,
            last_failed_at = NOW(),
            reason = EXCLUDED.reason
        """,
        (car_id, url, stage, reason[:2000]),
    )


def _write_facts(cur, item: dict[str, Any], facts: list[dict]) -> int:
    written = 0
    for f in facts:
        name = str(f.get("name") or "").strip()[:150]
        quoted = str(f.get("quoted_text") or "").strip()
        fact_type = f.get("fact_type")
        if not name or not quoted or fact_type not in (
            "spec", "option", "package", "package_feature", "trim_add",
        ):
            continue
        try:
            price = float(f["price"]) if f.get("price") is not None else None
        except (TypeError, ValueError):
            price = None
        cur.execute(
            """
            INSERT INTO car_image_vision_facts
                (car_id, vin, year, make, model, trim, fact_type, name, value_text, price,
                 image_url, quoted_text, extracted_by, review_verdict)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NULL)
            ON CONFLICT (car_id, image_url, fact_type, name) DO NOTHING
            """,
            (item["car_id"], item["vin"], item["year"], item["make"], item["model"],
             f.get("trim") or item["trim"], fact_type, name, f.get("value_text"), price,
             item["url"], quoted, EXTRACTED_BY_TAG),
        )
        written += cur.rowcount
    return written


def main() -> int:
    global MODEL_ID

    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--limit", type=int, help="cap on number of images processed this run")
    ap.add_argument("--model", default=MODEL_ID, help="LM Studio model identifier")
    ap.add_argument(
        "--concurrency", type=int, default=1,
        help="concurrent requests to LM Studio -- keep low if another local vision job "
             "(e.g. local_brochure_vision_extract.py) shares the same model instance",
    )
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    MODEL_ID = args.model

    from concurrent.futures import ThreadPoolExecutor, as_completed

    inventory_dsn(export=True)
    conn = db_connect(autocommit=True)
    cur = conn.cursor()

    items = _candidate_images(cur, args.limit)
    _log.info("%d sticker image(s) queued for local extraction, concurrency=%d", len(items), args.concurrency)
    if not items:
        return 0

    def _fetch_and_extract(item: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any] | None, str | None]:
        dest = _cache_path(item["car_id"], item["url"])
        if not _fetch_image(item["url"], dest):
            return item, None, "fetch"
        vehicle = " ".join(str(p) for p in (item["year"], item["make"], item["model"], item["trim"]) if p)
        result = _extract_image(dest, vehicle or "unknown vehicle")
        if result is None or "_error" in result:
            return item, result, "extract"
        return item, result, None

    images_ok = images_failed = facts_written = 0
    t_start = time.time()

    with ThreadPoolExecutor(max_workers=max(1, args.concurrency)) as pool:
        futures = [pool.submit(_fetch_and_extract, item) for item in items]
        for i, fut in enumerate(as_completed(futures), 1):
            item, result, failed_stage = fut.result()
            if failed_stage == "fetch":
                _record_failure(cur, item["car_id"], item["url"], "fetch", "image download failed")
                images_failed += 1
                continue
            if failed_stage == "extract":
                reason = (result or {}).get("_error", "unknown")
                stage = "extract_parse" if "parse" in reason else "extract_request"
                _record_failure(cur, item["car_id"], item["url"], stage, reason)
                images_failed += 1
                continue
            n = _write_facts(cur, item, result["facts"])
            facts_written += n
            images_ok += 1
            if i % 10 == 0 or i == len(items):
                _log.info(
                    "%d/%d image(s) | %d ok, %d failed | %d fact(s) written | %.1fs elapsed",
                    i, len(items), images_ok, images_failed, facts_written, time.time() - t_start,
                )

    elapsed = time.time() - t_start
    _log.info(
        "done: %d image(s) ok, %d failed, %d fact(s) written, %.1f min elapsed",
        images_ok, images_failed, facts_written, elapsed / 60,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
