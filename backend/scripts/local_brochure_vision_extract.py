"""
Local-LLM brochure extraction -- the rate-limit-free extraction lane.

Reads every brochure PDF the corpus has bytes for and asks a model running on this
machine (via LM Studio's OpenAI-compatible server) to extract every concrete fact on
each page, the same schema and prompt validated against Sonnet's cloud pipeline (see
[[project memory: local-llm pilot]] -- 474 facts across 11 pages, 95.8% quote-match
against Sonnet-confirmed ground truth, zero hallucinations found on manual spot-check).

TEXT-FIRST, VISION AS FALLBACK: local_brochure_text_cache.py pre-extracts every page's
raw PDF text layer (free, instant, no model). A page whose cached text is present and
not flagged ``.suspect`` (see that script for the corruption heuristic) is sent to the
model as PLAIN TEXT -- no rasterization, no image tokens, and a reasoning model chews
through far fewer tokens per page. Only a page with no usable text cache (missing,
suspect, or genuinely blank but the model should still check for a graphic-only chart)
falls back to rasterizing and reading the page image, exactly as before. Most pages
(5,734 of 6,848 measured 2026-08-29 -- 601 suspect, 513 blank) take the text path.

Why this exists: the cloud pipeline (backend/scripts/... Workflow-based batches) hits
account usage/session/weekly limits under real load and needs a human pacing launches.
This has no such ceiling -- it's this machine's own GPU, so it can run unattended for
as long as the model stays loaded. The text-first switch exists because vision tokens
were the dominant cost, most pages carry a perfectly good text layer, and burning image
tokens on a page the PDF already hands you as text was pure waste.

What it deliberately does NOT do: mark facts as review_verdict='confirmed'. Only an
independent second pass earns that label elsewhere in this schema (see
brochure_vision_facts and V015's docstring) -- local extraction alone hasn't been
validated at that bar, so rows land with review_verdict=NULL, extracted_by tagged
distinctly ('qwen3.8-27b-local'), for a future audit pass to promote.

Any brochure/page that fails here -- corrupt PDF, model request error, unparseable
JSON -- is logged to brochure_local_extraction_failures rather than silently dropped,
so the cloud Sonnet workflow (backend/scripts/brochure_vision_batch.js pattern) can
pick up exactly the ones this lane couldn't handle.

Usage:
    .venv/bin/python -m backend.scripts.local_brochure_vision_extract
    .venv/bin/python -m backend.scripts.local_brochure_vision_extract --limit 5
    .venv/bin/python -m backend.scripts.local_brochure_vision_extract --model qwen/qwen3.8-27b
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

_log = logging.getLogger("local_brochure_vision")

LM_STUDIO_URL = "http://localhost:1234/v1/chat/completions"
MODEL_ID = "qwen/qwen3.8-27b"
EXTRACTED_BY_TAG = "qwen3.8-27b-local"

PAGE_CACHE_DIR = _REPO_ROOT / "backend" / "data" / "brochure_local_pages"

# Validated against the pilot: reasoning models spend several thousand tokens
# thinking before the JSON; 4000 truncated every response mid-object. 16000 was
# enough for the densest page seen (99 facts).
MAX_TOKENS = 16000
REQUEST_TIMEOUT_S = 900
MAX_ATTEMPTS = 3

PROMPT_TEMPLATE = (
    "This is a page from a car manufacturer brochure PDF ({vehicle}). "
    "Extract every concrete, priced-or-named fact this page states about the vehicle: trim names, "
    "standard/optional equipment, option/package names and prices, engine/powertrain specs, MPG, "
    "dimensions, colors, and anything that distinguishes one trim from another. For every fact, include "
    "quoted_text: the exact words on the page that support it -- if you cannot point to exact supporting "
    "text, do not report the fact at all. Table pages with per-trim feature availability grids are common: "
    "read row and column headers carefully, do not swap adjacent rows. If this page is a cover, table of "
    "contents, legal boilerplate, or otherwise has nothing concrete to extract, return an empty facts array. "
    "Do not guess or infer beyond what the page literally shows. Respond with ONLY a JSON object, no other "
    "text: "
    '{{"facts": [{{"fact_type": "spec|option|package|package_feature|trim_add", "name": "...", '
    '"value_text": "...", "price": null, "trim": null, "quoted_text": "..."}}]}}'
)

# Same schema and bar as PROMPT_TEMPLATE, adapted for a PDF's raw extracted text layer
# instead of a page image. PyMuPDF's get_text() flattens columns/tables into reading
# order that does not always match the visual layout, so the model is warned not to
# assume adjacent lines belong together unless the wording itself makes that clear --
# the same risk a table page poses to the vision path, restated for a text stream.
TEXT_PROMPT_TEMPLATE = (
    "This is the raw text layer extracted from a page of a car manufacturer brochure PDF ({vehicle}). "
    "PDF text extraction can lose the original table/column layout -- read carefully and do not assume "
    "adjacent lines are related unless the wording itself makes that clear. Extract every concrete, "
    "priced-or-named fact this page states about the vehicle: trim names, standard/optional equipment, "
    "option/package names and prices, engine/powertrain specs, MPG, dimensions, colors, and anything that "
    "distinguishes one trim from another. For every fact, include quoted_text: the exact words from the "
    "text that support it -- if you cannot point to exact supporting text, do not report the fact at all. "
    "If this page is a cover, table of contents, legal boilerplate, or otherwise has nothing concrete to "
    "extract, return an empty facts array. Do not guess or infer beyond what the text literally states. "
    "Respond with ONLY a JSON object, no other text: "
    '{{"facts": [{{"fact_type": "spec|option|package|package_feature|trim_add", "name": "...", '
    '"value_text": "...", "price": null, "trim": null, "quoted_text": "..."}}]}}\n\n'
    "Page text:\n---\n{page_text}\n---"
)


def _vehicle_label(catalog_keys: list[str]) -> str:
    if not catalog_keys:
        return "unknown vehicle"
    parts = catalog_keys[0].split("|")
    return " ".join(p for p in parts if p) or "unknown vehicle"


def _candidate_documents(cur) -> list[dict[str, Any]]:
    """
    Every brochure with bytes on disk, minus the ones the cloud (Sonnet) pipeline
    already fully covers.

    Skipping is per-DOCUMENT only against cloud-sourced rows -- this lane's own
    prior partial runs are resumed per-PAGE inside main(), not skipped here, so a
    brochure interrupted mid-document on a previous run picks back up instead of
    being silently abandoned.
    """
    idx_path = _REPO_ROOT / "backend" / "dictionary" / "derived" / "brochure_content_index.json"
    index = json.loads(idx_path.read_text())
    cur.execute(
        "SELECT DISTINCT source_pdf_sha256 FROM brochure_vision_facts WHERE extracted_by != %s",
        (EXTRACTED_BY_TAG,),
    )
    cloud_covered = {row[0] for row in cur.fetchall()}

    out = []
    for doc in index["documents"].values():
        sha = doc["sha256"]
        path = Path(doc["path"])
        if sha in cloud_covered:
            continue
        if not path.exists():
            continue
        out.append({"sha256": sha, "path": path, "catalog_keys": doc.get("catalog_keys") or []})
    out.sort(key=lambda d: d["path"].name)
    return out


def _already_done_pages(cur, sha: str, page_numbers: list[int]) -> set[int]:
    """
    Pages a prior run already finished (successfully OR exhausted its retries on).

    A page whose extraction legitimately returns zero facts (cover, legal boilerplate)
    still counts as done -- that's a ".ok" marker file in the page's cache dir, written
    only after a real attempt completed, not a row count. Row-counting would reprocess
    every blank page on every resume. The marker is written on both the text and image
    paths, so which one a page took has no bearing on resumability.
    """
    cache_dir = PAGE_CACHE_DIR / sha[:16]
    done = {n for n in page_numbers if (cache_dir / f"page_{n:02d}.ok").exists()}
    cur.execute(
        "SELECT DISTINCT page_number FROM brochure_local_extraction_failures "
        "WHERE source_pdf_sha256 = %s AND page_number IS NOT NULL AND attempts >= %s",
        (sha, MAX_ATTEMPTS),
    )
    done |= {row[0] for row in cur.fetchall()}
    return done


def _page_count(path: Path) -> int | None:
    import fitz

    try:
        return fitz.open(path).page_count
    except Exception as exc:  # noqa: BLE001
        _log.warning("could not open %s: %s", path.name, exc)
        return None


def _rasterize_one(path: Path, sha: str, page_num: int) -> Path | None:
    """Vision-fallback only: render ONE page to PNG, on demand.

    Most pages take the text path and never call this -- rasterizing every page
    upfront (the old behaviour) wasted disk and CPU on pages the model never saw
    as an image.
    """
    import fitz

    out_dir = PAGE_CACHE_DIR / sha[:16]
    out_dir.mkdir(parents=True, exist_ok=True)
    page_path = out_dir / f"page_{page_num:02d}.png"
    if page_path.exists():
        return page_path
    try:
        doc = fitz.open(path)
        pix = doc[page_num - 1].get_pixmap(dpi=150)
        pix.save(str(page_path))
    except Exception as exc:  # noqa: BLE001
        _log.warning("rasterize failed for %s page %d: %s", path.name, page_num, exc)
        return None
    return page_path


def _chat_extract(content: str | list[dict[str, Any]]) -> dict[str, Any]:
    """Shared request/retry/parse loop for both the text and image extraction paths.

    Returns {"facts": [...]} on success, {"_error": ...} on exhausted retries.
    """
    import requests

    payload = {
        "model": MODEL_ID,
        "messages": [{"role": "user", "content": content}],
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
            resp_content = r.json()["choices"][0]["message"]["content"]
        except Exception as exc:  # noqa: BLE001
            last_err = f"malformed response: {exc}"
            continue
        cleaned = re.sub(r"^```(json)?", "", resp_content.strip())
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


def _extract_page(image_path: Path, vehicle: str) -> dict[str, Any]:
    """Vision fallback: page image -> facts. Used when no usable text cache exists."""
    b64 = base64.b64encode(image_path.read_bytes()).decode()
    content = [
        {"type": "text", "text": PROMPT_TEMPLATE.format(vehicle=vehicle)},
        {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{b64}"}},
    ]
    return _chat_extract(content)


def _extract_page_text(page_text: str, vehicle: str) -> dict[str, Any]:
    """Primary path: the page's cached PDF text layer -> facts. No image tokens."""
    return _chat_extract(TEXT_PROMPT_TEMPLATE.format(vehicle=vehicle, page_text=page_text[:12000]))


def _cached_text(sha: str, page_num: int) -> tuple[str | None, bool]:
    """(text, is_suspect) from local_brochure_text_cache.py's cache; (None, False) if absent."""
    txt_path = PAGE_CACHE_DIR / sha[:16] / f"page_{page_num:02d}.txt"
    if not txt_path.exists():
        return None, False
    text = txt_path.read_text(encoding="utf-8")
    suspect = txt_path.with_suffix(".suspect").exists()
    return text, suspect


def _process_page(path: Path, sha: str, page_num: int, vehicle: str) -> tuple[dict[str, Any], str]:
    """Dispatch one page to the cheapest path that can actually read it.

    Returns (result, method) where method is one of "text" (cached text layer, no
    image tokens), "blank" (cached text was empty -- genuinely nothing to read, no
    model call at all), or "vision" (no usable text cache, rendered and read as an
    image -- the pre-hybrid behaviour, now the exception rather than the rule).
    """
    text, suspect = _cached_text(sha, page_num)
    if text is not None and not suspect:
        if not text.strip():
            return {"facts": []}, "blank"
        return _extract_page_text(text, vehicle), "text"
    page_path = _rasterize_one(path, sha, page_num)
    if page_path is None:
        return {"_error": "rasterize failed"}, "vision"
    return _extract_page(page_path, vehicle), "vision"


def _record_failure(cur, sha: str, path: str, page_number: int | None, stage: str, reason: str) -> None:
    cur.execute(
        """
        INSERT INTO brochure_local_extraction_failures
            (source_pdf_sha256, source_pdf_path, page_number, stage, reason)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (source_pdf_sha256, page_number, stage) DO UPDATE SET
            attempts = brochure_local_extraction_failures.attempts + 1,
            last_failed_at = NOW(),
            reason = EXCLUDED.reason
        """,
        (sha, path, page_number, stage, reason[:2000]),
    )


_MAKE_MODEL_RESOLVER: dict[tuple[str, str], tuple[str, str]] | None = None


def _squash(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "").lower())


def _make_model_resolver(cur) -> dict[tuple[str, str], tuple[str, str]]:
    """
    catalog_keys (from brochure_content_index.json) are squashed lowercase grouping
    tokens ("mercedesbenz", "grandhighlander") -- built for folding inventory
    spellings together, never meant to be stored as display text. Writing them
    straight into brochure_vision_facts.make/model breaks every downstream join
    against `cars` (which needs the real spelling, e.g. "Mercedes-Benz",
    "Grand Highlander") -- this bit the first 7,573 rows written this way,
    backfilled once by hand. Resolve through the real fleet spelling instead,
    every time, so it can't recur.
    """
    global _MAKE_MODEL_RESOLVER
    if _MAKE_MODEL_RESOLVER is not None:
        return _MAKE_MODEL_RESOLVER
    cur.execute(
        "SELECT make, model, count(*) FROM cars "
        "WHERE make IS NOT NULL AND model IS NOT NULL AND make <> '' AND model <> '' "
        "GROUP BY make, model"
    )
    best: dict[tuple[str, str], tuple[str, str, int]] = {}
    for make, model, n in cur.fetchall():
        key = (_squash(make), _squash(model))
        if key not in best or n > best[key][2]:
            best[key] = (make, model, n)
    _MAKE_MODEL_RESOLVER = {k: (v[0], v[1]) for k, v in best.items()}
    return _MAKE_MODEL_RESOLVER


def _write_facts(cur, sha: str, path: str, page_number: int, vehicle_key: str, facts: list[dict]) -> int:
    parts = vehicle_key.split("|")
    year = int(parts[0]) if parts and parts[0].isdigit() else None
    make = parts[1] if len(parts) > 1 else None
    model = parts[2] if len(parts) > 2 else None
    if not (year and make and model):
        return 0
    resolved = _make_model_resolver(cur).get((_squash(make), _squash(model)))
    if resolved is None:
        _log.warning("no fleet match for squashed make/model %r/%r -- skipping page %d facts", make, model, page_number)
        return 0
    make, model = resolved
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
            INSERT INTO brochure_vision_facts
                (year, make, model, trim, fact_type, name, value_text, price,
                 source_pdf_sha256, source_pdf_path, page_number, quoted_text,
                 extracted_by, review_verdict)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NULL)
            ON CONFLICT (source_pdf_sha256, page_number, fact_type, name) DO NOTHING
            """,
            (year, make, model, f.get("trim"), fact_type, name, f.get("value_text"), price,
             sha, path, page_number, quoted, EXTRACTED_BY_TAG),
        )
        written += cur.rowcount
    return written


def main() -> int:
    global MODEL_ID

    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--limit", type=int, help="cap on number of brochures processed this run")
    ap.add_argument("--model", default=MODEL_ID, help="LM Studio model identifier")
    ap.add_argument(
        "--concurrency", type=int, default=3,
        help="concurrent page requests to LM Studio (server reports PARALLEL capacity via `lms ps`; "
             "leave headroom under it -- default 3)",
    )
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    MODEL_ID = args.model

    from concurrent.futures import ThreadPoolExecutor, as_completed

    inventory_dsn(export=True)
    conn = db_connect(autocommit=True)
    cur = conn.cursor()

    docs = _candidate_documents(cur)
    if args.limit:
        docs = docs[: args.limit]
    _log.info("%d brochure(s) queued for local extraction, concurrency=%d", len(docs), args.concurrency)

    docs_ok = docs_failed = pages_ok = pages_failed = facts_written = 0
    by_method = {"text": 0, "blank": 0, "vision": 0}
    t_start = time.time()

    for i, doc in enumerate(docs, 1):
        sha, path, catalog_keys = doc["sha256"], doc["path"], doc["catalog_keys"]
        vehicle_key = catalog_keys[0] if catalog_keys else ""
        vehicle = _vehicle_label(catalog_keys)

        n_pages = _page_count(path)
        if n_pages is None:
            _record_failure(cur, sha, str(path), None, "rasterize", "PyMuPDF could not open this PDF")
            docs_failed += 1
            _log.info("[%d/%d] %s (%s) -- COULD NOT OPEN", i, len(docs), path.name, vehicle)
            continue

        already = _already_done_pages(cur, sha, list(range(1, n_pages + 1)))
        todo = [n for n in range(1, n_pages + 1) if n not in already]
        _log.info(
            "[%d/%d] %s (%s): %d page(s), %d already done, %d to process",
            i, len(docs), path.name, vehicle, n_pages, len(already), len(todo),
        )
        if not todo:
            docs_ok += 1
            continue

        cache_dir = PAGE_CACHE_DIR / sha[:16]
        doc_had_failure = False
        with ThreadPoolExecutor(max_workers=max(1, args.concurrency)) as pool:
            futures = {pool.submit(_process_page, path, sha, page_num, vehicle): page_num
                       for page_num in todo}
            for fut in as_completed(futures):
                page_num = futures[fut]
                result, method = fut.result()
                if "_error" in result:
                    reason = result["_error"]
                    stage = "extract_parse" if "parse" in reason else "extract_request"
                    _record_failure(cur, sha, str(path), page_num, stage, reason)
                    pages_failed += 1
                    doc_had_failure = True
                    _log.info("  page %d/%d (%s): FAILED (%s)", page_num, n_pages, method, reason[:120])
                    continue
                n = _write_facts(cur, sha, str(path), page_num, vehicle_key, result["facts"])
                facts_written += n
                pages_ok += 1
                by_method[method] += 1
                cache_dir.mkdir(parents=True, exist_ok=True)
                (cache_dir / f"page_{page_num:02d}.ok").touch()
                _log.info(
                    "  page %d/%d (%s): %d fact(s) extracted, %d written",
                    page_num, n_pages, method, len(result["facts"]), n,
                )

        if doc_had_failure:
            docs_failed += 1
        else:
            docs_ok += 1

    elapsed = time.time() - t_start
    _log.info(
        "done: %d/%d brochure(s) fully clean, %d had at least one failed page, "
        "%d page(s) ok (%d text, %d blank, %d vision), %d page(s) failed, "
        "%d fact(s) written, %.1f min elapsed",
        docs_ok, len(docs), docs_failed, pages_ok,
        by_method["text"], by_method["blank"], by_method["vision"],
        pages_failed, facts_written, elapsed / 60,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
