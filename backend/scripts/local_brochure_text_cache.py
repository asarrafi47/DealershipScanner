"""
Pre-extract PDF text layers for the local-LLM hybrid pipeline -- cheap, instant,
zero GPU/model cost, so it can run far ahead of (and fully decoupled from) the
image-based extraction job without contending for the model.

Writes one .txt per page next to where the page image would be rasterized
(backend/data/brochure_local_pages/<sha16>/page_NN.txt), plus a per-page
".suspect" marker when the text looks corrupted (the known failure mode from
OEM-sticker PDFs with broken embedded fonts -- see memory: oem_sticker binary
corruption). A suspect page should fall back to the image/vision path rather
than trust its text.

Usage:
    .venv/bin/python -m backend.scripts.local_brochure_text_cache
    .venv/bin/python -m backend.scripts.local_brochure_text_cache --limit 20
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_log = logging.getLogger("brochure_text_cache")

PAGE_CACHE_DIR = _REPO_ROOT / "backend" / "data" / "brochure_local_pages"

# Non-printable / replacement-character density above this ratio marks a page
# "suspect" -- the same signature the corpus-wide oem_sticker corruption showed
# (raw bytes like \x13\x13... where a broken embedded font was misread).
_SUSPECT_RATIO = 0.02
_PRINTABLE_RE = re.compile(r"[ -~\n\t]")


def _is_suspect(text: str) -> bool:
    if not text.strip():
        return False  # legitimately blank page, not corrupted
    printable = len(_PRINTABLE_RE.findall(text))
    ratio_bad = 1 - (printable / max(1, len(text)))
    return ratio_bad > _SUSPECT_RATIO


def _candidate_documents() -> list[dict]:
    idx_path = _REPO_ROOT / "backend" / "dictionary" / "derived" / "brochure_content_index.json"
    index = json.loads(idx_path.read_text())
    out = []
    for doc in index["documents"].values():
        path = Path(doc["path"])
        if path.exists():
            out.append({"sha256": doc["sha256"], "path": path})
    out.sort(key=lambda d: d["path"].name)
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--limit", type=int, help="cap on number of brochures processed this run")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    import fitz

    docs = _candidate_documents()
    if args.limit:
        docs = docs[: args.limit]
    _log.info("%d brochure(s) to text-cache", len(docs))

    docs_done = pages_done = pages_suspect = pages_blank = docs_failed = 0

    for i, doc in enumerate(docs, 1):
        sha, path = doc["sha256"], doc["path"]
        out_dir = PAGE_CACHE_DIR / sha[:16]
        try:
            pdf = fitz.open(path)
        except Exception as exc:  # noqa: BLE001
            _log.warning("[%d/%d] %s: could not open (%s)", i, len(docs), path.name, exc)
            docs_failed += 1
            continue

        out_dir.mkdir(parents=True, exist_ok=True)
        n = pdf.page_count
        already = len(list(out_dir.glob("page_*.txt")))
        if already == n and n > 0:
            docs_done += 1
            continue

        for p in range(n):
            txt_path = out_dir / f"page_{p + 1:02d}.txt"
            if txt_path.exists():
                continue
            text = pdf[p].get_text()
            txt_path.write_text(text, encoding="utf-8")
            if not text.strip():
                pages_blank += 1
            elif _is_suspect(text):
                txt_path.with_suffix(".suspect").touch()
                pages_suspect += 1
            pages_done += 1

        docs_done += 1
        if i % 25 == 0 or i == len(docs):
            _log.info(
                "[%d/%d] %d page(s) cached so far (%d suspect, %d blank), %d doc(s) failed to open",
                i, len(docs), pages_done, pages_suspect, pages_blank, docs_failed,
            )

    _log.info(
        "done: %d/%d brochure(s), %d page(s) written, %d suspect, %d blank, %d doc(s) failed to open",
        docs_done, len(docs), pages_done, pages_suspect, pages_blank, docs_failed,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
