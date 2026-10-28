"""Durable source-PDF archive and the brochure value types.

Split out of ``brochure_extract`` (P17C.4); ``brochure_extract`` re-exports every name.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import shutil
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from backend.enrichment.dictionary_catalog import catalog_key

__all__ = [
    "BROCHURE_ARCHIVE_ENV",
    "BrochureExtractResult",
    "BrochureYMM",
    "DEFAULT_BROCHURE_ARCHIVE_DIR",
    "_ARCHIVE_GITIGNORE",
    "_FILENAME_YMM",
    "_REPO_ROOT",
    "_archive_make_dir",
    "_write_archive_meta",
    "archive_source_pdf",
    "archived_pdf_path",
    "brochure_archive_dir",
    "parse_brochure_filename",
    "sha256_file",
]

logger = logging.getLogger(__name__)


_REPO_ROOT = Path(__file__).resolve().parents[3]

# ---------------------------------------------------------------------------
# Durable source-PDF archive
# ---------------------------------------------------------------------------
#
# ``BROCHURES_DIR`` (backend/data/brochures) is a STAGING lane: the fetcher drops
# PDFs there and ``backend/scripts/delete_brochure_pdfs.py --confirm`` empties it
# once text has been extracted. That deletion is why the corpus cannot be
# re-parsed today — for 1,368 of 3,204 brochures the extractor kept only the
# pages that matched a trim-hint regex, and the PDF holding the discarded
# equipment grid is gone.
#
# The archive below is the copy that is never deleted. It is a separate tree
# from the staging lane, it is written before any delete, and re-archiving a file
# that is already there is a no-op. Point ``BROCHURE_ARCHIVE_DIR`` at durable
# storage (a mounted volume, an S3-synced directory) in production; the default
# is repo-local and carries its own ``.gitignore`` so the binaries are never
# committed.
#
#   backend/data/brochure_archive/
#     .gitignore                        # "*" — written on first archive write
#     mazda/2024_Mazda_CX-5_Brochure.pdf
#     mazda/2024_Mazda_CX-5_Brochure.pdf.meta.json
#
# The sidecar records sha256 + byte size + where the file came from, so a bullet
# quoted out of the text JSON can be tied back to the exact bytes it was read
# from. A name collision whose sha256 differs is stored beside the incumbent as
# ``<stem>__<sha12>.pdf`` — an archive write never overwrites another document.

BROCHURE_ARCHIVE_ENV = "BROCHURE_ARCHIVE_DIR"
DEFAULT_BROCHURE_ARCHIVE_DIR = _REPO_ROOT / "backend" / "data" / "brochure_archive"

_ARCHIVE_GITIGNORE = (
    "# Durable OEM brochure archive: source documents, never deleted, never committed.\n"
    "*\n"
)

# Bumped when the shape of a derived/brochure_text JSON changes.
#   (absent) v1  only pages whose text matched the trim-hint regex, text only
#   2            every page of the PDF, plus per-line cell x-coordinates

_FILENAME_YMM = re.compile(
    r"^(\d{4})_([^_]+)_(.+?)(?:_Brochure)?$",
    re.IGNORECASE,
)


@dataclass
class BrochureYMM:
    year: int
    make: str
    model: str

    @property
    def catalog_key(self) -> str:
        return catalog_key(self.year, self.make, self.model)


@dataclass
class BrochureExtractResult:
    ymm: BrochureYMM
    ladder_id: str | None = None
    trims_available: list[str] = field(default_factory=list)
    adds_by_trim: dict[str, list[str]] = field(default_factory=dict)
    standard_by_trim: dict[str, list[str]] = field(default_factory=dict)
    source_pdf: str = ""
    pages_used: list[int] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


def parse_brochure_filename(path: Path) -> BrochureYMM | None:
    m = _FILENAME_YMM.match(path.stem)
    if not m:
        return None
    return BrochureYMM(
        year=int(m.group(1)),
        make=m.group(2).replace("_", " ").strip(),
        model=m.group(3).replace("_", " ").strip(),
    )


def sha256_file(path: Path, *, chunk: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        while True:
            block = fh.read(chunk)
            if not block:
                break
            h.update(block)
    return h.hexdigest()


def brochure_archive_dir() -> Path:
    """Directory holding the source PDFs. Read from the env on every call."""
    raw = (os.environ.get(BROCHURE_ARCHIVE_ENV) or "").strip()
    return Path(raw).expanduser().resolve() if raw else DEFAULT_BROCHURE_ARCHIVE_DIR


def _archive_make_dir(ymm: BrochureYMM | None) -> str:
    make = (ymm.make if ymm else "").strip().lower()
    slug = re.sub(r"[^a-z0-9]+", "_", make).strip("_")
    return slug or "_unparsed"


def archived_pdf_path(path: Path, *, ymm: BrochureYMM | None = None) -> Path:
    """Where ``path`` belongs in the archive (does not touch the filesystem)."""
    return brochure_archive_dir() / _archive_make_dir(ymm or parse_brochure_filename(path)) / path.name


def archive_source_pdf(
    path: Path,
    *,
    ymm: BrochureYMM | None = None,
    source_url: str = "",
    dry_run: bool = False,
) -> Path | None:
    """
    Copy a brochure PDF into the durable archive and return its archived path.

    Never deletes, never overwrites: a same-named file whose sha256 differs is
    stored as ``<stem>__<sha12>.pdf`` beside it. Re-archiving identical bytes is
    a no-op that still returns the archived path, so callers can use a non-None
    return as "this document is safe on disk".
    """
    if not path.is_file():
        return None
    ymm = ymm or parse_brochure_filename(path)
    try:
        digest = sha256_file(path)
        size = path.stat().st_size
    except OSError as exc:
        logger.error("archive: cannot read %s: %s", path, exc)
        return None

    target = archived_pdf_path(path, ymm=ymm)
    if target.is_file():
        try:
            if sha256_file(target) == digest:
                if not dry_run:
                    _write_archive_meta(target, path, ymm, digest, size, source_url)
                return target
        except OSError:
            return None
        target = target.with_name(f"{target.stem}__{digest[:12]}{target.suffix}")
        if target.is_file():
            if not dry_run:
                _write_archive_meta(target, path, ymm, digest, size, source_url)
            return target

    if dry_run:
        return target

    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        gitignore = brochure_archive_dir() / ".gitignore"
        if not gitignore.is_file():
            gitignore.write_text(_ARCHIVE_GITIGNORE, encoding="utf-8")
        tmp = target.with_name(f".{target.name}.partial")
        shutil.copyfile(path, tmp)
        tmp.replace(target)
    except OSError as exc:
        logger.error("archive: failed to store %s: %s", path, exc)
        return None
    _write_archive_meta(target, path, ymm, digest, size, source_url)
    return target


def _write_archive_meta(
    target: Path,
    origin: Path,
    ymm: BrochureYMM | None,
    digest: str,
    size: int,
    source_url: str,
) -> None:
    meta_path = target.with_name(target.name + ".meta.json")
    if meta_path.is_file():
        try:
            prior = json.loads(meta_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            prior = {}
        if isinstance(prior, dict) and prior.get("sha256") == digest:
            return
    meta = {
        "sha256": digest,
        "bytes": size,
        "archived_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "archived_from": str(origin),
        "original_name": origin.name,
        "catalog_key": ymm.catalog_key if ymm else "",
        "year": ymm.year if ymm else None,
        "make": ymm.make if ymm else "",
        "model": ymm.model if ymm else "",
        "source_url": source_url,
    }
    try:
        meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    except OSError as exc:
        logger.error("archive: failed to write %s: %s", meta_path, exc)


