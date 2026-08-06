"""Local filenames, hashing, and the content-hash index: one document, one copy."""
from __future__ import annotations

import hashlib
import json
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from backend.enrichment.brochure_extract import parse_brochure_filename
from backend.enrichment.dictionary_catalog import (
    canonical_make,
    catalog_key,
)
from backend.enrichment.dictionary_paths import (
    BROCHURE_TEXT_DIR,
    DERIVED_DIR,
)

logger = logging.getLogger(__name__)

from .quarantine import (
    BROCHURE_TEXT_QUARANTINE_DIR,
)
from .tiers import (
    TIER_ARCHIVE,
    TIER_OEM,
)

# --------------------------------------------------------------------------
# Local filenames
# --------------------------------------------------------------------------


def brochure_pdf_filename(year: int, make: str, model: str) -> str | None:
    """
    Filename that ``brochure_extract.parse_brochure_filename`` maps back to
    ``catalog_key(year, make, model)``.

    Returns ``None`` when no such round-tripping name can be built, so a model
    whose name would be mis-parsed is skipped rather than filed under the wrong
    key. ``brochure_extract`` splits make from model on the first underscore and
    turns remaining underscores into spaces, so the make must not contain one.
    """
    make_part = re.sub(r"[_\s]+", " ", canonical_make(make or "").strip())
    model_part = re.sub(r"\s+", "_", (model or "").strip())
    if not make_part or not model_part:
        return None
    name = f"{year}_{make_part}_{model_part}_Brochure.pdf"

    parsed = parse_brochure_filename(Path(name))
    if parsed is None:
        return None
    if parsed.catalog_key != catalog_key(year, make, model):
        return None
    return name


def brochure_text_filename(year: int, make: str, model: str) -> str:
    """Name of the derived JSON the extraction lane will write."""
    return catalog_key(year, make, model).replace("|", "__") + ".json"


# --------------------------------------------------------------------------
# Content-hash index: one document, one copy
# --------------------------------------------------------------------------


def sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


@dataclass
class StoredDocument:
    sha256: str
    path: str
    bytes: int
    source_urls: list[str] = field(default_factory=list)
    catalog_keys: list[str] = field(default_factory=list)
    first_seen_at: str = ""
    #: :data:`TIER_OEM` / :data:`TIER_ARCHIVE`, or ``""`` for the 136 documents
    #: that were already on disk when tiers were introduced (2026-08-01) and were
    #: never recorded. Empty means "not recorded", never "assumed OEM" --
    #: back-filling it from the fetch ledger is what
    #: :meth:`ContentIndex.absorb_ledger` does where the ledger says so, and
    #: nothing invents it where it does not.
    tier: str = ""
    #: Authenticity evidence for an archive document: PDF metadata toolchain and,
    #: where we hold the OEM original, the comparison against it. Empty for OEM
    #: documents, which are their own provenance.
    authenticity: dict = field(default_factory=dict)

    def to_json(self) -> dict:
        payload = {
            "sha256": self.sha256,
            "path": self.path,
            "bytes": self.bytes,
            "source_urls": sorted(set(self.source_urls)),
            "catalog_keys": sorted(set(self.catalog_keys)),
            "first_seen_at": self.first_seen_at,
            "tier": self.tier,
        }
        if self.authenticity:
            payload["authenticity"] = self.authenticity
        return payload


class ContentIndex:
    """
    sha256 -> the single stored copy of that document.

    The first run of this fetcher stored one Mazda CX-50 spec deck twice
    (``2026_Mazda_Cx-50_Brochure.pdf`` and
    ``2026_Mazda_Mazda_CX-50_Brochure.pdf``, both sha256 ``a8723ce0...``)
    because inventory spells that model two ways. Every download now goes
    through :meth:`register`: a repeat hash reuses the copy already on disk and
    is reported as a duplicate rather than written, extracted and counted again.

    Also the resume mechanism -- :meth:`seen_url` short-circuits a URL already
    fetched, so re-running costs no download.
    """

    def __init__(self, path: Path) -> None:
        self.path = path
        self.by_sha: dict[str, StoredDocument] = {}
        self._urls: dict[str, str] = {}  # source_url -> sha256

    # -- persistence -------------------------------------------------------

    def load(self) -> "ContentIndex":
        if self.path.is_file():
            try:
                raw = json.loads(self.path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                raw = {}
            for sha, row in (raw.get("documents") or {}).items():
                self._put(
                    StoredDocument(
                        sha256=sha,
                        path=str(row.get("path") or ""),
                        bytes=int(row.get("bytes") or 0),
                        source_urls=list(row.get("source_urls") or []),
                        catalog_keys=list(row.get("catalog_keys") or []),
                        first_seen_at=str(row.get("first_seen_at") or ""),
                        tier=str(row.get("tier") or ""),
                        authenticity=dict(row.get("authenticity") or {}),
                    )
                )
        return self

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "documents": {
                sha: doc.to_json() for sha, doc in sorted(self.by_sha.items())
            }
        }
        self.path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

    def _put(self, doc: StoredDocument) -> None:
        self.by_sha[doc.sha256] = doc
        for url in doc.source_urls:
            self._urls[url] = doc.sha256

    # -- seeding -----------------------------------------------------------

    def scan_directory(self, pdf_dir: Path) -> dict[str, list[str]]:
        """
        Hash every PDF already on disk and fold it into the index.

        Returns ``{sha256: [paths]}`` for hashes stored more than once, which is
        exactly the duplicate report. Nothing is deleted here.
        """
        by_sha: dict[str, list[str]] = {}
        if not pdf_dir.is_dir():
            return {}
        for pdf in sorted(pdf_dir.glob("*.pdf")):
            sha = sha256_file(pdf)
            by_sha.setdefault(sha, []).append(str(pdf))
            existing = self.by_sha.get(sha)
            if existing is None:
                self._put(
                    StoredDocument(
                        sha256=sha, path=str(pdf), bytes=pdf.stat().st_size
                    )
                )
        return {sha: paths for sha, paths in by_sha.items() if len(paths) > 1}

    def absorb_ledger(self, ledger_path: Path) -> None:
        """Fold source URLs / catalog keys from the JSONL fetch ledger in."""
        if not ledger_path.is_file():
            return
        for line in ledger_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            sha = str(row.get("sha256") or "")
            if not sha:
                continue
            doc = self.by_sha.get(sha)
            if doc is None:
                doc = StoredDocument(
                    sha256=sha,
                    path=str(row.get("pdf_path") or ""),
                    bytes=int(row.get("bytes") or 0),
                    first_seen_at=str(row.get("fetched_at") or ""),
                )
                self.by_sha[sha] = doc
            if row.get("source_url"):
                doc.source_urls.append(str(row["source_url"]))
                self._urls[str(row["source_url"])] = sha
            if row.get("catalog_key"):
                doc.catalog_keys.append(str(row["catalog_key"]))
            if not doc.first_seen_at and row.get("fetched_at"):
                doc.first_seen_at = str(row["fetched_at"])
            # Only where the ledger line itself says so. Ledger lines written
            # before 2026-08-01 have no "tier" key and are left as "" -- an
            # unrecorded tier is reported as unrecorded, not back-dated to OEM.
            if not doc.tier and row.get("tier"):
                doc.tier = str(row["tier"])

    # -- queries -----------------------------------------------------------

    def seen_url(self, url: str) -> StoredDocument | None:
        sha = self._urls.get(url)
        return self.by_sha.get(sha) if sha else None

    def get(self, sha: str) -> StoredDocument | None:
        return self.by_sha.get(sha)

    def register(
        self,
        payload: bytes,
        *,
        preferred_path: Path,
        source_url: str,
        catalog_key_str: str,
        fetched_at: str,
        tier: str = "",
        authenticity: dict | None = None,
    ) -> tuple[StoredDocument, bool]:
        """
        Record ``payload`` once. Returns ``(document, is_new)``.

        ``is_new`` False means this exact content is already stored: the caller
        must not write a second file and must not count it as another brochure.
        The existing copy's path is returned so extraction can proceed from it.

        On a repeat hash the stored tier is **not** overwritten, only filled in
        when it was blank, and an OEM tier is never downgraded to archive. Same
        bytes reached from the archive as from the OEM means the OEM copy is the
        one we hold; that the archive also serves those exact bytes is evidence,
        and it is appended to ``authenticity`` rather than replacing the
        provenance of the copy on disk.
        """
        sha = sha256_bytes(payload)
        existing = self.by_sha.get(sha)
        if existing is not None and Path(existing.path).is_file():
            if source_url:
                existing.source_urls.append(source_url)
                self._urls[source_url] = sha
            if catalog_key_str:
                existing.catalog_keys.append(catalog_key_str)
            if tier and not existing.tier:
                existing.tier = tier
            if authenticity:
                existing.authenticity = {**existing.authenticity, **authenticity}
            if tier == TIER_ARCHIVE and existing.tier == TIER_OEM:
                existing.authenticity["archive_serves_identical_bytes"] = True
            return existing, False

        preferred_path.parent.mkdir(parents=True, exist_ok=True)
        preferred_path.write_bytes(payload)
        doc = StoredDocument(
            sha256=sha,
            path=str(preferred_path),
            bytes=len(payload),
            source_urls=[source_url] if source_url else [],
            catalog_keys=[catalog_key_str] if catalog_key_str else [],
            first_seen_at=fetched_at,
            tier=tier,
            authenticity=dict(authenticity or {}),
        )
        self._put(doc)
        return doc, True


# --------------------------------------------------------------------------
# Tier -> citation
# --------------------------------------------------------------------------

#: sha256 -> tier, resolved once per process from the content index.
_TIER_BY_SHA_CACHE: dict[str, str] | None = None


def _tier_by_sha(index_path: Path | None = None) -> dict[str, str]:
    global _TIER_BY_SHA_CACHE
    if _TIER_BY_SHA_CACHE is not None and index_path is None:
        return _TIER_BY_SHA_CACHE
    path = index_path or (DERIVED_DIR / "brochure_content_index.json")
    table: dict[str, str] = {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        raw = {}
    for sha, row in (raw.get("documents") or {}).items():
        tier = str((row or {}).get("tier") or "")
        if tier:
            table[sha] = tier
    if index_path is None:
        _TIER_BY_SHA_CACHE = table
    return table


def document_tier_for_citation(
    citation: str, *, index_path: Path | None = None
) -> str:
    """
    The source tier behind a rendered bullet's citation.

    A citation names a ``derived/brochure_text/<key>.json`` document. That file
    records the ``source_pdf_sha256`` it was extracted from, and the content
    index records the tier per sha256, so the chain

        bullet -> adds_provenance.source -> brochure_text.source_pdf_sha256
               -> content index -> tier

    is followed by re-opening the files, not by asking a register whether it once
    said something. Returns ``""`` when any link is missing -- an unknown tier is
    reported as unknown, never defaulted to ``"oem"``.
    """
    stem = Path(str(citation or "")).stem
    if not stem:
        return ""
    for directory in (BROCHURE_TEXT_DIR, BROCHURE_TEXT_QUARANTINE_DIR):
        path = directory / f"{stem}.json"
        if not path.is_file():
            continue
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return ""
        sha = str((data or {}).get("source_pdf_sha256") or "")
        return _tier_by_sha(index_path).get(sha, "") if sha else ""
    return ""
