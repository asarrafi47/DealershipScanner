#!/usr/bin/env python3
"""
Offline data bootstrapping: download PDF brochures from auto-brochures.com,
extract spec data, and inject into backend/dictionary/{Year}_{Make}_{Model}_Complete_Options.csv.

Usage:
    python -m backend.scripts.import_brochures_to_dictionary --url https://www.auto-brochures.com/jeep.html
    python -m backend.scripts.import_brochures_to_dictionary --url https://www.auto-brochures.com/ford.html --skip-download
    python -m backend.scripts.import_brochures_to_dictionary --all --download-only
    python -m backend.scripts.import_brochures_to_dictionary --all --download-only --force
    python -m backend.scripts.import_brochures_to_dictionary --retry-failed --local-only
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
import threading
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests
from bs4 import BeautifulSoup

try:
    import pdfplumber
    _PDF_BACKEND = "pdfplumber"
except ImportError:
    try:
        from pypdf import PdfReader
        _PDF_BACKEND = "pypdf"
    except ImportError:
        _PDF_BACKEND = None

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from backend.enrichment.dictionary_paths import (  # noqa: E402
    BROCHURES_DIR,
    DICTIONARY_ROOT,
    OPTIONS_RAW_DIR,
)

_DICTIONARY = DICTIONARY_ROOT
_TMP = BROCHURES_DIR

DICT_COLUMNS = [
    "Year", "Make", "Model", "Trim",
    "engineOptions", "engineDisplay", "forcedInduction",
    "transmissionOptions", "drivetrainOptions",
    "fuelType", "bodyStyle", "cylinders", "displacement",
    "mpg_city", "mpg_highway", "mpg_combined",
    "exteriorColors", "Packages", "packageDetails", "Options", "optionDetails",
]

# ── regex patterns ─────────────────────────────────────────────────────────────

_RE_SCREEN = re.compile(
    r'(\d+\.?\d*)-inch\s+(?:touchscreen|display|Uconnect|screen|color\s+color)',
    re.IGNORECASE,
)
_RE_AUDIO_BRAND = re.compile(
    r'\b(Alpine|Harman\s+Kardon|Bang\s+&\s+Olufsen|Bose|Sony|Infinity)\b',
    re.IGNORECASE,
)
_RE_SPEAKERS = re.compile(r'(\d+)-speaker', re.IGNORECASE)
_RE_SUBWOOFER = re.compile(
    r'(\d+)-inch\s+subwoofer|enclosed\s+subwoofer|cargo\s+side-wall',
    re.IGNORECASE,
)
_RE_INTERIOR = re.compile(
    r'\b(Leather-trimmed|Nappa|Laguna|Premium\s+cloth|Capri|Suede)\b',
    re.IGNORECASE,
)

# trim availability context
_RE_STANDARD_ON = re.compile(r'[Ss]tandard\s+on\s+([\w\s,/&]+?)(?:\.|;|$)', re.IGNORECASE)
_RE_AVAILABLE_ON = re.compile(r'[Aa]vailable\s+on\s+([\w\s,/&]+?)(?:\.|;|$)', re.IGNORECASE)

# anchor text like "2020 Jeep Renegade PDF Brochure"
_RE_ANCHOR_YMM = re.compile(
    r'(\d{4})\s+([\w\s\-]+?)\s*(?:PDF\s+Brochure|Brochure|Catalog)',
    re.IGNORECASE,
)
# raw year tokens anywhere in filename (used by _extract_year_from_filename)
_RE_YEAR_TOKEN = re.compile(r'[\s_-](\d{4})(?!\d)')
# model extracted from filename: {Make}_XX {Model}_{Year} or {Make}_XX {Model} {Year}
_RE_FILENAME_MODEL = re.compile(
    r'^[^_]+_[a-z]{2,3}\s+(.+?)[\s_]\d{4}',
    re.IGNORECASE,
)
# generic hyphen-sep fallbacks
_RE_URL_YMM_YEAR_FIRST = re.compile(
    r'(\d{4})[_-]([A-Za-z]+)[_-]([A-Za-z\-]+?)(?:[_-](?:USA|CAN|EN|UK|INTL))?\.\w+$',
    re.IGNORECASE,
)
_RE_URL_YEAR_LAST = re.compile(
    r'([A-Za-z]+)[_-]([A-Za-z\-]+?)[_-](\d{4})(?:[_-](?:USA|CAN|EN|UK|INTL))?\.\w+$',
    re.IGNORECASE,
)

_SPEC_SECTION_MARKERS = re.compile(
    r'(specifications|standard equipment|options\s+and\s+packages|available\s+features)',
    re.IGNORECASE,
)

def _extract_year_from_filename(fn: str) -> str | None:
    """
    Scan right-to-left for a 4-digit vehicle year (1940-2030).
    Rightmost valid year wins — avoids model numbers like '2500' in '2500HD_2023'.
    Handles: _2014V, -2020_GT4, _1974-le mans, _1977-casa grande.
    """
    for m in reversed(list(_RE_YEAR_TOKEN.finditer(fn))):
        yr = int(m.group(1))
        if 1940 <= yr <= 2030:
            return str(yr)
    return None


_REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}

_print_lock = threading.Lock()


def _log(msg: str) -> None:
    with _print_lock:
        print(msg)


class _RateLimiter:
    """Global rate limiter: enforces minimum interval between any two requests,
    regardless of how many worker threads are running."""

    def __init__(self, min_interval: float = 2.0) -> None:
        self._lock = threading.Lock()
        self._last: float = 0.0
        self._min = min_interval

    def acquire(self) -> None:
        with self._lock:
            now = time.monotonic()
            wait = self._min - (now - self._last)
            if wait > 0:
                time.sleep(wait)
            self._last = time.monotonic()


_rate_limiter = _RateLimiter(min_interval=1.0)


# ── Part 1: Scraper ────────────────────────────────────────────────────────────

def _parse_ymm_from_ab_url(href: str) -> tuple[str, str, str] | None:
    """
    Parse (year, make, model) from any auto-brochures.com URL by splitting path
    segments. Handles 3-segment, 4-segment, and deeper (5+) paths.

    Examples:
      /makes/ram/Ram_US 1500_2023.pdf          → (2023, ram, 1500)
      /makes/Jeep/Cherokee/Jeep_US Cherokee_2015.pdf → (2015, Jeep, Cherokee)
      /makes/Porsche/911/997/Porsche_US 911_2007.pdf → (2007, Porsche, 911)
      /makes/Smart/Smart_US Fortwo_2016.pdf    → (2016, Smart, Fortwo)
    """
    # must be an auto-brochures.com URL containing /makes/
    if "/makes/" not in href:
        return None

    filename = href.rstrip("/").rsplit("/", 1)[-1]

    year = _extract_year_from_filename(filename)
    if not year:
        return None

    # split path to get segments after /makes/
    try:
        after_makes = href.split("/makes/", 1)[1]
    except IndexError:
        return None
    segments = [s for s in after_makes.split("/") if s]
    # segments: [make, (optional model dirs...), filename]
    if len(segments) < 2:
        return None

    make = segments[0].replace("_", " ").strip()

    if len(segments) == 2:
        # /makes/{make}/{filename} — no model subdir, extract from filename
        mm = _RE_FILENAME_MODEL.match(filename)
        if mm:
            model = mm.group(1).strip()
        else:
            # No model between make-prefix and year (e.g. "Mini_US 2023.pdf").
            # Only accept as valid when make itself IS the product line — i.e. the
            # make slug has only one core model line (Mini, Smart, Genesis, Tesla…).
            # Reject multi-model makes (Chevrolet, Ford, etc.) where year-only
            # brochures span 20+ distinct models.
            _SINGLE_LINE_MAKES = {
                "mini", "smart", "genesis", "tesla", "scion", "saturn",
                "daewoo", "eagle", "hummer", "spyker", "ssc", "studebaker",
            }
            if make.lower() not in _SINGLE_LINE_MAKES:
                return None
            model = make
    else:
        # /makes/{make}/{model-dir}/.../{filename}
        # use the first subdir (segments[1]) as model — it's the canonical name
        model = segments[1].replace("_", " ").strip()

    if not make or not model:
        return None

    # reject multi-model / non-vehicle-specific slugs
    _JUNK = re.compile(
        r'Full.?Line|Full.?Range|Model.?Range|Part.?Range'
        r'|Black.?Label|People.?Movers?|E-Performance|Black.?Edition'
        r'|X.?Line|V-Series|TDi|Passione|Fuoriserie|Trofeo'
        r'|Hybrid(?!\s*\w)|SUV.?Line|Van.?Line|Truck.?Line|Car.?Line'
        r'|\d+.Years?|\d+_?Anniversary|Timeline|History',
        re.IGNORECASE,
    )
    if _JUNK.search(model):
        return None

    return year, make, model


def _parse_ymm_from_anchor(text: str, href: str) -> tuple[str, str, str] | None:
    """Extract (year, make, model) from anchor text, falling back to URL."""
    # try anchor text first
    m = _RE_ANCHOR_YMM.search(text)
    if m:
        year = m.group(1)
        raw = m.group(2).strip()
        parts = raw.split(None, 1)
        if len(parts) == 2:
            return year, parts[0], parts[1].replace(" ", "_")
        if len(parts) == 1:
            return year, parts[0], parts[0]

    # auto-brochures.com path: split-based parser handles 3/4/5-segment paths
    result = _parse_ymm_from_ab_url(href)
    if result:
        year, make, model = result
        return year, make, model.replace(" ", "_")

    # generic hyphen/underscore filename fallbacks
    filename = href.rstrip("/").rsplit("/", 1)[-1]
    m2 = _RE_URL_YMM_YEAR_FIRST.match(filename)
    if m2:
        return m2.group(1), m2.group(2), m2.group(3).replace("-", "_")
    m3 = _RE_URL_YEAR_LAST.match(filename)
    if m3:
        return m3.group(3), m3.group(1), m3.group(2).replace("-", "_")

    return None


def _is_valid_pdf(path: Path) -> bool:
    """True when path exists and begins with a PDF magic header."""
    try:
        with path.open("rb") as f:
            return f.read(4) == b"%PDF"
    except OSError:
        return False


def _collect_pdf_links(page_url: str) -> list[tuple[list[str], str, str, str]]:
    """Fetch manufacturer page and return list of (hrefs, year, make, model) tuples."""
    _log(f"[scraper] fetching {page_url}")
    try:
        resp = requests.get(page_url, headers=_REQUEST_HEADERS, timeout=20)
        resp.raise_for_status()
    except Exception as exc:
        _log(f"[scraper] ERROR fetching page: {exc}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    links: list[tuple[list[str], str, str, str]] = []
    hrefs_by_out: dict[str, list[str]] = {}
    meta_by_out: dict[str, tuple[str, str, str]] = {}

    for a in soup.find_all("a", href=True):
        href: str = a["href"]
        if not href.lower().endswith(".pdf"):
            continue
        if "autocatalogarchive.com" not in href and "auto-brochures.com" not in href:
            if not href.startswith("http"):
                href = "https://www.autocatalogarchive.com" + href
            else:
                continue

        text = a.get_text(" ", strip=True)
        parsed = _parse_ymm_from_anchor(text, href)
        if not parsed:
            # suppress noise for known multi-model / full-line brochures
            _FULL_LINE = re.compile(
                r'Full.?Line|Full.?Range|Model.?Range|V-Series|TDi|X.?Line'
                r'|RL.TL|ZDX|LeMans|Passione|partrange'
                r'|\d{4}$',  # bare year-only slug
                re.IGNORECASE,
            )
            if not _FULL_LINE.search(href):
                _log(f"[scraper] skip (can't parse YMM): {href}")
            continue

        year, make, model = parsed
        out_name = f"{year}_{make}_{model}_Brochure.pdf"
        meta_by_out.setdefault(out_name, (year, make, model))
        bucket = hrefs_by_out.setdefault(out_name, [])
        if href not in bucket:
            bucket.append(href)

    for out_name, hrefs in hrefs_by_out.items():
        year, make, model = meta_by_out[out_name]
        links.append((hrefs, year, make, model))

    _log(f"[scraper] found {len(links)} brochure link(s)")
    return links


def _fetch_pdf_bytes(href: str) -> bytes | None:
    """Download href and return content when it looks like a real PDF."""
    _rate_limiter.acquire()
    try:
        pdf_resp = requests.get(href, headers=_REQUEST_HEADERS, timeout=60)
        pdf_resp.raise_for_status()
        content = pdf_resp.content
        if content[:4] == b"%PDF" and len(content) > 1024:
            return content
        _log(f"[scraper] not a PDF ({len(content)} bytes): {href}")
    except Exception as exc:
        _log(f"[scraper] ERROR downloading {href}: {exc}")
    return None


def _download_one(hrefs: list[str], year: str, make: str, model: str, *, force: bool = False) -> Path | None:
    """Download a brochure PDF, trying alternate hrefs. Returns saved path or None."""
    out_name = f"{year}_{make}_{model}_Brochure.pdf"
    out_path = _TMP / out_name

    if out_path.exists() and _is_valid_pdf(out_path) and not force:
        _log(f"[scraper] skip (exists): {out_name}")
        return out_path

    if out_path.exists() and not _is_valid_pdf(out_path):
        _log(f"[scraper] replacing invalid file: {out_name}")
        out_path.unlink(missing_ok=True)

    _log(f"[scraper] downloading {out_name} …")
    for href in hrefs:
        content = _fetch_pdf_bytes(href)
        if content is None:
            continue
        out_path.write_bytes(content)
        _log(f"[scraper] saved {out_path.stat().st_size // 1024} KB → {out_name}")
        return out_path

    _log(f"[scraper] all {len(hrefs)} link(s) failed for {out_name}")
    return None


_KNOWN_MAKE_SLUGS: list[str] = [
    "acura", "alfa-romeo", "amc", "aston-martin", "audi", "bentley", "bmw",
    "buick", "cadillac", "chevrolet", "chrysler", "daewoo", "dodge", "eagle",
    "ferrari", "fiat", "ford", "genesis", "gmc", "honda", "hummer", "hyundai",
    "infiniti", "isuzu", "jaguar", "jeep", "kia", "lamborghini", "land-rover",
    "lexus", "lincoln", "lotus", "maserati", "mazda", "mercury", "mini",
    "mitsubishi", "nissan", "oldsmobile", "plymouth", "pontiac", "porsche",
    "ram", "rolls-royce", "saab", "saturn", "scion", "smart", "subaru",
    "suzuki", "tesla", "toyota", "volkswagen", "volvo",
]

_BASE_URL = "https://www.auto-brochures.com"


def _probe_make_url(slug: str) -> str | None:
    """Return the make page URL if it exists (HTTP 200), else None."""
    url = f"{_BASE_URL}/{slug}.html"
    try:
        r = requests.head(url, headers=_REQUEST_HEADERS, timeout=10, allow_redirects=True)
        if r.status_code == 200:
            return url
        _log(f"[scraper] {slug}: HTTP {r.status_code}, skip")
    except Exception as exc:
        _log(f"[scraper] {slug}: {exc}, skip")
    return None


def discover_make_urls(workers: int = 8) -> list[str]:
    """Probe all known make slugs in parallel and return live page URLs."""
    _log(f"[scraper] probing {len(_KNOWN_MAKE_SLUGS)} make slugs …")
    urls: list[str] = []
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="probe") as pool:
        futures = {pool.submit(_probe_make_url, slug): slug for slug in _KNOWN_MAKE_SLUGS}
        for fut in as_completed(futures):
            result = fut.result()
            if result:
                urls.append(result)
    urls.sort()
    _log(f"[scraper] {len(urls)} live make page(s) found")
    return urls


def scrape_and_download(page_url: str, workers: int = 4, *, force: bool = False) -> list[Path]:
    """Discover + download PDF brochures in parallel. Returns saved paths."""
    _TMP.mkdir(parents=True, exist_ok=True)

    links = _collect_pdf_links(page_url)
    if not links:
        return []

    saved: list[Path] = []
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="dl") as pool:
        futures = {
            pool.submit(_download_one, hrefs, yr, mk, mo, force=force): (yr, mk, mo)
            for hrefs, yr, mk, mo in links
        }
        for fut in as_completed(futures):
            try:
                result = fut.result()
                if result:
                    saved.append(result)
            except Exception as exc:
                ymm = futures[fut]
                _log(f"[scraper] unhandled error {ymm}: {exc}")

    return saved


def _merge_link(hrefs_by_out: dict[str, list[str]], meta_by_out: dict[str, tuple[str, str, str]],
                hrefs: list[str], year: str, make: str, model: str) -> None:
    out_name = f"{year}_{make}_{model}_Brochure.pdf"
    meta_by_out.setdefault(out_name, (year, make, model))
    bucket = hrefs_by_out.setdefault(out_name, [])
    for href in hrefs:
        if href not in bucket:
            bucket.append(href)


def scrape_all_makes(workers: int = 4, *, force: bool = False) -> list[Path]:
    """Discover every make on auto-brochures.com and download all brochures."""
    make_urls = discover_make_urls(workers=workers)
    if not make_urls:
        return []

    hrefs_by_out: dict[str, list[str]] = {}
    meta_by_out: dict[str, tuple[str, str, str]] = {}
    seen_lock = threading.Lock()

    def _collect_make(make_url: str) -> list[tuple[list[str], str, str, str]]:
        links = _collect_pdf_links(make_url)
        with seen_lock:
            for hrefs, year, make, model in links:
                _merge_link(hrefs_by_out, meta_by_out, hrefs, year, make, model)
        return links

    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="collect") as pool:
        futures = {pool.submit(_collect_make, url): url for url in make_urls}
        for fut in as_completed(futures):
            try:
                fut.result()
            except Exception as exc:
                _log(f"[scraper] error collecting {futures[fut]}: {exc}")

    all_links = [
        (hrefs_by_out[out_name], *meta_by_out[out_name])
        for out_name in sorted(hrefs_by_out)
    ]
    _log(f"[scraper] total brochures to download: {len(all_links)}")
    _TMP.mkdir(parents=True, exist_ok=True)

    all_saved: list[Path] = []
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="dl") as pool:
        futures = {
            pool.submit(_download_one, hrefs, yr, mk, mo, force=force): (yr, mk, mo)
            for hrefs, yr, mk, mo in all_links
        }
        for fut in as_completed(futures):
            try:
                result = fut.result()
                if result:
                    all_saved.append(result)
            except Exception as exc:
                ymm = futures[fut]
                _log(f"[scraper] unhandled error {ymm}: {exc}")

    return all_saved


def list_invalid_downloads() -> list[Path]:
    """Return tmp_brochures PDF paths that are missing or not real PDFs."""
    invalid: list[Path] = []
    for path in sorted(_TMP.glob("*.pdf")):
        if not _is_valid_pdf(path):
            invalid.append(path)
    return invalid


def retry_failed_downloads(workers: int = 4) -> tuple[list[Path], list[str]]:
    """Delete invalid downloads and retry from live make pages."""
    invalid = list_invalid_downloads()
    if not invalid:
        _log("[scraper] no invalid downloads found")
        return [], []

    _log(f"[scraper] retrying {len(invalid)} invalid download(s)")
    for path in invalid:
        path.unlink(missing_ok=True)

    make_urls = discover_make_urls(workers=workers)
    hrefs_by_out: dict[str, list[str]] = {}
    meta_by_out: dict[str, tuple[str, str, str]] = {}
    wanted = {p.name for p in invalid}

    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="collect") as pool:
        futures = {pool.submit(_collect_pdf_links, url): url for url in make_urls}
        for fut in as_completed(futures):
            try:
                for hrefs, year, make, model in fut.result():
                    out_name = f"{year}_{make}_{model}_Brochure.pdf"
                    if out_name in wanted:
                        _merge_link(hrefs_by_out, meta_by_out, hrefs, year, make, model)
            except Exception as exc:
                _log(f"[scraper] error collecting {futures[fut]}: {exc}")

    retry_links = [
        (hrefs_by_out[out_name], *meta_by_out[out_name])
        for out_name in sorted(wanted)
        if out_name in hrefs_by_out
    ]
    no_links = sorted(wanted - set(hrefs_by_out))
    failed_after_retry: list[str] = []

    saved: list[Path] = []
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="dl") as pool:
        futures = {
            pool.submit(_download_one, hrefs, yr, mk, mo, force=True): out_name
            for hrefs, yr, mk, mo in retry_links
            for out_name in [f"{yr}_{mk}_{mo}_Brochure.pdf"]
        }
        for fut in as_completed(futures):
            out_name = futures[fut]
            try:
                result = fut.result()
                if result:
                    saved.append(result)
                else:
                    failed_after_retry.append(out_name)
            except Exception as exc:
                _log(f"[scraper] unhandled error {out_name}: {exc}")
                failed_after_retry.append(out_name)

    return saved, sorted(set(no_links) | set(failed_after_retry))


# ── Part 2: PDF Extraction ─────────────────────────────────────────────────────

def _extract_spec_text_pdfplumber(path: Path) -> str:
    import pdfplumber  # noqa: PLC0415
    texts: list[str] = []
    try:
        with pdfplumber.open(str(path)) as pdf:
            total = len(pdf.pages)
            start = max(0, total - 4)
            for page in pdf.pages[start:]:
                try:
                    t = page.extract_text() or ""
                    texts.append(t)
                except Exception:
                    pass
    except Exception as exc:
        print(f"[pdf] ERROR reading {path.name}: {exc}")
    return "\n".join(texts)


def _extract_spec_text_pypdf(path: Path) -> str:
    from pypdf import PdfReader  # noqa: PLC0415
    texts: list[str] = []
    try:
        reader = PdfReader(str(path))
        total = len(reader.pages)
        start = max(0, total - 4)
        for page in reader.pages[start:]:
            try:
                t = page.extract_text() or ""
                texts.append(t)
            except Exception:
                pass
    except Exception as exc:
        print(f"[pdf] ERROR reading {path.name}: {exc}")
    return "\n".join(texts)


def extract_spec_text(path: Path) -> str:
    if _PDF_BACKEND == "pdfplumber":
        return _extract_spec_text_pdfplumber(path)
    if _PDF_BACKEND == "pypdf":
        return _extract_spec_text_pypdf(path)
    print("[pdf] no PDF library installed (pip install pdfplumber)")
    return ""


def _find_trim_context(line: str, surrounding_lines: list[str]) -> list[str]:
    """Return trim names mentioned in standard/available-on context near a line."""
    trims: list[str] = []
    for l in surrounding_lines:
        m = _RE_STANDARD_ON.search(l)
        if m:
            trims.extend(t.strip() for t in re.split(r'[,/&]', m.group(1)) if t.strip())
        m2 = _RE_AVAILABLE_ON.search(l)
        if m2:
            trims.extend(t.strip() for t in re.split(r'[,/&]', m2.group(1)) if t.strip())
    return trims


def parse_brochure(path: Path) -> dict:
    """
    Returns dict:
      {
        'year': str, 'make': str, 'model': str,
        'specs': {
          'screens': [...], 'audio_brands': [...], 'speakers': [...],
          'subwoofers': [...], 'interior': [...],
        },
        'trim_specs': { trim_name: {same keys} }
      }
    """
    stem = path.stem  # e.g. 2020_Jeep_Renegade_Brochure
    m = re.match(r'^(\d{4})_([^_]+)_(.+?)(?:_Brochure)?$', stem)
    if not m:
        print(f"[pdf] can't parse filename: {path.name}")
        return {}

    year, make, model = m.group(1), m.group(2), m.group(3).replace("_", " ")

    raw_text = extract_spec_text(path)
    if not raw_text.strip():
        print(f"[pdf] no text extracted from {path.name}")
        return {"year": year, "make": make, "model": model, "specs": {}, "trim_specs": {}}

    lines = raw_text.splitlines()

    # find where spec section starts
    spec_start = 0
    for i, line in enumerate(lines):
        if _SPEC_SECTION_MARKERS.search(line):
            spec_start = i
            break

    spec_lines = lines[spec_start:]

    # global spec accumulator
    all_specs: dict[str, list[str]] = {
        "screens": [], "audio_brands": [], "speakers": [],
        "subwoofers": [], "interior": [],
    }
    trim_specs: dict[str, dict[str, list[str]]] = {}

    window = 5  # lines of surrounding context to check for trim alignment

    for i, line in enumerate(spec_lines):
        ctx = spec_lines[max(0, i - window): i + window + 1]
        trims = _find_trim_context(line, ctx)

        findings: dict[str, list[str]] = {}

        for m2 in _RE_SCREEN.finditer(line):
            findings.setdefault("screens", []).append(f"{m2.group(1)}-inch touchscreen")
        for m2 in _RE_AUDIO_BRAND.finditer(line):
            findings.setdefault("audio_brands", []).append(m2.group(1))
        for m2 in _RE_SPEAKERS.finditer(line):
            findings.setdefault("speakers", []).append(f"{m2.group(1)}-speaker")
        for m2 in _RE_SUBWOOFER.finditer(line):
            findings.setdefault("subwoofers", []).append(m2.group(0).strip())
        for m2 in _RE_INTERIOR.finditer(line):
            findings.setdefault("interior", []).append(m2.group(1))

        for key, vals in findings.items():
            for v in vals:
                if v not in all_specs[key]:
                    all_specs[key].append(v)
            for trim in trims:
                trim_bucket = trim_specs.setdefault(trim, {
                    "screens": [], "audio_brands": [], "speakers": [],
                    "subwoofers": [], "interior": [],
                })
                for v in vals:
                    if v not in trim_bucket[key]:
                        trim_bucket[key].append(v)

    return {
        "year": year, "make": make, "model": model,
        "specs": all_specs, "trim_specs": trim_specs,
    }


def _build_option_string(specs: dict[str, list[str]]) -> str:
    """Flatten spec dict to a semicolon-separated option detail string."""
    parts: list[str] = []
    if specs.get("screens"):
        parts.append("; ".join(specs["screens"]))
    if specs.get("audio_brands"):
        parts.append("; ".join(specs["audio_brands"]) + " audio")
    if specs.get("speakers"):
        parts.append("; ".join(specs["speakers"]) + " system")
    if specs.get("subwoofers"):
        parts.append("; ".join(specs["subwoofers"]))
    if specs.get("interior"):
        parts.append("; ".join(specs["interior"]) + " seating")
    return " | ".join(parts)


# ── Part 3: CSV injection ──────────────────────────────────────────────────────

def _csv_path(year: str, make: str, model: str) -> Path:
    model_slug = model.replace(" ", "_")
    make_dir = make.replace(" ", "_") or "Unknown"
    out_dir = OPTIONS_RAW_DIR / make_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    legacy = _DICTIONARY / f"{year}_{make}_{model_slug}_Complete_Options.csv"
    if legacy.is_file() and not (out_dir / legacy.name).exists():
        return legacy
    return out_dir / f"{year}_{make}_{model_slug}_Complete_Options.csv"


def _load_csv(path: Path) -> tuple[list[str], list[dict]]:
    """Return (fieldnames, rows). Creates empty structure if file missing."""
    if not path.exists():
        return DICT_COLUMNS[:], []
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or DICT_COLUMNS[:]
        rows = list(reader)
    return list(fieldnames), rows


def _save_csv(path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def inject_brochure_data(result: dict) -> None:
    """Merge parsed brochure specs into the matching Complete_Options CSV."""
    if not result:
        return

    year = result["year"]
    make = result["make"]
    model = result["model"]
    global_specs = result.get("specs", {})
    trim_specs = result.get("trim_specs", {})

    csv_path = _csv_path(year, make, model)
    fieldnames, rows = _load_csv(csv_path)

    # ensure columns exist
    for col in DICT_COLUMNS:
        if col not in fieldnames:
            fieldnames.append(col)

    global_option_str = _build_option_string(global_specs)
    global_detail = f"[Brochure] {global_option_str}" if global_option_str else ""

    matched_any_trim = False

    for row in rows:
        trim_name = (row.get("Trim") or "").strip()
        if not trim_name:
            continue

        # find best matching trim-specific spec
        best_trim_key = None
        for tk in trim_specs:
            if tk.lower() in trim_name.lower() or trim_name.lower() in tk.lower():
                best_trim_key = tk
                break

        option_str = ""
        if best_trim_key:
            option_str = _build_option_string(trim_specs[best_trim_key])
        if not option_str and global_detail:
            option_str = global_option_str

        if not option_str:
            continue

        detail_tag = f"[Brochure] {option_str}"

        existing_options = row.get("Options") or ""
        existing_details = row.get("optionDetails") or ""

        # avoid duplicate injection
        if "[Brochure]" in existing_options:
            continue

        row["Options"] = (existing_options + "; " + option_str).lstrip("; ")
        row["optionDetails"] = (existing_details + " | " + detail_tag).lstrip(" | ")
        matched_any_trim = True

    # if no existing trims matched, append a summary row — but only once
    summary_already_present = any(
        (r.get("Trim") or "").strip() == "[Brochure Summary]" for r in rows
    )
    if not matched_any_trim and global_detail and not summary_already_present:
        new_row: dict = {col: "" for col in fieldnames}
        new_row["Year"] = year
        new_row["Make"] = make
        new_row["Model"] = model
        new_row["Trim"] = "[Brochure Summary]"
        new_row["Options"] = global_option_str
        new_row["optionDetails"] = global_detail
        rows.append(new_row)

    _save_csv(csv_path, fieldnames, rows)
    _log(f"[csv] wrote {csv_path.name} ({len(rows)} rows)")


# ── Entry point ────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--url", help="Single manufacturer page URL on auto-brochures.com")
    p.add_argument(
        "--all", action="store_true",
        help="Discover and download every make/model/year from auto-brochures.com",
    )
    p.add_argument(
        "--download-only", action="store_true",
        help="Download PDFs to backend/data/brochures only; skip CSV extraction/injection",
    )
    p.add_argument(
        "--force", action="store_true",
        help="Re-download even when a valid PDF already exists",
    )
    p.add_argument(
        "--skip-download", action="store_true",
        help="Skip scraping; process existing files in tmp_brochures only",
    )
    p.add_argument(
        "--local-only", action="store_true",
        help="Alias for --skip-download",
    )
    p.add_argument(
        "--dry-run", action="store_true",
        help="Extract and print parsed data without writing CSVs",
    )
    p.add_argument(
        "--retry-failed", action="store_true",
        help="Delete invalid tmp_brochures downloads (HTML/404) and retry from live site links",
    )
    p.add_argument(
        "--workers", type=int, default=4, metavar="N",
        help="Parallel worker threads for downloads and PDF processing (default: 4)",
    )
    return p.parse_args()


_csv_write_lock = threading.Lock()


def _process_one(pdf_path: Path, dry_run: bool) -> None:
    _log(f"\n── {pdf_path.name} ──")
    try:
        result = parse_brochure(pdf_path)
    except Exception as exc:
        _log(f"[main] ERROR parsing {pdf_path.name}: {exc}")
        return

    if not result:
        return

    if dry_run:
        import json
        _log(json.dumps(result, indent=2))
    else:
        try:
            with _csv_write_lock:
                inject_brochure_data(result)
        except Exception as exc:
            _log(f"[main] ERROR injecting {pdf_path.name}: {exc}")


def main() -> None:
    if _PDF_BACKEND is None:
        print("ERROR: no PDF library found. Install one:\n  pip install pdfplumber\n  pip install pypdf")
        sys.exit(1)

    args = _parse_args()
    skip_download = args.skip_download or args.local_only
    workers: int = max(1, args.workers)

    # Part 1: scrape + download (parallel)
    if args.retry_failed:
        saved, still_missing = retry_failed_downloads(workers=workers)
        print(f"\n[main] retry recovered {len(saved)} brochure(s)")
        if still_missing:
            print(f"[main] still unavailable on auto-brochures.com ({len(still_missing)}):")
            for name in still_missing:
                print(f"  - {name}")
        if skip_download and not saved:
            return
    elif not skip_download:
        if args.all:
            saved = scrape_all_makes(workers=workers, force=args.force)
            print(f"\n[main] downloaded {len(saved)} brochure PDF(s) → {_TMP}")
        elif args.url:
            saved = scrape_and_download(args.url, workers=workers, force=args.force)
            print(f"\n[main] downloaded {len(saved)} brochure PDF(s) → {_TMP}")
        else:
            print("ERROR: pass --url <make-page>, --all, --retry-failed, or --local-only")
            sys.exit(1)

    if args.download_only:
        pdf_count = len(list(_TMP.glob("*.pdf")))
        print(f"[main] download-only complete ({pdf_count} PDF(s) in {_TMP})")
        return

    # Parts 2 + 3: extract + inject (parallel)
    pdf_files = sorted(_TMP.glob("*.pdf"))
    if not pdf_files:
        print(f"[main] no PDFs found in {_TMP}")
        return

    print(f"[main] processing {len(pdf_files)} PDF(s) with {workers} worker(s)")
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="pdf") as pool:
        futures = {pool.submit(_process_one, p, args.dry_run): p for p in pdf_files}
        for fut in as_completed(futures):
            try:
                fut.result()
            except Exception as exc:
                _log(f"[main] unhandled error {futures[fut].name}: {exc}")

    print("\n[main] done.")


if __name__ == "__main__":
    main()
