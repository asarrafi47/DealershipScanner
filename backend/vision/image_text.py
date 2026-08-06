"""
Read the text that dealers publish inside listing photos.

Dealer galleries routinely include images that are not photographs of the car at all:
a scan of the Monroney window sticker, or a rendered "Vehicle Highlights" slide listing
packages and equipment. Those images carry option codes, package names and the true
original MSRP -- fields the inventory feed itself frequently omits or falsifies. Until
now the entire gallery was treated as opaque pixels.

This module turns those pixels into text, then into *candidate* facts. It deliberately
stops short of writing anything into ``cars``: extraction and adoption are separate
steps, because OCR of a third-party image is exactly the kind of low-trust source that
has burned this project before (see the fabricated trim "adds" incident -- 3,185 overlay
files, only 13 with provenance). Every candidate produced here carries the image URL it
came from and the OCR confidence that produced it, so a downstream gate can demand
evidence rather than assume it.

Engine
------
``macocr`` (tools/ocr/macocr.swift) wraps Apple's Vision framework: free, on-device, no
API key, no quota, and strong on the dense printed tables a Monroney label uses. That
matters at this scale -- 114,901 of 122,663 active cars carry a gallery, so a
per-image-priced cloud model is not a fleet-wide option. ``ANTHROPIC_API_KEY`` is empty
in this deployment, which is why the pre-existing Claude vision code in this package has
never executed a single time.

Providers are selected with ``IMAGE_TEXT_OCR_PROVIDER``:
  apple_vision  (default on macOS)  -- tools/ocr/bin/macocr
  tesseract                          -- if the operator installed it
  none                               -- disable; every call returns empty

Cross-contamination
-------------------
The single most dangerous failure here is reading *another car's* sticker. Dealers reuse
photos, and group feeds mix rooftops. So: if OCR finds a VIN in the image and it is not
this car's VIN, the whole image is discarded rather than partially trusted. A sticker
without a legible VIN yields equipment candidates but never a price -- an unverifiable
MSRP is worse than no MSRP, which is the bug this module exists to help fix.
"""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
import subprocess
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Sequence

_log = logging.getLogger(__name__)

_REPO_ROOT = Path(__file__).resolve().parents[2]
_MACOCR = _REPO_ROOT / "tools" / "ocr" / "bin" / "macocr"

# Bump when classification or extraction changes in a way that makes stored results
# stale. Rows recorded at an older version are eligible for re-extraction; rows at the
# current version are skipped, which is what makes a fleet-wide pass resumable.
# v2: image selection changed from head+tail to a stride across the whole gallery,
# after head+tail was shown to miss interleaved highlights slides entirely.
IMAGE_TEXT_VERSION = 2

# A Monroney is dense; a photo of a car is not. Images below this yield no useful
# structured content and are classified as photographs without further parsing.
_MIN_LINES_FOR_DOCUMENT = 6

# Vision reports per-line confidence in [0, 1]. Printed labels come back at ~1.0;
# anything this low is a reflection, a windshield sticker at an angle, or noise.
_MIN_MEAN_CONFIDENCE = 0.45

_VIN_RE = re.compile(r"\b([A-HJ-NPR-Z0-9]{17})\b")
# A price has to announce itself, either with a currency symbol or with thousands
# separators. Accepting bare integers was the original mistake here: a Monroney is full
# of four-digit numbers that are not money -- the model year ("MAZDA CX-30 ... 2023"),
# the GVWR ("4345 lbs"), option codes, and VIN tails. A first pass over real galleries
# duly invented options such as ("MAZDA CX-30 2.5 S Select", $2,023).
_MONEY_DOLLAR_RE = re.compile(r"\$\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]{2})?|[0-9]+\.[0-9]{2})")
_MONEY_GROUPED_RE = re.compile(r"(?<![0-9.])([0-9]{1,3}(?:,[0-9]{3})+(?:\.[0-9]{2})?)(?![0-9])")

# Lines stating a measurement. These carry grouped numbers ("GVWR: 4,345 lbs") that
# would otherwise read as prices, and they are never options.
_MEASUREMENT_MARKERS = (
    "gvwr",
    "gawr",
    "vin:",
    "vin ",
    "payload",
    "curb weight",
    "wheelbase",
    "displacement",
    "capacity",
    " lbs",
    " kg",
    "mpg",
    "miles per gallon",
    "fuel economy",
    "annual fuel",
    "rpm",
    "cu. ft",
    "cu ft",
    "octane",
    "seating for",
    "serial",
)

# Section headers and stray fragments that are not the name of anything.
_GENERIC_NAMES = {
    "package",
    "packages",
    "option",
    "options",
    "accessories",
    "equipment",
    "standard equipment",
    "optional equipment",
    "exterior",
    "interior",
    "mechanical",
    "safety",
    "technology",
    "warranty",
    "description",
    "model",
    "color",
    "upholstery",
    "total",
    "msrp",
}
_PHONE_RE = re.compile(r"\(?\d{3}\)?[\s.\-]\d{3}[\s.\-]\d{4}")
_DOMAIN_RE = re.compile(r"\b([a-z0-9][a-z0-9\-]{2,}\.(?:com|net|org))\b", re.I)

# Bulleted equipment lines. Vision renders the glyph inconsistently across the star,
# asterisk and bullet the slide templates use, so accept all of them.
_BULLET_RE = re.compile(r"^\s*[\*•★●·\-–]\s*(.+)$")

# Roll-up rows on a Monroney. They carry a dollar figure but are not options, and
# admitting them would invent an "option" costing as much as the whole car.
_SUMMARY_LINE_MARKERS = (
    "total msrp",
    "net total",
    "base price",
    "total options",
    "total price",
    "destination charge",
    "destination and handling",
    "subtotal",
    "accessories total",
    "grand total",
    "as equipped",
)

_STICKER_MARKERS = (
    "total msrp",
    "base price",
    "destination charge",
    "monroney",
    "total price",
    "manufacturer's suggested",
    "total options",
    "net total",
    "standard equipment",
)

_HIGHLIGHT_MARKERS = ("vehicle highlights", "shop now", "highlights")

# Contact/marketing chrome that shares the slide with real equipment. These lines are
# not equipment and must never become option candidates.
_NOISE_SUBSTRINGS = (
    "shop now",
    "text or call",
    "call or text",
    "questions text",
    "questions call",
    "ask for",
    "stock #",
    "stock number",
    "vehicle highlights",
    "see dealer",
    "while great effort",
    "not the actual",
    "for informational purposes",
    "please contact",
    "prices subject to change",
)


@dataclass(frozen=True)
class OcrResult:
    """Raw OCR for one image. ``error`` set means the image could not be read."""

    path: str
    text: str
    lines: tuple[str, ...]
    mean_confidence: float
    error: str | None = None
    # Parallel to ``lines``: (x, y, width, height), normalised, origin bottom-left.
    # Empty when the provider does not report geometry (tesseract), in which case
    # column pairing degrades to same-line parsing.
    boxes: tuple[tuple[float, float, float, float], ...] = ()

    @property
    def ok(self) -> bool:
        return self.error is None and bool(self.lines)


@dataclass
class ImageFindings:
    """
    What one gallery image appears to contain.

    ``kind`` is the classification; ``equipment``/``packages``/``priced_options`` are
    candidate facts; ``msrp`` is only ever populated for a sticker whose VIN matched.
    ``rejected_reason`` records why an image was discarded, so a fleet-wide run can be
    audited rather than silently dropping evidence.
    """

    url: str = ""
    kind: str = "unknown"  # window_sticker | highlights_slide | dealer_branding | photo | unknown
    mean_confidence: float = 0.0
    vin: str | None = None
    msrp: float | None = None
    equipment: list[str] = field(default_factory=list)
    packages: list[str] = field(default_factory=list)
    priced_options: list[dict[str, Any]] = field(default_factory=list)
    dealer_domains: list[str] = field(default_factory=list)
    rejected_reason: str | None = None
    text: str = ""

    @property
    def has_content(self) -> bool:
        return bool(self.equipment or self.packages or self.priced_options or self.msrp)


# --- OCR providers -----------------------------------------------------------------


def ocr_provider() -> str:
    """Configured provider name, defaulting to Apple Vision when its binary is built."""
    raw = (os.environ.get("IMAGE_TEXT_OCR_PROVIDER") or "").strip().lower()
    if raw:
        return raw
    return "apple_vision" if _MACOCR.exists() else "none"


def ocr_available() -> bool:
    provider = ocr_provider()
    if provider == "apple_vision":
        return _MACOCR.exists()
    if provider == "tesseract":
        return shutil.which("tesseract") is not None
    if provider in ("ollama", "qwen", "vlm"):
        from backend.vision.vlm_ollama import vlm_available

        return vlm_available()
    return False


def _ocr_apple_vision(paths: Sequence[Path]) -> list[OcrResult]:
    if not _MACOCR.exists():
        return [OcrResult(str(p), "", (), 0.0, "macocr_missing") for p in paths]

    # Pass paths via a file: a fleet run batches dozens of images and argv has a limit.
    with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False) as fh:
        fh.write("\n".join(str(p) for p in paths))
        list_path = fh.name
    try:
        proc = subprocess.run(
            [str(_MACOCR), "--paths-from", list_path],
            capture_output=True,
            text=True,
            timeout=max(60, 12 * len(paths)),
        )
    except subprocess.TimeoutExpired:
        return [OcrResult(str(p), "", (), 0.0, "ocr_timeout") for p in paths]
    except OSError as exc:
        return [OcrResult(str(p), "", (), 0.0, f"ocr_spawn_failed: {exc}") for p in paths]
    finally:
        try:
            os.unlink(list_path)
        except OSError:
            pass

    if proc.returncode != 0:
        reason = (proc.stderr or "").strip()[:120] or f"exit_{proc.returncode}"
        return [OcrResult(str(p), "", (), 0.0, reason) for p in paths]

    try:
        payload = json.loads(proc.stdout or "[]")
    except json.JSONDecodeError:
        return [OcrResult(str(p), "", (), 0.0, "ocr_bad_json") for p in paths]

    out: list[OcrResult] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        lines = tuple(str(x) for x in (item.get("lines") or []))
        raw_boxes = item.get("boxes") or []
        boxes: tuple[tuple[float, float, float, float], ...] = tuple(
            (float(b[0]), float(b[1]), float(b[2]), float(b[3]))
            for b in raw_boxes
            if isinstance(b, list) and len(b) == 4
        )
        out.append(
            OcrResult(
                path=str(item.get("path") or ""),
                text=str(item.get("text") or ""),
                lines=lines,
                mean_confidence=float(item.get("meanConfidence") or 0.0),
                error=item.get("error") or None,
                boxes=boxes if len(boxes) == len(lines) else (),
            )
        )
    return out


def _ocr_tesseract(paths: Sequence[Path]) -> list[OcrResult]:
    exe = shutil.which("tesseract")
    if not exe:
        return [OcrResult(str(p), "", (), 0.0, "tesseract_missing") for p in paths]
    results: list[OcrResult] = []
    for p in paths:
        try:
            proc = subprocess.run(
                [exe, str(p), "stdout"], capture_output=True, text=True, timeout=60
            )
        except (subprocess.TimeoutExpired, OSError) as exc:
            results.append(OcrResult(str(p), "", (), 0.0, f"tesseract_failed: {exc}"))
            continue
        lines = tuple(ln.strip() for ln in (proc.stdout or "").splitlines() if ln.strip())
        # tesseract does not report a comparable per-line confidence here; treat a
        # successful read as nominal so downstream thresholds stay meaningful.
        results.append(OcrResult(str(p), "\n".join(lines), lines, 0.75 if lines else 0.0))
    return results


def _ocr_ollama(paths: Sequence[Path]) -> list[OcrResult]:
    """
    Transcribe with a local vision-language model (qwen3-vl via Ollama).

    Slower than Apple Vision by roughly 5x, and it reports no per-line geometry, so the
    column pairing that recovers a two-column Monroney's prices is unavailable and
    extraction falls back to same-line parsing. What it buys in exchange is reading text
    OCR cannot: angled or reflected stickers, text rendered as artwork, and dealer
    branding baked into a photograph.

    The prompt asks for a verbatim transcription rather than a summary, so everything
    downstream -- classification, VIN gating, option extraction -- operates on the same
    shape of input as the OCR providers and needs no special case.
    """
    from backend.vision.vlm_ollama import _ask, vlm_available

    if not vlm_available():
        return [OcrResult(str(p), "", (), 0.0, "ollama_unavailable") for p in paths]

    prompt = (
        "Transcribe ALL text visible in this image, exactly as printed, one item per line. "
        "Preserve option codes, package names, prices and any VIN exactly. "
        "Do not summarise, translate, explain or add commentary. "
        "If the image contains no text, reply with nothing."
    )

    out: list[OcrResult] = []
    for p in paths:
        raw = _ask(p, prompt, expect_json=False)
        if raw is None:
            out.append(OcrResult(str(p), "", (), 0.0, "ollama_failed"))
            continue
        lines = tuple(ln.strip() for ln in str(raw).splitlines() if ln.strip())
        # The model has no calibrated confidence; treat a successful transcription as
        # nominal so the shared confidence floor keeps its meaning.
        out.append(OcrResult(str(p), "\n".join(lines), lines, 0.85 if lines else 0.0))
    return out


def ocr_image_files(paths: Sequence[Path | str]) -> list[OcrResult]:
    """OCR local image files. Always returns one result per input, in input order."""
    resolved = [Path(p) for p in paths]
    if not resolved:
        return []
    provider = ocr_provider()
    if provider == "apple_vision":
        return _ocr_apple_vision(resolved)
    if provider == "tesseract":
        return _ocr_tesseract(resolved)
    if provider in ("ollama", "qwen", "vlm"):
        return _ocr_ollama(resolved)
    return [OcrResult(str(p), "", (), 0.0, "ocr_disabled") for p in resolved]


# --- classification ----------------------------------------------------------------


def classify_image_text(result: OcrResult) -> str:
    """Bucket one OCR result. See ``ImageFindings.kind`` for the vocabulary."""
    if not result.ok:
        return "unknown"
    blob = result.text.lower()

    sticker_hits = sum(1 for m in _STICKER_MARKERS if m in blob)
    # Two independent markers, not one: "total price" alone shows up on promo overlays,
    # while a real Monroney always carries several of these phrases together.
    if sticker_hits >= 2 and len(result.lines) >= _MIN_LINES_FOR_DOCUMENT:
        return "window_sticker"

    if any(m in blob for m in _HIGHLIGHT_MARKERS) and _bullet_lines(result.lines):
        return "highlights_slide"

    if len(result.lines) < _MIN_LINES_FOR_DOCUMENT:
        # Little text: a photo, possibly with dealer signage in frame.
        return "dealer_branding" if _DOMAIN_RE.search(result.text) else "photo"

    return "unknown"


def _bullet_lines(lines: Iterable[str]) -> list[str]:
    out: list[str] = []
    for ln in lines:
        m = _BULLET_RE.match(ln)
        if m:
            out.append(m.group(1).strip())
    return out


def _is_noise(line: str) -> bool:
    low = line.lower().strip()
    if len(low) < 3 or len(low) > 90:
        return True
    if low.strip(" .:-•*") in _GENERIC_NAMES:
        return True
    if any(s in low for s in _NOISE_SUBSTRINGS):
        return True
    if any(s in low for s in _MEASUREMENT_MARKERS):
        return True
    if _PHONE_RE.search(low) or _DOMAIN_RE.search(low):
        return True
    # A bare number, price, or code fragment is not an equipment name.
    if not re.search(r"[a-z]{3}", low):
        return True
    # A 17-character VIN run means this line is an identifier, not a feature.
    if _VIN_RE.search(line.upper()):
        return True
    return False


def _parse_money(text: str) -> float | None:
    """
    The dollar amount on a line, or None.

    Requires explicit currency evidence -- a ``$`` or thousands separators. See
    ``_MONEY_DOLLAR_RE`` for why bare integers are refused.
    """
    if any(s in text.lower() for s in _MEASUREMENT_MARKERS):
        return None
    m = _MONEY_DOLLAR_RE.search(text) or _MONEY_GROUPED_RE.search(text)
    if not m:
        return None
    try:
        val = float(m.group(1).replace(",", ""))
    except ValueError:
        return None
    # Below this a "price" is an option code fragment; above it, OCR has run two
    # columns together into one number.
    return val if 100.0 <= val <= 500_000.0 else None


def _strip_money(text: str) -> str:
    """Remove price tokens so what remains is the option name."""
    return _MONEY_GROUPED_RE.sub("", _MONEY_DOLLAR_RE.sub("", text))


# --- extraction --------------------------------------------------------------------


def _detect_vin(text: str, expected_vin: str | None) -> tuple[str | None, bool]:
    """
    ``(vin_seen, confirms_this_car)``.

    Vision reliably reads the characters of a VIN but not always its layout -- a label
    wraps, or the block is spaced ("7MMVABAL1TN 458581"). A strict search therefore
    finds nothing and, because an unconfirmed sticker yields no price, every MSRP on a
    real gallery was being discarded. Collapsing whitespace and looking for the VIN we
    already know confirms those cases without ever inventing a VIN that was not there.
    """
    up = text.upper()
    m = _VIN_RE.search(up)
    seen = m.group(1) if m else None
    if expected_vin:
        exp = expected_vin.upper()
        if seen == exp or exp in re.sub(r"\s+", "", up):
            return exp, True
    return seen, False


def extract_findings(result: OcrResult, *, expected_vin: str | None = None, url: str = "") -> ImageFindings:
    """
    Turn one OCR result into candidate facts.

    ``expected_vin`` is the VIN of the car this image is attached to. When the image
    contains a *different* VIN the result is rejected outright -- see module docstring.
    """
    findings = ImageFindings(url=url, mean_confidence=result.mean_confidence, text=result.text)

    if not result.ok:
        findings.rejected_reason = result.error or "ocr_empty"
        return findings

    if result.mean_confidence < _MIN_MEAN_CONFIDENCE:
        findings.rejected_reason = f"low_confidence:{result.mean_confidence:.2f}"
        return findings

    findings.kind = classify_image_text(result)

    seen_vin, vin_confirmed = _detect_vin(result.text, expected_vin)
    findings.vin = seen_vin
    if expected_vin and seen_vin and not vin_confirmed:
        # Another car's document. Discard everything: a sticker that belongs to a
        # different VIN is not partially usable.
        findings.rejected_reason = f"vin_mismatch:{seen_vin}"
        return findings

    findings.dealer_domains = sorted({m.group(1).lower() for m in _DOMAIN_RE.finditer(result.text)})

    if findings.kind == "highlights_slide":
        _extract_highlights(result, findings)
    elif findings.kind == "window_sticker":
        _extract_sticker(result, findings, vin_confirmed=vin_confirmed)

    return findings


def _extract_highlights(result: OcrResult, findings: ImageFindings) -> None:
    """Equipment bullets off a 'Vehicle Highlights' slide."""
    seen: set[str] = set()
    for raw in _bullet_lines(result.lines):
        name = raw.strip(" .*-•")
        if _is_noise(name):
            continue
        key = name.lower()
        if key in seen:
            continue
        seen.add(key)
        if "package" in key or "pkg" in key:
            findings.packages.append(name)
        else:
            findings.equipment.append(name)


def _row_value(result: OcrResult, idx: int) -> float | None:
    """
    The dollar amount sitting on the same printed row as line ``idx``.

    A Monroney is a two-column table and Vision emits each column as its own
    observation, so "Total MSRP" and "$26,415.00" arrive as separate lines that are not
    adjacent in reading order. Pairing them needs geometry: find the money-bearing line
    whose vertical centre is within half a line-height of this one and which starts to
    its right, then take the nearest such candidate.

    Returns None when the provider gave no geometry, which keeps tesseract and the unit
    tests on the plain same-line path.
    """
    if not result.boxes or idx >= len(result.boxes):
        return None
    x, y, _w, h = result.boxes[idx]
    centre = y + h / 2.0
    tolerance = max(h * 0.6, 0.004)

    best: tuple[float, float] | None = None  # (distance, value)
    for j, (bx, by, _bw, bh) in enumerate(result.boxes):
        if j == idx:
            continue
        if bx < x:  # value columns sit to the right of their label
            continue
        if abs((by + bh / 2.0) - centre) > tolerance:
            continue
        val = _parse_money(result.lines[j])
        if val is None:
            continue
        dist = bx - x
        if best is None or dist < best[0]:
            best = (dist, val)
    return best[1] if best else None


def _extract_sticker(result: OcrResult, findings: ImageFindings, *, vin_confirmed: bool) -> None:
    """
    Priced options and total MSRP off a Monroney.

    The MSRP is only taken when the sticker's own VIN confirmed the car. Equipment names
    are safe to gather either way -- a wrong option name is a cosmetic error, a wrong
    price on a shopper-facing "below MSRP" badge is not.
    """
    # Pass 1: for every line, the price printed on it and the price sitting in the
    # column to its right.
    rows: list[tuple[int, str, str, bool, float | None, float | None]] = []
    for idx, ln in enumerate(result.lines):
        low = ln.lower()
        is_summary = any(m in low for m in _SUMMARY_LINE_MARKERS)
        same = _parse_money(ln)
        paired = None if same is not None else _row_value(result, idx)
        rows.append((idx, ln, low, is_summary, same, paired))

    # A figure that lines up with several labels identifies none of them. A Monroney
    # prints its standard-equipment columns alongside the priced-options column, so a
    # naive row match produced "Traction Control $150", "Double Wishbone Front
    # Suspension $300" -- each price claimed by three unrelated labels. Where the
    # pairing is ambiguous the option keeps its name and loses its price, rather than
    # us picking one of the candidates and presenting a guess as a fact.
    paired_counts: dict[float, int] = {}
    for _, _, _, is_summary, same, paired in rows:
        if not is_summary and same is None and paired is not None:
            paired_counts[paired] = paired_counts.get(paired, 0) + 1

    for idx, ln, low, is_summary, same, paired in rows:
        if is_summary:
            # Summary labels appear once on a sticker, so column pairing is unambiguous
            # here even though it is not for options.
            if vin_confirmed and findings.msrp is None and ("total msrp" in low or "net total" in low):
                findings.msrp = same if same is not None else paired
            # Every summary row stops here: it is never an option, priced or otherwise.
            continue

        price = same
        if price is None and paired is not None and paired_counts.get(paired, 0) == 1:
            price = paired

        # Strip any inline price and list glyphs so the remainder is the option name.
        name = _strip_money(ln).strip(" .$–-—•*★●·\t")
        if _is_noise(name):
            continue
        if price is not None:
            findings.priced_options.append({"name": name, "price": price})
        if "package" in low or "pkg" in low:
            findings.packages.append(name)
        elif price is None:
            findings.equipment.append(name)

    # De-duplicate while preserving sticker order.
    findings.equipment = _dedupe(findings.equipment)
    findings.packages = _dedupe(findings.packages)


def _dedupe(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for it in items:
        k = it.lower().strip()
        if k and k not in seen:
            seen.add(k)
            out.append(it)
    return out


# --- gallery-level entry point ------------------------------------------------------


def _select_images(urls: list[str], max_images: int) -> list[str]:
    """
    Which images to spend OCR on when a gallery is larger than the budget.

    Evenly spaced across the whole gallery, always keeping the first and last.

    Two earlier strategies were wrong, each for its own reason. Taking the head reads
    only exteriors and found 0 stickers across 40 cars. Taking head+tail assumed the
    documents sit at the end -- but on the gallery that started this work the
    "Vehicle Highlights" slides were at positions 6, 12 and 18 of 40, interleaved with
    the photographs, so a head+tail sample missed every one of them and recovered the
    dealer's domain from only 4 of 336 cars.

    Dealers interleave document shots wherever they like, so the only assumption that
    survives contact with real galleries is that they could be anywhere. A stride
    sample makes no assumption about position; full recall still needs a budget at
    least as large as the gallery.
    """
    n = len(urls)
    if n <= max_images or max_images <= 0:
        return urls
    if max_images == 1:
        return urls[:1]
    # Spread max_images picks across [0, n-1] inclusive, so both ends are covered.
    step = (n - 1) / (max_images - 1)
    idxs = sorted({int(round(i * step)) for i in range(max_images)})
    return [urls[i] for i in idxs]


def read_gallery(
    urls: Sequence[str],
    *,
    expected_vin: str | None = None,
    fetch=None,
    max_images: int = 60,
) -> list[ImageFindings]:
    """
    Download and read a car's gallery.

    ``fetch`` is an injection point for tests and for callers that already have bytes;
    it takes a URL and returns image bytes or None. The default fetcher is supplied by
    the caller-facing runner so this module stays free of HTTP policy (referer headers,
    pacing, proxies) that differs per platform.
    """
    findings: list[ImageFindings] = []
    if not urls or not ocr_available():
        return findings

    selected = _select_images(list(urls), max_images)
    tmpdir = Path(tempfile.mkdtemp(prefix="imgtext_"))
    try:
        staged: list[tuple[Path, str]] = []
        for idx, url in enumerate(selected):
            blob = fetch(url) if fetch else None
            if not blob:
                continue
            p = tmpdir / f"img_{idx:03d}.jpg"
            try:
                p.write_bytes(blob)
            except OSError:
                continue
            staged.append((p, url))

        if not staged:
            return findings

        results = ocr_image_files([p for p, _ in staged])
        for (path, url), res in zip(staged, results):
            findings.append(extract_findings(res, expected_vin=expected_vin, url=url))
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

    return findings


def summarize_gallery(findings: Sequence[ImageFindings]) -> dict[str, Any]:
    """Collapse per-image findings into one candidate record for a car."""
    equipment: list[str] = []
    packages: list[str] = []
    priced: list[dict[str, Any]] = []
    msrp: float | None = None
    sticker_urls: list[str] = []
    domains: set[str] = set()
    rejected: list[dict[str, str]] = []

    for f in findings:
        if f.rejected_reason:
            rejected.append({"url": f.url, "reason": f.rejected_reason})
            continue
        domains.update(f.dealer_domains)
        if f.kind == "window_sticker":
            sticker_urls.append(f.url)
            if msrp is None and f.msrp is not None:
                msrp = f.msrp
        equipment.extend(f.equipment)
        packages.extend(f.packages)
        priced.extend(f.priced_options)

    return {
        "version": IMAGE_TEXT_VERSION,
        "equipment": _dedupe(equipment),
        "packages": _dedupe(packages),
        "priced_options": priced,
        "sticker_msrp": msrp,
        "sticker_image_urls": sticker_urls,
        "dealer_domains": sorted(domains),
        "images_read": sum(1 for f in findings if not f.rejected_reason),
        "images_rejected": rejected,
    }
