"""
OEM Monroney / window sticker URL resolution by VIN WMI.

Tier 1 — reliable public PDF endpoints (post-scan fetch):
  Stellantis: Ram, Jeep, Dodge, Chrysler (``/hostd/windowsticker/getWindowStickerPdf.do``)
  Ford / Lincoln: ``windowsticker.forddirect.com`` (Lincoln may also try lincolnvehicles.com)

Tier 2 — experimental (opt-in ``WINDOW_STICKER_GM_EXPERIMENTAL=1``):
  GM: ``cws.gm.com/vs-cws/vehshop/v2/vehicle/windowsticker`` (unstable / rate-limited)

Not used for PDF storage (third-party build decode, not Monroney):
  BMW, Mercedes, Porsche, and similar decoder sites.

Usage: ``get_window_sticker_candidate_urls(vin)`` then ``fetch_window_sticker_pdf(vin)``.
"""
from __future__ import annotations

import logging
import os
import re
from typing import Any

logger = logging.getLogger(__name__)

_PYPDF_MISSING_LOGGED = False

# --- Stellantis (best support): WMI prefix → brand site host ---
_STELLANTIS_WMI: dict[str, str] = {
    # Ram
    "1C6": "ramtrucks",
    "3C6": "ramtrucks",
    # Jeep
    "1C4": "jeep",
    "1J4": "jeep",
    "1J8": "jeep",
    # Dodge
    "1B3": "dodge",
    "2B3": "dodge",
    "2D4": "dodge",
    "2C3": "dodge",
    # Chrysler
    "1C3": "chrysler",
    "2C4": "chrysler",
    "3C4": "chrysler",
    "2A4": "chrysler",
}

_FORD_WMI: frozenset[str] = frozenset({
    "1FA", "1FB", "1FC", "1FD", "1FM", "1FT", "1FU",
    "3FA", "3FB", "3FM",
    "2FM", "2FT",
})

_LINCOLN_WMI: frozenset[str] = frozenset({"5LM", "5LJ"})

_GM_WMI: frozenset[str] = frozenset({
    "1G1", "1G8", "2G1",  # Chevrolet
    "1GT", "1GC", "2GT", "3GT",  # GMC
    "1G4", "2G4", "KL4",  # Buick
    "1GY", "1GR",  # Cadillac
})

_STELLANTIS_PDF_PATH = "/hostd/windowsticker/getWindowStickerPdf.do?vin={vin}"
_FORD_PDF_URL = "https://www.windowsticker.forddirect.com/windowsticker.pdf?vin={vin}"
_LINCOLN_PDF_URL = "https://www.windowsticker.lincolnvehicles.com/windowsticker.pdf?vin={vin}"
_GM_EXPERIMENTAL_URL = (
    "https://cws.gm.com/vs-cws/vehshop/v2/vehicle/windowsticker?vin={vin}"
)
_GM_LEGACY_URL = "https://www.gmwindowsticker.com/window-sticker/window-sticker.pdf?vin={vin}"

_TIER_RELIABLE = "reliable_pdf"
_TIER_EXPERIMENTAL = "experimental"


def _vin_norm(vin: str | None) -> str | None:
    s = (vin or "").strip().upper()
    return s if len(s) == 17 else None


def _gm_experimental_enabled() -> bool:
    return (os.environ.get("WINDOW_STICKER_GM_EXPERIMENTAL") or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def _stellantis_pdf_url(vin: str, brand_host: str) -> str:
    return f"https://www.{brand_host}.com{_STELLANTIS_PDF_PATH.format(vin=vin)}"


def get_window_sticker_candidate_urls(vin: str) -> list[tuple[str, str]]:
    """
    Ordered ``(url, tier)`` candidates to try for this VIN.
    ``tier`` is ``reliable_pdf`` or ``experimental``.
    """
    vnorm = _vin_norm(vin)
    if not vnorm:
        return []

    wmi3 = vnorm[:3]
    out: list[tuple[str, str]] = []

    brand_host = _STELLANTIS_WMI.get(wmi3)
    if brand_host:
        out.append((_stellantis_pdf_url(vnorm, brand_host), _TIER_RELIABLE))

    if wmi3 in _FORD_WMI:
        out.append((_FORD_PDF_URL.format(vin=vnorm), _TIER_RELIABLE))

    if wmi3 in _LINCOLN_WMI:
        out.append((_FORD_PDF_URL.format(vin=vnorm), _TIER_RELIABLE))
        out.append((_LINCOLN_PDF_URL.format(vin=vnorm), _TIER_RELIABLE))

    if wmi3 in _GM_WMI and _gm_experimental_enabled():
        out.append((_GM_EXPERIMENTAL_URL.format(vin=vnorm), _TIER_EXPERIMENTAL))
        out.append((_GM_LEGACY_URL.format(vin=vnorm), _TIER_EXPERIMENTAL))

    return out


def get_window_sticker_url(vin: str) -> str | None:
    """Primary OEM sticker URL for this VIN, or ``None`` if unsupported."""
    candidates = get_window_sticker_candidate_urls(vin)
    return candidates[0][0] if candidates else None


def window_sticker_oem_family(vin: str) -> str | None:
    """Short label for logging: ``stellantis-jeep``, ``ford``, ``gm-experimental``, etc."""
    vnorm = _vin_norm(vin)
    if not vnorm:
        return None
    wmi3 = vnorm[:3]
    if host := _STELLANTIS_WMI.get(wmi3):
        return f"stellantis-{host}"
    if wmi3 in _FORD_WMI:
        return "ford"
    if wmi3 in _LINCOLN_WMI:
        return "lincoln"
    if wmi3 in _GM_WMI and _gm_experimental_enabled():
        return "gm-experimental"
    return None


def fetch_window_sticker_pdf(vin: str, *, timeout: float = 20.0) -> dict[str, Any] | None:
    """
    Fetch OEM window sticker bytes. Tries each candidate URL until a PDF is returned.
    """
    candidates = get_window_sticker_candidate_urls(vin)
    if not candidates:
        return None

    last_error: Exception | None = None
    for url, tier in candidates:
        try:
            result = _fetch_url_as_sticker(url, vin, timeout=timeout, tier=tier)
            if result and _is_valid_sticker_payload(result):
                return result
        except Exception as e:
            last_error = e
            logger.debug("Window sticker try failed %s (%s): %s", vin, url[:60], e)

    if last_error:
        logger.debug("Window sticker PDF fetch failed for %s: %s", vin, last_error)
    return None


def _fetch_url_as_sticker(
    url: str,
    vin: str,
    *,
    timeout: float,
    tier: str,
) -> dict[str, Any] | None:
    allowed = {u for u, _ in get_window_sticker_candidate_urls(vin)}
    if url not in allowed:
        logger.warning(
            "Window sticker fetch blocked: URL not in OEM candidate list (vin=%s)",
            vin,
        )
        return None

    import requests

    r = requests.get(
        url,
        timeout=timeout,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept": "application/pdf,*/*;q=0.8",
        },
        allow_redirects=True,
    )
    r.raise_for_status()
    ct = (r.headers.get("content-type") or "").lower()
    content = r.content
    raw_text = ""
    if "pdf" in ct or url.endswith(".pdf") or content[:4] == b"%PDF":
        raw_text = _extract_pdf_text(content)
    elif "html" in ct or "json" in ct:
        raw_text = r.text[:8000]
    else:
        raw_text = r.text[:4000] if r.text else _extract_pdf_text(content)

    return {
        "url": url,
        "tier": tier,
        "oem_family": window_sticker_oem_family(vin),
        "content": content,
        "content_type": ct or ("application/pdf" if content[:4] == b"%PDF" else "text/html"),
        "raw_text": raw_text[:14000],
        "options": _parse_options_from_sticker_text(raw_text),
        "msrp": _parse_msrp_from_sticker_text(raw_text),
    }


def _is_valid_sticker_payload(result: dict[str, Any]) -> bool:
    content = result.get("content") or b""
    if len(content) < 500:
        return False
    if content[:4] == b"%PDF":
        return True
    ct = str(result.get("content_type") or "").lower()
    return "pdf" in ct


def fetch_window_sticker_text(vin: str, *, timeout: float = 20.0) -> dict[str, Any] | None:
    """
    Fetch the OEM window sticker PDF for this VIN and extract text.
    Returns dict with keys: url, raw_text, options (list[str]), msrp (int|None)
    """
    fetched = fetch_window_sticker_pdf(vin, timeout=timeout)
    if not fetched:
        return None
    return {
        "url": fetched.get("url"),
        "raw_text": fetched.get("raw_text") or "",
        "options": fetched.get("options") or [],
        "msrp": fetched.get("msrp"),
    }


def _extract_pdf_text(raw: bytes) -> str:
    if not raw or raw[:4] != b"%PDF":
        return ""
    try:
        import pdfplumber
        from io import BytesIO

        with pdfplumber.open(BytesIO(raw)) as pdf:
            parts = []
            for page in pdf.pages[:6]:
                t = page.extract_text() or ""
                parts.append(t)
            joined = "\n".join(parts).strip()
            if joined:
                return joined
    except ImportError:
        pass
    except Exception as e:
        logger.debug("pdfplumber extract failed: %s", e)
    try:
        import pypdf
        from io import BytesIO

        reader = pypdf.PdfReader(BytesIO(raw))
        parts = []
        for page in reader.pages[:6]:
            parts.append(page.extract_text() or "")
        joined = "\n".join(parts).strip()
        if joined:
            return joined
    except ImportError:
        global _PYPDF_MISSING_LOGGED
        if not _PYPDF_MISSING_LOGGED:
            _PYPDF_MISSING_LOGGED = True
            import sys

            logger.warning(
                "pypdf not installed for %s — window sticker text extraction disabled. "
                "Run: %s -m pip install pypdf",
                sys.executable,
                sys.executable,
            )
    except Exception as e:
        logger.debug("pypdf extract failed: %s", e)
    try:
        import shutil
        import subprocess
        import tempfile

        if shutil.which("pdftotext"):
            with tempfile.NamedTemporaryFile(suffix=".pdf", delete=True) as tmp:
                tmp.write(raw)
                tmp.flush()
                proc = subprocess.run(
                    ["pdftotext", "-layout", tmp.name, "-"],
                    capture_output=True,
                    timeout=30,
                    check=False,
                )
                if proc.returncode == 0 and proc.stdout:
                    text = proc.stdout.decode("utf-8", errors="replace").strip()
                    if text:
                        return text
    except Exception as e:
        logger.debug("pdftotext extract failed: %s", e)
    return ""


_OPTION_LINE_RE = re.compile(
    r"^([A-Z][A-Za-z0-9 &/\-]{3,60})\s*[\$\d,]*\s*$",
    re.MULTILINE,
)
_MSRP_RE = re.compile(
    r"(?:total\s+msrp|base\s+msrp|msrp|sticker\s+price)[^\d]*(\d[\d,]+)",
    re.IGNORECASE,
)
_TOTAL_PRICE_RE = re.compile(
    r"total\s+price[^\d$]*\$?\s*([\d,]+)",
    re.IGNORECASE,
)
_BASE_PRICE_RE = re.compile(
    r"base\s+price[^\d$]*\$?\s*([\d,]+)",
    re.IGNORECASE,
)
_OPTIONAL_SECTION_RE = re.compile(
    r"OPTIONAL\s+EQUIPMENT\s*\([^)]*May\s+Replace[^)]*\)(.*?)(?=Destination\s+Charge|TOTAL\s+PRICE|WARRANTY|EPA|FUEL\s+ECONOMY|$)",
    re.IGNORECASE | re.DOTALL,
)
_CUST_PACKAGE_RE = re.compile(
    r"(Customer\s+Preferred\s+Package\s+[A-Z0-9]+[^\n$]*(?:\(\$[\d,]+\))?)",
    re.IGNORECASE,
)
_PACKAGE_LINE_RE = re.compile(
    r"^(\*{0,2}(?:customer\s+preferred\s+package|package|group)[^\n]{8,90}(?:\(\$[\d,]+\))?)",
    re.IGNORECASE | re.MULTILINE,
)

_NOISE_WORDS = frozenset({
    "page", "vehicle", "options", "packages", "model", "msrp", "base", "total",
    "price", "destination", "charges", "dealer", "installed", "equipment",
})


def _parse_options_from_sticker_text(text: str) -> list[str]:
    if not (text or "").strip():
        return []
    options: list[str] = []
    seen: set[str] = set()

    def _add(label: str) -> None:
        s = re.sub(r"\s+", " ", (label or "").strip())
        if len(s) < 6 or len(s) > 220:
            return
        if s.endswith("("):
            s = s[:-1].strip()
        if len(s) < 6:
            return
        k = s.lower()
        if k in seen:
            return
        if k in (
            "standard equipment",
            "optional equipment",
            "functional/safety features",
            "exterior color",
            "interior color",
        ):
            return
        if re.match(r"^(transmission|engine)\b", k):
            return
        seen.add(k)
        options.append(s)

    section = text
    sec_m = _OPTIONAL_SECTION_RE.search(text)
    if sec_m:
        section = sec_m.group(1)

    for m in _CUST_PACKAGE_RE.finditer(text):
        _add(m.group(1))

    for m in _PACKAGE_LINE_RE.finditer(section):
        _add(m.group(1))

    for line in section.splitlines():
        s = line.strip()
        if not s or len(s) < 8:
            continue
        if re.search(r"\(\$[\d,]+\)", s) and re.search(
            r"package|group|care|sunroof|wheel|towing|navigation|preferred",
            s,
            re.I,
        ):
            _add(s)
        elif s.startswith("•") or s.startswith("-") or s.startswith("*"):
            _add(s.lstrip("•-* ").strip())

    for m in _OPTION_LINE_RE.finditer(section):
        label = m.group(1).strip()
        words = set(label.lower().split())
        if words & _NOISE_WORDS and len(words) <= 2:
            continue
        if 5 <= len(label) <= 90:
            _add(label)

    return options[:60]


def _parse_msrp_from_sticker_text(text: str) -> int | None:
    for pat in (_TOTAL_PRICE_RE, _MSRP_RE, _BASE_PRICE_RE):
        m = pat.search(text)
        if not m:
            continue
        try:
            val = int(m.group(1).replace(",", ""))
            if 5000 <= val <= 500000:
                return val
        except ValueError:
            continue
    return None


_ENGINE_LINE_RE = re.compile(
    r"(\d+\.\d+)\s*L\s+(V\s*-?\s*\d|I\s*-?\s*\d|H\s*-?\s*\d|L\d)\b[^\n]{0,80}",
    re.IGNORECASE,
)
_ENGINE_LABEL_RE = re.compile(
    r"(?:^|\n)\s*(?:ENGINE|Engine)\s*[:\s]+([^\n]{6,120})",
    re.MULTILINE,
)
_TRANSMISSION_RE = re.compile(
    r"(?:^|\n)\s*(?:TRANSMISSION|Transmission)\s*[:\s]+([^\n]{6,100})",
    re.MULTILINE,
)
_TRANSMISSION_INLINE_RE = re.compile(
    r"(\d+\s*-?\s*Speed\s+(?:Automatic|Auto|Manual|Dual\s+Clutch)[^\n]{0,60})",
    re.IGNORECASE,
)
_DRIVETRAIN_RE = re.compile(
    r"\b(4X4|4WD|AWD|FWD|RWD|2WD|4x4|Quattro)\b",
    re.IGNORECASE,
)
_DOORS_RE = re.compile(
    r"\b(\d)\s*[- ]?\s*(?:DOOR|Door|Doors)\b",
    re.IGNORECASE,
)
_SEATING_RE = re.compile(
    r"\b(\d+)\s*(?:Passenger|passenger|Seating|seating)\b",
)
_TIRES_RE = re.compile(
    r"\b((?:LT|P)?\d{3}/\d{2}R\s*\d{2}[A-Z]?\s+[A-Za-z0-9 /\-]{3,48}(?:Tires?|tires?)?)\b",
)
_WHEELS_RE = re.compile(
    r"\b(\d{1,2}(?:\.\d)?\s*[-\"]\s*(?:Inch|in)\s+x\s+\d{1,2}(?:\.\d)?\s*[-\"]\s*(?:Inch|in)[^\n]{0,60}(?:Wheels?|wheels?)?)\b",
    re.IGNORECASE,
)
_EXTERIOR_COLOR_RE = re.compile(
    r"(?:Exterior\s+Color|EXTERIOR\s+COLOR)\s*\n?\s*([^\n]{4,120}(?:Exterior\s+Paint|Paint|Clear-Coat|Coat)?)",
    re.IGNORECASE,
)
_INTERIOR_COLOR_RE = re.compile(
    r"(?:Interior\s+Color|INTERIOR\s+COLOR)\s*\n?\s*([^\n]{4,140})",
    re.IGNORECASE,
)
_INTERIOR_MATERIAL_RE = re.compile(
    r"\b((?:Nappa|Premium|Leather|Cloth|Vinyl|Alcantara|Sensatec|SofTex|Leatherette)[^\n]{0,60}(?:Seats|Interior|Trim|Buckets?)?)\b",
    re.IGNORECASE,
)
_STANDARD_SECTION_RE = re.compile(
    r"STANDARD\s+EQUIPMENT\s*\([^)]*\)(.*?)(?=OPTIONAL\s+EQUIPMENT|Destination\s+Charge|TOTAL\s+PRICE|EPA|FUEL\s+ECONOMY|$)",
    re.IGNORECASE | re.DOTALL,
)


def _normalize_layout_token(raw: str) -> str:
    s = re.sub(r"\s+", "", (raw or "").upper())
    m = re.match(r"V(\d{1,2})", s)
    if m:
        return f"V{int(m.group(1))}"
    m = re.match(r"I(\d)", s)
    if m:
        return f"I{int(m.group(1))}"
    m = re.match(r"H(\d)", s)
    if m:
        return f"H{int(m.group(1))}"
    return ""


def _layout_to_cylinders(layout: str) -> int | None:
    m = re.match(r"[VIH](\d+)", layout or "")
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


def _clean_sticker_line(s: str, *, max_len: int = 140) -> str:
    out = re.sub(r"\s+", " ", (s or "").strip())
    out = re.sub(r"^:\s*", "", out)
    out = re.sub(r"\(\$[\d,]+\)\s*$", "", out).strip()
    return out[:max_len]


def _parse_engine_from_sticker_text(text: str) -> dict[str, Any]:
    blob = text or ""
    engine_raw = ""
    liters: float | None = None
    layout = ""

    for pat in (_ENGINE_LABEL_RE,):
        m = pat.search(blob)
        if m:
            engine_raw = _clean_sticker_line(m.group(1), max_len=120)
            break

    m = _ENGINE_LINE_RE.search(blob)
    if m:
        if not engine_raw:
            engine_raw = _clean_sticker_line(m.group(0), max_len=120)
        try:
            liters = float(m.group(1))
        except ValueError:
            liters = None
        layout = _normalize_layout_token(m.group(2))
    elif engine_raw:
        lm = re.search(r"(\d+\.\d+)\s*L", engine_raw, re.I)
        if lm:
            try:
                liters = float(lm.group(1))
            except ValueError:
                liters = None
        lm2 = re.search(r"\b(V\s*-?\s*\d|I\s*-?\s*\d|H\s*-?\s*\d)\b", engine_raw, re.I)
        if lm2:
            layout = _normalize_layout_token(lm2.group(1))

    display = ""
    if liters is not None and liters > 0:
        display = f"{liters:.1f}L"
        if layout:
            display = f"{display} {layout}"
    elif engine_raw:
        display = engine_raw[:80]

    return {
        "engine_raw": engine_raw,
        "engine_l": liters,
        "engine_layout": layout,
        "engine_display": display,
        "cylinders": _layout_to_cylinders(layout),
    }


def _parse_standard_highlights(text: str, *, limit: int = 18) -> list[str]:
    m = _STANDARD_SECTION_RE.search(text or "")
    if not m:
        return []
    section = m.group(1)
    out: list[str] = []
    seen: set[str] = set()
    for line in section.splitlines():
        s = _clean_sticker_line(line.strip("•-* "), max_len=120)
        if len(s) < 8 or len(s) > 120:
            continue
        if re.search(r"^(functional|interior|exterior)\s*/\s*safety", s, re.I):
            continue
        k = s.lower()
        if k in seen:
            continue
        seen.add(k)
        out.append(s)
        if len(out) >= limit:
            break
    return out


def parse_sticker_from_text(text: str) -> dict[str, Any]:
    """
    Extract Monroney fields from OEM sticker text (CDJR / Ford / Lincoln layout).
    No LLM — regex + section heuristics only.
    """
    blob = (text or "").strip()
    out: dict[str, Any] = {
        "options": [],
        "standard_highlights": [],
        "sticker_specs": {},
    }
    if not blob:
        return out

    engine = _parse_engine_from_sticker_text(blob)
    out.update({k: v for k, v in engine.items() if v not in (None, "", [])})

    tm = _TRANSMISSION_RE.search(blob)
    if tm:
        out["transmission"] = _clean_sticker_line(tm.group(1), max_len=90)
    else:
        ti = _TRANSMISSION_INLINE_RE.search(blob)
        if ti:
            out["transmission"] = _clean_sticker_line(ti.group(1), max_len=90)

    dm = _DRIVETRAIN_RE.search(blob)
    if dm:
        out["drivetrain"] = dm.group(1).upper().replace("4X4", "4WD")

    doors_m = _DOORS_RE.search(blob)
    if doors_m:
        out["doors"] = f"{doors_m.group(1)}-door"

    seat_m = _SEATING_RE.search(blob)
    if seat_m:
        out["seating"] = f"{seat_m.group(1)} passenger"

    tire_m = _TIRES_RE.search(blob)
    if tire_m:
        out["tires"] = _clean_sticker_line(tire_m.group(1), max_len=90)

    wheel_m = _WHEELS_RE.search(blob)
    if wheel_m:
        out["wheels"] = _clean_sticker_line(wheel_m.group(1), max_len=90)

    ext_m = _EXTERIOR_COLOR_RE.search(blob)
    if ext_m:
        out["exterior_color"] = _clean_sticker_line(ext_m.group(1), max_len=100)

    int_m = _INTERIOR_COLOR_RE.search(blob)
    if int_m:
        interior = _clean_sticker_line(int_m.group(1), max_len=120)
        out["interior_color"] = interior
        mat_m = _INTERIOR_MATERIAL_RE.search(interior)
        if mat_m:
            out["interior_material"] = _clean_sticker_line(mat_m.group(1), max_len=80)

    if not out.get("interior_material"):
        mat_m = _INTERIOR_MATERIAL_RE.search(blob)
        if mat_m:
            out["interior_material"] = _clean_sticker_line(mat_m.group(1), max_len=80)

    out["options"] = _parse_options_from_sticker_text(blob)
    out["standard_highlights"] = _parse_standard_highlights(blob)
    out["msrp"] = _parse_msrp_from_sticker_text(blob)

    specs: dict[str, str] = {}
    if out.get("engine_display"):
        specs["Engine"] = str(out["engine_display"])
    elif out.get("engine_raw"):
        specs["Engine"] = str(out["engine_raw"])
    for key, label in (
        ("transmission", "Transmission"),
        ("drivetrain", "Drivetrain"),
        ("doors", "Doors"),
        ("seating", "Seating"),
        ("exterior_color", "Exterior"),
        ("interior_color", "Interior"),
        ("interior_material", "Interior material"),
        ("tires", "Tires"),
        ("wheels", "Wheels"),
    ):
        val = out.get(key)
        if isinstance(val, str) and val.strip():
            specs[label] = val.strip()
    out["sticker_specs"] = specs
    return out


def pdf_text_extraction_available() -> bool:
    """True when at least one PDF text backend (pypdf / pdfplumber) is importable."""
    try:
        import pypdf  # noqa: F401

        return True
    except ImportError:
        pass
    try:
        import pdfplumber  # noqa: F401

        return True
    except ImportError:
        pass
    return False


def known_oem_engine_from_car(car: dict[str, Any]) -> dict[str, Any]:
    """
    Trim/title heuristics when Monroney text is unavailable.
    Jeep Wrangler / Gladiator Rubicon 392 = 6.4L V8 HEMI.
    """
    make = str(car.get("make") or "").strip().lower()
    blob = " ".join(
        str(car.get(k) or "") for k in ("model", "trim", "title")
    ).lower()
    if make == "jeep" and "392" in blob and ("wrangler" in blob or "gladiator" in blob):
        return {
            "engine_display": "6.4L V8",
            "engine_l": 6.4,
            "cylinders": 8,
            "engine_raw": "6.4L V8 HEMI",
            "sticker_specs": {"Engine": "6.4L V8"},
        }
    return {}


def oem_sticker_parsing_skip_claude(vin: str | None) -> bool:
    """CDJR / Ford / Lincoln Monroney PDFs parse reliably without Claude."""
    fam = window_sticker_oem_family(vin or "")
    if not fam:
        return False
    return fam.startswith("stellantis") or fam in ("ford", "lincoln")


def oem_hide_photo_analysis(vin: str | None, make: str | None = None) -> bool:
    """Sticker-first OEMs: skip vision/photo-analysis package UI."""
    if oem_sticker_parsing_skip_claude(vin):
        return True
    m = (make or "").strip().lower()
    return m in {"jeep", "chrysler", "dodge", "ram", "ford", "lincoln"}
