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
    r"OPTIONAL\s+EQUIPMENT(?:\s*\([^)]*\))?(.*?)(?=Destination\s+Charge|TOTAL\s+PRICE|WARRANTY|EPA|FUEL\s+ECONOMY|$)",
    re.IGNORECASE | re.DOTALL,
)
_MONRONEY_TRAILING_PRICE_RE = re.compile(
    r"^(.+?)\s+\$([\d,]+)(?:\.\d{2})?\s*$"
)
_MONRONEY_INDENTED_LINE_RE = re.compile(r"^(\s{2,}|\t)(.+)$")
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

_OPTION_NAME_NOISE = frozenset({
    "standard equipment",
    "optional equipment",
    "functional/safety features",
    "exterior color",
    "interior color",
    "added options",
    "description msrp",
})

_IPACKET_PRICED_LINE_RE = re.compile(
    r"^\s*([A-Z0-9]{2,4})\s+(.+?)\s+\$([\d,]+)(?:\.\d{2})?\s*$",
    re.MULTILINE,
)


def _parse_dollar_amount(raw: str) -> int | None:
    """Parse sticker price in whole dollars; negative for credits."""
    if not (raw or "").strip():
        return None
    s = str(raw).strip()
    is_credit = bool(
        re.search(r"\bcredit\b", s, re.I)
        or re.search(r"\(\$[\-\s]", s)
        or s.startswith("-")
        or "$-" in s
    )
    m = re.search(r"[\-$]?\s*\$([\d,]+)(?:\.\d{2})?", s)
    if not m:
        return None
    try:
        val = int(m.group(1).replace(",", ""))
    except ValueError:
        return None
    if val <= 0 or val > 500_000:
        return None
    return -val if is_credit else val


def _clean_sticker_option_name(name: str) -> str:
    s = re.sub(r"\s+", " ", (name or "").strip())
    s = re.sub(r"\(\$[\d,\.]+\)\s*:?\s*$", "", s).strip()
    s = re.sub(r"\s+\$[\d,\.]+(?:\.\d{2})?\s*$", "", s).strip()
    if s.endswith("("):
        s = s[:-1].strip()
    s = _repair_sticker_credit_name(s)
    return s


def _repair_sticker_credit_name(name: str) -> str:
    """Fix MSRP table layout artifacts where CREDIT column text merges into names."""
    s = (name or "").strip()
    if not s:
        return s
    fixes = (
        (r"Not O CREDITnal\b", "Not Original"),
        (r"Equi CREDIT(\d+)", r"Missing Standard Equipment \1"),
        (r"CREDITlance\b", "Performance Balance Package"),
        (r": 902 CREDIT\b", ": 902"),
        (r"\sCREDIT\s*$", ""),
        (r"\bCREDIT\b", " "),
    )
    for pat, repl in fixes:
        s = re.sub(pat, repl, s, flags=re.I)
    return re.sub(r"\s+", " ", s).strip(" :")


# Stellantis / CDJR iPacket rows like "UR 019" (upholstery subcode) — not a user-facing option.
_STELLANTIS_FACTORY_CODE_RE = re.compile(r"^[A-Z]{2}\s*\d{2,4}$", re.I)
_NUMERIC_SUBCODE_RE = re.compile(r"^\d{2,4}$")


def _is_sticker_factory_code_noise(name: str, code: str | None = None) -> bool:
    """True when a sticker row is only a factory option/interior code (e.g. UR 019)."""
    n = (name or "").strip()
    c = (code or "").strip()
    if not n and not c:
        return True
    combined = f"{c} {n}".strip()
    compact = re.sub(r"\s+", "", combined)
    if _STELLANTIS_FACTORY_CODE_RE.match(compact):
        return True
    if c and _NUMERIC_SUBCODE_RE.match(n) and re.fullmatch(r"[A-Z]{2,3}", c, re.I):
        return True
    if re.fullmatch(r"[A-Z]{2,3}", n, re.I) and not c:
        return True
    if _NUMERIC_SUBCODE_RE.match(n) and not re.search(r"[A-Za-z]{4,}", n):
        return True
    return False


def format_sticker_option_label(
    name: str,
    *,
    price: int | None = None,
    code: str | None = None,
) -> str:
    if _is_sticker_factory_code_noise(name, code):
        return ""
    bits: list[str] = []
    # Only show factory codes when paired with a real description (not a numeric subcode).
    if code and str(code).strip() and not _NUMERIC_SUBCODE_RE.match((name or "").strip()):
        bits.append(str(code).strip())
    clean = _clean_sticker_option_name(name)
    if clean:
        bits.append(clean)
    label = " ".join(bits).strip()
    if not label:
        return ""
    if price is None:
        return label
    if price < 0:
        return f"{label} — ${abs(price):,} credit"
    return f"{label} — ${price:,}"


def _is_monroney_package_name(name: str) -> bool:
    low = (name or "").strip().lower()
    if not low or low in _OPTION_NAME_NOISE:
        return False
    if re.search(r"\bpackage\b", low):
        return True
    if re.search(r"\bequipment\s+group\b", low):
        return True
    if re.search(r"\b(group|collection|edition)\b", low):
        return True
    if re.search(r"\b\d{1,3}[a-z]\b", low) and "package" in low:
        return True
    return False


def _parse_optional_equipment_block(section: str) -> list[dict[str, Any]]:
    """
    CDJR / Ford Monroney optional-equipment column: trailing prices, indented includes.
    Preserves document order for package grouping downstream.
    """
    if not (section or "").strip():
        return []
    items: list[dict[str, Any]] = []
    seen: set[str] = set()

    def _append(name: str, *, price: int | None = None, code: str | None = None) -> None:
        clean = _clean_sticker_option_name(name)
        if not clean or len(clean) < 3:
            return
        if _is_sticker_factory_code_noise(clean, code):
            return
        low = clean.lower()
        if low in _OPTION_NAME_NOISE:
            return
        key = f"{(code or '').lower()}|{low}|{price}"
        if key in seen:
            return
        seen.add(key)
        items.append(
            {
                "name": clean,
                "price": price,
                "code": (code or "").strip() or None,
                "label": format_sticker_option_label(clean, price=price, code=code),
            }
        )

    for raw_line in section.splitlines():
        if not raw_line.strip():
            continue
        s = raw_line.strip()
        if re.match(r"^optional\s+equipment", s, re.I):
            continue
        if re.match(r"^(description|msrp)\b", s, re.I):
            continue
        m_price = _MONRONEY_TRAILING_PRICE_RE.match(s)
        if m_price:
            name = m_price.group(1).strip()
            try:
                price = int(m_price.group(2).replace(",", ""))
            except ValueError:
                price = None
            code_m = re.match(r"^([A-Z0-9]{2,4})\s+(.+)$", name)
            if code_m:
                _append(code_m.group(2).strip(), price=price, code=code_m.group(1))
            else:
                _append(name, price=price)
            continue
        ind = _MONRONEY_INDENTED_LINE_RE.match(raw_line)
        if ind:
            feat = ind.group(2).strip().lstrip("•-*· ")
            if feat and len(feat) >= 3 and not re.match(r"^optional\s+equipment", feat, re.I):
                _append(feat, price=None)
            continue
        code_m = re.match(r"^([A-Z0-9]{2,4})\s+(.+)$", s)
        if code_m and _is_monroney_package_name(code_m.group(2)):
            _append(code_m.group(2).strip(), price=None, code=code_m.group(1))
            continue
        if _is_monroney_package_name(s):
            _append(s, price=None)
    return items


def parse_sticker_option_items(text: str) -> list[dict[str, Any]]:
    """Structured Monroney / iPacket options with optional prices."""
    if not (text or "").strip():
        return []
    items: list[dict[str, Any]] = []
    seen: set[str] = set()
    by_code: dict[str, int] = {}

    def _push(name: str, *, price: int | None = None, code: str | None = None) -> None:
        clean_name = _clean_sticker_option_name(name)
        if not clean_name or len(clean_name) < 3 or len(clean_name) > 180:
            return
        if _is_sticker_factory_code_noise(clean_name, code):
            return
        low = clean_name.lower()
        if low in _OPTION_NAME_NOISE:
            return
        if re.match(r"^(transmission|engine)\b", low):
            return
        code_key = (code or "").strip().lower()
        if code_key and code_key in by_code:
            existing = items[by_code[code_key]]
            if existing.get("price") is not None and price is None:
                return
            if price is not None:
                existing["name"] = clean_name
                existing["price"] = price
                existing["code"] = code_key.upper() if code_key else None
                existing["label"] = format_sticker_option_label(
                    clean_name, price=price, code=code
                )
                return
        key = f"{code_key}|{low}|{price}"
        if key in seen:
            return
        seen.add(key)
        label = format_sticker_option_label(clean_name, price=price, code=code)
        if len(label) < 4:
            return
        item = {
            "name": clean_name,
            "price": price,
            "code": (code or "").strip() or None,
            "label": label,
        }
        if code_key:
            by_code[code_key] = len(items)
        items.append(item)

    section = text
    sec_m = _OPTIONAL_SECTION_RE.search(text)
    if sec_m:
        section = sec_m.group(1)
        block_items = _parse_optional_equipment_block(section)
        if len(block_items) >= 2:
            return block_items[:80]

    added_sec = _ADDED_OPTIONS_SECTION_RE.search(text)
    added_blob = added_sec.group(1) if added_sec else text

    for row in _parse_ipacket_multiline_added_options(added_blob):
        _push(row["name"], price=row.get("price"), code=row.get("code"))

    for m in _CUST_PACKAGE_RE.finditer(text):
        raw = m.group(1).strip()
        _push(raw, price=_parse_dollar_amount(raw))

    for m in _PACKAGE_LINE_RE.finditer(section):
        raw = m.group(1).strip()
        _push(raw, price=_parse_dollar_amount(raw))

    for m in _IPACKET_PACKAGE_LINE_RE.finditer(text):
        _push(m.group(2).strip(), price=_parse_dollar_amount(m.group(0)), code=m.group(1))

    for m in _IPACKET_PRICED_LINE_RE.finditer(text):
        try:
            price = int(m.group(3).replace(",", ""))
        except ValueError:
            price = None
        _push(m.group(2).strip(), price=price, code=m.group(1))

    for m in _IPACKET_OPTION_LINE_RE.finditer(text):
        tail = m.group(0).split(m.group(2).strip(), 1)[-1]
        price = None if re.search(r"(—|-)\s*$", tail) else _parse_dollar_amount(tail)
        _push(m.group(2).strip(), price=price, code=m.group(1))

    for m in _ADDED_OPTION_PACKAGE_RE.finditer(added_blob):
        raw_name = m.group(2).strip()
        _push(raw_name, price=_parse_dollar_amount(raw_name), code=m.group(1))

    for m in _MONRONEY_PRICED_LINE_RE.finditer(added_blob):
        line = m.group(0)
        _push(m.group(2).strip(), price=_parse_dollar_amount(line), code=m.group(1))

    for line in section.splitlines():
        s = line.strip()
        if not s or len(s) < 8:
            continue
        if re.search(r"\(\$[\d,]+\)", s) and re.search(
            r"package|group|care|sunroof|wheel|towing|navigation|preferred",
            s,
            re.I,
        ):
            _push(s, price=_parse_dollar_amount(s))
        elif s.startswith("•") or s.startswith("-") or s.startswith("*"):
            raw = s.lstrip("•-* ").strip()
            _push(raw, price=_parse_dollar_amount(raw))

    for m in _OPTION_LINE_RE.finditer(section):
        label = m.group(1).strip()
        words = set(label.lower().split())
        if words & _NOISE_WORDS and len(words) <= 2:
            continue
        if 5 <= len(label) <= 90:
            _push(label, price=_parse_dollar_amount(label))

    return items[:80]


def coerce_sticker_option_item(raw: Any) -> dict[str, Any] | None:
    """Normalize legacy string or structured sticker option for UI/API."""
    if isinstance(raw, dict):
        name = _clean_sticker_option_name(str(raw.get("name") or raw.get("label") or ""))
        if not name:
            return None
        price = raw.get("price")
        if price is not None:
            try:
                price = int(price)
            except (TypeError, ValueError):
                price = _parse_dollar_amount(str(raw.get("label") or name))
        else:
            price = _parse_dollar_amount(str(raw.get("label") or name))
        code = str(raw.get("code") or "").strip() or None
        label = str(raw.get("label") or "").strip() or format_sticker_option_label(
            name, price=price, code=code
        )
        return {"name": name, "price": price, "code": code, "label": label}
    s = str(raw or "").strip()
    if not s:
        return None
    name = _clean_sticker_option_name(s)
    price = _parse_dollar_amount(s)
    return {
        "name": name or s,
        "price": price,
        "code": None,
        "label": s if (price is not None and "—" in s) else format_sticker_option_label(name or s, price=price),
    }


_STICKER_DISPLAY_NOISE_RE = re.compile(
    r"\bfactory\s*code\b|\btax\s*code\b|\bg\s*class\s*code\b|\bls1\b"
    r"|\bfactory\s*code\s*-\s*intermediate\b",
    re.I,
)

_STICKER_BOILERPLATE_RE = re.compile(
    r"\b(to:\s*soldto|model\s+year|label\s+is\s+added|federal\s+law|owner'?s\s+manual|"
    r"dealer\s+preparation|including\s+dealer|cannot\s+be\s+removed|based\s+on\s+price\s+of\s+options|"
    r"www\.jeep\.com|for\s+more\s+information\s+visit|standard\s+equipment\s+includes|"
    r"this\s+manual\s+may|select\s+your\s+options|choose\s+options\s+to\s+include|"
    r"vehicle\s+value|soldto|to:\s*ship)\b",
    re.I,
)


def is_sticker_boilerplate_line(text: str) -> bool:
    """True for Monroney legal boilerplate, MSRP table artifacts, and non-option noise."""
    s = (text or "").strip()
    if not s or len(s) < 3:
        return True
    low = s.lower()
    if _STICKER_DISPLAY_NOISE_RE.search(low):
        return True
    if _STICKER_BOILERPLATE_RE.search(low):
        return True
    if low.startswith(("model including", "clean air system exterior", "model year ")):
        return True
    if "exterior features" in low and len(s) > 120:
        return True
    return False


def _is_sticker_display_noise(name: str) -> bool:
    low = (name or "").strip().lower()
    if not low:
        return True
    if _STICKER_DISPLAY_NOISE_RE.search(low):
        return True
    return is_sticker_boilerplate_line(name)


def _filter_sticker_display_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for item in items:
        name = str(item.get("name") or "")
        code = str(item.get("code") or "").strip() or None
        if _is_sticker_display_noise(name):
            continue
        if _is_sticker_factory_code_noise(name, code):
            continue
        out.append(item)
    return out


def _is_sticker_package_header(item: dict[str, Any]) -> bool:
    name = str(item.get("name") or "").strip()
    low = name.lower()
    price = item.get("price")
    if low in {"base", "base price", "msrp"}:
        return False
    if _is_monroney_package_name(name):
        return True
    if price is None or price < 0:
        return False
    if "package" in low or " line" in low or low.endswith(" line"):
        return True
    if re.search(r"\b(group|collection|edition)\b", low):
        return True
    code = str(item.get("code") or "").strip()
    return bool(code and len(code) <= 4 and price > 0 and len(name) > len(code) + 3)


def _package_feature_dedupe_key(name: str, code: str | None = None) -> str:
    """Dedupe key for package header vs included lines (keep factory codes distinct)."""
    n = _repair_sticker_credit_name(name).strip().lower()
    c = str(code or "").strip().lower()
    if c:
        return f"{c}|{re.sub(r'\s+', ' ', n)}"
    return normalize_option_dedupe_key(name)


def _enrich_items_with_include_groups(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Attach indented/unpriced follow-on lines to a priced option (e.g. bucket seat)."""
    filtered = _filter_sticker_display_items(items)
    out: list[dict[str, Any]] = []
    i = 0
    while i < len(filtered):
        item = filtered[i]
        if item.get("price") is not None and not _is_sticker_package_header(item):
            features: list[dict[str, Any]] = []
            j = i + 1
            while j < len(filtered):
                nxt = filtered[j]
                if nxt.get("price") is not None or _is_sticker_package_header(nxt):
                    break
                feat_name = _repair_sticker_credit_name(str(nxt.get("name") or "").strip())
                if feat_name:
                    features.append({"name": feat_name, "code": nxt.get("code")})
                j += 1
            if features:
                enriched = dict(item)
                enriched["features"] = features
                out.append(enriched)
                i = j
                continue
        out.append(item)
        i += 1
    return out


def group_sticker_options_for_display(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Group flat sticker options into priced lines and expandable packages."""
    groups: list[dict[str, Any]] = []
    current_pkg: dict[str, Any] | None = None

    for item in _enrich_items_with_include_groups(items):
        name = _repair_sticker_credit_name(str(item.get("name") or "").strip())
        price = item.get("price")
        preset_features = item.get("features") if isinstance(item.get("features"), list) else None
        if preset_features:
            if current_pkg:
                groups.append(current_pkg)
                current_pkg = None
            groups.append(
                {
                    "kind": "package",
                    "name": name,
                    "price": price,
                    "code": item.get("code"),
                    "features": preset_features,
                }
            )
            continue
        if _is_sticker_package_header(item):
            if current_pkg is not None and price is None:
                feat_name = name
                feat_key = _package_feature_dedupe_key(feat_name, item.get("code"))
                header_key = _package_feature_dedupe_key(
                    str(current_pkg.get("name") or ""), current_pkg.get("code")
                )
                if feat_key and feat_key != header_key:
                    existing = {
                        _package_feature_dedupe_key(str(f.get("name") or ""), f.get("code"))
                        for f in current_pkg["features"]
                    }
                    if feat_key not in existing:
                        current_pkg["features"].append(
                            {"name": feat_name, "code": item.get("code")}
                        )
                continue
            if current_pkg:
                groups.append(current_pkg)
            current_pkg = {
                "kind": "package",
                "name": name,
                "price": price,
                "code": item.get("code"),
                "features": [],
            }
            continue

        if price is not None:
            if current_pkg:
                groups.append(current_pkg)
                current_pkg = None
            groups.append(
                {
                    "kind": "line",
                    "name": name,
                    "price": price,
                    "code": item.get("code"),
                }
            )
            continue

        if current_pkg is not None:
            feat_name = _repair_sticker_credit_name(name)
            feat_key = _package_feature_dedupe_key(feat_name, item.get("code"))
            header_key = _package_feature_dedupe_key(
                str(current_pkg.get("name") or ""), current_pkg.get("code")
            )
            if feat_key and feat_key != header_key:
                existing = {
                    _package_feature_dedupe_key(str(f.get("name") or ""), f.get("code"))
                    for f in current_pkg["features"]
                }
                if feat_key not in existing:
                    current_pkg["features"].append({"name": feat_name, "code": item.get("code")})
        elif not re.match(r"^[A-Z0-9]{2,4}\s", name):
            groups.append(
                {
                    "kind": "line",
                    "name": name,
                    "price": None,
                    "code": item.get("code"),
                }
            )

    if current_pkg:
        groups.append(current_pkg)

    for group in groups:
        if group.get("kind") != "package":
            continue
        if group.get("features"):
            continue
        name = str(group.get("name") or "")
        if re.search(r"\bpackage\b", name, re.I) and re.search(r"\b\d{1,3}[a-z]\b", name, re.I):
            continue
        group["kind"] = "line"
    return groups


def _is_sticker_base_header(group: dict[str, Any]) -> bool:
    name = str(group.get("name") or "").strip().lower()
    return name in {"base", "base price", "msrp"}


def _is_sticker_credit_group(group: dict[str, Any]) -> bool:
    price = group.get("price")
    if isinstance(price, (int, float)) and price < 0:
        return True
    name = str(group.get("name") or "")
    return bool(re.search(r"\bcredit\b", name, re.I))


def _is_sticker_credit_item(item: dict[str, Any]) -> bool:
    return _is_sticker_credit_group(item)


def _sticker_item_display_row(item: dict[str, Any]) -> dict[str, Any]:
    name = _repair_sticker_credit_name(str(item.get("name") or "").strip())
    return {
        "kind": "line",
        "name": name,
        "price": item.get("price"),
        "code": item.get("code"),
    }


def organize_sticker_option_sections(items: list[dict[str, Any]]) -> dict[str, Any]:
    """Split sticker rows into base, priced packages (with features), and flat options."""
    filtered = _filter_sticker_display_items(items)
    base: dict[str, Any] | None = None
    remainder: list[dict[str, Any]] = []
    i = 0
    while i < len(filtered):
        item = filtered[i]
        if _is_sticker_credit_item(item):
            i += 1
            continue
        row_probe = _sticker_item_display_row(item)
        if base is None and _is_sticker_base_header(row_probe):
            features: list[dict[str, Any]] = []
            i += 1
            while i < len(filtered):
                nxt = filtered[i]
                if _is_sticker_credit_item(nxt):
                    i += 1
                    continue
                if nxt.get("price") is not None:
                    break
                feat_name = _repair_sticker_credit_name(str(nxt.get("name") or "").strip())
                if feat_name:
                    features.append({"name": feat_name, "code": nxt.get("code")})
                i += 1
            base = {
                "kind": "base",
                "name": str(row_probe.get("name") or "Base"),
                "price": row_probe.get("price"),
                "features": features,
            }
            continue
        remainder.append(item)
        i += 1

    grouped = group_sticker_options_for_display(remainder)
    packages: list[dict[str, Any]] = []
    options: list[dict[str, Any]] = []
    for g in grouped:
        if g.get("kind") == "package":
            if g.get("features"):
                packages.append(g)
            else:
                options.append({**g, "kind": "line", "features": None})
        else:
            options.append(g)
    return {"base": base, "packages": packages, "options": options}


def sticker_option_sections_for_display(packages: dict[str, Any] | None) -> dict[str, Any]:
    return organize_sticker_option_sections(sticker_options_for_display(packages))


def normalize_option_dedupe_key(name: str) -> str:
    """Normalize option labels for cross-source dedupe."""
    s = _repair_sticker_credit_name(_clean_sticker_option_name(name)).strip()
    m = re.match(r"^([A-Z0-9]{2,4})\s+(.+)$", s)
    if m and re.search(r"\d", m.group(1)):
        s = m.group(2).strip()
    s = re.sub(r"[^a-z0-9]+", " ", s.lower()).strip()
    return re.sub(r"\s+", " ", s).strip()


def option_name_overlaps_sticker(name: str, sticker_keys: set[str]) -> bool:
    key = normalize_option_dedupe_key(name)
    if not key:
        return False
    if key in sticker_keys:
        return True
    for sk in sticker_keys:
        if not sk or len(sk) < 4:
            continue
        if sk in key or key in sk:
            return True
    return False


def build_sticker_option_dedupe_keys(items: list[dict[str, Any]]) -> set[str]:
    keys: set[str] = set()
    for item in items:
        if not isinstance(item, dict):
            continue
        for field in ("name", "label"):
            val = str(item.get(field) or "").strip()
            if val:
                keys.add(normalize_option_dedupe_key(val))
        for feat in item.get("features") or []:
            if isinstance(feat, dict):
                fn = str(feat.get("name") or "").strip()
                if fn:
                    keys.add(normalize_option_dedupe_key(fn))
    return {k for k in keys if k}


def sticker_options_for_display(packages: dict[str, Any] | None) -> list[dict[str, Any]]:
    """Return sticker options with name/price/label for templates and API."""
    if not isinstance(packages, dict):
        return []
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    priced = packages.get("sticker_options_priced")
    if isinstance(priced, list) and priced:
        for raw in priced:
            item = coerce_sticker_option_item(raw)
            if not item:
                continue
            key = normalize_option_dedupe_key(str(item.get("name") or item.get("label") or ""))
            if not key or key in seen:
                continue
            seen.add(key)
            out.append(item)
        return _filter_sticker_display_items(out)[:80]
    opts = packages.get("sticker_options")
    if isinstance(opts, list):
        for raw in opts:
            item = coerce_sticker_option_item(raw)
            if not item:
                continue
            key = normalize_option_dedupe_key(str(item.get("name") or item.get("label") or ""))
            if not key or key in seen:
                continue
            seen.add(key)
            out.append(item)
    return _filter_sticker_display_items(out)[:80]


def sticker_option_groups_for_display(packages: dict[str, Any] | None) -> list[dict[str, Any]]:
    """Return grouped sticker options (lines + packages with nested features)."""
    return group_sticker_options_for_display(sticker_options_for_display(packages))


def _parse_options_from_sticker_text(text: str) -> list[str]:
    return [item["label"] for item in parse_sticker_option_items(text)]


def _parse_msrp_from_sticker_text(text: str) -> int | None:
    m_net = _NET_TOTAL_RE.search(text or "")
    if m_net:
        try:
            val = int(float(m_net.group(1).replace(",", "")))
            if 5000 <= val <= 500000:
                return val
        except ValueError:
            pass
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
_MOTOR_LABEL_RE = re.compile(
    r"(?:^|\n)\s*(?:Motor|MOTOR)\s*:\s*([^\n]{6,140})",
    re.MULTILINE,
)
_TRANS_GEARBOX_RE = re.compile(
    r"(?:^|\n)\s*(?:Trans/Gear Box|TRANS/GEAR BOX)\s*:\s*([^\n]{6,120})",
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
_STELLANTIS_ORIGIN_NOISE_RE = re.compile(
    r"^(?:UNITED\s+STATES|CANADA|MEXICO|ITALY|FRANCE|GERMANY|JAPAN|CHINA|AUSTRIA|HUNGARY)$",
    re.IGNORECASE,
)
_EV_STICKER_RE = re.compile(
    r"Fuel\s+Economy\s+and\s+Environment\s+Electric\s+Vehicle",
    re.IGNORECASE,
)
_MPGE_CITY_HWY_RE = re.compile(
    r"MPGe[\s\n]*(\d+)[\s\n]*city[\s\n]*(\d+)[\s\n]*highway",
    re.IGNORECASE,
)
_BATTERY_KWH_RE = re.compile(
    r"(\d+(?:\.\d+)?)\s*kWh\s+Battery",
    re.IGNORECASE,
)
_ETORQUE_RE = re.compile(r"\be\s*-?\s*torque\b", re.IGNORECASE)
_TWIN_TURBO_RE = re.compile(
    r"\btwin\s*-?\s*turbo(?:charg(?:ed|er))?\b",
    re.IGNORECASE,
)
_SINGLE_TURBO_RE = re.compile(
    r"\b(?:turbo(?:charg(?:ed|er))?|ecoboost)\b",
    re.IGNORECASE,
)
_SUPERCHARGED_RE = re.compile(r"\bsupercharg", re.IGNORECASE)


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


def _is_invalid_sticker_engine_text(value: str | None) -> bool:
    s = _clean_sticker_line(str(value or ""), max_len=160)
    if len(s) < 3:
        return True
    if _STELLANTIS_ORIGIN_NOISE_RE.match(s):
        return True
    if re.search(r"^(?:COUNTRY OF ORIGIN|FINAL ASSEMBLY|WINDSOR|DETROIT)\b", s, re.I):
        return True
    return False


def sticker_engine_display_is_valid(value: str | None) -> bool:
    return not _is_invalid_sticker_engine_text(value)


def _sticker_is_electric_vehicle(text: str) -> bool:
    blob = text or ""
    if _EV_STICKER_RE.search(blob):
        return True
    if _BATTERY_KWH_RE.search(blob):
        return True
    motor = _MOTOR_LABEL_RE.search(blob)
    if motor and re.search(r"electric", motor.group(1), re.I):
        return True
    return False


def _parse_mpge_from_sticker_text(text: str) -> tuple[int | None, int | None]:
    m = _MPGE_CITY_HWY_RE.search(text or "")
    if not m:
        return None, None
    try:
        city = int(m.group(1))
        hwy = int(m.group(2))
    except ValueError:
        return None, None
    if city <= 0 or hwy <= 0:
        return None, None
    return city, hwy


def _parse_transmission_from_sticker_text(text: str) -> str:
    blob = text or ""
    gm = _TRANS_GEARBOX_RE.search(blob)
    if gm:
        val = _clean_sticker_line(gm.group(1), max_len=90)
        if val and not _is_invalid_sticker_engine_text(val):
            return val
    for pat in (_TRANSMISSION_RE,):
        m = pat.search(blob)
        if m:
            val = _clean_sticker_line(m.group(1), max_len=90)
            if val and not _is_invalid_sticker_engine_text(val):
                return val
    ti = _TRANSMISSION_INLINE_RE.search(blob)
    if ti:
        return _clean_sticker_line(ti.group(1), max_len=90)
    return ""


def _sticker_has_etorque(blob: str, engine_raw: str = "") -> bool:
    hay = f"{engine_raw}\n{blob or ''}"[:25000]
    return bool(_ETORQUE_RE.search(hay))


def _display_has_turbo(display: str) -> bool:
    return bool(re.search(r"\b(?:twin\s+turbo|turbo)\b", display or "", re.I))


def _engine_lines_for_turbo(blob: str) -> list[str]:
    out: list[str] = []
    for line in (blob or "").splitlines():
        ln = line.strip()
        if re.search(r"\d+\.\d+\s*L", ln, re.I):
            out.append(ln)
    return out


def _turbo_from_engine_text(text: str) -> str:
    hay = text or ""
    if not hay.strip():
        return ""
    if _TWIN_TURBO_RE.search(hay):
        return "Twin Turbo"
    if _SINGLE_TURBO_RE.search(hay):
        if _SUPERCHARGED_RE.search(hay) and not re.search(
            r"\b(?:turbo|ecoboost)\b", hay, re.I
        ):
            return ""
        return "Turbo"
    return ""


def _sticker_turbo_descriptor(blob: str, engine_raw: str = "") -> str:
    """Return ``Turbo`` or ``Twin Turbo`` when sticker engine text indicates forced induction."""
    priority: list[str] = []
    if (engine_raw or "").strip():
        priority.append(engine_raw)
    for pat in (_ENGINE_LABEL_RE, _MOTOR_LABEL_RE):
        m = pat.search(blob or "")
        if m:
            priority.append(m.group(1))
    for hay in priority:
        hit = _turbo_from_engine_text(hay)
        if hit:
            return hit
    for line in _engine_lines_for_turbo(blob):
        hit = _turbo_from_engine_text(line)
        if hit:
            return hit
    return ""


def _sticker_has_turbo(blob: str, engine_raw: str = "") -> bool:
    return bool(_sticker_turbo_descriptor(blob, engine_raw))


def _apply_turbo_to_engine_out(out: dict[str, Any], blob: str) -> None:
    if str(out.get("fuel_type") or "").strip().lower() == "electric":
        return
    display = str(out.get("engine_display") or "").strip()
    if not display or _display_has_turbo(display):
        return
    engine_raw = str(out.get("engine_raw") or "")
    desc = _sticker_turbo_descriptor(blob, engine_raw)
    if not desc:
        return
    out["engine_display"] = f"{display} {desc}"


def car_has_turbo_signal(car: dict[str, Any]) -> bool:
    """True when a CDJR row or stored packages mention turbo / twin-turbo engine."""
    if not is_cdjr_stellantis_car(car):
        return False
    parts: list[str] = []
    ed = car.get("engine_description")
    if isinstance(ed, str) and ed.strip():
        parts.append(ed)
    raw = car.get("packages")
    if raw:
        parts.append(str(raw))
    hay = "\n".join(parts)
    engine_raw = ed if isinstance(ed, str) else ""
    return _sticker_has_turbo(hay, engine_raw)


def upgrade_engine_display_for_turbo(display: str, car: dict[str, Any] | None = None) -> str:
    """Append ``Turbo`` / ``Twin Turbo`` for CDJR when sticker text indicates it but display omits it."""
    s = (display or "").strip()
    if not s or _display_has_turbo(s):
        return s
    if car is None or not is_cdjr_stellantis_car(car):
        return s
    if str(car.get("fuel_type") or "").strip().lower() == "electric":
        return s
    parts: list[str] = []
    ed = car.get("engine_description")
    if isinstance(ed, str) and ed.strip():
        parts.append(ed)
    raw = car.get("packages")
    if raw:
        parts.append(str(raw))
    hay = "\n".join(parts)
    engine_raw = ed if isinstance(ed, str) else ""
    desc = _sticker_turbo_descriptor(hay, engine_raw)
    if not desc:
        return s
    return f"{s} {desc}"


def _apply_etorque_mild_hybrid(out: dict[str, Any], blob: str) -> None:
    """RAM / Stellantis eTorque (2019+) → ``5.7L V8 Mild Hybrid`` style display."""
    if str(out.get("fuel_type") or "").strip().lower() == "electric":
        return
    engine_raw = str(out.get("engine_raw") or "")
    if not _sticker_has_etorque(blob, engine_raw):
        return
    display = str(out.get("engine_display") or "").strip()
    if not display or re.search(r"mild\s+hybrid", display, re.I):
        out["fuel_type"] = out.get("fuel_type") or "Hybrid"
        return
    liters = out.get("engine_l")
    layout = str(out.get("engine_layout") or "").strip()
    if isinstance(liters, (int, float)) and liters > 0 and layout:
        out["engine_display"] = f"{float(liters):.1f}L {layout} Mild Hybrid"
    elif re.search(r"\d+\.\d+\s*L\s+(?:V\d|I\d|H\d)\b", display, re.I):
        out["engine_display"] = f"{display} Mild Hybrid"
    else:
        out["engine_display"] = f"{display} Mild Hybrid"
    out["fuel_type"] = "Hybrid"


def car_has_etorque_signal(car: dict[str, Any]) -> bool:
    """True when dealer row or stored packages mention Stellantis eTorque."""
    parts: list[str] = []
    ed = car.get("engine_description")
    if isinstance(ed, str) and ed.strip():
        parts.append(ed)
    raw = car.get("packages")
    if raw:
        parts.append(str(raw))
    return _sticker_has_etorque("\n".join(parts))


def upgrade_engine_display_for_etorque(display: str, car: dict[str, Any] | None = None) -> str:
    """Append ``Mild Hybrid`` when eTorque is known but the display line omits it."""
    s = (display or "").strip()
    if not s or re.search(r"mild\s+hybrid", s, re.I):
        return s
    if car is None:
        return s
    if str(car.get("fuel_type") or "").strip().lower() == "electric":
        return s
    if not car_has_etorque_signal(car):
        return s
    return f"{s} Mild Hybrid"


def _parse_engine_from_sticker_text(text: str) -> dict[str, Any]:
    blob = text or ""
    if _sticker_is_electric_vehicle(blob):
        motor = _MOTOR_LABEL_RE.search(blob)
        engine_raw = ""
        if motor:
            engine_raw = _clean_sticker_line(motor.group(1), max_len=120)
        display = engine_raw or "Electric"
        if re.search(r"electric", display, re.I) and len(display) > 48:
            display = "Dual Electric Motors"
        out: dict[str, Any] = {
            "engine_raw": engine_raw or display,
            "engine_l": None,
            "engine_layout": "",
            "engine_display": display,
            "cylinders": 0,
            "fuel_type": "Electric",
        }
        city, hwy = _parse_mpge_from_sticker_text(blob)
        if city is not None:
            out["mpg_city"] = city
        if hwy is not None:
            out["mpg_highway"] = hwy
        batt = _BATTERY_KWH_RE.search(blob)
        if batt:
            out["battery_kwh"] = batt.group(1)
        return out

    engine_raw = ""
    liters: float | None = None
    layout = ""

    motor = _MOTOR_LABEL_RE.search(blob)
    if motor:
        engine_raw = _clean_sticker_line(motor.group(1), max_len=120)

    if not engine_raw or _is_invalid_sticker_engine_text(engine_raw):
        for pat in (_ENGINE_LABEL_RE,):
            m = pat.search(blob)
            if m:
                candidate = _clean_sticker_line(m.group(1), max_len=120)
                if not _is_invalid_sticker_engine_text(candidate):
                    engine_raw = candidate
                    break

    m = _ENGINE_LINE_RE.search(blob)
    if m:
        if not engine_raw or _is_invalid_sticker_engine_text(engine_raw):
            engine_raw = _clean_sticker_line(m.group(0), max_len=120)
        try:
            liters = float(m.group(1))
        except ValueError:
            liters = None
        layout = _normalize_layout_token(m.group(2))
    elif engine_raw and not _is_invalid_sticker_engine_text(engine_raw):
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
    elif engine_raw and not _is_invalid_sticker_engine_text(engine_raw):
        display = engine_raw[:80]

    out = {
        "engine_raw": engine_raw if not _is_invalid_sticker_engine_text(engine_raw) else "",
        "engine_l": liters,
        "engine_layout": layout,
        "engine_display": display,
        "cylinders": _layout_to_cylinders(layout),
    }
    _apply_turbo_to_engine_out(out, blob)
    _apply_etorque_mild_hybrid(out, blob)
    return out


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

    transmission = _parse_transmission_from_sticker_text(blob)
    if transmission:
        out["transmission"] = transmission

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

    items = parse_sticker_option_items(blob)
    out["option_items"] = items
    out["options"] = [x["label"] for x in items]
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


def dodge_charger_daytona_is_bev(car: dict[str, Any]) -> bool:
    """
    2024+ Charger Daytona nameplates are BEVs.
    2026+ plain ``Charger`` (non-Daytona) may be ICE again — do not treat as EV here.
    """
    make = str(car.get("make") or "").strip().lower()
    if make != "dodge":
        return False
    model = str(car.get("model") or "").strip().lower()
    trim = str(car.get("trim") or "").strip().lower()
    if "daytona" not in model and "daytona" not in trim:
        return False
    try:
        year = int(car.get("year") or 0)
    except (TypeError, ValueError):
        year = 0
    return year >= 2024


def known_oem_engine_from_car(car: dict[str, Any]) -> dict[str, Any]:
    """
    Trim/title heuristics when Monroney text is unavailable.
    Jeep Wrangler / Gladiator Rubicon 392 = 6.4L V8 HEMI.
    Dodge Charger Daytona (2024+) = BEV.
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
    if dodge_charger_daytona_is_bev(car):
        return {
            "engine_display": "Dual Electric Motors",
            "engine_l": None,
            "cylinders": 0,
            "engine_raw": "Electric Drive",
            "fuel_type": "Electric",
            "sticker_specs": {"Engine": "Dual Electric Motors"},
        }
    return {}


_CDJR_MAKES = frozenset({"jeep", "chrysler", "dodge", "ram"})

_IPACKET_STICKER_URL_RE = re.compile(
    r"https?://(?:djapi\.)?autoipacket\.com/v2/sticker-puller/download/[A-HJ-NPR-Z0-9]{11,17}(?:\?[^\s\"'<>]+)?",
    re.IGNORECASE,
)
_LISTING_STICKER_PATH_TOKENS = (
    "sticker-puller",
    "autoipacket.com",
    "ipacket.com",
    "monroney",
    "window-sticker",
    "window_sticker",
    "/sticker/",
    "msrp-options",
    "msrp_options",
)
_IPACKET_OPTION_LINE_RE = re.compile(
    r"^([A-Z0-9]{2,4})\s+(.+?)\s+(?:\$[\-]?[\d,]+\.\d{2}|—|-)\s*$",
    re.MULTILINE,
)
_IPACKET_PACKAGE_LINE_RE = re.compile(
    r"^([A-Z0-9]{2,4})\s+(.+?\bPackage\b[^$\n]{0,80})(?:\(\$[\d,]+\))?\s*:?\s*$",
    re.IGNORECASE | re.MULTILINE,
)
_LINKHREF_RE = re.compile(r'"linkHref"\s*:\s*"(https?://[^"]+)"', re.IGNORECASE)
_MQV_IFRAME_URL_RE = re.compile(
    r'"mqv_iframe_url"\s*:\s*"(https?://[^"]+)"',
    re.IGNORECASE,
)
_IPACKET_ACCOUNT_ID_RE = re.compile(
    r'(?:storeAccountId|accountId|mqv_api_key|siteId)["\']\s*:\s*["\']?(\d{2,12})',
    re.IGNORECASE,
)
_IPACKET_DOCUMENT_VIEWER_RE = re.compile(
    r"https?://document-viewer\.autoipacket\.com/sticker/([A-HJ-NPR-Z0-9]{17})(?:\?[^\s\"'<>]+)?",
    re.IGNORECASE,
)
_IPACKET_WEBSITE_PLUGIN_RE = re.compile(
    r"https?://djapi\.autoipacket\.com/v1/vdp/website-plugin\?[^\s\"'<>]+",
    re.IGNORECASE,
)
_ADDED_OPTION_PACKAGE_RE = re.compile(
    r"^([A-Z0-9]{2,4})\s+(.+?\(\$[\d,]+\)):?\s*(?:Includes\s+)?(.*)$",
    re.MULTILINE | re.IGNORECASE,
)
_ADDED_OPTIONS_SECTION_RE = re.compile(
    r"Added Options(.*?)(?:Standard Options|TOTAL SUGGESTED PRICE|Net Total|$)",
    re.IGNORECASE | re.DOTALL,
)
_MONRONEY_PRICED_LINE_RE = re.compile(
    r"^\s*([A-Z0-9]{2,4})\s+(.+?)\s+\$[\d,]+(?:\.\d{2})?\s*$",
    re.MULTILINE,
)
_NET_TOTAL_RE = re.compile(
    r"(?:net\s+total|total\s+suggested\s+price)[^\d$]*\$?\s*([\d,]+(?:\.\d{2})?)",
    re.IGNORECASE,
)
_IPACKET_CODE_LINE_RE = re.compile(r"^([A-Z0-9]{2,4})\s+(.+)$")


def _parse_ipacket_multiline_added_options(added_blob: str) -> list[dict[str, Any]]:
    """
    Mercedes / iPacket MSRP tables often put the price on the line after a wrapped description.
    """
    if not (added_blob or "").strip():
        return []
    lines = [ln.rstrip() for ln in added_blob.splitlines()]
    out: list[dict[str, Any]] = []
    i = 0
    while i < len(lines):
        raw = lines[i].strip()
        m = _IPACKET_CODE_LINE_RE.match(raw)
        if not m:
            i += 1
            continue
        code = m.group(1).strip()
        name_parts = [m.group(2).strip()]
        j = i + 1
        while j < len(lines):
            nxt = lines[j].strip()
            if not nxt:
                j += 1
                continue
            if nxt.startswith("$") or nxt in ("—", "-"):
                break
            if re.match(r"^credit$", nxt, re.I):
                j += 1
                break
            if _IPACKET_CODE_LINE_RE.match(nxt):
                break
            if nxt.lower() in ("description", "msrp", "base"):
                break
            name_parts.append(nxt)
            j += 1
        price: int | None = None
        if j < len(lines):
            nxt = lines[j].strip()
            if nxt.startswith("$"):
                neg = nxt.startswith("$-")
                pm = re.match(r"^\$[\-]?([\d,]+(?:\.\d{2})?)", nxt)
                if pm:
                    try:
                        price = int(float(pm.group(1).replace(",", "")))
                        if neg:
                            price = -price
                    except ValueError:
                        price = None
                j += 1
            elif nxt in ("—", "-"):
                j += 1
        full_name = " ".join(part for part in name_parts if part).strip()
        if full_name and full_name.lower() not in {"description", "msrp", "base"}:
            if not _is_sticker_factory_code_noise(full_name, code):
                out.append({"name": full_name, "price": price, "code": code})
        i = j if j > i else i + 1
    return out


def _vin_token_in_url(url: str) -> str | None:
    m = re.search(
        r"/(?:download|sticker|monroney)/([A-HJ-NPR-Z0-9]{11,17})(?:[/?]|$)",
        url,
        re.IGNORECASE,
    )
    if m:
        return m.group(1).upper()
    m2 = re.search(r"([A-HJ-NPR-Z0-9]{17})", url.upper())
    return m2.group(1) if m2 else None


def is_listing_sticker_url(url: str) -> bool:
    """True when a dealer-supplied URL likely points at a Monroney / MSRP options document."""
    ul = (url or "").strip().lower()
    if not ul.startswith(("http://", "https://")):
        return False
    if _IPACKET_STICKER_URL_RE.search(url or ""):
        return True
    if "autoipacket.com" in ul or "ipacket.us" in ul:
        return True
    if any(token in ul for token in _LISTING_STICKER_PATH_TOKENS):
        return True
    if re.search(r"\.(jpe?g|png|webp)(\?|#|$)", ul) and any(
        token in ul for token in ("monroney", "window-sticker", "window_sticker", "/sticker/", "msrp")
    ):
        if any(h in ul for h in ("dealer", "dealerinspire", "homenet", "inventory", "cdn.", "pictures.")):
            return True
    if ul.endswith(".pdf") and any(token in ul for token in ("sticker", "monroney", "msrp", "label")):
        if any(h in ul for h in ("dealer", "dealerinspire", "homenet", "inventory", "cdn.", "pictures.")):
            return True
    return False


def _ipacket_token_from_url(url: str) -> str | None:
    m = re.search(r"[?&]token=([^&\"'<>]+)", url or "", re.IGNORECASE)
    return m.group(1).strip() if m else None


def _ipacket_sticker_puller_download_url(vin: str, token: str) -> str | None:
    vnorm = re.sub(r"\s+", "", (vin or "").strip().upper())
    tok = (token or "").strip()
    if len(vnorm) != 17 or not tok:
        return None
    return f"https://djapi.autoipacket.com/v2/sticker-puller/download/{vnorm}?token={tok}"


def _ipacket_sticker_urls_from_website_plugin(vin: str) -> list[str]:
    """
    Resolve iPacket MSRP / window-sticker download URLs via the public VDP plugin API.
    Returns sticker-puller download links (with JWT) when the dealer uses iPacket.
    """
    vnorm = re.sub(r"\s+", "", (vin or "").strip().upper())
    if len(vnorm) != 17:
        return []
    try:
        import requests

        r = requests.get(
            "https://djapi.autoipacket.com/v1/vdp/website-plugin",
            params={"vin": vnorm, "enhanced_webicon": "true"},
            timeout=15,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
                ),
                "Accept": "application/json,*/*",
            },
        )
        if r.status_code == 404:
            return []
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        logger.debug("iPacket website-plugin lookup failed vin=%s: %s", vnorm, e)
        return []
    if not isinstance(data, dict):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for mod in data.get("modules") or []:
        if not isinstance(mod, dict):
            continue
        label = str(mod.get("label") or mod.get("description") or "").lower()
        mod_url = str(mod.get("url") or "").strip()
        if not mod_url.startswith(("http://", "https://")):
            continue
        is_msrp = any(
            tok in label
            for tok in ("msrp", "options info", "window sticker", "monroney", "original msrp")
        )
        is_sticker = "document-viewer.autoipacket.com/sticker/" in mod_url.lower()
        is_puller = "sticker-puller/download" in mod_url.lower()
        if not (is_msrp or is_sticker or is_puller):
            continue
        candidate = mod_url
        if is_sticker or is_puller:
            tok = _ipacket_token_from_url(mod_url)
            puller = _ipacket_sticker_puller_download_url(vnorm, tok or "") if tok else None
            if puller:
                candidate = puller
        if candidate not in seen and is_listing_sticker_url(candidate):
            seen.add(candidate)
            out.append(candidate)
    return out


def resolve_listing_sticker_fetch_url(url: str, vin: str) -> str:
    """
    Normalize dealer sticker URLs to a fetchable document endpoint.
    iPacket document-viewer pages are HTML shells; prefer sticker-puller download.
    """
    u = (url or "").strip()
    vnorm = re.sub(r"\s+", "", (vin or "").strip().upper())
    if not u or len(vnorm) != 17:
        return u
    ul = u.lower()
    if "sticker-puller/download" in ul:
        return u
    if "document-viewer.autoipacket.com/sticker/" in ul:
        tok = _ipacket_token_from_url(u)
        puller = _ipacket_sticker_puller_download_url(vnorm, tok or "") if tok else None
        if puller:
            return puller
    if _IPACKET_WEBSITE_PLUGIN_RE.match(u):
        for resolved in _ipacket_sticker_urls_from_website_plugin(vnorm):
            return resolved
    return u


def extract_listing_sticker_urls_from_html(
    html: str,
    *,
    vin: str | None = None,
    car: dict[str, Any] | None = None,
) -> list[str]:
    """Find iPacket, dealer Monroney PDF/image URLs embedded in listing HTML."""
    if not (html or "").strip():
        return []
    out: list[str] = []
    seen: set[str] = set()
    vnorm = re.sub(r"\s+", "", (vin or "").strip().upper())

    def _add(raw: str) -> None:
        u = (raw or "").strip().rstrip(".,;)")
        if not u or u in seen:
            return
        if not is_listing_sticker_url(u):
            return
        url_vin = _vin_token_in_url(u)
        if vnorm and url_vin and url_vin != vnorm:
            return
        seen.add(u)
        out.append(u)

    for m in _IPACKET_STICKER_URL_RE.finditer(html):
        _add(m.group(0))
    for m in _LINKHREF_RE.finditer(html):
        _add(m.group(1))
    for m in _MQV_IFRAME_URL_RE.finditer(html):
        _add(m.group(1))
    for m in re.finditer(
        r"https?://[^\s\"'<>]+(?:monroney|window[-_]?sticker|/sticker/|sticker-puller)[^\s\"'<>]*",
        html,
        re.IGNORECASE,
    ):
        _add(m.group(0))
    for m in re.finditer(
        r'"(?:stickerUrl|sticker_url|documentUrl|msrpUrl|monroneyUrl)"\s*:\s*"(https?://[^"]+)"',
        html,
        re.IGNORECASE,
    ):
        _add(m.group(1))

    if vnorm:
        account_ids: list[str] = []
        for m in _IPACKET_ACCOUNT_ID_RE.finditer(html):
            account_ids.append(m.group(1))
        if "autoipacket" in html.lower() or "ipacket" in html.lower():
            for acct in dict.fromkeys(account_ids):
                api_url = _ipacket_dlrcom_sticker_url(vnorm, acct)
                if api_url:
                    _add(api_url)
            if not any("sticker-puller/download" in u.lower() for u in out):
                from backend.scanner.dealer_sticker_provider import should_try_ipacket_website_plugin

                probe_car = dict(car or {})
                if vnorm and not probe_car.get("vin"):
                    probe_car["vin"] = vnorm
                if should_try_ipacket_website_plugin(probe_car, html):
                    for plugin_url in _ipacket_sticker_urls_from_website_plugin(vnorm):
                        _add(plugin_url)
    return out[:12]


def _ipacket_dlrcom_sticker_url(vin: str, store_account_id: str) -> str | None:
    """Resolve iPacket Vehicle Records link for a VIN via the dealer web-button API."""
    vnorm = re.sub(r"\s+", "", (vin or "").strip().upper())
    acct = re.sub(r"\D", "", str(store_account_id or ""))
    if not vnorm or len(vnorm) != 17 or not acct:
        return None
    try:
        import requests

        r = requests.post(
            "https://webicon.autoipacket.com/dlrcom",
            json={"vin": vnorm, "storeAccountId": acct, "dlrInfo": {}},
            timeout=15,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
                ),
                "Accept": "application/json,*/*",
                "Content-Type": "application/json",
            },
        )
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        logger.debug("iPacket dlrcom lookup failed vin=%s acct=%s: %s", vnorm, acct, e)
        return None
    if not isinstance(data, dict):
        return None
    for key in ("linkHref", "link_href", "iframeUrl", "iframe_url"):
        href = str(data.get(key) or "").strip()
        if href.startswith(("http://", "https://")):
            return href
    return None


def listing_sticker_url_candidates(car: dict[str, Any]) -> list[str]:
    """Ordered sticker document URLs to try for this listing (best first)."""
    vnorm = re.sub(r"\s+", "", str(car.get("vin") or "").strip().upper())
    urls = car_listing_sticker_urls(car)
    if not urls:
        return []
    scored: list[tuple[tuple[int, int], str]] = []
    for u in urls:
        if not isinstance(u, str) or not u.strip().startswith("http"):
            continue
        ul = u.lower()
        sc = 0
        if "sticker-puller/download" in ul:
            sc += 10
        if "token=" in ul:
            sc += 6
        if vnorm and vnorm in u.upper():
            sc += 4
        if ul.endswith(".pdf"):
            sc += 3
        if "monroney" in ul or "window-sticker" in ul:
            sc += 2
        scored.append(((sc, len(u)), u))
    scored.sort(key=lambda x: x[0], reverse=True)
    out: list[str] = []
    seen: set[str] = set()
    for _, u in scored:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def pick_best_listing_sticker_url(urls: list[str], *, vin: str | None = None) -> str | None:
    """Prefer iPacket sticker-puller download links with JWT tokens."""
    if not urls:
        return None
    vnorm = re.sub(r"\s+", "", (vin or "").strip().upper())

    def score(u: str) -> tuple[int, int]:
        ul = u.lower()
        sc = 0
        if "sticker-puller/download" in ul:
            sc += 10
        if "token=" in ul:
            sc += 6
        if vnorm and vnorm in u.upper():
            sc += 4
        if ul.endswith(".pdf"):
            sc += 3
        if "monroney" in ul or "window-sticker" in ul:
            sc += 2
        return (sc, len(u))

    good = [u for u in urls if isinstance(u, str) and u.strip().startswith("http")]
    if not good:
        return None
    good.sort(key=score, reverse=True)
    return good[0]


def fetch_listing_sticker(
    url: str,
    vin: str,
    *,
    timeout: float = 20.0,
) -> dict[str, Any] | None:
    """
    Fetch a dealer listing sticker (iPacket MSRP doc, Monroney PDF/image, etc.).
    URL must pass ``is_listing_sticker_url``; embedded VIN must match when present.
    """
    u = resolve_listing_sticker_fetch_url((url or "").strip(), vin)
    if not u or not is_listing_sticker_url(u):
        logger.warning("Listing sticker fetch blocked: URL not allowed (vin=%s)", vin)
        return None
    vnorm = re.sub(r"\s+", "", (vin or "").strip().upper())
    url_vin = _vin_token_in_url(u)
    if url_vin and vnorm and url_vin != vnorm:
        logger.warning(
            "Listing sticker fetch blocked: URL VIN mismatch (vin=%s url_vin=%s)",
            vnorm,
            url_vin,
        )
        return None

    import requests

    r = requests.get(
        u,
        timeout=timeout,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept": "application/pdf,text/plain,text/html,*/*;q=0.8",
        },
        allow_redirects=True,
    )
    r.raise_for_status()
    ct = (r.headers.get("content-type") or "").lower()
    content = r.content
    raw_text = ""
    if content[:4] == b"%PDF" or "pdf" in ct or u.lower().endswith(".pdf"):
        raw_text = _extract_pdf_text(content)
    elif content and not content.startswith(b"%PDF"):
        try:
            raw_text = content.decode("utf-8", errors="replace")
        except Exception:
            raw_text = r.text or ""
    if not raw_text and r.text:
        raw_text = r.text[:28000]

    nested_urls = extract_listing_sticker_urls_from_html(raw_text, vin=vnorm)
    nested_urls = [x for x in nested_urls if x != u]
    if nested_urls and not _parse_options_from_sticker_text(raw_text):
        try:
            deeper = fetch_listing_sticker(nested_urls[0], vnorm, timeout=timeout)
            if deeper and _is_valid_listing_sticker_payload(deeper):
                return deeper
        except Exception as e:
            logger.debug("Nested listing sticker fetch failed: %s", e)

    return {
        "url": u,
        "tier": "listing_sticker",
        "oem_family": "listing",
        "content": content,
        "content_type": ct or ("application/pdf" if content[:4] == b"%PDF" else "text/plain"),
        "raw_text": raw_text[:28000],
        "options": _parse_options_from_sticker_text(raw_text),
        "msrp": _parse_msrp_from_sticker_text(raw_text),
    }


def _is_valid_listing_sticker_payload(result: dict[str, Any]) -> bool:
    content = result.get("content") or b""
    raw_text = str(result.get("raw_text") or "")
    ct = str(result.get("content_type") or "").lower()
    if len(content) >= 500 and content[:4] == b"%PDF":
        return True
    if ("image/" in ct or re.search(r"\.(jpe?g|png|webp)(\?|$)", str(result.get("url") or ""), re.I)) and len(content) >= 20000:
        return True
    if len(raw_text) >= 200 and re.search(
        r"msrp|net\s+total|optional|package|base\s+price|sticker|monroney|added options",
        raw_text,
        re.I,
    ):
        return bool(result.get("options") or _parse_msrp_from_sticker_text(raw_text))
    return False


def is_sticker_media_url(url: str) -> bool:
    """True when a gallery or listing URL likely points at a Monroney / window sticker."""
    if is_listing_sticker_url(url):
        return True
    ul = (url or "").strip().lower()
    if not ul.startswith(("http://", "https://")):
        return False
    return any(token in ul for token in ("sticker", "monroney", "label"))


def listing_sticker_direct_image_url(url: str | None) -> str | None:
    """Return *url* only when it is safe to embed in ``<img>`` (JPEG/PNG/WebP), not HTML landing pages."""
    u = (url or "").strip()
    if not u.startswith(("http://", "https://")):
        return None
    if re.search(r"\.(jpe?g|png|webp)(\?|#|$)", u, re.I):
        return u
    return None


def sticker_embed_preview_url(
    car: dict[str, Any],
    *,
    preview_api_url: str,
    has_paid_access: bool,
) -> str | None:
    """In-page sticker preview: local PDF render API, or a direct listing image URL."""
    if not has_paid_access:
        return None
    from backend.enrichment.window_sticker_service import window_sticker_has_visual

    if window_sticker_has_visual(car):
        return preview_api_url
    for u in car_listing_sticker_urls(car):
        direct = listing_sticker_direct_image_url(u)
        if direct:
            return direct
    return None


def iter_car_image_urls(car: dict[str, Any]) -> list[str]:
    """Hero + gallery URLs in stable order."""
    urls: list[str] = []
    main = car.get("image_url")
    if isinstance(main, str) and main.strip().startswith("http"):
        urls.append(main.strip())
    g = car.get("gallery")
    if isinstance(g, list):
        for u in g:
            if isinstance(u, str) and u.strip().startswith("http") and u.strip() not in urls:
                urls.append(u.strip())
    elif isinstance(g, str) and g.strip():
        try:
            import json

            arr = json.loads(g)
        except (TypeError, ValueError, json.JSONDecodeError):
            arr = None
        if isinstance(arr, list):
            for u in arr:
                if isinstance(u, str) and u.strip().startswith("http") and u.strip() not in urls:
                    urls.append(u.strip())
    return urls


def is_cdjr_stellantis_car(car: dict[str, Any]) -> bool:
    """Ram / Jeep / Dodge / Chrysler (Stellantis WMI or make)."""
    vin = str(car.get("vin") or "")
    fam = window_sticker_oem_family(vin)
    if fam and fam.startswith("stellantis"):
        return True
    make = str(car.get("make") or "").strip().lower()
    return make in _CDJR_MAKES


def car_listing_sticker_urls(car: dict[str, Any]) -> list[str]:
    """Sticker URLs supplied on the dealer listing (field or gallery image)."""
    out: list[str] = []
    ws = str(car.get("window_sticker_url") or "").strip()
    if ws.startswith(("http://", "https://")) and is_listing_sticker_url(ws):
        out.append(ws)
    for u in iter_car_image_urls(car):
        if is_sticker_media_url(u) and u not in out:
            out.append(u)
    return out


def car_has_listing_sticker_signal(car: dict[str, Any]) -> bool:
    return bool(car_listing_sticker_urls(car))


def _car_has_sticker_options_in_packages(car: dict[str, Any]) -> bool:
    raw = car.get("packages")
    if not raw or not str(raw).strip():
        return False
    try:
        import json

        pj = json.loads(raw) if isinstance(raw, str) else raw
    except (TypeError, ValueError, json.JSONDecodeError):
        return False
    if not isinstance(pj, dict):
        return False
    opts = pj.get("sticker_options")
    return isinstance(opts, list) and len(opts) > 0


def car_listing_may_have_sticker(car: dict[str, Any]) -> bool:
    """True when the dealer listing likely exposes a downloadable window sticker."""
    from backend.scanner.dealer_sticker_provider import car_listing_sticker_fetch_eligible

    return car_listing_sticker_fetch_eligible(car)


def _sticker_has_meaningful_parsed_content(car: dict[str, Any]) -> bool:
    """True when packages_json includes real Monroney rows (not vision-only placeholders)."""
    import json

    raw = car.get("packages_json") or car.get("packages")
    if not raw:
        return False
    try:
        data = json.loads(raw) if isinstance(raw, str) else raw
    except (TypeError, ValueError, json.JSONDecodeError):
        return False
    if not isinstance(data, dict):
        return False
    for key in ("sticker_options_priced", "sticker_options", "sticker_option_sections"):
        block = data.get(key)
        if isinstance(block, list) and block:
            return True
        if isinstance(block, dict) and block:
            return True
    lines = data.get("sticker_spec_lines") or data.get("spec_lines")
    if isinstance(lines, list) and len(lines) >= 2:
        return True
    return False


def show_window_sticker_panel(car: dict[str, Any], ctx: dict[str, Any] | None = None) -> bool:
    """
    Show Packages & window sticker only when we have (or can display) real sticker data.
    """
    from backend.enrichment.window_sticker_service import window_sticker_has_visual

    ctx = ctx or {}
    if window_sticker_has_visual(car):
        return True
    if _sticker_has_meaningful_parsed_content(car):
        return True
    if ctx.get("listing_sticker_option_sections") or ctx.get("listing_sticker_options"):
        opts = ctx.get("listing_sticker_options") or []
        if opts and not all(
            "unreadable" in str(o).lower() or "visible on windshield" in str(o).lower()
            for o in opts
        ):
            return True
    ws_url = (car.get("window_sticker_url") or "").strip()
    if ws_url and listing_sticker_direct_image_url(ws_url):
        return True
    urls = car_listing_sticker_urls(car)
    if any(listing_sticker_direct_image_url(u) for u in urls):
        return True
    return False


def show_window_sticker_ui(car: dict[str, Any]) -> bool:
    """Deprecated alias — use ``show_window_sticker_panel`` with context when available."""
    return show_window_sticker_panel(car)


def should_auto_fetch_oem_window_sticker(car: dict[str, Any]) -> bool:
    """Post-scan OEM PDF fetch is limited to Stellantis / CDJR."""
    return is_cdjr_stellantis_car(car)


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
