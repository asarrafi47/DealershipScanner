"""Read/write project-root dealers.json (scanner manifest)."""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parent.parent
DEALERS_PATH = ROOT / "dealers.json"
PROVIDERS = frozenset({"dealer_dot_com", "dealer_on"})
DEALER_ID_RE = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")


def slug_from_url(url_str: str) -> str:
    """
    Canonical dealer_id slug from a site URL — must match ``scanner.js`` ``slugFromUrl``:
    hostname without ``www.``, non-alphanumerics → hyphens, trim, lower, fallback ``dealer``.
    """
    s = (url_str or "").strip()
    if not s:
        return "dealer"
    if not re.match(r"https?://", s, re.I):
        s = "https://" + s.lstrip("/")
    try:
        host = (urlparse(s).hostname or "").strip()
    except ValueError:
        return "dealer"
    if not host:
        return "dealer"
    host = re.sub(r"^www\.", "", host, flags=re.I)
    host = re.sub(r"[^a-z0-9]+", "-", host, flags=re.I)
    host = host.strip("-").lower()
    return host or "dealer"


def normalize_manifest_url(url: str) -> str:
    """HTTPS absolute URL, no trailing slash (manifest + dedupe key)."""
    u = str(url or "").strip().rstrip("/")
    if not u:
        return ""
    if not re.match(r"https?://", u, re.I):
        u = "https://" + u.lstrip("/")
    if u.lower().startswith("http://"):
        u = "https://" + u[7:]
    return u.rstrip("/")


def upsert_dealer_manifest_row(
    *,
    name: str,
    website_url: str,
    provider: str = "dealer_dot_com",
    dealer_id: str | None = None,
) -> tuple[str, str]:
    """
    Insert or update one row in ``dealers.json`` (project root).

    Dedupes by ``dealer_id`` or by normalized ``url`` (case-insensitive).

    Default ``provider`` matches ``scanner.js`` smart-import (``dealer_dot_com``). Some stacks
    (e.g. Keffer/CDJR) may need ``dealer_on`` once detected — pass explicitly when known.

    Returns ``("inserted"|"updated", dealer_id)``. Raises ``ValueError`` like ``validate_dealers``.

    Manual checklist: smart-import success → ``dealers.json`` contains row →
    ``python scanner.py --dealer-id <slug>`` picks it up.
    """
    if provider not in PROVIDERS:
        provider = "dealer_dot_com"
    url = normalize_manifest_url(website_url)
    if not url:
        raise ValueError("website_url is required for manifest upsert")
    did = (dealer_id or "").strip() or slug_from_url(url)
    if not DEALER_ID_RE.match(did):
        raise ValueError(f"dealer_id slug invalid after normalization: {did!r}")

    row: dict = {"name": (name or "").strip(), "url": url, "provider": provider, "dealer_id": did}
    if not row["name"]:
        raise ValueError("name is required for manifest upsert")

    if not DEALERS_PATH.is_file():
        DEALERS_PATH.parent.mkdir(parents=True, exist_ok=True)
        DEALERS_PATH.write_text("[]\n", encoding="utf-8")

    rows = load_dealers()
    key_url = url.lower()
    idx: int | None = None
    for i, r in enumerate(rows):
        rid = str(r.get("dealer_id") or "").strip()
        ru = normalize_manifest_url(str(r.get("url") or "")).lower()
        if rid == did or ru == key_url:
            idx = i
            break

    action = "updated" if idx is not None else "inserted"
    if idx is not None:
        prev = rows[idx]
        merged = dict(row)
        b = prev.get("brand")
        if b and str(b).strip():
            merged["brand"] = str(b).strip()
        rows[idx] = merged
    else:
        rows.append(row)

    save_dealers(rows)
    return action, did


def smart_import_scrape_succeeded(exit_code: int | None, log_text: str) -> bool:
    """
    True when ``scanner.js`` exited 0 and the log shows inventory was persisted
    (``SCAN_VEHICLE_COUNT:N`` with N>0 and/or ``Upserted N unique vehicles`` with N>0).
    """
    if exit_code != 0:
        return False
    lt = log_text or ""
    last_scan = 0
    for m in re.finditer(r"SCAN_VEHICLE_COUNT:(\d+)", lt):
        last_scan = max(last_scan, int(m.group(1)))
    if last_scan > 0:
        return True
    m = re.search(r"Upserted\s+(\d+)\s+unique\s+vehicles", lt, re.I)
    return bool(m and int(m.group(1)) > 0)


def smart_import_manifest_display_name(
    job_url: str,
    *,
    resolved: dict[str, Any] | None,
    error_partial: dict[str, Any],
    discovery: list[dict[str, Any]],
) -> str:
    """
    Dealer display name for ``dealers.json`` when full ``DealerCreate`` / registry insert is skipped.
    Prefer SMART_IMPORT_RESULT / partial ``name``, then DISCOVERY ``found_name``, then title-case slug.
    """
    if isinstance(resolved, dict):
        n = str(resolved.get("name") or "").strip()
        if n:
            return n
    n = str((error_partial or {}).get("name") or "").strip()
    if n:
        return n
    for ev in reversed(discovery or []):
        if not isinstance(ev, dict):
            continue
        fn = ev.get("name")
        if isinstance(fn, str) and fn.strip():
            return fn.strip()
        msg = str(ev.get("message") or "")
        if "Found name:" in msg:
            rest = msg.split("Found name:", 1)[-1].strip()
            if rest:
                return rest
    slug = slug_from_url(job_url)
    parts = [p.capitalize() for p in slug.split("-") if p]
    return " ".join(parts) if parts else (slug or "Dealer")


def dealers_file_path() -> Path:
    return DEALERS_PATH


def load_dealers() -> list[dict]:
    if not DEALERS_PATH.is_file():
        return []
    with open(DEALERS_PATH, encoding="utf-8") as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError(f"dealers.json is not valid JSON: {e}") from e
    if not isinstance(data, list):
        raise ValueError("dealers.json must be a JSON array")
    return [x for x in data if isinstance(x, dict)]


def normalize_dealer(row: dict) -> dict:
    name = str(row.get("name") or "").strip()
    url = str(row.get("url") or "").strip().rstrip("/")
    provider = str(row.get("provider") or "dealer_dot_com").strip()
    dealer_id = str(row.get("dealer_id") or "").strip()
    brand_raw = row.get("brand")
    brand_s = str(brand_raw).strip() if brand_raw is not None else ""
    out: dict = {
        "name": name,
        "url": url,
        "provider": provider,
        "dealer_id": dealer_id,
    }
    if brand_s:
        out["brand"] = brand_s
    return out


def validate_dealers(rows: list) -> list[dict]:
    if not isinstance(rows, list):
        raise ValueError("Expected a JSON array of dealership objects")
    normalized: list[dict] = []
    for i, row in enumerate(rows):
        if not isinstance(row, dict):
            raise ValueError(f"Row {i + 1} must be an object")
        n = normalize_dealer(row)
        if not n["name"]:
            raise ValueError(f"Row {i + 1}: name is required")
        if not n["url"]:
            raise ValueError(f"Row {i + 1}: url is required")
        if n["provider"] not in PROVIDERS:
            raise ValueError(
                f"Row {i + 1}: provider must be one of {', '.join(sorted(PROVIDERS))}"
            )
        if not n["dealer_id"]:
            raise ValueError(f"Row {i + 1}: dealer_id is required")
        if not DEALER_ID_RE.match(n["dealer_id"]):
            raise ValueError(
                f"Row {i + 1}: dealer_id must be a lowercase slug (e.g. long-beach-bmw)"
            )
        normalized.append(n)
    return normalized


def save_dealers(rows: list[dict]) -> None:
    validated = validate_dealers(rows)
    tmp = DEALERS_PATH.with_suffix(".json.tmp")
    text = json.dumps(validated, indent=2, ensure_ascii=False) + "\n"
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(DEALERS_PATH)
