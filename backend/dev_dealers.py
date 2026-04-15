"""Read/write project-root dealers.json (scanner manifest)."""
from __future__ import annotations

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEALERS_PATH = ROOT / "dealers.json"
PROVIDERS = frozenset({"dealer_dot_com", "dealer_on"})
DEALER_ID_RE = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")


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
