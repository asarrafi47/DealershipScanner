"""
Dealer manifest loading and filtering (dealers.json or registry DB).
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import sys
from typing import Any

from backend.scanner.constants import MANIFEST_PATH, OEM_MANUFACTURER_DOMAINS

logger = logging.getLogger(__name__)


def is_oem_manufacturer_url(url: str) -> bool:
    if not url:
        return False
    url_lower = url.lower()
    return any(domain in url_lower for domain in OEM_MANUFACTURER_DOMAINS)


def _load_dealers_from_db() -> list[dict]:
    from backend.db.dealerships_db import ensure_dealerships_table, get_conn

    conn = get_conn()
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    ensure_dealerships_table(cur)
    cur.execute(
        """
        SELECT id, name, dealer_website_url, website_url, city, state, zip_code
        FROM dealerships
        WHERE is_active = 1 AND duplicate_of_id IS NULL
          AND (
            (dealer_website_url IS NOT NULL AND TRIM(dealer_website_url) != '')
            OR (website_url IS NOT NULL AND TRIM(website_url) != '')
          )
        ORDER BY id ASC
        """
    )
    rows = cur.fetchall()
    conn.close()
    out: list[dict] = []
    for r in rows:
        url = (r["dealer_website_url"] or r["website_url"] or "").strip()
        if not url:
            continue
        slug = str(r["id"])
        out.append({
            "name": r["name"] or "",
            "url": url,
            "dealer_id": "db-" + slug,
            "dealership_registry_id": r["id"],
            "city": r["city"] or "",
            "state": r["state"] or "",
        })
    return out


def load_manifest() -> list[dict]:
    use_db = (os.environ.get("DEALERS_FROM_DB") or "").strip().lower() in ("1", "true", "yes")
    if use_db:
        dealers = _load_dealers_from_db()
        logger.info("Loaded %d dealers from DB (DEALERS_FROM_DB=1)", len(dealers))
        return dealers
    with open(MANIFEST_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def filter_manifest_by_dealer_id(dealers: list, dealer_id: str) -> list:
    want = (dealer_id or "").strip()
    if not want:
        return []
    return [d for d in dealers if (d.get("dealer_id") or "").strip() == want]


def _default_skip_dealer_substrings() -> tuple[str, ...]:
    return ("carmax.com", "carmax-com")


def filter_skip_dealers(dealers: list) -> list:
    raw = (os.environ.get("SCANNER_SKIP_DEALER_SUBSTRINGS") or "").strip()
    if raw:
        needles = tuple(s.strip().lower() for s in raw.split(",") if s.strip())
    else:
        needles = _default_skip_dealer_substrings()
    if not needles:
        return list(dealers)

    kept: list = []
    skipped: list[str] = []
    for d in dealers:
        blob = " ".join(str(d.get(k) or "") for k in ("dealer_id", "url", "name")).lower()
        if any(n in blob for n in needles):
            skipped.append(str(d.get("name") or d.get("dealer_id") or "?"))
            continue
        kept.append(d)
    if skipped:
        logger.info(
            "Skipped %d dealer(s) via skip list: %s",
            len(skipped),
            ", ".join(skipped[:8]) + ("…" if len(skipped) > 8 else ""),
        )
    return kept


def filter_manifest_by_shard(
    dealers: list[Any], shard_index: int, shard_count: int
) -> list[Any]:
    if shard_count <= 1:
        return list(dealers)
    if shard_index < 0 or shard_index >= shard_count:
        raise ValueError(f"shard_index must satisfy 0 <= index < {shard_count}, got {shard_index}")
    return [d for i, d in enumerate(dealers) if i % shard_count == shard_index]


def resolve_shard_cli_and_env(args: argparse.Namespace) -> tuple[int, int]:
    cli_idx = getattr(args, "shard_index", None)
    cli_cnt = getattr(args, "shard_count", None)
    if cli_idx is not None or cli_cnt is not None:
        if cli_idx is None or cli_cnt is None:
            logger.error("--shard-index and --shard-count must be provided together.")
            sys.exit(2)
        if cli_cnt < 1:
            logger.error("--shard-count must be >= 1 (got %s).", cli_cnt)
            sys.exit(2)
        if cli_idx < 0 or cli_idx >= cli_cnt:
            logger.error(
                "--shard-index must satisfy 0 <= index < --shard-count (got index=%s count=%s).",
                cli_idx,
                cli_cnt,
            )
            sys.exit(2)
        return (cli_idx, cli_cnt)

    raw_cnt = (os.environ.get("SCANNER_SHARD_COUNT") or "").strip()
    if not raw_cnt:
        return (0, 1)
    try:
        shard_count = int(raw_cnt)
    except ValueError:
        logger.error("SCANNER_SHARD_COUNT must be an integer (got %r).", raw_cnt)
        sys.exit(2)
    if shard_count < 1:
        logger.error("SCANNER_SHARD_COUNT must be >= 1 (got %s).", shard_count)
        sys.exit(2)
    if shard_count == 1:
        return (0, 1)

    raw_idx = (
        os.environ.get("SCANNER_SHARD_INDEX") or os.environ.get("JOB_COMPLETION_INDEX") or ""
    ).strip()
    if not raw_idx:
        logger.error(
            "SCANNER_SHARD_COUNT=%s requires SCANNER_SHARD_INDEX or JOB_COMPLETION_INDEX.",
            shard_count,
        )
        sys.exit(2)
    try:
        shard_index = int(raw_idx)
    except ValueError:
        logger.error(
            "Shard index must be an integer (got %r).",
            raw_idx,
        )
        sys.exit(2)
    if shard_index < 0 or shard_index >= shard_count:
        logger.error(
            "Shard index %s out of range for SCANNER_SHARD_COUNT=%s.",
            shard_index,
            shard_count,
        )
        sys.exit(2)
    return (shard_index, shard_count)


def filter_oem_manufacturers(dealers: list[dict]) -> list[dict]:
    return [d for d in dealers if not is_oem_manufacturer_url(d.get("url", ""))]
