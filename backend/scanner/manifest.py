"""
Dealer manifest loading and filtering (dealers.json or registry DB).
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sqlite3
import sys
from pathlib import Path
from typing import Any

from backend.scanner.constants import MANIFEST_PATH, OEM_MANUFACTURER_DOMAINS

logger = logging.getLogger(__name__)


def is_oem_manufacturer_url(url: str) -> bool:
    if not url:
        return False
    url_lower = url.lower()
    return any(domain in url_lower for domain in OEM_MANUFACTURER_DOMAINS)


def _host_to_dealer_id(url: str) -> str:
    """Canonical dealer_id from a website URL — the hostname with the scheme and
    leading ``www.`` stripped and dots turned to dashes (``longotoyota-com``).

    This MUST match how the rest of the system keys a dealer (the ``cars`` table,
    specials, the dealership page): a scan sourced from the registry has to land
    its inventory under the same ``dealer_id`` the hostname produces, or the
    registry row, its inventory and its specials never line up.
    """
    return re.sub(r"^https?://(www\.)?", "", (url or "").strip(), flags=re.I).split("/")[0].replace(".", "-")


def _load_dealers_from_db() -> list[dict]:
    from backend.db.dealerships_db import ensure_dealerships_table, get_conn

    # NB: don't set ``conn.row_factory = sqlite3.Row`` here — the inventory conn
    # is a Postgres compat wrapper whose rows are positionally indexable, and the
    # sqlite-only factory poisons the cursor so ``ensure_dealerships_table``'s
    # ``r[0]`` column probe crashes. Select a fixed column order and index it.
    conn = get_conn()
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
        r_id, r_name, r_dealer_url, r_site_url, r_city, r_state, _r_zip = (
            r[0], r[1], r[2], r[3], r[4], r[5], r[6]
        )
        url = (r_dealer_url or r_site_url or "").strip()
        if not url:
            continue
        dealer_id = _host_to_dealer_id(url)
        if not dealer_id:
            continue
        out.append({
            "name": r_name or "",
            "url": url,
            "dealer_id": dealer_id,
            "dealership_registry_id": r_id,
            "city": r_city or "",
            "state": r_state or "",
        })
    return out


def _load_active_inventory_dealers() -> list[dict]:
    """Roster of every dealer that has ACTIVE inventory in ``cars`` — the set a
    delta refresh should actually cover.

    This is deliberately distinct from :func:`_load_dealers_from_db` (the
    dealership *registry*, mostly never-scanned discovery candidates that is
    nearly disjoint from the dealers we have actually scanned). Using the
    registry for a delta sweep touches only the handful of dealers that happen
    to appear in both lists; the correct roster for "refresh what we have" is the
    distinct ``dealer_id`` set from live inventory. The base URL is derived from
    a stored listing URL; recipe eligibility is left to the delta path (a dealer
    with no stored recipe simply skips fast).
    """
    from urllib.parse import urlparse

    from backend.db.inventory_db import get_conn

    # NB: no ``row_factory`` — the Postgres compat rows are positionally indexed
    # (see the note in ``_load_dealers_from_db``).
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT dealer_id, MAX(dealer_name) AS name, MAX(source_url) AS any_url
        FROM cars
        WHERE COALESCE(listing_active, 1) = 1
          AND dealer_id IS NOT NULL AND TRIM(dealer_id) != ''
        GROUP BY dealer_id
        """
    )
    rows = cur.fetchall()
    conn.close()
    out: list[dict] = []
    for r in rows:
        did = str(r[0] or "").strip()
        if not did:
            continue
        name = str(r[1] or "").strip()
        any_url = str(r[2] or "").strip()
        u = urlparse(any_url)
        base = f"{u.scheme}://{u.netloc}" if u.scheme and u.netloc else ""
        out.append({"dealer_id": did, "url": base, "name": name or did})
    return out


def load_manifest() -> list[dict]:
    if (os.environ.get("DEALERS_FROM_ACTIVE_INVENTORY") or "").strip().lower() in (
        "1", "true", "yes",
    ):
        dealers = _load_active_inventory_dealers()
        logger.info(
            "Loaded %d dealers from active inventory (DEALERS_FROM_ACTIVE_INVENTORY=1)",
            len(dealers),
        )
        return dealers
    use_db = (os.environ.get("DEALERS_FROM_DB") or "").strip().lower() in ("1", "true", "yes")
    if use_db:
        dealers = _load_dealers_from_db()
        logger.info("Loaded %d dealers from DB (DEALERS_FROM_DB=1)", len(dealers))
        return dealers
    # Re-read env at call time so --manifest CLI arg (set via DEALERS_MANIFEST_PATH) takes effect
    # even after modules have already been imported.
    env_path = os.environ.get("DEALERS_MANIFEST_PATH", "").strip()
    path = Path(env_path) if env_path else MANIFEST_PATH
    logger.info("Loading manifest: %s (resolved %s)", path, path.resolve())
    with open(path, "r", encoding="utf-8") as f:
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
    if raw.lower() in ("none", "-"):
        return list(dealers)  # explicit full coverage — disable even the built-in skips
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


def filter_manifest_skip_flag(dealers: list[dict]) -> list[dict]:
    """Drop entries with ``skip: true`` in the manifest (offline, bot-blocked, DNS broken)."""
    kept, dropped = [], []
    for d in dealers:
        if d.get("skip"):
            dropped.append(str(d.get("name") or d.get("dealer_id") or "?"))
        else:
            kept.append(d)
    if dropped:
        logger.info(
            "Manifest skip flag: removed %d dealer(s): %s",
            len(dropped),
            ", ".join(dropped[:8]) + ("…" if len(dropped) > 8 else ""),
        )
    return kept
