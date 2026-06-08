"""
Memory-safe scanner profile for local Mac Mini (16 GB RAM) runs.

This module does **not** change the main ``scanner.py`` pipeline. It only sets
conservative environment defaults and delegates to ``backend.scanner.cli``.

Intended scope: ``workspace/manifest_92694_25mi.json`` (92694 / 25 mi dev scan).
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

logger = logging.getLogger("scanner.mac_mini_lite")

PROJECT_ROOT = Path(__file__).resolve().parents[2]

DEFAULT_MANIFEST_REL = "workspace/manifest_92694_25mi.json"
ALLOW_ANY_MANIFEST_ENV = "SCANNER_MAC_MINI_LITE_ALLOW_ANY_MANIFEST"

# ~1 dealer + 3 inventory pages, no VDP — keeps Playwright under ~4 pages / ~2 GB.
MAC_MINI_LITE_ENV: dict[str, str] = {
    "SCANNER_MAC_MINI_LITE": "1",
    "SCANNER_FAST_MODE": "1",
    "SCANNER_SCAN_ONLY": "1",
    # Concurrency — biggest RAM levers
    "SCANNER_MAX_DEALER_CONCURRENCY": "1",
    "SCANNER_MAX_VDP_CONCURRENCY": "1",
    # Inventory-only on 16 GB: skip VDP (run post_scan later on M4 Max if needed)
    "SCANNER_VDP_EP_MAX": "0",
    "SCANNER_VDP_PRICE_MAX": "0",
    "SCANNER_VDP_SPEC_GAP_MAX": "0",
    # Sister-store location checkbox filter misfires on model/trim facets (e.g. Anaheim Hyundai)
    "SCANNER_SISTER_STORE_FILTER": "0",
    "SCANNER_VDP_COMPLETENESS_PASS": "0",
    "SCANNER_VDP_DOWNLOAD_IMAGES": "0",
    # Inventory sweep
    "SCANNER_INVENTORY_PATHS": "core",
    "SCANNER_DEALER_ON_MAX_PAGES": "25",
    "SCANNER_DEALER_ON_MAX_TOTAL": "2500",
    "SCANNER_INFINITE_SCROLL_MAX_ROUNDS": "10",
    "SCANNER_INVENTORY_WAIT_MS": "12000",
    "SCANNER_HYDRATION_TIMEOUT_MS": "20000",
    # Claude / vision / post-scan (all off for capture-only)
    "SCANNER_CLAUDE_VDP": "0",
    "SCANNER_GALLERY_VISION_INLINE": "0",
    "SCANNER_GALLERY_VISION_POST": "0",
    "SCANNER_POST_INTERIOR_VISION": "0",
    "SCANNER_POST_WINDOW_STICKER": "0",
    "SCANNER_POST_LISTING_GAP_FILL": "0",
    "SCANNER_POST_ENRICH": "0",
    "SCANNER_POST_ENRICH_VISION": "0",
    "SCANNER_POST_REPAIR": "0",
    "SCANNER_POST_LISTING_DESCRIPTION": "0",
    "SCANNER_POST_GAS_PRICES": "0",
    "SCANNER_POST_DEALER_RATINGS": "0",
    "SCANNER_POST_DICT_ENRICH": "0",
}


def default_manifest_path() -> Path:
    raw = (os.environ.get("DEALERS_MANIFEST_PATH") or DEFAULT_MANIFEST_REL).strip()
    p = Path(raw)
    if not p.is_absolute():
        p = PROJECT_ROOT / p
    return p


def manifest_allowed_for_mac_mini_lite(manifest_path: Path | str) -> bool:
    """Restrict lite runner to the local 92694 / 25 mi manifest unless overridden."""
    if (os.environ.get(ALLOW_ANY_MANIFEST_ENV) or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    ):
        return True
    name = Path(manifest_path).name.lower()
    return "92694" in name


def apply_mac_mini_lite_env(*, force: bool = True) -> None:
    """Apply Mac Mini memory-safe scanner defaults."""
    for key, value in MAC_MINI_LITE_ENV.items():
        if force:
            os.environ[key] = value
        else:
            os.environ.setdefault(key, value)
    os.environ.pop("ANTHROPIC_API_KEY", None)


def resolve_manifest_from_argv(argv: list[str] | None = None) -> Path | None:
    args = list(argv or sys.argv[1:])
    for i, arg in enumerate(args):
        if arg == "--manifest" and i + 1 < len(args):
            raw = args[i + 1].strip()
            if raw:
                p = Path(raw)
                return p if p.is_absolute() else (PROJECT_ROOT / p).resolve()
        if arg.startswith("--manifest="):
            raw = arg.split("=", 1)[1].strip()
            if raw:
                p = Path(raw)
                return p if p.is_absolute() else (PROJECT_ROOT / p).resolve()
    env_raw = (os.environ.get("DEALERS_MANIFEST_PATH") or "").strip()
    if env_raw:
        p = Path(env_raw)
        return p if p.is_absolute() else (PROJECT_ROOT / p).resolve()
    return None


def validate_mac_mini_lite_scope(manifest_path: Path | None = None) -> None:
    mp = manifest_path or resolve_manifest_from_argv() or default_manifest_path()
    if not manifest_allowed_for_mac_mini_lite(mp):
        raise SystemExit(
            "scanner_mac_mini.py is scoped to the local 92694 / 25 mi manifest only. "
            f"Got: {mp}. Set {ALLOW_ANY_MANIFEST_ENV}=1 to override (not recommended on 16GB)."
        )
    if not mp.is_file():
        raise SystemExit(f"Manifest not found: {mp}")


def bind_manifest_path(manifest_path: Path) -> Path:
    """Pin manifest before scanner constants/cli import (``--manifest`` alone is not enough)."""
    mp = manifest_path.resolve()
    os.environ["DEALERS_MANIFEST_PATH"] = str(mp)
    import backend.scanner.constants as scanner_constants

    scanner_constants.MANIFEST_PATH = mp
    return mp


def run_mac_mini_cli_entry() -> None:
    """Entry for ``scanner_mac_mini.py`` — apply lite env, validate scope, run main CLI."""
    apply_mac_mini_lite_env(force=True)
    mp = bind_manifest_path(resolve_manifest_from_argv() or default_manifest_path())
    validate_mac_mini_lite_scope(mp)
    logger.info(
        "Mac Mini lite scanner: dealer_concurrency=1 inventory-only manifest=%s",
        mp,
    )
    from backend.scanner.cli import run_cli_entry

    run_cli_entry()


def dealers_with_inventory_rows() -> set[str]:
    """Dealer ids that already have active rows in the local inventory DB."""
    try:
        import sqlite3

        from backend.db.inventory_db import DB_PATH

        if not Path(DB_PATH).is_file():
            return set()
        conn = sqlite3.connect(DB_PATH)
        try:
            rows = conn.execute(
                """
                SELECT DISTINCT dealer_id FROM cars
                WHERE dealer_id IS NOT NULL AND TRIM(dealer_id) != ''
                  AND COALESCE(listing_active, 1) = 1
                """
            ).fetchall()
            return {str(r[0]).strip() for r in rows if r and r[0]}
        finally:
            conn.close()
    except Exception:
        logger.exception("Could not read dealers from inventory DB")
        return set()


def load_manifest_dealers(manifest_path: Path | None = None) -> list[dict]:
    import json

    mp = manifest_path or default_manifest_path()
    data = json.loads(mp.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        return []
    return [d for d in data if isinstance(d, dict) and str(d.get("dealer_id") or "").strip()]


def remaining_manifest_dealers(
    manifest_path: Path | None = None,
    *,
    skip_ids: set[str] | None = None,
) -> list[dict]:
    """Manifest dealers not yet present in inventory.db (for resume after a partial run)."""
    done = dealers_with_inventory_rows()
    skip = skip_ids or {"carmax-com"}
    out: list[dict] = []
    for dealer in load_manifest_dealers(manifest_path):
        did = str(dealer.get("dealer_id") or "").strip()
        if not did or did in skip or did in done:
            continue
        out.append(dealer)
    return out


def run_continue_mac_mini_scan(manifest_path: Path | None = None) -> int:
    """
    Resume 92694 scan one dealer per subprocess (fresh browser each time → lower RAM).

    Returns exit code 0 if all remaining dealers succeeded, 1 if any failed.
    """
    import subprocess

    apply_mac_mini_lite_env(force=True)
    mp = bind_manifest_path(manifest_path or default_manifest_path())
    validate_mac_mini_lite_scope(mp)
    remaining = remaining_manifest_dealers(mp)
    if not remaining:
        logger.info("Mac Mini continue: nothing left to scan in %s", mp)
        return 0

    logger.info("Mac Mini continue: %d dealer(s) remaining", len(remaining))
    failed = 0
    for dealer in remaining:
        did = str(dealer.get("dealer_id") or "").strip()
        name = str(dealer.get("name") or did).strip()
        env = os.environ.copy()
        env.update(MAC_MINI_LITE_ENV)
        env["DEALERS_MANIFEST_PATH"] = str(mp)
        cmd = [
            sys.executable,
            str(PROJECT_ROOT / "scanner_mac_mini.py"),
            "--manifest",
            str(mp),
            "--dealer-id",
            did,
            "--scan-only",
        ]
        logger.info("Mac Mini continue: starting %s (%s)", name, did)
        proc = subprocess.run(cmd, cwd=str(PROJECT_ROOT), env=env)
        if proc.returncode != 0:
            failed += 1
            logger.warning("Mac Mini continue: %s failed exit=%s", did, proc.returncode)
    return 1 if failed else 0
