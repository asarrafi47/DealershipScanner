#!/usr/bin/env python3
"""
Async homepage fingerprinting for ``dealers.json``: infer ``provider`` from HTML + headers.

Loads the manifest (defaulting to ``local_data/dealers.json`` when present), optionally filters
rows still on the discovery placeholder, fetches each homepage concurrently with aiohttp, assigns a
provider slug, then writes compact JSON back atomically via :func:`backend.dev.dealers.save_dealers`.

Examples::

  python backend/scripts/fingerprint_providers.py
  python backend/scripts/fingerprint_providers.py --manifest local_data/dealers.json --concurrency 35
  python backend/scripts/fingerprint_providers.py --rescan-all --dry-run

Close editors and pause cloud sync on the manifest while this runs; concurrent writes can corrupt
the file.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import socket
import sys
import warnings
from pathlib import Path
from typing import Any

warnings.filterwarnings(
    "ignore",
    message=r".*doesn't match a supported version.*",
)

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

try:
    from backend.utils.project_env import load_project_dotenv

    load_project_dotenv()
except ImportError:
    pass

import aiohttp
from aiohttp import ClientResponse

from backend.dev.dealers import load_dealers, normalize_manifest_url, save_dealers

_LOG = logging.getLogger("fingerprint_providers")

_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

# Rows assumed to be Overture/discovery placeholders until fingerprinted.
_DEFAULT_PLACEHOLDER_PROVIDERS = frozenset({"dealer_dot_com"})


def _default_manifest_path(repo_root: Path) -> Path:
    local = repo_root / "local_data" / "dealers.json"
    if local.is_file():
        return local.resolve()
    return (repo_root / "dealers.json").resolve()


def header_blob_from_response(resp: ClientResponse) -> str:
    """Concatenate all response header lines for signature checks (case preserved)."""
    lines: list[str] = []
    for key in resp.headers:
        for val in resp.headers.getall(key):
            lines.append(f"{key}:{val}")
    return "\n".join(lines)


def fingerprint_provider(*, html: str, header_blob: str) -> str:
    """
    Infer manifest ``provider`` from raw homepage HTML and header lines.

    Order is intentional: full-stack / OEM-site signals before layered search (Algolia).
    ``sincro_cdk`` avoids a bare ``cobalt`` substring (e.g. Chevrolet Cobalt model pages).
    """
    html_l = (html or "").lower()
    blob_l = (header_blob or "").lower()

    # Dealer.com — HTML domain references or DDC markers in cookies / headers.
    if "dealer.com" in html_l or "ddc" in blob_l:
        return "dealer_dot_com"

    if "dealerinspire.com" in html_l or "dealer inspire" in html_l:
        return "dealer_inspire"

    if "foxdealer.com" in html_l:
        return "fox_dealer"

    if "shiftdigital" in html_l:
        return "shift_digital"

    dealer_on_markers = ("dealeron.com", "dealeron.net", "//dealeron", "static.dealeron", "api.dealeron")
    if any(m in html_l for m in dealer_on_markers):
        return "dealer_on"

    sincro_markers = (
        "sincrodigital.com",
        "sincrodigital",
        "cdkglobal",
        "cdk.com",
        "cobalt.net",
        "cobaltgroup",
        "cobaltdigital",
    )
    if any(m in html_l for m in sincro_markers):
        return "sincro_cdk"

    if "algolia" in html_l or "algolia" in blob_l:
        return "algolia"

    return "unknown"


def _is_dns_failure(exc: BaseException) -> bool:
    """True when *exc* is a DNS resolution failure (quiet handling → DNS_FAIL)."""
    if isinstance(exc, socket.gaierror):
        return True
    cur: BaseException | None = exc
    seen: set[int] = set()
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        if isinstance(cur, socket.gaierror):
            return True
        cur = cur.__cause__ or cur.__context__
    msg = str(exc).lower()
    if "gaierror" in msg or "getaddrinfo" in msg or "name or service not known" in msg:
        return True
    return False


async def _fetch_homepage_once(
    session: aiohttp.ClientSession,
    url: str,
    *,
    timeout: aiohttp.ClientTimeout,
    max_body_bytes: int,
) -> tuple[str, str, str | None]:
    """
    Single GET attempt. Returns ``(html, header_blob, error_reason)``.
    DNS failures return ``DNS_FAIL`` (no exception propagation).
    """
    try:
        async with session.get(url, allow_redirects=True, timeout=timeout) as resp:
            blob = header_blob_from_response(resp)
            chunk = await resp.content.read(max_body_bytes + 1)
            if len(chunk) > max_body_bytes:
                chunk = chunk[:max_body_bytes]
            text = chunk.decode("utf-8", errors="replace")
            return text, blob, None
    except asyncio.TimeoutError:
        return "", "", "timeout"
    except socket.gaierror:
        return "", "", "DNS_FAIL"
    except aiohttp.ClientConnectorError as e:
        if _is_dns_failure(e):
            return "", "", "DNS_FAIL"
        return "", "", f"connect_error:{type(e).__name__}:{e}"
    except aiohttp.ClientSSLError as e:
        return "", "", f"ssl_error:{type(e).__name__}"
    except aiohttp.ClientError as e:
        if _is_dns_failure(e):
            return "", "", "DNS_FAIL"
        return "", "", f"client_error:{type(e).__name__}:{e}"
    except OSError as e:
        if isinstance(e, socket.gaierror) or _is_dns_failure(e):
            return "", "", "DNS_FAIL"
        return "", "", f"os_error:{type(e).__name__}:{e}"
    except Exception as e:  # noqa: BLE001 — manifest URLs are untrusted; never abort the batch
        if _is_dns_failure(e):
            return "", "", "DNS_FAIL"
        return "", "", f"unexpected:{type(e).__name__}:{e}"


async def fetch_homepage_body(
    session: aiohttp.ClientSession,
    url: str,
    *,
    timeout: aiohttp.ClientTimeout,
    timeout_retry: aiohttp.ClientTimeout,
    max_body_bytes: int,
) -> tuple[str, str, str | None]:
    """
    GET ``url`` with one retry on timeout only (longer ``timeout_retry``).

    DNS errors → ``DNS_FAIL`` silently (caller aggregates; no per-row console noise).
    """
    html, blob, err = await _fetch_homepage_once(
        session, url, timeout=timeout, max_body_bytes=max_body_bytes
    )
    if err == "timeout":
        html, blob, err = await _fetch_homepage_once(
            session, url, timeout=timeout_retry, max_body_bytes=max_body_bytes
        )
    return html, blob, err


async def fingerprint_one(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    row: dict[str, Any],
    *,
    timeout: aiohttp.ClientTimeout,
    timeout_retry: aiohttp.ClientTimeout,
    max_body_bytes: int,
) -> tuple[str, str | None, str]:
    """Fetch one row's ``url``, set ``row['provider']``. Returns ``(label, error_reason, provider)``."""
    raw_url = str(row.get("url") or "").strip()
    dealer_id = str(row.get("dealer_id") or "").strip()
    label = dealer_id or raw_url or "?"

    nu = normalize_manifest_url(raw_url)
    if not nu:
        row["provider"] = "unknown"
        return label, "bad_url", row["provider"]

    async with sem:
        html, blob, err = await fetch_homepage_body(
            session,
            nu,
            timeout=timeout,
            timeout_retry=timeout_retry,
            max_body_bytes=max_body_bytes,
        )

    if err:
        row["provider"] = "unknown"
        return label, err, row["provider"]

    row["provider"] = fingerprint_provider(html=html, header_blob=blob)
    row["url"] = nu
    return label, None, row["provider"]


async def run_async(
    rows: list[dict[str, Any]],
    *,
    concurrency: int,
    timeout_sec: float,
    timeout_retry_sec: float,
    max_body_bytes: int,
    progress_every: int,
) -> tuple[dict[str, int], dict[str, int]]:
    """
    Fingerprint all rows in-place.

    Returns ``(failure_breakdown, provider_counts)`` where *failure_breakdown* counts
    ``DNS_FAIL``, ``timeout``, ``bad_url``, and ``other``.
    """
    sem = asyncio.Semaphore(max(1, concurrency))
    timeout = aiohttp.ClientTimeout(total=timeout_sec)
    timeout_retry = aiohttp.ClientTimeout(total=timeout_retry_sec)
    conn_limit = max(40, min(200, concurrency * 3))
    connector = aiohttp.TCPConnector(
        limit=conn_limit,
        ssl=False,
        use_dns_cache=True,
        ttl_dns_cache=600,
        dns_cache_max_size=16384,
    )
    headers = {"User-Agent": _USER_AGENT, "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8"}

    counts: dict[str, int] = {}
    failures: dict[str, int] = {"DNS_FAIL": 0, "timeout": 0, "bad_url": 0, "other": 0}
    done = 0
    total = len(rows)

    async with aiohttp.ClientSession(connector=connector, headers=headers) as session:
        tasks = [
            asyncio.create_task(
                fingerprint_one(
                    session,
                    sem,
                    r,
                    timeout=timeout,
                    timeout_retry=timeout_retry,
                    max_body_bytes=max_body_bytes,
                )
            )
            for r in rows
        ]
        for fut in asyncio.as_completed(tasks):
            _label, err, prov = await fut
            done += 1
            counts[prov] = counts.get(prov, 0) + 1
            if err:
                if err == "DNS_FAIL":
                    failures["DNS_FAIL"] += 1
                elif err == "timeout":
                    failures["timeout"] += 1
                elif err == "bad_url":
                    failures["bad_url"] += 1
                else:
                    failures["other"] += 1
            pe = max(1, progress_every)
            if done % pe == 0 or done == total:
                _LOG.info("Fingerprinted %s/%s URLs…", done, total)

    return failures, counts


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    p = argparse.ArgumentParser(description="Fingerprint dealership homepage platforms into dealers.json.")
    p.add_argument(
        "--manifest",
        type=Path,
        default=None,
        help=f"Path to dealers.json (default: {_REPO_ROOT}/local_data/dealers.json if present, else {_REPO_ROOT}/dealers.json)",
    )
    p.add_argument(
        "--concurrency",
        type=int,
        default=32,
        help="Maximum simultaneous HTTP requests (default: 32; use ~25–40 to reduce DNS load).",
    )
    p.add_argument(
        "--timeout",
        type=float,
        default=12.0,
        help="Initial per-request total timeout in seconds (default: 12).",
    )
    p.add_argument(
        "--timeout-retry",
        type=float,
        default=20.0,
        help="Second-attempt timeout in seconds after a timeout (default: 20).",
    )
    p.add_argument(
        "--max-body-bytes",
        type=int,
        default=750_000,
        help="Cap bytes read per homepage for fingerprinting (default: 750000).",
    )
    p.add_argument(
        "--progress-every",
        type=int,
        default=500,
        help="Emit an INFO log line every N completed URLs (default: 500).",
    )
    p.add_argument(
        "--rescan-all",
        action="store_true",
        help="Fingerprint every row; default is only rows whose provider looks like a discovery placeholder.",
    )
    p.add_argument(
        "--placeholder-provider",
        action="append",
        default=None,
        metavar="SLUG",
        help=(
            "Treat this provider value as an unscanned placeholder (repeatable). "
            "Default if omitted: dealer_dot_com. Ignored with --rescan-all."
        ),
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and fingerprint but do not write the manifest.",
    )
    p.add_argument(
        "--backup",
        type=Path,
        default=None,
        help=f"Copy manifest here before overwrite (default: no backup).",
    )
    args = p.parse_args(argv)

    manifest = (
        args.manifest.expanduser().resolve()
        if args.manifest
        else _default_manifest_path(_REPO_ROOT)
    )
    if not manifest.is_file():
        _LOG.error("Manifest not found: %s", manifest)
        return 2

    all_rows = load_dealers(manifest)
    if not all_rows:
        _LOG.warning("Manifest is empty: %s", manifest)
        return 0

    placeholders = (
        frozenset(args.placeholder_provider)
        if args.placeholder_provider
        else _DEFAULT_PLACEHOLDER_PROVIDERS
    )

    if args.rescan_all:
        to_scan = all_rows
        skipped = 0
    else:
        to_scan = [r for r in all_rows if str(r.get("provider") or "").strip() in placeholders]
        skipped = len(all_rows) - len(to_scan)

    _LOG.info(
        "Loaded %s dealers from %s — scanning %s rows (%s skipped).",
        len(all_rows),
        manifest,
        len(to_scan),
        skipped,
    )

    if not to_scan:
        _LOG.info("Nothing to scan.")
        return 0

    failures, counts = asyncio.run(
        run_async(
            to_scan,
            concurrency=args.concurrency,
            timeout_sec=args.timeout,
            timeout_retry_sec=args.timeout_retry,
            max_body_bytes=args.max_body_bytes,
            progress_every=args.progress_every,
        )
    )

    summary = ", ".join(f"{k}={v}" for k, v in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])))
    fail_total = sum(failures.values())
    _LOG.info(
        "Done. Issues: DNS_FAIL=%s timeout=%s bad_url=%s other=%s (total %s). Provider counts: %s",
        failures["DNS_FAIL"],
        failures["timeout"],
        failures["bad_url"],
        failures["other"],
        fail_total,
        summary,
    )

    if args.dry_run:
        _LOG.info("Dry-run: not writing manifest.")
        return 0

    if args.backup:
        import shutil

        bak = args.backup.expanduser().resolve()
        bak.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(manifest, bak)
        _LOG.info("Backup written to %s", bak)

    save_dealers(all_rows, manifest, compact=True)
    _LOG.info("Wrote %s", manifest)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
