"""Exit gate for N18 (P12A.3/P12A.6): no direct HTTP client in ``backend/scanner`` outside
``backend/scanner/net``.

Every scanner fetch goes through ``backend.scanner.net.client`` (one fetch policy, proxy,
profile rotation, challenge detection). This test parses each ``.py`` under
``backend/scanner`` and counts imports of ``requests`` / ``curl_cffi`` / ``httpx`` /
``aiohttp`` / ``urllib3`` / ``urllib.request`` (function-level imports included) and
``urlopen(`` calls. Files below are audited debt with the count they may still hold; the
table only shrinks: a file that dropped below its count (or to zero) must be edited here.
"""
from __future__ import annotations

import ast
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
_SCANNER = _REPO / "backend" / "scanner"
_ALLOWED_DIR = "backend/scanner/net/"

_BANNED = {"requests", "curl_cffi", "httpx", "aiohttp", "urllib3"}

# file -> (sites still allowed, why it is not on net/client yet)
KNOWN_DEBT: dict[str, tuple[int, str]] = {
    "backend/scanner/http_fetch.py": (2, "urllib wrapper kept for callers that patch urlopen; moves to net/client with its callers"),
    "backend/scanner/carscommerce_harvest.py": (1, "urllib JSON harvest against the carscommerce search API"),
    "backend/scanner/platform_registry.py": (1, "urllib platform probe"),
    "backend/scanner/specials/fetch.py": (1, "urllib specials page fetch"),
    "backend/scanner/synth/http.py": (3, "urllib arms of the synth urllib -> curl_cffi chain; the cffi arm is on net/client"),
    "backend/scanner/post_scan/gap_fill.py": (1, "requests import; the fetches themselves moved in P12A.3"),
    "backend/scanner/post_scan/pipeline.py": (1, "function-level requests import in the post-scan pipeline"),
    "backend/scanner/post_scan/window_sticker.py": (4, "OEM sticker fetches (on demand only, left alone by owner rule)"),
    "backend/scanner/recipes.py": (1, "function-level requests import in the replay path (P12A.2 follow-up)"),
    "backend/scanner/scrapers/autowall.py": (1, "requests Session alias for cookie-persisting pagination"),
    "backend/scanner/scrapers/dealer_eprocess.py": (1, "urllib request alias"),
    "backend/scanner/scrapers/dealer_inspire.py": (1, "requests import for the search API POST"),
    "backend/scanner/scrapers/dealer_venom.py": (1, "requests import for the feed GET"),
    "backend/scanner/scrapers/shopperexpress.py": (1, "aiohttp import for the async feed"),
}


def _is_banned(module: str) -> bool:
    return module.split(".")[0] in _BANNED or module == "urllib.request"


def _sites(path: Path) -> int:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    n = 0
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            n += sum(1 for a in node.names if _is_banned(a.name))
        elif isinstance(node, ast.ImportFrom):
            mod = node.module or ""
            if _is_banned(mod) or (mod == "urllib" and any(a.name == "request" for a in node.names)):
                n += 1
        elif isinstance(node, ast.Call):
            f = node.func
            if (isinstance(f, ast.Attribute) and f.attr == "urlopen") or (isinstance(f, ast.Name) and f.id == "urlopen"):
                n += 1
    return n


def _scan() -> dict[str, int]:
    found: dict[str, int] = {}
    for p in sorted(_SCANNER.rglob("*.py")):
        rel = p.relative_to(_REPO).as_posix()
        if rel.startswith(_ALLOWED_DIR) or "/tests/" in rel:
            continue
        n = _sites(p)
        if n:
            found[rel] = n
    return found


def test_no_new_raw_http_clients_in_scanner():
    found = _scan()
    new = {f: n for f, n in found.items() if f not in KNOWN_DEBT}
    assert not new, f"direct HTTP client outside backend/scanner/net (use net.client): {new}"
    over = {f: (n, KNOWN_DEBT[f][0]) for f, n in found.items() if f in KNOWN_DEBT and n > KNOWN_DEBT[f][0]}
    assert not over, f"more raw HTTP sites than the audited debt allows: {over}"


def test_debt_table_only_shrinks():
    found = _scan()
    stale = {f: (found.get(f, 0), cap) for f, (cap, _why) in KNOWN_DEBT.items() if found.get(f, 0) < cap}
    assert not stale, f"lower these KNOWN_DEBT entries (or delete them): {stale}"


def test_net_client_is_the_only_curl_cffi_importer():
    offenders = []
    for p in sorted(_SCANNER.rglob("*.py")):
        rel = p.relative_to(_REPO).as_posix()
        if rel.startswith(_ALLOWED_DIR):
            continue
        tree = ast.parse(p.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            mods = [a.name for a in node.names] if isinstance(node, ast.Import) else (
                [node.module or ""] if isinstance(node, ast.ImportFrom) else [])
            if any(m.split(".")[0] == "curl_cffi" for m in mods):
                offenders.append(rel)
    assert not offenders, f"curl_cffi imported outside net/client: {offenders}"
