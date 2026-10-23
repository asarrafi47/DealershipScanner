"""CSP sources: esm.sh is gone from both policies (P3.4, security-2, 2026-10-18).

Nothing in ``frontend/`` loads from https://esm.sh, so allowing it in
``script-src`` and ``connect-src`` only widened what an injected script could
pull in. These tests pin the exact script/connect sources of the enforced and
report-only policies, check the headers a real response carries, and check that
no template or static script needs a cross-origin script source.
"""

from __future__ import annotations

import re
from pathlib import Path

FRONTEND = Path(__file__).resolve().parents[2] / "frontend"


def _directives(policy: str) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for part in policy.split(";"):
        tokens = part.split()
        if tokens:
            out[tokens[0]] = tokens[1:]
    return out


def test_policy_strings_have_no_esm_sh():
    from backend.web import security

    for value in (security._csp_header_value_enforced("n0nce"), security._CSP_REPORT_ONLY):
        assert "esm.sh" not in value


def test_enforced_policy_script_and_connect_sources_are_exact():
    from backend.web import security

    d = _directives(security._csp_header_value_enforced("n0nce"))
    assert d["script-src"] == ["'self'", "'nonce-n0nce'"]
    assert d["connect-src"] == ["'self'", "https://tile.openstreetmap.org"]
    assert d["default-src"] == ["'self'"]


def test_report_only_policy_script_and_connect_sources_are_exact():
    from backend.web import security

    d = _directives(security._CSP_REPORT_ONLY)
    assert d["script-src"] == ["'self'"]
    assert d["connect-src"] == ["'self'", "https://tile.openstreetmap.org"]
    assert d["default-src"] == ["'self'"]


def test_enforced_header_on_a_response_has_no_esm_sh(app_factory):
    app = app_factory(CSP_ENFORCE="1", CSP_REPORT_ONLY="").app
    with app.test_client() as c:
        rv = c.get("/login")
    assert rv.status_code == 200
    csp = rv.headers.get("Content-Security-Policy", "")
    assert csp, "CSP_ENFORCE=1 must send the enforced header"
    assert "esm.sh" not in csp
    d = _directives(csp)
    assert len(d["script-src"]) == 2 and d["script-src"][0] == "'self'"
    assert d["script-src"][1].startswith("'nonce-")
    assert d["connect-src"] == ["'self'", "https://tile.openstreetmap.org"]
    # The page's script tags carry the same nonce, so 'self' + nonce is enough.
    nonce = d["script-src"][1][len("'nonce-") : -1]
    body = rv.get_data(as_text=True)
    assert f'nonce="{nonce}"' in body


def test_report_only_header_on_a_response_has_no_esm_sh(app_factory):
    app = app_factory(CSP_ENFORCE="0", CSP_REPORT_ONLY="1").app
    with app.test_client() as c:
        rv = c.get("/login")
    assert rv.status_code == 200
    assert "Content-Security-Policy" not in rv.headers
    ro = rv.headers.get("Content-Security-Policy-Report-Only", "")
    assert ro, "CSP_REPORT_ONLY=1 must send the report-only header"
    assert "esm.sh" not in ro
    assert _directives(ro)["script-src"] == ["'self'"]


def test_no_frontend_file_references_esm_sh():
    hits = [
        str(p.relative_to(FRONTEND))
        for p in FRONTEND.rglob("*")
        if p.is_file()
        and p.suffix in {".html", ".js", ".mjs", ".css", ".json"}
        and "esm.sh" in p.read_text(encoding="utf-8", errors="ignore")
    ]
    assert hits == []


_CROSS_ORIGIN_SCRIPT_TAG = re.compile(r"<script[^>]*\ssrc=[\"']?(?:https?:)?//", re.IGNORECASE)
_CROSS_ORIGIN_IMPORT = re.compile(
    r"""(?:\bimport\s*\(\s*|\bfrom\s+|\bimport\s+|importScripts\s*\(\s*)["'](?:https?:)?//""",
)


def test_no_template_or_static_script_loads_a_cross_origin_script():
    """script-src is 'self' (+ nonce): a cross-origin script would be blocked."""
    tag_hits = [
        str(p.relative_to(FRONTEND))
        for p in (FRONTEND / "templates").rglob("*.html")
        if _CROSS_ORIGIN_SCRIPT_TAG.search(p.read_text(encoding="utf-8", errors="ignore"))
    ]
    import_hits = [
        str(p.relative_to(FRONTEND))
        for p in (FRONTEND / "static").rglob("*")
        if p.is_file()
        and p.suffix in {".js", ".mjs"}
        and _CROSS_ORIGIN_IMPORT.search(p.read_text(encoding="utf-8", errors="ignore"))
    ]
    assert tag_hits == []
    assert import_hits == []
