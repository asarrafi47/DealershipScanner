"""Fonts are self-hosted, so the CSP no longer allows Google Fonts hosts (visual review 2026-09-28, item F)."""

from pathlib import Path


def test_csp_strings_drop_google_fonts():
    from backend.web import security

    for value in (security._csp_header_value_enforced("n0nce"), security._CSP_REPORT_ONLY):
        assert "fonts.googleapis.com" not in value
        assert "fonts.gstatic.com" not in value
        assert "font-src 'self' data:;" in value
        assert "style-src-elem 'self';" in value


def test_no_template_loads_google_fonts():
    root = Path(__file__).resolve().parents[2] / "frontend"
    for p in list(root.rglob("*.html")) + list((root / "static" / "css").rglob("*.css")):
        text = p.read_text(encoding="utf-8", errors="ignore")
        assert "fonts.googleapis.com" not in text, p
        assert "fonts.gstatic.com" not in text, p
