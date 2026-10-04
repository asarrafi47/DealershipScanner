"""Templates must not contain <style> elements.

Production enforces CSP ``style-src-elem 'self'`` (backend/web/security.py,
``_csp_header_value_enforced``), which blocks every inline <style> element, so a
page that keeps its CSS in one renders unstyled in prod while looking fine in dev.
Page CSS belongs in frontend/static/css/ (admin and dealer-inventory pages use
16-admin-pages.css). Inline ``style=""`` attributes are allowed by
``style-src 'unsafe-inline'`` and are not checked here.
"""

from __future__ import annotations

import re
from pathlib import Path

from backend.web.security import _csp_header_value_enforced

REPO_ROOT = Path(__file__).resolve().parents[2]
TEMPLATES = REPO_ROOT / "frontend" / "templates"
STATIC_CSS = REPO_ROOT / "frontend" / "static" / "css"

_STYLE_ELEMENT = re.compile(r"<style[\s>]", re.IGNORECASE)


def test_no_template_contains_a_style_element() -> None:
    offenders = []
    for path in sorted(TEMPLATES.rglob("*.html")):
        for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if _STYLE_ELEMENT.search(line):
                offenders.append(f"{path.relative_to(REPO_ROOT)}:{lineno}")
    assert not offenders, (
        "Inline <style> elements are blocked by the production CSP "
        "(style-src-elem 'self'); move the rules into frontend/static/css/: "
        + ", ".join(offenders)
    )


def test_enforced_csp_still_blocks_style_elements() -> None:
    # The fix is to move CSS out of templates, not to relax the policy.
    csp = _csp_header_value_enforced("n")
    assert "style-src-elem 'self';" in csp


def test_admin_page_css_is_scoped_and_loaded() -> None:
    css = (STATIC_CSS / "16-admin-pages.css").read_text(encoding="utf-8")
    scopes = (
        ".admin-page--site",
        ".admin-page--dealers",
        ".admin-page--users",
        ".admin-page--user-form",
        ".admin-page--reviews",
        ".dealer-inv-body",
        ".scan-lab-listings-page",
    )
    # Every selector line starts with one of the page scopes, so no rule leaks
    # onto pages that happen to share a class name (.status-pill, .icon-button).
    body = re.sub(r"/\*.*?\*/", "", css, flags=re.S)
    body = re.sub(r"@keyframes[^{]*\{(?:[^{}]*\{[^{}]*\})*[^{}]*\}", "", body)
    for selector_block in re.findall(r"([^{}@]+)\{[^{}]*\}", body):
        for selector in selector_block.split(","):
            selector = selector.strip()
            if selector:
                assert selector.startswith(scopes), f"unscoped selector: {selector!r}"

    link = "css/16-admin-pages.css"
    for rel in ("admin/base_admin.html", "inventory/base_inventory.html", "dev_scan_lab_listings.html"):
        assert link in (TEMPLATES / rel).read_text(encoding="utf-8"), rel


def test_each_page_carries_its_scope_class() -> None:
    # The scoped rules only apply when the page's <body> carries the class; a page
    # that drops its body_class block silently loses its styling.
    expected = {
        "admin/site_hub.html": "admin-page--site",
        "admin/dealers.html": "admin-page--dealers",
        "admin/users.html": "admin-page--users",
        "admin/user_form.html": "admin-page--user-form",
        "admin/reviews.html": "admin-page--reviews",
        "inventory/base_inventory.html": "dealer-inv-body",
        "dev_scan_lab_listings.html": "scan-lab-listings-page",
    }
    for rel, cls in expected.items():
        text = (TEMPLATES / rel).read_text(encoding="utf-8")
        if rel.startswith("admin/"):
            assert re.search(r"\{%\s*block body_class\s*%\}\s*" + re.escape(cls) + r"\s*\{%\s*endblock", text), rel
        else:
            assert re.search(r"<body class=\"[^\"]*\b" + re.escape(cls) + r"\b", text), rel
    assert "{% block body_class %}" in (TEMPLATES / "admin/base_admin.html").read_text(encoding="utf-8")
