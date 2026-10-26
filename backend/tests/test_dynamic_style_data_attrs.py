"""Dynamic styles go through data-* + CSSOM, not style="" attributes (P16.8)."""

from __future__ import annotations

import re
from pathlib import Path

from backend.web import security

REPO_ROOT = Path(__file__).resolve().parents[2]
TEMPLATES = REPO_ROOT / "frontend" / "templates"
STATIC = REPO_ROOT / "frontend" / "static"

_STYLE_ATTR = re.compile(r"""\sstyle=["']""")


def test_no_template_has_any_style_attribute() -> None:
    offenders = []
    for path in sorted(TEMPLATES.rglob("*.html")):
        for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if _STYLE_ATTR.search(line):
                offenders.append(f"{path.relative_to(REPO_ROOT)}:{lineno}")
    assert not offenders, ", ".join(offenders)


def test_js_does_not_emit_style_attributes_or_css_text() -> None:
    offenders = []
    for path in sorted(STATIC.glob("*.js")):
        for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            stripped = line.strip()
            if stripped.startswith(("*", "//", "/*")):
                continue
            if re.search(r"""\sstyle=|\.cssText|setAttribute\(\s*["']style["']""", line):
                offenders.append(f"{path.relative_to(REPO_ROOT)}:{lineno}")
    assert not offenders, ", ".join(offenders)


def test_helper_is_loaded_on_every_page_head() -> None:
    head = (TEMPLATES / "_head_css.html").read_text(encoding="utf-8")
    assert "style_data.js" in head and "nonce=\"{{ csp_nonce }}\"" in head
    js = (STATIC / "style_data.js").read_text(encoding="utf-8")
    for attr in ("data-bg", "data-w", "data-bg-color"):
        assert attr in js
    # Used attributes are the ones the helper understands.
    used = set()
    for path in TEMPLATES.rglob("*.html"):
        used |= set(re.findall(r"\b(data-(?:bg|w|bg-color))=", path.read_text(encoding="utf-8")))
    assert used and used <= {"data-bg", "data-w", "data-bg-color"}


def test_marker_and_notice_classes_exist() -> None:
    css = (STATIC / "css" / "17-inline-style-classes.css").read_text(encoding="utf-8")
    for cls in ("find-dealers-marker-pin--db", "find-dealers-marker-pin--other", "compare-evict-notice"):
        assert f".{cls}{{" in css
    assert "find-dealers-marker-pin--db" in (STATIC / "car_dealer_map.js").read_text(encoding="utf-8")


def test_style_attr_report_only_policy(monkeypatch) -> None:
    from flask import Flask, g

    app = Flask(__name__)
    monkeypatch.setenv("CSP_ENFORCE", "1")
    for flag, expect in (("", False), ("1", True)):
        monkeypatch.setenv("CSP_STYLE_ATTR_REPORT_ONLY", flag)
        with app.test_request_context("/"):
            g.csp_nonce = "n0nce"
            resp = security._csp_headers(app.response_class("x"))
        assert ("style-src-attr 'none'" in resp.headers.get("Content-Security-Policy-Report-Only", "")) is expect
        assert "style-src-attr" not in resp.headers["Content-Security-Policy"]
