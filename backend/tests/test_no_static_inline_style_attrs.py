"""Static ``style="..."`` attributes in templates must be classes (P16.7).

Attributes that interpolate a value ({{ }} / {% %}) are P16.8's (data-* plus
CSSOM) and are listed in ``DYNAMIC_ALLOWED`` until that unit lands.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
TEMPLATES = REPO_ROOT / "frontend" / "templates"
CSS = REPO_ROOT / "frontend" / "static" / "css" / "17-inline-style-classes.css"

_STYLE_ATTR = re.compile(r"""\sstyle=(["'])(.*?)\1""", re.S)


def test_no_static_style_attributes_in_templates() -> None:
    offenders = []
    for path in sorted(TEMPLATES.rglob("*.html")):
        text = path.read_text(encoding="utf-8")
        for m in _STYLE_ATTR.finditer(text):
            body = m.group(2)
            if "{{" in body or "{%" in body:
                continue
            line = text.count("\n", 0, m.start()) + 1
            offenders.append(f"{path.relative_to(REPO_ROOT)}:{line}")
    assert not offenders, "static style attributes must be classes: " + ", ".join(offenders)


def test_style_class_sheet_is_loaded_last_and_defines_every_used_class() -> None:
    head = (TEMPLATES / "_head_css.html").read_text(encoding="utf-8")
    assert head.rstrip().endswith("17-inline-style-classes.css') }}?v={{ static_cache_ver }}\">")
    css = CSS.read_text(encoding="utf-8")
    defined = set(re.findall(r"\.(u-[a-z0-9-]+)\{", css))
    used = set()
    for path in TEMPLATES.rglob("*.html"):
        used |= set(re.findall(r"\bu-[a-z0-9-]+", path.read_text(encoding="utf-8")))
    assert used and used <= defined, sorted(used - defined)
