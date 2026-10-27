"""The ten flat ``backend.scanner.<name>`` alias shims are gone; nothing may import them again.

Walks every .py file's AST for Import/ImportFrom nodes and for mock.patch / monkeypatch
string targets that name a removed module. The rooftop_match logger name is a log-channel
literal kept on purpose (``logging.getLogger``), so only patch-style strings are checked.
"""
from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
REMOVED = {
    "dealer_location", "dealer_profile", "dealer_site_url", "dealer_sticker_provider",
    "listing_gap_fill", "post_pipeline", "rooftop_match", "vdp_html_recovery",
    "vdp_spec_extract", "window_sticker",
}
PKG = "backend.scanner"
PATCH_ATTRS = {"patch", "setattr", "setitem", "object", "delattr"}


def _is_removed(dotted: str) -> bool:
    parts = dotted.split(".")
    return len(parts) >= 3 and ".".join(parts[:2]) == PKG and parts[2] in REMOVED


def _violations(path: Path) -> list[str]:
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except (SyntaxError, UnicodeDecodeError):
        return []
    out: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            out += [f"{path}:{node.lineno} import {a.name}" for a in node.names if _is_removed(a.name)]
        elif isinstance(node, ast.ImportFrom):
            mod = node.module or ""
            if node.level == 0 and _is_removed(mod):
                out.append(f"{path}:{node.lineno} from {mod}")
            elif node.level == 0 and mod == PKG:
                out += [f"{path}:{node.lineno} from {PKG} import {a.name}" for a in node.names if a.name in REMOVED]
        elif isinstance(node, ast.Call):
            fn = node.func
            name = fn.attr if isinstance(fn, ast.Attribute) else getattr(fn, "id", "")
            if name in PATCH_ATTRS:
                for arg in node.args[:1]:
                    if isinstance(arg, ast.Constant) and isinstance(arg.value, str) and _is_removed(arg.value):
                        out.append(f"{path}:{node.lineno} patch target {arg.value}")
    return out


def test_no_scanner_alias_shims() -> None:
    for name in REMOVED:
        assert not (ROOT / "backend" / "scanner" / f"{name}.py").exists(), name
    bad: list[str] = []
    for top in ("backend", "scripts", "deploy", "debug"):
        base = ROOT / top
        if base.is_dir():
            for p in base.rglob("*.py"):
                if p == Path(__file__).resolve() or ".venv" in p.parts:
                    continue
                bad += _violations(p)
    for p in ROOT.glob("*.py"):
        bad += _violations(p)
    assert not bad, "\n".join(bad)
