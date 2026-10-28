"""P17C.6: every env var backend/scanner touches is registered in env_knobs.KNOBS (D-OD11)."""

from __future__ import annotations

import ast
import textwrap
from pathlib import Path

from backend.scanner import env_knobs

_REPO = Path(__file__).resolve().parents[2]
_SCANNER = _REPO / "backend" / "scanner"


def _touched(found: dict) -> set[str]:
    return set(found["reads"]) | set(found["sets"])


def test_every_env_name_the_scanner_touches_is_registered():
    found = env_knobs.scan_sources()
    unregistered = sorted(_touched(found) - set(env_knobs.KNOBS))
    assert not unregistered, f"add to backend/scanner/env_knobs.py KNOBS: {unregistered}"


def test_no_unaccounted_dynamic_env_access():
    found = env_knobs.scan_sources()
    stray = {f: ln for f, ln in found["dynamic"].items() if f not in env_knobs.DYNAMIC_SITES}
    assert not stray, f"env access with a non-constant name; use a constant or list it in DYNAMIC_SITES: {stray}"
    for rel, spec in env_knobs.DYNAMIC_SITES.items():
        src = (_SCANNER / rel).read_text(encoding="utf-8")
        for name in (*spec["reads"], *spec["sets"]):
            assert f'"{name}"' in src, f"{rel} no longer mentions {name}; update DYNAMIC_SITES"


def test_registered_name_has_a_reader_or_is_a_retirement_candidate():
    found = env_knobs.scan_sources()
    for name, knob in env_knobs.KNOBS.items():
        in_code = found["reads"].get(name, set())
        assert tuple(sorted(in_code)) == knob.readers, f"{name}: readers drifted, code says {sorted(in_code)}"
        if not knob.readers:
            assert knob.category == "browser_era", f"{name} has no reader and is not marked browser_era"
            assert name in found["sets"], f"{name} has no reader and nothing sets it: delete it"


def test_every_knob_has_a_category_and_doc():
    for name, knob in env_knobs.KNOBS.items():
        assert knob.category in env_knobs.CATEGORIES, name
        assert knob.doc.strip(), name
        assert knob.type in {"bool", "int", "float", "str", "path", "csv"}, name


def test_set_in_files_exist_and_mention_the_name():
    found = env_knobs.scan_sources()
    for name, knob in env_knobs.KNOBS.items():
        for rel in knob.set_in:
            p = _REPO / rel
            assert p.is_file(), f"{name}: {rel} is gone"
            assert name in p.read_text(encoding="utf-8", errors="ignore"), f"{name}: {rel} does not mention it"
        for rel in found["sets"].get(name, ()):
            assert f"backend/scanner/{rel}" in knob.set_in, f"{name}: scanner code sets it in {rel}"


def test_generated_doc_is_current():
    assert env_knobs.DOC_PATH.read_text(encoding="utf-8") == env_knobs.render_table(), (
        "run: python -m backend.scanner.env_knobs --write-doc"
    )


def test_lint_fails_on_an_unregistered_knob(tmp_path):
    (tmp_path / "m.py").write_text(
        textwrap.dedent(
            """
            import os
            A = "SCANNER_NOT_REGISTERED_ONE"
            def f():
                return os.environ.get(A), os.getenv("SCANNER_NOT_REGISTERED_TWO"), os.environ["SCANNER_NOT_REGISTERED_THREE"]
            def g(n):
                return os.environ.get(n)
            def h():
                return g("SCANNER_NOT_REGISTERED_FOUR")
            """
        )
    )
    found = env_knobs.scan_sources(tmp_path)
    assert _touched(found) == {f"SCANNER_NOT_REGISTERED_{n}" for n in ("ONE", "TWO", "THREE", "FOUR")}
    assert not found["dynamic"]  # g's own body is a wrapper, not a stray dynamic read


def test_lint_flags_a_dynamic_read_outside_a_wrapper(tmp_path):
    (tmp_path / "m.py").write_text("import os\nNAMES = ('A',)\nx = [os.environ.get(n) for n in NAMES]\n")
    assert env_knobs.scan_sources(tmp_path)["dynamic"] == {"m.py": [3]}


def test_registry_module_parses_without_importing_the_scanner():
    tree = ast.parse((_SCANNER / "env_knobs.py").read_text(encoding="utf-8"))
    imported = {a.name.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names}
    imported |= {n.module.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.module}
    assert imported <= {"__future__", "ast", "sys", "dataclasses", "pathlib"}
