"""Guard: tracked .py files must not use Python 3.12-only syntax (PEP 701 f-strings).

``ast.parse(feature_version=(3, 11))`` does not reject PEP 701 f-strings on a 3.12 parser, so
besides that parse this walks every f-string replacement field and flags a backslash, a
comment, or the enclosing quote character inside it (all SyntaxErrors before 3.12).
"""
import ast
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _problems(src: str, rel: str) -> list[str]:
    tree = ast.parse(src, filename=rel, feature_version=(3, 11))
    out: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.JoinedStr):
            continue
        whole = ast.get_source_segment(src, node) or ""
        stripped = whole.lstrip("fFrRbBuU")
        quote = stripped[:1]
        for part in node.values:
            if not isinstance(part, ast.FormattedValue):
                continue
            seg = ast.get_source_segment(src, part.value) or ""
            if "\\" in seg or "#" in seg or (quote and quote in seg and not stripped.startswith(quote * 3)):
                out.append(f"{rel}:{part.value.lineno}: 3.12-only f-string field {seg!r}")
    return out


def test_tracked_python_is_311_compatible():
    files = subprocess.run(
        ["git", "ls-files", "*.py"], cwd=ROOT, capture_output=True, text=True, check=True
    ).stdout.split()
    bad: list[str] = []
    for rel in files:
        path = ROOT / rel
        if not path.is_file():
            continue
        try:
            bad += _problems(path.read_text(encoding="utf-8"), rel)
        except SyntaxError as exc:
            bad.append(f"{rel}:{exc.lineno}: {exc.msg}")
    assert not bad, "\n".join(bad)


def test_guard_detects_backslash_in_fstring_field():
    assert _problems('x = f"{re.sub(r\'\\s+\', \' \', n)}"\n', "t.py")
