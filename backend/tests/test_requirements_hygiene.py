"""Every third-party package the code imports is declared in requirements.txt.

Remediation plan P1A.6 (security-0). backend/llm/client.py imported the
``anthropic`` SDK for months while nothing installed it, so prod car chat could
not reach Claude. ``pdfplumber`` (window stickers, brochure extraction) and
``brotli`` (the Dockerfile.web precompression step) had the same gap: they
arrived only as somebody else's transitive dependency, or not at all.

The check is static. It parses every ``.py`` file under ``backend/`` and
``scripts/`` with ``ast`` (nothing is imported, nothing touches the network)
and collects each import's top-level name, including imports inside functions
and inside ``try: ... except ImportError`` blocks, plus string-literal
``importlib.import_module("x")`` / ``__import__("x")`` calls. A name passes when
it is stdlib, first-party (this repo), declared in requirements.txt (following
``-r`` includes), or listed in ``ALLOWED_UNDECLARED`` below with a reason.
"""
from __future__ import annotations

import ast
import re
import sys
from collections import defaultdict
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
REQUIREMENTS = REPO_ROOT / "requirements.txt"
SCAN_DIRS = ("backend", "scripts")
SKIP_DIR_NAMES = {"__pycache__", "node_modules", ".venv", "venv", "site-packages"}

# Imported but deliberately not declared. Keep this short; every entry needs a
# reason, and test_allowlist_has_no_stale_entries fails once an entry is
# declared or no longer imported, so the list only ever shrinks.
ALLOWED_UNDECLARED: dict[str, str] = {
    # backend/scripts/image_downloader.py (offline script). Hard dependency of
    # anthropic 0.x (1.x moved to httpx2), huggingface_hub (sentence-transformers)
    # and crawl4ai. P15B.1 declares it explicitly if the crawl4ai removal needs that.
    "httpx": "transitive: anthropic<1, sentence-transformers, crawl4ai",
    # backend/db/geo.py, backend/vision/claude_vision.py. pgeocode pulls
    # pandas -> numpy; pgvector and sentence-transformers also require it.
    "numpy": "transitive: pgeocode, pgvector, sentence-transformers",
    # backend/main.py ProxyFix, backend/web/static.py safe_join, error types.
    # Flask hard-requires Werkzeug and its API is part of Flask's.
    "werkzeug": "transitive: flask",
    # Test-only. requirements.txt is the runtime / deploy-image file; CI runs
    # `pip install pytest` after it (.github/workflows/ci.yml). P2A.2 adds
    # requirements-test.txt.
    "pytest": "test-only: CI installs it separately",
}

# Distribution name (PEP 503 normalised) -> import names, only where the import
# name is not simply the normalised distribution name with "-" -> "_".
DIST_IMPORT_NAMES: dict[str, set[str]] = {
    "beautifulsoup4": {"bs4"},
    "pillow": {"PIL"},
    "python-dotenv": {"dotenv"},
    "pyjwt": {"jwt"},
    "pymupdf": {"fitz", "pymupdf"},
}

# The packages P1A.6 declares, with their floors. Pinned here so a later
# requirements reshuffle (P15B.1 / P15B.2) cannot silently drop one again.
P1A6_DECLARED = {
    "anthropic": "0.116",  # backend/llm/client.py: the only Claude transport
    "pdfplumber": "0.11",  # post_scan/window_sticker.py, enrichment/brochure_extract.py
    "brotli": "1.1",  # scripts/build_static_compressed.py in the Dockerfile.web build
}

_REQ_NAME = re.compile(r"^([A-Za-z0-9][A-Za-z0-9._-]*)")


def _normalise(dist: str) -> str:
    return re.sub(r"[-_.]+", "-", dist).lower()


def _requirement_lines(path: Path, _seen: set[Path] | None = None) -> list[tuple[Path, str]]:
    """(file, requirement text) for every requirement line, following ``-r``."""
    seen = _seen if _seen is not None else set()
    path = path.resolve()
    if path in seen:
        return []
    seen.add(path)
    out: list[tuple[Path, str]] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if line.startswith("#"):
            continue
        line = re.split(r"\s+#", line, maxsplit=1)[0].strip()
        if not line:
            continue
        include = re.match(r"^(?:-r|--requirement)(?:\s+|=)(\S+)$", line)
        if include:
            out.extend(_requirement_lines(path.parent / include.group(1), seen))
            continue
        if line.startswith("-"):
            continue  # pip options (-e, --index-url, -c ...) declare no import name
        out.append((path, line))
    return out


def declared_distributions(path: Path = REQUIREMENTS) -> dict[str, str]:
    """Normalised distribution name -> its requirement line."""
    out: dict[str, str] = {}
    for _src, line in _requirement_lines(path):
        m = _REQ_NAME.match(line)
        if m:
            out[_normalise(m.group(1))] = line
    return out


def declared_import_names(path: Path = REQUIREMENTS) -> set[str]:
    names: set[str] = set()
    for dist in declared_distributions(path):
        names |= DIST_IMPORT_NAMES.get(dist, {dist.replace("-", "_")})
    return names


def _local_names(directory: Path) -> set[str]:
    """Module and package names importable from ``directory`` on sys.path."""
    names: set[str] = set()
    if not directory.is_dir():
        return names
    for child in directory.iterdir():
        if child.is_dir() and not child.name.startswith(".") and child.name not in SKIP_DIR_NAMES:
            names.add(child.name)
        elif child.suffix == ".py":
            names.add(child.stem)
    return names


def _python_files(root: Path, scan_dirs=SCAN_DIRS):
    for base in scan_dirs:
        for path in sorted((root / base).rglob("*.py")):
            rel_parts = path.relative_to(root).parts[:-1]
            if any(p in SKIP_DIR_NAMES or p.startswith(".") for p in rel_parts):
                continue
            yield path


def _imported_top_names(tree: ast.AST):
    """(top-level name, line) for every absolute import in the tree."""
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                yield alias.name.split(".")[0], node.lineno
        elif isinstance(node, ast.ImportFrom):
            if node.level == 0 and node.module:
                yield node.module.split(".")[0], node.lineno
        elif isinstance(node, ast.Call) and node.args:
            func = node.func
            fname = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", None)
            first = node.args[0]
            if (
                fname in {"import_module", "__import__"}
                and isinstance(first, ast.Constant)
                and isinstance(first.value, str)
                and first.value
                and not first.value.startswith(".")
            ):
                yield first.value.split(".")[0], node.lineno


def third_party_imports(root: Path = REPO_ROOT, scan_dirs=SCAN_DIRS) -> dict[str, list[str]]:
    """Top-level import name -> ["path:line", ...] for every non-stdlib,
    non-first-party import under ``scan_dirs``."""
    stdlib = set(sys.stdlib_module_names)
    # First party: the repo root and backend/ both end up on sys.path (scripts
    # insert one or the other), and a script run by path sees its own directory.
    first_party = _local_names(root) | _local_names(root / "backend")
    found: dict[str, list[tuple[str, int]]] = defaultdict(list)
    for path in _python_files(root, scan_dirs):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        siblings = _local_names(path.parent)
        for name, lineno in _imported_top_names(tree):
            if name in stdlib or name in first_party or name in siblings:
                continue
            found[name].append((path.relative_to(root).as_posix(), lineno))
    return {name: [f"{rel}:{line}" for rel, line in sorted(sites)] for name, sites in found.items()}


@pytest.fixture(scope="module")
def imports() -> dict[str, list[str]]:
    return third_party_imports()


@pytest.fixture(scope="module")
def declared() -> set[str]:
    return declared_import_names()


def test_every_third_party_import_is_declared(imports, declared):
    missing = {
        name: sites
        for name, sites in sorted(imports.items())
        if name not in declared and name not in ALLOWED_UNDECLARED
    }
    assert not missing, (
        "third-party imports not declared in requirements.txt (declare them, or add "
        "them to ALLOWED_UNDECLARED with a reason):\n"
        + "\n".join(f"  {name}: {', '.join(sites[:5])}" for name, sites in missing.items())
    )


def test_allowlist_has_no_stale_entries(imports, declared):
    now_declared = sorted(n for n in ALLOWED_UNDECLARED if n in declared)
    assert not now_declared, f"declared now, drop from ALLOWED_UNDECLARED: {now_declared}"
    unused = sorted(n for n in ALLOWED_UNDECLARED if n not in imports)
    assert not unused, f"no longer imported, drop from ALLOWED_UNDECLARED: {unused}"


@pytest.mark.parametrize("dist,floor", sorted(P1A6_DECLARED.items()))
def test_p1a6_packages_declared_with_floor(dist, floor):
    line = declared_distributions().get(dist)
    assert line is not None, f"{dist} is imported by the app but missing from requirements.txt"
    assert f">={floor}" in line.replace(" ", ""), f"{dist}: expected a >={floor} floor, got {line!r}"


def _caps_below_major_1(requirement: str) -> bool:
    """True when the requirement's specifiers exclude every 1.x release."""
    spec = requirement.split(";", 1)[0]  # drop environment markers
    spec = re.sub(r"^[A-Za-z0-9._-]+(\[[^\]]*\])?", "", spec.strip())  # drop name and extras
    for clause in spec.split(","):
        m = re.match(r"\s*(<=|<|==|~=)\s*(\d+)(?:\.(\d+))?", clause)
        if not m:
            continue
        op, major, minor = m.group(1), int(m.group(2)), int(m.group(3) or 0)
        if op == "<" and (major == 0 or (major == 1 and minor == 0)):
            return True
        if op in {"<=", "==", "~="} and major == 0:
            return True
    return False


def test_anthropic_capped_below_1_while_the_client_sends_temperature():
    """anthropic 1.x dropped ``temperature`` from ``Messages.create`` (measured
    2026-10-07: 1.12.0 raises ``TypeError: ... unexpected keyword argument
    'temperature'``). backend/llm/client.py forwards it, and car chat
    (backend/utils/llm_client._complete_claude) always passes one, so an
    uncapped floor would install 1.x and break every chat call. Lift the cap
    together with the client change."""
    client_src = (REPO_ROOT / "backend" / "llm" / "client.py").read_text(encoding="utf-8")
    if 'kwargs["temperature"]' not in client_src:
        pytest.skip("backend/llm/client.py no longer forwards temperature; revisit the anthropic<1 cap")
    line = declared_distributions()["anthropic"]
    assert _caps_below_major_1(line), f"anthropic must stay below 1.0 while the client sends temperature: {line!r}"


@pytest.mark.parametrize(
    "requirement,capped",
    [
        ("anthropic>=0.116,<1", True),
        ("anthropic>=0.116, <1.0", True),
        ("anthropic~=0.116", True),
        ("anthropic==0.125.0", True),
        ("anthropic>=0.116", False),
        ("anthropic>=0.116,<2", False),
    ],
)
def test_caps_below_major_1_parser(requirement, capped):
    assert _caps_below_major_1(requirement) is capped


@pytest.mark.parametrize("dist", sorted(P1A6_DECLARED))
def test_p1a6_packages_are_really_imported(imports, dist):
    # Guards the declarations above against becoming orphans nobody notices.
    assert dist in imports, f"{dist} is declared for P1A.6 but nothing under backend/ or scripts/ imports it"


def test_detector_flags_undeclared_and_ignores_local(tmp_path):
    """The scan is not vacuous: it sees function-level, optional and dynamic
    imports, and skips stdlib, relative and first-party ones."""
    (tmp_path / "backend" / "pkg").mkdir(parents=True)
    (tmp_path / "backend" / "__init__.py").write_text("")
    (tmp_path / "backend" / "pkg" / "__init__.py").write_text("")
    (tmp_path / "backend" / "pkg" / "sibling.py").write_text("")
    (tmp_path / "backend" / "pkg" / "mod.py").write_text(
        "import os, json\n"
        "from backend.pkg import sibling\n"
        "from . import sibling as s2\n"
        "import sibling\n"
        "import pkg.sibling\n"
        "import declaredpkg.sub\n"
        "def f():\n"
        "    import lazy_one\n"
        "try:\n"
        "    import optional_two\n"
        "except ImportError:\n"
        "    optional_two = None\n"
        "import importlib\n"
        "importlib.import_module('dynamic_three')\n"
        "__import__('json')\n"
    )
    (tmp_path / "scripts").mkdir()
    (tmp_path / "scripts" / "tool.py").write_text("from bs4 import BeautifulSoup\n")
    (tmp_path / "requirements.txt").write_text(
        "# comment\n-r extra.txt\nDeclaredPkg>=1  # trailing comment\n--index-url https://example.invalid\n"
    )
    (tmp_path / "extra.txt").write_text("beautifulsoup4>=4\n")

    found = third_party_imports(tmp_path)
    assert set(found) == {"declaredpkg", "lazy_one", "optional_two", "dynamic_three", "bs4"}
    assert found["lazy_one"] == ["backend/pkg/mod.py:8"]

    names = declared_import_names(tmp_path / "requirements.txt")
    assert {"declaredpkg", "bs4"} <= names
    assert {n for n in found if n not in names} == {"lazy_one", "optional_two", "dynamic_three"}
