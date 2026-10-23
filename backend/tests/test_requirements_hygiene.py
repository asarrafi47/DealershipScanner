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

Test-only packages (pytest, pyyaml, ...) live in requirements-test.txt (P2A.2),
which CI installs next to requirements.txt and no deploy image installs. A name
declared only there passes for imports under ``backend/tests/`` and fails
anywhere else, so runtime code cannot lean on a package the image lacks.

The reverse direction (P15B.1, D-DEP1) is checked too: every distribution that
requirements.txt or requirements-scanner.txt declares is imported somewhere, or
listed in ``DECLARED_NOT_IMPORTED`` with a reason. crawl4ai sat in
requirements.txt with zero importers and pinned lxml below the PYSEC-2026-87
fix, so an orphan is a security cost, not just image weight.
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
SCANNER_REQUIREMENTS = REPO_ROOT / "requirements-scanner.txt"
TEST_REQUIREMENTS = REPO_ROOT / "requirements-test.txt"
RUNTIME_REQUIREMENTS = (REQUIREMENTS, SCANNER_REQUIREMENTS)
ALL_REQUIREMENTS = (REQUIREMENTS, SCANNER_REQUIREMENTS, TEST_REQUIREMENTS)
SCANNER_IMPORT_SWEEP = REPO_ROOT / "scripts" / "scanner_import_sweep.py"
TEST_CODE_PREFIX = "backend/tests/"
SCAN_DIRS = ("backend", "scripts")
SKIP_DIR_NAMES = {"__pycache__", "node_modules", ".venv", "venv", "site-packages"}

# Imported but deliberately not declared. Keep this short; every entry needs a
# reason, and test_allowlist_has_no_stale_entries fails once an entry is
# declared or no longer imported, so the list only ever shrinks.
ALLOWED_UNDECLARED: dict[str, str] = {
    # backend/scripts/image_downloader.py (offline script). Hard dependency of
    # anthropic 0.x (1.x moved to httpx2) and huggingface-hub (sentence-transformers).
    # P15B.1 removed crawl4ai, its third path; a fresh resolve of requirements.txt
    # on 2026-10-18 still installs it through those two, so it stays undeclared.
    "httpx": "transitive: anthropic<1, huggingface-hub (sentence-transformers)",
    # backend/db/geo.py, backend/vision/claude_vision.py. pgeocode pulls
    # pandas -> numpy; sentence-transformers also requires it (pgvector 0.5 no
    # longer does: fresh resolve of requirements.txt, 2026-10-18).
    "numpy": "transitive: pgeocode (pandas), sentence-transformers",
    # backend/main.py ProxyFix, backend/web/static.py safe_join, error types.
    # Flask hard-requires Werkzeug and its API is part of Flask's.
    "werkzeug": "transitive: flask",
    # backend/tests/test_css_url_filter.py (P3.5): jinja2.Undefined and
    # markupsafe.Markup drive the css_url filter the way templates call it.
    # Flask hard-requires Jinja2 (which hard-requires MarkupSafe); both are the
    # template engine Flask renders with.
    "jinja2": "transitive: flask",
    "markupsafe": "transitive: flask (jinja2)",
}

# Distribution name (PEP 503 normalised) -> import names, only where the import
# name is not simply the normalised distribution name with "-" -> "_".
DIST_IMPORT_NAMES: dict[str, set[str]] = {
    "beautifulsoup4": {"bs4"},
    "pillow": {"PIL"},
    "python-dotenv": {"dotenv"},
    "pyjwt": {"jwt"},
    "pymupdf": {"fitz", "pymupdf"},
    "pyyaml": {"yaml"},
}

# Declared in requirements.txt / requirements-scanner.txt but imported nowhere
# under backend/ or scripts/. Same rules as ALLOWED_UNDECLARED: every entry has a
# reason, and test_declared_not_imported_has_no_stale_entries fails once an entry
# is imported or no longer declared.
DECLARED_NOT_IMPORTED: dict[str, str] = {
    "gunicorn": "WSGI server, run as a CLI: scripts/docker-entrypoint-web.sh, Dockerfile.web",
    "idna": "security floor (SEC-076): requests, httpx, anyio and yarl require it",
    "pygments": "security floor (SEC-076): rich (transformers -> typer) and pytest require it",
    "lxml": "requirements-scanner.txt floor >=6.1.0 (PYSEC-2026-87); every BeautifulSoup call names html.parser",
}

# D-DEP1 orphans that P15B.1 removed (2026-10-18): nothing imports them, and
# crawl4ai held lxml below the PYSEC-2026-87 fix. patchright was only crawl4ai's
# browser driver. Re-adding one needs a real importer and a new decision.
REMOVED_ORPHANS = (
    "crawl4ai",
    "langchain-core",
    "langchain-openai",
    "langchain-text-splitters",
    "langsmith",
    "ollama",
    "patchright",
    "pyotp",
    "segno",
)

# lxml releases below this carry PYSEC-2026-87 (fixed in 6.1.0).
LXML_FIXED = "6.1.0"

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


def undeclared_imports(
    imports: dict[str, list[str]],
    declared: set[str],
    declared_for_tests: set[str],
    allowed=ALLOWED_UNDECLARED,
) -> dict[str, list[str]]:
    """Import name -> the sites no declaration covers. A requirements-test.txt
    name covers only sites under ``backend/tests/``."""
    missing: dict[str, list[str]] = {}
    for name, sites in sorted(imports.items()):
        if name in declared or name in allowed:
            continue
        if name in declared_for_tests:
            sites = [s for s in sites if not s.startswith(TEST_CODE_PREFIX)]
        if sites:
            missing[name] = sites
    return missing


@pytest.fixture(scope="module")
def declared() -> set[str]:
    return declared_import_names()


@pytest.fixture(scope="module")
def declared_for_tests() -> set[str]:
    return declared_import_names(TEST_REQUIREMENTS)


def test_every_third_party_import_is_declared(imports, declared, declared_for_tests):
    missing = undeclared_imports(imports, declared, declared_for_tests)
    assert not missing, (
        "third-party imports not declared in requirements.txt (declare them there, in "
        "requirements-test.txt when only backend/tests/ imports them, or add them to "
        "ALLOWED_UNDECLARED with a reason):\n"
        + "\n".join(f"  {name}: {', '.join(sites[:5])}" for name, sites in missing.items())
    )


def test_allowlist_has_no_stale_entries(imports, declared, declared_for_tests):
    now_declared = sorted(
        n
        for n in ALLOWED_UNDECLARED
        if not undeclared_imports({n: imports.get(n, ["?"])}, declared, declared_for_tests, allowed={})
    )
    assert not now_declared, f"declared now, drop from ALLOWED_UNDECLARED: {now_declared}"
    unused = sorted(n for n in ALLOWED_UNDECLARED if n not in imports)
    assert not unused, f"no longer imported, drop from ALLOWED_UNDECLARED: {unused}"


@pytest.mark.parametrize("dist,floor", sorted(P1A6_DECLARED.items()))
def test_p1a6_packages_declared_with_floor(dist, floor):
    line = declared_distributions().get(dist)
    assert line is not None, f"{dist} is imported by the app but missing from requirements.txt"
    assert f">={floor}" in line.replace(" ", ""), f"{dist}: expected a >={floor} floor, got {line!r}"


def _release(text: str) -> tuple[int, ...]:
    """Release tuple with trailing zeros dropped, so 1, 1.0 and 1.0.0 compare equal."""
    parts = [int(p) for p in text.split(".")]
    while len(parts) > 1 and parts[-1] == 0:
        parts.pop()
    return tuple(parts)


_SPEC_CLAUSE = re.compile(r"^\s*(===|==|~=|<=|<)\s*(\d+(?:\.\d+)*)(\.\*)?\s*$")


def _caps_below_major(requirement: str, major: int) -> bool:
    """True when one specifier clause alone excludes every release >= ``major``.0.0.

    Fails closed: a clause it cannot read (pre-release suffixes, ``!=``, lower
    bounds) never counts as a cap."""
    spec = requirement.split(";", 1)[0]  # drop environment markers
    spec = re.sub(r"^[A-Za-z0-9._-]+(\[[^\]]*\])?", "", spec.strip())  # drop name and extras
    bound = (major,)
    for clause in spec.split(","):
        m = _SPEC_CLAUSE.match(clause)
        if not m:
            continue
        op, release, wildcard = m.group(1), _release(m.group(2)), bool(m.group(3))
        if op == "<" and release <= bound:  # <1, <1.0, <0.200; not <1.0.1 (admits 1.0.0)
            return True
        if op == "<=" and release < bound:  # <=0.999; not <=1 (admits 1.0.0)
            return True
        if op in {"==", "==="} and (release[0] < major if wildcard else release < bound):
            return True
        if op == "~=" and release[0] < major:  # ~=0.116 is >=0.116,==0.*; ~=5.3 is ==5.*
            return True
    return False


def _caps_below_major_1(requirement: str) -> bool:
    return _caps_below_major(requirement, 1)


_LOWER_CLAUSE = re.compile(r"^\s*(>=|==|===|~=)\s*(\d+(?:\.\d+)*)\s*$")


def _floor(requirement: str) -> tuple[int, ...] | None:
    """Highest ``>=`` / ``==`` / ``~=`` release in the specifier, or None."""
    spec = requirement.split(";", 1)[0]
    spec = re.sub(r"^[A-Za-z0-9._-]+(\[[^\]]*\])?", "", spec.strip())
    floors = [_release(m.group(2)) for c in spec.split(",") if (m := _LOWER_CLAUSE.match(c))]
    return max(floors) if floors else None


def _sends_temperature(source: str) -> bool:
    """True when the module can put ``temperature`` into an SDK request: a
    parameter, a keyword argument or a string literal of that name anywhere in
    it. Deliberately broad (``kwargs["temperature"]``, ``kwargs.update(temperature=t)``,
    ``{"temperature": t}``, ``setdefault("temperature", t)`` all count), so a
    refactor cannot hide the forwarding from the cap guard below."""
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.arg) and node.arg == "temperature":
            return True
        if isinstance(node, ast.keyword) and node.arg == "temperature":
            return True
        if isinstance(node, ast.Constant) and node.value == "temperature":
            return True
    return False


def test_anthropic_capped_below_1_while_the_client_sends_temperature():
    """anthropic 1.x dropped ``temperature`` from ``Messages.create`` (measured
    2026-10-07: 1.12.0 raises ``TypeError: ... unexpected keyword argument
    'temperature'``). backend/llm/client.py forwards it, and car chat
    (backend/utils/llm_client._complete_claude) always passes one, so an
    uncapped floor would install 1.x and break every chat call.

    Fails closed, never skips: lifting the cap is allowed only once no
    ``temperature`` parameter, keyword or literal is left in the client."""
    client_src = (REPO_ROOT / "backend" / "llm" / "client.py").read_text(encoding="utf-8")
    line = declared_distributions()["anthropic"]
    assert _caps_below_major_1(line) or not _sends_temperature(client_src), (
        f"anthropic must stay below 1.0 while backend/llm/client.py sends temperature: {line!r}"
    )


def test_the_client_really_sends_temperature_today():
    # Pins the premise of the cap guard: if this fails, the detector went blind,
    # not the client. Update it together with the anthropic 1.x migration.
    client_src = (REPO_ROOT / "backend" / "llm" / "client.py").read_text(encoding="utf-8")
    assert _sends_temperature(client_src)


@pytest.mark.parametrize(
    "source,sends",
    [
        ('kwargs["temperature"] = t\n', True),
        ("kwargs['temperature'] = t\n", True),
        ("kwargs.update(temperature=t)\n", True),
        ('kwargs.setdefault("temperature", t)\n', True),
        ('req = {"model": m, "temperature": t}\n', True),
        ("def complete(prompt, *, temperature=None):\n    pass\n", True),
        ("client.messages.create(model=m, temperature=0.2)\n", True),
        ('kwargs = {"model": m, "max_tokens": 10}\n', False),
        ("def complete(prompt, *, top_k=None):\n    pass\n", False),
    ],
)
def test_sends_temperature_detector(source, sends):
    assert _sends_temperature(source) is sends


@pytest.mark.parametrize(
    "requirement,capped",
    [
        ("anthropic>=0.116,<1", True),
        ("anthropic>=0.116, <1.0", True),
        ("anthropic>=0.116,<1.0.0", True),
        ("anthropic>=0.116,<0.200", True),
        ("anthropic~=0.116", True),
        ("anthropic~=0.116.0", True),
        ("anthropic==0.125.0", True),
        ("anthropic==0.*", True),
        ("anthropic<=0.999", True),
        ("anthropic[bedrock]>=0.116,<1", True),
        ("anthropic>=0.116,<1; python_version >= '3.9'", True),
        ("anthropic>=0.116", False),
        ("anthropic>=0.116,<2", False),
        ("anthropic>=0.116,<1.0.1", False),
        ("anthropic<=1", False),
        ("anthropic<=1.0.0", False),
        ("anthropic==1.*", False),
        ("anthropic~=1.0", False),
        ("anthropic>=0.116,!=1.0.0", False),
        ("anthropic<1.0.0a1", False),
    ],
)
def test_caps_below_major_1_parser(requirement, capped):
    assert _caps_below_major_1(requirement) is capped


def test_test_requirements_cover_test_code_only():
    imports = {
        "yaml": ["backend/tests/test_ci_workflow.py:31"],
        "pytest": ["backend/tests/test_a.py:1", "backend/web/routes.py:2"],
        "requests": ["backend/net/client.py:1"],
        "numpy": ["backend/db/geo.py:3"],
    }
    missing = undeclared_imports(imports, {"requests"}, {"yaml", "pytest"}, allowed={"numpy": "x"})
    # Runtime code importing a test-only package is still caught.
    assert missing == {"pytest": ["backend/web/routes.py:2"]}


def test_requirements_test_declares_the_test_tooling():
    # pytest is no longer an ALLOWED_UNDECLARED entry: requirements-test.txt
    # declares it, and pyyaml backs backend/tests/test_ci_workflow.py.
    assert {"pytest", "yaml", "pytest_timeout"} <= declared_import_names(TEST_REQUIREMENTS)


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


# --- P15B.1 (D-DEP1): orphans out, lxml past PYSEC-2026-87 --------------------


def _declared_anywhere(dist: str) -> list[str]:
    return [
        f"{path.relative_to(REPO_ROOT).as_posix()}: {line}"
        for req in ALL_REQUIREMENTS
        for path, line in _requirement_lines(req)
        if (m := _REQ_NAME.match(line)) and _normalise(m.group(1)) == dist
    ]


@pytest.mark.parametrize("dist", REMOVED_ORPHANS)
def test_removed_orphans_stay_out(dist):
    found = _declared_anywhere(dist)
    assert not found, f"{dist} was removed by P15B.1 (D-DEP1, no importer); declared again in: {found}"


def test_crawl4ai_not_declared():
    # Named by the P15B.1 spec: crawl4ai pinned lxml~=5.3 and forced the
    # PYSEC-2026-87 pip-audit ignore.
    assert _declared_anywhere("crawl4ai") == []


@pytest.mark.parametrize("req", ALL_REQUIREMENTS, ids=lambda p: p.name)
def test_no_requirement_caps_lxml_below_6(req):
    for _path, line in _requirement_lines(req):
        m = _REQ_NAME.match(line)
        if m and _normalise(m.group(1)) == "lxml":
            assert not _caps_below_major(line, 6), f"{req.name}: lxml capped below 6 (PYSEC-2026-87): {line!r}"
            floor = _floor(line)
            assert floor is not None and floor >= _release(LXML_FIXED), (
                f"{req.name}: lxml needs a >={LXML_FIXED} floor (PYSEC-2026-87): {line!r}"
            )


def test_scanner_requirements_lift_lxml_to_the_fix():
    # The scanner image installs requirements-scanner.txt alone; its lxml line
    # was >=5.3 until P15B.1.
    line = declared_distributions(SCANNER_REQUIREMENTS).get("lxml")
    assert line is not None and (_floor(line) or (0,)) >= _release(LXML_FIXED), line


def _runtime_declared() -> dict[str, str]:
    out: dict[str, str] = {}
    for req in RUNTIME_REQUIREMENTS:
        out.update(declared_distributions(req))
    return out


def _orphans(declared: dict[str, str], imports: dict[str, list[str]], allowed) -> list[str]:
    imported = set(imports)
    return sorted(
        dist
        for dist in declared
        if dist not in allowed and not (DIST_IMPORT_NAMES.get(dist, {dist.replace("-", "_")}) & imported)
    )


def test_every_declared_runtime_distribution_is_imported(imports):
    orphans = _orphans(_runtime_declared(), imports, DECLARED_NOT_IMPORTED)
    assert not orphans, (
        "declared in requirements.txt / requirements-scanner.txt but imported nowhere under "
        "backend/ or scripts/ (remove it after a consumer audit, or add it to "
        f"DECLARED_NOT_IMPORTED with a reason): {orphans}"
    )


def test_declared_not_imported_has_no_stale_entries(imports):
    declared = _runtime_declared()
    gone = sorted(d for d in DECLARED_NOT_IMPORTED if d not in declared)
    assert not gone, f"no longer declared, drop from DECLARED_NOT_IMPORTED: {gone}"
    imported_now = sorted(
        d for d in DECLARED_NOT_IMPORTED if DIST_IMPORT_NAMES.get(d, {d.replace("-", "_")}) & set(imports)
    )
    assert not imported_now, f"imported now, drop from DECLARED_NOT_IMPORTED: {imported_now}"


def test_orphan_detector_flags_unimported_declarations():
    declared = {"flask": "flask>=3", "crawl4ai": "crawl4ai>=0.8", "gunicorn": "gunicorn>=21", "pillow": "pillow>=10"}
    imports = {"flask": ["backend/main.py:1"], "PIL": ["backend/x.py:2"]}
    assert _orphans(declared, imports, {"gunicorn": "cli"}) == ["crawl4ai"]


def _sweep_allowed_missing() -> set[str]:
    tree = ast.parse(SCANNER_IMPORT_SWEEP.read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(getattr(t, "id", None) == "ALLOWED_MISSING" for t in node.targets):
            return set(ast.literal_eval(node.value))
    raise AssertionError("ALLOWED_MISSING not found in scripts/scanner_import_sweep.py")


def test_scanner_sweep_allowlist_names_only_imported_packages(imports):
    # A name nothing imports can never be the missing module the sweep reports,
    # so it only hides a future regression. P15B.1 pruned the crawl4ai-era list.
    stale = sorted(n for n in _sweep_allowed_missing() if n not in imports)
    assert not stale, f"scripts/scanner_import_sweep.py ALLOWED_MISSING names packages nothing imports: {stale}"


@pytest.mark.parametrize(
    "requirement,major,capped",
    [
        ("lxml~=5.3", 6, True),
        ("lxml~=5.3.0", 6, True),
        ("lxml>=5.3,<6", 6, True),
        ("lxml<6.0", 6, True),
        ("lxml<=5.9", 6, True),
        ("lxml==5.4.0", 6, True),
        ("lxml==5.*", 6, True),
        ("lxml>=6.1.0", 6, False),
        ("lxml~=6.1", 6, False),
        ("lxml==6.*", 6, False),
        ("lxml<7", 6, False),
        ("lxml<6.0.1", 6, False),
        ("lxml>=5.3", 6, False),
    ],
)
def test_caps_below_major_parser(requirement, major, capped):
    assert _caps_below_major(requirement, major) is capped


@pytest.mark.parametrize(
    "requirement,floor",
    [
        ("lxml>=6.1.0", (6, 1)),
        ("lxml>=5.3", (5, 3)),
        ("lxml~=6.1", (6, 1)),
        ("lxml>=5.3,<6", (5, 3)),
        ("lxml<7", None),
        ("lxml", None),
    ],
)
def test_floor_parser(requirement, floor):
    assert _floor(requirement) == floor
