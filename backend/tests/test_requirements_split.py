"""Requirements are split per image (P15B.2) and every Dockerfile copies them all.

Static only: no Docker, no network. requirements.txt stays the dev/CI/discovery
superset by including the per-image files, so pip, CI and the hygiene test keep
one entry point.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from backend.tests.test_requirements_hygiene import declared_distributions

ROOT = Path(__file__).resolve().parents[2]
WEB = ROOT / "requirements-web.txt"
BROWSER = ROOT / "requirements-browser.txt"
SCANNER = ROOT / "requirements-scanner.txt"
BROWSER_DISTS = {"playwright", "playwright-stealth"}


def _includes(path: Path) -> list[str]:
    return re.findall(r"^-r\s+(\S+)", path.read_text(encoding="utf-8"), re.M)


def _own(path: Path) -> set[str]:
    # Distributions the file itself declares (no -r following).
    out = set()
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.split("#", 1)[0].strip()
        m = re.match(r"^([A-Za-z0-9][A-Za-z0-9._-]*)", line)
        if m and not line.startswith("-"):
            out.add(re.sub(r"[-_.]+", "-", m.group(1)).lower())
    return out


def test_top_level_requirements_includes_both_image_files():
    assert sorted(_includes(ROOT / "requirements.txt")) == ["requirements-browser.txt", "requirements-web.txt"]
    assert not _own(ROOT / "requirements.txt")


def test_browser_file_holds_only_the_browser_stack():
    assert _own(BROWSER) == BROWSER_DISTS


def test_web_file_has_no_browser_stack():
    assert not _own(WEB) & BROWSER_DISTS
    assert not _includes(WEB)


def test_superset_covers_web_and_browser_exactly():
    assert set(declared_distributions(ROOT / "requirements.txt")) == _own(WEB) | _own(BROWSER)


def test_scanner_file_stays_standalone_and_browser_free():
    assert not _includes(SCANNER)
    assert not _own(SCANNER) & BROWSER_DISTS


@pytest.mark.parametrize("name", ["Dockerfile.web", "Dockerfile.discovery", "Dockerfile.scanner"])
def test_every_dockerfile_copies_requirements_glob(name):
    body = (ROOT / name).read_text(encoding="utf-8")
    assert re.search(r"^COPY requirements\*\.txt \./?$", body, re.M), name
    assert not re.search(r"^COPY requirements(-\w+)?\.txt\b", body, re.M), name


def _installed_file(name: str) -> str:
    m = re.search(r"pip install[^\n]*?-r (requirements[\w-]*\.txt)", (ROOT / name).read_text(encoding="utf-8"))
    assert m, name
    return m.group(1)


def test_images_install_their_own_file():
    assert _installed_file("Dockerfile.web") == "requirements-web.txt"
    assert _installed_file("Dockerfile.scanner") == "requirements-scanner.txt"
    # Discovery installs scanner + browser (P15B.5); see test_dockerfiles.py.
    assert _installed_file("Dockerfile.discovery") == "requirements-scanner.txt"


def test_dockerignore_does_not_exclude_requirements():
    ignore = (ROOT / ".dockerignore").read_text(encoding="utf-8").splitlines()
    assert not [l for l in ignore if l.strip().startswith("requirements")]


def test_sweep_profiles():
    import importlib.util

    spec = importlib.util.spec_from_file_location("sweep", ROOT / "scripts" / "scanner_import_sweep.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    assert set(mod.PROFILES) == {"scanner", "web", "discovery"}
    assert mod.PROFILES["scanner"]["allowed"] is mod.ALLOWED_MISSING
    assert {"playwright", "playwright_stealth"} <= mod.PROFILES["web"]["allowed"]
    assert not {"playwright", "playwright_stealth"} & mod.PROFILES["discovery"]["allowed"]
