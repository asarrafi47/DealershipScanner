"""Static checks on the web and discovery Dockerfiles (P15B.4, P15B.5). No Docker, no network."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _body(name: str) -> str:
    return (ROOT / name).read_text(encoding="utf-8")


def _instructions(name: str) -> str:
    # Comment-free text, so prose that mentions a forbidden word cannot trip or mask a check.
    return "\n".join(l for l in _body(name).splitlines() if not l.lstrip().startswith("#"))


def test_web_installs_cpu_only_torch_before_requirements():
    body = _instructions("Dockerfile.web")
    m = re.search(r"pip install[^\n]*torch==[^\n]*--index-url https://download\.pytorch\.org/whl/cpu", body)
    assert m, "CPU torch install line missing"
    assert m.start() < body.index("requirements-web.txt")
    assert re.search(r"^ARG TORCH_VERSION=\d+\.\d+\.\d+$", body, re.M)


def test_web_has_no_nvidia_no_browser_and_copies_requirements_glob():
    body = _instructions("Dockerfile.web")
    assert "nvidia" not in body.lower()
    assert "cu12" not in body and "/whl/cu" not in body
    assert not re.search(r"playwright\s+install|patchright\s+install|chromium", body, re.I)
    assert re.search(r"^COPY requirements\*\.txt \./?$", body, re.M)


def test_discovery_installs_slim_scanner_plus_browser_files():
    body = _instructions("Dockerfile.discovery")
    m = re.search(r"pip install[^\n]*", body)
    assert m
    line = m.group(0)
    assert "-r requirements-scanner.txt" in line and "-r requirements-browser.txt" in line
    assert not re.search(r"-r requirements(-web)?\.txt", line)
    assert "torch" not in body and "sentence-transformers" not in body
    assert re.search(r"^COPY requirements\*\.txt \./?$", body, re.M)


def test_discovery_keeps_browser_install_and_worker_env():
    body = _instructions("Dockerfile.discovery")
    assert re.search(r"^RUN playwright install chromium", body, re.M)
    assert re.search(r"SCANNER_ALLOW_BROWSER=1", body)
    assert re.search(r"^ENV SCANNER_WORKER_JOB_TYPES=onboard$", body, re.M)
    # The browser install comes after the pip install that provides the playwright CLI.
    assert body.index("playwright install chromium") > body.index("requirements-browser.txt")
