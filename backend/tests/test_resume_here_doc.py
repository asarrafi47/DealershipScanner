"""RESUME_HERE.md is a pointer page: every link and path it names must exist (P18B.1)."""
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
PAGE = REPO_ROOT / "RESUME_HERE.md"


def test_links_resolve():
    text = PAGE.read_text(encoding="utf-8")
    targets = re.findall(r"\]\(([^)#]+)\)", text)
    assert targets
    missing = [t for t in targets if not t.startswith("http") and not (REPO_ROOT / t).exists()]
    assert not missing


def test_no_hardcoded_version_and_master_todo_retired():
    text = PAGE.read_text(encoding="utf-8")
    assert not re.search(r"\b\d+\.\d+\.\d+\b", text)
    assert "master-todo" not in text
    assert not (REPO_ROOT / "master-todo.md").exists()


def test_named_commands_exist():
    for rel in ("run.py", "scripts/bump_version.sh", "deploy/up.sh",
                "deploy/railway/deploy_web.sh", "deploy/railway/deploy_scanner_nightly.sh",
                "scripts/docker-entrypoint-web.sh"):
        assert (REPO_ROOT / rel).exists(), rel
