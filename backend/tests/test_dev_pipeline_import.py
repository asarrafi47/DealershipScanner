"""Dev / operator import and scan buttons run the Python dealer pipeline (audit B8),
and the dev escapers are safe inside quoted attributes (audit B9)."""

from __future__ import annotations

import io
import json
import re
import shutil
import subprocess
from pathlib import Path

import pytest

from backend.dev import console
from backend.dev import dealers as dd
from backend.dev import routes

REPO_ROOT = Path(__file__).resolve().parents[2]


class _FakePipeline:
    """Stands in for ``subprocess.Popen`` of ``backend.scripts.dealer_pipeline``:
    prints stage lines and writes ``triage.json`` into the ``--out`` directory."""

    calls: list[list[str]] = []
    verdict = "ok"
    rows = 42

    def __init__(self, cmd, **_kwargs):
        type(self).calls.append(list(cmd))
        dealer_id = cmd[cmd.index("--dealers") + 1]
        out_dir = Path(cmd[cmd.index("--out") + 1])
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "triage.json").write_text(
            json.dumps(
                {
                    "dealers": [
                        {
                            "dealer_id": dealer_id,
                            "verdict": type(self).verdict,
                            "reason": "complete and verified" if type(self).verdict == "ok" else "not_in_manifest_or_db",
                            "rows": type(self).rows,
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )
        self.stdout = io.StringIO(f"recipe  {dealer_id} saved\nscan    batch 1: {dealer_id} rc=0\n")
        self.pid = 0

    def poll(self):
        return 0

    def wait(self):
        return 0


@pytest.fixture()
def pipeline_env(monkeypatch: pytest.MonkeyPatch, tmp_path):
    manifest = tmp_path / "dealers.json"
    manifest.write_text("[]\n", encoding="utf-8")
    monkeypatch.setattr(routes, "DEALERS_PATH", manifest)
    monkeypatch.setattr(routes, "pipeline_out_dir", lambda tag: tmp_path / "out" / tag)
    monkeypatch.setattr(routes, "_smart_import_display_name", lambda url: "Example Motors")
    monkeypatch.setattr(routes, "_spawn_vector_reindex_background", lambda: None)
    monkeypatch.setattr(routes.subprocess, "Popen", _FakePipeline)
    _FakePipeline.calls = []
    _FakePipeline.verdict = "ok"
    _FakePipeline.rows = 42
    return manifest


def _new_job() -> str:
    job_id = "job" + str(len(_FakePipeline.calls)) + "x" * 8
    routes._dev_store_put("jobs", job_id, {"log": "", "discovery": [], "done": False})
    return job_id


def test_dealers_path_is_the_manifest_the_scanner_reads(monkeypatch: pytest.MonkeyPatch) -> None:
    # B8: backend/dev/dealers.py wrote backend/dealers.json; the scanner reads the root file.
    from backend.scanner import constants

    monkeypatch.delenv("DEALERS_MANIFEST_PATH", raising=False)
    assert dd.ROOT == REPO_ROOT
    assert (dd.ROOT / "dealers.json") == (constants.ROOT / "dealers.json")
    assert dd.DEALERS_PATH.resolve() == constants.MANIFEST_PATH.resolve()


def test_console_runs_from_repo_root_with_the_pipeline() -> None:
    assert console._PROJECT_ROOT == REPO_ROOT
    cmd = console.pipeline_command("example-com", Path("/tmp/out"), manifest_path=dd.DEALERS_PATH)
    assert cmd[1:4] == ["-m", "backend.scripts.dealer_pipeline", "--dealers"]
    assert (REPO_ROOT / "backend" / "scripts" / "dealer_pipeline.py").is_file()


def test_save_dealers_keeps_compact_manifest_compact(tmp_path) -> None:
    p = tmp_path / "dealers.json"
    p.write_text('[{"name":"A","url":"https://a.example","provider":"unknown","dealer_id":"a-example"}]\n')
    dd.upsert_dealer_manifest_row(name="B", website_url="https://b.example", provider="unknown", manifest_path=p)
    text = p.read_text()
    assert text.startswith('[{"name":"A"')
    assert len(text.splitlines()) == 1
    assert [r["dealer_id"] for r in json.loads(text)] == ["a-example", "b-example"]


def test_smart_import_adds_manifest_row_and_runs_pipeline(pipeline_env: Path) -> None:
    job_id = _new_job()
    routes._run_smart_import_job(job_id, "https://www.example-motors.com/", headed=False)

    rows = json.loads(pipeline_env.read_text())
    assert rows == [
        {"name": "Example Motors", "url": "https://www.example-motors.com", "provider": "unknown",
         "dealer_id": "example-motors-com"}
    ]
    (cmd,) = _FakePipeline.calls
    assert cmd[1:3] == ["-m", "backend.scripts.dealer_pipeline"]
    assert cmd[cmd.index("--dealers") + 1] == "example-motors-com"
    assert cmd[cmd.index("--manifest") + 1] == str(pipeline_env)
    assert not any("scanner.js" in part or part == "node" for part in cmd)

    job = routes._dev_store_get("jobs", job_id)
    assert job["done"] is True and job["exit_code"] == 0
    assert job["verdict"] == "ok" and job["rows"] == 42
    assert job["smart_error"] is None
    assert [ev["step"] for ev in job["discovery"]] == ["name", "recipe", "scan"]
    assert "Pipeline verdict: ok (42 rows)" in job["log"]


def test_smart_import_reports_failing_verdict(pipeline_env: Path) -> None:
    _FakePipeline.verdict = "no_recipe"
    _FakePipeline.rows = 0
    job_id = _new_job()
    routes._run_smart_import_job(job_id, "https://example-motors.com", headed=True)
    job = routes._dev_store_get("jobs", job_id)
    assert job["smart_error"]["reason"] == "no_recipe"
    assert "HTTP-only" in job["log"]  # headed is explained, not silently dropped


def test_smart_import_keeps_existing_manifest_identity(pipeline_env: Path) -> None:
    pipeline_env.write_text(json.dumps([
        {"name": "Long Beach BMW", "url": "https://www.longbeachbmw.com", "provider": "dealer_dot_com",
         "dealer_id": "long-beach-bmw"}
    ]))
    routes._run_smart_import_job(_new_job(), "https://www.longbeachbmw.com/", headed=False)
    rows = json.loads(pipeline_env.read_text())
    assert len(rows) == 1 and rows[0]["dealer_id"] == "long-beach-bmw"
    assert rows[0]["provider"] == "dealer_dot_com"
    assert _FakePipeline.calls[0][_FakePipeline.calls[0].index("--dealers") + 1] == "long-beach-bmw"


def test_test_scanner_scans_the_known_dealer_without_writing_manifest(pipeline_env: Path) -> None:
    pipeline_env.write_text(json.dumps([
        {"name": "Long Beach BMW", "url": "https://www.longbeachbmw.com", "provider": "dealer_dot_com",
         "dealer_id": "long-beach-bmw"}
    ]))
    before = pipeline_env.read_text()
    job_id = _new_job()
    routes._run_scanner_job(job_id, "https://www.longbeachbmw.com", headed=False)
    assert pipeline_env.read_text() == before
    assert _FakePipeline.calls[0][_FakePipeline.calls[0].index("--dealers") + 1] == "long-beach-bmw"
    job = routes._dev_store_get("jobs", job_id)
    assert job["done"] is True and job["verdict"] == "ok" and job["smart_error"] is None


def test_dev_status_reports_pipeline_not_node() -> None:
    status = routes._dev_status_shell()
    assert status["scan_pipeline_ok"] is True
    assert "dealer_pipeline" in status["scan_pipeline_status_line"]
    assert not any(k.startswith("node_") for k in status)


def test_no_dev_tool_spawns_retired_scanners() -> None:
    for name in ("routes.py", "console.py", "dealers.py", "pipeline_jobs.py"):
        src = (REPO_ROOT / "backend" / "dev" / name).read_text(encoding="utf-8")
        assert '"scanner.js"' not in src, name
        assert '_PROJECT_ROOT / "scanner.py"' not in src, name


def test_operator_api_exposes_queue_skip(monkeypatch: pytest.MonkeyPatch, tmp_path, app_factory) -> None:
    from backend.tests.test_admin_operator_api import _make_app

    app = _make_app(app_factory, monkeypatch, tmp_path)
    rules = {r.rule: r.methods for r in app.url_map.iter_rules()}
    assert "POST" in rules["/api/admin/operator/import-queue/<queue_id>/skip-item"]
    assert "POST" in rules["/api/admin/operator/smart-import"]


# --- B9: HTML escapers -------------------------------------------------------

_DIV_TRICK = re.compile(r"textContent\s*=.*\n\s*return\s+\w+\.innerHTML", re.M)


def test_no_static_escaper_uses_the_textcontent_trick() -> None:
    # textContent -> innerHTML leaves " and ' unescaped, which breaks value="…".
    offenders = [
        p.name
        for p in (REPO_ROOT / "frontend" / "static").glob("*.js")
        if _DIV_TRICK.search(p.read_text(encoding="utf-8"))
    ]
    assert offenders == []


@pytest.mark.skipif(shutil.which("node") is None, reason="node not installed")
def test_dev_js_escaper_escapes_quotes() -> None:
    src = (REPO_ROOT / "frontend" / "static" / "dev.js").read_text(encoding="utf-8")
    m = re.search(r"function escHtml\(s\) \{.*?\n    \}", src, re.S)
    assert m, "escHtml not found in dev.js"
    script = m.group(0) + '\nprocess.stdout.write(escHtml(`a"b\'c<d>&`) + "|" + escHtml(null));'
    out = subprocess.run(["node", "-e", script], capture_output=True, text=True, timeout=30, check=True).stdout
    assert out == "a&quot;b&#39;c&lt;d&gt;&amp;|"
