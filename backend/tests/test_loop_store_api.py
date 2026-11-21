"""Scan loop store, /api/loop, the cloud client, persist.sh and ops_hydrate.sh (all offline, no database)."""

from __future__ import annotations

import io
import json
import os
import subprocess
import tarfile
import threading
from pathlib import Path

import pytest
from flask import Flask
from werkzeug.serving import make_server

from backend.routes import loop_api
from backend.scanner.loop import store as ls

ROOT = Path(__file__).resolve().parents[2]
READ = "r" * 32
SUBMIT = "s" * 32
REQ = "rq-20261121T110000Z-01"
GOOD_REQUEST = {"schema": 1, "kind": "rescan", "dealers": ["abc-com"], "proposal": None, "fix_cycle": 1,
                "executor": "railway", "note": ""}


@pytest.fixture()
def store(monkeypatch):
    mem = ls.MemStore()
    monkeypatch.setattr(loop_api, "_store_override", mem)
    monkeypatch.setenv("LOOP_READ_TOKEN", READ)
    monkeypatch.setenv("LOOP_SUBMIT_TOKEN", SUBMIT)
    return mem


@pytest.fixture()
def client(store):
    app = Flask(__name__)
    app.register_blueprint(loop_api.bp)
    return app.test_client()


def _h(token):
    return {"Authorization": "Bearer " + token}


def _submit(client, path, text, token=SUBMIT):
    return client.post("/api/loop/submit", json={"path": path, "text": text}, headers=_h(token))


# --- the store ----------------------------------------------------------------------------------------------


@pytest.mark.parametrize("path", ["", "x", "../etc/passwd", "runs/../x", "runs//x", "runs/a b", "requests/", "config/" + "a" * 250,
                                  "other/file.md", "workspace/dealer_logs/..", "runs/.hidden/x"])
def test_bad_paths_are_refused(path):
    assert not ls.path_ok(path)


def test_put_get_list_delete_and_identical_put_is_a_noop():
    s = ls.MemStore()
    assert s.put("runs/fleet_20261103T090000Z/a.md", b"x") is True
    assert s.put("runs/fleet_20261103T090000Z/a.md", b"x") is False
    assert s.get("runs/fleet_20261103T090000Z/a.md") == b"x"
    assert [e.size for e in s.list("runs/")] == [1]
    assert s.delete_prefix("runs/") == 1 and s.list() == []


def test_caps_per_file_and_in_total():
    s = ls.MemStore(max_total=100)
    with pytest.raises(ls.StoreError):
        s.put("config/loop.json", b"x" * (ls.MAX_FILE_BYTES["config/"] + 1))
    s.put("findings/a.md", b"x" * 90)
    with pytest.raises(ls.StoreError, match="full"):
        s.put("findings/b.md", b"x" * 20)


def test_prune_runs_keeps_the_newest():
    s = ls.MemStore()
    for day in range(1, 6):
        s.put(f"runs/fleet_2026110{day}T090000Z/MANIFEST.json", b"{}")
    assert ls.prune_runs(s, 2) == 3
    assert {e.path.split("/")[1] for e in s.list("runs/")} == {"fleet_20261104T090000Z", "fleet_20261105T090000Z"}


def test_export_tar_has_exactly_the_requested_prefixes_and_refuses_a_huge_export(monkeypatch):
    s = ls.MemStore()
    s.put("findings/a.md", b"a")
    s.put("mirror/index.tsv", b"m")
    names = tarfile.open(fileobj=io.BytesIO(ls.export_tar(s, ["findings/"])), mode="r:gz").getnames()
    assert names == ["findings/a.md"]
    monkeypatch.setattr(ls, "MAX_EXPORT_BYTES", 1)
    with pytest.raises(ls.StoreError):
        ls.export_tar(s, ["findings/", "mirror/"])


# --- the API ------------------------------------------------------------------------------------------------


def test_the_api_does_not_exist_without_tokens(monkeypatch):
    monkeypatch.delenv("LOOP_READ_TOKEN", raising=False)
    monkeypatch.delenv("LOOP_SUBMIT_TOKEN", raising=False)
    monkeypatch.setattr(loop_api, "_store_override", ls.MemStore())
    app = Flask(__name__)
    app.register_blueprint(loop_api.bp)
    c = app.test_client()
    assert c.get("/api/loop/list").status_code == 404
    assert c.post("/api/loop/submit", json={}).status_code == 404


def test_a_short_token_never_enables_the_api(monkeypatch):
    monkeypatch.setenv("LOOP_READ_TOKEN", "short")
    monkeypatch.delenv("LOOP_SUBMIT_TOKEN", raising=False)
    monkeypatch.setattr(loop_api, "_store_override", ls.MemStore())
    app = Flask(__name__)
    app.register_blueprint(loop_api.bp)
    assert app.test_client().get("/api/loop/list", headers=_h("short")).status_code == 404


def test_reads_need_the_read_token_and_submit_needs_the_submit_token(client, store):
    store.put("findings/a.md", b"hi")
    assert client.get("/api/loop/list").status_code == 401
    assert client.get("/api/loop/list", headers=_h("x" * 32)).status_code == 401
    assert client.get("/api/loop/list", headers=_h(SUBMIT)).status_code == 401  # the submit token does not read
    listing = client.get("/api/loop/list?prefix=findings/", headers=_h(READ)).get_json()
    assert listing[0]["path"] == "findings/a.md"
    assert client.get("/api/loop/file?path=findings/a.md", headers=_h(READ)).data == b"hi"
    assert _submit(client, "findings/b.md", "x", READ).status_code == 401  # the read token does not write


def test_read_routes_refuse_bad_prefixes_and_paths(client):
    assert client.get("/api/loop/list?prefix=../x", headers=_h(READ)).status_code == 400
    assert client.get("/api/loop/file?path=../x", headers=_h(READ)).status_code == 400
    assert client.get("/api/loop/file?path=findings/none.md", headers=_h(READ)).status_code == 404
    assert client.get("/api/loop/export", headers=_h(READ)).status_code == 400


def test_submit_accepts_a_valid_request_once(client, store):
    path = f"requests/{REQ}.json"
    assert _submit(client, path, json.dumps(GOOD_REQUEST)).status_code == 200
    assert store.get(path)
    assert _submit(client, path, json.dumps(GOOD_REQUEST)).status_code == 409  # write-once


@pytest.mark.parametrize("path,text", [
    (f"requests/{REQ}.json", '{"kind": "rescan"}'),                                   # schema
    (f"requests/{REQ}.json", '{"kind":"rescan","kind":"apply"}'),                     # duplicate keys
    ("requests/not-a-request.json", json.dumps(GOOD_REQUEST)),                        # name
    (f"requests/{REQ}.status.json", "{}"),                                             # a status is Railway's
    ("runs/fleet_20261103T090000Z/MANIFEST.json", "{}"),                              # runs are Railway's
    ("mirror/index.tsv", "x"),
    ("config/loop.json", '{"enabled": true}'),                                         # config is the owner's
    ("fix_cycles/abc-com.json", "{}"),
    ("workspace/dealer_logs/abc-com/scan_runs.md", "x"),                              # only summary and location
    ("proposals/abc-com/pr-20261121T105900Z.json", "{}"),                              # schema
])
def test_submit_refuses_everything_outside_the_session_contract(client, path, text):
    assert _submit(client, path, text).status_code in (400, 409)


def test_submit_scrubs_notes_and_stores_them(client, store):
    r = _submit(client, "workspace/dealer_logs/abc-com/summary.md", "see https://feed.example/api?api_key=abcdef123456&p=2\n")
    assert r.status_code == 200
    assert b"abcdef123456" not in store.get("workspace/dealer_logs/abc-com/summary.md")
    assert _submit(client, "workspace/dealer_logs/abc-com/summary.md", "second version\n").status_code == 200  # notes can change
    assert store.get("workspace/dealer_logs/abc-com/summary.md") == b"second version\n"


def test_submit_refuses_a_bad_body_and_an_oversize_one(client):
    assert client.post("/api/loop/submit", json={"path": "findings/a.md"}, headers=_h(SUBMIT)).status_code == 400
    assert client.post("/api/loop/submit", json={"path": "findings/a.md", "text": 5}, headers=_h(SUBMIT)).status_code == 400
    assert _submit(client, "findings/a.md", "x" * 70000).status_code == 400


def test_unprocessed_requests_are_capped(client, store):
    for n in range(50):
        store.put(f"requests/rq-20261121T1100{n // 10}{n % 10}Z-01.json", b"{}")
    r = _submit(client, "requests/rq-20261122T110000Z-01.json", json.dumps(GOOD_REQUEST))
    assert r.status_code == 429


def test_responses_are_never_cached(client, store):
    store.put("findings/a.md", b"hi")
    assert "no-store" in client.get("/api/loop/list", headers=_h(READ)).headers["Cache-Control"]
    assert "no-store" in client.get("/api/loop/file?path=findings/a.md", headers=_h(READ)).headers["Cache-Control"]


# --- the client, persist.sh and ops_hydrate.sh against a live local server ----------------------------------------


@pytest.fixture()
def server(store):
    app = Flask(__name__)
    app.register_blueprint(loop_api.bp)
    srv = make_server("127.0.0.1", 0, app)
    thread = threading.Thread(target=srv.serve_forever, daemon=True)
    thread.start()
    yield "http://127.0.0.1:%d" % srv.server_port
    srv.shutdown()


def _env(url, **extra):
    base = {k: v for k, v in os.environ.items() if not k.startswith("LOOP_")}
    return {**base, "LOOP_API_URL": url, "LOOP_READ_TOKEN": READ, "LOOP_SUBMIT_TOKEN": SUBMIT, "PERSIST_SLEEP": "0", **extra}


def _client(*args, env):
    return subprocess.run(["python3", str(ROOT / "scripts" / "cloud" / "loop_client.py"), *args], capture_output=True, text=True, env=env)


def test_client_pull_writes_files_and_the_latest_run(server, store, tmp_path):
    store.put("workspace/dealer_logs/abc-com/discovery.md", b"## d\n")
    store.put("runs/fleet_20261103T090000Z/digest.md", b"digest\n")
    store.put("runs/LATEST.json", json.dumps({"run_id": "fleet_20261103T090000Z"}).encode())
    r = _client("pull", "--out", str(tmp_path / "d"), env=_env(server))
    assert r.returncode == 0, r.stderr
    assert (tmp_path / "d" / "workspace/dealer_logs/abc-com/discovery.md").read_text() == "## d\n"
    assert (tmp_path / "d" / "runs/fleet_20261103T090000Z/digest.md").exists()
    assert READ not in r.stdout + r.stderr


def test_client_push_sends_the_session_files_and_treats_exists_as_done(server, store, tmp_path):
    (tmp_path / "requests").mkdir()
    (tmp_path / "requests" / f"{REQ}.json").write_text(json.dumps(GOOD_REQUEST))
    (tmp_path / "workspace/dealer_logs/abc-com").mkdir(parents=True)
    (tmp_path / "workspace/dealer_logs/abc-com/summary.md").write_text("done\n")
    (tmp_path / "workspace/dealer_logs/abc-com/scan_runs.md").write_text("not sent\n")
    for _ in range(2):  # the second run hits "exists" for the request and still succeeds
        r = _client("push", "--root", str(tmp_path), env=_env(server))
        assert r.returncode == 0, r.stderr
    assert store.get(f"requests/{REQ}.json") and store.get("workspace/dealer_logs/abc-com/summary.md")
    assert store.get("workspace/dealer_logs/abc-com/scan_runs.md") is None
    assert SUBMIT not in r.stdout + r.stderr


def test_client_refuses_http_to_a_remote_host_and_reports_a_refused_file(server, store, tmp_path):
    assert _client("pull", "--out", str(tmp_path), env=_env("http://example.com")).returncode == 2
    (tmp_path / "requests").mkdir()
    (tmp_path / "requests" / f"{REQ}.json").write_text('{"kind": "x"}')
    r = _client("push", "--root", str(tmp_path), env=_env(server))
    assert r.returncode == 1 and "refused" in r.stderr


def test_persist_validates_locally_then_sends(server, store, tmp_path):
    data = tmp_path / "data"
    (data / "requests").mkdir(parents=True)
    (data / "requests" / f"{REQ}.json").write_text(json.dumps(GOOD_REQUEST))
    ok = subprocess.run(["bash", str(ROOT / "scripts/cloud/persist.sh")], capture_output=True, text=True,
                        env=_env(server, LOOP_DATA_DIR=str(data)))
    assert ok.returncode == 0, ok.stderr
    assert store.get(f"requests/{REQ}.json")
    (data / "requests" / "rq-20261121T120000Z-01.json").write_text('{"kind": "bad"}')
    bad = subprocess.run(["bash", str(ROOT / "scripts/cloud/persist.sh")], capture_output=True, text=True,
                         env=_env(server, LOOP_DATA_DIR=str(data)))
    assert bad.returncode == 1 and store.get("requests/rq-20261121T120000Z-01.json") is None


def test_persist_and_hydrate_need_their_tokens(tmp_path):
    env = {k: v for k, v in os.environ.items() if not k.startswith("LOOP_")}
    assert subprocess.run(["bash", str(ROOT / "scripts/cloud/persist.sh")], capture_output=True, env={**env, "LOOP_DATA_DIR": str(tmp_path)}).returncode == 2
    assert subprocess.run(["bash", str(ROOT / "scripts/cloud/ops_hydrate.sh"), str(tmp_path / "d")], capture_output=True, env=env).returncode == 2


def test_hydrate_pulls_into_a_data_folder_links_workspace_and_prints_exports(server, store, tmp_path):
    store.put("workspace/dealer_logs/abc-com/discovery.md", b"## d\n")
    src = tmp_path / "src"
    (src / "scripts/cloud").mkdir(parents=True)
    (src / "docs/scan_loop/data_template").mkdir(parents=True)
    (src / "scripts/cloud/loop_client.py").write_bytes((ROOT / "scripts/cloud/loop_client.py").read_bytes())
    (src / "docs/scan_loop/data_template/CLAUDE.md").write_text("rules\n")
    (src / ".gitignore").write_text("workspace/\n")
    git_env = {"GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@x.invalid", "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@x.invalid"}
    for args in (["init", "-q", "-b", "dev"], ["add", "-A"], ["commit", "-q", "-m", "x"]):
        subprocess.run(["git", *args], cwd=src, check=True, env={**os.environ, **git_env}, capture_output=True)
    code = tmp_path / "code"
    subprocess.run(["git", "clone", "-q", str(src), str(code)], check=True, capture_output=True)
    data = tmp_path / "data"
    r = subprocess.run(["bash", str(ROOT / "scripts/cloud/ops_hydrate.sh"), str(data), str(code)], capture_output=True, text=True,
                       env=_env(server, LOOP_CODE_URL="unused"))
    assert r.returncode == 0, r.stderr
    assert (data / "workspace/dealer_logs/abc-com/discovery.md").exists() and (data / "CLAUDE.md").read_text() == "rules\n"
    assert (code / "workspace").is_symlink()
    assert f'export LOOP_DATA_DIR="{data}"' in r.stdout and "RECIPES_DB_DISABLED=1" in r.stdout
    assert READ not in r.stdout + r.stderr
    st = subprocess.run(["git", "status", "--porcelain"], cwd=code, capture_output=True, text=True).stdout
    assert "workspace" not in st
    again = subprocess.run(["bash", str(ROOT / "scripts/cloud/ops_hydrate.sh"), str(data), str(code)], capture_output=True, text=True,
                           env=_env(server, LOOP_CODE_URL="unused"))
    assert again.returncode == 0, again.stderr


def test_template_files_exist_and_the_default_config_leaves_apply_off():
    from backend.scanner.loop.config import load_loop_config

    t = ROOT / "docs" / "scan_loop" / "data_template"
    assert (t / "CLAUDE.md").is_file() and (t / ".claude" / "settings.json").is_file()
    cfg = load_loop_config((t / "config" / "loop.json").read_bytes())
    assert cfg.enabled and "apply" not in cfg.kinds
    settings = json.loads((t / ".claude" / "settings.json").read_text())
    assert "WebFetch" in settings["permissions"]["deny"]
