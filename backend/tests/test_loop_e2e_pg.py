"""``pg`` tier: the whole loop end to end on a real Postgres, no network.

Railway publisher -> store (loop_files from migration V032) -> /api/loop -> cloud client pull -> a session files a request
and a note -> the Railway tick runs it -> its status comes back through the API.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import threading
from datetime import datetime, timezone
from pathlib import Path

import pytest
from flask import Flask
from werkzeug.serving import make_server

from backend.routes import loop_api
from backend.scanner.loop import store as loop_store
from backend.scanner.loop.config import load_loop_config
from backend.scripts import ops_publisher, ops_tick

pytestmark = pytest.mark.pg

ROOT = Path(__file__).resolve().parents[2]
FIXTURES = Path(__file__).resolve().parent / "fixtures" / "ops_publish"
READ, SUBMIT = "r" * 32, "s" * 32
REQ = "rq-20261121T110000Z-01"


def _client(*args, env):
    return subprocess.run(["python3", str(ROOT / "scripts/cloud/loop_client.py"), *args], capture_output=True, text=True, env=env)


def test_publish_pull_request_tick_status_round_trip(pg_db, tmp_path, monkeypatch):
    store = loop_store.PgStore(connect=pg_db.connect)
    monkeypatch.setattr(loop_api, "_store_override", store)
    monkeypatch.setenv("LOOP_READ_TOKEN", READ)
    monkeypatch.setenv("LOOP_SUBMIT_TOKEN", SUBMIT)

    # 1. Railway: a fleet run and a dealer log are published
    run = tmp_path / "pipeline" / "fleet_20261103T090000Z"
    for tag, fixture in {"s0": "scan_2026_11_03_s0", "s1": "scan_2026_11_03_s3", "s2": "scan_2026_11_03_s6"}.items():
        shutil.copytree(FIXTURES / fixture, run / tag)
    logs = tmp_path / "logs" / "abc-com"
    logs.mkdir(parents=True)
    (logs / "scan_runs.md").write_text("## run\nfetched https://feed.example/api?api_key=abcdef123456\n", encoding="utf-8")
    env = {"LOOP_STORE": "db", "DEALER_LOGS_ROOT": str(tmp_path / "logs"), "LOOP_TARGET": "prod"}
    result = ops_publisher.publish(run, env, "2026-11-03T12:00:00Z", catch_up=False, store=store)
    assert result["bundles"][0]["state"] == "published" and result["logs"] == 1
    assert b"abcdef123456" not in store.get("workspace/dealer_logs/abc-com/scan_runs.md")

    # 2. the web service's API on a live local port
    app = Flask(__name__)
    app.register_blueprint(loop_api.bp)
    server = make_server("127.0.0.1", 0, app)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        cenv = {**{k: v for k, v in os.environ.items() if not k.startswith("LOOP_")},
                "LOOP_API_URL": "http://127.0.0.1:%d" % server.server_port, "LOOP_READ_TOKEN": READ, "LOOP_SUBMIT_TOKEN": SUBMIT}

        # 3. a cloud session hydrates, files a request and a note
        data = tmp_path / "data"
        pulled = _client("pull", "--out", str(data), env=cenv)
        assert pulled.returncode == 0, pulled.stderr
        assert (data / "runs" / "fleet_20261103T090000Z" / "digest.md").is_file()
        assert (data / "workspace/dealer_logs/abc-com/scan_runs.md").is_file()
        (data / "requests").mkdir(exist_ok=True)
        (data / "requests" / f"{REQ}.json").write_text(json.dumps({
            "schema": 1, "kind": "rescan", "dealers": ["abc-com"], "proposal": None, "fix_cycle": 1,
            "executor": "railway", "note": "e2e"}))
        (data / "workspace/dealer_logs/abc-com/summary.md").write_text("session summary\n")
        pushed = _client("push", "--root", str(data), env=cenv)
        assert pushed.returncode == 0, pushed.stderr

        # 4. Railway tick: seed config, read the request, run it (the scan itself stubbed), store the status
        ops_tick.seed_config(store)
        tree = tmp_path / "tree"
        ops_tick.sync_down(store, tree)
        config = load_loop_config((tree / "config" / "loop.json").read_bytes())
        ctx = {"manifest_ids": {"abc-com"}, "do_not_scan": set(), "recipe_only_not_revived": set(), "blocked": set()}
        executors = ops_tick.build_executors(config, None, lambda kind, dealers: (True, kind + " ok"), tmp_path / "rb")
        now = datetime(2026, 11, 21, 12, 0, tzinfo=timezone.utc)
        counts = ops_tick.process(tree, tmp_path / "ledger.jsonl", config, ctx, executors, now, tmp_path / "rb")
        assert counts["ran"] == 1 and ops_tick.sync_up(store, tree) == 2

        # 5. the session sees the outcome through the API
        again = _client("pull", "--out", str(tmp_path / "data2"), env=cenv)
        assert again.returncode == 0, again.stderr
        status = json.loads((tmp_path / "data2" / "requests" / f"{REQ}.status.json").read_text())
        assert status["state"] == "done" and status["kind"] == "rescan"
        assert (tmp_path / "data2" / "fix_cycles" / "abc-com.json").is_file()
        assert (tmp_path / "data2" / "workspace/dealer_logs/abc-com/summary.md").read_text() == "session summary\n"
    finally:
        server.shutdown()
    assert READ not in pushed.stdout + pushed.stderr + again.stdout + again.stderr
