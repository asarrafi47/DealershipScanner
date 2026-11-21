"""Cards L19, L20: the Railway tick and the apply step, with fake effects (no network, no database)."""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

from backend.scanner.loop import bundle
from backend.scanner.loop import requests as rq
from backend.scanner.loop.config import DEFAULTS
from backend.scripts import ops_tick as tick

NOW = datetime(2026, 11, 21, 12, 0, 0, tzinfo=timezone.utc)
NAME = "rq-20261121T110000Z-01"
OLD = [{"dealer_id": "abc-com", "url": "https://feed.abc.example/old", "method": "GET"}]
NEW = {"dealer_id": "abc-com", "url": "https://feed.abc.example/api/inventory?page=1", "method": "GET",
       "content_type": "application/json", "post_template": None, "auth_headers": {}, "pagination": "page_query",
       "vehicle_rows": 10, "total_count": 10, "provider_hint": "x", "stale": False, "stale_reason": ""}
CTX = {"manifest_ids": {"abc-com", "def-com"}, "do_not_scan": set(), "recipe_only_not_revived": set(), "blocked": set()}
PROPOSAL = "proposals/abc-com/pr-20261121T105900Z.json"


def _request(kind="rescan", dealers=("abc-com",), proposal=None):
    return {"schema": 1, "kind": kind, "dealers": list(dealers), "proposal": proposal, "fix_cycle": 1,
            "executor": "railway", "note": ""}


def _put_request(clone, name=NAME, **kw):
    folder = clone / "requests"
    folder.mkdir(parents=True, exist_ok=True)
    (folder / (name + ".json")).write_text(json.dumps(_request(**kw)))


def _put_proposal(clone, base_sha):
    path = clone / PROPOSAL
    path.parent.mkdir(parents=True, exist_ok=True)
    body = {"schema": 1, "dealer_id": "abc-com", "base_canonical_sha256": base_sha, "recipes": [NEW],
            "scan_hints": {}, "evidence_refs": [], "rationale": "workspace/dealer_logs/abc-com/summary.md"}
    path.write_text(json.dumps(body))


class FakeIO:
    def __init__(self, gate="ok", canary=("ok", 1.0), fail_save=False):
        self.rows = {"abc-com": list(OLD)}
        self.saves = []
        self._gate, self._canary, self.fail_save = gate, canary, fail_save

    def current_rows(self, d):
        return list(self.rows[d])

    def gate(self, d, rows):
        return self._gate

    def save(self, d, rows):
        if self.fail_save and rows != OLD:
            raise RuntimeError("db down")
        self.saves.append(rows)
        self.rows[d] = list(rows)

    def canary(self, d):
        return self._canary


def _status(clone, name=NAME):
    return json.loads((clone / "requests" / (name + ".status.json")).read_text())


def _run(tmp_path, io=None, config=DEFAULTS, ctx=CTX):
    clone = tmp_path / "clone"
    io = io or FakeIO()
    ex = tick.build_executors(config, io, lambda kind, dealers: (True, kind + " ok"), tmp_path / "rb")
    return tick.process(clone, tmp_path / "ledger.jsonl", config, ctx, ex, NOW, tmp_path / "rb"), clone


def test_rescan_runs_and_writes_status_and_fix_cycle(tmp_path):
    _put_request(tmp_path / "clone")
    counts, clone = _run(tmp_path)
    assert counts["ran"] == 1
    assert _status(clone)["state"] == "done"
    cycle = json.loads((clone / "fix_cycles" / "abc-com.json").read_text())
    assert cycle["attempts"] == 1 and cycle["history"][0]["request"] == NAME


def test_second_tick_never_runs_a_finished_request_again(tmp_path):
    _put_request(tmp_path / "clone")
    _run(tmp_path)
    (tmp_path / "clone" / "requests" / (NAME + ".status.json")).unlink()
    counts, _ = _run(tmp_path)
    assert counts == {"rejected": 0, "ran": 0, "deferred": 0}


def test_hostile_file_is_rejected_with_schema(tmp_path):
    clone = tmp_path / "clone"
    (clone / "requests").mkdir(parents=True)
    (clone / "requests" / (NAME + ".json")).write_text('{"kind": "rescan", "kind": "apply"}')
    counts, _ = _run(tmp_path)
    assert counts["rejected"] == 1 and _status(clone)["reason_code"] == "schema"


def test_unknown_dealer_rejected(tmp_path):
    _put_request(tmp_path / "clone", dealers=("nope-com",))
    _, clone = _run(tmp_path)
    assert _status(clone)["reason_code"] == "unknown_dealer"


def test_file_changed_after_first_record_is_ignored(tmp_path):
    clone = tmp_path / "clone"
    _put_request(clone, dealers=("nope-com",))
    _run(tmp_path)
    (clone / "requests" / (NAME + ".status.json")).unlink()
    _put_request(clone, dealers=("abc-com",))
    counts, _ = _run(tmp_path)
    assert counts == {"rejected": 0, "ran": 0, "deferred": 0}


def test_interrupted_request_fails_with_interrupted(tmp_path):
    clone = tmp_path / "clone"
    _put_request(clone)
    sha = tick._sha((clone / "requests" / (NAME + ".json")).read_bytes())
    tick.append_ledger(tmp_path / "ledger.jsonl", tick._event(NOW, "start", NAME, sha, "rescan", ["abc-com"]))
    _run(tmp_path)
    assert _status(clone)["state"] == "failed" and _status(clone)["reason_code"] == "interrupted"


def test_apply_is_not_executed_when_config_leaves_it_out(tmp_path):
    clone = tmp_path / "clone"
    _put_request(clone, kind="apply", proposal=PROPOSAL)
    _put_proposal(clone, bundle.canonical_sha256(OLD))
    config = replace(DEFAULTS, kinds=("probe", "validate", "rescan"))
    io = FakeIO()
    _run(tmp_path, io, config)
    assert _status(clone)["reason_code"] == "rate_limited" and io.saves == []


def test_apply_happy_path_writes_rollback_first(tmp_path):
    clone = tmp_path / "clone"
    _put_request(clone, kind="apply", proposal=PROPOSAL)
    _put_proposal(clone, bundle.canonical_sha256(OLD))
    io = FakeIO()
    _run(tmp_path, io)
    assert _status(clone)["state"] == "done"
    assert io.rows["abc-com"] == [NEW]
    assert (tmp_path / "rb" / (NAME + ".abc-com.json")).is_file()


def test_apply_base_changed_writes_nothing(tmp_path):
    clone = tmp_path / "clone"
    _put_request(clone, kind="apply", proposal=PROPOSAL)
    _put_proposal(clone, "f" * 64)
    io = FakeIO()
    _run(tmp_path, io)
    assert _status(clone)["reason_code"] == "base_changed" and io.saves == []


def test_apply_validation_reject_writes_nothing(tmp_path):
    clone = tmp_path / "clone"
    _put_request(clone, kind="apply", proposal=PROPOSAL)
    _put_proposal(clone, bundle.canonical_sha256(OLD))
    io = FakeIO(gate="reject")
    _run(tmp_path, io)
    assert _status(clone)["reason_code"] == "validation_reject" and io.saves == []


def test_apply_canary_failure_rolls_back(tmp_path):
    clone = tmp_path / "clone"
    _put_request(clone, kind="apply", proposal=PROPOSAL)
    _put_proposal(clone, bundle.canonical_sha256(OLD))
    io = FakeIO(canary=("reject", 1.0))
    _run(tmp_path, io)
    assert _status(clone)["state"] == "rolled_back" and _status(clone)["reason_code"] == "canary_fail"
    assert io.rows["abc-com"] == bundle_rows_restored()


def bundle_rows_restored():
    return json.loads(bundle.canonical_recipe_bytes(OLD))


def test_apply_low_stamped_ratio_rolls_back(tmp_path):
    clone = tmp_path / "clone"
    _put_request(clone, kind="apply", proposal=PROPOSAL)
    _put_proposal(clone, bundle.canonical_sha256(OLD))
    io = FakeIO(canary=("ok", 0.5))
    _run(tmp_path, io)
    assert _status(clone)["state"] == "rolled_back"


def test_apply_save_failure_restores_old_set(tmp_path):
    clone = tmp_path / "clone"
    _put_request(clone, kind="apply", proposal=PROPOSAL)
    _put_proposal(clone, bundle.canonical_sha256(OLD))
    io = FakeIO(fail_save=True)
    _run(tmp_path, io)
    assert _status(clone)["state"] == "rolled_back"


def test_one_apply_per_dealer_per_day(tmp_path):
    clone = tmp_path / "clone"
    _put_proposal(clone, bundle.canonical_sha256(OLD))
    _put_request(clone, kind="apply", proposal=PROPOSAL)
    io = FakeIO()
    _run(tmp_path, io)
    second = "rq-20261121T111500Z-01"
    _put_request(clone, name=second, kind="apply", proposal=PROPOSAL)
    _run(tmp_path, io)
    assert _status(clone, second)["reason_code"] == "rate_limited"


def test_torn_ledger_line_is_ignored(tmp_path):
    ledger = tmp_path / "ledger.jsonl"
    ledger.write_text('{"ev":"finish"\n')
    assert tick.read_ledger(ledger) == []


def test_template_config_leaves_apply_off():
    raw = (Path(__file__).resolve().parents[2] / "docs/scan_loop/data_repo_template/config/loop.json").read_bytes()
    from backend.scanner.loop.config import load_loop_config
    assert "apply" not in load_loop_config(raw).kinds
