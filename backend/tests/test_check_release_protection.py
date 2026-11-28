"""scripts/check_release_protection.py: live rulesets vs docs/RELEASING.md (offline)."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "check_release_protection.py"

_spec = importlib.util.spec_from_file_location("check_release_protection", SCRIPT)
crp = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(crp)


def _live(name, target, include, rules, bypass=("DeployKey",), enforcement="active", with_bypass=True):
    rs = {
        "id": 1,
        "name": name,
        "target": target,
        "enforcement": enforcement,
        "conditions": {"ref_name": {"include": list(include), "exclude": []}},
        "rules": [{"type": t} for t in rules],
    }
    if with_bypass:
        rs["bypass_actors"] = [{"actor_id": 9, "actor_type": t, "bypass_mode": "always"} for t in bypass]
    return rs


@pytest.fixture(scope="module")
def expected():
    return crp.load_expected((REPO_ROOT / "docs" / "RELEASING.md").read_text(encoding="utf-8"))


def _matching(expected):
    return [_live(e["name"], e["target"], e["include"], e["rules"], e.get("bypass_actor_types", ())) for e in expected]


def test_docs_block_names_protect_main_and_the_tag_ruleset(expected):
    by = {e["name"]: e for e in expected}
    main = by["protect-main"]
    assert main["target"] == "branch" and main["include"] == ["refs/heads/main"]
    assert set(main["rules"]) == {"deletion", "non_fast_forward", "update"}
    assert main["bypass_actor_types"] == ["DeployKey"]
    tags = by["protect-release-tags"]
    assert tags["target"] == "tag" and tags["include"] == ["refs/tags/v*"]
    assert {"creation", "deletion", "update", "non_fast_forward"} <= set(tags["rules"])


def test_matching_state_has_no_differences(expected):
    assert crp.compare(expected, _matching(expected)) == ([], [])


def test_missing_tag_ruleset_and_extra_rule_and_bypass_are_reported(expected):
    live = [_live("protect-main", "branch", ["refs/heads/main"],
                  ["deletion", "non_fast_forward", "update", "required_signatures"],
                  bypass=("DeployKey", "Integration"))]
    diffs, _ = crp.compare(expected, live)
    text = "\n".join(diffs)
    assert "'protect-release-tags' is missing" in text
    assert "not in docs ['required_signatures']" in text
    assert "bypass actor types ['DeployKey', 'Integration']" in text


def test_include_and_enforcement_drift(expected):
    live = _matching(expected)
    live[0]["conditions"]["ref_name"]["include"] = ["~DEFAULT_BRANCH"]
    live[1]["enforcement"] = "evaluate"
    diffs, _ = crp.compare(expected, live)
    assert any("includes ['~DEFAULT_BRANCH']" in d for d in diffs)
    assert any("enforcement is 'evaluate'" in d for d in diffs)


def test_hidden_bypass_actors_are_a_note_not_a_difference(expected):
    live = _matching(expected)
    for rs in live:
        del rs["bypass_actors"]
    diffs, notes = crp.compare(expected, live)
    assert diffs == [] and len(notes) == 2


def test_cli_offline_warns_but_exits_zero_unless_strict(tmp_path, capsys, monkeypatch):
    monkeypatch.delenv("GITHUB_STEP_SUMMARY", raising=False)
    live = tmp_path / "live.json"
    live.write_text(json.dumps([]), encoding="utf-8")
    assert crp.main(["--live", str(live)]) == 0
    out = capsys.readouterr().out
    assert "::warning::ruleset differs" in out and "protect-main" in out
    assert crp.main(["--live", str(live), "--strict"]) == 1


def test_broken_docs_block_is_an_error(tmp_path, capsys):
    doc = tmp_path / "R.md"
    doc.write_text("no block here\n", encoding="utf-8")
    live = tmp_path / "live.json"
    live.write_text("[]", encoding="utf-8")
    assert crp.main(["--live", str(live), "--doc", str(doc)]) == 2
    assert "::error::" in capsys.readouterr().out


def test_api_failure_is_a_warning(monkeypatch, capsys):
    def boom(*_a, **_k):
        raise crp.urllib.error.URLError("offline")

    monkeypatch.setattr(crp, "_get", boom)
    monkeypatch.setenv("GH_TOKEN", "t0ken-value")
    assert crp.main(["--repo", "o/r"]) == 0
    out = capsys.readouterr().out
    assert "could not read the live rulesets" in out and "t0ken-value" not in out


def test_repo_argument_is_validated(capsys):
    assert crp.main(["--repo", "o/r/../x?"]) == 2
