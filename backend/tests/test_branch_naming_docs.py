"""Branch names agree across the release docs (NEXT_PHASES_PLAN N4.3, LE-5, D-REL2).

The 0A-2B phase runner named unit branches ``remediation/<unit>``; D-REL2 (a)
and NEXT_PHASES_PLAN Section 7.0 use ``phase/<NID>`` and ``unit/<unit id>``.
These tests read the docs as text (no git, no network) and pin:

- docs/RELEASING.md "Branching model" and the Section 10.1 phase-runner
  template name the same branch and worktree patterns (``<NID>`` is the
  plan's spelling of RELEASING.md's ``<id>``), and neither tells anyone to use
  ``remediation/<...>``;
- Section 7.0's branch convention says the same;
- retiring ``feature/http-only-scans`` is an owner step behind four gates
  (ksarrafi told, the first phase release on main, the mini check or a logged
  waiver, no checkout on the branch). The delete command appears once, after
  the gates, and no other doc tells anyone to delete the branch;
- the tip SHA the retirement section quotes is the 1.5.4 release-log commit;
- every in-page link in RELEASING.md resolves to a heading.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
RELEASING = REPO_ROOT / "docs" / "RELEASING.md"
PLAN = REPO_ROOT / "docs" / "NEXT_PHASES_PLAN_2026_10.md"
RETIRE_HEADING = "### Retiring `feature/http-only-scans`"
DELETE_CMD = "git push origin --delete feature/http-only-scans"

_BRANCH = re.compile(r"\b(phase|unit)/<([^>]+)>")
_WORKTREE = re.compile(r"\.claude/worktrees/((?:phase-)?<[^>]+>)")
_REMEDIATION = re.compile(r"remediation/<[^>]*>")


@pytest.fixture(scope="module")
def releasing() -> str:
    return RELEASING.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def plan() -> str:
    return PLAN.read_text(encoding="utf-8")


def _section(text: str, heading: str, level: str = "## ") -> str:
    start = text.index(heading)
    end = text.find("\n" + level, start + len(heading))
    return text[start:] if end == -1 else text[start:end]


def _branching_model(releasing: str) -> str:
    return _section(releasing, "## Branching model")


def _runner_template(plan: str) -> str:
    section = _section(plan, "### 10.1 Prompt template", level="### ")
    blocks = re.findall(r"^```\n(.*?)^```", section, flags=re.MULTILINE | re.DOTALL)
    assert blocks, "Section 10.1 has no fenced template"
    return blocks[0]


def _normalize(pattern: str) -> str:
    # The plan writes the phase id as <NID>; RELEASING.md writes <id>.
    return pattern.replace("<NID>", "<id>")


def _branch_patterns(text: str) -> set[str]:
    return {_normalize(f"{kind}/<{name}>") for kind, name in _BRANCH.findall(text)}


def _worktree_patterns(text: str) -> set[str]:
    return {_normalize(p) for p in _WORKTREE.findall(text)}


def _sentences_with(text: str, pattern: re.Pattern[str]) -> list[str]:
    flat = re.sub(r"\s+", " ", text)
    return [s for s in re.split(r"(?<=[.;])\s+", flat) if pattern.search(s)]


def test_releasing_branching_model_names_phase_and_unit_branches(releasing):
    section = _branching_model(releasing)
    assert _branch_patterns(section) == {"phase/<id>", "unit/<unit id>"}
    assert _worktree_patterns(section) == {"<unit id>", "phase-<id>"}
    assert "phase/<NID>" in section, "RELEASING.md should map its <id> to the plan's <NID>"
    assert "git worktree add .claude/worktrees/<unit id> -b unit/<unit id> phase/<id>" in section
    assert "git switch -c phase/<id> origin/main" in section


def test_runner_template_uses_the_same_names_as_releasing(releasing, plan):
    template = _runner_template(plan)
    section = _branching_model(releasing)
    assert _branch_patterns(template) == _branch_patterns(section)
    assert _worktree_patterns(template) == _worktree_patterns(section)
    assert "docs/RELEASING.md" in template


def test_section_7_0_branch_convention_agrees(releasing, plan):
    conventions = _section(plan, "### 7.0 Conventions every phase uses", level="### ")
    bullet = next(line for line in conventions.splitlines() if line.startswith("- **Branches.**"))
    assert _branch_patterns(bullet) == _branch_patterns(_branching_model(releasing))
    assert "<unit id>" in _worktree_patterns(bullet)


@pytest.mark.parametrize("which", ["releasing", "template"])
def test_remediation_branch_name_appears_only_as_retired(which, releasing, plan):
    text = releasing if which == "releasing" else _runner_template(plan)
    for sentence in _sentences_with(text, _REMEDIATION):
        assert re.search(r"\b(retired|never|not)\b", sentence), (
            f"{which} uses remediation/<...> as a live branch name: {sentence!r}"
        )


def test_http_only_scans_retirement_is_owner_gated(releasing):
    section = _section(releasing, RETIRE_HEADING, level="#")
    flat = re.sub(r"\s+", " ", section)
    for needle in (
        "ksarrafi",
        "D-REL2",
        "D-REL4",
        "P2B.8",
        "D-N2 (b)",
        "N4.4",
        "waive",
        "The owner runs every command in this section; an agent only prepares them",
    ):
        assert needle in flat, needle
    gates = [m.start() for m in re.finditer(r"^\d+\. \*\*", section, flags=re.MULTILINE)]
    assert len(gates) == 4, "expected four numbered gates"
    for i, start in enumerate(gates):
        end = gates[i + 1] if i + 1 < len(gates) else section.index(DELETE_CMD)
        gate = section[start:end]
        assert "Verify" in gate and "`" in gate[gate.index("Verify") :], f"gate {i + 1} has no Verify command"
    assert "merge-base --is-ancestor origin/feature/http-only-scans origin/main" in section
    assert "git worktree list | grep -F '[feature/http-only-scans]'" in section
    # The delete comes after every gate, and only here.
    assert section.index(DELETE_CMD) > gates[-1]
    assert releasing.count(DELETE_CMD) == 1


def test_branching_model_points_at_the_gated_retirement(releasing):
    bullets = _branching_model(releasing).split(RETIRE_HEADING)[0]
    bullet = next(b for b in bullets.split("\n- ") if b.startswith("`feature/http-only-scans`"))
    assert "(#retiring-featurehttp-only-scans)" in bullet
    flat = re.sub(r"\s+", " ", bullet)
    assert "The owner retires it" in flat and "No agent deletes it." in flat


def test_no_other_doc_deletes_http_only_scans():
    deleting = re.compile(
        r"--delete\s+feature/http-only-scans|:refs/heads/feature/http-only-scans\b"
        r"|branch\s+-[dD]\s+feature/http-only-scans"
    )
    offenders = []
    for path in [REPO_ROOT / "RESUME_HERE.md", *sorted((REPO_ROOT / "docs").rglob("*.md"))]:
        if path == RELEASING:
            continue
        if deleting.search(path.read_text(encoding="utf-8", errors="replace")):
            offenders.append(str(path.relative_to(REPO_ROOT)))
    assert not offenders, f"only RELEASING.md's gated section may delete the branch: {offenders}"


def test_retirement_tip_is_the_1_5_4_release_commit(releasing):
    row = re.search(r"^\| 1\.5\.4 \| `([0-9a-f]{9,40})` \|", releasing, flags=re.MULTILINE)
    assert row, "release log has no 1.5.4 row"
    tip = row.group(1)
    bullet = _branching_model(releasing).split(RETIRE_HEADING)[0]
    retire = _section(releasing, RETIRE_HEADING, level="#")
    assert f"`{tip}`" in bullet and "`v1.5.4`" in bullet
    assert f"`{tip}`" in retire and "v1.5.4" in retire


def _slug(heading: str) -> str:
    return re.sub(r"[^\w\- ]", "", heading.strip().lower()).replace(" ", "-")


def test_every_in_page_link_resolves(releasing):
    slugs: dict[str, int] = {}
    anchors = set()
    in_fence = False
    for line in releasing.splitlines():
        if line.lstrip().startswith("```"):
            in_fence = not in_fence
            continue
        m = None if in_fence else re.match(r"^#{1,6} (.+)$", line)
        if m:
            base = _slug(m.group(1))
            n = slugs.get(base, 0)
            anchors.add(base if n == 0 else f"{base}-{n}")
            slugs[base] = n + 1
    links = set(re.findall(r"\]\(#([^)]+)\)", releasing))
    assert "retiring-featurehttp-only-scans" in anchors
    assert links, "expected in-page links"
    assert not links - anchors, sorted(links - anchors)
