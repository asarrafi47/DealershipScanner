"""docs/SECURITY_MASTER_TODO.md: the summary table matches the rows (remediation P3.9).

The doc's Summary table once said "0 open" in every phase while SEC-096, SEC-097
and SEC-099 were In progress. These tests read the doc as text (no app, no
network) and pin:

- every "### SEC-nnn" row under a "## Phase N" heading has Status, Scope,
  Outcome, Validation and Last verified fields, Last verified is a date, and the
  Status starts with a known value;
- the Summary table has one line per phase heading, and each line's "Open items"
  count and "Open IDs" list equal the rows in that phase whose Status is open
  (In progress, Blocked or Not started);
- every row from SEC-104 on (security wave 1 and later) has a Changelog line;
- SEC-095 no longer claims Playwright runs Chromium with its sandbox by default
  (Playwright adds --no-sandbox unless chromium_sandbox=True).
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

DOC = Path(__file__).resolve().parents[2] / "docs" / "SECURITY_MASTER_TODO.md"

OPEN_STATUSES = ("In progress", "Blocked", "Not started")
CLOSED_STATUSES = ("Done", "Won't do", "Retired")
REQUIRED_FIELDS = ("Status", "Scope", "Outcome", "Validation", "Last verified")
FIRST_WAVE_ONE_ID = 104

_PHASE_HEADING = re.compile(r"^## Phase (\d+) — (.+)$")
_ROW_HEADING = re.compile(r"^### (SEC-(\d{3})) — (.+)$")
_FIELD_LINE = re.compile(r"^\| \*\*([A-Za-z][A-Za-z -]*)\*\* \| (.*) \|$")
_SEC_ID = re.compile(r"SEC-\d{3}")
_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}\b")


def _lines() -> list[str]:
    return DOC.read_text(encoding="utf-8").splitlines()


def _section(lines: list[str], heading: str) -> list[str]:
    """Lines after an exact "## " heading, up to the next "## " heading."""
    start = lines.index(heading) + 1
    out = []
    for line in lines[start:]:
        if line.startswith("## "):
            break
        out.append(line)
    return out


def _table_rows(lines: list[str]) -> list[list[str]]:
    """Cells of every markdown table body line (header and separator lines dropped)."""
    rows = []
    for line in lines:
        if not line.startswith("|"):
            continue
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if all(set(c) <= set("-: ") for c in cells):
            continue
        rows.append(cells)
    return rows[1:] if rows else rows


def _parse_rows() -> list[dict]:
    """One dict per "### SEC-nnn" row inside a "## Phase N" section."""
    rows: list[dict] = []
    phase = None
    current = None
    for line in _lines():
        if line.startswith("## "):
            m = _PHASE_HEADING.match(line)
            phase = int(m.group(1)) if m else None
            current = None
            continue
        m = _ROW_HEADING.match(line)
        if m and phase is not None:
            current = {"id": m.group(1), "num": int(m.group(2)), "phase": phase, "fields": {}}
            rows.append(current)
            continue
        if current is not None:
            fm = _FIELD_LINE.match(line)
            if fm:
                current["fields"][fm.group(1).strip()] = fm.group(2).strip()
    return rows


def _status_class(status: str) -> str:
    for value in OPEN_STATUSES:
        if status.startswith(value):
            return "open"
    for value in CLOSED_STATUSES:
        if status.startswith(value):
            return "closed"
    return "unknown"


def _summary() -> dict[int, dict]:
    out = {}
    for cells in _table_rows(_section(_lines(), "## Summary")):
        if not cells or not cells[0].isdigit():
            continue
        assert len(cells) >= 4, f"summary line needs Phase | Theme | Open items | Open IDs: {cells}"
        out[int(cells[0])] = {
            "theme": cells[1],
            "count": int(cells[2]),
            "ids": sorted(_SEC_ID.findall(cells[3])),
        }
    return out


def test_doc_has_phase_rows() -> None:
    rows = _parse_rows()
    assert len(rows) > 50
    assert {r["phase"] for r in rows} == {1, 2, 3, 4, 5, 6}


@pytest.mark.parametrize("row", _parse_rows(), ids=lambda r: f"{r['id']}-phase{r['phase']}")
def test_every_row_has_required_fields(row: dict) -> None:
    missing = [f for f in REQUIRED_FIELDS if not row["fields"].get(f)]
    assert not missing, f"{row['id']} lacks {missing}"
    assert _DATE.match(row["fields"]["Last verified"]), f"{row['id']} Last verified is not a date"
    assert _status_class(row["fields"]["Status"]) != "unknown", (
        f"{row['id']} Status {row['fields']['Status']!r} is not one of {OPEN_STATUSES + CLOSED_STATUSES}"
    )


def test_summary_counts_match_row_statuses() -> None:
    rows = _parse_rows()
    summary = _summary()
    phases = {r["phase"] for r in rows}
    assert set(summary) == phases, "the Summary table needs exactly one line per phase heading"
    for phase in sorted(phases):
        open_ids = sorted(
            r["id"] for r in rows if r["phase"] == phase and _status_class(r["fields"]["Status"]) == "open"
        )
        line = summary[phase]
        assert line["count"] == len(open_ids), (
            f"Phase {phase}: summary says {line['count']} open, rows say {len(open_ids)} {open_ids}"
        )
        assert line["ids"] == open_ids, f"Phase {phase}: summary lists {line['ids']}, rows say {open_ids}"


def test_summary_themes_match_phase_headings() -> None:
    headings = {}
    for line in _lines():
        m = _PHASE_HEADING.match(line)
        if m:
            headings[int(m.group(1))] = m.group(2).strip()
    summary = _summary()
    for phase, theme in headings.items():
        assert summary[phase]["theme"].split()[0] == theme.split()[0], (phase, summary[phase]["theme"], theme)


def test_wave_one_rows_have_changelog_lines() -> None:
    changelog = "\n".join(_section(_lines(), "## Changelog"))
    new_rows = [r for r in _parse_rows() if r["num"] >= FIRST_WAVE_ONE_ID]
    assert new_rows, "expected rows SEC-104 onward"
    missing = [r["id"] for r in new_rows if r["id"] not in changelog]
    assert not missing, f"no Changelog line names {missing}"


def test_wave_one_ids_are_unique() -> None:
    ids = [r["id"] for r in _parse_rows() if r["num"] >= FIRST_WAVE_ONE_ID]
    assert len(ids) == len(set(ids)), f"duplicate wave-1 ids: {ids}"


def test_sec_095_states_playwright_sandbox_default() -> None:
    sec095 = next(r for r in _parse_rows() if r["id"] == "SEC-095")
    outcome = sec095["fields"]["Outcome"]
    assert "Playwright runs without `--no-sandbox`" not in outcome
    assert "chromium_sandbox=True" in outcome
