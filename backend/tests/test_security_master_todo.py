"""docs/SECURITY_MASTER_TODO.md: the summary table matches the rows (remediation P3.9).

The doc's Summary table once said "0 open" in every phase while SEC-096, SEC-097
and SEC-099 were In progress. These tests read the doc as text (no app, no
network) and pin:

- every heading that starts with a SEC id is a "### SEC-nnn — Title" row under
  a "## Phase N" heading (a row under Future or Changelog, or with a hyphen for
  the em dash, would be invisible to the summary check);
- every row has Status, Scope, Outcome, Validation and Last verified fields,
  Last verified is a date, and the Status starts with a known value;
- no SEC id names two rows, except the five pre-existing duplicates the doc
  lists (renumbering them is remediation P18B.5);
- the Summary table has one line per phase heading, and each line's "Open items"
  count and "Open IDs" list equal the rows in that phase whose Status is open
  (In progress, Blocked or Not started);
- every row from SEC-104 on (security wave 1 and later) has a Changelog line;
- SEC-095 no longer claims Playwright runs Chromium with its sandbox by default
  (Playwright adds --no-sandbox unless chromium_sandbox=True);
- SEC-110 and the TRUSTED_PROXY_HOPS env row follow the vault-sync script: while
  the script still has a silent hops default, SEC-110 is open and the env row
  warns about it; once the P3.7 guard is merged, SEC-110 is Done and the warning
  and the unmerged-branch note are gone; while deploy/railway/README.md says not
  to run the script as written, the env row says so too (before and after the
  merge), so the security doc never reads as permission to run it.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
DOC = ROOT / "docs" / "SECURITY_MASTER_TODO.md"
SYNC_VAULT = ROOT / "deploy" / "railway" / "sync-vault-to-railway.sh"
RAILWAY_README = ROOT / "deploy" / "railway" / "README.md"
README_DO_NOT_RUN = "Do not run `sync-vault-to-railway.sh` as written"

OPEN_STATUSES = ("In progress", "Blocked", "Not started")
CLOSED_STATUSES = ("Done", "Won't do", "Retired")
REQUIRED_FIELDS = ("Status", "Scope", "Outcome", "Validation", "Last verified")
FIRST_WAVE_ONE_ID = 104

_PHASE_HEADING = re.compile(r"^## Phase (\d+) — (.+)$")
_ROW_HEADING = re.compile(r"^### (SEC-(\d{3})) — (.+)$")
_ANY_SEC_HEADING = re.compile(r"^#+\s*SEC-\d")
_FIELD_LINE = re.compile(r"^\| \*\*([A-Za-z][A-Za-z -]*)\*\* \| (.*) \|$")
_SEC_ID = re.compile(r"SEC-\d{3}")
_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}\b")
# A non-empty shell default such as ${TRUSTED_PROXY_HOPS:-1}: the hazard SEC-110 removes.
_SILENT_HOPS_DEFAULT = re.compile(r"\$\{TRUSTED_PROXY_HOPS:-[^}]")
HOPS_QUALIFIER = "Until SEC-110 merges"
# Pre-existing duplicates (id -> phases of its two rows). Remediation P18B.5 renumbers
# them; drop an id here and from the doc's "Duplicated IDs" note when it does.
KNOWN_DUPLICATE_IDS = {
    "SEC-061": [2, 3],
    "SEC-063": [2, 6],
    "SEC-064": [2, 6],
    "SEC-066": [4, 6],
    "SEC-089": [2, 6],
}


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


def test_every_sec_heading_is_a_phase_row() -> None:
    section = None
    misplaced = []
    for number, line in enumerate(_lines(), start=1):
        if line.startswith("## "):
            section = line
            continue
        if not _ANY_SEC_HEADING.match(line):
            continue
        if not _ROW_HEADING.match(line) or not (section and _PHASE_HEADING.match(section)):
            misplaced.append(f"line {number}: {line!r} (section {section!r})")
    rule = 'every SEC heading must be "### SEC-nnn — Title" under a "## Phase N" heading'
    assert not misplaced, f"{rule}: {'; '.join(misplaced)}"


def test_ids_are_unique_except_known_duplicates() -> None:
    phases_by_id: dict[str, list[int]] = {}
    for row in _parse_rows():
        phases_by_id.setdefault(row["id"], []).append(row["phase"])
    duplicates = {i: sorted(p) for i, p in phases_by_id.items() if len(p) > 1}
    assert duplicates == KNOWN_DUPLICATE_IDS, (
        f"duplicate SEC ids {duplicates}; expected only {KNOWN_DUPLICATE_IDS}. Use the next unused id for a new "
        "row; when P18B.5 renumbers one, drop it from KNOWN_DUPLICATE_IDS and the doc's note"
    )
    note = next(line for line in _lines() if line.startswith("**Duplicated IDs (history):**"))
    assert sorted(_SEC_ID.findall(note)) == sorted(KNOWN_DUPLICATE_IDS), "the doc's Duplicated IDs note is stale"


def test_sec_095_states_playwright_sandbox_default() -> None:
    sec095 = next(r for r in _parse_rows() if r["id"] == "SEC-095")
    outcome = sec095["fields"]["Outcome"]
    assert "Playwright runs without `--no-sandbox`" not in outcome
    assert "chromium_sandbox=True" in outcome


def _env_row(name: str) -> str:
    """The quick-reference table line for one environment variable."""
    prefix = f"| `{name}` |"
    hits = [
        line for line in _section(_lines(), "## Environment variables (quick reference)") if line.startswith(prefix)
    ]
    assert len(hits) == 1, f"expected one env table row for {name}, found {len(hits)}"
    return hits[0]


def test_sec_110_tracks_the_vault_sync_script() -> None:
    script = SYNC_VAULT.read_text(encoding="utf-8")
    sec110 = next(r for r in _parse_rows() if r["id"] == "SEC-110")
    status = sec110["fields"]["Status"]
    hops_row = _env_row("TRUSTED_PROXY_HOPS")
    if README_DO_NOT_RUN in RAILWAY_README.read_text(encoding="utf-8"):
        assert "do not run" in hops_row.lower(), (
            "deploy/railway/README.md says not to run sync-vault-to-railway.sh as written; "
            "the TRUSTED_PROXY_HOPS env row must say so too"
        )
    if _SILENT_HOPS_DEFAULT.search(script):
        assert _status_class(status) == "open", (
            "sync-vault-to-railway.sh still defaults TRUSTED_PROXY_HOPS, so SEC-110 cannot be closed"
        )
        assert HOPS_QUALIFIER in hops_row, (
            "the TRUSTED_PROXY_HOPS env row must warn that the script still sets 1 when the variable is unset"
        )
    else:
        assert status.startswith("Done"), "the P3.7 guard is in the script: set SEC-110 Status to Done"
        assert HOPS_QUALIFIER not in hops_row, (
            "the P3.7 guard is in the script: drop the 'Until SEC-110 merges' qualifier from the env row"
        )
        assert "unit/P3.7" not in sec110["fields"]["Scope"], (
            "the P3.7 guard is in the script: drop the unmerged-branch sentence from SEC-110 Scope"
        )
