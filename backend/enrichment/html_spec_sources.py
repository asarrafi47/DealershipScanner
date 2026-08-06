"""
Official HTML specification pages as a first-class citable source, alongside PDFs.

WHY THIS MODULE EXISTS
----------------------
The brochure lane resolves a document to ``{file, page}``. Three high-volume
makes were written off as "publishes nothing we can use", and re-read on
2026-08-02 that verdict says only **no ``.pdf`` hrefs were found**. Honda
publishes its per-trim equipment grid as an HTML table on hondanews.com, one
release per model year, and has done so continuously since 2011. An official
HTML spec page is exactly as citable as a brochure page; it needs a different
locator, which is what this module defines.

WHAT A CITATION MEANS HERE
--------------------------
A PDF bullet cites ``{file, page}``: re-open the file, confirm the sha256 the
transcript recorded, re-extract the page, check the sentence is printed on it.
An HTML bullet cites, and every element of this is written into the overlay
entry:

  ``url``               the page it came from, on an allowlisted official host
  ``retrieved_at``      UTC instant of the fetch that produced our copy
  ``document_sha256``   sha256 of the exact bytes we stored
  ``locator``           ``table[t]/row[r]/col[c]`` into the parsed structure of
                        THOSE bytes, plus the human-readable ``section`` and
                        ``column_header`` that address the same cell in words

Index locators are only meaningful because the bytes are pinned: the sha256 is
checked before the indices are used, so a page that changes cannot silently
re-point a citation at a different row. A changed page fails the hash and the
citation goes unverified — it does not get re-resolved against the new text.
That is also why **live re-fetching is not verification**: pages change, and a
claim checked against today's page says nothing about the page we quoted.

The stored HTML is verification substrate, exactly as the PDFs are. Nothing
serves it, links to it, or renders it. What ships is the extracted fact.

WHAT THIS EXTRACTOR WILL AND WILL NOT SAY
-----------------------------------------
Only one shape is read: a grid whose header row names one trim per column, whose
body rows carry a feature label in the first column and a **mark** (``•``, ``S``,
``-``, ``O`` …) in every trim column. That is the HTML analogue of the brochure
equipment grid, and it is the only shape in which the document itself says which
trim a feature belongs to.

Refused, deliberately:

* rows carrying measured VALUES rather than marks (``Horsepower … 192 @ 6,000``).
  Honda writes ``<<`` for "same as the cell to its left" in those rows; resolving
  ``<<`` means composing a claim the cell does not print, so value rows are
  skipped whole. This is the single largest thing left on the table — see
  :data:`REFUSED_BY_DESIGN`.
* any row whose cell count does not match the header's, so a shifted row can
  never hand a mark to the wrong trim;
* any table whose header row cannot be identified, or whose column names are not
  distinct, non-empty and short;
* any table on a page we cannot attribute to one year/make/model.

The column→trim mis-attribution failure has bitten this project repeatedly, so
attribution is proven per cell and re-proven at verification time rather than
assumed from position.

ONE DECLARED TRANSFORMATION
---------------------------
``<sup>`` content is dropped when reading cell text, so the page's footnote
reference markers do not end up inside a feature name (``Blind Spot Information
System (BSI)14``). Nothing else is altered: entities are decoded, whitespace is
collapsed. The rule is applied by the writer and by :func:`verify_citation`
identically, from the same stored bytes, so it stays re-checkable. Every other
edit to a quoted string is prohibited — the PDF lane already shipped
"Blind Spot Warning and Rear Cross Traffic Alert" for two printed list items and
had to walk it back.
"""

from __future__ import annotations

import hashlib
import html as html_module
import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urljoin, urlparse

from backend.enrichment.brochure_sources import (
    assess_text_quality,
    is_official_url,
    normalized_model_token,
    verify_document_identity,
)
from backend.enrichment.dictionary_paths import DERIVED_DIR

_REPO_ROOT = Path(__file__).resolve().parents[2]

#: Where fetched HTML is stored. Verification substrate only, same standing as
#: ``backend/data/brochures``: re-openable by a build step, never served.
HTML_SPEC_PAGES_DIR = _REPO_ROOT / "backend" / "data" / "spec_pages"

#: Transcripts. Same role as ``derived/brochure_text`` and deliberately NOT the
#: same directory: every consumer of ``brochure_text`` resolves ``source_pdf``
#: and would report an HTML transcript as a lost brochure.
HTML_SPEC_TEXT_DIR = DERIVED_DIR / "html_spec_text"

#: Overlay ``source`` string for ladders built from these pages. It is not in
#: ``brochure_extract.ADMISSIBLE_OVERLAY_SOURCES``, so nothing written by this
#: module renders anywhere until that table is edited on purpose. Fail closed.
OVERLAY_SOURCE = "html_spec_quoted"

#: What this extractor refuses to read out of a page it has already fetched and
#: stored, with the reason. Recorded so the gap is visible rather than implied.
REFUSED_BY_DESIGN: dict[str, str] = {
    "value_rows": (
        "rows whose trim cells carry measured values (engine type, horsepower, "
        "curb weight, dimensions). Honda prints '<<' for 'as the cell to my "
        "left' in these rows; the cell does not print the value, so quoting one "
        "there would be composition. Measured 2026-08-02 over the 20 Honda "
        "pages this lane holds: 2,378 data rows read and 1,288 refused "
        "(35.1%), of which 1,262 are value rows"
    ),
    "optional_marks": (
        "'O'/optional marks are parsed and recorded on the row, but no bullet is "
        "emitted from one -- 'available on this trim' is a different claim from "
        "'standard on this trim' and the ladder renders the latter"
    ),
    "leftmost_column": (
        "the first trim column of each powertrain group has no column to its "
        "left in the same group, so no add can be stated for it from this grid"
    ),
}


# --------------------------------------------------------------------------
# Mark vocabulary
# --------------------------------------------------------------------------

#: Cell contents that mean "standard on this trim".
STANDARD_MARKS: frozenset[str] = frozenset({"•", "●", "▪", "✓", "✔", "s", "std"})

#: Cell contents that mean "not available on this trim".
ABSENT_MARKS: frozenset[str] = frozenset({"-", "–", "—", "n/a", "na", "--"})

#: Cell contents that mean "optional/available". Parsed, never emitted.
OPTIONAL_MARKS: frozenset[str] = frozenset({"o", "opt", "optional", "p", "pkg"})

MARK_VOCABULARY: frozenset[str] = STANDARD_MARKS | ABSENT_MARKS | OPTIONAL_MARKS

MARK_STANDARD = "standard"
MARK_ABSENT = "absent"
MARK_OPTIONAL = "optional"


def classify_mark(cell: str) -> str | None:
    """``'standard'`` / ``'absent'`` / ``'optional'``, or ``None`` if not a mark.

    An EMPTY cell is not a mark. A blank in a trim column is ambiguous — the
    page may mean "no", or the author may have left it out — and guessing is the
    mis-attribution failure in miniature. Fail closed.
    """
    text = (cell or "").strip().lower().rstrip(".")
    if not text:
        return None
    if text in STANDARD_MARKS:
        return MARK_STANDARD
    if text in ABSENT_MARKS:
        return MARK_ABSENT
    if text in OPTIONAL_MARKS:
        return MARK_OPTIONAL
    return None


# --------------------------------------------------------------------------
# Table parsing
# --------------------------------------------------------------------------

#: Longest a cell may be and still be read as a trim name.
MAX_TRIM_NAME_CHARS = 40

#: Fewest trim columns a grid must have. Two columns cannot express a ladder and
#: are usually a two-column prose layout.
MIN_TRIM_COLUMNS = 3


@dataclass(frozen=True)
class Cell:
    text: str
    tag: str
    colspan: int


@dataclass
class Row:
    """One ``<tr>``, with colspans expanded so column index == trim index."""

    index: int
    cells: tuple[Cell, ...]
    expanded: tuple[str, ...]

    @property
    def width(self) -> int:
        return len(self.expanded)

    @property
    def is_full_span(self) -> bool:
        """A heading row: one cell covering every column."""
        return len(self.cells) == 1 and self.cells[0].colspan >= max(1, self.width)

    @property
    def label(self) -> str:
        return self.expanded[0] if self.expanded else ""


class _TableParser(HTMLParser):
    """Rows and cells of every ``<table>``, with ``<sup>`` content dropped."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.tables: list[list[list[Cell]]] = []
        self._table: list[list[Cell]] | None = None
        self._row: list[Cell] | None = None
        self._cell_tag: str | None = None
        self._buf: list[str] = []
        self._colspan = 1
        self._sup_depth = 0

    # -- structure --------------------------------------------------------
    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "table":
            if self._table is not None:
                self.tables.append(self._table)
            self._table = []
        elif tag == "tr" and self._table is not None:
            self._flush_cell()
            self._row = []
        elif tag in ("td", "th") and self._row is not None:
            self._flush_cell()
            self._cell_tag = tag
            self._buf = []
            self._sup_depth = 0
            raw = dict(attrs).get("colspan") or "1"
            try:
                self._colspan = max(1, min(64, int(str(raw).strip())))
            except ValueError:
                self._colspan = 1
        elif tag == "sup" and self._cell_tag is not None:
            self._sup_depth += 1
        elif tag == "br" and self._cell_tag is not None and self._sup_depth == 0:
            self._buf.append(" ")

    def handle_endtag(self, tag: str) -> None:
        if tag in ("td", "th"):
            self._flush_cell()
        elif tag == "sup":
            if self._sup_depth:
                self._sup_depth -= 1
        elif tag == "tr":
            self._flush_cell()
            if self._row is not None and self._table is not None:
                self._table.append(self._row)
            self._row = None
        elif tag == "table":
            self._flush_cell()
            if self._row is not None and self._table is not None:
                self._table.append(self._row)
            self._row = None
            if self._table is not None:
                self.tables.append(self._table)
            self._table = None

    def handle_data(self, data: str) -> None:
        if self._cell_tag is not None and self._sup_depth == 0:
            self._buf.append(data)

    # -- cells ------------------------------------------------------------
    def _flush_cell(self) -> None:
        if self._cell_tag is None:
            return
        text = clean_cell_text("".join(self._buf))
        if self._row is not None:
            self._row.append(Cell(text=text, tag=self._cell_tag, colspan=self._colspan))
        self._cell_tag = None
        self._buf = []
        self._colspan = 1
        self._sup_depth = 0

    def close(self) -> None:  # pragma: no cover - defensive
        super().close()
        self._flush_cell()
        if self._row is not None and self._table is not None:
            self._table.append(self._row)
            self._row = None
        if self._table is not None:
            self.tables.append(self._table)
            self._table = None


_WS = re.compile(r"\s+")


def clean_cell_text(raw: str) -> str:
    """Decoded, whitespace-collapsed cell text. No other edit."""
    text = html_module.unescape(raw or "")
    text = text.replace(" ", " ").replace("​", "")
    return _WS.sub(" ", text).strip()


def parse_tables(html: str) -> list[list[Row]]:
    """Every ``<table>`` on the page as rows with colspans expanded."""
    parser = _TableParser()
    try:
        parser.feed(html or "")
        parser.close()
    except Exception:  # noqa: BLE001 - malformed markup is expected
        pass

    tables: list[list[Row]] = []
    for raw_rows in parser.tables:
        rows: list[Row] = []
        for index, raw_cells in enumerate(raw_rows):
            expanded: list[str] = []
            for cell in raw_cells:
                expanded.extend([cell.text] * cell.colspan)
            rows.append(
                Row(index=index, cells=tuple(raw_cells), expanded=tuple(expanded))
            )
        tables.append(rows)
    return tables


# --------------------------------------------------------------------------
# Header detection: proving a column belongs to a trim
# --------------------------------------------------------------------------


@dataclass
class HeaderVerdict:
    ok: bool
    reason: str = ""
    row_index: int = -1
    width: int = 0
    trims: tuple[str, ...] = ()
    label_cell: str = ""
    #: Per-column group label from the row above (Honda: "Gas Engine" /
    #: "Hybrid"), ``""`` where the page states none.
    groups: tuple[str, ...] = ()


def _looks_like_trim_name(text: str) -> bool:
    if not text or len(text) > MAX_TRIM_NAME_CHARS:
        return False
    if not re.search(r"[0-9a-z]", text, re.I):
        return False
    if classify_mark(text) is not None:
        return False
    return True


def find_header_row(rows: list[Row]) -> HeaderVerdict:
    """
    The row that names one trim per column, or a refusal saying why not.

    Requirements, all of them:

    * the table has a single dominant width and the header row has it;
    * every column after the first holds a short, non-empty, non-mark name;
    * those names are DISTINCT — a repeated name means the row is a spanning
      group header (Honda's "Gas Engine / Gas Engine / Hybrid / Hybrid …")
      rather than the trim row, and reading it as trims would attribute four
      columns of marks to one name;
    * the header sits above at least one readable feature row.
    """
    if not rows:
        return HeaderVerdict(False, "table has no rows")

    widths = [row.width for row in rows if row.width > 1]
    if not widths:
        return HeaderVerdict(False, "table has no multi-column row")
    width = max(set(widths), key=widths.count)
    if width - 1 < MIN_TRIM_COLUMNS:
        return HeaderVerdict(
            False, f"table is {width} columns wide; need {MIN_TRIM_COLUMNS + 1}"
        )

    for row in rows:
        if row.width != width or row.is_full_span:
            continue
        names = row.expanded[1:]
        if not all(_looks_like_trim_name(name) for name in names):
            continue
        if len({name.casefold() for name in names}) != len(names):
            continue
        return HeaderVerdict(
            ok=True,
            row_index=row.index,
            width=width,
            trims=tuple(names),
            label_cell=row.label,
            groups=_group_labels(rows, row.index, width),
        )
    return HeaderVerdict(False, "no row names one distinct trim per column")


def _group_labels(rows: list[Row], header_index: int, width: int) -> tuple[str, ...]:
    """Per-column group names printed directly above the header row, if any."""
    for row in rows:
        if row.index != header_index - 1:
            continue
        if row.width != width or row.is_full_span:
            return ()
        names = row.expanded[1:]
        # A group row is one whose spanning cells repeat; if every name is
        # distinct it is not a grouping and we say nothing.
        if len({n.casefold() for n in names}) == len(names):
            return ()
        return tuple(names)
    return ()


# --------------------------------------------------------------------------
# Feature rows
# --------------------------------------------------------------------------


@dataclass
class FeatureRow:
    row_index: int
    label: str
    marks: tuple[str, ...]  # one of MARK_* per trim column
    section: str = ""
    subsection: str = ""

    @property
    def section_path(self) -> str:
        return " > ".join(p for p in (self.section, self.subsection) if p)


@dataclass
class SpecGrid:
    table_index: int
    header: HeaderVerdict
    features: tuple[FeatureRow, ...]
    #: Rows the table has that were not read, by reason. Reported, not hidden.
    refused_rows: dict[str, int] = field(default_factory=dict)

    @property
    def trims(self) -> tuple[str, ...]:
        return self.header.trims


def read_grid(rows: list[Row], table_index: int) -> tuple[SpecGrid | None, str]:
    """A parsed grid, or ``(None, reason)``."""
    header = find_header_row(rows)
    if not header.ok:
        return None, header.reason

    section = ""
    subsection = ""
    features: list[FeatureRow] = []
    refused: dict[str, int] = {}

    for row in rows:
        if row.index <= header.row_index:
            continue
        if row.is_full_span or (row.width == 1):
            text = row.label
            if not text:
                continue
            if row.cells and row.cells[0].tag == "th":
                section, subsection = text, ""
            else:
                subsection = text
            continue
        if row.width != header.width:
            refused["width_mismatch"] = refused.get("width_mismatch", 0) + 1
            continue
        label = row.label
        if not label:
            refused["no_row_label"] = refused.get("no_row_label", 0) + 1
            continue
        marks = [classify_mark(cell) for cell in row.expanded[1:]]
        if any(mark is None for mark in marks):
            refused["not_a_mark_row"] = refused.get("not_a_mark_row", 0) + 1
            continue
        if MARK_STANDARD not in marks:
            refused["no_standard_mark"] = refused.get("no_standard_mark", 0) + 1
            continue
        features.append(
            FeatureRow(
                row_index=row.index,
                label=label,
                marks=tuple(str(m) for m in marks),
                section=section,
                subsection=subsection,
            )
        )

    if not features:
        return None, "table has no readable feature row"
    return (
        SpecGrid(
            table_index=table_index,
            header=header,
            features=tuple(features),
            refused_rows=refused,
        ),
        "",
    )


def read_grids(html: str) -> tuple[list[SpecGrid], list[str]]:
    """Every readable grid on the page, and one refusal reason per rejection."""
    grids: list[SpecGrid] = []
    refusals: list[str] = []
    for table_index, rows in enumerate(parse_tables(html)):
        grid, reason = read_grid(rows, table_index)
        if grid is None:
            refusals.append(f"table[{table_index}]: {reason}")
        else:
            grids.append(grid)
    return grids, refusals


# --------------------------------------------------------------------------
# The citation
# --------------------------------------------------------------------------

LOCATOR_RE = re.compile(r"^table\[(\d+)\]/row\[(\d+)\]/col\[(\d+)\]$")


def locator_for(table_index: int, row_index: int, column: int) -> str:
    return f"table[{table_index}]/row[{row_index}]/col[{column}]"


def parse_locator(locator: str) -> tuple[int, int, int] | None:
    match = LOCATOR_RE.match((locator or "").strip())
    if not match:
        return None
    return int(match.group(1)), int(match.group(2)), int(match.group(3))


def sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass
class Citation:
    """One rendered bullet's full re-checkable address."""

    text: str
    trim: str
    url: str
    retrieved_at: str
    document_sha256: str
    locator: str
    column_header: str
    section: str
    #: The column immediately to the left in the same printed group, and its
    #: locator. An "add" is only written when the SAME ROW prints an absent-mark
    #: there, so both halves of the claim are cells in the document.
    relative_to: str = ""
    relative_locator: str = ""
    #: EVERY column to the left in the same group, all of which must print an
    #: absent-mark on this row. Adjacency alone is not enough — see
    #: :func:`build_citations`.
    absent_columns: tuple[int, ...] = ()

    def to_json(self) -> dict:
        payload = {
            "text": self.text,
            "url": self.url,
            "retrieved_at": self.retrieved_at,
            "document_sha256": self.document_sha256,
            "locator": self.locator,
            "column_header": self.column_header,
            "section": self.section,
        }
        if self.relative_to:
            payload["relative_to"] = self.relative_to
            payload["relative_locator"] = self.relative_locator
            payload["absent_columns"] = list(self.absent_columns)
        return payload


def _group_of(header: HeaderVerdict, column: int) -> str:
    """Group label of trim column ``column`` (1-based into ``expanded``)."""
    if not header.groups:
        return ""
    index = column - 1
    return header.groups[index] if 0 <= index < len(header.groups) else ""


def build_citations(
    grid: SpecGrid,
    *,
    url: str,
    retrieved_at: str,
    document_sha256: str,
) -> dict[str, list[Citation]]:
    """
    Per trim, the features the grid marks standard on it and absent on EVERY
    trim column to its left within the same printed group.

    Three rules, and all three are cells rather than judgement:

    * **Never across a group boundary.** Honda's Accord grid puts LX/SE under
      "Gas Engine" and Sport/EX-L/Sport-L/Touring under "Hybrid"; differencing
      the first hybrid column against the last gas column would state a
      powertrain change as a feature add.
    * **Absent on every column to the left, not just the adjacent one.**
      Adjacency alone was tried first and it reads a step BACK as an add: the
      2026 Prologue grid is ``EX (FWD) | EX (AWD) | Touring (FWD) | Touring
      (AWD) | Elite (AWD)``, and adjacency had "Touring (FWD) adds Single Motor
      Front-Wheel Drive" — true of the cells, nonsense as a claim, because
      EX (FWD) two columns left already had it. Requiring absence all the way
      left drops it, and drops the 2026 Pilot's "Touring adds Compact Spare
      Tire" (Sport and EX-L print it too; only TrailSport's full-size spare
      breaks the run).
    * **The leftmost column of a group states nothing**, having no column to its
      left to be absent on.

    What survives is still not "this trim is better than that one" — it is
    "this cell says standard and every cell left of it in this group says not
    available", which is precisely what verification re-checks.
    """
    out: dict[str, list[Citation]] = {}
    for column, trim in enumerate(grid.trims, start=1):
        group = _group_of(grid.header, column)
        left_columns = [
            c
            for c in range(1, column)
            if _group_of(grid.header, c) == group
        ]
        if not left_columns:
            continue
        adjacent = left_columns[-1]
        if adjacent != column - 1:
            # A gap means an intervening column of another group; the run to the
            # left is not contiguous and we say nothing about it.
            continue
        bullets: list[Citation] = []
        for feature in grid.features:
            if feature.marks[column - 1] != MARK_STANDARD:
                continue
            if any(feature.marks[c - 1] != MARK_ABSENT for c in left_columns):
                continue
            bullets.append(
                Citation(
                    text=feature.label,
                    trim=trim,
                    url=url,
                    retrieved_at=retrieved_at,
                    document_sha256=document_sha256,
                    locator=locator_for(grid.table_index, feature.row_index, column),
                    column_header=trim,
                    section=feature.section_path,
                    relative_to=grid.trims[adjacent - 1],
                    relative_locator=locator_for(
                        grid.table_index, feature.row_index, adjacent
                    ),
                    absent_columns=tuple(left_columns),
                )
            )
        if bullets:
            out[trim] = bullets
    return out


# --------------------------------------------------------------------------
# Verification: re-open the stored bytes and confirm the cell
# --------------------------------------------------------------------------

VERIFY_OK = "verified"
FAIL_NO_DOCUMENT = "stored_html_not_held"
FAIL_SHA_MISMATCH = "stored_html_sha256_mismatch"
FAIL_BAD_LOCATOR = "locator_unparseable"
FAIL_OUT_OF_RANGE = "locator_out_of_range"
FAIL_HEADER_MOVED = "column_header_does_not_match"
FAIL_TEXT_NOT_PRINTED = "text_not_printed_in_cited_row"
FAIL_NOT_STANDARD = "cited_cell_is_not_a_standard_mark"
FAIL_RELATIVE_NOT_ABSENT = "relative_cell_is_not_an_absent_mark"
FAIL_INCOMPLETE = "citation_incomplete"

_NON_ALNUM = re.compile(r"[^0-9a-z]+")
_GLYPHS = str.maketrans(
    {"’": "'", "‘": "'", "“": '"', "”": '"', "–": "-", "—": "-", "−": "-", " ": " "}
)


def normalize(text: object) -> str:
    """Alphanumeric token sequence, lowercased — the PDF verifier's rule."""
    return _NON_ALNUM.sub(" ", str(text or "").translate(_GLYPHS).lower()).strip()


def verify_citation(entry: dict, payload: bytes) -> str:
    """
    Re-derive one citation from the stored bytes. Returns a ``FAIL_*`` or
    :data:`VERIFY_OK`.

    This re-parses the document from scratch. It does not consult the transcript,
    the overlay, or any register this lane wrote: asking a citation register
    whether it issued a citation proves nothing about a document, and two stores
    were already caught certifying themselves that way.
    """
    text = str(entry.get("text") or "").strip()
    locator = str(entry.get("locator") or "").strip()
    recorded_sha = str(entry.get("document_sha256") or "").strip()
    header_name = str(entry.get("column_header") or "").strip()
    if not (text and locator and recorded_sha and header_name):
        return FAIL_INCOMPLETE
    if not payload:
        return FAIL_NO_DOCUMENT
    if sha256_bytes(payload) != recorded_sha:
        return FAIL_SHA_MISMATCH

    parsed = parse_locator(locator)
    if parsed is None:
        return FAIL_BAD_LOCATOR
    table_index, row_index, column = parsed

    tables = parse_tables(payload.decode("utf-8", errors="replace"))
    if table_index >= len(tables):
        return FAIL_OUT_OF_RANGE
    rows = tables[table_index]
    header = find_header_row(rows)
    if not header.ok:
        return FAIL_OUT_OF_RANGE
    if column < 1 or column >= header.width:
        return FAIL_OUT_OF_RANGE
    if normalize(header.trims[column - 1]) != normalize(header_name):
        return FAIL_HEADER_MOVED

    row = next((r for r in rows if r.index == row_index), None)
    if row is None or row.width != header.width:
        return FAIL_OUT_OF_RANGE
    if normalize(row.label) != normalize(text):
        return FAIL_TEXT_NOT_PRINTED
    if classify_mark(row.expanded[column]) != MARK_STANDARD:
        return FAIL_NOT_STANDARD

    relative_locator = str(entry.get("relative_locator") or "").strip()
    if relative_locator:
        relative = parse_locator(relative_locator)
        if relative is None:
            return FAIL_BAD_LOCATOR
        _, rel_row, rel_column = relative
        if rel_row != row_index or not (1 <= rel_column < header.width):
            return FAIL_OUT_OF_RANGE
        if normalize(header.trims[rel_column - 1]) != normalize(
            str(entry.get("relative_to") or "")
        ):
            return FAIL_HEADER_MOVED
        # Every column the citation claims is absent must still be absent, not
        # just the adjacent one. This is the check that stops a step BACK being
        # rendered as an add.
        columns = entry.get("absent_columns")
        columns = list(columns) if isinstance(columns, list) else [rel_column]
        if rel_column not in columns:
            return FAIL_RELATIVE_NOT_ABSENT
        for candidate in columns:
            if not isinstance(candidate, int) or not (1 <= candidate < header.width):
                return FAIL_OUT_OF_RANGE
            if classify_mark(row.expanded[candidate]) != MARK_ABSENT:
                return FAIL_RELATIVE_NOT_ABSENT
    return VERIFY_OK


# --------------------------------------------------------------------------
# Storage
# --------------------------------------------------------------------------


def catalog_key(year: int, make: str, model: str) -> str:
    return f"{year}|{make.strip().lower()}|{normalized_model_token(make, model)}"


def stem_for(year: int, make: str, model: str) -> str:
    make_token = re.sub(r"[^a-z0-9]+", "", (make or "").lower())
    return f"{year}__{make_token}__{normalized_model_token(make, model)}"


def stored_page_path(year: int, make: str, model: str) -> Path:
    return HTML_SPEC_PAGES_DIR / f"{stem_for(year, make, model)}.html"


def transcript_path(year: int, make: str, model: str) -> Path:
    return HTML_SPEC_TEXT_DIR / f"{stem_for(year, make, model)}.json"


def grid_text(grid: SpecGrid) -> str:
    """Flat text of a grid — what the identity and quality gates read."""
    lines = [grid.header.label_cell, "\t".join(grid.header.trims)]
    for feature in grid.features:
        lines.append(f"{feature.section_path}\t{feature.label}\t" + "\t".join(feature.marks))
    return "\n".join(lines)


#: Tags that end a line block. The identity gate measures how long the block
#: holding a model name is — a name in a 3,500-character run reads as prose, a
#: name in a heading reads as a declaration — so flattening the whole page into
#: one line would make every name look like prose and the gate would only ever
#: pass on grid text. Preserving the page's own block structure is what lets a
#: heading count as a heading.
_BLOCK_TAGS = (
    "p", "div", "br", "li", "tr", "td", "th", "h1", "h2", "h3", "h4", "h5", "h6",
    "section", "article", "header", "footer", "nav", "table", "ul", "ol", "dd",
    "dt", "figcaption", "blockquote", "title", "option",
)
_BLOCK_BOUNDARY = re.compile(
    r"(?is)</?(?:" + "|".join(_BLOCK_TAGS) + r")\b[^>]*>"
)


def page_text(html: str, grids: list[SpecGrid]) -> str:
    """Text the gates see: the grids, plus the page's own visible text."""
    stripped = re.sub(r"(?is)<(script|style)[^>]*>.*?</\1>", " ", html or "")
    stripped = _BLOCK_BOUNDARY.sub("\n", stripped)
    stripped = re.sub(r"(?s)<[^>]+>", " ", stripped)
    lines = [
        _WS.sub(" ", line).strip()
        for line in html_module.unescape(stripped).splitlines()
    ]
    visible = "\n".join(line for line in lines if line)
    return "\n".join([*(grid_text(g) for g in grids), visible])


@dataclass
class StoredPage:
    year: int
    make: str
    model: str
    url: str
    retrieved_at: str
    sha256: str
    bytes: int
    path: Path
    title: str = ""


def store_page(
    payload: bytes,
    *,
    year: int,
    make: str,
    model: str,
    url: str,
    retrieved_at: str,
    title: str = "",
    directory: Path | None = None,
) -> StoredPage:
    """Write the fetched bytes to disk exactly as received."""
    root = directory or HTML_SPEC_PAGES_DIR
    root.mkdir(parents=True, exist_ok=True)
    path = root / f"{stem_for(year, make, model)}.html"
    path.write_bytes(payload)
    return StoredPage(
        year=year,
        make=make,
        model=model,
        url=url,
        retrieved_at=retrieved_at,
        sha256=sha256_bytes(payload),
        bytes=len(payload),
        path=path,
        title=title,
    )


def _relative_to_repo(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(_REPO_ROOT))
    except ValueError:
        return str(path)


def build_transcript(page: StoredPage, grids: list[SpecGrid]) -> dict:
    """
    The transcript, in the shape ``derived/brochure_text`` uses, with the
    document identity fields swapped for HTML's.

    ``pages`` is kept as the key so a reader of both stores sees one shape; a
    "page" here is one TABLE, and ``page`` is its index, which is what a
    locator addresses. ``source_pdf`` is deliberately absent: a consumer that
    resolves it will find nothing and refuse the citation rather than resolve
    it to the wrong kind of document.
    """
    return {
        "catalog_key": catalog_key(page.year, page.make, page.model),
        "year": page.year,
        "make": page.make,
        "model": page.model,
        "source_media_type": "text/html",
        "source_html": _relative_to_repo(page.path),
        "source_html_sha256": page.sha256,
        "source_url": page.url,
        "source_title": page.title,
        "retrieved_at": page.retrieved_at,
        "bytes": page.bytes,
        "text_transformations": ["sup-elements dropped", "entities decoded", "whitespace collapsed"],
        "page_count": len(grids),
        "pages": [
            {
                "page": grid.table_index,
                "trim_hint": True,
                "trims": list(grid.trims),
                "header_row": grid.header.row_index,
                "column_groups": list(grid.header.groups),
                "refused_rows": dict(grid.refused_rows),
                "text": grid_text(grid),
                "rows": [
                    {
                        "row": feature.row_index,
                        "section": feature.section_path,
                        "label": feature.label,
                        "marks": list(feature.marks),
                    }
                    for feature in grid.features
                ],
            }
            for grid in grids
        ],
    }


def write_transcript(transcript: dict, *, directory: Path | None = None) -> Path:
    root = directory or HTML_SPEC_TEXT_DIR
    root.mkdir(parents=True, exist_ok=True)
    stem = str(transcript.get("catalog_key") or "").replace("|", "__")
    path = root / f"{stem}.json"
    path.write_text(json.dumps(transcript, indent=1, ensure_ascii=False), encoding="utf-8")
    return path


def build_overlay(
    page: StoredPage,
    grids: list[SpecGrid],
    transcript_rel_path: str,
) -> dict | None:
    """
    A ``trim_adds_by_year`` overlay in the same shape the brochure lane writes.

    ``source`` is :data:`OVERLAY_SOURCE`, which no admissibility table lists, so
    this renders nowhere until someone adds it on purpose.
    """
    citations: dict[str, list[Citation]] = {}
    trims: list[str] = []
    for grid in grids:
        for trim in grid.trims:
            if trim not in trims:
                trims.append(trim)
        for trim, bullets in build_citations(
            grid,
            url=page.url,
            retrieved_at=page.retrieved_at,
            document_sha256=page.sha256,
        ).items():
            citations.setdefault(trim, []).extend(bullets)
    if not citations:
        return None
    return {
        "catalog_key": catalog_key(page.year, page.make, page.model),
        "year": page.year,
        "make": page.make,
        "model": page.model,
        "source": OVERLAY_SOURCE,
        "source_document": transcript_rel_path,
        "source_url": page.url,
        "layouts": ["html_mark_grid"],
        "trims_available": trims,
        "trims_quoted": [t for t in trims if t in citations],
        "adds_by_trim": {
            trim: [c.text for c in bullets] for trim, bullets in citations.items()
        },
        "adds_provenance": {
            trim: [
                dict(c.to_json(), source=transcript_rel_path)
                for c in bullets
            ]
            for trim, bullets in citations.items()
        },
    }


# --------------------------------------------------------------------------
# Identity gate
# --------------------------------------------------------------------------


def verify_page_identity(
    html: str, grids: list[SpecGrid], year: int, make: str, model: str
) -> tuple[bool, str]:
    """
    The page must be attributable to THIS year/make/model, or it is refused.

    Two checks, both of which must pass, and neither of which is loosened for
    HTML:

    1. :func:`brochure_sources.verify_document_identity` with ``require_year``
       ON. Unlike a brochure PDF — a third of which never print their model year
       — a spec release is titled with its year, so demanding it costs nothing
       and stops a channel page's neighbouring-year release being stored under
       the wrong key.
    2. at least one grid whose header LABEL CELL names the model. Honda prints
       "2026 HONDA ACCORD SPECIFICATIONS & FEATURES" in the corner cell of the
       grid, which ties the columns to the vehicle rather than to the page.

    Check 2 is not belt-and-braces; on this corpus it is the check that works.
    Measured 2026-08-02 against the stored ``2026__honda__accord.html``: the
    page-level check ALONE passes when asked "is this the 2026 CR-V?", because a
    newsroom page carries a model navigation strip and "CR-V" therefore appears
    in a short block. Only the grid corner cell refuses it. A brochure PDF has
    no such navigation, which is why the PDF lane never needed this and why
    copying the PDF gate over unchanged would have been wrong.
    """
    text = page_text(html, grids)
    ok, reason = verify_document_identity(text, year, make, model, require_year=True)
    if not ok:
        return False, reason
    quality = assess_text_quality(text)
    if not quality.ok:
        return False, quality.reason
    for grid in grids:
        label = grid.header.label_cell
        if not label:
            continue
        named, _ = verify_document_identity(label, year, make, model, require_year=True)
        if named:
            return True, ""
    return False, (
        "no grid header cell names this year and model; the columns cannot be "
        "tied to the vehicle"
    )


# --------------------------------------------------------------------------
# Honda discovery
# --------------------------------------------------------------------------
#
# READ THE HREFS, DO NOT GUESS THE SLUGS. The Toyota fix in
# ``brochure_sources`` had to learn this: toyota.com/brochures/ silently
# redirects into one category, so a constructed URL resolved the whole line-up
# against a list with no trucks. Honda's chain is three anchors deep and every
# step below is an href read off the page before it:
#
#   1. https://hondanews.com/en-US/          -> "/honda-automobiles/channels/honda-<model>"
#   2. that channel page                     -> "?selectedTabId=<slug>-specs"
#   3. that specs tab                        -> "/en-US/honda-automobiles/releases/release-<id>-<year>-honda-<model>-specifications-features"
#
# Measured 2026-08-02: the home page carries 14 automobile channel anchors; the
# Accord channel's specs tab lists 24 releases spanning model years 2011-2026.

HONDA_NEWSROOM_HOME = "https://hondanews.com/en-US/"

_HONDA_CHANNEL_HREF = re.compile(
    r"^(?:/en-US)?/honda-automobiles/channels/([a-z0-9-]+)/?$", re.I
)


class _AnchorParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.anchors: list[tuple[str, str]] = []
        self._href: str | None = None
        self._buf: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag != "a":
            return
        href = dict(attrs).get("href")
        if href:
            self._href = href
            self._buf = []

    def handle_data(self, data: str) -> None:
        if self._href is not None:
            self._buf.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag == "a" and self._href is not None:
            self.anchors.append((self._href, clean_cell_text("".join(self._buf))))
            self._href = None
            self._buf = []


def anchors(html: str) -> list[tuple[str, str]]:
    parser = _AnchorParser()
    try:
        parser.feed(html or "")
    except Exception:  # noqa: BLE001
        pass
    return parser.anchors


def honda_model_channels(html: str, base_url: str = HONDA_NEWSROOM_HOME) -> dict[str, str]:
    """``{channel slug: absolute URL}`` for every automobile channel anchored."""
    out: dict[str, str] = {}
    for href, _ in anchors(html):
        match = _HONDA_CHANNEL_HREF.match(urlparse(urljoin(base_url, href)).path)
        if match:
            out.setdefault(match.group(1).lower(), urljoin(base_url, href))
    return out


def honda_channel_for_model(channels: dict[str, str], make: str, model: str) -> str | None:
    """
    The channel whose slug IS this model, matched on the normalised token.

    Exact-token only. ``honda-cr-v`` and ``cr-v-e-fcev`` are different vehicles
    and a prefix match would collapse them.
    """
    want = normalized_model_token(make, model)
    for slug, url in channels.items():
        token = re.sub(r"[^a-z0-9]+", "", slug.lower())
        if token in (want, f"honda{want}"):
            return url
    return None


_SPECS_TAB_HREF = re.compile(r"selectedTabId=([a-z0-9-]*specs)", re.I)


def honda_specs_tab_url(html: str, base_url: str) -> str | None:
    """The channel page's own "Specs" tab href."""
    for href, _ in anchors(html):
        if _SPECS_TAB_HREF.search(href):
            return urljoin(base_url, href)
    return None


@dataclass
class SpecRelease:
    url: str
    title: str
    year: int | None


_RELEASE_TITLE_RE = re.compile(
    r"<h5 class=\"content-title\">\s*<a href=\"([^\"]+)\"[^>]*>(.*?)</a>", re.S | re.I
)
_TITLE_YEAR_RE = re.compile(r"^\s*(19|20)(\d{2})\b")


def honda_spec_releases(html: str, base_url: str) -> list[SpecRelease]:
    """
    Releases listed on a specs tab, with the model year read off the TITLE.

    The year comes from the printed title ("2026 Honda Accord Specifications &
    Features"), not from the URL id, which carries none.
    """
    out: list[SpecRelease] = []
    for href, raw_title in _RELEASE_TITLE_RE.findall(html or ""):
        title = clean_cell_text(re.sub(r"(?s)<[^>]+>", " ", raw_title))
        match = _TITLE_YEAR_RE.match(title)
        year = int(match.group(0)) if match else None
        out.append(SpecRelease(url=urljoin(base_url, href), title=title, year=year))
    return out


def pick_spec_release(
    releases: list[SpecRelease], year: int, make: str, model: str
) -> tuple[SpecRelease | None, str]:
    """
    The one release for this year and model, or a refusal.

    The title must start with the model year and must name the model as a whole
    name. Where several qualify (Honda splits body styles: "2017 Honda Accord
    Sedan" / "2017 Accord Coupe" / "2017 Accord Hybrid") this REFUSES rather
    than choosing, because nothing in the title says which of them our inventory
    row means, and picking the first would be the variant mis-attribution the
    brochure lane already had to fix.
    """
    candidates = []
    for release in releases:
        if release.year != year:
            continue
        if "specification" not in release.title.lower():
            continue
        ok, _ = verify_document_identity(
            release.title, year, make, model, require_year=True
        )
        if ok:
            candidates.append(release)
    if not candidates:
        return None, f"no {year} specifications release listed for {make} {model}"
    if len(candidates) > 1:
        titles = " | ".join(c.title for c in candidates)
        return None, (
            f"{len(candidates)} {year} specifications releases match {make} "
            f"{model} and the titles do not say which is this vehicle: {titles}"
        )
    return candidates[0], ""


#: Makes whose HTML spec pages this module can discover end to end. Adding a
#: make means adding its discovery chain and MEASURING it, not listing it.
HTML_SPEC_MAKES: frozenset[str] = frozenset({"honda"})


#: What was measured for the other two makes named in this lane's brief, on
#: 2026-08-02, and why neither is in :data:`HTML_SPEC_MAKES`. These are fetch
#: results, not opinions; re-measure before trusting them.
HTML_SPEC_MAKE_NOTES: dict[str, str] = {
    "honda": (
        "SUPPORTED. hondanews.com publishes a per-model-year 'Specifications & "
        "Features' release whose body is one HTML table with a trim per column. "
        "Chain read off hrefs: home -> /honda-automobiles/channels/honda-<model> "
        "-> ?selectedTabId=<slug>-specs -> release. Measured 2026-08-02"
    ),
    "chevrolet": (
        "NOT SUPPORTED, measured 2026-08-02: "
        "www.chevrolet.com/trucks/silverado/1500 is 1,024,461 bytes with ZERO "
        "<table> elements, and the compare page it anchors "
        "(/shopping/configurator/truck/2026/silverado-1500/.../compare) is "
        "36,016 bytes of app shell, also with none. Nothing server-rendered to "
        "quote"
    ),
    "mercedesbenz": (
        "NOT SUPPORTED, measured 2026-08-02: "
        "www.mbusa.com/en/vehicles/class/gle/suv is 1,294,569 bytes with ZERO "
        "<table> elements; its per-trim compare page "
        "(/en/compare-vehicles/2026-gle-gle350w4) is 779,016 bytes, also zero; "
        "media.mbusa.com home is 486,647 bytes, also zero"
    ),
}


def html_spec_supported(make: str) -> bool:
    return re.sub(r"[^a-z0-9]+", "", (make or "").lower()) in HTML_SPEC_MAKES


def official_spec_page(url: str, make: str) -> bool:
    """Same host allowlist as the brochure lane. No separate door for HTML."""
    return is_official_url(url, make)


__all__ = [
    "Citation",
    "FeatureRow",
    "HTML_SPEC_MAKES",
    "HTML_SPEC_PAGES_DIR",
    "HTML_SPEC_TEXT_DIR",
    "HONDA_NEWSROOM_HOME",
    "OVERLAY_SOURCE",
    "REFUSED_BY_DESIGN",
    "SpecGrid",
    "SpecRelease",
    "StoredPage",
    "build_citations",
    "build_overlay",
    "build_transcript",
    "catalog_key",
    "classify_mark",
    "find_header_row",
    "honda_channel_for_model",
    "honda_model_channels",
    "honda_spec_releases",
    "honda_specs_tab_url",
    "html_spec_supported",
    "locator_for",
    "official_spec_page",
    "page_text",
    "parse_locator",
    "parse_tables",
    "pick_spec_release",
    "read_grids",
    "sha256_bytes",
    "store_page",
    "stored_page_path",
    "transcript_path",
    "utc_now",
    "verify_citation",
    "verify_page_identity",
    "write_transcript",
]
