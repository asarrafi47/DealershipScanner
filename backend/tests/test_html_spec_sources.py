"""
Tests for official HTML specification pages as a citable source.

The behaviours pinned here are the ones that stop a wrong claim reaching a car
page: a column is only read when the document names the trim it belongs to, a
row is only read when every trim cell is a mark, a comparison never crosses a
printed group boundary, and a citation only verifies against the exact bytes it
was taken from.

Two of the fixtures are LITERAL EXCERPTS of pages this lane fetched on
2026-08-02 (``backend/data/spec_pages/2026__honda__accord.html`` and
``2027__honda__hrv.html``), trimmed to the rows under test. Their shape is the
thing being pinned, so hand-written markup would test the test.
"""

from __future__ import annotations

import pytest

from backend.enrichment import html_spec_sources as hs


# --------------------------------------------------------------------------
# Fixtures
# --------------------------------------------------------------------------

#: Excerpt of the 2026 Accord grid: a spanning powertrain-group row, the trim
#: header, a value row that prints Honda's "<<" carry-forward, and four mark
#: rows including a footnote superscript.
ACCORD_TABLE = """
<table border="1" class="table">
<tbody>
<tr>
<td width="532">&nbsp;</td>
<td colspan="2" style="text-align: center;"><strong>Gas Engine</strong></td>
<td colspan="4" style="text-align: center;"><strong>Hybrid</strong></td>
</tr>
<tr style="background-color: #062060; color: white;">
<td width="532">2026 HONDA ACCORD <br />SPECIFICATIONS &amp; FEATURES</td>
<td style="text-align: center;">LX</td>
<td style="text-align: center;">SE</td>
<td style="text-align: center;">Sport</td>
<td style="text-align: center;">EX-L</td>
<td style="text-align: center;">Sport-L</td>
<td style="text-align: center;">Touring</td>
</tr>
<tr><th colspan="7">POWER UNIT</th></tr>
<tr><td colspan="7"><strong>ENGINE</strong></td></tr>
<tr>
<td>Displacement (cc)</td>
<td>1,498</td><td>&lt;&lt;</td><td>1,993</td><td>&lt;&lt;</td><td>&lt;&lt;</td><td>&lt;&lt;</td>
</tr>
<tr>
<td>Two-Motor Hybrid System</td>
<td>-</td><td>-</td><td>&bull;</td><td>&bull;</td><td>&bull;</td><td>&bull;</td>
</tr>
<tr><th colspan="7">FEATURES</th></tr>
<tr><td colspan="7"><strong>HONDA SENSING&reg;</strong></td></tr>
<tr>
<td>Blind Spot Information System (BSI)<sup>14</sup></td>
<td>-</td><td>&bull;</td><td>&bull;</td><td>&bull;</td><td>&bull;</td><td>&bull;</td>
</tr>
<tr>
<td>Heated Steering Wheel</td>
<td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>&bull;</td>
</tr>
<tr>
<td>Automatic-Dimming Rearview Mirror</td>
<td>-</td><td>-</td><td>-</td><td>&bull;</td><td>-</td><td>&bull;</td>
</tr>
</tbody>
</table>
"""

#: Excerpt of the 2027 HR-V grid: no group row, three trims.
HRV_TABLE = """
<table border="1" class="table"><tbody>
<tr>
<td>2027 HONDA HR-V SPECIFICATIONS &amp; FEATURES</td>
<td>LX</td><td>Sport</td><td>EX-L</td>
</tr>
<tr><th colspan="4">POWER UNIT</th></tr>
<tr><td colspan="4"><strong>CHASSIS</strong></td></tr>
<tr><td>Black Lug Nuts</td><td>-</td><td>&bull;</td><td>&bull;</td></tr>
<tr><td>Heated Front Seats</td><td>-</td><td>-</td><td>&bull;</td></tr>
</tbody></table>
"""

#: The specs-tab listing markup the release links are read out of.
SPECS_TAB_HTML = """
<div class="tab-pane active channel-tab" id="honda-accord-specs">
  <h5 class="content-title">
    <a href="/en-US/honda-automobiles/releases/release-aaa-2026-honda-accord-specifications-features">2026 Honda Accord Specifications &amp; Features
    </a>
  </h5>
  <h5 class="content-title">
    <a href="/en-US/honda-automobiles/releases/release-bbb-2025-honda-accord-specifications-features">2025 Honda Accord Specifications &amp; Features
    </a>
  </h5>
  <h5 class="content-title">
    <a href="/en-US/honda-automobiles/releases/release-ccc-2017-honda-accord-sedan-specifications">2017 Honda Accord Sedan Specifications &amp; Features
    </a>
  </h5>
  <h5 class="content-title">
    <a href="/en-US/honda-automobiles/releases/release-ddd-2017-accord-coupe">2017 Accord Coupe Specifications &amp; Features
    </a>
  </h5>
</div>
"""


def accord_grid() -> hs.SpecGrid:
    grids, refusals = hs.read_grids(ACCORD_TABLE)
    assert refusals == []
    return grids[0]


def hrv_grid() -> hs.SpecGrid:
    grids, _ = hs.read_grids(HRV_TABLE)
    return grids[0]


# --------------------------------------------------------------------------
# Table parsing
# --------------------------------------------------------------------------


def test_colspans_expand_so_column_index_is_trim_index():
    rows = hs.parse_tables(ACCORD_TABLE)[0]
    assert rows[0].expanded == (
        "",
        "Gas Engine",
        "Gas Engine",
        "Hybrid",
        "Hybrid",
        "Hybrid",
        "Hybrid",
    )
    assert all(row.width == 7 for row in rows if not row.is_full_span)


def test_superscript_footnote_markers_are_dropped_from_cell_text():
    """The declared transformation, and the only one."""
    grid = accord_grid()
    labels = [feature.label for feature in grid.features]
    assert "Blind Spot Information System (BSI)" in labels
    assert "Blind Spot Information System (BSI)14" not in labels


def test_entities_decode_but_text_is_otherwise_untouched():
    rows = hs.parse_tables(ACCORD_TABLE)[0]
    assert rows[1].label == "2026 HONDA ACCORD SPECIFICATIONS & FEATURES"


# --------------------------------------------------------------------------
# Header detection: proving a column belongs to a trim
# --------------------------------------------------------------------------


def test_header_row_is_the_trim_row_not_the_powertrain_group_row():
    """
    The regression this guard exists for: the group row above the header spans
    columns, so its expanded form repeats. Reading it as trims would hand four
    columns of marks to "Hybrid".
    """
    verdict = hs.find_header_row(hs.parse_tables(ACCORD_TABLE)[0])
    assert verdict.ok
    assert verdict.row_index == 1
    assert verdict.trims == ("LX", "SE", "Sport", "EX-L", "Sport-L", "Touring")
    assert verdict.groups == (
        "Gas Engine",
        "Gas Engine",
        "Hybrid",
        "Hybrid",
        "Hybrid",
        "Hybrid",
    )


def test_header_refused_when_column_names_repeat():
    table = """
    <table><tr><td>x</td><td>A</td><td>A</td><td>B</td><td>B</td></tr>
    <tr><td>Feature</td><td>-</td><td>&bull;</td><td>&bull;</td><td>&bull;</td></tr></table>
    """
    verdict = hs.find_header_row(hs.parse_tables(table)[0])
    assert not verdict.ok
    assert "distinct trim per column" in verdict.reason


def test_header_refused_when_a_column_name_is_a_mark():
    table = """
    <table><tr><td>x</td><td>LX</td><td>-</td><td>EX</td><td>Touring</td></tr>
    <tr><td>Feature</td><td>-</td><td>&bull;</td><td>&bull;</td><td>&bull;</td></tr></table>
    """
    assert not hs.find_header_row(hs.parse_tables(table)[0]).ok


def test_header_refused_when_a_column_name_is_a_sentence():
    long_name = "Standard on all models except where a package is specified below"
    table = f"""
    <table><tr><td>x</td><td>LX</td><td>{long_name}</td><td>EX</td><td>Touring</td></tr>
    <tr><td>Feature</td><td>-</td><td>&bull;</td><td>&bull;</td><td>&bull;</td></tr></table>
    """
    assert not hs.find_header_row(hs.parse_tables(table)[0]).ok


def test_two_column_table_is_not_a_trim_grid():
    table = "<table><tr><td>x</td><td>LX</td><td>EX</td></tr>" \
            "<tr><td>F</td><td>-</td><td>&bull;</td></tr></table>"
    verdict = hs.find_header_row(hs.parse_tables(table)[0])
    assert not verdict.ok
    assert "columns wide" in verdict.reason


def test_a_page_with_no_table_yields_no_grid_and_a_reason():
    grids, refusals = hs.read_grids("<html><body><p>2026 Accord</p></body></html>")
    assert grids == []
    assert refusals == []


# --------------------------------------------------------------------------
# Row reading
# --------------------------------------------------------------------------


def test_value_rows_are_refused_not_guessed():
    """
    Honda writes "<<" for "same as the cell to my left". Resolving it composes a
    claim the cell does not print, so the whole row is skipped.
    """
    grid = accord_grid()
    assert "Displacement (cc)" not in [f.label for f in grid.features]
    assert grid.refused_rows["not_a_mark_row"] == 1


def test_empty_trim_cell_is_not_read_as_a_no():
    table = """
    <table><tr><td>2026 HONDA ACCORD</td><td>LX</td><td>SE</td><td>EX</td><td>Touring</td></tr>
    <tr><td>Heated Steering Wheel</td><td></td><td>-</td><td>-</td><td>&bull;</td></tr></table>
    """
    grids, refusals = hs.read_grids(table)
    assert grids == []
    assert "no readable feature row" in refusals[0]


def test_section_headings_become_the_human_locator():
    grid = accord_grid()
    by_label = {f.label: f for f in grid.features}
    assert by_label["Two-Motor Hybrid System"].section_path == "POWER UNIT > ENGINE"
    assert (
        by_label["Blind Spot Information System (BSI)"].section_path
        == "FEATURES > HONDA SENSING®"
    )


def test_marks_are_classified_and_blanks_are_not_marks():
    assert hs.classify_mark("•") == hs.MARK_STANDARD
    assert hs.classify_mark("S") == hs.MARK_STANDARD
    assert hs.classify_mark("-") == hs.MARK_ABSENT
    assert hs.classify_mark("N/A") == hs.MARK_ABSENT
    assert hs.classify_mark("O") == hs.MARK_OPTIONAL
    assert hs.classify_mark("") is None
    assert hs.classify_mark("   ") is None
    assert hs.classify_mark("1,498") is None
    assert hs.classify_mark("<<") is None


# --------------------------------------------------------------------------
# Citations
# --------------------------------------------------------------------------


def cite(grid: hs.SpecGrid) -> dict[str, list[hs.Citation]]:
    return hs.build_citations(
        grid, url="https://hondanews.com/x", retrieved_at="2026-08-02T00:00:00Z",
        document_sha256="deadbeef",
    )


def test_no_comparison_crosses_a_printed_group_boundary():
    """
    Sport is the first Hybrid column and SE is the last Gas column. Differencing
    them would state a powertrain change as a feature add, so Sport gets nothing
    from this grid.
    """
    citations = cite(accord_grid())
    assert "Sport" not in citations
    assert "LX" not in citations  # leftmost column of the leftmost group


def test_an_add_requires_a_standard_mark_and_an_absent_mark_on_the_same_row():
    citations = cite(accord_grid())
    touring = {c.text: c for c in citations["Touring"]}
    assert "Heated Steering Wheel" in touring
    assert touring["Heated Steering Wheel"].relative_to == "Sport-L"
    # Standard on every column: nobody "adds" it.
    assert "Two-Motor Hybrid System" not in touring


def test_add_is_stated_against_the_adjacent_column_and_says_which():
    citations = cite(accord_grid())
    ex_l = {c.text: c for c in citations["EX-L"]}
    assert ex_l["Automatic-Dimming Rearview Mirror"].relative_to == "Sport"
    assert ex_l["Automatic-Dimming Rearview Mirror"].column_header == "EX-L"


def test_locator_addresses_the_cell_and_round_trips():
    citation = cite(hrv_grid())["Sport"][0]
    assert citation.text == "Black Lug Nuts"
    table_index, row_index, column = hs.parse_locator(citation.locator)
    rows = hs.parse_tables(HRV_TABLE)[table_index]
    row = next(r for r in rows if r.index == row_index)
    assert row.label == "Black Lug Nuts"
    assert row.expanded[column] == "•"
    assert hs.parse_locator(citation.relative_locator)[2] == column - 1


def test_citation_carries_url_timestamp_and_hash():
    citation = cite(hrv_grid())["EX-L"][0]
    payload = citation.to_json()
    for key in ("url", "retrieved_at", "document_sha256", "locator", "column_header"):
        assert payload[key], f"{key} missing from the citation"


# --------------------------------------------------------------------------
# Verification against the stored bytes
# --------------------------------------------------------------------------


def stored(table_html: str = HRV_TABLE) -> tuple[bytes, dict]:
    payload = table_html.encode("utf-8")
    grids, _ = hs.read_grids(table_html)
    citations = hs.build_citations(
        grids[0],
        url="https://hondanews.com/x",
        retrieved_at="2026-08-02T00:00:00Z",
        document_sha256=hs.sha256_bytes(payload),
    )
    return payload, citations["Sport"][0].to_json()


def test_a_true_citation_verifies():
    payload, entry = stored()
    assert hs.verify_citation(entry, payload) == hs.VERIFY_OK


def test_a_changed_page_fails_the_hash_rather_than_being_re_resolved():
    """
    The whole reason locators may be indices. Pinned bytes cannot silently
    re-point a citation at a different row; a changed page just does not verify.
    """
    payload, entry = stored()
    changed = payload.replace(b"Black Lug Nuts", b"Chrome Lug Nuts")
    assert hs.verify_citation(entry, changed) == hs.FAIL_SHA_MISMATCH


def test_missing_document_fails_closed():
    _, entry = stored()
    assert hs.verify_citation(entry, b"") == hs.FAIL_NO_DOCUMENT


def test_citation_without_a_locator_or_hash_is_incomplete():
    _, entry = stored()
    for key in ("text", "locator", "document_sha256", "column_header"):
        broken = dict(entry)
        broken[key] = ""
        assert hs.verify_citation(broken, b"x") == hs.FAIL_INCOMPLETE


def test_pointing_a_citation_at_another_trims_column_is_caught():
    payload, entry = stored()
    entry["locator"] = hs.locator_for(0, 3, 3)  # EX-L's column, still a "•"
    assert hs.verify_citation(entry, payload) == hs.FAIL_HEADER_MOVED


def test_text_that_is_not_the_cited_rows_label_is_caught():
    payload, entry = stored()
    entry["text"] = "Heated Front Seats"  # a real row, but not row 3
    assert hs.verify_citation(entry, payload) == hs.FAIL_TEXT_NOT_PRINTED


def test_a_cell_that_is_not_a_standard_mark_is_caught():
    payload, entry = stored()
    entry["locator"] = hs.locator_for(0, 3, 1)  # LX's column on the same row: "-"
    entry["column_header"] = "LX"
    assert hs.verify_citation(entry, payload) == hs.FAIL_NOT_STANDARD


def test_the_relative_cell_must_still_be_an_absent_mark():
    payload, entry = stored()
    entry["relative_locator"] = hs.locator_for(0, 3, 3)
    entry["relative_to"] = "EX-L"
    assert hs.verify_citation(entry, payload) == hs.FAIL_RELATIVE_NOT_ABSENT


def test_an_out_of_range_locator_is_caught():
    payload, entry = stored()
    entry["locator"] = hs.locator_for(0, 3, 9)
    assert hs.verify_citation(entry, payload) == hs.FAIL_OUT_OF_RANGE
    entry["locator"] = "row 3, third column"
    assert hs.verify_citation(entry, payload) == hs.FAIL_BAD_LOCATOR


def test_verification_normalises_only_case_whitespace_and_quote_glyphs():
    payload, entry = stored()
    entry["text"] = "black   lug nuts"
    assert hs.verify_citation(entry, payload) == hs.VERIFY_OK
    entry["text"] = "Black Lug Nuts and Wheels"
    assert hs.verify_citation(entry, payload) == hs.FAIL_TEXT_NOT_PRINTED


# --------------------------------------------------------------------------
# Identity: the page must be THIS vehicle
# --------------------------------------------------------------------------


def spec_page(table_html: str) -> str:
    """A page body big enough to clear the shared text-quality floor."""
    filler = " ".join(
        f"Footnote {n}: EPA estimated ratings of 29 city / 37 highway / 32 combined."
        for n in range(1, 60)
    )
    return f"<html><body>{table_html}<div>{filler}</div></body></html>"


def test_identity_accepts_the_page_it_belongs_to():
    html = spec_page(ACCORD_TABLE)
    grids, _ = hs.read_grids(html)
    ok, reason = hs.verify_page_identity(html, grids, 2026, "Honda", "Accord")
    assert ok, reason


def test_identity_refuses_another_model():
    html = spec_page(ACCORD_TABLE)
    grids, _ = hs.read_grids(html)
    ok, _ = hs.verify_page_identity(html, grids, 2026, "Honda", "CR-V")
    assert not ok


def test_identity_refuses_another_year():
    """
    ``require_year`` is ON for HTML. A spec release is titled with its year, so
    demanding it costs nothing and stops the neighbouring year's release being
    filed under this key.
    """
    html = spec_page(ACCORD_TABLE)
    grids, _ = hs.read_grids(html)
    ok, _ = hs.verify_page_identity(html, grids, 2025, "Honda", "Accord")
    assert not ok


def test_identity_refuses_a_page_whose_grid_header_does_not_name_the_vehicle():
    """
    Naming the model somewhere on the page is not enough: the columns have to be
    tied to the vehicle, which is what the grid's corner cell does.
    """
    anonymous = ACCORD_TABLE.replace(
        "2026 HONDA ACCORD <br />SPECIFICATIONS &amp; FEATURES", "SPECIFICATIONS"
    )
    # The page still says what it is; only the grid stops saying it.
    html = "<h1>2026 Honda Accord</h1>" + spec_page(anonymous)
    grids, _ = hs.read_grids(html)
    ok, reason = hs.verify_page_identity(html, grids, 2026, "Honda", "Accord")
    assert not ok
    assert "cannot be tied to the vehicle" in reason


def test_identity_refuses_a_page_too_thin_to_be_a_spec_release():
    grids, _ = hs.read_grids(HRV_TABLE)
    ok, _ = hs.verify_page_identity(HRV_TABLE, grids, 2027, "Honda", "HR-V")
    assert not ok


# --------------------------------------------------------------------------
# Honda discovery: read the hrefs, do not guess the slugs
# --------------------------------------------------------------------------

HOME_HTML = """
<a href="/honda-automobiles/channels/honda-accord">Accord</a>
<a href="/honda-automobiles/channels/honda-cr-v">CR-V</a>
<a href="/honda-automobiles/channels/cr-v-e-fcev">CR-V e:FCEV</a>
<a href="/honda-automobiles/channels/honda-hr-v">HR-V</a>
<a href="https://acuranews.com/en-US/channels/acura-mdx">MDX</a>
<a href="/en-US/powersports">Powersports</a>
"""


def test_model_channels_come_off_the_home_pages_own_hrefs():
    channels = hs.honda_model_channels(HOME_HTML)
    assert set(channels) == {"honda-accord", "honda-cr-v", "cr-v-e-fcev", "honda-hr-v"}
    assert channels["honda-accord"].startswith("https://hondanews.com/")


def test_channel_match_is_exact_and_does_not_collapse_a_sibling_vehicle():
    channels = hs.honda_model_channels(HOME_HTML)
    assert hs.honda_channel_for_model(channels, "Honda", "CR-V").endswith("honda-cr-v")
    # "CR-V e:FCEV" is a different vehicle; a prefix match would take honda-cr-v.
    assert hs.honda_channel_for_model(channels, "Honda", "Pilot") is None


def test_channel_match_refuses_a_body_style_suffixed_inventory_spelling():
    """
    Inventory says "Accord Sedan" and Honda's channel is "honda-accord". No
    alias is invented here: the vehicle is refused and reported, because a
    body-style suffix can also be a different vehicle ("Civic Sedan" vs "Civic
    Hatchback" vs "Civic Type R", all of which have their own release).
    """
    channels = hs.honda_model_channels(HOME_HTML)
    assert hs.honda_channel_for_model(channels, "Honda", "Accord Sedan") is None


def test_specs_tab_url_comes_off_the_channel_page():
    channel = "<a href='/en-US/honda-automobiles/channels/honda-accord?selectedTabId=honda-accord-specs'>Specs</a>"
    url = hs.honda_specs_tab_url(channel, "https://hondanews.com/en-US/honda-automobiles/channels/honda-accord")
    assert url and url.endswith("selectedTabId=honda-accord-specs")


def test_release_year_is_read_off_the_printed_title_not_the_url():
    releases = hs.honda_spec_releases(SPECS_TAB_HTML, "https://hondanews.com/")
    assert [r.year for r in releases] == [2026, 2025, 2017, 2017]
    assert releases[0].title == "2026 Honda Accord Specifications & Features"


def test_the_right_year_is_picked():
    releases = hs.honda_spec_releases(SPECS_TAB_HTML, "https://hondanews.com/")
    release, reason = hs.pick_spec_release(releases, 2025, "Honda", "Accord")
    assert release is not None and release.year == 2025, reason


def test_ambiguous_body_styles_are_refused_rather_than_guessed():
    """2017 splits Sedan and Coupe. Nothing says which our row means."""
    releases = hs.honda_spec_releases(SPECS_TAB_HTML, "https://hondanews.com/")
    release, reason = hs.pick_spec_release(releases, 2017, "Honda", "Accord")
    assert release is None
    assert "do not say which is this vehicle" in reason


def test_a_year_with_no_release_is_refused():
    releases = hs.honda_spec_releases(SPECS_TAB_HTML, "https://hondanews.com/")
    release, reason = hs.pick_spec_release(releases, 2019, "Honda", "Accord")
    assert release is None
    assert "no 2019 specifications release" in reason


# --------------------------------------------------------------------------
# The controls this module does not get its own copy of
# --------------------------------------------------------------------------


def test_html_pages_use_the_same_host_allowlist_as_pdfs():
    assert hs.official_spec_page("https://hondanews.com/en-US/x", "Honda")
    assert not hs.official_spec_page("https://automobiles.honda.com/accord", "Honda")
    assert not hs.official_spec_page("https://www.edmunds.com/honda/accord", "Honda")
    assert not hs.official_spec_page("https://hondanews.com/en-US/x", "Toyota")


def test_only_makes_with_a_measured_discovery_chain_are_supported():
    assert hs.html_spec_supported("Honda")
    for make in ("Chevrolet", "Mercedes-Benz", "BMW", "Ford"):
        assert not hs.html_spec_supported(make)


def test_overlay_source_is_not_admissible_yet():
    """
    Nothing this module writes may render until the admissibility table is
    edited on purpose. Fail closed.
    """
    from backend.enrichment.brochure_extract import ADMISSIBLE_OVERLAY_SOURCES

    assert hs.OVERLAY_SOURCE not in ADMISSIBLE_OVERLAY_SOURCES


def test_transcript_does_not_claim_to_be_a_pdf():
    """
    A consumer that resolves ``source_pdf`` must find nothing and refuse, rather
    than resolve an HTML transcript to the wrong kind of document.
    """
    payload = HRV_TABLE.encode("utf-8")
    page = hs.StoredPage(
        year=2027, make="Honda", model="HR-V", url="https://hondanews.com/x",
        retrieved_at="2026-08-02T00:00:00Z", sha256=hs.sha256_bytes(payload),
        bytes=len(payload), path=hs.stored_page_path(2027, "Honda", "HR-V"),
    )
    transcript = hs.build_transcript(page, [hrv_grid()])
    assert "source_pdf" not in transcript
    assert transcript["source_media_type"] == "text/html"
    assert transcript["source_html_sha256"] == page.sha256
    assert transcript["catalog_key"] == "2027|honda|hrv"


def test_stored_pages_and_transcripts_live_outside_the_pdf_stores():
    assert hs.HTML_SPEC_TEXT_DIR.name == "html_spec_text"
    assert "brochure_text" not in str(hs.HTML_SPEC_TEXT_DIR)
    assert hs.HTML_SPEC_PAGES_DIR.name == "spec_pages"


@pytest.mark.parametrize(
    "year,make,model,expected",
    [
        (2026, "Honda", "Accord", "2026__honda__accord"),
        (2026, "Honda", "CR-V", "2026__honda__crv"),
        (2027, "Honda", "HR-V", "2027__honda__hrv"),
    ],
)
def test_stems_follow_the_corpus_convention(year, make, model, expected):
    assert hs.stem_for(year, make, model) == expected


# --------------------------------------------------------------------------
# Absence must hold all the way left, not just next door
# --------------------------------------------------------------------------

#: The 2026 Prologue header, which interleaves drivetrains and has no group row.
#: Adjacency alone read "Touring (FWD) adds Single Motor Front-Wheel Drive".
PROLOGUE_TABLE = """
<table><tbody>
<tr>
<td>2026 HONDA PROLOGUE SPECIFICATIONS &amp; FEATURES</td>
<td>EX (FWD)</td><td>EX (AWD)</td><td>Touring (FWD)</td><td>Touring (AWD)</td>
</tr>
<tr><th colspan="5">POWER UNIT</th></tr>
<tr>
<td>Single Motor Front-Wheel Drive</td>
<td>&bull;</td><td>-</td><td>&bull;</td><td>-</td>
</tr>
<tr>
<td>Bose Premium Audio System</td>
<td>-</td><td>-</td><td>&bull;</td><td>&bull;</td>
</tr>
</tbody></table>
"""


def test_a_step_back_is_not_an_add():
    """
    ``Single Motor Front-Wheel Drive`` is standard on Touring (FWD) and absent on
    the column next to it — but EX (FWD), two columns left, prints it too. The
    cells are true; the claim would not be.
    """
    grids, _ = hs.read_grids(PROLOGUE_TABLE)
    citations = hs.build_citations(
        grids[0], url="u", retrieved_at="t", document_sha256="d"
    )
    touring = [c.text for c in citations["Touring (FWD)"]]
    assert "Single Motor Front-Wheel Drive" not in touring
    assert "Bose Premium Audio System" in touring


def test_an_add_records_every_column_it_claims_is_absent():
    grids, _ = hs.read_grids(PROLOGUE_TABLE)
    citation = hs.build_citations(
        grids[0], url="u", retrieved_at="t", document_sha256="d"
    )["Touring (FWD)"][0]
    assert citation.absent_columns == (1, 2)
    assert citation.relative_to == "EX (AWD)"


def test_verification_rechecks_every_claimed_absent_column():
    payload = PROLOGUE_TABLE.encode("utf-8")
    grids, _ = hs.read_grids(PROLOGUE_TABLE)
    entry = hs.build_citations(
        grids[0], url="u", retrieved_at="t",
        document_sha256=hs.sha256_bytes(payload),
    )["Touring (FWD)"][0].to_json()
    assert hs.verify_citation(entry, payload) == hs.VERIFY_OK

    # Claim a column that is NOT absent on this row and it fails, even though
    # the adjacent column still checks out. The bullet is "Bose Premium Audio
    # System", which column 4 (Touring AWD) also prints as standard.
    entry["absent_columns"] = [1, 2, 4]
    assert hs.verify_citation(entry, payload) == hs.FAIL_RELATIVE_NOT_ABSENT


def test_the_adjacent_column_must_be_among_the_claimed_absent_columns():
    payload = PROLOGUE_TABLE.encode("utf-8")
    grids, _ = hs.read_grids(PROLOGUE_TABLE)
    entry = hs.build_citations(
        grids[0], url="u", retrieved_at="t",
        document_sha256=hs.sha256_bytes(payload),
    )["Touring (FWD)"][0].to_json()
    entry["absent_columns"] = [1]
    assert hs.verify_citation(entry, payload) == hs.FAIL_RELATIVE_NOT_ABSENT
