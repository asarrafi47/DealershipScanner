"""Tests for OEM brochure source resolution.

The behaviours pinned here are the ones that protect the corpus from wrong
data: official-host-only filtering, robots.txt failing closed, mandatory
year+model matching, and filenames that round-trip to the right catalog_key.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

import backend.enrichment.brochure_sources as bs
from backend.enrichment.brochure_extract import parse_brochure_filename
from backend.enrichment.brochure_sources import (
    BLOCKED_BY_THEIR_EDGE,
    BLOCKED_BY_THEIR_RULES,
    BLOCKED_BY_US,
    DERIVED_STORES_FROM_BROCHURE_TEXT,
    ContentIndex,
    DerivedStore,
    DocLink,
    PacedFetcher,
    RobotsPolicy,
    apply_derived_quarantine,
    assess_text_quality,
    brochure_pdf_filename,
    brochure_text_filename,
    derived_quarantine_reason,
    discovery_pages,
    extract_document_links,
    gap_group_key,
    is_official_url,
    looks_like_pdf,
    model_token_variants,
    normalized_model_token,
    official_hosts_for,
    page_scoped_document,
    pick_best_document,
    plan_derived_quarantine,
    probe_target,
    quarantined_document_keys,
    score_document_link,
    unsupported_reason,
    verify_document_identity,
)
from backend.enrichment.dictionary_catalog import catalog_key
from backend.scripts.fetch_oem_brochures import build_gap_list


# --------------------------------------------------------------------------
# Official-host allowlist
# --------------------------------------------------------------------------


def test_official_host_accepts_manufacturer_press_cdn():
    assert is_official_url(
        "https://news.mazdausa.com/download/2026+Mazda+CX-50+Spec+Deck.pdf", "Mazda"
    )
    assert is_official_url(
        "https://www.kia.com/us/en/download/specifications/pdf/23020", "Kia"
    )


@pytest.mark.parametrize(
    "url",
    [
        "https://en.wikipedia.org/wiki/Mazda_CX-5",
        "https://www.auto-brochures.com/mazda.html",
        "https://www.somemazdadealer.com/brochures/2026-cx-5.pdf",
        "https://autocatalogarchive.com/mazda/cx5.pdf",
        "https://news.mazdausa.com.evil.example/spec.pdf",
    ],
)
def test_official_host_rejects_non_oem_sources(url):
    """Wikipedia/aggregator/dealer prose is what poisoned this feature before."""
    assert not is_official_url(url, "Mazda")


def test_official_host_is_per_make():
    """A Mazda document URL is not an acceptable source for a Kia."""
    url = "https://news.mazdausa.com/download/spec.pdf"
    assert is_official_url(url, "Mazda")
    assert not is_official_url(url, "Kia")


def test_unregistered_make_has_no_hosts_and_a_stated_reason():
    # Chevrolet used to be the example here. It was registered on 2026-08-01
    # (its hosts answer; it publishes no PDF), so the example moved to a make
    # that still has no host at all. Chevrolet's new state is covered by
    # test_registered_but_unresolved_make_still_reports_unsupported below.
    assert official_hosts_for("Lexus") == frozenset()
    assert unsupported_reason("Lexus")
    assert unsupported_reason("Mazda") is None


def test_ford_and_lincoln_hosts_are_registered_but_still_yield_nothing():
    """The largest gap in the fleet. Ford's own hosts are on the allowlist as a
    reviewed edit; every one of them refuses automated access, so the make must
    still report a reason rather than look fetchable.
    """
    from backend.enrichment.brochure_sources import HOST_ACCESS_NOTES

    for make in ("Ford", "Lincoln"):
        assert official_hosts_for(make), f"{make} host allowlist missing"
        reason = unsupported_reason(make)
        assert reason and "refus" in reason
    # The refusal is a measurement, not an assertion: EVERY registered host has
    # a recorded observation behind it, apex hosts included. ford.com and
    # lincoln.com answer (301 to the www host) rather than refuse, and leaving
    # them out of the record is how "every Ford host refuses us" got written
    # down without being true of every host.
    for make in ("Ford", "Lincoln"):
        for host in official_hosts_for(make):
            assert host in HOST_ACCESS_NOTES, host
            assert "robots.txt" in HOST_ACCESS_NOTES[host], host


def test_registered_but_unresolved_make_still_reports_unsupported():
    """Subaru has a correct host allowlist but a JS-rendered index: it yields
    nothing. Reporting it as supported would overstate what the fetcher can do.
    """
    assert official_hosts_for("Subaru")  # host allowlist is registered
    assert unsupported_reason("Subaru")


def test_every_registered_host_is_a_manufacturer_domain():
    """The allowlist is the only thing between us and an aggregator PDF."""
    from backend.enrichment.brochure_sources import OFFICIAL_HOSTS

    allowed_suffixes = (
        "mazdausa.com",
        "kiamedia.com",
        "kia.com",
        "toyota.com",
        "nissanusa.com",
        "subaru.com",
        "hyundainews.com",
        "hyundaiusa.com",
        "vw.com",
        "infinitiusa.com",
        "ford.com",
        "lincoln.com",
        # Registered 2026-08-01, all manufacturer-operated:
        "press.bmwgroup.com",
        "bmwusa.com",
        "chevrolet.com",
        "mbusa.com",
        "hondanews.com",
    )
    for make, hosts in OFFICIAL_HOSTS.items():
        for host in hosts:
            assert host.endswith(allowed_suffixes), f"{make}: {host}"


def test_non_http_scheme_rejected():
    assert not is_official_url("file:///etc/passwd", "Mazda")
    assert not is_official_url("ftp://news.mazdausa.com/x.pdf", "Mazda")


# --------------------------------------------------------------------------
# Link extraction
# --------------------------------------------------------------------------

_PAGE = """
<html><body>
  <a href="/download/2026+Mazda+CX-50+Spec+Deck.pdf">2026 CX-50 Spec Deck</a>
  <a href="https://www.auto-brochures.com/mazda/cx50.pdf">CX-50 brochure (aggregator)</a>
  <a href="/image/26MY_Mazda_Safety_Feature_Chart.pdf">Safety Feature Chart</a>
  <a href="/vehicles-2026-cx-50">CX-50 press release</a>
  <a href="javascript:void(0)">menu</a>
</body></html>
"""


def test_extract_document_links_keeps_only_official_pdfs():
    links = extract_document_links(
        _PAGE, "https://news.mazdausa.com/vehicles-2026-cx-50", "Mazda"
    )
    urls = {link.url for link in links}
    assert "https://news.mazdausa.com/download/2026+Mazda+CX-50+Spec+Deck.pdf" in urls
    # Aggregator dropped even though an official page linked to it.
    assert not any("auto-brochures" in url for url in urls)
    # Non-document link dropped.
    assert not any(url.endswith("/vehicles-2026-cx-50") for url in urls)


def test_extract_document_links_handles_extensionless_official_routes():
    html = '<a href="/us/en/download/specifications/pdf/23020">2026 Sportage Specifications</a>'
    links = extract_document_links(
        html, "https://www.kia.com/us/en/models/sportage/2026/specifications", "Kia"
    )
    assert [link.url for link in links] == [
        "https://www.kia.com/us/en/download/specifications/pdf/23020"
    ]


def test_extract_document_links_on_garbage_input_returns_nothing():
    assert extract_document_links("", "https://news.mazdausa.com/", "Mazda") == []
    assert extract_document_links("<<<not html", "https://news.mazdausa.com/", "Mazda") == []


# --------------------------------------------------------------------------
# Scoring: fail closed on year/model mismatch
# --------------------------------------------------------------------------


def test_score_requires_model_token():
    link = DocLink(
        url="https://news.mazdausa.com/image/26MY_Mazda_Safety_Feature_Chart.pdf",
        text="2026 Safety Feature Chart",
    )
    assert score_document_link(link, 2026, "Mazda CX-50").score == 0.0


def test_score_requires_year_token():
    """A 2024 spec deck must never be filed as the 2026 brochure."""
    link = DocLink(
        url="https://news.mazdausa.com/download/2024+Mazda+CX-50+Spec+Deck.pdf",
        text="2024 CX-50 Spec Deck",
    )
    assert score_document_link(link, 2026, "Mazda CX-50").score == 0.0


def test_score_matches_mazda_prefixed_model_names():
    """Inventory says 'Mazda CX-5'; the document says 'CX-5'."""
    link = DocLink(
        url="https://news.mazdausa.com/download/2026+CX5+Spec+Deck.pdf",
        text="2026 CX-5 Spec Deck",
    )
    assert score_document_link(link, 2026, "Mazda CX-5").score > 0


def test_owner_manual_loses_to_spec_deck():
    manual = DocLink(
        url="https://www.mazdausa.com/siteassets/2026/cx-5/2026-cx-5-owners-manual.pdf",
        text="2026 CX-5 Owner's Manual",
    )
    deck = DocLink(
        url="https://news.mazdausa.com/download/2026+CX-5+Spec+Deck.pdf",
        text="2026 CX-5 Spec Deck",
    )
    best = pick_best_document([manual, deck], 2026, "Mazda CX-5")
    assert best is not None and best.url == deck.url


def test_keywords_match_across_oem_filename_separators():
    """Real Mazda filenames join words with '+' / '_'; keywords must still match."""
    plus = DocLink(
        url="https://news.mazdausa.com/download/2026.01.13+26MY+CX5+Spec+Deck+FINAL.pdf"
    )
    underscore = DocLink(url="https://news.mazdausa.com/download/2026_CX-5_Fact_Sheet.pdf")
    assert "spec" in " ".join(score_document_link(plus, 2026, "Mazda CX-5").reasons)
    assert "fact sheet" in " ".join(
        score_document_link(underscore, 2026, "Mazda CX-5").reasons
    )


def test_full_spec_deck_beats_thin_fact_sheet():
    """Both are official; the trim-walk deck is the one worth having."""
    deck = DocLink(
        url="https://news.mazdausa.com/download/2026.01.13+26MY+CX5+Spec+Deck+FINAL.pdf"
    )
    sheet = DocLink(url="https://news.mazdausa.com/download/2026_CX-5_Fact_Sheet.pdf")
    best = pick_best_document([sheet, deck], 2026, "Mazda CX-5")
    assert best is not None and "Spec+Deck" in best.url


def test_single_character_model_token_cannot_match_a_year_digit():
    """'Mazda3' strips to '3', which is a substring of every 2013 document."""
    deck = DocLink(
        url="https://news.mazdausa.com/download/2013+CX-5+Spec+Deck.pdf",
        text="2013 CX-5 Spec Deck",
    )
    assert score_document_link(deck, 2013, "Mazda3", "Mazda").score == 0.0
    own = DocLink(
        url="https://news.mazdausa.com/download/2013+Mazda3+Spec+Deck.pdf",
        text="2013 Mazda3 Spec Deck",
    )
    assert score_document_link(own, 2013, "Mazda3", "Mazda").score > 0


def test_two_digit_model_year_token_counts_as_the_year():
    link = DocLink(url="https://news.mazdausa.com/download/26MY+CX5+Spec+Deck.pdf")
    assert score_document_link(link, 2026, "Mazda CX-5").score > 0


def test_plain_model_never_takes_a_plug_in_hybrid_brochure():
    """Both files live on nissanusa.com/brochures.html and both contain 'rogue'."""
    phev = DocLink(
        url="https://www.nissanusa.com/content/dam/Nissan/us/vehicle-brochures/"
        "2026/2026-nissan-rogue-plug-in-hybrid-brochure-en.pdf",
        text="Download PDF Brochure",
    )
    assert score_document_link(phev, 2026, "Rogue", "Nissan").score == 0.0
    assert "variant mismatch" in phev.reasons[0]


def test_a_variant_model_matches_its_own_brochure():
    phev = DocLink(
        url="https://www.nissanusa.com/content/dam/Nissan/us/vehicle-brochures/"
        "2026/2026-nissan-rogue-plug-in-hybrid-brochure-en.pdf",
        text="Download PDF Brochure",
    )
    assert score_document_link(phev, 2026, "Rogue Plug-in Hybrid", "Nissan").score > 0


def test_a_variant_model_does_not_take_the_base_car_brochure():
    """Symmetric: the qualifier must be on the document too, or we take nothing."""
    base = DocLink(
        url="https://www.toyota.com/content/dam/toyota/brochures/pdf/2026/"
        "rav4_ebrochure.pdf",
        text="Download 2026 PDF",
    )
    assert score_document_link(base, 2026, "RAV4 Plug-in Hybrid", "Toyota").score == 0.0
    assert score_document_link(base, 2026, "RAV4", "Toyota").score > 0


def test_lineup_index_picks_the_right_model_out_of_eleven():
    """The real www.toyota.com/brochures/ index, one entry per nameplate."""
    names = [
        "crown", "camry", "corolla", "corollahatchback", "2026-grcorolla",
        "prius", "priuspluginhybrid", "sienna", "mirai",
    ]
    links = [
        DocLink(
            url=f"https://www.toyota.com/content/dam/toyota/brochures/pdf/2026/"
            f"{n}_ebrochure.pdf",
            text="Download 2026 PDF",
        )
        for n in names
    ]
    best = pick_best_document(links, 2026, "Corolla", "Toyota")
    assert best is not None and best.url.endswith("corolla_ebrochure.pdf")
    best = pick_best_document(links, 2026, "Prius", "Toyota")
    assert best is not None and best.url.endswith("prius_ebrochure.pdf")
    best = pick_best_document(links, 2026, "Prius Plug-in Hybrid", "Toyota")
    assert best is not None and best.url.endswith("priuspluginhybrid_ebrochure.pdf")


def test_wrong_year_is_still_refused_from_a_lineup_index():
    """toyota.com/brochures/ only lists the current year; older years get nothing."""
    links = [
        DocLink(
            url="https://www.toyota.com/content/dam/toyota/brochures/pdf/2026/"
            "camry_ebrochure.pdf",
            text="Download 2026 PDF",
        )
    ]
    assert pick_best_document(links, 2021, "Camry", "Toyota") is None


def test_pick_best_returns_none_when_nothing_qualifies():
    links = [
        DocLink(url="https://news.mazdausa.com/download/warranty.pdf", text="Warranty"),
    ]
    assert pick_best_document(links, 2026, "Mazda CX-5") is None


# --------------------------------------------------------------------------
# Opaque documents: attribution from the page, verified against the PDF
# --------------------------------------------------------------------------

_KIA_SPEC_PAGE = "https://www.kiamedia.com/us/en/models/sportage/2026/specifications"


def _kia_links():
    return extract_document_links(
        '<a href="/us/en/download/specifications/pdf/23020">My Computer</a>',
        _KIA_SPEC_PAGE,
        "Kia",
    )


def test_opaque_document_is_refused_by_filename_scoring():
    """/download/specifications/pdf/23020 names neither the year nor the model."""
    assert pick_best_document(_kia_links(), 2026, "Sportage", "Kia") is None


def test_page_scoped_attribution_accepts_a_single_opaque_document():
    link = page_scoped_document(_kia_links(), _KIA_SPEC_PAGE, 2026, "Kia", "Sportage")
    assert link is not None
    assert link.url.endswith("/download/specifications/pdf/23020")


def test_page_scoped_attribution_refuses_a_page_for_another_vehicle():
    assert page_scoped_document(_kia_links(), _KIA_SPEC_PAGE, 2025, "Kia", "Sportage") is None
    assert page_scoped_document(_kia_links(), _KIA_SPEC_PAGE, 2026, "Kia", "Telluride") is None


def test_page_scoped_attribution_refuses_when_two_documents_are_offered():
    """With two opaque ids there is no way to say which one is this vehicle's."""
    html = (
        '<a href="/us/en/download/specifications/pdf/23020">A</a>'
        '<a href="/us/en/download/specifications/pdf/23021">B</a>'
    )
    links = extract_document_links(html, _KIA_SPEC_PAGE, "Kia")
    assert page_scoped_document(links, _KIA_SPEC_PAGE, 2026, "Kia", "Sportage") is None


def test_downloaded_document_must_name_its_own_model():
    ok, _ = verify_document_identity(
        "2026 SPORTAGE SPECIFICATIONS ...", 2026, "Kia", "Sportage"
    )
    assert ok
    bad, why = verify_document_identity(
        "2026 TELLURIDE SPECIFICATIONS ...", 2026, "Kia", "Sportage"
    )
    assert bad is False and "never mentions the model" in why


def test_identity_check_fails_closed_on_empty_text():
    assert verify_document_identity("", 2026, "Kia", "Sportage")[0] is False
    assert verify_document_identity(None, 2026, "Kia", "Sportage")[0] is False


def test_year_is_only_required_when_the_link_had_no_year_to_score():
    """1,044 of 3,210 corpus files never print their model year; not a default."""
    text = "SPORTAGE SPECIFICATIONS, no model year printed anywhere"
    assert verify_document_identity(text, 2026, "Kia", "Sportage")[0] is True
    strict, why = verify_document_identity(
        text, 2026, "Kia", "Sportage", require_year=True
    )
    assert strict is False and "model year" in why


def test_identity_check_tolerates_the_make_prefixed_spelling():
    ok, _ = verify_document_identity(
        "2026 MAZDA CX-50 SPECIFICATION DECK", 2026, "Mazda", "Mazda CX-50"
    )
    assert ok


# --------------------------------------------------------------------------
# Identity is a bounded, non-incidental naming check -- not substring-anywhere
# --------------------------------------------------------------------------


def test_identity_does_not_match_the_model_inside_a_longer_word():
    """Substring-anywhere matched 'GX' in 'GX460' and 'M' in 'Infiniti Mobile'."""
    for text, make, model in [
        ("2019 LEXUS GX460 SPECIFICATIONS", "Lexus", "GX"),
        ("Infiniti Mobile Entertainment System, power reclining", "Infiniti", "M"),
        ("integrating transmission, differential and AWD transfer case", "Nissan", "GT-R"),
    ]:
        ok, why = verify_document_identity(text, 2019, make, model)
        assert ok is False, f"{model} should not match {text!r}"
        assert "never mentions the model" in why or "long enough" in why


def test_identity_does_not_stitch_a_short_name_out_of_two_words():
    """'SC' used to be found in the 'S (C' of 'ESTIMATES (CITY/HIGHWAY)'."""
    text = "EPA FUEL ECONOMY ESTIMATES (CITY/HIGHWAY)\nAERODYNAMIC DRAG 0.31"
    assert verify_document_identity(text, 2010, "Lexus", "SC")[0] is False


def test_identity_still_cannot_separate_a_space_separated_sibling():
    """Honest limitation: 'ROGUE SPORT' contains 'ROGUE' as a whole word, so
    identity accepts it for the plain Rogue. Variant separation is done by
    score_document_link's qualifier symmetry, not here.
    """
    text = "2022 NISSAN ROGUE SPORT\n" + "equipment " * 400
    assert verify_document_identity(text, 2022, "Nissan", "Rogue Sport")[0] is True
    assert verify_document_identity(text, 2022, "Nissan", "Rogue")[0] is True


def test_identity_accepts_a_name_glued_to_the_next_word_by_extraction():
    """pdfplumber emits 'CX-5Specifications' and 'Mazda3Specifications'; the
    case change is the only word boundary left.
    """
    assert verify_document_identity(
        "CX-5Specifications&Capacities CX-5engine", 2013, "Mazda", "CX-5"
    )[0]
    assert verify_document_identity(
        "Colors, Wheels and Fabrics\n11AVALANCHE-0211", 2011, "Chevrolet", "Avalanche"
    )[0]


def test_identity_rejects_a_name_that_only_appears_in_a_footnote():
    """The real corpus failure: 2026__nissan__roguepluginhybrid is the base
    Rogue brochure, and its only occurrence of the PHEV's name is footnote 30
    on the disclaimer page, hyphenated across a line break.
    """
    disclaimer = (
        "30 EPA-estimated range and fuel economy for Rogue Plug-"
        "In Hybrid SL and Platinum only: all electric range up to 38 miles on a "
        "full charge and a combined electricity + gasoline range of 420 miles. "
        "Combined electricity + gasoline fuel economy estimate of 64 MPGe. Based "
        "on EPA formula of 33.7 kW/hour equal to one gallon of gasoline energy. "
        "Actual range and mileage will vary with trim level and options."
    )
    text = "R O G U E\n2 0 2 6\nCHOOSE YOUR TRIM LEVEL\n" + disclaimer
    ok, why = verify_document_identity(
        text, 2026, "Nissan", "Rogue Plug-In Hybrid"
    )
    assert ok is False
    assert "only inside running prose" in why
    # The same document is a perfectly good source for the plain Rogue.
    assert verify_document_identity(text, 2026, "Nissan", "Rogue")[0] is True


def test_identity_accepts_an_oem_cover_with_letter_spacing():
    """Nissan sets its covers as 'R O G U E'; Mazda hyphenates 'CX-5'."""
    assert verify_document_identity(
        "R O G U E\n2 0 2 6\n" + "x" * 500, 2026, "Nissan", "Rogue"
    )[0]
    assert verify_document_identity(
        "2026 MAZDA CX-5\nSPEC DECK", 2026, "Mazda", "CX-5"
    )[0]


def test_identity_accepts_a_title_wrapped_across_a_line_break():
    """Real BMW captures print '4\\nSERIES COUPE' as a running page header."""
    text = "BMW\n4\nSERIES COUPE\nEXTERIOR COLORS\n" + "x" * 500
    assert verify_document_identity(text, 2017, "BMW", "4 Series")[0]


def test_identity_separator_tolerance_does_not_span_other_words():
    """'MX 5' is the model; 'MAX 5' is not."""
    assert verify_document_identity("MX 5 SPECIFICATIONS", 2016, "Mazda", "MX-5")[0]
    assert verify_document_identity(
        "MAX 5 SPECIFICATIONS", 2016, "Mazda", "MX-5"
    )[0] is False


def test_identity_fails_closed_on_a_model_name_too_short_to_identify():
    """'3' matches the 2013 in every 2013 document and any standalone cell."""
    ok, why = verify_document_identity("2013 SPEC GRID 3 3 3", 2013, "", "3")
    assert ok is False and "long enough" in why


def test_a_single_character_model_falls_back_to_the_manufacturer_spelling():
    """41 corpus files are keyed '<year>__mazda__3'; the document says 'Mazda3'."""
    assert model_token_variants("Mazda", "3") == {"mazda3"}
    assert verify_document_identity(
        "MAZDA3 SPECIFICATIONS\n" + "x" * 400, 2019, "Mazda", "3"
    )[0]
    assert verify_document_identity(
        "MAZDA6 SPECIFICATIONS\n" + "x" * 400, 2019, "Mazda", "3"
    )[0] is False


# --------------------------------------------------------------------------
# robots.txt fails closed
# --------------------------------------------------------------------------


def _policy(responses):
    calls = []

    def fetch(url):
        calls.append(url)
        status, body = responses[url]
        if isinstance(body, Exception):
            raise body
        return status, body

    return RobotsPolicy(fetch), calls


def test_robots_403_blocks_the_whole_host():
    """A site refusing to serve robots.txt is refusing automated access."""
    policy, _ = _policy({"https://www.toyota.com/robots.txt": (403, "")})
    assert policy.allows("https://www.toyota.com/content/ebrochure/2026_tacoma.pdf") is False
    assert policy.verdict_for("www.toyota.com").status == "blocked"


def test_robots_transport_error_blocks_the_host():
    policy, _ = _policy(
        {"https://www.ford.com/robots.txt": (0, ConnectionError("stream reset"))}
    )
    assert policy.allows("https://www.ford.com/brochure.pdf") is False
    assert policy.verdict_for("www.ford.com").status == "error"


def test_robots_404_means_no_policy_so_allow():
    policy, _ = _policy({"https://news.mazdausa.com/robots.txt": (404, "")})
    assert policy.allows("https://news.mazdausa.com/download/spec.pdf") is True
    assert policy.verdict_for("news.mazdausa.com").status == "missing"


def test_robots_disallow_is_honoured_for_the_identified_agent():
    body = "User-agent: Claude-User\nDisallow: /private/\nAllow: /\n"
    policy, _ = _policy({"https://media.subaru.com/robots.txt": (200, body)})
    policy._agent = "Claude-User"
    assert policy.allows("https://media.subaru.com/view-spec.do?bFileId=1") is True
    assert policy.allows("https://media.subaru.com/private/x.pdf") is False


def test_browser_user_agent_is_held_to_the_wildcard_robots_group():
    """Sending a browser string claims no product token, so '*' rules bind us."""
    body = "User-agent: *\nDisallow: /private/\n\nUser-agent: Googlebot\nAllow: /\n"
    policy, _ = _policy({"https://media.subaru.com/robots.txt": (200, body)})
    assert policy._agent == "*"
    assert policy.allows("https://media.subaru.com/pressrelease/x") is True
    assert policy.allows("https://media.subaru.com/private/x.pdf") is False


def test_robots_is_fetched_once_per_host():
    policy, calls = _policy({"https://media.subaru.com/robots.txt": (200, "Allow: /\n")})
    for _ in range(4):
        policy.allows("https://media.subaru.com/a.pdf")
    assert calls.count("https://media.subaru.com/robots.txt") == 1


# --------------------------------------------------------------------------
# Filenames round-trip into the existing corpus layout
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "year,make,model",
    [
        (2026, "Mazda", "Mazda CX-5"),
        (2026, "Kia", "Sportage"),
        (2026, "Subaru", "Crosstrek"),
        (2016, "Jeep", "Grand Cherokee"),
        (2026, "Mercedes-Benz", "GLE"),
        (2026, "Volkswagen", "Atlas"),
    ],
)
def test_pdf_filename_round_trips_to_catalog_key(year, make, model):
    """The extractor derives the corpus key from the filename, so it must match."""
    name = brochure_pdf_filename(year, make, model)
    assert name is not None
    parsed = parse_brochure_filename(Path(name))
    assert parsed is not None
    assert parsed.catalog_key == catalog_key(year, make, model)


def test_pdf_filename_matches_existing_corpus_naming():
    assert brochure_pdf_filename(2016, "Jeep", "Grand Cherokee") == (
        "2016_Jeep_Grand_Cherokee_Brochure.pdf"
    )


def test_brochure_text_filename_matches_corpus_layout():
    assert brochure_text_filename(2012, "Dodge", "Durango") == "2012__dodge__durango.json"
    assert brochure_text_filename(2026, "Mazda", "Mazda CX-5") == "2026__mazda__mazdacx5.json"


def test_pdf_filename_refuses_empty_parts():
    assert brochure_pdf_filename(2026, "", "Tacoma") is None
    assert brochure_pdf_filename(2026, "Toyota", "  ") is None


# --------------------------------------------------------------------------
# Discovery pages stay on official hosts
# --------------------------------------------------------------------------


def test_discovery_pages_are_official_or_empty():
    for make, model in [
        ("Mazda", "Mazda CX-5"),
        ("Kia", "Sportage"),
        ("Subaru", "Crosstrek"),
        ("Hyundai", "Tucson"),
        ("Volkswagen", "Atlas"),
    ]:
        pages = discovery_pages(2026, make, model)
        assert pages, f"no discovery page for {make} {model}"
        assert all(is_official_url(p, make) for p in pages)


def test_no_discovery_pages_for_unsupported_make():
    # Ford has an allowlist but no discovery pattern: no index page has ever
    # been read, so inventing one would be a guess.
    assert discovery_pages(2026, "Ford", "F-150") == []
    assert discovery_pages(2026, "Chevrolet", "Silverado 1500") == []


# --------------------------------------------------------------------------
# Payload sniffing and pacing
# --------------------------------------------------------------------------


def test_looks_like_pdf_rejects_html_error_page():
    assert looks_like_pdf(b"%PDF-1.7\n...")
    assert not looks_like_pdf(b"<!DOCTYPE html><html>Access Denied")
    assert not looks_like_pdf(b"")


def test_fetcher_paces_requests_sequentially():
    """Bursts get this project soft-blocked; spacing is enforced, not advisory."""

    class _Resp:
        status_code = 200
        text = ""
        content = b""
        headers: dict[str, str] = {}

    class _Session:
        def get(self, *_args, **_kwargs):
            return _Resp()

    now = [0.0]
    slept: list[float] = []

    def sleep(seconds):
        slept.append(seconds)
        now[0] += seconds

    fetcher = PacedFetcher(
        delay=4.0, session=_Session(), sleep=sleep, clock=lambda: now[0]
    )
    for _ in range(3):
        fetcher.get_text("https://news.mazdausa.com/x")

    assert slept == [4.0, 4.0]  # no sleep before the first request
    assert fetcher.request_count == 3


def test_quality_gate_rejects_any_decode_failure():
    """Observed on the real 2026 Mazda CX-5 spec deck: 'PREM<FFFD>i<FFFD> PLUS'."""
    text = "2026 MAZDA CX-5 SPECIFICATION DECK 2.5 S PREM�i� PLUS " + ("x" * 5000)
    verdict = assess_text_quality(text)
    assert verdict.ok is False
    assert verdict.replacement_chars == 2
    assert "decode" in verdict.reason


def test_quality_gate_rejects_cover_page_only_extraction():
    """The real 2026 CX-30 deck yielded 390 chars: a lineup list, no equipment."""
    verdict = assess_text_quality("2026 CX-30 SPECIFICATION DECK LINEUP: " + "y" * 300)
    assert verdict.ok is False
    assert "chars" in verdict.reason


def test_quality_gate_accepts_a_real_spec_deck_length():
    verdict = assess_text_quality("CX-50 2.5 S PREFERRED features " * 2000)
    assert verdict.ok is True
    assert verdict.replacement_chars == 0


def test_quality_gate_rejects_empty_and_none():
    assert assess_text_quality("").ok is False
    assert assess_text_quality(None).ok is False


def test_each_user_agent_maps_to_the_robots_group_it_actually_matches():
    """A 'reachable' verdict must never be ambiguous about which UA got it."""
    from backend.enrichment.brochure_sources import (
        BROWSER_USER_AGENT,
        IDENTIFIED_USER_AGENT,
        USER_AGENT,
        robots_agent_for,
    )

    assert robots_agent_for(BROWSER_USER_AGENT) == "*"
    assert robots_agent_for(IDENTIFIED_USER_AGENT) == "Claude-User"
    assert "Claude-User" in IDENTIFIED_USER_AGENT
    assert USER_AGENT == BROWSER_USER_AGENT


def test_fetcher_exposes_the_robots_agent_for_its_user_agent():
    from backend.enrichment.brochure_sources import IDENTIFIED_USER_AGENT

    class _Session:
        def get(self, *_args, **_kwargs):
            raise AssertionError("no request expected")

    assert PacedFetcher(session=_Session()).robots_agent == "*"
    assert (
        PacedFetcher(session=_Session(), user_agent=IDENTIFIED_USER_AGENT).robots_agent
        == "Claude-User"
    )


# --------------------------------------------------------------------------
# Gap-list normalisation: one vehicle, one gap
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "make,model,expected",
    [
        ("Mazda", "Mazda CX-50", "cx50"),
        ("Mazda", "Cx-50", "cx50"),
        ("MAZDA", "CX-50", "cx50"),
        ("Mazda", "Mazda3", "3"),  # corpus is 2010__mazda__3.json
        ("Mercedes-Benz", "Mercedes-Benz GLE", "gle"),
        ("Mercedes-Benz", "GLE 350", "gle350"),
        ("RAM", "Ram 1500", "1500"),
        ("Ram", "1500", "1500"),
        ("Land Rover", "Land Rover Range Rover", "rangerover"),
        ("Kia", "Sportage", "sportage"),
        ("Jeep", "Jeep", "jeep"),  # stripping to empty is refused
        ("Kia", "", ""),
        # Drivetrain configuration, not a separate document -- OEMs cover every
        # drivetrain in one brochure, so "Tacoma 4WD" must fold onto "tacoma".
        ("Toyota", "Tacoma 4WD", "tacoma"),
        ("Toyota", "Tacoma 2WD", "tacoma"),
        ("Toyota", "Tundra 4WD", "tundra"),
        ("Subaru", "Outback AWD", "outback"),
        ("BMW", "330i RWD", "330i"),
        # Not a trailing drivetrain token -- must survive untouched.
        ("Mercedes-Benz", "GLE 350 4MATIC", "gle3504matic"),
    ],
)
def test_normalized_model_token_strips_the_make_prefix(make, model, expected):
    assert normalized_model_token(make, model) == expected


def test_gap_group_key_folds_the_known_duplicate_pair():
    """These two spellings sent the first run to fetch one PDF twice."""
    assert gap_group_key(2026, "Mazda", "Mazda CX-50") == gap_group_key(
        2026, "Mazda", "Cx-50"
    )
    assert gap_group_key(2026, "Mazda", "Mazda CX-50") == "2026|mazda|cx50"
    # Different vehicles stay apart.
    assert gap_group_key(2026, "Mazda", "CX-50") != gap_group_key(2026, "Mazda", "CX-5")
    assert gap_group_key(2025, "Mazda", "CX-50") != gap_group_key(2026, "Mazda", "CX-50")


def test_gap_list_counts_one_vehicle_once_and_sums_its_cars():
    rows = [
        (2026, "Mazda", "Mazda CX-50", 30),
        (2026, "Mazda", "Cx-50", 12),
        (2026, "Toyota", "RAV4", 100),
    ]
    gaps, stats = build_gap_list(rows, have=set())
    assert stats["raw_catalog_key_combos"] == 3
    assert stats["distinct_vehicles"] == 2
    assert stats["uncovered_vehicles"] == 2
    cx50 = next(g for g in gaps if g["group_key"] == "2026|mazda|cx50")
    assert cx50["cars"] == 42
    assert len(cx50["variants"]) == 2


def test_gap_list_ranks_by_active_car_count():
    rows = [
        (2026, "Mazda", "CX-50", 5),
        (2026, "Toyota", "RAV4", 900),
        (2026, "Kia", "Sportage", 40),
    ]
    gaps, _ = build_gap_list(rows, have=set())
    assert [g["cars"] for g in gaps] == [900, 40, 5]


def test_a_vehicle_covered_under_one_spelling_is_not_a_gap():
    """Re-fetching an alias spelling is exactly how the duplicate PDF happened."""
    rows = [
        (2026, "Mazda", "Mazda CX-50", 30),
        (2026, "Mazda", "Cx-50", 12),
    ]
    gaps, stats = build_gap_list(rows, have={"2026__mazda__cx50"})
    assert gaps == []
    assert stats["covered_cars"] == 42
    # The uncovered spelling is reported separately: a lookup-key problem, not
    # an acquisition one.
    assert stats["alias_only_keys"] == 1


def test_gap_primary_key_follows_the_make_stripped_corpus_convention():
    rows = [
        (2026, "Mazda", "Mazda CX-50", 30),  # more cars, but prefixed
        (2026, "Mazda", "Cx-50", 12),
    ]
    gaps, _ = build_gap_list(rows, have=set())
    assert gaps[0]["primary_catalog_key"] == "2026__mazda__cx50"
    assert gaps[0]["alias_catalog_keys"] == ["2026__mazda__mazdacx50"]


def test_gap_entries_carry_the_reason_a_make_yields_nothing():
    gaps, _ = build_gap_list([(2026, "Ford", "F-150", 10)], have=set())
    assert gaps[0]["unsupported_reason"]
    gaps, _ = build_gap_list([(2026, "Mazda", "CX-50", 10)], have=set())
    assert gaps[0]["unsupported_reason"] is None


# --------------------------------------------------------------------------
# Content-hash dedupe: one document, one copy
# --------------------------------------------------------------------------


def test_content_index_refuses_to_store_the_same_bytes_twice(tmp_path):
    """The real regression: one CX-50 spec deck, two model spellings."""
    index = ContentIndex(tmp_path / "index.json")
    payload = b"%PDF-1.7\n" + b"x" * 4096

    doc_a, new_a = index.register(
        payload,
        preferred_path=tmp_path / "2026_Mazda_Mazda_CX-50_Brochure.pdf",
        source_url="https://news.mazdausa.com/download/2026+Mazda+CX-50+Spec+Deck.pdf",
        catalog_key_str="2026|mazda|mazdacx50",
        fetched_at="2026-07-31T00:00:00+00:00",
    )
    doc_b, new_b = index.register(
        payload,
        preferred_path=tmp_path / "2026_Mazda_Cx-50_Brochure.pdf",
        source_url="https://news.mazdausa.com/download/2026+Mazda+CX-50+Spec+Deck.pdf",
        catalog_key_str="2026|mazda|cx50",
        fetched_at="2026-07-31T00:00:01+00:00",
    )

    assert new_a is True and new_b is False
    assert doc_a.sha256 == doc_b.sha256
    assert len(index.by_sha) == 1
    assert not (tmp_path / "2026_Mazda_Cx-50_Brochure.pdf").exists()
    assert sorted(set(doc_b.catalog_keys)) == ["2026|mazda|cx50", "2026|mazda|mazdacx50"]


def test_content_index_stores_genuinely_different_documents_separately(tmp_path):
    index = ContentIndex(tmp_path / "index.json")
    for name, body in (("a.pdf", b"%PDF-a"), ("b.pdf", b"%PDF-b")):
        index.register(
            body,
            preferred_path=tmp_path / name,
            source_url=f"https://news.mazdausa.com/{name}",
            catalog_key_str="2026|mazda|x",
            fetched_at="",
        )
    assert len(index.by_sha) == 2


def test_scan_directory_reports_duplicates_already_on_disk(tmp_path):
    (tmp_path / "one.pdf").write_bytes(b"%PDF-same")
    (tmp_path / "two.pdf").write_bytes(b"%PDF-same")
    (tmp_path / "three.pdf").write_bytes(b"%PDF-other")
    duplicates = ContentIndex(tmp_path / "index.json").scan_directory(tmp_path)
    assert len(duplicates) == 1
    assert sorted(Path(p).name for p in next(iter(duplicates.values()))) == [
        "one.pdf",
        "two.pdf",
    ]


def test_seen_url_makes_a_rerun_free(tmp_path):
    index = ContentIndex(tmp_path / "index.json")
    url = "https://news.mazdausa.com/download/x.pdf"
    assert index.seen_url(url) is None
    index.register(
        b"%PDF-1",
        preferred_path=tmp_path / "x.pdf",
        source_url=url,
        catalog_key_str="2026|mazda|x",
        fetched_at="",
    )
    index.save()
    assert ContentIndex(tmp_path / "index.json").load().seen_url(url) is not None


def test_content_index_survives_a_corrupt_file(tmp_path):
    path = tmp_path / "index.json"
    path.write_text("{not json", encoding="utf-8")
    assert ContentIndex(path).load().by_sha == {}


# --------------------------------------------------------------------------
# Reachability probe
# --------------------------------------------------------------------------


def _probe(page_body, *, robots_body="User-agent: *\nAllow: /\n", status=200):
    def fetch(url):
        if url.endswith("/robots.txt"):
            return 200, robots_body
        return status, page_body

    fetcher = type("F", (), {"get_text": staticmethod(fetch)})()
    return probe_target(
        "mazda",
        "news.mazdausa.com",
        "https://news.mazdausa.com/vehicles-2026-cx-50",
        fetcher,
        RobotsPolicy(fetch, agent="*"),
        "browser",
    )


def test_probe_reports_a_host_that_serves_documents():
    result = _probe(_PAGE)
    assert result.verdict == "serves_documents"
    assert result.document_links >= 1


def test_probe_distinguishes_a_js_shell_from_a_bare_index():
    """media.vw.com and hyundainews.com both answer 200 with a JS shell."""
    assert _probe("<html><body><div id=app></div></body></html>").verdict == "js_shell"
    assert _probe("<html><body>" + "spacer " * 3000 + "</body></html>").verdict == (
        "no_document_links"
    )


def test_probe_separates_no_documents_from_no_allowlist():
    """A page full of PDFs on an unregistered host is a registration gap."""
    html = (
        '<a href="https://www.gmc.com/content/2026_sierra_brochure.pdf">x</a>'
    )

    def fetch(url):
        return (200, "User-agent: *\nAllow: /\n") if url.endswith("robots.txt") else (200, html)

    fetcher = type("F", (), {"get_text": staticmethod(fetch)})()
    result = probe_target(
        "gmc",
        "www.gmc.com",
        "https://www.gmc.com/browse-brochures",
        fetcher,
        RobotsPolicy(fetch, agent="*"),
        "browser",
    )
    assert result.verdict == "documents_but_no_allowlist"
    assert result.any_pdf_links == 1
    assert result.document_links == 0


def test_probe_records_a_403_without_downloading_anything():
    result = _probe("<html>Access Denied</html>", status=403)
    assert result.verdict == "http_403"
    assert result.document_links == 0


def test_probe_reports_a_host_that_refuses_its_own_robots():
    def fetch(url):
        if url.endswith("/robots.txt"):
            return 403, ""
        raise AssertionError("must not fetch the page when robots.txt is refused")

    fetcher = type("F", (), {"get_text": staticmethod(fetch)})()
    result = probe_target(
        "honda",
        "automobiles.honda.com",
        "https://automobiles.honda.com/tools/brochures",
        fetcher,
        RobotsPolicy(fetch, agent="*"),
        "browser",
    )
    assert result.verdict == "robots_blocked"
    assert result.page_status == ""


def test_probe_names_which_side_declined():
    """`we do not allow that host` and `that host will not answer us` are different."""
    refuses = probe_target(
        "honda",
        "automobiles.honda.com",
        "https://automobiles.honda.com/tools/brochures",
        type("F", (), {"get_text": staticmethod(lambda url: (403, ""))})(),
        RobotsPolicy(lambda url: (403, ""), agent="*"),
        "browser",
    )
    assert refuses.verdict == "robots_blocked"
    assert refuses.blocked_by == BLOCKED_BY_THEIR_EDGE

    def published_rules(url):
        if url.endswith("/robots.txt"):
            return 200, "User-agent: *\nDisallow: /\n"
        raise AssertionError("must not fetch a page robots.txt forbids")

    disallowed = probe_target(
        "kia",
        "www.kiamedia.com",
        "https://www.kiamedia.com/us/en/models/sportage/2026/specifications",
        type("F", (), {"get_text": staticmethod(published_rules)})(),
        RobotsPolicy(published_rules, agent="*"),
        "browser",
    )
    assert disallowed.verdict == "robots_disallowed"
    assert disallowed.blocked_by == BLOCKED_BY_THEIR_RULES

    html = '<a href="https://www.gmc.com/2026_sierra_brochure.pdf">x</a>'

    def serves(url):
        if url.endswith("/robots.txt"):
            return 200, "User-agent: *\nAllow: /\n"
        return 200, html

    ours = probe_target(
        "gmc",
        "www.gmc.com",
        "https://www.gmc.com/browse-brochures",
        type("F", (), {"get_text": staticmethod(serves)})(),
        RobotsPolicy(serves, agent="*"),
        "browser",
    )
    assert ours.verdict == "documents_but_no_allowlist"
    assert ours.blocked_by == BLOCKED_BY_US
    assert ours.host_allowlisted is False


def test_probe_records_whether_the_host_is_ours_to_use():
    """An allowlisted host that answers is not `blocked_by` anything."""
    result = _probe(_PAGE)
    assert result.host_allowlisted is True
    assert result.blocked_by == ""
    assert result.to_json()["blocked_by"] == ""


# --------------------------------------------------------------------------
# Derived artifacts of a quarantined document
# --------------------------------------------------------------------------


def _fake_stores(tmp_path):
    """The three real stores, rehomed under tmp_path, keeping each one's rule."""
    return tuple(
        DerivedStore(
            name=s.name,
            live_dir=tmp_path / s.name,
            quarantine_dir=tmp_path / f"{s.name}_quarantine",
            rule=s.rule,
            why=s.why,
        )
        for s in DERIVED_STORES_FROM_BROCHURE_TEXT
    )


def _store(stores, name):
    return next(s for s in stores if s.name == name)


def _write(path: Path, payload) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_the_three_stores_a_brochure_document_feeds_are_all_registered():
    """A store missing from this table is a document that still reaches the page."""
    assert {s.name for s in DERIVED_STORES_FROM_BROCHURE_TEXT} == {
        "brochure_text_slim",
        "trim_candidates",
        "trim_adds_by_year",
    }
    # trim_adds_by_year must stay "citation": moving an UNCITED overlay out would
    # remove the trims_available that blocks Complete_Options CSV substitution,
    # i.e. it would put more text on the page, not less.
    assert _store(DERIVED_STORES_FROM_BROCHURE_TEXT, "trim_adds_by_year").rule == "citation"
    for name in ("brochure_text_slim", "trim_candidates"):
        assert _store(DERIVED_STORES_FROM_BROCHURE_TEXT, name).rule == "excerpt"
    # Siblings, not subdirectories, so <store>.glob("*.json") stops seeing them.
    for store in DERIVED_STORES_FROM_BROCHURE_TEXT:
        assert store.quarantine_dir.parent == store.live_dir.parent
        assert store.quarantine_dir != store.live_dir


def test_an_excerpt_of_a_quarantined_document_is_quarantined_by_key(tmp_path):
    stores = _fake_stores(tmp_path)
    slim = _store(stores, "brochure_text_slim")
    kept = _write(slim.live_dir / "2026__mazda__cx50.json", {"catalog_key": "2026|mazda|cx50"})
    doomed = _write(slim.live_dir / "2026__nissan__rogue.json", {"catalog_key": "x"})

    quarantined = frozenset({"2026__nissan__rogue"})
    assert derived_quarantine_reason(slim, doomed, quarantined)
    assert derived_quarantine_reason(slim, kept, quarantined) is None


def test_an_overlay_citing_a_quarantined_document_is_quarantined(tmp_path):
    """The exact live defect: the document was quarantined, its overlay was not."""
    stores = _fake_stores(tmp_path)
    overlays = _store(stores, "trim_adds_by_year")
    path = _write(
        overlays.live_dir / "2026__nissan__roguepluginhybrid.json",
        {
            "source": "brochure_text_quoted",
            "source_brochure_text": "derived/brochure_text/2026__nissan__roguepluginhybrid.json",
            "adds_by_trim": {"Platinum": ["Head-Up Display"]},
            "adds_provenance": {
                "Platinum": [
                    {
                        "text": "Head-Up Display",
                        "source": "derived/brochure_text/2026__nissan__roguepluginhybrid.json",
                        "page": 13,
                    }
                ]
            },
        },
    )
    reason = derived_quarantine_reason(
        overlays, path, frozenset({"2026__nissan__roguepluginhybrid"})
    )
    assert reason and "2026__nissan__roguepluginhybrid" in reason


def test_an_overlay_citing_a_document_we_no_longer_hold_is_quarantined(tmp_path):
    """A citation is only a citation while the named document is on disk."""
    stores = _fake_stores(tmp_path)
    overlays = _store(stores, "trim_adds_by_year")
    path = _write(
        overlays.live_dir / "2026__mazda__cx50.json",
        {
            "source": "brochure_text_quoted",
            "source_brochure_text": "derived/brochure_text/2026__mazda__nosuchfile.json",
            "adds_by_trim": {"Turbo": ["2.5L Turbo"]},
        },
    )
    reason = derived_quarantine_reason(overlays, path, frozenset())
    assert reason and "no longer hold" in reason


def test_an_uncited_overlay_at_a_quarantined_key_is_left_in_place(tmp_path, monkeypatch):
    """Moving it would remove a suppressor and put MORE uncited text on the page."""
    stores = _fake_stores(tmp_path)
    overlays = _store(stores, "trim_adds_by_year")
    slim = _store(stores, "brochure_text_slim")
    _write(
        overlays.live_dir / "2012__bmw__3series.json",
        {
            "source": "brochure_llm",
            "trims_available": ["328i", "335i"],
            "adds_by_trim": {"335i": ["Turbo six"]},
        },
    )
    _write(slim.live_dir / "2012__bmw__3series.json", {"combined_trim_pages_text": "x"})
    quarantine_dir = tmp_path / "brochure_text_quarantine"
    _write(quarantine_dir / "2012__bmw__3series.json", {"pages": []})

    monkeypatch.setattr(bs.quarantine, "DERIVED_STORES_FROM_BROCHURE_TEXT", stores)
    monkeypatch.setattr(bs.quarantine, "BROCHURE_TEXT_QUARANTINE_DIR", quarantine_dir)
    monkeypatch.setattr(bs.quarantine, "BROCHURE_TEXT_DIR", tmp_path / "brochure_text")
    to_move, left = plan_derived_quarantine()

    assert quarantined_document_keys() == frozenset({"2012__bmw__3series"})
    assert [(r["store"], r["file"]) for r in to_move] == [
        ("brochure_text_slim", "2012__bmw__3series.json")
    ]
    assert [r["store"] for r in left] == ["trim_adds_by_year"]
    assert "LADDER_BULLET_STORES" in str(left[0]["reason"])


def test_applying_the_sweep_moves_and_never_deletes(tmp_path, monkeypatch):
    stores = _fake_stores(tmp_path)
    slim = _store(stores, "brochure_text_slim")
    payload = {"combined_trim_pages_text": "--- page 1 ---\nSport / Adds to Base"}
    _write(slim.live_dir / "2026__nissan__rogue.json", payload)
    quarantine_dir = tmp_path / "brochure_text_quarantine"
    _write(quarantine_dir / "2026__nissan__rogue.json", {"pages": []})

    monkeypatch.setattr(bs.quarantine, "DERIVED_STORES_FROM_BROCHURE_TEXT", stores)
    monkeypatch.setattr(bs.quarantine, "BROCHURE_TEXT_QUARANTINE_DIR", quarantine_dir)
    monkeypatch.setattr(bs.quarantine, "BROCHURE_TEXT_DIR", tmp_path / "brochure_text")
    to_move, _ = plan_derived_quarantine()
    assert apply_derived_quarantine(to_move) == 1

    assert not (slim.live_dir / "2026__nissan__rogue.json").exists()
    moved = slim.quarantine_dir / "2026__nissan__rogue.json"
    assert json.loads(moved.read_text()) == payload  # moved, byte for byte, not deleted

    # Idempotent: a second sweep finds nothing left to move.
    again, _ = plan_derived_quarantine()
    assert again == []


def test_the_sweep_ignores_the_manifest_files_beside_the_artifacts(tmp_path, monkeypatch):
    stores = _fake_stores(tmp_path)
    slim = _store(stores, "brochure_text_slim")
    _write(slim.live_dir / "_index.json", {"note": "not a vehicle"})
    quarantine_dir = tmp_path / "brochure_text_quarantine"
    _write(quarantine_dir / "_why_these_are_here.json", {"files": []})
    _write(quarantine_dir / "2026__nissan__rogue.json", {"pages": []})

    monkeypatch.setattr(bs.quarantine, "DERIVED_STORES_FROM_BROCHURE_TEXT", stores)
    monkeypatch.setattr(bs.quarantine, "BROCHURE_TEXT_QUARANTINE_DIR", quarantine_dir)
    monkeypatch.setattr(bs.quarantine, "BROCHURE_TEXT_DIR", tmp_path / "brochure_text")
    assert quarantined_document_keys() == frozenset({"2026__nissan__rogue"})
    to_move, left = plan_derived_quarantine()
    assert to_move == [] and left == []


# --------------------------------------------------------------------------
# Source tiers: OEM first, archive as a named fallback
# --------------------------------------------------------------------------


def test_the_archive_host_is_a_tier_not_an_official_host():
    """The reviewed change added a tier BESIDE the OEM allowlist. It did not
    loosen it: is_official_url still means exactly what it meant, which is why
    test_official_host_rejects_non_oem_sources above still lists this URL.
    """
    url = "https://www.auto-brochures.com/makes/Mazda/3/Mazda_US%203_2014.pdf"
    assert bs.is_official_url(url, "Mazda") is False
    assert bs.is_archive_url(url) is True
    assert bs.source_tier_for_url(url, "Mazda") == bs.TIER_ARCHIVE
    assert bs.source_tier_for_url(
        "https://news.mazdausa.com/download/spec.pdf", "Mazda"
    ) == bs.TIER_OEM
    assert bs.source_tier_for_url("https://en.wikipedia.org/wiki/Mazda3", "Mazda") == ""


def test_oem_is_the_first_tier_in_the_preference_order():
    assert bs.TIER_PREFERENCE == (bs.TIER_OEM, bs.TIER_ARCHIVE)


def test_archive_urls_need_an_explicit_opt_in():
    """A tiered host is not automatically a permitted host."""
    url = "https://www.auto-brochures.com/makes/Kia/Sportage/Kia_US%20Sportage_2016.pdf"
    assert bs.is_allowed_source_url(url, "Kia") is False
    assert bs.is_allowed_source_url(url, "Kia", allow_archive=True) is True
    # An OEM URL never needs the opt-in.
    oem = "https://www.kia.com/us/en/download/specifications/pdf/23020"
    assert bs.is_allowed_source_url(oem, "Kia") is True


def test_an_unregistered_host_is_still_refused_at_both_tiers():
    for url in (
        "https://autocatalogarchive.com/mazda/cx5.pdf",
        "https://www.somemazdadealer.com/brochures/2026-cx-5.pdf",
        "https://www.auto-brochures.com.evil.example/x.pdf",
    ):
        assert bs.source_tier_for_url(url, "Mazda") == ""
        assert bs.is_allowed_source_url(url, "Mazda", allow_archive=True) is False


def test_archive_pacing_is_a_floor_not_a_default():
    """No robots.txt means no invitation, so this host gets MORE space, and a
    caller cannot take it away with --delay.
    """

    class _Session:
        def get(self, *_args, **_kwargs):
            raise AssertionError("no request expected")

    assert bs.ARCHIVE_DELAY_SECONDS > bs.DEFAULT_DELAY_SECONDS
    fast = bs.PacedFetcher.for_tier(bs.TIER_ARCHIVE, delay=3.0, session=_Session())
    assert fast.delay == bs.ARCHIVE_DELAY_SECONDS
    slower = bs.PacedFetcher.for_tier(bs.TIER_ARCHIVE, delay=30.0, session=_Session())
    assert slower.delay == 30.0
    oem = bs.PacedFetcher.for_tier(bs.TIER_OEM, delay=4.0, session=_Session())
    assert oem.delay == 4.0


# --------------------------------------------------------------------------
# robots.txt: the exemption is a named list, and HTML is not robots rules
# --------------------------------------------------------------------------

_HTML_404 = "<!DOCTYPE html>\n<html><head><title>404</title></head><body>Not found</body></html>"


def test_html_served_as_robots_txt_blocks_an_ordinary_host():
    """The accidental fail-open this change closes: robotparser reads an HTML
    page, finds no User-agent line, and concludes everything is allowed.
    """
    policy, _ = _policy({"https://www.toyota.com/robots.txt": (200, _HTML_404)})
    assert policy.allows("https://www.toyota.com/brochures/x.pdf") is False
    assert policy.verdict_for("www.toyota.com").status == "html"


def test_the_archive_host_is_exempt_by_name_and_says_so():
    """auto-brochures.com 302s /robots.txt to /404.html. The user reviewed that
    and admitted the host; the verdict records the exemption rather than
    pretending the host published permissive rules.
    """
    policy, _ = _policy({"https://www.auto-brochures.com/robots.txt": (200, _HTML_404)})
    assert policy.allows("https://www.auto-brochures.com/mazda.html") is True
    verdict = policy.verdict_for("www.auto-brochures.com")
    assert verdict.status == "exempt"
    assert "ROBOTS_UNREADABLE_EXEMPT_HOSTS" in verdict.detail
    assert verdict.readable is False  # exempt is not "they said yes"


def test_the_exemption_list_contains_only_the_archive():
    """An exemption is a reviewed policy edit, one host at a time. If this fails,
    somebody widened a control instead of amending it in the open.
    """
    assert bs.ROBOTS_UNREADABLE_EXEMPT_HOSTS == bs.ARCHIVE_HOSTS
    for make in bs.OFFICIAL_HOSTS:
        for host in bs.OFFICIAL_HOSTS[make]:
            assert host not in bs.ROBOTS_UNREADABLE_EXEMPT_HOSTS


def test_an_exempt_host_that_403s_is_still_allowed_but_a_normal_host_is_not():
    """The exemption covers unreadability of any kind for that host only."""
    policy, _ = _policy({"https://www.auto-brochures.com/robots.txt": (403, "")})
    assert policy.allows("https://www.auto-brochures.com/mazda.html") is True
    other, _ = _policy({"https://automobiles.honda.com/robots.txt": (403, "")})
    assert other.allows("https://automobiles.honda.com/tools/brochures") is False


def test_real_robots_rules_are_still_honoured_on_an_exempt_host():
    """Exempt means 'unreadable does not block', not 'rules do not apply'."""
    body = "User-agent: *\nDisallow: /makes/\n"
    policy, _ = _policy({"https://www.auto-brochures.com/robots.txt": (200, body)})
    assert policy.allows("https://www.auto-brochures.com/mazda.html") is True
    assert policy.allows("https://www.auto-brochures.com/makes/Mazda/x.pdf") is False


# --------------------------------------------------------------------------
# Archive discovery: read off the site's own anchors, scored by the same rules
# --------------------------------------------------------------------------

_ARCHIVE_NAV = """
<a href="http://www.auto-brochures.com/mazda.html">Mazda</a>
<a href="http://www.auto-brochures.com/mercedes_benz.html">Mercedes-Benz</a>
<a href="http://www.auto-brochures.com/index.html">home </a>
<a href="http://www.auto-brochures.com/_contact.html"> contact </a>
"""

_ARCHIVE_MAZDA_INDEX = """
<a href="http://www.auto-brochures.com/makes/Mazda/3/Mazda_US 3_2014.pdf">2014</a>
<a href="http://www.auto-brochures.com/makes/Mazda/3/Mazda_US 3_2013.pdf">2013</a>
<a href="http://www.auto-brochures.com/makes/Mazda/CX-5/Mazda_US CX-5_2014.pdf">2014</a>
<a href="https://someforum.example/mazda3-2014-brochure.pdf">mirror</a>
"""


def test_archive_make_pages_come_from_the_sites_own_navigation():
    """Not a slug template: the make index URL is an anchor we actually read."""
    pages = bs.archive_make_index_pages(_ARCHIVE_NAV)
    assert pages["mazda"] == "https://www.auto-brochures.com/mazda.html"
    assert pages["mercedesbenz"] == "https://www.auto-brochures.com/mercedes_benz.html"
    assert "home" not in pages and "contact" not in pages
    assert bs.archive_index_url("Mercedes-Benz", pages) == (
        "https://www.auto-brochures.com/mercedes_benz.html"
    )
    assert bs.archive_index_url("Rivian", pages) is None


def test_archive_links_are_https_upgraded_and_offsite_links_dropped():
    links = bs.extract_archive_document_links(
        _ARCHIVE_MAZDA_INDEX, "https://www.auto-brochures.com/mazda.html"
    )
    urls = {link.url for link in links}
    assert all(url.startswith("https://www.auto-brochures.com/") for url in urls)
    assert not any("someforum" in url for url in urls)
    assert len(urls) == 3


def test_archive_scoring_keeps_every_year_and_variant_gate():
    """Being a fallback buys a second place to look, not looser matching."""
    links = bs.extract_archive_document_links(
        _ARCHIVE_MAZDA_INDEX, "https://www.auto-brochures.com/mazda.html"
    )
    best = bs.pick_archive_document(links, 2014, "Mazda", "3")
    assert best is not None and best.url.endswith("Mazda_US 3_2014.pdf")
    # Wrong year: nothing, exactly as on the OEM tier.
    assert bs.pick_archive_document(links, 2019, "Mazda", "3") is None
    # A model the index does not carry: nothing.
    assert bs.pick_archive_document(links, 2014, "Mazda", "CX-9") is None


def test_the_archive_hostname_does_not_score_as_the_word_brochure():
    """'auto-brochures.com' contains a +5.0 keyword. Scoring the host would hand
    every archive candidate a bonus for where it lives rather than what it is.
    """
    link = DocLink(
        url="https://www.auto-brochures.com/makes/Mazda/3/Mazda_US%203_2014.pdf"
    )
    whole_url = score_document_link(
        DocLink(url=link.url), 2014, "3", "Mazda"
    )
    links = bs.extract_archive_document_links(
        '<a href="/makes/Mazda/3/Mazda_US 3_2014.pdf">x</a>',
        "https://www.auto-brochures.com/mazda.html",
    )
    picked = bs.pick_archive_document(links, 2014, "Mazda", "3")
    assert picked is not None
    assert picked.score < whole_url.score
    assert "brochure" not in " ".join(picked.reasons)


# --------------------------------------------------------------------------
# The tier travels with the document, all the way to a citation
# --------------------------------------------------------------------------


def test_the_content_index_records_and_reloads_the_tier(tmp_path):
    index = ContentIndex(tmp_path / "index.json")
    index.register(
        b"%PDF-archive",
        preferred_path=tmp_path / "a.pdf",
        source_url="https://www.auto-brochures.com/makes/Kia/x.pdf",
        catalog_key_str="2016|kia|sportage",
        fetched_at="",
        tier=bs.TIER_ARCHIVE,
        authenticity={"ok": True},
    )
    index.save()
    reloaded = ContentIndex(tmp_path / "index.json").load()
    doc = next(iter(reloaded.by_sha.values()))
    assert doc.tier == bs.TIER_ARCHIVE
    assert doc.authenticity == {"ok": True}


def test_an_oem_tier_is_never_downgraded_by_an_archive_copy(tmp_path):
    """Same bytes from both: the copy we hold is the OEM one, and the archive
    serving those exact bytes is evidence, not a change of provenance.
    """
    index = ContentIndex(tmp_path / "index.json")
    payload = b"%PDF-1.7 shared"
    index.register(
        payload,
        preferred_path=tmp_path / "oem.pdf",
        source_url="https://news.mazdausa.com/download/x.pdf",
        catalog_key_str="2014|mazda|3",
        fetched_at="",
        tier=bs.TIER_OEM,
    )
    doc, is_new = index.register(
        payload,
        preferred_path=tmp_path / "archive.pdf",
        source_url="https://www.auto-brochures.com/makes/Mazda/3/x.pdf",
        catalog_key_str="2014|mazda|3",
        fetched_at="",
        tier=bs.TIER_ARCHIVE,
    )
    assert is_new is False
    assert doc.tier == bs.TIER_OEM
    assert doc.authenticity.get("archive_serves_identical_bytes") is True
    assert not (tmp_path / "archive.pdf").exists()


def test_an_untiered_legacy_document_reports_no_tier_rather_than_guessing(tmp_path):
    """135 PDFs predate tiers. Reporting them as 'oem' would be inventing a fact."""
    index = ContentIndex(tmp_path / "index.json")
    index.register(
        b"%PDF-old",
        preferred_path=tmp_path / "old.pdf",
        source_url="https://news.mazdausa.com/download/old.pdf",
        catalog_key_str="2011|mazda|2",
        fetched_at="",
    )
    assert next(iter(index.by_sha.values())).tier == ""


def test_a_bullets_citation_resolves_back_to_the_tier_that_backs_it(tmp_path, monkeypatch):
    """The chain a rendered bullet has to be able to answer:
    citation -> brochure_text.source_pdf_sha256 -> content index -> tier.
    Followed by re-opening the files, not by asking a register about itself.
    """
    text_dir = tmp_path / "brochure_text"
    text_dir.mkdir()
    (text_dir / "2016__kia__sportage.json").write_text(
        json.dumps({"catalog_key": "2016|kia|sportage", "source_pdf_sha256": "abc123"}),
        encoding="utf-8",
    )
    index_path = tmp_path / "brochure_content_index.json"
    index_path.write_text(
        json.dumps({"documents": {"abc123": {"path": "x", "tier": "archive"}}}),
        encoding="utf-8",
    )
    monkeypatch.setattr(bs.storage, "BROCHURE_TEXT_DIR", text_dir)
    monkeypatch.setattr(bs.storage, "BROCHURE_TEXT_QUARANTINE_DIR", tmp_path / "nope")

    assert bs.document_tier_for_citation(
        "derived/brochure_text/2016__kia__sportage.json", index_path=index_path
    ) == bs.TIER_ARCHIVE
    # Unknown stays unknown; it is never defaulted to "oem".
    assert bs.document_tier_for_citation(
        "derived/brochure_text/2016__kia__telluride.json", index_path=index_path
    ) == ""
    assert bs.document_tier_for_citation("", index_path=index_path) == ""


# --------------------------------------------------------------------------
# Archive authenticity
# --------------------------------------------------------------------------


def _tiny_pdf(text: str = "2016 SPORTAGE SPECIFICATIONS") -> bytes:
    """A real, minimal, single-page PDF with a text layer."""
    pypdf = pytest.importorskip("pypdf")
    import io

    writer = pypdf.PdfWriter()
    writer.add_blank_page(width=200, height=200)
    buffer = io.BytesIO()
    writer.write(buffer)
    return buffer.getvalue()


def test_a_non_pdf_payload_fails_the_archive_check():
    record = bs.assess_archive_document(b"<!DOCTYPE html>Access Denied")
    assert record.ok is False and "not a PDF" in record.reason


def test_archive_metadata_is_recorded_for_every_document_not_gated_on():
    """A Producer allowlist would be worse than useless: the 135 PDFs this
    project fetched from manufacturer hosts carry 25 different Producers.
    """
    record = bs.assess_archive_document(_tiny_pdf())
    assert record.ok is True
    assert set(record.metadata) >= {
        "producer", "creator", "title", "creation_date", "page_count", "encrypted"
    }
    assert record.to_json()["tier"] == bs.TIER_ARCHIVE


def test_a_document_with_no_text_layer_is_kept_and_counted_not_failed():
    """A third of the archive is scanned paper. It yields nothing to quote --
    silence -- and the count is what we report; it is not a rejection.
    """
    record = bs.assess_archive_document(_tiny_pdf())
    assert record.pages_with_text == 0  # a blank page has no text layer
    assert record.has_text_layer is False
    assert record.ok is True


def test_identical_bytes_is_the_only_positive_evidence(tmp_path):
    payload = _tiny_pdf()
    oem = tmp_path / "oem.pdf"
    oem.write_bytes(payload)
    comparison = bs.compare_archive_to_oem(payload, oem, catalog_key_str="2016|kia|x")
    assert comparison.outcome == "identical_bytes"
    assert comparison.similarity == 1.0
    assert comparison.oem_sha256 == comparison.archive_sha256


def test_a_low_similarity_against_the_oem_copy_is_not_a_failure(tmp_path):
    """Measured 2026-08-01: 9 of 10 real pairs scored 0.005-0.015, because the
    OEM file we hold for 2014 Mazda3 is an 11-page press release and the
    archive's is the 23-page consumer brochure. Failing on that would quarantine
    genuine brochures, which is what the first version of this code did.
    """
    press_release = "for immediate release contact mazda north american operations " * 60
    brochure = "available on grand touring adds heated leather seats bose audio " * 60
    assert bs.text_similarity(press_release, brochure) < bs.DIFFERENT_DOCUMENT_SIMILARITY

    oem = tmp_path / "oem.pdf"
    oem.write_bytes(_tiny_pdf())
    record = bs.assess_archive_document(
        _tiny_pdf() + b"\n% different trailer\n",
        oem_pdf_path=oem,
        catalog_key_str="2014|mazda|3",
    )
    assert record.ok is True, record.reason
    assert record.comparison is not None


def test_similarity_is_symmetric_and_refuses_to_score_empty_text():
    a = "grand touring adds heated leather seats bose audio moonroof " * 20
    b = "grand touring adds heated leather seats bose audio sunroof " * 20
    assert bs.text_similarity(a, b) == bs.text_similarity(b, a)
    assert bs.text_similarity(a, a) == 1.0
    assert bs.text_similarity(a, "") == 0.0
    assert bs.text_similarity("", "") == 0.0


def test_a_failed_archive_document_is_moved_and_never_deleted(tmp_path, monkeypatch):
    monkeypatch.setattr(bs.archive_authenticity, "ARCHIVE_QUARANTINE_DIR", tmp_path / "quarantine")
    pdf = tmp_path / "2016_Kia_Sportage_Brochure.pdf"
    pdf.write_bytes(b"%PDF-payload")

    destination = bs.quarantine_archive_pdf(pdf, "document never names the model")
    assert not pdf.exists()
    assert destination.read_bytes() == b"%PDF-payload"

    record = json.loads((tmp_path / "quarantine" / "_why_these_are_here.json").read_text())
    assert record["count"] == 1
    assert record["files"][0]["reason"] == "document never names the model"
    assert record["files"][0]["tier"] == bs.TIER_ARCHIVE

    # Cumulative: a second batch does not erase the first one's reason.
    other = tmp_path / "2017_Kia_Sportage_Brochure.pdf"
    other.write_bytes(b"%PDF-other")
    bs.quarantine_archive_pdf(other, "encrypted")
    record = json.loads((tmp_path / "quarantine" / "_why_these_are_here.json").read_text())
    assert record["count"] == 2
    assert {r["file"] for r in record["files"]} == {pdf.name, other.name}


def test_the_quarantine_and_comparison_dirs_are_siblings_of_the_corpus():
    """Siblings, not subdirectories, so BROCHURES_DIR.glob('*.pdf') stops seeing
    them -- the same rule the brochure_text quarantine follows.
    """
    from backend.enrichment.dictionary_paths import BROCHURES_DIR

    for directory in (bs.ARCHIVE_QUARANTINE_DIR, bs.ARCHIVE_COMPARISON_DIR):
        assert directory.parent == BROCHURES_DIR.parent
        assert directory != BROCHURES_DIR


# --------------------------------------------------------------------------
# Resolution order: OEM is tried in full before the archive is touched
# --------------------------------------------------------------------------


class _FakeArchive:
    """Stands in for ArchiveResolver, and records whether it was consulted."""

    def __init__(self, doc_url: str | None = None) -> None:
        self.consulted = 0
        self._doc_url = doc_url

    def make_index_pages(self):
        self.consulted += 1
        return {"mazda": "https://www.auto-brochures.com/mazda.html"}

    @property
    def robots(self):
        return type("R", (), {"allows": staticmethod(lambda url: True),
                              "verdict_for": staticmethod(lambda url: None)})()

    def get_page(self, url):
        if self._doc_url is None:
            return 200, ""
        return 200, f'<a href="{self._doc_url}">x</a>'


def test_an_oem_hit_never_consults_the_archive(monkeypatch):
    from backend.scripts import fetch_oem_brochures as fob

    html = '<a href="/download/2014+Mazda3+Spec+Deck.pdf">2014 Mazda3 Spec Deck</a>'
    gap = {
        "group_key": "2014|mazda|3",
        "year": 2014,
        "make": "Mazda",
        "model": "Mazda3",
        "cars": 9,
        "primary_catalog_key": "2014__mazda__3",
        "alias_catalog_keys": [],
    }
    archive = _FakeArchive("https://www.auto-brochures.com/makes/Mazda/3/x_2014.pdf")
    page_cache = {p: (200, html) for p in bs.discovery_pages(2014, "Mazda", "Mazda3")}
    resolved = fob.resolve_source(
        gap,
        None,
        type("R", (), {"allows": staticmethod(lambda url: True)})(),
        page_cache,
        archive=archive,
    )
    assert resolved["status"] == "found"
    assert resolved["tier"] == bs.TIER_OEM
    assert archive.consulted == 0, "the archive was consulted despite an OEM hit"


def test_an_unsupported_make_falls_through_to_the_archive():
    from backend.scripts import fetch_oem_brochures as fob

    gap = {
        "group_key": "2016|chevrolet|tahoe",
        "year": 2016,
        "make": "Chevrolet",
        "model": "Tahoe",
        "cars": 7,
        "primary_catalog_key": "2016__chevrolet__tahoe",
        "alias_catalog_keys": [],
    }
    archive = _FakeArchive()
    archive.make_index_pages = lambda: {
        "chevrolet": "https://www.auto-brochures.com/chevrolet.html"
    }
    archive.get_page = lambda url: (
        200,
        '<a href="http://www.auto-brochures.com/makes/Chevrolet/Tahoe/'
        'Chevrolet_US Tahoe_2016.pdf">2016</a>',
    )
    resolved = fob.resolve_source(gap, None, None, {}, archive=archive)
    assert resolved["status"] == "found"
    assert resolved["tier"] == bs.TIER_ARCHIVE
    assert resolved["doc_url"].startswith("https://www.auto-brochures.com/")
    # The OEM refusal is kept on the record, not overwritten by the fallback.
    assert "chevrolet.com" in resolved["oem_refusal"]


def test_without_allow_archive_an_unsupported_make_still_yields_nothing():
    from backend.scripts import fetch_oem_brochures as fob

    gap = {
        "group_key": "2016|chevrolet|tahoe",
        "year": 2016,
        "make": "Chevrolet",
        "model": "Tahoe",
        "cars": 7,
        "primary_catalog_key": "2016__chevrolet__tahoe",
        "alias_catalog_keys": [],
    }
    resolved = fob.resolve_source(gap, None, None, {})
    assert resolved["status"] == "unsupported_make"
    assert resolved["tier"] == ""


# --------------------------------------------------------------------------
# Current-model-year reachability (2026-08-01)
# --------------------------------------------------------------------------


def test_toyota_discovery_covers_every_body_style_category():
    """
    Regression for the bug that hid Toyota's trucks and SUVs.

    ``https://www.toyota.com/brochures/`` answers HTTP 200 and then redirects
    into ``/brochures/cars-minivan/``, which lists 11 PDFs and no truck. With a
    single-URL discovery list the whole Toyota line-up was resolved against the
    cars/minivan page, so the 2026 Tundra -- 1,090 active cars, the second
    largest gap in the fleet -- looked like "no candidate link matched" rather
    than "we never asked the trucks page".
    """
    pages = discovery_pages(2026, "Toyota", "Tundra")
    assert len(pages) == 5
    assert all(is_official_url(p, "Toyota") for p in pages)
    for category in ("cars-minivan", "crossovers-suvs", "trucks", "electrified"):
        assert any(category in p for p in pages), category
    # Every gap for the make shares one page list, so page_cache fetches each
    # index at most once per run however many Toyota vehicles are processed.
    assert discovery_pages(2026, "Toyota", "Camry") == pages


@pytest.mark.parametrize(
    "make",
    ["BMW", "Chevrolet", "Mercedes-Benz", "Honda"],
)
def test_makes_registered_on_2026_08_01_carry_a_measurement_per_host(make):
    """
    A host is added to the allowlist only with a recorded response behind it.

    These four makes were ``unsupported_make`` -- refused before any request was
    issued. They now have registered hosts, and every one of those hosts has a
    per-host observation in HOST_ACCESS_NOTES. Registering a host is NOT a claim
    that the make is fetchable: each still reports a reason, because the
    discovery step is what is unresolved.
    """
    from backend.enrichment.brochure_sources import (
        DISCOVERY_UNRESOLVED,
        HOST_ACCESS_NOTES,
        UNSUPPORTED_MAKES,
        _make_token,
    )

    hosts = official_hosts_for(make)
    assert hosts, f"{make} has no registered host"
    for host in hosts:
        assert host in HOST_ACCESS_NOTES, f"{host} registered with no measurement"
        assert "2026-08-01" in HOST_ACCESS_NOTES[host], host

    token = _make_token(make)
    assert token in DISCOVERY_UNRESOLVED
    # Not left behind in the old table as well: unsupported_reason reads
    # DISCOVERY_UNRESOLVED first, so a stale duplicate would never be seen and
    # would read as a current measurement to anyone opening the file.
    assert token not in UNSUPPORTED_MAKES
    assert unsupported_reason(make)


def test_bmw_press_attachment_evidence_is_a_bmw_url_with_a_measurement():
    """The one newly registered host proven to serve a real document."""
    from backend.enrichment.brochure_sources import BMW_PRESS_ATTACHMENT_EVIDENCE

    assert "www.press.bmwgroup.com" in BMW_PRESS_ATTACHMENT_EVIDENCE
    assert "application/pdf" in BMW_PRESS_ATTACHMENT_EVIDENCE
    assert is_official_url(
        BMW_PRESS_ATTACHMENT_EVIDENCE.split()[0], "BMW"
    )


def test_registering_bmw_does_not_open_the_bmw_press_host_to_other_makes():
    url = "https://www.press.bmwgroup.com/usa/article/attachment/T0/1"
    assert is_official_url(url, "BMW")
    assert not is_official_url(url, "Mercedes-Benz")
    assert not is_official_url(url, "Toyota")


def test_no_discovery_pattern_was_invented_for_a_make_we_cannot_index():
    """
    Registering a host must not tempt anyone into synthesising document URLs.

    None of the four makes registered on 2026-08-01 has a discovery page
    builder, because for none of them was a per-vehicle index page ever read.
    """
    for make, model in (
        ("BMW", "X5"),
        ("Chevrolet", "Silverado 1500"),
        ("Mercedes-Benz", "GLE"),
        ("Honda", "HR-V"),
    ):
        assert discovery_pages(2026, make, model) == []


# --------------------------------------------------------------------------
# Re-acquiring a document at a quarantined key
# --------------------------------------------------------------------------


def _reacquired(tmp_path, monkeypatch, *, live_sha="new", quarantined_sha="old"):
    """A key whose brochure_text exists BOTH in quarantine and live."""
    stores = _fake_stores(tmp_path)
    live_dir = tmp_path / "brochure_text"
    quarantine_dir = tmp_path / "brochure_text_quarantine"
    _write(quarantine_dir / "2026__toyota__camry.json", {"source_pdf_sha256": quarantined_sha})
    _write(live_dir / "2026__toyota__camry.json", {"source_pdf_sha256": live_sha})
    monkeypatch.setattr(bs.quarantine, "DERIVED_STORES_FROM_BROCHURE_TEXT", stores)
    monkeypatch.setattr(bs.quarantine, "BROCHURE_TEXT_QUARANTINE_DIR", quarantine_dir)
    monkeypatch.setattr(bs.quarantine, "BROCHURE_TEXT_DIR", live_dir)
    return stores, live_dir, quarantine_dir


def test_a_re_acquired_key_is_still_reported_as_quarantined(tmp_path, monkeypatch):
    """
    Fetching a better document does NOT release the key.

    Releasing it was tried and reverted: the slim excerpt at that key is still
    the OLD document's text until something rebuilds it, and trim_ladder reads
    that excerpt at render time. The key stays; only a demonstrably rebuilt
    excerpt is spared.
    """
    _reacquired(tmp_path, monkeypatch)
    assert quarantined_document_keys() == frozenset({"2026__toyota__camry"})
    assert set(bs.superseded_quarantine_keys()) == {"2026__toyota__camry"}


def test_a_stale_excerpt_at_a_re_acquired_key_is_still_quarantined(tmp_path, monkeypatch):
    """Written before the replacement document: still the rejected text."""
    import os

    stores, live_dir, _ = _reacquired(tmp_path, monkeypatch)
    slim = _store(stores, "brochure_text_slim")
    stale = _write(slim.live_dir / "2026__toyota__camry.json", {"combined_trim_pages_text": "old"})
    live = live_dir / "2026__toyota__camry.json"
    os.utime(stale, (1, 1))
    os.utime(live, (1000, 1000))

    to_move, left = plan_derived_quarantine()
    assert [(r["store"], r["file"]) for r in to_move] == [
        ("brochure_text_slim", "2026__toyota__camry.json")
    ]
    assert "predates it" in str(to_move[0]["reason"])
    assert left == []


def test_an_excerpt_rebuilt_after_the_re_acquisition_is_left_in_place(tmp_path, monkeypatch):
    """Written after the replacement: it is an excerpt OF the good document."""
    import os

    stores, live_dir, _ = _reacquired(tmp_path, monkeypatch)
    slim = _store(stores, "brochure_text_slim")
    rebuilt = _write(slim.live_dir / "2026__toyota__camry.json", {"combined_trim_pages_text": "new"})
    os.utime(live_dir / "2026__toyota__camry.json", (1, 1))
    os.utime(rebuilt, (1000, 1000))

    to_move, left = plan_derived_quarantine()
    assert to_move == []
    assert [(r["store"], r["file"]) for r in left] == [
        ("brochure_text_slim", "2026__toyota__camry.json")
    ]
    assert "re-acquired" in str(left[0]["reason"])


def test_the_same_source_pdf_is_not_a_re_acquisition(tmp_path, monkeypatch):
    """
    Same PDF behind both copies -> nothing was replaced, nothing is spared.

    This is what stops the escape hatch being opened by a re-extraction of the
    very document that was rejected.
    """
    import os

    stores, live_dir, _ = _reacquired(
        tmp_path, monkeypatch, live_sha="same", quarantined_sha="same"
    )
    slim = _store(stores, "brochure_text_slim")
    newer = _write(slim.live_dir / "2026__toyota__camry.json", {"combined_trim_pages_text": "x"})
    os.utime(live_dir / "2026__toyota__camry.json", (1, 1))
    os.utime(newer, (1000, 1000))

    assert bs.superseded_quarantine_keys() == {}
    to_move, _left = plan_derived_quarantine()
    assert [(r["store"], r["file"]) for r in to_move] == [
        ("brochure_text_slim", "2026__toyota__camry.json")
    ]


def test_a_live_file_with_no_recorded_source_pdf_is_not_a_re_acquisition(
    tmp_path, monkeypatch
):
    """Fails closed: an unknown provenance releases nothing."""
    _reacquired(tmp_path, monkeypatch, live_sha="")
    assert bs.superseded_quarantine_keys() == {}


# --------------------------------------------------------------------------
# Digit density: a text layer that lost the document's numbers
# --------------------------------------------------------------------------


def test_quality_gate_rejects_a_digit_stripped_text_layer():
    """
    The real defect: ``derived/brochure_text/2015__bmw__x1.json``.

    170,126 chars, 362 digits. Re-opening the PDF (pdfplumber AND pypdf agree)
    shows the fonts map digit glyphs into the Private Use Area, so "2.0-liter"
    extracts as "<U+EA02>.<U+EA0A>-liter". Prose survives, every number does
    not, and the old gate passed it.
    """
    pua = chr(0xEA02)  # a Private Use Area codepoint, as the X1's fonts emit
    body = "The BMW X SAV is a product of BMW engineering magic. " * 200
    text = body + pua + "." + pua + "-liter inline four-cylinder"
    verdict = assess_text_quality(text)
    assert verdict.ok is False
    assert "digit" in verdict.reason
    assert "Private Use Area" in verdict.reason
    assert verdict.digit_density < bs.MIN_DIGIT_DENSITY_PER_100_LETTERS


def test_quality_gate_names_no_font_cause_when_there_are_no_glyph_codepoints():
    """A document that simply has no numbers is refused too, but for its own reason."""
    verdict = assess_text_quality("An expression of pure design and desire. " * 200)
    assert verdict.ok is False
    assert "digit" in verdict.reason
    assert "Private Use Area" not in verdict.reason


def test_quality_gate_accepts_a_normal_brochures_digit_density():
    """
    Calibrated on documents we hold: the 168 this lane fetched itself run
    1.206-9.64 digits per 100 letters, median 3.49.
    """
    text = "Engine 2.5L 187 hp 186 lb-ft 28 city 34 highway mpg seats 5 " * 200
    verdict = assess_text_quality(text)
    assert verdict.ok is True
    assert verdict.digit_density > bs.MIN_DIGIT_DENSITY_PER_100_LETTERS


def test_digit_density_is_not_computed_on_a_handful_of_letters():
    """Below the letter floor the ratio is noise; the length gate speaks first."""
    verdict = assess_text_quality("Design.")
    assert verdict.ok is False
    assert "chars of extracted text" in verdict.reason


def test_quality_verdict_reports_what_it_measured():
    verdict = assess_text_quality("Torque 258 lb-ft " * 300)
    assert verdict.letters > 0
    assert verdict.digits > 0
    assert verdict.private_use_chars == 0
    assert verdict.private_use_codepoints == 0
    assert verdict.private_use_run == 0


# --------------------------------------------------------------------------
# Private Use Area: a character class remapped by a font subset
# --------------------------------------------------------------------------
#
# The digit-density floor above was built for this class and does not close it.
# All 12 documents this gate refuses pass that floor. Codepoints are built with
# chr() rather than written as literals on purpose: a literal PUA character in
# source is invisible, and one of these constants lost a whole block to a
# copy/paste before this suite pinned it.

#: ``U+EA01..U+EA0A`` -- the ten codepoints seven BMW books in this corpus map
#: their digit glyphs onto, in the order 1,2,...,9,0.
_BMW_DIGIT_GLYPHS = [chr(0xEA01 + i) for i in range(10)]


def _remap_digits(text: str) -> str:
    """Rewrite every ASCII digit the way the BMW font subsets do."""
    return "".join(
        _BMW_DIGIT_GLYPHS[(int(ch) - 1) % 10] if ch.isdigit() else ch for ch in text
    )


def test_pua_pattern_covers_all_three_private_use_blocks():
    """
    Pins ``_PRIVATE_USE_AREA`` against silent truncation.

    A character class written with literal PUA characters is unreadable in a
    diff, and losing the BMP block leaves a pattern that still compiles, still
    matches the two supplementary planes nothing ever uses, and quietly passes
    everything. Every real document in this corpus is contaminated in the BMP
    block, so that failure would be invisible.
    """
    for codepoint in (0xE000, 0xEA04, 0xF06E, 0xF8FF, 0xF0000, 0xFFFFD, 0x100000):
        assert bs._PRIVATE_USE_AREA.search(chr(codepoint)), hex(codepoint)
    for codepoint in (ord("A"), ord("9"), 0xDFFF, 0xF900, 0x2022):
        assert not bs._PRIVATE_USE_AREA.search(chr(codepoint)), hex(codepoint)


def test_longest_consecutive_run_finds_the_remapped_block():
    assert bs._longest_consecutive_run([]) == (0, 0)
    assert bs._longest_consecutive_run([0xF06E]) == (0xF06E, 1)
    # The 2010 BMW Z4 shape: ten digit glyphs plus a scatter of icon glyphs.
    codepoints = [0xEA01 + i for i in range(10)] + [0xF08D, 0xF08E, 0xF090]
    assert bs._longest_consecutive_run(codepoints) == (0xEA01, 10)


def test_a_remapped_digit_class_is_refused_even_with_a_healthy_digit_density():
    """
    THE CLASS THE DIGIT FLOOR DOES NOT CLOSE.

    The remap is per-font, not per-document: ``2015__bmw__3series`` extracts at
    3.74 digits per 100 letters -- comfortably over the floor -- while every one
    of its ten distinct PUA codepoints is a digit glyph, because the spec tables
    use the subsetted font and the body copy does not. A density floor only sees
    the defect when the broken font covers the whole book.
    """
    intact = "Engine 2.5L 187 hp 186 lb-ft 28 city 34 highway mpg seats 5 " * 200
    broken = (
        _remap_digits("sDrive28i 3.0-liter 300 hp 0-60 in 4.6 seconds 1,795 lb 9 mpg ")
        * 40
    )
    verdict = assess_text_quality(intact + broken)

    assert verdict.digit_density > bs.MIN_DIGIT_DENSITY_PER_100_LETTERS
    assert verdict.ok is False
    assert "Private Use Area" in verdict.reason
    assert "U+EA01..U+EA0A" in verdict.reason
    assert verdict.private_use_codepoints == 10
    assert verdict.private_use_run == 10


def test_a_repeated_dingbat_is_not_a_remapped_class():
    """
    110 live documents carry one or two PUA codepoints and are sound.

    ``2014__audi__r8`` has 208 of them and they are all ``U+F06E``, the filled
    square in its equipment grid; ``2012__jaguar__xf`` uses ``U+E044`` as the
    inch mark. Refusing those would discard whole books over a glyph. The
    document stays; the individual bullet is refused instead, by
    ``brochure_extract.bullet_text_is_readable``.
    """
    text = ("Heated front seats " + chr(0xF06E) + " 2.0L 250 hp 18 in wheels ") * 300
    verdict = assess_text_quality(text)

    assert verdict.private_use_chars == 300
    assert verdict.private_use_codepoints == 1
    assert verdict.ok is True


def test_the_pua_gate_is_calibrated_inside_an_empty_band():
    """
    Measured 2026-08-02 over all 3,382 documents we hold (2,827 live in
    ``derived/brochure_text`` plus 555 already quarantined): the distinct-PUA
    histogram is 0, 1, 2, 3 ... then nothing at 4, 5, 6 or 7 ... then 8 and up.
    The threshold sits inside that empty band, so no held document is near it.
    """
    assert 4 <= bs.MAX_PRIVATE_USE_CODEPOINTS <= 7

    icons = "".join(chr(0xF08C + i) for i in range(3))
    keeps = ("Audio system " + icons + " 2.0L 250 hp 18 in wheels ") * 300
    assert assess_text_quality(keeps).private_use_codepoints == 3
    assert assess_text_quality(keeps).ok is True


def test_a_remapped_alphabet_is_refused_and_names_its_block():
    """
    ``2012__bentley__mulsanne``: its lowercase glyphs land on ``U+F761``
    upwards, ASCII shifted by 0xF700, so "the" extracts as
    ``<U+F774><U+F768><U+F765>``. 125 whole words in that book are gone, and its
    digit density is 1.61 -- over the floor. The book's own subset covers 22 of
    the 26 letters; the whole alphabet is used here so the reported block is the
    unambiguous ``a``-``z``.
    """
    shifted = "".join(chr(0xF700 + ord(ch)) for ch in "abcdefghijklmnopqrstuvwxyz")
    text = ("The Mulsanne 6.75 litre V8 505 bhp 752 lb ft " + shifted + " ") * 120
    verdict = assess_text_quality(text)

    assert verdict.ok is False
    assert verdict.digit_density > bs.MIN_DIGIT_DENSITY_PER_100_LETTERS
    assert "Private Use Area" in verdict.reason
    assert "U+F761..U+F77A" in verdict.reason
    assert verdict.private_use_run == 26


# --------------------------------------------------------------------------
# Derived nameplates: the M3 brochure filed as a 3 Series
# --------------------------------------------------------------------------

#: Verbatim from ``backend/data/brochures/2018_BMW_3_Series_Brochure.pdf``,
#: re-opened with pdfplumber on 2026-08-02: the running header on all nine
#: pages, and the one line in the whole document that says "3 Series".
_M3_HEADER = (
    "BMW M3 SEDAN EXTERIOR COLORS UPHOLSTERY INTERIOR TRIMS "
    "WHEELS / TIRES PACKAGES TECHNICAL DATA"
)
_M3_BODY_COPY = (
    "FOUR DOORS, FAST.\n"
    "The 3 Series is the best-selling BMW sedan. Add\n"
    "M to the equation and its personality transforms for\n"
    "the drivers who need an extra shot of adrenaline with\n"
    "their everyday driving."
)


def test_the_m3_brochure_is_refused_as_a_3_series_document():
    pages = [_M3_HEADER, _M3_BODY_COPY + "\n" + _M3_HEADER] + [_M3_HEADER] * 7
    verdict = bs.assess_nameplate_dominance(pages, "BMW", "3 Series")
    assert verdict.ok is False
    assert verdict.derived_name == "m3"
    assert verdict.derived_pages == 9
    assert verdict.base_pages == 1
    assert "M3" in verdict.reason


def test_the_naming_check_alone_still_passes_the_m3_brochure():
    """
    Why the subject check had to be added rather than the identity check tightened.

    ``verify_document_identity`` is a naming test and the M3 book does name the
    3 Series, in a short line block. It passes, correctly, and says nothing
    about which vehicle the document is about.
    """
    text = "\n".join([_M3_HEADER, _M3_BODY_COPY] + [_M3_HEADER] * 7)
    ok, _why = verify_document_identity(text, 2018, "BMW", "3 Series")
    assert ok is True


def test_a_combined_base_and_variant_brochure_is_still_accepted():
    """
    Audi ships one A4/S4 volume. Page 1 of ``2010__audi__a4`` really does read
    "A4 | S4 Premium Plus" and its spec grid has an A4 column and an S4 column.
    Refusing those would quarantine 46 genuine documents to catch one wrong one.
    """
    pages = [
        "A4 | S4 Premium Plus",
        "A4 | S4 Options",
        "A4 | S4 2010 Specifications",
        "A4 Model Configurations S4 Model Configurations",
    ]
    assert bs.assess_nameplate_dominance(pages, "Audi", "A4").ok is True


def test_one_stray_mention_of_a_variant_does_not_refuse_the_document():
    pages = ["2019 BMW 3 Series", "Sedan equipment", "Compare with the M3", "Colors"]
    verdict = bs.assess_nameplate_dominance(pages, "BMW", "3 Series")
    assert verdict.ok is True
    assert verdict.derived_pages == 1


def test_a_document_that_never_declares_the_base_nameplate_fails_closed():
    pages = [_M3_HEADER] * 4
    verdict = bs.assess_nameplate_dominance(pages, "BMW", "3 Series")
    assert verdict.ok is False
    assert verdict.base_pages == 0


def test_a_make_with_no_derived_family_is_untouched():
    assert bs.derived_nameplates_for("Toyota", "Camry") == frozenset()
    assert bs.assess_nameplate_dominance(["Corolla only"], "Toyota", "Camry").ok is True


def test_the_variant_is_allowed_to_be_its_own_document():
    """Filing the M3 book under the M3 is right; only the base filing is wrong."""
    assert bs.derived_nameplates_for("BMW", "M3") == frozenset()
    assert bs.assess_nameplate_dominance([_M3_HEADER] * 4, "BMW", "M3").ok is True


def test_derived_nameplates_are_invisible_to_every_other_control():
    """
    The table's whole reason for existing: these names share no substring with
    the base, so ``find_model_name`` and ``_VARIANT_QUALIFIERS`` cannot see
    them. A family that IS visible (``X5 M`` contains ``x5``) must not be added
    here -- it would be a second, weaker copy of a control that already works.
    """
    for make, family in bs.DERIVED_NAMEPLATES.items():
        for base, derived in family.items():
            for token in derived:
                assert token != base, (make, base, token)
                # Both spellings a document uses. "SQ5" contains "q5" as a
                # substring, which is exactly why the substring test is the
                # wrong one: find_model_name is word-bounded and does not match
                # it, so no existing control sees the derived nameplate.
                for spelling in (token, token.upper()):
                    assert not list(bs.find_model_name(spelling, base))
                    assert not list(bs.find_model_name(spelling, base.upper()))


# --------------------------------------------------------------------------
# The Chevrolet claim, reconciled against our own ledger
# --------------------------------------------------------------------------


def test_the_gm_entries_no_longer_claim_gm_publishes_nothing():
    """
    Regression guard for a false present-tense assertion.

    ``DISCOVERY_UNRESOLVED["chevrolet"]`` used to end "...publishes no PDF",
    while ``derived/brochure_fetch_log.jsonl`` already held 19 documents fetched
    from GM consumer hosts the day before. The entries must state what the
    ledger says, and must keep saying that the discovery step is unresolved --
    the documents were found out of band, not by anything in this repo.
    """
    chevrolet = bs.DISCOVERY_UNRESOLVED["chevrolet"]
    assert "publishes no PDF" not in chevrolet
    assert "DOES serve brochure PDFs" in chevrolet
    assert "web-search:official-host-only" in chevrolet
    assert "still unresolved" in chevrolet
    for text in (bs.UNSUPPORTED_MAKES["gmc"], bs.UNSUPPORTED_MAKES["buick"]):
        assert "out of band" in text
        assert "not in OFFICIAL_HOSTS" in text
    assert "19 documents" in bs.GM_LEDGER_EVIDENCE or "19 GM-host" in bs.GM_LEDGER_EVIDENCE


def test_the_gm_hosts_that_served_documents_are_still_not_allowlisted():
    """
    The correction records the documents; it does not smuggle in the hosts.

    Registering ``www.gmc.com`` or ``www.buick.com`` needs a measured robots.txt
    response in ``HOST_ACCESS_NOTES``, which this lane has not taken. Until then
    the honest state is: documents exist, we may not fetch them.
    """
    assert bs.official_hosts_for("GMC") == frozenset()
    assert bs.official_hosts_for("Buick") == frozenset()
    assert bs.is_official_url("https://www.gmc.com/x.pdf", "GMC") is False


def test_one_declaring_page_is_not_enough_to_refuse_a_filing():
    """
    The measured limit of the derived-nameplate check, pinned so it is not
    mistaken for closed.

    Re-run 2026-08-02 against the real
    ``backend/data/brochures/2018_BMW_3_Series_Brochure.pdf``: truncated to its
    first page the check returns ok, because
    :data:`VARIANT_MIN_DERIVED_PAGES` needs two declaring pages before it will
    call a nameplate the subject. A one-page M3 spec sheet filed as a 3 Series
    is still admitted. Two pages is already enough.
    """
    header = (
        "BMW M3 SEDAN EXTERIOR COLORS UPHOLSTERY INTERIOR TRIMS "
        "WHEELS / TIRES PACKAGES TECHNICAL DATA"
    )
    assert bs.assess_nameplate_dominance([header], "BMW", "3 Series").ok is True
    assert bs.assess_nameplate_dominance([header] * 2, "BMW", "3 Series").ok is False


def test_the_derived_nameplate_table_covers_two_of_the_corpus_makes():
    """
    Reach, stated as a number so nobody reads this control as general.

    Measured 2026-08-02 over ``derived/brochure_text``: 47 makes, 2,815
    documents, and only 123 of those documents are in a make this table lists.
    Golf/GTI, Impreza/WRX, C-Class/C63, F-150/Raptor and 1500/TRX all have the
    same shape as M3/3 Series and none of them is covered.
    """
    assert set(bs.DERIVED_NAMEPLATES) == {"bmw", "audi"}
    assert bs.derived_nameplates_for("BMW", "3 Series") == frozenset({"m3"})
    for make, model in (
        ("Volkswagen", "Golf"),
        ("Subaru", "Impreza"),
        ("Ford", "F-150"),
        ("Ram", "1500"),
        ("Mercedes-Benz", "C-Class"),
    ):
        assert bs.derived_nameplates_for(make, model) == frozenset(), (make, model)


# --------------------------------------------------------------------------
# The HTML specification lane
# --------------------------------------------------------------------------


def test_honda_stays_unfetchable_for_the_pdf_lane_and_says_where_it_moved():
    """
    Honda publishes no PDF we can reach, and that measurement is unchanged.

    What changed on 2026-08-02 is only the CONCLUSION drawn from it: "no .pdf
    hrefs" was standing in for "publishes nothing we can use", and the HTML lane
    refutes the second half. The PDF-side reason must therefore still refuse
    Honda, and must point at where the make is actually served, or the next
    reader repeats the same wrong inference.
    """
    reason = unsupported_reason("Honda")
    assert reason, "Honda must still be refused by the PDF acquisition path"
    assert "html_spec_sources" in reason


def test_the_html_lane_note_names_what_it_does_not_unlock():
    """
    Under-claiming is the requirement. Two of the three makes this lane looked
    at are not unlocked, and the note has to say so with the measurement, not
    quietly omit them.
    """
    note = bs.HTML_SPEC_LANE_NOTE
    assert "Honda" in note
    assert "Chevrolet" in note and "Mercedes-Benz" in note
    assert "does not unlock" in note

    from backend.enrichment.html_spec_sources import (
        HTML_SPEC_MAKE_NOTES,
        HTML_SPEC_MAKES,
        html_spec_supported,
    )

    assert HTML_SPEC_MAKES == frozenset({"honda"})
    for make in ("chevrolet", "mercedesbenz"):
        assert make in HTML_SPEC_MAKE_NOTES
        assert HTML_SPEC_MAKE_NOTES[make].startswith("NOT SUPPORTED")
        assert "2026-08-02" in HTML_SPEC_MAKE_NOTES[make]
    assert html_spec_supported("Honda")
    assert not html_spec_supported("Chevrolet")


def test_the_html_lane_gets_no_host_allowlist_of_its_own():
    """
    A second source kind must not become a second door. The HTML lane resolves
    hosts through the same ``is_official_url`` the PDF lane uses.
    """
    from backend.enrichment.html_spec_sources import official_spec_page

    for url, make in (
        ("https://hondanews.com/en-US/x", "Honda"),
        ("https://www.toyota.com/brochures", "Toyota"),
    ):
        assert official_spec_page(url, make) is is_official_url(url, make)
    assert not official_spec_page("https://automobiles.honda.com/accord", "Honda")
