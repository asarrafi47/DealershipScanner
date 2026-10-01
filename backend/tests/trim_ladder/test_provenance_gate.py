"""Provenance gate: a bullet renders only if it can name its page.

Split out of the former backend/tests/test_trim_ladder.py. The rung-name gate
fixtures (``_legacy_rung_gate_off`` autouse, ``rung_gate_on``) live in this
package's conftest.py.
"""

from __future__ import annotations

import pathlib

from backend.enrichment.trim_ladder import resolve_trim_ladder


# --- provenance gate: a bullet renders only if it can name its page ------


def _overlay_bullets(result: dict) -> list[str]:
    return [b for step in (result or {}).get("steps") or [] for b in step.get("adds") or []]


def test_uncited_overlay_bullets_do_not_render() -> None:
    """
    2011 Jeep Grand Cherokee Laredo claimed "3.6L Pentastar V6 engine with 360
    horsepower" — the 2011 Pentastar makes 290 (epa_extended_specs says 293) and
    360 is the 5.7 HEMI V8. It came from a manual_brochure_review overlay that
    cites nothing, so under the gate that rung says nothing at all.
    """
    result = resolve_trim_ladder(
        make="Jeep", model="Grand Cherokee", year=2011, trim="Laredo"
    )
    assert result is not None
    assert not any("360 horsepower" in b for b in _overlay_bullets(result))
    assert _overlay_bullets(result) == []


def test_kill_switch_restores_uncited_bullets(monkeypatch) -> None:
    """The gate is reversible: TRIM_ADDS_REQUIRE_PROVENANCE=0 brings the old page back."""
    monkeypatch.setenv("TRIM_ADDS_REQUIRE_PROVENANCE", "0")
    result = resolve_trim_ladder(
        make="Jeep", model="Grand Cherokee", year=2011, trim="Laredo"
    )
    assert any("360 horsepower" in b for b in _overlay_bullets(result))


def test_verified_overlay_still_renders() -> None:
    """
    The gate is not a blanket off switch — a VERIFIED page citation keeps its bullet.

    The 2026 RAV4 overlay cites ``2026_Toyota_RAV4_Brochure.pdf``, we still hold
    that PDF, and ``backend/scripts/verify_trim_citations.py`` re-opened page 7
    and found this line printed there. That is why it renders.

    (This used to assert on the 2020 Durango R/T. We no longer hold the 2020
    Durango brochure PDF, so no citation in that file can be re-opened and it
    renders nothing — see ``test_a_citation_we_cannot_reopen_does_not_render``.)
    """
    result = resolve_trim_ladder(make="Toyota", model="RAV4", year=2026, trim="SE")
    assert result is not None
    se = next(s for s in result["steps"] if s["name"] == "SE")
    assert "18-in multi-spoke black sport alloy wheels with black lug nuts" in se["adds"]
    citation = next(
        c for c in se["adds_citations"]
        if c["text"].startswith("18-in multi-spoke")
    )
    assert citation["store"] == "brochure_text_quoted"
    assert citation["page"] == 7


def test_a_citation_we_cannot_reopen_does_not_render() -> None:
    """
    A citation is only worth something while we can go back to the document.

    The 2010 Audi Q5 overlay cites a brochure PDF we do not hold, so the
    build-time verifier could not re-open a single one of its 17 citations and
    stamped them all ``verified: false`` / ``source_pdf_not_held``. Nothing in
    that file renders.

    (This used to be asserted on the 2020 Dodge Durango, whose PDF was missing
    at the time. That brochure has since been reacquired and 42 of its 44
    citations now verify, so it no longer demonstrates this rule — see
    ``test_durango_2020_renders_only_what_the_reopened_pdf_prints``.)
    """
    import json

    from backend.enrichment.dictionary_paths import TRIM_ADDS_BY_YEAR_DIR

    raw = json.loads(
        (TRIM_ADDS_BY_YEAR_DIR / "2010__audi__q5.json").read_text(encoding="utf-8")
    )
    entries = [e for v in raw["adds_provenance"].values() for e in v]
    assert entries
    assert all(e.get("verified") is False for e in entries)
    assert {e.get("verified_why") for e in entries} == {"source_pdf_not_held"}

    result = resolve_trim_ladder(make="Audi", model="Q5", year=2010, trim="Premium Plus")
    if result is not None:
        overlay_bullets = [
            c["text"]
            for step in result["steps"]
            for c in (step.get("adds_citations") or [])
            if c.get("store") == "brochure_text_quoted"
        ]
        assert overlay_bullets == [], overlay_bullets


def test_every_rendered_overlay_bullet_names_a_file_and_page() -> None:
    """
    Whatever the ladder renders for a catalog key that HAS an overlay must appear
    in that overlay's adds_provenance with a source file and a page number.
    """
    import json

    from backend.enrichment.brochure_extract import (
        ADMISSIBLE_OVERLAY_SOURCES,
        load_brochure_trim_overlay,
    )

    overlay_dir = (
        pathlib.Path(__file__).resolve().parents[2]
        / "dictionary/derived/trim_adds_by_year"
    )
    checked = 0
    for path in sorted(overlay_dir.glob("*.json")):
        raw = json.loads(path.read_text(encoding="utf-8"))
        if str(raw.get("source") or "") not in ADMISSIBLE_OVERLAY_SOURCES:
            continue
        year, make, model = raw["year"], raw["make"], raw["model"]
        overlay = load_brochure_trim_overlay(year, make, model) or {}
        provenance = raw.get("adds_provenance") or {}
        for trim, bullets in (overlay.get("adds_by_trim") or {}).items():
            cited = {
                str(e.get("text") or "").strip()
                for e in provenance.get(trim) or []
                if str(e.get("source") or "").strip() and e.get("page") is not None
            }
            for bullet in bullets:
                assert bullet in cited, f"{path.name} {trim}: {bullet}"
                checked += 1
    assert checked > 0


def test_uncited_overlays_contribute_nothing_anywhere() -> None:
    """Every overlay whose source is not admissible yields an empty adds_by_trim."""
    import json

    from backend.enrichment.brochure_extract import (
        ADMISSIBLE_OVERLAY_SOURCES,
        admissible_overlay_adds,
    )

    overlay_dir = (
        pathlib.Path(__file__).resolve().parents[2]
        / "dictionary/derived/trim_adds_by_year"
    )
    blocked = 0
    for path in sorted(overlay_dir.glob("*.json")):
        raw = json.loads(path.read_text(encoding="utf-8"))
        if str(raw.get("source") or "") in ADMISSIBLE_OVERLAY_SOURCES:
            continue
        assert admissible_overlay_adds(raw) == {}, path.name
        blocked += 1
    assert blocked > 3000


def test_admissible_overlay_adds_drops_bullets_missing_a_page() -> None:
    """A citation without a page number is not a citation."""
    from backend.enrichment.brochure_extract import admissible_overlay_adds

    overlay = {
        "source": "brochure_text_quoted",
        "year": 2026,
        "adds_by_trim": {"GT": ["Cited bullet", "Pageless bullet", "Unlisted bullet"]},
        "adds_provenance": {
            "GT": [
                {
                    "text": "Cited bullet",
                    "source": "derived/brochure_text/x.json",
                    "page": 7,
                    "verified": True,
                },
                {
                    "text": "Pageless bullet",
                    "source": "derived/brochure_text/x.json",
                    "verified": True,
                },
            ]
        },
    }
    assert admissible_overlay_adds(overlay) == {"GT": ["Cited bullet"]}
    assert admissible_overlay_adds(overlay, for_year=2026) == {"GT": ["Cited bullet"]}


def test_a_citation_the_verifier_has_not_signed_off_does_not_render() -> None:
    """
    ``verified`` has to be literally True. Missing, false, or a truthy string is
    not a pass — an overlay written before the verifier existed must be silent
    rather than grandfathered in.
    """
    from backend.enrichment.brochure_extract import admissible_overlay_adds

    def overlay(entry_extra: dict) -> dict:
        return {
            "source": "brochure_text_quoted",
            "year": 2026,
            "adds_by_trim": {"GT": ["A printed line"]},
            "adds_provenance": {
                "GT": [
                    {
                        "text": "A printed line",
                        "source": "derived/brochure_text/x.json",
                        "page": 7,
                        **entry_extra,
                    }
                ]
            },
        }

    assert admissible_overlay_adds(overlay({"verified": True})) == {"GT": ["A printed line"]}
    assert admissible_overlay_adds(overlay({})) == {}
    assert admissible_overlay_adds(overlay({"verified": False})) == {}
    assert admissible_overlay_adds(overlay({"verified": "false"})) == {}
    assert admissible_overlay_adds(overlay({"verified": 1})) == {}


def test_a_brochure_for_another_model_year_is_not_evidence_about_this_car() -> None:
    """
    ``load_brochure_trim_overlay`` reaches ±2 years for a trim LIST. A citation
    may not travel with it: a 2024 book does not say what a 2026 car has.
    """
    from backend.enrichment.brochure_extract import (
        admissible_overlay_adds,
        overlay_citable_for_year,
    )

    overlay = {
        "source": "brochure_text_quoted",
        "year": 2024,
        "make": "Toyota",
        "model": "RAV4",
        "adds_by_trim": {"XSE": ["Power tilt/slide moonroof"]},
        "adds_provenance": {
            "XSE": [
                {
                    "text": "Power tilt/slide moonroof",
                    "source": "derived/brochure_text/2024__toyota__rav4.json",
                    "page": 9,
                    "verified": True,
                }
            ]
        },
    }
    assert overlay_citable_for_year(overlay, 2024)
    assert not overlay_citable_for_year(overlay, 2025)
    assert not overlay_citable_for_year(overlay, 2026)
    assert not overlay_citable_for_year(overlay, None)
    assert admissible_overlay_adds(overlay, for_year=2024) == {
        "XSE": ["Power tilt/slide moonroof"]
    }
    assert admissible_overlay_adds(overlay, for_year=2025) == {}
    assert admissible_overlay_adds(overlay, for_year=2026) == {}


def test_the_rung_register_refuses_an_overlay_for_a_different_year() -> None:
    """
    Second, independent year check — the one inside the citation register.

    ``_build_ladder_result`` is reachable with an overlay a caller assembled
    itself (scripts, tests), so the register does not assume
    ``admissible_overlay_adds`` already ran.
    """
    from backend.enrichment.trim_ladder import (
        _CitationRegister,
        _register_overlay_citations,
    )

    overlay = {
        "source": "brochure_text_quoted",
        "year": 2026,
        "make": "Toyota",
        "model": "RAV4",
        "adds_provenance": {
            "XSE": [
                {
                    "text": "Power tilt/slide moonroof with one-touch open/close",
                    "source": "derived/brochure_text/2026__toyota__rav4.json",
                    "page": 9,
                    "verified": True,
                }
            ]
        },
    }
    bullet = "Power tilt/slide moonroof with one-touch open/close"

    same_year = _CitationRegister()
    _register_overlay_citations(
        same_year, overlay, "XSE", make="Toyota", model="RAV4", year=2026
    )
    assert same_year.citation_for(bullet) is not None

    for wrong in (2025, 2027, None):
        reg = _CitationRegister()
        _register_overlay_citations(
            reg, overlay, "XSE", make="Toyota", model="RAV4", year=wrong
        )
        assert reg.citation_for(bullet) is None, wrong

    wrong_make = _CitationRegister()
    _register_overlay_citations(
        wrong_make, overlay, "XSE", make="Honda", model="RAV4", year=2026
    )
    assert wrong_make.citation_for(bullet) is None

    unverified = {
        **overlay,
        "adds_provenance": {
            "XSE": [
                {**overlay["adds_provenance"]["XSE"][0], "verified": None},
            ]
        },
    }
    reg = _CitationRegister()
    _register_overlay_citations(
        reg, unverified, "XSE", make="Toyota", model="RAV4", year=2026
    )
    assert reg.citation_for(bullet) is None


def test_overlay_still_suppresses_substitute_copy_when_all_bullets_are_blocked() -> None:
    """
    Blocking an overlay's bullets must not hand the rung over to the generated
    "Mid-level trim with added convenience features" copy: silence, not filler.
    """
    result = resolve_trim_ladder(
        make="Jeep", model="Grand Cherokee", year=2011, trim="Laredo"
    )
    assert result is not None
    assert len(result["steps"]) >= 2
    assert all(not step["adds"] and not step["specs"] for step in result["steps"])


def test_blocked_overlay_is_not_refilled_from_uncited_stores() -> None:
    """
    Removing an uncited overlay bullet must not promote a different uncited line
    into its place. 2026 Nissan Sentra SR gained "Front-Wheel Drive" from the
    trim spec sheets the moment the overlay went quiet; the rung stays silent.
    """
    result = resolve_trim_ladder(make="Nissan", model="Sentra", year=2026, trim="SV")
    assert result is not None
    for step in result["steps"]:
        assert not step["adds"], (step["name"], step["adds"])
