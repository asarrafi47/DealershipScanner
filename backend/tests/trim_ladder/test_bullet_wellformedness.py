"""Bullet well-formedness: orphan fragments, doubled labels, prose.

Split out of the former backend/tests/test_trim_ladder.py. The rung-name gate
fixtures (``_legacy_rung_gate_off`` autouse, ``rung_gate_on``) live in this
package's conftest.py.
"""

from __future__ import annotations

from backend.enrichment.trim_ladder import resolve_trim_ladder
from backend.tests.trim_ladder._helpers import _all_bullets


# ---------------------------------------------------------------------------
# Bullet well-formedness — the orphan-fragment / doubled-label / prose classes
# a fleet sweep of the top-1500 (make, model, year, trim) combos found still
# rendering. Each test below names the producer that made the bad line.
# ---------------------------------------------------------------------------


def test_semicolon_inside_parentheses_is_not_a_split_point() -> None:
    """EPA-style engine cells lost their tail to a naive ';' split.

    "Hybrid 2.5L I4 (SIDI & PFI; Hybrid)" was cut into "Hybrid 2.5L I4 (SIDI &
    PFI" and "Hybrid)", and both halves reached the page.
    """
    from backend.enrichment.trim_ladder_knowledge import split_outside_brackets

    assert split_outside_brackets("Hybrid 2.5L I4 (SIDI & PFI; Hybrid)") == [
        "Hybrid 2.5L I4 (SIDI & PFI; Hybrid)"
    ]
    assert split_outside_brackets("A feature; B feature (x; y)") == [
        "A feature",
        "B feature (x; y)",
    ]


def test_unbalanced_bullet_is_never_rendered() -> None:
    from backend.enrichment.trim_ladder_knowledge import is_wellformed_trim_bullet

    for orphan in (
        "Four-Wheel Drive)",
        "Mild Hybrid)",
        "Hybrid 2.5L I4 (SIDI & PFI",
        "L5P Duramax 6.6 L V8 (2017-20) 445 hp (332 kW",
        "Stop-Start)",
    ):
        assert not is_wellformed_trim_bullet(orphan), orphan
    # An inch mark is a measurement, not an unbalanced quote.
    assert is_wellformed_trim_bullet('NissanConnect 8" touchscreen with Apple CarPlay')


def test_doubled_label_prefix_is_collapsed_not_shown() -> None:
    """/car/282933 (2020 GMC Sierra 1500 SLT) Base rung rendered the label twice."""
    from backend.enrichment.trim_ladder_knowledge import collapse_repeated_label_prefix

    assert (
        collapse_repeated_label_prefix("Engine Options: Engine Options: LV3 EcoTec3 4.3 L V6")
        == "Engine Options: LV3 EcoTec3 4.3 L V6"
    )
    assert (
        collapse_repeated_label_prefix("Engine Option Engine Options: LV3 EcoTec3")
        == "Engine Options: LV3 EcoTec3"
    )
    assert collapse_repeated_label_prefix("Screen Size: 8-inch touch screen") == (
        "Screen Size: 8-inch touch screen"
    )


def test_gmc_sierra_base_rung_has_no_doubled_label() -> None:
    import re

    result = resolve_trim_ladder(make="GMC", model="Sierra 1500", year=2020, trim="SLT")
    for bullet in _all_bullets(result):
        assert not re.match(r"^(.{3,30}?):\s*\1:", bullet), bullet
        assert bullet.count("(") == bullet.count(")"), bullet


def test_mid_sentence_fragments_are_dropped_but_marques_survive() -> None:
    from backend.enrichment.trim_ladder_knowledge import bullet_is_fragment

    for fragment in (
        "of torque, and front-wheel drive as standard.",
        "alongside second-generati",
        "subwoofer",
        "suede seating",
        "kWh AWD: 279 mi (449 km)",
        "From heated driver and front passenger seats to front-row glass, your s",
        "7-inch Display Au",
    ):
        assert bullet_is_fragment(fragment), fragment
    for real in (
        "quattro all-wheel drive",
        "i-FORCE MAX 3.4L-T V6 Hybrid 4x4 powertrain",
        "eAWD drivetrain standard",
        "Wood-wrapped steering wheel with Adaptive Cruise Control Stop and Go",
        "27.2 cubic feet cargo volume with rear seats up",
        "2.5L Atkinson-cycle I-4 with electric motor — 177 net hp",
    ):
        assert not bullet_is_fragment(real), real


def test_encyclopedia_prose_and_disclaimers_are_dropped() -> None:
    from backend.enrichment.trim_ladder_knowledge import is_wellformed_trim_bullet

    for prose in (
        "No diesel engine option is offered for this generation.",
        "Trucks ordered with bucket seats and center console now featured a wireless",
        "Honda Odyssey can refer to three motor vehicles manufactured by Honda",
        "Hill Start Assist Control is also standard which prevents rolling backward",
        "The 2012 model year A6 features all the driver assistance systems from the A8",
        "Other options include xenon plus headlights with an all-weather light",
        "SiriusXM Customer Agreement available at www.siriusxm.com",
        "Collision-avoidance system and is not a substitute for safe driving",
        "Engines Capacity Model year Power Torque Displacement Bore",
    ):
        assert not is_wellformed_trim_bullet(prose), prose


def test_warranty_and_service_programs_are_not_trim_adds() -> None:
    from backend.enrichment.trim_ladder_knowledge import is_generic_trim_add

    for line in (
        "Includes 5-year/60,000-mile powertrain warranty coverage",
        "Includes Courtesy Transportation Program",
        "Includes 2-year/24,000-mile scheduled maintenance program",
        "Includes 24/7 Lincoln Concierge service",
    ):
        assert is_generic_trim_add(line), line


def test_verb_prefixes_are_stripped_from_bullets() -> None:
    from backend.enrichment.trim_ladder_knowledge import strip_lower_rung_reference

    assert strip_lower_rung_reference("Adds Includes 4.6L V8 engine") == "4.6L V8 engine"
    assert strip_lower_rung_reference("Includes Blind Spot Monitor") == "Blind Spot Monitor"
    assert (
        strip_lower_rung_reference("Includes Luxury plus digital rearview mirror and cool box")
        # Capitalised on the way out: a lowercase head is what
        # ``bullet_is_fragment`` rejects, so the stripped remainder used to be
        # dropped instead of shown.
        == "Digital rearview mirror and cool box"
    )
    # Nothing to strip: leave the sentence alone rather than mangle it.
    assert strip_lower_rung_reference("Included premium audio") == "Included premium audio"


def test_trial_subscription_strip_leaves_a_finished_phrase() -> None:
    """generic_ram_1500 Laramie rendered "4G LTE WiFi Hotspot with 1GB/"."""
    from backend.enrichment.trim_ladder_knowledge import (
        rank_trim_adds,
        strip_trial_subscription_clause,
    )

    assert (
        strip_trial_subscription_clause("4G LTE WiFi Hotspot with 1GB/3-month trial")
        == "4G LTE WiFi Hotspot"
    )
    out = rank_trim_adds(["4G LTE WiFi Hotspot with 1GB/3-month trial"], year=2020)
    assert not any(b.endswith("/") for b in out), out


def test_brochure_source_tag_is_not_rendered() -> None:
    from backend.enrichment.trim_ladder import _bullet_display_parts

    parts = _bullet_display_parts(
        "Audio System Layout: [Brochure] Bose audio | 12-speaker system",
        trim_name="Premium",
        make="Mazda",
        model="CX-50",
        year=2025,
    )
    assert parts
    assert not any("[Brochure]" in p for p in parts), parts
    assert not any("|" in p for p in parts), parts


def test_spec_row_bullets_keep_bracketed_values_whole() -> None:
    from backend.enrichment.trim_ladder import _trim_spec_rows_to_bullets

    rows = [{"label": "Engine Options", "value": "Hybrid 2.5L I4 (SIDI & PFI; Hybrid); 3.4L V6"}]
    assert _trim_spec_rows_to_bullets(rows, trim_name="XSE") == [
        "Hybrid 2.5L I4 (SIDI & PFI; Hybrid)",
        "3.4L V6",
    ]
