"""Rung ORDER provenance.

Split out of the former backend/tests/test_trim_ladder.py. The rung-name gate
fixtures (``_legacy_rung_gate_off`` autouse, ``rung_gate_on``) live in this
package's conftest.py.
"""

from __future__ import annotations


# --- rung ORDER provenance ---------------------------------------------------

_DURANGO_DOC_BASIS = {
    "SXT": {
        "basis": "adds_to_edge",
        "direction": "named_as_baseline_by",
        "edge": {
            "trim": "GT",
            "below": "SXT",
            "page": 28,
            "source": "derived/brochure_text/2020__dodge__durango.json",
            "trim_quote": "GT",
            "below_quote": "Adds to SXT",
        },
    },
    "GT": {
        "basis": "adds_to_edge",
        "edge": {
            "trim": "GT",
            "below": "SXT",
            "page": 28,
            "source": "derived/brochure_text/2020__dodge__durango.json",
            "trim_quote": "GT",
            "below_quote": "Adds to SXT",
        },
    },
    "R/T": {
        "basis": "adds_to_edge",
        "edge": {
            "trim": "R/T",
            "below": "GT",
            "page": 29,
            "source": "derived/brochure_text/2020__dodge__durango.json",
            "trim_quote": "R/T",
            "below_quote": "Adds to GT",
        },
    },
    "Citadel": {
        "basis": "adds_to_edge",
        "edge": {
            "trim": "Citadel",
            "below": "GT",
            "page": 30,
            "source": "derived/brochure_text/2020__dodge__durango.json",
            "trim_quote": "CITADEL",
            "below_quote": "Adds to GT",
        },
    },
    "SRT": {
        "basis": "adds_to_edge",
        "edge": {
            "trim": "SRT",
            "below": "R/T",
            "page": 31,
            "source": "derived/brochure_text/2020__dodge__durango.json",
            "trim_quote": "SRT®",
            "below_quote": "Adds to R/T",
        },
    },
}


def _durango_overlay() -> dict:
    return {
        "source": "brochure_text_quoted",
        "year": 2020,
        "make": "Dodge",
        "model": "Durango",
        "rung_order": ["SXT", "GT", "R/T", "Citadel", "SRT"],
        "order_basis": _DURANGO_DOC_BASIS,
    }


def test_document_rung_order_is_top_first_and_year_gated() -> None:
    """The overlay prints base->top; the ladder runs top->base, so it is reversed."""
    from backend.enrichment.trim_ladder import _document_rung_order

    order, basis = _document_rung_order(
        _durango_overlay(), year=2020, make="Dodge", model="Durango"
    )
    assert order == ["SRT", "Citadel", "R/T", "GT", "SXT"]
    # Keyed by the rung-evidence key, so "SRT®" on the page finds the "SRT" rung.
    assert basis["srt"]["edge"]["below"] == "R/T"
    # A different model year's book orders nothing, same rule as its bullets.
    assert _document_rung_order(
        _durango_overlay(), year=2021, make="Dodge", model="Durango"
    ) == ([], {})
    assert _document_rung_order(None, year=2020, make="Dodge", model="Durango") == ([], {})


def test_document_order_overrides_the_hand_typed_rank_table() -> None:
    """luxury_rank puts Citadel above R/T above GT. The brochure does not."""
    from backend.enrichment.trim_ladder import _apply_document_order, _document_rung_order

    doc_order, doc_basis = _document_rung_order(
        _durango_overlay(), year=2020, make="Dodge", model="Durango"
    )
    # The order resolve_trim_ladder produced before the document was consulted.
    steps = [{"name": n} for n in ["Citadel", "R/T", "GT", "SRT"]]
    out = _apply_document_order(
        steps, doc_order, doc_basis, make="Dodge", model="Durango"
    )
    assert [s["name"] for s in out] == ["SRT", "Citadel", "R/T", "GT"]
    assert all(s["order_basis"]["basis"] == "adds_to_edge" for s in out)
    assert out[0]["order_basis"]["edge"]["page"] == 31
    # The incoming step dicts come out of an lru_cached ladder definition and
    # must not be written to.
    assert all("order_basis" not in s for s in steps)


def test_a_rung_the_document_does_not_place_keeps_its_neighbour() -> None:
    """Unplaced rungs are not swept to the bottom — that would assert a rank.

    Reproduces the 2022 Charger: the brochure walks the Scat Pack grades and
    never mentions SRT Hellcat Redeye Widebody, which is the top trim.
    """
    from backend.enrichment.trim_ladder import _apply_document_order

    doc_order = ["Scat Pack Widebody", "Scat Pack", "R/T", "GT"]
    doc_basis = {
        "scatpackwidebody": {"basis": "adds_to_edge", "edge": {"below": "Scat Pack"}},
        "scatpack": {"basis": "adds_to_edge", "edge": {"below": "R/T"}},
        "rt": {"basis": "adds_to_edge", "edge": {"below": "GT"}},
        "gt": {"basis": "adds_to_edge", "edge": {"below": "SXT"}},
    }
    steps = [
        {"name": n}
        for n in ["SRT Hellcat Redeye Widebody", "Scat Pack Widebody", "Scat Pack", "R/T", "GT", "SXT"]
    ]
    out = _apply_document_order(steps, doc_order, doc_basis, make="Dodge", model="Charger")
    assert [s["name"] for s in out] == [
        "SRT Hellcat Redeye Widebody",
        "Scat Pack Widebody",
        "Scat Pack",
        "R/T",
        "GT",
        "SXT",
    ]
    unplaced = {s["name"]: s["order_basis"]["basis"] for s in out}
    assert unplaced["SRT Hellcat Redeye Widebody"] == "unproven"
    assert unplaced["SXT"] == "unproven"
    assert unplaced["Scat Pack"] == "adds_to_edge"


def test_no_document_order_leaves_every_rung_labelled_unproven() -> None:
    """Silence is the default: the hand-typed table places the rung and says so."""
    from backend.enrichment.trim_ladder import _apply_document_order

    steps = [{"name": "Limited"}, {"name": "SE"}]
    out = _apply_document_order(steps, [], {}, make="Toyota", model="Camry")
    assert [s["name"] for s in out] == ["Limited", "SE"]
    assert all(s["order_basis"] == {
        "basis": "unproven",
        "store": "luxury_rank_table",
        "proven": False,
    } for s in out)


def test_ladder_order_verdict_is_the_weakest_rungs(monkeypatch) -> None:
    """One unproven rung makes the whole ladder unproven — a shopper cannot tell."""
    monkeypatch.setenv("TRIM_RUNGS_REQUIRE_PROVENANCE", "0")
    from backend.enrichment.trim_ladder import _build_ladder_result

    def _result(bases):
        ladder = {
            "id": "t",
            "source": "curated",
            "steps": [
                {"name": name, "aliases": [], "adds": [], "order_basis": basis}
                for name, basis in bases
            ],
        }
        return _build_ladder_result(
            ladder, make="Dodge", model="Durango", year=2020, trim=None
        )

    edge = {"basis": "adds_to_edge", "proven": True}
    printed = {"basis": "printed_sequence", "proven": False}
    unproven = {"basis": "unproven", "store": "luxury_rank_table", "proven": False}

    all_edges = _result([("SRT", edge), ("R/T", edge)])["order_provenance"]
    assert all_edges["basis"] == "adds_to_edge"
    assert all_edges["proven"] is True
    assert all_edges["ordered_rungs"] == all_edges["total_rungs"] == 2

    mixed = _result([("SRT", edge), ("R/T", printed)])["order_provenance"]
    assert mixed["basis"] == "printed_sequence"
    assert mixed["proven"] is False

    partial = _result([("SRT", edge), ("R/T", unproven)])["order_provenance"]
    assert partial["basis"] == "unproven"
    assert partial["ordered_rungs"] == 1
    assert partial["total_rungs"] == 2
