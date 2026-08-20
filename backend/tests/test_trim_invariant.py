"""
End-to-end tests for the ``car_trim_not_named_by_listing`` invariant and unit
tests for the heal script's selection logic.

The invariant (``_invariant_trim_not_named`` in
``backend/scripts/data_quality_invariants.py``) is the measurement; the heal
script (``backend/scripts/heal_matcher_trims.py``) acts on the provenance-proven
subset of what it measures. The heal imports the invariant's own token helpers,
so these tests exercise BOTH through the same normalization — if the token
logic drifts, both halves fail here together.

The invariant runs Python-side over a cursor iterator, so a fake cursor that
yields hand-built rows exercises the real shipped function end to end (same
approach as the monkeypatched runners in ``test_data_quality_invariants.py``);
only the SQL row-selection (active, non-empty trim) is out of frame.
"""

from __future__ import annotations

import sys
import types
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.scripts import data_quality_invariants as dq  # noqa: E402
from backend.scripts import heal_matcher_trims as heal  # noqa: E402

TRIM_SPEC = next(s for s in dq.SQL_INVARIANTS if s.id == "car_trim_not_named_by_listing")

#: Column order of the invariant's SELECT (data_quality_invariants.py:609).
_COLS = (
    "id", "year", "make", "model", "trim", "title",
    "model_full_raw", "description", "source_url",
)


class _FakeTrimCursor:
    """Yields pre-built rows in the invariant's SELECT column order."""

    def __init__(self, rows: list[dict]) -> None:
        self._rows = [tuple(r.get(c) for c in _COLS) for r in rows]
        self.executed: list[str] = []

    def execute(self, sql, *args) -> None:
        self.executed.append(sql)

    def __iter__(self):
        return iter(self._rows)


def _car(**overrides) -> dict:
    base = {
        "id": 1,
        "year": 2021,
        "make": "Jeep",
        "model": "Grand Cherokee",
        "trim": "Laredo",
        "title": None,
        "model_full_raw": None,
        "description": None,
        "source_url": None,
    }
    base.update(overrides)
    return base


def _run(rows: list[dict]) -> dq.InvariantResult:
    return dq._invariant_trim_not_named(_FakeTrimCursor(rows), TRIM_SPEC)


# ---------------------------------------------------------------------------
# 1. The invariant, end to end
# ---------------------------------------------------------------------------


def test_trim_named_in_title_is_not_a_violation() -> None:
    res = _run([_car(title="2021 Jeep Grand Cherokee Laredo 4WD")])
    assert res.count == 0
    assert res.denominator == 1


def test_trim_named_nowhere_is_a_violation_with_an_example() -> None:
    res = _run(
        [
            _car(
                title="2021 Jeep Grand Cherokee 4WD",
                model_full_raw="Grand Cherokee",
                description="One owner, clean history.",
                source_url="https://dealer.example/inventory/2021-jeep-grand-cherokee-4wd",
            )
        ]
    )
    assert res.count == 1
    assert res.denominator == 1
    assert res.examples[0]["trim"] == "Laredo"
    assert res.examples[0]["id"] == 1


def test_comma_list_trim_passes_when_one_component_is_named() -> None:
    """A dealer's own multi-trim label ("Latitude,North") must not count against
    the pipeline if the listing names ANY component."""
    res = _run(
        [_car(trim="Latitude,North", title="2022 Jeep Compass North 4x4")]
    )
    assert res.count == 0


def test_slash_list_trim_passes_when_one_component_is_named() -> None:
    res = _run([_car(trim="Latitude/North", title="2022 Jeep Compass North 4x4")])
    assert res.count == 0


def test_comma_list_trim_fails_when_no_component_is_named() -> None:
    res = _run([_car(trim="Latitude,North", title="2022 Jeep Compass Sport 4x4")])
    assert res.count == 1


def test_matching_is_punctuation_insensitive_both_ways() -> None:
    # Punctuated trim vs plain title...
    assert _run([_car(trim="S-Line", title="Audi A4 S Line quattro")]).count == 0
    # ...and plain trim vs punctuated title.
    assert _run([_car(trim="S Line", title="Audi A4 S-Line quattro")]).count == 0


def test_trim_in_the_url_slug_counts_as_named() -> None:
    """The VDP URL is listing text: hyphens normalize to spaces, so a slug names
    the trim as well as a title does."""
    res = _run(
        [
            _car(
                title="2021 Jeep Grand Cherokee",
                source_url="https://dealer.example/2021-jeep-grand-cherokee-laredo-c81234",
            )
        ]
    )
    assert res.count == 0


def test_synthesized_title_contamination_passes_the_invariant() -> None:
    """
    KNOWN BLIND SPOT — asserting CURRENT behavior, not desired behavior.

    If a pipeline stage synthesizes the title FROM the stored trim (title =
    f"{year} {make} {model} {trim}"), a matcher-written trim appears in the
    "listing text" and the invariant cannot see the contamination. That is why
    the heal script keys on spec_source_json provenance instead of on this
    invariant alone. Do not "fix" this here without changing how titles are
    stored; the invariant's definition is listing-text-based on purpose.
    """
    car = _car(trim="Overland")
    car["title"] = f"{car['year']} {car['make']} {car['model']} {car['trim']}"
    assert _run([car]).count == 0


def test_multi_word_trim_must_appear_as_a_contiguous_phrase() -> None:
    """'Grand Touring' scattered as ...Grand Cherokee... Touring... is not a match."""
    res = _run(
        [
            _car(
                trim="Grand Touring",
                title="Mazda MX-5 Grand Sport",
                description="Touring package available.",
            )
        ]
    )
    assert res.count == 1
    assert _run([_car(trim="Grand Touring", title="Mazda MX-5 Grand Touring")]).count == 0


def test_examples_are_capped_at_twenty_but_count_is_exact() -> None:
    rows = [_car(id=i, title="no trim here") for i in range(25)]
    res = _run(rows)
    assert res.count == 25
    assert res.denominator == 25
    assert len(res.examples) == 20


def test_result_is_wired_to_the_registered_spec() -> None:
    """The result must carry the registered invariant id, or the baseline gate
    in compare() would never see it."""
    res = _run([])
    assert res.id == "car_trim_not_named_by_listing"
    assert res.tier == "stored"
    assert res.count == 0 and res.denominator == 0


# ---------------------------------------------------------------------------
# 2. Heal-script selection logic (pure function; no DB)
# ---------------------------------------------------------------------------


def _spec_src(source: str) -> str:
    return '{"trim": {"source": "%s", "detail": "x", "fetched_at": "2026-08-01T00:00:00+00:00"}}' % source


def _heal_row(**overrides) -> dict:
    base = {
        "id": 7,
        "vin": "1C4RJFAG0MC000000",
        "trim": "Laredo",
        "title": "2021 Jeep Grand Cherokee 4WD",
        "model_full_raw": None,
        "description": None,
        "source_url": None,
        "spec_source_json": _spec_src("nhtsa_vpic"),
    }
    base.update(overrides)
    return base


def test_vpic_trim_not_named_is_a_target() -> None:
    assert heal.heal_target_source(_heal_row()) == "nhtsa_vpic"


def test_vpic_trim_named_in_listing_is_not_a_target() -> None:
    row = _heal_row(title="2021 Jeep Grand Cherokee Laredo 4WD")
    assert heal.heal_target_source(row) is None


def test_no_provenance_unnamed_trim_is_not_a_target() -> None:
    """The 5,532 no-provenance rows include real dealer dataLayer trims; the
    heal must never touch a trim it cannot prove a matcher wrote."""
    assert heal.heal_target_source(_heal_row(spec_source_json=None)) is None
    assert heal.heal_target_source(_heal_row(spec_source_json="")) is None
    assert heal.heal_target_source(_heal_row(spec_source_json="{}")) is None


def test_inventory_repair_trim_not_named_is_a_target() -> None:
    row = _heal_row(spec_source_json=_spec_src("inventory_repair"))
    assert heal.heal_target_source(row) == "inventory_repair"


def test_non_matcher_provenance_is_not_a_target() -> None:
    for src in ("dealer_datalayer", "matcher_trim_heal", "listing"):
        assert heal.heal_target_source(_heal_row(spec_source_json=_spec_src(src))) is None


def test_empty_or_missing_trim_is_not_a_target() -> None:
    assert heal.heal_target_source(_heal_row(trim=None)) is None
    assert heal.heal_target_source(_heal_row(trim="   ")) is None


def test_malformed_provenance_json_is_not_a_target() -> None:
    assert heal.heal_target_source(_heal_row(spec_source_json="{ not json")) is None
    assert heal.heal_target_source(_heal_row(spec_source_json='["list"]')) is None


def test_selection_uses_the_invariants_own_token_logic() -> None:
    """URL-slug naming and comma-lists must behave identically to the invariant."""
    named_by_slug = _heal_row(
        source_url="https://dealer.example/2021-jeep-grand-cherokee-laredo-c81234"
    )
    assert heal.heal_target_source(named_by_slug) is None
    multi = _heal_row(trim="Latitude,North", title="2022 Jeep Compass North 4x4")
    assert heal.heal_target_source(multi) is None


def test_target_source_survives_dict_shaped_spec_source_json() -> None:
    """Postgres jsonb may come back as a dict, not a string."""
    row = _heal_row(
        spec_source_json={"trim": {"source": "nhtsa_vpic", "detail": "x"}}
    )
    assert heal.heal_target_source(row) == "nhtsa_vpic"


# ---------------------------------------------------------------------------
# 3. The write-time-guard prerequisite (the re-fill trap)
# ---------------------------------------------------------------------------


def test_guard_check_refuses_when_predicate_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without spec_field_normalize.trim_named_in_listing, the backfill would
    re-write every blanked trim; the script must refuse to run at all."""
    fake = types.ModuleType("backend.utils.spec_field_normalize")
    monkeypatch.setitem(sys.modules, "backend.utils.spec_field_normalize", fake)
    with pytest.raises(SystemExit) as exc:
        heal.require_write_time_guard()
    assert "trim_named_in_listing" in str(exc.value)


def test_guard_check_passes_once_the_predicate_ships(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = types.ModuleType("backend.utils.spec_field_normalize")
    fake.trim_named_in_listing = lambda *a, **kw: True  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "backend.utils.spec_field_normalize", fake)
    heal.require_write_time_guard()  # must not raise
