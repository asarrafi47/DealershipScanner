"""D-N12 on every surface (unit N2.4, docs/NEXT_PHASES_PLAN_2026_10.md N2).

Owner decision D-N12 (2026-10-18): a New, non-CPO car with no odometer value is
complete; "Show no mileage line on New cars; keep 'Mileage not listed' for used
cars". N2.1 changed the completeness rule and the car page; the grid card and
the compare cell still printed "Mileage not listed". One helper now decides
(``mileage_display.mileage_line_hidden``) with the predicate the incomplete
index uses (``is_used_or_certified``), and each surface reads it:

* grid card (``serialize_car_for_listings_grid`` -> ``listings/card.js``),
* car page and ``GET /api/cars/<id>`` (``serialize_car_for_api`` -> ``car.html``),
* compare table (``compare_specs._fmt_mileage``, the empty-cell dash),
* home saved / recommended / history cards and the landing strip (grid rows).

Used and CPO cars, and a "new" car older than last model year, keep "Mileage
not listed" at 0 or NULL; a real odometer figure always shows.
"""
from __future__ import annotations

import json
import re
import shutil
import subprocess
from datetime import date, datetime
from pathlib import Path

import pytest

from backend.utils import listing_completeness
from backend.utils.car_serialize import serialize_car_for_api, serialize_car_for_listings_grid
from backend.utils.compare_specs import _compare_rows, _fmt_mileage
from backend.utils.mileage_display import is_used_or_certified, mileage_line_hidden, mileage_not_listed

_ROOT = Path(__file__).resolve().parents[2]
_TODAY = date(2026, 10, 18)
_Y = date.today().year  # the surfaces read the wall clock

# (id, row overrides, what every surface shows): "hidden" = no mileage line,
# "not_listed" = "Mileage not listed", anything else = the odometer text.
CASES = [
    ("new-null", {"condition": "New", "mileage": None, "year": _Y}, "hidden"),
    ("new-zero-current", {"condition": "New", "mileage": 0, "year": _Y}, "0 mi"),
    ("new-zero-old", {"condition": "New", "mileage": 0, "year": _Y - 3}, "not_listed"),
    ("new-null-old", {"condition": "New", "mileage": None, "year": _Y - 3}, "not_listed"),
    ("cpo-flag-null", {"condition": "New", "is_cpo": 1, "mileage": None, "year": _Y}, "not_listed"),
    ("cpo-label-null", {"condition": "Certified Pre-Owned", "mileage": None, "year": _Y - 2}, "not_listed"),
    ("used-zero", {"condition": "Used", "mileage": 0, "year": _Y - 5}, "not_listed"),
    ("used-null", {"condition": "Used", "mileage": None, "year": _Y - 5}, "not_listed"),
    ("used-figure", {"condition": "Used", "mileage": 34567, "year": _Y - 5}, "34,567 mi"),
    ("new-figure", {"condition": "New", "mileage": 12, "year": _Y}, "12 mi"),
]
_IDS = [c[0] for c in CASES]


def _row(**overrides):
    row = {"id": 77, "vin": "1HGCM82633A004352", "make": "Honda", "model": "Accord", "trim": "EX",
           "price": 30000, "dealer_id": "demo", "inactive": 0, "gallery": [], "history_highlights": [],
           "fuel_type": "Gasoline", "drivetrain": "FWD"}
    row.update(overrides)
    row.setdefault("title", f"{row.get('year')} Honda Accord EX")
    return row


# ── the rule ───────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("mileage", "kw", "hidden"),
    [
        pytest.param(None, {"condition": "New", "year": 2026}, True, id="new-null"),
        pytest.param("", {"condition": "New", "year": 2027}, True, id="new-empty"),
        pytest.param("  ", {"condition": "new", "is_cpo": 0, "year": 2026}, True, id="new-blank-str"),
        pytest.param(None, {"condition": "New", "year": 2025}, True, id="new-null-last-year"),
        pytest.param(None, {"condition": "New"}, True, id="new-null-no-year"),
        pytest.param(None, {"condition": "New", "year": 2024}, False, id="new-null-old"),
        pytest.param(0, {"condition": "New", "year": 2026}, False, id="new-zero"),
        pytest.param(0, {"condition": "New", "year": 2023}, False, id="new-zero-old"),
        pytest.param(12, {"condition": "New", "year": 2026}, False, id="new-12"),
        pytest.param(None, {"condition": "New", "is_cpo": 1, "year": 2026}, False, id="new-cpo-null"),
        pytest.param(None, {"condition": "Certified Pre-Owned", "year": 2026}, False, id="cpo-null"),
        pytest.param(None, {"condition": "Used", "year": 2026}, False, id="used-null"),
        pytest.param(0, {"condition": "Used", "year": 2020}, False, id="used-zero"),
        pytest.param(None, {"condition": "", "year": 2026}, True, id="blank-condition-null"),
        pytest.param(None, {"condition": "ctp", "year": 2026}, True, id="ctp-null"),
        pytest.param("n/a", {"condition": "New", "year": 2026}, False, id="new-unparseable"),
        pytest.param("—", {"condition": "New", "year": 2026}, False, id="new-dash"),
    ],
)
def test_mileage_line_hidden_rule(mileage, kw, hidden):
    assert mileage_line_hidden(mileage, today=_TODAY, **kw) is hidden
    if hidden:  # a hidden line is always a "not listed" odometer underneath
        assert mileage_not_listed(mileage, today=_TODAY, **kw) is True


_CONDITIONS = ["New", "new", " NEW ", "", None, "ctp", "demo", "Used", "Pre-Owned", "preowned",
               "Certified", "Certified Pre-Owned", "CPO", "carbravo", "GM Certified Used"]


@pytest.mark.parametrize("is_cpo", [None, 0, 1, True, "1"])
@pytest.mark.parametrize("condition", _CONDITIONS)
def test_display_and_completeness_share_one_predicate(condition, is_cpo):
    """The page hides the line exactly where the index calls a NULL odometer complete
    (for a model year that is not old)."""
    raw = {"mileage": None, "condition": condition, "is_cpo": is_cpo}
    assert listing_completeness._is_used_or_certified(raw) is is_used_or_certified(condition, is_cpo)
    complete = not listing_completeness._mileage_blank(raw)
    assert mileage_line_hidden(None, condition=condition, is_cpo=is_cpo, year=2026, today=_TODAY) is complete


# ── serializers (grid card payload, car page / API payload) ────────────────────


def _expect_flags(shows):
    return {"mileage_line_hidden": shows == "hidden", "mileage_not_listed": shows in ("hidden", "not_listed")}


@pytest.mark.parametrize(("case", "overrides", "shows"), CASES, ids=_IDS)
def test_serializers_carry_the_flag(case, overrides, shows):
    want = _expect_flags(shows)
    grid = serialize_car_for_listings_grid(_row(**overrides))
    api = serialize_car_for_api(_row(**overrides), verified_specs={}, include_extended_display=False)
    for out in (grid, api):
        assert {k: out[k] for k in want} == want, case
        keys = list(out)
        assert keys[keys.index("mileage_not_listed") + 1] == "mileage_line_hidden"


# ── grid card: real serializer output through listings/card.js ──────────────

_NODE_CARD_META = r"""
const { loadStatic } = require(process.env.DS_LOAD_JS);
const w = loadStatic(["ds_core.js", "sc-helpers.js", "listings/card.js"]);
const rows = JSON.parse(require("node:fs").readFileSync(0, "utf8"));
const out = rows.map((c) => {
    const html = w.DSL.renderCardHtml(c, new Set(), [], { loggedIn: false, zipForUrl: "", distanceMiles: null });
    const m = /<p class="result-meta">(.*?)<\/p>/.exec(html);
    return m ? m[1].replace(/<[^>]+>/g, "").split(" &middot; ") : null;
});
process.stdout.write(JSON.stringify(out));
"""


@pytest.mark.skipif(shutil.which("node") is None, reason="node not installed")
def test_grid_card_meta_line_per_case(monkeypatch):
    rows = [serialize_car_for_listings_grid(_row(**overrides)) for _case, overrides, _shows in CASES]
    monkeypatch.setenv("DS_LOAD_JS", str(_ROOT / "backend/tests/js/_load.js"))
    proc = subprocess.run(
        [shutil.which("node"), "-e", _NODE_CARD_META],
        input=json.dumps(rows, default=str),
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr[-2000:]
    metas = json.loads(proc.stdout)
    for (case, _overrides, shows), bits in zip(CASES, metas):
        assert bits is not None, case
        mileage_bits = [b for b in bits if b.endswith(" mi") or "Mileage" in b]
        if shows == "hidden":
            assert mileage_bits == [], (case, bits)
        elif shows == "not_listed":
            assert mileage_bits == ["Mileage not listed"], (case, bits)
        else:
            assert mileage_bits == [shows], (case, bits)
        assert bits[-2:] == ["Gasoline", "FWD"], (case, bits)


# ── compare table ──────────────────────────────────────────────────────────────


@pytest.mark.parametrize(("case", "overrides", "shows"), CASES, ids=_IDS)
def test_compare_mileage_cell_per_case(monkeypatch, case, overrides, shows):
    from backend.utils import compare_specs

    monkeypatch.setattr(compare_specs, "prepare_car_detail_context", lambda _raw: {"verified_specs": {}})
    car = compare_specs.serialize_car_for_compare(_row(**overrides))
    cell = next(r for r in _compare_rows([car]) if r["key"] == "mileage")["cells"][0]
    want = {"hidden": "—", "not_listed": "Mileage not listed"}.get(shows, shows)
    assert cell == want, case
    # The same answer from a bare row (no serializer flags): the helper decides.
    bare = {k: overrides.get(k) for k in ("mileage", "condition", "is_cpo", "year")}
    assert _fmt_mileage(bare) == want, case


def test_compare_new_null_cell_is_the_empty_dash_not_a_difference():
    """Two New cars without an odometer compare equal on mileage (both dashes)."""
    a = {"mileage": None, "condition": "New", "year": _Y, "mileage_line_hidden": True}
    row = next(r for r in _compare_rows([a, dict(a)]) if r["key"] == "mileage")
    assert row["cells"] == ["—", "—"] and row["differs"] is False


# ── car page and GET /api/cars/<id> ────────────────────────────────────────────


def _car_page_client(monkeypatch, tmp_path, app_factory, overrides):
    from backend.routes import cars_pages

    main = app_factory(ALLOW_DEFAULT_APP_USER="0", RATE_LIMIT_SQLITE_PATH=str(tmp_path / "rate_limits.db"))
    car = _row(**overrides)
    monkeypatch.setattr(cars_pages, "get_car_by_id", lambda *_a, **_k: dict(car))
    monkeypatch.setattr(cars_pages, "prepare_car_detail_context",
                        lambda _raw: {"verified_specs": {}, "gallery_images": [], "packages_panel_has_content": False})
    return main.app.test_client()


@pytest.mark.parametrize(("case", "overrides", "shows"), CASES, ids=_IDS)
def test_car_page_and_api_per_case(monkeypatch, tmp_path, app_factory, case, overrides, shows):
    with _car_page_client(monkeypatch, tmp_path, app_factory, overrides) as client:
        page = client.get("/car/77")
        api = client.get("/api/cars/77")
    assert page.status_code == 200 and api.status_code == 200
    html = page.get_data(as_text=True)
    api_car = api.get_json()["car"]
    assert {k: api_car[k] for k in ("mileage_line_hidden", "mileage_not_listed")} == _expect_flags(shows), case

    hero_unlisted = "Mileage not listed" in html
    glance = "<dt>Mileage</dt>" in html
    spec_row = '<span class="spec-label">Mileage</span>' in html
    if shows == "hidden":
        assert (hero_unlisted, glance, spec_row) == (False, False, False), case
        assert "car-detail-headline-mileage" not in html
    elif shows == "not_listed":
        assert hero_unlisted and glance, case
        assert "car-detail-headline-mileage__value" not in html
        # the spec sheet row exists only when a value (here 0) is stored
        assert spec_row is (overrides["mileage"] is not None), case
    else:
        figure = shows.removesuffix(" mi")
        assert not hero_unlisted and glance and spec_row, case
        assert f'<span class="car-detail-headline-mileage__value">{figure}</span>' in html


# ── home (saved / recommended / history cards) and the landing strip ──────────
# Both read grid-serializer rows; they print "<n> mi" or "— mi" and never said
# "Mileage not listed", so only the hidden case changes there.


def _render_template(app_factory, tmp_path, template, **ctx):
    from flask import render_template

    main = app_factory(ALLOW_DEFAULT_APP_USER="0", RATE_LIMIT_SQLITE_PATH=str(tmp_path / "rate_limits.db"))
    with main.app.test_request_context("/"):
        return render_template(template, **ctx)


def _texts(html, css_class):
    return re.findall(rf'<p class="{css_class}">(.*?)</p>', html)


@pytest.mark.parametrize(("case", "overrides", "shows"), CASES, ids=_IDS)
def test_home_cards_per_case(tmp_path, app_factory, case, overrides, shows):
    car = serialize_car_for_listings_grid(_row(**overrides))
    html = _render_template(
        app_factory, tmp_path, "home.html",
        saved_cars=[car], recommendations=[car], recently_compared=[car], recently_viewed=[car],
        recommendations_eyebrow="", recommendations_title="Recommended for You", recommendations_hint="",
    )
    metas = _texts(html, "reco-card-meta") + _texts(html, "saved-meta")
    assert len(metas) == 4, (case, metas)
    for meta in metas:
        if shows == "hidden":
            assert " mi" not in meta and meta.endswith("Gasoline"), (case, meta)
        else:
            assert " mi" in meta, (case, meta)


@pytest.mark.parametrize(("case", "overrides", "shows"), CASES, ids=_IDS)
def test_landing_strip_per_case(tmp_path, app_factory, case, overrides, shows):
    car = serialize_car_for_listings_grid(_row(dealer_name="Demo Honda", **overrides))
    html = _render_template(app_factory, tmp_path, "landing.html", now=datetime.now(),
                            inventory_count_display="Live", featured_cars=[car])
    (meta,) = _texts(html, "lps-car-meta")
    if shows == "hidden":
        assert meta == "EX &middot; Demo Honda", case
    else:
        mileage = shows if shows not in ("not_listed", "0 mi") else "— mi"
        assert meta == f"EX &middot; {mileage} &middot; Demo Honda", (case, meta)
