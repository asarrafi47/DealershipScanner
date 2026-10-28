"""P10C.3: the linker reconciles forced_induction after linking, in its own transaction."""
from __future__ import annotations

import json

import pytest

from backend.catalog import linker
from backend.catalog.resolver import _CANDIDATE_COLS

VIN1, VIN2, VIN3 = "1FTFW1E80TFA00001", "1FTFW1E80TFA00002", "1FTFW1E80TFA00003"


def _epa(id_, model, disp, cyl, t=None, s=None):
    row = dict.fromkeys(_CANDIDATE_COLS)
    row.update(id=id_, year=2022, make="Ford", model=model, trim="Pickup", cylinders=cyl,
               displacement=disp, drive="Rear-Wheel Drive", fuel_type="Regular Gasoline",
               t_charger=t, s_charger=s)
    return {k: v for k, v in row.items() if v is not None}


def _car(vin, engine_l, engine_desc, fi, prov=None, title="2022 Ford F150"):
    return dict(vin=vin, dealer_id="d1", year=2022, make="Ford", model="F150", trim="XLT",
                cylinders=8, engine_l=engine_l, engine_description=engine_desc, drivetrain="RWD",
                fuel_type="Gasoline", title=title, forced_induction=fi, spec_source_json=prov)


@pytest.fixture
def raw(pg_db, monkeypatch):
    # An NA-only 5.0 V8 family.
    pg_db._insert("epa_master", [_epa(1, "F150", 5.0, 8)], lambda i, r: {})
    monkeypatch.setattr("backend.enrichment.knowledge_engine.prime_vpic_cache", lambda vins: None)
    monkeypatch.setattr("backend.enrichment.knowledge_engine.lookup_vpic_from_cache", lambda vin: {})
    return pg_db


def _fi(raw, vin):
    with raw.connect() as conn:
        return conn.execute("SELECT forced_induction, spec_source_json FROM cars WHERE vin=%s", (vin,)).fetchone()


def _epa_id(raw, vin):
    with raw.connect() as conn:
        return conn.execute("SELECT epa_master_id FROM cars WHERE vin=%s", (vin,)).fetchone()[0]


@pytest.mark.pg
def test_heuristic_turbo_on_na_family_is_rewritten(raw) -> None:
    prov = json.dumps({"forced_induction": {"source": "trim_heuristic", "value": "Twin Turbocharged"}})
    raw.add_cars([_car(VIN1, 5.0, "5.0L V8", "Twin Turbocharged", prov)])
    linker.link_cars_by_vins([VIN1])
    label, new_prov = _fi(raw, VIN1)
    assert label == "Naturally Aspirated"
    assert json.loads(new_prov)["forced_induction"]["source"] == "epa_catalog"


@pytest.mark.pg
def test_blank_label_is_filled_from_family(raw) -> None:
    raw.add_cars([_car(VIN1, 5.0, "5.0L V8", None)])
    linker.link_cars_by_vins([VIN1])
    assert _fi(raw, VIN1)[0] == "Naturally Aspirated"


@pytest.mark.pg
def test_dealer_text_turbo_is_kept(raw) -> None:
    prov = json.dumps({"forced_induction": {"source": "dealer_feed", "value": "Turbocharged"}})
    raw.add_cars([_car(VIN2, 5.0, "5.0L V8 Turbo", "Turbocharged", prov)])
    linker.link_cars_by_vins([VIN2])
    assert _fi(raw, VIN2)[0] == "Turbocharged"


@pytest.mark.pg
def test_unknown_engine_leaves_value_alone(raw) -> None:
    raw.add_cars([_car(VIN3, None, None, "Turbocharged")])
    linker.link_cars_by_vins([VIN3])
    assert _fi(raw, VIN3)[0] == "Turbocharged"


@pytest.mark.pg
def test_fi_exception_leaves_links_committed(raw, monkeypatch) -> None:
    raw.add_cars([_car(VIN1, 5.0, "5.0L V8", "Twin Turbocharged")])

    def boom(*a, **k):
        raise RuntimeError("fi bug")

    monkeypatch.setattr("backend.utils.forced_induction.resolve_car_forced_induction", boom)
    n = linker.link_cars_by_vins([VIN1])
    assert n == 1
    assert _epa_id(raw, VIN1) == 1
    assert _fi(raw, VIN1)[0] == "Twin Turbocharged"

