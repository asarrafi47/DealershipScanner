"""P10C.3: the linker reconciles forced_induction after linking, in its own transaction."""
from __future__ import annotations

import json
import sqlite3

import pytest

from backend.catalog import linker
from backend.catalog.resolver import _CANDIDATE_COLS
from backend.db.inventory_compat import InventoryConnection

_CAR_COLS = (
    "id", "vin", "dealer_id", "listing_active", "year", "make", "model", "trim",
    "cylinders", "engine_l", "engine_description", "drivetrain", "fuel_type", "title",
    "description", "epa_master_id", "epa_match_confidence", "epa_match_method",
    "forced_induction", "spec_source_json",
)
VIN1, VIN2, VIN3 = "1FTFW1E80TFA00001", "1FTFW1E80TFA00002", "1FTFW1E80TFA00003"


def _epa(id_, model, disp, cyl, t=None, s=None):
    row = dict.fromkeys(_CANDIDATE_COLS)
    row.update(id=id_, year=2022, make="Ford", model=model, trim="Pickup", cylinders=cyl,
               displacement=disp, drive="Rear-Wheel Drive", fuel_type="Regular Gasoline",
               t_charger=t, s_charger=s)
    return tuple(row[c] for c in _CANDIDATE_COLS)


def _car(id_, vin, engine_l, engine_desc, fi, prov=None, title="2022 Ford F150"):
    row = dict.fromkeys(_CAR_COLS)
    row.update(id=id_, vin=vin, dealer_id="d1", listing_active=1, year=2022, make="Ford",
               model="F150", trim="XLT", cylinders=8, engine_l=engine_l,
               engine_description=engine_desc, drivetrain="RWD", fuel_type="Gasoline",
               title=title, forced_induction=fi, spec_source_json=prov)
    return tuple(row[c] for c in _CAR_COLS)


@pytest.fixture
def raw(monkeypatch):
    raw = sqlite3.connect(":memory:")
    raw.execute(f"CREATE TABLE cars ({', '.join(_CAR_COLS)})")
    raw.execute(f"CREATE TABLE epa_master ({', '.join(_CANDIDATE_COLS)})")
    # An NA-only 5.0 V8 family.
    raw.execute(f"INSERT INTO epa_master VALUES ({', '.join('?' * len(_CANDIDATE_COLS))})",
                _epa(1, "F150", 5.0, 8))
    raw.commit()
    monkeypatch.setattr("backend.enrichment.knowledge_engine.prime_vpic_cache", lambda vins: None)
    monkeypatch.setattr("backend.enrichment.knowledge_engine.lookup_vpic_from_cache", lambda vin: {})
    monkeypatch.setattr(linker, "get_conn",
                        lambda: InventoryConnection(raw, backend="sqlite", shared=True))
    yield raw
    raw.close()


def _fi(raw, id_):
    return raw.execute("SELECT forced_induction, spec_source_json FROM cars WHERE id=?", (id_,)).fetchone()


def test_heuristic_turbo_on_na_family_is_rewritten(raw) -> None:
    prov = json.dumps({"forced_induction": {"source": "trim_heuristic", "value": "Twin Turbocharged"}})
    raw.execute(f"INSERT INTO cars VALUES ({', '.join('?' * len(_CAR_COLS))})",
                _car(1, VIN1, 5.0, "5.0L V8", "Twin Turbocharged", prov))
    raw.commit()
    linker.link_cars_by_vins([VIN1])
    label, new_prov = _fi(raw, 1)
    assert label == "Naturally Aspirated"
    assert json.loads(new_prov)["forced_induction"]["source"] == "epa_catalog"


def test_blank_label_is_filled_from_family(raw) -> None:
    raw.execute(f"INSERT INTO cars VALUES ({', '.join('?' * len(_CAR_COLS))})",
                _car(1, VIN1, 5.0, "5.0L V8", None))
    raw.commit()
    linker.link_cars_by_vins([VIN1])
    assert _fi(raw, 1)[0] == "Naturally Aspirated"


def test_dealer_text_turbo_is_kept(raw) -> None:
    prov = json.dumps({"forced_induction": {"source": "dealer_feed", "value": "Turbocharged"}})
    raw.execute(f"INSERT INTO cars VALUES ({', '.join('?' * len(_CAR_COLS))})",
                _car(1, VIN2, 5.0, "5.0L V8 Turbo", "Turbocharged", prov))
    raw.commit()
    linker.link_cars_by_vins([VIN2])
    assert _fi(raw, 1)[0] == "Turbocharged"


def test_unknown_engine_leaves_value_alone(raw) -> None:
    raw.execute(f"INSERT INTO cars VALUES ({', '.join('?' * len(_CAR_COLS))})",
                _car(1, VIN3, None, None, "Turbocharged"))
    raw.commit()
    linker.link_cars_by_vins([VIN3])
    assert _fi(raw, 1)[0] == "Turbocharged"


def test_fi_exception_leaves_links_committed(raw, monkeypatch) -> None:
    raw.execute(f"INSERT INTO cars VALUES ({', '.join('?' * len(_CAR_COLS))})",
                _car(1, VIN1, 5.0, "5.0L V8", "Twin Turbocharged"))
    raw.commit()

    def boom(*a, **k):
        raise RuntimeError("fi bug")

    monkeypatch.setattr("backend.utils.forced_induction.resolve_car_forced_induction", boom)
    n = linker.link_cars_by_vins([VIN1])
    assert n == 1
    assert raw.execute("SELECT epa_master_id FROM cars WHERE id=1").fetchone()[0] == 1
    assert _fi(raw, 1)[0] == "Twin Turbocharged"
